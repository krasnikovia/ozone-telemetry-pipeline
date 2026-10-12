"""
Integration tests against the running stack (Kafka + processor + etl + Postgres).

    docker compose up -d
    docker compose run --rm -e RUN_INTEGRATION=1 tests pytest -v tests/integration

They publish hand-made events with past timestamps for a unique test device
straight to Kafka (no waiting for real minutes) and check what lands in
Postgres, the hourly/daily views, the Parquet lake and the DLQ.
Test rows are removed afterwards, and their hours are re-queued for export.
"""

import json
import os
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeVar

import psycopg
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pytest
from confluent_kafka import OFFSET_END, Consumer, Producer, TopicPartition

from telemetry.aggregation import MinuteWindowTracker, minute_floor
from telemetry.db import Repository
from telemetry.models import TelemetryEvent

pytestmark = pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1 with the stack running")

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DB = os.getenv("DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry")
LAKE = Path(os.getenv("LAKE_PATH", "./data/lake"))
T = TypeVar("T")


def wait_for(check: Callable[[], T], timeout_s: float = 60, what: str = "condition") -> T:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(1)
    raise AssertionError(f"timed out waiting for {what}")


def event(device: str, ts: datetime, power: float, frequency: float = 50.0) -> bytes:
    return json.dumps({
        "schema_version": 1, "device_id": device, "ts": ts.isoformat(),
        "temperature_c": 25.0, "pressure_bar": 10.0, "power_w": power, "frequency_hz": frequency, "status": 0,
    }).encode()


def query_one(sql: str, params: tuple):
    with psycopg.connect(DB) as conn:
        return conn.execute(sql, params).fetchone()


@pytest.fixture
def device():
    device_id = f"it-{uuid.uuid4().hex[:8]}"
    yield device_id
    with psycopg.connect(DB) as conn:  # keep the real data clean
        # Re-queue the touched hours, so the ETL rewrites their Parquet files without the test rows.
        for dataset, column in [("telemetry_raw", "ts"), ("telemetry_minute_agg", "minute_start")]:
            conn.execute(
                f"INSERT INTO dirty_hours (dataset, hour_start) "
                f"SELECT %s, date_trunc('hour', {column}, 'UTC') FROM {dataset} WHERE device_id = %s GROUP BY 2 "
                f"ON CONFLICT (dataset, hour_start) DO UPDATE SET revision = dirty_hours.revision + 1",
                (dataset, device_id),
            )
        for table in ["telemetry_minute_agg", "telemetry_raw", "dirty_minutes"]:
            conn.execute(f"DELETE FROM {table} WHERE device_id = %s", (device_id,))


def minute_row(device: str, minute: datetime):
    return query_one(
        "SELECT sample_count, power_avg_w, power_min_w, power_max_w FROM telemetry_minute_agg "
        "WHERE device_id = %s AND minute_start = %s",
        (device, minute),
    )


def dlq_end_offsets() -> dict[int, int]:
    """Current end of every DLQ partition, so we only read messages produced by this test."""
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": f"it-{uuid.uuid4()}"})
    try:
        partitions = consumer.list_topics("telemetry.dlq", timeout=10).topics["telemetry.dlq"].partitions
        return {p: consumer.get_watermark_offsets(TopicPartition("telemetry.dlq", p), timeout=10)[1] for p in partitions}
    finally:
        consumer.close()


def find_in_dlq(key: bytes, start_offsets: dict[int, int], timeout_s: float = 30) -> dict[str, str]:
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": f"it-{uuid.uuid4()}", "enable.auto.commit": False})
    consumer.assign([TopicPartition("telemetry.dlq", p, offset if offset >= 0 else OFFSET_END) for p, offset in start_offsets.items()])
    try:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if not msg.error() and msg.key() == key:
                    return {k: v.decode() for k, v in msg.headers()}
        raise AssertionError("timed out waiting for DLQ message")
    finally:
        consumer.close()


def lake_rows(dataset: str, device: str) -> int:
    path = LAKE / dataset
    if not path.exists():
        return 0
    table = pads.dataset(path, format="parquet", partitioning="hive").to_table(columns=["device_id"])
    return pc.sum(pc.equal(table["device_id"], device)).as_py() or 0


def test_events_flow_through_the_whole_pipeline(device):
    producer = Producer({"bootstrap.servers": KAFKA})
    dlq_start = dlq_end_offsets()
    t0 = minute_floor(datetime.now(UTC) - timedelta(minutes=10))
    key = device.encode()

    def send(payload: bytes) -> None:
        producer.produce("telemetry.raw", key=key, value=payload)

    # One full minute: power 1000..1059 W
    for i in range(60):
        send(event(device, t0 + timedelta(seconds=i), power=1000 + i))
    send(event(device, t0 + timedelta(seconds=5), power=1005))         # duplicate
    send(event(device, t0 + timedelta(seconds=6), 1, frequency=0.0))   # impossible value -> DLQ
    send(event(device, t0 + timedelta(seconds=75), power=500))         # moves the watermark past t0+1min
    producer.flush(10)

    # Minute aggregate: duplicate ignored, invalid message excluded
    row = wait_for(lambda: minute_row(device, t0), what="minute aggregate")
    sample_count, power_avg, power_min, power_max = row
    assert sample_count == 60
    assert power_avg == pytest.approx(1029.5)
    assert (power_min, power_max) == (1000, 1059)

    # Sliding 60 s window, frame (ts - 60 s, ts]:
    #   at t0+59 s -> readings t0+0..59 s (60 readings, avg 1029.5 W)
    #   at t0+75 s -> readings t0+16..59 s (44) + the 500 W reading itself = 45
    rolling = """SELECT samples_60s, power_avg_60s_w FROM telemetry_rolling_60s WHERE device_id = %s AND ts = %s"""
    samples, avg = query_one(rolling, (device, t0 + timedelta(seconds=59)))
    assert (samples, avg) == (60, pytest.approx(1029.5))
    assert query_one(rolling, (device, t0 + timedelta(seconds=75)))[0] == 45

    # Late event for the already closed minute -> minute is recomputed from raw
    send(event(device, t0 + timedelta(seconds=30, milliseconds=500), power=2000))
    producer.flush(10)
    row = wait_for(lambda: (r := minute_row(device, t0)) and r[0] == 61 and r, what="late event recompute")
    assert row[3] == 2000

    # The invalid message is in the DLQ with the reason in headers
    headers = find_in_dlq(key, dlq_start)
    assert headers["error_type"] == "schema_violation"
    assert "frequency_hz" in headers["error"]

    # The correction reaches the hourly/daily views and the Parquet lake (ETL runs every 60 s).
    # Expected: 61 samples in minute t0 + 1 sample (the 500 W event) in minute t0+1 = 62.
    # The second minute closes on the idle timeout, so wait for the full total.
    wait_for(
        lambda: query_one("SELECT sum(sample_count) FROM telemetry_hourly WHERE device_id = %s", (device,))[0] == 62,
        timeout_s=150, what="hourly view refresh",
    )
    daily = query_one("SELECT sum(sample_count) FROM telemetry_daily WHERE device_id = %s", (device,))[0]
    assert daily == 62
    wait_for(lambda: lake_rows("telemetry_raw", device) == 62, timeout_s=150, what="raw rows in Parquet")
    wait_for(lambda: lake_rows("telemetry_minute_agg", device) == 2, timeout_s=150, what="minute aggregates in Parquet")


def test_simulators_feed_the_pipeline_through_the_collectors():
    """simulator -> Modbus -> collector -> Kafka -> processor -> Postgres, with the real services."""
    devices = ["sim-001", "sim-002"]  # DEVICE_ID of the two collectors in docker-compose.yml

    def recent_samples(device_id: str) -> int:
        return query_one("SELECT count(*) FROM telemetry_raw WHERE device_id = %s AND ts > now() - interval '30 seconds'", (device_id,))[0]

    for device_id in devices:
        # ~1 Hz; a few readings per window go to the DLQ on purpose (FAULT_INJECTION_RATE).
        assert wait_for(lambda: recent_samples(device_id) >= 20, timeout_s=30, what=f"fresh readings from {device_id}")
        latest = query_one(
            "SELECT power_w, frequency_hz FROM telemetry_raw WHERE device_id = %s ORDER BY ts DESC LIMIT 1", (device_id,)
        )
        assert latest[0] > 0 and 45 <= latest[1] <= 55  # decoded from Modbus registers into real units
        # and closed minutes are aggregated
        assert wait_for(
            lambda: query_one("SELECT count(*) FROM telemetry_minute_agg WHERE device_id = %s", (device_id,))[0] > 0,
            timeout_s=90, what=f"minute aggregate for {device_id}",
        )


def test_dirty_minute_survives_a_crash_before_aggregation(device):
    """
    Regression: the process stores raw events (and their dirty-minute marker) and
    dies before aggregating. A fresh process must find and aggregate the minute
    from Postgres alone, because Kafka will not replay the committed messages.
    """
    t0 = minute_floor(datetime.now(UTC) - timedelta(minutes=30))
    events = [
        TelemetryEvent(schema_version=1, device_id=device, ts=t0 + timedelta(seconds=i),
                       temperature_c=25.0, pressure_bar=10.0, power_w=1000.0 + i, frequency_hz=50.0, status=0)
        for i in range(60)
    ]
    crashed = Repository(DB, sample_interval_s=1.0)
    assert crashed.insert_raw(events) == 60
    crashed.close()  # "crash": no aggregation happened
    assert minute_row(device, t0) is None

    # "Restart": rebuild the window state from dirty_minutes only (this test's device).
    repo = Repository(DB, sample_interval_s=1.0)
    clock = [0.0]
    tracker = MinuteWindowTracker(timedelta(seconds=10), idle_timeout_s=30, clock=lambda: clock[0])
    for dev, minute in repo.dirty_minutes():
        if dev == device:
            tracker.restore(dev, [minute])
    clock[0] = 31  # no new events for this device -> idle close
    closed = tracker.close_ready()
    assert closed == [(device, t0)]
    repo.aggregate_minutes(closed)

    assert minute_row(device, t0)[0] == 60
    assert query_one("SELECT count(*) FROM dirty_minutes WHERE device_id = %s", (device,))[0] == 0
    repo.close()
