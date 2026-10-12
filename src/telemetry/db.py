"""Postgres access for the stream processor. All writes are idempotent."""

import logging
import time
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import TypeVar

import psycopg

from telemetry.aggregation import ONE_MINUTE, DeviceMinute, minute_floor
from telemetry.models import NOMINAL_FREQUENCY_HZ, TelemetryEvent

log = logging.getLogger(__name__)
T = TypeVar("T")

# Duplicates (same device + event time) are silently ignored.
INSERT_RAW = """
INSERT INTO telemetry_raw (device_id, ts, temperature_c, pressure_bar, power_w, frequency_hz, status)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (device_id, ts) DO NOTHING
"""

MARK_MINUTE_DIRTY = """
INSERT INTO dirty_minutes (device_id, minute_start) VALUES (%s, %s)
ON CONFLICT DO NOTHING
"""

MARK_HOUR_DIRTY = """
INSERT INTO dirty_hours (dataset, hour_start) VALUES (%s, %s)
ON CONFLICT (dataset, hour_start) DO UPDATE SET revision = dirty_hours.revision + 1
"""

# Build (or rebuild) one minute aggregate from the raw table and upsert it.
# Running it twice gives the same result, and running it again after late
# data arrived corrects the aggregate.
AGGREGATE_MINUTE = """
INSERT INTO telemetry_minute_agg (
    device_id, minute_start, sample_count,
    power_avg_w, power_min_w, power_max_w, power_sum_w, energy_wh,
    frequency_avg_hz, frequency_min_hz, frequency_max_hz, frequency_sum_hz, frequency_max_abs_dev_hz,
    warning_count
)
SELECT
    device_id, %(minute)s, count(*),
    avg(power_w), min(power_w), max(power_w), sum(power_w), sum(power_w) * %(interval)s / 3600.0,
    avg(frequency_hz), min(frequency_hz), max(frequency_hz), sum(frequency_hz),
    max(abs(frequency_hz - %(nominal)s)),
    count(*) FILTER (WHERE status = 1)
FROM telemetry_raw
WHERE device_id = %(device_id)s AND ts >= %(minute)s AND ts < %(minute_end)s
GROUP BY device_id
ON CONFLICT (device_id, minute_start) DO UPDATE SET
    sample_count = EXCLUDED.sample_count,
    power_avg_w = EXCLUDED.power_avg_w,
    power_min_w = EXCLUDED.power_min_w,
    power_max_w = EXCLUDED.power_max_w,
    power_sum_w = EXCLUDED.power_sum_w,
    energy_wh = EXCLUDED.energy_wh,
    frequency_avg_hz = EXCLUDED.frequency_avg_hz,
    frequency_min_hz = EXCLUDED.frequency_min_hz,
    frequency_max_hz = EXCLUDED.frequency_max_hz,
    frequency_sum_hz = EXCLUDED.frequency_sum_hz,
    frequency_max_abs_dev_hz = EXCLUDED.frequency_max_abs_dev_hz,
    warning_count = EXCLUDED.warning_count,
    updated_at = now()
RETURNING device_id, minute_start, sample_count, power_avg_w, frequency_avg_hz
"""


def hour_floor(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


class ShutdownRequested(Exception):
    """Raised from a DB retry loop when the process was asked to stop."""


class Repository:
    def __init__(
        self,
        dsn: str,
        sample_interval_s: float,
        max_retry_s: float = 120.0,
        is_running: Callable[[], bool] = lambda: True,
    ) -> None:
        self.dsn = dsn
        self.sample_interval_s = sample_interval_s
        self.max_retry_s = max_retry_s
        self.is_running = is_running
        self._conn: psycopg.Connection | None = None

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.dsn, options="-c TimeZone=UTC", connect_timeout=10)
        return self._conn

    def _sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if not self.is_running():
                raise ShutdownRequested("stop requested while waiting for the database")
            time.sleep(0.5)

    def with_retry(self, operation: Callable[[psycopg.Connection], T], what: str) -> T:
        """
        Retry transient DB errors with exponential backoff, for at most `max_retry_s`.

        A DB outage must not send good data to the DLQ: while we retry, Kafka
        offsets are not committed. The retry is bounded (below Kafka's
        max.poll.interval.ms) and interruptible: if the DB stays down, the error
        propagates, the process exits, Docker restarts it and it re-reads the
        uncommitted messages from Kafka.
        """
        started = time.monotonic()
        backoff = 1.0
        while True:
            try:
                conn = self._connection()
                with conn.transaction():
                    return operation(conn)
            except psycopg.OperationalError as exc:
                if self._conn is not None:
                    self._conn.close()
                if time.monotonic() - started + backoff > self.max_retry_s:
                    log.error("db unavailable, giving up", extra={"fields": {"op": what, "error": str(exc)}})
                    raise
                log.warning("db unavailable, retrying", extra={"fields": {"op": what, "error": str(exc), "retry_in_s": backoff}})
                self._sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def insert_raw(self, events: list[TelemetryEvent]) -> int:
        """
        Insert raw events and, in the same transaction, queue their minutes for
        aggregation and their hours for Parquet export. Returns how many events
        were new (the rest were duplicates).
        """
        rows = [
            (e.device_id, e.ts, e.temperature_c, e.pressure_bar, e.power_w, e.frequency_hz, e.status)
            for e in events
        ]
        minutes = sorted({(e.device_id, minute_floor(e.ts)) for e in events})
        hours = sorted({("telemetry_raw", hour_floor(e.ts)) for e in events})

        def op(conn: psycopg.Connection) -> int:
            with conn.cursor() as cur:
                cur.executemany(INSERT_RAW, rows)
                inserted = cur.rowcount
                cur.executemany(MARK_MINUTE_DIRTY, minutes)
                cur.executemany(MARK_HOUR_DIRTY, hours)
                return inserted

        return self.with_retry(op, "insert_raw")

    def aggregate_minutes(self, minutes: Iterable[DeviceMinute]) -> list[tuple]:
        """
        Upsert the aggregates, remove the minutes from the work queue and queue
        their hours for export, all in one transaction.
        Returns (device_id, minute, samples, power_avg, freq_avg) rows.
        """
        minutes = sorted(set(minutes))
        if not minutes:
            return []
        params = [
            {
                "device_id": device_id,
                "minute": minute,
                "minute_end": minute + ONE_MINUTE,
                "interval": self.sample_interval_s,
                "nominal": NOMINAL_FREQUENCY_HZ,
            }
            for device_id, minute in minutes
        ]
        hours = sorted({("telemetry_minute_agg", hour_floor(minute)) for _, minute in minutes})

        def op(conn: psycopg.Connection) -> list[tuple]:
            rows: list[tuple] = []
            with conn.cursor() as cur:
                cur.executemany(AGGREGATE_MINUTE, params, returning=True)
                while True:
                    rows.extend(cur.fetchall())
                    if not cur.nextset():
                        break
                cur.executemany("DELETE FROM dirty_minutes WHERE device_id = %s AND minute_start = %s", minutes)
                cur.executemany(MARK_HOUR_DIRTY, hours)
            return rows

        return self.with_retry(op, "aggregate_minutes")

    def dirty_minutes(self) -> list[DeviceMinute]:
        """Minutes that have raw data but no up-to-date aggregate yet."""
        return self.with_retry(
            lambda conn: conn.execute("SELECT device_id, minute_start FROM dirty_minutes").fetchall(),
            "dirty_minutes",
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
