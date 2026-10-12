from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from telemetry.etl import DATASETS, partition_path, write_parquet_atomic


def test_partition_path_uses_utc_hour():
    # 01:30 at +02:00 is 23:30 UTC on the previous day
    local = datetime(2026, 10, 12, 1, 30, tzinfo=timezone(timedelta(hours=2)))
    path = partition_path(Path("/lake"), "telemetry_raw", local)
    assert path.as_posix() == "/lake/telemetry_raw/date=2026-10-11/hour=23/data.parquet"


def test_parquet_roundtrip_keeps_utc_timestamps(tmp_path: Path):
    raw = DATASETS[0]
    ts = datetime(2026, 10, 12, 12, 0, 0, 123000, tzinfo=UTC)
    row = {
        "device_id": "sim-001", "ts": ts, "temperature_c": 25.0, "pressure_bar": 10.0,
        "power_w": 1000.0, "frequency_hz": 50.0, "status": 0, "ingested_at": ts,
    }
    path = tmp_path / "date=2026-10-12" / "hour=12" / "data.parquet"

    write_parquet_atomic(pa.Table.from_pylist([row], schema=raw.schema), path)

    table = pq.read_table(path)
    assert table.schema.field("ts").type == pa.timestamp("ms", tz="UTC")
    assert table.column("ts")[0].as_py() == ts
    assert [f.name for f in path.parent.iterdir()] == ["data.parquet"]  # no temp file left behind


def test_rewriting_a_partition_replaces_it(tmp_path: Path):
    raw = DATASETS[0]
    path = tmp_path / "data.parquet"
    ts = datetime(2026, 10, 12, 12, tzinfo=UTC)
    row = {
        "device_id": "sim-001", "ts": ts, "temperature_c": 25.0, "pressure_bar": 10.0,
        "power_w": 1000.0, "frequency_hz": 50.0, "status": 0, "ingested_at": ts,
    }
    write_parquet_atomic(pa.Table.from_pylist([row], schema=raw.schema), path)
    write_parquet_atomic(pa.Table.from_pylist([row, {**row, "ts": ts + timedelta(seconds=1)}], schema=raw.schema), path)
    assert pq.read_table(path).num_rows == 2
