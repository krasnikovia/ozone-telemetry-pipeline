"""
Batch ETL/ELT job, runs every ETL_INTERVAL_S:

1. ELT in Postgres: refresh the hourly / daily materialized views.
2. Export to the Parquet "lake", Hive-partitioned by UTC date/hour:
       <lake>/<dataset>/date=YYYY-MM-DD/hour=HH/data.parquet
   Which hours to (re)write comes from the `dirty_hours` queue, filled by the
   processor in the same transaction as the data. No time-based lookback, so an
   ETL outage of any length is caught up, and late data is picked up.
   Each partition file is fully rewritten (temp file + atomic rename), which
   makes the export idempotent.

Run once:        python -m telemetry.etl --once
Rebuild lake:    python -m telemetry.etl --backfill   (queues every hour, then exports)
"""

import argparse
import logging
import os
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from telemetry.config import EtlConfig
from telemetry.logging_setup import setup_logging

log = logging.getLogger(__name__)

MATERIALIZED_VIEWS = ["telemetry_hourly", "telemetry_daily", "telemetry_daily_local"]

TS = pa.timestamp("ms", tz="UTC")


@dataclass(frozen=True)
class Dataset:
    name: str
    table: str
    time_column: str
    schema: pa.Schema


DATASETS = [
    Dataset(
        "telemetry_raw", "telemetry_raw", "ts",
        pa.schema([
            ("device_id", pa.string()), ("ts", TS), ("temperature_c", pa.float64()), ("pressure_bar", pa.float64()),
            ("power_w", pa.float64()), ("frequency_hz", pa.float64()), ("status", pa.int16()), ("ingested_at", TS),
        ]),
    ),
    Dataset(
        "telemetry_minute_agg", "telemetry_minute_agg", "minute_start",
        pa.schema([
            ("device_id", pa.string()), ("minute_start", TS), ("sample_count", pa.int32()),
            ("power_avg_w", pa.float64()), ("power_min_w", pa.float64()), ("power_max_w", pa.float64()),
            ("power_sum_w", pa.float64()), ("energy_wh", pa.float64()),
            ("frequency_avg_hz", pa.float64()), ("frequency_min_hz", pa.float64()), ("frequency_max_hz", pa.float64()),
            ("frequency_sum_hz", pa.float64()), ("frequency_max_abs_dev_hz", pa.float64()),
            ("warning_count", pa.int32()), ("updated_at", TS),
        ]),
    ),
]


def partition_path(lake: Path, dataset: str, hour_start: datetime) -> Path:
    hour_start = hour_start.astimezone(UTC)
    return lake / dataset / f"date={hour_start:%Y-%m-%d}" / f"hour={hour_start:%H}" / "data.parquet"


def write_parquet_atomic(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Dot-prefixed (dataset readers ignore it) and unique (two ETL runs never share a temp file).
    tmp = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)  # readers never see a half-written file


def refresh_views(conn: psycopg.Connection) -> None:
    populated = dict(conn.execute("SELECT matviewname, ispopulated FROM pg_matviews").fetchall())
    for view in MATERIALIZED_VIEWS:
        # CONCURRENTLY does not block readers, but only works on an already populated view.
        mode = "CONCURRENTLY " if populated.get(view) else ""
        conn.execute(f"REFRESH MATERIALIZED VIEW {mode}{view}")
    conn.commit()


def queued_hours(conn: psycopg.Connection, ds: Dataset) -> list[tuple[datetime, int]]:
    return conn.execute(
        "SELECT hour_start, revision FROM dirty_hours WHERE dataset = %s ORDER BY hour_start", (ds.name,)
    ).fetchall()


def export_hour(conn: psycopg.Connection, lake: Path, ds: Dataset, hour_start: datetime, revision: int) -> int:
    columns = ", ".join(ds.schema.names)
    rows = conn.execute(
        f"SELECT {columns} FROM {ds.table} WHERE {ds.time_column} >= %s AND {ds.time_column} < %s ORDER BY device_id, {ds.time_column}",
        (hour_start, hour_start + timedelta(hours=1)),
    ).fetchall()
    table = pa.Table.from_pylist([dict(zip(ds.schema.names, r)) for r in rows], schema=ds.schema)
    path = partition_path(lake, ds.name, hour_start)
    write_parquet_atomic(table, path)
    # Dequeue only if nobody re-dirtied the hour while we were exporting (revision unchanged).
    # Otherwise the row stays and the hour is exported again on the next run.
    conn.execute(
        "DELETE FROM dirty_hours WHERE dataset = %s AND hour_start = %s AND revision = %s",
        (ds.name, hour_start, revision),
    )
    conn.execute(
        """
        INSERT INTO etl_parquet_exports (dataset, hour_start, row_count, file_path, exported_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (dataset, hour_start) DO UPDATE
        SET row_count = EXCLUDED.row_count, file_path = EXCLUDED.file_path, exported_at = EXCLUDED.exported_at
        """,
        (ds.name, hour_start, len(rows), str(path)),
    )
    conn.commit()
    return len(rows)


def queue_all_hours(conn: psycopg.Connection) -> None:
    """Backfill: queue every hour that has data, e.g. to rebuild the lake from Postgres."""
    for ds in DATASETS:
        conn.execute(
            f"""
            INSERT INTO dirty_hours (dataset, hour_start)
            SELECT %s, date_trunc('hour', {ds.time_column}, 'UTC') FROM {ds.table} GROUP BY 2
            ON CONFLICT (dataset, hour_start) DO UPDATE SET revision = dirty_hours.revision + 1
            """,
            (ds.name,),
        )
    conn.commit()


def run_once(cfg: EtlConfig, backfill: bool = False) -> None:
    started = time.monotonic()
    lake = Path(cfg.lake_path)
    with psycopg.connect(cfg.database_url, options="-c TimeZone=UTC", connect_timeout=10) as conn:
        if backfill:
            queue_all_hours(conn)
        refresh_views(conn)
        exported = 0
        for ds in DATASETS:
            for hour_start, revision in queued_hours(conn, ds):
                rows = export_hour(conn, lake, ds, hour_start, revision)
                exported += 1
                log.info("partition exported", extra={"fields": {"dataset": ds.name, "hour": hour_start.isoformat(), "rows": rows}})
    log.info("etl run finished", extra={"fields": {"partitions_exported": exported, "duration_s": round(time.monotonic() - started, 2)}})


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh rollup views and export Parquet partitions")
    parser.add_argument("--once", action="store_true", help="run a single iteration and exit")
    parser.add_argument("--backfill", action="store_true", help="re-export every hour that has data, then exit")
    args = parser.parse_args()

    setup_logging("etl")
    cfg = EtlConfig()
    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    while running:
        try:
            run_once(cfg, backfill=args.backfill)
        except psycopg.OperationalError as exc:
            log.warning("etl run failed, will retry next interval", extra={"fields": {"error": str(exc)}})
        if args.once or args.backfill:
            break
        deadline = time.monotonic() + cfg.interval_s
        while running and time.monotonic() < deadline:
            time.sleep(0.5)


if __name__ == "__main__":
    main()
