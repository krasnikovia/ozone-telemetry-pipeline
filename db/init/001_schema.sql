-- Everything is stored as timestamptz and the database works in UTC.
ALTER DATABASE telemetry SET timezone TO 'UTC';

-- Raw events: the source of truth. One row per device per event time.
-- The primary key makes inserts idempotent: replays and duplicates are ignored.
CREATE TABLE IF NOT EXISTS telemetry_raw (
    device_id      text             NOT NULL,
    ts             timestamptz      NOT NULL,
    temperature_c  double precision NOT NULL,
    pressure_bar   double precision NOT NULL,
    power_w        double precision NOT NULL,
    frequency_hz   double precision NOT NULL,
    status         smallint         NOT NULL CHECK (status IN (0, 1)),
    ingested_at    timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, ts)
);

-- Per-minute aggregates produced by the stream processor.
-- Sums and counts are stored (not only averages) so that hourly/daily
-- rollups can compute exact weighted averages.
CREATE TABLE IF NOT EXISTS telemetry_minute_agg (
    device_id                 text             NOT NULL,
    minute_start              timestamptz      NOT NULL,
    sample_count              integer          NOT NULL CHECK (sample_count > 0),
    power_avg_w               double precision NOT NULL,
    power_min_w               double precision NOT NULL,
    power_max_w               double precision NOT NULL,
    power_sum_w               double precision NOT NULL,
    energy_wh                 double precision NOT NULL,
    frequency_avg_hz          double precision NOT NULL,
    frequency_min_hz          double precision NOT NULL,
    frequency_max_hz          double precision NOT NULL,
    frequency_sum_hz          double precision NOT NULL,
    frequency_max_abs_dev_hz  double precision NOT NULL,
    warning_count             integer          NOT NULL,
    updated_at                timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, minute_start)
);

-- Work queue: device-minutes whose aggregate must be (re)computed.
-- A row is written in the SAME transaction as the raw events and deleted in the
-- SAME transaction as the aggregate upsert. So after any crash, this table says
-- exactly which minutes still need work: nothing depends on in-memory state.
CREATE TABLE IF NOT EXISTS dirty_minutes (
    device_id     text        NOT NULL,
    minute_start  timestamptz NOT NULL,
    PRIMARY KEY (device_id, minute_start)
);

-- Work queue for the Parquet export: hours whose partition file must be rewritten.
-- Writers bump `revision`; the ETL deletes a row only if the revision did not
-- change while it was exporting, otherwise the hour stays queued for the next run.
CREATE TABLE IF NOT EXISTS dirty_hours (
    dataset     text        NOT NULL,
    hour_start  timestamptz NOT NULL,
    revision    bigint      NOT NULL DEFAULT 1,
    PRIMARY KEY (dataset, hour_start)
);

-- Log of Parquet exports (informational): what was written, when, how many rows.
CREATE TABLE IF NOT EXISTS etl_parquet_exports (
    dataset      text        NOT NULL,
    hour_start   timestamptz NOT NULL,
    row_count    integer     NOT NULL,
    file_path    text        NOT NULL,
    exported_at  timestamptz NOT NULL,
    PRIMARY KEY (dataset, hour_start)
);
