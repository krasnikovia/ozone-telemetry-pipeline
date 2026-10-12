-- Hourly and daily rollups built from the minute aggregates (ELT inside Postgres).
-- Materialized views are refreshed by the `etl` service (REFRESH ... CONCURRENTLY,
-- which needs a unique index and does not block readers).
--
-- date_trunc(..., 'UTC') makes bucketing independent of the session timezone.
-- completeness = received samples / expected samples at 1 Hz.

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_hourly AS
SELECT
    device_id,
    date_trunc('hour', minute_start, 'UTC')            AS hour_start,
    sum(sample_count)                                  AS sample_count,
    count(*)                                           AS minutes_with_data,
    round(sum(sample_count)::numeric / 3600, 4)        AS completeness,
    sum(power_sum_w) / sum(sample_count)               AS power_avg_w,
    min(power_min_w)                                   AS power_min_w,
    max(power_max_w)                                   AS power_max_w,
    sum(energy_wh)                                     AS energy_wh,
    sum(frequency_sum_hz) / sum(sample_count)          AS frequency_avg_hz,
    min(frequency_min_hz)                              AS frequency_min_hz,
    max(frequency_max_hz)                              AS frequency_max_hz,
    max(frequency_max_abs_dev_hz)                      AS frequency_max_abs_dev_hz,
    sum(warning_count)                                 AS warning_count
FROM telemetry_minute_agg
GROUP BY device_id, date_trunc('hour', minute_start, 'UTC')
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS telemetry_hourly_pk ON telemetry_hourly (device_id, hour_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_daily AS
SELECT
    device_id,
    (date_trunc('day', minute_start, 'UTC') AT TIME ZONE 'UTC')::date AS day_utc,
    sum(sample_count)                                  AS sample_count,
    count(*)                                           AS minutes_with_data,
    round(sum(sample_count)::numeric / 86400, 4)       AS completeness,
    sum(power_sum_w) / sum(sample_count)               AS power_avg_w,
    min(power_min_w)                                   AS power_min_w,
    max(power_max_w)                                   AS power_max_w,
    sum(energy_wh)                                     AS energy_wh,
    sum(frequency_sum_hz) / sum(sample_count)          AS frequency_avg_hz,
    min(frequency_min_hz)                              AS frequency_min_hz,
    max(frequency_max_hz)                              AS frequency_max_hz,
    max(frequency_max_abs_dev_hz)                      AS frequency_max_abs_dev_hz,
    sum(warning_count)                                 AS warning_count
FROM telemetry_minute_agg
GROUP BY device_id, date_trunc('day', minute_start, 'UTC')
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS telemetry_daily_pk ON telemetry_daily (device_id, day_utc);

-- Business day in Sweden (Europe/Stockholm). On DST switch days a local day
-- has 23 or 25 hours, so expected samples are computed from the real day length.
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_daily_local AS
WITH by_day AS (
    SELECT
        device_id,
        (minute_start AT TIME ZONE 'Europe/Stockholm')::date AS day_local,
        minute_start, sample_count, power_sum_w, power_min_w, power_max_w, energy_wh,
        frequency_sum_hz, frequency_min_hz, frequency_max_hz, frequency_max_abs_dev_hz, warning_count
    FROM telemetry_minute_agg
)
SELECT
    device_id,
    day_local,
    extract(epoch FROM ((day_local + 1)::timestamp AT TIME ZONE 'Europe/Stockholm')
                     - (day_local::timestamp AT TIME ZONE 'Europe/Stockholm')) / 3600 AS day_length_hours,
    sum(sample_count)                                  AS sample_count,
    round(sum(sample_count)::numeric /
          extract(epoch FROM ((day_local + 1)::timestamp AT TIME ZONE 'Europe/Stockholm')
                           - (day_local::timestamp AT TIME ZONE 'Europe/Stockholm')), 4) AS completeness,
    sum(power_sum_w) / sum(sample_count)               AS power_avg_w,
    min(power_min_w)                                   AS power_min_w,
    max(power_max_w)                                   AS power_max_w,
    sum(energy_wh)                                     AS energy_wh,
    sum(frequency_sum_hz) / sum(sample_count)          AS frequency_avg_hz,
    min(frequency_min_hz)                              AS frequency_min_hz,
    max(frequency_max_hz)                              AS frequency_max_hz,
    max(frequency_max_abs_dev_hz)                      AS frequency_max_abs_dev_hz,
    sum(warning_count)                                 AS warning_count
FROM by_day
GROUP BY device_id, day_local
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS telemetry_daily_local_pk ON telemetry_daily_local (device_id, day_local);
