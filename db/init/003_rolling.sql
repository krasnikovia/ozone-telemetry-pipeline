-- Rolling (sliding) 60-second statistics, recalculated for every reading.
--
-- telemetry_minute_agg answers "what happened in minute 12:00-12:01" (tumbling window).
-- This view answers "what happened in the 60 seconds up to this reading", i.e. (ts - 60 s, ts]
-- (sliding window): useful for a live dashboard or alerting on a moving average.
--
-- Timestamps have millisecond precision, so 59.999 s keeps exactly 60 s of readings
-- (a 60 s frame would include both ends: 61 readings at 1 Hz).
--
-- It is a plain view over the last hour only, so it is always up to date and cheap.
-- The extra minute in the inner filter makes the first rows of the hour have a full window.
CREATE OR REPLACE VIEW telemetry_rolling_60s AS
SELECT *
FROM (
    SELECT
        device_id,
        ts,
        power_w,
        frequency_hz,
        count(*)          OVER w AS samples_60s,
        avg(power_w)      OVER w AS power_avg_60s_w,
        min(power_w)      OVER w AS power_min_60s_w,
        max(power_w)      OVER w AS power_max_60s_w,
        avg(frequency_hz) OVER w AS frequency_avg_60s_hz,
        min(frequency_hz) OVER w AS frequency_min_60s_hz,
        max(frequency_hz) OVER w AS frequency_max_60s_hz
    FROM telemetry_raw
    WHERE ts > now() - interval '61 minutes'
    WINDOW w AS (PARTITION BY device_id ORDER BY ts RANGE BETWEEN interval '59.999 seconds' PRECEDING AND CURRENT ROW)
) rolling
WHERE ts > now() - interval '60 minutes';
