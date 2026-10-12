# Real-time telemetry pipeline

Modbus simulator → Kafka → stream processor → Postgres (raw, per-minute, hourly, daily) + Parquet lake.

```
┌───────────┐ Modbus TCP ┌───────────┐  telemetry.raw   ┌────────────┐   raw + minute aggs   ┌──────────┐
│ simulator │──────────▶ │ collector │ ───────────────▶ │ processor  │ ────────────────────▶ │ Postgres │
│  ×2 devs  │  poll 1 Hz │  (edge)   │  key=device_id   │ validate,  │                       │          │
└───────────┘            └───────────┘   (Kafka)        │ window 1m  │                       │ MVs:     │
                                                        └─────┬──────┘                       │ hourly,  │
                                                              │ poison messages              │ daily    │
                                                              ▼                              └────┬─────┘
                                                        telemetry.dlq                             │ every 60 s
                                                                                            ┌─────▼─────┐
                                                                                            │    etl    │──▶ data/lake/*.parquet
                                                                                            └───────────┘
```

## Quick start

```bash
docker compose up -d --build
docker compose logs -f processor        # "minute aggregated" lines appear after ~1 minute
docker compose run --rm tests           # unit tests
docker compose run --rm -e RUN_INTEGRATION=1 tests pytest -v tests/integration   # integration tests (stack must be running)
docker compose run --rm etl python -m telemetry.etl --backfill   # rebuild the whole Parquet lake from Postgres (stack running)
docker compose down -v                  # stop and delete all data (Postgres, Kafka)
rm -rf data/lake/*                      # also delete the Parquet files (they live on the host)
```

Look at the data:

```bash
docker compose exec postgres psql -U telemetry -c "SELECT * FROM telemetry_minute_agg ORDER BY minute_start DESC LIMIT 5"
docker compose exec postgres psql -U telemetry -c "SELECT * FROM telemetry_rolling_60s ORDER BY ts DESC LIMIT 5"
docker compose exec postgres psql -U telemetry -c "SELECT * FROM telemetry_hourly"
docker compose exec postgres psql -U telemetry -c "SELECT * FROM telemetry_daily_local"
ls -R data/lake
```

> **Ports:** Postgres is published on `5432` and Kafka on `29092`. If they are taken, set `PG_HOST_PORT` / `KAFKA_HOST_PORT`, e.g.
> `PG_HOST_PORT=15432 docker compose up -d --build`.
>
> **Linux:** the `etl` container writes Parquet into `./data/lake` as uid/gid 1000. If your user has other ids, start with
> `HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d --build`.

Look at the DLQ (the collector corrupts ~2% of messages on purpose, `FAULT_INJECTION_RATE`):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.dlq --from-beginning --property print.headers=true --max-messages 5
```

## Services

| Service | Role |
|---|---|
| `simulator`, `simulator-2` | Two simulated devices. Upstream [mock-modbus-sim](https://github.com/rajat-bhardwaj/mock-modbus-sim), vendored unchanged; `run_server.py` starts only its server part, bound to `0.0.0.0`. |
| `collector`, `collector-2` | One per device (`sim-001`, `sim-002`). Polls 5 holding registers every second, decodes them, stamps event time (UTC), publishes JSON to `telemetry.raw`. Does not validate: bad readings are rejected downstream. |
| `kafka` / `kafka-init` | Single-node Kafka in KRaft mode; topics are created explicitly (auto-create off). |
| `processor` | Validates, writes raw events, decides when a minute is final and aggregates it, routes poison messages to `telemetry.dlq`. Runs as a single instance (see below). |
| `etl` | Every 60 s: refreshes hourly/daily materialized views, exports queued hours to Parquet. |
| `postgres` | Storage and serving layer. Schema in `db/init/`. |

## Design decisions

### Kafka over RabbitMQ
Telemetry is an append-only stream that we may need to **re-read**: after a processor crash (uncommitted messages are re-processed) or to backfill a new consumer. Kafka keeps the log (7 days here), and RabbitMQ deletes a message once it is acked. Kafka also gives **per-key ordering**: the key is `device_id`, so one device's events stay ordered within a partition and partitions scale out across devices. This is close to the target GCP setup (Pub/Sub with ordering keys). The cost is that the DLQ is a plain topic we write to ourselves, while RabbitMQ has dead-letter exchanges built in.

### Message contract
JSON, one event per message, versioned (`schema_version`), validated with Pydantic:

```json
{"schema_version": 1, "device_id": "sim-001", "ts": "2026-10-12T12:00:00.123Z",
 "temperature_c": 25.1, "pressure_bar": 10.2, "power_w": 1012.0, "frequency_hz": 50.02, "status": 0}
```

Units are in the field names. Extra fields, NaN/Inf, naive timestamps and physically implausible values are rejected. Bounds: frequency 45–55 Hz, power 0–65535 W, temperature −40…120 °C, status ∈ {0, 1}. In production I would move to Avro/Protobuf with a schema registry. JSON keeps this exercise readable.

### Datetime handling
- The simulator registers carry **no timestamp**, so the collector stamps **event time** when it reads them: `datetime.now(UTC)`, truncated to ms. Millisecond precision is part of the contract: the processor truncates any finer timestamp from other producers.
- Every stage works in UTC: Pydantic requires a timezone and converts to UTC, Postgres stores `timestamptz` and the sessions use `TimeZone=UTC`, Parquet uses `timestamp[ms, tz=UTC]`, partitions are UTC `date=/hour=`.
- Aggregation uses **event time, not processing time**, so a backlog replayed after an outage lands in the correct minutes.
- Events more than 5 minutes in the future are rejected as clock skew.
- `telemetry_daily_local` adds the Swedish business day (`Europe/Stockholm`). It accounts for 23- and 25-hour DST days when computing completeness.
- The collector polls on a fixed monotonic grid, so the sampling rate does not drift.

### Stream processing: per-minute windows
- **Tumbling 1-minute windows per device, on event time.** Inputs: **power** (avg, min, max, sum, energy Wh) and **grid frequency** (avg, min, max, max |f − 50 Hz|). Also `sample_count` (completeness, expected 60/min) and `warning_count`. Power gives energy, which is what gets traded. Frequency deviation is what grid services such as FCR react to.
- **Watermark** = max event time seen − `ALLOWED_LATENESS_S` (10 s). A minute is final when the watermark passes its end: we wait 10 s for out-of-order events. If a device goes silent, its open minutes are closed after `IDLE_TIMEOUT_S` of wall time.
- **Who computes what.** Statistics of a final minute are computed by **one SQL statement over `telemetry_raw`** and upserted into `telemetry_minute_agg`. The raw table is the single source of truth: an aggregate can always be rebuilt from it, and duplicates cannot distort it because raw rows are unique per `(device_id, ts)`.
- **Which minutes need work is stored durably.** `dirty_minutes` is a work queue in Postgres:
  - a minute is added in the **same transaction** as its raw events;
  - it is removed in the **same transaction** as its aggregate upsert.

  So at any moment, even right after a crash, Postgres knows exactly which minutes still have to be aggregated. The in-memory `MinuteWindowTracker` only decides *when* a queued minute is final (watermark / idle). On startup it is rebuilt from `dirty_minutes`.
- **Late events** use the same path. A late event (its minute is already behind the watermark) re-queues and re-opens its minute, which is final immediately: it is recomputed from raw and the aggregate is corrected, not dropped.
- Min, max, sum and count are stored, not only averages, so hourly and daily rollups compute **exact weighted averages** (`sum(sum)/sum(count)`).
- *Trade-off:* computing in Postgres costs one small query per device-minute (a range scan on the primary key). That is negligible at this scale. At thousands of devices I would aggregate in the stream (in memory) or use TimescaleDB continuous aggregates, which work the same way: incremental materialization over the raw hypertable.

### "Rolling" aggregates
The task says *rolling per-minute aggregates*. I read it as **per-minute buckets** (tumbling windows), because the hourly and daily rollups are built on top of them, and only non-overlapping buckets can be summed up exactly. The sliding interpretation is covered too: `telemetry_rolling_60s` is a view with window functions (`RANGE BETWEEN interval '59.999 seconds' PRECEDING AND CURRENT ROW`; timestamps are in ms) that gives, for every reading, the avg/min/max of the 60 s up to it, `(ts − 60 s, ts]`. That suits a live dashboard or alerting on a moving average.

### Delivery guarantees, crash recovery, idempotency
- Producer: `enable.idempotence=true`, `acks=all`.
- Consumer: auto-commit off. Offsets are committed only after the raw insert (together with its dirty-minute marker) and the DLQ write are durable → **at-least-once**. A minute may still be open at that point; that is safe because it is queued in `dirty_minutes`. Re-processing a message is harmless because every write is idempotent: `ON CONFLICT DO NOTHING` for raw, recomputation from raw for aggregates. Result: **effectively-once** from the moment Kafka acknowledged a message.
- **Crash between storing raw data and aggregating** (offsets already committed, so Kafka will not replay): the minute is still in `dirty_minutes`, so the restarted processor finds it and aggregates it.
- **Edge boundary.** The guarantee starts at Kafka. If Kafka is unreachable, the collector buffers in memory for up to 10 minutes and waits when its local queue is full. Longer outages or a collector crash lose the buffered readings. Production would add a durable on-disk spool at the edge (e.g. a local TimescaleDB or SQLite buffer).
- **Single processor instance.** Recovery reads all of `dirty_minutes`, not only the minutes of the partitions a process owns, and two processes could aggregate the same minute concurrently. Running several processors would need partition-aware recovery and serialized aggregate writes. One instance easily handles this load.

### DLQ: poison messages vs. transient errors
- **Poison message** (bad JSON, schema violation, impossible value, future timestamp) → `telemetry.dlq`. The original bytes are kept unchanged, so the message can be replayed. The reason, source partition/offset and time go in Kafka headers.
- **Transient error** (Postgres down) → retry with exponential backoff, offsets not committed, no DLQ. A database outage must never turn good data into "dead letters". The retry is bounded (`DB_MAX_RETRY_S`, 120 s per database call, connect timeout 10 s) and reacts to a stop signal. If the database stays down, the processor exits, Docker restarts it, and it re-reads the uncommitted messages.

### ETL/ELT and storage
- **Postgres** is the serving and query layer. `telemetry_raw` → `telemetry_minute_agg` (stream) → `telemetry_hourly`, `telemetry_daily`, `telemetry_daily_local` (materialized views, ELT in SQL), plus the live `telemetry_rolling_60s` view. The ETL refreshes them `CONCURRENTLY`, so readers are not blocked. Rollups are built from minute aggregates, not raw rows: 60× fewer rows.
- **Parquet** is the analytics and archive layer, the stand-in for GCS + BigQuery: `data/lake/<dataset>/date=YYYY-MM-DD/hour=HH/data.parquet` for `telemetry_raw` and `telemetry_minute_agg`. Which hours to (re)write comes from the `dirty_hours` queue, filled in the same transaction as the data: raw inserts and aggregate upserts bump the hour's `revision`. The ETL dequeues an hour only if its revision did not change during the export, otherwise it exports it again on the next run. There is no time window: late data and ETL outages of any length are caught up. `--backfill` queues every hour, to rebuild the lake from Postgres. Every export rewrites the partition through a temp file + atomic rename, which makes it idempotent and avoids half-written files.
- The Parquet writer is a separate batch job reading from Postgres, not part of the stream processor. The stream path then has only one transactional sink to keep consistent with Kafka offsets, and the lake can be rebuilt from Postgres at any time.

### Logs and observability
JSON logs on stdout (`ts`, `level`, `service`, message + structured fields), ready for Cloud Logging, Loki or ELK. Each service logs periodic counters (consumed / inserted / duplicates / dlq / late / minutes_closed / open_windows). In production these would be Prometheus metrics, with alerts on DLQ rate, consumer lag and completeness < 95%.

## Data model

| Table / view | Grain | Key |
|---|---|---|
| `telemetry_raw` | one reading | `(device_id, ts)` |
| `telemetry_minute_agg` | device × minute (UTC) | `(device_id, minute_start)` |
| `telemetry_rolling_60s` (view) | one reading + stats of the 60 s up to it, last hour | `(device_id, ts)` |
| `telemetry_hourly` (MV) | device × hour (UTC) | `(device_id, hour_start)` |
| `telemetry_daily` (MV) | device × day (UTC) | `(device_id, day_utc)` |
| `telemetry_daily_local` (MV) | device × day (Europe/Stockholm) | `(device_id, day_local)` |
| `dirty_minutes` | work queue: minutes to aggregate | `(device_id, minute_start)` |
| `dirty_hours` | work queue: Parquet partitions to rewrite | `(dataset, hour_start)` |
| `etl_parquet_exports` | log of exports (rows, file, time) | `(dataset, hour_start)` |

**Sampling contract: 1 Hz.** `energy_wh` = Σ power × `SAMPLE_INTERVAL_S` / 3600, and `completeness` divides by 60 / 3600 / 86400 expected samples. `POLL_INTERVAL_S` (collector) and `SAMPLE_INTERVAL_S` (processor) must stay equal, which they are by default. Missing samples show up as lower `completeness`, not as invented energy.

## Tests

**Unit tests** (`tests/test_*.py`, no infrastructure needed): register decoding (incl. negative INT16); validation and DLQ classification (bad JSON, missing fields, out of range, NaN, naive or future timestamps, timezone conversion); window logic (watermark, out-of-order, late events, idle close, several devices, restore after restart); Parquet partitioning and timestamp round-trip.

**Integration tests** (`tests/integration/`, need the running stack). They publish hand-made events with past timestamps for a unique test device straight to Kafka, so no waiting for real minutes:
- *whole pipeline*: a full minute, a duplicate, an invalid message and a late event. Checks the minute aggregate (duplicate ignored, invalid excluded, late event corrects it: 60 → 61 samples), the sliding 60 s window, the DLQ message and its reason, the hourly and daily views, and the rows in the Parquet lake;
- *real devices*: readings from both simulators arrive through the collectors, decoded into real units, and get minute aggregates;
- *crash before aggregation*: raw rows are stored, then the process "dies" before aggregating. A fresh tracker rebuilt from `dirty_minutes` must aggregate the minute.

The tests delete their rows afterwards and re-queue the touched hours, so the lake is rewritten without them.

```bash
docker compose run --rm tests                                                    # unit
docker compose run --rm -e RUN_INTEGRATION=1 tests pytest -v tests/integration   # integration
```

## Checking consistency

Every minute aggregate must match a `GROUP BY` over the raw table (`ok` is `t` in every row):

```sql
SELECT a.minute_start, a.sample_count, r.cnt,
       a.sample_count = r.cnt AND abs(a.power_avg_w - r.avg_p) < 1e-6 AS ok
FROM telemetry_minute_agg a
JOIN (SELECT device_id, date_trunc('minute', ts) m, count(*) cnt, avg(power_w) avg_p
      FROM telemetry_raw GROUP BY 1, 2) r
  ON r.device_id = a.device_id AND r.m = a.minute_start
ORDER BY 1;
```

## Notes

- **Repeated readings.** The simulator refreshes its registers only when ≥ 1.0 s passed since its last refresh, triggered by a read. With 1 Hz polling, two consecutive reads sometimes return the same values. They are still distinct samples (different `ts`). A real device would expose its own measurement timestamp or a counter to detect stale values.
- **DLQ duplicates on replay.** If the processor crashes after sending to the DLQ but before committing, replayed poison messages are sent to the DLQ again. DLQ consumers can deduplicate by the `source_partition` + `source_offset` headers.
- **Git Bash on Windows** rewrites absolute paths such as `/opt/kafka/...`. Prefix the `docker compose exec kafka ...` commands with `MSYS_NO_PATHCONV=1`.


## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `POLL_INTERVAL_S` | 1.0 | Modbus polling interval |
| `FAULT_INJECTION_RATE` | 0 (0.02 in compose) | Share of deliberately corrupted messages |
| `ALLOWED_LATENESS_S` | 10 | How long to wait for out-of-order events |
| `IDLE_TIMEOUT_S` | 30 | Close windows of a silent device |
| `MAX_CLOCK_SKEW_S` | 300 | Reject events further in the future |
| `ETL_INTERVAL_S` | 60 | ETL schedule |
| `SAMPLE_INTERVAL_S` | 1.0 | Sampling interval used for energy; must equal `POLL_INTERVAL_S` (1 Hz contract) |
| `DB_MAX_RETRY_S` | 120 | Processor gives up on an unavailable database after this, exits and is restarted |
| `PG_HOST_PORT` / `KAFKA_HOST_PORT` | 5432 / 29092 | Host ports published by docker compose |
