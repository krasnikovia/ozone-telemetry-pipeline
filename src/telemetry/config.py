"""Configuration from environment variables (12-factor style, set in docker-compose)."""

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    raw_topic: str = _env("KAFKA_RAW_TOPIC", "telemetry.raw")
    dlq_topic: str = _env("KAFKA_DLQ_TOPIC", "telemetry.dlq")
    consumer_group: str = _env("KAFKA_CONSUMER_GROUP", "telemetry-processor")


@dataclass(frozen=True)
class CollectorConfig:
    modbus_host: str = _env("MODBUS_HOST", "localhost")
    modbus_port: int = int(_env("MODBUS_PORT", "5020"))
    modbus_device_id: int = int(_env("MODBUS_DEVICE_ID", "1"))
    # Logical asset id used downstream; one collector polls one device.
    device_id: str = _env("DEVICE_ID", "sim-001")
    poll_interval_s: float = float(_env("POLL_INTERVAL_S", "1.0"))
    # Share of messages deliberately corrupted to demonstrate the DLQ (0 = off).
    fault_injection_rate: float = float(_env("FAULT_INJECTION_RATE", "0"))


@dataclass(frozen=True)
class ProcessorConfig:
    database_url: str = _env("DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry")
    # How long we wait for out-of-order events before a minute window is closed.
    allowed_lateness_s: float = float(_env("ALLOWED_LATENESS_S", "10"))
    # Close open windows of a device that stopped sending (wall clock).
    idle_timeout_s: float = float(_env("IDLE_TIMEOUT_S", "30"))
    # Events further in the future than this are treated as a bad device clock.
    max_clock_skew_s: float = float(_env("MAX_CLOCK_SKEW_S", "300"))
    # Nominal sampling interval, used to turn power samples into energy (Wh).
    # The pipeline is built for 1 Hz: must match the collector's POLL_INTERVAL_S.
    sample_interval_s: float = float(_env("SAMPLE_INTERVAL_S", "1.0"))
    # Give up on an unavailable database after this long (stays below Kafka's
    # max.poll.interval.ms = 300 s); the process then exits and is restarted.
    db_max_retry_s: float = float(_env("DB_MAX_RETRY_S", "120"))
    batch_size: int = int(_env("BATCH_SIZE", "500"))
    stats_interval_s: float = float(_env("STATS_INTERVAL_S", "30"))


@dataclass(frozen=True)
class EtlConfig:
    database_url: str = _env("DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry")
    lake_path: str = _env("LAKE_PATH", "./data/lake")
    interval_s: float = float(_env("ETL_INTERVAL_S", "60"))
