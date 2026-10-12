"""
Stream processor: Kafka `telemetry.raw` -> validation -> Postgres (raw + minute aggregates).

Delivery semantics: at-least-once from Kafka + idempotent writes = effectively once.

Per batch:
  1. validate every message; poison messages go to the DLQ topic with the reason in headers
  2. insert valid events into telemetry_raw (ON CONFLICT DO NOTHING) and, in the same
     transaction, queue their minutes in `dirty_minutes`
  3. tell the window tracker about each event
  4. aggregate the minutes that are final (incl. minutes re-opened by late events)
     and remove them from `dirty_minutes`, in one transaction
  5. commit Kafka offsets (everything is durably in Postgres / the DLQ at this point)

Designed to run as a single instance (see README).
"""

import logging
import signal
import time
from datetime import UTC, datetime, timedelta

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition

from telemetry.aggregation import DeviceMinute, MinuteWindowTracker, minute_floor
from telemetry.config import KafkaConfig, ProcessorConfig
from telemetry.db import Repository, ShutdownRequested
from telemetry.logging_setup import setup_logging
from telemetry.models import InvalidEvent, TelemetryEvent, parse_event

log = logging.getLogger(__name__)


class DeadLetterQueue:
    """
    Publishes the original message bytes unchanged, plus error metadata in headers.
    Keeping the payload untouched means a fixed consumer can replay the DLQ later.
    """

    def __init__(self, kafka: KafkaConfig) -> None:
        self.topic = kafka.dlq_topic
        self.producer = Producer({"bootstrap.servers": kafka.bootstrap_servers, "enable.idempotence": True, "acks": "all"})
        self.delivery_errors: list[str] = []

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.delivery_errors.append(str(err))

    def send(self, msg: Message, error: InvalidEvent) -> None:
        headers = {
            "error_type": error.error_type,
            "error": str(error)[:1000],
            "source_topic": msg.topic(),
            "source_partition": str(msg.partition()),
            "source_offset": str(msg.offset()),
            "failed_at": datetime.now(UTC).isoformat(),
        }
        self.producer.produce(self.topic, key=msg.key(), value=msg.value(), headers=headers, on_delivery=self._on_delivery)
        self.producer.poll(0)

    def flush(self) -> None:
        # Offsets of DLQ'd messages are committed only after the DLQ write is acknowledged.
        # Crashing here (before the commit) means the batch is re-read and retried after restart.
        remaining = self.producer.flush(30)
        if remaining or self.delivery_errors:
            errors, self.delivery_errors = self.delivery_errors, []
            raise KafkaException(f"DLQ delivery failed: {remaining} pending, errors: {errors[:3]}")


class Processor:
    def __init__(self, cfg: ProcessorConfig, kafka: KafkaConfig) -> None:
        self.cfg = cfg
        self.kafka = kafka
        self.running = True
        self.repo = Repository(cfg.database_url, cfg.sample_interval_s, cfg.db_max_retry_s, is_running=lambda: self.running)
        self.dlq = DeadLetterQueue(kafka)
        self.windows = MinuteWindowTracker(
            allowed_lateness=timedelta(seconds=cfg.allowed_lateness_s),
            idle_timeout_s=cfg.idle_timeout_s,
        )
        self.consumer = Consumer(
            {
                "bootstrap.servers": kafka.bootstrap_servers,
                "group.id": kafka.consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
            }
        )
        self.stats = dict.fromkeys(["consumed", "inserted", "duplicates", "dlq", "late", "minutes_closed"], 0)

    def stop(self, *_: object) -> None:
        self.running = False

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Minutes queued by a previous (crashed) run are re-opened from Postgres,
        # so they still get aggregated even though Kafka will not replay them.
        dirty = self.repo.dirty_minutes()
        for device_id, minute in dirty:
            self.windows.restore(device_id, [minute])
        log.info("partitions assigned", extra={"fields": {"partitions": [p.partition for p in partitions], "restored_dirty_minutes": len(dirty)}})

    def run(self) -> None:
        self.consumer.subscribe([self.kafka.raw_topic], on_assign=self._on_assign)
        log.info("processor started", extra={"fields": {"topic": self.kafka.raw_topic, "group": self.kafka.consumer_group}})

        last_stats = time.monotonic()
        try:
            while self.running:
                messages = self.consumer.consume(num_messages=self.cfg.batch_size, timeout=1.0)
                # Called even for an empty batch, so idle devices still get their windows closed.
                self.process_batch(messages)
                if time.monotonic() - last_stats >= self.cfg.stats_interval_s:
                    log.info("processor stats", extra={"fields": {**self.stats, "open_windows": self.windows.open_windows}})
                    last_stats = time.monotonic()
        except ShutdownRequested:
            log.info("stop requested during a database retry")
        finally:
            log.info("shutting down")
            self.consumer.close()
            self.repo.close()

    def process_batch(self, messages: list[Message]) -> None:
        now = datetime.now(UTC)
        skew = timedelta(seconds=self.cfg.max_clock_skew_s)
        valid: list[TelemetryEvent] = []
        consumed_any = False

        # 1. validate
        for msg in messages:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("consumer error", extra={"fields": {"error": str(msg.error())}})
                continue
            consumed_any = True
            self.stats["consumed"] += 1
            try:
                valid.append(parse_event(msg.value(), now, skew))
            except InvalidEvent as err:
                self.stats["dlq"] += 1
                log.warning(
                    "invalid message sent to DLQ",
                    extra={"fields": {"error_type": err.error_type, "error": str(err), "partition": msg.partition(), "offset": msg.offset()}},
                )
                self.dlq.send(msg, err)

        # 2. store raw events
        if valid:
            inserted = self.repo.insert_raw(valid)
            self.stats["inserted"] += inserted
            self.stats["duplicates"] += len(valid) - inserted

        # 3. track windows
        late: set[DeviceMinute] = set()
        for event in valid:
            if self.windows.add(event.device_id, event.ts):
                late.add((event.device_id, minute_floor(event.ts)))

        # 4. aggregate final minutes (a late event's minute is final right away)
        closed = self.windows.close_ready()
        if closed:
            for device_id, minute, samples, power_avg, freq_avg in self.repo.aggregate_minutes(closed):
                log.info(
                    "minute aggregated",
                    extra={"fields": {"device_id": device_id, "minute": minute.isoformat(), "samples": samples,
                                      "power_avg_w": round(power_avg, 1), "frequency_avg_hz": round(freq_avg, 3),
                                      "reason": "late_data" if (device_id, minute) in late else "window_closed"}},
                )
            self.stats["minutes_closed"] += len(closed)
        self.stats["late"] += len(late)

        # 5. commit
        if consumed_any:
            self.dlq.flush()
            self.consumer.commit(asynchronous=False)


def main() -> None:
    setup_logging("processor")
    processor = Processor(ProcessorConfig(), KafkaConfig())
    signal.signal(signal.SIGTERM, processor.stop)
    signal.signal(signal.SIGINT, processor.stop)
    processor.run()


if __name__ == "__main__":
    main()
