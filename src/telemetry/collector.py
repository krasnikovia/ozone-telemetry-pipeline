"""
Edge collector: polls the Modbus device and publishes telemetry events to Kafka.

- Event time is stamped here (UTC, ms precision): the simulator registers carry no timestamp.
- The collector does NOT validate readings: a broken sensor value is published as is
  and rejected downstream by the processor (-> DLQ), so one bad reading never stops collection.
- Kafka key = device_id, so all events of one device land in one partition, in order.
- Producer is idempotent with acks=all: broker retries do not create duplicates
  and a message is acknowledged only once it is durably written.
- If Kafka is unreachable, librdkafka buffers messages in memory and retries for up
  to 10 minutes. Delivery guarantees start once Kafka has acknowledged a message;
  there is no durable on-disk spool at the edge (see README).
- If Modbus is unreachable we reconnect with exponential backoff.
"""

import json
import logging
import random
import signal
import time
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Message, Producer
from pymodbus.client import ModbusTcpClient

from telemetry.config import CollectorConfig, KafkaConfig
from telemetry.logging_setup import setup_logging
from telemetry.models import REGISTER_COUNT, SCHEMA_VERSION, decode_registers

log = logging.getLogger(__name__)


def build_payload(device_id: str, registers: list[int], read_at: datetime) -> dict:
    """Message body for one reading. Validation happens in the processor."""
    read_at = read_at.astimezone(UTC)
    ts = read_at.replace(microsecond=read_at.microsecond // 1000 * 1000)
    return {
        "schema_version": SCHEMA_VERSION,
        "device_id": device_id,
        "ts": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        **decode_registers(registers),
    }


def corrupt_payload(payload: dict) -> bytes:
    """Produce a broken message on purpose (FAULT_INJECTION_RATE) to exercise the DLQ."""
    payload = dict(payload)
    kind = random.choice(["bad_json", "out_of_range", "missing_field", "naive_ts"])
    if kind == "bad_json":
        return b'{"device_id": "' + payload["device_id"].encode() + b'", "ts": '
    if kind == "out_of_range":
        payload["frequency_hz"] = 0.0
    elif kind == "missing_field":
        payload.pop("power_w")
    else:
        payload["ts"] = payload["ts"].removesuffix("Z")
    return json.dumps(payload).encode()


class Collector:
    def __init__(self, cfg: CollectorConfig, kafka: KafkaConfig) -> None:
        self.cfg = cfg
        self.kafka = kafka
        self.running = True
        self.client = ModbusTcpClient(cfg.modbus_host, port=cfg.modbus_port, timeout=2)
        self.producer = Producer(
            {
                "bootstrap.servers": kafka.bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 50,
                "compression.type": "lz4",
                # Keep retrying for up to 10 minutes of broker unavailability.
                "message.timeout.ms": 600_000,
            }
        )
        self.sent = 0
        self.failed = 0

    def stop(self, *_: object) -> None:
        self.running = False

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed", extra={"fields": {"error": str(err), "topic": msg.topic()}})
        else:
            self.sent += 1

    def _ensure_connected(self) -> None:
        backoff = 1.0
        while self.running and not self.client.connected:
            if self.client.connect():
                log.info("connected to modbus", extra={"fields": {"host": self.cfg.modbus_host, "port": self.cfg.modbus_port}})
                return
            log.warning("modbus connect failed, retrying", extra={"fields": {"retry_in_s": backoff}})
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def _read_registers(self) -> list[int] | None:
        try:
            result = self.client.read_holding_registers(0, count=REGISTER_COUNT, device_id=self.cfg.modbus_device_id)
        except Exception:  # connection dropped mid-read
            log.exception("modbus read raised, reconnecting")
            self.client.close()
            return None
        if result.isError():
            log.warning("modbus error response", extra={"fields": {"result": str(result)}})
            return None
        return list(result.registers)

    def publish(self, payload: dict) -> None:
        value = json.dumps(payload).encode()
        if self.cfg.fault_injection_rate and random.random() < self.cfg.fault_injection_rate:
            value = corrupt_payload(payload)
            log.info("fault injected", extra={"fields": {"payload": value.decode(errors="replace")}})
        key = payload["device_id"].encode()
        while self.running:
            try:
                self.producer.produce(self.kafka.raw_topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                # Local queue is full (Kafka down for a long time): wait for deliveries to free space.
                log.warning("producer queue full, waiting", extra={"fields": {"queued": len(self.producer)}})
                self.producer.poll(1.0)
        self.producer.poll(0)  # serve delivery callbacks

    def run(self) -> None:
        log.info("collector started", extra={"fields": {"device_id": self.cfg.device_id, "topic": self.kafka.raw_topic}})
        # Schedule on a fixed grid (monotonic clock) so the poll rate does not drift.
        next_tick = time.monotonic()
        last_stats = time.monotonic()
        while self.running:
            self._ensure_connected()
            registers = self._read_registers()
            if registers is not None:
                self.publish(build_payload(self.cfg.device_id, registers, datetime.now(UTC)))

            if time.monotonic() - last_stats >= 30:
                log.info("collector stats", extra={"fields": {"delivered": self.sent, "failed": self.failed, "queued": len(self.producer)}})
                last_stats = time.monotonic()

            next_tick += self.cfg.poll_interval_s
            delay = next_tick - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:  # we fell behind (e.g. reconnect); skip missed ticks instead of bursting
                next_tick = time.monotonic()

        log.info("shutting down, flushing producer")
        remaining = self.producer.flush(10)
        if remaining:
            log.error("messages not delivered before shutdown", extra={"fields": {"lost": remaining}})
        self.client.close()


def main() -> None:
    setup_logging("collector")
    collector = Collector(CollectorConfig(), KafkaConfig())
    signal.signal(signal.SIGTERM, collector.stop)
    signal.signal(signal.SIGINT, collector.stop)
    collector.run()


if __name__ == "__main__":
    main()
