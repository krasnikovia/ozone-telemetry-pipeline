import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from telemetry.collector import build_payload
from telemetry.models import InvalidEvent, decode_registers, parse_event

NOW = datetime(2026, 10, 12, 12, 0, 0, tzinfo=UTC)
SKEW = timedelta(minutes=5)


def payload(**overrides) -> bytes:
    data = {
        "schema_version": 1,
        "device_id": "sim-001",
        "ts": "2026-10-12T11:59:30.123Z",
        "temperature_c": 25.1,
        "pressure_bar": 10.2,
        "power_w": 1012.0,
        "frequency_hz": 50.02,
        "status": 0,
    }
    data.update(overrides)
    return json.dumps(data).encode()


def test_decode_registers_scales_values():
    assert decode_registers([251, 102, 1012, 5002, 1]) == {
        "temperature_c": 25.1,
        "pressure_bar": 10.2,
        "power_w": 1012.0,
        "frequency_hz": 50.02,
        "status": 1,
    }


def test_decode_registers_negative_temperature_is_twos_complement():
    # -5.0 C -> -50 -> 65486 as an unsigned 16-bit register
    assert decode_registers([65486, 100, 0, 5000, 0])["temperature_c"] == -5.0


def test_decode_registers_rejects_wrong_length():
    with pytest.raises(ValueError):
        decode_registers([1, 2, 3])


def test_build_payload_truncates_to_milliseconds_and_uses_utc():
    payload = build_payload("sim-001", [251, 102, 1012, 5002, 0], datetime(2026, 10, 12, 14, 0, 0, 123456, tzinfo=timezone(timedelta(hours=2))))
    assert payload["ts"] == "2026-10-12T12:00:00.123Z"
    assert parse_event(json.dumps(payload).encode(), NOW, SKEW).power_w == 1012.0


def test_collector_publishes_impossible_sensor_values_instead_of_crashing():
    # frequency register 0 -> 0.0 Hz: the collector must not fail; the processor sends it to the DLQ
    payload = build_payload("sim-001", [251, 102, 1012, 0, 0], NOW)
    with pytest.raises(InvalidEvent):
        parse_event(json.dumps(payload).encode(), NOW, SKEW)


def test_parse_truncates_microseconds_to_milliseconds():
    event = parse_event(payload(ts="2026-10-12T11:59:30.123456Z"), NOW, SKEW)
    assert event.ts.microsecond == 123000


def test_parse_valid_event():
    event = parse_event(payload(), NOW, SKEW)
    assert event.ts == datetime(2026, 10, 12, 11, 59, 30, 123000, tzinfo=UTC)
    assert event.power_w == 1012.0


def test_parse_converts_offset_to_utc():
    event = parse_event(payload(ts="2026-10-12T13:59:30+02:00"), NOW, SKEW)
    assert event.ts == datetime(2026, 10, 12, 11, 59, 30, tzinfo=UTC)
    assert event.ts.utcoffset() == timezone.utc.utcoffset(None)


@pytest.mark.parametrize(
    ("raw", "error_type"),
    [
        (b"", "empty_payload"),
        (b'{"device_id": "sim-001", "ts": ', "invalid_json"),
        (payload(frequency_hz=0.0), "schema_violation"),  # physically impossible
        (payload(power_w=-5), "schema_violation"),
        (payload(status=7), "schema_violation"),
        (payload(ts="2026-10-12T11:59:30"), "schema_violation"),  # naive datetime
        (payload(ts="2026-10-12T13:00:00Z"), "schema_violation"),  # 1h in the future
        (payload(unexpected="x"), "schema_violation"),
        (payload(schema_version=2), "schema_violation"),
    ],
)
def test_parse_rejects_poison_messages(raw, error_type):
    with pytest.raises(InvalidEvent) as exc_info:
        parse_event(raw, NOW, SKEW)
    assert exc_info.value.error_type == error_type


def test_parse_rejects_missing_field():
    data = json.loads(payload())
    del data["power_w"]
    with pytest.raises(InvalidEvent, match="power_w"):
        parse_event(json.dumps(data).encode(), NOW, SKEW)


def test_parse_rejects_nan():
    raw = payload().replace(b"1012.0", b"NaN")
    with pytest.raises(InvalidEvent):
        parse_event(raw, NOW, SKEW)
