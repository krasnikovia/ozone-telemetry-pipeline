"""
Data contract of the pipeline.

The Modbus register layout comes from the upstream simulator:

    0: temperature  INT16  / 10   (deg C)
    1: pressure     UINT16 / 10   (bar)
    2: power        UINT16        (W)
    3: frequency    UINT16 / 100  (Hz)
    4: status       UINT16        (0 = OK, 1 = warning)

`TelemetryEvent` is the message on the Kafka `telemetry.raw` topic (JSON).
Validation bounds are "physically plausible" limits: anything outside them is
a broken sensor or a broken message and goes to the DLQ instead of polluting
aggregates.
"""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, ValidationInfo, field_validator

SCHEMA_VERSION = 1
REGISTER_COUNT = 5
NOMINAL_FREQUENCY_HZ = 50.0


def decode_registers(registers: Sequence[int]) -> dict[str, float | int]:
    """Convert raw 16-bit holding registers into engineering units."""
    if len(registers) != REGISTER_COUNT:
        raise ValueError(f"expected {REGISTER_COUNT} registers, got {len(registers)}")

    temperature_raw = registers[0]
    if temperature_raw >= 32768:  # two's complement INT16
        temperature_raw -= 65536

    return {
        "temperature_c": temperature_raw / 10.0,
        "pressure_bar": registers[1] / 10.0,
        "power_w": float(registers[2]),
        "frequency_hz": registers[3] / 100.0,
        "status": int(registers[4]),
    }


class TelemetryEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal[1]
    device_id: str = Field(min_length=1, max_length=64)
    # Event time: when the collector read the registers. Must carry a timezone.
    ts: AwareDatetime
    temperature_c: float = Field(ge=-40, le=120)
    pressure_bar: float = Field(ge=0, le=100)
    power_w: float = Field(ge=0, le=65535)
    frequency_hz: float = Field(ge=45, le=55)
    status: Literal[0, 1]

    @field_validator("ts")
    @classmethod
    def _normalise_ts(cls, value: datetime, info: ValidationInfo) -> datetime:
        # UTC, millisecond precision (the contract; the rolling-window SQL relies on it).
        value = value.astimezone(UTC)
        value = value.replace(microsecond=value.microsecond // 1000 * 1000)
        context = info.context or {}
        now: datetime | None = context.get("now")
        max_skew: timedelta | None = context.get("max_clock_skew")
        if now is not None and max_skew is not None and value > now + max_skew:
            raise ValueError(f"event time {value.isoformat()} is in the future (device clock skew?)")
        return value

    def to_json(self) -> bytes:
        return self.model_dump_json().encode()


class InvalidEvent(Exception):
    """A message that can never be processed (poison message) -> DLQ."""

    def __init__(self, error_type: str, message: str) -> None:
        super().__init__(message)
        self.error_type = error_type


def parse_event(raw: bytes | None, now: datetime, max_clock_skew: timedelta) -> TelemetryEvent:
    if not raw:
        raise InvalidEvent("empty_payload", "message has no value")
    try:
        return TelemetryEvent.model_validate_json(raw, context={"now": now, "max_clock_skew": max_clock_skew})
    except ValidationError as exc:
        kinds = {err["type"] for err in exc.errors()}
        error_type = "invalid_json" if "json_invalid" in kinds else "schema_violation"
        # Compact, human-readable reason that also lands in the DLQ headers.
        reason = "; ".join(f"{'.'.join(map(str, e['loc'])) or '<root>'}: {e['msg']}" for e in exc.errors())
        raise InvalidEvent(error_type, reason) from exc
