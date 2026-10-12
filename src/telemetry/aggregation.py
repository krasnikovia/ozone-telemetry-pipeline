"""
Decides when a device-minute is final (event-time tumbling windows).

The tracker does not compute statistics. It only answers "which minutes can be
aggregated now?". The aggregation itself is one SQL query over the raw table
(see db.py), and the durable list of minutes waiting for aggregation is the
`dirty_minutes` table. This in-memory state is only a cache of it and is
rebuilt from Postgres after a restart.

A minute [m, m+1min) of a device is closed when either
  - the device's watermark (max event time seen - allowed lateness) passed m+1min, or
  - the device has been silent for `idle_timeout_s` (wall clock), so we stop waiting.

A late event (its minute is already behind the watermark) simply re-opens its
minute, which is closed again on the next `close_ready()` call: the aggregate is
recomputed from raw and corrected. One code path for normal and late data.

Pure in-memory logic, no I/O, so it is easy to unit-test.
"""

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

ONE_MINUTE = timedelta(minutes=1)

DeviceMinute = tuple[str, datetime]


def minute_floor(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


@dataclass
class _DeviceState:
    open_minutes: set[datetime] = field(default_factory=set)
    max_event_ts: datetime | None = None
    last_seen: float = 0.0  # monotonic clock

    def watermark(self, allowed_lateness: timedelta) -> datetime | None:
        return self.max_event_ts - allowed_lateness if self.max_event_ts else None


class MinuteWindowTracker:
    def __init__(
        self,
        allowed_lateness: timedelta,
        idle_timeout_s: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.allowed_lateness = allowed_lateness
        self.idle_timeout_s = idle_timeout_s
        self.clock = clock
        self._devices: dict[str, _DeviceState] = {}

    def restore(self, device_id: str, minutes: Iterable[datetime]) -> None:
        """Re-open minutes that are still in `dirty_minutes` (after a restart)."""
        state = self._devices.setdefault(device_id, _DeviceState())
        state.open_minutes.update(minutes)
        state.last_seen = self.clock()

    def add(self, device_id: str, ts: datetime) -> bool:
        """Register an event. Returns True if it is late (its minute is already behind the watermark)."""
        state = self._devices.setdefault(device_id, _DeviceState())
        minute = minute_floor(ts)
        watermark = state.watermark(self.allowed_lateness)
        late = watermark is not None and minute + ONE_MINUTE <= watermark

        state.open_minutes.add(minute)
        state.last_seen = self.clock()
        if state.max_event_ts is None or ts > state.max_event_ts:
            state.max_event_ts = ts
        return late

    def close_ready(self) -> list[DeviceMinute]:
        """Return the minutes that became final and forget them."""
        closed: list[DeviceMinute] = []
        now = self.clock()
        for device_id, state in self._devices.items():
            if not state.open_minutes:
                continue
            idle = now - state.last_seen >= self.idle_timeout_s
            watermark = state.watermark(self.allowed_lateness)
            for minute in sorted(state.open_minutes):
                if not idle and (watermark is None or minute + ONE_MINUTE > watermark):
                    break  # sorted: later minutes are not ready either
                state.open_minutes.discard(minute)
                closed.append((device_id, minute))
        return closed

    @property
    def open_windows(self) -> int:
        return sum(len(s.open_minutes) for s in self._devices.values())
