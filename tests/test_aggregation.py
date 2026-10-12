from datetime import UTC, datetime, timedelta

import pytest

from telemetry.aggregation import MinuteWindowTracker, minute_floor

T0 = datetime(2026, 10, 12, 12, 0, 0, tzinfo=UTC)
DEV = "sim-001"


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def windows(clock: FakeClock) -> MinuteWindowTracker:
    return MinuteWindowTracker(allowed_lateness=timedelta(seconds=10), idle_timeout_s=30, clock=clock)


def test_minute_floor():
    assert minute_floor(at(59.999)) == T0


def test_minute_stays_open_until_watermark_passes(windows):
    for s in range(60):
        windows.add(DEV, at(s))
    windows.add(DEV, at(65))  # watermark = 12:00:55 < 12:01:00
    assert windows.close_ready() == []

    windows.add(DEV, at(70))  # watermark = 12:01:00 -> minute 12:00 is final
    assert windows.close_ready() == [(DEV, T0)]
    assert windows.close_ready() == []  # reported only once


def test_out_of_order_event_within_lateness_is_not_late(windows):
    windows.add(DEV, at(30))
    windows.add(DEV, at(65))
    assert windows.add(DEV, at(59)) is False  # arrives after 12:01:05 but minute 12:00 is still open


def test_late_event_reopens_its_minute_and_it_closes_immediately(windows):
    windows.add(DEV, at(10))
    windows.add(DEV, at(80))
    assert windows.close_ready() == [(DEV, T0)]

    assert windows.add(DEV, at(20)) is True  # minute 12:00 is behind the watermark
    assert windows.close_ready() == [(DEV, T0)]  # recomputed right away


def test_several_minutes_close_in_order(windows):
    windows.add(DEV, at(10))
    windows.add(DEV, at(70))
    windows.add(DEV, at(130))
    windows.add(DEV, at(185))  # watermark 12:02:55 -> 12:00 and 12:01 are final
    assert windows.close_ready() == [(DEV, T0), (DEV, at(60))]


def test_idle_device_minutes_are_closed_by_wall_clock(windows, clock):
    windows.add(DEV, at(10))
    clock.now = 29
    assert windows.close_ready() == []
    clock.now = 31
    assert windows.close_ready() == [(DEV, T0)]


def test_devices_are_independent(windows):
    windows.add("a", at(10))
    windows.add("b", at(10))
    windows.add("a", at(80))
    assert windows.close_ready() == [("a", T0)]


def test_restored_dirty_minute_closes_when_events_continue(windows):
    # After a restart, Postgres says minute 12:00 still needs aggregation
    windows.restore(DEV, [T0])
    windows.add(DEV, at(130))
    assert windows.close_ready() == [(DEV, T0)]


def test_restored_dirty_minute_closes_when_no_new_events_arrive(windows, clock):
    windows.restore(DEV, [T0])
    clock.now = 31
    assert windows.close_ready() == [(DEV, T0)]


def test_old_event_after_restart_is_aggregated(windows, clock):
    # Regression: a fully aggregated, idle device gets an old event after a restart.
    # The tracker knows nothing about the device; the minute must still be aggregated.
    windows.add(DEV, at(5))
    assert windows.close_ready() == []
    clock.now = 31
    assert windows.close_ready() == [(DEV, T0)]
