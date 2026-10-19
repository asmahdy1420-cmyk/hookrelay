from __future__ import annotations

from datetime import datetime, timedelta

from hookrelay.breaker import CLOSED, HALF_OPEN, OPEN, BreakerState, CircuitBreaker

T0 = datetime(2026, 1, 1)
CB = CircuitBreaker(failure_threshold=3, reset_timeout=timedelta(seconds=60))


def fail_n(s: BreakerState, n: int) -> BreakerState:
    for _ in range(n):
        s = CB.on_failure(s, T0)
    return s


def test_opens_after_threshold_consecutive_failures() -> None:
    s = fail_n(BreakerState(), 2)
    assert s.state == CLOSED and CB.before_request(s, T0).allowed
    s = CB.on_failure(s, T0)
    assert s.state == OPEN


def test_success_resets_failure_count() -> None:
    s = CB.on_success(fail_n(BreakerState(), 2))
    assert fail_n(s, 2).state == CLOSED


def test_open_rejects_until_reset_timeout_then_allows_one_probe() -> None:
    s = fail_n(BreakerState(), 3)
    d = CB.before_request(s, T0 + timedelta(seconds=59))
    assert not d.allowed and d.retry_at == T0 + timedelta(seconds=60)

    probe = CB.before_request(s, T0 + timedelta(seconds=60))
    assert probe.allowed and probe.state.state == HALF_OPEN
    # A second caller while the probe is in flight is held back.
    assert not CB.before_request(probe.state, T0 + timedelta(seconds=61)).allowed


def test_half_open_probe_outcome_closes_or_reopens() -> None:
    half = CB.before_request(fail_n(BreakerState(), 3), T0 + timedelta(seconds=60)).state
    assert CB.on_success(half).state == CLOSED
    reopened = CB.on_failure(half, T0 + timedelta(seconds=61))
    assert reopened.state == OPEN and reopened.opened_at == T0 + timedelta(seconds=61)


def test_abandoned_probe_is_replaced_after_timeout() -> None:
    half = CB.before_request(fail_n(BreakerState(), 3), T0 + timedelta(seconds=60)).state
    later = CB.before_request(half, T0 + timedelta(seconds=121))
    assert later.allowed and later.state.probe_started_at == T0 + timedelta(seconds=121)
