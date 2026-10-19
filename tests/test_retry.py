from __future__ import annotations

import random
from datetime import datetime, timezone

import pytest

from hookrelay.retry import RetryPolicy, Verdict, classify_status, parse_retry_after


@pytest.mark.parametrize(
    ("code", "verdict"),
    [
        (200, Verdict.SUCCESS),
        (204, Verdict.SUCCESS),
        (500, Verdict.RETRY),
        (503, Verdict.RETRY),
        (429, Verdict.RETRY),
        (408, Verdict.RETRY),
        (400, Verdict.FAIL),
        (401, Verdict.FAIL),
        (404, Verdict.FAIL),
        (410, Verdict.FAIL),
        (301, Verdict.FAIL),
    ],
)
def test_classify_status(code: int, verdict: Verdict) -> None:
    assert classify_status(code) is verdict


def test_full_jitter_stays_within_exponential_ceiling_and_cap() -> None:
    policy = RetryPolicy(max_attempts=10, base_delay=2.0, max_delay=30.0)
    rng = random.Random(42)
    for attempt, ceiling in [(1, 2), (2, 4), (3, 8), (4, 16), (5, 30), (9, 30)]:
        samples = [policy.backoff(attempt, rng) for _ in range(200)]
        assert all(0 <= s <= ceiling for s in samples)
        # Full jitter really spreads values over the whole range.
        assert max(samples) - min(samples) > ceiling * 0.5


def test_retry_after_overrides_backoff_but_is_capped() -> None:
    policy = RetryPolicy(base_delay=2.0, max_delay=600.0)
    assert policy.next_delay(1, retry_after=120) == 120
    assert policy.next_delay(1, retry_after=86_400) == 600


def test_parse_retry_after_seconds_and_http_date() -> None:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert parse_retry_after("30") == 30.0
    assert parse_retry_after("Thu, 01 Jan 2026 12:01:30 GMT", now) == 90.0
    assert parse_retry_after("Thu, 01 Jan 2026 11:00:00 GMT", now) == 0.0
    assert parse_retry_after("soon") is None
    assert parse_retry_after(None) is None


def test_exhausted() -> None:
    policy = RetryPolicy(max_attempts=3)
    assert not policy.exhausted(2)
    assert policy.exhausted(3)
