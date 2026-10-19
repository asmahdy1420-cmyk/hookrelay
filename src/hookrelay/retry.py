"""Retry policy: response classification, exponential backoff with full jitter.

Pure functions only — no I/O, no clock reads unless a clock is passed in —
so every branch is unit-testable.
"""

from __future__ import annotations

import enum
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

# 408 Request Timeout, 425 Too Early and 429 Too Many Requests are 4xx codes
# that describe a transient condition, so they are retried like 5xx.
RETRYABLE_4XX = frozenset({408, 425, 429})


class Verdict(str, enum.Enum):
    SUCCESS = "success"
    RETRY = "retry"
    FAIL = "fail"


def classify_status(status_code: int) -> Verdict:
    if 200 <= status_code < 300:
        return Verdict.SUCCESS
    if status_code >= 500 or status_code in RETRYABLE_4XX:
        return Verdict.RETRY
    # 3xx (redirects are not followed) and remaining 4xx: the receiver
    # understood us and said no. Retrying the same request will not help.
    return Verdict.FAIL


def parse_retry_after(value: Optional[str], now: Optional[datetime] = None) -> Optional[float]:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) into seconds."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(0.0, (when - current).total_seconds())


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 8
    base_delay: float = 2.0
    max_delay: float = 3600.0

    def backoff(self, attempt: int, rng: Optional[random.Random] = None) -> float:
        """Full-jitter delay before retry number ``attempt`` (1-based).

        ``uniform(0, min(cap, base * 2**(attempt-1)))`` — spreads retries
        from many failed deliveries so they do not hit a recovering
        endpoint in synchronised waves.
        """
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        ceiling = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
        return (rng or random).uniform(0, ceiling)

    def next_delay(
        self,
        attempt: int,
        retry_after: Optional[float] = None,
        rng: Optional[random.Random] = None,
    ) -> float:
        """Delay to wait after ``attempt`` failed; honours ``Retry-After``."""
        if retry_after is not None:
            return min(self.max_delay, max(0.0, retry_after))
        return self.backoff(attempt, rng)

    def exhausted(self, attempts_made: int) -> bool:
        return attempts_made >= self.max_attempts
