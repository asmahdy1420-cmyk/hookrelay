"""Per-endpoint circuit breaker as a pure state machine.

State lives in the database (one row per endpoint), so the breaker works
across many worker processes. This module only computes transitions::

    CLOSED --(failures >= threshold)--> OPEN
    OPEN --(reset_timeout elapsed, next request)--> HALF_OPEN (one probe)
    HALF_OPEN --(probe succeeds)--> CLOSED
    HALF_OPEN --(probe fails)--> OPEN

While HALF_OPEN only one probe is in flight; other callers are told to come
back later. If a probe never reports (worker crashed), it is considered
abandoned after ``reset_timeout`` and a new probe is allowed.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Optional

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"


@dataclass(frozen=True)
class BreakerState:
    state: str = CLOSED
    consecutive_failures: int = 0
    opened_at: Optional[datetime] = None
    probe_started_at: Optional[datetime] = None


@dataclass(frozen=True)
class Decision:
    allowed: bool
    state: BreakerState
    retry_at: Optional[datetime] = None


@dataclass(frozen=True)
class CircuitBreaker:
    failure_threshold: int = 5
    reset_timeout: timedelta = timedelta(seconds=60)

    def before_request(self, s: BreakerState, now: datetime) -> Decision:
        if s.state == CLOSED:
            return Decision(True, s)

        if s.state == OPEN:
            assert s.opened_at is not None
            reopen_at = s.opened_at + self.reset_timeout
            if now < reopen_at:
                return Decision(False, s, retry_at=reopen_at)
            return Decision(True, replace(s, state=HALF_OPEN, probe_started_at=now))

        # HALF_OPEN: a probe is already out unless it has been abandoned.
        assert s.probe_started_at is not None
        probe_deadline = s.probe_started_at + self.reset_timeout
        if now < probe_deadline:
            return Decision(False, s, retry_at=probe_deadline)
        return Decision(True, replace(s, probe_started_at=now))

    def on_success(self, s: BreakerState) -> BreakerState:
        return BreakerState()

    def on_failure(self, s: BreakerState, now: datetime) -> BreakerState:
        failures = s.consecutive_failures + 1
        if s.state == HALF_OPEN or failures >= self.failure_threshold:
            return BreakerState(
                state=OPEN, consecutive_failures=failures, opened_at=now, probe_started_at=None
            )
        return replace(s, consecutive_failures=failures)
