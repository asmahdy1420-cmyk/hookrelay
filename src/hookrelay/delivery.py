"""Inbound processing, fan-out and outbound delivery attempts.

Plain functions taking a session factory, an HTTP client and a clock, so the
Celery tasks are thin wrappers and everything here is testable without a
broker. Each attempt uses two short transactions around the HTTP call; no
database lock is held while waiting on the network.
"""

from __future__ import annotations

import json
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hookrelay import signature
from hookrelay.breaker import CircuitBreaker
from hookrelay.config import Settings
from hookrelay.models import DeadLetter, Delivery, Endpoint, InboundEvent
from hookrelay.retry import RetryPolicy, Verdict, classify_status, parse_retry_after

PENDING = "pending"
DELIVERED = "delivered"
DEAD = "dead"

# Pushed into the future while an attempt is in flight so the dispatcher
# does not hand the same delivery to a second worker.
LEASE_MARGIN = timedelta(seconds=30)


@dataclass(frozen=True)
class AttemptResult:
    outcome: str  # delivered | retry_scheduled | dead | deferred | skipped
    status_code: Optional[int] = None
    next_attempt_at: Optional[datetime] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class Delivered:
    verdict: Verdict
    status_code: Optional[int]
    retry_after: Optional[float]
    error: Optional[str]


def policy_from(settings: Settings) -> RetryPolicy:
    return RetryPolicy(settings.max_attempts, settings.base_delay, settings.max_delay)


def breaker_from(settings: Settings) -> CircuitBreaker:
    return CircuitBreaker(
        settings.breaker_threshold, timedelta(seconds=settings.breaker_reset_seconds)
    )


# --------------------------------------------------------------------------- inbound


def process_inbound_event(
    factory: sessionmaker[Session], inbound_id: int, now: datetime
) -> list[int]:
    """Fan an inbound event out to matching endpoints; return new delivery ids.

    Idempotent: the (event, endpoint) unique constraint plus the status check
    mean a redelivered task creates nothing twice.
    """
    with factory() as session, session.begin():
        event = session.get(InboundEvent, inbound_id, with_for_update=True)
        if event is None or event.status != "received":
            return []
        existing = set(
            session.scalars(select(Delivery.endpoint_id).where(Delivery.event_id == event.id))
        )
        created: list[Delivery] = []
        for endpoint in session.scalars(select(Endpoint).where(Endpoint.active.is_(True))):
            if endpoint.id in existing or not endpoint.subscribes_to(event.event_type):
                continue
            d = Delivery(event_id=event.id, endpoint_id=endpoint.id, next_attempt_at=now)
            session.add(d)
            created.append(d)
        event.status = "processed"
        event.processed_at = now
        session.flush()
        return [d.id for d in created]


# --------------------------------------------------------------------------- outbound


def send(
    client: httpx.Client,
    endpoint: Endpoint,
    event: InboundEvent,
    attempt: int,
    timeout: float,
    now: datetime,
) -> Delivered:
    body = event.raw_body.encode()
    headers = {
        "Content-Type": "application/json",
        "X-Hookrelay-Event-Id": event.event_id,
        "X-Hookrelay-Event-Type": event.event_type,
        "X-Hookrelay-Attempt": str(attempt),
        "X-Hookrelay-Signature": signature.build_header(
            endpoint.secret, body, int(now.replace(tzinfo=timezone.utc).timestamp())
        ),
    }
    try:
        resp = client.post(endpoint.url, content=body, headers=headers, timeout=timeout)
    except httpx.TimeoutException as exc:
        return Delivered(Verdict.RETRY, None, None, f"timeout: {exc.__class__.__name__}")
    except httpx.TransportError as exc:
        return Delivered(Verdict.RETRY, None, None, f"transport: {exc.__class__.__name__}")
    retry_after = parse_retry_after(resp.headers.get("Retry-After"))
    error = None if resp.is_success else resp.text[:500]
    return Delivered(classify_status(resp.status_code), resp.status_code, retry_after, error)


def _dead_letter(session: Session, d: Delivery, reason: str) -> None:
    d.status = DEAD
    session.add(
        DeadLetter(
            delivery_id=d.id,
            reason=reason,
            attempts=d.attempts,
            last_status_code=d.last_status_code,
            last_error=d.last_error,
        )
    )


def attempt_delivery(
    factory: sessionmaker[Session],
    delivery_id: int,
    client: httpx.Client,
    settings: Settings,
    clock: Callable[[], datetime],
    rng: Optional[random.Random] = None,
) -> AttemptResult:
    policy, breaker = policy_from(settings), breaker_from(settings)

    # Transaction 1: claim the delivery and ask the breaker for permission.
    now = clock()
    with factory() as session, session.begin():
        d = session.get(Delivery, delivery_id, with_for_update=True)
        if d is None or d.status != PENDING:
            return AttemptResult("skipped")
        endpoint = session.get(Endpoint, d.endpoint_id, with_for_update=True)
        assert endpoint is not None
        decision = breaker.before_request(endpoint.get_breaker(), now)
        endpoint.set_breaker(decision.state)
        if not decision.allowed:
            # Not an attempt: the endpoint is known-bad, wait for the breaker.
            d.next_attempt_at = decision.retry_at or now
            return AttemptResult("deferred", next_attempt_at=d.next_attempt_at)
        attempt = d.attempts + 1
        d.next_attempt_at = now + timedelta(seconds=settings.delivery_timeout) + LEASE_MARGIN
        event = session.get(InboundEvent, d.event_id)
        assert event is not None
        target = endpoint
        session.expunge_all()

    # No transaction open during the network call.
    result = send(client, target, event, attempt, settings.delivery_timeout, now)

    # Transaction 2: record the outcome.
    now = clock()
    with factory() as session, session.begin():
        d = session.get(Delivery, delivery_id, with_for_update=True)
        endpoint = session.get(Endpoint, target.id, with_for_update=True)
        assert d is not None and endpoint is not None
        d.attempts = attempt
        d.last_status_code = result.status_code
        d.last_error = result.error

        if result.verdict is Verdict.SUCCESS:
            endpoint.set_breaker(breaker.on_success(endpoint.get_breaker()))
            d.status, d.delivered_at = DELIVERED, now
            return AttemptResult("delivered", result.status_code)

        if result.verdict is Verdict.FAIL:
            # A 4xx proves the endpoint is up; it does not count against it.
            endpoint.set_breaker(breaker.on_success(endpoint.get_breaker()))
            _dead_letter(session, d, "rejected")
            return AttemptResult("dead", result.status_code, error=result.error)

        endpoint.set_breaker(breaker.on_failure(endpoint.get_breaker(), now))
        if policy.exhausted(attempt):
            _dead_letter(session, d, "max_attempts")
            return AttemptResult("dead", result.status_code, error=result.error)
        delay = policy.next_delay(attempt, result.retry_after, rng)
        d.next_attempt_at = now + timedelta(seconds=delay)
        return AttemptResult("retry_scheduled", result.status_code, d.next_attempt_at, result.error)


def claim_due(
    factory: sessionmaker[Session], now: datetime, lease: timedelta, limit: int = 100
) -> list[int]:
    """Pick pending deliveries whose time has come. ``SKIP LOCKED`` lets
    several dispatchers run without handing out the same row twice."""
    with factory() as session, session.begin():
        rows = list(
            session.scalars(
                select(Delivery)
                .where(Delivery.status == PENDING, Delivery.next_attempt_at <= now)
                .order_by(Delivery.next_attempt_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        for d in rows:
            d.next_attempt_at = now + lease
        return [d.id for d in rows]


def replay_dead_letter(
    factory: sessionmaker[Session], dead_letter_id: int, now: datetime
) -> Optional[int]:
    """Reset the underlying delivery for a fresh run. Returns its id, or
    ``None`` if the dead letter does not exist or was already replayed."""
    with factory() as session, session.begin():
        dl = session.get(DeadLetter, dead_letter_id, with_for_update=True)
        if dl is None or dl.replayed_at is not None:
            return None
        d = session.get(Delivery, dl.delivery_id, with_for_update=True)
        assert d is not None
        dl.replayed_at = now
        d.status, d.attempts, d.next_attempt_at = PENDING, 0, now
        d.last_error = d.last_status_code = None
        return d.id


def parse_event(body: bytes) -> tuple[str, str]:
    """Extract (event_id, event_type) from a JSON body; raise ValueError."""
    data = json.loads(body)
    if not isinstance(data, dict):
        raise ValueError("payload must be a JSON object")
    event_id = data.get("id")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("payload needs a string 'id'")
    event_type = data.get("type")
    return event_id, event_type if isinstance(event_type, str) else ""
