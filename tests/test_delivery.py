from __future__ import annotations

import json
from datetime import timedelta

import httpx
from sqlalchemy import select

from hookrelay import tasks
from hookrelay.models import DeadLetter, Delivery, Endpoint, InboundEvent
from tests.conftest import ADMIN


def make_delivery(session_factory, endpoint, clock, event_id: str = "evt_1") -> int:
    with session_factory() as s:
        ev = InboundEvent(
            provider="acme",
            event_id=event_id,
            event_type="t",
            raw_body=json.dumps({"id": event_id}),
            status="processed",
        )
        s.add(ev)
        s.flush()
        d = Delivery(event_id=ev.id, endpoint_id=endpoint.id, next_attempt_at=clock())
        s.add(d)
        s.commit()
        return d.id


def load(session_factory, model, pk):
    with session_factory() as s:
        return s.get(model, pk)


def test_5xx_schedules_retry_with_backoff(session_factory, endpoint, receiver, clock) -> None:
    receiver.respond(503)
    did = make_delivery(session_factory, endpoint, clock)
    assert tasks.deliver(did) == "retry_scheduled"
    d = load(session_factory, Delivery, did)
    assert d.status == "pending" and d.attempts == 1 and d.last_status_code == 503
    assert clock() <= d.next_attempt_at <= clock() + timedelta(seconds=2)


def test_429_honours_retry_after(session_factory, endpoint, receiver, clock) -> None:
    receiver.respond(429, {"Retry-After": "120"})
    did = make_delivery(session_factory, endpoint, clock)
    tasks.deliver(did)
    assert load(session_factory, Delivery, did).next_attempt_at == clock() + timedelta(seconds=120)


def test_4xx_is_dead_lettered_without_retry(session_factory, endpoint, receiver, clock) -> None:
    receiver.respond(422)
    did = make_delivery(session_factory, endpoint, clock)
    assert tasks.deliver(did) == "dead"
    with session_factory() as s:
        dl = s.scalar(select(DeadLetter))
        assert dl.reason == "rejected" and dl.attempts == 1 and dl.last_status_code == 422
        # A 4xx means the endpoint is alive: the breaker is not charged.
        assert s.get(Endpoint, endpoint.id).breaker_failures == 0


def test_timeouts_retry_until_max_attempts_then_dead_letter(
    session_factory, endpoint, receiver, clock
) -> None:
    for _ in range(3):
        receiver.raise_(httpx.ReadTimeout("slow"))
    did = make_delivery(session_factory, endpoint, clock)
    outcomes = []
    for _ in range(3):
        outcomes.append(tasks.deliver(did))
        # Step past both the backoff and the breaker's reset timeout.
        clock.advance(seconds=3600)
    assert outcomes == ["retry_scheduled", "retry_scheduled", "dead"]
    with session_factory() as s:
        dl = s.scalar(select(DeadLetter))
        assert dl.reason == "max_attempts" and dl.attempts == 3
        assert "timeout" in dl.last_error


def test_breaker_opens_and_defers_without_calling_endpoint(
    session_factory, endpoint, receiver, clock
) -> None:
    receiver.respond(500)
    receiver.respond(500)
    a = make_delivery(session_factory, endpoint, clock, "evt_a")
    b = make_delivery(session_factory, endpoint, clock, "evt_b")
    c = make_delivery(session_factory, endpoint, clock, "evt_c")
    tasks.deliver(a)
    tasks.deliver(b)  # second consecutive failure: threshold 2 -> open
    assert load(session_factory, Endpoint, endpoint.id).breaker_state == "open"

    assert tasks.deliver(c) == "deferred"
    assert len(receiver.requests) == 2  # no network call while open
    deferred = load(session_factory, Delivery, c)
    assert deferred.attempts == 0
    assert deferred.next_attempt_at == clock() + timedelta(seconds=60)

    # After the reset timeout one probe goes through and closes the breaker.
    clock.advance(seconds=60)
    assert tasks.deliver(c) == "delivered"
    assert load(session_factory, Endpoint, endpoint.id).breaker_state == "closed"


def test_dispatch_due_redelivers_only_due_rows(session_factory, endpoint, receiver, clock) -> None:
    receiver.respond(503, {"Retry-After": "30"})
    did = make_delivery(session_factory, endpoint, clock)
    tasks.deliver(did)
    assert tasks.dispatch_due() == 0  # not due yet
    clock.advance(seconds=30)
    assert tasks.dispatch_due() == 1
    d = load(session_factory, Delivery, did)
    assert d.status == "delivered" and d.attempts == 2


def test_redelivered_task_is_idempotent(session_factory, endpoint, receiver, clock) -> None:
    did = make_delivery(session_factory, endpoint, clock)
    assert tasks.deliver(did) == "delivered"
    assert tasks.deliver(did) == "skipped"  # e.g. acks_late redelivery after a crash
    assert len(receiver.requests) == 1


def test_replay_dead_letter(client, session_factory, endpoint, receiver, clock) -> None:
    receiver.respond(410)
    did = make_delivery(session_factory, endpoint, clock)
    tasks.deliver(did)
    [dl] = client.get("/admin/dead-letters", headers=ADMIN).json()
    assert dl["delivery_id"] == did and dl["reason"] == "rejected"

    r = client.post(f"/admin/dead-letters/{dl['id']}/replay", headers=ADMIN)
    assert r.status_code == 202
    assert load(session_factory, Delivery, did).status == "delivered"
    assert client.get("/admin/dead-letters", headers=ADMIN).json() == []
    again = client.post(f"/admin/dead-letters/{dl['id']}/replay", headers=ADMIN)
    assert again.status_code == 409
