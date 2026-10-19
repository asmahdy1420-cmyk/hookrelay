"""Celery wiring. Tasks are thin: the logic lives in ``hookrelay.delivery``.

Retries are *not* scheduled with ``countdown``/ETA. With a Redis broker an
ETA task sits in a worker's memory and is redelivered after the visibility
timeout, which makes long backoffs unreliable. Instead each delivery stores
``next_attempt_at`` and a beat task (``dispatch_due``) claims due rows with
``SELECT ... FOR UPDATE SKIP LOCKED``. The database is the source of truth.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Optional

import httpx
from celery import Celery
from sqlalchemy import select

from hookrelay import db, delivery
from hookrelay.config import Settings, get_settings
from hookrelay.models import InboundEvent

celery_app = Celery("hookrelay")

# Swappable for tests.
clock: Callable[[], datetime] = db.utcnow
_http_client: Optional[httpx.Client] = None

STALE_INBOUND_AFTER = timedelta(minutes=1)


def configure_celery(settings: Settings) -> None:
    celery_app.conf.update(
        broker_url=settings.redis_url,
        result_backend=None,
        task_ignore_result=True,
        # Ack after the task finishes; if the worker dies mid-task the message
        # goes back to the queue. Tasks are idempotent, so a rerun is safe.
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        task_always_eager=settings.celery_eager,
        task_eager_propagates=True,
        task_serializer="json",
        accept_content=["json"],
        beat_schedule={
            "dispatch-due-deliveries": {
                "task": "hookrelay.dispatch_due",
                "schedule": settings.dispatch_interval,
            }
        },
    )


configure_celery(get_settings())


def set_http_client(client: Optional[httpx.Client]) -> None:
    global _http_client
    _http_client = client


def http_client() -> httpx.Client:
    global _http_client
    if _http_client is None:
        _http_client = httpx.Client(follow_redirects=False)
    return _http_client


@celery_app.task(name="hookrelay.process_inbound")
def process_inbound(inbound_id: int) -> list[int]:
    ids = delivery.process_inbound_event(db.session_factory(), inbound_id, clock())
    for delivery_id in ids:
        deliver.delay(delivery_id)
    return ids


@celery_app.task(name="hookrelay.deliver")
def deliver(delivery_id: int) -> str:
    result = delivery.attempt_delivery(
        db.session_factory(), delivery_id, http_client(), get_settings(), clock
    )
    return result.outcome


@celery_app.task(name="hookrelay.dispatch_due")
def dispatch_due() -> int:
    settings = get_settings()
    now = clock()
    factory = db.session_factory()
    lease = timedelta(seconds=settings.delivery_timeout) + delivery.LEASE_MARGIN
    ids = delivery.claim_due(factory, now, lease)
    for delivery_id in ids:
        deliver.delay(delivery_id)

    # Safety net: an event committed but never enqueued (broker blip).
    with factory() as session:
        stale = list(
            session.scalars(
                select(InboundEvent.id)
                .where(
                    InboundEvent.status == "received",
                    InboundEvent.received_at < now - STALE_INBOUND_AFTER,
                )
                .limit(100)
            )
        )
    for inbound_id in stale:
        process_inbound.delay(inbound_id)
    return len(ids)
