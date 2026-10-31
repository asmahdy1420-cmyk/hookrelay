from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select

from hookrelay import signature
from hookrelay.models import DeadLetter, Delivery, Endpoint, InboundEvent
from tests.conftest import ADMIN


def signed(payload: dict, secret: str = "whsec_current") -> tuple[bytes, dict]:
    body = json.dumps(payload).encode()
    return body, {
        "X-Hookrelay-Signature": signature.build_header(secret, body, int(time.time())),
        "Content-Type": "application/json",
    }


def test_valid_webhook_is_accepted_and_delivered(
    client, session_factory, endpoint, receiver, clock
) -> None:
    body, headers = signed({"id": "evt_1", "type": "invoice.paid", "amount": 42})
    resp = client.post("/webhooks/acme", content=body, headers=headers)
    assert resp.status_code == 202

    # Celery runs eagerly in tests: the fan-out and delivery already happened.
    assert len(receiver.requests) == 1
    sent = receiver.requests[0]
    assert sent.content == body
    assert sent.headers["X-Hookrelay-Event-Id"] == "evt_1"
    # The subscriber can verify our outbound signature with its own secret.
    signature.verify(
        ["endpoint-secret-0123456789"],
        sent.headers["X-Hookrelay-Signature"],
        sent.content,
        now=clock().replace(tzinfo=timezone.utc).timestamp(),
    )
    with session_factory() as s:
        assert s.scalar(select(Delivery.status)) == "delivered"


def test_bad_signature_is_rejected_and_not_stored(client, session_factory) -> None:
    body, headers = signed({"id": "evt_1"}, secret="wrong")
    assert client.post("/webhooks/acme", content=body, headers=headers).status_code == 401
    with session_factory() as s:
        assert s.scalar(select(func.count(InboundEvent.id))) == 0


def test_rotated_secret_is_accepted(client, endpoint, receiver, clock) -> None:
    body, headers = signed({"id": "evt_2"}, secret="whsec_previous")
    assert client.post("/webhooks/acme", content=body, headers=headers).status_code == 202


def test_duplicate_event_id_is_deduplicated(client, session_factory, endpoint, receiver, clock):
    body, headers = signed({"id": "evt_dup", "type": "x"})
    assert client.post("/webhooks/acme", content=body, headers=headers).status_code == 202
    second = client.post("/webhooks/acme", content=body, headers=headers)
    assert second.status_code == 200 and second.json()["status"] == "duplicate"
    assert len(receiver.requests) == 1
    with session_factory() as s:
        assert s.scalar(select(func.count(InboundEvent.id))) == 1


def test_unknown_provider_and_bad_payload(client) -> None:
    body, headers = signed({"id": "evt_1"})
    assert client.post("/webhooks/nope", content=body, headers=headers).status_code == 404
    body, headers = signed({"no_id": True})
    assert client.post("/webhooks/acme", content=body, headers=headers).status_code == 400


def test_endpoint_subscription_filter(client, session_factory, receiver, clock) -> None:
    r = client.post(
        "/endpoints",
        json={"url": "https://a.example/h", "secret": "x" * 16, "event_types": ["order.created"]},
        headers=ADMIN,
    )
    assert r.status_code == 201
    body, headers = signed({"id": "evt_3", "type": "order.refunded"})
    client.post("/webhooks/acme", content=body, headers=headers)
    assert receiver.requests == []


def test_admin_routes_require_token(client) -> None:
    assert client.get("/admin/dead-letters").status_code == 401
    assert client.get("/admin/dead-letters", headers={"X-Admin-Token": "nope"}).status_code == 401
    assert client.get("/admin/dead-letters", headers=ADMIN).json() == []


@pytest.mark.parametrize("limit", [0, -1, 501])
def test_dead_letter_limit_out_of_range_is_rejected(client, limit) -> None:
    resp = client.get(f"/admin/dead-letters?limit={limit}", headers=ADMIN)
    assert resp.status_code == 422


@pytest.mark.parametrize("limit", [1, 500])
def test_dead_letter_limit_boundaries_are_accepted(client, limit) -> None:
    resp = client.get(f"/admin/dead-letters?limit={limit}", headers=ADMIN)
    assert resp.status_code == 200


def test_dead_letter_listing_hides_replayed_unless_requested(
    client, session_factory, endpoint
) -> None:
    s = session_factory()
    ep_id = s.scalar(select(Endpoint.id))
    event = InboundEvent(provider="acme", event_id="evt_dl", raw_body="{}")
    s.add(event)
    s.flush()
    delivery = Delivery(event_id=event.id, endpoint_id=ep_id, status="dead")
    s.add(delivery)
    s.flush()
    s.add(DeadLetter(delivery_id=delivery.id, reason="max_attempts", attempts=3))
    replayed = DeadLetter(delivery_id=delivery.id, reason="max_attempts", attempts=3)
    replayed.replayed_at = datetime(2026, 1, 1)
    s.add(replayed)
    s.commit()
    s.close()
    pending = client.get("/admin/dead-letters", headers=ADMIN).json()
    assert len(pending) == 1 and pending[0]["replayed_at"] is None
    url = "/admin/dead-letters?include_replayed=true"
    assert len(client.get(url, headers=ADMIN).json()) == 2
    assert len(client.get(url + "&limit=1", headers=ADMIN).json()) == 1
