from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator
from datetime import datetime

# Configure before hookrelay is imported (settings are cached).
os.environ.update(
    HOOKRELAY_DATABASE_URL="sqlite://",
    HOOKRELAY_CELERY_EAGER="true",
    HOOKRELAY_ADMIN_TOKEN="admin-test-token",
    HOOKRELAY_PROVIDER_SECRETS=json.dumps({"acme": ["whsec_current", "whsec_previous"]}),
    HOOKRELAY_MAX_ATTEMPTS="3",
    HOOKRELAY_BREAKER_THRESHOLD="2",
    HOOKRELAY_BREAKER_RESET_SECONDS="60",
)

import httpx
import pytest
from fastapi.testclient import TestClient

from hookrelay import db, tasks
from hookrelay.api import create_app
from hookrelay.models import Base, Endpoint

ADMIN = {"X-Admin-Token": "admin-test-token"}


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs: float) -> None:
        from datetime import timedelta

        self.now += timedelta(**kwargs)


class Receiver:
    """Scripted subscriber: responses are popped in order; requests recorded."""

    def __init__(self) -> None:
        self.script: list[Callable[[httpx.Request], httpx.Response]] = []
        self.requests: list[httpx.Request] = []

    def respond(self, status: int, headers: dict | None = None) -> None:
        self.script.append(lambda req: httpx.Response(status, headers=headers or {}))

    def raise_(self, exc: Exception) -> None:
        def _raise(req: httpx.Request) -> httpx.Response:
            raise exc

        self.script.append(_raise)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if not self.script:
            return httpx.Response(200)
        return self.script.pop(0)(request)


@pytest.fixture
def session_factory() -> Iterator:
    engine = db.configure("sqlite://")
    Base.metadata.create_all(engine)
    yield db.session_factory()
    Base.metadata.drop_all(engine)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    c = FakeClock(datetime(2026, 1, 1, 12, 0, 0))
    monkeypatch.setattr(tasks, "clock", c)
    return c


@pytest.fixture
def receiver() -> Iterator[Receiver]:
    r = Receiver()
    tasks.set_http_client(httpx.Client(transport=httpx.MockTransport(r.handler)))
    yield r
    tasks.set_http_client(None)


@pytest.fixture
def endpoint(session_factory) -> Endpoint:
    with session_factory() as s:
        ep = Endpoint(url="https://subscriber.example/hook", secret="endpoint-secret-0123456789")
        s.add(ep)
        s.commit()
        return ep


@pytest.fixture
def client(session_factory) -> Iterator[TestClient]:
    with TestClient(create_app(create_tables=False)) as c:
        yield c
