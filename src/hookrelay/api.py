"""HTTP API: webhook ingestion, endpoint registry and dead-letter admin."""

from __future__ import annotations

import hmac
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from hookrelay import db, delivery, signature, tasks
from hookrelay.config import Settings, get_settings
from hookrelay.models import Base, DeadLetter, Endpoint, InboundEvent

log = logging.getLogger("hookrelay")

# Headers worth keeping for audit/debugging; never store auth material.
_KEEP_HEADERS = ("content-type", "user-agent", "x-hookrelay-signature")


# --------------------------------------------------------------------------- schemas


class EndpointIn(BaseModel):
    url: str = Field(pattern=r"^https?://")
    secret: str = Field(min_length=16)
    event_types: List[str] = Field(default_factory=list)


class EndpointOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    url: str
    event_types: List[str]
    active: bool
    breaker_state: str
    breaker_failures: int


class DeadLetterOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    delivery_id: int
    reason: str
    attempts: int
    last_status_code: Optional[int]
    last_error: Optional[str]
    created_at: datetime
    replayed_at: Optional[datetime]


# --------------------------------------------------------------------------- dependencies


def settings_dep() -> Settings:
    return get_settings()


def enqueue_inbound_dep() -> Callable[[int], Any]:
    return tasks.process_inbound.delay


def enqueue_delivery_dep() -> Callable[[int], Any]:
    return tasks.deliver.delay


def require_admin(
    x_admin_token: Optional[str] = Header(default=None),
    settings: Settings = Depends(settings_dep),
) -> None:
    if not x_admin_token or not hmac.compare_digest(x_admin_token, settings.admin_token):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid admin token")


# --------------------------------------------------------------------------- app


def create_app(create_tables: bool = True) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if create_tables:
            # Fine for a small service; a larger one would use Alembic.
            Base.metadata.create_all(db.get_engine())
        yield

    app = FastAPI(title="hookrelay", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/webhooks/{provider}", status_code=status.HTTP_202_ACCEPTED)
    async def receive_webhook(
        provider: str,
        request: Request,
        session: Session = Depends(db.get_session),
        settings: Settings = Depends(settings_dep),
        enqueue: Callable[[int], Any] = Depends(enqueue_inbound_dep),
    ) -> JSONResponse:
        secrets = settings.provider_secrets.get(provider)
        if not secrets:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown provider")

        body = await request.body()
        try:
            signature.verify(
                secrets,
                request.headers.get("x-hookrelay-signature"),
                body,
                tolerance=settings.signature_tolerance,
            )
        except signature.SignatureError as exc:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc)) from exc

        try:
            event_id, event_type = delivery.parse_event(body)
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

        event = InboundEvent(
            provider=provider,
            event_id=event_id,
            event_type=event_type,
            raw_body=body.decode(),
            headers={k: v for k, v in request.headers.items() if k in _KEEP_HEADERS},
        )
        session.add(event)
        try:
            session.commit()
        except IntegrityError:
            # Unique (provider, event_id): the database decides, not a
            # read-then-write check that two concurrent requests could race.
            session.rollback()
            return JSONResponse({"status": "duplicate", "event_id": event_id}, status_code=200)

        try:
            enqueue(event.id)
        except Exception:  # broker down: the event is safe, dispatch_due will pick it up
            log.exception("enqueue failed for inbound event %s", event.id)
        return JSONResponse({"status": "accepted", "id": event.id}, status_code=202)

    @app.post(
        "/endpoints",
        response_model=EndpointOut,
        status_code=201,
        dependencies=[Depends(require_admin)],
    )
    def create_endpoint(data: EndpointIn, session: Session = Depends(db.get_session)) -> Endpoint:
        ep = Endpoint(url=data.url, secret=data.secret, event_types=data.event_types)
        session.add(ep)
        session.commit()
        return ep

    @app.get("/endpoints", response_model=List[EndpointOut], dependencies=[Depends(require_admin)])
    def list_endpoints(session: Session = Depends(db.get_session)) -> list:
        return list(session.scalars(select(Endpoint).order_by(Endpoint.id)))

    @app.get(
        "/admin/dead-letters",
        response_model=List[DeadLetterOut],
        dependencies=[Depends(require_admin)],
    )
    def list_dead_letters(
        include_replayed: bool = False,
        limit: int = 50,
        session: Session = Depends(db.get_session),
    ) -> list:
        q = select(DeadLetter).order_by(DeadLetter.id.desc()).limit(min(limit, 500))
        if not include_replayed:
            q = q.where(DeadLetter.replayed_at.is_(None))
        return list(session.scalars(q))

    @app.post(
        "/admin/dead-letters/{dead_letter_id}/replay",
        status_code=202,
        dependencies=[Depends(require_admin)],
    )
    def replay(
        dead_letter_id: int,
        enqueue: Callable[[int], Any] = Depends(enqueue_delivery_dep),
    ) -> dict:
        delivery_id = delivery.replay_dead_letter(db.session_factory(), dead_letter_id, db.utcnow())
        if delivery_id is None:
            raise HTTPException(status.HTTP_409_CONFLICT, "not found or already replayed")
        enqueue(delivery_id)
        return {"status": "replaying", "delivery_id": delivery_id}

    return app


app = create_app()
