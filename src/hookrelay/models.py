"""Database schema."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from hookrelay.breaker import CLOSED, BreakerState
from hookrelay.db import utcnow


class Base(DeclarativeBase):
    pass


class InboundEvent(Base):
    """A raw webhook exactly as received. Dedupe key: (provider, event_id)."""

    __tablename__ = "inbound_events"
    __table_args__ = (UniqueConstraint("provider", "event_id", name="uq_inbound_provider_event"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(64))
    event_id: Mapped[str] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(255), default="")
    raw_body: Mapped[str] = mapped_column(Text)
    headers: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(16), default="received")
    received_at: Mapped[datetime] = mapped_column(default=utcnow)
    processed_at: Mapped[Optional[datetime]] = mapped_column(default=None)


class Endpoint(Base):
    """A subscriber URL plus its circuit-breaker state."""

    __tablename__ = "endpoints"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(String(2048))
    secret: Mapped[str] = mapped_column(String(255))
    event_types: Mapped[list[str]] = mapped_column(JSON, default=list)
    active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    breaker_state: Mapped[str] = mapped_column(String(16), default=CLOSED)
    breaker_failures: Mapped[int] = mapped_column(default=0)
    breaker_opened_at: Mapped[Optional[datetime]] = mapped_column(default=None)
    breaker_probe_started_at: Mapped[Optional[datetime]] = mapped_column(default=None)

    def subscribes_to(self, event_type: str) -> bool:
        return not self.event_types or event_type in self.event_types

    def get_breaker(self) -> BreakerState:
        return BreakerState(
            state=self.breaker_state or CLOSED,
            consecutive_failures=self.breaker_failures or 0,
            opened_at=self.breaker_opened_at,
            probe_started_at=self.breaker_probe_started_at,
        )

    def set_breaker(self, s: BreakerState) -> None:
        self.breaker_state = s.state
        self.breaker_failures = s.consecutive_failures
        self.breaker_opened_at = s.opened_at
        self.breaker_probe_started_at = s.probe_started_at


class Delivery(Base):
    """One event going to one endpoint. Unique per pair so fan-out is idempotent."""

    __tablename__ = "deliveries"
    __table_args__ = (
        UniqueConstraint("event_id", "endpoint_id", name="uq_delivery_event_endpoint"),
        Index("ix_deliveries_due", "status", "next_attempt_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("inbound_events.id"))
    endpoint_id: Mapped[int] = mapped_column(ForeignKey("endpoints.id"))
    status: Mapped[str] = mapped_column(String(16), default="pending")
    attempts: Mapped[int] = mapped_column(default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(default=utcnow)
    last_status_code: Mapped[Optional[int]] = mapped_column(default=None)
    last_error: Mapped[Optional[str]] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(default=None)

    event: Mapped[InboundEvent] = relationship()
    endpoint: Mapped[Endpoint] = relationship()


class DeadLetter(Base):
    """A delivery that will not be retried automatically any more."""

    __tablename__ = "dead_letters"

    id: Mapped[int] = mapped_column(primary_key=True)
    delivery_id: Mapped[int] = mapped_column(ForeignKey("deliveries.id"))
    reason: Mapped[str] = mapped_column(String(32))
    attempts: Mapped[int] = mapped_column()
    last_status_code: Mapped[Optional[int]] = mapped_column(default=None)
    last_error: Mapped[Optional[str]] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    replayed_at: Mapped[Optional[datetime]] = mapped_column(default=None)

    delivery: Mapped[Delivery] = relationship()
