"""Engine and session factory."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from hookrelay.config import get_settings

_engine: Optional[Engine] = None
_sessionmaker: Optional[sessionmaker[Session]] = None


def utcnow() -> datetime:
    """Naive UTC timestamp. All stored datetimes are naive UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def configure(url: str) -> Engine:
    global _engine, _sessionmaker
    kwargs: dict = {"future": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
        if ":memory:" in url or url == "sqlite://":
            kwargs["poolclass"] = StaticPool
    else:
        kwargs["pool_pre_ping"] = True
    _engine = create_engine(url, **kwargs)
    _sessionmaker = sessionmaker(bind=_engine, expire_on_commit=False)
    return _engine


def get_engine() -> Engine:
    if _engine is None:
        configure(get_settings().database_url)
    assert _engine is not None
    return _engine


def session_factory() -> sessionmaker[Session]:
    get_engine()
    assert _sessionmaker is not None
    return _sessionmaker


def get_session() -> Iterator[Session]:
    """FastAPI dependency."""
    with session_factory()() as session:
        yield session
