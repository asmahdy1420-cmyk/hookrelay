"""Settings read from environment variables."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from functools import lru_cache


def _bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _secrets(value: str) -> dict[str, list[str]]:
    """``{"acme": "s1"}`` or ``{"acme": ["new", "old"]}`` -> provider -> secrets."""
    raw = json.loads(value) if value else {}
    return {k: [v] if isinstance(v, str) else list(v) for k, v in raw.items()}


@dataclass
class Settings:
    database_url: str = "sqlite:///./hookrelay.db"
    redis_url: str = "redis://localhost:6379/0"
    admin_token: str = "change-me"
    provider_secrets: dict[str, list[str]] = field(default_factory=dict)
    signature_tolerance: int = 300
    delivery_timeout: float = 10.0
    max_attempts: int = 8
    base_delay: float = 2.0
    max_delay: float = 3600.0
    breaker_threshold: int = 5
    breaker_reset_seconds: float = 60.0
    dispatch_interval: float = 5.0
    celery_eager: bool = False

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get
        return cls(
            database_url=env("HOOKRELAY_DATABASE_URL", cls.database_url),
            redis_url=env("HOOKRELAY_REDIS_URL", cls.redis_url),
            admin_token=env("HOOKRELAY_ADMIN_TOKEN", cls.admin_token),
            provider_secrets=_secrets(env("HOOKRELAY_PROVIDER_SECRETS", "")),
            signature_tolerance=int(env("HOOKRELAY_SIGNATURE_TOLERANCE", "300")),
            delivery_timeout=float(env("HOOKRELAY_DELIVERY_TIMEOUT", "10")),
            max_attempts=int(env("HOOKRELAY_MAX_ATTEMPTS", "8")),
            base_delay=float(env("HOOKRELAY_BASE_DELAY", "2")),
            max_delay=float(env("HOOKRELAY_MAX_DELAY", "3600")),
            breaker_threshold=int(env("HOOKRELAY_BREAKER_THRESHOLD", "5")),
            breaker_reset_seconds=float(env("HOOKRELAY_BREAKER_RESET_SECONDS", "60")),
            dispatch_interval=float(env("HOOKRELAY_DISPATCH_INTERVAL", "5")),
            celery_eager=_bool(env("HOOKRELAY_CELERY_EAGER", "false")),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()
