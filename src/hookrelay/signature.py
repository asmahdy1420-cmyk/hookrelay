"""HMAC-SHA256 webhook signatures with timestamp-based replay protection.

Header format (Stripe-style)::

    X-Hookrelay-Signature: t=1700000000,v1=<hex hmac>

The signed payload is ``f"{t}.{raw_body}"``. Binding the timestamp into the
MAC means an attacker cannot reuse an old signature with a fresh timestamp,
and the tolerance window bounds how long a captured request stays valid.
Several ``v1`` entries are accepted so a provider can rotate secrets.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional

SCHEME = "v1"
DEFAULT_TOLERANCE_SECONDS = 300


class SignatureError(Exception):
    """Raised when a signature header is missing, malformed, stale or wrong."""


@dataclass(frozen=True)
class ParsedHeader:
    timestamp: int
    signatures: tuple[str, ...]


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    message = str(timestamp).encode() + b"." + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def build_header(secret: str, body: bytes, timestamp: Optional[int] = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    return f"t={ts},{SCHEME}={compute_signature(secret, ts, body)}"


def parse_header(header: Optional[str]) -> ParsedHeader:
    if not header:
        raise SignatureError("missing signature header")
    timestamp: Optional[int] = None
    signatures: list[str] = []
    for part in header.split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            continue
        if key == "t":
            try:
                timestamp = int(value)
            except ValueError as exc:
                raise SignatureError("invalid timestamp") from exc
        elif key == SCHEME:
            signatures.append(value)
    if timestamp is None:
        raise SignatureError("timestamp missing from signature header")
    if not signatures:
        raise SignatureError(f"no {SCHEME} signature in header")
    return ParsedHeader(timestamp=timestamp, signatures=tuple(signatures))


def verify(
    secrets: Iterable[str],
    header: Optional[str],
    body: bytes,
    *,
    now: Optional[float] = None,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
) -> int:
    """Verify ``header`` against ``body``; return the signed timestamp.

    ``secrets`` is an iterable so the current and previous secret can both be
    accepted during rotation. Comparison is constant-time.
    """
    parsed = parse_header(header)
    current = time.time() if now is None else now
    if abs(current - parsed.timestamp) > tolerance:
        raise SignatureError("timestamp outside tolerance window")
    for secret in secrets:
        expected = compute_signature(secret, parsed.timestamp, body)
        if any(hmac.compare_digest(expected, sig) for sig in parsed.signatures):
            return parsed.timestamp
    raise SignatureError("signature mismatch")
