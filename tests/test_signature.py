from __future__ import annotations

import pytest

from hookrelay import signature

BODY = b'{"id":"evt_1","type":"invoice.paid"}'
NOW = 1_800_000_000


def test_roundtrip_accepts_valid_signature() -> None:
    header = signature.build_header("s3cret", BODY, NOW)
    assert signature.verify(["s3cret"], header, BODY, now=NOW + 10) == NOW


def test_rejects_tampered_body() -> None:
    header = signature.build_header("s3cret", BODY, NOW)
    with pytest.raises(signature.SignatureError, match="mismatch"):
        signature.verify(["s3cret"], header, BODY.replace(b"paid", b"void"), now=NOW)


def test_rejects_replayed_request_outside_tolerance() -> None:
    header = signature.build_header("s3cret", BODY, NOW)
    with pytest.raises(signature.SignatureError, match="tolerance"):
        signature.verify(["s3cret"], header, BODY, now=NOW + 301, tolerance=300)


def test_timestamp_is_bound_into_mac() -> None:
    """Swapping in a fresh timestamp must invalidate an old signature."""
    old = signature.build_header("s3cret", BODY, NOW)
    forged = old.replace(f"t={NOW}", f"t={NOW + 1000}")
    with pytest.raises(signature.SignatureError, match="mismatch"):
        signature.verify(["s3cret"], forged, BODY, now=NOW + 1000)


def test_accepts_previous_secret_during_rotation() -> None:
    header = signature.build_header("old-secret", BODY, NOW)
    signature.verify(["new-secret", "old-secret"], header, BODY, now=NOW)


@pytest.mark.parametrize("header", [None, "", "v1=abc", "t=notanint,v1=abc", f"t={NOW}"])
def test_rejects_malformed_headers(header: str | None) -> None:
    with pytest.raises(signature.SignatureError):
        signature.verify(["s3cret"], header, BODY, now=NOW)
