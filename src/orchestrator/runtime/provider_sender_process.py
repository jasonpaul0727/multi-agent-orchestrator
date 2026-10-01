"""Fixed, bounded child protocol for one Provider HTTPS request.

This module intentionally depends only on the Python standard library so the
host can stage exactly this file into a dedicated systemd service. The child
does not load application configuration, read ambient proxy settings, or emit
termination claims.
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field
import http.client
import json
import re
import ssl
import sys
from urllib.parse import urlsplit


MAX_PROVIDER_SENDER_FRAME_BYTES = 12 * 1024 * 1024
MAX_PROVIDER_SENDER_BODY_BYTES = 8_000_000
MAX_PROVIDER_SENDER_RESPONSE_BYTES = 8_000_000
_MAX_HEADERS = 32
_REQUEST_HEADERS = frozenset({
    "accept",
    "anthropic-version",
    "authorization",
    "content-type",
    "idempotency-key",
    "x-api-key",
    "x-client-request-id",
    "x-request-id",
})
_RESPONSE_HEADERS = frozenset({"request-id", "x-request-id", "retry-after"})
_HEADER_NAME = re.compile(r"^[a-z0-9-]{1,128}$", re.ASCII)


@dataclass(frozen=True, slots=True)
class ProviderSenderRequest:
    """Validated request frame. The authorization value is not repr'd."""

    url: str
    headers: tuple[tuple[str, str], ...]
    body: bytes
    timeout_ms: int
    max_response_bytes: int

    def __repr__(self) -> str:
        return (
            "ProviderSenderRequest(url=<redacted>, headers=<redacted>, "
            f"body_bytes={len(self.body)}, timeout_ms={self.timeout_ms}, "
            f"max_response_bytes={self.max_response_bytes})"
        )


@dataclass(frozen=True, slots=True)
class ProviderSenderResponse:
    """Sanitized Provider HTTP response frame."""

    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes = field(repr=False)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Provider sender JSON contains a duplicate key")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise ValueError(f"Provider sender JSON contains invalid constant {value}")


def decode_provider_sender_request(frame: bytes) -> ProviderSenderRequest:
    """Decode one bounded, exact-shape host-to-child request frame."""

    if not isinstance(frame, bytes) or not frame or len(frame) > MAX_PROVIDER_SENDER_FRAME_BYTES:
        raise ValueError("Provider sender request frame is outside its byte bound")
    try:
        payload = json.loads(
            frame.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("Provider sender request frame is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("Provider sender request must be a JSON object")
    required = {
        "version", "url", "headers", "body_b64", "timeout_ms", "max_response_bytes"
    }
    if set(payload) != required:
        raise ValueError("Provider sender request has unexpected or missing fields")
    if (
        isinstance(payload["version"], bool)
        or not isinstance(payload["version"], int)
        or payload["version"] != 1
    ):
        raise ValueError("Provider sender request version is unsupported")

    url = payload["url"]
    if not isinstance(url, str) or not 1 <= len(url) <= 4_096:
        raise ValueError("Provider sender URL is outside its text bound")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Provider sender URL is invalid") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.hostname.isascii()
        or "%" in parsed.hostname
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in url)
        or (port is not None and not 1 <= port <= 65_535)
    ):
        raise ValueError("Provider sender accepts only canonical credential-free HTTPS URLs")

    raw_headers = payload["headers"]
    if not isinstance(raw_headers, list) or not 1 <= len(raw_headers) <= _MAX_HEADERS:
        raise ValueError("Provider sender headers are outside their count bound")
    headers: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in raw_headers:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not isinstance(item[1], str)
        ):
            raise ValueError("Provider sender headers must be name/value pairs")
        name = item[0].lower()
        value = item[1]
        if (
            not _HEADER_NAME.fullmatch(name)
            or name not in _REQUEST_HEADERS
            or name in seen
            or len(value) > 4_096
            or not value.isascii()
            or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        ):
            raise ValueError("Provider sender header is invalid or duplicated")
        seen.add(name)
        headers.append((name, value))
    if ("authorization" in seen) == ("x-api-key" in seen):
        raise ValueError("Provider sender requires exactly one provider credential header")
    if dict(headers).get("content-type", "").lower() != "application/json":
        raise ValueError("Provider sender requires JSON content-type")

    body_b64 = payload["body_b64"]
    if not isinstance(body_b64, str) or len(body_b64) > 4 * ((MAX_PROVIDER_SENDER_BODY_BYTES + 2) // 3):
        raise ValueError("Provider sender request body is outside its byte bound")
    try:
        body = base64.b64decode(body_b64.encode("ascii", errors="strict"), validate=True)
    except (UnicodeError, binascii.Error) as exc:
        raise ValueError("Provider sender request body encoding is invalid") from exc
    if not body or len(body) > MAX_PROVIDER_SENDER_BODY_BYTES:
        raise ValueError("Provider sender request body is outside its byte bound")

    timeout_ms = payload["timeout_ms"]
    max_response_bytes = payload["max_response_bytes"]
    if (
        isinstance(timeout_ms, bool)
        or not isinstance(timeout_ms, int)
        or not 1 <= timeout_ms <= 3_600_000
        or isinstance(max_response_bytes, bool)
        or not isinstance(max_response_bytes, int)
        or not 1 <= max_response_bytes <= MAX_PROVIDER_SENDER_RESPONSE_BYTES
    ):
        raise ValueError("Provider sender timeout or response bound is invalid")
    return ProviderSenderRequest(
        url=url,
        headers=tuple(headers),
        body=body,
        timeout_ms=timeout_ms,
        max_response_bytes=max_response_bytes,
    )


def encode_provider_sender_request(
    *,
    url: str,
    headers: tuple[tuple[str, str], ...],
    body: bytes,
    timeout_ms: int,
    max_response_bytes: int,
) -> bytes:
    """Build the exact bounded frame consumed by the fixed child helper."""

    if not isinstance(body, bytes):
        raise ValueError("Provider sender request body must be bytes")
    payload = {
        "version": 1,
        "url": url,
        "headers": [[name, value] for name, value in headers],
        "body_b64": base64.b64encode(body).decode("ascii"),
        "timeout_ms": timeout_ms,
        "max_response_bytes": max_response_bytes,
    }
    try:
        frame = json.dumps(
            payload, ensure_ascii=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("Provider sender request values are not JSON-safe") from exc
    decode_provider_sender_request(frame)
    return frame


def decode_provider_sender_response(
    frame: bytes, *, max_response_bytes: int
) -> ProviderSenderResponse:
    """Decode one exact child response; a child cannot provide stop proof."""

    if (
        not isinstance(frame, bytes)
        or not isinstance(max_response_bytes, int)
        or isinstance(max_response_bytes, bool)
        or not 1 <= max_response_bytes <= MAX_PROVIDER_SENDER_RESPONSE_BYTES
        or len(frame) > (4 * ((max_response_bytes + 2) // 3) + 16_384)
    ):
        raise ValueError("Provider sender response frame is outside its byte bound")
    try:
        payload = json.loads(
            frame.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("Provider sender response frame is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("Provider sender response must be a JSON object")
    required = {"version", "status", "headers", "body_b64"}
    if set(payload) != required:
        raise ValueError("Provider sender response has unexpected or missing fields")
    if (
        isinstance(payload["version"], bool)
        or not isinstance(payload["version"], int)
        or payload["version"] != 1
    ):
        raise ValueError("Provider sender response version is unsupported")
    status = payload["status"]
    if isinstance(status, bool) or not isinstance(status, int) or not 100 <= status <= 599:
        raise ValueError("Provider sender response status is invalid")

    raw_headers = payload["headers"]
    if not isinstance(raw_headers, list) or len(raw_headers) > len(_RESPONSE_HEADERS):
        raise ValueError("Provider sender response headers are outside their bound")
    headers: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in raw_headers:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not isinstance(item[1], str)
        ):
            raise ValueError("Provider sender response headers must be name/value pairs")
        name = item[0].lower()
        value = item[1]
        if (
            name not in _RESPONSE_HEADERS
            or name in seen
            or len(value) > 256
            or not value.isascii()
            or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        ):
            raise ValueError("Provider sender response header is invalid or duplicated")
        seen.add(name)
        headers.append((name, value))

    body_b64 = payload["body_b64"]
    max_b64_bytes = 4 * ((max_response_bytes + 2) // 3)
    if not isinstance(body_b64, str) or len(body_b64) > max_b64_bytes:
        raise ValueError("Provider sender response body is outside its byte bound")
    try:
        body = base64.b64decode(body_b64.encode("ascii", errors="strict"), validate=True)
    except (UnicodeError, binascii.Error) as exc:
        raise ValueError("Provider sender response body encoding is invalid") from exc
    if len(body) > max_response_bytes:
        raise ValueError("Provider sender response body is outside its byte bound")
    return ProviderSenderResponse(status=status, headers=tuple(headers), body=body)


def _perform_https_post(request: ProviderSenderRequest) -> ProviderSenderResponse:
    parsed = urlsplit(request.url)
    connection = http.client.HTTPSConnection(
        parsed.hostname,
        parsed.port,
        timeout=request.timeout_ms / 1000,
        context=ssl.create_default_context(),
    )
    target = parsed.path or "/"
    try:
        connection.request("POST", target, body=request.body, headers=dict(request.headers))
        response = connection.getresponse()
        body = response.read(request.max_response_bytes + 1)
        if len(body) > request.max_response_bytes:
            raise ValueError("Provider response exceeded its byte bound")
        headers: list[tuple[str, str]] = []
        for name in ("request-id", "x-request-id", "retry-after"):
            value = response.getheader(name)
            if value and len(value) <= 256 and value.isascii() and not any(
                ord(character) < 0x20 or ord(character) == 0x7F for character in value
            ):
                headers.append((name, value))
        return ProviderSenderResponse(
            status=int(response.status), headers=tuple(headers), body=body
        )
    finally:
        connection.close()


def _encode_provider_sender_response(response: ProviderSenderResponse) -> bytes:
    payload = {
        "version": 1,
        "status": response.status,
        "headers": [[name, value] for name, value in response.headers],
        "body_b64": base64.b64encode(response.body).decode("ascii"),
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def main() -> int:
    """Read one frame, issue at most one HTTPS POST, and emit one bounded frame."""

    frame = sys.stdin.buffer.read(MAX_PROVIDER_SENDER_FRAME_BYTES + 1)
    try:
        request = decode_provider_sender_request(frame)
        response = _perform_https_post(request)
        encoded = _encode_provider_sender_response(response)
        sys.stdout.buffer.write(encoded)
        sys.stdout.buffer.flush()
        return 0
    except Exception:
        # Never put URL, headers, body, or exception details in stderr/journal.
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "MAX_PROVIDER_SENDER_BODY_BYTES",
    "MAX_PROVIDER_SENDER_FRAME_BYTES",
    "MAX_PROVIDER_SENDER_RESPONSE_BYTES",
    "ProviderSenderRequest",
    "ProviderSenderResponse",
    "decode_provider_sender_request",
    "decode_provider_sender_response",
    "encode_provider_sender_request",
]
