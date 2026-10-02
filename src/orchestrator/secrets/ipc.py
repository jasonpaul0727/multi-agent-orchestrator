"""Strict version-1 JSON contract for the Secret Broker IPC boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Literal
from urllib.parse import urlsplit

from pydantic import ValidationError

from orchestrator.models.gateway import SecretAccessContext


MAX_SECRET_BROKER_FRAME_BYTES = 16_384
_REQUEST_FIELDS: frozenset[str] = frozenset(
    {"version", "session_nonce", "secret_ref", "provider_id", "endpoint", "purpose", "context"}
)
_RESPONSE_FIELDS: frozenset[str] = frozenset(
    {"version", "request_id", "provider_id", "endpoint", "purpose", "status", "header_name", "credential_value"}
)
_CONTEXT_FIELDS: frozenset[str] = frozenset(
    {"request_id", "run_id", "node_id", "attempt_id", "fencing_generation", "accepted_route_id", "budget_reservation_id"}
)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$")
_SECRET_REF = re.compile(
    r"^(?:env:[A-Za-z_][A-Za-z0-9_]*|(?:keyring|plugin):[^\s=]{1,500})$"
)
_NONCE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_\x60|~0-9A-Za-z-]{1,128}$")
_SAFE_PURPOSE = "model_inference"
_MAX_ENDPOINT_LENGTH = 2_048
_MAX_CREDENTIAL_LENGTH = 4_096


@dataclass(frozen=True, slots=True, repr=False)
class SecretBrokerRequest:
    version: int
    session_nonce: str = field(repr=False)
    secret_ref: str
    provider_id: str
    endpoint: str
    purpose: str
    context: SecretAccessContext

    def __repr__(self) -> str:
        return (
            "SecretBrokerRequest("
            f"version={self.version!r}, session_nonce=<redacted>, "
            f"secret_ref={self.secret_ref!r}, provider_id={self.provider_id!r}, "
            f"endpoint={self.endpoint!r}, purpose={self.purpose!r}, context={self.context!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class SecretBrokerResponse:
    version: int
    request_id: str
    provider_id: str
    endpoint: str
    purpose: str
    status: Literal["credential", "denied", "unavailable"]
    header_name: str | None
    credential_value: str | None = field(repr=False)

    def __repr__(self) -> str:
        redacted = "<redacted>" if self.credential_value is not None else None
        return (
            "SecretBrokerResponse("
            f"version={self.version!r}, request_id={self.request_id!r}, "
            f"provider_id={self.provider_id!r}, endpoint={self.endpoint!r}, "
            f"purpose={self.purpose!r}, status={self.status!r}, "
            f"header_name={self.header_name!r}, credential_value={redacted})"
        )


def encode_secret_broker_request(request: SecretBrokerRequest) -> bytes:
    try:
        if not isinstance(request, SecretBrokerRequest):
            raise ValueError
        context = request.context.model_dump(mode="json") if isinstance(request.context, SecretAccessContext) else None
        payload: dict[str, object] = {
            "version": request.version,
            "session_nonce": request.session_nonce,
            "secret_ref": request.secret_ref,
            "provider_id": request.provider_id,
            "endpoint": request.endpoint,
            "purpose": request.purpose,
            "context": context,
        }
        validated = _validate_request_payload(payload)
        return _encode_payload(_request_payload(validated))
    except (AttributeError, TypeError, ValueError, ValidationError, RecursionError):
        raise ValueError("Secret Broker request is invalid") from None


def decode_secret_broker_request(frame: bytes) -> SecretBrokerRequest:
    payload = _decode_exact_json_object(frame, max_bytes=MAX_SECRET_BROKER_FRAME_BYTES)
    return _validate_request_payload(payload)


def encode_secret_broker_response(response: SecretBrokerResponse) -> bytes:
    try:
        if not isinstance(response, SecretBrokerResponse):
            raise ValueError
        payload: dict[str, object] = {
            "version": response.version,
            "request_id": response.request_id,
            "provider_id": response.provider_id,
            "endpoint": response.endpoint,
            "purpose": response.purpose,
            "status": response.status,
            "header_name": response.header_name,
            "credential_value": response.credential_value,
        }
        validated = _validate_response_payload(payload)
        return _encode_payload(_response_payload(validated))
    except (AttributeError, TypeError, ValueError, ValidationError, RecursionError):
        raise ValueError("Secret Broker response is invalid") from None


def decode_secret_broker_response(frame: bytes) -> SecretBrokerResponse:
    payload = _decode_exact_json_object(frame, max_bytes=MAX_SECRET_BROKER_FRAME_BYTES)
    return _validate_response_payload(payload)


def _decode_exact_json_object(frame: bytes, *, max_bytes: int) -> dict[str, object]:
    if not isinstance(frame, bytes):
        raise ValueError("Secret Broker frame is invalid")
    if len(frame) > max_bytes:
        raise ValueError("Secret Broker frame exceeds its bound")
    try:
        text = frame.decode("utf-8")
        payload = json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_non_finite,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        raise ValueError("Secret Broker frame is invalid") from None
    except ValueError as exc:
        if str(exc) == "Secret Broker payload has duplicate fields":
            raise ValueError("Secret Broker payload has duplicate fields") from None
        raise ValueError("Secret Broker frame is invalid") from None
    if not isinstance(payload, dict):
        raise ValueError("Secret Broker frame must contain one JSON object")
    return payload


def _validate_request_payload(payload: dict[str, object]) -> SecretBrokerRequest:
    if not isinstance(payload, dict):
        raise ValueError("Secret Broker request schema is invalid")
    _validate_keys(payload, _REQUEST_FIELDS)
    _validate_version(payload.get("version"))
    nonce = _nonce(payload.get("session_nonce"))
    secret_ref = payload.get("secret_ref")
    if not isinstance(secret_ref, str) or len(secret_ref) > 512 or _SECRET_REF.fullmatch(secret_ref) is None:
        raise ValueError("Secret Broker request schema is invalid")
    provider_id = _identifier(payload.get("provider_id"))
    endpoint = _endpoint(payload.get("endpoint"))
    purpose = payload.get("purpose")
    if purpose != _SAFE_PURPOSE:
        raise ValueError("Secret Broker request schema is invalid")
    context_payload = payload.get("context")
    if not isinstance(context_payload, dict) or set(context_payload) != _CONTEXT_FIELDS:
        raise ValueError("Secret Broker request schema is invalid")
    try:
        context = SecretAccessContext.model_validate(context_payload)
    except (ValidationError, TypeError, ValueError, RecursionError):
        raise ValueError("Secret Broker request schema is invalid") from None
    return SecretBrokerRequest(1, nonce, secret_ref, provider_id, endpoint, purpose, context)


def _validate_response_payload(payload: dict[str, object]) -> SecretBrokerResponse:
    if not isinstance(payload, dict):
        raise ValueError("Secret Broker response schema is invalid")
    _validate_keys(payload, _RESPONSE_FIELDS)
    _validate_version(payload.get("version"))
    request_id = _identifier(payload.get("request_id"))
    provider_id = _identifier(payload.get("provider_id"))
    endpoint = _endpoint(payload.get("endpoint"))
    purpose = payload.get("purpose")
    status = payload.get("status")
    if (
        purpose != _SAFE_PURPOSE
        or not isinstance(status, str)
        or status not in {"credential", "denied", "unavailable"}
    ):
        raise ValueError("Secret Broker response schema is invalid")
    header_name = payload.get("header_name")
    credential_value = payload.get("credential_value")
    if status == "credential":
        if not isinstance(header_name, str) or _HEADER_NAME.fullmatch(header_name) is None:
            raise ValueError("Secret Broker response schema is invalid")
        if (
            not isinstance(credential_value, str)
            or not credential_value
            or len(credential_value) > _MAX_CREDENTIAL_LENGTH
            or not credential_value.isascii()
            or any(ord(character) < 0x20 or ord(character) == 0x7F for character in credential_value)
        ):
            raise ValueError("Secret Broker response schema is invalid")
    elif header_name is not None or credential_value is not None:
        raise ValueError("Secret Broker response schema is invalid")
    return SecretBrokerResponse(
        1, request_id, provider_id, endpoint, purpose, status, header_name, credential_value
    )


def _validate_keys(payload: dict[str, object], expected: frozenset[str]) -> None:
    keys = set(payload)
    if keys != expected:
        if keys - expected:
            raise ValueError("Secret Broker payload has unexpected fields")
        raise ValueError("Secret Broker payload has missing fields")


def _validate_version(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value != 1:
        raise ValueError("Secret Broker payload version is unsupported")


def _identifier(value: object) -> str:
    if not isinstance(value, str) or len(value) > 128 or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("Secret Broker payload contains an invalid identifier")
    return value


def _nonce(value: object) -> str:
    if not isinstance(value, str) or _NONCE.fullmatch(value) is None:
        raise ValueError("Secret Broker request contains an invalid nonce")
    return value


def _endpoint(value: object) -> str:
    if not isinstance(value, str) or len(value) > _MAX_ENDPOINT_LENGTH or not value.isascii():
        raise ValueError("Secret Broker payload contains an invalid endpoint")
    if any(ord(character) < 0x21 or ord(character) == 0x7F for character in value):
        raise ValueError("Secret Broker payload contains an invalid endpoint")
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        raise ValueError("Secret Broker payload contains an invalid endpoint") from None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or any(part in {".", ".."} for part in parsed.path.split("/"))
    ):
        raise ValueError("Secret Broker payload contains an invalid endpoint")
    return value


def _request_payload(request: SecretBrokerRequest) -> dict[str, object]:
    return {
        "version": request.version,
        "session_nonce": request.session_nonce,
        "secret_ref": request.secret_ref,
        "provider_id": request.provider_id,
        "endpoint": request.endpoint,
        "purpose": request.purpose,
        "context": request.context.model_dump(mode="json"),
    }


def _response_payload(response: SecretBrokerResponse) -> dict[str, object]:
    return {
        "version": response.version,
        "request_id": response.request_id,
        "provider_id": response.provider_id,
        "endpoint": response.endpoint,
        "purpose": response.purpose,
        "status": response.status,
        "header_name": response.header_name,
        "credential_value": response.credential_value,
    }


def _encode_payload(payload: dict[str, object]) -> bytes:
    try:
        encoded = json.dumps(payload, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, RecursionError):
        raise ValueError("Secret Broker payload is invalid") from None
    if len(encoded) > MAX_SECRET_BROKER_FRAME_BYTES:
        raise ValueError("Secret Broker frame exceeds its bound")
    return encoded


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Secret Broker payload has duplicate fields")
        result[key] = value
    return result


def _reject_non_finite(_value: str) -> object:
    raise ValueError("Secret Broker payload contains a non-finite number")
