"""Bounded HTTPS transport and fail-closed provider Model Gateway.

Credentials are supplied only by an injected Secret Broker implementation.
The default broker is deliberately unavailable: this repository does not yet
implement the P4 Secret Broker and must not fall back to environment variables.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
import re
import ssl
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPSHandler,
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

from orchestrator.config.models import ModelRegistryManifest, ProviderSpec
from orchestrator.models.adapters import adapter_for
from orchestrator.models.gateway import (
    AdapterResponse,
    CancellationSignal,
    ModelGatewayError,
    ModelGatewayFailure,
    ModelRequest,
    ModelResponse,
    resolve_provider_url,
    validate_gateway_request,
    validate_gateway_response,
)
from orchestrator.validation import revalidate_model


_PROVIDER_CODE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$")
_ALLOWED_AUTH_HEADERS = {
    "openai_responses": "authorization",
    "anthropic_messages": "x-api-key",
    "openai_compatible": "authorization",
}
_MAX_RESPONSE_BYTES = 8_000_000


@dataclass(frozen=True, slots=True)
class ProviderCredential:
    """Short-lived credential material from a trusted broker; never repr'd."""

    header_name: str
    value: str = field(repr=False)

    def __repr__(self) -> str:
        return f"ProviderCredential(header_name={self.header_name!r}, value=<redacted>)"


class SecretBroker(Protocol):
    async def acquire_provider_credential(
        self,
        *,
        secret_ref: str,
        provider: ProviderSpec,
        endpoint: str,
        purpose: str,
    ) -> ProviderCredential | None: ...


class AcceptedRouteVerifier(Protocol):
    async def is_accepted(self, request: ModelRequest) -> bool: ...


@dataclass(frozen=True, slots=True)
class HTTPTransportResponse:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


class HTTPTransport(Protocol):
    async def post_json(
        self,
        *,
        url: str,
        headers: tuple[tuple[str, str], ...],
        body: bytes,
        timeout_ms: int,
        cancellation: CancellationSignal | None,
        max_response_bytes: int,
    ) -> HTTPTransportResponse: ...


class HTTPTransportTimedOut(TimeoutError):
    pass


class HTTPTransportCancelled(RuntimeError):
    def __init__(self, *, may_have_been_sent: bool) -> None:
        self.may_have_been_sent = may_have_been_sent
        super().__init__("provider transport cancelled")


class HTTPTransportFailed(RuntimeError):
    def __init__(self, *, may_have_been_sent: bool) -> None:
        self.may_have_been_sent = may_have_been_sent
        super().__init__("provider transport failed")


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class UrllibHTTPSTransport:
    """Real, bounded HTTPS POST transport with redirects and ambient proxies off."""

    async def post_json(
        self,
        *,
        url: str,
        headers: tuple[tuple[str, str], ...],
        body: bytes,
        timeout_ms: int,
        cancellation: CancellationSignal | None,
        max_response_bytes: int,
    ) -> HTTPTransportResponse:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("provider transport accepts only credential-free HTTPS URLs")
        if cancellation is not None and cancellation.cancelled:
            raise HTTPTransportCancelled(may_have_been_sent=False)
        task = asyncio.create_task(
            asyncio.to_thread(
                self._send,
                url,
                headers,
                body,
                timeout_ms / 1000,
                max_response_bytes,
            )
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_ms / 1000
        while not task.done():
            if cancellation is not None and cancellation.cancelled:
                task.add_done_callback(_consume_background_result)
                raise HTTPTransportCancelled(may_have_been_sent=True)
            remaining = deadline - loop.time()
            if remaining <= 0:
                task.add_done_callback(_consume_background_result)
                raise HTTPTransportTimedOut()
            await asyncio.wait({task}, timeout=min(remaining, 0.05))
        try:
            return task.result()
        except HTTPError as exc:  # defensive; _send normally converts it to a response
            raise HTTPTransportFailed(may_have_been_sent=True) from exc
        except (URLError, OSError, TimeoutError, ValueError) as exc:
            raise HTTPTransportFailed(may_have_been_sent=True) from exc

    @staticmethod
    def _send(
        url: str,
        headers: tuple[tuple[str, str], ...],
        body: bytes,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> HTTPTransportResponse:
        request = Request(url, data=body, headers=dict(headers), method="POST")
        opener = build_opener(
            ProxyHandler({}),
            HTTPSHandler(context=ssl.create_default_context()),
            _NoRedirect(),
        )
        try:
            response = opener.open(request, timeout=timeout_seconds)
        except HTTPError as exc:
            response = exc
        with response:
            raw = response.read(max_response_bytes + 1)
            if len(raw) > max_response_bytes:
                raise ValueError("provider response exceeded the configured byte limit")
            captured: list[tuple[str, str]] = []
            for name in ("request-id", "x-request-id", "retry-after"):
                value = response.headers.get(name)
                if value and _is_header_value(value):
                    captured.append((name, value[:256]))
            return HTTPTransportResponse(
                status=int(response.status),
                headers=tuple(captured),
                body=raw,
            )


class UnavailableSecretBroker:
    """Safe default until the audited Secret Broker is implemented."""

    async def acquire_provider_credential(
        self,
        *,
        secret_ref: str,
        provider: ProviderSpec,
        endpoint: str,
        purpose: str,
    ) -> ProviderCredential | None:
        return None


class ProviderModelGateway:
    """Preflight, encode, transport, decode, and bind one accepted model call."""

    def __init__(
        self,
        *,
        registry: ModelRegistryManifest,
        accepted_route_verifier: AcceptedRouteVerifier | None = None,
        secret_broker: SecretBroker | None = None,
        transport: HTTPTransport | None = None,
    ) -> None:
        self.registry = revalidate_model(ModelRegistryManifest, registry)
        self.accepted_route_verifier = accepted_route_verifier
        self.secret_broker = secret_broker or UnavailableSecretBroker()
        self.transport = transport or UrllibHTTPSTransport()

    async def invoke(
        self,
        request: ModelRequest,
        *,
        cancellation: CancellationSignal | None = None,
    ) -> ModelResponse:
        try:
            request = revalidate_model(ModelRequest, request)
        except (TypeError, ValueError):
            self._fail("invalid_request", "preflight", "not_sent", False)
        if cancellation is not None and cancellation.cancelled:
            self._fail("cancelled", "preflight", "not_sent", False)
        try:
            provider, model = validate_gateway_request(request, self.registry)
        except (TypeError, ValueError):
            self._fail("stale_decision", "preflight", "not_sent", False)
        if self.accepted_route_verifier is None:
            self._fail("stale_decision", "preflight", "not_sent", False)
        try:
            accepted = await self.accepted_route_verifier.is_accepted(request)
        except Exception:
            accepted = False
        if not accepted:
            self._fail("stale_decision", "preflight", "not_sent", False)

        adapter = adapter_for(provider.adapter)
        try:
            encoded = adapter.encode_request(request, model)
            url = resolve_provider_url(provider, encoded)
        except (TypeError, ValueError):
            self._fail("invalid_request", "preflight", "not_sent", False)
        if cancellation is not None and cancellation.cancelled:
            self._fail("cancelled", "preflight", "not_sent", False)
        try:
            credential = await self.secret_broker.acquire_provider_credential(
                secret_ref=provider.secret_ref,
                provider=provider,
                endpoint=provider.effective_endpoint,
                purpose="model_inference",
            )
        except Exception:
            credential = None
        if credential is None:
            self._fail("credential_delivery_unsupported", "preflight", "not_sent", False)
        auth_header = credential.header_name.lower()
        if (
            auth_header != _ALLOWED_AUTH_HEADERS[provider.adapter]
            or not credential.value
            or len(credential.value) > 4_096
            or not _is_header_value(credential.value)
        ):
            self._fail("credential_unavailable", "preflight", "not_sent", False)

        headers = [
            ("Content-Type", "application/json"),
            ("Accept", "application/json"),
            (credential.header_name, _auth_value(provider.adapter, credential.value)),
            ("Idempotency-Key", request.idempotency_key),
            ("X-Request-Id", request.request_id),
        ]
        headers.extend((item.name, item.value) for item in encoded.headers)
        try:
            response = await self.transport.post_json(
                url=url,
                headers=tuple(headers),
                body=encoded.body_json.encode("utf-8"),
                timeout_ms=request.timeout_ms,
                cancellation=cancellation,
                max_response_bytes=_MAX_RESPONSE_BYTES,
            )
        except HTTPTransportCancelled as exc:
            self._fail(
                "cancelled",
                "transport",
                "unknown" if exc.may_have_been_sent else "not_sent",
                False,
            )
        except HTTPTransportTimedOut:
            self._fail("timeout", "transport", "unknown", False)
        except Exception as exc:
            may_have_been_sent = getattr(exc, "may_have_been_sent", True)
            self._fail(
                "transport_error",
                "transport",
                "unknown" if may_have_been_sent else "not_sent",
                False,
            )

        provider_request_id = _response_header(response.headers, {"request-id", "x-request-id"})
        if response.status < 200 or response.status >= 300:
            self._raise_provider_status(response, provider_request_id)
        try:
            body_text = response.body.decode("utf-8", errors="strict")
            adapter_response = AdapterResponse(
                http_status=response.status,
                body_json=body_text,
                provider_request_id=provider_request_id,
            )
            decoded = adapter.decode_response(
                adapter_response,
                request_id=request.request_id,
                model_id=request.model_id,
            )
            return validate_gateway_response(request, decoded)
        except Exception:
            self._fail("invalid_response", "decode", "unknown", False)

    def _raise_provider_status(self, response: HTTPTransportResponse, request_id: str | None) -> None:
        code = _provider_error_code(response.body)
        retry_after = _retry_after_ms(response.headers)
        if 300 <= response.status < 400:
            self._fail(
                "invalid_response", "transport", "known_failure", False,
                http_status=response.status, provider_request_id=request_id,
            )
        if response.status in {401, 403}:
            self._fail("authentication_failed", "provider", "known_failure", False,
                       http_status=response.status, provider_code=code, provider_request_id=request_id)
        if response.status == 429:
            self._fail("rate_limited", "provider", "known_failure", True,
                       http_status=response.status, provider_code=code, provider_request_id=request_id,
                       retry_after_ms=retry_after)
        if response.status in {408, 504}:
            self._fail("timeout", "provider", "unknown", False,
                       http_status=response.status, provider_code=code, provider_request_id=request_id)
        if response.status >= 500:
            self._fail("provider_unavailable", "provider", "unknown", False,
                       http_status=response.status, provider_code=code, provider_request_id=request_id)
        self._fail("invalid_request", "provider", "known_failure", False,
                   http_status=response.status, provider_code=code, provider_request_id=request_id)

    @staticmethod
    def _fail(
        code: str,
        phase: str,
        outcome: str,
        retryable: bool,
        *,
        http_status: int | None = None,
        provider_code: str | None = None,
        provider_request_id: str | None = None,
        retry_after_ms: int | None = None,
    ) -> None:
        raise ModelGatewayError(
            ModelGatewayFailure(
                code=code,
                phase=phase,
                outcome=outcome,
                retryable=retryable,
                http_status=http_status,
                provider_code=provider_code,
                provider_request_id=provider_request_id,
                retry_after_ms=retry_after_ms,
            )
        )


def _auth_value(adapter: str, secret: str) -> str:
    return secret if adapter == "anthropic_messages" else f"Bearer {secret}"


def _is_header_value(value: str) -> bool:
    return not any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)


def _response_header(headers: tuple[tuple[str, str], ...], names: set[str]) -> str | None:
    return next((value[:256] for name, value in headers if name.lower() in names and _is_header_value(value)), None)


def _provider_error_code(body: bytes) -> str | None:
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(value, dict):
        return None
    error = value.get("error")
    code = error.get("code") if isinstance(error, dict) else None
    if code is None and isinstance(error, dict):
        code = error.get("type")
    return code[:128] if isinstance(code, str) and _PROVIDER_CODE.fullmatch(code) else None


def _retry_after_ms(headers: tuple[tuple[str, str], ...]) -> int | None:
    value = _response_header(headers, {"retry-after"})
    if value is None or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value):
        return None
    seconds = float(value)
    if seconds >= 86_400:
        return 86_400_000
    return int(seconds * 1000)


def _consume_background_result(task: asyncio.Task[object]) -> None:
    try:
        task.result()
    except BaseException:
        pass


__all__ = [
    "AcceptedRouteVerifier",
    "HTTPTransport",
    "HTTPTransportCancelled",
    "HTTPTransportFailed",
    "HTTPTransportResponse",
    "HTTPTransportTimedOut",
    "ProviderCredential",
    "ProviderModelGateway",
    "SecretBroker",
    "UnavailableSecretBroker",
    "UrllibHTTPSTransport",
]
