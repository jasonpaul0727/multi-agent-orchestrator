"""Bounded HTTPS transport and fail-closed provider Model Gateway.

Credentials are supplied only by an injected Secret Broker implementation.
The default broker is deliberately unavailable: this repository does not yet
implement the P4 Secret Broker and must not fall back to environment variables.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
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
from orchestrator.identifiers import new_id
from orchestrator.models.adapters import adapter_for
from orchestrator.models.gateway import (
    AdapterResponse,
    CancellationSignal,
    ModelGatewayError,
    ModelGatewayFailure,
    ModelRequest,
    ModelResponse,
    SecretAccessContext,
    resolve_provider_url,
    validate_gateway_request,
    validate_gateway_response,
)
from orchestrator.models.provider_calls import (
    ProviderCallJournal,
    ProviderCallReplayBlocked,
    ProviderCallSnapshot,
)
from orchestrator.models.provider_sender import ProviderSenderTerminationReceipt
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
    provider_id: str
    endpoint: str
    purpose: str

    def __repr__(self) -> str:
        return (
            f"ProviderCredential(header_name={self.header_name!r}, "
            f"provider_id={self.provider_id!r}, endpoint={self.endpoint!r}, "
            f"purpose={self.purpose!r}, value=<redacted>)"
        )


class SecretBroker(Protocol):
    async def acquire_provider_credential(
        self,
        *,
        secret_ref: str,
        provider: ProviderSpec,
        endpoint: str,
        purpose: str,
        context: SecretAccessContext,
    ) -> ProviderCredential | None: ...


class AcceptedRouteVerifier(Protocol):
    async def is_accepted(self, request: ModelRequest) -> bool: ...


@dataclass(frozen=True, slots=True)
class HTTPTransportResponse:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    termination_receipt: ProviderSenderTerminationReceipt | None = None


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
        call_binding: ProviderCallSnapshot,
    ) -> HTTPTransportResponse: ...


class HTTPTransportTimedOut(TimeoutError):
    def __init__(
        self,
        *,
        may_have_been_sent: bool = True,
        termination_receipt: ProviderSenderTerminationReceipt | None = None,
    ) -> None:
        self.may_have_been_sent = may_have_been_sent
        self.termination_receipt = termination_receipt
        super().__init__("provider transport timed out")


class HTTPTransportCancelled(RuntimeError):
    def __init__(
        self,
        *,
        may_have_been_sent: bool,
        termination_receipt: ProviderSenderTerminationReceipt | None = None,
    ) -> None:
        self.may_have_been_sent = may_have_been_sent
        self.termination_receipt = termination_receipt
        super().__init__("provider transport cancelled")


class HTTPTransportFailed(RuntimeError):
    def __init__(
        self,
        *,
        may_have_been_sent: bool,
        termination_receipt: ProviderSenderTerminationReceipt | None = None,
    ) -> None:
        self.may_have_been_sent = may_have_been_sent
        self.termination_receipt = termination_receipt
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
        call_binding: ProviderCallSnapshot | None = None,
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


class SystemdProviderHTTPSTransport:
    """Run exactly one Provider HTTPS request in a host-supervised systemd unit."""

    def __init__(self, *, launcher: object | None = None) -> None:
        if launcher is None:
            from orchestrator.isolation.provider_sender import SystemdProviderSenderLauncher

            launcher = SystemdProviderSenderLauncher()
        self.launcher = launcher

    async def post_json(
        self,
        *,
        url: str,
        headers: tuple[tuple[str, str], ...],
        body: bytes,
        timeout_ms: int,
        cancellation: CancellationSignal | None,
        max_response_bytes: int,
        call_binding: ProviderCallSnapshot,
    ) -> HTTPTransportResponse:
        from orchestrator.runtime.provider_sender_process import encode_provider_sender_request
        from orchestrator.isolation.provider_sender import ProviderSenderResult

        _validate_sender_binding(call_binding, body, headers)
        try:
            frame = encode_provider_sender_request(
                url=url,
                headers=headers,
                body=body,
                timeout_ms=timeout_ms,
                max_response_bytes=max_response_bytes,
            )
            launch_task = asyncio.create_task(
                asyncio.to_thread(
                    self.launcher.launch,
                    frame,
                    timeout_seconds=max(1, math.ceil(timeout_ms / 1_000) + 1),
                    output_bytes=4 * ((max_response_bytes + 2) // 3) + 16_384,
                )
            )
        except Exception:
            raise HTTPTransportFailed(may_have_been_sent=False) from None

        try:
            session = await asyncio.shield(launch_task)
        except asyncio.CancelledError:
            try:
                session = await _await_task_uninterruptibly(launch_task)
            except BaseException:
                raise HTTPTransportCancelled(may_have_been_sent=False) from None
            result = await self._cancel_and_wait(session)
            if not isinstance(result, ProviderSenderResult):
                raise HTTPTransportCancelled(may_have_been_sent=True) from None
            receipt = _host_sender_receipt(call_binding, result)
            raise HTTPTransportCancelled(
                may_have_been_sent=result.input_written,
                termination_receipt=receipt,
            ) from None
        except Exception:
            raise HTTPTransportFailed(may_have_been_sent=False) from None

        if cancellation is not None and cancellation.cancelled:
            result = await self._cancel_and_wait(session)
            receipt = (
                _host_sender_receipt(call_binding, result)
                if isinstance(result, ProviderSenderResult)
                else None
            )
            raise HTTPTransportCancelled(
                may_have_been_sent=(
                    result.input_written
                    if isinstance(result, ProviderSenderResult)
                    else True
                ),
                termination_receipt=receipt,
            ) from None

        wait_task = asyncio.create_task(asyncio.to_thread(session.wait))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_ms / 1_000
        while not wait_task.done():
            if cancellation is not None and cancellation.cancelled:
                result = await self._cancel_and_wait(session, wait_task)
                receipt = (
                    _host_sender_receipt(call_binding, result)
                    if isinstance(result, ProviderSenderResult)
                    else None
                )
                raise HTTPTransportCancelled(
                    may_have_been_sent=(
                        result.input_written
                        if isinstance(result, ProviderSenderResult)
                        else True
                    ),
                    termination_receipt=receipt,
                ) from None
            remaining = deadline - loop.time()
            if remaining <= 0:
                result = await self._cancel_and_wait(session, wait_task)
                receipt = (
                    _host_sender_receipt(call_binding, result)
                    if isinstance(result, ProviderSenderResult)
                    else None
                )
                raise HTTPTransportTimedOut(
                    may_have_been_sent=(
                        result.input_written
                        if isinstance(result, ProviderSenderResult)
                        else True
                    ),
                    termination_receipt=receipt,
                ) from None
            try:
                await asyncio.wait({wait_task}, timeout=min(remaining, 0.05))
            except asyncio.CancelledError:
                result = await self._cancel_and_wait(session, wait_task)
                receipt = (
                    _host_sender_receipt(call_binding, result)
                    if isinstance(result, ProviderSenderResult)
                    else None
                )
                raise HTTPTransportCancelled(
                    may_have_been_sent=(
                        result.input_written
                        if isinstance(result, ProviderSenderResult)
                        else True
                    ),
                    termination_receipt=receipt,
                ) from None

        try:
            result = wait_task.result()
        except BaseException:
            raise HTTPTransportFailed(may_have_been_sent=True) from None
        if not isinstance(result, ProviderSenderResult):
            raise HTTPTransportFailed(may_have_been_sent=True) from None
        receipt = _host_sender_receipt(call_binding, result)
        if receipt is None:
            raise HTTPTransportFailed(
                may_have_been_sent=result.input_written,
                termination_receipt=None,
            ) from None
        if result.timed_out:
            raise HTTPTransportTimedOut(
                may_have_been_sent=result.input_written,
                termination_receipt=receipt,
            ) from None
        if result.cancelled or result.output_limited or result.response is None:
            raise HTTPTransportFailed(
                may_have_been_sent=result.input_written,
                termination_receipt=receipt,
            ) from None
        return HTTPTransportResponse(
            status=result.response.status,
            headers=result.response.headers,
            body=result.response.body,
            termination_receipt=receipt,
        )

    async def _cancel_and_wait(self, session: object, wait_task=None):
        cancel_task = asyncio.create_task(asyncio.to_thread(session.cancel))
        try:
            await _await_task_uninterruptibly(cancel_task)
        except BaseException:
            # The wait path still has to finish and prove the cgroup is empty.
            pass
        if wait_task is None:
            wait_task = asyncio.create_task(asyncio.to_thread(session.wait))
        try:
            return await _await_task_uninterruptibly(wait_task)
        except BaseException:
            return None


class UnavailableSecretBroker:
    """Safe default when the host application has not configured a broker."""

    async def acquire_provider_credential(
        self,
        *,
        secret_ref: str,
        provider: ProviderSpec,
        endpoint: str,
        purpose: str,
        context: SecretAccessContext,
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
        provider_call_journal: ProviderCallJournal | None = None,
    ) -> None:
        self.registry = revalidate_model(ModelRegistryManifest, registry)
        self.accepted_route_verifier = accepted_route_verifier
        self.secret_broker = secret_broker or UnavailableSecretBroker()
        self.transport = transport if transport is not None else SystemdProviderHTTPSTransport()
        self.provider_call_journal = provider_call_journal

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
        if self.provider_call_journal is None:
            self._fail("provider_journal_unavailable", "preflight", "not_sent", False)
        try:
            prior_call = self.provider_call_journal.read(request)
        except Exception:
            self._fail("provider_journal_unavailable", "preflight", "not_sent", False)
        if prior_call is not None:
            self._fail("idempotency_conflict", "preflight", "not_sent", False)
        provider_correlation_id = (
            "maestro-" + new_id()
            if provider.adapter == "openai_responses"
            else None
        )
        try:
            credential = await self.secret_broker.acquire_provider_credential(
                secret_ref=provider.secret_ref,
                provider=provider,
                endpoint=provider.effective_endpoint,
                purpose="model_inference",
                context=SecretAccessContext(
                    request_id=request.request_id,
                    run_id=request.run_id,
                    node_id=request.node_id,
                    attempt_id=request.attempt_id,
                    fencing_generation=request.fencing_generation,
                    accepted_route_id=request.accepted_route.decision_id,
                    budget_reservation_id=request.budget_reservation_id,
                ),
            )
        except Exception:
            credential = None
        if credential is None:
            self._fail("credential_delivery_unsupported", "preflight", "not_sent", False)
        if (
            not isinstance(credential, ProviderCredential)
            or not isinstance(credential.header_name, str)
            or not isinstance(credential.value, str)
            or not isinstance(credential.provider_id, str)
            or not isinstance(credential.endpoint, str)
            or not isinstance(credential.purpose, str)
        ):
            self._fail("credential_unavailable", "preflight", "not_sent", False)
        auth_header = credential.header_name.lower()
        if (
            auth_header != _ALLOWED_AUTH_HEADERS[provider.adapter]
            or not credential.value.isascii()
            or credential.provider_id != provider.id
            or credential.endpoint != provider.effective_endpoint
            or credential.purpose != "model_inference"
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
        if provider_correlation_id is not None:
            headers.append(("X-Client-Request-Id", provider_correlation_id))
        header_names = {name.lower(): value for name, value in headers}
        for item in encoded.headers:
            prior_value = header_names.get(item.name)
            if prior_value is not None:
                if prior_value != item.value:
                    self._fail("invalid_request", "preflight", "not_sent", False)
                continue
            header_names[item.name] = item.value
            headers.append((item.name, item.value))

        try:
            self.provider_call_journal.record_intent(
                request,
                provider_id=provider.id,
                provider_adapter=provider.adapter,
                request_body=encoded.body_json.encode("utf-8"),
                provider_correlation_id=provider_correlation_id,
            )
        except ProviderCallReplayBlocked:
            self._fail("idempotency_conflict", "preflight", "not_sent", False)
        except Exception:
            self._fail("provider_journal_unavailable", "preflight", "not_sent", False)

        try:
            call_binding = self.provider_call_journal.read(request)
        except Exception:
            self._record_outcome(
                request,
                outcome="not_sent",
                failure_code="provider_journal_unavailable",
            )
            self._fail("provider_journal_unavailable", "transport", "not_sent", False)
        if call_binding is None or call_binding.status != "dispatching":
            self._record_outcome(
                request,
                outcome="not_sent",
                failure_code="provider_journal_unavailable",
            )
            self._fail("provider_journal_unavailable", "transport", "not_sent", False)
        try:
            response = await self.transport.post_json(
                url=url,
                headers=tuple(headers),
                body=encoded.body_json.encode("utf-8"),
                timeout_ms=request.timeout_ms,
                cancellation=cancellation,
                max_response_bytes=_MAX_RESPONSE_BYTES,
                call_binding=call_binding,
            )
        except HTTPTransportCancelled as exc:
            outcome = "unknown" if exc.may_have_been_sent else "not_sent"
            self._record_outcome(
                request,
                outcome=outcome,
                failure_code="cancelled",
                termination_receipt=exc.termination_receipt,
            )
            self._fail(
                "cancelled",
                "transport",
                outcome,
                False,
            )
        except HTTPTransportTimedOut as exc:
            outcome = "unknown" if exc.may_have_been_sent else "not_sent"
            self._record_outcome(
                request,
                outcome=outcome,
                failure_code="timeout",
                termination_receipt=exc.termination_receipt,
            )
            self._fail("timeout", "transport", outcome, False)
        except Exception as exc:
            may_have_been_sent = getattr(exc, "may_have_been_sent", True)
            outcome = "unknown" if may_have_been_sent else "not_sent"
            self._record_outcome(
                request,
                outcome=outcome,
                failure_code="transport_error",
                termination_receipt=getattr(exc, "termination_receipt", None),
            )
            self._fail(
                "transport_error",
                "transport",
                outcome,
                False,
            )

        provider_request_id = _response_header(response.headers, {"request-id", "x-request-id"})
        if response.status < 200 or response.status >= 300:
            try:
                self._raise_provider_status(response, provider_request_id)
            except ModelGatewayError as exc:
                self._record_outcome(
                    request,
                    outcome=exc.failure.outcome,
                    provider_request_id=exc.failure.provider_request_id,
                    http_status=response.status,
                    failure_code=exc.failure.code,
                    termination_receipt=response.termination_receipt,
                )
                raise
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
            validated = validate_gateway_response(request, decoded)
        except Exception:
            self._record_outcome(
                request,
                outcome="unknown",
                provider_request_id=provider_request_id,
                http_status=response.status,
                failure_code="invalid_response",
                termination_receipt=response.termination_receipt,
            )
            self._fail("invalid_response", "decode", "unknown", False)
        self._record_outcome(
            request,
            outcome="known_success",
            provider_request_id=provider_request_id,
            http_status=response.status,
            usage=validated.usage,
            termination_receipt=response.termination_receipt,
        )
        return validated

    def _record_outcome(
        self,
        request: ModelRequest,
        *,
        outcome: str,
        provider_request_id: str | None = None,
        http_status: int | None = None,
        failure_code: str | None = None,
        usage: object | None = None,
        termination_receipt: ProviderSenderTerminationReceipt | None = None,
    ) -> None:
        if self.provider_call_journal is None:
            self._fail("outcome_unknown", "settlement", "unknown", False)
        try:
            self.provider_call_journal.record_outcome(
                request,
                outcome=outcome,
                provider_request_id=provider_request_id,
                http_status=http_status,
                failure_code=failure_code,
                usage=usage,
                termination_receipt=termination_receipt,
            )
        except Exception:
            # A result that cannot be durably settled must remain unknown. In
            # particular, recovery must never infer that another dispatch is safe.
            self._fail(
                "outcome_unknown",
                "settlement",
                "unknown",
                False,
                provider_request_id=provider_request_id,
            )

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


async def _await_task_uninterruptibly(task: asyncio.Task):
    """Wait for a supervisor operation to settle despite repeated task.cancel()."""

    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


def _validate_sender_binding(
    call: ProviderCallSnapshot,
    body: bytes,
    headers: tuple[tuple[str, str], ...],
) -> None:
    if (
        not isinstance(call, ProviderCallSnapshot)
        or call.status != "dispatching"
        or not isinstance(body, bytes)
        or "sha256:" + hashlib.sha256(body).hexdigest() != call.request_hash
    ):
        raise HTTPTransportFailed(may_have_been_sent=False)
    request_ids = [value for name, value in headers if name.lower() == "x-request-id"]
    idempotency_keys = [
        value for name, value in headers if name.lower() == "idempotency-key"
    ]
    if (
        request_ids != [call.request_id]
        or len(idempotency_keys) != 1
        or "sha256:" + hashlib.sha256(idempotency_keys[0].encode("utf-8")).hexdigest()
        != call.idempotency_key_hash
    ):
        raise HTTPTransportFailed(may_have_been_sent=False)


def _host_sender_receipt(
    call: ProviderCallSnapshot, result: object
) -> ProviderSenderTerminationReceipt | None:
    receipt = getattr(result, "termination_receipt", None)
    control_group = getattr(receipt, "control_group", None)
    unit_name = getattr(receipt, "unit_name", None)
    active_state = getattr(receipt, "active_state", None)
    cgroup_empty = getattr(receipt, "cgroup_empty", None)
    if (
        receipt is None
        or not isinstance(control_group, str)
        or not control_group.startswith("/")
        or unit_name != getattr(result, "unit_name", None)
        or active_state not in {"inactive", "failed"}
        or cgroup_empty is not True
    ):
        return None
    try:
        return ProviderSenderTerminationReceipt.model_validate(
            {
                "provider_call_stream_id": call.stream_id,
                "run_id": call.run_id,
                "node_id": call.node_id,
                "attempt_id": call.attempt_id,
                "fencing_generation": call.fencing_generation,
                "accepted_route_id": call.accepted_route_id,
                "budget_reservation_id": call.budget_reservation_id,
                "provider_id": call.provider_id,
                "model_id": call.model_id,
                "registry_manifest_hash": call.registry_manifest_hash,
                "request_hash": call.request_hash,
                "unit_name": unit_name,
                "cgroup_path_hash": "sha256:"
                + hashlib.sha256(control_group.encode("utf-8")).hexdigest(),
                "active_state": active_state,
                "cgroup_empty": cgroup_empty,
                "observed_at": datetime.now(timezone.utc),
            },
        )
    except (TypeError, ValueError):
        return None


__all__ = [
    "AcceptedRouteVerifier",
    "HTTPTransport",
    "HTTPTransportCancelled",
    "HTTPTransportFailed",
    "HTTPTransportResponse",
    "HTTPTransportTimedOut",
    "ProviderCredential",
    "ProviderModelGateway",
    "SystemdProviderHTTPSTransport",
    "SecretBroker",
    "UnavailableSecretBroker",
    "UrllibHTTPSTransport",
]
