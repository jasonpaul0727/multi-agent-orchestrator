"""Durable, replay-safe journal for external Provider model calls."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from types import MappingProxyType
from typing import Literal, Mapping, Protocol

from orchestrator.config.models import ProviderAdapter
from orchestrator.identifiers import new_id
from orchestrator.models.gateway import ModelRequest, TokenUsage
from orchestrator.persistence import EventDraft, IdempotencyConflict, SQLiteEventStore, StaleStream
from orchestrator.persistence.sqlite_event_store import canonical_json


ProviderCallStatus = Literal[
    "dispatching", "not_sent", "known_failure", "known_success", "unknown"
]
_PROVIDER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$")


class ProviderCallReplayBlocked(RuntimeError):
    """A durable call intent already exists; a new provider dispatch is unsafe."""


class ProviderCallJournal(Protocol):
    """Durable call-intent and terminal-receipt interface used by the Gateway."""

    def record_intent(
        self,
        request: ModelRequest,
        *,
        provider_id: str,
        provider_adapter: ProviderAdapter,
        request_body: bytes,
        provider_correlation_id: str | None = None,
    ) -> str: ...

    def read(self, request: ModelRequest) -> ProviderCallSnapshot | None: ...

    def record_outcome(
        self,
        request: ModelRequest,
        *,
        outcome: Literal["not_sent", "known_failure", "known_success", "unknown"],
        provider_request_id: str | None = None,
        http_status: int | None = None,
        failure_code: str | None = None,
        usage: TokenUsage | Mapping[str, object] | None = None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ProviderCallSnapshot:
    """Sanitized provider-call facts suitable for recovery and reconciliation."""

    stream_id: str
    run_id: str
    node_id: str
    attempt_id: str
    fencing_generation: int
    request_id: str
    idempotency_key_hash: str
    accepted_route_id: str
    budget_reservation_id: str
    provider_id: str
    provider_adapter: ProviderAdapter
    provider_correlation_id: str | None
    model_id: str
    registry_manifest_hash: str
    request_hash: str
    status: ProviderCallStatus
    provider_request_id: str | None = None
    http_status: int | None = None
    failure_code: str | None = None
    usage: Mapping[str, object] | None = None


class SQLiteProviderCallJournal:
    """Append-only provider intent/outcome journal backed by the event store.

    The request payload, prompt, credentials, raw provider body, and completion
    text are intentionally excluded. A stable scope hash identifies the stream;
    a fresh append id makes claiming that scope a single-writer CAS operation.
    """

    def __init__(self, event_store: SQLiteEventStore) -> None:
        if not isinstance(event_store, SQLiteEventStore):
            raise TypeError("ProviderCallJournal requires a durable SQLite event store")
        self.event_store = event_store

    def record_intent(
        self,
        request: ModelRequest,
        *,
        provider_id: str,
        provider_adapter: ProviderAdapter,
        request_body: bytes,
        provider_correlation_id: str | None = None,
    ) -> str:
        if (
            not isinstance(provider_id, str)
            or not _PROVIDER_ID.fullmatch(provider_id)
            or provider_id != request.accepted_route.provider_id
        ):
            raise ValueError("provider call does not match its accepted route")
        if not isinstance(provider_adapter, str) or provider_adapter not in (
            "openai_responses", "anthropic_messages", "openai_compatible"
        ):
            raise ValueError("provider call adapter is unsupported")
        if provider_adapter == "openai_responses":
            if (
                not isinstance(provider_correlation_id, str)
                or not provider_correlation_id
                or len(provider_correlation_id) > 512
                or not provider_correlation_id.isascii()
                or not _safe_text(provider_correlation_id)
            ):
                raise ValueError("provider correlation id is invalid")
        elif provider_correlation_id is not None:
            raise ValueError("provider correlation id is invalid for this adapter")
        if not isinstance(request_body, bytes) or len(request_body) > 8_000_000:
            raise ValueError("provider request body must be bounded bytes")

        stream_id = _stream_id(request)
        payload = {
            **_identity(request),
            "idempotency_key_hash": _hash(request.idempotency_key.encode("utf-8")),
            "accepted_route_id": request.accepted_route.decision_id,
            "provider_id": provider_id,
            "provider_adapter": provider_adapter,
            "provider_correlation_id": provider_correlation_id,
            "model_id": request.model_id,
            "registry_manifest_hash": request.accepted_route.registry_manifest_hash,
            "budget_reservation_id": request.budget_reservation_id,
            "request_hash": _hash(request_body),
        }
        event = EventDraft(
            "ProviderCallIntentRecorded",
            payload,
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            fencing_generation=request.fencing_generation,
            causation_id=request.accepted_route.decision_id,
        )
        try:
            self.event_store.append(
                "provider_call",
                stream_id,
                expected_version=0,
                events=[event],
                idempotency_key="provider-call-claim:" + new_id(),
            )
        except (StaleStream, IdempotencyConflict) as exc:
            raise ProviderCallReplayBlocked("provider call scope already has a durable intent") from exc
        return stream_id

    def record_outcome(
        self,
        request: ModelRequest,
        *,
        outcome: Literal["not_sent", "known_failure", "known_success", "unknown"],
        provider_request_id: str | None = None,
        http_status: int | None = None,
        failure_code: str | None = None,
        usage: TokenUsage | Mapping[str, object] | None = None,
    ) -> None:
        if outcome not in {"not_sent", "known_failure", "known_success", "unknown"}:
            raise ValueError("invalid provider call outcome")
        if provider_request_id is not None and (
            not isinstance(provider_request_id, str)
            or not provider_request_id
            or len(provider_request_id) > 256
            or not _safe_text(provider_request_id)
        ):
            raise ValueError("provider request id is invalid")
        if http_status is not None and (
            isinstance(http_status, bool) or not isinstance(http_status, int)
            or not 100 <= http_status <= 599
        ):
            raise ValueError("HTTP status is invalid")
        if failure_code is not None and (
            not isinstance(failure_code, str) or not _PROVIDER_ID.fullmatch(failure_code)
        ):
            raise ValueError("failure code is invalid")
        usage_payload: dict[str, object] | None = None
        if usage is not None:
            parsed_usage = TokenUsage.model_validate(usage)
            usage_payload = parsed_usage.model_dump(mode="json", exclude_none=True)
        if self.read(request) is None:
            raise ValueError("provider call outcome requires a prior durable intent")
        payload: dict[str, object] = {
            "outcome": outcome,
            "provider_request_id": provider_request_id,
            "http_status": http_status,
            "failure_code": failure_code,
            "usage": usage_payload,
        }
        try:
            self.event_store.append(
                "provider_call",
                _stream_id(request),
                expected_version=1,
                events=[EventDraft(
                    "ProviderCallOutcomeRecorded",
                    payload,
                    run_id=request.run_id,
                    node_id=request.node_id,
                    attempt_id=request.attempt_id,
                    fencing_generation=request.fencing_generation,
                    causation_id=request.accepted_route.decision_id,
                )],
                idempotency_key="provider-call-outcome:" + _stream_id(request),
            )
        except (StaleStream, IdempotencyConflict) as exc:
            raise ValueError("provider call has no intent or already has a different outcome") from exc

    def read(self, request: ModelRequest) -> ProviderCallSnapshot | None:
        events = self.event_store.read_stream("provider_call", _stream_id(request))
        if not events:
            return None
        if len(events) > 2 or events[0].event_type != "ProviderCallIntentRecorded":
            raise ValueError("provider call stream violates the journal contract")
        intent = events[0]
        if intent.run_id != request.run_id or intent.node_id != request.node_id:
            raise ValueError("provider call stream binding does not match request")
        if intent.attempt_id != request.attempt_id or intent.fencing_generation != request.fencing_generation:
            raise ValueError("provider call stream binding does not match request")
        payload = intent.payload
        expected_binding = {
            "request_id": request.request_id,
            "idempotency_key_hash": _hash(request.idempotency_key.encode("utf-8")),
            "accepted_route_id": request.accepted_route.decision_id,
            "budget_reservation_id": request.budget_reservation_id,
            "provider_id": request.accepted_route.provider_id,
            "model_id": request.model_id,
            "registry_manifest_hash": request.accepted_route.registry_manifest_hash,
        }
        if any(payload.get(key) != value for key, value in expected_binding.items()):
            raise ValueError("provider call stream binding does not match request")
        terminal = events[1].payload if len(events) == 2 else {}
        if len(events) == 2 and events[1].event_type != "ProviderCallOutcomeRecorded":
            raise ValueError("provider call stream has an unknown terminal event")
        if len(events) == 2 and (
            events[1].run_id != intent.run_id
            or events[1].node_id != intent.node_id
            or events[1].attempt_id != intent.attempt_id
            or events[1].fencing_generation != intent.fencing_generation
            or events[1].causation_id != intent.causation_id
        ):
            raise ValueError("provider call outcome binding does not match intent")
        status: ProviderCallStatus = "dispatching" if not terminal else terminal["outcome"]
        usage = terminal.get("usage")
        return ProviderCallSnapshot(
            stream_id=intent.stream_id,
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            fencing_generation=request.fencing_generation,
            request_id=request.request_id,
            idempotency_key_hash=payload["idempotency_key_hash"],
            accepted_route_id=payload["accepted_route_id"],
            budget_reservation_id=payload["budget_reservation_id"],
            provider_id=payload["provider_id"],
            provider_adapter=payload["provider_adapter"],
            provider_correlation_id=payload["provider_correlation_id"],
            model_id=payload["model_id"],
            registry_manifest_hash=payload["registry_manifest_hash"],
            request_hash=payload["request_hash"],
            status=status,
            provider_request_id=terminal.get("provider_request_id"),
            http_status=terminal.get("http_status"),
            failure_code=terminal.get("failure_code"),
            usage=None if usage is None else MappingProxyType(dict(usage)),
        )

    def unresolved(self) -> tuple[ProviderCallSnapshot, ...]:
        """Return calls needing recovery/reconciliation after a restart."""
        calls: list[ProviderCallSnapshot] = []
        for stream_id in self.event_store.stream_ids("provider_call"):
            events = self.event_store.read_stream("provider_call", stream_id)
            if not events or events[0].event_type != "ProviderCallIntentRecorded":
                raise ValueError("provider call stream violates the journal contract")
            payload = events[0].payload
            # Reconstruct only the recovery facts from persisted identity. A
            # ModelRequest is not recreated because it would require prompt data.
            status = "dispatching"
            terminal: Mapping[str, object] = {}
            if len(events) > 2:
                raise ValueError("provider call stream has too many events")
            if len(events) == 2:
                if events[1].event_type != "ProviderCallOutcomeRecorded":
                    raise ValueError("provider call stream has an unknown terminal event")
                terminal = events[1].payload
                status = terminal["outcome"]
            if status not in {"dispatching", "unknown"}:
                continue
            calls.append(ProviderCallSnapshot(
                stream_id=stream_id,
                run_id=payload["run_id"],
                node_id=payload["node_id"],
                attempt_id=payload["attempt_id"],
                fencing_generation=payload["fencing_generation"],
                request_id=payload["request_id"],
                idempotency_key_hash=payload["idempotency_key_hash"],
                accepted_route_id=payload["accepted_route_id"],
                budget_reservation_id=payload["budget_reservation_id"],
                provider_id=payload["provider_id"],
                provider_adapter=payload["provider_adapter"],
                provider_correlation_id=payload["provider_correlation_id"],
                model_id=payload["model_id"],
                registry_manifest_hash=payload["registry_manifest_hash"],
                request_hash=payload["request_hash"],
                status=status,
                provider_request_id=terminal.get("provider_request_id"),
                http_status=terminal.get("http_status"),
                failure_code=terminal.get("failure_code"),
                usage=None if terminal.get("usage") is None else MappingProxyType(dict(terminal["usage"])),
            ))
        return tuple(calls)


def _identity(request: ModelRequest) -> dict[str, object]:
    return {
        "run_id": request.run_id,
        "node_id": request.node_id,
        "attempt_id": request.attempt_id,
        "fencing_generation": request.fencing_generation,
        "request_id": request.request_id,
    }


def _stream_id(request: ModelRequest) -> str:
    digest = hashlib.sha256(canonical_json(_identity(request)).encode("utf-8")).hexdigest()
    return "call-" + digest


def _hash(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _safe_text(value: str) -> bool:
    return not any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)


__all__ = [
    "ProviderCallJournal",
    "ProviderCallReplayBlocked",
    "ProviderCallSnapshot",
    "SQLiteProviderCallJournal",
]
