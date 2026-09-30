"""Durable, replay-safe journal for external Provider model calls."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import re
from types import MappingProxyType
from typing import Literal, Mapping, Protocol

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.budget.models import UsageRecord
from orchestrator.config.models import ProviderAdapter
from orchestrator.identifiers import new_id
from orchestrator.models.gateway import ModelRequest, TokenUsage
from orchestrator.persistence import EventDraft, IdempotencyConflict, SQLiteEventStore, StaleStream
from orchestrator.persistence.sqlite_event_store import canonical_json


ProviderCallStatus = Literal[
    "dispatching", "not_sent", "known_failure", "known_success", "unknown",
    "settlement_pending", "reconciled",
]
_PROVIDER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$", re.ASCII)


class ProviderCallJournalConflict(RuntimeError):
    """A Provider reconciliation stream already contains a conflicting claim."""


class ProviderCallReconciliation(BaseModel):
    """Immutable combined Provider evidence and host-termination proof."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_call_stream_id: StrictStr = Field(min_length=1, max_length=256)
    provider_adapter: ProviderAdapter
    provider_correlation_id: StrictStr | None = Field(default=None, max_length=512)
    run_id: StrictStr = Field(min_length=1, max_length=128)
    node_id: StrictStr = Field(min_length=1, max_length=128)
    attempt_id: StrictStr = Field(min_length=1, max_length=128)
    fencing_generation: StrictInt = Field(ge=1)
    provider_id: StrictStr = Field(min_length=1, max_length=128)
    model_id: StrictStr = Field(min_length=1, max_length=128)
    accepted_route_id: StrictStr = Field(min_length=1, max_length=256)
    budget_reservation_id: StrictStr = Field(min_length=1, max_length=256)
    registry_manifest_hash: StrictStr
    request_hash: StrictStr
    effect: Literal["not_received", "received_and_charged"]
    usage: UsageRecord | None = None
    provider_request_id: StrictStr | None = Field(default=None, max_length=256)
    evidence_source: Literal["provider_signed_receipt", "provider_authoritative_api"]
    evidence_digest: StrictStr
    termination_receipt_hash: StrictStr
    observed_at: datetime

    @field_validator("registry_manifest_hash", "request_hash", "evidence_digest", "termination_receipt_hash")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("value must be a SHA-256 content hash")
        return value

    @field_validator("provider_correlation_id", "provider_request_id")
    @classmethod
    def validate_optional_provider_ids(cls, value: str | None) -> str | None:
        if value is not None and (
            not value or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        ):
            raise ValueError("Provider identifier must be non-empty safe text")
        return value

    @field_validator("observed_at")
    @classmethod
    def require_aware_observation_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_evidence_binding(self) -> ProviderCallReconciliation:
        if self.provider_adapter == "openai_responses":
            if (
                self.provider_correlation_id is None
                or not self.provider_correlation_id.isascii()
                or len(self.provider_correlation_id) > 512
            ):
                raise ValueError("OpenAI Responses reconciliation requires an ASCII correlation id")
        elif self.provider_correlation_id is not None:
            raise ValueError("this Provider adapter cannot bind a correlation id")
        if self.effect == "not_received" and self.usage is not None:
            raise ValueError("not_received reconciliation cannot include usage")
        if self.effect == "received_and_charged" and self.usage is None:
            raise ValueError("received_and_charged reconciliation requires exact usage")
        if self.usage is not None and (
            self.usage.run_id != self.run_id
            or self.usage.reservation_id != self.budget_reservation_id
        ):
            raise ValueError("reconciliation usage does not match Run reservation")
        return self


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
    reconciliation: ProviderCallReconciliation | None = None
    reconciliation_event_id: str | None = None
    reconciled_at: datetime | None = None
    settlement_applied: bool = False


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
        prior = self.read(request)
        if prior is None:
            raise ValueError("provider call outcome requires a prior durable intent")
        if prior.status != "dispatching":
            raise ValueError("provider call outcome conflicts with a terminal or reconciled call")
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
        snapshot = self.read_call(_stream_id(request))
        if snapshot is None:
            return None
        if (
            snapshot.run_id != request.run_id
            or snapshot.node_id != request.node_id
            or snapshot.attempt_id != request.attempt_id
            or snapshot.fencing_generation != request.fencing_generation
        ):
            raise ValueError("provider call stream binding does not match request")
        expected_binding = {
            "request_id": request.request_id,
            "idempotency_key_hash": _hash(request.idempotency_key.encode("utf-8")),
            "accepted_route_id": request.accepted_route.decision_id,
            "budget_reservation_id": request.budget_reservation_id,
            "provider_id": request.accepted_route.provider_id,
            "model_id": request.model_id,
            "registry_manifest_hash": request.accepted_route.registry_manifest_hash,
        }
        events = self.event_store.read_stream("provider_call", _stream_id(request))
        payload = events[0].payload
        if any(payload.get(key) != value for key, value in expected_binding.items()):
            raise ValueError("provider call stream binding does not match request")
        return snapshot

    def read_call(self, stream_id: str) -> ProviderCallSnapshot | None:
        """Project a provider-call stream without reconstructing prompt data."""
        if not isinstance(stream_id, str) or not stream_id:
            raise ValueError("provider call stream id is invalid")
        events = self.event_store.read_stream("provider_call", stream_id)
        if not events:
            return None
        if len(events) > 4 or events[0].event_type != "ProviderCallIntentRecorded":
            raise ValueError("provider call stream violates the journal contract")
        intent = events[0]
        payload = intent.payload
        outcome_event = next(
            (event for event in events[1:] if event.event_type == "ProviderCallOutcomeRecorded"),
            None,
        )
        reconciliation_event = next(
            (event for event in events[1:] if event.event_type == "ProviderCallReconciliationRecorded"),
            None,
        )
        settlement_event = next(
            (event for event in events[1:] if event.event_type == "ProviderCallSchedulerSettlementApplied"),
            None,
        )
        terminal = {} if outcome_event is None else outcome_event.payload
        proof: ProviderCallReconciliation | None = None
        reconciled_at: datetime | None = None
        if reconciliation_event is not None:
            proof_payload = dict(reconciliation_event.payload)
            reconciled_at_value = proof_payload.pop("reconciled_at", None)
            try:
                proof = ProviderCallReconciliation.model_validate(proof_payload)
                reconciled_at = _parse_aware_datetime(reconciled_at_value, "reconciled_at")
            except (TypeError, ValueError) as exc:
                raise ValueError("provider call reconciliation projection is invalid") from exc
        if reconciliation_event is not None:
            status: ProviderCallStatus = "reconciled" if settlement_event is not None else "settlement_pending"
        else:
            status = "dispatching" if not terminal else terminal["outcome"]
        usage = terminal.get("usage")
        return ProviderCallSnapshot(
            stream_id=intent.stream_id,
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
            usage=None if usage is None else MappingProxyType(dict(usage)),
            reconciliation=proof,
            reconciliation_event_id=None if reconciliation_event is None else reconciliation_event.event_id,
            reconciled_at=reconciled_at,
            settlement_applied=settlement_event is not None,
        )

    def unresolved(self) -> tuple[ProviderCallSnapshot, ...]:
        """Return calls needing recovery/reconciliation after a restart."""
        calls: list[ProviderCallSnapshot] = []
        for stream_id in self.event_store.stream_ids("provider_call"):
            snapshot = self.read_call(stream_id)
            if snapshot is None:
                raise ValueError("provider call stream disappeared during recovery scan")
            if snapshot.status not in {"dispatching", "unknown"}:
                continue
            calls.append(snapshot)
        return tuple(calls)

    def pending_settlements(self) -> tuple[ProviderCallSnapshot, ...]:
        """Return persisted reconciliation proofs not yet applied to Scheduler."""
        pending = []
        for stream_id in self.event_store.stream_ids("provider_call"):
            snapshot = self.read_call(stream_id)
            if snapshot is None:
                raise ValueError("provider call stream disappeared during settlement scan")
            if snapshot.status == "settlement_pending":
                pending.append(snapshot)
        return tuple(pending)

    def _append_reconciliation(
        self,
        stream_id: str,
        proof: ProviderCallReconciliation,
        reconciled_at: datetime,
    ) -> str:
        if not isinstance(proof, ProviderCallReconciliation):
            proof = ProviderCallReconciliation.model_validate(proof)
        _require_aware_datetime(reconciled_at, "reconciled_at")
        snapshot = self.read_call(stream_id)
        if snapshot is None:
            raise ProviderCallJournalConflict("provider call intent is missing")
        _assert_proof_matches_call(snapshot, proof)
        if snapshot.reconciliation is not None:
            if snapshot.reconciliation == proof and snapshot.reconciled_at == reconciled_at:
                assert snapshot.reconciliation_event_id is not None
                return snapshot.reconciliation_event_id
            raise ProviderCallJournalConflict("provider reconciliation conflicts with prior proof")
        if snapshot.status not in {"dispatching", "unknown"}:
            raise ProviderCallJournalConflict("terminal provider call cannot be reconciled")
        payload = {
            **_call_binding(snapshot),
            "provider_request_id": proof.provider_request_id,
            "effect": proof.effect,
            "usage": None if proof.usage is None else proof.usage.model_dump(mode="json"),
            "evidence_source": proof.evidence_source,
            "evidence_digest": proof.evidence_digest,
            "termination_receipt_hash": proof.termination_receipt_hash,
            "observed_at": proof.observed_at.isoformat(),
            "reconciled_at": reconciled_at.isoformat(),
        }
        events = self.event_store.read_stream("provider_call", stream_id)
        try:
            stored = self.event_store.append(
                "provider_call",
                stream_id,
                expected_version=len(events),
                events=[EventDraft(
                    "ProviderCallReconciliationRecorded",
                    payload,
                    run_id=snapshot.run_id,
                    node_id=snapshot.node_id,
                    attempt_id=snapshot.attempt_id,
                    fencing_generation=snapshot.fencing_generation,
                    causation_id=snapshot.accepted_route_id,
                )],
                idempotency_key="provider-call-reconciliation:" + stream_id,
            )
            return stored[0].event_id
        except (StaleStream, IdempotencyConflict) as exc:
            latest = self.read_call(stream_id)
            if (
                latest is not None
                and latest.reconciliation == proof
                and latest.reconciled_at == reconciled_at
                and latest.reconciliation_event_id is not None
            ):
                return latest.reconciliation_event_id
            raise ProviderCallJournalConflict(
                "provider reconciliation lost a concurrent append or conflicts with prior proof"
            ) from exc

    def _record_scheduler_settlement(
        self, stream_id: str, reconciliation_event_id: str
    ) -> None:
        snapshot = self.read_call(stream_id)
        if snapshot is None or snapshot.reconciliation is None:
            raise ProviderCallJournalConflict("provider reconciliation proof is missing")
        if snapshot.reconciliation_event_id != reconciliation_event_id:
            raise ProviderCallJournalConflict("scheduler settlement conflicts with reconciliation event")
        if snapshot.settlement_applied:
            return
        payload = {
            **_call_binding(snapshot),
            "reconciliation_event_id": reconciliation_event_id,
        }
        events = self.event_store.read_stream("provider_call", stream_id)
        try:
            self.event_store.append(
                "provider_call",
                stream_id,
                expected_version=len(events),
                events=[EventDraft(
                    "ProviderCallSchedulerSettlementApplied",
                    payload,
                    run_id=snapshot.run_id,
                    node_id=snapshot.node_id,
                    attempt_id=snapshot.attempt_id,
                    fencing_generation=snapshot.fencing_generation,
                    causation_id=reconciliation_event_id,
                )],
                idempotency_key="provider-call-scheduler-settlement:" + stream_id,
            )
        except (StaleStream, IdempotencyConflict) as exc:
            latest = self.read_call(stream_id)
            if (
                latest is not None
                and latest.settlement_applied
                and latest.reconciliation_event_id == reconciliation_event_id
            ):
                return
            raise ProviderCallJournalConflict(
                "scheduler settlement lost a concurrent append or conflicts with prior marker"
            ) from exc


def _call_binding(call: ProviderCallSnapshot) -> dict[str, object]:
    return {
        "provider_call_stream_id": call.stream_id,
        "provider_adapter": call.provider_adapter,
        "provider_correlation_id": call.provider_correlation_id,
        "run_id": call.run_id,
        "node_id": call.node_id,
        "attempt_id": call.attempt_id,
        "fencing_generation": call.fencing_generation,
        "provider_id": call.provider_id,
        "model_id": call.model_id,
        "accepted_route_id": call.accepted_route_id,
        "budget_reservation_id": call.budget_reservation_id,
        "registry_manifest_hash": call.registry_manifest_hash,
        "request_hash": call.request_hash,
    }


def _assert_proof_matches_call(
    call: ProviderCallSnapshot, proof: ProviderCallReconciliation
) -> None:
    expected = _call_binding(call)
    if any(getattr(proof, name) != value for name, value in expected.items()):
        raise ProviderCallJournalConflict("Provider reconciliation identity binding does not match call")


def _require_aware_datetime(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _parse_aware_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp") from exc
    _require_aware_datetime(parsed, field_name)
    return parsed


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
    "ProviderCallJournalConflict",
    "ProviderCallReplayBlocked",
    "ProviderCallReconciliation",
    "ProviderCallSnapshot",
    "SQLiteProviderCallJournal",
]
