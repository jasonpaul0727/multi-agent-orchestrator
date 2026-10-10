"""Fail-closed boundary for reconciling ambiguous external Provider calls.

Provider evidence and host-observed Attempt termination are independent facts.
This module combines them only after two injected verifiers have independently
validated their respective domains; no default implementation can settle a
call.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import re
from typing import Literal, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from orchestrator.budget.models import UsageRecord
from orchestrator.budget import BudgetLedger
from orchestrator.models.provider_calls import (
    ProviderCallJournalConflict,
    ProviderCallReconciliation,
    ProviderCallSnapshot,
    SQLiteProviderCallJournal,
)
from orchestrator.models.provider_sender import ProviderSenderTerminationReceipt
from orchestrator.validation import revalidate_model


_MAX_EVIDENCE_BYTES = 65_536
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$", re.ASCII)


class ReconciliationRejected(RuntimeError):
    """Evidence or termination proof is not sufficient to resolve a call."""


class ProviderEvidenceUnsupported(ReconciliationRejected):
    """No authoritative verifier is configured for this Provider evidence."""


class ProviderEvidenceResult(BaseModel):
    """Provider-only evidence, deliberately excluding host termination facts."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_call_stream_id: StrictStr = Field(min_length=1, max_length=256)
    provider_adapter: Literal["openai_responses", "anthropic_messages", "openai_compatible"]
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
    observed_at: datetime

    @field_validator("registry_manifest_hash", "request_hash", "evidence_digest")
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
    def validate_evidence_binding(self) -> ProviderEvidenceResult:
        if self.provider_adapter == "openai_responses":
            if (
                self.provider_correlation_id is None
                or not self.provider_correlation_id.isascii()
            ):
                raise ValueError("OpenAI Responses evidence requires an ASCII correlation id")
        elif self.provider_correlation_id is not None:
            raise ValueError("this Provider adapter cannot bind a correlation id")
        if self.effect == "not_received" and self.usage is not None:
            raise ValueError("not_received evidence cannot include usage")
        if self.effect == "received_and_charged" and self.usage is None:
            raise ValueError("received_and_charged evidence requires exact usage")
        if self.usage is not None and (
            self.usage.run_id != self.run_id
            or self.usage.reservation_id != self.budget_reservation_id
        ):
            raise ValueError("Provider evidence usage does not match Run reservation")
        if self.usage is not None and self.usage.status != "committed":
            raise ValueError("Provider evidence requires committed exact usage")
        return self


class ProviderEvidenceVerifier(Protocol):
    """Verify one bounded raw receipt against an exact persisted call."""

    def verify(
        self, call: ProviderCallSnapshot, raw_evidence: bytes
    ) -> ProviderEvidenceResult: ...


class AttemptTerminationVerifier(Protocol):
    """Prove the matching sender/Attempt/fence is no longer live."""

    def verify_stopped(self, call: ProviderCallSnapshot, receipt: object) -> str: ...


class UnavailableProviderEvidenceVerifier:
    """Default Provider evidence adapter; intentionally cannot reconcile."""

    def verify(
        self, call: ProviderCallSnapshot, raw_evidence: bytes
    ) -> ProviderEvidenceResult:
        raise ProviderEvidenceUnsupported(
            "no authoritative Provider evidence source is configured"
        )


class UnavailableAttemptTerminationVerifier:
    """Default host supervisor adapter until Attempt-bound proof is available."""

    def verify_stopped(self, call: ProviderCallSnapshot, receipt: object) -> str:
        raise ReconciliationRejected(
            "no Attempt-bound termination witness is configured"
        )


class ProviderSenderTerminationVerifier:
    """Accept the exact stored receipt for an unresolved call or proof replay."""

    def __init__(self, journal: _ReconciliationJournal | SQLiteProviderCallJournal) -> None:
        self.journal = journal

    def verify_stopped(self, call: ProviderCallSnapshot, receipt: object) -> str:
        try:
            call = _revalidate_call_snapshot(call, allow_reconciliation=True)
            candidate = revalidate_model(ProviderSenderTerminationReceipt, receipt)
            persisted = self.journal.read_call(call.stream_id)
            if persisted is None:
                raise ValueError("persisted Provider call is missing")
            persisted = _revalidate_call_snapshot(persisted, allow_reconciliation=True)
        except Exception:
            raise ReconciliationRejected(
                "persisted Provider sender receipt could not be validated"
            ) from None
        is_unresolved = call.status in {"dispatching", "unknown"}
        is_exact_proof_replay = (
            call.status in {"settlement_pending", "reconciled"}
            and call.reconciliation is not None
            and call.reconciliation.termination_receipt_hash == candidate.receipt_hash
        )
        if (
            not (is_unresolved or is_exact_proof_replay)
            or persisted != call
            or call.termination_receipt is None
            or persisted.termination_receipt is None
            or candidate != call.termination_receipt
            or candidate != persisted.termination_receipt
        ):
            raise ReconciliationRejected(
                "Provider sender receipt is not the exact persisted proof for this call"
            )
        bindings = (
            (candidate.provider_call_stream_id, call.stream_id),
            (candidate.run_id, call.run_id),
            (candidate.node_id, call.node_id),
            (candidate.attempt_id, call.attempt_id),
            (candidate.fencing_generation, call.fencing_generation),
            (candidate.accepted_route_id, call.accepted_route_id),
            (candidate.budget_reservation_id, call.budget_reservation_id),
            (candidate.provider_id, call.provider_id),
            (candidate.model_id, call.model_id),
            (candidate.registry_manifest_hash, call.registry_manifest_hash),
            (candidate.request_hash, call.request_hash),
        )
        if any(receipt_value != call_value for receipt_value, call_value in bindings):
            raise ReconciliationRejected(
                "Provider sender receipt does not match the persisted call binding"
            )
        return candidate.receipt_hash


class _ReconciliationJournal(Protocol):
    def read_call(self, stream_id: str) -> ProviderCallSnapshot | None: ...

    def _append_reconciliation(
        self,
        stream_id: str,
        proof: ProviderCallReconciliation,
        reconciled_at: datetime,
    ) -> str: ...


class ProviderReconciliationService:
    """Verify, combine, and persist evidence for one ambiguous Provider call."""

    def __init__(
        self,
        *,
        journal: _ReconciliationJournal | SQLiteProviderCallJournal,
        scheduler: object,
        evidence_verifier: ProviderEvidenceVerifier | None = None,
        termination_verifier: AttemptTerminationVerifier | None = None,
    ) -> None:
        self.journal = journal
        self.scheduler = scheduler
        self.evidence_verifier = (
            evidence_verifier
            if evidence_verifier is not None
            else UnavailableProviderEvidenceVerifier()
        )
        self.termination_verifier = (
            termination_verifier
            if termination_verifier is not None
            else UnavailableAttemptTerminationVerifier()
        )

    def reconcile(
        self,
        stream_id: str,
        raw_evidence: bytes,
        termination_receipt: object,
        reconciled_at: datetime,
    ) -> ProviderCallSnapshot:
        if not isinstance(stream_id, str) or not stream_id:
            raise ReconciliationRejected("provider call stream id is invalid")
        if not isinstance(raw_evidence, bytes):
            raise ReconciliationRejected("reconciliation requires raw evidence bytes")
        if not raw_evidence:
            raise ReconciliationRejected("provider evidence bytes cannot be empty")
        if len(raw_evidence) > _MAX_EVIDENCE_BYTES:
            raise ReconciliationRejected("provider evidence exceeds the 64 KiB bound")
        if (
            not isinstance(reconciled_at, datetime)
            or reconciled_at.tzinfo is None
            or reconciled_at.utcoffset() is None
        ):
            raise ReconciliationRejected("reconciled_at must be timezone-aware")
        try:
            call = self.journal.read_call(stream_id)
        except Exception as exc:
            raise ReconciliationRejected("provider call journal could not be read") from exc
        if call is None:
            raise ReconciliationRejected("provider call intent is missing")
        call = _revalidate_call_snapshot(call, allow_reconciliation=True)
        if call.reconciliation is not None:
            return self._replay_persisted_proof(
                call,
                raw_evidence=raw_evidence,
                termination_receipt=termination_receipt,
                reconciled_at=reconciled_at,
            )
        if call.status not in {"dispatching", "unknown"}:
            raise ReconciliationRejected("only unresolved Provider calls may be reconciled")
        if call.provider_adapter != "openai_responses" or not call.provider_correlation_id:
            raise ProviderEvidenceUnsupported(
                "no exact-evidence verifier is enabled for this Provider adapter"
            )
        expected_currency = _require_unknown_attempt(
            self.scheduler, call, reconciled_at=reconciled_at
        )

        try:
            provider_result = self.evidence_verifier.verify(call, raw_evidence)
            provider_result = revalidate_model(ProviderEvidenceResult, provider_result)
        except ProviderEvidenceUnsupported:
            raise ProviderEvidenceUnsupported(
                "no authoritative Provider evidence source is available"
            ) from None
        except Exception:
            raise ReconciliationRejected(
                "Provider evidence verifier rejected the receipt"
            ) from None
        if not isinstance(provider_result, ProviderEvidenceResult):
            raise ReconciliationRejected("Provider evidence verifier returned an invalid result")
        if (
            provider_result.provider_adapter != "openai_responses"
            or not provider_result.provider_correlation_id
        ):
            raise ProviderEvidenceUnsupported(
                "Provider evidence result is not bound to a supported OpenAI Responses route"
            )
        if any(
            getattr(provider_result, name) != expected
            for name, expected in _provider_call_binding(call).items()
        ):
            raise ReconciliationRejected("Provider evidence identity binding does not match call")
        expected_evidence_digest = "sha256:" + hashlib.sha256(raw_evidence).hexdigest()
        if provider_result.evidence_digest != expected_evidence_digest:
            raise ReconciliationRejected("Provider evidence digest does not match raw evidence")
        if provider_result.usage is not None:
            if provider_result.usage.currency != expected_currency:
                raise ReconciliationRejected(
                    "Provider usage currency does not match the Run reservation"
                )
            normalized_usage = revalidate_model(
                UsageRecord,
                provider_result.usage.model_copy(
                    update={"settlement_key": f"provider-reconcile-{call.stream_id}"}
                ),
            )
            provider_result = provider_result.model_copy(update={"usage": normalized_usage})

        try:
            termination_digest = self.termination_verifier.verify_stopped(
                call, termination_receipt
            )
        except Exception:
            raise ReconciliationRejected(
                "Attempt termination witness was rejected"
            ) from None
        if (
            not isinstance(termination_digest, str)
            or not _SHA256.fullmatch(termination_digest)
        ):
            raise ReconciliationRejected("Attempt termination verifier returned an invalid digest")

        proof_fields = provider_result.model_dump(mode="python")
        proof_fields["termination_receipt_hash"] = termination_digest
        try:
            proof = revalidate_model(
                ProviderCallReconciliation,
                ProviderCallReconciliation.model_validate(proof_fields),
            )
        except Exception as exc:
            raise ReconciliationRejected(
                "verified Provider evidence cannot form a final proof"
            ) from exc
        try:
            self.journal._append_reconciliation(stream_id, proof, reconciled_at)
            persisted = self.journal.read_call(stream_id)
        except ProviderCallJournalConflict as exc:
            raise ReconciliationRejected(
                "Provider reconciliation conflicts with journal state"
            ) from exc
        except Exception as exc:
            raise ReconciliationRejected(
                "Provider reconciliation could not be persisted"
            ) from exc
        if (
            persisted is None
            or persisted.reconciliation != proof
            or persisted.reconciled_at != reconciled_at
        ):
            raise ReconciliationRejected(
                "persisted Provider reconciliation did not match proof"
            )
        self._apply_one_settlement(persisted)
        settled = self.journal.read_call(stream_id)
        if settled is None or settled.status != "reconciled":
            raise ReconciliationRejected("Scheduler settlement marker was not persisted")
        return _revalidate_call_snapshot(settled, allow_reconciliation=True)

    def _replay_persisted_proof(
        self,
        call: ProviderCallSnapshot,
        *,
        raw_evidence: bytes,
        termination_receipt: object,
        reconciled_at: datetime,
    ) -> ProviderCallSnapshot:
        proof = call.reconciliation
        if proof is None or call.reconciled_at is None:
            raise ReconciliationRejected("persisted Provider proof is incomplete")
        evidence_digest = "sha256:" + hashlib.sha256(raw_evidence).hexdigest()
        if evidence_digest != proof.evidence_digest or reconciled_at != call.reconciled_at:
            raise ReconciliationRejected("Provider reconciliation conflicts with persisted proof")
        try:
            termination_digest = self.termination_verifier.verify_stopped(
                call, termination_receipt
            )
        except Exception:
            raise ReconciliationRejected(
                "Attempt termination witness was rejected"
            ) from None
        if termination_digest != proof.termination_receipt_hash:
            raise ReconciliationRejected("Provider reconciliation conflicts with persisted proof")
        if not call.settlement_applied:
            self._apply_one_settlement(call)
            settled = self.journal.read_call(call.stream_id)
            if settled is None:
                raise ReconciliationRejected("provider call disappeared after Scheduler settlement")
            call = _revalidate_call_snapshot(settled, allow_reconciliation=True)
        return call

    def apply_pending_settlements(self) -> tuple[str, ...]:
        """Apply only already-persisted proofs; never queries either verifier."""
        try:
            pending = self.journal.pending_settlements()
        except Exception:
            raise ReconciliationRejected("pending Provider settlements could not be read") from None
        applied: list[str] = []
        for value in pending:
            call = _revalidate_call_snapshot(value, allow_reconciliation=True)
            if call.status != "settlement_pending" or call.reconciliation is None:
                raise ReconciliationRejected("pending Provider settlement has no persisted proof")
            self._apply_one_settlement(call)
            applied.append(call.stream_id)
        return tuple(applied)

    def _apply_one_settlement(self, call: ProviderCallSnapshot) -> None:
        proof = call.reconciliation
        if proof is None or call.reconciled_at is None:
            raise ReconciliationRejected("Provider reconciliation proof is missing")
        try:
            _require_scheduler_evidence(self.scheduler, call, call.reconciled_at)
            usage = proof.usage
            self.scheduler.reconcile_attempt(
                run_id=call.run_id,
                node_id=call.node_id,
                attempt_id=call.attempt_id,
                fencing_generation=call.fencing_generation,
                reconciled_at=call.reconciled_at,
                outcome="failed",
                usage=usage,
                known_no_effect=proof.effect == "not_received",
            )
            event_id = call.reconciliation_event_id
            if not isinstance(event_id, str) or not event_id:
                raise ReconciliationRejected("Provider reconciliation event id is missing")
            self.journal._record_scheduler_settlement(call.stream_id, event_id)
        except ReconciliationRejected:
            raise
        except Exception:
            raise ReconciliationRejected(
                "verified Provider outcome could not be settled through Scheduler"
            ) from None


def _provider_call_binding(call: ProviderCallSnapshot) -> dict[str, object]:
    binding = {
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
    if call.provider_request_id is not None:
        binding["provider_request_id"] = call.provider_request_id
    return binding


def _revalidate_call_snapshot(
    value: object, *, allow_reconciliation: bool = False
) -> ProviderCallSnapshot:
    if not isinstance(value, ProviderCallSnapshot):
        raise ReconciliationRejected("provider call journal returned an invalid snapshot")
    call = ProviderCallSnapshot(
        stream_id=value.stream_id,
        run_id=value.run_id,
        node_id=value.node_id,
        attempt_id=value.attempt_id,
        fencing_generation=value.fencing_generation,
        request_id=value.request_id,
        idempotency_key_hash=value.idempotency_key_hash,
        accepted_route_id=value.accepted_route_id,
        budget_reservation_id=value.budget_reservation_id,
        provider_id=value.provider_id,
        provider_adapter=value.provider_adapter,
        provider_correlation_id=value.provider_correlation_id,
        model_id=value.model_id,
        registry_manifest_hash=value.registry_manifest_hash,
        request_hash=value.request_hash,
        status=value.status,
        provider_request_id=value.provider_request_id,
        http_status=value.http_status,
        failure_code=value.failure_code,
        usage=value.usage,
        termination_receipt=value.termination_receipt,
        reconciliation=value.reconciliation,
        reconciliation_event_id=value.reconciliation_event_id,
        reconciled_at=value.reconciled_at,
        settlement_applied=value.settlement_applied,
    )
    identifiers = (
        call.stream_id, call.run_id, call.node_id, call.attempt_id, call.request_id,
        call.accepted_route_id,
        call.budget_reservation_id,
        call.provider_id,
        call.model_id,
    )
    if any(
        not isinstance(identifier, str)
        or not identifier
        or len(identifier) > 256
        or any(
            ord(character) < 0x20 or ord(character) == 0x7F
            for character in identifier
        )
        for identifier in identifiers
    ):
        raise ReconciliationRejected("provider call snapshot has invalid identifiers")
    if (
        isinstance(call.fencing_generation, bool)
        or not isinstance(call.fencing_generation, int)
        or call.fencing_generation < 1
        or (call.provider_adapter is not None and (
            not isinstance(call.provider_adapter, str)
            or call.provider_adapter not in (
            "openai_responses", "anthropic_messages", "openai_compatible"
            )
        ))
        or not isinstance(call.status, str)
        or call.status not in (
            "dispatching", "not_sent", "known_failure", "known_success", "unknown",
            "settlement_pending", "reconciled",
        )
    ):
        raise ReconciliationRejected("provider call snapshot has invalid state")
    if any(
        not isinstance(value, str) or not _SHA256.fullmatch(value)
        for value in (
            call.idempotency_key_hash,
            call.registry_manifest_hash,
            call.request_hash,
        )
    ):
        raise ReconciliationRejected("provider call snapshot has invalid content hashes")
    correlation_id = call.provider_correlation_id
    if call.provider_adapter == "openai_responses":
        if (
            not isinstance(correlation_id, str)
            or not correlation_id
            or not correlation_id.isascii()
            or len(correlation_id) > 512
            or any(
                ord(character) < 0x20 or ord(character) == 0x7F
                for character in correlation_id
            )
        ):
            raise ReconciliationRejected("provider call snapshot has invalid correlation binding")
    elif correlation_id is not None:
        raise ReconciliationRejected("provider call snapshot has unsupported correlation binding")
    has_proof = call.reconciliation is not None
    if not allow_reconciliation and (
        has_proof
        or call.reconciliation_event_id is not None
        or call.reconciled_at is not None
        or call.settlement_applied
    ):
        raise ReconciliationRejected("provider call snapshot already carries reconciliation state")
    if has_proof:
        try:
            proof = revalidate_model(ProviderCallReconciliation, call.reconciliation)
        except Exception:
            raise ReconciliationRejected("provider call snapshot proof is invalid") from None
        if any(
            getattr(proof, name) != expected
            for name, expected in _provider_call_binding(call).items()
        ):
            raise ReconciliationRejected("provider call snapshot proof binding is invalid")
        if proof.usage is not None and proof.usage.settlement_key != (
            f"provider-reconcile-{call.stream_id}"
        ):
            raise ReconciliationRejected("provider call snapshot settlement key is invalid")
        if (
            call.status not in {"settlement_pending", "reconciled"}
            or not isinstance(call.reconciliation_event_id, str)
            or not call.reconciliation_event_id
            or call.reconciled_at is None
            or call.reconciled_at.tzinfo is None
            or call.reconciled_at.utcoffset() is None
            or (call.status == "reconciled") != call.settlement_applied
        ):
            raise ReconciliationRejected("provider call snapshot settlement state is invalid")
    elif (
        call.status in {"settlement_pending", "reconciled"}
        or call.reconciliation_event_id is not None
        or call.reconciled_at is not None
        or call.settlement_applied
    ):
        raise ReconciliationRejected("provider call snapshot settlement state is incomplete")
    return call


def _require_unknown_attempt(
    scheduler: object, call: ProviderCallSnapshot, *, reconciled_at: datetime
) -> str:
    try:
        state = scheduler.lifecycle.replay(call.run_id)
        node = state.node(call.node_id)
        attempt = next(
            (item for item in node.attempts if item.attempt_id == call.attempt_id), None
        )
        if attempt is None or attempt.status != "outcome_unknown":
            raise ReconciliationRejected(
                "Scheduler Attempt must be OutcomeUnknown before Provider reconciliation"
            )
        if (
            attempt.fencing_generation != call.fencing_generation
            or attempt.reservation_id != call.budget_reservation_id
            or attempt.model_id != call.model_id
            or attempt.provider_id != call.provider_id
        ):
            raise ReconciliationRejected("Scheduler Attempt binding does not match Provider call")
        _require_scheduler_evidence(scheduler, call, reconciled_at)
        reservation = BudgetLedger(scheduler.event_store).get_reservation(
            call.budget_reservation_id, run_id=call.run_id
        )
        return reservation.currency
    except ReconciliationRejected:
        raise
    except Exception:
        raise ReconciliationRejected("Scheduler Attempt state could not be verified") from None


def _require_scheduler_evidence(
    scheduler: object, call: ProviderCallSnapshot, reconciled_at: datetime
) -> tuple[datetime, object]:
    try:
        events = scheduler.event_store.read_stream("scheduler", "global")
    except Exception:
        raise ReconciliationRejected("Scheduler event evidence could not be read") from None
    matching = lambda event: (
        event.payload.get("run_id") == call.run_id
        and event.payload.get("node_id") == call.node_id
        and event.payload.get("attempt_id") == call.attempt_id
        and event.fencing_generation == call.fencing_generation
    )
    unknown = next(
        (event for event in events if event.event_type == "AttemptOutcomeUnknown" and matching(event)),
        None,
    )
    accepted = next(
        (
            event
            for event in events
            if event.event_type == "RoutingDecisionAccepted" and matching(event)
        ),
        None,
    )
    if unknown is None or accepted is None:
        raise ReconciliationRejected("Scheduler Attempt lacks matching OutcomeUnknown evidence")
    try:
        unknown_at = datetime.fromisoformat(unknown.payload["completed_at"])
        route = accepted.payload["accepted_route"]
        if (
            route["run_id"] != call.run_id
            or route["node_id"] != call.node_id
            or route["attempt_id"] != call.attempt_id
            or route["fencing_generation"] != call.fencing_generation
            or route["budget_reservation_id"] != call.budget_reservation_id
            or route["provider_id"] != call.provider_id
            or route["model_id"] != call.model_id
            or route["decision_id"] != call.accepted_route_id
            or route["registry_manifest_hash"] != call.registry_manifest_hash
            or unknown_at.tzinfo is None
            or unknown_at.utcoffset() is None
        ):
            raise ValueError("accepted route binding mismatch")
    except Exception:
        raise ReconciliationRejected("Scheduler Attempt evidence is malformed") from None
    if reconciled_at < unknown_at:
        raise ReconciliationRejected("reconciliation timestamp predates OutcomeUnknown")
    return unknown_at, accepted


__all__ = [
    "AttemptTerminationVerifier",
    "ProviderEvidenceResult",
    "ProviderEvidenceUnsupported",
    "ProviderEvidenceVerifier",
    "ProviderReconciliationService",
    "ReconciliationRejected",
    "UnavailableAttemptTerminationVerifier",
    "UnavailableProviderEvidenceVerifier",
]
