from dataclasses import replace
from datetime import datetime, timezone
import hashlib

import pytest

from orchestrator.budget.models import UsageRecord
from orchestrator.models.provider_calls import (
    ProviderCallReconciliation,
    ProviderCallSnapshot,
)
from orchestrator.provider_reconciliation import (
    ProviderEvidenceResult,
    ProviderEvidenceUnsupported,
    ProviderReconciliationService,
    ReconciliationRejected,
    UnavailableAttemptTerminationVerifier,
    UnavailableProviderEvidenceVerifier,
)


def _call(**overrides):
    values = {
        "stream_id": "call-" + "a" * 64,
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "fencing_generation": 1,
        "request_id": "request-1",
        "idempotency_key_hash": "sha256:" + "b" * 64,
        "accepted_route_id": "decision-1",
        "budget_reservation_id": "reservation-1",
        "provider_id": "primary",
        "provider_adapter": "openai_responses",
        "provider_correlation_id": "maestro-correlation-1",
        "model_id": "model-1",
        "registry_manifest_hash": "sha256:" + "c" * 64,
        "request_hash": "sha256:" + "d" * 64,
        "status": "unknown",
    }
    values.update(overrides)
    return ProviderCallSnapshot(**values)


def _evidence_result(call, **overrides):
    values = {
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
        "effect": "not_received",
        "usage": None,
        "provider_request_id": None,
        "evidence_source": "provider_signed_receipt",
        "evidence_digest": "sha256:" + hashlib.sha256(
            b"authenticated receipt bytes"
        ).hexdigest(),
        "observed_at": datetime(2026, 9, 29, 12, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return ProviderEvidenceResult(**values)


class _Journal:
    def __init__(self, call):
        self.call = call
        self.appended = []

    def read_call(self, stream_id):
        assert stream_id == self.call.stream_id
        return self.call

    def _append_reconciliation(self, stream_id, proof, reconciled_at):
        assert stream_id == self.call.stream_id
        self.appended.append((proof, reconciled_at))
        self.call = replace(
            self.call,
            status="settlement_pending",
            reconciliation=proof,
            reconciliation_event_id="reconciliation-event-1",
            reconciled_at=reconciled_at,
        )
        return "reconciliation-event-1"


class FakeProviderEvidenceVerifier:
    def __init__(self, evidence_result):
        self.evidence_result = evidence_result
        self.raw_evidence = []

    def verify(self, call, raw_evidence):
        assert call.status in {"dispatching", "unknown"}
        self.raw_evidence.append(raw_evidence)
        return self.evidence_result


class FakeAttemptTerminationVerifier:
    def __init__(self, expected_digest="sha256:" + "f" * 64):
        self.expected_digest = expected_digest
        self.calls = []

    def verify_stopped(self, call, receipt):
        self.calls.append((call, receipt))
        return self.expected_digest


def _service(journal, evidence_verifier, termination_verifier):
    return ProviderReconciliationService(
        journal=journal,
        scheduler=object(),
        evidence_verifier=evidence_verifier,
        termination_verifier=termination_verifier,
    )


def test_unavailable_provider_evidence_verifier_leaves_call_unknown():
    call = _call()
    journal = _Journal(call)
    service = _service(
        journal,
        UnavailableProviderEvidenceVerifier(),
        FakeAttemptTerminationVerifier(),
    )

    with pytest.raises(ProviderEvidenceUnsupported):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"{}",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert journal.appended == []
    assert call.status == "unknown"


@pytest.mark.parametrize("evidence_size", [65_537])
def test_oversized_provider_evidence_is_rejected_before_verification(evidence_size):
    call = _call()
    journal = _Journal(call)
    evidence_verifier = FakeProviderEvidenceVerifier(_evidence_result(call))
    termination_verifier = FakeAttemptTerminationVerifier()
    service = _service(journal, evidence_verifier, termination_verifier)

    with pytest.raises(ReconciliationRejected, match="64 KiB"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"x" * evidence_size,
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert evidence_verifier.raw_evidence == []
    assert termination_verifier.calls == []
    assert journal.appended == []


@pytest.mark.parametrize("proof_kind", ["provider_result", "final_proof"])
def test_caller_supplied_proof_objects_cannot_bypass_raw_evidence_verifier(proof_kind):
    call = _call()
    journal = _Journal(call)
    evidence_verifier = FakeProviderEvidenceVerifier(_evidence_result(call))
    service = _service(
        journal,
        evidence_verifier,
        FakeAttemptTerminationVerifier(),
    )
    proof_object = _evidence_result(call)
    if proof_kind == "final_proof":
        proof_object = ProviderCallReconciliation(
            **proof_object.model_dump(),
            termination_receipt_hash="sha256:" + "f" * 64,
        )

    with pytest.raises(ReconciliationRejected, match="raw evidence bytes"):
        service.reconcile(
            call.stream_id,
            raw_evidence=proof_object,
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert evidence_verifier.raw_evidence == []
    assert journal.appended == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"provider_id": "other-provider"},
        {"request_hash": "sha256:" + "f" * 64},
        {"fencing_generation": 2},
        {"attempt_id": "other-attempt"},
    ],
)
def test_evidence_result_must_match_complete_call_binding_before_termination(overrides):
    call = _call()
    journal = _Journal(call)
    evidence = _evidence_result(call, **overrides)
    termination_verifier = FakeAttemptTerminationVerifier()
    service = _service(
        journal,
        FakeProviderEvidenceVerifier(evidence),
        termination_verifier,
    )

    with pytest.raises(ReconciliationRejected, match="binding"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"authenticated receipt bytes",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert termination_verifier.calls == []
    assert journal.appended == []


def test_evidence_digest_must_match_the_bounded_raw_receipt_bytes():
    call = _call()
    journal = _Journal(call)
    evidence = _evidence_result(call, evidence_digest="sha256:" + "a" * 64)
    termination_verifier = FakeAttemptTerminationVerifier()
    service = _service(
        journal,
        FakeProviderEvidenceVerifier(evidence),
        termination_verifier,
    )

    with pytest.raises(ReconciliationRejected, match="digest"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"authenticated receipt bytes",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert termination_verifier.calls == []
    assert journal.appended == []


def test_missing_attempt_termination_witness_keeps_provider_call_unresolved():
    call = _call()
    journal = _Journal(call)
    service = _service(
        journal,
        FakeProviderEvidenceVerifier(_evidence_result(call)),
        UnavailableAttemptTerminationVerifier(),
    )

    with pytest.raises(ReconciliationRejected, match="termination"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"authenticated receipt bytes",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert journal.appended == []


def test_termination_verifier_must_return_a_sha256_receipt_digest():
    call = _call()
    journal = _Journal(call)
    service = _service(
        journal,
        FakeProviderEvidenceVerifier(_evidence_result(call)),
        FakeAttemptTerminationVerifier("not-a-digest"),
    )

    with pytest.raises(ReconciliationRejected, match="invalid digest"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"authenticated receipt bytes",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert journal.appended == []


def test_known_terminal_provider_call_cannot_be_reconciled():
    call = _call(status="known_success")
    journal = _Journal(call)
    evidence_verifier = FakeProviderEvidenceVerifier(_evidence_result(call))
    termination_verifier = FakeAttemptTerminationVerifier()
    service = _service(journal, evidence_verifier, termination_verifier)

    with pytest.raises(ReconciliationRejected, match="unresolved"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"authenticated receipt bytes",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert evidence_verifier.raw_evidence == []
    assert termination_verifier.calls == []
    assert journal.appended == []


def test_malformed_call_snapshot_is_rejected_before_provider_lookup():
    call = _call(provider_correlation_id="not-ascii-é")
    journal = _Journal(call)
    evidence_verifier = FakeProviderEvidenceVerifier(_evidence_result(_call()))
    service = _service(
        journal,
        evidence_verifier,
        FakeAttemptTerminationVerifier(),
    )

    with pytest.raises(ReconciliationRejected, match="correlation binding"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"authenticated receipt bytes",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert evidence_verifier.raw_evidence == []
    assert journal.appended == []


def test_verified_provider_only_evidence_is_combined_with_host_digest():
    call = _call()
    journal = _Journal(call)
    evidence = _evidence_result(call)
    termination_digest = "sha256:" + "f" * 64
    termination_verifier = FakeAttemptTerminationVerifier(termination_digest)
    service = _service(
        journal,
        FakeProviderEvidenceVerifier(evidence),
        termination_verifier,
    )
    reconciled_at = datetime(2026, 9, 29, 13, tzinfo=timezone.utc)

    persisted = service.reconcile(
        call.stream_id,
        raw_evidence=b"authenticated receipt bytes",
        termination_receipt=object(),
        reconciled_at=reconciled_at,
    )

    assert persisted.status == "settlement_pending"
    assert len(journal.appended) == 1
    proof, persisted_at = journal.appended[0]
    assert proof.provider_call_stream_id == call.stream_id
    assert proof.evidence_digest == evidence.evidence_digest
    assert proof.termination_receipt_hash == termination_digest
    assert persisted_at == reconciled_at
    assert termination_verifier.calls[0][0] == call


def test_provider_evidence_rejects_aggregate_only_or_malformed_usage():
    call = _call()
    usage = UsageRecord(
        reservation_id=call.budget_reservation_id,
        run_id=call.run_id,
        settlement_key="settlement-1",
        currency="USD",
    )

    with pytest.raises(ValueError, match="usage"):
        _evidence_result(call, effect="received_and_charged", usage=None)
    with pytest.raises(ValueError, match="usage"):
        _evidence_result(call, effect="not_received", usage=usage)
    with pytest.raises(ValueError, match="usage"):
        _evidence_result(
            call,
            effect="received_and_charged",
            usage=usage.model_copy(update={"run_id": "other-run"}),
        )
    with pytest.raises(ValueError, match="committed"):
        _evidence_result(
            call,
            effect="received_and_charged",
            usage=UsageRecord(
                reservation_id=call.budget_reservation_id,
                run_id=call.run_id,
                settlement_key="settlement-observed",
                currency="USD",
                status="observed",
            ),
        )


def test_generic_compatible_provider_has_no_enabled_exact_evidence_verifier():
    call = _call(provider_adapter="openai_compatible", provider_correlation_id=None)
    journal = _Journal(call)
    verifier = FakeProviderEvidenceVerifier(_evidence_result(call))
    service = _service(journal, verifier, FakeAttemptTerminationVerifier())

    with pytest.raises(ProviderEvidenceUnsupported, match="adapter"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"authenticated receipt bytes",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert verifier.raw_evidence == []
    assert journal.appended == []


def test_provider_evidence_rejects_unsupported_operator_assertion_source():
    call = _call()
    with pytest.raises(ValueError, match="evidence_source"):
        _evidence_result(call, evidence_source="operator_assertion")


def test_empty_provider_evidence_is_rejected_before_verification():
    call = _call()
    journal = _Journal(call)
    verifier = FakeProviderEvidenceVerifier(_evidence_result(call))
    service = _service(journal, verifier, FakeAttemptTerminationVerifier())

    with pytest.raises(ReconciliationRejected, match="empty"):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )

    assert verifier.raw_evidence == []
    assert journal.appended == []
