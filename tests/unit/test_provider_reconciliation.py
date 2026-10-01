from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import tempfile
from types import SimpleNamespace

import pytest

from orchestrator.budget import BudgetLedger, CostEstimate, RunLimit
from orchestrator.budget.models import UsageRecord
from orchestrator.models.provider_calls import (
    ProviderCallReconciliation,
    ProviderCallSnapshot,
)
from orchestrator.models.provider_sender import ProviderSenderTerminationReceipt
import orchestrator.provider_reconciliation as reconciliation_module
from orchestrator.provider_reconciliation import (
    ProviderEvidenceResult,
    ProviderEvidenceUnsupported,
    ProviderReconciliationService,
    ReconciliationRejected,
    UnavailableAttemptTerminationVerifier,
    UnavailableProviderEvidenceVerifier,
)
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore


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

    def _record_scheduler_settlement(self, stream_id, reconciliation_event_id):
        assert stream_id == self.call.stream_id
        assert reconciliation_event_id == self.call.reconciliation_event_id
        self.call = replace(self.call, status="reconciled", settlement_applied=True)


class _Scheduler:
    def __init__(self, call):
        self._temp_dir = tempfile.TemporaryDirectory()
        base = SQLiteEventStore(f"{self._temp_dir.name}/events.db")
        ledger = BudgetLedger(
            base,
            run_limits={call.run_id: RunLimit(max_cost_minor=10, max_tokens=10)},
        )
        reservation = ledger.reserve(
            call.run_id,
            CostEstimate(amount_minor=1, currency="USD", token_limit=1, snapshot_id="test"),
            reservation_id=call.budget_reservation_id,
        )
        ledger.mark_unknown(reservation.reservation_id, run_id=call.run_id)
        attempt = SimpleNamespace(
            attempt_id=call.attempt_id,
            fencing_generation=call.fencing_generation,
            reservation_id=call.budget_reservation_id,
            model_id=call.model_id,
            provider_id=call.provider_id,
            status="outcome_unknown",
        )
        route = {
            "decision_id": call.accepted_route_id,
            "run_id": call.run_id,
            "node_id": call.node_id,
            "attempt_id": call.attempt_id,
            "fencing_generation": call.fencing_generation,
            "budget_reservation_id": call.budget_reservation_id,
            "provider_id": call.provider_id,
            "model_id": call.model_id,
        }
        events = [
            SimpleNamespace(
                event_type="RoutingDecisionAccepted",
                payload={
                    "run_id": call.run_id,
                    "node_id": call.node_id,
                    "attempt_id": call.attempt_id,
                    "accepted_route": route,
                },
                fencing_generation=call.fencing_generation,
            ),
            SimpleNamespace(
                event_type="AttemptOutcomeUnknown",
                payload={
                    "run_id": call.run_id,
                    "node_id": call.node_id,
                    "attempt_id": call.attempt_id,
                    "completed_at": "2026-09-29T12:00:00+00:00",
                },
                fencing_generation=call.fencing_generation,
            ),
        ]

        class _EventStore:
            def read_stream(self, stream_type, stream_id, *args, **kwargs):
                if stream_type == "scheduler" and stream_id == "global":
                    return events
                return base.read_stream(stream_type, stream_id, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(base, name)

        class _Lifecycle:
            def replay(self, run_id):
                assert run_id == call.run_id
                return SimpleNamespace(
                    node=lambda node_id: SimpleNamespace(
                        attempts=(attempt,) if node_id == call.node_id else ()
                    )
                )

        self.event_store = _EventStore()
        self.lifecycle = _Lifecycle()

    def reconcile_attempt(self, **_kwargs):
        return None


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
        scheduler=_Scheduler(journal.call),
        evidence_verifier=evidence_verifier,
        termination_verifier=termination_verifier,
    )


def _sender_receipt(call, **overrides):
    values = {
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
        "unit_name": "maestro-provider-" + "a" * 32 + ".service",
        "cgroup_path_hash": "sha256:" + "e" * 64,
        "active_state": "inactive",
        "cgroup_empty": True,
        "observed_at": datetime(2026, 9, 29, 12, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return ProviderSenderTerminationReceipt(**values)


def test_provider_sender_termination_verifier_requires_exact_persisted_receipt():
    call = _call()
    receipt = _sender_receipt(call)
    persisted = replace(call, termination_receipt=receipt)
    verifier_type = getattr(
        reconciliation_module, "ProviderSenderTerminationVerifier", None
    )
    assert verifier_type is not None, "ProviderSenderTerminationVerifier is not implemented"
    verifier = verifier_type(_Journal(persisted))

    assert verifier.verify_stopped(persisted, receipt) == receipt.receipt_hash


def test_provider_sender_termination_verifier_rejects_unpersisted_or_mismatched_receipt():
    call = _call()
    receipt = _sender_receipt(call)
    verifier_type = getattr(
        reconciliation_module, "ProviderSenderTerminationVerifier", None
    )
    assert verifier_type is not None, "ProviderSenderTerminationVerifier is not implemented"

    with pytest.raises(ReconciliationRejected, match="persisted"):
        verifier_type(_Journal(call)).verify_stopped(call, receipt)

    persisted = replace(call, termination_receipt=receipt)
    forged = receipt.model_copy(update={"attempt_id": "attempt-other"})
    with pytest.raises(ReconciliationRejected):
        verifier_type(_Journal(persisted)).verify_stopped(persisted, forged)


@pytest.mark.parametrize("failure_source", ["provider", "termination", "invalid-termination-digest"])
def test_untrusted_verifier_failure_does_not_expose_private_receipts_or_settle(failure_source):
    call = _call()
    journal = _Journal(call)

    class EvidenceVerifier:
        def verify(self, _call, _raw):
            if failure_source == "provider":
                raise ValueError("private-api-key and private receipt bytes")
            return _evidence_result(call)

    class TerminationVerifier:
        def verify_stopped(self, _call, _receipt):
            if failure_source == "termination":
                raise ValueError("private-api-key and private host receipt")
            return "private-invalid-digest"

    service = _service(journal, EvidenceVerifier(), TerminationVerifier())
    with pytest.raises(ReconciliationRejected) as failure:
        service.reconcile(
            call.stream_id, raw_evidence=b"authenticated receipt bytes",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )
    assert "private" not in str(failure.value)
    assert failure.value.__suppress_context__ or failure_source == "invalid-termination-digest"
    assert journal.call == call and journal.appended == []
    assert not any(event.event_type == "CostCommitted" for event in
                   service.scheduler.event_store.read_stream("budget", call.run_id))


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

    assert persisted.status == "reconciled"
    assert len(journal.appended) == 1
    proof, persisted_at = journal.appended[0]
    assert proof.provider_call_stream_id == call.stream_id
    assert proof.evidence_digest == evidence.evidence_digest
    assert proof.termination_receipt_hash == termination_digest
    assert persisted_at == reconciled_at
    assert termination_verifier.calls[0][0] == call


def test_persisted_sender_proof_can_be_replayed_after_settlement():
    receipt_bytes = b"authenticated receipt bytes"
    reconciled_at = datetime(2026, 9, 29, 13, tzinfo=timezone.utc)
    call = _call(termination_receipt=None)
    receipt = _sender_receipt(call)
    call = replace(call, termination_receipt=receipt)
    journal = _Journal(call)
    verifier_type = getattr(
        reconciliation_module, "ProviderSenderTerminationVerifier", None
    )
    assert verifier_type is not None, "ProviderSenderTerminationVerifier is not implemented"

    first_service = _service(
        journal,
        FakeProviderEvidenceVerifier(_evidence_result(call)),
        verifier_type(journal),
    )
    first_result = first_service.reconcile(
        call.stream_id,
        raw_evidence=receipt_bytes,
        termination_receipt=receipt,
        reconciled_at=reconciled_at,
    )

    journal.call = replace(journal.call, status="settlement_pending", settlement_applied=False)
    replay_service = _service(
        journal,
        FakeProviderEvidenceVerifier(_evidence_result(call)),
        verifier_type(journal),
    )
    replay_result = replay_service.reconcile(
        call.stream_id,
        raw_evidence=receipt_bytes,
        termination_receipt=receipt,
        reconciled_at=reconciled_at,
    )
    settled_replay_service = _service(
        journal,
        FakeProviderEvidenceVerifier(_evidence_result(call)),
        verifier_type(journal),
    )
    settled_replay_result = settled_replay_service.reconcile(
        call.stream_id,
        raw_evidence=receipt_bytes,
        termination_receipt=receipt,
        reconciled_at=reconciled_at,
    )

    assert first_result.status == "reconciled"
    assert replay_result.status == "reconciled"
    assert settled_replay_result == first_result
    assert len(journal.appended) == 1


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
