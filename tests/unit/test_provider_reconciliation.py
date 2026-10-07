from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import tempfile
import traceback
from types import SimpleNamespace

import pytest

from orchestrator.budget import BudgetLedger, CostEstimate, RunLimit
from orchestrator.budget.models import UsageRecord
from orchestrator.models.provider_calls import (
    ProviderCallReconciliation,
    ProviderCallJournalConflict,
    ProviderCallSnapshot,
    SQLiteProviderCallJournal,
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
from orchestrator.persistence import EventContractError
from orchestrator.persistence.events import validate_event_contract
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore, canonical_json


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
            "registry_manifest_hash": call.registry_manifest_hash,
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


@pytest.fixture
def durable_unknown_attempt(tmp_path):
    from tests.unit.lifecycle.test_scheduler import (
        NOW, _provider_call_request_for_crash, accept, routed_pair, run_setup, scheduler,
    )
    database = tmp_path / "reconciliation-identity.db"
    with SQLiteEventStore(database) as store:
        registry, config, _lifecycle, manifest = run_setup(store)
        control = scheduler(store)
        routing, decision = routed_pair(registry, config, manifest)
        accepted = accept(control, routing, decision)
        control.finish_attempt(run_id=routing.run_id, node_id=routing.node_id,
            attempt_id=routing.attempt_id, fencing_generation=routing.fencing_generation,
            completed_at=NOW + timedelta(minutes=1), outcome="outcome_unknown")
        request = _provider_call_request_for_crash(accepted, registry)
        # Keep request/prompt objects out of pytest's fixture diagnostics.
        yield lambda: (database, store, control, request)


@pytest.mark.parametrize("path", ["direct", "pending-restart"])
def test_reconciliation_rejects_wrong_frozen_registry_without_releasing_holds(durable_unknown_attempt, path):
    from tests.unit.lifecycle.test_scheduler import scheduler
    database, store, _control, request = durable_unknown_attempt()
    bad_request = request.model_copy(update={"accepted_route": request.accepted_route.model_copy(
        update={"registry_manifest_hash": "sha256:" + "f" * 64})})
    journal = SQLiteProviderCallJournal(store)
    stream_id = journal.record_intent(bad_request, provider_id="primary", provider_adapter="openai_responses",
        provider_correlation_id="maestro-registry-binding", request_body=b"{}")
    journal.record_outcome(bad_request, outcome="unknown", failure_code="timeout")
    call = journal.read_call(stream_id)
    evidence = _evidence_result(call)
    reconciled_at = datetime(2026, 9, 29, 13, tzinfo=timezone.utc)
    if path == "pending-restart":
        proof = ProviderCallReconciliation(**evidence.model_dump(), termination_receipt_hash="sha256:" + "f" * 64)
        journal._append_reconciliation(stream_id, proof, reconciled_at)
    # Reopen durable state rather than relying on an in-memory accepted route.
    with SQLiteEventStore(database) as reopened:
        journal = SQLiteProviderCallJournal(reopened)
        control = scheduler(reopened)
        before = journal.read_call(stream_id)
        scheduler_events = reopened.read_stream("scheduler", "global")
        budget_events = reopened.read_stream("budget", request.run_id)
        provider_events = reopened.read_stream("provider_call", stream_id)
        service = ProviderReconciliationService(journal=journal, scheduler=control,
            evidence_verifier=FakeProviderEvidenceVerifier(evidence), termination_verifier=FakeAttemptTerminationVerifier())
        with pytest.raises(ReconciliationRejected, match="Scheduler Attempt evidence"):
            if path == "direct":
                service.reconcile(stream_id, b"authenticated receipt bytes", object(), reconciled_at)
            else:
                service.apply_pending_settlements()
        assert journal.read_call(stream_id) == before
        assert reopened.read_stream("provider_call", stream_id) == provider_events
        assert reopened.read_stream("scheduler", "global") == scheduler_events
        assert reopened.read_stream("budget", request.run_id) == budget_events
        assert BudgetLedger(reopened).get_reservation(request.budget_reservation_id, run_id=request.run_id).status == "unknown"
        active = control.recovery.recover(request.run_id).active_attempts
        assert len(active) == 1 and active[0].status == "outcome_unknown"


def test_missing_frozen_registry_evidence_rejects_before_provider_verification():
    call = _call()
    journal = _Journal(call)
    verifier = FakeProviderEvidenceVerifier(_evidence_result(call))
    service = _service(journal, verifier, FakeAttemptTerminationVerifier())
    accepted = service.scheduler.event_store.read_stream("scheduler", "global")[0]
    accepted.payload["accepted_route"].pop("registry_manifest_hash")
    before = service.scheduler.event_store.read_stream("budget", call.run_id)
    with pytest.raises(ReconciliationRejected, match="Scheduler Attempt evidence"):
        service.reconcile(call.stream_id, b"authenticated receipt bytes", object(),
            datetime(2026, 9, 29, 13, tzinfo=timezone.utc))
    assert journal.call == call and journal.appended == []
    assert verifier.raw_evidence == []
    assert service.scheduler.event_store.read_stream("budget", call.run_id) == before


@pytest.mark.parametrize("provider_request_id", [None, "req-conflicting"])
@pytest.mark.parametrize("effect", ["not_received", "received_and_charged"])
def test_conflicting_provider_request_id_cannot_settle_unknown_call(durable_unknown_attempt, provider_request_id, effect):
    _database, store, control, request = durable_unknown_attempt()
    journal = SQLiteProviderCallJournal(store)
    stream_id = journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
        provider_correlation_id="maestro-request-binding", request_body=b"{}")
    journal.record_outcome(request, outcome="unknown", provider_request_id="req-original", failure_code="timeout")
    call = journal.read_call(stream_id)
    usage = None if effect == "not_received" else UsageRecord(run_id=call.run_id,
        reservation_id=call.budget_reservation_id, settlement_key="provider-usage", currency="USD",
        cost_minor=3, input_tokens=3, output_tokens=2)
    evidence = _evidence_result(call, provider_request_id=provider_request_id, effect=effect, usage=usage)
    termination = FakeAttemptTerminationVerifier()
    service = ProviderReconciliationService(journal=journal, scheduler=control,
        evidence_verifier=FakeProviderEvidenceVerifier(evidence), termination_verifier=termination)
    budget_events = store.read_stream("budget", request.run_id)
    scheduler_events = store.read_stream("scheduler", "global")
    with pytest.raises(ReconciliationRejected, match="identity binding"):
        service.reconcile(stream_id, b"authenticated receipt bytes", object(),
            datetime(2026, 9, 29, 13, tzinfo=timezone.utc))
    assert journal.read_call(stream_id) == call
    assert termination.calls == []
    assert store.read_stream("budget", request.run_id) == budget_events
    assert store.read_stream("scheduler", "global") == scheduler_events
    assert BudgetLedger(store).get_reservation(request.budget_reservation_id, run_id=request.run_id).status == "unknown"
    active = control.recovery.recover(request.run_id).active_attempts
    assert len(active) == 1 and active[0].status == "outcome_unknown"


@pytest.mark.parametrize("provider_request_id", [None, "req-conflicting"])
def test_journal_proof_must_match_durable_provider_request_id(durable_unknown_attempt, provider_request_id):
    _database, store, _control, request = durable_unknown_attempt()
    journal = SQLiteProviderCallJournal(store)
    stream_id = journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
        provider_correlation_id="maestro-request-binding", request_body=b"{}")
    journal.record_outcome(request, outcome="unknown", provider_request_id="req-original", failure_code="timeout")
    call = journal.read_call(stream_id)
    proof = ProviderCallReconciliation(**_evidence_result(call, provider_request_id=provider_request_id).model_dump(),
        termination_receipt_hash="sha256:" + "f" * 64)
    with pytest.raises(ProviderCallJournalConflict, match="identity binding"):
        journal._append_reconciliation(stream_id, proof, datetime(2026, 9, 29, 13, tzinfo=timezone.utc))
    assert journal.read_call(stream_id) == call
    assert len(store.read_stream("provider_call", stream_id)) == 2


@pytest.mark.parametrize("path", ["event-contract", "journal-reopen", "pending-restart", "proof-replay"])
@pytest.mark.parametrize("provider_request_id", [None, "req-conflicting"])
def test_tampered_request_id_proof_is_rejected_on_replay(durable_unknown_attempt, path, provider_request_id):
    from tests.unit.lifecycle.test_scheduler import scheduler
    database, store, _control, request = durable_unknown_attempt()
    journal = SQLiteProviderCallJournal(store)
    stream_id = journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
        provider_correlation_id="maestro-request-binding", request_body=b"{}")
    journal.record_outcome(request, outcome="unknown", provider_request_id="req-original", failure_code="timeout")
    call = journal.read_call(stream_id)
    proof = ProviderCallReconciliation(**_evidence_result(call, provider_request_id="req-original").model_dump(),
        termination_receipt_hash="sha256:" + "f" * 64)
    reconciled_at = datetime(2026, 9, 29, 13, tzinfo=timezone.utc)
    event_id = journal._append_reconciliation(stream_id, proof, reconciled_at)
    payload = dict(store.read_stream("provider_call", stream_id)[-1].payload, provider_request_id=provider_request_id)
    serialized = canonical_json(payload)
    # Preserve hash integrity so the missing identity check, not a checksum
    # mismatch, is what must reject the tampered persisted proof.
    # This fresh test database deliberately models storage tampering; reopening
    # below reinstalls the normal immutable-event trigger.
    store._connection.execute("DROP TRIGGER events_immutable_update")
    store._connection.execute("UPDATE events SET payload_json=?, payload_hash=? WHERE event_id=?",
        (serialized, hashlib.sha256(serialized.encode()).hexdigest(), event_id))
    store._connection.commit()
    with SQLiteEventStore(database) as reopened:
        journal = SQLiteProviderCallJournal(reopened)
        control = scheduler(reopened)
        provider_events = reopened.read_stream("provider_call", stream_id)
        scheduler_events = reopened.read_stream("scheduler", "global")
        budget_events = reopened.read_stream("budget", request.run_id)
        service = ProviderReconciliationService(journal=journal, scheduler=control,
            termination_verifier=FakeAttemptTerminationVerifier())
        expected_error = EventContractError if path in {"event-contract", "journal-reopen"} else ReconciliationRejected
        with pytest.raises(expected_error):
            if path == "event-contract":
                validate_event_contract(provider_events)
            elif path == "journal-reopen":
                journal.read_call(stream_id)
            elif path == "pending-restart":
                service.apply_pending_settlements()
            else:
                service.reconcile(stream_id, b"authenticated receipt bytes", object(), reconciled_at)
        assert reopened.read_stream("provider_call", stream_id) == provider_events
        assert reopened.read_stream("scheduler", "global") == scheduler_events
        assert reopened.read_stream("budget", request.run_id) == budget_events
        assert BudgetLedger(reopened).get_reservation(request.budget_reservation_id, run_id=request.run_id).status == "unknown"
        active = control.recovery.recover(request.run_id).active_attempts
        assert len(active) == 1 and active[0].status == "outcome_unknown"


@pytest.mark.parametrize("path", ["pending", "proof-replay"])
def test_service_revalidates_request_id_in_a_supplied_proof_snapshot(path):
    call = _call(provider_request_id="req-original")
    proof = ProviderCallReconciliation(**_evidence_result(call, provider_request_id="req-conflicting").model_dump(),
        termination_receipt_hash="sha256:" + "f" * 64)
    reconciled_at = datetime(2026, 9, 29, 13, tzinfo=timezone.utc)
    pending = replace(call, status="settlement_pending", reconciliation=proof,
        reconciliation_event_id="proof-event", reconciled_at=reconciled_at)
    journal = _Journal(pending)
    journal.pending_settlements = lambda: (journal.call,)
    service = _service(journal, None, FakeAttemptTerminationVerifier())
    before = service.scheduler.event_store.read_stream("budget", call.run_id)
    with pytest.raises(ReconciliationRejected, match="proof binding"):
        if path == "pending":
            service.apply_pending_settlements()
        else:
            service.reconcile(call.stream_id, b"authenticated receipt bytes", object(), reconciled_at)
    assert journal.call == pending
    assert service.scheduler.event_store.read_stream("budget", call.run_id) == before


@pytest.mark.parametrize("original_id, evidence_id", [(None, "req-discovered"), ("req-original", "req-original")])
def test_matching_or_newly_discovered_provider_request_id_can_settle(durable_unknown_attempt, original_id, evidence_id):
    _database, store, control, request = durable_unknown_attempt()
    journal = SQLiteProviderCallJournal(store)
    stream_id = journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
        provider_correlation_id="maestro-request-binding", request_body=b"{}")
    journal.record_outcome(request, outcome="unknown", provider_request_id=original_id, failure_code="timeout")
    call = journal.read_call(stream_id)
    service = ProviderReconciliationService(journal=journal, scheduler=control,
        evidence_verifier=FakeProviderEvidenceVerifier(_evidence_result(call, provider_request_id=evidence_id)),
        termination_verifier=FakeAttemptTerminationVerifier())
    settled = service.reconcile(stream_id, b"authenticated receipt bytes", object(),
        datetime(2026, 9, 29, 13, tzinfo=timezone.utc))
    assert settled.status == "reconciled" and settled.reconciliation.provider_request_id == evidence_id
    assert control.recovery.recover(request.run_id).active_attempts == ()


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


@pytest.mark.parametrize("source", ["provider", "termination"])
@pytest.mark.parametrize("error_type", [ProviderEvidenceUnsupported, ReconciliationRejected])
@pytest.mark.parametrize("chained", [False, True])
def test_typed_verifier_failures_redact_messages_and_chains(source, error_type, chained):
    call = _call()
    journal = _Journal(call)
    sentinel = "private-verifier-sentinel-79b"

    def reject():
        if chained:
            try:
                raise ValueError(sentinel)
            except ValueError as cause:
                raise error_type(sentinel) from cause
        raise error_type(sentinel)

    class EvidenceVerifier:
        def verify(self, _call, _raw):
            if source == "provider":
                reject()
            return _evidence_result(call)

    class TerminationVerifier:
        def verify_stopped(self, _call, _receipt):
            reject()

    service = _service(journal, EvidenceVerifier(), TerminationVerifier())
    before = service.scheduler.event_store.read_stream("budget", call.run_id)
    expected_type = ProviderEvidenceUnsupported if source == "provider" and error_type is ProviderEvidenceUnsupported else ReconciliationRejected
    with pytest.raises(expected_type) as failure:
        service.reconcile(
            call.stream_id, b"authenticated receipt bytes", object(),
            datetime(2026, 9, 29, 13, tzinfo=timezone.utc),
        )
    assert sentinel not in str(failure.value)
    assert sentinel not in "".join(traceback.format_exception(failure.value))
    assert failure.value.__suppress_context__
    assert journal.call == call and journal.appended == []
    assert service.scheduler.event_store.read_stream("budget", call.run_id) == before


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


@pytest.mark.parametrize("overrides, message", [
    ({"provider_correlation_id": "not-ascii-é"}, "correlation binding"),
    ({"registry_manifest_hash": None}, "content hashes"),
    ({"registry_manifest_hash": ""}, "content hashes"),
])
def test_malformed_call_snapshot_is_rejected_before_provider_lookup(overrides, message):
    call = _call(**overrides)
    journal = _Journal(call)
    evidence_verifier = FakeProviderEvidenceVerifier(_evidence_result(_call()))
    service = _service(
        journal,
        evidence_verifier,
        FakeAttemptTerminationVerifier(),
    )

    with pytest.raises(ReconciliationRejected, match=message):
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
