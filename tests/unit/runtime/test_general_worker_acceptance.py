"""General terminal proofs, private bytes, trusted usage and atomic replay."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from orchestrator.artifacts import ArtifactStore, EphemeralArtifactGrantAuthority
from orchestrator.models import ModelResponse, TokenUsage
from orchestrator.runtime.acceptance_coordinator import AttemptExecutionCoordinator, ProposalAcceptanceError
from orchestrator.runtime.contracts import ArtifactRef, VerificationTask
from orchestrator.runtime.general_worker_authority import SchedulerProviderDispatchAuthority
from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal, GeneralWorkerJournalError
from orchestrator.runtime.verification_journal import VerifierProposalJournal
from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
from orchestrator.scheduler import Scheduler, SchedulerError
from tests.unit.runtime.test_attempt_execution_coordinator import TrustedUsage
from tests.unit.runtime.test_general_worker_authority import dispatch_case, reserve
from tests.unit.runtime.test_general_worker_journal import epoch, provider_success
from tests.unit.runtime.test_verification_journal import _evidence
from tests.unit.runtime.test_verifier_process import _ChildLauncher
from tests.unit.lifecycle.test_scheduler import _resolver_durable_state


@pytest.fixture(autouse=True)
def offline_verifier(monkeypatch):
    original = IsolatedVerifierProcess.__init__
    def initialize(self, **kwargs):
        kwargs.setdefault("launcher", _ChildLauncher())
        original(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, "__init__", initialize)


class CountedUsage(TrustedUsage):
    def __init__(self, store):
        self.store = store
        self.calls = 0

    def usage_for(self, snapshot):
        assert not self.store._connection.in_transaction
        self.calls += 1
        return super().usage_for(snapshot)


def terminal_case(tmp_path, *, stopped=True, trusted=True, required_check_ids=("artifact-integrity",), response_changes=None):
    import hashlib
    import json
    case = dispatch_case(tmp_path, required_check_ids=required_check_ids)
    grants = EphemeralArtifactGrantAuthority()
    artifacts = ArtifactStore(tmp_path / "artifacts", event_store=case.store, grant_verifier=grants.verify)
    source = CountedUsage(case.store)
    scheduler = Scheduler(case.store, limits=case.scheduler.limits, artifact_store=artifacts,
        usage_source=source if trusted else None, artifact_grants=grants)
    case.scheduler = scheduler
    case.authority = SchedulerProviderDispatchAuthority(scheduler, case.journal)
    reserve(case)
    provider_success(case.store, case.request, body=case.body)
    response = ModelResponse(request_id=case.request.request_id, model_id=case.request.model_id,
        output_text="terminal model text", finish_reason="stop",
        usage=TokenUsage(status="reported", input_tokens=1, output_tokens=2))
    if response_changes:
        response = response.model_copy(update=response_changes)
    raw = json.dumps(response.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    provenance = {**{key: str(value) for key, value in case.identity.items()},
        "agent_instance_id": case.operation.context.agent_instance_id,
        "general_worker_operation_id": case.operation.operation_id}
    response_record = artifacts.publish_bytes(raw, artifact_type="general-model-response", media_type="application/json",
        source={**provenance, "general_worker_kind": "response"}, readable_scope=(case.operation.operation_id,))
    ref = ArtifactRef(digest=response_record.digest, size_bytes=response_record.size,
        artifact_type=response_record.artifact_type, media_type=response_record.media_type)
    output = response.output_text.encode()
    case.operation = case.journal.capture_response(case.operation, response_artifact=ref,
        output_digest="sha256:" + hashlib.sha256(output).hexdigest(), output_byte_count=len(output))
    prepared, ack, stop = epoch("9c483536-3c61-41e7-977b-6559b2732937")
    case.operation = case.journal.prepare_turn(case.operation, phase="complete", prepared=prepared)
    case.operation = case.journal.ack_turn(case.operation, receipt=ack)
    if stopped:
        case.operation = case.journal.stop_turn(case.operation, receipt=stop)
    output_record = artifacts.publish_bytes(output, artifact_type="text", media_type="text/plain; charset=utf-8",
        source={**provenance, "general_worker_kind": "output"}, readable_scope=(case.identity["run_id"],))
    task = VerificationTask(context=case.operation.context, acceptance_contract="maestro.artifact-verification/v1",
        required_check_ids=required_check_ids, candidate_artifacts=(ArtifactRef(digest=output_record.digest,
            size_bytes=output_record.size, artifact_type=output_record.artifact_type, media_type=output_record.media_type),))
    proposals = VerifierProposalJournal(case.store)
    proposal = proposals.record(task, _evidence(task))
    if stopped:
        case.operation = case.journal.record_proposal(case.operation, task_sha256=proposal.task_sha256,
            evidence_sha256=proposal.evidence_sha256)
    coordinator = AttemptExecutionCoordinator(scheduler=scheduler, artifact_store=artifacts,
        verifier=IsolatedVerifierProcess(launcher=_ChildLauncher()), journal=proposals,
        grants=grants, usage_source=source)
    case.source, case.artifacts, case.proposal, case.coordinator = source, artifacts, proposal, coordinator
    case.response_ref = ref
    return case


@pytest.fixture
def terminal(tmp_path):
    case = terminal_case(tmp_path)
    yield case
    case.store.close()


def direct(case, *, usage=None):
    binding = case.scheduler._resolve_general_verification_binding(**case.identity, as_of=datetime.now(timezone.utc))
    value = TrustedUsage().usage_for(binding.active_attempt) if usage is None else usage
    return case.scheduler.finish_verified_attempt(**case.identity, task_sha256=case.proposal.task_sha256,
        completed_at=datetime.now(timezone.utc), usage=value)


def test_direct_acceptance_cannot_bypass_general_stop(tmp_path):
    case = terminal_case(tmp_path, stopped=False)
    try:
        before = _resolver_durable_state(case.store)
        with pytest.raises((SchedulerError, GeneralWorkerJournalError)):
            direct(case)
        assert _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["finish", "reconcile", "cancel"])
def test_failure_cannot_release_unproved_worker(tmp_path, entry):
    case = terminal_case(tmp_path, stopped=False)
    try:
        now = datetime.now(timezone.utc)
        if entry == "reconcile":
            case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
        if entry == "cancel":
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
        before = _resolver_durable_state(case.store)
        with pytest.raises((SchedulerError, GeneralWorkerJournalError)):
            if entry == "finish":
                case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="failed", known_no_effect=True)
            elif entry == "reconcile":
                case.scheduler.reconcile_attempt(**case.identity, reconciled_at=now, outcome="failed", known_no_effect=True)
            else:
                case.scheduler.acknowledge_cancellation(**case.identity, stopped_at=now,
                    stop_receipt_hash="sha256:"+"a"*64, no_effect_receipt_hash="sha256:"+"b"*64)
        assert _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


def test_accepted_terminal_and_usage_commit_roll_back_together(terminal, monkeypatch):
    before = _resolver_durable_state(terminal.store)
    original = terminal.scheduler.agents.complete_attempt
    def fail(*args, **kwargs):
        original(*args, **kwargs)
        assert terminal.store._connection.in_transaction
        assert any(event.event_type == "GeneralWorkerAccepted" for event in terminal.store.read_stream(
            "general_worker", terminal.operation.identity.stream_id))
        raise RuntimeError("offline injected atomicity failure")
    monkeypatch.setattr(terminal.scheduler.agents, "complete_attempt", fail)
    with pytest.raises((SchedulerError, GeneralWorkerJournalError)):
        direct(terminal)
    assert _resolver_durable_state(terminal.store) == before


def test_lost_reply_replays_historical_result_without_active_lease(terminal, monkeypatch):
    direct(terminal)
    before = _resolver_durable_state(terminal.store)
    def forbidden(*args, **kwargs):
        raise AssertionError("historical replay attempted fresh authority")
    monkeypatch.setattr(terminal.source, "usage_for", forbidden)
    monkeypatch.setattr(terminal.scheduler, "_resolve_general_verification_binding", forbidden)
    monkeypatch.setattr(terminal.coordinator, "_now", forbidden)
    result = terminal.scheduler.read_general_worker_result(terminal.operation.identity)
    assert result.status == "accepted" and result.artifacts == terminal.proposal.task.candidate_artifacts
    assert result.task_sha256 == terminal.proposal.task_sha256
    assert terminal.coordinator.accept_proposal(**terminal.identity, task_sha256=terminal.proposal.task_sha256) == terminal.proposal
    assert _resolver_durable_state(terminal.store) == before


def test_direct_general_terminal_verifies_files_before_lock(terminal, monkeypatch):
    observed = []
    original = Path.open
    def read(path, *args, **kwargs):
        assert not terminal.store._connection.in_transaction
        observed.append(path)
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", read)
    direct(terminal)
    assert observed and terminal.source.calls == 1


def test_ambient_transaction_cannot_trigger_general_file_preflight(terminal, monkeypatch):
    observed = []
    def forbidden(*args, **kwargs):
        observed.append("file")
        raise AssertionError("file read under caller transaction")
    monkeypatch.setattr(Path, "open", forbidden)
    def outer(events, version):
        with pytest.raises((SchedulerError, GeneralWorkerJournalError)):
            terminal.scheduler.finish_verified_attempt(**terminal.identity, task_sha256=terminal.proposal.task_sha256,
                completed_at=datetime.now(timezone.utc), usage=None)
    terminal.store.append_checked("scheduler", "global", "ambient-terminal", outer)
    assert observed == [] and terminal.source.calls == 0


@pytest.mark.parametrize("fault", ["source-missing", "caller-usage", "provider-tokens", "output-bytes", "response-bytes"])
def test_general_terminal_rejects_missing_trusted_usage_or_byte_proof(tmp_path, fault):
    case = terminal_case(tmp_path, trusted=fault != "source-missing")
    try:
        before = _resolver_durable_state(case.store)
        usage = None
        if fault == "caller-usage":
            binding = case.scheduler._resolve_general_verification_binding(**case.identity, as_of=datetime.now(timezone.utc))
            usage = TrustedUsage().usage_for(binding.active_attempt).model_copy(update={"cost_minor": 0})
        if fault == "provider-tokens":
            original = case.source.usage_for
            case.source.usage_for = lambda snapshot: original(snapshot).model_copy(update={"input_tokens": 99})
        if fault.endswith("bytes"):
            digest = case.response_ref.digest if fault == "response-bytes" else case.operation.capture.output_digest
            case.artifacts._path_for_digest(digest).write_bytes(b"corrupt")
        with pytest.raises((SchedulerError, GeneralWorkerJournalError, ProposalAcceptanceError)):
            direct(case, usage=usage)
        assert _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


def test_coordinator_general_path_samples_trusted_usage_once_outside_transaction(terminal, monkeypatch):
    original = IsolatedVerifierProcess.verify
    calls = []
    def verify(self, *args, **kwargs):
        assert not terminal.store._connection.in_transaction
        calls.append("verify")
        return original(self, *args, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, "verify", verify)
    returned = terminal.coordinator.accept_proposal(**terminal.identity, task_sha256=terminal.proposal.task_sha256)
    assert returned == terminal.proposal and terminal.source.calls == 1 and calls == ["verify"]
    assert terminal.scheduler.read_general_worker_result(terminal.operation.identity).status == "accepted"


def test_general_acceptance_requires_fresh_independent_verifier(terminal, monkeypatch):
    before = _resolver_durable_state(terminal.store)
    calls = []
    def rejected(self, *args, **kwargs):
        assert not terminal.store._connection.in_transaction
        calls.append("verify")
        raise ValueError("offline independent verifier rejection")
    monkeypatch.setattr(IsolatedVerifierProcess, "verify", rejected)
    with pytest.raises(SchedulerError):
        direct(terminal)
    assert calls == ["verify"] and terminal.source.calls == 0
    assert _resolver_durable_state(terminal.store) == before


def test_forged_accepted_evidence_cannot_bypass_frozen_json_check(tmp_path):
    case = terminal_case(tmp_path, required_check_ids=("artifact-integrity", "json"))
    try:
        before = _resolver_durable_state(case.store)
        with pytest.raises(SchedulerError):
            direct(case)
        assert case.source.calls == 0
        assert _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


def test_general_acceptance_requires_owned_artifact_grants(terminal):
    terminal.scheduler._general_artifact_grants = None
    before = _resolver_durable_state(terminal.store)
    with pytest.raises(SchedulerError):
        direct(terminal)
    assert terminal.source.calls == 0 and _resolver_durable_state(terminal.store) == before


def test_application_shares_usage_and_grants_and_preserves_old_imports(tmp_path):
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.runtime import attempt_usage, acceptance_coordinator
    from tests.unit.lifecycle.test_scheduler import _application_limits
    source = TrustedUsage()
    with ControlPlaneApplication(tmp_path / "wired.db", limits=_application_limits(),
        artifact_root=tmp_path / "artifacts", usage_source=source) as app:
        assert app._scheduler._general_usage_source is app._acceptance_coordinator._usage_source is source
        assert app._scheduler._general_artifact_grants is app._acceptance_coordinator._grants is app._artifact_grants
    assert attempt_usage.ProposalAcceptanceError is acceptance_coordinator.ProposalAcceptanceError
    assert attempt_usage.UnavailableAttemptUsageSource is acceptance_coordinator.UnavailableAttemptUsageSource


@pytest.mark.parametrize("fault", ["missing-accepted", "completed-at", "usage-hash", "settlement-key", "publication"])
def test_bootstrap_rejects_unpaired_success(tmp_path, fault):
    from orchestrator.application import ControlPlaneApplication, StartupRecoveryFailed
    from tests.integration.test_recovery import _replace_proof_payload, _durable_rows
    case = terminal_case(tmp_path)
    direct(case)
    accepted = case.store.read_stream("general_worker", case.operation.identity.stream_id)[-1]
    if fault == "missing-accepted":
        case.store._connection.execute("DROP TRIGGER events_immutable_delete")
        case.store._connection.execute("DELETE FROM events WHERE event_id = ?", (accepted.event_id,))
        case.store._connection.execute("UPDATE stream_versions SET current_version=current_version-1 "
            "WHERE stream_type='general_worker' AND stream_id=?", (accepted.stream_id,))
        case.store._connection.execute("DELETE FROM idempotency_records WHERE stream_type='general_worker' "
            "AND idempotency_key=?", (accepted.idempotency_key,))
    else:
        data = dict(accepted.payload["data"])
        name, value = {"completed-at": ("completed_at", "2026-01-01T00:00:00+00:00"),
            "usage-hash": ("usage_sha256", "sha256:" + "f"*64),
            "settlement-key": ("settlement_key", "other-settlement"),
            "publication": ("output_publication_id", "other-publication")}[fault]
        data[name] = value
        _replace_proof_payload(case.store, accepted, {**accepted.payload, "data": data})
        # Keep the general event structurally canonical, so rejection must
        # come from the reciprocal terminal/usage/publication links.
        from orchestrator.runtime.general_worker_journal import _event_key
        new_key = _event_key(case.operation.operation_id, "Accepted", data)
        case.store._connection.execute("UPDATE events SET idempotency_key=? WHERE event_id=?", (new_key, accepted.event_id))
        case.store._connection.execute("UPDATE idempotency_records SET idempotency_key=? "
            "WHERE stream_type='general_worker' AND idempotency_key=?", (new_key, accepted.idempotency_key))
    case.store.close()
    before = _durable_rows(tmp_path / "events.db")
    with pytest.raises(StartupRecoveryFailed):
        ControlPlaneApplication(tmp_path / "events.db", limits=case.scheduler.limits, artifact_root=tmp_path / "artifacts")
    assert _durable_rows(tmp_path / "events.db") == before


def test_reopened_historical_acceptance_needs_no_live_usage_or_scope(tmp_path, monkeypatch):
    from orchestrator.application import ControlPlaneApplication
    case = terminal_case(tmp_path)
    direct(case)
    expected = case.scheduler.read_general_worker_result(case.operation.identity)
    case.store.close()
    with ControlPlaneApplication(tmp_path / "events.db", limits=case.scheduler.limits,
        artifact_root=tmp_path / "artifacts") as app:
        def forbidden(*args, **kwargs):
            raise AssertionError("historical replay crossed external boundary")
        monkeypatch.setattr(IsolatedVerifierProcess, "verify", forbidden)
        monkeypatch.setattr(Path, "open", forbidden)
        assert app._scheduler.read_general_worker_result(case.operation.identity) == expected
        assert app._acceptance_coordinator.accept_proposal(**case.identity,
            task_sha256=case.proposal.task_sha256) == case.proposal


@pytest.mark.parametrize("stopped", [False, True], ids=["sender-stop-only", "worker-and-sender-stopped"])
def test_startup_provider_settlement_requires_separate_worker_stop(tmp_path, stopped):
    from orchestrator.application import ControlPlaneApplication, StartupRecoveryFailed
    from orchestrator.models.provider_calls import ProviderCallReconciliation
    from tests.integration.test_recovery import _durable_rows
    case = dispatch_case(tmp_path, register=False)
    operation = case.journal.request(**case.registration)
    prepared, ack, stop = epoch()
    operation = case.journal.prepare_turn(operation, phase="request", prepared=prepared)
    operation = case.journal.ack_turn(operation, receipt=ack)
    if stopped:
        from orchestrator.runtime.general_worker_protocol import GeneralWorkerRequest
        operation = case.journal.stop_turn(operation, receipt=stop, request_proposal=GeneralWorkerRequest(
            operation.context, prepared.launch_id, operation.input_commitment))
        operation = case.journal.reserve_call(operation, request=case.request, request_body=case.body)
    # Seed persisted Provider evidence independently, as startup must handle
    # untrusted historical journals as well as current Gateway-produced state.
    journal = case.provider_journal
    journal.record_intent(case.request, provider_id=case.request.accepted_route.provider_id,
        provider_adapter="openai_responses", request_body=case.body, provider_correlation_id="offline-correlation")
    journal.record_outcome(case.request, outcome="unknown", failure_code="timeout")
    call = journal.read(case.request)
    now = datetime.now(timezone.utc)
    case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
    proof = ProviderCallReconciliation(**{name: getattr(call, name) for name in (
        "provider_adapter", "provider_correlation_id", "run_id", "node_id", "attempt_id", "fencing_generation",
        "provider_id", "model_id", "accepted_route_id", "budget_reservation_id", "registry_manifest_hash", "request_hash")},
        provider_call_stream_id=call.stream_id, effect="not_received", usage=None, provider_request_id=None,
        evidence_source="provider_signed_receipt", evidence_digest="sha256:"+"d"*64,
        termination_receipt_hash="sha256:"+"c"*64, observed_at=now)
    journal._append_reconciliation(call.stream_id, proof, now)
    case.store.close()
    before = _durable_rows(tmp_path / "events.db")
    if stopped:
        with ControlPlaneApplication(tmp_path / "events.db", limits=case.scheduler.limits) as app:
            assert app.startup_report.applied_provider_calls == (call.stream_id,)
            assert app.startup_report.held_attempts == 0
            assert app._scheduler._general_recovery_scope is None
        with ControlPlaneApplication(tmp_path / "events.db", limits=case.scheduler.limits) as reopened:
            assert reopened.startup_report.held_attempts == 0
    else:
        with pytest.raises(StartupRecoveryFailed):
            ControlPlaneApplication(tmp_path / "events.db", limits=case.scheduler.limits)
        assert _durable_rows(tmp_path / "events.db") == before


def test_general_terminal_rechecks_withdrawn_authority_after_usage(terminal):
    original = terminal.source.usage_for
    def withdraw(snapshot):
        usage = original(snapshot)
        terminal.lifecycle.request_cancel("run-1", reason_code="user_request")
        return usage
    terminal.source.usage_for = withdraw
    with pytest.raises(SchedulerError):
        direct(terminal)
    assert terminal.source.calls == 1
    assert terminal.journal.read(terminal.operation.identity).accepted_result is None
    assert not any(event.event_type == "AttemptSlotReleased" for event in terminal.store.read_stream("scheduler", "global"))
    assert terminal.scheduler.recovery.recover_control_state("run-1").active_attempts


@pytest.mark.parametrize("fault", ["response-source", "output-scope", "response-request", "response-usage"])
def test_general_terminal_rejects_wrong_publication_and_response_bindings(tmp_path, fault):
    from tests.integration.test_recovery import _replace_proof_payload
    changes = {"response-request": {"request_id": "other-request"},
        "response-usage": {"usage": TokenUsage(status="reported", input_tokens=99, output_tokens=2)}}
    case = terminal_case(tmp_path, response_changes=changes.get(fault))
    try:
        digest = case.response_ref.digest if fault.startswith("response") else case.operation.capture.output_digest
        event = next(event for event in case.store.read_stream("artifact", digest)
            if event.event_type == "ArtifactPublished")
        payload = dict(event.payload)
        if fault.endswith("source"):
            payload["source"] = {**payload["source"], "general_worker_operation_id": "other-operation"}
        elif fault.endswith("scope"):
            payload["readable_scope"] = ["other-run"]
        if fault in {"response-source", "output-scope"}:
            _replace_proof_payload(case.store, event, payload)
        before = _resolver_durable_state(case.store)
        with pytest.raises(SchedulerError):
            direct(case)
        assert case.source.calls == 0 and _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


def test_fresh_accepted_evidence_must_match_exact_durable_hash(tmp_path, monkeypatch):
    case = terminal_case(tmp_path, required_check_ids=("artifact-integrity", "utf8-text"))
    try:
        original = IsolatedVerifierProcess.verify
        calls = []
        def mismatched(self, *args, **kwargs):
            evidence = original(self, *args, **kwargs)
            assert evidence.outcome == "accepted"
            calls.append("fresh-accepted")
            return evidence.model_copy(update={"checks": tuple(reversed(evidence.checks))})
        monkeypatch.setattr(IsolatedVerifierProcess, "verify", mismatched)
        before = _resolver_durable_state(case.store)
        with pytest.raises(SchedulerError):
            direct(case)
        assert calls == ["fresh-accepted"] and case.source.calls == 0
        assert _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


def test_general_preflight_error_does_not_retain_private_provider_exception(terminal, monkeypatch):
    def private_error(*args, **kwargs):
        raise ValueError("private-output-sentinel")
    monkeypatch.setattr(IsolatedVerifierProcess, "verify", private_error)
    with pytest.raises(SchedulerError) as error:
        direct(terminal)
    assert error.value.__cause__ is None and error.value.__context__ is None
    assert "private-output-sentinel" not in str(error.value)


def test_general_acceptance_callbacks_and_files_are_all_outside_checked_transactions(terminal, monkeypatch):
    observed = set()
    def outside(original, label):
        def checked(*args, **kwargs):
            assert not terminal.store._connection.in_transaction, label
            observed.add(label)
            return original(*args, **kwargs)
        return checked
    for method in ("open", "stat", "lstat", "resolve", "read_bytes", "read_text"):
        monkeypatch.setattr(Path, method, outside(getattr(Path, method), method))
    monkeypatch.setattr(_ChildLauncher, "launch", outside(_ChildLauncher.launch, "process"))
    monkeypatch.setattr(EphemeralArtifactGrantAuthority, "issue",
        outside(EphemeralArtifactGrantAuthority.issue, "grant"))
    direct(terminal)
    assert {"open", "stat", "process", "grant"} <= observed
    assert terminal.source.calls == 1


def test_general_acceptance_uses_locked_utc_after_trusted_usage(terminal, monkeypatch):
    import orchestrator.scheduler.core as core
    from datetime import timedelta
    original = terminal.source.usage_for
    def expired(snapshot):
        value = original(snapshot)
        class FutureUTC(datetime):
            @classmethod
            def now(cls, tz=None):
                assert terminal.store._connection.in_transaction
                return cls.fromtimestamp((snapshot.lease_expires_at + timedelta(seconds=1)).timestamp(), timezone.utc)
        monkeypatch.setattr(core, "datetime", FutureUTC)
        return value
    terminal.source.usage_for = expired
    before = _resolver_durable_state(terminal.store)
    with pytest.raises(SchedulerError):
        direct(terminal)
    assert terminal.source.calls == 1 and _resolver_durable_state(terminal.store) == before


def test_late_general_registration_cannot_use_legacy_success_callback(tmp_path, monkeypatch):
    from tests.support.attempt_acceptance import proposal_for_attempt
    case = dispatch_case(tmp_path, register=False)
    try:
        proposal = proposal_for_attempt(case.scheduler, **{name: case.identity[name]
            for name in ("run_id", "node_id", "attempt_id")})
        binding = case.scheduler._resolve_general_verification_binding(**case.identity, as_of=datetime.now(timezone.utc))
        usage = TrustedUsage().usage_for(binding.active_attempt)
        original = case.store.append_checked
        registered = []
        def register(stream_type, stream_id, key, decide):
            if key.startswith("finish:") and not registered:
                registered.append(True)
                case.journal.request(**case.registration)
            return original(stream_type, stream_id, key, decide)
        monkeypatch.setattr(case.store, "append_checked", register)
        with pytest.raises(SchedulerError):
            case.scheduler.finish_verified_attempt(**case.identity, task_sha256=proposal.task_sha256,
                completed_at=datetime.now(timezone.utc), usage=usage)
        assert registered == [True]
        assert not any(event.event_type == "AttemptSlotReleased" for event in case.store.read_stream("scheduler", "global"))
    finally:
        case.store.close()


@pytest.mark.parametrize("call_state", ["reserved", "unknown", "success"])
@pytest.mark.parametrize("entry", ["finish", "reconcile", "cancel"])
@pytest.mark.parametrize("claim", ["raw-zero", "no-effect"])
def test_known_release_cannot_replace_provider_authority_with_caller_claim(tmp_path, call_state, entry, claim):
    case = terminal_case(tmp_path) if call_state == "success" else dispatch_case(tmp_path)
    try:
        binding = case.scheduler._resolve_general_verification_binding(**case.identity, as_of=datetime.now(timezone.utc))
        usage = TrustedUsage().usage_for(binding.active_attempt).model_copy(update={
            "cost_minor": 0, "input_tokens": 0, "output_tokens": 0})
        if call_state != "success":
            reserve(case)
        if call_state == "unknown":
            case.provider_journal.record_intent(case.request, provider_id=case.request.accepted_route.provider_id,
                provider_adapter="openai_responses", request_body=case.body, provider_correlation_id="offline-correlation")
            case.provider_journal.record_outcome(case.request, outcome="unknown", failure_code="timeout")
        now = datetime.now(timezone.utc)
        if entry == "reconcile":
            case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
        if entry == "cancel":
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
        before = _resolver_durable_state(case.store)
        kwargs = {"usage": usage} if claim == "raw-zero" else {"known_no_effect": True}
        with pytest.raises((SchedulerError, GeneralWorkerJournalError)):
            if entry == "finish":
                case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="failed", **kwargs)
            elif entry == "reconcile":
                case.scheduler.reconcile_attempt(**case.identity, reconciled_at=now, outcome="failed", **kwargs)
            else:
                cancel_claim = {"usage": usage} if claim == "raw-zero" else {"no_effect_receipt_hash": "sha256:"+"a"*64}
                case.scheduler.acknowledge_cancellation(**case.identity, stopped_at=now,
                    stop_receipt_hash="sha256:"+"b"*64, **cancel_claim)
        assert _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["finish", "cancel"])
def test_known_cost_release_requires_one_trusted_usage_sample_and_replays_without_it(tmp_path, entry):
    case = terminal_case(tmp_path)
    try:
        binding = case.scheduler._resolve_general_verification_binding(**case.identity, as_of=datetime.now(timezone.utc))
        usage = TrustedUsage().usage_for(binding.active_attempt)
        now = datetime.now(timezone.utc)
        if entry == "cancel":
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
        def release():
            if entry == "finish":
                case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="failed", usage=usage)
            else:
                case.scheduler.acknowledge_cancellation(**case.identity, stopped_at=now,
                    stop_receipt_hash="sha256:"+"c"*64, usage=usage)
        release()
        assert case.source.calls == 1
        before = _resolver_durable_state(case.store)
        def forbidden(*args, **kwargs):
            raise AssertionError("historical failure sampled fresh usage")
        case.source.usage_for = forbidden
        release()
        assert _resolver_durable_state(case.store) == before
        assert not case.scheduler.recovery.recover_control_state("run-1").active_attempts
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["finish", "reconcile", "cancel"])
def test_stopped_pure_request_without_call_allows_no_effect_release(tmp_path, entry):
    case = dispatch_case(tmp_path, request_observed=False)
    try:
        now = datetime.now(timezone.utc)
        if entry == "reconcile":
            case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
            case.scheduler.reconcile_attempt(**case.identity, reconciled_at=now, outcome="failed", known_no_effect=True)
        elif entry == "finish":
            case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="failed", known_no_effect=True)
        else:
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
            case.scheduler.acknowledge_cancellation(**case.identity, stopped_at=now,
                stop_receipt_hash="sha256:"+"b"*64, no_effect_receipt_hash="sha256:"+"c"*64)
        assert not case.scheduler.recovery.recover_control_state("run-1").active_attempts
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["finish", "cancel"])
@pytest.mark.parametrize("fault", ["source-missing", "caller-zero-cost", "source-token-mismatch"])
def test_known_cost_failure_cannot_bypass_trusted_usage(tmp_path, entry, fault):
    case = terminal_case(tmp_path, trusted=fault != "source-missing")
    try:
        binding = case.scheduler._resolve_general_verification_binding(**case.identity, as_of=datetime.now(timezone.utc))
        usage = TrustedUsage().usage_for(binding.active_attempt)
        if fault == "caller-zero-cost":
            usage = usage.model_copy(update={"cost_minor": 0})
        if fault == "source-token-mismatch":
            original = case.source.usage_for
            case.source.usage_for = lambda snapshot: original(snapshot).model_copy(update={"input_tokens": 99})
        if entry == "cancel":
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
        before = _resolver_durable_state(case.store)
        with pytest.raises(SchedulerError):
            if entry == "finish":
                case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                    outcome="failed", usage=usage)
            else:
                case.scheduler.acknowledge_cancellation(**case.identity, stopped_at=datetime.now(timezone.utc),
                    stop_receipt_hash="sha256:"+"c"*64, usage=usage)
        assert _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


@pytest.mark.parametrize("fault", [None, "usage", "effect", "time"])
def test_general_reconcile_consumes_only_exact_durable_authority(tmp_path, fault):
    from datetime import timedelta
    from orchestrator.models.provider_calls import ProviderCallReconciliation
    case = dispatch_case(tmp_path)
    try:
        binding = case.scheduler._resolve_general_verification_binding(**case.identity, as_of=datetime.now(timezone.utc))
        usage = TrustedUsage().usage_for(binding.active_attempt)
        reserve(case)
        journal = case.provider_journal
        journal.record_intent(case.request, provider_id=case.request.accepted_route.provider_id,
            provider_adapter="openai_responses", request_body=case.body, provider_correlation_id="offline-correlation")
        journal.record_outcome(case.request, outcome="unknown", failure_code="timeout")
        call = journal.read(case.request)
        usage = usage.model_copy(update={"settlement_key": "provider-reconcile-" + call.stream_id})
        now = datetime.now(timezone.utc)
        case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
        proof = ProviderCallReconciliation(**{name: getattr(call, name) for name in (
            "provider_adapter", "provider_correlation_id", "run_id", "node_id", "attempt_id", "fencing_generation",
            "provider_id", "model_id", "accepted_route_id", "budget_reservation_id", "registry_manifest_hash", "request_hash")},
            provider_call_stream_id=call.stream_id, effect="received_and_charged", usage=usage, provider_request_id=None,
            evidence_source="provider_signed_receipt", evidence_digest="sha256:"+"d"*64,
            termination_receipt_hash="sha256:"+"c"*64, observed_at=now)
        journal._append_reconciliation(call.stream_id, proof, now)
        kwargs = dict(usage=usage, known_no_effect=False, reconciled_at=now)
        if fault == "usage":
            kwargs["usage"] = usage.model_copy(update={"cost_minor": 0})
        if fault == "effect":
            kwargs.update(usage=None, known_no_effect=True)
        if fault == "time":
            kwargs["reconciled_at"] = now + timedelta(seconds=1)
        before = _resolver_durable_state(case.store)
        if fault is not None:
            with pytest.raises(SchedulerError):
                case.scheduler.reconcile_attempt(**case.identity, outcome="failed", **kwargs)
            assert _resolver_durable_state(case.store) == before
        else:
            # Default unavailable usage source cannot be sampled for durable proof.
            case.scheduler.reconcile_attempt(**case.identity, outcome="failed", **kwargs)
            settled = _resolver_durable_state(case.store)
            case.scheduler.reconcile_attempt(**case.identity, outcome="failed", **kwargs)
            assert _resolver_durable_state(case.store) == settled
            assert not case.scheduler.recovery.recover_control_state("run-1").active_attempts
    finally:
        case.store.close()


def test_known_cost_release_rechecks_new_same_node_hold_after_usage(terminal):
    binding = terminal.scheduler._resolve_general_verification_binding(**terminal.identity, as_of=datetime.now(timezone.utc))
    usage = TrustedUsage().usage_for(binding.active_attempt)
    original = terminal.source.usage_for
    def hold(snapshot):
        value = original(snapshot)
        other = terminal.request.model_copy(update={"request_id": "other-request", "idempotency_key": "other-key"})
        terminal.provider_journal.record_intent(other, provider_id=other.accepted_route.provider_id,
            provider_adapter="openai_responses", request_body=terminal.body, provider_correlation_id="other-correlation")
        return value
    terminal.source.usage_for = hold
    with pytest.raises((SchedulerError, GeneralWorkerJournalError)):
        terminal.scheduler.finish_attempt(**terminal.identity, completed_at=datetime.now(timezone.utc), outcome="failed", usage=usage)
    assert terminal.source.calls == 1
    assert not any(event.event_type == "AttemptSlotReleased" for event in terminal.store.read_stream("scheduler", "global"))
