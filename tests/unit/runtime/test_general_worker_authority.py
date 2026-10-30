"""Owned general resolution has no artifact-byte claim or filesystem callback."""

import os
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.persistence import SQLiteEventStore
from orchestrator.recovery.run import RecoveredControlState, RunRecoveryError
from orchestrator.runtime.general_worker_authority import general_worker_host_scope
from orchestrator.runtime.general_worker_authority import SchedulerProviderDispatchAuthority
from orchestrator.runtime.general_worker_journal import GeneralWorkerJournalError
from tests.unit.runtime.test_attempt_execution_coordinator import prepare_application
from tests.unit.lifecycle.test_scheduler import _resolver_durable_state


@pytest.fixture
def prepared(tmp_path):
    value = prepare_application(tmp_path)
    yield value
    value.app.close()


def test_general_control_projection_never_reads_artifact_files_under_lock(prepared, monkeypatch):
    app = prepared.app
    calls = []
    def forbidden(*args, **kwargs):
        calls.append("artifact")
        raise AssertionError("artifact bytes under SQLite")
    monkeypatch.setattr(app._artifact_store, "verify_run_artifacts", forbidden)
    monkeypatch.setattr(app._artifact_store, "pending_publications_for_run", forbidden)
    monkeypatch.setattr(Path, "read_bytes", forbidden)
    monkeypatch.setattr(Path, "read_text", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    def checked(events, version):
        state = app._scheduler.recovery.recover_control_state("run-1")
        assert type(state) is RecoveredControlState
        assert not hasattr(state, "artifacts")
        binding = app._scheduler._resolve_general_verification_binding(
            **prepared.identity, as_of=prepared.clock.now)
        assert binding.context == prepared.task.context
    app._event_store.append_checked("general_control_test", "run-1", "read", checked)
    assert calls == []


def test_full_recovery_still_rejects_corrupt_artifacts(prepared):
    app = prepared.app
    digest = prepared.task.candidate_artifacts[0].digest
    app._artifact_store._path_for_digest(digest).write_bytes(b"corrupt artifact bytes")
    assert app._scheduler.recovery.recover_control_state("run-1").active_attempts
    with pytest.raises(RunRecoveryError):
        app._scheduler.recovery.recover("run-1")


def test_general_binding_matches_full_frozen_context(prepared):
    scheduler = prepared.app._scheduler
    full = scheduler.resolve_verification_binding(**prepared.identity, as_of=prepared.clock.now)
    general = scheduler._resolve_general_verification_binding(**prepared.identity, as_of=prepared.clock.now)
    assert general == full
    assert scheduler._resolve_general_active_attempt(**prepared.identity,
        as_of=prepared.clock.now) == full.active_attempt


def test_host_scope_is_shared_but_database_copy_differs(tmp_path):
    with SQLiteEventStore(tmp_path / "first.db") as first:
        scope = general_worker_host_scope(first)
        with SQLiteEventStore(tmp_path / "first.db") as second:
            assert general_worker_host_scope(second) == scope
        with SQLiteEventStore(tmp_path / "copy.db") as copied:
            assert general_worker_host_scope(copied) != scope
        os.link(tmp_path / "first.db", tmp_path / "alias.db")
        with pytest.raises(GeneralWorkerJournalError):
            general_worker_host_scope(first)


def test_host_scope_rejects_ambient_transaction_before_file_read(tmp_path, monkeypatch):
    with SQLiteEventStore(tmp_path / "events.db") as store:
        def forbidden(*args, **kwargs):
            raise AssertionError("file access under lock")
        monkeypatch.setattr(Path, "read_text", forbidden)
        def checked(events, version):
            with pytest.raises(GeneralWorkerJournalError):
                general_worker_host_scope(store)
        store.append_checked("host_scope_test", "scope", "read", checked)


def test_operation_binding_uses_exact_frozen_route_and_context(prepared):
    from dataclasses import replace
    from orchestrator.runtime.general_worker_authority import validate_operation_binding
    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal, metadata_hash
    app = prepared.app
    binding = app._scheduler._resolve_general_verification_binding(**prepared.identity, as_of=prepared.clock.now)
    scope = general_worker_host_scope(app._event_store)
    # This legacy acceptance fixture has no workspace admission; this unit
    # exercises comparison with an independently supplied host commitment.
    workspace = "sha256:" + "c" * 64
    operation = GeneralWorkerJournal(app._event_store).request(context=binding.context,
        input_commitment="sha256:" + "a" * 64,
        accepted_route_sha256=metadata_hash(binding.active_attempt.accepted_route.model_dump(mode="json")),
        workspace_identity_hash=workspace, host_scope_hash=scope, output_byte_limit=16_384)
    validate_operation_binding(operation, binding, workspace_identity_hash=workspace, host_scope_hash=scope)
    for field in ("accepted_route_sha256", "workspace_identity_hash", "host_scope_hash"):
        with pytest.raises(GeneralWorkerJournalError):
            validate_operation_binding(replace(operation, **{field: "sha256:" + "b" * 64}),
                binding, workspace_identity_hash=workspace, host_scope_hash=scope)


def dispatch_case(tmp_path, *, register=True, request_observed=True, required_check_ids=("artifact-integrity",),
    previous_status=None):
    """Offline model and broker, with actual frozen admission and durable store."""
    from orchestrator.config import ConfigCandidate, ConfigManager
    from orchestrator.lifecycle import LifecycleController, NodeSpec
    from orchestrator.models import CostSnapshotRefs, ModelMessage, ModelRequest, SQLiteProviderCallJournal
    from orchestrator.models.adapters import adapter_for
    from orchestrator.models.gateway import validate_gateway_request
    from orchestrator.routing import compile_node_contract
    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal, metadata_hash, _request_operation, _safe
    from orchestrator.runtime.general_worker_protocol import GeneralWorkerRequest
    from orchestrator.scheduler import Scheduler
    from orchestrator.security import PolicyAuthority, PolicyManifest
    from tests.support.attempt_acceptance import ADMISSION
    from tests.unit.lifecycle.test_scheduler import registry, effective_config, routed_pair, _application_limits
    from tests.unit.runtime.test_general_worker_journal import epoch

    store = SQLiteEventStore(tmp_path / "events.db")
    reg = registry()
    resolved = effective_config(reg)
    policy = PolicyManifest(authorities=(PolicyAuthority(source="system", max_permission="read-only",
        allowed_actions=("model_invoke",), allowed_tools=("model:model-1",)),))
    contract = compile_node_contract(run_id="run-1", node_id="node-1", role="coder", task_text="private-task",
        config=resolved.config, registry=reg, policy_manifest=policy, context_tokens=1000,
        max_output_tokens=500, required_capabilities=("text",))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    ConfigManager(ConfigCandidate(resolved=resolved, registry=reg)).start_run("run-1", store, workspace=workspace)
    lifecycle = LifecycleController(store)
    lifecycle.initialize_run("run-1")
    lifecycle.append_nodes("run-1", (NodeSpec(node_id="node-1", role="coder", planning_contract=contract,
        planning_contract_hash=contract.contract_hash, max_attempts=3),), expected_graph_version=0,
        idempotency_key="graph", policy_manifest=policy)
    lifecycle.start_run("run-1")
    scheduler = Scheduler(store, limits=_application_limits())
    routing, decision = routed_pair(reg, resolved.config, policy, contract_hash=contract.contract_hash)
    now = datetime.now(timezone.utc)
    accepted = scheduler.accept_routing(routing, decision, accepted_at=now-timedelta(seconds=1),
        lease_expires_at=now+timedelta(minutes=5), **{**ADMISSION, "required_check_ids": required_check_ids})
    previous = None
    if previous_status is not None:
        from orchestrator.budget import UsageRecord
        from orchestrator.models import ModelGatewayFailure
        previous = accepted
        usage = (UsageRecord(run_id="run-1", reservation_id=accepted.reservation.reservation_id,
            settlement_key="older-call-usage", currency="USD", cost_minor=1, input_tokens=1, output_tokens=2)
            if previous_status == "committed" else None)
        scheduler.finish_attempt(run_id="run-1", node_id="node-1", attempt_id=routing.attempt_id,
            fencing_generation=1, completed_at=now, outcome="failed", usage=usage, known_no_effect=usage is None)
        plan = scheduler.plan_attempt_recovery(run_id="run-1", node_id="node-1", attempt_id=routing.attempt_id,
            fencing_generation=1, source_decision=decision, contract=contract,
            failure=ModelGatewayFailure(code="rate_limited", phase="provider", outcome="known_failure", retryable=True, http_status=429))
        routing, decision = routed_pair(reg, resolved.config, policy, attempt=2, contract_hash=contract.contract_hash,
            recovery_action=plan.action, recovery_authorization=plan.authorization,
            prior_decision_hash=decision.decision_hash, previous_model_id="model-1", failure_category="transient", retry_level=1)
        accepted = scheduler.accept_routing(routing, decision, accepted_at=now,
            lease_expires_at=now+timedelta(minutes=5), **{**ADMISSION, "required_check_ids": required_check_ids})
    identity = dict(run_id="run-1", node_id="node-1", attempt_id=routing.attempt_id, fencing_generation=routing.fencing_generation)
    binding = scheduler._resolve_general_verification_binding(**identity, as_of=now)
    journal = GeneralWorkerJournal(store)
    authority = SchedulerProviderDispatchAuthority(scheduler, journal)
    registration = dict(context=binding.context, input_commitment="sha256:" + "a"*64,
        accepted_route_sha256=metadata_hash(accepted.accepted_route.model_dump(mode="json")),
        workspace_identity_hash=lifecycle.replay("run-1").workspace_identity_hash,
        host_scope_hash=general_worker_host_scope(store), output_byte_limit=16384)
    operation = journal.request(**registration) if register else _request_operation(_safe(registration))
    if register:
        prepared, ack, stop = epoch()
        operation = journal.prepare_turn(operation, phase="request", prepared=prepared)
        operation = journal.ack_turn(operation, receipt=ack)
        operation = journal.stop_turn(operation, receipt=stop, request_proposal=GeneralWorkerRequest(
            operation.context, prepared.launch_id, operation.input_commitment) if request_observed else None)
    request = ModelRequest(request_id=operation.request_id, idempotency_key=operation.idempotency_key,
        **identity, budget_reservation_id=accepted.reservation.reservation_id,
        model_id=accepted.accepted_route.model_id, accepted_route=accepted.accepted_route,
        messages=(ModelMessage(role="user", content="private-task"),), max_output_tokens=32,
        reasoning_effort=accepted.accepted_route.reasoning_effort, timeout_ms=5000,
        cost_snapshots=CostSnapshotRefs(registry_manifest_hash=reg.content_hash, price_snapshot_id=reg.content_hash,
            tokenizer_snapshot_id="sha256:"+"a"*64, fx_snapshot_id="sha256:"+"a"*64, estimator_snapshot_id="sha256:"+"a"*64))
    provider, model = validate_gateway_request(request, reg)
    body = adapter_for(provider.adapter).encode_request(request, model).body_json.encode()
    return SimpleNamespace(store=store, scheduler=scheduler, lifecycle=lifecycle, authority=authority,
        journal=journal, operation=operation, request=request, body=body, registry=reg,
        provider_journal=SQLiteProviderCallJournal(store), identity=identity, registration=registration, previous=previous)


@pytest.fixture
def dispatch(tmp_path):
    value = dispatch_case(tmp_path)
    yield value
    value.store.close()


class OfflineBroker:
    def __init__(self, change=None):
        self.calls = 0
        self.change = change

    async def acquire_provider_credential(self, **kwargs):
        from orchestrator.models.transport import ProviderCredential
        self.calls += 1
        if self.change:
            await self.change()
        return ProviderCredential("authorization", "offline-secret", "primary",
            "https://api.openai.com/v1", "model_inference")


class OfflineTransport:
    def __init__(self, change=None):
        self.calls = 0
        self.change = change

    async def post_json(self, **kwargs):
        from orchestrator.models.transport import HTTPTransportFailed
        self.calls += 1
        if self.change:
            self.change()
        # An offline ambiguous sender preserves its exact in-flight hold.
        raise HTTPTransportFailed(may_have_been_sent=True)


def gateway(case, *, authority=True, verifier=None, broker=None, transport=None):
    from orchestrator.models import ProviderModelGateway
    return ProviderModelGateway(registry=case.registry, provider_call_journal=case.provider_journal,
        dispatch_authority=case.authority if authority is True else authority,
        accepted_route_verifier=verifier, secret_broker=broker or OfflineBroker(),
        transport=transport or OfflineTransport())


def reserve(case):
    case.operation = case.authority.reserve_call(case.operation, request=case.request, request_body=case.body)


@pytest.mark.parametrize("withdrawal", ["pause", "cancel", "expiry", "fencing", "same-node-hold"])
def test_delayed_broker_pause_before_intent_sends_nothing(dispatch, monkeypatch, withdrawal):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    async def change():
        assert not dispatch.store._connection.in_transaction
        if withdrawal == "pause":
            dispatch.lifecycle.pause_run("run-1", reason_code="user_request")
        elif withdrawal == "cancel":
            dispatch.lifecycle.request_cancel("run-1", reason_code="user_request")
        elif withdrawal == "same-node-hold":
            other = dispatch.request.model_copy(update={"request_id": "other-request", "idempotency_key": "other-key"})
            dispatch.provider_journal.record_intent(other, provider_id="primary", provider_adapter="openai_responses",
                request_body=b"other-body", provider_correlation_id="other-correlation")
        elif withdrawal == "expiry":
            import orchestrator.runtime.general_worker_authority as authority_module
            class Later(datetime):
                @classmethod
                def now(cls, tz=None):
                    return datetime.now(timezone.utc) + timedelta(hours=1)
            monkeypatch.setattr(authority_module, "datetime", Later)
        else:
            # Change the exact persisted lease generation while broker is out of
            # lock. Cross-stream replay must reject it before any Provider send.
            from tests.support.worker_input import corrupt_worker_input_event
            corrupt_worker_input_event(dispatch.store, ("scheduler", "global"),
                lambda payload: payload["accepted_route"].update(fencing_generation=2))
    broker, transport = OfflineBroker(change), OfflineTransport()
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(dispatch.request))
    assert (broker.calls, transport.calls) == (1, 0)
    assert dispatch.provider_journal.read(dispatch.request) is None


def test_intent_committed_before_withdrawal_retains_inflight_hold(dispatch):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    def pause():
        assert not dispatch.store._connection.in_transaction
        assert dispatch.provider_journal.read(dispatch.request).status == "dispatching"
        dispatch.lifecycle.pause_run("run-1", reason_code="user_request")
    broker, transport = OfflineBroker(), OfflineTransport(pause)
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(dispatch.request))
    assert (broker.calls, transport.calls) == (1, 1)
    assert dispatch.provider_journal.read(dispatch.request).status == "unknown"
    assert dispatch.scheduler._ledger_for_run("run-1").get_reservation(
        dispatch.request.budget_reservation_id, run_id="run-1").status == "reserved"


def test_two_dispatch_authorities_claim_once(dispatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
    from orchestrator.scheduler import Scheduler
    barrier = Barrier(2)
    path = dispatch.store._connection.execute("PRAGMA database_list").fetchone()[2]
    def race():
        with SQLiteEventStore(path) as store:
            journal = GeneralWorkerJournal(store)
            authority = SchedulerProviderDispatchAuthority(Scheduler(store, limits=dispatch.scheduler.limits), journal)
            operation = journal.read(dispatch.operation.identity)
            barrier.wait(timeout=10)
            try:
                authority.reserve_call(operation, request=dispatch.request, request_body=dispatch.body)
                return asyncio.run(authority.is_accepted(dispatch.request))
            except GeneralWorkerJournalError:
                return False
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: race(), range(2)))
    assert sorted(outcomes) == [False, True]
    assert len([event for event in dispatch.store.read_stream("general_worker", dispatch.operation.identity.stream_id)
        if event.event_type == "GeneralWorkerCallReserved"]) == 1


def test_reopened_reserved_call_cannot_remint_dispatch_permit(dispatch):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    dispatch.authority = SchedulerProviderDispatchAuthority(dispatch.scheduler, dispatch.journal)
    with pytest.raises(GeneralWorkerJournalError):
        reserve(dispatch)
    broker, transport = OfflineBroker(), OfflineTransport()
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(dispatch.request))
    assert (broker.calls, transport.calls) == (0, 0)


def test_one_live_permit_cannot_start_two_gateway_invocations(dispatch):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    async def invoke_twice():
        entered, release = asyncio.Event(), asyncio.Event()
        async def delayed():
            entered.set()
            await release.wait()
        broker, transport = OfflineBroker(delayed), OfflineTransport()
        model = gateway(dispatch, broker=broker, transport=transport)
        first = asyncio.create_task(model.invoke(dispatch.request))
        await entered.wait()
        with pytest.raises(ModelGatewayError):
            await model.invoke(dispatch.request)
        release.set()
        with pytest.raises(ModelGatewayError):
            await first
        assert (broker.calls, transport.calls) == (1, 1)
    asyncio.run(invoke_twice())


@pytest.mark.parametrize("foreign", [False, True])
def test_plain_verifier_or_mismatched_store_cannot_authorize_general_dispatch(dispatch, tmp_path, foreign):
    from orchestrator.models import ModelGatewayError
    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
    from orchestrator.scheduler import Scheduler
    calls = []
    class Plain:
        async def is_accepted(self, request):
            calls.append(1)
            return True
    reserve(dispatch)
    broker, transport = OfflineBroker(), OfflineTransport()
    with SQLiteEventStore(tmp_path / "foreign.db") as other:
        authority = SchedulerProviderDispatchAuthority(Scheduler(other, limits=dispatch.scheduler.limits),
            GeneralWorkerJournal(other)) if foreign else None
        with pytest.raises(ModelGatewayError):
            asyncio.run(gateway(dispatch, authority=authority, verifier=Plain(),
                broker=broker, transport=transport).invoke(dispatch.request))
    assert (calls, broker.calls, transport.calls) == ([], 0, 0)


def test_registered_general_gateway_cannot_use_legacy_authority_path(dispatch):
    from orchestrator.models import ModelGatewayError
    calls = []
    class Plain:
        async def is_accepted(self, request):
            calls.append(1)
            return True
    broker, transport = OfflineBroker(), OfflineTransport()
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, authority=None, verifier=Plain(), broker=broker,
            transport=transport).invoke(dispatch.request.model_copy(update={"request_id": "rotated"})))
    assert (calls, broker.calls, transport.calls) == ([], 0, 0)


def test_host_scope_mismatch_blocks_before_external_work(dispatch, monkeypatch):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    import orchestrator.runtime.general_worker_authority as module
    monkeypatch.setattr(module, "general_worker_host_scope", lambda store: "sha256:" + "f" * 64)
    broker, transport = OfflineBroker(), OfflineTransport()
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(dispatch.request))
    assert (broker.calls, transport.calls) == (0, 0)


def captured(case):
    from tests.unit.runtime.test_general_worker_journal import provider_success, capture
    reserve(case)
    provider_success(case.store, case.request, body=case.body)
    case.operation = capture(case.journal, case.operation)


def known_failure(case, *, request=None, usage="reported"):
    request = request or case.request.model_copy(update={"request_id": "other-request", "idempotency_key": "other-key"})
    value = {"reported": {"status": "reported", "input_tokens": 100, "output_tokens": 200},
        "settled": {"status": "reported", "input_tokens": 1, "output_tokens": 2},
        "estimated": {"status": "estimated", "input_tokens": 100, "output_tokens": 200},
        "unavailable": {"status": "unavailable"}, "missing": None}[usage]
    case.provider_journal.record_intent(request, provider_id=request.accepted_route.provider_id,
        provider_adapter="openai_responses", request_body=case.body, provider_correlation_id="offline-failed-correlation")
    case.provider_journal.record_outcome(request, outcome="known_failure", failure_code="provider_error", usage=value)
    return case.provider_journal.read(request)


@pytest.mark.parametrize("purpose", ["request", "dispatch", "completion"])
@pytest.mark.parametrize("usage", ["reported", "estimated", "unavailable", "missing"])
def test_known_failure_cost_hold_blocks_general_phase(tmp_path, purpose, usage):
    case = dispatch_case(tmp_path, register=purpose != "request")
    try:
        if purpose == "request":
            case.operation = case.journal.request(**case.registration)
        if purpose == "completion":
            captured(case)
        known_failure(case, usage=usage)
        before = _resolver_durable_state(case.store)
        with pytest.raises(GeneralWorkerJournalError):
            case.authority.resolve_eligibility(case.operation, purpose=purpose)
        assert _resolver_durable_state(case.store) == before
    finally:
        case.store.close()


def test_known_failure_cost_hold_blocks_fresh_reservation(dispatch):
    known_failure(dispatch)
    before = _resolver_durable_state(dispatch.store)
    with pytest.raises(GeneralWorkerJournalError):
        reserve(dispatch)
    assert _resolver_durable_state(dispatch.store) == before
    assert dispatch.journal.read(dispatch.operation.identity).call is None


@pytest.mark.parametrize("own", [False, True], ids=["unrelated-same-attempt", "own-failed-call"])
def test_known_failure_cost_hold_blocks_gateway_before_broker(dispatch, own):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    known_failure(dispatch, request=dispatch.request if own else None)
    before = _resolver_durable_state(dispatch.store)
    with pytest.raises(GeneralWorkerJournalError):
        dispatch.authority.resolve_eligibility(dispatch.operation, purpose="dispatch")
    broker, transport = OfflineBroker(), OfflineTransport()
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(dispatch.request))
    assert (broker.calls, transport.calls) == (0, 0)
    assert _resolver_durable_state(dispatch.store) == before


def test_known_failure_appearing_during_broker_blocks_intent(dispatch):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    async def change():
        assert not dispatch.store._connection.in_transaction
        known_failure(dispatch)
    broker, transport = OfflineBroker(change), OfflineTransport()
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(dispatch.request))
    assert (broker.calls, transport.calls) == (1, 0)
    assert dispatch.provider_journal.read(dispatch.request) is None


@pytest.mark.parametrize("settled", ["committed", "released"])
def test_settled_older_same_node_known_failure_does_not_block_completion(tmp_path, settled):
    case = dispatch_case(tmp_path, previous_status=settled)
    try:
        old = case.previous.accepted_route
        request = case.request.model_copy(update={"request_id": "older-request", "idempotency_key": "older-key",
            "attempt_id": old.attempt_id, "fencing_generation": old.fencing_generation,
            "budget_reservation_id": old.budget_reservation_id, "accepted_route": old})
        call = known_failure(case, request=request, usage="settled" if settled == "committed" else "unavailable")
        assert call.attempt_id != case.identity["attempt_id"] and call.node_id == case.identity["node_id"]
        assert case.scheduler._ledger_for_run("run-1").get_reservation(call.budget_reservation_id, run_id="run-1").status == settled
        captured(case)
        assert case.authority.resolve_eligibility(case.operation, purpose="completion").context == case.operation.context
    finally:
        case.store.close()


def test_own_captured_success_is_only_completion_or_acceptance_authority(dispatch):
    from orchestrator.runtime.contracts import ArtifactRef, VerificationTask
    from orchestrator.runtime.verification_journal import VerifierProposalJournal
    from tests.unit.runtime.test_general_worker_journal import epoch
    from tests.unit.runtime.test_verification_journal import _evidence
    captured(dispatch)
    assert dispatch.authority.resolve_eligibility(dispatch.operation, purpose="completion").context == dispatch.operation.context
    for purpose in ("request", "dispatch", "acceptance"):
        with pytest.raises(GeneralWorkerJournalError):
            dispatch.authority.resolve_eligibility(dispatch.operation, purpose=purpose)
    assert asyncio.run(dispatch.authority.is_accepted(dispatch.request)) is False
    prepared, ack, stop = epoch("9c483536-3c61-41e7-977b-6559b2732937")
    operation = dispatch.journal.prepare_turn(dispatch.operation, phase="complete", prepared=prepared)
    operation = dispatch.journal.ack_turn(operation, receipt=ack)
    operation = dispatch.journal.stop_turn(operation, receipt=stop)
    task = VerificationTask(context=operation.context, candidate_artifacts=(ArtifactRef(
        digest=operation.capture.output_digest, size_bytes=operation.capture.output_byte_count,
        artifact_type="text", media_type="text/plain; charset=utf-8"),),
        required_check_ids=("artifact-integrity",), acceptance_contract="maestro.artifact-verification/v1")
    proposal = VerifierProposalJournal(dispatch.store).record(task, _evidence(task))
    operation = dispatch.journal.record_proposal(operation, task_sha256=proposal.task_sha256,
        evidence_sha256=proposal.evidence_sha256)
    assert dispatch.authority.resolve_eligibility(operation, purpose="acceptance").context == task.context
    with pytest.raises(GeneralWorkerJournalError):
        dispatch.authority.resolve_eligibility(operation, purpose="dispatch")


@pytest.mark.parametrize("hold", ["dispatching", "unknown", "known_success", "effect", "workspace"])
def test_captured_success_never_exempts_unrelated_holds(dispatch, hold):
    from orchestrator.persistence import EventDraft
    from tests.unit.runtime.test_general_worker_journal import provider_success
    captured(dispatch)
    if hold in {"dispatching", "unknown", "known_success"}:
        request = dispatch.request.model_copy(update={"request_id": "other-request", "idempotency_key": "other-key"})
        if hold == "known_success":
            provider_success(dispatch.store, request, body=dispatch.body)
        else:
            dispatch.provider_journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
                request_body=dispatch.body, provider_correlation_id="other-correlation")
            if hold == "unknown":
                dispatch.provider_journal.record_outcome(request, outcome="unknown")
    elif hold == "effect":
        dispatch.store.append("budget", "run-1", dispatch.store.current_version("budget", "run-1"),
            [EventDraft("EffectIntentRecorded", {"effect_id": "effect-one", "recovery_class": "manual_only"},
                **dispatch.identity, correlation_id="run-1", causation_id=dispatch.request.accepted_route.decision_id)], "effect-one")
    else:
        workspace_hash = dispatch.operation.workspace_identity_hash
        dispatch.store.append("workspace_executions", "workspace:" + workspace_hash[7:], 0,
            [EventDraft("WorkspaceExecutionIntended", {"request_id": "workspace-one", "workspace_identity_hash": workspace_hash,
                "request_hash": "sha256:"+"a"*64, "profile_hash": "sha256:"+"b"*64},
                **dispatch.identity, causation_id=dispatch.request.accepted_route.decision_id)], "workspace-one")
    with pytest.raises(GeneralWorkerJournalError):
        dispatch.authority.resolve_eligibility(dispatch.operation, purpose="completion")


@pytest.mark.parametrize("field,value", [("request_id", "rotated"), ("idempotency_key", "rotated"),
    ("attempt_id", "other-attempt"), ("fencing_generation", 2)])
def test_request_rotation_never_bypasses_owned_dispatch(dispatch, field, value):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    request = dispatch.request.model_copy(update={field: value})
    if field in {"attempt_id", "fencing_generation"}:
        request = request.model_copy(update={"accepted_route": request.accepted_route.model_copy(update={field: value})})
    broker, transport = OfflineBroker(), OfflineTransport()
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(request))
    assert (broker.calls, transport.calls) == (0, 0)


def test_general_dispatch_checked_callbacks_use_no_files_or_external_callbacks(dispatch, monkeypatch):
    from orchestrator.models import ModelGatewayError
    import orchestrator.runtime.general_worker_authority as module
    observed = []
    for name in ("read_text", "read_bytes", "open", "stat"):
        original = getattr(Path, name)
        def checked_file(*args, _original=original, **kwargs):
            assert not dispatch.store._connection.in_transaction
            return _original(*args, **kwargs)
        monkeypatch.setattr(Path, name, checked_file)
    class LockedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            assert dispatch.store._connection.in_transaction
            observed.append("locked-clock")
            return datetime.now(timezone.utc)
    monkeypatch.setattr(module, "datetime", LockedClock)
    async def broker_check():
        assert not dispatch.store._connection.in_transaction
    reserve(dispatch)
    model = gateway(dispatch, broker=OfflineBroker(broker_check))
    with pytest.raises(ModelGatewayError):
        asyncio.run(model.invoke(dispatch.request))
    assert dispatch.provider_journal.read(dispatch.request).status == "unknown"
    assert len(observed) == 3
    assert not dispatch.authority._pending and not dispatch.authority._claims


def test_changed_gateway_encoding_cannot_record_intent(dispatch):
    from orchestrator.models import ModelGatewayError
    # The reservation hash must match Gateway's own actual encoding.
    dispatch.operation = dispatch.authority.reserve_call(dispatch.operation,
        request=dispatch.request, request_body=b"different-encoded-body")
    broker, transport = OfflineBroker(), OfflineTransport()
    with pytest.raises(ModelGatewayError):
        asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(dispatch.request))
    assert (broker.calls, transport.calls) == (1, 0)
    assert dispatch.provider_journal.read(dispatch.request) is None
    assert not dispatch.authority._pending and not dispatch.authority._claims


def test_dead_claim_owner_never_restores_dispatch_permit(dispatch):
    import gc
    import weakref
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    async def scenario():
        entered = asyncio.Event()
        async def owner():
            assert await dispatch.authority.is_accepted(dispatch.request)
            entered.set()
            await asyncio.Event().wait()
        task = asyncio.create_task(owner())
        reference = weakref.ref(task)
        await entered.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        del task
        await asyncio.sleep(0)
        gc.collect()
        assert reference() is None
        assert not dispatch.authority._pending and not dispatch.authority._claims
        for authority in (dispatch.authority, SchedulerProviderDispatchAuthority(dispatch.scheduler, dispatch.journal)):
            broker, transport = OfflineBroker(), OfflineTransport()
            with pytest.raises(ModelGatewayError):
                await gateway(dispatch, authority=authority, broker=broker, transport=transport).invoke(dispatch.request)
            assert (broker.calls, transport.calls) == (0, 0)
    asyncio.run(scenario())


def test_general_registration_during_broker_cannot_fall_through_legacy_intent(tmp_path):
    from orchestrator.models import ModelGatewayError
    case = dispatch_case(tmp_path, register=False)
    class Plain:
        async def is_accepted(self, request):
            return True
    async def register():
        case.journal.request(**case.registration)
    broker, transport = OfflineBroker(register), OfflineTransport()
    try:
        with pytest.raises(ModelGatewayError):
            asyncio.run(gateway(case, authority=None, verifier=Plain(), broker=broker,
                transport=transport).invoke(case.request))
        assert (broker.calls, transport.calls) == (1, 0)
        assert case.provider_journal.read(case.request) is None
    finally:
        case.store.close()


def test_non_general_atomic_guard_creates_no_phantom_general_events(tmp_path):
    from orchestrator.models import ModelGatewayError
    case = dispatch_case(tmp_path, register=False)
    class Plain:
        async def is_accepted(self, request):
            return True
    broker, transport = OfflineBroker(), OfflineTransport()
    try:
        with pytest.raises(ModelGatewayError):
            asyncio.run(gateway(case, authority=None, verifier=Plain(), broker=broker,
                transport=transport).invoke(case.request))
        assert (broker.calls, transport.calls) == (1, 1)
        assert case.provider_journal.read(case.request).status == "unknown"
        assert case.store.stream_ids("general_worker") == []
    finally:
        case.store.close()


def test_changed_provider_journal_dependency_cannot_escape_atomic_store(dispatch, tmp_path):
    from orchestrator.models import ModelGatewayError, SQLiteProviderCallJournal
    reserve(dispatch)
    with SQLiteEventStore(tmp_path / "foreign.db") as other:
        async def change():
            dispatch.authority.provider_journal = SQLiteProviderCallJournal(other)
        broker, transport = OfflineBroker(change), OfflineTransport()
        with pytest.raises(ModelGatewayError):
            asyncio.run(gateway(dispatch, broker=broker, transport=transport).invoke(dispatch.request))
        assert (broker.calls, transport.calls) == (1, 0)
        assert other.stream_ids("provider_call") == []
        assert dispatch.provider_journal.read(dispatch.request) is None


def test_owned_reservation_rejects_outer_transaction_before_scope_or_permit(dispatch, monkeypatch):
    touched = []
    def forbidden(*args, **kwargs):
        touched.append("filesystem")
        raise AssertionError("ambient transaction performed file read")
    monkeypatch.setattr(Path, "read_text", forbidden)
    before = dispatch.journal.read(dispatch.operation.identity)
    def outer(events, version):
        with pytest.raises(GeneralWorkerJournalError):
            dispatch.authority.reserve_call(dispatch.operation, request=dispatch.request, request_body=dispatch.body)
        assert not dispatch.authority._pending and not dispatch.authority._claims
        assert dispatch.journal.read(dispatch.operation.identity) == before
        raise RuntimeError("rollback outer transaction")
    with pytest.raises(RuntimeError, match="rollback outer transaction"):
        dispatch.store.append_checked("scheduler", "global", "outer-reserve", outer)
    assert touched == []
    assert dispatch.journal.read(dispatch.operation.identity) == before


@pytest.mark.parametrize("entry", ["invoke", "is_accepted"])
def test_general_dispatch_rejects_ambient_transaction_before_external_work(dispatch, monkeypatch, entry):
    from orchestrator.models import ModelGatewayError
    reserve(dispatch)
    touched = []
    def forbidden(*args, **kwargs):
        touched.append("filesystem")
        raise AssertionError("ambient transaction performed file read")
    monkeypatch.setattr(Path, "read_text", forbidden)
    class Plain:
        async def is_accepted(self, request):
            touched.append("route-callback")
            return True
    broker, transport = OfflineBroker(), OfflineTransport()
    model = gateway(dispatch, verifier=Plain(), broker=broker, transport=transport)
    def outer(events, version):
        if entry == "invoke":
            with pytest.raises(ModelGatewayError):
                asyncio.run(model.invoke(dispatch.request))
        else:
            assert asyncio.run(dispatch.authority.is_accepted(dispatch.request)) is False
    dispatch.store.append_checked("scheduler", "global", "outer-invoke", outer)
    assert (touched, broker.calls, transport.calls) == ([], 0, 0)
    assert dispatch.provider_journal.read(dispatch.request) is None
    assert not dispatch.authority._pending and not dispatch.authority._claims


def test_interrupted_request_with_exact_stop_can_claim_fresh_pure_request_epoch(tmp_path):
    from tests.unit.runtime.test_general_worker_journal import epoch
    from orchestrator.runtime.general_worker_protocol import GeneralWorkerRequest
    case = dispatch_case(tmp_path, request_observed=False)
    try:
        assert case.authority.resolve_eligibility(case.operation, purpose="request").context == case.operation.context
        prepared, ack, stop = epoch("9c483536-3c61-41e7-977b-6559b2732937")
        operation = case.journal.prepare_turn(case.operation, phase="request", prepared=prepared)
        operation = case.journal.ack_turn(operation, receipt=ack)
        operation = case.journal.stop_turn(operation, receipt=stop, request_proposal=GeneralWorkerRequest(
            operation.context, prepared.launch_id, operation.input_commitment))
        case.operation = operation
        reserve(case)
        assert len(case.operation.turns) == 2 and case.operation.phase == "call_reserved"
        with pytest.raises(GeneralWorkerJournalError):
            case.authority.resolve_eligibility(case.operation, purpose="request")
        assert case.provider_journal.read(case.request) is None
    finally:
        case.store.close()


@pytest.mark.parametrize("state", ["reserved", "unacknowledged", "unstopped"])
def test_interrupted_request_without_exact_stop_cannot_retry(tmp_path, state):
    from tests.unit.runtime.test_general_worker_journal import epoch
    case = dispatch_case(tmp_path, register=state == "reserved")
    try:
        if state == "reserved":
            reserve(case)
            operation = case.operation
        else:
            operation = case.journal.request(**case.registration)
            prepared, ack, _stop = epoch()
            operation = case.journal.prepare_turn(operation, phase="request", prepared=prepared)
            if state == "unstopped":
                operation = case.journal.ack_turn(operation, receipt=ack)
        assert case.journal.read(operation.identity) == operation
        with pytest.raises(GeneralWorkerJournalError):
            case.authority.resolve_eligibility(operation, purpose="request")
        with pytest.raises(GeneralWorkerJournalError):
            case.journal.prepare_turn(operation, phase="request",
                prepared=epoch("9c483536-3c61-41e7-977b-6559b2732937")[0])
    finally:
        case.store.close()
