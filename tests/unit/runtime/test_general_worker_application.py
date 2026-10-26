"""Public general API readiness, owned composition and startup validation."""

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import inspect
from types import SimpleNamespace

import pytest

from orchestrator.application import ApplicationNotReady, ControlPlaneApplication, StartupRecoveryFailed
from orchestrator.models import ProviderModelGateway
from orchestrator.runtime.general_worker_execution import GeneralWorkerUnavailable
from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
from orchestrator.runtime.general_worker_process import GeneralWorkerProcess
from tests.unit.lifecycle.test_scheduler import _application_limits, _resolver_durable_state
from tests.unit.runtime.test_attempt_execution_coordinator import TrustedUsage
from tests.unit.runtime.test_general_worker_authority import OfflineBroker
from tests.unit.runtime.test_general_worker_execution import execution_case, OfflineSuccessTransport
from tests.unit.runtime.test_general_worker_recovery import checkpoint, no_external_work
from tests.support.worker_input import corrupt_worker_input_event


@pytest.fixture
def case(tmp_path):
    value = execution_case(tmp_path)
    try:
        yield value
    finally:
        value.app.close()


def configured_reopen(case, *, configure=True, broker=None, transport=None, usage_source=True,
    host_workspace=None, host_registry=None):
    from orchestrator.runtime.general_worker_execution import GeneralWorkerHostConfig
    private, workspace = case.service._owner_root.parent, case.service._workspace
    registry = case.service._gateway.registry
    case.app.close()
    broker, transport = broker or OfflineBroker(), transport or OfflineSuccessTransport()
    config = GeneralWorkerHostConfig(workspace=host_workspace or workspace, registry=host_registry or registry,
        secret_broker=broker, transport=transport) if configure else None
    app = ControlPlaneApplication(private / "events.db", limits=_application_limits(), artifact_root=private / "artifacts",
        usage_source=TrustedUsage() if usage_source else None, general_worker_config=config)
    case.app = app
    case.service = app._general_worker_execution
    case.journal = GeneralWorkerJournal(app._event_store)
    case.broker, case.transport = broker, transport
    return app


@pytest.mark.parametrize("configured", [False, True])
def test_startup_validates_without_dispatch(case, monkeypatch, configured):
    checkpoint(case, monkeypatch)
    observed = []
    original = GeneralWorkerJournal.validate_operations
    def validate(journal):
        assert journal.event_store._connection.in_transaction
        observed.append(journal.event_store)
        return original(journal)
    def forbidden(*args, **kwargs):
        pytest.fail("startup dispatched general work")
    monkeypatch.setattr(GeneralWorkerJournal, "validate_operations", validate)
    monkeypatch.setattr(GeneralWorkerProcess, "prepare_launch", forbidden)
    monkeypatch.setattr(GeneralWorkerProcess, "start_turn", forbidden)
    monkeypatch.setattr(ProviderModelGateway, "invoke", forbidden)
    monkeypatch.setattr(OfflineBroker, "acquire_provider_credential", forbidden)
    monkeypatch.setattr(OfflineSuccessTransport, "post_json", forbidden)
    app = configured_reopen(case, configure=configured)
    assert observed == [app._event_store]
    assert app.startup_report.held_attempts == 1


def test_startup_rejects_invalid_general_journal_while_unconfigured(case, monkeypatch):
    async def cut(*args, **kwargs):
        raise RuntimeError("offline requested checkpoint")
    with monkeypatch.context() as patch:
        patch.setattr(case.service, "_turn", cut)
        asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    corrupt_worker_input_event(case.app._event_store, ("general_worker", case.identity.stream_id),
        lambda payload: payload["data"].update(input_commitment="invalid"))
    private = case.service._owner_root.parent
    case.app.close()
    with pytest.raises(StartupRecoveryFailed):
        ControlPlaneApplication(private / "events.db", limits=_application_limits(), artifact_root=private / "artifacts")


@pytest.mark.parametrize("entry", ["execute_general_worker_task", "recover_general_worker_task"])
def test_public_methods_fail_before_ready_or_unconfigured(tmp_path, monkeypatch, entry):
    bootstrap = ControlPlaneApplication._bootstrap
    def inspect_bootstrap(app):
        with pytest.raises(ApplicationNotReady):
            asyncio.run(getattr(app, entry)(identity=None, task_text=None))
        return bootstrap(app)
    monkeypatch.setattr(ControlPlaneApplication, "_bootstrap", inspect_bootstrap)
    app = ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits())
    try:
        with pytest.raises(GeneralWorkerUnavailable) as caught:
            asyncio.run(getattr(app, entry)(identity=None, task_text=None))
        assert caught.value.__cause__ is None and caught.value.__context__ is None
    finally:
        app.close()
    with pytest.raises(ApplicationNotReady):
        asyncio.run(getattr(app, entry)(identity=None, task_text=None))


@pytest.mark.parametrize("entry", ["execute_general_worker_task", "recover_general_worker_task"])
def test_explicit_application_reopen_requires_exact_input(case, monkeypatch, entry):
    checkpoint(case, monkeypatch)
    app = configured_reopen(case)
    operation = case.journal.read(case.identity)
    before = _resolver_durable_state(app._event_store)
    wrong = asyncio.run(getattr(app, entry)(identity=case.identity, task_text=case.text + " changed"))
    assert wrong.status == "blocked"
    assert _resolver_durable_state(app._event_store) == before
    assert case.journal.read(case.identity).capture == operation.capture
    assert case.broker.calls == 0 and case.transport.calls == []
    result = asyncio.run(getattr(app, entry)(identity=case.identity, task_text=case.text))
    assert result.status == "accepted", result
    assert case.broker.calls == 0 and case.transport.calls == []
    no_external_work(case, monkeypatch, verifier=True)
    assert asyncio.run(getattr(app, entry)(identity=case.identity, task_text=case.text)) == result


def test_application_owns_exact_general_dependencies_and_public_identity_only(case):
    app = configured_reopen(case)
    service = app._general_worker_execution
    authority = service._gateway.dispatch_authority
    assert service._scheduler is app._scheduler
    assert service._inputs is app._worker_inputs
    assert service._artifacts is app._artifact_store
    assert service._grants is app._artifact_grants
    assert service._coordinator is app._acceptance_coordinator
    assert service._proposals._event_store is app._event_store is authority.event_store
    assert service._gateway.provider_call_journal is authority.provider_journal
    for method in (app.execute_general_worker_task, app.recover_general_worker_task):
        assert set(inspect.signature(method).parameters) == {"identity", "task_text"}
        for override in ("usage", "clock", "request", "model_request", "cursor", "success", "service"):
            with pytest.raises(TypeError):
                asyncio.run(method(identity=case.identity, task_text=case.text, **{override: object()}))


@pytest.mark.parametrize("identity", [None, {}, "not-an-identity"])
def test_public_invalid_identity_is_detached_without_external_work(case, monkeypatch, identity):
    app = configured_reopen(case)
    no_external_work(case, monkeypatch, verifier=True)
    with pytest.raises(GeneralWorkerUnavailable) as caught:
        asyncio.run(app.execute_general_worker_task(identity=identity, task_text=case.text))
    assert caught.value.__context__ is None and caught.value.__cause__ is None


def test_general_constructor_rejects_foreign_bound_service_without_using_it(case):
    private = case.service._owner_root.parent
    class ForeignService:
        def __getattr__(self, name):
            pytest.fail("application consulted a foreign bound service")
    with pytest.raises(StartupRecoveryFailed) as caught:
        ControlPlaneApplication(private / "other.db", limits=_application_limits(), general_worker_config=ForeignService())
    assert caught.value.__cause__ is None and caught.value.__context__ is None
    assert caught.value.__suppress_context__ is True


@pytest.mark.parametrize("change", [dict(attempt_id="rotated-attempt"), dict(fencing_generation=2)])
def test_same_node_unknown_changed_identity_cannot_create_work(case, monkeypatch, change):
    checkpoint(case, monkeypatch, "dispatching")
    app = configured_reopen(case)
    no_external_work(case, monkeypatch, verifier=True)
    before = _resolver_durable_state(app._event_store)
    result = asyncio.run(app.execute_general_worker_task(identity=replace(case.identity, **change), task_text=case.text))
    assert result.status == "blocked"
    assert _resolver_durable_state(app._event_store) == before
    assert case.journal.read(case.identity).phase == "call_reserved"


def test_general_exports_and_default_unavailable_services(case):
    from orchestrator.application import GeneralWorkerHostConfig
    from orchestrator.runtime import GeneralWorkerIdentity, GeneralWorkerResult, GeneralWorkerExecutionService
    assert type(case.identity) is GeneralWorkerIdentity
    assert isinstance(case.service, GeneralWorkerExecutionService)
    from orchestrator.models.transport import UnavailableSecretBroker, SystemdProviderHTTPSTransport
    private, workspace, registry = case.service._owner_root.parent, case.service._workspace, case.service._gateway.registry
    case.app.close()
    with ControlPlaneApplication(private / "events.db", limits=_application_limits(), artifact_root=private / "artifacts",
        general_worker_config=GeneralWorkerHostConfig(workspace=workspace, registry=registry)) as app:
        gateway = app._general_worker_execution._gateway
        assert type(gateway.secret_broker) is UnavailableSecretBroker
        assert type(gateway.transport) is SystemdProviderHTTPSTransport
        result = asyncio.run(app.execute_general_worker_task(identity=case.identity, task_text=case.text))
        assert type(result) is GeneralWorkerResult and result.status == "blocked"


@pytest.mark.parametrize("changed", ["workspace", "registry"])
def test_reopened_config_must_match_operation_immutable_bindings(case, monkeypatch, tmp_path, changed):
    checkpoint(case, monkeypatch)
    workspace = tmp_path / "other-workspace"
    workspace.mkdir()
    registry = case.service._gateway.registry
    model, = registry.models
    other_registry = registry.model_copy(update={"models": (model.model_copy(update={"remote_model": "other-remote"}),)})
    app = configured_reopen(case, host_workspace=workspace if changed == "workspace" else None,
        host_registry=other_registry if changed == "registry" else None)
    no_external_work(case, monkeypatch, verifier=True)
    before = _resolver_durable_state(app._event_store)
    result = asyncio.run(app.recover_general_worker_task(identity=case.identity, task_text=case.text))
    assert result.status == "blocked"
    assert _resolver_durable_state(app._event_store) == before


def test_same_node_unknown_other_attempt_blocks(case, monkeypatch):
    """A late older legacy intent cannot be hidden by valid new admission."""
    from orchestrator.models import ModelGatewayFailure
    from orchestrator.runtime.general_worker_journal import GeneralWorkerIdentity
    from tests.unit.lifecycle.test_scheduler import routed_pair
    app = configured_reopen(case)
    scheduler = app._scheduler
    old = dict(run_id=case.identity.run_id, node_id=case.identity.node_id,
        attempt_id=case.identity.attempt_id, fencing_generation=case.identity.fencing_generation)
    now = datetime.now(timezone.utc)
    task = app._worker_inputs._prepare_general_worker_task(**old, task_text=case.text)
    binding = scheduler._resolve_general_verification_binding(**old, as_of=now)
    older_request = case.service._model_request(SimpleNamespace(identity=case.identity,
        context=task.context, request_id="older-legacy-request", idempotency_key="older-legacy-call"), task, binding)
    scheduler.finish_attempt(**old, completed_at=now, outcome="failed", known_no_effect=True)
    plan = scheduler.plan_attempt_recovery(**old, source_decision=case.decision, contract=case.contract,
        failure=ModelGatewayFailure(code="rate_limited", phase="provider", outcome="known_failure", retryable=True, http_status=429))
    request, decision = routed_pair(case.registry, case.config, case.policy, attempt=2,
        contract_hash=case.contract.contract_hash, recovery_action=plan.action, recovery_authorization=plan.authorization,
        prior_decision_hash=case.decision.decision_hash, previous_model_id="model-1", failure_category="transient", retry_level=1)
    assessment, = decision.candidate_assessments
    decision = decision.model_copy(update={"candidate_assessments": (assessment.model_copy(update={
        "estimated_cost": assessment.estimated_cost.model_copy(update={"price_snapshot_id": case.registry.content_hash})}),)})
    app.accept_routing_with_input(request, decision, task_text=case.text, accepted_at=now,
        lease_expires_at=now+timedelta(minutes=5))
    current = dict(run_id=request.run_id, node_id=request.node_id, attempt_id=request.attempt_id, fencing_generation=2)
    # Demonstrate valid exact new admission before the late older effect arrives.
    assert app._worker_inputs._prepare_general_worker_task(**current, task_text=case.text).context.attempt_id == request.attempt_id
    journal = case.service._gateway.provider_call_journal
    journal.record_intent(older_request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"offline-older-body", provider_correlation_id="older-legacy")
    journal.record_outcome(older_request, outcome="unknown", failure_code="outcome_unknown")
    no_external_work(case, monkeypatch, verifier=True)
    before = _resolver_durable_state(app._event_store)
    identity = GeneralWorkerIdentity(**current)
    result = asyncio.run(app.execute_general_worker_task(identity=identity, task_text=case.text))
    assert result.status == "blocked" and case.journal.read(identity) is None
    assert _resolver_durable_state(app._event_store) == before
    assert journal.read(older_request).status == "unknown"


@pytest.mark.parametrize("visible", ["database", "artifacts", "workspace-alias"])
def test_configured_application_rejects_visible_storage_before_dispatch(tmp_path, monkeypatch, visible):
    from orchestrator.runtime.general_worker_execution import GeneralWorkerHostConfig
    from tests.unit.lifecycle.test_scheduler import registry
    workspace, private = tmp_path / "workspace", tmp_path / "private"
    workspace.mkdir()
    private.mkdir()
    configured_workspace = workspace
    database = (workspace if visible == "database" else private) / "events.db"
    artifacts = (workspace if visible == "artifacts" else private) / "artifacts"
    if visible == "workspace-alias":
        configured_workspace = tmp_path / "workspace-alias"
        configured_workspace.symlink_to(private, target_is_directory=True)
    def forbidden(*args, **kwargs):
        pytest.fail("visible application storage dispatched work")
    monkeypatch.setattr(GeneralWorkerProcess, "prepare_launch", forbidden)
    monkeypatch.setattr(GeneralWorkerProcess, "start_turn", forbidden)
    monkeypatch.setattr(ProviderModelGateway, "invoke", forbidden)
    monkeypatch.setattr(OfflineBroker, "acquire_provider_credential", forbidden)
    monkeypatch.setattr(OfflineSuccessTransport, "post_json", forbidden)
    with pytest.raises(StartupRecoveryFailed) as caught:
        ControlPlaneApplication(database, limits=_application_limits(), artifact_root=artifacts,
            general_worker_config=GeneralWorkerHostConfig(workspace=configured_workspace, registry=registry(),
                secret_broker=OfflineBroker(), transport=OfflineSuccessTransport()))
    assert caught.value.__cause__ is None and caught.value.__context__ is None
    assert caught.value.__suppress_context__ is True


@pytest.mark.parametrize("invalid", ["relative-workspace", "foreign-registry", "missing-artifacts"])
def test_host_configuration_rejects_invalid_or_unavailable_dependencies(case, invalid):
    from orchestrator.runtime.general_worker_execution import GeneralWorkerHostConfig
    from pathlib import Path
    workspace, registry = case.service._workspace, case.service._gateway.registry
    if invalid == "missing-artifacts":
        with pytest.raises(StartupRecoveryFailed):
            ControlPlaneApplication(case.service._owner_root.parent / "empty.db", limits=_application_limits(),
                general_worker_config=GeneralWorkerHostConfig(workspace=workspace, registry=registry))
    else:
        with pytest.raises(GeneralWorkerUnavailable) as caught:
            GeneralWorkerHostConfig(workspace=Path("relative") if invalid == "relative-workspace" else workspace,
                registry=object() if invalid == "foreign-registry" else registry)
        assert caught.value.__cause__ is None and caught.value.__context__ is None
