"""Recovery prepares only the exact committed successor using owned UTC time."""

from datetime import datetime, timedelta, timezone
import subprocess

import pytest

from orchestrator.application import ApplicationNotReady, ControlPlaneApplication
from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.recovery_continuation import (
    RecoveryContinuationService, RecoveryRoutingInputs,
)
from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError
from orchestrator.runtime.worker_input import WorkerInputError
from orchestrator.runtime.worker_input_service import WorkerInputService
from tests.support.recovery_continuation import durable_state, recovery_case, routing_observations
from tests.unit.lifecycle.test_scheduler import _application_limits


TEXT = "implement a small change"


def live_arguments(case):
    now = datetime.now(timezone.utc)
    return dict(
        run_id="run-1", node_id="node-1", source_attempt_id="attempt-node-1-1",
        source_fencing_generation=1, task_text=TEXT,
        routing_inputs=RecoveryRoutingInputs(**routing_observations(case, as_of=now)),
        accepted_at=now - timedelta(seconds=1), lease_expires_at=now + timedelta(minutes=2),
    )


def test_reopen_recovery_prepares_exact_successor_task(tmp_path, monkeypatch):
    # Catches returning admission metadata without a real task, or preparing the failed source.
    database = tmp_path / "reopen.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store, routing_budget=True)
        arguments = live_arguments(case)
        source_hash = store.read_stream("scheduler", "global")[0].payload[
            "verification_admission"]["input_manifest_hash"]
        source_event_count = len(store.read_stream("scheduler", "global"))
    from orchestrator.models.gateway import ModelGateway
    from orchestrator.models.transport import UrllibHTTPSTransport
    from orchestrator.runtime.worker_process import IsolatedWorkerProcess
    from orchestrator.secrets.broker import AuditedSecretBroker

    def forbidden(*args, **kwargs):
        pytest.fail("task preparation crossed an execution boundary")

    for target, method in (
        (IsolatedWorkerProcess, "__init__"), (ModelGateway, "__init__"),
        (UrllibHTTPSTransport, "__init__"), (subprocess, "Popen"),
        (AuditedSecretBroker, "acquire_provider_credential_decision"),
    ):
        monkeypatch.setattr(target, method, forbidden)
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        result = app.continue_recovery(**arguments)
        assert result.status == "prepared" and result.reason == "recovery_task_prepared"
        task, route = result.task, result.accepted_route
        assert task is not None and route is not None
        assert (task.context.run_id, task.context.node_id, task.context.attempt_id,
                task.context.fencing_generation) == ("run-1", "node-1", route.attempt_id, 2)
        assert task.context.input_manifest_hash == source_hash
        assert task.task_text == TEXT and task.role == "coder"
        assert task.input_artifacts == task.tool_capabilities == ()
        assert len(app._event_store.read_stream("scheduler", "global")) == source_event_count + 1
        assert app._scheduler.agents.replay("run-1").total_created == 2
        before = durable_state(app._event_store)
        arguments["routing_inputs"] = None
        repeated = app.continue_recovery(**arguments)
        assert repeated.status == "prepared" and repeated.task == task
        assert repeated.accepted_route == route and durable_state(app._event_store) == before


@pytest.mark.parametrize("clock_kind", ["recording", "raising"])
def test_recovery_preparation_never_invokes_injected_clock(tmp_path, clock_kind):
    # Catches reusing the public injected clock, even outside the lock or as cached time.
    with SQLiteEventStore(tmp_path / "clock.db") as store:
        case = recovery_case(store, routing_budget=True)
        calls = []

        def clock():
            calls.append("called")
            if clock_kind == "raising":
                raise RuntimeError("unowned clock")
            return datetime(2000, 1, 1, tzinfo=timezone.utc)

        inputs = WorkerInputService(case.scheduler, clock=clock)
        service = RecoveryContinuationService(case.scheduler, inputs)
        result = service.continue_recovery(**live_arguments(case))
        assert result.status == "prepared" and result.task is not None
        assert calls == []


def test_preparation_failure_retains_committed_admission_and_retry_resumes(tmp_path, monkeypatch):
    # Catches rolling admission back with preparation, response leaks, or reserving again on retry.
    database = tmp_path / "failure.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store, routing_budget=True)
        service = RecoveryContinuationService(case.scheduler, WorkerInputService(case.scheduler))
        arguments = live_arguments(case)
        append = store.append_checked
        stages = []

        def fail_preparation(stream_type, stream_id, key, decide):
            if key == "recovery-preparation-snapshot":
                assert not store._connection.in_transaction
                with SQLiteEventStore(database) as reader:
                    accepted = [event for event in reader.read_stream("scheduler", "global")
                                if event.event_type == "RoutingDecisionAccepted"]
                    assert len(accepted) == 2
                    stages.append("admission-visible-on-other-connection")
            if key == "worker-input-recovery-preparation-snapshot":
                assert store._connection.in_transaction
                raise RuntimeError(TEXT)
            return append(stream_type, stream_id, key, decide)

        with monkeypatch.context() as patch:
            patch.setattr(store, "append_checked", fail_preparation)
            result = service.continue_recovery(**arguments)
        assert stages == ["admission-visible-on-other-connection"]
        assert (result.status, result.reason, result.task) == (
            "admitted", "task_preparation_failed", None,
        )
        route = result.accepted_route
        assert route is not None and TEXT not in repr(result)
        assert case.scheduler.agents.replay("run-1").total_created == 2
        assert case.scheduler.agents.replay("run-1").active_count == 1
        before = durable_state(store)
        assert len([e for e in store.read_stream("budget", "run-1")
                    if e.event_type == "BudgetReserved"]) == 2
        arguments["routing_inputs"] = None
        resumed = service.continue_recovery(**arguments)
        assert resumed.status == "prepared" and resumed.task.task_text == TEXT
        assert resumed.accepted_route == route and durable_state(store) == before


@pytest.mark.parametrize("fault", ["text-mismatch", "context-agent", "frame-size"])
def test_internal_recovery_preparation_reuses_input_context_and_size_validation(tmp_path, monkeypatch, fault):
    # Catches a separate recovery task builder omitting existing validation.
    from orchestrator.runtime import worker_input_service

    with SQLiteEventStore(tmp_path / "validation.db") as store:
        case = recovery_case(store, routing_budget=True)
        inputs = WorkerInputService(case.scheduler)
        service = RecoveryContinuationService(case.scheduler, inputs)
        admitted = service.admit(**live_arguments(case))
        route = admitted.accepted_route
        before = durable_state(store)
        text, expected = TEXT, "worker_input_preparation_failed"
        if fault == "text-mismatch":
            text, expected = TEXT + " ", "worker_input_mismatch"
        elif fault == "context-agent":
            resolve = case.scheduler.resolve_verification_binding

            def wrong_agent(**kwargs):
                value = resolve(**kwargs)
                return value.model_copy(update={"context": value.context.model_copy(
                    update={"agent_instance_id": "other-agent"})})

            monkeypatch.setattr(case.scheduler, "resolve_verification_binding", wrong_agent)
        else:
            monkeypatch.setattr(worker_input_service, "MAX_WORKER_INPUT_FRAME_BYTES", 10)
            expected = "worker_input_too_large"
        with pytest.raises(WorkerInputError) as caught:
            WorkerInputService._prepare_recovery_worker_task(
                inputs, run_id="run-1", node_id="node-1", attempt_id=route.attempt_id,
                fencing_generation=2, task_text=text,
            )
        assert caught.value.code == expected and durable_state(store) == before


def test_continuation_rejects_late_input_method_override_before_admission(tmp_path):
    # Catches accepting a concrete service which becomes unsafe after composition.
    with SQLiteEventStore(tmp_path / "override.db") as store:
        case = recovery_case(store, routing_budget=True)
        inputs = WorkerInputService(case.scheduler)
        service = RecoveryContinuationService(case.scheduler, inputs)
        inputs._prepare_recovery_worker_task = lambda **kwargs: pytest.fail("foreign method")
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError):
            service.continue_recovery(**live_arguments(case))
        assert durable_state(store) == before


def test_application_continuation_requires_ready_before_arguments(tmp_path, monkeypatch):
    # Catches admitting via the explicit continuation API before bootstrap commits or after close.
    bootstrap = ControlPlaneApplication._bootstrap
    arguments = dict(run_id=None, node_id=None, source_attempt_id=None,
                     source_fencing_generation=None, accepted_at=None, lease_expires_at=None)

    def inspect_bootstrap(app):
        with pytest.raises(ApplicationNotReady):
            app.continue_recovery(**arguments)
        return bootstrap(app)

    monkeypatch.setattr(ControlPlaneApplication, "_bootstrap", inspect_bootstrap)
    app = ControlPlaneApplication(tmp_path / "ready.db", limits=_application_limits())
    app.close()
    with pytest.raises(ApplicationNotReady):
        app.continue_recovery(**arguments)
