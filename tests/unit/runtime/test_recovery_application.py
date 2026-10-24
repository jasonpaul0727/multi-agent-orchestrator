"""Host recovery inventory requires committed readiness and owned composition."""

import subprocess

import pytest

from orchestrator.application import (
    ApplicationNotReady, ControlPlaneApplication, StartupRecoveryFailed,
)
from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.recovery_continuation import RecoveryContinuationService
from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError
from orchestrator.runtime.worker_input_service import WorkerInputService
from tests.support.recovery_continuation import durable_state, recovery_case
from tests.unit.lifecycle.test_scheduler import _application_limits, scheduler


def test_ready_application_lists_reopened_recovery_without_dispatch_or_writes(tmp_path, monkeypatch):
    # Catches missing inventory, detached composition, query writes or automatic execution.
    from orchestrator.models.gateway import ModelGateway
    from orchestrator.models.transport import UrllibHTTPSTransport
    from orchestrator.runtime.worker_process import IsolatedWorkerProcess
    from orchestrator.secrets.broker import AuditedSecretBroker

    database = tmp_path / "recovery.db"
    with SQLiteEventStore(database) as store:
        recovery_case(store)
        before = durable_state(store)

    def forbidden(*args, **kwargs):
        pytest.fail("recovery inventory crossed an execution boundary")

    for target, method in (
        (IsolatedWorkerProcess, "__init__"), (ModelGateway, "__init__"),
        (UrllibHTTPSTransport, "__init__"), (subprocess, "Popen"),
        (AuditedSecretBroker, "acquire_provider_credential_decision"),
    ):
        monkeypatch.setattr(target, method, forbidden)
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        item, = app.list_recovery_opportunities(run_id="run-1")
        assert (item.source_attempt_id, item.source_fencing_generation,
                item.status, item.reason) == (
            "attempt-node-1-1", 1, "ready", "bounded_same_model_retry",
        )
        assert item.successor is None
        assert durable_state(app._event_store) == before
        assert app._recovery_opportunities._scheduler is app._scheduler
        assert app._recovery_continuation._scheduler is app._scheduler
        assert app._recovery_continuation._worker_inputs is app._worker_inputs
        assert type(app._worker_inputs) is WorkerInputService
        assert app._worker_inputs._scheduler is app._scheduler


def test_recovery_query_requires_readiness_during_bootstrap_and_after_close(tmp_path, monkeypatch):
    # Catches readiness assigned before bootstrap commits, or argument checks before the gate.
    bootstrap = ControlPlaneApplication._bootstrap

    def inspect_bootstrap(app):
        with pytest.raises(ApplicationNotReady):
            app.list_recovery_opportunities(run_id=None)
        return bootstrap(app)

    monkeypatch.setattr(ControlPlaneApplication, "_bootstrap", inspect_bootstrap)
    app = ControlPlaneApplication(tmp_path / "ready.db", limits=_application_limits())
    app.close()
    with pytest.raises(ApplicationNotReady):
        app.list_recovery_opportunities(run_id=None)


def test_failed_startup_never_exposes_recovery_query(tmp_path, monkeypatch):
    # Catches a leaked failed-startup instance retaining query authority.
    leaked = []

    def fail_bootstrap(app):
        leaked.append(app)
        raise RuntimeError("startup failed")

    monkeypatch.setattr(ControlPlaneApplication, "_bootstrap", fail_bootstrap)
    with pytest.raises(StartupRecoveryFailed):
        ControlPlaneApplication(tmp_path / "failed.db", limits=_application_limits())
    with pytest.raises(ApplicationNotReady):
        leaked[0].list_recovery_opportunities(run_id="run-1")


@pytest.mark.parametrize("composition", ["other-database", "same-file-connection", "same-store-scheduler"])
def test_recovery_rejects_input_service_with_different_scheduler_without_writes(tmp_path, composition):
    # Catches equating a file/store with the exact transaction and authority owner.
    database = tmp_path / "source.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store)
        before = durable_state(store)
        other = None
        try:
            if composition == "same-store-scheduler":
                other_scheduler = scheduler(store)
            else:
                other = SQLiteEventStore(database if composition == "same-file-connection"
                                         else tmp_path / "other.db")
                other_scheduler = scheduler(other)
            other_before = tuple(other._connection.iterdump()) if other is not None else None
            with pytest.raises(RecoveryContinuationError) as caught:
                RecoveryContinuationService(case.scheduler, WorkerInputService(other_scheduler))
            assert caught.value.code == "recovery_continuation_invalid"
            assert durable_state(store) == before
            if other is not None:
                assert tuple(other._connection.iterdump()) == other_before
        finally:
            if other is not None:
                other.close()


@pytest.mark.parametrize("substitute", ["subclass", "duck", "public-override", "clock-method-override"])
def test_recovery_rejects_overridable_input_substitutes_without_writes(tmp_path, substitute):
    # Catches caller-defined methods entering the future preparation transaction.
    with SQLiteEventStore(tmp_path / "owned.db") as store:
        case = recovery_case(store)
        before = durable_state(store)

        def forbidden(*args, **kwargs):
            pytest.fail("unowned input-service code executed")

        class OverridingInputs(WorkerInputService):
            prepare_worker_task = forbidden

        class DuckInputs:
            _scheduler = case.scheduler
            prepare_worker_task = forbidden

        if substitute == "subclass":
            inputs = OverridingInputs(case.scheduler)
        elif substitute == "duck":
            inputs = DuckInputs()
        else:
            inputs = WorkerInputService(case.scheduler)
            setattr(inputs, "prepare_worker_task" if substitute == "public-override" else "_now", forbidden)
        with pytest.raises(RecoveryContinuationError) as caught:
            RecoveryContinuationService(case.scheduler, inputs)
        assert caught.value.code == "recovery_continuation_invalid"
        assert durable_state(store) == before


def test_concrete_shared_scheduler_composition_preserves_legacy_clock(tmp_path):
    # Catches rejecting the supported injected clock or calling it during composition.
    with SQLiteEventStore(tmp_path / "shared.db") as store:
        control = scheduler(store)

        def forbidden_clock():
            pytest.fail("composition sampled a host clock")

        inputs = WorkerInputService(control, clock=forbidden_clock)
        service = RecoveryContinuationService(control, inputs)
        assert service._scheduler is control and service._worker_inputs is inputs
