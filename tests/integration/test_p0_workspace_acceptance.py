"""Crash recovery and explicit trusted reconciliation of workspace writes."""

import multiprocessing

import pytest

from tests.support.isolation_runtime import private_isolation_runtime, use_private_isolation_runtime
from tests.unit.runtime.test_workspace_execution import execution_case


def test_explicit_reconciliation_finishes_committed_publication_without_reexecution(
    tmp_path, monkeypatch, use_private_isolation_runtime
):
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.isolation import launcher
    from tests.support.process_crash import block_at_crash_point, kill_at_crash_point
    from tests.unit.lifecycle.test_scheduler import _application_limits

    monkeypatch.setattr(
        launcher,
        "__file__",
        str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"),
    )
    case = execution_case(tmp_path, command=("/usr/bin/touch", "committed-once"))
    case.app.close()
    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe()

    def child():
        with ControlPlaneApplication(
            tmp_path / "events.db",
            limits=_application_limits(),
            workspace_execution_host=case.host,
        ) as app:
            def die_before_operation_terminal(*_args, **_kwargs):
                assert (case.workspace / "committed-once").is_file()
                block_at_crash_point(child_pipe, "gateway-receipt-before-service-terminal")

            app._workspace_execution._finish = die_before_operation_terminal
            app.execute_workspace_task(**case.identity, task_text=case.text)

    process = context.Process(target=child)
    process.start()
    child_pipe.close()
    try:
        kill_at_crash_point(
            process,
            parent_pipe,
            expected_point="gateway-receipt-before-service-terminal",
            timeout_seconds=30,
        )
    finally:
        parent_pipe.close()

    with ControlPlaneApplication(
        tmp_path / "events.db",
        limits=_application_limits(),
        workspace_execution_host=case.host,
    ) as app:
        operation_id, = app._event_store.stream_ids("workspace_worker_operation")
        operation_events = app._event_store.read_stream("workspace_worker_operation", operation_id)
        held_attempts = app.startup_report.held_attempts
        assert held_attempts == 1
        assert [event.event_type for event in operation_events] == [
            "WorkspaceWorkerOperationRequested",
            "WorkspaceWorkerStopped",
        ]
        assert (case.workspace / "committed-once").is_file()
        assert list(case.host.journal_root.glob("publish-*"))

        # Startup and the ordinary execute path are validation/replay only.
        # They may report uncertainty, but cannot dispatch a second Worker or
        # sweep the committed journal.
        from orchestrator.runtime.workspace_execution import IsolatedWorkspaceWorker
        from orchestrator.isolation import SystemdOverlayCandidateLauncher

        def forbidden_dispatch(*_args, **_kwargs):
            pytest.fail("startup or duplicate execution dispatched a process")

        monkeypatch.setattr(IsolatedWorkspaceWorker, "propose", forbidden_dispatch)
        monkeypatch.setattr(SystemdOverlayCandidateLauncher, "launch", forbidden_dispatch)
        duplicate = app.execute_workspace_task(**case.identity, task_text=case.text)
        assert duplicate.outcome == "outcome_unknown"
        assert duplicate.reason_code == "operation_in_progress"
        assert [event.event_type for event in app._event_store.read_stream(
            "workspace_worker_operation", operation_id
        )] == ["WorkspaceWorkerOperationRequested", "WorkspaceWorkerStopped"]
        assert list(case.host.journal_root.glob("publish-*"))

        result = app.reconcile_workspace_publication(run_id="run-1", operation_id=operation_id)

        assert result.outcome == "published"
        assert result.publication_receipt is not None
        assert (case.workspace / "committed-once").is_file()
        assert not list(case.host.journal_root.iterdir())
        assert [event.event_type for event in app._event_store.read_stream(
            "workspace_worker_operation", operation_id
        )] == [
            "WorkspaceWorkerOperationRequested",
            "WorkspaceWorkerStopped",
            "WorkspaceWorkerOperationFinished",
        ]
        assert app.startup_report.held_attempts == held_attempts


@pytest.mark.parametrize(
    "phase,expected_outcome",
    [
        ("creation", "unknown"),
        ("preparing", "not_started"),
        ("staging_rename", "not_started"),
        ("staging_fsync", "not_started"),
        ("backup", "rolled_back"),
        ("prepared", "rolled_back"),
        ("apply-entry", "rolled_back"),
        ("parent_fsync", "rolled_back"),
        ("committed", "committed"),
        ("run-receipt", "committed"),
        ("workspace-resolution", "committed"),
        ("acknowledgement", "committed"),
    ],
)
def test_explicit_reconciliation_recovers_each_durable_publication_boundary(
    tmp_path, monkeypatch, use_private_isolation_runtime, phase, expected_outcome
):
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.isolation import launcher
    from orchestrator.tools.workspace_write import WorkspaceWriteGateway
    from tests.support.process_crash import block_at_crash_point, kill_at_crash_point
    from tests.unit.lifecycle.test_scheduler import _application_limits

    monkeypatch.setattr(
        launcher,
        "__file__",
        str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"),
    )
    case = execution_case(
        tmp_path,
        command=(
            "/usr/bin/python3",
            "-c",
            "from pathlib import Path; Path('new').write_text('after'); "
            "Path('existing').write_text('after')",
        ),
    )
    (case.workspace / "existing").write_text("before")
    case.app.close()
    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe()

    def child():
        import orchestrator.isolation.workspace_publish as publication

        def crash(point):
            def stop_at_boundary(*_args, **_kwargs):
                block_at_crash_point(child_pipe, point)

            return stop_at_boundary

        with ControlPlaneApplication(
            tmp_path / "events.db",
            limits=_application_limits(),
            workspace_execution_host=case.host,
        ) as app:
            if phase in {
                "creation", "preparing", "staging_rename", "staging_fsync",
                "backup", "prepared", "parent_fsync", "committed",
            }:
                original = publication._publication_boundary

                def publication_boundary(stage, transaction_id):
                    expected = "backup" if phase == "backup" else phase
                    if stage == expected:
                        block_at_crash_point(child_pipe, phase)
                    original(stage, transaction_id)

                publication._publication_boundary = publication_boundary
            elif phase == "apply-entry":
                publication._after_publish_entry = crash(phase)
            elif phase == "run-receipt":
                def before_workspace_resolution(self, *args, **kwargs):
                    block_at_crash_point(child_pipe, phase)

                WorkspaceWriteGateway._resolve_publication_completed = before_workspace_resolution
            elif phase == "workspace-resolution":
                app._workspace_execution._finish = crash(phase)
            elif phase == "acknowledgement":
                import orchestrator.runtime.workspace_execution as service

                service.acknowledge_workspace_publication = crash(phase)
            else:
                raise AssertionError(phase)
            app.execute_workspace_task(**case.identity, task_text=case.text)

    process = context.Process(target=child)
    process.start()
    child_pipe.close()
    try:
        kill_at_crash_point(
            process,
            parent_pipe,
            expected_point=phase,
            timeout_seconds=30,
        )
    finally:
        parent_pipe.close()

    with ControlPlaneApplication(
        tmp_path / "events.db",
        limits=_application_limits(),
        workspace_execution_host=case.host,
    ) as app:
        operation_id, = app._event_store.stream_ids("workspace_worker_operation")
        before_files = {
            path.relative_to(case.workspace).as_posix(): path.read_bytes()
            for path in case.workspace.rglob("*")
            if path.is_file()
        }
        before_events = tuple(
            event.event_type
            for event in app._event_store.read_stream("workspace_worker_operation", operation_id)
        )
        held_attempts = app.startup_report.held_attempts
        assert held_attempts == 1
        assert before_events in (
            ("WorkspaceWorkerOperationRequested", "WorkspaceWorkerStopped"),
            ("WorkspaceWorkerOperationRequested", "WorkspaceWorkerStopped", "WorkspaceWorkerOperationFinished"),
        )
        assert list(case.host.journal_root.iterdir())
        # Construction did not roll back, finish, or acknowledge anything.
        assert {
            path.relative_to(case.workspace).as_posix(): path.read_bytes()
            for path in case.workspace.rglob("*")
            if path.is_file()
        } == before_files
        recovered = app.reconcile_workspace_publication(run_id="run-1", operation_id=operation_id)
        if expected_outcome == "unknown":
            assert recovered.outcome == "outcome_unknown"
            assert recovered.reason_code == "execution_unknown"
            assert {
                path.relative_to(case.workspace).as_posix(): path.read_bytes()
                for path in case.workspace.rglob("*")
                if path.is_file()
            } == before_files
            assert list(case.host.journal_root.iterdir())
        elif expected_outcome in {"not_started", "rolled_back"}:
            assert recovered.outcome == "failed"
            assert recovered.reason_code == "execution_failed"
            assert (case.workspace / "existing").read_text() == "before"
            assert not (case.workspace / "new").exists()
            assert recovered.publication_receipt is None
        else:
            assert recovered.outcome == "published"
            assert (case.workspace / "existing").read_text() == "after"
            assert (case.workspace / "new").read_text() == "after"
            assert recovered.publication_receipt is not None
        if expected_outcome != "unknown":
            assert not list(case.host.journal_root.iterdir())
        assert app.startup_report.held_attempts == held_attempts


@pytest.mark.parametrize("phase", ["worker-proposal-return", "worker-stop-recorded"])
def test_worker_crash_windows_remain_unknown_until_stop_and_tool_evidence_are_durable(
    tmp_path, monkeypatch, use_private_isolation_runtime, phase
):
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.isolation import SystemdReadOnlyLauncher, launcher
    from tests.support.process_crash import block_at_crash_point, kill_at_crash_point
    from tests.unit.lifecycle.test_scheduler import _application_limits

    monkeypatch.setattr(
        launcher,
        "__file__",
        str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"),
    )
    case = execution_case(tmp_path, command=("/usr/bin/true",))
    case.app.close()
    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe()

    def child():
        with ControlPlaneApplication(
            tmp_path / "events.db",
            limits=_application_limits(),
            workspace_execution_host=case.host,
        ) as app:
            if phase == "worker-proposal-return":
                original = SystemdReadOnlyLauncher.launch

                class WaitBeforeReturning:
                    def __init__(self, session):
                        self._session = session

                    def wait(self):
                        self._session.wait()
                        block_at_crash_point(child_pipe, phase)

                    def cancel(self):
                        return self._session.cancel()

                    def close(self):
                        self._session.close()

                def launch(self, *args, **kwargs):
                    return WaitBeforeReturning(original(self, *args, **kwargs))

                SystemdReadOnlyLauncher.launch = launch
            else:
                original = app._workspace_execution._record_worker_stop

                def recorded_then_die(*args, **kwargs):
                    original(*args, **kwargs)
                    block_at_crash_point(child_pipe, phase)

                app._workspace_execution._record_worker_stop = recorded_then_die
            app.execute_workspace_task(**case.identity, task_text=case.text)

    process = context.Process(target=child)
    process.start()
    child_pipe.close()
    try:
        kill_at_crash_point(process, parent_pipe, expected_point=phase, timeout_seconds=30)
    finally:
        parent_pipe.close()

    with ControlPlaneApplication(
        tmp_path / "events.db",
        limits=_application_limits(),
        workspace_execution_host=case.host,
    ) as app:
        operation_id, = app._event_store.stream_ids("workspace_worker_operation")
        events = app._event_store.read_stream("workspace_worker_operation", operation_id)
        held_attempts = app.startup_report.held_attempts
        assert held_attempts == 1
        if phase == "worker-proposal-return":
            assert [event.event_type for event in events] == [
                "WorkspaceWorkerOperationRequested",
            ]
        else:
            assert [event.event_type for event in events] == [
                "WorkspaceWorkerOperationRequested",
                "WorkspaceWorkerStopped",
            ]
        before = tuple(event.event_type for event in events)
        result = app.reconcile_workspace_publication(run_id="run-1", operation_id=operation_id)
        assert result.outcome == "outcome_unknown"
        assert result.reason_code == "execution_unknown"
        assert not list(case.workspace.iterdir())
        assert not list(case.host.journal_root.iterdir())
        assert tuple(event.event_type for event in app._event_store.read_stream(
            "workspace_worker_operation", operation_id
        )) == before
        assert app.startup_report.held_attempts == held_attempts


def test_unknown_tool_stop_is_not_reconciled_or_cleaned(tmp_path, monkeypatch, use_private_isolation_runtime):
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.isolation import launcher, SystemdOverlayCandidateLauncher
    from tests.support.process_crash import block_at_crash_point, kill_at_crash_point
    from tests.unit.lifecycle.test_scheduler import _application_limits

    monkeypatch.setattr(
        launcher,
        "__file__",
        str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"),
    )
    case = execution_case(tmp_path, command=("/usr/bin/touch", "candidate-only"))
    case.app.close()
    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe()

    def child():
        with ControlPlaneApplication(
            tmp_path / "events.db",
            limits=_application_limits(),
            workspace_execution_host=case.host,
        ) as app:
            original = SystemdOverlayCandidateLauncher.launch

            class WaitWithoutReportingStop:
                def __init__(self, session):
                    self._session = session

                def wait(self):
                    self._session.wait()
                    block_at_crash_point(child_pipe, "tool-stopped-before-durable-proof")

                def close(self):
                    self._session.close()

            def launch(self, *args, **kwargs):
                return WaitWithoutReportingStop(original(self, *args, **kwargs))

            SystemdOverlayCandidateLauncher.launch = launch
            app.execute_workspace_task(**case.identity, task_text=case.text)

    process = context.Process(target=child)
    process.start()
    child_pipe.close()
    try:
        kill_at_crash_point(
            process,
            parent_pipe,
            expected_point="tool-stopped-before-durable-proof",
            timeout_seconds=30,
        )
    finally:
        parent_pipe.close()

    with ControlPlaneApplication(
        tmp_path / "events.db",
        limits=_application_limits(),
        workspace_execution_host=case.host,
    ) as app:
        operation_id, = app._event_store.stream_ids("workspace_worker_operation")
        held_attempts = app.startup_report.held_attempts
        assert held_attempts == 1
        result = app.reconcile_workspace_publication(run_id="run-1", operation_id=operation_id)
        assert result.outcome == "outcome_unknown"
        assert result.reason_code == "execution_unknown"
        assert not (case.workspace / "candidate-only").exists()
        assert not list(case.host.journal_root.iterdir())
        assert [event.event_type for event in app._event_store.read_stream(
            "workspace_worker_operation", operation_id
        )] == ["WorkspaceWorkerOperationRequested", "WorkspaceWorkerStopped"]
        assert app.startup_report.held_attempts == held_attempts


def test_real_approval_requires_fresh_attempt_and_reconciles_committed_write(
    tmp_path, monkeypatch, use_private_isolation_runtime
):
    from datetime import timedelta

    import pytest

    from orchestrator.approvals import (
        ApprovalError,
        ApprovalPolicyState,
        ApprovalPrincipal,
        ApprovalService,
        ExecutionAttempt,
    )
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.budget import BudgetLedger
    from orchestrator.isolation import launcher
    from tests.support.workspace_execution import (
        DurableAcceptedAttemptAuthority,
        accept_fresh_generation,
        finish_failed_attempt_without_effect,
    )
    from tests.support.process_crash import block_at_crash_point, kill_at_crash_point
    from tests.unit.lifecycle.test_scheduler import NOW, _application_limits

    monkeypatch.setattr(
        launcher,
        "__file__",
        str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"),
    )

    def approval_factory(store):
        def authenticate(credential):
            if credential == "trusted-human":
                return ApprovalPrincipal(
                    principal_id="human-1",
                    principal_type="human",
                    permissions=frozenset({"approval:approve"}),
                )
            return ApprovalPrincipal(
                principal_id="untrusted-service",
                principal_type="service",
                permissions=frozenset({"tool:execute"}),
            )

        return ApprovalService(
            event_store=store,
            budget_ledger=BudgetLedger(store),
            authenticator=authenticate,
            attempt_authority=DurableAcceptedAttemptAuthority(tmp_path / "events.db"),
            policy_state=lambda _request: ApprovalPolicyState(0, 0),
            now=lambda: NOW + timedelta(seconds=2),
        )

    case = execution_case(
        tmp_path,
        approval=True,
        command=("/usr/bin/touch", "approved-once"),
        host_changes={
            "approval_factory": approval_factory,
            "approval_requester": lambda _request: "trusted-local-session",
        },
    )
    try:
        waiting = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert waiting.outcome == "awaiting_approval"
        assert waiting.approval_request_id
        assert not list(case.workspace.iterdir())
        workspace_id = "workspace:" + case.app._scheduler.lifecycle.replay(
            "run-1"
        ).workspace_identity_hash[7:]
        assert not case.app._event_store.read_stream("workspace_executions", workspace_id)

        approvals = case.app._workspace_execution._approvals
        with pytest.raises(ApprovalError):
            approvals.approve(waiting.approval_request_id, "untrusted", reason="not authorized")
        issued = approvals.approve(
            waiting.approval_request_id,
            "trusted-human",
            reason="approve the exact bound workspace effect",
        )
        with pytest.raises(ApprovalError):
            approvals.bind_to_attempt(
                issued.approval_grant_id,
                ExecutionAttempt(
                    run_id=case.identity["run_id"],
                    node_id=case.identity["node_id"],
                    attempt_id=case.identity["attempt_id"],
                    fencing_generation=case.identity["fencing_generation"],
                    isolation_profile_hash="sha256:" + "a" * 64,
                ),
            )

        finish_failed_attempt_without_effect(
            case,
            case.identity,
            completed_at=case.clock.now + timedelta(seconds=1),
        )
        fresh_identity = accept_fresh_generation(case, 2)
        case.identity = fresh_identity
        case.app.close()

        context = multiprocessing.get_context("fork")
        parent_pipe, child_pipe = context.Pipe()

        def child():
            with ControlPlaneApplication(
                tmp_path / "events.db",
                limits=_application_limits(),
                workspace_execution_host=case.host,
            ) as app:
                def after_gateway(*_args, **_kwargs):
                    assert (case.workspace / "approved-once").is_file()
                    block_at_crash_point(child_pipe, "approved-commit-before-operation-terminal")

                app._workspace_execution._finish = after_gateway
                app.execute_workspace_task(
                    **case.identity,
                    task_text=case.text,
                    approval_grant_id=issued.approval_grant_id,
                )

        process = context.Process(target=child)
        process.start()
        child_pipe.close()
        try:
            kill_at_crash_point(
                process,
                parent_pipe,
                expected_point="approved-commit-before-operation-terminal",
                timeout_seconds=30,
            )
        finally:
            parent_pipe.close()

        with ControlPlaneApplication(
            tmp_path / "events.db",
            limits=_application_limits(),
            workspace_execution_host=case.host,
        ) as app:
            operation_id = next(
                operation
                for operation in app._event_store.stream_ids("workspace_worker_operation")
                if any(
                    event.attempt_id == case.identity["attempt_id"]
                    for event in app._event_store.read_stream("workspace_worker_operation", operation)
                )
            )
            before = app._event_store.read_stream("workspace_worker_operation", operation_id)
            assert [event.event_type for event in before] == [
                "WorkspaceWorkerOperationRequested",
                "WorkspaceWorkerStopped",
            ]
            result = app.reconcile_workspace_publication(run_id="run-1", operation_id=operation_id)
            assert result.outcome == "published"
            assert (case.workspace / "approved-once").is_file()
            assert not list(case.host.journal_root.iterdir())

            security = app._event_store.read_stream("security", "run-1")
            bound, = [event for event in security if event.event_type == "ApprovalGrantBound"]
            assert bound.attempt_id == case.identity["attempt_id"]
            assert bound.fencing_generation == case.identity["fencing_generation"]
            assert bound.payload["attempt_id"] == case.identity["attempt_id"]
            completed = [event for event in security if event.event_type == "WorkspacePublicationCompleted"]
            assert len(completed) == 1
            budget = app._event_store.read_stream("budget", "run-1")
            applied, = [event for event in budget if event.event_type == "EffectReceiptRecorded"]
            assert applied.payload["outcome"] == "applied"
            workspace_id = "workspace:" + app._scheduler.lifecycle.replay(
                "run-1"
            ).workspace_identity_hash[7:]
            tool_stops = [
                event for event in app._event_store.read_stream("workspace_executions", workspace_id)
                if event.event_type == "WorkspaceExecutionStopped"
            ]
            assert len(tool_stops) == 1
            assert tool_stops[0].payload["proof"] == "terminated"
            assert before[1].payload["termination_receipt"]["unit_name"] != (
                tool_stops[0].payload["termination_receipt"]["unit_name"]
            )
            assert app.execute_workspace_task(
                **case.identity,
                task_text=case.text,
                approval_grant_id=issued.approval_grant_id,
            ) == result
            assert not list(case.host.journal_root.iterdir())
    finally:
        case.app.close()


def test_revoke_during_real_candidate_closes_authority_then_stops_process(
    tmp_path, monkeypatch, use_private_isolation_runtime
):
    context = multiprocessing.get_context("fork")
    revoked = context.Event()
    candidate_started = context.Event()
    candidate_cancelled = context.Event()

    from orchestrator.application import ControlPlaneApplication
    from orchestrator.isolation import SystemdOverlayCandidateLauncher, launcher
    from orchestrator.tools.gateway import PolicyState
    from tests.unit.lifecycle.test_scheduler import _application_limits

    monkeypatch.setattr(
        launcher,
        "__file__",
        str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"),
    )
    original_launch = SystemdOverlayCandidateLauncher.launch

    class ObservedSession:
        def __init__(self, session):
            self._session = session

        def wait(self):
            return self._session.wait()

        def cancel(self):
            assert revoked.is_set(), "the authority must be closed before process stop"
            candidate_cancelled.set()
            return self._session.cancel()

        def close(self):
            self._session.close()

    def launch(self, *args, **kwargs):
        session = original_launch(self, *args, **kwargs)
        candidate_started.set()
        return ObservedSession(session)

    monkeypatch.setattr(SystemdOverlayCandidateLauncher, "launch", launch)
    case = execution_case(
        tmp_path,
        command=(
            "/usr/bin/python3",
            "-c",
            "from pathlib import Path; import time; time.sleep(0.4); "
            "Path('candidate-side-effect').write_text('must-not-publish'); time.sleep(20)",
        ),
        host_changes={
            "policy_state": lambda _request: PolicyState(0, 0, revoked=revoked.is_set()),
        },
    )
    case.app.close()

    def child():
        with ControlPlaneApplication(
            tmp_path / "events.db",
            limits=_application_limits(),
            workspace_execution_host=case.host,
        ) as app:
            app.execute_workspace_task(**case.identity, task_text=case.text)

    process = context.Process(target=child)
    process.start()
    try:
        assert candidate_started.wait(30), "real overlay candidate did not start"
        revoked.set()
        assert candidate_cancelled.wait(30), "Gateway did not stop the supervised process"
        process.join(30)
        assert not process.is_alive()
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.kill()
            process.join(5)

    with ControlPlaneApplication(
        tmp_path / "events.db",
        limits=_application_limits(),
        workspace_execution_host=case.host,
    ) as app:
        operation_id, = app._event_store.stream_ids("workspace_worker_operation")
        operation_events = app._event_store.read_stream("workspace_worker_operation", operation_id)
        assert operation_events[-1].event_type == "WorkspaceWorkerOperationFinished"
        assert operation_events[-1].payload["result"]["outcome"] == "failed"
        assert not (case.workspace / "candidate-side-effect").exists()
        assert not list(case.host.journal_root.iterdir())
        workspace_id = "workspace:" + app._scheduler.lifecycle.replay(
            "run-1"
        ).workspace_identity_hash[7:]
        execution_events = app._event_store.read_stream("workspace_executions", workspace_id)
        stops = [event for event in execution_events if event.event_type == "WorkspaceExecutionStopped"]
        assert len(stops) == 1
        assert stops[0].payload["proof"] == "terminated"
        assert stops[0].payload["termination_receipt"]["cgroup_empty"] is True
        assert not any(event.event_type == "WorkspacePublicationIntent" for event in
                       app._event_store.read_stream("security", "run-1"))
        assert not any(event.event_type == "AttemptSlotReleased" for event in
                       app._event_store.read_stream("scheduler", "global"))
