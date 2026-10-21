"""Required real accepted Attempt -> stopped Worker -> stopped Overlay -> receipt."""

import os

import pytest

from tests.support.isolation_runtime import private_isolation_runtime, use_private_isolation_runtime
from tests.unit.runtime.test_workspace_execution import api, execution_case


@pytest.mark.parametrize("concurrent", [False, True])
def test_real_worker_gateway_publication_and_replay(tmp_path, monkeypatch, use_private_isolation_runtime, concurrent):
    m = api()
    from orchestrator.isolation import launcher, SystemdReadOnlyLauncher, SystemdOverlayCandidateLauncher
    from orchestrator.models.gateway import ModelGateway
    from orchestrator.tools.workspace_write import WorkspaceWriteGateway
    monkeypatch.setattr(launcher, "__file__", str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"))
    command = ("/usr/bin/python3", "-c", "from pathlib import Path; import os; "
        "Path('created').write_bytes(b'new'); Path('updated').write_bytes(b'after'); "
        "Path('updated').chmod(0o640); Path('deleted').unlink(); "
        "Path('source').rename('target'); os.symlink('created','link')")
    case = execution_case(tmp_path, command=command)
    for name, content in (("updated", b"before"), ("deleted", b"remove"), ("source", b"move")):
        (case.workspace / name).write_bytes(content)
    workers, candidates, gateways = [], [], []
    real_worker = SystemdReadOnlyLauncher.launch
    real_candidate = SystemdOverlayCandidateLauncher.launch
    real_gateway = WorkspaceWriteGateway.execute
    def worker(self, workspace, command, **kwargs):
        assert not case.app._event_store._connection.in_transaction
        assert str(case.workspace).encode() not in kwargs["input_bytes"]
        assert b"approval_grant" not in kwargs["input_bytes"]
        session = real_worker(self, workspace, command, **kwargs)
        workers.append(session.wait())
        return session
    def candidate(self, *args, **kwargs):
        assert not case.app._event_store._connection.in_transaction
        assert len(workers) == 1 and workers[0].termination_confirmed
        assert (case.workspace / "updated").read_bytes() == b"before"
        session = real_candidate(self, *args, **kwargs)
        result = session.wait()
        candidates.append(result.execution)
        assert (case.workspace / "updated").read_bytes() == b"before"
        return session
    def gateway(self, *args, **kwargs):
        request = args[0]
        events = self._events.read_stream("workspace_worker_operation", request.request_id)
        assert [event.event_type for event in events] == [
            "WorkspaceWorkerOperationRequested", "WorkspaceWorkerStopped"]
        assert events[-1].payload["termination_receipt"]["unit_name"] == workers[0].unit_name
        gateways.append(1)
        return real_gateway(self, *args, **kwargs)
    def provider(*args, **kwargs):
        pytest.fail("workspace tool path invoked a Provider")
    monkeypatch.setattr(SystemdReadOnlyLauncher, "launch", worker)
    monkeypatch.setattr(SystemdOverlayCandidateLauncher, "launch", candidate)
    monkeypatch.setattr(WorkspaceWriteGateway, "execute", gateway)
    monkeypatch.setattr(ModelGateway, "invoke", provider)
    try:
        if concurrent:
            from concurrent.futures import ThreadPoolExecutor
            from threading import Barrier
            from orchestrator.application import ControlPlaneApplication
            from tests.unit.lifecycle.test_scheduler import _application_limits
            barrier = Barrier(2)
            def execute():
                with ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits(),
                                              workspace_execution_host=case.host) as app:
                    barrier.wait(timeout=15)
                    return app.execute_workspace_task(**case.identity, task_text=case.text)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: execute(), range(2)))
            assert all(item.outcome in {"published", "outcome_unknown"} for item in results)
            result = next(item for item in results if item.outcome == "published")
        else:
            result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome == "published", result
        assert result.publication_receipt.entries_published >= 6
        assert (case.workspace / "created").read_bytes() == b"new"
        assert (case.workspace / "updated").read_bytes() == b"after"
        assert (case.workspace / "updated").stat().st_mode & 0o777 == 0o640
        assert not (case.workspace / "deleted").exists()
        assert not (case.workspace / "source").exists()
        assert (case.workspace / "target").read_bytes() == b"move"
        assert os.readlink(case.workspace / "link") == "created"
        assert len(workers) == len(candidates) == len(gateways) == 1
        assert candidates[0].termination_confirmed
        assert workers[0].unit_name != candidates[0].unit_name
        assert case.app.execute_workspace_task(**case.identity, task_text=case.text) == result
        assert len(workers) == len(candidates) == len(gateways) == 1
        assert case.app.recover_run("run-1").lifecycle.node("node-1").status == "running"
        assert not case.app._event_store.stream_ids("provider_call")
        assert not list(case.host.journal_root.iterdir())
    finally:
        case.app.close()


def test_host_death_after_gateway_retains_independent_worker_stop(tmp_path, monkeypatch, use_private_isolation_runtime):
    m = api()
    import multiprocessing
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.isolation import launcher
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.support.process_crash import block_at_crash_point, kill_at_crash_point
    monkeypatch.setattr(launcher, "__file__", str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"))
    case = execution_case(tmp_path, command=("/usr/bin/touch", "published"))
    case.app.close()
    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe()
    def child():
        with ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits(),
                                      workspace_execution_host=case.host) as app:
            def crash(*args, **kwargs):
                assert (case.workspace / "published").exists()
                block_at_crash_point(child_pipe, "gateway-receipt-before-service-terminal")
            app._workspace_execution._finish = crash
            app.execute_workspace_task(**case.identity, task_text=case.text)
    process = context.Process(target=child)
    process.start()
    child_pipe.close()
    try:
        kill_at_crash_point(process, parent_pipe,
            expected_point="gateway-receipt-before-service-terminal", timeout_seconds=30)
    finally:
        parent_pipe.close()
    with ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits(),
                                  workspace_execution_host=case.host) as app:
        result = app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome == "outcome_unknown" and result.reason_code == "operation_in_progress"
        events = app._event_store.read_stream("workspace_worker_operation", result.operation_id)
        assert [event.event_type for event in events] == [
            "WorkspaceWorkerOperationRequested", "WorkspaceWorkerStopped"]
        assert events[-1].payload["termination_receipt"]["cgroup_empty"] is True
        assert (case.workspace / "published").exists()
        assert list(case.host.journal_root.glob("publish-*"))
        assert sum(event.event_type == "WorkspacePublicationCompleted" for event in
                   app._event_store.read_stream("security", "run-1")) == 1


def test_real_worker_creates_bound_approval_request_without_tool_launch(tmp_path, monkeypatch, use_private_isolation_runtime):
    api()
    from orchestrator.isolation import launcher, SystemdOverlayCandidateLauncher
    from orchestrator.approvals import ApprovalService, ApprovalPolicyState
    from orchestrator.budget import BudgetLedger
    from tests.unit.lifecycle.test_scheduler import NOW
    from datetime import timedelta
    monkeypatch.setattr(launcher, "__file__", str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"))
    def factory(store):
        assert not store._connection.in_transaction
        return ApprovalService(event_store=store, budget_ledger=BudgetLedger(store),
            authenticator=lambda _: pytest.fail("request creation must not authenticate a grant"),
            attempt_authority=None, policy_state=lambda _: ApprovalPolicyState(0, 0),
            now=lambda: NOW + timedelta(seconds=2))
    def requester(request):
        assert not case.app._event_store._connection.in_transaction
        return "trusted-local-session"
    case = execution_case(tmp_path, approval=True, host_changes={
        "approval_factory": factory, "approval_requester": requester})
    def forbidden(*args, **kwargs):
        pytest.fail("awaiting approval launched candidate tool")
    monkeypatch.setattr(SystemdOverlayCandidateLauncher, "launch", forbidden)
    try:
        result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome == "awaiting_approval" and result.approval_request_id
        events = case.app._event_store.read_stream("security", "run-1")
        approved, = [event for event in events if event.event_type == "ApprovalRequested"]
        assert approved.payload["approval_request"]["requester_id"] == "trusted-local-session"
        assert approved.payload["approval_request"]["origin_attempt_id"] == case.identity["attempt_id"]
        assert case.app.execute_workspace_task(**case.identity, task_text=case.text) == result
        assert not list(case.workspace.iterdir())
    finally:
        case.app.close()


def test_real_worker_tool_cannot_read_control_files_or_open_sockets(tmp_path, monkeypatch, use_private_isolation_runtime):
    api()
    from orchestrator.isolation import launcher, SystemdOverlayCandidateLauncher
    monkeypatch.setattr(launcher, "__file__", str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"))
    secret = tmp_path / "private-control"
    secret.write_text("sentinel-control-c7db")
    code = """from pathlib import Path
import socket
for family in (socket.AF_INET, socket.AF_UNIX):
    try:
        socket.socket(family, socket.SOCK_STREAM)
    except OSError:
        pass
    else:
        raise AssertionError('raw socket exposed')
for target in TARGETS:
    try:
        Path(target).read_bytes()
    except OSError:
        pass
    else:
        raise AssertionError('control file exposed: ' + target)
Path('denials-confirmed').write_text('ok')
""".replace("TARGETS", repr((str(secret), str(tmp_path / "events.db"), ".git/config", ".maestro/secrets", ".env")))
    case = execution_case(tmp_path, command=("/usr/bin/python3", "-c", code))
    (case.workspace / ".git").mkdir()
    (case.workspace / ".git/config").write_text("sentinel-control-c7db")
    (case.workspace / ".maestro").mkdir()
    (case.workspace / ".maestro/secrets").write_text("sentinel-control-c7db")
    (case.workspace / ".env").write_text("sentinel-control-c7db")
    executions = []
    launch = SystemdOverlayCandidateLauncher.launch
    def observed(self, *args, **kwargs):
        session = launch(self, *args, **kwargs)
        executions.append(session.wait().execution)
        return session
    monkeypatch.setattr(SystemdOverlayCandidateLauncher, "launch", observed)
    try:
        result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome == "published", (result, [item.stderr for item in executions])
        assert (case.workspace / "denials-confirmed").read_text() == "ok"
        assert secret.read_text() == "sentinel-control-c7db"
        dump = repr(tuple(case.app._event_store._connection.iterdump()))
        assert "sentinel-control-c7db" not in dump + repr(result)
        assert not case.app._event_store.stream_ids("provider_call")
    finally:
        case.app.close()


def test_real_worker_tool_cannot_publish_secret_configuration(tmp_path, monkeypatch, use_private_isolation_runtime):
    api()
    from orchestrator.isolation import launcher
    monkeypatch.setattr(launcher, "__file__", str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"))
    case = execution_case(tmp_path, command=("/usr/bin/python3", "-c",
        "from pathlib import Path; Path('.env').write_text('replace'); Path('side-effect').write_text('no')"))
    (case.workspace / ".env").write_text("secret-original")
    try:
        result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome != "published" and result.publication_receipt is None
        assert (case.workspace / ".env").read_text() == "secret-original"
        assert not (case.workspace / "side-effect").exists()
        assert not any(event.event_type == "WorkspacePublicationCompleted" for event in
                       case.app._event_store.read_stream("security", "run-1"))
    finally:
        case.app.close()


@pytest.mark.parametrize("stream", ["security", "workspace_publications", "workspace_executions"])
def test_published_replay_rejects_gateway_proof_from_another_attempt(tmp_path, monkeypatch, use_private_isolation_runtime, stream):
    api()
    from orchestrator.isolation import launcher
    from orchestrator.tools.gateway import ToolAuditUnavailable
    from orchestrator.application import ControlPlaneApplication, StartupRecoveryFailed
    from tests.unit.lifecycle.test_scheduler import _application_limits
    monkeypatch.setattr(launcher, "__file__", str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"))
    case = execution_case(tmp_path, command=("/usr/bin/touch", "published"))
    result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
    assert result.outcome == "published"
    store = case.app._event_store
    connection = store._connection
    kind = "WorkspaceExecutionStopped" if stream == "workspace_executions" else "WorkspacePublicationCompleted"
    trigger = connection.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='events_immutable_update'").fetchone()[0]
    connection.execute("DROP TRIGGER events_immutable_update")
    try:
        connection.execute("UPDATE events SET attempt_id = 'another-attempt' WHERE stream_type = ? AND event_type = ?", (stream, kind))
    finally:
        connection.execute(trigger)
    with pytest.raises(ToolAuditUnavailable):
        case.app.execute_workspace_task(**case.identity, task_text=case.text)
    case.app.close()
    with pytest.raises(StartupRecoveryFailed):
        ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits())


def test_real_readonly_runtime_rejects_hardlinked_secret_input(tmp_path, monkeypatch, use_private_isolation_runtime):
    from orchestrator.isolation import launcher, InvalidSandboxRequest, SystemdReadOnlyLauncher
    monkeypatch.setattr(launcher, "__file__", str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / ".env").write_bytes(b"hardlink-secret-sentinel")
    os.link(workspace / ".env", workspace / "config-copy")
    with pytest.raises(InvalidSandboxRequest, match="snapshot failed closed"):
        session = SystemdReadOnlyLauncher().launch(workspace, ["/usr/bin/cat", "config-copy"])
        session.wait()
    assert (workspace / ".env").read_bytes() == b"hardlink-secret-sentinel"


def test_real_readonly_runtime_rejects_secret_unlink_race(tmp_path, monkeypatch, use_private_isolation_runtime):
    from orchestrator.isolation import launcher, InvalidSandboxRequest, SystemdReadOnlyLauncher
    monkeypatch.setattr(launcher, "__file__", str(use_private_isolation_runtime / "orchestrator/isolation/launcher.py"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    secret = workspace / ".env"
    secret.write_bytes(b"unlink-secret-sentinel")
    os.link(secret, workspace / "config-copy")
    real_stat = os.stat
    unlinked = False
    def racing_stat(path, *args, **kwargs):
        nonlocal unlinked
        if not unlinked and path == "config-copy" and kwargs.get("dir_fd") is not None:
            secret.unlink()
            unlinked = True
        return real_stat(path, *args, **kwargs)
    monkeypatch.setattr(os, "stat", racing_stat)
    with pytest.raises(InvalidSandboxRequest, match="snapshot failed closed"):
        session = SystemdReadOnlyLauncher().launch(workspace, ["/usr/bin/cat", "config-copy"])
        session.wait()
