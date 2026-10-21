"""Application gate and exactly-once host execution contracts."""

import importlib.util
import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from orchestrator.application import ApplicationNotReady
from tests.support.worker_input import make_worker_input_case, admit_worker_input_case


def execution_case(tmp_path, *, tools=True, command=("/usr/bin/true",), host_changes=None, approval=False):
    m = api()
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.config import ConfigCandidate, ConfigManager
    from orchestrator.isolation import SandboxLimits
    from orchestrator.lifecycle import LifecycleController, NodeSpec
    from orchestrator.routing import compile_node_contract
    from orchestrator.security import PolicyAuthority, PolicyManifest
    from orchestrator.tools.gateway import PolicyState
    from tests.unit.lifecycle.test_scheduler import (
        NOW, registry, effective_config, routed_pair, _application_limits,
    )
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    leases, journals = tmp_path / "leases", tmp_path / "journals"
    leases.mkdir(mode=0o700)
    journals.mkdir(mode=0o700)
    clock = SimpleNamespace(now=NOW + timedelta(seconds=2))
    host = m.WorkspaceExecutionHost(**(dict(workspace=workspace, lease_root=leases,
        journal_root=journals, limits=SandboxLimits(), policy_state=lambda request: PolicyState(0, 0),
        clock=lambda: clock.now) | (host_changes or {})))
    app = ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits(),
                                  workspace_execution_host=host)
    reg = registry()
    resolved = effective_config(reg)
    policy = PolicyManifest(authorities=(PolicyAuthority(source="system", max_permission="workspace-write",
        allowed_actions=("model_invoke", "reversible_workspace_write"),
        allowed_tools=("model:model-1", "workspace.write-candidate"),
        approval_actions=("reversible_workspace_write",) if approval else ()),))
    text = json.dumps({"schema": 1, "tool_id": "workspace.write-candidate", "command": command},
                      sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    tool_ids = ("workspace.write-candidate",) if tools else ()
    contract = compile_node_contract(run_id="run-1", node_id="node-1", role="coder", task_text=text,
        config=resolved.config, registry=reg, policy_manifest=policy, context_tokens=1000,
        max_output_tokens=500, required_capabilities=("text",), tool_ids=tool_ids)
    ConfigManager(ConfigCandidate(resolved=resolved, registry=reg)).start_run("run-1", app._event_store,
                                                                            workspace=workspace)
    lifecycle = LifecycleController(app._event_store)
    lifecycle.initialize_run("run-1")
    lifecycle.append_nodes("run-1", (NodeSpec(node_id="node-1", role="coder",
        planning_contract=contract, planning_contract_hash=contract.contract_hash,
        tool_ids=tool_ids, max_attempts=3),), expected_graph_version=0,
        idempotency_key="graph", policy_manifest=policy)
    lifecycle.start_run("run-1")
    request, decision = routed_pair(reg, resolved.config, policy, contract_hash=contract.contract_hash)
    app.accept_routing_with_input(request, decision, task_text=text,
        accepted_at=NOW + timedelta(seconds=1), lease_expires_at=NOW + timedelta(hours=1))
    return SimpleNamespace(app=app, workspace=workspace, host=host, text=text, clock=clock,
        identity=dict(run_id="run-1", node_id="node-1", attempt_id=request.attempt_id, fencing_generation=1),
        lifecycle=lifecycle, registry=reg, config=resolved.config, policy=policy,
        contract=contract)


def api():
    assert importlib.util.find_spec("orchestrator.runtime.workspace_execution"), "workspace service missing"
    from orchestrator.runtime import workspace_execution
    return workspace_execution


def test_missing_host_is_safe_block_and_closed_app_is_gated(tmp_path):
    api()
    case = make_worker_input_case(tmp_path / "events.db")
    try:
        admit_worker_input_case(case)
        result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome == "blocked"
        assert result.reason_code == "isolation_unavailable"
        assert result.publication_atomicity == "per_path_crash_recoverable"
        case.app.close()
        with pytest.raises(ApplicationNotReady):
            case.app.execute_workspace_task(**case.identity, task_text=case.text)
    finally:
        case.app.close()


@pytest.mark.parametrize("fault", ["text", "canonical-substitution", "natural-language", "fence", "run", "node", "attempt", "cancel", "tool"])
def test_unbound_or_stale_input_never_launches(tmp_path, monkeypatch, fault):
    m = api()
    case = execution_case(tmp_path, tools=fault != "tool")
    def forbidden(*args, **kwargs):
        pytest.fail("unbound input reached untrusted execution")
    monkeypatch.setattr(m.IsolatedWorkspaceWorker, "propose", forbidden)
    try:
        if fault == "text": case.text += " "
        if fault == "canonical-substitution": case.text = case.text.replace("/usr/bin/true", "/usr/bin/false")
        if fault == "natural-language": case.text = "please make a change"
        if fault == "fence": case.identity["fencing_generation"] = 2
        if fault in {"run", "node", "attempt"}: case.identity[fault + "_id"] = "other"
        if fault == "cancel": case.lifecycle.request_cancel("run-1", reason_code="user_cancelled")
        result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome == "blocked"
        assert not list(case.workspace.iterdir())
        assert not case.app._event_store.stream_ids("workspace_worker_operation")
    finally:
        case.app.close()


def test_unknown_worker_start_is_reserved_and_never_retried(tmp_path, monkeypatch):
    m = api()
    from orchestrator.isolation import IsolationStartUnknown
    case = execution_case(tmp_path)
    calls = []
    def unknown(*args, **kwargs):
        assert not case.app._event_store._connection.in_transaction
        calls.append(1)
        raise IsolationStartUnknown("sensitive-error")
    # Replace only the external isolation boundary; real admission and CAS remain.
    monkeypatch.setattr(m, "verify_workspace_isolation", lambda *a, **k: SimpleNamespace(
        outcome="eligible", profile_hash="sha256:" + "a" * 64,
        workspace_identity_hash=case.app._scheduler.lifecycle.replay("run-1").workspace_identity_hash))
    monkeypatch.setattr(m.IsolatedWorkspaceWorker, "propose", unknown)
    try:
        first = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        second = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert first == second
        assert first.outcome == "outcome_unknown"
        assert len(calls) == 1
        events = case.app._event_store.read_stream("workspace_worker_operation", first.operation_id)
        assert [e.event_type for e in events] == ["WorkspaceWorkerOperationRequested", "WorkspaceWorkerOperationFinished"]
        assert case.text not in repr(events)
        assert "sensitive-error" not in repr(first) + repr(events)
    finally:
        case.app.close()


@pytest.mark.parametrize("override", ["workspace", "profile", "principal", "scope", "operation_id"])
def test_request_cannot_supply_host_authority(tmp_path, override):
    api()
    case = make_worker_input_case(tmp_path / "events.db")
    try:
        with pytest.raises(TypeError):
            case.app.execute_workspace_task(**case.identity, task_text=case.text,
                                             **{override: "forged"})
    finally:
        case.app.close()


def unavailable(monkeypatch, m):
    from orchestrator.isolation import IsolationNotStarted
    def no_runtime(*args, **kwargs):
        raise IsolationNotStarted("private runtime error")
    monkeypatch.setattr(m, "verify_workspace_isolation", no_runtime)


def test_exact_terminal_replay_survives_expiry_and_reopen(tmp_path, monkeypatch):
    m = api()
    from orchestrator.application import ControlPlaneApplication
    from tests.unit.lifecycle.test_scheduler import _application_limits
    case = execution_case(tmp_path)
    unavailable(monkeypatch, m)
    result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
    case.clock.now += timedelta(days=1)
    case.app.close()
    with ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits(),
                                  workspace_execution_host=case.host) as app:
        assert app.execute_workspace_task(**case.identity, task_text=case.text) == result


@pytest.mark.parametrize("field", ["role", "context", "program_hash", "scope_hash", "causation_id", "result", "reason"])
def test_startup_rejects_corrupt_operation_binding(tmp_path, monkeypatch, field):
    m = api()
    from orchestrator.application import ControlPlaneApplication, StartupRecoveryFailed
    from tests.support.worker_input import corrupt_worker_input_event
    from tests.unit.lifecycle.test_scheduler import _application_limits
    case = execution_case(tmp_path)
    unavailable(monkeypatch, m)
    result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
    def corrupt(payload):
        if field == "result":
            payload["result"]["operation_id"] = "sha256:" + "0" * 64
        elif field == "reason":
            payload["result"]["reason_code"] = "workspace_published"
        else:
            payload[field] = "forged"
    corrupt_worker_input_event(case.app._event_store, ("workspace_worker_operation", result.operation_id), corrupt)
    case.app.close()
    with pytest.raises(StartupRecoveryFailed):
        ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits())


@pytest.mark.parametrize("phase", ["workspace-worker-snapshot", "request", "terminal"])
def test_audit_failure_is_fatal_and_never_reexecutes(tmp_path, monkeypatch, phase):
    m = api()
    from orchestrator.tools.gateway import ToolAuditUnavailable
    case = execution_case(tmp_path)
    unavailable(monkeypatch, m)
    original = case.app._event_store.append_checked
    def fail(stream, stream_id, key, decide):
        if key == phase and stream in {"scheduler", "workspace_worker_operation"}:
            raise OSError("private database error")
        return original(stream, stream_id, key, decide)
    monkeypatch.setattr(case.app._event_store, "append_checked", fail)
    try:
        with pytest.raises(ToolAuditUnavailable) as caught:
            case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert "private database error" not in str(caught.value)
        assert caught.value.__context__ is None
        assert caught.value.__cause__ is None
        monkeypatch.setattr(case.app._event_store, "append_checked", original)
        if phase == "terminal":
            result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
            assert result.outcome == "outcome_unknown" and result.reason_code == "operation_in_progress"
        else:
            assert not case.app._event_store.stream_ids("workspace_worker_operation")
    finally:
        case.app.close()


@pytest.mark.parametrize("fault", ["command", "reference", "context", "cancel", "graph", "route-context", "no-stop"])
def test_post_worker_substitution_or_authority_loss_prevents_gateway(tmp_path, monkeypatch, fault):
    m = api()
    from orchestrator.runtime.workspace_worker import decode_workspace_proposal, proposal_bytes
    from tests.unit.runtime.test_worker_process import _sandbox_result
    from tests.support.worker_input import corrupt_worker_input_event
    from orchestrator.lifecycle import NodeSpec
    case = execution_case(tmp_path)
    monkeypatch.setattr(m, "verify_workspace_isolation", lambda *a, **k: SimpleNamespace(
        outcome="eligible", profile_hash="sha256:" + "a" * 64,
        workspace_identity_hash=case.app._scheduler.lifecycle.replay("run-1").workspace_identity_hash))
    def propose(worker, workspace, task):
        assert not case.app._event_store._connection.in_transaction
        worker.termination_receipt = _sandbox_result().termination_receipt if fault != "no-stop" else None
        proposal = decode_workspace_proposal(proposal_bytes(task))
        if fault == "command": return proposal.model_copy(update={"command": ("/usr/bin/false",)})
        if fault == "reference": return proposal.model_copy(update={"tool_reference": proposal.tool_reference.model_copy(update={"grant_id": "forged"})})
        if fault == "context": return proposal.model_copy(update={"context": proposal.context.model_copy(update={"graph_version": 2})})
        if fault == "cancel": case.lifecycle.request_cancel("run-1", reason_code="user_cancelled")
        if fault == "graph":
            case.lifecycle.append_nodes("run-1", (NodeSpec(node_id="extra", role="reviewer", planning_contract_hash="sha256:" + "0" * 64),),
                expected_graph_version=1, idempotency_key="change-graph")
        if fault == "route-context":
            corrupt_worker_input_event(case.app._event_store, ("scheduler", "global"),
                lambda p: p["verification_context"].update(input_manifest_hash="sha256:" + "0" * 64))
        return proposal
    def gateway(*a, **k):
        pytest.fail("invalid or stale Worker proposal reached Gateway")
    monkeypatch.setattr(m.IsolatedWorkspaceWorker, "propose", propose)
    monkeypatch.setattr(m.WorkspaceWriteGateway, "execute", gateway)
    try:
        if fault == "route-context":
            from orchestrator.tools.gateway import ToolAuditUnavailable
            with pytest.raises(ToolAuditUnavailable):
                case.app.execute_workspace_task(**case.identity, task_text=case.text)
            assert not case.app._event_store.read_stream("security", "run-1")
            return
        result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome in {"blocked", "failed", "outcome_unknown"}
        assert not case.app._event_store.read_stream("security", "run-1")
    finally:
        case.app.close()


def test_wire_proposal_requires_schema_key(tmp_path):
    from orchestrator.runtime.workspace_worker import decode_workspace_proposal, proposal_bytes, WorkspaceWorkerError
    from tests.unit.runtime.test_workspace_worker import envelope
    payload = json.loads(proposal_bytes(envelope()))
    payload["schema_id"] = payload.pop("schema")
    with pytest.raises(WorkspaceWorkerError):
        decode_workspace_proposal(json.dumps(payload).encode())


def test_insecure_host_roots_block_before_worker(tmp_path, monkeypatch):
    m = api()
    case = execution_case(tmp_path)
    case.host.lease_root.chmod(0o755)
    monkeypatch.setattr(m, "verify_workspace_isolation", lambda *a, **k: SimpleNamespace(
        outcome="eligible", profile_hash="sha256:" + "a" * 64,
        workspace_identity_hash=case.app._scheduler.lifecycle.replay("run-1").workspace_identity_hash))
    def worker(*args, **kwargs):
        pytest.fail("unsafe host roots reached Worker")
    monkeypatch.setattr(m.IsolatedWorkspaceWorker, "propose", worker)
    try:
        result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome == "blocked" and result.reason_code == "isolation_unavailable"
    finally:
        case.app.close()


def test_host_approval_factory_requires_exact_application_store(tmp_path):
    m = api()
    from orchestrator.application import ControlPlaneApplication, StartupRecoveryFailed
    from orchestrator.approvals import ApprovalService
    from orchestrator.budget import BudgetLedger
    from orchestrator.persistence import SQLiteEventStore
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from dataclasses import replace
    case = execution_case(tmp_path)
    case.app.close()
    with SQLiteEventStore(tmp_path / "other.db") as other:
        wrong = ApprovalService(event_store=other, budget_ledger=BudgetLedger(other),
            authenticator=lambda _: None, attempt_authority=None, policy_state=lambda _: None)
        host = replace(case.host, approval_factory=lambda _: wrong, approval_requester=lambda _: "local")
        with pytest.raises(StartupRecoveryFailed):
            ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits(),
                                    workspace_execution_host=host)


@pytest.mark.parametrize("change", [
    {"approval_factory": lambda _: None}, {"approval_requester": lambda _: "forged"},
    {"policy_state": None}, {"clock": None}, {"limits": {}}, {"workspace": "untrusted-path"},
])
def test_host_configuration_is_validated(tmp_path, change):
    with pytest.raises(ValueError):
        execution_case(tmp_path, host_changes=change)


@pytest.mark.parametrize("change", ["cancel", "revoke"])
def test_admission_probe_cannot_hide_authority_change_before_worker(tmp_path, monkeypatch, change):
    m = api()
    from orchestrator.tools.gateway import PolicyState
    state = {"revoked": False}
    case = execution_case(tmp_path, host_changes={"policy_state": lambda _: PolicyState(0, 0, revoked=state["revoked"])})
    def probe(*args, **kwargs):
        if change == "cancel": case.lifecycle.request_cancel("run-1", reason_code="user_cancelled")
        else: state["revoked"] = True
        return SimpleNamespace(outcome="eligible", profile_hash="sha256:" + "a" * 64,
            workspace_identity_hash=case.app._scheduler.lifecycle.replay("run-1").workspace_identity_hash)
    def worker(*args, **kwargs):
        pytest.fail("changed authority reached Worker")
    monkeypatch.setattr(m, "verify_workspace_isolation", probe)
    monkeypatch.setattr(m.IsolatedWorkspaceWorker, "propose", worker)
    try:
        result = case.app.execute_workspace_task(**case.identity, task_text=case.text)
        assert result.outcome == "blocked"
    finally:
        case.app.close()


@pytest.mark.parametrize("corruption", ["missing-receipt", "unknown-stop", "proposal", "extra"])
def test_incomplete_operation_with_corrupt_worker_stop_is_quarantined(tmp_path, monkeypatch, corruption):
    m = api()
    from orchestrator.application import ControlPlaneApplication, StartupRecoveryFailed
    from orchestrator.runtime.workspace_worker import decode_workspace_proposal, proposal_bytes
    from orchestrator.tools.gateway import ToolAuditUnavailable
    from tests.support.worker_input import corrupt_worker_input_event
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_worker_process import _sandbox_result
    case = execution_case(tmp_path)
    monkeypatch.setattr(m, "verify_workspace_isolation", lambda *a, **k: SimpleNamespace(
        outcome="eligible", profile_hash="sha256:" + "a" * 64,
        workspace_identity_hash=case.app._scheduler.lifecycle.replay("run-1").workspace_identity_hash))
    def propose(worker, workspace, task):
        worker.termination_receipt = _sandbox_result().termination_receipt
        return decode_workspace_proposal(proposal_bytes(task))
    def unavailable_audit(*args, **kwargs):
        raise ToolAuditUnavailable("test audit unavailable")
    monkeypatch.setattr(m.IsolatedWorkspaceWorker, "propose", propose)
    monkeypatch.setattr(m.WorkspaceWriteGateway, "execute", unavailable_audit)
    with pytest.raises(ToolAuditUnavailable):
        case.app.execute_workspace_task(**case.identity, task_text=case.text)
    operation, = case.app._event_store.stream_ids("workspace_worker_operation")
    assert [event.event_type for event in case.app._event_store.read_stream(
        "workspace_worker_operation", operation)] == ["WorkspaceWorkerOperationRequested", "WorkspaceWorkerStopped"]
    def corrupt(payload):
        if corruption == "missing-receipt": payload.pop("termination_receipt")
        elif corruption == "unknown-stop": payload["termination_receipt"]["cgroup_empty"] = False
        elif corruption == "proposal": payload["proposal_hash"] = "sha256:" + "0" * 64
        else: payload["untrusted"] = "extra"
    corrupt_worker_input_event(case.app._event_store, ("workspace_worker_operation", operation), corrupt)
    with pytest.raises(ToolAuditUnavailable):
        case.app.execute_workspace_task(**case.identity, task_text=case.text)
    assert not list(case.workspace.iterdir())
    case.app.close()
    with pytest.raises(StartupRecoveryFailed):
        ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits())
