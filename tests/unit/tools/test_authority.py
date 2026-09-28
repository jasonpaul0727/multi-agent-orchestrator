from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.agents.registry import AgentInstance, AgentRegistry
from orchestrator.lifecycle.models import AttemptState, NodeSpec
from orchestrator.isolation import SandboxResult
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.security import PolicyAuthority, PolicyManifest
from orchestrator.tools.authority import DurableAttemptAuthority
from orchestrator.tools.workspace_write import WORKSPACE_WRITE_TOOL_ID, WorkspaceWriteRequest
from orchestrator.tools.gateway import READ_ONLY_COMMAND_TOOL_ID, PolicyState, ToolGateway, ToolRequest
from orchestrator.tools.authority import _attempt_ref
from orchestrator.workspace_identity import workspace_identity_hash


_HASH = "sha256:" + "c" * 64
_NOW = datetime(2026, 9, 25, 12, tzinfo=timezone.utc)


def _append(store: SQLiteEventStore, stream: str, stream_id: str, draft: EventDraft, key: str):
    version = store.current_version(stream, stream_id)
    return store.append(stream, stream_id, version, (draft,), key)[0]


def _fixture(
    tmp_path, *, tool_ids=(READ_ONLY_COMMAND_TOOL_ID,), lease_seconds=3600, workspace_bound=True
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    manifest = PolicyManifest(
        authorities=(PolicyAuthority(
            source="system",
            max_permission="read-only",
            allowed_actions=("safe_read",),
            allowed_tools=(READ_ONLY_COMMAND_TOOL_ID,),
        ),)
    )
    agent_id = AgentRegistry.agent_id_for_attempt("run-1", "attempt-1")
    lease = (_NOW + timedelta(seconds=lease_seconds)).isoformat()
    node = NodeSpec(
        node_id="node-1",
        role="coder",
        planning_contract_hash=_HASH,
        tool_ids=tool_ids,
    )
    attempt = AttemptState(
        attempt_id="attempt-1",
        agent_instance_id=agent_id,
        fencing_generation=1,
        decision_hash=_HASH,
        policy_manifest_hash=manifest.content_hash,
        reasoning_effort="low",
        model_id="model-1",
        provider_id="provider-1",
        reservation_id="reservation-1",
        lease_expires_at=lease,
        status="accepted",
    )
    init_payload = {
        "run_id": "run-1", "config_hash": _HASH, "registry_hash": _HASH,
        "max_nodes": 2, "max_depth": 1,
    }
    if workspace_bound:
        init_payload["workspace_identity_hash"] = workspace_identity_hash(workspace)
    _append(store, "run_lifecycle", "run-1", EventDraft("RunInitialized", init_payload), "init")
    _append(store, "run_lifecycle", "run-1", EventDraft("GraphNodesAppended", {
        "graph_version": 1,
        "nodes": [node.model_dump(mode="json")],
        "policy_manifest": manifest.model_dump(mode="json"),
        "policy_manifest_hash": manifest.content_hash,
    }), "graph")
    _append(store, "run_lifecycle", "run-1", EventDraft("RunStarted", {"run_id": "run-1"}), "start")
    acceptance = _append(store, "run_lifecycle", "run-1", EventDraft(
        "AttemptAccepted",
        {"node_id": "node-1", "attempt": attempt.model_dump(mode="json")},
        run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
        correlation_id="run-1", causation_id=_HASH,
    ), "attempt")
    agent = AgentInstance(
        agent_instance_id=agent_id, run_id="run-1", node_id="node-1", attempt_id="attempt-1",
        created_by_attempt_id="attempt-1", parent_agent_instance_id=None, depth=0,
        role="coder", model_id="model-1", provider_id="provider-1",
        decision_hash=_HASH, policy_manifest_hash=manifest.content_hash,
        reasoning_effort="low", fencing_generation=1, status="created",
    )
    _append(store, "agent_registry", "run-1", EventDraft(
        "AgentInstanceCreated", {"instance": agent.model_dump(mode="json")},
        run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
        correlation_id="run-1", causation_id=_HASH,
    ), "agent-create")
    _append(store, "agent_registry", "run-1", EventDraft(
        "AgentStarted", {"agent_instance_id": agent_id},
        run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
        correlation_id="run-1", causation_id=_HASH,
    ), "agent-start")
    ref = _attempt_ref("run-1", "node-1", "attempt-1")
    _append(store, "scheduler", "global", EventDraft(
        "RoutingDecisionAccepted", {
            "attempt_ref": ref, "run_id": "run-1", "node_id": "node-1",
            "attempt_id": "attempt-1", "agent_instance_id": agent_id,
            "decision_hash": _HASH, "lease_expires_at": lease,
            "tool_ids": list(tool_ids), "recovery_action": "initial",
        },
        run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
        correlation_id="run-1", causation_id=_HASH,
    ), "route")
    authority = DurableAttemptAuthority(
        database, workspace=workspace, policy_manifest_hash=manifest.content_hash,
        clock=lambda: _NOW,
    )
    request = ToolRequest(
        request_id="tool-1", run_id="run-1", node_id="node-1", attempt_id="attempt-1",
        fencing_generation=1, role="coder", causation_id=acceptance.event_id,
        workspace=str(workspace), command=("/usr/bin/printf", "ok"),
    )
    return store, authority, request, manifest, ref


def test_durable_authority_accepts_only_live_bound_attempt(tmp_path) -> None:
    store, authority, request, _, _ = _fixture(tmp_path)
    assert authority.is_current(request)
    store.close()


def test_durable_authority_accepts_write_tool_only_when_route_names_it(tmp_path) -> None:
    tool_ids = (READ_ONLY_COMMAND_TOOL_ID, WORKSPACE_WRITE_TOOL_ID)
    store, authority, request, _, _ = _fixture(tmp_path, tool_ids=tool_ids)
    write_request = WorkspaceWriteRequest.model_validate(request.model_dump())

    assert authority.is_current(write_request)
    assert not authority.is_current(write_request.model_copy(update={"attempt_id": "attempt-2"}))
    store.close()


def test_durable_authority_is_available_from_tools_package() -> None:
    import orchestrator.tools as tools

    assert tools.DurableAttemptAuthority is DurableAttemptAuthority


@pytest.mark.parametrize("change", [
    {"run_id": "run-2"}, {"node_id": "node-2"}, {"attempt_id": "attempt-2"},
    {"fencing_generation": 2}, {"role": "reviewer"},
    {"causation_id": "forged-acceptance"},
])
def test_durable_authority_rejects_spoofed_identity(tmp_path, change) -> None:
    store, authority, request, _, _ = _fixture(tmp_path)
    assert not authority.is_current(request.model_copy(update=change))
    store.close()


def test_durable_authority_rejects_other_workspace_and_unlisted_tool(tmp_path) -> None:
    store, authority, request, _, _ = _fixture(tmp_path, tool_ids=())
    assert not authority.is_current(request)
    other = tmp_path / "other"
    other.mkdir()
    assert not authority.is_current(request.model_copy(update={"workspace": str(other)}))
    store.close()


def test_durable_authority_rejects_workspace_replaced_at_same_path(tmp_path) -> None:
    store, authority, request, _, _ = _fixture(tmp_path)
    workspace = tmp_path / "workspace"
    displaced = tmp_path / "workspace-original"
    workspace.rename(displaced)
    workspace.mkdir()

    assert not authority.is_current(request)
    store.close()


def test_durable_authority_rejects_run_without_frozen_workspace_binding(tmp_path) -> None:
    store, authority, request, _, _ = _fixture(tmp_path, workspace_bound=False)

    assert not authority.is_current(request)
    store.close()


def test_durable_authority_rejects_expired_lease_without_sweeper(tmp_path) -> None:
    store, authority, request, _, _ = _fixture(tmp_path, lease_seconds=0)
    assert not authority.is_current(request)
    store.close()


def test_durable_authority_rejects_paused_run_and_policy_mismatch(tmp_path) -> None:
    store, authority, request, _, _ = _fixture(tmp_path)
    other = DurableAttemptAuthority(
        tmp_path / "events.db", workspace=tmp_path / "workspace",
        policy_manifest_hash=_HASH, clock=lambda: _NOW,
    )
    assert not other.is_current(request)
    _append(store, "run_lifecycle", "run-1", EventDraft("RunPaused", {"run_id": "run-1"}), "pause")
    assert not authority.is_current(request)
    store.close()


def test_durable_authority_rejects_released_or_unknown_scheduler_attempt(tmp_path) -> None:
    store, authority, request, _, ref = _fixture(tmp_path)
    _append(store, "scheduler", "global", EventDraft(
        "AttemptOutcomeUnknown", {"attempt_ref": ref, "reason": "lease_expired"},
        run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
        correlation_id="run-1", causation_id=_HASH,
    ), "unknown")
    assert not authority.is_current(request)
    store.close()


def test_durable_authority_is_safe_from_monitor_thread(tmp_path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    store, authority, request, _, _ = _fixture(tmp_path)
    with ThreadPoolExecutor(max_workers=2) as executor:
        assert all(executor.map(authority.is_current, (request, request)))
    store.close()


def test_durable_authority_can_recheck_inside_gateway_write_transaction(tmp_path) -> None:
    class Session:
        def wait(self) -> SandboxResult:
            return SandboxResult(
                unit_name="test.service", returncode=0, stdout=b"ok", stderr=b"",
                elapsed_seconds=0.01, termination_confirmed=True, cancelled=False,
                timed_out=False, output_limited=False,
            )

        def cancel(self) -> bool:
            return True

    class Launcher:
        def launch(self, workspace, command, *, limits, expected_workspace_identity_hash):
            assert expected_workspace_identity_hash == workspace_identity_hash(workspace)
            return Session()

    store, authority, request, manifest, _ = _fixture(tmp_path)
    gateway = ToolGateway(
        run_id="run-1", workspace=tmp_path / "workspace", event_store=store,
        policy_manifest=manifest, attempt_authority=authority,
        policy_state=lambda _request: PolicyState(0, 0), launcher=Launcher(),
    )

    result = gateway.execute(request)

    assert result.outcome == "completed"
    assert result.stdout == b"ok"
    assert store.read_stream("security", "run-1")[-1].event_type == "ToolExecutionCompleted"
    store.close()
