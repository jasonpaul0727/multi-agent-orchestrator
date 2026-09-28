from datetime import datetime, timezone
import hashlib

from orchestrator.approvals import (
    ApprovalPolicyState,
    ApprovalPrincipal,
    ApprovalService,
    ExecutionAttempt,
)
from orchestrator.budget import BudgetLedger, RunLimit
from orchestrator.isolation import SandboxResult, SandboxTerminationReceipt
from orchestrator.persistence import SQLiteEventStore
from orchestrator.security.policy import PolicyAuthority, PolicyManifest
from orchestrator.tools import PolicyState, ToolGateway, ToolRequest


_NOW = datetime(2026, 9, 26, 18, 0, tzinfo=timezone.utc)


def _hash(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _manifest() -> PolicyManifest:
    from orchestrator.tools import READ_ONLY_COMMAND_TOOL_ID

    return PolicyManifest(
        authorities=(PolicyAuthority(
            source="system",
            max_permission="read-only",
            allowed_actions=("safe_read",),
            allowed_tools=(READ_ONLY_COMMAND_TOOL_ID,),
            approval_actions=("safe_read",),
        ),)
    )


def _tool_request(
    workspace,
    *,
    request_id: str,
    attempt_id: str,
    generation: int,
    command: tuple[str, ...] = ("/usr/bin/printf", "approval-scoped output"),
    role: str = "worker",
) -> ToolRequest:
    return ToolRequest(
        request_id=request_id,
        run_id="run-1",
        node_id="node-1",
        attempt_id=attempt_id,
        fencing_generation=generation,
        role=role,
        causation_id=f"{attempt_id}-accepted-event",
        workspace=str(workspace),
        command=command,
    )


class _ToolAuthority:
    def is_current(self, request: ToolRequest) -> bool:
        return request.run_id == "run-1" and request.node_id == "node-1"


class _ApprovalAuthority:
    def is_current(self, attempt: ExecutionAttempt) -> bool:
        return attempt.attempt_id == "attempt-2" and attempt.fencing_generation == 3


class _Session:
    def wait(self):
        return SandboxResult(
            unit_name="maestro-attempt-" + "1" * 32 + ".service",
            returncode=0,
            stdout=b"approval-scoped output",
            stderr=b"",
            elapsed_seconds=0.01,
            termination_receipt=SandboxTerminationReceipt(
                unit_name="maestro-attempt-" + "1" * 32 + ".service",
                control_group="/user.slice/user-1000.slice/user@1000.service/app.slice/maestro-attempt-" + "1" * 32 + ".service",
                active_state="inactive",
                cgroup_empty=True,
            ),
            cancelled=False,
            timed_out=False,
            output_limited=False,
        )

    def cancel(self):
        return True


class _Launcher:
    def __init__(self):
        self.calls = []

    def launch(self, workspace, command, *, limits, expected_workspace_identity_hash):
        self.calls.append((workspace, command, expected_workspace_identity_hash))
        return _Session()


def _approval_fixture(tmp_path, *, tool_authority=None, service_adapter=None, currency="USD"):
    store = SQLiteEventStore(tmp_path / "approval-additional.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100, currency=currency)},
    )
    approvals = ApprovalService(
        event_store=store,
        budget_ledger=ledger,
        authenticator=lambda credential: ApprovalPrincipal(
            principal_id=credential,
            principal_type="human",
            permissions=frozenset({"approval:approve"}),
        ),
        attempt_authority=_ApprovalAuthority(),
        policy_state=lambda request: ApprovalPolicyState(1, 4),
        now=lambda: _NOW,
    )
    launcher = _Launcher()
    gateway = ToolGateway(
        run_id="run-1",
        workspace=tmp_path,
        event_store=store,
        policy_manifest=_manifest(),
        attempt_authority=tool_authority or _ToolAuthority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=launcher,
        approval_service=service_adapter or approvals,
        approval_requester=lambda request: "requester-1",
        approval_isolation_profile_hash=_hash("measured-readonly-profile"),
        approval_now=lambda: _NOW,
    )
    return store, approvals, launcher, gateway


def test_approval_tool_gateway_requires_fresh_attempt_and_records_effect_receipt(tmp_path):
    store = SQLiteEventStore(tmp_path / "approval-tool.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    approvals = ApprovalService(
        event_store=store,
        budget_ledger=ledger,
        authenticator=lambda credential: ApprovalPrincipal(
            principal_id=credential,
            principal_type="human",
            permissions=frozenset({"approval:approve"}),
        ),
        attempt_authority=_ApprovalAuthority(),
        policy_state=lambda request: ApprovalPolicyState(1, 4),
        now=lambda: _NOW,
    )
    launcher = _Launcher()
    gateway = ToolGateway(
        run_id="run-1",
        workspace=tmp_path,
        event_store=store,
        policy_manifest=_manifest(),
        attempt_authority=_ToolAuthority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=launcher,
        approval_service=approvals,
        approval_requester=lambda request: "requester-1",
        approval_isolation_profile_hash=_hash("measured-readonly-profile"),
        approval_now=lambda: _NOW,
    )

    waiting = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-1", attempt_id="attempt-1", generation=2,
    ))

    assert waiting.outcome == "awaiting_approval"
    assert waiting.approval_request_id is not None
    assert launcher.calls == []
    request_event = next(
        event for event in store.read_stream("security", "run-1")
        if event.event_type == "ApprovalRequested"
    )
    assert request_event.payload["approval_request_id"] == waiting.approval_request_id
    assert "approval-scoped output" not in repr(request_event.payload)
    issued = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact read")

    completed = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-2", attempt_id="attempt-2", generation=3,
    ), approval_grant_id=issued.approval_grant_id)

    assert completed.outcome == "completed"
    assert completed.stdout == b"approval-scoped output"
    assert len(launcher.calls) == 1
    security_types = [event.event_type for event in store.read_stream("security", "run-1")]
    assert security_types.count("ApprovalGrantBound") == 1
    assert security_types.count("ToolApprovalConsumed") == 1
    assert security_types[-2:] == ["ToolExecutionStarted", "ToolExecutionCompleted"]
    budget_events = store.read_stream("budget", "run-1")
    assert [event.event_type for event in budget_events] == [
        "EffectIntentRecorded",
        "ApprovalGrantConsumed",
        "BudgetReserved",
        "EffectReceiptRecorded",
    ]
    assert budget_events[-1].payload["outcome"] == "applied"
    assert budget_events[-1].payload["receipt_hash"].startswith("sha256:")


def test_approval_tool_gateway_rejects_approved_work_on_origin_attempt(tmp_path):
    # This test exercises the approval service's fresh-attempt rule through the
    # host integration: an approved request does not turn its origin Attempt
    # into an executable capability.
    store = SQLiteEventStore(tmp_path / "approval-origin.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    approvals = ApprovalService(
        event_store=store,
        budget_ledger=ledger,
        authenticator=lambda credential: ApprovalPrincipal(
            principal_id=credential,
            principal_type="human",
            permissions=frozenset({"approval:approve"}),
        ),
        attempt_authority=_ApprovalAuthority(),
        policy_state=lambda request: ApprovalPolicyState(1, 4),
        now=lambda: _NOW,
    )
    gateway = ToolGateway(
        run_id="run-1",
        workspace=tmp_path,
        event_store=store,
        policy_manifest=_manifest(),
        attempt_authority=_ToolAuthority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=_Launcher(),
        approval_service=approvals,
        approval_requester=lambda request: "requester-1",
        approval_isolation_profile_hash=_hash("measured-readonly-profile"),
        approval_now=lambda: _NOW,
    )
    waiting = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-1", attempt_id="attempt-1", generation=2,
    ))
    issued = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact read")

    denied = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-origin-replay", attempt_id="attempt-1", generation=2,
    ), approval_grant_id=issued.approval_grant_id)

    assert denied.outcome == "denied"
    assert launcher_not_started(store)
    assert not any(
        event.event_type == "ApprovalGrantBound"
        for event in store.read_stream("security", "run-1")
    )


def test_approval_scope_mismatch_does_not_bind_or_consume_grant(tmp_path):
    store = SQLiteEventStore(tmp_path / "approval-scope.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    approvals = ApprovalService(
        event_store=store,
        budget_ledger=ledger,
        authenticator=lambda credential: ApprovalPrincipal(
            principal_id=credential,
            principal_type="human",
            permissions=frozenset({"approval:approve"}),
        ),
        attempt_authority=_ApprovalAuthority(),
        policy_state=lambda request: ApprovalPolicyState(1, 4),
        now=lambda: _NOW,
    )
    launcher = _Launcher()
    gateway = ToolGateway(
        run_id="run-1",
        workspace=tmp_path,
        event_store=store,
        policy_manifest=_manifest(),
        attempt_authority=_ToolAuthority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=launcher,
        approval_service=approvals,
        approval_requester=lambda request: "requester-1",
        approval_isolation_profile_hash=_hash("measured-readonly-profile"),
        approval_now=lambda: _NOW,
    )
    waiting = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-1", attempt_id="attempt-1", generation=2,
    ))
    issued = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact read")

    mismatched = gateway.execute(_tool_request(
        tmp_path,
        request_id="tool-request-wrong-scope",
        attempt_id="attempt-2",
        generation=3,
        command=("/usr/bin/id",),
    ), approval_grant_id=issued.approval_grant_id)

    assert mismatched.outcome == "denied"
    assert launcher.calls == []
    assert not any(
        event.event_type == "ApprovalGrantBound"
        for event in store.read_stream("security", "run-1")
    )

    wrong_role = gateway.execute(_tool_request(
        tmp_path,
        request_id="tool-request-wrong-role",
        attempt_id="attempt-2",
        generation=3,
        role="director",
    ), approval_grant_id=issued.approval_grant_id)
    assert wrong_role.outcome == "denied"
    assert not any(
        event.event_type == "ApprovalGrantBound"
        for event in store.read_stream("security", "run-1")
    )

    completed = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-2", attempt_id="attempt-2", generation=3,
    ), approval_grant_id=issued.approval_grant_id)
    assert completed.outcome == "completed"
    assert len(launcher.calls) == 1


def test_approval_authority_loss_after_consumption_writes_not_applied_receipt(tmp_path):
    class LosingAuthority:
        def __init__(self):
            self.calls = 0

        def is_current(self, request):
            self.calls += 1
            return self.calls <= 5

    store, approvals, launcher, gateway = _approval_fixture(
        tmp_path,
        tool_authority=LosingAuthority(),
    )
    waiting = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-1", attempt_id="attempt-1", generation=2,
    ))
    issued = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact read")

    try:
        gateway.execute(_tool_request(
            tmp_path, request_id="tool-request-2", attempt_id="attempt-2", generation=3,
        ), approval_grant_id=issued.approval_grant_id)
    except PermissionError as exc:
        assert "before isolated launch" in str(exc)
    else:
        raise AssertionError("revoked approval-bound attempt should not launch")

    assert launcher.calls == []
    budget_events = store.read_stream("budget", "run-1")
    assert budget_events[-1].event_type == "EffectReceiptRecorded"
    assert budget_events[-1].payload["outcome"] == "not_applied"
    assert store.read_stream("security", "run-1")[-1].event_type == "ToolExecutionCancelled"


def test_effect_receipt_write_failure_withholds_output_and_blocks_replay(tmp_path):
    store, approvals, launcher, _gateway = _approval_fixture(tmp_path)

    class ReceiptUnavailable:
        def uses_event_store(self, event_store):
            return approvals.uses_event_store(event_store)

        def __getattr__(self, name):
            return getattr(approvals, name)

        def record_effect_receipt(self, *args, **kwargs):
            raise OSError("receipt persistence unavailable")

    gateway = ToolGateway(
        run_id="run-1",
        workspace=tmp_path,
        event_store=store,
        policy_manifest=_manifest(),
        attempt_authority=_ToolAuthority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=launcher,
        approval_service=ReceiptUnavailable(),
        approval_requester=lambda request: "requester-1",
        approval_isolation_profile_hash=_hash("measured-readonly-profile"),
        approval_now=lambda: _NOW,
    )
    waiting = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-1", attempt_id="attempt-1", generation=2,
    ))
    issued = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact read")
    replay_request = _tool_request(
        tmp_path, request_id="tool-request-2", attempt_id="attempt-2", generation=3,
    )

    result = gateway.execute(replay_request, approval_grant_id=issued.approval_grant_id)

    assert result.outcome == "execution_unknown"
    assert result.stdout == b""
    assert result.termination_confirmed is True
    assert store.read_stream("budget", "run-1")[-1].event_type == "BudgetReserved"
    assert store.read_stream("security", "run-1")[-1].event_type == "ToolExecutionOutcomeUnknown"
    try:
        gateway.execute(replay_request, approval_grant_id=issued.approval_grant_id)
    except Exception as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("a request without a durable receipt must not replay")
    assert len(launcher.calls) == 1


def launcher_not_started(store):
    return not any(
        event.event_type == "ToolExecutionStarted"
        for event in store.read_stream("security", "run-1")
    )


def test_approval_tool_gateway_uses_run_budget_currency_for_zero_cost_effect(tmp_path):
    store, approvals, launcher, gateway = _approval_fixture(tmp_path, currency="CAD")

    waiting = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-cad-1", attempt_id="attempt-1", generation=2,
    ))
    issued = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact read")

    completed = gateway.execute(_tool_request(
        tmp_path, request_id="tool-request-cad-2", attempt_id="attempt-2", generation=3,
    ), approval_grant_id=issued.approval_grant_id)

    assert completed.outcome == "completed"
    assert launcher.calls
    budget_events = store.read_stream("budget", "run-1")
    reservation = next(event for event in budget_events if event.event_type == "BudgetReserved")
    assert reservation.payload["currency"] == "CAD"
