from datetime import datetime, timezone
import hashlib
import multiprocessing

import pytest

from orchestrator.approvals import (
    ApprovalInvalid,
    ApprovalPolicyState,
    ApprovalPrincipal,
    ApprovalService,
    ExecutionAttempt,
)
from orchestrator.budget import BudgetLedger, RunLimit
from orchestrator.isolation import SandboxResult, SandboxTerminationReceipt
from orchestrator.persistence import SQLiteEventStore
from orchestrator.security.policy import PolicyAuthority, PolicyManifest
from orchestrator.tools import PolicyState, ToolGateway, ToolRequest, ToolRequestAlreadyUsed
from tests.support.process_crash import block_at_crash_point, kill_at_crash_point


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


def _approval_fixture(
    tmp_path, *, tool_authority=None, service_adapter=None, currency="USD",
    approval_attempt_authority=None, launcher=None,
):
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
        attempt_authority=approval_attempt_authority or _ApprovalAuthority(),
        policy_state=lambda request: ApprovalPolicyState(1, 4),
        now=lambda: _NOW,
    )
    launcher = launcher if launcher is not None else _Launcher()
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


class _MarkerLauncher(_Launcher):
    def __init__(self, marker):
        super().__init__()
        self.marker = marker

    def launch(self, *args, **kwargs):
        # Exclusive creation detects launch across process/service lifetimes.
        with self.marker.open("x") as output:
            output.write("launch called\n")
        return super().launch(*args, **kwargs)


def _execute_approval_until_crash(tmp_path, grant_id, marker, pipe, point):
    store, approvals, _launcher, gateway = _approval_fixture(
        tmp_path, launcher=_MarkerLauncher(marker),
    )
    try:
        if point == "approval.bound-before-consume":
            real_bind = approvals.bind_to_attempt

            def bind_then_block(*args, **kwargs):
                result = real_bind(*args, **kwargs)
                block_at_crash_point(pipe, point)
                return result

            approvals.bind_to_attempt = bind_then_block
        elif point == "approval.consume-before-commit":
            real_append = store.append_checked
            in_budget_append = False

            def trace_commit(statement):
                if in_budget_append and statement.strip().upper() == "COMMIT":
                    block_at_crash_point(pipe, point)

            def append_and_arm(stream_type, stream_id, key, decide):
                nonlocal in_budget_append
                in_budget_append = stream_type == "budget"
                try:
                    result = real_append(stream_type, stream_id, key, decide)
                finally:
                    in_budget_append = False
                if stream_type == "security" and key == f"approval-bind:{grant_id}":
                    # The binding COMMIT has returned. Only the later budget
                    # transaction's COMMIT can reach this crash barrier.
                    store._connection.set_trace_callback(trace_commit)
                return result

            store.append_checked = append_and_arm
        elif point == "approval.consume-after-commit":
            real_consume = approvals.consume_and_intend

            def consume_then_block(*args, **kwargs):
                result = real_consume(*args, **kwargs)
                block_at_crash_point(pipe, point)
                return result

            approvals.consume_and_intend = consume_then_block
        else:
            raise AssertionError("unexpected approval crash point")

        gateway.execute(_tool_request(
            tmp_path, request_id="tool-request-2", attempt_id="attempt-2", generation=3,
        ), approval_grant_id=grant_id)
        raise AssertionError("approval execution returned without reaching crash point")
    finally:
        store.close()
        pipe.close()


class _NewerApprovalAuthority:
    def is_current(self, attempt):
        return attempt.attempt_id == "attempt-3" and attempt.fencing_generation == 4


@pytest.mark.parametrize("point, consumed", [
    pytest.param("approval.bound-before-consume", False,
                 id="sigkill_after_binding_before_consume_leaves_grant_unusable"),
    pytest.param("approval.consume-before-commit", False,
                 id="sigkill_consume_before_commit_leaves_grant_unusable"),
    pytest.param("approval.consume-after-commit", True,
                 id="sigkill_consume_after_commit_blocks_replay"),
])
def test_approval_sigkill_binding_and_consumption(tmp_path, point, consumed):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("hard process termination requires the fork start method")
    # Each parameter gets an independent DB and grant, with the exact successful
    # execution scope. A bound but unconsumed grant cannot be revived by retry.
    marker = tmp_path / "launcher-called"
    store, approvals, launcher, gateway = _approval_fixture(tmp_path)
    try:
        waiting = gateway.execute(_tool_request(
            tmp_path, request_id="tool-request-1", attempt_id="attempt-1", generation=2,
        ))
        assert waiting.outcome == "awaiting_approval"
        assert waiting.approval_request_id is not None
        issued = approvals.approve(
            waiting.approval_request_id, "human-1", reason="approve exact read",
        )
        assert launcher.calls == []
    finally:
        store.close()

    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe()
    child = context.Process(target=_execute_approval_until_crash, args=(
        tmp_path, issued.approval_grant_id, marker, child_pipe, point,
    ))
    child.start()
    child_pipe.close()
    try:
        kill_at_crash_point(child, parent_pipe, expected_point=point, timeout_seconds=10.0)
    finally:
        parent_pipe.close()
        child.close()

    # Recovery uses a new connection and real services, never inherited state.
    store, approvals, launcher, gateway = _approval_fixture(
        tmp_path, launcher=_MarkerLauncher(marker),
    )
    try:
        security = store.read_stream("security", "run-1")
        budget = store.read_stream("budget", "run-1")
        bound = [event for event in security if event.event_type == "ApprovalGrantBound"]
        assert len(bound) == 1
        assert bound[0].payload["approval_grant_id"] == issued.approval_grant_id
        assert bound[0].payload["attempt_id"] == "attempt-2"
        assert bound[0].payload["fencing_generation"] == 3
        assert [event.event_type for event in budget] == (
            ["EffectIntentRecorded", "ApprovalGrantConsumed", "BudgetReserved"]
            if consumed else []
        )
        assert launcher_not_started(store)
        assert not marker.exists()

        if not consumed:
            with pytest.raises(ApprovalInvalid, match="already bound"):
                approvals.bind_to_attempt(issued.approval_grant_id, ExecutionAttempt(
                    run_id="run-1", node_id="node-1", attempt_id="attempt-2",
                    fencing_generation=3,
                    isolation_profile_hash=_hash("measured-readonly-profile"),
                ))

        with pytest.raises(ToolRequestAlreadyUsed):
            gateway.execute(_tool_request(
                tmp_path, request_id="tool-request-2", attempt_id="attempt-2", generation=3,
            ), approval_grant_id=issued.approval_grant_id)
        assert store.read_stream("security", "run-1") == security
        assert store.read_stream("budget", "run-1") == budget
        assert launcher.calls == []
        assert not marker.exists()
    finally:
        store.close()

    if not consumed:
        store, approvals, _launcher, _gateway = _approval_fixture(
            tmp_path, approval_attempt_authority=_NewerApprovalAuthority(),
        )
        try:
            with pytest.raises(ApprovalInvalid, match="already bound"):
                approvals.bind_to_attempt(issued.approval_grant_id, ExecutionAttempt(
                    run_id="run-1", node_id="node-1", attempt_id="attempt-3",
                    fencing_generation=4,
                    isolation_profile_hash=_hash("measured-readonly-profile"),
                ))
            assert store.read_stream("security", "run-1") == security
            assert store.read_stream("budget", "run-1") == budget
            assert not marker.exists()
        finally:
            store.close()


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
