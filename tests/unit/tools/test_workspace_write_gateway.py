from __future__ import annotations

from pathlib import Path
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from orchestrator.isolation import (
    OverlayCandidateResult,
    SandboxLimits,
    SandboxResult,
    SandboxTerminationReceipt,
    WorkspacePublishError,
    WorkspaceLeaseBusy,
    acquire_workspace_write_lease,
    export_overlay_diff,
    snapshot_workspace,
)
from orchestrator.approvals import (
    ApprovalPolicyState,
    ApprovalPrincipal,
    ApprovalService,
    ExecutionAttempt,
)
from orchestrator.budget import BudgetLedger, RunLimit
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.security.policy import PolicyAuthority, PolicyManifest
from orchestrator.tools import (
    PolicyState,
    ToolAuditUnavailable,
    ToolRequestAlreadyUsed,
    WorkspaceWriteGateway,
    WorkspaceWriteRequest,
)
from orchestrator.workspace_identity import workspace_identity_hash


class _Authority:
    def __init__(self, valid: bool = True) -> None:
        self.valid = valid

    def is_current(self, request) -> bool:
        return self.valid


class _ApprovalAuthority:
    def is_current(self, attempt: ExecutionAttempt) -> bool:
        return attempt.attempt_id == "attempt-2" and attempt.fencing_generation == 3


class _LoseFreshAttemptAtStart:
    def __init__(self) -> None:
        self.fresh_checks = 0

    def is_current(self, request) -> bool:
        if request.attempt_id != "attempt-2":
            return True
        self.fresh_checks += 1
        return self.fresh_checks < 4


class _CandidateSession:
    def __init__(self, result: OverlayCandidateResult) -> None:
        self.result = result
        self.closed = False
        self.cancelled = False

    def wait(self) -> OverlayCandidateResult:
        return self.result

    def cancel(self) -> bool:
        self.cancelled = True
        return True

    def close(self) -> None:
        self.closed = True


class _CandidateLauncher:
    def __init__(self, session: _CandidateSession) -> None:
        self.session = session
        self.calls = []

    def launch(self, workspace, command, *, limits, expected_workspace_identity_hash):
        self.calls.append((workspace, command, limits, expected_workspace_identity_hash))
        return self.session


def _manifest(*, allow: bool = True, approval: bool = False) -> PolicyManifest:
    return PolicyManifest(
        authorities=(PolicyAuthority(
            source="system",
            max_permission="workspace-write",
            allowed_actions=("reversible_workspace_write",) if allow else ("safe_read",),
            allowed_tools=("workspace.write-candidate",) if allow else ("not-this-tool",),
            approval_actions=("reversible_workspace_write",) if approval else (),
        ),)
    )


def _request(
    workspace: Path,
    request_id: str = "write-1",
    *,
    attempt_id: str = "attempt-1",
    generation: int = 2,
) -> WorkspaceWriteRequest:
    return WorkspaceWriteRequest(
        request_id=request_id,
        run_id="run-1",
        node_id="node-1",
        attempt_id=attempt_id,
        fencing_generation=generation,
        role="worker",
        causation_id="attempt-accepted-event",
        workspace=str(workspace),
        command=("/usr/bin/touch", "private-command-argument"),
    )


def _fixture(
    tmp_path: Path,
    *,
    allow: bool = True,
    authority: _Authority | None = None,
    approval: bool = False,
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "existing.txt").write_text("old\n", encoding="utf-8")
    candidate_root = tmp_path / "candidate"
    candidate_root.mkdir()
    lower = candidate_root / "lower"
    snapshot_workspace(workspace, lower)
    upper = candidate_root / "upper"
    upper.mkdir()
    (upper / "new.txt").write_text("private candidate data\n", encoding="utf-8")
    diff = export_overlay_diff(lower, upper, candidate_root / "validated")
    sandbox = SandboxResult(
        unit_name="maestro-candidate-" + "4" * 32 + ".scope",
        returncode=0,
        stdout=b"private output",
        stderr=b"",
        elapsed_seconds=0.01,
        termination_receipt=SandboxTerminationReceipt(
            unit_name="maestro-candidate-" + "4" * 32 + ".scope",
            control_group="/user.slice/user-1000.slice/user@1000.service/app.slice/maestro-candidate-" + "4" * 32 + ".scope",
            active_state="inactive",
            cgroup_empty=True,
        ),
        cancelled=False,
        timed_out=False,
        output_limited=False,
    )
    session = _CandidateSession(OverlayCandidateResult(sandbox, lower, diff))
    launcher = _CandidateLauncher(session)
    events = SQLiteEventStore(tmp_path / "events.db")
    approval_service = None
    now = datetime(2026, 9, 26, 18, 0, tzinfo=timezone.utc)
    if approval:
        ledger = BudgetLedger(
            events,
            run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
        )
        approval_service = ApprovalService(
            event_store=events,
            budget_ledger=ledger,
            authenticator=lambda credential: ApprovalPrincipal(
                principal_id=credential,
                principal_type="human",
                permissions=frozenset({"approval:approve"}),
            ),
            attempt_authority=_ApprovalAuthority(),
            policy_state=lambda request: ApprovalPolicyState(1, 4),
            now=lambda: now,
        )
    lease_root = tmp_path / "lease"
    lease_root.mkdir(mode=0o700)
    journal_root = tmp_path / "journal"
    journal_root.mkdir(mode=0o700)
    gateway = WorkspaceWriteGateway(
        run_id="run-1",
        workspace=workspace,
        event_store=events,
        policy_manifest=_manifest(allow=allow, approval=approval),
        attempt_authority=authority or _Authority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=launcher,
        lease_root=lease_root,
        journal_root=journal_root,
        limits=SandboxLimits(),
        isolation_profile_hash="sha256:" + "a" * 64,
        approval_service=approval_service,
        approval_requester=(lambda request: "requester-1") if approval else None,
        approval_now=lambda: now,
    )
    return workspace, events, launcher, session, lease_root, journal_root, gateway, approval_service


def test_allowed_write_is_attempt_bound_audited_and_published(tmp_path: Path) -> None:
    workspace, events, launcher, session, _leases, journal, gateway, _approvals = _fixture(tmp_path)

    result = gateway.execute(_request(workspace).model_dump())

    assert result.outcome == "completed"
    assert (workspace / "new.txt").read_text(encoding="utf-8") == "private candidate data\n"
    assert session.closed
    assert len(launcher.calls) == 1
    assert result.stdout == b"private output"
    event_types = [event.event_type for event in events.read_stream("security", "run-1")]
    assert event_types == [
        "ToolRequestReceived",
        "PolicyDecision",
        "CapabilityGrant",
        "ToolCapabilityConsumed",
        "ToolExecutionStarted",
        "WorkspacePublicationIntent",
        "WorkspacePublicationCompleted",
        "ToolExecutionCompleted",
    ]
    audit = repr([event.payload for event in events.read_stream("security", "run-1")])
    assert "private candidate data" not in audit
    assert "private-command-argument" not in audit
    assert str(workspace) not in audit
    assert list(journal.iterdir()) == []


def test_denied_or_stale_attempt_never_launches_or_mutates_workspace(tmp_path: Path) -> None:
    workspace, events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(
        tmp_path, allow=False
    )

    result = gateway.execute(_request(workspace))

    assert result.outcome == "denied"
    assert launcher.calls == []
    assert not (workspace / "new.txt").exists()


def test_publication_intent_audit_failure_prevents_any_workspace_write(tmp_path: Path) -> None:
    workspace, events, launcher, session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)
    original_append = events.append_checked

    def fail_publication_intent(stream_type, stream_id, idempotency_key, decide):
        if idempotency_key.startswith("workspace-publication-intent:"):
            raise OSError("audit store unavailable")
        return original_append(stream_type, stream_id, idempotency_key, decide)

    events.append_checked = fail_publication_intent

    with pytest.raises(ToolAuditUnavailable):
        gateway.execute(_request(workspace))

    assert not (workspace / "new.txt").exists()
    assert session.closed
    assert len(launcher.calls) == 1


def test_prepared_publication_error_records_confirmed_rollback(tmp_path: Path, monkeypatch) -> None:
    workspace, events, _launcher, _session, _leases, journal, gateway, _approvals = _fixture(tmp_path)

    from orchestrator.isolation import workspace_publish

    def fail_after_first_entry(_transaction_id, _path):
        raise WorkspacePublishError("injected publication interruption")

    monkeypatch.setattr(workspace_publish, "_after_publish_entry", fail_after_first_entry)

    result = gateway.execute(_request(workspace))

    assert result.outcome == "failed"
    assert not (workspace / "new.txt").exists()
    assert list(journal.iterdir()) == []
    run_events = events.read_stream("security", "run-1")
    global_events = events.read_stream("workspace_publications", gateway._publication_stream_id)
    assert any(event.event_type == "WorkspacePublicationAborted" for event in run_events)
    assert any(event.event_type == "WorkspacePublicationAborted" for event in global_events)


def test_mid_publication_conflict_keeps_unconfirmed_intent_unresolved(
    tmp_path: Path, monkeypatch
) -> None:
    workspace, events, launcher, session, leases, journal, gateway, _approvals = _fixture(tmp_path)
    lower = session.result.lower_root
    upper = tmp_path / "two-entry-upper"
    upper.mkdir()
    (upper / "new.txt").write_text("private candidate data\n", encoding="utf-8")
    (upper / "second.txt").write_text("another candidate\n", encoding="utf-8")
    diff = export_overlay_diff(lower, upper, tmp_path / "two-entry-candidate")
    session.result = OverlayCandidateResult(session.result.execution, lower, diff)

    from orchestrator.isolation import workspace_publish

    original_hook = workspace_publish._after_publish_entry

    def create_external_conflict(transaction_id, path):
        original_hook(transaction_id, path)
        if path == "new.txt":
            (workspace / "second.txt").write_text("external edit\n", encoding="utf-8")

    monkeypatch.setattr(workspace_publish, "_after_publish_entry", create_external_conflict)

    result = gateway.execute(_request(workspace))

    assert result.outcome == "execution_unknown"
    assert (workspace / "new.txt").read_text(encoding="utf-8") == "private candidate data\n"
    assert (workspace / "second.txt").read_text(encoding="utf-8") == "external edit\n"
    assert any(path.name.startswith("publish-") for path in journal.iterdir())
    run_events = events.read_stream("security", "run-1")
    global_events = events.read_stream("workspace_publications", gateway._publication_stream_id)
    assert not any(event.event_type == "WorkspacePublicationAborted" for event in run_events)
    assert [event.event_type for event in global_events] == ["WorkspacePublicationIntent"]

    blocked = gateway.execute(_request(workspace, "write-2"))
    assert blocked.outcome == "denied"
    assert len(launcher.calls) == 1


def test_prepared_journal_creation_error_records_not_applied(tmp_path: Path, monkeypatch) -> None:
    workspace, events, _launcher, _session, _leases, journal, gateway, _approvals = _fixture(tmp_path)

    from orchestrator.isolation import workspace_publish

    def fail_before_prepared(*_args, **_kwargs):
        raise WorkspacePublishError("injected pre-journal failure")

    monkeypatch.setattr(workspace_publish, "_capture_originals", fail_before_prepared)

    result = gateway.execute(_request(workspace))

    assert result.outcome == "failed"
    assert not (workspace / "new.txt").exists()
    assert list(journal.iterdir()) == []
    assert events.read_stream("security", "run-1")[-2].event_type == "WorkspacePublicationAborted"


def test_committed_publish_cleanup_error_keeps_success_outcome(tmp_path: Path, monkeypatch) -> None:
    workspace, events, _launcher, _session, _leases, journal, gateway, _approvals = _fixture(tmp_path)

    from orchestrator.isolation import workspace_publish

    original_remove = workspace_publish._remove_transaction

    def remove_then_fail(directory_fd, transaction_name):
        original_remove(directory_fd, transaction_name)
        raise WorkspacePublishError("injected post-commit cleanup failure")

    monkeypatch.setattr(workspace_publish, "_remove_transaction", remove_then_fail)

    result = gateway.execute(_request(workspace))

    assert result.outcome == "completed"
    assert (workspace / "new.txt").read_text(encoding="utf-8") == "private candidate data\n"
    assert events.read_stream("security", "run-1")[-1].event_type == "ToolExecutionCompleted"
    assert list(journal.iterdir()) == []


@pytest.mark.parametrize("phase", ["completed", "aborted"])
def test_missing_run_receipt_keeps_workspace_intent_unresolved(
    tmp_path: Path, monkeypatch, phase: str
) -> None:
    authority = _Authority()
    workspace, events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(
        tmp_path, authority=authority
    )
    original_append = events.append_checked
    failed = False

    def fail_run_receipt(stream_type, stream_id, idempotency_key, decide):
        nonlocal failed
        expected_key = "workspace-publication-receipt:" if phase == "completed" else "workspace-publication-aborted:"
        if not failed and stream_type == "security" and idempotency_key.startswith(expected_key):
            failed = True
            raise OSError("injected Run audit failure")
        return original_append(stream_type, stream_id, idempotency_key, decide)

    events.append_checked = fail_run_receipt
    if phase == "aborted":
        from orchestrator.isolation import workspace_publish

        original_hook = workspace_publish._after_publish_entry

        def revoke_before_commit(transaction_id, path):
            authority.valid = False
            original_hook(transaction_id, path)

        monkeypatch.setattr(workspace_publish, "_after_publish_entry", revoke_before_commit)

    with pytest.raises(ToolAuditUnavailable):
        gateway.execute(_request(workspace))

    global_events = events.read_stream("workspace_publications", gateway._publication_stream_id)
    assert [event.event_type for event in global_events] == ["WorkspacePublicationIntent"]
    authority.valid = True
    blocked = gateway.execute(_request(workspace, "write-2"))
    assert blocked.outcome == "denied"
    assert len(launcher.calls) == 1


def test_approval_requires_a_fresh_attempt_and_records_effect_receipt(tmp_path: Path) -> None:
    workspace, events, launcher, session, _leases, journal, gateway, approvals = _fixture(
        tmp_path, approval=True
    )

    waiting = gateway.execute(_request(workspace))

    assert waiting.outcome == "awaiting_approval"
    assert waiting.approval_request_id is not None
    assert launcher.calls == []
    grant = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact workspace change")

    completed = gateway.execute(
        _request(workspace, "write-2", attempt_id="attempt-2", generation=3),
        approval_grant_id=grant.approval_grant_id,
    )

    assert completed.outcome == "completed"
    assert (workspace / "new.txt").read_text(encoding="utf-8") == "private candidate data\n"
    assert session.closed
    assert len(launcher.calls) == 1
    budget_events = events.read_stream("budget", "run-1")
    assert [event.event_type for event in budget_events] == [
        "EffectIntentRecorded", "ApprovalGrantConsumed", "BudgetReserved", "EffectReceiptRecorded",
    ]
    assert budget_events[-1].payload["outcome"] == "applied"
    assert list(journal.iterdir()) == []


def test_approval_grant_cannot_change_the_command_scope(tmp_path: Path) -> None:
    workspace, _events, launcher, _session, _leases, _journal, gateway, approvals = _fixture(
        tmp_path, approval=True
    )
    waiting = gateway.execute(_request(workspace))
    grant = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact scope")

    changed = _request(
        workspace,
        "write-changed-command",
        attempt_id="attempt-2",
        generation=3,
    ).model_copy(update={"command": ("/usr/bin/id",)})
    result = gateway.execute(changed, approval_grant_id=grant.approval_grant_id)

    assert result.outcome == "denied"
    assert launcher.calls == []


def test_revoked_approved_attempt_gets_not_applied_receipt_without_launch(tmp_path: Path) -> None:
    authority = _LoseFreshAttemptAtStart()
    workspace, events, launcher, _session, _leases, _journal, gateway, approvals = _fixture(
        tmp_path, approval=True, authority=authority
    )
    waiting = gateway.execute(_request(workspace))
    grant = approvals.approve(waiting.approval_request_id, "human-1", reason="approve exact scope")

    result = gateway.execute(
        _request(workspace, "write-2", attempt_id="attempt-2", generation=3),
        approval_grant_id=grant.approval_grant_id,
    )

    assert result.outcome == "denied"
    assert launcher.calls == []
    budget_events = events.read_stream("budget", "run-1")
    assert budget_events[-1].event_type == "EffectReceiptRecorded"
    assert budget_events[-1].payload["outcome"] == "not_applied"
    assert events.read_stream("security", "run-1")[-1].event_type == "ToolExecutionBlocked"


def test_approval_required_without_service_never_launches(tmp_path: Path) -> None:
    workspace, events, launcher, _session, leases, journal, _gateway, _approvals = _fixture(tmp_path)
    approval_gateway = WorkspaceWriteGateway(
        run_id="run-1",
        workspace=workspace,
        event_store=events,
        policy_manifest=_manifest(approval=True),
        attempt_authority=_Authority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=launcher,
        lease_root=leases,
        journal_root=journal,
        limits=SandboxLimits(),
        isolation_profile_hash="sha256:" + "a" * 64,
    )

    result = approval_gateway.execute(_request(workspace))

    assert result.outcome == "awaiting_approval"
    assert launcher.calls == []
    assert not (workspace / "new.txt").exists()


def test_revocation_after_candidate_execution_rolls_back_publication(tmp_path: Path) -> None:
    authority = _Authority()
    workspace, events, _launcher, _session, _leases, journal, gateway, _approvals = _fixture(
        tmp_path, authority=authority
    )

    from orchestrator.isolation import workspace_publish

    original_hook = workspace_publish._after_publish_entry

    def revoke_after_first_write(_transaction_id, _path):
        authority.valid = False
        original_hook(_transaction_id, _path)

    workspace_publish._after_publish_entry = revoke_after_first_write
    try:
        result = gateway.execute(_request(workspace))
    finally:
        workspace_publish._after_publish_entry = original_hook

    assert result.outcome == "authority_lost"
    assert not (workspace / "new.txt").exists()
    assert list(journal.iterdir()) == []
    event_types = [event.event_type for event in events.read_stream("security", "run-1")]
    assert "WorkspacePublicationIntent" in event_types
    assert "WorkspacePublicationCompleted" not in event_types
    assert "WorkspacePublicationAborted" in event_types
    assert event_types[-1] == "ToolExecutionCancelled"


def test_stale_candidate_conflict_is_audited_as_not_applied(tmp_path: Path) -> None:
    workspace, events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)
    (workspace / "new.txt").write_text("concurrent edit\n", encoding="utf-8")

    result = gateway.execute(_request(workspace))

    assert result.outcome == "failed"
    assert (workspace / "new.txt").read_text(encoding="utf-8") == "concurrent edit\n"
    assert len(launcher.calls) == 1
    event_types = [event.event_type for event in events.read_stream("security", "run-1")]
    assert "WorkspacePublicationAborted" in event_types
    assert "WorkspacePublicationCompleted" not in event_types


def test_stale_attempt_is_audited_and_never_launches(tmp_path: Path) -> None:
    workspace, events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(
        tmp_path, authority=_Authority(valid=False)
    )

    with pytest.raises(PermissionError, match="attempt is not current"):
        gateway.execute(_request(workspace))

    assert launcher.calls == []
    assert not (workspace / "new.txt").exists()
    assert events.read_stream("security", "run-1")[-1].event_type == "ToolExecutionBlocked"


def test_workspace_lease_contention_prevents_second_publication(tmp_path: Path) -> None:
    workspace, events, launcher, _session, lease_root, journal, gateway, _approvals = _fixture(tmp_path)

    with acquire_workspace_write_lease(workspace, lease_root):
        result = gateway.execute(_request(workspace))

    assert result.outcome == "denied"
    assert launcher.calls == []
    assert not (workspace / "new.txt").exists()
    assert list(journal.iterdir()) == []
    assert events.read_stream("security", "run-1")[-1].event_type == "ToolExecutionBlocked"


def test_workspace_lease_is_held_during_candidate_execution(tmp_path: Path) -> None:
    workspace, _events, _launcher, session, lease_root, _journal, gateway, _approvals = _fixture(tmp_path)
    original_wait = session.wait
    lock_was_held = False

    def check_exclusive_lease_during_execution():
        nonlocal lock_was_held
        try:
            competing = acquire_workspace_write_lease(workspace, lease_root)
        except WorkspaceLeaseBusy:
            lock_was_held = True
        else:
            competing.close()
        return original_wait()

    session.wait = check_exclusive_lease_during_execution

    result = gateway.execute(_request(workspace))

    assert result.outcome == "completed"
    assert lock_was_held


def test_completed_request_id_cannot_replay_candidate_or_publication(tmp_path: Path) -> None:
    workspace, _events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)

    first = gateway.execute(_request(workspace))
    assert first.outcome == "completed"
    with pytest.raises(ToolRequestAlreadyUsed, match="write request id already exists"):
        gateway.execute(_request(workspace))

    assert len(launcher.calls) == 1
    assert (workspace / "new.txt").read_text(encoding="utf-8") == "private candidate data\n"


def test_unresolved_durable_intent_blocks_new_write_attempt(tmp_path: Path) -> None:
    workspace, events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)
    from orchestrator.persistence.events import EventDraft

    events.append_checked(
        "security",
        "run-1",
        "seed-unresolved-publication",
        lambda _items, _version: [EventDraft(
            "WorkspacePublicationIntent",
            {"request_id": "old-write", "transaction_id": "a" * 32},
            run_id="run-1",
            node_id="node-1",
            attempt_id="attempt-1",
            fencing_generation=2,
            causation_id="attempt-accepted-event",
        )],
    )

    result = gateway.execute(_request(workspace, "new-write"))

    assert result.outcome == "denied"
    assert launcher.calls == []
    assert not (workspace / "new.txt").exists()
    assert events.read_stream("security", "run-1")[-1].payload["reason"] == "prior_publication_outcome_unresolved"


def test_unresolved_intent_from_another_run_blocks_shared_workspace(tmp_path: Path) -> None:
    workspace, events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)
    from orchestrator.persistence.events import EventDraft

    publication_stream = "workspace:" + workspace_identity_hash(workspace).removeprefix("sha256:")
    events.append_checked(
        "workspace_publications",
        publication_stream,
        "seed-other-run-unresolved-publication",
        lambda _items, _version: [EventDraft(
            "WorkspacePublicationIntent",
            {"request_id": "old-write", "transaction_id": "b" * 32},
            run_id="different-run",
            node_id="node-9",
            attempt_id="attempt-9",
            fencing_generation=9,
            causation_id="attempt-accepted-other-run",
        )],
    )

    result = gateway.execute(_request(workspace, "new-write"))

    assert result.outcome == "denied"
    assert launcher.calls == []
    assert not (workspace / "new.txt").exists()


def test_publication_receipt_failure_leaves_intent_and_blocks_a_retry(tmp_path: Path) -> None:
    workspace, events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)
    original_append = events.append_checked

    def fail_publication_receipt(stream_type, stream_id, idempotency_key, decide):
        if idempotency_key.startswith("workspace-publication-receipt:"):
            raise OSError("receipt store unavailable")
        return original_append(stream_type, stream_id, idempotency_key, decide)

    events.append_checked = fail_publication_receipt
    with pytest.raises(ToolAuditUnavailable):
        gateway.execute(_request(workspace, "write-uncertain"))

    assert (workspace / "new.txt").exists()
    assert "WorkspacePublicationIntent" in [
        event.event_type for event in events.read_stream("security", "run-1")
    ]
    # A later attempt over the same durable event stream cannot replay the
    # unresolved effect.
    retry = gateway.execute(_request(workspace, "write-after-restart"))
    assert retry.outcome == "denied"
    assert len(launcher.calls) == 1


def test_failed_candidate_never_publishes_even_when_diff_is_present(tmp_path: Path) -> None:
    workspace, events, _launcher, session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)
    session.result = replace(
        session.result,
        execution=replace(session.result.execution, returncode=1),
        candidate_error="execution_failed",
    )

    result = gateway.execute(_request(workspace))

    assert result.outcome == "failed"
    assert not (workspace / "new.txt").exists()
    assert events.read_stream("security", "run-1")[-1].event_type == "ToolExecutionFailed"


def test_candidate_wait_failure_is_unknown_and_never_publishes(tmp_path: Path) -> None:
    workspace, events, _launcher, session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)

    def fail_wait():
        raise RuntimeError("lost candidate wait channel")

    session.wait = fail_wait

    result = gateway.execute(_request(workspace))

    assert result.outcome == "execution_unknown"
    assert not (workspace / "new.txt").exists()
    assert session.closed
    assert events.read_stream("security", "run-1")[-1].event_type == "ToolExecutionOutcomeUnknown"


def test_candidate_transport_start_failure_is_unknown_and_never_publishes(tmp_path: Path) -> None:
    workspace, events, launcher, _session, _leases, _journal, gateway, _approvals = _fixture(tmp_path)

    def fail_launch(*_args, **_kwargs):
        raise RuntimeError("uncertain transient unit start")

    launcher.launch = fail_launch

    result = gateway.execute(_request(workspace))

    assert result.outcome == "execution_unknown"
    assert not (workspace / "new.txt").exists()
    assert events.read_stream("security", "run-1")[-1].event_type == "ToolExecutionOutcomeUnknown"
