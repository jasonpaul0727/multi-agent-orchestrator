from __future__ import annotations

import threading
import hashlib
from pathlib import Path

import pytest

from orchestrator.isolation import SandboxLimits, SandboxResult
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.security.policy import PolicyAuthority, PolicyManifest
from orchestrator.tools import (
    READ_ONLY_COMMAND_TOOL_ID,
    PolicyState,
    ToolAuditUnavailable,
    ToolGateway,
    ToolRequest,
    ToolRequestAlreadyUsed,
)


def _manifest(*, allow: bool = True, approval: bool = False) -> PolicyManifest:
    return PolicyManifest(
        authorities=(
            PolicyAuthority(
                source="system",
                max_permission="read-only",
                allowed_actions=("safe_read",),
                allowed_tools=(READ_ONLY_COMMAND_TOOL_ID,) if allow else ("different.tool",),
                approval_actions=("safe_read",) if approval else (),
            ),
        )
    )


def _request(workspace: Path, request_id: str = "tool-1") -> ToolRequest:
    return ToolRequest(
        request_id=request_id,
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=2,
        role="worker",
        causation_id="attempt-accepted-event",
        workspace=str(workspace),
        command=("/usr/bin/printf", "private output"),
    )


class _Authority:
    def __init__(self, valid: bool = True) -> None:
        self.valid = valid
        self.calls = 0

    def is_current(self, request: ToolRequest) -> bool:
        self.calls += 1
        return self.valid


class _Session:
    def __init__(self) -> None:
        self.cancelled = False

    def wait(self) -> SandboxResult:
        return SandboxResult(
            unit_name="maestro-test.service",
            returncode=0,
            stdout=b"private output",
            stderr=b"",
            elapsed_seconds=0.01,
            termination_confirmed=True,
            cancelled=False,
            timed_out=False,
            output_limited=False,
        )

    def cancel(self) -> bool:
        self.cancelled = True
        return True


class _Launcher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[str, ...], SandboxLimits]] = []
        self.session = _Session()

    def launch(self, workspace: str, command: tuple[str, ...], *, limits: SandboxLimits) -> _Session:
        self.calls.append((workspace, command, limits))
        return self.session


def _gateway(
    store: SQLiteEventStore,
    launcher: _Launcher,
    *,
    allow: bool = True,
    approval: bool = False,
    authority: _Authority | None = None,
    policy_state=None,
) -> ToolGateway:
    return ToolGateway(
        run_id="run-1",
        event_store=store,
        policy_manifest=_manifest(allow=allow, approval=approval),
        attempt_authority=authority or _Authority(),
        policy_state=policy_state or (lambda request: PolicyState(1, 4)),
        launcher=launcher,
        monitor_interval_seconds=0.01,
    )


def test_allowed_read_only_request_is_audited_without_raw_command_or_output(tmp_path: Path) -> None:
    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = _Launcher()
    result = _gateway(store, launcher).execute(_request(tmp_path))

    assert result.outcome == "completed"
    assert result.stdout == b"private output"
    assert result.stdout_sha256 == "sha256:" + hashlib.sha256(b"private output").hexdigest()
    events = store.read_stream("security", "run-1")
    event_types = [event.event_type for event in events]
    assert event_types == [
        "ToolRequestReceived",
        "PolicyDecision",
        "CapabilityGrant",
        "ToolCapabilityConsumed",
        "ToolExecutionStarted",
        "ToolExecutionCompleted",
    ]
    encoded = repr([event.payload for event in events])
    assert "private output" not in encoded
    assert "/usr/bin/printf" not in encoded
    assert str(tmp_path) not in encoded
    terminal = events[-1].payload
    assert terminal["stdout_sha256"] == result.stdout_sha256
    assert terminal["stdout_bytes"] == len(result.stdout)
    assert terminal["termination_confirmed"] is True


def test_policy_deny_never_launches_and_is_audited(tmp_path: Path) -> None:
    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = _Launcher()

    result = _gateway(store, launcher, allow=False).execute(_request(tmp_path))

    assert result.outcome == "denied"
    assert launcher.calls == []
    events = store.read_stream("security", "run-1")
    assert [event.event_type for event in events] == ["ToolRequestReceived", "PolicyDecision"]
    assert events[-1].payload["outcome"] == "deny"


def test_approval_required_is_not_treated_as_permission_to_execute(tmp_path: Path) -> None:
    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = _Launcher()

    result = _gateway(store, launcher, approval=True).execute(_request(tmp_path))

    assert result.outcome == "awaiting_approval"
    assert launcher.calls == []
    events = store.read_stream("security", "run-1")
    assert [event.event_type for event in events] == [
        "ToolRequestReceived",
        "PolicyDecision",
        "ApprovalRequested",
    ]


def test_stale_attempt_fails_closed_before_grant_or_launch(tmp_path: Path) -> None:
    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = _Launcher()
    authority = _Authority(valid=False)

    with pytest.raises(PermissionError, match="not current"):
        _gateway(store, launcher, authority=authority).execute(_request(tmp_path))

    assert launcher.calls == []
    events = store.read_stream("security", "run-1")
    assert [event.event_type for event in events] == ["ToolExecutionBlocked"]
    assert events[0].payload["reason"] == "attempt_not_current"


def test_gateway_is_bound_to_one_run_snapshot(tmp_path: Path) -> None:
    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = _Launcher()

    with pytest.raises(PermissionError, match="does not belong"):
        _gateway(store, launcher).execute(_request(tmp_path, request_id="tool-2").model_copy(update={"run_id": "run-2"}))

    assert launcher.calls == []
    assert store.stream_ids("security") == []


def test_policy_state_provider_failure_fails_closed_and_is_audited(tmp_path: Path) -> None:
    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = _Launcher()

    def unavailable(_request: ToolRequest) -> PolicyState:
        raise OSError("policy projection unavailable")

    with pytest.raises(PermissionError, match="policy state"):
        _gateway(store, launcher, policy_state=unavailable).execute(_request(tmp_path))

    assert launcher.calls == []
    events = store.read_stream("security", "run-1")
    assert [event.event_type for event in events] == ["ToolExecutionBlocked"]
    assert events[0].payload["reason"] == "policy_state_unavailable"


def test_policy_version_change_before_launch_fails_closed(tmp_path: Path) -> None:
    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = _Launcher()
    states = iter((PolicyState(1, 4), PolicyState(2, 4)))

    with pytest.raises(PermissionError, match="changed"):
        _gateway(store, launcher, policy_state=lambda request: next(states)).execute(_request(tmp_path))

    assert launcher.calls == []
    events = store.read_stream("security", "run-1")
    assert events[-1].event_type == "ToolExecutionBlocked"
    assert events[-1].payload["reason"] == "policy_state_changed"


def test_request_id_cannot_replay_a_previously_started_tool(tmp_path: Path) -> None:
    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = _Launcher()
    gateway = _gateway(store, launcher)
    request = _request(tmp_path)
    gateway.execute(request)

    with pytest.raises(ToolRequestAlreadyUsed, match="already exists"):
        gateway.execute(request)

    assert len(launcher.calls) == 1


def test_request_audit_failure_prevents_launch(tmp_path: Path) -> None:
    class BrokenStore:
        def append_checked(self, *args, **kwargs):
            raise OSError("storage unavailable")

    launcher = _Launcher()
    gateway = ToolGateway(
        run_id="run-1",
        event_store=BrokenStore(),
        policy_manifest=_manifest(),
        attempt_authority=_Authority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=launcher,
    )

    with pytest.raises(ToolAuditUnavailable):
        gateway.execute(_request(tmp_path))

    assert launcher.calls == []


def test_authority_loss_during_execution_cancels_and_discards_success_status(tmp_path: Path) -> None:
    class BlockingSession(_Session):
        def __init__(self) -> None:
            super().__init__()
            self.done = threading.Event()

        def wait(self) -> SandboxResult:
            self.done.wait(1)
            base = super().wait()
            return SandboxResult(
                unit_name=base.unit_name,
                returncode=-9 if self.cancelled else base.returncode,
                stdout=base.stdout,
                stderr=base.stderr,
                elapsed_seconds=base.elapsed_seconds,
                termination_confirmed=True,
                cancelled=self.cancelled,
                timed_out=False,
                output_limited=False,
            )

        def cancel(self) -> bool:
            accepted = super().cancel()
            self.done.set()
            return accepted

    class BlockingLauncher(_Launcher):
        def __init__(self) -> None:
            self.calls = []
            self.session = BlockingSession()

    authority = _Authority()
    # The first two authority checks admit the request; the live monitor then
    # observes revocation and cancels the session while its wait is in flight.
    calls = 0

    def check(request: ToolRequest) -> bool:
        nonlocal calls
        calls += 1
        return calls <= 2

    class ChangingAuthority:
        def is_current(self, request: ToolRequest) -> bool:
            return check(request)

    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = BlockingLauncher()
    result = _gateway(store, launcher, authority=ChangingAuthority()).execute(_request(tmp_path))

    assert result.outcome == "authority_lost"
    assert launcher.session.cancelled
    assert result.stdout == b""
    assert result.stdout_sha256 is not None
    assert store.read_stream("security", "run-1")[-1].event_type == "ToolExecutionCancelled"


def test_unconfirmed_termination_is_recorded_unknown_and_output_is_discarded(tmp_path: Path) -> None:
    class UnconfirmedSession(_Session):
        def wait(self) -> SandboxResult:
            result = super().wait()
            return SandboxResult(
                unit_name=result.unit_name,
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                elapsed_seconds=result.elapsed_seconds,
                termination_confirmed=False,
                cancelled=False,
                timed_out=False,
                output_limited=False,
            )

    class UnconfirmedLauncher(_Launcher):
        def __init__(self) -> None:
            self.calls = []
            self.session = UnconfirmedSession()

    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = UnconfirmedLauncher()
    result = _gateway(store, launcher).execute(_request(tmp_path))

    assert result.outcome == "execution_unknown"
    assert result.stdout == b""
    assert result.stdout_sha256 is not None
    assert result.termination_confirmed is False
    assert store.read_stream("security", "run-1")[-1].event_type == "ToolExecutionOutcomeUnknown"


def test_session_wait_exception_is_audited_and_requests_cancellation(tmp_path: Path) -> None:
    class BrokenSession(_Session):
        def wait(self) -> SandboxResult:
            raise OSError("wait channel broke")

        def cancel(self) -> bool:
            self.cancelled = True
            return True

    class BrokenLauncher(_Launcher):
        def __init__(self) -> None:
            self.calls = []
            self.session = BrokenSession()

    store = SQLiteEventStore(tmp_path / "events.db")
    launcher = BrokenLauncher()
    with pytest.raises(OSError, match="wait channel broke"):
        _gateway(store, launcher).execute(_request(tmp_path))

    assert launcher.session.cancelled
    events = store.read_stream("security", "run-1")
    assert events[-1].event_type == "ToolExecutionOutcomeUnknown"
    assert events[-1].payload["reason"] == "session_wait_failed"


def test_launcher_failure_is_terminally_audited(tmp_path: Path) -> None:
    class BrokenLauncher:
        def launch(self, *args, **kwargs):
            raise OSError("isolation unavailable")

    store = SQLiteEventStore(tmp_path / "events.db")
    gateway = ToolGateway(
        run_id="run-1",
        event_store=store,
        policy_manifest=_manifest(),
        attempt_authority=_Authority(),
        policy_state=lambda request: PolicyState(1, 4),
        launcher=BrokenLauncher(),
    )
    with pytest.raises(OSError, match="isolation unavailable"):
        gateway.execute(_request(tmp_path))

    events = store.read_stream("security", "run-1")
    assert events[-1].event_type == "ToolExecutionFailed"
    assert events[-1].payload["reason"] == "launcher_unavailable"


def test_request_rejects_unbounded_or_non_absolute_inputs(tmp_path: Path) -> None:
    base = _request(tmp_path).model_dump()
    with pytest.raises(ValueError):
        ToolRequest(**{**base, "workspace": "relative"})
    with pytest.raises(ValueError):
        ToolRequest(**{**base, "command": ("/bin/echo", "x" * 70_000)})
    with pytest.raises(ValueError):
        ToolRequest(**{**base, "command": ("/bin/echo", "")})
