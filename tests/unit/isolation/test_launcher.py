from __future__ import annotations

import orchestrator.isolation.launcher as launcher_module

from pathlib import Path
import os
import io
import subprocess
import tempfile
import threading
import time

import pytest

from orchestrator.isolation.launcher import (
    InvalidSandboxRequest,
    IsolationUnavailable,
    SandboxLimits,
    SandboxSession,
    SystemdReadOnlyLauncher,
    _systemd_path_supported,
    _create_staging,
    _expand_runtime_paths,
    _validate_command,
    _validated_workspace,
)


class _CollectorControl(BaseException):
    pass


class _CollectorPipe(io.FileIO):
    """A real, fixture-owned pipe end with finite boundary fault injection."""

    def __init__(self, fd, mode, name):
        super().__init__(fd, mode, closefd=True)
        self.name_for_test = name
        self.close_calls = 0
        self.close_error = None
        self.close_before_error = False
        self.fileno_error = None
        self.closed_error = None

    def fileno(self):
        if self.fileno_error is not None:
            raise self.fileno_error
        return super().fileno()

    @property
    def closed(self):
        if self.closed_error is not None:
            raise self.closed_error
        return super().closed

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            if self.close_before_error:
                super().close()
            raise self.close_error
        super().close()


@pytest.fixture
def collector_rig(monkeypatch, request):
    """Retain real objects so teardown never retries a possibly reused fd."""
    from types import SimpleNamespace

    ends = []
    pipes = {}
    for name in ("stdin", "stdout", "stderr"):
        reader_fd, writer_fd = os.pipe()
        reader = _CollectorPipe(reader_fd, "rb", name if name != "stdin" else "input-reader")
        writer = _CollectorPipe(writer_fd, "wb", name if name == "stdin" else name + "-writer")
        ends.extend((reader, writer))
        pipes[name] = writer if name == "stdin" else reader
        if name != "stdin" and not getattr(request, "param", False):
            io.FileIO.close(writer)

    class Transport:
        returncode = 0
        stdin, stdout, stderr = (pipes[name] for name in ("stdin", "stdout", "stderr"))

        def poll(self):
            # Keep input writable until completion, with no external process.
            return 0 if io.FileIO.closed.__get__(self.stdin) else None

        def wait(self, *, timeout):
            assert timeout == 2.0
            return 0

    process = Transport()
    session = SandboxSession(process=process, unit_name="collector.service", systemctl="unused",
        client_env={}, output_limit=1024, timeout_seconds=10, input_bytes=b"payload",
        staging=SimpleNamespace(cleanup=lambda: pytest.fail("direct collector cannot clean staging")))
    state = SimpleNamespace(pipes=pipes, process=process, session=session, selectors=[],
        constructor_error=None, register_error=None, register_at=1, select_error=None,
        unregister_error=None, selector_close_error=None, before_select=None)
    selector_type = launcher_module.selectors.DefaultSelector

    class Selector:
        def __init__(self):
            self.real = selector_type()
            self.close_calls = 0
            self.register_calls = 0

        def register(self, stream, events):
            self.register_calls += 1
            if state.register_error is not None and self.register_calls == state.register_at:
                raise state.register_error
            return self.real.register(stream, events)

        def unregister(self, stream):
            if state.unregister_error is not None:
                raise state.unregister_error
            return self.real.unregister(stream)

        def get_map(self):
            return self.real.get_map()

        def select(self, timeout):
            if state.before_select is not None:
                state.before_select()
            if state.select_error is not None:
                raise state.select_error
            return self.real.select(timeout)

        def close(self):
            self.close_calls += 1
            self.real.close()
            if state.selector_close_error is not None:
                raise state.selector_close_error

    def selector_factory():
        if state.constructor_error is not None:
            raise state.constructor_error
        selector = Selector()
        state.selectors.append(selector)
        return selector

    monkeypatch.setattr(launcher_module.selectors, "DefaultSelector", selector_factory)
    try:
        yield state
    finally:
        for selector in state.selectors:
            selector.real.close()
        for stream in ends:
            # Bypass injection only on this fixture's retained FileIO objects.
            io.FileIO.close(stream)
            stream.closed_error = stream.fileno_error = stream.close_error = None


def _assert_collector_closed_once(rig):
    assert [rig.pipes[name].close_calls for name in ("stdin", "stdout", "stderr")] == [1, 1, 1]
    assert all(selector.close_calls == 1 for selector in rig.selectors)
    for stream in rig.pipes.values():
        if stream.close_error is None or stream.close_before_error:
            assert io.FileIO.closed.__get__(stream)


@pytest.mark.parametrize(("boundary", "position"), [("constructor", 0)] + [
    (boundary, position) for boundary in ("fileno", "set-blocking", "register") for position in (1, 2, 3)])
def test_collector_setup_failure_closes_every_owned_pipe(collector_rig, monkeypatch, boundary, position):
    rig = collector_rig
    primary = OSError("setup failure")
    if boundary == "constructor":
        rig.constructor_error = primary
    elif boundary == "register":
        rig.register_error, rig.register_at = primary, position
    else:
        stream = rig.pipes[("stdout", "stderr", "stdin")[position - 1]]
        if boundary == "fileno":
            stream.fileno_error = primary
        else:
            target_fd = io.FileIO.fileno(stream)
            set_blocking = os.set_blocking

            def fail_set_blocking(fd, value):
                if fd == target_fd:
                    raise primary
                return set_blocking(fd, value)

            monkeypatch.setattr(os, "set_blocking", fail_set_blocking)
    with pytest.raises(OSError) as caught:
        rig.session._collect()
    assert caught.value is primary
    _assert_collector_closed_once(rig)
    if boundary == "constructor":
        assert rig.selectors == []  # No returned selector handle was owned.


@pytest.mark.parametrize("boundary", ["closed", "read-fileno", "unregister", "select"])
def test_collector_loop_boundary_failure_still_closes_owned_resources(collector_rig, boundary):
    rig = collector_rig
    primary = OSError("loop failure")
    if boundary == "closed":
        rig.pipes["stdin"].closed_error = primary
    elif boundary == "read-fileno":
        rig.before_select = lambda: setattr(rig.pipes["stdout"], "fileno_error", primary)
    elif boundary == "unregister":
        rig.unregister_error = primary
    else:
        rig.select_error = primary
    with pytest.raises(OSError) as caught:
        rig.session._collect()
    assert caught.value is primary
    _assert_collector_closed_once(rig)


@pytest.mark.parametrize("path", ["empty-input", "eof", "input-complete", "input-oserror", "input-after-exit"])
@pytest.mark.parametrize("after_close", [False, True])
def test_collector_records_normal_close_attempt_before_failure(collector_rig, monkeypatch, path, after_close):
    rig = collector_rig
    primary = OSError("normal close failure")
    stream = rig.pipes["stdout" if path == "eof" else "stdin"]
    stream.close_error, stream.close_before_error = primary, after_close
    if path == "empty-input":
        rig.session._input_bytes = b""
    elif path == "input-oserror":
        def broken_write(_fd, _payload):
            raise BrokenPipeError("input peer closed")
        monkeypatch.setattr(os, "write", broken_write)
    elif path == "input-after-exit":
        monkeypatch.setattr(rig.process, "poll", lambda: 0)
    with pytest.raises(OSError) as caught:
        rig.session._collect()
    assert caught.value is primary
    _assert_collector_closed_once(rig)


@pytest.mark.parametrize("failed_close", ["selector", "stdin", "stdout", "stderr"])
def test_collector_cleanup_error_does_not_skip_other_closes_or_mask_primary(collector_rig, failed_close):
    rig = collector_rig
    primary = OSError("collection failure")
    secondary = OSError("private secondary sentinel")
    rig.select_error = primary
    if failed_close == "selector":
        rig.selector_close_error = secondary
    else:
        rig.pipes[failed_close].close_error = secondary
    with pytest.raises(OSError) as caught:
        rig.session._collect()
    assert caught.value is primary
    _assert_collector_closed_once(rig)
    assert caught.value.__notes__ == [
        "sandbox_session_selector_close_failed" if failed_close == "selector" else "sandbox_session_pipe_close_failed",
        "sandbox_session_secondary_failure",
    ]
    import traceback
    assert "private secondary sentinel" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("primary_control", [False, True])
def test_collector_preserves_primary_control_or_first_cleanup_control(collector_rig, primary_control):
    rig = collector_rig
    primary = _CollectorControl("primary") if primary_control else OSError("ordinary primary")
    first_control = _CollectorControl("first cleanup")
    rig.select_error = primary
    rig.selector_close_error = OSError("private selector secondary")
    rig.pipes["stdin"].close_error = first_control
    rig.pipes["stdout"].close_error = _CollectorControl("private later control")
    rig.pipes["stderr"].close_error = OSError("private pipe secondary")
    with pytest.raises(BaseException) as caught:
        rig.session._collect()
    assert caught.value is (primary if primary_control else first_control)
    _assert_collector_closed_once(rig)
    notes = caught.value.__notes__
    assert len(notes) == len(set(notes))
    assert set(notes) == {"sandbox_session_selector_close_failed", "sandbox_session_pipe_close_failed",
        "sandbox_session_secondary_failure"} | (set() if primary_control else {"sandbox_session_primary_superseded"})


def test_collector_first_cleanup_error_is_preserved_without_primary(collector_rig):
    rig = collector_rig
    # Real EOF and input completion have already attempted the pipes by this point.
    first = OSError("selector final close")
    rig.selector_close_error = first
    with pytest.raises(OSError) as caught:
        rig.session._collect()
    assert caught.value is first
    _assert_collector_closed_once(rig)


@pytest.mark.parametrize("collector_rig", [True], indirect=True)
def test_collector_first_final_cleanup_control_wins_without_primary(collector_rig, monkeypatch):
    rig = collector_rig
    # Held real output pipes reach the existing bounded cancellation break.
    rig.session._stop_grace_seconds = 0.001
    rig.session._cancel_requested.set()
    monkeypatch.setattr(rig.session, "cancel", lambda: False)
    first = _CollectorControl("first final control")
    rig.selector_close_error = OSError("private ordinary final cleanup")
    rig.pipes["stdout"].close_error = first
    rig.pipes["stderr"].close_error = _CollectorControl("private later final control")
    with pytest.raises(_CollectorControl) as caught:
        rig.session._collect()
    assert caught.value is first
    _assert_collector_closed_once(rig)
    assert set(first.__notes__) == {"sandbox_session_selector_close_failed",
        "sandbox_session_pipe_close_failed", "sandbox_session_secondary_failure"}


@pytest.mark.parametrize("notes_fail", [False, True])
def test_collector_note_insertion_cannot_replace_selected_error(collector_rig, notes_fail):
    class Primary(OSError):
        def add_note(self, _note):
            pytest.fail("must bypass an exception's add_note override")

        def __setattr__(self, name, value):
            if name == "__notes__" and notes_fail:
                raise _CollectorControl("note insertion failed")
            return super().__setattr__(name, value)

    rig = collector_rig
    primary = Primary("collection")
    rig.select_error = primary
    rig.selector_close_error = OSError("private cleanup")
    with pytest.raises(Primary) as caught:
        rig.session._collect()
    assert caught.value is primary
    _assert_collector_closed_once(rig)
    if not notes_fail:
        assert primary.__notes__ == ["sandbox_session_selector_close_failed", "sandbox_session_secondary_failure"]


def test_collector_deduplicates_owned_objects_and_preserves_stdin_eligibility(collector_rig):
    rig = collector_rig
    rig.process.stderr = rig.process.stdout
    rig.session._input_bytes = None
    rig.constructor_error = OSError("setup")
    with pytest.raises(OSError):
        rig.session._collect()
    assert rig.pipes["stdout"].close_calls == 1
    assert rig.pipes["stdin"].close_calls == rig.pipes["stderr"].close_calls == 0


@pytest.fixture
def wait_rig(monkeypatch):
    from types import SimpleNamespace

    unit, cgroup = _attempt_identity()
    rig = SimpleNamespace(errors={}, fail_at={}, callbacks={}, counts={}, events=[], cgroup=cgroup)

    def step(name):
        rig.events.append(name)
        rig.counts[name] = rig.counts.get(name, 0) + 1
        callback = rig.callbacks.get(name)
        if callback is not None:
            callback()
        if name in rig.errors and rig.counts[name] == rig.fail_at.get(name, rig.counts[name]):
            raise rig.errors[name]

    class Transport:
        returncode = 0

        def poll(self):
            step("poll")
            return rig.poll_result

        def wait(self, *, timeout):
            assert timeout == 2.0
            step("wait")
            return 0

    rig.poll_result = 0
    rig.staging = SimpleNamespace(cleanup=lambda: step("cleanup"), discard=lambda: step("discard"))
    rig.session = SandboxSession(process=Transport(), unit_name=unit, systemctl="unused",
        client_env={}, output_limit=1024, timeout_seconds=10, staging=rig.staging)
    rig.result = launcher_module.SandboxResult(unit, 0, b"stdout", b"stderr", 0.25, None,
        False, False, False, True)
    rig.receipt = launcher_module.SandboxTerminationReceipt(unit,
        "/" + cgroup.relative_to("/sys/fs/cgroup").as_posix(), "inactive", True)

    def collect(**_private):
        step("collect")
        return rig.result

    def read_receipt(**_arguments):
        step("receipt")
        return rig.receipt

    def binding(*_arguments):
        step("binding")
        return "stopped"

    owner = SimpleNamespace(_retire_session=lambda session: step("retire"))

    def owner_ref():
        step("owner")
        return owner

    rig.session._owner_ref = owner_ref
    monkeypatch.setattr(rig.session, "_collect", collect)
    monkeypatch.setattr(rig.session, "cancel", lambda: step("cancel"))
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", read_receipt)
    monkeypatch.setattr(launcher_module, "_observe_prepared_binding", binding)
    return rig


def _assert_wait_failure_replay_is_detached_and_inert(rig):
    counts = dict(rig.counts)
    failures = []
    for _ in range(2):
        # A caller may replay while already handling an unrelated exception.
        try:
            raise RuntimeError("private unrelated caller context")
        except RuntimeError:
            with pytest.raises(IsolationUnavailable) as caught:
                rig.session.wait()
        assert type(caught.value) is IsolationUnavailable
        assert caught.value.args == ("sandbox_session_wait_failed",)
        assert caught.value.__cause__ is caught.value.__context__ is None
        failures.append(caught.value)
    assert failures[0] is not failures[1]
    assert rig.counts == counts
    assert rig.session._result is None
    assert getattr(rig.session, "_wait_failed", False) is True
    assert getattr(rig.session, "_wait_owner_thread", None) is None
    assert not any(isinstance(value, BaseException) or value is rig.result
        for value in vars(rig.session).values())


@pytest.mark.parametrize("boundary", ["cancel", "wait", "wait-timeout", "poll", "cleanup"])
def test_wait_primary_failure_survives_outer_cleanup_and_replay_is_inert(wait_rig, boundary):
    rig = wait_rig
    primary = OSError("primary collection failure")
    rig.errors["collect"] = primary
    phase = "wait" if boundary == "wait-timeout" else boundary
    rig.errors[phase] = (subprocess.TimeoutExpired("private wait sentinel", 2)
        if boundary == "wait-timeout" else OSError("private cleanup sentinel"))
    latch_observations = []
    for name in ("cancel", "wait", "poll", "cleanup"):
        rig.callbacks[name] = lambda: latch_observations.append(getattr(rig.session, "_wait_failed", False))
    with pytest.raises(OSError) as caught:
        rig.session.wait()
    assert caught.value is primary
    assert rig.counts["cancel"] == rig.counts["wait"] == rig.counts["poll"] == 1
    assert rig.counts.get("cleanup", 0) == (0 if phase == "poll" else 1)
    assert latch_observations and all(latch_observations)
    assert len(primary.__notes__) == len(set(primary.__notes__))
    import traceback
    assert "private" not in "".join(traceback.format_exception(primary))
    _assert_wait_failure_replay_is_detached_and_inert(rig)


@pytest.mark.parametrize("primary_control", [False, True])
@pytest.mark.parametrize("first_control_phase", ["cancel", "wait"])
def test_wait_preserves_primary_control_or_first_outer_cleanup_control(wait_rig, primary_control, first_control_phase):
    rig = wait_rig
    primary = _CollectorControl("primary control") if primary_control else OSError("primary")
    first = _CollectorControl("first cleanup control")
    rig.errors.update(collect=primary, cancel=OSError("private cancel"),
        wait=_CollectorControl("private later wait"), poll=_CollectorControl("private later poll"))
    rig.errors[first_control_phase] = first
    with pytest.raises(BaseException) as caught:
        rig.session.wait()
    assert caught.value is (primary if primary_control else first)
    assert rig.counts == {"collect": 1, "cancel": 1, "wait": 1, "poll": 1}
    notes = caught.value.__notes__
    assert len(notes) == len(set(notes))
    assert set(notes) == {"sandbox_session_cancel_cleanup_failed", "sandbox_session_wait_cleanup_failed",
        "sandbox_session_poll_cleanup_failed", "sandbox_session_secondary_failure"} | (
        set() if primary_control else {"sandbox_session_primary_superseded"})
    _assert_wait_failure_replay_is_detached_and_inert(rig)


@pytest.mark.parametrize("boundary", ["poll-final", "cleanup", "discard", "owner", "retire", "receipt", "receipt-poll", "binding"])
def test_wait_pending_result_is_not_cached_after_teardown_or_receipt_failure(wait_rig, boundary):
    rig = wait_rig
    primary = OSError("pending result failure")
    scoped = boundary not in {"cleanup", "poll-final"}
    if scoped:
        rig.session._scope_cgroup = rig.cgroup
    phase = "poll" if boundary in {"poll-final", "receipt-poll"} else boundary
    rig.errors[phase] = primary
    if boundary == "receipt-poll":
        rig.fail_at["poll"] = 1
    if boundary == "binding":
        rig.session._launch_receipt = launcher_module.SandboxLaunchReceipt(
            "f" * 32, rig.session.unit_name, rig.receipt.control_group, "a" * 32, "b" * 64)
    unpublished = []
    for name in ("receipt", "binding", "cleanup", "discard", "owner", "retire"):
        rig.callbacks[name] = lambda: unpublished.append(rig.session._result is None)
    with pytest.raises(OSError) as caught:
        rig.session.wait()
    assert caught.value is primary
    assert rig.session._result is None
    assert all(unpublished)
    if boundary in {"discard", "poll-final", "receipt-poll", "receipt", "binding"}:
        assert rig.counts.get("retire", 0) == 0
    if boundary in {"poll-final", "receipt-poll"}:
        assert rig.counts.get("cleanup", 0) == rig.counts.get("discard", 0) == 0
    assert rig.counts.get("cancel", 0) == rig.counts.get("wait", 0) == (
        1 if boundary in {"receipt", "receipt-poll", "binding"} else 0)
    _assert_wait_failure_replay_is_detached_and_inert(rig)


@pytest.mark.parametrize("cleanup_enabled", [False, True])
def test_wait_publishes_after_qualified_cleanup_and_replays_same_object(wait_rig, cleanup_enabled):
    rig = wait_rig
    rig.session._scope_cgroup = rig.cgroup
    rig.session._cleanup_on_termination = cleanup_enabled
    unpublished = []
    for name in ("discard", "owner", "retire"):
        rig.callbacks[name] = lambda: unpublished.append(rig.session._result is None)
    result = rig.session.wait()
    assert result.termination_receipt is rig.receipt
    assert all(unpublished)
    assert rig.counts.get("discard", 0) == int(cleanup_enabled)
    assert rig.counts["retire"] == 1
    assert rig.counts.get("cleanup", 0) == rig.counts.get("cancel", 0) == rig.counts.get("wait", 0) == 0
    if cleanup_enabled:
        assert rig.events.index("discard") < rig.events.index("owner") < rig.events.index("retire")
    counts = dict(rig.counts)
    assert rig.session.wait() is result
    assert rig.counts == counts
    assert getattr(rig.session, "_wait_owner_thread", None) is None


def test_wait_fresh_poll_failure_never_uses_earlier_exit_to_delete_or_retire(wait_rig):
    rig = wait_rig
    rig.session._scope_cgroup = rig.cgroup
    rig.errors["poll"] = OSError("fresh final poll")
    rig.fail_at["poll"] = 2
    with pytest.raises(OSError):
        rig.session.wait()
    assert rig.counts["receipt"] == 1
    assert rig.counts.get("discard", 0) == rig.counts.get("owner", 0) == rig.counts.get("retire", 0) == 0
    _assert_wait_failure_replay_is_detached_and_inert(rig)


class _FiniteReentryLock:
    """Make the unfixed recursive-acquire bug fail promptly rather than hang."""

    def __init__(self, on_acquire=lambda: None):
        self.real = threading.Lock()
        self.acquisitions = 0
        self.on_acquire = on_acquire

    def __enter__(self):
        self.acquisitions += 1
        if not self.real.acquire(timeout=0.1):
            raise RuntimeError("recursive wait tried to acquire its owned lock")
        self.on_acquire()
        return self

    def __exit__(self, *_error):
        self.real.release()


@pytest.mark.parametrize("boundary", ["collect", "cleanup", "cancel"])
def test_wait_reentry_refuses_before_lock_without_poisoning_caught_outer_call(wait_rig, boundary):
    rig = wait_rig
    lock = _FiniteReentryLock()
    rig.session._wait_lock = lock
    nested_errors = []

    def nested():
        with pytest.raises(IsolationUnavailable) as caught:
            rig.session.wait()
        assert caught.value.args == ("sandbox_session_wait_failed",)
        assert caught.value.__context__ is caught.value.__cause__ is None
        nested_errors.append(caught.value)

    rig.callbacks[boundary] = nested
    if boundary == "cancel":
        primary = OSError("outer collection")
        rig.errors["collect"] = primary
        with pytest.raises(OSError) as caught:
            rig.session.wait()
        assert caught.value is primary
        _assert_wait_failure_replay_is_detached_and_inert(rig)
    else:
        assert rig.session.wait() is rig.result
        assert not getattr(rig.session, "_wait_failed", False)
    assert len(nested_errors) == lock.acquisitions == 1
    assert getattr(rig.session, "_wait_owner_thread", None) is None


def test_wait_escaping_reentry_becomes_primary_failure_and_latches(wait_rig):
    rig = wait_rig
    rig.session._wait_lock = _FiniteReentryLock()
    rig.callbacks["collect"] = lambda: rig.session.wait()
    with pytest.raises(IsolationUnavailable) as caught:
        rig.session.wait()
    assert caught.value.args == ("sandbox_session_wait_failed",)
    assert rig.counts["collect"] == rig.counts["cancel"] == rig.counts["wait"] == 1
    _assert_wait_failure_replay_is_detached_and_inert(rig)


def test_wait_checks_failure_latch_again_after_acquiring_lock(wait_rig):
    rig = wait_rig
    rig.session._wait_lock = _FiniteReentryLock(lambda: setattr(rig.session, "_wait_failed", True))
    with pytest.raises(IsolationUnavailable, match="sandbox_session_wait_failed"):
        rig.session.wait()
    assert rig.counts == {}


@pytest.mark.parametrize("selected_phase", ["body", "collector-cleanup", "outer-cleanup"])
def test_wait_collector_and_outer_cleanup_share_fixed_notes_and_control_priority(collector_rig, monkeypatch, selected_phase):
    rig = collector_rig
    body = _CollectorControl("selected body") if selected_phase == "body" else OSError("body")
    selector = _CollectorControl("selected collector cleanup") if selected_phase == "collector-cleanup" else OSError("private selector")
    outer = _CollectorControl("selected outer cleanup")
    rig.select_error = body
    rig.selector_close_error = selector
    rig.pipes["stdout"].close_error = OSError("private pipe")
    counts = []

    def cleanup_failure(name, error):
        counts.append(name)
        raise error

    monkeypatch.setattr(rig.session, "cancel", lambda: cleanup_failure("cancel", outer))
    monkeypatch.setattr(rig.process, "wait", lambda **_options: cleanup_failure("wait", _CollectorControl("private wait")))
    monkeypatch.setattr(rig.process, "poll", lambda: 0)
    monkeypatch.setattr(rig.session._staging, "cleanup", lambda: cleanup_failure("staging", OSError("private staging")))
    with pytest.raises(BaseException) as caught:
        rig.session.wait()
    assert caught.value is {"body": body, "collector-cleanup": selector, "outer-cleanup": outer}[selected_phase]
    assert counts == ["cancel", "wait", "staging"]
    _assert_collector_closed_once(rig)
    notes = caught.value.__notes__
    assert len(notes) == len(set(notes))
    assert set(notes) == {"sandbox_session_selector_close_failed", "sandbox_session_pipe_close_failed",
        "sandbox_session_cancel_cleanup_failed", "sandbox_session_wait_cleanup_failed",
        "sandbox_session_staging_cleanup_failed", "sandbox_session_secondary_failure"} | (
        set() if selected_phase == "body" else {"sandbox_session_primary_superseded"})
    import traceback
    assert "private" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("reject_notes", [False, True])
def test_wait_unbound_notes_and_insertion_failure_do_not_replace_primary(wait_rig, reject_notes):
    class Primary(OSError):
        def add_note(self, _note):
            pytest.fail("bound add_note must not be invoked")

        def __setattr__(self, name, value):
            if name == "__notes__" and reject_notes:
                raise _CollectorControl("private insertion failure")
            return super().__setattr__(name, value)

    rig = wait_rig
    primary = Primary("primary")
    rig.errors.update(collect=primary, cancel=OSError("private cancel"), wait=OSError("private wait"))
    with pytest.raises(Primary) as caught:
        rig.session.wait()
    assert caught.value is primary
    if not reject_notes:
        assert set(primary.__notes__) == {"sandbox_session_cancel_cleanup_failed",
            "sandbox_session_wait_cleanup_failed", "sandbox_session_secondary_failure"}
    _assert_wait_failure_replay_is_detached_and_inert(rig)


def test_wait_deduplicates_own_notes_without_traversing_foreign_notes(wait_rig):
    class ForeignNotes(list):
        def __iter__(self):
            pytest.fail("must not traverse pre-existing notes")

        def __contains__(self, _value):
            pytest.fail("must not search pre-existing notes")

    rig = wait_rig
    primary = OSError("primary")
    primary.__notes__ = ForeignNotes(["foreign note"])
    rig.errors.update(collect=primary, cancel=OSError("private cancel"), wait=OSError("private wait"))
    with pytest.raises(OSError) as caught:
        rig.session.wait()
    assert caught.value is primary
    assert list.count(primary.__notes__, "foreign note") == 1
    assert list.count(primary.__notes__, "sandbox_session_secondary_failure") == 1


@pytest.mark.parametrize(("collector_rig", "boundary"), [
    (False, "body"), (True, "selector-close"), (True, "pipe-close")], indirect=["collector_rig"])
def test_wait_latches_at_first_collector_failure_before_remaining_closes(collector_rig, monkeypatch, boundary):
    rig = collector_rig
    failure = OSError("first collector failure")
    if boundary == "body":
        rig.select_error = failure
    else:
        rig.session._stop_grace_seconds = 0.001
        rig.session._cancel_requested.set()
        if boundary == "selector-close":
            rig.selector_close_error = failure
        else:
            rig.pipes["stdout"].close_error = failure
    observations = []
    for name in ("stdout", "stderr"):
        original = rig.pipes[name].close

        def observing_close(name=name, original=original):
            observations.append((name, getattr(rig.session, "_wait_failed", False)))
            original()

        monkeypatch.setattr(rig.pipes[name], "close", observing_close)
    monkeypatch.setattr(rig.session, "cancel", lambda: False)
    monkeypatch.setattr(rig.process, "wait", lambda **_options: 0)
    monkeypatch.setattr(rig.session._staging, "cleanup", lambda: None)
    with pytest.raises(OSError) as caught:
        rig.session.wait()
    assert caught.value is failure
    assert observations == [("stdout", boundary != "pipe-close"), ("stderr", True)]
    _assert_collector_closed_once(rig)


def test_wait_refuses_after_direct_collector_failure_without_repeating_pipe_actions(collector_rig, monkeypatch):
    rig = collector_rig
    rig.constructor_error = OSError("collector setup")
    with pytest.raises(OSError):
        rig.session._collect()
    _assert_collector_closed_once(rig)
    monkeypatch.setattr(rig.session, "cancel", lambda: pytest.fail("failed replay must not cancel"))
    with pytest.raises(IsolationUnavailable, match="sandbox_session_wait_failed"):
        rig.session.wait()
    _assert_collector_closed_once(rig)


def test_wait_refuses_after_direct_collector_result_conversion_failure(collector_rig, monkeypatch):
    rig = collector_rig
    failure = OSError("transport returncode inspection")

    def fail_returncode(_process):
        raise failure

    monkeypatch.setattr(type(rig.process), "returncode", property(fail_returncode))
    with pytest.raises(OSError) as caught:
        rig.session._collect()
    assert caught.value is failure
    _assert_collector_closed_once(rig)
    assert getattr(rig.session, "_wait_failed", False) is True
    with pytest.raises(IsolationUnavailable, match="sandbox_session_wait_failed"):
        rig.session.wait()
    _assert_collector_closed_once(rig)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("memory_bytes", 1),
        ("tasks", 0),
        ("cpu_percent", 0),
        ("timeout_seconds", 0),
        ("output_bytes", 0),
        ("nofile", 1),
        ("file_bytes", 0),
        ("tasks", True),
    ],
)
def test_limits_reject_invalid_values(field: str, value: int) -> None:
    with pytest.raises(InvalidSandboxRequest):
        SandboxLimits(**{field: value})


@pytest.mark.parametrize("leader_exited", [True, False])
def test_scope_collection_has_bounded_stop_grace_even_with_held_pipes(monkeypatch, leader_exited) -> None:
    class Transport:
        stdin = None
        returncode = 0 if leader_exited else None
        def __init__(self):
            self.writers = []
            for name in ("stdout", "stderr"):
                reader, writer = os.pipe()
                setattr(self, name, os.fdopen(reader, "rb", buffering=0))
                self.writers.append(writer)
        def poll(self):
            return self.returncode
        def wait(self, **_options):
            pytest.fail("must not block waiting for an unconfirmed transport")
    transport = Transport()
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda args, **_options: calls.append(args) or subprocess.CompletedProcess(args, 1))
    with tempfile.TemporaryDirectory() as temporary:
        staging = type("Stage", (), {"cleanup": lambda _self: None})()
        session = SandboxSession(process=transport, unit_name="scope.scope", systemctl="systemctl",
            client_env={}, output_limit=1024, timeout_seconds=10, staging=staging,
            cancel_after_transport_exit=True, stop_grace_seconds=0.01)
        session._cancel_requested.set()
        try:
            result = session.wait()
        finally:
            for writer in transport.writers:
                os.close(writer)
    assert calls and calls[0][-1] == "scope.scope"
    assert result.elapsed_seconds < 1
    assert not result.termination_confirmed
    assert result.returncode == (0 if leader_exited else 125)


def test_collection_error_cleanup_wait_is_bounded_by_default(monkeypatch):
    class Transport:
        def poll(self):
            return None
        def wait(self, timeout=None):
            assert timeout is not None and 0 < timeout <= 5, "cleanup wait has no finite deadline"
            raise subprocess.TimeoutExpired("held", timeout)
    session = SandboxSession(
        process=Transport(), unit_name="unit.service", systemctl="ctl", client_env={},
        output_limit=1024, timeout_seconds=1, staging=type("Stage", (), {"cleanup": lambda _s: None})(),
    )
    monkeypatch.setattr(session, "_collect", lambda *, _cleanup_notes=None: (_ for _ in ()).throw(OSError("collector failed")))
    monkeypatch.setattr(subprocess, "run", lambda args, **_kw: subprocess.CompletedProcess(args, 1))
    with pytest.raises(OSError, match="collector failed"):
        session.wait()


def test_live_launcher_cannot_issue_not_found_stop_receipt(monkeypatch):
    class PendingLaunch:
        stdin = None
        returncode = None

        def __init__(self):
            self.writers = []
            for name in ("stdout", "stderr"):
                reader, writer = os.pipe()
                setattr(self, name, os.fdopen(reader, "rb", buffering=0))
                self.writers.append(writer)

        def poll(self):
            return None

    def systemctl(arguments, **_kwargs):
        if "show" in arguments:
            return subprocess.CompletedProcess(arguments, 0, stdout=b"ActiveState=inactive\nLoadState=not-found\nControlGroup=\n")
        return subprocess.CompletedProcess(arguments, 1)

    monkeypatch.setattr(subprocess, "run", systemctl)
    process = PendingLaunch()
    unit, cgroup = _attempt_identity()
    staging = _TrackingStaging()
    session = SandboxSession(process=process, unit_name=unit, systemctl="systemctl",
        client_env={}, output_limit=1024, timeout_seconds=10, staging=staging,
        scope_cgroup=cgroup, stop_grace_seconds=0.01)
    session._cancel_requested.set()
    try:
        result = session.wait()
        assert not result.termination_confirmed
        assert result.returncode == 125 and result.elapsed_seconds < 1
        assert session.wait() is result
        assert staging.discard_count == staging.cleanup_count == 0
    finally:
        for writer in process.writers:
            os.close(writer)


@pytest.mark.parametrize("grace", [None, True, 0, -1, float("inf"), float("nan")])
def test_session_rejects_unbounded_stop_grace(grace):
    with pytest.raises(InvalidSandboxRequest, match="stop grace"):
        SandboxSession(process=None, unit_name="unit.service", systemctl="ctl", client_env={},
                       output_limit=1024, timeout_seconds=1, staging=None, stop_grace_seconds=grace)


@pytest.mark.parametrize("command", [[], "echo hi", [""], ["echo", "bad\x00arg"]])
def test_commands_reject_ambiguous_or_invalid_arguments(command) -> None:
    with pytest.raises(InvalidSandboxRequest):
        _validate_command(command)


@pytest.mark.parametrize("payload", ["not-bytes", bytearray(b"bytes"), b"x" * 1_048_577])
def test_launcher_rejects_invalid_stdin_before_start(monkeypatch, tmp_path: Path, payload) -> None:
    import orchestrator.isolation.launcher as module

    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    with pytest.raises(InvalidSandboxRequest, match="stdin payload"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            tmp_path, ["/bin/true"], input_bytes=payload
        )


def test_workspace_must_be_absolute_real_and_existing(tmp_path: Path) -> None:
    with pytest.raises(InvalidSandboxRequest, match="absolute"):
        _validated_workspace(Path("relative"))
    with pytest.raises(InvalidSandboxRequest, match="existing real directory"):
        _validated_workspace(tmp_path / "missing")
    link = tmp_path / "workspace-link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(InvalidSandboxRequest, match="real directory"):
        _validated_workspace(link)


def test_systemd_property_paths_fail_closed_on_ambiguous_separators(tmp_path: Path) -> None:
    assert _systemd_path_supported(tmp_path)
    assert not _systemd_path_supported(tmp_path / "with space")
    assert not _systemd_path_supported(tmp_path / "colon:name")
    with pytest.raises(InvalidSandboxRequest, match="size"):
        _validate_command(["x" * 32769])


def test_trusted_runtime_token_resolves_only_existing_files_within_bind(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "worker.py").write_text("pass\n", encoding="utf-8")
    target = tmp_path / "target"
    assert _expand_runtime_paths(
        ["/usr/bin/python3", "@maestro-runtime@/worker.py"], source, target
    ) == ["/usr/bin/python3", str(target / "worker.py")]
    for path in (
        "@maestro-runtime@/missing.py", "@maestro-runtime@/../escape.py", "@maestro-runtime@/",
    ):
        with pytest.raises(InvalidSandboxRequest, match="runtime path"):
            _expand_runtime_paths([path], source, target)


def test_staging_refuses_a_workspace_that_covers_all_host_paths() -> None:
    with pytest.raises(IsolationUnavailable, match="outside the workspace"):
        _create_staging(Path("/"))


def test_non_linux_or_missing_systemd_fails_before_launch(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module

    monkeypatch.setattr(module.platform, "system", lambda: "Windows")
    with pytest.raises(IsolationUnavailable, match="Linux-only"):
        SystemdReadOnlyLauncher().launch(tmp_path, ["/bin/true"])

    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module.shutil, "which", lambda _name: None)
    with pytest.raises(IsolationUnavailable, match="required"):
        SystemdReadOnlyLauncher().launch(tmp_path, ["/bin/true"])


def test_workspace_snapshot_failure_is_reported_as_invalid_request(monkeypatch, tmp_path: Path) -> None:
    from orchestrator.isolation.workspace import WorkspaceBoundaryError
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_cgroup_parent", lambda *_args: Path("/sys/fs/cgroup/user.slice/app.slice"))
    monkeypatch.setattr(module, "snapshot_workspace", lambda *_args, **_kwargs: (_ for _ in ()).throw(WorkspaceBoundaryError()))
    with pytest.raises(InvalidSandboxRequest, match="snapshot"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


def test_launcher_rejects_excessive_workspace_snapshot_before_launch(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module
    from orchestrator.isolation.workspace import WorkspaceBoundaryError

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_cgroup_parent", lambda *_args: Path("/sys/fs/cgroup/user.slice/app.slice"))
    monkeypatch.setattr(module, "snapshot_workspace", lambda *_args, **_kwargs: (_ for _ in ()).throw(WorkspaceBoundaryError("too large")))
    with pytest.raises(InvalidSandboxRequest, match="snapshot"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


@pytest.mark.parametrize(
    "failure",
    [
        subprocess.CompletedProcess(["systemctl"], 1),
        OSError("missing systemctl"),
        subprocess.TimeoutExpired(["systemctl"], 5),
    ],
)
def test_launcher_requires_healthy_systemd_manager(monkeypatch, tmp_path: Path, failure) -> None:
    import orchestrator.isolation.launcher as module

    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    if isinstance(failure, BaseException):
        monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(failure))
        reason = "probe failed"
    else:
        monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: failure)
        reason = "usable systemd"
    with pytest.raises(IsolationUnavailable, match=reason):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            tmp_path, ["/bin/true"]
        )


def test_launcher_rejects_missing_or_ambiguous_trusted_runtime(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    monkeypatch.setattr(module, "_systemd_cgroup_parent", lambda *_args: Path("/sys/fs/cgroup/user.slice/app.slice"))
    monkeypatch.setattr(
        module,
        "__file__",
        str(tmp_path / "missing" / "runtime" / "orchestrator" / "isolation" / "launcher.py"),
    )
    with pytest.raises(IsolationUnavailable, match="trusted isolation runtime"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )

    runtime = tmp_path / "runtime space" / "src"
    runtime.mkdir(parents=True)
    monkeypatch.setattr(
        module,
        "__file__",
        str(runtime / "orchestrator" / "isolation" / "launcher.py"),
    )
    with pytest.raises(InvalidSandboxRequest, match="spaces"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


def test_launcher_retains_staging_when_systemd_client_start_is_interrupted(
    monkeypatch, tmp_path: Path
) -> None:
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    monkeypatch.setattr(module, "_systemd_cgroup_parent", lambda *_args: Path("/sys/fs/cgroup/user.slice/app.slice"))

    staging = tempfile.TemporaryDirectory(dir=tmp_path)
    staging_path = Path(staging.name)

    def fail_to_start(*args, **kwargs):
        assert staging._finalizer.peek() is None
        (staging_path / "marker").write_text("retained", encoding="utf-8")
        raise KeyboardInterrupt

    monkeypatch.setattr(module, "_create_staging", lambda _root: staging)
    monkeypatch.setattr(module.subprocess, "Popen", fail_to_start)
    with pytest.raises(KeyboardInterrupt):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )

    assert staging._finalizer.peek() is None
    assert (staging_path / "marker").read_text(encoding="utf-8") == "retained"


def test_launcher_retains_staging_when_systemd_client_start_is_uncertain(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    monkeypatch.setattr(module, "_systemd_cgroup_parent", lambda *_args: Path("/sys/fs/cgroup/user.slice/app.slice"))
    staging = tempfile.TemporaryDirectory(dir=tmp_path)
    staging_path = Path(staging.name)
    monkeypatch.setattr(module, "_create_staging", lambda _root: staging)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("no process")))

    with pytest.raises(IsolationUnavailable, match="could not be started"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )

    assert staging_path.exists()
    staging.cleanup()  # This test double never starts a child.


def test_launcher_cleans_staging_when_bind_targets_cannot_be_created(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    monkeypatch.setattr(module, "_systemd_cgroup_parent", lambda *_args: Path("/sys/fs/cgroup/user.slice/app.slice"))
    original_mkdir = Path.mkdir

    def fail_runtime_target(path: Path, *args, **kwargs):
        if path.name == "runtime" and path.parent.name.startswith("maestro-sandbox-"):
            raise OSError("cannot create mount target")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_runtime_target)
    with pytest.raises(IsolationUnavailable, match="mount targets"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


def test_cancel_acceptance_is_visible_before_collector_returns(monkeypatch) -> None:
    import orchestrator.isolation.launcher as module

    process = subprocess.Popen(
        ["/usr/bin/python3", "-c", "import time; time.sleep(30)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    signal_sent = threading.Event()
    allow_return = threading.Event()

    def slow_but_accepted_signal(*args, **kwargs):
        process.kill()
        signal_sent.set()
        assert allow_return.wait(timeout=3)
        return subprocess.CompletedProcess(args[0], 0)

    monkeypatch.setattr(module.subprocess, "run", slow_but_accepted_signal)
    session = SandboxSession(
        process=process,
        unit_name="test-attempt.service",
        systemctl="systemctl",
        client_env={"PATH": "/usr/bin:/bin"},
        output_limit=1024,
        timeout_seconds=30,
        staging=tempfile.TemporaryDirectory(),
    )
    results = []
    waiter = threading.Thread(target=lambda: results.append(session.wait()), daemon=True)
    waiter.start()
    time.sleep(0.05)
    cancel_results = []
    canceller = threading.Thread(target=lambda: cancel_results.append(session.cancel()), daemon=True)
    canceller.start()
    assert signal_sent.wait(timeout=3)
    time.sleep(0.05)
    allow_return.set()
    canceller.join(timeout=3)
    waiter.join(timeout=3)

    assert not canceller.is_alive()
    assert not waiter.is_alive()
    assert cancel_results == [True]
    assert results and results[0].cancelled
    assert not results[0].termination_confirmed


def test_session_transmits_large_stdin_without_stdout_deadlock() -> None:
    payload = b"p" * 262_144
    process = subprocess.Popen(
        [
            "/usr/bin/python3", "-c",
            "import sys; sys.stdout.buffer.write(b'x'*65536); sys.stdout.flush(); "
            "data=sys.stdin.buffer.read(); print(len(data))",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    session = SandboxSession(
        process=process,
        unit_name="test-attempt.service",
        systemctl="systemctl",
        client_env={"PATH": "/usr/bin:/bin"},
        output_limit=131_072,
        timeout_seconds=10,
        staging=tempfile.TemporaryDirectory(),
        input_bytes=payload,
    )
    result = session.wait()
    assert result.returncode == 0
    assert result.input_written
    assert result.stdout == b"x" * 65536 + b"262144\n"


def test_session_marks_early_exit_as_incomplete_stdin() -> None:
    process = subprocess.Popen(
        ["/usr/bin/python3", "-c", "print('done')"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    session = SandboxSession(
        process=process,
        unit_name="test-attempt.service",
        systemctl="systemctl",
        client_env={"PATH": "/usr/bin:/bin"},
        output_limit=1024,
        timeout_seconds=10,
        staging=tempfile.TemporaryDirectory(),
        input_bytes=b"x" * 1_048_576,
    )
    result = session.wait()
    assert result.returncode == 0
    assert result.stdout == b"done\n"
    assert not result.input_written


def test_session_output_limit_stops_bounded_stdin(monkeypatch) -> None:
    import orchestrator.isolation.launcher as module

    process = subprocess.Popen(
        ["/usr/bin/python3", "-c", "import sys,time; sys.stdout.buffer.write(b'x'*1048576); sys.stdout.flush(); time.sleep(30)"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )

    def kill_unit(*args, **kwargs):
        process.kill()
        return subprocess.CompletedProcess(args[0], 0)

    monkeypatch.setattr(module.subprocess, "run", kill_unit)
    session = SandboxSession(
        process=process,
        unit_name="test-attempt.service",
        systemctl="systemctl",
        client_env={"PATH": "/usr/bin:/bin"},
        output_limit=1024,
        timeout_seconds=10,
        staging=tempfile.TemporaryDirectory(),
        input_bytes=b"x" * 1_048_576,
    )
    result = session.wait()
    assert result.output_limited
    assert not result.termination_confirmed
    assert len(result.stdout) + len(result.stderr) <= 1024
    assert not result.input_written


def test_session_cancellation_stops_bounded_stdin(monkeypatch) -> None:
    import orchestrator.isolation.launcher as module

    process = subprocess.Popen(
        ["/usr/bin/python3", "-c", "import time; time.sleep(30)"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )

    def kill_unit(*args, **kwargs):
        process.kill()
        return subprocess.CompletedProcess(args[0], 0)

    monkeypatch.setattr(module.subprocess, "run", kill_unit)
    session = SandboxSession(
        process=process,
        unit_name="test-attempt.service",
        systemctl="systemctl",
        client_env={"PATH": "/usr/bin:/bin"},
        output_limit=1024,
        timeout_seconds=10,
        staging=tempfile.TemporaryDirectory(),
        input_bytes=b"x" * 1_048_576,
    )
    results = []
    waiter = threading.Thread(target=lambda: results.append(session.wait()), daemon=True)
    waiter.start()
    time.sleep(0.1)
    assert session.cancel()
    waiter.join(timeout=3)
    assert not waiter.is_alive()
    assert results and results[0].cancelled
    assert not results[0].termination_confirmed
    assert not results[0].input_written


def test_systemd_client_environment_requires_runtime_directory(monkeypatch) -> None:
    import orchestrator.isolation.launcher as module

    for name in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "SYSTEMD_BUS_ADDRESS"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(IsolationUnavailable, match="XDG_RUNTIME_DIR"):
        module._systemd_client_environment()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")

def test_sandbox_result_derives_termination_from_host_receipt() -> None:
    result = launcher_module.SandboxResult(
        unit_name="maestro-attempt-" + "a" * 32 + ".service",
        returncode=0,
        stdout=b"",
        stderr=b"",
        elapsed_seconds=0.01,
        termination_receipt=None,
        cancelled=False,
        timed_out=False,
        output_limited=False,
        input_written=True,
    )
    assert not result.termination_confirmed

    receipt = launcher_module.SandboxTerminationReceipt(
        unit_name=result.unit_name,
        control_group="/user.slice/user-1000.slice/user@1000.service/app.slice/" + result.unit_name,
        active_state="inactive",
        cgroup_empty=True,
    )
    confirmed = launcher_module.SandboxResult(
        unit_name=result.unit_name,
        returncode=0,
        stdout=b"",
        stderr=b"",
        elapsed_seconds=0.01,
        termination_receipt=receipt,
        cancelled=False,
        timed_out=False,
        output_limited=False,
        input_written=True,
    )
    assert confirmed.termination_confirmed
    assert confirmed.termination_receipt is receipt


@pytest.mark.parametrize(
    ("active_state", "load_state", "control_group", "empty", "confirmed"),
    [
        ("inactive", "loaded", "/user.slice/user-1000.slice/user@1000.service/app.slice/maestro-attempt-" + "b" * 32 + ".service", True, True),
        ("failed", "loaded", "/user.slice/user-1000.slice/user@1000.service/app.slice/maestro-attempt-" + "b" * 32 + ".service", True, True),
        ("active", "loaded", "/user.slice/user-1000.slice/user@1000.service/app.slice/maestro-attempt-" + "b" * 32 + ".service", True, False),
        ("inactive", "loaded", "/user.slice/unexpected/maestro-attempt-" + "b" * 32 + ".service", True, False),
        ("inactive", "loaded", "/user.slice/user-1000.slice/user@1000.service/app.slice/maestro-attempt-" + "b" * 32 + ".service", False, False),
        ("inactive", "not-found", "", True, True),
    ],
)
def test_systemd_termination_receipt_requires_inactive_exact_empty_scope(
    monkeypatch, tmp_path: Path, active_state, load_state, control_group, empty, confirmed
) -> None:
    unit = "maestro-attempt-" + "b" * 32 + ".service"
    expected = Path(
        "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice"
    ) / unit
    response = subprocess.CompletedProcess(
        ["systemctl"],
        0,
        stdout=(
            f"ActiveState={active_state}\n"
            f"LoadState={load_state}\n"
            f"ControlGroup={control_group}\n"
        ).encode("ascii"),
        stderr=b"",
    )
    monkeypatch.setattr(launcher_module.subprocess, "run", lambda *_args, **_kwargs: response)
    monkeypatch.setattr(launcher_module, "_cgroup_is_empty", lambda _path: empty)

    receipt = launcher_module._read_termination_receipt(
        unit_name=unit,
        expected_cgroup=expected,
        client_env={"PATH": "/usr/bin:/bin"},
        timeout_seconds=0.01,
    )

    assert (receipt is not None) is confirmed
    if confirmed:
        assert receipt.unit_name == unit
        assert receipt.control_group == "/" + str(expected.relative_to("/sys/fs/cgroup"))
        assert receipt.active_state == active_state
        assert receipt.cgroup_empty


@pytest.mark.parametrize(
    "stdout",
    [
        b"ActiveState=inactive\nLoadState=loaded\n",
        b"ActiveState=inactive\nActiveState=inactive\nLoadState=loaded\nControlGroup=/safe\n",
        b"ActiveState=inactive\nLoadState=loaded\nControlGroup=../../escape\n",
    ],
)
def test_systemd_termination_receipt_rejects_malformed_unit_evidence(
    monkeypatch, stdout: bytes
) -> None:
    unit = "maestro-attempt-" + "c" * 32 + ".service"
    expected = Path("/sys/fs/cgroup/user.slice/app.slice") / unit
    response = subprocess.CompletedProcess(["systemctl"], 0, stdout=stdout, stderr=b"")
    monkeypatch.setattr(launcher_module.subprocess, "run", lambda *_args, **_kwargs: response)
    monkeypatch.setattr(launcher_module, "_cgroup_is_empty", lambda _path: True)

    receipt = launcher_module._read_termination_receipt(
        unit_name=unit,
        expected_cgroup=expected,
        client_env={},
        timeout_seconds=0.01,
    )

    assert receipt is None


def test_systemd_termination_receipt_fails_closed_when_inspection_is_unavailable(
    monkeypatch,
) -> None:
    unit = "maestro-attempt-" + "d" * 32 + ".service"
    expected = Path("/sys/fs/cgroup/user.slice/app.slice") / unit

    def unavailable(*_args, **_kwargs):
        raise subprocess.TimeoutExpired("systemctl", 1)

    monkeypatch.setattr(launcher_module.subprocess, "run", unavailable)

    assert launcher_module._read_termination_receipt(
        unit_name=unit,
        expected_cgroup=expected,
        client_env={},
        timeout_seconds=0.01,
    ) is None


@pytest.mark.parametrize(
    ("unit_name", "control_group", "active_state", "empty"),
    [
        ("bad.service", "/user.slice/app.slice/bad.service", "inactive", True),
        ("maestro-attempt-" + "e" * 32 + ".service", "/user.slice/app.slice/../escaped", "inactive", True),
        ("maestro-attempt-" + "e" * 32 + ".service", "/user.slice/other.slice/" + "maestro-attempt-" + "e" * 32 + ".service", "inactive", True),
        ("maestro-attempt-" + "e" * 32 + ".service", "/user.slice/app.slice/" + "maestro-attempt-" + "e" * 32 + ".service", "active", True),
        ("maestro-attempt-" + "e" * 32 + ".service", "/user.slice/app.slice/" + "maestro-attempt-" + "e" * 32 + ".service", "inactive", False),
    ],
)
def test_termination_receipt_rejects_invalid_unit_or_cgroup_evidence(
    unit_name, control_group, active_state, empty
) -> None:
    with pytest.raises(ValueError):
        launcher_module.SandboxTerminationReceipt(
            unit_name=unit_name,
            control_group=control_group,
            active_state=active_state,
            cgroup_empty=empty,
        )


class _TrackingStaging:
    def __init__(self) -> None:
        self.discard_count = 0
        self.cleanup_count = 0

    def cleanup(self) -> None:
        self.cleanup_count += 1

    def discard(self) -> None:
        self.discard_count += 1


def _completed_process() -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        ["/usr/bin/python3", "-c", "print('done')"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )


def _attempt_identity() -> tuple[str, Path]:
    unit = "maestro-attempt-" + "f" * 32 + ".service"
    cgroup = Path("/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice") / unit
    return unit, cgroup


def test_session_retains_staging_and_replays_result_when_stop_is_unverified(monkeypatch) -> None:
    unit, cgroup = _attempt_identity()
    staging = _TrackingStaging()
    checks = []
    monkeypatch.setattr(
        launcher_module, "_read_termination_receipt",
        lambda **arguments: checks.append(arguments) or None,
    )
    session = SandboxSession(
        process=_completed_process(),
        unit_name=unit,
        systemctl="systemctl",
        client_env={},
        output_limit=1024,
        timeout_seconds=10,
        staging=staging,
        scope_cgroup=cgroup,
    )

    result = session.wait()

    assert result.returncode == 0
    assert not result.termination_confirmed
    assert session.wait() is result
    assert len(checks) == 1
    assert checks[0]["unit_name"] == unit
    assert checks[0]["expected_cgroup"] == cgroup
    assert staging.discard_count == 0
    assert staging.cleanup_count == 0


def test_session_discards_staging_once_only_after_receipt(monkeypatch) -> None:
    unit, cgroup = _attempt_identity()
    staging = _TrackingStaging()
    receipt = launcher_module.SandboxTerminationReceipt(
        unit_name=unit,
        control_group="/" + str(cgroup.relative_to("/sys/fs/cgroup")),
        active_state="inactive",
        cgroup_empty=True,
    )
    checks = []
    monkeypatch.setattr(
        launcher_module, "_read_termination_receipt",
        lambda **arguments: checks.append(arguments) or receipt,
    )
    session = SandboxSession(
        process=_completed_process(),
        unit_name=unit,
        systemctl="systemctl",
        client_env={},
        output_limit=1024,
        timeout_seconds=10,
        staging=staging,
        scope_cgroup=cgroup,
    )

    result = session.wait()

    assert result.termination_receipt is receipt
    assert result.termination_confirmed
    assert session.wait() is result
    assert len(checks) == 1
    assert staging.discard_count == 1
    assert staging.cleanup_count == 0


def test_systemd_cgroup_parent_resolves_only_verified_app_slice(monkeypatch) -> None:
    parent = "/user.slice/user-1000.slice/user@1000.service/app.slice"
    response = subprocess.CompletedProcess(
        ["systemctl"], 0, stdout=f"ControlGroup={parent}\n".encode(), stderr=b"",
    )
    opened = []
    monkeypatch.setattr(launcher_module.subprocess, "run", lambda *_args, **_kwargs: response)
    monkeypatch.setattr(
        launcher_module, "_open_cgroup_directory",
        lambda path: opened.append(path) or os.open("/dev/null", os.O_RDONLY),
    )

    result = launcher_module._systemd_cgroup_parent("systemctl", {})

    assert result == Path("/sys/fs/cgroup") / parent.lstrip("/")
    assert opened == [result]


@pytest.mark.parametrize(
    "output",
    [
        b"",
        b"ControlGroup=/user.slice/app.slice/../escaped\n",
        b"ControlGroup=/user.slice/other.slice\n",
        b"ControlGroup=/user.slice/app.slice\nControlGroup=/user.slice/app.slice\n",
    ],
)
def test_systemd_cgroup_parent_rejects_untrusted_or_malformed_path(
    monkeypatch, output: bytes
) -> None:
    response = subprocess.CompletedProcess(["systemctl"], 0, stdout=output, stderr=b"")
    monkeypatch.setattr(launcher_module.subprocess, "run", lambda *_args, **_kwargs: response)

    with pytest.raises(IsolationUnavailable, match="app.slice cgroup"):
        launcher_module._systemd_cgroup_parent("systemctl", {})


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b"populated 0\nfrozen 0\n", True),
        (b"populated 1\nfrozen 0\n", False),
        (b"populated 0\npopulated 0\n", False),
        (b"populated yes\n", False),
        (b"frozen 0\n", False),
    ],
)
def test_cgroup_empty_requires_a_strict_populated_zero_witness(
    monkeypatch, tmp_path: Path, payload: bytes, expected: bool
) -> None:
    (tmp_path / "cgroup.events").write_bytes(payload)
    monkeypatch.setattr(
        launcher_module, "_open_cgroup_directory",
        lambda _path: os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY),
    )

    assert launcher_module._cgroup_is_empty(Path("/sys/fs/cgroup/user.slice/app.slice/unit")) is expected


def test_termination_receipt_polling_bounds_each_systemctl_call(monkeypatch) -> None:
    unit = "maestro-attempt-" + "9" * 32 + ".service"
    expected = Path("/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice") / unit
    response = subprocess.CompletedProcess(
        ["systemctl"], 0,
        stdout=(
            "ActiveState=active\nLoadState=loaded\n"
            "ControlGroup=/user.slice/user-1000.slice/user@1000.service/app.slice/" + unit + "\n"
        ).encode(),
        stderr=b"",
    )
    timeouts = []

    def show(*_args, **kwargs):
        timeouts.append(kwargs["timeout"])
        return response

    monkeypatch.setattr(launcher_module.subprocess, "run", show)
    monkeypatch.setattr(launcher_module.time, "sleep", lambda _duration: None)

    result = launcher_module._read_termination_receipt(
        unit_name=unit,
        expected_cgroup=expected,
        client_env={},
        timeout_seconds=0.01,
    )

    assert result is None
    assert timeouts
    assert all(0 < timeout <= 0.01 for timeout in timeouts)


def test_scoped_session_detaches_temporary_directory_finalizer_until_verified_stop(
    monkeypatch, tmp_path: Path
) -> None:
    unit, cgroup = _attempt_identity()
    temporary = tempfile.TemporaryDirectory(dir=tmp_path)
    staging_path = Path(temporary.name)
    (staging_path / "marker").write_text("must remain", encoding="utf-8")
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", lambda **_arguments: None)
    session = SandboxSession(
        process=_completed_process(),
        unit_name=unit,
        systemctl="systemctl",
        client_env={},
        output_limit=1024,
        timeout_seconds=10,
        staging=temporary,
        scope_cgroup=cgroup,
    )

    assert temporary._finalizer.peek() is None
    result = session.wait()

    assert not result.termination_confirmed
    assert (staging_path / "marker").read_text(encoding="utf-8") == "must remain"
    temporary.cleanup()


def test_prepared_identity_precedes_launch(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("preparation must not start or inspect any process")
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    launcher = SystemdReadOnlyLauncher()
    first, second = launcher.prepare_launch(), launcher.prepare_launch()
    import uuid
    assert uuid.UUID(first.launch_id).version == 4
    assert first.unit_name == "maestro-attempt-" + uuid.UUID(first.launch_id).hex + ".service"
    assert first != second


def _observed_worker_profile():
    return dict(PrivateNetwork="yes", PrivateTmp="yes", PrivateUsers="yes", PrivateDevices="yes",
        ProtectHome="tmpfs", ProtectSystem="strict", ProtectProc="invisible", ProcSubset="pid",
        ProtectControlGroups="yes", ProtectKernelTunables="yes", ProtectKernelModules="yes",
        ProtectKernelLogs="yes", ProtectClock="yes", ProtectHostname="yes", NoNewPrivileges="yes",
        CapabilityBoundingSet="", RestrictAddressFamilies="AF_UNIX", RestrictRealtime="yes",
        RestrictSUIDSGID="yes", LockPersonality="yes", MemoryDenyWriteExecute="yes",
        WorkingDirectory="/tmp/staging/workspace", KillMode="control-group", Environment="",
        ExecStart="{ path=/usr/bin/env ; argv[]=/usr/bin/env -i /usr/bin/python3 -I -S /tmp/worker.py --child ; ignore_errors=no ; pending=yes }",
        MemoryMax="536870912", MemorySwapMax="0", TasksMax="64", CPUQuotaPerSecUSec="1s",
        CPUQuotaPeriodUSec="100ms", RuntimeMaxUSec="10s", TimeoutStopUSec="1s",
        LimitNOFILE="256", LimitNOFILESoft="256", LimitFSIZE="67108864", LimitFSIZESoft="67108864",
        LimitCORE="0", LimitCORESoft="0", InaccessiblePaths="-/run/user -/etc/shadow",
        BindReadOnlyPaths="/tmp/source:/tmp/staging/workspace:rbind /tmp/runtime:/tmp/staging/runtime:rbind")


@pytest.mark.parametrize("change", [dict(Id="wrong.service"), dict(LoadState="not-found"),
    dict(ControlGroup="/user.slice/app.slice/wrong.service"), dict(PrivateNetwork="no"),
    dict(ProtectSystem="full"), dict(NoNewPrivileges="no"), dict(MemoryMax="infinity")])
def test_ack_requires_observed_identity(monkeypatch, change):
    launch = launcher_module.PreparedSandboxLaunch(
        "4dc48974-60de-476b-8828-e6d979729813",
        "maestro-attempt-4dc4897460de476b8828e6d979729813.service")
    cgroup = Path("/sys/fs/cgroup/user.slice/app.slice") / launch.unit_name
    profile = _observed_worker_profile()
    properties = dict(Id=launch.unit_name, LoadState="loaded", ActiveState="active",
        ControlGroup="/user.slice/app.slice/" + launch.unit_name, InvocationID="a" * 32, **profile)
    monkeypatch.setattr(subprocess, "run", lambda *args, **_kwargs: subprocess.CompletedProcess(
        args, 0, stdout="".join(f"{k}={v}\n" for k, v in (properties | change).items()).encode()))
    assert launcher_module._read_launch_receipt(prepared_launch=launch, expected_cgroup=cgroup,
        expected_profile=launcher_module._normalise_launch_profile(profile), client_env={}, timeout_seconds=0.01) is None


def test_exact_observed_identity_issues_launch_receipt_before_wait(monkeypatch):
    launch = launcher_module.PreparedSandboxLaunch(
        "4dc48974-60de-476b-8828-e6d979729813",
        "maestro-attempt-4dc4897460de476b8828e6d979729813.service")
    control_group = "/user.slice/app.slice/" + launch.unit_name
    profile = _observed_worker_profile()
    properties = dict(Id=launch.unit_name, LoadState="loaded", ActiveState="active",
        ControlGroup=control_group, InvocationID="a" * 32, **profile)
    monkeypatch.setattr(subprocess, "run", lambda *args, **_kwargs: subprocess.CompletedProcess(
        args, 0, stdout="".join(f"{k}={v}\n" for k, v in properties.items()).encode()))
    receipt = launcher_module._read_launch_receipt(prepared_launch=launch,
        expected_cgroup=Path("/sys/fs/cgroup") / control_group.lstrip("/"),
        expected_profile=launcher_module._normalise_launch_profile(profile), client_env={}, timeout_seconds=0.01)
    assert receipt.launch_id == launch.launch_id and receipt.unit_name == launch.unit_name
    assert receipt.control_group == control_group and receipt.invocation_id == "a" * 32
    assert receipt.profile_sha256.startswith("sha256:")
    assert not hasattr(receipt, "cgroup_empty")


def test_epoch_cannot_be_reused_or_substituted(monkeypatch, tmp_path):
    from dataclasses import replace
    launcher = SystemdReadOnlyLauncher(systemd_run="run", systemctl="ctl")
    prepared = launcher.prepare_launch()
    # Consume an epoch even when validation proves no start, preventing retries
    # from accidentally reusing a journalled prepared identity.
    with pytest.raises(launcher_module.IsolationNotStarted):
        launcher.launch(tmp_path, [], prepared_launch=prepared)
    for other in (prepared, replace(prepared, unit_name="maestro-attempt-" + "a" * 32 + ".service")):
        with pytest.raises(launcher_module.IsolationNotStarted):
            launcher.launch(tmp_path, ["/bin/true"], prepared_launch=other)


def test_unacknowledged_prepared_launch_cannot_claim_stop():
    launcher = SystemdReadOnlyLauncher()
    prepared = launcher.prepare_launch()
    with pytest.raises(launcher_module.IsolationStartUnknown) as caught:
        launcher.stop_prepared_launch(prepared, None)
    assert caught.value.__context__ is None and caught.value.__cause__ is None


def _reconstructed_launch():
    prepared = launcher_module.PreparedSandboxLaunch("4dc48974-60de-476b-8828-e6d979729813",
        "maestro-attempt-4dc4897460de476b8828e6d979729813.service")
    group = "/user.slice/app.slice/" + prepared.unit_name
    digest = launcher_module._profile_sha256(launcher_module._normalise_launch_profile(_observed_worker_profile()))
    receipt = launcher_module.SandboxLaunchReceipt(prepared.launch_id, prepared.unit_name, group, "a" * 32, digest)
    return prepared, receipt


@pytest.mark.parametrize("collected", [False, True])
def test_reconstructed_acknowledged_launch_can_stop_after_restart(monkeypatch, collected):
    prepared, receipt = _reconstructed_launch()
    properties = dict(_observed_worker_profile(), Id=prepared.unit_name, ActiveState="inactive",
        LoadState="not-found" if collected else "loaded", ControlGroup="" if collected else receipt.control_group,
        InvocationID="" if collected else receipt.invocation_id)
    kills = []
    def systemctl(args, **_kwargs):
        if "kill" in args:
            kills.append(args)
            return subprocess.CompletedProcess(args, 0)
        selected = {arg.removeprefix("--property=") for arg in args if arg.startswith("--property=")}
        return subprocess.CompletedProcess(args, 0, stdout="".join(
            f"{key}={properties[key]}\n" for key in selected).encode())
    monkeypatch.setattr(launcher_module, "_systemd_client_environment", lambda: {})
    monkeypatch.setattr(subprocess, "run", systemctl)
    monkeypatch.setattr(launcher_module, "_cgroup_is_empty", lambda _path: True)
    # A newly created launcher owns no in-memory session/prepare state.
    stopped = SystemdReadOnlyLauncher(systemctl="ctl").stop_prepared_launch(prepared, receipt)
    assert stopped.unit_name == prepared.unit_name and stopped.control_group == receipt.control_group
    assert stopped.cgroup_empty


@pytest.mark.parametrize("change", [dict(InvocationID="b" * 32), dict(Id="wrong.service"),
    dict(ControlGroup="/user.slice/app.slice/other.service"), dict(PrivateNetwork="no"),
    dict(Environment="SECRET=private-secret")])
def test_recovered_stop_rejects_replaced_invocation_or_profile_before_signal(monkeypatch, change):
    prepared, receipt = _reconstructed_launch()
    properties = dict(_observed_worker_profile(), Id=prepared.unit_name, ActiveState="active",
        LoadState="loaded", ControlGroup=receipt.control_group, InvocationID=receipt.invocation_id) | change
    def systemctl(args, **_kwargs):
        assert "kill" not in args, "a substituted binding must not be signalled"
        return subprocess.CompletedProcess(args, 0, stdout="".join(f"{k}={v}\n" for k, v in properties.items()).encode())
    monkeypatch.setattr(launcher_module, "_systemd_client_environment", lambda: {})
    monkeypatch.setattr(subprocess, "run", systemctl)
    with pytest.raises(launcher_module.IsolationStopUnknown):
        SystemdReadOnlyLauncher(systemctl="ctl").stop_prepared_launch(prepared, receipt)


def test_normal_wait_rejects_replaced_invocation_even_with_empty_cgroup(monkeypatch):
    prepared, receipt = _reconstructed_launch()
    properties = dict(_observed_worker_profile(), Id=prepared.unit_name, ActiveState="inactive",
        LoadState="loaded", ControlGroup=receipt.control_group, InvocationID="b" * 32)
    monkeypatch.setattr(subprocess, "run", lambda args, **_kw: subprocess.CompletedProcess(args, 0,
        stdout="".join(f"{k}={v}\n" for k, v in properties.items()).encode()))
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", lambda **_kw:
        launcher_module.SandboxTerminationReceipt(prepared.unit_name, receipt.control_group, "inactive", True))
    session = SandboxSession(process=_completed_process(), unit_name=prepared.unit_name,
        systemctl="ctl", client_env={}, output_limit=1024, timeout_seconds=10,
        staging=_TrackingStaging(), scope_cgroup=Path("/sys/fs/cgroup") / receipt.control_group.lstrip("/"))
    session._launch_receipt = receipt
    assert not session.wait().termination_confirmed


@pytest.mark.parametrize("property_name", sorted(_observed_worker_profile()))
def test_observer_rejects_every_missing_profile_property(monkeypatch, property_name):
    prepared, receipt = _reconstructed_launch()
    profile = _observed_worker_profile()
    properties = dict(profile, Id=prepared.unit_name, ActiveState="active", LoadState="loaded",
        ControlGroup=receipt.control_group, InvocationID=receipt.invocation_id)
    del properties[property_name]
    monkeypatch.setattr(subprocess, "run", lambda args, **_kw: subprocess.CompletedProcess(args, 0,
        stdout="".join(f"{k}={v}\n" for k, v in properties.items()).encode()))
    assert launcher_module._read_launch_receipt(prepared_launch=prepared,
        expected_cgroup=Path("/sys/fs/cgroup") / receipt.control_group.lstrip("/"),
        expected_profile=launcher_module._normalise_launch_profile(profile), client_env={}, timeout_seconds=0.001) is None


def _mock_prepared_launch(monkeypatch):
    import io
    from types import SimpleNamespace
    monkeypatch.setattr(launcher_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(launcher_module, "_systemd_client_environment", lambda: {})
    monkeypatch.setattr(launcher_module, "_systemd_cgroup_parent",
        lambda *_args: Path("/sys/fs/cgroup/user.slice/app.slice"))
    monkeypatch.setattr(subprocess, "run", lambda args, **_kw: subprocess.CompletedProcess(args, 0))
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: SimpleNamespace(
        stdin=None, stdout=io.BytesIO(), stderr=io.BytesIO(), returncode=0, poll=lambda: 0))


def test_prepared_launch_returns_observed_ack_without_collecting(monkeypatch, tmp_path):
    _mock_prepared_launch(monkeypatch)
    launcher = SystemdReadOnlyLauncher(systemd_run="run", systemctl="ctl")
    prepared = launcher.prepare_launch()
    def observe(**kwargs):
        assert kwargs["prepared_launch"] == prepared
        expected = kwargs["expected_profile"]
        assert expected["PrivateNetwork"] == "yes" and expected["ProtectSystem"] == "strict"
        assert expected["CPUQuotaPerSecUSec"] == "1000000"
        assert expected["MemoryMax"] == "536870912"
        assert expected["ExecStart"][1].startswith("/usr/bin/env -i PATH=/usr/bin:/bin ")
        assert "BindReadOnlyPaths" in expected and "InaccessiblePaths" in expected
        group = "/" + kwargs["expected_cgroup"].relative_to(Path("/sys/fs/cgroup")).as_posix()
        return launcher_module.SandboxLaunchReceipt(prepared.launch_id, prepared.unit_name,
            group, "a" * 32, launcher_module._profile_sha256(expected))
    monkeypatch.setattr(launcher_module, "_read_launch_receipt", observe)
    monkeypatch.setattr(SandboxSession, "wait", lambda _self: pytest.fail("launch must return before wait"))
    session = launcher.launch(tmp_path, ["/bin/true"], prepared_launch=prepared)
    assert session.launch_receipt.launch_id == prepared.launch_id
    assert session._result is None
    # The test transport ran no untrusted command and already exited.
    session._process.stdout.close()
    session._process.stderr.close()
    launcher_module._discard_staging(session._staging)


def test_prepared_unacknowledged_launch_retains_staging_and_consumes_epoch(monkeypatch, tmp_path):
    _mock_prepared_launch(monkeypatch)
    monkeypatch.setattr(launcher_module, "_read_launch_receipt", lambda **_kwargs: None)
    launcher = SystemdReadOnlyLauncher(systemd_run="run", systemctl="ctl")
    prepared = launcher.prepare_launch()
    with pytest.raises(launcher_module.IsolationStartUnknown) as caught:
        launcher.launch(tmp_path, ["/bin/true"], prepared_launch=prepared)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    session = launcher._sessions[prepared.launch_id]
    assert session.launch_receipt is None and session._staging.path.exists()
    with pytest.raises(launcher_module.IsolationNotStarted):
        launcher.launch(tmp_path, ["/bin/true"], prepared_launch=prepared)
    # Only this test's already-completed fake transport can be discarded here.
    session._process.stdout.close()
    session._process.stderr.close()
    launcher_module._discard_staging(session._staging)


@pytest.mark.parametrize("token", ["has space", "semi;colon", "brace{", "quote\"", "back\\slash"])
def test_only_prepared_launch_rejects_ambiguous_exec_tokens_before_spawn(monkeypatch, tmp_path, token):
    _mock_prepared_launch(monkeypatch)
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("ambiguous argv must not start"))
    launcher = SystemdReadOnlyLauncher(systemd_run="run", systemctl="ctl")
    with pytest.raises(launcher_module.IsolationNotStarted):
        launcher.launch(tmp_path, ["/bin/echo", token], prepared_launch=launcher.prepare_launch())


@pytest.mark.parametrize("change", [dict(InaccessiblePaths="-/run/user"),
    dict(BindReadOnlyPaths="/tmp/source:/tmp/staging/workspace:rbind"),
    dict(RestrictAddressFamilies="AF_UNIX AF_INET"), dict(LimitNOFILESoft="1024"),
    dict(ExecStart="{ path=/bin/echo ; argv[]=/bin/echo bypass ; ignore_errors=no ; pending=yes }")])
def test_weakened_path_exec_and_resource_profiles_never_ack(monkeypatch, change):
    prepared, receipt = _reconstructed_launch()
    profile = _observed_worker_profile()
    properties = dict(profile, Id=prepared.unit_name, ActiveState="active", LoadState="loaded",
        ControlGroup=receipt.control_group, InvocationID=receipt.invocation_id) | change
    monkeypatch.setattr(subprocess, "run", lambda args, **_kw: subprocess.CompletedProcess(args, 0,
        stdout="".join(f"{k}={v}\n" for k, v in properties.items()).encode()))
    assert launcher_module._read_launch_receipt(prepared_launch=prepared,
        expected_cgroup=Path("/sys/fs/cgroup") / receipt.control_group.lstrip("/"),
        expected_profile=launcher_module._normalise_launch_profile(profile), client_env={}, timeout_seconds=0.01) is None


def test_reconstructed_live_launch_is_killed_as_whole_unit_before_stop_proof(monkeypatch):
    prepared, receipt = _reconstructed_launch()
    properties = dict(_observed_worker_profile(), Id=prepared.unit_name, ActiveState="active",
        LoadState="loaded", ControlGroup=receipt.control_group, InvocationID=receipt.invocation_id)
    killed = []
    def systemctl(args, **_kwargs):
        if "kill" in args:
            assert args[-1] == prepared.unit_name
            assert "--kill-whom=all" in args and "--signal=SIGKILL" in args
            killed.append(prepared.unit_name)
            properties["ActiveState"] = "inactive"
            return subprocess.CompletedProcess(args, 0)
        names = {arg.removeprefix("--property=") for arg in args if arg.startswith("--property=")}
        return subprocess.CompletedProcess(args, 0, stdout="".join(
            f"{key}={properties[key]}\n" for key in names).encode())
    monkeypatch.setattr(launcher_module, "_systemd_client_environment", lambda: {})
    monkeypatch.setattr(subprocess, "run", systemctl)
    monkeypatch.setattr(launcher_module, "_cgroup_is_empty", lambda _path: True)
    result = SystemdReadOnlyLauncher(systemctl="ctl").stop_prepared_launch(prepared, receipt)
    assert killed == [prepared.unit_name] and result.cgroup_empty


@pytest.mark.parametrize("fault", [None, "mismatched-binding", "unknown-binding", "populated", "final-mismatch"])
def test_recovered_stop_resamples_strict_receipt_only_after_exact_stopped_binding(monkeypatch, fault):
    prepared, receipt = _reconstructed_launch()
    state = dict(killed=False, collectors=0)
    def systemctl(args, **kwargs):
        if "kill" in args:
            assert args[-1] == prepared.unit_name and "--kill-whom=all" in args
            state["killed"] = True
            return subprocess.CompletedProcess(args, 0)
        names = {arg.removeprefix("--property=") for arg in args if arg.startswith("--property=")}
        if names == {"ActiveState", "LoadState", "ControlGroup"}:
            state["collectors"] += 1
            # Real observed transition: inactive but still loaded/blank cgroup
            # fails the unchanged standard collector. Only its next sample can
            # witness collected identity plus independently empty old cgroup.
            values = dict(ActiveState="inactive", LoadState="loaded" if state["collectors"] == 1 else "not-found",
                ControlGroup="")
        elif not state["killed"]:
            values = dict(_observed_worker_profile(), Id=prepared.unit_name, LoadState="loaded",
                ActiveState="active", ControlGroup=receipt.control_group, InvocationID=receipt.invocation_id)
        else:
            values = dict(_observed_worker_profile(), Id=prepared.unit_name, LoadState="not-found",
                ActiveState="inactive", ControlGroup="", InvocationID="")
            if fault == "unknown-binding":
                return subprocess.CompletedProcess(args, 1, stdout=b"")
            if fault == "mismatched-binding" or fault == "final-mismatch" and state["collectors"] >= 2:
                values.update(LoadState="loaded", ControlGroup=receipt.control_group, InvocationID="b" * 32)
        return subprocess.CompletedProcess(args, 0, stdout="".join(f"{key}={values[key]}\n" for key in names).encode())
    monkeypatch.setattr(launcher_module, "_systemd_client_environment", lambda: {})
    monkeypatch.setattr(subprocess, "run", systemctl)
    monkeypatch.setattr(launcher_module, "_cgroup_is_empty", lambda path: fault != "populated")
    collector_calls = []
    original_collector = launcher_module._read_termination_receipt
    def collect(**kwargs):
        collector_calls.append(kwargs)
        return original_collector(**kwargs)
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", collect)
    launcher = SystemdReadOnlyLauncher(systemctl="ctl")
    if fault is None:
        stopped = launcher.stop_prepared_launch(prepared, receipt)
        assert stopped.cgroup_empty and stopped.control_group == receipt.control_group
        assert len(collector_calls) == 2
    else:
        with pytest.raises(launcher_module.IsolationStopUnknown):
            launcher.stop_prepared_launch(prepared, receipt)
        assert len(collector_calls) == (1 if fault in {"mismatched-binding", "unknown-binding"} else 2)
    assert state["killed"]


def test_collected_systemd_unit_may_omit_execstart_but_requires_exact_stopped_identity(monkeypatch):
    prepared, receipt = _reconstructed_launch()
    # Captured real systemd shape: no ExecStart at all for a collected unit.
    properties = dict(Id=prepared.unit_name, LoadState="not-found", ActiveState="inactive",
        ControlGroup="", InvocationID="", PrivateNetwork="no")
    monkeypatch.setattr(subprocess, "run", lambda args, **_kw: subprocess.CompletedProcess(args, 0,
        stdout="".join(f"{k}={v}\n" for k, v in properties.items()).encode()))
    assert launcher_module._observe_prepared_binding(prepared, receipt, "ctl", {}) == "stopped"
    properties["InvocationID"] = "b" * 32
    assert launcher_module._observe_prepared_binding(prepared, receipt, "ctl", {}) is None


def test_loaded_inactive_without_invocation_is_polled_until_actual_launch_observation(monkeypatch):
    prepared, receipt = _reconstructed_launch()
    profile = _observed_worker_profile()
    pending = dict(profile, Id=prepared.unit_name, LoadState="loaded", ActiveState="inactive",
        ControlGroup="", InvocationID="")
    running = pending | dict(ActiveState="active", ControlGroup=receipt.control_group, InvocationID=receipt.invocation_id)
    calls = []
    def systemctl(args, **_kw):
        calls.append(args)
        value = pending if len(calls) == 1 else running
        return subprocess.CompletedProcess(args, 0, stdout="".join(f"{k}={v}\n" for k, v in value.items()).encode())
    monkeypatch.setattr(subprocess, "run", systemctl)
    observed = launcher_module._read_launch_receipt(prepared_launch=prepared,
        expected_cgroup=Path("/sys/fs/cgroup") / receipt.control_group.lstrip("/"),
        expected_profile=launcher_module._normalise_launch_profile(profile), client_env={}, timeout_seconds=0.1)
    assert observed == receipt and len(calls) == 2


@pytest.mark.parametrize(("value", "microseconds"), [("1.250000s", "1250000"),
    ("0.001ms", "1"), ("1.5ms", "1500"), ("1min 0.5s", "60500000"),
    ("0.000001s", "1"), ("24h", "86400000000"), ("0.000000s", "0")])
def test_effective_resource_timespans_normalize_finite_fractions_exactly(value, microseconds):
    assert launcher_module._duration_usec(value) == microseconds


@pytest.mark.parametrize("value", ["NaN", "Infinity", "inf", "1e999s", "-1s", "+1s",
    "0.1us", "1.0000001s", "1.0000000s", "24h 1us", "999999999d",
    "18446744073709551615us", "1.2.3s", "9" * 200 + "s"])
def test_resource_timespans_reject_nonfinite_negative_submicrosecond_and_overflow(value):
    with pytest.raises(ValueError):
        launcher_module._duration_usec(value)


@pytest.mark.parametrize("field", ["launch_id", "unit_name", "control_group", "invocation_id", "profile_sha256"])
def test_constructed_launch_receipt_fields_rejected_before_any_callback(monkeypatch, field):
    from dataclasses import replace
    calls = []
    class Hostile:
        def __ne__(self, other):
            calls.append("ne")
            return False
        def __eq__(self, other):
            calls.append("eq")
            return True
        def __str__(self):
            calls.append("str")
            return "private-receipt-sentinel"
        def __fspath__(self):
            calls.append("fspath")
            return "/private-receipt-sentinel"
    prepared, receipt = _reconstructed_launch()
    unsafe = replace(receipt, **{field: Hostile()})
    with pytest.raises(ValueError):
        launcher_module._validate_launch_receipt(prepared, unsafe)
    assert calls == []
    monkeypatch.setattr(launcher_module, "_systemd_client_environment",
        lambda: pytest.fail("invalid receipt must fail before consulting the host"))
    with pytest.raises(launcher_module.IsolationStartUnknown) as caught:
        SystemdReadOnlyLauncher().stop_prepared_launch(prepared, unsafe)
    assert calls == []
    assert caught.value.__cause__ is None and caught.value.__context__ is None
    assert "private-" not in str(caught.value) + repr(caught.value)


def _local_prepared_lifecycle(monkeypatch):
    # Use actual bounded pipe collection; replace only systemd observations
    # and the systemd transport with a controlled local echo interpreter.
    popen = subprocess.Popen
    _mock_prepared_launch(monkeypatch)
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kw: popen(
        ["/usr/bin/python3", "-c", "import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0))
    def acknowledge(**kw):
        prepared = kw["prepared_launch"]
        group = "/" + kw["expected_cgroup"].relative_to(Path("/sys/fs/cgroup")).as_posix()
        return launcher_module.SandboxLaunchReceipt(prepared.launch_id, prepared.unit_name,
            group, "a" * 32, launcher_module._profile_sha256(kw["expected_profile"]))
    def stopped(**kw):
        return launcher_module.SandboxTerminationReceipt(kw["unit_name"],
            "/" + kw["expected_cgroup"].relative_to(Path("/sys/fs/cgroup")).as_posix(), "inactive", True)
    monkeypatch.setattr(launcher_module, "_read_launch_receipt", acknowledge)
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", stopped)
    monkeypatch.setattr(launcher_module, "_observe_prepared_binding", lambda *_args: "stopped")
    return stopped


def test_confirmed_stopped_sessions_do_not_accumulate_or_retain_input(monkeypatch, tmp_path):
    import weakref
    _local_prepared_lifecycle(monkeypatch)
    launcher = SystemdReadOnlyLauncher(systemd_run="run", systemctl="ctl")
    for _ in range(8):
        prepared = launcher.prepare_launch()
        payload = b"private-input-sentinel" * 1024
        session = launcher.launch(tmp_path, ["/usr/bin/cat"], prepared_launch=prepared, input_bytes=payload)
        result = session.wait()
        try:
            assert result.stdout == payload and result.termination_confirmed
            assert session.wait() is result
            assert prepared.launch_id not in launcher._sessions
            assert session._input_bytes is None
            assert prepared.launch_id in launcher._consumed
            reference = weakref.ref(session)
        finally:
            # Every test transport is controlled and has already exited.
            launcher_module._discard_staging(session._staging)
        del session, result
        assert reference() is None
    assert not launcher._sessions


def test_unknown_sessions_remain_strongly_retained_with_no_retry(monkeypatch, tmp_path):
    import weakref
    _local_prepared_lifecycle(monkeypatch)
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", lambda **_kw: None)
    launcher = SystemdReadOnlyLauncher(systemd_run="run", systemctl="ctl")
    prepared = launcher.prepare_launch()
    payload = b"private-unknown-input"
    session = launcher.launch(tmp_path, ["/usr/bin/cat"], prepared_launch=prepared, input_bytes=payload)
    result = session.wait()
    assert not result.termination_confirmed and session.wait() is result
    reference = weakref.ref(session)
    del session
    assert reference() is launcher._sessions[prepared.launch_id]
    assert reference()._input_bytes == payload
    with pytest.raises(launcher_module.IsolationNotStarted):
        launcher.launch(tmp_path, ["/usr/bin/cat"], prepared_launch=prepared)
    launcher_module._discard_staging(reference()._staging)  # Controlled local test transport only.


def test_fresh_recovered_stop_retires_prior_cached_unknown_and_replays_wait(monkeypatch, tmp_path):
    stopped = _local_prepared_lifecycle(monkeypatch)
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", lambda **_kw: None)
    launcher = SystemdReadOnlyLauncher(systemd_run="run", systemctl="ctl")
    prepared = launcher.prepare_launch()
    session = launcher.launch(tmp_path, ["/usr/bin/cat"], prepared_launch=prepared, input_bytes=b"private-recovery-input")
    cached = session.wait()
    assert not cached.termination_confirmed and prepared.launch_id in launcher._sessions
    receipt = session.launch_receipt
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", stopped)
    fresh = launcher.stop_prepared_launch(prepared, receipt)
    assert fresh.cgroup_empty and prepared.launch_id not in launcher._sessions
    assert session._input_bytes is None
    assert session.wait() is cached
    assert launcher.stop_prepared_launch(prepared, receipt) == fresh
    with pytest.raises(launcher_module.IsolationNotStarted):
        launcher.launch(tmp_path, ["/usr/bin/cat"], prepared_launch=prepared)
