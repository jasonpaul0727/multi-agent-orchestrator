"""Deterministic startup-deadline tests for the recovery race fixtures."""

import traceback

import pytest

from tests.integration import test_recovery_continuation as recovery


class Clock:
    def __init__(self):
        self.value = 100.0

    def now(self):
        return self.value


class Pipe:
    def __init__(self, pid, clock):
        self.pid = pid
        self.clock = clock
        self.messages = []
        self.sent = []
        self.closed = False

    def recv(self):
        return self.messages.pop(0)

    def send(self, message):
        self.sent.append((self.clock.now(), message))

    def close(self):
        self.closed = True


class Process:
    def __init__(self, pid, clock):
        self.pid = pid
        self.clock = clock
        self.sentinel = object()
        self.exitcode = None
        self.started = False

    def start(self):
        self.started = True
        self.clock.value += 1.0


class Schedule:
    def __init__(self, clock, events, parents):
        self.clock = clock
        self.events = list(events)
        self.parents = parents
        self.timeouts = []

    def legacy_receive(self, pipe, *, timeout_seconds=15.0):
        deadline = self.clock.value + timeout_seconds
        while self.events and self.events[0][0] <= deadline:
            at, _, callback = self.events.pop(0)
            self.clock.value = max(self.clock.value, at)
            callback()
            if pipe.messages:
                return pipe.recv()
        if pipe.messages:
            return pipe.recv()
        self.clock.value = deadline
        raise AssertionError("process rendezvous timed out")

    def wait(self, handles, timeout):
        self.timeouts.append(timeout)
        # No child may receive the startup acknowledgement before both are ready.
        assert all(not any(message == "startup_ack" for _, message in p.sent) for p in self.parents)
        if self.events and self.events[0][0] <= self.clock.value + timeout:
            at, handle, callback = self.events.pop(0)
            assert handle in handles
            self.clock.value = at
            callback()
            return [handle]
        self.clock.value += timeout
        return []


def fixture(order):
    clock = Clock()
    parents = [Pipe(101, clock), Pipe(102, clock)]
    children = [Pipe(101, clock), Pipe(102, clock)]
    processes = [Process(101, clock), Process(102, clock)]
    events = []
    for at, index in zip((103.0, 114.0), order):
        pipe = parents[index]
        events.append((at, pipe, lambda p=pipe: p.messages.append(("ready", p.pid))))
    return clock, parents, children, processes, Schedule(clock, events, parents)


def run_start(monkeypatch, clock, parents, children, processes, schedule):
    monkeypatch.setattr(recovery, "_clock", clock.now, raising=False)
    monkeypatch.setattr(recovery, "_wait_connections", schedule.wait, raising=False)
    # Existing code's sequential wait uses this legacy fake, making the
    # behavioral RED an assertion failure rather than an undefined API.
    monkeypatch.setattr(recovery, "receive_process_message", schedule.legacy_receive)
    recovery._start_pair(parents, children, processes)

@pytest.mark.parametrize("order", [(0, 1), (1, 0)], ids=["first-fast", "second-fast"])
def test_shared_deadline_accepts_both_start_orders_and_acks_only_when_ready(order, monkeypatch):
    clock, parents, children, processes, schedule = fixture(order)
    run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert all(p.started for p in processes) and all(c.closed for c in children)
    assert schedule.timeouts == [13.0, 12.0]
    for pipe in parents:
        assert len(pipe.sent) == 2  # deadlines and acknowledgement are both required
        assert pipe.sent[0][1][0] == "startup_deadlines"
        assert pipe.sent[0][1][1] == 115.0  # computed before either start call
        assert pipe.sent[-1] == (114.0, "startup_ack")


def test_startup_timeout_uses_one_total_deadline_and_never_acks(monkeypatch):
    clock, parents, children, processes, schedule = fixture((0, 1))
    schedule.events = schedule.events[:1]
    with pytest.raises(AssertionError, match=r"phase=startup.*child=1.*pid=102"):
        run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert clock.now() == 115.0
    assert schedule.timeouts == [13.0, 12.0]
    assert all(not any(message == "startup_ack" for _, message in p.sent) for p in parents)


def test_child_exit_before_ready_identifies_exact_process_without_ack(monkeypatch):
    clock, parents, children, processes, schedule = fixture((0, 1))
    schedule.events = [(103.0, processes[1].sentinel,
                        lambda: setattr(processes[1], "exitcode", 7))]
    with pytest.raises(AssertionError, match=r"phase=startup.*child=1.*pid=102.*exitcode=7"):
        run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert all(not any(message == "startup_ack" for _, message in p.sent) for p in parents)


def test_already_ready_child_exit_before_peer_ready_is_not_acknowledged(monkeypatch):
    clock, parents, children, processes, schedule = fixture((0, 1))
    schedule.events = [schedule.events[0], (104.0, processes[0].sentinel,
                                           lambda: setattr(processes[0], "exitcode", 9))]
    with pytest.raises(AssertionError, match=r"phase=startup.*child=0.*pid=101.*exitcode=9"):
        run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert all(not any(message == "startup_ack" for _, message in p.sent) for p in parents)


def test_startup_timeout_reports_last_child_phase(monkeypatch):
    clock, parents, children, processes, schedule = fixture((0, 1))
    schedule.events = [schedule.events[0], (104.0, parents[1], lambda: parents[1].messages.append(
        ("phase", "opening_application", 102)))]
    with pytest.raises(AssertionError, match=r"phase=startup.*child=1.*pid=102.*child_phase=opening_application.*reason=deadline"):
        run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert clock.now() == 115.0


class TimedPipe:
    def __init__(self, clock, events):
        self.clock = clock
        self.events = list(events)
        self.pending = None
        self.timeouts = []

    def poll(self, timeout):
        self.timeouts.append(timeout)
        if self.events and self.events[0][0] <= self.clock.value + timeout:
            at, self.pending = self.events.pop(0)
            self.clock.value = at
            return True
        self.clock.value += timeout
        return False

    def recv(self):
        return self.pending


def test_child_waits_for_ack_before_start_and_uses_shared_execution_deadline(monkeypatch):
    clock = Clock()
    pipe = TimedPipe(clock, [(114.0, "startup_ack"), (116.0, "start")])
    monkeypatch.setattr(recovery, "_clock", clock.now)
    recovery._await_pair_start(pipe, 115.0, 205.0)
    assert pipe.timeouts == [15.0, 91.0]
    assert clock.now() == 116.0


def test_child_start_wait_is_bounded_by_parent_execution_deadline(monkeypatch):
    clock = Clock()
    pipe = TimedPipe(clock, [(114.0, "startup_ack")])
    monkeypatch.setattr(recovery, "_clock", clock.now)
    with pytest.raises(AssertionError, match=r"phase=start .*deadline expired"):
        recovery._await_pair_start(pipe, 115.0, 205.0)
    assert clock.now() == 205.0


def test_child_cannot_start_without_startup_ack(monkeypatch):
    clock = Clock()
    pipe = TimedPipe(clock, [(101.0, "start")])
    monkeypatch.setattr(recovery, "_clock", clock.now)
    with pytest.raises(AssertionError):
        recovery._await_pair_start(pipe, 115.0, 205.0)
    assert len(pipe.timeouts) == 1


def test_cleanup_attempts_both_owned_children_even_when_first_reap_fails(monkeypatch):
    clock, parents, children, processes, _ = fixture((0, 1))
    reaped = []

    def reap(process):
        reaped.append(process.pid)
        if process is processes[0]:
            raise AssertionError("first child cleanup unconfirmed")

    monkeypatch.setattr(recovery, "reap_process", reap)
    with pytest.raises(AssertionError, match="first child cleanup unconfirmed"):
        recovery._close_pair(parents, children, processes)
    assert reaped == [101, 102]
    assert all(pipe.closed for pipe in (*parents, *children))


def test_child_rejects_expired_deadline_before_opening_database(monkeypatch):
    clock = Clock()
    clock.value = 115.0
    monkeypatch.setattr(recovery, "_clock", clock.now)
    monkeypatch.setattr(recovery, "_receive_until", lambda pipe, deadline, *, phase: ("startup_deadlines", 115.0, 205.0))
    with pytest.raises(AssertionError, match=r"phase=startup_deadlines deadline expired"):
        recovery._receive_pair_deadlines(object())


def test_ack_pipe_failure_reports_child_without_sending_start(monkeypatch):
    clock, parents, children, processes, schedule = fixture((0, 1))
    original_send = parents[1].send

    def fail_ack(message):
        if message == "startup_ack":
            raise BrokenPipeError("private pipe details")
        original_send(message)

    parents[1].send = fail_ack
    with pytest.raises(AssertionError, match=r"phase=startup.*child=1.*pid=102.*reason=ack_send_failed") as failure:
        run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert "private pipe details" not in str(failure.value)
    assert all(not any(message == "start" for _, message in p.sent) for p in parents)


@pytest.mark.parametrize("failure_site", ["reap", "pipe_close"])
def test_primary_startup_failure_survives_cleanup_failure(monkeypatch, failure_site):
    _, parents, children, processes, _ = fixture((0, 1))
    reaped = []

    def reap(process):
        reaped.append(process.pid)
        if failure_site == "reap" and process is processes[0]:
            raise OSError("synthetic-private-cleanup-detail")

    if failure_site == "pipe_close":
        def failed_close():
            parents[0].closed = True
            raise OSError("synthetic-private-cleanup-detail")
        parents[0].close = failed_close
    monkeypatch.setattr(recovery, "reap_process", reap)
    with pytest.raises(AssertionError, match=r"phase=startup child=1 pid=102") as caught:
        try:
            raise AssertionError("phase=startup child=1 pid=102 reason=deadline")
        finally:
            recovery._close_pair(parents, children, processes)
    assert reaped == [101, 102]
    assert all(pipe.closed for pipe in (*parents, *children))
    assert caught.value.__notes__ == ["pair_cleanup_unconfirmed"]
    assert "synthetic-private-cleanup-detail" not in repr(caught.value.__notes__)
    assert "synthetic-private-cleanup-detail" not in "".join(traceback.format_exception(caught.value))


def test_startup_pipe_read_error_reports_safe_child_phase(monkeypatch):
    clock, parents, children, processes, schedule = fixture((0, 1))

    def fail_read():
        raise ConnectionResetError("synthetic-private-pipe-detail")

    parents[0].recv = fail_read
    with pytest.raises(AssertionError, match=r"phase=startup.*child=0.*pid=101.*reason=pipe_read_failed") as caught:
        run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert "synthetic-private-pipe-detail" not in str(caught.value)
    assert "synthetic-private-pipe-detail" not in "".join(traceback.format_exception(caught.value))
    assert all(not any(message == "startup_ack" for _, message in p.sent) for p in parents)


@pytest.mark.parametrize("failure_site", ["poll", "recv"])
def test_child_pipe_read_errors_report_safe_phase(monkeypatch, failure_site):
    clock = Clock()
    monkeypatch.setattr(recovery, "_clock", clock.now)

    class FailedPipe:
        def poll(self, timeout):
            if failure_site == "poll":
                raise ConnectionResetError("synthetic-private-pipe-detail")
            return True

        def recv(self):
            raise OSError("synthetic-private-pipe-detail")

    with pytest.raises(AssertionError, match=r"phase=start .*pipe_read_failed") as caught:
        recovery._receive_until(FailedPipe(), 115.0, phase="start")
    assert "synthetic-private-pipe-detail" not in str(caught.value)
    assert "synthetic-private-pipe-detail" not in "".join(traceback.format_exception(caught.value))
    assert clock.now() == 100.0


@pytest.mark.parametrize("primary", [False, True], ids=["no-primary", "ordinary-primary"])
@pytest.mark.parametrize("signal", ["interrupt", "exit"])
def test_cleanup_propagates_interruption_after_attempting_all_children_and_pipes(monkeypatch, primary, signal):
    _, parents, children, processes, _ = fixture((0, 1))
    reaped = []
    interruption = KeyboardInterrupt() if signal == "interrupt" else SystemExit(23)

    def reap(process):
        reaped.append(process.pid)
        if process is processes[0]:
            raise OSError("synthetic-private-cleanup-detail")
        raise interruption

    def failed_close():
        parents[0].closed = True
        raise OSError("synthetic-private-pipe-detail")

    parents[0].close = failed_close
    monkeypatch.setattr(recovery, "reap_process", reap)
    with pytest.raises(type(interruption)) as caught:
        try:
            if primary:
                raise AssertionError("phase=startup child=1 pid=102 reason=deadline")
        finally:
            recovery._close_pair(parents, children, processes)
    assert caught.value is interruption
    if signal == "exit":
        assert caught.value.code == 23
    assert reaped == [101, 102]
    assert all(pipe.closed for pipe in (*parents, *children))
    formatted = "".join(traceback.format_exception(caught.value))
    assert "synthetic-private-cleanup-detail" not in formatted
    assert "synthetic-private-pipe-detail" not in formatted


def test_startup_wait_oserror_is_redacted_and_fails(monkeypatch):
    clock, parents, children, processes, schedule = fixture((0, 1))

    def fail_wait(handles, timeout):
        raise OSError("synthetic-private-wait-detail")

    schedule.wait = fail_wait
    with pytest.raises(AssertionError, match=r"phase=startup.*reason=wait_failed") as caught:
        run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert "synthetic-private-wait-detail" not in "".join(traceback.format_exception(caught.value))
    assert all(not any(message == "startup_ack" for _, message in p.sent) for p in parents)


def test_startup_wait_does_not_swallow_interrupt(monkeypatch):
    clock, parents, children, processes, schedule = fixture((0, 1))
    interruption = KeyboardInterrupt()

    def interrupt_wait(handles, timeout):
        raise interruption

    schedule.wait = interrupt_wait
    with pytest.raises(KeyboardInterrupt) as caught:
        run_start(monkeypatch, clock, parents, children, processes, schedule)
    assert caught.value is interruption
    assert all(not any(message == "startup_ack" for _, message in p.sent) for p in parents)


@pytest.mark.parametrize("signal", ["interrupt", "exit", "generator"])
def test_existing_interruption_survives_ordinary_cleanup_failure(monkeypatch, signal):
    _, parents, children, processes, _ = fixture((0, 1))
    reaped = []
    interruption = {"interrupt": KeyboardInterrupt(), "exit": SystemExit(23), "generator": GeneratorExit()}[signal]

    def reap(process):
        reaped.append(process.pid)
        if process is processes[0]:
            raise OSError("synthetic-private-cleanup-detail")

    monkeypatch.setattr(recovery, "reap_process", reap)
    with pytest.raises(type(interruption)) as caught:
        try:
            raise interruption
        finally:
            recovery._close_pair(parents, children, processes)
    assert caught.value is interruption
    assert caught.value.__notes__ == ["pair_cleanup_unconfirmed"]
    assert reaped == [101, 102]
    assert all(pipe.closed for pipe in (*parents, *children))
    assert "synthetic-private-cleanup-detail" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("primary", [False, True], ids=["no-primary", "ordinary-primary"])
@pytest.mark.parametrize("signal", ["interrupt", "exit", "generator"])
@pytest.mark.parametrize("source", ["reap", "close"])
def test_only_cleanup_control_flow_is_marked_unconfirmed(monkeypatch, primary, signal, source):
    _, parents, children, processes, _ = fixture((0, 1))
    reaped = []
    interruption = {"interrupt": KeyboardInterrupt(), "exit": SystemExit(23), "generator": GeneratorExit()}[signal]

    def reap(process):
        reaped.append(process.pid)
        if source == "reap" and process is processes[0]:
            raise interruption

    closed = []
    for index, pipe in enumerate((*parents, *children)):
        original_close = pipe.close

        def close(index=index, original_close=original_close):
            closed.append(index)
            if source == "close" and index == 0:
                raise interruption
            original_close()

        pipe.close = close

    monkeypatch.setattr(recovery, "reap_process", reap)
    with pytest.raises(type(interruption)) as caught:
        try:
            if primary:
                raise AssertionError("phase=startup reason=deadline")
        finally:
            recovery._close_pair(parents, children, processes)
    assert caught.value is interruption
    if signal == "exit":
        assert caught.value.code == 23
    assert caught.value.__notes__ == ["pair_cleanup_unconfirmed"]
    assert reaped == [101, 102]
    assert closed == [0, 1, 2, 3]
    assert all(pipe.closed for pipe in (*parents, *children)[1:])


@pytest.mark.parametrize("primary", [False, True], ids=["no-primary", "control-flow-primary"])
def test_multiple_cleanup_control_flows_preserve_primary_or_first(monkeypatch, primary):
    _, parents, children, processes, _ = fixture((0, 1))
    reaped = []
    closed = []
    original = GeneratorExit()
    first = KeyboardInterrupt()
    second = SystemExit(23)

    def reap(process):
        reaped.append(process.pid)
        raise first if process is processes[0] else second

    for index, pipe in enumerate((*parents, *children)):
        original_close = pipe.close

        def close(index=index, original_close=original_close):
            closed.append(index)
            original_close()

        pipe.close = close

    monkeypatch.setattr(recovery, "reap_process", reap)
    # An active control-flow exception takes priority; otherwise retain only the first cleanup interruption.
    expected = original if primary else first
    with pytest.raises(type(expected)) as caught:
        try:
            if primary:
                raise original
        finally:
            recovery._close_pair(parents, children, processes)
    assert caught.value is expected
    assert caught.value.__notes__ == ["pair_cleanup_unconfirmed"]
    assert reaped == [101, 102]
    assert closed == [0, 1, 2, 3]
    assert all(pipe.closed for pipe in (*parents, *children))
