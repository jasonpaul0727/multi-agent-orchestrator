"""Connection-bound observational adapter; no launch or stop authority."""
import threading
from types import SimpleNamespace
import pytest

from orchestrator.isolation import _systemd_job_adapter as adapter
from orchestrator.isolation._systemd_job_monitor import JobRemovedEvent

OWNER, EPOCH = ":1.42", 5
UNIT = "own.service"
PATH = "/org/freedesktop/systemd1/job/19"

class Monitor:
    def __init__(self):
        self.owner, self.epoch, self.unknown = OWNER, EPOCH, False
        self.batches = []
        self.closed = False
    def poll(self, timeout_ms=0, *, operation_timeout_ms=1000):
        return self.batches.pop(0) if self.batches else ()
    def close(self):
        self.closed = self.unknown = True

def setup(monkeypatch, **kwargs):
    monitor = Monitor()
    monkeypatch.setattr(adapter.JobMonitor, "open", lambda **limits: monitor)
    instance = adapter.JobMonitorCorrelation.open(UNIT, **kwargs)
    assert instance is not None
    return instance, monitor

def event(job=19, unit=UNIT, sender=OWNER, epoch=EPOCH):
    return JobRemovedEvent(sender, epoch, job, f"/org/freedesktop/systemd1/job/{job}", unit, "done")

def test_early_signal_and_unrelated_then_reply(monkeypatch):
    instance, monitor = setup(monkeypatch)
    monitor.batches.append((event(20, "noise.socket"), event()))
    assert instance.poll() is None
    assert instance.reply(19, PATH) is None
    assert instance.poll().job_path == PATH
    instance.close()
    assert instance.poll() is None

def test_lost_reply_timeout_invalidates(monkeypatch):
    instance, monitor = setup(monkeypatch)
    monitor.batches.append((event(),))
    assert instance.poll() is None
    instance.timeout()
    instance.reply(19, PATH)
    assert instance.poll() is None and instance.unknown
    instance.close()

@pytest.mark.parametrize("failure", ["unknown", "owner", "epoch", "none", "sender", "event_epoch", "exception"])
def test_connection_failure_invalidates_completed_observation(monkeypatch, failure):
    instance, monitor = setup(monkeypatch)
    instance.reply(19, PATH)
    monitor.batches.append((event(),))
    assert instance.poll() is not None
    if failure == "unknown": monitor.unknown = True
    elif failure == "owner": monitor.owner = ":1.43"
    elif failure == "epoch": monitor.epoch += 1
    elif failure == "none": monitor.batches.append(None)
    elif failure == "sender": monitor.batches.append((event(20, "noise.socket", sender=":1.43"),))
    elif failure == "event_epoch": monitor.batches.append((event(20, "noise.socket", epoch=6),))
    else:
        def broken(timeout_ms=0, *, operation_timeout_ms=1000): raise OSError("disconnect")
        monitor.poll = broken
    assert instance.poll() is None and instance.unknown
    instance.close()

def test_overflow_and_close(monkeypatch):
    instance, monitor = setup(monkeypatch, max_events=1)
    monitor.batches.append((event(20, "a.socket"), event(21, "b.socket")))
    assert instance.poll() is None and instance.unknown
    instance.close()
    assert monitor.closed

def test_poll_and_close_are_serialized(monkeypatch):
    instance, monitor = setup(monkeypatch)
    entered, release = threading.Event(), threading.Event()
    def blocked(timeout_ms=0, *, operation_timeout_ms=1000):
        entered.set()
        assert release.wait(5)
        return ()
    monitor.poll = blocked
    t1 = threading.Thread(target=instance.poll)
    t2 = threading.Thread(target=instance.close)
    t1.start()
    assert entered.wait(5)
    t2.start()
    release.set()
    t1.join(5); t2.join(5)
    assert not t1.is_alive() and not t2.is_alive()
    assert instance.unknown and monitor.closed


def test_real_manager_job_property_correlates_independent_signal():
    import errno
    import os
    import subprocess
    import tempfile
    import time
    import uuid
    from tests.integration.test_mediation_parent_cgroup_capability import require_manager
    env, _, _ = require_manager()
    unit = "maestroadapter" + uuid.uuid4().hex + ".service"
    instance = adapter.JobMonitorCorrelation.open(unit)
    assert instance is not None
    fifo_dir = None
    try:
        fifo_dir = tempfile.TemporaryDirectory(prefix="maestro-job-")
        fifo = os.path.join(fifo_dir.name, "release")
        os.mkfifo(fifo)
        run = subprocess.run(["systemd-run", "--user", "--quiet", "--no-block",
            "--unit=" + unit.removesuffix(".service"), "--property=Type=oneshot",
            "--", "/usr/bin/cat", fifo], env=env, capture_output=True, timeout=5)
        assert run.returncode == 0, run.stderr
        import shlex
        unit_reply = subprocess.run(["busctl", "--user", "call", "org.freedesktop.systemd1",
            "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "GetUnit", "s", unit],
            env=env, capture_output=True, text=True, timeout=5)
        assert unit_reply.returncode == 0, unit_reply.stderr
        unit_fields = shlex.split(unit_reply.stdout)
        assert len(unit_fields) == 2 and unit_fields[0] == "o", unit_reply.stdout
        job_reply = subprocess.run(["busctl", "--user", "get-property", "org.freedesktop.systemd1",
            unit_fields[1], "org.freedesktop.systemd1.Unit", "Job"], env=env,
            capture_output=True, text=True, timeout=5)
        assert job_reply.returncode == 0, job_reply.stderr
        fields = shlex.split(job_reply.stdout)
        assert len(fields) == 3 and fields[0] == "(uo)", f"manager Job property unavailable: {job_reply.stdout!r}"
        job_id, path = int(fields[1]), fields[2]
        assert path == "/org/freedesktop/systemd1/job/" + str(job_id)
        instance.reply(job_id, path)
        # The oneshot start job cannot finish before the FIFO reader sees EOF.
        # Nonblocking open confirms the unit has opened the read side; closing
        # this writer deliberately releases that exact unit.
        release_deadline = time.monotonic() + 5
        while True:
            try:
                writer = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK | os.O_CLOEXEC)
                break
            except OSError as exc:
                if exc.errno != errno.ENXIO or time.monotonic() >= release_deadline:
                    raise
                time.sleep(0.01)
        try:
            assert instance.poll(0) is None and not instance.unknown
        finally:
            os.close(writer)
        deadline = time.monotonic() + 5
        observation = None
        while time.monotonic() < deadline and observation is None and not instance.unknown:
            observation = instance.poll(200)
        assert observation is not None and observation.job_path == path
        assert observation.unit == unit and observation.result == "done"
    finally:
        import sys
        original = sys.exception()
        errors = []
        try: instance.close()
        except BaseException as exc: errors.append("close failed: " + repr(exc))
        try:
            stop = subprocess.run(["systemctl", "--user", "stop", unit], env=env,
                capture_output=True, timeout=5)
            show = subprocess.run(["systemctl", "--user", "show", unit,
                "--property=LoadState", "--property=ActiveState"], env=env,
                capture_output=True, text=True, timeout=5)
            state = dict(line.split("=", 1) for line in show.stdout.splitlines() if "=" in line)
            if show.returncode or state.get("ActiveState") not in {"inactive", "failed"} or (
                stop.returncode and state.get("LoadState") != "not-found"):
                errors.append("unit cleanup unverified")
        except Exception as exc: errors.append("unit cleanup failed: " + repr(exc))
        try:
            if fifo_dir is not None: fifo_dir.cleanup()
        except Exception as exc: errors.append("barrier cleanup failed: " + repr(exc))
        if errors:
            if original is not None: original.add_note("; ".join(errors))
            else: raise AssertionError("; ".join(errors))


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_interrupt_invalidates_and_propagates(monkeypatch, error_type):
    instance, monitor = setup(monkeypatch)
    error = error_type("interrupted")
    def broken(timeout_ms=0, *, operation_timeout_ms=1000): raise error
    monitor.poll = broken
    try:
        with pytest.raises(error_type) as caught: instance.poll()
        assert caught.value is error and instance.unknown
    finally:
        instance.close()


def test_poll_forwards_budget_and_failure_closes(monkeypatch):
    instance, monitor = setup(monkeypatch)
    calls = []
    def poll(timeout_ms=0, *, operation_timeout_ms=1000):
        calls.append((timeout_ms, operation_timeout_ms))
        return None
    monitor.poll = poll
    assert instance.poll(10, operation_timeout_ms=25) is None
    assert calls == [(10, 25)] and instance.unknown
    instance.close()
    assert monitor.closed


def test_close_failure_invalidates_before_propagating(monkeypatch):
    instance, monitor = setup(monkeypatch)
    instance.reply(19, PATH)
    monitor.batches.append((event(),))
    assert instance.poll() is not None
    def close(): raise OSError("cleanup")
    monitor.close = close
    with pytest.raises(OSError): instance.close()
    assert instance.unknown and instance.poll() is None


def test_failed_open_returns_none(monkeypatch):
    monkeypatch.setattr(adapter.JobMonitor, "open", lambda **limits: None)
    assert adapter.JobMonitorCorrelation.open(UNIT) is None


def test_monitor_status_failure_invalidates(monkeypatch):
    instance, monitor = setup(monkeypatch)
    instance.reply(19, PATH)
    monitor.batches.append((event(),))
    assert instance.poll() is not None
    class BrokenStatus(Monitor):
        @property
        def unknown(self): raise OSError("status unavailable")
        @unknown.setter
        def unknown(self, value): pass
    monitor.__class__ = BrokenStatus
    assert instance.poll() is None and instance.unknown
    instance.close()


@pytest.mark.parametrize("original_type,cleanup_type,expected_type", [
    (ValueError, OSError, ValueError),
    (KeyboardInterrupt, OSError, KeyboardInterrupt),
    (ValueError, KeyboardInterrupt, KeyboardInterrupt),
    (KeyboardInterrupt, SystemExit, KeyboardInterrupt),
])
def test_open_failure_preserves_exception_priority(monkeypatch, original_type, cleanup_type, expected_type):
    monitor = Monitor()
    original, cleanup = original_type("construct"), cleanup_type("cleanup")
    monkeypatch.setattr(adapter.JobMonitor, "open", lambda **limits: monitor)
    def fail_init(self, *args, **kwargs): raise original
    def fail_close(): raise cleanup
    monkeypatch.setattr(adapter.JobMonitorCorrelation, "__init__", fail_init)
    monitor.close = fail_close
    with pytest.raises(expected_type) as caught:
        adapter.JobMonitorCorrelation.open(UNIT)
    expected = cleanup if expected_type is cleanup_type and original_type is not cleanup_type else original
    assert caught.value is expected
    assert any("failure" in note for note in caught.value.__notes__)
