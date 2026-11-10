"""Read-only sd-bus monitoring; no production authority is inferred."""
import os
import shutil
import subprocess
import threading
import time
import uuid

import pytest

from orchestrator.isolation._systemd_job_monitor import JobMonitor, _EventBuffer
from tests.integration.test_mediation_parent_cgroup_capability import require_manager


def test_buffer_limits_malformed_and_owner_change_are_sticky_unknown():
    buffer = _EventBuffer(":1.42", 1, 80)
    buffer.add(":1.42", 19, "/org/freedesktop/systemd1/job/19", "own.service", "done")
    assert not buffer.unknown
    buffer.add(":1.42", 20, "/org/freedesktop/systemd1/job/20", "other.socket", "done")
    assert buffer.unknown and buffer.drain() is None
    buffer = _EventBuffer(":1.42", 4, 1024)
    buffer.add(":1.43", 19, "/org/freedesktop/systemd1/job/19", "own.service", "done")
    assert buffer.unknown
    buffer = _EventBuffer(":1.42", 4, 1024)
    buffer.add(":1.42", 20, "/org/freedesktop/systemd1/job/19", "own.service", "done")
    assert buffer.unknown
    buffer = _EventBuffer(":1.42", 4, 1024)
    buffer.owner_changed(":1.43")
    buffer.owner_changed(":1.42")
    assert buffer.unknown and buffer.drain() is None


def test_close_and_poll_are_serialized_without_network():
    monitor = JobMonitor.open()
    if monitor is None:
        if os.environ.get("MEDIATION_SANDBOX_REQUIRED") == "1": pytest.fail("sd-bus manager unavailable")
        pytest.skip("sd-bus user manager unavailable")
    barrier = threading.Barrier(3)
    results = []
    def polling():
        barrier.wait()
        results.append(monitor.poll(100))
    def closing():
        barrier.wait()
        monitor.close()
    threads = [threading.Thread(target=polling), threading.Thread(target=closing)]
    for thread in threads: thread.start()
    barrier.wait()
    for thread in threads: thread.join(5)
    assert all(not thread.is_alive() for thread in threads)
    assert monitor.unknown and monitor.poll(0) is None


def test_real_monitor_observes_another_process_owned_job():
    env, _, _ = require_manager()
    monitor = JobMonitor.open()
    assert monitor is not None
    owner = monitor.owner
    unit = "maestroprobe" + uuid.uuid4().hex + ".service"
    try:
        result = subprocess.run(["systemd-run", "--user", "--quiet", "--no-block",
            "--unit=" + unit.removesuffix(".service"), "--", "/usr/bin/sleep", "1"],
            env=env, capture_output=True, timeout=5)
        assert result.returncode == 0, result.stderr
        deadline = time.monotonic() + 5
        observed = []
        while time.monotonic() < deadline and not any(event.unit == unit for event in observed):
            batch = monitor.poll(200)
            assert batch is not None
            observed.extend(batch)
        matching = [event for event in observed if event.unit == unit]
        assert matching and matching[0].sender == owner
        assert matching[0].path == "/org/freedesktop/systemd1/job/" + str(matching[0].job_id)
        assert matching[0].result == "done"
    finally:
        import sys
        original = sys.exception()
        failures = []
        try:
            monitor.close()
        except BaseException as exc:
            failures.append("monitor close failed: " + repr(exc))
        stop = show = None
        try:
            stop = subprocess.run(["systemctl", "--user", "stop", unit], env=env,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        except Exception as exc:
            failures.append("unit stop failed: " + repr(exc))
        try:
            show = subprocess.run(["systemctl", "--user", "show", unit, "--property=LoadState",
                "--property=ActiveState"], env=env, capture_output=True, text=True, timeout=5)
        except Exception as exc:
            failures.append("unit state unavailable: " + repr(exc))
        if show is not None:
            state = dict(line.split("=", 1) for line in show.stdout.splitlines() if "=" in line)
            if (show.returncode != 0 or state.get("ActiveState") not in {"inactive", "failed"}
                or (stop is not None and stop.returncode and state.get("LoadState") != "not-found")):
                failures.append("unit cleanup unverified")
        if failures:
            if original is None:
                raise AssertionError("; ".join(failures))
            original.add_note("; ".join(failures))



def _synthetic_monitor(monkeypatch, process, owner=":1.42"):
    import ctypes
    class ABI:
        def __init__(self): self.process = process
        def wait(self, bus, timeout): return 0
        def bus_close(self, bus): pass
        def bus_unref(self, bus): pass
        def slot_unref(self, slot): pass
    monitor = JobMonitor(ABI(), 4, 1024)
    monitor._bus = ctypes.c_void_p(1)
    monitor._owner_name = owner
    monitor._buffer = _EventBuffer(owner, 4, 1024, monitor.epoch)
    monkeypatch.setattr(monitor, "_get_owner", lambda timeout_us=1000000: owner)
    return monitor


def test_disconnect_and_owner_change_fail_closed(monkeypatch):
    import errno
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: -errno.ECONNRESET)
    assert monitor.poll(0) is None and monitor.unknown
    assert monitor.poll(0) is None
    monitor.close()
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    monkeypatch.setattr(monitor, "_get_owner", lambda timeout_us=1000000: ":1.99")
    assert monitor.poll(0) is None and monitor.unknown
    monitor.close()


def test_poll_message_budget_and_invalid_timeout_fail_closed(monkeypatch):
    calls = []
    def process(bus, message):
        calls.append(1)
        return 1
    monitor = _synthetic_monitor(monkeypatch, process)
    assert monitor.poll(0) is None and monitor.unknown
    assert len(calls) == 257
    monitor.close()
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    assert monitor.poll(True) is None and monitor.unknown
    monitor.close()


def test_open_returns_none_when_system_library_missing(monkeypatch):
    from orchestrator.isolation import _systemd_job_monitor as module
    def absent(): raise OSError("missing library")
    monkeypatch.setattr(module, "_ABI", absent)
    assert JobMonitor.open() is None


def test_close_continues_releasing_resources_after_interrupt(monkeypatch):
    import ctypes
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    calls = []
    interrupt = KeyboardInterrupt("slot close")
    def slot_unref(slot):
        calls.append("slot")
        raise interrupt
    monitor._slots = [ctypes.c_void_p(2)]
    monitor._callbacks = [object()]
    monitor._abi.slot_unref = slot_unref
    monitor._abi.bus_close = lambda bus: calls.append("close")
    monitor._abi.bus_unref = lambda bus: calls.append("unref")
    with pytest.raises(KeyboardInterrupt) as raised:
        monitor.close()
    assert raised.value is interrupt
    assert calls == ["slot", "close", "unref"]
    assert monitor.unknown
    monitor.close()


def test_c_string_copy_is_bounded_and_rejects_invalid_ascii():
    import ctypes
    from orchestrator.isolation._systemd_job_monitor import _decode_cstring
    valid = ctypes.create_string_buffer(b"abc")
    assert _decode_cstring(ctypes.addressof(valid), 3) == "abc"
    with pytest.raises(ValueError): _decode_cstring(ctypes.addressof(valid), 2)
    invalid = ctypes.create_string_buffer(bytes([255]))
    with pytest.raises(UnicodeError): _decode_cstring(ctypes.addressof(invalid), 3)
    with pytest.raises(ValueError): _decode_cstring(None, 3)


def test_owner_transition_always_breaks_continuity():
    buffer = _EventBuffer(":1.42", 4, 1024)
    buffer.owner_changed(":1.42")
    assert buffer.unknown


def test_poll_interruption_is_propagated_and_marks_unknown(monkeypatch):
    interrupt = KeyboardInterrupt("poll interrupted")
    def process(bus, message): raise interrupt
    monitor = _synthetic_monitor(monkeypatch, process)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            monitor.poll(0)
        assert caught.value is interrupt and monitor.unknown
        assert monitor.poll(0) is None
    finally:
        monitor.close()


def test_owner_watch_is_installed_before_first_owner_read(monkeypatch):
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    calls = []
    monkeypatch.setattr(monitor, "_install_owner_match", lambda: calls.append("watch"))
    monkeypatch.setattr(monitor, "_get_owner", lambda timeout_us=1000000: (calls.append("owner") or ":1.42"))
    monkeypatch.setattr(monitor, "_install_job_match", lambda: calls.append("job"))
    monkeypatch.setattr(monitor, "_subscribe", lambda: calls.append("subscribe"))
    monkeypatch.setattr(monitor, "_drain_ready", lambda *a: calls.append("drain"))
    monitor._establish()
    assert calls.index("watch") < calls.index("owner") < calls.index("job") < calls.index("subscribe") < calls.index("drain")
    monitor.close()


def test_queued_owner_change_during_sync_query_prevents_return(monkeypatch):
    pending = []
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    monitor._buffer.add(":1.42", 19, "/org/freedesktop/systemd1/job/19", "own.service", "done")
    def owner(timeout_us=1000000):
        pending.append("changed")
        return ":1.42"
    def process(bus, message):
        if pending:
            pending.pop()
            monitor._buffer.owner_changed(":1.43")
            return 1
        return 0
    monitor._abi.process = process
    monkeypatch.setattr(monitor, "_get_owner", owner)
    assert monitor.poll(0) is None and monitor.unknown
    monitor.close()


def test_poll_operation_budget_is_explicit_and_bounded(monkeypatch):
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    budgets = []
    def owner(timeout_us):
        budgets.append(timeout_us)
        return ":1.42"
    monkeypatch.setattr(monitor, "_get_owner", owner)
    assert monitor.poll(0, operation_timeout_ms=25) == ()
    assert len(budgets) == 1 and 0 < budgets[0] <= 25000
    monitor.close()


def test_callback_interrupt_is_recorded_and_propagated_by_poll(monkeypatch):
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    error = KeyboardInterrupt("callback")
    monitor._record_callback_error(error)
    with pytest.raises(KeyboardInterrupt) as raised:
        monitor.poll(0)
    assert raised.value is error and monitor.unknown
    monitor.close()


def test_successful_native_match_interruption_retains_owner_and_bus(monkeypatch):
    import ctypes
    from orchestrator.isolation import _systemd_job_monitor as module
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    marker = object()
    monitor._abi.match = lambda bus, slot, rule, callback, userdata: (
        ctypes.cast(slot, ctypes.POINTER(ctypes.c_void_p)).__setitem__(0, ctypes.c_void_p(7)),
        (_ for _ in ()).throw(KeyboardInterrupt("post-match")))[1]
    monkeypatch.setattr(module, "_KEEPERS", [monitor])
    with pytest.raises(KeyboardInterrupt):
        monitor._register("fake", marker)
    monitor.close()
    assert monitor in module._KEEPERS and monitor._bus.value == 1
    assert monitor._callbacks == [marker] and monitor._installation_uncertain


def test_registration_append_failure_never_calls_native(monkeypatch):
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    class FailAppend(list):
        def append(self, item): raise MemoryError("allocation")
    monitor._slots = FailAppend()
    monitor._abi.match = lambda *args: pytest.fail("native match must not run")
    with pytest.raises(MemoryError): monitor._register("fake", object())
    monitor.close()


def test_keeper_full_rejects_before_open(monkeypatch):
    from orchestrator.isolation import _systemd_job_monitor as module
    class ABI:
        def open(self, bus): pytest.fail("must not open")
    monkeypatch.setattr(module, "_ABI", ABI)
    monkeypatch.setattr(module, "_KEEPERS", [object()] * module._KEEPER_LIMIT)
    assert JobMonitor.open() is None


def test_owner_change_queued_before_first_owner_read_prevents_open(monkeypatch):
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    monkeypatch.setattr(monitor, "_install_owner_match", lambda: setattr(monitor, "_owner_changed", True))
    monkeypatch.setattr(monitor, "_get_owner", lambda timeout_us=1000000: ":1.42")
    monkeypatch.setattr(monitor, "_install_job_match", lambda: None)
    monkeypatch.setattr(monitor, "_subscribe", lambda: None)
    monkeypatch.setattr(monitor, "_drain_ready", lambda *a: 0)
    with pytest.raises(ValueError): monitor._establish()
    monitor.close()


def test_teardown_failure_never_releases_keeper_on_second_close(monkeypatch):
    import ctypes
    from orchestrator.isolation import _systemd_job_monitor as module
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    marker = object()
    monitor._slots = [ctypes.c_void_p(2)]
    monitor._callbacks = [marker]
    monitor._abi.slot_unref = lambda slot: (_ for _ in ()).throw(OSError("native failure"))
    monkeypatch.setattr(module, "_KEEPERS", [monitor])
    with pytest.raises(OSError): monitor.close()
    monitor.close()
    assert monitor in module._KEEPERS and monitor._callbacks == [marker]
    assert monitor._preserved_bus.value == 1


def test_poll_returns_empty_batch_with_strict_owner_signature(monkeypatch):
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    try:
        assert monitor.poll(0) == ()
        assert not monitor.unknown
    finally:
        monitor.close()


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
def test_ctypes_callback_interrupt_is_propagated(monkeypatch, error_type):
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    error = error_type("inside callback")
    def sender(message):
        raise error
    monitor._abi.sender = sender
    monitor._make_callbacks()
    def process(bus, message):
        monitor._job_callback(None, None, None)
        return 1
    monitor._abi.process = process
    try:
        with pytest.raises(error_type) as caught:
            monitor.poll(0)
        assert caught.value is error
        assert monitor.unknown
    finally:
        monitor.close()


def test_poll_owner_query_overruns_deadline_returns_unknown(monkeypatch):
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    def slow_owner(timeout_us):
        time.sleep(0.06)
        return ":1.42"
    monkeypatch.setattr(monitor, "_get_owner", slow_owner)
    try:
        started = time.monotonic()
        assert monitor.poll(0, operation_timeout_ms=20) is None
        elapsed = time.monotonic() - started
        assert 0.05 <= elapsed < 0.5
        assert monitor.unknown
    finally:
        monitor.close()


def test_aba_queued_during_owner_query_is_drained_before_return(monkeypatch):
    import ctypes
    monitor = _synthetic_monitor(monkeypatch, lambda bus, message: 0)
    monitor._make_callbacks()
    strings = [ctypes.create_string_buffer(s) for s in (
        b"org.freedesktop.systemd1", b":1.42", b":1.43",
        b"org.freedesktop.systemd1", b":1.43", b":1.42")]
    pending = []
    string_iter = iter(strings)
    def read(message, kind, output):
        ctypes.cast(output, ctypes.POINTER(ctypes.c_void_p))[0] = ctypes.addressof(next(string_iter))
        return 1
    monitor._abi.read = read
    monitor._abi.at_end = lambda message, complete: 1
    def owner(timeout_us):
        pending.extend([1, 1])
        return ":1.42"
    def process(bus, message):
        if pending:
            pending.pop()
            monitor._owner_callback(None, None, None)
            return 1
        return 0
    monkeypatch.setattr(monitor, "_get_owner", owner)
    monitor._abi.process = process
    try:
        assert monitor.poll(0) is None
        assert monitor.unknown
    finally:
        monitor.close()


def test_poll_rejects_deadline_overrun_in_final_native_process(monkeypatch):
    calls = 0
    def process(bus, message):
        nonlocal calls
        calls += 1
        if calls == 2:
            time.sleep(0.06)
        return 0
    monitor = _synthetic_monitor(monkeypatch, process)
    try:
        started = time.monotonic()
        assert monitor.poll(0, operation_timeout_ms=20) is None
        assert 0.05 <= time.monotonic() - started < 0.5
        assert calls == 2 and monitor.unknown
    finally:
        monitor.close()
