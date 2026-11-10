"""Real parent cgroup sampling; no stop or launch authority is implied."""
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

import pytest

from orchestrator.isolation._cgroup_v2_observer import CgroupObserver
from tests.integration.test_mediation_parent_cgroup_capability import require_manager


def _cleanup_owned(env, unit, slice_name, observer, proc):
    failures = []
    if observer is not None:
        try:
            observer.close()
        except Exception as exc:
            failures.append("observer close failed: " + repr(exc))
    for owned in (unit, slice_name):
        stopped = shown = None
        try:
            stopped = subprocess.run(["systemctl", "--user", "stop", owned], env=env,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        except Exception as exc:
            failures.append("owned unit stop failed: " + repr(exc))
        try:
            shown = subprocess.run(["systemctl", "--user", "show", owned, "--property=LoadState",
                "--property=ActiveState"], env=env, capture_output=True, text=True, timeout=5)
        except Exception as exc:
            failures.append("owned unit observation failed: " + repr(exc))
        if shown is not None:
            state = dict(line.split("=", 1) for line in shown.stdout.splitlines() if "=" in line)
            if (shown.returncode or state.get("ActiveState") not in {"inactive", "failed"}
                or state.get("LoadState") not in {"loaded", "not-found"}
                or (stopped is not None and stopped.returncode and state.get("LoadState") != "not-found")):
                failures.append("owned unit cleanup unknown")
    try:
        live = proc.poll() is None
    except Exception as exc:
        failures.append("transport poll failed: " + repr(exc))
        live = True
    if live:
        try:
            proc.kill()
        except Exception as exc:
            failures.append("transport kill failed: " + repr(exc))
    try:
        proc.communicate(timeout=5)
    except Exception as exc:
        failures.append("transport cleanup failed: " + repr(exc))
    return failures


def test_real_parent_population_changes_when_owned_actor_exits():
    env, _, _ = require_manager()
    name = "maestroprobe" + uuid.uuid4().hex
    slice_name, unit = name + ".slice", name + ".service"
    proc = subprocess.Popen(["systemd-run", "--user", "--quiet", "--wait", "--pipe", "--collect",
        "--slice=" + slice_name, "--unit=" + name, "--", "/usr/bin/python3", "-I", "-S", "-c",
        "import sys; sys.stdin.buffer.read()"], env=env, stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    observer = None
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            show = subprocess.run(["systemctl", "--user", "show", unit, "--property=Id",
                "--property=ActiveState", "--property=ControlGroup", "--property=InvocationID"],
                env=env, capture_output=True, text=True, timeout=3)
            values = dict(line.split("=", 1) for line in show.stdout.splitlines() if "=" in line)
            if values.get("ActiveState") == "active" and values.get("ControlGroup"):
                break
            if proc.poll() is not None: pytest.fail("owned actor exited before observation")
            time.sleep(0.05)
        else:
            pytest.fail("owned actor did not become active")
        assert values["Id"] == unit and values["InvocationID"]
        group = Path(values["ControlGroup"].lstrip("/"))
        assert group.name == unit and group.parent.name == slice_name
        observer = CgroupObserver.open(group.parent.as_posix())
        assert observer is not None
        assert observer.observe() == 1
        stdout, stderr = proc.communicate(b"done", timeout=15)
        assert proc.returncode == 0, stderr
        assert observer.observe() == 0
    finally:
        original = sys.exception()
        failures = _cleanup_owned(env, unit, slice_name, observer, proc)
        if failures:
            error = AssertionError("; ".join(failures))
            if original is None:
                raise error
            original.add_note(str(error))


def test_cleanup_continues_after_independent_failures(monkeypatch):
    class BrokenObserver:
        def close(self): raise OSError("close")
    class BrokenProcess:
        def poll(self): raise OSError("poll")
        def kill(self): raise OSError("kill")
        def communicate(self, timeout): raise OSError("wait")
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        if args[2] == "stop": raise OSError("stop")
        return subprocess.CompletedProcess(args, 0, stdout=chr(10).join(["LoadState=not-found", "ActiveState=inactive"]))
    monkeypatch.setattr(subprocess, "run", run)
    failures = _cleanup_owned({}, "own.service", "own.slice", BrokenObserver(), BrokenProcess())
    assert [args[2] for args in calls] == ["stop", "show", "stop", "show"]
    assert len(failures) == 6
