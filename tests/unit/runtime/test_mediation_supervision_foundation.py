"""Local crash regression, not systemd or authenticated permit evidence."""
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from orchestrator.lifecycle import LifecycleController
from orchestrator.persistence import SQLiteEventStore
from orchestrator.persistence.mediation_identity import mediation_stream_id
from orchestrator.runtime.mediation_journal import MediationJournal
from orchestrator.scheduler import Scheduler
from tests.unit.lifecycle.test_scheduler import _application_limits


TRANSPORT = r'''
import os, pathlib, sys, time
root = pathlib.Path(sys.argv[1])
host_pid = int(sys.argv[2])
def publish(name, value):
    temporary = root / (name + '.tmp')
    temporary.write_text(value)
    temporary.replace(root / name)
publish('transport-ready', str(os.getpid()))
deadline = time.monotonic() + 90
while not (root / 'release-transport').exists():
    if time.monotonic() > deadline:
        sys.exit(2)
    time.sleep(0.02)
publish('transport-submitted', str(os.getppid() != host_pid))
'''

HOST = r'''
import pathlib, subprocess, sys, time
from tests.unit.runtime.test_general_worker_authority import dispatch_case
from tests.unit.runtime.test_mediation_cross_version_races import v2_operation
from tests.unit.runtime.test_mediation_review_fixes import intent, record_inert
from orchestrator.runtime.mediation_journal import MediationJournal
root = pathlib.Path(sys.argv[1])
case = dispatch_case(root, register=False)
candidate = v2_operation(case)
journal = MediationJournal(case.store)
journal.register(candidate)
record_inert(journal, candidate.operation_id,
             intent(operation_id=candidate.operation_id, fencing_generation=candidate.fencing_generation))
journal.close(candidate.operation_id)
case.store.close()
# The independent child models delayed local submission; it is not systemd.
subprocess.Popen([sys.executable, '-c', sys.argv[2], str(root), str(__import__('os').getpid())],
                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                 close_fds=True)
while True:
    time.sleep(1)
'''

# A dedicated Linux child subreaper owns the entire test-only process group.
# The supervisor leads the test-only session. It reaps the host and adopted
# children on graceful cleanup; the parent keeps the supervisor unreaped
# until its bounded process-group fallback has run.
SUPERVISOR = r'''
import ctypes, os, pathlib, signal, subprocess, sys
root = pathlib.Path(sys.argv[1])
if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), 'PR_SET_CHILD_SUBREAPER')
host = subprocess.Popen([sys.executable, '-c', sys.argv[2], str(root), sys.argv[3]],
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                        stderr=sys.stderr)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
try:
    command = sys.stdin.readline().strip()
    if command == 'kill':
        os.kill(host.pid, signal.SIGKILL)
        result = os.waitid(os.P_PID, host.pid, os.WEXITED | os.WNOWAIT)
        if result.si_code != os.CLD_KILLED or result.si_status != signal.SIGKILL:
            raise RuntimeError('host did not die from SIGKILL')
        temp = root / 'host-dead.tmp'
        temp.write_text('SIGKILL')
        temp.replace(root / 'host-dead')
        sys.stdin.readline()
finally:
    try:
        os.killpg(os.getpgrp(), signal.SIGTERM)
    except ProcessLookupError:
        pass
    host.wait()
    while True:
        try:
            os.waitpid(-1, 0)
        except ChildProcessError:
            break
'''


def wait_for(path, supervisor=None, timeout=30):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if supervisor is not None and os.waitid(os.P_PID, supervisor.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None:
            pytest.fail(f"supervisor exited before {path.name}")
        if time.monotonic() >= deadline:
            pytest.fail(f"timed out waiting for {path.name}")
        time.sleep(0.02)

def cleanup_supervisor(supervisor, timeout=15):
    """Bounded cleanup; do not reap the test-owned group leader before killpg."""
    active_error = sys.exception()
    cleanup_errors = []
    group_safe = True
    try:
        try:
            os.set_blocking(supervisor.stdin.fileno(), False)
            os.write(supervisor.stdin.fileno(), b"stop\n")
        except OSError as exc:
            cleanup_errors.append(f"stop command failed: {exc}")
        deadline = time.monotonic() + timeout
        while os.waitid(os.P_PID, supervisor.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
            if time.monotonic() >= deadline:
                cleanup_errors.append("supervisor cleanup timed out")
                break
            time.sleep(0.02)
    except Exception as exc:
        group_safe = False
        cleanup_errors.append(f"supervisor status failed: {exc}")
    finally:
        # The child remains unreaped here, even if it exited unexpectedly, so
        # its session/group ID cannot refer to an unrelated process.
        try:
            if group_safe:
                os.killpg(supervisor.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception as exc:
            cleanup_errors.append(f"group termination failed: {exc}")
        try:
            supervisor.wait(timeout=5)
        except Exception as exc:
            cleanup_errors.append(f"supervisor wait failed: {exc}")
        for stream in (supervisor.stdin, supervisor.stderr):
            try:
                stream.close()
            except Exception as exc:
                cleanup_errors.append(f"stream close failed: {exc}")
    if cleanup_errors or supervisor.returncode != 0:
        error = AssertionError(f"supervisor cleanup failed: {cleanup_errors}; exit={supervisor.returncode}")
        if active_error is not None:
            active_error.add_note(str(error))
        else:
            raise error

def assert_public_release_held(database):
    identity = dict(run_id="run-1", node_id="node-1", attempt_id="attempt-node-1-1", fencing_generation=1)
    with SQLiteEventStore(database) as store:
        state = MediationJournal(store).load(mediation_stream_id(**identity))
        assert state.closed
        assert len(state.actors) == 1
        assert state.actors[0].status == "launch_unknown"
        now = datetime.now(timezone.utc)
        attempt = LifecycleController(store).replay(identity["run_id"]).node(identity["node_id"]).attempts[-1]
        assert attempt.attempt_id == identity["attempt_id"]
        assert now < datetime.fromisoformat(attempt.lease_expires_at)
        with pytest.raises(ValueError, match="^mediation operation is unresolved$"):
            Scheduler(store, limits=_application_limits()).finish_attempt(
                **identity, completed_at=now, outcome="failed", known_no_effect=True)
        assert not any(event.event_type == "AttemptSlotReleased"
                       for event in store.read_stream("scheduler", "global"))


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux SIGKILL and child subreaper")
def test_crashed_host_cannot_release_while_delayed_local_transport_can_still_submit(tmp_path):
    env = os.environ.copy()
    project = Path(__file__).resolve().parents[3]
    env["PYTHONPATH"] = os.pathsep.join((str(project), str(project / "src"), env.get("PYTHONPATH", "")))
    supervisor = subprocess.Popen([sys.executable, "-c", SUPERVISOR, str(tmp_path), HOST, TRANSPORT],
                                  stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.PIPE, text=True, env=env, start_new_session=True)
    try:
        wait_for(tmp_path / "transport-ready", supervisor)
        supervisor.stdin.write("kill\n")
        supervisor.stdin.flush()
        wait_for(tmp_path / "host-dead", supervisor)
        assert (tmp_path / "host-dead").read_text() == "SIGKILL"
        assert not (tmp_path / "transport-submitted").exists()
        assert_public_release_held(tmp_path / "events.db")
        (tmp_path / "release-transport").touch()
        wait_for(tmp_path / "transport-submitted", supervisor)
        assert (tmp_path / "transport-submitted").read_text() == "True"
        assert_public_release_held(tmp_path / "events.db")
    finally:
        cleanup_supervisor(supervisor)


def test_public_release_succeeds_without_inert_v2_history(tmp_path):
    from tests.unit.runtime.test_general_worker_authority import dispatch_case

    case = dispatch_case(tmp_path, register=False)
    identity = case.identity
    case.store.close()
    with SQLiteEventStore(tmp_path / "events.db") as store:
        Scheduler(store, limits=_application_limits()).finish_attempt(
            **identity, completed_at=datetime.now(timezone.utc), outcome="failed", known_no_effect=True)
        assert any(event.event_type == "AttemptSlotReleased"
                   for event in store.read_stream("scheduler", "global"))


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux child subreaper")
def test_supervisor_cleans_up_child_even_before_ready_signal(tmp_path):
    child = "import pathlib,sys,time; pathlib.Path(sys.argv[1]).touch(); time.sleep(60)"
    host = "import subprocess,sys,time; subprocess.Popen([sys.executable, '-c', sys.argv[2], sys.argv[1] + '/child-created']); time.sleep(60)"
    supervisor = subprocess.Popen([sys.executable, "-c", SUPERVISOR, str(tmp_path), host, child],
                                  stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        wait_for(tmp_path / "child-created", supervisor)
    finally:
        cleanup_supervisor(supervisor)



@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux process groups")
@pytest.mark.parametrize("mode", ["timeout", "broken_pipe"])
def test_parent_cleanup_kills_group_when_stop_fails(tmp_path, mode):
    stubborn = r'''
import os, pathlib, subprocess, sys, time
root = pathlib.Path(sys.argv[1])
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
if sys.argv[2] == 'broken_pipe':
    os.close(0)
temp = root / 'child-pid.tmp'
temp.write_text(str(child.pid))
temp.replace(root / 'child-pid')
time.sleep(60)
'''
    supervisor = subprocess.Popen([sys.executable, "-c", stubborn, str(tmp_path), mode],
                                  stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        wait_for(tmp_path / "child-pid", supervisor)
        child_pid = int((tmp_path / "child-pid").read_text())
        with pytest.raises(AssertionError, match="supervisor cleanup failed"):
            cleanup_supervisor(supervisor, timeout=0.2)
        deadline = time.monotonic() + 5
        proc_stat = Path(f"/proc/{child_pid}/stat")
        while proc_stat.exists():
            try:
                # Zombies are dead; their eventual reaping belongs to init if
                # the forced kill also ended the test subreaper.
                state = proc_stat.read_text().rsplit(")", 1)[1].strip().split()[0]
            except FileNotFoundError:
                break
            if state == "Z":
                break
            assert time.monotonic() < deadline, "test child remained running"
            time.sleep(0.02)
    finally:
        if supervisor.returncode is None:
            cleanup_supervisor(supervisor, timeout=0.2)




@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux process groups")
def test_cleanup_error_does_not_mask_original_failure(monkeypatch):
    supervisor = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                  stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.PIPE, text=True, start_new_session=True)
    real_killpg = os.killpg

    def terminate_then_fail(group, sig):
        real_killpg(group, sig)
        raise OSError("injected cleanup error")

    monkeypatch.setattr(os, "killpg", terminate_then_fail)
    with pytest.raises(AssertionError, match="original failure") as captured:
        try:
            raise AssertionError("original failure")
        finally:
            cleanup_supervisor(supervisor, timeout=0.1)
    assert any("injected cleanup error" in note for note in captured.value.__notes__)
    assert supervisor.returncode is not None

