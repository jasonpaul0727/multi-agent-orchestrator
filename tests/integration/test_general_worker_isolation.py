"""Required real systemd Worker isolation: failures never become skips."""

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

import pytest

from orchestrator.isolation import (
    IsolationNotStarted, IsolationStartUnknown, SandboxLimits, SystemdReadOnlyLauncher,
)
from orchestrator.runtime.contracts import AttemptContext, WorkerTask
from orchestrator.runtime.general_worker_process import GeneralWorkerProcess
from orchestrator.runtime.general_worker_protocol import GeneralWorkerRequest, GeneralWorkerTurn


def task():
    h = "sha256:" + "a" * 64
    return WorkerTask(context=AttemptContext(run_id="run", node_id="node", attempt_id="attempt",
        agent_instance_id="worker", fencing_generation=1, graph_version=1,
        input_manifest_hash=h, effective_config_hash=h, registry_hash=h,
        policy_manifest_hash=h, routing_decision_hash=h, planning_contract_hash=h),
        role="coder", task_text="private-task-sentinel é\r\n", input_artifacts=(),
        tool_capabilities=(), output_byte_limit=16_384)


def show(unit, names):
    result = subprocess.run(["systemctl", "--user", "show", unit,
        *("--property=" + name for name in names)], capture_output=True, check=False, timeout=3)
    assert result.returncode == 0, "required systemd observation failed"
    return dict(line.split("=", 1) for line in result.stdout.decode("ascii").splitlines())


def empty(receipt):
    assert receipt.cgroup_empty and receipt.active_state in {"inactive", "failed"}
    group = Path("/sys/fs/cgroup") / receipt.control_group.lstrip("/")
    assert group.name == receipt.unit_name and group.parent.name == "app.slice"
    try:
        values = dict(line.split() for line in (group / "cgroup.events").read_text().splitlines())
    except FileNotFoundError:
        return
    assert values["populated"] == "0"


def cleanup_owned(launcher):
    # These tests alone own every session in this fresh launcher. On an ack
    # failure, kill/drain the actual transport and require existing host stop
    # proof before staging can be discarded. No fabricated receipt is used.
    for session in launcher._sessions.values():
        if session._result is not None and session._result.termination_confirmed:
            empty(session._result.termination_receipt)
            continue
        session.cancel()
        result = session.wait()
        assert result.termination_receipt is not None, "test cleanup could not prove whole-unit stop"
        empty(result.termination_receipt)


def test_real_request_and_complete_turns_are_readonly_nonetwork(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "input.txt").write_text("unchanged")
    (workspace / ".env").write_text("private-test-environment")
    launcher = SystemdReadOnlyLauncher()
    worker = GeneralWorkerProcess(launcher=launcher)
    try:
        for phase in ("request", "complete"):
            prepared = launcher.prepare_launch()
            text = "private-response-sentinel é\r\n" if phase == "complete" else None
            turn = GeneralWorkerTurn(phase, prepared.launch_id, task(), text)
            session = worker.start_turn(workspace, turn, prepared_launch=prepared)
            receipt = session.launch_receipt
            assert receipt.launch_id == prepared.launch_id
            observed = show(prepared.unit_name, ("Id", "InvocationID", "ControlGroup", "PrivateNetwork"))
            assert observed == dict(Id=prepared.unit_name, InvocationID=receipt.invocation_id,
                ControlGroup=receipt.control_group, PrivateNetwork="yes")
            result = session.wait()
            empty(result.termination_receipt)
            assert result.proposal is not None and result.returncode == 0 and result.input_written
            if phase == "request":
                assert type(result.proposal) is GeneralWorkerRequest
                assert result.proposal.context == turn.task.context
            else:
                content = result.proposal.blobs[0]
                assert content.content == text.encode()
                assert content.artifact.digest == "sha256:" + hashlib.sha256(text.encode()).hexdigest()
        # A companion command under the same fully observed prepared profile
        # exercises OS restrictions; the useful fixed child has no test hooks.
        probe = """import json,sys,socket
from pathlib import Path
sys.stdin.buffer.read()
out={}
for key,path in [('readonly','output.txt'),('input_readonly','input.txt')]:
 try: Path(path).write_text('changed');out[key]=False
 except OSError: out[key]=True
try: socket.socket(socket.AF_INET,socket.SOCK_STREAM);out['nonetwork']=False
except OSError: out['nonetwork']=True
try: Path('.env').read_text();out['hidden_env']=False
except OSError: out['hidden_env']=True
print(json.dumps(out,sort_keys=True))
"""
        token = "exec(bytes([" + ",".join(str(b) for b in probe.encode()) + "]))"
        prepared = launcher.prepare_launch()
        sandbox = launcher.launch(workspace, ["/usr/bin/python3", "-I", "-S", "-c", token],
            prepared_launch=prepared, input_bytes=b"go", limits=SandboxLimits(timeout_seconds=10))
        result = sandbox.wait()
        empty(result.termination_receipt)
        assert result.returncode == 0 and result.stderr == b""
        assert json.loads(result.stdout) == dict(readonly=True, input_readonly=True, nonetwork=True, hidden_env=True)
        assert (workspace / "input.txt").read_text() == "unchanged"
        assert not (workspace / "output.txt").exists()
    finally:
        cleanup_owned(launcher)


def test_real_sigkill_worker_proves_empty_cgroup(tmp_path):
    launcher = SystemdReadOnlyLauncher()
    worker = GeneralWorkerProcess(launcher=launcher)
    prepared = launcher.prepare_launch()
    try:
        session = worker.start_turn(tmp_path, GeneralWorkerTurn("complete", prepared.launch_id,
            task(), "private-response-sentinel"), prepared_launch=prepared)
        deadline = time.monotonic() + 5
        main_pid = None
        while time.monotonic() < deadline:
            observed = show(prepared.unit_name, ("MainPID",))
            candidate = int(observed["MainPID"])
            if candidate > 0:
                try:
                    command = (Path("/proc") / str(candidate) / "cmdline").read_bytes().split(b"\0")
                    if command[:3] == [b"/usr/bin/python3", b"-I", b"-S"] and command[3].endswith(b"/general_worker_process.py"):
                        main_pid = candidate
                        break
                except (OSError, IndexError):
                    pass
            time.sleep(0.02)
        assert main_pid is not None, "real useful Worker MainPID did not become ready"
        os.kill(main_pid, signal.SIGKILL)
        result = session.wait()
        assert result.proposal is None and result.returncode != 0
        assert result.launch_receipt == session.launch_receipt
        empty(result.termination_receipt)
    finally:
        cleanup_owned(launcher)


def test_prepared_unacked_launch_never_retries(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    entered, release = tmp_path / "entered", tmp_path / "release"
    wrapper = tmp_path / "delayed-systemd-run"
    wrapper.write_text("#!/usr/bin/python3\nimport os,sys,time\nfrom pathlib import Path\n"
        f"Path({str(entered)!r}).write_text('one')\n"
        "deadline=time.monotonic()+15\n"
        f"while not Path({str(release)!r}).exists():\n"
        " if time.monotonic()>deadline: raise SystemExit(124)\n"
        " time.sleep(.02)\n"
        "os.execv('/usr/bin/systemd-run',['systemd-run',*sys.argv[1:]])\n")
    wrapper.chmod(0o700)
    launcher = SystemdReadOnlyLauncher(systemd_run=str(wrapper))
    prepared = launcher.prepare_launch()
    turn = GeneralWorkerTurn("request", prepared.launch_id, task())
    worker = GeneralWorkerProcess(launcher=launcher)
    try:
        with pytest.raises(IsolationStartUnknown):
            worker.start_turn(workspace, turn, prepared_launch=prepared)
        assert entered.read_text() == "one"
        assert launcher._sessions[prepared.launch_id].launch_receipt is None
        with pytest.raises(IsolationNotStarted):
            worker.start_turn(workspace, turn, prepared_launch=prepared)
        with pytest.raises(IsolationStartUnknown):
            launcher.stop_prepared_launch(prepared, None)
        release.touch()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if int(show(prepared.unit_name, ("MainPID",))["MainPID"]) > 0:
                break
            time.sleep(0.02)
        else:
            pytest.fail("delayed real unit never appeared after acknowledgement deadline")
        assert launcher._sessions[prepared.launch_id].launch_receipt is None
    finally:
        release.touch()
        cleanup_owned(launcher)
    with pytest.raises(IsolationNotStarted):
        worker.start_turn(workspace, turn, prepared_launch=prepared)


def test_real_legal_fractional_cpu_quota_profile(tmp_path):
    launcher = SystemdReadOnlyLauncher()
    worker = GeneralWorkerProcess(launcher=launcher, limits=SandboxLimits(cpu_percent=125, timeout_seconds=10))
    prepared = launcher.prepare_launch()
    try:
        session = worker.start_turn(tmp_path, GeneralWorkerTurn("request", prepared.launch_id, task()),
            prepared_launch=prepared)
        assert show(prepared.unit_name, ("CPUQuotaPerSecUSec",)) == {"CPUQuotaPerSecUSec": "1.250000s"}
        assert session.launch_receipt.invocation_id
        result = session.wait()
        assert type(result.proposal) is GeneralWorkerRequest
        empty(result.termination_receipt)
    finally:
        cleanup_owned(launcher)
