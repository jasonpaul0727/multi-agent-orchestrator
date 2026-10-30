"""Supervision must bind launch observation and exact whole-unit stop."""

from dataclasses import replace
from pathlib import Path
import subprocess
import threading

import pytest

from orchestrator.isolation import (
    IsolationNotStarted, IsolationStartUnknown, IsolationStopUnknown,
    PreparedSandboxLaunch, SandboxLaunchReceipt, SandboxLimits, SandboxResult, SandboxTerminationReceipt,
)
from orchestrator.runtime.contracts import AttemptContext, WorkerTask
from orchestrator.runtime.general_worker_process import GeneralWorkerProcess
from orchestrator.runtime.general_worker_protocol import (
    GeneralWorkerTurn, encode_general_worker_turn, _proposal_bytes,
)
from orchestrator.runtime.worker_process import IsolatedWorkerProcess


LAUNCH = "4dc48974-60de-476b-8828-e6d979729813"
UNIT = "maestro-attempt-4dc4897460de476b8828e6d979729813.service"
CGROUP = "/user.slice/user-1000.slice/user@1000.service/app.slice/" + UNIT


def turn(phase="complete"):
    h = "sha256:" + "a" * 64
    context = AttemptContext(run_id="run", node_id="node", attempt_id="attempt",
        agent_instance_id="worker", fencing_generation=1, graph_version=1,
        input_manifest_hash=h, effective_config_hash=h, registry_hash=h,
        policy_manifest_hash=h, routing_decision_hash=h, planning_contract_hash=h)
    task = WorkerTask(context=context, role="coder", task_text="private-task-sentinel",
        input_artifacts=(), tool_capabilities=(), output_byte_limit=16_384)
    return GeneralWorkerTurn(phase, LAUNCH, task, "private-response-sentinel" if phase == "complete" else None)


def prepared():
    return PreparedSandboxLaunch(LAUNCH, UNIT)


def ack():
    return SandboxLaunchReceipt(LAUNCH, UNIT, CGROUP, "a" * 32, "sha256:" + "b" * 64)


def terminal(**changes):
    values = dict(unit_name=UNIT, returncode=0, stdout=_proposal_bytes(encode_general_worker_turn(turn())),
        stderr=b"", elapsed_seconds=0.01, termination_receipt=SandboxTerminationReceipt(
            UNIT, CGROUP, "inactive", True), cancelled=False, timed_out=False,
        output_limited=False, input_written=True)
    return SandboxResult(**(values | changes))


class Session:
    def __init__(self, result=None, receipt=None):
        self.launch_receipt = ack() if receipt is None else receipt
        self.result = result or terminal()
        self.waited = False
        self.cancelled = False

    def wait(self):
        self.waited = True
        return self.result

    def cancel(self):
        self.cancelled = True
        return True


class Launcher:
    def __init__(self, session=None, error=None):
        self.session = session or Session()
        self.error = error
        self.calls = []

    def launch(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.error:
            raise self.error
        return self.session


def detached(kind, call):
    with pytest.raises(kind) as caught:
        call()
    assert caught.value.__cause__ is None and caught.value.__context__ is None
    assert "private-" not in str(caught.value) + repr(caught.value)


def test_observed_launch_receipt_available_before_wait(tmp_path):
    launcher = Launcher()
    session = GeneralWorkerProcess(launcher=launcher).start_turn(tmp_path, turn(), prepared_launch=prepared())
    assert session.launch_receipt == ack() and not launcher.session.waited
    result = session.wait()
    assert result.launch_receipt == ack() and result.termination_receipt.control_group == CGROUP
    assert result.proposal.blobs[0].content == b"private-response-sentinel"
    assert "private-response-sentinel" not in repr(result) + repr(session)
    assert session.wait() is result
    (args, kwargs), = launcher.calls
    assert args == (tmp_path, ["/usr/bin/python3", "-I", "-S",
        "@maestro-runtime@/orchestrator/runtime/general_worker_process.py", "--child"])
    assert kwargs["limits"].output_bytes == 1_048_576
    assert kwargs["prepared_launch"] == prepared()


def test_owned_prepare_and_recovered_stop_delegate_to_same_launcher():
    class OwnedLauncher(Launcher):
        def prepare_launch(self):
            return prepared()

        def stop_prepared_launch(self, intent, receipt):
            assert intent == prepared() and receipt == ack()
            return terminal().termination_receipt

    process = GeneralWorkerProcess(launcher=OwnedLauncher())
    assert process.prepare_launch() == prepared()
    assert process.stop_prepared_launch(prepared(), ack()).cgroup_empty


def test_workspace_changed_after_precheck_cannot_launch_general_turn(tmp_path, monkeypatch):
    import orchestrator.isolation.launcher as module
    from orchestrator.isolation import SystemdReadOnlyLauncher
    from orchestrator.workspace_identity import workspace_identity_hash

    (tmp_path / "input.txt").write_text("frozen")
    commitment = workspace_identity_hash(tmp_path)
    replacement = tmp_path.with_name(tmp_path.name + "-replacement")
    tmp_path.rename(replacement)
    tmp_path.mkdir()
    (tmp_path / "input.txt").write_text("replacement after admission")
    snapshots = []
    original = module.snapshot_workspace
    def snapshot(*args, **kwargs):
        snapshots.append(kwargs["expected_identity_hash"])
        return original(*args, **kwargs)
    monkeypatch.setattr(module, "snapshot_workspace", snapshot)
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    monkeypatch.setattr(module, "_systemd_cgroup_parent", lambda *args: Path("/unused"))
    monkeypatch.setattr(module.subprocess, "Popen", lambda *args, **kwargs: pytest.fail("mutated snapshot launched"))
    process = GeneralWorkerProcess(launcher=SystemdReadOnlyLauncher(systemd_run="/usr/bin/true", systemctl="/usr/bin/true"))
    intent = process.prepare_launch()
    with pytest.raises(IsolationNotStarted):
        process.start_turn(tmp_path, replace(turn(), launch_id=intent.launch_id), prepared_launch=intent,
            expected_workspace_identity_hash=commitment)
    assert snapshots == [commitment]


def test_workspace_identity_is_not_a_content_freeze(tmp_path):
    from orchestrator.workspace_identity import workspace_identity_hash
    (tmp_path / "input.txt").write_text("first")
    commitment = workspace_identity_hash(tmp_path)
    (tmp_path / "input.txt").write_text("second")
    assert workspace_identity_hash(tmp_path) == commitment


def test_missing_stop_never_yields_candidate(tmp_path):
    for receipt in (None, SandboxTerminationReceipt(UNIT,
        "/user.slice/other/app.slice/" + UNIT, "inactive", True)):
        launcher = Launcher(Session(terminal(termination_receipt=receipt)))
        detached(IsolationStopUnknown, lambda: GeneralWorkerProcess(launcher=launcher).run_turn(
            tmp_path, turn(), prepared_launch=prepared()))


@pytest.mark.parametrize("change", [dict(returncode=64), dict(cancelled=True), dict(timed_out=True),
    dict(output_limited=True), dict(input_written=False), dict(stderr=b"private-stderr"),
    dict(stdout=b"private-malformed")])
def test_verified_stop_remains_available_without_bad_proposal(tmp_path, change):
    launcher = Launcher(Session(terminal(**change)))
    result = GeneralWorkerProcess(launcher=launcher).run_turn(tmp_path, turn(), prepared_launch=prepared())
    assert result.proposal is None and result.termination_receipt.cgroup_empty


def test_prelaunch_and_unknown_start_errors_are_detached_and_distinct(tmp_path):
    for kind in (IsolationNotStarted, IsolationStartUnknown):
        launcher = Launcher(error=kind("private-task-sentinel"))
        detached(kind, lambda: GeneralWorkerProcess(launcher=launcher).start_turn(
            tmp_path, turn(), prepared_launch=prepared()))
    launcher = Launcher()
    detached(IsolationNotStarted, lambda: GeneralWorkerProcess(launcher=launcher).start_turn(
        tmp_path, replace(turn(), launch_id="d0364b41-1058-4222-97c3-ec983a64ccae"), prepared_launch=prepared()))
    assert launcher.calls == []


def test_missing_or_substituted_ack_is_start_unknown(tmp_path):
    for receipt in (False, SandboxLaunchReceipt("d0364b41-1058-4222-97c3-ec983a64ccae",
        "maestro-attempt-d0364b411058422297c3ec983a64ccae.service",
        "/user.slice/app.slice/maestro-attempt-d0364b411058422297c3ec983a64ccae.service", "a" * 32, "sha256:" + "b" * 64)):
        detached(IsolationStartUnknown, lambda: GeneralWorkerProcess(launcher=Launcher(Session(receipt=receipt))).start_turn(
            tmp_path, turn(), prepared_launch=prepared()))


def test_cancellation_stops_supervised_session_and_withholds_candidate(tmp_path):
    cancelled = threading.Event()
    cancelled.set()
    launcher = Launcher(Session(terminal(cancelled=True)))
    result = GeneralWorkerProcess(launcher=launcher).run_turn(tmp_path, turn(),
        prepared_launch=prepared(), cancellation=cancelled)
    assert launcher.session.cancelled and result.proposal is None


def test_old_worker_remains_blocked():
    script = Path(__file__).resolve().parents[3] / "src/orchestrator/runtime/worker_process.py"
    result = subprocess.run(["/usr/bin/python3", "-I", "-S", str(script), "--child"],
        input=turn().task.model_dump_json().encode(), capture_output=True, timeout=5, check=False)
    assert result.returncode == 0 and result.stderr == b""
    import json
    assert json.loads(result.stdout)["outcome"] == "blocked"
    assert not hasattr(IsolatedWorkerProcess, "run_turn")


def test_supplied_limits_cannot_expand_protocol_frame_collection(tmp_path):
    launcher = Launcher()
    GeneralWorkerProcess(launcher=launcher, limits=SandboxLimits(output_bytes=2_097_152)).run_turn(
        tmp_path, turn(), prepared_launch=prepared())
    assert launcher.calls[0][1]["limits"].output_bytes == 1_048_576


def test_cancellation_after_wait_begins_is_supervised(tmp_path):
    cancellation, waiting, killed = threading.Event(), threading.Event(), threading.Event()
    class WaitingSession(Session):
        def wait(self):
            waiting.set()
            assert killed.wait(timeout=2)
            return terminal(cancelled=True)
        def cancel(self):
            killed.set()
            return True
    results = []
    worker = GeneralWorkerProcess(launcher=Launcher(WaitingSession()))
    waiter = threading.Thread(target=lambda: results.append(worker.run_turn(tmp_path,
        turn(), prepared_launch=prepared(), cancellation=cancellation)))
    waiter.start()
    assert waiting.wait(timeout=2)
    cancellation.set()
    waiter.join(timeout=3)
    assert not waiter.is_alive() and len(results) == 1
    assert results[0].proposal is None and results[0].cancelled


def test_transport_exception_is_detached_stop_unknown(tmp_path):
    class FailingSession(Session):
        def wait(self):
            raise RuntimeError("private-response-sentinel")
    detached(IsolationStopUnknown, lambda: GeneralWorkerProcess(launcher=Launcher(FailingSession())).run_turn(
        tmp_path, turn(), prepared_launch=prepared()))


@pytest.mark.parametrize("field", ["launch_id", "unit_name"])
def test_hostile_ack_identity_rejected_before_comparison_callback(tmp_path, field):
    calls = []
    class HostileIdentity:
        def __ne__(self, other):
            calls.append("ne")
            return False
    receipt = replace(ack(), **{field: HostileIdentity()})
    detached(IsolationStartUnknown, lambda: GeneralWorkerProcess(launcher=Launcher(Session(receipt=receipt))).start_turn(
        tmp_path, turn(), prepared_launch=prepared()))
    assert calls == []
