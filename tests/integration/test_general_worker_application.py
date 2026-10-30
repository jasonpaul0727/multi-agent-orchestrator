"""Required real general host flow; Provider transport and usage are offline fixtures."""

import asyncio
import json
from pathlib import Path
import subprocess
import threading
import time

import pytest

from orchestrator.isolation import SystemdReadOnlyLauncher
from tests.unit.runtime.test_general_worker_execution import execution_case


def test_normal_general_host_flow_supervises_real_children_off_event_loop(tmp_path, monkeypatch):
    case = execution_case(tmp_path)
    owner = threading.get_ident()
    launches = []
    original = SystemdReadOnlyLauncher.launch
    def launch(self, workspace, command, **kwargs):
        assert threading.get_ident() != owner
        launches.append(tuple(command))
        time.sleep(0.05)
        return original(self, workspace, command, **kwargs)
    monkeypatch.setattr(SystemdReadOnlyLauncher, "launch", launch)
    async def run():
        pending = asyncio.create_task(case.service.execute(identity=case.identity, task_text=case.text))
        ticks = 0
        while not pending.done():
            await asyncio.sleep(0.01)
            ticks += 1
        result = await pending
        assert result.status == "accepted", result
        assert ticks >= 10
    try:
        asyncio.run(run())
        assert len(launches) == 4
        assert sum(command[-1] == "--child" and "general_worker_process.py" in command[-2]
            for command in launches) == 2
        assert sum("verifier_process.py" in command[-2] for command in launches) == 2
    finally:
        case.app.close()


def test_public_application_normal_flow_and_real_private_storage_read_denial(tmp_path):
    from tests.unit.runtime.test_general_worker_application import configured_reopen
    from orchestrator.isolation import SandboxLimits
    case = execution_case(tmp_path)
    try:
        app = configured_reopen(case)
        result = asyncio.run(app.execute_general_worker_task(identity=case.identity, task_text=case.text))
        assert result.status == "accepted", result
        private = case.service._owner_root.parent
        blob = app._artifact_store._path_for_digest(result.artifacts[0].digest)
        paths = dict(database=str(private / "events.db"), artifact=str(blob),
            artifact_root=str(app._artifact_store._root), owners=str(case.service._owner_root))
        # Establish actual host availability first. Denial is not inferred from
        # a folder name or caused by nonexistent test data.
        assert Path(paths["database"]).read_bytes().startswith(b"SQLite format 3")
        assert Path(paths["artifact"]).read_bytes() == "candidate café".encode()
        assert tuple(Path(paths["artifact_root"]).iterdir())
        assert tuple(Path(paths["owners"]).iterdir())
        probe = "import json\nfrom pathlib import Path\npaths=" + repr(paths) + "\nout={}\n"
        probe += ("for key,value in paths.items():\n"
            " try:\n"
            "  p=Path(value)\n"
            "  list(p.iterdir()) if key.endswith('root') or key=='owners' else p.read_bytes()\n"
            "  out[key]=False\n"
            " except OSError: out[key]=True\n"
            "print(json.dumps(out,sort_keys=True))\n")
        token = "exec(bytes([" + ",".join(str(byte) for byte in probe.encode()) + "]))"
        # Test-only companion probe uses the actual same read-only systemd +
        # Landlock profile. Production fixed Worker turns never receive paths.
        launcher = SystemdReadOnlyLauncher()
        prepared = launcher.prepare_launch()
        sandbox = launcher.launch(case.service._workspace, ["/usr/bin/python3", "-I", "-S", "-c", token],
            prepared_launch=prepared, input_bytes=b"probe", limits=SandboxLimits(timeout_seconds=10))
        assert sandbox.launch_receipt.launch_id == prepared.launch_id
        observed = subprocess.run(["systemctl", "--user", "show", prepared.unit_name, "--property=PrivateTmp"],
            capture_output=True, check=True, timeout=3)
        assert observed.stdout.strip() == b"PrivateTmp=yes"
        finished = sandbox.wait()
        assert finished.returncode == 0 and finished.stderr == b"" and finished.input_written
        assert finished.termination_receipt.cgroup_empty
        assert json.loads(finished.stdout) == {key: True for key in paths}
    finally:
        case.app.close()


def test_recovered_owner_contention_has_one_winner(tmp_path, monkeypatch):
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.runtime.general_worker_execution import GeneralWorkerHostConfig
    from orchestrator.runtime.general_worker_process import GeneralWorkerProcess
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_attempt_execution_coordinator import TrustedUsage
    from tests.unit.runtime.test_general_worker_application import configured_reopen
    from tests.unit.runtime.test_general_worker_authority import OfflineBroker
    from tests.unit.runtime.test_general_worker_execution import OfflineSuccessTransport
    from tests.unit.runtime.test_general_worker_recovery import checkpoint
    case = execution_case(tmp_path)
    second = None
    release, entered = threading.Event(), threading.Event()
    try:
        checkpoint(case, monkeypatch)
        first = configured_reopen(case)
        private = case.service._owner_root.parent
        second_broker, second_transport = OfflineBroker(), OfflineSuccessTransport()
        second = ControlPlaneApplication(private / "events.db", limits=_application_limits(),
            artifact_root=private / "artifacts", usage_source=TrustedUsage(),
            general_worker_config=GeneralWorkerHostConfig(workspace=case.service._workspace,
                registry=case.service._gateway.registry, secret_broker=second_broker, transport=second_transport))
        original = GeneralWorkerProcess.start_turn
        launches = []
        def start(self, *args, **kwargs):
            launches.append(kwargs["prepared_launch"].launch_id)
            entered.set()
            assert release.wait(5)
            return original(self, *args, **kwargs)
        monkeypatch.setattr(GeneralWorkerProcess, "start_turn", start)
        async def run():
            winner = asyncio.create_task(first.recover_general_worker_task(identity=case.identity, task_text=case.text))
            while not entered.is_set():
                if winner.done():
                    pytest.fail(str(await winner))
                await asyncio.sleep(0.005)
            contender = await second.recover_general_worker_task(identity=case.identity, task_text=case.text)
            assert contender.status == "in_progress"
            release.set()
            assert (await winner).status == "accepted"
        asyncio.run(run())
        assert len(launches) == 1
        assert case.broker.calls == second_broker.calls == 0
        assert case.transport.calls == second_transport.calls == []
    finally:
        release.set()
        if second is not None:
            second.close()
        case.app.close()
