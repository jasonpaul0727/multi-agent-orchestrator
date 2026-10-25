"""Required real general host flow; Provider transport and usage are offline fixtures."""

import asyncio
import threading
import time

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
