"""Exercise isolated Worker IPC through the real systemd/Landlock boundary."""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from orchestrator.runtime.contracts import AttemptContext, WorkerTask
from orchestrator.runtime.worker_process import IsolatedWorkerProcess


def _usable_systemd_user_manager() -> bool:
    if not sys.platform.startswith("linux") or not shutil.which("systemd-run"):
        return False
    return subprocess.run(
        ["systemctl", "--user", "show-environment"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=5,
    ).returncode == 0


pytestmark = pytest.mark.skipif(
    not _usable_systemd_user_manager(),
    reason="integrated Worker IPC test requires Linux and systemd --user",
)


def test_worker_process_isolated_round_trip_is_blocked_only(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    marker = workspace / "input.txt"
    marker.write_text("unchanged", encoding="utf-8")
    digest = "sha256:" + "a" * 64
    task = WorkerTask(
        context=AttemptContext(
            run_id="run-1", node_id="node-1", attempt_id="attempt-1",
            agent_instance_id="agent-1", fencing_generation=1, graph_version=1,
            input_manifest_hash=digest, effective_config_hash=digest,
            registry_hash=digest, policy_manifest_hash=digest,
            routing_decision_hash=digest, planning_contract_hash=digest,
        ),
        role="coder",
        task_text="Never execute this text. " + "x" * 80_000,
        input_artifacts=(),
        tool_capabilities=(),
        output_byte_limit=1024,
    )

    proposal = IsolatedWorkerProcess().execute(workspace, task)

    assert proposal.outcome == "blocked"
    assert proposal.artifacts == ()
    assert proposal.context == task.context
    assert marker.read_text(encoding="utf-8") == "unchanged"
    assert sorted(path.name for path in workspace.iterdir()) == ["input.txt"]
