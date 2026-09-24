from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from orchestrator.isolation import SandboxLimits
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.security.policy import PolicyAuthority, PolicyManifest
from orchestrator.tools import READ_ONLY_COMMAND_TOOL_ID, PolicyState, ToolGateway, ToolRequest


def _usable_systemd_user_manager() -> bool:
    if not sys.platform.startswith("linux") or not shutil.which("systemd-run"):
        return False
    try:
        return subprocess.run(
            ["systemctl", "--user", "show-environment"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=5,
        ).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


pytestmark = pytest.mark.skipif(
    not _usable_systemd_user_manager(),
    reason="integrated tool gateway requires Linux and systemd --user",
)


class _CurrentAttempt:
    def is_current(self, request: ToolRequest) -> bool:
        return request.fencing_generation == 9 and request.causation_id == "attempt-accepted"


def test_gateway_executes_only_after_policy_and_real_isolation(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "input.txt").write_text("visible data", encoding="utf-8")
    outside = tmp_path / "outside-secret.txt"
    outside.write_text("must remain inaccessible", encoding="utf-8")
    request = ToolRequest(
        request_id="gateway-e2e-1",
        run_id="gateway-run-1",
        node_id="worker-node-1",
        attempt_id="worker-attempt-1",
        fencing_generation=9,
        role="worker",
        causation_id="attempt-accepted",
        workspace=str(workspace),
        command=(
            "/usr/bin/python3",
            "-c",
            "from pathlib import Path\nimport sys\nprint(Path('input.txt').read_text())\n"
            "try:\n print(Path(sys.argv[1]).read_text())\n"
            "except OSError:\n print('outside-denied')",
            str(outside),
        ),
    )
    events = SQLiteEventStore(tmp_path / "events.db")
    manifest = PolicyManifest(
        authorities=(
            PolicyAuthority(
                source="system",
                max_permission="read-only",
                allowed_actions=("safe_read",),
                allowed_tools=(READ_ONLY_COMMAND_TOOL_ID,),
            ),
        )
    )
    gateway = ToolGateway(
        run_id=request.run_id,
        event_store=events,
        policy_manifest=manifest,
        attempt_authority=_CurrentAttempt(),
        policy_state=lambda _request: PolicyState(3, 2),
        limits=SandboxLimits(timeout_seconds=15, output_bytes=4096),
    )

    result = gateway.execute(request)

    assert result.outcome == "completed"
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert result.termination_confirmed
    assert result.stdout == b"visible data\noutside-denied\n"
    assert b"must remain inaccessible" not in result.stdout + result.stderr
    stream = events.read_stream("security", request.run_id)
    assert [event.event_type for event in stream][-3:] == [
        "ToolCapabilityConsumed",
        "ToolExecutionStarted",
        "ToolExecutionCompleted",
    ]
    terminal = stream[-1].payload
    assert terminal["stdout_sha256"] == "sha256:" + hashlib.sha256(result.stdout).hexdigest()
    assert terminal["stdout_bytes"] == len(result.stdout)
    assert "visible data" not in json.dumps([event.payload for event in stream])
    assert str(outside) not in json.dumps([event.payload for event in stream])
