"""End-to-end execution through the non-mocked systemd isolation launcher."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time

import pytest

from orchestrator.isolation import (
    InvalidSandboxRequest,
    SandboxLimits,
    SandboxResult,
    SystemdReadOnlyLauncher,
)


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
    reason="integrated isolation tests require Linux and systemd --user",
)


def _assert_empty_cgroup_receipt(result: SandboxResult) -> None:
    receipt = result.termination_receipt
    assert receipt is not None
    assert receipt.unit_name == result.unit_name
    assert receipt.active_state in {"inactive", "failed"}
    assert receipt.cgroup_empty
    assert receipt.control_group.endswith("/app.slice/" + result.unit_name)
    cgroup = Path("/sys/fs/cgroup") / receipt.control_group.lstrip("/")
    try:
        events = (cgroup / "cgroup.events").read_text(encoding="ascii")
    except FileNotFoundError:
        return
    values = dict(line.split() for line in events.splitlines())
    assert values.get("populated") == "0"


def _wait_for_unit_child(
    session, *, expected_comm: str, timeout_seconds: float = 5
) -> int:
    """Wait until the unit's main process has actually spawned its child."""

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                [
                    "systemctl", "--user", "show", session.unit_name,
                    "--property=MainPID", "--value",
                ],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                check=False,
                text=True,
                timeout=1,
            )
        except (OSError, subprocess.TimeoutExpired):
            result = None
        if result is not None and result.returncode == 0:
            main_pid = result.stdout.strip()
            if main_pid.isdecimal() and int(main_pid) > 0:
                children_path = (
                    Path("/proc") / main_pid / "task" / main_pid / "children"
                )
                try:
                    child_pids = children_path.read_text(encoding="ascii").split()
                except OSError:
                    child_pids = []
                for child_pid in child_pids:
                    try:
                        comm = (
                            (Path("/proc") / child_pid / "comm")
                            .read_text(encoding="ascii")
                            .strip()
                        )
                    except OSError:
                        continue
                    if comm == expected_comm:
                        return int(child_pid)
        time.sleep(0.02)
    raise AssertionError(
        f"systemd unit did not spawn a {expected_comm!r} child before timeout"
    )


def test_readonly_trusted_bootstrap_cannot_be_shadowed_by_workspace_package(tmp_path: Path) -> None:
    package = tmp_path / "orchestrator"
    package.mkdir()
    (package / "__init__.py").write_text("print('bootstrap hijacked'); raise SystemExit(21)")
    (tmp_path / "sitecustomize.py").write_text("print('site hijacked'); raise SystemExit(22)")
    result = SystemdReadOnlyLauncher().launch(
        tmp_path, ["/usr/bin/python3", "-I", "-S", "-c", "print('requested')"],
    ).wait()
    assert result.returncode == 0, result.stderr + result.stdout
    assert result.stdout == b"requested\n" and result.termination_confirmed
    _assert_empty_cgroup_receipt(result)


def test_launcher_enforces_filesystem_network_environment_and_resource_boundaries(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / ".git").mkdir()
    (workspace / ".git" / "config-marker").write_text("protected", encoding="utf-8")
    (workspace / ".maestro").mkdir()
    (workspace / ".maestro" / "state-marker").write_text("protected", encoding="utf-8")
    visible = workspace / "input.txt"
    visible.write_text("input", encoding="utf-8")
    home = str(Path.home())
    code = r"""
import json
import os
from pathlib import Path
import resource
import socket

result = {
    'input': Path('input.txt').read_text(),
    'nofile': resource.getrlimit(resource.RLIMIT_NOFILE)[0],
    'fsize': resource.getrlimit(resource.RLIMIT_FSIZE)[0],
    'clean_env': os.getenv('MAESTRO_EXPECT_MEMORY') is None and os.getenv('OPENAI_API_KEY') is None,
    'tmp_write': False,
    'workspace_write_blocked': False,
    'git_hidden': False,
    'control_hidden': False,
    'home_hidden': not Path(os.environ['TEST_HOST_HOME']).exists(),
    'etc_hidden': False,
    'network_blocked': False,
}
Path('/tmp/private-write').write_text('temporary')
result['tmp_write'] = True
try:
    Path('output.txt').write_text('should-not-write')
except OSError:
    result['workspace_write_blocked'] = True
for key, path in [('git_hidden', '.git/config-marker'), ('control_hidden', '.maestro/state-marker')]:
    try:
        Path(path).read_text()
    except OSError:
        result[key] = True
try:
    socket.socket(socket.AF_INET, socket.SOCK_STREAM)
except OSError:
    result['network_blocked'] = True
try:
    Path('/etc/shadow').read_text()
except OSError:
    result['etc_hidden'] = True
print(json.dumps(result, sort_keys=True))
"""
    # The host-home value is passed as a one-off test argument, never inherited.
    wrapped = "import os,sys; os.environ['TEST_HOST_HOME']=sys.argv[1]; exec(" + repr(code) + ")"
    session = SystemdReadOnlyLauncher().launch(
        workspace,
        ["/usr/bin/python3", "-c", wrapped, home],
        limits=SandboxLimits(
            memory_bytes=256 * 1024 * 1024,
            tasks=16,
            cpu_percent=50,
            timeout_seconds=10,
            output_bytes=16 * 1024,
            nofile=64,
            file_bytes=1024 * 1024,
        ),
    )
    result = session.wait()
    assert session.wait() is result

    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert result.termination_confirmed
    _assert_empty_cgroup_receipt(result)
    observed = json.loads(result.stdout)
    assert observed == {
        "clean_env": True,
        "control_hidden": True,
        "etc_hidden": True,
        "fsize": 1024 * 1024,
        "git_hidden": True,
        "home_hidden": True,
        "input": "input",
        "network_blocked": True,
        "nofile": 64,
        "tmp_write": True,
        "workspace_write_blocked": True,
    }
    assert not (workspace / "output.txt").exists()


def test_launcher_output_limit_kills_unbounded_writer(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    result = SystemdReadOnlyLauncher().launch(
        workspace,
        ["/usr/bin/python3", "-c", "import sys,time; [(sys.stdout.write('x'*4096+'\\n'),sys.stdout.flush()) for _ in iter(int,1)]"],
        limits=SandboxLimits(timeout_seconds=10, output_bytes=1024),
    ).wait()
    assert result.output_limited
    assert len(result.stdout) + len(result.stderr) <= 1024
    assert result.returncode != 0
    _assert_empty_cgroup_receipt(result)


def test_launcher_runtime_limit_terminates_command(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    result = SystemdReadOnlyLauncher().launch(
        workspace,
        ["/usr/bin/python3", "-c", "import time; time.sleep(30)"],
        limits=SandboxLimits(timeout_seconds=1, output_bytes=1024),
    ).wait()
    assert result.timed_out
    assert result.returncode != 0
    assert result.elapsed_seconds < 5
    _assert_empty_cgroup_receipt(result)


def test_launcher_cancellation_kills_entire_service_process_tree(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    code = "import subprocess,time; p=subprocess.Popen(['/bin/sleep','60']); time.sleep(60)"
    session = SystemdReadOnlyLauncher().launch(
        workspace,
        ["/usr/bin/python3", "-c", code],
        limits=SandboxLimits(timeout_seconds=30, output_bytes=1024),
    )
    results = []
    waiter = threading.Thread(target=lambda: results.append(session.wait()), daemon=True)
    waiter.start()
    child_pid = None
    try:
        child_pid = _wait_for_unit_child(session, expected_comm="sleep")
    finally:
        if child_pid is None:
            session.cancel()
            waiter.join(timeout=5)
    cancellation_accepted = session.cancel()
    waiter.join(timeout=5)
    assert not waiter.is_alive()
    assert cancellation_accepted
    assert results and results[0].cancelled
    assert results[0].termination_confirmed
    _assert_empty_cgroup_receipt(results[0])
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            os.kill(child_pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)
    else:
        pytest.fail("systemd cancellation left the child process alive")


def test_launcher_rejects_workspace_escape_before_execution(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    secret = tmp_path / "outside-secret"
    secret.write_text("must-not-read", encoding="utf-8")
    (workspace / "escape").symlink_to(secret)
    with pytest.raises(InvalidSandboxRequest, match="snapshot"):
        SystemdReadOnlyLauncher().launch(workspace, ["/usr/bin/python3", "-c", "print('bad')"])
