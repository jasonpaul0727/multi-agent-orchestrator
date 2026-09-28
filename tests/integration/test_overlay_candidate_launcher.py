"""Real candidate execution under systemd, namespaces and OverlayFS."""

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

import orchestrator.isolation as isolation


def _systemd_available() -> bool:
    if not sys.platform.startswith("linux") or not shutil.which("systemd-run"):
        return False
    return subprocess.run(
        ["systemctl", "--user", "show-environment"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=5,
    ).returncode == 0


pytestmark = pytest.mark.skipif(
    not _systemd_available(), reason="live candidate backend requires Linux/systemd --user",
)


def test_candidate_write_is_private_and_command_has_no_host_authority(
    tmp_path: Path, monkeypatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "input.txt").write_text("original\n")
    for name in (".git", ".maestro"):
        (workspace / name).mkdir()
        (workspace / name / "marker").write_text("protected")
    secret = tmp_path / "host-secret"
    secret.write_text("canary-host-only")
    monkeypatch.setenv("MAESTRO_CANARY_SECRET", "canary-env-only")
    code = r'''
import ctypes, errno, json, os, socket
from pathlib import Path
Path('input.txt').write_text('candidate\n')
Path('new.txt').write_text('new candidate\n')
result = {'clean_env': os.getenv('MAESTRO_CANARY_SECRET') is None,
          'host_hidden': not Path(__import__('sys').argv[1]).exists(),
          'controls_hidden': not Path('.git/marker').exists() and not Path('.maestro/marker').exists(),
          'proc_hidden': not Path('/proc/1/root').exists(),
          'home_hidden': not Path('/home/paul2').exists(),
          'net_denied': False, 'unix_denied': False, 'chroot_denied': False,
          'mount_denied': False, 'root_immutable': False, 'xattr_denied': False}
for key, family in [('net_denied', socket.AF_INET), ('unix_denied', socket.AF_UNIX)]:
    try:
        socket.socket(family, socket.SOCK_STREAM)
    except OSError as exc:
        result[key] = exc.errno == errno.EPERM
libc = ctypes.CDLL(None, use_errno=True)
result['chroot_denied'] = libc.chroot(b'/tmp') == -1 and ctypes.get_errno() == errno.EPERM
result['mount_denied'] = libc.mount(b'none', b'/tmp', b'tmpfs', 0, None) == -1 and ctypes.get_errno() == errno.EPERM
try:
    os.chmod('/', 0o777)
except OSError as exc:
    result['root_immutable'] = exc.errno == errno.EROFS
try:
    os.setxattr('input.txt', 'user.overlay.origin', b'forged')
except OSError as exc:
    result['xattr_denied'] = exc.errno == errno.EPERM
print(json.dumps(result, sort_keys=True))
'''
    launcher = isolation.SystemdOverlayCandidateLauncher()
    with launcher.launch(workspace, ["/usr/bin/python3", "-c", code, str(secret)]) as session:
        result = session.wait()
        assert result.execution.returncode == 0, result.execution.stderr.decode(errors="replace")
        assert result.execution.termination_confirmed
        assert json.loads(result.execution.stdout) == {
            "clean_env": True, "host_hidden": True, "controls_hidden": True,
            "proc_hidden": True, "home_hidden": True, "net_denied": True,
            "unix_denied": True, "chroot_denied": True, "mount_denied": True,
            "root_immutable": True, "xattr_denied": True,
        }
        assert result.diff is not None
        assert [(entry.path, entry.operation) for entry in result.diff.entries] == [
            ("input.txt", "modify"), ("new.txt", "add"),
        ]
        candidate = result.diff.candidate_root
        assert (candidate / "input.txt").read_text() == "candidate\n"
        assert (candidate / "new.txt").read_text() == "new candidate\n"
        assert (result.lower_root / "input.txt").read_text() == "original\n"
        isolation.validate_overlay_candidate(result.lower_root, result.diff)
        assert session.wait() is result
    assert not candidate.exists()
    assert (workspace / "input.txt").read_text() == "original\n"
    assert not (workspace / "new.txt").exists()
    assert secret.read_text() == "canary-host-only"


def test_candidate_session_forwards_bounded_stdin_and_reaps_detached_children(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "input.txt").write_text("original")
    code = r'''
import os, sys, threading, time
from pathlib import Path
data = sys.stdin.buffer.read()
thread = threading.Thread(target=lambda: Path('input.txt').write_text('early'))
thread.start()
thread.join()
if os.fork() == 0:
    os.setsid()
    time.sleep(10)
    Path('input.txt').write_text('late')
    os._exit(0)
print(len(data))
'''
    with isolation.SystemdOverlayCandidateLauncher().launch(
        workspace, ["/usr/bin/python3", "-c", code], input_bytes=b"x" * 80_000,
    ) as session:
        result = session.wait()
        assert result.execution.returncode == 0, result.execution.stderr
        assert result.execution.termination_confirmed and result.execution.input_written
        assert result.execution.elapsed_seconds < 5
        assert result.execution.stdout == b"80000\n"
        assert result.diff is not None
        assert (result.diff.candidate_root / "input.txt").read_text() == "early"
    assert (workspace / "input.txt").read_text() == "original"


@pytest.mark.parametrize("operation", [
    "Path('input.txt').unlink()",
    "os.link('input.txt', 'hardlink')",
    "Path('escape').symlink_to('../../host-secret')",
    "Path('.git').mkdir(); Path('.git/config').write_text('tamper')",
    "Path('a').write_text('one'); Path('b').write_text('two')",
], ids=["whiteout", "hardlink", "escaping-symlink", "control-path", "entry-limit"])
def test_candidate_export_rejects_unsupported_or_unsafe_changes(tmp_path: Path, operation: str) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "input.txt").write_text("original")
    with isolation.SystemdOverlayCandidateLauncher(candidate_entries=1).launch(
        workspace, ["/usr/bin/python3", "-c", "import os; from pathlib import Path; " + operation],
    ) as session:
        result = session.wait()
        assert result.execution.termination_confirmed
        assert result.execution.returncode != 0
        assert result.diff is None
    assert (workspace / "input.txt").read_text() == "original"


@pytest.mark.parametrize(("kind", "code"), [
    ("timeout", "import time; time.sleep(20)"),
    ("output", "import os;\nwhile True: os.write(1, b'x'*4096)"),
    ("disk", "from pathlib import Path; Path('large').write_bytes(b'x' * (2*1024*1024))"),
    ("file", "from pathlib import Path; Path('large').write_bytes(b'x' * (128*1024))"),
    ("failed", "from pathlib import Path; Path('partial').write_text('partial'); raise SystemExit(7)"),
])
def test_resource_or_execution_failure_never_returns_candidate(tmp_path: Path, kind: str, code: str) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    limits = isolation.SandboxLimits(
        timeout_seconds=1 if kind == "timeout" else 10,
        output_bytes=1024 if kind == "output" else 16_384,
        file_bytes=64*1024 if kind == "file" else 4*1024*1024,
    )
    with isolation.SystemdOverlayCandidateLauncher(candidate_bytes=1024*1024).launch(
        workspace, ["/usr/bin/python3", "-c", code], limits=limits,
    ) as session:
        result = session.wait()
        assert result.diff is None
        assert result.execution.termination_confirmed
        assert result.execution.returncode != 0
        if kind == "timeout":
            assert result.execution.timed_out
        elif kind == "output":
            assert result.execution.output_limited
            assert len(result.execution.stdout) + len(result.execution.stderr) <= 1024
        elif kind == "disk":
            assert b"No space left on device" in result.execution.stderr
        elif kind == "file":
            assert b"File too large" in result.execution.stderr
    assert list(workspace.iterdir()) == []


def test_cancellation_stops_scope_and_removes_only_private_staging(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    with isolation.SystemdOverlayCandidateLauncher().launch(
        workspace, ["/usr/bin/python3", "-c", "import time; time.sleep(20)"],
    ) as session:
        captured = []
        waiter = threading.Thread(target=lambda: captured.append(session.wait()))
        waiter.start()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            query = subprocess.run(
                ["systemctl", "--user", "is-active", session.unit_name], capture_output=True, check=False,
            )
            if query.stdout.strip() == b"active":
                break
            time.sleep(0.02)
        assert session.cancel()
        waiter.join(5)
        assert not waiter.is_alive()
        result, = captured
        assert result.diff is None
        assert result.execution.cancelled and result.execution.termination_confirmed
        stage = result.lower_root.parent
    assert not stage.exists()
    assert workspace.exists()


def test_candidate_is_compatible_with_explicit_host_lease_and_journal_publisher(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "input.txt").write_text("original")
    leases = tmp_path / "leases"
    journal = tmp_path / "journal"
    leases.mkdir(mode=0o700)
    journal.mkdir(mode=0o700)
    with isolation.SystemdOverlayCandidateLauncher().launch(
        workspace, ["/usr/bin/python3", "-c", "from pathlib import Path; Path('input.txt').write_text('candidate')"],
    ) as session:
        result = session.wait()
        assert result.diff is not None, result.execution.stderr
        assert (workspace / "input.txt").read_text() == "original"
        with isolation.acquire_workspace_write_lease(workspace, leases) as lease:
            receipt = isolation.publish_workspace_diff(result.lower_root, workspace, result.diff, lease, journal)
        assert receipt.entries_published == 1
    assert (workspace / "input.txt").read_text() == "candidate"
    assert list(journal.iterdir()) == []


@pytest.mark.parametrize("untrusted_host_cwd", [False, True])
def test_workspace_python_package_cannot_shadow_trusted_bootstraps(tmp_path: Path, monkeypatch, untrusted_host_cwd) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    package = workspace / "orchestrator"
    package.mkdir()
    (package / "__init__.py").write_text("print('bootstrap hijacked'); raise SystemExit(21)")
    (workspace / "sitecustomize.py").write_text("print('site hijacked'); raise SystemExit(22)")
    if untrusted_host_cwd:
        monkeypatch.chdir(workspace)
    with isolation.SystemdOverlayCandidateLauncher().launch(
        workspace, ["/usr/bin/python3", "-I", "-S", "-c", "from pathlib import Path; Path('requested.txt').write_text('requested')"],
    ) as session:
        result = session.wait()
        assert result.execution.returncode == 0, result.execution.stderr + result.execution.stdout
        assert result.execution.stdout == b""
        assert result.diff is not None
        assert [(entry.path, entry.operation) for entry in result.diff.entries] == [("requested.txt", "add")]
    assert not (workspace / "requested.txt").exists()


def test_runtime_deadline_kills_command_ignoring_sigterm(tmp_path) -> None:
    with isolation.SystemdOverlayCandidateLauncher().launch(
        tmp_path, ["/usr/bin/python3", "-c", "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready', flush=True); time.sleep(30)"],
        limits=isolation.SandboxLimits(timeout_seconds=1),
    ) as session:
        results = []
        waiter = threading.Thread(target=lambda: results.append(session.wait()))
        waiter.start()
        waiter.join(8)
        if waiter.is_alive():
            # Emergency cleanup keeps a regression bounded without trusting cancel().
            subprocess.run(["systemctl", "--user", "kill", "--kill-whom=all", "--signal=SIGKILL", session.unit_name], check=False)
            waiter.join(5)
            pytest.fail("deadline did not stop SIGTERM-resistant descendants promptly")
        result, = results
        assert result.execution.timed_out and result.execution.termination_confirmed
        assert result.execution.elapsed_seconds < 8
        assert result.execution.stdout == b"ready\n"
        assert result.diff is None
