"""Live probes for the supported Linux isolation primitives.

These tests invoke a transient *user* systemd unit and real kernel namespaces;
they are not mocks and are intentionally Linux/systemd gated.
"""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import shutil
import select
import socket
import subprocess
import sys
import textwrap
import uuid

import pytest


def _systemd_user_available() -> bool:
    if not sys.platform.startswith("linux") or shutil.which("systemd-run") is None:
        return False
    result = subprocess.run(
        ["systemctl", "--user", "show-environment"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=5,
    )
    return result.returncode == 0


pytestmark = pytest.mark.skipif(
    not _systemd_user_available(),
    reason="live isolation probe requires Linux and a usable systemd user manager",
)


def _run_unit(*, unit: str, properties: tuple[str, ...], command: list[str]) -> subprocess.CompletedProcess[str]:
    args = ["systemd-run", "--user", "--quiet", "--wait", "--pipe", "--collect", f"--unit={unit}"]
    args.extend(f"--property={value}" for value in properties)
    args.extend(("--", *command))
    return subprocess.run(
        args,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
        env=os.environ.copy(),
    )


def _base_properties(workspace: Path) -> tuple[str, ...]:
    target = workspace.parent / "sandbox-target"
    target.mkdir(exist_ok=True)
    readonly_binds: list[str] = []
    for name in (".git", ".maestro"):
        source = workspace / name
        destination = target / name
        if source.is_dir():
            destination.mkdir(exist_ok=True)
        elif source.is_file():
            destination.touch(exist_ok=True)
        else:
            continue
        readonly_binds.append(f"BindReadOnlyPaths={source}:{destination}")
    return (
        "PrivateNetwork=yes",
        "PrivateTmp=yes",
        "PrivateUsers=yes",
        "ProtectHome=tmpfs",
        "ProtectSystem=strict",
        "ProtectProc=invisible",
        "NoNewPrivileges=yes",
        "CapabilityBoundingSet=",
        "InaccessiblePaths=/run/user",
        "InaccessiblePaths=/run/dbus",
        "RestrictAddressFamilies=AF_UNIX",
        f"BindPaths={workspace}:{target}",
        f"ReadWritePaths={target}",
        *readonly_binds,
        f"WorkingDirectory={target}",
    )


def test_transient_unit_enforces_cgroup_and_rlimit_values():
    code = textwrap.dedent(
        """
        import json
        from pathlib import Path
        import resource

        cgroup = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1].lstrip("/")
        values = {
            "memory_max": (cgroup / "memory.max").read_text().strip(),
            "pids_max": (cgroup / "pids.max").read_text().strip(),
            "cpu_max": (cgroup / "cpu.max").read_text().strip(),
            "nofile": resource.getrlimit(resource.RLIMIT_NOFILE)[0],
            "fsize": resource.getrlimit(resource.RLIMIT_FSIZE)[0],
        }
        print(json.dumps(values, sort_keys=True))
        """
    )
    result = _run_unit(
        unit=f"maestro-isolation-limits-{uuid.uuid4().hex}.service",
        properties=(
            "MemoryMax=268435456",
            "TasksMax=16",
            "CPUQuota=50%",
            "RuntimeMaxSec=20",
            "LimitNOFILE=64",
            "LimitFSIZE=1048576",
            "LimitCORE=0",
            "PrivateNetwork=yes",
            "NoNewPrivileges=yes",
        ),
        command=["/usr/bin/python3", "-c", code],
    )

    assert result.returncode == 0, result.stderr
    limits = json.loads(result.stdout)
    assert limits == {
        "cpu_max": "50000 100000",
        "fsize": 1048576,
        "memory_max": "268435456",
        "nofile": 64,
        "pids_max": "16",
    }


def test_transient_unit_kills_worker_when_memory_limit_is_exceeded():
    code = "payload = bytearray(128 * 1024 * 1024); payload[::4096] = b'x' * (len(payload) // 4096); print('allocated')"
    result = _run_unit(
        unit=f"maestro-isolation-memory-{uuid.uuid4().hex}.service",
        properties=(
            "MemoryMax=67108864",
            "MemorySwapMax=0",
            "TasksMax=8",
            "RuntimeMaxSec=15",
            "LimitNOFILE=64",
            "PrivateNetwork=yes",
        ),
        command=["/usr/bin/python3", "-c", code],
    )

    assert result.returncode != 0
    assert "allocated" not in result.stdout


def test_pids_limit_blocks_extra_process_and_private_network_home_and_controls(
    tmp_path: Path,
):
    workspace = tmp_path / "sandbox-workspace"
    (workspace / ".git").mkdir(parents=True)
    (workspace / ".maestro").mkdir()
    (workspace / ".git" / "config-marker").write_text("control", encoding="utf-8")
    (workspace / ".maestro" / "state-marker").write_text("control", encoding="utf-8")
    outside_secret = tmp_path / "outside-secret"
    outside_secret.write_text("host-only", encoding="utf-8")
    (workspace / "escape-link").symlink_to(outside_secret)
    sandbox_root = workspace.parent / "sandbox-target"
    sandbox_root.mkdir()

    code = textwrap.dedent(
        """
        import errno
        import json
        import os
        from pathlib import Path
        import socket

        result = {"workspace_write": False, "git_write_blocked": False,
                  "control_write_blocked": False, "escape_read_blocked": False,
                  "home_hidden": False, "bus_hidden": False, "network_blocked": False,
                  "pids_limit": False}
        Path("/workspace/output.txt").write_text("candidate", encoding="utf-8")
        result["workspace_write"] = True
        for name, path in (("git_write_blocked", "/workspace/.git/config-marker"),
                           ("control_write_blocked", "/workspace/.maestro/state-marker")):
            try:
                Path(path).write_text("tamper", encoding="utf-8")
            except OSError:
                result[name] = True
        try:
            Path("/workspace/escape-link").read_text(encoding="utf-8")
        except OSError:
            result["escape_read_blocked"] = True
        result["home_hidden"] = not Path("/home/paul2").exists()
        try:
            Path("/run/user/1000/bus").stat()
        except OSError:
            result["bus_hidden"] = True
        try:
            socket.create_connection(("1.1.1.1", 443), timeout=0.25)
        except OSError:
            result["network_blocked"] = True

        read_fd, write_fd = os.pipe()
        child = os.fork()
        if child == 0:
            os.close(write_fd)
            os.read(read_fd, 1)
            os._exit(0)
        os.close(read_fd)
        try:
            os.fork()
        except OSError as exc:
            result["pids_limit"] = exc.errno == errno.EAGAIN
        finally:
            os.write(write_fd, b"x")
            os.close(write_fd)
            os.waitpid(child, 0)
        print(json.dumps(result, sort_keys=True))
        """
    )
    result = _run_unit(
        unit=f"maestro-isolation-boundary-{uuid.uuid4().hex}.service",
        properties=(
            *_base_properties(workspace),
            "TasksMax=2",
            "MemoryMax=268435456",
            "CPUQuota=50%",
            "RuntimeMaxSec=20",
            "LimitNOFILE=64",
            "LimitFSIZE=1048576",
            "LimitCORE=0",
        ),
        command=["/usr/bin/python3", "-c", code.replace("/workspace", str(sandbox_root))],
    )

    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout)
    assert observed == {
        "bus_hidden": True,
        "control_write_blocked": True,
        "escape_read_blocked": True,
        "git_write_blocked": True,
        "home_hidden": True,
        "network_blocked": True,
        "pids_limit": True,
        "workspace_write": True,
    }
    assert (workspace / "output.txt").read_text(encoding="utf-8") == "candidate"
    assert (workspace / ".git" / "config-marker").read_text(encoding="utf-8") == "control"
    assert (workspace / ".maestro" / "state-marker").read_text(encoding="utf-8") == "control"


def test_git_worktree_pointer_cannot_reach_host_control_directory(tmp_path: Path):
    if shutil.which("git") is None:
        pytest.skip("Git worktree probe requires git")
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(["git", "init", str(repository)], check=True, capture_output=True, text=True)
    (repository / "README.txt").write_text("baseline", encoding="utf-8")
    subprocess.run(["git", "-C", str(repository), "add", "README.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "-c", "user.name=Probe", "-c",
         "user.email=probe@example.invalid", "commit", "-m", "baseline"],
        check=True,
        capture_output=True,
        text=True,
    )
    worktree = tmp_path / "agent-worktree"
    subprocess.run(
        ["git", "-C", str(repository), "worktree", "add", "-b", "agent-probe", str(worktree)],
        check=True,
        capture_output=True,
        text=True,
    )
    (worktree / ".maestro").mkdir()
    target = worktree.parent / "sandbox-target"
    target.mkdir()

    code = textwrap.dedent(
        """
        import json
        from pathlib import Path
        result = {"worktree_read": Path("/workspace/README.txt").read_text(encoding="utf-8") == "baseline",
                  "git_metadata_hidden": False, "git_pointer_ro": False}
        pointer = Path("/workspace/.git").read_text(encoding="utf-8").strip()
        metadata = Path(pointer.removeprefix("gitdir: "))
        try:
            (metadata / "HEAD").read_text(encoding="utf-8")
        except OSError:
            result["git_metadata_hidden"] = True
        try:
            Path("/workspace/.git").write_text("tamper", encoding="utf-8")
        except OSError:
            result["git_pointer_ro"] = True
        print(json.dumps(result, sort_keys=True))
        """
    ).replace("/workspace", str(target))
    result = _run_unit(
        unit=f"maestro-isolation-worktree-{uuid.uuid4().hex}.service",
        properties=(
            *_base_properties(worktree),
            "TasksMax=16",
            "MemoryMax=268435456",
            "RuntimeMaxSec=20",
            "LimitNOFILE=64",
        ),
        command=["/usr/bin/python3", "-c", code],
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "git_metadata_hidden": True,
        "git_pointer_ro": True,
        "worktree_read": True,
    }
    pointer = (worktree / ".git").read_text(encoding="utf-8").strip()
    host_gitdir = Path(pointer.removeprefix("gitdir: "))
    assert (host_gitdir / "HEAD").is_file()
    assert (repository / "README.txt").read_text(encoding="utf-8") == "baseline"


def test_transient_unit_kill_stops_its_entire_process_tree():
    unit = f"maestro-isolation-kill-{uuid.uuid4().hex}.service"
    process = subprocess.Popen(
        [
            "systemd-run", "--user", "--quiet", "--wait", "--pipe",
            f"--unit={unit}", "--property=TasksMax=8", "--property=RuntimeMaxSec=60",
            "--", "/bin/sh", "-c", "sleep 60 & printf '%s\\n' \"$!\"; wait",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )
    assert process.stdout is not None
    ready, _, _ = select.select([process.stdout], [], [], 5)
    assert ready, "worker did not report its child process"
    child_pid = int(process.stdout.readline().strip())

    killed = subprocess.run(
        ["systemctl", "--user", "kill", "--kill-whom=all", "--signal=SIGKILL", unit],
        text=True,
        capture_output=True,
        check=False,
        timeout=5,
        env=os.environ.copy(),
    )
    assert killed.returncode == 0, killed.stderr
    _, stderr = process.communicate(timeout=5)
    assert process.returncode != 0, stderr

    try:
        state = Path(f"/proc/{child_pid}/stat").read_text(encoding="utf-8").split()[2]
    except FileNotFoundError:
        state = "gone"
    assert state in {"gone", "Z"}, f"child process remains executable with state {state}"


def test_unprivileged_user_namespace_supports_bounded_overlay_workspace(tmp_path: Path):
    if shutil.which("unshare") is None or shutil.which("mount") is None:
        pytest.skip("overlay isolation probe requires unshare and mount")
    lower = tmp_path / "lower"
    lower.mkdir()
    (lower / "baseline.txt").write_text("original", encoding="utf-8")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    merged = tmp_path / "merged"
    merged.mkdir()
    code = textwrap.dedent(
        f"""
        import json
        import os
        from pathlib import Path
        import subprocess

        lower = Path({str(lower)!r})
        scratch = Path({str(scratch)!r})
        merged = Path({str(merged)!r})
        subprocess.run(["mount", "--make-rprivate", "/"], check=True)
        subprocess.run(["mount", "-t", "tmpfs", "-o", "size=4194304", "tmpfs", str(scratch)], check=True)
        upper = scratch / "upper"
        work = scratch / "work"
        upper.mkdir()
        work.mkdir()
        options = f"lowerdir={{lower}},upperdir={{upper}},workdir={{work}}"
        subprocess.run(["mount", "-t", "overlay", "overlay", "-o", options, str(merged)], check=True)
        assert (merged / "baseline.txt").read_text(encoding="utf-8") == "original"
        (merged / "new-output.txt").write_text("candidate", encoding="utf-8")
        stat = os.statvfs(scratch)
        capacity_limited = False
        try:
            (scratch / "capacity-probe.bin").write_bytes(b"x" * (8 * 1024 * 1024))
        except OSError:
            capacity_limited = True
        print(json.dumps({{"tmpfs_limit_bytes": stat.f_frsize * stat.f_blocks,
                          "output_visible": (merged / "new-output.txt").read_text(encoding="utf-8"),
                          "tmpfs_write_limited": capacity_limited,
                          "lower_unchanged": not (lower / "new-output.txt").exists()}}))
        """
    )
    result = subprocess.run(
        [
            "unshare", "--user", "--map-root-user", "--mount", "--fork", "--",
            "/usr/bin/python3", "-c", code,
        ],
        text=True,
        capture_output=True,
        check=False,
        timeout=15,
    )

    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout)
    assert observed["lower_unchanged"] is True
    assert observed["output_visible"] == "candidate"
    assert observed["tmpfs_limit_bytes"] <= 4 * 1024 * 1024
    assert observed["tmpfs_write_limited"] is True
    assert (lower / "baseline.txt").read_text(encoding="utf-8") == "original"
    assert not (lower / "new-output.txt").exists()
