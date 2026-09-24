"""Trusted systemd service bootstrap; never pass its environment to the tool."""

from __future__ import annotations

import os
from pathlib import Path
import resource
import sys

from .landlock import (
    LandlockUnavailable,
    FsAccess,
    PathGrant,
    READ_EXECUTE,
    WORKSPACE_WRITE,
    restrict_current_process,
)


def main(arguments: list[str]) -> int:
    if not arguments or arguments[0] != "--" or len(arguments) < 2:
        return 64
    try:
        _verify_limits()
        restrict_current_process(_runtime_grants())
    except Exception as exc:  # Fail closed before launching any requested code.
        detail = str(exc) if isinstance(exc, LandlockUnavailable) else type(exc).__name__
        print(f"sandbox setup failed: {detail}", file=sys.stderr, flush=True)
        return 78

    clean_env = {
        "PATH": "/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "HOME": "/nonexistent",
        "TMPDIR": "/tmp",
    }
    try:
        os.execvpe(arguments[1], arguments[1:], clean_env)
    except OSError as exc:
        print(f"sandbox command could not start: {exc.errno}", file=sys.stderr, flush=True)
        return 127
    return 127


def _runtime_grants() -> list[PathGrant]:
    grants: list[PathGrant] = []
    seen: set[Path] = set()

    # Keep executable/runtime roots explicit; never grant the host filesystem
    # root. Sensitive /etc credential paths are hidden by the unit mount rules.
    for path in (Path("/usr"), Path("/bin"), Path("/lib"), Path("/lib64")):
        if path.exists():
            _add_grant(grants, seen, path, READ_EXECUTE)
    if Path("/etc").exists():
        _add_grant(grants, seen, Path("/etc"), READ_EXECUTE)
    if Path("/dev").exists():
        _add_grant(grants, seen, Path("/dev"), READ_EXECUTE | FsAccess.WRITE_FILE)
    # Workspace/runtime binds and scratch space are deliberately staged below
    # the systemd-private /tmp; systemd mount flags keep the two binds read-only.
    scratch_rights = WORKSPACE_WRITE | FsAccess.MAKE_SYM | FsAccess.MAKE_SOCK | FsAccess.MAKE_FIFO
    _add_grant(grants, seen, Path("/tmp"), scratch_rights)
    return grants


def _add_grant(grants: list[PathGrant], seen: set[Path], path: Path, access) -> None:
    resolved = path.resolve(strict=True)
    if resolved not in seen:
        grants.append(PathGrant(resolved, access))
        seen.add(resolved)


def _verify_limits() -> None:
    expected_memory = int(os.environ["MAESTRO_EXPECT_MEMORY"])
    expected_tasks = int(os.environ["MAESTRO_EXPECT_TASKS"])
    expected_cpu = int(os.environ["MAESTRO_EXPECT_CPU"])
    expected_nofile = int(os.environ["MAESTRO_EXPECT_NOFILE"])
    expected_fsize = int(os.environ["MAESTRO_EXPECT_FSIZE"])

    cgroup_path = None
    for line in Path("/proc/self/cgroup").read_text(encoding="ascii").splitlines():
        if line.startswith("0::"):
            cgroup_path = line[3:]
            break
    if cgroup_path is None:
        raise RuntimeError("cgroup v2 is not active")
    cgroup = (Path("/sys/fs/cgroup") / cgroup_path.lstrip("/")).resolve(strict=True)
    cgroup_root = Path("/sys/fs/cgroup").resolve(strict=True)
    cgroup.relative_to(cgroup_root)

    if (cgroup / "memory.max").read_text(encoding="ascii").strip() != str(expected_memory):
        raise RuntimeError("MemoryMax is not effective")
    if (cgroup / "memory.swap.max").read_text(encoding="ascii").strip() != "0":
        raise RuntimeError("MemorySwapMax is not effective")
    if (cgroup / "pids.max").read_text(encoding="ascii").strip() != str(expected_tasks):
        raise RuntimeError("TasksMax is not effective")
    cpu_fields = (cgroup / "cpu.max").read_text(encoding="ascii").split()
    if cpu_fields != [str(expected_cpu * 1000), "100000"]:
        raise RuntimeError("CPUQuota is not effective")
    if resource.getrlimit(resource.RLIMIT_NOFILE)[0] != expected_nofile:
        raise RuntimeError("LimitNOFILE is not effective")
    if resource.getrlimit(resource.RLIMIT_FSIZE)[0] != expected_fsize:
        raise RuntimeError("LimitFSIZE is not effective")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
