"""Trusted final pre-exec stage inside the private candidate filesystem root."""

from __future__ import annotations

import os
from pathlib import Path
import resource
import sys

from .candidate_security import restrict_candidate_syscalls, verify_candidate_privileges
from .landlock import FsAccess, PathGrant, READ_EXECUTE, WORKSPACE_WRITE, restrict_current_process


def main(arguments: list[str]) -> int:
    if len(arguments) < 2 or arguments[0] != "--":
        return 64
    try:
        verify_candidate_privileges()
        for name, limit in (("MAESTRO_EXPECT_NOFILE", resource.RLIMIT_NOFILE),
                            ("MAESTRO_EXPECT_FSIZE", resource.RLIMIT_FSIZE)):
            if resource.getrlimit(limit) != (int(os.environ[name]),) * 2:
                raise RuntimeError("candidate rlimit is not effective")
        grants = [PathGrant(Path("/usr"), READ_EXECUTE),
                  PathGrant(Path("/runtime"), READ_EXECUTE),
                  PathGrant(Path("/dev"), READ_EXECUTE | FsAccess.WRITE_FILE)]
        for path in ("/workspace", "/tmp"):
            grants.append(PathGrant(Path(path), WORKSPACE_WRITE | FsAccess.MAKE_SYM))
        restrict_current_process(grants)
        restrict_candidate_syscalls()
    except Exception as exc:
        print(f"candidate setup failed: {type(exc).__name__}", file=sys.stderr, flush=True)
        return 78
    environment = {
        "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "HOME": "/nonexistent", "TMPDIR": "/tmp",
    }
    try:
        os.execvpe(arguments[1], arguments[1:], environment)
    except OSError as exc:
        print(f"candidate command could not start: {exc.errno}", file=sys.stderr, flush=True)
        return 127
    return 127


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
