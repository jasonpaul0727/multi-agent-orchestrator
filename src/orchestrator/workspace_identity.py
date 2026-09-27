"""Stable, privacy-preserving identity for a host-bound Run workspace."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat


def workspace_identity_hash(workspace: str | Path) -> str:
    """Return a path/dev/inode binding for one real workspace directory.

    The absolute path is hashed, never persisted in the Run event. Directory
    replacement at the same path and moving a bound directory both change the
    identity. A final-component symlink and filesystem root are rejected.
    """

    path = Path(workspace)
    if not path.is_absolute():
        raise ValueError("workspace must be an absolute directory")
    try:
        before = path.lstat()
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError("workspace identity cannot be established") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
        raise ValueError("workspace must be a real directory")
    try:
        canonical = path.resolve(strict=True)
        opened = canonical.stat(follow_symlinks=False)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError("workspace identity cannot be established") from exc
    if canonical == Path(canonical.anchor):
        raise ValueError("workspace must not be a filesystem root")
    if (
        not stat.S_ISDIR(opened.st_mode)
        or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
    ):
        raise ValueError("workspace changed while its identity was checked")

    return workspace_identity_hash_from_stat(canonical, opened.st_dev, opened.st_ino)


def workspace_identity_hash_from_stat(workspace: str | Path, device: int, inode: int) -> str:
    """Hash a canonical path with the identity from an already-open directory FD."""

    path = Path(workspace)
    if not path.is_absolute() or path == Path(path.anchor):
        raise ValueError("workspace must be a non-root absolute path")
    if (
        isinstance(device, bool) or not isinstance(device, int) or device < 0
        or isinstance(inode, bool) or not isinstance(inode, int) or inode < 0
    ):
        raise ValueError("workspace device and inode must be non-negative integers")
    digest = hashlib.sha256()
    digest.update(b"maestro-workspace-identity-v1\0")
    digest.update(os.fsencode(path))
    digest.update(b"\0")
    digest.update(str(device).encode("ascii"))
    digest.update(b"\0")
    digest.update(str(inode).encode("ascii"))
    return "sha256:" + digest.hexdigest()


__all__ = ["workspace_identity_hash", "workspace_identity_hash_from_stat"]
