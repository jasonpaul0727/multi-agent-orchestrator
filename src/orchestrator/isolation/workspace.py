"""Preflight checks for a workspace passed to an OS sandbox."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import os
from pathlib import Path
import stat


class WorkspaceBoundaryError(RuntimeError):
    """A workspace tree cannot be proven to stay inside its declared root."""


@dataclass(frozen=True)
class WorkspaceInspection:
    """Non-sensitive summary produced by a complete, no-follow tree scan."""

    regular_files: int
    directories: int
    symlinks: int


def inspect_workspace(root: str | Path) -> WorkspaceInspection:
    """Reject mount, symlink, and hard-link paths that could escape ``root``.

    The scan does not follow symbolic links.  In-tree symbolic links are
    reported but left for the sandbox's own path policy to constrain.  Every
    regular-file inode must have all of its hard links inside the tree, and
    nested mount points or special files are rejected.
    """

    workspace = Path(root)
    try:
        root_stat = workspace.lstat()
        resolved_root = workspace.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceBoundaryError("workspace root cannot be inspected safely") from exc
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise WorkspaceBoundaryError("workspace root must be a real directory")

    inode_entries: Counter[tuple[int, int]] = Counter()
    inode_link_counts: dict[tuple[int, int], int] = {}
    directory_count = 1
    symlink_count = 0
    root_device = root_stat.st_dev

    def walk_error(_error: OSError) -> None:
        raise WorkspaceBoundaryError("workspace contains a directory that cannot be inspected")

    for current, directory_names, file_names in os.walk(
        workspace, topdown=True, followlinks=False, onerror=walk_error
    ):
        current_path = Path(current)
        for name in tuple(directory_names):
            path = current_path / name
            try:
                info = path.lstat()
            except OSError as exc:
                raise WorkspaceBoundaryError("workspace changed during boundary inspection") from exc
            if stat.S_ISLNK(info.st_mode):
                _require_in_tree_symlink(path, resolved_root)
                symlink_count += 1
                directory_names.remove(name)
                continue
            if not stat.S_ISDIR(info.st_mode):
                raise WorkspaceBoundaryError("workspace contains a non-directory path component")
            if info.st_dev != root_device or os.path.ismount(path):
                raise WorkspaceBoundaryError("workspace contains a nested mount point")
            directory_count += 1

        for name in file_names:
            path = current_path / name
            try:
                info = path.lstat()
            except OSError as exc:
                raise WorkspaceBoundaryError("workspace changed during boundary inspection") from exc
            if stat.S_ISLNK(info.st_mode):
                _require_in_tree_symlink(path, resolved_root)
                symlink_count += 1
                continue
            if not stat.S_ISREG(info.st_mode):
                raise WorkspaceBoundaryError("workspace contains a special file")
            if info.st_dev != root_device:
                raise WorkspaceBoundaryError("workspace contains a nested mount point")
            inode = (info.st_dev, info.st_ino)
            inode_entries[inode] += 1
            inode_link_counts[inode] = info.st_nlink

    for inode, observed_links in inode_entries.items():
        if inode_link_counts[inode] > observed_links:
            raise WorkspaceBoundaryError("workspace contains a hard link to an undeclared path")

    return WorkspaceInspection(
        regular_files=sum(inode_entries.values()),
        directories=directory_count,
        symlinks=symlink_count,
    )


def _require_in_tree_symlink(path: Path, root: Path) -> None:
    try:
        target = path.resolve(strict=False)
        target.relative_to(root)
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceBoundaryError("workspace symlink escapes its declared root") from exc
