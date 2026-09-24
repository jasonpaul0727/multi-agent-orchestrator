"""Preflight checks and bounded snapshots for OS-isolated workspaces."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import stat


class WorkspaceBoundaryError(RuntimeError):
    """A workspace tree cannot be proven to stay inside its declared root."""


@dataclass(frozen=True)
class WorkspaceInspection:
    """Non-sensitive summary produced by a complete, no-follow tree scan."""

    regular_files: int
    directories: int
    symlinks: int


def snapshot_workspace(
    root: str | Path,
    destination: str | Path,
    *,
    max_entries: int = 1_000_000,
    max_bytes: int = 4 * 1024 * 1024 * 1024,
    max_depth: int = 256,
) -> WorkspaceInspection:
    """Copy a bounded, no-follow workspace snapshot using directory handles.

    The source root is opened once and every descendant is opened relative to
    an already-open directory with ``O_NOFOLLOW``. Only regular files,
    directories, and lexically in-root relative symlinks are copied. The
    control directories ``.git`` and ``.maestro`` are omitted. The resulting
    tree has no writable mode bits and is suitable for a read-only bind mount.
    """

    for name, value, minimum in (
        ("max_entries", max_entries, 1),
        ("max_bytes", max_bytes, 0),
        ("max_depth", max_depth, 1),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise WorkspaceBoundaryError(f"{name} must be a valid positive bound")

    source = Path(root)
    target = Path(destination)
    try:
        initial = source.lstat()
        if stat.S_ISLNK(initial.st_mode) or not stat.S_ISDIR(initial.st_mode):
            raise WorkspaceBoundaryError("workspace root must be a real directory")
        resolved_source = source.resolve(strict=True)
        resolved_target = target.resolve(strict=False)
        try:
            resolved_target.relative_to(resolved_source)
        except ValueError:
            pass
        else:
            raise WorkspaceBoundaryError("snapshot destination must be outside the source workspace")
        source_fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except WorkspaceBoundaryError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceBoundaryError("workspace root cannot be opened safely") from exc

    root_stat = os.fstat(source_fd)
    if (root_stat.st_dev, root_stat.st_ino) != (initial.st_dev, initial.st_ino):
        os.close(source_fd)
        raise WorkspaceBoundaryError("workspace root changed while opening its snapshot")
    try:
        if target.exists() or target.is_symlink():
            raise WorkspaceBoundaryError("snapshot destination must not already exist")
        target.mkdir(mode=0o700, parents=False)
    except WorkspaceBoundaryError:
        os.close(source_fd)
        raise
    except OSError as exc:
        os.close(source_fd)
        raise WorkspaceBoundaryError("snapshot destination cannot be created") from exc

    entries = 1
    bytes_copied = 0
    file_count = 0
    directory_count = 1
    symlink_count = 0
    directory_paths: list[Path] = []
    try:
        root_mount_id = _descriptor_mount_id(source_fd)
    except BaseException:
        os.close(source_fd)
        shutil.rmtree(target, ignore_errors=True)
        raise

    def add_entry() -> None:
        nonlocal entries
        entries += 1
        if entries > max_entries:
            raise WorkspaceBoundaryError("workspace exceeds the snapshot entry limit")

    def validate_relative_symlink(relative_parent: tuple[str, ...], link: str) -> None:
        if not link or os.path.isabs(link):
            raise WorkspaceBoundaryError("workspace snapshot only allows relative symlinks")
        parts = list(relative_parent)
        for component in link.split("/"):
            if component in ("", "."):
                continue
            if component == "..":
                if not parts:
                    raise WorkspaceBoundaryError("workspace symlink escapes its declared root")
                parts.pop()
            else:
                parts.append(component)

    def copy_directory(source_fd: int, output: Path, relative: tuple[str, ...], depth: int) -> None:
        nonlocal bytes_copied, file_count, directory_count, symlink_count
        if depth > max_depth:
            raise WorkspaceBoundaryError("workspace exceeds the snapshot depth limit")
        try:
            names = sorted(os.listdir(source_fd))
        except OSError as exc:
            raise WorkspaceBoundaryError("workspace directory cannot be read safely") from exc
        for name in names:
            if name in (".", "..") or "/" in name or "\x00" in name:
                raise WorkspaceBoundaryError("workspace contains an invalid directory entry")
            if not relative and name in (".git", ".maestro"):
                continue
            try:
                before = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
            except OSError as exc:
                raise WorkspaceBoundaryError("workspace changed during snapshot inspection") from exc
            add_entry()
            output_path = output / name
            child_relative = (*relative, name)
            if stat.S_ISLNK(before.st_mode):
                try:
                    link = os.readlink(name, dir_fd=source_fd)
                    after = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
                except OSError as exc:
                    raise WorkspaceBoundaryError("workspace symlink changed during snapshot") from exc
                if (before.st_dev, before.st_ino, before.st_mode) != (after.st_dev, after.st_ino, after.st_mode):
                    raise WorkspaceBoundaryError("workspace symlink changed during snapshot")
                validate_relative_symlink(relative, link)
                os.symlink(link, output_path)
                symlink_count += 1
                continue
            if stat.S_ISDIR(before.st_mode):
                if before.st_dev != root_stat.st_dev:
                    raise WorkspaceBoundaryError("workspace contains a nested mount point")
                try:
                    child_fd = os.open(
                        name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                        dir_fd=source_fd,
                    )
                except OSError as exc:
                    raise WorkspaceBoundaryError("workspace directory changed during snapshot") from exc
                try:
                    opened = os.fstat(child_fd)
                    if not stat.S_ISDIR(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                        raise WorkspaceBoundaryError("workspace directory changed during snapshot")
                    if _descriptor_mount_id(child_fd) != root_mount_id:
                        raise WorkspaceBoundaryError("workspace contains a nested mount point")
                    output_path.mkdir(mode=0o700)
                    directory_count += 1
                    copy_directory(child_fd, output_path, child_relative, depth + 1)
                    directory_paths.append(output_path)
                finally:
                    os.close(child_fd)
                continue
            if not stat.S_ISREG(before.st_mode) or before.st_dev != root_stat.st_dev:
                raise WorkspaceBoundaryError("workspace contains a special file or nested mount")
            if before.st_size > max_bytes - bytes_copied:
                raise WorkspaceBoundaryError("workspace exceeds the snapshot byte limit")
            try:
                file_fd = os.open(
                    name,
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                    dir_fd=source_fd,
                )
            except OSError as exc:
                raise WorkspaceBoundaryError("workspace file changed during snapshot") from exc
            try:
                opened = os.fstat(file_fd)
                if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                    raise WorkspaceBoundaryError("workspace file changed during snapshot")
                if _descriptor_mount_id(file_fd) != root_mount_id:
                    raise WorkspaceBoundaryError("workspace contains a nested mount point")
                with output_path.open("xb") as output_file:
                    while True:
                        chunk = os.read(file_fd, 1024 * 1024)
                        if not chunk:
                            break
                        bytes_copied += len(chunk)
                        if bytes_copied > max_bytes:
                            raise WorkspaceBoundaryError("workspace exceeds the snapshot byte limit")
                        output_file.write(chunk)
                output_path.chmod(0o555 if opened.st_mode & 0o111 else 0o444)
                file_count += 1
            except OSError as exc:
                raise WorkspaceBoundaryError("workspace file could not be snapshotted") from exc
            finally:
                os.close(file_fd)

    try:
        copy_directory(source_fd, target, (), 0)
    except BaseException:
        shutil.rmtree(target, ignore_errors=True)
        raise
    finally:
        os.close(source_fd)
    for directory in sorted(directory_paths, key=lambda item: len(item.parts), reverse=True):
        directory.chmod(0o555)
    target.chmod(0o555)
    return WorkspaceInspection(
        regular_files=file_count,
        directories=directory_count,
        symlinks=symlink_count,
    )


def _descriptor_mount_id(descriptor: int) -> int:
    """Read Linux mount identity from an already-open descriptor."""

    try:
        information = Path(f"/proc/self/fdinfo/{descriptor}").read_text(encoding="ascii")
        value = next(line.split(":", 1)[1].strip() for line in information.splitlines() if line.startswith("mnt_id:"))
        return int(value)
    except (OSError, StopIteration, ValueError) as exc:
        raise WorkspaceBoundaryError("filesystem mount identity cannot be verified") from exc


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
