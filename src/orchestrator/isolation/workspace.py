"""Preflight checks and bounded snapshots for OS-isolated workspaces."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
from typing import Literal


class WorkspaceBoundaryError(RuntimeError):
    """A workspace tree cannot be proven to stay inside its declared root."""


@dataclass(frozen=True)
class WorkspaceInspection:
    """Non-sensitive summary produced by a complete, no-follow tree scan."""

    regular_files: int
    directories: int
    symlinks: int


@dataclass(frozen=True)
class WorkspaceDiffEntry:
    """One validated staged change exported from an OverlayFS upper layer."""

    path: str
    operation: Literal["add", "modify"]
    kind: Literal["file", "directory", "symlink"]
    mode: int
    size: int
    digest: str
    baseline_digest: str | None = None


@dataclass(frozen=True)
class WorkspaceDiff:
    """Bounded candidate tree and deterministic manifest; never applied implicitly."""

    candidate_root: Path
    entries: tuple[WorkspaceDiffEntry, ...]
    total_bytes: int
    manifest_hash: str


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


def export_overlay_diff(
    lower_root: str | Path,
    upper_root: str | Path,
    candidate_root: str | Path,
    *,
    max_entries: int = 100_000,
    max_bytes: int = 1024 * 1024 * 1024,
    max_depth: int = 256,
) -> WorkspaceDiff:
    """Safely export OverlayFS additions/updates without touching the workspace.

    The exporter fails closed on whiteouts/deletions, opaque or xattr-bearing
    upper entries, special files, nested mounts, hard links, control-directory
    paths, symlink traversal through the lower tree, and limit violations.
    Only a private candidate tree is produced; the caller must separately
    approve, audit, serialize, and apply it.
    """

    for name, value in (
        ("max_entries", max_entries),
        ("max_bytes", max_bytes),
        ("max_depth", max_depth),
    ):
        minimum = 0 if name == "max_bytes" else 1
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise WorkspaceBoundaryError(f"{name} must be a valid bound")

    lower = Path(lower_root)
    upper = Path(upper_root)
    target = Path(candidate_root)
    try:
        lower_stat = lower.lstat()
        upper_stat = upper.lstat()
        if not stat.S_ISDIR(lower_stat.st_mode) or stat.S_ISLNK(lower_stat.st_mode):
            raise WorkspaceBoundaryError("overlay lower root must be a real directory")
        if not stat.S_ISDIR(upper_stat.st_mode) or stat.S_ISLNK(upper_stat.st_mode):
            raise WorkspaceBoundaryError("overlay upper root must be a real directory")
        lower_path = lower.resolve(strict=True)
        upper_path = upper.resolve(strict=True)
        target_parent = target.parent.resolve(strict=True)
        target_path = target_parent / target.name
        if target.exists() or target.is_symlink():
            raise WorkspaceBoundaryError("candidate destination must not already exist")
        if any(
            _is_within(candidate, boundary)
            for candidate in (target_path,)
            for boundary in (lower_path, upper_path)
        ):
            raise WorkspaceBoundaryError("candidate destination overlaps an OverlayFS input")
        lower_fd = os.open(
            lower, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        try:
            upper_fd = os.open(
                upper, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
            )
        except BaseException:
            os.close(lower_fd)
            raise
    except WorkspaceBoundaryError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceBoundaryError("OverlayFS roots cannot be opened safely") from exc

    if (os.fstat(lower_fd).st_dev, os.fstat(lower_fd).st_ino) != (
        lower_stat.st_dev,
        lower_stat.st_ino,
    ) or (os.fstat(upper_fd).st_dev, os.fstat(upper_fd).st_ino) != (
        upper_stat.st_dev,
        upper_stat.st_ino,
    ):
        os.close(lower_fd)
        os.close(upper_fd)
        raise WorkspaceBoundaryError("OverlayFS roots changed while opening")

    try:
        target.mkdir(mode=0o700)
    except BaseException:
        os.close(lower_fd)
        os.close(upper_fd)
        raise
    entries: list[WorkspaceDiffEntry] = []
    total_bytes = 0
    try:
        source_mount_id = _descriptor_mount_id(upper_fd)
    except BaseException:
        shutil.rmtree(target, ignore_errors=True)
        os.close(lower_fd)
        os.close(upper_fd)
        raise
    entry_count = 0

    def add_entry() -> None:
        nonlocal entry_count
        entry_count += 1
        if entry_count > max_entries:
            raise WorkspaceBoundaryError("OverlayFS diff exceeds the entry limit")

    def copy_directory(
        source_fd: int,
        destination: Path,
        relative: tuple[str, ...],
        depth: int,
    ) -> None:
        nonlocal total_bytes
        if depth > max_depth:
            raise WorkspaceBoundaryError("OverlayFS diff exceeds the depth limit")
        _reject_xattrs(source_fd)
        try:
            names = sorted(os.listdir(source_fd))
        except OSError as exc:
            raise WorkspaceBoundaryError("OverlayFS upper directory cannot be read") from exc
        for name in names:
            if name in (".", "..") or "/" in name or "\x00" in name:
                raise WorkspaceBoundaryError("OverlayFS upper contains an invalid path component")
            if not relative and name in (".git", ".maestro"):
                raise WorkspaceBoundaryError("OverlayFS diff targets a protected control directory")
            try:
                before = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
            except OSError as exc:
                raise WorkspaceBoundaryError("OverlayFS upper changed during export") from exc
            add_entry()
            child = (*relative, name)
            relative_path = "/".join(child)
            lower_kind = _lower_entry_kind(lower_fd, child)
            mode = stat.S_IMODE(before.st_mode) & 0o777
            if stat.S_ISDIR(before.st_mode):
                if lower_kind not in (None, "directory"):
                    raise WorkspaceBoundaryError("OverlayFS upper changes a path's file type")
                try:
                    child_fd = os.open(
                        name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                        dir_fd=source_fd,
                    )
                except OSError as exc:
                    raise WorkspaceBoundaryError("OverlayFS upper directory changed during export") from exc
                try:
                    opened = os.fstat(child_fd)
                    if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                        raise WorkspaceBoundaryError("OverlayFS upper directory changed during export")
                    if _descriptor_mount_id(child_fd) != source_mount_id:
                        raise WorkspaceBoundaryError("OverlayFS upper contains a nested mount")
                    _reject_xattrs(child_fd)
                    output = destination / name
                    output.mkdir(mode=0o700)
                    copy_directory(child_fd, output, child, depth + 1)
                finally:
                    os.close(child_fd)
                entries.append(
                    WorkspaceDiffEntry(
                        relative_path,
                        "modify" if lower_kind == "directory" else "add",
                        "directory",
                        mode,
                        0,
                        _manifest_digest("directory", mode, 0),
                        None,
                    )
                )
                continue
            if stat.S_ISREG(before.st_mode):
                if before.st_nlink != 1:
                    raise WorkspaceBoundaryError("OverlayFS upper contains a hard-linked file")
                if lower_kind not in (None, "file"):
                    raise WorkspaceBoundaryError("OverlayFS upper changes a path's file type")
                if before.st_size > max_bytes - total_bytes:
                    raise WorkspaceBoundaryError("OverlayFS diff exceeds the byte limit")
                try:
                    file_fd = os.open(
                        name,
                        os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                        dir_fd=source_fd,
                    )
                except OSError as exc:
                    raise WorkspaceBoundaryError("OverlayFS upper file changed during export") from exc
                digest = hashlib.sha256()
                size = 0
                output = destination / name
                try:
                    opened = os.fstat(file_fd)
                    if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
                        before.st_dev,
                        before.st_ino,
                    ) or opened.st_nlink != 1 or opened.st_size != before.st_size or (
                        opened.st_mtime_ns,
                        opened.st_ctime_ns,
                    ) != (before.st_mtime_ns, before.st_ctime_ns):
                        raise WorkspaceBoundaryError("OverlayFS upper file changed during export")
                    _reject_xattrs(file_fd)
                    with output.open("xb") as output_file:
                        while True:
                            chunk = os.read(file_fd, 1024 * 1024)
                            if not chunk:
                                break
                            size += len(chunk)
                            total_bytes += len(chunk)
                            if total_bytes > max_bytes:
                                raise WorkspaceBoundaryError("OverlayFS diff exceeds the byte limit")
                            digest.update(chunk)
                            output_file.write(chunk)
                    after = os.fstat(file_fd)
                    if size != before.st_size or (
                        opened.st_dev,
                        opened.st_ino,
                        opened.st_size,
                        opened.st_mtime_ns,
                        opened.st_ctime_ns,
                    ) != (
                        after.st_dev,
                        after.st_ino,
                        after.st_size,
                        after.st_mtime_ns,
                        after.st_ctime_ns,
                    ):
                        raise WorkspaceBoundaryError("OverlayFS upper file changed while reading")
                    output.chmod(0o600)
                except OSError as exc:
                    raise WorkspaceBoundaryError("OverlayFS upper file could not be exported") from exc
                finally:
                    os.close(file_fd)
                entries.append(
                    WorkspaceDiffEntry(
                        relative_path,
                        "modify" if lower_kind == "file" else "add",
                        "file",
                        mode,
                        size,
                        "sha256:" + digest.hexdigest(),
                        (
                            _lower_content_digest(lower_fd, child, "file", max_bytes)
                            if lower_kind == "file"
                            else None
                        ),
                    )
                )
                continue
            if stat.S_ISLNK(before.st_mode):
                if lower_kind not in (None, "symlink"):
                    raise WorkspaceBoundaryError("OverlayFS upper changes a path's file type")
                try:
                    link = os.readlink(name, dir_fd=source_fd)
                    after = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
                except OSError as exc:
                    raise WorkspaceBoundaryError("OverlayFS upper symlink changed during export") from exc
                if (before.st_dev, before.st_ino, before.st_mode) != (
                    after.st_dev,
                    after.st_ino,
                    after.st_mode,
                ):
                    raise WorkspaceBoundaryError("OverlayFS upper symlink changed during export")
                _validate_relative_symlink(relative, link)
                encoded = os.fsencode(link)
                total_bytes += len(encoded)
                if total_bytes > max_bytes:
                    raise WorkspaceBoundaryError("OverlayFS diff exceeds the byte limit")
                os.symlink(link, destination / name)
                entries.append(
                    WorkspaceDiffEntry(
                        relative_path,
                        "modify" if lower_kind == "symlink" else "add",
                        "symlink",
                        0o777,
                        len(encoded),
                        "sha256:" + hashlib.sha256(encoded).hexdigest(),
                        (
                            _lower_content_digest(lower_fd, child, "symlink", max_bytes)
                            if lower_kind == "symlink"
                            else None
                        ),
                    )
                )
                continue
            raise WorkspaceBoundaryError("OverlayFS upper contains a whiteout or special file")

    try:
        copy_directory(upper_fd, target, (), 0)
        entries.sort(key=lambda entry: entry.path)
        canonical = json.dumps(
            [
                {
                    "path": entry.path,
                    "operation": entry.operation,
                    "kind": entry.kind,
                    "mode": entry.mode,
                    "size": entry.size,
                    "digest": entry.digest,
                    "baseline_digest": entry.baseline_digest,
                }
                for entry in entries
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return WorkspaceDiff(
            candidate_root=target,
            entries=tuple(entries),
            total_bytes=total_bytes,
            manifest_hash="sha256:" + hashlib.sha256(canonical).hexdigest(),
        )
    except BaseException:
        shutil.rmtree(target, ignore_errors=True)
        raise
    finally:
        os.close(lower_fd)
        os.close(upper_fd)


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _manifest_digest(kind: str, mode: int, size: int) -> str:
    value = f"{kind}:{mode:o}:{size}".encode("ascii")
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _reject_xattrs(descriptor: int) -> None:
    try:
        attributes = os.listxattr(descriptor)
    except OSError as exc:
        raise WorkspaceBoundaryError("OverlayFS metadata cannot be inspected") from exc
    if attributes:
        raise WorkspaceBoundaryError("OverlayFS upper contains unsupported extended attributes")


def _lower_entry_kind(lower_fd: int, path: tuple[str, ...]) -> str | None:
    current_fd = os.dup(lower_fd)
    try:
        for component in path[:-1]:
            try:
                info = os.stat(component, dir_fd=current_fd, follow_symlinks=False)
            except FileNotFoundError:
                return None
            if stat.S_ISLNK(info.st_mode):
                raise WorkspaceBoundaryError("OverlayFS path traverses a lower symlink")
            if not stat.S_ISDIR(info.st_mode):
                raise WorkspaceBoundaryError("OverlayFS path traverses a non-directory lower entry")
            next_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=current_fd,
            )
            os.close(current_fd)
            current_fd = next_fd
        try:
            info = os.stat(path[-1], dir_fd=current_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if stat.S_ISREG(info.st_mode):
            return "file"
        if stat.S_ISDIR(info.st_mode):
            return "directory"
        if stat.S_ISLNK(info.st_mode):
            return "symlink"
        return "special"
    finally:
        os.close(current_fd)


def _lower_content_digest(
    lower_fd: int,
    path: tuple[str, ...],
    kind: Literal["file", "symlink"],
    max_bytes: int,
) -> str:
    """Hash a modified lower entry through no-follow directory descriptors."""

    parent_fd = os.dup(lower_fd)
    try:
        for component in path[:-1]:
            try:
                before = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as exc:
                raise WorkspaceBoundaryError("Overlay baseline path changed during hashing") from exc
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
                raise WorkspaceBoundaryError("Overlay baseline path is not a safe directory")
            try:
                next_fd = os.open(
                    component,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=parent_fd,
                )
            except OSError as exc:
                raise WorkspaceBoundaryError("Overlay baseline path changed during hashing") from exc
            opened = os.fstat(next_fd)
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                os.close(next_fd)
                raise WorkspaceBoundaryError("Overlay baseline path changed during hashing")
            os.close(parent_fd)
            parent_fd = next_fd

        leaf = path[-1]
        try:
            before = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise WorkspaceBoundaryError("Overlay baseline entry disappeared during hashing") from exc
        if kind == "symlink":
            if not stat.S_ISLNK(before.st_mode):
                raise WorkspaceBoundaryError("Overlay baseline entry changed type during hashing")
            try:
                value = os.fsencode(os.readlink(leaf, dir_fd=parent_fd))
                after = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as exc:
                raise WorkspaceBoundaryError("Overlay baseline symlink changed during hashing") from exc
            if (before.st_dev, before.st_ino, before.st_mode) != (
                after.st_dev,
                after.st_ino,
                after.st_mode,
            ):
                raise WorkspaceBoundaryError("Overlay baseline symlink changed during hashing")
            if len(value) > max_bytes:
                raise WorkspaceBoundaryError("Overlay baseline symlink exceeds the byte limit")
            return "sha256:" + hashlib.sha256(value).hexdigest()

        if not stat.S_ISREG(before.st_mode) or before.st_size > max_bytes:
            raise WorkspaceBoundaryError("Overlay baseline file exceeds the byte limit")
        try:
            descriptor = os.open(
                leaf,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise WorkspaceBoundaryError("Overlay baseline file changed during hashing") from exc
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
                before.st_dev,
                before.st_ino,
            ) or opened.st_nlink != 1 or opened.st_size != before.st_size or (
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            ) != (before.st_mtime_ns, before.st_ctime_ns):
                raise WorkspaceBoundaryError("Overlay baseline file changed during hashing")
            digest = hashlib.sha256()
            size = 0
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    raise WorkspaceBoundaryError("Overlay baseline file exceeds the byte limit")
                digest.update(chunk)
            after = os.fstat(descriptor)
            if size != before.st_size or (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ):
                raise WorkspaceBoundaryError("Overlay baseline file changed during hashing")
            return "sha256:" + digest.hexdigest()
        except OSError as exc:
            raise WorkspaceBoundaryError("Overlay baseline file could not be hashed") from exc
        finally:
            os.close(descriptor)
    finally:
        os.close(parent_fd)


def _validate_relative_symlink(relative_parent: tuple[str, ...], link: str) -> None:
    if not link or os.path.isabs(link):
        raise WorkspaceBoundaryError("OverlayFS diff only allows relative symlinks")
    parts = list(relative_parent)
    for component in link.split("/"):
        if component in ("", "."):
            continue
        if component == "..":
            if not parts:
                raise WorkspaceBoundaryError("OverlayFS symlink escapes its declared root")
            parts.pop()
        else:
            parts.append(component)


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
