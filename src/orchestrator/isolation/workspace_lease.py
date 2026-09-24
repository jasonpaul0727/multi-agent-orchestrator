"""Cross-process exclusive write leases for shared workspaces."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows has no flock
    fcntl = None  # type: ignore[assignment]

from .workspace import WorkspaceBoundaryError, _is_within


_MAX_JOURNAL_BYTES = 64 * 1024


class WorkspaceLeaseError(WorkspaceBoundaryError):
    """A workspace write lease cannot be proven safe or current."""


class WorkspaceLeaseBusy(WorkspaceLeaseError):
    """Another process currently owns the workspace write lease."""


class WorkspaceLeaseUnavailable(WorkspaceLeaseError):
    """The platform cannot provide the required advisory lock primitive."""


class WorkspaceWriteLease:
    """Held cross-process lock with a durable monotonic fencing generation.

    The caller must retain this object across every workspace mutation and
    re-run :meth:`assert_current` immediately before publishing. The lease
    does not itself apply changes or make a multi-file publication atomic.
    """

    def __init__(
        self,
        *,
        workspace_root: Path,
        workspace_device: int,
        workspace_inode: int,
        lease_root: Path,
        lease_root_device: int,
        lease_root_inode: int,
        lock_name: str,
        lock_fd: int,
        lease_root_fd: int,
        generation: int,
        lease_id: str,
    ) -> None:
        self.workspace_root = workspace_root
        self.workspace_device = workspace_device
        self.workspace_inode = workspace_inode
        self.generation = generation
        self.lease_id = lease_id
        self._lease_root = lease_root
        self._lease_root_device = lease_root_device
        self._lease_root_inode = lease_root_inode
        self._lock_name = lock_name
        self._lock_fd = lock_fd
        self._lease_root_fd = lease_root_fd
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def assert_current(self) -> None:
        """Fail closed if either root, journal, or held lock changed."""

        if self._closed:
            raise WorkspaceLeaseError("workspace write lease is closed")
        try:
            root_fd_info = os.fstat(self._lease_root_fd)
            root_path_info = os.stat(self._lease_root, follow_symlinks=False)
            if (
                not stat.S_ISDIR(root_fd_info.st_mode)
                or stat.S_ISLNK(root_path_info.st_mode)
                or root_path_info.st_uid != os.getuid()
                or stat.S_IMODE(root_path_info.st_mode) != 0o700
                or (root_fd_info.st_dev, root_fd_info.st_ino)
                != (self._lease_root_device, self._lease_root_inode)
                or (root_path_info.st_dev, root_path_info.st_ino)
                != (self._lease_root_device, self._lease_root_inode)
            ):
                raise WorkspaceLeaseError("workspace lease directory changed")

            lock_info = os.fstat(self._lock_fd)
            named_lock = os.stat(
                self._lock_name,
                dir_fd=self._lease_root_fd,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(lock_info.st_mode)
                or lock_info.st_nlink != 1
                or lock_info.st_uid != os.getuid()
                or stat.S_IMODE(lock_info.st_mode) != 0o600
                or (named_lock.st_dev, named_lock.st_ino) != (lock_info.st_dev, lock_info.st_ino)
            ):
                raise WorkspaceLeaseError("workspace lease journal changed")

            workspace_info = os.stat(self.workspace_root, follow_symlinks=False)
            if (
                not stat.S_ISDIR(workspace_info.st_mode)
                or stat.S_ISLNK(workspace_info.st_mode)
                or (workspace_info.st_dev, workspace_info.st_ino)
                != (self.workspace_device, self.workspace_inode)
            ):
                raise WorkspaceLeaseError("workspace root changed while leased")

            records, has_partial_tail = _read_journal(self._lock_fd, repair_tail=False)
        except WorkspaceLeaseError:
            raise
        except OSError as exc:
            raise WorkspaceLeaseError("workspace write lease cannot be verified") from exc
        if has_partial_tail or not records:
            raise WorkspaceLeaseError("workspace lease journal is incomplete")
        latest = records[-1]
        if (
            latest["generation"] != self.generation
            or latest["lease_id"] != self.lease_id
            or latest["workspace_device"] != self.workspace_device
            or latest["workspace_inode"] != self.workspace_inode
        ):
            raise WorkspaceLeaseError("workspace write lease fencing generation is stale")

    def close(self) -> None:
        """Release the advisory lock and descriptors; safe to call repeatedly."""

        if self._closed:
            return
        self._closed = True
        try:
            if fcntl is not None:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        for descriptor in (self._lock_fd, self._lease_root_fd):
            try:
                os.close(descriptor)
            except OSError:
                pass

    def __enter__(self) -> WorkspaceWriteLease:
        self.assert_current()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def acquire_workspace_write_lease(
    workspace_root: str | Path,
    lease_root: str | Path,
) -> WorkspaceWriteLease:
    """Acquire one exclusive lease and append a durable fencing generation.

    ``lease_root`` must already be a real, current-user-owned ``0700``
    directory outside the workspace. This API only serializes and fences host
    writers; callers still need an application-level approval and a
    crash-recoverable publication protocol.
    """

    if fcntl is None:
        raise WorkspaceLeaseUnavailable("exclusive workspace write leases are unsupported")

    lease_root_path, lease_root_info, lease_root_fd = _open_private_lease_root(lease_root)
    lock_fd: int | None = None
    locked = False
    transferred = False
    try:
        workspace_path, workspace_info = _inspect_workspace_root(workspace_root)
        if _is_within(lease_root_path, workspace_path) or _is_within(workspace_path, lease_root_path):
            raise WorkspaceLeaseError("workspace and lease roots must be disjoint")

        key = hashlib.sha256(os.fsencode(str(workspace_path))).hexdigest()
        lock_name = f"workspace-{key}.lease"
        try:
            os.stat(lock_name, dir_fd=lease_root_fd, follow_symlinks=False)
            existed = True
        except FileNotFoundError:
            existed = False
        flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        try:
            lock_fd = os.open(lock_name, flags, 0o600, dir_fd=lease_root_fd)
        except OSError as exc:
            raise WorkspaceLeaseError("workspace lease journal cannot be opened safely") from exc
        _verify_lock_file(lease_root_fd, lock_name, lock_fd)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise WorkspaceLeaseBusy("workspace already has an active write lease") from exc
        except OSError as exc:
            raise WorkspaceLeaseError("workspace write lease cannot be acquired") from exc
        locked = True
        if not existed:
            os.fsync(lease_root_fd)

        current_workspace_path, current_workspace_info = _inspect_workspace_root(workspace_path)
        if (current_workspace_info.st_dev, current_workspace_info.st_ino) != (
            workspace_info.st_dev,
            workspace_info.st_ino,
        ):
            raise WorkspaceLeaseError("workspace root changed while acquiring its lease")
        _verify_private_root_path(lease_root_path, lease_root_info, lease_root_fd)
        _verify_lock_file(lease_root_fd, lock_name, lock_fd)

        records, _ = _read_journal(lock_fd, repair_tail=True)
        generation = records[-1]["generation"] + 1 if records else 1
        lease_id = secrets.token_hex(16)
        record = {
            "generation": generation,
            "lease_id": lease_id,
            "workspace_device": workspace_info.st_dev,
            "workspace_inode": workspace_info.st_ino,
        }
        _append_journal_record(lock_fd, record)
        lease = WorkspaceWriteLease(
            workspace_root=current_workspace_path,
            workspace_device=workspace_info.st_dev,
            workspace_inode=workspace_info.st_ino,
            lease_root=lease_root_path,
            lease_root_device=lease_root_info.st_dev,
            lease_root_inode=lease_root_info.st_ino,
            lock_name=lock_name,
            lock_fd=lock_fd,
            lease_root_fd=lease_root_fd,
            generation=generation,
            lease_id=lease_id,
        )
        lock_fd = None
        lease_root_fd = -1
        locked = False
        transferred = True
        try:
            lease.assert_current()
        except BaseException:
            lease.close()
            raise
        return lease
    except WorkspaceLeaseError:
        raise
    except OSError as exc:
        raise WorkspaceLeaseError("workspace write lease could not be persisted") from exc
    finally:
        if not transferred:
            if locked and lock_fd is not None:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                except OSError:
                    pass
            if lock_fd is not None:
                try:
                    os.close(lock_fd)
                except OSError:
                    pass
            if lease_root_fd >= 0:
                try:
                    os.close(lease_root_fd)
                except OSError:
                    pass


def _open_private_lease_root(root: str | Path) -> tuple[Path, os.stat_result, int]:
    supplied = Path(root)
    try:
        supplied_info = supplied.lstat()
    except OSError as exc:
        raise WorkspaceLeaseError("private workspace lease directory is unavailable") from exc
    if stat.S_ISLNK(supplied_info.st_mode) or not stat.S_ISDIR(supplied_info.st_mode):
        raise WorkspaceLeaseError("private workspace lease root must be a real directory")
    try:
        resolved = supplied.resolve(strict=True)
        resolved_info = resolved.lstat()
        if (
            stat.S_ISLNK(resolved_info.st_mode)
            or not stat.S_ISDIR(resolved_info.st_mode)
            or resolved_info.st_uid != os.getuid()
            or stat.S_IMODE(resolved_info.st_mode) != 0o700
        ):
            raise WorkspaceLeaseError("private workspace lease root must be owned mode 0700")
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(resolved, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (resolved_info.st_dev, resolved_info.st_ino):
                raise WorkspaceLeaseError("private workspace lease root changed while opening")
        except BaseException:
            os.close(descriptor)
            raise
        return resolved, opened, descriptor
    except WorkspaceLeaseError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceLeaseError("private workspace lease root cannot be opened safely") from exc


def _inspect_workspace_root(root: str | Path) -> tuple[Path, os.stat_result]:
    supplied = Path(root)
    try:
        supplied_info = supplied.lstat()
        if stat.S_ISLNK(supplied_info.st_mode) or not stat.S_ISDIR(supplied_info.st_mode):
            raise WorkspaceLeaseError("workspace root must be a real directory")
        resolved = supplied.resolve(strict=True)
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(resolved, flags)
        try:
            opened = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if (opened.st_dev, opened.st_ino) != (supplied_info.st_dev, supplied_info.st_ino):
            raise WorkspaceLeaseError("workspace root changed while opening")
        return resolved, opened
    except WorkspaceLeaseError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspaceLeaseError("workspace root cannot be opened safely") from exc


def _verify_private_root_path(root: Path, expected: os.stat_result, root_fd: int) -> None:
    current = os.stat(root, follow_symlinks=False)
    opened = os.fstat(root_fd)
    if (
        stat.S_ISLNK(current.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or current.st_uid != os.getuid()
        or stat.S_IMODE(current.st_mode) != 0o700
        or (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino)
        or (opened.st_dev, opened.st_ino) != (expected.st_dev, expected.st_ino)
    ):
        raise WorkspaceLeaseError("private workspace lease root changed")


def _verify_lock_file(root_fd: int, name: str, descriptor: int) -> None:
    info = os.fstat(descriptor)
    named = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or (named.st_dev, named.st_ino) != (info.st_dev, info.st_ino)
    ):
        raise WorkspaceLeaseError("workspace lease journal is not a private regular file")


def _read_journal(
    descriptor: int,
    *,
    repair_tail: bool,
) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = os.pread(descriptor, _MAX_JOURNAL_BYTES + 1, 0)
    except OSError as exc:
        raise WorkspaceLeaseError("workspace lease journal cannot be read") from exc
    if len(payload) > _MAX_JOURNAL_BYTES:
        raise WorkspaceLeaseError("workspace lease journal exceeds its size bound")
    complete_size = payload.rfind(b"\n") + 1
    has_partial_tail = complete_size != len(payload)
    if has_partial_tail and repair_tail:
        try:
            os.ftruncate(descriptor, complete_size)
            os.fsync(descriptor)
        except OSError as exc:
            raise WorkspaceLeaseError("workspace lease journal tail cannot be repaired") from exc
        payload = payload[:complete_size]
        has_partial_tail = False
    records: list[dict[str, Any]] = []
    expected_generation = 1
    for line in payload[:complete_size].splitlines():
        try:
            value = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise WorkspaceLeaseError("workspace lease journal is corrupt") from exc
        if (
            not isinstance(value, dict)
            or set(value) != {"generation", "lease_id", "workspace_device", "workspace_inode"}
            or isinstance(value.get("generation"), bool)
            or not isinstance(value.get("generation"), int)
            or value["generation"] != expected_generation
            or not isinstance(value.get("lease_id"), str)
            or len(value["lease_id"]) != 32
            or any(character not in "0123456789abcdef" for character in value["lease_id"])
            or any(
                isinstance(value.get(field), bool)
                or not isinstance(value.get(field), int)
                or value[field] < 0
                for field in ("workspace_device", "workspace_inode")
            )
        ):
            raise WorkspaceLeaseError("workspace lease journal has an invalid record")
        records.append(value)
        expected_generation += 1
    return records, has_partial_tail


def _append_journal_record(descriptor: int, record: dict[str, Any]) -> None:
    line = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
    try:
        os.lseek(descriptor, 0, os.SEEK_END)
        offset = 0
        while offset < len(line):
            written = os.write(descriptor, line[offset:])
            if written <= 0:
                raise OSError("short journal write")
            offset += written
        os.fsync(descriptor)
    except OSError as exc:
        raise WorkspaceLeaseError("workspace lease generation could not be persisted") from exc
