"""Lease-bound, journaled publication for validated OverlayFS candidates.

This is a host-side primitive, not a Worker capability. Each regular-file or
symlink replacement is atomic; a multi-entry publish is not atomically visible
to unrelated readers. A durable journal makes interrupted batches recoverable
by rolling them back while holding the same workspace lease.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
from typing import Any

from .workspace import (
    WorkspaceBoundaryError,
    WorkspaceDiff,
    _descriptor_mount_id,
    _is_within,
    _validate_relative_symlink,
    check_workspace_publish_conflicts,
    validate_overlay_candidate,
)
from .workspace_lease import WorkspaceWriteLease


_MAX_JOURNAL_BYTES = 32 * 1024 * 1024
_JOURNAL_SCHEMA = 1


class WorkspacePublishError(WorkspaceBoundaryError):
    """A workspace candidate could not be safely published."""


class WorkspacePublishConflict(WorkspacePublishError):
    """The live workspace no longer matches the candidate's lower snapshot."""


class WorkspacePublishRecoveryConflict(WorkspacePublishError):
    """Recovery found a target changed outside the recorded transaction."""


@dataclass(frozen=True)
class WorkspacePublishReceipt:
    """Non-sensitive summary of a completed journaled publication."""

    transaction_id: str
    manifest_hash: str
    entries_published: int
    lease_generation: int


def publish_workspace_diff(
    lower_root: str | Path,
    workspace_root: str | Path,
    diff: WorkspaceDiff,
    lease: WorkspaceWriteLease,
    journal_root: str | Path,
    *,
    max_entries: int = 100_000,
    max_bytes: int = 1024 * 1024 * 1024,
    max_backup_bytes: int = 1024 * 1024 * 1024,
) -> WorkspacePublishReceipt:
    """Publish additions/updates under an exclusive lease with crash rollback.

    The complete candidate and its lower snapshot are revalidated while the
    lease is held. Live-path conflicts are checked again before any mutation.
    Original file bytes are copied into a private transaction directory and a
    durable ``prepared`` journal is installed before the first workspace
    mutation. A crash before a durable ``committed`` marker is rolled back by
    :func:`recover_workspace_publications`.

    Deletions/renames are not represented by the current OverlayFS diff
    contract. The caller remains responsible for an audited approval decision;
    this low-level primitive is not exposed to Worker processes.
    """

    _verify_lease_for_workspace(lease, workspace_root)
    if isinstance(max_backup_bytes, bool) or not isinstance(max_backup_bytes, int) or max_backup_bytes < 0:
        raise WorkspacePublishError("max_backup_bytes must be a non-negative integer")
    validate_overlay_candidate(
        lower_root,
        diff,
        max_entries=max_entries,
        max_bytes=max_bytes,
    )
    conflicts = check_workspace_publish_conflicts(
        lower_root,
        workspace_root,
        diff,
        max_bytes=max_bytes,
    )
    if conflicts.is_conflicted:
        raise WorkspacePublishConflict("workspace candidate conflicts with live paths")

    roots = _open_roots(lower_root, workspace_root, diff.candidate_root, journal_root)
    transaction_id = secrets.token_hex(16)
    transaction_name = f"publish-{transaction_id}"
    transaction_fd: int | None = None
    try:
        _assert_roots_disjoint(roots)
        lease.assert_current()
        _verify_workspace_fd(roots.workspace_fd, lease)
        _assert_no_pending_transactions(roots.journal_fd)
        try:
            os.mkdir(transaction_name, 0o700, dir_fd=roots.journal_fd)
            os.fsync(roots.journal_fd)
            transaction_fd = os.open(
                transaction_name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=roots.journal_fd,
            )
        except OSError as exc:
            raise WorkspacePublishError("private publication journal cannot be created") from exc
        os.mkdir("backups", 0o700, dir_fd=transaction_fd)
        backups_fd = os.open(
            "backups",
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=transaction_fd,
        )
        try:
            records, backup_bytes = _capture_originals(
                roots,
                diff,
                backups_fd,
                transaction_id,
                max_backup_bytes=max_backup_bytes,
            )
            lease.assert_current()
            _verify_workspace_fd(roots.workspace_fd, lease)
            payload = _make_payload(
                status="prepared",
                transaction_id=transaction_id,
                workspace_device=lease.workspace_device,
                workspace_inode=lease.workspace_inode,
                lease_generation=lease.generation,
                manifest_hash=diff.manifest_hash,
                backup_bytes=backup_bytes,
                records=records,
            )
            _write_journal(transaction_fd, payload)
        finally:
            os.close(backups_fd)

        for record in records:
            lease.assert_current()
            _verify_workspace_fd(roots.workspace_fd, lease)
            _assert_live_state(roots.workspace_fd, roots.workspace_mount_id, record, "before")
            _apply_record(roots, record, transaction_id)
            _after_publish_entry(transaction_id, record["path"])

        lease.assert_current()
        _verify_workspace_fd(roots.workspace_fd, lease)
        os.fsync(roots.workspace_fd)
        _write_journal(transaction_fd, {**payload, "status": "committed"})
        receipt = WorkspacePublishReceipt(
            transaction_id=transaction_id,
            manifest_hash=diff.manifest_hash,
            entries_published=len(records),
            lease_generation=lease.generation,
        )
        os.close(transaction_fd)
        transaction_fd = None
        _remove_transaction(roots.journal_fd, transaction_name)
        return receipt
    except WorkspaceBoundaryError:
        raise
    except OSError as exc:
        raise WorkspacePublishError("workspace candidate publication failed") from exc
    finally:
        if transaction_fd is not None:
            os.close(transaction_fd)
        roots.close()


def recover_workspace_publications(
    workspace_root: str | Path,
    lease: WorkspaceWriteLease,
    journal_root: str | Path,
) -> tuple[str, ...]:
    """Recover or clean every interrupted publication for the leased workspace.

    Prepared transactions are rolled back idempotently; committed transactions
    are only cleaned up. An invalid journal or unexpected live target is a
    hard stop and remains on disk for operator investigation.
    """

    _verify_lease_for_workspace(lease, workspace_root)
    roots = _open_roots(None, workspace_root, None, journal_root)
    recovered: list[str] = []
    try:
        _assert_roots_disjoint(roots)
        lease.assert_current()
        _verify_workspace_fd(roots.workspace_fd, lease)
        try:
            names = sorted(os.listdir(roots.journal_fd))
        except OSError as exc:
            raise WorkspacePublishError("publication journal directory cannot be scanned") from exc
        for name in names:
            if not name.startswith("publish-"):
                continue
            if not re.fullmatch(r"publish-[0-9a-f]{32}", name):
                raise WorkspacePublishError("publication journal contains an invalid transaction name")
            info = os.stat(name, dir_fd=roots.journal_fd, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                raise WorkspacePublishError("publication transaction is not a real directory")
            transaction_fd = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=roots.journal_fd,
            )
            try:
                _verify_private_directory(transaction_fd, "publication transaction")
                try:
                    payload = _read_journal(transaction_fd)
                except FileNotFoundError:
                    # The publisher creates this directory before any mutation;
                    # a crash before the prepared record is therefore harmless.
                    os.close(transaction_fd)
                    transaction_fd = -1
                    _remove_transaction(roots.journal_fd, name)
                    recovered.append(name.removeprefix("publish-"))
                    continue
                _validate_payload(payload, name, lease)
                if payload["status"] == "prepared":
                    _rollback_records(roots, transaction_fd, payload["records"])
                    lease.assert_current()
                    _verify_workspace_fd(roots.workspace_fd, lease)
                    os.fsync(roots.workspace_fd)
                os.close(transaction_fd)
                transaction_fd = -1
                _remove_transaction(roots.journal_fd, name)
                recovered.append(payload["transaction_id"])
            finally:
                if transaction_fd >= 0:
                    os.close(transaction_fd)
        return tuple(recovered)
    finally:
        roots.close()


@dataclass
class _Roots:
    workspace_fd: int
    workspace_path: Path
    workspace_mount_id: int
    journal_fd: int
    journal_path: Path
    candidate_fd: int | None
    candidate_path: Path | None
    lower_path: Path | None

    def close(self) -> None:
        for descriptor in (self.candidate_fd, self.journal_fd, self.workspace_fd):
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def _open_roots(
    lower_root: str | Path | None,
    workspace_root: str | Path,
    candidate_root: str | Path | None,
    journal_root: str | Path,
) -> _Roots:
    workspace_path, workspace_fd, _ = _open_directory(workspace_root, private=False)
    try:
        opened_journal_path, journal_fd, _ = _open_directory(journal_root, private=True)
    except BaseException:
        os.close(workspace_fd)
        raise
    try:
        lower_path = Path(lower_root).resolve(strict=True) if lower_root is not None else None
    except (OSError, RuntimeError, ValueError) as exc:
        os.close(journal_fd)
        os.close(workspace_fd)
        raise WorkspacePublishError("lower snapshot cannot be resolved safely") from exc
    candidate_fd: int | None = None
    candidate_path: Path | None = None
    if candidate_root is not None:
        try:
            candidate_path, candidate_fd, _ = _open_directory(candidate_root, private=True)
        except BaseException:
            os.close(journal_fd)
            os.close(workspace_fd)
            raise
    try:
        mount_id = _descriptor_mount_id(workspace_fd)
    except BaseException:
        for descriptor in (candidate_fd, journal_fd, workspace_fd):
            if descriptor is not None:
                os.close(descriptor)
        raise
    return _Roots(
        workspace_fd=workspace_fd,
        workspace_path=workspace_path,
        workspace_mount_id=mount_id,
        journal_fd=journal_fd,
        journal_path=opened_journal_path,
        candidate_fd=candidate_fd,
        candidate_path=candidate_path,
        lower_path=lower_path,
    )


def _open_directory(root: str | Path, *, private: bool) -> tuple[Path, int, os.stat_result]:
    path = Path(root)
    try:
        before = path.lstat()
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
            raise WorkspacePublishError("publication roots must be real directories")
        resolved = path.resolve(strict=True)
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            os.close(descriptor)
            raise WorkspacePublishError("publication root changed while opening")
        if private:
            _verify_private_directory(descriptor, "private publication root")
        return resolved, descriptor, opened
    except WorkspacePublishError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspacePublishError("publication root cannot be opened safely") from exc


def _verify_private_directory(descriptor: int, label: str) -> None:
    info = os.fstat(descriptor)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise WorkspacePublishError(f"{label} must be current-user owned mode 0700")


def _assert_roots_disjoint(roots: _Roots) -> None:
    boundaries = [roots.workspace_path]
    boundaries.extend(path for path in (roots.lower_path, roots.candidate_path) if path is not None)
    for boundary in boundaries:
        if _is_within(roots.journal_path, boundary) or _is_within(boundary, roots.journal_path):
            raise WorkspacePublishError("publication journal and workspace inputs must be disjoint")


def _verify_lease_for_workspace(lease: WorkspaceWriteLease, workspace_root: str | Path) -> None:
    if not isinstance(lease, WorkspaceWriteLease):
        raise WorkspacePublishError("publication requires a live workspace write lease")
    lease.assert_current()
    try:
        info = os.stat(workspace_root, follow_symlinks=False)
        resolved = Path(workspace_root).resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise WorkspacePublishError("leased workspace cannot be inspected") from exc
    if (
        resolved != lease.workspace_root
        or (info.st_dev, info.st_ino) != (lease.workspace_device, lease.workspace_inode)
    ):
        raise WorkspacePublishError("write lease does not belong to the publication workspace")


def _verify_workspace_fd(descriptor: int, lease: WorkspaceWriteLease) -> None:
    info = os.fstat(descriptor)
    if (info.st_dev, info.st_ino) != (lease.workspace_device, lease.workspace_inode):
        raise WorkspacePublishError("workspace root changed during publication")
    lease.assert_current()


def _capture_originals(
    roots: _Roots,
    diff: WorkspaceDiff,
    backups_fd: int,
    transaction_id: str,
    *,
    max_backup_bytes: int,
) -> tuple[list[dict[str, Any]], int]:
    assert roots.candidate_fd is not None
    records: list[dict[str, Any]] = []
    backup_bytes = 0
    ordered = sorted(diff.entries, key=lambda item: (len(item.path.split("/")), item.path))
    for index, entry in enumerate(ordered):
        parts = tuple(entry.path.split("/"))
        live = _state_at(roots.workspace_fd, roots.workspace_mount_id, parts, max_bytes=max_backup_bytes)
        expected_kind = None if entry.operation == "add" else entry.kind
        if (live is None and expected_kind is not None) or (live is not None and live["kind"] != expected_kind):
            raise WorkspacePublishConflict("workspace candidate conflicts with live path types")
        before: dict[str, Any] | None
        backup_name: str | None = None
        if live is None:
            before = None
        else:
            before = live
            if live["kind"] == "file":
                if live["size"] > max_backup_bytes - backup_bytes:
                    raise WorkspacePublishError("workspace rollback backup exceeds its byte limit")
                backup_name = f"{index:08x}.backup"
                _copy_live_backup(roots.workspace_fd, roots.workspace_mount_id, parts, backups_fd, backup_name, live)
                backup_bytes += live["size"]
        if entry.kind == "directory" and entry.mode & 0o700 != 0o700:
            raise WorkspacePublishError("published directories must retain owner read/write/execute access")
        if entry.operation == "modify" and entry.kind in ("file", "symlink"):
            if live is None or live.get("digest") != entry.baseline_digest:
                raise WorkspacePublishConflict("workspace changed after the candidate conflict check")
        temp_name = f".mp{transaction_id}{index:x}"
        parent = _try_open_parent(roots.workspace_fd, roots.workspace_mount_id, parts[:-1])
        if parent is not None:
            try:
                try:
                    os.stat(temp_name, dir_fd=parent, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise WorkspacePublishError("publication temporary path already exists")
            finally:
                os.close(parent)
        after = {"kind": entry.kind, "mode": entry.mode}
        if entry.kind in ("file", "symlink"):
            after.update({"digest": entry.digest, "size": entry.size})
        if entry.kind == "file" and not entry.mode & 0o400:
            raise WorkspacePublishError("published files must retain owner read access for recovery verification")
        if entry.kind == "symlink":
            target = _candidate_symlink_target(roots.candidate_fd, parts)
            encoded_target = os.fsencode(target)
            if len(encoded_target) != entry.size or "sha256:" + hashlib.sha256(encoded_target).hexdigest() != entry.digest:
                raise WorkspacePublishError("candidate symlink changed after validation")
            after["target"] = target
        records.append(
            {
                "path": entry.path,
                "operation": entry.operation,
                "before": before,
                "after": after,
                "backup": backup_name,
                "temporary": temp_name,
            }
        )
    return records, backup_bytes


def _copy_live_backup(
    root_fd: int,
    mount_id: int,
    parts: tuple[str, ...],
    backup_dir_fd: int,
    backup_name: str,
    state: dict[str, Any],
) -> None:
    parent_fd = _open_parent(root_fd, mount_id, parts[:-1])
    source_fd = -1
    destination_fd = -1
    try:
        source_fd = os.open(
            parts[-1],
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            dir_fd=parent_fd,
        )
        opened = os.fstat(source_fd)
        if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
            raise WorkspacePublishError("workspace backup source is not a private regular file")
        destination_fd = os.open(
            backup_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=backup_dir_fd,
        )
        digest = hashlib.sha256()
        size = 0
        while True:
            chunk = os.read(source_fd, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
            _write_all(destination_fd, chunk)
        after = os.fstat(source_fd)
        if (
            size != state["size"]
            or "sha256:" + digest.hexdigest() != state["digest"]
            or (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        ):
            raise WorkspacePublishConflict("workspace changed while creating rollback backup")
        # Keep rollback copies readable even if the original was owner-write-only.
        os.fchmod(destination_fd, 0o600)
        os.fsync(destination_fd)
        os.fsync(backup_dir_fd)
    except OSError as exc:
        raise WorkspacePublishError("workspace rollback backup cannot be persisted") from exc
    finally:
        for descriptor in (destination_fd, source_fd, parent_fd):
            if descriptor >= 0:
                os.close(descriptor)


def _make_payload(
    *,
    status: str,
    transaction_id: str,
    workspace_device: int,
    workspace_inode: int,
    lease_generation: int,
    manifest_hash: str,
    backup_bytes: int,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema": _JOURNAL_SCHEMA,
        "status": status,
        "transaction_id": transaction_id,
        "workspace_device": workspace_device,
        "workspace_inode": workspace_inode,
        "lease_generation": lease_generation,
        "manifest_hash": manifest_hash,
        "backup_bytes": backup_bytes,
        "records": records,
    }


def _write_journal(transaction_fd: int, payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    if len(encoded) > _MAX_JOURNAL_BYTES:
        raise WorkspacePublishError("publication journal exceeds its size bound")
    temporary = ".state.tmp"
    try:
        try:
            os.unlink(temporary, dir_fd=transaction_fd)
        except FileNotFoundError:
            pass
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=transaction_fd,
        )
        try:
            _write_all(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, "state.json", src_dir_fd=transaction_fd, dst_dir_fd=transaction_fd)
        os.fsync(transaction_fd)
    except OSError as exc:
        raise WorkspacePublishError("publication journal cannot be persisted") from exc


def _read_journal(transaction_fd: int) -> dict[str, Any]:
    descriptor = os.open(
        "state.json",
        os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
        dir_fd=transaction_fd,
    )
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
            raise WorkspacePublishError("publication journal is not a private regular file")
        if info.st_size > _MAX_JOURNAL_BYTES:
            raise WorkspacePublishError("publication journal exceeds its size bound")
        chunks: list[bytes] = []
        remaining = _MAX_JOURNAL_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        encoded = b"".join(chunks)
        if len(encoded) > _MAX_JOURNAL_BYTES or not encoded.endswith(b"\n"):
            raise WorkspacePublishError("publication journal is incomplete")
        try:
            value = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WorkspacePublishError("publication journal is corrupt") from exc
        if not isinstance(value, dict):
            raise WorkspacePublishError("publication journal has an invalid schema")
        return value
    finally:
        os.close(descriptor)


def _validate_payload(payload: dict[str, Any], name: str, lease: WorkspaceWriteLease) -> None:
    required = {
        "schema", "status", "transaction_id", "workspace_device", "workspace_inode",
        "lease_generation", "manifest_hash", "backup_bytes", "records",
    }
    if set(payload) != required:
        raise WorkspacePublishError("publication journal has an invalid schema")
    transaction_id = name.removeprefix("publish-")
    if (
        isinstance(payload["schema"], bool)
        or payload["schema"] != _JOURNAL_SCHEMA
        or payload["status"] not in ("prepared", "committed")
        or payload["transaction_id"] != transaction_id
        or not isinstance(payload["workspace_device"], int)
        or isinstance(payload["workspace_device"], bool)
        or payload["workspace_device"] != lease.workspace_device
        or not isinstance(payload["workspace_inode"], int)
        or isinstance(payload["workspace_inode"], bool)
        or payload["workspace_inode"] != lease.workspace_inode
        or not isinstance(payload["lease_generation"], int)
        or isinstance(payload["lease_generation"], bool)
        or payload["lease_generation"] < 1
        or payload["lease_generation"] > lease.generation
        or not isinstance(payload["manifest_hash"], str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", payload["manifest_hash"])
        or isinstance(payload["backup_bytes"], bool)
        or not isinstance(payload["backup_bytes"], int)
        or payload["backup_bytes"] < 0
        or not isinstance(payload["records"], list)
    ):
        raise WorkspacePublishError("publication journal does not belong to the leased workspace")
    if len(payload["records"]) > 100_000:
        raise WorkspacePublishError("publication journal exceeds its entry limit")
    previous: tuple[int, str] | None = None
    seen: set[str] = set()
    for index, record in enumerate(payload["records"]):
        if not isinstance(record, dict) or set(record) != {
            "path", "operation", "before", "after", "backup", "temporary"
        }:
            raise WorkspacePublishError("publication journal contains a malformed record")
        path = record["path"]
        parts = path.split("/") if isinstance(path, str) else []
        if (
            not parts
            or any(part in ("", ".", "..") or "\\" in part or "\x00" in part for part in parts)
            or parts[0] in (".git", ".maestro")
            or path in seen
            or record["operation"] not in ("add", "modify")
            or not isinstance(record["temporary"], str)
            or record["temporary"] != f".mp{transaction_id}{index:x}"
        ):
            raise WorkspacePublishError("publication journal contains an invalid path")
        sort_key = (len(parts), path)
        if previous is not None and sort_key < previous:
            raise WorkspacePublishError("publication journal paths are not canonical")
        previous = sort_key
        seen.add(path)
        _validate_state(record["before"], allow_none=True)
        _validate_state(record["after"], allow_none=False)
        if (record["operation"] == "add") != (record["before"] is None):
            raise WorkspacePublishError("publication journal operation does not match its baseline")
        if record["before"] is not None and record["before"]["kind"] != record["after"]["kind"]:
            raise WorkspacePublishError("publication journal changes a path type")
        if record["backup"] is not None and (
            not isinstance(record["backup"], str) or record["backup"] != f"{index:08x}.backup"
        ):
            raise WorkspacePublishError("publication journal backup reference is malformed")
        if record["before"] is not None and record["before"]["kind"] == "file":
            if record["backup"] is None:
                raise WorkspacePublishError("publication journal is missing a file rollback backup")
        elif record["backup"] is not None:
            raise WorkspacePublishError("publication journal has an unnecessary rollback backup")
        if record["before"] is not None and record["before"]["kind"] == "symlink":
            try:
                _validate_relative_symlink(tuple(parts[:-1]), record["before"]["target"])
            except WorkspaceBoundaryError as exc:
                raise WorkspacePublishError("publication journal contains an unsafe baseline symlink") from exc
        if record["after"]["kind"] == "symlink":
            try:
                _validate_relative_symlink(tuple(parts[:-1]), record["after"]["target"])
            except WorkspaceBoundaryError as exc:
                raise WorkspacePublishError("publication journal contains an unsafe candidate symlink") from exc


def _validate_state(value: Any, *, allow_none: bool) -> None:
    if value is None:
        if allow_none:
            return
        raise WorkspacePublishError("publication journal omits its candidate state")
    if not isinstance(value, dict) or value.get("kind") not in ("file", "directory", "symlink"):
        raise WorkspacePublishError("publication journal contains an invalid path state")
    kind = value["kind"]
    fields = {"kind", "mode"}
    if kind == "file":
        fields |= {"digest", "size"}
    elif kind == "symlink":
        fields |= {"digest", "size", "target"}
    if set(value) != fields or isinstance(value["mode"], bool) or not isinstance(value["mode"], int) or not 0 <= value["mode"] <= 0o777:
        raise WorkspacePublishError("publication journal contains malformed path metadata")
    if kind in ("file", "symlink"):
        if (
            not isinstance(value["digest"], str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", value["digest"])
            or isinstance(value["size"], bool)
            or not isinstance(value["size"], int)
            or value["size"] < 0
        ):
            raise WorkspacePublishError("publication journal contains invalid content metadata")
    if kind == "symlink" and not isinstance(value["target"], str):
        raise WorkspacePublishError("publication journal contains an invalid symlink target")


def _rollback_records(
    roots: _Roots,
    transaction_fd: int,
    records: list[dict[str, Any]],
) -> None:
    for record in reversed(records):
        parts = tuple(record["path"].split("/"))
        _remove_temporary_if_safe(roots, parts, record["temporary"], record["after"])
        current = _state_at(roots.workspace_fd, roots.workspace_mount_id, parts, max_bytes=1024 * 1024 * 1024)
        before = record["before"]
        after = record["after"]
        if _states_equal(current, before):
            continue
        if (
            before is None
            and after["kind"] == "directory"
            and current == {"kind": "directory", "mode": 0o700}
        ):
            _restore_before(roots, transaction_fd, record)
            continue
        if not _states_equal(current, after):
            raise WorkspacePublishRecoveryConflict("workspace target changed after interrupted publication")
        _restore_before(roots, transaction_fd, record)


def _apply_record(roots: _Roots, record: dict[str, Any], transaction_id: str) -> None:
    parts = tuple(record["path"].split("/"))
    parent_fd = _open_parent(roots.workspace_fd, roots.workspace_mount_id, parts[:-1])
    leaf = parts[-1]
    entry = record["after"]
    temporary = record["temporary"]
    try:
        if entry["kind"] == "directory":
            if record["before"] is None:
                os.mkdir(leaf, 0o700, dir_fd=parent_fd)
            directory_fd = os.open(
                leaf,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            try:
                if _descriptor_mount_id(directory_fd) != roots.workspace_mount_id:
                    raise WorkspacePublishError("workspace target is on a nested mount")
                os.fchmod(directory_fd, entry["mode"])
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        elif entry["kind"] == "file":
            assert roots.candidate_fd is not None
            source_parent = _open_parent(roots.candidate_fd, _descriptor_mount_id(roots.candidate_fd), parts[:-1])
            source_fd = -1
            destination_fd = -1
            try:
                source_fd = os.open(
                    leaf,
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                    dir_fd=source_parent,
                )
                opened = os.fstat(source_fd)
                if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                    raise WorkspacePublishError("candidate file changed after validation")
                destination_fd = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600,
                    dir_fd=parent_fd,
                )
                digest = hashlib.sha256()
                size = 0
                while True:
                    chunk = os.read(source_fd, 1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    digest.update(chunk)
                    _write_all(destination_fd, chunk)
                if size != entry["size"] or "sha256:" + digest.hexdigest() != entry["digest"]:
                    raise WorkspacePublishError("candidate file changed after validation")
                os.fchmod(destination_fd, entry["mode"])
                os.fsync(destination_fd)
            finally:
                for descriptor in (destination_fd, source_fd, source_parent):
                    if descriptor >= 0:
                        os.close(descriptor)
            _install_temporary(parent_fd, temporary, leaf, record["before"] is None)
        else:
            assert roots.candidate_fd is not None
            target = _candidate_symlink_target(roots.candidate_fd, parts)
            if target != entry["target"]:
                raise WorkspacePublishError("candidate symlink changed after validation")
            os.symlink(target, temporary, dir_fd=parent_fd)
            os.fsync(parent_fd)
            _install_temporary(parent_fd, temporary, leaf, record["before"] is None)
        os.fsync(parent_fd)
    except OSError as exc:
        raise WorkspacePublishError("workspace entry could not be installed safely") from exc
    finally:
        os.close(parent_fd)


def _install_temporary(parent_fd: int, temporary: str, leaf: str, no_replace: bool) -> None:
    if no_replace:
        try:
            os.link(
                temporary,
                leaf,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except (OSError, NotImplementedError) as exc:
            raise WorkspacePublishError("filesystem cannot install a new entry without replacement") from exc
        os.unlink(temporary, dir_fd=parent_fd)
    else:
        os.replace(temporary, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)


def _restore_before(roots: _Roots, transaction_fd: int, record: dict[str, Any]) -> None:
    parts = tuple(record["path"].split("/"))
    parent_fd = _open_parent(roots.workspace_fd, roots.workspace_mount_id, parts[:-1])
    leaf = parts[-1]
    before = record["before"]
    try:
        if before is None:
            info = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                directory_fd = os.open(
                    leaf,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=parent_fd,
                )
                try:
                    if os.listdir(directory_fd):
                        raise WorkspacePublishRecoveryConflict("new workspace directory is not empty during rollback")
                finally:
                    os.close(directory_fd)
                os.rmdir(leaf, dir_fd=parent_fd)
            else:
                os.unlink(leaf, dir_fd=parent_fd)
        elif before["kind"] == "directory":
            directory_fd = os.open(
                leaf,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            try:
                os.fchmod(directory_fd, before["mode"])
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        elif before["kind"] == "file":
            backup_fd = os.open(
                "backups",
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=transaction_fd,
            )
            try:
                _restore_backup_file(backup_fd, record["backup"], parent_fd, record["temporary"], leaf, before)
            finally:
                os.close(backup_fd)
        else:
            os.symlink(before["target"], record["temporary"], dir_fd=parent_fd)
            os.replace(record["temporary"], leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        os.fsync(parent_fd)
    except WorkspacePublishRecoveryConflict:
        raise
    except OSError as exc:
        raise WorkspacePublishRecoveryConflict("workspace target could not be restored safely") from exc
    finally:
        os.close(parent_fd)


def _restore_backup_file(
    backup_dir_fd: int,
    backup_name: str | None,
    parent_fd: int,
    temporary: str,
    leaf: str,
    expected: dict[str, Any],
) -> None:
    if backup_name is None:
        raise WorkspacePublishError("publication journal is missing a rollback backup")
    source_fd = os.open(
        backup_name,
        os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
        dir_fd=backup_dir_fd,
    )
    destination_fd = -1
    try:
        info = os.fstat(source_fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != expected["size"]:
            raise WorkspacePublishError("rollback backup is not a valid regular file")
        destination_fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=parent_fd,
        )
        digest = hashlib.sha256()
        size = 0
        while True:
            chunk = os.read(source_fd, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
            _write_all(destination_fd, chunk)
        if size != expected["size"] or "sha256:" + digest.hexdigest() != expected["digest"]:
            raise WorkspacePublishError("rollback backup integrity check failed")
        os.fchmod(destination_fd, expected["mode"])
        os.fsync(destination_fd)
    finally:
        for descriptor in (destination_fd, source_fd):
            if descriptor >= 0:
                os.close(descriptor)
    os.replace(temporary, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)


def _remove_temporary_if_safe(
    roots: _Roots,
    parts: tuple[str, ...],
    temporary: str,
    expected: dict[str, Any],
) -> None:
    parent_fd = _try_open_parent(roots.workspace_fd, roots.workspace_mount_id, parts[:-1])
    if parent_fd is None:
        return
    try:
        try:
            info = os.stat(temporary, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if expected["kind"] == "file":
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink not in (1, 2)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) not in (0o600, expected["mode"])
            ):
                raise WorkspacePublishRecoveryConflict("unexpected publication temporary entry")
        elif expected["kind"] == "symlink":
            if not stat.S_ISLNK(info.st_mode) or os.readlink(temporary, dir_fd=parent_fd) != expected["target"]:
                raise WorkspacePublishRecoveryConflict("publication temporary symlink was changed")
        else:
            raise WorkspacePublishRecoveryConflict("unexpected directory publication temporary")
        os.unlink(temporary, dir_fd=parent_fd)
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def _state_at(
    root_fd: int,
    mount_id: int,
    parts: tuple[str, ...],
    *,
    max_bytes: int,
) -> dict[str, Any] | None:
    parent_fd = _try_open_parent(root_fd, mount_id, parts[:-1])
    if parent_fd is None:
        return None
    try:
        try:
            info = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if stat.S_ISDIR(info.st_mode):
            descriptor = os.open(
                parts[-1],
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            try:
                if _descriptor_mount_id(descriptor) != mount_id:
                    raise WorkspacePublishError("workspace target is on a nested mount")
                opened = os.fstat(descriptor)
                if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                    raise WorkspacePublishError("workspace directory changed during inspection")
            finally:
                os.close(descriptor)
            return {"kind": "directory", "mode": stat.S_IMODE(info.st_mode) & 0o777}
        if stat.S_ISREG(info.st_mode):
            descriptor = os.open(
                parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                dir_fd=parent_fd,
            )
            try:
                opened = os.fstat(descriptor)
                if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1 or _descriptor_mount_id(descriptor) != mount_id:
                    raise WorkspacePublishError("workspace file is not a safe regular file")
                if opened.st_size > max_bytes:
                    raise WorkspacePublishError("workspace file exceeds the rollback inspection bound")
                digest = _digest_fd(descriptor, max_bytes)
                after = os.fstat(descriptor)
                if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns) != (
                    after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns
                ):
                    raise WorkspacePublishConflict("workspace file changed during inspection")
                return {
                    "kind": "file",
                    "mode": stat.S_IMODE(opened.st_mode) & 0o777,
                    "digest": digest,
                    "size": opened.st_size,
                }
            finally:
                os.close(descriptor)
        if stat.S_ISLNK(info.st_mode):
            target = os.readlink(parts[-1], dir_fd=parent_fd)
            encoded = os.fsencode(target)
            if len(encoded) > max_bytes:
                raise WorkspacePublishError("workspace symlink exceeds the rollback inspection bound")
            after = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
            if (info.st_dev, info.st_ino, info.st_mode) != (after.st_dev, after.st_ino, after.st_mode):
                raise WorkspacePublishConflict("workspace symlink changed during inspection")
            return {
                "kind": "symlink",
                "mode": 0o777,
                "digest": "sha256:" + hashlib.sha256(encoded).hexdigest(),
                "size": len(encoded),
                "target": target,
            }
        raise WorkspacePublishError("workspace target is a special file")
    finally:
        os.close(parent_fd)


def _assert_live_state(root_fd: int, mount_id: int, record: dict[str, Any], state_name: str) -> None:
    parts = tuple(record["path"].split("/"))
    current = _state_at(root_fd, mount_id, parts, max_bytes=1024 * 1024 * 1024)
    if not _states_equal(current, record[state_name]):
        if state_name == "before":
            raise WorkspacePublishConflict("workspace changed while publication was being prepared")
        raise WorkspacePublishRecoveryConflict("workspace target changed after publication")


def _states_equal(left: dict[str, Any] | None, right: dict[str, Any] | None) -> bool:
    if left is None or right is None:
        return left is right
    return left == right


def _open_parent(root_fd: int, mount_id: int, parts: tuple[str, ...]) -> int:
    descriptor = _try_open_parent(root_fd, mount_id, parts)
    if descriptor is None:
        raise WorkspacePublishError("workspace parent directory is missing")
    return descriptor


def _try_open_parent(root_fd: int, mount_id: int, parts: tuple[str, ...]) -> int | None:
    descriptor = os.dup(root_fd)
    try:
        for component in parts:
            try:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                os.close(descriptor)
                return None
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
                raise WorkspacePublishError("workspace path traverses a non-directory or symlink")
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptor,
            )
            opened = os.fstat(child)
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                os.close(child)
                raise WorkspacePublishError("workspace parent changed during path resolution")
            if _descriptor_mount_id(child) != mount_id:
                os.close(child)
                raise WorkspacePublishError("workspace path traverses a nested mount")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def _candidate_symlink_target(candidate_fd: int, parts: tuple[str, ...]) -> str:
    parent_fd = _open_parent(candidate_fd, _descriptor_mount_id(candidate_fd), parts[:-1])
    try:
        info = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISLNK(info.st_mode):
            raise WorkspacePublishError("candidate symlink changed after validation")
        target = os.readlink(parts[-1], dir_fd=parent_fd)
        _validate_relative_symlink(parts[:-1], target)
        return target
    finally:
        os.close(parent_fd)


def _digest_fd(descriptor: int, max_bytes: int) -> str:
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = os.read(descriptor, min(1024 * 1024, max_bytes - size + 1))
        if not chunk:
            break
        size += len(chunk)
        if size > max_bytes:
            raise WorkspacePublishError("workspace file exceeds the rollback inspection bound")
        digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def _assert_no_pending_transactions(journal_fd: int) -> None:
    try:
        pending = [name for name in os.listdir(journal_fd) if name.startswith("publish-")]
    except OSError as exc:
        raise WorkspacePublishError("publication journal directory cannot be scanned") from exc
    if pending:
        raise WorkspacePublishError("pending publication recovery must run before another publish")


def _remove_transaction(journal_fd: int, transaction_name: str) -> None:
    path = f"/proc/self/fd/{journal_fd}/{transaction_name}"
    try:
        info = os.stat(transaction_name, dir_fd=journal_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(info.st_mode):
        raise WorkspacePublishError("publication transaction path changed before cleanup")
    # The transaction directory is private and was created by this module;
    # shutil.rmtree does not follow a symlink supplied as its root.
    shutil.rmtree(path)
    os.fsync(journal_fd)


def _after_publish_entry(_transaction_id: str, _path: str) -> None:
    """Fault-injection seam used by subprocess crash-recovery tests."""
