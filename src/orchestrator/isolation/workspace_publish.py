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
from typing import Any, Callable, Literal

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


class WorkspacePublishError(WorkspaceBoundaryError):
    """A workspace candidate could not be safely published."""


class WorkspacePublishConflict(WorkspacePublishError):
    """The live workspace no longer matches the candidate's lower snapshot."""


class WorkspacePublishRecoveryConflict(WorkspacePublishError):
    """Recovery found a target changed outside the recorded transaction."""


class WorkspacePublishAuthorizationLost(WorkspacePublishError):
    """Publication fencing or policy authorization was revoked mid-transaction."""


@dataclass(frozen=True)
class WorkspacePublishReceipt:
    """Non-sensitive summary of a completed journaled publication."""

    transaction_id: str
    manifest_hash: str
    entries_published: int
    lease_generation: int


@dataclass(frozen=True)
class WorkspacePublicationRecovery:
    """A journal transaction whose final effect was proven during recovery."""

    transaction_id: str
    outcome: Literal["not_started", "rolled_back", "committed"]
    manifest_hash: str | None = None
    entries_published: int = 0
    lease_generation: int | None = None


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
    transaction_id: str | None = None,
    authorize: Callable[[], bool] | None = None,
    retain_committed: bool = False,
) -> WorkspacePublishReceipt:
    """Publish a bounded change set under an exclusive lease with crash rollback.

    The complete candidate and its lower snapshot are revalidated while the
    lease is held. Live-path conflicts are checked again before any mutation.
    Original file bytes are copied into a private transaction directory and a
    durable ``prepared`` journal is installed before the first workspace
    mutation. A crash before a durable ``committed`` marker is rolled back by
    :func:`recover_workspace_publications`.

    Schema-2 deletions and renames retain complete preimages. Retained journals
    require explicit acknowledgement after the caller's durable receipts.
    The caller remains responsible for an audited approval decision;
    this low-level primitive is not exposed to Worker processes.
    """

    _verify_lease_for_workspace(lease, workspace_root)
    if type(retain_committed) is not bool:
        raise WorkspacePublishError("retain_committed must be a boolean")
    schema = 2 if retain_committed or diff.schema_version == 2 else 1
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
    roots.lease = lease
    roots.authorize = authorize
    if transaction_id is None:
        transaction_id = secrets.token_hex(16)
    elif not isinstance(transaction_id, str) or re.fullmatch(r"[0-9a-f]{32}", transaction_id) is None:
        roots.close()
        raise WorkspacePublishError("transaction_id must be a 128-bit lowercase hexadecimal value")
    transaction_name = f"publish-{transaction_id}"
    creation_name = f"staging-{transaction_id}" if schema == 2 else transaction_name
    transaction_fd: int | None = None
    committed_receipt: WorkspacePublishReceipt | None = None
    try:
        _assert_roots_disjoint(roots)
        lease.assert_current()
        _verify_workspace_fd(roots.workspace_fd, lease)
        _require_publish_authority(authorize)
        _assert_no_pending_transactions(roots.journal_fd)
        try:
            os.mkdir(creation_name, 0o700, dir_fd=roots.journal_fd)
            os.fsync(roots.journal_fd)
            transaction_fd = os.open(
                creation_name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=roots.journal_fd,
            )
        except OSError as exc:
            raise WorkspacePublishError("private publication journal cannot be created") from exc
        if schema == 2:
            _publication_boundary("creation", transaction_id)
            # A missing state file in an established schema-2 transaction is
            # corruption, not the legacy pre-preparation cleanup case. This
            # durable marker never authorizes an outcome or mutation itself.
            marker = os.open("schema-2", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=transaction_fd)
            try:
                os.fsync(marker)
            finally:
                os.close(marker)
            os.fsync(transaction_fd)
            preparing = _make_payload(
                status="preparing", transaction_id=transaction_id,
                workspace_device=lease.workspace_device, workspace_inode=lease.workspace_inode,
                lease_generation=lease.generation, manifest_hash=diff.manifest_hash,
                backup_bytes=0, records=[], schema=2, retain_committed=retain_committed,
            )
            _write_journal(transaction_fd, preparing)
            _publication_boundary("preparing", transaction_id)
            os.rename(creation_name, transaction_name, src_dir_fd=roots.journal_fd, dst_dir_fd=roots.journal_fd)
            _publication_boundary("staging_rename", transaction_id)
            os.fsync(roots.journal_fd)
            _publication_boundary("staging_fsync", transaction_id)
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
                schema=schema,
            )
            lease.assert_current()
            _verify_workspace_fd(roots.workspace_fd, lease)
            _require_publish_authority(authorize)
            payload = _make_payload(
                status="prepared",
                transaction_id=transaction_id,
                workspace_device=lease.workspace_device,
                workspace_inode=lease.workspace_inode,
                lease_generation=lease.generation,
                manifest_hash=diff.manifest_hash,
                backup_bytes=backup_bytes,
                records=records,
                schema=schema,
                retain_committed=retain_committed,
            )
            if schema == 2:
                # Verify the complete backup set before allowing even the first
                # live mutation; normalized snapshot modes are not preimages.
                _validate_payload(payload, transaction_name, lease)
                _verify_backups(transaction_fd, records)
                for record in records:
                    _assert_live_state(roots.workspace_fd, roots.workspace_mount_id, record, "before")
                if check_workspace_publish_conflicts(lower_root, workspace_root, diff, max_bytes=max_bytes).is_conflicted:
                    raise WorkspacePublishConflict("workspace changed while capturing preimages")
            _write_journal(transaction_fd, payload)
            _publication_boundary("prepared", transaction_id)
        finally:
            os.close(backups_fd)

        for record in records:
            lease.assert_current()
            _verify_workspace_fd(roots.workspace_fd, lease)
            _require_publish_authority(authorize)
            _assert_live_state(roots.workspace_fd, roots.workspace_mount_id, record, "before")
            _apply_record(roots, record, transaction_id)
            _after_publish_entry(transaction_id, record["path"])

        lease.assert_current()
        _verify_workspace_fd(roots.workspace_fd, lease)
        _require_publish_authority(authorize)
        os.fsync(roots.workspace_fd)
        _publication_boundary("parent_fsync", transaction_id)
        _write_journal(transaction_fd, {**payload, "status": "committed"})
        _publication_boundary("committed", transaction_id)
        committed_receipt = WorkspacePublishReceipt(
            transaction_id=transaction_id,
            manifest_hash=diff.manifest_hash,
            entries_published=len(records),
            lease_generation=lease.generation,
        )
        os.close(transaction_fd)
        transaction_fd = None
        try:
            if not retain_committed:
                _remove_transaction(roots.journal_fd, transaction_name)
        except Exception:
            # The durable committed marker proves the workspace effect. Cleanup
            # is recoverable and must not turn an applied write into an unknown
            # result merely because removing its journal failed.
            pass
        return committed_receipt
    except WorkspaceBoundaryError:
        if committed_receipt is not None:
            return committed_receipt
        raise
    except OSError as exc:
        if committed_receipt is not None:
            return committed_receipt
        raise WorkspacePublishError("workspace candidate publication failed") from exc
    finally:
        if transaction_fd is not None:
            try:
                os.close(transaction_fd)
            except OSError:
                if committed_receipt is None:
                    raise
        roots.close()


def _require_publish_authority(authorize: Callable[[], bool] | None) -> None:
    if authorize is None:
        return
    try:
        current = authorize()
    except Exception as exc:
        raise WorkspacePublishAuthorizationLost("workspace publication authority is unavailable") from exc
    if current is not True:
        raise WorkspacePublishAuthorizationLost("workspace publication authority was revoked")


def recover_workspace_publications(
    workspace_root: str | Path,
    lease: WorkspaceWriteLease,
    journal_root: str | Path,
) -> tuple[str, ...]:
    """Recover or clean every interrupted publication for the leased workspace.

    Prepared transactions are rolled back idempotently. Retained schema-2
    outcomes require acknowledgement; legacy committed transactions are cleaned
    up. Invalid journals or unexpected targets remain for investigation.
    """

    return tuple(
        recovery.transaction_id
        for recovery in recover_workspace_publications_with_outcomes(
            workspace_root, lease, journal_root
        )
    )


def recover_workspace_publications_with_outcomes(
    workspace_root: str | Path,
    lease: WorkspaceWriteLease,
    journal_root: str | Path,
) -> tuple[WorkspacePublicationRecovery, ...]:
    """Recover pending journals and report only outcomes proven by the journal.

    An incomplete ``prepared`` transaction is rolled back; a durable
    ``committed`` transaction is never rolled back. Schema-2 ``preparing``
    evidence proves no mutation started. Unbound staging is blocked; only
    legacy publish directories retain their no-journal cleanup semantics.
    """

    _verify_lease_for_workspace(lease, workspace_root)
    roots = _open_roots(None, workspace_root, None, journal_root)
    roots.lease = lease
    recovered: list[WorkspacePublicationRecovery] = []
    try:
        _assert_roots_disjoint(roots)
        lease.assert_current()
        _verify_workspace_fd(roots.workspace_fd, lease)
        try:
            names = sorted(os.listdir(roots.journal_fd))
        except OSError as exc:
            raise WorkspacePublishError("publication journal directory cannot be scanned") from exc
        transaction_names = [name for name in names if name.startswith(("publish-", "staging-"))]
        if len({name.split("-", 1)[-1] for name in transaction_names}) != len(transaction_names):
            raise WorkspacePublishError("publication journal has duplicate transaction identities")
        for name in names:
            if not name.startswith(("publish-", "staging-")):
                continue
            if not re.fullmatch(r"(?:publish|staging)-[0-9a-f]{32}", name):
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
                    try:
                        os.stat("schema-2", dir_fd=transaction_fd, follow_symlinks=False)
                        schema2 = True
                    except FileNotFoundError:
                        schema2 = False
                    if name.startswith("staging-") or schema2:
                        raise WorkspacePublishError("unbound publication staging requires investigation")
                    # The publisher creates this directory before any mutation;
                    # a crash before the prepared record is therefore harmless.
                    os.close(transaction_fd)
                    transaction_fd = -1
                    _remove_transaction(roots.journal_fd, name)
                    recovered.append(WorkspacePublicationRecovery(
                        transaction_id=name.removeprefix("publish-"),
                        outcome="not_started",
                    ))
                    continue
                _validate_payload(payload, name, lease)
                retained = payload.get("retain_committed", False)
                if payload["status"] == "prepared":
                    if payload["schema"] == 2:
                        _verify_backups(transaction_fd, payload["records"])
                    _rollback_records(roots, transaction_fd, payload["records"], lease=lease)
                    lease.assert_current()
                    _verify_workspace_fd(roots.workspace_fd, lease)
                    os.fsync(roots.workspace_fd)
                    outcome = "rolled_back"
                elif payload["status"] in ("preparing", "not_started"):
                    outcome = "not_started"
                elif payload["status"] == "rolled_back":
                    outcome = "rolled_back"
                else:
                    outcome = "committed"
                if payload["schema"] == 2 and outcome != "committed":
                    _write_journal(transaction_fd, {**payload, "status": outcome})
                recovery = WorkspacePublicationRecovery(
                    transaction_id=payload["transaction_id"],
                    outcome=outcome,
                    manifest_hash=payload["manifest_hash"],
                    entries_published=len(payload["records"]),
                    lease_generation=payload["lease_generation"],
                )
                os.close(transaction_fd)
                transaction_fd = -1
                if not retained and not (payload["schema"] == 2 and outcome != "committed"):
                    _remove_transaction(roots.journal_fd, name)
                recovered.append(recovery)
            finally:
                if transaction_fd >= 0:
                    os.close(transaction_fd)
        return tuple(recovered)
    finally:
        roots.close()


def acknowledge_workspace_publication(
    workspace_root: str | Path, lease: WorkspaceWriteLease, journal_root: str | Path,
    receipt: WorkspacePublishReceipt,
) -> None:
    """Release committed evidence after trusted durable receipt reconciliation.

    This conveys no approval authority. A committed journal is never rolled back.
    Repeating an acknowledgement after cleanup is harmless.
    """
    if not isinstance(receipt, WorkspacePublishReceipt):
        raise WorkspacePublishError("a publication receipt is required")
    _acknowledge(workspace_root, lease, journal_root, receipt, "committed")


def acknowledge_workspace_recovery(
    workspace_root: str | Path, lease: WorkspaceWriteLease, journal_root: str | Path,
    recovery: WorkspacePublicationRecovery,
) -> None:
    """Release proven abort evidence after its trusted durable abort receipt."""
    if not isinstance(recovery, WorkspacePublicationRecovery) or recovery.outcome not in ("not_started", "rolled_back"):
        raise WorkspacePublishError("a proven abort recovery is required")
    _acknowledge(workspace_root, lease, journal_root, recovery, recovery.outcome)


def _acknowledge(workspace_root, lease, journal_root, receipt, outcome):
    _verify_lease_for_workspace(lease, workspace_root)
    if (not isinstance(receipt.transaction_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", receipt.transaction_id) is None
            or not isinstance(receipt.manifest_hash, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", receipt.manifest_hash) is None
            or type(receipt.lease_generation) is not int or receipt.lease_generation < 1
            or receipt.lease_generation > lease.generation
            or type(receipt.entries_published) is not int or receipt.entries_published < 0):
        raise WorkspacePublishError("invalid publication acknowledgement binding")
    roots = _open_roots(None, workspace_root, None, journal_root)
    try:
        _assert_roots_disjoint(roots)
        _verify_workspace_fd(roots.workspace_fd, lease)
        names = [prefix + receipt.transaction_id for prefix in ("publish-", "staging-", "acknowledged-")]
        found = [name for name in names if name in os.listdir(roots.journal_fd)]
        if len(found) > 1:
            raise WorkspacePublishError("ambiguous publication acknowledgement")
        if not found:
            return
        name = found[0]
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=roots.journal_fd)
        try:
            _verify_private_directory(fd, "publication transaction")
            try:
                payload = _read_journal(fd)
            except FileNotFoundError:
                if name.startswith("acknowledged-"):
                    # A crash in cleanup may have removed the binding already.
                    # Leave the residue for inspection; never invent a no-effect
                    # outcome or use the name as authority to delete more data.
                    return
                raise
            _validate_payload(payload, name, lease)
            if (payload["status"] != outcome
                    or payload["manifest_hash"] != receipt.manifest_hash
                    or payload["lease_generation"] != receipt.lease_generation
                    or len(payload["records"]) != receipt.entries_published):
                raise WorkspacePublishError("publication acknowledgement does not match durable outcome")
            lease.assert_current()
            _publication_boundary("acknowledgement", receipt.transaction_id)
            acknowledged = "acknowledged-" + receipt.transaction_id
            if name != acknowledged:
                os.rename(name, acknowledged, src_dir_fd=roots.journal_fd, dst_dir_fd=roots.journal_fd)
                os.fsync(roots.journal_fd)
            _publication_boundary("acknowledged", receipt.transaction_id)
            _remove_transaction(roots.journal_fd, acknowledged)
        finally:
            os.close(fd)
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
    lease: WorkspaceWriteLease | None = None
    authorize: Callable[[], bool] | None = None

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
    schema: int = 1,
) -> tuple[list[dict[str, Any]], int]:
    assert roots.candidate_fd is not None
    records: list[dict[str, Any]] = []
    backup_bytes = 0
    ordered = sorted(diff.entries, key=lambda item: _record_order(item.path, item.operation, item.kind)
                     if schema == 2 else (len(item.path.split("/")), item.path))
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
            if entry.operation == "delete" and live["kind"] == "directory":
                directory_fd = _open_parent(roots.workspace_fd, roots.workspace_mount_id, parts)
                try:
                    before = {**live, "children": sorted(os.listdir(directory_fd))}
                finally:
                    os.close(directory_fd)
            if live["kind"] == "file":
                if live["size"] > max_backup_bytes - backup_bytes:
                    raise WorkspacePublishError("workspace rollback backup exceeds its byte limit")
                backup_name = f"{index:08x}.backup"
                _copy_live_backup(roots.workspace_fd, roots.workspace_mount_id, parts, backups_fd, backup_name, live)
                backup_bytes += live["size"]
                _publication_boundary("backup", transaction_id)
        if entry.operation != "delete" and entry.kind == "directory" and entry.mode & 0o700 != 0o700:
            raise WorkspacePublishError("published directories must retain owner read/write/execute access")
        if entry.operation in ("modify", "delete") and entry.kind in ("file", "symlink"):
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
        after = None if entry.operation == "delete" else {"kind": entry.kind, "mode": entry.mode}
        if after is not None and entry.kind in ("file", "symlink"):
            after.update({"digest": entry.digest, "size": entry.size})
        if after is not None and entry.kind == "file" and not entry.mode & 0o400:
            raise WorkspacePublishError("published files must retain owner read access for recovery verification")
        if after is not None and entry.kind == "symlink":
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


def _record_order(path: str, operation: str, kind: str) -> tuple[int, int, str]:
    depth = len(path.split("/"))
    if operation == "delete":
        return (3 if kind == "directory" else 2, -depth, path)
    return (0 if kind == "directory" else 1, depth, path)


def _verify_backups(transaction_fd: int, records: list[dict[str, Any]]) -> None:
    """Read and validate every rollback file before changing any live path."""
    files = [r for r in records if r["backup"] is not None]
    if not files:
        return
    directory = -1
    try:
        directory = os.open("backups", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=transaction_fd)
        _verify_private_directory(directory, "rollback backups")
        for record in files:
            state = _state_at(directory, _descriptor_mount_id(directory), (record["backup"],), max_bytes=record["before"]["size"])
            if (state is None or state["kind"] != "file" or state["mode"] != 0o600
                    or state["digest"] != record["before"]["digest"] or state["size"] != record["before"]["size"]):
                raise WorkspacePublishError("rollback backup integrity check failed")
    except OSError as exc:
        raise WorkspacePublishError("rollback backup is missing or unsafe") from exc
    finally:
        if directory >= 0:
            os.close(directory)


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
            chunk = os.read(source_fd, min(1024 * 1024, state["size"] - size + 1))
            if not chunk:
                break
            size += len(chunk)
            if size > state["size"]:
                raise WorkspacePublishConflict("workspace grew while creating rollback backup")
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
    schema: int = 1,
    retain_committed: bool = False,
) -> dict[str, Any]:
    return {
        "schema": schema,
        **({"retain_committed": retain_committed} if schema == 2 else {}),
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
            value = json.loads(encoded, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        except (UnicodeDecodeError, ValueError) as exc:
            raise WorkspacePublishError("publication journal is corrupt") from exc
        if not isinstance(value, dict):
            raise WorkspacePublishError("publication journal has an invalid schema")
        return value
    finally:
        os.close(descriptor)


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate journal key")
        value[key] = item
    return value


def _invalid_constant(value):
    raise ValueError("nonfinite journal value")


def _validate_payload(payload: dict[str, Any], name: str, lease: WorkspaceWriteLease) -> None:
    required = {
        "schema", "status", "transaction_id", "workspace_device", "workspace_inode",
        "lease_generation", "manifest_hash", "backup_bytes", "records",
    }
    schema = payload.get("schema")
    if schema == 2:
        required.add("retain_committed")
    if set(payload) != required:
        raise WorkspacePublishError("publication journal has an invalid schema")
    transaction_id = name.split("-", 1)[-1]
    if (
        type(schema) is not int or schema not in (1, 2)
        or (schema == 2 and type(payload["retain_committed"]) is not bool)
        or payload["status"] not in (("prepared", "committed") if schema == 1 else ("preparing", "prepared", "committed", "rolled_back", "not_started"))
        or (name.startswith("staging-") and (schema != 2 or payload["status"] not in ("preparing", "not_started")))
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
    if payload["status"] in ("preparing", "not_started") and (payload["records"] or payload["backup_bytes"]):
        raise WorkspacePublishError("unstarted publication contains mutation records")
    previous = None
    seen: set[str] = set()
    folded: set[str] = set()
    for index, record in enumerate(payload["records"]):
        if not isinstance(record, dict) or set(record) != {
            "path", "operation", "before", "after", "backup", "temporary"
        }:
            raise WorkspacePublishError("publication journal contains a malformed record")
        path = record["path"]
        parts = path.split("/") if isinstance(path, str) else []
        if (
            not parts
            or any(part in ("", ".", "..") or "\\" in part
                   or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in part) for part in parts)
            or parts[0].casefold() in (".git", ".maestro", ".env")
            or path in seen
            or path.casefold() in folded
            or record["operation"] not in (("add", "modify") if schema == 1 else ("add", "modify", "delete"))
            or not isinstance(record["temporary"], str)
            or record["temporary"] != f".mp{transaction_id}{index:x}"
        ):
            raise WorkspacePublishError("publication journal contains an invalid path")
        seen.add(path)
        folded.add(path.casefold())
        _validate_state(record["before"], allow_none=True)
        _validate_state(record["after"], allow_none=record["operation"] == "delete")
        if ((record["after"] is not None and "children" in record["after"])
                or (record["before"] is not None and "children" in record["before"] and record["operation"] != "delete")):
            raise WorkspacePublishError("publication topology is only valid for a removed directory")
        if record["operation"] == "delete" and (record["before"] is None or record["after"] is not None):
            raise WorkspacePublishError("publication deletion has invalid preimage")
        kind = (record["before"] if record["operation"] == "delete" else record["after"])["kind"]
        sort_key = _record_order(path, record["operation"], kind) if schema == 2 else (len(parts), path)
        if previous is not None and sort_key < previous:
            raise WorkspacePublishError("publication journal paths are not canonical")
        previous = sort_key
        if (record["operation"] == "add") != (record["before"] is None):
            raise WorkspacePublishError("publication journal operation does not match its baseline")
        if record["before"] is not None and record["after"] is not None and record["before"]["kind"] != record["after"]["kind"]:
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
        if record["after"] is not None and record["after"]["kind"] == "symlink":
            try:
                _validate_relative_symlink(tuple(parts[:-1]), record["after"]["target"])
            except WorkspaceBoundaryError as exc:
                raise WorkspacePublishError("publication journal contains an unsafe candidate symlink") from exc
    by_path = {record["path"]: record for record in payload["records"]}
    children_by_parent: dict[str, list[str]] = {}
    for path in by_path:
        parent, _, child = path.rpartition("/")
        children_by_parent.setdefault(parent, []).append(child)
    for record in payload["records"]:
        parts = record["path"].split("/")
        for depth in range(1, len(parts)):
            ancestor = by_path.get("/".join(parts[:depth]))
            if ancestor is not None:
                ancestor_state = ancestor["before"] if ancestor["operation"] == "delete" else ancestor["after"]
                if (ancestor_state["kind"] != "directory"
                        or (ancestor["operation"] == "delete" and record["operation"] != "delete")
                        or (ancestor["operation"] == "add" and record["operation"] == "delete")):
                    raise WorkspacePublishError("publication journal contains overlapping records")
        before = record["before"]
        if record["operation"] == "delete" and before["kind"] == "directory":
            children = sorted(children_by_parent.get(record["path"], []))
            if before.get("children") != children:
                raise WorkspacePublishError("publication directory preimage is incomplete")
    if sum(r["before"]["size"] for r in payload["records"] if r["backup"] is not None) != payload["backup_bytes"]:
        raise WorkspacePublishError("publication backup size does not match records")


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
    elif "children" in value:
        fields.add("children")
        children = value["children"]
        if (not isinstance(children, list) or any(not isinstance(c, str) or not c or c in (".", "..") or "/" in c or "\\" in c or "\x00" in c for c in children)
                or children != sorted(set(children))):
            raise WorkspacePublishError("publication directory topology is malformed")
    if set(value) != fields or isinstance(value["mode"], bool) or not isinstance(value["mode"], int) or not 0 <= value["mode"] <= 0o7777:
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
    *,
    lease: WorkspaceWriteLease | None = None,
) -> None:
    # A previous recovery can itself die while restoring a child. Remove only
    # verified transaction temporaries before checking parent topology.
    for record in records:
        if lease is not None:
            _verify_workspace_fd(roots.workspace_fd, lease)
        _remove_temporary_if_safe(
            roots, tuple(record["path"].split("/")), record["temporary"],
            record["after"] or record["before"], rollback=record["before"],
        )
    for record in reversed(records):
        if lease is not None:
            _verify_workspace_fd(roots.workspace_fd, lease)
        parts = tuple(record["path"].split("/"))
        current = _state_at(roots.workspace_fd, roots.workspace_mount_id, parts, max_bytes=1024 * 1024 * 1024)
        before = record["before"]
        after = record["after"]
        if before is not None and "children" in before and current is not None:
            directory = _open_parent(roots.workspace_fd, roots.workspace_mount_id, parts)
            try:
                if not set(os.listdir(directory)).issubset(before["children"]):
                    raise WorkspacePublishRecoveryConflict("removed directory contains unrecognized children")
            finally:
                os.close(directory)
        if _states_equal(current, before):
            continue
        if (after is None and before is not None and before["kind"] == "directory"
                and current == {"kind": "directory", "mode": 0o700}):
            _restore_before(roots, transaction_fd, record)
            continue
        if (
            before is None
            and after is not None and after["kind"] == "directory"
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
        _check_mutation_authority(roots)
        if entry is None:
            # Only enumerated tombstones can remove a path. rmdir refuses any
            # late child; no recursive deletion is authorized by a directory.
            _assert_live_state(roots.workspace_fd, roots.workspace_mount_id, record, "before")
            if record["before"]["kind"] == "directory":
                os.rmdir(leaf, dir_fd=parent_fd)
            else:
                os.unlink(leaf, dir_fd=parent_fd)
        elif entry["kind"] == "directory":
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
                _check_mutation_authority(roots)
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
                    chunk = os.read(source_fd, min(1024 * 1024, entry["size"] - size + 1))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > entry["size"]:
                        raise WorkspacePublishError("candidate file changed after validation")
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
            _check_mutation_authority(roots)
            _assert_live_state(roots.workspace_fd, roots.workspace_mount_id, record, "before")
            _install_temporary(parent_fd, temporary, leaf, record["before"] is None)
        else:
            assert roots.candidate_fd is not None
            target = _candidate_symlink_target(roots.candidate_fd, parts)
            if target != entry["target"]:
                raise WorkspacePublishError("candidate symlink changed after validation")
            os.symlink(target, temporary, dir_fd=parent_fd)
            os.fsync(parent_fd)
            _check_mutation_authority(roots)
            _assert_live_state(roots.workspace_fd, roots.workspace_mount_id, record, "before")
            _install_temporary(parent_fd, temporary, leaf, record["before"] is None)
        os.fsync(parent_fd)
    except OSError as exc:
        raise WorkspacePublishError("workspace entry could not be installed safely") from exc
    finally:
        os.close(parent_fd)


def _check_mutation_authority(roots: _Roots) -> None:
    if roots.lease is not None:
        _verify_workspace_fd(roots.workspace_fd, roots.lease)
    _require_publish_authority(roots.authorize)


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
            if record["after"] is None:
                try:
                    os.mkdir(leaf, 0o700, dir_fd=parent_fd)
                except FileExistsError:
                    if _state_at(roots.workspace_fd, roots.workspace_mount_id, parts, max_bytes=0) != {"kind": "directory", "mode": 0o700}:
                        raise WorkspacePublishRecoveryConflict("rollback directory changed before restoration")
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
                _restore_backup_file(
                    backup_fd, record["backup"], parent_fd, record["temporary"], leaf, before,
                    roots=roots, record=record,
                )
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
    *,
    roots: _Roots,
    record: dict[str, Any],
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
    # Copying and syncing may outlive the lease or the observed target state.
    # Keep the journal and temporary intact if either has changed.
    _check_mutation_authority(roots)
    _assert_live_state(roots.workspace_fd, roots.workspace_mount_id, record, "after")
    os.replace(temporary, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)


def _remove_temporary_if_safe(
    roots: _Roots,
    parts: tuple[str, ...],
    temporary: str,
    expected: dict[str, Any],
    *,
    rollback: dict[str, Any] | None = None,
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
                or stat.S_IMODE(info.st_mode) not in (0o600, expected["mode"], rollback["mode"] if rollback else 0o600)
            ):
                raise WorkspacePublishRecoveryConflict("unexpected publication temporary entry")
        elif expected["kind"] == "symlink":
            if not stat.S_ISLNK(info.st_mode) or os.readlink(temporary, dir_fd=parent_fd) not in (expected["target"], rollback["target"] if rollback else expected["target"]):
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
            return {"kind": "directory", "mode": stat.S_IMODE(info.st_mode)}
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
                    "mode": stat.S_IMODE(opened.st_mode),
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
    return {k: v for k, v in left.items() if k != "children"} == {k: v for k, v in right.items() if k != "children"}


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
        pending = [name for name in os.listdir(journal_fd) if name.startswith(("publish-", "staging-"))]
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


def _publication_boundary(_stage: str, _transaction_id: str) -> None:
    """Observable durable boundary for private-fixture process-death tests."""
