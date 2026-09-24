from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from orchestrator.isolation import (
    WorkspaceLeaseBusy,
    WorkspaceLeaseError,
    acquire_workspace_write_lease,
)
from orchestrator.isolation import workspace_lease as lease_module


def _roots(tmp_path: Path) -> tuple[Path, Path]:
    workspace = tmp_path / "workspace"
    lease_root = tmp_path / "private-leases"
    workspace.mkdir()
    lease_root.mkdir(mode=0o700)
    lease_root.chmod(0o700)
    return workspace, lease_root


def _lock_file(workspace: Path, lease_root: Path) -> Path:
    key = hashlib.sha256(os.fsencode(str(workspace.resolve()))).hexdigest()
    return lease_root / f"workspace-{key}.lease"


def test_workspace_lease_increments_fencing_generation_and_closes_idempotently(
    tmp_path: Path,
) -> None:
    workspace, lease_root = _roots(tmp_path)
    first = acquire_workspace_write_lease(workspace, lease_root)
    assert first.generation == 1
    assert len(first.lease_id) == 32
    first.assert_current()
    first.close()
    first.close()
    assert first.closed
    with pytest.raises(WorkspaceLeaseError, match="closed"):
        first.assert_current()

    with acquire_workspace_write_lease(workspace, lease_root) as second:
        assert second.generation == 2
        second.assert_current()
    assert second.closed
    records = [json.loads(line) for line in _lock_file(workspace, lease_root).read_text().splitlines()]
    assert [item["generation"] for item in records] == [1, 2]
    assert all(item["workspace_inode"] == workspace.stat().st_ino for item in records)


def test_workspace_lease_serializes_across_processes_and_releases_after_crash(
    tmp_path: Path,
) -> None:
    workspace, lease_root = _roots(tmp_path)
    lease = acquire_workspace_write_lease(workspace, lease_root)
    script = (
        "from orchestrator.isolation import WorkspaceLeaseBusy, acquire_workspace_write_lease; "
        "import sys; "
        "try_lease = None; "
        "\ntry:\n"
        " try_lease = acquire_workspace_write_lease(sys.argv[1], sys.argv[2])\n"
        "except WorkspaceLeaseBusy:\n"
        " print('busy')\n"
        "else:\n"
        " print('acquired:' + str(try_lease.generation))\n"
        " try_lease.close()\n"
    )
    repo_root = Path(__file__).resolve().parents[3]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "src")

    busy = subprocess.run(
        [sys.executable, "-c", script, str(workspace), str(lease_root)],
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert busy.returncode == 0, busy.stderr
    assert busy.stdout.strip() == "busy"
    lease.close()

    restarted = subprocess.run(
        [sys.executable, "-c", script, str(workspace), str(lease_root)],
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert restarted.returncode == 0, restarted.stderr
    assert restarted.stdout.strip() == "acquired:2"


def test_workspace_lease_repairs_only_an_incomplete_tail(tmp_path: Path) -> None:
    workspace, lease_root = _roots(tmp_path)
    with acquire_workspace_write_lease(workspace, lease_root) as first:
        assert first.generation == 1
    journal = _lock_file(workspace, lease_root)
    with journal.open("ab") as output:
        output.write(b'{"generation":')

    with acquire_workspace_write_lease(workspace, lease_root) as second:
        assert second.generation == 2
        second.assert_current()
    records = [json.loads(line) for line in journal.read_text().splitlines()]
    assert len(records) == 2


@pytest.mark.parametrize("invalid_record", (b"{}\n", b"not-json\n"))
def test_workspace_lease_rejects_corrupt_complete_journal_records(
    tmp_path: Path, invalid_record: bytes
) -> None:
    workspace, lease_root = _roots(tmp_path)
    with acquire_workspace_write_lease(workspace, lease_root):
        pass
    _lock_file(workspace, lease_root).write_bytes(invalid_record)

    with pytest.raises(WorkspaceLeaseError, match="journal"):
        acquire_workspace_write_lease(workspace, lease_root)


def test_workspace_lease_rejects_oversized_journals_and_permissive_lock_files(
    tmp_path: Path,
) -> None:
    workspace, lease_root = _roots(tmp_path)
    with acquire_workspace_write_lease(workspace, lease_root):
        pass
    journal = _lock_file(workspace, lease_root)
    journal.write_bytes(b"x" * (lease_module._MAX_JOURNAL_BYTES + 1))
    with pytest.raises(WorkspaceLeaseError, match="size bound"):
        acquire_workspace_write_lease(workspace, lease_root)

    journal.write_text("", encoding="ascii")
    journal.chmod(0o644)
    with pytest.raises(WorkspaceLeaseError, match="private regular file"):
        acquire_workspace_write_lease(workspace, lease_root)


@pytest.mark.parametrize("mode", (0o755, 0o750))
def test_workspace_lease_requires_private_owned_directory(
    tmp_path: Path, mode: int
) -> None:
    workspace, lease_root = _roots(tmp_path)
    lease_root.chmod(mode)
    with pytest.raises(WorkspaceLeaseError, match="owned mode 0700"):
        acquire_workspace_write_lease(workspace, lease_root)


def test_workspace_lease_rejects_symlink_roots_and_overlapping_lease_root(
    tmp_path: Path,
) -> None:
    workspace, lease_root = _roots(tmp_path)
    workspace_link = tmp_path / "workspace-link"
    workspace_link.symlink_to(workspace, target_is_directory=True)
    with pytest.raises(WorkspaceLeaseError, match="workspace root"):
        acquire_workspace_write_lease(workspace_link, lease_root)

    lease_link = tmp_path / "lease-link"
    lease_link.symlink_to(lease_root, target_is_directory=True)
    with pytest.raises(WorkspaceLeaseError, match="real directory"):
        acquire_workspace_write_lease(workspace, lease_link)

    in_workspace = workspace / ".lease-state"
    in_workspace.mkdir(mode=0o700)
    in_workspace.chmod(0o700)
    with pytest.raises(WorkspaceLeaseError, match="must be disjoint"):
        acquire_workspace_write_lease(workspace, in_workspace)


def test_workspace_lease_refuses_symlink_lock_file(tmp_path: Path) -> None:
    workspace, lease_root = _roots(tmp_path)
    outside = tmp_path / "outside"
    outside.write_text("secret", encoding="utf-8")
    _lock_file(workspace, lease_root).symlink_to(outside)

    with pytest.raises(WorkspaceLeaseError, match="cannot be opened safely"):
        acquire_workspace_write_lease(workspace, lease_root)
    assert outside.read_text(encoding="utf-8") == "secret"


def test_workspace_lease_reports_missing_roots_and_unsupported_platform(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, lease_root = _roots(tmp_path)
    with pytest.raises(WorkspaceLeaseError, match="workspace root"):
        acquire_workspace_write_lease(tmp_path / "missing", lease_root)

    monkeypatch.setattr(lease_module, "fcntl", None)
    with pytest.raises(lease_module.WorkspaceLeaseUnavailable, match="unsupported"):
        acquire_workspace_write_lease(workspace, lease_root)


@pytest.mark.parametrize("failure_at", ("open", "flock"))
def test_workspace_lease_normalizes_lock_open_and_lock_syscall_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_at: str
) -> None:
    workspace, lease_root = _roots(tmp_path)
    if failure_at == "open":
        real_open = os.open

        def fail_lock_open(path, flags, *args, **kwargs):
            if kwargs.get("dir_fd") is not None:
                raise PermissionError("simulated lock open failure")
            return real_open(path, flags, *args, **kwargs)

        monkeypatch.setattr(lease_module.os, "open", fail_lock_open)
        message = "cannot be opened safely"
    else:
        def fail_flock(_descriptor: int, _operation: int) -> None:
            raise OSError("simulated flock syscall failure")

        monkeypatch.setattr(lease_module.fcntl, "flock", fail_flock)
        message = "cannot be acquired"

    with pytest.raises(WorkspaceLeaseError, match=message):
        acquire_workspace_write_lease(workspace, lease_root)


def test_workspace_lease_revalidates_root_and_journal_while_held(tmp_path: Path) -> None:
    workspace, lease_root = _roots(tmp_path)
    lease = acquire_workspace_write_lease(workspace, lease_root)
    journal = _lock_file(workspace, lease_root)
    journal.chmod(0o644)
    with pytest.raises(WorkspaceLeaseError, match="journal changed"):
        lease.assert_current()
    journal.chmod(0o600)
    lease.assert_current()

    old_workspace = tmp_path / "workspace-old"
    os.rename(workspace, old_workspace)
    workspace.mkdir()
    with pytest.raises(WorkspaceLeaseError, match="root changed"):
        lease.assert_current()
    lease.close()
    replacement = acquire_workspace_write_lease(workspace, lease_root)
    replacement.close()


def test_workspace_lease_assert_current_detects_stale_partial_and_private_root_changes(
    tmp_path: Path,
) -> None:
    workspace, lease_root = _roots(tmp_path)
    lease = acquire_workspace_write_lease(workspace, lease_root)
    journal = _lock_file(workspace, lease_root)
    original = journal.read_bytes()
    record = json.loads(original.splitlines()[0])
    record["lease_id"] = "f" * 32
    journal.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(WorkspaceLeaseError, match="fencing generation is stale"):
        lease.assert_current()

    journal.write_bytes(original + b"partial")
    with pytest.raises(WorkspaceLeaseError, match="journal is incomplete"):
        lease.assert_current()
    journal.write_bytes(original)

    lease_root.chmod(0o755)
    with pytest.raises(WorkspaceLeaseError, match="directory changed"):
        lease.assert_current()
    lease_root.chmod(0o700)
    lease.assert_current()
    lease.close()


def test_workspace_lease_persist_failure_releases_the_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, lease_root = _roots(tmp_path)
    real_fsync = os.fsync

    def fail_lock_fsync(descriptor: int) -> None:
        if os.fstat(descriptor).st_ino == _lock_file(workspace, lease_root).stat().st_ino:
            raise OSError("simulated journal fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(lease_module.os, "fsync", fail_lock_fsync)
    with pytest.raises(WorkspaceLeaseError, match="persisted"):
        acquire_workspace_write_lease(workspace, lease_root)
    monkeypatch.setattr(lease_module.os, "fsync", real_fsync)
    with acquire_workspace_write_lease(workspace, lease_root) as lease:
        # The append completed before fsync reported an unknown durability
        # outcome, so the next owner consumes the following fencing number.
        assert lease.generation == 2


def test_workspace_lease_rejects_journal_read_and_tail_repair_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, lease_root = _roots(tmp_path)
    with acquire_workspace_write_lease(workspace, lease_root):
        pass
    journal = _lock_file(workspace, lease_root)
    real_pread = os.pread

    def fail_read(descriptor: int, size: int, offset: int) -> bytes:
        if os.fstat(descriptor).st_ino == journal.stat().st_ino:
            raise OSError("simulated journal read failure")
        return real_pread(descriptor, size, offset)

    monkeypatch.setattr(lease_module.os, "pread", fail_read)
    with pytest.raises(WorkspaceLeaseError, match="journal cannot be read"):
        acquire_workspace_write_lease(workspace, lease_root)
    monkeypatch.setattr(lease_module.os, "pread", real_pread)

    with journal.open("ab") as output:
        output.write(b"partial-tail")
    real_fsync = os.fsync

    def fail_repair_fsync(descriptor: int) -> None:
        if os.fstat(descriptor).st_ino == journal.stat().st_ino:
            raise OSError("simulated tail repair fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(lease_module.os, "fsync", fail_repair_fsync)
    with pytest.raises(WorkspaceLeaseError, match="tail cannot be repaired"):
        acquire_workspace_write_lease(workspace, lease_root)
    monkeypatch.setattr(lease_module.os, "fsync", real_fsync)
    with acquire_workspace_write_lease(workspace, lease_root) as lease:
        assert lease.generation == 2


def test_workspace_lease_closes_descriptors_even_when_unlock_or_close_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, lease_root = _roots(tmp_path)
    lease = acquire_workspace_write_lease(workspace, lease_root)
    lock_fd = lease._lock_fd
    root_fd = lease._lease_root_fd
    real_close = os.close
    real_flock = lease_module.fcntl.flock

    def fail_unlock(descriptor: int, operation: int) -> None:
        if descriptor == lock_fd and operation == lease_module.fcntl.LOCK_UN:
            raise OSError("simulated unlock failure")
        real_flock(descriptor, operation)

    close_failed = False

    def fail_lock_close_once(descriptor: int) -> None:
        nonlocal close_failed
        if descriptor == lock_fd and not close_failed:
            close_failed = True
            raise OSError("simulated close failure")
        real_close(descriptor)

    monkeypatch.setattr(lease_module.fcntl, "flock", fail_unlock)
    monkeypatch.setattr(lease_module.os, "close", fail_lock_close_once)
    lease.close()
    monkeypatch.setattr(lease_module.os, "close", real_close)
    monkeypatch.setattr(lease_module.fcntl, "flock", real_flock)
    assert lease.closed
    with pytest.raises(OSError):
        os.fstat(root_fd)
    real_close(lock_fd)
