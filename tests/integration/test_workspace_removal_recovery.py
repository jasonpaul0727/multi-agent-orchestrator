"""Real process-death tests; every mutation is inside a private pytest fixture."""
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys

import pytest

from orchestrator.isolation import (
    acquire_workspace_write_lease, export_workspace_view_diff,
    publish_workspace_diff, recover_workspace_publications_with_outcomes,
    snapshot_workspace, WorkspaceLeaseError, WorkspacePublishError, WorkspacePublishRecoveryConflict,
)
from orchestrator.isolation import workspace_publish as publisher


def removal_fixture(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "source").mkdir(mode=0o751)
    (workspace / "source/sub").mkdir(mode=0o711)
    (workspace / "source/sub/data").write_bytes(b"original\x00payload")
    (workspace / "source/sub/data").chmod(0o640)
    (workspace / "source/link").symlink_to("sub/data")
    (workspace / "destination").write_bytes(b"replaced destination")
    (workspace / "standalone").write_bytes(b"remove me")
    (workspace / "untracked").write_bytes(b"keep me")
    lower = tmp_path / "lower"
    snapshot_workspace(workspace, lower)
    final = tmp_path / "final"
    shutil.copytree(workspace, final, symlinks=True)
    shutil.rmtree(final / "source")
    (final / "standalone").unlink()
    (final / "destination").write_bytes(b"original\x00payload")
    (final / "untracked").chmod((lower / "untracked").stat().st_mode & 0o777)
    (final / "new").mkdir()
    (final / "new/data").write_bytes(b"new payload")
    diff = export_workspace_view_diff(lower, final, tmp_path / "candidate")
    journal = tmp_path / "journal"
    journal.mkdir(mode=0o700)
    return workspace, lower, journal, diff


def tree_state(root):
    result = {}
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        value = os.readlink(path) if path.is_symlink() else None if path.is_dir() else path.read_bytes()
        result[path.relative_to(root).as_posix()] = (stat.S_IFMT(info.st_mode), stat.S_IMODE(info.st_mode), value)
    return result


def test_removal_and_rename_overwrite_publish_then_acknowledge(tmp_path):
    workspace, lower, journal, diff = removal_fixture(tmp_path)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        receipt = publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True)
        assert not (workspace / "source").exists()
        assert not (workspace / "standalone").exists()
        assert (workspace / "destination").read_bytes() == b"original\x00payload"
        assert (workspace / "new/data").read_bytes() == b"new payload"
        assert (workspace / "untracked").read_bytes() == b"keep me"
        committed = tree_state(workspace)
        for _ in range(2):
            recovered, = recover_workspace_publications_with_outcomes(workspace, lease, journal)
            assert recovered.outcome == "committed"
            assert recovered.transaction_id == receipt.transaction_id
            assert tree_state(workspace) == committed
        with pytest.raises(WorkspacePublishError):
            publisher.acknowledge_workspace_publication(workspace, lease, journal, replace(receipt, manifest_hash="sha256:" + "0" * 64))
        assert list(journal.glob("publish-*"))
        publisher.acknowledge_workspace_publication(workspace, lease, journal, receipt)
        publisher.acknowledge_workspace_publication(workspace, lease, journal, receipt)
        assert not list(journal.glob("publish-*"))


@pytest.mark.parametrize("lose_lease", [False, True])
def test_rollback_copy_preserves_intervening_writer(tmp_path, monkeypatch, lose_lease):
    workspace, lower, journal, diff = removal_fixture(tmp_path)

    def stop_after_removal(transaction, path):
        if path == "source":
            raise WorkspacePublishError("interrupted after removal")

    monkeypatch.setattr(publisher, "_after_publish_entry", stop_after_removal)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="interrupted after removal"):
            publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True)
        transaction, = journal.glob("publish-*")
        state_path = transaction / "state.json"
        original_state = state_path.read_bytes()
        record, = (
            value for value in json.loads(original_state)["records"]
            if value["path"] == "standalone"
        )
        backup = transaction / "backups" / record["backup"]
        original_backup = backup.read_bytes()
        read = os.read
        intervened = False

        def write_during_backup_copy(descriptor, count):
            nonlocal intervened
            chunk = read(descriptor, count)
            # Backup verification happens earlier, without a restoration
            # temporary. Intervene only after the real copy has started.
            if (not intervened and chunk
                    and (workspace / record["temporary"]).exists()
                    and Path(f"/proc/self/fd/{descriptor}").resolve() == backup):
                intervened = True
                if lose_lease:
                    lease.close()
                (workspace / "standalone").write_bytes(b"external-new-writer")
            return chunk

        monkeypatch.setattr(os, "read", write_during_backup_copy)
        error = None
        try:
            recover_workspace_publications_with_outcomes(workspace, lease, journal)
        except (WorkspaceLeaseError, WorkspacePublishRecoveryConflict) as exc:
            error = exc
        assert intervened
        assert (workspace / "standalone").read_bytes() == b"external-new-writer"
        assert isinstance(error, WorkspaceLeaseError if lose_lease else WorkspacePublishRecoveryConflict)
        assert state_path.read_bytes() == original_state
        assert backup.read_bytes() == original_backup


@pytest.mark.parametrize("rollback", [False, True])
def test_directory_mode_change_can_contain_child_removal(tmp_path, monkeypatch, rollback):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "kept").mkdir(mode=0o751)
    (workspace / "kept/child").write_bytes(b"old child")
    lower = tmp_path / "lower"
    snapshot_workspace(workspace, lower)
    final = tmp_path / "final"
    shutil.copytree(lower, final)
    (final / "kept").chmod(0o700)
    (final / "kept/child").unlink()
    diff = export_workspace_view_diff(lower, final, tmp_path / "candidate")
    journal = tmp_path / "journal"
    journal.mkdir(mode=0o700)
    def stop_after_removal(transaction, path):
        if path == "kept/child":
            raise WorkspacePublishError("interrupted after removal")
    if rollback:
        monkeypatch.setattr(publisher, "_after_publish_entry", stop_after_removal)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        if rollback:
            with pytest.raises(WorkspacePublishError, match="interrupted after removal"):
                publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True)
            recovery, = recover_workspace_publications_with_outcomes(workspace, lease, journal)
            assert recovery.outcome == "rolled_back"
            assert (workspace / "kept/child").read_bytes() == b"old child"
            assert stat.S_IMODE((workspace / "kept").stat().st_mode) == 0o751
        else:
            receipt = publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True)
            assert receipt.entries_published == 2
            assert not (workspace / "kept/child").exists()
            assert stat.S_IMODE((workspace / "kept").stat().st_mode) == 0o700


@pytest.mark.parametrize("seam", ["creation", "preparing", "staging_rename", "staging_fsync", "backup", "prepared", *[f"apply:{i}" for i in range(1, 9)], "parent_fsync", "committed", "acknowledgement"])
def test_subprocess_death_preserves_old_or_committed_tree(tmp_path, seam):
    workspace, lower, journal, diff = removal_fixture(tmp_path)
    original = tree_state(workspace)
    script = f'''
import os
from pathlib import Path
from orchestrator.isolation import WorkspaceDiff, WorkspaceDiffEntry, acquire_workspace_write_lease
from orchestrator.isolation import workspace_publish as p
diff = WorkspaceDiff(Path({str(diff.candidate_root)!r}), tuple(WorkspaceDiffEntry(**v) for v in { [asdict(e) for e in diff.entries]!r}), {diff.total_bytes}, {diff.manifest_hash!r}, 2)
seam = {seam!r}
count = 0
def die(stage, transaction):
    if stage == seam:
        os._exit(79)
def applied(transaction, path):
    global count
    count += 1
    if seam == "apply:" + str(count):
        os._exit(79)
p._publication_boundary = die
p._after_publish_entry = applied
with acquire_workspace_write_lease({str(workspace)!r}, {str(journal)!r}) as lease:
    receipt = p.publish_workspace_diff({str(lower)!r}, {str(workspace)!r}, diff, lease, {str(journal)!r}, retain_committed=True)
    p.acknowledge_workspace_publication({str(workspace)!r}, lease, {str(journal)!r}, receipt)
'''
    process = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert process.returncode == 79, process.stderr
    after_commit = seam in ("committed", "acknowledgement")
    with acquire_workspace_write_lease(workspace, journal) as lease:
        if seam == "creation":
            for _ in range(2):
                with pytest.raises(WorkspacePublishError, match="unbound"):
                    recover_workspace_publications_with_outcomes(workspace, lease, journal)
                assert tree_state(workspace) == original
                assert list(journal.glob("staging-*"))
            return
        for _ in range(2):
            recovery, = recover_workspace_publications_with_outcomes(workspace, lease, journal)
            assert recovery.outcome == ("committed" if after_commit else "not_started" if seam in ("backup", "preparing", "staging_rename", "staging_fsync") else "rolled_back")
            if after_commit:
                assert tree_state(workspace) == {
                    "destination": (stat.S_IFREG, 0o644, b"original\x00payload"),
                    "new": (stat.S_IFDIR, 0o755, None),
                    "new/data": (stat.S_IFREG, 0o644, b"new payload"),
                    "untracked": original["untracked"],
                }
            else:
                assert tree_state(workspace) == original
        if after_commit:
            receipt = publisher.WorkspacePublishReceipt(recovery.transaction_id, recovery.manifest_hash, recovery.entries_published, recovery.lease_generation)
            publisher.acknowledge_workspace_publication(workspace, lease, journal, receipt)
        else:
            with pytest.raises(WorkspacePublishError):
                publisher.acknowledge_workspace_recovery(workspace, lease, journal, replace(recovery, manifest_hash="sha256:" + "0" * 64))
            publisher.acknowledge_workspace_recovery(workspace, lease, journal, recovery)
            publisher.acknowledge_workspace_recovery(workspace, lease, journal, recovery)
        assert recover_workspace_publications_with_outcomes(workspace, lease, journal) == ()


@pytest.mark.parametrize("damage", ["missing", "corrupt", "overflow"])
def test_complete_preimages_verified_before_first_mutation(tmp_path, monkeypatch, damage):
    workspace, lower, journal, diff = removal_fixture(tmp_path)
    original = tree_state(workspace)
    capture = publisher._capture_originals
    def damage_backup(*args, **kwargs):
        result = capture(*args, **kwargs)
        backup = next(journal.glob("publish-*/backups/*.backup"))
        if damage == "missing":
            backup.unlink()
        else:
            backup.write_bytes(b"corrupt")
        return result
    if damage != "overflow":
        monkeypatch.setattr(publisher, "_capture_originals", damage_backup)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError):
            publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True, max_backup_bytes=1 if damage == "overflow" else 1024)
        assert tree_state(workspace) == original
        assert list(journal.glob("publish-*"))


def test_new_live_child_prevents_directory_removal_and_retains_evidence(tmp_path, monkeypatch):
    workspace, lower, journal, diff = removal_fixture(tmp_path)
    apply = publisher._apply_record
    def interfere(roots, record, transaction):
        if record["path"] == "source/sub" and record["operation"] == "delete":
            (workspace / "source/sub/external").write_bytes(b"external")
        return apply(roots, record, transaction)
    monkeypatch.setattr(publisher, "_apply_record", interfere)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError):
            publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True)
        assert (workspace / "source/sub/external").read_bytes() == b"external"
        with pytest.raises(WorkspacePublishRecoveryConflict):
            recover_workspace_publications_with_outcomes(workspace, lease, journal)
        assert list(journal.glob("publish-*"))
        assert (workspace / "untracked").read_bytes() == b"keep me"


def test_recovery_does_not_partially_restore_when_any_backup_is_damaged(tmp_path, monkeypatch):
    workspace, lower, journal, diff = removal_fixture(tmp_path)
    def stop(transaction, path):
        if path == "source":
            raise WorkspacePublishError("interrupted after final removal")
    monkeypatch.setattr(publisher, "_after_publish_entry", stop)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError):
            publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True)
        interrupted = tree_state(workspace)
        next(journal.glob("publish-*/backups/*.backup")).unlink()
        with pytest.raises(WorkspacePublishError):
            recover_workspace_publications_with_outcomes(workspace, lease, journal)
        assert tree_state(workspace) == interrupted


@pytest.mark.parametrize("operation", ["symlink", "mkdir"])
def test_rollback_restore_can_resume_after_process_death(tmp_path, monkeypatch, operation):
    workspace, lower, journal, diff = removal_fixture(tmp_path)
    original = tree_state(workspace)
    def stop(transaction, path):
        if path == "source":
            raise WorkspacePublishError("interrupted")
    monkeypatch.setattr(publisher, "_after_publish_entry", stop)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError):
            publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True)
    script = f'''
import os
from orchestrator.isolation import acquire_workspace_write_lease, recover_workspace_publications_with_outcomes
real = os.{operation}
def die(*args, **kwargs):
    real(*args, **kwargs)
    os._exit(79)
os.umask(0o077)
os.{operation} = die
with acquire_workspace_write_lease({str(workspace)!r}, {str(journal)!r}) as lease:
    recover_workspace_publications_with_outcomes({str(workspace)!r}, lease, {str(journal)!r})
'''
    child = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert child.returncode == 79, child.stderr
    with acquire_workspace_write_lease(workspace, journal) as lease:
        recovery, = recover_workspace_publications_with_outcomes(workspace, lease, journal)
        assert recovery.outcome == "rolled_back"
        assert tree_state(workspace) == original


def test_death_during_ack_cleanup_cannot_report_false_not_started(tmp_path):
    workspace, lower, journal, diff = removal_fixture(tmp_path)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        receipt = publish_workspace_diff(lower, workspace, diff, lease, journal, retain_committed=True)
    committed = tree_state(workspace)
    script = f'''
import os
from orchestrator.isolation import acquire_workspace_write_lease, acknowledge_workspace_publication, WorkspacePublishReceipt
real = os.unlink
def die(path, *args, **kwargs):
    real(path, *args, **kwargs)
    if path == "state.json":
        os._exit(79)
os.unlink = die
with acquire_workspace_write_lease({str(workspace)!r}, {str(journal)!r}) as lease:
    acknowledge_workspace_publication({str(workspace)!r}, lease, {str(journal)!r}, WorkspacePublishReceipt(**{asdict(receipt)!r}))
'''
    child = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert child.returncode == 79, child.stderr
    with acquire_workspace_write_lease(workspace, journal) as lease:
        assert recover_workspace_publications_with_outcomes(workspace, lease, journal) == ()
        assert tree_state(workspace) == committed
        publisher.acknowledge_workspace_publication(workspace, lease, journal, receipt)
        assert tree_state(workspace) == committed
        assert list(journal.glob("acknowledged-*"))
