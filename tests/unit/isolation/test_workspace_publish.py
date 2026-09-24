from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import textwrap
import json
import stat
from dataclasses import replace
from types import SimpleNamespace

import pytest

from orchestrator.isolation import (
    WorkspacePublishConflict,
    WorkspacePublishError,
    WorkspacePublishRecoveryConflict,
    acquire_workspace_write_lease,
    export_overlay_diff,
    publish_workspace_diff,
    recover_workspace_publications,
)
from orchestrator.isolation import workspace_publish
from orchestrator.isolation import workspace as workspace_module


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path, object]:
    workspace = tmp_path / "workspace"
    lower = tmp_path / "lower"
    upper = tmp_path / "upper"
    candidate = tmp_path / "candidate"
    journal = tmp_path / "journal"
    for root in (workspace, lower, upper):
        root.mkdir()
    for root in (workspace, lower):
        (root / "changed.txt").write_text("old\n", encoding="utf-8")
        (root / "stable.txt").write_text("stay\n", encoding="utf-8")
        (root / "existing").mkdir(mode=0o700)
    (upper / "changed.txt").write_text("new\n", encoding="utf-8")
    (upper / "added").mkdir()
    (upper / "added" / "nested.txt").write_text("added\n", encoding="utf-8")
    (upper / "existing").mkdir(mode=0o755)
    (upper / "link").symlink_to("added/nested.txt")
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    diff = export_overlay_diff(lower, upper, candidate)
    return lower, workspace, candidate, journal, upper, diff


def test_publish_applies_candidate_under_lease_and_cleans_journal(tmp_path: Path) -> None:
    lower, workspace, candidate, journal, _upper, diff = _fixture(tmp_path)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        receipt = publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert receipt.manifest_hash == diff.manifest_hash
        assert receipt.entries_published == len(diff.entries)
        assert receipt.lease_generation == lease.generation
        assert (workspace / "changed.txt").read_text(encoding="utf-8") == "new\n"
        assert (workspace / "added" / "nested.txt").read_text(encoding="utf-8") == "added\n"
        assert os.readlink(workspace / "link") == "added/nested.txt"
        assert (workspace / "existing").stat().st_mode & 0o777 == 0o755
        assert (workspace / "stable.txt").read_text(encoding="utf-8") == "stay\n"
        assert recover_workspace_publications(workspace, lease, journal) == ()
    assert sorted(path.name for path in journal.iterdir() if path.name.startswith("publish-")) == []
    assert candidate.exists()


def test_publish_rejects_live_conflict_without_mutating_workspace(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    (workspace / "changed.txt").write_text("outside edit\n", encoding="utf-8")
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishConflict):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "outside edit\n"
    assert not any(path.name.startswith("publish-") for path in journal.iterdir())


def _run_publish_crash(
    tmp_path: Path,
    *,
    crash_after: int,
    hook: str = "after_entry",
) -> tuple[Path, Path, Path, Path, Path, object, subprocess.CompletedProcess[str]]:
    lower, workspace, candidate, journal, _upper, diff = _fixture(tmp_path)
    script = textwrap.dedent(
        f"""
        import os
        from pathlib import Path
        from orchestrator.isolation import acquire_workspace_write_lease, publish_workspace_diff
        from orchestrator.isolation import workspace_publish
        lower = Path({str(lower)!r})
        workspace = Path({str(workspace)!r})
        journal = Path({str(journal)!r})
        candidate = Path({str(candidate)!r})
        from orchestrator.isolation import workspace as workspace_module
        from orchestrator.isolation import WorkspaceDiff, WorkspaceDiffEntry
        import json
        entries = []
        for item in json.loads(Path({str(tmp_path / 'manifest.json')!r}).read_text()):
            entries.append(WorkspaceDiffEntry(**item))
        diff = WorkspaceDiff(candidate, tuple(entries), {diff.total_bytes}, {diff.manifest_hash!r})
        """
    )
    if hook == "after_entry":
        script += textwrap.dedent(
            f"""
            count = 0
            def crash(_tx, _path):
                global count
                count += 1
                if count == {crash_after}:
                    os._exit(73)
            workspace_publish._after_publish_entry = crash
            with acquire_workspace_write_lease(workspace, journal) as lease:
                publish_workspace_diff(lower, workspace, diff, lease, journal)
            """
        )
    elif hook == "cleanup":
        script += textwrap.dedent(
            """
            def crash(*_args):
                os._exit(74)
            workspace_publish._remove_transaction = crash
            with acquire_workspace_write_lease(workspace, journal) as lease:
                publish_workspace_diff(lower, workspace, diff, lease, journal)
            """
        )
    else:
        script += textwrap.dedent(
            """
            def crash(*_args):
                os._exit(75)
            workspace_publish._apply_record = crash
            with acquire_workspace_write_lease(workspace, journal) as lease:
                publish_workspace_diff(lower, workspace, diff, lease, journal)
            """
        )
    manifest = [entry.__dict__ for entry in diff.entries]
    (tmp_path / "manifest.json").write_text(__import__("json").dumps(manifest), encoding="utf-8")
    env = os.environ.copy()
    source_root = str(Path(__file__).resolve().parents[3] / "src")
    env["PYTHONPATH"] = source_root + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True)
    return lower, workspace, candidate, journal, Path(source_root), diff, result


@pytest.mark.parametrize("crash_after", [1, 2, 3, 4, 5])
def test_process_death_rolls_back_every_published_entry(tmp_path: Path, crash_after: int) -> None:
    lower, workspace, _candidate, journal, _source, _diff, result = _run_publish_crash(
        tmp_path, crash_after=crash_after
    )
    assert result.returncode == 73, result.stderr
    expected_changed = "new\n" if crash_after >= 2 else "old\n"
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == expected_changed
    with acquire_workspace_write_lease(workspace, journal) as lease:
        recovered = recover_workspace_publications(workspace, lease, journal)
        assert len(recovered) == 1
        assert recover_workspace_publications(workspace, lease, journal) == ()
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "old\n"
    assert not (workspace / "added").exists()
    assert not (workspace / "link").exists()
    assert (workspace / "existing").stat().st_mode & 0o777 == 0o700
    assert (lower / "changed.txt").read_text(encoding="utf-8") == "old\n"


def test_recovery_fails_closed_if_workspace_changed_after_process_death(tmp_path: Path) -> None:
    _lower, workspace, _candidate, journal, _source, _diff, result = _run_publish_crash(
        tmp_path, crash_after=1
    )
    assert result.returncode == 73
    (workspace / "changed.txt").write_text("third party\n", encoding="utf-8")
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishRecoveryConflict):
            recover_workspace_publications(workspace, lease, journal)
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "third party\n"
    assert any(path.name.startswith("publish-") for path in journal.iterdir())


def test_recovery_retains_new_directory_if_unrecognized_content_appears(tmp_path: Path) -> None:
    _lower, workspace, _candidate, journal, _source, _diff, result = _run_publish_crash(
        tmp_path, crash_after=1
    )
    assert result.returncode == 73
    (workspace / "added" / "unrecognized.txt").write_text("keep", encoding="utf-8")
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishRecoveryConflict, match="not empty"):
            recover_workspace_publications(workspace, lease, journal)
    assert (workspace / "added" / "unrecognized.txt").read_text(encoding="utf-8") == "keep"


def test_recovery_fails_closed_if_rollback_backup_is_damaged(tmp_path: Path) -> None:
    _lower, workspace, _candidate, journal, _source, _diff, result = _run_publish_crash(
        tmp_path, crash_after=2
    )
    assert result.returncode == 73
    transaction = next(path for path in journal.iterdir() if path.name.startswith("publish-"))
    backup = transaction / "backups" / "00000001.backup"
    backup.write_text("bad\n", encoding="utf-8")
    backup.chmod(0o600)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="integrity check"):
            recover_workspace_publications(workspace, lease, journal)
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "new\n"


def test_recovery_fails_closed_if_rollback_backup_is_missing(tmp_path: Path) -> None:
    _lower, workspace, _candidate, journal, _source, _diff, result = _run_publish_crash(
        tmp_path, crash_after=2
    )
    assert result.returncode == 73
    transaction = next(path for path in journal.iterdir() if path.name.startswith("publish-"))
    (transaction / "backups" / "00000001.backup").unlink()
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishRecoveryConflict, match="could not be restored safely"):
            recover_workspace_publications(workspace, lease, journal)


def test_committed_process_death_is_not_rolled_back(tmp_path: Path) -> None:
    _lower, workspace, _candidate, journal, _source, _diff, result = _run_publish_crash(
        tmp_path, crash_after=0, hook="cleanup"
    )
    assert result.returncode == 74
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "new\n"
    with acquire_workspace_write_lease(workspace, journal) as lease:
        recovered = recover_workspace_publications(workspace, lease, journal)
        assert len(recovered) == 1
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "new\n"


def test_publish_requires_lease_for_same_workspace(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    with acquire_workspace_write_lease(other, journal) as lease:
        with pytest.raises(Exception, match="does not belong"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)


def test_incomplete_prepared_journal_blocks_new_publish_until_recovered(tmp_path: Path) -> None:
    lower, workspace, candidate, journal, _source, diff, _result = _run_publish_crash(
        tmp_path, crash_after=0, hook="before_apply"
    )
    assert candidate.exists()
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="recovery must run"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
        publish_workspace_diff(lower, workspace, diff, lease, journal)
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "new\n"


def test_recovery_removes_crash_before_prepared_journal(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    journal = tmp_path / "journal"
    workspace.mkdir()
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    transaction_id = "a" * 32
    transaction = journal / f"publish-{transaction_id}"
    transaction.mkdir(mode=0o700)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        assert recover_workspace_publications(workspace, lease, journal) == (transaction_id,)
    assert not transaction.exists()


def test_recovery_rejects_corrupt_journal_and_retains_it(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    journal = tmp_path / "journal"
    workspace.mkdir()
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    transaction = journal / f"publish-{'b' * 32}"
    transaction.mkdir(mode=0o700)
    (transaction / "state.json").write_text("not-json\n", encoding="utf-8")
    (transaction / "state.json").chmod(0o600)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="corrupt"):
            recover_workspace_publications(workspace, lease, journal)
    assert transaction.exists()


@pytest.mark.parametrize("bad_value", [True, -1])
def test_publish_rejects_invalid_backup_bounds(tmp_path: Path, bad_value: object) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="max_backup_bytes"):
            publish_workspace_diff(lower, workspace, diff, lease, journal, max_backup_bytes=bad_value)  # type: ignore[arg-type]


def test_publish_rejects_backup_budget_below_existing_content(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="rollback inspection bound"):
            publish_workspace_diff(lower, workspace, diff, lease, journal, max_backup_bytes=3)
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "old\n"


def test_publish_requires_private_disjoint_journal_root(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    nested_journal = workspace / "control"
    nested_journal.mkdir(mode=0o700)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="disjoint"):
            publish_workspace_diff(lower, workspace, diff, lease, nested_journal)
        bad_mode = tmp_path / "bad-mode"
        bad_mode.mkdir(mode=0o755)
        bad_mode.chmod(0o755)
        with pytest.raises(WorkspacePublishError, match="mode 0700"):
            publish_workspace_diff(lower, workspace, diff, lease, bad_mode)


def test_publish_refuses_existing_unknown_transaction_name(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    (journal / "publish-not-a-transaction-id").mkdir(mode=0o700)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="pending publication recovery"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)


def test_recovery_rejects_invalid_transaction_names_and_non_directories(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    journal = tmp_path / "journal"
    workspace.mkdir()
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    invalid_name = journal / "publish-invalid"
    invalid_name.mkdir()
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="invalid transaction name"):
            recover_workspace_publications(workspace, lease, journal)
    invalid_name.rmdir()
    not_directory = journal / f"publish-{'c' * 32}"
    not_directory.write_text("occupied", encoding="utf-8")
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="not a real directory"):
            recover_workspace_publications(workspace, lease, journal)


def test_recovery_requires_private_transaction_directory(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    journal = tmp_path / "journal"
    workspace.mkdir()
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    transaction = journal / f"publish-{'d' * 32}"
    transaction.mkdir(mode=0o755)
    transaction.chmod(0o755)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="mode 0700"):
            recover_workspace_publications(workspace, lease, journal)


def test_publish_detects_path_occupied_after_conflict_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    original_state = workspace_publish._state_at
    occupied = False

    def occupy_added_path(root_fd: int, mount_id: int, parts: tuple[str, ...], *, max_bytes: int):
        nonlocal occupied
        result = original_state(root_fd, mount_id, parts, max_bytes=max_bytes)
        if parts == ("added",) and result is None and not occupied:
            (workspace / "added").mkdir()
            occupied = True
            return original_state(root_fd, mount_id, parts, max_bytes=max_bytes)
        return result

    monkeypatch.setattr(workspace_publish, "_state_at", occupy_added_path)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishConflict, match="path types"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
    assert (workspace / "added").is_dir()


def test_publish_rejects_preexisting_transaction_temp_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    transaction_id = "e" * 32
    (workspace / f".mp{transaction_id}0").write_text("keep", encoding="utf-8")
    monkeypatch.setattr(workspace_publish.secrets, "token_hex", lambda _count: transaction_id)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="temporary path already exists"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
    assert (workspace / f".mp{transaction_id}0").read_text(encoding="utf-8") == "keep"


def test_publish_enforces_aggregate_rollback_backup_limit(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, upper, _diff = _fixture(tmp_path)
    for root, content in ((lower, "orig\n"), (workspace, "orig\n")):
        (root / "second.txt").write_text(content, encoding="utf-8")
    (upper / "second.txt").write_text("next\n", encoding="utf-8")
    diff = export_overlay_diff(lower, upper, tmp_path / "candidate-2")
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="rollback backup exceeds its byte limit"):
            publish_workspace_diff(lower, workspace, diff, lease, journal, max_backup_bytes=8)
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "old\n"
    assert (workspace / "second.txt").read_text(encoding="utf-8") == "orig\n"


def test_publish_rejects_candidate_directory_without_recovery_access(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, upper, _diff = _fixture(tmp_path)
    (upper / "added").chmod(0o500)
    diff = export_overlay_diff(lower, upper, tmp_path / "candidate-2")
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="owner read/write/execute"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)


def test_publish_rejects_symlinked_journal_root(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    journal_alias = tmp_path / "journal-alias"
    journal_alias.symlink_to(journal, target_is_directory=True)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="real directories"):
            publish_workspace_diff(lower, workspace, diff, lease, journal_alias)


def test_publish_wraps_filesystem_error_and_preserves_recoverable_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)

    def fail_apply(*_args: object) -> None:
        raise OSError("simulated disk error")

    monkeypatch.setattr(workspace_publish, "_apply_record", fail_apply)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="publication failed"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "old\n"


def test_backup_fsync_error_fails_closed_with_recoverable_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        real_fsync = workspace_publish.os.fsync

        def fail_backup_fsync(descriptor: int) -> None:
            try:
                descriptor_path = os.readlink(f"/proc/self/fd/{descriptor}")
            except OSError:
                descriptor_path = ""
            if "/backups/" in descriptor_path:
                raise OSError("backup fsync failure")
            real_fsync(descriptor)

        monkeypatch.setattr(workspace_publish.os, "fsync", fail_backup_fsync)
        with pytest.raises(WorkspacePublishError, match="rollback backup cannot be persisted"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "old\n"


def test_publish_detects_candidate_file_mutated_after_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, candidate, journal, _upper, diff = _fixture(tmp_path)
    original_capture = workspace_publish._capture_originals

    def capture_then_mutate(*args: object, **kwargs: object):
        result = original_capture(*args, **kwargs)
        (candidate / "changed.txt").write_text("mutated candidate\n", encoding="utf-8")
        return result

    monkeypatch.setattr(workspace_publish, "_capture_originals", capture_then_mutate)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="candidate file changed after validation"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "old\n"


def test_publish_rejects_unreadable_final_file_mode(tmp_path: Path) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    entries = tuple(
        replace(entry, mode=0o200) if entry.path == "changed.txt" else entry
        for entry in diff.entries
    )
    unsafe_diff = replace(
        diff,
        entries=entries,
        manifest_hash=workspace_module._workspace_diff_hash(entries),
    )
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="owner read access"):
            publish_workspace_diff(lower, workspace, unsafe_diff, lease, journal)


def test_publish_binds_symlink_target_to_candidate_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, candidate, journal, _upper, diff = _fixture(tmp_path)
    original_check = workspace_publish.check_workspace_publish_conflicts

    def check_then_mutate(*args: object, **kwargs: object):
        result = original_check(*args, **kwargs)
        (candidate / "link").unlink()
        (candidate / "link").symlink_to("stable.txt")
        return result

    monkeypatch.setattr(workspace_publish, "check_workspace_publish_conflicts", check_then_mutate)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="candidate symlink changed after validation"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
    assert not (workspace / "link").exists()


def test_publish_detects_symlink_mutated_after_journal_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, candidate, journal, _upper, diff = _fixture(tmp_path)
    original_capture = workspace_publish._capture_originals

    def capture_then_mutate(*args: object, **kwargs: object):
        result = original_capture(*args, **kwargs)
        (candidate / "link").unlink()
        (candidate / "link").symlink_to("stable.txt")
        return result

    monkeypatch.setattr(workspace_publish, "_capture_originals", capture_then_mutate)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="candidate symlink changed after validation"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    assert not (workspace / "link").exists()


def test_publish_rejects_forged_lease_and_missing_workspace_path(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    journal = tmp_path / "journal"
    workspace.mkdir()
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    with pytest.raises(WorkspacePublishError, match="live workspace write lease"):
        workspace_publish._verify_lease_for_workspace(None, workspace)  # type: ignore[arg-type]
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="cannot be inspected"):
            workspace_publish._verify_lease_for_workspace(lease, tmp_path / "missing")
        other = tmp_path / "other"
        other.mkdir()
        descriptor = os.open(other, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with pytest.raises(WorkspacePublishError, match="root changed"):
                workspace_publish._verify_workspace_fd(descriptor, lease)
        finally:
            os.close(descriptor)


def test_backup_rejects_hardlink_race(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    original_state = workspace_publish._state_at
    linked = False

    def link_after_state(root_fd: int, mount_id: int, parts: tuple[str, ...], *, max_bytes: int):
        nonlocal linked
        result = original_state(root_fd, mount_id, parts, max_bytes=max_bytes)
        if parts == ("changed.txt",) and result is not None and not linked:
            os.link(workspace / "changed.txt", workspace / "external-link")
            linked = True
        return result

    monkeypatch.setattr(workspace_publish, "_state_at", link_after_state)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="private regular file"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    (workspace / "external-link").unlink()


def test_backup_rejects_content_race_while_copying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    original_write = workspace_publish._write_all
    raced = False

    def write_then_edit_backup(destination_fd: int, payload: bytes) -> None:
        nonlocal raced
        original_write(destination_fd, payload)
        try:
            destination_path = os.readlink(f"/proc/self/fd/{destination_fd}")
        except OSError:
            return
        if "/backups/" in destination_path and not raced:
            (workspace / "changed.txt").write_text("concurrent edit\n", encoding="utf-8")
            raced = True

    monkeypatch.setattr(workspace_publish, "_write_all", write_then_edit_backup)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishConflict, match="while creating rollback backup"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    assert raced
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "concurrent edit\n"


def test_workspace_state_rejects_special_files_and_long_symlinks(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFO creation is unavailable")
    os.mkfifo(workspace / "pipe")
    (workspace / "link").symlink_to("target-that-is-too-long")
    root_fd = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY)
    try:
        mount_id = workspace_publish._descriptor_mount_id(root_fd)
        with pytest.raises(WorkspacePublishError, match="special file"):
            workspace_publish._state_at(root_fd, mount_id, ("pipe",), max_bytes=100)
        with pytest.raises(WorkspacePublishError, match="symlink exceeds"):
            workspace_publish._state_at(root_fd, mount_id, ("link",), max_bytes=2)
    finally:
        os.close(root_fd)


def test_transaction_cleanup_rejects_non_directory_and_roots_close_is_idempotent(tmp_path: Path) -> None:
    directory = tmp_path / "journal"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        (directory / "publish-file").write_text("no", encoding="utf-8")
        with pytest.raises(WorkspacePublishError, match="path changed before cleanup"):
            workspace_publish._remove_transaction(descriptor, "publish-file")
    finally:
        os.close(descriptor)
    roots = workspace_publish._Roots(
        workspace_fd=-1,
        workspace_path=directory,
        workspace_mount_id=0,
        journal_fd=-1,
        journal_path=directory,
        candidate_fd=-1,
        candidate_path=None,
        lower_path=None,
    )
    roots.close()


def test_publish_transaction_creation_error_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    original_mkdir = workspace_publish.os.mkdir

    def deny_transaction(name: str, *args: object, **kwargs: object) -> None:
        if isinstance(name, str) and name.startswith("publish-"):
            raise PermissionError("journal root denied")
        original_mkdir(name, *args, **kwargs)

    monkeypatch.setattr(workspace_publish.os, "mkdir", deny_transaction)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="journal cannot be created"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)


def test_recovery_wraps_journal_directory_scan_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "workspace"
    journal = tmp_path / "journal"
    workspace.mkdir()
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    journal_info = journal.stat()
    real_listdir = workspace_publish.os.listdir

    def deny_journal(descriptor: int | str | os.PathLike[str]):
        if isinstance(descriptor, int):
            info = os.fstat(descriptor)
            if (info.st_dev, info.st_ino) == (journal_info.st_dev, journal_info.st_ino):
                raise PermissionError("journal scan denied")
        return real_listdir(descriptor)

    with acquire_workspace_write_lease(workspace, journal) as lease:
        monkeypatch.setattr(workspace_publish.os, "listdir", deny_journal)
        with pytest.raises(WorkspacePublishError, match="cannot be scanned"):
            recover_workspace_publications(workspace, lease, journal)


def test_open_roots_closes_descriptors_after_each_root_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "workspace"
    journal = tmp_path / "journal"
    candidate = tmp_path / "candidate"
    workspace.mkdir()
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    candidate.mkdir(mode=0o700)
    candidate.chmod(0o700)
    with pytest.raises(WorkspacePublishError, match="lower snapshot cannot be resolved"):
        workspace_publish._open_roots(tmp_path / "missing-lower", workspace, None, journal)
    with pytest.raises(WorkspacePublishError, match="cannot be opened safely"):
        workspace_publish._open_roots(None, workspace, tmp_path / "missing-candidate", journal)

    real_mount = workspace_publish._descriptor_mount_id
    monkeypatch.setattr(
        workspace_publish,
        "_descriptor_mount_id",
        lambda _descriptor: (_ for _ in ()).throw(WorkspacePublishError("mount probe failed")),
    )
    with pytest.raises(WorkspacePublishError, match="mount probe failed"):
        workspace_publish._open_roots(None, workspace, candidate, journal)
    monkeypatch.setattr(workspace_publish, "_descriptor_mount_id", real_mount)


def test_open_directory_detects_root_identity_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    real_fstat = workspace_publish.os.fstat

    def changed_identity(descriptor: int):
        info = real_fstat(descriptor)
        return SimpleNamespace(st_dev=info.st_dev, st_ino=info.st_ino + 1)

    monkeypatch.setattr(workspace_publish.os, "fstat", changed_identity)
    with pytest.raises(WorkspacePublishError, match="changed while opening"):
        workspace_publish._open_directory(workspace, private=False)


def test_candidate_change_after_read_only_conflict_check_is_detected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    original_check = workspace_publish.check_workspace_publish_conflicts

    def change_after_check(*args: object, **kwargs: object) -> object:
        result = original_check(*args, **kwargs)
        (workspace / "changed.txt").write_text("raced update\n", encoding="utf-8")
        return result

    monkeypatch.setattr(workspace_publish, "check_workspace_publish_conflicts", change_after_check)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishConflict, match="after the candidate conflict check"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "raced update\n"


def test_modified_symlink_is_restored_after_interrupted_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower = tmp_path / "lower"
    workspace = tmp_path / "workspace"
    upper = tmp_path / "upper"
    candidate = tmp_path / "candidate"
    journal = tmp_path / "journal"
    for root in (lower, workspace, upper):
        root.mkdir()
    for root in (lower, workspace):
        (root / "old.txt").write_text("old", encoding="utf-8")
        (root / "new.txt").write_text("new", encoding="utf-8")
        (root / "link").symlink_to("old.txt")
    (upper / "link").symlink_to("new.txt")
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    diff = export_overlay_diff(lower, upper, candidate)
    assert [(entry.path, entry.operation) for entry in diff.entries] == [("link", "modify")]

    def fail_after_install(_transaction_id: str, _path: str) -> None:
        raise WorkspacePublishError("simulated interruption")

    monkeypatch.setattr(workspace_publish, "_after_publish_entry", fail_after_install)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="simulated interruption"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert os.readlink(workspace / "link") == "new.txt"
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    assert os.readlink(workspace / "link") == "old.txt"


def test_recovery_handles_crash_after_new_directory_mkdir_before_chmod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)

    def mkdir_then_interrupt(roots: object, record: dict[str, object], _transaction_id: str) -> None:
        if record["path"] == "added":
            roots = roots  # keep the injected boundary explicit for coverage
            os.mkdir("added", 0o700, dir_fd=roots.workspace_fd)
            raise WorkspacePublishError("crash between mkdir and chmod")
        raise AssertionError("the parent directory should be the first publication entry")

    monkeypatch.setattr(workspace_publish, "_apply_record", mkdir_then_interrupt)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="mkdir and chmod"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert (workspace / "added").is_dir()
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    assert not (workspace / "added").exists()
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "old\n"


def test_recovery_handles_crash_between_link_and_temp_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, workspace, _candidate, journal, _upper, diff = _fixture(tmp_path)
    original_install = workspace_publish._install_temporary

    def link_then_interrupt(parent_fd: int, temporary: str, leaf: str, no_replace: bool) -> None:
        info = os.stat(temporary, dir_fd=parent_fd, follow_symlinks=False)
        if no_replace and stat.S_ISREG(info.st_mode):
            os.link(
                temporary,
                leaf,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
            raise WorkspacePublishError("crash between hard link and temp unlink")
        original_install(parent_fd, temporary, leaf, no_replace)

    monkeypatch.setattr(workspace_publish, "_install_temporary", link_then_interrupt)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="hard link and temp unlink"):
            publish_workspace_diff(lower, workspace, diff, lease, journal)
        assert (workspace / "added" / "nested.txt").exists()
        assert len(recover_workspace_publications(workspace, lease, journal)) == 1
    assert not (workspace / "added").exists()
    assert (workspace / "changed.txt").read_text(encoding="utf-8") == "old\n"


def test_recovery_rejects_tampered_backup_reference(tmp_path: Path) -> None:
    _lower, workspace, _candidate, journal, _source, _diff, result = _run_publish_crash(
        tmp_path, crash_after=1
    )
    assert result.returncode == 73
    transaction = next(path for path in journal.iterdir() if path.name.startswith("publish-"))
    payload = json.loads((transaction / "state.json").read_text(encoding="utf-8"))
    payload["records"][1]["backup"] = "../../outside.backup"
    (transaction / "state.json").write_text(json.dumps(payload) + "\n", encoding="utf-8")
    (transaction / "state.json").chmod(0o600)
    with acquire_workspace_write_lease(workspace, journal) as lease:
        with pytest.raises(WorkspacePublishError, match="backup reference"):
            recover_workspace_publications(workspace, lease, journal)
    assert transaction.exists()


@pytest.mark.parametrize(
    ("value", "allow_none", "message"),
    [
        (None, False, "omits its candidate"),
        ([], False, "invalid path state"),
        ({"kind": "file", "mode": 0o600}, False, "malformed path metadata"),
        ({"kind": "file", "mode": 0o600, "digest": "sha256:" + "g" * 64, "size": 0}, False, "invalid content metadata"),
        ({"kind": "symlink", "mode": 0o777, "digest": "sha256:" + "0" * 64, "size": 1, "target": 5}, False, "invalid symlink target"),
        (None, True, ""),
    ],
)
def test_journal_state_schema_rejects_malformed_values(value: object, allow_none: bool, message: str) -> None:
    if not message:
        workspace_publish._validate_state(value, allow_none=allow_none)
        return
    with pytest.raises(WorkspacePublishError, match=message):
        workspace_publish._validate_state(value, allow_none=allow_none)


def test_journal_payload_schema_validates_owner_generation_and_paths(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    journal = tmp_path / "journal"
    workspace.mkdir()
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    transaction_id = "f" * 32
    with acquire_workspace_write_lease(workspace, journal) as lease:
        payload = {
            "schema": 1,
            "status": "prepared",
            "transaction_id": transaction_id,
            "workspace_device": lease.workspace_device,
            "workspace_inode": lease.workspace_inode,
            "lease_generation": lease.generation,
            "manifest_hash": "sha256:" + "0" * 64,
            "backup_bytes": 0,
            "records": [],
        }
        workspace_publish._validate_payload(payload, f"publish-{transaction_id}", lease)
        for key, value in (("schema", True), ("lease_generation", "1"), ("manifest_hash", "sha256:bad")):
            malformed = dict(payload)
            malformed[key] = value
            with pytest.raises(WorkspacePublishError, match="leased workspace"):
                workspace_publish._validate_payload(malformed, f"publish-{transaction_id}", lease)

        record = {
            "path": "safe.txt",
            "operation": "add",
            "before": None,
            "after": {"kind": "file", "mode": 0o600, "digest": "sha256:" + "0" * 64, "size": 0},
            "backup": None,
            "temporary": f".mp{transaction_id}0",
        }
        valid_record_payload = {**payload, "records": [record]}
        workspace_publish._validate_payload(valid_record_payload, f"publish-{transaction_id}", lease)
        invalid_records = [
            {**record, "temporary": "../../escape"},
            {**record, "path": ".git/config"},
            {**record, "operation": "modify"},
            {**record, "backup": "00000000.backup"},
            {**record, "path": "a/../b"},
            {"path": "safe.txt"},
        ]
        for invalid in invalid_records:
            with pytest.raises(WorkspacePublishError):
                workspace_publish._validate_payload(
                    {**payload, "records": [invalid]}, f"publish-{transaction_id}", lease
                )
        file_state = {
            "kind": "file", "mode": 0o600, "digest": "sha256:" + "1" * 64, "size": 1
        }
        directory_to_file = {
            **record,
            "operation": "modify",
            "before": {"kind": "directory", "mode": 0o700},
        }
        with pytest.raises(WorkspacePublishError, match="changes a path type"):
            workspace_publish._validate_payload(
                {**payload, "records": [directory_to_file]}, f"publish-{transaction_id}", lease
            )
        missing_file_backup = {
            **record,
            "operation": "modify",
            "before": file_state,
        }
        with pytest.raises(WorkspacePublishError, match="missing a file rollback backup"):
            workspace_publish._validate_payload(
                {**payload, "records": [missing_file_backup]}, f"publish-{transaction_id}", lease
            )
        symlink_state = {
            "kind": "symlink", "mode": 0o777, "digest": "sha256:" + "2" * 64,
            "size": len("../escape"), "target": "../escape",
        }
        unsafe_candidate_symlink = {
            **record,
            "after": symlink_state,
            "temporary": f".mp{transaction_id}0",
        }
        with pytest.raises(WorkspacePublishError, match="unsafe candidate symlink"):
            workspace_publish._validate_payload(
                {**payload, "records": [unsafe_candidate_symlink]}, f"publish-{transaction_id}", lease
            )
        reverse_order = [
            {**record, "path": "z.txt", "temporary": f".mp{transaction_id}0"},
            {**record, "path": "a.txt", "temporary": f".mp{transaction_id}1"},
        ]
        with pytest.raises(WorkspacePublishError, match="not canonical"):
            workspace_publish._validate_payload(
                {**payload, "records": reverse_order}, f"publish-{transaction_id}", lease
            )


def test_private_directory_opening_and_safe_parent_resolution(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    file_path = workspace / "file"
    file_path.write_text("x", encoding="utf-8")
    (workspace / "link").symlink_to("file")
    root_fd = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY)
    try:
        mount_id = workspace_publish._descriptor_mount_id(root_fd)
        assert workspace_publish._try_open_parent(root_fd, mount_id, ("missing",)) is None
        with pytest.raises(WorkspacePublishError, match="non-directory or symlink"):
            workspace_publish._try_open_parent(root_fd, mount_id, ("file",))
        with pytest.raises(WorkspacePublishError, match="non-directory or symlink"):
            workspace_publish._try_open_parent(root_fd, mount_id, ("link",))
        with pytest.raises(WorkspacePublishError, match="parent directory is missing"):
            workspace_publish._open_parent(root_fd, mount_id, ("missing",))
        assert workspace_publish._state_at(root_fd, mount_id, ("absent",), max_bytes=10) is None
        with pytest.raises(WorkspacePublishError, match="rollback inspection bound"):
            workspace_publish._state_at(root_fd, mount_id, ("file",), max_bytes=0)
        descriptor = os.open(file_path, os.O_RDONLY)
        try:
            with pytest.raises(WorkspacePublishError, match="rollback inspection bound"):
                workspace_publish._digest_fd(descriptor, 0)
        finally:
            os.close(descriptor)
    finally:
        os.close(root_fd)

    with pytest.raises(WorkspacePublishError, match="real directories"):
        workspace_publish._open_directory(file_path, private=False)
    alias = tmp_path / "alias"
    alias.symlink_to(workspace, target_is_directory=True)
    with pytest.raises(WorkspacePublishError, match="real directories"):
        workspace_publish._open_directory(alias, private=False)
    with pytest.raises(WorkspacePublishError, match="cannot be opened safely"):
        workspace_publish._open_directory(tmp_path / "does-not-exist", private=False)


def test_journal_read_and_write_fail_closed_on_bounds_and_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    transaction = tmp_path / "transaction"
    transaction.mkdir(mode=0o700)
    transaction.chmod(0o700)
    descriptor = os.open(transaction, os.O_RDONLY | os.O_DIRECTORY)
    try:
        monkeypatch.setattr(workspace_publish, "_MAX_JOURNAL_BYTES", 8)
        with pytest.raises(WorkspacePublishError, match="size bound"):
            workspace_publish._write_journal(descriptor, {"payload": "larger than eight"})
        monkeypatch.setattr(workspace_publish, "_MAX_JOURNAL_BYTES", 1024)
        (transaction / "state.json").write_text("[]\n", encoding="utf-8")
        (transaction / "state.json").chmod(0o600)
        with pytest.raises(WorkspacePublishError, match="invalid schema"):
            workspace_publish._read_journal(descriptor)
        (transaction / "state.json").write_text("{}", encoding="utf-8")
        (transaction / "state.json").chmod(0o600)
        with pytest.raises(WorkspacePublishError, match="incomplete"):
            workspace_publish._read_journal(descriptor)
        (transaction / "state.json").write_text("{}\n", encoding="utf-8")
        (transaction / "state.json").chmod(0o644)
        with pytest.raises(WorkspacePublishError, match="private regular file"):
            workspace_publish._read_journal(descriptor)
        (transaction / "state.json").chmod(0o600)
        (transaction / "state.json").write_text("0123456789012345\n", encoding="utf-8")
        monkeypatch.setattr(workspace_publish, "_MAX_JOURNAL_BYTES", 8)
        with pytest.raises(WorkspacePublishError, match="size bound"):
            workspace_publish._read_journal(descriptor)
        monkeypatch.setattr(workspace_publish, "_MAX_JOURNAL_BYTES", 1024)
        monkeypatch.setattr(workspace_publish.os, "fsync", lambda _fd: (_ for _ in ()).throw(OSError("fsync")))
        with pytest.raises(WorkspacePublishError, match="journal cannot be persisted"):
            workspace_publish._write_journal(descriptor, {"payload": "small"})
    finally:
        os.close(descriptor)
def test_cleanup_and_scanning_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "journal"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        workspace_publish._remove_transaction(descriptor, "absent")
        (directory / "publish-x").write_text("not a directory", encoding="utf-8")
        with pytest.raises(WorkspacePublishError, match="path changed before cleanup"):
            workspace_publish._remove_transaction(descriptor, "publish-x")
        monkeypatch.setattr(workspace_publish.os, "listdir", lambda _fd: (_ for _ in ()).throw(OSError("denied")))
        with pytest.raises(WorkspacePublishError, match="cannot be scanned"):
            workspace_publish._assert_no_pending_transactions(descriptor)
    finally:
        os.close(descriptor)
