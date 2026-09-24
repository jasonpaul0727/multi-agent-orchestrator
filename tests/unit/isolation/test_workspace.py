from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.isolation import WorkspaceBoundaryError, inspect_workspace
from orchestrator.isolation import workspace as workspace_module


def test_inspection_accepts_internal_hardlinks_and_symlinks(tmp_path):
    workspace = tmp_path / "workspace"
    nested = workspace / "src"
    nested.mkdir(parents=True)
    source = nested / "module.py"
    source.write_text("value = 1\n", encoding="utf-8")
    os.link(source, nested / "module-alias.py")
    (workspace / "module-link.py").symlink_to(source)

    report = inspect_workspace(workspace)

    assert report.regular_files == 2
    assert report.directories == 2
    assert report.symlinks == 1


def test_inspection_rejects_hard_link_to_a_path_outside_workspace(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside_secret = tmp_path / "outside-secret"
    outside_secret.write_text("secret", encoding="utf-8")
    os.link(outside_secret, workspace / "linked-secret")

    with pytest.raises(WorkspaceBoundaryError, match="hard link"):
        inspect_workspace(workspace)

    os.unlink(workspace / "linked-secret")


def test_inspection_rejects_symlink_escape_and_special_files(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "escape").symlink_to(tmp_path / "outside")
    with pytest.raises(WorkspaceBoundaryError, match="symlink escapes"):
        inspect_workspace(workspace)

    (workspace / "escape").unlink()
    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFO boundary check requires POSIX")
    os.mkfifo(workspace / "command.fifo")
    with pytest.raises(WorkspaceBoundaryError, match="special file"):
        inspect_workspace(workspace)


def test_inspection_does_not_follow_internal_directory_symlinks(tmp_path):
    workspace = tmp_path / "workspace"
    real_dir = workspace / "real"
    real_dir.mkdir(parents=True)
    (real_dir / "source.py").write_text("pass\n", encoding="utf-8")
    (workspace / "alias").symlink_to(real_dir, target_is_directory=True)

    report = inspect_workspace(workspace)

    assert report.directories == 2
    assert report.regular_files == 1
    assert report.symlinks == 1


def test_inspection_rejects_symlink_or_non_directory_root(tmp_path):
    directory = tmp_path / "workspace"
    directory.mkdir()
    link = tmp_path / "workspace-link"
    link.symlink_to(directory)
    with pytest.raises(WorkspaceBoundaryError, match="real directory"):
        inspect_workspace(link)
    with pytest.raises(WorkspaceBoundaryError, match="cannot be inspected"):
        inspect_workspace(directory / "missing")


def test_inspection_rejects_nested_mount(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    mountpoint = workspace / "mounted"
    mountpoint.mkdir(parents=True)
    monkeypatch.setattr(os.path, "ismount", lambda path: path == mountpoint)

    with pytest.raises(WorkspaceBoundaryError, match="nested mount"):
        inspect_workspace(workspace)


def test_inspection_rejects_non_directory_components_and_walk_errors(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "not-directory").write_text("file", encoding="utf-8")

    def fake_walk(root, *, topdown, followlinks, onerror):
        return iter([(str(root), ["not-directory"], [])])

    monkeypatch.setattr(workspace_module.os, "walk", fake_walk)
    with pytest.raises(WorkspaceBoundaryError, match="non-directory path"):
        inspect_workspace(workspace)

    def failing_walk(root, *, topdown, followlinks, onerror):
        onerror(PermissionError("private path detail"))
        return iter(())

    monkeypatch.setattr(workspace_module.os, "walk", failing_walk)
    with pytest.raises(WorkspaceBoundaryError, match="cannot be inspected") as error:
        inspect_workspace(workspace)
    assert "private path detail" not in str(error.value)


@pytest.mark.parametrize("entry_kind", ("directory", "file"))
def test_inspection_rejects_tree_changes_and_device_crossings(
    tmp_path, monkeypatch, entry_kind
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    entry = workspace / "nested"
    if entry_kind == "directory":
        entry.mkdir()
    else:
        entry.write_text("x", encoding="utf-8")
    original_lstat = Path.lstat

    def disappeared(self):
        if self == entry:
            raise OSError("path moved")
        return original_lstat(self)

    monkeypatch.setattr(Path, "lstat", disappeared)
    with pytest.raises(WorkspaceBoundaryError, match="changed during"):
        inspect_workspace(workspace)

    monkeypatch.setattr(Path, "lstat", original_lstat)
    entry_stat = original_lstat(entry)

    def other_device(self):
        if self == entry:
            return SimpleNamespace(
                st_mode=entry_stat.st_mode,
                st_dev=entry_stat.st_dev + 1,
                st_ino=entry_stat.st_ino,
                st_nlink=entry_stat.st_nlink,
            )
        return original_lstat(self)

    monkeypatch.setattr(Path, "lstat", other_device)
    with pytest.raises(WorkspaceBoundaryError, match="nested mount"):
        inspect_workspace(workspace)
