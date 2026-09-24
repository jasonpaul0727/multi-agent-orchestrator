from __future__ import annotations

import os
from pathlib import Path
import stat

import pytest

from orchestrator.isolation.workspace import WorkspaceBoundaryError, snapshot_workspace


def test_snapshot_copies_only_safe_content_and_omits_control_directories(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "nested").mkdir()
    (source / "nested" / "input.txt").write_text("stable input", encoding="utf-8")
    (source / "executable.sh").write_text("#!/bin/sh\ntrue\n", encoding="utf-8")
    (source / "executable.sh").chmod(0o755)
    (source / "link").symlink_to("nested/input.txt")
    for name in (".git", ".maestro"):
        (source / name).mkdir()
        (source / name / "private").write_text("not copied", encoding="utf-8")
    destination = tmp_path / "snapshot"

    summary = snapshot_workspace(source, destination)

    assert summary.regular_files == 2
    assert summary.directories == 2
    assert summary.symlinks == 1
    assert (destination / "nested" / "input.txt").read_text(encoding="utf-8") == "stable input"
    assert os.readlink(destination / "link") == "nested/input.txt"
    assert os.access(destination / "nested" / "input.txt", os.W_OK) is False
    assert stat.S_IMODE((destination / "executable.sh").stat().st_mode) == 0o555
    assert not (destination / ".git").exists()
    assert not (destination / ".maestro").exists()
    for path in (destination, destination / "nested"):
        assert stat.S_IMODE(path.stat().st_mode) == 0o555


@pytest.mark.parametrize("target", ["/etc/passwd", "../../outside", "../../../tmp/escape"])
def test_snapshot_rejects_absolute_or_escaping_symlinks(tmp_path: Path, target: str) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "link").symlink_to(target)
    with pytest.raises(WorkspaceBoundaryError, match="symlink"):
        snapshot_workspace(source, tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()


def test_snapshot_rejects_special_files_and_removes_partial_tree(tmp_path: Path) -> None:
    if not hasattr(os, "mkfifo"):
        pytest.skip("named pipes require POSIX")
    source = tmp_path / "source"
    source.mkdir()
    (source / "before.txt").write_text("copied first", encoding="utf-8")
    os.mkfifo(source / "pipe")
    destination = tmp_path / "snapshot"
    with pytest.raises(WorkspaceBoundaryError, match="special file"):
        snapshot_workspace(source, destination)
    assert not destination.exists()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_entries": 1}, "entry limit"),
        ({"max_bytes": 2}, "byte limit"),
        ({"max_depth": 1}, "depth limit"),
        ({"max_entries": 0}, "max_entries"),
        ({"max_bytes": -1}, "max_bytes"),
        ({"max_depth": True}, "max_depth"),
    ],
)
def test_snapshot_enforces_size_depth_and_option_bounds(tmp_path: Path, kwargs, message: str) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "a" / "b").mkdir(parents=True)
    (source / "a" / "b" / "file").write_text("12345", encoding="utf-8")
    with pytest.raises(WorkspaceBoundaryError, match=message):
        snapshot_workspace(source, tmp_path / "snapshot", **kwargs)
    assert not (tmp_path / "snapshot").exists()


def test_snapshot_rejects_existing_target_and_symlinked_root(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    destination = tmp_path / "already-there"
    destination.mkdir()
    with pytest.raises(WorkspaceBoundaryError, match="destination"):
        snapshot_workspace(source, destination)
    link = tmp_path / "root-link"
    link.symlink_to(source, target_is_directory=True)
    with pytest.raises(WorkspaceBoundaryError, match="root"):
        snapshot_workspace(link, tmp_path / "other")


def test_snapshot_destination_cannot_be_inside_source(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    with pytest.raises(WorkspaceBoundaryError, match="outside the source"):
        snapshot_workspace(source, source / "snapshot")
    assert not (source / "snapshot").exists()


def test_snapshot_removes_destination_when_mount_identity_cannot_be_read(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.workspace as workspace_module

    source = tmp_path / "source"
    source.mkdir()
    destination = tmp_path / "snapshot"

    def unavailable(_descriptor: int) -> int:
        raise WorkspaceBoundaryError("mount id unavailable")

    monkeypatch.setattr(workspace_module, "_descriptor_mount_id", unavailable)
    with pytest.raises(WorkspaceBoundaryError, match="mount id"):
        snapshot_workspace(source, destination)
    assert not destination.exists()


def test_snapshot_pins_root_against_path_replacement(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "marker").write_text("workspace", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "marker").write_text("outside", encoding="utf-8")
    moved = tmp_path / "original-root"
    real_open = os.open
    changed = False

    def racing_open(path, flags, *args, **kwargs):
        nonlocal changed
        if not changed and Path(path) == source:
            changed = True
            source.rename(moved)
            source.symlink_to(outside, target_is_directory=True)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", racing_open)
    with pytest.raises(WorkspaceBoundaryError, match="root"):
        snapshot_workspace(source, tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()


def test_snapshot_rejects_file_replaced_by_symlink_during_open(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    victim = source / "item"
    victim.write_text("authorized", encoding="utf-8")
    outside = tmp_path / "outside-secret"
    outside.write_text("not authorized", encoding="utf-8")
    real_open = os.open
    replaced = False

    def racing_open(path, flags, *args, **kwargs):
        nonlocal replaced
        if not replaced and path == "item" and kwargs.get("dir_fd") is not None:
            replaced = True
            victim.unlink()
            victim.symlink_to(outside)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", racing_open)
    with pytest.raises(WorkspaceBoundaryError, match="file changed"):
        snapshot_workspace(source, tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()


def test_snapshot_rejects_directory_replaced_by_symlink_during_open(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    nested = source / "nested"
    nested.mkdir()
    (nested / "marker").write_text("workspace", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "marker").write_text("not workspace", encoding="utf-8")
    moved = source / "moved"
    real_open = os.open
    replaced = False

    def racing_open(path, flags, *args, **kwargs):
        nonlocal replaced
        if not replaced and path == "nested" and kwargs.get("dir_fd") is not None:
            replaced = True
            nested.rename(moved)
            nested.symlink_to(outside, target_is_directory=True)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", racing_open)
    with pytest.raises(WorkspaceBoundaryError, match="directory changed"):
        snapshot_workspace(source, tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()


def test_snapshot_rejects_nested_mount_identity(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.workspace as workspace_module

    source = tmp_path / "source"
    (source / "nested").mkdir(parents=True)
    identifiers = iter((10, 11))
    monkeypatch.setattr(workspace_module, "_descriptor_mount_id", lambda _descriptor: next(identifiers))
    with pytest.raises(WorkspaceBoundaryError, match="nested mount"):
        snapshot_workspace(source, tmp_path / "snapshot")
    assert not (tmp_path / "snapshot").exists()
