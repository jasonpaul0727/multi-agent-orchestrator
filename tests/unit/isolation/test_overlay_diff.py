from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.isolation import WorkspaceBoundaryError, export_overlay_diff
from orchestrator.isolation import workspace as workspace_module


def _roots(tmp_path: Path) -> tuple[Path, Path, Path]:
    lower = tmp_path / "lower"
    upper = tmp_path / "upper"
    candidate = tmp_path / "candidate"
    lower.mkdir()
    upper.mkdir()
    (lower / "kept.txt").write_text("baseline\n", encoding="utf-8")
    (lower / "changed.txt").write_text("before\n", encoding="utf-8")
    (upper / "changed.txt").write_text("after\n", encoding="utf-8")
    (upper / "new-dir").mkdir()
    (upper / "new-dir" / "new.txt").write_text("new\n", encoding="utf-8")
    return lower, upper, candidate


def test_export_overlay_diff_creates_deterministic_private_candidate(tmp_path: Path) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (upper / "link").symlink_to("new-dir/new.txt")

    result = export_overlay_diff(lower, upper, candidate)

    assert (candidate / "changed.txt").read_text(encoding="utf-8") == "after\n"
    assert (candidate / "new-dir" / "new.txt").read_text(encoding="utf-8") == "new\n"
    assert os.readlink(candidate / "link") == "new-dir/new.txt"
    assert (lower / "changed.txt").read_text(encoding="utf-8") == "before\n"
    assert (lower / "kept.txt").read_text(encoding="utf-8") == "baseline\n"
    assert [(item.path, item.operation, item.kind) for item in result.entries] == [
        ("changed.txt", "modify", "file"),
        ("link", "add", "symlink"),
        ("new-dir", "add", "directory"),
        ("new-dir/new.txt", "add", "file"),
    ]
    assert result.total_bytes == len(b"after\nnew\n") + len(b"new-dir/new.txt")
    assert result.manifest_hash.startswith("sha256:")
    assert result.candidate_root == candidate

    second = export_overlay_diff(lower, upper, tmp_path / "candidate-2")
    assert second.manifest_hash == result.manifest_hash


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_entries": 2}, "entry limit"),
        ({"max_bytes": 2}, "byte limit"),
        ({"max_depth": 1}, "depth limit"),
        ({"max_entries": True}, "max_entries"),
        ({"max_bytes": -1}, "max_bytes"),
        ({"max_depth": 0}, "max_depth"),
    ],
)
def test_export_overlay_diff_enforces_bounds_and_cleans_candidate(
    tmp_path: Path, kwargs: dict[str, int], message: str
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    if "max_depth" in kwargs:
        (upper / "new-dir" / "deep").mkdir()
        (upper / "new-dir" / "deep" / "nested.txt").write_text("depth", encoding="utf-8")

    with pytest.raises(WorkspaceBoundaryError, match=message):
        export_overlay_diff(lower, upper, candidate, **kwargs)

    assert not candidate.exists()


@pytest.mark.parametrize("link", ("/etc/passwd", "../../outside", "../../../escape"))
def test_export_overlay_diff_rejects_absolute_or_escaping_symlinks(
    tmp_path: Path, link: str
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (upper / "escape").symlink_to(link)

    with pytest.raises(WorkspaceBoundaryError, match="symlink"):
        export_overlay_diff(lower, upper, candidate)

    assert not candidate.exists()


def test_export_overlay_diff_rejects_protected_paths_special_files_and_hardlinks(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (upper / ".git").mkdir()
    with pytest.raises(WorkspaceBoundaryError, match="protected"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()

    (upper / ".git").rmdir()
    if hasattr(os, "mkfifo"):
        os.mkfifo(upper / "fifo")
        with pytest.raises(WorkspaceBoundaryError, match="whiteout or special"):
            export_overlay_diff(lower, upper, candidate)
        assert not candidate.exists()
        (upper / "fifo").unlink()

    os.link(upper / "changed.txt", upper / "hardlink.txt")
    with pytest.raises(WorkspaceBoundaryError, match="hard-linked"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()


def test_export_overlay_diff_rejects_lower_symlink_traversal_and_type_changes(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (lower / "pivot").symlink_to(outside, target_is_directory=True)
    (upper / "pivot").mkdir()
    (upper / "pivot" / "payload").write_text("data", encoding="utf-8")

    with pytest.raises(WorkspaceBoundaryError, match="file type"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()
    lower_fd = os.open(lower, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(WorkspaceBoundaryError, match="lower symlink"):
            workspace_module._lower_entry_kind(lower_fd, ("pivot", "payload"))
    finally:
        os.close(lower_fd)

    (upper / "pivot" / "payload").unlink()
    (upper / "pivot").rmdir()
    (lower / "pivot").unlink()
    (lower / "typed").write_text("file", encoding="utf-8")
    (upper / "typed").mkdir()
    with pytest.raises(WorkspaceBoundaryError, match="file type"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()


def test_export_overlay_diff_rejects_xattrs_and_lower_non_directory_traversal(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (upper / "nested").mkdir()
    (upper / "nested" / "file").write_text("data", encoding="utf-8")
    setter = getattr(os, "setxattr", None)
    if setter is None:
        pytest.skip("extended attributes are unavailable")
    try:
        setter(upper / "nested", "user.maestro.test", b"x")
    except OSError as exc:
        pytest.skip(f"filesystem does not allow test xattrs: {exc}")
    with pytest.raises(WorkspaceBoundaryError, match="extended attributes"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()

    os.removexattr(upper / "nested", "user.maestro.test")
    (lower / "file-parent").write_text("not a directory", encoding="utf-8")
    (upper / "file-parent").mkdir()
    (upper / "file-parent" / "child").write_text("data", encoding="utf-8")
    with pytest.raises(WorkspaceBoundaryError, match="file type"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()
    lower_fd = os.open(lower, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(WorkspaceBoundaryError, match="non-directory lower entry"):
            workspace_module._lower_entry_kind(lower_fd, ("file-parent", "child"))
    finally:
        os.close(lower_fd)


def test_export_overlay_diff_rejects_overlap_existing_roots_and_missing_parent(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    occupied = upper / "occupied"
    occupied.mkdir()
    with pytest.raises(WorkspaceBoundaryError, match="must not already exist"):
        export_overlay_diff(lower, upper, occupied)
    with pytest.raises(WorkspaceBoundaryError, match="overlaps"):
        export_overlay_diff(lower, upper, lower / "candidate")
    with pytest.raises(WorkspaceBoundaryError, match="cannot be opened safely"):
        export_overlay_diff(tmp_path / "missing", upper, candidate)
    assert not candidate.exists()


def test_export_overlay_diff_rejects_invalid_root_types_and_cleans_on_mount_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    lower_file = tmp_path / "lower-file"
    lower_file.write_text("not a directory", encoding="utf-8")
    with pytest.raises(WorkspaceBoundaryError, match="lower root"):
        export_overlay_diff(lower_file, upper, candidate)

    upper_link = tmp_path / "upper-link"
    upper_link.symlink_to(upper, target_is_directory=True)
    with pytest.raises(WorkspaceBoundaryError, match="upper root"):
        export_overlay_diff(lower, upper_link, candidate)

    def unavailable(_descriptor: int) -> int:
        raise WorkspaceBoundaryError("mount id unavailable")

    monkeypatch.setattr(workspace_module, "_descriptor_mount_id", unavailable)
    with pytest.raises(WorkspaceBoundaryError, match="mount id unavailable"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()


def test_export_overlay_diff_cleans_candidate_when_upper_scan_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)

    def denied(_descriptor: int) -> list[str]:
        raise PermissionError("private path detail")

    monkeypatch.setattr(workspace_module.os, "listdir", denied)
    with pytest.raises(WorkspaceBoundaryError, match="upper directory cannot be read") as error:
        export_overlay_diff(lower, upper, candidate)
    assert "private path detail" not in str(error.value)
    assert not candidate.exists()


def test_export_overlay_diff_rejects_upper_entry_race_and_unreadable_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    original_stat = os.stat

    def disappeared(path, *args, **kwargs):
        if path == "changed.txt" and kwargs.get("dir_fd") is not None:
            raise FileNotFoundError("raced path")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(workspace_module.os, "stat", disappeared)
    with pytest.raises(WorkspaceBoundaryError, match="changed during export"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()

    monkeypatch.setattr(workspace_module.os, "stat", original_stat)

    def xattrs_unavailable(_descriptor: int) -> list[str]:
        raise OSError("metadata unavailable")

    monkeypatch.setattr(workspace_module.os, "listxattr", xattrs_unavailable)
    with pytest.raises(WorkspaceBoundaryError, match="metadata cannot be inspected"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()


def test_export_overlay_diff_handles_changed_directory_and_parent_relative_symlink(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (lower / "existing-dir").mkdir()
    (upper / "existing-dir").mkdir()
    (upper / "existing-dir" / "file.txt").write_text("new", encoding="utf-8")
    (upper / "existing-dir" / "relative-link").symlink_to("../changed.txt")

    result = export_overlay_diff(lower, upper, candidate)

    directory = next(item for item in result.entries if item.path == "existing-dir")
    assert directory.operation == "modify"
    assert directory.kind == "directory"
    assert os.readlink(candidate / "existing-dir" / "relative-link") == "../changed.txt"


def test_overlay_helpers_report_special_lower_entries_and_validate_mount_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower = tmp_path / "lower"
    lower.mkdir()
    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFO boundary check requires POSIX")
    os.mkfifo(lower / "special")
    lower_fd = os.open(lower, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert workspace_module._lower_entry_kind(lower_fd, ("special",)) == "special"
    finally:
        os.close(lower_fd)

    monkeypatch.setattr(
        Path,
        "read_text",
        lambda _path, **_kwargs: "pos:\t1\nmnt_id:\t42\n",
    )
    assert workspace_module._descriptor_mount_id(123) == 42
    monkeypatch.setattr(Path, "read_text", lambda _path, **_kwargs: "pos:\t1\n")
    with pytest.raises(WorkspaceBoundaryError, match="mount identity"):
        workspace_module._descriptor_mount_id(123)


def test_export_overlay_diff_closes_pinned_roots_on_open_and_identity_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    real_open = os.open
    real_fstat = os.fstat
    descriptors: list[int] = []

    def fail_upper_open(path, flags, *args, **kwargs):
        if Path(path) == upper:
            raise PermissionError("upper unavailable")
        descriptor = real_open(path, flags, *args, **kwargs)
        if Path(path) == lower:
            descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(workspace_module.os, "open", fail_upper_open)
    with pytest.raises(WorkspaceBoundaryError, match="roots cannot be opened safely"):
        export_overlay_diff(lower, upper, candidate)
    with pytest.raises(OSError):
        real_fstat(descriptors[0])

    monkeypatch.setattr(workspace_module.os, "open", real_open)
    descriptors.clear()

    def record_roots(path, flags, *args, **kwargs):
        descriptor = real_open(path, flags, *args, **kwargs)
        if Path(path) in (lower, upper):
            descriptors.append(descriptor)
        return descriptor

    def replace_first_identity(descriptor: int):
        info = real_fstat(descriptor)
        if descriptor == descriptors[0]:
            return SimpleNamespace(st_dev=info.st_dev, st_ino=info.st_ino + 1)
        return info

    monkeypatch.setattr(workspace_module.os, "open", record_roots)
    monkeypatch.setattr(workspace_module.os, "fstat", replace_first_identity)
    with pytest.raises(WorkspaceBoundaryError, match="roots changed while opening"):
        export_overlay_diff(lower, upper, candidate)
    assert len(descriptors) == 2
    for descriptor in descriptors:
        with pytest.raises(OSError):
            real_fstat(descriptor)
    assert not candidate.exists()


def test_export_overlay_diff_closes_roots_when_candidate_creation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    original_mkdir = Path.mkdir

    def denied_mkdir(path: Path, *args, **kwargs) -> None:
        if path == candidate:
            raise PermissionError("candidate unavailable")
        original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", denied_mkdir)
    with pytest.raises(PermissionError, match="candidate unavailable"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()


def test_export_overlay_diff_rejects_symlink_replacement_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (upper / "link").symlink_to("changed.txt")
    original_stat = os.stat
    link_stats = 0

    def race_link(path, *args, **kwargs):
        nonlocal link_stats
        info = original_stat(path, *args, **kwargs)
        if path == "link" and kwargs.get("dir_fd") is not None:
            link_stats += 1
            if link_stats == 2:
                return SimpleNamespace(
                    st_dev=info.st_dev, st_ino=info.st_ino + 1, st_mode=info.st_mode
                )
        return info

    monkeypatch.setattr(workspace_module.os, "stat", race_link)
    with pytest.raises(WorkspaceBoundaryError, match="symlink changed during export"):
        export_overlay_diff(lower, upper, candidate)
    assert not candidate.exists()

