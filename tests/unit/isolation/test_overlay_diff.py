from __future__ import annotations

import hashlib
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.isolation import (
    WorkspaceBoundaryError,
    export_overlay_diff,
    validate_overlay_candidate,
)
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
    validate_overlay_candidate(lower, result)

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
    changed = next(item for item in result.entries if item.path == "changed.txt")
    added = next(item for item in result.entries if item.path == "new-dir/new.txt")
    assert changed.baseline_digest == "sha256:" + hashlib.sha256(b"before\n").hexdigest()
    assert added.baseline_digest is None
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
    (lower / "existing-dir" / "relative-link").symlink_to("old.txt")
    (upper / "existing-dir").mkdir()
    (upper / "existing-dir" / "file.txt").write_text("new", encoding="utf-8")
    (upper / "existing-dir" / "relative-link").symlink_to("../changed.txt")

    result = export_overlay_diff(lower, upper, candidate)

    directory = next(item for item in result.entries if item.path == "existing-dir")
    assert directory.operation == "modify"
    assert directory.kind == "directory"
    assert os.readlink(candidate / "existing-dir" / "relative-link") == "../changed.txt"
    link = next(item for item in result.entries if item.path == "existing-dir/relative-link")
    assert link.operation == "modify"
    assert link.baseline_digest == "sha256:" + hashlib.sha256(b"old.txt").hexdigest()


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


def test_lower_baseline_digest_rejects_stale_stat_sample_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower = tmp_path / "lower"
    lower.mkdir()
    source = lower / "changed.txt"
    source.write_text("base", encoding="utf-8")
    lower_fd = os.open(lower, os.O_RDONLY | os.O_DIRECTORY)
    original_stat = os.stat

    def stale_stat(path, *args, **kwargs):
        info = original_stat(path, *args, **kwargs)
        if path == "changed.txt" and kwargs.get("dir_fd") is not None:
            return SimpleNamespace(
                st_mode=info.st_mode,
                st_dev=info.st_dev,
                st_ino=info.st_ino,
                st_size=info.st_size,
                st_mtime_ns=info.st_mtime_ns - 1,
                st_ctime_ns=info.st_ctime_ns - 1,
            )
        return info

    monkeypatch.setattr(workspace_module.os, "stat", stale_stat)
    try:
        with pytest.raises(WorkspaceBoundaryError, match="changed during hashing"):
            workspace_module._lower_content_digest(
                lower_fd, ("changed.txt",), "file", 1024
            )
    finally:
        os.close(lower_fd)


def test_lower_baseline_digest_rejects_missing_or_unsafe_parent_paths(
    tmp_path: Path,
) -> None:
    lower = tmp_path / "lower"
    lower.mkdir()
    (lower / "regular-file").write_text("data", encoding="utf-8")
    (lower / "directory-link").symlink_to(tmp_path, target_is_directory=True)
    lower_fd = os.open(lower, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(WorkspaceBoundaryError, match="path changed during hashing"):
            workspace_module._lower_content_digest(
                lower_fd, ("missing-parent", "leaf"), "file", 64
            )
        with pytest.raises(WorkspaceBoundaryError, match="safe directory"):
            workspace_module._lower_content_digest(
                lower_fd, ("directory-link", "leaf"), "file", 64
            )
        with pytest.raises(WorkspaceBoundaryError, match="safe directory"):
            workspace_module._lower_content_digest(
                lower_fd, ("regular-file", "leaf"), "file", 64
            )
        with pytest.raises(WorkspaceBoundaryError, match="disappeared during hashing"):
            workspace_module._lower_content_digest(
                lower_fd, ("missing-leaf",), "file", 64
            )
    finally:
        os.close(lower_fd)


def test_lower_baseline_digest_bounds_and_validates_symlink_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower = tmp_path / "lower"
    lower.mkdir()
    (lower / "file").write_text("data", encoding="utf-8")
    (lower / "link").symlink_to("long-target")
    lower_fd = os.open(lower, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(WorkspaceBoundaryError, match="changed type"):
            workspace_module._lower_content_digest(lower_fd, ("file",), "symlink", 64)
        with pytest.raises(WorkspaceBoundaryError, match="file exceeds the byte limit"):
            workspace_module._lower_content_digest(lower_fd, ("link",), "file", 64)
        with pytest.raises(WorkspaceBoundaryError, match="symlink exceeds the byte limit"):
            workspace_module._lower_content_digest(lower_fd, ("link",), "symlink", 2)

        def failed_readlink(_path, *, dir_fd):
            raise OSError("link unavailable")

        monkeypatch.setattr(workspace_module.os, "readlink", failed_readlink)
        with pytest.raises(WorkspaceBoundaryError, match="symlink changed during hashing"):
            workspace_module._lower_content_digest(lower_fd, ("link",), "symlink", 64)
    finally:
        os.close(lower_fd)


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


def test_export_overlay_diff_rejects_file_metadata_change_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    real_open = os.open
    real_fstat = os.fstat
    file_descriptor: int | None = None
    file_stat_count = 0

    def track_upper_file(path, flags, *args, **kwargs):
        nonlocal file_descriptor
        descriptor = real_open(path, flags, *args, **kwargs)
        if path == "changed.txt" and kwargs.get("dir_fd") is not None:
            file_descriptor = descriptor
        return descriptor

    def mutate_metadata(descriptor: int):
        nonlocal file_stat_count
        info = real_fstat(descriptor)
        if descriptor == file_descriptor:
            file_stat_count += 1
            if file_stat_count == 2:
                return SimpleNamespace(
                    st_dev=info.st_dev,
                    st_ino=info.st_ino,
                    st_size=info.st_size,
                    st_mtime_ns=info.st_mtime_ns + 1,
                    st_ctime_ns=info.st_ctime_ns,
                )
        return info

    monkeypatch.setattr(workspace_module.os, "open", track_upper_file)
    monkeypatch.setattr(workspace_module.os, "fstat", mutate_metadata)
    with pytest.raises(WorkspaceBoundaryError, match="file changed while reading"):
        export_overlay_diff(lower, upper, candidate)
    assert file_stat_count >= 2
    assert not candidate.exists()


def test_validate_overlay_candidate_rejects_tampered_bytes_extras_and_stale_lower(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    (candidate / "changed.txt").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(WorkspaceBoundaryError, match="bytes do not match"):
        validate_overlay_candidate(lower, diff)
    assert candidate.exists()

    (candidate / "changed.txt").write_text("after\n", encoding="utf-8")
    (candidate / "undeclared.txt").write_text("unexpected", encoding="utf-8")
    with pytest.raises(WorkspaceBoundaryError, match="undeclared"):
        validate_overlay_candidate(lower, diff)
    (candidate / "undeclared.txt").unlink()

    (lower / "changed.txt").write_text("concurrent\n", encoding="utf-8")
    with pytest.raises(WorkspaceBoundaryError, match="baseline digest is stale"):
        validate_overlay_candidate(lower, diff)


def test_validate_overlay_candidate_rejects_manifest_tampering_and_extra_boundaries(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    forged = replace(diff, manifest_hash="sha256:" + "0" * 64)
    with pytest.raises(WorkspaceBoundaryError, match="digest or byte count"):
        validate_overlay_candidate(lower, forged)

    forged_entry = replace(diff.entries[0], path="../escape")
    forged = replace(diff, entries=(forged_entry, *diff.entries[1:]))
    with pytest.raises(WorkspaceBoundaryError, match="invalid path or value"):
        validate_overlay_candidate(lower, forged)

    with pytest.raises(WorkspaceBoundaryError, match="exceeds limits"):
        validate_overlay_candidate(lower, diff, max_entries=1)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("operation", "replace", "unsupported operation"),
        ("operation", "add-with-baseline", "added candidate entry"),
        ("kind", "socket", "unsupported entry kind"),
        ("baseline_digest", None, "requires a lower baseline"),
    ],
)
def test_validate_overlay_candidate_rejects_invalid_entry_contracts(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    original = next(entry for entry in diff.entries if entry.path == "changed.txt")
    if field == "operation" and value == "add-with-baseline":
        modified = replace(original, operation="add")
    else:
        modified = replace(original, **{field: value})
    forged = replace(diff, entries=(modified, *diff.entries[1:]))

    with pytest.raises(WorkspaceBoundaryError, match=message):
        validate_overlay_candidate(lower, forged)
    assert (candidate / "changed.txt").read_text(encoding="utf-8") == "after\n"


def test_validate_overlay_candidate_rejects_invalid_inputs_and_bounds(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)

    with pytest.raises(WorkspaceBoundaryError, match="invalid type"):
        validate_overlay_candidate(lower, object())  # type: ignore[arg-type]
    with pytest.raises(WorkspaceBoundaryError, match="max_entries"):
        validate_overlay_candidate(lower, diff, max_entries=True)
    with pytest.raises(WorkspaceBoundaryError, match="max_bytes"):
        validate_overlay_candidate(lower, diff, max_bytes=-1)
    with pytest.raises(WorkspaceBoundaryError, match="max_depth"):
        validate_overlay_candidate(lower, diff, max_depth=0)
    with pytest.raises(WorkspaceBoundaryError, match="exceeds limits or is malformed"):
        validate_overlay_candidate(lower, replace(diff, total_bytes=-1))
    with pytest.raises(WorkspaceBoundaryError, match="exceeds limits or is malformed"):
        validate_overlay_candidate(lower, replace(diff, manifest_hash="not-a-digest"))


def test_validate_overlay_candidate_checks_kinds_inventory_and_symlink_targets(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (upper / "new-link").symlink_to("new-dir/new.txt")
    diff = export_overlay_diff(lower, upper, candidate)

    directory = next(entry for entry in diff.entries if entry.path == "new-dir")
    (candidate / "new-dir" / "new.txt").unlink()
    (candidate / "new-dir").rmdir()
    (candidate / "new-dir").write_text("not a directory", encoding="utf-8")
    with pytest.raises(WorkspaceBoundaryError, match="file kind or link count"):
        validate_overlay_candidate(lower, diff)
    (candidate / "new-dir").unlink()
    (candidate / "new-dir").mkdir(mode=0o700)
    (candidate / "new-dir" / "new.txt").write_text("new\n", encoding="utf-8")
    (candidate / "new-dir" / "new.txt").chmod(0o600)

    link_path = candidate / "new-link"
    link_path.unlink()
    link_path.symlink_to("new-dir/old.txt")
    with pytest.raises(WorkspaceBoundaryError, match="symlink target differs"):
        validate_overlay_candidate(lower, diff)
    link_path.unlink()
    link_path.symlink_to("new-dir/new.txt")

    (candidate / "changed.txt").unlink()
    with pytest.raises(WorkspaceBoundaryError, match="inventory"):
        validate_overlay_candidate(lower, diff)
    assert directory.path == "new-dir"


def test_validate_overlay_candidate_rejects_candidate_xattrs_and_special_files(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    setter = getattr(os, "setxattr", None)
    remover = getattr(os, "removexattr", None)
    if setter is None or remover is None:
        pytest.skip("extended attributes are unavailable")
    try:
        setter(candidate / "changed.txt", "user.maestro.test", b"x")
    except OSError as exc:
        pytest.skip(f"filesystem does not allow test xattrs: {exc}")
    with pytest.raises(WorkspaceBoundaryError, match="extended attributes"):
        validate_overlay_candidate(lower, diff)
    remover(candidate / "changed.txt", "user.maestro.test")

    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFO boundary check requires POSIX")
    (candidate / "new-dir" / "new.txt").unlink()
    os.mkfifo(candidate / "new-dir" / "new.txt")
    with pytest.raises(WorkspaceBoundaryError, match="special file"):
        validate_overlay_candidate(lower, diff)


@pytest.mark.parametrize(
    ("relative_path", "mode", "message"),
    [
        (".", 0o755, "root permissions are not private"),
        ("changed.txt", 0o644, "file permissions are not private"),
        ("new-dir", 0o755, "directory permissions are not private"),
    ],
)
def test_validate_overlay_candidate_rejects_public_candidate_permissions(
    tmp_path: Path, relative_path: str, mode: int, message: str
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    (candidate / relative_path).chmod(mode)

    with pytest.raises(WorkspaceBoundaryError, match=message):
        validate_overlay_candidate(lower, diff)


def test_validate_overlay_candidate_rejects_directory_manifest_tampering(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    directory = next(entry for entry in diff.entries if entry.path == "new-dir")

    with pytest.raises(WorkspaceBoundaryError, match="directory metadata"):
        validate_overlay_candidate(lower, replace(diff, entries=(
            replace(directory, digest="sha256:" + "0" * 64),
            *(entry for entry in diff.entries if entry.path != "new-dir"),
        )))

    with pytest.raises(WorkspaceBoundaryError, match="directory metadata"):
        validate_overlay_candidate(lower, replace(diff, entries=(
            replace(directory, size=1),
            *(entry for entry in diff.entries if entry.path != "new-dir"),
        )))


def test_validate_overlay_candidate_rejects_symlinked_roots(tmp_path: Path) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    lower_link = tmp_path / "lower-link"
    candidate_link = tmp_path / "candidate-link"
    lower_link.symlink_to(lower, target_is_directory=True)
    candidate_link.symlink_to(candidate, target_is_directory=True)

    with pytest.raises(WorkspaceBoundaryError, match="lower root"):
        validate_overlay_candidate(lower_link, diff)
    with pytest.raises(WorkspaceBoundaryError, match="candidate root"):
        validate_overlay_candidate(lower, replace(diff, candidate_root=candidate_link))


def test_validate_overlay_candidate_rejects_overlapping_roots_and_malformed_entries(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (lower / "new-dir").mkdir()
    diff = export_overlay_diff(lower, upper, candidate)
    nested_candidate = lower / "nested-candidate"
    nested_candidate.mkdir(mode=0o700)
    with pytest.raises(WorkspaceBoundaryError, match="overlaps"):
        validate_overlay_candidate(lower, replace(diff, candidate_root=nested_candidate))

    with pytest.raises(WorkspaceBoundaryError, match="invalid entry"):
        validate_overlay_candidate(lower, replace(diff, entries=("not-an-entry",)))  # type: ignore[arg-type]

    directory = next(entry for entry in diff.entries if entry.path == "new-dir")
    forged_directory = replace(directory, baseline_digest="sha256:" + "0" * 64)
    with pytest.raises(WorkspaceBoundaryError, match="baseline must be structural"):
        validate_overlay_candidate(
            lower,
            replace(
                diff,
                entries=(forged_directory, *(e for e in diff.entries if e.path != "new-dir")),
            ),
        )


def test_validate_overlay_candidate_closes_lower_root_if_candidate_open_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    real_open = os.open
    real_fstat = os.fstat
    lower_descriptors: list[int] = []

    def deny_candidate(path, flags, *args, **kwargs):
        if Path(path) == candidate:
            raise PermissionError("candidate unavailable")
        descriptor = real_open(path, flags, *args, **kwargs)
        if Path(path) == lower:
            lower_descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(workspace_module.os, "open", deny_candidate)
    with pytest.raises(WorkspaceBoundaryError, match="cannot be opened safely"):
        validate_overlay_candidate(lower, diff)
    assert len(lower_descriptors) == 1
    with pytest.raises(OSError):
        real_fstat(lower_descriptors[0])


def test_validate_overlay_candidate_enforces_read_limits_and_symlink_kind(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    (upper / "new-link").symlink_to("new-dir/new.txt")
    diff = export_overlay_diff(lower, upper, candidate)

    (candidate / "changed.txt").write_text("x" * (diff.total_bytes + 1), encoding="utf-8")
    with pytest.raises(WorkspaceBoundaryError, match="byte limit"):
        validate_overlay_candidate(lower, diff, max_bytes=diff.total_bytes)
    (candidate / "changed.txt").write_text("after\n", encoding="utf-8")

    link = next(entry for entry in diff.entries if entry.kind == "symlink")
    forged_link = replace(link, kind="file")
    forged_entries = tuple(forged_link if entry == link else entry for entry in diff.entries)
    with pytest.raises(WorkspaceBoundaryError, match="entry kind differs"):
        validate_overlay_candidate(
            lower,
            replace(
                diff,
                entries=forged_entries,
                manifest_hash=workspace_module._workspace_diff_hash(forged_entries),
            ),
        )

    forged_link = replace(link, size=link.size + 1)
    forged_entries = tuple(forged_link if entry == link else entry for entry in diff.entries)
    with pytest.raises(WorkspaceBoundaryError, match="symlink exceeds its manifest bounds"):
        validate_overlay_candidate(
            lower,
            replace(
                diff,
                entries=forged_entries,
                total_bytes=diff.total_bytes + 1,
                manifest_hash=workspace_module._workspace_diff_hash(forged_entries),
            ),
        )


def test_validate_overlay_candidate_rejects_lower_type_and_hashing_changes(
    tmp_path: Path,
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)

    (lower / "changed.txt").unlink()
    (lower / "changed.txt").mkdir()
    with pytest.raises(WorkspaceBoundaryError, match="operation does not match"):
        validate_overlay_candidate(lower, diff)
    (lower / "changed.txt").rmdir()
    (lower / "changed.txt").write_text("before\n", encoding="utf-8")
    os.link(lower / "changed.txt", lower / "extra-link.txt")
    with pytest.raises(WorkspaceBoundaryError, match="baseline file changed during hashing"):
        validate_overlay_candidate(lower, diff)


def test_validate_overlay_candidate_reports_candidate_listing_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lower, upper, candidate = _roots(tmp_path)
    diff = export_overlay_diff(lower, upper, candidate)
    real_listdir = os.listdir
    candidate_info = candidate.stat()

    def denied(path):
        if isinstance(path, int):
            try:
                info = os.fstat(path)
            except OSError:
                pass
            else:
                if (info.st_dev, info.st_ino) == (candidate_info.st_dev, candidate_info.st_ino):
                    raise PermissionError("private directory details")
        return real_listdir(path)

    monkeypatch.setattr(workspace_module.os, "listdir", denied)
    with pytest.raises(WorkspaceBoundaryError, match="directory cannot be read safely"):
        validate_overlay_candidate(lower, diff)

