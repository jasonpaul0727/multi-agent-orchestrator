"""Final visible trees are the only source of removal authority."""
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import shutil

import pytest

import orchestrator.isolation as isolation
from orchestrator.isolation import workspace as workspace_module


def export(lower, view, candidate, **bounds):
    exporter = getattr(isolation, "export_workspace_view_diff", None)
    assert exporter is not None, "final-view exporter is required"
    return exporter(lower, view, candidate, **bounds)


def roots(tmp_path):
    lower = tmp_path / "lower"
    lower.mkdir()
    (lower / "tree").mkdir()
    (lower / "tree" / "file").write_bytes(b"old")
    (lower / "link").symlink_to("tree/file")
    view = tmp_path / "view"
    shutil.copytree(lower, view, symlinks=True)
    return lower, view, tmp_path / "candidate"


def test_unchanged_view_is_empty_and_hash_is_domain_separated(tmp_path):
    lower, view, candidate = roots(tmp_path)
    diff = export(lower, view, candidate)
    assert diff.schema_version == 2
    assert diff.entries == () and diff.total_bytes == 0
    assert list(candidate.iterdir()) == []
    assert diff.manifest_hash == "sha256:" + hashlib.sha256(b"maestro.workspace-diff.v2\0[]").hexdigest()
    isolation.validate_overlay_candidate(lower, diff)


@pytest.mark.parametrize("kind", ["file", "directory", "symlink"])
def test_remove_is_explicit_payload_free_and_revalidated(tmp_path, kind):
    lower, view, candidate = roots(tmp_path)
    path = {"file": "tree/file", "directory": "tree", "symlink": "link"}[kind]
    if kind == "directory":
        shutil.rmtree(view / path)
    else:
        (view / path).unlink()
    diff = export(lower, view, candidate)
    assert [e.path for e in diff.entries] == (["tree", "tree/file"] if kind == "directory" else [path])
    for entry in diff.entries:
        assert entry.operation == "delete" and entry.baseline_digest is not None
        assert entry.size == 0 and entry.mode == 0
        assert entry.digest == "sha256:" + hashlib.sha256(b"").hexdigest()
        assert not (candidate / entry.path).exists() and not (candidate / entry.path).is_symlink()
    assert diff.total_bytes == 0
    assert (lower / "tree/file").read_bytes() == b"old"
    isolation.validate_overlay_candidate(lower, diff)


@pytest.mark.parametrize("kind", ["file", "directory", "symlink"])
def test_rename_and_rename_overwrite_have_exact_operations(tmp_path, kind):
    lower, view, candidate = roots(tmp_path)
    source = {"file": "tree/file", "directory": "tree", "symlink": "link"}[kind]
    (view / source).rename(view / "renamed")
    diff = export(lower, view, candidate)
    want = {"file": [("renamed", "add"), ("tree/file", "delete")],
            "directory": [("renamed", "add"), ("renamed/file", "add"), ("tree", "delete"), ("tree/file", "delete")],
            "symlink": [("link", "delete"), ("renamed", "add")]}[kind]
    assert [(e.path, e.operation) for e in diff.entries] == want
    assert diff.total_bytes == (9 if kind == "symlink" else 3)
    isolation.validate_overlay_candidate(lower, diff)
    assert export(lower, view, tmp_path / "second").manifest_hash == diff.manifest_hash


@pytest.mark.parametrize("kind", ["file", "symlink"])
def test_compatible_rename_overwrite_modifies_destination(tmp_path, kind):
    lower, view, candidate = roots(tmp_path)
    for root in (lower, view):
        if kind == "file":
            (root / "destination").write_bytes(b"previous")
        else:
            (root / "destination").symlink_to("missing")
    source = "tree/file" if kind == "file" else "link"
    (view / source).replace(view / "destination")
    diff = export(lower, view, candidate)
    assert [(e.path, e.operation) for e in diff.entries] == sorted([("destination", "modify"), (source, "delete")])
    isolation.validate_overlay_candidate(lower, diff)


def test_directory_preimage_conflicts_on_extra_live_descendant(tmp_path):
    lower, view, candidate = roots(tmp_path)
    live = tmp_path / "live"
    shutil.copytree(lower, live, symlinks=True)
    shutil.rmtree(view / "tree")
    diff = export(lower, view, candidate)
    assert not isolation.check_workspace_publish_conflicts(lower, live, diff).is_conflicted
    (live / "tree/extra").write_bytes(b"concurrent")
    report = isolation.check_workspace_publish_conflicts(lower, live, diff)
    assert [(c.path, c.reason) for c in report.conflicts] == [("tree", "baseline_changed")]


@pytest.mark.parametrize("forgery", ["baseline", "payload", "omitted-child", "mode", "digest", "schema", "boolean-schema", "entry-type"])
def test_constructed_tombstones_fail_independent_validation(tmp_path, forgery):
    lower, view, candidate = roots(tmp_path)
    shutil.rmtree(view / "tree")
    diff = export(lower, view, candidate)
    entries = list(diff.entries)
    if forgery == "baseline": entries[0] = replace(entries[0], baseline_digest="sha256:" + "0" * 64)
    elif forgery == "payload": (candidate / "tree").mkdir()
    elif forgery == "omitted-child": entries.pop()
    elif forgery == "mode": entries[0] = replace(entries[0], mode=0o700)
    elif forgery == "digest": entries[0] = replace(entries[0], digest="sha256:" + "0" * 64)
    elif forgery == "schema": diff = replace(diff, schema_version=3)
    elif forgery == "boolean-schema": diff = replace(diff, schema_version=True)
    elif forgery == "entry-type": entries[0] = object()
    diff = replace(diff, entries=tuple(entries))
    if forgery not in ("entry-type", "schema", "boolean-schema"):
        diff = replace(diff, manifest_hash=workspace_module._workspace_diff_hash(diff.entries, schema_version=2))
    with pytest.raises(isolation.WorkspaceBoundaryError):
        isolation.validate_overlay_candidate(lower, diff)


@pytest.mark.parametrize("name", [".git", ".maestro", "bad\\name", "bad\nname", "bad\x7fname"])
def test_unsafe_names_fail_closed(tmp_path, name):
    lower, view, candidate = roots(tmp_path)
    (view / name).write_bytes(b"bad")
    with pytest.raises(isolation.WorkspaceBoundaryError): export(lower, view, candidate)
    assert not candidate.exists()


@pytest.mark.parametrize("attack", ["case", "hardlink", "fifo", "absolute-link", "escaping-link", "backslash-link", "xattr", "type"])
def test_unsafe_visible_trees_fail_closed(tmp_path, attack):
    lower, view, candidate = roots(tmp_path)
    if attack == "case": (view / "LINK").write_bytes(b"collision")
    elif attack == "hardlink": os.link(view / "tree/file", view / "hard")
    elif attack == "fifo": os.mkfifo(view / "fifo")
    elif attack == "xattr": os.setxattr(view / "tree/file", "user.test", b"bad")
    elif attack == "type":
        (view / "tree/file").unlink()
        (view / "tree/file").mkdir()
    else: (view / "escape").symlink_to({"absolute-link": "/etc/passwd", "escaping-link": "../outside", "backslash-link": "..\\outside"}[attack])
    with pytest.raises(isolation.WorkspaceBoundaryError): export(lower, view, candidate)
    assert not candidate.exists()


@pytest.mark.parametrize("bounds", [{"max_entries": 2}, {"max_bytes": 2}, {"max_depth": 1}, {"max_entries": True}, {"max_bytes": -1}, {"max_depth": 0}])
def test_inventory_bounds_include_unchanged_baseline(tmp_path, bounds):
    lower, view, candidate = roots(tmp_path)
    with pytest.raises(isolation.WorkspaceBoundaryError): export(lower, view, candidate, **bounds)
    assert not candidate.exists()


@pytest.mark.parametrize("path", ["../outside", "/absolute", "tree/../file", "tree\\file", "bad\nname", ".git/config", "TREE/file"])
def test_forged_paths_rejected_before_payload_admission(tmp_path, path):
    lower, view, candidate = roots(tmp_path)
    (view / "tree/file").unlink()
    diff = export(lower, view, candidate)
    entry = replace(diff.entries[0], path=path)
    diff = replace(diff, entries=(entry,), manifest_hash=workspace_module._workspace_diff_hash([entry], schema_version=2))
    with pytest.raises(isolation.WorkspaceBoundaryError): isolation.validate_overlay_candidate(lower, diff)


@pytest.mark.parametrize("field,value", [("total_bytes", True), ("entries", []), ("candidate_root", None), ("manifest_hash", None)])
def test_malformed_models_fail_with_boundary_error(tmp_path, field, value):
    lower, view, candidate = roots(tmp_path)
    diff = export(lower, view, candidate)
    with pytest.raises(isolation.WorkspaceBoundaryError):
        isolation.validate_overlay_candidate(lower, replace(diff, **{field: value}))


def test_directory_mode_change_exports_metadata_without_unchanged_child_bytes(tmp_path):
    lower, view, candidate = roots(tmp_path)
    (view / "tree").chmod(0o700)
    diff = export(lower, view, candidate)
    assert [(e.path, e.operation, e.mode) for e in diff.entries] == [("tree", "modify", 0o700)]
    assert diff.total_bytes == 0 and not (candidate / "tree/file").exists()
    isolation.validate_overlay_candidate(lower, diff)


@pytest.mark.parametrize("attack", ["mount", "root-swap", "file-mutation", "directory-mutation"])
def test_inventory_rejects_mount_and_concurrent_identity_changes(tmp_path, monkeypatch, attack):
    from orchestrator.isolation import workspace_view as module
    lower, view, candidate = roots(tmp_path)
    if attack == "mount":
        real = module._descriptor_mount_id
        def mount(fd):
            return real(fd) + (os.readlink(f"/proc/self/fd/{fd}").endswith("/tree/file"))
        monkeypatch.setattr(module, "_descriptor_mount_id", mount)
    elif attack == "root-swap":
        real = module.os.open
        def open_root(path, flags, *args, **kwargs):
            if path == view:
                view.rename(tmp_path / "old-view")
                view.mkdir()
            return real(path, flags, *args, **kwargs)
        monkeypatch.setattr(module.os, "open", open_root)
    else:
        real = module.os.read
        fired = False
        def mutate(fd, length):
            nonlocal fired
            result = real(fd, length)
            if not fired and os.readlink(f"/proc/self/fd/{fd}").endswith("/view/tree/file"):
                fired = True
                if attack == "file-mutation": (view / "tree/file").write_bytes(b"changed")
                else: (view / "tree/extra").write_bytes(b"extra")
            return result
        monkeypatch.setattr(module.os, "read", mutate)
    with pytest.raises(isolation.WorkspaceBoundaryError): export(lower, view, candidate)
    assert not candidate.exists()


def test_legacy_empty_manifest_keeps_v1_hash(tmp_path):
    lower = tmp_path / "lower"
    upper = tmp_path / "upper"
    lower.mkdir()
    upper.mkdir()
    diff = isolation.export_overlay_diff(lower, upper, tmp_path / "candidate")
    assert diff.schema_version == 1 and diff.entries == ()
    assert diff.manifest_hash == "sha256:" + hashlib.sha256(b"[]").hexdigest()
    for version in (0, True, 3):
        with pytest.raises(isolation.WorkspaceBoundaryError):
            isolation.validate_overlay_candidate(lower, replace(diff, schema_version=version))


def test_mutation_of_previously_scanned_subtree_is_rejected(tmp_path, monkeypatch):
    from orchestrator.isolation import workspace_view as module
    lower, view, candidate = roots(tmp_path)
    (view / "z-last").write_bytes(b"last")
    real = module.os.read
    fired = False
    def mutate(fd, length):
        nonlocal fired
        result = real(fd, length)
        if not fired and os.readlink(f"/proc/self/fd/{fd}").endswith("/view/z-last"):
            fired = True
            (view / "tree/file").write_bytes(b"changed")
        return result
    monkeypatch.setattr(module.os, "read", mutate)
    with pytest.raises(isolation.WorkspaceBoundaryError): export(lower, view, candidate)
    assert fired and not candidate.exists()


def test_baseline_root_swap_between_inventories_is_rejected(tmp_path, monkeypatch):
    from orchestrator.isolation import workspace_view as module
    lower, view, candidate = roots(tmp_path)
    real = module.os.read
    fired = False
    def swap(fd, length):
        nonlocal fired
        result = real(fd, length)
        if not fired and os.readlink(f"/proc/self/fd/{fd}").endswith("/view/tree/file"):
            fired = True
            lower.rename(tmp_path / "old-lower")
            shutil.copytree(tmp_path / "old-lower", lower, symlinks=True)
        return result
    monkeypatch.setattr(module.os, "read", swap)
    with pytest.raises(isolation.WorkspaceBoundaryError): export(lower, view, candidate)
    assert fired and not candidate.exists()


def test_directory_preimage_uses_snapshot_normalization_not_live_mode_authority(tmp_path):
    from orchestrator.isolation.overlay_launcher import _enable_overlay_owner_writes
    live = tmp_path / "live"
    live.mkdir(mode=0o700)
    (live / "private").mkdir(mode=0o700)
    (live / "private/data").write_bytes(b"private data")
    (live / "private/data").chmod(0o600)
    (live / "private/executable").write_bytes(b"executable")
    (live / "private/executable").chmod(0o700)
    lower = tmp_path / "lower"
    isolation.snapshot_workspace(live, lower)
    _enable_overlay_owner_writes(lower)
    view = tmp_path / "view"
    view.mkdir()
    diff = export(lower, view, tmp_path / "candidate")
    assert not isolation.check_workspace_publish_conflicts(lower, live, diff).is_conflicted
    assert (live / "private").stat().st_mode & 0o777 == 0o700
    assert (live / "private/data").stat().st_mode & 0o777 == 0o600
    assert (live / "private/executable").stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("unchanged", [False, True])
@pytest.mark.parametrize("target", ["b/../outside", "b/.git/config", "b/.maestro/control"])
def test_chained_symlink_escape_or_protected_target_rejected(tmp_path, unchanged, target):
    lower = tmp_path / "lower"
    view = tmp_path / "view"
    lower.mkdir()
    view.mkdir()
    (tmp_path / "outside").write_bytes(b"outside sentinel")
    for root in ([lower, view] if unchanged else [view]):
        (root / "a").mkdir()
        (root / "a/b").symlink_to("..")
        (root / "a/c").symlink_to(target)
    if target == "b/../outside":
        assert (view / "a/c").resolve() == tmp_path / "outside"
    with pytest.raises(isolation.WorkspaceBoundaryError):
        export(lower, view, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_independent_validation_checks_changed_link_against_unchanged_baseline_links(tmp_path):
    lower = tmp_path / "lower"
    lower.mkdir()
    (lower / "a").mkdir()
    (lower / "a/b").symlink_to("..")
    (lower / "a/c").symlink_to("safe")
    candidate = tmp_path / "candidate"
    candidate.mkdir(mode=0o700)
    (candidate / "a").mkdir(mode=0o700)
    (candidate / "a/c").symlink_to("b/../outside")
    entry = isolation.WorkspaceDiffEntry("a/c", "modify", "symlink", 0o777, 12,
        "sha256:" + hashlib.sha256(b"b/../outside").hexdigest(),
        "sha256:" + hashlib.sha256(b"safe").hexdigest())
    diff = isolation.WorkspaceDiff(candidate, (entry,), 12,
        workspace_module._workspace_diff_hash([entry], schema_version=2), 2)
    with pytest.raises(isolation.WorkspaceBoundaryError):
        isolation.validate_overlay_candidate(lower, diff)


def test_independent_validation_checks_unchanged_link_after_intermediate_link_change(tmp_path):
    lower = tmp_path / "lower"
    lower.mkdir()
    (lower / "a").mkdir()
    (lower / "a/safe").mkdir()
    (lower / "a/b").symlink_to("safe")
    (lower / "a/c").symlink_to("b/../outside")
    candidate = tmp_path / "candidate"
    candidate.mkdir(mode=0o700)
    (candidate / "a").mkdir(mode=0o700)
    (candidate / "a/b").symlink_to("..")
    entry = isolation.WorkspaceDiffEntry("a/b", "modify", "symlink", 0o777, 2,
        "sha256:" + hashlib.sha256(b"..").hexdigest(),
        "sha256:" + hashlib.sha256(b"safe").hexdigest())
    diff = isolation.WorkspaceDiff(candidate, (entry,), 2,
        workspace_module._workspace_diff_hash([entry], schema_version=2), 2)
    with pytest.raises(isolation.WorkspaceBoundaryError):
        isolation.validate_overlay_candidate(lower, diff)


@pytest.mark.parametrize("cyclic", [False, True])
def test_safe_symlink_chains_pass_and_cycles_are_bounded(tmp_path, cyclic):
    lower = tmp_path / "lower"
    view = tmp_path / "view"
    lower.mkdir()
    view.mkdir()
    (view / "target").write_bytes(b"safe")
    (view / "first").symlink_to("second")
    (view / "second").symlink_to("first" if cyclic else "target")
    if cyclic:
        with pytest.raises(isolation.WorkspaceBoundaryError):
            export(lower, view, tmp_path / "candidate")
    else:
        diff = export(lower, view, tmp_path / "candidate")
        isolation.validate_overlay_candidate(lower, diff)
        assert (diff.candidate_root / "first").read_bytes() == b"safe"


@pytest.mark.parametrize("mask", [0o022, 0o000, 0o077])
@pytest.mark.parametrize("kind", ["file", "symlink"])
def test_changed_payload_under_two_unchanged_ancestors_has_private_scaffolds(tmp_path, mask, kind):
    lower = tmp_path / "lower"
    (lower / "a/b").mkdir(parents=True)
    if kind == "file":
        (lower / "a/b/changed").write_bytes(b"old")
    else:
        (lower / "a/b/changed").symlink_to("old")
    view = tmp_path / "view"
    shutil.copytree(lower, view, symlinks=True)
    if kind == "file":
        (view / "a/b/changed").write_bytes(b"new")
    else:
        (view / "a/b/changed").unlink()
        (view / "a/b/changed").symlink_to("new")
    previous_mask = os.umask(mask)
    try:
        diff = export(lower, view, tmp_path / "candidate")
    finally:
        os.umask(previous_mask)
    assert [(e.path, e.operation) for e in diff.entries] == [("a/b/changed", "modify")]
    assert diff.total_bytes == 3
    assert (diff.candidate_root / "a").stat().st_mode & 0o777 == 0o700
    assert (diff.candidate_root / "a/b").stat().st_mode & 0o777 == 0o700
    isolation.validate_overlay_candidate(lower, diff)
    if kind == "file":
        assert (lower / "a/b/changed").read_bytes() == b"old"
        assert (diff.candidate_root / "a/b/changed").read_bytes() == b"new"
    else:
        assert os.readlink(lower / "a/b/changed") == "old"
        assert os.readlink(diff.candidate_root / "a/b/changed") == "new"
