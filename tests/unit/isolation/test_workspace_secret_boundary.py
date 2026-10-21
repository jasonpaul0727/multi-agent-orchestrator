"""Root secret configuration is never input or a publishable candidate path."""
import os

import pytest

from orchestrator.isolation import (
    WorkspaceBoundaryError, export_workspace_view_diff, snapshot_workspace,
)


@pytest.mark.parametrize("name", [".env", ".ENV"])
@pytest.mark.parametrize("kind", ["file", "directory", "symlink", "fifo"])
def test_root_secret_is_omitted_without_opening_or_following(tmp_path, name, kind):
    source = tmp_path / "source"
    source.mkdir()
    secret = source / name
    outside = tmp_path / "outside"
    outside.write_bytes(b"secret-sentinel")
    if kind == "file": secret.write_bytes(b"secret-sentinel")
    elif kind == "directory":
        secret.mkdir()
        (secret / "key").write_bytes(b"secret-sentinel")
    elif kind == "symlink": secret.symlink_to(outside)
    else: os.mkfifo(secret)
    (source / "safe").write_bytes(b"ok")
    destination = tmp_path / "snapshot"
    snapshot_workspace(source, destination)
    assert sorted(path.name for path in destination.iterdir()) == ["safe"]
    assert outside.read_bytes() == b"secret-sentinel"


@pytest.mark.parametrize("name", [".env", ".ENV"])
@pytest.mark.parametrize("kind", ["file", "directory", "symlink", "indirect-symlink"])
def test_candidate_cannot_target_root_secret_configuration(tmp_path, name, kind):
    lower, view = tmp_path / "lower", tmp_path / "view"
    lower.mkdir()
    view.mkdir()
    if kind == "file": (view / name).write_bytes(b"replace")
    elif kind == "directory": (view / name).mkdir()
    elif kind == "symlink": (view / name).symlink_to("safe")
    else: (view / "alias").symlink_to(name)
    with pytest.raises(WorkspaceBoundaryError):
        export_workspace_view_diff(lower, view, tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


@pytest.mark.parametrize("secret_name", [".env", ".ENV"])
@pytest.mark.parametrize("alias", ["config-copy", "nested/config-copy"])
def test_snapshot_rejects_hardlink_alias_of_protected_secret(tmp_path, secret_name, alias):
    source = tmp_path / "source"
    source.mkdir()
    secret = source / secret_name
    secret.write_bytes(b"hardlink-secret-sentinel")
    copy = source / alias
    copy.parent.mkdir(exist_ok=True)
    os.link(secret, copy)
    destination = tmp_path / "snapshot"
    with pytest.raises(WorkspaceBoundaryError, match="hardlink"):
        snapshot_workspace(source, destination)
    assert not destination.exists()
    assert secret.read_bytes() == copy.read_bytes() == b"hardlink-secret-sentinel"


@pytest.mark.parametrize("phase", ["open", "read"])
def test_snapshot_rejects_hardlink_added_after_path_inspection(tmp_path, monkeypatch, phase):
    source = tmp_path / "source"
    source.mkdir()
    copy = source / "config-copy"
    copy.write_bytes(b"hardlink-secret-sentinel")
    identity = (copy.stat().st_dev, copy.stat().st_ino)
    real_open, real_read = os.open, os.read
    linked = False
    def add_alias():
        nonlocal linked
        os.link(copy, source / ".env")
        linked = True
    def racing_open(path, flags, *args, **kwargs):
        if phase == "open" and not linked and path == "config-copy" and kwargs.get("dir_fd") is not None:
            add_alias()
        return real_open(path, flags, *args, **kwargs)
    def racing_read(fd, size):
        info = os.fstat(fd)
        if phase == "read" and not linked and (info.st_dev, info.st_ino) == identity:
            add_alias()
        return real_read(fd, size)
    monkeypatch.setattr(os, "open", racing_open)
    monkeypatch.setattr(os, "read", racing_read)
    destination = tmp_path / "snapshot"
    with pytest.raises(WorkspaceBoundaryError, match="hardlink"):
        snapshot_workspace(source, destination)
    assert not destination.exists()


@pytest.mark.parametrize("secret_name", [".env", ".ENV"])
@pytest.mark.parametrize("alias", ["-copy", "config-copy"])
def test_snapshot_rejects_protected_inode_after_secret_unlink(tmp_path, monkeypatch, secret_name, alias):
    source = tmp_path / "source"
    source.mkdir()
    secret = source / secret_name
    secret.write_bytes(b"unlink-secret-sentinel")
    os.link(secret, source / alias)
    real_stat, real_open = os.stat, os.open
    unlinked = False
    opened_aliases = []
    def racing_stat(path, *args, **kwargs):
        nonlocal unlinked
        if not unlinked and path == alias and kwargs.get("dir_fd") is not None:
            secret.unlink()
            unlinked = True
        return real_stat(path, *args, **kwargs)
    def observed_open(path, flags, *args, **kwargs):
        if path == alias and kwargs.get("dir_fd") is not None:
            opened_aliases.append(path)
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, "stat", racing_stat)
    monkeypatch.setattr(os, "open", observed_open)
    destination = tmp_path / "snapshot"
    with pytest.raises(WorkspaceBoundaryError):
        snapshot_workspace(source, destination)
    assert not opened_aliases, "protected inode must be rejected before opening its bytes"
    assert not destination.exists()


@pytest.mark.parametrize("mutation", ["add-protected-name", "change-protected-file"])
def test_snapshot_rejects_protected_mutation_after_enumeration(tmp_path, monkeypatch, mutation):
    source = tmp_path / "source"
    source.mkdir()
    (source / "ordinary").write_bytes(b"safe")
    secret = source / ".env"
    if mutation == "change-protected-file": secret.write_bytes(b"initial-secret")
    real_stat, real_fstat = os.stat, os.fstat
    root_before = source.stat()
    changed = False
    def stable_root_fstat(fd):
        info = real_fstat(fd)
        # Namespace changes need an explicit name check, even when directory
        # timestamps/size do not distinguish two operations in one clock tick.
        if mutation == "add-protected-name" and (info.st_dev, info.st_ino) == (root_before.st_dev, root_before.st_ino):
            return root_before
        return info
    def racing_stat(path, *args, **kwargs):
        nonlocal changed
        if not changed and path == "ordinary" and kwargs.get("dir_fd") is not None:
            secret.write_bytes(b"replacement-secret")
            changed = True
        return real_stat(path, *args, **kwargs)
    monkeypatch.setattr(os, "stat", racing_stat)
    monkeypatch.setattr(os, "fstat", stable_root_fstat)
    destination = tmp_path / "snapshot"
    with pytest.raises(WorkspaceBoundaryError):
        snapshot_workspace(source, destination)
    assert not destination.exists()
