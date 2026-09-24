from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.isolation import (
    FsAccess,
    LandlockResult,
    LandlockUnavailable,
    PathGrant,
    WORKSPACE_WRITE,
)
from orchestrator.isolation import landlock


class FakeLibc:
    def __init__(self, *, create=11, add=0, no_new_privs=0, restrict=0):
        self.create = create
        self.add = add
        self.no_new_privs = no_new_privs
        self.restrict = restrict
        self.closed_fd = None

    def syscall(self, number, *args):
        if number == 444:
            if self.create < 0:
                return self.create
            return os.open(os.devnull, os.O_RDONLY)
        if number == 445:
            return self.add
        if number == 446:
            return self.restrict
        raise AssertionError(f"unexpected syscall {number}")

    def prctl(self, *_args):
        return self.no_new_privs


def _use_fake_libc(monkeypatch, fake):
    monkeypatch.setattr(landlock, "landlock_abi", lambda: 3)
    monkeypatch.setattr(landlock, "_syscalls", lambda: (444, 445, 446))
    monkeypatch.setattr(landlock.ctypes, "CDLL", lambda *_args, **_kwargs: fake)


def test_abi_query_and_architecture_errors_fail_closed(monkeypatch):
    monkeypatch.setattr(landlock.platform, "machine", lambda: "unknown-cpu")
    with pytest.raises(LandlockUnavailable, match="unknown on this architecture"):
        landlock.landlock_abi()

    monkeypatch.setattr(landlock, "_syscalls", lambda: (444, 445, 446))
    fake = FakeLibc(create=-1)
    monkeypatch.setattr(landlock.ctypes, "CDLL", lambda *_args, **_kwargs: fake)
    ctypes.set_errno(errno.EPERM)
    with pytest.raises(LandlockUnavailable, match="ABI query"):
        landlock.landlock_abi()


def test_landlock_requires_abi_three_nonempty_grants_and_typed_rights(tmp_path, monkeypatch):
    monkeypatch.setattr(landlock, "landlock_abi", lambda: 2)
    with pytest.raises(LandlockUnavailable, match="ABI 3"):
        landlock.restrict_current_process((PathGrant(Path("/"), FsAccess.EXECUTE),))

    monkeypatch.setattr(landlock, "landlock_abi", lambda: 3)
    with pytest.raises(LandlockUnavailable, match="empty Landlock grant"):
        landlock.restrict_current_process(())

    fake = FakeLibc()
    _use_fake_libc(monkeypatch, fake)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for grants, message in (
        (("untyped",), "typed filesystem rights"),
        ((PathGrant(workspace, FsAccess(0)),), "empty or unsupported rights"),
        ((PathGrant(workspace, FsAccess(1 << 20)),), "empty or unsupported rights"),
        ((PathGrant(Path("."), WORKSPACE_WRITE),), "paths must be absolute"),
    ):
        with pytest.raises(LandlockUnavailable, match=message):
            landlock.restrict_current_process(grants)


@pytest.mark.parametrize(
    ("fake", "message"),
    (
        (FakeLibc(create=-1), "ruleset creation"),
        (FakeLibc(add=-1), "path grant was rejected"),
        (FakeLibc(no_new_privs=-1), "no_new_privs"),
        (FakeLibc(restrict=-1), "could not be applied"),
    ),
)
def test_landlock_syscall_failures_are_errors_and_close_open_paths(
    tmp_path, monkeypatch, fake, message
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _use_fake_libc(monkeypatch, fake)
    ctypes.set_errno(errno.EACCES)

    with pytest.raises(LandlockUnavailable, match=message):
        landlock.restrict_current_process((PathGrant(workspace, WORKSPACE_WRITE),))


def test_landlock_rejects_symlinked_grant_paths_and_returns_applied_evidence(
    tmp_path, monkeypatch
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    symlink = tmp_path / "workspace-link"
    symlink.symlink_to(workspace)
    _use_fake_libc(monkeypatch, FakeLibc())
    with pytest.raises(LandlockUnavailable, match="cannot be opened safely"):
        landlock.restrict_current_process((PathGrant(symlink, WORKSPACE_WRITE),))

    result = landlock.restrict_current_process((PathGrant(workspace, WORKSPACE_WRITE),))
    assert result == LandlockResult(abi=3, grant_count=1)
