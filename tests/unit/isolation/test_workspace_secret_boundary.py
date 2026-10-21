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
