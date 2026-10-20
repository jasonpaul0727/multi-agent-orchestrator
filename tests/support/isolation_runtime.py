"""Native-filesystem copy of exact tested sources for real isolation tests."""

from pathlib import Path
import shutil

import pytest


@pytest.fixture(scope="session")
def private_isolation_runtime(tmp_path_factory):
    from orchestrator.isolation import overlay_launcher
    source = Path(overlay_launcher.__file__).resolve().parents[2]
    target = tmp_path_factory.mktemp("verified-runtime") / "src"
    shutil.copytree(source, target, ignore=shutil.ignore_patterns("__pycache__"),
                    copy_function=shutil.copyfile)
    target.chmod(0o700)
    for path in target.rglob("*"):
        if path.is_dir():
            path.chmod(0o700)
        else:
            path.chmod(0o600)
            assert path.read_bytes() == (source / path.relative_to(target)).read_bytes()
    return target


@pytest.fixture
def use_private_isolation_runtime(private_isolation_runtime, monkeypatch):
    from orchestrator.isolation import overlay_launcher
    monkeypatch.setattr(overlay_launcher, "__file__",
                        str(private_isolation_runtime / "orchestrator/isolation/overlay_launcher.py"))
    return private_isolation_runtime
