from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from orchestrator.isolation.launcher import (
    InvalidSandboxRequest,
    IsolationUnavailable,
    SandboxLimits,
    SystemdReadOnlyLauncher,
    _systemd_path_supported,
    _create_staging,
    _validate_command,
    _validated_workspace,
)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("memory_bytes", 1),
        ("tasks", 0),
        ("cpu_percent", 0),
        ("timeout_seconds", 0),
        ("output_bytes", 0),
        ("nofile", 1),
        ("file_bytes", 0),
        ("tasks", True),
    ],
)
def test_limits_reject_invalid_values(field: str, value: int) -> None:
    with pytest.raises(InvalidSandboxRequest):
        SandboxLimits(**{field: value})


@pytest.mark.parametrize("command", [[], "echo hi", [""], ["echo", "bad\x00arg"]])
def test_commands_reject_ambiguous_or_invalid_arguments(command) -> None:
    with pytest.raises(InvalidSandboxRequest):
        _validate_command(command)


def test_workspace_must_be_absolute_real_and_existing(tmp_path: Path) -> None:
    with pytest.raises(InvalidSandboxRequest, match="absolute"):
        _validated_workspace(Path("relative"))
    with pytest.raises(InvalidSandboxRequest, match="existing real directory"):
        _validated_workspace(tmp_path / "missing")
    link = tmp_path / "workspace-link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(InvalidSandboxRequest, match="real directory"):
        _validated_workspace(link)


def test_systemd_property_paths_fail_closed_on_ambiguous_separators(tmp_path: Path) -> None:
    assert _systemd_path_supported(tmp_path)
    assert not _systemd_path_supported(tmp_path / "with space")
    assert not _systemd_path_supported(tmp_path / "colon:name")
    with pytest.raises(InvalidSandboxRequest, match="size"):
        _validate_command(["x" * 32769])


def test_staging_refuses_a_workspace_that_covers_all_host_paths() -> None:
    with pytest.raises(IsolationUnavailable, match="outside the workspace"):
        _create_staging(Path("/"))


def test_non_linux_or_missing_systemd_fails_before_launch(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module

    monkeypatch.setattr(module.platform, "system", lambda: "Windows")
    with pytest.raises(IsolationUnavailable, match="Linux-only"):
        SystemdReadOnlyLauncher().launch(tmp_path, ["/bin/true"])

    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module.shutil, "which", lambda _name: None)
    with pytest.raises(IsolationUnavailable, match="required"):
        SystemdReadOnlyLauncher().launch(tmp_path, ["/bin/true"])


def test_workspace_snapshot_failure_is_reported_as_invalid_request(monkeypatch, tmp_path: Path) -> None:
    from orchestrator.isolation.workspace import WorkspaceBoundaryError
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "snapshot_workspace", lambda *_args: (_ for _ in ()).throw(WorkspaceBoundaryError()))
    with pytest.raises(InvalidSandboxRequest, match="snapshot"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


def test_launcher_rejects_excessive_workspace_snapshot_before_launch(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module
    from orchestrator.isolation.workspace import WorkspaceBoundaryError

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "snapshot_workspace", lambda *_args: (_ for _ in ()).throw(WorkspaceBoundaryError("too large")))
    with pytest.raises(InvalidSandboxRequest, match="snapshot"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


@pytest.mark.parametrize(
    "failure",
    [
        subprocess.CompletedProcess(["systemctl"], 1),
        OSError("missing systemctl"),
        subprocess.TimeoutExpired(["systemctl"], 5),
    ],
)
def test_launcher_requires_healthy_systemd_manager(monkeypatch, tmp_path: Path, failure) -> None:
    import orchestrator.isolation.launcher as module

    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    if isinstance(failure, BaseException):
        monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(failure))
        reason = "probe failed"
    else:
        monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: failure)
        reason = "usable systemd"
    with pytest.raises(IsolationUnavailable, match=reason):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            tmp_path, ["/bin/true"]
        )


def test_launcher_rejects_missing_or_ambiguous_trusted_runtime(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    monkeypatch.setattr(
        module,
        "__file__",
        str(tmp_path / "missing" / "runtime" / "orchestrator" / "isolation" / "launcher.py"),
    )
    with pytest.raises(IsolationUnavailable, match="trusted isolation runtime"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )

    runtime = tmp_path / "runtime space" / "src"
    runtime.mkdir(parents=True)
    monkeypatch.setattr(
        module,
        "__file__",
        str(runtime / "orchestrator" / "isolation" / "launcher.py"),
    )
    with pytest.raises(InvalidSandboxRequest, match="spaces"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


def test_launcher_cleans_staging_when_systemd_client_cannot_start(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))

    def fail_to_start(*args, **kwargs):
        raise OSError("no process")

    monkeypatch.setattr(module.subprocess, "Popen", fail_to_start)
    with pytest.raises(IsolationUnavailable, match="could not be started"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


def test_launcher_cleans_staging_when_bind_targets_cannot_be_created(monkeypatch, tmp_path: Path) -> None:
    import orchestrator.isolation.launcher as module

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module, "_systemd_client_environment", lambda: {"PATH": "/usr/bin:/bin"})
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    original_mkdir = Path.mkdir

    def fail_runtime_target(path: Path, *args, **kwargs):
        if path.name == "runtime" and path.parent.name.startswith("maestro-sandbox-"):
            raise OSError("cannot create mount target")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_runtime_target)
    with pytest.raises(IsolationUnavailable, match="mount targets"):
        SystemdReadOnlyLauncher(systemd_run="systemd-run", systemctl="systemctl").launch(
            workspace, ["/bin/true"]
        )


def test_systemd_client_environment_requires_runtime_directory(monkeypatch) -> None:
    import orchestrator.isolation.launcher as module

    for name in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "SYSTEMD_BUS_ADDRESS"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(IsolationUnavailable, match="XDG_RUNTIME_DIR"):
        module._systemd_client_environment()
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
