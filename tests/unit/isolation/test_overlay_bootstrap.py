import json
import os
from pathlib import Path
import signal
import subprocess
from types import SimpleNamespace

import pytest

from orchestrator.isolation import _overlay_bootstrap as module
from orchestrator.isolation.workspace import WorkspaceBoundaryError


def test_mount_failure_discloses_operation_not_host_detail(monkeypatch) -> None:
    monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 1, stderr=b"private secret path"))
    with pytest.raises(module._MountSetupError, match="operation=--bind, target=lower") as error:
        module._mount("--bind", "/private/secret", "/private/stage/lower")
    assert "secret" not in str(error.value)


def test_runtime_rejects_unmeasured_architecture(monkeypatch) -> None:
    monkeypatch.setattr(module.platform, "machine", lambda: "unknown")
    with pytest.raises(RuntimeError, match="architecture"):
        module._runtime_directories()


@pytest.mark.parametrize("membership", ["0::/expected/scope\n", "0::/wrong/scope\n", "0::/expected/scope\n0::/wrong/scope\n", ""])
def test_actual_scope_membership_matches_host_stop_proof(tmp_path, membership) -> None:
    source = tmp_path / "membership"
    source.write_text(membership)
    if membership == "0::/expected/scope\n":
        module._verify_scope_identity("/expected/scope", membership_file=source)
    else:
        with pytest.raises(RuntimeError, match="cgroup identity"):
            module._verify_scope_identity("/expected/scope", membership_file=source)


def test_root_uses_bounded_private_layers_and_readonly_top(tmp_path, monkeypatch) -> None:
    (tmp_path / "lower").mkdir()
    operations = []
    monkeypatch.setattr(module, "_mount", lambda *args: operations.append(args))
    view = module._prepare_root(tmp_path, {"candidate_bytes": 4096, "scratch_bytes": 8192, "runtime_source": "/trusted/runtime"})
    assert view == tmp_path / "rootfs"
    assert operations[0] == ("--make-rprivate", "/")
    assert operations[-1] == ("-o", "remount,bind,ro,nosuid,nodev", str(view))
    overlay, = [args for args in operations if args[:2] == ("-t", "overlay")]
    assert "index=off,metacopy=off,redirect_dir=nofollow" in overlay[4]
    assert "userxattr" in overlay[4]
    assert any("size=4096" in part for args in operations for part in args)
    assert any("size=8192" in part for args in operations for part in args)
    assert list((view / "proc").iterdir()) == []
    sources = {args[1] for args in operations if args[0] == "--bind"}
    assert "/usr" not in sources and "/home" not in sources and "/proc" not in sources
    assert "/trusted/runtime" in sources


def test_chroot_precedes_privilege_drop(monkeypatch, tmp_path) -> None:
    operations = []
    monkeypatch.setattr(module.os, "chroot", lambda path: operations.append(("chroot", path)))
    monkeypatch.setattr(module.os, "chdir", lambda path: operations.append(("chdir", path)))
    monkeypatch.setattr(module, "drop_candidate_privileges", lambda: operations.append(("drop",)))
    module._enter_command_root(tmp_path)
    assert operations == [("chroot", tmp_path), ("chdir", "/workspace"), ("drop",)]


def test_descendant_kill_can_never_run_outside_namespace_init(monkeypatch) -> None:
    monkeypatch.setattr(module.os, "getpid", lambda: 123)
    def forbidden(*_args):
        pytest.fail("must never signal host processes")
    monkeypatch.setattr(module.os, "kill", forbidden)
    with pytest.raises(RuntimeError, match="namespace init"):
        module._terminate_descendants()


@pytest.mark.parametrize("no_children", [True, False])
def test_namespace_init_reaps_all_children_even_with_interruption(monkeypatch, no_children) -> None:
    # Never execute a real kill(-1) in the test runner.
    monkeypatch.setattr(module.os, "getpid", lambda: 1)
    killed = []
    def kill(pid, sig):
        killed.append((pid, sig))
        if no_children:
            raise ProcessLookupError()
    monkeypatch.setattr(module.os, "kill", kill)
    sequence = iter([InterruptedError(), (2, 9), ChildProcessError()])
    def reap(pid, options):
        assert (pid, options) == (-1, 0)
        result = next(sequence)
        if isinstance(result, BaseException):
            raise result
        return result
    monkeypatch.setattr(module.os, "waitpid", reap)
    module._terminate_descendants()
    assert killed == [(-1, signal.SIGKILL)]


def test_only_known_content_neutral_overlay_metadata_is_removed(tmp_path) -> None:
    upper = tmp_path / "upper"
    upper.mkdir()
    nested = upper / "nested"
    nested.mkdir()
    file = nested / "file"
    file.write_text("candidate")
    outside = tmp_path / "outside"
    outside.write_text("untouched")
    (upper / "link").symlink_to(outside)
    os.setxattr(upper, "user.overlay.uuid", b"x"*16)
    os.setxattr(nested, "user.overlay.impure", b"y")
    os.setxattr(file, "user.overlay.origin", b"")
    os.setxattr(outside, "user.not-trusted", b"unchanged")
    module._strip_kernel_metadata(upper)
    assert not os.listxattr(upper) and not os.listxattr(nested) and not os.listxattr(file)
    assert os.getxattr(outside, "user.not-trusted") == b"unchanged"
    assert file.read_text() == "candidate"


@pytest.mark.parametrize(("name", "value"), [
    ("user.overlay.uuid", b"wrong"), ("user.overlay.impure", b"n"),
    ("user.overlay.origin", b"nonempty"), ("user.overlay.redirect", b"../escape"),
    ("user.other", b"attribute"),
])
def test_unknown_overlay_metadata_is_not_normalized_away(tmp_path, name, value) -> None:
    os.setxattr(tmp_path, name, value)
    with pytest.raises(WorkspaceBoundaryError, match="metadata"):
        module._strip_kernel_metadata(tmp_path)
    assert os.getxattr(tmp_path, name) == value


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    root = tmp_path / "stage"
    root.mkdir()
    lower = root / "lower"
    lower.mkdir()
    (lower / "input.txt").write_text("original")
    upper = root / "buffer" / "upper"
    upper.mkdir(parents=True)
    (upper / "input.txt").write_text("candidate")
    view = root / "rootfs"
    (view / "workspace").mkdir(parents=True)
    (root / "config.json").write_text(json.dumps({"command": ["command", "arg"], "candidate_entries": 10, "candidate_bytes": 1024, "expected_cgroup": "/expected/scope"}))
    monkeypatch.setenv("MAESTRO_EXPECT_NOFILE", "256")
    monkeypatch.setenv("MAESTRO_EXPECT_FSIZE", "1024")
    monkeypatch.setenv("SECRET_CANARY", "never inherited")
    monkeypatch.setattr(module.os, "getpid", lambda: 1)
    monkeypatch.setattr(module.resource, "setrlimit", lambda *_args: None)
    monkeypatch.setattr(module, "_verify_limits", lambda: None)
    monkeypatch.setattr(module, "_verify_scope_identity", lambda _expected: None)
    monkeypatch.setattr(module, "_prepare_root", lambda *_args: view)
    monkeypatch.setattr(module, "_enter_command_root", lambda _view: None)
    events = []
    def start(args, **options):
        assert args == ["/usr/bin/python3", "-P", "-S", "-m", "orchestrator.isolation._candidate_exec", "--", "command", "arg"]
        assert "SECRET_CANARY" not in options["env"]
        assert options["close_fds"]
        options["preexec_fn"]()
        events.append("start")
        return SimpleNamespace(wait=lambda: events.append("exit") or 0)
    monkeypatch.setattr(module.subprocess, "Popen", start)
    monkeypatch.setattr(module, "_terminate_descendants", lambda: events.append("reaped"))
    def unmount(*_args, **_options):
        events.append("unmount")
        return subprocess.CompletedProcess([], 0)
    monkeypatch.setattr(module.subprocess, "run", unmount)
    return root, events


def test_export_and_completion_only_after_descendant_stop_and_unmount(prepared) -> None:
    root, events = prepared
    assert module.main([str(root)]) == 0
    assert events == ["start", "exit", "reaped", "unmount"]
    payload = json.loads((root / "completion.json").read_text())
    assert payload["schema_version"] == 1
    assert payload["entries"][0]["path"] == "input.txt"
    assert (root / "candidate" / "input.txt").read_text() == "candidate"
    assert (root / "lower" / "input.txt").read_text() == "original"
    assert not (root / "completion.tmp").exists()
    assert (root / "completion.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("returncode", [7, 127, -9])
def test_failed_command_reaped_but_never_exported(prepared, monkeypatch, returncode) -> None:
    root, events = prepared
    monkeypatch.setattr(module.subprocess, "Popen", lambda *_a, **_k: SimpleNamespace(wait=lambda: returncode))
    assert module.main([str(root)]) == (7 if returncode == 7 else 1)
    assert events == ["reaped"]
    assert not (root / "candidate").exists() and not (root / "completion.json").exists()


@pytest.mark.parametrize("boundary", ["limits", "mounts", "exec", "unmount", "export", "completion"])
def test_incomplete_boundary_cannot_generate_completion(prepared, monkeypatch, capsys, boundary) -> None:
    root, _events = prepared
    def failure(*_a, **_k):
        raise RuntimeError("private host secret")
    if boundary == "limits":
        monkeypatch.setattr(module, "_verify_limits", failure)
    elif boundary == "mounts":
        monkeypatch.setattr(module, "_prepare_root", failure)
    elif boundary == "exec":
        monkeypatch.setattr(module.subprocess, "Popen", failure)
    elif boundary == "unmount":
        monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 1))
    elif boundary == "export":
        (root / "buffer" / "upper" / "escape").symlink_to("../../outside")
    else:
        monkeypatch.setattr(module, "_write_completion", failure)
    assert module.main([str(root)]) == 78
    assert not (root / "completion.json").exists()
    stderr = capsys.readouterr().err
    assert "candidate boundary failed at" in stderr and "private host secret" not in stderr


def test_namespace_guard_and_argument_validation_prevent_host_setup(monkeypatch, tmp_path, capsys) -> None:
    assert module.main([]) == 64
    monkeypatch.setattr(module.os, "getpid", lambda: 123)
    assert module.main([str(tmp_path)]) == 78
    assert "namespace" in capsys.readouterr().err
