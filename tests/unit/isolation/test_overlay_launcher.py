"""Candidate admission and retention with real filesystem validation."""

from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import tempfile

import pytest

from orchestrator.isolation import InvalidSandboxRequest, IsolationUnavailable, SandboxLimits, SandboxResult, SandboxTerminationReceipt
from orchestrator.isolation import overlay_launcher as module
from orchestrator.isolation import _overlay_bootstrap as bootstrap
from orchestrator.isolation._overlay_bootstrap import _write_completion
from orchestrator.isolation.workspace import export_overlay_diff


def test_overlay_candidate_scope_retains_supported_resource_limits() -> None:
    properties = module._candidate_scope_properties(SandboxLimits())
    assert "MemoryMax=536870912" in properties
    assert not any(value.startswith("InaccessiblePaths=") for value in properties)


def test_overlay_candidate_mounts_empty_readonly_private_user_runtime(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "lower").mkdir()
    operations = []
    monkeypatch.setattr(bootstrap, "_mount", lambda *args: operations.append(args))
    view = bootstrap._prepare_root(
        tmp_path, {"candidate_bytes": 4096, "scratch_bytes": 8192, "runtime_source": "/trusted/runtime"},
    )
    private_runtime = view / "run/user"
    mounts = [args for args in operations if args[-1] == str(private_runtime)]
    assert len(mounts) == 1
    mount, = mounts
    assert mount[:3] == ("-t", "tmpfs", "-o")
    assert {"ro", "mode=000", "nosuid", "nodev", "noexec"} <= set(mount[3].split(","))
    assert mount[4] == "tmpfs"
    assert private_runtime.is_dir() and list(private_runtime.iterdir()) == []
    assert not any(args[0] == "--bind" and args[1] in {"/run", "/run/user"} for args in operations)


def test_private_user_runtime_mount_failure_aborts_candidate_setup(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "lower").mkdir()
    operations = []

    def mount(*args):
        operations.append(args)
        if args[-1] == str(tmp_path / "rootfs/run/user"):
            raise bootstrap._MountSetupError("private runtime mount denied")

    monkeypatch.setattr(bootstrap, "_mount", mount)
    with pytest.raises(bootstrap._MountSetupError, match="private runtime"):
        bootstrap._prepare_root(
            tmp_path, {"candidate_bytes": 4096, "scratch_bytes": 8192, "runtime_source": "/trusted/runtime"},
        )
    assert operations[-1][-1] == str(tmp_path / "rootfs/run/user")


def _candidate(root: Path):
    root.mkdir(mode=0o700)
    (root / "lower").mkdir()
    (root / "upper").mkdir()
    (root / "lower/input").write_bytes(b"original")
    (root / "upper/input").write_bytes(b"candidate")
    diff = export_overlay_diff(root / "lower", root / "upper", root / "candidate")
    _write_completion(root, diff)
    return diff


def _execution(**updates):
    unit = "maestro-candidate-" + "d" * 32 + ".scope"
    receipt = SandboxTerminationReceipt(
        unit_name=unit,
        control_group="/user.slice/user-1000.slice/user@1000.service/app.slice/" + unit,
        active_state="inactive",
        cgroup_empty=True,
    )
    return replace(SandboxResult(unit, 0, b"untrusted stdout", b"", 0.01,
                                 receipt, False, False, False), **updates)


class _Transport:
    unit_name = "maestro-candidate-" + "d" * 32 + ".scope"

    def __init__(self, result):
        self.result = result

    def wait(self):
        return self.result

    def cancel(self):
        return True


class _Staging:
    def __init__(self, root):
        self.root = root

    def discard(self):
        (self.root / "discarded").touch()


def test_session_revalidates_real_candidate_and_explicit_close_ends_lifetime(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "stage"
    _candidate(root)
    monkeypatch.setattr(module, "_scope_stopped", lambda *_args: True)
    session = module.OverlayCandidateSession(_Transport(_execution()), _Staging(root), root, {}, 10, 1024, scope_cgroup=tmp_path / "group")
    result = session.wait()
    assert result.diff is not None
    assert (result.diff.candidate_root / "input").read_bytes() == b"candidate"
    assert result.candidate_error is None
    assert session.wait() is result
    session.close()
    session.close()
    assert (root / "discarded").exists()
    with pytest.raises(RuntimeError, match="closed"):
        session.wait()


def test_unconfirmed_stop_retains_staging_until_later_confirmation(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "stage"
    _candidate(root)
    monkeypatch.setattr(module, "_scope_stopped", lambda *_args: False)
    session = module.OverlayCandidateSession(_Transport(_execution()), _Staging(root), root, {}, 10, 1024, scope_cgroup=tmp_path / "group")
    result = session.wait()
    assert result.diff is None and result.candidate_error == "termination_unconfirmed"
    with pytest.raises(IsolationUnavailable, match="retained"):
        session.close()
    assert not (root / "discarded").exists()
    monkeypatch.setattr(module, "_scope_stopped", lambda *_args: True)
    session.close()
    assert (root / "discarded").exists()


@pytest.mark.parametrize("updates", [
    {"returncode": 1}, {"cancelled": True}, {"timed_out": True},
    {"output_limited": True}, {"input_written": False},
])
def test_no_candidate_on_unclean_execution(monkeypatch, tmp_path: Path, updates) -> None:
    root = tmp_path / "stage"
    _candidate(root)
    monkeypatch.setattr(module, "_scope_stopped", lambda *_args: True)
    with module.OverlayCandidateSession(_Transport(_execution(**updates)), _Staging(root), root, {}, 10, 1024, scope_cgroup=tmp_path / "group") as session:
        assert session.cancel()
        result = session.wait()
        assert result.diff is None and result.candidate_error == "execution_failed"


def test_corrupt_bytes_and_forged_stdout_cannot_admit_candidate(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "stage"
    _candidate(root)
    (root / "candidate/input").write_bytes(b"corrupted")
    monkeypatch.setattr(module, "_scope_stopped", lambda *_args: True)
    session = module.OverlayCandidateSession(_Transport(_execution()), _Staging(root), root, {}, 10, 1024, scope_cgroup=tmp_path / "group")
    result = session.wait()
    assert result.diff is None and result.candidate_error == "candidate_invalid"
    session.close()


@pytest.mark.parametrize("change", [
    lambda value: [], lambda value: {**value, "schema_version": True},
    lambda value: {**value, "schema_version": 2}, lambda value: {**value, "extra": "forged"},
    lambda value: {**value, "entries": {}}, lambda value: {**value, "entries": [1]},
    lambda value: {**value, "total_bytes": True},
    lambda value: {**value, "manifest_hash": "sha256:" + "a"*64},
])
def test_completion_is_strict_and_candidate_is_rehashed(tmp_path: Path, change) -> None:
    root = tmp_path / "stage"
    _candidate(root)
    completion = root / "completion.json"
    value = json.loads(completion.read_text())
    completion.write_text(json.dumps(change(value)))
    with pytest.raises((ValueError, TypeError, module.WorkspaceBoundaryError)):
        module._read_candidate(root, 10, 1024)


@pytest.mark.parametrize("payload", [b"not JSON", b'{"schema_version":1,"schema_version":1}', b'{"schema_version":NaN}'])
def test_malformed_duplicate_and_nonfinite_completion_fail(tmp_path: Path, payload: bytes) -> None:
    root = tmp_path / "stage"
    _candidate(root)
    (root / "completion.json").write_bytes(payload)
    with pytest.raises(ValueError):
        module._read_candidate(root, 10, 1024)


@pytest.mark.parametrize("kind", ["permissions", "symlink", "hardlink", "oversized", "entries"])
def test_completion_file_must_be_private_regular_and_bounded(monkeypatch, tmp_path: Path, kind: str) -> None:
    root = tmp_path / "stage"
    _candidate(root)
    completion = root / "completion.json"
    if kind == "permissions":
        completion.chmod(0o644)
    elif kind == "symlink":
        completion.rename(root / "original")
        completion.symlink_to(root / "original")
    elif kind == "hardlink":
        os.link(completion, root / "alias")
    elif kind == "oversized":
        monkeypatch.setattr(module, "_MAX_COMPLETION_BYTES", 10)
    with pytest.raises((OSError, ValueError)):
        module._read_candidate(root, 0 if kind == "entries" else 10, 1024)


@pytest.mark.parametrize(("stdout", "returncode", "expected"), [
    (b"LoadState=not-found\nActiveState=inactive\n", 1, True),
    (b"LoadState=loaded\nActiveState=failed\n", 0, True),
    (b"", 1, False), (b"LoadState=loaded\nActiveState=active\n", 0, False),
])
def test_stop_confirmation_requires_manager_state(monkeypatch, stdout, returncode, expected) -> None:
    clock = iter([0, 3])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], returncode, stdout, b""))
    monkeypatch.setattr(module, "_cgroup_is_empty", lambda _group: True)
    assert module._scope_stopped("candidate.scope", {}, Path("/unused")) is expected


@pytest.mark.parametrize("error", [OSError(), subprocess.TimeoutExpired("systemctl", 3)])
def test_stop_query_error_is_not_stop_evidence(monkeypatch, error) -> None:
    def failure(*_a, **_k):
        raise error
    monkeypatch.setattr(module.subprocess, "run", failure)
    assert not module._scope_stopped("candidate.scope", {}, Path("/unused"))


def test_stop_confirmation_can_wait_for_scope_transition(monkeypatch) -> None:
    states = iter([b"LoadState=loaded\nActiveState=active\n", b"LoadState=loaded\nActiveState=inactive\n"])
    monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 0, next(states), b""))
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(module, "_cgroup_is_empty", lambda _group: True)
    assert module._scope_stopped("candidate.scope", {}, Path("/unused"))


@pytest.mark.parametrize(("key", "value"), [
    ("candidate_bytes", 0), ("candidate_bytes", True), ("candidate_bytes", 64*1024*1024+1),
    ("candidate_entries", 10_001), ("scratch_bytes", "1"),
])
def test_invalid_candidate_budgets_fail_before_launch(key: str, value) -> None:
    with pytest.raises(InvalidSandboxRequest):
        module.SystemdOverlayCandidateLauncher(**{key: value})


def test_unsupported_platform_or_missing_kernel_tools_fail(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(module.platform, "system", lambda: "Windows")
    with pytest.raises(IsolationUnavailable, match="Linux-only"):
        module.SystemdOverlayCandidateLauncher().launch(tmp_path, ["true"])
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module.shutil, "which", lambda _name: None)
    with pytest.raises(IsolationUnavailable, match="requires"):
        module.SystemdOverlayCandidateLauncher().launch(tmp_path, ["true"])


@pytest.mark.parametrize("options", [{"input_bytes": b"x"*1_048_577}, {"input_bytes": "text"}, {"limits": {}}])
def test_invalid_input_or_limits_do_not_start_scope(tmp_path: Path, options) -> None:
    with pytest.raises(InvalidSandboxRequest):
        module.SystemdOverlayCandidateLauncher().launch(tmp_path, ["/bin/true"], **options)


@pytest.mark.parametrize("failure", [1, OSError(), subprocess.TimeoutExpired("manager", 5)])
def test_manager_probe_failure_prevents_execution(monkeypatch, tmp_path: Path, failure) -> None:
    def query(*_a, **_k):
        if isinstance(failure, BaseException):
            raise failure
        return subprocess.CompletedProcess([], failure)
    monkeypatch.setattr(module.subprocess, "run", query)
    with pytest.raises(IsolationUnavailable):
        module.SystemdOverlayCandidateLauncher().launch(tmp_path, ["/bin/true"])


def test_wrong_frozen_workspace_identity_rejects_before_scope(tmp_path: Path) -> None:
    with pytest.raises(InvalidSandboxRequest, match="snapshot"):
        module.SystemdOverlayCandidateLauncher().launch(
            tmp_path, ["/bin/true"], expected_workspace_identity_hash="sha256:" + "a"*64,
        )


def test_untrackable_start_failure_retains_even_if_scope_not_yet_registered(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    stages = []
    def stage(_workspace):
        temporary = tempfile.TemporaryDirectory(prefix="candidate-unit-", dir=tmp_path)
        stages.append(temporary)
        return temporary
    def start(*_a, **_k):
        raise OSError("transport unavailable")
    monkeypatch.setattr(module, "_create_staging", stage)
    monkeypatch.setattr(module, "_scope_stopped", lambda *_a: True)
    monkeypatch.setattr(module, "_candidate_cgroup_parent", lambda _env: Path("/sys/fs/cgroup/app.slice"))
    monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 0))
    monkeypatch.setattr(module.subprocess, "Popen", start)
    with pytest.raises(IsolationUnavailable, match="retained at"):
        module.SystemdOverlayCandidateLauncher().launch(workspace, ["/bin/true"])
    assert len(stages) == 1
    assert Path(stages[0].name).exists()
    stages[0].cleanup()  # The fake transport could not have launched a real scope.


def test_uncertain_transport_start_does_not_remove_potentially_mounted_stage(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    stages = []
    def stage(_workspace):
        temporary = tempfile.TemporaryDirectory(prefix="candidate-unit-", dir=tmp_path)
        stages.append(temporary)
        return temporary
    def start(*_a, **_k):
        raise OSError("transport unavailable")
    monkeypatch.setattr(module, "_create_staging", stage)
    monkeypatch.setattr(module, "_scope_stopped", lambda *_a: False)
    monkeypatch.setattr(module, "_candidate_cgroup_parent", lambda _env: Path("/sys/fs/cgroup/app.slice"))
    monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 0))
    monkeypatch.setattr(module.subprocess, "Popen", start)
    with pytest.raises(IsolationUnavailable, match="retained at"):
        module.SystemdOverlayCandidateLauncher().launch(workspace, ["/bin/true"])
    assert len(stages) == 1 and Path(stages[0].name).exists()
    stages[0].cleanup()  # Test transport never spawned a real process.


@pytest.mark.parametrize("content", [b"populated 0\nfrozen 0\n", b"populated 1\nfrozen 0\n", b"populated 0\npopulated 1\n", b"unknown\n", b"populated true\n"])
def test_empty_cgroup_requires_unambiguous_kernel_proof(tmp_path, content) -> None:
    group = tmp_path / "cgroup"
    group.mkdir()
    (group / "cgroup.events").write_bytes(content)
    assert module._cgroup_is_empty(group) is (content == b"populated 0\nfrozen 0\n")


def test_vanished_cgroup_is_empty_but_missing_or_symlinked_events_are_not(tmp_path) -> None:
    group = tmp_path / "cgroup"
    assert module._cgroup_is_empty(group)
    group.mkdir()
    assert not module._cgroup_is_empty(group)
    outside = tmp_path / "fake-events"
    outside.write_text("populated 0\n")
    (group / "cgroup.events").symlink_to(outside)
    assert not module._cgroup_is_empty(group)


def test_failed_unit_with_populated_cgroup_is_not_stopped(monkeypatch, tmp_path) -> None:
    group = tmp_path / "group"
    group.mkdir()
    (group / "cgroup.events").write_text("populated 1\nfrozen 0\n")
    monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 0, b"LoadState=loaded\nActiveState=failed\n"))
    assert not module._scope_stopped("candidate.scope", {}, group)


@pytest.mark.parametrize("name", [b"", b"/", b"/../app.slice", b"/user.slice/not-app.slice", b"not-absolute/app.slice", b"\xff"])
def test_cgroup_parent_cannot_be_forged(monkeypatch, name) -> None:
    monkeypatch.setattr(module.subprocess, "run", lambda *_a, **_k: subprocess.CompletedProcess([], 0, name))
    with pytest.raises(IsolationUnavailable, match="cgroup parent"):
        module._candidate_cgroup_parent({})


def test_completion_fifo_rejects_without_waiting_for_writer(tmp_path) -> None:
    os.mkfifo(tmp_path / "completion.json", 0o600)
    # Run in a child with a deadline so a regression cannot hang pytest.
    process = subprocess.run([
        os.sys.executable, "-c",
        "from pathlib import Path; from orchestrator.isolation.overlay_launcher import _read_candidate; "
        "_read_candidate(Path(__import__('sys').argv[1]), 10, 1024)", str(tmp_path),
    ], capture_output=True, timeout=3, env={**os.environ, "PYTHONPATH": str(Path(module.__file__).resolve().parents[2])})
    assert process.returncode != 0
    assert b"bounded private record" in process.stderr
