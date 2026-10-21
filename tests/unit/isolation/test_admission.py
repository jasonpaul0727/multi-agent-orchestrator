"""Admission must reject absent evidence before any request command starts."""

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
import importlib

import pytest

import orchestrator.isolation as isolation
from orchestrator.workspace_identity import workspace_identity_hash
from tests.support.isolation_runtime import private_isolation_runtime, use_private_isolation_runtime


def _admission():
    assert hasattr(isolation, "WorkspaceIsolationAdmission"), "measured admission is missing"
    return importlib.import_module("orchestrator.isolation.admission")


@pytest.mark.parametrize("platform", ["Windows", "Darwin", "FreeBSD"])
def test_unsupported_host_never_probes_or_launches(monkeypatch, tmp_path, platform):
    admission = _admission()
    monkeypatch.setattr(admission.platform, "system", lambda: platform)
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: pytest.fail("unsupported host probed"))
    result = admission.verify_workspace_isolation(tmp_path, limits=isolation.SandboxLimits())
    assert result.outcome == "blocked" and result.reason_code == "isolation_unavailable"
    assert result.profile_hash is None
    assert result.workspace_identity_hash == workspace_identity_hash(tmp_path)


@pytest.mark.parametrize("machine", ["aarch64", "i686", "unknown"])
def test_unmeasured_native_architecture_is_blocked(monkeypatch, tmp_path, machine):
    admission = _admission()
    monkeypatch.setattr(admission.platform, "machine", lambda: machine)
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: pytest.fail("unmeasured host probed"))
    assert admission.verify_workspace_isolation(tmp_path, limits=isolation.SandboxLimits()).outcome == "blocked"


def test_blocked_admission_cannot_carry_verified_profile(tmp_path):
    admission = _admission()
    args = dict(outcome="blocked", reason_code="isolation_unavailable", profile_hash=None,
                workspace_identity_hash=workspace_identity_hash(tmp_path), observed_at=datetime.now(timezone.utc))
    model = admission.WorkspaceIsolationAdmission(**args)
    with pytest.raises((FrozenInstanceError, ValueError)):
        model.outcome = "eligible"
    for changes in ({"profile_hash": "sha256:" + "a"*64}, {"outcome": "eligible"},
                    {"reason_code": None}, {"observed_at": "now"}, {"extra": True},
                    {"workspace_identity_hash": "claimed"}):
        with pytest.raises((ValueError, TypeError)):
            admission.WorkspaceIsolationAdmission(**(args | changes))


def test_failed_or_unstopped_probe_is_not_positive_evidence(monkeypatch, tmp_path):
    admission = _admission()
    monkeypatch.setattr(admission, "_observe_profile", lambda *_a, **_k: {"profile": "fixed"})
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: False)
    result = admission.verify_workspace_isolation(tmp_path, limits=isolation.SandboxLimits())
    assert result.outcome == "blocked" and result.profile_hash is None


def test_fingerprint_binds_observed_controls_limits_and_workspace(monkeypatch, tmp_path):
    admission = _admission()
    observed = {"control": "enforced"}
    monkeypatch.setattr(admission, "_observe_profile", lambda limits, **_k: observed | {"limits": limits.memory_bytes})
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: True)
    limits = isolation.SandboxLimits()
    first = admission.verify_workspace_isolation(tmp_path, limits=limits)
    second = admission.verify_workspace_isolation(tmp_path, limits=limits)
    assert first.outcome == second.outcome == "eligible"
    assert first.profile_hash == second.profile_hash and first.observed_at != second.observed_at
    assert admission.verify_workspace_isolation(tmp_path, limits=replace(limits, tasks=63)).workspace_identity_hash == first.workspace_identity_hash
    changed = admission.verify_workspace_isolation(tmp_path, limits=replace(limits, memory_bytes=limits.memory_bytes*2))
    assert changed.profile_hash != first.profile_hash
    observed["control"] = "changed"
    with pytest.raises(isolation.IsolationNotStarted):
        admission.revalidate_workspace_isolation(tmp_path, limits=limits, admission=first)
    moved = tmp_path / "replacement"
    moved.mkdir()
    with pytest.raises(isolation.IsolationNotStarted):
        admission.revalidate_workspace_isolation(moved, limits=limits, admission=first)


def test_probe_time_control_drift_invalidates_result(monkeypatch, tmp_path):
    admission = _admission()
    calls = iter([{"control": 1}, {"control": 2}])
    monkeypatch.setattr(admission, "_observe_profile", lambda *_a, **_k: next(calls))
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: True)
    assert admission.verify_workspace_isolation(tmp_path, limits=isolation.SandboxLimits()).outcome == "blocked"


def test_request_workspace_cannot_contain_trusted_runtime(monkeypatch, tmp_path):
    admission = _admission()
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    monkeypatch.setattr(admission, "_runtime_root", lambda: runtime)
    monkeypatch.setattr(admission, "_observe_profile", lambda *_a, **_k: {"measured": True})
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: True)
    assert admission.verify_workspace_isolation(tmp_path, limits=isolation.SandboxLimits()).outcome == "blocked"


def test_wrong_native_binary_is_not_measured_as_supported(monkeypatch, tmp_path, use_private_isolation_runtime):
    admission = _admission()
    executable = tmp_path / "wrong-native"
    executable.write_bytes(b"\x7fELF\x02\x01" + bytes(12) + b"\xb7\x00" + bytes(44))  # AArch64 ELF.
    executable.chmod(0o700)
    monkeypatch.setattr(admission.shutil, "which", lambda *_a, **_k: str(executable))
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: pytest.fail("wrong native binary probed"))
    with pytest.raises(isolation.IsolationNotStarted, match="native"):
        admission._observe_profile(isolation.SandboxLimits())


def test_invalid_command_is_proven_nonstart_without_admission_probe(monkeypatch, tmp_path):
    admission = _admission()
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: pytest.fail("invalid request probed"))
    with pytest.raises(isolation.IsolationNotStarted):
        isolation.SystemdOverlayCandidateLauncher().launch(tmp_path, [])


@pytest.mark.parametrize("loss", ["disappeared", "symlink"])
@pytest.mark.parametrize("entrypoint", ["verify", "launch"])
def test_workspace_identity_loss_is_proven_nonstart(monkeypatch, tmp_path, loss, entrypoint):
    admission = _admission()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    bound_identity = workspace_identity_hash(workspace)
    moved = tmp_path / "moved"
    workspace.rename(moved)
    if loss == "symlink":
        workspace.symlink_to(moved, target_is_directory=True)
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: pytest.fail("lost identity probed"))
    monkeypatch.setattr(isolation.SystemdOverlayCandidateLauncher, "_launch",
                        lambda *_a, **_k: pytest.fail("lost identity launched"))
    with pytest.raises(isolation.IsolationNotStarted):
        if entrypoint == "verify":
            admission.verify_workspace_isolation(workspace, limits=isolation.SandboxLimits())
        else:
            isolation.SystemdOverlayCandidateLauncher().launch(
                workspace, ("/usr/bin/true",), expected_workspace_identity_hash=bound_identity,
            )


def test_runtime_bytecode_changes_invalidate_verified_fingerprint(monkeypatch, tmp_path, use_private_isolation_runtime):
    admission = _admission()
    cache = use_private_isolation_runtime / "orchestrator/isolation/__pycache__"
    cache.mkdir(mode=0o700)
    bytecode = cache / "unused.cpython-312.pyc"
    bytecode.write_bytes(b"first bytecode identity")
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: True)
    measured = admission.verify_workspace_isolation(tmp_path, limits=isolation.SandboxLimits())
    assert measured.outcome == "eligible"
    try:
        bytecode.write_bytes(b"changed executable bytecode")
        with pytest.raises(isolation.IsolationNotStarted):
            admission.revalidate_workspace_isolation(tmp_path, limits=isolation.SandboxLimits(), admission=measured)
    finally:
        bytecode.unlink()
        cache.rmdir()


def test_probe_late_nonstart_claim_cannot_cleanup_unverified_process(monkeypatch, tmp_path):
    from pathlib import Path
    admission = _admission()
    temporary = admission.tempfile.TemporaryDirectory(dir=tmp_path)
    root = Path(temporary.name)
    closed = []
    class UncertainSession:
        def wait(self):
            raise isolation.IsolationNotStarted("late probe failure")
        def close(self):
            closed.append(True)
    monkeypatch.setattr(admission.tempfile, "TemporaryDirectory", lambda **_: temporary)
    monkeypatch.setattr(isolation.SystemdOverlayCandidateLauncher, "_launch", lambda *_a, **_k: UncertainSession())
    try:
        assert admission._run_probe(isolation.SandboxLimits()) is False
        assert root.exists() and closed == []
    finally:
        temporary.cleanup()  # The fake probe has no live process.


@pytest.mark.parametrize("kind", ["missing_binary", "private_root", "runtime"])
def test_unsafe_host_observations_prevent_probe(monkeypatch, tmp_path, kind):
    admission = _admission()
    if kind == "missing_binary":
        monkeypatch.setattr(admission.shutil, "which", lambda *_a, **_k: None)
    elif kind == "private_root":
        unsafe = tmp_path / "unsafe"
        unsafe.mkdir(mode=0o755)
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(unsafe))
    else:
        unsafe = tmp_path / "runtime"
        unsafe.mkdir(mode=0o777)
        unsafe.chmod(0o777)
        monkeypatch.setattr(admission, "_runtime_root", lambda: unsafe)
    monkeypatch.setattr(admission, "_run_probe", lambda *_a, **_k: pytest.fail("unsafe host probed"))
    assert admission.verify_workspace_isolation(tmp_path, limits=isolation.SandboxLimits()).outcome == "blocked"
