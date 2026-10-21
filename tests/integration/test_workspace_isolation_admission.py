"""Required live admission controls: intentionally no skip or fake positive."""

from dataclasses import replace

import pytest

from orchestrator.isolation import (
    IsolationNotStarted, SandboxLimits, SystemdOverlayCandidateLauncher,
    verify_workspace_isolation,
)
from orchestrator.isolation import admission, overlay_launcher
from tests.support.isolation_runtime import private_isolation_runtime, use_private_isolation_runtime

pytestmark = pytest.mark.usefixtures("use_private_isolation_runtime")


def test_live_measured_profile_then_real_request_has_separate_probe_count(tmp_path, monkeypatch):
    calls = {"trusted_probe": 0, "untrusted_command": 0}
    probe_results = []
    raw = SystemdOverlayCandidateLauncher._launch
    def track(self, workspace, command, **kwargs):
        calls["trusted_probe" if command[-1] == admission._PROBE else "untrusted_command"] += 1
        session = raw(self, workspace, command, **kwargs)
        if command[-1] == admission._PROBE:
            probe_results.append(session.wait())
        return session
    monkeypatch.setattr(SystemdOverlayCandidateLauncher, "_launch", track)
    measured = verify_workspace_isolation(tmp_path, limits=SandboxLimits())
    assert measured.outcome == "eligible", probe_results
    with SystemdOverlayCandidateLauncher().launch(
        tmp_path, ("/usr/bin/python3", "-c", "from pathlib import Path; Path('request-marker').write_text('executed')"),
        expected_profile_hash=measured.profile_hash,
        expected_workspace_identity_hash=measured.workspace_identity_hash,
    ) as session:
        result = session.wait()
        assert result.execution.returncode == 0, result.execution.stderr
        assert result.execution.termination_confirmed
        assert result.diff is not None
        assert (result.diff.candidate_root / "request-marker").read_text() == "executed"
    assert calls == {"trusted_probe": 2, "untrusted_command": 1}
    assert not (tmp_path / "request-marker").exists()


def test_live_missing_effective_memory_control_never_runs_request(tmp_path, monkeypatch):
    original = overlay_launcher._candidate_scope_properties
    monkeypatch.setattr(overlay_launcher, "_candidate_scope_properties",
        lambda limits: tuple(p for p in original(limits) if not p.startswith("MemoryMax=")))
    calls = {"trusted_probe": 0, "untrusted_command": 0}
    raw = SystemdOverlayCandidateLauncher._launch
    def track(self, workspace, command, **kwargs):
        calls["trusted_probe" if command[-1] == admission._PROBE else "untrusted_command"] += 1
        return raw(self, workspace, command, **kwargs)
    monkeypatch.setattr(SystemdOverlayCandidateLauncher, "_launch", track)
    with pytest.raises(IsolationNotStarted):
        SystemdOverlayCandidateLauncher().launch(
            tmp_path, ("/usr/bin/touch", "must-not-run"),
        )
    assert calls == {"trusted_probe": 1, "untrusted_command": 0}
    assert list(tmp_path.iterdir()) == []


def test_live_control_drift_after_probe_rejects_before_transport(tmp_path, monkeypatch):
    original = overlay_launcher.snapshot_workspace
    properties = overlay_launcher._candidate_scope_properties
    snapshots = 0
    def drift(*args, **kwargs):
        nonlocal snapshots
        original(*args, **kwargs)
        snapshots += 1
        if snapshots == 2:  # First is the real probe, second the actual request.
            monkeypatch.setattr(overlay_launcher, "_candidate_scope_properties",
                lambda limits: tuple(p for p in properties(limits) if not p.startswith("TasksMax=")))
    monkeypatch.setattr(overlay_launcher, "snapshot_workspace", drift)
    with pytest.raises(IsolationNotStarted):
        SystemdOverlayCandidateLauncher().launch(tmp_path, ("/usr/bin/touch", "must-not-run"))
    assert snapshots == 2 and list(tmp_path.iterdir()) == []


def test_live_stronger_policy_profile_does_not_authorize_weaker_limits(tmp_path):
    stronger = verify_workspace_isolation(tmp_path, limits=replace(SandboxLimits(), tasks=32))
    assert stronger.outcome == "eligible"
    with pytest.raises(IsolationNotStarted):
        SystemdOverlayCandidateLauncher().launch(
            tmp_path, ("/usr/bin/touch", "must-not-run"), expected_profile_hash=stronger.profile_hash,
        )
    assert list(tmp_path.iterdir()) == []
