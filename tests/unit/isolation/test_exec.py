from __future__ import annotations

import errno
from pathlib import Path
import resource

import pytest

from orchestrator.isolation import _exec
from orchestrator.isolation.landlock import LandlockUnavailable


def _mock_cgroup(monkeypatch, *, overrides: dict[str, str] | None = None) -> None:
    root = Path("/sys/fs/cgroup")
    cgroup = root / "maestro-test"
    values = {
        str(cgroup / "memory.max"): "268435456",
        str(cgroup / "memory.swap.max"): "0",
        str(cgroup / "pids.max"): "16",
        str(cgroup / "cpu.max"): "50000 100000",
    }
    values.update(overrides or {})
    original_read = Path.read_text
    original_resolve = Path.resolve

    def read_text(path: Path, *args, **kwargs):
        if path == Path("/proc/self/cgroup"):
            return "0::/maestro-test\n"
        if str(path) in values:
            return values[str(path)]
        return original_read(path, *args, **kwargs)

    def resolve(path: Path, *args, **kwargs):
        if path in (root, cgroup):
            return path
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    monkeypatch.setattr(Path, "resolve", resolve)
    monkeypatch.setattr(resource, "getrlimit", lambda which: (64, 64) if which == resource.RLIMIT_NOFILE else (1024 * 1024, 1024 * 1024))
    for name, value in {
        "MAESTRO_EXPECT_MEMORY": "268435456",
        "MAESTRO_EXPECT_TASKS": "16",
        "MAESTRO_EXPECT_CPU": "50",
        "MAESTRO_EXPECT_NOFILE": "64",
        "MAESTRO_EXPECT_FSIZE": str(1024 * 1024),
    }.items():
        monkeypatch.setenv(name, value)


def test_verify_limits_accepts_exact_cgroup_and_rlimit_values(monkeypatch) -> None:
    _mock_cgroup(monkeypatch)
    _exec._verify_limits()


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"memory.max": "1"}, "MemoryMax"),
        ({"memory.swap.max": "1"}, "MemorySwapMax"),
        ({"pids.max": "2"}, "TasksMax"),
        ({"cpu.max": "1000 100000"}, "CPUQuota"),
    ],
)
def test_verify_limits_rejects_unenforced_cgroup_values(monkeypatch, override, message) -> None:
    _mock_cgroup(monkeypatch, overrides={
        "/sys/fs/cgroup/maestro-test/" + name: value for name, value in override.items()
    })
    with pytest.raises(RuntimeError, match=message):
        _exec._verify_limits()


@pytest.mark.parametrize(
    ("which", "soft", "message"),
    [
        (resource.RLIMIT_NOFILE, 32, "LimitNOFILE"),
        (resource.RLIMIT_FSIZE, 512, "LimitFSIZE"),
    ],
)
def test_verify_limits_rejects_unenforced_rlimits(monkeypatch, which, soft, message) -> None:
    _mock_cgroup(monkeypatch)
    def getrlimit(requested):
        if requested == resource.RLIMIT_NOFILE:
            value = soft if which == resource.RLIMIT_NOFILE else 64
            return value, value
        value = soft if which == resource.RLIMIT_FSIZE else 1024 * 1024
        return value, value

    monkeypatch.setattr(resource, "getrlimit", getrlimit)
    with pytest.raises(RuntimeError, match=message):
        _exec._verify_limits()


def test_verify_limits_rejects_missing_v2_and_cgroup_escape(monkeypatch) -> None:
    root = Path("/sys/fs/cgroup")
    original_read = Path.read_text
    original_resolve = Path.resolve
    _mock_cgroup(monkeypatch)
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **k: "1:cpu:/legacy\n" if path == Path("/proc/self/cgroup") else original_read(path, *a, **k))
    with pytest.raises(RuntimeError, match="cgroup v2"):
        _exec._verify_limits()

    def escaped_resolve(path: Path, *args, **kwargs):
        if path == root / "maestro-test":
            return Path("/outside")
        if path == root:
            return root
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", escaped_resolve)
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **k: "0::/maestro-test\n" if path == Path("/proc/self/cgroup") else original_read(path, *a, **k))
    with pytest.raises(ValueError):
        _exec._verify_limits()


def test_runtime_grants_never_include_host_root() -> None:
    grants = _exec._runtime_grants()
    paths = {grant.path for grant in grants}
    assert Path("/") not in paths
    assert Path("/tmp") in paths
    assert Path("/usr") in paths
    assert Path("/etc") in paths


def test_exec_bootstrap_rejects_bad_arguments_and_fails_closed(monkeypatch, capsys) -> None:
    assert _exec.main([]) == 64
    assert _exec.main(["not-a-separator", "/bin/true"]) == 64

    def fail_verify():
        raise RuntimeError("bad limits")

    monkeypatch.setattr(_exec, "_verify_limits", fail_verify)
    assert _exec.main(["--", "/bin/true"]) == 78
    assert "sandbox setup failed: RuntimeError" in capsys.readouterr().err


def test_exec_bootstrap_applies_landlock_then_execs_with_clean_environment(monkeypatch, capsys) -> None:
    monkeypatch.setattr(_exec, "_verify_limits", lambda: None)
    applied = []
    monkeypatch.setattr(_exec, "restrict_current_process", lambda grants: applied.extend(grants))
    observed = {}

    def exec_failure(path, argv, env):
        observed.update(path=path, argv=argv, env=env)
        raise OSError(errno.ENOENT, "not found")

    monkeypatch.setattr(_exec.os, "execvpe", exec_failure)
    assert _exec.main(["--", "/missing-tool", "arg"]) == 127
    assert applied
    assert observed["path"] == "/missing-tool"
    assert observed["argv"] == ["/missing-tool", "arg"]
    assert observed["env"] == {
        "PATH": "/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "HOME": "/nonexistent",
        "TMPDIR": "/tmp",
    }
    assert "sandbox command could not start: 2" in capsys.readouterr().err


def test_exec_bootstrap_reports_landlock_failure_without_running_tool(monkeypatch, capsys) -> None:
    monkeypatch.setattr(_exec, "_verify_limits", lambda: None)

    def fail_landlock(_grants):
        raise LandlockUnavailable("test restriction failure")

    monkeypatch.setattr(_exec, "restrict_current_process", fail_landlock)
    assert _exec.main(["--", "/bin/true"]) == 78
    assert "test restriction failure" in capsys.readouterr().err
