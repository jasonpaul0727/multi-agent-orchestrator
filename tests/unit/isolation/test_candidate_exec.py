from pathlib import Path
import errno

import pytest

from orchestrator.isolation import _candidate_exec as module


@pytest.fixture
def prepared(monkeypatch):
    monkeypatch.setenv("MAESTRO_EXPECT_NOFILE", "256")
    monkeypatch.setenv("MAESTRO_EXPECT_FSIZE", "65536")
    monkeypatch.setattr(module.resource, "getrlimit", lambda limit: (256, 256) if limit == module.resource.RLIMIT_NOFILE else (65536, 65536))
    monkeypatch.setattr(module, "verify_candidate_privileges", lambda: None)
    monkeypatch.setattr(module, "restrict_current_process", lambda _grants: None)
    monkeypatch.setattr(module, "restrict_candidate_syscalls", lambda: None)
    # No test can exec or restrict the pytest process.
    monkeypatch.setattr(module.os, "execvpe", lambda *_args: None)


@pytest.mark.parametrize("arguments", [[], ["--"], ["command", "argument"]])
def test_requires_explicit_command_boundary(arguments) -> None:
    assert module.main(arguments) == 64


def test_clean_environment_and_only_private_write_grants(prepared, monkeypatch) -> None:
    monkeypatch.setenv("SECRET_CANARY", "never forwarded")
    grants = []
    calls = []
    monkeypatch.setattr(module, "restrict_current_process", grants.extend)
    monkeypatch.setattr(module.os, "execvpe", lambda *args: calls.append(args))
    assert module.main(["--", "/usr/bin/python3", "-V"]) == 127  # exec cannot return on success.
    assert calls == [("/usr/bin/python3", ["/usr/bin/python3", "-V"], {
        "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "HOME": "/nonexistent", "TMPDIR": "/tmp",
    })]
    assert {grant.path for grant in grants} == {Path(p) for p in ("/usr", "/runtime", "/dev", "/workspace", "/tmp")}
    assert all(not grant.access & module.FsAccess.WRITE_FILE for grant in grants if grant.path in {Path("/usr"), Path("/runtime")})


@pytest.mark.parametrize("boundary", ["verify_candidate_privileges", "restrict_current_process", "restrict_candidate_syscalls", "rlimit", "missing_limit"])
def test_incomplete_controls_never_start_command(prepared, monkeypatch, capsys, boundary) -> None:
    started = []
    monkeypatch.setattr(module.os, "execvpe", lambda *_args: started.append(True))
    def failure(*_args):
        raise RuntimeError("secret host detail")
    if boundary == "rlimit":
        monkeypatch.setattr(module.resource, "getrlimit", lambda _limit: (1, 2))
    elif boundary == "missing_limit":
        monkeypatch.delenv("MAESTRO_EXPECT_NOFILE")
    else:
        monkeypatch.setattr(module, boundary, failure)
    assert module.main(["--", "true"]) == 78
    assert not started
    assert "secret host detail" not in capsys.readouterr().err


def test_exec_failure_is_bounded_and_does_not_expose_host_path(prepared, monkeypatch, capsys) -> None:
    def failure(*_args):
        raise OSError(errno.ENOENT, "private host path")
    monkeypatch.setattr(module.os, "execvpe", failure)
    assert module.main(["--", "missing"]) == 127
    assert capsys.readouterr().err == "candidate command could not start: 2\n"
