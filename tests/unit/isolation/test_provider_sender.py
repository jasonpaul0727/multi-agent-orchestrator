from __future__ import annotations

import base64
import importlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

import orchestrator.isolation.launcher as launcher_module
from orchestrator.isolation.launcher import (
    InvalidSandboxRequest,
    IsolationUnavailable,
    SandboxLimits,
    SandboxSession,
    SandboxTerminationReceipt,
    _service_properties,
)


_MODULE = "orchestrator.isolation.provider_sender"


def _sender_module():
    assert importlib.util.find_spec(_MODULE) is not None, (
        "the dedicated systemd Provider sender launcher is not implemented"
    )
    return importlib.import_module(_MODULE)


def _request_frame(runtime_module, *, url="https://api.example.test/v1/responses"):
    return runtime_module.encode_provider_sender_request(
        url=url,
        headers=(
            ("accept", "application/json"),
            ("authorization", "Bearer sender-secret-marker"),
            ("content-type", "application/json"),
        ),
        body=b'{"model":"test"}',
        timeout_ms=500,
        max_response_bytes=1_024,
    )


def test_provider_sender_launcher_uses_separate_network_enabled_profile(monkeypatch, tmp_path):
    sender_module = _sender_module()
    runtime_module = importlib.import_module("orchestrator.runtime.provider_sender_process")
    frame = _request_frame(runtime_module)
    client_env = {"PATH": "/usr/bin:/bin", "XDG_RUNTIME_DIR": "/tmp/test-runtime"}
    parent = Path(
        "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice"
    )
    launched = {}

    class Process:
        stdin = stdout = stderr = None
        returncode = None

        def poll(self):
            return self.returncode

    def popen(arguments, **kwargs):
        launched["arguments"] = arguments
        launched["kwargs"] = kwargs
        return Process()

    monkeypatch.setattr(sender_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(sender_module, "_systemd_client_environment", lambda: client_env)
    monkeypatch.setattr(sender_module, "_systemd_cgroup_parent", lambda *_args: parent)
    monkeypatch.setattr(
        sender_module.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, stdout=b"", stderr=b""),
    )
    monkeypatch.setattr(sender_module.subprocess, "Popen", popen)
    launcher = sender_module.SystemdProviderSenderLauncher(
        systemd_run="systemd-run-test", systemctl="systemctl-test"
    )

    session = launcher.launch(frame, timeout_seconds=3, output_bytes=4_096)

    arguments = launched["arguments"]
    properties = [item.removeprefix("--property=") for item in arguments if item.startswith("--property=")]
    rendered_arguments = " ".join(arguments)
    assert session.unit_name.startswith("maestro-provider-")
    assert session.unit_name.endswith(".service")
    assert "--slice=app.slice" in arguments
    assert "--wait" in arguments and "--pipe" in arguments and "--collect" in arguments
    assert "PrivateNetwork=no" in properties
    assert "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6" in properties
    assert "NoNewPrivileges=yes" in properties
    binds = [value for value in properties if value.startswith("BindReadOnlyPaths=")]
    assert len(binds) == 1
    source, target = binds[0].removeprefix("BindReadOnlyPaths=").split(":", 1)
    assert Path(source).is_file()
    assert Path(target).is_file()
    assert source != target
    assert Path(target).parent.name == "runtime"
    assert Path(target).parent.parent == Path(source).parent
    assert "workspace" not in " ".join(binds)
    assert "sender-secret-marker" not in rendered_arguments
    assert "sender-secret-marker" not in repr(launched["kwargs"]["env"])
    assert launched["kwargs"]["stdin"] == subprocess.PIPE
    assert launched["kwargs"]["env"] == client_env
    assert "PrivateNetwork=yes" in _service_properties(
        root=tmp_path,
        runtime_source=tmp_path,
        workspace_target=tmp_path / "workspace",
        runtime_target=tmp_path / "runtime",
        limits=SandboxLimits(),
    )
    session._sandbox_session._staging.discard()


def test_provider_sender_launcher_rejects_invalid_frame_and_bounds_before_systemd(monkeypatch):
    sender_module = _sender_module()
    runtime_module = importlib.import_module("orchestrator.runtime.provider_sender_process")
    frame = _request_frame(runtime_module)
    launcher = sender_module.SystemdProviderSenderLauncher(
        systemd_run="systemd-run-test", systemctl="systemctl-test"
    )
    monkeypatch.setattr(
        sender_module.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("invalid sender requests must fail before systemd"),
    )

    with pytest.raises(InvalidSandboxRequest, match="frame"):
        launcher.launch(frame.replace(b"api.example.test/v1", b"api.example.test/v1?key=x"), timeout_seconds=3, output_bytes=4_096)
    with pytest.raises(InvalidSandboxRequest, match="timeout"):
        launcher.launch(frame, timeout_seconds=0, output_bytes=4_096)
    with pytest.raises(InvalidSandboxRequest, match="output bound"):
        launcher.launch(frame, timeout_seconds=3, output_bytes=1)


def test_isolation_package_import_remains_stdlib_only_for_worker_bootstrap():
    source_root = Path(__file__).parents[3] / "src"
    code = (
        "import sys; "
        f"sys.path.insert(0, {str(source_root)!r}); "
        "import orchestrator.isolation; "
        "assert 'pydantic' not in sys.modules"
    )

    result = subprocess.run(
        [sys.executable, "-S", "-c", code],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr.decode(errors="replace")


class _TrackingStaging:
    def __init__(self):
        self.discard_count = 0
        self.cleanup_count = 0

    def cleanup(self):
        self.cleanup_count += 1

    def discard(self):
        self.discard_count += 1


def _provider_unit():
    return "maestro-provider-" + "f" * 32 + ".service"


def _provider_cgroup(unit):
    return Path("/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice") / unit


def _completed_process(stdout=b"", *, returncode=0):
    code = (
        "import sys; sys.stdout.buffer.write(" + repr(stdout) + "); sys.exit(" + str(returncode) + ")"
    )
    return subprocess.Popen(
        [sys.executable, "-c", code], stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
    )


def test_provider_sender_session_returns_parsed_response_only_with_host_receipt(monkeypatch):
    sender_module = _sender_module()
    body = b'{"ok":true}'
    frame = json.dumps(
        {
            "version": 1,
            "status": 200,
            "headers": [["x-request-id", "request-1"]],
            "body_b64": base64.b64encode(body).decode("ascii"),
        },
        separators=(",", ":"),
    ).encode()
    unit = _provider_unit()
    cgroup = _provider_cgroup(unit)
    receipt = SandboxTerminationReceipt(
        unit_name=unit,
        control_group="/" + str(cgroup.relative_to("/sys/fs/cgroup")),
        active_state="inactive",
        cgroup_empty=True,
    )
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", lambda **_args: receipt)
    sandbox = SandboxSession(
        process=_completed_process(frame), unit_name=unit, systemctl="systemctl", client_env={},
        output_limit=4_096, timeout_seconds=3, staging=_TrackingStaging(), scope_cgroup=cgroup,
    )

    result = sender_module.SystemdProviderSenderSession(
        sandbox, max_response_bytes=1_024
    ).wait()

    assert result.termination_receipt is receipt
    assert result.response is not None
    assert result.response.status == 200
    assert result.response.headers == (("x-request-id", "request-1"),)
    assert result.response.body == body


def test_provider_sender_session_withholds_response_and_retains_staging_without_receipt(monkeypatch):
    sender_module = _sender_module()
    unit = _provider_unit()
    cgroup = _provider_cgroup(unit)
    staging = _TrackingStaging()
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", lambda **_args: None)
    sandbox = SandboxSession(
        process=_completed_process(b"malicious-looking-output"),
        unit_name=unit,
        systemctl="systemctl",
        client_env={},
        output_limit=4_096,
        timeout_seconds=3,
        staging=staging,
        scope_cgroup=cgroup,
    )

    with pytest.raises(IsolationUnavailable, match="termination|stop|receipt"):
        sender_module.SystemdProviderSenderSession(
            sandbox, max_response_bytes=1_024
        ).wait()

    assert staging.discard_count == 0
    assert staging.cleanup_count == 0


def test_provider_sender_session_discards_unverified_child_frame_on_failure(monkeypatch):
    sender_module = _sender_module()
    unit = _provider_unit()
    cgroup = _provider_cgroup(unit)
    receipt = SandboxTerminationReceipt(
        unit_name=unit,
        control_group="/" + str(cgroup.relative_to("/sys/fs/cgroup")),
        active_state="failed",
        cgroup_empty=True,
    )
    monkeypatch.setattr(launcher_module, "_read_termination_receipt", lambda **_args: receipt)
    sandbox = SandboxSession(
        process=_completed_process(b'{"version":1}', returncode=1),
        unit_name=unit,
        systemctl="systemctl",
        client_env={},
        output_limit=4_096,
        timeout_seconds=3,
        staging=_TrackingStaging(),
        scope_cgroup=cgroup,
    )

    result = sender_module.SystemdProviderSenderSession(
        sandbox, max_response_bytes=1_024
    ).wait()

    assert result.termination_receipt is receipt
    assert result.response is None


@pytest.mark.parametrize(
    ("active_state", "control_group_suffix", "cgroup_events", "expected_receipt"),
    [
        ("inactive", None, b"populated 0\nfrozen 0\n", True),
        ("inactive", "/wrong.service", b"populated 0\nfrozen 0\n", False),
        ("active", None, b"populated 0\nfrozen 0\n", False),
        ("inactive", None, b"populated 1\nfrozen 0\n", False),
    ],
)
def test_provider_sender_receipt_requires_exact_inactive_empty_cgroup(
    monkeypatch, tmp_path, active_state, control_group_suffix, cgroup_events, expected_receipt
):
    unit = _provider_unit()
    cgroup_root = tmp_path / "sysfs"
    cgroup = cgroup_root / "user.slice" / "app.slice" / unit
    cgroup.mkdir(parents=True)
    (cgroup / "cgroup.events").write_bytes(cgroup_events)
    monkeypatch.setattr(launcher_module, "_CGROUP_ROOT", cgroup_root)
    exact_group = "/" + cgroup.relative_to(cgroup_root).as_posix()
    reported_group = exact_group if control_group_suffix is None else "/user.slice/app.slice" + control_group_suffix
    output = (
        f"ActiveState={active_state}\nLoadState=loaded\nControlGroup={reported_group}\n"
    ).encode("ascii")
    monkeypatch.setattr(
        launcher_module.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, stdout=output, stderr=b""
        ),
    )

    receipt = launcher_module._read_termination_receipt(
        unit_name=unit,
        expected_cgroup=cgroup,
        client_env={},
        timeout_seconds=0.02,
        systemctl="systemctl-test",
    )

    assert (receipt is not None) is expected_receipt
    if receipt is not None:
        assert receipt.unit_name == unit
        assert receipt.control_group == exact_group
        assert receipt.cgroup_empty is True
