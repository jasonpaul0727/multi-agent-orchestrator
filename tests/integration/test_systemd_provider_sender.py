from __future__ import annotations

import shutil
import sys
import importlib.util

import pytest

import orchestrator.isolation as isolation_exports


def _usable_systemd_user_manager() -> bool:
    if not sys.platform.startswith("linux") or not shutil.which("systemd-run"):
        return False
    import subprocess

    return subprocess.run(
        ["systemctl", "--user", "show-environment"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=5,
    ).returncode == 0


pytestmark = pytest.mark.skipif(
    not _usable_systemd_user_manager(),
    reason="integrated Provider sender tests require Linux and systemd --user",
)


def test_live_provider_sender_stops_exact_unit_after_loopback_connection_refusal():
    assert hasattr(isolation_exports, "SystemdProviderSenderLauncher"), (
        "the dedicated systemd Provider sender launcher is not implemented"
    )
    runtime_name = "orchestrator.runtime.provider_sender_process"
    assert importlib.util.find_spec(runtime_name) is not None, (
        "the fixed Provider sender helper module is not implemented"
    )
    from orchestrator.runtime.provider_sender_process import encode_provider_sender_request

    frame = encode_provider_sender_request(
        url="https://127.0.0.1:1/v1/responses",
        headers=(
            ("accept", "application/json"),
            ("authorization", "Bearer test-only"),
            ("content-type", "application/json"),
        ),
        body=b"{}",
        timeout_ms=500,
        max_response_bytes=1_024,
    )

    result = isolation_exports.SystemdProviderSenderLauncher().launch(
        frame, timeout_seconds=4, output_bytes=4_096
    ).wait()

    assert result.response is None
    assert result.returncode != 0
    assert result.termination_receipt is not None
    assert result.termination_receipt.unit_name == result.unit_name
    assert result.termination_receipt.active_state in {"inactive", "failed"}
    assert result.termination_receipt.cgroup_empty is True
