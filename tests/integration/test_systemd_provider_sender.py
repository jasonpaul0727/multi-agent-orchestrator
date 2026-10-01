from __future__ import annotations

import hashlib
import importlib.util
import shutil
import socket
import sys
import threading

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


def test_live_gateway_transport_cancellation_waits_for_exact_sender_stop():
    import asyncio

    from orchestrator.models.provider_calls import ProviderCallSnapshot

    import orchestrator.models.transport as transport_module

    transport_type = getattr(
        transport_module, "SystemdProviderHTTPSTransport", None
    )
    assert transport_type is not None, "the supervised Gateway transport is not implemented"

    request_body = b'{"input":"integration-cancel"}'
    binding = ProviderCallSnapshot(
        stream_id="call-" + "a" * 64,
        run_id="run-integration",
        node_id="node-integration",
        attempt_id="attempt-integration",
        fencing_generation=1,
        request_id="request-integration",
        idempotency_key_hash="sha256:"
        + hashlib.sha256(b"idem-integration").hexdigest(),
        accepted_route_id="decision-integration",
        budget_reservation_id="reservation-integration",
        provider_id="primary",
        provider_adapter="openai_compatible",
        provider_correlation_id=None,
        model_id="model-integration",
        registry_manifest_hash="sha256:" + "c" * 64,
        request_hash="sha256:" + hashlib.sha256(request_body).hexdigest(),
        status="dispatching",
    )
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(20)
    started = threading.Event()
    stop = threading.Event()
    connection_closed = threading.Event()

    def accept_and_hold_tls():
        try:
            try:
                connection, _address = listener.accept()
            except socket.timeout:
                return
            started.set()
            with connection:
                connection.settimeout(0.1)
                while not stop.is_set():
                    try:
                        if not connection.recv(4096):
                            break
                    except TimeoutError:
                        continue
        finally:
            connection_closed.set()
            listener.close()

    server = threading.Thread(target=accept_and_hold_tls, daemon=True)
    server.start()
    port = listener.getsockname()[1]
    transport = transport_type()

    async def cancel_after_connection_starts():
        invocation = asyncio.create_task(
            transport.post_json(
                url=f"https://127.0.0.1:{port}/v1/chat/completions",
                headers=(
                    ("accept", "application/json"),
                    ("authorization", "Bearer integration-only"),
                    ("content-type", "application/json"),
                    ("idempotency-key", "idem-integration"),
                    ("x-request-id", "request-integration"),
                ),
                body=request_body,
                timeout_ms=30_000,
                cancellation=None,
                max_response_bytes=1_024,
                call_binding=binding,
            )
        )
        if not await asyncio.to_thread(started.wait, 15):
            invocation.cancel()
            with pytest.raises(transport_module.HTTPTransportCancelled):
                await asyncio.wait_for(invocation, timeout=10)
            pytest.fail("sender never connected to local sink")
        invocation.cancel()
        with pytest.raises(transport_module.HTTPTransportCancelled) as failure:
            await asyncio.wait_for(invocation, timeout=10)
        assert failure.value.may_have_been_sent is True
        receipt = failure.value.termination_receipt
        assert receipt is not None
        assert receipt.provider_call_stream_id == binding.stream_id
        assert receipt.attempt_id == binding.attempt_id
        assert receipt.cgroup_empty is True

    try:
        asyncio.run(cancel_after_connection_starts())
    finally:
        stop.set()
        server.join(timeout=5)
    assert connection_closed.is_set()
