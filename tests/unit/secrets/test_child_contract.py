"""Child descriptor and default-backend contracts, measured in the test process.

The separate live subprocess tests prove containment; these checks expose the
same startup contract to coverage without changing production environment or
claiming coverage instrumentation follows the sanitized subprocess.
"""
import asyncio
import copy
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from orchestrator.config.models import ProviderSpec
from orchestrator.models.gateway import SecretAccessContext
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.secrets import SecretAccessRule, UnixSocketSecretBroker
from orchestrator.secrets import _broker_child as child
from orchestrator.secrets.process import _read_frame


def descriptor(tmp_path):
    provider = ProviderSpec(id="primary", adapter="openai_compatible",
        endpoint="https://provider.example.test/v1", secret_ref="env:MAESTRO_TEST_KEY",
        enabled=True)
    rule = SecretAccessRule(provider.id, provider.secret_ref, provider.effective_endpoint,
                            "model_inference", frozenset({"run-1"}))
    record = dict(version=1, expected_parent_pid=os.getppid(),
        event_store_path=str(tmp_path / "events.db"), socket_path=str(tmp_path / "broker.sock"),
        session_nonce="child-contract-nonce", providers=[provider.model_dump(mode="json")],
        rules=[dict(provider_id=rule.provider_id, secret_ref=rule.secret_ref,
                    endpoint=rule.endpoint, purpose=rule.purpose,
                    allowed_run_ids=["run-1"])])
    return record, provider, rule


def test_child_decodes_frozen_provider_and_rule_without_secret_material(tmp_path):
    # A parser that drops Run/endpoint/adapter constraints changes these values.
    record, _provider, _rule = descriptor(tmp_path)
    rules, providers = child._decode_bootstrap(record)
    assert len(rules) == 1
    assert rules[0].allowed_run_ids == frozenset({"run-1"})
    assert rules[0].endpoint == "https://provider.example.test/v1"
    assert rules[0].secret_ref == "env:MAESTRO_TEST_KEY"
    assert set(providers) == {"primary"}
    assert providers["primary"].adapter == "openai_compatible"
    assert providers["primary"].enabled is True


@pytest.mark.parametrize("bad", ["unknown_key", "missing_key", "version_bool", "version_2",
    "parent_bool", "parent_zero", "rules_not_list", "providers_not_list", "rule_not_object",
    "rule_extra_key", "runs_not_list", "run_not_string", "duplicate_provider",
    "relative_event_path", "relative_socket_path", "non_string_event_path"])
def test_child_rejects_untrusted_bootstrap_descriptors(tmp_path, bad):
    # Each malformed record must fail before it can create an IPC listener.
    record, _provider, _rule = descriptor(tmp_path)
    if bad == "unknown_key":
        record["secret_value"] = "never-accepted"
    elif bad == "missing_key":
        record.pop("session_nonce")
    elif bad == "version_bool":
        record["version"] = True
    elif bad == "version_2":
        record["version"] = 2
    elif bad == "parent_bool":
        record["expected_parent_pid"] = True
    elif bad == "parent_zero":
        record["expected_parent_pid"] = 0
    elif bad == "rules_not_list":
        record["rules"] = {}
    elif bad == "providers_not_list":
        record["providers"] = {}
    elif bad == "rule_not_object":
        record["rules"] = [None]
    elif bad == "rule_extra_key":
        record["rules"][0]["secret_value"] = "never-accepted"
    elif bad == "runs_not_list":
        record["rules"][0]["allowed_run_ids"] = "run-1"
    elif bad == "run_not_string":
        record["rules"][0]["allowed_run_ids"] = [1]
    elif bad == "duplicate_provider":
        record["providers"].append(copy.deepcopy(record["providers"][0]))
    elif bad == "relative_event_path":
        record["event_store_path"] = "events.db"
    elif bad == "relative_socket_path":
        record["socket_path"] = "broker.sock"
    elif bad == "non_string_event_path":
        record["event_store_path"] = None
    with pytest.raises(ValueError):
        child._decode_bootstrap(record)
    assert not (tmp_path / "broker.sock").exists()


def test_production_child_default_backend_never_reads_host_environment(tmp_path, monkeypatch):
    # Swapping the production unavailable store for an ambient env store would
    # return this host value or fail startup, violating this observable contract.
    record, provider, _rule = descriptor(tmp_path)
    with SQLiteEventStore(tmp_path / "events.db"):
        pass
    monkeypatch.setenv("MAESTRO_TEST_KEY", "host-key-must-not-be-read")
    monkeypatch.setattr(child.ctypes, "CDLL", lambda *_args, **_kwargs: SimpleNamespace(prctl=lambda *_: 0))
    monkeypatch.setattr(child, "_read_frame", lambda *_args, **_kwargs: record)
    real_close = os.close
    monkeypatch.setattr(child.os, "close", lambda fd: None if fd == 0 else real_close(fd))
    handlers = {}
    monkeypatch.setattr(child.signal, "signal", lambda sig, handler: handlers.update({sig: handler}))

    class ChildExit(BaseException):
        def __init__(self, code):
            self.code = code

    def exit_child(code):
        raise ChildExit(code)

    monkeypatch.setattr(child.os, "_exit", exit_child)
    ready_read, ready_write = os.pipe()
    actual_server = child.UnixSecretBrokerServer

    class RedirectReadyServer(actual_server):
        def serve_forever(self, *, readiness_fd=None):
            # Process stdio is the doubled boundary; actual IPC/server is real.
            return super().serve_forever(readiness_fd=ready_write)

    monkeypatch.setattr(child, "UnixSecretBrokerServer", RedirectReadyServer)
    observed = []
    failures = []

    def request_then_stop():
        try:
            ready = _read_frame(ready_read, deadline=time.monotonic() + 5, max_bytes=1024)
            assert ready["status"] == "ready"
            broker = UnixSocketSecretBroker(socket_path=Path(record["socket_path"]),
                                            session_nonce=record["session_nonce"])
            context = SecretAccessContext(request_id="child-request", run_id="run-1", node_id="node-1",
                attempt_id="attempt-1", fencing_generation=1, accepted_route_id="route-1",
                budget_reservation_id="reservation-1")
            observed.append(asyncio.run(broker.acquire_provider_credential(
                secret_ref=provider.secret_ref, provider=provider, endpoint=provider.effective_endpoint,
                purpose="model_inference", context=context)))
        except BaseException as error:
            failures.append(error)
        finally:
            handler = handlers.get(child.signal.SIGTERM)
            if handler is not None:
                handler()

    requester = threading.Thread(target=request_then_stop, daemon=True)
    requester.start()
    try:
        with pytest.raises(ChildExit) as stopped:
            child.main()
        assert stopped.value.code == 0
    finally:
        requester.join(timeout=6)
        real_close(ready_read)
    assert not requester.is_alive()
    assert not failures
    assert observed == [None]
    with SQLiteEventStore(tmp_path / "events.db") as events:
        audit = events.read_stream("security", "run-1")
    assert [event.event_type for event in audit] == ["SecretAccessGranted", "SecretCredentialUnavailable"]
    found = b"host-key-must-not-be-read" in (tmp_path / "events.db").read_bytes()
    assert not found, "host environment credential entered the audit"
