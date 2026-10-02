"""Real subprocess replay and Linux parent-death containment."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import select
import signal
import sys

from orchestrator.config.models import ProviderSpec
from orchestrator.models.gateway import SecretAccessContext
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.secrets import SecretAccessRule


def _setup(tmp_path):
    from orchestrator.secrets import SecretBrokerProcessManager
    events = tmp_path / "events.db"
    with SQLiteEventStore(events):
        pass
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    provider = ProviderSpec(id="primary", adapter="openai_responses", secret_ref="env:MODEL_KEY", enabled=True)
    rule = SecretAccessRule(provider.id, provider.secret_ref, provider.effective_endpoint,
                            "model_inference", frozenset({"run-1"}))
    return SecretBrokerProcessManager(runtime_root=runtime, event_store_path=events), provider, rule, events


def test_restart_replay_is_denied_after_child_restart(tmp_path):
    # Reusing durable request IDs must not create a second access decision.
    manager, provider, rule, events = _setup(tmp_path)
    paths = []
    for _ in range(2):
        session = manager.start(rules=(rule,), providers={provider.id: provider})
        paths.append(session.socket_path)
        try:
            assert asyncio.run(session.client.acquire_provider_credential(
                secret_ref=provider.secret_ref, provider=provider, endpoint=provider.effective_endpoint,
                purpose="model_inference", context=SecretAccessContext(request_id="request-1",
                run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
                accepted_route_id="route-1", budget_reservation_id="reservation-1"))) is None
        finally:
            session.close()
    assert paths[0] != paths[1]
    with SQLiteEventStore(events) as store:
        audit = store.read_stream("security", "run-1")
    assert [event.event_type for event in audit] == ["SecretAccessGranted", "SecretCredentialUnavailable"]


def test_manager_child_exits_when_disposable_parent_exits(tmp_path):
    # Losing the supervisor must terminate the broker without a close() call.
    manager, provider, rule, events = _setup(tmp_path)
    code = '''
import json, os, sys
from pathlib import Path
from orchestrator.config.models import ProviderSpec
from orchestrator.secrets import SecretAccessRule, SecretBrokerProcessManager
p = ProviderSpec(id="primary", adapter="openai_responses", secret_ref="env:MODEL_KEY", enabled=True)
r = SecretAccessRule(p.id, p.secret_ref, p.effective_endpoint, "model_inference", frozenset({"run-1"}))
s = SecretBrokerProcessManager(runtime_root=Path(sys.argv[1]), event_store_path=Path(sys.argv[2])).start(rules=(r,), providers={p.id:p})
print(json.dumps({"pid":s.process_pid, "socket":str(s.socket_path)}), flush=True)
sys.stdin.read(1)
os._exit(0)
'''
    parent = subprocess.Popen([sys.executable, "-c", code, str(manager._runtime_root), str(events)],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src")})
    child_handle = None
    try:
        record = json.loads(parent.stdout.readline())
        child_pid = record["pid"]
        child_handle = os.pidfd_open(child_pid)
        assert Path(record["socket"]).exists()
        parent.stdin.write(b"x"); parent.stdin.flush()
        assert parent.wait(timeout=5) == 0
        assert select.select([child_handle], [], [], 5)[0], "broker survived supervisor death"
    finally:
        if parent.poll() is None:
            parent.kill(); parent.wait(timeout=5)
        if child_handle is not None:
            try:
                signal.pidfd_send_signal(child_handle, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                os.close(child_handle)
