"""Real durable fixtures for Worker input admission and later re-supply."""

from datetime import timedelta
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from orchestrator.application import ControlPlaneApplication
from orchestrator.scheduler import AcceptedAttempt
from tests.unit.lifecycle.test_scheduler import (
    NOW, _application_limits, routed_pair, run_setup_with_frozen_contract,
)


def make_worker_input_case(database: Path) -> SimpleNamespace:
    app = ControlPlaneApplication(database, limits=_application_limits())
    try:
        reg, config, lifecycle, policy, contract = run_setup_with_frozen_contract(app._event_store)
        request, decision = routed_pair(reg, config, policy, contract_hash=contract.contract_hash)
        return SimpleNamespace(
            app=app, request=request, decision=decision, lifecycle=lifecycle,
            clock=SimpleNamespace(now=NOW + timedelta(seconds=2)),
            text="implement a small change",
            identity=dict(run_id=request.run_id, node_id=request.node_id,
                          attempt_id=request.attempt_id, fencing_generation=1),
        )
    except BaseException:
        app.close()
        raise


def admit_worker_input_case(case) -> AcceptedAttempt:
    case.accepted = case.app.accept_routing_with_input(
        case.request, case.decision, task_text=case.text,
        accepted_at=NOW + timedelta(seconds=1),
        lease_expires_at=NOW + timedelta(minutes=1),
    )
    return case.accepted


def prepare_worker_input_case(case):
    case.app._worker_inputs._clock = lambda: case.clock.now
    return case.app.prepare_worker_task(**case.identity, task_text=case.text)


def corrupt_worker_input_event(store, stream, mutate):
    """Test hostile durable data, keeping payload integrity valid for replay."""
    event = store.read_stream(*stream)[-1]
    payload = json.loads(json.dumps(event.payload))
    mutate(payload)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    trigger = store._connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='events_immutable_update'"
    ).fetchone()[0]
    store._connection.execute("DROP TRIGGER events_immutable_update")
    try:
        store._connection.execute(
            "UPDATE events SET payload_json = ?, payload_hash = ? WHERE event_id = ?",
            (encoded, hashlib.sha256(encoded.encode()).hexdigest(), event.event_id),
        )
    finally:
        store._connection.execute(trigger)
