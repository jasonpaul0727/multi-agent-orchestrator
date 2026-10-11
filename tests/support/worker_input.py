"""Real durable fixtures for Worker input admission and later re-supply."""

from datetime import timedelta
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
