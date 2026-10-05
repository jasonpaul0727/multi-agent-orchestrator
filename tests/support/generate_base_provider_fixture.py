"""Regenerate the upgrade fixture using an archived BASE checkout, never HEAD.

Usage: PYTHONPATH=<base>/src python3 <this-file> <base> <output-json>
BASE: 5bb9e58699b8f86ddb54a497fde273a19af35bac.
"""
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
import sys
import tempfile

from orchestrator.budget import UsageRecord
from orchestrator.lifecycle import NodeSpec
from orchestrator.models import CostSnapshotRefs, ModelMessage, ModelRequest, SQLiteProviderCallJournal
from orchestrator.persistence import SQLiteEventStore


def main():
    base, output = map(Path, sys.argv[1:])
    spec = importlib.util.spec_from_file_location("base_scheduler_fixture", base / "tests/unit/lifecycle/test_scheduler.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    requests = {}
    with tempfile.TemporaryDirectory() as temporary, SQLiteEventStore(Path(temporary) / "base.db") as store:
        states = ("dispatching", "unknown", "not_sent", "known_failure", "known_success")
        nodes = tuple(NodeSpec(node_id=state, role="coder", planning_contract_hash=helper.HASH, max_attempts=2) for state in states)
        reg, config, _lifecycle, manifest = helper.run_setup(store, nodes=nodes)
        control = helper.scheduler(store)
        journal = SQLiteProviderCallJournal(store)
        for state in states:
            routing, decision = helper.routed_pair(reg, config, manifest, node_id=state)
            accepted = helper.accept(control, routing, decision)
            route = accepted.accepted_route
            request = ModelRequest(
                request_id="provider-" + state, idempotency_key="provider-idem-" + state,
                run_id=route.run_id, node_id=route.node_id, attempt_id=route.attempt_id,
                fencing_generation=route.fencing_generation, budget_reservation_id=route.budget_reservation_id,
                model_id=route.model_id, accepted_route=route,
                messages=(ModelMessage(role="user", content="synthetic upgrade fixture"),),
                max_output_tokens=32, reasoning_effort=route.reasoning_effort, timeout_ms=5000,
                cost_snapshots=CostSnapshotRefs(registry_manifest_hash=reg.content_hash,
                    tokenizer_snapshot_id=helper.HASH, fx_snapshot_id=helper.HASH,
                    price_snapshot_id=reg.content_hash, estimator_snapshot_id=helper.HASH),
            )
            journal.record_intent(request, provider_id=route.provider_id, request_body=b"{}")
            if state != "dispatching":
                journal.record_outcome(request, outcome=state,
                    http_status={"known_success": 200, "known_failure": 500}.get(state),
                    failure_code=None if state == "known_success" else "fixture_failure",
                    usage={"status": "reported", "input_tokens": 3, "output_tokens": 2} if state == "known_success" else None)
            usage = UsageRecord(run_id=route.run_id, reservation_id=route.budget_reservation_id,
                status="committed", cost_minor=0, currency="USD", input_tokens=3, output_tokens=2,
                settlement_key="fixture-success") if state == "known_success" else None
            control.finish_attempt(run_id=route.run_id, node_id=route.node_id, attempt_id=route.attempt_id,
                fencing_generation=route.fencing_generation, completed_at=helper.NOW + timedelta(seconds=10),
                outcome="outcome_unknown" if state in {"dispatching", "unknown"} else "succeeded" if state == "known_success" else "failed",
                usage=usage, known_no_effect=state in {"not_sent", "known_failure"})
            requests[state] = request.model_dump(mode="json")
        tables = {name: [dict(row) for row in store._connection.execute("SELECT * FROM " + name)]
                  for name in ("events", "stream_versions", "idempotency_records")}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"base": "5bb9e58699b8f86ddb54a497fde273a19af35bac", "requests": requests, "tables": tables}, indent=2) + "\n")


if __name__ == "__main__":
    main()
