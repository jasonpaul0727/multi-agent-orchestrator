"""Recovery discovery reads current durable authority without executing work."""

import subprocess
from datetime import timedelta

import pytest

from orchestrator.persistence import SQLiteEventStore
from tests.support.recovery_continuation import consume_recovery, durable_state, recovery_case
from tests.unit.lifecycle.test_scheduler import (
    NOW, scheduler, _provider_call_request_for_crash, routed_pair, accept,
)


def service(control):
    # A missing API should fail an executing behavioral test, not collection.
    from orchestrator.runtime import recovery_opportunities
    return recovery_opportunities.RecoveryOpportunityService(control)


def test_reopen_lists_known_failure_without_writes(tmp_path):
    # Catches missing discovery, wrong source binding, and accidental query writes.
    database = tmp_path / "recovery.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store)
        before = durable_state(store)
    with SQLiteEventStore(database) as store:
        items = service(scheduler(store)).list_opportunities(run_id="run-1")
        assert len(items) == 1
        item = items[0]
        assert (item.run_id, item.node_id, item.source_attempt_id,
                item.source_fencing_generation, item.status, item.reason) == (
            "run-1", "node-1", "attempt-node-1-1", 1, "ready", "bounded_same_model_retry",
        )
        assert item.source_decision_hash == case.decision.decision_hash
        assert item.authorization_hash == case.plan.authorization.authorization_hash
        assert item.evidence_hash == case.plan.authorization.evidence_hash
        assert item.source_decision_hash == "sha256:f7c617472a7e0c8a075e9597b597ccc655fd079cf40510dc2d442990b6324cd0"
        assert item.authorization_hash == "sha256:7f601ca8f9e4d6e259294ecb9665a4d30c547ba53efa15aca9e9fcdfbf6c7009"
        assert item.evidence_hash == "sha256:3ea9e7c2b7e0748f1b455c17de4a3476d1c9fe7298b84f5c43e8bdb34ede8b4b"
        assert item.action == "same_model_retry" and item.successor is None
        assert durable_state(store) == before


@pytest.mark.parametrize("unknown,code,status,reason", [
    (True, "timeout", "reconciliation_required", "provider_outcome_unknown"),
    (False, "invalid_response", "new_child_required", "targeted_repair_or_reaggregation_node_required"),
])
def test_unknown_and_new_child_plans_report_reasons(tmp_path, unknown, code, status, reason):
    # Catches treating an unknown or repair-only plan as retry authority.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, failure_code=code, unknown=unknown)
        before = durable_state(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == (status, reason)
        assert item.source_attempt_id == "attempt-node-1-1"
        assert item.source_fencing_generation == 1
        assert item.source_decision_hash == case.decision.decision_hash
        assert item.authorization_hash is None and item.action is None
        classified = next(event for event in store.read_stream("scheduler", "global")
                          if event.event_type == "AttemptFailureClassified")
        assert item.evidence_hash == classified.payload["classification"]["evidence"]["evidence_hash"]
        assert durable_state(store) == before


def test_query_executes_no_worker_or_provider(tmp_path, monkeypatch):
    # Catches discovery crossing any execution, credential, or process boundary.
    from orchestrator.models.gateway import ModelGateway
    from orchestrator.models.transport import UrllibHTTPSTransport
    from orchestrator.runtime.worker_process import IsolatedWorkerProcess
    from orchestrator.secrets.broker import AuditedSecretBroker

    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store)
        before = durable_state(store)
        calls = []

        def forbidden(*args, **kwargs):
            calls.append("forbidden-effect")
            pytest.fail("recovery discovery invoked an external effect")

        for target, method in (
            (IsolatedWorkerProcess, "execute"), (ModelGateway, "invoke"),
            (UrllibHTTPSTransport, "post_json"),
            (AuditedSecretBroker, "acquire_provider_credential_decision"),
            (subprocess, "Popen"),
        ):
            monkeypatch.setattr(target, method, forbidden)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert item.status == "ready"
        assert calls == [] and durable_state(store) == before


@pytest.mark.parametrize("transition,reason", [
    ("pause", "run_paused"), ("cancel", "run_cancelled"),
])
def test_inactive_run_blocks_opportunity(tmp_path, transition, reason):
    # Catches reporting ready while durable Run authority prevents continuation.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store)
        if transition == "pause":
            case.lifecycle.pause_run("run-1", reason_code="user_request")
        else:
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
            case.lifecycle.complete_cancellation_if_idle("run-1")
        before = durable_state(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == ("blocked", reason)
        assert durable_state(store) == before


def test_unresolved_current_provider_call_blocks_known_failure(tmp_path):
    # Catches trusting historical failure classification over current journal state.
    from orchestrator.models.provider_calls import SQLiteProviderCallJournal

    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store)
        journal = SQLiteProviderCallJournal(store)
        request = _provider_call_request_for_crash(case.accepted, case.registry)
        journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
                              provider_correlation_id="durable-call-1", request_body=b"{}")
        before = durable_state(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == ("reconciliation_required", "provider_outcome_unknown")
        assert item.authorization_hash == case.plan.authorization.authorization_hash
        assert durable_state(store) == before


def test_unconsumed_source_is_blocked_after_newer_attempt(tmp_path):
    # Catches an older unconsumed authorization becoming a second retry path.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store)
        request, decision = routed_pair(case.registry, case.config, case.policy, attempt=2,
                                       contract_hash=case.contract.contract_hash)
        accept(case.scheduler, request, decision)
        before = durable_state(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == ("blocked", "stale_source_attempt")
        assert item.source_attempt_id == "attempt-node-1-1"
        assert durable_state(store) == before


@pytest.mark.parametrize("successor_state", ["active", "failed", "paused", "outcome_unknown"])
def test_consumed_successor_reopens_to_exact_accepted_route(tmp_path, successor_state):
    # Catches rediscovering consumed authorization as stale/ready or inventing a new route.
    database = tmp_path / "consumed.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store)
        request, _, accepted = consume_recovery(case)
        if successor_state in {"failed", "outcome_unknown"}:
            case.scheduler.finish_attempt(
                run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
                fencing_generation=2, completed_at=NOW + timedelta(seconds=10),
                outcome=successor_state, known_no_effect=successor_state == "failed",
            )
        elif successor_state == "paused":
            case.lifecycle.pause_run("run-1", reason_code="user_request")
        before = durable_state(store)
    with SQLiteEventStore(database) as store:
        query = service(scheduler(store))
        item, = query.list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == ("already_admitted", "recovery_authorization_consumed")
        assert (item.source_attempt_id, item.source_fencing_generation) == ("attempt-node-1-1", 1)
        assert item.authorization_hash == case.plan.authorization.authorization_hash
        assert item.source_decision_hash == case.decision.decision_hash
        assert item.successor == accepted.accepted_route
        assert (item.successor.run_id, item.successor.node_id, item.successor.attempt_id,
                item.successor.fencing_generation, item.successor.model_id) == (
            "run-1", "node-1", "attempt-node-1-2", 2, "model-1",
        )
        assert query.list_opportunities(run_id="run-1") == (item,)
        assert durable_state(store) == before


def test_older_consumed_source_stays_bound_while_latest_source_is_ready(tmp_path):
    # Catches joining by node/latest attempt instead of the exact authorization hash.
    from orchestrator.models import ModelGatewayFailure

    with SQLiteEventStore(tmp_path / "chain.db") as store:
        case = recovery_case(store)
        request, decision, accepted = consume_recovery(case)
        case.scheduler.finish_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=2, completed_at=NOW + timedelta(seconds=10),
            outcome="failed", known_no_effect=True,
        )
        plan = case.scheduler.plan_attempt_recovery(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=2, source_decision=decision, contract=case.contract,
            failure=ModelGatewayFailure(code="rate_limited", phase="provider", outcome="known_failure",
                                        retryable=True, http_status=429),
        )
        before = durable_state(store)
        older, latest = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (older.source_fencing_generation, older.status) == (1, "already_admitted")
        assert older.successor == accepted.accepted_route
        assert older.authorization_hash == case.plan.authorization.authorization_hash
        assert (latest.source_fencing_generation, latest.status, latest.reason) == (
            2, "ready", "bounded_same_model_retry",
        )
        assert latest.successor is None and latest.authorization_hash == plan.authorization.authorization_hash
        assert latest.source_decision_hash == decision.decision_hash
        assert durable_state(store) == before
        _, _, third = consume_recovery(
            case, source_decision=decision, plan=plan, retry_level=2,
            exhausted_model_ids=("model-1",),
        )
        after_third = durable_state(store)
        older, latest = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (older.status, latest.status) == ("already_admitted", "already_admitted")
        assert older.successor == accepted.accepted_route
        assert latest.successor == third.accepted_route
        assert (older.successor.attempt_id, latest.successor.attempt_id) == (
            "attempt-node-1-2", "attempt-node-1-3",
        )
        assert durable_state(store) == after_third


def test_internal_record_retains_exact_source_and_successor_events(tmp_path):
    # Catches discarding the durable join needed by the later continuation transaction.
    from orchestrator.runtime.recovery_opportunities import load_recovery_records

    with SQLiteEventStore(tmp_path / "record.db") as store:
        case = recovery_case(store)
        _, _, accepted = consume_recovery(case)
        before = durable_state(store)
        records = []

        def read_under_lock(events, _version):
            records.extend(load_recovery_records(case.scheduler, run_id="run-1", events=events))
            return None

        store.append_checked("scheduler", "global", "test-recovery-records", read_under_lock)
        record, = records
        assert record.source_event.event_type == "RoutingDecisionAccepted"
        assert record.source_event.attempt_id == "attempt-node-1-1"
        assert record.successor_event.event_type == "RoutingDecisionAccepted"
        assert record.successor_event.attempt_id == "attempt-node-1-2"
        assert record.successor_event.payload["recovery_authorization_hash"] == record.opportunity.authorization_hash
        assert record.opportunity.successor == accepted.accepted_route
        assert record.plan == case.plan
        assert record.classification.disposition == "classified"
        assert durable_state(store) == before


@pytest.mark.parametrize("code,reason", [
    ("authentication_failed", "failure_not_recoverable"),
    ("context_length_exceeded", "capability_escalation_not_configured"),
])
def test_blocked_plans_preserve_durable_reason(tmp_path, code, reason):
    # Catches flattening an explicit persisted blocked reason into generic readiness.
    with SQLiteEventStore(tmp_path / "blocked.db") as store:
        case = recovery_case(store, failure_code=code)
        before = durable_state(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == ("blocked", reason)
        assert item.action is None and item.authorization_hash is None and item.successor is None
        assert durable_state(store) == before
