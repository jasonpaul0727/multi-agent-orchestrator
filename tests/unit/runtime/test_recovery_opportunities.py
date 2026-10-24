"""Recovery discovery reads current durable authority without executing work."""

import subprocess
from datetime import timedelta
import json
import traceback

import pytest

from orchestrator.persistence import SQLiteEventStore
from tests.support.recovery_continuation import (
    consume_recovery, duplicate_event, durable_state, recovery_case, rewrite_event_payload,
)
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


_PRIVATE = "private_input_sentinel_c87f"


def assert_owned_error(error, code="recovery_continuation_failed"):
    assert type(error).__name__ == "RecoveryContinuationError"
    assert error.code == code and error.args == (code,) and vars(error) == {"code": code}
    assert error.__cause__ is None and error.__context__ is None
    assert _PRIVATE not in "".join(traceback.format_exception(error))


@pytest.mark.parametrize("role", ["reviewer", "director"])
def test_role_plan_from_real_producer_reopens_for_new_child_only(tmp_path, role):
    # Catches replay rejecting valid producer output, or accidentally admitting its role authorization.
    from orchestrator.scheduler import SchedulerError

    database = tmp_path / "role-plan.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store, failure_code="context_length_exceeded", escalation_role=role)
        assert (case.plan.outcome, case.plan.action, case.plan.target_role) == (
            "new_child_required", role + "_node", role,
        )
        before = tuple(store._connection.iterdump())
    with SQLiteEventStore(database) as store:
        control = scheduler(store)
        assert control.recovery.recover("run-1").active_attempts == ()
        item, = service(control).list_opportunities(run_id="run-1")
        assert (item.status, item.reason, item.action) == (
            "new_child_required", "independent_role_node_required", role + "_node",
        )
        assert item.authorization_hash == case.plan.authorization.authorization_hash
        assert item.successor is None
        request, decision = routed_pair(
            case.registry, case.config, case.policy, attempt=2,
            contract_hash=case.contract.contract_hash, recovery_action=case.plan.action,
            recovery_authorization=case.plan.authorization,
            prior_decision_hash=case.decision.decision_hash, previous_model_id="model-1",
            failure_category="capability_failure", retry_level=1,
        )
        with pytest.raises(SchedulerError, match="graph or health-control"):
            accept(control, request, decision)
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize("role", ["reviewer", "director"])
def test_role_plan_malformed_authorization_is_rejected(tmp_path, role):
    # Catches relaxing source/evidence/authorization binding while admitting the supported plan shape.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "bad-role.db") as store:
        recovery_case(store, failure_code="context_length_exceeded", escalation_role=role)
        rewrite_event_payload(store, "RecoveryPlanCreated", lambda payload:
                              payload["plan"]["authorization"].update(evidence_hash="sha256:" + "0" * 64))
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(scheduler(store)).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before


def test_legacy_source_without_input_commitment_is_blocked(tmp_path):
    # Catches presenting a legacy accepted route as eligible for input-bound continuation.
    with SQLiteEventStore(tmp_path / "legacy.db") as store:
        case = recovery_case(store, legacy_input=True)
        before = durable_state(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == ("blocked", "source_input_unavailable")
        assert item.authorization_hash == case.plan.authorization.authorization_hash
        assert durable_state(store) == before


@pytest.mark.parametrize("field,value", [
    ("run_id", "other-run"), ("node_id", "other-node"), ("attempt_id", "other-attempt"),
    ("agent_instance_id", "other-agent"), ("fencing_generation", True),
    ("fencing_generation", 2), ("graph_version", True), ("graph_version", 2),
    ("input_manifest_hash", "sha256:" + "0" * 64),
    ("effective_config_hash", "sha256:" + "0" * 64),
    ("registry_hash", "sha256:" + "0" * 64), ("policy_manifest_hash", "sha256:" + "0" * 64),
    ("routing_decision_hash", "sha256:" + "0" * 64),
    ("planning_contract_hash", "sha256:" + "0" * 64), ("undeclared", _PRIVATE),
], ids=["run", "node", "attempt", "agent", "generation-bool", "generation-mismatch",
        "graph-bool", "graph-mismatch", "input-hash", "config-hash", "registry-hash",
        "policy-hash", "decision-hash", "contract-hash", "undeclared-field"])
def test_corrupt_historical_context_fails_closed_without_writes(tmp_path, field, value):
    # Catches trusting a valid-looking copied context instead of exact historical admission authority.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "bad-context.db") as store:
        case = recovery_case(store)
        rewrite_event_payload(store, "RoutingDecisionAccepted", lambda payload:
                              payload["verification_context"].update({field: value}))
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize("mutation", [
    "partial-commitment", "missing-context", "contract-id", "checks", "extra-commitment",
    "graph-version-bool", "route-decision", "provider-id", "source-generation-bool",
    "nested-route-bool", "source-extra",
])
def test_corrupt_source_commitment_is_owned_and_private(tmp_path, mutation):
    # Catches ignored source/admission fields and unsafe nested route payloads.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    def mutate(payload):
        if mutation == "partial-commitment":
            payload["verification_admission"] = None
        elif mutation == "missing-context":
            payload["verification_context"] = None
        elif mutation == "contract-id":
            payload["verification_contract"] = _PRIVATE
        elif mutation == "checks":
            payload["required_check_ids"] = [_PRIVATE]
        elif mutation == "extra-commitment":
            payload["verification_admission"][_PRIVATE] = _PRIVATE
        elif mutation == "graph-version-bool":
            payload["graph_version"] = True
        elif mutation == "route-decision":
            payload["accepted_route"]["decision_id"] = "other-decision"
        elif mutation == "provider-id":
            payload["provider_id"] = "other-provider"
        elif mutation == "source-generation-bool":
            payload["fencing_generation"] = True
        elif mutation == "nested-route-bool":
            payload["accepted_route"]["fencing_generation"] = True
        else:
            payload[_PRIVATE] = _PRIVATE

    with SQLiteEventStore(tmp_path / "bad-source.db") as store:
        case = recovery_case(store)
        rewrite_event_payload(store, "RoutingDecisionAccepted", mutate)
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize("target", ["plan", "classification"])
@pytest.mark.parametrize("reason", [_PRIVATE, _PRIVATE + "\nraw task", "x" * 1000],
                         ids=["private-code", "multiline", "oversized"])
def test_corrupt_persisted_reasons_do_not_escape(tmp_path, target, reason, caplog):
    # Catches echoing even syntactically valid arbitrary persisted reason strings as metadata.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "bad-reason.db") as store:
        case = recovery_case(store)
        event_type = "RecoveryPlanCreated" if target == "plan" else "AttemptFailureClassified"
        rewrite_event_payload(store, event_type, lambda payload: payload[target].update(reason=reason))
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert reason not in caplog.text
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize("event_type", ["RoutingDecisionAccepted", "AttemptFailureClassified", "RecoveryPlanCreated"])
def test_duplicate_durable_history_is_rejected_without_writes(tmp_path, event_type):
    # Catches lossy dictionary joins hiding duplicated durable evidence or authorization.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "duplicate.db") as store:
        case = recovery_case(store)
        duplicate_event(store, event_type)
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize("event_type", ["AttemptFailureClassified", "RecoveryPlanCreated"])
def test_orphaned_durable_recovery_history_is_rejected(tmp_path, event_type):
    # Catches joining source/evidence solely by node rather than exact persisted identity.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "orphan.db") as store:
        case = recovery_case(store)
        rewrite_event_payload(store, event_type, lambda payload: payload.update(attempt_ref="sha256:" + "0" * 64))
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before


def test_historical_graph_version_survives_later_graph_append(tmp_path):
    # Catches substituting the current graph for the source's frozen admission graph.
    from orchestrator.lifecycle import NodeSpec
    from tests.unit.lifecycle.test_scheduler import _fixture_contract

    with SQLiteEventStore(tmp_path / "graph.db") as store:
        case = recovery_case(store)
        extra = _fixture_contract(case.registry, case.config, case.policy, node_id="node-2")
        case.lifecycle.append_nodes("run-1", (NodeSpec(
            node_id="node-2", role="coder", planning_contract_hash=extra.contract_hash,
            planning_contract=extra,
        ),), expected_graph_version=1, idempotency_key="later-graph", policy_manifest=case.policy)
        before = durable_state(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert item.status == "ready"
        assert durable_state(store) == before


@pytest.mark.parametrize("run_id", ["", "bad id", "node\n", "a" * 129, True, 1, None])
def test_malformed_query_identity_is_owned_and_read_only(tmp_path, run_id):
    # Catches coercing or accepting caller-controlled identifiers before acquiring the snapshot.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "identity.db") as store:
        case = recovery_case(store)
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id=run_id)
        assert_owned_error(caught.value, "recovery_continuation_invalid")
        assert durable_state(store) == before


@pytest.mark.parametrize("mutation", ["generation", "extra-field", "model-extra"])
def test_opportunity_revalidates_unsafe_nested_route_model(tmp_path, mutation):
    # Catches Pydantic trusting unchecked nested model_copy data across the metadata boundary.
    from pydantic import ValidationError
    from orchestrator.runtime.recovery_opportunities import RecoveryOpportunity

    with SQLiteEventStore(tmp_path / "unsafe.db") as store:
        case = recovery_case(store)
        _, _, accepted = consume_recovery(case)
        unsafe = accepted.accepted_route.model_copy(update={
            "fencing_generation" if mutation == "generation" else "undeclared":
            True if mutation == "generation" else _PRIVATE,
        })
        if mutation == "model-extra":
            unsafe = accepted.accepted_route.model_copy()
            object.__setattr__(unsafe, "__pydantic_extra__", {"undeclared": _PRIVATE})
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        before = durable_state(store)
        with pytest.raises(ValidationError):
            RecoveryOpportunity(**{**item.model_dump(), "successor": unsafe})
        assert durable_state(store) == before


@pytest.mark.parametrize("outcome", [None, "unknown", "known_success"])
def test_current_unsettled_journal_preserves_holds_and_requires_reconciliation(tmp_path, outcome):
    # Catches reopening dispatch, unknown, or unaccounted success as a fresh continuation opportunity.
    from orchestrator.models.provider_calls import SQLiteProviderCallJournal

    database = tmp_path / "journal.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store)
        request = _provider_call_request_for_crash(case.accepted, case.registry)
        journal = SQLiteProviderCallJournal(store)
        journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
                              provider_correlation_id="current-call", request_body=b"{}")
        if outcome is not None:
            journal.record_outcome(request, outcome=outcome, http_status=200 if outcome == "known_success" else None)
        before = durable_state(store)
    with SQLiteEventStore(database) as store:
        item, = service(scheduler(store)).list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == ("reconciliation_required", "provider_outcome_unknown")
        assert durable_state(store) == before


def test_query_privacy_includes_metadata_events_logs_and_errors(tmp_path, caplog):
    # Catches exposing task/provider observations through public metadata or failure surfaces.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "privacy.db") as store:
        case = recovery_case(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert set(type(item).model_fields) == {
            "run_id", "node_id", "source_attempt_id", "source_fencing_generation", "source_decision_hash",
            "authorization_hash", "evidence_hash", "action", "status", "reason", "successor",
        }
        assert "implement a small change" not in repr(item) + json.dumps(item.model_dump(mode="json"))
        assert "implement a small change" not in repr(tuple(store._connection.iterdump()))
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id=_PRIVATE + " ")
        assert_owned_error(caught.value, "recovery_continuation_invalid")
        assert _PRIVATE not in caplog.text


@pytest.mark.parametrize("outcome", ["not_sent", "known_failure", "known_success"])
def test_accounted_or_known_failure_journal_does_not_block_retry(tmp_path, outcome):
    # Catches broad journal-status blocking that breaks ordinary settled retry paths.
    from orchestrator.models.provider_calls import SQLiteProviderCallJournal

    with SQLiteEventStore(tmp_path / "known-call.db") as store:
        case = recovery_case(store, accounted_usage=outcome == "known_success")
        request = _provider_call_request_for_crash(case.accepted, case.registry)
        journal = SQLiteProviderCallJournal(store)
        journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
                              provider_correlation_id="known-call", request_body=b"{}")
        journal.record_outcome(request, outcome=outcome, http_status=200 if outcome == "known_success" else None)
        before = durable_state(store)
        item, = service(case.scheduler).list_opportunities(run_id="run-1")
        assert (item.status, item.reason) == ("ready", "bounded_same_model_retry")
        assert durable_state(store) == before


@pytest.mark.parametrize("mutation", ["missing", "unsafe-field", "config-hash"])
def test_inconsistent_frozen_contract_is_owned_error(tmp_path, mutation):
    # Catches overlooking missing or changed frozen source authority after input was committed.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    def mutate(payload):
        contract = payload["nodes"][0]["planning_contract"]
        if mutation == "missing":
            payload["nodes"][0]["planning_contract"] = None
        elif mutation == "unsafe-field":
            contract["context_tokens"] = True
        else:
            contract["config_hash"] = "sha256:" + "0" * 64

    with SQLiteEventStore(tmp_path / "contract.db") as store:
        case = recovery_case(store)
        rewrite_event_payload(store, "GraphNodesAppended", mutate,
                              stream_type="run_lifecycle", stream_id="run-1")
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize("mutation", ["unknown-event", "bool-generation", "source-route"])
def test_malformed_current_journal_is_sanitized(tmp_path, mutation):
    # Catches trusting journal identities without validating current serialized durable facts.
    from orchestrator.models.provider_calls import SQLiteProviderCallJournal
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "bad-journal.db") as store:
        case = recovery_case(store)
        request = _provider_call_request_for_crash(case.accepted, case.registry)
        stream_id = SQLiteProviderCallJournal(store).record_intent(
            request, provider_id="primary", provider_adapter="openai_responses",
            provider_correlation_id="bad-call", request_body=b"{}",
        )
        field, value = {
            "unknown-event": (_PRIVATE, _PRIVATE), "bool-generation": ("fencing_generation", True),
            "source-route": ("accepted_route_id", "other-route"),
        }[mutation]
        rewrite_event_payload(store, "ProviderCallIntentRecorded", lambda payload: payload.update({field: value}),
                              stream_type="provider_call", stream_id=stream_id)
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize("field", ["evidence_hash", "policy_manifest_hash"])
def test_well_formed_but_misbound_authorization_is_rejected(tmp_path, field):
    # Catches accepting a freshly hashed authorization that references the wrong source commitment.
    from orchestrator.routing import RecoveryAuthorization
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    with SQLiteEventStore(tmp_path / "misbound.db") as store:
        case = recovery_case(store)

        def mutate(payload):
            values = dict(payload["plan"]["authorization"])
            values.pop("authorization_hash")
            values[field] = "sha256:" + "0" * 64
            payload["plan"]["authorization"] = RecoveryAuthorization.create(**values).model_dump(mode="json")

        rewrite_event_payload(store, "RecoveryPlanCreated", mutate)
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before


@pytest.mark.parametrize("field,value", [
    ("failure_code", "context_length_exceeded"), ("retry_level", 1),
    ("failure_category", "capability_failure"), ("retry_safe", False),
    ("failed_model_unavailable", True), ("provider_code_hash", "sha256:" + "0" * 64),
    ("exhausted_model_ids", ["model-1"]),
], ids=["failure-code", "retry-counter", "category", "retry-safety", "availability", "provider-code-hash", "exhaustion"])
def test_classification_must_match_committed_failure_and_source_counters(tmp_path, field, value):
    # Catches well-typed evidence edits that preserve the old authorization/evidence hash.
    from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError

    def mutate(payload):
        if field == "failure_code":
            payload["failure"]["code"] = value
        elif field == "provider_code_hash":
            payload[field] = value
        else:
            payload["classification"]["evidence"][field] = value

    with SQLiteEventStore(tmp_path / "failure-binding.db") as store:
        case = recovery_case(store)
        rewrite_event_payload(store, "AttemptFailureClassified", mutate)
        before = tuple(store._connection.iterdump())
        with pytest.raises(RecoveryContinuationError) as caught:
            service(case.scheduler).list_opportunities(run_id="run-1")
        assert_owned_error(caught.value)
        assert tuple(store._connection.iterdump()) == before
