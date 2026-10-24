"""Atomic admission consumes durable recovery authority with explicit input."""

from datetime import timedelta
import hashlib
import json
import traceback

import pytest

from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError
from orchestrator.runtime.worker_input_service import WorkerInputService
from tests.support.recovery_continuation import (
    consume_recovery, durable_state, recovery_case, rewrite_event_payload, routing_observations,
)
from tests.unit.lifecycle.test_scheduler import NOW, routed_pair, scheduler


TEXT = "implement a small change"


def continuation(case):
    # Import inside the executing test so absence is a behavioral RED.
    from orchestrator.runtime import recovery_continuation
    return recovery_continuation.RecoveryContinuationService(
        case.scheduler, WorkerInputService(case.scheduler),
    )


def admit(case, **changes):
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    values = dict(
        run_id="run-1", node_id="node-1", source_attempt_id="attempt-node-1-1",
        source_fencing_generation=1, task_text=TEXT,
        routing_inputs=RecoveryRoutingInputs(**routing_observations(case)),
        accepted_at=NOW + timedelta(seconds=7), lease_expires_at=NOW + timedelta(minutes=2),
    )
    values.update(changes)
    return continuation(case).admit(**values)


def test_known_failure_admits_derived_successor_once(tmp_path):
    # Catches duplicate admission/holds, synthetic routes, wrong IDs or prompt persistence.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        original = tuple(store.read_stream("scheduler", "global"))
        result = admit(case)
        assert result.status == "admitted" and result.task is None
        route = result.accepted_route
        digest = hashlib.sha256(json.dumps({
            "schema": "maestro.recovery-successor.v1", "run_id": "run-1", "node_id": "node-1",
            "source_attempt_id": "attempt-node-1-1", "source_fencing_generation": 1,
            "authorization_hash": case.plan.authorization.authorization_hash,
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        assert route.attempt_id == "recovery-attempt-" + digest
        assert (route.fencing_generation, route.model_id, route.provider_id) == (2, "model-1", "primary")
        events = store.read_stream("scheduler", "global")
        assert tuple(events[:len(original)]) == original
        assert len(events) == len(original) + 1
        successor = events[-1]
        assert successor.event_type == "RoutingDecisionAccepted"
        assert successor.idempotency_key == "accept:recovery-request-" + digest
        assert successor.payload["accepted_route"] == route.model_dump(mode="json")
        assert successor.payload["recovery_authorization_hash"] == case.plan.authorization.authorization_hash
        assert successor.payload["verification_admission"] == original[0].payload["verification_admission"]
        assert (successor.payload["retry_level"], successor.payload["exhausted_model_ids"],
                successor.payload["failure_category"]) == (1, [], "transient")
        assert len([e for e in store.read_stream("budget", "run-1") if e.event_type == "BudgetReserved"]) == 2
        agents = case.scheduler.agents.replay("run-1")
        assert (agents.total_created, agents.active_count) == (2, 1)
        active, = case.scheduler.recovery.recover("run-1").active_attempts
        assert active.attempt_id == route.attempt_id
        before = durable_state(store)
        repeated = admit(case)
        assert repeated.status == "already_admitted" and repeated.accepted_route == route
        assert durable_state(store) == before
        assert TEXT not in repr(result) and TEXT not in "\n".join(store._connection.iterdump())


@pytest.mark.parametrize("text", [TEXT + " ", "implement a different change", " ", "\ud800"],
                         ids=["trailing-space", "different", "blank", "surrogate"])
def test_changed_input_cannot_admit(tmp_path, text):
    # Catches normalization or classification standing in for exact source input equality.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError) as caught:
            admit(case, task_text=text)
        assert caught.value.code in {"recovery_input_invalid", "recovery_input_mismatch"}
        assert durable_state(store) == before
        assert "implement a different change" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("missing,status", [("task_text", "waiting_for_input"), ("routing_inputs", "waiting_for_routing")])
def test_missing_input_or_routing_waits_without_effects(tmp_path, missing, status):
    # Catches acquiring a hold before all explicit admission inputs arrive.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        before = durable_state(store)
        result = admit(case, **{missing: None})
        assert result.status == status and result.task is None and result.accepted_route is None
        assert durable_state(store) == before


@pytest.mark.parametrize("field,value", [
    ("retry_level", True), ("exhausted_model_ids", ["model-1", "model-1"]),
    ("verification_context", None), ("graph_version", 99),
])
def test_corrupt_source_counters_or_frozen_binding_cannot_admit(tmp_path, field, value):
    # Catches trusting a source without Task 1's exact historical validation.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        rewrite_event_payload(store, "RoutingDecisionAccepted", lambda p: p.update({field: value}))
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError):
            admit(case)
        assert durable_state(store) == before


def test_nested_acceptance_failure_rolls_back_all_resources(tmp_path, monkeypatch):
    # Catches the outer snapshot failing to roll back nested Scheduler writes.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        before = durable_state(store)
        register = case.scheduler.agents.register_attempt

        def fail_after_register(*args, **kwargs):
            register(*args, **kwargs)
            raise RuntimeError("opaque failure")

        monkeypatch.setattr(case.scheduler.agents, "register_attempt", fail_after_register)
        with pytest.raises(RecoveryContinuationError):
            admit(case)
        assert durable_state(store) == before


@pytest.mark.parametrize("code,escalate,action,model", [
    ("provider_unavailable", None, "same_tier_fallback", "model-2"),
    ("context_length_exceeded", "tier:standard", "capability_escalation", "model-3"),
])
def test_real_router_admits_fallback_and_escalation(tmp_path, code, escalate, action, model):
    # Catches reusing the failed model or ignoring the persisted authorized candidates.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True, multi_model=True,
                             failure_code=code, escalation_role=escalate)
        assert case.decision.selected_model_id == "model-1"
        assert len(case.decision.candidate_assessments) == (3 if escalate is None else 2)
        assert case.plan.action == action
        source_events = tuple(store.read_stream("scheduler", "global"))
        result = admit(case)
        assert (result.status, result.accepted_route.model_id) == ("admitted", model)
        event = store.read_stream("scheduler", "global")[-1]
        assert event.payload["recovery_action"] == action
        assert event.payload["prior_decision_hash"] == case.decision.decision_hash
        assert event.payload["recovery_authorization_hash"] == case.plan.authorization.authorization_hash
        assert tuple(store.read_stream("scheduler", "global")[:-1]) == source_events
        assert case.scheduler.agents.replay("run-1").total_created == 2
        before = durable_state(store)
        assert admit(case, routing_inputs=None).accepted_route == result.accepted_route
        assert durable_state(store) == before


@pytest.mark.parametrize("legacy", [False, True], ids=["derived", "existing-scheduler-api"])
def test_committed_response_loss_replays_exact_identity_before_routing(tmp_path, monkeypatch, legacy):
    # Catches duplicate holds after lost response, new observations/time or legacy consumed IDs.
    database = tmp_path / "replay.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store, routing_budget=True)
        if legacy:
            _, _, accepted = consume_recovery(case)
            route = accepted.accepted_route
        else:
            append = store.append_checked

            def lose_outer_response(stream_type, stream_id, key, decide):
                result = append(stream_type, stream_id, key, decide)
                if key == "recovery-continuation-snapshot":
                    raise RuntimeError("response lost after commit")
                return result

            with monkeypatch.context() as patch:
                patch.setattr(store, "append_checked", lose_outer_response)
                with pytest.raises(RecoveryContinuationError):
                    admit(case)
            from orchestrator.models import AcceptedModelRoute
            route = AcceptedModelRoute.model_validate(
                store.read_stream("scheduler", "global")[-1].payload["accepted_route"],
            )
        before = durable_state(store)
    with SQLiteEventStore(database) as reopened:
        case.store, case.scheduler = reopened, scheduler(reopened)
        from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
        observations = routing_observations(case)
        observations["eligibility"] = observations["eligibility"].model_copy(update={
            "available_tokens": 0, "authorization_revoked": True,
        })
        for routing in (None, RecoveryRoutingInputs(**observations)):
            result = admit(case, routing_inputs=routing, accepted_at=NOW + timedelta(seconds=20),
                           lease_expires_at=NOW + timedelta(minutes=4))
            assert result.status == "already_admitted" and result.accepted_route == route
            assert result.task is None and result.opportunity.successor == route
            assert durable_state(reopened) == before
        waiting = admit(case, task_text=None, routing_inputs=None)
        assert waiting.status == "waiting_for_input" and waiting.accepted_route == route
        with pytest.raises(RecoveryContinuationError) as caught:
            admit(case, task_text="different explicit input", routing_inputs=None)
        assert caught.value.code == "recovery_input_mismatch"
        assert durable_state(reopened) == before


@pytest.mark.parametrize("state", ["failed", "paused", "cancelling", "expired", "unknown", "newer"])
def test_consumed_inactive_successor_keeps_identity_without_stale_authority(tmp_path, state):
    # Catches historical consumption being returned as current active authority.
    with SQLiteEventStore(tmp_path / "inactive.db") as store:
        case = recovery_case(store, routing_budget=True)
        _, _, accepted = consume_recovery(case)
        as_of = NOW + timedelta(seconds=20)
        if state in {"failed", "unknown", "newer"}:
            case.scheduler.finish_attempt(
                run_id="run-1", node_id="node-1", attempt_id=accepted.accepted_route.attempt_id,
                fencing_generation=2, completed_at=NOW + timedelta(seconds=12),
                outcome="outcome_unknown" if state == "unknown" else "failed",
                known_no_effect=state != "unknown",
            )
        if state == "paused":
            case.lifecycle.pause_run("run-1", reason_code="user_request")
        elif state == "cancelling":
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
        elif state == "expired":
            as_of = NOW + timedelta(minutes=2)
        elif state == "newer":
            request, decision = routed_pair(case.registry, case.config, case.policy, attempt=3,
                                            contract_hash=case.contract.contract_hash)
            WorkerInputService(case.scheduler).accept_routing_with_input(
                request, decision, task_text=TEXT, accepted_at=NOW + timedelta(seconds=15),
                lease_expires_at=NOW + timedelta(minutes=3),
            )
        before = durable_state(store)
        result = admit(case, routing_inputs=None, accepted_at=as_of,
                       lease_expires_at=NOW + timedelta(minutes=4))
        assert result.status == "successor_inactive"
        assert result.accepted_route == accepted.accepted_route and result.task is None
        assert result.opportunity.successor == accepted.accepted_route
        assert durable_state(store) == before


def test_later_same_model_retry_preserves_exhausted_model_safety_boundary(tmp_path):
    # Catches bypassing Router's conservative exhausted-model rule to force a retry.
    from orchestrator.models import ModelGatewayFailure
    from orchestrator.runtime.recovery_opportunities import RecoveryOpportunityService

    with SQLiteEventStore(tmp_path / "retry.db") as store:
        case = recovery_case(store, routing_budget=True)
        request, decision, _ = consume_recovery(case)
        case.scheduler.finish_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=2, completed_at=NOW + timedelta(seconds=12),
            outcome="failed", known_no_effect=True,
        )
        plan = case.scheduler.plan_attempt_recovery(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=2, source_decision=decision, contract=case.contract,
            failure=ModelGatewayFailure(code="rate_limited", phase="provider",
                                       outcome="known_failure", retryable=True, http_status=429),
        )
        assert plan.action == "same_model_retry"
        latest = RecoveryOpportunityService(case.scheduler).list_opportunities(run_id="run-1")[-1]
        assert latest.status == "ready"
        before = durable_state(store)
        result = admit(case, source_attempt_id=request.attempt_id, source_fencing_generation=2,
                       accepted_at=NOW + timedelta(seconds=15))
        assert result.status == "blocked" and result.accepted_route is None
        assert durable_state(store) == before
        classified = store.read_stream("scheduler", "global")[-2]
        evidence = classified.payload["classification"]["evidence"]
        assert (evidence["retry_level"], evidence["exhausted_model_ids"]) == (1, ["model-1"])


@pytest.mark.parametrize("field,value", [
    ("budget_ledger_version", 0), ("budget_currency", "EUR"),
    ("available_cost_minor", 99), ("available_tokens", 4999), ("available_tokens", None),
], ids=["version", "currency", "cost", "tokens", "unbounded-tokens"])
def test_routing_budget_observations_must_match_current_ledger(tmp_path, field, value):
    # Catches accepting a stale or inflated dynamic budget view.
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    with SQLiteEventStore(tmp_path / "stale-budget.db") as store:
        case = recovery_case(store, routing_budget=True)
        observations = routing_observations(case)
        observations["eligibility"] = observations["eligibility"].model_copy(update={field: value})
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError):
            admit(case, routing_inputs=RecoveryRoutingInputs(**observations))
        assert durable_state(store) == before


@pytest.mark.parametrize("scope", ["provider", "model"])
def test_health_references_are_rechecked_against_durable_current_state(tmp_path, scope):
    # Catches healthy-looking references that predate a committed health update.
    from orchestrator.routing import HealthAggregateKey, HealthController
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    with SQLiteEventStore(tmp_path / "health.db") as store:
        case = recovery_case(store, routing_budget=True)
        observations = routing_observations(case)
        policy = case.config.health_policies[case.config.presets[case.config.active_preset].health_policy_ref]
        provider = HealthAggregateKey(registry_manifest_hash=case.registry.content_hash,
                                      scope="provider", provider_id="primary")
        model = HealthAggregateKey(registry_manifest_hash=case.registry.content_hash,
                                   scope="model", provider_id="primary", model_id="model-1")
        HealthController(store).record_outcome(
            provider_key=provider, model_key=model, policy=policy, event_time=NOW,
            idempotency_key="fresh-health", success=True,
        )
        current = routing_observations(case)["eligibility"].model_health[0]
        old = observations["eligibility"].model_health[0]
        stale = current.model_copy(update={scope: getattr(old, scope)})
        observations["eligibility"] = observations["eligibility"].model_copy(update={"model_health": (stale,)})
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError):
            admit(case, routing_inputs=RecoveryRoutingInputs(**observations))
        assert durable_state(store) == before


@pytest.mark.parametrize("change", ["revoked", "denied", "credential", "tokenizer", "fx", "policy"])
def test_router_evaluates_current_trusted_host_policy_and_costing_facts(tmp_path, change):
    # Catches bypassing real Router policy, credential metadata, or costing expiry.
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    with SQLiteEventStore(tmp_path / "facts.db") as store:
        case = recovery_case(store, routing_budget=True)
        observations = routing_observations(case)
        eligibility = observations["eligibility"]
        if change == "revoked":
            observations["eligibility"] = eligibility.model_copy(update={"authorization_revoked": True})
        elif change == "denied":
            observations["eligibility"] = eligibility.model_copy(update={"emergency_denied_model_ids": ("model-1",)})
        elif change == "credential":
            unavailable = eligibility.secret_availability[0].model_copy(update={"available": False})
            observations["eligibility"] = eligibility.model_copy(update={"secret_availability": (unavailable,)})
        elif change == "tokenizer":
            expired = observations["tokenizer_snapshot"].bindings[0].model_copy(update={"expires_at": NOW})
            observations["tokenizer_snapshot"] = observations["tokenizer_snapshot"].model_copy(update={"bindings": (expired,)})
        elif change == "fx":
            observations["fx_snapshot"] = observations["fx_snapshot"].model_copy(update={"expires_at": NOW})
        else:
            observations["eligibility"] = eligibility.model_copy(update={"policy_manifest_hash": "sha256:" + "0" * 64})
        before = durable_state(store)
        if change == "policy":
            with pytest.raises(RecoveryContinuationError):
                admit(case, routing_inputs=RecoveryRoutingInputs(**observations))
        else:
            result = admit(case, routing_inputs=RecoveryRoutingInputs(**observations))
            assert result.status == "blocked" and result.accepted_route is None
        assert durable_state(store) == before


@pytest.mark.parametrize("kind", ["dispatching", "unknown", "known_success", "effect"])
def test_current_unknown_outcomes_prevent_admission_after_prior_ready_query(tmp_path, kind):
    # Catches using prior ready inventory after a new unresolved durable operation.
    from orchestrator.models.provider_calls import SQLiteProviderCallJournal
    from orchestrator.persistence import EventDraft
    from orchestrator.runtime.recovery_opportunities import RecoveryOpportunityService
    from tests.unit.lifecycle.test_scheduler import _provider_call_request_for_crash
    with SQLiteEventStore(tmp_path / "outcomes.db") as store:
        case = recovery_case(store, routing_budget=True)
        assert RecoveryOpportunityService(case.scheduler).list_opportunities(run_id="run-1")[0].status == "ready"
        if kind == "effect":
            request, decision = routed_pair(case.registry, case.config, case.policy, attempt=2,
                                            contract_hash=case.contract.contract_hash)
            WorkerInputService(case.scheduler).accept_routing_with_input(
                request, decision, task_text=TEXT, accepted_at=NOW + timedelta(seconds=6),
                lease_expires_at=NOW + timedelta(minutes=2),
            )
            store.append("budget", "run-1", store.current_version("budget", "run-1"), [EventDraft(
                "EffectIntentRecorded", {"effect_id": "effect-1", "recovery_class": "queryable",
                                         "provider_idempotency_key": "opaque-effect-key"},
                run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
                fencing_generation=2, causation_id=decision.decision_hash,
            )], "new-effect-intent")
            active = case.scheduler.resolve_active_attempt(
                run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
                fencing_generation=2, as_of=NOW + timedelta(seconds=7),
            )
            assert active.accepted_route.attempt_id == request.attempt_id
            assert active.reservation.status == "reserved"
            assert case.scheduler.agents.replay("run-1").for_attempt(request.attempt_id).status == "active"
        else:
            journal = SQLiteProviderCallJournal(store)
            request = _provider_call_request_for_crash(case.accepted, case.registry)
            journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
                                  provider_correlation_id="bounded-call-1", request_body=b"{}")
            if kind != "dispatching":
                journal.record_outcome(request, outcome=kind, http_status=200 if kind == "known_success" else None)
        before = durable_state(store)
        result = admit(case)
        assert result.status == "reconciliation_required" and result.accepted_route is None
        assert durable_state(store) == before


@pytest.mark.parametrize("cap,value", [("max_agents", 1), ("max_attempts", 1), ("max_total_tokens", 100)])
def test_frozen_attempt_agent_and_token_caps_leave_no_partial_admission(tmp_path, cap, value):
    # Catches bypassing caps or leaving partial resources after a nested rejection.
    with SQLiteEventStore(tmp_path / "caps.db") as store:
        case = recovery_case(store, routing_budget=True, limits={cap: value})
        before = durable_state(store)
        if cap in {"max_total_tokens", "max_attempts"}:
            assert admit(case).status == "blocked"
        else:
            with pytest.raises(RecoveryContinuationError):
                admit(case)
        assert durable_state(store) == before


def test_current_cost_budget_exhaustion_blocks_real_priced_successor(tmp_path):
    # Catches overlooking accounted usage when a real priced candidate needs a hold.
    with SQLiteEventStore(tmp_path / "cost.db") as store:
        case = recovery_case(store, routing_budget=True, priced=True,
                             accounted_usage=True, accounted_cost=100)
        assert case.accepted.reservation.reserved_minor == 15
        assert routing_observations(case)["eligibility"].available_cost_minor == 0
        before = durable_state(store)
        assert admit(case).status == "blocked"
        assert durable_state(store) == before


@pytest.mark.parametrize("scope", ["system", "run", "provider", "tool", "frozen_run"])
def test_current_concurrency_caps_prevent_successor_without_partial_holds(tmp_path, scope):
    # Catches checking resource availability before another node consumes the slot.
    from orchestrator.lifecycle import NodeSpec
    from tests.unit.lifecycle.test_scheduler import _fixture_contract
    with SQLiteEventStore(tmp_path / "concurrency.db") as store:
        case = recovery_case(store, routing_budget=True, tool_ids=("read_file",),
                             limits={"max_concurrency": 1} if scope == "frozen_run" else None)
        contract = _fixture_contract(case.registry, case.config, case.policy, node_id="node-2")
        state = case.lifecycle.replay("run-1")
        case.lifecycle.append_nodes("run-1", (NodeSpec(
            node_id="node-2", role="coder", planning_contract_hash=contract.contract_hash,
            planning_contract=contract, tool_ids=("read_file",),
        ),), expected_graph_version=state.graph_version, idempotency_key="other-node", policy_manifest=case.policy)
        case.scheduler = scheduler(store, **({scope: 1} if scope != "frozen_run" else {}))
        request, decision = routed_pair(case.registry, case.config, case.policy, node_id="node-2",
                                        contract_hash=contract.contract_hash)
        WorkerInputService(case.scheduler).accept_routing_with_input(
            request, decision, task_text=TEXT, accepted_at=NOW + timedelta(seconds=6),
            lease_expires_at=NOW + timedelta(minutes=2),
        )
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError):
            admit(case)
        assert durable_state(store) == before


def test_same_node_recovery_preserves_frozen_zero_depth_limit(tmp_path):
    # Catches treating same-node retry as a deeper child Agent.
    with SQLiteEventStore(tmp_path / "depth.db") as store:
        case = recovery_case(store, routing_budget=True, limits={"max_depth": 0})
        assert admit(case).status == "admitted"
        assert [agent.depth for agent in case.scheduler.agents.replay("run-1").instances] == [0, 0]


@pytest.mark.parametrize("transition", ["pause", "cancel"])
def test_current_run_authority_blocks_unconsumed_recovery(tmp_path, transition):
    # Catches using persisted retry authorization after Run scheduling is stopped.
    with SQLiteEventStore(tmp_path / "run.db") as store:
        case = recovery_case(store, routing_budget=True)
        if transition == "pause":
            case.lifecycle.pause_run("run-1", reason_code="user_request")
        else:
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
            case.lifecycle.complete_cancellation_if_idle("run-1")
        before = durable_state(store)
        result = admit(case)
        assert result.status == "blocked" and result.accepted_route is None
        assert durable_state(store) == before


@pytest.mark.parametrize("location", ["envelope", "eligibility", "health", "tokenizer", "fx", "policy", "subclass", "boolean"])
def test_unsafe_observation_models_fail_closed_without_payload_leak(tmp_path, location):
    # Catches unsafe-copy extras/subclasses being silently stripped at revalidation.
    from orchestrator.routing import EligibilitySnapshot
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    marker = "bounded-private-payload"
    with SQLiteEventStore(tmp_path / "unsafe.db") as store:
        case = recovery_case(store, routing_budget=True)
        observations = routing_observations(case)
        if location == "envelope":
            supplied = RecoveryRoutingInputs(**observations).model_copy(update={"opaque_extra": marker})
        else:
            if location == "eligibility":
                observations["eligibility"] = observations["eligibility"].model_copy(update={"opaque_extra": marker})
            elif location == "health":
                entry = observations["eligibility"].model_health[0]
                entry = entry.model_copy(update={"model": entry.model.model_copy(update={"opaque_extra": marker})})
                observations["eligibility"] = observations["eligibility"].model_copy(update={"model_health": (entry,)})
            elif location == "tokenizer":
                binding = observations["tokenizer_snapshot"].bindings[0].model_copy(update={"opaque_extra": marker})
                observations["tokenizer_snapshot"] = observations["tokenizer_snapshot"].model_copy(update={"bindings": (binding,)})
            elif location == "fx":
                observations["fx_snapshot"] = observations["fx_snapshot"].model_copy(update={"opaque_extra": marker})
            elif location == "boolean":
                observations["eligibility"] = observations["eligibility"].model_copy(update={"available_cost_minor": True})
            elif location == "policy":
                authority = observations["policy_manifest"].authorities[0].model_copy(update={"opaque_extra": marker})
                observations["policy_manifest"] = observations["policy_manifest"].model_copy(update={"authorities": (authority,)})
            else:
                class ExtendedEligibility(EligibilitySnapshot):
                    opaque_extra: str = marker
                observations["eligibility"] = ExtendedEligibility(**observations["eligibility"].model_dump())
            supplied = RecoveryRoutingInputs(**observations)
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError) as caught:
            admit(case, routing_inputs=supplied)
        assert marker not in "".join(traceback.format_exception(caught.value))
        assert caught.value.__context__ is None and caught.value.__cause__ is None
        assert durable_state(store) == before


def test_admission_executes_no_worker_provider_or_credential_effect(tmp_path, monkeypatch, caplog, capsys):
    # Catches crossing the admission-only boundary or logging re-supplied task text.
    import subprocess
    from orchestrator.models.gateway import ModelGateway
    from orchestrator.models.transport import UrllibHTTPSTransport
    from orchestrator.runtime.worker_process import IsolatedWorkerProcess
    from orchestrator.secrets.broker import AuditedSecretBroker
    with SQLiteEventStore(tmp_path / "no-effects.db") as store:
        case = recovery_case(store, routing_budget=True)
        def forbidden(*args, **kwargs):
            pytest.fail("admission invoked a forbidden external boundary")
        for target, name in ((subprocess, "Popen"), (ModelGateway, "invoke"),
                             (UrllibHTTPSTransport, "post_json"), (IsolatedWorkerProcess, "execute"),
                             (AuditedSecretBroker, "acquire_provider_credential_decision"),
                             (WorkerInputService, "prepare_worker_task")):
            monkeypatch.setattr(target, name, forbidden)
        result = admit(case)
        assert result.status == "admitted" and result.task is None
        captured = capsys.readouterr()
        assert TEXT not in captured.out + captured.err + caplog.text + repr(result)


def test_current_open_health_circuit_blocks_even_with_matching_refs(tmp_path):
    # Catches treating fresh observations as proof that the model is eligible.
    from orchestrator.routing import HealthAggregateKey, HealthController
    with SQLiteEventStore(tmp_path / "open-health.db") as store:
        case = recovery_case(store, routing_budget=True)
        policy = case.config.health_policies[case.config.presets[case.config.active_preset].health_policy_ref]
        provider = HealthAggregateKey(registry_manifest_hash=case.registry.content_hash,
                                      scope="provider", provider_id="primary")
        model = HealthAggregateKey(registry_manifest_hash=case.registry.content_hash,
                                   scope="model", provider_id="primary", model_id="model-1")
        for index in range(4):
            HealthController(store).record_outcome(
                provider_key=provider, model_key=model, policy=policy,
                event_time=NOW + timedelta(seconds=index), idempotency_key=f"open-health-{index}", success=False,
            )
        assert routing_observations(case)["eligibility"].model_health[0].model.state == "open"
        before = durable_state(store)
        assert admit(case).status == "blocked"
        assert durable_state(store) == before


@pytest.mark.parametrize("changes", [
    {"run_id": "unsafe id"}, {"node_id": None}, {"source_attempt_id": "x" * 129},
    {"source_fencing_generation": True}, {"source_fencing_generation": 0},
    {"task_text": 123}, {"task_text": "x" * 100001},
], ids=["run", "node", "attempt", "boolean-generation", "zero-generation", "nontext", "large-text"])
def test_invalid_explicit_admission_inputs_are_owned_and_write_nothing(tmp_path, changes):
    # Catches malformed caller inputs reaching resource acquisition or leaking payloads.
    with SQLiteEventStore(tmp_path / "invalid.db") as store:
        case = recovery_case(store, routing_budget=True)
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError) as caught:
            admit(case, **changes)
        assert caught.value.code in {"recovery_continuation_invalid", "recovery_input_invalid"}
        assert caught.value.__context__ is None and caught.value.__cause__ is None
        assert durable_state(store) == before


@pytest.mark.parametrize("kind", ["nested-callback-model", "invalid-empty-extra-storage"])
def test_malformed_nested_data_never_executes_model_callbacks_under_lock(tmp_path, kind):
    # Catches a foreign BaseModel hidden in a primitive set reaching revalidation.
    from pydantic import BaseModel, ConfigDict, model_validator
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    calls = []
    class ForeignData(BaseModel):
        model_config = ConfigDict(frozen=True)
        @model_validator(mode="after")
        def callback(self):
            calls.append("unexpected-validation-callback")
            return self
    with SQLiteEventStore(tmp_path / "callback.db") as store:
        case = recovery_case(store, routing_budget=True)
        observations = routing_observations(case)
        if kind == "nested-callback-model":
            authority = observations["policy_manifest"].authorities[0].model_copy(update={
                "allowed_tools": frozenset({ForeignData.model_construct()}),
            })
            observations["policy_manifest"] = observations["policy_manifest"].model_copy(update={"authorities": (authority,)})
        else:
            object.__setattr__(observations["eligibility"], "__pydantic_extra__", [])
        supplied = RecoveryRoutingInputs(**observations)
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError):
            admit(case, routing_inputs=supplied)
        assert calls == []
        assert durable_state(store) == before


@pytest.mark.parametrize("location", [
    "accepted_at", "lease_expires_at", "tokenizer-effective", "tokenizer-expiry", "fx-effective", "fx-expiry",
])
@pytest.mark.parametrize("kind", ["custom-timezone", "datetime-subclass", "zoneinfo-subclass", "timezone-metaclass"])
def test_executable_datetime_objects_are_rejected_before_any_callback(tmp_path, location, kind):
    # Catches exact datetimes carrying executable tzinfo and timestamp subclasses.
    from datetime import datetime, tzinfo
    from zoneinfo import ZoneInfo
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    calls = []
    with SQLiteEventStore(tmp_path / "timezone.db") as store:
        case = recovery_case(store, routing_budget=True)
        class CallbackTimezone(tzinfo):
            def utcoffset(self, value):
                calls.append(("custom-offset", store._connection.in_transaction))
                return timedelta(0)
            def dst(self, value):
                calls.append(("custom-dst", store._connection.in_transaction))
                return timedelta(0)
            def tzname(self, value):
                calls.append(("custom-name", store._connection.in_transaction))
                return "timezone-private-payload"
        class CallbackZoneInfo(ZoneInfo):
            def utcoffset(self, value):
                calls.append(("zoneinfo-offset", store._connection.in_transaction))
                return timedelta(0)
        class CallbackMeta(type):
            def __hash__(cls):
                calls.append(("timezone-class-hash", store._connection.in_transaction))
                return super().__hash__()
        class MetaTimezone(CallbackTimezone, metaclass=CallbackMeta):
            pass
        class CallbackDatetime(datetime):
            def __getattribute__(self, name):
                if name in {"tzinfo", "utcoffset", "isoformat"}:
                    calls.append(("datetime-access", store._connection.in_transaction))
                return super().__getattribute__(name)
            def utcoffset(self):
                calls.append(("datetime-offset", store._connection.in_transaction))
                return timedelta(0)
        observations = routing_observations(case)
        if location in {"accepted_at", "lease_expires_at"}:
            original = NOW + (timedelta(seconds=7) if location == "accepted_at" else timedelta(minutes=2))
        elif location.startswith("tokenizer"):
            field = "effective_from" if location.endswith("effective") else "expires_at"
            original = getattr(observations["tokenizer_snapshot"].bindings[0], field)
        else:
            field = "effective_from" if location.endswith("effective") else "expires_at"
            original = getattr(observations["fx_snapshot"], field)
        if kind == "datetime-subclass":
            poisoned = CallbackDatetime(original.year, original.month, original.day, original.hour,
                                        original.minute, original.second, tzinfo=original.tzinfo)
        else:
            zone = {"custom-timezone": CallbackTimezone, "zoneinfo-subclass": lambda: CallbackZoneInfo("UTC"),
                    "timezone-metaclass": MetaTimezone}[kind]()
            poisoned = original.replace(tzinfo=zone)
        changes = {}
        if location in {"accepted_at", "lease_expires_at"}:
            changes[location] = poisoned
        else:
            if location.startswith("tokenizer"):
                binding = observations["tokenizer_snapshot"].bindings[0].model_copy(update={field: poisoned})
                observations["tokenizer_snapshot"] = observations["tokenizer_snapshot"].model_copy(update={"bindings": (binding,)})
            else:
                observations["fx_snapshot"] = observations["fx_snapshot"].model_copy(update={field: poisoned})
            changes["routing_inputs"] = RecoveryRoutingInputs(**observations)
        before = durable_state(store)
        calls.clear()
        error = None
        try:
            admit(case, **changes)
        except RecoveryContinuationError as exc:
            error = exc
        assert calls == []
        assert error is not None
        assert error.code == ("recovery_continuation_invalid" if location in {
            "accepted_at", "lease_expires_at",
        } else "recovery_continuation_failed")
        assert error.__context__ is None and error.__cause__ is None
        assert "timezone-private-payload" not in "".join(traceback.format_exception(error))
        assert durable_state(store) == before


@pytest.mark.parametrize("representation", ["utc", "fixed-offset", "pydantic", "zoneinfo", "json-parsed"])
def test_trusted_concrete_timezone_representations_preserve_admission(tmp_path, representation):
    # Catches rejecting supported host/JSON timestamps while closing executable timezones.
    from datetime import timezone
    from pydantic_core import TzInfo
    from zoneinfo import ZoneInfo
    from orchestrator.models import FXSnapshot, TokenizerBinding, TokenizerSnapshot
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    zones = {"utc": timezone.utc, "fixed-offset": timezone(timedelta(hours=5, minutes=30)),
             "pydantic": TzInfo(-7 * 3600), "zoneinfo": ZoneInfo("America/Los_Angeles")}
    with SQLiteEventStore(tmp_path / "trusted-timezone.db") as store:
        case = recovery_case(store, routing_budget=True)
        observations = routing_observations(case)
        if representation == "json-parsed":
            observations["tokenizer_snapshot"] = TokenizerSnapshot(bindings=tuple(
                TokenizerBinding.model_validate_json(binding.model_dump_json())
                for binding in observations["tokenizer_snapshot"].bindings
            ))
            observations["fx_snapshot"] = FXSnapshot.model_validate_json(observations["fx_snapshot"].model_dump_json())
            zone = observations["fx_snapshot"].expires_at.tzinfo
            assert type(zone) is TzInfo
        else:
            zone = zones[representation]
            binding = observations["tokenizer_snapshot"].bindings[0]
            binding = binding.model_copy(update={name: getattr(binding, name).astimezone(zone)
                                                  for name in ("effective_from", "expires_at")})
            observations["tokenizer_snapshot"] = observations["tokenizer_snapshot"].model_copy(update={"bindings": (binding,)})
            fx = observations["fx_snapshot"]
            observations["fx_snapshot"] = fx.model_copy(update={name: getattr(fx, name).astimezone(zone)
                                                                 for name in ("effective_from", "expires_at")})
        result = admit(case, routing_inputs=RecoveryRoutingInputs(**observations),
                       accepted_at=(NOW + timedelta(seconds=7)).astimezone(zone),
                       lease_expires_at=(NOW + timedelta(minutes=2)).astimezone(zone))
        assert result.status == "admitted" and result.accepted_route.fencing_generation == 2


@pytest.mark.parametrize("kind", ["scalar-metaclass", "str-subclass-key", "foreign-key"])
def test_declared_data_rejection_does_not_execute_class_or_copied_key_callbacks(tmp_path, kind):
    # Catches hashing foreign scalar types or comparing copied model dictionary keys.
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    calls = []
    with SQLiteEventStore(tmp_path / "guard-callback.db") as store:
        case = recovery_case(store, routing_budget=True)
        class CallbackMeta(type):
            def __hash__(cls):
                calls.append(("scalar-class-hash", store._connection.in_transaction))
                return super().__hash__()
        class ForeignScalar(metaclass=CallbackMeta):
            pass
        class CallbackKey(str):
            def __hash__(self):
                calls.append(("key-hash", store._connection.in_transaction))
                return hash("budget_currency")
            def __eq__(self, other):
                calls.append(("key-compare", store._connection.in_transaction))
                return str.__eq__(self, other)
        class ForeignKey:
            def __hash__(self):
                calls.append(("foreign-key-hash", store._connection.in_transaction))
                return hash("budget_currency")
            def __eq__(self, other):
                calls.append(("foreign-key-compare", store._connection.in_transaction))
                return False
        observations = routing_observations(case)
        if kind == "scalar-metaclass":
            update = {"budget_ledger_version": ForeignScalar()}
        else:
            key = CallbackKey("unexpected-copy-key") if kind == "str-subclass-key" else ForeignKey()
            update = {key: "bounded-copy-payload"}
        observations["eligibility"] = observations["eligibility"].model_copy(update=update)
        supplied = RecoveryRoutingInputs(**observations)
        before = durable_state(store)
        calls.clear()
        with pytest.raises(RecoveryContinuationError) as caught:
            admit(case, routing_inputs=supplied)
        assert calls == []
        assert caught.value.__context__ is None and caught.value.__cause__ is None
        assert durable_state(store) == before
