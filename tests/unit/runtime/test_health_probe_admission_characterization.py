"""Offline health-probe admission characterization, with no Provider send.

The health outcomes and failure/no-effect inputs are synthetic. A Router
selection is not a durable Scheduler admission or production proof.
"""

from contextlib import contextmanager
from datetime import timedelta

import pytest

from orchestrator.persistence import SQLiteEventStore
from orchestrator.persistence.events import EventContractError
from orchestrator.routing import (
    HealthAggregateKey,
    HealthController,
    HealthVersionRef,
    ModelRouter,
    ProbeLease,
    RecoveryAuthorization,
    RoutingRequest,
)
from orchestrator.scheduler.core import StaleRoutingDecision
from tests.support.recovery_continuation import (
    forbid_recovery_execution,
    recovery_case,
    routing_observations,
)
from tests.unit.lifecycle.test_scheduler import NOW


@contextmanager
def _forbid_all_execution():
    from orchestrator.models.transport import ProviderModelGateway, SystemdProviderHTTPSTransport
    from orchestrator.runtime.general_worker_process import GeneralWorkerProcess

    with forbid_recovery_execution() as attempted, pytest.MonkeyPatch.context() as patch:
        for owner, method in (
            (ProviderModelGateway, "invoke"),
            (SystemdProviderHTTPSTransport, "post_json"),
            (GeneralWorkerProcess, "start_turn"),
            (GeneralWorkerProcess, "run_turn"),
        ):
            def forbidden(*args, boundary=f"{owner.__name__}.{method}", **kwargs):
                attempted.append(boundary)
                raise AssertionError("offline characterization crossed an execution boundary")

            patch.setattr(owner, method, forbidden)
        yield attempted


@pytest.mark.parametrize("unknown", [False, True], ids=["synthetic-known-failure", "unknown"])
def test_unpersisted_probe_authorization_cannot_be_admitted(tmp_path, unknown):
    with _forbid_all_execution() as attempted:
        with SQLiteEventStore(tmp_path / "probe.db") as store:
            case = recovery_case(store, unknown=unknown, routing_budget=True, multi_model=True)
            preset = case.config.presets[case.config.active_preset]
            policy = case.config.health_policies[preset.health_policy_ref]
            provider = HealthAggregateKey(
                registry_manifest_hash=case.registry.content_hash, scope="provider", provider_id="primary",
            )
            model = HealthAggregateKey(
                registry_manifest_hash=case.registry.content_hash, scope="model",
                provider_id="primary", model_id="model-1",
            )
            health = HealthController(store)
            provider_state, model_state = health.record_outcome(
                provider_key=provider, model_key=model, policy=policy,
                event_time=NOW + timedelta(seconds=6), idempotency_key="synthetic-health-outcome",
                success=False, failure_category="transient", retry_after_ms=1,
            )
            assert provider_state.state == model_state.state == "open"
            start = max(provider_state.cooldown_until, model_state.cooldown_until) + timedelta(seconds=1)
            actual_lease = health.acquire_probe_lease(
                provider_key=provider, model_key=model, policy=policy, event_time=start,
                lease_id="real-probe", holder_id="synthetic-controller",
                expires_at=start + timedelta(minutes=1), idempotency_key="real-probe",
            )
            fake_lease = ProbeLease.create(
                lease_id="caller-probe", holder_id="caller-holder",
                registry_manifest_hash=actual_lease.registry_manifest_hash,
                provider_id=actual_lease.provider_id, model_id=actual_lease.model_id,
                expires_at=actual_lease.expires_at, aggregates=actual_lease.aggregates,
            )
            assert fake_lease.lease_hash != actual_lease.lease_hash
            assert health.read(provider, default_policy=policy).active_probe_lease_id == actual_lease.lease_id
            assert health.read(model, default_policy=policy).active_probe_lease_id == actual_lease.lease_id
            coordinator = tuple(store.read_stream("health_coordinator", "global"))
            assert any(event.payload.get("lease_id") == actual_lease.lease_id for event in coordinator)
            assert all(event.payload.get("lease_id") != fake_lease.lease_id for event in coordinator)

            source_events = tuple(store.read_stream("scheduler", "global"))
            classified = next(event for event in source_events if event.event_type == "AttemptFailureClassified")
            evidence = classified.payload["classification"]["evidence"]
            authorization = RecoveryAuthorization.create(
                action="health_probe", source_decision_hash=case.decision.decision_hash,
                previous_model_id="model-1", authorized_model_ids=("model-1",),
                policy_manifest_hash=case.policy.content_hash, evidence_hash=evidence["evidence_hash"],
            )
            assert all(
                (event.payload.get("plan", {}).get("authorization") or {}).get("authorization_hash")
                != authorization.authorization_hash
                for event in source_events if event.event_type == "RecoveryPlanCreated"
            )
            values = case.request.model_dump(mode="python")
            values.update(
                request_id="caller-probe-request", attempt_id="caller-probe-attempt", fencing_generation=2,
                routing_as_of_event_time=start + timedelta(seconds=1), recovery_action="health_probe",
                recovery_authorization=authorization, prior_decision_hash=authorization.source_decision_hash,
                previous_model_id=authorization.previous_model_id, probe_lease=fake_lease,
                failure_category=evidence["failure_category"], retry_level=evidence["retry_level"] + 1,
                exhausted_model_ids=tuple(evidence["exhausted_model_ids"]),
            )
            request = RoutingRequest(**values)
            router_args = dict(
                config=case.config, registry=case.registry, contract=case.contract,
                **routing_observations(case, as_of=start),
            )
            router = ModelRouter()
            no_lease = router.route(RoutingRequest(**{**values, "probe_lease": None}), **router_args)
            assert (no_lease.outcome, no_lease.blocked_reason) == ("blocked", "health_probe_lease_required")
            assert router.route(RoutingRequest(**{**values, "probe_lease": actual_lease}), **router_args).outcome == "selected"
            wrong_ref = HealthVersionRef(aggregate_id="sha256:" + "e" * 64, version=1, generation=1)
            wrong_lease = ProbeLease.create(
                lease_id="wrong-probe", holder_id="caller-holder",
                registry_manifest_hash=actual_lease.registry_manifest_hash,
                provider_id=actual_lease.provider_id, model_id=actual_lease.model_id,
                expires_at=actual_lease.expires_at, aggregates=(wrong_ref,),
            )
            assert router.route(RoutingRequest(**{**values, "probe_lease": wrong_lease}), **router_args).outcome == "blocked"
            decision = router.route(request, **router_args)
            assert decision.outcome == "selected" and decision.selected_model_id == "model-1"
            assert decision.request_hash == request.request_hash

            database_before = tuple(store._connection.iterdump())
            budget_before = case.scheduler._ledger_for_run("run-1").available("run-1")
            node_before = case.lifecycle.replay("run-1").node("node-1")
            assert node_before.status == ("awaiting_reconciliation" if unknown else "ready")
            if unknown:
                # The node is not ready, so Scheduler rejects before the event contract.
                error, message = StaleRoutingDecision, "Ready node"
                assert budget_before.unknown_tokens == 1500
            else:
                # The event contract validates the durable plan after Scheduler's
                # health_probe branch skips its own recovery-authorization check.
                error, message = EventContractError, "recovery route acceptance has no matching persisted plan"
            with pytest.raises(error, match=message):
                case.scheduler.accept_routing(
                    request, decision, accepted_at=start + timedelta(seconds=2),
                    lease_expires_at=start + timedelta(minutes=2),
                )
            assert tuple(store._connection.iterdump()) == database_before
            assert case.scheduler._ledger_for_run("run-1").available("run-1") == budget_before
            assert case.lifecycle.replay("run-1").node("node-1") == node_before
        assert attempted == []
