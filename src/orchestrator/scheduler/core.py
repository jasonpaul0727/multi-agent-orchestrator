"""Atomic route acceptance, budget reservation, concurrency slots, and fencing."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from orchestrator.agents import AgentRegistry
from orchestrator.artifacts import ArtifactStore
from orchestrator.budget import BudgetLedger, BudgetReservation, RunLimit, UsageRecord
from orchestrator.config.runtime import RunConfigSnapshot
from orchestrator.models import AcceptedModelRoute, ModelGatewayFailure
from orchestrator.persistence import EventDraft, SQLiteEventStore, StoredEvent
from orchestrator.routing import (
    FailureClassification,
    RecoveryController,
    RecoveryPlan,
    RoutingDecision,
    RoutingRequest,
    classify_gateway_failure,
)
from orchestrator.routing.planning import PlanningNodeContract
from orchestrator.recovery.run import RunRecoveryCoordinator, RunRecoveryError
from orchestrator.validation import revalidate_model

from orchestrator.lifecycle.controller import (
    LifecycleConflict,
    LifecycleController,
    LifecycleError,
)
from orchestrator.lifecycle.models import AttemptState


_SCHEDULER_STREAM = "scheduler"
_SCHEDULER_ID = "global"


def _hash(value: object) -> str:
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class SchedulerError(RuntimeError):
    """A selected route cannot safely become an accepted attempt."""


class ConcurrencyLimitExceeded(SchedulerError):
    """A system, Run, provider, or tool slot envelope has no free capacity."""


class StaleRoutingDecision(SchedulerError):
    """A route does not bind the current frozen Run and ready node state."""


class ConcurrencyLimits(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    system_active_attempts: StrictInt = Field(gt=0)
    run_active_attempts: StrictInt = Field(gt=0)
    provider_active_attempts: StrictInt = Field(gt=0)
    tool_active_attempts: StrictInt = Field(gt=0)


class AcceptedAttempt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    routing_decision: RoutingDecision
    accepted_route: AcceptedModelRoute
    reservation: BudgetReservation
    agent_instance_id: str
    lease_expires_at: datetime

    @field_validator("lease_expires_at")
    @classmethod
    def validate_lease_expiry(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("lease_expires_at must include a UTC offset")
        return value


class Scheduler:
    """Coordinate the selected model attempt with its durable resource holds.

    The scheduler, lifecycle projection, and BudgetLedger must use the exact
    same SQLiteEventStore object. Nested `append_checked` calls share its outer
    `BEGIN IMMEDIATE`, making budget, lifecycle, and global slot records one
    database transaction.
    """

    def __init__(
        self,
        event_store: SQLiteEventStore,
        *,
        limits: ConcurrencyLimits,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self.event_store = event_store
        self.limits = limits
        self.lifecycle = LifecycleController(event_store)
        self.agents = AgentRegistry(event_store)
        self.recovery = RunRecoveryCoordinator(event_store, artifact_store=artifact_store)

    def accept_routing(
        self,
        request: RoutingRequest,
        decision: RoutingDecision,
        *,
        accepted_at: datetime,
        lease_expires_at: datetime,
    ) -> AcceptedAttempt:
        request = revalidate_model(RoutingRequest, request)
        decision = revalidate_model(RoutingDecision, decision)
        accepted_at = _aware(accepted_at, "accepted_at")
        lease_expires_at = _aware(lease_expires_at, "lease_expires_at")
        if request.routing_as_of_event_time > accepted_at:
            raise SchedulerError("routing snapshot time cannot be later than acceptance time")
        if lease_expires_at <= accepted_at:
            raise SchedulerError("attempt lease must extend past the routing event time")
        snapshot = self.lifecycle.config_snapshot(request.run_id)
        limit = _run_limit(snapshot)
        ledger = BudgetLedger(self.event_store, {request.run_id: limit})
        attempt_ref = _attempt_ref(request.run_id, request.node_id, request.attempt_id)
        idempotency_key = f"accept:{request.request_id}"

        def decide(events: list[StoredEvent], version: int):
            try:
                self.recovery.recover(request.run_id)
            except RunRecoveryError as exc:
                raise SchedulerError("Run recovery consistency check failed") from exc
            prior = next(
                (item for item in events if item.idempotency_key == idempotency_key), None
            )
            if prior is not None:
                if (
                    prior.event_type != "RoutingDecisionAccepted"
                    or prior.payload.get("request_hash") != request.request_hash
                    or prior.payload.get("decision_hash") != decision.decision_hash
                    or prior.payload.get("accepted_at") != accepted_at.isoformat()
                    or prior.payload.get("lease_expires_at") != lease_expires_at.isoformat()
                ):
                    raise SchedulerError("route acceptance idempotency key was reused")
                return None

            state = self.lifecycle.replay(request.run_id)
            if (
                request.run_id != decision.run_id
                or request.node_id != decision.node_id
                or request.attempt_id != decision.attempt_id
                or request.fencing_generation != decision.fencing_generation
                or request.request_hash != decision.request_hash
                or request.config_hash != snapshot.effective_config_hash
                or request.registry_hash != snapshot.registry_manifest_hash
                or decision.config_hash != snapshot.effective_config_hash
                or decision.registry_hash != snapshot.registry_manifest_hash
                or state.config_hash != snapshot.effective_config_hash
                or state.registry_hash != snapshot.registry_manifest_hash
            ):
                raise StaleRoutingDecision("route does not bind the frozen Run and request")
            if state.status != "running":
                raise LifecycleError("routing can only be accepted for a running Run")
            try:
                node = state.node(request.node_id)
            except KeyError as exc:
                raise StaleRoutingDecision("route references a node outside the frozen graph") from exc
            if node.status != "ready" or request.planning_contract_hash != node.spec.planning_contract_hash:
                raise StaleRoutingDecision("route does not match the Ready node planning contract")
            frozen_contract = node.spec.planning_contract
            if frozen_contract is not None and (
                state.policy_manifest_hash != frozen_contract.policy_manifest_hash
                or request.policy_manifest_hash != frozen_contract.policy_manifest_hash
            ):
                raise StaleRoutingDecision("route policy version differs from the frozen node contract")
            if request.fencing_generation != len(node.attempts) + 1:
                raise LifecycleConflict("route fencing generation is not monotonic")
            if len(node.attempts) >= node.spec.max_attempts:
                raise LifecycleError("node has exhausted its configured attempt limit")
            if decision.outcome != "selected" or decision.selected_model_id is None:
                raise StaleRoutingDecision("blocked routing decisions cannot be accepted")
            if decision.policy_manifest_hash != request.policy_manifest_hash:
                raise StaleRoutingDecision("route policy version does not match its request")
            if request.recovery_action == "initial" and (
                request.failure_category is not None
                or request.retry_level != 0
                or request.exhausted_model_ids
            ):
                raise StaleRoutingDecision("initial route cannot carry recovery counters")
            if request.recovery_action not in {"initial", "health_probe"}:
                _verify_persisted_recovery_authorization(
                    events, request=request, decision=decision
                )
            assessment = next(
                (item for item in decision.candidate_assessments
                 if item.model_id == decision.selected_model_id),
                None,
            )
            if (
                assessment is None
                or not assessment.eligible
                or assessment.provider_id != decision.selected_provider_id
                or assessment.policy_decision.outcome != "allow"
                or assessment.exclusion_reasons
                or assessment.estimated_cost is None
            ):
                raise StaleRoutingDecision("selected route has no eligible policy and cost proof")
            policy_request = assessment.policy_decision.request
            if (
                policy_request.run_id != request.run_id
                or policy_request.node_id != request.node_id
                or policy_request.attempt_id != request.attempt_id
                or policy_request.fencing_generation != request.fencing_generation
                or policy_request.role != node.spec.role
                or policy_request.action_category != "model_invoke"
                or policy_request.tool_id != f"model:{decision.selected_model_id}"
                or policy_request.policy_manifest_hash != request.policy_manifest_hash
                or assessment.policy_decision.policy_manifest_hash != request.policy_manifest_hash
                or policy_request.normalized_request_hash != _policy_action_hash(
                    request,
                    model_id=decision.selected_model_id,
                    provider_id=decision.selected_provider_id,
                )
            ):
                raise StaleRoutingDecision("policy proof is not scoped to this model attempt")
            if assessment.estimated_cost.currency != limit.currency:
                raise StaleRoutingDecision("selected route cost currency differs from Run budget")
            model = next(
                (item for item in snapshot.registry_manifest.models if item.id == decision.selected_model_id),
                None,
            )
            provider = next(
                (item for item in snapshot.registry_manifest.providers if item.id == decision.selected_provider_id),
                None,
            )
            envelope = snapshot.resolved_config.config.policy_envelope
            if (
                model is None
                or provider is None
                or model.provider != provider.id
                or not provider.enabled
                or (envelope.allowed_models and model.id not in envelope.allowed_models)
                or model.id in envelope.denied_models
                or (envelope.allowed_providers and provider.id not in envelope.allowed_providers)
                or provider.id in envelope.denied_providers
                or _tier_rank(model.tier) < _tier_rank(envelope.min_tier)
            ):
                raise StaleRoutingDecision("selected model is outside the frozen registry or Run envelope")

            _ensure_concurrency(
                events,
                run_id=request.run_id,
                provider_id=decision.selected_provider_id,
                tool_ids=node.spec.tool_ids,
                limits=self.limits,
                run_limit=_run_concurrency_limit(snapshot, self.limits),
            )
            reservation_id = "reservation-" + attempt_ref.removeprefix("sha256:")
            reservation = ledger.reserve(
                request.run_id,
                assessment.estimated_cost,
                token_limit=assessment.estimated_cost.token_limit,
                reservation_id=reservation_id,
                idempotency_key=idempotency_key,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                fencing_generation=request.fencing_generation,
                correlation_id=request.run_id,
                causation_id=decision.decision_hash,
            )
            route = AcceptedModelRoute(
                decision_id="decision-" + decision.decision_hash.removeprefix("sha256:"),
                run_id=request.run_id,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                fencing_generation=request.fencing_generation,
                budget_reservation_id=reservation.reservation_id,
                model_id=decision.selected_model_id,
                provider_id=decision.selected_provider_id,
                reasoning_effort=decision.reasoning_effort,
                registry_manifest_hash=decision.registry_hash,
            )
            attempt = AttemptState(
                attempt_id=request.attempt_id,
                agent_instance_id=AgentRegistry.agent_id_for_attempt(
                    request.run_id, request.attempt_id
                ),
                fencing_generation=request.fencing_generation,
                decision_hash=decision.decision_hash,
                policy_manifest_hash=request.policy_manifest_hash,
                reasoning_effort=decision.reasoning_effort,
                model_id=route.model_id,
                provider_id=route.provider_id,
                reservation_id=reservation.reservation_id,
                lease_expires_at=lease_expires_at.isoformat(),
                status="accepted",
            )
            self.lifecycle.record_attempt_accepted(
                request.run_id,
                node_id=request.node_id,
                attempt=attempt,
                decision_hash=decision.decision_hash,
                causation_id=decision.decision_hash,
            )
            self.agents.register_attempt(request.run_id, node=node.spec, attempt=attempt)
            payload = {
                "attempt_ref": attempt_ref,
                "run_id": request.run_id,
                "node_id": request.node_id,
                "attempt_id": request.attempt_id,
                "agent_instance_id": attempt.agent_instance_id,
                "provider_id": route.provider_id,
                "tool_ids": list(node.spec.tool_ids),
                "request_hash": request.request_hash,
                "decision_hash": decision.decision_hash,
                "failure_category": request.failure_category,
                "retry_level": request.retry_level,
                "exhausted_model_ids": list(request.exhausted_model_ids),
                "accepted_route": route.model_dump(mode="json"),
                "reservation_id": reservation.reservation_id,
                "accepted_at": accepted_at.isoformat(),
                "lease_expires_at": lease_expires_at.isoformat(),
                "recovery_action": request.recovery_action,
                "prior_decision_hash": request.prior_decision_hash,
                "recovery_authorization_hash": (
                    None if request.recovery_authorization is None
                    else request.recovery_authorization.authorization_hash
                ),
            }
            return [
                EventDraft(
                    "RoutingDecisionAccepted",
                    payload,
                    run_id=request.run_id,
                    node_id=request.node_id,
                    attempt_id=request.attempt_id,
                    fencing_generation=request.fencing_generation,
                    correlation_id=request.run_id,
                    causation_id=decision.decision_hash,
                )
            ]

        self.event_store.append_checked(
            _SCHEDULER_STREAM, _SCHEDULER_ID, idempotency_key, decide
        )
        accepted = next(
            event for event in self.event_store.read_stream(_SCHEDULER_STREAM, _SCHEDULER_ID)
            if event.idempotency_key == idempotency_key
        )
        route = AcceptedModelRoute.model_validate(accepted.payload["accepted_route"])
        reservation = ledger.get_reservation(route.budget_reservation_id, run_id=request.run_id)
        return AcceptedAttempt(
            routing_decision=decision,
            accepted_route=route,
            reservation=reservation,
            agent_instance_id=AgentRegistry.agent_id_for_attempt(
                request.run_id, request.attempt_id
            ),
            lease_expires_at=lease_expires_at,
        )

    def plan_attempt_recovery(
        self,
        *,
        run_id: str,
        node_id: str,
        attempt_id: str,
        fencing_generation: int,
        source_decision: RoutingDecision,
        contract: PlanningNodeContract,
        failure: ModelGatewayFailure,
    ) -> RecoveryPlan:
        """Persist sanitized failure evidence and its bounded recovery plan.

        This method never executes a retry. A later route acceptance must
        consume the exact persisted authorization once; unknown provider
        outcomes remain blocked until reconciliation.
        """

        source_decision = revalidate_model(RoutingDecision, source_decision)
        contract = revalidate_model(PlanningNodeContract, contract)
        failure = revalidate_model(ModelGatewayFailure, failure)
        if (
            source_decision.run_id != run_id
            or source_decision.node_id != node_id
            or source_decision.attempt_id != attempt_id
            or source_decision.fencing_generation != fencing_generation
            or contract.run_id != run_id
            or contract.node_id != node_id
            or contract.contract_hash != source_decision.planning_contract_hash
        ):
            raise StaleRoutingDecision("failure recovery inputs do not match the source attempt")
        attempt_ref = _attempt_ref(run_id, node_id, attempt_id)
        idempotency_key = f"recovery-plan:{attempt_ref}"
        result: dict[str, object] = {}

        def decide(events: list[StoredEvent], version: int):
            try:
                self.recovery.recover(run_id)
            except RunRecoveryError as exc:
                raise SchedulerError("Run recovery consistency check failed") from exc
            accepted = next(
                (
                    event for event in events
                    if event.event_type == "RoutingDecisionAccepted"
                    and event.payload.get("attempt_ref") == attempt_ref
                ),
                None,
            )
            if (
                accepted is None
                or accepted.payload.get("decision_hash") != source_decision.decision_hash
                or accepted.payload.get("request_hash") != source_decision.request_hash
                or accepted.fencing_generation != fencing_generation
                or accepted.payload.get("run_id") != run_id
                or accepted.payload.get("node_id") != node_id
                or accepted.payload.get("attempt_id") != attempt_id
            ):
                raise StaleRoutingDecision("failure evidence is not bound to the persisted accepted route")
            accepted_route = AcceptedModelRoute.model_validate(accepted.payload["accepted_route"])
            if (
                source_decision.outcome != "selected"
                or source_decision.selected_model_id != accepted_route.model_id
                or source_decision.selected_provider_id != accepted_route.provider_id
                or source_decision.registry_hash != accepted_route.registry_manifest_hash
            ):
                raise StaleRoutingDecision("failure source decision disagrees with its accepted route")
            snapshot = self.lifecycle.config_snapshot(run_id)
            state = self.lifecycle.replay(run_id)
            node = state.node(node_id)
            if (
                node.spec.planning_contract_hash != contract.contract_hash
                or node.spec.role != contract.role
                or contract.config_hash != snapshot.effective_config_hash
                or contract.registry_hash != snapshot.registry_manifest_hash
            ):
                raise StaleRoutingDecision("failure contract differs from the frozen Run node")
            terminal = next(
                (
                    event for event in events
                    if event.payload.get("attempt_ref") == attempt_ref
                    and event.event_type in {"AttemptSlotReleased", "AttemptOutcomeUnknown"}
                ),
                None,
            )
            if terminal is None:
                raise SchedulerError("failure recovery requires a durably ended Attempt")
            if failure.outcome in {"unknown", "known_success"}:
                if terminal.event_type != "AttemptOutcomeUnknown":
                    raise SchedulerError("unresolved Gateway outcome is not persisted as OutcomeUnknown")
            elif terminal.event_type != "AttemptSlotReleased" or terminal.payload.get("outcome") != "failed":
                raise SchedulerError("known Gateway failure requires a durably failed Attempt")

            prior_failures: set[str] = set()
            recorded_exhausted = accepted.payload.get("exhausted_model_ids", [])
            retry_level = accepted.payload.get("retry_level", fencing_generation - 1)
            if (
                type(retry_level) is not int
                or retry_level < 0
                or not isinstance(recorded_exhausted, list)
                or any(not isinstance(item, str) for item in recorded_exhausted)
            ):
                raise SchedulerError("accepted route has invalid persisted recovery counters")
            prior_failures.update(recorded_exhausted)
            accepted_by_ref = {
                event.payload.get("attempt_ref"): event
                for event in events
                if event.event_type == "RoutingDecisionAccepted"
                and event.payload.get("run_id") == run_id
                and event.payload.get("node_id") == node_id
                and event.fencing_generation < fencing_generation
            }
            for prior_event in events:
                prior_ref = prior_event.payload.get("attempt_ref")
                prior_acceptance = accepted_by_ref.get(prior_ref)
                if (
                    prior_acceptance is not None
                    and prior_event.event_type == "AttemptSlotReleased"
                    and prior_event.payload.get("outcome") == "failed"
                ):
                    prior_failures.add(
                        AcceptedModelRoute.model_validate(
                            prior_acceptance.payload["accepted_route"]
                        ).model_id
                    )
            classification = classify_gateway_failure(
                failure,
                source_decision=source_decision,
                retry_level=retry_level,
                exhausted_model_ids=tuple(sorted(prior_failures)),
            )
            if classification.evidence is None:
                plan = _blocked_recovery_plan(classification.reason)
            else:
                plan = RecoveryController().plan(
                    source_decision=source_decision,
                    contract=contract,
                    config=snapshot.resolved_config.config,
                    registry=snapshot.registry_manifest,
                    evidence=classification.evidence,
                )
            classification_payload = classification.model_dump(mode="json")
            plan_payload = plan.model_dump(mode="json")
            failure_summary = {
                "code": failure.code,
                "phase": failure.phase,
                "outcome": failure.outcome,
                "retryable": failure.retryable,
                "http_status": failure.http_status,
                "retry_after_ms": failure.retry_after_ms,
            }
            classified_payload = {
                "run_id": run_id,
                "node_id": node_id,
                "attempt_id": attempt_id,
                "fencing_generation": fencing_generation,
                "attempt_ref": attempt_ref,
                "decision_hash": source_decision.decision_hash,
                "failure": failure_summary,
                "provider_code_hash": (
                    None if failure.provider_code is None
                    else _hash({"provider_code": failure.provider_code})
                ),
                "classification": classification_payload,
            }
            plan_event_payload = {
                "run_id": run_id,
                "node_id": node_id,
                "attempt_id": attempt_id,
                "fencing_generation": fencing_generation,
                "attempt_ref": attempt_ref,
                "decision_hash": source_decision.decision_hash,
                "evidence_hash": (
                    None if classification.evidence is None
                    else classification.evidence.evidence_hash
                ),
                "plan": plan_payload,
            }
            prior_operation = [event for event in events if event.idempotency_key == idempotency_key]
            if prior_operation:
                if (
                    len(prior_operation) != 2
                    or prior_operation[0].event_type != "AttemptFailureClassified"
                    or prior_operation[0].payload != classified_payload
                    or prior_operation[1].event_type != "RecoveryPlanCreated"
                    or prior_operation[1].payload != plan_event_payload
                ):
                    raise SchedulerError("failure recovery idempotency key was reused")
                result["plan"] = plan
                return None
            result["plan"] = plan
            return [
                EventDraft(
                    "AttemptFailureClassified",
                    classified_payload,
                    run_id=run_id,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    fencing_generation=fencing_generation,
                    correlation_id=run_id,
                    causation_id=source_decision.decision_hash,
                ),
                EventDraft(
                    "RecoveryPlanCreated",
                    plan_event_payload,
                    run_id=run_id,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    fencing_generation=fencing_generation,
                    correlation_id=run_id,
                    causation_id=source_decision.decision_hash,
                ),
            ]

        self.event_store.append_checked(
            _SCHEDULER_STREAM, _SCHEDULER_ID, idempotency_key, decide
        )
        return RecoveryPlan.model_validate(result["plan"])

    def finish_attempt(
        self,
        *,
        run_id: str,
        node_id: str,
        attempt_id: str,
        fencing_generation: int,
        completed_at: datetime,
        outcome: Literal["succeeded", "failed", "outcome_unknown"],
        usage: UsageRecord | None = None,
        known_no_effect: bool = False,
    ) -> None:
        completed_at = _aware(completed_at, "completed_at")
        if usage is not None:
            usage = revalidate_model(UsageRecord, usage)
        if known_no_effect and outcome != "failed":
            raise SchedulerError("no-effect proof is valid only for a failed attempt")
        if usage is not None and known_no_effect:
            raise SchedulerError("usage and no-effect proof are mutually exclusive")
        attempt_ref = _attempt_ref(run_id, node_id, attempt_id)
        op_payload = {
            "attempt_ref": attempt_ref,
            "run_id": run_id,
            "node_id": node_id,
            "attempt_id": attempt_id,
            "fencing_generation": fencing_generation,
            "completed_at": completed_at.isoformat(),
            "outcome": outcome,
            "usage_hash": None if usage is None else _hash(usage.model_dump(mode="json")),
            "known_no_effect": known_no_effect,
        }
        key = f"finish:{attempt_ref}"

        def decide(events: list[StoredEvent], version: int):
            prior = next((item for item in events if item.idempotency_key == key), None)
            if prior is not None:
                if prior.event_type not in {"AttemptSlotReleased", "AttemptOutcomeUnknown"} or any(
                    prior.payload.get(name) != value for name, value in op_payload.items()
                ):
                    raise SchedulerError("attempt completion idempotency key was reused")
                return None
            accepted = _active_acceptance(events, attempt_ref)
            if accepted is None or accepted.payload.get("run_id") != run_id:
                raise LifecycleConflict("attempt is not holding an active scheduler lease")
            if accepted.fencing_generation != fencing_generation:
                raise LifecycleConflict("attempt completion is from a stale fencing generation")
            accepted_at = datetime.fromisoformat(accepted.payload["accepted_at"])
            if completed_at < accepted_at:
                raise LifecycleConflict("attempt completion precedes route acceptance")
            expires_at = datetime.fromisoformat(accepted.payload["lease_expires_at"])
            if outcome != "outcome_unknown" and completed_at >= expires_at:
                raise LifecycleConflict("late attempt result must be reconciled, not accepted")
            decision_hash = accepted.payload["decision_hash"]
            reservation_id = accepted.payload["reservation_id"]
            ledger = self._ledger_for_run(run_id)
            if outcome == "outcome_unknown":
                if usage is not None or known_no_effect:
                    raise SchedulerError("unknown outcome cannot include a settlement or no-effect claim")
                ledger.mark_unknown(reservation_id, run_id=run_id, reason="provider_outcome_unknown")
                self.lifecycle.record_attempt_completed(
                    run_id,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    fencing_generation=fencing_generation,
                    outcome="outcome_unknown",
                    causation_id=decision_hash,
                )
                self.agents.complete_attempt(
                    run_id,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    agent_instance_id=accepted.payload["agent_instance_id"],
                    fencing_generation=fencing_generation,
                    causation_id=decision_hash,
                    outcome="outcome_unknown",
                )
                return [
                    EventDraft(
                        "AttemptOutcomeUnknown",
                        op_payload,
                        run_id=run_id,
                        node_id=node_id,
                        attempt_id=attempt_id,
                        fencing_generation=fencing_generation,
                        correlation_id=run_id,
                        causation_id=decision_hash,
                    )
                ]
            if usage is not None:
                _validate_usage(usage, run_id=run_id, reservation_id=reservation_id)
                ledger.commit_usage(
                    reservation_id,
                    usage,
                    settlement_key=usage.settlement_key,
                    run_id=run_id,
                )
            elif outcome == "failed" and known_no_effect:
                ledger.release(reservation_id, run_id=run_id, reason="attempt_failed_no_effect")
            else:
                raise SchedulerError("a known terminal result needs usage or an explicit no-effect proof")
            self.lifecycle.record_attempt_completed(
                run_id,
                node_id=node_id,
                attempt_id=attempt_id,
                fencing_generation=fencing_generation,
                outcome=outcome,
                causation_id=decision_hash,
            )
            self.agents.complete_attempt(
                run_id,
                node_id=node_id,
                attempt_id=attempt_id,
                agent_instance_id=accepted.payload["agent_instance_id"],
                fencing_generation=fencing_generation,
                causation_id=decision_hash,
                outcome=outcome,
            )
            self.lifecycle.complete_cancellation_if_idle(run_id)
            return [
                EventDraft(
                    "AttemptSlotReleased",
                    op_payload,
                    run_id=run_id,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    fencing_generation=fencing_generation,
                    correlation_id=run_id,
                    causation_id=decision_hash,
                )
            ]

        self.event_store.append_checked(_SCHEDULER_STREAM, _SCHEDULER_ID, key, decide)

    def acknowledge_cancellation(
        self,
        *,
        run_id: str,
        node_id: str,
        attempt_id: str,
        fencing_generation: int,
        stopped_at: datetime,
        stop_receipt_hash: str,
        usage: UsageRecord | None = None,
        no_effect_receipt_hash: str | None = None,
    ) -> None:
        """Release an attempt only after a runtime supplies a stop receipt.

        A local stop receipt alone cannot prove that a model provider incurred
        no charge, so cancellation requires usage to settle or explicit proof
        that no provider/tool effect occurred.
        """

        stopped_at = _aware(stopped_at, "stopped_at")
        if usage is not None:
            usage = revalidate_model(UsageRecord, usage)
        if no_effect_receipt_hash is not None and (
            not isinstance(no_effect_receipt_hash, str)
            or not no_effect_receipt_hash.startswith("sha256:")
            or len(no_effect_receipt_hash) != 71
            or any(character not in "0123456789abcdef" for character in no_effect_receipt_hash[7:])
        ):
            raise SchedulerError("no-effect receipt hash must be a sha256 content hash")
        if usage is not None and no_effect_receipt_hash is not None:
            raise SchedulerError("usage and no-effect proof are mutually exclusive")
        if usage is None and no_effect_receipt_hash is None:
            raise SchedulerError("cancellation requires usage or an explicit no-effect proof")
        attempt_ref = _attempt_ref(run_id, node_id, attempt_id)
        op_payload = {
            "attempt_ref": attempt_ref,
            "run_id": run_id,
            "node_id": node_id,
            "attempt_id": attempt_id,
            "fencing_generation": fencing_generation,
            "outcome": "cancelled",
            "stopped_at": stopped_at.isoformat(),
            "stop_receipt_hash": stop_receipt_hash,
            "usage_hash": None if usage is None else _hash(usage.model_dump(mode="json")),
            "no_effect_receipt_hash": no_effect_receipt_hash,
        }
        key = f"cancel-ack:{attempt_ref}"

        def decide(events: list[StoredEvent], version: int):
            prior = next((item for item in events if item.idempotency_key == key), None)
            if prior is not None:
                if prior.event_type != "AttemptSlotReleased" or dict(prior.payload) != op_payload:
                    raise SchedulerError("attempt cancellation idempotency key was reused")
                return None
            accepted = _active_acceptance(events, attempt_ref)
            if accepted is None or accepted.payload.get("run_id") != run_id:
                raise LifecycleConflict("attempt is not holding an active scheduler lease")
            if accepted.fencing_generation != fencing_generation:
                raise LifecycleConflict("cancellation receipt is from a stale fencing generation")
            if stopped_at < datetime.fromisoformat(accepted.payload["accepted_at"]):
                raise LifecycleConflict("cancellation receipt precedes attempt acceptance")
            if stopped_at >= datetime.fromisoformat(accepted.payload["lease_expires_at"]):
                raise LifecycleConflict("expired attempt must be reconciled, not cancelled")
            state = self.lifecycle.replay(run_id)
            if state.status != "cancelling":
                raise SchedulerError("cancellation acknowledgement requires a cancelling Run")
            cancellation_event = next(
                event for event in reversed(self.event_store.read_stream("run_lifecycle", run_id))
                if event.event_type == "RunCancellationRequested"
            )
            reservation_id = accepted.payload["reservation_id"]
            ledger = self._ledger_for_run(run_id)
            if usage is not None:
                _validate_usage(usage, run_id=run_id, reservation_id=reservation_id)
                ledger.commit_usage(
                    reservation_id, usage, settlement_key=usage.settlement_key, run_id=run_id
                )
            elif no_effect_receipt_hash is not None:
                ledger.release(reservation_id, run_id=run_id, reason="attempt_cancelled_no_effect")
            else:
                raise SchedulerError("cancellation has neither usage nor a no-effect receipt")
            self.lifecycle.record_attempt_cancelled(
                run_id,
                node_id=node_id,
                attempt_id=attempt_id,
                fencing_generation=fencing_generation,
                stop_receipt_hash=stop_receipt_hash,
                causation_id=cancellation_event.event_id,
            )
            self.agents.cancel_attempt(
                run_id,
                node_id=node_id,
                attempt_id=attempt_id,
                agent_instance_id=accepted.payload["agent_instance_id"],
                fencing_generation=fencing_generation,
                causation_id=cancellation_event.event_id,
            )
            self.lifecycle.complete_cancellation_if_idle(run_id)
            return [
                EventDraft(
                    "AttemptSlotReleased", op_payload,
                    run_id=run_id, node_id=node_id, attempt_id=attempt_id,
                    fencing_generation=fencing_generation, correlation_id=run_id,
                    causation_id=cancellation_event.event_id,
                )
            ]

        self.event_store.append_checked(_SCHEDULER_STREAM, _SCHEDULER_ID, key, decide)

    def mark_expired_attempts_unknown(self, *, as_of: datetime) -> tuple[str, ...]:
        """Fence expired leases into OutcomeUnknown without freeing their slots."""

        as_of = _aware(as_of, "as_of")
        events = self.event_store.read_stream(_SCHEDULER_STREAM, _SCHEDULER_ID)
        active: dict[str, StoredEvent] = {}
        unknown = {
            item.payload.get("attempt_ref")
            for item in events
            if item.event_type == "AttemptOutcomeUnknown"
        }
        for event in events:
            if event.event_type == "RoutingDecisionAccepted":
                active[event.payload["attempt_ref"]] = event
            elif event.event_type == "AttemptSlotReleased":
                active.pop(event.payload["attempt_ref"], None)
        marked: list[str] = []
        for attempt_ref, event in sorted(active.items()):
            if attempt_ref in unknown:
                continue
            expires_at = datetime.fromisoformat(event.payload["lease_expires_at"])
            if expires_at > as_of:
                continue
            try:
                self.finish_attempt(
                    run_id=event.payload["run_id"],
                    node_id=event.payload["node_id"],
                    attempt_id=event.payload["attempt_id"],
                    fencing_generation=event.fencing_generation,
                    completed_at=as_of,
                    outcome="outcome_unknown",
                )
            except LifecycleConflict:
                # A completion/reconciliation may have won after this read.
                continue
            marked.append(event.payload["attempt_id"])
        return tuple(marked)

    def reconcile_attempt(
        self,
        *,
        run_id: str,
        node_id: str,
        attempt_id: str,
        fencing_generation: int,
        reconciled_at: datetime,
        outcome: Literal["succeeded", "failed"],
        usage: UsageRecord | None = None,
        known_no_effect: bool = False,
    ) -> None:
        reconciled_at = _aware(reconciled_at, "reconciled_at")
        if usage is not None:
            usage = revalidate_model(UsageRecord, usage)
        if known_no_effect and outcome != "failed":
            raise SchedulerError("no-effect proof is valid only for a failed attempt")
        attempt_ref = _attempt_ref(run_id, node_id, attempt_id)
        op_payload = {
            "attempt_ref": attempt_ref,
            "run_id": run_id,
            "node_id": node_id,
            "attempt_id": attempt_id,
            "fencing_generation": fencing_generation,
            "reconciled_at": reconciled_at.isoformat(),
            "outcome": outcome,
            "usage_hash": None if usage is None else _hash(usage.model_dump(mode="json")),
            "known_no_effect": known_no_effect,
        }
        key = f"reconcile:{attempt_ref}"

        def decide(events: list[StoredEvent], version: int):
            prior = next((item for item in events if item.idempotency_key == key), None)
            if prior is not None:
                if prior.event_type != "AttemptSlotReleased" or any(
                    prior.payload.get(name) != value for name, value in op_payload.items()
                ):
                    raise SchedulerError("attempt reconciliation idempotency key was reused")
                return None
            unknown = next(
                (item for item in events if item.event_type == "AttemptOutcomeUnknown"
                 and item.payload.get("attempt_ref") == attempt_ref),
                None,
            )
            if unknown is None:
                raise LifecycleConflict("only an OutcomeUnknown attempt can be reconciled")
            if unknown.fencing_generation != fencing_generation:
                raise LifecycleConflict("reconciliation is from a stale fencing generation")
            unknown_at = datetime.fromisoformat(unknown.payload["completed_at"])
            if reconciled_at < unknown_at:
                raise LifecycleConflict("reconciliation predates the unknown-outcome event")
            accepted = next(
                (item for item in events if item.event_type == "RoutingDecisionAccepted"
                 and item.payload.get("attempt_ref") == attempt_ref),
                None,
            )
            if accepted is None:
                raise LifecycleConflict("unknown attempt has no accepted route evidence")
            reservation_id = accepted.payload["reservation_id"]
            ledger = self._ledger_for_run(run_id)
            settlement_usage = usage
            if settlement_usage is None:
                if not known_no_effect:
                    raise SchedulerError("reconciliation needs reported usage or no-effect proof")
                settlement_usage = UsageRecord(
                    reservation_id=reservation_id,
                    run_id=run_id,
                    settlement_key=f"reconciled-no-effect:{attempt_id}",
                    currency=ledger.get_reservation(reservation_id, run_id=run_id).currency,
                    cost_minor=0,
                    status="committed",
                )
            elif known_no_effect:
                raise SchedulerError("usage and no-effect proof are mutually exclusive")
            _validate_usage(settlement_usage, run_id=run_id, reservation_id=reservation_id)
            ledger.reconcile_unknown(
                reservation_id,
                settlement_usage,
                settlement_key=settlement_usage.settlement_key,
                run_id=run_id,
            )
            decision_hash = accepted.payload["decision_hash"]
            self.lifecycle.record_attempt_reconciled(
                run_id,
                node_id=node_id,
                attempt_id=attempt_id,
                fencing_generation=fencing_generation,
                outcome=outcome,
                causation_id=decision_hash,
            )
            self.agents.reconcile_attempt(
                run_id,
                node_id=node_id,
                attempt_id=attempt_id,
                agent_instance_id=accepted.payload["agent_instance_id"],
                fencing_generation=fencing_generation,
                causation_id=decision_hash,
                outcome=outcome,
            )
            self.lifecycle.complete_cancellation_if_idle(run_id)
            return [
                EventDraft(
                    "AttemptSlotReleased",
                    op_payload,
                    run_id=run_id,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    fencing_generation=fencing_generation,
                    correlation_id=run_id,
                    causation_id=decision_hash,
                )
            ]

        self.event_store.append_checked(_SCHEDULER_STREAM, _SCHEDULER_ID, key, decide)

    def _ledger_for_run(self, run_id: str) -> BudgetLedger:
        return BudgetLedger(self.event_store, {run_id: _run_limit(self.lifecycle.config_snapshot(run_id))})


def _run_limit(snapshot: RunConfigSnapshot) -> RunLimit:
    config = snapshot.resolved_config.config
    requested = config.presets[config.active_preset].requested_budget
    envelope = config.policy_envelope
    cost_caps = [value for value in (requested.max_cost_minor, envelope.max_cost_minor) if value is not None]
    token_caps = [value for value in (requested.max_total_tokens, envelope.max_total_tokens) if value is not None]
    return RunLimit(
        max_cost_minor=min(cost_caps) if cost_caps else None,
        max_tokens=min(token_caps) if token_caps else None,
        currency=envelope.currency,
    )


def _run_concurrency_limit(snapshot: RunConfigSnapshot, limits: ConcurrencyLimits) -> int:
    config = snapshot.resolved_config.config
    requested = config.presets[config.active_preset].requested_budget.max_concurrency
    envelope = config.policy_envelope.max_concurrency
    return min(limits.run_active_attempts, requested, envelope)


def _attempt_ref(run_id: str, node_id: str, attempt_id: str) -> str:
    value = json.dumps((run_id, node_id, attempt_id), ensure_ascii=False, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _blocked_recovery_plan(reason: str) -> RecoveryPlan:
    return RecoveryPlan(
        outcome="blocked",
        action=None,
        authorization=None,
        target_role=None,
        authorized_model_ids=(),
        reason=reason,
    )


def _verify_persisted_recovery_authorization(
    events: list[StoredEvent], *, request: RoutingRequest, decision: RoutingDecision
) -> None:
    authorization = request.recovery_authorization
    if authorization is None:
        raise StaleRoutingDecision("recovery authorization is missing")
    if request.recovery_action not in {
        "same_model_retry", "same_tier_fallback", "capability_escalation"
    }:
        raise SchedulerError("recovery action requires graph or health-control integration")
    plan_event = next(
        (
            event for event in events
            if event.event_type == "RecoveryPlanCreated"
            and event.run_id == request.run_id
            and event.payload.get("decision_hash") == request.prior_decision_hash
            and event.payload.get("plan", {}).get("authorization") == authorization.model_dump(mode="json")
        ),
        None,
    )
    if plan_event is None:
        raise StaleRoutingDecision("recovery authorization was not persisted by Scheduler")
    plan = RecoveryPlan.model_validate_json(json.dumps(plan_event.payload.get("plan")))
    if (
        plan.outcome != "authorized"
        or plan.action != request.recovery_action
        or decision.selected_model_id not in plan.authorized_model_ids
    ):
        raise StaleRoutingDecision("recovery route is not allowed by its persisted plan")
    classified_event = next(
        (
            event for event in events
            if event.event_type == "AttemptFailureClassified"
            and event.payload.get("attempt_ref") == plan_event.payload.get("attempt_ref")
            and event.payload.get("decision_hash") == request.prior_decision_hash
        ),
        None,
    )
    if classified_event is None:
        raise StaleRoutingDecision("persisted recovery plan has no failure evidence")
    classification = FailureClassification.model_validate_json(
        json.dumps(classified_event.payload.get("classification"))
    )
    evidence = classification.evidence
    if evidence is None or (
        request.failure_category != evidence.failure_category
        or request.retry_level != evidence.retry_level + 1
        or request.exhausted_model_ids != evidence.exhausted_model_ids
    ):
        raise StaleRoutingDecision("recovery request counters differ from persisted failure evidence")
    source = next(
        (
            event for event in events
            if event.event_type == "RoutingDecisionAccepted"
            and event.payload.get("decision_hash") == request.prior_decision_hash
            and event.payload.get("run_id") == request.run_id
        ),
        None,
    )
    if source is None:
        raise StaleRoutingDecision("recovery plan source attempt is missing")
    if (
        plan.action in {"same_model_retry", "same_tier_fallback", "capability_escalation"}
        and (
            source.payload.get("node_id") != request.node_id
            or request.fencing_generation != source.fencing_generation + 1
        )
    ):
        raise StaleRoutingDecision("same-node recovery must use the next fencing generation")
    consumed = any(
        event.event_type == "RoutingDecisionAccepted"
        and event.payload.get("recovery_authorization_hash") == authorization.authorization_hash
        for event in events
    )
    if consumed:
        raise StaleRoutingDecision("recovery authorization was already consumed")


def _policy_action_hash(
    request: RoutingRequest, *, model_id: str, provider_id: str
) -> str:
    return _hash(
        {
            "action": "model_invoke",
            "attempt_id": request.attempt_id,
            "fencing_generation": request.fencing_generation,
            "model_id": model_id,
            "provider_id": provider_id,
            "request_hash": request.request_hash,
            "run_id": request.run_id,
            "node_id": request.node_id,
        }
    )


def _tier_rank(tier: str) -> int:
    return {"economy": 0, "standard": 1, "high": 2}[tier]


def _aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a UTC offset")
    return value


def _active_acceptance(
    events: list[StoredEvent], attempt_ref: str
) -> StoredEvent | None:
    accepted = next(
        (item for item in events if item.event_type == "RoutingDecisionAccepted"
         and item.payload.get("attempt_ref") == attempt_ref),
        None,
    )
    if accepted is None:
        return None
    released = any(
        item.event_type == "AttemptSlotReleased"
        and item.payload.get("attempt_ref") == attempt_ref
        for item in events
    )
    return None if released else accepted


def _ensure_concurrency(
    events: list[StoredEvent],
    *,
    run_id: str,
    provider_id: str,
    tool_ids: tuple[str, ...],
    limits: ConcurrencyLimits,
    run_limit: int,
) -> None:
    active: dict[str, StoredEvent] = {}
    for event in events:
        if event.event_type == "RoutingDecisionAccepted":
            active[event.payload["attempt_ref"]] = event
        elif event.event_type == "AttemptSlotReleased":
            active.pop(event.payload["attempt_ref"], None)
    current = tuple(active.values())
    if len(current) >= limits.system_active_attempts:
        raise ConcurrencyLimitExceeded("system active-attempt limit is exhausted")
    if sum(item.payload.get("run_id") == run_id for item in current) >= run_limit:
        raise ConcurrencyLimitExceeded("Run active-attempt limit is exhausted")
    if sum(item.payload.get("provider_id") == provider_id for item in current) >= limits.provider_active_attempts:
        raise ConcurrencyLimitExceeded("provider active-attempt limit is exhausted")
    for tool_id in tool_ids:
        active_for_tool = sum(tool_id in item.payload.get("tool_ids", []) for item in current)
        if active_for_tool >= limits.tool_active_attempts:
            raise ConcurrencyLimitExceeded("tool active-attempt limit is exhausted")


def _validate_usage(usage: UsageRecord, *, run_id: str, reservation_id: str) -> None:
    if usage.run_id != run_id or usage.reservation_id != reservation_id:
        raise SchedulerError("usage does not belong to the active attempt reservation")
    if usage.status != "committed":
        raise SchedulerError("scheduler completion requires committed usage")


__all__ = [
    "AcceptedAttempt",
    "ConcurrencyLimitExceeded",
    "ConcurrencyLimits",
    "Scheduler",
    "SchedulerError",
    "StaleRoutingDecision",
]
