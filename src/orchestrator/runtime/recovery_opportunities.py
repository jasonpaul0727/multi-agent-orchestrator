"""Read-only discovery of durable recovery plans under Scheduler serialization."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator

from orchestrator.config.effective import RecoveryAction
from orchestrator.models import AcceptedModelRoute, ModelGatewayFailure
from orchestrator.models.provider_calls import SQLiteProviderCallJournal
from orchestrator.persistence import StoredEvent
from orchestrator.persistence.events import validate_event_contract
from orchestrator.lifecycle.controller import apply_lifecycle_event
from orchestrator.routing import FailureClassification, RecoveryPlan
from orchestrator.routing.recovery import RecoveryEvidence, _failure_category
from orchestrator.routing.planning import PlanningNodeContract
from orchestrator.runtime.contracts import AttemptContext
from orchestrator.scheduler import Scheduler
from orchestrator.scheduler.core import _verification_admission
from orchestrator.validation import revalidate_model


_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$"
_HASH = r"^sha256:[0-9a-f]{64}$"
_ERROR_CODES = frozenset({
    "recovery_continuation_invalid", "recovery_continuation_failed",
    "recovery_input_invalid", "recovery_input_mismatch",
})
_BLOCKED_REASONS = frozenset({
    "source_decision_not_selected", "stale_recovery_source", "unknown_outcome_requires_reconciliation",
    "recovery_role_not_enabled", "source_model_missing", "duplicate_exhausted_model",
    "transient_outcome_requires_reconciliation", "same_tier_fallback_exhausted",
    "escalation_role_not_enabled", "escalation_role_has_no_candidates",
    "capability_escalation_not_configured", "capability_escalation_candidates_exhausted",
    "recovery_candidates_empty", "initial_is_not_a_recovery_action",
    "successful_call_requires_settlement", "failure_not_recoverable",
})
_AUTHORIZED_REASONS = {
    "same_model_retry": "bounded_same_model_retry",
    "same_tier_fallback": "same_tier_fallback_after_unavailable_or_retry_exhausted",
    "capability_escalation": "evidence_bound_higher_tier_capability_escalation",
}
_SOURCE_FIELDS = frozenset({
    "attempt_ref", "run_id", "node_id", "attempt_id", "agent_instance_id", "graph_version",
    "verification_admission", "verification_context", "verification_contract", "required_check_ids",
    "provider_id", "tool_ids", "request_hash", "decision_hash", "failure_category", "retry_level",
    "exhausted_model_ids", "accepted_route", "reservation_id", "accepted_at", "lease_expires_at",
    "recovery_action", "prior_decision_hash", "recovery_authorization_hash",
})


class RecoveryContinuationError(ValueError):
    """Stable owned failure containing no input or backend exception data."""

    def __init__(self, code: str) -> None:
        self.code = code if type(code) is str and code in _ERROR_CODES else "recovery_continuation_failed"
        super().__init__(self.code)


class RecoveryOpportunity(BaseModel):
    """Bounded recovery metadata; neither task text nor routing observations."""

    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", hide_input_in_errors=True)

    run_id: StrictStr = Field(pattern=_IDENTIFIER)
    node_id: StrictStr = Field(pattern=_IDENTIFIER)
    source_attempt_id: StrictStr = Field(pattern=_IDENTIFIER)
    source_fencing_generation: StrictInt = Field(gt=0)
    source_decision_hash: StrictStr = Field(pattern=_HASH)
    authorization_hash: StrictStr | None = Field(default=None, pattern=_HASH)
    evidence_hash: StrictStr | None = Field(default=None, pattern=_HASH)
    action: RecoveryAction | None = None
    status: Literal["ready", "already_admitted", "reconciliation_required", "new_child_required", "blocked"]
    reason: StrictStr = Field(pattern=r"^[a-z][a-z0-9_]{0,127}$")
    successor: AcceptedModelRoute | None = None

    @field_validator("successor", mode="before")
    @classmethod
    def revalidate_successor(cls, value):
        if isinstance(value, AcceptedModelRoute):
            if not set(value.__dict__) <= set(AcceptedModelRoute.model_fields):
                raise ValueError("unexpected accepted route fields")
            extra = value.model_extra
            if extra is not None and (not isinstance(extra, dict) or extra):
                raise ValueError("unexpected accepted route extras")
            return revalidate_model(AcceptedModelRoute, value)
        return value


@dataclass(frozen=True)
class RecoverySourceRecord:
    """Validated internal join; never exposed as the public inventory."""

    opportunity: RecoveryOpportunity
    plan: RecoveryPlan
    classification: FailureClassification
    source_event: StoredEvent
    successor_event: StoredEvent | None = None


def load_recovery_records(
    scheduler: Scheduler, *, run_id: str, events: list[StoredEvent],
) -> tuple[RecoverySourceRecord, ...]:
    """Load one consistent snapshot; caller must hold a checked transaction."""
    try:
        return _load_recovery_records(scheduler, run_id=run_id, events=events)
    except Exception:
        pass
    raise RecoveryContinuationError("recovery_continuation_failed") from None


def _load_recovery_records(scheduler, *, run_id, events):
    recovered = scheduler.recovery.recover(run_id)
    lifecycle = recovered.lifecycle
    events = [revalidate_model(StoredEvent, event) for event in events]
    for index, event in enumerate(events, 1):
        encoded = json.dumps(event.payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if (event.stream_type != "scheduler" or event.stream_id != "global"
            or event.stream_version != index
            or event.payload_hash != hashlib.sha256(encoded.encode()).hexdigest()):
            raise ValueError("invalid Scheduler history")
    validate_event_contract(events)
    relevant = [event for event in events if event.run_id == run_id]
    sources = {
        event.payload["attempt_ref"]: event for event in relevant
        if event.event_type == "RoutingDecisionAccepted"
    }
    successors = {
        event.payload["recovery_authorization_hash"]: event for event in sources.values()
        if event.payload.get("recovery_authorization_hash") is not None
    }
    classifications = {
        event.payload["attempt_ref"]: FailureClassification.model_validate_json(
            json.dumps(event.payload["classification"])
        )
        for event in relevant if event.event_type == "AttemptFailureClassified"
    }
    history = _historical_admissions(scheduler, run_id)
    has_input = {}
    for source in sources.values():
        _check_identity(source, run_id)
        has_input[source.payload["attempt_ref"]] = _check_source(
            scheduler, source=source, historical=history, lifecycle=lifecycle,
        )
    plan_sources = {event.payload["attempt_ref"] for event in relevant if event.event_type == "RecoveryPlanCreated"}
    if set(classifications) != plan_sources:
        raise ValueError("orphaned recovery evidence")
    for event in relevant:
        if event.event_type == "AttemptFailureClassified":
            ref = event.payload["attempt_ref"]
            _check_classification(event, classifications[ref], sources[ref], sources, relevant)
    unresolved_nodes = _unsettled_nodes(scheduler, run_id=run_id, sources=sources)
    unresolved_nodes.update(
        effect.node_id for effect in recovered.effects if effect.status == "outcome_unknown"
    )
    records = []
    for event in relevant:
        if event.event_type != "RecoveryPlanCreated":
            continue
        source = sources[event.payload["attempt_ref"]]
        classification = classifications[event.payload["attempt_ref"]]
        plan = RecoveryPlan.model_validate_json(json.dumps(event.payload["plan"]))
        _check_identity(event, run_id)
        _check_recovery_plan(plan, classification, source)
        node = lifecycle.node(source.node_id)
        attempt = next(item for item in node.attempts if item.attempt_id == source.attempt_id)
        successor_event = (
            None if plan.authorization is None
            else successors.get(plan.authorization.authorization_hash)
        )
        successor = (
            None if successor_event is None
            else AcceptedModelRoute.model_validate(successor_event.payload["accepted_route"])
        )
        status, reason = "blocked", plan.reason
        # Consumption is a historical identity, never current task/dispatch authority.
        if successor is not None:
            status, reason = "already_admitted", "recovery_authorization_consumed"
        elif lifecycle.status != "running":
            reason = "run_" + lifecycle.status
        elif attempt.status == "outcome_unknown" or node.spec.node_id in unresolved_nodes:
            status, reason = "reconciliation_required", "provider_outcome_unknown"
        elif node.attempts[-1].attempt_id != source.attempt_id:
            reason = "stale_source_attempt"
        elif not has_input[source.payload["attempt_ref"]]:
            reason = "source_input_unavailable"
        elif plan.outcome == "new_child_required":
            status = "new_child_required"
        elif plan.outcome == "authorized" and attempt.status == "failed":
            status = "ready"
        evidence = classification.evidence
        opportunity = RecoveryOpportunity(
            run_id=run_id, node_id=source.node_id, source_attempt_id=source.attempt_id,
            source_fencing_generation=source.fencing_generation,
            source_decision_hash=source.payload["decision_hash"],
            authorization_hash=None if plan.authorization is None else plan.authorization.authorization_hash,
            evidence_hash=None if evidence is None else evidence.evidence_hash,
            action=plan.action, status=status, reason=reason, successor=successor,
        )
        records.append(RecoverySourceRecord(
            opportunity=opportunity, plan=plan, classification=classification, source_event=source,
            successor_event=successor_event,
        ))
    return tuple(sorted(records, key=lambda item: (
        item.opportunity.node_id, item.opportunity.source_fencing_generation,
        item.opportunity.source_attempt_id,
    )))


def _check_identity(event, run_id):
    payload = event.payload
    for name in ("run_id", "node_id", "attempt_id"):
        value = getattr(event, name)
        if type(value) is not str or re.fullmatch(_IDENTIFIER, value) is None or payload.get(name) != value:
            raise ValueError("invalid recovery identity")
    if (event.run_id != run_id or type(event.fencing_generation) is not int
        or event.fencing_generation <= 0 or event.correlation_id != run_id
        or not isinstance(payload.get("decision_hash"), str)
        or re.fullmatch(_HASH, payload["decision_hash"]) is None
        or event.causation_id != payload["decision_hash"]):
        raise ValueError("invalid recovery binding")
    if event.event_type != "RoutingDecisionAccepted" and (
        type(payload.get("fencing_generation")) is not int
        or payload["fencing_generation"] != event.fencing_generation
    ):
        raise ValueError("invalid recovery generation")


def _historical_admissions(scheduler, run_id):
    result, state = {}, None
    for event in scheduler.event_store.read_stream("run_lifecycle", run_id):
        event = revalidate_model(StoredEvent, event)
        if event.event_type == "AttemptAccepted":
            key = (event.node_id, event.attempt_id, event.fencing_generation)
            if key in result or state is None:
                raise ValueError("invalid historical admission")
            result[key] = state
        state = apply_lifecycle_event(run_id, state, event)
    return result


def _check_source(scheduler, *, source, historical, lifecycle):
    payload = source.payload
    if not set(payload) <= _SOURCE_FIELDS:
        raise ValueError("unexpected accepted source fields")
    state = historical[(source.node_id, source.attempt_id, source.fencing_generation)]
    node = state.node(source.node_id)
    attempt = next(item for item in lifecycle.node(source.node_id).attempts if item.attempt_id == source.attempt_id)
    route = AcceptedModelRoute.model_validate(payload["accepted_route"])
    if (route.decision_id != "decision-" + payload["decision_hash"].removeprefix("sha256:")
        or payload.get("provider_id") != route.provider_id
        or payload.get("agent_instance_id") != attempt.agent_instance_id
        or payload.get("reservation_id") != route.budget_reservation_id):
        raise ValueError("invalid historical route binding")
    commitment = tuple(payload.get(name) for name in (
        "verification_admission", "verification_context", "verification_contract", "required_check_ids",
    ))
    if all(value is None for value in commitment):
        return False
    admission = payload.get("verification_admission")
    if not isinstance(admission, dict) or _verification_admission(**admission) != admission:
        raise ValueError("invalid historical input commitment")
    contract = node.spec.planning_contract
    if contract is None:
        raise ValueError("missing historical frozen contract")
    contract = revalidate_model(PlanningNodeContract, contract)
    snapshot = scheduler.lifecycle.config_snapshot(source.run_id)
    if (contract.run_id != source.run_id or contract.node_id != source.node_id
        or contract.role != node.spec.role or contract.contract_hash != node.spec.planning_contract_hash
        or contract.contract_hash != lifecycle.node(source.node_id).spec.planning_contract_hash
        or contract.config_hash != snapshot.effective_config_hash
        or contract.registry_hash != snapshot.registry_manifest_hash
        or contract.policy_manifest_hash != state.policy_manifest_hash
        or attempt.policy_manifest_hash != contract.policy_manifest_hash
        or type(payload.get("graph_version")) is not int
        or payload["graph_version"] != state.graph_version):
        raise ValueError("invalid historical frozen contract")
    expected = AttemptContext(
        run_id=source.run_id, node_id=source.node_id, attempt_id=source.attempt_id,
        agent_instance_id=attempt.agent_instance_id, fencing_generation=source.fencing_generation,
        graph_version=state.graph_version, input_manifest_hash=admission["input_manifest_hash"],
        effective_config_hash=snapshot.effective_config_hash, registry_hash=snapshot.registry_manifest_hash,
        policy_manifest_hash=contract.policy_manifest_hash, routing_decision_hash=payload["decision_hash"],
        planning_contract_hash=contract.contract_hash,
    )
    if (AttemptContext.model_validate(payload.get("verification_context")) != expected
        or payload.get("verification_contract") != admission["verification_contract"]
        or payload.get("required_check_ids") != admission["required_check_ids"]):
        raise ValueError("historical input commitment disagrees")
    return True


def _check_recovery_plan(plan, classification, source):
    allowed_classification_reasons = {
        "classified": {"gateway_failure_classified"},
        "reconciliation_required": {"provider_outcome_unknown"},
        "blocked": {"source_decision_not_selected", "successful_call_requires_settlement", "failure_not_recoverable"},
    }
    if classification.reason not in allowed_classification_reasons[classification.disposition]:
        raise ValueError("invalid persisted classification reason")
    if plan.outcome == "authorized":
        if plan.reason != _AUTHORIZED_REASONS.get(plan.action) or plan.target_role is not None:
            raise ValueError("invalid authorized plan reason")
    elif plan.outcome == "new_child_required":
        if plan.authorization is None:
            if (plan.reason != "targeted_repair_or_reaggregation_node_required" or plan.action is not None
                or plan.target_role is not None or plan.authorized_model_ids):
                raise ValueError("invalid repair plan")
        elif (plan.reason != "independent_role_node_required" or plan.action not in {"reviewer_node", "director_node"}
              or plan.target_role != plan.action.removesuffix("_node")):
            raise ValueError("invalid role plan")
    elif (plan.reason not in _BLOCKED_REASONS or plan.action is not None or plan.authorization is not None
          or plan.target_role is not None or plan.authorized_model_ids):
        raise ValueError("invalid blocked plan")
    if plan.authorization is not None:
        route = AcceptedModelRoute.model_validate(source.payload["accepted_route"])
        context = source.payload.get("verification_context")
        if (plan.authorization.previous_model_id != route.model_id
            or (context is not None and plan.authorization.policy_manifest_hash != context["policy_manifest_hash"])):
            raise ValueError("invalid plan source authorization")


def _check_classification(event, classification, source, sources, events):
    """Verify the persisted v1 evidence commitment without inventing a RoutingDecision."""
    _check_identity(event, source.run_id)
    payload = event.payload
    if (set(payload) != {"run_id", "node_id", "attempt_id", "fencing_generation", "attempt_ref",
                        "decision_hash", "failure", "provider_code_hash", "classification"}
        or set(payload["failure"]) != {"code", "phase", "outcome", "retryable", "http_status", "retry_after_ms"}):
        raise ValueError("invalid persisted failure fields")
    failure = ModelGatewayFailure.model_validate(payload["failure"])
    category = _failure_category(failure.code)
    if failure.outcome == "known_success":
        expected = FailureClassification(disposition="blocked", reason="successful_call_requires_settlement", evidence=None)
    elif failure.outcome != "unknown" and category is None:
        expected = FailureClassification(disposition="blocked", reason="failure_not_recoverable", evidence=None)
    else:
        retry = source.payload.get("retry_level", source.fencing_generation - 1)
        exhausted = source.payload.get("exhausted_model_ids", [])
        if (type(retry) is not int or retry < 0 or not isinstance(exhausted, list)
            or any(type(item) is not str or re.fullmatch(_IDENTIFIER, item) is None for item in exhausted)
            or len(exhausted) != len(set(exhausted))):
            raise ValueError("invalid source recovery counters")
        failed_refs = {item.payload["attempt_ref"] for item in events
                       if item.event_type == "AttemptSlotReleased" and item.payload.get("outcome") == "failed"}
        previous_models = {
            AcceptedModelRoute.model_validate(item.payload["accepted_route"]).model_id
            for ref, item in sources.items() if ref in failed_refs
            and item.node_id == source.node_id and item.fencing_generation < source.fencing_generation
        }
        exhausted = tuple(sorted(set(exhausted) | previous_models))
        provider_code_hash = payload.get("provider_code_hash")
        if provider_code_hash is not None and (
            type(provider_code_hash) is not str or re.fullmatch(_HASH, provider_code_hash) is None
        ):
            raise ValueError("invalid failure provider commitment")
        route = AcceptedModelRoute.model_validate(source.payload["accepted_route"])
        facts = {
            "source_decision_hash": source.payload["decision_hash"], "model_id": route.model_id,
            "run_id": source.run_id, "node_id": source.node_id, "attempt_id": source.attempt_id,
            "fencing_generation": source.fencing_generation, "failure_code": failure.code,
            "failure_phase": failure.phase, "failure_outcome": failure.outcome,
            "failure_retryable": failure.retryable, "http_status": failure.http_status,
            "retry_after_ms": failure.retry_after_ms, "provider_code_hash": provider_code_hash,
            "retry_level": retry, "exhausted_model_ids": list(exhausted),
        }
        evidence_hash = "sha256:" + hashlib.sha256(json.dumps(
            facts, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        unknown = failure.outcome == "unknown"
        evidence = RecoveryEvidence(
            failure_category=category or "transient", evidence_hash=evidence_hash,
            retry_level=retry, retry_safe=failure.retryable and failure.outcome in {"not_sent", "known_failure"},
            failed_model_unavailable=failure.code == "provider_unavailable", outcome_unknown=unknown,
            exhausted_model_ids=exhausted,
        )
        expected = FailureClassification(
            disposition="reconciliation_required" if unknown else "classified",
            reason="provider_outcome_unknown" if unknown else "gateway_failure_classified", evidence=evidence,
        )
    if classification != expected:
        raise ValueError("persisted failure commitment disagrees with source history")


def _unsettled_nodes(scheduler, *, run_id, sources):
    return {call.node_id for call in _unsettled_calls(scheduler, run_id=run_id, sources=sources)}


def _unsettled_calls(scheduler, *, run_id, sources):
    """Shared Provider hold definitions, retaining exact call identities."""
    result = []
    journal = SQLiteProviderCallJournal(scheduler.event_store)
    for stream_id in scheduler.event_store.stream_ids("provider_call"):
        call = journal.read_call(stream_id)
        if call is None:
            raise ValueError("missing current Provider journal")
        if call.run_id != run_id:
            continue
        source = next((source for source in sources.values() if source.node_id == call.node_id
                       and source.attempt_id == call.attempt_id), None)
        if source is None:
            raise ValueError("Provider journal has no accepted source")
        route = AcceptedModelRoute.model_validate(source.payload["accepted_route"])
        if (call.fencing_generation != source.fencing_generation or call.accepted_route_id != route.decision_id
            or call.budget_reservation_id != route.budget_reservation_id
            or call.model_id != route.model_id or call.provider_id != route.provider_id
            or call.registry_manifest_hash != route.registry_manifest_hash):
            raise ValueError("Provider journal source bindings disagree")
        if call.status in {"dispatching", "unknown", "settlement_pending"}:
            result.append(call)
        elif call.status == "known_success":
            reservation = scheduler._ledger_for_run(run_id).get_reservation(call.budget_reservation_id, run_id=run_id)
            if reservation.status != "committed":
                result.append(call)
    return tuple(result)


class RecoveryOpportunityService:
    """Expose only metadata from a single no-write durable snapshot."""

    def __init__(self, scheduler: Scheduler) -> None:
        self._scheduler = scheduler

    def list_opportunities(self, *, run_id: str) -> tuple[RecoveryOpportunity, ...]:
        error_code = "recovery_continuation_failed"
        try:
            if type(run_id) is not str or re.fullmatch(_IDENTIFIER, run_id) is None:
                error_code = "recovery_continuation_invalid"
                raise ValueError("invalid Run identity")
            result = None

            def read_under_lock(events, _version):
                nonlocal result
                result = tuple(record.opportunity for record in load_recovery_records(
                    self._scheduler, run_id=run_id, events=events,
                ))
                return None

            self._scheduler.event_store.append_checked(
                "scheduler", "global", "recovery-opportunity-snapshot", read_under_lock,
            )
            if result is not None:
                return result
        except Exception:
            pass
        # Raise outside the handler so raw payload-bearing exceptions are detached.
        raise RecoveryContinuationError(error_code) from None


__all__ = ["RecoveryContinuationError", "RecoveryOpportunity", "RecoveryOpportunityService"]
