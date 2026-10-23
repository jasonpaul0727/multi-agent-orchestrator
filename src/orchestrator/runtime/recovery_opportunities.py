"""Read-only discovery of durable recovery plans under Scheduler serialization."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from orchestrator.config.effective import RecoveryAction
from orchestrator.models import AcceptedModelRoute
from orchestrator.models.provider_calls import SQLiteProviderCallJournal
from orchestrator.persistence import StoredEvent
from orchestrator.routing import FailureClassification, RecoveryPlan
from orchestrator.scheduler import Scheduler


_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$"
_HASH = r"^sha256:[0-9a-f]{64}$"
_ERROR_CODES = frozenset({
    "recovery_continuation_invalid", "recovery_continuation_failed",
    "recovery_input_invalid", "recovery_input_mismatch",
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
    reason: StrictStr
    successor: AcceptedModelRoute | None = None


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
    recovered = scheduler.recovery.recover(run_id)
    lifecycle = recovered.lifecycle
    unresolved_nodes = {
        call.node_id for call in SQLiteProviderCallJournal(scheduler.event_store).unresolved()
        if call.run_id == run_id
    }
    unresolved_nodes.update(
        effect.node_id for effect in recovered.effects if effect.status == "outcome_unknown"
    )
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
    records = []
    for event in relevant:
        if event.event_type != "RecoveryPlanCreated":
            continue
        source = sources[event.payload["attempt_ref"]]
        classification = classifications[event.payload["attempt_ref"]]
        plan = RecoveryPlan.model_validate_json(json.dumps(event.payload["plan"]))
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
