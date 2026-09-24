"""Fail-closed cross-stream reconstruction for one persisted Run."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from orchestrator.agents import AgentRegistry, AgentRegistryState
from orchestrator.artifacts import ArtifactRecord, ArtifactStore
from orchestrator.budget import BudgetBalance, BudgetLedger, RunLimit
from orchestrator.lifecycle import LifecycleController, LifecycleError
from orchestrator.lifecycle.models import AttemptState, RunLifecycleState
from orchestrator.models import AcceptedModelRoute, ModelGatewayFailure
from orchestrator.persistence import SQLiteEventStore, StoredEvent
from orchestrator.routing import FailureClassification, RecoveryPlan


class RunRecoveryError(RuntimeError):
    """A Run's persisted projections do not form a safe, consistent state."""


class RecoveredAttemptLease(BaseModel):
    """An unresolved scheduler lease that must remain held after replay."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    attempt_ref: StrictStr = Field(min_length=1)
    node_id: StrictStr = Field(min_length=1)
    attempt_id: StrictStr = Field(min_length=1)
    fencing_generation: StrictInt = Field(gt=0)
    status: Literal["active", "outcome_unknown"]
    lease_expires_at: StrictStr = Field(min_length=1)


@dataclass(frozen=True)
class RecoveredEffect:
    """External effect state; an intent without receipt is never replay-safe."""

    effect_id: str
    node_id: str
    attempt_id: str
    fencing_generation: int
    recovery_class: Literal["idempotent", "queryable", "manual_only"]
    status: Literal["outcome_unknown", "applied", "not_applied"]
    intent_event_id: str
    receipt_event_id: str | None
    provider_idempotency_key_hash: str | None


@dataclass(frozen=True)
class RecoveredArtifact:
    """Verified artifact publication metadata with no content bytes."""

    digest: str
    publication_id: str
    size: int
    artifact_type: str
    lifecycle_state: str
    node_id: str | None
    attempt_id: str | None


@dataclass(frozen=True)
class RecoveredRun:
    """Consistent lifecycle, Agent, budget, effect, artifact, and scheduler views."""

    lifecycle: RunLifecycleState
    agents: AgentRegistryState
    budget: BudgetBalance
    active_attempts: tuple[RecoveredAttemptLease, ...]
    effects: tuple[RecoveredEffect, ...] = ()
    artifacts: tuple[RecoveredArtifact, ...] = ()


class RunRecoveryCoordinator:
    """Rebuild and cross-check the durable control-plane views for one Run.

    This operation is read-only. It never resubmits work, releases a lease,
    settles a reservation, or guesses the outcome of an interrupted call.
    """

    def __init__(
        self,
        event_store: SQLiteEventStore,
        *,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self.event_store = event_store
        self.artifact_store = artifact_store

    def recover(self, run_id: str) -> RecoveredRun:
        try:
            return self._recover(run_id)
        except RunRecoveryError:
            raise
        except Exception as exc:
            # Do not echo payloads or backend exception strings: both can
            # contain prompt, path, or provider data.
            raise RunRecoveryError("Run recovery failed a durable-state invariant") from exc

    def _recover(self, run_id: str) -> RecoveredRun:
        lifecycle_controller = LifecycleController(self.event_store)
        lifecycle = lifecycle_controller.replay(run_id)
        if lifecycle.run_id != run_id:
            raise RunRecoveryError("lifecycle projection belongs to a different Run")

        agents = AgentRegistry(self.event_store).replay(run_id)
        config_snapshot = lifecycle_controller.config_snapshot(run_id)
        budget = BudgetLedger(
            self.event_store,
            {run_id: _run_limit(config_snapshot)},
        )
        balance = budget.replay(run_id)

        accepted, active = self._scheduler_state(run_id)
        attempts: dict[tuple[str, str], AttemptState] = {}
        for node in lifecycle.nodes:
            for attempt in node.attempts:
                key = (node.spec.node_id, attempt.attempt_id)
                if key in attempts:
                    raise RunRecoveryError("lifecycle projection duplicates an Attempt")
                attempts[key] = attempt

        if set(accepted) != set(attempts):
            raise RunRecoveryError("scheduler and lifecycle Attempt inventories disagree")
        if len(agents.instances) != len(attempts):
            raise RunRecoveryError("Agent Registry and lifecycle Attempt counts disagree")
        effects = self._recover_effects(run_id, attempts)
        for effect in effects:
            attempt = attempts[(effect.node_id, effect.attempt_id)]
            if effect.status == "outcome_unknown" and attempt.status in {
                "succeeded", "failed", "cancelled"
            }:
                raise RunRecoveryError("terminal Attempt has an unresolved external effect")
        artifacts = self._recover_artifacts(run_id, attempts)

        active_attempts: list[RecoveredAttemptLease] = []
        expected_agent_status = {
            "accepted": "active",
            "outcome_unknown": "outcome_unknown",
            "succeeded": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
        }
        for key, attempt in attempts.items():
            record = accepted[key]
            route = record["route"]
            scheduler_status = record["status"]
            if (
                record["agent_instance_id"] != attempt.agent_instance_id
                or record["fencing_generation"] != attempt.fencing_generation
                or record["decision_hash"] != attempt.decision_hash
                or record["reservation_id"] != attempt.reservation_id
                or route.run_id != run_id
                or route.node_id != key[0]
                or route.attempt_id != attempt.attempt_id
                or route.fencing_generation != attempt.fencing_generation
                or route.budget_reservation_id != attempt.reservation_id
                or route.model_id != attempt.model_id
                or route.provider_id != attempt.provider_id
                or route.registry_manifest_hash != lifecycle.registry_hash
                or route.reasoning_effort != attempt.reasoning_effort
            ):
                raise RunRecoveryError("accepted route and lifecycle Attempt bindings disagree")

            try:
                agent = agents.for_attempt(attempt.attempt_id)
            except KeyError as exc:
                raise RunRecoveryError("lifecycle Attempt has no Agent Registry entry") from exc
            expected_scheduler_status = (
                "active" if attempt.status == "accepted"
                else "outcome_unknown" if attempt.status == "outcome_unknown"
                else "released"
            )
            if scheduler_status != expected_scheduler_status:
                raise RunRecoveryError("scheduler lease and lifecycle Attempt states disagree")
            if (
                agent.agent_instance_id != attempt.agent_instance_id
                or agent.run_id != run_id
                or agent.node_id != key[0]
                or agent.fencing_generation != attempt.fencing_generation
                or agent.decision_hash != attempt.decision_hash
                or agent.policy_manifest_hash != attempt.policy_manifest_hash
                or agent.reasoning_effort != attempt.reasoning_effort
                or agent.model_id != attempt.model_id
                or agent.provider_id != attempt.provider_id
                or agent.status != expected_agent_status[attempt.status]
            ):
                raise RunRecoveryError("Agent Registry and lifecycle Attempt states disagree")

            if attempt.status in {"succeeded", "failed", "cancelled"}:
                if record.get("terminal_outcome") != attempt.status:
                    raise RunRecoveryError("released scheduler lease and terminal Attempt outcome disagree")
            elif record.get("terminal_outcome") is not None:
                raise RunRecoveryError("non-terminal Attempt has a terminal scheduler outcome")

            try:
                reservation = budget.get_reservation(attempt.reservation_id, run_id=run_id)
            except Exception as exc:
                raise RunRecoveryError("lifecycle Attempt has no budget reservation") from exc
            if attempt.status == "accepted" and reservation.status != "reserved":
                raise RunRecoveryError("active Attempt does not hold its reserved budget")
            if attempt.status == "outcome_unknown" and reservation.status != "unknown":
                raise RunRecoveryError("unknown Attempt does not hold unknown-outcome budget")
            if attempt.status == "succeeded" and reservation.status != "committed":
                raise RunRecoveryError("succeeded Attempt has no committed budget settlement")
            if attempt.status in {"failed", "cancelled"} and reservation.status not in {
                "committed", "released"
            }:
                raise RunRecoveryError("terminal Attempt has an unsettled budget reservation")

            if scheduler_status in {"active", "outcome_unknown"}:
                active_attempts.append(record["lease"])

        if lifecycle.status in {"succeeded", "failed", "cancelled"} and active_attempts:
            raise RunRecoveryError("terminal Run still has an active scheduler lease")
        if len(active_attempts) != agents.active_count:
            raise RunRecoveryError("Agent Registry and scheduler active counts disagree")
        if balance.reserved_minor < 0 or balance.unknown_minor < 0:
            raise RunRecoveryError("budget replay produced a negative held balance")
        return RecoveredRun(
            lifecycle=lifecycle,
            agents=agents,
            budget=balance,
            active_attempts=tuple(sorted(active_attempts, key=lambda item: item.attempt_ref)),
            effects=effects,
            artifacts=artifacts,
        )

    def _recover_effects(
        self,
        run_id: str,
        attempts: dict[tuple[str, str], AttemptState],
    ) -> tuple[RecoveredEffect, ...]:
        events = self.event_store.read_stream("budget", run_id)
        intents: dict[str, StoredEvent] = {}
        receipts: dict[str, StoredEvent] = {}
        consumed: dict[tuple[str, str], StoredEvent] = {}
        for event in events:
            payload = event.payload
            if event.event_type == "ApprovalGrantConsumed":
                grant_id = payload.get("approval_grant_id")
                effect_id = payload.get("effect_id")
                if isinstance(grant_id, str) and isinstance(effect_id, str):
                    consumed[(grant_id, effect_id)] = event
                continue
            if event.event_type == "EffectIntentRecorded":
                effect_id = payload.get("effect_id")
                if not isinstance(effect_id, str) or not effect_id.strip() or effect_id in intents:
                    raise RunRecoveryError("external effect intent identity is invalid or duplicated")
                key = (event.node_id, event.attempt_id)
                attempt = attempts.get(key)
                if (
                    attempt is None
                    or event.run_id != run_id
                    or event.fencing_generation != attempt.fencing_generation
                ):
                    raise RunRecoveryError("external effect intent references an unknown or stale Attempt")
                grant_id = payload.get("approval_grant_id")
                if grant_id is not None and (not isinstance(grant_id, str) or not grant_id.strip()):
                    raise RunRecoveryError("approved effect intent has an invalid grant reference")
                intents[effect_id] = event
                continue
            if event.event_type == "EffectReceiptRecorded":
                effect_id = payload.get("effect_id")
                intent = intents.get(effect_id) if isinstance(effect_id, str) else None
                if intent is None or effect_id in receipts:
                    raise RunRecoveryError("external effect receipt is orphaned or duplicated")
                if (
                    event.node_id != intent.node_id
                    or event.attempt_id != intent.attempt_id
                    or event.fencing_generation != intent.fencing_generation
                ):
                    raise RunRecoveryError("external effect receipt is bound to a stale Attempt")
                outcome = payload.get("outcome")
                if outcome is None and (payload.get("receipt_id") or payload.get("receipt_hash")):
                    outcome = "applied"  # compatibility with older receipt envelopes
                if outcome not in {"applied", "not_applied"}:
                    raise RunRecoveryError("external effect receipt has no valid outcome")
                receipt_hash = payload.get("receipt_hash")
                receipt_id = payload.get("receipt_id")
                has_receipt_evidence = (
                    isinstance(receipt_hash, str)
                    and re.fullmatch(r"sha256:[0-9a-f]{64}", receipt_hash) is not None
                ) or (isinstance(receipt_id, str) and bool(receipt_id.strip()))
                if not has_receipt_evidence:
                    raise RunRecoveryError("external effect receipt has no valid evidence reference")
                receipts[effect_id] = event

        recovered: list[RecoveredEffect] = []
        for effect_id, intent in intents.items():
            payload = intent.payload
            grant_id = payload.get("approval_grant_id")
            if grant_id is not None:
                consumed_event = consumed.get((grant_id, effect_id))
                if (
                    consumed_event is None
                    or consumed_event.stream_version <= intent.stream_version
                    or consumed_event.attempt_id != intent.attempt_id
                    or consumed_event.fencing_generation != intent.fencing_generation
                ):
                    raise RunRecoveryError("approved effect intent has no matching grant consumption")
            recovery_class = payload.get("recovery_class", "manual_only")
            if recovery_class not in {"idempotent", "queryable", "manual_only"}:
                raise RunRecoveryError("external effect has an unsupported recovery class")
            key_hash = payload.get("provider_idempotency_key_hash")
            if key_hash is None and isinstance(payload.get("provider_idempotency_key"), str):
                # Legacy events stored the key itself. Never copy it into the
                # recovered projection; expose only a one-way digest.
                key_hash = _sha256(payload["provider_idempotency_key"])
            if key_hash is not None and (
                not isinstance(key_hash, str)
                or re.fullmatch(r"sha256:[0-9a-f]{64}", key_hash) is None
            ):
                raise RunRecoveryError("external effect idempotency-key hash is invalid")
            receipt = receipts.get(effect_id)
            status = "outcome_unknown" if receipt is None else receipt.payload.get("outcome")
            if status is None and receipt is not None:
                status = "applied"
            recovered.append(
                RecoveredEffect(
                    effect_id=effect_id,
                    node_id=intent.node_id,
                    attempt_id=intent.attempt_id,
                    fencing_generation=intent.fencing_generation or 0,
                    recovery_class=recovery_class,
                    status=status,
                    intent_event_id=intent.event_id,
                    receipt_event_id=None if receipt is None else receipt.event_id,
                    provider_idempotency_key_hash=key_hash,
                )
            )
        return tuple(sorted(recovered, key=lambda item: item.effect_id))

    def _recover_artifacts(
        self,
        run_id: str,
        attempts: dict[tuple[str, str], AttemptState],
    ) -> tuple[RecoveredArtifact, ...]:
        has_publication = False
        for digest in self.event_store.stream_ids("artifact"):
            for event in self.event_store.read_stream("artifact", digest):
                source = event.payload.get("source")
                if (
                    event.event_type == "ArtifactPublished"
                    and isinstance(source, dict)
                    and source.get("run_id") == run_id
                ):
                    has_publication = True
                    break
            if has_publication:
                break
        if self.artifact_store is None:
            if has_publication:
                raise RunRecoveryError("Run has artifact publications but no artifact verifier is configured")
            return ()
        if getattr(self.artifact_store, "_event_store", None) is not self.event_store:
            raise RunRecoveryError("artifact verifier is bound to a different event store")
        records: list[ArtifactRecord] = self.artifact_store.verify_run_artifacts(run_id)
        recovered: list[RecoveredArtifact] = []
        for record in records:
            source = record.source
            node_id = source.get("node_id")
            attempt_id = source.get("attempt_id")
            generation = source.get("fencing_generation")
            if generation is not None and attempt_id is None:
                raise RunRecoveryError("artifact provenance has a fencing generation without an Attempt")
            if attempt_id is not None:
                if not isinstance(node_id, str) or not isinstance(attempt_id, str):
                    raise RunRecoveryError("artifact provenance has an incomplete Attempt identity")
                attempt = attempts.get((node_id, attempt_id))
                if attempt is None:
                    raise RunRecoveryError("artifact provenance references an unknown Attempt")
                if generation is not None and (
                    not isinstance(generation, str)
                    or not generation.isdecimal()
                    or int(generation) != attempt.fencing_generation
                ):
                    raise RunRecoveryError("artifact provenance has a stale fencing generation")
            recovered.append(
                RecoveredArtifact(
                    digest=record.digest,
                    publication_id=record.publication_id,
                    size=record.size,
                    artifact_type=record.artifact_type,
                    lifecycle_state=record.lifecycle_state,
                    node_id=node_id if isinstance(node_id, str) else None,
                    attempt_id=attempt_id if isinstance(attempt_id, str) else None,
                )
            )
        return tuple(recovered)

    def _scheduler_state(
        self, run_id: str
    ) -> tuple[dict[tuple[str, str], dict[str, object]], dict[str, dict[str, object]]]:
        attempts: dict[tuple[str, str], dict[str, object]] = {}
        by_ref: dict[str, dict[str, object]] = {}
        classifications: dict[str, tuple[StoredEvent, FailureClassification]] = {}
        recovery_plan_sources: set[str] = set()
        plans_by_authorization: dict[str, tuple[StoredEvent, RecoveryPlan]] = {}
        consumed_authorizations: set[str] = set()
        accepted_events_by_ref: dict[str, StoredEvent] = {}
        events = self.event_store.read_stream("scheduler", "global")
        for event in events:
            payload = event.payload
            if payload.get("run_id") != run_id:
                continue
            node_id = payload.get("node_id")
            attempt_id = payload.get("attempt_id")
            attempt_ref = payload.get("attempt_ref")
            if (
                event.run_id != run_id
                or not isinstance(node_id, str)
                or not isinstance(attempt_id, str)
                or not isinstance(attempt_ref, str)
                or event.node_id != node_id
                or event.attempt_id != attempt_id
                or event.fencing_generation is None
                or event.fencing_generation <= 0
                or attempt_ref != _attempt_ref(run_id, node_id, attempt_id)
            ):
                raise RunRecoveryError("scheduler event has invalid Run execution identity")
            key = (node_id, attempt_id)
            if event.event_type == "RoutingDecisionAccepted":
                if key in attempts or attempt_ref in by_ref:
                    raise RunRecoveryError("scheduler stream duplicates a route acceptance")
                try:
                    route = AcceptedModelRoute.model_validate(payload.get("accepted_route"))
                except (TypeError, ValueError) as exc:
                    raise RunRecoveryError("scheduler accepted route is invalid") from exc
                authorization_hash = payload.get("recovery_authorization_hash")
                recovery_action = payload.get("recovery_action", "initial")
                prior_decision_hash = payload.get("prior_decision_hash")
                if authorization_hash is None:
                    if recovery_action != "initial" or prior_decision_hash is not None:
                        raise RunRecoveryError("initial route has inconsistent recovery context")
                else:
                    persisted = plans_by_authorization.get(authorization_hash)
                    if persisted is None:
                        raise RunRecoveryError("accepted recovery route has no persisted plan")
                    plan_event, plan = persisted
                    if (
                        plan_event.run_id != run_id
                        or plan_event.payload.get("decision_hash") != prior_decision_hash
                        or plan.action != recovery_action
                        or route.model_id not in plan.authorized_model_ids
                        or authorization_hash in consumed_authorizations
                    ):
                        raise RunRecoveryError("accepted recovery route disagrees with its persisted plan")
                    source_classification = classifications.get(
                        plan_event.payload.get("attempt_ref")
                    )
                    evidence = (
                        None if source_classification is None
                        else source_classification[1].evidence
                    )
                    if (
                        evidence is None
                        or node_id != plan_event.node_id
                        or event.fencing_generation != plan_event.fencing_generation + 1
                        or payload.get("failure_category") != evidence.failure_category
                        or payload.get("retry_level") != evidence.retry_level + 1
                        or payload.get("exhausted_model_ids", []) != list(evidence.exhausted_model_ids)
                    ):
                        raise RunRecoveryError(
                            "accepted recovery route counters disagree with persisted failure evidence"
                        )
                    source_acceptance = accepted_events_by_ref.get(
                        plan_event.payload.get("attempt_ref")
                    )
                    if source_acceptance is None:
                        raise RunRecoveryError("accepted recovery plan source route is missing")
                    if (
                        recovery_action not in {
                            "same_model_retry", "same_tier_fallback", "capability_escalation"
                        }
                        or source_acceptance.payload.get("node_id") != node_id
                        or event.fencing_generation != source_acceptance.fencing_generation + 1
                    ):
                        raise RunRecoveryError(
                            "accepted recovery route is outside the supported same-node boundary"
                        )
                    consumed_authorizations.add(authorization_hash)
                accepted_at = _parse_aware_timestamp(payload.get("accepted_at"))
                lease_expires_at = _parse_aware_timestamp(payload.get("lease_expires_at"))
                if lease_expires_at <= accepted_at:
                    raise RunRecoveryError("scheduler lease expires before route acceptance")
                lease = RecoveredAttemptLease(
                    attempt_ref=attempt_ref,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    fencing_generation=event.fencing_generation,
                    status="active",
                    lease_expires_at=lease_expires_at.isoformat(),
                )
                record: dict[str, object] = {
                    "route": route,
                    "agent_instance_id": payload.get("agent_instance_id"),
                    "reservation_id": payload.get("reservation_id"),
                    "decision_hash": payload.get("decision_hash"),
                    "fencing_generation": event.fencing_generation,
                    "attempt_ref": attempt_ref,
                    "status": "active",
                    "lease": lease,
                    "terminal_outcome": None,
                }
                attempts[key] = record
                by_ref[attempt_ref] = record
                accepted_events_by_ref[attempt_ref] = event
                continue

            record = by_ref.get(attempt_ref)
            if record is None or (node_id, attempt_id) not in attempts:
                raise RunRecoveryError("scheduler event references an unknown accepted Attempt")
            lease = record["lease"]
            if (
                event.node_id != node_id
                or event.attempt_id != attempt_id
                or event.fencing_generation != record["fencing_generation"]
                or lease.attempt_ref != attempt_ref
            ):
                raise RunRecoveryError("scheduler Attempt event has stale execution context")
            if event.event_type == "AttemptOutcomeUnknown":
                if record["status"] != "active":
                    raise RunRecoveryError("AttemptOutcomeUnknown is out of order")
                record["status"] = "outcome_unknown"
                record["lease"] = lease.model_copy(update={"status": "outcome_unknown"})
            elif event.event_type == "AttemptSlotReleased":
                if record["status"] not in {"active", "outcome_unknown"}:
                    raise RunRecoveryError("AttemptSlotReleased is duplicated or out of order")
                outcome = payload.get("outcome")
                if outcome not in {"succeeded", "failed", "cancelled"}:
                    raise RunRecoveryError("released scheduler lease has no valid terminal outcome")
                record["status"] = "released"
                record["terminal_outcome"] = outcome
            elif event.event_type == "AttemptFailureClassified":
                if record["status"] not in {"released", "outcome_unknown"}:
                    raise RunRecoveryError("failure classification precedes a durable Attempt outcome")
                if payload.get("decision_hash") != record.get("decision_hash"):
                    raise RunRecoveryError("failure classification is bound to a different route")
                try:
                    classification = FailureClassification.model_validate_json(
                        json.dumps(payload.get("classification"))
                    )
                    failure = ModelGatewayFailure.model_validate(payload.get("failure"))
                except (TypeError, ValueError) as exc:
                    raise RunRecoveryError("persisted failure classification is invalid") from exc
                if failure.outcome == "unknown":
                    if record["status"] != "outcome_unknown" or classification.disposition != "reconciliation_required":
                        raise RunRecoveryError("unknown Gateway outcome lost its reconciliation gate")
                elif failure.outcome == "known_success":
                    if record["status"] != "outcome_unknown" or classification.disposition != "blocked":
                        raise RunRecoveryError("known-success settlement failure is not blocked")
                elif record.get("terminal_outcome") != "failed":
                    raise RunRecoveryError("known Gateway failure has no failed Attempt outcome")
                if attempt_ref in classifications:
                    raise RunRecoveryError("Attempt has duplicate failure classifications")
                classifications[attempt_ref] = (event, classification)
            elif event.event_type == "RecoveryPlanCreated":
                classified = classifications.get(attempt_ref)
                if classified is None:
                    raise RunRecoveryError("RecoveryPlan has no preceding failure classification")
                if attempt_ref in recovery_plan_sources:
                    raise RunRecoveryError("Attempt has duplicate RecoveryPlan events")
                classification_event, classification = classified
                if (
                    payload.get("decision_hash") != record.get("decision_hash")
                    or classification_event.payload.get("decision_hash") != payload.get("decision_hash")
                    or payload.get("evidence_hash") != (
                        None if classification.evidence is None else classification.evidence.evidence_hash
                    )
                ):
                    raise RunRecoveryError("RecoveryPlan is not bound to its failure evidence")
                try:
                    plan = RecoveryPlan.model_validate_json(json.dumps(payload.get("plan")))
                except (TypeError, ValueError) as exc:
                    raise RunRecoveryError("persisted RecoveryPlan is invalid") from exc
                authorization = plan.authorization
                if plan.outcome == "authorized":
                    if (
                        authorization is None
                        or authorization.source_decision_hash != record.get("decision_hash")
                        or authorization.evidence_hash != payload.get("evidence_hash")
                        or plan.action != authorization.action
                        or tuple(sorted(plan.authorized_model_ids))
                        != authorization.authorized_model_ids
                    ):
                        raise RunRecoveryError("persisted recovery authorization is inconsistent")
                    if authorization.authorization_hash in plans_by_authorization:
                        raise RunRecoveryError("recovery authorization is persisted more than once")
                    plans_by_authorization[authorization.authorization_hash] = (event, plan)
                elif authorization is not None:
                    raise RunRecoveryError("non-authorized RecoveryPlan carries an authorization")
                recovery_plan_sources.add(attempt_ref)
            else:
                raise RunRecoveryError("scheduler Run event type is unsupported during recovery")
        return attempts, by_ref


def _run_limit(snapshot: object) -> RunLimit:
    config = snapshot.resolved_config.config
    requested = config.presets[config.active_preset].requested_budget
    envelope = config.policy_envelope
    cost_caps = [
        value for value in (requested.max_cost_minor, envelope.max_cost_minor)
        if value is not None
    ]
    token_caps = [
        value for value in (requested.max_total_tokens, envelope.max_total_tokens)
        if value is not None
    ]
    return RunLimit(
        max_cost_minor=min(cost_caps) if cost_caps else None,
        max_tokens=min(token_caps) if token_caps else None,
        currency=envelope.currency,
    )


def _attempt_ref(run_id: str, node_id: str, attempt_id: str) -> str:
    value = json.dumps((run_id, node_id, attempt_id), ensure_ascii=False, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _parse_aware_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise RunRecoveryError("scheduler event timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RunRecoveryError("scheduler event timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RunRecoveryError("scheduler event timestamp lacks a timezone")
    return parsed


__all__ = [
    "RecoveredArtifact",
    "RecoveredAttemptLease",
    "RecoveredEffect",
    "RecoveredRun",
    "RunRecoveryCoordinator",
    "RunRecoveryError",
]
