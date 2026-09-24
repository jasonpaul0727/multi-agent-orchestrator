"""Deterministic recovery planning; the router only applies its authorization."""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, model_validator

from orchestrator.config.effective import EffectiveConfig, RecoveryAction, RoleName
from orchestrator.config.models import ModelRegistryManifest
from orchestrator.models import ModelGatewayFailure
from orchestrator.routing.engine import RecoveryAuthorization, RoutingDecision
from orchestrator.routing.planning import PlanningNodeContract


def _hash(value: object) -> str:
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class _RecoveryModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class RecoveryEvidence(_RecoveryModel):
    failure_category: Literal["transient", "output_invalid", "task_failure", "capability_failure"]
    evidence_hash: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    retry_level: StrictInt = Field(ge=0)
    retry_safe: StrictBool = False
    failed_model_unavailable: StrictBool = False
    outcome_unknown: StrictBool = False
    exhausted_model_ids: tuple[StrictStr, ...] = ()

    @model_validator(mode="after")
    def reject_unsafe_unknown_retry(self) -> "RecoveryEvidence":
        if self.outcome_unknown and self.retry_safe:
            raise ValueError("an unknown outcome cannot be marked retry-safe")
        return self


class RecoveryPlan(_RecoveryModel):
    """A controller decision, including whether a new role node is required."""

    outcome: Literal["authorized", "new_child_required", "blocked"]
    action: RecoveryAction | None
    authorization: RecoveryAuthorization | None
    target_role: RoleName | None
    authorized_model_ids: tuple[StrictStr, ...]
    reason: StrictStr


class FailureClassification(_RecoveryModel):
    """Sanitized deterministic classification handed to recovery planning."""

    disposition: Literal["classified", "reconciliation_required", "blocked"]
    reason: StrictStr
    evidence: RecoveryEvidence | None

    @model_validator(mode="after")
    def validate_disposition_evidence(self) -> "FailureClassification":
        if self.disposition == "blocked" and self.evidence is not None:
            raise ValueError("blocked classifications cannot carry recovery evidence")
        if self.disposition != "blocked" and self.evidence is None:
            raise ValueError("classified failures require recovery evidence")
        if self.disposition == "reconciliation_required" and not self.evidence.outcome_unknown:
            raise ValueError("reconciliation requires an unknown provider outcome")
        if self.disposition == "classified" and self.evidence.outcome_unknown:
            raise ValueError("unknown provider outcomes require reconciliation")
        return self


def classify_gateway_failure(
    failure: ModelGatewayFailure,
    *,
    source_decision: RoutingDecision,
    retry_level: int,
    exhausted_model_ids: tuple[str, ...] = (),
) -> FailureClassification:
    """Turn sanitized Gateway facts into bounded RecoveryController evidence.

    Raw provider bodies and request IDs are deliberately excluded. Unknown
    outcomes always require reconciliation before any retry/fallback plan.
    """

    if source_decision.outcome != "selected" or source_decision.selected_model_id is None:
        return FailureClassification(
            disposition="blocked", reason="source_decision_not_selected", evidence=None
        )
    if failure.outcome == "known_success":
        return FailureClassification(
            disposition="blocked", reason="successful_call_requires_settlement", evidence=None
        )
    if failure.outcome == "unknown":
        category = _failure_category(failure.code) or "transient"
        disposition: Literal["classified", "reconciliation_required", "blocked"] = (
            "reconciliation_required"
        )
        reason = "provider_outcome_unknown"
    else:
        category = _failure_category(failure.code)
        if category is None:
            return FailureClassification(
                disposition="blocked", reason="failure_not_recoverable", evidence=None
            )
        disposition = "classified"
        reason = "gateway_failure_classified"

    provider_code_hash = (
        None if failure.provider_code is None else _hash({"provider_code": failure.provider_code})
    )
    evidence_hash = _hash(
        {
            "source_decision_hash": source_decision.decision_hash,
            "model_id": source_decision.selected_model_id,
            "run_id": source_decision.run_id,
            "node_id": source_decision.node_id,
            "attempt_id": source_decision.attempt_id,
            "fencing_generation": source_decision.fencing_generation,
            "failure_code": failure.code,
            "failure_phase": failure.phase,
            "failure_outcome": failure.outcome,
            "failure_retryable": failure.retryable,
            "http_status": failure.http_status,
            "retry_after_ms": failure.retry_after_ms,
            "provider_code_hash": provider_code_hash,
            "retry_level": retry_level,
            "exhausted_model_ids": sorted(exhausted_model_ids),
        }
    )
    evidence = RecoveryEvidence(
        failure_category=category,
        evidence_hash=evidence_hash,
        retry_level=retry_level,
        retry_safe=failure.retryable and failure.outcome in {"not_sent", "known_failure"},
        failed_model_unavailable=(failure.code == "provider_unavailable"),
        outcome_unknown=failure.outcome == "unknown",
        exhausted_model_ids=exhausted_model_ids,
    )
    return FailureClassification(disposition=disposition, reason=reason, evidence=evidence)


def _failure_category(code: str) -> Literal[
    "transient", "output_invalid", "task_failure", "capability_failure"
] | None:
    if code in {"rate_limited", "provider_unavailable", "timeout", "transport_error", "outcome_unknown"}:
        return "transient"
    if code == "invalid_response":
        return "output_invalid"
    if code in {"context_length_exceeded", "output_limit_exceeded"}:
        return "capability_failure"
    return None


class RecoveryController:
    """Map validated failure evidence to a bounded, auditable recovery action."""

    def plan(
        self,
        *,
        source_decision: RoutingDecision,
        contract: PlanningNodeContract,
        config: EffectiveConfig,
        registry: ModelRegistryManifest,
        evidence: RecoveryEvidence,
    ) -> RecoveryPlan:
        if source_decision.outcome != "selected" or source_decision.selected_model_id is None:
            return _blocked("source_decision_not_selected")
        if (
            source_decision.run_id != contract.run_id
            or source_decision.node_id != contract.node_id
            or source_decision.planning_contract_hash != contract.contract_hash
            or source_decision.config_hash != config.content_hash
            or source_decision.registry_hash != registry.content_hash
        ):
            return _blocked("stale_recovery_source")
        if evidence.outcome_unknown:
            return _blocked("unknown_outcome_requires_reconciliation")
        preset = config.presets.get(config.active_preset)
        profile = None if preset is None else preset.roles.get(contract.role)
        if profile is None:
            return _blocked("recovery_role_not_enabled")
        model_by_id = {model.id: model for model in registry.models}
        failed_model = model_by_id.get(source_decision.selected_model_id)
        if failed_model is None:
            return _blocked("source_model_missing")
        if len(evidence.exhausted_model_ids) != len(set(evidence.exhausted_model_ids)):
            return _blocked("duplicate_exhausted_model")

        candidates = tuple(item.model_id for item in source_decision.candidate_assessments)
        if evidence.failure_category == "transient":
            if evidence.retry_safe and not evidence.failed_model_unavailable and evidence.retry_level < profile.retries:
                return self._authorization(
                    action="same_model_retry",
                    source_decision=source_decision,
                    policy_hash=source_decision.policy_manifest_hash,
                    evidence=evidence,
                    previous_model=failed_model.id,
                    model_ids=(failed_model.id,),
                    reason="bounded_same_model_retry",
                )
            if not evidence.failed_model_unavailable and evidence.retry_level < profile.retries:
                return _blocked("transient_outcome_requires_reconciliation")
            fallback_ids = tuple(
                model_id
                for model_id in candidates
                if model_id != failed_model.id
                and model_id not in evidence.exhausted_model_ids
                and model_id in model_by_id
                and model_by_id[model_id].tier == failed_model.tier
                and failed_model.capabilities.issubset(model_by_id[model_id].capabilities)
            )
            if fallback_ids:
                return self._authorization(
                    action="same_tier_fallback",
                    source_decision=source_decision,
                    policy_hash=source_decision.policy_manifest_hash,
                    evidence=evidence,
                    previous_model=failed_model.id,
                    model_ids=fallback_ids,
                    reason="same_tier_fallback_after_unavailable_or_retry_exhausted",
                )
            return _blocked("same_tier_fallback_exhausted")

        if evidence.failure_category in {"output_invalid", "task_failure"}:
            return RecoveryPlan(
                outcome="new_child_required",
                action=None,
                authorization=None,
                target_role=None,
                authorized_model_ids=(),
                reason="targeted_repair_or_reaggregation_node_required",
            )

        target = profile.escalate_to
        if target in {"reviewer", "director"}:
            role: RoleName = target  # type: ignore[assignment]
            target_profile = preset.roles.get(role)
            if target_profile is None:
                return _blocked("escalation_role_not_enabled")
            model_ids = _expand_profile_candidates(target_profile.candidates, registry)
            action: RecoveryAction = "reviewer_node" if role == "reviewer" else "director_node"
            authorization = RecoveryAuthorization.create(
                action=action,
                source_decision_hash=source_decision.decision_hash,
                previous_model_id=failed_model.id,
                authorized_model_ids=model_ids,
                policy_manifest_hash=source_decision.policy_manifest_hash,
                evidence_hash=evidence.evidence_hash,
            ) if model_ids else None
            if authorization is None:
                return _blocked("escalation_role_has_no_candidates")
            return RecoveryPlan(
                outcome="new_child_required",
                action=action,
                authorization=authorization,
                target_role=role,
                authorized_model_ids=model_ids,
                reason="independent_role_node_required",
            )

        if target is None or not target.startswith("tier:"):
            return _blocked("capability_escalation_not_configured")
        target_tier = target[5:]
        tier_order = {"economy": 0, "standard": 1, "high": 2}
        target_order = tier_order[target_tier]
        escalation_ids = tuple(
            model_id
            for model_id in _expand_profile_candidates((target,), registry)
            if model_id != failed_model.id
            and model_id not in evidence.exhausted_model_ids
            and model_id in model_by_id
            and tier_order[model_by_id[model_id].tier] > tier_order[failed_model.tier]
            and tier_order[model_by_id[model_id].tier] >= target_order
            and failed_model.capabilities.issubset(model_by_id[model_id].capabilities)
        )
        if not escalation_ids:
            return _blocked("capability_escalation_candidates_exhausted")
        return self._authorization(
            action="capability_escalation",
            source_decision=source_decision,
            policy_hash=source_decision.policy_manifest_hash,
            evidence=evidence,
            previous_model=failed_model.id,
            model_ids=escalation_ids,
            reason="evidence_bound_higher_tier_capability_escalation",
        )

    @staticmethod
    def _authorization(
        *,
        action: RecoveryAction,
        source_decision: RoutingDecision,
        policy_hash: str,
        evidence: RecoveryEvidence,
        previous_model: str,
        model_ids: tuple[str, ...],
        reason: str,
    ) -> RecoveryPlan:
        if not model_ids:
            return _blocked("recovery_candidates_empty")
        auth_action = action
        if auth_action == "initial":
            return _blocked("initial_is_not_a_recovery_action")
        authorization = RecoveryAuthorization.create(
            action=auth_action,  # type: ignore[arg-type]
            source_decision_hash=source_decision.decision_hash,
            previous_model_id=previous_model,
            authorized_model_ids=model_ids,
            policy_manifest_hash=policy_hash,
            evidence_hash=evidence.evidence_hash,
        )
        return RecoveryPlan(
            outcome="authorized",
            action=auth_action,
            authorization=authorization,
            target_role=None,
            authorized_model_ids=tuple(sorted(model_ids)),
            reason=reason,
        )


def _expand_profile_candidates(candidates: tuple[str, ...], registry: ModelRegistryManifest) -> tuple[str, ...]:
    output: list[str] = []
    for candidate in candidates:
        if candidate.startswith("tier:"):
            tier = candidate.removeprefix("tier:")
            matches = sorted(model.id for model in registry.models if model.tier == tier)
        else:
            matches = [candidate] if any(model.id == candidate for model in registry.models) else []
        output.extend(item for item in matches if item not in output)
    return tuple(output)


def _blocked(reason: str) -> RecoveryPlan:
    return RecoveryPlan(
        outcome="blocked",
        action=None,
        authorization=None,
        target_role=None,
        authorized_model_ids=(),
        reason=reason,
    )


__all__ = [
    "FailureClassification",
    "RecoveryController",
    "RecoveryEvidence",
    "RecoveryPlan",
    "classify_gateway_failure",
]
