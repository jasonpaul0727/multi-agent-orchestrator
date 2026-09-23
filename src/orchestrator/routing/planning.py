"""Deterministic Planning-stage classification and frozen node constraints."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator

from orchestrator.config.effective import (
    Complexity,
    EffectiveConfig,
    GuardConstraints,
    GuardRule,
    PolicyEnvelope,
    PresetName,
    RoleName,
    RoleProfile,
    Risk,
    TaskClass,
)
from orchestrator.config.models import ModelRegistryManifest, tighten_policy_envelopes
from orchestrator.routing.classifier import ClassificationResult, TaskClassifier
from orchestrator.security import PolicyManifest
from orchestrator.validation import revalidate_model


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_TIER_ORDER = {"economy": 0, "standard": 1, "high": 2}


def _hash(payload: object) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class PlanningError(ValueError):
    """Stable, non-sensitive reason Planning could not freeze a node contract."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class PlanningNodeContract(BaseModel):
    """Self-contained immutable policy and candidate envelope for a Ready node."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    schema_version: Literal[1] = 1
    run_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    role: RoleName
    preset: PresetName
    classification: ClassificationResult
    config_hash: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    registry_hash: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    policy_manifest_hash: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    candidate_model_ids: tuple[StrictStr, ...] = Field(min_length=1)
    required_capabilities: tuple[StrictStr, ...]
    context_tokens: StrictInt = Field(ge=0)
    max_output_tokens: StrictInt = Field(gt=0)
    reasoning_effort: StrictStr = Field(min_length=1)
    policy_envelope: PolicyEnvelope
    require_independent_review: StrictBool
    require_approval: StrictBool
    max_parallel_candidates: StrictInt = Field(ge=0)
    matched_guard_rule_ids: tuple[StrictStr, ...]

    @field_validator(
        "candidate_model_ids", "required_capabilities", "matched_guard_rule_ids", mode="before"
    )
    @classmethod
    def normalize_tuples(cls, value: object, info: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must be an array")
        return tuple(value)

    @field_validator("candidate_model_ids", "required_capabilities")
    @classmethod
    def validate_identifiers(cls, value: tuple[str, ...], info: object) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must be unique")
        if any(item != item.strip() or not _IDENTIFIER.fullmatch(item) for item in value):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must contain stable identifiers")
        return value

    @property
    def contract_hash(self) -> str:
        return _hash(self.model_dump(mode="json"))


def compile_node_contract(
    *,
    run_id: str,
    node_id: str,
    role: RoleName,
    task_text: str,
    config: EffectiveConfig,
    registry: ModelRegistryManifest,
    policy_manifest: PolicyManifest,
    context_tokens: int,
    max_output_tokens: int | None = None,
    required_capabilities: tuple[str, ...] = (),
) -> PlanningNodeContract:
    """Classify a task, apply all matching guards, and freeze its node envelope."""

    config = revalidate_model(EffectiveConfig, config)
    policy_manifest = revalidate_model(PolicyManifest, policy_manifest)
    if config.registry_manifest_ref != registry.content_hash:
        raise PlanningError("registry_snapshot_mismatch")
    registry = revalidate_model(ModelRegistryManifest, registry)
    if context_tokens < 0 or (max_output_tokens is not None and max_output_tokens <= 0):
        raise PlanningError("invalid_token_requirement")
    classifier = TaskClassifier(config.classifier)
    classification = classifier.classify(task_text)
    preset = config.presets[config.active_preset]
    profile: RoleProfile | None = preset.roles.get(role)
    if profile is None:
        raise PlanningError("role_not_enabled")
    planned_output_tokens = max_output_tokens or profile.max_output_tokens
    if planned_output_tokens > profile.max_output_tokens:
        raise PlanningError("output_requirement_exceeds_role_limit")
    if len(set(required_capabilities)) != len(required_capabilities):
        raise PlanningError("duplicate_required_capability")

    model_by_id = {model.id: model for model in registry.models}
    providers = {provider.id: provider for provider in registry.providers}
    candidate_ids = _expand_candidates(profile.candidates, registry)
    if not candidate_ids:
        raise PlanningError("role_has_no_registered_candidates")
    if any(
        model_id not in model_by_id
        or model_by_id[model_id].provider not in providers
        or not providers[model_by_id[model_id].provider].enabled
        for model_id in candidate_ids
    ):
        raise PlanningError("role_candidate_provider_disabled")

    currency = config.policy_envelope.currency
    if currency is None:
        raise PlanningError("policy_currency_missing")
    constraints = [
        config.policy_envelope,
        _requested_budget_envelope(preset.requested_budget, currency),
    ]
    matching_guards: list[tuple[str, GuardConstraints]] = []
    for rule in (*config.mandatory_guard_rules, *preset.guard_rules):
        if _guard_matches(
            rule,
            role=role,
            task_class=classification.task_class,
            complexity=classification.complexity,
            risk=classification.risk,
            required_capabilities=set(required_capabilities),
            context_tokens=context_tokens,
            output_tokens=planned_output_tokens,
        ):
            matching_guards.append((rule.id, rule.constraints))
            constraints.append(_guard_envelope(rule.constraints, currency))

    # Non-disableable baseline guards apply even to custom config manifests.
    mandatory_floor: str | None = None
    requires_review = False
    if classification.risk == "critical":
        mandatory_floor = "high"
        requires_review = True
    elif classification.risk == "high" or classification.complexity == "unknown":
        mandatory_floor = "standard"
        requires_review = True
    if mandatory_floor is not None:
        constraints.append(PolicyEnvelope(min_tier=mandatory_floor))
        matching_guards.append(("system:classification-risk-floor", GuardConstraints(min_tier=mandatory_floor)))
    effective_policy = tighten_policy_envelopes(*constraints)
    requires_review = requires_review or effective_policy.require_independent_review is True

    guard_requires_approval = any(
        constraints_for_guard.require_approval is True
        for _, constraints_for_guard in matching_guards
    )
    return PlanningNodeContract(
        run_id=run_id,
        node_id=node_id,
        role=role,
        preset=config.active_preset,
        classification=classification,
        config_hash=config.content_hash,
        registry_hash=registry.content_hash,
        policy_manifest_hash=policy_manifest.content_hash,
        candidate_model_ids=candidate_ids,
        required_capabilities=tuple(sorted(required_capabilities)),
        context_tokens=context_tokens,
        max_output_tokens=planned_output_tokens,
        reasoning_effort=profile.reasoning_effort,
        policy_envelope=effective_policy,
        require_independent_review=requires_review,
        require_approval=(effective_policy.require_approval is True or guard_requires_approval),
        max_parallel_candidates=effective_policy.max_parallel_candidates or 0,
        matched_guard_rule_ids=tuple(rule_id for rule_id, _ in matching_guards),
    )


def _expand_candidates(
    candidates: tuple[str, ...], registry: ModelRegistryManifest
) -> tuple[str, ...]:
    ids: list[str] = []
    for candidate in candidates:
        if candidate.startswith("tier:"):
            tier = candidate[5:]
            matches = sorted(model.id for model in registry.models if model.tier == tier)
        else:
            matches = [candidate] if any(model.id == candidate for model in registry.models) else []
        for model_id in matches:
            if model_id not in ids:
                ids.append(model_id)
    return tuple(ids)


def _requested_budget_envelope(requested: object, currency: str) -> PolicyEnvelope:
    fields = (
        "max_cost_minor",
        "max_total_tokens",
        "max_agents",
        "max_depth",
        "max_concurrency",
        "max_parallel_candidates",
    )
    values = {name: getattr(requested, name) for name in fields if getattr(requested, name) is not None}
    return PolicyEnvelope(currency=currency, **values)


def _guard_envelope(constraints: GuardConstraints, currency: str) -> PolicyEnvelope:
    return PolicyEnvelope(currency=currency, **constraints.model_dump(mode="python", exclude_none=True))


def _guard_matches(
    rule: GuardRule,
    *,
    role: str,
    task_class: TaskClass,
    complexity: Complexity,
    risk: Risk,
    required_capabilities: set[str],
    context_tokens: int,
    output_tokens: int,
) -> bool:
    conditions = rule.when
    return (
        (conditions.role is None or role in conditions.role)
        and (conditions.task_class is None or task_class in conditions.task_class)
        and (conditions.complexity is None or complexity in conditions.complexity)
        and (conditions.risk is None or risk in conditions.risk)
        and (
            conditions.required_capabilities_all is None
            or set(conditions.required_capabilities_all).issubset(required_capabilities)
        )
        and (
            conditions.context_tokens is None
            or conditions.context_tokens.minimum <= context_tokens <= conditions.context_tokens.maximum
        )
        and (
            conditions.output_tokens is None
            or conditions.output_tokens.minimum <= output_tokens <= conditions.output_tokens.maximum
        )
    )


__all__ = ["PlanningError", "PlanningNodeContract", "compile_node_contract"]
