"""Replayable candidate filtering and deterministic model routing."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.budget.models import CostEstimate
from orchestrator.config.effective import (
    EffectiveConfig,
    HealthState,
    RecoveryAction,
    RoleProfile,
    SelectorRule,
    SelectorTarget,
)
from orchestrator.config.models import ModelRegistryManifest, ModelSpec, ProviderSpec, ReasoningEffort
from orchestrator.models import (
    CostingDataUnavailable,
    FXSnapshot,
    TokenizerSnapshot,
    estimate_model_cost,
)
from orchestrator.routing.health import HealthAggregateKey, HealthAggregateRef, ProbeLease
from orchestrator.routing.classifier import ClassificationResult
from orchestrator.routing.planning import PlanningNodeContract
from orchestrator.security import PolicyDecision, PolicyEngine, PolicyManifest, PolicyRequest
from orchestrator.validation import revalidate_model


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_TIER_ORDER = {"economy": 0, "standard": 1, "high": 2}
_HEALTH_ORDER = {"healthy": 0, "degraded": 1, "half_open": 2, "open": 3}
_REASON_ORDER = (
    "provider_disabled",
    "model_denied",
    "provider_denied",
    "model_not_allowlisted",
    "provider_not_allowlisted",
    "tier_below_minimum",
    "required_capability_missing",
    "capability_not_allowlisted",
    "context_window_exceeded",
    "output_limit_exceeded",
    "reasoning_effort_unsupported",
    "policy_denied",
    "approval_required",
    "health_probe_lease_required",
    "credential_unavailable",
    "lifecycle_circuit_open",
    "health_snapshot_missing",
    "provider_unhealthy",
    "model_unhealthy",
    "health_degraded_preferred_healthy",
    "budget_currency_mismatch",
    "costing_unavailable",
    "cost_limit_exceeded",
    "token_limit_exceeded",
    "recovery_action_not_supported",
    "recovery_authorization_mismatch",
    "recovery_candidate_exhausted",
)


def _hash(payload: object) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("routing_as_of_event_time must include a UTC offset")
    return value


class _RoutingModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class ModelHealthSnapshot(_RoutingModel):
    provider_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    model_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    provider: HealthAggregateRef
    model: HealthAggregateRef

    @property
    def effective_state(self) -> HealthState:
        if _HEALTH_ORDER[self.provider.state] >= _HEALTH_ORDER[self.model.state]:
            return self.provider.state
        return self.model.state


class SecretAvailability(_RoutingModel):
    secret_ref: StrictStr = Field(min_length=1, pattern=r"^(?:env:[A-Za-z_][A-Za-z0-9_]*|(?:keyring|plugin):[^\s=]+)$")
    available: StrictBool
    version: StrictInt = Field(ge=0)


class EligibilityBlock(_RoutingModel):
    subject_kind: Literal["model", "provider"]
    subject_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    reason: Literal["lifecycle_circuit_open", "temporarily_unavailable"]


class EligibilitySnapshot(_RoutingModel):
    """Versioned dynamic facts used as the complete router eligibility input."""

    schema_version: Literal[1] = 1
    policy_manifest_hash: StrictStr = Field(pattern=_HASH.pattern)
    budget_ledger_version: StrictInt = Field(ge=0)
    budget_currency: StrictStr = Field(pattern=r"^[A-Z]{3}$")
    available_cost_minor: StrictInt = Field(ge=0)
    available_tokens: StrictInt | None = Field(default=None, ge=0)
    secret_availability: tuple[SecretAvailability, ...]
    model_health: tuple[ModelHealthSnapshot, ...]
    blocks: tuple[EligibilityBlock, ...] = ()
    revocation_version: StrictInt = Field(ge=0)
    emergency_deny_version: StrictInt = Field(ge=0)
    authorization_revoked: StrictBool = False
    emergency_denied_model_ids: tuple[StrictStr, ...] = ()

    @field_validator(
        "secret_availability", "model_health", "blocks", "emergency_denied_model_ids", mode="before"
    )
    @classmethod
    def normalize_arrays(cls, value: object, info: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must be an array")
        values = tuple(value)
        return tuple(sorted(values)) if getattr(info, "field_name", None) == "emergency_denied_model_ids" else values

    @model_validator(mode="after")
    def validate_unique_entries(self) -> "EligibilitySnapshot":
        secrets = [entry.secret_ref for entry in self.secret_availability]
        health = [(entry.provider_id, entry.model_id) for entry in self.model_health]
        blocks = [(entry.subject_kind, entry.subject_id, entry.reason) for entry in self.blocks]
        if len(secrets) != len(set(secrets)):
            raise ValueError("secret availability entries must be unique by secret_ref")
        model_ids = [entry.model_id for entry in self.model_health]
        if len(health) != len(set(health)) or len(model_ids) != len(set(model_ids)):
            raise ValueError("health entries must be unique by provider/model")
        provider_refs: dict[str, HealthAggregateRef] = {}
        for entry in self.model_health:
            prior = provider_refs.setdefault(entry.provider_id, entry.provider)
            if prior != entry.provider:
                raise ValueError("provider health references must be consistent across models")
        if len(blocks) != len(set(blocks)):
            raise ValueError("eligibility blocks must be unique")
        if len(self.emergency_denied_model_ids) != len(set(self.emergency_denied_model_ids)):
            raise ValueError("emergency denied models must be unique")
        return self

    @property
    def snapshot_id(self) -> str:
        payload = self.model_dump(mode="json")
        payload["secret_availability"] = sorted(payload["secret_availability"], key=lambda item: item["secret_ref"])
        payload["model_health"] = sorted(
            payload["model_health"], key=lambda item: (item["provider_id"], item["model_id"])
        )
        payload["blocks"] = sorted(
            payload["blocks"], key=lambda item: (item["subject_kind"], item["subject_id"], item["reason"])
        )
        return _hash(payload)


class RecoveryAuthorization(_RoutingModel):
    """Content-addressed Controller authorization; the Router cannot invent it."""

    action: Literal[
        "same_model_retry",
        "same_tier_fallback",
        "capability_escalation",
        "reviewer_node",
        "director_node",
        "health_probe",
    ]
    source_decision_hash: StrictStr
    previous_model_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    authorized_model_ids: tuple[StrictStr, ...] = Field(min_length=1)
    policy_manifest_hash: StrictStr = Field(pattern=_HASH.pattern)
    evidence_hash: StrictStr = Field(pattern=_HASH.pattern)
    authorization_hash: StrictStr = Field(pattern=_HASH.pattern)

    @field_validator("source_decision_hash")
    @classmethod
    def validate_source_hash(cls, value: str) -> str:
        if not _HASH.fullmatch(value):
            raise ValueError("source_decision_hash must be a sha256 hash")
        return value

    @field_validator("authorized_model_ids", mode="before")
    @classmethod
    def normalize_models(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("authorized_model_ids must be an array")
        return tuple(value)

    @field_validator("authorized_model_ids")
    @classmethod
    def unique_authorized_models(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)) or any(not _IDENTIFIER.fullmatch(item) for item in value):
            raise ValueError("authorized_model_ids must be unique stable identifiers")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def verify_authorization_hash(self) -> "RecoveryAuthorization":
        payload = self.model_dump(mode="json", exclude={"authorization_hash"})
        if self.authorization_hash != _hash(payload):
            raise ValueError("authorization_hash does not match its recovery authorization")
        return self

    @classmethod
    def create(
        cls,
        *,
        action: Literal[
            "same_model_retry",
            "same_tier_fallback",
            "capability_escalation",
            "reviewer_node",
            "director_node",
            "health_probe",
        ],
        source_decision_hash: str,
        previous_model_id: str,
        authorized_model_ids: tuple[str, ...],
        policy_manifest_hash: str,
        evidence_hash: str,
    ) -> "RecoveryAuthorization":
        payload = {
            "action": action,
            "source_decision_hash": source_decision_hash,
            "previous_model_id": previous_model_id,
            "authorized_model_ids": authorized_model_ids,
            "policy_manifest_hash": policy_manifest_hash,
            "evidence_hash": evidence_hash,
        }
        return cls(**payload, authorization_hash=_hash(payload))


class RoutingRequest(_RoutingModel):
    """Immutable dispatch input; changed dynamic facts require a new request."""

    request_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    run_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    attempt_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    fencing_generation: StrictInt = Field(ge=0)
    config_hash: StrictStr = Field(pattern=_HASH.pattern)
    registry_hash: StrictStr = Field(pattern=_HASH.pattern)
    planning_contract_hash: StrictStr = Field(pattern=_HASH.pattern)
    policy_manifest_hash: StrictStr = Field(pattern=_HASH.pattern)
    routing_as_of_event_time: datetime
    prior_decision_hash: StrictStr | None = None
    previous_model_id: StrictStr | None = None
    recovery_action: RecoveryAction = "initial"
    recovery_authorization: RecoveryAuthorization | None = None
    probe_lease: ProbeLease | None = None
    failure_category: Literal["transient", "output_invalid", "task_failure", "capability_failure"] | None = None
    retry_level: StrictInt = Field(default=0, ge=0)
    exhausted_model_ids: tuple[StrictStr, ...] = ()
    health_state: HealthState | None = None

    @field_validator("routing_as_of_event_time")
    @classmethod
    def require_aware_time(cls, value: datetime) -> datetime:
        return _aware(value)

    @field_validator("prior_decision_hash")
    @classmethod
    def validate_prior_decision_hash(cls, value: str | None) -> str | None:
        if value is not None and not _HASH.fullmatch(value):
            raise ValueError("prior_decision_hash must be a sha256 hash")
        return value

    @field_validator("previous_model_id")
    @classmethod
    def validate_previous_model_id(cls, value: str | None) -> str | None:
        if value is not None and not _IDENTIFIER.fullmatch(value):
            raise ValueError("previous_model_id must be a stable identifier")
        return value

    @field_validator("exhausted_model_ids", mode="before")
    @classmethod
    def normalize_exhausted(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("exhausted_model_ids must be an array")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def validate_recovery(self) -> "RoutingRequest":
        authorization = self.recovery_authorization
        if self.recovery_action == "initial" and authorization is not None:
            raise ValueError("initial routing must not carry recovery authorization")
        if self.recovery_action == "initial" and (
            self.prior_decision_hash is not None or self.previous_model_id is not None
        ):
            raise ValueError("initial routing must not carry a prior decision")
        if self.recovery_action != "initial":
            if authorization is None or authorization.action != self.recovery_action:
                raise ValueError("non-initial routing requires matching recovery authorization")
            if authorization.policy_manifest_hash != self.policy_manifest_hash:
                raise ValueError("recovery authorization belongs to a different policy version")
            if (
                self.prior_decision_hash != authorization.source_decision_hash
                or self.previous_model_id != authorization.previous_model_id
            ):
                raise ValueError("recovery authorization is not bound to the prior decision")
        if self.recovery_action != "health_probe" and self.probe_lease is not None:
            raise ValueError("only health probes may carry a ProbeLease")
        if self.recovery_action == "health_probe" and self.probe_lease is not None:
            if authorization is None or set(authorization.authorized_model_ids) != {self.probe_lease.model_id}:
                raise ValueError("health probe authorization must target exactly its ProbeLease model")
        if len(self.exhausted_model_ids) != len(set(self.exhausted_model_ids)):
            raise ValueError("exhausted_model_ids must be unique")
        return self

    @property
    def request_hash(self) -> str:
        return _hash(self.model_dump(mode="json"))


class CandidateAssessment(_RoutingModel):
    model_id: StrictStr
    provider_id: StrictStr
    configured_rank: StrictInt = Field(ge=0)
    eligible: StrictBool
    estimated_cost: CostEstimate | None = None
    health_state: HealthState | None = None
    provider_health: HealthAggregateRef | None = None
    model_health: HealthAggregateRef | None = None
    policy_decision: PolicyDecision
    exclusion_reasons: tuple[StrictStr, ...]


class RoutingDecision(_RoutingModel):
    """Complete, inspectable decision; eligible order is stable across replay."""

    schema_version: Literal[1] = 1
    request_id: StrictStr
    request_hash: StrictStr = Field(pattern=_HASH.pattern)
    run_id: StrictStr
    node_id: StrictStr
    attempt_id: StrictStr
    fencing_generation: StrictInt = Field(ge=0)
    config_hash: StrictStr = Field(pattern=_HASH.pattern)
    registry_hash: StrictStr = Field(pattern=_HASH.pattern)
    planning_contract_hash: StrictStr = Field(pattern=_HASH.pattern)
    policy_manifest_hash: StrictStr = Field(pattern=_HASH.pattern)
    eligibility_snapshot_id: StrictStr = Field(pattern=_HASH.pattern)
    matched_selector_rule_id: StrictStr | None
    candidate_assessments: tuple[CandidateAssessment, ...]
    eligible_order: tuple[StrictStr, ...]
    reasoning_effort: ReasoningEffort
    outcome: Literal["selected", "blocked"]
    selected_model_id: StrictStr | None
    selected_provider_id: StrictStr | None
    blocked_reason: Literal[
        "credential_unavailable",
        "policy_denied",
        "approval_required",
        "health_probe_lease_required",
        "budget_unavailable",
        "all_candidates_unhealthy",
        "capability_unavailable",
        "limit_exhausted",
    ] | None

    @field_validator("candidate_assessments", "eligible_order", mode="before")
    @classmethod
    def normalize_arrays(cls, value: object, info: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def validate_outcome(self) -> "RoutingDecision":
        if self.outcome == "selected":
            if not self.selected_model_id or not self.selected_provider_id or not self.eligible_order:
                raise ValueError("selected decision requires one eligible model and provider")
            if self.selected_model_id != self.eligible_order[0] or self.blocked_reason is not None:
                raise ValueError("selected model must be first in eligible order")
        elif self.selected_model_id is not None or self.selected_provider_id is not None or self.blocked_reason is None:
            raise ValueError("blocked decision requires a reason and no selected model")
        return self

    @property
    def decision_hash(self) -> str:
        return _hash(self.model_dump(mode="json"))


class ModelRouter:
    """Pure route/filter/sort operation. It does not persist or dispatch a call."""

    def __init__(self, policy_engine: PolicyEngine | None = None) -> None:
        self.policy_engine = policy_engine or PolicyEngine()

    def route(
        self,
        request: RoutingRequest,
        *,
        config: EffectiveConfig,
        registry: ModelRegistryManifest,
        contract: PlanningNodeContract,
        eligibility: EligibilitySnapshot,
        policy_manifest: PolicyManifest,
        tokenizer_snapshot: TokenizerSnapshot,
        fx_snapshot: FXSnapshot,
    ) -> RoutingDecision:
        # Re-parse serialized data at this trust boundary. Pydantic's
        # model_copy(update=...) intentionally skips validation and must not
        # permit a caller to smuggle stale nested snapshots into routing.
        request = revalidate_model(RoutingRequest, request)
        config = revalidate_model(EffectiveConfig, config)
        registry = revalidate_model(ModelRegistryManifest, registry)
        contract = revalidate_model(PlanningNodeContract, contract)
        eligibility = revalidate_model(EligibilitySnapshot, eligibility)
        policy_manifest = revalidate_model(PolicyManifest, policy_manifest)
        tokenizer_snapshot = revalidate_model(TokenizerSnapshot, tokenizer_snapshot)
        fx_snapshot = revalidate_model(FXSnapshot, fx_snapshot)
        self._validate_snapshot_binding(
            request, config, registry, contract, eligibility, policy_manifest
        )
        preset = config.presets[config.active_preset]
        profile = preset.roles.get(contract.role)
        if profile is None:
            raise ValueError("routing contract references a role that is not enabled")
        matching_rules = [
            rule for rule in preset.selector_rules if _selector_matches(rule, request, contract)
        ]
        selected_rule = max(matching_rules, key=lambda item: (item.priority, item.id)) if matching_rules else None
        if selected_rule is None:
            candidate_ids = contract.candidate_model_ids
            allow_degraded = False
            reasoning_effort = profile.reasoning_effort
        else:
            candidate_ids = _expand_targets(
                (selected_rule.select, *selected_rule.fallback), registry
            )
            allow_degraded = selected_rule.allow_degraded
            reasoning_effort = selected_rule.reasoning_effort or profile.reasoning_effort

        if (
            selected_rule is None
            and request.recovery_action == "capability_escalation"
            and profile.escalate_to is not None
            and profile.escalate_to.startswith("tier:")
        ):
            candidate_ids = _expand_targets(
                (SelectorTarget(tier=profile.escalate_to.removeprefix("tier:")),),
                registry,
            )

        candidate_ids = _apply_recovery_filter(
            request, candidate_ids, profile, contract.role, registry
        )
        models = {model.id: model for model in registry.models}
        providers = {provider.id: provider for provider in registry.providers}
        secret_status = {item.secret_ref: item for item in eligibility.secret_availability}
        health_status = {
            item.model_id: item for item in eligibility.model_health
        }
        assessments: list[CandidateAssessment] = []
        base_eligible_health: set[str] = set()

        for rank, model_id in enumerate(candidate_ids):
            model = models.get(model_id)
            if model is None:
                continue
            provider = providers[model.provider]
            health = health_status.get(model.id)
            if health is not None:
                expected_provider = HealthAggregateKey(
                    registry_manifest_hash=registry.content_hash,
                    scope="provider",
                    provider_id=provider.id,
                ).aggregate_id
                expected_model = HealthAggregateKey(
                    registry_manifest_hash=registry.content_hash,
                    scope="model",
                    provider_id=provider.id,
                    model_id=model.id,
                ).aggregate_id
                if (
                    health.provider_id != provider.id
                    or health.model_id != model.id
                    or health.provider.aggregate_id != expected_provider
                    or health.model.aggregate_id != expected_model
                ):
                    health = None
            probe_authorized = _health_lease_authorizes(
                request, model.id, provider.id, health
            )
            policy_decision = self.policy_engine.evaluate(
                _policy_request(request, model, provider, contract.role, eligibility), policy_manifest
            )
            cost, reasons = _assess_candidate(
                request=request,
                contract=contract,
                model=model,
                provider=provider,
                eligibility=eligibility,
                secret_status=secret_status,
                health=health,
                probe_authorized=probe_authorized,
                policy_decision=policy_decision,
                registry=registry,
                reasoning_effort=reasoning_effort,
                tokenizer_snapshot=tokenizer_snapshot,
                fx_snapshot=fx_snapshot,
            )
            if not reasons and health is not None and health.effective_state == "healthy":
                base_eligible_health.add(model.id)
            assessments.append(
                CandidateAssessment(
                    model_id=model.id,
                    provider_id=provider.id,
                    configured_rank=rank,
                    eligible=not reasons,
                    estimated_cost=cost,
                    health_state=None if health is None else health.effective_state,
                    provider_health=None if health is None else health.provider,
                    model_health=None if health is None else health.model,
                    policy_decision=policy_decision,
                    exclusion_reasons=_ordered_reasons(reasons),
                )
            )

        if base_eligible_health and not allow_degraded:
            assessments = [
                item.model_copy(
                    update={
                        "eligible": False,
                        "exclusion_reasons": _ordered_reasons(
                            (*item.exclusion_reasons, "health_degraded_preferred_healthy")
                        ),
                    }
                )
                if item.eligible and item.health_state == "degraded"
                else item
                for item in assessments
            ]

        rank_by_model = {item.model_id: item.configured_rank for item in assessments}
        selected_assessments = sorted(
            (item for item in assessments if item.eligible and item.estimated_cost is not None),
            key=lambda item: (
                item.estimated_cost.amount_minor,
                rank_by_model[item.model_id],
                item.model_id,
            ),
        )
        eligible_order = tuple(item.model_id for item in selected_assessments)
        selected = selected_assessments[0] if selected_assessments else None
        blocked_reason = None if selected is not None else (
            "health_probe_lease_required"
            if request.recovery_action == "health_probe"
            else _blocked_reason(assessments)
        )
        return RoutingDecision(
            request_id=request.request_id,
            request_hash=request.request_hash,
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            fencing_generation=request.fencing_generation,
            config_hash=request.config_hash,
            registry_hash=request.registry_hash,
            planning_contract_hash=request.planning_contract_hash,
            policy_manifest_hash=request.policy_manifest_hash,
            eligibility_snapshot_id=eligibility.snapshot_id,
            matched_selector_rule_id=None if selected_rule is None else selected_rule.id,
            candidate_assessments=tuple(assessments),
            eligible_order=eligible_order,
            reasoning_effort=reasoning_effort,
            outcome="selected" if selected is not None else "blocked",
            selected_model_id=None if selected is None else selected.model_id,
            selected_provider_id=None if selected is None else selected.provider_id,
            blocked_reason=blocked_reason,
        )

    @staticmethod
    def _validate_snapshot_binding(
        request: RoutingRequest,
        config: EffectiveConfig,
        registry: ModelRegistryManifest,
        contract: PlanningNodeContract,
        eligibility: EligibilitySnapshot,
        policy_manifest: PolicyManifest,
    ) -> None:
        expected = (
            (request.config_hash, config.content_hash, "configuration"),
            (request.registry_hash, registry.content_hash, "registry"),
            (request.policy_manifest_hash, policy_manifest.content_hash, "policy"),
            (request.planning_contract_hash, contract.contract_hash, "planning contract"),
            (contract.config_hash, config.content_hash, "planning configuration"),
            (contract.registry_hash, registry.content_hash, "planning registry"),
            (contract.policy_manifest_hash, policy_manifest.content_hash, "planning policy"),
            (eligibility.policy_manifest_hash, policy_manifest.content_hash, "eligibility policy"),
        )
        for actual, expected_value, label in expected:
            if actual != expected_value:
                raise ValueError(f"routing request has a stale {label} snapshot")
        if (
            request.run_id != contract.run_id
            or request.node_id != contract.node_id
            or request.fencing_generation < 0
        ):
            raise ValueError("routing request does not match the frozen node contract")


def _policy_request(
    request: RoutingRequest,
    model: ModelSpec,
    provider: ProviderSpec,
    role: str,
    eligibility: EligibilitySnapshot,
) -> PolicyRequest:
    normalized_hash = _hash(
        {
            "action": "model_invoke",
            "attempt_id": request.attempt_id,
            "fencing_generation": request.fencing_generation,
            "model_id": model.id,
            "provider_id": provider.id,
            "request_hash": request.request_hash,
            "run_id": request.run_id,
            "node_id": request.node_id,
        }
    )
    return PolicyRequest(
        request_id=request.request_id,
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        role=role,
        action_category="model_invoke",
        tool_id=f"model:{model.id}",
        required_permission="read-only",
        normalized_request_hash=normalized_hash,
        policy_manifest_hash=request.policy_manifest_hash,
        revocation_version=eligibility.revocation_version,
        emergency_deny_version=eligibility.emergency_deny_version,
        revoked=eligibility.authorization_revoked,
        emergency_denied=model.id in eligibility.emergency_denied_model_ids,
    )


def _assess_candidate(
    *,
    request: RoutingRequest,
    contract: PlanningNodeContract,
    model: ModelSpec,
    provider: ProviderSpec,
    eligibility: EligibilitySnapshot,
    secret_status: dict[str, SecretAvailability],
    health: ModelHealthSnapshot | None,
    probe_authorized: bool,
    policy_decision: PolicyDecision,
    registry: ModelRegistryManifest,
    reasoning_effort: str,
    tokenizer_snapshot: TokenizerSnapshot,
    fx_snapshot: FXSnapshot,
) -> tuple[CostEstimate | None, set[str]]:
    reasons: set[str] = set()
    policy = contract.policy_envelope
    if not provider.enabled:
        reasons.add("provider_disabled")
    if model.id in (policy.denied_models or ()):
        reasons.add("model_denied")
    if provider.id in (policy.denied_providers or ()):
        reasons.add("provider_denied")
    if policy.allowed_models is not None and model.id not in policy.allowed_models:
        reasons.add("model_not_allowlisted")
    if policy.allowed_providers is not None and provider.id not in policy.allowed_providers:
        reasons.add("provider_not_allowlisted")
    if policy.min_tier is not None and _TIER_ORDER[model.tier] < _TIER_ORDER[policy.min_tier]:
        reasons.add("tier_below_minimum")
    if not set(contract.required_capabilities).issubset(model.capabilities):
        reasons.add("required_capability_missing")
    if (
        policy.allowed_capabilities is not None
        and not set(contract.required_capabilities).issubset(policy.allowed_capabilities)
    ):
        reasons.add("capability_not_allowlisted")
    if contract.context_tokens > model.context_window:
        reasons.add("context_window_exceeded")
    if contract.max_output_tokens > min(model.max_output_tokens, model.context_window):
        reasons.add("output_limit_exceeded")
    if reasoning_effort not in model.supported_reasoning_efforts:
        reasons.add("reasoning_effort_unsupported")
    if policy_decision.outcome == "deny":
        reasons.add("policy_denied")
    elif policy_decision.outcome == "needs_approval":
        reasons.add("approval_required")

    secret = secret_status.get(provider.secret_ref)
    if secret is None or not secret.available:
        reasons.add("credential_unavailable")
    if any(
        block.subject_id == model.id and block.subject_kind == "model"
        or block.subject_id == provider.id and block.subject_kind == "provider"
        for block in eligibility.blocks
        if block.reason == "lifecycle_circuit_open"
    ):
        reasons.add("lifecycle_circuit_open")
    for block in eligibility.blocks:
        if block.subject_id == model.id and block.subject_kind == "model" or (
            block.subject_id == provider.id and block.subject_kind == "provider"
        ):
            reasons.add(block.reason)

    if health is None:
        reasons.add("health_snapshot_missing")
    elif health.effective_state in {"open", "half_open"} and not (
        request.recovery_action == "health_probe"
        and probe_authorized
        and health.effective_state == "half_open"
    ):
        reasons.add("provider_unhealthy" if health.provider.state in {"open", "half_open"} else "model_unhealthy")

    if eligibility.budget_currency != policy.currency:
        reasons.add("budget_currency_mismatch")

    cost: CostEstimate | None = None
    try:
        cost = estimate_model_cost(
            registry,
            tokenizer_snapshot,
            fx_snapshot,
            model_id=model.id,
            input_tokens=contract.context_tokens,
            output_tokens=contract.max_output_tokens,
            reasoning_tokens=(
                contract.max_output_tokens if reasoning_effort != "none" else 0
            ),
            target_currency=policy.currency or eligibility.budget_currency,
            as_of=request.routing_as_of_event_time,
        )
    except CostingDataUnavailable:
        reasons.add("costing_unavailable")
    except (ValueError, StopIteration):
        reasons.add("costing_unavailable")

    if cost is not None:
        if cost.amount_minor > eligibility.available_cost_minor or (
            policy.max_cost_minor is not None and cost.amount_minor > policy.max_cost_minor
        ):
            reasons.add("cost_limit_exceeded")
        total_tokens = cost.total_tokens
        if (
            policy.max_total_tokens is not None and total_tokens > policy.max_total_tokens
        ) or (
            eligibility.available_tokens is not None and total_tokens > eligibility.available_tokens
        ):
            reasons.add("token_limit_exceeded")
    return cost, reasons


def _selector_matches(rule: SelectorRule, request: RoutingRequest, contract: PlanningNodeContract) -> bool:
    when = rule.when
    classification = contract.classification
    return (
        (when.role is None or contract.role in when.role)
        and (when.task_class is None or classification.task_class in when.task_class)
        and (when.complexity is None or classification.complexity in when.complexity)
        and (when.risk is None or classification.risk in when.risk)
        and (when.failure_category is None or request.failure_category in when.failure_category)
        and (
            when.authorized_recovery_action is None
            or request.recovery_action in when.authorized_recovery_action
        )
        and (when.health_state is None or request.health_state in when.health_state)
        and (
            when.required_capabilities_all is None
            or set(when.required_capabilities_all).issubset(contract.required_capabilities)
        )
        and (
            when.context_tokens is None
            or when.context_tokens.minimum <= contract.context_tokens <= when.context_tokens.maximum
        )
        and (
            when.output_tokens is None
            or when.output_tokens.minimum <= contract.max_output_tokens <= when.output_tokens.maximum
        )
        and (
            when.retry_level is None
            or when.retry_level.minimum <= request.retry_level <= when.retry_level.maximum
        )
    )


def _expand_targets(
    targets: tuple[SelectorTarget, ...], registry: ModelRegistryManifest
) -> tuple[str, ...]:
    output: list[str] = []
    for target in targets:
        if target.model is not None:
            matches = [target.model] if any(model.id == target.model for model in registry.models) else []
        else:
            matches = sorted(model.id for model in registry.models if model.tier == target.tier)
        for model_id in matches:
            if model_id not in output:
                output.append(model_id)
    return tuple(output)


def _apply_recovery_filter(
    request: RoutingRequest,
    candidate_ids: tuple[str, ...],
    profile: RoleProfile,
    role: str,
    registry: ModelRegistryManifest,
) -> tuple[str, ...]:
    if request.recovery_action == "initial":
        return candidate_ids
    authorization = request.recovery_authorization
    assert authorization is not None
    if request.recovery_action == "health_probe":
        lease = request.probe_lease
        if (
            lease is None
            or lease.registry_manifest_hash != request.registry_hash
            or lease.model_id not in authorization.authorized_model_ids
            or request.routing_as_of_event_time >= lease.expires_at
        ):
            return ()
        return tuple(model_id for model_id in candidate_ids if model_id == lease.model_id)
    allowed = set(authorization.authorized_model_ids)
    candidates = [model_id for model_id in candidate_ids if model_id in allowed]
    models = {model.id: model for model in registry.models}
    previous = models.get(authorization.previous_model_id)
    if previous is None:
        return ()
    if request.recovery_action == "same_model_retry":
        if (
            request.retry_level > profile.retries
            or allowed != {previous.id}
            or previous.id in request.exhausted_model_ids
        ):
            return ()
        candidates = [model_id for model_id in candidates if model_id == previous.id]
    elif request.recovery_action == "same_tier_fallback":
        candidates = [
            model_id
            for model_id in candidates
            if model_id != previous.id
            and models[model_id].tier == previous.tier
            and previous.capabilities.issubset(models[model_id].capabilities)
            and model_id not in request.exhausted_model_ids
        ]
    elif request.recovery_action == "capability_escalation":
        target = profile.escalate_to
        if target is None or target in {"reviewer", "director"}:
            return ()
        target_tier = target.removeprefix("tier:")
        candidates = [
            model_id
            for model_id in candidates
            if model_id not in request.exhausted_model_ids
            and _TIER_ORDER[models[model_id].tier] > _TIER_ORDER[previous.tier]
            and _TIER_ORDER[models[model_id].tier] >= _TIER_ORDER[target_tier]
            and previous.capabilities.issubset(models[model_id].capabilities)
        ]
    elif request.recovery_action == "reviewer_node":
        candidates = candidates if role == "reviewer" else []
    elif request.recovery_action == "director_node":
        candidates = candidates if role == "director" else []
    return tuple(candidates)


def _health_lease_authorizes(
    request: RoutingRequest,
    model_id: str,
    provider_id: str,
    health: ModelHealthSnapshot | None,
) -> bool:
    lease = request.probe_lease
    authorization = request.recovery_authorization
    if (
        request.recovery_action != "health_probe"
        or lease is None
        or authorization is None
        or health is None
        or lease.model_id != model_id
        or lease.provider_id != provider_id
        or lease.registry_manifest_hash != request.registry_hash
        or request.routing_as_of_event_time >= lease.expires_at
        or authorization.authorized_model_ids != (model_id,)
    ):
        return False
    expected = {
        ref.aggregate_id: (ref.version, ref.generation)
        for ref, state in (
            (health.provider, health.provider.state),
            (health.model, health.model.state),
        )
        if state == "half_open"
    }
    actual = {
        ref.aggregate_id: (ref.version, ref.generation)
        for ref in lease.aggregates
    }
    return bool(expected) and actual == expected


def _ordered_reasons(reasons: tuple[str, ...] | set[str]) -> tuple[str, ...]:
    order = {reason: index for index, reason in enumerate(_REASON_ORDER)}
    return tuple(sorted(set(reasons), key=lambda item: (order.get(item, len(order)), item)))


def _blocked_reason(assessments: list[CandidateAssessment]) -> str:
    all_reasons = [set(item.exclusion_reasons) for item in assessments]
    if not all_reasons:
        return "limit_exhausted"
    if all("credential_unavailable" in reasons for reasons in all_reasons):
        return "credential_unavailable"
    if all("approval_required" in reasons for reasons in all_reasons):
        return "approval_required"
    if all("policy_denied" in reasons for reasons in all_reasons):
        return "policy_denied"
    if all(
        reasons.intersection(
            {"costing_unavailable", "cost_limit_exceeded", "token_limit_exceeded", "budget_currency_mismatch"}
        )
        for reasons in all_reasons
    ):
        return "budget_unavailable"
    if all(reasons.intersection({"provider_unhealthy", "model_unhealthy", "health_snapshot_missing"}) for reasons in all_reasons):
        return "all_candidates_unhealthy"
    if all("recovery_action_not_supported" in reasons for reasons in all_reasons):
        return "limit_exhausted"
    return "capability_unavailable"


__all__ = [
    "CandidateAssessment",
    "EligibilityBlock",
    "EligibilitySnapshot",
    "HealthAggregateRef",
    "ModelHealthSnapshot",
    "ModelRouter",
    "RecoveryAuthorization",
    "RoutingDecision",
    "RoutingRequest",
    "SecretAvailability",
]
