"""Complete configuration schema, four-layer resolution, and provenance."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Generic, Literal, TypeVar, get_args

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_serializer, field_validator, model_validator
from pydantic_core import core_schema

from orchestrator.config.models import (
    ModelRegistryManifest,
    PolicyEnvelope,
    ReasoningEffort,
    Tier,
    tighten_policy_envelopes,
)


PresetName = Literal["economic", "balanced", "quality", "custom"]
RoleName = Literal[
    "planner", "coder", "document_analyst", "researcher", "tester", "reviewer", "director"
]
TaskClass = Literal[
    "mechanical", "analysis", "code_change", "document_analysis", "research", "review", "planning", "unknown"
]
Complexity = Literal["low", "medium", "high", "unknown"]
Risk = Literal["low", "medium", "high", "critical"]
FailureCategory = Literal["transient", "output_invalid", "task_failure", "capability_failure"]
RecoveryAction = Literal[
    "initial", "same_model_retry", "same_tier_fallback", "capability_escalation", "reviewer_node", "director_node", "health_probe"
]
HealthState = Literal["healthy", "degraded", "open", "half_open"]
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_ROLES = frozenset(
    {"planner", "coder", "document_analyst", "researcher", "tester", "reviewer", "director"}
)
_TIER_ORDER = {"economy": 0, "standard": 1, "high": 2}

K = TypeVar("K")
V = TypeVar("V")


class FrozenDict(dict, Generic[K, V]):
    """A JSON-object-shaped mapping that cannot be mutated after validation."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        dict.__init__(self, *args, **kwargs)

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> core_schema.CoreSchema:
        args = get_args(source)
        key_type, value_type = args if len(args) == 2 else (Any, Any)
        return core_schema.no_info_after_validator_function(
            cls, handler.generate_schema(dict[key_type, value_type])
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, schema: Any, handler: Any) -> dict[str, Any]:
        return handler(schema)

    def _immutable(self, *_: Any, **__: Any) -> None:
        raise TypeError("configuration mappings are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class _StrictModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


def _as_tuple(value: object, field_name: str) -> tuple[object, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field_name} must be an array")
    return tuple(value)


def _identifier(value: str, field_name: str) -> str:
    if value != value.strip() or not _ID.fullmatch(value):
        raise ValueError(f"{field_name} must be a stable non-blank identifier")
    return value


class RequestedBudget(_StrictModel):
    """A preset's desired limits; hard limits live in PolicyEnvelope."""

    max_cost_minor: StrictInt | None = Field(ge=0)
    max_total_tokens: StrictInt | None = Field(ge=0)
    max_agents: StrictInt | None = Field(ge=0)
    max_depth: StrictInt | None = Field(ge=0)
    max_concurrency: StrictInt | None = Field(ge=0)
    max_parallel_candidates: StrictInt | None = Field(ge=0)


class IntRange(_StrictModel):
    minimum: StrictInt = Field(ge=0)
    maximum: StrictInt = Field(ge=0)

    @model_validator(mode="after")
    def valid_range(self) -> "IntRange":
        if self.maximum < self.minimum:
            raise ValueError("maximum must be greater than or equal to minimum")
        return self


class SelectorWhen(_StrictModel):
    role: tuple[RoleName, ...] | None = None
    task_class: tuple[TaskClass, ...] | None = None
    complexity: tuple[Complexity, ...] | None = None
    risk: tuple[Risk, ...] | None = None
    failure_category: tuple[FailureCategory, ...] | None = None
    authorized_recovery_action: tuple[RecoveryAction, ...] | None = None
    health_state: tuple[HealthState, ...] | None = None
    required_capabilities_all: tuple[StrictStr, ...] | None = None
    context_tokens: IntRange | None = None
    output_tokens: IntRange | None = None
    retry_level: IntRange | None = None

    @field_validator(
        "role", "task_class", "complexity", "risk", "failure_category",
        "authorized_recovery_action", "health_state", "required_capabilities_all", mode="before"
    )
    @classmethod
    def normalize_arrays(cls, value: object, info: Any) -> tuple[object, ...] | None:
        return None if value is None else _as_tuple(value, info.field_name)

    @model_validator(mode="after")
    def nonempty_conditions(self) -> "SelectorWhen":
        for name in (
            "role", "task_class", "complexity", "risk", "failure_category",
            "authorized_recovery_action", "health_state", "required_capabilities_all"
        ):
            value = getattr(self, name)
            if value is not None and not value:
                raise ValueError(f"{name} condition cannot be empty")
        return self


class GuardWhen(_StrictModel):
    role: tuple[RoleName, ...] | None = None
    task_class: tuple[TaskClass, ...] | None = None
    complexity: tuple[Complexity, ...] | None = None
    risk: tuple[Risk, ...] | None = None
    required_capabilities_all: tuple[StrictStr, ...] | None = None
    context_tokens: IntRange | None = None
    output_tokens: IntRange | None = None

    @field_validator(
        "role", "task_class", "complexity", "risk", "required_capabilities_all", mode="before"
    )
    @classmethod
    def normalize_arrays(cls, value: object, info: Any) -> tuple[object, ...] | None:
        return None if value is None else _as_tuple(value, info.field_name)

    @model_validator(mode="after")
    def nonempty_conditions(self) -> "GuardWhen":
        for name in ("role", "task_class", "complexity", "risk", "required_capabilities_all"):
            value = getattr(self, name)
            if value is not None and not value:
                raise ValueError(f"{name} condition cannot be empty")
        return self


class SelectorTarget(_StrictModel):
    model: StrictStr | None = None
    tier: Tier | None = None

    @field_validator("model")
    @classmethod
    def validate_model_ref(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "model")

    @model_validator(mode="after")
    def exactly_one_target(self) -> "SelectorTarget":
        if (self.model is None) == (self.tier is None):
            raise ValueError("exactly one of model or tier is required")
        return self


class SelectorRule(_StrictModel):
    id: StrictStr = Field(min_length=1)
    priority: StrictInt = Field(ge=0)
    when: SelectorWhen
    select: SelectorTarget
    reasoning_effort: ReasoningEffort | None
    fallback: tuple[SelectorTarget, ...]
    allow_degraded: StrictBool

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return _identifier(value, "selector rule id")

    @field_validator("fallback", mode="before")
    @classmethod
    def normalize_fallback(cls, value: object) -> tuple[object, ...]:
        return _as_tuple(value, "fallback")


class SelectorTombstone(_StrictModel):
    id: StrictStr = Field(min_length=1)
    disabled: Literal[True]

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return _identifier(value, "selector rule id")


SelectorOverride = SelectorRule | SelectorTombstone


class GuardConstraints(_StrictModel):
    max_cost_minor: StrictInt | None = Field(default=None, ge=0)
    max_total_tokens: StrictInt | None = Field(default=None, ge=0)
    max_agents: StrictInt | None = Field(default=None, ge=0)
    max_depth: StrictInt | None = Field(default=None, ge=0)
    max_concurrency: StrictInt | None = Field(default=None, ge=0)
    allowed_models: frozenset[StrictStr] | None = None
    allowed_providers: frozenset[StrictStr] | None = None
    allowed_capabilities: frozenset[StrictStr] | None = None
    denied_models: frozenset[StrictStr] | None = None
    denied_providers: frozenset[StrictStr] | None = None
    min_tier: Tier | None = None
    require_independent_review: StrictBool | None = None
    require_approval: StrictBool | None = None
    max_parallel_candidates: StrictInt | None = Field(default=None, ge=0)

    @field_serializer(
        "allowed_models", "allowed_providers", "allowed_capabilities", "denied_models", "denied_providers"
    )
    def serialize_sets(self, value: frozenset[str] | None) -> list[str] | None:
        return None if value is None else sorted(value)

    @field_validator(
        "allowed_models", "allowed_providers", "allowed_capabilities", "denied_models", "denied_providers", mode="before"
    )
    @classmethod
    def normalize_sets(cls, value: object) -> frozenset[object] | None:
        if value is None:
            return None
        if not isinstance(value, (list, tuple, set, frozenset)):
            raise ValueError("constraint values must be arrays")
        return frozenset(value)

    @model_validator(mode="after")
    def has_constraints(self) -> "GuardConstraints":
        if all(getattr(self, name) is None for name in type(self).model_fields):
            raise ValueError("guard constraints must tighten at least one policy field")
        return self


class GuardRule(_StrictModel):
    id: StrictStr = Field(min_length=1)
    when: GuardWhen
    constraints: GuardConstraints

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return _identifier(value, "guard rule id")


class RoleProfile(_StrictModel):
    candidates: tuple[StrictStr, ...]
    reasoning_effort: ReasoningEffort
    max_output_tokens: StrictInt = Field(gt=0)
    retries: StrictInt = Field(ge=0)
    escalate_to: StrictStr | None

    @field_validator("candidates", mode="before")
    @classmethod
    def normalize_candidates(cls, value: object) -> tuple[object, ...]:
        return _as_tuple(value, "candidates")

    @field_validator("candidates")
    @classmethod
    def validate_candidates(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if not values:
            raise ValueError("candidates must not be empty")
        for candidate in values:
            if candidate.startswith("tier:"):
                if candidate[5:] not in _TIER_ORDER:
                    raise ValueError("candidate tier reference is invalid")
            else:
                _identifier(candidate, "candidate model reference")
        if len(values) != len(set(values)):
            raise ValueError("candidates must be unique")
        return values

    @field_validator("escalate_to")
    @classmethod
    def validate_escalation(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value.startswith("tier:"):
            if value[5:] not in _TIER_ORDER:
                raise ValueError("escalation tier reference is invalid")
            return value
        if value not in {"reviewer", "director"}:
            raise ValueError("escalation must target a higher tier, reviewer, or director")
        return value


class HealthPolicy(_StrictModel):
    id: StrictStr = Field(min_length=1)
    failure_window_ms: StrictInt = Field(gt=0)
    degrade_after: StrictInt = Field(gt=0)
    open_after: StrictInt = Field(gt=0)
    recovery_successes: StrictInt = Field(gt=0)
    cooldown_ms: StrictInt = Field(gt=0)
    max_probe_permits: Literal[1]

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return _identifier(value, "health policy id")

    @model_validator(mode="after")
    def ordered_thresholds(self) -> "HealthPolicy":
        if self.open_after <= self.degrade_after:
            raise ValueError("open_after must be greater than degrade_after")
        return self


class HealthPolicyOverlay(_StrictModel):
    """A recursive partial update to a named system health policy."""

    failure_window_ms: StrictInt | None = Field(default=None, gt=0)
    degrade_after: StrictInt | None = Field(default=None, gt=0)
    open_after: StrictInt | None = Field(default=None, gt=0)
    recovery_successes: StrictInt | None = Field(default=None, gt=0)
    cooldown_ms: StrictInt | None = Field(default=None, gt=0)
    max_probe_permits: Literal[1] | None = None


class ClassifierSpec(_StrictModel):
    id: Literal["orchestrator.deterministic.v1"]
    version: StrictStr = Field(min_length=1)
    normalization_version: StrictStr = Field(min_length=1)
    taxonomy_version: StrictStr = Field(min_length=1)


class ClassifierOverlay(_StrictModel):
    id: Literal["orchestrator.deterministic.v1"] | None = None
    version: StrictStr | None = Field(default=None, min_length=1)
    normalization_version: StrictStr | None = Field(default=None, min_length=1)
    taxonomy_version: StrictStr | None = Field(default=None, min_length=1)


class Preset(_StrictModel):
    requested_budget: RequestedBudget
    roles: FrozenDict[RoleName, RoleProfile]
    selector_rules: tuple[SelectorRule, ...]
    guard_rules: tuple[GuardRule, ...]
    health_policy_ref: StrictStr = Field(min_length=1)

    @field_validator("roles", mode="before")
    @classmethod
    def require_mapping(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            raise ValueError("roles must be a mapping")
        return value

    @field_validator("selector_rules", "guard_rules", mode="before")
    @classmethod
    def normalize_rules(cls, value: object, info: Any) -> tuple[object, ...]:
        return _as_tuple(value, info.field_name)

    @field_validator("health_policy_ref")
    @classmethod
    def validate_health_ref(cls, value: str) -> str:
        return _identifier(value, "health policy reference")

    @model_validator(mode="after")
    def validate_rules_and_roles(self) -> "Preset":
        if not self.roles:
            raise ValueError("a complete preset must enable at least one role")
        selector_ids = [rule.id for rule in self.selector_rules]
        guard_ids = [rule.id for rule in self.guard_rules]
        if len(selector_ids) != len(set(selector_ids)):
            raise ValueError("selector rule ids must be unique within a preset")
        if len(guard_ids) != len(set(guard_ids)):
            raise ValueError("guard rule ids must be unique within a preset")
        for index, left in enumerate(self.selector_rules):
            for right in self.selector_rules[index + 1 :]:
                if left.priority == right.priority and _selector_rules_overlap(left.when, right.when):
                    raise ValueError("same-priority selector rules must not possibly overlap")
        return self


class EffectiveConfig(_StrictModel):
    """A complete, validated preference manifest with hard policy envelope."""

    schema_version: Literal[1]
    active_preset: PresetName
    registry_manifest_ref: StrictStr
    policy_envelope: PolicyEnvelope
    mandatory_guard_rules: tuple[GuardRule, ...]
    presets: FrozenDict[PresetName, Preset]
    classifier: ClassifierSpec
    health_policies: FrozenDict[StrictStr, HealthPolicy]

    @field_validator("registry_manifest_ref")
    @classmethod
    def validate_registry_hash(cls, value: str) -> str:
        if not _HASH.fullmatch(value):
            raise ValueError("registry_manifest_ref must be a sha256 content reference")
        return value

    @field_validator("mandatory_guard_rules", mode="before")
    @classmethod
    def normalize_mandatory_guards(cls, value: object) -> tuple[object, ...]:
        return _as_tuple(value, "mandatory_guard_rules")

    @field_validator("presets", "health_policies", mode="before")
    @classmethod
    def require_mappings(cls, value: object, info: Any) -> object:
        if not isinstance(value, Mapping):
            raise ValueError(f"{info.field_name} must be a mapping")
        return value

    @model_validator(mode="after")
    def validate_complete_graph(self) -> "EffectiveConfig":
        required_limits = (
            "max_cost_minor", "max_total_tokens", "max_agents", "max_depth",
            "max_concurrency", "max_parallel_candidates"
        )
        missing_limits = [
            name for name in required_limits if getattr(self.policy_envelope, name) is None
        ]
        if missing_limits or self.policy_envelope.currency is None:
            raise ValueError(
                "complete config requires an explicit currency and finite hard limits: "
                + ", ".join(missing_limits)
            )
        required = {"economic", "balanced", "quality", self.active_preset}
        missing = sorted(required - set(self.presets))
        if missing:
            raise ValueError(f"required preset definitions are missing: {missing}")
        if not self.health_policies:
            raise ValueError("at least one health policy is required")
        for key, policy in self.health_policies.items():
            if key != policy.id:
                raise ValueError("health policy key must match its id")
        for preset in self.presets.values():
            if preset.health_policy_ref not in self.health_policies:
                raise ValueError("preset references an unknown health policy")
            role_names = set(preset.roles)
            if role_names - _ROLES:
                raise ValueError("preset contains an unknown role")
            for role, profile in preset.roles.items():
                if profile.escalate_to in {"reviewer", "director"} and profile.escalate_to not in role_names:
                    raise ValueError("role escalation references a disabled role")
            _validate_role_escalation_graph(preset.roles)
        mandatory_ids = [rule.id for rule in self.mandatory_guard_rules]
        if len(mandatory_ids) != len(set(mandatory_ids)):
            raise ValueError("mandatory guard rule ids must be unique")
        return self

    def canonical_json(self) -> str:
        return json.dumps(self.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @property
    def content_hash(self) -> str:
        digest = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        return f"sha256:{digest}"


class RequestedBudgetOverlay(_StrictModel):
    max_cost_minor: StrictInt | None = Field(default=None, ge=0)
    max_total_tokens: StrictInt | None = Field(default=None, ge=0)
    max_agents: StrictInt | None = Field(default=None, ge=0)
    max_depth: StrictInt | None = Field(default=None, ge=0)
    max_concurrency: StrictInt | None = Field(default=None, ge=0)
    max_parallel_candidates: StrictInt | None = Field(default=None, ge=0)


class RoleProfileOverlay(_StrictModel):
    candidates: tuple[StrictStr, ...] | None = None
    reasoning_effort: ReasoningEffort | None = None
    max_output_tokens: StrictInt | None = Field(default=None, gt=0)
    retries: StrictInt | None = Field(default=None, ge=0)
    escalate_to: StrictStr | None = None

    @field_validator("candidates", mode="before")
    @classmethod
    def normalize_candidates(cls, value: object) -> tuple[object, ...] | None:
        return None if value is None else _as_tuple(value, "candidates")


class PresetOverlay(_StrictModel):
    requested_budget: RequestedBudgetOverlay | None = None
    roles: FrozenDict[RoleName, RoleProfileOverlay | None] | None = None
    selector_rules: tuple[SelectorOverride, ...] | None = None
    guard_rules: tuple[GuardRule, ...] | None = None
    health_policy_ref: StrictStr | None = None

    @field_validator("roles", mode="before")
    @classmethod
    def require_role_mapping(cls, value: object) -> object:
        if value is not None and not isinstance(value, Mapping):
            raise ValueError("roles must be a mapping")
        return value

    @field_validator("selector_rules", "guard_rules", mode="before")
    @classmethod
    def normalize_rule_arrays(cls, value: object, info: Any) -> tuple[object, ...] | None:
        return None if value is None else _as_tuple(value, info.field_name)

    @field_validator("health_policy_ref")
    @classmethod
    def validate_health_ref(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "health policy reference")

    @model_validator(mode="after")
    def unique_layer_rules(self) -> "PresetOverlay":
        if self.selector_rules is not None:
            ids = [rule.id for rule in self.selector_rules]
            if len(ids) != len(set(ids)):
                raise ValueError("selector rule ids must be unique within one overlay layer")
        if self.guard_rules is not None:
            ids = [rule.id for rule in self.guard_rules]
            if len(ids) != len(set(ids)):
                raise ValueError("guard rule ids must be unique within one overlay layer")
        return self


class ConfigOverlay(_StrictModel):
    """A partial overlay; immutable registry and mandatory guards are omitted."""

    schema_version: Literal[1] | None = None
    active_preset: PresetName | None = None
    policy_envelope: PolicyEnvelope | None = None
    presets: FrozenDict[PresetName, PresetOverlay | None] | None = None
    classifier: ClassifierOverlay | None = None
    health_policies: FrozenDict[StrictStr, HealthPolicyOverlay | None] | None = None

    @field_validator("presets", "health_policies", mode="before")
    @classmethod
    def require_overlay_mappings(cls, value: object, info: Any) -> object:
        if value is not None and not isinstance(value, Mapping):
            raise ValueError(f"{info.field_name} must be a mapping")
        return value


class ConfigSource(StrEnum):
    SYSTEM_DEFAULT = "system_default"
    USER_GLOBAL = "user_global"
    PROJECT = "project"
    RUN = "run"


class ResolvedConfig(_StrictModel):
    config: EffectiveConfig
    field_sources: FrozenDict[StrictStr, tuple[ConfigSource, ...]]
    policy_envelope_sources: FrozenDict[StrictStr, tuple[ConfigSource, ...]]
    layers: tuple[ConfigSource, ...]

    @property
    def content_hash(self) -> str:
        """The manifest hash excludes provenance and layer-presence metadata."""
        return self.config.content_hash


def _validate_role_escalation_graph(roles: Mapping[str, RoleProfile]) -> None:
    edges = {
        name: profile.escalate_to
        for name, profile in roles.items()
        if profile.escalate_to in roles
    }
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(role: str) -> None:
        if role in visiting:
            raise ValueError("role escalation graph must not contain cycles")
        if role in visited:
            return
        visiting.add(role)
        target = edges.get(role)
        if target is not None:
            visit(target)
        visiting.remove(role)
        visited.add(role)

    for role in roles:
        visit(role)


def _selector_rules_overlap(left: SelectorWhen, right: SelectorWhen) -> bool:
    for name in (
        "role", "task_class", "complexity", "risk", "failure_category",
        "authorized_recovery_action", "health_state"
    ):
        left_values = getattr(left, name)
        right_values = getattr(right, name)
        if left_values is not None and right_values is not None and not set(left_values).intersection(right_values):
            return False
    for name in ("context_tokens", "output_tokens", "retry_level"):
        left_range = getattr(left, name)
        right_range = getattr(right, name)
        if left_range is not None and right_range is not None:
            if max(left_range.minimum, right_range.minimum) > min(left_range.maximum, right_range.maximum):
                return False
    return True


def _pointer(path: tuple[str, ...]) -> str:
    return "/" + "/".join(part.replace("~", "~0").replace("/", "~1") for part in path)


def _record_leaves(
    sources: dict[str, tuple[ConfigSource, ...]], value: Any, source: ConfigSource,
    path: tuple[str, ...], *, record_none: bool = False
) -> None:
    if value is None and not record_none:
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            _record_leaves(sources, child, source, (*path, str(key)), record_none=record_none)
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            entry = str(child.get("id")) if (
                path and path[-1] in {"selector_rules", "guard_rules"}
                and isinstance(child, Mapping) and child.get("id") is not None
            ) else str(index)
            _record_leaves(sources, child, source, (*path, entry), record_none=record_none)
    else:
        key = _pointer(path)
        sources[key] = (source,)


def _drop_source_subtree(sources: dict[str, tuple[ConfigSource, ...]], path: tuple[str, ...]) -> None:
    root = _pointer(path)
    for key in tuple(sources):
        if key == root or key.startswith(root + "/"):
            del sources[key]


def _replace_source_subtree(
    sources: dict[str, tuple[ConfigSource, ...]], value: Any, source: ConfigSource, path: tuple[str, ...]
) -> None:
    _drop_source_subtree(sources, path)
    _record_leaves(sources, value, source, path)


def _merge_selector_rules(
    base: list[dict[str, Any]], override: list[dict[str, Any]], source: ConfigSource,
    sources: dict[str, tuple[ConfigSource, ...]], path: tuple[str, ...]
) -> list[dict[str, Any]]:
    result = [dict(rule) for rule in base]
    indexes = {rule["id"]: index for index, rule in enumerate(result)}
    for rule in override:
        rule_id = rule["id"]
        rule_path = (*path, rule_id)
        index = indexes.get(rule_id)
        if rule.get("disabled") is True:
            if index is not None:
                result.pop(index)
                indexes = {item["id"]: i for i, item in enumerate(result)}
            _drop_source_subtree(sources, rule_path)
            sources[_pointer(rule_path)] = (source,)
            continue
        clean_rule = dict(rule)
        clean_rule.pop("disabled", None)
        if index is None:
            indexes[rule_id] = len(result)
            result.append(clean_rule)
        else:
            result[index] = clean_rule
        _replace_source_subtree(sources, clean_rule, source, rule_path)
    return result


def _merge_guard_rules(
    base: list[dict[str, Any]], override: list[dict[str, Any]], source: ConfigSource,
    sources: dict[str, tuple[ConfigSource, ...]], path: tuple[str, ...]
) -> list[dict[str, Any]]:
    result = [dict(rule) for rule in base]
    indexes = {rule["id"]: index for index, rule in enumerate(result)}
    for rule in override:
        rule_id = rule["id"]
        index = indexes.get(rule_id)
        if index is None:
            indexes[rule_id] = len(result)
            result.append(dict(rule))
            _record_leaves(sources, rule, source, (*path, rule_id))
        elif result[index] == rule:
            # Identical repeats are harmless and provenance remains multi-source.
            rule_path = (*path, rule_id)
            for key in tuple(sources):
                if key == _pointer(rule_path) or key.startswith(_pointer(rule_path) + "/"):
                    sources[key] = tuple(dict.fromkeys((*sources[key], source)))
        else:
            raise ValueError(f"guard rule id {rule_id!r} cannot be replaced or weakened")
    return result


def _merge_mapping(
    base: Any, override: Any, source: ConfigSource, sources: dict[str, tuple[ConfigSource, ...]],
    path: tuple[str, ...] = ()
) -> Any:
    if override is None:
        return base
    if path and path[-1] == "selector_rules":
        return _merge_selector_rules(base or [], override, source, sources, path)
    if path and path[-1] == "guard_rules":
        return _merge_guard_rules(base or [], override, source, sources, path)
    if isinstance(base, Mapping) and isinstance(override, Mapping):
        result = dict(base)
        for key, value in override.items():
            if value is None:
                continue
            key = str(key)
            child_path = (*path, key)
            if key not in result:
                result[key] = value
                _record_leaves(sources, value, source, child_path)
            else:
                result[key] = _merge_mapping(result[key], value, source, sources, child_path)
        return result
    _replace_source_subtree(sources, override, source, path)
    return override


def resolve_effective_config(
    system_default: EffectiveConfig,
    *,
    registry: ModelRegistryManifest,
    user_global: ConfigOverlay | None = None,
    project: ConfigOverlay | None = None,
    run: ConfigOverlay | None = None,
) -> ResolvedConfig:
    """Resolve system -> user -> project -> run, retaining leaf provenance."""
    layer_values = (
        (ConfigSource.USER_GLOBAL, user_global),
        (ConfigSource.PROJECT, project),
        (ConfigSource.RUN, run),
    )
    if system_default.registry_manifest_ref != registry.content_hash:
        raise ValueError("system configuration registry hash does not match supplied manifest")

    resolved = system_default.model_dump(mode="json", exclude_unset=True)
    field_sources: dict[str, tuple[ConfigSource, ...]] = {}
    _record_leaves(field_sources, resolved, ConfigSource.SYSTEM_DEFAULT, (), record_none=True)
    policy_sources: dict[str, tuple[ConfigSource, ...]] = {}
    for name in PolicyEnvelope.model_fields:
        value = getattr(system_default.policy_envelope, name)
        if value is not None:
            policy_sources[name] = (ConfigSource.SYSTEM_DEFAULT,)

    for source, overlay in layer_values:
        if overlay is None:
            continue
        raw = overlay.model_dump(mode="json", exclude_unset=True)
        # Overlay schema_version describes the overlay document; the effective
        # manifest always retains the system default's schema version.
        raw.pop("schema_version", None)
        policy_raw = raw.pop("policy_envelope", None)
        if policy_raw is not None:
            current = PolicyEnvelope.model_validate(resolved["policy_envelope"])
            incoming = PolicyEnvelope.model_validate(policy_raw)
            resolved["policy_envelope"] = tighten_policy_envelopes(current, incoming).model_dump(mode="json", exclude_none=True)
            for name in PolicyEnvelope.model_fields:
                if getattr(incoming, name) is not None:
                    policy_sources[name] = tuple(dict.fromkeys((*policy_sources.get(name, ()), source)))
                    pointer = _pointer(("policy_envelope", name))
                    field_sources[pointer] = policy_sources[name]
        resolved = _merge_mapping(resolved, raw, source, field_sources)

    config = EffectiveConfig.model_validate(resolved)
    _validate_registry_references(config, registry)
    return ResolvedConfig(
        config=config,
        field_sources=FrozenDict(field_sources),
        policy_envelope_sources=FrozenDict(policy_sources),
        layers=(ConfigSource.SYSTEM_DEFAULT, *(source for source, overlay in layer_values if overlay is not None)),
    )


def _target_models(target: SelectorTarget, registry: ModelRegistryManifest) -> set[str]:
    if target.model is not None:
        return {target.model} if target.model in {model.id for model in registry.models} else set()
    return {model.id for model in registry.models if model.tier == target.tier}


def _validate_registry_references(config: EffectiveConfig, registry: ModelRegistryManifest) -> None:
    if config.registry_manifest_ref != registry.content_hash:
        raise ValueError("configuration registry hash does not match supplied manifest")
    providers = {provider.id: provider for provider in registry.providers}
    models = {model.id: model for model in registry.models}
    available = {
        model.id for model in registry.models
        if model.provider in providers and providers[model.provider].enabled
    }
    capabilities = {capability for model in registry.models for capability in model.capabilities}
    for constraints in (config.policy_envelope,):
        if constraints.allowed_models is not None and set(constraints.allowed_models) - set(models):
            raise ValueError("policy envelope references an unknown model")
        if constraints.denied_models is not None and set(constraints.denied_models) - set(models):
            raise ValueError("policy envelope references an unknown model")
        if constraints.allowed_providers is not None and set(constraints.allowed_providers) - set(providers):
            raise ValueError("policy envelope references an unknown provider")
        if constraints.denied_providers is not None and set(constraints.denied_providers) - set(providers):
            raise ValueError("policy envelope references an unknown provider")
        if constraints.allowed_capabilities is not None and set(constraints.allowed_capabilities) - capabilities:
            raise ValueError("policy envelope references an unknown capability")
    for preset in config.presets.values():
        role_names = set(preset.roles)
        for profile in preset.roles.values():
            candidate_models: set[str] = set()
            for candidate in profile.candidates:
                candidate_models.update(
                    model.id for model in registry.models
                    if (candidate[5:] == model.tier if candidate.startswith("tier:") else candidate == model.id)
                )
            if not candidate_models:
                raise ValueError("role candidate reference does not resolve to a registered model")
            if candidate_models - available:
                raise ValueError("role candidate references a disabled provider")
            if any(profile.reasoning_effort not in models[model_id].supported_reasoning_efforts for model_id in candidate_models):
                raise ValueError("role reasoning effort is unsupported by a registered candidate")
            if any(profile.max_output_tokens > models[model_id].max_output_tokens for model_id in candidate_models):
                # A profile may deliberately have candidates filtered at runtime; don't let an impossible
                # output cap silently reach dispatch.
                raise ValueError("role output limit exceeds a candidate model's registered maximum")
            escalation = profile.escalate_to
            if escalation and escalation.startswith("tier:"):
                target_tier = escalation[5:]
                tiers = {models[model_id].tier for model_id in candidate_models}
                if any(_TIER_ORDER[target_tier] <= _TIER_ORDER[tier] for tier in tiers):
                    raise ValueError("capability escalation must target a strictly higher tier")
            elif escalation and escalation in {"reviewer", "director"} and escalation not in role_names:
                raise ValueError("role escalation references a disabled role")
        for rule in preset.selector_rules:
            if rule.when.role and set(rule.when.role) - role_names:
                raise ValueError("selector rule references a disabled role")
            if rule.when.required_capabilities_all and set(rule.when.required_capabilities_all) - capabilities:
                raise ValueError("selector rule references an unknown capability")
            for target in (rule.select, *rule.fallback):
                matches = _target_models(target, registry)
                if not matches or matches - available:
                    raise ValueError("selector rule target does not resolve to enabled registered models")
                if rule.reasoning_effort and any(
                    rule.reasoning_effort not in models[model_id].supported_reasoning_efforts
                    for model_id in matches
                ):
                    raise ValueError("selector reasoning effort is unsupported by a target model")
            selected = _target_models(rule.select, registry)
            for fallback in rule.fallback:
                fallback_models = _target_models(fallback, registry)
                for model_id in fallback_models:
                    for selected_id in selected:
                        if models[model_id].tier != models[selected_id].tier:
                            raise ValueError("selector fallback must stay at the selected tier")
                        if not models[selected_id].capabilities.issubset(models[model_id].capabilities):
                            raise ValueError("selector fallback cannot reduce capabilities")
        for rule in (*preset.guard_rules, *config.mandatory_guard_rules):
            constraints = rule.constraints
            if constraints.allowed_models is not None and set(constraints.allowed_models) - set(models):
                raise ValueError("guard rule references an unknown model")
            if constraints.allowed_providers is not None and set(constraints.allowed_providers) - set(providers):
                raise ValueError("guard rule references an unknown provider")
            if constraints.allowed_capabilities is not None and set(constraints.allowed_capabilities) - capabilities:
                raise ValueError("guard rule references an unknown capability")
            if constraints.denied_models is not None and set(constraints.denied_models) - set(models):
                raise ValueError("guard rule references an unknown model")
            if constraints.denied_providers is not None and set(constraints.denied_providers) - set(providers):
                raise ValueError("guard rule references an unknown provider")
            if rule.when.required_capabilities_all and set(rule.when.required_capabilities_all) - capabilities:
                raise ValueError("guard rule references an unknown capability")


def effective_config_json_schema() -> dict[str, Any]:
    return EffectiveConfig.model_json_schema()


def config_overlay_json_schema() -> dict[str, Any]:
    return ConfigOverlay.model_json_schema()


__all__ = [
    "ClassifierOverlay", "ClassifierSpec", "Complexity", "ConfigOverlay", "ConfigSource", "EffectiveConfig", "FailureCategory",
    "FrozenDict", "GuardConstraints", "GuardRule", "GuardWhen", "HealthPolicy", "HealthPolicyOverlay", "HealthState", "IntRange",
    "Preset", "PresetName", "PresetOverlay", "RecoveryAction", "ResolvedConfig", "RequestedBudget",
    "RoleName", "RoleProfile", "Risk", "SelectorRule", "SelectorTarget", "SelectorTombstone",
    "SelectorWhen", "TaskClass", "config_overlay_json_schema", "effective_config_json_schema",
    "resolve_effective_config",
]
