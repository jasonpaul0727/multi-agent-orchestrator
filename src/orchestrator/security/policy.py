"""Pure, deterministic policy evaluation over immutable authority snapshots."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator
from orchestrator.validation import revalidate_model


PermissionLevel = Literal["read-only", "workspace-write", "full-trust"]
ActionCategory = Literal[
    "safe_read",
    "managed_web_read",
    "reversible_workspace_write",
    "irreversible_delete",
    "secret_use",
    "external_mutation",
    "host_privileged",
    "model_invoke",
    "always_deny",
]
PolicyEffect = Literal["allow", "needs_approval", "deny"]
PolicyOutcome = Literal["allow", "needs_approval", "deny"]
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_PERMISSION_ORDER = {"read-only": 0, "workspace-write": 1, "full-trust": 2}
_EFFECT_ORDER = {"allow": 0, "needs_approval": 1, "deny": 2}
_ALWAYS_DENIED = frozenset({"always_deny"})


def _hash(payload: object) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _identifier(value: str, field: str) -> str:
    if value != value.strip() or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field} must be a stable non-blank identifier")
    return value


class _PolicyModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class PolicyAuthority(_PolicyModel):
    """One authority layer; allowlists intersect and denies/approvals accumulate."""

    source: Literal["system", "caller", "user", "project", "run"]
    max_permission: PermissionLevel
    allowed_actions: frozenset[ActionCategory] | None = None
    allowed_tools: frozenset[StrictStr] | None = None
    denied_actions: frozenset[ActionCategory] = frozenset()
    denied_tools: frozenset[StrictStr] = frozenset()
    approval_actions: frozenset[ActionCategory] = frozenset()

    @field_validator("allowed_actions", "denied_actions", "approval_actions", mode="before")
    @classmethod
    def freeze_actions(cls, value: object) -> frozenset[object] | None:
        if value is None:
            return None
        if not isinstance(value, (set, frozenset, tuple, list)):
            raise ValueError("actions must be an array")
        return frozenset(value)

    @field_validator("allowed_tools", "denied_tools", mode="before")
    @classmethod
    def freeze_tools(cls, value: object) -> frozenset[object] | None:
        if value is None:
            return None
        if not isinstance(value, (set, frozenset, tuple, list)):
            raise ValueError("tools must be an array")
        return frozenset(value)

    @field_validator("allowed_tools", "denied_tools")
    @classmethod
    def validate_tool_ids(cls, value: frozenset[str] | None) -> frozenset[str] | None:
        if value is not None:
            for tool_id in value:
                _identifier(tool_id, "tool id")
        return value

    @model_validator(mode="after")
    def require_system_allowlists(self) -> "PolicyAuthority":
        if self.source == "system" and (not self.allowed_actions or not self.allowed_tools):
            raise ValueError("system authority requires non-empty action and tool allowlists")
        return self


class PolicyRule(_PolicyModel):
    """A finite, exact-match rule; rules can only further restrict authority."""

    id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    effect: PolicyEffect
    action_categories: frozenset[ActionCategory] = Field(min_length=1)
    tool_ids: frozenset[StrictStr] | None = None
    roles: frozenset[StrictStr] | None = None

    @field_validator("action_categories", "tool_ids", "roles", mode="before")
    @classmethod
    def freeze_sets(cls, value: object, info: object) -> frozenset[object] | None:
        if value is None:
            return None
        if not isinstance(value, (set, frozenset, tuple, list)):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must be an array")
        return frozenset(value)

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return _identifier(value, "policy rule id")

    @field_validator("tool_ids", "roles")
    @classmethod
    def validate_identifiers(cls, value: frozenset[str] | None, info: object) -> frozenset[str] | None:
        if value is not None:
            for item in value:
                _identifier(item, getattr(info, "field_name", "identifier"))
        return value

    @model_validator(mode="after")
    def nonempty_match_sets(self) -> "PolicyRule":
        for field_name in ("tool_ids", "roles"):
            value = getattr(self, field_name)
            if value is not None and not value:
                raise ValueError(f"{field_name} cannot be empty")
        return self


class PolicyManifest(_PolicyModel):
    """Content-addressed authority and rule snapshot used by one Run."""

    schema_version: Literal[1] = 1
    authorities: tuple[PolicyAuthority, ...] = Field(min_length=1)
    rules: tuple[PolicyRule, ...] = ()

    @field_validator("authorities", "rules", mode="before")
    @classmethod
    def normalize_sequences(cls, value: object, info: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def validate_layers(self) -> "PolicyManifest":
        sources = [layer.source for layer in self.authorities]
        expected_order = ["system", "caller", "user", "project", "run"]
        if not sources or sources[0] != "system" or sources != sorted(sources, key=expected_order.index):
            raise ValueError("policy authorities must start with system and follow authority order")
        if len(sources) != len(set(sources)):
            raise ValueError("policy authority sources must be unique")
        rule_ids = [rule.id for rule in self.rules]
        if len(rule_ids) != len(set(rule_ids)):
            raise ValueError("policy rule ids must be unique")
        return self

    @property
    def content_hash(self) -> str:
        payload = self.model_dump(mode="json")
        payload["authorities"] = sorted(payload["authorities"], key=lambda item: item["source"])
        for authority in payload["authorities"]:
            for name in (
                "allowed_actions",
                "allowed_tools",
                "denied_actions",
                "denied_tools",
                "approval_actions",
            ):
                if authority[name] is not None:
                    authority[name] = sorted(authority[name])
        payload["rules"] = sorted(payload["rules"], key=lambda item: item["id"])
        for rule in payload["rules"]:
            for name in ("action_categories", "tool_ids", "roles"):
                if rule[name] is not None:
                    rule[name] = sorted(rule[name])
        return _hash(payload)


class PolicyRequest(_PolicyModel):
    """Normalized action identity; raw arguments stay outside the policy log."""

    request_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    run_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    attempt_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    fencing_generation: StrictInt = Field(ge=0)
    role: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    action_category: ActionCategory
    tool_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    required_permission: PermissionLevel
    normalized_request_hash: StrictStr
    policy_manifest_hash: StrictStr
    revocation_version: StrictInt = Field(ge=0)
    emergency_deny_version: StrictInt = Field(ge=0)
    revoked: StrictBool = False
    emergency_denied: StrictBool = False

    @field_validator("normalized_request_hash", "policy_manifest_hash")
    @classmethod
    def validate_hashes(cls, value: str) -> str:
        if not _HASH.fullmatch(value):
            raise ValueError("policy references must be sha256 content hashes")
        return value


class PolicyDecision(_PolicyModel):
    """Deterministic, scope-bound result with stable explanation codes."""

    request: PolicyRequest
    policy_manifest_hash: StrictStr
    effective_max_permission: PermissionLevel
    outcome: PolicyOutcome
    matched_rule_ids: tuple[StrictStr, ...]
    reasons: tuple[StrictStr, ...]
    decision_hash: StrictStr

    @field_validator("policy_manifest_hash", "decision_hash")
    @classmethod
    def validate_hashes(cls, value: str) -> str:
        if not _HASH.fullmatch(value):
            raise ValueError("policy references must be sha256 content hashes")
        return value

    @model_validator(mode="after")
    def verify_decision_hash(self) -> "PolicyDecision":
        payload = self.model_dump(mode="json", exclude={"decision_hash"})
        if self.decision_hash != _hash(payload):
            raise ValueError("decision_hash does not match the policy decision")
        if self.policy_manifest_hash != self.request.policy_manifest_hash:
            raise ValueError("policy decision does not match the request policy version")
        return self


class PolicyEngine:
    """Evaluate action authority without I/O, clocks, ambient state, or model calls."""

    def evaluate(self, request: PolicyRequest, manifest: PolicyManifest) -> PolicyDecision:
        request = revalidate_model(PolicyRequest, request)
        manifest = revalidate_model(PolicyManifest, manifest)
        if request.policy_manifest_hash != manifest.content_hash:
            raise ValueError("policy request references a different manifest version")

        max_permission = min(
            (layer.max_permission for layer in manifest.authorities),
            key=_PERMISSION_ORDER.__getitem__,
        )
        action_allowlists = [layer.allowed_actions for layer in manifest.authorities if layer.allowed_actions is not None]
        tool_allowlists = [layer.allowed_tools for layer in manifest.authorities if layer.allowed_tools is not None]
        allowed_actions = set.intersection(*(set(value) for value in action_allowlists))
        allowed_tools = set.intersection(*(set(value) for value in tool_allowlists))
        denied_actions = set().union(*(layer.denied_actions for layer in manifest.authorities))
        denied_tools = set().union(*(layer.denied_tools for layer in manifest.authorities))
        approval_actions = set().union(*(layer.approval_actions for layer in manifest.authorities))

        matches = tuple(sorted(rule.id for rule in manifest.rules if _matches(rule, request)))
        matching_rules = tuple(rule for rule in manifest.rules if rule.id in matches)
        reasons: set[str] = set()
        hard_deny = False
        needs_approval = False

        if request.revoked:
            hard_deny = True
            reasons.add("authorization_revoked")
        if request.emergency_denied:
            hard_deny = True
            reasons.add("emergency_deny")
        if request.action_category in _ALWAYS_DENIED:
            hard_deny = True
            reasons.add("system_always_deny")
        if request.action_category in denied_actions:
            hard_deny = True
            reasons.add("action_denied")
        if request.tool_id in denied_tools:
            hard_deny = True
            reasons.add("tool_denied")
        if _PERMISSION_ORDER[request.required_permission] > _PERMISSION_ORDER[max_permission]:
            hard_deny = True
            reasons.add("permission_level_exceeded")
        if request.action_category not in allowed_actions:
            hard_deny = True
            reasons.add("action_not_allowlisted")
        if request.tool_id not in allowed_tools:
            hard_deny = True
            reasons.add("tool_not_allowlisted")
        if request.action_category in approval_actions:
            needs_approval = True
            reasons.add("approval_required_by_authority")

        for rule in matching_rules:
            if rule.effect == "deny":
                hard_deny = True
                reasons.add(f"rule_denied:{rule.id}")
            elif rule.effect == "needs_approval":
                needs_approval = True
                reasons.add(f"rule_requires_approval:{rule.id}")
            else:
                reasons.add(f"rule_allowed:{rule.id}")

        outcome: PolicyOutcome = "deny" if hard_deny else "needs_approval" if needs_approval else "allow"
        if not reasons:
            reasons.add("within_authority")
        payload = {
            "request": request.model_dump(mode="json"),
            "policy_manifest_hash": manifest.content_hash,
            "effective_max_permission": max_permission,
            "outcome": outcome,
            "matched_rule_ids": matches,
            "reasons": tuple(sorted(reasons)),
        }
        return PolicyDecision(**payload, decision_hash=_hash(payload))


def _matches(rule: PolicyRule, request: PolicyRequest) -> bool:
    return (
        request.action_category in rule.action_categories
        and (rule.tool_ids is None or request.tool_id in rule.tool_ids)
        and (rule.roles is None or request.role in rule.roles)
    )


__all__ = [
    "ActionCategory",
    "PermissionLevel",
    "PolicyAuthority",
    "PolicyDecision",
    "PolicyEffect",
    "PolicyEngine",
    "PolicyManifest",
    "PolicyOutcome",
    "PolicyRequest",
    "PolicyRule",
]
