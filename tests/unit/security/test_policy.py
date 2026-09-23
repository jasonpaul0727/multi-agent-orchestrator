import pytest
from pydantic import ValidationError

from orchestrator.security import (
    PolicyAuthority,
    PolicyEngine,
    PolicyManifest,
    PolicyRequest,
    PolicyRule,
)


HASH = "sha256:" + "a" * 64


def manifest(*, rules=(), caller=None, approval_actions=()):
    system = PolicyAuthority(
        source="system",
        max_permission="full-trust",
        allowed_actions={"safe_read", "external_mutation", "model_invoke"},
        allowed_tools={"fs.read", "git.push", "model:model-1"},
        approval_actions=frozenset(approval_actions),
    )
    layers = (system,) if caller is None else (system, caller)
    return PolicyManifest(authorities=layers, rules=tuple(rules))


def request(policy_manifest_hash, *, action="safe_read", tool="fs.read", permission="read-only", **updates):
    values = {
        "request_id": "policy-req-1",
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "fencing_generation": 3,
        "role": "coder",
        "action_category": action,
        "tool_id": tool,
        "required_permission": permission,
        "normalized_request_hash": HASH,
        "policy_manifest_hash": policy_manifest_hash,
        "revocation_version": 4,
        "emergency_deny_version": 2,
    }
    values.update(updates)
    return PolicyRequest(**values)


def test_policy_deny_precedes_approval_and_allow_independent_of_rule_order():
    allow = PolicyRule(
        id="a-allow",
        effect="allow",
        action_categories={"external_mutation"},
        tool_ids={"git.push"},
    )
    deny = PolicyRule(
        id="z-deny",
        effect="deny",
        action_categories={"external_mutation"},
        tool_ids={"git.push"},
    )
    first = manifest(rules=(allow, deny), approval_actions={"external_mutation"})
    reversed_manifest = manifest(rules=(deny, allow), approval_actions={"external_mutation"})

    assert first.content_hash == reversed_manifest.content_hash
    result = PolicyEngine().evaluate(
        request(first.content_hash, action="external_mutation", tool="git.push", permission="full-trust"),
        first,
    )

    assert result.outcome == "deny"
    assert result.matched_rule_ids == ("a-allow", "z-deny")
    assert "approval_required_by_authority" in result.reasons
    assert "rule_denied:z-deny" in result.reasons


def test_policy_authority_allowlists_intersect_and_permission_only_tightens():
    policy = manifest(
        caller=PolicyAuthority(
            source="caller",
            max_permission="workspace-write",
            allowed_actions={"safe_read"},
            allowed_tools={"fs.read"},
        )
    )
    result = PolicyEngine().evaluate(
        request(policy.content_hash, action="external_mutation", tool="git.push", permission="full-trust"),
        policy,
    )

    assert result.outcome == "deny"
    assert result.effective_max_permission == "workspace-write"
    assert {"action_not_allowlisted", "tool_not_allowlisted", "permission_level_exceeded"}.issubset(
        result.reasons
    )


def test_approval_is_not_an_allow_and_emergency_deny_wins():
    policy = manifest(approval_actions={"external_mutation"})
    approved_later = PolicyEngine().evaluate(
        request(policy.content_hash, action="external_mutation", tool="git.push", permission="full-trust"),
        policy,
    )
    emergency = PolicyEngine().evaluate(
        request(
            policy.content_hash,
            action="safe_read",
            emergency_denied=True,
        ),
        policy,
    )

    assert approved_later.outcome == "needs_approval"
    assert emergency.outcome == "deny"
    assert "emergency_deny" in emergency.reasons


def test_policy_decisions_bind_attempt_generation_and_policy_version():
    policy = manifest()
    base = request(policy.content_hash)
    decision = PolicyEngine().evaluate(base, policy)
    other_attempt = PolicyEngine().evaluate(
        base.model_copy(update={"attempt_id": "attempt-2"}), policy
    )

    assert decision.decision_hash != other_attempt.decision_hash
    with pytest.raises(ValueError, match="different manifest version"):
        PolicyEngine().evaluate(request(HASH), policy)
    with pytest.raises(ValidationError, match="system authority requires"):
        PolicyAuthority(source="system", max_permission="read-only")
