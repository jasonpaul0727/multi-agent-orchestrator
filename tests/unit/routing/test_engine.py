from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.config.effective import EffectiveConfig, SelectorRule
from orchestrator.config.models import ModelRegistryManifest, ModelSpec, PriceSpec, ProviderSpec
from orchestrator.models import (
    ExchangeRate,
    FXSnapshot,
    ModelGatewayFailure,
    TokenizerBinding,
    TokenizerSnapshot,
)
from orchestrator.routing import (
    EligibilityBlock,
    EligibilitySnapshot,
    FailureClassification,
    HealthAggregateKey,
    HealthAggregateRef,
    ModelHealthSnapshot,
    ModelRouter,
    RecoveryAuthorization,
    RecoveryController,
    RecoveryEvidence,
    HealthVersionRef,
    ProbeLease,
    RoutingRequest,
    SecretAvailability,
    classify_gateway_failure,
    compile_node_contract,
)
from orchestrator.security import PolicyAuthority, PolicyManifest, PolicyRule


NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
HASH = "sha256:" + "b" * 64
ROLES = (
    "planner",
    "coder",
    "document_analyst",
    "researcher",
    "tester",
    "reviewer",
    "director",
)
MODEL_RATES = {
    "model-economy-b": (200, "economy"),
    "model-economy-a": (100, "economy"),
    "model-standard": (500, "standard"),
    "model-high": (900, "high"),
}


def registry():
    provider = ProviderSpec(
        id="primary",
        adapter="openai_responses",
        secret_ref="env:MODEL_KEY",
        enabled=True,
    )
    models = tuple(
        ModelSpec(
            id=model_id,
            provider="primary",
            remote_model=f"remote-{model_id}",
            tier=tier,
            capabilities={"text", "tools"},
            context_window=128_000,
            max_output_tokens=2_000,
            supported_reasoning_efforts={"none", "low"},
            price=PriceSpec(
                currency="USD",
                input_minor_per_million=rate,
                output_minor_per_million=rate,
                max_tool_cost_minor=0,
                estimator_id="tokens.v1",
                effective_from=NOW - timedelta(days=2),
                expires_at=NOW + timedelta(days=5),
            ),
        )
        for model_id, (rate, tier) in MODEL_RATES.items()
    )
    return ModelRegistryManifest(providers=(provider,), models=models)


def policy_manifest(*, rules=()):
    return PolicyManifest(
        authorities=(
            PolicyAuthority(
                source="system",
                max_permission="read-only",
                allowed_actions={"model_invoke"},
                allowed_tools={f"model:{model_id}" for model_id in MODEL_RATES},
            ),
        ),
        rules=tuple(rules),
    )


def selector_rule(rule_id, priority, *, task_classes=("code_change",), complexity=None, model_id):
    when = {"task_class": list(task_classes)}
    if complexity is not None:
        when["complexity"] = [complexity]
    return SelectorRule(
        id=rule_id,
        priority=priority,
        when=when,
        select={"model": model_id},
        reasoning_effort=None,
        fallback=(),
        allow_degraded=False,
    )


def effective_config(reg, *, selector_rules=(), candidates=None):
    profile = {
        "candidates": candidates or [
            "model-economy-b",
            "model-economy-a",
            "model-standard",
            "model-high",
        ],
        "reasoning_effort": "none",
        "max_output_tokens": 2_000,
        "retries": 2,
        "escalate_to": None,
    }
    preset = {
        "requested_budget": {
            "max_cost_minor": 1_000,
            "max_total_tokens": 300_000,
            "max_agents": 50,
            "max_depth": 8,
            "max_concurrency": 8,
            "max_parallel_candidates": 3,
        },
        "roles": {role: dict(profile) for role in ROLES},
        "selector_rules": list(selector_rules),
        "guard_rules": [],
        "health_policy_ref": "health-default",
    }
    return EffectiveConfig.model_validate(
        {
            "schema_version": 1,
            "active_preset": "balanced",
            "registry_manifest_ref": reg.content_hash,
            "policy_envelope": {
                "currency": "USD",
                "max_cost_minor": 2_000,
                "max_total_tokens": 400_000,
                "max_agents": 60,
                "max_depth": 9,
                "max_concurrency": 10,
                "allowed_models": list(MODEL_RATES),
                "allowed_providers": ["primary"],
                "allowed_capabilities": ["text", "tools"],
                "denied_models": [],
                "denied_providers": [],
                "min_tier": "economy",
                "require_independent_review": False,
                "require_approval": False,
                "max_parallel_candidates": 4,
            },
            "mandatory_guard_rules": [],
            "presets": {
                name: preset for name in ("economic", "balanced", "quality", "custom")
            },
            "classifier": {
                "id": "orchestrator.deterministic.v1",
                "version": "1.0.0",
                "normalization_version": "unicode-nfkc-v1",
                "taxonomy_version": "maestro-task-taxonomy-v1",
            },
            "health_policies": {
                "health-default": {
                    "id": "health-default",
                    "failure_window_ms": 60_000,
                    "degrade_after": 2,
                    "open_after": 4,
                    "recovery_successes": 2,
                    "cooldown_ms": 10_000,
                    "max_probe_permits": 1,
                }
            },
        }
    )


def costing_snapshots():
    tokens = TokenizerSnapshot(
        bindings=tuple(
            TokenizerBinding(
                model_id=model_id,
                tokenizer_id="tokenizer.fixed",
                tokenizer_version="2026.09",
                estimator_id="tokens.v1",
                estimator_version="1.0",
                effective_from=NOW - timedelta(days=3),
                expires_at=NOW + timedelta(days=10),
            )
            for model_id in MODEL_RATES
        )
    )
    fx = FXSnapshot(
        base_currency="USD",
        source_id="fx.usd.2026-09-23",
        effective_from=NOW - timedelta(days=1),
        expires_at=NOW + timedelta(days=1),
        rates=(),
    )
    return tokens, fx


def health_entries(states=None):
    states = states or {}
    registry_hash = registry().content_hash
    provider_key = HealthAggregateKey(
        registry_manifest_hash=registry_hash,
        scope="provider",
        provider_id="primary",
    )
    return tuple(
        ModelHealthSnapshot(
            provider_id="primary",
            model_id=model_id,
            provider=HealthAggregateRef(
                aggregate_id=provider_key.aggregate_id,
                version=3,
                generation=1,
                state="healthy",
            ),
            model=HealthAggregateRef(
                aggregate_id=HealthAggregateKey(
                    registry_manifest_hash=registry_hash,
                    scope="model",
                    provider_id="primary",
                    model_id=model_id,
                ).aggregate_id,
                version=4,
                generation=1,
                state=states.get(model_id, "healthy"),
            ),
        )
        for model_id in MODEL_RATES
    )


def eligible_snapshot(policy, *, states=None, available_cost=2_000, secret_available=True, **updates):
    values = {
        "policy_manifest_hash": policy.content_hash,
        "budget_ledger_version": 7,
        "budget_currency": "USD",
        "available_cost_minor": available_cost,
        "available_tokens": 400_000,
        "secret_availability": (
            SecretAvailability(secret_ref="env:MODEL_KEY", available=secret_available, version=4),
        ),
        "model_health": health_entries(states),
        "revocation_version": 2,
        "emergency_deny_version": 5,
    }
    values.update(updates)
    return EligibilitySnapshot(**values)


def inputs(*, text="implement a small change", rules=(), policy_rules=(), available_cost=2_000, states=None, secret=True):
    reg = registry()
    config = effective_config(reg, selector_rules=rules)
    policy = policy_manifest(rules=policy_rules)
    contract = compile_node_contract(
        run_id="run-1",
        node_id="node-1",
        role="coder",
        task_text=text,
        config=config,
        registry=reg,
        policy_manifest=policy,
        context_tokens=100_000,
        max_output_tokens=1_000,
        required_capabilities=("text",),
    )
    request = RoutingRequest(
        request_id="route-1",
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=1,
        config_hash=config.content_hash,
        registry_hash=reg.content_hash,
        planning_contract_hash=contract.contract_hash,
        policy_manifest_hash=policy.content_hash,
        routing_as_of_event_time=NOW,
    )
    eligibility = eligible_snapshot(
        policy, states=states, available_cost=available_cost, secret_available=secret
    )
    tokens, fx = costing_snapshots()
    return request, config, reg, contract, eligibility, policy, tokens, fx


def route(args):
    request, config, reg, contract, eligibility, policy, tokens, fx = args
    return ModelRouter().route(
        request,
        config=config,
        registry=reg,
        contract=contract,
        eligibility=eligibility,
        policy_manifest=policy,
        tokenizer_snapshot=tokens,
        fx_snapshot=fx,
    )


def test_route_is_deterministic_filters_and_sorts_by_worst_cost_then_configured_rank():
    args = inputs()
    first = route(args)
    replayed = route(args)
    estimates = {item.model_id: item.estimated_cost for item in first.candidate_assessments}

    assert first == replayed
    assert first.decision_hash == replayed.decision_hash
    assert first.outcome == "selected"
    assert first.eligible_order == ("model-economy-a", "model-economy-b", "model-standard", "model-high")
    assert first.selected_model_id == "model-economy-a"
    assert estimates["model-economy-a"].price_snapshot_id == args[2].content_hash
    assert first.request_hash == args[0].request_hash
    selected_assessment = next(
        item for item in first.candidate_assessments if item.model_id == first.selected_model_id
    )
    assert selected_assessment.provider_health.version == 3
    assert selected_assessment.model_health.version == 4


def test_selector_uses_highest_matching_priority_and_records_rule():
    args = inputs(
        rules=(
            selector_rule("prefer-b", 2, model_id="model-economy-b"),
            selector_rule("prefer-a", 9, model_id="model-economy-a", complexity="low"),
        )
    )
    decision = route(args)

    assert decision.matched_selector_rule_id == "prefer-a"
    assert decision.selected_model_id == "model-economy-a"
    assert tuple(item.model_id for item in decision.candidate_assessments) == ("model-economy-a",)


def test_unknown_and_high_risk_planning_floor_excludes_economy():
    args = inputs(text="implement a security change")
    contract = args[3]
    decision = route(args)
    low_candidates = {
        item.model_id: item for item in decision.candidate_assessments
        if item.model_id.startswith("model-economy")
    }

    assert contract.policy_envelope.min_tier == "standard"
    assert contract.require_independent_review is True
    assert decision.selected_model_id == "model-standard"
    assert all("tier_below_minimum" in item.exclusion_reasons for item in low_candidates.values())


def test_policy_secret_health_and_budget_failures_are_explained():
    deny_all = PolicyRule(
        id="deny-models",
        effect="deny",
        action_categories={"model_invoke"},
    )
    denied = route(inputs(policy_rules=(deny_all,)))
    assert denied.outcome == "blocked"
    assert denied.blocked_reason == "policy_denied"
    assert all(item.policy_decision.outcome == "deny" for item in denied.candidate_assessments)

    missing_secret = route(inputs(secret=False))
    assert missing_secret.blocked_reason == "credential_unavailable"

    no_budget = route(inputs(available_cost=5))
    assert no_budget.blocked_reason == "budget_unavailable"
    assert all("cost_limit_exceeded" in item.exclusion_reasons for item in no_budget.candidate_assessments)

    unhealthy = route(inputs(states={model_id: "open" for model_id in MODEL_RATES}))
    assert unhealthy.blocked_reason == "all_candidates_unhealthy"
    assert all("model_unhealthy" in item.exclusion_reasons for item in unhealthy.candidate_assessments)


def test_healthy_candidate_wins_over_cheaper_degraded_candidate():
    args = inputs(states={"model-economy-a": "degraded"})
    decision = route(args)
    assessments = {item.model_id: item for item in decision.candidate_assessments}

    assert decision.selected_model_id == "model-economy-b"
    assert assessments["model-economy-a"].eligible is False
    assert "health_degraded_preferred_healthy" in assessments["model-economy-a"].exclusion_reasons


def test_recovery_authorization_is_bound_and_filters_to_controller_candidates():
    args = inputs()
    original = route(args)
    request, config, reg, contract, eligibility, policy, tokens, fx = args
    authorization = RecoveryAuthorization.create(
        action="same_tier_fallback",
        source_decision_hash=original.decision_hash,
        previous_model_id="model-economy-b",
        authorized_model_ids=("model-economy-a",),
        policy_manifest_hash=policy.content_hash,
        evidence_hash=HASH,
    )
    fallback_request = request.model_copy(
        update={
            "request_id": "route-fallback",
            "attempt_id": "attempt-2",
            "fencing_generation": 2,
            "recovery_action": "same_tier_fallback",
            "recovery_authorization": authorization,
            "prior_decision_hash": original.decision_hash,
            "previous_model_id": "model-economy-b",
            "retry_level": 1,
        }
    )
    fallback = ModelRouter().route(
        fallback_request,
        config=config,
        registry=reg,
        contract=contract,
        eligibility=eligibility,
        policy_manifest=policy,
        tokenizer_snapshot=tokens,
        fx_snapshot=fx,
    )

    assert fallback.selected_model_id == "model-economy-a"
    assert tuple(item.model_id for item in fallback.candidate_assessments) == ("model-economy-a",)
    with pytest.raises(ValueError, match="different policy version"):
        stale_authorization = RecoveryAuthorization.create(
            action="same_tier_fallback",
            source_decision_hash=original.decision_hash,
            previous_model_id="model-economy-b",
            authorized_model_ids=("model-economy-a",),
            policy_manifest_hash=HASH,
            evidence_hash=HASH,
        )
        RoutingRequest(
            **{
                **request.model_dump(),
                "recovery_action": "same_tier_fallback",
                "recovery_authorization": stale_authorization.model_dump(mode="json"),
                "prior_decision_hash": original.decision_hash,
                "previous_model_id": "model-economy-b",
            }
        )


def test_recovery_controller_bounds_retries_and_same_tier_fallback():
    args = inputs()
    original = route(args)
    request, config, reg, contract, eligibility, policy, tokens, fx = args
    controller = RecoveryController()
    retry_plan = controller.plan(
        source_decision=original,
        contract=contract,
        config=config,
        registry=reg,
        evidence=RecoveryEvidence(
            failure_category="transient",
            evidence_hash=HASH,
            retry_level=0,
            retry_safe=True,
        ),
    )
    assert retry_plan.action == "same_model_retry"
    assert retry_plan.authorized_model_ids == (original.selected_model_id,)

    retry_request = request.model_copy(
        update={
            "request_id": "route-retry",
            "attempt_id": "attempt-retry",
            "fencing_generation": 2,
            "recovery_action": retry_plan.action,
            "recovery_authorization": retry_plan.authorization,
            "prior_decision_hash": original.decision_hash,
            "previous_model_id": original.selected_model_id,
            "retry_level": 1,
        }
    )
    retry_decision = ModelRouter().route(
        retry_request,
        config=config,
        registry=reg,
        contract=contract,
        eligibility=eligibility,
        policy_manifest=policy,
        tokenizer_snapshot=tokens,
        fx_snapshot=fx,
    )
    assert retry_decision.selected_model_id == original.selected_model_id

    fallback_plan = controller.plan(
        source_decision=original,
        contract=contract,
        config=config,
        registry=reg,
        evidence=RecoveryEvidence(
            failure_category="transient",
            evidence_hash=HASH,
            retry_level=0,
            failed_model_unavailable=True,
        ),
    )
    assert fallback_plan.action == "same_tier_fallback"
    assert fallback_plan.authorized_model_ids == ("model-economy-b",)
    unresolved = controller.plan(
        source_decision=original,
        contract=contract,
        config=config,
        registry=reg,
        evidence=RecoveryEvidence(
            failure_category="transient",
            evidence_hash=HASH,
            retry_level=0,
            retry_safe=False,
        ),
    )
    assert unresolved.outcome == "blocked"
    assert unresolved.reason == "transient_outcome_requires_reconciliation"
    repair = controller.plan(
        source_decision=original,
        contract=contract,
        config=config,
        registry=reg,
        evidence=RecoveryEvidence(
            failure_category="output_invalid",
            evidence_hash=HASH,
            retry_level=0,
        ),
    )
    assert repair.outcome == "new_child_required"
    assert repair.action is None


def test_recovery_controller_never_falls_back_for_unknown_outcome_even_after_retry_budget():
    args = inputs()
    original = route(args)
    _request, config, registry, contract, _eligibility, _policy, _tokens, _fx = args

    plan = RecoveryController().plan(
        source_decision=original,
        contract=contract,
        config=config,
        registry=registry,
        evidence=RecoveryEvidence(
            failure_category="transient",
            evidence_hash=HASH,
            retry_level=100,
            failed_model_unavailable=True,
            outcome_unknown=True,
        ),
    )

    assert plan.outcome == "blocked"
    assert plan.reason == "unknown_outcome_requires_reconciliation"
    assert plan.action is None


def test_recovery_evidence_cannot_mark_unknown_outcome_retry_safe():
    with pytest.raises(ValueError, match="unknown outcome cannot be marked retry-safe"):
        RecoveryEvidence(
            failure_category="transient",
            evidence_hash=HASH,
            retry_level=0,
            retry_safe=True,
            outcome_unknown=True,
        )


def test_gateway_failure_classification_is_deterministic_and_sanitized():
    args = inputs()
    original = route(args)
    failure = ModelGatewayFailure(
        code="rate_limited",
        phase="provider",
        outcome="known_failure",
        retryable=True,
        http_status=429,
        provider_code="quota_window",
        provider_request_id="provider-request-secret",
        retry_after_ms=250,
    )

    classified = classify_gateway_failure(
        failure, source_decision=original, retry_level=0
    )
    replayed = classify_gateway_failure(
        failure.model_copy(update={"provider_request_id": "different-request-id"}),
        source_decision=original,
        retry_level=0,
    )

    assert classified == replayed
    assert classified.disposition == "classified"
    assert classified.reason == "gateway_failure_classified"
    assert classified.evidence is not None
    assert classified.evidence.failure_category == "transient"
    assert classified.evidence.retry_safe is True
    assert classified.evidence.outcome_unknown is False
    _request, config, reg, contract, _eligibility, _policy, _tokens, _fx = args
    recovery_plan = RecoveryController().plan(
        source_decision=original,
        contract=contract,
        config=config,
        registry=reg,
        evidence=classified.evidence,
    )
    assert recovery_plan.action == "same_model_retry"
    assert recovery_plan.authorized_model_ids == (original.selected_model_id,)


@pytest.mark.parametrize(
    "code,phase",
    [("timeout", "transport"), ("transport_error", "transport"), ("outcome_unknown", "settlement")],
)
def test_gateway_unknown_outcome_classification_requires_reconciliation(code, phase):
    original = route(inputs())
    failure = ModelGatewayFailure(
        code=code,
        phase=phase,
        outcome="unknown",
        retryable=False,
    )
    result = classify_gateway_failure(failure, source_decision=original, retry_level=1)
    args = inputs()
    _request, config, reg, contract, _eligibility, _policy, _tokens, _fx = args

    assert result.disposition == "reconciliation_required"
    assert result.evidence is not None
    assert result.evidence.outcome_unknown is True
    plan = RecoveryController().plan(
        source_decision=original,
        contract=contract,
        config=config,
        registry=reg,
        evidence=result.evidence,
    )
    assert plan.outcome == "blocked"
    assert plan.reason == "unknown_outcome_requires_reconciliation"
    assert plan.action is None


@pytest.mark.parametrize(
    "code,phase,outcome",
    [
        ("policy_denied", "preflight", "not_sent"),
        ("authentication_failed", "provider", "known_failure"),
        ("credential_unavailable", "preflight", "not_sent"),
    ],
)
def test_gateway_nonrecoverable_failure_classification_is_blocked(code, phase, outcome):
    original = route(inputs())
    result = classify_gateway_failure(
        ModelGatewayFailure(
            code=code,
            phase=phase,
            outcome=outcome,
            retryable=False,
        ),
        source_decision=original,
        retry_level=0,
    )

    assert result.disposition == "blocked"
    assert result.reason == "failure_not_recoverable"
    assert result.evidence is None


@pytest.mark.parametrize(
    "code,category",
    [
        ("invalid_response", "output_invalid"),
        ("context_length_exceeded", "capability_failure"),
        ("output_limit_exceeded", "capability_failure"),
    ],
)
def test_gateway_failure_classification_preserves_repair_category(code, category):
    original = route(inputs())
    result = classify_gateway_failure(
        ModelGatewayFailure(
            code=code,
            phase="provider",
            outcome="known_failure",
            retryable=False,
        ),
        source_decision=original,
        retry_level=0,
    )

    assert result.disposition == "classified"
    assert result.evidence is not None
    assert result.evidence.failure_category == category
    assert result.evidence.retry_safe is False


def test_gateway_failure_classification_requires_selected_source_and_consistent_evidence():
    blocked_source = route(inputs()).model_copy(update={"outcome": "blocked"})
    failure = ModelGatewayFailure(
        code="rate_limited",
        phase="provider",
        outcome="known_failure",
        retryable=True,
    )

    result = classify_gateway_failure(
        failure, source_decision=blocked_source, retry_level=0
    )
    assert result.disposition == "blocked"
    assert result.reason == "source_decision_not_selected"
    assert result.evidence is None

    evidence = RecoveryEvidence(
        failure_category="transient",
        evidence_hash=HASH,
        retry_level=0,
        outcome_unknown=True,
    )
    with pytest.raises(ValueError, match="unknown provider outcomes require reconciliation"):
        FailureClassification(
            disposition="classified",
            reason="invalid",
            evidence=evidence,
        )


def test_gateway_known_success_never_enters_failure_retry_or_fallback_planning():
    original = route(inputs())
    result = classify_gateway_failure(
        ModelGatewayFailure(
            code="usage_unavailable",
            phase="settlement",
            outcome="known_success",
            retryable=False,
        ),
        source_decision=original,
        retry_level=0,
    )

    assert result.disposition == "blocked"
    assert result.reason == "successful_call_requires_settlement"
    assert result.evidence is None


def test_capability_escalation_uses_declared_tier_and_probe_lease_scope():
    args = inputs()
    request, config, reg, old_contract, eligibility, policy, tokens, fx = args
    presets = dict(config.presets)
    balanced = presets["balanced"]
    roles = dict(balanced.roles)
    roles["coder"] = roles["coder"].model_copy(
        update={"candidates": ("model-economy-a",), "escalate_to": "tier:standard"}
    )
    presets["balanced"] = balanced.model_copy(update={"roles": roles})
    config = config.model_copy(update={"presets": presets})
    contract = compile_node_contract(
        run_id="run-1",
        node_id="node-1",
        role="coder",
        task_text="implement a small change",
        config=config,
        registry=reg,
        policy_manifest=policy,
        context_tokens=100_000,
        max_output_tokens=1_000,
        required_capabilities=("text",),
    )
    initial_request = request.model_copy(
        update={"config_hash": config.content_hash, "planning_contract_hash": contract.contract_hash}
    )
    source = ModelRouter().route(
        initial_request,
        config=config,
        registry=reg,
        contract=contract,
        eligibility=eligibility,
        policy_manifest=policy,
        tokenizer_snapshot=tokens,
        fx_snapshot=fx,
    )
    plan = RecoveryController().plan(
        source_decision=source,
        contract=contract,
        config=config,
        registry=reg,
        evidence=RecoveryEvidence(
            failure_category="capability_failure",
            evidence_hash=HASH,
            retry_level=0,
        ),
    )
    assert plan.action == "capability_escalation"
    escalation_request = initial_request.model_copy(
        update={
            "request_id": "route-escalation",
            "attempt_id": "attempt-escalation",
            "fencing_generation": 2,
            "recovery_action": "capability_escalation",
            "recovery_authorization": plan.authorization,
            "prior_decision_hash": source.decision_hash,
            "previous_model_id": source.selected_model_id,
            "failure_category": "capability_failure",
        }
    )
    escalation = ModelRouter().route(
        escalation_request,
        config=config,
        registry=reg,
        contract=contract,
        eligibility=eligibility,
        policy_manifest=policy,
        tokenizer_snapshot=tokens,
        fx_snapshot=fx,
    )
    assert escalation.selected_model_id == "model-standard"

    probe_health = health_entries({"model-economy-b": "half_open"})
    probe_eligibility = eligible_snapshot(policy, states={"model-economy-b": "half_open"})
    probe_lease = ProbeLease.create(
        lease_id="probe-route-1",
        holder_id="health-controller",
        registry_manifest_hash=reg.content_hash,
        provider_id="primary",
        model_id="model-economy-b",
        expires_at=NOW + timedelta(minutes=1),
        aggregates=(
            HealthVersionRef(
                aggregate_id=HealthAggregateKey(
                    registry_manifest_hash=reg.content_hash,
                    scope="model",
                    provider_id="primary",
                    model_id="model-economy-b",
                ).aggregate_id,
                version=4,
                generation=1,
            ),
        ),
    )
    base_args = inputs()
    base_request, base_config, base_reg, base_contract, _, base_policy, base_tokens, base_fx = base_args
    source = route(base_args)
    auth = RecoveryAuthorization.create(
        action="health_probe",
        source_decision_hash=source.decision_hash,
        previous_model_id=source.selected_model_id,
        authorized_model_ids=("model-economy-b",),
        policy_manifest_hash=base_policy.content_hash,
        evidence_hash=HASH,
    )
    probe_request = base_request.model_copy(
        update={
            "request_id": "route-probe",
            "recovery_action": "health_probe",
            "recovery_authorization": auth,
            "prior_decision_hash": source.decision_hash,
            "previous_model_id": source.selected_model_id,
            "health_state": "half_open",
            "probe_lease": probe_lease,
        }
    )
    probe_eligibility = probe_eligibility.model_copy(update={"model_health": probe_health})
    probe = ModelRouter().route(
        probe_request,
        config=base_config,
        registry=base_reg,
        contract=base_contract,
        eligibility=probe_eligibility,
        policy_manifest=base_policy,
        tokenizer_snapshot=base_tokens,
        fx_snapshot=base_fx,
    )
    assert probe.outcome == "selected"
    assert probe.selected_model_id == "model-economy-b"


def test_routing_request_rejects_stale_snapshots_and_probe_without_lease_blocks():
    args = inputs()
    request, config, reg, contract, eligibility, policy, tokens, fx = args
    with pytest.raises(ValueError, match="stale registry snapshot"):
        ModelRouter().route(
            request.model_copy(update={"registry_hash": HASH}),
            config=config,
            registry=reg,
            contract=contract,
            eligibility=eligibility,
            policy_manifest=policy,
            tokenizer_snapshot=tokens,
            fx_snapshot=fx,
        )

    authorization = RecoveryAuthorization.create(
        action="health_probe",
        source_decision_hash=route(args).decision_hash,
        previous_model_id="model-economy-a",
        authorized_model_ids=("model-economy-a",),
        policy_manifest_hash=policy.content_hash,
        evidence_hash=HASH,
    )
    probe_request = request.model_copy(
        update={
            "recovery_action": "health_probe",
            "recovery_authorization": authorization,
            "prior_decision_hash": authorization.source_decision_hash,
            "previous_model_id": authorization.previous_model_id,
        }
    )
    probe = ModelRouter().route(
        probe_request,
        config=config,
        registry=reg,
        contract=contract,
        eligibility=eligibility,
        policy_manifest=policy,
        tokenizer_snapshot=tokens,
        fx_snapshot=fx,
    )
    assert probe.outcome == "blocked"
    assert probe.blocked_reason == "health_probe_lease_required"
