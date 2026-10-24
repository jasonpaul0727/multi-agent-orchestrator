"""Explicit-input recovery admission through existing Scheduler authority."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
from types import UnionType
from typing import Annotated, Literal, Union, get_args, get_origin
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, StrictStr, model_validator
from pydantic_core import TzInfo

from orchestrator.models import AcceptedModelRoute, FXSnapshot, TokenizerSnapshot
from orchestrator.routing import (
    EligibilitySnapshot, HealthAggregateKey, HealthController, ModelRouter, RoutingRequest,
)
from orchestrator.runtime.contracts import WorkerTask
from orchestrator.runtime.recovery_opportunities import (
    RecoveryContinuationError, RecoveryOpportunity, load_recovery_records,
)
from orchestrator.runtime.worker_input import WorkerInputError, build_worker_input_manifest
from orchestrator.runtime.worker_input_service import WorkerInputService
from orchestrator.scheduler import Scheduler, SchedulerError
from orchestrator.security import PolicyManifest
from orchestrator.validation import revalidate_model


_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$"
_TRUSTED_TIMEZONE_TYPES = (timezone, TzInfo, ZoneInfo)


def _is_data_datetime(value):
    """Inspect exact built-in slots without invoking caller timezone methods."""
    return type(value) is datetime and any(
        type(value.tzinfo) is trusted for trusted in _TRUSTED_TIMEZONE_TYPES
    )


def _check_declared_routing_data(value, expected):
    """Reject unsafe copies/subclasses before revalidation can discard extras."""
    origin = get_origin(expected)
    if origin is Annotated:
        return _check_declared_routing_data(value, get_args(expected)[0])
    if origin in {Union, UnionType}:
        alternatives = get_args(expected)
        if value is None and type(None) in alternatives:
            return
        nonnull = tuple(item for item in alternatives if item is not type(None))
        if len(nonnull) != 1:
            raise ValueError("unexpected routing union data")
        return _check_declared_routing_data(value, nonnull[0])
    if isinstance(expected, type) and issubclass(expected, BaseModel):
        if type(value) is not expected:
            raise ValueError("unexpected routing model type")
        extra = value.model_extra
        values = value.__dict__
        if (type(values) is not dict
            or any(type(key) is not str or key not in expected.model_fields for key in values)
            or extra is not None and (type(extra) is not dict or len(extra) != 0)):
            raise ValueError("unexpected routing model data")
        for name, field in expected.model_fields.items():
            _check_declared_routing_data(getattr(value, name), field.annotation)
    elif origin in {tuple, list, set, frozenset}:
        if type(value) is not origin:
            raise ValueError("unexpected routing sequence data")
        element_type = get_args(expected)[0]
        for item in value:
            _check_declared_routing_data(item, element_type)
    elif expected is datetime or type(value) is datetime:
        if not _is_data_datetime(value):
            raise ValueError("unexpected routing datetime data")
    elif all(type(value) is not trusted for trusted in (str, int, bool, type(None))):
        raise ValueError("unexpected routing scalar data")


def _check_current_routing_inputs(scheduler, run_id, snapshot, observations):
    balance = scheduler._ledger_for_run(run_id).available(run_id)
    eligibility = observations.eligibility
    if (eligibility.budget_currency, eligibility.budget_ledger_version,
        eligibility.available_cost_minor, eligibility.available_tokens) != (
            balance.currency, balance.reservation_version, balance.available_minor, balance.available_tokens,
        ):
        raise ValueError("stale routing budget observations")
    config = snapshot.resolved_config.config
    policy = config.health_policies[config.presets[config.active_preset].health_policy_ref]
    controller = HealthController(scheduler.event_store)
    models = {model.id: model for model in snapshot.registry_manifest.models}
    for entry in eligibility.model_health:
        model = models.get(entry.model_id)
        if model is None or model.provider != entry.provider_id:
            raise ValueError("invalid routing health binding")
        provider_key = HealthAggregateKey(
            registry_manifest_hash=snapshot.registry_manifest_hash,
            scope="provider", provider_id=model.provider,
        )
        model_key = HealthAggregateKey(
            registry_manifest_hash=snapshot.registry_manifest_hash,
            scope="model", provider_id=model.provider, model_id=model.id,
        )
        if (entry.provider != controller.read(provider_key, default_policy=policy).routing_ref
            or entry.model != controller.read(model_key, default_policy=policy).routing_ref):
            raise ValueError("stale routing health observations")


class RecoveryRoutingInputs(BaseModel):
    """Host-supplied routing observations; no executable or credential access."""

    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", hide_input_in_errors=True)

    eligibility: EligibilitySnapshot
    policy_manifest: PolicyManifest
    tokenizer_snapshot: TokenizerSnapshot
    fx_snapshot: FXSnapshot


class RecoveryContinuationResult(BaseModel):
    """Bounded metadata and exact accepted identity, without historic decisions."""

    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", hide_input_in_errors=True)

    status: Literal[
        "ready", "already_admitted", "reconciliation_required", "new_child_required", "blocked",
        "waiting_for_input", "waiting_for_routing", "admitted", "prepared", "successor_inactive",
    ]
    reason: StrictStr = Field(pattern=r"^[a-z][a-z0-9_]{0,127}$")
    opportunity: RecoveryOpportunity
    accepted_route: AcceptedModelRoute | None = None
    task: WorkerTask | None = Field(default=None, repr=False)

    @model_validator(mode="after")
    def validate_result_binding(self):
        if (self.task is not None) != (self.status == "prepared"):
            raise ValueError("only prepared results carry a task")
        if self.status in {"admitted", "already_admitted", "prepared", "successor_inactive"}:
            if self.accepted_route is None:
                raise ValueError("accepted identity is required")
        return self


class RecoveryContinuationService:
    """Route and consume one persisted authorization in one checked transaction."""

    def __init__(self, scheduler: Scheduler, worker_inputs: WorkerInputService) -> None:
        self._scheduler = scheduler
        self._worker_inputs = worker_inputs

    def admit(
        self, *, run_id: str, node_id: str, source_attempt_id: str,
        source_fencing_generation: int, task_text: str | None = None,
        routing_inputs: RecoveryRoutingInputs | None = None,
        accepted_at: datetime, lease_expires_at: datetime,
    ) -> RecoveryContinuationResult:
        error_code = "recovery_continuation_failed"
        try:
            if (any(type(value) is not str or re.fullmatch(_IDENTIFIER, value) is None
                    for value in (run_id, node_id, source_attempt_id))
                or type(source_fencing_generation) is not int or source_fencing_generation <= 0
                or any(not _is_data_datetime(value) for value in (accepted_at, lease_expires_at))
                or lease_expires_at <= accepted_at):
                raise RecoveryContinuationError("recovery_continuation_invalid")
            result = None

            def admit_under_lock(events, _version):
                nonlocal result
                records = load_recovery_records(self._scheduler, run_id=run_id, events=events)
                record = next((record for record in records if (
                    record.opportunity.node_id == node_id
                    and record.opportunity.source_attempt_id == source_attempt_id
                    and record.opportunity.source_fencing_generation == source_fencing_generation
                )), None)
                if record is None:
                    raise RecoveryContinuationError("recovery_continuation_invalid")
                opportunity = record.opportunity
                source = record.source_event
                existing = opportunity.successor

                def respond(status, reason, route=existing):
                    return RecoveryContinuationResult(
                        status=status, reason=reason, opportunity=opportunity, accepted_route=route,
                    )

                # Terminal sources have historical commitments, not active task authority.
                state = self._scheduler.lifecycle.replay(run_id)
                node = state.node(node_id)
                binding = source.payload.get("verification_admission")
                if task_text is not None:
                    try:
                        manifest = build_worker_input_manifest(
                            run_id=run_id, node_id=node_id, role=node.spec.role, task_text=task_text,
                        )
                    except WorkerInputError:
                        raise RecoveryContinuationError("recovery_input_invalid") from None
                    if binding is None or not hmac.compare_digest(
                        manifest.content_hash, binding["input_manifest_hash"],
                    ):
                        raise RecoveryContinuationError("recovery_input_mismatch")
                if opportunity.status not in {"ready", "already_admitted"}:
                    result = respond(opportunity.status, opportunity.reason)
                    return None
                if task_text is None:
                    result = respond("waiting_for_input", "explicit_input_required")
                    return None
                if existing is not None:
                    # Consumption is historical; only the exact current lease
                    # can still be considered an active successor. Resolve it
                    # before touching new routing observations or request IDs.
                    active = None
                    if state.status == "running" and node.attempts[-1].attempt_id == existing.attempt_id:
                        try:
                            active = self._scheduler.resolve_active_attempt(
                                run_id=run_id, node_id=node_id, attempt_id=existing.attempt_id,
                                fencing_generation=existing.fencing_generation, as_of=accepted_at,
                            )
                        except SchedulerError:
                            pass
                    if active is None:
                        result = respond("successor_inactive", "recovery_successor_inactive")
                        return None
                    if active.accepted_route != existing:
                        raise RecoveryContinuationError("recovery_continuation_failed")
                    result = respond("already_admitted", "recovery_authorization_consumed")
                    return None
                if routing_inputs is None:
                    result = respond("waiting_for_routing", "routing_inputs_required")
                    return None
                _check_declared_routing_data(routing_inputs, RecoveryRoutingInputs)
                observations = revalidate_model(RecoveryRoutingInputs, routing_inputs)
                snapshot = self._scheduler.lifecycle.config_snapshot(run_id)
                _check_current_routing_inputs(self._scheduler, run_id, snapshot, observations)
                evidence = record.classification.evidence
                authorization = record.plan.authorization
                if (evidence is None or authorization is None or binding is None
                    or record.plan.action not in {
                        "same_model_retry", "same_tier_fallback", "capability_escalation",
                    }):
                    raise RecoveryContinuationError("recovery_continuation_failed")
                digest = hashlib.sha256(json.dumps({
                    "schema": "maestro.recovery-successor.v1", "run_id": run_id,
                    "node_id": node_id, "source_attempt_id": source_attempt_id,
                    "source_fencing_generation": source_fencing_generation,
                    "authorization_hash": authorization.authorization_hash,
                }, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
                request = RoutingRequest(
                    request_id="recovery-request-" + digest, run_id=run_id, node_id=node_id,
                    attempt_id="recovery-attempt-" + digest,
                    fencing_generation=source_fencing_generation + 1,
                    config_hash=snapshot.effective_config_hash,
                    registry_hash=snapshot.registry_manifest_hash,
                    planning_contract_hash=node.spec.planning_contract_hash,
                    policy_manifest_hash=node.spec.planning_contract.policy_manifest_hash,
                    routing_as_of_event_time=accepted_at,
                    prior_decision_hash=opportunity.source_decision_hash,
                    previous_model_id=authorization.previous_model_id,
                    recovery_action=record.plan.action, recovery_authorization=authorization,
                    failure_category=evidence.failure_category, retry_level=evidence.retry_level + 1,
                    exhausted_model_ids=evidence.exhausted_model_ids,
                )
                decision = ModelRouter().route(
                    request, config=snapshot.resolved_config.config, registry=snapshot.registry_manifest,
                    contract=node.spec.planning_contract,
                    eligibility=observations.eligibility, policy_manifest=observations.policy_manifest,
                    tokenizer_snapshot=observations.tokenizer_snapshot, fx_snapshot=observations.fx_snapshot,
                )
                if decision.outcome != "selected":
                    result = respond("blocked", "routing_candidates_unavailable")
                    return None
                accepted = self._scheduler.accept_routing(
                    request, decision, accepted_at=accepted_at, lease_expires_at=lease_expires_at,
                    input_manifest_hash=manifest.content_hash,
                    verification_contract=binding["verification_contract"],
                    required_check_ids=tuple(binding["required_check_ids"]),
                )
                result = respond("admitted", "recovery_successor_admitted", accepted.accepted_route)
                # Nested Scheduler acceptance already advanced this same stream.
                return None

            self._scheduler.event_store.append_checked(
                "scheduler", "global", "recovery-continuation-snapshot", admit_under_lock,
            )
            if result is not None:
                return result
        except RecoveryContinuationError as exc:
            if type(exc) is RecoveryContinuationError:
                error_code = exc.code
        except Exception:
            pass
        # Detach validation inputs, provider details and host exceptions.
        raise RecoveryContinuationError(error_code) from None


__all__ = ["RecoveryRoutingInputs", "RecoveryContinuationResult", "RecoveryContinuationService"]
