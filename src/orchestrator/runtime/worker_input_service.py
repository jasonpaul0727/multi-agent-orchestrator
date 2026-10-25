"""Host-owned input admission and task preparation under Scheduler serialization."""

from __future__ import annotations

from datetime import datetime, timezone
import hmac
import re
from typing import Callable

from orchestrator.routing import RoutingDecision, RoutingRequest
from orchestrator.runtime.verifier_process import BUILTIN_VERIFICATION_CONTRACT_ID
from orchestrator.runtime.contracts import AttemptContext, WorkerTask
from orchestrator.runtime.worker_input import (
    MAX_WORKER_INPUT_FRAME_BYTES, WORKER_INPUT_OUTPUT_BYTES,
    WorkerInputError, build_worker_input_manifest,
)
from orchestrator.scheduler import AcceptedAttempt, Scheduler
from orchestrator.validation import revalidate_model


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}", re.ASCII)


class WorkerInputService:
    """Derive inputs in memory; existing Scheduler authority owns every write."""

    def __init__(self, scheduler: Scheduler, *, clock: Callable[[], datetime] = _utc_now) -> None:
        self._scheduler = scheduler
        self._clock = clock

    def _now(self) -> datetime:
        now = self._clock()
        if (not isinstance(now, datetime) or now.tzinfo is None
            or now.utcoffset() != timezone.utc.utcoffset(now)):
            raise WorkerInputError("worker_input_preparation_failed")
        return now

    def prepare_worker_task(
        self, *, run_id: str, node_id: str, attempt_id: str,
        fencing_generation: int, task_text: str,
    ) -> WorkerTask:
        """Return bounded data at a serialized validity point, without execution."""
        return WorkerInputService._prepare_worker_task(
            self, run_id=run_id, node_id=node_id, attempt_id=attempt_id,
            fencing_generation=fencing_generation, task_text=task_text, recovery=False,
        )

    def _prepare_recovery_worker_task(
        self, *, run_id: str, node_id: str, attempt_id: str,
        fencing_generation: int, task_text: str,
    ) -> WorkerTask:
        """Owned recovery path; sample standard-library UTC only after locking."""
        return WorkerInputService._prepare_worker_task(
            self, run_id=run_id, node_id=node_id, attempt_id=attempt_id,
            fencing_generation=fencing_generation, task_text=task_text, recovery=True,
        )

    def _prepare_worker_task(
        self, *, run_id: str, node_id: str, attempt_id: str,
        fencing_generation: int, task_text: str, recovery: bool, general: bool = False,
    ) -> WorkerTask:
        """Share all binding/input/context/frame validation between both paths."""
        error_code = "worker_input_preparation_failed"
        try:
            if (any((type(value) is not str if recovery else not isinstance(value, str))
                    or _IDENTIFIER.fullmatch(value) is None
                    for value in (run_id, node_id, attempt_id))
                or type(fencing_generation) is not int or fencing_generation <= 0):
                raise WorkerInputError("worker_input_invalid")
            identity = dict(run_id=run_id, node_id=node_id, attempt_id=attempt_id,
                            fencing_generation=fencing_generation)
            result = None

            def prepare_under_lock(_events, _version):
                nonlocal result
                now = datetime.now(timezone.utc) if recovery else self._now()
                resolver = (self._scheduler._resolve_general_verification_binding if general
                    else self._scheduler.resolve_verification_binding)
                binding = resolver(**identity, as_of=now)
                context = revalidate_model(AttemptContext, binding.context)
                state = self._scheduler.lifecycle.replay(run_id)
                node = state.node(node_id)
                contract = node.spec.planning_contract
                if contract is None or (
                    context.run_id != run_id or context.node_id != node_id
                    or context.attempt_id != attempt_id
                    or context.fencing_generation != fencing_generation
                    or context.agent_instance_id != binding.active_attempt.agent_instance_id
                    or context.graph_version != state.graph_version
                    or context.effective_config_hash != state.config_hash
                    or context.registry_hash != state.registry_hash
                    or context.policy_manifest_hash != state.policy_manifest_hash
                    or contract.run_id != run_id or contract.node_id != node_id
                    or contract.role != node.spec.role
                    or contract.config_hash != context.effective_config_hash
                    or contract.registry_hash != context.registry_hash
                    or contract.policy_manifest_hash != context.policy_manifest_hash
                    or contract.contract_hash != node.spec.planning_contract_hash
                    or contract.contract_hash != context.planning_contract_hash
                ):
                    raise WorkerInputError("worker_input_preparation_failed")
                manifest = build_worker_input_manifest(
                    run_id=run_id, node_id=node_id, role=node.spec.role, task_text=task_text,
                )
                if not hmac.compare_digest(manifest.content_hash, context.input_manifest_hash):
                    raise WorkerInputError("worker_input_mismatch")
                result = WorkerTask(
                    context=context, role=node.spec.role, task_text=task_text,
                    input_artifacts=(), tool_capabilities=(),
                    output_byte_limit=WORKER_INPUT_OUTPUT_BYTES,
                )
                encoded = result.model_dump_json().encode("utf-8", errors="strict")
                if len(encoded) > MAX_WORKER_INPUT_FRAME_BYTES:
                    raise WorkerInputError("worker_input_too_large")
                return None

            self._scheduler.event_store.append_checked(
                "scheduler", "global", "worker-input-recovery-preparation-snapshot"
                if recovery else "worker-input-preparation-snapshot", prepare_under_lock,
            )
            if result is None:
                raise WorkerInputError("worker_input_preparation_failed")
            return result
        except WorkerInputError as exc:
            if type(exc) is WorkerInputError and type(vars(exc).get("code")) is str:
                for safe_code in ("worker_input_invalid", "worker_input_too_large", "worker_input_mismatch"):
                    if vars(exc).get("code") == safe_code:
                        error_code = safe_code
        except Exception:
            pass
        # Detach owned errors, including validation inputs and opaque codes.
        raise WorkerInputError(error_code) from None

    def _prepare_general_worker_task(
        self, *, run_id: str, node_id: str, attempt_id: str,
        fencing_generation: int, task_text: str,
    ) -> WorkerTask:
        """Use owned metadata-only recovery and locked standard-library UTC."""
        return WorkerInputService._prepare_worker_task(
            self, run_id=run_id, node_id=node_id, attempt_id=attempt_id,
            fencing_generation=fencing_generation, task_text=task_text, recovery=True, general=True,
        )

    def accept_routing_with_input(
        self, request: RoutingRequest, decision: RoutingDecision, *, task_text: str,
        accepted_at: datetime, lease_expires_at: datetime,
    ) -> AcceptedAttempt:
        error_code = "worker_input_admission_failed"
        try:
            request = revalidate_model(RoutingRequest, request)
            decision = revalidate_model(RoutingDecision, decision)
            result = None

            def admit_under_lock(_events, _version):
                nonlocal result
                state = self._scheduler.lifecycle.replay(request.run_id)
                node = state.node(request.node_id)
                contract = node.spec.planning_contract
                if contract is None or (
                    contract.run_id != request.run_id
                    or contract.node_id != request.node_id
                    or contract.role != node.spec.role
                    or contract.contract_hash != node.spec.planning_contract_hash
                    or contract.contract_hash != request.planning_contract_hash
                    or contract.contract_hash != decision.planning_contract_hash
                ):
                    raise WorkerInputError("worker_input_admission_failed")
                manifest = build_worker_input_manifest(
                    run_id=request.run_id, node_id=request.node_id,
                    role=node.spec.role, task_text=task_text,
                )
                result = self._scheduler.accept_routing(
                    request, decision, accepted_at=accepted_at, lease_expires_at=lease_expires_at,
                    input_manifest_hash=manifest.content_hash,
                    verification_contract=BUILTIN_VERIFICATION_CONTRACT_ID,
                    required_check_ids=("artifact-integrity",),
                )
                return None

            self._scheduler.event_store.append_checked(
                "scheduler", "global", "worker-input-admission-snapshot", admit_under_lock,
            )
            if result is None:
                raise WorkerInputError("worker_input_admission_failed")
            return result
        except WorkerInputError as exc:
            if type(exc) is WorkerInputError and type(vars(exc).get("code")) is str:
                for safe_code in ("worker_input_invalid", "worker_input_too_large"):
                    if vars(exc).get("code") == safe_code:
                        error_code = safe_code
        except Exception:
            pass
        # Keep only a trusted code; no caught exception is linked to the result.
        raise WorkerInputError(error_code) from None


__all__ = ["WorkerInputService"]
