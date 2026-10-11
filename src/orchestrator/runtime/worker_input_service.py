"""Host-owned input commitments under the Scheduler's admission transaction."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from orchestrator.routing import RoutingDecision, RoutingRequest
from orchestrator.runtime.verifier_process import BUILTIN_VERIFICATION_CONTRACT_ID
from orchestrator.runtime.worker_input import WorkerInputError, build_worker_input_manifest
from orchestrator.scheduler import AcceptedAttempt, Scheduler
from orchestrator.validation import revalidate_model


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class WorkerInputService:
    """Derive inputs in memory; existing Scheduler authority owns every write."""

    def __init__(self, scheduler: Scheduler, *, clock: Callable[[], datetime] = _utc_now) -> None:
        self._scheduler = scheduler
        self._clock = clock

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
