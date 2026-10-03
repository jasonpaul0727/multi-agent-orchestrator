"""Trusted control-plane composition and atomic startup recovery.

Construction completes durable recovery before exposing admission. This host
API does not dispatch Workers, tools, or Provider requests during startup.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from orchestrator.artifacts import ArtifactStore
from orchestrator.models import AcceptedModelRoute
from orchestrator.models.provider_calls import ProviderCallSnapshot, SQLiteProviderCallJournal
from orchestrator.persistence import SQLiteEventStore
from orchestrator.provider_reconciliation import (
    AttemptTerminationVerifier,
    ProviderEvidenceVerifier,
    ProviderReconciliationService,
)
from orchestrator.recovery import RecoveredRun
from orchestrator.routing import RoutingDecision, RoutingRequest
from orchestrator.scheduler import AcceptedAttempt, ConcurrencyLimits, Scheduler
from orchestrator.validation import revalidate_model


class ApplicationNotReady(RuntimeError):
    """A closed or incompletely recovered application cannot authorize work."""

    code = "application_not_ready"


class StartupRecoveryFailed(RuntimeError):
    """Startup rejected durable state; no admission handle was returned."""

    code = "startup_recovery_failed"


@dataclass(frozen=True, slots=True)
class StartupRecoveryReport:
    """Metadata from the committed startup snapshot, not replay authority."""

    run_ids: tuple[str, ...]
    initializing_run_ids: tuple[str, ...]
    applied_provider_calls: tuple[str, ...]
    unresolved_provider_calls: tuple[str, ...]
    held_attempts: int
    unknown_attempts: int


class ControlPlaneApplication:
    """Own one SQLite connection shared by Scheduler, journal, and recovery.

    ``limits`` is the trusted host concurrency envelope. Artifact verification
    uses the supplied private ``artifact_root`` on the same event connection;
    published artifacts without an available store fail recovery closed.
    Provider verifiers are used only by explicit host reconciliation calls.
    """

    def __init__(
        self,
        database: str | Path,
        *,
        limits: ConcurrencyLimits,
        artifact_root: str | Path | None = None,
        evidence_verifier: ProviderEvidenceVerifier | None = None,
        termination_verifier: AttemptTerminationVerifier | None = None,
    ) -> None:
        self._ready = False
        self._closed = False
        store = None
        try:
            limits = revalidate_model(ConcurrencyLimits, limits)
            store = SQLiteEventStore(database)
            self._event_store = store
            artifacts = ArtifactStore(artifact_root, event_store=store) if artifact_root is not None else None
            self._scheduler = Scheduler(store, limits=limits, artifact_store=artifacts)
            self._journal = SQLiteProviderCallJournal(store)
            self._reconciliation = ProviderReconciliationService(
                journal=self._journal,
                scheduler=self._scheduler,
                evidence_verifier=evidence_verifier,
                termination_verifier=termination_verifier,
            )
            # Assign readiness only after append_checked has committed.
            self._report = self._bootstrap()
            self._ready = True
        except Exception:
            if store is not None:
                store.close()
            self._closed = True
            raise StartupRecoveryFailed("control-plane startup recovery failed") from None

    @property
    def startup_report(self) -> StartupRecoveryReport:
        return self._report

    def _bootstrap(self) -> StartupRecoveryReport:
        report = None

        def recover_under_lock(_events, _version):
            nonlocal report
            before = self._recover_all_runs()
            self._validate_provider_calls(before)
            applied = self._reconciliation.apply_pending_settlements()
            after = self._recover_all_runs()
            self._validate_provider_calls(after)
            if self._journal.pending_settlements():
                raise StartupRecoveryFailed("startup left an unapplied Provider proof")
            held = tuple(lease for run in after.values() for lease in run.active_attempts)
            report = StartupRecoveryReport(
                run_ids=tuple(sorted(after)),
                initializing_run_ids=tuple(sorted(set(self._event_store.stream_ids("run")) - after.keys())),
                applied_provider_calls=applied,
                unresolved_provider_calls=tuple(call.stream_id for call in self._journal.unresolved()),
                held_attempts=len(held),
                unknown_attempts=sum(lease.status == "outcome_unknown" for lease in held),
            )
            return None

        # Nested settlement appends share this BEGIN IMMEDIATE transaction.
        # Returning None appends no synthetic readiness event; readiness is a
        # process-local gate set only after the database commit returns.
        self._event_store.append_checked(
            "application_bootstrap", "global", "startup-recovery", recover_under_lock
        )
        if report is None:
            raise StartupRecoveryFailed("startup produced no recovery report")
        return report

    def _recover_all_runs(self) -> dict[str, RecoveredRun]:
        run_ids = set(self._event_store.stream_ids("run_lifecycle"))
        config_ids = set(self._event_store.stream_ids("run"))
        if run_ids - config_ids:
            raise StartupRecoveryFailed("lifecycle Run has no frozen config")
        for run_id in sorted(config_ids):
            self._scheduler.lifecycle.config_snapshot(run_id)
        # A frozen config may precede lifecycle initialization after a crash;
        # it is reported separately. Agent/budget state cannot exist in that
        # window because admission requires an initialized lifecycle Run.
        for stream_type in ("agent_registry", "budget"):
            if set(self._event_store.stream_ids(stream_type)) - run_ids:
                raise StartupRecoveryFailed("control-plane projection has no lifecycle Run")
        return {
            run_id: self._scheduler.recovery.recover(run_id)
            for run_id in sorted(run_ids)
        }

    def _validate_provider_calls(self, runs: dict[str, RecoveredRun]) -> None:
        routes = {}
        for event in self._event_store.read_stream("scheduler", "global"):
            if event.event_type != "RoutingDecisionAccepted":
                continue
            route = AcceptedModelRoute.model_validate(event.payload["accepted_route"])
            if route.run_id not in runs:
                raise StartupRecoveryFailed("accepted route has no recovered Run")
            routes[(route.run_id, route.node_id, route.attempt_id, route.fencing_generation)] = route
        for stream_id in self._event_store.stream_ids("provider_call"):
            call = self._journal.read_call(stream_id)
            if call is None:
                raise StartupRecoveryFailed("Provider intent disappeared during recovery")
            route = routes.get((call.run_id, call.node_id, call.attempt_id, call.fencing_generation))
            if route is None or any(
                getattr(call, call_name) != getattr(route, route_name)
                for call_name, route_name in (
                    ("accepted_route_id", "decision_id"),
                    ("budget_reservation_id", "budget_reservation_id"),
                    ("registry_manifest_hash", "registry_manifest_hash"),
                    ("provider_id", "provider_id"),
                    ("model_id", "model_id"),
                )
            ):
                raise StartupRecoveryFailed("Provider call does not bind an accepted route")
            config = self._scheduler.lifecycle.config_snapshot(call.run_id)
            provider = next(p for p in config.registry_manifest.providers if p.id == call.provider_id)
            if provider.adapter != call.provider_adapter:
                raise StartupRecoveryFailed("Provider adapter differs from the frozen Registry")
            node = runs[call.run_id].lifecycle.node(call.node_id)
            attempt = next(a for a in node.attempts if a.attempt_id == call.attempt_id)
            if call.status in {"dispatching", "unknown"} and attempt.status not in {"accepted", "outcome_unknown"}:
                raise StartupRecoveryFailed("terminal Attempt has an unresolved Provider call")
            if call.settlement_applied:
                if attempt.status != "failed" or call.reconciliation is None or call.reconciled_at is None:
                    raise StartupRecoveryFailed("Provider settlement marker has no failed Attempt")
                # The public Scheduler operation verifies the exact durable
                # idempotency payload. A matching applied settlement is a no-op;
                # another terminal outcome or different usage is rejected.
                proof = call.reconciliation
                self._scheduler.reconcile_attempt(
                    run_id=call.run_id, node_id=call.node_id, attempt_id=call.attempt_id,
                    fencing_generation=call.fencing_generation, reconciled_at=call.reconciled_at,
                    outcome="failed", usage=proof.usage, known_no_effect=proof.effect == "not_received",
                )

    def _require_ready(self) -> None:
        if not self._ready or self._closed:
            raise ApplicationNotReady("control-plane application is not ready")

    def accept_routing(
        self, request: RoutingRequest, decision: RoutingDecision, *,
        accepted_at: datetime, lease_expires_at: datetime,
    ) -> AcceptedAttempt:
        self._require_ready()
        return self._scheduler.accept_routing(
            request, decision, accepted_at=accepted_at, lease_expires_at=lease_expires_at
        )

    def recover_run(self, run_id: str) -> RecoveredRun:
        self._require_ready()
        return self._scheduler.recovery.recover(run_id)

    def reconcile_provider_call(
        self, stream_id: str, *, raw_evidence: bytes,
        termination_receipt: object, reconciled_at: datetime,
    ) -> ProviderCallSnapshot:
        self._require_ready()
        return self._reconciliation.reconcile(
            stream_id, raw_evidence=raw_evidence,
            termination_receipt=termination_receipt, reconciled_at=reconciled_at,
        )

    def close(self) -> None:
        if not self._closed:
            self._ready = False
            self._closed = True
            self._event_store.close()

    def __enter__(self) -> ControlPlaneApplication:
        self._require_ready()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()


__all__ = ["ApplicationNotReady", "ControlPlaneApplication", "StartupRecoveryFailed", "StartupRecoveryReport"]
