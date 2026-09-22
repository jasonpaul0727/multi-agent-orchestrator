"""Read-only, version-aware projections rebuilt from the event stream.

Projections are derived views.  They consume :class:`StoredEvent` objects in
stream order and expose the last applied event version so a caller can tell how
far a projection lags behind the authoritative stream.  A projection must never
write to the event store or make a scheduling decision; the classes here hold
no store handle and expose no mutation surface.

The event stream is the single source of truth, so every projection can be
discarded and rebuilt by replaying events.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from .redaction import Redactor

if TYPE_CHECKING:  # pragma: no cover - typing only
    from orchestrator.persistence.events import StoredEvent


class ProjectionError(RuntimeError):
    """Raised when a projection is fed an event it cannot apply."""


class _ProjectionView(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    stream_id: str
    event_version: int


def _stream_id_of(event: "StoredEvent") -> str:
    stream_id = getattr(event, "stream_id", None)
    if stream_id:
        return str(stream_id)
    payload = getattr(event, "payload", {}) or {}
    for key in ("run_id", "node_id", "attempt_id", "stream_id"):
        value = payload.get(key)
        if value:
            return str(value)
    raise ProjectionError("event does not identify a projection stream")


class _StreamProjection:
    """Common accumulator: per-stream derived state plus version tracking."""

    view_model: type[_ProjectionView] = _ProjectionView

    def __init__(self, redactor: Redactor | None = None) -> None:
        self._redactor = redactor if redactor is not None else Redactor()
        self._state: dict[str, dict[str, Any]] = {}
        self._versions: dict[str, int] = {}
        self._last_applied_version: int = 0

    @property
    def last_applied_version(self) -> int:
        """The highest stream version this projection has applied."""

        return self._last_applied_version

    def apply(self, event: "StoredEvent") -> None:
        version = getattr(event, "stream_version", None)
        if not isinstance(version, int) or version <= 0:
            raise ProjectionError("stream_version must be a positive integer")
        stream_id = _stream_id_of(event)
        state = self._state.setdefault(stream_id, self._initial_state())
        self._reduce(state, event)
        # Versions only advance; replaying an older event never rewinds lag.
        self._versions[stream_id] = max(self._versions.get(stream_id, 0), version)
        self._last_applied_version = max(self._last_applied_version, version)

    def read(self, stream_id: str) -> _ProjectionView:
        if stream_id not in self._state:
            raise KeyError(stream_id)
        payload = dict(self._state[stream_id])
        payload["stream_id"] = stream_id
        payload["event_version"] = self._versions.get(stream_id, 0)
        return self.view_model(**payload)

    def lag(self, current_stream_version: int, stream_id: str) -> int:
        """How far behind the authoritative stream this projection is.

        A stale ``current_stream_version`` never yields a negative lag.
        """

        applied = self._versions.get(stream_id, 0)
        return max(0, current_stream_version - applied)

    # -- subclass hooks -------------------------------------------------------

    def _initial_state(self) -> dict[str, Any]:
        return {}

    def _reduce(self, state: dict[str, Any], event: "StoredEvent") -> None:
        raise NotImplementedError


# --- Run ---------------------------------------------------------------------

_RUN_STATUS_BY_EVENT = {
    "RunCreated": "Intake",
    "InputAccepted": "Planning",
    "PlanningStarted": "Planning",
    "PlanningCompleted": "Executing",
    "RunResumed": "Executing",
    "RunPaused": "Paused",
    "RunBlocked": "Blocked",
    "RunCancelled": "Cancelled",
    "RunFailed": "Failed",
    "RunCompleted": "Completed",
    "RunDelivered": "Delivered",
}


class RunView(_ProjectionView):
    status: str = "Unknown"


class RunProjection(_StreamProjection):
    """Projects Run lifecycle status keyed by run id."""

    view_model = RunView

    def _initial_state(self) -> dict[str, Any]:
        return {"status": "Unknown"}

    def _reduce(self, state: dict[str, Any], event: "StoredEvent") -> None:
        status = _RUN_STATUS_BY_EVENT.get(event.event_type)
        if status is not None:
            state["status"] = status


# --- Budget ------------------------------------------------------------------

class BudgetView(_ProjectionView):
    reserved_minor: int = 0
    reserved_tokens: int = 0
    used_minor: int = 0
    used_tokens: int = 0
    released_minor: int = 0
    released_tokens: int = 0
    unknown_minor: int = 0
    unknown_tokens: int = 0


class BudgetProjection(_StreamProjection):
    """Projects reserved / used / released / unknown totals per Run."""

    view_model = BudgetView

    def _initial_state(self) -> dict[str, Any]:
        return {
            "reserved_minor": 0,
            "reserved_tokens": 0,
            "used_minor": 0,
            "used_tokens": 0,
            "released_minor": 0,
            "released_tokens": 0,
            "unknown_minor": 0,
            "unknown_tokens": 0,
        }

    def _reduce(self, state: dict[str, Any], event: "StoredEvent") -> None:
        payload = event.payload or {}
        kind = event.event_type
        if kind == "BudgetReserved":
            state["reserved_minor"] += _int(payload, "reserved_minor", "amount_minor")
            state["reserved_tokens"] += _int(payload, "reserved_tokens", "token_limit")
        elif kind == "CostCommitted":
            # UsageObserved is raw provider evidence; counting both it and the
            # settlement event would double-count every successful call.
            state["used_minor"] += _int(payload, "cost_minor", "amount_minor", "used_minor")
            state["used_tokens"] += _int(payload, "total_tokens", "used_tokens", "tokens")
        elif kind == "BudgetReleased":
            state["released_minor"] += _int(payload, "released_minor", "amount_minor")
            state["released_tokens"] += _int(payload, "released_tokens", "tokens")
        elif kind in ("OutcomeUnknown", "AwaitingReconciliation"):
            state["unknown_minor"] += _int(payload, "unknown_minor", "amount_minor", "reserved_minor")
            state["unknown_tokens"] += _int(payload, "unknown_tokens", "reserved_tokens", "tokens")


# --- Cost --------------------------------------------------------------------

class CostView(_ProjectionView):
    committed_minor: int = 0
    committed_count: int = 0


class CostProjection(_StreamProjection):
    """Projects settled cost per Run from CostCommitted / CostAdjusted."""

    view_model = CostView

    def _initial_state(self) -> dict[str, Any]:
        return {"committed_minor": 0, "committed_count": 0}

    def _reduce(self, state: dict[str, Any], event: "StoredEvent") -> None:
        if event.event_type in ("CostCommitted", "CostAdjusted"):
            state["committed_minor"] += _int(event.payload or {}, "cost_minor", "amount_minor")
            state["committed_count"] += 1


# --- Approval ----------------------------------------------------------------

_APPROVAL_STATUS_BY_EVENT = {
    "ApprovalRequested": "pending",
    "ApprovalGrant": "granted",
    "ApprovalGranted": "granted",
    "ApprovalGrantConsumed": "consumed",
    "ApprovalDenied": "denied",
    "ApprovalRevoked": "revoked",
    "ApprovalExpired": "expired",
}


class ApprovalView(_ProjectionView):
    status: str = "unknown"


class ApprovalProjection(_StreamProjection):
    """Projects approval lifecycle status."""

    view_model = ApprovalView

    def _initial_state(self) -> dict[str, Any]:
        return {"status": "unknown"}

    def _reduce(self, state: dict[str, Any], event: "StoredEvent") -> None:
        status = _APPROVAL_STATUS_BY_EVENT.get(event.event_type)
        if status is not None:
            state["status"] = status


# --- Audit -------------------------------------------------------------------

class AuditView(_ProjectionView):
    effect_count: int = 0
    last_event_type: str | None = None
    last_detail: dict[str, Any] = Field(default_factory=dict)


class AuditProjection(_StreamProjection):
    """Projects a redacted audit trail of side-effect and security evidence."""

    view_model = AuditView

    _EFFECT_EVENTS = frozenset(
        {
            "EffectIntentRecorded",
            "EffectReceiptRecorded",
            "PolicyDecision",
            "CapabilityGrant",
            "ApprovalGrantConsumed",
        }
    )

    def _initial_state(self) -> dict[str, Any]:
        return {"effect_count": 0, "last_event_type": None, "last_detail": {}}

    def _reduce(self, state: dict[str, Any], event: "StoredEvent") -> None:
        if event.event_type not in self._EFFECT_EVENTS:
            return
        state["effect_count"] += 1
        state["last_event_type"] = event.event_type
        # Evidence payloads may carry external ids or paths; always redact.
        cleaned = self._redactor.clean(dict(event.payload or {}))
        state["last_detail"] = cleaned if isinstance(cleaned, dict) else {}

    def explain(self, stream_id: str) -> str:
        view = self.read(stream_id)
        return (
            f"audit[{stream_id}] count={view.effect_count} "
            f"last={view.last_event_type} detail={view.last_detail!r}"
        )


def _int(payload: dict[str, Any], *keys: str) -> int:
    for key in keys:
        if key in payload:
            value = payload[key]
            if isinstance(value, bool) or not isinstance(value, int):
                raise ProjectionError(f"{key} must be an integer")
            return value
    return 0


__all__ = [
    "ApprovalProjection",
    "ApprovalView",
    "AuditProjection",
    "AuditView",
    "BudgetProjection",
    "BudgetView",
    "CostProjection",
    "CostView",
    "ProjectionError",
    "RunProjection",
    "RunView",
]
