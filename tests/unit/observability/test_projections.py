"""Read-only, version-aware projections rebuilt from the event stream."""

from datetime import datetime, timezone
import hashlib
import json

import pytest

from orchestrator.identifiers import new_id
from orchestrator.persistence.events import StoredEvent
from orchestrator.observability.projections import (
    ApprovalProjection,
    AuditProjection,
    BudgetProjection,
    CostProjection,
    ProjectionError,
    RunProjection,
)


def _hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def event(event_type, version, payload=None, *, stream_type="run", stream_id=None):
    payload = dict(payload or {})
    resolved_stream_id = (
        stream_id
        or payload.get("run_id")
        or payload.get("node_id")
        or payload.get("stream_id")
        or "run-1"
    )
    execution_context = {}
    if event_type in {
        "PolicyDecision",
        "CapabilityGrant",
        "ApprovalRequested",
        "ApprovalGrant",
        "ApprovalGranted",
        "ApprovalGrantConsumed",
        "ApprovalDenied",
        "ApprovalRevoked",
        "ApprovalExpired",
        "RoutingRequest",
        "RoutingDecision",
        "EffectIntentRecorded",
        "EffectReceiptRecorded",
    }:
        execution_context = {
            "run_id": "run-1",
            "node_id": "node-1",
            "attempt_id": "attempt-1",
            "fencing_generation": 1,
            "causation_id": "cause-1",
        }
    return StoredEvent(
        event_id=new_id(),
        stream_type=stream_type,
        stream_id=resolved_stream_id,
        stream_version=version,
        event_type=event_type,
        schema_version=1,
        occurred_at=datetime.now(timezone.utc),
        payload=payload,
        payload_hash=_hash(payload),
        idempotency_key=f"key-{version}",
        **execution_context,
    )


def test_projection_reports_event_version_and_lag():
    projection = RunProjection()
    projection.apply(event("RunCreated", version=1, payload={"run_id": "run-1"}))
    assert projection.read("run-1").event_version == 1
    assert projection.lag(current_stream_version=3, stream_id="run-1") == 2


def test_run_projection_tracks_status_transitions():
    projection = RunProjection()
    projection.apply(event("RunCreated", 1, {"run_id": "run-1"}))
    projection.apply(event("InputAccepted", 2, {"run_id": "run-1"}))
    view = projection.read("run-1")
    assert view.status == "Planning"
    assert view.event_version == 2
    assert projection.last_applied_version == 2


def test_lag_is_zero_when_projection_is_current_and_never_negative():
    projection = RunProjection()
    projection.apply(event("RunCreated", 5, {"run_id": "run-1"}))
    assert projection.lag(current_stream_version=5, stream_id="run-1") == 0
    # A stale current version must not produce a negative lag.
    assert projection.lag(current_stream_version=3, stream_id="run-1") == 0


def test_reading_an_unknown_stream_raises():
    projection = RunProjection()
    with pytest.raises(KeyError):
        projection.read("missing")


def test_budget_projection_accumulates_reserved_used_released_unknown():
    projection = BudgetProjection()
    projection.apply(
        event("BudgetReserved", 1, {"run_id": "run-1", "reserved_minor": 100, "reserved_tokens": 200},
              stream_type="budget"))
    projection.apply(
        event("CostCommitted", 2, {"run_id": "run-1", "cost_minor": 40, "total_tokens": 90},
              stream_type="budget"))
    projection.apply(
        event("BudgetReleased", 3, {"run_id": "run-1", "released_minor": 60, "released_tokens": 110},
              stream_type="budget"))
    view = projection.read("run-1")
    assert view.reserved_minor == 100
    assert view.used_minor == 40
    assert view.released_minor == 60
    assert view.event_version == 3


def test_budget_projection_marks_unknown_outcome():
    projection = BudgetProjection()
    projection.apply(
        event("BudgetReserved", 1, {"run_id": "run-1", "reserved_minor": 100, "reserved_tokens": 200},
              stream_type="budget"))
    projection.apply(
        event("OutcomeUnknown", 2, {"run_id": "run-1", "unknown_minor": 100, "unknown_tokens": 200},
              stream_type="budget"))
    view = projection.read("run-1")
    assert view.unknown_minor == 100
    assert view.unknown_tokens == 200


def test_cost_projection_sums_committed_cost():
    projection = CostProjection()
    projection.apply(event("CostCommitted", 1, {"run_id": "run-1", "cost_minor": 30}, stream_type="budget"))
    projection.apply(event("CostCommitted", 2, {"run_id": "run-1", "cost_minor": 12}, stream_type="budget"))
    projection.apply(event("CostAdjusted", 3, {"run_id": "run-1", "cost_minor": 5}, stream_type="budget"))
    view = projection.read("run-1")
    assert view.committed_minor == 47
    assert view.event_version == 3


def test_approval_projection_tracks_lifecycle():
    projection = ApprovalProjection()
    projection.apply(event("ApprovalRequested", 1, {"stream_id": "appr-1"}, stream_type="security"))
    assert projection.read("appr-1").status == "pending"
    projection.apply(event("ApprovalGrant", 2, {"stream_id": "appr-1"}, stream_type="security"))
    assert projection.read("appr-1").status == "granted"
    projection.apply(event("ApprovalGrantConsumed", 3, {"stream_id": "appr-1"}, stream_type="security"))
    assert projection.read("appr-1").status == "consumed"


def test_audit_projection_counts_and_redacts_effect_evidence():
    from orchestrator.observability.redaction import Redactor

    projection = AuditProjection(
        redactor=Redactor(secret_values=["sk-secret"], protected_paths=["/workspace/private"])
    )
    projection.apply(
        event(
            "EffectIntentRecorded",
            1,
            {
                "stream_id": "audit-1",
                "external_request_id": "req-9",
                "detail": {"token": "sk-secret", "path": "/workspace/private/x"},
            },
            stream_type="audit",
        )
    )
    view = projection.read("audit-1")
    assert view.effect_count == 1
    rendered = repr(view.last_detail) + projection.explain("audit-1")
    assert "sk-secret" not in rendered
    assert "/workspace/private" not in rendered


def test_projection_rejects_non_positive_versions():
    # StoredEvent itself forbids version <= 0, so the projection guard is
    # exercised with a malformed duck-typed event.
    from types import SimpleNamespace

    projection = RunProjection()
    malformed = SimpleNamespace(
        event_type="RunCreated", stream_version=0, stream_id="run-1", payload={"run_id": "run-1"}
    )
    with pytest.raises(ProjectionError):
        projection.apply(malformed)


def test_projection_does_not_expose_event_store_mutation():
    projection = RunProjection()
    assert not hasattr(projection, "append")
    assert not hasattr(projection, "_event_store")
