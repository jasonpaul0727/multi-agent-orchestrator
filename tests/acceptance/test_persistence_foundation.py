from datetime import datetime, timedelta, timezone

from orchestrator.artifacts import ArtifactAccessGrant, ArtifactStore
from orchestrator.budget import BudgetLedger, CostEstimate, RunLimit
from orchestrator.observability import (
    AuditProjection,
    BudgetProjection,
    CostProjection,
    RunProjection,
)
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.recovery import RecoveryBootstrap


def _context(causation_id):
    return {
        "run_id": "run-acceptance",
        "node_id": "node-effect",
        "attempt_id": "attempt-1",
        "fencing_generation": 1,
        "correlation_id": "correlation-1",
        "causation_id": causation_id,
    }


def test_persistence_foundation_acceptance_flow_recovers_interrupted_effect(tmp_path):
    database = tmp_path / "control.db"
    store = SQLiteEventStore(database)
    store.append(
        "run",
        "run-acceptance",
        0,
        [
            EventDraft("RunCreated", {"run_id": "run-acceptance"}),
            EventDraft("PlanningCompleted", {"run_id": "run-acceptance"}),
        ],
        "create-and-plan",
    )

    ledger = BudgetLedger(
        store,
        run_limits={
            "run-acceptance": RunLimit(max_cost_minor=100, max_tokens=100)
        },
    )
    reservation = ledger.reserve(
        "run-acceptance",
        CostEstimate(amount_minor=20, currency="USD", token_limit=8),
        idempotency_key="model-call-1",
        node_id="node-model",
        attempt_id="attempt-1",
        fencing_generation=1,
        correlation_id="correlation-1",
        causation_id="routing-decision-1",
    )

    artifacts = ArtifactStore(
        tmp_path / "private-artifacts",
        event_store=store,
        grant_verifier=lambda grant: grant.signature == "trusted-test-signature",
    )
    artifact = artifacts.publish_bytes(
        b"approved analysis result",
        artifact_type="analysis",
        source={
            "run_id": "run-acceptance",
            "node_id": "node-model",
            "attempt_id": "attempt-1",
        },
        readable_scope=("run-acceptance",),
        lifecycle_state="final",
    )

    run_events = store.append(
        "run",
        "run-acceptance",
        2,
        [
            EventDraft(
                "ApprovalGrantConsumed",
                {"approval_grant_id": "grant-1", "effect_id": "effect-1"},
                **_context("policy-decision-1"),
            ),
            EventDraft(
                "BudgetReserved",
                {
                    "approval_grant_id": "grant-1",
                    "reservation_id": "effect-budget-1",
                    "reserved_minor": 4,
                    "reserved_tokens": 0,
                    "run_id": "run-acceptance",
                },
                **_context("approval-consumption-1"),
            ),
            EventDraft(
                "EffectIntentRecorded",
                {
                    "effect_id": "effect-1",
                    "provider_idempotency_key": "provider-effect-1",
                    "request_hash": "request-hash-1",
                },
                **_context("effect-budget-1"),
            ),
        ],
        "approved-effect-intent",
    )
    intent = run_events[-1]

    # The process stops after the durable intent. Recovery sees the intent and
    # continues by asking the provider for the same idempotency key.
    store.close()
    restarted = SQLiteEventStore(database)
    recovered_before_receipt = RecoveryBootstrap(restarted).recover(
        "run",
        "run-acceptance",
        reducers=lambda state, event: (state or []) + [event.event_type],
    )
    assert recovered_before_receipt.state[-1] == "EffectIntentRecorded"
    assert recovered_before_receipt.event_version == 5

    provider_receipts = {}
    provider_calls = 0
    provider_key = intent.payload["provider_idempotency_key"]
    if provider_key not in provider_receipts:
        provider_calls += 1
        provider_receipts[provider_key] = "receipt-1"
    receipt = EventDraft(
        "EffectReceiptRecorded",
        {"effect_id": "effect-1", "receipt_id": provider_receipts[provider_key]},
        **_context(intent.event_id),
    )
    restarted.append(
        "run",
        "run-acceptance",
        5,
        [receipt],
        "approved-effect-receipt",
    )
    restarted.append(
        "run",
        "run-acceptance",
        6,
        [EventDraft("RunCompleted", {"run_id": "run-acceptance"})],
        "complete-run",
    )

    restarted_ledger = BudgetLedger(restarted)
    restarted_artifacts = ArtifactStore(
        tmp_path / "private-artifacts",
        event_store=restarted,
        grant_verifier=lambda access_grant: access_grant.signature
        == "trusted-test-signature",
    )
    settled = restarted_ledger.commit_usage(
        reservation.reservation_id,
        {"input_tokens": 5, "output_tokens": 2, "cost_minor": 3},
        settlement_key="model-settlement-1",
    )
    grant = ArtifactAccessGrant(
        digest=artifact.digest,
        scope=("run-acceptance",),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        issuer="test-control-plane",
        signature="trusted-test-signature",
    )

    assert provider_calls == 1
    assert settled.cost_minor == 3
    assert (
        restarted_artifacts.read_bytes(artifact.digest, grant=grant)
        == b"approved analysis result"
    )
    balance = restarted_ledger.available("run-acceptance")
    assert (balance.reserved_minor, balance.used_minor, balance.released_minor) == (0, 3, 17)

    final_events = restarted.read_stream("run", "run-acceptance")
    run_projection = RunProjection()
    audit_projection = AuditProjection()
    for event in final_events:
        run_projection.apply(event)
        audit_projection.apply(event)
    assert run_projection.read("run-acceptance").status == "Completed"
    assert audit_projection.read("run-acceptance").effect_count == 3
    assert [event.event_type for event in final_events].count("EffectReceiptRecorded") == 1

    budget_projection = BudgetProjection()
    cost_projection = CostProjection()
    budget_events = restarted.read_stream("budget", "run-acceptance")
    for event in budget_events:
        budget_projection.apply(event)
        cost_projection.apply(event)
    assert budget_projection.read("run-acceptance").used_minor == 3
    assert cost_projection.read("run-acceptance").committed_minor == 3

    recovered_final = RecoveryBootstrap(restarted).recover(
        "run",
        "run-acceptance",
        reducers=lambda state, event: (state or []) + [event.event_type],
    )
    assert recovered_final.state[-1] == "RunCompleted"
    assert recovered_final.event_version == 7
