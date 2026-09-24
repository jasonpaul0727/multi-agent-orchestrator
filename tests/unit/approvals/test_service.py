from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import threading

import pytest

from orchestrator.approvals import (
    ApprovalAlreadyConsumed,
    ApprovalError,
    ApprovalExpired,
    ApprovalInvalid,
    ApprovalPolicyState,
    ApprovalPrincipal,
    ApprovalRequest,
    ApprovalService,
    EffectIntentSpec,
    ExecutionAttempt,
)
from orchestrator.budget import BudgetLedger, CostEstimate, RunLimit
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore


_NOW = datetime(2026, 9, 23, 18, 0, tzinfo=timezone.utc)


def _hash(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _intent() -> EffectIntentSpec:
    return EffectIntentSpec(
        effect_id="effect-1",
        target_hash=_hash("target"),
        parameters_hash=_hash("parameters"),
        provider_idempotency_key="vendor-key-1",
        maximum_cost_minor=20,
        recovery_class="queryable",
    )


def _request(intent: EffectIntentSpec, *, expires_at: datetime | None = None) -> ApprovalRequest:
    return ApprovalRequest(
        approval_request_id="approval-1",
        run_id="run-1",
        node_id="node-1",
        requester_id="worker-principal",
        origin_attempt_id="attempt-1",
        origin_fencing_generation=1,
        causation_id="approval-required-event",
        action_category="external_mutation",
        tool_id="provider.publish",
        target_hash=intent.target_hash,
        parameters_hash=intent.parameters_hash,
        effect_intent_hash=intent.content_hash,
        policy_manifest_hash=_hash("policy"),
        revocation_version=3,
        emergency_deny_version=2,
        expires_at=expires_at or _NOW + timedelta(hours=1),
    )


class _AttemptAuthority:
    def __init__(self, attempt_id: str = "attempt-2") -> None:
        self.attempt_id = attempt_id

    def is_current(self, attempt: ExecutionAttempt) -> bool:
        return attempt.attempt_id == self.attempt_id and attempt.fencing_generation == 2


class _Fixture:
    def __init__(self, path: Path, *, now: list[datetime] | None = None, state: ApprovalPolicyState | None = None) -> None:
        self.store = SQLiteEventStore(path)
        self.ledger = BudgetLedger(
            self.store,
            run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
        )
        self.now = now or [_NOW]
        self.state = state or ApprovalPolicyState(3, 2)
        self.service = self.make_service()

    def make_service(self) -> ApprovalService:
        return ApprovalService(
            event_store=self.store,
            budget_ledger=self.ledger,
            authenticator=lambda credential: ApprovalPrincipal(
                principal_id=credential,
                principal_type="human",
                permissions=frozenset({"approval:approve", "approval:deny", "approval:revoke"}),
            ),
            attempt_authority=_AttemptAuthority(),
            policy_state=lambda _request: self.state,
            now=lambda: self.now[0],
        )


def _attempt() -> ExecutionAttempt:
    return ExecutionAttempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-2",
        fencing_generation=2,
        isolation_profile_hash=_hash("isolation-profile"),
    )


def _estimate(amount: int = 7) -> CostEstimate:
    return CostEstimate(amount_minor=amount, currency="USD", token_limit=10, snapshot_id="estimate-v1")


def _approved_bound_fixture(tmp_path: Path) -> tuple[_Fixture, EffectIntentSpec, ExecutionAttempt, str]:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    fixture.service.create_request(_request(intent))
    issued = fixture.service.approve("approval-1", "human-1", reason="user authorized the exact publish")
    attempt = _attempt()
    fixture.service.bind_to_attempt(issued.approval_grant_id, attempt)
    return fixture, intent, attempt, issued.approval_grant_id


def test_approval_binds_new_attempt_and_consumes_with_intent_and_budget_atomically(tmp_path: Path) -> None:
    fixture, intent, attempt, grant_id = _approved_bound_fixture(tmp_path)

    consumed = fixture.service.consume_and_intend(grant_id, attempt, intent, _estimate())

    assert consumed.approval_request_id == "approval-1"
    assert consumed.effect_id == intent.effect_id
    assert consumed.reservation.reserved_minor == 7
    security = fixture.store.read_stream("security", "run-1")
    budget = fixture.store.read_stream("budget", "run-1")
    assert [event.event_type for event in security] == [
        "ApprovalRequested",
        "ApprovalGranted",
        "ApprovalGrant",
        "ApprovalGrantBound",
    ]
    assert [event.event_type for event in budget] == [
        "EffectIntentRecorded",
        "ApprovalGrantConsumed",
        "BudgetReserved",
    ]
    assert budget[0].payload["approval_grant_id"] == grant_id
    assert budget[0].payload["provider_idempotency_key_hash"] == _hash("vendor-key-1")
    assert "vendor-key-1" not in repr([event.payload for event in security + budget])


def test_grant_cannot_resume_origin_attempt_or_bind_twice(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    fixture.service.create_request(_request(intent))
    issued = fixture.service.approve("approval-1", "human-1", reason="approved")
    origin_attempt = ExecutionAttempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=1,
        isolation_profile_hash=_hash("isolation-profile"),
    )
    with pytest.raises(ApprovalInvalid, match="originating"):
        fixture.service.bind_to_attempt(issued.approval_grant_id, origin_attempt)

    fixture.service.bind_to_attempt(issued.approval_grant_id, _attempt())
    with pytest.raises(ApprovalInvalid, match="already bound"):
        fixture.service.bind_to_attempt(issued.approval_grant_id, _attempt())


def test_scope_change_cost_overrun_and_second_consumption_are_rejected(tmp_path: Path) -> None:
    fixture, intent, attempt, grant_id = _approved_bound_fixture(tmp_path)
    changed = intent.model_copy(update={"provider_idempotency_key": "different-key"})
    with pytest.raises(ApprovalInvalid, match="differ from the approved intent"):
        fixture.service.consume_and_intend(grant_id, attempt, changed, _estimate())
    with pytest.raises(ApprovalInvalid, match="maximum cost"):
        fixture.service.consume_and_intend(grant_id, attempt, intent, _estimate(21))

    fixture.service.consume_and_intend(grant_id, attempt, intent, _estimate())
    with pytest.raises(ApprovalAlreadyConsumed):
        fixture.service.consume_and_intend(grant_id, attempt, intent, _estimate())


def test_effect_receipt_is_attempt_bound_and_idempotent(tmp_path: Path) -> None:
    fixture, intent, attempt, grant_id = _approved_bound_fixture(tmp_path)
    consumed = fixture.service.consume_and_intend(grant_id, attempt, intent, _estimate())

    receipt_id = fixture.service.record_effect_receipt(
        consumed,
        attempt,
        outcome="applied",
        receipt_hash=_hash("provider-receipt"),
    )
    assert fixture.service.record_effect_receipt(
        consumed,
        attempt,
        outcome="applied",
        receipt_hash=_hash("provider-receipt"),
    ) == receipt_id
    assert fixture.store.read_stream("budget", "run-1")[-1].event_type == "EffectReceiptRecorded"


def test_expired_request_is_audited_and_cannot_be_approved(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    fixture.service.create_request(_request(intent, expires_at=_NOW + timedelta(seconds=1)))
    fixture.now[0] = _NOW + timedelta(seconds=2)

    with pytest.raises(ApprovalExpired):
        fixture.service.approve("approval-1", "human-1", reason="too late")

    events = fixture.store.read_stream("security", "run-1")
    assert events[-1].event_type == "ApprovalExpired"
    assert not any(event.event_type == "ApprovalGrant" for event in events)


def test_stale_policy_versions_revoke_instead_of_issuing_grant(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db", state=ApprovalPolicyState(3, 2))
    intent = _intent()
    fixture.service.create_request(_request(intent))
    fixture.state = ApprovalPolicyState(4, 2)

    with pytest.raises(ApprovalInvalid, match="versions changed"):
        fixture.service.approve("approval-1", "human-1", reason="stale")

    assert fixture.store.read_stream("security", "run-1")[-1].event_type == "ApprovalRevoked"


def test_missing_approver_scope_is_denied_and_requester_payload_is_hash_only(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    request = _request(intent)
    fixture.service.create_request(request)

    def weak_auth(_credential: str):
        return ApprovalPrincipal(
            principal_id="agent-1",
            principal_type="service",
            permissions=frozenset({"approval:read"}),
        )

    weak = ApprovalService(
        event_store=fixture.store,
        budget_ledger=fixture.ledger,
        authenticator=weak_auth,
        attempt_authority=_AttemptAuthority(),
        policy_state=lambda _request: fixture.state,
        now=lambda: fixture.now[0],
    )
    with pytest.raises(ApprovalInvalid, match="lacks"):
        weak.approve("approval-1", "agent-token", reason="attempt")

    payload = fixture.store.read_stream("security", "run-1")[0].payload
    assert payload["request_hash"] == request.content_hash
    assert "vendor-key-1" not in repr(payload)
    assert payload["approval_request"]["parameters_hash"] == request.parameters_hash


def test_denial_and_revocation_are_terminal_and_audited(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    fixture.service.create_request(_request(intent))
    fixture.service.deny("approval-1", "human-1", reason="user denied")
    with pytest.raises(ApprovalInvalid, match="already denied"):
        fixture.service.approve("approval-1", "human-1", reason="later")

    other = _Fixture(tmp_path / "revoke.db")
    other.service.create_request(_request(intent))
    issued = other.service.approve("approval-1", "human-1", reason="approved")
    other.service.revoke(issued.approval_grant_id, "human-1", reason="user changed mind")
    with pytest.raises(ApprovalInvalid, match="revoked"):
        other.service.bind_to_attempt(issued.approval_grant_id, _attempt())


def test_concurrent_consume_calls_commit_exactly_one_effect_intent(tmp_path: Path) -> None:
    fixture, intent, attempt, grant_id = _approved_bound_fixture(tmp_path)
    fixture.service  # Complete request/approval/binding on the seed connection.
    database = tmp_path / "approvals.db"
    barrier = threading.Barrier(2)

    def consume() -> str:
        store = SQLiteEventStore(database)
        ledger = BudgetLedger(
            store,
            run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
        )
        service = ApprovalService(
            event_store=store,
            budget_ledger=ledger,
            authenticator=lambda credential: ApprovalPrincipal(
                principal_id=credential,
                principal_type="human",
                permissions=frozenset({"approval:approve"}),
            ),
            attempt_authority=_AttemptAuthority(),
            policy_state=lambda _request: ApprovalPolicyState(3, 2),
            now=lambda: _NOW,
        )
        barrier.wait()
        try:
            service.consume_and_intend(grant_id, attempt, intent, _estimate())
            return "consumed"
        except ApprovalAlreadyConsumed:
            return "rejected"
        except ApprovalError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _index: consume(), range(2)))

    assert sorted(outcomes) == ["consumed", "rejected"]
    budget = fixture.store.read_stream("budget", "run-1")
    assert sum(event.event_type == "ApprovalGrantConsumed" for event in budget) == 1
    assert sum(event.event_type == "EffectIntentRecorded" for event in budget) == 1


def test_approval_models_reject_untrusted_or_unbounded_scope_values() -> None:
    intent = _intent()
    with pytest.raises(ValueError, match="sha256"):
        _request(intent).model_copy(update={"target_hash": "not-a-hash"}).model_validate(
            _request(intent).model_copy(update={"target_hash": "not-a-hash"}).model_dump()
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        _request(intent, expires_at=datetime(2026, 9, 23, 18, 0))
    with pytest.raises(ValueError, match="permissions must be an array"):
        ApprovalPrincipal(principal_id="approver", principal_type="human", permissions="approval:approve")
    with pytest.raises(ValueError, match="non-negative integers"):
        ApprovalPolicyState(True, 0)
    with pytest.raises(ValueError, match="sha256"):
        ExecutionAttempt(
            run_id="run-1", node_id="node-1", attempt_id="attempt-2",
            fencing_generation=2, isolation_profile_hash="missing",
        )


def test_request_is_idempotent_but_immutable_and_expiry_is_bounded(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    request = _request(intent)
    original_hash = fixture.service.create_request(request)
    assert fixture.service.create_request(request) == original_hash
    changed = request.model_copy(update={"requester_id": "other-worker"})
    with pytest.raises(ApprovalInvalid, match="reused with different scope"):
        fixture.service.create_request(changed)
    with pytest.raises(ApprovalExpired, match="next 24 hours"):
        fixture.service.create_request(
            _request(intent, expires_at=_NOW + timedelta(hours=25)).model_copy(
                update={"approval_request_id": "approval-too-long"}
            )
        )


def test_approval_authentication_replay_and_reason_validation_fail_closed(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    fixture.service.create_request(_request(intent))
    with pytest.raises(ApprovalInvalid, match="authority is required"):
        fixture.service.approve("approval-1", "", reason="approved")
    with pytest.raises(ApprovalInvalid, match="authentication failed"):
        broken = ApprovalService(
            event_store=fixture.store,
            budget_ledger=fixture.ledger,
            authenticator=lambda _credential: (_ for _ in ()).throw(RuntimeError("auth offline")),
            attempt_authority=_AttemptAuthority(),
            policy_state=lambda _request: fixture.state,
        )
        broken.approve("approval-1", "credential", reason="approved")
    with pytest.raises(ApprovalInvalid, match="bounded non-blank"):
        fixture.service.approve("approval-1", "human-1", reason="  ")
    first = fixture.service.approve("approval-1", "human-1", reason="approved")
    assert fixture.service.approve("approval-1", "human-1", reason="same decision").approval_grant_id == first.approval_grant_id
    with pytest.raises(ApprovalInvalid, match="does not exist"):
        fixture.service.approve("missing", "human-1", reason="approved")


def test_decision_revoke_and_attempt_scope_boundaries_are_enforced(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    fixture.service.create_request(_request(intent))
    fixture.service.deny("approval-1", "human-1", reason="denied")
    with pytest.raises(ApprovalInvalid, match="terminal decision"):
        fixture.service.deny("approval-1", "human-1", reason="again")

    active = _Fixture(tmp_path / "active.db")
    active.service.create_request(_request(intent))
    issued = active.service.approve("approval-1", "human-1", reason="approved")
    with pytest.raises(ApprovalInvalid, match="Run/Node"):
        active.service.bind_to_attempt(issued.approval_grant_id, _attempt().model_copy(update={"node_id": "node-2"}))
    with pytest.raises(ApprovalInvalid, match="fencing generation"):
        active.service.bind_to_attempt(issued.approval_grant_id, _attempt().model_copy(update={"fencing_generation": 1}))
    active.service.bind_to_attempt(issued.approval_grant_id, _attempt())
    active.service.revoke(issued.approval_grant_id, "human-1", reason="revoke")
    active.service.revoke(issued.approval_grant_id, "human-1", reason="idempotent revoke")
    with pytest.raises(ApprovalInvalid, match="revoked"):
        active.service.consume_and_intend(issued.approval_grant_id, _attempt(), intent, _estimate())


def test_binding_and_consumption_expiry_policy_and_attempt_authority_fail_closed(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    intent = _intent()
    fixture.service.create_request(_request(intent, expires_at=_NOW + timedelta(seconds=1)))
    issued = fixture.service.approve("approval-1", "human-1", reason="approved")
    fixture.now[0] += timedelta(seconds=2)
    with pytest.raises(ApprovalExpired):
        fixture.service.bind_to_attempt(issued.approval_grant_id, _attempt())

    stale = _Fixture(tmp_path / "stale.db")
    stale.service.create_request(_request(intent))
    stale_issued = stale.service.approve("approval-1", "human-1", reason="approved")
    stale.state = ApprovalPolicyState(4, 2)
    with pytest.raises(ApprovalInvalid, match="stale"):
        stale.service.bind_to_attempt(stale_issued.approval_grant_id, _attempt())
    assert stale.store.read_stream("security", "run-1")[-1].event_type == "ApprovalRevoked"

    unavailable = _Fixture(tmp_path / "unavailable.db")
    unavailable.service.create_request(_request(intent))
    missing_binding = unavailable.service.approve("approval-1", "human-1", reason="approved")
    with pytest.raises(ApprovalInvalid, match="not been bound"):
        unavailable.service.consume_and_intend(missing_binding.approval_grant_id, _attempt(), intent, _estimate())


def test_attempt_authority_is_rechecked_inside_atomic_consumption(tmp_path: Path) -> None:
    fixture, intent, attempt, grant_id = _approved_bound_fixture(tmp_path)

    class ExpiresAfterPreflight:
        checks = 0

        def is_current(self, _attempt: ExecutionAttempt) -> bool:
            self.checks += 1
            return self.checks == 1

    authority = ExpiresAfterPreflight()
    service = ApprovalService(
        event_store=fixture.store,
        budget_ledger=fixture.ledger,
        authenticator=lambda credential: ApprovalPrincipal(
            principal_id=credential,
            principal_type="human",
            permissions=frozenset({"approval:approve"}),
        ),
        attempt_authority=authority,
        policy_state=lambda _request: fixture.state,
        now=lambda: fixture.now[0],
    )
    with pytest.raises(ApprovalInvalid, match="authority changed"):
        service.consume_and_intend(grant_id, attempt, intent, _estimate())
    assert authority.checks == 2
    assert fixture.store.read_stream("budget", "run-1") == []


def test_receipt_validation_mismatch_and_replay_with_changed_result_are_rejected(tmp_path: Path) -> None:
    fixture, intent, attempt, grant_id = _approved_bound_fixture(tmp_path)
    consumed = fixture.service.consume_and_intend(grant_id, attempt, intent, _estimate())
    with pytest.raises(ValueError, match="sha256"):
        fixture.service.record_effect_receipt(consumed, attempt, outcome="applied", receipt_hash="bad")
    with pytest.raises(ValueError, match="outcome"):
        fixture.service.record_effect_receipt(consumed, attempt, outcome="maybe", receipt_hash=_hash("receipt"))
    with pytest.raises(ApprovalInvalid, match="does not match"):
        fixture.service.record_effect_receipt(consumed, attempt.model_copy(update={"attempt_id": "attempt-3"}), outcome="applied", receipt_hash=_hash("receipt"))
    fixture.service.record_effect_receipt(consumed, attempt, outcome="applied", receipt_hash=_hash("receipt"))
    with pytest.raises(ApprovalInvalid, match="different result"):
        fixture.service.record_effect_receipt(consumed, attempt, outcome="not_applied", receipt_hash=_hash("different"))


def test_lookup_rejects_invalid_and_missing_ids_and_request_ids_are_global(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    with pytest.raises(ApprovalInvalid, match="invalid"):
        fixture.service.approve("bad id", "human-1", reason="approved")
    with pytest.raises(ApprovalInvalid, match="does not exist"):
        fixture.service.approve("not-present", "human-1", reason="approved")

    intent = _intent()
    fixture.service.create_request(_request(intent))
    second_request = _request(intent).model_copy(update={"run_id": "run-2"})
    with pytest.raises(ApprovalInvalid, match="already used by another Run"):
        fixture.service.create_request(second_request)


def test_service_requires_shared_event_store_and_rejects_stale_attempt_authority(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path / "approvals.db")
    foreign = SQLiteEventStore(tmp_path / "foreign.db")
    with pytest.raises(ValueError, match="share one SQLiteEventStore"):
        ApprovalService(
            event_store=foreign,
            budget_ledger=fixture.ledger,
            authenticator=lambda credential: ApprovalPrincipal(
                principal_id=credential, principal_type="human", permissions=frozenset({"approval:approve"})
            ),
            attempt_authority=_AttemptAuthority(),
            policy_state=lambda _request: fixture.state,
        )
    fixture.service.create_request(_request(_intent()))
    issued = fixture.service.approve("approval-1", "human-1", reason="approved")
    stale = ApprovalService(
        event_store=fixture.store,
        budget_ledger=fixture.ledger,
        authenticator=lambda credential: ApprovalPrincipal(
            principal_id=credential, principal_type="human", permissions=frozenset({"approval:approve"})
        ),
        attempt_authority=type("UnavailableAuthority", (), {"is_current": lambda _self, _attempt: False})(),
        policy_state=lambda _request: fixture.state,
        now=lambda: fixture.now[0],
    )
    with pytest.raises(ApprovalInvalid, match="not current"):
        stale.bind_to_attempt(issued.approval_grant_id, _attempt())
