"""Cross-version registration must serialize on the real SQLite store."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from threading import Event

import pytest

from orchestrator.persistence import SQLiteEventStore
from orchestrator.persistence.mediation_identity import mediation_stream_id
from orchestrator.runtime.general_worker_journal import GeneralWorkerJournalError
from orchestrator.runtime.mediation_actors import MediationOperation
from orchestrator.runtime.mediation_journal import MediationJournal, MediationJournalError
from tests.unit.runtime.test_general_worker_authority import dispatch_case
from tests.unit.runtime.test_mediation_review_fixes import digest, operation, record_inert

TIMEOUT = 10


def v2_operation(case):
    data = asdict(operation())
    del data["commitment_sha256"]
    data.update(case.identity)
    data["operation_id"] = mediation_stream_id(data["run_id"], data["node_id"], data["attempt_id"], data["fencing_generation"])
    return MediationOperation(**data, commitment_sha256=digest(data))


@pytest.mark.parametrize("winner", ["v1", "v2"])
def test_cross_version_metadata_registration_order_for_same_admitted_attempt(tmp_path, winner):
    case = dispatch_case(tmp_path, register=False)
    entered, finished, attempting = Event(), Event(), Event()
    candidate = v2_operation(case)

    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "v1":
                with pytest.raises(MediationJournalError):
                    MediationJournal(store).register(candidate)
            else:
                with pytest.raises(GeneralWorkerJournalError):
                    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
                    GeneralWorkerJournal(store).request(**case.registration)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "v1":
                    case.journal.request(**case.registration)
                else:
                    MediationJournal(case.store).register(candidate)
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "v1-v2-lock-test", first_commit)
            future.result(timeout=TIMEOUT)
    finally:
        finished.set()
        case.store.close()


@pytest.mark.parametrize("winner", ["permit", "release"])
def test_permit_and_scheduler_release_are_serialized(tmp_path, winner):
    from datetime import datetime, timezone
    from orchestrator.scheduler import Scheduler
    from orchestrator.scheduler.core import SchedulerError
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_mediation_review_fixes import intent

    case = dispatch_case(tmp_path, register=False)
    data = asdict(operation())
    del data["commitment_sha256"]
    data.update(operation_id="v2-current", **case.identity)
    data["operation_id"] = mediation_stream_id(data["run_id"], data["node_id"], data["attempt_id"], data["fencing_generation"])
    candidate = MediationOperation(**data, commitment_sha256=digest(data))
    actor = intent(operation_id=candidate.operation_id, fencing_generation=candidate.fencing_generation)
    entered, finished, attempting = Event(), Event(), Event()

    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "permit":
                with pytest.raises((SchedulerError, ValueError)):
                    Scheduler(store, limits=_application_limits()).finish_attempt(
                        **case.identity, completed_at=datetime.now(timezone.utc),
                        outcome="failed", known_no_effect=True)
            else:
                with pytest.raises(MediationJournalError):
                    MediationJournal(store).register(candidate)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "permit":
                    journal = MediationJournal(case.store)
                    journal.register(candidate)
                    record_inert(journal, candidate.operation_id, actor)
                else:
                    case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                        outcome="failed", known_no_effect=True)
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "permit-release-lock-test", first_commit)
            future.result(timeout=TIMEOUT)
    finally:
        finished.set()
        case.store.close()


def test_exact_cancellation_receipt_remains_read_only_after_v2_metadata(tmp_path):
    from datetime import datetime, timezone
    from tests.unit.runtime.test_mediation_review_fixes import H
    case = dispatch_case(tmp_path, register=False)
    try:
        case.lifecycle.request_cancel(case.identity["run_id"], reason_code="user_request")
        receipt = dict(**case.identity, stopped_at=datetime.now(timezone.utc),
                       stop_receipt_hash=H, no_effect_receipt_hash=H)
        case.scheduler.acknowledge_cancellation(**receipt)
        data = asdict(operation())
        del data["commitment_sha256"]
        data.update(case.identity)
        data["operation_id"] = mediation_stream_id(**case.identity)
        from orchestrator.persistence import EventDraft
        candidate = MediationOperation(**data, commitment_sha256=digest(data))
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inert recovery fixture: not an admitted launch or authority.
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        case.scheduler.acknowledge_cancellation(**receipt)
    finally:
        case.store.close()


def test_exact_verified_receipt_remains_read_only_after_v2_metadata(tmp_path):
    from orchestrator.persistence import EventDraft
    from tests.support.attempt_acceptance import ADMISSION
    from tests.unit.lifecycle.test_scheduler import _proof_attempt, proposal_for_attempt
    store, control, _, request, _, kwargs = _proof_attempt(tmp_path, **ADMISSION)
    try:
        proposal = proposal_for_attempt(control, run_id="run-1", node_id="node-1", attempt_id=request.attempt_id)
        control.finish_verified_attempt(**kwargs, task_sha256=proposal.task_sha256)
        identity = dict(run_id="run-1", node_id="node-1", attempt_id=request.attempt_id, fencing_generation=1)
        data = asdict(operation())
        del data["commitment_sha256"]
        data.update(identity)
        data["operation_id"] = mediation_stream_id(**identity)
        candidate = MediationOperation(**data, commitment_sha256=digest(data))
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inert recovery fixture; no host authorization or launch.
        store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        control.finish_verified_attempt(**kwargs, task_sha256=proposal.task_sha256)
    finally:
        store.close()


def test_successor_admission_rejects_inert_prior_v2_history(tmp_path):
    from datetime import datetime, timedelta, timezone
    from orchestrator.models import ModelGatewayFailure
    from orchestrator.persistence import EventDraft
    from tests.support.attempt_acceptance import ADMISSION
    from tests.unit.lifecycle.test_scheduler import routed_pair
    case = dispatch_case(tmp_path, register=False, retries=2)
    try:
        now = datetime.now(timezone.utc)
        case.scheduler.finish_attempt(**case.identity, completed_at=now,
                                      outcome="failed", known_no_effect=True)
        plan = case.scheduler.plan_attempt_recovery(
            **case.identity, source_decision=case.decision, contract=case.contract,
            failure=ModelGatewayFailure(code="rate_limited", phase="provider",
                outcome="known_failure", retryable=True, http_status=429))
        routing, decision = routed_pair(case.registry, case.config, case.policy,
            attempt=2, contract_hash=case.contract.contract_hash, recovery_action=plan.action,
            recovery_authorization=plan.authorization, prior_decision_hash=case.decision.decision_hash,
            previous_model_id="model-1", failure_category="transient", retry_level=1)
        data = asdict(operation())
        del data["commitment_sha256"]
        data.update(case.identity)
        data["operation_id"] = mediation_stream_id(**case.identity)
        candidate = MediationOperation(**data, commitment_sha256=digest(data))
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inert historical registry fixture; this is not an authenticated launch.
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        with pytest.raises(Exception):
            case.scheduler.accept_routing(routing, decision, accepted_at=now,
                lease_expires_at=now + timedelta(minutes=5), **ADMISSION)
        assert len(case.lifecycle.replay("run-1").node("node-1").attempts) == 1
    finally:
        case.store.close()


@pytest.mark.parametrize("winner", ["permit", "cancel"])
def test_inert_permit_and_cancellation_request_order(tmp_path, winner):
    from orchestrator.lifecycle import LifecycleController
    from tests.unit.runtime.test_mediation_review_fixes import intent
    case = dispatch_case(tmp_path, register=False)
    candidate = v2_operation(case)
    actor = intent(operation_id=candidate.operation_id, fencing_generation=candidate.fencing_generation)
    journal = MediationJournal(case.store)
    journal.register(candidate)
    entered, finished, attempting = Event(), Event(), Event()

    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "permit":
                LifecycleController(store).request_cancel(candidate.run_id, reason_code="user_request")
            else:
                with pytest.raises(MediationJournalError):
                    record_inert(MediationJournal(store), candidate.operation_id, actor)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "permit":
                    record_inert(journal, candidate.operation_id, actor)
                else:
                    case.lifecycle.request_cancel(candidate.run_id, reason_code="user_request")
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "permit-cancel-lock-test", first_commit)
            future.result(timeout=TIMEOUT)
        assert case.lifecycle.replay(candidate.run_id).status == "cancelling"
        actors = journal.load(candidate.operation_id).actors
        assert len(actors) == (1 if winner == "permit" else 0)
        if actors:
            assert actors[0].status == "launch_unknown"
        late = intent("later", 2, operation_id=candidate.operation_id,
                      fencing_generation=candidate.fencing_generation,
                      unit_name="later.service", launch_id="later")
        with pytest.raises(MediationJournalError):
            record_inert(journal, candidate.operation_id, late)
    finally:
        finished.set()
        case.store.close()


@pytest.mark.parametrize("winner", ["v2", "reconcile"])
def test_reconcile_and_inert_registry_serialization(tmp_path, winner):
    from datetime import datetime, timezone
    from orchestrator.scheduler import Scheduler
    from orchestrator.scheduler.core import SchedulerError
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_mediation_review_fixes import intent
    case = dispatch_case(tmp_path, register=False)
    candidate = v2_operation(case)
    actor = intent(operation_id=candidate.operation_id, fencing_generation=candidate.fencing_generation)
    if winner == "v2":
        journal = MediationJournal(case.store)
        journal.register(candidate)
        record_inert(journal, candidate.operation_id, actor)
    case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                  outcome="outcome_unknown")
    entered, finished, attempting = Event(), Event(), Event()

    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "v2":
                with pytest.raises((SchedulerError, ValueError)):
                    Scheduler(store, limits=_application_limits()).reconcile_attempt(
                        **case.identity, reconciled_at=datetime.now(timezone.utc),
                        outcome="failed", known_no_effect=True)
            else:
                with pytest.raises(MediationJournalError):
                    MediationJournal(store).register(candidate)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "reconcile":
                    case.scheduler.reconcile_attempt(**case.identity, reconciled_at=datetime.now(timezone.utc),
                                                     outcome="failed", known_no_effect=True)
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "reconcile-lock-test", first_commit)
            future.result(timeout=TIMEOUT)
    finally:
        finished.set()
        case.store.close()


def test_public_v1_prepare_rejects_inert_v2_history(tmp_path):
    from orchestrator.persistence import EventDraft
    from tests.unit.runtime.test_general_worker_journal import epoch
    case = dispatch_case(tmp_path, register=False)
    try:
        v1 = case.journal.request(**case.registration)
        candidate = v2_operation(case)
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inert history verifies the public gate, not a host launch.
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        with pytest.raises(GeneralWorkerJournalError):
            case.journal.prepare_turn(v1, phase="request", prepared=epoch()[0])
        assert case.journal.read(v1.identity).turns == ()
    finally:
        case.store.close()


@pytest.mark.parametrize("winner", ["intent", "unknown"])
def test_outcome_unknown_and_inert_intent_order_preserves_hold(tmp_path, winner):
    from datetime import datetime, timezone
    from orchestrator.scheduler import Scheduler
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_mediation_review_fixes import intent
    case = dispatch_case(tmp_path, register=False)
    candidate = v2_operation(case)
    journal = MediationJournal(case.store)
    journal.register(candidate)
    actor = intent(operation_id=candidate.operation_id, fencing_generation=candidate.fencing_generation)
    entered, finished, attempting = Event(), Event(), Event()

    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "intent":
                Scheduler(store, limits=_application_limits()).finish_attempt(
                    **case.identity, completed_at=datetime.now(timezone.utc), outcome="outcome_unknown")
            else:
                with pytest.raises(MediationJournalError):
                    record_inert(MediationJournal(store), candidate.operation_id, actor)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "intent":
                    record_inert(journal, candidate.operation_id, actor)
                else:
                    case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                                  outcome="outcome_unknown")
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "unknown-intent-lock-test", first_commit)
            future.result(timeout=TIMEOUT)
        state = journal.load(candidate.operation_id)
        assert len(state.actors) == (1 if winner == "intent" else 0)
        if state.actors:
            assert state.actors[0].status == "launch_unknown"
        assert any(e.event_type == "AttemptOutcomeUnknown" for e in case.store.read_stream("scheduler", "global"))
        assert not any(e.event_type == "AttemptSlotReleased" for e in case.store.read_stream("scheduler", "global"))
    finally:
        finished.set()
        case.store.close()


def test_unknown_transition_preserves_fence_despite_corrupt_v2_census(tmp_path):
    from datetime import datetime, timezone
    from orchestrator.budget import BudgetLedger
    case = dispatch_case(tmp_path, register=False)
    try:
        candidate = v2_operation(case)
        MediationJournal(case.store).register(candidate)
        conn = case.store._connection
        conn.execute("DROP TRIGGER mediation_scope_immutable_delete")
        conn.execute("DELETE FROM mediation_scope WHERE stream_id=?", (candidate.operation_id,))
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="outcome_unknown")
        events = case.store.read_stream("scheduler", "global")
        assert any(e.event_type == "AttemptOutcomeUnknown" for e in events)
        assert not any(e.event_type == "AttemptSlotReleased" for e in events)
        attempt = case.lifecycle.replay("run-1").node("node-1").attempts[-1]
        assert BudgetLedger(case.store).get_reservation(attempt.reservation_id, run_id="run-1").status == "unknown"
        with pytest.raises(ValueError):
            case.scheduler.reconcile_attempt(**case.identity, reconciled_at=datetime.now(timezone.utc),
                                             outcome="failed", known_no_effect=True)
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["eligibility", "authority_reserve", "journal_reserve"])
def test_public_v1_provider_progress_rejects_inert_v2_history(tmp_path, entry):
    from orchestrator.persistence import EventDraft
    case = dispatch_case(tmp_path)
    try:
        candidate = v2_operation(case)
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inert historical metadata exercises the public v1 paths.
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        with pytest.raises(GeneralWorkerJournalError):
            if entry == "eligibility":
                case.authority.resolve_eligibility(case.operation, purpose="dispatch")
            elif entry == "authority_reserve":
                case.authority.reserve_call(case.operation, request=case.request, request_body=case.body)
            else:
                case.journal.reserve_call(case.operation, request=case.request, request_body=case.body)
        assert case.journal.read(case.operation.identity).phase == "worker_stopped"
    finally:
        case.store.close()


def test_late_v1_ack_and_stop_remain_recordable_with_inert_v2_history(tmp_path):
    from orchestrator.persistence import EventDraft
    from tests.unit.runtime.test_general_worker_journal import epoch
    case = dispatch_case(tmp_path, register=False)
    try:
        initial = case.journal.request(**case.registration)
        prepared, ack, stop = epoch()
        ready = case.journal.prepare_turn(initial, phase="request", prepared=prepared)
        candidate = v2_operation(case)
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inert history; it does not authenticate either receipt.
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        acknowledged = case.journal.ack_turn(ready, receipt=ack)
        stopped = case.journal.stop_turn(acknowledged, receipt=stop)
        assert stopped.turns[-1].termination_receipt == stop
        assert case.journal.prepare_turn(initial, phase="request", prepared=prepared) == stopped
    finally:
        case.store.close()


@pytest.mark.parametrize("winner", ["intent", "expiry"])
def test_expiry_fence_and_inert_intent_order_preserves_hold(tmp_path, winner):
    from datetime import datetime, timedelta, timezone
    from orchestrator.scheduler import Scheduler
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_mediation_review_fixes import intent
    case = dispatch_case(tmp_path, register=False)
    candidate = v2_operation(case)
    journal = MediationJournal(case.store)
    journal.register(candidate)
    actor = intent(operation_id=candidate.operation_id, fencing_generation=candidate.fencing_generation)
    expired = datetime.now(timezone.utc) + timedelta(hours=1)
    entered, finished, attempting = Event(), Event(), Event()
    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "intent":
                assert case.identity["attempt_id"] in Scheduler(
                    store, limits=_application_limits()).mark_expired_attempts_unknown(as_of=expired)
            else:
                with pytest.raises(MediationJournalError):
                    record_inert(MediationJournal(store), candidate.operation_id, actor)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "intent":
                    record_inert(journal, candidate.operation_id, actor)
                else:
                    assert case.identity["attempt_id"] in case.scheduler.mark_expired_attempts_unknown(as_of=expired)
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "expiry-intent-lock-test", first_commit)
            future.result(timeout=TIMEOUT)
        events = case.store.read_stream("scheduler", "global")
        assert any(e.event_type == "AttemptOutcomeUnknown" for e in events)
        assert not any(e.event_type == "AttemptSlotReleased" for e in events)
        actors = journal.load(candidate.operation_id).actors
        assert len(actors) == (1 if winner == "intent" else 0)
        if actors:
            assert actors[0].status == "launch_unknown"
    finally:
        finished.set()
        case.store.close()


@pytest.mark.parametrize("entry", ["claim", "intent"])
def test_v1_provider_claim_and_intent_stop_after_v2_history(tmp_path, entry):
    import asyncio
    from orchestrator.persistence import EventDraft
    from tests.unit.runtime.test_general_worker_authority import reserve
    case = dispatch_case(tmp_path)
    try:
        reserve(case)
        candidate = v2_operation(case)
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        def insert_inert():
            # Inert history, not evidence of a launch.
            case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        async def run():
            if entry == "claim":
                insert_inert()
                assert await case.authority.is_accepted(case.request) is False
            else:
                assert await case.authority.is_accepted(case.request) is True
                insert_inert()
                with pytest.raises(GeneralWorkerJournalError):
                    case.authority.record_intent(case.request,
                        provider_id=case.request.accepted_route.provider_id,
                        provider_adapter="openai_responses", request_body=case.body)
        asyncio.run(run())
        assert case.provider_journal.read(case.request) is None
    finally:
        case.store.close()


@pytest.mark.parametrize("winner", ["v1", "older_v2"])
def test_cross_attempt_generation_metadata_blocks_new_v1(tmp_path, winner):
    from orchestrator.persistence import EventDraft
    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
    case = dispatch_case(tmp_path, register=False, previous_status="released")
    prior_route = case.previous.accepted_route
    identity = dict(run_id=prior_route.run_id, node_id=prior_route.node_id,
                    attempt_id=prior_route.attempt_id, fencing_generation=prior_route.fencing_generation)
    data = asdict(operation())
    del data["commitment_sha256"]
    data.update(identity)
    data["operation_id"] = mediation_stream_id(**identity)
    candidate = MediationOperation(**data, commitment_sha256=digest(data))
    entered, finished, attempting = Event(), Event(), Event()

    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "v1":
                with pytest.raises(MediationJournalError):
                    MediationJournal(store).register(candidate)
            else:
                with pytest.raises(GeneralWorkerJournalError):
                    GeneralWorkerJournal(store).request(**case.registration)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "v1":
                    case.journal.request(**case.registration)
                else:
                    payload = dict(schema_version=2, operation_id=candidate.operation_id,
                                   action="Registered", data=asdict(candidate))
                    draft = EventDraft("MediationRegistered", payload, **identity,
                                       correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
                    # Historical inert fixture: prior lease is no longer active.
                    case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "cross-attempt-lock-test", first_commit)
            future.result(timeout=TIMEOUT)
    finally:
        finished.set()
        case.store.close()


@pytest.mark.parametrize("damage", ["unresolved", "hidden_scope"])
def test_cancel_request_is_retained_but_auto_completion_holds_on_v2_history(tmp_path, damage):
    from datetime import datetime, timezone
    from orchestrator.persistence import EventDraft
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        candidate = v2_operation(case)
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inert historical fixture: cancellation must not infer actor termination.
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        if damage == "hidden_scope":
            case.store._connection.execute("DROP TRIGGER mediation_scope_immutable_delete")
            case.store._connection.execute("DELETE FROM mediation_scope WHERE stream_id=?", (candidate.operation_id,))
        before = sum(e.event_type == "AttemptSlotReleased" for e in case.store.read_stream("scheduler", "global"))
        result = case.lifecycle.request_cancel(case.identity["run_id"], reason_code="user_request")
        lifecycle = case.store.read_stream("run_lifecycle", case.identity["run_id"])
        assert any(e.event_type == "RunCancellationRequested" for e in lifecycle)
        assert not any(e.event_type == "RunCancelled" for e in lifecycle)
        assert result.status == "cancelling"
        assert result.node("node-1").status == "ready"
        assert sum(e.event_type == "AttemptSlotReleased" for e in case.store.read_stream("scheduler", "global")) == before
    finally:
        case.store.close()


def two_node_case(tmp_path, *, admit_second=False):
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    from orchestrator.config import ConfigCandidate, ConfigManager
    from orchestrator.lifecycle import LifecycleController, NodeSpec
    from orchestrator.routing import compile_node_contract
    from orchestrator.scheduler import Scheduler
    from orchestrator.security import PolicyAuthority, PolicyManifest
    from tests.support.attempt_acceptance import ADMISSION
    from tests.unit.lifecycle.test_scheduler import registry, effective_config, routed_pair, _application_limits

    store = SQLiteEventStore(tmp_path / "events.db")
    reg = registry()
    resolved = effective_config(reg)
    policy = PolicyManifest(authorities=(PolicyAuthority(source="system", max_permission="read-only",
        allowed_actions=("model_invoke",), allowed_tools=("model:model-1",)),))
    contracts = [compile_node_contract(run_id="run-1", node_id=node, role="coder", task_text="private-task",
        config=resolved.config, registry=reg, policy_manifest=policy, context_tokens=1000,
        max_output_tokens=500, required_capabilities=("text",))
        for node in ("node-1", "node-2")]
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    ConfigManager(ConfigCandidate(resolved=resolved, registry=reg)).start_run("run-1", store, workspace=workspace)
    lifecycle = LifecycleController(store)
    lifecycle.initialize_run("run-1")
    lifecycle.append_nodes("run-1", tuple(NodeSpec(node_id=contract.node_id, role="coder",
        planning_contract=contract, planning_contract_hash=contract.contract_hash, max_attempts=3)
        for contract in contracts), expected_graph_version=0, idempotency_key="graph", policy_manifest=policy)
    lifecycle.start_run("run-1")
    scheduler = Scheduler(store, limits=_application_limits())
    routing, decision = routed_pair(reg, resolved.config, policy, contract_hash=contracts[0].contract_hash)
    now = datetime.now(timezone.utc)
    scheduler.accept_routing(routing, decision, accepted_at=now-timedelta(seconds=1),
                            lease_expires_at=now+timedelta(minutes=5), **ADMISSION)
    identity = dict(run_id="run-1", node_id="node-1", attempt_id=routing.attempt_id,
                    fencing_generation=routing.fencing_generation)
    identity2 = None
    if admit_second:
        route2, decision2 = routed_pair(reg, resolved.config, policy, node_id="node-2",
                                        contract_hash=contracts[1].contract_hash)
        scheduler.accept_routing(route2, decision2, accepted_at=now-timedelta(seconds=1),
                                 lease_expires_at=now+timedelta(minutes=5), **ADMISSION)
        identity2 = dict(run_id="run-1", node_id="node-2", attempt_id=route2.attempt_id,
                         fencing_generation=route2.fencing_generation)
    return SimpleNamespace(store=store, scheduler=scheduler, lifecycle=lifecycle, identity=identity,
                           identity2=identity2, expiry=now+timedelta(minutes=6))


@pytest.mark.parametrize("winner", ["intent", "cancel"])
@pytest.mark.parametrize("damage", [False, True])
def test_multi_node_cancel_request_remains_safe_with_v2_history(tmp_path, winner, damage):
    from orchestrator.lifecycle import LifecycleController
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    from tests.unit.runtime.test_mediation_review_fixes import intent
    case = two_node_case(tmp_path)
    candidate = v2_operation(case)
    journal = MediationJournal(case.store)
    actor = intent(operation_id=candidate.operation_id, fencing_generation=candidate.fencing_generation)
    journal.register(candidate)
    if damage:
        case.store._connection.execute("DROP TRIGGER mediation_scope_immutable_delete")
        case.store._connection.execute("DELETE FROM mediation_scope WHERE stream_id=?", (candidate.operation_id,))
    entered, finished, attempting = Event(), Event(), Event()
    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "intent" and not damage:
                LifecycleController(store).request_cancel(candidate.run_id, reason_code="user_request")
            elif winner == "intent":
                # Damaged census cannot prevent the safety request.
                LifecycleController(store).request_cancel(candidate.run_id, reason_code="user_request")
            else:
                with pytest.raises(MediationJournalError):
                    record_inert(MediationJournal(store), candidate.operation_id, actor)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "intent":
                    if damage:
                        with pytest.raises(MediationJournalError):
                            record_inert(journal, candidate.operation_id, actor)
                    else:
                        record_inert(journal, candidate.operation_id, actor)
                else:
                    case.lifecycle.request_cancel(candidate.run_id, reason_code="user_request")
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "multi-node-cancel-lock", first_commit)
            future.result(timeout=TIMEOUT)
        state = case.lifecycle.replay(candidate.run_id)
        assert state.status == "cancelling"
        assert len(state.nodes) == 2
        assert state.node("node-2").status == "ready"
        events = case.store.read_stream("scheduler", "global")
        assert not any(e.event_type == "AttemptSlotReleased" for e in events)
        gate = GeneralWorkerGate(case.store)
        def release_check(_events, _version):
            gate.check_release(candidate.run_id, candidate.node_id)
        with pytest.raises(ValueError):
            case.store.append_checked("scheduler", "global", "cancel-release-check", release_check)
    finally:
        finished.set()
        case.store.close()


@pytest.mark.parametrize("winner", ["successor", "older_v2"])
def test_successor_and_prior_v2_history_both_commit_orders(tmp_path, winner):
    from datetime import datetime, timedelta, timezone
    from orchestrator.models import ModelGatewayFailure
    from orchestrator.persistence import EventDraft
    from orchestrator.scheduler import Scheduler
    from orchestrator.scheduler.core import SchedulerError
    from tests.support.attempt_acceptance import ADMISSION
    from tests.unit.lifecycle.test_scheduler import routed_pair, _application_limits
    case = dispatch_case(tmp_path, register=False, retries=2)
    now = datetime.now(timezone.utc)
    case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="failed", known_no_effect=True)
    plan = case.scheduler.plan_attempt_recovery(
        **case.identity, source_decision=case.decision, contract=case.contract,
        failure=ModelGatewayFailure(code="rate_limited", phase="provider", outcome="known_failure",
                                   retryable=True, http_status=429))
    routing, decision = routed_pair(case.registry, case.config, case.policy, attempt=2,
        contract_hash=case.contract.contract_hash, recovery_action=plan.action,
        recovery_authorization=plan.authorization, prior_decision_hash=case.decision.decision_hash,
        previous_model_id="model-1", failure_category="transient", retry_level=1)
    candidate = v2_operation(case)
    entered, finished, attempting = Event(), Event(), Event()
    def other_connection():
        with SQLiteEventStore(tmp_path / "events.db") as store:
            entered.set()
            assert finished.wait(TIMEOUT)
            attempting.set()
            if winner == "successor":
                with pytest.raises(MediationJournalError):
                    MediationJournal(store).register(candidate)
            else:
                with pytest.raises(SchedulerError):
                    Scheduler(store, limits=_application_limits()).accept_routing(
                        routing, decision, accepted_at=now,
                        lease_expires_at=now+timedelta(minutes=5), **ADMISSION)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(other_connection)
            assert entered.wait(TIMEOUT)
            def first_commit(_events, _version):
                if winner == "successor":
                    case.scheduler.accept_routing(routing, decision, accepted_at=now,
                        lease_expires_at=now+timedelta(minutes=5), **ADMISSION)
                else:
                    payload = dict(schema_version=2, operation_id=candidate.operation_id,
                                   action="Registered", data=asdict(candidate))
                    draft = EventDraft("MediationRegistered", payload, **case.identity,
                                       correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
                    # Inert historical fixture; not an active launch.
                    case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
                finished.set()
                assert attempting.wait(TIMEOUT)
            case.store.append_checked("scheduler", "global", "successor-history-lock-test", first_commit)
            future.result(timeout=TIMEOUT)
    finally:
        finished.set()
        case.store.close()


def test_direct_lifecycle_successor_cannot_bypass_unresolved_v2(tmp_path):
    from datetime import datetime, timezone
    from orchestrator.persistence import EventDraft
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        node = case.lifecycle.replay("run-1").node("node-1")
        successor = node.attempts[-1].model_copy(update={"attempt_id": "direct-successor",
                                                         "fencing_generation": 2,
                                                         "status": "accepted"})
        candidate = v2_operation(case)
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inert recovery fixture; no launch or host proof.
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        with pytest.raises(ValueError):
            case.lifecycle.record_attempt_accepted("run-1", node_id="node-1", attempt=successor,
                decision_hash="sha256:" + "a" * 64, causation_id="sha256:" + "a" * 64)
        assert len(case.lifecycle.replay("run-1").node("node-1").attempts) == 1
    finally:
        case.store.close()


def test_v1_journal_terminal_validation_uses_bounded_context_not_global_stream(tmp_path, monkeypatch):
    case = dispatch_case(tmp_path)
    try:
        original = case.store.read_stream
        def read(stream_type, stream_id, *args, **kwargs):
            if stream_type == "scheduler":
                raise AssertionError("unbounded scheduler stream read")
            return original(stream_type, stream_id, *args, **kwargs)
        monkeypatch.setattr(case.store, "read_stream", read)
        assert case.journal.read(case.operation.identity) == case.operation
    finally:
        case.store.close()


def test_cancel_completion_holds_when_fixed_census_budget_exceeded(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    import orchestrator.lifecycle.controller as module
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        monkeypatch.setattr(module, "MAX_CANCEL_CENSUS_WORK", 1, raising=False)
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelling"
        assert not any(e.event_type == "RunCancelled" for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()


@pytest.mark.parametrize("stream", ["mediation_v2", "general_worker"])
def test_cancel_completion_holds_on_unknown_node_scope(tmp_path, stream):
    from datetime import datetime, timezone
    from orchestrator.persistence import EventDraft
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        identity = dict(run_id="run-1", node_id="unknown-node", attempt_id="unknown-attempt",
                        fencing_generation=1)
        if stream == "mediation_v2":
            data = asdict(operation())
            del data["commitment_sha256"]
            data.update(identity)
            data["operation_id"] = mediation_stream_id(**identity)
            candidate = MediationOperation(**data, commitment_sha256=digest(data))
            payload = dict(schema_version=2, operation_id=candidate.operation_id,
                           action="Registered", data=asdict(candidate))
            draft = EventDraft("MediationRegistered", payload, **identity,
                               correlation_id="run-1", causation_id=candidate.commitment_sha256)
            stream_id, key = candidate.operation_id, digest(payload)
        else:
            draft = EventDraft("GeneralWorkerRequested", {}, **identity,
                               correlation_id="run-1", causation_id="unknown")
            stream_id, key = "unknown-worker", "unknown-worker"
        # Inert malformed history tests the census, never a host proof.
        case.store.append(stream, stream_id, 0, [draft], key)
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelling"
        assert not any(e.event_type == "RunCancelled" for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()

@pytest.mark.parametrize("corruption", ["scope", "version", "header"])
def test_node_gate_preflights_v2_raw_fields_before_materializing(tmp_path, corruption):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    case = dispatch_case(tmp_path, register=False)
    candidate = v2_operation(case)
    try:
        MediationJournal(case.store).register(candidate)
        conn = case.store._connection
        if corruption == "scope":
            conn.execute("DROP TRIGGER mediation_scope_immutable_update")
            conn.execute("UPDATE mediation_scope SET attempt_id=? WHERE stream_id=?", ("x" * 100000, candidate.operation_id))
        elif corruption == "version":
            conn.execute("UPDATE stream_versions SET current_version=? WHERE stream_type='mediation_v2' AND stream_id=?", ("x" * 100000, candidate.operation_id))
        else:
            conn.execute("DROP TRIGGER events_immutable_update")
            conn.execute("UPDATE events SET attempt_id=? WHERE stream_type='mediation_v2' AND stream_id=?", ("x" * 100000, candidate.operation_id))
        with case.store._cancellation_proof_write():
            with pytest.raises(CancellationHeld, match="budget"):
                GeneralWorkerGate(case.store)._v2_census("run-1", "node-1")
    finally:
        case.store.close()


def test_node_gate_preflights_v1_payload_before_decode(tmp_path, monkeypatch):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    case = dispatch_case(tmp_path)
    decoded = []
    original = case.store._row_to_event
    def observed(row):
        if row["stream_type"] == "general_worker":
            decoded.append(True)
        return original(row)
    try:
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET payload_json=? WHERE stream_type='general_worker' AND stream_version=1", ("x" * 100000,))
        monkeypatch.setattr(case.store, "_row_to_event", observed)
        with case.store._cancellation_proof_write():
            with pytest.raises(CancellationHeld, match="budget"):
                GeneralWorkerGate(case.store).check_release("run-1", "node-1")
        assert not decoded
    finally:
        case.store.close()


def test_node_gate_ignores_corrupt_history_for_another_node(tmp_path):
    from datetime import datetime, timezone
    from orchestrator.persistence import EventDraft
    case = dispatch_case(tmp_path, register=False)
    try:
        case.store.append("general_worker", "other-node-stream", 0,
            [EventDraft("GeneralWorkerRequested", {}, run_id="run-1", node_id="other-node",
                        attempt_id="other-attempt", fencing_generation=1,
                        correlation_id="run-1", causation_id="other")], "other")
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET payload_json=? WHERE stream_type='general_worker' AND stream_id='other-node-stream'", ("x" * 100000,))
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
    finally:
        case.store.close()


def test_node_census_rechecks_same_connection_mutation(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    case = dispatch_case(tmp_path, register=False)
    try:
        candidate = v2_operation(case)
        MediationJournal(case.store).register(candidate)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        gate = GeneralWorkerGate(case.store)
        budget = gate._budget()
        with case.store._cancellation_proof_write():
            assert gate._v2_census("run-1", "node-1", budget)
            conn.execute("UPDATE events SET payload_json=? WHERE stream_type='mediation_v2' AND stream_id=?", ("x" * 100000, candidate.operation_id))
            with pytest.raises(CancellationHeld, match="budget"):
                gate._v2_census("run-1", "node-1", budget)
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["request_replay", "ack"])
@pytest.mark.parametrize("corruption", ["oversized", "hidden_tail"])
def test_public_v1_mutation_preflights_history_before_decode(tmp_path, monkeypatch, entry, corruption):
    from tests.unit.runtime.test_general_worker_journal import epoch
    case = dispatch_case(tmp_path, register=False)
    decoded = []
    original = case.store._row_to_event
    def observed(row):
        if row["stream_type"] == "general_worker":
            decoded.append(True)
        return original(row)
    try:
        op = case.journal.request(**case.registration)
        conn = case.store._connection
        if corruption == "oversized":
            conn.execute("DROP TRIGGER events_immutable_update")
            conn.execute("UPDATE events SET payload_json=? WHERE stream_type='general_worker' AND stream_id=?", ("x" * 100000, op.identity.stream_id))
        else:
            conn.execute("UPDATE stream_versions SET current_version=0 WHERE stream_type='general_worker' AND stream_id=?", (op.identity.stream_id,))
        count = conn.execute("SELECT count(*) FROM events").fetchone()[0]
        monkeypatch.setattr(case.store, "_row_to_event", observed)
        with pytest.raises(GeneralWorkerJournalError):
            if entry == "request_replay":
                case.journal.request(**case.registration)
            else:
                case.journal.ack_turn(op, receipt=epoch()[1])
        assert not decoded
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == count
    finally:
        case.store.close()


def test_public_capture_preflights_provider_before_decode(tmp_path, monkeypatch):
    from tests.unit.runtime.test_general_worker_authority import reserve
    from tests.unit.runtime.test_general_worker_journal import provider_success, capture
    case = dispatch_case(tmp_path)
    decoded = []
    original = case.store._row_to_event
    def observed(row):
        if row["stream_type"] == "provider_call":
            decoded.append(True)
        return original(row)
    try:
        reserve(case)
        provider_success(case.store, case.request, body=case.body)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET payload_json=? WHERE stream_type='provider_call' AND stream_version=1", ("x" * 100000,))
        count = conn.execute("SELECT count(*) FROM events").fetchone()[0]
        monkeypatch.setattr(case.store, "_row_to_event", observed)
        with pytest.raises(GeneralWorkerJournalError):
            capture(case.journal, case.operation)
        assert not decoded
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == count
    finally:
        case.store.close()


def test_public_eligibility_preflights_worker_before_decode(tmp_path, monkeypatch):
    case = dispatch_case(tmp_path, register=False)
    decoded = []
    original = case.store._row_to_event
    def observed(row):
        if row["stream_type"] == "general_worker":
            decoded.append(True)
        return original(row)
    try:
        operation = case.journal.request(**case.registration)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET payload_json=? WHERE stream_type='general_worker' AND stream_id=?", ("x" * 100000, operation.identity.stream_id))
        count = conn.execute("SELECT count(*) FROM events").fetchone()[0]
        monkeypatch.setattr(case.store, "_row_to_event", observed)
        with pytest.raises(GeneralWorkerJournalError):
            case.authority.resolve_eligibility(operation, purpose="request")
        assert not decoded
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == count
    finally:
        case.store.close()


def test_node_census_rejects_unknown_v2_envelope(tmp_path):
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    case = dispatch_case(tmp_path, register=False)
    try:
        candidate = v2_operation(case)
        MediationJournal(case.store).register(candidate)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET schema_version=42 WHERE stream_type='mediation_v2' AND stream_id=?", (candidate.operation_id,))
        with case.store._cancellation_proof_write():
            with pytest.raises(ValueError):
                GeneralWorkerGate(case.store)._v2_census("run-1", "node-1")
    finally:
        case.store.close()


def test_node_census_hides_corrupt_lifecycle_diagnostics(tmp_path):
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    case = dispatch_case(tmp_path, register=False)
    secret = "private-task-listing-secret"
    try:
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET event_type=? WHERE stream_type='run_lifecycle' AND stream_version=1", (secret,))
        with case.store._cancellation_proof_write():
            with pytest.raises(ValueError) as error:
                GeneralWorkerGate(case.store)._v2_census("run-1", "node-1")
        assert secret not in str(error.value)
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["publish", "consume"])
def test_v1_post_commit_grant_preflights_before_decode(tmp_path, monkeypatch, entry):
    from tests.unit.runtime.test_general_worker_journal import epoch
    case = dispatch_case(tmp_path, register=False)
    decoded = []
    original = case.store._row_to_event
    def observed(row):
        if row["stream_type"] == "general_worker":
            decoded.append(True)
        return original(row)
    try:
        op = case.journal.request(**case.registration)
        prepared = epoch()[0]
        case.journal.prepare_turn(op, phase="request", prepared=prepared)
        marker = case.journal._fresh_prepare
        assert marker is not None
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET payload_json=? WHERE stream_type='general_worker' AND stream_id=? AND stream_version=1", ("x" * 100000, op.identity.stream_id))
        monkeypatch.setattr(case.store, "_row_to_event", observed)
        if entry == "publish":
            case.journal._fresh_prepare = None
            try:
                case.journal._publish_fresh_prepare(marker, case.journal._prepare_generation)
            except Exception:
                pass
            assert case.journal._fresh_prepare is None
        else:
            assert not case.journal._consume_fresh_prepare(op.identity, "request", prepared)
        assert not decoded
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["load", "register", "close"])
@pytest.mark.parametrize("damage", ["header", "version", "lag"])
def test_public_v2_history_preflights_raw_metadata(tmp_path, monkeypatch, entry, damage):
    case = dispatch_case(tmp_path, register=False)
    decoded = []
    original = case.store._row_to_event
    def observed(row):
        if row["stream_type"] == "mediation_v2":
            decoded.append(True)
        return original(row)
    try:
        candidate = v2_operation(case)
        journal = MediationJournal(case.store)
        journal.register(candidate)
        conn = case.store._connection
        if damage == "header":
            conn.execute("DROP TRIGGER events_immutable_update")
            conn.execute("UPDATE events SET correlation_id=? WHERE stream_type='mediation_v2' AND stream_id=?", ("x" * 100000, candidate.operation_id))
        elif damage == "version":
            conn.execute("UPDATE stream_versions SET current_version=? WHERE stream_type='mediation_v2' AND stream_id=?", ("x" * 100000, candidate.operation_id))
        else:
            conn.execute("UPDATE stream_versions SET current_version=0 WHERE stream_type='mediation_v2' AND stream_id=?", (candidate.operation_id,))
        before = conn.execute("SELECT count(*) FROM events").fetchone()[0]
        monkeypatch.setattr(case.store, "_row_to_event", observed)
        with pytest.raises(MediationJournalError):
            if entry == "load": journal.load(candidate.operation_id)
            elif entry == "register": journal.register(candidate)
            else: journal.close(candidate.operation_id)
        assert not decoded
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == before
    finally:
        case.store.close()


def test_expiry_marks_both_nodes_despite_one_corrupt_v2_scope(tmp_path):
    from orchestrator.budget import BudgetLedger
    case = two_node_case(tmp_path, admit_second=True)
    try:
        from types import SimpleNamespace
        admissions = [e for e in case.store.read_stream("scheduler", "global") if e.event_type == "RoutingDecisionAccepted"]
        first = min(admissions, key=lambda e: e.payload["attempt_ref"]).node_id
        first_identity = case.identity if first == "node-1" else case.identity2
        candidate = v2_operation(SimpleNamespace(identity=first_identity))
        MediationJournal(case.store).register(candidate)
        conn = case.store._connection
        conn.execute("DROP TRIGGER mediation_scope_immutable_delete")
        conn.execute("DELETE FROM mediation_scope WHERE stream_id=?", (candidate.operation_id,))
        assert set(case.scheduler.mark_expired_attempts_unknown(as_of=case.expiry)) == {
            case.identity["attempt_id"], case.identity2["attempt_id"]}
        events = case.store.read_stream("scheduler", "global")
        assert {e.node_id for e in events if e.event_type == "AttemptOutcomeUnknown"} == {"node-1", "node-2"}
        assert not any(e.event_type == "AttemptSlotReleased" for e in events)
        state = case.lifecycle.replay("run-1")
        for node in state.nodes:
            attempt = node.attempts[-1]
            assert BudgetLedger(case.store).get_reservation(attempt.reservation_id, run_id="run-1").status == "unknown"
    finally:
        case.store.close()


def test_expiry_continues_to_healthy_node_when_first_reservation_missing(tmp_path):
    import json
    from orchestrator.persistence.sqlite_event_store import _sha256_json
    case = two_node_case(tmp_path, admit_second=True)
    try:
        admissions = [e for e in case.store.read_stream("scheduler", "global") if e.event_type == "RoutingDecisionAccepted"]
        first, second = sorted(admissions, key=lambda e: e.payload["attempt_ref"])
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        row = conn.execute("SELECT payload_json FROM events WHERE stream_type='budget' AND event_type='BudgetReserved' AND node_id=?", (first.node_id,)).fetchone()
        payload = json.loads(row[0]); payload["reservation_id"] = "unrelated-reservation"
        conn.execute("UPDATE events SET payload_json=?, payload_hash=? WHERE stream_type='budget' AND event_type='BudgetReserved' AND node_id=?", (json.dumps(payload), _sha256_json(payload), first.node_id))
        assert case.scheduler.mark_expired_attempts_unknown(as_of=case.expiry) == (second.attempt_id,)
        assert case.scheduler.last_expiry_holds == (first.attempt_id,)
        events = case.store.read_stream("scheduler", "global")
        assert {e.node_id for e in events if e.event_type == "AttemptOutcomeUnknown"} == {second.node_id}
        assert not any(e.event_type == "AttemptSlotReleased" for e in events)
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["load", "register", "close"])
def test_public_v2_rejects_gap_and_tail_before_decode(tmp_path, monkeypatch, entry):
    import uuid
    case = dispatch_case(tmp_path, register=False)
    decoded = []
    original = case.store._row_to_event
    def observed(row):
        if row["stream_type"] == "mediation_v2":
            decoded.append(True)
        return original(row)
    try:
        candidate = v2_operation(case)
        journal = MediationJournal(case.store)
        journal.register(candidate)
        conn = case.store._connection
        conn.execute("""INSERT INTO events SELECT ?, stream_type, stream_id, 3, event_type, schema_version,
            occurred_at, payload_json, payload_hash, ?, run_id, node_id, attempt_id, fencing_generation,
            correlation_id, causation_id FROM events WHERE stream_type='mediation_v2' AND stream_id=? AND stream_version=1""",
            (str(uuid.uuid4()), str(uuid.uuid4()), candidate.operation_id))
        conn.execute("UPDATE stream_versions SET current_version=2 WHERE stream_type='mediation_v2' AND stream_id=?", (candidate.operation_id,))
        before = conn.execute("SELECT count(*) FROM events").fetchone()[0]
        monkeypatch.setattr(case.store, "_row_to_event", observed)
        with pytest.raises(MediationJournalError):
            if entry == "load": journal.load(candidate.operation_id)
            elif entry == "register": journal.register(candidate)
            else: journal.close(candidate.operation_id)
        assert not decoded
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == before
    finally:
        case.store.close()
