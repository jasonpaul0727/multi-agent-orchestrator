"""Pre-decode budgets for the cancellation proof reader."""
import pytest

from orchestrator.persistence import SQLiteEventStore


def test_bounded_lifecycle_window_rejects_before_any_event_decode(tmp_path, monkeypatch):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    decoded = []
    original = SQLiteEventStore._row_to_event

    def observe(row):
        decoded.append(True)
        return original(row)

    try:
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
        with case.store._cancellation_proof_write():
            assert case.store._has_owned_write()
            budget = CancellationBudget(max_rows=1, max_bytes=100000, max_row_bytes=100000,
                                        max_streams=20, max_nodes=20)
            with pytest.raises(CancellationHeld, match="budget"):
                read_event_window(case.store, "run_lifecycle", "run-1", budget)
        assert decoded == []
    finally:
        case.store.close()


def test_cancellation_proof_rejects_unknown_ambient_transaction(tmp_path):
    with SQLiteEventStore(tmp_path / "events.db") as store:
        store._connection.execute("BEGIN")
        try:
            with pytest.raises(ValueError):
                with store._cancellation_proof_write():
                    pass
        finally:
            store._connection.rollback()


@pytest.mark.parametrize("column", ["payload_json", "idempotency_key"])
def test_oversized_raw_field_is_held_before_decode(tmp_path, monkeypatch, column):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET " + column + "=? WHERE stream_type='run_lifecycle' AND stream_version=1",
                     ("x" * 100001,))
        decoded = []
        original = SQLiteEventStore._row_to_event
        def observe(row):
            decoded.append(True)
            return original(row)
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
        with case.store._cancellation_proof_write():
            with pytest.raises(CancellationHeld, match="budget"):
                read_event_window(case.store, "run_lifecycle", "run-1",
                    CancellationBudget(max_rows=200, max_bytes=1000000, max_row_bytes=100000,
                                       max_streams=20, max_nodes=20))
        assert decoded == []
    finally:
        case.store.close()


def test_window_rejects_physical_tail_even_if_version_is_unchanged(tmp_path, monkeypatch):
    import uuid
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        conn = case.store._connection
        conn.execute("""INSERT INTO events SELECT ?, stream_type, stream_id, 100, event_type, schema_version,
            occurred_at, payload_json, payload_hash, ?, run_id, node_id, attempt_id, fencing_generation,
            correlation_id, causation_id FROM events WHERE stream_type='run_lifecycle' AND stream_version=1""",
            (str(uuid.uuid4()), str(uuid.uuid4())))
        decoded = []
        original = SQLiteEventStore._row_to_event
        def observe(row):
            decoded.append(True)
            return original(row)
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
        with case.store._cancellation_proof_write():
            with pytest.raises(CancellationHeld, match="integrity"):
                read_event_window(case.store, "run_lifecycle", "run-1",
                    CancellationBudget(max_rows=200, max_bytes=1000000, max_row_bytes=100000,
                                       max_streams=20, max_nodes=20))
        assert decoded == []
    finally:
        case.store.close()


def test_budget_cannot_cross_write_transactions(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, read_event_window
    with SQLiteEventStore(tmp_path / "events.db") as store:
        budget = CancellationBudget(max_rows=20, max_bytes=100000, max_row_bytes=10000,
                                    max_streams=20, max_nodes=20)
        with store._cancellation_proof_write():
            assert read_event_window(store, "run_lifecycle", "none", budget) == ([], 0)
        with store._cancellation_proof_write():
            with pytest.raises(ValueError, match="transaction changed"):
                read_event_window(store, "run_lifecycle", "none", budget)


def test_run_catalog_detects_unknown_node_without_decoding_payload(tmp_path, monkeypatch):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, read_run_catalog
    from orchestrator.persistence import EventDraft
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        draft = EventDraft("GeneralWorkerRequested", {}, run_id="run-1", node_id="unknown-node",
                           attempt_id="unknown", fencing_generation=1, correlation_id="run-1", causation_id="x")
        case.store.append("general_worker", "unknown-worker", 0, [draft], "unknown")
        decoded = []
        original = SQLiteEventStore._row_to_event
        def observe(row):
            decoded.append(True)
            return original(row)
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
        with case.store._cancellation_proof_write():
            scopes, physical = read_run_catalog(case.store, "run-1",
                CancellationBudget(max_rows=200, max_bytes=1000000, max_row_bytes=100000,
                                   max_streams=20, max_nodes=20))
        assert ("general_worker", "unknown-worker", "unknown-node", "unknown", 1) in scopes
        assert ("general_worker", "unknown-worker", "unknown-node", "unknown", 1, 1) in physical
        assert decoded == []
    finally:
        case.store.close()


def append_cancel_request(case):
    from orchestrator.lifecycle.controller import _hash
    from orchestrator.persistence import EventDraft
    payload = {"run_id": "run-1", "reason_code": "user_request"}
    draft = EventDraft("RunCancellationRequested", payload, run_id="run-1",
                       correlation_id="run-1", causation_id=_hash(payload))
    store = case.store
    store.append("run_lifecycle", "run-1", store.current_version("run_lifecycle", "run-1"),
                 [draft], "run-cancel-request:run-1")


def test_completion_uses_bounded_projection_without_snapshot_or_replay(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from orchestrator.persistence.snapshots import SnapshotStore
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        def forbidden(*_args, **_kwargs):
            raise AssertionError("unbounded replay or snapshot")
        monkeypatch.setattr(case.lifecycle, "replay", forbidden)
        monkeypatch.setattr(SnapshotStore, "load_valid", forbidden)
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "completed"
        assert result.state.status == "cancelled"
    finally:
        case.store.close()


def test_completion_budget_hold_does_not_decode_any_lifecycle_event(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET idempotency_key=? WHERE stream_type='run_lifecycle' AND stream_version=1",
                     ("x" * 1000000,))
        decoded = []
        original = SQLiteEventStore._row_to_event
        def observe(row):
            decoded.append(True)
            return original(row)
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "held"
        assert result.reason == "budget"
        assert decoded == []
    finally:
        case.store.close()


def test_no_call_v1_stopped_history_can_complete_cancellation(tmp_path):
    from datetime import datetime, timezone
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    # Existing v1 fixture models the legacy receipt contract only.
    case = dispatch_case(tmp_path)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelled"
    finally:
        case.store.close()


def test_request_exposes_non_sensitive_completion_hold_reason(tmp_path):
    from datetime import datetime, timezone
    from orchestrator.persistence import EventDraft
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    from tests.unit.runtime.test_mediation_review_fixes import operation, digest
    from orchestrator.runtime.mediation_actors import MediationOperation
    from orchestrator.persistence.mediation_identity import mediation_stream_id
    from dataclasses import asdict
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        data = asdict(operation())
        del data["commitment_sha256"]
        data.update(case.identity)
        data["operation_id"] = mediation_stream_id(**case.identity)
        candidate = MediationOperation(**data, commitment_sha256=digest(data))
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **case.identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelling"
        assert case.lifecycle.last_cancellation_completion.outcome == "held"
        assert case.lifecycle.last_cancellation_completion.reason == "unsupported"
    finally:
        case.store.close()


def test_unstopped_legacy_v1_history_does_not_complete_cancellation(tmp_path):
    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    from tests.unit.runtime.test_general_worker_journal import epoch
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        case.journal.prepare_turn(operation, phase="request", prepared=epoch()[0])
        # Lifecycle outcome alone is not a stop receipt or Scheduler release.
        case.lifecycle.record_attempt_completed("run-1", node_id="node-1",
            attempt_id=case.identity["attempt_id"], fencing_generation=case.identity["fencing_generation"],
            outcome="failed", causation_id="sha256:" + "a" * 64)
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelling"
        assert case.lifecycle.last_cancellation_completion.outcome == "held"
    finally:
        case.store.close()


def test_outer_rollback_discards_request_and_bounded_completion(tmp_path):
    from datetime import datetime, timezone
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        def outer(_events, _version):
            state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
            assert state.status == "cancelled"
            assert case.lifecycle.last_cancellation_completion.outcome == "pending_commit"
            raise RuntimeError("outer rollback")
        with pytest.raises(RuntimeError):
            case.store.append_checked("scheduler", "global", "cancel-rollback-test", outer)
        assert case.lifecycle.replay("run-1").status == "running"
        assert not any(e.event_type in {"RunCancellationRequested", "RunCancelled"}
                       for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()


def test_captured_v1_links_are_checked_with_bounded_provider_window(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, validate_v1_links
    from tests.unit.runtime.test_general_worker_authority import dispatch_case, captured
    # Existing v1 legacy fixture; not a v2 stop or host-authentication proof.
    case = dispatch_case(tmp_path)
    try:
        captured(case)
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=1000, max_bytes=2000000, max_row_bytes=100000,
                                        max_streams=50, max_nodes=20)
            validate_v1_links(case.store, case.operation, budget)
            assert ("provider_call", case.operation.call.provider_call_stream_id) in budget._cache
    finally:
        case.store.close()


def test_accepted_v1_reciprocal_links_use_bounded_dependency_windows(tmp_path, monkeypatch):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, validate_v1_links, validate_v1_terminal
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import terminal_case, direct
    from tests.unit.runtime.test_verifier_process import _ChildLauncher

    original = IsolatedVerifierProcess.__init__
    def offline_verifier(self, **kwargs):
        kwargs.setdefault("launcher", _ChildLauncher())
        original(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, "__init__", offline_verifier)
    # Existing v1 legacy fixture only; it is not a v2 host stop proof.
    case = terminal_case(tmp_path)
    try:
        direct(case)
        accepted = case.journal.read(case.operation.identity)
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=2000, max_bytes=4000000, max_row_bytes=200000,
                                        max_streams=100, max_nodes=20)
            validate_v1_links(case.store, accepted, budget)
            validate_v1_terminal(case.store, accepted, budget)
            assert ("budget", "run-1") in budget._cache
            assert ("artifact", accepted.capture.response_artifact.digest) in budget._cache
            assert ("artifact", accepted.acceptance.artifacts[0].digest) in budget._cache
    finally:
        case.store.close()


@pytest.mark.parametrize("dependency", ["provider", "proposal", "budget", "response", "output", "release"])
def test_accepted_v1_oversized_dependency_holds_before_decode(tmp_path, monkeypatch, dependency):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, validate_v1_links, validate_v1_terminal
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import terminal_case, direct
    from tests.unit.runtime.test_verifier_process import _ChildLauncher

    original = IsolatedVerifierProcess.__init__
    def offline_verifier(self, **kwargs):
        kwargs.setdefault("launcher", _ChildLauncher())
        original(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, "__init__", offline_verifier)
    case = terminal_case(tmp_path)
    try:
        direct(case)
        accepted = case.journal.read(case.operation.identity)
        targets = {
            "provider": ("provider_call", accepted.call.provider_call_stream_id),
            "proposal": ("verification_proposals", "run-1"),
            "budget": ("budget", "run-1"),
            "response": ("artifact", accepted.capture.response_artifact.digest),
            "output": ("artifact", accepted.acceptance.artifacts[0].digest),
            "release": ("scheduler", "global"),
        }
        stream, stream_id = targets[dependency]
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        extra = " AND event_type='AttemptSlotReleased'" if dependency == "release" else " AND stream_version=1"
        conn.execute("UPDATE events SET payload_json=? WHERE stream_type=? AND stream_id=?" + extra,
                     ("x" * 200001, stream, stream_id))
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=2000, max_bytes=4000000, max_row_bytes=200000,
                                        max_streams=100, max_nodes=20)
            with pytest.raises(CancellationHeld) as error:
                validate_v1_links(case.store, accepted, budget)
                validate_v1_terminal(case.store, accepted, budget)
            assert error.value.reason == "budget"
    finally:
        case.store.close()


def test_cached_window_is_invalidated_after_same_transaction_write(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        budget = CancellationBudget(max_rows=1000, max_bytes=2000000, max_row_bytes=100000,
                                    max_streams=20, max_nodes=20)
        with case.store._cancellation_proof_write():
            read_event_window(case.store, "run_lifecycle", "run-1", budget)
            conn.execute("UPDATE events SET payload_json=? WHERE stream_type='run_lifecycle' AND stream_version=1",
                         ("x" * 100001,))
            with pytest.raises(CancellationHeld, match="budget"):
                read_event_window(case.store, "run_lifecycle", "run-1", budget)
    finally:
        case.store.close()


def test_unexpected_decoder_failure_has_internal_reason_without_secret(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        secret = "private-task-listing-secret"
        def broken(_row):
            raise RuntimeError(secret)
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(broken))
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "held" and result.reason == "internal"
        assert secret not in repr(result)
        assert case.lifecycle.last_cancellation_completion.reason == "internal"
    finally:
        case.store.close()


def test_oversized_scope_header_is_held_before_decoding(tmp_path, monkeypatch):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, read_run_catalog
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    from tests.unit.runtime.test_mediation_cross_version_races import v2_operation
    from orchestrator.runtime.mediation_journal import MediationJournal
    case = dispatch_case(tmp_path, register=False)
    try:
        candidate = v2_operation(case)
        MediationJournal(case.store).register(candidate)
        conn = case.store._connection
        conn.execute("DROP TRIGGER mediation_scope_immutable_update")
        conn.execute("UPDATE mediation_scope SET node_id=?", ("x" * 100001,))
        decoded = []
        original = SQLiteEventStore._row_to_event
        def observe(row):
            decoded.append(True)
            return original(row)
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
        with case.store._cancellation_proof_write():
            with pytest.raises(CancellationHeld, match="budget"):
                read_run_catalog(case.store, "run-1",
                    CancellationBudget(max_rows=200, max_bytes=1000000, max_row_bytes=100000,
                                       max_streams=20, max_nodes=20))
        assert decoded == []
    finally:
        case.store.close()


def test_multi_node_cancel_completes_with_bounded_accepted_v1_links(tmp_path, monkeypatch):
    from orchestrator.lifecycle import LifecycleController, NodeSpec
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import terminal_case, direct
    from tests.unit.runtime.test_verifier_process import _ChildLauncher

    original_append = LifecycleController.append_nodes
    def add_second_node(self, run_id, nodes, **kwargs):
        first = nodes[0]
        second_contract = first.planning_contract.model_copy(update={"node_id": "node-2"})
        second = NodeSpec(node_id="node-2", role=first.role, planning_contract=second_contract,
                          planning_contract_hash=second_contract.contract_hash, max_attempts=3)
        return original_append(self, run_id, (*nodes, second), **kwargs)
    monkeypatch.setattr(LifecycleController, "append_nodes", add_second_node)
    original_init = IsolatedVerifierProcess.__init__
    def offline_verifier(self, **kwargs):
        kwargs.setdefault("launcher", _ChildLauncher())
        original_init(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, "__init__", offline_verifier)
    # Existing v1 receipt fixture, not proof of v2 host termination.
    case = terminal_case(tmp_path)
    try:
        direct(case)
        assert case.lifecycle.replay("run-1").status == "running"
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelled"
        assert case.lifecycle.last_cancellation_completion.outcome == "completed"
    finally:
        case.store.close()


def test_cancel_request_rejects_unknown_ambient_transaction_without_append(tmp_path):
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        conn = case.store._connection
        conn.execute("BEGIN")
        try:
            with pytest.raises(ValueError):
                case.lifecycle.request_cancel("run-1", reason_code="user_request")
            assert not any(e.event_type == "RunCancellationRequested"
                           for e in case.store.read_stream("run_lifecycle", "run-1"))
        finally:
            conn.rollback()
    finally:
        case.store.close()


@pytest.mark.parametrize("dependency", ["provider", "budget", "output"])
def test_multi_node_cancel_holds_on_oversized_accepted_dependency(tmp_path, monkeypatch, dependency):
    from orchestrator.lifecycle import LifecycleController, NodeSpec
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import terminal_case, direct
    from tests.unit.runtime.test_verifier_process import _ChildLauncher

    original_append = LifecycleController.append_nodes
    def add_second_node(self, run_id, nodes, **kwargs):
        first = nodes[0]
        second_contract = first.planning_contract.model_copy(update={"node_id": "node-2"})
        second = NodeSpec(node_id="node-2", role=first.role, planning_contract=second_contract,
                          planning_contract_hash=second_contract.contract_hash, max_attempts=3)
        return original_append(self, run_id, (*nodes, second), **kwargs)
    monkeypatch.setattr(LifecycleController, "append_nodes", add_second_node)
    original_init = IsolatedVerifierProcess.__init__
    def offline_verifier(self, **kwargs):
        kwargs.setdefault("launcher", _ChildLauncher())
        original_init(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, "__init__", offline_verifier)
    case = terminal_case(tmp_path)
    try:
        direct(case)
        accepted = case.journal.read(case.operation.identity)
        targets = {
            "provider": ("provider_call", accepted.call.provider_call_stream_id),
            "budget": ("budget", "run-1"),
            "output": ("artifact", accepted.acceptance.artifacts[0].digest),
        }
        stream, stream_id = targets[dependency]
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET payload_json=? WHERE stream_type=? AND stream_id=? AND stream_version=1",
                     ("x" * 65537, stream, stream_id))
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelling"
        assert case.lifecycle.last_cancellation_completion.reason == "budget"
        assert not any(e.event_type == "RunCancelled" for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()


def test_zero_version_entry_without_header_is_not_treated_as_absent(tmp_path):
    from datetime import datetime, timezone
    from orchestrator.persistence.mediation_identity import mediation_stream_id
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        stream_id = mediation_stream_id(**case.identity)
        case.store._connection.execute(
            "INSERT INTO stream_versions(stream_type, stream_id, current_version) VALUES('mediation_v2', ?, 0)",
            (stream_id,))
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelling"
        assert case.lifecycle.last_cancellation_completion.reason == "integrity"
    finally:
        case.store.close()


def test_oversized_noninteger_version_is_held_before_materialization(tmp_path, monkeypatch):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET stream_version=? WHERE stream_type='run_lifecycle' AND stream_version=1",
                     ("x" * 100001,))
        decoded = []
        original = SQLiteEventStore._row_to_event
        def observe(row):
            decoded.append(True)
            return original(row)
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
        with case.store._cancellation_proof_write():
            with pytest.raises(CancellationHeld) as error:
                read_event_window(case.store, "run_lifecycle", "run-1",
                    CancellationBudget(max_rows=200, max_bytes=1000000, max_row_bytes=100000,
                                       max_streams=20, max_nodes=20))
            assert error.value.reason == "budget"
        assert decoded == []
    finally:
        case.store.close()


def test_oversized_stream_version_table_field_is_held_without_raw_fetch(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, read_version
    with SQLiteEventStore(tmp_path / "events.db") as store:
        store._connection.execute(
            "INSERT INTO stream_versions(stream_type, stream_id, current_version) VALUES('mediation_v2', ?, ?)",
            ("candidate", "x" * 100001))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld) as error:
                read_version(store, "mediation_v2", "candidate",
                    CancellationBudget(max_rows=10, max_bytes=1000000, max_row_bytes=100000,
                                       max_streams=20, max_nodes=20))
            assert error.value.reason == "budget"


def test_request_persists_when_completion_cannot_decode_budgeted_state(tmp_path):
    from datetime import datetime, timezone
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET idempotency_key=? WHERE stream_type='run_lifecycle' AND stream_version=1",
                     ("x" * 1000000,))
        result = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert result.outcome == "held" and result.reason == "budget" and result.state is None
        event_types = [row[0] for row in conn.execute(
            "SELECT event_type FROM events WHERE stream_type='run_lifecycle' AND stream_id='run-1'")]
        assert "RunCancellationRequested" in event_types
        assert "RunCancelled" not in event_types
    finally:
        case.store.close()


def test_lifecycle_only_cancelled_attempt_cannot_finish_run_with_active_slot(tmp_path):
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        # This low-level transition is not a Scheduler settlement or release.
        case.lifecycle.record_attempt_cancelled("run-1", node_id="node-1",
            attempt_id=case.identity["attempt_id"], fencing_generation=case.identity["fencing_generation"],
            stop_receipt_hash="sha256:" + "a" * 64, causation_id="sha256:" + "b" * 64)
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "held"
        assert not any(e.event_type == "RunCancelled" for e in case.store.read_stream("run_lifecycle", "run-1"))
        assert not any(e.event_type == "AttemptSlotReleased" for e in case.store.read_stream("scheduler", "global"))
    finally:
        case.store.close()


def test_completion_crash_rolls_back_release_and_retry_succeeds(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from orchestrator.lifecycle import LifecycleController
    from orchestrator.scheduler import Scheduler
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        receipt = dict(**case.identity, stopped_at=datetime.now(timezone.utc),
                       stop_receipt_hash="sha256:" + "a" * 64,
                       no_effect_receipt_hash="sha256:" + "b" * 64)
        def crash(_run_id):
            raise RuntimeError("simulated completion crash")
        monkeypatch.setattr(case.scheduler.lifecycle, "complete_cancellation_if_idle", crash)
        with pytest.raises(RuntimeError, match="simulated completion crash"):
            case.scheduler.acknowledge_cancellation(**receipt)
        with SQLiteEventStore(tmp_path / "events.db") as other:
            releases = [e for e in other.read_stream("scheduler", "global") if e.event_type == "AttemptSlotReleased"]
            assert len(releases) == 0
            assert LifecycleController(other).replay("run-1").status == "cancelling"
            retry = Scheduler(other, limits=_application_limits())
            retry.acknowledge_cancellation(**receipt)
            assert len([e for e in other.read_stream("scheduler", "global") if e.event_type == "AttemptSlotReleased"]) == 1
            assert LifecycleController(other).replay("run-1").status == "cancelled"
            before = other.read_stream("budget", "run-1")
            retry.acknowledge_cancellation(**receipt)
            assert other.read_stream("budget", "run-1") == before
    finally:
        case.store.close()


def test_outer_rollback_discards_scheduler_release_and_completion(tmp_path):
    from datetime import datetime, timezone
    from orchestrator.lifecycle import LifecycleController
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        receipt = dict(**case.identity, stopped_at=datetime.now(timezone.utc),
                       stop_receipt_hash="sha256:" + "a" * 64,
                       no_effect_receipt_hash="sha256:" + "b" * 64)
        def outer(_events, _version):
            case.scheduler.acknowledge_cancellation(**receipt)
            assert case.scheduler.lifecycle.last_cancellation_completion.outcome == "pending_commit"
            raise RuntimeError("outer rollback")
        with pytest.raises(RuntimeError, match="outer rollback"):
            case.store.append_checked("scheduler", "global", "terminal-rollback-test", outer)
        with SQLiteEventStore(tmp_path / "events.db") as other:
            assert not any(e.event_type == "AttemptSlotReleased" for e in other.read_stream("scheduler", "global"))
            state = LifecycleController(other).replay("run-1")
            assert state.status == "cancelling"
            assert state.node("node-1").status == "running"
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["finish", "reconcile"])
def test_other_terminal_crash_rolls_back_then_retry_is_idempotent(tmp_path, monkeypatch, entry):
    from datetime import datetime, timedelta, timezone
    from orchestrator.lifecycle import LifecycleController
    from orchestrator.scheduler import Scheduler
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        at = datetime.now(timezone.utc)
        if entry == "reconcile":
            case.scheduler.finish_attempt(**case.identity, completed_at=at, outcome="outcome_unknown")
            at += timedelta(microseconds=1)
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        def invoke(scheduler):
            if entry == "finish":
                return scheduler.finish_attempt(**case.identity, completed_at=at,
                                                outcome="failed", known_no_effect=True)
            return scheduler.reconcile_attempt(**case.identity, reconciled_at=at,
                                               outcome="failed", known_no_effect=True)
        def crash(_run_id):
            raise RuntimeError("simulated completion crash")
        monkeypatch.setattr(case.scheduler.lifecycle, "complete_cancellation_if_idle", crash)
        with pytest.raises(RuntimeError, match="simulated completion crash"):
            invoke(case.scheduler)
        with SQLiteEventStore(tmp_path / "events.db") as other:
            assert not any(e.event_type == "AttemptSlotReleased" for e in other.read_stream("scheduler", "global"))
            assert LifecycleController(other).replay("run-1").status == "cancelling"
            retry = Scheduler(other, limits=_application_limits())
            invoke(retry)
            before = other.read_stream("budget", "run-1")
            assert len([e for e in other.read_stream("scheduler", "global") if e.event_type == "AttemptSlotReleased"]) == 1
            assert LifecycleController(other).replay("run-1").status == "cancelled"
            invoke(retry)
            assert other.read_stream("budget", "run-1") == before
    finally:
        case.store.close()


@pytest.mark.parametrize("entry", ["finish", "reconcile"])
def test_other_terminal_outer_rollback_discards_settlement_and_completion(tmp_path, entry):
    from datetime import datetime, timedelta, timezone
    from orchestrator.lifecycle import LifecycleController
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        at = datetime.now(timezone.utc)
        if entry == "reconcile":
            case.scheduler.finish_attempt(**case.identity, completed_at=at, outcome="outcome_unknown")
            at += timedelta(microseconds=1)
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        before = case.store.read_stream("budget", "run-1")
        def outer(_events, _version):
            if entry == "finish":
                case.scheduler.finish_attempt(**case.identity, completed_at=at,
                                              outcome="failed", known_no_effect=True)
            else:
                case.scheduler.reconcile_attempt(**case.identity, reconciled_at=at,
                                                 outcome="failed", known_no_effect=True)
            assert case.scheduler.lifecycle.last_cancellation_completion.outcome == "pending_commit"
            raise RuntimeError("outer rollback")
        with pytest.raises(RuntimeError, match="outer rollback"):
            case.store.append_checked("scheduler", "global", "terminal-rollback-test", outer)
        with SQLiteEventStore(tmp_path / "events.db") as other:
            assert other.read_stream("budget", "run-1") == before
            assert not any(e.event_type == "AttemptSlotReleased" for e in other.read_stream("scheduler", "global"))
            assert LifecycleController(other).replay("run-1").status == "cancelling"
    finally:
        case.store.close()


def test_startup_sees_no_release_after_precommit_crash(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from orchestrator.application import ControlPlaneApplication
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        receipt = dict(**case.identity, stopped_at=datetime.now(timezone.utc),
                       stop_receipt_hash="sha256:" + "a" * 64,
                       no_effect_receipt_hash="sha256:" + "b" * 64)
        def crash(_run_id):
            raise RuntimeError("simulated completion crash")
        monkeypatch.setattr(case.scheduler.lifecycle, "complete_cancellation_if_idle", crash)
        with pytest.raises(RuntimeError, match="simulated completion crash"):
            case.scheduler.acknowledge_cancellation(**receipt)
    finally:
        case.store.close()
    with ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits()) as app:
        assert app.recover_run("run-1").lifecycle.status == "cancelling"
        releases = [e for e in app._event_store.read_stream("scheduler", "global")
                    if e.event_type == "AttemptSlotReleased"]
        assert len(releases) == 0


@pytest.mark.parametrize("entry", ["finish", "cancel", "reconcile"])
def test_terminal_release_and_completion_share_single_commit(tmp_path, monkeypatch, entry):
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime, timedelta, timezone
    from threading import Event
    from orchestrator.lifecycle import LifecycleController
    from tests.unit.runtime.test_general_worker_authority import dispatch_case

    case = dispatch_case(tmp_path, register=False)
    ready, inspect, observed = Event(), Event(), Event()
    try:
        at = datetime.now(timezone.utc)
        if entry == "reconcile":
            case.scheduler.finish_attempt(**case.identity, completed_at=at, outcome="outcome_unknown")
            at += timedelta(microseconds=1)
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        before_budget = case.store.read_stream("budget", "run-1")
        def invoke():
            if entry == "finish":
                case.scheduler.finish_attempt(**case.identity, completed_at=at,
                                              outcome="failed", known_no_effect=True)
            elif entry == "cancel":
                case.scheduler.acknowledge_cancellation(**case.identity, stopped_at=at,
                    stop_receipt_hash="sha256:" + "a" * 64, no_effect_receipt_hash="sha256:" + "b" * 64)
            else:
                case.scheduler.reconcile_attempt(**case.identity, reconciled_at=at,
                                                 outcome="failed", known_no_effect=True)
        def observer():
            with SQLiteEventStore(tmp_path / "events.db") as store:
                lifecycle = LifecycleController(store)
                ready.set()
                assert inspect.wait(10)
                releases = [e for e in store.read_stream("scheduler", "global")
                            if e.event_type == "AttemptSlotReleased"]
                state = lifecycle.replay("run-1")
                observed.set()
                return len(releases), state.status
        def crash(_run_id):
            inspect.set()
            assert observed.wait(10)
            raise RuntimeError("simulated precommit crash")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(observer)
            assert ready.wait(10)
            original = case.scheduler.lifecycle.complete_cancellation_if_idle
            monkeypatch.setattr(case.scheduler.lifecycle, "complete_cancellation_if_idle", crash)
            with pytest.raises(RuntimeError, match="simulated precommit crash"):
                invoke()
            assert future.result(timeout=10) == (0, "cancelling")
            monkeypatch.setattr(case.scheduler.lifecycle, "complete_cancellation_if_idle", original)
        with SQLiteEventStore(tmp_path / "events.db") as other:
            assert other.read_stream("budget", "run-1") == before_budget
            assert not any(e.event_type == "AttemptSlotReleased" for e in other.read_stream("scheduler", "global"))
            assert LifecycleController(other).replay("run-1").status == "cancelling"
        invoke()
        with SQLiteEventStore(tmp_path / "events.db") as other:
            assert len([e for e in other.read_stream("scheduler", "global") if e.event_type == "AttemptSlotReleased"]) == 1
            assert LifecycleController(other).replay("run-1").status == "cancelled"
            before = (other.read_stream("scheduler", "global"), other.read_stream("budget", "run-1"),
                      other.read_stream("run_lifecycle", "run-1"))
        invoke()
        with SQLiteEventStore(tmp_path / "events.db") as other:
            assert (other.read_stream("scheduler", "global"), other.read_stream("budget", "run-1"),
                    other.read_stream("run_lifecycle", "run-1")) == before
    finally:
        inspect.set()
        case.store.close()


def test_budget_held_release_remains_pending_across_startup_until_explicit_retry(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    import orchestrator.lifecycle.controller as lifecycle_module
    from orchestrator.application import ControlPlaneApplication
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        monkeypatch.setattr(lifecycle_module, "MAX_CANCEL_CENSUS_WORK", 1)
        case.scheduler.acknowledge_cancellation(**case.identity, stopped_at=datetime.now(timezone.utc),
            stop_receipt_hash="sha256:" + "a" * 64, no_effect_receipt_hash="sha256:" + "b" * 64)
        assert case.scheduler.lifecycle.last_cancellation_completion.reason == "budget"
    finally:
        case.store.close()
    monkeypatch.setattr(lifecycle_module, "MAX_CANCEL_CENSUS_WORK", 100)
    with ControlPlaneApplication(tmp_path / "events.db", limits=_application_limits()) as app:
        assert app.recover_run("run-1").lifecycle.status == "cancelling"
        assert len([e for e in app._event_store.read_stream("scheduler", "global")
                    if e.event_type == "AttemptSlotReleased"]) == 1
        result = app._scheduler.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "completed"
        assert result.state.status == "cancelled"


def test_accepted_v1_tampered_release_usage_is_integrity_hold(tmp_path, monkeypatch):
    import hashlib
    import json
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, validate_v1_links, validate_v1_terminal
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import terminal_case, direct
    from tests.unit.runtime.test_verifier_process import _ChildLauncher

    original = IsolatedVerifierProcess.__init__
    def offline_verifier(self, **kwargs):
        kwargs.setdefault("launcher", _ChildLauncher())
        original(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, "__init__", offline_verifier)
    case = terminal_case(tmp_path)
    try:
        direct(case)
        accepted = case.journal.read(case.operation.identity)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        payload = json.loads(conn.execute(
            "SELECT payload_json FROM events WHERE stream_type='scheduler' AND event_type='AttemptSlotReleased'").fetchone()[0])
        payload["usage_hash"] = "sha256:" + "f" * 64
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        conn.execute("UPDATE events SET payload_json=?, payload_hash=? WHERE stream_type='scheduler' AND event_type='AttemptSlotReleased'",
                     (raw, hashlib.sha256(raw.encode()).hexdigest()))
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=2000, max_bytes=4000000, max_row_bytes=200000,
                                        max_streams=100, max_nodes=20)
            validate_v1_links(case.store, accepted, budget)
            with pytest.raises(CancellationHeld) as error:
                validate_v1_terminal(case.store, accepted, budget)
            assert error.value.reason == "integrity"
    finally:
        case.store.close()


def test_nontext_payload_is_rejected_before_decoding(tmp_path, monkeypatch):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET payload_json=CAST(payload_json AS BLOB) WHERE stream_type='run_lifecycle' AND stream_version=1")
        decoded = []
        original = SQLiteEventStore._row_to_event
        def observe(row):
            decoded.append(True)
            return original(row)
        monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
        with case.store._cancellation_proof_write():
            with pytest.raises(CancellationHeld) as error:
                read_event_window(case.store, "run_lifecycle", "run-1",
                    CancellationBudget(max_rows=200, max_bytes=1000000, max_row_bytes=100000,
                                       max_streams=20, max_nodes=20))
            assert error.value.reason == "integrity"
        assert decoded == []
    finally:
        case.store.close()


def test_missing_proof_index_is_diagnostic_integrity_hold(tmp_path):
    from datetime import datetime, timezone
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                                      outcome="failed", known_no_effect=True)
        case.store._connection.execute("DROP INDEX ix_cancel_events_run")
        state = case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert state.status == "cancelling"
        assert case.lifecycle.last_cancellation_completion.reason == "integrity"
        assert any(e.event_type == "RunCancellationRequested" for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()


@pytest.mark.parametrize("has_worker", [False, True])
@pytest.mark.parametrize("damage", ["missing", "unsettled", "oversized", "identity"])
def test_cancel_completion_requires_settled_admitted_reservation(tmp_path, has_worker, damage):
    import json
    from datetime import datetime, timezone
    from orchestrator.persistence.sqlite_event_store import _sha256_json
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=has_worker)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc), outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        if damage in {"missing", "unsettled"}:
            conn.execute("DROP TRIGGER events_immutable_delete")
            if damage == "missing":
                conn.execute("DELETE FROM events WHERE stream_type='budget' AND stream_id='run-1'")
            else:
                row = conn.execute("SELECT stream_version FROM events WHERE stream_type='budget' AND event_type='BudgetReleased'").fetchone()
                assert row is not None
                conn.execute("DELETE FROM events WHERE stream_type='budget' AND stream_version=?", (row[0],))
                conn.execute("UPDATE stream_versions SET current_version=? WHERE stream_type='budget' AND stream_id='run-1'", (row[0] - 1,))
        elif damage == "oversized":
            conn.execute("UPDATE events SET payload_json=? WHERE stream_type='budget' AND stream_version=1", ("x" * 100000,))
        else:
            conn.execute("UPDATE events SET node_id='other-node' WHERE stream_type='budget' AND event_type='BudgetReserved'")
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "held"
        events = case.store.read_stream("run_lifecycle", "run-1")
        assert any(event.event_type == "RunCancellationRequested" for event in events)
        assert not any(event.event_type == "RunCancelled" for event in events)
    finally:
        case.store.close()


@pytest.mark.parametrize("stream_type,event_type", [("run_lifecycle", "RunInitialized"), ("scheduler", "AttemptSlotReleased"), ("budget", "BudgetReserved")])
def test_cancellation_rejects_unknown_evidence_envelope(tmp_path, stream_type, event_type):
    from datetime import datetime, timezone
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc), outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET schema_version=999 WHERE stream_type=? AND event_type=?", (stream_type, event_type))
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "held"
        assert not any(e.event_type == "RunCancelled" for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()


def test_savepoint_rollback_cache_is_not_reused_by_public_completion(tmp_path):
    from datetime import datetime, timezone
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc), outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=1000, max_bytes=2000000, max_row_bytes=65536, max_streams=50, max_nodes=20)
            conn.execute("SAVEPOINT cache_experiment")
            conn.execute("UPDATE events SET event_type='ExperimentOnly' WHERE stream_type='budget' AND event_type='BudgetReleased'")
            changed, _ = read_event_window(case.store, "budget", "run-1", budget)
            assert any(e.event_type == "ExperimentOnly" for e in changed)
            conn.execute("ROLLBACK TO SAVEPOINT cache_experiment")
            conn.execute("RELEASE SAVEPOINT cache_experiment")
            counters = (budget.rows, budget.bytes, budget.streams)
            with pytest.raises(ValueError, match="rollback"):
                read_event_window(case.store, "budget", "run-1", budget)
            assert (budget.rows, budget.bytes, budget.streams) == counters
            fresh = CancellationBudget(max_rows=1000, max_bytes=2000000, max_row_bytes=65536, max_streams=50, max_nodes=20)
            restored, _ = read_event_window(case.store, "budget", "run-1", fresh)
            assert any(e.event_type == "BudgetReleased" for e in restored)
            result = case.lifecycle.complete_cancellation_if_idle("run-1")
            assert result.outcome == "pending_commit"
        assert case.lifecycle.replay("run-1").status == "cancelled"
    finally:
        case.store.close()


@pytest.mark.parametrize("dependency", ["proposal", "artifact"])
def test_multi_node_cancel_rejects_unknown_accepted_envelope(tmp_path, monkeypatch, dependency):
    from orchestrator.lifecycle import LifecycleController, NodeSpec
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import terminal_case, direct
    from tests.unit.runtime.test_verifier_process import _ChildLauncher
    original_append = LifecycleController.append_nodes
    def add_second(self, run_id, nodes, **kwargs):
        first = nodes[0]
        contract = first.planning_contract.model_copy(update={"node_id": "node-2"})
        second = NodeSpec(node_id="node-2", role=first.role, planning_contract=contract,
                          planning_contract_hash=contract.contract_hash, max_attempts=3)
        return original_append(self, run_id, (*nodes, second), **kwargs)
    monkeypatch.setattr(LifecycleController, "append_nodes", add_second)
    original_init = IsolatedVerifierProcess.__init__
    def offline(self, **kwargs):
        kwargs.setdefault("launcher", _ChildLauncher())
        original_init(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, "__init__", offline)
    case = terminal_case(tmp_path)
    try:
        direct(case)
        accepted = case.journal.read(case.operation.identity)
        stream, stream_id = (("verification_proposals", "run-1") if dependency == "proposal"
                             else ("artifact", accepted.acceptance.artifacts[0].digest))
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET schema_version=999 WHERE stream_type=? AND stream_id=?", (stream, stream_id))
        case.lifecycle.request_cancel("run-1", reason_code="user_request")
        assert case.lifecycle.last_cancellation_completion.outcome == "held"
        assert not any(e.event_type == "RunCancelled" for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()


@pytest.mark.parametrize("version", [1, 2, "mixed", 999])
def test_bounded_provider_links_preserve_supported_envelopes(tmp_path, version):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld, validate_v1_links
    from tests.unit.runtime.test_general_worker_authority import dispatch_case, captured
    case = dispatch_case(tmp_path)
    try:
        captured(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        if version == "mixed":
            conn.execute("UPDATE events SET schema_version=CASE WHEN stream_version=1 THEN 1 ELSE 2 END WHERE stream_type='provider_call'")
        else:
            conn.execute("UPDATE events SET schema_version=? WHERE stream_type='provider_call'", (version,))
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=1000, max_bytes=2000000, max_row_bytes=65536, max_streams=50, max_nodes=20)
            if version == 999:
                with pytest.raises(CancellationHeld, match="integrity"):
                    validate_v1_links(case.store, case.operation, budget)
            else:
                validate_v1_links(case.store, case.operation, budget)
    finally:
        case.store.close()


def test_cancel_reconcile_zero_committed_requires_budget_release(tmp_path):
    from datetime import datetime, timedelta, timezone
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        now = datetime.now(timezone.utc)
        case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
        case.scheduler.reconcile_attempt(**case.identity, reconciled_at=now+timedelta(seconds=1), outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        conn.execute("UPDATE events SET event_type='LostRelease' WHERE stream_type='budget' AND event_type='BudgetReleased'")
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "held"
        assert not any(e.event_type == "RunCancelled" for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()


def test_cancel_reconcile_rejects_cost_conflicting_with_zero_no_effect(tmp_path):
    import json
    from datetime import datetime, timedelta, timezone
    from orchestrator.persistence.sqlite_event_store import _sha256_json
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        now = datetime.now(timezone.utc)
        case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
        case.scheduler.reconcile_attempt(**case.identity, reconciled_at=now+timedelta(seconds=1), outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        for kind in ("CostCommitted", "BudgetReleased"):
            row = conn.execute("SELECT payload_json FROM events WHERE stream_type='budget' AND event_type=?", (kind,)).fetchone()
            payload = json.loads(row[0])
            if kind == "CostCommitted": payload["cost_minor"] = 1
            else: payload["released_minor"] = max(0, payload["released_minor"] - 1)
            conn.execute("UPDATE events SET payload_json=?, payload_hash=? WHERE stream_type='budget' AND event_type=?", (json.dumps(payload), _sha256_json(payload), kind))
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "held"
        assert not any(e.event_type == "RunCancelled" for e in case.store.read_stream("run_lifecycle", "run-1"))
    finally:
        case.store.close()


def test_cancel_reconcile_rejects_committed_fee_mismatch(tmp_path):
    import json
    from datetime import datetime, timedelta, timezone
    from orchestrator.persistence.sqlite_event_store import _sha256_json
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        now = datetime.now(timezone.utc)
        case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
        case.scheduler.reconcile_attempt(**case.identity, reconciled_at=now+timedelta(seconds=1), outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        row = conn.execute("SELECT payload_json FROM events WHERE stream_type='budget' AND event_type='CostCommitted'").fetchone()
        payload = json.loads(row[0]); payload["provider_fee_minor"] = 1
        conn.execute("UPDATE events SET payload_json=?, payload_hash=? WHERE stream_type='budget' AND event_type='CostCommitted'", (json.dumps(payload), _sha256_json(payload)))
        assert case.lifecycle.complete_cancellation_if_idle("run-1").outcome == "held"
    finally:
        case.store.close()


def test_cancel_reconcile_rejects_matching_usage_and_commit_with_wrong_currency(tmp_path):
    import json
    from datetime import datetime, timedelta, timezone
    from orchestrator.persistence.sqlite_event_store import _sha256_json
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        now = datetime.now(timezone.utc)
        case.scheduler.finish_attempt(**case.identity, completed_at=now, outcome="outcome_unknown")
        case.scheduler.reconcile_attempt(**case.identity, reconciled_at=now+timedelta(seconds=1), outcome="failed", known_no_effect=True)
        append_cancel_request(case)
        conn = case.store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        for kind in ("UsageObserved", "CostCommitted"):
            row = conn.execute("SELECT payload_json FROM events WHERE stream_type='budget' AND event_type=?", (kind,)).fetchone()
            payload = json.loads(row[0]); payload["currency"] = "EUR"
            conn.execute("UPDATE events SET payload_json=?, payload_hash=? WHERE stream_type='budget' AND event_type=?", (json.dumps(payload), _sha256_json(payload), kind))
        result = case.lifecycle.complete_cancellation_if_idle("run-1")
        assert result.outcome == "held"
        events = case.store.read_stream("run_lifecycle", "run-1")
        assert any(event.event_type == "RunCancellationRequested" for event in events)
        assert not any(event.event_type == "RunCancelled" for event in events)
    finally:
        case.store.close()


@pytest.mark.parametrize("statement", ["ROLLBACK TO nested", "ROLLBACK TRANSACTION TO SAVEPOINT nested", "/* prefix */ ROLLBACK /* x */ TO /* y */ SAVEPOINT nested"])
def test_budget_is_invalidated_by_each_owned_savepoint_rollback(tmp_path, statement):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=1000, max_bytes=2000000, max_row_bytes=65536, max_streams=50, max_nodes=20)
            read_event_window(case.store, "run_lifecycle", "run-1", budget)
            counters = (budget.rows, budget.bytes, budget.streams)
            case.store._connection.execute("SAVEPOINT nested")
            case.store._connection.execute(statement)
            case.store._connection.execute("RELEASE nested")
            with pytest.raises(ValueError, match="rollback"):
                read_event_window(case.store, "run_lifecycle", "run-1", budget)
            assert (budget.rows, budget.bytes, budget.streams) == counters
    finally:
        case.store.close()


def test_budget_cache_survives_successful_nested_savepoint_without_extra_charge(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=1000, max_bytes=2000000, max_row_bytes=65536, max_streams=50, max_nodes=20)
            original = read_event_window(case.store, "run_lifecycle", "run-1", budget)
            counters = (budget.rows, budget.bytes, budget.streams)
            with case.store._cancellation_proof_write():
                assert read_event_window(case.store, "run_lifecycle", "run-1", budget) is original
            assert read_event_window(case.store, "run_lifecycle", "run-1", budget) is original
            assert (budget.rows, budget.bytes, budget.streams) == counters
    finally:
        case.store.close()


def test_budget_cannot_cross_outer_rollback(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, read_event_window
    with SQLiteEventStore(tmp_path / "events.db") as store:
        budget = CancellationBudget(max_rows=20, max_bytes=100000, max_row_bytes=10000, max_streams=20, max_nodes=20)
        with pytest.raises(RuntimeError, match="outer abort"):
            with store._cancellation_proof_write():
                read_event_window(store, "run_lifecycle", "none", budget)
                raise RuntimeError("outer abort")
        counters = (budget.rows, budget.bytes, budget.streams)
        with store._cancellation_proof_write():
            with pytest.raises(ValueError, match="transaction changed"):
                read_event_window(store, "run_lifecycle", "none", budget)
        assert (budget.rows, budget.bytes, budget.streams) == counters


def test_budget_fails_closed_after_nested_store_scope_rolls_back(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, read_event_window
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        with case.store._cancellation_proof_write():
            budget = CancellationBudget(max_rows=1000, max_bytes=2000000, max_row_bytes=65536, max_streams=50, max_nodes=20)
            read_event_window(case.store, "run_lifecycle", "run-1", budget)
            counters = (budget.rows, budget.bytes, budget.streams)
            with pytest.raises(RuntimeError, match="nested abort"):
                with case.store._cancellation_proof_write():
                    raise RuntimeError("nested abort")
            with pytest.raises(ValueError, match="rollback"):
                read_event_window(case.store, "run_lifecycle", "run-1", budget)
            assert (budget.rows, budget.bytes, budget.streams) == counters
    finally:
        case.store.close()
