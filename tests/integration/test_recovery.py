import sqlite3
import hashlib
import json
import multiprocessing
import os
from pathlib import Path

import pytest

from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.snapshots import SnapshotStore
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.recovery import (
    BudgetInvariantFailure,
    EventChainFailure,
    RecoveryBootstrap,
    SecurityInvariantFailure,
    bootstrap_recovery,
    recover,
    recover_aggregate,
)
from orchestrator.application import ControlPlaneApplication, StartupRecoveryFailed
from tests.support.attempt_acceptance import ADMISSION, proposal_for_attempt
from tests.unit.lifecycle.test_scheduler import _proof_attempt, scheduler


def _success_proof_writer(directory, commit):
    store, control, _, request, _, kwargs = _proof_attempt(Path(directory), **ADMISSION)
    proposal = proposal_for_attempt(control, run_id="run-1", node_id="node-1",
                                    attempt_id=request.attempt_id)
    if commit:
        control.finish_verified_attempt(**kwargs, task_sha256=proposal.task_sha256)
    # Lose the response after the durable proposal or atomic acceptance commit.
    os._exit(73)


def _success_proof_startup(database, output):
    from tests.unit.lifecycle.test_scheduler import scheduler
    with SQLiteEventStore(database) as store:
        limits = scheduler(store).limits
    try:
        with ControlPlaneApplication(database, limits=limits) as app:
            output.put(("ready", app.recover_run("run-1").lifecycle.status))
    except StartupRecoveryFailed:
        output.put(("rejected", None))


def _fresh_startup(database):
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    child = context.Process(target=_success_proof_startup, args=(str(database), output))
    child.start()
    child.join(20)
    if child.is_alive():
        child.kill()
        child.join()
        pytest.fail("startup child timed out")
    assert child.exitcode == 0
    result = output.get(timeout=5)
    output.close()
    return result


def _durable_rows(database):
    with sqlite3.connect(database) as connection:
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        return {table: connection.execute('SELECT * FROM "' + table + '" ORDER BY rowid').fetchall()
                for table in tables}


def _replace_proof_payload(store, event, payload):
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    store._connection.execute("DROP TRIGGER IF EXISTS events_immutable_update")
    store._connection.execute(
        "UPDATE events SET payload_json = ?, payload_hash = ? WHERE event_id = ?",
        (encoded, hashlib.sha256(encoded.encode()).hexdigest(), event.event_id),
    )


@pytest.mark.parametrize("commit", [False, True], ids=["proposal_only", "committed_success_proof"])
def test_fresh_process_replays_proposal_only_or_committed_success_proof(tmp_path, commit):
    child = multiprocessing.get_context("spawn").Process(
        target=_success_proof_writer, args=(str(tmp_path), commit))
    child.start()
    child.join(20)
    if child.is_alive():
        child.kill()
        child.join()
        pytest.fail("proof writer child timed out")
    assert child.exitcode == 73
    database = tmp_path / "proof.db"
    before = _durable_rows(database)
    assert _fresh_startup(database) == ("ready", "succeeded" if commit else "running")
    assert _durable_rows(database) == before
    with SQLiteEventStore(database) as store:
        control = scheduler(store)
        proposals = store.read_stream("verification_proposals", "run-1")
        assert len(proposals) == 1
        terminals = [event for event in store.read_stream("scheduler", "global")
                     if event.event_type == "AttemptSlotReleased"]
        if commit:
            from datetime import timedelta
            from orchestrator.budget import UsageRecord
            from tests.unit.lifecycle.test_scheduler import NOW
            acceptance = store.read_stream("scheduler", "global")[0]
            control.finish_verified_attempt(
                run_id="run-1", node_id="node-1", attempt_id=acceptance.attempt_id,
                fencing_generation=1, completed_at=NOW + timedelta(seconds=3),
                task_sha256=proposals[0].payload["task_sha256"],
                usage=UsageRecord(run_id="run-1", reservation_id=acceptance.payload["reservation_id"],
                                  settlement_key="verified", currency="USD", cost_minor=2))
            assert len(terminals) == 1
            assert terminals[0].payload["task_sha256"] == proposals[0].payload["task_sha256"]
            assert terminals[0].payload["evidence_sha256"] == proposals[0].payload["evidence_sha256"]
        else:
            assert terminals == []
            recovered = control.recovery.recover("run-1")
            assert len(recovered.active_attempts) == 1
            assert recovered.budget.reserved_minor == 20
    assert _durable_rows(database) == before


@pytest.mark.parametrize("fault", [
    "missing-proposal", "corrupt-proposal", "corrupt-tail", "duplicate-proposal",
    "task-hash", "evidence-hash", "lifecycle-task-hash", "lifecycle-evidence-hash",
    "generation", "legacy_success", "context", "route", "admission", "expired",
    "other-proof", "rejected-proof", "duplicate-success", "orphan-success",
    "lifecycle-policy",
])
def test_startup_rejects_invalid_success_proof_without_writes(tmp_path, fault):
    store, control, _, request, _, kwargs = _proof_attempt(tmp_path, **ADMISSION)
    proposal = proposal_for_attempt(control, run_id="run-1", node_id="node-1",
                                    attempt_id=request.attempt_id)
    control.finish_verified_attempt(**kwargs, task_sha256=proposal.task_sha256)
    release = store.read_stream("scheduler", "global")[-1]
    acceptance = store.read_stream("scheduler", "global")[0]
    terminal = next(event for event in store.read_stream("run_lifecycle", "run-1")
                    if event.event_type == "AttemptCompleted")
    if fault == "missing-proposal":
        store._connection.execute("DROP TRIGGER events_immutable_delete")
        store._connection.execute("DELETE FROM events WHERE stream_type = 'verification_proposals'")
    elif fault == "corrupt-proposal":
        event = store.read_stream("verification_proposals", "run-1")[0]
        _replace_proof_payload(store, event, {**event.payload, "evidence_sha256": "sha256:" + "f" * 64})
    elif fault in {"corrupt-tail", "duplicate-proposal"}:
        event = store.read_stream("verification_proposals", "run-1")[0]
        from orchestrator.persistence import EventDraft
        draft = EventDraft("MalformedProposal", {}, run_id="run-1", node_id="node-1",
                           attempt_id=request.attempt_id, fencing_generation=1,
                           causation_id=proposal.task_sha256)
        appended = store.append("verification_proposals", "run-1", 1, [draft], "corrupt-tail")[0]
        if fault == "duplicate-proposal":
            store._connection.execute("DROP TRIGGER events_immutable_update")
            store._connection.execute(
                "UPDATE events SET event_type = ?, payload_json = ?, payload_hash = ?, "
                "idempotency_key = ?, correlation_id = ? WHERE event_id = ?",
                (event.event_type, json.dumps(event.payload, sort_keys=True, separators=(",", ":")),
                 event.payload_hash, event.idempotency_key, event.correlation_id, appended.event_id))
    elif fault in {"task-hash", "evidence-hash", "lifecycle-task-hash", "lifecycle-evidence-hash"}:
        event = terminal if fault.startswith("lifecycle-") else release
        field = "task_sha256" if "task" in fault else "evidence_sha256"
        _replace_proof_payload(store, event, {**event.payload, field: "sha256:" + "f" * 64})
    elif fault == "legacy_success":
        for event in (release, terminal):
            _replace_proof_payload(store, event, {key: value for key, value in event.payload.items()
                                                if key not in {"task_sha256", "evidence_sha256"}})
    elif fault == "generation":
        _replace_proof_payload(store, release, {**release.payload, "fencing_generation": 2})
    elif fault == "lifecycle-policy":
        event = next(event for event in store.read_stream("run_lifecycle", "run-1")
                     if event.event_type == "AttemptAccepted")
        _replace_proof_payload(store, event, {**event.payload, "attempt": {
            **event.payload["attempt"], "policy_manifest_hash": "sha256:" + "f" * 64}})
    elif fault in {"context", "route", "admission"}:
        payload = dict(acceptance.payload)
        field = {"context": "verification_context", "route": "accepted_route",
                 "admission": "verification_admission"}[fault]
        payload[field] = {**payload[field],
                          {"context": "input_manifest_hash", "route": "model_id",
                           "admission": "input_manifest_hash"}[fault]: "sha256:" + "f" * 64}
        _replace_proof_payload(store, acceptance, payload)
    elif fault == "expired":
        _replace_proof_payload(store, release, {**release.payload,
                                               "completed_at": acceptance.payload["lease_expires_at"]})
    elif fault in {"other-proof", "rejected-proof"}:
        other = proposal_for_attempt(control, run_id="run-1", node_id="node-1",
                                     attempt_id=request.attempt_id,
                                     context_changes={"graph_version": 2},
                                     outcome="rejected" if fault == "rejected-proof" else "accepted")
        for event in (release, terminal):
            _replace_proof_payload(store, event, {**event.payload, "task_sha256": other.task_sha256,
                                                 "evidence_sha256": other.evidence_sha256})
    elif fault == "duplicate-success":
        from orchestrator.persistence import EventDraft
        duplicate = store.append("scheduler", "global", 2, [EventDraft(
            "MalformedSuccess", {}, run_id=release.run_id, node_id=release.node_id,
            attempt_id=release.attempt_id, fencing_generation=release.fencing_generation,
            causation_id=release.causation_id)], "duplicate-success")[0]
        _replace_proof_payload(store, duplicate, release.payload)
        store._connection.execute("UPDATE events SET event_type = ? WHERE event_id = ?",
                                  (release.event_type, duplicate.event_id))
    elif fault == "orphan-success":
        payload = {**release.payload, "run_id": "missing-run"}
        _replace_proof_payload(store, release, payload)
        store._connection.execute("UPDATE events SET run_id = 'missing-run' WHERE event_id = ?",
                                  (release.event_id,))
    store.close()
    before = _durable_rows(tmp_path / "proof.db")
    assert _fresh_startup(tmp_path / "proof.db") == ("rejected", None)
    assert _durable_rows(tmp_path / "proof.db") == before


def test_recovery_loads_snapshot_and_replays_only_input_tail_to_planning(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    created = events.append(
        "run", "run-1", 0, [EventDraft("RunCreated", {})], "create-run-1"
    )[0]
    snapshots = SnapshotStore(database)
    snapshots.save_snapshot("run", "run-1", 1, "Created", 1, created.event_id)
    events.append(
        "run", "run-1", 1, [EventDraft("InputAccepted", {})], "input-run-1"
    )

    reduced_events = []

    def reduce_created(state, event):
        reduced_events.append(event.event_type)
        return "Created"

    def reduce_input(state, event):
        reduced_events.append(event.event_type)
        return "Planning"

    result = RecoveryBootstrap(events, snapshots).recover(
        "run",
        "run-1",
        reducers={"RunCreated": reduce_created, "InputAccepted": reduce_input},
    )

    assert result.state == "Planning"
    assert result.event_version == 2
    assert result.snapshot_used is True
    assert result.replayed_from_version == 1
    assert result.replayed_event_count == 1
    assert reduced_events == ["InputAccepted"]


def test_recovery_falls_back_to_full_replay_when_snapshot_hash_is_corrupt(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    created = events.append(
        "run", "run-1", 0, [EventDraft("RunCreated", {})], "create-run-1"
    )[0]
    snapshots = SnapshotStore(database)
    snapshots.save_snapshot("run", "run-1", 1, "Created", 1, created.event_id)
    events.append(
        "run", "run-1", 1, [EventDraft("InputAccepted", {})], "input-run-1"
    )
    snapshots.close()
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET state_hash = ? WHERE aggregate_id = ?",
            ("f" * 64, "run-1"),
        )

    reduced_events = []
    result = RecoveryBootstrap(events, SnapshotStore(database)).recover(
        "run",
        "run-1",
        reducers={
            "RunCreated": lambda state, event: reduced_events.append(event.event_type) or "Created",
            "InputAccepted": lambda state, event: reduced_events.append(event.event_type)
            or "Planning",
        },
    )

    assert result.state == "Planning"
    assert result.snapshot_used is False
    assert result.replayed_from_version == 0
    assert result.replayed_event_count == 2
    assert reduced_events == ["RunCreated", "InputAccepted"]


def test_recovery_rebuilds_projections_from_the_complete_event_stream(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append(
        "run",
        "run-1",
        0,
        [EventDraft("RunCreated", {}), EventDraft("InputAccepted", {})],
        "events-run-1",
    )

    result = RecoveryBootstrap(events).recover(
        "run",
        "run-1",
        reducers={
            "RunCreated": lambda state, event: "Created",
            "InputAccepted": lambda state, event: "Planning",
        },
        projections={
            "timeline": lambda state, event: (state or []) + [event.event_type],
        },
        projection_states={"timeline": []},
    )

    assert result.projections == {"timeline": ["RunCreated", "InputAccepted"]}


def test_recovery_raises_typed_budget_failure_before_replay(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")

    with pytest.raises(BudgetInvariantFailure) as failure:
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            reducers={"RunCreated": lambda state, event: "Created"},
            max_replay_events=0,
        )

    assert failure.value.category == "budget"


def test_recovery_raises_typed_security_failure_for_rejected_stream(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")

    with pytest.raises(SecurityInvariantFailure) as failure:
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            reducers={"RunCreated": lambda state, event: "Created"},
            security_hook=lambda event_stream: False,
        )

    assert failure.value.category == "security"


def test_recovery_raises_typed_event_chain_failure_for_a_gap(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    first = events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")[0]
    events.append("run", "run-1", 1, [EventDraft("InputAccepted", {})], "input")
    events.close()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER events_immutable_update")
        connection.execute(
            "UPDATE events SET stream_version = 3 WHERE event_id = ?",
            (first.event_id,),
        )

    reopened = SQLiteEventStore(database)
    with pytest.raises(EventChainFailure) as failure:
        RecoveryBootstrap(reopened).recover(
            "run",
            "run-1",
            reducers={
                "RunCreated": lambda state, event: "Created",
                "InputAccepted": lambda state, event: "Planning",
            },
        )

    assert failure.value.category == "event_chain"


@pytest.mark.parametrize(
    "event_type",
    [
        "UnknownEffect",
        "EffectUnknown",
        "UnknownEffectDetected",
        "EffectOutcomeUnknown",
        "OutcomeCannotBeDetermined",
        "OutcomeUnknown",
    ],
)
def test_recovery_routes_unknown_effect_outcome_events_to_findings_and_hook(
    tmp_path, event_type
):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append(
        "run",
        "run-1",
        0,
        [
            EventDraft("RunCreated", {}),
            EventDraft(event_type, {"effect_id": "effect-1"}),
        ],
        "events-run-1",
    )
    hook_calls = []

    result = RecoveryBootstrap(events).recover(
        "run",
        "run-1",
        reducers={"RunCreated": lambda state, event: "Created"},
        effect_hook=lambda state: hook_calls.append(state),
    )

    assert result.state == "Created"
    assert result.unknown_effects == ("effect-1",)
    assert hook_calls == ["Created"]


def test_unknown_effect_outcome_is_never_sent_to_a_generic_reducer(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append(
        "run",
        "run-1",
        0,
        [EventDraft("RunCreated", {}), EventDraft("OutcomeUnknown", {})],
        "events-run-1",
    )
    reduced_event_types = []

    def reducer(state, event):
        reduced_event_types.append(event.event_type)
        return state

    result = RecoveryBootstrap(events).recover("run", "run-1", reducers=reducer)

    assert reduced_event_types == ["RunCreated"]
    assert result.unknown_effects


def test_explicit_unknown_effect_reducer_can_transition_to_reconciliation(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append(
        "run",
        "run-1",
        0,
        [EventDraft("RunCreated", {}), EventDraft("OutcomeUnknown", {})],
        "events-run-1",
    )

    result = RecoveryBootstrap(events).recover(
        "run",
        "run-1",
        reducers={
            "RunCreated": lambda state, event: "Running",
            "OutcomeUnknown": lambda state, event: "AwaitingReconciliation",
        },
    )

    assert result.state == "AwaitingReconciliation"


def test_effect_hook_receives_canonical_unknown_outcome_events(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append(
        "run",
        "run-1",
        0,
        [EventDraft("RunCreated", {}), EventDraft("OutcomeUnknown", {})],
        "events-run-1",
    )
    seen_event_types = []

    def effect_hook(events):
        seen_event_types.extend(event.event_type for event in events)

    RecoveryBootstrap(events).recover(
        "run",
        "run-1",
        reducers={"RunCreated": lambda state, event: "Created"},
        effect_hook=effect_hook,
    )

    assert seen_event_types == ["OutcomeUnknown"]


def test_unknown_event_hook_failure_is_wrapped_without_leaking_its_message(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")

    def unknown_event_hook(state, event):
        raise ValueError("secret-payload-should-not-escape")

    with pytest.raises(EventChainFailure) as failure:
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            reducers={},
            unknown_event_hook=unknown_event_hook,
        )

    assert isinstance(failure.value.__cause__, ValueError)
    assert "secret-payload" not in str(failure.value)


def test_recovery_uses_an_atomic_event_stream_snapshot_reader(tmp_path):
    database = tmp_path / "recovery.db"
    source_store = SQLiteEventStore(database)
    source_events = source_store.append(
        "run", "run-1", 0, [EventDraft("RunCreated", {})], "create"
    )

    class AtomicReader:
        def read_stream_snapshot(self, aggregate_type, aggregate_id):
            return source_events, 1

        def read_stream(self, aggregate_type, aggregate_id, after_version=0):
            raise AssertionError("recovery used non-atomic stream reads")

        def current_version(self, aggregate_type, aggregate_id):
            raise AssertionError("recovery used a second version read")

    result = RecoveryBootstrap(AtomicReader()).recover(
        "run",
        "run-1",
        reducers={"RunCreated": lambda state, event: "Created"},
    )

    assert result.state == "Created"


def test_lease_hook_failure_is_wrapped_as_event_chain_failure(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")

    with pytest.raises(EventChainFailure) as failure:
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            reducers={"RunCreated": lambda state, event: "Created"},
            lease_hook=lambda state: (_ for _ in ()).throw(ValueError("secret")),
        )

    assert isinstance(failure.value.__cause__, ValueError)


def test_hook_return_iterable_failure_is_wrapped_as_event_chain_failure(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")

    def lease_hook(state):
        def findings():
            yield "lease-1"
            raise ValueError("secret")

        return findings()

    with pytest.raises(EventChainFailure) as failure:
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            reducers={"RunCreated": lambda state, event: "Created"},
            lease_hook=lease_hook,
        )

    assert isinstance(failure.value.__cause__, ValueError)


def test_hook_iterator_creation_failure_is_wrapped_as_event_chain_failure(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")

    class BrokenIterable:
        def __iter__(self):
            raise ValueError("secret")

    with pytest.raises(EventChainFailure) as failure:
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            reducers={"RunCreated": lambda state, event: "Created"},
            lease_hook=lambda state: BrokenIterable(),
        )

    assert isinstance(failure.value.__cause__, ValueError)


def test_source_less_snapshot_is_anchored_by_version_for_tail_replay(tmp_path):
    database = tmp_path / "recovery.db"
    events = SQLiteEventStore(database)
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")
    snapshots = SnapshotStore(database)
    snapshots.save("run", "run-1", state="Created", version=1)
    events.append("run", "run-1", 1, [EventDraft("InputAccepted", {})], "input")

    result = RecoveryBootstrap(events, snapshots).recover(
        "run",
        "run-1",
        reducers={"InputAccepted": lambda state, event: "Planning"},
    )

    assert result.snapshot_used is True
    assert result.replayed_from_version == 1
    assert result.replayed_event_count == 1
    assert result.state == "Planning"


def test_recovery_validates_options_and_reports_result_aliases(tmp_path):
    events = SQLiteEventStore(tmp_path / "recovery.db")
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")
    recovery = RecoveryBootstrap(events)

    with pytest.raises(ValueError, match="aggregate_type"):
        recovery.recover(" ", "run-1", lambda state, event: state)
    with pytest.raises(TypeError, match="reducers"):
        recovery.recover("run", "run-1", object())
    with pytest.raises(TypeError, match="projections"):
        recovery.recover(
            "run", "run-1", lambda state, event: state, projections=[]
        )
    with pytest.raises(TypeError, match="projection_states"):
        recovery.recover(
            "run", "run-1", lambda state, event: state, projection_states=[]
        )
    with pytest.raises(ValueError, match="snapshot_schema_version"):
        recovery.recover(
            "run", "run-1", lambda state, event: state, snapshot_schema_version=0
        )
    with pytest.raises(ValueError, match="only one of lease_hook"):
        recovery.recover(
            "run",
            "run-1",
            lambda state, event: state,
            lease_hook=lambda state: None,
            lease_checker=lambda state: None,
        )

    recovered = recovery.recover(
        "run", "run-1", lambda state, event: event.event_type
    )
    assert recovered.version == recovered.event_version == 1
    assert recovered.aggregate_state == recovered.state == "RunCreated"


def test_recovery_uses_wildcard_reducer_and_unknown_event_hook(tmp_path):
    events = SQLiteEventStore(tmp_path / "recovery.db")
    events.append(
        "run",
        "run-1",
        0,
        [EventDraft("RunCreated", {}), EventDraft("CustomEvent", {})],
        "events",
    )
    hook_calls = []

    recovered = RecoveryBootstrap(events).recover(
        "run",
        "run-1",
        {"RunCreated": lambda state, event: "created"},
        unknown_event_hook=lambda event: hook_calls.append(event.event_type),
    )
    assert recovered.state == "created"
    assert hook_calls == ["CustomEvent"]

    wildcard = RecoveryBootstrap(events).recover(
        "run", "run-1", {"*": lambda state, event: (state or []) + [event.event_type]}
    )
    assert wildcard.state == ["RunCreated", "CustomEvent"]


def test_recovery_wraps_noncallable_and_failing_reducers(tmp_path):
    events = SQLiteEventStore(tmp_path / "recovery.db")
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")
    with pytest.raises(EventChainFailure, match="reducer is not callable"):
        RecoveryBootstrap(events).recover("run", "run-1", {"RunCreated": 1})
    with pytest.raises(EventChainFailure, match="reducer failed"):
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            {"RunCreated": lambda state, event: (_ for _ in ()).throw(RuntimeError("secret"))},
        )


def test_projection_rebuild_mapping_skips_unhandled_events_and_validates_handlers(tmp_path):
    events = SQLiteEventStore(tmp_path / "recovery.db")
    events.append(
        "run",
        "run-1",
        0,
        [EventDraft("RunCreated", {}), EventDraft("InputAccepted", {})],
        "events",
    )
    result = RecoveryBootstrap(events).recover(
        "run",
        "run-1",
        lambda state, event: event.event_type,
        projections={"timeline": {"InputAccepted": lambda state, event: [event.event_type]}},
    )
    assert result.projections == {"timeline": ["InputAccepted"]}

    with pytest.raises(TypeError, match="must be callable or a mapping"):
        RecoveryBootstrap(events).recover(
            "run", "run-1", lambda state, event: state, projections={"bad": 1}
        )
    with pytest.raises(EventChainFailure, match="projection reducer is not callable"):
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            lambda state, event: state,
            projections={"bad": {"RunCreated": 1}},
        )
    with pytest.raises(EventChainFailure, match="projection rebuild failed"):
        RecoveryBootstrap(events).recover(
            "run",
            "run-1",
            lambda state, event: state,
            projections={"bad": lambda state, event: 1 / 0},
        )


def test_recovery_accepts_budget_and_security_hooks_and_wraps_failures(tmp_path):
    events = SQLiteEventStore(tmp_path / "recovery.db")
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")
    recovery = RecoveryBootstrap(events)
    assert recovery.recover(
        "run",
        "run-1",
        lambda state, event: state,
        budget=lambda event_stream: len(event_stream) == 1,
        security_hook=lambda event_stream: True,
    ).event_version == 1

    with pytest.raises(BudgetInvariantFailure, match="budget hook failed"):
        recovery.recover(
            "run",
            "run-1",
            lambda state, event: state,
            budget=lambda event_stream: 1 / 0,
        )
    with pytest.raises(SecurityInvariantFailure, match="security hook failed"):
        recovery.recover(
            "run",
            "run-1",
            lambda state, event: state,
            security_hook=lambda event_stream: 1 / 0,
        )


def test_recovery_budget_aliases_and_one_shot_helpers(tmp_path):
    events = SQLiteEventStore(tmp_path / "recovery.db")
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")
    reducer = lambda state, event: event.event_type
    for helper in (recover, bootstrap_recovery, recover_aggregate):
        result = helper(events, "run", "run-1", reducer, budget={"max_events": 1})
        assert result.state == "RunCreated"

    with pytest.raises(ValueError, match="max_replay_events"):
        RecoveryBootstrap(events).recover(
            "run", "run-1", reducer, max_replay_events=True
        )
    with pytest.raises(ValueError, match="budget replay limit"):
        RecoveryBootstrap(events).recover(
            "run", "run-1", reducer, budget={"max_events": -1}
        )
    with pytest.raises(ValueError, match="non-negative integer"):
        RecoveryBootstrap(events).recover("run", "run-1", reducer, budget=-1)


def test_recovery_hook_accepts_scalar_findings_and_rejects_broken_iterators(tmp_path):
    events = SQLiteEventStore(tmp_path / "recovery.db")
    events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")
    recovery = RecoveryBootstrap(events)
    scalar = recovery.recover(
        "run", "run-1", lambda state, event: state, lease_hook=lambda state: 42
    )
    assert scalar.expired_leases == (42,)
    string = recovery.recover(
        "run", "run-1", lambda state, event: state, lease_hook=lambda state: "lease-extra"
    )
    assert string.expired_leases == ("lease-extra",)

    class BrokenIterable:
        def __iter__(self):
            raise TypeError("not iterable after all")

    with pytest.raises(EventChainFailure, match="invariant hook failed"):
        recovery.recover(
            "run",
            "run-1",
            lambda state, event: state,
            lease_hook=lambda state: BrokenIterable(),
        )
