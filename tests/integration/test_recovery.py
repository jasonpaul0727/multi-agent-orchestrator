import sqlite3

import pytest

from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.snapshots import SnapshotStore
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.recovery import (
    BudgetInvariantFailure,
    EventChainFailure,
    RecoveryBootstrap,
    SecurityInvariantFailure,
)


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
