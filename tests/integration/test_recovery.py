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
