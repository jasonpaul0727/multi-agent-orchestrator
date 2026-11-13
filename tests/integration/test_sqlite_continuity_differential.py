"""Differential characterization only: no epoch or guard authority."""

import hashlib
import json
import sqlite3
import subprocess
import sys

import pytest

from orchestrator.artifacts import ArtifactStore
from orchestrator.artifacts.incremental_checkpoint import (
    initialize_incremental,
    rebuild_incremental,
    snapshot_incremental,
)
from orchestrator.artifacts.transactional_dirty import install_dirty_tracking, register_digest
from orchestrator.lifecycle.cancel_completion import (
    CancellationBudget,
    CancellationHeld,
    validate_v1_links,
    validate_v1_terminal,
)
from orchestrator.persistence import SQLiteEventStore
from orchestrator.persistence._epoch_model import EpochModel, ModelState


def _budget():
    return CancellationBudget(4000, 4_000_000, 200_000, 300, 100)


def _receipt(kind, epoch="old", generation=1, **extra):
    return {
        "version": 1, "kind": kind, "instance": "simulation",
        "epoch": epoch, "generation": generation, **extra,
    }


def _initialize(store, digests):
    with store._cancellation_proof_write():
        for digest in digests:
            initialize_incremental(store, digest, _budget())
        install_dirty_tracking(store)
        for digest in digests:
            register_digest(store, digest)
            rebuild_incremental(store, digest, _budget())


def _marker(store, digest):
    return store._connection.execute(
        "SELECT dirty FROM artifact_dirty_marker WHERE digest=?", (digest,)
    ).fetchone()[0]


@pytest.mark.skipif(sys.platform != "linux", reason="requires independent Linux SQLite process")
def test_simulated_retirement_gap_allows_real_legacy_writer_then_requires_rebuild(tmp_path):
    db = tmp_path / "retirement.db"
    with SQLiteEventStore(db) as store:
        artifacts = ArtifactStore(tmp_path / "artifacts", event_store=store)
        a = artifacts.publish_bytes(b"shared", source={"run_id": "target"}, readable_scope=("target",))
        other = artifacts.publish_bytes(b"shared", source={"run_id": "other-private"}, readable_scope=("other-private",))
        separate = artifacts.publish_bytes(b"different", source={"run_id": "separate"}, readable_scope=("separate",))
        assert a.digest == other.digest != separate.digest
        _initialize(store, (a.digest, separate.digest))
        separate_index = [tuple(row) for row in store._connection.execute(
            "SELECT * FROM artifact_incremental_publication WHERE digest=?", (separate.digest,)
        )]

    # The model is intentionally synthetic: these records do not supervise the writer.
    model = EpochModel()
    model.start(_receipt("guard_prepare"))
    model.mark_db_prepared(_receipt("db_prepared"))
    model.activate_simulated(_receipt("guard_active"))
    for digest in (a.digest, separate.digest):
        model.digest_register(_receipt("digest_register", digest=digest.removeprefix("sha256:")))
        model.digest_rebuild_simulated(_receipt("digest_rebuild_simulated", digest=digest.removeprefix("sha256:")))
    assert model.retire(_receipt("retire")) is ModelState.RETIRED
    assert model.guard_release(_receipt("guard_release")) is ModelState.RETIRED

    # No parent DB connection remains open. A real independent legacy writer
    # alters only the non-target publication and resets the marker using TEMP.
    script = """import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
c.execute("CREATE TEMP TRIGGER reset_marker AFTER UPDATE ON main.artifact_incremental_publication BEGIN UPDATE artifact_dirty_marker SET dirty=0 WHERE digest=NEW.digest; END")
assert c.execute("UPDATE artifact_incremental_publication SET version=version+100 WHERE digest=? AND publication_id=?", (sys.argv[2],sys.argv[3])).rowcount == 1
c.commit()
assert c.execute('SELECT dirty FROM artifact_dirty_marker WHERE digest=?', (sys.argv[2],)).fetchone()[0] == 0
c.close()
"""
    child = subprocess.run(
        [sys.executable, "-c", script, str(db), a.digest, other.publication_id],
        capture_output=True, text=True, timeout=15,
    )
    assert child.returncode == 0, child.stderr[-2000:]
    assert model.restart(_receipt("restart")) is ModelState.ABSENT
    assert model.restart_pending
    model.start(_receipt("guard_prepare", "new", 2))
    model.mark_db_prepared(_receipt("db_prepared", "new", 2))
    assert model.activate_simulated(_receipt("guard_active", "new", 2)) is ModelState.ACTIVE_SIMULATED
    assert model.registered_digests_rebuild_required_simulated == 2

    with SQLiteEventStore(db) as reopened:
        assert _marker(reopened, a.digest) == 0  # stale marker is not evidence
        assert reopened._connection.execute(
            "SELECT version FROM artifact_incremental_publication WHERE digest=? AND publication_id=?",
            (a.digest, other.publication_id),
        ).fetchone()[0] > 100
        with reopened._cancellation_proof_write():
            observed = snapshot_incremental(reopened, a.digest, a.publication_id, _budget())
        assert observed.payload["publication_id"] == a.publication_id
        assert other.publication_id not in str(observed.payload)
        with reopened._cancellation_proof_write():
            rebuild_incremental(reopened, a.digest, _budget())
        model.digest_rebuild_simulated(_receipt(
            "digest_rebuild_simulated", "new", 2, digest=a.digest.removeprefix("sha256:")
        ))
        assert model.registered_digests_rebuild_required_simulated == 1
        actual = [tuple(row) for row in reopened._connection.execute(
            "SELECT publication_id,event_id,version FROM artifact_incremental_publication WHERE digest=? ORDER BY version",
            (a.digest,),
        )]
        published = [event for event in reopened.read_stream("artifact", a.digest)
                     if event.event_type == "ArtifactPublished"]
        assert actual == [(event.payload["publication_id"], event.event_id, event.stream_version)
                          for event in published]
        assert _marker(reopened, separate.digest) == 0
        assert [tuple(row) for row in reopened._connection.execute(
            "SELECT * FROM artifact_incremental_publication WHERE digest=?", (separate.digest,)
        )] == separate_index


@pytest.fixture
def offline_terminal(tmp_path, monkeypatch):
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import direct, terminal_case
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
        assert accepted.phase == "accepted"
        yield case, accepted
    finally:
        case.store.close()


def _full_v1(store, accepted, budget):
    with store._cancellation_proof_write():
        validate_v1_links(store, accepted, budget)
        return validate_v1_terminal(store, accepted, budget)


def _rebuild(store, digest, budget):
    with store._cancellation_proof_write():
        return rebuild_incremental(store, digest, budget)


def _modify_target_history(store, digest, damage):
    conn = store._connection
    if damage in ("malformed_before", "bad_hash", "missing_event", "unknown_schema", "gap", "storage_type", "invalid_json", "oversize_row"):
        trigger = "events_immutable_delete" if damage == "missing_event" else "events_immutable_update"
        ddl = conn.execute("SELECT sql FROM sqlite_master WHERE name=?", (trigger,)).fetchone()[0]
        conn.execute("DROP TRIGGER " + trigger)
        try:
            if damage == "missing_event":
                conn.execute("DELETE FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (digest,))
            elif damage == "bad_hash":
                conn.execute("UPDATE events SET payload_hash=? WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", ("0" * 64, digest))
            elif damage == "unknown_schema":
                conn.execute("UPDATE events SET schema_version=2 WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (digest,))
            elif damage == "gap":
                conn.execute("UPDATE events SET stream_version=3 WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (digest,))
            elif damage == "storage_type":
                conn.execute("UPDATE events SET idempotency_key=? WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (sqlite3.Binary(b"wrong-sqlite-type"), digest))
            elif damage == "invalid_json":
                raw = '{"bad"'
                conn.execute("UPDATE events SET payload_json=?,payload_hash=? WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (raw, hashlib.sha256(raw.encode()).hexdigest(), digest))
            elif damage == "oversize_row":
                conn.execute("UPDATE events SET idempotency_key=? WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", ("x" * 210000, digest))
            else:
                row = conn.execute("SELECT payload_json FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (digest,)).fetchone()
                payload = json.loads(row[0])
                payload["size"] = -1
                raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                conn.execute("UPDATE events SET event_type='ArtifactPublished',payload_json=?,payload_hash=? WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (raw, hashlib.sha256(raw.encode()).hexdigest(), digest))
        finally:
            conn.execute(ddl)
    elif damage in ("malformed_after", "tail", "duplicate_publication"):
        row = conn.execute("SELECT payload_json FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=2", (digest,)).fetchone()
        payload = json.loads(row[0])
        if damage != "duplicate_publication":
            payload.update(size=-1, publication_id="synthetic-later")
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        version = 999 if damage == "tail" else 3
        if damage != "tail":
            conn.execute("UPDATE stream_versions SET current_version=3 WHERE stream_type='artifact' AND stream_id=?", (digest,))
        conn.execute("""INSERT INTO events SELECT 'synthetic-later',stream_type,stream_id,?,
            'ArtifactPublished',schema_version,occurred_at,?,?,'synthetic-later',run_id,node_id,
            attempt_id,fencing_generation,correlation_id,causation_id FROM events
            WHERE stream_type='artifact' AND stream_id=? AND stream_version=2""",
            (version, raw, hashlib.sha256(raw.encode()).hexdigest(), digest))
    elif damage == "orphan":
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DELETE FROM stream_versions WHERE stream_type='artifact' AND stream_id=?", (digest,))
    elif damage == "long_schema":
        conn.execute('CREATE INDEX "' + ('q' * 70000) + '" ON events(stream_id)')
    else:
        raise AssertionError(damage)


@pytest.mark.parametrize(("damage", "full_ok", "rebuild_ok"), [
    ("valid", True, True),
    ("malformed_before", False, False),
    ("malformed_after", True, False),  # full v1 stops parsing after target
    ("missing_event", False, False),
    ("unknown_schema", False, False),
    ("gap", False, False),
    ("storage_type", False, False),
    ("invalid_json", False, False),
    ("oversize_row", False, False),
    ("bad_hash", False, False),
    ("tail", False, False),
    ("duplicate_publication", True, False),  # target found before later duplicate
    ("orphan", False, False),
    ("long_schema", True, False),  # diagnostic preflights schema, v1 does not
])
def test_real_v1_and_diagnostic_rebuild_differential(offline_terminal, damage, full_ok, rebuild_ok):
    case, accepted = offline_terminal
    digest = accepted.capture.response_artifact.digest
    _initialize(case.store, (digest,))
    if damage != "valid":
        _modify_target_history(case.store, digest, damage)
        # A schema-only change need not mark a digest dirty.
        assert _marker(case.store, digest) == (0 if damage == "long_schema" else 1)
    full_budget, rebuild_budget = _budget(), _budget()
    if full_ok:
        result = _full_v1(case.store, accepted, full_budget)
        assert result.event_type == "AttemptSlotReleased"
    else:
        with pytest.raises(CancellationHeld):
            _full_v1(case.store, accepted, full_budget)
    if rebuild_ok:
        _rebuild(case.store, digest, rebuild_budget)
        assert _marker(case.store, digest) == 0
    else:
        with pytest.raises(CancellationHeld):
            _rebuild(case.store, digest, rebuild_budget)
        assert _marker(case.store, digest) == (0 if damage == "long_schema" else 1)
    assert full_budget.rows > 0 and full_budget.bytes > 0
    assert rebuild_budget.rows > 0 and rebuild_budget.bytes > 0


def test_v1_and_rebuild_have_independent_real_budget_limits(offline_terminal):
    case, accepted = offline_terminal
    digest = accepted.capture.response_artifact.digest
    _initialize(case.store, (digest,))
    # v1 and the diagnostic consume independent budgets over different scopes.
    full_budget = CancellationBudget(1, 1024, 1024, 1, 1)
    rebuild_budget = CancellationBudget(1, 1024, 1024, 1, 1)
    with pytest.raises(CancellationHeld) as full_failure:
        _full_v1(case.store, accepted, full_budget)
    with pytest.raises(CancellationHeld) as rebuild_failure:
        _rebuild(case.store, digest, rebuild_budget)
    assert full_failure.value.reason == rebuild_failure.value.reason == "budget"
    assert full_budget.rows <= full_budget.max_rows
    assert rebuild_budget.rows <= rebuild_budget.max_rows


@pytest.mark.parametrize("other_count", [1, 3])
def test_v1_and_rebuild_preserve_other_run_publication_and_digest_privacy(offline_terminal, tmp_path, other_count):
    case, accepted = offline_terminal
    artifacts = ArtifactStore(tmp_path / "artifacts", event_store=case.store)
    output_digest = accepted.acceptance.artifacts[0].digest
    others = [
        artifacts.publish_bytes(
            b"terminal model text", artifact_type="text", media_type="text/plain; charset=utf-8",
            source={"run_id": f"other-private-run-{n}"}, readable_scope=(f"other-private-run-{n}",),
        )
        for n in range(other_count)
    ]
    separate = artifacts.publish_bytes(
        b"separate-private-content", source={"run_id": "separate-private"},
        readable_scope=("separate-private",),
    )
    assert all(other.digest == output_digest for other in others)
    assert output_digest != separate.digest
    _initialize(case.store, (output_digest, separate.digest))
    unrelated_before = [tuple(row) for row in case.store._connection.execute(
        "SELECT * FROM artifact_incremental_publication WHERE digest=?", (separate.digest,)
    )]
    release = _full_v1(case.store, accepted, _budget())
    assert release.event_type == "AttemptSlotReleased"
    assert "other-private-run" not in str(release.payload)
    _rebuild(case.store, output_digest, _budget())
    with case.store._cancellation_proof_write():
        target = snapshot_incremental(
            case.store, output_digest, accepted.acceptance.output_publication_id, _budget()
        )
    assert target.payload["publication_id"] == accepted.acceptance.output_publication_id
    assert all(other.publication_id not in str(target.payload) for other in others)
    assert "other-private-run" not in str(target.payload)
    assert _marker(case.store, separate.digest) == 0
    assert [tuple(row) for row in case.store._connection.execute(
        "SELECT * FROM artifact_incremental_publication WHERE digest=?", (separate.digest,)
    )] == unrelated_before


def test_rebuild_malformed_marker_schema_remains_integrity_held(offline_terminal):
    case, accepted = offline_terminal
    digest = accepted.capture.response_artifact.digest
    _initialize(case.store, (digest,))
    case.store._connection.execute("DELETE FROM artifact_dirty_schema")
    with pytest.raises(CancellationHeld) as failure:
        _rebuild(case.store, digest, _budget())
    assert failure.value.reason == "integrity"


def test_rebuild_exhausts_rows_in_physical_history_scan_without_changing_dirty_state(offline_terminal, monkeypatch):
    from orchestrator.artifacts import evidence_checkpoint, incremental_checkpoint
    from orchestrator.artifacts.transactional_dirty import require_clean_marker

    case, accepted = offline_terminal
    store = case.store
    digest = accepted.capture.response_artifact.digest
    _initialize(store, (digest,))
    store._connection.execute("UPDATE artifact_dirty_marker SET dirty=1 WHERE digest=?", (digest,))
    before_checkpoint = tuple(store._connection.execute(
        "SELECT * FROM artifact_incremental_checkpoint WHERE digest=?", (digest,)
    ).fetchone())
    before_index = [tuple(row) for row in store._connection.execute(
        "SELECT * FROM artifact_incremental_publication WHERE digest=? ORDER BY publication_id", (digest,)
    )]
    # Measure actual preflight costs, then leave exactly one row for the real
    # history reader after its two-row overhead: fixture has two event rows.
    probe = _budget()
    with store._cancellation_proof_write():
        probe.bind(store)
        require_clean_marker(store._connection, digest, probe, allow_dirty=True, require_tracking=True)
        assert incremental_checkpoint._structure(store._connection, probe)
    limited = CancellationBudget(probe.rows + 3, 4_000_000, 200_000, 300, 100)
    entered = []
    real_reader = evidence_checkpoint.read_event_window

    def observe_reader(current_store, stream_type, stream_id, current_budget):
        entered.append((stream_type, stream_id, current_budget.rows))
        return real_reader(current_store, stream_type, stream_id, current_budget)

    monkeypatch.setattr(evidence_checkpoint, "read_event_window", observe_reader)
    with pytest.raises(CancellationHeld) as failure:
        _rebuild(store, digest, limited)
    assert failure.value.reason == "budget"
    assert entered == [("artifact", digest, probe.rows + 2)]
    assert limited.streams == 1  # physical metadata scan began
    assert _marker(store, digest) == 1
    assert tuple(store._connection.execute(
        "SELECT * FROM artifact_incremental_checkpoint WHERE digest=?", (digest,)
    ).fetchone()) == before_checkpoint
    assert [tuple(row) for row in store._connection.execute(
        "SELECT * FROM artifact_incremental_publication WHERE digest=? ORDER BY publication_id", (digest,)
    )] == before_index


def test_rebuild_sqlite_error_remains_integrity_held(offline_terminal):
    case, accepted = offline_terminal
    digest = accepted.capture.response_artifact.digest
    _initialize(case.store, (digest,))
    conn = case.store._connection

    def deny_schema_read(action, table, column, database, source):
        if action == sqlite3.SQLITE_READ and table == "artifact_dirty_schema":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(deny_schema_read)
    try:
        with pytest.raises(CancellationHeld) as failure:
            _rebuild(case.store, digest, _budget())
    finally:
        conn.set_authorizer(None)
    assert failure.value.reason == "integrity"


@pytest.mark.parametrize("binding", [
    "source", "readable_scope", "size", "artifact_type", "media_type", "publication_id",
])
def test_valid_artifact_record_with_wrong_accepted_binding_can_rebuild_but_full_v1_holds(
    offline_terminal, binding
):
    case, accepted = offline_terminal
    store = case.store
    digest = accepted.capture.response_artifact.digest
    _initialize(store, (digest,))
    conn = store._connection
    row = conn.execute(
        "SELECT payload_json FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=2",
        (digest,),
    ).fetchone()
    payload = json.loads(row[0])
    if binding == "source":
        payload["source"]["run_id"] = "wrong-synthetic-run"
    elif binding == "readable_scope":
        payload["readable_scope"] = ["wrong-synthetic-scope"]
    elif binding == "size":
        payload["size"] += 1
    elif binding == "artifact_type":
        payload["type"] = "wrong-synthetic-type"
    elif binding == "media_type":
        payload["media_type"] = "application/x-synthetic-wrong"
    elif binding == "publication_id":
        payload["publication_id"] = "wrong-synthetic-publication"
    else:
        raise AssertionError(binding)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    ddl = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='events_immutable_update'"
    ).fetchone()[0]
    conn.execute("DROP TRIGGER events_immutable_update")
    try:
        conn.execute(
            "UPDATE events SET payload_json=?, payload_hash=? "
            "WHERE stream_type='artifact' AND stream_id=? AND stream_version=2",
            (raw, hashlib.sha256(raw.encode()).hexdigest(), digest),
        )
    finally:
        conn.execute(ddl)
    assert _marker(store, digest) == 1

    # The genuine v1 path applies the accepted-operation binding; a syntactically
    # valid artifact record is insufficient. This is an artifact-local failure.
    with store._cancellation_proof_write():
        budget = _budget()
        validate_v1_links(store, accepted, budget)
        with pytest.raises(CancellationHeld) as failure:
            validate_v1_terminal(store, accepted, budget)
    assert failure.value.reason == "integrity"

    # Rebuild only checks generic record/history validity. It can report clean
    # on these cases; neither the clean marker nor lookup grants authorization.
    _rebuild(store, digest, _budget())
    assert _marker(store, digest) == 0
    with store._cancellation_proof_write():
        event = snapshot_incremental(store, digest, payload["publication_id"], _budget())
    generic_record = ArtifactStore._record_from_event(event, digest)
    assert generic_record.publication_id == payload["publication_id"]
    assert event.payload == payload


def test_v1_and_rebuild_exhaust_total_bytes_at_target_history(offline_terminal, monkeypatch):
    from orchestrator.artifacts import evidence_checkpoint, incremental_checkpoint
    from orchestrator.artifacts.transactional_dirty import require_clean_marker
    from orchestrator.lifecycle import cancel_completion

    case, accepted = offline_terminal
    store = case.store
    digest = accepted.capture.response_artifact.digest
    _initialize(store, (digest,))

    # Observe the v1 budget immediately before its target artifact read.
    actual_reader = cancel_completion.read_event_window
    full_entry = []
    def observe_full(current_store, stream_type, stream_id, current_budget, **kwargs):
        if stream_type == "artifact" and stream_id == digest:
            full_entry.append(current_budget.bytes)
        return actual_reader(current_store, stream_type, stream_id, current_budget, **kwargs)
    monkeypatch.setattr(cancel_completion, "read_event_window", observe_full)
    assert _full_v1(store, accepted, _budget()).event_type == "AttemptSlotReleased"
    assert len(full_entry) == 1
    limit = CancellationBudget(4000, full_entry[0] + 32, 200_000, 300, 100)
    full_entry.clear()
    with pytest.raises(CancellationHeld) as full_failure:
        _full_v1(store, accepted, limit)
    assert full_failure.value.reason == "budget"
    assert len(full_entry) == 1

    # Measure diagnostic metadata cost and allow the reader overhead but not
    # the full target history bytes, with row limits held ample.
    probe = _budget()
    with store._cancellation_proof_write():
        probe.bind(store)
        require_clean_marker(store._connection, digest, probe, allow_dirty=True, require_tracking=True)
        assert incremental_checkpoint._structure(store._connection, probe)
    history_entry = []
    actual_diagnostic_reader = evidence_checkpoint.read_event_window
    def observe_diagnostic(current_store, stream_type, stream_id, current_budget):
        history_entry.append((stream_type, stream_id))
        return actual_diagnostic_reader(current_store, stream_type, stream_id, current_budget)
    monkeypatch.setattr(evidence_checkpoint, "read_event_window", observe_diagnostic)
    limited_rebuild = CancellationBudget(4000, probe.bytes + 512 + 32, 200_000, 300, 100)
    with pytest.raises(CancellationHeld) as rebuild_failure:
        _rebuild(store, digest, limited_rebuild)
    assert rebuild_failure.value.reason == "budget"
    assert history_entry == [("artifact", digest)]
    assert limited_rebuild.streams == 1
