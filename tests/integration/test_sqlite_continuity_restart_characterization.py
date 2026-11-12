"""Real-process SQLite characterization; simulated guard is not an authority."""

import os
from pathlib import Path
import select
import shutil
import signal
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
from orchestrator.isolation._sqlite_storage_observation import (
    compare_storage_observations,
    observe_storage,
)
from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld
from orchestrator.persistence import SQLiteEventStore
from orchestrator.persistence._epoch_model import EpochModel, ModelState, assess_recovery


pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux SIGKILL and pipes")


def _budget():
    return CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100)


def _fixture(store, tmp_path):
    record = ArtifactStore(tmp_path / "artifacts", event_store=store).publish_bytes(
        b"owned-fixture", source={"run_id": "local"}, readable_scope=("local",)
    )
    with store._cancellation_proof_write():
        initialize_incremental(store, record.digest, _budget())
        install_dirty_tracking(store)
        register_digest(store, record.digest)
        rebuild_incremental(store, record.digest, _budget())
    return record


def _marker(store, digest):
    return store._connection.execute(
        "SELECT dirty FROM artifact_dirty_marker WHERE digest=?", (digest,)
    ).fetchone()[0]


def _diagnose(store, record):
    with store._cancellation_proof_write():
        return snapshot_incremental(store, record.digest, record.publication_id, _budget())


def _start(script, *args):
    return subprocess.Popen(
        [sys.executable, "-u", "-c", script, *(str(arg) for arg in args)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )


def _ready(child):
    assert select.select([child.stdout], [], [], 15)[0], (
        f"own child did not reach barrier; returncode={child.poll()}"
    )
    line = child.stdout.readline().strip()
    if line != "ready":
        # Fixtures contain only synthetic inputs. Bound any child traceback.
        output, error = child.communicate(timeout=5)
        pytest.fail(
            f"own child failed before barrier; returncode={child.returncode}; "
            f"line={line[:500]!r}; stdout={output[:500]!r}; stderr={error[:2000]!r}"
        )


def _reap(child):
    if child.poll() is None:
        child.kill()
    child.communicate(timeout=10)


def test_writer_first_blocks_rebuild_then_committed_dirty_holds(tmp_path):
    db = tmp_path / "writer-first.db"
    with SQLiteEventStore(db) as store:
        record = _fixture(store, tmp_path)
        child = _start("""import sqlite3,sys
c=sqlite3.connect(sys.argv[1],timeout=10)
c.execute('BEGIN IMMEDIATE')
assert c.execute('UPDATE artifact_incremental_publication SET version=version WHERE digest=?',(sys.argv[2],)).rowcount == 1
print('ready',flush=True)
assert sys.stdin.read(1) == 'c'
c.commit()
c.close()
""", db, record.digest)
        try:
            _ready(child)
            assert _marker(store, record.digest) == 0  # writer has not committed
            store._connection.execute("PRAGMA busy_timeout=100")
            with pytest.raises(sqlite3.OperationalError) as error:
                with store._cancellation_proof_write():
                    rebuild_incremental(store, record.digest, _budget())
            assert error.value.sqlite_errorcode in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
            assert not store._connection.in_transaction
            out, err = child.communicate(input="c", timeout=10)
            assert child.returncode == 0, (out, err)
            assert _marker(store, record.digest) == 1
            with pytest.raises(CancellationHeld):
                _diagnose(store, record)
            with store._cancellation_proof_write():
                rebuild_incremental(store, record.digest, _budget())
            assert _marker(store, record.digest) == 0
            assert _diagnose(store, record).payload["publication_id"] == record.publication_id
        finally:
            _reap(child)


def test_rebuild_first_forces_actual_busy_then_old_writer_commits_dirty(tmp_path):
    db = tmp_path / "rebuild-first.db"
    with SQLiteEventStore(db) as store:
        record = _fixture(store, tmp_path)
        store._connection.execute(
            "UPDATE artifact_dirty_marker SET dirty=1 WHERE digest=?", (record.digest,)
        )
        child = None
        try:
            with store._cancellation_proof_write():
                rebuild_incremental(store, record.digest, _budget())
                assert _marker(store, record.digest) == 0
                child = _start("""import sqlite3,sys
c=sqlite3.connect(sys.argv[1],timeout=0.1)
try:
    c.execute('BEGIN IMMEDIATE')
except sqlite3.OperationalError as e:
    assert e.sqlite_errorcode in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED), e
else:
    raise AssertionError('old writer acquired held rebuild lock')
print('busy',flush=True)
assert sys.stdin.read(1) == 'c'
c.execute('BEGIN IMMEDIATE')
assert c.execute('UPDATE artifact_incremental_publication SET version=version WHERE digest=?',(sys.argv[2],)).rowcount == 1
c.commit()
print('committed',flush=True)
c.close()
""", db, record.digest)
                assert select.select([child.stdout], [], [], 15)[0], "child did not report actual lock result"
                assert child.stdout.readline().strip() == "busy"
                assert _marker(store, record.digest) == 0
            out, err = child.communicate(input="c", timeout=10)
            assert child.returncode == 0, (out, err)
            assert out.strip() == "committed"
            assert _marker(store, record.digest) == 1
            with pytest.raises(CancellationHeld):
                _diagnose(store, record)
        finally:
            if child is not None:
                _reap(child)

def _receipt(kind):
    return {"version": 1, "kind": kind, "instance": "fixture", "epoch": "old", "generation": 1}


@pytest.mark.parametrize("committed", [False, True], ids=["before-commit", "after-commit"])
def test_owned_rebuild_sigkill_then_all_handles_closed_and_reopened(tmp_path, committed):
    db = tmp_path / "killed-rebuild.db"
    with SQLiteEventStore(db) as store:
        record = _fixture(store, tmp_path)
        expected_checkpoint = tuple(store._connection.execute(
            "SELECT * FROM artifact_incremental_checkpoint WHERE digest=?", (record.digest,)
        ).fetchone())
        expected_publications = [tuple(row) for row in store._connection.execute(
            "SELECT * FROM artifact_incremental_publication WHERE digest=? ORDER BY publication_id",
            (record.digest,),
        )]
        events = store.read_stream("artifact", record.digest)
        original_event_rows = [tuple(row) for row in store._connection.execute(
            "SELECT * FROM events WHERE stream_type=? AND stream_id=? ORDER BY stream_version",
            ("artifact", record.digest),
        )]
        original_version_rows = [tuple(row) for row in store._connection.execute(
            "SELECT * FROM stream_versions WHERE stream_type=? AND stream_id=?",
            ("artifact", record.digest),
        )]
        assert len(original_event_rows) == len(events) and len(original_version_rows) == 1
        publication = next(event for event in events if event.event_type == "ArtifactPublished")
        assert expected_publications == [
            (record.digest, record.publication_id, publication.event_id, publication.stream_version)
        ]
        assert expected_checkpoint[2:4] == (events[-1].stream_version, events[-1].event_id)
        # Corrupt only the diagnostic index/checkpoint; real history remains valid.
        store._connection.execute(
            "UPDATE artifact_incremental_publication SET version=version+100 WHERE digest=?", (record.digest,)
        )
        store._connection.execute(
            "UPDATE artifact_incremental_checkpoint SET version=version+100 WHERE digest=?", (record.digest,)
        )
        assert _marker(store, record.digest) == 1
        old_checkpoint = tuple(store._connection.execute(
            "SELECT * FROM artifact_incremental_checkpoint WHERE digest=?", (record.digest,)
        ).fetchone())
        old_publications = [tuple(row) for row in store._connection.execute(
            "SELECT * FROM artifact_incremental_publication WHERE digest=? ORDER BY publication_id",
            (record.digest,),
        )]
        assert old_checkpoint != expected_checkpoint and old_publications != expected_publications
    # Parent closed its SQLiteEventStore before the only child opens the database.
    child = _start("""import sys
from orchestrator.persistence import SQLiteEventStore
from orchestrator.artifacts.incremental_checkpoint import rebuild_incremental
from orchestrator.lifecycle.cancel_completion import CancellationBudget
with SQLiteEventStore(sys.argv[1]) as store:
    with store._cancellation_proof_write():
        rebuild_incremental(store, sys.argv[2], CancellationBudget(4000,4*1024*1024,65536,300,100))
        if sys.argv[3] == '0':
            print('ready',flush=True)
            sys.stdin.read(1)
    if sys.argv[3] == '1':
        print('ready',flush=True)
        sys.stdin.read(1)
""", db, record.digest, int(committed))
    try:
        _ready(child)
        child.send_signal(signal.SIGKILL)
        child.communicate(timeout=10)
        assert child.returncode == -signal.SIGKILL
    finally:
        _reap(child)
    # No test-owned DB handle remains open; this is a fresh connection.
    with SQLiteEventStore(db) as reopened:
        assert [tuple(row) for row in reopened._connection.execute(
            "SELECT * FROM events WHERE stream_type=? AND stream_id=? ORDER BY stream_version",
            ("artifact", record.digest),
        )] == original_event_rows
        assert [tuple(row) for row in reopened._connection.execute(
            "SELECT * FROM stream_versions WHERE stream_type=? AND stream_id=?",
            ("artifact", record.digest),
        )] == original_version_rows
        assert _marker(reopened, record.digest) == int(not committed)
        assert tuple(reopened._connection.execute(
            "SELECT * FROM artifact_incremental_checkpoint WHERE digest=?", (record.digest,)
        ).fetchone()) == (expected_checkpoint if committed else old_checkpoint)
        assert [tuple(row) for row in reopened._connection.execute(
            "SELECT * FROM artifact_incremental_publication WHERE digest=? ORDER BY publication_id",
            (record.digest,),
        )] == (expected_publications if committed else old_publications)
        if committed:
            assert _diagnose(reopened, record).payload["publication_id"] == record.publication_id
        else:
            with pytest.raises(CancellationHeld):
                _diagnose(reopened, record)
    # These are synthetic model inputs, independent of the real SQLite transaction above.
    simulated_guard = _receipt("guard_active" if committed else "guard_prepare")
    simulated_db = _receipt("db_prepared") if committed else None
    assessment = assess_recovery(simulated_guard, simulated_db)
    assert assessment.state is ModelState.UNCERTAIN
    assert assessment.restart_requires_new_epoch
    # A second, synthetic cut covers retirement before guard release. It is
    # deliberately separate from the child process and has no real guard.
    model = EpochModel()
    model.start(_receipt("guard_prepare"))
    model.mark_db_prepared(_receipt("db_prepared"))
    model.activate_simulated(_receipt("guard_active"))
    assert model.retire(_receipt("retire")) is ModelState.RETIRED
    if committed:
        assert model.guard_release_unknown(_receipt("guard_release_unknown")) is ModelState.UNCERTAIN
    assert model.restart(_receipt("restart")) is ModelState.UNCERTAIN


def test_historical_db_and_wal_copies_do_not_prove_current_binding(tmp_path):
    db = tmp_path / "source.db"
    historical = tmp_path / "historical.db"
    first_child = _start("""import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
assert c.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
c.execute('PRAGMA wal_autocheckpoint=0')
c.execute('CREATE TABLE local_history (value TEXT)')
c.execute("INSERT INTO local_history VALUES ('first-generation')")
c.commit()
print('ready',flush=True)
sys.stdin.read(1)
""", db)
    try:
        _ready(first_child)
        first_child.send_signal(signal.SIGKILL)
        first_child.communicate(timeout=10)
        assert first_child.returncode == -signal.SIGKILL
    finally:
        _reap(first_child)

    # No test-owned connection is open. Archive both files from generation one.
    source_wal = Path(str(db) + "-wal")
    old_wal = Path(str(historical) + "-wal")
    assert source_wal.is_file() and source_wal.stat().st_size > 0
    shutil.copyfile(db, historical)
    shutil.copyfile(source_wal, old_wal)
    first_wal_bytes = old_wal.read_bytes()

    # A later, independent process advances the original database and dies.
    # Only after it has exited do we inspect or copy its SQLite files.
    later_child = _start("""import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
assert c.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
c.execute('PRAGMA wal_autocheckpoint=0')
assert c.execute('SELECT value FROM local_history').fetchone()[0] == 'first-generation'
c.execute("UPDATE local_history SET value='second-generation'")
c.commit()
print('ready',flush=True)
sys.stdin.read(1)
""", db)
    try:
        _ready(later_child)
        later_child.send_signal(signal.SIGKILL)
        later_child.communicate(timeout=10)
        assert later_child.returncode == -signal.SIGKILL
    finally:
        _reap(later_child)

    assert source_wal.is_file() and source_wal.read_bytes() != first_wal_bytes
    before = observe_storage(historical)
    after = observe_storage(db)
    assert before.preflight_ok and after.preflight_ok, (
        f"storage unavailable: before={before.reason!r}/{before.findings!r}, "
        f"after={after.reason!r}/{after.findings!r}"
    )
    assert "wal_present" in before.findings and "wal_present" in after.findings
    comparison = compare_storage_observations(before, after)
    assert "db_identity_changed" in comparison.findings
    assert "wal_identity_changed" in comparison.findings
    assert "sqlite_handle_binding_unproven" in comparison.unknowns
    assert "between_samples_unobserved" in comparison.unknowns

    # The historical DB/WAL pair has genuinely older contents. Opening each
    # pair is done only after the comparison, and proves no authority.
    with sqlite3.connect(historical) as connection:
        assert connection.execute("SELECT value FROM local_history").fetchall() == [("first-generation",)]
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT value FROM local_history").fetchall() == [("second-generation",)]
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
