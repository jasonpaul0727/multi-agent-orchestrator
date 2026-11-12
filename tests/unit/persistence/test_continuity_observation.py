import hashlib
import importlib
import importlib.util
import json
import sqlite3
import subprocess
import sys

import pytest

from orchestrator.artifacts import ArtifactStore
from orchestrator.artifacts.incremental_checkpoint import initialize_incremental
from orchestrator.artifacts.transactional_dirty import install_dirty_tracking, register_digest
from orchestrator.lifecycle.cancel_completion import CancellationBudget
from orchestrator.persistence import SQLiteEventStore


def observation_module():
    name = 'orchestrator.persistence._continuity_observation'
    assert importlib.util.find_spec(name) is not None, 'schema observation implementation missing'
    return importlib.import_module(name)


def state(path):
    with sqlite3.connect(path) as conn:
        return (conn.execute('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').fetchall(),
                conn.execute('SELECT count(*) FROM events').fetchone())


def test_observe_real_schema_reports_known_triggers_and_unknown_continuity(tmp_path):
    module = observation_module()
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        artifact = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'sample', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, artifact.digest, CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100))
            install_dirty_tracking(store)
            register_digest(store, artifact.digest)
        before = state(db)
        result = module._run_schema_observation(db, module.ObservationLimits())
        assert ('trigger', 'artifact_dirty_stream_versions_update') in {(o.kind, o.name) for o in result.objects}
        index = next(o for o in result.objects if o.name == 'sqlite_autoindex_artifact_dirty_marker_1')
        assert index.kind == 'index' and index.unique is True
        trigger = next(o for o in result.objects if o.name == 'artifact_dirty_stream_versions_update')
        ddl = store._connection.execute("SELECT sql FROM sqlite_master WHERE name=?", (trigger.name,)).fetchone()[0]
        assert trigger.ddl_identity == hashlib.sha256(b'text:' + ddl.encode()).hexdigest()
        assert all(len(o.ddl_identity) == 64 for o in result.objects)
        assert 'writer_inventory_unproven' in result.reasons
        assert 'cross_connection_temp_unobservable' in result.reasons
        assert state(db) == before
    legacy = tmp_path / 'legacy.db'
    with SQLiteEventStore(legacy):
        before = state(legacy)
        result = module._run_schema_observation(legacy, module.ObservationLimits())
        assert not any(o.name == 'artifact_dirty_marker' for o in result.objects)
        assert state(legacy) == before


def test_schema_snapshot_rejects_small_budget_without_sql_mutation(tmp_path):
    module = observation_module()
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db):
        before = state(db)
        result = module._run_schema_observation(db, module.ObservationLimits(max_rows=1, max_bytes=128, max_queries=3))
        assert 'budget_exhausted' in result.reasons
        assert result.objects == ()
        assert state(db) == before


def test_cli_reports_distinct_schema_findings_without_migrating_database(tmp_path):
    valid, altered = tmp_path / 'valid.db', tmp_path / 'secret-path.db'
    for path in (valid, altered):
        with SQLiteEventStore(path):
            pass
    with sqlite3.connect(altered) as conn:
        conn.execute('DROP TRIGGER events_immutable_update')
    before = [state(p) for p in (valid, altered)]
    outputs = []
    for path in (valid, altered):
        proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity', '--database', str(path)],
                              capture_output=True, text=True, timeout=15)
        assert proc.returncode == 0, proc.stderr
        assert str(path) not in proc.stdout + proc.stderr
        assert len(proc.stdout) < 16384
        report = json.loads(proc.stdout)
        assert report['non_authoritative'] is True
        assert 'storage_not_observed' in report['unknowns']
        assert 'writer_inventory_unproven' in report['unknowns']
        assert 'cross_connection_temp_unobservable' in report['unknowns']
        outputs.append(report)
    assert 'events_immutable_update_name_present' in outputs[0]['findings']
    assert 'events_immutable_update_name_missing' in outputs[1]['findings']
    assert [state(p) for p in (valid, altered)] == before



def test_schema_snapshot_uses_one_owned_readonly_transaction(tmp_path, monkeypatch):
    module = observation_module()
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db):
        pass
    real_connect = sqlite3.connect
    connections = []

    class ObservedConnection(sqlite3.Connection):
        def execute(self, sql, *args):
            self.commands.append(sql)
            return super().execute(sql, *args)

        def rollback(self):
            self.rolled_back = self.in_transaction
            return super().rollback()

        def close(self):
            self.closed_by_observer = True
            return super().close()

    def connect(*args, **kwargs):
        assert kwargs.get('uri') is True
        assert '?mode=ro' in args[0]
        conn = real_connect(*args, factory=ObservedConnection, **kwargs)
        conn.commands = []
        conn.closed_by_observer = False
        conn.rolled_back = False
        connections.append(conn)
        return conn

    monkeypatch.setattr(module.sqlite3, 'connect', connect)
    module._observe_in_process(db, module.ObservationLimits())
    assert len(connections) == 1
    assert connections[0].commands[0] == 'BEGIN'
    assert connections[0].rolled_back
    assert connections[0].closed_by_observer


def test_cli_missing_database_is_redacted_and_does_not_create_file(tmp_path):
    path = tmp_path / 'secret-missing.db'
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity', '--database', str(path)],
                          capture_output=True, text=True, timeout=15)
    assert proc.returncode != 0
    assert str(path) not in proc.stdout + proc.stderr
    assert 'database_unavailable' in json.loads(proc.stdout)['findings']
    assert not path.exists()



def test_snapshot_reads_live_wal_without_immutable_shortcut(tmp_path):
    module = observation_module()
    db = tmp_path / 'wal.db'
    conn = sqlite3.connect(db)
    try:
        assert conn.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        conn.execute('PRAGMA wal_autocheckpoint=0')
        conn.execute('CREATE TABLE wal_only (x)')
        conn.commit()
        db_bytes = db.read_bytes()
        wal = tmp_path / 'wal.db-wal'
        wal_bytes = wal.read_bytes()
        result = module._run_schema_observation(db, module.ObservationLimits())
        assert any(o.kind == 'table' and o.name == 'wal_only' for o in result.objects)
        assert db.read_bytes() == db_bytes
        assert wal.read_bytes() == wal_bytes
        # SHM can be used or changed by SQLite; no write-free claim is made.
    finally:
        conn.close()


def test_snapshot_cleanup_error_is_redacted_and_not_a_success(tmp_path, monkeypatch):
    module = observation_module()
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db):
        pass
    real_connect = sqlite3.connect
    connections = []

    class BrokenRollback(sqlite3.Connection):
        def rollback(self):
            raise sqlite3.OperationalError('secret path from VFS')

        def close(self):
            self.was_closed = True
            return super().close()

    def connect(*args, **kwargs):
        conn = real_connect(*args, factory=BrokenRollback, **kwargs)
        conn.was_closed = False
        connections.append(conn)
        return conn

    monkeypatch.setattr(module.sqlite3, 'connect', connect)
    result = module._observe_in_process(db, module.ObservationLimits())
    assert result.objects == ()
    assert 'database_unavailable' in result.reasons
    assert connections[0].was_closed



def test_runner_rejects_external_connection_and_preserves_caller_hooks(tmp_path):
    import pytest
    module = observation_module()
    assert hasattr(module, 'ObservationLimits') and hasattr(module, '_run_schema_observation')
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db):
        pass
    conn = sqlite3.connect(db, isolation_level=None)
    traced, authorized = [], []
    conn.set_trace_callback(traced.append)
    def authorizer(action, arg1, arg2, database, source):
        authorized.append(action)
        return sqlite3.SQLITE_OK
    conn.set_authorizer(authorizer)
    conn.execute('BEGIN')
    traced.clear()
    authorized.clear()
    try:
        with pytest.raises(TypeError):
            module._run_schema_observation(conn, module.ObservationLimits())
        assert traced == [] and authorized == []
        conn.execute('SELECT count(*) FROM events').fetchone()
        assert traced and authorized
    finally:
        conn.rollback()
        conn.close()


def test_caller_commit_begin_cannot_replace_private_snapshot(tmp_path, monkeypatch):
    module = observation_module()
    assert hasattr(module, 'ObservationLimits') and hasattr(module, '_run_schema_observation')
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db):
        pass
    caller = sqlite3.connect(db, isolation_level=None)
    caller.execute('BEGIN')
    caller.execute('SELECT count(*) FROM events').fetchone()
    real_connect = sqlite3.connect
    seen = []

    class PrivateConnection(sqlite3.Connection):
        def execute(self, sql, *args):
            value = super().execute(sql, *args)
            if sql == 'BEGIN':
                caller.execute('COMMIT')
                caller.execute('BEGIN')
                seen.append((caller.in_transaction, self.in_transaction, caller is self))
            return value

    def connect(*args, **kwargs):
        return real_connect(*args, factory=PrivateConnection, **kwargs)

    monkeypatch.setattr(module.sqlite3, 'connect', connect)
    try:
        result = module._observe_in_process(db, module.ObservationLimits())
        assert seen == [(True, True, False)]
        assert any(o.name == 'events' for o in result.objects)
        assert 'writer_inventory_unproven' in result.reasons
    finally:
        caller.rollback()
        caller.close()


def test_private_connection_closes_on_failure(tmp_path, monkeypatch):
    module = observation_module()
    assert hasattr(module, 'ObservationLimits') and hasattr(module, '_run_schema_observation')
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db):
        pass
    real_connect = sqlite3.connect
    connections = []

    class PrivateConnection(sqlite3.Connection):
        def execute(self, sql, *args):
            if sql == 'BEGIN':
                raise sqlite3.OperationalError('private path')
            return super().execute(sql, *args)

        def close(self):
            self.closed_by_runner = True
            return super().close()

    def connect(*args, **kwargs):
        conn = real_connect(*args, factory=PrivateConnection, **kwargs)
        conn.closed_by_runner = False
        connections.append(conn)
        return conn

    monkeypatch.setattr(module.sqlite3, 'connect', connect)
    result = module._observe_in_process(db, module.ObservationLimits())
    assert result.objects == () and 'database_unavailable' in result.reasons
    assert connections[0].closed_by_runner



@pytest.mark.parametrize('has_table,maximum,expected', [
    (True, 0, 'budget_exhausted'),
    (True, 1, 'budget_exhausted'),
    (False, 1, 'budget_exhausted'),
    (False, 2, 'budget_exhausted'),
    (False, 3, None),
    (True, 4, 'budget_exhausted'),
    (True, 5, None),
])
def test_schema_budget_never_fetches_beyond_row_limit(tmp_path, monkeypatch, has_table, maximum, expected):
    module = observation_module()
    real_connect = sqlite3.connect
    observed = []

    class CountingCursor(sqlite3.Cursor):
        def fetchone(self):
            self.calls += 1
            row = super().fetchone()
            if row is not None:
                self.materialized += 1
            return row

    class CountingConnection(sqlite3.Connection):
        def cursor(self, *args, **kwargs):
            cursor = super().cursor(factory=CountingCursor)
            cursor.calls = 0
            cursor.materialized = 0
            observed.append(cursor)
            return cursor

    def connect(*args, **kwargs):
        return real_connect(*args, factory=CountingConnection, **kwargs)

    db = tmp_path / 'limit.db'
    with real_connect(db) as fixture:
        if has_table:
            fixture.execute('CREATE TABLE fixture (x)')
    monkeypatch.setattr(module.sqlite3, 'connect', connect)
    result = module._observe_in_process(db, module.ObservationLimits(max_rows=maximum))
    assert sum(cursor.calls for cursor in observed) <= maximum
    assert sum(cursor.materialized for cursor in observed) <= maximum
    if expected:
        assert expected in result.reasons
        assert result.objects == ()
    else:
        assert 'budget_exhausted' not in result.reasons
        assert bool(result.objects) is has_table


def test_cli_trigger_finding_reports_name_only_for_noop_definition(tmp_path):
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db):
        pass
    with sqlite3.connect(db) as conn:
        conn.execute('DROP TRIGGER events_immutable_update')
        conn.execute('CREATE TRIGGER events_immutable_update AFTER UPDATE ON events BEGIN SELECT 1; END')
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity', '--database', str(db)],
                          capture_output=True, text=True, timeout=15)
    assert proc.returncode == 0, proc.stderr
    findings = json.loads(proc.stdout)['findings']
    assert 'events_immutable_update_name_present' in findings
    assert 'events_immutable_update_present' not in findings



@pytest.mark.parametrize('kind', ['fifo', 'directory', 'database_link', 'wal_link', 'shm_link'])
def test_cli_rejects_special_or_symlink_paths_without_disclosure(tmp_path, kind):
    import os
    db = tmp_path / 'private-name.db'
    if kind == 'fifo':
        os.mkfifo(db)
    elif kind == 'directory':
        db.mkdir()
    elif kind == 'database_link':
        target = tmp_path / 'target.db'
        with sqlite3.connect(target) as conn:
            conn.execute('CREATE TABLE fixture (x)')
        db.symlink_to(target)
    else:
        with sqlite3.connect(db) as conn:
            conn.execute('CREATE TABLE fixture (x)')
        (tmp_path / ('private-name.db-wal' if kind == 'wal_link' else 'private-name.db-shm')).symlink_to(tmp_path / 'target')
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity', '--database', str(db)],
                          capture_output=True, text=True, timeout=4)
    assert proc.returncode != 0
    assert str(tmp_path) not in proc.stdout + proc.stderr
    assert 'database_unavailable' in json.loads(proc.stdout)['findings']


def test_hung_helper_times_out_and_reaps_own_process(tmp_path, monkeypatch):
    import time
    module = observation_module()
    assert hasattr(module, '_HELPER_TIMEOUT_SECONDS'), 'bounded helper not implemented'
    db = tmp_path / 'private-name.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    real_popen = subprocess.Popen
    spawned = []

    def start_hung(*args, **kwargs):
        child = real_popen([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
        spawned.append(child)
        return child

    monkeypatch.setattr(module.subprocess, 'Popen', start_hung)
    started = time.monotonic()
    result = module._run_schema_observation(db, module.ObservationLimits())
    assert time.monotonic() - started < module._HELPER_TIMEOUT_SECONDS + 3
    assert 'database_unavailable' in result.reasons and result.objects == ()
    assert len(spawned) == 1 and spawned[0].poll() is not None


def test_runner_detects_database_path_replacement(tmp_path, monkeypatch):
    import os
    module = observation_module()
    assert hasattr(module, '_observe_in_process'), 'isolated helper implementation missing'
    db = tmp_path / 'events.db'
    replacement = tmp_path / 'replacement.db'
    saved = tmp_path / 'saved.db'
    for path in (db, replacement):
        with sqlite3.connect(path) as conn:
            conn.execute('CREATE TABLE fixture (x)')
    real_connect = sqlite3.connect

    def replace_on_open(*args, **kwargs):
        os.rename(db, saved)
        os.rename(replacement, db)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(module.sqlite3, 'connect', replace_on_open)
    result = module._observe_in_process(db, module.ObservationLimits())
    assert 'database_unavailable' in result.reasons and result.objects == ()



def test_interrupted_observation_reaps_own_helper(tmp_path, monkeypatch):
    module = observation_module()
    db = tmp_path / 'events.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    real_popen = subprocess.Popen
    spawned = []

    class InterruptOnce:
        def __init__(self, child):
            self.child = child
            self.interrupted = False

        def communicate(self, *args, **kwargs):
            if not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt()
            return self.child.communicate(*args, **kwargs)

        def kill(self):
            return self.child.kill()

    def start_hung(*args, **kwargs):
        child = real_popen([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
        spawned.append(child)
        return InterruptOnce(child)

    monkeypatch.setattr(module.subprocess, 'Popen', start_hung)
    try:
        with pytest.raises(KeyboardInterrupt):
            module._run_schema_observation(db, module.ObservationLimits())
        assert len(spawned) == 1 and spawned[0].poll() is not None
    finally:
        for child in spawned:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=3)



def test_helper_cleanup_unconfirmed_is_redacted_failure(tmp_path, monkeypatch):
    module = observation_module()
    from orchestrator.diagnostics.sqlite_continuity import diagnose_store
    db = tmp_path / 'private-name.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    real_popen = subprocess.Popen
    spawned = []

    class UnkillableSimulation:
        def __init__(self, child):
            self.child = child

        def communicate(self, *args, **kwargs):
            raise subprocess.TimeoutExpired('redacted', kwargs.get('timeout', 0))

        def kill(self):
            raise OSError('private path must not escape')

    def start_hung(*args, **kwargs):
        child = real_popen([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
        spawned.append(child)
        return UnkillableSimulation(child)

    monkeypatch.setattr(module.subprocess, 'Popen', start_hung)
    try:
        report = diagnose_store(db, module.ObservationLimits())
        assert report.non_authoritative
        assert 'helper_cleanup_unconfirmed' in report.findings
        assert str(tmp_path) not in repr(report)
    finally:
        for child in spawned:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=3)


def test_cli_rejects_symlink_in_parent_directory(tmp_path):
    directory = tmp_path / 'real'
    directory.mkdir()
    db = directory / 'events.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    alias = tmp_path / 'alias'
    alias.symlink_to(directory, target_is_directory=True)
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                           '--database', str(alias / 'events.db')],
                          capture_output=True, text=True, timeout=4)
    assert proc.returncode != 0
    assert 'database_unavailable' in json.loads(proc.stdout)['findings']
    assert str(tmp_path) not in proc.stdout + proc.stderr



def test_wal_checkpoint_and_final_close_report_sidecar_changed(tmp_path, monkeypatch):
    import selectors
    module = observation_module()
    db = tmp_path / 'events.db'
    script = """import sqlite3, sys
c = sqlite3.connect(sys.argv[1], isolation_level=None)
c.execute('PRAGMA journal_mode=WAL')
c.execute('PRAGMA wal_autocheckpoint=0')
c.execute('CREATE TABLE fixture (x)')
c.execute('INSERT INTO fixture VALUES (1)')
print('ready', flush=True)
sys.stdin.readline()
c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
c.close()
"""
    writer = subprocess.Popen([sys.executable, '-c', script, str(db)], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(writer.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=5), 'writer did not start'
            assert writer.stdout.readline().strip() == 'ready'
        assert (tmp_path / 'events.db-wal').is_file()
        assert (tmp_path / 'events.db-shm').is_file()
        real_connect = sqlite3.connect

        class ReleasingConnection(sqlite3.Connection):
            def close(self):
                result = super().close()
                _, errors = writer.communicate('close\n', timeout=5)
                assert writer.returncode == 0, errors
                return result

        def connect(*args, **kwargs):
            return real_connect(*args, factory=ReleasingConnection, **kwargs)

        monkeypatch.setattr(module.sqlite3, 'connect', connect)
        result = module._observe_in_process(db, module.ObservationLimits())
        assert 'sidecar_changed' in result.reasons
        assert 'database_unavailable' not in result.reasons
        assert any(o.kind == 'table' and o.name == 'fixture' for o in result.objects)
        assert not (tmp_path / 'events.db-wal').exists()
        assert not (tmp_path / 'events.db-shm').exists()
        from orchestrator.diagnostics import sqlite_continuity as cli
        monkeypatch.setattr(cli, '_run_schema_observation', lambda *_: result)
        report = cli.diagnose_store(db, module.ObservationLimits())
        assert report.non_authoritative is True
        assert 'sidecar_changed' in report.unknowns
        assert str(tmp_path) not in repr(report)
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.communicate(timeout=5)


def test_observer_reads_valid_wal_without_existing_shm(tmp_path):
    import selectors
    import shutil
    module = observation_module()
    source = tmp_path / 'source.db'
    script = """import sqlite3, sys
c = sqlite3.connect(sys.argv[1], isolation_level=None)
c.execute('PRAGMA journal_mode=WAL')
c.execute('PRAGMA wal_autocheckpoint=0')
c.execute('CREATE TABLE wal_fixture (x)')
c.execute('INSERT INTO wal_fixture VALUES (7)')
print('ready', flush=True)
sys.stdin.readline()
c.close()
"""
    writer = subprocess.Popen([sys.executable, '-c', script, str(source)], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(writer.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=5), 'writer did not start'
            assert writer.stdout.readline().strip() == 'ready'
        copied = tmp_path / 'copy.db'
        shutil.copyfile(source, copied)
        shutil.copyfile(tmp_path / 'source.db-wal', tmp_path / 'copy.db-wal')
        assert not (tmp_path / 'copy.db-shm').exists()
        result = module._run_schema_observation(copied, module.ObservationLimits())
        assert 'database_unavailable' not in result.reasons
        assert 'sidecar_changed' in result.reasons
        assert any(o.kind == 'table' and o.name == 'wal_fixture' for o in result.objects)
        assert 'writer_inventory_unproven' in result.reasons
    finally:
        if writer.poll() is None:
            try:
                writer.communicate('close\n', timeout=5)
            except subprocess.TimeoutExpired:
                writer.kill()
                writer.communicate(timeout=5)
                raise
        assert writer.returncode == 0


def test_sidecar_becoming_symlink_still_fails_redacted(tmp_path, monkeypatch):
    module = observation_module()
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    real_connect = sqlite3.connect

    class SidecarLinkConnection(sqlite3.Connection):
        def close(self):
            result = super().close()
            (tmp_path / 'private.db-shm').symlink_to(tmp_path / 'private-target')
            return result

    def connect(*args, **kwargs):
        return real_connect(*args, factory=SidecarLinkConnection, **kwargs)

    monkeypatch.setattr(module.sqlite3, 'connect', connect)
    result = module._observe_in_process(db, module.ObservationLimits())
    assert 'database_unavailable' in result.reasons
    assert result.objects == ()
    assert str(tmp_path) not in repr(result)



def test_independent_close_reopen_recreates_sidecars_as_unknown(tmp_path, monkeypatch):
    import os
    import selectors
    module = observation_module()
    db = tmp_path / 'events.db'
    script = """import sqlite3, sys
c = sqlite3.connect(sys.argv[1], isolation_level=None)
c.execute('PRAGMA journal_mode=WAL')
c.execute('PRAGMA wal_autocheckpoint=0')
c.execute('CREATE TABLE IF NOT EXISTS fixture (x)')
c.execute('INSERT INTO fixture VALUES (1)')
print('ready', flush=True)
sys.stdin.readline()
c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
c.close()
"""
    children = []
    retained = []

    def start_writer():
        child = subprocess.Popen([sys.executable, '-c', script, str(db)], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        children.append(child)
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=5), 'writer did not start'
            assert child.stdout.readline().strip() == 'ready'
        return child

    first = start_writer()
    try:
        wal, shm = tmp_path / 'events.db-wal', tmp_path / 'events.db-shm'
        # Retain the old fixture inodes while SQLite unlinks its names, so a
        # newly created sidecar cannot immediately reuse those inode numbers.
        retained.extend((os.open(wal, os.O_RDONLY), os.open(shm, os.O_RDONLY)))
        before = (os.fstat(retained[0]).st_ino, os.fstat(retained[1]).st_ino)
        real_connect = sqlite3.connect

        class ReopeningConnection(sqlite3.Connection):
            def close(self):
                result = super().close()
                _, errors = first.communicate('close\n', timeout=5)
                assert first.returncode == 0, errors
                assert not wal.exists() and not shm.exists()
                start_writer()
                return result

        def connect(*args, **kwargs):
            return real_connect(*args, factory=ReopeningConnection, **kwargs)

        monkeypatch.setattr(module.sqlite3, 'connect', connect)
        result = module._observe_in_process(db, module.ObservationLimits())
        after = (wal.stat().st_ino, shm.stat().st_ino)
        assert before != after
        assert 'sidecar_changed' in result.reasons
        assert 'database_unavailable' not in result.reasons
        assert 'writer_inventory_unproven' in result.reasons
    finally:
        for fd in retained:
            os.close(fd)
        for child in children:
            if child.poll() is None:
                try:
                    child.communicate('close\n', timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.communicate(timeout=5)
                    raise


@pytest.mark.parametrize('kind', ['table_name', 'table_ddl', 'index_name', 'trigger_name',
                                  'index_ddl', 'trigger_ddl', 'temp_table_name',
                                  'temp_index_name', 'temp_index_ddl',
                                  'temp_trigger_name', 'temp_trigger_ddl'])
def test_metadata_name_and_sql_preflight_before_python_materialization(tmp_path, monkeypatch, kind):
    module = observation_module()
    db = tmp_path / 'metadata.db'
    huge = 'z' * 70000
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
        if kind == 'table_name':
            conn.execute(f'CREATE TABLE "{huge}" (x)')
        elif kind == 'table_ddl':
            conn.execute(f'CREATE TABLE oversized_table ("{huge}" TEXT)')
        elif kind == 'index_name':
            conn.execute(f'CREATE INDEX "{huge}" ON fixture(x)')
        elif kind == 'trigger_name':
            conn.execute(f'CREATE TRIGGER "{huge}" AFTER INSERT ON fixture BEGIN SELECT 1; END')
        elif kind == 'index_ddl':
            conn.execute(f"CREATE INDEX short_idx ON fixture(x) WHERE x='{huge}'")
        elif kind == 'trigger_ddl':
            conn.execute(f"CREATE TRIGGER short_tr AFTER INSERT ON fixture BEGIN SELECT '{huge}'; END")
    real_connect = sqlite3.connect
    fetched = []

    class GuardCursor(sqlite3.Cursor):
        def fetchone(self):
            row = super().fetchone()
            if row is not None:
                fetched.append(1)
                assert not any(isinstance(value, str) and len(value.encode()) > 65536 for value in row), (
                    'oversized metadata reached Python')
            return row

    class GuardConnection(sqlite3.Connection):
        def cursor(self, *args, **kwargs):
            return super().cursor(factory=GuardCursor)

    def connect(*args, **kwargs):
        conn = real_connect(*args, factory=GuardConnection, **kwargs)
        if kind == 'temp_table_name':
            conn.execute(f'CREATE TEMP TABLE "{huge}" (x)')
        elif kind in ('temp_index_name', 'temp_index_ddl'):
            conn.execute('CREATE TEMP TABLE local_table (x)')
            if kind == 'temp_index_name':
                conn.execute(f'CREATE INDEX "{huge}" ON local_table(x)')
            else:
                conn.execute(f"CREATE INDEX local_idx ON local_table(x) WHERE x='{huge}'")
        elif kind == 'temp_trigger_name':
            conn.execute(f'CREATE TEMP TRIGGER "{huge}" AFTER INSERT ON main.fixture BEGIN SELECT 1; END')
        elif kind == 'temp_trigger_ddl':
            conn.execute(f"CREATE TEMP TRIGGER local_tr AFTER INSERT ON main.fixture BEGIN SELECT '{huge}'; END")
        return conn

    monkeypatch.setattr(module.sqlite3, 'connect', connect)
    result = module._observe_in_process(db, module.ObservationLimits())
    assert fetched, 'custom cursor did not observe actual SQLite fetches'
    assert 'budget_exhausted' in result.reasons
    assert result.objects == ()
    assert str(tmp_path) not in repr(result)


def test_foreign_temp_triggers_are_not_inspectable_by_one_shot_runner(tmp_path, monkeypatch):
    module = observation_module()
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    foreign = sqlite3.connect(db)
    try:
        foreign.execute('CREATE TEMP TRIGGER foreign_trigger AFTER INSERT ON main.fixture BEGIN SELECT 1; END')
        normal = module._run_schema_observation(db, module.ObservationLimits())
        assert normal.temp_objects == ()
        assert 'cross_connection_temp_unobservable' in normal.reasons
        proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity', '--database', str(db)],
                              capture_output=True, text=True, timeout=10)
        assert proc.returncode == 0, proc.stderr
        assert 'current_connection_temp_triggers_none' in json.loads(proc.stdout)['findings']
        assert 'foreign_trigger' not in proc.stdout + proc.stderr
        real_connect = sqlite3.connect

        def connect(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            conn.execute('CREATE TEMP TRIGGER current_trigger AFTER INSERT ON main.fixture BEGIN SELECT 1; END')
            return conn

        monkeypatch.setattr(module.sqlite3, 'connect', connect)
        current = module._observe_in_process(db, module.ObservationLimits())
        assert [(o.kind, o.name) for o in current.temp_objects] == [('trigger', 'current_trigger')]
        assert not any(o.name == 'foreign_trigger' for o in current.temp_objects)
        assert 'cross_connection_temp_unobservable' in current.reasons
        from orchestrator.diagnostics import sqlite_continuity as cli
        monkeypatch.setattr(cli, '_run_schema_observation', lambda *_: current)
        report = cli.diagnose_store(db, module.ObservationLimits())
        assert 'current_connection_temp_triggers_present' in report.findings
        assert 'foreign_trigger' not in repr(report) and 'current_trigger' not in repr(report)
        assert foreign.execute("SELECT count(*) FROM sqlite_temp_master WHERE name='foreign_trigger'").fetchone()[0] == 1
    finally:
        foreign.close()



def test_private_temp_unique_and_nonunique_indexes_are_observed(tmp_path, monkeypatch):
    module = observation_module()
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        conn.execute('CREATE TEMP TABLE local_table (x, y)')
        conn.execute('CREATE UNIQUE INDEX local_unique ON local_table(x)')
        conn.execute('CREATE INDEX local_nonunique ON local_table(y)')
        return conn

    monkeypatch.setattr(module.sqlite3, 'connect', connect)
    result = module._observe_in_process(db, module.ObservationLimits())
    indexes = {item.name: item.unique for item in result.temp_objects if item.kind == 'index'}
    assert indexes == {'local_unique': True, 'local_nonunique': False}
    assert 'cross_connection_temp_unobservable' in result.reasons


def test_malformed_isolated_schema_returns_redacted_failure(tmp_path):
    db = tmp_path / 'secret-path.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
        conn.execute('CREATE TRIGGER my_trigger AFTER INSERT ON fixture BEGIN SELECT 1; END')
        conn.execute('PRAGMA writable_schema=ON')
        conn.execute("UPDATE sqlite_master SET sql=NULL WHERE name='my_trigger'")
        conn.execute('PRAGMA writable_schema=OFF')
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                           '--database', str(db)], capture_output=True, text=True, timeout=10)
    assert proc.returncode != 0
    report = json.loads(proc.stdout)
    assert set(report['findings']) & {'database_unavailable', 'metadata_invalid', 'budget_exhausted'}
    assert report['non_authoritative'] is True
    assert str(tmp_path) not in proc.stdout + proc.stderr
    assert 'my_trigger' not in proc.stdout + proc.stderr



def test_cli_oversized_schema_is_bounded_and_redacted(tmp_path):
    db = tmp_path / 'private-db-name.db'
    name = 'private_index_' + 'z' * 70000
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
        conn.execute(f'CREATE INDEX "{name}" ON fixture(x)')
    before = db.read_bytes()
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                           '--database', str(db)], capture_output=True, text=True, timeout=10)
    assert proc.returncode != 0
    assert 'budget_exhausted' in json.loads(proc.stdout)['findings']
    assert len(proc.stdout) < 4096 and not proc.stderr
    assert str(tmp_path) not in proc.stdout and 'private_index_' not in proc.stdout
    assert db.read_bytes() == before
