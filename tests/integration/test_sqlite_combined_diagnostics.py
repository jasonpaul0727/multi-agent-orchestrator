"""Combined local diagnostics remain bounded observations, never authority."""
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from orchestrator.artifacts import ArtifactStore
from orchestrator.lifecycle.cancel_completion import CancellationBudget, read_event_window
from orchestrator.persistence import EventDraft, SQLiteEventStore


def invoke(db, *args):
    return subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                           '--database', str(db), *args], capture_output=True, text=True, timeout=20)


def assert_unknowns(report):
    assert report['non_authoritative'] is True
    assert {'cross_connection_temp_unobservable', 'writer_inventory_unproven',
            'physical_storage_durability_unproven', 'sqlite_handle_binding_unproven'} <= set(report['unknowns'])
    assert 'eligible' not in report and 'authorized' not in report


def test_cli_reports_actual_schema_and_sidecar_findings_together(tmp_path):
    valid, changed = tmp_path / 'valid-private.db', tmp_path / 'changed-private.db'
    for db in (valid, changed):
        with SQLiteEventStore(db):
            pass
    writer = sqlite3.connect(changed, isolation_level=None)
    try:
        assert writer.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('DROP TRIGGER events_immutable_update')
        assert Path(str(changed) + '-wal').exists() and Path(str(changed) + '-shm').exists()
        first, second = invoke(valid), invoke(changed)
        assert first.returncode == second.returncode == 0
        reports = [json.loads(proc.stdout) for proc in (first, second)]
        for report in reports:
            assert_unknowns(report)
        assert {'events_immutable_update_name_present', 'wal_missing', 'shm_missing'} <= set(reports[0]['findings'])
        assert {'events_immutable_update_name_missing', 'wal_present', 'shm_present'} <= set(reports[1]['findings'])
        assert all(str(tmp_path) not in proc.stdout + proc.stderr and len(proc.stdout) < 4096
                   for proc in (first, second))
    finally:
        writer.close()


def test_cli_limits_and_redacts_untrusted_sqlite_metadata_and_data(tmp_path):
    db = tmp_path / 'private-name.db'
    private_name = 'private_index_' + 'q' * 70000
    writer = sqlite3.connect(db, isolation_level=None)
    try:
        assert writer.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('CREATE TABLE fixture (x)')
        writer.execute('INSERT INTO fixture VALUES (?)', ('private_payload_marker',))
        writer.execute(f'CREATE INDEX "{private_name}" ON fixture(x)')
        proc = invoke(db)
        assert proc.returncode != 0
        report = json.loads(proc.stdout)
        assert_unknowns(report)
        assert {'budget_exhausted', 'db_present', 'wal_present', 'shm_present'} <= set(report['findings'])
        assert len(proc.stdout) < 4096
        assert not proc.stderr
        assert all(value not in proc.stdout for value in (str(tmp_path), 'private_index_', 'private_payload_marker', 'CREATE INDEX'))
        assert writer.execute('SELECT x FROM fixture').fetchone() == ('private_payload_marker',)
    finally:
        writer.close()


def test_cli_malformed_arguments_do_not_echo_untrusted_values(tmp_path):
    db = tmp_path / 'private-db-path'
    proc = invoke(db, '--unknown-private-argument', 'private-argument-value')
    assert proc.returncode != 0
    assert 'private' not in proc.stdout + proc.stderr
    assert len(proc.stdout) + len(proc.stderr) < 4096


def test_non_opt_in_v1_store_still_writes_and_full_validator_is_unchanged(tmp_path):
    db = tmp_path / 'private.db'
    with SQLiteEventStore(db) as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        record = artifacts.publish_bytes(b'private artifact bytes', source={'run_id': 'run-1'},
                                         readable_scope=('run-1',))
        def full_validation():
            with store._cancellation_proof_write():
                return read_event_window(store, 'artifact', record.digest,
                                         CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100))
        before_events, before_version = full_validation()
        before_verified = artifacts.verify_run_artifacts('run-1')
        first = invoke(db)
        assert first.returncode == 0
        assert_unknowns(json.loads(first.stdout))
        store.append('run', 'run-1', 0, [EventDraft('RunCreated', {'private': 'payload'})], 'first')
        second = artifacts.publish_bytes(b'other artifact', source={'run_id': 'run-2'},
                                         readable_scope=('run-2',))
        after = invoke(db)
        assert after.returncode == 0
        assert_unknowns(json.loads(after.stdout))
        assert store.current_version('run', 'run-1') == 1
        assert second.digest != record.digest
        assert full_validation() == (before_events, before_version)
        assert artifacts.verify_run_artifacts('run-1') == before_verified == [record]
        assert all('private artifact bytes' not in proc.stdout + proc.stderr and
                   str(tmp_path) not in proc.stdout + proc.stderr for proc in (first, after))


def test_malformed_database_is_redacted_and_reports_uncertainty(tmp_path):
    db = tmp_path / 'private.db'
    raw = b'private corrupted sqlite content\x00' * 8
    db.write_bytes(raw)
    proc = invoke(db)
    assert proc.returncode != 0
    report = json.loads(proc.stdout)
    assert_unknowns(report)
    assert 'database_unavailable' in report['findings']
    assert 'private' not in proc.stdout + proc.stderr
    assert len(proc.stdout) < 4096
    assert db.read_bytes() == raw


def test_comparison_itself_reports_physical_storage_unknown(tmp_path):
    db = tmp_path / 'private.db'
    with SQLiteEventStore(db):
        pass
    proc = invoke(db, '--compare-delay-ms', '0')
    assert proc.returncode == 0
    report = json.loads(proc.stdout)
    assert_unknowns(report['before'])
    assert_unknowns(report['after'])
    assert 'physical_storage_durability_unproven' in report['comparison']['unknowns']
    assert 'between_samples_unobserved' in report['comparison']['unknowns']
    assert 'sqlite_handle_binding_unproven' in report['comparison']['unknowns']
    assert str(tmp_path) not in proc.stdout + proc.stderr


def test_diagnosis_preserves_complete_v1_cancellation_validation(tmp_path, monkeypatch):
    from orchestrator.lifecycle.cancel_completion import validate_v1_links, validate_v1_terminal
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import direct, terminal_case
    from tests.unit.runtime.test_verifier_process import _ChildLauncher

    # Existing offline fixture writes synthetic provider evidence; it makes no provider call.
    original = IsolatedVerifierProcess.__init__
    def offline_verifier(self, **kwargs):
        kwargs.setdefault('launcher', _ChildLauncher())
        original(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, '__init__', offline_verifier)
    case = terminal_case(tmp_path)
    try:
        direct(case)
        accepted = case.journal.read(case.operation.identity)
        assert accepted.phase == 'accepted'
        assert accepted.call is not None and accepted.capture is not None and accepted.acceptance is not None
        expected_windows = {
            ('provider_call', accepted.call.provider_call_stream_id),
            ('verification_proposals', accepted.identity.run_id),
            ('budget', accepted.identity.run_id),
            ('artifact', accepted.capture.response_artifact.digest),
            ('artifact', accepted.acceptance.artifacts[0].digest),
        }
        def full_validation():
            with case.store._cancellation_proof_write():
                budget = CancellationBudget(2000, 4_000_000, 200_000, 100, 20)
                links = validate_v1_links(case.store, accepted, budget)
                release = validate_v1_terminal(case.store, accepted, budget)
                assert expected_windows <= set(budget._cache)
                assert release.event_type == 'AttemptSlotReleased'
                return links, release, {key: budget._cache[key][0] for key in expected_windows}
        before_validation = full_validation()
        before_scheduler_events = case.store.read_stream('scheduler', 'global')
        connection = case.store._connection
        before_dump = tuple(connection.iterdump())
        before_version = connection.execute('PRAGMA data_version').fetchone()[0]
        before_changes = connection.total_changes
        report = invoke(tmp_path / 'events.db')
        assert report.returncode == 0
        assert_unknowns(json.loads(report.stdout))
        assert str(tmp_path) not in report.stdout + report.stderr
        assert 'terminal model text' not in report.stdout + report.stderr
        assert tuple(connection.iterdump()) == before_dump
        assert connection.execute('PRAGMA data_version').fetchone()[0] == before_version
        assert connection.total_changes == before_changes
        assert full_validation() == before_validation
        assert case.store.read_stream('scheduler', 'global') == before_scheduler_events
        assert tuple(connection.iterdump()) == before_dump
        assert connection.total_changes == before_changes
    finally:
        case.store.close()
