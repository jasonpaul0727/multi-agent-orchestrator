import json
import os
from pathlib import Path
import selectors
import sqlite3
import subprocess
import sys

import pytest


def wait_line(pipe):
    with selectors.DefaultSelector() as selector:
        selector.register(pipe, selectors.EVENT_READ)
        assert selector.select(timeout=8), 'fixture did not respond'
        return pipe.readline().strip()


def test_cli_two_snapshot_comparison_reports_real_wal_lifecycle(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    db = tmp_path / 'private.db'
    script = """import sqlite3, sys
p = sys.argv[1]
a = sqlite3.connect(p, isolation_level=None)
a.execute('PRAGMA journal_mode=WAL')
a.execute('PRAGMA wal_autocheckpoint=0')
a.execute('CREATE TABLE fixture (x)')
a.execute('INSERT INTO fixture VALUES (1)')
b = sqlite3.connect(p, isolation_level=None)
b.execute('SELECT * FROM fixture').fetchall()
print('ready', flush=True)
sys.stdin.readline()
b.close()
a.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
a.close()
print('closed', flush=True)
sys.stdin.readline()
a = sqlite3.connect(p)
a.execute('SELECT * FROM fixture').fetchall()
a.close()
"""
    worker = subprocess.Popen([sys.executable, '-c', script, str(db)], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert wait_line(worker.stdout) == 'ready'
        assert Path(str(db) + '-wal').exists() and Path(str(db) + '-shm').exists()
        def between(seconds):
            assert seconds == 0
            worker.stdin.write('close\n')
            worker.stdin.flush()
            assert wait_line(worker.stdout) == 'closed'
        monkeypatch.setattr(cli, '_comparison_delay', between)
        assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) == 0
        captured = capsys.readouterr()
        report = json.loads(captured.out)
        comparison = report['comparison']
        assert {'wal_present_to_missing', 'shm_present_to_missing'} <= set(comparison['findings'])
        assert {'between_samples_unobserved', 'sqlite_handle_binding_unproven'} <= set(comparison['unknowns'])
        assert report['before']['non_authoritative'] and report['after']['non_authoritative']
        assert str(tmp_path) not in captured.out + captured.err
        assert len(captured.out) < 16384
        worker.stdin.write('reopen\n')
        worker.stdin.flush()
        assert worker.wait(timeout=5) == 0
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)
        worker.stdin.close()
        worker.stdout.close()
        worker.stderr.close()


def test_cli_comparison_observes_real_db_replacement(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    db, replacement = tmp_path / 'private.db', tmp_path / 'replacement.db'
    for path in (db, replacement):
        with sqlite3.connect(path) as conn:
            conn.execute('CREATE TABLE fixture (x)')
    worker = subprocess.Popen([sys.executable, '-c',
                               'import os,sys; sys.stdin.readline(); os.replace(sys.argv[1],sys.argv[2]); print("done",flush=True)',
                               str(replacement), str(db)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True)
    try:
        def between(seconds):
            worker.stdin.write('replace\n')
            worker.stdin.flush()
            assert wait_line(worker.stdout) == 'done'
        monkeypatch.setattr(cli, '_comparison_delay', between)
        assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) == 0
        captured = capsys.readouterr()
        report = json.loads(captured.out)
        assert 'db_identity_changed' in report['comparison']['findings']
        assert 'sqlite_handle_binding_unproven' in report['comparison']['unknowns']
        assert 'between_samples_unobserved' in report['comparison']['unknowns']
        assert str(tmp_path) not in captured.out + captured.err
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=5)


def test_compare_independent_storage_samples_and_rejects_invalid(tmp_path):
    from orchestrator.isolation._sqlite_storage_observation import (
        StorageObservation, compare_storage_observations, observe_storage,
    )
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    first = observe_storage(db)
    second = observe_storage(db)
    result = compare_storage_observations(first, second)
    assert {'parent_sample_matches', 'db_sample_matches', 'wal_sample_matches', 'shm_sample_matches'} <= set(result.findings)
    assert {'between_samples_unobserved', 'sqlite_handle_binding_unproven'} <= set(result.unknowns)
    assert str(tmp_path) not in repr(result)
    for bad in (None, 'private', StorageObservation(('db_present',), True)):
        with pytest.raises((TypeError, ValueError)):
            compare_storage_observations(first, bad)


@pytest.mark.parametrize('delay', ['-1', '1001', 'private-value', '1.5'])
def test_cli_rejects_invalid_delay_without_leaking_input(tmp_path, capsys, delay):
    from orchestrator.diagnostics import sqlite_continuity as cli
    with pytest.raises(SystemExit):
        cli.main(['--database', str(tmp_path / 'private-db'), '--compare-delay-ms', delay])
    output = capsys.readouterr()
    assert 'private' not in output.out + output.err


def test_cli_comparison_rejects_observed_symlink(tmp_path, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    Path(str(db) + '-wal').symlink_to(tmp_path / 'absent')
    assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) != 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert 'wal_symlink_seen' in report['before']['findings']
    assert 'between_samples_unobserved' in report['comparison']['unknowns']
    assert str(tmp_path) not in captured.out + captured.err


def test_cli_comparison_reports_wal_reopened_between_samples(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    db = tmp_path / 'private.db'
    conn = sqlite3.connect(db)
    assert conn.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
    conn.execute('CREATE TABLE fixture (x)')
    conn.close()
    assert not Path(str(db) + '-wal').exists()
    script = """import sqlite3, sys
sys.stdin.readline()
a = sqlite3.connect(sys.argv[1], isolation_level=None)
a.execute('PRAGMA wal_autocheckpoint=0')
a.execute('INSERT INTO fixture VALUES (2)')
b = sqlite3.connect(sys.argv[1], isolation_level=None)
b.execute('SELECT * FROM fixture').fetchall()
print('ready', flush=True)
sys.stdin.readline()
b.close()
a.close()
"""
    worker = subprocess.Popen([sys.executable, '-c', script, str(db)], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        def between(seconds):
            worker.stdin.write('open\n')
            worker.stdin.flush()
            assert wait_line(worker.stdout) == 'ready'
        monkeypatch.setattr(cli, '_comparison_delay', between)
        assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) == 0
        report = json.loads(capsys.readouterr().out)
        assert {'wal_missing_to_present', 'shm_missing_to_present'} <= set(report['comparison']['findings'])
        assert 'between_samples_unobserved' in report['comparison']['unknowns']
        worker.stdin.write('close\n')
        worker.stdin.flush()
        assert worker.wait(timeout=5) == 0
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=5)


def test_cli_comparison_uses_fresh_schema_helpers(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    from orchestrator.persistence import _continuity_observation as schema
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    real = subprocess.Popen
    helpers = []
    def record(args, **kwargs):
        child = real(args, **kwargs)
        if len(args) >= 3 and args[1:3] == ['-m', 'orchestrator.persistence._continuity_observation']:
            helpers.append(child)
        return child
    monkeypatch.setattr(schema.subprocess, 'Popen', record)
    assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) == 0
    result = json.loads(capsys.readouterr().out)
    assert len(helpers) == 2 and helpers[0].pid != helpers[1].pid
    assert all(child.poll() is not None for child in helpers)
    assert {'parent_sample_matches', 'db_sample_matches', 'wal_sample_matches', 'shm_sample_matches'} <= set(result['comparison']['findings'])
    assert 'between_samples_unobserved' in result['comparison']['unknowns']


def test_comparison_cli_process_accepts_bounds_and_preserves_single_report(tmp_path):
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    for args in ([], ['--compare-delay-ms', '0'], ['--compare-delay-ms', '1000']):
        proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                               '--database', str(db), *args], capture_output=True, text=True, timeout=15)
        assert proc.returncode == 0, proc.stderr
        result = json.loads(proc.stdout)
        assert result['non_authoritative'] is True
        assert ('comparison' in result) == bool(args)
        assert str(tmp_path) not in proc.stdout + proc.stderr
        assert len(proc.stdout) < 16384
        assert 'path_sample' not in proc.stdout and 'sidecar_sample' not in proc.stdout
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_comparison_rejects_malformed_private_samples(tmp_path):
    from dataclasses import replace
    from orchestrator.isolation._sqlite_storage_observation import (
        StorageObservation, compare_storage_observations, observe_storage,
    )
    db = tmp_path / 'db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    valid = observe_storage(db)
    class ReasonSubclass(str):
        pass
    malformed = [StorageObservation((), False, ReasonSubclass('database_unavailable')),
                 replace(valid, findings=('private-string',)),
                 replace(valid, findings=('db_present', 'db_present')),
                 replace(valid, sidecar_sample=(None,)),
                 replace(valid, path_sample=((True, 1, 16384, 0),)),
                 StorageObservation(('wal_present',), False, 'database_unavailable'),
                 StorageObservation(('db_present', 'wal_missing', 'shm_missing'),
                                    False, 'helper_cleanup_unconfirmed')]
    for sample in malformed:
        with pytest.raises(ValueError, match='invalid storage observation'):
            compare_storage_observations(valid, sample)


def test_comparison_reports_regular_sidecar_identity_replacement(tmp_path):
    from orchestrator.isolation._sqlite_storage_observation import compare_storage_observations, observe_storage
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    shm, replacement = Path(str(db) + '-shm'), tmp_path / 'replacement'
    shm.write_bytes(b'old private contents')
    replacement.write_bytes(b'new private contents')
    before = observe_storage(db)
    os.replace(replacement, shm)
    after = observe_storage(db)
    result = compare_storage_observations(before, after)
    assert 'shm_identity_changed' in result.findings
    assert 'between_samples_unobserved' in result.unknowns
    assert 'sqlite_handle_binding_unproven' in result.unknowns
    assert str(tmp_path) not in repr(result)
    assert 'private contents' not in repr(result)


def test_cli_comparison_rejects_sidecar_made_unsafe_between_reports(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    def between(seconds):
        Path(str(db) + '-wal').symlink_to(tmp_path / 'private-target')
    monkeypatch.setattr(cli, '_comparison_delay', between)
    assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) != 0
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert 'wal_symlink_seen' in result['after']['findings']
    assert {'parent_unknown', 'db_unknown', 'wal_unknown', 'shm_unknown'} == set(result['comparison']['findings'])
    assert 'between_samples_unobserved' in result['comparison']['unknowns']
    assert str(tmp_path) not in captured.out + captured.err
    assert not (tmp_path / 'private-target').exists()


def test_comparison_cleanup_uncertainty_stays_failure(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    from orchestrator.persistence import _continuity_observation as schema
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    original = cli._run_schema_observation
    calls = 0
    def fail_second(path, limits):
        nonlocal calls
        calls += 1
        if calls == 2:
            return schema._failed(schema.ObservationReason.HELPER_CLEANUP_UNCONFIRMED)
        return original(path, limits)
    monkeypatch.setattr(cli, '_run_schema_observation', fail_second)
    assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) != 0
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert 'helper_cleanup_unconfirmed' in result['after']['findings']
    assert {'parent_unknown', 'db_unknown', 'wal_unknown', 'shm_unknown'} == set(result['comparison']['findings'])
    assert str(tmp_path) not in captured.out + captured.err


def test_cli_comparison_observes_parent_replacement_with_same_database_inode(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    parent = tmp_path / 'parent'
    parent.mkdir()
    db = parent / 'private.db'
    connection = sqlite3.connect(db)
    connection.execute('CREATE TABLE fixture (x)')
    connection.close()
    script = """import os, pathlib, sys
parent = pathlib.Path(sys.argv[1])
moved = parent.with_name('moved')
sys.stdin.readline()
parent.rename(moved)
parent.mkdir()
os.link(moved / 'private.db', parent / 'private.db')
(moved / 'private.db').unlink()
print('done', flush=True)
"""
    worker = subprocess.Popen([sys.executable, '-c', script, str(parent)], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        def between(seconds):
            worker.stdin.write('replace\n')
            worker.stdin.flush()
            assert wait_line(worker.stdout) == 'done'
        monkeypatch.setattr(cli, '_comparison_delay', between)
        assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) == 0
        captured = capsys.readouterr()
        report = json.loads(captured.out)
        assert 'parent_identity_changed' in report['comparison']['findings']
        assert 'db_sample_matches' in report['comparison']['findings']
        assert {'between_samples_unobserved', 'sqlite_handle_binding_unproven'} <= set(report['comparison']['unknowns'])
        assert str(tmp_path) not in captured.out + captured.err
        assert 'path_sample' not in captured.out and 'inode' not in captured.out
        assert worker.wait(timeout=5) == 0
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=5)


def test_cli_comparison_observes_regular_sidecar_replacement(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    shm, replacement = Path(str(db) + '-shm'), tmp_path / 'replacement'
    shm.write_bytes(b'private old')
    replacement.write_bytes(b'private new')
    worker = subprocess.Popen([sys.executable, '-c',
                               'import os,sys; sys.stdin.readline(); os.replace(sys.argv[1],sys.argv[2]); print("done",flush=True)',
                               str(replacement), str(shm)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True)
    try:
        def between(seconds):
            worker.stdin.write('replace\n')
            worker.stdin.flush()
            assert wait_line(worker.stdout) == 'done'
        monkeypatch.setattr(cli, '_comparison_delay', between)
        assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) == 0
        captured = capsys.readouterr()
        report = json.loads(captured.out)
        assert 'shm_identity_changed' in report['comparison']['findings']
        assert 'between_samples_unobserved' in report['comparison']['unknowns']
        assert str(tmp_path) not in captured.out + captured.err
        assert 'private old' not in captured.out and 'private new' not in captured.out
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=5)


@pytest.mark.parametrize('invalid', ['short_path', 'db_type', 'link_flag', 'sidecar_type'])
def test_cli_comparison_rejects_malformed_storage_helper(tmp_path, monkeypatch, capsys, invalid):
    from dataclasses import asdict
    from orchestrator.diagnostics import sqlite_continuity as cli
    from orchestrator.isolation import _sqlite_storage_observation as storage
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE fixture (x)')
    payload = asdict(storage.observe_storage(db))
    # Convert tuple fields to mutable JSON arrays, retaining a real valid baseline.
    payload = json.loads(json.dumps(payload))
    if invalid == 'short_path':
        payload['path_sample'] = payload['path_sample'][-1:]
    elif invalid == 'db_type':
        payload['path_sample'][-1][2] = 0o040000
    elif invalid == 'link_flag':
        payload['findings'].append('db_link_count_gt_one')
    else:
        payload['sidecar_sample'][0] = [1, 2, 0o040000, 1]
        payload['findings'][payload['findings'].index('wal_missing')] = 'wal_present'
    real_popen = subprocess.Popen
    class FakeChild:
        returncode = 0
        def communicate(self, *args, **kwargs):
            return json.dumps(payload).encode(), b''
    def intercept(args, **kwargs):
        if len(args) >= 3 and args[1:3] == ['-m', 'orchestrator.isolation._sqlite_storage_observation']:
            return FakeChild()
        return real_popen(args, **kwargs)
    monkeypatch.setattr(storage.subprocess, 'Popen', intercept)
    assert cli.main(['--database', str(db), '--compare-delay-ms', '0']) != 0
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert 'database_unavailable' in result['before']['findings']
    assert 'database_unavailable' in result['after']['findings']
    assert 'between_samples_unobserved' in result['comparison']['unknowns']
    assert str(tmp_path) not in captured.out + captured.err
    assert 'Traceback' not in captured.out + captured.err
    assert 'path_sample' not in captured.out and 'sidecar_sample' not in captured.out
