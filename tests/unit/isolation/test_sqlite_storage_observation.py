import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest


def cli(path):
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                           '--database', str(path)], capture_output=True, text=True, timeout=8)
    report = json.loads(proc.stdout)
    assert str(path.parent) not in proc.stdout + proc.stderr
    assert 'sqlite_handle_binding_unproven' in report['unknowns']
    assert report['non_authoritative'] is True
    return proc, report


def test_cli_reports_present_missing_and_hardlinked_sidecars(tmp_path):
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    wal = Path(str(db) + '-wal')
    shm = Path(str(db) + '-shm')
    before = db.read_bytes()
    proc, report = cli(db)
    assert proc.returncode == 0
    assert {'db_present', 'wal_missing', 'shm_missing'} <= set(report['findings'])
    assert db.read_bytes() == before
    assert not wal.exists() and not shm.exists()

    # A regular hardlink is a pathname fact, not an ownership or VFS claim.
    wal.write_bytes(b'private sidecar content')
    alias = tmp_path / 'alias'
    os.link(wal, alias)
    proc, report = cli(db)
    assert {'db_present', 'wal_present', 'wal_link_count_gt_one', 'shm_missing'} <= set(report['findings'])
    assert 'private sidecar content' not in proc.stdout + proc.stderr
    assert wal.read_bytes() == b'private sidecar content'


@pytest.mark.parametrize('role', ['db', 'wal', 'shm'])
def test_cli_refuses_symlink_before_sqlite_open(tmp_path, role):
    db = tmp_path / 'private.db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    target = tmp_path / 'target'
    target.write_bytes(b'private marker')
    selected = {'db': db, 'wal': Path(str(db) + '-wal'), 'shm': Path(str(db) + '-shm')}[role]
    if role == 'db':
        db.unlink()
    selected.symlink_to(target)
    proc, report = cli(db)
    assert proc.returncode != 0
    assert f'{role}_symlink_seen' in report['findings']
    assert target.read_bytes() == b'private marker'
    assert 'private marker' not in proc.stdout + proc.stderr
    assert not Path(str(db) + '-wal').exists() or role == 'wal'
    assert not Path(str(db) + '-shm').exists() or role == 'shm'


@pytest.mark.parametrize('kind', ['missing', 'directory', 'fifo', 'relative'])
def test_cli_rejects_invalid_database_without_creating_files(tmp_path, kind):
    db = tmp_path / 'private.db'
    if kind == 'directory':
        db.mkdir()
    elif kind == 'fifo':
        if not hasattr(os, 'mkfifo'):
            pytest.skip('FIFO unavailable')
        os.mkfifo(db)
    before = {p.name for p in tmp_path.iterdir()}
    path = Path('relative-private.db') if kind == 'relative' else db
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                           '--database', str(path)], capture_output=True, text=True, timeout=8,
                          cwd=tmp_path)
    report = json.loads(proc.stdout)
    assert proc.returncode != 0
    assert 'sqlite_handle_binding_unproven' in report['unknowns']
    assert {'database_unavailable', 'db_missing' if kind == 'missing' else
            'db_nonregular' if kind in ('directory', 'fifo') else 'path_invalid'} <= set(report['findings'])
    assert {p.name for p in tmp_path.iterdir()} == before
    assert str(tmp_path) not in proc.stdout + proc.stderr


def test_cli_active_wal_reports_real_sidecars(tmp_path):
    db = tmp_path / 'active.db'
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        assert connection.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        connection.execute('PRAGMA wal_autocheckpoint=0')
        connection.execute('CREATE TABLE example (x)')
        connection.execute('INSERT INTO example VALUES (1)')
        proc, report = cli(db)
        assert proc.returncode == 0
        assert {'db_present', 'wal_present', 'shm_present'} <= set(report['findings'])
        assert connection.execute('SELECT x FROM example').fetchall() == [(1,)]
    finally:
        connection.close()


def test_cli_rejects_parent_symlink(tmp_path):
    real = tmp_path / 'real'
    real.mkdir()
    db = real / 'database'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    alias = tmp_path / 'alias'
    alias.symlink_to(real, target_is_directory=True)
    proc, report = cli(alias / 'database')
    assert proc.returncode != 0
    assert 'parent_symlink_seen' in report['findings']


def test_parent_and_db_replacement_between_samples_are_rejected(tmp_path, monkeypatch):
    from orchestrator.diagnostics import sqlite_continuity as cli_module
    from orchestrator.persistence._continuity_observation import ObservationLimits
    original = tmp_path / 'database'
    with sqlite3.connect(original) as connection:
        connection.execute('CREATE TABLE example (x)')
    replacement = tmp_path / 'replacement'
    with sqlite3.connect(replacement) as connection:
        connection.execute('CREATE TABLE replacement (x)')
    real_run = cli_module._run_schema_observation

    def replace_during_observation(path, limits):
        result = real_run(path, limits)
        os.replace(replacement, original)
        return result

    monkeypatch.setattr(cli_module, '_run_schema_observation', replace_during_observation)
    report = cli_module.diagnose_store(original, ObservationLimits())
    assert 'database_unavailable' in report.findings
    assert 'sqlite_handle_binding_unproven' in report.unknowns


def test_supervised_storage_timeout_and_unconfirmed_cleanup(tmp_path, monkeypatch):
    from orchestrator.isolation import _sqlite_storage_observation as storage
    real_popen = subprocess.Popen
    children = []

    class Hung:
        def __init__(self, child, cannot_kill):
            self.child = child
            self.cannot_kill = cannot_kill
        def communicate(self, *args, **kwargs):
            raise subprocess.TimeoutExpired('redacted', 0)
        def kill(self):
            if self.cannot_kill:
                raise OSError('private path')
            self.child.kill()

    def run(cannot_kill):
        def start(*args, **kwargs):
            child = real_popen([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
            children.append(child)
            return Hung(child, cannot_kill)
        monkeypatch.setattr(storage.subprocess, 'Popen', start)
        return storage.observe_storage(tmp_path / 'db')

    try:
        # A timeout during cleanup must never be reported as a missing file.
        for cannot_kill in (False, True):
            result = run(cannot_kill)
            assert result.reason == 'helper_cleanup_unconfirmed'
            assert not result.preflight_ok and not result.findings
            assert str(tmp_path) not in repr(result)
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=3)


def test_unsupported_and_malformed_storage_results_fail_closed(tmp_path, monkeypatch):
    from orchestrator.isolation import _sqlite_storage_observation as storage
    monkeypatch.setattr(storage.sys, 'platform', 'unsupported')
    assert storage._observe(tmp_path / 'db').findings == ('storage_unsupported',)
    class BadChild:
        returncode = 0
        def communicate(self, *args, **kwargs):
            return b'{"findings":[],"preflight_ok":true,"reason":null,"path_sample":[],"sidecar_sample":[]}', b''
    monkeypatch.setattr(storage.subprocess, 'Popen', lambda *a, **k: BadChild())
    result = storage.observe_storage(tmp_path / 'db')
    assert not result.preflight_ok and result.reason == 'database_unavailable'



def test_sqlite_is_not_opened_when_preflight_sees_a_sidecar_symlink(tmp_path, monkeypatch):
    from orchestrator.diagnostics import sqlite_continuity as cli_module
    from orchestrator.persistence._continuity_observation import ObservationLimits
    db = tmp_path / 'db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    Path(str(db) + '-wal').symlink_to(tmp_path / 'absent-target')
    def forbidden(*args):
        pytest.fail('SQLite helper started despite an observed symlink')
    monkeypatch.setattr(cli_module, '_run_schema_observation', forbidden)
    report = cli_module.diagnose_store(db, ObservationLimits())
    assert 'wal_symlink_seen' in report.findings
    assert 'database_unavailable' in report.findings
    assert not (tmp_path / 'absent-target').exists()


def test_parent_replacement_between_samples_is_rejected(tmp_path, monkeypatch):
    from orchestrator.diagnostics import sqlite_continuity as cli_module
    from orchestrator.persistence._continuity_observation import ObservationLimits
    parent = tmp_path / 'parent'
    parent.mkdir()
    replacement = tmp_path / 'replacement'
    replacement.mkdir()
    for directory in (parent, replacement):
        with sqlite3.connect(directory / 'db') as connection:
            connection.execute('CREATE TABLE example (x)')
    real_run = cli_module._run_schema_observation
    def swap(path, limits):
        result = real_run(path, limits)
        parent.rename(tmp_path / 'old-parent')
        replacement.rename(parent)
        return result
    monkeypatch.setattr(cli_module, '_run_schema_observation', swap)
    report = cli_module.diagnose_store(parent / 'db', ObservationLimits())
    assert 'database_unavailable' in report.findings
    assert 'sqlite_handle_binding_unproven' in report.unknowns


def test_timeout_with_reaped_child_is_bounded_failure(tmp_path, monkeypatch):
    from orchestrator.isolation import _sqlite_storage_observation as storage
    real_popen = subprocess.Popen
    children = []
    class TimeoutOnce:
        def __init__(self, child):
            self.child = child
            self.calls = 0
        def communicate(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired('redacted', 0)
            return self.child.communicate(*args, **kwargs)
        def kill(self):
            self.child.kill()
    def start(*args, **kwargs):
        child = real_popen([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
        children.append(child)
        return TimeoutOnce(child)
    monkeypatch.setattr(storage.subprocess, 'Popen', start)
    try:
        result = storage.observe_storage(tmp_path / 'db')
        assert result.reason == 'database_unavailable'
        assert not result.preflight_ok and not result.findings
        assert children[0].poll() is not None
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=3)


def test_unrelated_directory_creation_does_not_look_like_parent_replacement(tmp_path, monkeypatch):
    from orchestrator.diagnostics import sqlite_continuity as cli_module
    from orchestrator.persistence._continuity_observation import ObservationLimits
    db = tmp_path / 'db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    real_run = cli_module._run_schema_observation
    def concurrent_directory(path, limits):
        result = real_run(path, limits)
        (tmp_path / 'unrelated').mkdir()
        return result
    monkeypatch.setattr(cli_module, '_run_schema_observation', concurrent_directory)
    report = cli_module.diagnose_store(db, ObservationLimits())
    assert 'database_unavailable' not in report.findings
    assert 'storage_continuity_unproven' in report.unknowns


def test_filesystem_only_observation_does_not_modify_fixture(tmp_path):
    from orchestrator.isolation._sqlite_storage_observation import observe_storage
    db = tmp_path / 'db'
    wal = Path(str(db) + '-wal')
    shm = Path(str(db) + '-shm')
    db.write_bytes(b'database marker')
    wal.write_bytes(b'wal marker')
    shm.write_bytes(b'shm marker')
    def state():
        return {p.name: (p.read_bytes(), p.stat().st_mtime_ns, p.stat().st_mode)
                for p in tmp_path.iterdir()}
    before = state()
    result = observe_storage(db)
    assert result.preflight_ok
    assert {'db_present', 'wal_present', 'shm_present'} <= set(result.findings)
    assert state() == before
    assert str(tmp_path) not in repr(result)


def test_cli_refuses_fifo_sidecar_without_opening_it(tmp_path):
    if not hasattr(os, 'mkfifo'):
        pytest.skip('FIFO unavailable')
    db = tmp_path / 'db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    os.mkfifo(Path(str(db) + '-wal'))
    proc, report = cli(db)
    assert proc.returncode != 0
    assert 'wal_nonregular' in report['findings']


@pytest.mark.parametrize('when,operation', [
    ('before_schema', 'create'),
    ('after_schema', 'create'),
    ('after_schema', 'replace'),
    ('both', 'create_remove'),
])
def test_cli_observes_sidecar_changes_across_helper_gaps(tmp_path, monkeypatch, capsys, when, operation):
    import selectors
    from orchestrator.diagnostics import sqlite_continuity as cli_module
    db = tmp_path / 'private-db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    sidecar = Path(str(db) + '-shm')
    replacement = tmp_path / 'replacement'
    if operation == 'replace':
        sidecar.write_bytes(b'old private contents')
        replacement.write_bytes(b'new private contents')
    script = """import os, pathlib, sys
sidecar, replacement, operation = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
for line in sys.stdin:
    if line.strip() == 'create':
        sidecar.write_bytes(b'private sidecar')
    elif line.strip() == 'replace':
        os.replace(replacement, sidecar)
    elif line.strip() == 'remove':
        sidecar.unlink()
    print('done', flush=True)
"""
    worker = subprocess.Popen([sys.executable, '-c', script, str(sidecar), str(replacement), operation],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True)
    real_run = cli_module._run_schema_observation
    def step(command):
        worker.stdin.write(command + '\n')
        worker.stdin.flush()
        with selectors.DefaultSelector() as selector:
            selector.register(worker.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=5), 'sidecar worker timed out'
            assert worker.stdout.readline().strip() == 'done'
    def coordinated(path, limits):
        if when in ('before_schema', 'both'):
            step('create')
        result = real_run(path, limits)
        if when == 'after_schema':
            step(operation)
        elif when == 'both':
            step('remove')
        return result
    monkeypatch.setattr(cli_module, '_run_schema_observation', coordinated)
    try:
        assert cli_module.main(['--database', str(db)]) == 0
        captured = capsys.readouterr()
        report = json.loads(captured.out)
        assert 'sidecar_changed' in report['unknowns']
        assert 'storage_continuity_unproven' in report['unknowns']
        assert 'sqlite_handle_binding_unproven' in report['unknowns']
        assert 'database_unavailable' not in report['findings']
        assert str(tmp_path) not in captured.out + captured.err
        assert 'private sidecar' not in captured.out + captured.err
    finally:
        worker.stdin.close()
        try:
            worker.wait(timeout=5)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(timeout=5)
            worker.stdout.close()
            worker.stderr.close()


@pytest.mark.parametrize('operation', ['mkdir', 'replace_parent'])
def test_schema_helper_parent_samples_during_query(tmp_path, monkeypatch, capsys, operation):
    from orchestrator.diagnostics import sqlite_continuity as cli_module
    from orchestrator.persistence import _continuity_observation as schema
    parent = tmp_path / 'parent'
    parent.mkdir()
    db = parent / 'db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    replacement = tmp_path / 'replacement'
    if operation == 'replace_parent':
        replacement.mkdir()
        with sqlite3.connect(replacement / 'db') as connection:
            connection.execute('CREATE TABLE other (x)')
    worker_script = """import pathlib, sys
parent, replacement, operation = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
sys.stdin.readline()
if operation == 'mkdir':
    (parent / 'unrelated').mkdir()
else:
    parent.rename(parent.with_name('old-parent'))
    replacement.rename(parent)
print('done', flush=True)
"""
    real_popen = subprocess.Popen
    worker = real_popen([sys.executable, '-c', worker_script, str(parent), str(replacement), operation],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    real_runner = cli_module._run_schema_observation
    observations = []
    helper_script = """import os, sys
from orchestrator.persistence import _continuity_observation as s
original = s._read_schema
sent = False
def sync(conn, budget, name):
    global sent
    if not sent:
        sent = True
        os.write(int(sys.argv[1]), b'go\\n')
        if os.read(int(sys.argv[2]), 5) != b'done\\n':
            raise RuntimeError('worker failed')
    return original(conn, budget, name)
s._read_schema = sync
s._helper_main()
"""
    def intercept(args, **kwargs):
        if len(args) >= 3 and args[1:3] == ['-m', 'orchestrator.persistence._continuity_observation']:
            write_fd, read_fd = worker.stdin.fileno(), worker.stdout.fileno()
            kwargs['pass_fds'] = (write_fd, read_fd)
            return real_popen([sys.executable, '-c', helper_script, str(write_fd), str(read_fd)], **kwargs)
        return real_popen(args, **kwargs)
    def record(path, limits):
        result = real_runner(path, limits)
        observations.append(result)
        return result
    monkeypatch.setattr(schema.subprocess, 'Popen', intercept)
    monkeypatch.setattr(cli_module, '_run_schema_observation', record)
    try:
        rc = cli_module.main(['--database', str(db)])
        report = json.loads(capsys.readouterr().out)
        assert len(observations) == 1
        if operation == 'mkdir':
            assert rc == 0
            assert schema.ObservationReason.DATABASE_UNAVAILABLE not in observations[0].reasons
        else:
            assert rc != 0
            assert schema.ObservationReason.DATABASE_UNAVAILABLE in observations[0].reasons
        assert 'sqlite_handle_binding_unproven' in report['unknowns']
        assert worker.wait(timeout=5) == 0
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)
        worker.stdin.close()
        worker.stdout.close()
        worker.stderr.close()


def test_real_sigint_reaps_only_owned_storage_helper(tmp_path):
    import selectors
    import signal
    import time
    bystander = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    script = """import json, subprocess, sys
from orchestrator.isolation import _sqlite_storage_observation as storage
real = subprocess.Popen
class Wrapper:
    def __init__(self, child): self.child = child
    def communicate(self, *args, **kwargs):
        print(json.dumps({'pid': self.child.pid}), flush=True)
        return self.child.communicate(*args, **kwargs)
    def kill(self): self.child.kill()
def start(*args, **kwargs):
    return Wrapper(real([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs))
storage.subprocess.Popen = start
try:
    storage.observe_storage(sys.argv[1])
except KeyboardInterrupt:
    print('interrupted', flush=True)
"""
    supervisor = subprocess.Popen([sys.executable, '-c', script, str(tmp_path / 'db')],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                  start_new_session=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(supervisor.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=8), 'supervisor did not start'
            child_pid = json.loads(supervisor.stdout.readline())['pid']
        time.sleep(0.2)
        supervisor.send_signal(signal.SIGINT)
        out, err = supervisor.communicate(timeout=5)
        assert supervisor.returncode == 0, err
        assert 'interrupted' in out
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
        assert bystander.poll() is None
    finally:
        try:
            os.killpg(supervisor.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if supervisor.poll() is None:
            supervisor.communicate(timeout=5)
        if bystander.poll() is None:
            bystander.kill()
            bystander.wait(timeout=5)


def test_interruption_reports_unconfirmed_helper_cleanup(tmp_path, monkeypatch):
    from orchestrator.isolation import _sqlite_storage_observation as storage
    class UnconfirmedChild:
        def __init__(self):
            self.calls = 0
        def communicate(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise KeyboardInterrupt()
            raise subprocess.TimeoutExpired('redacted', 0)
        def kill(self):
            raise OSError('private error')
    monkeypatch.setattr(storage.subprocess, 'Popen', lambda *a, **k: UnconfirmedChild())
    with pytest.raises(KeyboardInterrupt) as caught:
        storage.observe_storage(tmp_path / 'private-db')
    assert 'helper_cleanup_unconfirmed' in ' '.join(getattr(caught.value, '__notes__', ()))
    assert str(tmp_path) not in repr(caught.value.__notes__)


def test_schema_real_sigint_reaps_only_owned_helper(tmp_path):
    import selectors
    import signal
    import time
    bystander = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    script = """import json, subprocess, sys
from orchestrator.persistence import _continuity_observation as schema
real = subprocess.Popen
class Wrapper:
    def __init__(self, child): self.child = child
    def communicate(self, *args, **kwargs):
        print(json.dumps({'pid': self.child.pid}), flush=True)
        return self.child.communicate(*args, **kwargs)
    def kill(self): self.child.kill()
def start(*args, **kwargs):
    return Wrapper(real([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs))
schema.subprocess.Popen = start
try:
    schema._run_schema_observation(sys.argv[1], schema.ObservationLimits())
except KeyboardInterrupt as exc:
    print('interrupted', json.dumps(getattr(exc, '__notes__', [])), flush=True)
"""
    supervisor = subprocess.Popen([sys.executable, '-c', script, str(tmp_path / 'db')],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                  start_new_session=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(supervisor.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=8), 'supervisor did not start'
            child_pid = json.loads(supervisor.stdout.readline())['pid']
        time.sleep(0.2)
        supervisor.send_signal(signal.SIGINT)
        out, err = supervisor.communicate(timeout=5)
        assert supervisor.returncode == 0, err
        assert 'interrupted []' in out
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
        assert bystander.poll() is None
    finally:
        try:
            os.killpg(supervisor.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if supervisor.poll() is None:
            supervisor.communicate(timeout=5)
        if bystander.poll() is None:
            bystander.kill()
            bystander.wait(timeout=5)


@pytest.mark.parametrize('failure', ['kill', 'reap'])
def test_schema_interrupt_reports_unconfirmed_cleanup(tmp_path, monkeypatch, failure):
    from orchestrator.persistence import _continuity_observation as schema
    class UnconfirmedChild:
        def __init__(self):
            self.calls = 0
        def communicate(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise KeyboardInterrupt('original')
            if failure == 'reap':
                raise subprocess.TimeoutExpired('private error', 0)
            return b'', b''
        def kill(self):
            if failure == 'kill':
                raise OSError('private error')
    monkeypatch.setattr(schema.subprocess, 'Popen', lambda *a, **k: UnconfirmedChild())
    with pytest.raises(KeyboardInterrupt, match='original') as caught:
        schema._run_schema_observation(tmp_path / 'private-db', schema.ObservationLimits())
    assert 'helper_cleanup_unconfirmed' in getattr(caught.value, '__notes__', ())
    assert 'private' not in repr(caught.value.__notes__)


@pytest.mark.parametrize('samples,changed', [
    ([], False),
    ([[None, None]], False),
    ([[None, None], [None, None], [None, None]], False),
    ([[None, None], [[1, 2, 32768, 1], None]], False),
    ([[None, None], [None, None]], True),
])
def test_schema_helper_rejects_incomplete_or_inconsistent_success_samples(tmp_path, monkeypatch, samples, changed):
    from orchestrator.persistence import _continuity_observation as schema
    reasons = ['writer_inventory_unproven', 'cross_connection_temp_unobservable']
    if changed:
        reasons.append('sidecar_changed')
    payload = {'objects': [{'kind': 'table', 'name': 'private_payload',
                            'ddl_identity': 'opaque', 'unique': None}],
               'recursive_triggers': 0, 'reasons': reasons, 'temp_objects': [],
               'sidecar_samples': samples}
    class FakeChild:
        returncode = 0
        def communicate(self, *args, **kwargs):
            return json.dumps(payload).encode(), b''
    monkeypatch.setattr(schema.subprocess, 'Popen', lambda *a, **k: FakeChild())
    result = schema._run_schema_observation(tmp_path / 'private-db', schema.ObservationLimits())
    assert schema.ObservationReason.DATABASE_UNAVAILABLE in result.reasons
    assert not result.objects and not result.sidecar_samples
    assert 'private' not in repr(result)


@pytest.mark.parametrize('samples,changed', [
    ([], False),
    ([[None, None], [None, None]], False),
    ([[None, None], [[1, 2, 32768, 1], None]], True),
])
def test_schema_helper_error_remains_error_with_valid_optional_samples(tmp_path, monkeypatch, samples, changed):
    from orchestrator.persistence import _continuity_observation as schema
    reasons = ['writer_inventory_unproven', 'cross_connection_temp_unobservable', 'database_unavailable']
    if changed:
        reasons.append('sidecar_changed')
    payload = {'objects': [], 'recursive_triggers': None, 'reasons': reasons,
               'temp_objects': [], 'sidecar_samples': samples}
    class FakeChild:
        returncode = 0
        def communicate(self, *args, **kwargs):
            return json.dumps(payload).encode(), b''
    monkeypatch.setattr(schema.subprocess, 'Popen', lambda *a, **k: FakeChild())
    result = schema._run_schema_observation(tmp_path / 'private-db', schema.ObservationLimits())
    assert schema.ObservationReason.DATABASE_UNAVAILABLE in result.reasons
    assert len(result.sidecar_samples) == len(samples)
    assert (schema.ObservationReason.SIDECAR_CHANGED in result.reasons) == changed
    assert 'private' not in repr(result)


def test_cli_redacts_malformed_schema_helper_response(tmp_path, monkeypatch, capsys):
    from orchestrator.diagnostics import sqlite_continuity as cli_module
    from orchestrator.persistence import _continuity_observation as schema
    db = tmp_path / 'private-db'
    with sqlite3.connect(db) as connection:
        connection.execute('CREATE TABLE example (x)')
    payload = {'objects': [{'kind': 'table', 'name': 'private_payload',
                            'ddl_identity': 'opaque', 'unique': None}],
               'recursive_triggers': 0,
               'reasons': ['writer_inventory_unproven', 'cross_connection_temp_unobservable'],
               'temp_objects': [], 'sidecar_samples': []}
    real_popen = subprocess.Popen
    class FakeChild:
        returncode = 0
        def communicate(self, *args, **kwargs):
            return json.dumps(payload).encode(), b''
    def intercept(args, **kwargs):
        if len(args) >= 3 and args[1:3] == ['-m', 'orchestrator.persistence._continuity_observation']:
            return FakeChild()
        return real_popen(args, **kwargs)
    monkeypatch.setattr(schema.subprocess, 'Popen', intercept)
    rc = cli_module.main(['--database', str(db)])
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert rc != 0
    assert 'database_unavailable' in report['findings']
    assert 'private' not in captured.out + captured.err
    assert 'sqlite_handle_binding_unproven' in report['unknowns']
