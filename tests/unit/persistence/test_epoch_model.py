"""Non-authoritative epoch trace replay, using only local fixtures."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


pytestmark = pytest.mark.skipif(os.name != 'posix', reason='secure dirfd traversal unavailable')


def record(kind, **updates):
    value = {'version': 1, 'kind': kind, 'instance': 'fixture-instance',
             'epoch': 'fixture-epoch', 'generation': 1}
    value.update(updates)
    return value


def records():
    return [record('guard_prepare'), record('db_prepared'), record('guard_active')]


@pytest.fixture
def private_dir(tmp_path):
    path = tmp_path / 'private'
    path.mkdir(mode=0o700)
    return path


def write_trace(directory, data):
    path = directory / 'private-trace.jsonl'
    path.write_text(''.join(json.dumps(row) + '\n' for row in data), encoding='utf-8')
    return path


def cli(directory, path, *extra):
    return subprocess.run(
        [sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
         '--simulate-journal', str(path), '--simulation-directory', str(directory), *extra],
        capture_output=True, text=True, timeout=10)


def test_cli_replays_simulation_without_database_mutation(private_dir):
    trace = write_trace(private_dir, records())
    unrelated = private_dir / 'database.db'
    unrelated.write_bytes(b'unchanged')
    before = unrelated.read_bytes()
    proc = cli(private_dir, trace)
    assert proc.returncode == 0, proc.stderr
    report = json.loads(proc.stdout)
    assert report['simulation_only'] is True
    assert report['non_authoritative'] is True
    assert report['state'] == 'ACTIVE_SIMULATED'
    assert report['transitions'] == ['ESTABLISHING', 'ESTABLISHING', 'ACTIVE_SIMULATED']
    assert unrelated.read_bytes() == before
    output = proc.stdout + proc.stderr
    assert str(private_dir) not in output
    assert 'fixture-instance' not in output and 'fixture-epoch' not in output
    assert 'eligible' not in output.lower() and 'capability' not in output.lower()


@pytest.mark.parametrize('rows', [
    [record('db_prepared'), record('guard_prepare'), record('guard_active')],
    [record('guard_prepare'), record('guard_prepare'), record('db_prepared'), record('guard_active')],
    [record('guard_prepare'), record('db_prepared', instance='other'), record('guard_active')],
    [record('guard_prepare'), record('db_prepared', epoch='other'), record('guard_active')],
    [record('guard_prepare'), record('db_prepared', generation=2), record('guard_active')],
    [record('guard_prepare'), record('db_prepared', generation=True), record('guard_active')],
    [record('guard_prepare', version=True), record('db_prepared'), record('guard_active')],
    [record('guard_prepare', instance='x' * 4096), record('db_prepared'), record('guard_active')],
    [record('guard_prepare', extra='unexpected'), record('db_prepared'), record('guard_active')],
])
def test_invalid_transitions_are_uncertain_and_cannot_recover(private_dir, rows):
    proc = cli(private_dir, write_trace(private_dir, rows))
    report = json.loads(proc.stdout)
    assert report['simulation_only'] is True
    assert report['state'] == 'UNCERTAIN'
    assert 'ACTIVE_SIMULATED' not in report['transitions']
    assert 'eligible' not in proc.stdout.lower()


def test_simulation_rejects_fifo_symlink_hardlink_oversize_and_leaks_no_path(private_dir):
    normal = write_trace(private_dir, records())
    fifo = private_dir / 'fifo'
    os.mkfifo(fifo)
    link = private_dir / 'link'
    link.symlink_to(normal)
    hard = private_dir / 'hard'
    os.link(normal, hard)
    oversized = private_dir / 'oversized'
    oversized.write_bytes(b'x' * (1024 * 1024 + 1))
    for path in (fifo, link, hard, normal, oversized):
        proc = cli(private_dir, path)
        report = json.loads(proc.stdout)
        assert proc.returncode != 0
        assert report['simulation_only'] is True
        assert report['state'] == 'UNCERTAIN'
        assert report['reason'] == 'input_unavailable'
        assert str(private_dir) not in proc.stdout + proc.stderr


def test_simulation_rejects_unapproved_root_and_directories(private_dir, tmp_path):
    trace = write_trace(private_dir, records())
    other = tmp_path / 'other'
    other.mkdir(mode=0o700)
    assert json.loads(cli(other, trace).stdout)['reason'] == 'input_unavailable'
    private_dir.chmod(0o755)
    assert json.loads(cli(private_dir, trace).stdout)['reason'] == 'input_unavailable'


def test_database_and_simulation_modes_are_exclusive(private_dir):
    trace = write_trace(private_dir, records())
    proc = cli(private_dir, trace, '--database', str(private_dir / 'missing.db'))
    assert proc.returncode != 0
    assert not (private_dir / 'missing.db').exists()


def test_model_only_activates_after_ordered_matching_prepares():
    from orchestrator.persistence._epoch_model import EpochModel, ModelState
    model = EpochModel()
    assert model.start(records()[0]) is ModelState.ESTABLISHING
    assert model.mark_db_prepared(records()[1]) is ModelState.ESTABLISHING
    assert model.activate_simulated(records()[2]) is ModelState.ACTIVE_SIMULATED
    assert not hasattr(model, 'authorize')


def test_duplicate_out_of_order_and_wrong_instance_are_uncertain():
    from orchestrator.persistence._epoch_model import EpochModel, ModelState
    model = EpochModel()
    assert model.start(records()[0]) is ModelState.ESTABLISHING
    assert model.mark_db_prepared(record('db_prepared', instance='other')) is ModelState.UNCERTAIN
    assert model.mark_db_prepared(records()[1]) is ModelState.UNCERTAIN
    assert model.activate_simulated(records()[2]) is ModelState.UNCERTAIN


def test_recovering_even_matching_old_active_records_cannot_resume_old_epoch():
    from orchestrator.persistence._epoch_model import ModelState, recover_model
    assert recover_model(records()[2], records()[1]) is ModelState.UNCERTAIN


def test_simulation_rejects_duplicate_json_keys(private_dir):
    trace = private_dir / 'duplicate'
    trace.write_text('{"version":1,"version":1,"kind":"guard_prepare",'
                     '"instance":"i","epoch":"e","generation":1}\n', encoding='utf-8')
    proc = cli(private_dir, trace)
    assert json.loads(proc.stdout)['state'] == 'UNCERTAIN'
    assert json.loads(proc.stdout)['reason'] == 'invalid_trace'


def test_simulation_read_timeout_returns_input_unavailable(private_dir, monkeypatch):
    from orchestrator.diagnostics import _simulation_trace as simulation
    trace = write_trace(private_dir, records())
    real_popen = subprocess.Popen
    children = []

    def sleeping_child(*args, **kwargs):
        child = real_popen([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(simulation, '_HELPER_TIMEOUT_SECONDS', 0.1)
    monkeypatch.setattr(simulation.subprocess, 'Popen', sleeping_child)
    result = simulation.simulate_journal(private_dir, trace)
    assert result.reason == 'input_unavailable'
    assert result.state == 'UNCERTAIN'
    assert len(children) == 1 and children[0].poll() is not None


@pytest.mark.parametrize('payload', [
    b'{"version":1,"kind":"guard_prepare","instance":"i","epoch":"e","generation":1,"extra":0}\n',
    b'{"version":1,"kind":"guard_prepare","instance":"' + b'x' * 5000 + b'","epoch":"e","generation":1}\n',
    b'{}\n' * 129,
    b'\xff\n',
    b'{"version":NaN}\n',
    b'[]\n',
])
def test_malformed_or_excessive_records_fail_closed(private_dir, payload):
    trace = private_dir / 'trace'
    trace.write_bytes(payload)
    proc = cli(private_dir, trace)
    report = json.loads(proc.stdout)
    assert proc.returncode != 0
    assert report['state'] == 'UNCERTAIN'
    assert report['reason'] == 'invalid_trace'
    assert str(private_dir) not in proc.stdout + proc.stderr


def test_duplicate_or_replayed_generation_cannot_reactivate(private_dir):
    first = records()
    stale = records()
    proc = cli(private_dir, write_trace(private_dir, first + stale))
    report = json.loads(proc.stdout)
    assert proc.returncode != 0
    assert report['state'] == 'UNCERTAIN'


def test_symlinked_root_component_is_rejected(private_dir, tmp_path):
    trace = write_trace(private_dir, records())
    alias = tmp_path / 'alias'
    alias.symlink_to(private_dir, target_is_directory=True)
    proc = cli(alias, alias / trace.name)
    assert json.loads(proc.stdout)['reason'] == 'input_unavailable'


def test_file_change_during_read_is_rejected(private_dir, monkeypatch):
    from orchestrator.diagnostics import _simulation_trace as simulation
    trace = write_trace(private_dir, records())
    real_read = os.read
    changed = False

    def changing_read(fd, count):
        nonlocal changed
        data = real_read(fd, count)
        if data and not changed:
            changed = True
            with trace.open('ab') as stream:
                stream.write(b' ')
        return data

    monkeypatch.setattr(simulation.os, 'read', changing_read)
    with pytest.raises(ValueError):
        simulation._read_trace(private_dir, trace)
    assert changed


def test_simulation_directory_rejected_with_database_mode(private_dir):
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                           '--database', str(private_dir / 'database'),
                           '--simulation-directory', str(private_dir)],
                          capture_output=True, text=True, timeout=10)
    assert proc.returncode != 0
    assert not (private_dir / 'database').exists()


def test_bad_simulation_arguments_do_not_echo_input(private_dir):
    trace = write_trace(private_dir, records())
    secret = str(private_dir / 'private-extra-argument')
    proc = cli(private_dir, trace, secret)
    assert proc.returncode != 0
    assert secret not in proc.stdout + proc.stderr


def test_nonregular_input_never_gets_a_data_open(private_dir, monkeypatch):
    from orchestrator.diagnostics import _simulation_trace as simulation
    fifo = private_dir / 'fifo-sentinel'
    os.mkfifo(fifo)
    real_open = os.open
    final_opens = []

    def tracked_open(path, flags, *args, **kwargs):
        if path == fifo.name:
            final_opens.append(flags)
        return real_open(path, flags, *args, **kwargs)

    supported = simulation.os.supports_dir_fd | {tracked_open}
    monkeypatch.setattr(simulation.os, 'open', tracked_open)
    monkeypatch.setattr(simulation.os, 'supports_dir_fd', supported)
    with pytest.raises((OSError, ValueError)):
        simulation._read_trace(private_dir, fifo)
    assert final_opens
    assert all(flags & os.O_PATH for flags in final_opens)


@pytest.mark.parametrize('failure', ['kill', 'reap'])
def test_cleanup_failure_is_explicit_and_non_authoritative(private_dir, monkeypatch, failure):
    from orchestrator.diagnostics import _simulation_trace as simulation
    trace = write_trace(private_dir, records())

    class UnreapableChild:
        returncode = None
        calls = 0
        def communicate(self, *args, **kwargs):
            self.calls += 1
            if failure == 'kill' and self.calls == 2:
                self.returncode = 0
                return b'', b''
            raise subprocess.TimeoutExpired('fixture', 0.1)
        def kill(self):
            if failure == 'kill':
                raise PermissionError('fixture')

    monkeypatch.setattr(simulation.subprocess, 'Popen', lambda *args, **kwargs: UnreapableChild())
    report = simulation.simulate_journal(private_dir, trace)
    assert report.simulation_only is True
    assert report.non_authoritative is True
    assert report.state == 'UNCERTAIN'
    assert report.reason == 'helper_cleanup_unconfirmed'


def test_interrupt_reaps_only_owned_helper(private_dir):
    trace = write_trace(private_dir, records())
    program = r'''
import os, signal, subprocess, sys, threading
from orchestrator.diagnostics import _simulation_trace as simulation
real_popen = subprocess.Popen
children = []
other = real_popen([sys.executable, '-c', 'import time; time.sleep(60)'])
def spawn(*args, **kwargs):
    child = real_popen([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
    children.append(child)
    return child
simulation.subprocess.Popen = spawn
timer = threading.Timer(0.25, lambda: os.kill(os.getpid(), signal.SIGINT))
timer.start()
try:
    try:
        simulation.simulate_journal(sys.argv[1], sys.argv[2])
    except KeyboardInterrupt:
        assert len(children) == 1 and children[0].poll() is not None
        assert other.poll() is None
        print('interrupted_owned_child_reaped')
    else:
        raise AssertionError('interrupt not propagated')
finally:
    timer.cancel()
    other.terminate()
    other.wait(timeout=5)
'''
    proc = subprocess.run([sys.executable, '-c', program, str(private_dir), str(trace)],
                          capture_output=True, text=True, timeout=10)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == 'interrupted_owned_child_reaped'


def test_unverified_procfs_never_reopens_anchor(private_dir, monkeypatch):
    from orchestrator.diagnostics import _simulation_trace as simulation
    trace = write_trace(private_dir, records())
    opened = []
    real_open = os.open

    def tracked_open(path, flags, *args, **kwargs):
        opened.append((path, flags))
        return real_open(path, flags, *args, **kwargs)

    supported = simulation.os.supports_dir_fd | {tracked_open}
    monkeypatch.setattr(simulation.os, 'open', tracked_open)
    monkeypatch.setattr(simulation.os, 'supports_dir_fd', supported)
    monkeypatch.setattr(simulation, '_procfs', lambda fd: (_ for _ in ()).throw(ValueError('fixture')))
    with pytest.raises(ValueError):
        simulation._read_trace(private_dir, trace)
    assert any(path == trace.name and flags & os.O_PATH for path, flags in opened)
    assert not any(isinstance(path, str) and path.isdecimal() for path, flags in opened)


def test_path_replacement_after_anchor_cannot_select_new_inode(private_dir, monkeypatch):
    from orchestrator.diagnostics import _simulation_trace as simulation
    trace = write_trace(private_dir, records())
    original = private_dir / 'original'
    real_open = os.open
    replaced = False

    def replacing_open(path, flags, *args, **kwargs):
        nonlocal replaced
        fd = real_open(path, flags, *args, **kwargs)
        if path == trace.name and not replaced:
            replaced = True
            trace.rename(original)
            trace.write_text('different inode', encoding='utf-8')
        return fd

    supported = simulation.os.supports_dir_fd | {replacing_open}
    monkeypatch.setattr(simulation.os, 'open', replacing_open)
    monkeypatch.setattr(simulation.os, 'supports_dir_fd', supported)
    with pytest.raises(ValueError):
        simulation._read_trace(private_dir, trace)
    assert replaced
