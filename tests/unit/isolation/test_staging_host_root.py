"""Isolated module executions test canonical lifetime without resetting live roots."""
import builtins
import ctypes
import importlib
import importlib.util
import os
from pathlib import Path
import resource
import subprocess
import sys
import threading
import types
import pytest

_CANONICAL = 'orchestrator.isolation._staging_gate'
_PATH = Path(__file__).parents[3] / 'src/orchestrator/isolation/_staging_gate.py'


@pytest.fixture(autouse=True)
def deny_process_controls(monkeypatch):
    def denied(*args, **kwargs):
        pytest.fail('real process control forbidden in metadata root tests')
    monkeypatch.setattr(subprocess, 'Popen', denied)
    monkeypatch.setattr(resource, 'setrlimit', denied)
    real_cdll = ctypes.CDLL
    def metadata_only_library(*args, **kwargs):
        library = real_cdll(*args, **kwargs)
        for name in ('prctl', 'unshare', 'close_range', 'setrlimit', 'fork'):
            setattr(library, name, denied)
        return library
    monkeypatch.setattr(ctypes, 'CDLL', metadata_only_library)
    for name in ('fork', 'forkpty', 'unshare', 'posix_spawn', 'posix_spawnp'):
        if hasattr(os, name):
            monkeypatch.setattr(os, name, denied)


def execute(monkeypatch, name=_CANONICAL):
    spec = importlib.util.spec_from_file_location(name, _PATH)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    exec(compile(_PATH.read_bytes(), str(_PATH), 'exec'), module.__dict__)
    return module


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    a = importlib.import_module('orchestrator.isolation._staging_arena')
    c = importlib.import_module('orchestrator.isolation._staging_contract')
    roots = a.create_fixture_roots(tmp_path)
    arena = a.SlotArena.open_existing(roots.arena_fd)
    arena.mark_unknown(arena.reserve(), c.ReasonCode.CONTROL_LOST)
    arena.close()
    roots.close()
    module = execute(monkeypatch)
    monkeypatch.setattr(module, '_PRESERVED_PARENT', str(tmp_path))
    yield module
    root = module._HOST_ROOT
    # Test-local disposal of an isolated, poisoned context's known handles only.
    ledger = root._bootstrap.get('owner')
    if ledger is not None:
        ledger.close_all()


def test_missing_module_is_setup_evidence():
    assert _PATH.is_file(), 'Increment A canonical metadata root is missing'


def test_metadata_root_same_identity_eight_fds_no_submission_and_permanent_tombstone(isolated):
    m = isolated
    root = m.NativeHostRoot.open_preserved()
    assert root is m._HOST_ROOT and m.NativeHostRoot.open_preserved() is root
    assert len(root._bootstrap['owner'].open_fds) == 8
    for forbidden in ('submit', 'probe_native_prerequisites', 'begin_retention_fixture', 'observe_attempt', 'converge_retention_fixture'):
        assert not hasattr(root, forbidden)
    assert root.close().value == 'CLOSED'
    assert m._HOST_ROOT is root and root.close().value == 'CLOSED'
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()
    with pytest.raises(RuntimeError):
        m.NativeHostRoot()


def test_bootstrap_exists_before_lazy_import_failure_and_retains_unreserved_debt(isolated, monkeypatch):
    m = isolated
    root = m._HOST_ROOT
    real_import = builtins.__import__
    imports = []
    class Control(BaseException):
        pass
    control = Control()
    def failing(name, *args, **kwargs):
        imports.append(name)
        if name.endswith('_staging_ownership'):
            assert m._HOST_ROOT is root and root._entries[0] is root._bootstrap and root._busy
            raise control
        return real_import(name, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(builtins, '__import__', failing)
        with pytest.raises(Control) as error:
            m.NativeHostRoot.open_preserved()
        def no_fd_operation(*args, **kwargs):
            pytest.fail('bootstrap debt close attempted FD work')
        patch.setattr(os, 'open', no_fd_operation)
        patch.setattr(os, 'close', no_fd_operation)
        assert root.close() is m.CloseOutcome.RETAINED
        assert sum(name.endswith('_staging_ownership') for name in imports) == 1
    assert error.value is control
    assert root._debt and root.close() is m.CloseOutcome.RETAINED
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()


def test_opaque_arena_constructor_failure_keeps_known_roots_without_touching_slot1(isolated, monkeypatch):
    m = isolated
    from orchestrator.isolation import _staging_arena as a
    record = Path(m._PRESERVED_PARENT) / 'staging-fixture/arena/slot-1.record'
    before = record.read_bytes()
    def fail(*args, **kwargs):
        raise RuntimeError('opaque constructor has not returned')
    monkeypatch.setattr(a.SlotArena, 'open_existing', fail)
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()
    root = m._HOST_ROOT
    assert root._debt and len(root._bootstrap['owner'].open_fds) == 4
    assert root.close().value == 'RETAINED' and record.read_bytes() == before


def test_alias_reload_origin_pid_thread_and_reentry_refuse(isolated, monkeypatch):
    m = isolated
    root = m.NativeHostRoot.open_preserved()
    with pytest.raises(RuntimeError):
        execute(monkeypatch, 'orchestrator.isolation._alias_gate')
    with pytest.raises(RuntimeError):
        exec(compile(_PATH.read_bytes(), str(_PATH), 'exec'), m.__dict__)
    assert m._HOST_ROOT is root
    old_origin = m.__spec__.origin
    m.__spec__.origin = '/untrusted/gate.py'
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()
    m.__spec__.origin = old_origin
    old_pid = root._pid
    root._pid = -1
    with pytest.raises(RuntimeError):
        root.close()
    root._pid = old_pid
    results = []
    def other_thread():
        try:
            m.NativeHostRoot.open_preserved()
        except RuntimeError:
            results.append('refused')
    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join()
    assert results == ['refused']
    root._busy = True
    assert root.close().value == 'RETAINED'
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()
    root._busy = False
    assert root.close().value == 'CLOSED'


def test_close_uncertainty_is_once_only_poison_and_retains_tombstone(isolated, monkeypatch):
    m = isolated
    root = m.NativeHostRoot.open_preserved()
    owner = root._bootstrap['owner']
    fd = owner.open_fds[-1]
    real_close = os.close
    calls = []
    def uncertain(number):
        calls.append(number)
        if number == fd:
            raise OSError(5, 'unit close uncertainty')
        real_close(number)
    with monkeypatch.context() as patch:
        patch.setattr(os, 'close', uncertain)
        assert root.close().value == 'POISONED'
        assert root.close().value == 'POISONED'
    assert calls.count(fd) == 1 and root._debt and fd in owner.open_fds and len(owner.open_fds) == 4
    real_close(fd)
    assert m._HOST_ROOT is root


def test_manifest_contains_implemented_a_sources_only():
    from orchestrator.isolation._staging_contract import _MANIFEST_SOURCES, _CODE_ROOT
    declared = {path for _, path, _ in _MANIFEST_SOURCES}
    names = ('_staging_a2_contract.py', '_staging_ownership.py', '_staging_native.py',
             '_staging_reopen.py', '_staging_audit.py', '_staging_gate.py')
    for name in names:
        assert str(_CODE_ROOT / name) in declared, f'unmanifested implemented source: {name}'
    for role, path, kind in _MANIFEST_SOURCES:
        assert Path(path).exists()
        if role.startswith('code_'):
            assert not Path(path).name in ('_staging_protocol.py', '_staging_supervisor.py', '_staging_entry.py', '_staging_retention_entry.py', '_staging_release.py')

def test_interpreter_origin_and_non_main_interpreter_refuse_before_metadata(isolated, monkeypatch):
    m = isolated
    fake = types.SimpleNamespace(__spec__=types.SimpleNamespace(origin='/untrusted/subinterpreters.so'),
                                 get_current=lambda: 0, get_main=lambda: 0)
    monkeypatch.setitem(sys.modules, '_xxsubinterpreters', fake)
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()
    assert m._HOST_ROOT._debt and m._HOST_ROOT._bootstrap['owner'] is None
    with pytest.raises(RuntimeError):
        m._HOST_ROOT.close()




def test_controlled_import_failure_after_ledger_creation_latches_both_debts(isolated, monkeypatch):
    m = isolated
    real_import = builtins.__import__
    def fail(name, *args, **kwargs):
        if name == '' and '_staging_audit' in kwargs.get('fromlist', ()):
            raise RuntimeError('unit controlled audit import')
        if len(args) >= 3 and '_staging_audit' in (args[2] or ()):
            raise RuntimeError('unit controlled audit import')
        return real_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', fail)
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()
    root = m._HOST_ROOT
    assert root._debt and root._bootstrap['owner'].has_debt and root._state.has_debt
    assert root.close() is m.CloseOutcome.RETAINED

def test_ready_root_consults_sticky_actual_ledger_debt_before_open_or_close(isolated):
    m = isolated
    root = m.NativeHostRoot.open_preserved()
    owner = root._bootstrap['owner']
    owner.poison_debt(root._deps['NativePhase'].HISTORY_COMMIT)
    before = owner.open_fds
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()
    assert root.close() is m.CloseOutcome.RETAINED
    assert owner.open_fds == before


@pytest.mark.parametrize('constructor', ['AcquisitionLedger', 'HostState'])
def test_partial_bootstrap_constructor_control_identity_and_debt(isolated, monkeypatch, constructor):
    m = isolated
    from orchestrator.isolation import _staging_ownership as o
    class Control(BaseException):
        pass
    signal = Control()
    def fail():
        raise signal
    monkeypatch.setattr(o, constructor, fail)
    with pytest.raises(Control) as raised:
        m.NativeHostRoot.open_preserved()
    assert raised.value is signal
    assert m._HOST_ROOT._debt and m._HOST_ROOT.close() is m.CloseOutcome.RETAINED


def test_matching_foreign_file_and_origin_refuse_before_controlled_bootstrap(isolated, monkeypatch):
    m = isolated
    entered = []
    def load(root):
        entered.append(True)
        raise RuntimeError('controlled bootstrap reached')
    monkeypatch.setattr(m, '__file__', '/untrusted/gate.py')
    monkeypatch.setattr(m.__spec__, 'origin', '/untrusted/gate.py')
    monkeypatch.setattr(m.NativeHostRoot, '_load_main_interpreter', load)
    with pytest.raises(RuntimeError):
        m.NativeHostRoot.open_preserved()
    assert entered == []


def test_secondary_native_thread_cannot_install_canonical_root(monkeypatch):
    results = []
    def attempt():
        try:
            module = execute(monkeypatch)
        except RuntimeError:
            results.append('refused')
        else:
            results.append('installed')
    thread = threading.Thread(target=attempt)
    thread.start()
    thread.join()
    assert results == ['refused']
