"""Constructive ownership never treats an attempted close as confirmed closure."""
import importlib
import os
import pytest
from test_staging_host_root import deny_process_controls


def modules():
    return (importlib.import_module('orchestrator.isolation._staging_ownership'),
            importlib.import_module('orchestrator.isolation._staging_a2_contract'))


def test_fd_is_owned_before_validation_and_close_attempt_is_once(monkeypatch, tmp_path):
    o, a = modules()
    owner = o.AcquisitionLedger()
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    cap = owner.adopt_fd(fd, role=a.Role.HOST, identity=(os.fstat(fd).st_dev, os.fstat(fd).st_ino))
    calls = []
    real = os.close
    def fail(number):
        calls.append(number)
        raise OSError(5, 'unit close uncertainty')
    with monkeypatch.context() as patch:
        patch.setattr(o.os, 'close', fail)
        with pytest.raises(OSError):
            owner.close_once(cap)
        owner.close_all()
    assert calls == [fd]
    assert owner.has_debt and not owner.all_closed
    real(fd)


def test_new_cleanup_control_identity_wins_ordinary_primary_and_closes_remaining(monkeypatch, tmp_path):
    o, a = modules()
    class Control(BaseException):
        pass
    control = Control()
    primary = RuntimeError('unit body')
    owner = o.AcquisitionLedger()
    fds = [os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY) for _ in range(2)]
    for fd in fds:
        info = os.fstat(fd)
        owner.adopt_fd(fd, role=a.Role.HOST, identity=(info.st_dev, info.st_ino))
    real = os.close
    calls = []
    def close(number):
        calls.append(number)
        if number == fds[1]:
            raise control
        real(number)
    with monkeypatch.context() as patch:
        patch.setattr(o.os, 'close', close)
        with pytest.raises(Control) as raised:
            owner.close_all(primary=primary)
    assert raised.value is control and calls == list(reversed(fds))
    assert owner.has_debt
    real(fds[1])


def test_primary_control_identity_beats_cleanup_control(monkeypatch, tmp_path):
    o, a = modules()
    class Control(BaseException):
        pass
    first, second = Control(), Control()
    owner = o.AcquisitionLedger()
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    info = os.fstat(fd)
    owner.adopt_fd(fd, role=a.Role.HOST, identity=(info.st_dev, info.st_ino))
    real = os.close
    def close(number):
        raise second
    with monkeypatch.context() as patch:
        patch.setattr(o.os, 'close', close)
        with pytest.raises(Control) as raised:
            owner.close_all(primary=first)
    assert raised.value is first
    real(fd)


def test_no_token_debt_blocks_entire_pool_and_close_keeps_known_ownership():
    o, a = modules()
    state = o.HostState()
    placeholder = object()
    state.install_bootstrap(placeholder)
    assert state.busy and state.entries == (placeholder, None)
    state.poison_debt(a.NativePhase.REOPEN_ROOTS)
    assert state.has_debt and not state.busy
    with pytest.raises(a.ContractError):
        state.install_bootstrap(object())
    assert state.entries == (placeholder, None)
    assert state.close_outcome() is o.HostCloseState.RETAINED



def test_returned_arena_fds_are_adopted_without_a_new_lazy_import(monkeypatch, tmp_path):
    import builtins
    o, a = modules()
    from orchestrator.isolation._staging_arena import SlotArena, create_fixture_roots
    roots = create_fixture_roots(tmp_path)
    arena = SlotArena.open_existing(roots.arena_fd)
    owner = o.AcquisitionLedger()
    real_import = builtins.__import__
    def no_import_after_return(name, *args, **kwargs):
        if name.endswith('_staging_arena'):
            raise RuntimeError('fallible import after returned arena')
        return real_import(name, *args, **kwargs)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(builtins, '__import__', no_import_after_return)
            owner.adopt_returned_arena(arena)
        assert len(owner.open_fds) == 4
    finally:
        owner.close_all()
        arena.close()
        roots.close()

def test_stale_same_inode_acquisition_close_does_not_close_new_generation(tmp_path):
    o, a = modules()
    owner = o.AcquisitionLedger()
    old_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    info = os.fstat(old_fd)
    old = owner.adopt_fd(old_fd, role=a.Role.HOST, identity=(info.st_dev, info.st_ino))
    owner.close_once(old)
    new_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    assert new_fd == old_fd
    new = owner.adopt_fd(new_fd, role=a.Role.HOST, identity=(info.st_dev, info.st_ino))
    try:
        owner.close_once(old)
        assert os.fstat(new_fd).st_ino == info.st_ino
    finally:
        owner.close_once(new)


def test_returned_arena_has_one_close_path_even_after_number_reuse(tmp_path):
    o, a = modules()
    from orchestrator.isolation._staging_arena import SlotArena, create_fixture_roots
    roots = create_fixture_roots(tmp_path)
    arena = SlotArena.open_existing(roots.arena_fd)
    owner = o.AcquisitionLedger()
    owner.adopt_returned_arena(arena)
    owner.close_all()
    reused = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            arena.close()
        except OSError:
            pass
        assert os.fstat(reused).st_ino == os.stat(tmp_path).st_ino
    finally:
        try:
            os.close(reused)
        except OSError:
            pass
        roots.close()

@pytest.mark.parametrize('group_kind', ['arena', 'roots'])
def test_delegated_group_uncertainty_does_not_enable_second_raw_close(tmp_path, monkeypatch, group_kind):
    o, a = modules()
    from orchestrator.isolation import _staging_arena as v, _staging_reopen as r
    original = v.create_fixture_roots(tmp_path)
    owner = o.AcquisitionLedger()
    if group_kind == 'arena':
        obj = owner.adopt_returned_arena(v.SlotArena.open_existing(original.arena_fd))
        target = obj._arena_fd
        reopen_path = tmp_path / 'staging-fixture/arena'
    else:
        original.close()
        obj = r.open_fixture_roots_existing(tmp_path, owner=owner)
        target = obj._parent_fd
        reopen_path = tmp_path
    real_close = os.close
    attempts = []
    def ambiguous(fd):
        attempts.append(fd)
        real_close(fd)
        if fd == target:
            raise OSError(5, 'closed but outcome ambiguous')
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'close', ambiguous)
            with pytest.raises(OSError):
                owner.close_all()
        assert owner.has_debt and not owner.all_closed
        reused = os.open(reopen_path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            assert reused == target
            obj.close()
            owner.close_all()
            assert os.fstat(reused).st_ino == os.stat(reopen_path).st_ino
            assert attempts.count(target) == 1
        finally:
            real_close(reused)
    finally:
        original.close()


def test_roots_opaque_success_that_skips_one_close_remains_debt(tmp_path, monkeypatch):
    o, a = modules()
    from orchestrator.isolation import _staging_arena as v, _staging_reopen as r
    original = v.create_fixture_roots(tmp_path)
    original.close()
    owner = o.AcquisitionLedger()
    roots = r.open_fixture_roots_existing(tmp_path, owner=owner)
    target = roots._parent_fd
    real_fstat = os.fstat
    calls = []
    def stat_with_a1_skip(fd):
        if fd == target:
            calls.append(fd)
            if len(calls) == 2:
                raise OSError(5, 'A1 intentionally ignores this fstat failure')
        return real_fstat(fd)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'fstat', stat_with_a1_skip)
            with pytest.raises(Exception):
                owner.close_all()
        assert owner.has_debt and not owner.all_closed
        assert real_fstat(target).st_ino == os.stat(tmp_path).st_ino
        roots.close()
        owner.close_all()
        assert real_fstat(target).st_ino == os.stat(tmp_path).st_ino
    finally:
        os.close(target)

@pytest.mark.parametrize('fault_point', ['poison_debt', 'record_cleanup'])
def test_group_close_control_survives_uncertainty_bookkeeping_failure(tmp_path, monkeypatch, fault_point):
    o, a = modules()
    from orchestrator.isolation import _staging_arena as v
    class Stop(BaseException):
        pass
    signal = Stop()
    roots = v.create_fixture_roots(tmp_path)
    arena = v.SlotArena.open_existing(roots.arena_fd)
    owner = o.AcquisitionLedger()
    owner.adopt_returned_arena(arena)
    expected = set(owner.open_fds)
    target = arena._record_fds[-1]
    actual_close = os.close
    attempts = []
    def close(fd):
        attempts.append(fd)
        if fd == target:
            raise signal
        actual_close(fd)
    def bookkeeping(*args):
        raise MemoryError('unit group uncertainty bookkeeping')
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'close', close)
            patch.setattr(owner, fault_point, bookkeeping)
            with pytest.raises(Stop) as raised:
                owner.close_all()
        assert raised.value is signal
        assert set(attempts) == expected and len(attempts) == 4
        assert owner.has_debt and not owner.all_closed
        assert all(record.attempted and not record.closed for record in owner._records.values())
        owner.close_all()
        arena.close()
        assert len(attempts) == 4
        assert all(not isinstance(value, BaseException) for value in vars(owner).values())
    finally:
        actual_close(target)
        roots.close()
