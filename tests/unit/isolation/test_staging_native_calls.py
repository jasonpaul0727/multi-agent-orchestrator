"""Inert syscall doubles exercise ordering; real process controls are forbidden."""
import ctypes
import importlib
import os
import resource
import subprocess
import pytest


@pytest.fixture(autouse=True)
def deny_process_controls(monkeypatch):
    def denied(*args, **kwargs):
        pytest.fail('real process control forbidden in Increment A')
    for name in ('fork', 'forkpty', 'unshare', 'posix_spawn', 'posix_spawnp'):
        if hasattr(os, name):
            monkeypatch.setattr(os, name, denied)
    monkeypatch.setattr(subprocess, 'Popen', denied)
    monkeypatch.setattr(resource, 'setrlimit', denied)
    monkeypatch.setattr(ctypes, 'CDLL', denied)


class Inert:
    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.events = []
        self.unshared = False
        self.map_values = {}
        self.errno = 1
        self.refuse = False
        self.bad_map = False
        self.parent = 321
        self.child_registered = False
        self.groups = []
    def get_errno(self):
        self.events.append('errno')
        return self.errno
    def unshare(self, flags):
        assert flags == 0x30020000
        self.events.append('unshare')
        if self.refuse:
            return -1
        self.unshared = True
        return 0
    def namespace(self, name, owner, role, phase=None):
        self.events.append(name)
        if name == 'pid_for_children':
            assert self.child_registered, 'pre-C pid_for_children read'
            return (4, 40)
        return {'user': (1, 11 if self.unshared else 10), 'mnt': (2, 21 if self.unshared else 20), 'pid': (3, 30)}[name]
    def open(self, path, flags):
        self.events.append(path)
        leaf = path.rsplit('/', 1)[-1]
        fd = os.open(self.tmp_path / leaf, os.O_CREAT | os.O_RDWR, 0o600)
        self.map_values[fd] = leaf
        return fd
    def fstat(self, fd):
        return os.fstat(fd)
    def write(self, fd, data):
        self.events.append((self.map_values[fd], data))
        return len(data)
    def read_file(self, path, owner, role):
        if self.bad_map:
            from orchestrator.isolation._staging_contract import ContractError
            raise ContractError()
        return {'/proc/self/uid_map': b'         0       1000          1\n',
                '/proc/self/gid_map': b'         0       1000          1\n',
                '/proc/self/setgroups': b'deny\n', '/proc/sys/kernel/overflowgid': b'65534\n'}[path]
    def getgroups(self):
        return [0 if group == 1000 else 65534 for group in self.groups] if self.unshared else list(self.groups)
    def getuid(self):
        return 0 if self.unshared else 1000
    def geteuid(self):
        return 0 if self.unshared else 1000
    def getgid(self):
        return 0 if self.unshared else 1000
    def getegid(self):
        return 0 if self.unshared else 1000
    def prctl(self, option, a2, a3, a4, a5):
        self.events.append(('prctl', option, a2))
        if option == 37:
            ctypes.cast(a2, ctypes.POINTER(ctypes.c_int))[0] = 1
        return 0
    def getppid(self):
        return self.parent
    def pidfd_ready(self, fd):
        return False


@pytest.fixture
def native(monkeypatch, tmp_path):
    n = importlib.import_module('orchestrator.isolation._staging_native')
    a = importlib.import_module('orchestrator.isolation._staging_a2_contract')
    o = importlib.import_module('orchestrator.isolation._staging_ownership')
    fake = Inert(tmp_path)
    monkeypatch.setattr(n, '_Syscalls', lambda: fake)
    owner = o.AcquisitionLedger()
    yield n, a, owner, fake
    owner.close_all()


def test_t_does_not_read_child_link_until_actual_registered_c(native):
    n, a, owner, fake = native
    calls = n.NativeCalls(owner, role=a.Role.T)
    fake.groups = [1000, 123]
    pre = calls.unshare_and_map(1000, 1000, (1000, 123))
    assert fake.events.count('pid') == 2
    assert 'pid_for_children' not in fake.events
    assert pre.before.pid == pre.after.pid == (3, 30)
    assert pre.before.user != pre.after.user and pre.before.mount != pre.after.mount
    assert ('uid_map', b'0 1000 1\n') in fake.events
    assert ('setgroups', b'deny\n') in fake.events
    assert ('gid_map', b'0 1000 1\n') in fake.events
    registration = a.ChildRegistration(a.Role.C, a.Role.T, 456, 0b11110)
    identity = a.NamespacePayload(((1, 11), (2, 21), (4, 40)), 1, 0)
    fake.child_registered = True
    calls.verify_registered_c(pre, registration, identity)
    assert fake.events[-2:] == ['pid_for_children', 'pid']


def test_checked_refusal_errno_is_immediate_and_only_the_failed_call(native):
    n, a, owner, fake = native
    fake.refuse = True
    calls = n.NativeCalls(owner, role=a.Role.T)
    with pytest.raises(n.NativeCallFailure) as error:
        calls.unshare_and_map(1000, 1000, ())
    assert error.value.record == a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    assert fake.events[-2:] == ['unshare', 'errno']
    assert all(not isinstance(item, tuple) for item in fake.events)


@pytest.mark.parametrize('errno', [None, 3, 4, 11, 12, 24])
def test_non_refusal_errno_never_becomes_unsupported(native, errno):
    n, a, owner, fake = native
    fake.refuse, fake.errno = True, errno
    with pytest.raises(n.NativeCallFailure) as error:
        n.NativeCalls(owner, role=a.Role.T).unshare_and_map(1000, 1000, ())
    assert error.value.record.kind is a.FailureKind.NATIVE_OPERATION_FAILURE
    assert error.value.record.errno == errno


def test_successful_unshare_followed_by_contract_error_has_null_errno(native):
    n, a, owner, fake = native
    fake.bad_map = True
    with pytest.raises(n.NativeCallFailure) as error:
        n.NativeCalls(owner, role=a.Role.T).unshare_and_map(1000, 1000, ())
    assert error.value.record == a.FailureRecord(a.Role.T, a.NativePhase.VERIFY_MAPS, a.FailureKind.CONTRACT_FAILURE, None)
    assert 'errno' not in fake.events
    assert a.derive_status(error.value.record, None, a.OwnerState.CLOSED, ()) is a.FixtureStatus.FAILED


def test_invariant_and_incomplete_registration_refuse_before_pid_child_read(native):
    n, a, owner, fake = native
    calls = n.NativeCalls(owner, role=a.Role.T)
    pre = calls.unshare_and_map(1000, 1000, ())
    identity = a.NamespacePayload(((1, 11), (2, 21), (4, 40)), 1, 0)
    with pytest.raises(n.NativeCallFailure) as error:
        calls.verify_registered_c(pre, a.ChildRegistration(a.Role.C, a.Role.T, 456, 0b01110), identity)
    assert error.value.record.kind is a.FailureKind.INVARIANT_FAILURE
    assert 'pid_for_children' not in fake.events


def test_host_cannot_change_process_controls(native):
    n, a, owner, fake = native
    calls = n.NativeCalls(owner, role=a.Role.HOST)
    for method, args in ((calls.set_subreaper, ()), (calls.set_nondumpable, ()),
                         (calls.unshare_and_map, (1000, 1000, ())), (calls.close_high_fds, (3,))):
        with pytest.raises(n.NativeCallFailure):
            method(*args)
    assert fake.events == []


def test_c_close_range_requires_owned_live_fd1_socket_before_any_dup(native, tmp_path):
    n, a, owner, fake = native
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    info = os.fstat(fd)
    owner.adopt_fd(fd, role=a.Role.C, identity=(info.st_dev, info.st_ino))
    def unexpected_dup(number):
        raise AssertionError('dup attempted before owned FD1 terminal endpoint')
    fake.dup_high = unexpected_dup
    with pytest.raises(n.NativeCallFailure) as error:
        n.NativeCalls(owner, role=a.Role.C).close_high_fds(fd)
    assert error.value.record.kind is a.FailureKind.INVARIANT_FAILURE

def test_wrong_supplied_host_identity_refuses_before_unshare(native):
    n, a, owner, fake = native
    with pytest.raises(n.NativeCallFailure) as error:
        n.NativeCalls(owner, role=a.Role.T).unshare_and_map(1001, 1000, ())
    assert error.value.record.kind is a.FailureKind.INVARIANT_FAILURE
    assert 'unshare' not in fake.events


def test_successful_unshare_with_wrong_own_pid_namespace_is_internal_failure(native):
    n, a, owner, fake = native
    real_namespace = fake.namespace
    def changed_pid(name, ledger, role, phase=None):
        identity = real_namespace(name, ledger, role, phase)
        return (3, 31) if fake.unshared and name == 'pid' else identity
    fake.namespace = changed_pid
    with pytest.raises(n.NativeCallFailure) as error:
        n.NativeCalls(owner, role=a.Role.T).unshare_and_map(1000, 1000, ())
    assert error.value.record == a.FailureRecord(a.Role.T, a.NativePhase.VERIFY_USER_MOUNT_AFTER_UNSHARE,
                                               a.FailureKind.INVARIANT_FAILURE, None)
    assert 'errno' not in fake.events


def test_child_parent_guard_accepts_zero_namespace_visible_parent_pid(native, monkeypatch):
    from types import SimpleNamespace
    from orchestrator.isolation._staging_ownership import FDKind
    n, a, owner, fake = native
    cap = owner.adopt_fd(900, role=a.Role.T, identity=(99, 900), kind=FDKind.PIDFD)
    owner.bind_parent_pidfd(cap, child=a.Role.C, parent_pid=321, visible_pid=0)
    info = SimpleNamespace(st_dev=99, st_ino=900, st_mode=0o700)
    fake.parent = 0
    fake.fstat = lambda fd: info
    fake.pidfd_matches = lambda fd, procfs_pid, ledger, role: fd == 900 and procfs_pid == 321 and ledger is owner and role is a.Role.C
    with monkeypatch.context() as patch:
        patch.setattr(os, 'fstat', lambda fd: info)
        patch.setattr(os, 'close', lambda fd: None)
        try:
            n.NativeCalls(owner, role=a.Role.C).guard_parent(cap, 0)
        finally:
            owner.close_all()
    assert ('prctl', 1, 9) in fake.events

def test_libc_load_internal_error_is_finite_failed_not_unsupported(monkeypatch):
    n = importlib.import_module('orchestrator.isolation._staging_native')
    a = importlib.import_module('orchestrator.isolation._staging_a2_contract')
    o = importlib.import_module('orchestrator.isolation._staging_ownership')
    def missing_symbol():
        raise AttributeError('unit absent ABI symbol')
    monkeypatch.setattr(n, '_Syscalls', missing_symbol)
    with pytest.raises(n.NativeCallFailure) as error:
        n.NativeCalls(o.AcquisitionLedger(), role=a.Role.T)
    assert error.value.record == a.FailureRecord(a.Role.T, a.NativePhase.LOAD_LIBC, a.FailureKind.INVARIANT_FAILURE, None)

def test_subreaper_and_nondumpable_are_fixed_checked_prctl_pairs(native):
    n, a, owner, fake = native
    n.NativeCalls(owner, role=a.Role.R).set_subreaper()
    n.NativeCalls(owner, role=a.Role.T).set_nondumpable()
    assert [event[1] for event in fake.events] == [36, 37, 4, 3]
    assert fake.events[0] == ('prctl', 36, 1) and fake.events[2] == ('prctl', 4, 0)


def test_subreaper_getter_mismatch_is_not_a_refusal(native):
    n, a, owner, fake = native
    def wrong_getter(option, a2, a3, a4, a5):
        return 0
    fake.prctl = wrong_getter
    with pytest.raises(n.NativeCallFailure) as error:
        n.NativeCalls(owner, role=a.Role.R).set_subreaper()
    assert error.value.record == a.FailureRecord(a.Role.R, a.NativePhase.VERIFY_SUBREAPER,
                                               a.FailureKind.INVARIANT_FAILURE, None)


@pytest.mark.parametrize('refuse', [False, True])
def test_close_range_inert_tracks_bulk_close_once_and_retains_fd1(native, monkeypatch, refuse):
    import stat
    from types import SimpleNamespace
    from orchestrator.isolation._staging_ownership import FDKind
    n, a, owner, fake = native
    binding = a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32)
    outside = owner.adopt_fd(20, role=a.Role.C, identity=(10, 20), kind=FDKind.DIRECTORY)
    owner.bind_outside_directory(outside, binding=binding)
    source = owner.adopt_fd(21, role=a.Role.C, identity=(10, 21), kind=FDKind.CONTROL_SOCKET)
    owner.bind_control_socket(source, peer=a.Role.T, binding=binding)
    owner.adopt_c_terminal(1, source=source)
    def info(fd):
        return SimpleNamespace(st_dev=10, st_ino=21 if fd in (1, 21) else 20,
                               st_mode=stat.S_IFSOCK if fd in (1, 21) else stat.S_IFDIR)
    fake.fstat = info
    def duplicate(fd):
        fake.events.append(('dup', fd))
        return 4096
    fake.dup_high = duplicate
    fake.set_nofile = lambda: fake.events.append(('rlimit', 1024))
    fake.inventory = lambda ledger, role: {0, 1, 2, 20, 21, 4096}
    def close_range(first, last, flags):
        fake.events.append(('close_range', first, last, flags))
        return -1 if refuse else 0
    fake.close_range = close_range
    fake.fd_closed = lambda fd: True
    closed = []
    with monkeypatch.context() as patch:
        # Synthetic descriptors/provenance only; pytest FD1 is never touched.
        patch.setattr(os, 'fstat', info)
        patch.setattr(os, 'close', lambda fd: closed.append(fd))
        try:
            calls = n.NativeCalls(owner, role=a.Role.C)
            if refuse:
                with pytest.raises(n.NativeCallFailure) as error:
                    calls.close_high_fds(outside)
                assert error.value.record.kind is a.FailureKind.NATIVE_REJECTION
                assert owner.has_debt and set(owner.open_fds) == {1, 20, 21, 4096}
            else:
                calls.close_high_fds(outside)
                assert owner.open_fds == (1,) and not owner.has_debt
            assert ('close_range', 3, 4294967295, 2) in fake.events
            assert fake.events.index(('dup', 20)) < fake.events.index(('rlimit', 1024))
        finally:
            owner.close_all()
            owner.close_all()
    assert closed == [1]

def test_parent_guard_refuses_owned_directory_masquerading_as_pidfd(native, tmp_path):
    n, a, owner, fake = native
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    info = os.fstat(fd)
    cap = owner.adopt_fd(fd, role=a.Role.T, identity=(info.st_dev, info.st_ino))
    fake.parent = 0
    with pytest.raises(n.NativeCallFailure):
        n.NativeCalls(owner, role=a.Role.C).guard_parent(cap, 0)


def test_map_close_error_is_finite_fd_close_cleanup_not_map_refusal(native, monkeypatch):
    n, a, owner, fake = native
    real_close = os.close
    failed = []
    def close(fd):
        failed.append(fd)
        raise OSError(5, 'unit close ambiguity')
    calls = n.NativeCalls(owner, role=a.Role.T)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'close', close)
            with pytest.raises(n.NativeCallFailure) as error:
                calls._map('uid_map', b'0 1000 1\n', a.NativePhase.OPEN_UID_MAP, a.NativePhase.WRITE_UID_MAP)
        assert error.value.cleanup == a.FailureRecord(a.Role.T, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None)
        assert calls.phase is a.NativePhase.FD_CLOSE and owner.has_debt
    finally:
        for fd in failed:
            real_close(fd)

def test_aggregate_read_keeps_origin_and_finite_cleanup_record(native, tmp_path, monkeypatch):
    n, a, owner, fake = native
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    info = os.fstat(fd)
    cap = owner.adopt_fd(fd, role=a.Role.T, identity=(info.st_dev, info.st_ino))
    real_close = os.close
    def read_with_cleanup():
        primary = OSError(5, 'unit read origin')
        try:
            raise primary
        finally:
            n._close_owned(owner, cap, primary)
    def close(number):
        raise OSError(13, 'unit cleanup')
    calls = n.NativeCalls(owner, role=a.Role.T)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'close', close)
            with pytest.raises(n.NativeCallFailure) as raised:
                calls._operation(a.NativePhase.VERIFY_MAPS, read_with_cleanup)
        assert raised.value.record == a.FailureRecord(a.Role.T, a.NativePhase.VERIFY_MAPS, a.FailureKind.NATIVE_OPERATION_FAILURE, 5)
        assert raised.value.cleanup == a.FailureRecord(a.Role.T, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None)
        assert calls.phase is a.NativePhase.FD_CLOSE
    finally:
        real_close(fd)


@pytest.mark.parametrize('primary_control', [False, True])
def test_map_cleanup_preserves_selected_control_identity_and_finite_origin(native, monkeypatch, primary_control):
    n, a, owner, fake = native
    class Control(BaseException):
        pass
    primary, cleanup = Control(), Control()
    def write(fd, raw):
        if primary_control:
            raise primary
        raise OSError(1, 'unit map refusal')
    def close(fd):
        raise cleanup
    fake.write = write
    calls = n.NativeCalls(owner, role=a.Role.T)
    real_close = os.close
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'close', close)
            with pytest.raises(Control) as raised:
                calls._map('uid_map', b'x', a.NativePhase.OPEN_UID_MAP, a.NativePhase.WRITE_UID_MAP)
        assert raised.value is (primary if primary_control else cleanup)
        assert owner.cause.kind is (a.FailureKind.INTERRUPTED if primary_control else a.FailureKind.NATIVE_REJECTION)
        assert owner.cleanup == a.FailureRecord(a.Role.T, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None)
        assert all(not isinstance(value, BaseException) for value in vars(owner).values())
    finally:
        for fd in owner.open_fds:
            real_close(fd)

@pytest.mark.parametrize('damage', ['wrong_kind', 'wrong_role', 'unbound', 'stale_generation', 'wrong_kernel_kind'])
def test_parent_guard_requires_exact_pidfd_capability_provenance(native, monkeypatch, damage):
    from types import SimpleNamespace
    from orchestrator.isolation._staging_ownership import FDKind
    n, a, owner, fake = native
    cap = owner.adopt_fd(900, role=a.Role.S if damage == 'wrong_role' else a.Role.T,
                         identity=(99, 1), kind=FDKind.DIRECTORY if damage == 'wrong_kind' else FDKind.PIDFD)
    fake.parent = 0
    fake.fstat = lambda fd: SimpleNamespace(st_dev=99, st_ino=1, st_mode=0o700)
    fake.pidfd_matches = lambda *args: damage != 'wrong_kernel_kind'
    if damage not in ('wrong_kind', 'wrong_role', 'unbound'):
        owner.bind_parent_pidfd(cap, child=a.Role.C, parent_pid=321, visible_pid=0)
    with monkeypatch.context() as patch:
        patch.setattr(os, 'fstat', fake.fstat)
        patch.setattr(os, 'close', lambda fd: None)
        try:
            if damage == 'stale_generation':
                owner.close_once(cap)
                owner.adopt_fd(900, role=a.Role.T, identity=(99, 1), kind=FDKind.PIDFD)
            with pytest.raises(n.NativeCallFailure):
                n.NativeCalls(owner, role=a.Role.C).guard_parent(cap, 0)
            assert not any(isinstance(event, tuple) and event[0] == 'prctl' for event in fake.events)
        finally:
            owner.close_all()


@pytest.mark.parametrize('damage', ['file_outside', 'alias_outside', 'wrong_attempt', 'unbound_terminal', 'stale_terminal'])
def test_high_fd_controls_require_bound_directory_and_specific_terminal(native, monkeypatch, damage):
    import stat
    from types import SimpleNamespace
    from orchestrator.isolation._staging_ownership import FDKind
    n, a, owner, fake = native
    binding = a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32)
    wrong_binding = a.AttemptBinding('55' * 16, '22' * 16, '33' * 16, '44' * 32)
    outside = owner.adopt_fd(20, role=a.Role.C, identity=(10, 20), kind=FDKind.DIRECTORY)
    owner.bind_outside_directory(outside, binding=wrong_binding if damage == 'wrong_attempt' else binding)
    source = owner.adopt_fd(21, role=a.Role.C, identity=(10, 21), kind=FDKind.CONTROL_SOCKET)
    owner.bind_control_socket(source, peer=a.Role.T, binding=binding)
    if damage == 'unbound_terminal':
        owner.adopt_fd(1, role=a.Role.C, identity=(10, 21), kind=FDKind.TERMINAL_SOCKET)
    else:
        terminal = owner.adopt_c_terminal(1, source=source)
    def info(fd):
        return SimpleNamespace(st_dev=10, st_ino=21 if fd in (1, 21) else 20,
            st_mode=stat.S_IFSOCK if fd in (1, 21) else stat.S_IFREG if damage == 'file_outside' else stat.S_IFDIR)
    fake.fstat = info
    def no_dup(fd):
        pytest.fail('invalid provenance reached duplicate/control operations')
    fake.dup_high = no_dup
    with monkeypatch.context() as patch:
        patch.setattr(os, 'fstat', info)
        patch.setattr(os, 'close', lambda fd: None)
        try:
            if damage == 'alias_outside':
                outside = owner.adopt_fd(22, role=a.Role.C, identity=(10, 20), kind=FDKind.DIRECTORY)
            elif damage == 'stale_terminal':
                owner.close_once(terminal)
                owner.adopt_fd(1, role=a.Role.C, identity=(10, 21), kind=FDKind.TERMINAL_SOCKET)
            with pytest.raises(n.NativeCallFailure):
                n.NativeCalls(owner, role=a.Role.C).close_high_fds(outside)
        finally:
            owner.close_all()

@pytest.mark.parametrize('fdinfo_pid', [321, 0])
def test_c_guard_uses_procfs_parent_pid_and_zero_getppid_with_actual_parser(tmp_path, monkeypatch, fdinfo_pid):
    from types import SimpleNamespace
    from orchestrator.isolation import _staging_native as n, _staging_ownership as o, _staging_a2_contract as a
    info_path = tmp_path / 'fdinfo'
    info_path.write_bytes(f'Pid:\t{fdinfo_pid}\nNSpid:\t{fdinfo_pid}\n'.encode('ascii'))
    owner = o.AcquisitionLedger()
    cap = owner.adopt_fd(900, role=a.Role.T, identity=(99, 900), kind=o.FDKind.PIDFD)
    owner.bind_parent_pidfd(cap, child=a.Role.C, parent_pid=321, visible_pid=0)
    events = []
    actual_open, actual_fstat, actual_close = os.open, os.fstat, os.close
    def mapped_open(path, flags):
        assert path == '/proc/self/fdinfo/900'
        return actual_open(info_path, flags)
    def fstat(fd):
        return SimpleNamespace(st_dev=99, st_ino=900) if fd == 900 else actual_fstat(fd)
    def prctl(*args):
        events.append(args)
        return 0
    def ready(fd):
        assert fd == 900
        events.append('ready')
        return False
    calls = object.__new__(n.NativeCalls)
    calls.owner, calls.role = owner, a.Role.C
    calls._backend = SimpleNamespace(fstat=fstat, getppid=lambda: 0, pidfd_ready=ready, prctl=prctl,
        pidfd_matches=lambda fd, procfs_pid, ledger, role: n._Syscalls.pidfd_matches(None, fd, procfs_pid, ledger, role))
    with monkeypatch.context() as patch:
        patch.setattr(os, 'open', mapped_open)
        patch.setattr(os, 'fstat', fstat)
        patch.setattr(os, 'close', lambda fd: None if fd == 900 else actual_close(fd))
        patch.setattr(os, 'readlink', lambda path: 'anon_inode:[pidfd]')
        try:
            assert n._Syscalls.pidfd_matches(None, 900, 321, owner, a.Role.C) is (fdinfo_pid == 321)
            assert n._Syscalls.pidfd_matches(None, 900, 0, owner, a.Role.C) is (fdinfo_pid == 0)
            if fdinfo_pid == 321:
                calls.guard_parent(cap, 0)
                assert events == ['ready', (1, 9, 0, 0, 0), 'ready']
            else:
                with pytest.raises(n.NativeCallFailure):
                    calls.guard_parent(cap, 0)
                assert events == []
        finally:
            owner.close_all()


@pytest.mark.parametrize('aggregate', ['read_file', 'namespace', 'pidfd_matches', 'inventory'])
def test_actual_aggregate_eio_origin_survives_cleanup_control(tmp_path, monkeypatch, aggregate):
    from types import SimpleNamespace
    from orchestrator.isolation import _staging_native as n, _staging_ownership as o, _staging_a2_contract as a
    class Stop(BaseException):
        pass
    signal = Stop()
    path = tmp_path / 'metadata'
    path.write_bytes(b'Pid:\t321\n')
    owner = o.AcquisitionLedger()
    calls = object.__new__(n.NativeCalls)
    calls.owner, calls.role = owner, a.Role.T
    actual_open, actual_fstat, actual_close = os.open, os.fstat, os.close
    stat_calls = []
    def mapped_open(name, flags):
        return actual_open(tmp_path if aggregate == 'inventory' else path, flags)
    def fstat(fd):
        stat_calls.append(fd)
        if aggregate == 'namespace' and len(stat_calls) == 2:
            raise OSError(5, 'unit aggregate body EIO')
        return actual_fstat(fd)
    def read(fd, size):
        raise OSError(5, 'unit aggregate body EIO')
    def close(fd):
        raise signal
    if aggregate == 'read_file':
        phase, method, args = a.NativePhase.VERIFY_MAPS, n._Syscalls.read_file, (None, '/proc/self/uid_map', owner, a.Role.T)
    elif aggregate == 'namespace':
        phase, method, args = a.NativePhase.READ_NAMESPACE_BEFORE, n._Syscalls.namespace, (None, 'user', owner, a.Role.T)
    elif aggregate == 'pidfd_matches':
        phase, method, args = a.NativePhase.GUARD_PARENT, n._Syscalls.pidfd_matches, (None, 900, 321, owner, a.Role.T)
    else:
        backend = SimpleNamespace(libc=SimpleNamespace(syscall=lambda *args: -1))
        phase, method, args = a.NativePhase.INVENTORY_FDS, n._Syscalls.inventory, (backend, owner, a.Role.T)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'open', mapped_open)
            patch.setattr(os, 'read', read)
            patch.setattr(os, 'fstat', fstat)
            patch.setattr(os, 'close', close)
            patch.setattr(os, 'readlink', lambda name: 'anon_inode:[pidfd]')
            patch.setattr(ctypes, 'get_errno', lambda: 5)
            with pytest.raises(Stop) as raised:
                calls._operation(phase, method, *args)
        assert raised.value is signal
        assert owner.cause == a.FailureRecord(a.Role.T, phase, a.FailureKind.NATIVE_OPERATION_FAILURE, 5)
        assert owner.cleanup == a.FailureRecord(a.Role.T, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None)
        assert calls.phase is a.NativePhase.FD_CLOSE and owner.has_debt
        assert all(not isinstance(value, BaseException) for value in vars(owner).values())
    finally:
        for fd in owner.open_fds:
            actual_close(fd)

@pytest.mark.parametrize('body_kind,close_kind,record_control', [
    ('control', 'success', False),
    ('control', 'ordinary', False),
    ('control', 'control', True),
    ('ordinary', 'success', False),
    ('ordinary', 'ordinary', False),
    ('ordinary', 'control', False),
    ('ordinary', 'control', True),
])
def test_origin_bookkeeping_fault_never_skips_close_or_displaces_selected_control(tmp_path, monkeypatch, body_kind, close_kind, record_control):
    from orchestrator.isolation import _staging_native as n, _staging_ownership as o, _staging_a2_contract as a
    class Stop(BaseException):
        pass
    body = Stop() if body_kind == 'control' else OSError(5, 'unit ordinary body')
    bookkeeping = Stop() if record_control else MemoryError('unit finite bookkeeping')
    cleanup = Stop()
    path = tmp_path / 'metadata'
    path.write_bytes(b'x')
    owner = o.AcquisitionLedger()
    calls = object.__new__(n.NativeCalls)
    calls.owner, calls.role = owner, a.Role.T
    actual_open, actual_close, actual_record = os.open, os.close, owner.record_cause
    attempts, visits = [], []
    def record(record):
        visits.append(record)
        if len(visits) == 1:
            raise bookkeeping
        actual_record(record)
    def read(fd, size):
        raise body
    def close(fd):
        attempts.append(fd)
        if close_kind == 'control':
            raise cleanup
        if close_kind == 'ordinary':
            raise OSError(13, 'unit ordinary close')
        actual_close(fd)
    selected = body if body_kind == 'control' else bookkeeping if record_control else cleanup if close_kind == 'control' else None
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'open', lambda *args: actual_open(path, os.O_RDONLY))
            patch.setattr(os, 'read', read)
            patch.setattr(os, 'close', close)
            patch.setattr(owner, 'record_cause', record)
            if selected is not None:
                with pytest.raises(Stop) as raised:
                    calls._operation(a.NativePhase.VERIFY_MAPS, n._Syscalls.read_file, None, '/proc/self/uid_map', owner, a.Role.T)
                assert raised.value is selected
            else:
                with pytest.raises(n.NativeCallFailure) as raised:
                    calls._operation(a.NativePhase.VERIFY_MAPS, n._Syscalls.read_file, None, '/proc/self/uid_map', owner, a.Role.T)
                assert raised.value.record == a.FailureRecord(a.Role.T, a.NativePhase.VERIFY_MAPS, a.FailureKind.NATIVE_OPERATION_FAILURE, 5)
        assert len(attempts) == 1
        records = tuple(owner._records.values())
        assert len(records) == 1 and records[0].attempted
        assert records[0].closed is (close_kind == 'success')
        assert owner.has_debt and not owner.all_closed and calls.phase is a.NativePhase.FD_CLOSE
        owner.close_all()
        assert len(attempts) == 1
        assert all(not isinstance(value, BaseException) for value in vars(owner).values())
    finally:
        for record in owner._records.values():
            if not record.closed:
                actual_close(record.cap.fd)


@pytest.mark.parametrize('fault_point', ['poison_debt', 'record_cleanup'])
def test_cleanup_bookkeeping_fault_preserves_body_control_and_sticky_debt(tmp_path, monkeypatch, fault_point):
    from orchestrator.isolation import _staging_native as n, _staging_ownership as o, _staging_a2_contract as a
    class Stop(BaseException):
        pass
    body = Stop()
    path = tmp_path / 'metadata'
    path.write_bytes(b'x')
    owner = o.AcquisitionLedger()
    calls = object.__new__(n.NativeCalls)
    calls.owner, calls.role = owner, a.Role.T
    actual_open, actual_close = os.open, os.close
    attempts = []
    def read(fd, size):
        raise body
    def close(fd):
        attempts.append(fd)
        raise OSError(13, 'unit close failure')
    def bookkeeping(*args):
        raise MemoryError('unit cleanup bookkeeping')
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'open', lambda *args: actual_open(path, os.O_RDONLY))
            patch.setattr(os, 'read', read)
            patch.setattr(os, 'close', close)
            patch.setattr(owner, fault_point, bookkeeping)
            with pytest.raises(Stop) as raised:
                calls._operation(a.NativePhase.VERIFY_MAPS, n._Syscalls.read_file, None, '/proc/self/uid_map', owner, a.Role.T)
        assert raised.value is body
        assert len(attempts) == 1 and all(record.attempted for record in owner._records.values())
        assert owner.has_debt and not owner.all_closed and calls.phase is a.NativePhase.FD_CLOSE
        assert all(not isinstance(value, BaseException) for value in vars(owner).values())
    finally:
        for fd in owner.open_fds:
            actual_close(fd)

@pytest.mark.parametrize('fault_point', ['poison_debt', 'record_cleanup'])
def test_cleanup_control_beats_ordinary_body_when_close_bookkeeping_fails(tmp_path, monkeypatch, fault_point):
    from orchestrator.isolation import _staging_native as n, _staging_ownership as o, _staging_a2_contract as a
    class Stop(BaseException):
        pass
    cleanup = Stop()
    path = tmp_path / 'metadata'
    path.write_bytes(b'x')
    owner = o.AcquisitionLedger()
    calls = object.__new__(n.NativeCalls)
    calls.owner, calls.role = owner, a.Role.T
    actual_open, actual_close = os.open, os.close
    attempts = []
    def read(fd, size):
        raise OSError(5, 'unit ordinary body')
    def close(fd):
        attempts.append(fd)
        raise cleanup
    def bookkeeping(*args):
        raise MemoryError('unit close bookkeeping')
    try:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'open', lambda *args: actual_open(path, os.O_RDONLY))
            patch.setattr(os, 'read', read)
            patch.setattr(os, 'close', close)
            patch.setattr(owner, fault_point, bookkeeping)
            with pytest.raises(Stop) as raised:
                calls._operation(a.NativePhase.VERIFY_MAPS, n._Syscalls.read_file, None, '/proc/self/uid_map', owner, a.Role.T)
        assert raised.value is cleanup and len(attempts) == 1
        assert owner.cause == a.FailureRecord(a.Role.T, a.NativePhase.VERIFY_MAPS, a.FailureKind.NATIVE_OPERATION_FAILURE, 5)
        assert owner.has_debt and not owner.all_closed and calls.phase is a.NativePhase.FD_CLOSE
    finally:
        for fd in owner.open_fds:
            actual_close(fd)
