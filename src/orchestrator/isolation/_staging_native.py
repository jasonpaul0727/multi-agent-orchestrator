"""Checked fixed Linux x86_64 controls. Never invoked by host metadata entry."""
import ctypes
import fcntl
import os
import resource
import select
import stat
import struct

from ._staging_a2_contract import (
    ChildRegistration, FailureKind, FailureRecord, NamespacePayload,
    NamespaceSnapshot, NativePhase, PreChildNamespaces, Role, _REFUSALS,
)
from ._staging_contract import ContractError, _integer, _require
from ._staging_ownership import AcquisitionLedger, FDKind

_LIBC = '/usr/lib/x86_64-linux-gnu/libc.so.6'
_UNSHARE = 0x30020000  # CLONE_NEWUSER | CLONE_NEWNS | CLONE_NEWPID


class NativeCallFailure(Exception):
    def __init__(self, record, *, cause=None, cleanup=None):
        self.record = record
        self.cause = cause if cause is not None else (None if record.kind is FailureKind.CLEANUP_UNKNOWN else record)
        self.cleanup = cleanup if cleanup is not None else (record if record.kind is FailureKind.CLEANUP_UNKNOWN else None)
        super().__init__('native_operation_unconfirmed')


def _adopt(owner, role, fd):
    cap = owner.adopt_fd(fd, role=role, identity=None)
    info = os.fstat(fd)
    owner.bind_identity(cap, (info.st_dev, info.st_ino))
    return cap


class _Syscalls:
    """Explicit ABI, no discovery or external helper. Construct only in helpers."""
    def __init__(self):
        self.libc = ctypes.CDLL(_LIBC, use_errno=True)
        self.libc.prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
        self.libc.prctl.restype = ctypes.c_int
        self.libc.unshare.argtypes = [ctypes.c_int]
        self.libc.unshare.restype = ctypes.c_int
        self.libc.close_range.argtypes = [ctypes.c_uint, ctypes.c_uint, ctypes.c_int]
        self.libc.close_range.restype = ctypes.c_int
        # Linux x86_64 getdents64 on our owned /proc/self/fd handle only.
        self.libc.syscall.argtypes = [ctypes.c_long, ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t]
        self.libc.syscall.restype = ctypes.c_long

    get_errno = staticmethod(ctypes.get_errno)
    open = staticmethod(os.open)
    fstat = staticmethod(os.fstat)
    write = staticmethod(os.write)
    getuid = staticmethod(os.getuid)
    geteuid = staticmethod(os.geteuid)
    getgid = staticmethod(os.getgid)
    getegid = staticmethod(os.getegid)
    getgroups = staticmethod(os.getgroups)
    getppid = staticmethod(os.getppid)

    def prctl(self, *args):
        return self.libc.prctl(*args)

    def unshare(self, flags):
        return self.libc.unshare(flags)

    def close_range(self, first, last, flags):
        return self.libc.close_range(first, last, flags)

    def namespace(self, name, owner, role, phase=NativePhase.READ_NAMESPACE_BEFORE):
        _require(name in ('user', 'mnt', 'pid', 'pid_for_children'))
        fd = _adopt(owner, role, os.open('/proc/self/ns/' + name, os.O_RDONLY | os.O_CLOEXEC))
        primary = None
        try:
            info = os.fstat(fd.fd)
            return (info.st_dev, info.st_ino)
        except BaseException as exc:
            primary = exc
            raise
        finally:
            _close_owned(owner, fd, primary, phase=phase)

    def read_file(self, path, owner, role):
        _require(path in ('/proc/self/uid_map', '/proc/self/gid_map', '/proc/self/setgroups', '/proc/sys/kernel/overflowgid'))
        fd = _adopt(owner, role, os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW))
        primary = None
        try:
            raw = os.read(fd.fd, 257)
            _require(len(raw) <= 256)
            return raw
        except BaseException as exc:
            primary = exc
            raise
        finally:
            _close_owned(owner, fd, primary, phase=NativePhase.VERIFY_MAPS)

    def pidfd_matches(self, fd, procfs_pid, owner, role):
        if os.readlink(f'/proc/self/fd/{fd}') != 'anon_inode:[pidfd]':
            return False
        cap = _adopt(owner, role, os.open(f'/proc/self/fdinfo/{fd}', os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW))
        primary = None
        try:
            raw = os.read(cap.fd, 4097)
            _require(len(raw) <= 4096)
            pids = [line.split(b':', 1)[1].strip() for line in raw.splitlines() if line.startswith(b'Pid:')]
            return pids == [str(procfs_pid).encode('ascii')]
        except BaseException as exc:
            primary = exc
            raise
        finally:
            _close_owned(owner, cap, primary, phase=NativePhase.GUARD_PARENT)
    def pidfd_ready(self, fd):
        poller = select.poll()
        poller.register(fd, select.POLLIN)
        events = poller.poll(0)
        _require(all(mask == select.POLLIN for _, mask in events))
        return bool(events)

    def dup_high(self, fd):
        return fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 4096)

    def set_nofile(self):
        resource.setrlimit(resource.RLIMIT_NOFILE, (1024, 1024))

    def inventory(self, owner, role):
        fd = _adopt(owner, role, os.open('/proc/self/fd', os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW))
        primary = None
        try:
            buffer = ctypes.create_string_buffer(16384)
            count = self.libc.syscall(217, fd.fd, buffer, len(buffer))
            if count == -1:
                err = ctypes.get_errno()
                raise OSError(err, 'fd_inventory_failed')
            _require(0 <= count < len(buffer))
            raw = buffer.raw[:count]
            names = set()
            offset = 0
            while offset < count:
                _require(offset + 19 <= count)
                length = struct.unpack_from('=H', raw, offset + 16)[0]
                _require(length >= 20 and offset + length <= count)
                name = raw[offset + 19:offset + length].split(b'\0', 1)[0]
                if name not in (b'.', b'..'):
                    _require(name.isdigit() and len(name) <= 10)
                    number = int(name)
                    _integer(number, 2**31 - 1)
                    _require(number not in names and len(names) < 128)
                    names.add(number)
                offset += length
            # One bounded additional getdents confirms exhaustion, not a truncation.
            count = self.libc.syscall(217, fd.fd, buffer, len(buffer))
            if count == -1:
                err = ctypes.get_errno()
                raise OSError(err, 'fd_inventory_failed')
            _require(count == 0)
            return names - {fd.fd}
        except BaseException as exc:
            primary = exc
            raise
        finally:
            _close_owned(owner, fd, primary, phase=NativePhase.INVENTORY_FDS)

    def fd_closed(self, fd):
        try:
            fcntl.fcntl(fd, fcntl.F_GETFD)
        except OSError as exc:
            err = exc.errno
            if err == 9:
                return True
            raise
        return False


def _record_aggregate_origin(owner, cap, primary, phase):
    """Freeze the finite body fact before cleanup can select a different object."""
    if primary is None or phase is None:
        return
    if isinstance(primary, NativeCallFailure):
        if primary.cause is not None:
            owner.record_cause(primary.cause)
        return
    errno = None
    if isinstance(primary, OSError):
        errno = primary.errno
        if type(errno) is not int or not 1 <= errno <= 4095:
            errno = None
        kind = FailureKind.NATIVE_OPERATION_FAILURE
    elif isinstance(primary, ContractError):
        kind = FailureKind.CONTRACT_FAILURE
    elif isinstance(primary, Exception):
        kind = FailureKind.INVARIANT_FAILURE
    else:
        kind = FailureKind.INTERRUPTED
    owner.record_cause(FailureRecord(owner._record(cap).role, phase, kind, errno))


def _close_owned(owner, cap, primary, *, phase=None):
    origin_failure = close_failure = debt_failure = cleanup_failure = None
    try:
        _record_aggregate_origin(owner, cap, primary, phase)
    except BaseException as exc:
        origin_failure = exc
    # Bookkeeping can fail, but cannot bypass this acquisition's once-close.
    try:
        owner.close_once(cap)
    except BaseException as exc:
        close_failure = exc
    if origin_failure is None and close_failure is None:
        return
    try:
        owner.poison_debt(NativePhase.FD_CLOSE)
    except BaseException as exc:
        debt_failure = exc
    finally:
        # The fixed field exists from construction. Preserve uncertainty even
        # when the normal debt-recording method itself failed.
        if owner._debt_phase is None:
            owner._debt_phase = NativePhase.FD_CLOSE
    finite = None
    try:
        finite = owner.record_cleanup(owner._record(cap).role)
    except BaseException as exc:
        cleanup_failure = exc
    # Apply A2 selection to the entire cleanup path, including bookkeeping.
    if primary is not None and not isinstance(primary, Exception):
        raise primary
    for failure in (origin_failure, close_failure, debt_failure, cleanup_failure):
        if failure is not None and not isinstance(failure, Exception):
            raise failure
    if isinstance(primary, NativeCallFailure):
        if primary.cleanup is None and finite is not None:
            primary.cleanup = finite
        raise primary
    if primary is not None:
        raise primary
    if finite is not None:
        raise NativeCallFailure(finite, cause=owner.cause, cleanup=finite) from None
    for failure in (origin_failure, close_failure, debt_failure, cleanup_failure):
        if failure is not None:
            raise failure

class NativeCalls:
    def __init__(self, owner, *, role):
        _require(type(owner) is AcquisitionLedger and type(role) is Role)
        self.owner, self.role = owner, role
        self.phase = NativePhase.LOAD_LIBC
        self._backend = self._operation(self.phase, _Syscalls)

    def _failure(self, kind, errno=None):
        record = FailureRecord(self.role, self.phase, kind, errno)
        self.owner.record_cause(record)
        if self.owner.cleanup is not None:
            self.phase = NativePhase.FD_CLOSE
        raise NativeCallFailure(record, cleanup=self.owner.cleanup) from None

    def _invariant(self, condition):
        if not condition:
            self._failure(FailureKind.INVARIANT_FAILURE)

    def _role(self, allowed):
        self._invariant(self.role in allowed)

    def _operation(self, phase, call, *args, refusal=False):
        self.phase = phase
        try:
            return call(*args)
        except OSError as exc:
            err = exc.errno  # captured from THIS syscall, before any cleanup.
            if type(err) is not int or not 1 <= err <= 4095:
                err = None
            kind = (FailureKind.NATIVE_REJECTION if refusal and err in _REFUSALS.get(phase, ())
                    else FailureKind.NATIVE_OPERATION_FAILURE)
            self._failure(kind, err)
        except ContractError:
            self._failure(FailureKind.CONTRACT_FAILURE)
        except NativeCallFailure as exc:
            if exc.cleanup is not None:
                self.phase = NativePhase.FD_CLOSE
            raise
        except Exception:
            self._failure(FailureKind.INVARIANT_FAILURE)
        except BaseException:
            if self.owner.cleanup is not None or self.owner.has_debt:
                self.phase = NativePhase.FD_CLOSE
            elif self.owner.cause is None:
                try:
                    self.owner.record_cause(FailureRecord(self.role, self.phase, FailureKind.INTERRUPTED, None))
                except BaseException:
                    if self.owner._debt_phase is None:
                        self.owner._debt_phase = NativePhase.FD_CLOSE
                    self.phase = NativePhase.FD_CLOSE
            raise

    def _libc(self, phase, call, *args):
        self.phase = phase
        result = self._operation(phase, call, *args, refusal=True)
        if result == -1:
            err = self._backend.get_errno()  # MUST be the immediate next operation.
            if type(err) is not int or not 1 <= err <= 4095:
                err = None
            self._failure(FailureKind.NATIVE_REJECTION if err in _REFUSALS.get(phase, ())
                          else FailureKind.NATIVE_OPERATION_FAILURE, err)
        self._invariant(type(result) is int and result >= 0)
        return result

    def set_subreaper(self):
        self.phase = NativePhase.SET_SUBREAPER
        self._role((Role.R,))
        self._libc(self.phase, self._backend.prctl, 36, 1, 0, 0, 0)
        value = ctypes.c_int()
        self._libc(NativePhase.VERIFY_SUBREAPER, self._backend.prctl, 37, ctypes.addressof(value), 0, 0, 0)
        self._invariant(value.value == 1)

    def _owned(self, cap, *, kind, role):
        try:
            return self.owner.require_current(cap, kind=kind, role=role)
        except ContractError:
            self._failure(FailureKind.INVARIANT_FAILURE)

    def guard_parent(self, parent_pidfd, expected_pid):
        self.phase = NativePhase.GUARD_PARENT
        self._role((Role.R, Role.S, Role.T, Role.C))
        _integer(expected_pid, 2**31 - 1)
        record = self._owned(parent_pidfd, kind=FDKind.PIDFD, role=Role(int(self.role) - 1))
        self._invariant(self.owner._parents.get(self.role) is parent_pidfd and record.binding is not None
                        and record.binding[0] is self.role and record.binding[2] == expected_pid
                        and (expected_pid == 0 if self.role is Role.C else expected_pid > 0))
        info = self._operation(self.phase, self._backend.fstat, parent_pidfd.fd)
        self._invariant(record.identity is not None and (info.st_dev, info.st_ino) == record.identity)
        # fdinfo uses the inherited host procfs view; getppid below uses C's
        # own PID namespace, where its outside parent is visible as zero.
        self._invariant(self._operation(self.phase, self._backend.pidfd_matches, parent_pidfd.fd, record.binding[1], self.owner, self.role))
        self._invariant(self._operation(self.phase, self._backend.getppid) == expected_pid
                        and not self._operation(self.phase, self._backend.pidfd_ready, parent_pidfd.fd))
        self._libc(self.phase, self._backend.prctl, 1, 9, 0, 0, 0)
        self._invariant(self._operation(self.phase, self._backend.getppid) == expected_pid
                        and not self._operation(self.phase, self._backend.pidfd_ready, parent_pidfd.fd))
    def set_nondumpable(self):
        self.phase = NativePhase.SET_NONDUMPABLE
        self._role((Role.R, Role.S, Role.T, Role.C))
        self._libc(self.phase, self._backend.prctl, 4, 0, 0, 0, 0)
        self._invariant(self._libc(NativePhase.VERIFY_NONDUMPABLE, self._backend.prctl, 3, 0, 0, 0, 0) == 0)

    def _snapshot(self, phase):
        return NamespaceSnapshot(*(self._operation(phase, self._backend.namespace, name, self.owner, self.role, phase)
                                   for name in ('user', 'mnt', 'pid')))

    def _map(self, leaf, raw, open_phase, write_phase):
        fd = self._operation(open_phase, self._backend.open, '/proc/self/' + leaf,
                             os.O_WRONLY | os.O_CLOEXEC | os.O_NOFOLLOW, refusal=True)
        cap = self.owner.adopt_fd(fd, role=self.role, identity=None)
        primary = None
        try:
            info = self._operation(open_phase, self._backend.fstat, fd)
            self.owner.bind_identity(cap, (info.st_dev, info.st_ino))
            count = self._operation(write_phase, self._backend.write, fd, raw, refusal=True)
            self._invariant(type(count) is int and count == len(raw))
        except BaseException as exc:
            primary = exc
            raise
        finally:
            try:
                _close_owned(self.owner, cap, primary, phase=self.phase)
            except BaseException:
                self.phase = NativePhase.FD_CLOSE
                raise

    def unshare_and_map(self, uid, gid, groups):
        self.phase = NativePhase.VALIDATE_REQUEST
        self._role((Role.T,))
        _integer(uid, 2**32 - 1)
        _integer(gid, 2**32 - 1)
        _require(uid > 0 and type(groups) is tuple and len(groups) <= 16)
        for group in groups:
            _integer(group, 2**32 - 1)
        for call, expected in ((self._backend.getuid, uid), (self._backend.geteuid, uid),
                               (self._backend.getgid, gid), (self._backend.getegid, gid)):
            self._invariant(self._operation(NativePhase.VALIDATE_REQUEST, call) == expected)
        self._invariant(self._operation(NativePhase.VALIDATE_REQUEST, self._backend.getgroups) == list(groups))
        before = self._snapshot(NativePhase.READ_NAMESPACE_BEFORE)
        self._libc(NativePhase.UNSHARE, self._backend.unshare, _UNSHARE)
        self._map('uid_map', f'0 {uid} 1\n'.encode('ascii'), NativePhase.OPEN_UID_MAP, NativePhase.WRITE_UID_MAP)
        self._map('setgroups', b'deny\n', NativePhase.OPEN_SETGROUPS, NativePhase.WRITE_SETGROUPS)
        self._map('gid_map', f'0 {gid} 1\n'.encode('ascii'), NativePhase.OPEN_GID_MAP, NativePhase.WRITE_GID_MAP)
        for path, expected in (('uid_map', [b'0', str(uid).encode(), b'1']),
                               ('gid_map', [b'0', str(gid).encode(), b'1']), ('setgroups', [b'deny'])):
            raw = self._operation(NativePhase.VERIFY_MAPS, self._backend.read_file, '/proc/self/' + path, self.owner, self.role)
            self._invariant(type(raw) is bytes and raw.split() == expected)
        overflow = self._operation(NativePhase.VERIFY_MAPS, self._backend.read_file, '/proc/sys/kernel/overflowgid', self.owner, self.role)
        self._invariant(overflow.strip() == b'65534')
        for call in (self._backend.getuid, self._backend.geteuid, self._backend.getgid, self._backend.getegid):
            self._invariant(self._operation(NativePhase.VERIFY_MAPS, call) == 0)
        self._invariant(self._operation(NativePhase.VERIFY_MAPS, self._backend.getgroups)
                        == [0 if group == gid else 65534 for group in groups])
        after = self._snapshot(NativePhase.VERIFY_USER_MOUNT_AFTER_UNSHARE)
        self._invariant(before.user != after.user and before.mount != after.mount and before.pid == after.pid)
        return PreChildNamespaces(before, after)

    def verify_registered_c(self, pre, c_registration, c_identity):
        self.phase = NativePhase.VERIFY_PID_CHILD_AFTER_REGISTER
        self._role((Role.T,))
        self._invariant(type(pre) is PreChildNamespaces and type(c_registration) is ChildRegistration
                        and type(c_identity) is NamespacePayload)
        self._invariant(c_registration.role is Role.C and c_registration.parent is Role.T
                        and c_registration.registered_role_bits == 30)
        self._invariant(c_identity.pid == 1 and c_identity.ppid == 0
                        and c_identity.identities[:2] == (pre.after.user, pre.after.mount))
        child = self._operation(self.phase, self._backend.namespace, 'pid_for_children', self.owner, self.role, self.phase)
        own_pid = self._operation(self.phase, self._backend.namespace, 'pid', self.owner, self.role, self.phase)
        self._invariant(child == c_identity.identities[2] and own_pid == pre.before.pid and child != own_pid)

    def close_high_fds(self, outside_fd):
        self.phase = NativePhase.DUP_HIGH_FD
        self._role((Role.C,))
        outside = self._owned(outside_fd, kind=FDKind.DIRECTORY, role=Role.C)
        self._invariant(self.owner._outside is outside_fd and outside_fd.fd >= 3 and outside.binding is not None)
        terminal_cap = self.owner._terminal
        terminal = self._owned(terminal_cap, kind=FDKind.TERMINAL_SOCKET, role=Role.C)
        self._invariant(terminal_cap.fd == 1 and terminal.binding is not None
                        and terminal.binding[1][0] is Role.T and terminal.binding[1][1] == outside.binding)
        source_cap = terminal.binding[0]
        source = self._owned(source_cap, kind=FDKind.CONTROL_SOCKET, role=Role.C)
        self._invariant(source.binding == terminal.binding[1] and source.identity == terminal.identity)
        terminal_info = self._operation(self.phase, self._backend.fstat, terminal_cap.fd)
        outside_info = self._operation(self.phase, self._backend.fstat, outside_fd.fd)
        self._invariant(stat.S_ISSOCK(terminal_info.st_mode) and stat.S_ISDIR(outside_info.st_mode)
                        and (terminal_info.st_dev, terminal_info.st_ino) == terminal.identity
                        and (outside_info.st_dev, outside_info.st_ino) == outside.identity)
        high = self._operation(self.phase, self._backend.dup_high, outside_fd.fd)
        high_cap = self.owner.adopt_fd(high, role=self.role, identity=None, kind=FDKind.DIRECTORY)
        info = self._operation(self.phase, self._backend.fstat, high)
        self.owner.bind_identity(high_cap, (info.st_dev, info.st_ino))
        self._invariant(high >= 4096 and (info.st_dev, info.st_ino) == outside.identity and stat.S_ISDIR(info.st_mode))
        self._operation(NativePhase.SET_NOFILE, self._backend.set_nofile)
        inventory = self._operation(NativePhase.INVENTORY_FDS, self._backend.inventory, self.owner, self.role)
        known = tuple(cap for cap, item in self.owner._records.items() if not item.closed and cap.fd >= 3)
        self._invariant(type(inventory) is set and {fd for fd in inventory if fd >= 3} == {cap.fd for cap in known})
        for cap in known:
            record = self.owner.require_current(cap)
            self._invariant(record.group is None)
            record.attempted = True
        try:
            self._libc(NativePhase.CLOSE_RANGE, self._backend.close_range, 3, 2**32 - 1, 2)
            for cap in known:
                self._invariant(self._operation(NativePhase.VERIFY_HIGH_FDS, self._backend.fd_closed, cap.fd))
                self.owner._record(cap).closed = True
        except BaseException:
            self.owner.poison_debt(self.phase)
            raise
