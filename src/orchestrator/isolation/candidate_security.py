"""Fail-closed privilege and syscall controls for the candidate command only."""

from __future__ import annotations

import ctypes
import errno
import platform


class CandidateSecurityUnavailable(RuntimeError):
    """The command must not start with incomplete kernel restrictions."""


class _CapHeader(ctypes.Structure):
    _fields_ = [("version", ctypes.c_uint32), ("pid", ctypes.c_int)]


class _CapData(ctypes.Structure):
    _fields_ = [
        ("effective", ctypes.c_uint32), ("permitted", ctypes.c_uint32),
        ("inheritable", ctypes.c_uint32),
    ]


class _Comparison(ctypes.Structure):
    _fields_ = [
        ("arg", ctypes.c_uint32), ("op", ctypes.c_uint32),
        ("datum_a", ctypes.c_uint64), ("datum_b", ctypes.c_uint64),
    ]


def _capability_indices(libc):
    for index in range(65):
        value = libc.prctl(23, index, 0, 0, 0)  # PR_CAPBSET_READ
        if value < 0:
            if ctypes.get_errno() == errno.EINVAL and index > 0:
                return
            raise CandidateSecurityUnavailable("capability bounding set cannot be queried")
        if index == 64:
            raise CandidateSecurityUnavailable("unsupported capability width")
        yield index, value


def drop_candidate_privileges() -> None:
    """Call in the dedicated namespace child after chroot, before its exec."""

    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(38, 1, 0, 0, 0) != 0:  # PR_SET_NO_NEW_PRIVS
        raise CandidateSecurityUnavailable("no-new-privileges could not be established")
    for index, present in _capability_indices(libc):
        if present and libc.prctl(24, index, 0, 0, 0) != 0:  # PR_CAPBSET_DROP
            raise CandidateSecurityUnavailable("capability bounding set could not be cleared")
    if libc.prctl(47, 4, 0, 0, 0) != 0:  # PR_CAP_AMBIENT, CLEAR_ALL
        raise CandidateSecurityUnavailable("ambient capabilities could not be cleared")
    header = _CapHeader(0x20080522, 0)  # Linux capability ABI v3
    values = (_CapData * 2)()
    if libc.capset(ctypes.byref(header), values) != 0:
        raise CandidateSecurityUnavailable("process capabilities could not be cleared")


def verify_candidate_privileges() -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(39, 0, 0, 0, 0) != 1:  # PR_GET_NO_NEW_PRIVS
        raise CandidateSecurityUnavailable("no-new-privileges is not active")
    header = _CapHeader(0x20080522, 0)
    values = (_CapData * 2)()
    if libc.capget(ctypes.byref(header), values) != 0:
        raise CandidateSecurityUnavailable("process capabilities cannot be queried")
    if any(item.effective or item.permitted or item.inheritable for item in values):
        raise CandidateSecurityUnavailable("process still holds capabilities")
    if any(present for _, present in _capability_indices(libc)):
        raise CandidateSecurityUnavailable("process still has a capability bounding set")


def restrict_candidate_syscalls() -> None:
    """Load native-architecture seccomp rules; fork/thread creation stays usable."""

    if platform.machine().lower() not in {"x86_64", "amd64", "aarch64", "arm64"}:
        raise CandidateSecurityUnavailable("unsupported seccomp architecture")
    try:
        library = ctypes.CDLL("libseccomp.so.2", use_errno=True)
    except OSError as exc:
        raise CandidateSecurityUnavailable("libseccomp is required") from exc
    library.seccomp_init.argtypes = [ctypes.c_uint32]
    library.seccomp_init.restype = ctypes.c_void_p
    library.seccomp_release.argtypes = [ctypes.c_void_p]
    library.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    library.seccomp_syscall_resolve_name.restype = ctypes.c_int
    library.seccomp_rule_add_array.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint,
        ctypes.POINTER(_Comparison),
    ]
    library.seccomp_load.argtypes = [ctypes.c_void_p]
    context = library.seccomp_init(0x00050000 | errno.EPERM)  # Default deny, including new syscalls.
    if not context:
        raise CandidateSecurityUnavailable("seccomp filter could not be created")

    def rule(name: str, action: int, *, comparison=None, required: bool = False) -> None:
        number = library.seccomp_syscall_resolve_name(name.encode("ascii"))
        if number < 0:
            if required:
                raise CandidateSecurityUnavailable("a required syscall rule cannot be resolved")
            return  # Native-architecture omissions stay denied by default.
        pointer = None if comparison is None else ctypes.pointer(comparison)
        if library.seccomp_rule_add_array(
            context, action, number, int(comparison is not None), pointer,
        ) != 0:
            raise CandidateSecurityUnavailable("a required syscall rule could not be added")

    try:
        for name in ("read", "write", "openat", "close", "execve", "exit_group"):
            rule(name, 0x7FFF0000, required=True)
        for name in (
            "readv", "writev", "pread64", "pwrite64", "preadv", "pwritev", "preadv2", "pwritev2",
            "open", "openat2", "close_range", "stat", "fstat", "lstat", "newfstatat", "statx",
            "statfs", "fstatfs", "access", "faccessat", "faccessat2", "lseek", "getdents", "getdents64",
            "getcwd", "chdir", "fchdir", "readlink", "readlinkat", "unlink", "unlinkat", "mkdir", "mkdirat", "rmdir",
            "link", "linkat", "symlink", "symlinkat", "rename", "renameat", "renameat2",
            "chmod", "fchmod", "fchmodat", "utime", "utimes", "utimensat", "futimesat",
            "truncate", "ftruncate", "fallocate", "fsync", "fdatasync", "sync_file_range",
            "copy_file_range", "sendfile", "mmap", "mprotect", "munmap", "mremap", "madvise",
            "msync", "mincore", "brk", "pipe", "pipe2", "dup", "dup2", "dup3", "fcntl", "flock",
            "poll", "ppoll", "select", "pselect6", "epoll_create", "epoll_create1", "epoll_ctl",
            "epoll_wait", "epoll_pwait", "epoll_pwait2", "eventfd", "eventfd2", "ioctl",
            "getuid", "geteuid", "getgid", "getegid", "getgroups", "getpid", "getppid", "gettid",
            "getpgrp", "getpgid", "getsid", "setpgid", "setsid", "sched_getaffinity", "sched_yield",
            "sched_getparam", "sched_getscheduler", "sched_setaffinity", "getpriority",
            "prlimit64", "getrlimit", "getrusage", "uname", "sysinfo", "clock_gettime",
            "clock_getres", "gettimeofday", "time", "nanosleep", "clock_nanosleep", "getrandom",
            "rt_sigaction", "rt_sigprocmask", "rt_sigreturn", "rt_sigsuspend", "rt_sigpending",
            "rt_sigtimedwait", "rt_sigqueueinfo", "rt_tgsigqueueinfo", "sigaltstack", "kill", "tkill",
            "tgkill", "fork", "vfork", "execveat", "wait4", "waitid", "exit", "futex", "futex_waitv",
            "set_tid_address", "set_robust_list", "get_robust_list", "rseq", "arch_prctl", "fadvise64",
            "membarrier", "getxattr", "lgetxattr", "fgetxattr", "listxattr", "llistxattr", "flistxattr",
            "capget", "prctl", "restart_syscall", "alarm", "setitimer", "getitimer", "timer_create",
            "timer_settime", "timer_delete", "timer_gettime", "timer_getoverrun",
        ):
            rule(name, 0x7FFF0000)
        # glibc falls back to clone when clone3 reports ENOSYS. Namespace flags
        # on native clone are denied individually, while ordinary fork/threads
        # are allowed and remain in the parent's cgroup and PID namespace.
        rule("clone3", 0x00050000 | errno.ENOSYS, required=True)
        rule("clone", 0x7FFF0000, comparison=_Comparison(0, 7, 0x7E020080, 0), required=True)
        if library.seccomp_load(context) != 0:
            raise CandidateSecurityUnavailable("seccomp filter could not be installed")
    finally:
        library.seccomp_release(context)
