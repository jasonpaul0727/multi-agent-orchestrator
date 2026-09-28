import ctypes
import errno

import pytest

from orchestrator.isolation import candidate_security as module


class _Libc:
    def __init__(self, *, present=False, failure=None, capget=0, capset=0, held=False, nnp=1):
        self.present = present
        self.failure = failure
        self.capget_result = capget
        self.capset_result = capset
        self.held = held
        self.nnp = nnp
        self.dropped = []

    def prctl(self, operation, index, *_args):
        if operation == self.failure:
            return -1
        if operation == 23:
            if index == 3:
                ctypes.set_errno(errno.EINVAL)
                return -1
            return int(self.present)
        if operation == 24:
            self.dropped.append(index)
        return self.nnp if operation == 39 else 0

    def capget(self, _header, values):
        values[1].inheritable = int(self.held)
        return self.capget_result

    def capset(self, header, values):
        assert header._obj.version == 0x20080522
        assert all(not item.effective and not item.permitted and not item.inheritable for item in values)
        return self.capset_result


def test_all_capability_sets_are_cleared_and_verified(monkeypatch) -> None:
    libc = _Libc(present=True)
    monkeypatch.setattr(module.ctypes, "CDLL", lambda *_a, **_k: libc)
    module.drop_candidate_privileges()
    assert libc.dropped == [0, 1, 2]
    libc.present = False
    module.verify_candidate_privileges()


@pytest.mark.parametrize("options", [{"failure": 38}, {"present": True, "failure": 24}, {"failure": 47}, {"capset": -1}])
def test_privilege_drop_failure_is_fatal(monkeypatch, options) -> None:
    monkeypatch.setattr(module.ctypes, "CDLL", lambda *_a, **_k: _Libc(**options))
    with pytest.raises(module.CandidateSecurityUnavailable):
        module.drop_candidate_privileges()


@pytest.mark.parametrize("options", [{"nnp": 0}, {"capget": -1}, {"held": True}, {"present": True}])
def test_verification_rejects_any_remaining_authority(monkeypatch, options) -> None:
    monkeypatch.setattr(module.ctypes, "CDLL", lambda *_a, **_k: _Libc(**options))
    with pytest.raises(module.CandidateSecurityUnavailable):
        module.verify_candidate_privileges()


@pytest.mark.parametrize("reason", ["query", "width"])
def test_capability_query_failure_or_future_width_fails_closed(reason) -> None:
    class Unknown:
        def prctl(self, *_args):
            ctypes.set_errno(errno.EPERM)
            return -1 if reason == "query" else 0
    with pytest.raises(module.CandidateSecurityUnavailable):
        list(module._capability_indices(Unknown()))


class _Function:
    def __init__(self, function):
        self.function = function
    def __call__(self, *args):
        return self.function(*args)


class _Seccomp:
    def __init__(self, *, missing=None, init=1, add=0, load=0):
        self.names = []
        self.rules = []
        self.defaults = []
        self.released = []
        self.seccomp_init = _Function(lambda action: self.defaults.append(action) or init)
        self.seccomp_release = _Function(self.released.append)
        self.seccomp_syscall_resolve_name = _Function(lambda name: self.resolve(name, missing))
        self.seccomp_rule_add_array = _Function(lambda ctx, action, number, count, pointer: self.add(action, number, count, pointer, add))
        self.seccomp_load = _Function(lambda _ctx: load)

    def resolve(self, name, missing):
        self.names.append(name.decode())
        return -1 if name.decode() == missing else len(self.names) - 1

    def add(self, action, number, count, pointer, result):
        comparison = None if not count else (pointer.contents.arg, pointer.contents.op, pointer.contents.datum_a, pointer.contents.datum_b)
        self.rules.append((self.names[number], action, comparison))
        return result


def test_filter_default_denies_new_syscalls_and_namespace_creation(monkeypatch) -> None:
    library = _Seccomp(missing="stat")  # Optional native-architecture omission.
    monkeypatch.setattr(module.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(module.ctypes, "CDLL", lambda *_a, **_k: library)
    module.restrict_candidate_syscalls()
    assert library.defaults == [0x00050000 | errno.EPERM]
    assert library.released == [1]
    assert ("clone", 0x7FFF0000, (0, 7, 0x7E020080, 0)) in library.rules
    assert ("clone3", 0x00050000 | errno.ENOSYS, None) in library.rules
    allowed = {name for name, action, _comparison in library.rules if action == 0x7FFF0000}
    assert {"read", "write", "execve", "getcwd"} <= allowed
    assert not {"socket", "mount", "setns", "unshare", "chroot", "setxattr", "fsetxattr", "lsetxattr", "removexattr", "io_uring_setup", "bpf"} & allowed


@pytest.mark.parametrize("options", [{"missing": "read"}, {"init": 0}, {"add": -1}, {"load": -1}])
def test_filter_install_failure_does_not_silently_continue(monkeypatch, options) -> None:
    library = _Seccomp(**options)
    monkeypatch.setattr(module.platform, "machine", lambda: "aarch64")
    monkeypatch.setattr(module.ctypes, "CDLL", lambda *_a, **_k: library)
    with pytest.raises(module.CandidateSecurityUnavailable):
        module.restrict_candidate_syscalls()
    assert library.released == ([] if options.get("init") == 0 else [1])


def test_filter_requires_supported_architecture_and_library(monkeypatch) -> None:
    monkeypatch.setattr(module.platform, "machine", lambda: "unsupported")
    with pytest.raises(module.CandidateSecurityUnavailable, match="architecture"):
        module.restrict_candidate_syscalls()
    monkeypatch.setattr(module.platform, "machine", lambda: "amd64")
    def missing(*_a, **_k):
        raise OSError("missing")
    monkeypatch.setattr(module.ctypes, "CDLL", missing)
    with pytest.raises(module.CandidateSecurityUnavailable, match="libseccomp"):
        module.restrict_candidate_syscalls()
