import os
import select
import subprocess
import sys
import pytest
from orchestrator.isolation._broker_ownership import BrokerOwnership, OwnershipState

pytestmark = pytest.mark.skipif(os.name != "posix" or not sys.platform.startswith("linux"), reason="Linux flock required")

def make_lock(tmp_path):
    path = tmp_path / "broker.lock"
    path.touch(mode=0o600)
    return path

def acquire(path):
    return BrokerOwnership.acquire(path, store_id="store-a", namespace="manager-a")

def test_live_owner_excludes_an_independent_process_and_releases(tmp_path):
    path = make_lock(tmp_path)
    owner = acquire(path)
    assert owner.observe() is OwnershipState.OBSERVED_LOCKED
    code = "from orchestrator.isolation._broker_ownership import BrokerOwnership; import sys; x=BrokerOwnership.acquire(sys.argv[1], store_id='store-a', namespace='manager-a'); print(x.observe().value, flush=True); x.close()"
    def child():
        return subprocess.check_output([sys.executable, "-c", code, str(path)], text=True, env={**os.environ, "PYTHONPATH": os.path.abspath("src")}, timeout=10).strip()
    assert child() == "unknown"
    owner.close()
    assert child() == "observed_locked"

def test_replaced_inode_permanently_invalidates_old_owner(tmp_path):
    path = make_lock(tmp_path)
    owner = acquire(path)
    assert owner.observe() is OwnershipState.OBSERVED_LOCKED
    path.unlink()
    path.touch(mode=0o600)
    other = acquire(path)
    assert other.observe() is OwnershipState.OBSERVED_LOCKED
    assert owner.observe() is OwnershipState.UNKNOWN
    other.close()
    assert owner.observe() is OwnershipState.UNKNOWN
    owner.close()

def test_symlink_and_hardlink_are_unknown(tmp_path):
    path = make_lock(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(path)
    assert acquire(link).observe() is OwnershipState.UNKNOWN
    os.link(path, tmp_path / "hard")
    assert acquire(path).observe() is OwnershipState.UNKNOWN

def test_explicit_unlock_invalidates_owner(tmp_path):
    import fcntl
    owner = acquire(make_lock(tmp_path))
    assert owner.observe() is OwnershipState.OBSERVED_LOCKED
    fcntl.flock(owner._fd, fcntl.LOCK_UN)
    assert owner.observe() is OwnershipState.UNKNOWN
    owner.close()


def test_forbidden_unlock_allows_another_lock_to_fool_observation(tmp_path):
    # Demonstrates a limit, never a continuity or dispatch proof.
    import fcntl
    path = make_lock(tmp_path)
    owner = acquire(path)
    other = None
    try:
        fcntl.flock(owner._fd, fcntl.LOCK_UN)
        other = acquire(path)
        assert other.observe() is OwnershipState.OBSERVED_LOCKED
        assert owner.observe() is OwnershipState.OBSERVED_LOCKED
    finally:
        if other is not None:
            other.close()
        owner.close()


def test_forbidden_fd_reuse_makes_close_affect_replacement(tmp_path):
    path = make_lock(tmp_path)
    owner = acquire(path)
    original_fd = owner._fd
    os.close(original_fd)
    replacement = os.open(path, os.O_RDWR)
    if replacement != original_fd:
        os.dup2(replacement, original_fd)
        os.close(replacement)
        replacement = original_fd
    assert owner.observe() is OwnershipState.UNKNOWN
    owner.close()
    with pytest.raises(OSError):
        os.fstat(replacement)


def test_sigkill_releases_independent_owner(tmp_path):
    import signal
    path = make_lock(tmp_path)
    code = ("from orchestrator.isolation._broker_ownership import BrokerOwnership; "
            "import sys; x=BrokerOwnership.acquire(sys.argv[1], store_id='store-a', namespace='manager-a'); "
            "print(x.observe().value, flush=True); sys.stdin.read()")
    env = {**os.environ, "PYTHONPATH": os.path.abspath("src")}
    child = subprocess.Popen([sys.executable, "-c", code, str(path)], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, text=True, env=env)
    try:
        assert select.select([child.stdout], [], [], 10)[0], "child did not report acquisition"
        assert child.stdout.readline().strip() == "observed_locked"
        assert acquire(path).observe() is OwnershipState.UNKNOWN
        child.send_signal(signal.SIGKILL)
        child.wait(timeout=10)
        owner = acquire(path)
        assert owner.observe() is OwnershipState.OBSERVED_LOCKED
        owner.close()
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        child.stdin.close()
        child.stdout.close()


def test_forked_child_is_unknown_and_parent_stays_held(tmp_path):
    path = make_lock(tmp_path)
    owner = acquire(path)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            os.write(write_fd, owner.observe().value.encode())
        finally:
            os._exit(0)
    os.close(write_fd)
    reaped = False
    try:
        assert select.select([read_fd], [], [], 10)[0], "forked child did not report"
        assert os.read(read_fd, 32) == b"unknown"
        _, status = os.waitpid(pid, 0)
        reaped = True
        assert os.waitstatus_to_exitcode(status) == 0
        assert owner.observe() is OwnershipState.OBSERVED_LOCKED
    finally:
        if not reaped:
            import signal
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        os.close(read_fd)
        owner.close()


def test_exec_does_not_inherit_lock_descriptors(tmp_path):
    path = make_lock(tmp_path)
    owner = acquire(path)
    code = ("import os,sys; "
            "print(','.join(str(fd) for fd in map(int,sys.argv[1:]) "
            "if os.path.exists('/proc/self/fd/'+str(fd))))")
    result = subprocess.check_output([sys.executable, "-c", code, str(owner._fd)],
                                     text=True, close_fds=False, timeout=10)
    assert result.strip() == ""
    assert owner.observe() is OwnershipState.OBSERVED_LOCKED
    owner.close()


def test_fifo_is_rejected_without_blocking(tmp_path):
    path = tmp_path / "fifo"
    os.mkfifo(path, 0o600)
    code = ("from orchestrator.isolation._broker_ownership import BrokerOwnership; "
            "import sys; print(BrokerOwnership.acquire(sys.argv[1], store_id='s', namespace='n').observe().value)")
    env = {**os.environ, "PYTHONPATH": os.path.abspath("src")}
    assert subprocess.check_output([sys.executable, "-c", code, str(path)], text=True,
                                   env=env, timeout=3).strip() == "unknown"


def test_parent_replacement_invalidates_owner(tmp_path):
    directory = tmp_path / "owned"
    directory.mkdir(mode=0o700)
    path = make_lock(directory)
    owner = acquire(path)
    assert owner.observe() is OwnershipState.OBSERVED_LOCKED
    directory.rename(tmp_path / "old")
    directory.mkdir(mode=0o700)
    make_lock(directory)
    assert owner.observe() is OwnershipState.UNKNOWN
    owner.close()


def test_hardlink_created_after_acquisition_invalidates_owner(tmp_path):
    path = make_lock(tmp_path)
    owner = acquire(path)
    os.link(path, tmp_path / "extra")
    assert owner.observe() is OwnershipState.UNKNOWN
    owner.close()


def test_closed_descriptor_is_unknown(tmp_path):
    owner = acquire(make_lock(tmp_path))
    os.close(owner._fd)
    assert owner.observe() is OwnershipState.UNKNOWN
    owner.close()


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_fork_while_another_thread_holds_mutex_never_blocks_child(tmp_path):
    import select
    import signal
    import threading

    owner = acquire(make_lock(tmp_path))
    ready = threading.Event()
    release = threading.Event()
    def hold():
        with owner._mutex:
            ready.set()
            release.wait()
    thread = threading.Thread(target=hold)
    thread.start()
    assert ready.wait(timeout=3)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            result = owner.observe()
            owner.close()
            os.write(write_fd, result.value.encode())
        finally:
            os._exit(0)
    os.close(write_fd)
    try:
        readable, _, _ = select.select([read_fd], [], [], 3)
        assert readable, "forked child blocked on inherited mutex"
        assert os.read(read_fd, 32) == b"unknown"
    finally:
        release.set()
        thread.join(timeout=3)
        if not readable:
            os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        os.close(read_fd)
        owner.close()


def test_duplicated_lock_remains_exclusive_until_last_close(tmp_path):
    path = make_lock(tmp_path)
    owner = acquire(path)
    duplicate = os.dup(owner._fd)
    try:
        owner.close()
        assert acquire(path).observe() is OwnershipState.UNKNOWN
    finally:
        os.close(duplicate)
    new_owner = acquire(path)
    assert new_owner.observe() is OwnershipState.OBSERVED_LOCKED
    new_owner.close()


def test_reopen_error_followed_by_close_releases_lock(tmp_path, monkeypatch):
    import orchestrator.isolation._broker_ownership as module
    path = make_lock(tmp_path)
    owner = acquire(path)
    original = module._open_path
    def fail(_path):
        raise OSError("injected reopen error")
    monkeypatch.setattr(module, "_open_path", fail)
    assert owner.observe() is OwnershipState.UNKNOWN
    owner.close()
    monkeypatch.setattr(module, "_open_path", original)
    replacement = acquire(path)
    assert replacement.observe() is OwnershipState.OBSERVED_LOCKED
    replacement.close()


def test_initial_observation_failure_closes_acquired_fd(tmp_path, monkeypatch):
    import orchestrator.isolation._broker_ownership as module
    path = make_lock(tmp_path)
    original = module._open_path
    opened = []
    def fail_second(value):
        if opened:
            raise OSError("injected initial observation failure")
        fd, identities = original(value)
        opened.append(fd)
        return fd, identities
    monkeypatch.setattr(module, "_open_path", fail_second)
    owner = acquire(path)
    assert owner.observe() is OwnershipState.UNKNOWN
    with pytest.raises(OSError):
        os.fstat(opened[0])


def test_probe_close_failure_invalidates_observation(tmp_path, monkeypatch):
    import orchestrator.isolation._broker_ownership as module
    owner = acquire(make_lock(tmp_path))
    original_open = module._open_path
    original_close = os.close
    probes = []
    def track(path):
        fd, ids = original_open(path)
        probes.append(fd)
        return fd, ids
    def fail_probe(fd):
        if probes and fd == probes[-1]:
            raise OSError("injected close failure")
        original_close(fd)
    monkeypatch.setattr(module, "_open_path", track)
    monkeypatch.setattr(os, "close", fail_probe)
    try:
        assert owner.observe() is OwnershipState.UNKNOWN
        assert owner.observe() is OwnershipState.UNKNOWN
    finally:
        monkeypatch.setattr(os, "close", original_close)
        if probes:
            original_close(probes[-1])
        owner.close()


def test_flock_interrupt_closes_acquired_fd(tmp_path, monkeypatch):
    import orchestrator.isolation._broker_ownership as module
    path = make_lock(tmp_path)
    opened = []
    original_open = module._open_path
    def track(value):
        fd, ids = original_open(value)
        opened.append(fd)
        return fd, ids
    def interrupt(*args):
        raise KeyboardInterrupt("original interrupt")
    monkeypatch.setattr(module, "_open_path", track)
    monkeypatch.setattr(module.fcntl, "flock", interrupt)
    with pytest.raises(KeyboardInterrupt, match="original interrupt"):
        acquire(path)
    with pytest.raises(OSError):
        os.fstat(opened[0])


def test_directory_close_error_still_closes_new_child(tmp_path, monkeypatch):
    import orchestrator.isolation._broker_ownership as module
    path = make_lock(tmp_path)
    original_open = os.open
    original_close = os.close
    opened = []
    closed = []
    def track_open(*args, **kwargs):
        fd = original_open(*args, **kwargs)
        opened.append(fd)
        return fd
    def interrupt_after_close(fd):
        original_close(fd)
        closed.append(fd)
        if len(closed) == 1:
            raise KeyboardInterrupt("directory interrupt")
    monkeypatch.setattr(os, "open", track_open)
    monkeypatch.setattr(os, "close", interrupt_after_close)
    with pytest.raises(KeyboardInterrupt, match="directory interrupt"):
        module._open_path(str(path))
    assert set(opened) == set(closed)


def test_observation_interrupt_closes_probe_and_invalidates(tmp_path, monkeypatch):
    import orchestrator.isolation._broker_ownership as module
    owner = acquire(make_lock(tmp_path))
    original_open = module._open_path
    probes = []
    def track(path):
        fd, ids = original_open(path)
        probes.append(fd)
        return fd, ids
    def interrupt(*args):
        raise KeyboardInterrupt("observation interrupted")
    monkeypatch.setattr(module, "_open_path", track)
    monkeypatch.setattr(module.fcntl, "flock", interrupt)
    try:
        with pytest.raises(KeyboardInterrupt, match="observation interrupted"):
            owner.observe()
        with pytest.raises(OSError):
            os.fstat(probes[0])
        assert owner.observe() is OwnershipState.UNKNOWN
    finally:
        owner.close()


def test_original_interrupt_wins_over_cleanup_error(tmp_path, monkeypatch):
    import orchestrator.isolation._broker_ownership as module
    path = make_lock(tmp_path)
    original_open = module._open_path
    original_close = os.close
    opened = []
    def track(value):
        fd, ids = original_open(value)
        opened.append(fd)
        return fd, ids
    def interrupt(*args):
        raise KeyboardInterrupt("original interrupt")
    def close_then_error(fd):
        original_close(fd)
        if opened and fd == opened[0]:
            raise OSError("cleanup error")
    monkeypatch.setattr(module, "_open_path", track)
    monkeypatch.setattr(module.fcntl, "flock", interrupt)
    monkeypatch.setattr(os, "close", close_then_error)
    with pytest.raises(KeyboardInterrupt, match="original interrupt") as error:
        acquire(path)
    assert any("cleanup error" in note for note in error.value.__notes__)
    with pytest.raises(OSError):
        os.fstat(opened[0])


def test_async_interrupt_between_open_and_registration_can_leak(tmp_path, monkeypatch):
    """Characterize why callers must defer asynchronous exceptions."""
    import dis
    import orchestrator.isolation._broker_ownership as module

    path = make_lock(tmp_path)
    original_open = os.open
    opened = []
    def track_open(*args, **kwargs):
        fd = original_open(*args, **kwargs)
        opened.append(fd)
        return fd
    monkeypatch.setattr(os, "open", track_open)
    instructions = list(dis.get_instructions(module._open_path))
    store_index = next(i for i, inst in enumerate(instructions)
                       if inst.opname == "STORE_FAST" and inst.argval == "directory")
    interrupt_line = instructions[store_index + 1].starts_line
    previous = sys.gettrace()
    def trace(frame, event, arg):
        if frame.f_code is module._open_path.__code__:
            if event == "line" and frame.f_lineno == interrupt_line:
                raise KeyboardInterrupt("between open and registration")
        return trace
    try:
        sys.settrace(trace)
        with pytest.raises(KeyboardInterrupt, match="between open and registration"):
            module._open_path(str(path))
    finally:
        sys.settrace(previous)
    try:
        assert len(opened) == 1
        os.fstat(opened[0])  # The unregistered descriptor is still open.
    finally:
        for fd in opened:
            os.close(fd)


def test_async_interrupt_during_close_can_lose_fd_ownership(tmp_path):
    """Characterize why deferral is required for close as well as acquire."""
    import inspect
    from orchestrator.isolation._broker_ownership import BrokerOwnership

    owner = acquire(make_lock(tmp_path))
    descriptor = owner._fd
    lines, first_line = inspect.getsourcelines(BrokerOwnership.close)
    interrupt_line = first_line + next(i for i, line in enumerate(lines)
                                       if "self._valid = False" in line)
    previous = sys.gettrace()
    def trace(frame, event, arg):
        if (frame.f_code is BrokerOwnership.close.__code__ and event == "line"
                and frame.f_lineno == interrupt_line):
            raise KeyboardInterrupt("close transfer interrupted")
        return trace
    try:
        sys.settrace(trace)
        with pytest.raises(KeyboardInterrupt, match="close transfer interrupted"):
            owner.close()
    finally:
        sys.settrace(previous)
    try:
        assert owner._fd == -1
        os.fstat(descriptor)
    finally:
        os.close(descriptor)
        owner.close()
