import os
from pathlib import Path

import pytest


@pytest.fixture
def group(tmp_path, monkeypatch):
    from orchestrator.isolation import _cgroup_v2_observer as mod
    root = tmp_path / "root"
    target = root / "parent"
    target.mkdir(parents=True)
    (target / "cgroup.events").write_text("populated 1\nfrozen 0\n")
    monkeypatch.setattr(mod, "_ROOT", root)
    monkeypatch.setattr(mod, "_cgroup2_mount", lambda mount: True)
    monkeypatch.setattr(mod, "_boot_id", lambda: "boot-a")
    return mod, root, target


def test_pinned_observation_and_invalid_content(group):
    mod, root, target = group
    with mod.CgroupObserver.open("parent") as observer:
        assert observer.observe() == 1
        (target / "cgroup.events").write_text("populated 0\nfrozen 0\n")
        assert observer.observe() == 0
        for contents in ("", "populated 2\n", "populated 0\npopulated 1\n", "populated 0\n" + "x" * 4097):
            (target / "cgroup.events").write_text(contents)
            assert observer.observe() is None
    assert observer.observe() is None


def test_replacement_missing_and_boot_change_are_unknown(group, monkeypatch):
    mod, root, target = group
    with mod.CgroupObserver.open("parent") as observer:
        monkeypatch.setattr(mod, "_boot_id", lambda: "boot-b")
        assert observer.observe() is None
        monkeypatch.setattr(mod, "_boot_id", lambda: "boot-a")
        (target / "cgroup.events").unlink()
        assert observer.observe() is None
        (target / "cgroup.events").write_text("populated 0\n")
        assert observer.observe() is None


@pytest.mark.parametrize("path", ["", "/parent", "../parent", "parent/../other", "parent//x", "./parent"])
def test_untrusted_paths_are_rejected(group, path):
    mod, _, _ = group
    assert mod.CgroupObserver.open(path) is None


def test_symlink_and_mount_failure_are_unknown(group, monkeypatch):
    mod, root, target = group
    (root / "link").symlink_to(target, target_is_directory=True)
    assert mod.CgroupObserver.open("link") is None
    monkeypatch.setattr(mod, "_cgroup2_mount", lambda mount: False)
    assert mod.CgroupObserver.open("parent") is None


def test_directory_or_events_replaced_during_read_is_unknown(group, monkeypatch):
    mod, root, target = group
    observer = mod.CgroupObserver.open("parent")
    assert observer is not None
    read = mod._read_population
    def replace_during_read(fd):
        value = read(fd)
        target.rename(root / "old")
        target.mkdir()
        (target / "cgroup.events").write_text("populated 0\n")
        return value
    monkeypatch.setattr(mod, "_read_population", replace_during_read)
    try:
        assert observer.observe() is None
    finally:
        observer.close()


def test_symlink_events_and_closed_fd_lifecycle(group):
    mod, root, target = group
    events = target / "cgroup.events"
    events.unlink()
    events.symlink_to(root / "other")
    assert mod.CgroupObserver.open("parent") is None
    events.unlink()
    events.write_text("populated 1\n")
    before = len(os.listdir("/proc/self/fd"))
    for _ in range(5):
        observer = mod.CgroupObserver.open("parent")
        assert observer is not None
        observer.close()
        observer.close()
    assert len(os.listdir("/proc/self/fd")) == before


def test_mount_change_or_events_file_swap_during_read_is_unknown(group, monkeypatch):
    mod, root, target = group
    observer = mod.CgroupObserver.open("parent")
    assert observer is not None
    original = mod._read_population
    def swapped(fd):
        value = original(fd)
        (target / "cgroup.events").rename(target / "old-events")
        (target / "cgroup.events").write_text("populated 0\n")
        return value
    monkeypatch.setattr(mod, "_read_population", swapped)
    try:
        assert observer.observe() is None
    finally:
        observer.close()
    monkeypatch.setattr(mod, "_cgroup2_mount", lambda mount: False)
    assert mod.CgroupObserver.open("parent") is None


def test_invalid_frozen_value_and_unknown_field_are_rejected(group):
    mod, _, target = group
    observer = mod.CgroupObserver.open("parent")
    assert observer is not None
    try:
        for text in ("populated 0\nfrozen 2\n", "populated 0\nunexpected 1\n"):
            (target / "cgroup.events").write_text(text)
            assert observer.observe() is None
    finally:
        observer.close()


def test_boot_change_during_read_is_unknown(group, monkeypatch):
    mod, _, _ = group
    observer = mod.CgroupObserver.open("parent")
    assert observer is not None
    read = mod._read_population
    def changed(fd):
        result = read(fd)
        monkeypatch.setattr(mod, "_boot_id", lambda: "other-boot")
        return result
    monkeypatch.setattr(mod, "_read_population", changed)
    try:
        assert observer.observe() is None
    finally:
        observer.close()


def test_concurrent_directory_removal_during_sample_is_unknown(group, monkeypatch):
    import threading
    mod, _, target = group
    observer = mod.CgroupObserver.open("parent")
    assert observer is not None
    entered, resumed = threading.Event(), threading.Event()
    read = mod._read_population
    def blocked(fd):
        value = read(fd)
        entered.set()
        assert resumed.wait(5)
        return value
    monkeypatch.setattr(mod, "_read_population", blocked)
    result = []
    thread = threading.Thread(target=lambda: result.append(observer.observe()))
    thread.start()
    try:
        assert entered.wait(5)
        (target / "cgroup.events").unlink()
        target.rmdir()
    finally:
        resumed.set()
        thread.join(5)
        observer.close()
    assert not thread.is_alive() and result == [None]


@pytest.mark.parametrize("which", ["events", "directory"])
def test_close_failure_does_not_leak_or_close_reused_fd(group, monkeypatch, which):
    mod, _, _ = group
    observer = mod.CgroupObserver.open("parent")
    assert observer is not None
    event_fd, dir_fd = observer._events, observer._directory
    failed = event_fd if which == "events" else dir_fd
    real_close = os.close
    calls = []
    def faulty(fd):
        calls.append(fd)
        real_close(fd)
        if fd == failed:
            raise OSError("injected close failure after close")
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(OSError):
            observer.close()
    assert event_fd in calls and dir_fd in calls
    reused = os.open(os.devnull, os.O_RDONLY)
    os.dup2(reused, failed)
    try:
        observer.close()
        os.fstat(failed)
    finally:
        real_close(failed)
        if reused != failed: real_close(reused)


def test_walk_close_failure_closes_new_descriptor(group, monkeypatch):
    mod, root, _ = group
    real_open, real_close = os.open, os.close
    opened = []
    def track(*args, **kwargs):
        fd = real_open(*args, **kwargs)
        if args[0] == root or args[0] == "parent": opened.append(fd)
        return fd
    def faulty(fd):
        if opened and fd == opened[0]:
            real_close(fd)
            raise OSError("old close failed")
        return real_close(fd)
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "open", track)
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(OSError): mod._walk(["parent"])
    try:
        assert len(opened) == 2
        with pytest.raises(OSError): os.fstat(opened[1])
    finally:
        for fd in opened:
            try: real_close(fd)
            except OSError: pass


def test_open_failure_closes_both_descriptors_even_if_close_raises(group, monkeypatch):
    mod, _, _ = group
    real_close = os.close
    owned = []
    closed = []
    def inconsistent(self):
        owned.extend((self._events, self._directory))
        return False
    def faulty(fd):
        if fd in owned:
            closed.append(fd)
            real_close(fd)
            if fd == owned[0]: raise OSError("close failed")
        else:
            real_close(fd)
    monkeypatch.setattr(mod.CgroupObserver, "_consistent", inconsistent)
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        assert mod.CgroupObserver.open("parent") is None
    assert closed == owned


def test_observe_and_close_are_serialized(group, monkeypatch):
    import threading
    mod, _, _ = group
    observer = mod.CgroupObserver.open("parent")
    assert observer is not None
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    read = mod._read_population
    def blocked(fd):
        entered.set()
        assert release.wait(5)
        return read(fd)
    monkeypatch.setattr(mod, "_read_population", blocked)
    result = []
    sampling = threading.Thread(target=lambda: result.append(observer.observe()))
    closing = threading.Thread(target=lambda: (observer.close(), closed.set()))
    sampling.start()
    try:
        assert entered.wait(5)
        closing.start()
        assert not closed.wait(0.05)
    finally:
        release.set()
        sampling.join(5)
        closing.join(5)
    assert result == [1] and closed.is_set() and observer.observe() is None


def test_interrupted_open_closes_pinned_fds_and_propagates(group, monkeypatch):
    mod, _, _ = group
    owned = []
    def interrupted(self):
        owned.extend((self._events, self._directory))
        raise KeyboardInterrupt()
    monkeypatch.setattr(mod.CgroupObserver, "_consistent", interrupted)
    with pytest.raises(KeyboardInterrupt):
        mod.CgroupObserver.open("parent")
    try:
        assert len(owned) == 2
        for fd in owned:
            with pytest.raises(OSError): os.fstat(fd)
    finally:
        for fd in owned:
            try: os.close(fd)
            except OSError: pass


def test_keyboard_interrupt_on_first_close_closes_second_and_never_retries(group, monkeypatch):
    mod, _, _ = group
    observer = mod.CgroupObserver.open("parent")
    events_fd, dir_fd = observer._events, observer._directory
    real_close = os.close
    seen = []
    interrupt = KeyboardInterrupt("close interruption")
    def faulty(fd):
        seen.append(fd)
        real_close(fd)
        if fd == events_fd: raise interrupt
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(KeyboardInterrupt) as raised:
            observer.close()
    assert raised.value is interrupt and seen == [events_fd, dir_fd]
    reused = os.open(os.devnull, os.O_RDONLY)
    os.dup2(reused, events_fd)
    try:
        observer.close()
        os.fstat(events_fd)
    finally:
        real_close(events_fd)
        if reused != events_fd: real_close(reused)


def test_open_preserves_original_error_when_cleanup_interrupted(group, monkeypatch):
    mod, _, _ = group
    owned = []
    real_close = os.close
    original = KeyboardInterrupt("original")
    def inconsistent(self):
        owned.extend((self._events, self._directory))
        raise original
    def faulty(fd):
        real_close(fd)
        if owned and fd == owned[0]: raise KeyboardInterrupt("cleanup")
    monkeypatch.setattr(mod.CgroupObserver, "_consistent", inconsistent)
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(KeyboardInterrupt) as raised:
            mod.CgroupObserver.open("parent")
    try:
        assert raised.value is original
        assert len(owned) == 2
        for fd in owned:
            with pytest.raises(OSError): os.fstat(fd)
    finally:
        for fd in owned:
            try: real_close(fd)
            except OSError: pass


def test_later_interrupt_takes_priority_after_earlier_close_error(group, monkeypatch):
    mod, _, _ = group
    first, second = os.open(os.devnull, os.O_RDONLY), os.open(os.devnull, os.O_RDONLY)
    real_close = os.close
    interrupt = KeyboardInterrupt("second")
    def faulty(fd):
        real_close(fd)
        if fd == first: raise OSError("first")
        if fd == second: raise interrupt
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(KeyboardInterrupt) as raised:
            mod._close_owned(first, second)
    assert raised.value is interrupt
    assert any("first" in note for note in interrupt.__notes__)
    for fd in (first, second):
        with pytest.raises(OSError): os.fstat(fd)


def test_cleanup_interrupt_is_not_swallowed_by_ordinary_open_failure(group, monkeypatch):
    mod, _, _ = group
    owned = []
    real_close = os.close
    interrupt = KeyboardInterrupt("cleanup interrupt")
    def inconsistent(self):
        owned.extend((self._events, self._directory))
        return False
    def faulty(fd):
        real_close(fd)
        if owned and fd == owned[0]: raise interrupt
    monkeypatch.setattr(mod.CgroupObserver, "_consistent", inconsistent)
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(KeyboardInterrupt) as raised:
            mod.CgroupObserver.open("parent")
    assert raised.value is interrupt
    for fd in owned:
        with pytest.raises(OSError): os.fstat(fd)


@pytest.mark.parametrize("original_is_interrupt", [True, False])
def test_context_preserves_original_interrupt_or_prioritizes_cleanup_interrupt(group, monkeypatch, original_is_interrupt):
    mod, _, _ = group
    observer = mod.CgroupObserver.open("parent")
    events = observer._events
    real_close = os.close
    original = KeyboardInterrupt("body interrupt") if original_is_interrupt else ValueError("body error")
    cleanup = KeyboardInterrupt("cleanup interrupt")
    def faulty(fd):
        real_close(fd)
        if fd == events: raise cleanup
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(KeyboardInterrupt) as caught:
            with observer:
                raise original
    expected = original if original_is_interrupt else cleanup
    assert caught.value is expected
    assert any(("cleanup interrupt" if original_is_interrupt else "body error") in note
               for note in expected.__notes__)


def test_context_without_body_error_propagates_close_error(group, monkeypatch):
    mod, _, _ = group
    observer = mod.CgroupObserver.open("parent")
    events = observer._events
    real_close = os.close
    error = OSError("close error")
    def faulty(fd):
        real_close(fd)
        if fd == events: raise error
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(OSError) as caught:
            with observer:
                pass
    assert caught.value is error


def test_context_preserves_body_error_when_close_has_ordinary_error(group, monkeypatch):
    mod, _, _ = group
    observer = mod.CgroupObserver.open("parent")
    events = observer._events
    real_close = os.close
    original = ValueError("body error")
    def faulty(fd):
        real_close(fd)
        if fd == events: raise OSError("close error")
    with monkeypatch.context() as patch:
        patch.setattr(mod.os, "close", faulty)
        with pytest.raises(ValueError) as caught:
            with observer:
                raise original
    assert caught.value is original
    assert any("close error" in note for note in original.__notes__)
