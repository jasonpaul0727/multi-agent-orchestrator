"""Real finite disk state must never infer cleanup from a free lock."""

from dataclasses import replace
import importlib
import importlib.util
import os
from pathlib import Path
import threading

import pytest


def arena_module():
    name = 'orchestrator.isolation._staging_arena'
    assert importlib.util.find_spec(name) is not None, 'durable two-slot arena missing'
    return importlib.import_module(name)


@pytest.fixture
def owned(tmp_path, request):
    def create():
        a = arena_module()
        roots = a.create_fixture_roots(tmp_path)
        request.addfinalizer(roots.close)
        return a, roots, tmp_path / 'staging-fixture'
    return create


def test_two_fixed_slots_refuse_third_and_reopen_keeps_unknown(owned):
    a, roots, root = owned()
    from orchestrator.isolation._staging_contract import ReasonCode
    first = a.SlotArena.open_existing(roots.arena_fd)
    second = a.SlotArena.open_existing(roots.arena_fd)
    try:
        one = first.reserve()
        two = second.reserve()
        assert (one.slot_index, two.slot_index) == (0, 1)
        assert one.token != two.token
        assert first.reserve() is None
        first.mark_unknown(one, ReasonCode.LOCAL_CLEANUP_UNKNOWN)
    finally:
        first.close()
        second.close()
    # Age, process-owner disappearance and an unlocked file confer no release.
    for entry in (root / 'arena').iterdir():
        os.utime(entry, (1, 1), follow_symlinks=False)
    reopened = a.SlotArena.open_existing(roots.arena_fd)
    try:
        assert reopened.reserve() is None
        before = (root / 'arena' / 'slot-0.record').read_bytes()
        reopened.mark_unknown(one, ReasonCode.INTERRUPTED)
        assert (root / 'arena' / 'slot-0.record').read_bytes() == before
    finally:
        reopened.close()


def test_reservation_is_bound_to_exact_generation_token_and_builtin_slot(owned):
    a, roots, root = owned()
    from orchestrator.isolation._staging_contract import ContractError, ReasonCode
    arena = a.SlotArena.open_existing(roots.arena_fd)
    try:
        reservation = arena.reserve()
        for wrong in (replace(reservation, token='0' * 32),
                      replace(reservation, arena_generation='0' * 32)):
            with pytest.raises(a.ArenaUnavailable):
                arena.mark_unknown(wrong, ReasonCode.LOCAL_CLEANUP_UNKNOWN)
        with pytest.raises(ContractError):
            replace(reservation, slot_index=True)
    finally:
        arena.close()


@pytest.mark.parametrize('mutation', ['truncate', 'extra', 'replace', 'symlink', 'missing', 'bad_json'])
def test_damaged_or_replaced_record_refuses_without_repair(owned, mutation):
    a, roots, root = owned()
    record = root / 'arena' / 'slot-0.record'
    if mutation == 'truncate':
        record.write_bytes(b'{}')
    elif mutation == 'extra':
        (record.parent / 'slot-2.record').write_bytes(b'not an extra slot')
    elif mutation == 'replace':
        replacement = record.parent / 'replacement'
        replacement.write_bytes(record.read_bytes())
        replacement.chmod(0o600)
        os.replace(replacement, record)
    elif mutation == 'symlink':
        record.unlink()
        record.symlink_to('/dev/null')
    elif mutation == 'missing':
        record.unlink()
    else:
        original = record.read_bytes()
        record.write_bytes(b'{"schema":NaN}' + b' ' * (len(original) - 14))
    before = {item.name for item in record.parent.iterdir()}
    with pytest.raises(a.ArenaUnavailable):
        a.SlotArena.open_existing(roots.arena_fd)
    assert {item.name for item in record.parent.iterdir()} == before


def test_live_handle_refuses_substituted_anchor_and_never_touches_replacement(owned):
    a, roots, root = owned()
    arena = a.SlotArena.open_existing(roots.arena_fd)
    original = root / 'arena'
    original.rename(root / 'original-arena')
    original.mkdir(mode=0o700)
    marker = original / 'unrelated'
    marker.write_bytes(b'keep')
    try:
        with pytest.raises(a.ArenaUnavailable):
            arena.reserve()
        assert marker.read_bytes() == b'keep'
    finally:
        arena.close()


@pytest.mark.parametrize('kind', ['existing', 'symlink_parent', 'unsafe_parent'])
def test_fixture_creation_never_reinitializes_or_uses_unsafe_parent(tmp_path, kind):
    a = arena_module()
    if kind == 'existing':
        roots = a.create_fixture_roots(tmp_path)
        roots.close()
        target = tmp_path
    elif kind == 'symlink_parent':
        actual = tmp_path / 'actual'
        actual.mkdir(mode=0o700)
        target = tmp_path / 'alias'
        target.symlink_to(actual, target_is_directory=True)
    else:
        tmp_path.chmod(0o777)
        target = tmp_path
    with pytest.raises(a.ArenaUnavailable):
        a.create_fixture_roots(target)


def test_fsync_failure_poison_is_visible_on_reopen_and_preserves_control_flow(owned, monkeypatch):
    a, roots, root = owned()
    arena = a.SlotArena.open_existing(roots.arena_fd)
    real_fsync = os.fsync
    failure = KeyboardInterrupt('selected control flow')
    monkeypatch.setattr(a.os, 'fsync', lambda fd: (_ for _ in ()).throw(failure))
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            arena.reserve()
        assert caught.value is failure
        monkeypatch.setattr(a.os, 'fsync', real_fsync)
        with pytest.raises(a.ArenaUnavailable):
            arena.reserve()
        with pytest.raises(a.ArenaUnavailable):
            a.SlotArena.open_existing(roots.arena_fd)
    finally:
        monkeypatch.setattr(a.os, 'fsync', real_fsync)
        arena.close()


def test_disk_error_during_unknown_does_not_free_occupied_slot(owned, monkeypatch):
    a, roots, root = owned()
    from orchestrator.isolation._staging_contract import ReasonCode
    arena = a.SlotArena.open_existing(roots.arena_fd)
    reservation = arena.reserve()
    real_pwrite = os.pwrite
    monkeypatch.setattr(a.os, 'pwrite', lambda *args: (_ for _ in ()).throw(OSError('private path')))
    try:
        with pytest.raises(a.ArenaUnavailable) as caught:
            arena.mark_unknown(reservation, ReasonCode.LOCAL_CLEANUP_UNKNOWN)
        assert 'private path' not in str(caught.value)
    finally:
        monkeypatch.setattr(a.os, 'pwrite', real_pwrite)
        arena.close()
    reopened = a.SlotArena.open_existing(roots.arena_fd)
    try:
        assert reopened.reserve().slot_index == 1
        assert reopened.reserve() is None
    finally:
        reopened.close()


def test_reopen_closes_every_acquired_fd_when_constructor_fails(owned, monkeypatch):
    a, roots, root = owned()
    before = set(os.listdir('/proc/self/fd'))
    real_open = os.open
    def fail_record(path, *args, **kwargs):
        if path == 'slot-1.record':
            raise OSError('private path')
        return real_open(path, *args, **kwargs)
    monkeypatch.setattr(a.os, 'open', fail_record)
    with pytest.raises(a.ArenaUnavailable):
        a.SlotArena.open_existing(roots.arena_fd)
    assert set(os.listdir('/proc/self/fd')) == before


def test_independent_open_lock_contention_refuses_without_allocating(owned):
    a, roots, root = owned()
    arena = a.SlotArena.open_existing(roots.arena_fd)
    lock_fd = os.open(root / 'arena' / 'arena.header', os.O_RDWR | os.O_NOFOLLOW)
    a.fcntl.flock(lock_fd, a.fcntl.LOCK_EX)
    # This safety release makes a mistaken blocking implementation fail rather
    # than hang pytest. It creates no helper graph or data allocation.
    release = threading.Timer(0.2, a.fcntl.flock, args=(lock_fd, a.fcntl.LOCK_UN))
    release.start()
    try:
        with pytest.raises(a.ArenaUnavailable):
            arena.reserve()
        release.join(timeout=1)
        assert not release.is_alive()
        assert arena.reserve().slot_index == 0
    finally:
        release.join(timeout=1)
        os.close(lock_fd)
        arena.close()


def test_secondary_unlock_control_flow_cannot_replace_selected_primary(owned, monkeypatch):
    a, roots, root = owned()
    arena = a.SlotArena.open_existing(roots.arena_fd)
    primary = KeyboardInterrupt('selected')
    secondary = SystemExit('secondary')
    real_flock = a.fcntl.flock
    def fail_unlock(fd, operation):
        if operation == a.fcntl.LOCK_UN:
            raise secondary
        return real_flock(fd, operation)
    monkeypatch.setattr(a.os, 'fsync', lambda fd: (_ for _ in ()).throw(primary))
    monkeypatch.setattr(a.fcntl, 'flock', fail_unlock)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            arena.reserve()
        assert caught.value is primary
    finally:
        monkeypatch.setattr(a.fcntl, 'flock', real_flock)
        arena.close()


@pytest.mark.parametrize('target', ['root', 'source', 'slot'])
def test_unsafe_owned_directory_refuses_without_reclaim(owned, target):
    a, roots, root = owned()
    paths = {'root': root, 'source': root / 'source', 'slot': root / 'arena' / 'slot-0'}
    paths[target].chmod(0o777)
    with pytest.raises(a.ArenaUnavailable):
        a.SlotArena.open_existing(roots.arena_fd)


@pytest.mark.parametrize('change', ['extra', 'duplicate', 'nonfinite'])
def test_fixed_record_codec_refuses_extra_duplicate_and_nonfinite_fields(owned, change):
    a, roots, root = owned()
    path = root / 'arena' / 'slot-0.record'
    raw = path.read_bytes()
    if change == 'extra':
        mutated = raw.rstrip()[:-1] + b',"unexpected":true}'
    elif change == 'duplicate':
        mutated = raw.rstrip()[:-1] + b',"slot_index":0}'
    else:
        mutated = raw.rstrip().replace(b'"slot_index":0', b'"slot_index":NaN')
    path.write_bytes(mutated + b' ' * (len(raw) - len(mutated)))
    with pytest.raises(a.ArenaUnavailable):
        a.SlotArena.open_existing(roots.arena_fd)


@pytest.mark.parametrize('primary_kind', [KeyboardInterrupt, RuntimeError])
def test_binding_close_failures_preserve_body_exception_and_attempt_every_fd_once(owned, monkeypatch, primary_kind):
    a, roots, root = owned()
    arena = a.SlotArena.open_existing(roots.arena_fd)
    primary = primary_kind('selected body exception')
    secondary = SystemExit('secondary close exception')
    closed = []
    real_close = os.close
    def close_then_fail(fd):
        closed.append(fd)
        real_close(fd)
        raise secondary
    try:
        with monkeypatch.context() as patch:
            patch.setattr(a, '_fd_directory', lambda fd: (_ for _ in ()).throw(primary))
            patch.setattr(a.os, 'close', close_then_fail)
            with pytest.raises(primary_kind) as caught:
                a._binding((arena._arena_fd, arena._header))
            assert caught.value is primary
        assert len(closed) == 2 and len(set(closed)) == 2
        assert primary.__notes__ == ['owned_fd_close_unconfirmed']
    finally:
        arena.close()


@pytest.mark.parametrize('target', ['record', 'header'])
@pytest.mark.parametrize('reader', ['live', 'reopen'])
def test_single_metadata_file_rollback_refuses_without_repair(owned, target, reader):
    a, roots, root = owned()
    from orchestrator.isolation._staging_contract import ReasonCode
    arena = a.SlotArena.open_existing(roots.arena_fd)
    path = root / 'arena' / ('slot-0.record' if target == 'record' else 'arena.header')
    saved = path.read_bytes()
    first = arena.reserve()
    arena.mark_unknown(first, ReasonCode.LOCAL_CLEANUP_UNKNOWN)
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        assert os.pwrite(fd, saved, 0) == len(saved)
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        with pytest.raises(a.ArenaUnavailable):
            if reader == 'live':
                arena.reserve()
            else:
                arena.close()
                opened = a.SlotArena.open_existing(roots.arena_fd)
                try:
                    opened.reserve()
                finally:
                    opened.close()
        assert path.read_bytes() == saved
    finally:
        arena.close()


@pytest.mark.parametrize('failing_sync', [1, 2, 3])
def test_each_transition_sync_failure_keeps_target_slot_unavailable(owned, monkeypatch, failing_sync):
    a, roots, root = owned()
    arena = a.SlotArena.open_existing(roots.arena_fd)
    real_fsync = os.fsync
    calls = 0
    def fail_selected_sync(fd):
        nonlocal calls
        calls += 1
        if calls == failing_sync:
            raise OSError('private sync failure')
        real_fsync(fd)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(a.os, 'fsync', fail_selected_sync)
            with pytest.raises(a.ArenaUnavailable):
                arena.reserve()
        with pytest.raises(a.ArenaUnavailable):
            arena.reserve()
    finally:
        arena.close()
    try:
        reopened = a.SlotArena.open_existing(roots.arena_fd)
    except a.ArenaUnavailable:
        return
    try:
        # A completed byte write followed by a failed sync can already be
        # visible; even then the uncertain target may never appear FREE.
        remaining = reopened.reserve()
        assert remaining is None or remaining.slot_index == 1
        assert reopened.reserve() is None
    finally:
        reopened.close()


@pytest.mark.parametrize('slot', [0, 1])
@pytest.mark.parametrize('reader', ['live', 'reopen'])
def test_nonempty_host_anchor_refuses_and_preserves_owned_evidence(owned, slot, reader):
    a, roots, root = owned()
    arena = a.SlotArena.open_existing(roots.arena_fd)
    marker = root / 'arena' / f'slot-{slot}' / 'unexpected'
    marker.write_bytes(b'keep owned evidence')
    try:
        with pytest.raises(a.ArenaUnavailable):
            if reader == 'live':
                arena.reserve()
            else:
                arena.close()
                opened = a.SlotArena.open_existing(roots.arena_fd)
                opened.close()
        assert marker.read_bytes() == b'keep owned evidence'
    finally:
        arena.close()
