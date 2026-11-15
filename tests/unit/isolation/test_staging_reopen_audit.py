"""Existing-root and durable audit checks use new temporary A1 fixtures only."""
from dataclasses import replace
import hashlib
import importlib
import os
import pytest
from test_staging_host_root import deny_process_controls


@pytest.fixture
def fixture(tmp_path):
    a = importlib.import_module('orchestrator.isolation._staging_arena')
    c = importlib.import_module('orchestrator.isolation._staging_contract')
    roots = a.create_fixture_roots(tmp_path)
    arena = a.SlotArena.open_existing(roots.arena_fd)
    reservation = arena.reserve()
    arena.mark_unknown(reservation, c.ReasonCode.CONTROL_LOST)
    arena.close()
    roots.close()
    yield tmp_path


def modules():
    return (importlib.import_module('orchestrator.isolation._staging_reopen'),
            importlib.import_module('orchestrator.isolation._staging_audit'),
            importlib.import_module('orchestrator.isolation._staging_ownership'))


def test_existing_root_has_eight_independently_opened_admin_fds_and_raw_baseline(fixture):
    r, audit, o = modules()
    from orchestrator.isolation._staging_arena import SlotArena
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(SlotArena.open_existing(roots.arena_fd))
        assert len(owner.open_fds) == 8 and len(set(owner.open_fds)) == 8
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
            assert baseline.raw_slot0 == (fixture / 'staging-fixture/arena/slot-0.record').read_bytes()
            assert baseline.header0_digest == hashlib.sha256(baseline.raw_slot0).hexdigest()
            assert baseline.header0_revision == 2
            assert len(baseline.identities) == 9
            assert len(baseline.to_bytes()) == 1024
            assert audit.Slot0Baseline.from_bytes(baseline.to_bytes()) == baseline
            audit.assert_slot0_unchanged_locked(arena, baseline, expected_transition=None)
            with pytest.raises(audit.ContractError):
                audit.Slot0Baseline.from_bytes(baseline.to_bytes()[:-1] + b'\1')
            with pytest.raises(audit.ContractError):
                replace(baseline, header0_revision=1)
            with pytest.raises(audit.ContractError):
                audit.assert_slot0_unchanged_locked(arena, baseline, expected_transition=True)
        finally:
            arena._leave()
    finally:
        owner.close_all()


@pytest.mark.parametrize('damage', ['missing', 'extra', 'symlink', 'source_replace', 'header_replace', 'slot0_change'])
def test_reopen_refuses_damage_without_create_or_repair(fixture, damage):
    r, audit, o = modules()
    root = fixture / 'staging-fixture'
    if damage == 'missing':
        (root / 'source').rmdir()
    elif damage == 'extra':
        (root / 'unexpected').write_bytes(b'keep')
    elif damage == 'symlink':
        (root / 'source').rmdir()
        (root / 'source').symlink_to('/tmp', target_is_directory=True)
    elif damage == 'source_replace':
        (root / 'source').rename(fixture / 'old-source')
        (root / 'source').mkdir(mode=0o700)
    elif damage == 'header_replace':
        original = root / 'arena/arena.header'
        data = original.read_bytes()
        original.rename(root / 'source/old-header')
        original.write_bytes(data)
        original.chmod(0o600)
    else:
        record = root / 'arena/slot-0.record'
        record.write_bytes(record.read_bytes().replace(b'UNKNOWN', b'RESERVE', 1))
    before = sorted(str(p.relative_to(fixture)) for p in fixture.rglob('*'))
    owner = o.AcquisitionLedger()
    try:
        with pytest.raises((audit.ContractError, OSError, r.ArenaUnavailable)):
            roots = r.open_fixture_roots_existing(fixture, owner=owner)
            from orchestrator.isolation._staging_arena import SlotArena
            try:
                owner.adopt_returned_arena(SlotArena.open_existing(roots.arena_fd))
            except BaseException:
                owner.poison_debt(audit.NativePhase.REOPEN_ROOTS)
                raise
        assert sorted(str(p.relative_to(fixture)) for p in fixture.rglob('*')) == before
    finally:
        owner.close_all()


def test_slot0_raw_change_after_capture_refuses(fixture):
    r, audit, o = modules()
    from orchestrator.isolation._staging_arena import SlotArena
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(SlotArena.open_existing(roots.arena_fd))
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
            os.pwrite(arena._record_fds[0], b'X', 0)
            with pytest.raises(audit.ContractError):
                audit.assert_slot0_unchanged_locked(arena, baseline, expected_transition=None)
        finally:
            arena._leave()
    finally:
        owner.close_all()


def test_ext4_and_mountid_must_both_agree(fixture, monkeypatch):
    r, audit, o = modules()
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        # Real ordinary metadata operations only: no helper or native controls.
        audit.verify_ext4_metadata(owner.parent, owner=owner)
        assert len(owner.open_fds) == 4
        monkeypatch.setattr(audit, '_fstatfs_magic', lambda fd: 0x01021997)
        with pytest.raises(audit.ContractError):
            audit.verify_ext4_metadata(owner.parent, owner=owner)
        assert not audit._mount_matches(b'42 1 0:1 / / rw - 9p drvfs rw\n', 42, os.makedev(0, 1))
        assert not audit._mount_matches(b'43 1 0:1 / / rw - ext4 /dev/x rw\n', 42, os.makedev(0, 1))
        assert audit._mount_matches(b'42 1 0:1 / / rw - ext4 /dev/x rw\n', 42, os.makedev(0, 1))
    finally:
        owner.close_all()


def test_baseline_commit_single_write_file_then_parent_fsync_readback_no_replace(fixture, monkeypatch):
    r, audit, o = modules()
    from orchestrator.isolation._staging_arena import SlotArena
    from orchestrator.isolation._staging_contract import OutputBudget
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(SlotArena.open_existing(roots.arena_fd))
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
        finally:
            arena._leave()
        events = []
        real_write, real_fsync = os.write, os.fsync
        def write(fd, data):
            events.append(('write', len(data)))
            return real_write(fd, data)
        def fsync(fd):
            events.append(('fsync', 'parent' if fd == roots._parent_fd else 'file'))
            return real_fsync(fd)
        monkeypatch.setattr(audit.os, 'write', write)
        monkeypatch.setattr(audit.os, 'fsync', fsync)
        budget = OutputBudget()
        audit.commit_baseline(owner.parent, baseline, owner=owner, budget=budget)
        assert (fixture / 'staging-a2-baseline-v2.bin').read_bytes() == baseline.to_bytes()
        assert events[-3:] == [('write', 1024), ('fsync', 'file'), ('fsync', 'parent')]
        assert budget.used == 1024 and len(owner.open_fds) == 8
        with pytest.raises(FileExistsError):
            audit.commit_baseline(owner.parent, baseline, owner=owner, budget=budget)
        assert owner.has_debt
    finally:
        owner.close_all()


def test_history_strict_origin_facts_padding_and_single_commit(fixture):
    r, audit, o = modules()
    from orchestrator.isolation._staging_a2_contract import AttemptBinding, ExitKind, Role
    from orchestrator.isolation._staging_contract import OutputBudget
    history = audit.RetentionHistory(AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32), '55' * 32,
        (audit.ProcessFact(Role.R, Role.HOST, 101, 100, ExitKind.EXITED, 0, True),
         audit.ProcessFact(Role.S, Role.R, 102, 101, ExitKind.EXITED, 0, True),
         audit.ProcessFact(Role.T, Role.S, 103, 102, ExitKind.EXITED, 0, True)), 0xff, 14)
    raw = history.to_bytes()
    assert len(raw) == 512 and raw[:5] == b'SRH2\2'
    assert audit.RetentionHistory.from_bytes(raw) == history
    with pytest.raises(audit.ContractError):
        audit.RetentionHistory.from_bytes(raw[:-1] + b'\1')
    with pytest.raises(audit.ContractError):
        replace(history, facts=history.facts[:2])
    with pytest.raises(audit.ContractError):
        replace(history, facts=history.facts[:2] + (replace(history.facts[2], reaped=False),))
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        budget = OutputBudget()
        audit.commit_history(owner.parent, history, owner=owner, budget=budget)
        assert budget.used == 512
        assert (fixture / 'staging-a2-retention-v2.bin').read_bytes() == raw
        assert len(owner.open_fds) == 4
    finally:
        owner.close_all()


@pytest.mark.parametrize('fault', ['short_write', 'file_fsync', 'parent_fsync', 'readback', 'close'])
def test_commit_uncertainty_poison_never_unlinks_or_retries(fixture, monkeypatch, fault):
    r, audit, o = modules()
    from orchestrator.isolation._staging_arena import SlotArena
    from orchestrator.isolation._staging_contract import OutputBudget
    owner = o.AcquisitionLedger()
    failed_fd = []
    real_close = os.close
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(SlotArena.open_existing(roots.arena_fd))
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
        finally:
            arena._leave()
        real_write, real_fsync, real_read = os.write, os.fsync, os.read
        counts = {'write': 0, 'parent': 0}
        def write(fd, data):
            counts['write'] += 1
            if fault == 'short_write':
                return real_write(fd, data[:-1])
            return real_write(fd, data)
        def fsync(fd):
            if fd == roots._parent_fd:
                counts['parent'] += 1
                if fault == 'parent_fsync' and counts['parent'] == 2:
                    raise OSError(5, 'unit directory fsync')
            elif fault == 'file_fsync':
                raise OSError(5, 'unit file fsync')
            return real_fsync(fd)
        def read(fd, size):
            raw = real_read(fd, size)
            return raw[:-1] + b'X' if fault == 'readback' and size == 1025 else raw
        def close(fd):
            if fault == 'close' and fd not in failed_fd and os.fstat(fd).st_size == 1024:
                failed_fd.append(fd)
                raise OSError(5, 'unit close')
            return real_close(fd)
        with monkeypatch.context() as patch:
            patch.setattr(os, 'write', write)
            patch.setattr(os, 'fsync', fsync)
            patch.setattr(os, 'read', read)
            patch.setattr(os, 'close', close)
            with pytest.raises((audit.ContractError, OSError)):
                audit.commit_baseline(owner.parent, baseline, owner=owner, budget=OutputBudget())
        assert owner.has_debt and counts['write'] == 1
        assert (fixture / 'staging-a2-baseline-v2.bin').exists()
    finally:
        owner.close_all()
        for fd in failed_fd:
            real_close(fd)


def test_exact_pending_slot1_intent_checks_slot0_without_weakening_a1(fixture):
    r, audit, o = modules()
    from orchestrator.isolation import _staging_arena as a
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(a.SlotArena.open_existing(roots.arena_fd))
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
            pending = {**arena._header, 'pending': {'slot_index': 1, 'token': '77' * 16, 'revision': 1}}
            expected = audit._prepare_slot1_transition_locked(arena, token='77' * 16, revision=1)
            a._write_fixed(arena._header_fd, pending, 1024)
            with pytest.raises(audit.ContractError):
                arena._read()
            audit.assert_slot0_unchanged_locked(arena, baseline, expected_transition=expected)
            a._write_fixed(arena._header_fd, {**pending, 'pending': {'slot_index': 1, 'token': '88' * 16, 'revision': 1}}, 1024)
            with pytest.raises(audit.ContractError):
                audit.assert_slot0_unchanged_locked(arena, baseline, expected_transition=expected)
        finally:
            arena._leave()
    finally:
        owner.close_all()

def test_pending_boolean_intent_and_observed_corrupt_record_are_rejected(fixture):
    r, audit, o = modules()
    from orchestrator.isolation import _staging_arena as a
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(a.SlotArena.open_existing(roots.arena_fd))
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
            pending = {**arena._header, 'pending': {'slot_index': True, 'token': None, 'revision': True}}
            a._write_fixed(arena._header_fd, pending, 1024)
            os.pwrite(arena._record_fds[1], b'X', 0)
            with pytest.raises((audit.ContractError, TypeError)):
                expected = audit._PendingSlot1Transition(arena, None, 1,
                    hashlib.sha256(os.pread(arena._header_fd, 1024, 0)).hexdigest(),
                    hashlib.sha256(os.pread(arena._record_fds[1], 512, 0)).hexdigest())
                audit.assert_slot0_unchanged_locked(arena, baseline, expected_transition=expected)
        finally:
            arena._leave()
    finally:
        owner.close_all()


@pytest.mark.parametrize('artifact', ['baseline', 'history'])
def test_audit_writer_refuses_owned_source_as_parent_before_creation(fixture, artifact):
    r, audit, o = modules()
    from orchestrator.isolation._staging_arena import SlotArena
    from orchestrator.isolation._staging_contract import OutputBudget
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(SlotArena.open_existing(roots.arena_fd))
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
        finally:
            arena._leave()
        if artifact == 'baseline':
            value = baseline
            commit = audit.commit_baseline
        else:
            from orchestrator.isolation._staging_a2_contract import AttemptBinding, ExitKind, Role
            value = audit.RetentionHistory(AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32), '55' * 32,
                (audit.ProcessFact(Role.R, Role.HOST, 101, 100, ExitKind.EXITED, 0, True),
                 audit.ProcessFact(Role.S, Role.R, 102, 101, ExitKind.EXITED, 0, True),
                 audit.ProcessFact(Role.T, Role.S, 103, 102, ExitKind.EXITED, 0, True)), 0xff, 14)
            commit = audit.commit_history
        with pytest.raises(audit.ContractError):
            commit(owner._fixture_caps[2], value, owner=owner, budget=OutputBudget())
        assert not (fixture / 'staging-fixture/source/staging-a2-baseline-v2.bin').exists()
        assert not (fixture / 'staging-fixture/source/staging-a2-retention-v2.bin').exists()
    finally:
        owner.close_all()

@pytest.mark.parametrize('damage', ['bool_slot', 'bool_revision', 'short_record', 'wrong_schema', 'canonical_uncommitted'])
def test_pending_requires_independent_canonical_preintent_commitment(fixture, damage):
    r, audit, o = modules()
    from orchestrator.isolation import _staging_arena as v
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(v.SlotArena.open_existing(roots.arena_fd))
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
            expected = audit._prepare_slot1_transition_locked(arena, token='77' * 16, revision=1)
            intent = {'slot_index': 1, 'token': '77' * 16, 'revision': 1}
            if damage == 'bool_slot':
                intent['slot_index'] = True
            elif damage == 'bool_revision':
                intent['revision'] = True
            v._write_fixed(arena._header_fd, {**arena._header, 'pending': intent}, 1024)
            if damage == 'short_record':
                os.ftruncate(arena._record_fds[1], 511)
            elif damage in ('wrong_schema', 'canonical_uncommitted'):
                record = v._loads(expected.prior_slot1, v._RECORD_FIELDS)
                if damage == 'wrong_schema':
                    record['schema'] = 'invalid'
                else:
                    record.update(state='RESERVED', token='88' * 16)
                v._write_fixed(arena._record_fds[1], record, 512)
            with pytest.raises(audit.ContractError):
                audit.assert_slot0_unchanged_locked(arena, baseline, expected_transition=expected)
            with pytest.raises(audit.ContractError):
                arena._read()
        finally:
            arena._leave()
    finally:
        owner.close_all()


def test_same_parent_inode_alias_cannot_replace_canonical_parent_acquisition(fixture):
    r, audit, o = modules()
    from orchestrator.isolation._staging_arena import SlotArena
    from orchestrator.isolation._staging_a2_contract import Role
    from orchestrator.isolation._staging_ownership import FDKind
    from orchestrator.isolation._staging_contract import OutputBudget
    owner = o.AcquisitionLedger()
    try:
        roots = r.open_fixture_roots_existing(fixture, owner=owner)
        arena = owner.adopt_returned_arena(SlotArena.open_existing(roots.arena_fd))
        arena._enter()
        try:
            baseline = audit.capture_slot0_locked(arena, roots)
        finally:
            arena._leave()
        fd = os.open('.', os.O_RDONLY | os.O_DIRECTORY, dir_fd=roots._parent_fd)
        info = os.fstat(fd)
        alias = owner.adopt_fd(fd, role=Role.HOST, identity=(info.st_dev, info.st_ino), kind=FDKind.DIRECTORY)
        with pytest.raises(audit.ContractError):
            audit.commit_baseline(alias, baseline, owner=owner, budget=OutputBudget())
        assert not (fixture / 'staging-a2-baseline-v2.bin').exists()
    finally:
        owner.close_all()
