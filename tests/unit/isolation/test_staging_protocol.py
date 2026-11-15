"""Local sockets and inert metadata only; these tests prove no native closure."""
import os
import socket
import struct
import subprocess
import time
from dataclasses import replace

import pytest

from orchestrator.isolation import _staging_a2_contract as a
from orchestrator.isolation import _staging_protocol as p
from orchestrator.isolation._staging_contract import ContractError
from orchestrator.isolation._staging_ownership import AcquisitionLedger, FDKind


BINDING = a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32)


@pytest.fixture(autouse=True)
def no_process_or_native(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('B1 must not submit processes or native controls')
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    monkeypatch.setattr(os, 'fork', forbidden)
    monkeypatch.setattr(os, 'unshare', forbidden, raising=False)
    import resource
    import ctypes
    monkeypatch.setattr(resource, 'setrlimit', forbidden)
    monkeypatch.setattr(ctypes, 'CDLL', forbidden)


def frame(op, src, dst, sequence, payload=None):
    return a.Frame(op, src, dst, sequence, BINDING, payload)


def test_clock_uses_original_deadline_and_one_cleanup_end():
    clock = p.ProtocolClock(10)
    assert clock.deadline_ns == 30_000_000_010
    assert clock.begin_cleanup(29_000_000_010) == 34_000_000_010
    assert clock.begin_cleanup(30_000_000_010) == 34_000_000_010
    with pytest.raises(ContractError):
        clock.check(30_000_000_010)
    clock.check(34_000_000_009, terminal=True)
    with pytest.raises(ContractError):
        clock.check(34_000_000_010, terminal=True)


def test_clock_late_first_cleanup_is_capped_at_original_plus_five_seconds():
    clock = p.ProtocolClock(10)
    assert clock.begin_cleanup(80_000_000_010) == 35_000_000_010
    with pytest.raises(ContractError):
        clock.check(80_000_000_010, terminal=True)


def test_budget_keeps_history_inside_terminal_domain(tmp_path):
    owner, budget = make_budget(tmp_path)
    assert budget.charge_ordinary(14336)
    assert not budget.charge_ordinary(1)
    for slot in range(8):
        budget.claim_terminal(slot)
    assert budget.charge_history(512)
    assert not budget.charge_history(1)
    assert budget.snapshot() == (14337, 255, 513)
    with pytest.raises(ContractError):
        budget.claim_terminal(0)
    owner.close_all()


def make_budget(tmp_path):
    owner = AcquisitionLedger()
    fd = os.open(tmp_path / 'shared', os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    st = os.fstat(fd)
    cap = owner.adopt_fd(fd, role=a.Role.HOST, identity=(st.st_dev, st.st_ino))
    budget = p.ProtocolBudget.create(owner, cap)
    return owner, budget


def test_budget_is_shared_between_independent_views(tmp_path):
    owner, budget = make_budget(tmp_path)
    other = p.ProtocolBudget.attach(owner, budget.acquisition)
    assert budget.charge_ordinary(27 * 192 + 1024 + 1536)
    assert other.snapshot() == (7744, 0, 0)
    other.claim_terminal(7)
    assert budget.snapshot() == (7744, 128, 0)
    assert other.charge_history(512)
    assert budget.snapshot() == (7744, 128, 512)
    owner.close_all()


def test_fixture_prefix_and_native_grant_are_distinct():
    cursor, facts = inert_cursor(a.Role.HOST, p.ProtocolFamily.FIXTURE)
    start = frame(a.Opcode.START, a.Role.HOST, a.Role.R, 0,
                  a.StartPayload((0, 1, 2, 3, 4, 5), 10, 30_000_000_010, 1000, 1000))
    cursor.accept(start, (100, 1000, 1000), now_ns=11)
    cursor.accept(frame(a.Opcode.GROUPS, a.Role.HOST, a.Role.R, 1, a.GroupsPayload(())),
                  (100, 1000, 1000), now_ns=11)
    with pytest.raises(ContractError):
        cursor.accept(frame(a.Opcode.GO_NATIVE, a.Role.HOST, a.Role.R, 2),
                      (100, 1000, 1000), now_ns=11)


# Hand-transcribed addendum, including the independent per-direction counters.
NATIVE = (
    ('START', 0, 1, 0, None), ('GROUPS', 0, 1, 1, None), ('HELLO', 1, 0, 0, 1),
    ('ACK_REGISTER', 0, 1, 2, 1), ('HELLO', 2, 1, 0, 2),
    ('IDENTITY', 1, 0, 1, 2), ('ACK_REGISTER', 0, 1, 3, 2),
    ('ACK_REGISTER', 1, 2, 0, 2), ('HELLO', 3, 2, 0, 3),
    ('IDENTITY', 2, 1, 1, 3), ('IDENTITY', 1, 0, 2, 3),
    ('ACK_REGISTER', 0, 1, 4, 3), ('ACK_REGISTER', 1, 2, 1, 3),
    ('ACK_REGISTER', 2, 3, 0, 3), ('GO_NATIVE', 0, 1, 5, None),
    ('GO_NATIVE', 1, 2, 2, None), ('GO_NATIVE', 2, 3, 1, None),
    ('HELLO', 4, 3, 0, 4), ('IDENTITY', 3, 2, 1, 4),
    ('IDENTITY', 2, 1, 2, 4), ('IDENTITY', 1, 0, 3, 4),
    ('ACK_REGISTER', 0, 1, 6, 4), ('ACK_REGISTER', 1, 2, 3, 4),
    ('ACK_REGISTER', 2, 3, 2, 4), ('ACK_REGISTER', 3, 4, 0, 4),
    ('NS_IDENTITY', 4, 3, 1, None), ('GO_CLOSE_RANGE', 3, 4, 1, None),
)


def inert_cursor(role, family=p.ProtocolFamily.NATIVE, *, created=4, parent=True):
    owner = AcquisitionLedger()
    facts = p._OwnerFacts(owner)
    facts.local = p._LocalIdentity(role, 100 + int(role), 1000, 1000)
    facts.credentials = {r: (100 + int(r), 1000, 1000) for r in a.Role}
    if role is a.Role.HOST: register_peer(facts, a.Role.R)
    for neighbor in a.Role:
        if abs(int(role) - int(neighbor)) == 1:
            facts.endpoints[neighbor] = owner.adopt_fd(900 + int(neighbor), role=role,
                identity=(1, 900 + int(neighbor)), kind=FDKind.CONTROL_SOCKET)
    if role is not a.Role.HOST and parent:
        ancestor = a.Role(int(role) - 1)
        cap = owner.adopt_fd(700, role=ancestor, identity=(1, 700), kind=FDKind.PIDFD)
        owner.bind_parent_pidfd(cap, child=role, parent_pid=100 + int(ancestor),
                                visible_pid=0 if role is a.Role.C else 100 + int(ancestor))
        if ancestor is not a.Role.HOST:
            facts.peers[ancestor] = p._Peer(100 + int(ancestor), 1000, 1000, a.Role(int(ancestor) - 1), cap)
    cursor = p.ProtocolCursor(BINDING, family, role)
    cursor._bind_owner(facts, p.ProtocolClock(10))
    return cursor, facts


def register_peer(facts, role):
    """Inert stand-in for B2's independent returned pidfd registration."""
    if role in facts.peers: return facts.peers[role]
    fd = 800 + int(role)
    cap = facts.owner.adopt_fd(fd, role=role, identity=(1, fd), kind=FDKind.PIDFD)
    peer = p._Peer(100 + int(role), 1000, 1000, a.Role(int(role) - 1), cap)
    facts.peers[role] = peer
    return peer


def supply_reap(facts, subject, *, kind=a.ExitKind.EXITED, status=0, closed=0):
    # Independent inert wait result, not reconstructed from a decoded payload.
    facts.targeted_reaps[subject] = (facts.known(subject).pidfd, 100 + int(subject), kind, status, closed)


def ordinary_frame(row):
    op, src, dst, sequence, subject = row
    opcode, src, dst = a.Opcode[op], a.Role(src), a.Role(dst)
    payload = None
    if op == 'START':
        payload = a.StartPayload((0, 1, 2, 3, 4, 5), 10, 30_000_000_010, 1000, 1000)
    elif op == 'GROUPS':
        payload = a.GroupsPayload((1000,))
    elif subject is not None:
        subject = a.Role(subject)
        parent, pid = a.Role(int(subject) - 1), 100 + int(subject)
        payload = (a.IdentityPayload(subject, parent, pid, 1000, 1000) if op == 'IDENTITY'
                   else a.RegistrationPayload(subject, pid, parent))
    elif op == 'NS_IDENTITY':
        payload = a.NamespacePayload(((1, 2), (1, 3), (1, 4)), 1, 0)
    return frame(opcode, src, dst, sequence, payload)


def prefix(cursor, facts, count, now_ns=11):
    for index, row in enumerate(NATIVE[:count], 1):
        if index == 18 and cursor.role is a.Role.T: facts.maps_verified = True
        if index == 26 and cursor.role is a.Role.C:
            facts.maps_verified = facts.namespace_checked = True
        if index == 27 and cursor.role is a.Role.T: facts.namespace_checked = True
        value = ordinary_frame(row)
        if cursor.role in (value.src, value.dst):
            if value.opcode is a.Opcode.START:
                value = replace(value, payload=replace(value.payload, started_ns=cursor.clock.started_ns, deadline_ns=cursor.clock.deadline_ns))
            if value.opcode is a.Opcode.HELLO and value.dst is cursor.role:
                register_peer(facts, value.payload.subject)
            unknown = value.opcode is a.Opcode.IDENTITY and value.dst is cursor.role
            if unknown: assert value.payload.subject not in facts.peers
            transition = cursor.accept(value, message_credentials(facts, value), now_ns=now_ns)
            if unknown:
                assert transition.pending_identity == value.payload
                assert value.payload.subject not in facts.peers
                register_peer(facts, value.payload.subject)
    facts.prefix = cursor._progress


def message_credentials(facts, value):
    if (value.src, value.dst) == (a.Role.T, a.Role.C): return (0, 0, 0)
    if (value.src, value.dst) == (a.Role.C, a.Role.T): return (104, 0, 0)
    if (value.src, value.dst) == (a.Role.S, a.Role.T) and value.opcode is a.Opcode.ACK_REGISTER and value.payload.subject is a.Role.C:
        return (102, 0, 0)
    return facts.credentials[value.src]


@pytest.mark.parametrize('role', tuple(a.Role))
def test_normal_27_frame_trace_checks_each_direction_without_minting_facts(role):
    cursor, facts = inert_cursor(role)
    facts.namespace_checked = True  # inert metadata, not a real namespace proof
    prefix(cursor, facts, 27)
    assert facts.reaped == facts.exited == facts.closed == set()
    assert max(cursor._sequences.values()) <= 7
    with pytest.raises(ContractError):
        cursor.accept(ordinary_frame(NATIVE[-1]), facts.credentials[a.Role.T], now_ns=11)


@pytest.mark.parametrize('mutation', ('gap', 'attempt', 'token', 'generation', 'manifest', 'credentials', 'role', 'subject', 'endpoint'))
def test_ordinary_refuses_bad_binding_sequence_role_and_exact_acquisition(mutation):
    cursor, facts = inert_cursor(a.Role.HOST)
    prefix(cursor, facts, 2)
    value = ordinary_frame(NATIVE[2])
    credentials = facts.credentials[value.src]
    if mutation == 'gap':
        value = replace(value, sequence=1)
    elif mutation in ('attempt', 'token', 'generation', 'manifest'):
        value = replace(value, binding=replace(BINDING, **{mutation: 'ff' * (32 if mutation == 'manifest' else 16)}))
    elif mutation == 'credentials':
        credentials = (999, 1000, 1000)
    elif mutation == 'role':
        value = frame(a.Opcode.HELLO, a.Role.S, a.Role.R, 0, a.RegistrationPayload(a.Role.S, 102, a.Role.R))
    elif mutation == 'subject':
        value = replace(value, payload=a.RegistrationPayload(a.Role.C, 104, a.Role.T))
    else:
        old = facts.endpoints[a.Role.R]
        # Same number/identity with a different generation is not this edge.
        facts.endpoints[a.Role.R] = type(old)(old.fd, old._owner, old._generation + 1)
    with pytest.raises(ContractError):
        cursor.accept(value, credentials, now_ns=11)


def test_decoded_reap_observation_cannot_add_exit_reap_or_closure_facts():
    cursor, facts = inert_cursor(a.Role.S)
    facts.namespace_checked = True
    prefix(cursor, facts, 27)
    cursor._select_terminal(a.TerminalFamily.N_C, now_ns=12)
    value = frame(a.Opcode.REAP_ACK, a.Role.T, a.Role.S, 2,
        a.ExitPayload(a.Role.C, a.Role.T, 104, a.ExitKind.EXITED, 0, 0, None, None))
    decoded = a.Frame.from_bytes(value.to_bytes())
    cursor.accept(decoded, facts.credentials[a.Role.T], now_ns=13)
    assert facts.exited == facts.reaped == facts.closed == set()
    assert facts.targeted_reaps == {}
    with pytest.raises(ContractError):
        cursor.accept(terminal_frames('N_C')[2], facts.credentials[a.Role.S], now_ns=13)


def test_manifest_includes_only_existing_protocol_file():
    from orchestrator.isolation import _staging_contract as contract
    entries = {role: path for role, path, kind in contract._MANIFEST_SOURCES}
    assert os.path.isfile(entries['code_protocol'])
    assert not {'code_supervisor', 'code_entry', 'code_retention_entry', 'code_release'} & entries.keys()


def exit_payload(subject, cause=None):
    return a.ExitPayload(a.Role(subject), a.Role(subject - 1), 100 + subject,
                         a.ExitKind.EXITED, 0, 0, cause, None)


def closed_payload(bits, cause=None):
    cause = cause or a.FailureRecord(a.Role.R, a.NativePhase.CONTROL_CODEC, a.FailureKind.PROTOCOL_FAILURE, None)
    status = a.FixtureStatus.UNSUPPORTED if cause.kind is a.FailureKind.NATIVE_REJECTION else a.FixtureStatus.FAILED
    reason = a.ReasonCode.UNSUPPORTED_NAMESPACES if status is a.FixtureStatus.UNSUPPORTED else a.ReasonCode.MALFORMED_PROTOCOL
    return a.ResultPayload(status, reason, 0, a.OwnerState.CLOSED, bits, bits, cause, None)


def terminal_frames(family, cause=None):
    if family == 'N_C':
        return (frame(a.Opcode.EXIT_NOTE, a.Role.C, a.Role.T, 2, exit_payload(4)),
                frame(a.Opcode.REAP_ACK, a.Role.T, a.Role.S, 2, exit_payload(4)),
                frame(a.Opcode.REAP_ACK, a.Role.S, a.Role.R, 3, exit_payload(4)),
                frame(a.Opcode.REAP_ACK, a.Role.S, a.Role.R, 4, exit_payload(3)),
                frame(a.Opcode.RESULT_CLOSED, a.Role.R, a.Role.HOST, 4, closed_payload(28)),
                frame(a.Opcode.PREFIX_CLOSED, a.Role.R, a.Role.HOST, 5, a.PrefixPayload(28, 28, 28, closed_payload(28).cause, None)))
    spec = {'R_0': (0, 0), 'R_1': (1, 0), 'S_0': (2, 4), 'T_0': (3, 12)}
    sequence, bits = spec[family]
    tail = (frame(a.Opcode.RESULT_CLOSED, a.Role.R, a.Role.HOST, sequence, closed_payload(bits, cause)),
            frame(a.Opcode.PREFIX_CLOSED, a.Role.R, a.Role.HOST, sequence + 1,
                  a.PrefixPayload(bits, bits, bits, closed_payload(bits, cause).cause, None)))
    if family == 'S_0':
        return (frame(a.Opcode.EXIT_NOTE, a.Role.S, a.Role.R, 1, exit_payload(2, cause)), *tail)
    if family == 'T_0':
        return (frame(a.Opcode.EXIT_NOTE, a.Role.T, a.Role.S, 1, exit_payload(3, cause)),
                frame(a.Opcode.REAP_ACK, a.Role.S, a.Role.R, 2, exit_payload(3, cause)), *tail)
    return tail


@pytest.mark.parametrize('family,count,created,slots', [('N_C', 27, 4, 6), ('R_0', 2, 1, 2),
    ('R_1', 3, 1, 2), ('R_1', 4, 1, 2), ('S_0', 8, 2, 3), ('T_0', 14, 3, 4),
    ('T_0', 15, 3, 4), ('T_0', 16, 3, 4), ('T_0', 17, 3, 4)])
def test_exact_native_terminal_tables(family, count, created, slots):
    cursors = [inert_cursor(a.Role(role), created=created) for role in range(created + 1)]
    for cursor, facts in cursors:
        facts.namespace_checked = True
        prefix(cursor, facts, count)
    values = terminal_frames(family)
    producer = values[0].src
    cursors[int(producer)][0]._select_terminal(a.TerminalFamily[family], now_ns=12)
    assert len(values) == slots
    for slot, value in enumerate(values):
        assert 90 <= len(value.to_bytes()) <= 160
        for cursor, facts in cursors:
            if cursor.role in (value.src, value.dst):
                if cursor.role is value.src:
                    if value.opcode is a.Opcode.REAP_ACK:
                        facts.exited.add(value.payload.subject)
                        facts.reaped.add(value.payload.subject)
                        facts.closed.add(value.payload.subject)
                        if value.payload.parent is cursor.role:
                            supply_reap(facts, value.payload.subject)
                    final_tail = value.opcode is a.Opcode.EXIT_NOTE or value.opcode is a.Opcode.REAP_ACK and value.payload.parent is cursor.role
                    if final_tail and cursor.role in (a.Role.S, a.Role.T):
                        exit_peer = a.Role(int(cursor.role) - 1)
                        for record in facts.owner._records.values():
                            if record.cap is not facts.endpoints[exit_peer]: record.attempted = record.closed = True
                    if value.opcode in (a.Opcode.RESULT_CLOSED, a.Opcode.PREFIX_CLOSED):
                        facts.exited = facts.reaped = facts.closed = {role for role in facts.peers if role > cursor.role}
                        facts.owner.cause = value.payload.cause
                        for record in facts.owner._records.values():
                            if record.cap is not facts.endpoints[a.Role.HOST]: record.attempted = record.closed = True
                transition = cursor.accept(value, message_credentials(facts, value), now_ns=13)
                assert transition.terminal_slot == slot
    # The terminal label has not closed a single acquisition or minted proof.
    assert all(not facts.owner.all_closed for cursor, facts in cursors)
    assert a.Role.R not in cursors[0][1].reaped


def test_fixture_exact_seven_terminal_slots_and_original_cleanup_window(tmp_path):
    budget_owner, budget = make_budget(tmp_path)
    cursors = [inert_cursor(a.Role(role), p.ProtocolFamily.FIXTURE, created=3) for role in range(4)]
    for cursor, facts in cursors:
        prefix(cursor, facts, 14)
        cursor.clock._bind_budget(budget)
        if cursor.role is not a.Role.HOST:
            facts.parked, facts.state = True, a.OwnerState.PARKED_UNKNOWN
    cursors[1][0]._select_terminal(a.TerminalFamily.T_0, now_ns=12)
    parked = a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.LOCAL_CLEANUP_UNKNOWN, 0,
                             a.OwnerState.PARKED_UNKNOWN, 12, 0, None, None)
    values = (frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, 3, parked),
              frame(a.Opcode.CONVERGE_PREUNSHARE, a.Role.HOST, a.Role.R, 5),
              frame(a.Opcode.CONVERGE_PREUNSHARE, a.Role.R, a.Role.S, 2),
              frame(a.Opcode.CONVERGE_PREUNSHARE, a.Role.S, a.Role.T, 1),
              frame(a.Opcode.EXIT_NOTE, a.Role.T, a.Role.S, 1, exit_payload(3)),
              frame(a.Opcode.REAP_ACK, a.Role.S, a.Role.R, 2, exit_payload(3)),
              frame(a.Opcode.PREFIX_CLOSED, a.Role.R, a.Role.HOST, 4, a.PrefixPayload(12, 12, 12, None, None)))
    for slot, value in enumerate(values):
        for cursor, facts in cursors:
            if cursor.role in (value.src, value.dst):
                if slot >= 1: facts.parked = True
                if slot >= 1: facts.state = a.OwnerState.PARKED_UNKNOWN
                if cursor.role is value.src and value.opcode in (a.Opcode.EXIT_NOTE, a.Opcode.REAP_ACK):
                    facts.exited = facts.reaped = facts.closed = {role for role in facts.peers if role > cursor.role}
                    if value.opcode is a.Opcode.REAP_ACK:
                        supply_reap(facts, value.payload.subject)
                    exit_peer = a.Role(int(cursor.role) - 1)
                    for record in facts.owner._records.values():
                        if record.cap is not facts.endpoints[exit_peer]: record.attempted = record.closed = True
                if value.opcode is a.Opcode.PREFIX_CLOSED and cursor.role is a.Role.R:
                    facts.exited = facts.reaped = facts.closed = {a.Role.S, a.Role.T}
                    for record in facts.owner._records.values():
                        if record.cap is not facts.endpoints[a.Role.HOST]: record.attempted = record.closed = True
                assert cursor.accept(value, facts.credentials[value.src], now_ns=13).terminal_slot == slot
                assert cursor.clock.cleanup_end == 5_000_000_012
    assert a.Role.R not in cursors[0][1].reaped
    budget_owner.close_all()


def test_closed_abort_cannot_select_an_uncreated_or_unregistered_prefix():
    cursor, facts = inert_cursor(a.Role.HOST, created=2)
    prefix(cursor, facts, 2)
    register_peer(facts, a.Role.S)
    with pytest.raises(ContractError):
        cursor._select_terminal(a.TerminalFamily.R_0, now_ns=12)


def test_pregrant_T_cannot_claim_unshare_failure():
    cursor, facts = inert_cursor(a.Role.S, created=3)
    prefix(cursor, facts, 14)
    cursor._select_terminal(a.TerminalFamily.T_0, now_ns=12)
    cause = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    value = terminal_frames('T_0', cause)[0]
    with pytest.raises(ContractError):
        cursor.accept(value, facts.credentials[a.Role.T], now_ns=13)


@pytest.mark.parametrize('count,next_sequence', [(2, 0), (3, 1), (5, 1), (6, 2), (10, 2), (11, 3), (20, 3), (21, 4), (27, 4)])
def test_U_uses_one_report_with_existing_R_H_sequence(count, next_sequence):
    cursor, facts = inert_cursor(a.Role.HOST)
    facts.namespace_checked = True
    prefix(cursor, facts, count)
    cursor._select_terminal(a.TerminalFamily.U, now_ns=12)
    value = frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, next_sequence,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 0, 0, None, None))
    cursor.accept(value, facts.credentials[a.Role.R], now_ns=13)
    with pytest.raises(ContractError):
        cursor.accept(replace(value, sequence=next_sequence + 1), facts.credentials[a.Role.R], now_ns=13)


def test_ambiguous_transmission_never_resets_or_replays_R_report():
    cursor, facts = inert_cursor(a.Role.HOST)
    prefix(cursor, facts, 2)
    cursor.mark_uncertain(a.Role.R, a.Role.HOST)
    cursor._select_terminal(a.TerminalFamily.U, now_ns=12)
    value = frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, 0,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 0, 0, None, None))
    with pytest.raises(ContractError):
        cursor.accept(value, facts.credentials[a.Role.R], now_ns=13)


def delegated_socket(owner, value, role=a.Role.HOST):
    cap = owner.adopt_fd(value.fileno(), role=role, identity=None, kind=FDKind.CONTROL_SOCKET)
    owner._delegate(value, (cap,))
    info = os.fstat(cap.fd)
    owner.bind_identity(cap, (info.st_dev, info.st_ino))
    return cap


def test_stale_socket_number_is_not_closed_by_ledger_or_later_wrapper(tmp_path):
    owner = AcquisitionLedger()
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    cap = delegated_socket(owner, left)
    old_fd = cap.fd
    os.close(old_fd)  # inert simulation of external ownership corruption
    replacement = os.open(tmp_path / 'replacement', os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    assert replacement == old_fd
    try:
        with pytest.raises(ContractError):
            owner.close_all()
        assert owner.has_debt
        assert left.fileno() == -1
        left.close()
        owner.close_all()
        os.fstat(replacement)
    finally:
        # Detach on the old implementation too, to keep a failing RED safe.
        if left.fileno() != -1:
            left.detach()
        os.close(replacement)
        right.close()


class LocalControl(BaseException):
    pass


def test_socket_close_control_survives_bookkeeping_and_is_not_retried(monkeypatch):
    owner = AcquisitionLedger()
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    cap = delegated_socket(owner, left)
    control = LocalControl()
    original = socket.socket.close
    attempts = []
    def uncertain_close(value):
        if value is left:
            attempts.append(value.fileno())
            raise control
        return original(value)
    def broken_bookkeeping(*args):
        raise MemoryError()
    monkeypatch.setattr(socket.socket, 'close', uncertain_close)
    monkeypatch.setattr(owner, 'poison_debt', broken_bookkeeping)
    monkeypatch.setattr(owner, 'record_cleanup', broken_bookkeeping)
    try:
        with pytest.raises(LocalControl) as caught:
            owner.close_all()
        assert caught.value is control
        assert owner.has_debt and not owner.all_closed
        assert left.fileno() == -1
        owner.close_all()
        assert attempts == [cap.fd]
        assert not any(isinstance(item, BaseException) for item in vars(owner).values())
    finally:
        if left.fileno() != -1:
            left.detach()
        os.close(cap.fd)
        original(right)


def test_socket_close_failure_retains_actual_non_host_actor(monkeypatch):
    owner = AcquisitionLedger()
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    cap = delegated_socket(owner, left, a.Role.S)
    original = socket.socket.close
    def uncertain_close(value):
        if value is left: raise OSError(5, 'inert')
        return original(value)
    monkeypatch.setattr(socket.socket, 'close', uncertain_close)
    try:
        with pytest.raises(OSError): owner.close_all()
        assert owner.cleanup == a.FailureRecord(a.Role.S, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None)
    finally:
        if left.fileno() != -1: left.detach()
        os.close(cap.fd)
        original(right)


def local_channel(tmp_path, *, role=a.Role.R, family=p.ProtocolFamily.NATIVE):
    budget_owner, budget = make_budget(tmp_path)
    cursor, facts = inert_cursor(role, family, created=1)
    cursor.clock = p.ProtocolClock(time.monotonic_ns())
    facts.credentials = {r: (os.getpid(), os.getuid(), os.getgid()) for r in a.Role}
    facts.local = p._LocalIdentity(role, os.getpid(), os.getuid(), os.getgid())
    if role is a.Role.R:
        facts.owner._record(facts.owner._parents[a.Role.R]).binding = (a.Role.R, os.getpid(), os.getpid())
    own, remote = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    peer = a.Role.HOST if role is a.Role.R else a.Role.R
    # Remove the inert endpoint entry; its synthetic number is not an OS FD.
    facts.endpoints.pop(peer)
    channel = p.BoundChannel(own, facts.owner, cursor, budget, peer)
    start = frame(a.Opcode.START, a.Role.HOST, a.Role.R, 0,
        a.StartPayload((0, 1, 2, 3, 4, 5), cursor.clock.started_ns,
                       cursor.clock.deadline_ns, os.getuid(), os.getgid()))
    return channel, remote, start, budget_owner


def close_channel(channel, remote, budget_owner):
    channel.close()
    remote.close()
    budget_owner.close_all()


def test_local_seqpacket_receive_uses_per_message_credentials_and_no_receive_charge(tmp_path):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    try:
        remote.send(start.to_bytes())
        value = channel.receive()
        assert value == start
        assert budget_owner is not channel.owner
        assert channel.budget.snapshot() == (0, 0, 0)
        assert channel._socket.getsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED) == 1
    finally:
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('case', ['missing', 'wrong', 'extra', 'unknown', 'short_credentials',
    'truncated_data', 'truncated_control', 'oversized', 'bad_magic', 'rights_before_bad', 'rights_after_bad'])
def test_bounded_ancillary_rejection_closes_delivered_rights_before_validation(tmp_path, monkeypatch, case):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    delivered = []
    cred = (socket.SOL_SOCKET, socket.SCM_CREDENTIALS, struct.pack('@iii', os.getpid(), os.getuid(), os.getgid()))
    ancillary, flags, raw = [cred], 0, start.to_bytes()
    if case == 'missing': ancillary = []
    if case == 'wrong': ancillary = [(socket.SOL_SOCKET, socket.SCM_CREDENTIALS, struct.pack('@iii', os.getpid() + 1, os.getuid(), os.getgid()))]
    if case == 'extra': ancillary = [cred, cred]
    if case == 'unknown': ancillary.append((999, 999, b'unknown'))
    if case == 'short_credentials': ancillary = [(socket.SOL_SOCKET, socket.SCM_CREDENTIALS, b'x')]
    if case == 'truncated_data': flags = socket.MSG_TRUNC
    if case == 'truncated_control': flags = socket.MSG_CTRUNC
    if case == 'oversized': raw = b'x' * 161
    if case == 'bad_magic': raw = b'FAIL' + raw[4:]
    if case.startswith('rights_'):
        delivered = [os.dup(remote.fileno()), os.dup(remote.fileno())]
        rights = (socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('@ii', *delivered))
        bad = (999, 999, b'bad')
        ancillary = [bad, rights] if case == 'rights_after_bad' else [rights, bad]
    received = []
    def recv(value, data_size, ancillary_size, recv_flags):
        assert value is channel._socket
        received.append((data_size, ancillary_size, recv_flags))
        return raw, ancillary, flags, None
    monkeypatch.setattr(socket.socket, 'recvmsg', recv)
    # A real readable packet makes the absolute-deadline readiness path run.
    remote.send(b'ready')
    try:
        with pytest.raises(ContractError):
            channel.receive()
        assert received == [(161, 32, socket.MSG_CMSG_CLOEXEC | socket.MSG_DONTWAIT)]
        for fd in delivered:
            with pytest.raises(OSError): os.fstat(fd)
        assert not channel.cursor._sequences
        assert channel.cursor._facts.ambiguous
    finally:
        close_channel(channel, remote, budget_owner)


def test_actual_rights_and_ancillary_truncation_adopt_every_delivered_fd(tmp_path):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    before = set(channel.owner._records)
    try:
        # The 32-byte production envelope is entirely consumed by credentials.
        # Disable them ONLY for this direct kernel ancillary/cleanup exercise,
        # so the kernel actually delivers four rights and truncates the rest.
        channel._socket.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 0)
        remote.sendmsg([start.to_bytes()], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
            struct.pack('@8i', *([remote.fileno()] * 8)))])
        raw, ancillary, flags, address = channel._socket.recvmsg(161, 32, socket.MSG_CMSG_CLOEXEC)
        assert flags & socket.MSG_CTRUNC
        assert channel._close_delivered_rights(ancillary)
        added = set(channel.owner._records) - before
        assert len(added) == 4
        for cap in added:
            assert channel.owner._record(cap).closed
            with pytest.raises(OSError): os.fstat(cap.fd)
    finally:
        close_channel(channel, remote, budget_owner)


def test_uncertain_send_charges_once_preserves_control_and_never_retries(tmp_path, monkeypatch):
    channel, remote, start, budget_owner = local_channel(tmp_path, role=a.Role.HOST)
    original = socket.socket.sendmsg
    control = LocalControl()
    def delivered_then_lost(value, buffers, ancdata, flags):
        original(value, buffers, ancdata, flags)
        raise control
    monkeypatch.setattr(socket.socket, 'sendmsg', delivered_then_lost)
    try:
        with pytest.raises(LocalControl) as caught:
            channel.send(start)
        assert caught.value is control
        assert remote.recv(161) == start.to_bytes()
        with pytest.raises(ContractError): channel.send(start)
        assert channel.budget.snapshot() == (192, 0, 0)
        assert not channel.cursor._sequences
        assert channel.cursor._facts.ambiguous
    finally:
        close_channel(channel, remote, budget_owner)


def test_eof_is_control_loss_without_closure_or_reap(tmp_path):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    remote.close()
    try:
        with pytest.raises(ContractError): channel.receive()
        assert channel.cursor._facts.ambiguous
        assert channel.cursor._facts.reaped == channel.cursor._facts.closed == set()
    finally:
        close_channel(channel, remote, budget_owner)


def test_C_parent_zero_credentials_require_exact_inherited_parent_capability():
    cursor, facts = inert_cursor(a.Role.C, parent=False)
    facts.namespace_checked = True
    # C sees the registered T outside its PID namespace as PID 0, uid/gid 0.
    value = ordinary_frame(NATIVE[24])
    cursor.accept(ordinary_frame(NATIVE[17]), (104, 0, 0), now_ns=11)
    with pytest.raises(ContractError):
        cursor.accept(value, (0, 0, 0), now_ns=11)
    cap = facts.owner.adopt_fd(700, role=a.Role.T, identity=(1, 700), kind=FDKind.PIDFD)
    facts.owner.bind_parent_pidfd(cap, child=a.Role.C, parent_pid=103, visible_pid=0)
    facts.peers[a.Role.T] = p._Peer(103, 1000, 1000, a.Role.S, cap)
    cursor.accept(value, (0, 0, 0), now_ns=11)


def test_T_receives_C_mapped_credentials_instead_of_host_ids():
    cursor, facts = inert_cursor(a.Role.T)
    prefix(cursor, facts, 17)
    register_peer(facts, a.Role.C)
    value = ordinary_frame(NATIVE[17])
    with pytest.raises(ContractError): cursor.accept(value, (104, 1000, 1000), now_ns=11)
    cursor.accept(value, (104, 0, 0), now_ns=11)


def test_namespace_view_cannot_be_faked_by_role_credential_dictionary():
    cursor, facts = inert_cursor(a.Role.HOST)
    prefix(cursor, facts, 2)
    facts.credentials[a.Role.R] = (999, 1000, 1000)
    with pytest.raises(ContractError): cursor.accept(ordinary_frame(NATIVE[2]), (999, 1000, 1000), now_ns=11)


@pytest.mark.parametrize('mode', ['late', 'lost', 'unparked'])
def test_fixture_converge_requires_current_parked_peer_and_original_cleanup_end(mode):
    cursor, facts = inert_cursor(a.Role.T, p.ProtocolFamily.FIXTURE, created=3)
    prefix(cursor, facts, 14)
    cursor._select_terminal(a.TerminalFamily.T_0, now_ns=12)
    if mode != 'unparked': facts.parked = True
    if mode == 'lost': cursor.mark_uncertain(a.Role.S, a.Role.T)
    value = frame(a.Opcode.CONVERGE_PREUNSHARE, a.Role.S, a.Role.T, 1)
    with pytest.raises(ContractError):
        cursor.accept(value, facts.credentials[a.Role.S], now_ns=5_000_000_012 if mode == 'late' else 13)
    assert cursor.clock.begin_cleanup(5_000_000_013) == 5_000_000_012


def test_U_after_normal_terminal_prefix_uses_remaining_slot_without_replaying_result():
    cursor, facts = inert_cursor(a.Role.R)
    facts.namespace_checked = True
    prefix(cursor, facts, 27)
    facts.exited = facts.reaped = facts.closed = set(facts.peers)
    cursor._select_terminal(a.TerminalFamily.N_C, now_ns=12)
    cursor.accept(terminal_frames('N_C')[2], facts.credentials[a.Role.S], now_ns=13)
    facts.terminal_count = 3  # B2 observes the shared producer counter, not a frame.
    facts.ambiguous = True
    facts.parked, facts.state = True, a.OwnerState.PARKED_UNKNOWN
    cursor._select_terminal(a.TerminalFamily.U, now_ns=14)
    value = frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, 4,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 28, 0, None, None))
    transition = cursor.accept(value, facts.credentials[a.Role.R], now_ns=15)
    assert transition.terminal_slot == 3
    with pytest.raises(ContractError): cursor._select_terminal(a.TerminalFamily.N_C, now_ns=16)


def test_T_reap_ACK_cannot_replace_authenticated_child_cause():
    cursor, facts = inert_cursor(a.Role.S, created=3)
    prefix(cursor, facts, 17)
    facts.exited = facts.reaped = facts.closed = set(facts.peers)
    cursor._select_terminal(a.TerminalFamily.T_0, now_ns=12)
    cause = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    note, ack, result, closed = terminal_frames('T_0', cause)
    cursor.accept(note, facts.credentials[a.Role.T], now_ns=13)
    supply_reap(facts, a.Role.T)
    with pytest.raises(ContractError):
        cursor.accept(replace(ack, payload=replace(ack.payload, cause=None)), facts.credentials[a.Role.S], now_ns=13)
    for record in facts.owner._records.values():
        if record.cap is not facts.endpoints[a.Role.R]: record.attempted = record.closed = True
    cursor.accept(ack, facts.credentials[a.Role.S], now_ns=13)


def test_clock_and_binding_do_not_allow_decoded_transition_to_add_owner_facts():
    cursor, facts = inert_cursor(a.Role.HOST)
    prefix(cursor, facts, 2)
    transition = p.Transition(a.Opcode.PREFIX_CLOSED, None, 7)
    assert not hasattr(transition, 'closed') and not hasattr(transition, 'release')
    assert facts.prefix == 2 and facts.reaped == facts.closed == facts.exited == set()


def test_failed_socket_initialization_retains_exact_wrapper_before_fallible_validation(tmp_path, monkeypatch):
    budget_owner, budget = make_budget(tmp_path)
    cursor, facts = inert_cursor(a.Role.R, created=1)
    sock, remote = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    number = sock.fileno()
    control = LocalControl()
    def fail_identity(*args): raise control
    monkeypatch.setattr(facts.owner, 'bind_identity', fail_identity)
    try:
        with pytest.raises(LocalControl) as caught:
            p.BoundChannel(sock, facts.owner, cursor, budget, a.Role.HOST)
        assert caught.value is control and facts.owner.has_debt
        cap = facts.owner._live[number]
        assert facts.owner._record(cap).group.object is sock
        with pytest.raises(ContractError): facts.owner.close_once(cap)
        assert sock.fileno() == -1
        # fstat/binding failed: closure remains uncertain, no raw retry.
        facts.owner.close_once(cap)
        os.fstat(number)
    finally:
        if sock.fileno() != -1: sock.detach()
        os.close(number)
        remote.close()
        budget_owner.close_all()


def test_channel_does_not_consult_creation_time_peer_credentials(tmp_path, monkeypatch):
    original = socket.socket.getsockopt
    def forbid_peercred(value, level, option, *args):
        if option == socket.SO_PEERCRED: pytest.fail('creation credentials are not message identity')
        return original(value, level, option, *args)
    monkeypatch.setattr(socket.socket, 'getsockopt', forbid_peercred)
    channel, remote, start, budget_owner = local_channel(tmp_path)
    try:
        remote.send(start.to_bytes())
        assert channel.receive() == start
    finally:
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('stage', ['read', 'lock', 'unlock', 'short_write'])
def test_budget_failure_retains_debt_and_original_control(tmp_path, monkeypatch, stage):
    owner, budget = make_budget(tmp_path)
    primary, cleanup = LocalControl(), LocalControl()
    original_lock = p.fcntl.lockf
    def lock(fd, operation, *args):
        if stage == 'lock': raise primary
        value = original_lock(fd, operation, *args)
        if operation == p.fcntl.LOCK_UN: raise cleanup
        return value
    def failed_read(*args): raise primary
    def broken_debt(*args): raise MemoryError()
    monkeypatch.setattr(p.fcntl, 'lockf', lock)
    monkeypatch.setattr(owner, 'poison_debt', broken_debt)
    if stage == 'read': monkeypatch.setattr(os, 'pread', failed_read)
    if stage == 'short_write': monkeypatch.setattr(os, 'pwrite', lambda fd, raw, offset: len(raw) - 1)
    try:
        if stage in ('read', 'lock'):
            with pytest.raises(LocalControl) as caught: budget.charge_ordinary(1)
            assert caught.value is primary
        else:
            with pytest.raises(LocalControl) as caught: budget.charge_ordinary(1)
            assert caught.value is cleanup
        assert owner.has_debt
        assert not any(isinstance(item, BaseException) for item in vars(owner).values())
    finally:
        original_lock(budget.acquisition.fd, p.fcntl.LOCK_UN, budget._STRUCT.size, 0)
        owner.close_all()


def test_budget_initial_write_failure_is_retained_debt(tmp_path, monkeypatch):
    owner = AcquisitionLedger()
    fd = os.open(tmp_path / 'new-shared', os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    info = os.fstat(fd)
    cap = owner.adopt_fd(fd, role=a.Role.HOST, identity=(info.st_dev, info.st_ino))
    control = LocalControl()
    def write(*args): raise control
    monkeypatch.setattr(os, 'pwrite', write)
    try:
        with pytest.raises(LocalControl) as caught: p.ProtocolBudget.create(owner, cap)
        assert caught.value is control
        assert owner.has_debt
    finally:
        owner.close_all()


@pytest.mark.parametrize('count', [0, 1])
def test_U_before_binding_allows_no_R_cause_frame(count):
    cursor, facts = inert_cursor(a.Role.R, created=1)
    prefix(cursor, facts, count)
    cursor._select_terminal(a.TerminalFamily.U, now_ns=12)
    value = frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, 0,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 0, 0, None, None))
    with pytest.raises(ContractError): cursor.accept(value, (101, 1000, 1000), now_ns=13)


def prepaid_C_channel(tmp_path):
    budget_owner, budget = make_budget(tmp_path)
    cursor, facts = inert_cursor(a.Role.C)
    cursor.clock = p.ProtocolClock(time.monotonic_ns())
    facts.endpoints.pop(a.Role.T)
    own, remote = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    channel = p.BoundChannel(own, facts.owner, cursor, budget, a.Role.T)
    facts.owner.bind_control_socket(channel.acquisition, peer=a.Role.T, binding=BINDING)
    facts.namespace_checked = True
    prefix(cursor, facts, 27, now_ns=cursor.clock.started_ns + 1)
    cursor._select_terminal(a.TerminalFamily.N_C, now_ns=cursor.clock.started_ns + 2)
    return channel, remote, budget_owner


def test_prepaid_C_credit_requires_exact_terminal_acquisition_and_source(tmp_path):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    try:
        channel.prepay_c_exit()
        assert channel.budget.snapshot() == (0, 1, 0)
        source = channel.acquisition
        # Inert bookkeeping ONLY: no dup2, FD1 wrapper or real FD1 operation.
        cap = channel.owner.adopt_c_terminal(1, source=source)
        assert channel._installed_terminal(cap).cap is cap
        with pytest.raises(ContractError): channel._terminal_ready(cap)
        clone = type(cap)(cap.fd, cap._owner, cap._generation)
        with pytest.raises(ContractError): channel._terminal_ready(clone)
        with pytest.raises(ContractError): channel.prepay_c_exit()
        impostor = channel.owner.adopt_fd(777, role=a.Role.C, identity=channel.owner._record(cap).identity, kind=FDKind.TERMINAL_SOCKET)
        with pytest.raises(ContractError): channel._terminal_ready(impostor)
        channel.owner._record(cap).binding = (impostor, (a.Role.T, BINDING))
        with pytest.raises(ContractError): channel._terminal_ready(cap)
        assert channel.cursor._sequences[(a.Role.C, a.Role.T)] == 2
    finally:
        close_channel(channel, remote, budget_owner)


def test_prepaid_C_credit_is_refused_after_original_cleanup_end(tmp_path, monkeypatch):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    end = channel.cursor.clock.cleanup_end
    monkeypatch.setattr(p.time, 'monotonic_ns', lambda: end)
    try:
        with pytest.raises(ContractError): channel.prepay_c_exit()
        assert channel.budget.snapshot() == (0, 0, 0)
    finally:
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('prepaid', [False, True])
def test_C_cannot_send_exit_on_original_high_endpoint(tmp_path, prepaid):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    remote.setblocking(False)
    try:
        if prepaid: channel.prepay_c_exit()
        with pytest.raises(ContractError): channel.send(terminal_frames('N_C')[0])
        with pytest.raises(BlockingIOError): remote.recv(161)
    finally:
        close_channel(channel, remote, budget_owner)


def test_peer_registration_cannot_borrow_another_roles_pidfd():
    cursor, facts = inert_cursor(a.Role.HOST)
    prefix(cursor, facts, 2)
    wrong_pidfd = register_peer(facts, a.Role.T).pidfd
    facts.peers[a.Role.R] = replace(facts.peers[a.Role.R], pidfd=wrong_pidfd)
    with pytest.raises(ContractError): cursor.accept(ordinary_frame(NATIVE[2]), (101, 1000, 1000), now_ns=11)


def test_successful_local_send_receive_charges_only_producer_and_commits_sequences(tmp_path):
    sender, sock, start, budget_owner = local_channel(tmp_path, role=a.Role.HOST)
    cursor, facts = inert_cursor(a.Role.R, created=1)
    facts.endpoints.pop(a.Role.HOST)
    facts.credentials[a.Role.HOST] = (os.getpid(), os.getuid(), os.getgid())
    facts.owner._record(facts.owner._parents[a.Role.R]).binding = (a.Role.R, os.getpid(), os.getpid())
    cursor.clock = sender.cursor.clock
    receiver = p.BoundChannel(sock, facts.owner, cursor, sender.budget, a.Role.HOST)
    try:
        sender.send(start)
        assert receiver.receive() == start
        assert sender.budget.snapshot() == (192, 0, 0)
        groups = frame(a.Opcode.GROUPS, a.Role.HOST, a.Role.R, 1, a.GroupsPayload(()))
        sender.send(groups)
        assert receiver.receive() == groups
        assert sender.budget.snapshot() == (384, 0, 0)
        assert sender.cursor._sequences[(a.Role.HOST, a.Role.R)] == 2
        assert cursor._sequences[(a.Role.HOST, a.Role.R)] == 2
    finally:
        sender.close()
        receiver.close()
        budget_owner.close_all()


def test_receive_after_deadline_refuses_without_waiting_or_closing_owner(tmp_path):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    channel.cursor.clock.deadline_ns = time.monotonic_ns() - 1
    try:
        with pytest.raises(ContractError): channel.receive()
        assert channel.owner.cause.kind is a.FailureKind.PROTOCOL_FAILURE
        assert not channel.owner._record(channel.acquisition).closed
        assert channel.cursor._facts.ambiguous
    finally:
        close_channel(channel, remote, budget_owner)


def test_native_grant_refuses_a_prematurely_existing_C():
    cursor, facts = inert_cursor(a.Role.HOST)
    prefix(cursor, facts, 14)
    register_peer(facts, a.Role.C)
    with pytest.raises(ContractError):
        cursor.accept(ordinary_frame(NATIVE[14]), (100, 1000, 1000), now_ns=11)


def test_independent_channel_views_share_one_absolute_cleanup_end(tmp_path):
    sender, sock, start, budget_owner = local_channel(tmp_path, role=a.Role.HOST)
    cursor, facts = inert_cursor(a.Role.R, created=1)
    facts.endpoints.pop(a.Role.HOST)
    cursor.clock = p.ProtocolClock(sender.cursor.clock.started_ns)
    other_budget = p.ProtocolBudget.attach(budget_owner, sender.budget.acquisition)
    receiver = p.BoundChannel(sock, facts.owner, cursor, other_budget, a.Role.HOST)
    try:
        started = sender.cursor.clock.started_ns
        assert sender.cursor.clock.begin_cleanup(started + 1) == started + 5_000_000_001
        assert cursor.clock.begin_cleanup(started + 1_000_000_001) == started + 5_000_000_001
        assert sender.cursor.clock.cleanup_end == cursor.clock.cleanup_end
        with pytest.raises(ContractError): cursor.clock.check(started + 5_000_000_001, terminal=True)
    finally:
        sender.close()
        receiver.close()
        budget_owner.close_all()


def test_global_ordinary_exhaustion_refuses_a_native_grant(tmp_path):
    owner, budget = make_budget(tmp_path)
    cursor, facts = inert_cursor(a.Role.HOST, created=3)
    cursor.clock._bind_budget(budget)
    prefix(cursor, facts, 14)
    assert not budget.charge_ordinary(14337)
    try:
        with pytest.raises(ContractError):
            cursor.accept(ordinary_frame(NATIVE[14]), (100, 1000, 1000), now_ns=11)
    finally:
        owner.close_all()


@pytest.mark.parametrize('fault', ['unpack', 'iteration'])
def test_received_rights_parse_control_closes_known_caps_and_retains_opaque_debt(tmp_path, monkeypatch, fault):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    delivered = [os.dup(remote.fileno()), os.dup(remote.fileno())]
    control = LocalControl()
    original_unpack = struct.unpack
    rights = (socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('@ii', *delivered))
    if fault == 'unpack':
        def broken_unpack(format, raw):
            if format == '@i' and raw == struct.pack('@i', delivered[1]): raise control
            return original_unpack(format, raw)
        monkeypatch.setattr(struct, 'unpack', broken_unpack)
        ancillary = [rights]
    else:
        class InterruptedMessages:
            def __iter__(self):
                yield (socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('@i', delivered[0]))
                raise control
        ancillary = InterruptedMessages()
    monkeypatch.setattr(socket.socket, 'recvmsg', lambda *args: (start.to_bytes(), ancillary, 0, None))
    remote.send(b'ready')
    try:
        with pytest.raises(LocalControl) as caught: channel.receive()
        assert caught.value is control
        assert channel.owner.has_debt and not channel.owner.all_closed
        assert channel.owner._record(channel.owner._live[delivered[0]]).closed
        with pytest.raises(OSError): os.fstat(delivered[0])
        assert delivered[1] not in channel.owner._live
        os.fstat(delivered[1])  # Unknown census is retained as opaque debt.
    finally:
        for fd in delivered:
            try: os.close(fd)
            except OSError: pass
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('body_control', [False, True])
def test_mark_uncertain_bookkeeping_cannot_replace_selected_control(tmp_path, monkeypatch, body_control):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    primary = LocalControl() if body_control else OSError(5, 'inert')
    cleanup = LocalControl()
    def receive(*args): raise primary
    def mark(*args): raise cleanup
    monkeypatch.setattr(socket.socket, 'recvmsg', receive)
    monkeypatch.setattr(channel.cursor, 'mark_uncertain', mark)
    remote.send(b'ready')
    try:
        with pytest.raises(LocalControl) as caught: channel.receive()
        assert caught.value is (primary if body_control else cleanup)
        assert channel.owner.has_debt
        assert channel.cursor._facts.ambiguous
        assert not any(isinstance(value, BaseException) for value in vars(channel.owner).values())
    finally:
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('blocked', ['parked', 'cause', 'cleanup', 'shared_cleanup'])
def test_review5_native_grant_requires_eligible_running_owner(tmp_path, blocked):
    owner, budget = make_budget(tmp_path)
    cursor, facts = inert_cursor(a.Role.HOST, created=3)
    cursor.clock._bind_budget(budget)
    prefix(cursor, facts, 14)
    if blocked == 'parked': facts.parked = True
    if blocked == 'cause': facts.owner.record_cause(a.FailureRecord(a.Role.HOST, a.NativePhase.CONTROL_IO, a.FailureKind.PROTOCOL_FAILURE, None))
    if blocked == 'cleanup': facts.owner.record_cleanup(a.Role.HOST)
    if blocked == 'shared_cleanup': cursor.clock.begin_cleanup(12)
    try:
        with pytest.raises(ContractError): cursor.accept(ordinary_frame(NATIVE[14]), (100, 1000, 1000), now_ns=13)
    finally:
        owner.close_all()


def test_review6_send_rechecks_original_cutoff_after_charging(tmp_path, monkeypatch):
    channel, remote, start, budget_owner = local_channel(tmp_path, role=a.Role.HOST)
    original = channel.budget.charge_ordinary
    def charge(size):
        result = original(size)
        monkeypatch.setattr(p.time, 'monotonic_ns', lambda: channel.cursor.clock.deadline_ns)
        return result
    monkeypatch.setattr(channel.budget, 'charge_ordinary', charge)
    remote.setblocking(False)
    try:
        with pytest.raises(ContractError): channel.send(start)
        assert channel.budget.snapshot()[0] == 192
        with pytest.raises(BlockingIOError): remote.recv(161)
    finally:
        close_channel(channel, remote, budget_owner)


def test_review7_direct_exit_note_cannot_assign_another_actor_cause():
    cursor, facts = inert_cursor(a.Role.R, created=2)
    prefix(cursor, facts, 8)
    cursor._select_terminal(a.TerminalFamily.S_0, now_ns=12)
    cause = a.FailureRecord(a.Role.R, a.NativePhase.SET_SUBREAPER, a.FailureKind.NATIVE_REJECTION, 1)
    note = terminal_frames('S_0', cause)[0]
    with pytest.raises(ContractError): cursor.accept(note, (102, 1000, 1000), now_ns=13)


def test_review7_result_parked_requires_actual_parked_state():
    cursor, facts = inert_cursor(a.Role.R, created=1)
    prefix(cursor, facts, 2)
    cursor._select_terminal(a.TerminalFamily.U, now_ns=12)
    value = frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, 0,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.RUNNING, 0, 0, None, None))
    with pytest.raises(ContractError): cursor.accept(value, (101, 1000, 1000), now_ns=13)


@pytest.mark.parametrize('erase', ['cause', 'cleanup'])
def test_review7_U_preserves_authenticated_first_diagnostics(erase):
    cursor, facts = inert_cursor(a.Role.R, created=3)
    prefix(cursor, facts, 17)
    facts.exited = {a.Role.T}
    cursor._select_terminal(a.TerminalFamily.T_0, now_ns=12)
    cause = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    cleanup = a.FailureRecord(a.Role.T, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None)
    ack = replace(terminal_frames('T_0', cause)[1], payload=replace(exit_payload(3, cause), cleanup=cleanup))
    cursor.accept(ack, (102, 1000, 1000), now_ns=13)
    facts.terminal_count = 2
    facts.parked = True
    facts.state = a.OwnerState.PARKED_UNKNOWN
    cursor._select_terminal(a.TerminalFamily.U, now_ns=14)
    value = frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, 3,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 12, 0,
                        None if erase == 'cause' else cause, None if erase == 'cleanup' else cleanup))
    with pytest.raises(ContractError): cursor.accept(value, (101, 1000, 1000), now_ns=15)


def test_review8_R0_live_nonterminal_file_blocks_closed_report(tmp_path):
    cursor, facts = inert_cursor(a.Role.R, created=1)
    prefix(cursor, facts, 2)
    fd = os.open(tmp_path / 'attempt-file', os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    info = os.fstat(fd)
    cap = facts.owner.adopt_fd(fd, role=a.Role.R, identity=(info.st_dev, info.st_ino))
    # The only live synthetic resource permitted at R's exit tail is H control.
    for record in facts.owner._records.values():
        if record.cap not in (cap, facts.endpoints[a.Role.HOST]): record.attempted = record.closed = True
    cause = a.FailureRecord(a.Role.R, a.NativePhase.CONTROL_CODEC, a.FailureKind.PROTOCOL_FAILURE, None)
    facts.owner.record_cause(cause)
    cursor._select_terminal(a.TerminalFamily.R_0, now_ns=12)
    value = terminal_frames('R_0', cause)[0]
    try:
        with pytest.raises(ContractError): cursor.accept(value, (101, 1000, 1000), now_ns=13)
    finally:
        facts.owner.close_once(cap)


def test_review9_actual_socket_must_equal_this_peers_fact_acquisition(tmp_path):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    facts = channel.cursor._facts
    facts.endpoints[a.Role.HOST], facts.endpoints[a.Role.S] = facts.endpoints[a.Role.S], facts.endpoints[a.Role.HOST]
    remote.send(start.to_bytes())
    try:
        with pytest.raises(ContractError): channel.receive()
    finally:
        close_channel(channel, remote, budget_owner)


def test_review9_host_bootstrap_requires_already_registered_R():
    cursor, facts = inert_cursor(a.Role.HOST, created=1)
    facts.peers.clear()
    with pytest.raises(ContractError): cursor.accept(ordinary_frame(NATIVE[0]), (100, 1000, 1000), now_ns=11)


def test_review9_R_bootstrap_requires_exact_trusted_parent_provenance():
    cursor, facts = inert_cursor(a.Role.R, created=1)
    facts.owner._parents.clear()
    with pytest.raises(ContractError): cursor.accept(ordinary_frame(NATIVE[0]), (100, 1000, 1000), now_ns=11)


def test_review11_socket_close_actor_lookup_cannot_replace_control(monkeypatch):
    owner = AcquisitionLedger()
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    cap = delegated_socket(owner, left, a.Role.T)
    control = LocalControl()
    original_close, original_record = socket.socket.close, owner._record
    closing = False
    def close(value):
        nonlocal closing
        if value is left:
            closing = True
            raise control
        return original_close(value)
    def record(value):
        if closing: raise MemoryError()
        return original_record(value)
    monkeypatch.setattr(socket.socket, 'close', close)
    monkeypatch.setattr(owner, '_record', record)
    try:
        with pytest.raises(LocalControl) as caught: owner.close_once(cap)
        assert caught.value is control and owner.has_debt
        assert owner.cleanup.role is a.Role.T
    finally:
        if left.fileno() != -1: left.detach()
        os.close(cap.fd)
        original_close(right)


@pytest.mark.parametrize('role,count,index,subject', [(a.Role.HOST, 5, 5, a.Role.S),
    (a.Role.R, 9, 9, a.Role.T), (a.Role.S, 18, 18, a.Role.C)])
def test_review2_authenticated_identity_precedes_independent_registration(role, count, index, subject):
    cursor, facts = inert_cursor(role)
    prefix(cursor, facts, count)
    assert subject not in facts.peers
    value = ordinary_frame(NATIVE[index])
    transition = cursor.accept(value, message_credentials(facts, value), now_ns=11)
    assert transition.pending_identity == value.payload
    assert subject not in facts.peers
    onward = ordinary_frame(NATIVE[index + 1])
    with pytest.raises(ContractError): cursor.accept(onward, message_credentials(facts, onward), now_ns=11)
    register_peer(facts, subject)
    cursor.accept(onward, message_credentials(facts, onward), now_ns=11)


@pytest.mark.parametrize('opcode', [a.Opcode.RESULT_CLOSED, a.Opcode.RESULT_PARKED])
def test_review3_H_receives_early_terminal_without_remote_family_oracle(opcode):
    cursor, facts = inert_cursor(a.Role.HOST, created=1)
    prefix(cursor, facts, 2)
    value = terminal_frames('R_0')[0] if opcode is a.Opcode.RESULT_CLOSED else frame(opcode, a.Role.R, a.Role.HOST, 0,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 0, 0, None, None))
    assert cursor._terminal is None
    cursor.accept(value, (101, 1000, 1000), now_ns=13)
    assert facts.reaped == facts.closed == set()


@pytest.mark.parametrize('credentials,accepted', [((102, 0, 0), True), ((102, 1000, 1000), False)])
def test_review4_post_map_ACK_C_has_receiver_relative_mapped_ids(credentials, accepted):
    cursor, facts = inert_cursor(a.Role.T)
    facts.namespace_checked = True
    prefix(cursor, facts, 23)
    facts.maps_verified = True
    value = ordinary_frame(NATIVE[23])
    if accepted: cursor.accept(value, credentials, now_ns=11)
    else:
        with pytest.raises(ContractError): cursor.accept(value, credentials, now_ns=11)


@pytest.mark.parametrize('fault', ['adoption_then_iteration', 'record_sweep'])
def test_review10_census_preserves_first_control_and_attempts_known_closes(tmp_path, monkeypatch, fault):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    delivered = [os.dup(remote.fileno()), os.dup(remote.fileno())]
    first, later = LocalControl(), LocalControl()
    original_adopt = channel.owner.adopt_fd
    if fault == 'adoption_then_iteration':
        def adopt(fd, **kwargs):
            cap = original_adopt(fd, **kwargs)
            raise first
        monkeypatch.setattr(channel.owner, 'adopt_fd', adopt)
        class Messages:
            def __iter__(self):
                yield (socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('@i', delivered[0]))
                raise later
        ancillary = Messages()
    else:
        ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('@ii', *delivered))]
        generation = channel.owner._generation
        class InterruptedRecords(dict):
            def values(self):
                for record in super().values():
                    yield record
                    if record.cap._generation > generation: raise later
        channel.owner._records = InterruptedRecords(channel.owner._records)
    monkeypatch.setattr(socket.socket, 'recvmsg', lambda *args: (start.to_bytes(), ancillary, 0, None))
    remote.send(b'ready')
    try:
        with pytest.raises(LocalControl) as caught: channel.receive()
        assert caught.value is (first if fault == 'adoption_then_iteration' else later)
        assert channel.owner.has_debt
        expected_closed = delivered[:1] if fault == 'adoption_then_iteration' else delivered
        for fd in expected_closed:
            with pytest.raises(OSError): os.fstat(fd)
    finally:
        for fd in delivered:
            try: os.close(fd)
            except OSError: pass
        close_channel(channel, remote, budget_owner)


def install_inert_FD1(channel, monkeypatch, *, verified):
    """Real owned duplicate as transport; FD1 calls are inert doubles only."""
    source = channel.acquisition
    source_wrapper = channel._socket
    source_info = os.fstat(source.fd)
    terminal = socket.socket(fileno=os.dup(source.fd))
    raw_terminal = terminal.fileno()
    cap = channel.owner.adopt_c_terminal(1, source=source)
    channel.owner._delegate(terminal, (cap,))
    original_fileno, original_fstat = socket.socket.fileno, os.fstat
    def fileno(value):
        if value is terminal: return 1 if original_fileno(value) >= 0 else -1
        return original_fileno(value)
    def fstat(fd):
        if fd == 1:
            if original_fileno(terminal) >= 0: return source_info
            raise OSError(9, 'inert FD1')
        return original_fstat(fd)
    monkeypatch.setattr(socket.socket, 'fileno', fileno)
    monkeypatch.setattr(os, 'fstat', fstat)
    class InertTerminalPoll:
        def register(self, fd, events):
            assert fd == 1 and events == p.select.POLLOUT
        def poll(self, timeout):
            assert type(timeout) is int and 0 < timeout <= 5000
            return [(1, p.select.POLLOUT)]
    monkeypatch.setattr(p.select, 'poll', InertTerminalPoll)
    channel._use_terminal(terminal, cap)
    # Inert B2 transfer: detach first, then close only the owned raw old number.
    raw_source = source_wrapper.detach()
    os.close(raw_source)
    channel.owner._record(source).group = None
    channel.owner._record(source).attempted = channel.owner._record(source).closed = True
    if verified:
        for record in channel.owner._records.values():
            if record.cap.fd >= 3: record.attempted = record.closed = True
        channel.budget._owner.close_once(channel.budget.acquisition)
    return cap, raw_terminal


def test_review1_C_terminal_installation_is_not_verified_bulk_closure(tmp_path, monkeypatch):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    channel.prepay_c_exit()
    try:
        cap, raw_terminal = install_inert_FD1(channel, monkeypatch, verified=False)
        with pytest.raises(ContractError): channel._terminal_ready(cap)
    finally:
        close_channel(channel, remote, budget_owner)


def test_review1_C_prepaid_send_survives_closed_pidfds_and_budget_without_high_access(tmp_path, monkeypatch):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    channel.prepay_c_exit()
    note = terminal_frames('N_C')[0]
    try:
        install_inert_FD1(channel, monkeypatch, verified=True)
        def forbidden(*args, **kwargs): pytest.fail('prepaid send touched closed high-FD context')
        monkeypatch.setattr(channel.budget, '_fd', forbidden)
        monkeypatch.setattr(channel.cursor._facts, 'identity', forbidden)
        channel.send(note)
        assert remote.recv(161) == note.to_bytes()
        with pytest.raises(ContractError): channel.send(note)
    finally:
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('role', [a.Role.HOST, a.Role.R])
def test_review9_bootstrap_ids_match_checked_host_credentials(role):
    cursor, facts = inert_cursor(role, created=1)
    start = ordinary_frame(NATIVE[0])
    bad = replace(start, payload=replace(start.payload, uid=2000))
    with pytest.raises(ContractError): cursor.accept(bad, (100, 1000, 1000), now_ns=11)


def test_review9_unbound_R_pidfd_identity_is_not_registration():
    cursor, facts = inert_cursor(a.Role.HOST, created=1)
    facts.owner._record(facts.peers[a.Role.R].pidfd).identity = None
    with pytest.raises(ContractError): cursor.accept(ordinary_frame(NATIVE[0]), (100, 1000, 1000), now_ns=11)


def test_review3_cleanup_start_during_poll_allows_legal_remote_abort(tmp_path, monkeypatch):
    channel, remote, start, budget_owner = local_channel(tmp_path, role=a.Role.HOST)
    cursor, facts = channel.cursor, channel.cursor._facts
    facts.peers[a.Role.R] = replace(facts.peers[a.Role.R], pid=os.getpid())
    prefix(cursor, facts, 2, now_ns=cursor.clock.started_ns + 1)
    value = terminal_frames('R_0')[0]
    remote.send(value.to_bytes())
    channel.budget.claim_terminal(0)
    end = cursor.clock.deadline_ns
    now = iter([end - 2, end + 1])
    monkeypatch.setattr(p.time, 'monotonic_ns', lambda: next(now, end + 1))
    class Poll:
        def register(self, fd, events): assert fd == channel.acquisition.fd
        def poll(self, timeout):
            cursor.clock.begin_cleanup(end - 1)
            return [(channel.acquisition.fd, p.select.POLLIN)]
    monkeypatch.setattr(p.select, 'poll', Poll)
    try:
        assert cursor._terminal is None
        assert channel.receive() == value
        assert cursor.clock.cleanup_end == end + 4_999_999_999
        assert facts.reaped == facts.closed == set()
    finally:
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('fault', ['expired', 'sequence', 'binding', 'pid'])
def test_review1_frozen_C_context_refuses_changes_and_never_retries(tmp_path, monkeypatch, fault):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    channel.prepay_c_exit()
    end = channel.cursor.clock.cleanup_end
    note = terminal_frames('N_C')[0]
    try:
        install_inert_FD1(channel, monkeypatch, verified=True)
        remote.setblocking(False)
        if fault == 'expired': monkeypatch.setattr(p.time, 'monotonic_ns', lambda: end)
        if fault == 'sequence': note = replace(note, sequence=3)
        if fault == 'binding': note = replace(note, binding=replace(BINDING, token='ff' * 16))
        if fault == 'pid': note = replace(note, payload=replace(note.payload, pid=999))
        with pytest.raises(ContractError): channel.send(note)
        with pytest.raises(ContractError): channel.send(terminal_frames('N_C')[0])
        with pytest.raises(BlockingIOError): remote.recv(161)
    finally:
        close_channel(channel, remote, budget_owner)


def test_review10_post_recv_accounting_fault_retains_opaque_debt(tmp_path, monkeypatch):
    channel, remote, start, budget_owner = local_channel(tmp_path)
    fd = os.dup(remote.fileno())
    ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack('@i', fd))]
    control = LocalControl()
    def before_census(*args): raise control
    monkeypatch.setattr(socket.socket, 'recvmsg', lambda *args: (start.to_bytes(), ancillary, 0, None))
    monkeypatch.setattr(channel, '_close_delivered_rights', before_census)
    remote.send(b'ready')
    try:
        with pytest.raises(LocalControl) as caught: channel.receive()
        assert caught.value is control
        assert channel.owner.has_debt
        assert fd not in channel.owner._live
        os.fstat(fd)
    finally:
        os.close(fd)
        close_channel(channel, remote, budget_owner)


def test_review7_direct_ACK_uses_actual_wait_status_without_replacing_child_cause():
    cursor, facts = inert_cursor(a.Role.S, created=3)
    prefix(cursor, facts, 17)
    cursor._select_terminal(a.TerminalFamily.T_0, now_ns=12)
    cause = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    note, ack, _, _ = terminal_frames('T_0', cause)
    cursor.accept(note, (103, 1000, 1000), now_ns=13)
    facts.exited = facts.reaped = facts.closed = {a.Role.T}
    for record in facts.owner._records.values():
        if record.cap is not facts.endpoints[a.Role.R]: record.attempted = record.closed = True
    actual = replace(ack, payload=replace(ack.payload, exit_kind=a.ExitKind.SIGNALED, status=9, closed_fds=7))
    supply_reap(facts, a.Role.T, kind=a.ExitKind.SIGNALED, status=9, closed=7)
    cursor.accept(actual, (102, 1000, 1000), now_ns=13)
    assert cursor._claims[a.Role.T].exit_kind is a.ExitKind.SIGNALED
    assert cursor._claims[a.Role.T].closed_fds == 7
    assert cursor._cause == cause


def test_review7_direct_EXIT_must_use_actual_local_finite_origin():
    cursor, facts = inert_cursor(a.Role.T, created=3)
    prefix(cursor, facts, 17)
    cursor._select_terminal(a.TerminalFamily.T_0, now_ns=12)
    for record in facts.owner._records.values():
        if record.cap is not facts.endpoints[a.Role.S]: record.attempted = record.closed = True
    invented = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    with pytest.raises(ContractError): cursor.accept(terminal_frames('T_0', invented)[0], (103, 1000, 1000), now_ns=13)


def test_review2_ACK_registration_must_match_discovered_outer_ids():
    cursor, facts = inert_cursor(a.Role.HOST)
    prefix(cursor, facts, 5)
    cursor.accept(ordinary_frame(NATIVE[5]), (101, 1000, 1000), now_ns=11)
    registered = register_peer(facts, a.Role.S)
    facts.peers[a.Role.S] = replace(registered, uid=2000)
    with pytest.raises(ContractError): cursor.accept(ordinary_frame(NATIVE[6]), (100, 1000, 1000), now_ns=11)


def test_review1_prepay_cannot_replace_authenticated_outer_self_identity(tmp_path):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    channel.cursor._facts.local = replace(channel.cursor._facts.local, pid=999)
    try:
        with pytest.raises(ContractError): channel.prepay_c_exit()
        assert channel.budget.snapshot()[1] == 0
    finally:
        close_channel(channel, remote, budget_owner)


def test_round2_A_C_readiness_never_constructs_a_real_poll(tmp_path, monkeypatch):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    channel.prepay_c_exit()
    def real_poll_forbidden(*args): pytest.fail('real FD1 readiness must never be polled')
    monkeypatch.setattr(p.select, 'poll', real_poll_forbidden)
    try:
        install_inert_FD1(channel, monkeypatch, verified=True)
        channel.send(terminal_frames('N_C')[0])
        assert remote.recv(161) == terminal_frames('N_C')[0].to_bytes()
    finally:
        close_channel(channel, remote, budget_owner)


def inert_H_sender(tmp_path, family=p.ProtocolFamily.NATIVE):
    channel, remote, start, budget_owner = local_channel(tmp_path, role=a.Role.HOST, family=family)
    facts = channel.cursor._facts
    facts.local = p._LocalIdentity(a.Role.HOST, 100, 1000, 1000)
    facts.credentials = {role: (100 + int(role), 1000, 1000) for role in a.Role}
    prefix(channel.cursor, facts, 14, now_ns=channel.cursor.clock.started_ns + 1)
    return channel, remote, budget_owner


def test_round2_5_cleanup_started_during_charge_vetoes_native_grant(tmp_path, monkeypatch):
    channel, remote, budget_owner = inert_H_sender(tmp_path)
    original = channel.budget.charge_ordinary
    def charge(size):
        result = original(size)
        channel.cursor.clock.begin_cleanup(time.monotonic_ns())
        return result
    monkeypatch.setattr(channel.budget, 'charge_ordinary', charge)
    remote.setblocking(False)
    try:
        with pytest.raises(ContractError): channel.send(ordinary_frame(NATIVE[14]))
        assert channel.budget.snapshot()[0] == 192
        with pytest.raises(BlockingIOError): remote.recv(161)
    finally:
        close_channel(channel, remote, budget_owner)


def test_round2_6_terminal_end_is_resolved_before_final_time_sample(tmp_path, monkeypatch):
    channel, remote, budget_owner = inert_H_sender(tmp_path, p.ProtocolFamily.FIXTURE)
    cursor, facts = channel.cursor, channel.cursor._facts
    parked = frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, 3,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 12, 0, None, None))
    channel.budget.claim_terminal(0)
    cursor.accept(parked, (101, 1000, 1000), now_ns=cursor.clock.started_ns + 2)
    facts.parked, facts.state = True, a.OwnerState.PARKED_UNKNOWN
    end = cursor.clock.cleanup_end
    now = [end - 1]
    encoded = [False]
    original_end, original_encode = channel.budget._cleanup_end, a.Frame.to_bytes
    def resolve_end():
        value = original_end()
        if encoded[0]: now[0] = end
        return value
    def encode(value):
        raw = original_encode(value)
        if value.opcode is a.Opcode.CONVERGE_PREUNSHARE: encoded[0] = True
        return raw
    monkeypatch.setattr(channel.budget, '_cleanup_end', resolve_end)
    monkeypatch.setattr(a.Frame, 'to_bytes', encode)
    monkeypatch.setattr(p.time, 'monotonic_ns', lambda: now[0])
    remote.setblocking(False)
    try:
        with pytest.raises(ContractError): channel.send(frame(a.Opcode.CONVERGE_PREUNSHARE, a.Role.HOST, a.Role.R, 5))
        assert encoded[0] and now[0] == end
        assert channel.budget.snapshot()[1] == 3
        with pytest.raises(BlockingIOError): remote.recv(161)
    finally:
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('order', ['remote_first', 'local_first'])
@pytest.mark.parametrize('field', ['cause', 'cleanup'])
def test_round2_7_first_established_diagnostic_survives_later_other_source(order, field):
    cursor, facts = inert_cursor(a.Role.R, created=3)
    prefix(cursor, facts, 17)
    remote_record = (a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
                     if field == 'cause' else a.FailureRecord(a.Role.T, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None))
    local_record = (a.FailureRecord(a.Role.R, a.NativePhase.CONTROL_IO, a.FailureKind.PROTOCOL_FAILURE, None)
                    if field == 'cause' else a.FailureRecord(a.Role.R, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None))
    def establish_local():
        if field == 'cause': facts.owner.record_cause(local_record)
        else: facts.owner.record_cleanup(a.Role.R)
    if order == 'local_first': establish_local()
    ack = terminal_frames('T_0')[1]
    ack = replace(ack, payload=replace(ack.payload, **{field: remote_record}))
    cursor.accept(ack, (102, 1000, 1000), now_ns=13)
    if order == 'remote_first': establish_local()
    facts.terminal_count = 2
    facts.parked, facts.state = True, a.OwnerState.PARKED_UNKNOWN
    cursor._select_terminal(a.TerminalFamily.U, now_ns=14)
    first = remote_record if order == 'remote_first' else local_record
    value = frame(a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST, 3,
        a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 12, 0,
                        first if field == 'cause' else None, first if field == 'cleanup' else None))
    cursor.accept(value, (101, 1000, 1000), now_ns=15)
    assert getattr(value.payload, field) == first


@pytest.mark.parametrize('which', ['result', 'prefix'])
def test_round2_B_remote_abort_cannot_hide_local_created_unregistered_child(which):
    cursor, facts = inert_cursor(a.Role.R, created=2)
    prefix(cursor, facts, 8)
    cursor.accept(terminal_frames('S_0')[0], (102, 1000, 1000), now_ns=13)
    facts.exited = facts.reaped = facts.closed = {a.Role.S}
    facts.state = a.OwnerState.CLEANING
    for record in facts.owner._records.values():
        if record.cap is not facts.endpoints[a.Role.HOST]: record.attempted = record.closed = True
    cause = closed_payload(4).cause
    facts.owner.record_cause(cause)
    result, closed = terminal_frames('S_0')[1:]
    if which == 'prefix': cursor.accept(result, (101, 1000, 1000), now_ns=14)
    facts.created = {a.Role.T}
    with pytest.raises(ContractError): cursor.accept(result if which == 'result' else closed, (101, 1000, 1000), now_ns=15)


@pytest.mark.parametrize('uncertainty', ['edge', 'owner'])
def test_round2_C_frozen_exit_rechecks_uncertainty_after_prepay(tmp_path, monkeypatch, uncertainty):
    channel, remote, budget_owner = prepaid_C_channel(tmp_path)
    channel.prepay_c_exit()
    try:
        install_inert_FD1(channel, monkeypatch, verified=True)
        if uncertainty == 'edge': channel.cursor.mark_uncertain(a.Role.C, a.Role.T)
        else: channel.cursor._facts.ambiguous = True
        remote.setblocking(False)
        with pytest.raises(ContractError): channel.send(terminal_frames('N_C')[0])
        with pytest.raises(BlockingIOError): remote.recv(161)
    finally:
        close_channel(channel, remote, budget_owner)


@pytest.mark.parametrize('bad', ['missing', 'kind', 'status', 'mask', 'cap'])
def test_round2_D_direct_ACK_binds_exact_independent_wait_and_closure(bad):
    cursor, facts = inert_cursor(a.Role.S, created=3)
    prefix(cursor, facts, 17)
    note, ack, _, _ = terminal_frames('T_0')
    cursor.accept(note, (103, 1000, 1000), now_ns=13)
    facts.exited = facts.reaped = facts.closed = {a.Role.T}
    for record in facts.owner._records.values():
        if record.cap is not facts.endpoints[a.Role.R]: record.attempted = record.closed = True
    target = facts.peers[a.Role.T].pidfd
    observation = (target, 103, a.ExitKind.EXITED, 0, 0)
    facts.targeted_reaps = {} if bad == 'missing' else {a.Role.T: observation}
    if bad == 'kind': ack = replace(ack, payload=replace(ack.payload, exit_kind=a.ExitKind.SIGNALED))
    if bad == 'status': ack = replace(ack, payload=replace(ack.payload, status=7))
    if bad == 'mask': ack = replace(ack, payload=replace(ack.payload, closed_fds=1))
    if bad == 'cap':
        clone = type(target)(target.fd, target._owner, target._generation)
        facts.targeted_reaps[a.Role.T] = (clone, 103, a.ExitKind.EXITED, 0, 0)
    with pytest.raises(ContractError): cursor.accept(ack, (102, 1000, 1000), now_ns=14)


@pytest.mark.parametrize('changed', ['parked', 'cause'])
def test_round2_5_local_eligibility_is_read_after_final_shared_preparation(tmp_path, monkeypatch, changed):
    channel, remote, budget_owner = inert_H_sender(tmp_path)
    prepared = [False]
    original_update, original_encode = channel.budget._update, a.Frame.to_bytes
    def encode(value):
        raw = original_encode(value)
        if value.opcode is a.Opcode.GO_NATIVE: prepared[0] = True
        return raw
    def update(*args, **kwargs):
        state = original_update(*args, **kwargs)
        if prepared[0]:
            if changed == 'parked': channel.cursor._facts.parked = True
            else: channel.owner.record_cause(a.FailureRecord(a.Role.HOST, a.NativePhase.CONTROL_IO, a.FailureKind.PROTOCOL_FAILURE, None))
        return state
    monkeypatch.setattr(a.Frame, 'to_bytes', encode)
    monkeypatch.setattr(channel.budget, '_update', update)
    remote.setblocking(False)
    try:
        with pytest.raises(ContractError): channel.send(ordinary_frame(NATIVE[14]))
        assert channel.budget.snapshot()[0] == 192
        with pytest.raises(BlockingIOError): remote.recv(161)
    finally:
        close_channel(channel, remote, budget_owner)
