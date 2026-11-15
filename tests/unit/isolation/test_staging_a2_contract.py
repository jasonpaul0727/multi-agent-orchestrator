"""Finite v2 observations are strict values, never release authority."""
from dataclasses import replace
import importlib
import json
import pytest
from test_staging_host_root import deny_process_controls


def mod():
    return importlib.import_module('orchestrator.isolation._staging_a2_contract')


def test_failure_codec_rejects_extra_unknown_and_bool_errno():
    a = mod()
    record = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    assert json.loads(record.to_bytes()) == {'role': 'T', 'phase': 'UNSHARE', 'kind': 'NATIVE_REJECTION', 'errno': 1}
    assert a.FailureRecord.from_bytes(record.to_bytes()) == record
    for change in ({'errno': True}, {'errno': 0}, {'errno': 4096}, {'role': 'W'}, {'kind': 'UNSUPPORTED'}, {'path': '/proc'}):
        data = json.loads(record.to_bytes()) | change
        with pytest.raises(a.ContractError):
            a.FailureRecord.from_bytes(json.dumps(data).encode())
    with pytest.raises(a.ContractError):
        a.FailureRecord.from_bytes(b'{"role":"T","role":"T","phase":"UNSHARE","kind":"NATIVE_REJECTION","errno":1}')


def test_closed_contract_failure_is_failed_cleanup_unknown_wins_without_losing_cause():
    a = mod()
    cause = a.FailureRecord(a.Role.T, a.NativePhase.VERIFY_MAPS, a.FailureKind.CONTRACT_FAILURE, None)
    cleanup = a.FailureRecord(a.Role.T, a.NativePhase.FD_CLOSE, a.FailureKind.CLEANUP_UNKNOWN, None)
    assert a.derive_status(cause, None, a.OwnerState.CLOSED, ()) is a.FixtureStatus.FAILED
    assert a.derive_status(cause, cleanup, a.OwnerState.PARKED_UNKNOWN, ()) is a.FixtureStatus.UNKNOWN
    refusal = replace(cause, phase=a.NativePhase.UNSHARE, kind=a.FailureKind.NATIVE_REJECTION, errno=1)
    assert a.derive_status(refusal, None, a.OwnerState.CLOSED, ()) is a.FixtureStatus.UNSUPPORTED
    with pytest.raises(a.ContractError):
        replace(cause, errno=1)


def test_envelope_exact_fields_binding_and_no_authority():
    a = mod()
    binding = a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32)
    cause = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    receipt = a.NativePrerequisiteReceipt(a.FixtureStatus.UNSUPPORTED, binding.manifest, (), a.ReasonCode.UNSUPPORTED_NAMESPACES)
    envelope = a.ExecutionEnvelope(binding.attempt, binding.token, binding.generation, binding.manifest,
                                   receipt, cause, None, a.OwnerState.CLOSED, 14, 14)
    assert a.ExecutionEnvelope.from_bytes(envelope.to_bytes()) == envelope
    assert len(envelope.to_bytes()) <= 1536
    for change in ({'release': True}, {'data_grant_released': True}, {'worker_created': 0}, {'registered_role_bits': True},
                   {'schema': 'maestro.staging-native-execution.v3'}, {'manifest': '00' * 32}):
        with pytest.raises(a.ContractError):
            a.ExecutionEnvelope.from_bytes(json.dumps(json.loads(envelope.to_bytes()) | change).encode())
    with pytest.raises(a.ContractError):
        a.NativePrerequisiteReceipt.from_bytes(envelope.to_bytes())


def test_frame_binary_header_and_fixed_payloads():
    a = mod()
    binding = a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32)
    frame = a.Frame(a.Opcode.GO_NATIVE, a.Role.HOST, a.Role.R, 5, binding, None)
    raw = frame.to_bytes()
    assert raw == b'SNF2\x02\x07\x00\x01\x00\x05' + bytes.fromhex('11' * 16 + '22' * 16 + '33' * 16 + '44' * 32)
    assert len(raw) == 90
    assert a.Frame.from_bytes(raw) == frame
    groups = a.Frame(a.Opcode.GROUPS, a.Role.HOST, a.Role.R, 1, binding, a.GroupsPayload(tuple(range(16))))
    assert len(groups.to_bytes()) == 155
    assert a.Frame.from_bytes(groups.to_bytes()) == groups
    for bad in (raw + b'\0', raw[:5] + b'\xff' + raw[6:], raw[:8] + b'\x00\x08' + raw[10:], raw[:-1], raw + b'\0' * 71):
        with pytest.raises(a.ContractError):
            a.Frame.from_bytes(bad)
    with pytest.raises(a.ContractError):
        replace(frame, sequence=True)
    with pytest.raises(a.ContractError):
        replace(frame, payload={})

def test_all_typed_payload_lengths_roundtrip_and_reject_trailing_bytes():
    a = mod()
    binding = a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32)
    cause = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    rows = (
        (a.Opcode.START, a.Role.HOST, a.Role.R, a.StartPayload((0, 1, 2, 3, 4, 5), 10, 30_000_000_010, 1000, 1000), 48),
        (a.Opcode.HELLO, a.Role.R, a.Role.HOST, a.RegistrationPayload(a.Role.R, 101, a.Role.HOST), 6),
        (a.Opcode.ACK_REGISTER, a.Role.HOST, a.Role.R, a.RegistrationPayload(a.Role.T, 103, a.Role.S), 6),
        (a.Opcode.IDENTITY, a.Role.R, a.Role.HOST, a.IdentityPayload(a.Role.T, a.Role.S, 103, 1000, 1000), 14),
        (a.Opcode.NS_IDENTITY, a.Role.C, a.Role.T, a.NamespacePayload(((1, 2), (3, 4), (5, 6)), 1, 0), 56),
        (a.Opcode.RESULT_PARKED, a.Role.R, a.Role.HOST,
         a.ResultPayload(a.FixtureStatus.UNKNOWN, a.ReasonCode.CONTROL_LOST, 0, a.OwnerState.PARKED_UNKNOWN, 12, 0, cause, None), 16),
        (a.Opcode.RESULT_CLOSED, a.Role.R, a.Role.HOST,
         a.ResultPayload(a.FixtureStatus.UNSUPPORTED, a.ReasonCode.UNSUPPORTED_NAMESPACES, 0, a.OwnerState.CLOSED, 12, 12, cause, None), 16),
        (a.Opcode.CONVERGE_PREUNSHARE, a.Role.S, a.Role.T, None, 0),
        (a.Opcode.GO_CLOSE_RANGE, a.Role.T, a.Role.C, None, 0),
        (a.Opcode.EXIT_NOTE, a.Role.T, a.Role.S,
         a.ExitPayload(a.Role.T, a.Role.S, 103, a.ExitKind.EXITED, 0, 31, cause, None), 29),
        (a.Opcode.REAP_ACK, a.Role.S, a.Role.R,
         a.ExitPayload(a.Role.T, a.Role.S, 103, a.ExitKind.EXITED, 0, 31, cause, None), 29),
        (a.Opcode.PREFIX_CLOSED, a.Role.R, a.Role.HOST, a.PrefixPayload(12, 12, 12, cause, None), 13),
    )
    for opcode, src, dst, payload, length in rows:
        frame = a.Frame(opcode, src, dst, 0, binding, payload)
        raw = frame.to_bytes()
        assert len(raw) == 90 + length and a.Frame.from_bytes(raw) == frame
        with pytest.raises(a.ContractError):
            a.Frame.from_bytes(raw + b'\0')

def test_observed_host_envelope_requires_complete_registered_reaped_prefix():
    a = mod()
    receipt = a.NativePrerequisiteReceipt(a.FixtureStatus.OBSERVED, '44' * 32, tuple(a.NativePrimitive), a.ReasonCode.CHECKS_OBSERVED)
    with pytest.raises(a.ContractError):
        a.ExecutionEnvelope('11' * 16, '22' * 16, '33' * 16, '44' * 32, receipt, None, None, a.OwnerState.CLOSED, 0, 0)


def test_local_runner_result_cannot_claim_runner_self_reap():
    a = mod()
    cause = a.FailureRecord(a.Role.T, a.NativePhase.UNSHARE, a.FailureKind.NATIVE_REJECTION, 1)
    with pytest.raises(a.ContractError):
        a.Frame(a.Opcode.RESULT_CLOSED, a.Role.R, a.Role.HOST, 3,
            a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32),
            a.ResultPayload(a.FixtureStatus.UNSUPPORTED, a.ReasonCode.UNSUPPORTED_NAMESPACES, 0, a.OwnerState.CLOSED, 14, 14, cause, None))

def test_failure_role_must_exist_in_reported_registration_prefix():
    a = mod()
    binding = a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32)
    cause = a.FailureRecord(a.Role.C, a.NativePhase.CLOSE_RANGE, a.FailureKind.NATIVE_REJECTION, 1)
    receipt = a.NativePrerequisiteReceipt(a.FixtureStatus.UNSUPPORTED, binding.manifest, (), a.ReasonCode.UNSUPPORTED_FD_CLOSE)
    with pytest.raises(a.ContractError):
        a.ExecutionEnvelope(binding.attempt, binding.token, binding.generation, binding.manifest,
                            receipt, cause, None, a.OwnerState.CLOSED, 14, 14)
    with pytest.raises(a.ContractError):
        a.Frame(a.Opcode.RESULT_CLOSED, a.Role.R, a.Role.HOST, 3, binding,
            a.ResultPayload(a.FixtureStatus.UNSUPPORTED, a.ReasonCode.UNSUPPORTED_FD_CLOSE, 0,
                            a.OwnerState.CLOSED, 12, 12, cause, None))


def test_observed_host_and_local_descendant_views_are_distinct_and_complete():
    a = mod()
    binding = a.AttemptBinding('11' * 16, '22' * 16, '33' * 16, '44' * 32)
    receipt = a.NativePrerequisiteReceipt(a.FixtureStatus.OBSERVED, binding.manifest, tuple(a.NativePrimitive), a.ReasonCode.CHECKS_OBSERVED)
    host = a.ExecutionEnvelope(binding.attempt, binding.token, binding.generation, binding.manifest,
                               receipt, None, None, a.OwnerState.CLOSED, 30, 30)
    assert a.ExecutionEnvelope.from_bytes(host.to_bytes()) == host
    result = a.Frame(a.Opcode.RESULT_CLOSED, a.Role.R, a.Role.HOST, 4, binding,
        a.ResultPayload(a.FixtureStatus.OBSERVED, a.ReasonCode.CHECKS_OBSERVED, 15,
                        a.OwnerState.CLOSED, 28, 28, None, None))
    assert a.Frame.from_bytes(result.to_bytes()) == result
