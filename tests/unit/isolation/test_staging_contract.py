"""Finite, local-only contracts: validation must happen before any allocation."""

from dataclasses import FrozenInstanceError, fields
import importlib
import importlib.util
import json
import os
from pathlib import Path

import pytest


def contract():
    # A missing implementation is an assertion failure, not a collection error.
    name = 'orchestrator.isolation._staging_contract'
    assert importlib.util.find_spec(name) is not None, 'finite staging contracts missing'
    return importlib.import_module(name)


def native_request(c):
    return c.NativePrerequisiteRequest(3, 4, c.PROFILE_ID, 100, 30_000_000_100)


def test_native_request_is_immutable_redacted_and_has_no_case_selector():
    c = contract()
    request = native_request(c)
    assert 'source_fd' not in repr(request) and 'arena_fd' not in repr(request)
    assert 'case_id' not in {item.name for item in fields(request)}
    with pytest.raises(FrozenInstanceError):
        request.profile_id = 'other'
    assert c.NativePrerequisiteRequest.from_bytes(request.to_bytes()) == request


@pytest.mark.parametrize('field,value', [
    ('source_fd', True), ('arena_fd', -1), ('profile_id', 'other'),
    ('started_ns', 1.0), ('deadline_ns', 30_000_000_101),
])
def test_native_request_rejects_wrong_exact_types_and_deadline(field, value):
    c = contract()
    values = dict(source_fd=3, arena_fd=4, profile_id=c.PROFILE_ID,
                  started_ns=100, deadline_ns=30_000_000_100)
    values[field] = value
    with pytest.raises(c.ContractError):
        c.NativePrerequisiteRequest(**values)


@pytest.mark.parametrize('payload', [
    b'{"schema":NaN}', b'{"schema":Infinity}', b'{"schema":1,"schema":2}',
    b'[]', b'{}', b'\xff', b' ' * 16385,
])
def test_strict_request_decoder_rejects_nonfinite_duplicate_and_oversized_input(payload):
    c = contract()
    with pytest.raises(c.ContractError):
        c.NativePrerequisiteRequest.from_bytes(payload)


def test_native_request_rejects_extra_selector_and_mutation_before_encoding():
    c = contract()
    request = native_request(c)
    data = json.loads(request.to_bytes())
    data['case_id'] = 'baseline'
    with pytest.raises(c.ContractError):
        c.NativePrerequisiteRequest.from_bytes(json.dumps(data).encode())
    object.__setattr__(request, 'source_fd', False)
    with pytest.raises(c.ContractError):
        request.to_bytes()


def test_gate_case_strings_do_not_create_new_operations():
    c = contract()
    request = c.GateRequest(3, 4, c.PROFILE_ID, c.CaseId.BASELINE, 100, 30_000_000_100)
    assert c.GateRequest.from_bytes(request.to_bytes()) == request
    with pytest.raises(c.ContractError):
        c.GateRequest(3, 4, c.PROFILE_ID, 'baseline', 100, 30_000_000_100)
    data = json.loads(request.to_bytes())
    data['case_id'] = 'arbitrary_command'
    with pytest.raises(c.ContractError):
        c.GateRequest.from_bytes(json.dumps(data).encode())


def test_native_receipt_cannot_decode_fixture_or_campaign_authority():
    c = contract()
    receipt = c.NativePrerequisiteReceipt(c.FixtureStatus.OBSERVED, 'a' * 64,
                                         tuple(c.NativePrimitive), c.ReasonCode.CHECKS_OBSERVED)
    data = json.loads(receipt.to_bytes())
    assert data['stage'] == 'native_prerequisites_only'
    assert data['local_observation_only'] is True and data['non_authoritative'] is True
    assert set(data) == {'schema', 'stage', 'status', 'profile_digest',
                         'completed_primitives', 'reason_code',
                         'local_observation_only', 'non_authoritative'}
    assert c.NativePrerequisiteReceipt.from_bytes(receipt.to_bytes()) == receipt
    data['status'] = 'PASS'
    with pytest.raises(c.ContractError):
        c.NativePrerequisiteReceipt.from_bytes(json.dumps(data).encode())
    with pytest.raises(c.ContractError):
        c.NativePrerequisiteReceipt(c.FixtureStatus.OBSERVED, 'a' * 64,
                                    (c.NativePrimitive.PIDFD,), c.ReasonCode.CHECKS_OBSERVED)


def test_fixture_requires_real_oracle_check_domain_and_cannot_become_native_receipt():
    c = contract()
    with pytest.raises(c.ContractError):
        c.FixtureObservation(c.CaseId.BASELINE, c.FixtureStatus.OBSERVED, 'a' * 64,
                             (c.CheckId.PROFILE,), c.ReasonCode.CHECKS_OBSERVED)
    observation = c.FixtureObservation(c.CaseId.BASELINE, c.FixtureStatus.UNKNOWN,
                                       'a' * 64, (), c.ReasonCode.LOCAL_CLEANUP_UNKNOWN)
    assert c.FixtureObservation.from_bytes(observation.to_bytes()) == observation
    with pytest.raises(c.ContractError):
        c.NativePrerequisiteReceipt.from_bytes(observation.to_bytes())
    with pytest.raises(c.ContractError):
        c.CampaignObservation.from_bytes(observation.to_bytes())


def test_campaign_refuses_missing_or_wrongly_ordered_results():
    c = contract()
    results = tuple(c.FixtureObservation(case, c.FixtureStatus.UNKNOWN, 'a' * 64,
                                        (), c.ReasonCode.LOCAL_CLEANUP_UNKNOWN)
                    for case in c.MANDATORY_CASES)
    report = c.CampaignObservation(c.CampaignStatus.UNKNOWN, 'a' * 64, results,
                                  c.ReasonCode.LOCAL_CLEANUP_UNKNOWN)
    assert c.CampaignObservation.from_bytes(report.to_bytes()) == report
    for incorrect in (results[:-1], results[::-1], (results[0],) * len(results)):
        with pytest.raises(c.ContractError):
            c.CampaignObservation(c.CampaignStatus.UNKNOWN, 'a' * 64, incorrect,
                                  c.ReasonCode.LOCAL_CLEANUP_UNKNOWN)
    with pytest.raises(c.ContractError):
        c.CampaignObservation(c.CampaignStatus.PASS, 'a' * 64, results,
                              c.ReasonCode.CHECKS_OBSERVED)


def test_one_absolute_deadline_and_cleanup_budget(monkeypatch):
    c = contract()
    deadline = c.Deadline(100, 30_000_000_100, 35_000_000_100)
    monkeypatch.setattr(c.time, 'monotonic_ns', lambda: 29_000_000_100)
    assert deadline.remaining_seconds() == 1.0
    assert deadline.remaining_seconds(cleanup=True) == 6.0
    monkeypatch.setattr(c.time, 'monotonic_ns', lambda: 36_000_000_100)
    assert deadline.remaining_seconds() == 0.0
    assert deadline.remaining_seconds(cleanup=True) == 0.0
    with pytest.raises(c.ContractError):
        c.Deadline(100, 30_000_000_100, 35_000_000_101)


def test_output_budget_charges_cumulatively_and_saturates():
    c = contract()
    budget = c.OutputBudget()
    assert budget.charge(10000) is True
    assert budget.charge(6384) is True
    assert budget.charge(1) is False
    assert budget.used == 16385
    assert budget.charge(2**63) is False
    assert budget.used == 16385
    with pytest.raises(c.ContractError):
        budget.charge(True)


def test_manifest_uses_fixed_native_prefix_and_verifies_real_bytes(monkeypatch):
    c = contract()
    monkeypatch.setenv('PYTHONPATH', '/untrusted/code')
    manifest = c.build_profile_manifest()
    assert manifest.domain == 'native_prerequisites_only'
    assert manifest.interpreter == '/usr/bin/python3.12'
    assert manifest.python_version == '3.12.3'
    assert manifest.architecture == 'x86_64'
    assert c.ProfileManifest.from_bytes(manifest.to_bytes()) == manifest
    c.verify_profile_manifest(manifest)
    for role in ('code_contract', 'code_arena', 'code_landlock', 'code_capabilities',
                 'interpreter', 'native_libc', 'native_seccomp'):
        item = next(item for item in manifest.objects if item.role == role)
        assert len(item.sha256) == 64 and Path(item.resolved_path).is_absolute()
    data = json.loads(manifest.to_bytes())
    data['profile_id'] = 'other'
    with pytest.raises(c.ContractError):
        c.ProfileManifest.from_bytes(json.dumps(data).encode())
    data = json.loads(manifest.to_bytes())
    data['objects'][0]['sha256'] = 'b' * 64
    with pytest.raises(c.ContractError):
        c.ProfileManifest.from_bytes(json.dumps(data).encode())
    object.__setattr__(manifest, 'digest', 'c' * 64)
    with pytest.raises(c.ContractError):
        c.verify_profile_manifest(manifest)


def test_fd_binding_rejects_swapped_closed_and_writable_descriptors(tmp_path):
    c = contract()
    from orchestrator.isolation._staging_arena import create_fixture_roots
    roots = create_fixture_roots(tmp_path)
    try:
        request = c.NativePrerequisiteRequest(roots.source_fd, roots.arena_fd,
                                             c.PROFILE_ID, 100, 30_000_000_100)
        c.validate_request(request, roots=roots)
        wrong = c.NativePrerequisiteRequest(roots.arena_fd, roots.source_fd,
                                           c.PROFILE_ID, 100, 30_000_000_100)
        with pytest.raises(c.ContractError):
            c.validate_request(wrong, roots=roots)
        os.close(roots.source_fd)
        with pytest.raises(c.ContractError):
            c.validate_request(request, roots=roots)
    finally:
        roots.close()


@pytest.mark.parametrize('primary_kind', [KeyboardInterrupt, RuntimeError])
def test_manifest_close_failure_preserves_body_exception_and_closes_once(tmp_path, monkeypatch, primary_kind):
    c = contract()
    source = tmp_path / 'owned-code'
    source.write_bytes(b'finite own bytes')
    primary = primary_kind('selected body exception')
    secondary = SystemExit('secondary close exception')
    real_close = os.close
    closed = []
    def close_then_fail(fd):
        closed.append(fd)
        real_close(fd)
        raise secondary
    with monkeypatch.context() as patch:
        patch.setattr(c.os, 'read', lambda *args: (_ for _ in ()).throw(primary))
        patch.setattr(c.os, 'close', close_then_fail)
        with pytest.raises(primary_kind) as caught:
            c._manifest_object('code_contract', str(source), 'file')
        assert caught.value is primary
    assert len(closed) == 1
    assert primary.__notes__ == ['owned_fd_close_unconfirmed']
