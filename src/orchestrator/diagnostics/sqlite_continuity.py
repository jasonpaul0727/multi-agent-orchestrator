"""Opt-in local SQLite schema diagnostics; output is non-authoritative.

SQLite mode=ro avoids database writes but may use or create WAL/SHM sidecars,
depending on the VFS and existing sidecars. Observed sidecar changes are
non-authoritative unknowns. Table names do not identify who opened a
database or validate its schema. This is not a storage or handle-binding proof.
"""

import argparse
from dataclasses import asdict, dataclass
import json
import sys
from time import sleep as _comparison_delay
from pathlib import Path

from orchestrator.isolation._sqlite_storage_observation import (
    StorageObservation, compare_storage_observations, observe_storage,
)
from orchestrator.persistence._continuity_observation import (
    ObservationLimits, ObservationReason, _run_schema_observation, _compare_schema_observations,
)


@dataclass(frozen=True)
class DiagnosticReport:
    non_authoritative: bool
    findings: tuple[str, ...]
    unknowns: tuple[str, ...]


def _diagnose_observation(path: Path, storage: StorageObservation, observation):
    errors = {ObservationReason.BUDGET_EXHAUSTED, ObservationReason.DATABASE_UNAVAILABLE,
              ObservationReason.METADATA_INVALID, ObservationReason.HELPER_CLEANUP_UNCONFIRMED}
    failures = (tuple(reason.value for reason in observation.reasons if reason in errors)
                if observation is not None else (storage.reason,))
    after = None
    if storage.preflight_ok:
        after = observe_storage(path)
        if not after.preflight_ok or after.path_sample != storage.path_sample:
            failures += (after.reason or 'database_unavailable',)
    findings = list(storage.findings) + list(failures)
    if not failures:
        names = {(item.kind, item.name) for item in observation.objects}
        for table in ('events', 'stream_versions', 'snapshots'):
            suffix = 'present' if ('table', table) in names else 'missing'
            findings.append(f'{table}_table_name_{suffix}')
        for trigger in ('events_immutable_update', 'events_immutable_delete'):
            suffix = 'present' if ('trigger', trigger) in names else 'missing'
            findings.append(f'{trigger}_name_{suffix}')
        findings.append(f'recursive_triggers_{observation.recursive_triggers}')
        temp_status = 'present' if any(item.kind == 'trigger' for item in observation.temp_objects) else 'none'
        findings.append(f'current_connection_temp_triggers_{temp_status}')
    unknowns = ['sqlite_handle_binding_unproven', 'storage_continuity_unproven',
                'physical_storage_durability_unproven',
                'pathname_samples_may_race', 'sqlite_read_only_may_create_sidecars',
                'writer_inventory_unproven', 'schema_continuity_unproven',
                'cross_connection_temp_unobservable', 'independent_connection_state_unobserved']
    samples = []
    if storage.preflight_ok:
        samples.append(storage.sidecar_sample)
    if observation is not None:
        samples.extend(observation.sidecar_samples)
    if after is not None and after.preflight_ok:
        samples.append(after.sidecar_sample)
    if ((observation is not None and ObservationReason.SIDECAR_CHANGED in observation.reasons)
            or any(left != right for left, right in zip(samples, samples[1:]))):
        unknowns.append('sidecar_changed')
    report = DiagnosticReport(non_authoritative=True, findings=tuple(findings),
                              unknowns=tuple(unknowns))
    sample = storage if not failures else StorageObservation((), False, 'database_unavailable')
    return report, sample


def _diagnose_sample(path: Path, limits: ObservationLimits):
    storage = observe_storage(path)
    observation = _run_schema_observation(path, limits) if storage.preflight_ok else None
    return _diagnose_observation(path, storage, observation)


def diagnose_store(path: Path, limits: ObservationLimits) -> DiagnosticReport:
    return _diagnose_sample(path, limits)[0]


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    simulation_requested = any(arg.startswith('--simulate') for arg in arguments)
    comparison_requested = any(arg.startswith('--compare-delay-ms') for arg in arguments)

    class _Parser(argparse.ArgumentParser):
        def error(self, message):
            if simulation_requested:
                message = 'invalid simulation arguments'
            elif comparison_requested:
                message = 'invalid comparison arguments'
            else:
                message = 'invalid diagnostic arguments'
            super().error(message)

    parser = _Parser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--database', type=Path)
    mode.add_argument('--simulate-journal', type=Path)
    parser.add_argument('--compare-delay-ms', type=int)
    parser.add_argument('--simulation-directory', type=Path,
                        help='existing private directory containing the simulation trace')
    args = parser.parse_args(arguments)
    if args.compare_delay_ms is not None and not 0 <= args.compare_delay_ms <= 1000:
        parser.error('invalid comparison delay')
    if args.simulate_journal is not None:
        if args.compare_delay_ms is not None:
            parser.error('comparison requires database mode')
        if args.simulation_directory is None:
            parser.error('--simulation-directory is required in simulation mode')
        from orchestrator.diagnostics._simulation_trace import simulate_journal
        report = simulate_journal(args.simulation_directory, args.simulate_journal)
        print(json.dumps(asdict(report), sort_keys=True, separators=(',', ':')))
        return int(report.reason not in ('ok', 'retired'))
    if args.simulation_directory is not None:
        parser.error('--simulation-directory requires simulation mode')
    failures = {'budget_exhausted', 'database_unavailable', 'metadata_invalid', 'helper_cleanup_unconfirmed'}
    if args.compare_delay_ms is None:
        report = diagnose_store(args.database, ObservationLimits())
        print(json.dumps(asdict(report), sort_keys=True, separators=(',', ':')))
        return int(bool(failures.intersection(report.findings)))
    limits = ObservationLimits()
    samples = []
    storage = None
    def begin_sample():
        nonlocal storage
        storage = observe_storage(args.database)
        return storage.preflight_ok
    def end_sample(observation):
        samples.append(_diagnose_observation(args.database, storage, observation))
        return samples[-1][1].preflight_ok
    schema_comparison = _compare_schema_observations(
        args.database, limits, lambda: _comparison_delay(args.compare_delay_ms / 1000),
        begin_sample=begin_sample, end_sample=end_sample)
    (first, first_sample), (second, second_sample) = samples
    comparison = compare_storage_observations(first_sample, second_sample)
    output = {'non_authoritative': True, 'before': asdict(first), 'after': asdict(second),
              'comparison': asdict(comparison), 'schema_comparison': asdict(schema_comparison)}
    print(json.dumps(output, sort_keys=True, separators=(',', ':')))
    return int(bool(failures.intersection(first.findings) or failures.intersection(second.findings)))


if __name__ == '__main__':
    raise SystemExit(main())
