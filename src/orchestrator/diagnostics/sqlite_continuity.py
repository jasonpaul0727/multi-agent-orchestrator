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
from pathlib import Path

from orchestrator.isolation._sqlite_storage_observation import observe_storage
from orchestrator.persistence._continuity_observation import (
    ObservationLimits, ObservationReason, _run_schema_observation,
)


@dataclass(frozen=True)
class DiagnosticReport:
    non_authoritative: bool
    findings: tuple[str, ...]
    unknowns: tuple[str, ...]


def diagnose_store(path: Path, limits: ObservationLimits) -> DiagnosticReport:
    storage = observe_storage(path)
    observation = _run_schema_observation(path, limits) if storage.preflight_ok else None
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
                'pathname_samples_may_race', 'sqlite_read_only_may_create_sidecars',
                'writer_inventory_unproven',
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
    return DiagnosticReport(non_authoritative=True, findings=tuple(findings),
                            unknowns=tuple(unknowns))


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    simulation_requested = any(arg.startswith('--simulate') for arg in arguments)

    class _Parser(argparse.ArgumentParser):
        def error(self, message):
            if simulation_requested:
                message = 'invalid simulation arguments'
            super().error(message)

    parser = _Parser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--database', type=Path)
    mode.add_argument('--simulate-journal', type=Path)
    parser.add_argument('--simulation-directory', type=Path,
                        help='existing private directory containing the simulation trace')
    args = parser.parse_args(arguments)
    if args.simulate_journal is not None:
        if args.simulation_directory is None:
            parser.error('--simulation-directory is required in simulation mode')
        from orchestrator.diagnostics._simulation_trace import simulate_journal
        report = simulate_journal(args.simulation_directory, args.simulate_journal)
        print(json.dumps(asdict(report), sort_keys=True, separators=(',', ':')))
        return int(report.reason not in ('ok', 'retired'))
    if args.simulation_directory is not None:
        parser.error('--simulation-directory requires simulation mode')
    report = diagnose_store(args.database, ObservationLimits())
    print(json.dumps(asdict(report), sort_keys=True, separators=(',', ':')))
    failures = {'budget_exhausted', 'database_unavailable', 'metadata_invalid', 'helper_cleanup_unconfirmed'}
    return int(bool(failures.intersection(report.findings)))


if __name__ == '__main__':
    raise SystemExit(main())
