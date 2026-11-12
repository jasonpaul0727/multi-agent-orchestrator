"""Opt-in local SQLite schema diagnostics; output is non-authoritative.

SQLite mode=ro avoids database writes but may use or create WAL/SHM sidecars,
depending on the VFS and existing sidecars. Observed sidecar changes are
non-authoritative unknowns. This is not a storage or handle-binding proof.
"""

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path

from orchestrator.persistence._continuity_observation import (
    ObservationLimits, ObservationReason, _run_schema_observation,
)


@dataclass(frozen=True)
class DiagnosticReport:
    non_authoritative: bool
    findings: tuple[str, ...]
    unknowns: tuple[str, ...]


def diagnose_store(path: Path, limits: ObservationLimits) -> DiagnosticReport:
    observation = _run_schema_observation(path, limits)
    errors = {ObservationReason.BUDGET_EXHAUSTED, ObservationReason.DATABASE_UNAVAILABLE,
              ObservationReason.METADATA_INVALID, ObservationReason.HELPER_CLEANUP_UNCONFIRMED}
    failures = tuple(reason.value for reason in observation.reasons if reason in errors)
    findings = list(failures)
    if not failures:
        names = {(item.kind, item.name) for item in observation.objects}
        for trigger in ('events_immutable_update', 'events_immutable_delete'):
            suffix = 'present' if ('trigger', trigger) in names else 'missing'
            findings.append(f'{trigger}_name_{suffix}')
        findings.append(f'recursive_triggers_{observation.recursive_triggers}')
        temp_status = 'present' if any(item.kind == 'trigger' for item in observation.temp_objects) else 'none'
        findings.append(f'current_connection_temp_triggers_{temp_status}')
    unknowns = ['storage_not_observed', 'writer_inventory_unproven',
                'cross_connection_temp_unobservable']
    if ObservationReason.SIDECAR_CHANGED in observation.reasons:
        unknowns.append('sidecar_changed')
    return DiagnosticReport(non_authoritative=True, findings=tuple(findings),
                            unknowns=tuple(unknowns))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True, type=Path)
    args = parser.parse_args(argv)
    report = diagnose_store(args.database, ObservationLimits())
    print(json.dumps(asdict(report), sort_keys=True, separators=(',', ':')))
    failures = {'budget_exhausted', 'database_unavailable', 'metadata_invalid', 'helper_cleanup_unconfirmed'}
    return int(bool(failures.intersection(report.findings)))


if __name__ == '__main__':
    raise SystemExit(main())
