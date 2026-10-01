# Control-plane startup recovery

`orchestrator.application.ControlPlaneApplication` is the trusted host entry
point for the current admission, Run recovery, and Provider reconciliation APIs.
It owns one SQLite connection shared by Scheduler, ProviderCallJournal, budget,
lifecycle recovery, and the optional ArtifactStore. Construction returns only
after startup recovery has committed successfully.

```python
from pathlib import Path
from orchestrator.application import ControlPlaneApplication
from orchestrator.scheduler import ConcurrencyLimits

control = Path("/private/maestro-control")
with ControlPlaneApplication(
    control / "events.db",
    artifact_root=control / "artifacts",
    limits=ConcurrencyLimits(
        system_active_attempts=16,
        run_active_attempts=4,
        provider_active_attempts=4,
        tool_active_attempts=4,
    ),
) as application:
    report = application.startup_report
```

The caller supplies a trusted host concurrency envelope; frozen Run budgets,
policies, Registry snapshots, and Scheduler acceptance remain authoritative.
The report describes the committed startup snapshot. It is not a live status
feed or authorization to retry a Provider call.

## Transaction and readiness

The entire bootstrap runs under one SQLite `BEGIN IMMEDIATE` transaction:

1. Validate all frozen Run configs and recover initialized Runs. Reject orphan
   budget/Agent inventories, absent config snapshots, invalid artifacts, and
   cross-stream lifecycle/Scheduler/Agent/budget disagreements.
2. Match every Provider-call stream to the exact accepted route, Attempt fence,
   reservation, model, Provider adapter, and frozen Registry. Unresolved calls
   on terminal Attempts are rejected. Existing settlement markers must match
   the exact Scheduler reconciliation operation.
3. Apply only proofs already recorded as `settlement_pending`, using the
   existing Scheduler API and canonical idempotency keys. Revalidate all Run
   and Provider-call state after settlement.
4. Commit, then make the process-local application handle ready.

A late failure rolls back all startup settlement writes, including nested
Scheduler, budget, Agent, lifecycle, and Provider-marker events. Process death
before the outer commit has the same behavior. Independent processes serialize
bootstrap through SQLite, so the same proof is settled once. Reopening a
successful application produces no duplicate settlement or slot release.

Config snapshots whose lifecycle initialization was interrupted remain intact
and appear in `initializing_run_ids`; startup does not start those Runs. Calls
without an accepted proof remain unresolved, with budget/concurrency holds
intact, and appear in `unresolved_provider_calls`. Startup never contacts a
Provider or verifier, obtains credentials, dispatches a Worker/tool, or
replays an unknown external call.

## Host API and remaining integration

`accept_routing()` uses the existing Scheduler and its current-state checks.
`recover_run()` returns the trusted recovery view. `reconcile_provider_call()`
requires injected authoritative Provider and termination verifiers; their
defaults still reject new evidence. Closing the application closes its owned
connection and rejects subsequent operations with `application_not_ready`.
Bootstrap failures use `startup_recovery_failed` and suppress backend exception
text, raw evidence, secrets, and protected paths.

This composition root is a Python host API. Functional Worker execution,
automatic recovery-plan dispatch, CLI/MCP, and complete end-to-end security
acceptance remain to integrate. The real urllib Provider sender currently uses
a host background thread that can outlive a cancelled coroutine: a Worker
systemd stop receipt does not prove that sender stopped. Provider sender
supervision and authoritative Provider evidence remain separate production
reconciliation requirements.
