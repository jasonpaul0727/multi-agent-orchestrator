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
   Authentic BASE v1 intents without adapter/correlation metadata retain their
   exact route/Registry binding and recover without inventing those fields.
   Their unknown holds and replay block remain; new evidence reconciliation is
   unsupported without a real historical correlation.
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

## Host API

`accept_routing()` uses the existing Scheduler and its current-state checks.
`recover_run()` returns the trusted recovery view. `reconcile_provider_call()`
requires injected authoritative Provider and termination verifiers; their
defaults still reject new evidence. Closing the application closes its owned
connection and rejects subsequent operations with `application_not_ready`.
Bootstrap failures use `startup_recovery_failed` and suppress backend exception
text, raw evidence, secrets, and protected paths.

## Explicit recovery continuation

`list_recovery_opportunities(*, run_id)` returns bounded metadata from the
current durable snapshot. Construction, startup and inventory queries never
admit a successor or dispatch work. A consumed authorization's `already_admitted`
inventory status identifies a historical accepted route; it does not authorize
preparing or executing that Attempt now. Both recovery host methods require
committed application readiness and reject use after close or failed startup.

For a selected opportunity, the host explicitly re-supplies the exact original
task text and calls `continue_recovery` with these keyword arguments:

```python
result = application.continue_recovery(
    run_id=opportunity.run_id,
    node_id=opportunity.node_id,
    source_attempt_id=opportunity.source_attempt_id,
    source_fencing_generation=opportunity.source_fencing_generation,
    task_text=explicit_task_text,
    routing_inputs=trusted_routing_inputs,
    accepted_at=accepted_at,
    lease_expires_at=lease_expires_at,
)
```

`trusted_routing_inputs` is a concrete `RecoveryRoutingInputs`, containing data
observations rather than executable services. The host supplies the frozen
`PolicyManifest`, current budget availability/version from the Run ledger,
Provider/model health versions from `HealthController`, and trusted secret
availability/revocation/emergency-deny observations from its host inventory.
Tokenizer bindings and FX snapshots come from trusted host metadata with
validity windows covering the supplied admission time. Secret availability is
an observation; it grants no credential or dispatch authority. The admission
transaction rechecks durable budget and health state. Callback objects,
overridable data types and foreign timezone implementations are rejected; no
host callback, credential lookup or Provider request runs under either lock.

Missing text returns `waiting_for_input`; missing observations for a new
successor return `waiting_for_routing`. Only the persisted same-model retry,
same-tier fallback or capability-escalation authorization can admit through
the existing Scheduler. Repair/new-child planning returns metadata without
creating a node. Task text remains in memory; its input commitment is persisted
without the plaintext. Different text cannot replace that commitment, including
on replay of an already consumed authorization.

Continuation commits admission before opening a second checked preparation
transaction on the exact same Scheduler/store. That snapshot requires the Run
to be running, the exact successor to be latest, active and unexpired, and
current same-node Provider/effect outcomes to be resolved. A package-internal
owned WorkerInputService path shares the public input/context/frame validation
and samples standard-library UTC after acquiring the lock. It never calls the
legacy injected clock or an instance-overridden method, and exposes no public
`as_of` override. A different input-service Scheduler, including another
connection to the same file, or an overridable substitute is rejected before
admission. Existing public WorkerInputService clock behavior remains supported.

Only `status="prepared"` carries a real `WorkerTask`, for the exact committed
successor. Preparation returns data at a serialized validity point; it neither
launches a Worker nor promises the lease remains valid indefinitely. Paused or
cancelled Run authority returns `blocked`; an inactive/expired/superseded
successor returns `successor_inactive`; current unknown outcomes return
`reconciliation_required`. These responses retain any accepted route and
resource holds, with no task or readmission. Preparation failure returns
`admitted` with bounded reason `task_preparation_failed`, keeping the committed
identity/resources. A later explicit call with exact text can prepare the same
still-active successor; routing observations may be omitted on that replay.

## Remaining integration

This composition root is a Python host API. A separate P0 path now executes one
fixed `workspace.write-candidate` tool for an accepted Attempt through a real
systemd Worker and host `WorkspaceWriteGateway`; it does not dispatch a general
Worker, model task, or recovery plan. Automatic recovery-plan dispatch, general
Worker/Scheduler integration, CLI/MCP, and complete end-to-end security
acceptance remain unimplemented. The default Provider transport is the fixed
systemd sender, with bounded host stop waits and exact unit/cgroup receipts.
An unconfirmed stop returns an unknown failure, retains staging, and cannot
release effect/budget holds. The urllib thread transport is an explicit legacy
option and supplies no supervised sender proof. A Worker stop receipt does not
prove that either sender stopped; Provider charge evidence remains independent.

`SecretBrokerProcessManager` exposes the separate Linux Broker process API:
readiness-gated Unix client, bounded IPC, audit-before-value access and pidfd
shutdown. Its runtime root must be canonical and private under `/run/user/<uid>`
and absent from runtime/source binds. Its production backend remains
`UnavailableSecretValueStore`; the application does not automatically start or
wire a production credential store. Production authoritative Provider evidence,
secret-backend integration and full Gateway/Worker composition remain open.

The P0 clean-checkout smoke was local security/correctness evidence only. It
made no Provider request and does not establish provider delivery/charges or
production reconciliation. The final full suite, private wheel build and
contents review, exact environment, profile hashes, and limits are recorded in
[`P0 security closure validation`](../validation/2026-10-18-p0-security-closure.md).
The two original P0 acceptance items are closed; this does not close the
production credential backend, general Worker composition, or V1 platform gate.

The explicit recovery inventory/admission/task preparation APIs are a partial
P3 slice. The process-race/crash regressions cover two independent spawned
application owners, cancellation/health/budget lock winners, real parent-issued
SIGKILL before/after admission commit and before preparation, exact input
re-supply, and fresh reopens with unresolved Provider calls. Their source
identity, full coverage, required real-isolation matrix and package checks are
recorded in [P3 recovery coordination validation](../validation/2026-10-23-p3-recovery-coordination.md).
Independent Task 4 and whole-slice reviews approved candidate
`5d82fc03f352ac4a4137a7910d65e750c75312a3`, with no Critical/Important findings.
The documentation correction's scoped review and root's main integration
remain pending, as do general resumed Worker execution, production credential/evidence
integration and complete P3 acceptance. A later same-model retry plan may still
authorize a model excluded by Router exhaustion; that unresolved upstream
policy mismatch stays safely blocked without acquiring new holds.
