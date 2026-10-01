# Control-plane startup recovery

Scope: introduce the host application composition root that opens one SQLite
store, restores already-verified Provider settlements, validates every Run,
and enables admission only after a successful bootstrap. Continue the approved
V1 plan in the existing isolated codex branch.

## Decisions

- Startup uses one `BEGIN IMMEDIATE` transaction through `append_checked`.
  Run preflight, pending-proof settlement, and postflight share its connection.
  A failure or process death rolls back the entire bootstrap settlement batch.
- No startup operation calls a Provider, obtains credentials, resumes a Worker,
  dispatches tools, or guesses an unknown result. Persisted proof is the only
  authority for settlement. Unknown calls retain their resource holds.
- Cross-check all Provider-call identities against persisted accepted routes
  and recovered lifecycle Attempts. A Worker systemd stop receipt is not a
  proof that the host Gateway HTTP sender stopped: urllib currently runs in
  a host background thread. Sender supervision remains a separate task.
- The application owns its connection. Its constructor bootstraps before
  returning; its admission/recovery/reconciliation methods reject closed state.
  Errors contain stable codes and sanitized text. Startup reports are metadata
  projections, never authorization for replay.

## Task 1: Application composition and startup gate

Write failing integration tests in the Scheduler test module using its real
Run/route/budget fixtures. Implement `orchestrator.application` with
`ControlPlaneApplication`, immutable `StartupRecoveryReport`, and typed errors.
The constructor optionally creates ArtifactStore on the exact same SQLite
connection. Wrap trusted routing acceptance, Run recovery, and explicit Provider
reconciliation behind the readiness gate.

## Task 2: Restart, concurrency, and rollback proof

Test automatic pending-proof settlement after reopen, unchanged unknown holds,
exact call/route matching, idempotent repeated startup, two independent startup
connections, fail-closed corrupt unrelated Run, rollback after a late failure,
and process death inside bootstrap before its outer commit. No test needs paid
Provider credentials. Keep tests at real database and subprocess boundaries.

## Task 3: Verification and release

Run focused application/Scheduler/journal/contract tests, full tests and the
90% coverage gate, compilation, dependency check, wheel build, and diff check.
Keep coverage report precision at two decimals so rounding cannot hide a result
below the 90% threshold. Exercise explicit host reconciliation inputs, injected
and unavailable verifiers, and frozen-Registry adapter bindings.
Review the change independently, address substantive findings, document the
application entry point and remaining production gates, commit, push, and
verify the remote branch ref. Main checkout user changes remain outside scope.

## Review focus

Readiness before commit; startup inventory races; partial settlements on any
failure; orphan/misbound calls; terminal Attempts with unresolved calls; absence
of external side effects; sanitized failures; closed connection ownership.

## Slice acceptance evidence — 2026-09-30

- All 1,238 tests pass, including 26 host application cases and a real spawned
  process death before the outer startup commit.
- Full-suite coverage is 90.05%; the new application module is 96.88%. The
  unchanged 90% gate now uses two-decimal precision instead of rounding to
  whole percentages. CAS race tests synchronize at the actual append boundary
  and cover both identical and conflicting concurrent proofs.
- Compilation, dependency consistency, wheel build, and diff checks pass.
  Independent review found no actionable implementation findings.
- No paid Provider calls or live credentials were used. This host API slice is
  not full V1 delivery; the production integrations listed above remain open.
