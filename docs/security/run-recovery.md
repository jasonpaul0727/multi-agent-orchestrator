# Run crash recovery boundary (partial P3 slice)

`RunRecoveryCoordinator` is a read-only consistency gate for one persisted Run.
It replays lifecycle, Agent Registry, budget, Scheduler lease, external-effect,
and artifact-publication state before new scheduling is admitted. It does not
start, terminate, or recreate Worker processes and does not execute recovery
plans.

## External effects

- An `EffectIntentRecorded` without a matching `EffectReceiptRecorded` is
  projected as `outcome_unknown`. Recovery never retries it implicitly,
  including intents marked `idempotent` or `queryable`.
- A receipt must match the original node, attempt, and fencing generation and
  include a valid outcome plus receipt evidence. Duplicate/orphan records,
  stale fencing, missing approval-grant consumption, or a terminal Attempt
  with an unresolved effect fail recovery closed.
- Legacy events that stored a provider idempotency key are projected with
  only its SHA-256 hash. This does not remove the original value from old event
  history; new producers must persist only the hash.
- Reconciliation against a provider, receipt authenticity verification, and
  safe continuation after reconciliation are not implemented here.

## Artifacts

- If a Run has `ArtifactPublished` metadata, recovery requires an
  `ArtifactStore` bound to the exact same `SQLiteEventStore` instance. Missing
  verifier or store mismatch fails closed. Hosts using Scheduler admission
  must pass that verifier as `Scheduler(..., artifact_store=...)`; omitting it
  intentionally blocks new attempts after artifact publication.
- Each matching publication is schema-validated and its content-addressed
  object is streamed through SHA-256 and byte-size verification. The recovery
  projection contains metadata only; artifact bytes are not returned.
- When publication metadata carries an attempt ID, node/attempt identity and
  any supplied fencing generation are checked against the Run replay. A
  fencing generation without an attempt is rejected.
- New publication attempts first append an `ArtifactPublicationIntent` with
  their declared source (including Run/Attempt provenance when provided), then
  install the content-addressed bytes, then append `ArtifactPublished`.
  Recovery returns pending intent metadata and checks any present object's
  digest/size. A pending candidate is never accepted as an artifact output,
  adopted, or deleted automatically. Worker publishers must include Run,
  node, and Attempt-generation source fields for Run-level attribution.
- A process death after the intent but before byte installation is reported as
  `missing`; death after byte installation but before `ArtifactPublished` is
  reported as `orphaned_blob`. If the digest was later published under another
  publication ID, the stale intent is reported as `already_published` rather
  than claiming ownership of that publication.
- Legacy/manual filesystem blobs with no intent remain unattributable to a
  Run. `ArtifactStore.find_orphan_blobs()` separately lists regular,
  content-address-verified files with no `ArtifactPublished` metadata. It
  serializes each candidate against publication using the digest lock, returns
  digests only, and never deletes or adopts objects. This remains a global
  inventory, not a garbage collector.

## Verification and remaining P3 work

### 2026-10-05 offline process-death evidence (scoped)

On Ubuntu 24.04.4 LTS under WSL2 (kernel `6.6.87.2-microsoft-standard-WSL2`,
systemd `255.4-1ubuntu8.17`, Python 3.12.3), the focused crash/recovery,
Approval, budget, workspace-publication, systemd-launcher and blocked-Worker
targets completed with **266 passed and no skips**. The full coverage gate
completed with **1,536 passed and 90.15% total coverage**. The focused command
was:

```bash
python3 -m pytest -o addopts= tests/integration/test_crash_matrix.py tests/integration/test_approval_tool_gateway.py tests/unit/approvals tests/unit/budget tests/unit/lifecycle/test_scheduler.py tests/unit/isolation/test_workspace_publish.py tests/integration/test_systemd_launcher.py tests/integration/test_isolated_worker_process.py -q
```

The matrix uses parent-issued `SIGKILL` and fresh stores/connections to prove:

- budget reservation and usage settlement rollback before SQLite `COMMIT`;
  post-commit response loss replays one durable result;
- an `ApprovalGrantBound` grant killed before consumption remains bound and
  unusable (a fresh approval is required); pre/post-commit consume deaths do
  not launch the tool, and committed consumption cannot replay the request;
- an external test receiver that applied an effect before the caller died
  without `EffectReceiptRecorded` remains `outcome_unknown`, with budget and
  active-slot holds retained and no recovery re-invocation;
- artifact deaths after intent and after blob installation remain pending as
  `missing` and `orphaned_blob`, respectively, preserving Run/Node/Attempt/
  fencing-generation provenance without admission or cleanup; and
- the retained fake Provider dispatch case refuses same-identity replay
  before Broker access or transport.

The base test environment initially lacked pytest-cov; the exact coverage
command was rerun in a temporary `/tmp` validation environment with
pytest-cov 7.1.0 and coverage 7.16.2. This did not change project dependency
metadata. Exact full gate and ancillary commands/results:

```bash
python3 -m pytest --cov=orchestrator --cov-report=term-missing --cov-fail-under=90  # 1,536 passed; 90.15%
python3 -m compileall -q src                                                   # passed
python3 -m pip check                                                           # No broken requirements found.
python3 -m pip wheel . --no-deps --wheel-dir /tmp/maestro-p3-crash-wheel      # built multi_agent_orchestrator-0.1.0-py3-none-any.whl
```

The wheel SHA-256 was `0e15cef23e2cd84b427d6715437c4a1de3d9b483b356a1b88394920b5ae315df`.
It proves packaging only, not runtime deployment compatibility. No live/paid
Provider request or production Provider-authority evidence was used.

Unit/restart tests cover unknown-to-receipted effect projection, terminal
unknown-effect rejection, missing verifier, artifact content tampering, and
reopening the same database/artifact directory. Abrupt subprocess-death tests
now cover durable effect-intent-only and intent-plus-receipt records, completed
artifact publication, and both pending artifact publication windows, followed
by reopening and recovery. The Scheduler
admission transaction and the explicit attempt-reconciliation transaction are
also tested with child-process death before commit and after commit/lost IPC
response. Before-commit death leaves the attempt `OutcomeUnknown` with budget,
Agent, and lease held; after-commit death replays a single terminal event and a
repeated reconciliation is idempotent. These tests exercise the trusted host
API's no-effect attestation path; they do not query a Provider or authenticate
provider-side receipts. No live Worker process is being supervised or
restarted.

The Gateway now exposes a deterministic failure classifier that turns only
sanitized failure facts into `RecoveryEvidence`: retryable known failures may
enter bounded planning; output/capability failures retain their repair class;
unknown outcomes require reconciliation; nonrecoverable preflight failures
and known-success settlement failures are blocked. Its evidence hash omits
provider request IDs and never stores raw provider bodies. `Scheduler` can
persist the classification and `RecoveryPlanCreated` together after the source
Attempt has durably failed or entered `OutcomeUnknown`. Before a same-node
retry/fallback/escalation is accepted, it verifies that the authorization is
persisted, matches the source plan, is consumed once, and that retry level,
failure class, and exhausted-model counters exactly match the evidence. Run
replay checks those bindings again. This is a bounded recovery-control API, not
an automatic Worker retry loop; the host still has to decide to call it and
route the next Attempt.

The measured 2026-10-05 matrix covers only the specified offline durable
boundaries. Functional Worker/OS lifecycle stop coordination, bounded retry
execution through a Worker/application service, and provider-side effect
reconciliation remain unimplemented. Run-scoped artifact accounting covers
only durable publication intents; legacy or bare filesystem orphans remain
unattributable. This is not full crash recovery and does not satisfy the P3 or
V1 delivery gate by itself.
