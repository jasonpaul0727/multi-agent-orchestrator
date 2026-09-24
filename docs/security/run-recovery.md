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
- Filesystem blobs with no `ArtifactPublished` event cannot be attributed to a
  Run by this operation. `ArtifactStore.find_orphan_blobs()` separately lists
  regular, content-address-verified files with no publication metadata. It
  serializes each candidate against publication using the digest lock, returns
  digests only, and never deletes or adopts objects. This is a global inventory,
  not a Run-level recovery action or a garbage collector.

## Verification and remaining P3 work

Unit/restart tests cover unknown-to-receipted effect projection, terminal
unknown-effect rejection, missing verifier, artifact content tampering, and
reopening the same database/artifact directory. Abrupt subprocess-death tests
now cover durable effect-intent-only and intent-plus-receipt records as well as
an artifact publication, followed by reopening and recovery. Existing process
death around Scheduler admission still covers only its SQLite transaction
boundary; no live Worker process is being supervised or restarted.

The Gateway now exposes a deterministic failure classifier that turns only
sanitized failure facts into `RecoveryEvidence`: retryable known failures may
enter bounded planning; output/capability failures retain their repair class;
unknown outcomes require reconciliation; nonrecoverable preflight failures
and known-success settlement failures are blocked. Its evidence hash omits
provider request IDs and never stores raw provider bodies. This classifier is
not yet persisted with the Run event stream or wired into a Worker/Scheduler
retry loop.

The cross-process interruption matrix, real Worker/OS termination receipt,
bounded retry integration, provider-side effect reconciliation, and
Run-attributable orphan-artifact accounting remain unimplemented. This slice
is not full crash recovery and does not satisfy the P3 or V1 delivery gate by
itself.
