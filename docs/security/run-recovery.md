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

The complete cross-process interruption matrix, real Worker/OS termination
receipt, bounded retry integration, and provider-side effect reconciliation
remain unimplemented. Run-scoped artifact accounting covers only durable
publication intents; legacy or bare filesystem orphans remain unattributable.
This slice is not full crash recovery and does not satisfy the P3 or V1
delivery gate by itself.
