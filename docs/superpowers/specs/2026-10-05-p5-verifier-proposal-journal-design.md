# P5 Verifier Proposal Journal Design

**Status:** Controller-authored next-slice design; limited to evidence proposal persistence
**Date:** 2026-10-05
**Target:** Make bounded, attempt-scoped Verifier proposals durable and replayable without accepting a Node

## Goal

Persist the output of the existing isolated Verifier as an immutable, bounded proposal in the host EventStore. A fresh process/store instance must be able to read the exact task/evidence pair and verify its canonical digest. Concurrent identical submissions are idempotent; a conflicting proposal cannot reuse the same idempotency identity.

This is a P5 foundation slice only. Recording evidence must not complete an Attempt, accept a Node, release Scheduler resources, advance dependent nodes, or imply that a proposal is true.

## Current boundary

`VerificationTask` and `VerificationEvidence` are strict bounded IPC contracts. `IsolatedVerifierProcess` revalidates Attempt and ArtifactStore provenance, executes deterministic format checks in a systemd read-only child, and returns a validated proposal. The evidence is not currently durable. The Worker remains blocked-only and cannot produce candidates.

## Design decisions

1. Add a host-only `VerifierProposalJournal` over `SQLiteEventStore`, using one dedicated `verification_proposals` stream per Run and the exact event type `VerifierProposalRecorded`. It accepts only revalidated `VerificationTask`/`VerificationEvidence` values and calls the existing `validate_verification_evidence` gate before any write.
2. Store the bounded task and evidence JSON, their canonical SHA-256 hashes, the exact Run/Node/Attempt/generation/graph bindings, and the artifact digest set. Store no artifact bytes, credentials, Provider request/response, prompts, workspace paths, or exception text. Restrict the supported verifier ID and acceptance contract to the existing built-in `builtin.readonly-v1` / `maestro.artifact-verification/v1` pair and supported deterministic checks; no arbitrary prompt-like contract text is journaled.
3. Encode canonical JSON as UTF-8 with sorted object keys, compact separators, `ensure_ascii=False`, and `allow_nan=False`; hash those exact bytes with SHA-256 and persist payload `schema_version=1`. Derive a stable idempotency key from Run, Node, Attempt, generation, and task hash. The event `causation_id` is the exact task hash; `correlation_id` is the Run ID. Repeating the exact proposal returns the existing record. A different evidence payload under the same identity fails closed; a distinct task hash is a distinct proposal.
4. Read/replay reconstructs the strict models from durable payloads, re-runs exact task/evidence validation, recomputes both content hashes, and checks the event type/schema, stream type/ID, Run/Node/Attempt/generation headers, correlation/causation IDs, derived idempotency key, and payload identity. It rejects corruption rather than returning a partial record. It does not re-read ArtifactStore bytes or assert that referenced objects remain available.
5. The journal records proposals only. It does not invoke a Verifier, independently attest process origin, prove that the referenced Attempt is still active, accept or reject a Node, add lifecycle transitions, or call the Scheduler. The application layer must call it only after `IsolatedVerifierProcess.verify()` returns. Durable acceptance and success gating remain separate follow-up work.
6. Enforce a hard 1 MiB encoded-record ceiling before opening a write transaction, plus at most 1,024 proposals and 16 MiB of canonical payload bytes per Run. Check count/aggregate bytes against the complete stream inside `append_checked`'s SQLite write lock, and enforce the same caps during replay. The count cap also bounds event-row overhead; together the caps bound full-stream reads and append scans. All operations use the existing SQLite append/CAS API; no new dependency or schema migration is expected.

## Failure and concurrency behavior

- Invalid schemas, task/evidence binding mismatch, missing/extra checks, unsupported verifier/check/contract, malformed or header-mismatched durable events, hash mismatch, per-record/per-Run count or byte limit violations, or encoded oversize fail closed and write nothing.
- Two connections recording the same proposal leave exactly one durable event and both read the same immutable record. Re-submitting an existing proposal remains idempotent when the Run is already at either configured cap because no new record is appended.
- Conflicting content cannot overwrite, supersede, or silently repair a prior event. Recovery only reads proposals; it never executes verification or promotes state.
- EventStore unavailability propagates as a stable journal error without including task/evidence contents in its message.

## Non-goals

- Functional Worker execution, Gateway/Tool/Secret Broker calls, candidate generation, project test execution, semantic review, Final Review, lifecycle acceptance events, Scheduler settlement, automatic recovery, CLI/MCP, or paid Provider calls.
- Claiming that hash-consistent event data proves a Verifier process actually ran. The journal's trust boundary begins at the trusted host caller.
- Retaining ArtifactStore content or revalidating object availability when replaying historical evidence.

## Acceptance

1. Unit and real-SQLite tests cover proposal round-trip through fresh connections, exact attempt/check/artifact binding, unsupported or malformed input rejection, payload/header/idempotency corruption rejection, per-record and per-Run count/byte limits, duplicate idempotent calls, and concurrent identical writes.
2. Tests prove recording and replay do not mutate lifecycle/Scheduler streams or any ArtifactStore object.
3. Run the focused runtime suite and the full project gate: total coverage at least 90%, compileall, `pip check`, wheel build, and branch-range `git diff --check`.
4. Independent review precedes commit/push to `codex/p3-systemd-termination-receipts`; verify the remote tip and clean tracked worktree. Do not merge `main`.

## Remaining P5/V1 blockers after this slice

The journal remains proposal storage, not completion. Functional Worker/model/tool execution, persisted control-plane acceptance of independently verified candidates, semantic/project-test validation, full Agent collaboration, CLI/MCP, production identity/secret backends, end-to-end security acceptance, authoritative Provider reconciliation, and measured cost/token/rework improvements all remain open.
