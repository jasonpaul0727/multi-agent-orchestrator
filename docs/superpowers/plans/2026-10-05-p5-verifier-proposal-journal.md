# P5 Verifier Proposal Journal Plan

> **For agentic implementers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` to implement this plan one reviewed task at a time. Do not commit or push before independent review of the exact candidate.

**Goal:** Persist bounded, Attempt-scoped Verifier evidence proposals across process restart without promoting a lifecycle or Scheduler success state.

**Architecture:** Add a host-side append-only journal over the existing `SQLiteEventStore`. It accepts already validated task/evidence models, revalidates exact bindings and supported acceptance contract, computes canonical content hashes, writes a single CAS/idempotency-protected event, and verifies all bindings/hashes again on replay. This journal is intentionally separate from LifecycleController and Scheduler so proposal recording cannot imply acceptance.

**Tech stack:** Existing Pydantic contracts, `SQLiteEventStore`, Python stdlib JSON/SHA-256, pytest.

**Binding design:** `docs/superpowers/specs/2026-10-05-p5-verifier-proposal-journal-design.md`.

## Progress — 2026-10-05

- [x] Task 1: proposal journal implementation, independent review and measured
  implementation gate complete; committed and pushed as
  `3f744311e931d20b6f7ad79a37354a265fae8a50`.
- [x] Task 2: documentation independently approved; the controller's final
  whole-slice verification and remote-tip check passed.
- [x] Close this narrow proposal-journal plan after Task 2 review and the final
  gate. This closure does not complete broader P5 or V1; their unchecked items
  stay open.

Task 1's post-review gate recorded 1,567 passed, zero skipped, total coverage
90.12% and journal-module coverage 87.73%. Focused runtime/live Verifier
integration recorded 211 passed, zero skipped. `compileall`, `pip check`,
wheel build and branch whitespace checks passed.
Journal-module coverage is below 90%; the configured 90% gate applies to total
coverage, and no module-specific threshold is configured.

The controller's final whole-slice gate also passed: 211 focused runtime/live
Verifier integration tests and 1,567 full-suite tests, zero skips in either
suite, 90.12% total coverage and 87.73% journal-module coverage. `compileall`,
`pip check`, wheel build, full-branch `git diff --check` and remote-tip
verification all passed. This closes only durable Verifier proposal replay;
Node acceptance, lifecycle/Scheduler integration and all broader P5/V1
completion requirements remain open.

## Task 1: Implement the proposal journal with restart and concurrency tests

**Files:**
- Add: `src/orchestrator/runtime/verification_journal.py`
- Update: `src/orchestrator/runtime/contracts.py`
- Update: `src/orchestrator/runtime/verifier_process.py`
- Update: `src/orchestrator/runtime/__init__.py`
- Add: `tests/unit/runtime/test_verification_journal.py`

1. Add tests first for strict task/evidence validation, one record round-tripped through a fresh SQLiteEventStore, canonical hashes and attempt/artifact bindings, identical idempotent re-submit (including at count/byte caps), conflicting same-identity submit, oversized record rejection, per-Run proposal-count/aggregate-byte caps, corrupt payload/header/idempotency rejection, and concurrent identical writes on separate connections.
2. Add an immutable proposal record shape that stores validated task/evidence JSON, canonical digests, and explicit Run/Node/Attempt/generation/graph/artifact binding fields. Do not store artifact bytes or arbitrary acceptance-contract text.
3. Promote the existing fixed verifier ID and contract ID to shared public constants; the process and journal must accept only that verifier/contract and the currently supported check set.
4. Revalidate caller-supplied models (rather than trusting an existing model instance) and re-run `validate_verification_evidence` before append and during replay.
5. Implement `VerifierProposalJournal.record(task, evidence)` and `read_run(run_id)` on a dedicated `verification_proposals` stream using event type `VerifierProposalRecorded`. Define UTF-8 canonical JSON (`sort_keys=True`, compact separators, `ensure_ascii=False`, `allow_nan=False`) and payload schema version 1. Use the task hash as event causation ID and Run ID as correlation ID; derive the idempotency key from Run/Node/Attempt/generation/task hash. On replay, verify event type/schema, stream/run/node/attempt/generation headers, correlation/causation, and idempotency key against the payload/task. Enforce 1 MiB per record and, inside the `append_checked` transaction, at most 1,024 proposals and 16 MiB aggregate canonical payload bytes per Run; enforce the same limits during replay. Test that identical retries still succeed at either cap; they must not append another event. Use EventStore append/CAS/idempotency; do not create direct database tables or migrations.
6. Add an assertion that journal record/replay leaves lifecycle and Scheduler streams untouched, and does not access or mutate ArtifactStore bytes.
7. Run focused tests and the full existing suite; report exact counts, skips, coverage, and commands. Do not mark P5 or V1 complete.

## Task 2: Record evidence and close this plan after verification

**Files:**
- Update: `docs/security/worker-runtime.md`
- Update: `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`
- Update: this plan

1. Document the exact journal guarantees and limitations: durable proposal replay only, trusted-host caller requirement, no Node acceptance/Scheduler effects, and remaining Worker/Verifier/product gaps.
2. Mark only this subplan's completed steps after the measured gate passes; preserve all broader P5/V1 unchecked states.
3. Run `git diff --check` on the entire branch range from `git merge-base origin/main HEAD`, `compileall`, `pip check`, wheel build, focused tests, and full coverage gate.

## Execution sequence

- Dispatch Task 1 to an implementer; review the exact uncommitted diff independently.
- If review finds a correctness/security issue, implementer fixes it; obtain a focused re-review before commit/push.
- After Task 1's review passes, commit and push it, verify the remote tip, then start Task 2.
- Dispatch Task 2 to a documentation implementer; independently review the exact docs candidate before commit/push.
- Finish with a whole-slice self-review, full project gate, remote-tip verification, and explicit statement that P5/V1 remain incomplete.

## Self-review before execution

- Scope is intentionally below full P5: it persists verifier *proposals* only and does not change lifecycle or Scheduler success semantics.
- The current verifier supports only one fixed deterministic acceptance contract, so arbitrary free-form acceptance text is excluded from persistence.
- EventStore CAS is the concurrency boundary; tests must use separate real SQLite connections, not only in-memory fakes.
- Adding a second table or direct DB access would duplicate durability ownership and is explicitly prohibited.
- Reviewer must verify that replay treats durable payloads and event metadata as untrusted, that per-record size validation happens before writes, and that count/aggregate caps are enforced atomically under the EventStore lock.
- If the strict existing contract cannot safely support this scope without adding lifecycle semantics, stop and revise this plan instead of widening the task silently.
