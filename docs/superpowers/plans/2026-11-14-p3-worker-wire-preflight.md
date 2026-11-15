# P3 Worker wire encoder preflight implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: subagent-driven development, strict TDD and independent stage/whole review.

**Goal:** Apply existing v1 encoder limits before excess copying/encoding/traversal.
**Architecture:** Schema-aware bounded snapshot for outbound turn values; reuse current final validators/canonical encoding. No journal, service prepare or launcher changes.
**Tech stack:** Python standard library and existing contract models/pytest.
**Spec:** `docs/superpowers/specs/2026-11-14-p3-worker-wire-preflight-design.md` including independent review clarification.

## Global constraints

Keep accepted canonical bytes/commitment, list/tuple, exact builtin types, UUID/duplicates/order, integer policy and fixed public errors. No new upper bounds on fencing/graph/artifact-size integers, no Provider/auth/schema/unknown changes, no dependencies. Never touch the existing snapshot status-only flag. Three meaningful ordinary branch pushes minimum for this task; quota before dispatch and after each push, stop expansion if used>=50% or unavailable, daily max5 completions. No source edits while a full suite runs.

## Review focus

- Extra/same-count forged dictionary keys must not invoke caller callbacks.
- Array counts rejected before conversion; controlled grow/shrink cannot cause unbounded iteration.
- Invalid scalar containers and long strings rejected before recursive traversal/encoding.
- Completion response cheap character check and exact multibyte byte check preserve accepted bytes.
- Invalid start_turn reaches no launcher, but does not release any earlier prepared/unknown state.

## Task 1: Existing-limit preflight correction

### A: Contract shape, scalar and collection preflight

**Modify:** `src/orchestrator/runtime/general_worker_protocol.py`, `tests/unit/runtime/test_general_worker_protocol.py`.
- [x] RED tests with129 artifacts/65 capabilities asserting zero nested conversion; overlong text and invalid scalar structure before generic copy; unknown/forged keys with fixed expected field count and callback sentinels.
- [x] GREEN bounded shape/key snapshot and existing scalar/count validation before conversion; preserve final schema validation and all accepted semantics. Independent A review, focused tests, real commit/push; quota after push.

### B: Bounded outbound snapshot and response encoding

- [x] RED finite source grow/shrink during element conversion, wrong nested scalar values, completion char-cap before copying, exact Unicode byte boundaries/surrogates/empty response.
- [x] GREEN schema-aware fixed-index element snapshots and bounded dict items; eliminate generic recursive outbound input copying while retaining _plain for existing decode callers. Independent B review, focused tests, meaningful commit/push; quota.

### C: Compatibility and actual start_turn boundary

**Test:** protocol file and `tests/unit/runtime/test_general_worker_process.py`.
- [x] Add literal canonical bytes and input commitment fixtures for list/tuple, all existing bounds and valid integers beyond64bits; prove legacy v1 output identical.
- [x] Real GeneralWorkerProcess.start_turn with owned fake launcher counts zero calls for rejected inputs; preserve controls/BaseException and fixed detached errors. Verify original protocol/process tests.
- [x] Independent C and whole review, meaningful test/documentation commit/push; quota. No tests that just restate implementation internals without meaningful behavior.
- [x] Final full suite on exact final source (may share one full verification with independently reviewed SQLite task); preserve red reports and investigate failures. Update exact result/SHA/remaining P3 boundaries in existing progress ledger; no complete-P3 claim.

Final validation: combined source 2a684d9426cfd3a5b622979ed85ffa8706ffbc24, independent task/stage/whole reviews PASS, full pytest 5042 passed / 1 xfailed / 28 JUnit record_property compatibility warnings in 1441.11s. JUnit p3-durable-wire-full-2a684d9-20261115.xml retained in the existing progress ledger. This completes this bounded task only; full P3, production admission/stop/settlement authority and staging custody remain open.
