# P3 Dual-View Offline Model Implementation Plan

> **For agentic workers:** Use subagent-driven development, TDD, independent task and final review.

**Goal:** Test the internally consistent versioned dual-view proposal with an explicitly non-authoritative, finite, in-memory model.
**Architecture:** A tests-only immutable state machine accepts synthetic values; no product module or external action consumes its outputs. Real admission, full v1 validation and unknown reconciliation stay unchanged.
**Tech stack:** Python standard library and existing pytest.
**Spec:** `docs/superpowers/specs/2026-11-14-p3-dual-view-offline-model-design.md`, including 2026-11-15 review amendments.

## Global constraints

No production imports in the model, provider, broker, worker, subprocess, I/O, credentials, schema, runtime adapter or production data. Fixed bounded enumerations only; output states explicitly simulation-only/non-authoritative. The model cannot justify a production decision. No deployment, main merge, force push or dependency installation. Three substantive ordinary branch pushes are required for this one task, each reviewed and accompanied by a fresh quota check after pushing. Preserve unrelated files and the existing marked snapshot test.

## Review focus

- First effect observation before the stop barrier, including repeated YES and replay: remain held.
- After winner reservation consumes the only synthetic budget, repeated checks use that exact reservation and do not spend again.
- Unknown or revoked facts before and after winner remain blocked; old winner/history is not erased.
- Actual v1 exclusion is respected in model and the model does not replace any v1 checks.
- Competing versions and entries cannot both win; replay rejects inconsistent history without partially changing state.

## Task 1: Finite isolated state machine and adversarial verification

**Files:** create `tests/support/dual_view_simulation.py`, `tests/unit/runtime/test_dual_view_simulation.py`; documentation may reference this plan and the spec. No production files.
**Interfaces:** `initial_state()` returns immutable synthetic state. `step(state, command)` returns a new immutable state and a marked synthetic observation. Define precise frozen enums and command types solely within the model, in accordance with the spec; no production-compatible identity, credential, authorization, serialization or adapter.

- [x] A. Write and run actual failing tests, then implement source registration, bounded immutable envelopes, historical and eligibility views, synthetic facts and ordering, safe invalid inputs, v1/B exclusion rules and first-YES ordinal. Cover strict post-stop effect observation and repeated YES including replay expectations. Review A independently, commit and ordinary push meaningful A; read quota immediately after push.
- [x] B. Write failing tests, then implement proposal, pending, atomic synthetic winner and reservation, exact idempotent replay of request, source/version/entry competition, generation closure, budget/attempt/retry bounds, revoke/unknown and synthetic dispatch checks. Enumerate both orderings for v1 fallback versus B same-model when source is historically excluded, and v1 versus B same-model when it is not; protect winner-bound zero-balance reservation. Review B independently, commit and ordinary push meaningful B; read quota immediately after push.
- [x] C. Write failing tests, then implement crash and bounded replay with exact harness anchor, event-time validation, zero pending after replay, monotonic ordinals, no partial state changes on invalid history, winner/reservation preservation and post-winner invalidation. Cover damaged/missing/duplicate/reordered events, malformed commands and boundary/negative cases; add dependency/I/O isolation verification. Review C independently, commit and ordinary push meaningful C; read quota immediately after push.
- [x] D. Perform independent whole-slice spec and quality review, resolve findings, run appropriate focused tests and one fresh full suite on the final source, report exact commit and outcomes; no claim of production readiness. Update the corresponding existing progress ledger without overwriting prior evidence.

If a spec contradiction, required trust root, worktree access, quota threshold or failed check prevents safe progress, preserve work and report precisely; never fabricate tests or use synthetic evidence as production authority.

Final verification at 2f6ebda: independent whole-slice review PASS; focused 343 passed; full-suite repeat 4712 passed, 1 xfailed. The first full suite had 4711 passed, 1 failed, 1 xfailed; initial storage preflight failed for an unproven reason. Preserve both JUnit reports in the progress ledger. A previously interrupted run also reported a test-owned SQLite cleanup error; its known process group exited. These results do not prove production readiness.
