# P3 private SQLite cross-version commit implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use subagent-driven development with TDD, independent stage and whole-slice review.

**Goal:** Validate local cross-version unique commit and complete replay using real private SQLite WAL and independent processes.
**Architecture:** Tests-only sibling codec/store consume finite model commands. Each owned transaction fully replays and cross-checks projections before committing events/winner/reservation/resources together; no adapter to production.
**Tech stack:** Python standard library sqlite3/json, existing pytest, owned local /tmp SQLite and independent Python processes.
**Spec:** `docs/superpowers/specs/2026-11-14-p3-durable-cross-version-commit-design.md` including reviewer amendments.

## Global constraints

Preserve existing pure model and production code. Full v1/unknown reconciliation unchanged; no credentials/Provider calls, dependency installs, deployment or main merge. Only test-owned databases; no recovery by deleting DB/WAL, no production path/adapter. All results remain marked synthetic and non-authoritative. At most 64 events, 2 sources/requests/generations; event 16 KiB, total event 1 MiB, projection 2 MiB. Quota before dispatch and after each substantive push, halt expansion if used >=50% or unavailable; daily max 5 completed tasks. Preserve existing snapshot test status-only flag and all prior evidence.

## Review focus

1. Exact request replay after response loss and after unknown/revocation must preserve history once, while any new unauthorized mutation remains blocked.
2. Every database read/projection/winner/resource check sees a single transaction snapshot; no pre-lock stale decision.
3. Malformed or inconsistent schema/codec/history refuses without logical repair; coherent total rollback is explicitly outside internal consistency proof.
4. Precommit SIGKILL gives no partial state; postcommit loss gives complete state and duplicate replay. Cut points use ack/barriers, not sleeps.
5. SQLite 3.45.1 constrained no-checkpoint workload and trusted test namespace are limitations; no production isolation or durable external authority claims.

## Task 1: Durable cross-version commit and replay prerequisite

### Stage A: Strict codec and private store lifecycle

**Files:** create `tests/support/dual_view_sqlite_codec.py`, `tests/support/dual_view_sqlite_store.py`, `tests/unit/runtime/test_dual_view_sqlite_store.py`.
**Interfaces:** finite `encode(value)->str`, `decode(text)->value` using exact registered types; `create_store(path:Path)->None`; context manager `SqliteDualViewStore(path)` and `read()->ModelState`.

- [x] Write failing behavioral tests for exact roundtrip, duplicate JSON keys, unknown tags/fields/versions, bool-as-int/non-finite/depth/size rejection; immutable input isolation.
- [x] Run RED, implement only required codec.
- [x] Write RED tests for exclusive create, missing/open-existing/foreign DB/schema/version refusal and verified empty read; implement lifecycle and WAL/FULL/bounded timeout settings.
- [x] Independently review A and fix findings; normalize own new files, focused tests, commit and ordinary feature push; immediately check quota.

### Stage B: One-transaction events, winner and resources

**Interfaces:** `apply(command,*,hook=None)->(ModelState,Observation)` accepts only fixed persistent commands; `commit(envelope,*,hook=None)->(ModelState,Observation)`, owns local Prepare+Commit; `read()` always fully verifies.

- [x] RED: source registered, separate stop/effect YES facts, v1 history unchanged, B proposal/commit produces one winner/reservation and debit; exact repeat unchanged; altered envelope conflicts; other version/entry superseded according to model.
- [x] RED: unknown/closed generation/stale source/expiry/limits block; winner retained after unknown but no new winner; wrong source/fences invalid; all event/projection/index rows match replay.
- [x] RED: exception after events/projection/index update rolls back; malformed/gapped events, changed projections/resources/winner and foreign schema refuse without repair. Add bounded hooks by spec and preserve primary exception when cleanup fails.
- [x] GREEN, independent B review, focused tests, second substantive commit/push; quota immediately after push.

### Stage C: Actual process races, SIGKILL and reopen

**Files:** create `tests/integration/test_dual_view_sqlite_processes.py`; optional `tests/support/dual_view_sqlite_process.py` fixed-purpose test harness.

- [x] RED/GREEN: real independent processes with opened/locked/phase ack/release protocol, shared bounded startup deadline, verify actual busy lock before deterministic loser proceeds; both winner orders and simultaneous race, exact duplicate/different envelope, independent source and unknown/generation first-writer races.
- [x] RED/GREEN: SIGKILL only owned child after after_begin/after_events/after_projection/after_indexes/before_commit/after_commit acknowledgement; every precommit cut old state, postcommit complete state and retry once. New process opens/validates entire DB after all writers exit; pending empty.
- [x] Set wal_autocheckpoint=0 on every connection and keep parent connection across races/cuts, no manual checkpoint until all writers gone; record runtime/compile options and filesystem. Verify no production reverse dependency or Provider/network access; do not modify old pure model.
- [x] Independently review C and resolve findings; focused tests, third substantive commit/push; quota immediately after push.

### Finish

- [x] Independent whole-slice spec/quality review, fixes with TDD, exact final source verification.
- [x] One final full pytest with explicit isolated PYTHONPATH and distinct JUnit in the same existing progress ledger. Do not modify source or duplicate suite while live; investigate any failure, preserve all red evidence.
- [x] Record final local/remote SHA, actual coverage and limitations, completed count and next P3 gates in progress.md. Mark only this local persistence prerequisite complete; do not claim P3 complete or activate production retry.

Final validation: combined source 2a684d9426cfd3a5b622979ed85ffa8706ffbc24, independent task/stage/whole reviews PASS, full pytest 5042 passed / 1 xfailed / 28 JUnit record_property compatibility warnings in 1441.11s. JUnit p3-durable-wire-full-2a684d9-20261115.xml retained in the existing progress ledger. This completes this bounded task only; full P3, production admission/stop/settlement authority and staging custody remain open.
