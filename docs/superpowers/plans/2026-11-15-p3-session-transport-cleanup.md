# SandboxSession Transport Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Attempt to close each already-owned local transport resource once after any collector failure, preserve the selected exception, and prevent failed waits from returning cached success or repeating resource actions.

**Architecture:** `_collect` maintains a finite identity-deduplicated close ledger from before selector setup. `wait` uses the same local error priority, preserves existing dependent teardown eligibility, publishes a result after successful eligible cleanup, and stores only a Boolean on failure. All behavior remains inside the existing session serialization boundary.

**Tech Stack:** Existing Python standard library and pytest; real owned `os.pipe`/FileIO/selector fixtures, finite fake transport/staging/owner collaborators. No new process or dependency.

**Spec:** `docs/superpowers/specs/2026-11-15-p3-session-transport-cleanup-design.md`, independently reviewed PASS in the sole P3 ledger `session-cleanup-design-review.md`.

## Global Constraints

- Modify only `src/orchestrator/isolation/launcher.py` in `SandboxSession._collect/wait` and small private helpers plus `tests/unit/isolation/test_launcher.py`. Do not alter public signatures or constructor parameters, receipt validators, `_retire_session`, staging allocators or pathname cleanup qualifications.
- Complete v1 validation and prepared/unknown/Provider holds remain unchanged. No Provider/systemd/native child, credentials, installation, deployment or main integration.
- Control flow is any `BaseException` which is not an `Exception`. Primary control flow wins by exact identity; otherwise the first cleanup control flow wins; otherwise the original ordinary primary or first ordinary cleanup error wins. Remaining independent cleanup is attempted.
- Fixed notes are exactly the finite deduplicated literals in the spec; use unbound `BaseException.add_note` and protect against insertion failure. Do not inspect/copy arbitrary existing notes or expose nonselected secondary messages/chains.
- Record close attempts before invoking close and deduplicate by object identity, including all normal EOF/input-completion paths. Never retry an attempted close or a reused numeric FD.
- Keep output bytes, caps, chunks, input_written, cancellation behavior, stop-grace values and healthy cross-thread serialization. No global resource/time/stop claim.
- A fresh poll alone supplies the existing exit gate. Poll failure supplies no deletion eligibility. Qualified discard must succeed before owner retirement; `cleanup_on_termination=False` preserves its existing handoff path.
- Delay `_result` until eligible cleanup succeeds. Failure stores only a Boolean; later calls raise new `IsolationUnavailable('sandbox_session_wait_failed')` without collect, cancel, wait, poll, path cleanup, retirement or receipt resampling. Same-thread active reentry refuses locally; a caught nested refusal does not poison healthy outer execution.
- Use only the existing isolated worktree and sole P3 ledger. Preserve unrelated snapshot status-only M. Parent owns ordinary commits/pushes and quota checks; no subagent commits/pushes or full suite.

## Review Focus

1. Constructor failure before a selector handle exists still attempts every known eligible pipe close (A).
2. A close which fails before or after actually closing cannot make later cleanup close a replacement FD (C).
3. Ordinary primary plus multiple cleanup controls selects the first control, while an original custom control remains identical (A/B).
4. A pending successful result followed by poll/discard/retirement failure never becomes a successful repeated wait (B).
5. Reentrant cleanup and concurrent second wait obey the failure latch without a second reader or repeated deletion/signal (B/C).

---

### Increment A: Collector ownership and exception-safe close attempts

**Files:** Modify `launcher.py` `_collect` and private finite local helpers; extend `test_launcher.py`.

**Consumes:** Existing stdout/stderr and existing input-owned stdin, existing selector loop, `_wait_lock` serialization.
**Produces:** Same `SandboxResult` or the spec-selected original exception after all finite close attempts. Small private helpers may be reused by B; no general cleanup framework or public API.

- [ ] Write behavioral REDs using own real pipes for selector construction/registration/set_blocking failure, empty-input/EOF close failure, body error plus selector/pipe close failures; distinguish missing-interface/setup errors from behavioral RED.
- [ ] Verify exact once per object, remaining eligible attempts, actual owned FD closure where close succeeds, selected object identity and finite notes. Constructor failure does not claim closure of hidden unreturned handles.
- [ ] Implement close records before setup and before every close, optional owned selector, unified local priority and best-effort fixed notes. Keep the existing loop's limits and cancellation flow.
- [ ] Run focused tests with explicit worktree PYTHONPATH and existing venv: `python -m pytest -o addopts= -p no:cacheprovider tests/unit/isolation/test_launcher.py -q`. No actual systemd/native/Provider calls from new tests.
- [ ] Self-review, write `session-cleanup-a-report.md` in the sole ledger, stop writes for independent spec/quality review. Parent records real commit and ordinary feature push, then immediately checks quota. A is a partial milestone, not a completed public wait fix.

### Increment B: Public wait failure and cache discipline

**Files:** Modify the same `launcher.py` wait/private helpers and `test_launcher.py` only.

**Consumes:** A's finite close/error machinery and existing receipt/cleanup conditions.
**Produces:** Healthy identical-object result replay, delayed result publication, finite failure latch and local same-thread reentry protection; no retained exception/traceback in session state.

- [ ] Write/run genuine REDs for outer cancel/process-wait/poll/staging/owner failures and post-result cleanup then repeated wait; require zero repeated side effects and no fabricated receipt.
- [ ] Implement primary-versus-secondary rule across both methods, deduplicate fixed note categories without traversing arbitrary existing notes; preserve exact first selected control object, including custom direct BaseException. Test unbound-note insertion failure.
- [ ] Keep independent cancellation and bounded process-wait attempts after body failure, with existing TimeoutExpired semantics. Preserve fresh poll and discard-before-retire dependencies, including `cleanup_on_termination=False`.
- [ ] Publish pending result only after eligible teardown succeeds; latch failure before error cleanup, fail repeated calls before resource actions and under the wait lock, reject only same-thread active reentry while healthy other threads serialize. Do not cache the original error or pending result.
- [ ] Run focused affected launcher tests and concrete consumer regressions warranted by changed wait behavior. Write `session-cleanup-b-report.md`, independent review/fixes, then parent commit/push/quota.

### Increment C: Real FD-reuse and compatibility evidence

**Files:** Add meaningful boundary regressions to `test_launcher.py`; correct only newly demonstrated defects in the same scoped launcher code.

**Consumes:** Reviewed A/B behavior.
**Produces:** Actual descriptor reuse/alias and thread/replay evidence, not a synthetic stop witness.

- [ ] Use own real pipes and controlled duplication: close attempt may fail before or after actual close; place a fixture replacement at the old FD number where applicable and prove it survives later collector/wait cleanup. Tests explicitly clean their own sentinels without touching unrelated descriptors.
- [ ] Cover aliased pipe objects, caught same-thread reentry, bounded barrier-driven second thread, healthy identical result replay, bytes/status/input/output cap preservation, false cleanup flag and failed-poll nondeletion. Do not relabel already-green guards as RED.
- [ ] Run focused changed tests plus existing related launcher/overlay/GeneralWorker consumer tests as justified, without real Provider/systemd calls. Write `session-cleanup-c-report.md`; independent scoped review and parent third substantive test/evidence commit/push/quota.

## Whole-task acceptance

- [ ] Independent whole-slice review compares final scoped changes against the spec, tests, all note/error/caching/receipt boundaries and real FD-reuse evidence.
- [ ] One fresh full suite on final reviewed source with explicit isolated PYTHONPATH, persisted JUnit/log in the sole ledger, no concurrent full run or source changes. Inspect all failures and strict expected failure separately; native staging's existing UNSUPPORTED mandatory evidence cannot be hidden or called green.
- [ ] Record exact source HEAD, three substantive pushes, reviews, targeted/full results and remaining boundaries. Mark this task complete only on its truthful applicable acceptance; keep native Stage 0 and P3 overall incomplete where required evidence is absent. No automatic main merge or P5 transition.
