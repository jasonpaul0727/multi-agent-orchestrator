# Design: exception-safe SandboxSession transport cleanup

Status: Independent design review PASS; implementation not yet started.
Existing user authorization permits direct development after independent review
PASS without another approval question.

## Scope and concrete defects

Correct the finite selector/pipe resources already owned by SandboxSession and
its wait failure/cache behavior. Production scope is only _collect/wait and small
private local helpers in src/orchestrator/isolation/launcher.py; regressions are
in tests/unit/isolation/test_launcher.py. No public signature/constructor change,
new generalized cleanup framework or dependency on the parallel native A2 work.

Current source has four concrete gaps:

- Selector construction/registration precedes _collect's try/finally (589-608),
  so early errors skip owned cleanup.
- Selector.close or one pipe close prevents remaining closes (663-668).
- wait's cancel/process.wait/poll/staging/retirement errors can mask the original
  failure (569-585).
- _result is cached before final cleanup (567): cleanup may fail on first wait,
  then repeated wait silently returns that cached result (547-548).

This slice needs only own real pipes/selectors and fault injection, not new
processes, systemd, namespaces, Provider calls or credentials. It does not solve
late-session custody, staging pathname safety or stop/resource authority.

## Finite owned-close contract

Keep the existing _wait_lock as the sole normal collector/reader serialization.
After acquiring it, capture each eligible stdout/stderr and input-owned stdin
reference into a local close record before fallible selector setup. Preserve the
existing stdin eligibility; do not close unrelated caller resources. A returned
selector becomes owned immediately. A constructor which never returns supplies
no selector handle: do not claim to recover hidden constructor allocations.

Each record is object reference plus attempted flag. Deduplicate aliases by
object identity. Mark attempted BEFORE every first close, including EOF,
input-completion and empty-input closes. Finally never retries an attempted
close, regardless of its result or .closed. Do not reconstruct/retry numeric FDs
which may have been reused. Exactly-once attempt does not prove a failed close
completed.

Put selector creation, set_blocking/registration, the loop and every close in one
protected ownership scope. Independently attempt eligible selector, stdin,
stdout and stderr closes after any failure. Errors inspecting fileno/.closed or
unregistering cannot bypass the remaining close records. Preserve successful
byte collection, output cap/chunks, input_written, cancellation retries and all
stop-grace/timeout values. Do not add another reader, signal or resource.

## Unified failure selection and bounded notes

Control flow means ANY BaseException which is not an Exception, including a
custom subclass. Apply this rule through both methods:

1. Primary control flow wins by exact object identity.
2. Otherwise FIRST cleanup control flow wins by exact identity.
3. Otherwise original ordinary primary wins; with none, first ordinary cleanup
   failure wins.
4. Attempt remaining independent eligible cleanup before raising. Never convert
   failure into success or replace the selected object with a later error.

Setup/collection/receipt-observation failures are primary. Final cleanup after a
calculated pending result has no earlier primary. A selected _collect failure
keeps its primary role in wait's outer cleanup. Failure references remain local
to this call; no exception/traceback is stored in session state.

Secondary notes are fixed, deduplicated, at most one each:
`sandbox_session_selector_close_failed`, `sandbox_session_pipe_close_failed`,
`sandbox_session_cancel_cleanup_failed`, `sandbox_session_wait_cleanup_failed`,
`sandbox_session_poll_cleanup_failed`, `sandbox_session_staging_cleanup_failed`,
`sandbox_session_retirement_failed`, `sandbox_session_primary_superseded`,
`sandbox_session_secondary_failure`.

Use unbound BaseException.add_note, bypassing a subclass's add_note override.
Catch any BaseException from insertion without replacing the selected object or
recursively noting that failure. Never inspect/copy arbitrary existing notes or
include dynamic messages, class names, paths, payloads or raw tracebacks. Raise
the selected object outside secondary capture blocks with incidental chaining
suppressed (`raise selected from None`). Its identity/arguments remain; no promise
is made to redact private data already in that selected object's own arguments.

## Preserve existing teardown eligibility and dependencies

After a primary wait failure, attempt the existing single cancel and existing
process.wait(timeout=self._stop_grace_seconds); cancel failure must not prevent
this independent bounded wait attempt. TimeoutExpired stays secondary. Do not
add cancellation requests, extend deadlines or claim native hard interruption.

Keep the existing fresh poll gate. Poll failure is a cleanup error and supplies
NO deletion/retirement eligibility; never substitute old returncode/earlier poll.
Use the current LOCAL pending result for the same receipt checks before public
cache publication.

| Existing condition | Required action/dependency |
| --- | --- |
| Fresh poll says live, or poll fails | No staging deletion/retirement. |
| Fresh poll says exited, _scope_cgroup is None | Same existing _staging.cleanup once; no new retirement branch. |
| Scoped exit without pending termination_confirmed result | No discard/retirement. |
| Scoped exit + existing valid receipt + cleanup_on_termination=True | Existing _discard_staging once. ONLY success permits existing live-owner _retire_session once. Discard failure retains custody and must not fall through to retirement. |
| Same qualified result + cleanup_on_termination=False | Skip discard and preserve existing eligible owner-retirement/handoff behavior. Do not delete retained staging or invent a new receiver/authority condition. |

Owner lookup/retirement failures use the same selection rule. Retirement is a
dependent action, not unconditional finally cleanup. Do not retry a partially
applied retirement or claim its mutations rolled back. Do not change receipt
validators, _retire_session, _RetainedStaging, staging allocation or any pathname
cleanup qualification. Incomplete cleanup creates no stop evidence; independently
observed valid stop data is not retroactively declared false.

## Result publication, failure latch and local reentry

Keep calculated SandboxResult local until ALL currently eligible existing cleanup
succeeds. Only then publish that SAME object to self._result under _wait_lock.
Normal repeated wait returns the identical object without new side effects. A
normal result lacking a termination receipt remains the existing conservative
result; absence of proof alone is not a newly invented exception.

On any collection/receipt/teardown failure, set private finite _wait_failed=True
BEFORE further error cleanup. Do not persist the pending success or selected
exception, traceback, Future or arbitrary object graph. First call raises the
chosen original object. Later calls raise a new detached instance of EXISTING
IsolationUnavailable with fixed literal `sandbox_session_wait_failed`; no collect,
cancel, process.wait, poll, staging cleanup, retirement or receipt resampling.

Check this failed latch before lock acquisition and again under lock. A transient
private owner-thread ID, set/cleared while owning _wait_lock, also refuses active
SAME-THREAD wait reentry before lock acquisition with the same fixed error. This
is only local wait state protection: other threads retain existing serialization.
A caught nested refusal does not poison an otherwise successful outer call; an
escaping refusal enters normal failure selection. Private flags may initialize
on first wait; no constructor/API change is needed. Interrupted acquisition
before ownership begins is outside the new cleanup commitment.

The latch prevents repeated unsafe resource actions; it is not cleanup or actor
stop proof. Caller-owned raised tracebacks may remain live, and existing unknown
input/session/staging retention is neither removed nor globally bounded.

## TDD and three meaningful increments

All fixtures use real own os.pipe/FileIO/selector resources, finite synthetic
transport/staging/owner collaborators, explicit counters and bounded own-thread
barriers. No new child/systemd/Provider/native capability experiment. Synthetic
returncodes/receipts prove no stop. Existing held-pipe, no-live-transport-stop,
output-limit, prepared-launch and stop-grace cases remain compatibility controls.

1. **Collector ownership and close coverage.** Genuine RED for constructor/no
   returned handle, each set_blocking/registration boundary, empty-input/EOF
   close, collection failure, selector.close then each pipe close failing.
   Require every remaining eligible attempt and exact once per object. Implement
   the local ownership scope and selected-failure bookkeeping; do not yet claim
   the complete public wait/cache behavior fixed.
2. **Wait failure/cache contract.** Genuine RED for outer cancel/wait/poll/staging/
   owner failures, discard-failure retirement, and a valid pending result whose
   final cleanup fails but next wait returns old success. Implement unified rule,
   bounded notes, dependent retirement, delayed _result, failed latch and local
   reentry guard. Include custom direct BaseException, ordinary primary plus
   first cleanup control flow, multiple secondary controls, no-primary ordinary
   cleanup, custom add_note override and unbound-note insertion failure. Assert
   selected identity, no private secondary sentinel in formatted output, fixed
   detached repeated error and zero repeated side-effect counters.
3. **Real FD-reuse and compatibility evidence.** Add independently meaningful
   actual descriptor-alias/reuse regressions: after an owned close attempt,
   arrange a fixture replacement at the old FD number and prove later cleanup
   does not close it; test failure before and after real close. Verify handled
   same-thread reentry, serialized second thread, normal identical-object replay,
   cleanup_on_termination=False, unchanged bytes/status/limits and no deletion
   after failed poll. These are real boundary evidence, not copies of helper
   tests. If already GREEN, label characterization rather than fabricate RED;
   any newly found defect gets its own RED and smallest correction.

Each increment needs scoped independent review and a real ordinary feature-branch
push with the existing immediate quota check. Do not manufacture a third code
change or repeat tests to meet push count; the third increment must independently
add the real resource/replay evidence above. Whole-slice independent review and a
fresh final-head full suite precede task completion. No implementation plan or
source/test change is part of this design-only task.

## Review focus and limits

Review first-owned-resource recording, constructor-without-handle boundary,
object alias/FD reuse, selected error chaining/notes, pending result versus receipt
qualification, fast failed/reentrant paths versus cross-thread serialization,
discard-before-retire dependency and partially applied cleanup. The design fixes
the exception class/message, priority, latch and ownership decisions; no policy
choice is intentionally deferred to implementation. Real REDs remain unexecuted.

Only this finite local transport contract is corrected. Late to_thread results,
operation-lease custody, hidden post-Popen sessions, global staging/RSS/FD caps,
manager queue fencing, trusted stop/effect/settlement and old-writer continuity
remain open. Complete v1 validation and prepared/unknown/Provider holds remain
unchanged. This slice cannot complete P3 or prove staging pathname safety.
