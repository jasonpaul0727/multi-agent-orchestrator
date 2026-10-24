# General Worker recovery preflight and acceptance map

This records preparation for the new design, not implementation acceptance.
The human authorized design, independent review and direct development on
LA2026-10-24, explicitly skipping further questions. The existing subagent
development/TDD/task-review/whole-slice-review workflow is retained.

## Source and environment

- Product source base: `36423cfdf9f7d4c6a9e755bc555d3a9999b0953d` on
  `codex/p3-systemd-termination-receipts` in the existing isolated worktree.
- Its committed tree matches previously integrated main
  `2ab247db388d326cae60b2a3d58ef22117dbec3c`; that main integration belongs
  to the already-closed prerequisite. This work has no main integration authority.
- Design commit/push: `a5bfd34`; complete plan commit/push: `c319f3f`.
  Both ordinary pushes succeeded; authoritative remote codex matched c319f3f.
- Existing Python virtual environment and SQLite/systemd isolation are reused.
  No dependencies installed, no source changed for this preflight.
- `systemctl --user is-system-running` reported `degraded`, which by itself
  neither certifies nor disproves the required isolation profile. Task1 must
  run the real new isolation tests with zero skips before acceptance.
- Main's unrelated untracked files, all branches/worktrees and recovery
  evidence remain preserved. No PR, cleanup, paid call or real credential.

## Independent design review

One architecture/builder reviewer read the complete design and concrete
existing APIs. Three important buildability questions were resolved:

1. Completion alone may consume its own captured known_success call while
   budget remains reserved. Exact call/context/usage/sender-stop/capture
   bindings must match; every unrelated hold remains blocking. This never
   grants credentials or another invoke.
2. An owned same-store authority checks current Scheduler state after
   credential acquisition and atomically claims Provider intent. Intent
   commit is the in-flight authorization point; earlier withdrawal prevents
   send, later withdrawal retains effects/holds and blocks acceptance.
3. The general accepted checkpoint joins Scheduler terminal/usage/release
   in the same transaction. Lost replies replay exact historical evidence
   without a released active lease or fresh completion time/usage.
   Private ArtifactPublished is an earlier durable prerequisite.

The same reviewer reread the updated design: **nothing important left; zero
remaining important questions**. This is a design gate only. The full report
and scoped follow-up remain in this plan's own recovery workspace.

## Compatibility baseline, not new feature evidence

The focused source-base command used `PYTHONPATH=src:.` and the existing
virtual-environment Python to run:

- `tests/unit/runtime/test_worker_process.py`
- `tests/unit/runtime/test_worker_output.py`
- `tests/unit/runtime/test_attempt_execution_coordinator.py`
- `tests/unit/models/test_provider_call_journal.py`

Result: **345 passed in27.07s**. JUnit reports345 tests,0 failures,0 errors,
0 skipped. Raw log/XML are retained in the new plan's workspace. No new
Worker, recovery or Provider acceptance claim is made from this baseline.
Read-only path-probing diagnostics encountered nonexistent filenames; no
source/test failures or mutations resulted. A raw XML display was oversized;
use parsed aggregate attributes when reading it again, not a repeated run.

## Acceptance ownership

| Deliverable | Owning task | Required evidence |
| --- | --- | --- |
| Bounded request/complete Worker, prepared launch/stop |1|Hostile-input TDD and actual SIGKILL/empty cgroup, old blocked Worker unchanged|
| Journal, phase holds, post-credential send authorization, atomic terminal |2|Exact replay/identity/race/rollback/public-bypass regression tests|
| Explicit useful execution and no-dispatch startup/recovery |3|Signed private capture, frozen acceptance, unknown/missing-proof holds|
| Fresh host process after real Worker kill |4|Real artifact + independent success + exactly1 Provider invocation|
| Unknown Provider no-resend across restart |4|Zero broker/transport/Worker activity for blocked states and rotated IDs|
| Slice completion |4|Full suite >=90% statement coverage; required real isolation0skips; compile/pip/wheel inventory and independent final review|

Production Provider delivery/charge authority, arbitrary tool-agent execution,
the full lifecycle matrix and later retry/Router exhaustion liveness remain
separate full-P3 gates. Offline Provider/usage fixtures cannot close them.

LA Oct24 starts1/5 completed tasks. NewTask0 counts only after its third
substantive push and independent task gate. The observable usage window at
the last push was23% used/77% remaining; this is not total token balance.
Recheck before dispatch and after every push; used>=50 or unreadable usage
stops further development. Retain progress and avoid duplicate dispatch.
