# Explicit recovery coordination: prerequisite slice evidence

This records the recovery-to-admission/preparation prerequisite, not complete
P3, general Worker recovery, production Provider authority, or V1 readiness.
`ControlPlaneApplication.list_recovery_opportunities` returns bounded durable
metadata. The host must explicitly re-supply the exact source task text to
`continue_recovery`; one checked admission transaction consumes existing
Scheduler authority, and a separate current-authority transaction prepares the
exact successor's `WorkerTask`. These APIs never launch a Worker, fetch a
credential, or contact a Provider. Unknown outcomes retain holds and withhold
task authority; historical consumption is not live dispatch permission.

## Identity and current gate status

- Implementation base: `621cacd7931725ef7c7bf97132192925bc32e0f8`.
- Task 4 base: `320a5202fc436e03ff9b7a5eb2584ca2d3e28716`.
- Frozen tested source commit: `f39de87df674d84cb552fe1e84dcf7e659b6f375`.
- Frozen `src` Git tree: `ab32e2b3bdaa96cc7245de24217a238ba00a25f8`.
- Frozen `tests` Git tree: `266d65e7adfeb6533276b8ae152daad186298a2e`.
- Per-file SHA-256 manifest: 197 tracked source/test files;
  `eb90f1a4c7e2026f432158eb49617996f1a8f8bbe9afd9351c3578040374599c`.
- Git path/mode/blob manifest:
  `452cdda206df9b69fe7eedba0623d63407f945826ea278eddc96a9ab6d923c35`.
- Final documentation/reviewed branch HEAD: **pending root commit/push/review
  bookkeeping**. Documentation changes must preserve the frozen source/test
  trees and manifests above.
- Final verified remote `main` full SHA: **pending root integration**. No merge
  or `main` push has occurred at this evidence checkpoint.
- Independent Task 4 review and broad whole-slice review: **pending**. This
  document does not mark Task 4, the plan, or full P3 complete.

All private logs, JUnit, coverage data, manifests, the wheel checker and wheel
are retained under this plan's ignored directory:
`.superpowers/sdd/2026-10-23-p3-recovery-coordination-implementation/`.
The detailed failure/command/self-review history is `task-4-report.md` there.
No sibling plan's scratch evidence was inspected or changed for this task.

## Process and crash acceptance

Task 4 A (`b183b36fc7784f45ed41667dc02bb05c4d00a4d1`) and B
(`f39de87df674d84cb552fe1e84dcf7e659b6f375`) are root-pushed and remotely
verified. Their 12 real process cases cover:

- Two independent spawned ready application/store owners contending for one
  authorization, with same/conflicting input and observations. Full durable
  rows must equal the winner's snapshot captured while holding the outer lock;
  exactly one successor/consumption/reservation/Agent/slot remains.
- Cancellation, durable Provider/model health version changes, and a budget
  hold winning the outer lock. A losing continuation cannot write successor
  state or resources from stale observations.
- Parent-issued `SIGKILL` after real nested admission before outer commit,
  after outer commit before reply, and before the separate preparation lock.
  Reopen requires either complete rollback or the exact same committed
  successor, with explicit missing/changed/exact input retries and no duplicate
  holds. The parent asserts actual `-SIGKILL` exit and bounded reaping.
- Dispatching and unknown Provider journal calls on the consumed successor,
  followed by two independent fresh spawned reopens each. Exact-input
  continuation remains `reconciliation_required` with no task, unchanged
  durable state, full holds and zero Broker/transport/Worker/launch attempts.

Literal hand-checked successor holds are **15 minor currency units and 1500
tokens** (1000 input + 500 output at 10000 minor/million). Unknown/used amounts
stay zero in these known-failure/reopen scenarios; the explicit independent
budget mutation holds only its intentional 1 minor/1 token. Route, reservation,
Agent, lifecycle Attempt, slot and original source input commitment all bind.

A's focused checkpoint passed **540 tests, zero failures/errors/skips**;
B's final focused checkpoint passed **336 tests, zero failures/errors/skips**.
These initially passing cases characterize existing production behavior.
Restored mutations independently demonstrated sensitivity: omitting current
budget/health checks caused two forbidden-write failures; omitting the current
Provider journal preparation gate caused two forbidden-preparation failures.
These are mutation evidence, not missing-feature RED or product defects.

All unsuccessful attempts are preserved. A initially had one combined
collection error from duplicate test module basenames, resolved by a narrow
integration package marker. B initially had five IPC fixture failures from
pickling immutable event mappings, corrected to plain owned metadata; its next
run had two incorrect historical-inventory/missing-input expectations, corrected
against the existing contract. No production serializer or live gate was
relaxed. B's corrected behavioral run passed 5 cases before final regressions.

Privacy assertions scan main DB/WAL/SHM where present, while open and after
close, plus redacted startup/opportunity/result/task/error representations and
captured logs/stdout/stderr. A retained-artifact scan checked 20 Task 4
log/XML/report files with zero raw synthetic-input matches. Durable input,
request and body hashes remain expected commitments; raw task bytes remain
memory-only and are never used in identifiers or test IDs.

After final verification and documentation review, the C privacy scan checked
**44 retained Task 4 text artifacts and changed documents**, again with zero
plaintext matches. It covers final test/coverage/isolation logs and JUnit,
environment/build/failure records, manifests and the report, plus all six
changed documentation files. The test-source fixture constant itself is not a
diagnostic output; hashes are deliberately permitted commitments.

## Final verification

The single final full coverage run passed **2860 tests, zero failures/errors/
skips, in 864.32 seconds** (exit 0). Same-run statement coverage is **90.55%**:
19630 statements, 1856 missing; the >=90% gate passed (exit 0). This includes
the new integration namespace alongside all existing tests. The subsequent
eight-file required real isolation matrix passed **117 tests, zero failures/
errors/skips, in 143.78 seconds** (exit 0). Both JUnit files independently
confirm those exact counts. No duplicate full run was performed on unchanged
source. All 197 per-file hashes and the Git path/mode/blob inventory still
match after verification; `git diff --check` passed.

The test runtime is the existing Python 3.12.3 at
`/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`, with
`PYTHONPATH=src:.`. Environment: Ubuntu 24.04.4 LTS under WSL2 x86_64,
kernel `6.6.87.2-microsoft-standard-WSL2`, systemd
`255.4-1ubuntu8.18`, uid/gid 1000. Relevant runtime packages: pytest 8.4.2,
coverage 7.16.1, Pydantic 2.13.5, PyYAML 6.0.3. No dependency installation,
credential access or paid Provider call was performed.

`compileall -q src tests` passed and `pip check` reported **No broken
requirements found**. The project runtime has build 1.6.1 but no setuptools or
wheel; that diagnostic warning is retained, and no known-incomplete build was
attempted there. Packaging reused the already available private Python at
`/tmp/maestro-p0-task6-wheel.qDhql9/venv/bin/python`: build 1.4.0,
setuptools 68.1.2 and wheel 0.48.0, with no installs. Its upstream `bdist_wheel`
FutureWarning is retained; build output is not described as warning-free.

The private candidate wheel is
`task-4-c-wheel/multi_agent_orchestrator-0.1.0-py3-none-any.whl`, SHA-256
`e8ec5e2a83a08d537e0b74ce031ec90c283f186002193a6c2d9723eb87527a1b`.
Its 102 unique archive entries consist of exactly **98 tracked Python modules**
with byte-for-byte source matches and the four expected dist-info files.
There are no tests, docs, bytecode, private artifacts or extra modules. Python
requirement, pydantic/PyYAML runtime requirements and `py3-none-any` tag were
checked. This proves packaging for the frozen source, not deployment or
production delivery.

An initial environment package-version probe failed in shell parsing before
Python ran; a faithful failure record is retained as
`task-4-c-probe-failure.txt`. The corrected direct environment probe succeeded.
This was a command-quoting failure, not a product/test failure or reason to
repeat the full suite. All test fixture and mutation failures remain separately
accounted above and in the detailed report.

### Retained evidence hashes

Paths below are relative to the private evidence directory stated above.

| Artifact | SHA-256 |
| --- | --- |
| `task-4-c-full.log` | `a359aeec11ac66268950e76904d77a77507a87df7088374ae267004c59f54ece` |
| `task-4-c-full.xml` | `7164db6aa36aaf8c14c97916a7c10f5b0bfe2d1871a478bf735dfb080def6edc` |
| `task-4-c.coverage` | `c49a1e5a3bb26fb4aa38fdc97199321ad793f358405732e4dabb0083a65f0d38` |
| `task-4-c-coverage.log` | `644013fb2d98604f7e639b9e71514dbfb43ee399b8d9d7391bc6cd5e8ba3aa03` |
| `task-4-c-isolation.log` | `20e8d6882783b240722a8e47389706db23d354a1b5d7a22df83d64b266df8f6d` |
| `task-4-c-isolation.xml` | `d6c47415f54e83474cf70b854e74d6b7244ac6f67f30d4545c8ef5fa1320c136` |
| `task-4-c-junit-summary.log` | `c0ac1473ebff7f1714b1ce30e3fe3c1aa552897f4c5ddc4a52a59ef3c869a90f` |
| `task-4-c-manifest-final.log` | `7f008f1e6cf769711fb638a8b622816955cbbb9d5350e6dba97ed640183af5af` |
| `task-4-c-compileall.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `task-4-c-pip-check.log` | `9261363b733079a641c2e4cc9bc46ffa1d8336945a87f807b6cf68847dbc9b09` |
| `task-4-c-environment.log` | `cf740f00aad06eb4b82f2a5231a274e8cc173d65618dd82a7a5f7de8c49a9ecf` |
| `task-4-c-build-environment.log` | `692be0ca8d62432cdd0e82ce4a209309a761f9e5cd70526deecd6899a4549357` |
| `task-4-c-build.log` | `172935bd146c1d28966538537224b100ad3633d28050ed0517ef45d33c34a625` |
| `task-4-c-wheel-check.log` | `d877e0a3c62b14112518f50a614f67c783a8281901cdb47da8f31ec3c1553860` |
| `task-4-c-check-wheel.py` | `9947a9aa653b2e7e3253bad1626e77333ae85fcd961080d86c9327ade47d98f9` |
| `task-4-c-probe-failure.txt` | `686363a2724cbd1cd4a6748a140a13cec862a1f9121ded55258ff77f993b2768` |
| `task-4-c-rulings.log` | `964c8199405f27054a6f11e4295522221b2ffdd0d48cc80333e437fc06dd8e68` |
| `task-4-c-privacy-final.log` | `e9280b40d08e9926e7668adbb163a605edcb06567a9cb8530d51a6f0ad7afcff` |

The source/test manifest and candidate wheel hashes are recorded in the
identity/package sections. Original A/B failures, focused results, mutation
failures and privacy scan hashes remain in the detailed Task 4 report; they
were not overwritten by these final gates.

### Reproducible commands

From the isolated worktree, `PY` is the test Python above; `BUILD_PY` is the
existing private build Python. Logged commands use `set -o pipefail` and `tee`
with distinct retained names. `EVIDENCE` is this plan's ignored directory.

```bash
git diff --exit-code -- src tests
git ls-files -z -- src tests | LC_ALL=C sort -z | xargs -0 sha256sum
git ls-files -s -- src tests
COVERAGE_FILE="$EVIDENCE/task-4-c.coverage" PYTHONPATH=src:. "$PY" -m coverage run -m pytest -o addopts= -q --junitxml="$EVIDENCE/task-4-c-full.xml"
COVERAGE_FILE="$EVIDENCE/task-4-c.coverage" "$PY" -m coverage report --fail-under=90
PYTHONPATH=src:. "$PY" -m pytest -o addopts= tests/integration/test_p0_workspace_acceptance.py tests/integration/test_overlay_candidate_launcher.py tests/integration/test_secret_broker_process.py tests/integration/test_systemd_provider_sender.py tests/integration/test_systemd_launcher.py tests/integration/test_workspace_worker_execution.py tests/integration/test_workspace_isolation_admission.py tests/integration/test_workspace_removal_recovery.py -q --junitxml="$EVIDENCE/task-4-c-isolation.xml"
"$PY" -m compileall -q src tests
"$PY" -m pip check
"$BUILD_PY" -m build --wheel --no-isolation --outdir "$EVIDENCE/task-4-c-wheel"
"$PY" "$EVIDENCE/task-4-c-check-wheel.py" "$EVIDENCE/task-4-c-wheel"
sha256sum -c "$EVIDENCE/task-4-c-src-tests.sha256"
git diff --check
```

## Security branch self-review

Reviewed all five changed production files from implementation base to the
frozen source, including the full two new recovery modules. This self-review
does not replace the pending independent reviews.

| Branch/boundary | Observed control and verification |
| --- | --- |
| Inventory/history | Strict identities, event payload hashes/schema, historical lifecycle/context, frozen config/contract/input binding, classification evidence/counters and allowed plan shapes are validated. Legacy missing commitment blocks continuation; malformed history produces detached bounded errors. Consumed route identity stays historical. |
| Unknown calls/effects | Exact Provider journal/accepted-route bindings, unresolved and unaccounted success states are checked under admission/preparation locks. Unknown holds remain and explicit task preparation is withheld. Fresh process tests and restored mutation protect the current-journal branch. |
| New admission | Exact raw-input commitment, same-node generation/counters and persisted allowed authorization feed real Router/Scheduler under one outer transaction. Current budget/health observations are rechecked; losing processes leave no writes. |
| Untrusted observation data | Exact concrete types/storage/keys and trusted concrete timezone identities precede revalidation; subclasses/copied extras, executable timezone or scalar/key callbacks fail without execution. Errors are detached and bounded. |
| Committed replay/preparation | Existing consumption is resolved before new observations; input still must match. Separate running/latest/active/unexpired/unknown checks use the exact owned Scheduler/store and callback-free UTC sampled after locking. Shared WorkerInputService context/frame validation remains authoritative; no public time override. |
| New-child compatibility | Run replay accepts only the already persisted reviewer/director role authorization shape with exact source/evidence/role bindings; same-node consumption still requires an authorized same-node plan. No child node is created here. |
| Application/privacy | Public methods require committed readiness. Constructors, inventory and continuation have no execution boundary; raw text remains memory-only. Hook/kill/guard code lives only in tests. |

## Rulings carried into this acceptance

The following are all six `Ruling:` entries in this plan's retained
`progress.md` at this checkpoint, in ledger order. Line references identify
the recorded decision; the source ledger is retained, not cleaned up.

| Ledger reference | Ruling and reason | Cost if wrong / retained boundary |
| --- | --- | --- |
| `progress.md:90` | Include the narrow new-child replay compatibility fix because Controller and event contract already produce that authorization shape. | Could accept inconsistent authorization shape; exact evidence/source/role binding and prohibition on same-node role-plan consumption remain. |
| `progress.md:146` | Preserve Router's exhausted-model conservative block rather than force a later same-model retry through new admission authority. | A later bounded retry may remain listed ready but unable to route; unresolved liveness is an explicit full-P3 gap. |
| `progress.md:212` | Use identity-only primitive-leaf type checks in the same non-executing declared-data boundary after a demonstrated metaclass callback. | Could reject valid scalar representations or widen the fix surface; exact built-in primitives and safe timestamp positives remain supported. |
| `progress.md:223` | Require built-in dict storage and built-in str keys before field-name membership, removing caller-key set construction. | Rejects previously tolerated copied-model storage/key subclasses; declared built-in data compatibility is preserved. |
| `progress.md:254` | Use an owned callback-free recovery input path with trusted UTC after locking and shared validation/exact Scheduler identity. | Could cause validation drift or reject dependency substitutions; legacy public injected-clock behavior remains covered. |
| `progress.md:380` | Add only `tests/integration/__init__.py` to resolve the required integration/unit basename collision without a global pytest import-mode change. | Changes integration import qualification; focused runs and final full suite must demonstrate compatibility. |

The latest direct human authorization, recorded in the same ledger, supersedes
the initial plan's no-merge restriction only after Task 4 and whole-slice
acceptance. Root alone may then merge the codex branch into `main` and
ordinary-push without another confirmation, preserving unrelated files and
untracked entries. No force-push, PR, deploy, evidence/branch cleanup or paid
Provider work is authorized. Substantive conflicts or branch protection remain
stop boundaries. The final remote `main` full SHA must be verified and reported
after that operation; it is intentionally pending above.

## Remaining gates

Independent Task 4 and whole-slice reviews, root's C push/quota checkpoint,
review closure and verified ordinary `main` integration remain outstanding.
General resumed Worker execution and lifecycle interruption/restart, production
credential backend, authoritative Provider delivery/charge evidence and cost
reconciliation, full crash coverage and full P3/V1 acceptance remain open.
The later same-model retry/exhausted-model mismatch is safely blocked but not
solved here. No automatic transition to the deferred P5 plan is implied.
