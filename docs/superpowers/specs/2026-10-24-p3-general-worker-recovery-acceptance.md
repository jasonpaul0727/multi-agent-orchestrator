# General Worker recovery acceptance evidence

This slice enables bounded useful terminal-text work through fixed isolated
Workers, host-mediated Provider dispatch, private response/output artifacts and
independent verification. An application process can restart on the same Linux
boot, UID and concrete SQLite/artifact stores, receive the exact task input
again, and finish an already captured known-success response without another
Provider invocation. Ambiguous effects remain held and block new dispatch.

This is local acceptance evidence with explicitly offline Provider, credential,
sender-stop and trusted-usage fixtures. It does not establish online delivery,
actual billing or completion of all P3 work. An independent Task4 review of
883c99c..3c96b4b found no Critical, Important or Minor issues; its focused outside-diff check confirmed exact task-text equality. The later
whole-slice review found one Important cross-generation authority-fallback
issue; commit 280036a added the full-identity rejection and regression, and
the scoped re-review found no remaining findings. The required full run after
that fix exposed a legacy SQLite migration-order failure; commit d41d229
moved the context index after migration on both transaction paths and added a
migration assertion, and its scoped re-review found no findings. The final
full suite and review status are recorded below. Reviewers did not rerun tests.
No PR, main integration, deployment or P5 execution is part of this acceptance.

## Tested revision and provenance

- Task4 base: `883c99c16d9998697f0918f6b3c6843ef3172dc4`.
- A: `7cbb71b93ec1fdbeba42614be2e27a6bf5d9ab2c`, ordinarily pushed, reviewed by
  the controller as evidence: 65 focused tests, zero failures/errors/skips,
  135.60 seconds.
- B/C base: `0f2ea93b58ebfa1ab5a995841f77d0821d1d88cb`, ordinarily pushed;
  170 focused tests, zero failures/errors/skips, 331.92 seconds.
- C adds one actual Worker RuntimeMax timeout/restart case and this record.
  No production-source change was part of C itself; later review fixes are
  recorded below. The C-stage producer included the ownership cancellation fix `43e51ff`.
- Earlier full runs (including 3295 tests/90.76% at `0c4`) precede that producer
  fix and are retained as historical evidence, not current acceptance.

Original evidence is retained, ignored, in
`.superpowers/sdd/2026-10-24-p3-general-worker-recovery-implementation/` (PREFIX
below). `task-4-report.md` contains exact development iterations, commands,
per-run counts and distinct provenance. Initial missing-harness/import and
instrumentation/bookkeeping failures are labeled test-harness failures; they
are not claimed as production RED/GREEN defects. No correct product behavior
was sabotaged to manufacture a failing acceptance test.

The C-stage pre-review full run froze 220 tracked source/test/config files;
`task-4-c-full-linux.source-hashes.json` records every SHA-256. The test/harness
revision is frozen throughout the run. Documentation/proof updates do not
change the tested product. Original runner/pytest PIDs, start/finish/exit,
versions, commands, source checks and final results are persisted atomically in
`task-4-c-full-linux.result.json`; no implicit full-suite retry or coverage combination
is allowed.

## Real crash and continuation evidence

`tests/integration/test_general_worker_restart_process.py` exercises public
configured ControlPlaneApplication execution/recovery in separate Python
interpreters, each with a fresh App and SQLite connections on the same actual
database device/inode and private stores. The parent independently validates
committed journal state and obtains/releases BEGIN IMMEDIATE before crashing
either process. Exact identity and literal input are re-supplied; stores are
never copied or restored.

After actual completion launch ACK, test-only host instrumentation pauses
before the real wait can write stdin. The parent matches unit, InvocationID,
ControlGroup and MainPID, then requires actual `/usr/bin/python3 -I -S
.../general_worker_process.py --child`, matching /proc cgroup and kernel
pipe_read on pipe stdin. A bootstrap argv merely containing the filename is
insufficient. Only the verified actual MainPID receives SIGKILL in the primary
test. Then the owned old host is killed/reaped and a new interpreter recovers.

All successful continuations require actual newly published UTF-8 bytes
`candidate café` (15 bytes; SHA-256
`de5201f9083a3bd5999b34ab4b2f0014be9f48b0450613bcce83b51ad6739b01`), actual
fixed isolated Verifier executions, real grants/private-byte reads, real
empty-cgroup stop receipts and one lifecycle `AttemptCompleted` event with
outcome `succeeded` and matching task/evidence hashes. The reciprocal general,
Provider, proposal, budget, lifecycle and Scheduler links validate. No new
`AttemptSucceeded` event/schema is introduced.

Representative original A proof:

| cut | old host | new host | actual old MainPID | Provider count |
| --- | --- | --- | --- | --- |
| completion SIGKILL | 825345 | 825398 | 825368 | 1 |
| request ACK then host death | 825443 | 825502 | 825449 | 1 |
| terminal reply lost | 825563 | 825634 | already stopped | 1 unchanged |

Completion unit `maestro-attempt-bf0f07a78d9a49628fe6420e168bb9bd.service`,
InvocationID `61f1f3c1697c4ee0ac927a8ea3ddf00b`, has exact old stop proof before
Host825398 starts its fresh completion and two Verifiers. The response capture
exists before the crash; the output artifact does not. Request orphan unit
`maestro-attempt-7d966c9c17e54e679810b867c2090876.service`, InvocationID
`6b2234943bf740bd8727bc8c0d04fe76`, is stopped/proven empty by a new launcher
before the fresh request. Terminal replay returns identical result, task,
evidence, artifacts, projection and event rows without active input, fresh
Verifier, Provider or usage. Proofs are in `task-4-a-focused-green-data/`.

### Actual Worker timeout differs from lease expiry

`test_real_worker_runtime_timeout_then_new_host_completes_without_resend`
was added because B's real lease expiry is not Worker RuntimeMax supervision.
It leaves the real ACKed fixed completion child blocked on stdin for the actual
default systemd RuntimeMax, observes the emitted profile and the unit's real
systemd journal timeout, then kills only the old host. A new host proves the
exact old epoch stopped and completes the captured response with Provider=1.
The parent does not SIGKILL the Worker and call that a timeout.

Its focused proof passed: **1 test, zero failures/errors/skips, 21.74 seconds**.
Old/new hosts 838098/838553, actual MainPID838118, unit
`maestro-attempt-000f7899fd3b4660b9d4ddfe2c3c05a5.service`, InvocationID
`2f993597acee4eec829c37e14ba1e272`, observed RuntimeMaxUSec=`10s`. The actual
systemd journal says the runtime limit was reached and UNIT_RESULT=`timeout`.
The transient unit was collected before a durable Result property could be
sampled; its absence/default Result alone is expressly not treated as timeout
proof. Actual scoped journal records and subsequent exact empty-cgroup receipt
are retained in `task-4-c-timeout-focused-data/.../restart-evidence.json`.

## No-resend, concurrency and current authority

`tests/integration/test_general_worker_recovery_matrix.py` contributes 19 B
JUnit cases. Six blocked-cut cases each perform seven public API checks in a
new host: exact execute, changed input, changed run/node/attempt/generation,
and exact explicit recover. These are six cases with seven checks each, not
42 JUnit cases. All replies block and event rows/budget remain unchanged.

Counters distinguish the actual offline post_json entry (`transport`) from
the explicitly synthetic offline effect counter (`provider`). The latter is
committed to a separate test-only SQLite ledger before the after-effect crash.
Neither counter represents a real paid request or a production charge.

| old durable cut | Provider state | old effect / entry / broker | new effect / entry / broker / Worker / Verifier / usage |
| --- | --- | --- | --- |
| reserved | no intent | 0 / 0 / 0 | all 0 |
| dispatching before effect | dispatching | 0 / 1 / 1 | all 0 |
| dispatching after effect | dispatching | 1 / 1 / 1 | all 0 |
| ambiguous delivered fixture | unknown | 1 / 1 / 1 | all 0 |
| unsettled HTTP429 fixture | known_failure | 1 / 1 / 1 | all 0 |
| private publication, missing capture | known_success | 1 / 1 / 1 | all 0 |

The no-capture case independently confirms real ArtifactPublished/raw private
bytes and their hash, but never infers a durable capture from that publication.
All blocked budgets retain 20 minor units and 10 reserved tokens, with zero
used/released minor units. A Provider unknown may remain reserved occupancy;
no unsupported claim is made about relabeling it in the BudgetLedger.

Two separate recovered hosts contend on the real operation flock. Host829912
wins; Host829914 first returns in_progress, then receives the identical
historical accepted result after the winner. There is one Provider effect,
one transport entry, one trusted settlement, one accepted general checkpoint,
one successful AttemptCompleted and one slot release. Winner completion unit
`maestro-attempt-cf17877163314f84a4ccfbf0dac88bc9.service` has InvocationID
`90caffdf8e4f44ac85981b9aa28edf10`; its actual fresh Verifiers are
`maestro-attempt-84d263c2aa5342f8921c51039b319776.service` and
`maestro-attempt-d161acdd012c4a298b7cc1730ae9b3a3.service`.

Late-authority cases run a real fresh acceptance Verifier in the new host,
then withdraw authority before the final locked decision. Pause/cancellation
use public lifecycle methods from another App/connection. Lease expiry uses
an actual admitted 12-second lease and actual UTC (not a clock mock). Original
expiry proof: lease `2026-10-26T01:24:47.282748+00:00`, observed
`2026-10-26T01:24:47.283479+00:00`. The fencing case is explicitly a hostile
durable route mutation; it is not legitimate higher-generation admission.
All four block with unchanged held budget and no terminal writes. Separate
paused/cancelled real request-orphan cases require exact stop and no successor.

Corrupt bytes, missing private path, wrong operation source, wrong scope and
inconsistent capture metadata all actually fail at new-host bootstrap, with
zero new effects and unchanged event/budget state. Hostile tamper fixtures
preserve payload hashing for testing; they do not represent valid authority.
Private bytes removed from their expected path are retained as evidence.

Terminal rollback injects a test-only exception after actual terminal writes
inside the actual outer transaction. Parent sees proposed state and no partial
accepted/lifecycle/usage/slot writes after rollback. Host830483 then uses the
same proposal/capture/turn history and a new real Verifier
`maestro-attempt-8f4cc26025df41039bb5cc9cb66a04a6.service`, without another
Worker, credential, transport or Provider call. Overall trusted-usage reads=2,
durable settlement=1; the rolled-back usage read is not a second charge.

Original B proofs: `task-4-b-focused-green-data/` and
`task-4-b-proof-inventory.json`. All 19 proofs have successful bounded cleanup,
no retained assertion/cleanup errors and 39 distinct host PIDs, distinct from
parent829710. Complete actual unit/profile/invocation/cgroup/termination,
evidence, budget and counter sequencing is retained there. The fresh full run
also reruns these cases; its own proof inventory is recorded separately below.

## C-stage full verification and packaging (pre-review-fix evidence)

Environment: WSL Ubuntu24.04, Linux6.6.87.2-microsoft-standard-WSL2,
systemd255.4-1ubuntu8.18, Python3.12.3/GCC13.3.0, SQLite3.45.1,
pytest8.4.2, coverage7.16.1, pydantic2.13.5, PyYAML6.0.3, hypothesis6.168.0.
No dependency installation was used.

The first full-run configuration was invalid: its basetemp was inside the
Windows-backed `/mnt/c` plan directory. The complete original run is retained:
3324 cases, 358 failures, 2966 passes, zero errors/skips, 28 warnings,
1670.97 seconds; pytest exit1, coverage81.123959%, report/JSON exit2. Source
hashes remained unchanged. This failed trial is not final acceptance evidence.

The 358 original names/messages/full tracebacks and 40 message groups are in
`task-4-c-full.failures.json` and `task-4-c-full.failure-groups.json`. DrvFS
returned mode0777 despite fixtures requesting0700 (observed device40), rejected
FIFO/overlay operations, exceeded AF_UNIX path lengths, and put standalone
PrivateTmp probe secrets outside `/tmp`. The existing guards correctly rejected
unsafe private directories; no guard or fixture was weakened. The controller
explicitly authorized one configuration-only corrected full run, using native
Linux `/tmp`, a distinct prefix and the exact same 220 frozen file hashes.
Original failed data, coverage, JUnit, logs and metadata remain intact.

Corrected full-run command (the ignored runner adds the fresh-path guard,
SIGINT-default restoration in parent/child launch, durable PID/exit records,
source freeze and same-data report generation):

```sh
PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python \
  PREFIX/task-4-c-run-full-linux.py
# runner executes:
python -m coverage run --data-file=PREFIX/task-4-c-full-linux.coverage-data \
  -m pytest -o addopts= -q --basetemp=/tmp/maestro-t4c-_9de2ent/pytest \
  --junitxml=PREFIX/task-4-c-full-linux.junit.xml
python -m coverage report --data-file=PREFIX/task-4-c-full-linux.coverage-data --fail-under=90
python -m coverage json --data-file=PREFIX/task-4-c-full-linux.coverage-data \
  -o PREFIX/task-4-c-full-linux.coverage-summary.json
```

Failed-trial runnerPID838958/pytestPID838965 remain recorded separately.
Corrected runnerPID848208/pytestPID848215 and original native data path are in
`task-4-c-full-linux.result.json`: native root `/tmp/maestro-t4c-_9de2ent`,
device2096, inode1016497, mode0700, UID1000. Child/parent SIGINT defaults were
restored (initial child SigIgn0); all220 source hashes exactly match the original
trial. No authority stores were moved or copied to correct the configuration;
the fresh test run creates its own stores directly on Linux. All are retained.
The coverage data file, report log and JSON are distinct paths, not coverage
auto-combine sidecars. This C-stage corrected run completed with **3324 passed, zero
failures/errors/skips, 1149.84 seconds (19m09s)**. JUnit time1149.184s;
pytest process elapsed1153.832s; runner including reports1165.937s. Actual
pytest/report/coverage-JSON/runner exit codes are all0. Same-run statement
coverage is **19630/21620=90.79555966697502% (90.80%)**, with1990 missing and229
excluded statements. All30 GeneralWorker integration cases ran with zero skips.
All220 source/test/config bytes remained unchanged after the run and after
packaging. The frozen-map file SHA-256 is
`d5e86d2aeac45b306852325f15763d902ea942719846e604cdff1da815ef9288`.

### Fresh full-run process proofs

`task-4-c-full-linux.proof-inventory.json` independently inventories the fresh
full's23 proof JSONs:19 matrix cases plus4 restart/timeout cases,47 unique host
PIDs, all distinct from parent848215. Every proof records successful cleanup
and no assertion/cleanup error. Original native SQLite/private artifact stores
remain at `/tmp/maestro-t4c-_9de2ent/pytest`; none was copied or restored.

| fresh full scenario | old host | new host(s) | original Worker MainPID | Provider effect / transport entry |
| --- | --- | --- | --- | --- |
| completion SIGKILL | 849408 | 849457 | 849429 | 1 / 1 |
| request orphan | 849507 | 849535 | 849513 | 1 / 1 |
| terminal reply lost | 849600 | 849654 | already stopped | 1 / 1 unchanged |
| actual RuntimeMax timeout | 849657 | 850079 | 849677 | 1 / 1 |
| recovered contention | 848800 | winner848819, contender848821 | recorded in receipts | 1 / 1 |
| rollback/proposed continuation | 849333 | 849387 | recorded in receipts | 1 / 1 |
| committed unknown | 848714 | 848735 | already stopped | baseline1 / 1, both deltas0 |

Fresh completion SIGKILL unit
`maestro-attempt-f221725b61a24d2a89caee59f2f6bd8d.service`, InvocationID
`e9ecb66605884954beecc5345fa9e80d`, has actual fixed argv and pipe_read before
SIGKILL. Fresh request orphan unit
`maestro-attempt-d982a351b01b4563937297dda3f2300b.service`, InvocationID
`89379d99425a488bb6de4e8f20f041a4`, has exact old stop before successor.
Fresh timeout unit `maestro-attempt-5e2326a83d7d430cb8e73efbe389a156.service`,
InvocationID `2de8e16366114d7fb913c30e1272b4c1`, actual RuntimeMax10s,
has same-unit systemd runtime-limit and UNIT_RESULT=timeout journal records
before new-host recovery. Each fresh success has exactly one accepted,
successful AttemptCompleted, CostCommitted and slot release. Fresh rollback
retains two trusted-usage reads but only one durable settlement.

### Earlier C package check (pre-review-fix source)

After the full suite exited, these checks ran without another suite:

| check | actual command (python paths below) | exit / duration |
| --- | --- | --- |
| compilation | main Python `-m compileall -q src tests` | 0 / 3.532s |
| dependency consistency | main Python `-m pip check` | 0 / 0.377s; no broken requirements |
| private wheel | private builder `-m build --wheel --no-isolation --outdir /tmp/maestro-t4c-wheel-uvrr5vnf/wheels /tmp/maestro-t4c-wheel-uvrr5vnf/source` | 0 / 1.026s |
| actual wheel imports | main Python `-I /tmp/maestro-t4c-wheel-uvrr5vnf/cold-import.py /tmp/maestro-t4c-wheel-uvrr5vnf/installed /tmp/maestro-t4c-wheel-uvrr5vnf/expected-python-hashes.json` | 0 / 0.579s |

Main Python is `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`.
The previously existing private builder is
`/tmp/maestro-p0-task6-wheel.qDhql9/venv/bin/python` (Python3.12.3,
setuptools68.1.2, build1.4.0, wheel0.48.0). It had no orchestrator import before
building. Build dependencies were already present: no network/dependency
installation or shared environment mutation occurred. Source-only staging,
wheel and extraction remain in native mode0700 private root
`/tmp/maestro-t4c-wheel-uvrr5vnf`. Normal staging modes avoid importing DrvFS's
synthetic0777 metadata; source BYTES match the frozen source exactly.

Wheel: `multi_agent_orchestrator-0.1.0-py3-none-any.whl`, SHA-256
`df0e18acaf516499a467c1baa5d84ad00763621c42d7e351b438de11ae763e73`.
Its110 members contain106 product Python modules plus distribution metadata,
with no tests or evidence directories. Every Python archive member's bytes
match its frozen source SHA. All106 modules cold-imported under `-I` with
PYTHONPATH removed, from actual private extracted-wheel paths, not the source
checkout/editable package. Imported module bytes were checked again. Public
App/config/runtime exports resolved, version0.1.0, runtime pydantic2.13.5.
This is wheel integrity/import evidence, not a production wheel deployment.
Exact commands/PIDs/paths/hashes/module inventory are retained in
`task-4-c-package.result.json` and the four named check logs.

### Earlier C persistence privacy (pre-review-fix source)

`task-4-c-full-linux.privacy.json` records scans of the23 concrete SQLiteEventStore
fixture database files and every event payload row from the fresh real
restart/matrix proofs. Both UTF-8 and JSON ASCII-escaped forms of exact task
text, response text and offline credential fixture values have **zero hits**.
No WAL/SHM files remained after those processes closed, so no live-WAL snapshot
claim is made. Existing WAL/SHM files would have been scanned if present.

Raw private response/output artifacts intentionally contain response bytes;
their positive presence is recorded separately and is not an EventStore leak.
The test-only offline counter DB/debug logs and unrelated intentionally hostile
legacy test databases are outside this23-case scan. Required real existing-byte
private-root read-denial tests and the rest of the privacy compatibility suite
also passed in the full run. These fixture scans do not establish a claim about
real credentials or production requests, neither of which was used.

### Final post-review verification at d41d229bcb222851cb8a5f08d6cc9917b6dbb7be

The broad review of 36423cf..e0fa68f found one Important issue in
src/orchestrator/models/transport.py: a registered operation with matching
run/node/attempt but a different generation could evade the exact-identity
lookup and fall back to a permissive legacy verifier. Commit
280036a42f83d3c0bb54643558c0248ea0a9da70 rejects that cross-generation
mismatch at lookup and immediately before Provider-intent creation, with a
regression for the changed generation and absent dispatch authority. The
reviewer's scoped re-review confirmed the fix and reported no new findings.

The first full suite after that source change retained a separate migration
failure (3325 passed, one failed, zero errors/skips). Its traceback showed the
new events index was created before legacy rows gained run_id and related
columns. Commit d41d229bcb222851cb8a5f08d6cc9917b6dbb7be moves index creation
after migration in both the savepoint and outer-transaction paths, and asserts
the migrated index while preserving the old event. The migration test and its
directly affected five-file group passed (222 tests); the scoped re-review
confirmed ordering, transactionality, idempotency and row preservation, with
no findings. Both review-fix commits were ordinarily pushed.

The one final full run at d41d229bcb222851cb8a5f08d6cc9917b6dbb7be completed
with **3326 passed, zero failures/errors/skips**. Pytest reports 1440.04
seconds; the runner recorded 1445.97 seconds. Same-run coverage is
**19670/21666=90.78740884334903% (90.79%)**; report and JSON exits were zero.
The runner recorded source_unchanged=true for its 220-file frozen source,
test and configuration map. The complete JUnit, coverage data/report/summary,
pytest log, source map and runner result are retained as
task-4-post-migration-full.*. No production source or test file changed after
this run; subsequent work only generated ignored evidence and updated these
documents.

Post-run package checks used the product environment for compileall and
pip check, both exit 0. The existing product venv lacked build backend
packages; two failed attempts and their logs are retained and are not counted
as wheel evidence. A fresh mode-0700 builder-only venv at
/tmp/maestro-t4-worker-build-20261030-0156/venv supplied build 1.4.0,
setuptools 68.1.2 and wheel 0.48.0 without changing the product venv or any
shared environment. From exact frozen pyproject.toml and src/ bytes, the
isolated no-build-isolation command produced
multi_agent_orchestrator-0.1.0-py3-none-any.whl with SHA-256
fcadc27fba6946b9ed854a481f850538f17a507064668061fbb4a4fa2128ca8e.
The 110 wheel members include 106 product Python modules and distribution
metadata; no tests or evidence directories are included. All 106 modules
cold-imported with isolated mode from the private extraction, and their bytes
matched the frozen source. The package result records source_still_unchanged=true.
This verifies wheel integrity/imports, not deployment.

The refreshed privacy scan covers the 23 SQLite stores and event rows retained
by this final run. It found **zero hits** for the exact task, response and
offline-credential sentinels. No WAL/SHM files remained after process close, so
this is not a live-WAL observation. The scan excludes offline effect-counter
stores, raw private response artifacts, unrelated hostile legacy fixtures and
debug logs. See task-4-post-migration-full.privacy.json and
task-4-post-migration-full.proof-inventory.json; all scan artifacts remain
in the ignored task evidence directory.

## Rulings and cost if wrong

This inventory copies all 18 explicit rulings and the two recorded refinements
from this plan's authoritative controller ledger. It introduces no new product
authority. Ledger line numbers refer to this plan's `progress.md`.

| # / ledger line | Decision | Cost if wrong / limitation |
| --- | --- | --- |
| 1 / 52 — Completion exception | Only this exact captured known-success call may authorize completion/acceptance; never another dispatch. | An overbroad exception could admit unrelated effects; identity, usage, stop and capture must all bind. |
| 2 / 56 — Send boundary | A post-credential committed Provider intent is in-flight send authority; withdrawal cannot retroactively prove no effect. | One already-authorized call may deliver after cancellation; preserve its holds. |
| 3 / 60 — Atomic success | General accepted metadata, Scheduler/lifecycle/agent terminal and usage/slot settlement commit together; private publication is earlier. | Rollback/replay coupling can regress legacy paths; verify both. |
| 4 / 108 — Two input hashes | Transport fingerprint commits domain + one NUL + canonical full WorkerTask; admission manifest hash remains separate. | Confusing hashes rejects valid replay or permits mismatched input. |
| 5 / 133 — Recovered receipt | A validated durable same-store launch ACK can authorize exact orphan stopping; process-local prepare tracking authorizes new launch only. | A forged receipt could stop unrelated work; callers never supply receipts and live identities are rechecked. |
| 6 / 139 — Observed profile | ACK verifies every emitted effective security/resource property with structural normalization, rather than a subset or unrelated defaults. | Systemd normalization differences may block availability. |
| 7 / 148 — Prepared argv | Prepared launches reject whitespace/separators that make observed ExecStart ambiguous; legacy omitted behavior stays. | Generic prepared commands may need future unambiguous encoding support. |
| 8 / 221 — SQL boundary | New general authority shares typed metadata-only control replay; bytes/callbacks stay outside SQL, direct ambient transaction bypasses reject. | Projection parity or preflight/commit binding errors could weaken proof or availability. |
| 9 / 243 — Private layout | Canonical DB/artifact/owner roots must be outside readable workspace and runtime source; aliases cannot bypass overlap checks. | Otherwise-safe excluded in-tree layouts remain unavailable. |
| 10 / 291 — Ambient layout refinement | Also reject roots readable through actual ambient grants; allow only documented private /tmp treatment with real existing-byte denial proof. | Additional layouts become unavailable; directory names alone provide no privacy. |
| 11 / 251 — Host restart scope | Bind Linux boot and effective UID, and support same-host-boot application restart only. | OS reboot or host migration remains blocked pending separate authority. |
| 12 / 315 — Store scope refinement | Also bind concrete SQLite canonical-path hash/device/inode; reject hardlinks, memory DB and copied/moved authority stores. | Store moves, aliases, copies and in-memory execution need a separate design. |
| 13 / 369 — One-use dispatch | Mint a process-local permit only after a new checked reservation commit; claim once before broker and consume at intent; never reconstruct on replay. | Pre-intent owner loss or transient failure remains held for explicit resolution. |
| 14 / 401 — Pure request retry | A fresh request epoch is allowed only with no call/capture and every old request ACK+STOP under unchanged current authority/holds. | A broader predicate could replay effectful calls. |
| 15 / 418 — Owned usage | Scheduler owns the trusted usage service and shares the App source with Coordinator; caller zero-cost records are not authority. | Standalone SDK composition must wire it correctly; unavailable/failing usage stays held. |
| 16 / 433 — Fresh independent verifier | General Scheduler owns a fresh fixed verifier and trusted grant authority; Coordinator delegates without duplicate acceptance verification. | Extra isolated verification and signer wiring are required. |
| 17 / 446 — Snapshot binding | Forward frozen workspace identity into the actual launcher snapshot, beyond an earlier precheck. | A legitimate directory identity change blocks that old operation's new launch. |
| 18 / 583 — Identity clarification | Workspace identity commits canonical path/device/inode, not tree bytes; do not introduce a content-admission schema. | Same-directory content edits are not pinned across restarts; exact task input is separately committed. |
| 19 / 590 — Async ownership seam | Prepare SQLite/grants/metadata on the owner thread; run immutable bounded DB-free verifier staging/process work off-loop; share sync/async acceptance. | Internal producer maintenance and sync/async compatibility risk increase. |
| 20 / 655 — Stop observation | After exact ACK and fresh stopped binding, permit one bounded reread of the same strict termination/empty-cgroup proof, then recheck stopped binding. | Extra bounded stop latency; ambiguous, foreign or populated cases remain held. |

## Remaining bounds and review gates

Workers have no Provider credentials, network, SQLite/Scheduler access or host
publication authority. Their output is bounded terminal text; tool calls,
arbitrary shell execution and workspace publication fail closed in this path.
The built-in Verifier proves bounded artifact checks, not semantic code quality.
Raw task/response bytes belong only in caller memory/private artifact handling;
the event journal stores bounded identifiers, hashes, counts and proof metadata.

Trusted production usage remains unavailable by default. Online delivery,
production charging/accounting, wider Provider operations, operational resolution
of ambiguous holds and the remaining full-P3 gates need their own authorization
and evidence. This design supports same-host-boot/same-concrete-store restart;
reboot, host migration, copied/moved/rewound stores and in-memory authority are
not supported. Workspace identity pins canonical directory path/device/inode,
not same-directory file contents; exact task input has its own commitment.

In-process unit/barrier checks are compatibility evidence only. The named
independent-host tests use real distinct interpreters/Apps/connections and real
OS receipts; the offline effects/usage and hostile durable fixtures are labeled
separately. No test crash hook or caller-supplied proof/skip parameter was added
to production. Existing blocked-only Workers, legacy/non-general Attempts and
their artifact startup checks remain covered by the full suite.

The metadata-only SQL boundary applies to new general authority paths. Existing
legacy/bootstrap artifact recovery retains its prior file checks; this evidence
does not reclassify those older checks as IO-free.

The independent Task4 review and the broad whole-slice review are complete.
The broad review's Important finding and the subsequent migration-order issue
were addressed in 280036a and d41d229; each received scoped re-review with no
remaining findings. Reviewers did not rerun tests. Counts and coverage above
come from the retained final runner/JUnit/coverage artifacts. No PR, main
integration, deployment or P5 execution is part of this acceptance.
