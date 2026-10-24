# Task 1 report — Feature A only

Status: Feature A DONE; stopped for controller push/quota gate. Features B/C are pending explicit controller resumption. No full-suite or real systemd-isolation claim is made here.

Worktree: `C:/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator`; WSL Git branch `codex/p3-systemd-termination-receipts`; implementation base `f6d6f72`.

## Implemented

- Frozen `GeneralWorkerTurn` and `GeneralWorkerRequest` data contracts; task and response are excluded from turn repr.
- A 1,048,576-byte maximum input/proposal frame with strict UTF-8 JSON, exact fields, duplicate-key rejection, nonfinite/overflow rejection, bounded exact task/context validation, and canonical UUIDv4 launch identities.
- A fixed stdlib-only child entry: request returns the exact context/launch plus a transport task fingerprint; completion emits one actual UTF-8 text candidate with actual SHA256/byte count and canonical base64. Terminal content is limited to `min(task.output_byte_limit, 16_384)` bytes.
- Lazy host adapters reconstruct the typed task/context and reuse `decode_worker_output` for candidate integrity and attempt binding. Completion additionally binds exact response bytes and the text media/type contract.
- Invalid host model subclasses are rejected before accessing their attributes. Constructor-bypassed field values/extras, nested context subclasses, and noncanonical capability order fail with a detached fixed error. Original exceptions are not retained as cause/context.

Controller interface ruling applied: `input_commitment` is `sha256:` plus SHA256 of `b"maestro.general-worker-input.v1\0" + canonical_full_WorkerTask_JSON`. It is a transport task fingerprint and does not replace or relabel `task.context.input_manifest_hash` / `WorkerInputManifest.content_hash`. The launch identity is echoed separately. Candidate metadata is `artifact_type="text"`, `media_type="text/plain; charset=utf-8"`.

## TDD evidence

All commands below ran through WSL in the named worktree, using `PYTHONPATH=src:.` and `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`.

Initial RED, before either production file existed:

```text
PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/runtime/test_general_worker_protocol.py -q

ERROR collecting tests/unit/runtime/test_general_worker_protocol.py
ModuleNotFoundError: No module named 'orchestrator.runtime.general_worker_protocol'
Interrupted: 1 error during collection
```

This expected missing-interface failure is retained in `task-1-a-red.log`. Required tests were written first: request roundtrip, real UTF-8 completion, malformed/stale/oversized input, and constructor/subclass callback rejection. First GREEN after implementation: 17 tests passed.

Self-review RED for an independently identified fingerprint normalization issue:

```text
PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest -o addopts= -q tests/unit/runtime/test_general_worker_protocol.py::test_constructed_task_cannot_change_fingerprint_on_roundtrip

FAILED test_constructed_task_cannot_change_fingerprint_on_roundtrip
Failed: DID NOT RAISE <class 'orchestrator.runtime.general_worker_protocol.GeneralWorkerProtocolError'>
1 failed in 5.55s
```

The failing test demonstrated acceptance of a constructor-bypassed task whose capability order (and therefore fingerprint) would change after typed decoding. The fix rejects noncanonical capability order in both host and child validation.

Final GREEN after that fix:

```text
PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest -o addopts= -q tests/unit/runtime/test_general_worker_protocol.py tests/unit/runtime/test_worker_output.py tests/unit/runtime/test_worker_process.py --junitxml=.superpowers/sdd/2026-10-24-p3-general-worker-recovery-implementation/task-1-a-green.xml

298 passed in 9.58s
```

The 38 new protocol cases include subprocess execution under `/usr/bin/python3 -I -S`, proving child startup/operation without site packages; malformed wire input exits 64 with empty stdout/stderr. Bounds include accepting a padded exactly-1MiB frame, rejecting a larger frame, handling multibyte UTF-8, accepting exactly the output cap, and rejecting one extra byte. Existing `worker_output` and blocked-only `worker_process` tests provide focused compatibility coverage. No warnings/failures/skips appeared. JUnit evidence is retained in `task-1-a-green.xml`. `git diff --cached --check` passed.

The full suite is deliberately deferred to the Feature C checkpoint by the binding controller brief, overriding the generic skill's earlier full-suite timing.

## Files and commit

Product/test files:

- `src/orchestrator/runtime/general_worker_protocol.py`
- `src/orchestrator/runtime/general_worker_process.py` (fixed child only at this checkpoint)
- `tests/unit/runtime/test_general_worker_protocol.py`

Evidence: this report, `task-1-a-red.log`, and `task-1-a-green.xml` beside the brief.

Feature A commit subject: `feat(runtime): add bounded useful general Worker protocol` (the containing commit; SHA returned to controller separately).

## Self-review and boundaries

Reviewed all staged product/test diffs against the Feature A brief. Corrected the capability-order issue through RED/GREEN. No existing product file, launcher, admission hash, database/journal, credential, dependency, or paid Provider path was changed. No callbacks, tool execution, publication authority, or stop receipt is introduced by this protocol. No delegation, push, PR, deploy, or cleanup was performed.

No unresolved Feature A correctness concerns. Feature B must supervise the larger encoded proposal frame (base64/metadata can exceed the 16,384-byte decoded content cap), expose/persist the observed launch receipt before wait, and withhold candidates without verified termination. Real systemd isolation and the one full-suite run remain Feature C work; the subprocess tests here do not assert those properties.

## Feature B — supervised lifecycle checkpoint

Status: Feature B DONE; stop for controller push/quota gate. Feature C, real profile normalization checks, required real isolation, and the one full-suite run remain pending controller resumption. Feature A commit `d2196a7` is preserved.

### Implemented

- Added `PreparedSandboxLaunch`, `SandboxLaunchReceipt`, and `IsolationStopUnknown`, exported by `orchestrator.isolation`. Preparation selects a canonical UUIDv4 and exact owned `maestro-attempt-{uuid.hex}.service` without launching/probing. New launches consume their in-memory prepared epoch once, including failed preflight; reconstructed receipts are accepted for recovery stop without process-local ownership state.
- Prepared launches observe exact systemd Id, LoadState, active state, ControlGroup, canonical nonzero InvocationID, and the complete effective security/resource profile before returning a session. Profile normalization covers every `_service_properties` property, including hard/soft rlimits, cgroup limits, systemd timespans, sorted RO bind/path sets, inaccessible path sets, address family restrictions, empty service Environment, and exact `/usr/bin/env -i` executable/argv binding. The receipt stores a SHA256 commitment to normalized profile data rather than the raw profile paths; it makes no stop claim.
- Prepared launch supports the fixed safe argv used by the useful Worker. Whitespace, quoting, non-ASCII, or systemctl separators that make observed argv ambiguous fail before Popen. The controller approved this prepared-only restriction; omitted-prepared launch keeps its previous behavior.
- Added `stop_prepared_launch`: reconstructs and verifies the journalled identity; rejects changed Id/cgroup/InvocationID/profile before signalling; kills the whole live unit with SIGKILL; then uses the existing exact stopped-systemd/empty-cgroup observer and rechecks invocation/profile. Previously acknowledged collected units can prove stop from authoritative not-found/inactive state and an empty/missing exact retained cgroup. No ack remains detached ambiguous-start; missing/mismatched stop proof remains detached stop-unknown. A still-live local systemd-run transport prevents a stop witness; staging remains retained on ambiguity.
- Added a final invocation/profile check to normal prepared session completion so a same-name replacement cannot gain a candidate merely because its cgroup is empty. Existing unprepared sessions do not run the new observation path.
- Added `GeneralWorkerProcess.start_turn`/`run_turn`, `GeneralWorkerSession`, and the controller-approved `GeneralWorkerTurnResult` fields. The launch receipt is available before `wait`, allowing later service journalling without a DB-lock callback. Wait requires exact host termination proof. Clean stopped output is decoded; invalid/cancelled/failed stopped transport returns `proposal=None` with the stop witness. Errors are detached into distinct no-start, start-unknown, and stop-unknown states.
- Supervision supports cancellation before/during wait. Encoded output collection is capped at 1MiB even if supplied limits request more; decoded terminal text remains capped at 16,384 bytes. The old Worker remains blocked-only. The fixed child still runs under system Python `-I -S`, with host imports after its exit guard.

Controller rulings applied: receipt invocation/profile fields, journal reconstruction after restart, complete effective profile observation, fixed prepared argv restriction, and proposal=None on confirmed stopped invalid transport. Root's matching interface/verification edits to the tracked implementation plan are included at its request. The tracked Feature A report is updated with permission; new B XML remains ignored and is not force-added.

### Feature B RED/GREEN evidence

Commands ran in the same WSL worktree/interpreter as Feature A. Initial RED:

```text
PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest -o addopts= -q tests/unit/runtime/test_general_worker_process.py
ImportError: cannot import name 'IsolationStopUnknown' from 'orchestrator.isolation'
1 error in 1.14s

PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest -o addopts= -q tests/unit/isolation/test_launcher.py -k "prepared_identity or ack_requires or observed_identity or epoch_cannot or unacknowledged"
AttributeError: 'SystemdReadOnlyLauncher' object has no attribute 'prepare_launch'
AttributeError: module 'orchestrator.isolation.launcher' has no attribute 'PreparedSandboxLaunch'
11 failed, 75 deselected in 2.04s
```

Both were expected absent-interface failures before Feature B implementation. Initial focused GREEN after implementing launcher/session APIs: `99 passed in 2.78s`.

Self-review RED: `test_normal_wait_rejects_replaced_invocation_even_with_empty_cgroup` failed (`AssertionError: assert not True`, `1 failed, 133 deselected in 0.96s`) because normal wait still accepted an old-style terminal receipt after the invocation changed. The fix reobserves exact invocation/profile before keeping a prepared termination receipt; focused protocol/Worker/launcher GREEN then reported `227 passed in 5.99s`.

Self-review RED: `test_supplied_limits_cannot_expand_protocol_frame_collection` failed (`assert 2097152 == 1048576`, `1 failed, 14 deselected in 1.59s`). The fix clamps supplied output collection limits to the protocol cap. Additional cases cover post-wait-start cancellation, recovered live-unit whole-unit signalling, and detached transport exceptions.

One intermediate test run exposed recursion in the new test fake (`_mock_prepared_launch` patched Popen and then called a helper which itself used Popen): `2 failed, 157 passed in 4.49s`. This was a test setup defect, fixed with a specific completed transport double; it required no product change. The corrected targeted run reported `159 passed in 3.80s` before the final two coverage cases.

Final GREEN after all product/test edits:

```text
PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest -o addopts= -q tests/unit/isolation/test_launcher.py tests/unit/isolation/test_provider_sender.py tests/unit/isolation/test_overlay_launcher.py tests/unit/runtime/test_general_worker_protocol.py tests/unit/runtime/test_general_worker_process.py tests/unit/runtime/test_worker_process.py tests/unit/runtime/test_worker_output.py tests/unit/runtime/test_workspace_worker.py tests/unit/runtime/test_verifier_process.py --junitxml=.superpowers/sdd/2026-10-24-p3-general-worker-recovery-implementation/task-1-b-green.xml

622 passed in 23.96s
```

No failures, warnings, errors, or skips. This is focused compatibility coverage, not the full suite. JUnit is retained in the ignored plan workspace as `task-1-b-green.xml`. Missing-profile-property cases exercise every observed effective profile key; explicit cases reject weakened bind/path/argv/environment/address-family/rlimit settings. Recovery tests use a new launcher with reconstructed receipt data, covering loaded/inactive and collected units, and exact live-unit SIGKILL before terminal proof. Existing Provider sender/overlay launcher/blocked Worker/candidate decoder/workspace Worker/verifier tests all passed. `git diff --check` passed.

### Feature B files, self-review, and limits

Product/test changes: `src/orchestrator/isolation/launcher.py`, `src/orchestrator/isolation/__init__.py`, `src/orchestrator/runtime/general_worker_process.py`, `tests/unit/isolation/test_launcher.py`, and new `tests/unit/runtime/test_general_worker_process.py`. Also included: controller-edited tracked plan and this already-tracked report. Commit subject: `feat(isolation): supervise prepared general Worker launches` (containing commit; SHA returned separately).

Self-reviewed the full source/test diff and corrected the normal-wait invocation and collection-cap gaps through failing regressions. Restored omitted-launch OSError chaining exactly; no legacy sandbox control was relaxed. The existing launcher was already large (~885 lines) and now contains about 340 additional lines; no unplanned file split or unrelated refactor was made. Profile paths are ephemeral in the host observer and only their commitment enters the new launch receipt; raw task/response bytes remain on bounded private pipes and repr excludes proposal content.

No unresolved Feature B correctness concerns from focused tests. Real systemd property rendering/normalization is not yet pinned on this host; unsupported/missing properties fail ambiguous rather than weakening checks. Confirming supported rendering, read-only/no-network behavior, real MainPID SIGKILL/empty-cgroup proof, and no retry after real unacknowledged start remains Feature C. No full suite, paid Provider call, push, PR, deploy, delegation, cleanup, dependency change, journal/database implementation, or history rewrite was performed in B.
