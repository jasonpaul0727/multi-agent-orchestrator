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
