# P5 Worker Input Re-supply Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bind exact re-supplied text to accepted routing and construct a bounded WorkerTask from one durable active Attempt snapshot, without retaining plaintext or executing a Worker.

**Architecture:** A pure immutable manifest module canonicalizes and hashes input. A host-only WorkerInputService composes existing Scheduler/EventStore authority under nested append_checked transactions; ControlPlaneApplication exposes ready-gated admission and preparation methods. No child, Provider, tool, artifact publication, settlement, or input store is added.

**Tech Stack:** Existing Python >=3.12, Pydantic 2, SQLite, pytest and coverage; existing Linux/systemd integration tests. No new dependency.

**Spec:** `docs/superpowers/specs/2026-10-09-p5-worker-input-resupply-design.md` (user-approved 2026-10-10).

**Status:** Proposed implementation plan; preserve the user's Subagent-driven execution method, but await this written-plan review before product implementation.

**Execution method:** Subagent-driven is already selected. The header's template alternative is not an invitation or authorization to switch to Native/executing-plans.

## Global Constraints

- "The text is not trimmed, case-folded, newline-normalized, or Unicode-normalized."
- "Text retains the existing 100,000-character maximum; canonical manifest bytes and serialized `WorkerTask` must each fit the existing 1,048,576-byte IPC envelope."
- "The task contains only the binding's immutable `AttemptContext`, the authoritative role, validated caller text, empty artifact/capability tuples, and a host-fixed output limit of 16,384 bytes."
- "The trusted time is sampled after acquiring the outer preparation serialization lock, not before waiting for it."
- "The construction result is data, not a capability to execute or complete an Attempt."
- "Event payloads, persistent projection rows, audit, logs, exceptions, argv, environment, and ArtifactStore must not gain raw task text from this path."
- "Matching a new commitment proves equality with text accepted at routing admission, not equality with text previously consumed by the planner."
- "Full repository tests pass with total coverage at least 90%; required existing real systemd isolation tests are not skipped."
- "A completed stage is committed and pushed to `codex/p3-systemd-termination-receipts`; the main checkout's unrelated changes remain untouched."
- No paid Provider, live secret, protected input store, install, deployment, main merge, destructive cleanup, automatic execution, or semantic/V1 certification is authorized by this plan.

## Review Focus

1. Unsafe model_construct/model_copy input and lone surrogates must be rejected before hash/task use with no plaintext in formatted exceptions (Task 1 tests).
2. A retry with the same routing key and different text must not add reservations, Agents, slots, or a replacement input hash (Task 2 tests).
3. SQLite lock acquisition can cross expiry; preparation must sample time under the acquired lock and reject, not use an earlier timestamp (Task 3 test).
4. Graph append or cancellation winning a race must not produce mixed role/context/graph authority; preparing task data first does not authorize later dispatch (Tasks 2/3 tests).
5. A process can die after nested admission writes but before the outer commit, or after commit before response; restart must show all-or-nothing state and exact retry without automatic work (Task 2 crash tests, Task 3 recovery tests).

## Workspace and commands

Retain the existing isolated worktree. Windows path: `C:/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator`; WSL path: `/mnt/c/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator`. Branch: `codex/p3-systemd-termination-receipts`. Planning BASE: `fd5a9351d2b75f71b72e28442f6143f8f946c9fa`. Never move to or reset the unrelated main checkout.

Use WSL Git and the existing Python `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`. Run the following commands from the WSL worktree; no environment file or real credentials are loaded:

```bash
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest --override-ini=addopts= tests/unit/runtime/test_worker_input.py -q
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m coverage run -m pytest --override-ini=addopts= -q
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m coverage report
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m compileall -q src tests
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pip check
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m build --wheel
git diff --check
```

Normal wheel builds may use temporary declared PEP517 build tooling; do not install into the shared runtime. Record failed commands too. A missing systemd capability is an unmet isolation gate, not permission to hide required skips. Parent coverage is not child-process coverage.

After each task: preserve the RED/GREEN outputs and exact candidate SHA, run focused tests plus full coverage gate, obtain a fresh independent scoped review, fix and re-review substantive findings, then let the controller push the exact reviewed commit. Verify actual remote SHA and clean worktree before the next task. Each task's examples below are executable starting tests/algorithms, not permission to replace the complete listed matrix with one example.

Use this plan's ignored recovery workspace `.superpowers/sdd/2026-10-10-p5-worker-input-resupply/` for the progress ledger, task briefs, test/review reports and recorded rulings. Read only this workspace and the named predecessor acceptance evidence when resuming. Preserve it; do not recursively delete scratch. The approved spec and this plan travel together to every implementer/reviewer.

## File structure and interfaces

- Create `src/orchestrator/runtime/worker_input.py`: immutable manifest, stable sanitized input error, strict canonicalization/hash; no persistence/service imports.
- Create `src/orchestrator/runtime/worker_input_service.py`: host orchestration for input-bound admission and preparation; one shared Scheduler connection, no Worker/Provider/tool execution.
- Modify `src/orchestrator/application.py`: construct the private service and forward two ready-gated methods; keep startup and proof acceptance unchanged.
- Modify `src/orchestrator/runtime/__init__.py`: export manifest/error/builder only; do not make a child runner out of the host service.
- Modify `src/orchestrator/runtime/contracts.py` only to set `WorkerTask.task_text` to `repr=False`; preserve its validation, wire schema and serialization. Extend `tests/unit/runtime/test_contracts.py` with the repr/serialization regression in Task 3.
- Create `tests/unit/runtime/test_worker_input.py`, `tests/unit/runtime/test_worker_input_service.py`, `tests/support/worker_input.py`, and `tests/integration/test_worker_input_recovery.py` as owned tests/fixtures.
- Modify `docs/security/worker-runtime.md` and `README.md` with each deliverable; distinguish prepared task data from blocked-only production execution.
- No changes to Scheduler success, Provider settlement, SQLite schema, Worker child, Verifier acceptance, Broker, or ArtifactStore are planned. If an existing authority primitive must change, stop and expand the task brief/review scope explicitly, not silently.

### Task 1: Strict versioned input manifest

**Files:** Create `src/orchestrator/runtime/worker_input.py`; modify `src/orchestrator/runtime/__init__.py`; create `tests/unit/runtime/test_worker_input.py`; update `docs/security/worker-runtime.md` with the pure input contract only.

**Interfaces:**

- Consumes existing `RoleName`, `revalidate_model`, and AttemptContext identifier shape.
- Produces `WorkerInputError(ValueError)` with a stable non-sensitive `code`, `WorkerInputManifest`, and `build_worker_input_manifest(*, run_id: str, node_id: str, role: RoleName, task_text: str) -> WorkerInputManifest`.
- Manifest fields are `schema_id: Literal["maestro.worker-input.v1"] = Field(default="maestro.worker-input.v1", alias="schema")`, strict Run/Node identifiers (1..128, same ASCII pattern as AttemptContext), authoritative RoleName, nonblank `task_text` (1..100000; `repr=False`), and `input_artifacts: tuple[()] = ()`. Use `ConfigDict(frozen=True, strict=True, extra="forbid", hide_input_in_errors=True, populate_by_name=True)`: existing `revalidate_model` rebuilds Python field names, so `schema_id` must be accepted internally on the supported Pydantic >=2.7 floor. The alias is used for canonical JSON; no wire method may expose a different field name.
- Properties `canonical_bytes: bytes` and `content_hash: str` revalidate the manifest including unsafe copies and enforce `MAX_WORKER_INPUT_FRAME_BYTES = 1_048_576`. Property failures are wrapped as WorkerInputError from None. `WORKER_INPUT_OUTPUT_BYTES = 16_384` is the fixed later task output bound, not a new user option.

- [ ] **Step 1: Add the fixed canonical vector and rejection tests.**

```python
from orchestrator.runtime.worker_input import build_worker_input_manifest

def test_known_canonical_vector():
    value = build_worker_input_manifest(run_id="run-1", node_id="node-1",
        role="coder", task_text="implement a small change")
    assert value.canonical_bytes == (
        b'{"input_artifacts":[],"node_id":"node-1","role":"coder",'
        b'"run_id":"run-1","schema":"maestro.worker-input.v1",'
        b'"task_text":"implement a small change"}')
    assert value.content_hash == (
        "sha256:c024f0eefb7b0362f5783d560036a89c3564b1f4a656e108c655a799fbd37e2c")
```

Add parametrized tests for blank/non-string text, invalid/non-string/oversized identifiers, invalid role, 100001 characters, surrogate `"\ud800"`, unsafe model_copy/model_construct, nonempty input_artifacts, extra fields, malformed schema, and sentinel text absent from repr/formatted traceback. Pin valid alias and Python-name construction, ordinary revalidation and valid-field unsafe-copy revalidation to the same canonical `schema` key/hash. Explicitly reject an unsafe `model_copy(update={"unknown_field": sentinel})` rather than silently dropping the added attribute; test nonempty `model_extra` too. Compare distinct hashes for leading/trailing whitespace, CRLF/LF, composed/decomposed Unicode, Run/Node/role changes. Reordered input mappings must serialize identically. Include max-valid-size text; force the byte-bound branch with the pytest fixture's `monkeypatch.setattr(orchestrator.runtime.worker_input, "MAX_WORKER_INPUT_FRAME_BYTES", reduced_test_limit)` and automatic restoration, not a test-local imported cap: bounded 100000-character valid manifests cannot naturally reach 1 MiB, so do not fabricate a reachable production input or change the public bound.

- [ ] **Step 2: Run the new module tests and record the actual RED reason.**

Run the Task 1 focused command above. Expected initial failure: missing worker_input module/API; rejection-specific tests must subsequently fail for the intended missing validation, not unrelated fixture errors.

- [ ] **Step 3: Implement the pure model/builder/properties.**

Canonicalization algorithm after revalidation:

```python
payload = value.model_dump(mode="json", by_alias=True)
encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True,
    separators=(",", ":"), allow_nan=False).encode("utf-8", errors="strict")
if len(encoded) > MAX_WORKER_INPUT_FRAME_BYTES:
    raise WorkerInputError("worker_input_too_large")
digest = "sha256:" + hashlib.sha256(b"maestro.worker-input.v1\x00" + encoded).hexdigest()
```

Before `revalidate_model`, locally reject surviving unexpected object fields: require `set(value.__dict__) <= set(WorkerInputManifest.model_fields)` and an empty/None `value.model_extra`. The shared helper rebuilds declared fields only and would otherwise discard unsafe-copy extras. Do not change that shared helper or pretend to recover unknown keys already discarded by Pydantic's `model_construct`; raw mapping validation still forbids unknown keys. Then `value` is a fresh validated WorkerInputManifest, never an unchecked caller object. No text normalization, persistence, logging, cache, reference resolution or child execution occurs. Wrapper error construction must not include validation inputs; raw model_validation is internal, public builder/property errors suppress underlying exceptions in formatted tracebacks. Reject nonempty arrays rather than filtering them out. Export only the named pure API.

- [ ] **Step 4: Run GREEN plus existing runtime contract tests and the full coverage gate.**

```bash
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest --override-ini=addopts= tests/unit/runtime/test_worker_input.py tests/unit/runtime/test_contracts.py -q
```

The existing `tests/unit/runtime/test_contracts.py` filename has been verified with `rg --files`. All new branches must have targeted tests; preserve the repository's 90% gate and no-required-skip condition with the full commands above.

- [ ] **Step 5: Update the contract documentation and self-check exact diff.**

Describe the domain-separated schema, exact text comparison, no confidentiality/original-planner-authenticity claim, and no production dispatch/admission changes yet. Run `git diff --check` and inspect only the named file diff.

- [ ] **Step 6: Commit, independently review, repair if needed, then push this stage.**

```bash
git add -- src/orchestrator/runtime/worker_input.py src/orchestrator/runtime/__init__.py tests/unit/runtime/test_worker_input.py docs/security/worker-runtime.md
git commit -m "feat: define canonical worker input commitment"
git push origin HEAD:refs/heads/codex/p3-systemd-termination-receipts
```

The push is controller-owned and runs only after clean scoped review; the commit may be created as a review candidate earlier. Save BASE/HEAD/test/review evidence in this plan's ledger and verify local/tracking/actual remote SHA equality.

### Task 2: Atomic input-bound application admission

**Files:** Create `src/orchestrator/runtime/worker_input_service.py`, `tests/support/worker_input.py`, `tests/unit/runtime/test_worker_input_service.py`, `tests/integration/test_worker_input_recovery.py`; modify `src/orchestrator/application.py` and `docs/security/worker-runtime.md`.

**Interfaces:**

- Consumes Task 1 builder/content_hash/WorkerInputError, existing `Scheduler.accept_routing()` and `SQLiteEventStore.append_checked()`, RoutingRequest/Decision, AcceptedAttempt.
- Produces `WorkerInputService(scheduler: Scheduler, *, clock: Callable[[], datetime] = _utc_now)` with `accept_routing_with_input(request: RoutingRequest, decision: RoutingDecision, *, task_text: str, accepted_at: datetime, lease_expires_at: datetime) -> AcceptedAttempt`.
- Application exposes the same admission signature and privately owns `_worker_inputs`. No input hash, role, arbitrary check set, workspace/path, usage, result or authority body parameter is accepted. Trusted built-in contract and checks are fixed to `BUILTIN_VERIFICATION_CONTRACT_ID` and `("artifact-integrity",)` for this minimal slice; future richer output contracts need their own design.
- Owned test helper `make_worker_input_case(database: Path) -> SimpleNamespace` returns app/request/decision/lifecycle/clock/text/identity, without accepting an Attempt. `admit_worker_input_case(case) -> AcceptedAttempt` uses Task 2 API with exact fixture times and assigns `case.accepted`. Use the existing real durable setup, not fabricated proof fixtures.

- [ ] **Step 1: Create the minimal durable fixture and RED admission tests.**

```python
from datetime import timedelta
from types import SimpleNamespace
import pytest
from orchestrator.application import ControlPlaneApplication
from tests.unit.lifecycle.test_scheduler import (
    NOW, _application_limits, run_setup_with_frozen_contract, routed_pair)

def make_worker_input_case(database):
    app = ControlPlaneApplication(database, limits=_application_limits())
    reg, config, lifecycle, policy, contract = run_setup_with_frozen_contract(app._event_store)
    request, decision = routed_pair(reg, config, policy, contract_hash=contract.contract_hash)
    clock = SimpleNamespace(now=NOW + timedelta(seconds=2))
    return SimpleNamespace(app=app, request=request, decision=decision,
        lifecycle=lifecycle, clock=clock, text="implement a small change",
        identity=dict(run_id=request.run_id, node_id=request.node_id,
                      attempt_id=request.attempt_id, fencing_generation=1))

def admit_worker_input_case(case):
    case.accepted = case.app.accept_routing_with_input(case.request, case.decision,
        task_text=case.text, accepted_at=NOW + timedelta(seconds=1),
        lease_expires_at=NOW + timedelta(minutes=1))
    return case.accepted

# Put this fixture in tests/unit/runtime/test_worker_input_service.py;
# import the two helpers above from tests/support/worker_input.py.
@pytest.fixture
def case(tmp_path):
    value = make_worker_input_case(tmp_path / "input-events.db")
    try:
        yield value
    finally:
        value.app.close()
```

Add fixtures that close the app in finally. Starting test: admit, resolve_verification_binding at `case.clock.now`, assert its hash equals the canonical manifest of the durable node role and its required checks equal `(artifact-integrity,)`. Re-admit identical data and assert `_resolver_durable_state` plus BudgetLedger/Agent/slot state unchanged; re-admit changed text and assert WorkerInputError and the same state. Closed app raises ApplicationNotReady; unsafe requests/decisions, missing Node, graph/planning/role mismatch, no full planning contract and invalid times reject without persistent input or resource writes. Inspect the public Python signature and reject caller overrides rather than dropping them.

- [ ] **Step 2: Run RED for application admission and service tests.**

```bash
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest --override-ini=addopts= tests/unit/runtime/test_worker_input_service.py -q
```

Expected initial failure: new admission method/service missing. Fixture setup must use existing actual routing/Run snapshots so a synthetic-authority failure is not misreported as the intended RED.

- [ ] **Step 3: Implement admission as one outer serialized callback and wire the application.**

```python
result = None
def admit_under_lock(_events, _version):
    nonlocal result
    state = self._scheduler.lifecycle.replay(request.run_id)
    node = state.node(request.node_id)
    manifest = build_worker_input_manifest(run_id=request.run_id,
        node_id=request.node_id, role=node.spec.role, task_text=task_text)
    result = self._scheduler.accept_routing(request, decision,
        accepted_at=accepted_at, lease_expires_at=lease_expires_at,
        input_manifest_hash=manifest.content_hash,
        verification_contract=BUILTIN_VERIFICATION_CONTRACT_ID,
        required_check_ids=("artifact-integrity",))
    return None
self._scheduler.event_store.append_checked("scheduler", "global",
    "worker-input-admission-snapshot", admit_under_lock)
```

Revalidate routing models before entering, reject inconsistent Node/planning roles, and suppress raw underlying exceptions at the service boundary as WorkerInputError. The existing Scheduler performs all accepted-route/resource/context writes; no text/manifest/extra readiness or snapshot event is appended. The outer callback's None does not discard nested Scheduler writes; it commits or rolls them back together. Do not introduce a new connection, direct commit, lifecycle authority, or recursive import into the Worker child. Application constructs the service before bootstrap without running it and forwards admission only after `_require_ready()`.

- [ ] **Step 4: Add transaction-race and fresh-process admission crash tests.**

Use two independently opened connections created inside their owning threads/processes (SQLite `check_same_thread=True`). Add bounded Event/Pipe rendezvous, not time.sleep-based schedules, for identical admissions (one effect), conflicting text (one winner, loser rejected), cancellation winning before admission, and graph append winning before admission. Graph updates retain existing Scheduler semantics; they must not change the role/hash of an accepted Node.

Create module-level Linux spawn child functions in the owned recovery test. Close parent app before spawning. Child wraps its Scheduler.accept_routing: call the real function, then `block_at_crash_point(pipe, "after-nested-input-admission")` while the service's outer transaction still holds; parent uses existing `kill_at_crash_point()` and reopens with a fresh app. Assert no partial reservation, Agent, slot, or acceptance survived. A second child calls the real application admission and blocks at `"after-input-admission-commit"`; parent kills it, reopens, and retries exact admission, proving one durable commitment/effect. Only marker names/status/context hashes are sent back; no sentinel text is added to argv/environment/logs. Child cleanup is bounded and always reaps even when the test assertion fails.

- [ ] **Step 5: Run GREEN, old authority regressions and full coverage.**

```bash
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest --override-ini=addopts= tests/unit/runtime/test_worker_input.py tests/unit/runtime/test_worker_input_service.py tests/integration/test_worker_input_recovery.py tests/unit/lifecycle/test_scheduler.py -q
```

Verify rollback by real SQLite reopen, not only mock call counts. Search persistent SQLite payloads/log captures for the unique sentinel text; it must be absent. Keep legacy hash admission, proof gates, startup proposal audit and unknown-hold behavior unchanged. Run full coverage plus compileall/pip check before candidate commit.

- [ ] **Step 6: Document, commit, independently review and push this stage.**

Documentation states canonical admission is now available but no task preparation or functional Worker exists yet. Stage exactly the named new/modified files, commit `feat: bind worker inputs during atomic admission`, review the exact task BASE..HEAD, repair/review findings, push the reviewed HEAD to the existing codex branch, and record clean/local/actual remote evidence before Task 3.

### Task 3: Frozen task preparation, restart/race acceptance and handoff

**Files:** Modify `src/orchestrator/runtime/worker_input_service.py`, `src/orchestrator/runtime/contracts.py` (task-text repr only), `src/orchestrator/application.py`, `tests/support/worker_input.py`, `tests/unit/runtime/test_contracts.py`, `tests/unit/runtime/test_worker_input_service.py`, `tests/integration/test_worker_input_recovery.py`, `docs/security/worker-runtime.md`, `README.md`; update this plan's checkbox/delivery evidence only after verified steps.

**Interfaces:**

- Consumes Task 1 canonical manifest and bounds, Task 2 private application service/admission, existing `Scheduler.resolve_verification_binding(*, run_id, node_id, attempt_id, fencing_generation, as_of)`.
- Produces `WorkerInputService.prepare_worker_task(*, run_id: str, node_id: str, attempt_id: str, fencing_generation: int, task_text: str) -> WorkerTask`, forwarded by the same public application signature; no `as_of` or clock argument on that public method.
- Tests may set the private composed service `_clock` to deterministic trusted fixture time; production uses aware UTC and validates it. No new public execution capability is granted by a prepared task.

- [ ] **Step 1: Write RED exact preparation and read-only persistence tests.**

```python
def test_preparation_uses_exact_frozen_binding(case):
    admit_worker_input_case(case)
    case.app._worker_inputs._clock = lambda: case.clock.now
    before = _resolver_durable_state(case.app._event_store)
    task = case.app.prepare_worker_task(**case.identity, task_text=case.text)
    binding = case.app._scheduler.resolve_verification_binding(
        **case.identity, as_of=case.clock.now)
    assert task.context == binding.context
    assert task.role == "coder" and task.task_text == case.text
    assert task.input_artifacts == task.tool_capabilities == ()
    assert task.output_byte_limit == 16_384
    assert case.text not in repr(task)
    assert task.model_dump(mode="json")["task_text"] == case.text
    assert _resolver_durable_state(case.app._event_store) == before
```

Add a direct existing WorkerTask contract regression using its real contract fixture: sentinel absent from repr, text unchanged in JSON, and JSON round-trip equality. This fails before the repr-only fix even independently of the missing preparation method. Add missing text (required argument, no cache), mismatched text, cross-Run/Node/Attempt/generation, bool generation, Unicode normalization differences, closed app, legacy absent/arbitrary hash, malformed/missing planning/context, changed graph, swapped role and invalid/naive/non-UTC clock tests. All invalid cases assert durable event/idempotency/reservation/Agent/slot inventory unchanged. Monkeypatch execution/Gateway/tool/Broker/ArtifactStore publication entry points to fail if called; preparation must still produce only task data.

- [ ] **Step 2: Run RED for preparation tests.**

```bash
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest --override-ini=addopts= tests/unit/runtime/test_worker_input_service.py -q
```

Expected failing cases target missing preparation method, hash check, authoritative snapshot, or clock ordering; the already approved Task 2 admission tests continue passing.

Run the direct repr regression separately with `python -m pytest --override-ini=addopts= tests/unit/runtime/test_contracts.py -q` using the same named Python executable; record its expected plaintext-in-repr failure before changing contracts.py.

- [ ] **Step 3: Implement a no-write serialized preparation callback.**

```python
result = None
def prepare_under_lock(_events, _version):
    nonlocal result
    now = self._now()  # validate aware UTC; called AFTER outer lock acquisition
    binding = self._scheduler.resolve_verification_binding(**identity, as_of=now)
    state = self._scheduler.lifecycle.replay(identity["run_id"])
    node = state.node(identity["node_id"])
    manifest = build_worker_input_manifest(run_id=identity["run_id"],
        node_id=identity["node_id"], role=node.spec.role, task_text=task_text)
    if not hmac.compare_digest(manifest.content_hash, binding.context.input_manifest_hash):
        raise WorkerInputError("worker_input_mismatch")
    result = WorkerTask(context=binding.context, role=node.spec.role,
        task_text=task_text, input_artifacts=(), tool_capabilities=(),
        output_byte_limit=WORKER_INPUT_OUTPUT_BYTES)
    encoded = result.model_dump_json().encode("utf-8", errors="strict")
    if len(encoded) > MAX_WORKER_INPUT_FRAME_BYTES:
        raise WorkerInputError("worker_input_too_large")
    return None
self._scheduler.event_store.append_checked("scheduler", "global",
    "worker-input-preparation-snapshot", prepare_under_lock)
```

Validate all identities against AttemptContext's strict ASCII bounds before reads; `type(fencing_generation) is int and fencing_generation > 0`. Check Node role equals its frozen planning contract role and binding graph/config/Registry/planning identity before task construction. Only return the fresh revalidated result after outer transaction completes; missing result or any internal invariant fails sanitized. No cached input, append draft, child dispatch, grant, artifact access, tool/Provider, proposal, usage or release. Public application forwarding uses `_require_ready()`. Preparation validity is at the clock sample after acquiring serialization; dispatch must recheck independently in a future slice.

Change only `WorkerTask.task_text`'s existing field declaration to `Field(min_length=1, max_length=100_000, repr=False)`. Its plaintext is deliberately available to the caller in memory and explicit IPC serialization, but not incidental task repr. No wire field, validator, bound or execution authority changes. For the service's serialized-task bound branch, monkeypatch the constant in `orchestrator.runtime.worker_input_service` that the length check actually reads; pytest restores it afterwards.

- [ ] **Step 4: Pin lock-wait, cancellation and graph concurrency without flaky sleeps.**

For the expiry test, one owning thread/connection holds BEGIN IMMEDIATE. In another owning thread, construct the application before taking that lock, instrument the EventStore append_checked entry with a test-only Event marking "about to acquire outer preparation lock", then attempt preparation. Release the first lock only after moving the trusted fixture clock from an unexpired value to exactly the lease deadline. Assert WorkerInputError and no task. The worker's `_clock` call must be observed after the release marker; monkeypatch/barrier errors always release the blocker and bound joins to 10 seconds. Do not hold a lock while constructing a second app that requires startup locking.

Similarly test cancellation/graph append committed before preparation's serialization versus preparation completing first. If the mutation wins, reject; if preparation wins, only task data exists and a second preparation after mutation rejects. Do not demand an impossible guarantee that a task remains valid forever after returning. Use distinct SQLite connections and strict joins rather than sharing a connection across threads.

- [ ] **Step 5: Exercise fresh-process recovery and frozen-config/privacy invariants.**

Add module-level spawn child `reopen_and_prepare(database, text_or_none, trusted_fixture_time_iso, pipe)` in the owned recovery test. Construct a fresh ControlPlaneApplication, set only its private fixture clock, and return stable status plus context/output-limit metadata (not text) over the bounded test Pipe. Parent asserts exact re-supply succeeds while still active; missing, changed, expired, cancelled, terminal or stale inputs cannot prepare. All reopened cases make no startup Worker call and no automatic state promotion; no TTL resurrection or proposal-to-success is introduced.

Change an independent ConfigManager/file candidate while the original Run remains frozen; preparation must still carry original effective-config/Registry/planning hashes. Inject a preparation error after the authoritative binding read and verify an unchanged reopened database. Capture repr/formatted tracebacks and SQLite event/projection/idempotency/audit payloads for sentinel absence; include legacy/no-hash admission rejection without retrofitting. Preserve the explicit distinction from original planner text.

- [ ] **Step 6: Run final focused/full gates and update truthful product docs.**

```bash
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest --override-ini=addopts= tests/unit/runtime/test_worker_input.py tests/unit/runtime/test_contracts.py tests/unit/runtime/test_worker_input_service.py tests/integration/test_worker_input_recovery.py tests/unit/runtime/test_attempt_execution_coordinator.py tests/unit/lifecycle/test_scheduler.py tests/integration/test_isolated_worker_process.py tests/integration/test_isolated_verifier_process.py tests/integration/test_systemd_provider_sender.py -q
```

Run full coverage, compileall, pip check, normal wheel and diff check using the workspace commands. Save exact counts, percentage, skips, elapsed results and candidate SHA. Docs replace only the outdated "no production WorkerTask is constructed" statement with the now-measured preparation API, maintain blocked-only actual Worker and no semantic/product execution claims, and link the approved spec/this plan. Do not copy old acceptance test numbers into a new-results statement.

- [ ] **Step 7: Commit, task review, whole-branch review, repair gate and push.**

Commit the exact named product/tests/docs files with `feat: prepare worker tasks from frozen input authority`. Obtain Task 3 scoped review, then compute the whole-branch review BASE with `git merge-base origin/main HEAD` and review that exact `BASE..HEAD` range rather than only this last task. Provide the prior durable acceptance review/fix records to distinguish already reviewed unchanged boundaries, but do not waive review of the current complete diff or new integration edges. Preserve every finding and declined-scope ruling; a failed or interrupted review is incomplete, not permission to push.

Resolve substantive findings with focused RED/GREEN tests and one scoped final repair review; repeat gates affected by actual changes and get full exact-source coverage before final push. Push only the final reviewed codex commit; no merge or PR is implied. Record HEAD/tracking/actual remote match, clean checkout, phase deliverable, and remaining Worker/publisher/Gateway/V1 gaps. If closing tracking checkboxes needs a docs-only bookkeeping commit after push, independently check that small diff and push it too, without claiming new tests ran for a docs-only change.

## Spec coverage self-review

- Canonical schema, domain, identity/text exactness, strict/unsafe validation, bounds and errors: Task 1.
- Host-owned admission, frozen role/check binding, current graph and atomic/idempotent resources: Task 2.
- Under-lock UTC sample, exact binding/role/hash and bounded non-authoritative task: Task 3.
- Startup no-input/no-execution, restart re-supply, absent/legacy bindings, no mutation/retention/cache: Tasks 2/3.
- Explicit planner-authenticity, confidentiality and same-UID limitations: Tasks 1/3 documentation.
- Concurrency, crash points, sentinel privacy, full coverage/no-required-skip and stage review/push: each task plus Task 3 final review.
- Deferred functional execution, candidate publication, gateways/usage, complete recovery, seven roles, semantic checks, CLI/MCP and real paid benchmarks: documented exclusions, not dropped V1 requirements.

## Handoff gate

The user has already selected Subagent-driven and individual stage pushes. Preserve that method; do not ask again or silently switch to Native. This plan is complete only as a reviewed planning artifact until the user confirms it captures the approved design. After confirmation, execute Task 1 first using the named required execution skill and task-level reviews; user silence is not written-plan approval or permission for any excluded action.
