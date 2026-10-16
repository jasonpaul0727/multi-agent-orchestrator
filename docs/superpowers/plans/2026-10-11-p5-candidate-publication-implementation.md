# P5 Trusted Candidate Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a trusted, ready host retain a bounded Worker candidate and atomically publish its complete set under the original Attempt's current authority, with durable replay and no execution or success authority.

**Architecture:** First remove SQLite-to-artifact-lock recovery paths and provide dynamically discovered database snapshots. Then introduce the bounded candidate journal, historical prefix audit and every read/admission gate before introducing any producer. Finally implement durable preparation, retained installation and guarded atomic finalization behind the ready-gated application API.

**Tech Stack:** Python >=3.12, existing Pydantic >=2.7,<3 and PyYAML >=6.0,<7, SQLite WAL/event streams, Linux filesystem locks/fsync, pytest, coverage.py, existing systemd integration tests. No new dependency or migration.

**Spec:** `docs/superpowers/specs/2026-10-11-p5-candidate-publication-design.md`, delivered at `8312d899960be6e5d60d46cf6fa0c5244d5cc4a1`; SHA-256 `8b83bfcbd3794374195bb15348222d132b0b529bdb4084db75bab151398addba`. The spec's older source BASE and draft review-status prose describe its design origin; the planning brief records completed independent architecture review and the delivered binding revision. Neither this plan nor its report claims implementation acceptance.

## Global Constraints

The following binding statements are copied verbatim from the spec:

- “A trusted host publication prerequisite. Production Worker execution remains blocked-only.”
- “`payload` is exact nonempty `bytes`, maximum 1,048,576 bytes; identity fields use the exact existing AttemptContext identifier/generation rules.”
- “Clock injection is composition/test-only and must return aware UTC; request callers cannot backdate a decision.”
- “The current narrow profile requires the original `maestro.artifact-verification/v1` binding with exactly `("artifact-integrity",)` required checks and empty inputs/capabilities; it cannot retrofit a missing original admission.”
- “Version 1 uses one host-owned policy `maestro.candidate-policy.v1`:”
- “Every proposed reference must specify `artifact_type="worker-candidate"` and `media_type="application/octet-stream"`. Other values reject.”
- “The host recomputes digest and size. It supplies `schema_version=1`, `redaction_state="unknown"`, `readable_scope=(run_id,)`, `references=()`, and `lifecycle_state="temporary"`.”
- “`source` contains the five existing exact provenance fields (generation encoded as its canonical positive decimal string), plus `publication_protocol="maestro.candidate-publication.v1"` and `candidate_set_id`.”
- “`created_at` is the host preparation time, frozen for retry; `publication_id` is deterministic as defined below.”
- “Use a new `candidate_publications` stream per Run, with two permitted event types: `CandidatePublicationPrepared` and `CandidatePublicationCommitted`, schema `maestro.candidate-publication.v1`.”
- “Canonical JSON everywhere in this protocol means sorted keys, compact separators, `ensure_ascii=False`, `allow_nan=False`, strict UTF-8. Sort member references by digest; reference order in a received WorkerResult is not set identity. Let `H(tag, value)` be `sha256:` plus lowercase SHA-256 of UTF-8 `tag`, a zero byte, then canonical JSON of `value`.”
- “`request_id = H("maestro.candidate-request.v1", {run_id,node_id,attempt_id,fencing_generation})`.”
- “`result_id_hash = H("maestro.candidate-result-id.v1", result.result_id)`.”
- “`set_id = H("maestro.candidate-set.v1", {request_id,context,result_id_hash,policy_id,output_byte_limit,members})`.”
- “Each member's `publication_id` is `candidate-v1:` plus the hexadecimal part of `H("maestro.candidate-member.v1", {set_id,digest})`.”
- “The prepared payload has exactly `schema`, `request_id`, `set_id`, `context`, `result_id_hash`, `policy_id`, `output_byte_limit`, `reservation_id`, `accepted_route_id`, `accepted_at`, `lease_expires_at`, `prepared_at`, `authority_heads`, `members`, and `records`.”
- “`records` is the sorted complete `ArtifactRecord.to_event_payload()` list, one per member. All timestamps are aware UTC ISO strings normalized to six fractional digits and `+00:00`; other representations reject in this new protocol.”
- “`prepared_hash = H("maestro.candidate-prepared.v1", prepared_payload)` is referenced by the commit.”
- “The committed payload has exactly `schema`, `request_id`, `set_id`, `prepared_hash`, `authority_heads`, and `committed_at`.”
- “Idempotency keys are `candidate-prepare:<request hex>` and `candidate-commit:<request hex>`. Headers require exact Run/Node/Attempt/generation, `correlation_id=run_id`, and `causation_id=set_id`; stream ID must equal Run.”
- “`authority_heads` is the sorted exact five-element array for streams `run/<run_id>`, `run_lifecycle/<run_id>`, `agent_registry/<run_id>`, `budget/<run_id>` and `scheduler/global`, sampled within the respective preparation/final transaction after authority validation. Each element has exactly `stream_type`, `stream_id`, positive integer `version`, `event_id` and `payload_hash` of that head.”
- “Referenced heads must exist and match exactly; commit heads cannot precede preparation heads.”
- “The initial transaction also appends each corresponding existing `ArtifactPublicationIntent` with its complete frozen record under `artifact-intent:<publication_id>`.”
- “The final transaction appends matching `ArtifactPublished` events under `artifact-publication:<publication_id>`.”
- “New candidate artifact events use the same complete execution headers as their set events.”
- “Identical concurrent requests converge on one preparation and one commitment. Same Attempt with changed result ID, context, bytes, count or metadata conflicts, including after a crash; no second set is admitted.”
- “Reordered references/blobs or JSON whitespace produce the same identity after decoding.”
- “Required global rule: **never acquire a digest lock or the publication mutex while owning a SQLite transaction**.”
- “No operation may hold the process mutex while waiting for a second digest lock.”
- “Retry collection at most three times; continuous concurrent changes return a stable unavailable error, with no skipped checks.”
- “Every multi-stream candidate read must use one database-only snapshot spanning the first journal/member discovery read, preparation/commit replay, every member intent/publication, and relevant preparation-index/namespace checks.”
- “Reuse an existing outer transaction on the same connection when present; nested reads must not end the caller's transaction or establish a later view.”
- “A full store read or verifier path invoked under an outer transaction must fail closed before such work rather than close a caller-owned transaction.”
- “The **authority linearization point** is this final in-transaction guarded decision, conditional on the outer transaction committing.”
- “The **visibility point** is the SQLite commit of all publications and the set marker.”
- “Preserve decoder frame/content/count bounds: 1 MiB raw frame, 1..128 unique members, aggregate decoded bytes at most 16,384, and existing per-field strict limits.”
- “The new journal allows at most 1,024 prepared requests per Run and at most 16 MiB of canonical candidate journal payloads, with at most 1 MiB per event payload.”
- “Preparation reserves a conservative 16,384-byte allowance for its possible commit, including its five authority heads; that allowance counts toward the Run limit until replaced by the actual commit payload, which must fit it.”
- “Head event identifiers are bounded to 128 characters in this protocol; payload hashes are exactly the existing 64 lowercase hex digits, and stream names/IDs have the fixed names or existing Run identifier bound.”
- “Identical retries at the cap remain no-ops; failed/conflicting requests do not grow durable history.”
- “There are at most two candidate events and two artifact events per member for a request.”
- “Installed candidate content is at most 16 MiB per capped Run, excluding preexisting/shared bytes and private crash leftovers; no automatic deletion policy is introduced.”
- “Expose only fixed `CandidatePublicationError.code` literals: `candidate_publication_invalid`, `candidate_publication_too_large`, `candidate_publication_conflict`, `candidate_publication_inactive`, `candidate_publication_unavailable`, and `candidate_publication_uncertain`.”
- “Raise a fresh allowlisted error after leaving owned exception handlers; `from None` alone is insufficient.”
- “The existing production stdout budget is 16,384 encoded bytes. A 16,384-byte binary artifact requires 21,848 Base64 characters before JSON and metadata.”

Additional binding execution constraints: work only in `/mnt/c/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator`, WSL Git, branch `codex/p3-systemd-termination-receipts`, initial BASE above. Every Python command uses `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`. No shared-venv installs; normal PEP517 isolation may bootstrap only its temporary build environment. Do not edit main, Worker transport, production usage/settlement policy or unrelated subsystems. Do not execute arbitrary candidate commands, use credentials, invoke a paid Provider, deploy, merge, or create a PR. Candidate APIs accept no model instances, path, grant, receipt, role, source override, caller clock or no-effect claim. No final-object deletion, automatic publication/retry, startup execution, success/usage/resource event, input-text persistence, or public recovery bypass.

## Review Focus

1. A reader discovers a prepared set just before another connection commits it: keep discovery and every dynamically discovered member in the same snapshot, including an outer caller-owned transaction. Task 1 `test_dynamic_snapshot_owns_only_its_transaction`; Task 2 `test_candidate_read_race_is_one_view`.
2. An old generic publication and a pending candidate share bytes, possibly across Runs: preserve the old Run's independently authorized grant, but reject pending candidate admission and cross-Run provenance. Task 2 `test_shared_digest_does_not_transfer_authority`.
3. Metadata work advances the clock or SQLite waits across lease expiry: persist the exact final guarded clock sample or roll back every new publication. Task 3 `test_final_clock_sample_controls_whole_commit` and `test_lock_wait_crosses_lease_deadline`.
4. A frozen/subclassed/unsafe constructed value, forged private result, or sensitive backend error reaches any new trust crossing: recursively rebuild expected schemas, reject extras/type substitutions, and detach the final owned error. Task 2 `test_constructed_models_cannot_bypass_set_gate`; Task 3 `test_decoder_and_private_result_are_not_authority` and `test_owned_errors_are_content_free`.
5. A committed historical set survives a later graph append/cancellation while its old preparation heads remain valid: replay exact per-stream prefixes, reject modified heads/orphan links, and return the historical receipt without an active check. Task 2 `test_historical_heads_use_prefix_reducers`; Task 3 `test_committed_retry_after_expiry_is_read_only`.

---

## Repository map and serialized delivery boundaries

The source calls to full recovery are `Scheduler.resolve_active_attempt` (core.py:202), `accept_routing` (:286), `plan_attempt_recovery` (:572), and `ControlPlaneApplication._recover_all_runs` (application.py:176). Indirect outer owners are `resolve_verification_binding`, `finish_verified_attempt`, both WorkerInputService methods and application `_bootstrap`. `AttemptExecutionCoordinator.accept_proposal` calls resolution before grants and terminal completion later; it does not own an outer transaction. Provider reconciliation startup settles through Scheduler while `_bootstrap` owns the transaction. No public `skip_artifacts` argument is introduced.

`SQLiteEventStore.read_streams_consistent` (sqlite_event_store.py:762) currently takes a fixed key list. `ArtifactStore._digest_lock` (store.py:1213) already acquires the digest OS lock then `_PUBLICATION_LOCK`; callers must not acquire the mutex separately or nest digest contexts. Existing `_record_metadata` (:1013) mutates `_records` before an enclosing transaction necessarily commits. `read_bytes/get_record/list_publications/verify_run_artifacts` load metadata independently; all need the coherent candidate helper. `pending_publications_for_run` (:886) currently combines parsing and filesystem observation under digest locks.

Existing reducer reuse: `apply_lifecycle_event` in lifecycle/controller.py, `reduce_agent_registry` in agents/registry.py, `BudgetLedger._replay_states/_balance_from_events` in budget/ledger.py and `RunRecoveryCoordinator._scheduler_state` in recovery/run.py. Factor pure prefix entry points from these, never a temporary database, fake store, live snapshot mutation or synthetic cross-stream ordering. The immutable RunConfigSnapshot is parsed from the first RunCreated event; prefix reads must still validate every referenced Run-stream event/head.

| Stage | Deliverable | Primary owned files |
| --- | --- | --- |
| 1 | Ordinary recovery/read/authority operations obey the lock order and dynamic snapshot ownership; prefix reducers are available and tested | persistence/sqlite_event_store.py; artifacts/store.py; recovery/run.py; recovery/artifact_inventory.py (new); lifecycle/controller.py; budget/ledger.py; scheduler/core.py; runtime/worker_input_service.py; application.py |
| 2 | Strict candidate history can be audited and every consumer rejects incomplete/changed sets; no candidate producer exists | artifacts/candidate_schema.py, candidate_journal.py, candidate_authority.py (new); persistence/events.py; stage-1 integration files; runtime/artifact_admission.py, verifier_process.py, acceptance_coordinator.py |
| 3 | Complete trusted publisher, retained install, status, historical retry and crash/race coverage | runtime/candidate_publication.py (new); artifacts/store.py; application.py; focused new test modules |

All paths in the table are under `src/orchestrator/`. Stages are serialized and reviewed/pushed individually. Stage 2 includes all gates in one reviewable change; never land a producer in advance of these gates. Each task owns its tests and relevant additions to `docs/security/worker-runtime.md`. A stage ends only after the common final-source gate, implementer's tested local commit, independent committed-range task review, source-fix reacceptance/local commit/scoped review if necessary, and controller push verification. Stage 3 also receives independent whole-slice review.

## Shared execution and acceptance procedure (required at every source stage)

Run each command from the isolated WSL checkout. Save actual RED/GREEN commands, outputs, timing, counts, skipped/warning summary, diff scope, source hashes and review dispositions in `.superpowers/sdd/2026-10-11-p5-candidate-publication-implementation/task-N-report.md` for the current numbered task. These are future execution artifacts, not results of this planning turn.

- [ ] Before source edits, record `git rev-parse HEAD`, `git status --short`, and exact task scope; preserve unrelated changes. Set `maestro_stage` to `1`, `2` or `3` for the current task and create its evidence directory. Freeze the final source/test/config bytes before the acceptance run using the existing files plus all new task files:

```bash
maestro_stage=1
maestro_evidence=".superpowers/sdd/2026-10-11-p5-candidate-publication-implementation/stage-$maestro_stage"
mkdir -p "$maestro_evidence"
git rev-parse HEAD > "$maestro_evidence/base.txt"
find src/orchestrator tests -type f -name '*.py' -print0 | sort -z | xargs -0 sha256sum > "$maestro_evidence/source.sha256"
sha256sum pyproject.toml >> "$maestro_evidence/source.sha256"
```

- [ ] Run the task's final focused command, then the complete suite and coverage gate on these frozen bytes. Required existing live systemd tests must execute with zero skips/warnings; unavailable isolation is a failed gate, not a skipped success. There is no pytest-cov dependency.

```bash
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m coverage run -m pytest --override-ini=addopts= -q -ra -W error
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m coverage report --fail-under=90
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m compileall -q src tests
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pip check
git diff --check
git diff --check 8312d899960be6e5d60d46cf6fa0c5244d5cc4a1
sha256sum --check "$maestro_evidence/source.sha256"
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -c 'import pathlib, orchestrator; assert pathlib.Path(orchestrator.__file__).resolve().is_relative_to(pathlib.Path("src").resolve()); print(orchestrator.__file__)'
```

- [ ] Build the wheel in a new retained temporary directory. Compare every packaged orchestrator Python file to this checkout, including newly introduced files; do not install it in the shared environment.

```bash
maestro_wheel_dir=$(mktemp -d /tmp/maestro-candidate-wheel.XXXXXX)
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m build --wheel --outdir "$maestro_wheel_dir"
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -c 'import pathlib,sys,zipfile; root=pathlib.Path("src"); wheels=list(pathlib.Path(sys.argv[1]).glob("*.whl")); assert len(wheels)==1; z=zipfile.ZipFile(wheels[0]); files=sorted(root.joinpath("orchestrator").rglob("*.py")); expected={p.relative_to(root).as_posix() for p in files}; actual={n for n in z.namelist() if n.startswith("orchestrator/") and n.endswith(".py")}; assert expected==actual; assert all(z.read(p.relative_to(root).as_posix())==p.read_bytes() for p in files); print(len(files), "packaged source files equal")' "$maestro_wheel_dir"
sha256sum --check "$maestro_evidence/source.sha256"
```

- [ ] Self-review the complete task diff and test results, then the implementer stages only the task's listed files and creates the tested local task commit using its suggested message. Record `git rev-parse HEAD` and verify every committed source/test/config blob against `source.sha256` with `git show HEAD:<path>` and SHA-256, including newly tracked files. Send the actual task BASE..HEAD and full frozen-byte evidence to the controller. The controller obtains independent committed-range task review with both spec-compliance and code-quality verdicts. The original implementer resolves each accepted finding within task scope. Every source/test fix gets covering tests, a fresh final-source full gate, a new tested local fix commit and scoped committed-range review; unchanged source evidence is reused after review, not rerun/redated for review-only or documentation changes. Never quote earlier acceptance numbers as current publisher evidence. Local commits are not permission to push.

- [ ] After independent review passes, the controller verifies the implementer's actual reviewed local HEAD and committed source/test/config blobs against `source.sha256`, then pushes that reviewed stage. Record actual `git rev-parse HEAD`, `git rev-parse @{upstream}`, `git ls-remote --heads origin codex/p3-systemd-termination-receipts` and `git status --short`. All three SHAs must agree and tracked status must be clean. An implementer never pushes. Suggested stage messages are specified below. Source acceptance binds to the reviewed actual SHA plus the frozen manifest, not a guessed future SHA. No post-review source edit can ride this push without its own gate, local commit and scoped review.

## Task 1: Database snapshots, outside-transaction inventory and prefix replay prerequisites

**Files:**

- Modify: `src/orchestrator/persistence/sqlite_event_store.py:762` (dynamic snapshot and transaction state), `src/orchestrator/artifacts/store.py:609` (entry guards), `:819` and `:886` (separate metadata from filesystem observation).
- Create: `src/orchestrator/recovery/artifact_inventory.py` (private store-bound capture/witness scope).
- Modify: `src/orchestrator/recovery/run.py:113` and `:475` (database-only recovery and scheduler reducer), `src/orchestrator/lifecycle/controller.py:86`, `src/orchestrator/budget/ledger.py:1346` (prefix helpers).
- Modify: `src/orchestrator/scheduler/core.py:136,256,535,753,958`, `src/orchestrator/runtime/worker_input_service.py:40,119`, `src/orchestrator/application.py:127` (all outer owners).
- Create: `tests/support/candidate_publication.py`, `tests/unit/persistence/test_dynamic_snapshot.py`, `tests/integration/test_artifact_recovery_lock_order.py`, `tests/unit/recovery/test_artifact_inventory.py`.
- Modify: `tests/unit/lifecycle/test_scheduler.py`, `tests/unit/runtime/test_worker_input_service.py`, `tests/integration/test_recovery.py`, `docs/security/worker-runtime.md`.

**Interfaces (all new unless explicitly marked existing):**

| Producer | Exact interface and responsibility |
| --- | --- |
| SQLiteEventStore | `read_snapshot(self) -> AbstractContextManager[None]`; `in_transaction: bool` property; `_has_checked_write_owner(self) -> bool` backed by private callback-depth accounting in append_checked. Own read BEGIN/commit/rollback only when initially outside a transaction; otherwise reuse outer view without commit/rollback. No filesystem work. |
| SQLiteEventStore | `validated_stream(self, stream_type: str, stream_id: str) -> tuple[StoredEvent, ...]`; verify current_version equals length, consecutive versions and existing validate_event_contract. Intended for bounded protocol streams/prefix inputs; no new ordering across streams. |
| ArtifactStore | `_inventory_metadata(self) -> ArtifactMetadataInventory`; one database snapshot captures all artifact streams, their validated heads and membership, parsed publication/intent identity lists. `_observe_inventory(self, metadata: ArtifactMetadataInventory) -> ArtifactObservations`; outside SQLite, verify published files and pending files under one digest lock at a time. |
| recovery/artifact_inventory.py | Frozen internal `StreamWitness(stream_type: str, stream_id: str, version: int, event_id: str | None, payload_hash: str | None, entries: tuple[tuple[int, str, str], ...])`; entries are every validated event's version/event_id/payload_hash, not just the head. `ArtifactMetadataInventory(witness: tuple[StreamWitness, ...], publications: tuple[ArtifactRecord, ...], pending: tuple[tuple[StoredEvent, ArtifactRecord], ...])`; `ArtifactObservations(publications: tuple[ArtifactRecord, ...], pending: tuple[PendingArtifactPublication, ...])`. These are data, never public authority. |
| RunRecoveryCoordinator | `_checked(self, run_ids: tuple[str, ...] | None, stream_type: str, stream_id: str, key: str, decide: Callable[[list[StoredEvent], int], Iterable[EventDraft] | None]) -> list[StoredEvent]`. Own outer capture/retry/write transaction; nested calls require its exact active private scope. `None` means discover all Run/config/lifecycle membership for startup. |
| RunRecoveryCoordinator | `_recover_in_transaction(self, run_id: str) -> RecoveredRun`; requires SQLite ownership and registered matching inventory scope; never collects files. Existing `recover(run_id)` retains its public full verification result and uses `_checked((run_id,), "scheduler", "global", "recover:" + run_id, decide)`, whose callback captures the result and returns None. |
| RunRecoveryCoordinator | `_capture_inventory(self, run_ids: tuple[str, ...] | None) -> _RecoveryInventory`; `_validate_inventory(self, inventory: _RecoveryInventory) -> None`; `_RecoveryInventory` holds exact coordinator, store and ArtifactStore object identities, member lists, witness, observations and private nonce. Capture registers the exact returned object in `_captured`; validation requires that identity, and nested recovery additionally requires `_scope is inventory`. `_checked` clears both registrations in finally on every attempt. No public parameter accepts it. |
| lifecycle/controller.py | `config_snapshot_from_events(run_id: str, events: tuple[StoredEvent, ...]) -> RunConfigSnapshot`; existing `reduce_lifecycle(run_id: str, events: list[StoredEvent]) -> RunLifecycleState` at line 579 is the prefix fold. Reuse it directly, with no new duplicate reducer and no persisted snapshot shortcuts for prefixes. |
| budget/ledger.py | `replay_budget_events(run_id: str, events: tuple[StoredEvent, ...], limit: RunLimit) -> tuple[BudgetBalance, tuple[BudgetReservation, ...]]`; uses existing `_balance_from_events` and `_replay_states`, after contract/sequence validation. |
| recovery/run.py | `reduce_scheduler_attempts(run_id: str, events: tuple[StoredEvent, ...]) -> tuple[dict[tuple[str, str], dict[str, object]], dict[str, dict[str, object]]]`; exact existing `_scheduler_state` loop, now passed events. Existing `_scheduler_state` delegates to it. |
| tests/support/candidate_publication.py | `candidate_case(tmp_path: Path) -> SimpleNamespace`; `candidate_frame(task: WorkerTask, contents: tuple[bytes, ...], *, result_id: str = "result-1", reverse: bool = False) -> bytes`; `event_counts(store: SQLiteEventStore) -> dict[tuple[str, str], int]`; `assert_held(case) -> None`. Concrete builders below. |

- [ ] **Step 1: Write the snapshot ownership regression first.** Use separate connections to the same real WAL database; opening them must precede outer write locks. This test demonstrates dynamic discovery and both nested ownership variants without waiting for an impossible competing writer commit.

```python
from contextlib import nullcontext
import pytest
from orchestrator.persistence import EventDraft, SQLiteEventStore

@pytest.mark.parametrize("outer", [None, "BEGIN", "BEGIN IMMEDIATE"])
def test_dynamic_snapshot_owns_only_its_transaction(tmp_path, outer):
    a = SQLiteEventStore(tmp_path / "events.db")
    b = SQLiteEventStore(tmp_path / "events.db")
    try:
        a.append("probe", "first", 0, [EventDraft("Probe", {"value": 1})], "one")
        if outer:
            a._connection.execute(outer)
        with a.read_snapshot():
            assert a.stream_ids("probe") == ["first"]
            if outer != "BEGIN IMMEDIATE":
                b.append("probe", "second", 0, [EventDraft("Probe", {"value": 2})], "two")
            assert a.stream_ids("probe") == ["first"]
            assert a.read_stream("probe", "second") == []
        assert a.in_transaction is (outer is not None)
        if outer:
            a._connection.rollback()
        if outer == "BEGIN IMMEDIATE":
            b.append("probe", "second", 0, [EventDraft("Probe", {"value": 2})], "two")
        assert a.stream_ids("probe") == ["first", "second"]
        if outer:
            a._connection.execute(outer)
        with pytest.raises(RuntimeError, match="fixture failure"):
            with a.read_snapshot():
                a.read_stream("probe", "first")
                raise RuntimeError("fixture failure")
        assert a.in_transaction is (outer is not None)
    finally:
        if a._connection.in_transaction:
            a._connection.rollback()
        a.close()
        b.close()
```

- [ ] **Step 2: Run RED.**

```bash
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/persistence/test_dynamic_snapshot.py -q
```

Expected failure: `SQLiteEventStore` has no `read_snapshot`/`in_transaction`. Do not treat an import/path error as the intended RED.

- [ ] **Step 3: Add the snapshot primitive and validated stream helper; refactor fixed-key reads to use it.** Add contextlib imports and these bodies inside SQLiteEventStore. The complete dynamic lookup body must be inside `with read_snapshot()`, not only its final known-key fetch.

```python
@property
def in_transaction(self) -> bool:
    return self._connection.in_transaction

@contextmanager
def read_snapshot(self):
    owned = not self._connection.in_transaction
    if owned:
        self._connection.execute("BEGIN")
    try:
        yield
        if owned:
            self._connection.commit()
    except BaseException:
        if owned:
            self._connection.rollback()
        raise

def validated_stream(self, stream_type, stream_id):
    with self.read_snapshot():
        events, version = self.read_stream_with_version(stream_type, stream_id)
        actual_count = self._connection.execute(
            "SELECT COUNT(*) FROM events WHERE stream_type=? AND stream_id=?",
            (stream_type, stream_id)).fetchone()[0]
        if version != len(events) or actual_count != len(events) or any(
            event.stream_version != position
            for position, event in enumerate(events, 1)
        ):
            raise EventIntegrityError("stream version mismatch")
        validate_event_contract(events)
        return tuple(events)
```

In `read_streams_consistent`, retain all existing input validation; replace its transaction body with `with self.read_snapshot(): return {key: self.read_stream(*key) for key in keys}`. A nested exception leaves the caller's transaction open; this read-only helper never undoes the caller's writes. Add tests for malformed/missing event versions using an isolated test database, preserving immutable trigger definitions when injecting corruption.

- [ ] **Step 4: Introduce public test data builders.** Add these bodies to `tests/support/candidate_publication.py`; imports of existing public `run_setup_with_frozen_contract`, `routed_pair`, and `NOW` are allowed. Do not import a private fixture from another test module. In added tests within test_scheduler.py, import candidate_publication support locally inside test functions so this existing public fixture dependency does not create a module-initialization cycle. No fixture extraction or fourth support module is required.

Every test module containing a `ControlPlaneApplication(...)` snippet below must explicitly import `ConcurrencyLimits` with `from orchestrator.scheduler import ConcurrencyLimits` (retain an existing identical import rather than duplicate it). All opens, reopens and spawned-child constructors pass the explicit `8/4/8/4` limits shown; child functions construct their own instance in the child. This is test configuration, not a new production default. A TypeError or failed child setup is a test failure, never an expected security rejection or successful crash rendezvous.

```python
import base64
import hashlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from orchestrator.application import ControlPlaneApplication
from orchestrator.scheduler import ConcurrencyLimits
from orchestrator.runtime.contracts import ArtifactRef, WorkerResult
from tests.unit.lifecycle.test_scheduler import NOW, routed_pair, run_setup_with_frozen_contract

def candidate_case(tmp_path):
    app = ControlPlaneApplication(
        tmp_path / "control.db", artifact_root=tmp_path / "artifacts",
        limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4,
                                 provider_active_attempts=8, tool_active_attempts=4),
    )
    reg, config, lifecycle, policy, contract = run_setup_with_frozen_contract(app._event_store)
    request, decision = routed_pair(reg, config, policy, contract_hash=contract.contract_hash)
    text = "implement a small change"
    epoch = datetime.now(timezone.utc)
    accepted = app.accept_routing_with_input(
        request, decision, task_text=text, accepted_at=epoch,
        lease_expires_at=epoch + timedelta(hours=1),
    )
    clock = SimpleNamespace(now=epoch + timedelta(seconds=1))
    app._worker_inputs._clock = lambda: clock.now
    identity = dict(run_id=request.run_id, node_id=request.node_id,
                    attempt_id=request.attempt_id, fencing_generation=1)
    task = app.prepare_worker_task(**identity, task_text=text)
    return SimpleNamespace(app=app, store=app._event_store, artifacts=app._artifact_store,
        identity=identity, task=task, text=text, clock=clock, accepted=accepted,
        request=request, decision=decision, contract=contract, policy=policy, registry=reg, config=config,
        database=tmp_path / "control.db", root=tmp_path / "artifacts")

def candidate_frame(task, contents, *, result_id="result-1", reverse=False):
    refs = tuple(ArtifactRef(digest="sha256:" + hashlib.sha256(body).hexdigest(),
        size_bytes=len(body), artifact_type="worker-candidate",
        media_type="application/octet-stream") for body in contents)
    result = WorkerResult(context=task.context, result_id=result_id,
                          outcome="candidate", artifacts=refs)
    blobs = [dict(digest=ref.digest, content_base64=base64.b64encode(body).decode("ascii"))
             for ref, body in zip(refs, contents)]
    value = result.model_dump(mode="json")
    if reverse:
        value["artifacts"].reverse()
        blobs.reverse()
    return json.dumps(dict(schema="maestro.worker-output.v1", result=value, blobs=blobs),
                      ensure_ascii=False).encode("utf-8")

def event_counts(store):
    return {(row[0], row[1]): row[2] for row in store._connection.execute(
        "SELECT stream_type, stream_id, current_version FROM stream_versions")}

def assert_held(case):
    run = case.app._scheduler.recovery.recover(case.identity["run_id"])
    assert len(run.active_attempts) == 1
    assert run.active_attempts[0].status == "active"
    assert run.lifecycle.node(case.identity["node_id"]).attempts[-1].status == "accepted"
    assert run.budget.used_minor == 0
```

- [ ] **Step 5: Split artifact metadata collection from observation.** Move intent/publication pairing from `pending_publications_for_run` to `_inventory_metadata` and move its lstat/rehash/content-state branch to `_observe_inventory`. Capture every artifact stream, including streams with only foreign-Run records. Capture candidate streams now as opaque validated stream witnesses; Task 2 adds candidate replay and applies candidate byte/count preflight before their first inventory decoding as well as before journal reads. Capture complete `run`, `run_lifecycle` and `candidate_publications` memberships, including empty sets. Store exact head version/event_id/hash and each validated event's version/ID/payload_hash; an empty witness has version zero, None head fields and empty entries. Compare all entries under the authority transaction, so changed earlier records cannot hide behind an unchanged head. Revalidate complete stream/record fields before accepting them. Snapshot closure precedes the first `_digest_lock`; observation loops over sorted unique digests and enters exactly one existing `_digest_lock`, which already owns the mutex.

Under that digest lock, the observer checks the captured published records with `_verify_file_and_size`, and constructs PendingArtifactPublication for each unmatched identity. `already_published` describes shared content presence only; it never closes another identity's pending intent. Public `verify_run_artifacts` and `pending_publications_for_run` use this exact materialized view and filter by Run after observation. Do not reload another metadata list during observation. Recheck the witness in the owning recovery transaction, not while holding all locks.

Add the following guard at the start of `publish_bytes`, `_digest_lock`, all full filesystem read/metadata/inventory methods and `_observe_inventory`, before temp creation, path access, grant verification or acquiring the mutex:

```python
def _require_no_transaction(self):
    if isinstance(self._event_store, SQLiteEventStore) and self._event_store.in_transaction:
        raise ArtifactMetadataError("artifact operation requires closed database transaction")
```

For generic tiny append-only adapters, preserve current noncandidate behavior; candidate paths in Task 2 require a real SQLiteEventStore. Add same-thread lock-depth tracking to `_digest_lock` before OS acquisition: if this ArtifactStore operation already holds a digest lock in this thread, fail before acquiring another. Use a module `threading.local()` depth shared across instances because `_PUBLICATION_LOCK` is process-wide. Decrement in the existing finally path. This prevents crossed-digest nesting, including across store instances, without changing single-digest use.

- [ ] **Step 6: Implement private recovery witness scopes and adapt every outer owner.** `_capture_inventory` is forbidden while `event_store.in_transaction`; `_validate_inventory` requires it. The inventory includes all artifact/candidate stream witnesses and full Run/config/lifecycle memberships. Compare exact current memberships and validated heads within the write lock. Changes raise new internal `InventoryChanged(RunRecoveryError)`, with fixed text. The witness's owner/store/artifact-store references must be identical to the operation's composition; public constructed values cannot set the active scope. Each capture creates a fresh nonce; nested calls can only use the exact object currently held by `_checked`.

Use the following `_checked` control flow, with `_scope` initialized to None in the coordinator constructor. The concrete `_capture_inventory`, `_validate_inventory`, and `_recover_in_transaction` bodies are in R2-A; none accepts caller-provided bypass flags.

```python
def _checked(self, run_ids, stream_type, stream_id, key, decide):
    if self.event_store.in_transaction:
        if self._scope is None:
            raise RunRecoveryError("recovery inventory is unavailable in transaction")
        self._validate_inventory(self._scope)
        return self.event_store.append_checked(stream_type, stream_id, key, decide)
    for attempt in range(3):
        inventory = self._capture_inventory(run_ids)
        def checked(events, version):
            self._validate_inventory(inventory)
            self._scope = inventory
            try:
                return decide(events, version)
            finally:
                self._scope = None
        try:
            return self.event_store.append_checked(stream_type, stream_id, key, checked)
        except InventoryChanged:
            if attempt == 2:
                break
        finally:
            self._captured = None
    raise RunRecoveryError("recovery inventory unavailable")
```

Preserve an existing scope while nesting (the nested branch never replaces it). `_recover_in_transaction` validates scope identity and uses its verified publications/pending observations in the existing provenance checks. It replays current DB lifecycle/Agent/budget/scheduler/effects normally; it does not substitute empty inventories. `recover()` called inside that scope invokes `_recover_in_transaction`; called inside an unrelated raw SQLite transaction fails closed. Outside SQLite, `recover()` wraps `_recover_in_transaction` in `_checked` and returns its captured result only after success. No retry occurs after a commit-response failure, and only InventoryChanged triggers the three-attempt recapture loop.

Replace the existing outer append calls with these exact calls, retaining each existing callback body except its recovery call now consumes the scope:

```python
# Scheduler.resolve_active_attempt
self.recovery._checked((run_id,), _SCHEDULER_STREAM, _SCHEDULER_ID,
                       f"resolve:{attempt_ref}", resolve)
# Scheduler.accept_routing
self.recovery._checked((request.run_id,), _SCHEDULER_STREAM, _SCHEDULER_ID,
                       idempotency_key, decide)
# Scheduler.plan_attempt_recovery
self.recovery._checked((run_id,), _SCHEDULER_STREAM, _SCHEDULER_ID,
                       idempotency_key, decide)
# Scheduler.resolve_verification_binding
self.recovery._checked((run_id,), _SCHEDULER_STREAM, _SCHEDULER_ID,
                       f"verification-binding:{attempt_ref}", resolve)
# Scheduler.finish_verified_attempt
self.recovery._checked((run_id,), _SCHEDULER_STREAM, _SCHEDULER_ID, key, decide)
# WorkerInputService.prepare_worker_task
self._scheduler.recovery._checked((run_id,), "scheduler", "global",
                                 "worker-input-preparation-snapshot", prepare_under_lock)
# WorkerInputService.accept_routing_with_input
self._scheduler.recovery._checked((request.run_id,), "scheduler", "global",
                                 "worker-input-admission-snapshot", admit_under_lock)
# ControlPlaneApplication._bootstrap
self._scheduler.recovery._checked(None, "application_bootstrap", "global",
                                 "startup-recovery", recover_under_lock)
```

Collect all startup inventories before the write lock, then reuse the exact observations before/after Provider settlement. Newly created Run membership invalidates/retries startup; it never receives an invented empty inventory. Keep readiness assignment after successful outer commit. No recovery call is introduced into Scheduler methods that do not currently need it.

- [ ] **Step 7: Factor and test historical reducers without changing live projections.** Copy the existing `_scheduler_state` loop into the pure `reduce_scheduler_attempts` function and replace its one store read with the supplied tuple. Current method reads a validated stream and delegates. Extract config parsing into `config_snapshot_from_events`; historical lifecycle prefixes call existing `reduce_lifecycle` directly, while live replay keeps its valid snapshot optimization. Add `replay_budget_events` as the existing reducer composition below, not a second accounting implementation:

```python
def replay_budget_events(run_id, events, limit):
    rows = list(events)
    validate_event_contract(rows)
    if any(event.stream_version != index for index, event in enumerate(rows, 1)):
        raise ValueError("budget prefix has a version gap")
    states = BudgetLedger._replay_states(run_id, rows)
    balance = BudgetLedger._balance_from_events(run_id, rows, limit, len(rows))
    reservations = tuple(states[key].reservation for key in sorted(states))
    return balance, reservations
```

Exercise a valid accepted Attempt prefix, then cancel/settle in later events and prove the old prefix still returns its original reservation/Agent/lifecycle state while live projections remain cancelled/settled. Also test gaps, duplicate releases, missing RunCreated and altered payload hashes through real store reads. Keep full existing reducer validation and account for `_run_limit` as currently derived from the RunConfigSnapshot; do not infer ordering from timestamps.

- [ ] **Step 8: Write and run the real lock-order/inventory tests.** In `test_artifact_recovery_lock_order.py`, use `multiprocessing.get_context("spawn")`, child-local SQLite connections and a parent Pipe with `poll(10)`/`join(15)` bounds. Child A wraps the existing `_record_intent` and pauses after `_digest_lock` acquired, before its SQLite append. Child B invokes one of `resolve_active_attempt`, `resolve_verification_binding`, `accept_routing`, `plan_attempt_recovery`, WorkerInput prepare/admit, application startup, public recovery. Establish B's inventory start via a Pipe event before its digest wait. A is then released, must append/finish, and B must finish or return the intended domain rejection within 15 seconds; no SQLite-then-digest deadlock is allowed. Open stores in child processes before the rendezvous. `plan_attempt_recovery` may reject a live Attempt, but it must exercise the real recovery call first.

Add a two-thread variant (separate stores/connections per thread) pausing after the real process mutex acquisition; use two digests in opposite requested order. No sleep-only assertion: barriers announce actual lock acquisition and actual SQLite callback entry. Failpoints supplement real lock behavior; none replaces it with a Boolean capability mock. Always release rendezvous in finally and terminate/reap only test-owned stuck processes after reporting failure.

In `test_artifact_inventory.py`, capture then append a new artifact stream or candidate stream on another connection; inside `_checked` verify retry occurs outside SQLite, exactly three attempts maximum, and failed attempts append no lifecycle/control changes. Inject new Run/config/lifecycle membership during startup capture and assert retry rather than missing inventory. Assert full readers/verifier entry inside raw outer read/write transaction fail before grant/path/lock callbacks and leave the owner transaction open. Test foreign coordinator/store/subclass/constructed witness rejection by attempting `_validate_inventory` without its active identity registration. Check normal success/error snapshot closure before actual digest locks.

```python
def test_inventory_change_does_not_repeat_committed_mutation(tmp_path, monkeypatch):
    case = candidate_case(tmp_path)
    recovery = case.app._scheduler.recovery
    original = recovery._validate_inventory
    calls = []
    def changed_once(inventory):
        calls.append(case.store.in_transaction)
        if len(calls) == 1:
            raise InventoryChanged("recovery inventory changed")
        return original(inventory)
    monkeypatch.setattr(recovery, "_validate_inventory", changed_once)
    try:
        recovery._checked(("run-1",), "probe", "one", "once",
            lambda events, version: [EventDraft("Probe", {"value": 1})])
        assert calls == [True, True]
        assert len(case.store.read_stream("probe", "one")) == 1
        assert not case.store.in_transaction
    finally:
        case.app.close()
```

This fault-injection test supplements the real writer tests; it is not the lock-safety proof.

- [ ] **Step 9: Run GREEN and regressions, update documentation, and complete stage acceptance.**

```bash
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/persistence tests/unit/artifacts tests/unit/recovery tests/unit/lifecycle/test_scheduler.py tests/unit/runtime/test_worker_input_service.py tests/integration/test_artifact_recovery_lock_order.py tests/integration/test_recovery.py tests/integration/test_worker_input_recovery.py -q -ra -W error
```

Document only the verified recovery lock-order/snapshot behavior and unchanged blocked-only boundary in `docs/security/worker-runtime.md`. Complete the common gate/review/controller delivery procedure. Suggested commit: `fix: separate recovery inventory from SQLite authority transactions`.

## Task 2: Strict candidate journal, historical witnesses and complete consumer gates

**Files:**

- Create: `src/orchestrator/artifacts/candidate_schema.py`, `src/orchestrator/artifacts/candidate_journal.py`, `src/orchestrator/artifacts/candidate_authority.py`.
- Modify: `src/orchestrator/persistence/events.py:139`; `src/orchestrator/artifacts/store.py:459,609,734,759,781,802,819,886,1075`; `src/orchestrator/recovery/artifact_inventory.py`, `src/orchestrator/recovery/run.py`; `src/orchestrator/scheduler/core.py:821,958`; `src/orchestrator/application.py:127,180`.
- Modify: `src/orchestrator/runtime/artifact_admission.py:27`, `src/orchestrator/runtime/verifier_process.py:71`, `src/orchestrator/runtime/acceptance_coordinator.py:79`, `src/orchestrator/runtime/worker_input_service.py:40`.
- Create: `tests/unit/artifacts/test_candidate_schema.py`, `tests/unit/artifacts/test_candidate_journal.py`, `tests/unit/runtime/test_candidate_set_gates.py`, `tests/integration/test_candidate_read_snapshots.py`, `tests/integration/test_candidate_history.py`.
- Modify: `tests/support/candidate_publication.py`, existing artifact/admission/verifier/Scheduler tests, `docs/security/worker-runtime.md`.

**Interfaces:**

| Producer | Exact interface |
| --- | --- |
| candidate_schema.py | `canonical_bytes(value: object) -> bytes`; `candidate_hash(tag: str, value: object) -> str`; `utc_text(value: datetime) -> str`; `parse_utc_text(value: object) -> datetime`; `validate_prepared(payload: object) -> dict[str, object]`; `validate_committed(payload: object, prepared: dict[str, object]) -> dict[str, object]`; `validate_candidate_events(events: Sequence[StoredEvent]) -> None`. `StoredEvent` is type-checking-only here: runtime code is duck-free field access after the event boundary validates it. Schema module has no store/journal imports. |
| candidate_authority.py | Frozen `CandidateAuthority(context: AttemptContext, role: str, reservation_id: str, accepted_route_id: str, accepted_at: datetime, lease_expires_at: datetime)`; `capture_authority_heads(store: SQLiteEventStore, run_id: str) -> tuple[dict[str, object], ...]`; `validate_historical_authority(store: SQLiteEventStore, context: AttemptContext, heads: tuple[dict[str, object], ...], at: datetime) -> CandidateAuthority`. Requires an existing snapshot and the exact five prefixes. |
| candidate_journal.py | Frozen `CandidateSet(prepared_json: bytes, committed_json: bytes | None)` with properties `prepared`, `committed`, `context`, `members`, `records`, `request_id`, `set_id`; each mapping property returns a fresh parsed mapping; tuple model properties rebuild expected schemas. Content bytes/input text are never present. Data object alone conveys no authority. |
| CandidatePublicationJournal | `__init__(event_store: SQLiteEventStore)` rejects nonexact/unreadable stores; `lookup(self, *, run_id: str, node_id: str, attempt_id: str, fencing_generation: int) -> CandidateSet | None`; `read_run(self, run_id: str) -> tuple[CandidateSet, ...]`; `require_set(self, context: AttemptContext, members: tuple[ArtifactRef, ...]) -> CandidateSet | None`; `publication_view(self, digest: str) -> tuple[ArtifactRecord, ...]`; `audit_all(self) -> None`. Each begins/reuses read_snapshot before any discovery and validates all dynamic links inside it. None from require_set means legacy identity with no preparation/namespace in this database, not an unavailable adapter. `CandidateJournalError(RuntimeError)` carries only fixed text `candidate journal invalid`; no caught error is attached. |
| CandidatePublicationJournal | `_read_run_in_snapshot(self, run_id: str, view: dict | None = None) -> tuple[CandidateSet, ...]`; `_check_links(self, candidate: CandidateSet, view: dict) -> None`; `_require_intent(self, *, run_id: str, request_id: str, set_id: str, digest: str) -> ArtifactRecord`. First two require transaction ownership; _require_intent owns/reuses its snapshot. Only private callers pass call-local view caches. These helpers use the pure ArtifactStore record parser, never filesystem reads or store instances. |
| ArtifactStore | `candidate_records(self, context: AttemptContext, members: tuple[ArtifactRef, ...]) -> tuple[ArtifactRecord, ...]`. Database-only journal exact-set gate followed, outside snapshot, by rehash of those exact materialized records; legacy branch uses the same coherent inventory and exact provenance checks. Reject outer transaction entry before I/O. |
| worker_input_service.py | `task_from_authority(authority: CandidateAuthority, task_text: str) -> WorkerTask`: bounds text, builds original input manifest with durable role, compares commitment, returns empty-input/capability task at output cap. `CandidateAuthority` import is type-only/local to avoid cycles. Existing preparation uses this after its complete current checks; historical retry will use historical authority in Task 3. |
| Test support | `append_candidate_fixture(case, contents: tuple[bytes, ...], *, commit: bool) -> CandidateSet` creates real journal/intents/optional published events with valid authority heads, plus real bytes for committed fixtures, using trusted low-level test writes. It never invokes a product publisher. `rewrite_event(store, event_id: str, *, payload: dict | None = None, headers: dict | None = None) -> None` changes only a test database after temporarily dropping/restoring immutable update trigger and recomputes canonical payload hash. |

At all new model trust crossings use exact expected types, not overridden `model_dump`, properties, subclass validators, or `model_copy` as validation. Reject surviving extras in `__dict__` and `__pydantic_extra__` before any expected-field copy. Rebuild nested AttemptContext/ArtifactRef/WorkerTask/WorkerResult/VerificationTask models recursively from known field maps. Reject subclasses outright at these crossings; this is stricter and simpler than trusting their declared fields. Inputs from persisted FrozenDict/FrozenList are copied to plain JSON containers only after exact known keys/types/bounds checks; these are trusted event-store wrappers, not arbitrary Mapping implementations. Entry errors in new candidate paths are detached, fixed strings. Existing legacy semantics remain only when the journal positively establishes no preparation/namespace conflict.

- [ ] **Step 1: Write deterministic identity/schema tests and run RED.** Include the independent hash oracle, zero bytes, binary bytes, sorting and canonical timestamp boundary:

```python
import hashlib
import json
from datetime import datetime, timezone
import pytest
from orchestrator.artifacts.candidate_schema import candidate_hash, utc_text, parse_utc_text

def test_tagged_hash_and_utc_are_exact():
    value = {"fencing_generation": 1, "attempt_id": "a", "node_id": "n", "run_id": "r"}
    expected = hashlib.sha256(b'maestro.candidate-request.v1\0' +
        b'{"attempt_id":"a","fencing_generation":1,"node_id":"n","run_id":"r"}').hexdigest()
    assert candidate_hash("maestro.candidate-request.v1", value) == "sha256:" + expected
    assert utc_text(datetime(2026, 10, 11, tzinfo=timezone.utc)) == "2026-10-11T00:00:00.000000+00:00"
    for bad in ("2026-10-11T00:00:00Z", "2026-10-11T00:00:00+00:00", True, None):
        with pytest.raises(ValueError):
            parse_utc_text(bad)
```

```bash
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/artifacts/test_candidate_schema.py -q
```

Expected: missing candidate schema module. After implementation, this test must pass without changing its oracle.

- [ ] **Step 2: Implement pure canonical schema validation.** Define constants `SCHEMA = "maestro.candidate-publication.v1"`, `POLICY = "maestro.candidate-policy.v1"`, `EVENT_CAP = 1_048_576`, `RUN_CAP = 16 * 1024 * 1024`, `REQUEST_CAP = 1024`, `COMMIT_RESERVE = 16_384`, `CONTENT_CAP = 16_384`, `MEMBER_CAP = 128`. Use these exact primitive implementations:

```python
def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8", errors="strict")

def candidate_hash(tag, value):
    return "sha256:" + hashlib.sha256(tag.encode("utf-8") + b"\0" + canonical_bytes(value)).hexdigest()

def utc_text(value):
    if type(value) is not datetime or value.utcoffset() != timedelta(0):
        raise ValueError("candidate timestamp invalid")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")

def parse_utc_text(value):
    if type(value) is not str:
        raise ValueError("candidate timestamp invalid")
    parsed = datetime.fromisoformat(value)
    if utc_text(parsed) != value:
        raise ValueError("candidate timestamp invalid")
    return parsed
```

`validate_prepared` requires the exact field set from Global Constraints; exact schema/policy/output cap; exact AttemptContext field set and identifier regex/strict ints/digests; result/request/set digest format; exact sorted unique 1..128 member schemas; aggregate sizes <=16,384; reservation/route identity are bounded nonempty existing identifiers; timestamps canonical and `accepted_at <= prepared_at < lease_expires_at`; exact five sorted heads with positive strict-int version, bounded event ID and 64 lowercase hash. Recompute request and set hashes from validated fields. No arbitrary result ID is stored.

Rebuild records from the same member list, context, set_id and prepared_at with the fixed host policy, then compare their complete canonical payloads byte-for-byte with `records`. Require exact keys `publication_id,digest,type,size,media_type,source,schema_version,redaction_state,readable_scope,references,lifecycle_state,created_at`, exact source keys `run_id,node_id,attempt_id,fencing_generation,agent_instance_id,publication_protocol,candidate_set_id`, no optional tool/model or unknown source keys. `size` and `schema_version` reject bool. Source generation is `str(context.fencing_generation)`; scope is exactly `[run_id]`; references `[]`; record type/media/redaction/lifecycle literals fixed above. Deterministic member ID is recomputed, not accepted from metadata.

Change `ArtifactRecord.to_event_payload` only for source containing this exact protocol: use `created_at.astimezone(timezone.utc).isoformat(timespec="microseconds")`. Existing generic output retains its current `.isoformat()` representation. Candidate schema validation compares raw event payload before the generic record parser can normalize whitespace, omitted publication IDs or empty scope. This handles zero-microsecond preparation times without changing the spec's record-equality rule.

`validate_committed` requires its exact six keys, exact request/set, recomputed prepared_hash, canonical committed_at with `prepared_at <= committed_at < lease_expires_at`, and exact heads whose versions are >= the corresponding prep versions. Incrementally reject event size >1 MiB; commits additionally <=16,384. Replay counts prepares <=1,024 and canonical total bytes plus a 16,384 reservation for every uncommitted preparation <=16 MiB. A commit replaces only its own allowance with its actual size; commit-before-prepare, duplicate prepare/commit, changed ID, unsupported event, duplicate Attempt key or nonconsecutive stream versions rejects. Different requests may interleave prepare/commit events; do not require adjacent pairs.

Add `validate_candidate_events` dispatch at the start of `validate_event_contract` when any row has `stream_type == "candidate_publications"` or a candidate event type. Reject candidate event types on any other stream. Verify full header, idempotency key, schema_version==1, exact Run stream ID and same-context prep/commit. Add the two types to causal execution-header requirements. Artifact candidate headers are validated by journal cross-links, and namespace collisions are rejected even if a candidate event was inserted on an artifact stream.

- [ ] **Step 3: Implement bounded journal replay and historical prefix authority.** `read_run` opens read_snapshot before Run discovery or aggregate-size queries. Before decoding any candidate payload, SQL-preflight this Run's raw stream with count <=2,048, sum of UTF-8 payload storage lengths <=16 MiB and max <=1 MiB. Then call `validated_stream`, canonical schema replay and reserved-byte accounting. These storage preflights are conservative safeguards; canonical caps still apply after parsing. Do not use global event ordering, current graph state or latest-state snapshots to audit historical heads.

Inside `_read_run_in_snapshot`, build maps by request_id and exact `(run_id,node_id,attempt_id,generation)` from the validated candidate stream. For every preparation: load all five complete validated source streams in the same snapshot; slice each at its head.version; compare last event ID/hash with the head; run `validate_event_contract` on each prefix; call `config_snapshot_from_events`, existing `reduce_lifecycle`, `reduce_agent_registry`, `replay_budget_events`, and `reduce_scheduler_attempts`. For each phase ensure the original acceptance exists in its scheduler prefix, active scheduler lease/Agent/Attempt, reserved matching budget, uncancelled Run, current-at-that-prefix graph and role/planning/config/registry/policy relationships, full echoed context, route/model/provider/reservation/decision identity and original verification admission (`maestro.artifact-verification/v1`, exactly artifact-integrity). Require its accepted timestamp and lease to exactly match preparation, and phase time within that exact lease. Return CandidateAuthority from the reconstructed data. Reuse/factor WorkerInputService's full contract comparison; never use only child hashes. Check lifecycle Attempt and Agent policy commitment as well as Node contract.

`capture_authority_heads` requires an existing write transaction and serializes these five exact current heads after live authority resolution. It must not infer which stream changed last. Historical replay validates prefixes without writing snapshots or recovering files; stale-but-valid pending preparation passes. Missing source prefix, schema/hash/version/header/cancellation/reservation inconsistency rejects. At committed phase, compare immutable CandidateAuthority fields to preparation phase in addition to nondecreasing heads/time. A later current graph append or cancellation cannot invalidate the recorded valid prefixes.

`_check_links` dynamically reads each member's artifact stream within the same snapshot, validates its event sequence, and selects by exact publication_id. A preparation requires exactly one matching intent and zero published events when uncommitted, or exactly one matching publication when committed. Match complete raw payload, full execution headers, idempotency key and digest stream ID; require intent stream version before publication. Independent generic/other-set publications of the same digest are retained separately. Every Run lookup, including an absent-status lookup, enumerates all artifact stream IDs inside its snapshot and checks reserved events belonging to that Run against the preparation index; orphan intents/publications must not appear absent. `audit_all` additionally scans every candidate namespace/source marker globally, including malformed or missing Run identity. A source key's presence or `candidate-v1:` ID signals reserved handling; any partial/conflicting combination rejects, never falls through to generic parsing. A malformed candidate stream without a known Run also fails audit. Use only a call-local map of validated streams/decoded Runs to avoid recursion and repeated loads inside this snapshot; never persist that map across calls or rollback. `_check_links` accesses that raw stream map, not another public journal lookup.

Every public journal method follows this structure; callbacks never acquire filesystem locks:

```python
def lookup(self, *, run_id, node_id, attempt_id, fencing_generation):
    identity = dict(run_id=run_id, node_id=node_id, attempt_id=attempt_id,
                    fencing_generation=fencing_generation)
    request_id = candidate_hash("maestro.candidate-request.v1", identity)
    with self._event_store.read_snapshot():
        sets = self._read_run_in_snapshot(run_id)
        return next((item for item in sets if item.request_id == request_id), None)
```

Validate identity strict types/identifier bounds before hashing. `require_set` first strictly rebuilds context/members, then opens the snapshot before preparation discovery. If a prep exists, require committed and exact canonical sorted context/member equality; selecting an older legacy publication cannot bypass it. If no prep exists, examine selected artifact streams' namespace markers and exact provenance; only a genuinely legacy identity returns None. `publication_view` opens its snapshot before loading the actual ArtifactPublished list and validates every candidate parent set and relevant preparation index, then returns those exact materialized records. No preloaded `_records` list may be joined to a later journal snapshot. It does not call ArtifactStore or grant reads.

- [ ] **Step 4: Define durable fixtures and journal tests.** Implement `append_candidate_fixture` in test support by decoding `candidate_frame`, computing fixed IDs/payloads with the schema functions, collecting real current heads in a `recovery._checked` preparation callback, appending all member intents with complete headers, and returning the prepared draft from that callback. For `commit=True`, install bytes via ordinary trusted generic `publish_bytes` before the candidate final transaction (generic records use distinct IDs and a separate `source={"run_id": "fixture-storage"}`); append each exact candidate ArtifactPublished and the candidate marker in one real final transaction with recorded heads/clock. This is test-only event construction; no product producer API is added. Generic fixture events do not share the candidate Attempt identity, and require_set still chooses only the candidate records. Always re-read the final CandidateSet from a fresh journal instance; never return the constructed payload as authority.

Parameterize `test_candidate_journal_rejects_corruption` over every prepared/commit key, context field, head field, source field, record field, event header, idempotency key, missing intent/publication, extra intent/publication, duplicate prepare/commit, commit-before-prepare, missing marker, marker source stripped, noncanonical timestamp/generation, mismatched size/shared digest, bad stream IDs and orphan candidate artifacts. For a field corruption, `rewrite_event` recomputes payload hash so the candidate-specific invariant is actually reached; separately test wrong payload hashes without rewriting them. Copy trigger SQL from sqlite_master, restore it in finally, and reopen the test database after injection.

```python
def test_historical_heads_use_prefix_reducers(tmp_path):
    case = candidate_case(tmp_path)
    try:
        saved = append_candidate_fixture(case, (b"\0", b"\xff\x80"), commit=True)
        before = event_counts(case.store)
        case.app._scheduler.lifecycle.request_cancel("run-1", reason_code="fixture_cancel")
        after = event_counts(case.store)
        assert after != before
        loaded = CandidatePublicationJournal(case.store).lookup(**case.identity)
        assert loaded.prepared_json == saved.prepared_json
        assert loaded.committed_json == saved.committed_json
        assert event_counts(case.store) == after
    finally:
        case.app.close()
```

The cancellation API is the existing `LifecycleController.request_cancel(run_id, *, reason_code)` at controller.py:294. Also append a real graph expansion after a committed valid fixture and assert old prefix stability. In `test_constructed_models_cannot_bypass_set_gate`, exercise model_construct/model_copy extras, nested field overriding subclasses, bool generations/sizes, non-tuple members, duplicate/extra refs, wrong full context and tampered records. Assert no durable history growth.

For caps, create real admitted Attempts in one Run using repeated Nodes with frozen contracts and sequential accepted/released states; do not lower constants to claim boundary coverage. Use existing public `run_setup(store, run_id="run-1", nodes=nodes, max_agents=1025, freeze_contracts=True)` with 1,025 distinct `NodeSpec(node_id=f"node-{i}", role="coder", planning_contract_hash="sha256:" + "e" * 64, max_attempts=1)` seeds. It replaces the seed with each real compiled frozen contract. Read each Node's actual contract, call public routed_pair with its node_id/hash, then accept_routing_with_input. Commit each zero-content set before advancing to the next Attempt, then call Scheduler.finish_attempt with `outcome="failed", known_no_effect=True` only in this synthetic fixture where no Worker or Provider ever executed; this returns reserved test budget and slots. Do not try to hold 1,024 preparations simultaneously: their commit reservations alone consume 16 MiB before positive preparation bytes. After 1,024 committed sets, exact retry is a no-op and request 1,025 rejects. For byte accounting use the exact integer boundary tests in the addendum plus real SQLite transaction tests; do not claim all integer byte sizes are reachable canonical valid payload sizes. Maximum legal identifier lengths/members make large preparations. Assert prepared commit can replace its reserved allowance at the cap. Test member counts 1/128/129; aggregate content bytes 0/16,384/16,385; raw frame cap/cap+1; commit allowance/allowance+1; 128/129-character head IDs; wrong numeric types; and nonfinite canonical input. None allocates based on declared sizes.

- [ ] **Step 5: Integrate every consumer atomically in this stage.** Add generic `publish_bytes` rejection of either reserved source key before staging; do not add a public publication_id override. The private generic intent/metadata entry points also reject reserved records; Task 3 supplies separate operations. No candidate cache fallback is allowed. Candidate `_records` writes are removed/avoided in metadata-only code; normal durable reads always use event-store data, including after rollback. Generic append-only adapters remain compatible only for genuinely generic artifacts with no reserved fields.

ArtifactStore read_bytes/get_record/list_publications/get_records load `publication_view` before any path/grant action and authorize from that exact tuple after snapshot closure. verify_run_artifacts/pending inventory capture now includes journal cross-links in their initial database snapshot; no later independent join. For a pending candidate whose digest has a valid A publication, A's metadata remains usable, but no B publication record exists. For a corrupted candidate parent present in the selected digest's actual publication set, fail closed. Keep the usual digest/scope/expiry/use-count grant policy and size/rehash checks.

`candidate_records` opens one snapshot, loads `require_set` and actual records inside that view, then closes it before verifying files. With prep, return exactly its records; without prep, select exact legacy provenance/size/type/media using the same materialized view. Store exception adapters raise detached stable consumer errors, outside catch handlers.

Replace admission's inventory loop with fresh model reconstruction followed by `candidate_records`. A generic inventory-only adapter cannot establish absence of an Attempt preparation; reject it for admission (legacy compatibility is provided through the real store). Direct IsolatedVerifierProcess.verify uses the same gate before the first `grant_for_digest` call/staged file, and rejects outer transaction entry before grant work. AttemptExecutionCoordinator performs the same gate after resolving fresh authority and loading/validating the proposal, before invoking the independent verifier. Scheduler.finish_verified_attempt calls database-only `require_set` in its terminal callback before idempotent return and before budget/lifecycle/Agent writes. Its historical success audit and application proposal audit require exact committed sets when prep/namespace indicates candidate protocol; the existing historical admission checks remain in force.

Startup calls `journal.audit_all()` inside the existing atomic startup transaction, before Provider settlement and before readiness. Every candidate stream (even prep-only or proposal-free) is audited; orphan candidate artifact identities fail. Stage-1 capture includes complete candidate witnesses; its validator compares them, and metadata-only journal helpers reuse the transaction. Published bytes still use the outside inventory. Startup never auto-publishes a preparation, executes a verifier or settles from publication. For new and historical terminal acceptance, also require `committed_at <= completed_at`. During historical success audit, require the commit's scheduler head version to precede the recorded successful release version and its lifecycle head to precede the matching lifecycle terminal version. These comparisons are within each respective stream, not a claimed global order. This prevents inserting a candidate preparation/commit after an already recorded success and treating it as the prerequisite for that old success.

- [ ] **Step 6: Pin all gates, rollback cache safety and shared-digest grants.** Add parameterized real database tests for consumers `admit_candidate_artifacts`, direct verifier, acceptance coordinator, low-level Scheduler finish, historical success audit, proposal startup, store read_bytes/get_record/list_publications/get_records, verified inventory and pending inventory. For every applicable consumer exercise subset, superset, same-digest changed metadata, mixed-set, pending, missing marker, stripped source marker and selected older legacy publication. Gate failure precedes grant issuance, staging/child launch and resource/usage events. Use real EphemeralArtifactGrantAuthority to issue A/B scoped grants; do not accept a Boolean grant verifier as authority evidence.

```python
def test_shared_digest_does_not_transfer_authority(tmp_path):
    case = candidate_case(tmp_path)
    try:
        old = case.artifacts.publish_bytes(b"same", source={"run_id": "run-A"},
                                          readable_scope=("run-A",))
        pending = append_candidate_fixture(case, (b"same",), commit=False)
        grant = case.app._artifact_grants.issue(
            digest=old.digest, run_id="run-A", expires_at=datetime.now(timezone.utc) + timedelta(minutes=1))
        assert case.artifacts.read_bytes(old.digest, grant=grant) == b"same"
        metadata_grant = case.app._artifact_grants.issue(
            digest=old.digest, run_id="run-A", expires_at=datetime.now(timezone.utc) + timedelta(minutes=1))
        assert all(record.source["run_id"] == "run-A"
                   for record in case.artifacts.list_publications(old.digest, grant=metadata_grant))
        with pytest.raises(CandidateJournalError):
            CandidatePublicationJournal(case.store).require_set(case.task.context, pending.members)
        assert len(case.artifacts.pending_publications_for_run("run-1")) == 1
        assert_held(case)
    finally:
        case.app.close()
```

Inject a final metadata append then force an outer rollback, and assert fresh and same-instance journal/read/inventory reject eligibility; poison `_records` with the rolled-back candidate as an additional defense test. Confirm existing trusted generic publications/proofs with no preparation still pass their existing suites. Frozen receipt-like objects are not accepted by any set gate.

- [ ] **Step 7: Deterministic separate-process read races for every discovery order.** Build pending fixtures, spawn a writer process which opens its own store before barriers and atomically appends all candidate metadata/marker. The reader hooks its actual first `validated_stream`/stream discovery read to send `discovered` after data has been read, waits for the writer's committed acknowledgement (10-second Pipe timeout), then resumes remaining dynamic member/cross-link reads. For journal-first and artifact-first queries, result must be coherent old prepared/ineligible or complete committed; never corruption. Use two-member sets with one digest also independently published in another Run, so dynamic member discovery and shared metadata are exercised. Force ArtifactStore methods to discard any preloaded publication list by committing after a deliberately captured old list and entering the actual snapshot thereafter.

Parameterize reader method over lookup/status-shaped lookup, require_set, publication_view, read_bytes/get_record/list_publications/get_records, candidate_records and verified inventory. Repeat database-only helpers inside outer `BEGIN` and `BEGIN IMMEDIATE`. For outer write: writer sends `attempting` before append, reader asserts it has not received `committed`, finishes nested reads, verifies the outer transaction is still open, releases it, then waits for writer commit. Never wait for writer completion while holding that write lock. Full filesystem/verifier methods under either outer transaction reject before path/grant work; they must not close the outer transaction. Add exception-path closure assertions by observing `store.in_transaction` at the first actual file/hash/lock/grant function. Process waits are bounded and no sleeps establish order.

- [ ] **Step 8: Run GREEN, document the gates and complete stage acceptance.**

```bash
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/artifacts tests/unit/runtime tests/unit/lifecycle/test_scheduler.py tests/integration/test_candidate_read_snapshots.py tests/integration/test_candidate_history.py tests/integration/test_artifact_recovery_lock_order.py tests/integration/test_isolated_verifier_process.py -q -ra -W error
```

Document reserved namespace, strict whole-set eligibility, historical-prefix audit and the absence of a producer API at this stage in worker-runtime.md. Complete common gate/review/controller delivery. Suggested commit: `feat: enforce durable candidate set journal at every consumer`.

## Task 3: Retained installation, atomic guarded publication and ready-gated host APIs

**Files:**

- Create: `src/orchestrator/runtime/candidate_publication.py`.
- Modify: `src/orchestrator/artifacts/store.py`, `src/orchestrator/application.py`. The shared WorkerInputService task builder is already complete in Task 2 and is consumed unchanged here.
- Create: `tests/unit/runtime/test_candidate_publication.py`, `tests/integration/test_candidate_publication_transactions.py`, `tests/integration/test_candidate_publication_crashes.py`.
- Modify: `tests/support/candidate_publication.py`, `tests/integration/test_candidate_read_snapshots.py`, `tests/integration/test_artifact_recovery_lock_order.py`, `docs/security/worker-runtime.md`.

**Interfaces:**

| Producer | Exact interface |
| --- | --- |
| candidate_publication.py | `CandidatePublicationError(ValueError)` with only the six Global Constraints codes; frozen slotted `CandidatePublicationReceipt(request_id: str, set_id: str, state: Literal["committed"] = "committed")`; frozen slotted `CandidatePublicationStatus(state: Literal["absent", "prepared", "committed"], request_id: str | None, set_id: str | None)`. No methods accept these objects as authority. |
| CandidatePublicationService | `__init__(self, *, event_store: SQLiteEventStore, scheduler: Scheduler, artifact_store: ArtifactStore, clock: Callable[[], datetime] = _utc_now)`; `_now(self) -> datetime`; `publish(self, *, run_id: str, node_id: str, attempt_id: str, fencing_generation: int, task_text: str, payload: bytes) -> CandidatePublicationReceipt`; `status(self, *, run_id: str, node_id: str, attempt_id: str, fencing_generation: int) -> CandidatePublicationStatus`. Module `_utc_now() -> datetime` returns `datetime.now(timezone.utc)`. Require exact real store, scheduler.event_store and artifact_store._event_store object identity, same scheduler.recovery.artifact_store, readable/open connection, no outer transaction. |
| ArtifactStore | `_record_candidate_intent(self, record: ArtifactRecord, context: AttemptContext, set_id: str) -> None`; `_record_candidate_metadata(self, record: ArtifactRecord, context: AttemptContext, set_id: str) -> None`: require same SQLite write transaction, exact schema/header/namespace, append only with no files/locks/cache; do not expose as application APIs. |
| ArtifactStore | `_candidate_installation_session(self) -> AbstractContextManager[object]` creates/registers one opaque operation token and clears its issued-results map on exit. `_stage_candidate_blob(self, *, operation: object, run_id: str, request_id: str, set_id: str, digest: str, content: bytes) -> _CandidateInstallation` requires that active token and durable replay lookup under one digest lock. `_validate_candidate_installations(self, operation: object, results: tuple[_CandidateInstallation, ...], prepared: CandidateSet) -> None` compares exact issued object identities and complete frozen members without filesystem I/O. Frozen `_CandidateInstallation` contains store identity, request/set/digest/size/token; the store registers it only after actual successful fsync. A service must not register an arbitrary returned object itself. Forged, stale-call, foreign-store and duplicate results reject. |
| CandidatePublicationService | `_publish(self, identity: dict[str, object], task_text: str, payload: bytes) -> CandidatePublicationReceipt`; `_validate_decoded(self, task: WorkerTask, decoded: object) -> DecodedWorkerOutput`; `_prepare(self, identity: dict, task_text: str, decoded: DecodedWorkerOutput) -> CandidateSet`; `_finalize(self, prepared: CandidateSet, operation: object, installations: tuple[_CandidateInstallation, ...], task_text: str) -> CandidatePublicationReceipt`. Internal methods always re-read durable identity; no constructed CandidateSet is sufficient authority. |

- [ ] **Step 1: Write the public whole-request test and run RED.** This test uses the real database/filesystem, stable IDs and resource holds; it does not execute a Worker or grant bytes to a child.

```python
def test_publish_whole_set_and_exact_retry(tmp_path):
    case = candidate_case(tmp_path)
    try:
        case.app._candidate_publications._clock = lambda: case.clock.now
        raw = candidate_frame(case.task, (b"", b"\0\xff", b"third"))
        before = event_counts(case.store)
        first = case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
        after = event_counts(case.store)
        assert first.state == "committed"
        assert case.app.candidate_publication_status(**case.identity).state == "committed"
        reordered = candidate_frame(case.task, (b"", b"\0\xff", b"third"), reverse=True)
        second = case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=reordered)
        assert second == first
        assert event_counts(case.store) == after
        assert after[("candidate_publications", "run-1")] == 2
        assert all(after.get(key) == value for key, value in before.items()
                   if key[0] in {"run", "run_lifecycle", "agent_registry", "budget", "scheduler"})
        assert_held(case)
    finally:
        case.app.close()
```

```bash
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/runtime/test_candidate_publication.py::test_publish_whole_set_and_exact_retry -q
```

Expected: application has no candidate publisher. No existing test result counts as RED for this feature.

- [ ] **Step 2: Implement strict boundary, historical-first lookup and safe errors.** Bound exact bytes before parsing (1..1,048,576), and exact str text before canonicalization (1..100,000 chars, strict UTF-8, input/frame commitment cap), exact identity strings/generation. Reject bytearray/memoryview/bytes subclass, string subclass, bool and extra request keywords. The constructor requires identical same-store composition; unavailable/mismatched/closed/append-only fake adapters fail with `candidate_publication_unavailable` detached errors. Reject outer transaction entry before recovery collection, file work or grants. `_now` accepts exact aware UTC datetime, no caller time.

```python
_CODES = frozenset({"candidate_publication_invalid", "candidate_publication_too_large",
    "candidate_publication_conflict", "candidate_publication_inactive",
    "candidate_publication_unavailable", "candidate_publication_uncertain"})

class CandidatePublicationError(ValueError):
    def __init__(self, code):
        self.code = code if type(code) is str and code in _CODES else "candidate_publication_invalid"
        super().__init__(self.code)

def publish(self, *, run_id, node_id, attempt_id, fencing_generation, task_text, payload):
    code = "candidate_publication_unavailable"
    try:
        return self._publish(dict(run_id=run_id, node_id=node_id, attempt_id=attempt_id,
                                  fencing_generation=fencing_generation), task_text, payload)
    except CandidatePublicationError as error:
        if type(error) is CandidatePublicationError and type(vars(error).get("code")) is str:
            if vars(error)["code"] in _CODES:
                code = vars(error)["code"]
    except Exception:
        pass
    raise CandidatePublicationError(code)
```

Within `_publish`, track whether preparation may have happened; map every unexpected exception after entering preparation commit to uncertain unless durable replay proves the exact state. Never use the public wrapper's default unavailable to hide ambiguous effects. Safe classification is done before leaving inner handlers and no raw caught object is retained in returned error/context/cause. Invalid policy/frame/text => invalid; proven bound => too_large; changed valid normalized identity for same request => conflict; proven authority loss => inactive; unavailable before any possible effect => unavailable; ambiguous prepare/final outcome => uncertain. Status errors never return absent for corruption/unreadable data.

Lookup durable request first. If committed, use preparation's replay-validated historical authority and `task_from_authority` to verify exact text, call decoder, revalidate result and compare set hash. Return historical receipt with no new clock call, file operation, grant or write even if currently cancelled/expired/replaced. For absent/prepared requests, collect inventory and resolve current task via WorkerInputService/current authority before staging; preparation/finalization recheck again. Only candidate outcome allowed; blocked/failed return invalid with no candidate events.

`_validate_decoded` rejects any nonexact DecodedWorkerOutput/DecodedCandidateBlob, nonexact tuple/bytes, unexpected slot-bearing subclass, and nonexact WorkerResult/ArtifactRef/AttemptContext or extras. Rebuild the complete expected model tree; require full context equality, candidate outcome, unique complete digest mapping, exact artifact-reference equality between result and each blob, fixed policy, exact SHA-256 and actual byte sizes, 1..128 members and aggregate <=16,384. Model_construct/model_copy frozen data is not trusted. Sort both refs and blobs by digest before identity. Do not call `.model_dump` on an unvalidated subclass. An injected decoder return is checked before `_prepare`.

- [ ] **Step 3: Implement preparation and metadata-only operations.** `_prepare` uses recovery._checked on candidate_publications/Run with `candidate-prepare:<request hex>`. Inside its callback: validate witness; read complete current journal; compare existing request first (same identity no-op even at caps); sample now after SQLite lock; resolve full current binding and role/planning/input relationships with the registered recovery scope; require original narrow admission; check accepted <=now<expiry and no backward time; build frozen records with host time; capture exact authority heads; enforce caps including commit reservation; append every intent using `_record_candidate_intent`, then return the single preparation draft. Inner intent writes and outer prep commit are one transaction. An intent method validates the proposed record against its expected full policy and identity; it never calls journal linkage replay while a new set is incomplete within that same transaction.

Use these metadata-only bodies with fresh context validation and record canonical policy checks before the call. The helper's event type is restricted internally to the two artifact types; generic `_record_intent/_record_metadata` cannot reach this helper with reserved data:

```python
def _append_candidate_record(self, event_type, record, context, set_id):
    if (type(self._event_store) is not SQLiteEventStore or not self._event_store.in_transaction
        or not self._event_store._has_checked_write_owner()):
        raise ArtifactMetadataError("candidate metadata requires owning transaction")
    if event_type not in ("ArtifactPublicationIntent", "ArtifactPublished"):
        raise ArtifactMetadataError("candidate metadata event invalid")
    key = ("artifact-intent:" if event_type == "ArtifactPublicationIntent"
           else "artifact-publication:") + record.publication_id
    self._event_store.append("artifact", record.digest,
        self._event_store.current_version("artifact", record.digest),
        [EventDraft(event_type, record.to_event_payload(), run_id=context.run_id,
            node_id=context.node_id, attempt_id=context.attempt_id,
            fencing_generation=context.fencing_generation,
            correlation_id=context.run_id, causation_id=set_id)], key)
```

There is no `_records` update or retry spin. Re-read confirmed complete preparation after successful append_checked. If a response is lost, attempt one coherent durable lookup on a usable connection; if complete prep exists proceed only after validating it, if unreadable return uncertain, if definitely rolled back report the classified failure. Do not install based on an in-memory prepared record. If the original connection is unusable, the API returns uncertain; the trusted caller can reopen through application composition and query/retry. Do not silently open a guessed database or replace identity.

- [ ] **Step 4: Implement retained single-digest installation.** `_stage_candidate_blob` checks no transaction before temporary creation; validates exact bytes/digest, then opens one existing `_digest_lock` (already digest -> mutex). Inside that lock, use journal._require_intent in its own short read_snapshot to reload exact durable set/intent and compare actual size/content/set identity; close snapshot before filesystem work. Arbitrary prepared records, caller receipts, foreign installations and invalid constructed results cannot authorize this operation. Prepared-but-committed is harmless if exact; replay remains mandatory.

Use the existing private root, tempfile mode 0600, os.fdopen write/flush/fsync, `_path_for_digest`, `_verify_file_and_size`, atomic `os.link`, `_fsync_directory` and normal private-temp cleanup. Never invoke ordinary publish_bytes or acquire multiple digest locks. This core installation algorithm preserves final objects on every exception:

```python
final_path = self._path_for_digest(record.digest)
fd, name = tempfile.mkstemp(prefix=".tmp-", dir=self._root)
temporary = Path(name)
try:
    try:
        os.fchmod(fd, 0o600)
    except BaseException:
        os.close(fd)
        raise
    with os.fdopen(fd, "wb") as staged:
        staged.write(content)
        staged.flush()
        os.fsync(staged.fileno())
    try:
        os.link(temporary, final_path)
    except FileExistsError:
        pass
    if self._verify_file_and_size(final_path, record.digest) != record.size:
        raise ArtifactIntegrityError(record.digest)
    self._fsync_directory()
finally:
    try:
        temporary.unlink(missing_ok=True)
    except OSError:
        pass
```

Execute this inside the single digest context only after durable lookup is closed. Existing symlink/nonregular final paths fail verified file opening; do not follow/replace/delete them. Directory fsync occurs also on reuse/FileExistsError. Close fd if os.fchmod fails before fdopen ownership transfer; no final-path unlink in any branch. Stage in sorted digest order, release one before the next. ArtifactStore registers issued installation object identities only after successful directory barrier. The service encloses the whole stage/finalize sequence in `_candidate_installation_session`, passes the opaque operation token to stage/finalize, and the store compares all result identities/full frozen members at finalization. Registry entries are scoped to token and cleared on all success/error paths. These results are process-local installation evidence, never durable authority or caller capabilities; restart always rehashes/reinstalls explicitly.

- [ ] **Step 5: Implement final authority decision and status/application composition.** `_finalize` obtains a fresh outside inventory and enters recovery._checked. Within callback, lookup complete prep/intents/prior commit; an exact already committed result is historical no-op. Call the store's `_validate_candidate_installations(operation, installations, fresh_prepared)` to compare issued object identities, exact same store/set/request, unique full member set and sizes. No file/hash/path/stat/lock/grant work occurs inside this transaction.

Sample after-lock time; resolve binding/current full task and compare original complete context, role, route/reservation/lease and prep. Append every ArtifactPublished via the metadata-only operation. Then capture authority_heads and take the last clock sample; require exact UTC, `final >= after_lock`, `final >= prepared_at`, and `accepted_at <= final < lease_expires_at`. Construct the single commit payload only after that guard. The callback returns one CandidatePublicationCommitted EventDraft carrying `utc_text(final)`. No further authority-dependent work follows; append_checked appends marker/commits. Do not re-read the clock for marker timestamp or acceptance. Any metadata/guard/marker/commit failure rolls back all final rows; installed blobs and prep survive.

```python
final = self._now()
if final < after_lock or final < prepared_at or not accepted_at <= final < expires_at:
    raise CandidatePublicationError("candidate_publication_inactive")
commit = dict(schema=SCHEMA, request_id=prepared.request_id, set_id=prepared.set_id,
              prepared_hash=candidate_hash("maestro.candidate-prepared.v1", prepared.prepared),
              authority_heads=list(heads), committed_at=utc_text(final))
return [EventDraft("CandidatePublicationCommitted", commit,
    run_id=context.run_id, node_id=context.node_id, attempt_id=context.attempt_id,
    fencing_generation=context.fencing_generation,
    correlation_id=context.run_id, causation_id=prepared.set_id)]
```

After commit, replay exact durable journal in one snapshot before issuing informational receipt. Response-loss/unreadable handling follows the same uncertainty rule as preparation. A valid final decision can commit/return after lease expiry; it does not refresh lease or grant later authority. Callback body must not recursively validate the now-partial final metadata before marker return; the eventual complete stream/journal replay happens after outer commit.

Application constructs CandidatePublicationService only when its existing real ArtifactStore exists, on the same store/scheduler/recovery composition, before startup. Add exact `publish_worker_candidate` and `candidate_publication_status` keyword-only signatures from the service, invoke `_require_ready` first, reject absent service with detached unavailable error. Status opens/reuses the journal's snapshot before discovery, returns absent only after valid readable absence, prepared/committed with deterministic IDs otherwise; no clock/active check/files/grants. API signatures contain no decoded task/result, role, metadata, path, time or receipt parameter. Close/unready application errors preserve readiness semantics and do not mutate state.

- [ ] **Step 6: Adversarial input and detached error tests.** Parameterize payload bytes subclass/bytearray/memoryview/empty/overcap, text subclass/blank/100,001 chars/lone surrogate, bool/nonpositive generations, overlong identifiers, changed result IDs/context/digests/sizes/count/policy, noncandidate frames, duplicate/partial/extra mappings, decoder-return dataclass subclasses/object.__new__/object.__setattr__, nested model_construct/model_copy extras and overridden model_dump. Assert no candidate/intent/file effects for invalid new inputs. For valid changed request after prep/commit assert conflict and unchanged durable counts/IDs/timestamps.

Inject a unique sensitive sentinel in raw frame, task text, decoder exception, nested validation, filesystem/backend/clock errors before and after preparation. Inspect `str`, `repr`, `.args`, `.code`, `.__context__`, `.__cause__` recursively and captured log text plus persisted event payloads. All owned candidate errors have fixed allowlisted code/args, None context/cause; raw bytes/text/Base64/backend messages/path never persist/log. After ambiguous effects the code is uncertain, not no-effect. Use `caplog` and direct SQL event payload reads only in tests. No memory-erasure claim.

```python
def test_owned_errors_are_content_free(tmp_path, monkeypatch, caplog):
    case = candidate_case(tmp_path)
    sentinel = "SENSITIVE-CANDIDATE-7e119"
    try:
        service = case.app._candidate_publications
        def broken_clock():
            raise RuntimeError(sentinel)
        monkeypatch.setattr(service, "_clock", broken_clock)
        with pytest.raises(CandidatePublicationError) as caught:
            case.app.publish_worker_candidate(**case.identity, task_text=case.text,
                payload=candidate_frame(case.task, (b"\0",)))
        error = caught.value
        assert error.__context__ is None and error.__cause__ is None
        assert error.args == (error.code,)
        assert sentinel not in str(error) + repr(error) + caplog.text
        assert all(sentinel not in row[0] for row in case.store._connection.execute(
            "SELECT payload_json FROM events"))
    finally:
        case.app.close()
```

- [ ] **Step 7: Pin clocks, current authority and actual transaction races.** `test_final_clock_sample_controls_whole_commit` supplies a deterministic sequence clock: task/prepare samples, final after-lock sample at expiry minus 2 seconds, and last sample at expiry minus 1 second. Monkeypatch the metadata-only append wrapper to advance the trusted test clock between member appends. Read the marker and assert committed_at equals the exact final sample, differs from after-lock, and all member records retain preparation created_at. Parameterize final sample equal expiry, past expiry, before prep and before after-lock: every case must leave prep/intents/blobs but zero final publication/marker rows and no cache eligibility. Inject failures immediately after guard (EventDraft/outer append seam), in marker append, and at actual SQLite commit (connection wrapper or append_checked wrapper before/after real commit). Replay distinguishes rollback vs response loss; valid guard plus delayed response after expiry remains committed.

In `test_lock_wait_crosses_lease_deadline`, a second real SQLite connection owns BEGIN IMMEDIATE while publisher announces entry into append_checked before lock acquisition; advance test clock to exact expiry, release holder, and assert the after-lock sample rejects. Do not wait on a writer while retaining its conflicting lock. Use Pipes/Events with 10-second rendezvous/15-second join bounds. Parameterize authority changes committed before prepare/finalization and after publication: cancellation, graph append, accepted fence replacement, reservation release/settlement, Agent terminal status, lease expiry. Create valid transitions through existing APIs where possible; malformed isolated reservation/Agent corruption must fail as unavailable/integrity, not be asserted as valid lifecycle. Before final decision changes reject the entire set; after valid commit history remains and current acceptance fails independently. No publication call writes usage, lifecycle, Agent completion or resource release.

Run identical and changed same-Attempt publishers concurrently using independent process-local applications/connections. Same request converges to exactly one prep/commit and two artifact events per member; changed request produces one winner/one conflict. Race ordinary publish with candidate stage on a shared digest, different Attempts and different Runs; assert retained inode/content, distinct scoped metadata, stable retry timestamps and no cross-Run reads. Extend Task-1 inversion detector to pause `_stage_candidate_blob` after actual digest/mutex acquisition before its journal read. Exercise sorted multi-member processing and opposite input order across threads, with no nested digest ownership.

- [ ] **Step 8: Implement the complete crash/restart matrix with real process death.** Use existing `tests.support.process_crash.block_at_crash_point` and `kill_at_crash_point` and spawned child entry points in `tests/integration/test_candidate_publication_crashes.py`. Children reopen the application's real database/artifact root and use the persisted original identity/text/frame from the test-only parent input; no credentials/Provider request. Test wrappers announce exact call boundary via Pipe and block; parent sends SIGKILL only to that test child. Ten-second barrier, fifteen-second join; no uncontrolled sleeps. Every reopen uses fresh objects and audits readiness, journal, exact event counts, actual retained files and resource holds.

| Parameter points (test names use these exact labels) | Reopened expectation |
| --- | --- |
| `before_prepare`, `before_intent_0`, `after_intent_0`, `before_intent_1`, `after_intent_1`, `before_prepare_commit` | Rolled-back outer preparation: no candidate prep/intents/files from request. Existing independent artifacts unchanged. |
| `after_prepare_commit`, `before_temp_0`, `after_temp_fsync_0`, `before_temp_1`, `after_temp_fsync_1` | Complete prepared set/intents, no metadata/marker; private leftovers may exist, no automatic deletion or install. |
| `before_link_0`, `after_link_0`, `before_dir_fsync_0`, `after_dir_fsync_0`, and same four `_1` points | Complete preparation, 0/1/2 retained final files depending actual cut, no metadata/marker. Any present file is rehashed under lock on explicit retry. |
| `before_final`, `before_metadata_0`, `after_metadata_0`, `before_metadata_1`, `after_metadata_1`, `before_final_guard`, `after_final_guard`, `before_marker`, `after_marker`, `before_final_commit` | All final DB rows rolled back; prep/intents/files retained, status prepared, every candidate gate rejects. |
| `after_final_commit`, `before_response`, `after_response` | Complete exact set visible, status committed, historical exact retry is read-only/idempotent. |
| `prepare_commit_response_lost`, `final_commit_response_lost`, `lookup_unreadable_after_prepare`, `lookup_unreadable_after_final` | Real committed/rolled-back database plus response fault determines coherent replay; unreadable returns uncertain and no speculative install/delete/republish. |

For every installed-file cut test both fresh digest and a digest already published by Run A. For reuse cuts wrap actual directory fsync and prove it is executed; Run A remains readable under its own valid grant. Include OSError-after-link and OSError-after-fsync injection as well as kill. A commit-response exception wrapper calls the real commit first then raises; precommit exception raises before it. SQLite connection methods are immutable in CPython, so wrap the connection in a delegating test class exposing execute/in_transaction/commit/rollback/close and forwarding other attributes, or patch the enclosing append_checked boundary; do not assume monkeypatching sqlite3.Connection.commit works.

Start a fresh ControlPlaneApplication after each retained state and compare resource/event counts before/after startup: no auto-publication/verifier/Worker/provider call, no settlement from candidate state, no deletion. Existing Provider-settlement startup is tested separately for its permitted atomic behavior; corrupt candidate history must roll that startup transaction back. Missing committed bytes, malformed scope, marker/head/context/order/header/digest corruption fail startup/readiness and retain files. Pending stale history is valid and reported with observational content state; no automatic retry.

- [ ] **Step 9: Add historical exact retry and status barrier tests.** Reuse Task-2 read barriers on actual public status and historical retry, both journal-first and artifact-first lookup internals, with concurrent real finalization. Exact committed retry after cancellation/expiry/fence replacement reconstructs original role/text and decodes same frame, returns historical receipt without calling `_now`, WorkerInputService.prepare_worker_task, staging, grant issuance or a write callback. Changed valid bytes/result/context reject; wrong text cannot compare and rejects. Status of absent/prepared/committed is content-free, ready-gated and corruption-safe. Receiving/constructing any receipt cannot satisfy admission or terminal completion. After finalization, run existing independent verifier/acceptance path with the complete set and test trusted usage fixture; publication by itself leaves Attempt accepted and reservations held.

- [ ] **Step 10: Run GREEN, document exact boundary, complete stage and whole-slice acceptance.**

```bash
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/artifacts tests/unit/runtime tests/unit/lifecycle/test_scheduler.py tests/integration/test_candidate_publication_transactions.py tests/integration/test_candidate_publication_crashes.py tests/integration/test_candidate_read_snapshots.py tests/integration/test_candidate_history.py tests/integration/test_artifact_recovery_lock_order.py tests/integration/test_isolated_worker_process.py tests/integration/test_isolated_verifier_process.py tests/integration/test_systemd_launcher.py tests/integration/test_systemd_provider_sender.py tests/integration/test_systemd_isolation_probe.py -q -ra -W error
```

Update worker-runtime.md to describe the actual ready-gated trusted host APIs, deterministic committed receipt/status, full-set policy, retained pending states, explicit retry, historical clock meaning, same-store composition and no content logging. Replace the now-obsolete claim that a candidate-byte publisher is absent; retain blocked-only Worker and current encoded transport limits, no process attestation, semantic success, usage service, streaming output, P5/V1 completion or paid Provider claims. Include measured stage/final evidence only after it exists.

Complete common final-source gate and independent task review. Controller then requests independent whole-slice review of BASE through the final source candidate, focusing on all five Review Focus risks, cross-task interfaces, all current recovery owners, every consumer and the crash table. Resolve only scoped findings through the original task implementer with fresh source acceptance/review. Bind final results to actual final committed source SHA; reuse unchanged-source test evidence after review. Suggested commit: `feat: publish trusted Worker candidate sets atomically`. Controller pushes only after review and actual upstream/remote/clean verification. Do not begin transport integration as part of this task.

## Concrete implementation/test addendum, owned by the indicated tasks

These bodies belong to the task files named below, not a fourth independently shipped stage. The **execution order** is the RED-first schedule in the revision section below: write each named test group and capture its behavioral RED before its corresponding production edits, irrespective of where explanatory snippets appear in this document. Imports used by snippets are explicit here; add the corresponding imports to each owning module. Helpers use real database/filesystem state and never import another module's private fixture.

### Revision R1: predecode ownership boundary (Task 2)

Add `tests/unit/persistence/test_candidate_read_bounds.py` to Task 2's test files and `sqlite_event_store.py` to its modified files. Implement `_preflight_candidate_rows(self, stream_type: str, stream_id: str) -> None` and `_read_rows_unchecked` below. Rename the existing `_read_rows` implementation to `_read_rows_unchecked`; preserve its SQL/row conversion body. Put this wrapper at **every** caller's existing `_read_rows` name. Replace `read_stream`'s independent SELECT/fetchall with the wrapper invocation shown. Also insert the same preflight into the independent versioned loader at the exact boundary specified below. No journal callback is the first preflight.

```python
def _preflight_candidate_rows(self, stream_type, stream_id):
    if stream_type != "candidate_publications":
        return
    if not self._connection.in_transaction:
        raise EventIntegrityError("candidate read requires snapshot")
    # SUM/MAX inspect SQLite storage, never return payloads to Python.
    row = self._connection.execute(
        "SELECT COUNT(*), COALESCE(SUM(length(CAST(payload_json AS BLOB))),0), "
        "COALESCE(MAX(length(CAST(payload_json AS BLOB))),0) "
        "FROM events WHERE stream_type=? AND stream_id=?",
        (stream_type, stream_id),
    ).fetchone()
    if row[0] > 2048 or row[1] > 16 * 1024 * 1024 or row[2] > 1_048_576:
        raise EventIntegrityError("candidate stored payload bounds exceeded")

def _read_rows(self, stream_type, stream_id, *, first_version, last_version):
    with self.read_snapshot():
        self._preflight_candidate_rows(stream_type, stream_id)
        return self._read_rows_unchecked(stream_type, stream_id,
            first_version=first_version, last_version=last_version)

def read_stream(self, stream_type, stream_id, after_version=0):
    stream_type = _validate_identifier(stream_type, "stream_type")
    stream_id = _validate_identifier(stream_id, "stream_id")
    after_version = _validate_version(after_version, "after_version")
    with self.read_snapshot():
        return self._read_rows(stream_type, stream_id,
            first_version=after_version + 1,
            last_version=self.current_version(stream_type, stream_id))
```

In the existing `read_stream_with_version(self, stream_type: str, stream_id: str, after_version: int = 0) -> tuple[list[StoredEvent], int]`, insert this first statement inside its existing `try`, after BEGIN or SAVEPOINT acquisition and **before** its independent payload SELECT/fetchall:

```python
self._preflight_candidate_rows(stream_type, stream_id)
```

Keep that method's argument validation, row/version queries, decoding, return shape and own-transaction/savepoint success/error cleanup unchanged. The preflight and all payload/version reads consequently use that same owned/reused snapshot. `validated_stream`, `stream_witness`, fresh inventory capture and journal `_stream` inherit this bound through their existing versioned-read call; their later COUNT/schema checks are not substitutes for it.

`append_checked` already owns BEGIN IMMEDIATE/savepoint before `_read_rows`; normal append (including its idempotent return) already owns its transaction before `_read_rows`. Thus both check **fresh** raw bounds before returning any payload column. An empty version range still checks the entire raw candidate stream, so corrupt orphan rows beyond stream_versions cannot bypass the guard. Canonical event/replay validation and commit-reservation accounting remain additional checks after this raw guard. `validated_stream` must also detect orphan rows: compare its length/current_version with SQL COUNT over the whole stream in the same snapshot, including version zero/beyond-head corruption. No cap is lowered for tests.

- [ ] Write/run this regression before the wrapper. In the first Task-2 RED it must reach `_row_to_event` on the poisoned stream and raise the assertion below, not merely fail a missing module import. Then implement the wrapper and rerun the exact command for GREEN.

```python
import hashlib
import sqlite3
import pytest
from orchestrator.persistence import EventDraft, SQLiteEventStore, EventIntegrityError
from tests.support.candidate_publication import candidate_case

@pytest.mark.parametrize("kind", ["event", "aggregate", "count"])
@pytest.mark.parametrize("entry, outer", [
    ("checked", None), ("append", None), ("read", None),
    ("versioned", None), ("validated", None), ("inventory", None),
    ("versioned", "BEGIN"), ("versioned", "BEGIN IMMEDIATE"),
    ("validated", "BEGIN"), ("validated", "BEGIN IMMEDIATE"),
])
def test_candidate_bounds_precede_decode_after_capture(tmp_path, monkeypatch, kind, entry, outer):
    case = candidate_case(tmp_path)
    recovery = case.app._scheduler.recovery
    # A separate real connection already exists before writer ownership.
    other = sqlite3.connect(case.database, isolation_level=None)
    # Fresh inventory must reach its loader, not reject an existing capture registration.
    inventory = None if entry == "inventory" else recovery._capture_inventory(("run-1",))
    assert not case.store.in_transaction
    try:
        count, size = {"event": (1, 1_048_577),
                       "aggregate": (17, 1_048_576),
                       "count": (2049, 2)}[kind]
        payload = "{}" if size == 2 else '"' + "x" * (size - 2) + '"'
        other.execute("BEGIN IMMEDIATE")
        other.execute("INSERT INTO stream_versions VALUES (?, ?, ?)",
                      ("candidate_publications", "run-1", count))
        for i in range(count):
            other.execute("INSERT INTO events (event_id,stream_type,stream_id,stream_version,"
                "event_type,schema_version,occurred_at,payload_json,payload_hash,idempotency_key,"
                "run_id,node_id,attempt_id,fencing_generation,correlation_id,causation_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (f"bound-{i}", "candidate_publications", "run-1", i + 1,
                 "CandidatePublicationPrepared", 1, "2026-10-11T00:00:00+00:00",
                 payload, hashlib.sha256(payload.encode()).hexdigest(), f"bound-{i}",
                 "run-1", "node-1", "attempt-1", 1, "run-1", "fixture"))
        other.commit()
        original = case.store._row_to_event
        def checked_row(row):
            assert row["stream_type"] != "candidate_publications", "decoded before bounds"
            return original(row)
        monkeypatch.setattr(case.store, "_row_to_event", checked_row)
        monkeypatch.setattr(case.artifacts, "_digest_lock",
            lambda *a: pytest.fail("filesystem entered before rejection"))
        # Construct a valid generic draft OUTSIDE raises; bounds must fire before
        # the later candidate-stream event-contract check can reject this probe.
        probe = EventDraft("Probe", {"value": 1}, **case.identity,
                           correlation_id="run-1", causation_id="bounds-probe")
        if outer:
            case.store._connection.execute(outer)
        with pytest.raises(EventIntegrityError, match="bounds exceeded"):
            if entry == "checked":
                # Uses the already valid captured inventory; corruption arrived AFTER capture.
                case.store.append_checked("candidate_publications", "run-1", "bound",
                    lambda events, version: recovery._validate_inventory(inventory))
            elif entry == "append":
                case.store.append("candidate_publications", "run-1", count,
                    [probe], "bound")
            elif entry == "read":
                case.store.read_stream("candidate_publications", "run-1")
            elif entry == "versioned":
                case.store.read_stream_with_version("candidate_publications", "run-1")
            elif entry == "validated":
                case.store.validated_stream("candidate_publications", "run-1")
            else:
                recovery._capture_inventory(("run-1",))
        assert case.store.in_transaction is (outer is not None)
    finally:
        if case.store.in_transaction:
            case.store._connection.rollback()
        other.close()
        case.app.close()
```

Command: `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/persistence/test_candidate_read_bounds.py -q -ra -W error`.

### Revision R2-A: inventory ownership and database-only recovery bodies (Task 1)

In `recovery/artifact_inventory.py`, add dataclass imports and the following alongside the three inventory dataclasses in the Task-1 interface table. This captures every artifact event hash, not just latest metadata, and all discovery memberships before observation. `run_ids=None` means all discovered Runs. An explicit Run without artifacts works without a configured ArtifactStore; any relevant intent/publication still fails as it does today.

```python
_MEMBERSHIP_TYPES = ("artifact", "candidate_publications", "run", "run_lifecycle")

@dataclass(frozen=True)
class StreamWitness:
    stream_type: str
    stream_id: str
    version: int
    event_id: str | None
    payload_hash: str | None
    entries: tuple[tuple[int, str, str], ...]

@dataclass(frozen=True)
class ArtifactMetadataInventory:
    witness: tuple[StreamWitness, ...]
    publications: tuple[ArtifactRecord, ...]
    pending: tuple[tuple[StoredEvent, ArtifactRecord], ...]

@dataclass(frozen=True)
class ArtifactObservations:
    publications: tuple[ArtifactRecord, ...]
    pending: tuple[PendingArtifactPublication, ...]

@dataclass(frozen=True)
class _RecoveryInventory:
    owner: object
    store: object
    artifacts: object
    nonce: object
    run_ids: tuple[str, ...]
    membership: tuple[tuple[str, tuple[str, ...]], ...]
    witness: tuple[StreamWitness, ...]
    observations: ArtifactObservations

def stream_witness(store, kind, name):
    events = store.validated_stream(kind, name)
    head = events[-1] if events else None
    return StreamWitness(kind, name, len(events),
        None if head is None else head.event_id,
        None if head is None else head.payload_hash,
        tuple((e.stream_version, e.event_id, e.payload_hash) for e in events))

def inventory_membership(store):
    return tuple((kind, tuple(sorted(store.stream_ids(kind))))
                 for kind in _MEMBERSHIP_TYPES)
```

Add these methods to RunRecoveryCoordinator and initialize `_captured = self._scope = None`. `_inventory_metadata` is called while this snapshot is active and must reuse it; no observer receives a live store iterator. The actual event tuples used by `_inventory_metadata` and witness construction are read in this same snapshot.

```python
class InventoryChanged(RunRecoveryError):
    pass

def _capture_inventory(self, run_ids):
    if self.event_store.in_transaction or self._captured is not None:
        raise RunRecoveryError("recovery capture ownership invalid")
    store, artifacts = self.event_store, self.artifact_store
    if artifacts is not None and artifacts._event_store is not store:
        raise RunRecoveryError("artifact verifier is bound to a different event store")
    with store.read_snapshot():
        membership = inventory_membership(store)
        names = dict(membership)
        runs = tuple(sorted(set(names["run"]) | set(names["run_lifecycle"])))
        selected = runs if run_ids is None else tuple(sorted(set(run_ids)))
        witness = tuple(stream_witness(store, kind, name)
            for kind, members in membership if kind in ("artifact", "candidate_publications")
            for name in members)
        if artifacts is None:
            for digest in names["artifact"]:
                for event in store.validated_stream("artifact", digest):
                    source = event.payload.get("source")
                    if (event.event_type in ("ArtifactPublished", "ArtifactPublicationIntent")
                        and isinstance(source, dict) and source.get("run_id") in selected):
                        raise RunRecoveryError("Run artifacts require configured verifier")
            metadata = None
        else:
            metadata = artifacts._inventory_metadata()
    observations = (ArtifactObservations((), ()) if metadata is None
                    else artifacts._observe_inventory(metadata))
    result = _RecoveryInventory(self, store, artifacts, object(), selected,
                                membership, witness, observations)
    self._captured = result
    return result

def _validate_inventory(self, inventory):
    if (type(inventory) is not _RecoveryInventory or self._captured is not inventory
        or inventory.owner is not self or inventory.store is not self.event_store
        or inventory.artifacts is not self.artifact_store
        or not self.event_store._has_checked_write_owner()):
        raise RunRecoveryError("recovery inventory ownership invalid")
    membership = inventory_membership(self.event_store)
    if membership != inventory.membership:
        raise InventoryChanged("recovery inventory changed")
    current = tuple(stream_witness(self.event_store, kind, name)
                    for kind, names in membership if kind in ("artifact", "candidate_publications")
                    for name in names)
    if current != inventory.witness:
        raise InventoryChanged("recovery inventory changed")

def _recover_in_transaction(self, run_id):
    scope = self._scope
    if scope is None or run_id not in scope.run_ids:
        raise RunRecoveryError("recovery inventory scope missing")
    self._validate_inventory(scope)
    return self._recover_database(run_id)

def _observed_publications(self, run_id):
    if self._scope is None:
        raise RunRecoveryError("recovery inventory scope missing")
    return tuple(r for r in self._scope.observations.publications
                 if r.source.get("run_id") == run_id)

def _observed_pending(self, run_id):
    if self._scope is None:
        raise RunRecoveryError("recovery inventory scope missing")
    return tuple(p for p in self._scope.observations.pending
                 if p.record.source.get("run_id") == run_id)

def recover(self, run_id):
    result = None
    failed = False
    try:
        if self.event_store.in_transaction:
            return self._recover_in_transaction(run_id)
        def decide(events, version):
            nonlocal result
            result = self._recover_in_transaction(run_id)
            return None
        self._checked((run_id,), "scheduler", "global", "recover:" + run_id, decide)
    except Exception:
        failed = True
    if failed or result is None:
        raise RunRecoveryError("Run recovery failed a durable-state invariant")
    return result
```

Rename existing `_recover` to `_recover_database`; retain its entire current lifecycle/Agent/budget/effects/lease/provenance cross-check body. In `_recover_artifacts`, replace only the discovery/configured-store/`verify_run_artifacts` block preceding `recovered: list[RecoveredArtifact]` with `records = self._observed_publications(run_id)`. In `_recover_pending_artifacts`, replace only its store/try/`pending_publications_for_run` block preceding `recovered: list[RecoveredPendingArtifact]` with `pending = self._observed_pending(run_id)`. Retain both existing projection/provenance loops exactly. These are mechanical relocations of unchanged checks, not new abbreviated reducers. `_recover_database` remains private and is called only by `_recover_in_transaction`.

The first `_validate_inventory` protects outside observations against external changes; later nested operations in the same writer may intentionally append run_lifecycle events during startup settlements. The code tracks artifact/candidate event witnesses and all four discovery memberships, **not mutable Run/lifecycle event prefixes**. Current lifecycle/Agent/budget/scheduler/effect events are re-reduced on every recovery, including after Provider settlement. This avoids falsely retrying a legitimate own lifecycle append while retaining every artifact event hash and every Run membership. When candidate preparation/finalization intentionally appends artifact events, perform the last live recovery before those appends; do not recurse into recovery afterward.

In ArtifactStore, these bodies perform the metadata/observation split. Add local imports of inventory dataclasses to avoid the store/recovery import cycle, and `stat` (already present). `_record_from_event` remains the existing complete parser; candidate raw validation in Task 2 precedes it.

```python
def _inventory_metadata(self):
    from orchestrator.recovery.artifact_inventory import (
        ArtifactMetadataInventory, stream_witness)
    publications, pending, witnesses = [], [], []
    with self._event_store.read_snapshot():
        for digest in sorted(self._event_store.stream_ids("artifact")):
            events = self._event_store.validated_stream("artifact", digest)
            witnesses.append(stream_witness(self._event_store, "artifact", digest))
            intents, published = {}, {}
            for event in events:
                if event.event_type not in ("ArtifactPublished", "ArtifactPublicationIntent"):
                    continue
                record = self._record_from_event(event, digest)
                target = published if event.event_type == "ArtifactPublished" else intents
                if record.publication_id in target:
                    raise ArtifactIntegrityError(digest, "duplicate publication identity")
                target[record.publication_id] = (event, record)
            for publication_id, (event, record) in intents.items():
                final = published.get(publication_id)
                if final is None:
                    pending.append((event, record))
                elif final[1] != record or event.stream_version >= final[0].stream_version:
                    raise ArtifactIntegrityError(digest, "publication intent mismatch")
            publications.extend(record for event, record in published.values())
    return ArtifactMetadataInventory(tuple(witnesses), tuple(publications), tuple(pending))

def _observe_inventory(self, metadata):
    from orchestrator.recovery.artifact_inventory import ArtifactObservations
    self._require_no_transaction()
    pending = []
    digests = sorted({r.digest for r in metadata.publications} |
                     {r.digest for event, r in metadata.pending})
    for digest in digests:
        published = tuple(r for r in metadata.publications if r.digest == digest)
        waiting = tuple((e, r) for e, r in metadata.pending if r.digest == digest)
        with self._digest_lock(digest):
            path = self._path_for_digest(digest)
            if published:
                size = self._verify_file_and_size(path, digest)
                if any(r.size != size for r in published):
                    raise ArtifactIntegrityError(digest, "published size mismatch")
            for event, record in waiting:
                try:
                    mode = path.lstat().st_mode
                except FileNotFoundError:
                    state = "missing"
                else:
                    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
                        raise ArtifactIntegrityError(digest, "pending object invalid")
                    if self._verify_file_and_size(path, digest) != record.size:
                        raise ArtifactIntegrityError(digest, "pending size mismatch")
                    state = "already_published" if published else "orphaned_blob"
                pending.append(PendingArtifactPublication(record, event.event_id, state))
    return ArtifactObservations(metadata.publications, tuple(pending))
```

Public inventory methods call `_require_no_transaction`, materialize `_inventory_metadata`, observe it after closure, and return the filtered publication/pending tuples in existing stable sort order. Tiny non-SQLite generic adapters retain the original generic implementation branch; recovery itself requires SQLite. Task 2 adds `CandidatePublicationJournal.audit_all()` inside `_inventory_metadata`'s snapshot before parsing any reserved record, and filters no journal or artifact links out of the witness.

### Task 1: projection and lock-entry details

### Revision R3-B: exact crash seams, real child target and replay assertions (Task 3)

Put `install_candidate_hooks(app, monkeypatch, hit)` in the existing `tests/support/candidate_publication.py`; its Task-3-only imports are local as shown, so importing Task-1 support never imports the not-yet-created publisher. Import that public helper explicitly in `tests/integration/test_candidate_publication_crashes.py`, `tests/integration/test_candidate_publication_transactions.py`, and `tests/unit/runtime/test_candidate_publication.py`. Keep `CRASH_POINTS`, `candidate_crash_child` and the crash tests below in `test_candidate_publication_crashes.py`; that module imports `os`, `stat`, `tempfile`, `multiprocessing`, `datetime`, `pytest`, `ControlPlaneApplication`, `CandidatePublicationJournal`, and existing process_crash helpers. The installer wraps real operations rather than replacing locks, writes, fsync or commits. `hit` blocks only at the selected name. `_prepare` and `_finalize` phase wrappers bracket their complete methods; only an actual BEGIN IMMEDIATE transaction triggers commit seams, not read-snapshot commits or nested savepoint release.

```python
CRASH_POINTS = (
    "before_prepare", "before_intent_0", "after_intent_0", "before_intent_1", "after_intent_1",
    "before_prepare_commit", "after_prepare_commit",
    "before_temp_0", "after_temp_fsync_0", "before_temp_1", "after_temp_fsync_1",
    "before_link_0", "after_link_0", "before_dir_fsync_0", "after_dir_fsync_0",
    "before_link_1", "after_link_1", "before_dir_fsync_1", "after_dir_fsync_1",
    "before_final", "before_metadata_0", "after_metadata_0", "before_metadata_1", "after_metadata_1",
    "before_final_guard", "after_final_guard", "before_marker", "after_marker",
    "before_final_commit", "after_final_commit", "before_response", "after_response",
)

def install_candidate_hooks(app, monkeypatch, hit):
    import os
    import stat
    import tempfile
    from orchestrator.runtime import candidate_publication as publication
    service, store, artifacts = app._candidate_publications, app._event_store, app._artifact_store
    state = dict(phase=None, member=-1, intent=0, metadata=0, clock=0)
    class ConnectionProxy:
        def __init__(self, real):
            self.real, self.writing = real, False
        def __getattr__(self, name):
            return getattr(self.real, name)
        def execute(self, sql, parameters=()):
            result = self.real.execute(sql, parameters)
            if sql.strip().upper() == "BEGIN IMMEDIATE":
                self.writing = True
            elif sql.strip().upper() == "BEGIN":
                self.writing = False
            return result
        def commit(self):
            phase = state["phase"] if self.writing else None
            if phase in ("prepare", "final"):
                hit("before_" + phase + "_commit")
            result = self.real.commit()
            self.writing = False
            if phase in ("prepare", "final"):
                hit("after_" + phase + "_commit")
            return result
        def rollback(self):
            self.writing = False
            return self.real.rollback()
    monkeypatch.setattr(store, "_connection", ConnectionProxy(store._connection))
    for name, phase in (("_prepare", "prepare"), ("_finalize", "final")):
        original = getattr(service, name)
        def phase_call(*args, _original=original, _phase=phase, **kwargs):
            hit("before_" + _phase)
            prior = state["phase"]
            state["phase"] = _phase
            try:
                return _original(*args, **kwargs)
            finally:
                state["phase"] = prior
        monkeypatch.setattr(service, name, phase_call)
    for name, label in (("_record_candidate_intent", "intent"), ("_record_candidate_metadata", "metadata")):
        original = getattr(artifacts, name)
        def record(*args, _original=original, _label=label, **kwargs):
            index = state[_label]
            state[_label] += 1
            hit(f"before_{_label}_{index}")
            result = _original(*args, **kwargs)
            hit(f"after_{_label}_{index}")
            return result
        monkeypatch.setattr(artifacts, name, record)
    stage = artifacts._stage_candidate_blob
    def stage_call(**kwargs):
        state["member"] += 1
        return stage(**kwargs)
    monkeypatch.setattr(artifacts, "_stage_candidate_blob", stage_call)
    mkstemp, fsync, link, directory = tempfile.mkstemp, os.fsync, os.link, artifacts._fsync_directory
    def temporary(*args, **kwargs):
        hit(f"before_temp_{state['member']}")
        return mkstemp(*args, **kwargs)
    def sync(fd):
        regular = stat.S_ISREG(os.fstat(fd).st_mode)
        result = fsync(fd)
        if regular and state["member"] >= 0:
            hit(f"after_temp_fsync_{state['member']}")
        return result
    def linking(*args, **kwargs):
        hit(f"before_link_{state['member']}")
        try:
            result = link(*args, **kwargs)
        except FileExistsError:
            hit(f"after_link_{state['member']}")
            raise
        hit(f"after_link_{state['member']}")
        return result
    def directory_sync():
        hit(f"before_dir_fsync_{state['member']}")
        result = directory()
        hit(f"after_dir_fsync_{state['member']}")
        return result
    monkeypatch.setattr(tempfile, "mkstemp", temporary)
    monkeypatch.setattr(os, "fsync", sync)
    monkeypatch.setattr(os, "link", linking)
    monkeypatch.setattr(artifacts, "_fsync_directory", directory_sync)
    now = service._now
    def clock():
        if state["phase"] == "final":
            state["clock"] += 1
            if state["clock"] == 2:
                hit("before_final_guard")
        return now()
    monkeypatch.setattr(service, "_now", clock)
    make_event = publication._event
    def draft(kind, payload, context, set_id):
        if kind == "CandidatePublicationCommitted":
            hit("after_final_guard")
        return make_event(kind, payload, context, set_id)
    monkeypatch.setattr(publication, "_event", draft)
    append = store.append
    def appending(kind, name, expected, events, key):
        rows = list(events)
        marker = any(e.event_type == "CandidatePublicationCommitted" for e in rows)
        if marker:
            hit("before_marker")
        result = append(kind, name, expected, rows, key)
        if marker:
            hit("after_marker")
        return result
    monkeypatch.setattr(store, "append", appending)

def candidate_crash_child(database, root, identity, text, payload, now, point, pipe):
    app = ControlPlaneApplication(database, artifact_root=root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
    app._candidate_publications._clock = lambda: datetime.fromisoformat(now)
    def hit(name):
        if name == point:
            block_at_crash_point(pipe, name)
    with pytest.MonkeyPatch.context() as patch:
        install_candidate_hooks(app, patch, hit)
        receipt = app.publish_worker_candidate(**identity, task_text=text, payload=payload)
        hit("before_response")
        assert receipt.state == "committed"
        hit("after_response")
    app.close()

@pytest.mark.parametrize("point", CRASH_POINTS)
@pytest.mark.parametrize("shared", [False, True])
def test_actual_process_death_preserves_candidate_state(tmp_path, point, shared):
    case = candidate_case(tmp_path)
    bodies = (b"first", b"second")
    refs = tuple(sorted((ArtifactRef(digest="sha256:" + hashlib.sha256(b).hexdigest(),
        size_bytes=len(b), artifact_type="worker-candidate", media_type="application/octet-stream")
        for b in bodies), key=lambda r: r.digest))
    if shared:
        for body in bodies:
            case.artifacts.publish_bytes(body, source={"run_id": "run-A"}, readable_scope=("run-A",))
    payload = candidate_frame(case.task, bodies)
    before = event_counts(case.store)
    case.app.close()
    mp = multiprocessing.get_context("spawn")
    parent, child = mp.Pipe()
    process = mp.Process(target=candidate_crash_child, args=(case.database, case.root,
        case.identity, case.text, payload, case.clock.now.isoformat(), point, child))
    try:
        process.start()
        kill_at_crash_point(process, parent, expected_point=point, timeout_seconds=10)
        reopened = ControlPlaneApplication(case.database, artifact_root=case.root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
        try:
            journal = CandidatePublicationJournal(reopened._event_store)
            state = reopened.candidate_publication_status(**case.identity)
            absent = point in {"before_prepare", "before_intent_0", "after_intent_0",
                "before_intent_1", "after_intent_1", "before_prepare_commit"}
            committed = point in {"after_final_commit", "before_response", "after_response"}
            assert state.state == ("absent" if absent else "committed" if committed else "prepared")
            candidate = journal.lookup(**case.identity)
            if absent:
                assert candidate is None
            else:
                assert candidate.members == refs
                for record in candidate.records:
                    rows = [e for e in reopened._event_store.read_stream("artifact", record.digest)
                            if e.payload.get("publication_id") == record.publication_id]
                    assert [e.event_type for e in rows] == (["ArtifactPublicationIntent", "ArtifactPublished"]
                        if committed else ["ArtifactPublicationIntent"])
            after = event_counts(reopened._event_store)
            assert all(after.get(k) == v for k, v in before.items()
                       if k[0] in {"run", "run_lifecycle", "agent_registry", "budget", "scheduler"})
            files_before = {p.name: p.read_bytes() for p in case.root.iterdir()
                            if p.is_file() and not p.name.startswith(".")}
            for body in bodies:
                digest = "sha256:" + hashlib.sha256(body).hexdigest()
                path = reopened._artifact_store._path_for_digest(digest)
                if path.exists():
                    assert path.read_bytes() == body
                if shared or committed:
                    assert path.exists()
            existing_member_paths = [reopened._artifact_store._path_for_digest(r.digest)
                                     for r in refs if reopened._artifact_store._path_for_digest(r.digest).exists()]
            assert len(existing_member_paths) == (2 if shared else expected_installed(point))
            if not absent and not committed:
                with pytest.raises(CandidateJournalError):
                    journal.require_set(case.task.context, refs)
            reopened.close()
            reopened = ControlPlaneApplication(case.database, artifact_root=case.root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
            assert event_counts(reopened._event_store) == after
            assert {p.name: p.read_bytes() for p in case.root.iterdir()
                    if p.is_file() and not p.name.startswith(".")} == files_before
            # Explicit retry alone completes pending work; startup did not.
            reopened._candidate_publications._clock = lambda: case.clock.now
            receipt = reopened.publish_worker_candidate(**case.identity, task_text=case.text, payload=payload)
            assert receipt.state == "committed"
            assert len(reopened._event_store.read_stream("candidate_publications", "run-1")) == 2
        finally:
            reopened.close()
    finally:
        if process.pid is not None and process.is_alive():
            process.terminate()
            process.join(15)
        parent.close()
        child.close()
```

The test's exact file-count expectation is derived from the selected cut: for fresh digests, before second member installation all first-member `after_link/after_dir_fsync` cuts retain one final path; second-member equivalents retain two; all finalization/response cuts retain two; before preparation/temp0/link0 retain zero. Add `assert len(existing_member_paths) == expected_installed(point)` using the explicit table below; shared mode always retains two. Private `.tmp-*` leftovers are deliberately excluded from count/deletion assertions.

```python
def expected_installed(point):
    if point in {"before_prepare", "before_intent_0", "after_intent_0", "before_intent_1",
        "after_intent_1", "before_prepare_commit", "after_prepare_commit", "before_temp_0",
        "after_temp_fsync_0", "before_link_0"}:
        return 0
    if point in {"after_link_0", "before_dir_fsync_0", "after_dir_fsync_0", "before_temp_1",
                 "after_temp_fsync_1", "before_link_1"}:
        return 1
    return 2
```

Separate response-fault tests reuse the **same real seams** without killing. This distinguishes an actual committed transaction from a backend exception string. Parameterize each phase's before/after commit, with and without unreadable reconciliation. Fault only once; otherwise replay itself could be repeatedly sabotaged without testing the intended boundary.

```python
@pytest.mark.parametrize("phase", ["prepare", "final"])
@pytest.mark.parametrize("side", ["before", "after"])
@pytest.mark.parametrize("unreadable", [False, True])
def test_commit_response_fault_uses_durable_replay(tmp_path, monkeypatch, phase, side, unreadable):
    case = candidate_case(tmp_path)
    fired = []
    real_lookup = case.app._candidate_publications._journal.lookup
    def hit(name):
        if name == f"{side}_{phase}_commit" and not fired:
            fired.append(name)
            if unreadable:
                def unavailable(**kwargs):
                    raise OSError("sensitive response fixture")
                monkeypatch.setattr(case.app._candidate_publications._journal, "lookup", unavailable)
            raise OSError("sensitive response fixture")
    try:
        case.app._candidate_publications._clock = lambda: case.clock.now
        install_candidate_hooks(case.app, monkeypatch, hit)
        payload = candidate_frame(case.task, (b"first", b"second"))
        if side == "after" and not unreadable:
            assert case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=payload).state == "committed"
        else:
            with pytest.raises(CandidatePublicationError) as caught:
                case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=payload)
            assert caught.value.code == ("candidate_publication_uncertain" if unreadable
                                         else "candidate_publication_unavailable")
            assert caught.value.__context__ is caught.value.__cause__ is None
        assert fired == [f"{side}_{phase}_commit"]
        monkeypatch.setattr(case.app._candidate_publications._journal, "lookup", real_lookup)
        state = case.app.candidate_publication_status(**case.identity).state
        assert state == ("absent" if phase == "prepare" and side == "before" else
                         "prepared" if side == "before" or unreadable and phase == "prepare" else "committed")
        if phase == "final":
            assert all(case.artifacts._path_for_digest("sha256:" + hashlib.sha256(b).hexdigest()).exists()
                       for b in (b"first", b"second"))
    finally:
        case.app.close()
```

Add OSError-after-link and after-dir-fsync cases by using the same hit function at both members' corresponding points; expected status prepared, zero member publications/marker, retained present final paths, and detached uncertain. Reusing an existing Run-A object must call the real directory fsync before issuing installation; the reuse-side hook proves that branch. Read Run A with a fresh real authority/grant after every injected failure to prove its independently authorized metadata/bytes survived.

### Revision R2-E: complete publisher control flow and store-issued installation (Task 3)

In candidate_publication.py use the schema/authority/journal helpers defined above, `WorkerInputError/task_from_authority`, `decode_worker_output/DecodedWorkerOutput/DecodedCandidateBlob`, and existing EventDraft. This section replaces prose-only callback assignments. Internal `_decision_code` reads only exact allowlisted owned error codes, never `str(exc)`; `_write_then_lookup` distinguishes definite replay from ambiguous effects. `CandidatePublicationService.__init__` sets the following fields only after checking exact SQLiteEventStore, Scheduler, ArtifactStore types, identity of scheduler.event_store/artifact_store._event_store/scheduler.recovery.event_store/scheduler.recovery.artifact_store, open connection via `SELECT 1`, and no transaction. Catch constructor failures and raise detached unavailable after the handler. Fields are `_store`, `_scheduler`, `_artifacts`, `_clock`, `_journal = CandidatePublicationJournal(event_store)`.

```python
def _decision_code(error):
    if type(error) is CandidatePublicationError:
        value = vars(error).get("code")
        if type(value) is str and value in _CODES:
            return value
    return "candidate_publication_unavailable"

def _event(kind, payload, context, set_id):
    return EventDraft(kind, payload, run_id=context.run_id, node_id=context.node_id,
        attempt_id=context.attempt_id, fencing_generation=context.fencing_generation,
        correlation_id=context.run_id, causation_id=set_id)

def _now(self):
    value = self._clock()
    utc_text(value)
    return value

def _task(authority, text):
    failed = False
    try:
        return task_from_authority(authority, text)
    except (WorkerInputError, ValueError):
        failed = True
    if failed:
        raise CandidatePublicationError("candidate_publication_invalid")

def _authority(self, identity, at):
    state = self._scheduler.lifecycle.replay(identity["run_id"])
    node = state.node(identity["node_id"])
    matching = [a for a in node.attempts if a.attempt_id == identity["attempt_id"]]
    if (state.status in ("cancelling", "cancelled", "failed", "succeeded") or
        not matching or matching[-1].status != "accepted" or
        matching[-1].fencing_generation != identity["fencing_generation"] or
        at >= datetime.fromisoformat(matching[-1].lease_expires_at)):
        raise CandidatePublicationError("candidate_publication_inactive")
    accepted = [e for e in self._store.validated_stream("scheduler", "global")
        if e.event_type == "RoutingDecisionAccepted" and e.run_id == identity["run_id"] and
        e.node_id == identity["node_id"] and e.attempt_id == identity["attempt_id"]]
    require(len(accepted) == 1)
    admission = plain(accepted[0].payload)
    original_context = context_json(admission["verification_context"])
    require(original_context.graph_version == admission["graph_version"] and
            type(admission["graph_version"]) is int)
    if (at < datetime.fromisoformat(admission["accepted_at"]) or
        state.graph_version > original_context.graph_version):
        raise CandidatePublicationError("candidate_publication_inactive")
    return current_candidate_authority(self._scheduler, identity, at)

def _current_task(self, identity, task_text):
    result = None
    def decide(events, version):
        nonlocal result
        result = _task(self._authority(identity, self._now()), task_text)
        return None
    self._scheduler.recovery._checked((identity["run_id"],), "scheduler", "global",
                                     "candidate-input-snapshot", decide)
    require(result is not None)
    return result

def _validate_decoded(self, task, decoded):
    task = exact_model(WorkerTask, task)
    require(task.input_artifacts == () and task.tool_capabilities == () and
            task.output_byte_limit == CONTENT_CAP)
    require(type(decoded) is DecodedWorkerOutput and type(decoded.blobs) is tuple)
    result = exact_model(WorkerResult, decoded.result)
    require(result.context == task.context and result.outcome == "candidate" and
            result.failure_code is None and 1 <= len(decoded.blobs) <= MEMBER_CAP)
    refs = {r.digest: r for r in result.artifacts}
    require(len(refs) == len(result.artifacts) == len(decoded.blobs))
    seen, total, blobs = set(), 0, []
    for blob in decoded.blobs:
        require(type(blob) is DecodedCandidateBlob and type(blob.content) is bytes)
        ref = exact_model(ArtifactRef, blob.artifact)
        require(ref.digest not in seen and refs.get(ref.digest) == ref and
            ref.artifact_type == "worker-candidate" and ref.media_type == "application/octet-stream" and
            ref.size_bytes == len(blob.content) and
            ref.digest == "sha256:" + hashlib.sha256(blob.content).hexdigest())
        total += len(blob.content)
        if total > CONTENT_CAP:
            raise CandidatePublicationError("candidate_publication_too_large")
        seen.add(ref.digest)
        blobs.append(DecodedCandidateBlob(ref, blob.content))
    ordered = tuple(sorted(blobs, key=lambda b: b.artifact.digest))
    rebuilt = WorkerResult(context=result.context, result_id=result.result_id, outcome="candidate",
                           artifacts=tuple(b.artifact for b in ordered), failure_code=None)
    return DecodedWorkerOutput(rebuilt, ordered)

def _decoded_identity(decoded):
    members = [b.artifact.model_dump(mode="json") for b in decoded.blobs]
    return set_identity(decoded.result.context,
        candidate_hash("maestro.candidate-result-id.v1", decoded.result.result_id), members)

def _same_request(candidate, identity):
    if candidate.set_id != candidate_hash("maestro.candidate-set.v1", identity):
        raise CandidatePublicationError("candidate_publication_conflict")
    p = candidate.prepared
    if any(p[k] != v for k, v in identity.items()):
        raise CandidatePublicationError("candidate_publication_conflict")

def _write_then_lookup(self, identity, expected, write, *, committed):
    code = None
    try:
        write()
    except Exception as error:
        code = _decision_code(error)
    # Run this AFTER leaving the write handler, including commit-response loss.
    found, readable = None, False
    try:
        if self._store.in_transaction:
            # An unusable/ambiguous transaction cannot prove anything.
            raise ValueError("candidate transaction still open")
        found = self._journal.lookup(**identity)
        readable = True
    except Exception:
        pass
    if not readable:
        raise CandidatePublicationError("candidate_publication_uncertain")
    if found is not None:
        _same_request(found, expected)
        if not committed or found.committed is not None:
            return found
    if code is not None:
        # Readable absence/prepared state proves this phase did not commit.
        raise CandidatePublicationError(code)
    raise CandidatePublicationError("candidate_publication_uncertain")

def _prepare(self, identity, task_text, decoded):
    expected = _decoded_identity(decoded)
    key = expected["request_id"]
    def decide(events, version):
        old = self._journal.lookup(**identity)
        if old is not None:
            _same_request(old, expected)
            if old.committed is None:
                now = self._now()
                if now < parse_utc_text(old.prepared["prepared_at"]):
                    raise CandidatePublicationError("candidate_publication_inactive")
                task = _task(self._authority(identity, now), task_text)
                if task.context != old.context:
                    raise CandidatePublicationError("candidate_publication_inactive")
                self._validate_decoded(task, decoded)
            return None
        now = self._now()
        authority = self._authority(identity, now)
        task = _task(authority, task_text)
        checked = self._validate_decoded(task, decoded)
        require(_decoded_identity(checked) == expected)
        set_id = candidate_hash("maestro.candidate-set.v1", expected)
        payload = dict(expected, schema=SCHEMA, set_id=set_id,
            reservation_id=authority.reservation_id, accepted_route_id=authority.accepted_route_id,
            accepted_at=utc_text(authority.accepted_at), lease_expires_at=utc_text(authority.lease_expires_at),
            prepared_at=utc_text(now), authority_heads=list(capture_authority_heads(self._store, identity["run_id"])),
            records=[record_payload(task.context, set_id, m, now) for m in expected["members"]])
        validate_prepared(payload)
        preparations, commits, used = replay_candidate_events(events)
        if len(preparations) >= REQUEST_CAP:
            raise CandidatePublicationError("candidate_publication_too_large")
        try:
            account_candidate_bytes(used, preparation_size=len(canonical_bytes(payload)))
        except ValueError:
            raise CandidatePublicationError("candidate_publication_too_large") from None
        # No more recovery/journal audit after partial intents begin.
        for raw in payload["records"]:
            self._artifacts._record_candidate_intent(parsed_record(raw), task.context, set_id)
        return [_event("CandidatePublicationPrepared", payload, task.context, set_id)]
    return self._write_then_lookup(identity, expected, lambda:
        self._scheduler.recovery._checked((identity["run_id"],), "candidate_publications",
            identity["run_id"], "candidate-prepare:" + key[7:], decide), committed=False)

def _finalize(self, prepared, operation, installations, task_text):
    identity = context_identity(prepared.context)
    expected = {k: prepared.prepared[k] for k in ("request_id", "context", "result_id_hash",
                                                "policy_id", "output_byte_limit", "members")}
    def decide(events, version):
        fresh = self._journal.lookup(**identity)
        require(fresh is not None)
        _same_request(fresh, expected)
        if fresh.committed is not None:
            return None
        p, context = fresh.prepared, fresh.context
        self._artifacts._validate_candidate_installations(operation, installations, fresh)
        after_lock = self._now()
        authority = self._authority(identity, after_lock)
        task = _task(authority, task_text)
        if task.context != context or (authority.reservation_id, authority.accepted_route_id,
            utc_text(authority.accepted_at), utc_text(authority.lease_expires_at)) != (
            p["reservation_id"], p["accepted_route_id"], p["accepted_at"], p["lease_expires_at"]):
            raise CandidatePublicationError("candidate_publication_inactive")
        # All authority checks precede metadata. No partial-journal replay below.
        for record in fresh.records:
            self._artifacts._record_candidate_metadata(record, context, fresh.set_id)
        heads = capture_authority_heads(self._store, context.run_id)
        prepared_at = parse_utc_text(p["prepared_at"])
        accepted_at = parse_utc_text(p["accepted_at"])
        expires_at = parse_utc_text(p["lease_expires_at"])
        # Last authority-dependent operation; this exact sample is persisted.
        final = self._now()
        if final < after_lock or final < prepared_at or not accepted_at <= final < expires_at:
            raise CandidatePublicationError("candidate_publication_inactive")
        commit = dict(schema=SCHEMA, request_id=fresh.request_id, set_id=fresh.set_id,
            prepared_hash=candidate_hash("maestro.candidate-prepared.v1", p),
            authority_heads=list(heads), committed_at=utc_text(final))
        return [_event("CandidatePublicationCommitted", commit, context, fresh.set_id)]
    found = self._write_then_lookup(identity, expected, lambda:
        self._scheduler.recovery._checked((identity["run_id"],), "candidate_publications",
            identity["run_id"], "candidate-commit:" + expected["request_id"][7:], decide), committed=True)
    return CandidatePublicationReceipt(found.request_id, found.set_id)

def _publish(self, identity, task_text, payload):
    if self._store.in_transaction:
        raise CandidatePublicationError("candidate_publication_unavailable")
    try:
        identity = identity_json(identity)
        require(type(payload) is bytes and bool(payload) and type(task_text) is str and bool(task_text.strip()))
        if len(payload) > 1_048_576 or len(task_text) > 100_000:
            raise CandidatePublicationError("candidate_publication_too_large")
        task_text.encode("utf-8", errors="strict")
    except CandidatePublicationError:
        raise
    except Exception:
        raise CandidatePublicationError("candidate_publication_invalid") from None
    old = self._journal.lookup(**identity)
    if old is not None:
        p = old.prepared
        with self._store.read_snapshot():
            authority = validate_historical_authority(self._store, old.context,
                tuple(p["authority_heads"]), parse_utc_text(p["prepared_at"]))
        task = _task(authority, task_text)
    else:
        task = self._current_task(identity, task_text)
    decode_code = None
    try:
        comparison_task = task
        if old is not None:
            # This context is ONLY a validated comparison value, never live authority.
            # The original text/role commitment was already checked against old history.
            raw_context = context_json(json.loads(payload)["result"]["context"])
            require(context_identity(raw_context) == identity)
            comparison_task = WorkerTask(context=raw_context, role=task.role, task_text=task.task_text,
                input_artifacts=(), tool_capabilities=(), output_byte_limit=CONTENT_CAP)
        decoded = self._validate_decoded(comparison_task, decode_worker_output(payload, comparison_task))
    except WorkerOutputError as error:
        decode_code = ("candidate_publication_too_large" if type(error) is WorkerOutputError and
                       vars(error).get("code") == "worker_output_too_large" else "candidate_publication_invalid")
    except CandidatePublicationError as error:
        decode_code = _decision_code(error)
    except Exception:
        decode_code = "candidate_publication_invalid"
    if decode_code is not None:
        raise CandidatePublicationError(decode_code)
    if old is not None:
        _same_request(old, _decoded_identity(decoded))
        if old.committed is not None:
            return CandidatePublicationReceipt(old.request_id, old.set_id)
    prepared = self._prepare(identity, task_text, decoded)
    if prepared.committed is not None:
        return CandidatePublicationReceipt(prepared.request_id, prepared.set_id)
    install_code = "candidate_publication_uncertain"
    try:
        with self._artifacts._candidate_installation_session() as operation:
            installed = tuple(self._artifacts._stage_candidate_blob(operation=operation,
                run_id=identity["run_id"], request_id=prepared.request_id, set_id=prepared.set_id,
                digest=blob.artifact.digest, content=blob.content) for blob in decoded.blobs)
            return self._finalize(prepared, operation, installed, task_text)
    except CandidatePublicationError as error:
        install_code = _decision_code(error)
    except Exception:
        pass
    raise CandidatePublicationError(install_code)

def status(self, *, run_id, node_id, attempt_id, fencing_generation):
    code = "candidate_publication_unavailable"
    invalid = False
    try:
        identity = identity_json(dict(run_id=run_id, node_id=node_id, attempt_id=attempt_id,
                                     fencing_generation=fencing_generation))
    except Exception:
        invalid = True
    if invalid:
        raise CandidatePublicationError("candidate_publication_invalid")
    try:
        found = self._journal.lookup(**identity)
        if found is None:
            return CandidatePublicationStatus("absent", None, None)
        return CandidatePublicationStatus("prepared" if found.committed is None else "committed",
                                          found.request_id, found.set_id)
    except Exception as error:
        code = _decision_code(error)
    raise CandidatePublicationError(code)
```

The public publish wrapper already specified earlier is the **only exposed** exception boundary: every internal `from None` above is subsequently discarded, and its fresh final owned error is raised after leaving that wrapper's handlers. The installation-session handler uses uncertain for unexpected errors after durable preparation; it never deletes a final file. Wrong text/manifest mismatch maps through `_task` to invalid, including historical retry. An exact pending retry reconstructs the old committed input identity before comparing it, but rechecks live authority inside `_prepare` before any install; a committed retry returns before that live branch.

Store-side installation registry is concrete, private and process-local. Add `_candidate_operations: dict[object, dict[str, _CandidateInstallation]] = {}` in ArtifactStore.__init__. No session object may come from the API. Imports `contextmanager`, `dataclass` already exist or are added. `_install_candidate_bytes(record, content)` is exactly the retained single-digest filesystem core in Task 3 Step 4, with **no lock inside**: the caller below owns it. It returns None only after successful file verification and directory fsync, including reuse.

```python
@dataclass(frozen=True, slots=True)
class _CandidateInstallation:
    store: object
    operation: object
    request_id: str
    set_id: str
    digest: str
    size: int

@contextmanager
def _candidate_installation_session(self):
    self._require_no_transaction()
    operation = object()
    self._candidate_operations[operation] = {}
    try:
        yield operation
    finally:
        self._candidate_operations.pop(operation, None)

def _stage_candidate_blob(self, *, operation, run_id, request_id, set_id, digest, content):
    self._require_no_transaction()
    require(operation in self._candidate_operations and type(content) is bytes and
            digest == "sha256:" + hashlib.sha256(content).hexdigest())
    with self._digest_lock(digest):
        journal = CandidatePublicationJournal(self._event_store)
        record = journal._require_intent(run_id=run_id, request_id=request_id, set_id=set_id, digest=digest)
        require(not self._event_store.in_transaction and record.size == len(content))
        self._install_candidate_bytes(record, content)
        issued = _CandidateInstallation(self, operation, request_id, set_id, digest, len(content))
        self._candidate_operations[operation][digest] = issued
        return issued

def _validate_candidate_installations(self, operation, results, prepared):
    require(self._event_store._has_checked_write_owner() and type(results) is tuple and
            operation in self._candidate_operations and len(results) == len(prepared.members))
    issued = self._candidate_operations[operation]
    require(len({r.digest for r in results if type(r) is _CandidateInstallation}) == len(results))
    expected = {r.digest: r.size_bytes for r in prepared.members}
    require(set(issued) == set(expected))
    for result in results:
        require(type(result) is _CandidateInstallation and issued.get(result.digest) is result and
            result.store is self and result.operation is operation and
            result.request_id == prepared.request_id and result.set_id == prepared.set_id and
            result.digest in expected and result.size == expected[result.digest])
```

Before `_append_candidate_record`'s existing append, execute the following. This check is independent of partial journal linkage. Generic metadata wrappers reject reserved(record.to_event_payload()) at entry, including source-marker or member-ID alone.

```python
require(type(record) is ArtifactRecord)
context = exact_model(AttemptContext, context)
member = dict(digest=record.digest, size_bytes=record.size,
              artifact_type=record.artifact_type, media_type=record.media_type)
sorted_members([member])
require(canonical_bytes(record.to_event_payload()) == canonical_bytes(
        record_payload(context, set_id, member, record.created_at)))

def _record_candidate_intent(self, record, context, set_id):
    return self._append_candidate_record("ArtifactPublicationIntent", record, context, set_id)

def _record_candidate_metadata(self, record, context, set_id):
    return self._append_candidate_record("ArtifactPublished", record, context, set_id)
```

### Revision R3-A: executable corruption and consumer matrix (Task 2)

In `tests/support/candidate_publication.py`, add `candidate_proposal(case, members)` and `candidate_usage(case)` below. These are explicit synthetic test evidence and usage, not production attestation. Unit gate tests may create them without executing a verifier; the committed positive acceptance integration test below invokes the real isolated verifier independently. Imports are `VerificationTask, VerificationEvidence, VerificationCheck, BUILTIN_VERIFIER_ID`, `VerifierProposalJournal`, and `UsageRecord`.

```python
def candidate_proposal(case, members):
    task = VerificationTask(context=case.task.context, candidate_artifacts=members,
        required_check_ids=("artifact-integrity",), acceptance_contract="maestro.artifact-verification/v1")
    digests = tuple(sorted(r.digest for r in members))
    evidence = VerificationEvidence(context=task.context, verifier_id=BUILTIN_VERIFIER_ID,
        outcome="accepted", inspected_digests=digests,
        checks=(VerificationCheck(check_id="artifact-integrity", passed=True, evidence_digests=digests),))
    return VerifierProposalJournal(case.store).record(task, evidence)

def candidate_usage(case):
    reservation = case.app._scheduler._ledger_for_run(case.identity["run_id"]).get_reservation(
        case.accepted.reservation.reservation_id, run_id=case.identity["run_id"])
    return UsageRecord(run_id=case.identity["run_id"], reservation_id=reservation.reservation_id,
        settlement_key="candidate-fixture-usage", currency=reservation.currency,
        cost_minor=0, status="committed")

def public_consumer(case, name, members, proposal, grant):
    from orchestrator.runtime.artifact_admission import admit_candidate_artifacts
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from orchestrator.runtime.acceptance_coordinator import AttemptExecutionCoordinator
    from orchestrator.runtime.verification_journal import VerifierProposalJournal
    from orchestrator.artifacts import EphemeralArtifactGrantAuthority
    from orchestrator.runtime.contracts import WorkerResult
    context = case.task.context
    digest = members[0].digest
    journal = CandidatePublicationJournal(case.store)
    if name == "admission":
        return admit_candidate_artifacts(case.task, WorkerResult(context=context,
            result_id="result-1", outcome="candidate", artifacts=members), case.artifacts)
    if name == "verifier":
        return IsolatedVerifierProcess().verify(proposal.task, case.artifacts, grant_for_digest=grant)
    if name == "coordinator":
        class Usage:
            def usage_for(self, snapshot):
                return candidate_usage(case)
        authority = EphemeralArtifactGrantAuthority()
        coordinator = AttemptExecutionCoordinator(scheduler=case.app._scheduler,
            artifact_store=case.artifacts, verifier=IsolatedVerifierProcess(),
            journal=VerifierProposalJournal(case.store), grants=authority,
            usage_source=Usage(), clock=lambda: case.clock.now)
        return coordinator.accept_proposal(**case.identity, task_sha256=proposal.task_sha256)
    if name == "terminal":
        return case.app._scheduler.finish_verified_attempt(**case.identity,
            task_sha256=proposal.task_sha256, usage=candidate_usage(case), completed_at=case.clock.now)
    if name == "historical":
        return case.app._scheduler.validate_success_proofs()
    if name == "proposal_audit":
        return case.app._scheduler.recovery._checked(None, "application_bootstrap", "global",
            "fixture-proposal-audit", lambda events, version:
                case.app._validate_verifier_proposals(case.app._recover_all_runs()))
    if name == "startup":
        reopened = ControlPlaneApplication(case.database, artifact_root=case.root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
        reopened.close()
        return None
    if name == "read_bytes":
        return case.artifacts.read_bytes(digest, grant=grant(digest))
    if name == "get_record":
        return case.artifacts.get_record(digest, grant=grant(digest))
    if name == "list_publications":
        return case.artifacts.list_publications(digest, grant=grant(digest))
    if name == "get_records":
        return case.artifacts.get_records(digest, grant=grant(digest))
    if name == "candidate_records":
        return case.artifacts.candidate_records(context, members)
    if name == "inventory":
        return case.artifacts.verify_run_artifacts(context.run_id)
    if name == "pending_inventory":
        return case.artifacts.pending_publications_for_run(context.run_id)
    if name == "lookup":
        return journal.lookup(**case.identity)
    if name == "require_set":
        return journal.require_set(context, members)
    if name == "publication_view":
        return journal.publication_view(digest)
    raise AssertionError(name)
```

For unit pre-grant failure assertions monkeypatch `EphemeralArtifactGrantAuthority.issue` and the store's `_authorize` with counters that call the real method; direct-verifier `grant` appends to the same counter and issues a real one-use grant. Store public adapters issue the caller's input grant before invoking a read; assert `_authorize` (not that external caller-side issuance) was never called on corruption. Never replace verification with a Boolean grant authority.

In `test_candidate_set_gates.py`, parameterize exact-set consumers independently from record-only readers. Record-only APIs have no set argument: subset/superset cannot sensibly be passed to read_bytes, so their matrix varies durable link corruption instead. Pending inventory/status legitimately observes pending; it must not be required to reject valid pending history.

```python
SET_CONSUMERS = ("admission", "verifier", "coordinator", "terminal", "candidate_records", "require_set")
READ_CONSUMERS = ("read_bytes", "get_record", "list_publications", "get_records", "inventory",
                  "pending_inventory", "lookup", "publication_view", "proposal_audit", "startup")

@pytest.mark.parametrize("consumer", SET_CONSUMERS)
@pytest.mark.parametrize("shape", ["subset", "superset", "changed_metadata", "mixed", "pending", "legacy"])
def test_every_set_consumer_rejects_changed_set(tmp_path, monkeypatch, consumer, shape):
    from orchestrator.artifacts import EphemeralArtifactGrantAuthority
    case = candidate_case(tmp_path)
    try:
        saved = append_candidate_fixture(case, (b"first", b"second"), commit=shape != "pending")
        members = saved.members
        if shape in ("subset", "legacy"):
            members = members[:1]
        elif shape == "superset":
            members = members + (ArtifactRef(digest="sha256:" + "c" * 64, size_bytes=1,
                artifact_type="worker-candidate", media_type="application/octet-stream"),)
        elif shape == "changed_metadata":
            members = (members[0].model_copy(update={"size_bytes": 99}),) + members[1:]
        elif shape == "mixed":
            members = (members[0], ArtifactRef(digest="sha256:" + "d" * 64, size_bytes=1,
                artifact_type="worker-candidate", media_type="application/octet-stream"))
        if shape == "legacy":
            # A real older-looking generic publication cannot override the prep index.
            case.artifacts.publish_bytes(b"first", artifact_type="worker-candidate",
                media_type="application/octet-stream", readable_scope=("run-1",),
                source=dict(run_id="run-1", node_id=case.task.context.node_id,
                    attempt_id=case.task.context.attempt_id, fencing_generation="1",
                    agent_instance_id=case.task.context.agent_instance_id))
        proposal = candidate_proposal(case, members)
        before = event_counts(case.store)
        calls = []
        real_issue = EphemeralArtifactGrantAuthority.issue
        def issue(self, **kwargs):
            calls.append("grant")
            return real_issue(self, **kwargs)
        monkeypatch.setattr(EphemeralArtifactGrantAuthority, "issue", issue)
        authority = EphemeralArtifactGrantAuthority()
        grant = lambda digest: authority.issue(digest=digest, run_id="run-1",
                                              expires_at=case.accepted.lease_expires_at)
        expected_error = {"admission": CandidateArtifactError, "verifier": VerifierProcessError,
            "coordinator": ProposalAcceptanceError, "terminal": SchedulerError,
            "candidate_records": ArtifactMetadataError, "require_set": CandidateJournalError}[consumer]
        with pytest.raises(expected_error):
            public_consumer(case, consumer, members, proposal, grant)
        assert calls == []
        assert event_counts(case.store) == before
        assert_held(case)
    finally:
        case.app.close()
```

Import the six named consumer exception types from their respective modules. Do not accept AssertionError, AttributeError, ImportError or TypeError as a security RED/GREEN result. `candidate_records` uses its detached ArtifactMetadataError boundary below; require_set always translates malformed values to CandidateJournalError.

The exhaustive schema mutation table below is executable rather than a field checklist. Put it in `test_candidate_journal.py`. `leaf_paths` includes each container as a deletion target as well as every scalar; deleting a complete context/head/record and deleting any one of its fields are separate cases. `rewrite_event` from the earlier addendum rehashes the mutated payload so envelope hash failures do not hide the schema test.

```python
def leaf_paths(value, path=()):
    yield path
    if isinstance(value, dict):
        for key, child in value.items():
            yield from leaf_paths(child, path + (key,))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from leaf_paths(child, path + (index,))

def removed(value, path):
    result = json.loads(json.dumps(value))
    if not path:
        return {}
    parent = result
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    return result

@pytest.mark.parametrize("phase", ["prepare", "commit"])
def test_every_candidate_payload_field_is_bound(tmp_path, phase):
    case = candidate_case(tmp_path)
    try:
        saved = append_candidate_fixture(case, (b"a", b"b"), commit=True)
        event = case.store.read_stream("candidate_publications", "run-1")[phase == "commit"]
        original = plain(event.payload)
        for path in tuple(leaf_paths(original)):
            rewrite_event(case.store, event.event_id, payload=removed(original, path))
            with pytest.raises(CandidateJournalError):
                CandidatePublicationJournal(case.store).lookup(**case.identity)
            rewrite_event(case.store, event.event_id, payload=original)
        assert CandidatePublicationJournal(case.store).lookup(**case.identity) == saved
    finally:
        case.app.close()

@pytest.mark.parametrize("header", ["run_id", "node_id", "attempt_id", "fencing_generation",
                                   "correlation_id", "causation_id", "idempotency_key", "schema_version"])
@pytest.mark.parametrize("kind", ["CandidatePublicationPrepared", "CandidatePublicationCommitted",
                                 "ArtifactPublicationIntent", "ArtifactPublished"])
def test_all_candidate_event_headers_are_bound(tmp_path, header, kind):
    case = candidate_case(tmp_path)
    try:
        saved = append_candidate_fixture(case, (b"a", b"b"), commit=True)
        streams = case.store.read_stream("candidate_publications", "run-1")
        streams += [e for m in saved.members for e in case.store.read_stream("artifact", m.digest)]
        event = next(e for e in streams if e.event_type == kind and
                     (kind.startswith("Candidate") or reserved(e.payload)))
        value = 999 if header in ("schema_version", "fencing_generation") else "wrong"
        rewrite_event(case.store, event.event_id, headers={header: value})
        with pytest.raises(CandidateJournalError):
            CandidatePublicationJournal(case.store).lookup(**case.identity)
    finally:
        case.app.close()
```

Add the same field-removal loop for each reserved ArtifactPublicationIntent/ArtifactPublished raw payload, restoring it between cases; this runs every source/record field through the actual journal rather than only validate_prepared. Corrupt each of five referenced source stream head payload hashes without recomputation, and also rewrite a validly rehashed source event's decision/lease/policy field: the former must hit event integrity, the latter the historical join/head hash mismatch.

For link/ordering corruption use this actual transaction injector in test support. It disables only triggers on this **test database's events table**, saves/restores their exact definitions and commits the corruption, then every consumer enters through a fresh object. Raw SQL is never used by the product to bypass immutable events.

```python
def corrupt_events(store, action):
    connection = store._connection
    triggers = connection.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='events'").fetchall()
    connection.execute("BEGIN IMMEDIATE")
    try:
        for name, sql in triggers:
            connection.execute('DROP TRIGGER "' + name.replace('"', '""') + '"')
        action(connection)
        for name, sql in triggers:
            connection.execute(sql)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise

def corrupt_link(case, saved, mutation):
    store = case.store
    journal = store.read_stream("candidate_publications", "run-1")
    artifact = [e for e in store.read_stream("artifact", saved.members[0].digest) if reserved(e.payload)]
    target = {"missing_intent": artifact[0], "missing_publication": artifact[1],
              "missing_marker": journal[1], "orphan": journal[0]}.get(mutation)
    if target is not None:
        corrupt_events(store, lambda c: c.execute("DELETE FROM events WHERE event_id=?", (target.event_id,)))
    elif mutation in ("duplicate_prepare", "duplicate_commit", "duplicate_intent", "duplicate_publication"):
        event = {"duplicate_prepare": journal[0], "duplicate_commit": journal[1],
                 "duplicate_intent": artifact[0], "duplicate_publication": artifact[1]}[mutation]
        def duplicate(c):
            row = dict(c.execute("SELECT * FROM events WHERE event_id=?", (event.event_id,)).fetchone())
            row["event_id"] = "duplicate-fixture"
            row["idempotency_key"] = "duplicate-fixture"
            row["stream_version"] = store.current_version(event.stream_type, event.stream_id) + 1
            names = tuple(row)
            c.execute("INSERT INTO events (" + ",".join(names) + ") VALUES (" + ",".join("?" for _ in names) + ")",
                      tuple(row[n] for n in names))
            c.execute("UPDATE stream_versions SET current_version=? WHERE stream_type=? AND stream_id=?",
                      (row["stream_version"], event.stream_type, event.stream_id))
        corrupt_events(store, duplicate)
    elif mutation == "wrong_artifact_stream":
        def misplaced(c):
            row = dict(c.execute("SELECT * FROM events WHERE event_id=?", (artifact[0].event_id,)).fetchone())
            row["event_id"] = "misplaced-fixture"
            row["stream_id"] = "sha256:" + "f" * 64
            row["stream_version"] = 1
            c.execute("INSERT INTO stream_versions VALUES (?, ?, ?)", ("artifact", row["stream_id"], 1))
            names = tuple(row)
            c.execute("INSERT INTO events (" + ",".join(names) + ") VALUES (" + ",".join("?" for _ in names) + ")",
                      tuple(row[n] for n in names))
        corrupt_events(store, misplaced)
    elif mutation == "commit_before_prepare":
        def reverse(c):
            c.execute("UPDATE events SET stream_version=99 WHERE event_id=?", (journal[0].event_id,))
            c.execute("UPDATE events SET stream_version=1 WHERE event_id=?", (journal[1].event_id,))
            c.execute("UPDATE events SET stream_version=2 WHERE event_id=?", (journal[0].event_id,))
        corrupt_events(store, reverse)
    elif mutation == "stripped_marker":
        raw = plain(artifact[1].payload)
        raw["source"].pop("publication_protocol")
        rewrite_event(store, artifact[1].event_id, payload=raw)
    else:
        raise AssertionError(mutation)

LINK_MUTATIONS = ("missing_intent", "missing_publication", "missing_marker", "orphan",
    "duplicate_prepare", "duplicate_commit", "duplicate_intent", "duplicate_publication",
    "commit_before_prepare", "stripped_marker", "wrong_artifact_stream")

@pytest.mark.parametrize("mutation", LINK_MUTATIONS)
@pytest.mark.parametrize("consumer", READ_CONSUMERS + SET_CONSUMERS + ("historical",))
def test_corrupt_links_fail_every_consumer(tmp_path, monkeypatch, mutation, consumer):
    case = candidate_case(tmp_path)
    try:
        saved = append_candidate_fixture(case, (b"a", b"b"), commit=True)
        proposal = candidate_proposal(case, saved.members)
        if consumer == "historical":
            case.app._scheduler.finish_verified_attempt(**case.identity,
                task_sha256=proposal.task_sha256, completed_at=case.clock.now, usage=candidate_usage(case))
        corrupt_link(case, saved, mutation)
        before = event_counts(case.store)
        authority = EphemeralArtifactGrantAuthority()
        grant = lambda digest: authority.issue(digest=digest, run_id="run-1",
            expires_at=case.accepted.lease_expires_at)
        authorized = []
        real = case.artifacts._authorize
        def authorize(*a, **kw):
            authorized.append(True)
            return real(*a, **kw)
        monkeypatch.setattr(case.artifacts, "_authorize", authorize)
        with pytest.raises((CandidateJournalError, ArtifactMetadataError, ArtifactIntegrityError,
                            CandidateArtifactError, VerifierProcessError, ProposalAcceptanceError,
                            SchedulerError, StartupRecoveryFailed)):
            public_consumer(case, consumer, saved.members, proposal, grant)
        assert authorized == []
        assert event_counts(case.store) == before
    finally:
        case.app.close()
```

Import `StartupRecoveryFailed` from application.py. Do not catch a generic Exception in the final executable negative matrix. For deletion cases, malformed stream sequence may reject before candidate cross-link code; dedicated rehashed-field and nonmissing-row cases still reach the candidate-specific path. Every mutation has a positive unmodified control first, so an always-failing fixture cannot pass the matrix.

### Revision R2-B: strict schema and replay bodies (Task 2)

The following are concrete bodies in `candidate_schema.py`, in addition to the canonical/time/accounting helpers above. Imports: `hashlib`, `json`, `re`, datetime types, `AttemptContext`, `ArtifactRef`, `WorkerTask`, `WorkerResult`, `VerificationTask`, and `ToolCapabilityRef` from runtime.contracts. Runtime contracts already load verifier_process lazily for its own contract imports; keep candidate imports in verifier/admission entry functions local. No top-level journal import is added to contracts or events.

```python
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}")
_PREP_KEYS = frozenset(("schema", "request_id", "set_id", "context", "result_id_hash",
    "policy_id", "output_byte_limit", "reservation_id", "accepted_route_id", "accepted_at",
    "lease_expires_at", "prepared_at", "authority_heads", "members", "records"))
_COMMIT_KEYS = frozenset(("schema", "request_id", "set_id", "prepared_hash",
                        "authority_heads", "committed_at"))

def require(condition):
    if not condition:
        raise ValueError("candidate invariant invalid")

def plain(value):
    # Only these two event-store-owned wrappers may cross the JSON boundary.
    from orchestrator.persistence.events import _FrozenDict, _FrozenList
    if type(value) in (dict, _FrozenDict):
        require(all(type(k) is str for k in value))
        return {k: plain(v) for k, v in value.items()}
    if type(value) in (list, _FrozenList):
        return [plain(v) for v in value]
    require(value is None or type(value) in (str, bool, int, float))
    return value

def exact_model(cls, value):
    require(type(value) is cls)
    fields = cls.model_fields
    extras = value.__pydantic_extra__
    require(set(vars(value)) == set(fields))
    require(extras is None or (type(extras) is dict and len(extras) == 0))
    nested = {"context": AttemptContext}
    arrays = {"artifacts": ArtifactRef, "candidate_artifacts": ArtifactRef,
              "input_artifacts": ArtifactRef, "tool_capabilities": ToolCapabilityRef}
    copied = {}
    for key in fields:
        item = vars(value)[key]
        if key in nested:
            item = exact_model(nested[key], item)
        elif key in arrays:
            require(type(item) is tuple)
            item = tuple(exact_model(arrays[key], x) for x in item)
        elif key == "required_check_ids":
            require(type(item) is tuple and all(type(x) is str for x in item))
        else:
            require(item is None or type(item) in (str, int, bool))
        copied[key] = item
    return cls.model_validate(copied)

def context_json(value):
    value = plain(value)
    require(type(value) is dict and set(value) == set(AttemptContext.model_fields))
    return AttemptContext.model_validate(value)

def identity_json(value):
    require(type(value) is dict and set(value) == {
        "run_id", "node_id", "attempt_id", "fencing_generation"})
    for key in ("run_id", "node_id", "attempt_id"):
        require(type(value[key]) is str and _IDENTIFIER.fullmatch(value[key]) is not None)
    require(type(value["fencing_generation"]) is int and value["fencing_generation"] > 0)
    return dict(value)

def context_identity(context):
    return {k: getattr(context, k) for k in
            ("run_id", "node_id", "attempt_id", "fencing_generation")}

def sorted_members(value):
    require(type(value) is list and 1 <= len(value) <= MEMBER_CAP)
    members = []
    for raw in value:
        require(type(raw) is dict and set(raw) == set(ArtifactRef.model_fields))
        ref = ArtifactRef.model_validate(raw)
        require(ref.artifact_type == "worker-candidate" and
                ref.media_type == "application/octet-stream")
        members.append(ref.model_dump(mode="json"))
    require(members == sorted(members, key=lambda x: x["digest"]))
    require(len({x["digest"] for x in members}) == len(members))
    require(sum(x["size_bytes"] for x in members) <= CONTENT_CAP)
    return members

def head_keys(run_id):
    return tuple(sorted((("run", run_id), ("run_lifecycle", run_id),
        ("agent_registry", run_id), ("budget", run_id), ("scheduler", "global"))))

def validated_heads(value, run_id):
    require(type(value) is list and len(value) == 5)
    for head, key in zip(value, head_keys(run_id)):
        require(type(head) is dict and set(head) == {
            "stream_type", "stream_id", "version", "event_id", "payload_hash"})
        require((head["stream_type"], head["stream_id"]) == key)
        require(type(head["version"]) is int and head["version"] > 0)
        require(type(head["event_id"]) is str and 1 <= len(head["event_id"]) <= 128)
        require(type(head["payload_hash"]) is str and
                re.fullmatch(r"[0-9a-f]{64}", head["payload_hash"]) is not None)
    return value

def record_payload(context, set_id, member, prepared_at):
    return dict(publication_id="candidate-v1:" + candidate_hash(
        "maestro.candidate-member.v1", dict(set_id=set_id, digest=member["digest"]))[7:],
        digest=member["digest"], type="worker-candidate", size=member["size_bytes"],
        media_type="application/octet-stream", schema_version=1, redaction_state="unknown",
        readable_scope=[context.run_id], references=[], lifecycle_state="temporary",
        created_at=utc_text(prepared_at), source=dict(
            run_id=context.run_id, node_id=context.node_id, attempt_id=context.attempt_id,
            fencing_generation=str(context.fencing_generation),
            agent_instance_id=context.agent_instance_id, publication_protocol=SCHEMA,
            candidate_set_id=set_id))

def set_identity(context, result_id_hash, members):
    return dict(request_id=candidate_hash("maestro.candidate-request.v1", context_identity(context)),
        context=context.model_dump(mode="json"), result_id_hash=result_id_hash,
        policy_id=POLICY, output_byte_limit=CONTENT_CAP, members=members)

def validate_prepared(payload):
    p = plain(payload)
    require(type(p) is dict and set(p) == _PREP_KEYS)
    require(len(canonical_bytes(p)) <= EVENT_CAP)
    context = context_json(p["context"])
    require(p["schema"] == SCHEMA and p["policy_id"] == POLICY)
    require(type(p["output_byte_limit"]) is int and p["output_byte_limit"] == CONTENT_CAP)
    for key in ("request_id", "set_id", "result_id_hash"):
        require(type(p[key]) is str and _DIGEST.fullmatch(p[key]) is not None)
    for key in ("reservation_id", "accepted_route_id"):
        require(type(p[key]) is str and _IDENTIFIER.fullmatch(p[key]) is not None)
    accepted, prepared, expires = (parse_utc_text(p[k]) for k in
                                  ("accepted_at", "prepared_at", "lease_expires_at"))
    require(accepted <= prepared < expires)
    validated_heads(p["authority_heads"], context.run_id)
    members = sorted_members(p["members"])
    identity = set_identity(context, p["result_id_hash"], members)
    require(p["request_id"] == identity["request_id"])
    require(p["set_id"] == candidate_hash("maestro.candidate-set.v1", identity))
    records = [record_payload(context, p["set_id"], m, prepared) for m in members]
    require(canonical_bytes(p["records"]) == canonical_bytes(records))
    return p

def validate_committed(payload, prepared):
    p, c = validate_prepared(prepared), plain(payload)
    require(type(c) is dict and set(c) == _COMMIT_KEYS)
    require(len(canonical_bytes(c)) <= COMMIT_RESERVE)
    require(c["schema"] == SCHEMA and c["request_id"] == p["request_id"] and
            c["set_id"] == p["set_id"] and c["prepared_hash"] ==
            candidate_hash("maestro.candidate-prepared.v1", p))
    at = parse_utc_text(c["committed_at"])
    require(parse_utc_text(p["prepared_at"]) <= at < parse_utc_text(p["lease_expires_at"]))
    validated_heads(c["authority_heads"], p["context"]["run_id"])
    require(all(b["version"] >= a["version"] for a, b in
                zip(p["authority_heads"], c["authority_heads"])))
    return c

def candidate_headers(event, context, set_id, key, stream_type, stream_id):
    require(event.stream_type == stream_type and event.stream_id == stream_id and
        event.schema_version == 1 and event.idempotency_key == key and
        event.run_id == context.run_id and event.node_id == context.node_id and
        event.attempt_id == context.attempt_id and
        type(event.fencing_generation) is int and
        event.fencing_generation == context.fencing_generation and
        event.correlation_id == context.run_id and event.causation_id == set_id)

def replay_candidate_events(events):
    prepared, committed, used = {}, {}, 0
    for position, event in enumerate(events, 1):
        require(event.stream_version == position)
        payload = plain(event.payload)
        size = len(canonical_bytes(payload))
        require(size <= EVENT_CAP)
        if event.event_type == "CandidatePublicationPrepared":
            p = validate_prepared(payload)
            key = p["request_id"]
            require(key not in prepared and len(prepared) < REQUEST_CAP)
            used = account_candidate_bytes(used, preparation_size=size)
            prepared[key] = p
            prefix = "candidate-prepare:"
        else:
            require(event.event_type == "CandidatePublicationCommitted")
            key = payload.get("request_id")
            require(type(key) is str and key in prepared and key not in committed)
            p = prepared[key]
            committed[key] = validate_committed(payload, p)
            used = account_candidate_bytes(used, committed_size=size)
            prefix = "candidate-commit:"
        context = context_json(p["context"])
        candidate_headers(event, context, p["set_id"], prefix + key[7:],
                          "candidate_publications", context.run_id)
    return prepared, committed, used

def validate_candidate_events(events):
    replay_candidate_events(events)
```

In events.validate_event_contract, candidate dispatch uses the **whole single stream**, then continues existing generic header validation; wrapping this failure as EventContractError uses fixed text. Mixed candidate event names on other stream types reject. `plain` is the sole sanctioned event wrapper conversion; primitive wrong types survive to strict schema validation instead of being stringified/coerced. Model reconstruction uses `vars` only after exact class checking; nested subclasses/extras are rejected before any model serializer is called.

### Revision R2-C: five-prefix historical authority (Task 2)

In `candidate_authority.py`, import `config_snapshot_from_events, reduce_lifecycle` from lifecycle.controller, `reduce_agent_registry` from agents.registry, `replay_budget_events` from budget.ledger, and `_run_limit, reduce_scheduler_attempts` from recovery.run. Import the schema helpers above. This is prefix reduction, not a fake live store or recursive recovery call.

```python
@dataclass(frozen=True, slots=True)
class CandidateAuthority:
    context: AttemptContext
    role: str
    reservation_id: str
    accepted_route_id: str
    accepted_at: datetime
    lease_expires_at: datetime

def capture_authority_heads(store, run_id):
    require(store._has_checked_write_owner())
    heads = []
    for kind, name in head_keys(run_id):
        events = store.validated_stream(kind, name)
        require(bool(events))
        last = events[-1]
        heads.append(dict(stream_type=kind, stream_id=name, version=last.stream_version,
                          event_id=last.event_id, payload_hash=last.payload_hash))
    validated_heads(heads, run_id)
    return tuple(heads)

def validate_historical_authority(store, context, heads, at):
    require(store.in_transaction)
    context = exact_model(AttemptContext, context)
    utc_text(at)
    heads = validated_heads([plain(h) for h in heads], context.run_id)
    prefixes = {}
    for head in heads:
        key = (head["stream_type"], head["stream_id"])
        events = store.validated_stream(*key)
        require(len(events) >= head["version"])
        prefix = events[:head["version"]]
        last = prefix[-1]
        require(last.event_id == head["event_id"] and last.payload_hash == head["payload_hash"])
        validate_event_contract(list(prefix))
        prefixes[key[0]] = prefix
    run = context.run_id
    config = config_snapshot_from_events(run, prefixes["run"])
    lifecycle = reduce_lifecycle(run, list(prefixes["run_lifecycle"]))
    agents = reduce_agent_registry(run, list(prefixes["agent_registry"]))
    balance, reservations = replay_budget_events(run, prefixes["budget"], _run_limit(config))
    accepted, active = reduce_scheduler_attempts(run, prefixes["scheduler"])
    key = (context.node_id, context.attempt_id)
    require(key in accepted and lifecycle.status not in ("cancelling", "cancelled", "failed", "succeeded"))
    record = accepted[key]
    route = record["route"]
    node = lifecycle.node(context.node_id)
    attempt = next(a for a in node.attempts if a.attempt_id == context.attempt_id)
    agent = agents.for_attempt(context.attempt_id)
    reservation = next(r for r in reservations if r.reservation_id == attempt.reservation_id)
    events = [e for e in prefixes["scheduler"] if e.event_type == "RoutingDecisionAccepted"
              and e.run_id == run and e.node_id == context.node_id
              and e.attempt_id == context.attempt_id]
    require(len(events) == 1)
    event, contract = events[0], node.spec.planning_contract
    p = plain(event.payload)
    require(contract is not None and record["status"] == "active" and
            record["attempt_ref"] in active and attempt.status == "accepted" and agent.status == "active")
    require(event.fencing_generation == context.fencing_generation and
        event.correlation_id == run and event.causation_id == context.routing_decision_hash and
        p["run_id"] == run and p["node_id"] == context.node_id and
        p["attempt_id"] == context.attempt_id and p["decision_hash"] == context.routing_decision_hash)
    require(record["agent_instance_id"] == attempt.agent_instance_id == agent.agent_instance_id ==
        context.agent_instance_id == p["agent_instance_id"] and
        record["fencing_generation"] == attempt.fencing_generation == agent.fencing_generation ==
        route.fencing_generation == context.fencing_generation and
        record["decision_hash"] == attempt.decision_hash == agent.decision_hash == context.routing_decision_hash)
    require(route.run_id == agent.run_id == run and
        route.node_id == agent.node_id == context.node_id and route.attempt_id == context.attempt_id and
        route.model_id == attempt.model_id == agent.model_id and
        route.provider_id == attempt.provider_id == agent.provider_id == p["provider_id"] and
        route.reasoning_effort == attempt.reasoning_effort == agent.reasoning_effort and
        route.registry_manifest_hash == lifecycle.registry_hash == config.registry_manifest_hash == context.registry_hash)
    require(route.decision_id == "decision-" + context.routing_decision_hash[7:] and
        record["reservation_id"] == p["reservation_id"] == route.budget_reservation_id ==
        attempt.reservation_id == reservation.reservation_id)
    require(reservation.status == "reserved" and reservation.run_id == run and
        reservation.node_id == context.node_id and reservation.attempt_id == context.attempt_id and
        reservation.fencing_generation == context.fencing_generation and
        reservation.correlation_id == run and reservation.causation_id == context.routing_decision_hash)
    require(lifecycle.workspace_identity_hash == config.workspace_identity_hash and
        lifecycle.config_hash == config.effective_config_hash == context.effective_config_hash and
        type(p["graph_version"]) is int and p["graph_version"] == lifecycle.graph_version == context.graph_version)
    require(contract.run_id == run and contract.node_id == context.node_id and
        contract.role == node.spec.role and contract.config_hash == context.effective_config_hash and
        contract.registry_hash == context.registry_hash and
        contract.policy_manifest_hash == lifecycle.policy_manifest_hash == attempt.policy_manifest_hash ==
        agent.policy_manifest_hash == context.policy_manifest_hash and
        contract.contract_hash == node.spec.planning_contract_hash == context.planning_contract_hash)
    admission = dict(input_manifest_hash=context.input_manifest_hash,
        verification_contract="maestro.artifact-verification/v1", required_check_ids=["artifact-integrity"])
    require(p.get("verification_admission") == admission and
        p.get("verification_contract") == admission["verification_contract"] and
        p.get("required_check_ids") == admission["required_check_ids"] and
        context_json(p.get("verification_context")) == context)
    accepted_at = datetime.fromisoformat(p["accepted_at"])
    expires_at = datetime.fromisoformat(p["lease_expires_at"])
    utc_text(accepted_at)
    utc_text(expires_at)
    require(datetime.fromisoformat(attempt.lease_expires_at) == expires_at and
        datetime.fromisoformat(record["lease"].lease_expires_at) == expires_at and
        accepted_at <= at < expires_at)
    return CandidateAuthority(context, node.spec.role, reservation.reservation_id,
                              route.decision_id, accepted_at, expires_at)
```

`validate_event_contract` is imported locally inside this function to avoid startup cycles. Existing live Scheduler checks remain authoritative and are not replaced with this historical reader. Add `current_candidate_authority(scheduler, identity: dict, at: datetime) -> CandidateAuthority` in this same module: it invokes the existing live binding (and thus scoped full recovery), captures current heads, then uses the **same** prefix validator for complete comparison. This provides shared candidate-specific role/planning/empty-profile rules while retaining all current Scheduler checks:

```python
def current_candidate_authority(scheduler, identity, at):
    binding = scheduler.resolve_verification_binding(**identity, as_of=at)
    require(binding.verification_contract == "maestro.artifact-verification/v1" and
            binding.required_check_ids == ("artifact-integrity",))
    result = validate_historical_authority(scheduler.event_store,
        exact_model(AttemptContext, binding.context),
        capture_authority_heads(scheduler.event_store, identity["run_id"]), at)
    active = binding.active_attempt
    require((result.reservation_id, result.accepted_route_id, result.accepted_at, result.lease_expires_at) ==
        (active.reservation.reservation_id, active.accepted_route.decision_id,
         active.accepted_at, active.lease_expires_at))
    return result
```

`task_from_authority` in WorkerInputService uses the existing manifest builder and exact context; imports `hmac`, `WorkerInputError`, frame/content constants are already present:

```python
def task_from_authority(authority, task_text):
    if type(task_text) is not str or not 1 <= len(task_text) <= 100_000 or not task_text.strip():
        raise WorkerInputError("worker_input_invalid")
    task_text.encode("utf-8", errors="strict")
    context = exact_model(AttemptContext, authority.context)
    manifest = build_worker_input_manifest(run_id=context.run_id, node_id=context.node_id,
                                         role=authority.role, task_text=task_text)
    if not hmac.compare_digest(manifest.content_hash, context.input_manifest_hash):
        raise WorkerInputError("worker_input_mismatch")
    task = WorkerTask(context=context, role=authority.role, task_text=task_text,
                     input_artifacts=(), tool_capabilities=(), output_byte_limit=16_384)
    if len(task.model_dump_json().encode("utf-8")) > MAX_WORKER_INPUT_FRAME_BYTES:
        raise WorkerInputError("worker_input_too_large")
    return task
```

### Revision R2-D: call-local journal, namespace links and actual-record selection (Task 2)

Add these helpers to candidate_journal.py. Import the named schema helpers, including an explicit `from .candidate_schema import _IDENTIFIER` (not a wildcard import), and `ArtifactStore`/`ArtifactRecord` locally inside record parsing methods; do not import ArtifactStore at module load. The private `_check_links` signature is refined to `_check_links(self, candidate, view: dict) -> None`; `_read_run_in_snapshot(self, run_id, view: dict | None = None)` accepts an optional **call-local** stream cache. No cache is stored on `self`, reused across public calls, or accepted from public callers.

```python
class CandidateJournalError(RuntimeError):
    pass

def reserved(payload):
    source = payload.get("source")
    return ((type(payload.get("publication_id")) is str and
             payload["publication_id"].startswith("candidate-v1:")) or
            (isinstance(source, dict) and
             ("publication_protocol" in source or "candidate_set_id" in source)))

def parsed_record(payload):
    from orchestrator.artifacts.store import ArtifactRecord
    p = plain(payload)
    return ArtifactRecord.from_metadata(p["digest"], size=p["size"], artifact_type=p["type"],
        media_type=p["media_type"], source=p["source"], schema_version=p["schema_version"],
        redaction_state=p["redaction_state"], readable_scope=tuple(p["readable_scope"]),
        references=tuple(p["references"]), lifecycle_state=p["lifecycle_state"],
        created_at=parse_utc_text(p["created_at"]), publication_id=p["publication_id"])

def _stream(self, view, kind, name):
    require(self._event_store.in_transaction)
    key = (kind, name)
    if key not in view:
        view[key] = self._event_store.validated_stream(kind, name)
    return view[key]

def _check_links(self, candidate, view):
    p, c, context = candidate.prepared, candidate.committed, candidate.context
    for raw in p["records"]:
        events = self._stream(view, "artifact", raw["digest"])
        matching = [e for e in events if e.payload.get("publication_id") == raw["publication_id"]]
        require([e.event_type for e in matching] ==
                (["ArtifactPublicationIntent"] if c is None else
                 ["ArtifactPublicationIntent", "ArtifactPublished"]))
        for event in matching:
            require(canonical_bytes(plain(event.payload)) == canonical_bytes(raw))
            prefix = "artifact-intent:" if event.event_type == "ArtifactPublicationIntent" else "artifact-publication:"
            candidate_headers(event, context, candidate.set_id, prefix + raw["publication_id"],
                              "artifact", raw["digest"])

def _read_run_in_snapshot(self, run_id, view=None):
    require(self._event_store.in_transaction)
    view = {} if view is None else view
    events = self._stream(view, "candidate_publications", run_id)
    preparations, commits, used = replay_candidate_events(events)
    result, record_index = [], {}
    for key, p in preparations.items():
        c = commits.get(key)
        context = context_json(p["context"])
        before = validate_historical_authority(self._event_store, context,
            tuple(p["authority_heads"]), parse_utc_text(p["prepared_at"]))
        require((before.reservation_id, before.accepted_route_id, utc_text(before.accepted_at),
                 utc_text(before.lease_expires_at)) ==
                (p["reservation_id"], p["accepted_route_id"], p["accepted_at"], p["lease_expires_at"]))
        if c is not None:
            after = validate_historical_authority(self._event_store, context,
                tuple(c["authority_heads"]), parse_utc_text(c["committed_at"]))
            require(before == after)
        candidate = CandidateSet(canonical_bytes(p), None if c is None else canonical_bytes(c))
        self._check_links(candidate, view)
        result.append(candidate)
        for record in p["records"]:
            require(record["publication_id"] not in record_index)
            record_index[record["publication_id"]] = record
    # Discovery remains inside the same snapshot, even when the Run has NO prep.
    for digest in self._event_store.stream_ids("artifact"):
        for event in self._stream(view, "artifact", digest):
            raw = plain(event.payload)
            if not reserved(raw):
                continue
            source = raw.get("source")
            require(type(source) is dict and source.get("publication_protocol") == SCHEMA and
                    type(source.get("run_id")) is str and _IDENTIFIER.fullmatch(source["run_id"]) is not None)
            if source["run_id"] != run_id:
                continue
            require(raw.get("publication_id") in record_index)
            require(canonical_bytes(raw) == canonical_bytes(record_index[raw["publication_id"]]))
            require(raw["digest"] == digest)
            require(event.event_type in ("ArtifactPublicationIntent", "ArtifactPublished"))
    return tuple(result)

def _safe_read(self, callback):
    failed = False
    try:
        with self._event_store.read_snapshot():
            return callback({})
    except Exception:
        failed = True
    if failed:
        raise CandidateJournalError("candidate journal invalid")

def read_run(self, run_id):
    def read(view):
        identity_json(dict(run_id=run_id, node_id="check", attempt_id="check", fencing_generation=1))
        return self._read_run_in_snapshot(run_id, view)
    return self._safe_read(read)

def lookup(self, *, run_id, node_id, attempt_id, fencing_generation):
    def read(view):
        identity = identity_json(dict(run_id=run_id, node_id=node_id, attempt_id=attempt_id,
                                      fencing_generation=fencing_generation))
        key = candidate_hash("maestro.candidate-request.v1", identity)
        return next((c for c in self._read_run_in_snapshot(run_id, view) if c.request_id == key), None)
    return self._safe_read(read)

def require_set(self, context, members):
    def read(view):
        checked = exact_model(AttemptContext, context)
        require(type(members) is tuple and 1 <= len(members) <= MEMBER_CAP)
        refs = tuple(exact_model(ArtifactRef, r) for r in members)
        require(len({r.digest for r in refs}) == len(refs))
        sets = self._read_run_in_snapshot(checked.run_id, view)
        key = candidate_hash("maestro.candidate-request.v1", context_identity(checked))
        candidate = next((c for c in sets if c.request_id == key), None)
        if candidate is not None:
            require(candidate.committed is not None and candidate.context == checked and
                candidate.members == tuple(sorted(refs, key=lambda r: r.digest)))
            return candidate
        # No prep found; reserved matching publication cannot become legacy.
        for ref in refs:
            for e in self._stream(view, "artifact", ref.digest):
                source = e.payload.get("source", {})
                if isinstance(source, dict) and all(source.get(k) == v for k, v in
                    dict(run_id=checked.run_id, node_id=checked.node_id, attempt_id=checked.attempt_id,
                         fencing_generation=str(checked.fencing_generation),
                         agent_instance_id=checked.agent_instance_id).items()):
                    require(not reserved(e.payload))
        return None
    return self._safe_read(read)

def publication_view(self, digest):
    def read(view):
        from orchestrator.artifacts.store import ArtifactStore
        events = self._stream(view, "artifact", digest)
        runs = set()
        for e in events:
            source = e.payload.get("source", {})
            if isinstance(source, dict) and type(source.get("run_id")) is str:
                runs.add(source["run_id"])
            elif reserved(e.payload):
                require(False)
        # The preparation index is independent of artifact source markers/IDs.
        # A stripped or missing artifact row must not hide a known parent set.
        for run in self._event_store.stream_ids("candidate_publications"):
            preparations, commits, used = replay_candidate_events(
                self._stream(view, "candidate_publications", run))
            if any(any(m["digest"] == digest for m in p["members"]) for p in preparations.values()):
                runs.add(run)
        for run in sorted(runs):
            self._read_run_in_snapshot(run, view)
        return tuple(ArtifactStore._record_from_event(e, digest)
                     for e in events if e.event_type == "ArtifactPublished")
    return self._safe_read(read)

def audit_all(self):
    def read(view):
        runs = set(self._event_store.stream_ids("candidate_publications"))
        for digest in self._event_store.stream_ids("artifact"):
            for event in self._stream(view, "artifact", digest):
                if reserved(event.payload):
                    source = event.payload.get("source")
                    require(isinstance(source, dict) and type(source.get("run_id")) is str)
                    runs.add(source["run_id"])
        for run in sorted(runs):
            identity_json(dict(run_id=run, node_id="check", attempt_id="check", fencing_generation=1))
            self._read_run_in_snapshot(run, view)
    self._safe_read(read)

def _require_intent(self, *, run_id, request_id, set_id, digest):
    def read(view):
        candidate = next(c for c in self._read_run_in_snapshot(run_id, view)
                         if c.request_id == request_id and c.set_id == set_id)
        return next(r for r in candidate.records if r.digest == digest)
    return self._safe_read(read)
```

Public parameter reconstruction is inside `_safe_read`'s callback, so malformed-input exceptions receive the same detached fixed journal error. Internal pure helpers keep ValueError for schema unit tests. CandidateSet properties are simple new parses, never cached authority:

```python
@dataclass(frozen=True, slots=True)
class CandidateSet:
    prepared_json: bytes
    committed_json: bytes | None
    @property
    def prepared(self):
        return json.loads(self.prepared_json)
    @property
    def committed(self):
        return None if self.committed_json is None else json.loads(self.committed_json)
    @property
    def context(self):
        return context_json(self.prepared["context"])
    @property
    def members(self):
        return tuple(ArtifactRef.model_validate(r) for r in self.prepared["members"])
    @property
    def records(self):
        return tuple(parsed_record(r) for r in self.prepared["records"])
    @property
    def request_id(self):
        return self.prepared["request_id"]
    @property
    def set_id(self):
        return self.prepared["set_id"]
```

Add this actual selection body to ArtifactStore. Legacy provenance selection is permitted only after require_set established the absence of a candidate preparation in this same view. `publication_view` parses the actual rows under that outer snapshot; the following files check runs only after closure, never authorizes through `_records`.

```python
def candidate_records(self, context, members):
    self._require_no_transaction()
    from orchestrator.artifacts.candidate_journal import CandidatePublicationJournal
    from orchestrator.artifacts.candidate_schema import exact_model, require
    from orchestrator.runtime.contracts import AttemptContext, ArtifactRef
    context = exact_model(AttemptContext, context)
    require(type(members) is tuple)
    members = tuple(exact_model(ArtifactRef, r) for r in members)
    journal = CandidatePublicationJournal(self._event_store)
    with self._event_store.read_snapshot():
        candidate = journal.require_set(context, members)
        if candidate is not None:
            selected = candidate.records
        else:
            source = dict(run_id=context.run_id, node_id=context.node_id, attempt_id=context.attempt_id,
                fencing_generation=str(context.fencing_generation), agent_instance_id=context.agent_instance_id)
            selected_list = []
            for ref in sorted(members, key=lambda r: r.digest):
                matches = tuple(r for r in journal.publication_view(ref.digest)
                                if all(r.source.get(k) == v for k, v in source.items()))
                require(bool(matches))
                require(all((r.size, r.artifact_type, r.media_type) ==
                            (ref.size_bytes, ref.artifact_type, ref.media_type) for r in matches))
                selected_list.append(matches[-1])
            selected = tuple(selected_list)
    for record in selected:
        with self._digest_lock(record.digest):
            if self._verify_file_and_size(self._path_for_digest(record.digest), record.digest) != record.size:
                raise ArtifactIntegrityError(record.digest, "candidate size mismatch")
    return selected
```

At each runtime consumer replace the old per-ref inventory selection with this body after exact reconstruction: `records = artifact_store.candidate_records(task.context, task.candidate_artifacts)` for verifier/VerificationTask, and `records = artifact_store.candidate_records(task.context, result.artifacts)` for admission/WorkerTask. In admission preserve `validate_worker_result` after rebuilding both exact task/result; translate any caught failure to fresh `CandidateArtifactError("candidate artifacts unavailable")` **after** the handler. In direct verifier keep existing grant/temporary-file/launch code after this line. Coordinator calls it for `proposal.task` before its verifier invocation. No per-ref loop is itself an authorization fallback.

In Scheduler terminal callback call `CandidatePublicationJournal(self.event_store).require_set(proposal.task.context, proposal.task.candidate_artifacts)` immediately after journal proposal load, before the prior-idempotent return. If a set is returned, reject `parse_utc_text(set.committed["committed_at"]) > completed_at`. In `_validate_success_proofs`, use the same gate after the existing historical proposal/task validation and additionally compare the candidate commit head versions against that loop's actual scheduler release event version and lifecycle terminal event version. In application `_validate_verifier_proposals`, call require_set after existing task/admission validation. All these consumers use local imports, preserve original independent success/admission checks and do not call candidate_records while holding SQLite. In `_bootstrap` call audit_all at callback entry before validate_success_proofs and Provider settlement; metadata capture also audits before observations. These are database-only additions; no byte grants or filesystem work is introduced into callbacks.

`config_snapshot_from_events` moves the existing config parser with full prefix validation; the existing method delegates after its validated read. Implement it in lifecycle/controller.py with existing `json`, `RunConfigSnapshot`, `LifecycleError` imports and `validate_event_contract` from persistence.events:

```python
def config_snapshot_from_events(run_id, events):
    if not events or events[0].event_type != "RunCreated":
        raise LifecycleError("RunConfigSnapshot is missing")
    if any(event.stream_type != "run" or event.stream_id != run_id or
           event.stream_version != i for i, event in enumerate(events, 1)):
        raise LifecycleError("Run config prefix is invalid")
    validate_event_contract(list(events))
    return RunConfigSnapshot.model_validate_json(json.dumps(events[0].payload["config_snapshot"]))
```

Snapshot ownership is independent of whether the transaction is read-only or write-owning; `capture_authority_heads` is used only inside `_checked`/append_checked writers. SQLite's `in_transaction` alone does not distinguish a raw BEGIN read transaction from BEGIN IMMEDIATE. Maintain private write-owner depth inside `SQLiteEventStore.append_checked` for its full callback duration, including nested savepoints, and expose `_has_checked_write_owner() -> bool` to candidate metadata/head helpers. A raw user BEGIN without that owner cannot call metadata-only finalization or manufacture a recovery scope. Tests open raw outer read/write transactions only for database reads; full candidate service entry rejects both.

Initialize `_checked_write_depth = 0` in both SQLiteEventStore construction paths (`__init__` and any alternate constructor using `__new__`). Replace the single `drafts = decide(events, current_version)` statement in append_checked with this exact fragment; add the method below to the class. All BEGIN/savepoint/commit/rollback code remains the current implementation.

```python
self._checked_write_depth += 1
try:
    drafts = decide(events, current_version)
finally:
    self._checked_write_depth -= 1

def _has_checked_write_owner(self):
    return self._connection.in_transaction and self._checked_write_depth > 0
```

In ArtifactStore._digest_lock add `_require_no_transaction()` and the depth check **before** constructing/opening the lock path. The process-global `_DIGEST_DEPTH = threading.local()` is shared by all store instances. After the actual OS flock succeeds and before entering the real mutex, set depth to one; in the existing finally clear it only if this call set it. These exact inserted fragments preserve the existing OS/mutex acquisition and release code:

```python
self._require_no_transaction()
if getattr(_DIGEST_DEPTH, "value", 0):
    raise ArtifactMetadataError("nested digest lock is forbidden")
entered_depth = False
# Immediately after the existing `locked = True`:
_DIGEST_DEPTH.value = 1
entered_depth = True
# First statements in the existing finally:
if entered_depth:
    _DIGEST_DEPTH.value = 0
```

Candidate event contract dispatch is this exact first block in validate_event_contract, before its existing reducers. Import the schema function locally to avoid module initialization cycles:

```python
candidate_types = {"CandidatePublicationPrepared", "CandidatePublicationCommitted"}
if any(e.stream_type == "candidate_publications" or e.event_type in candidate_types for e in events):
    invalid = False
    try:
        from orchestrator.artifacts.candidate_schema import validate_candidate_events
        if any(e.stream_type != "candidate_publications" for e in events):
            raise ValueError("candidate event stream mismatch")
        validate_candidate_events(events)
    except Exception:
        invalid = True
    if invalid:
        raise EventContractError("candidate event contract invalid")
```

At the first line of ordinary publish_bytes after its docstring, reject reserved source keys before tempfile creation. Its keyword `type` shadows the built-in, so use membership checks rather than calling `type(source)` in that method:

```python
self._require_no_transaction()
if source is not None and ("publication_protocol" in source or "candidate_set_id" in source):
    raise ArtifactMetadataError("candidate namespace requires candidate service")
```

At `_record_intent` and `_record_metadata` entry add `if reserved(record.to_event_payload()): raise ArtifactMetadataError("candidate namespace requires candidate service")`. Only dedicated checked-owner candidate methods may append reserved records. CandidateRecord.to_event_payload uses this exact expression for its existing created_at field, leaving all generic output unchanged:

```python
"created_at": (self.created_at.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if self.source.get("publication_protocol") == "maestro.candidate-publication.v1"
    else self.created_at.isoformat()),
```

No per-call file safety error should be translated to a stale-authority result. InventoryChanged is the only recapture signal; unreadable/invalid metadata and verified content mismatches fail immediately. `_capture_inventory` registers `_captured` only after observations complete, and clears a failed capture's partial registration in finally. `_checked` clears `_captured` on success/rollback/uncertainty. The private `threading.local()` digest-depth guard is checked before `os.open` and incremented only on actual OS lock acquisition; every finally decrements only when the current call incremented it. It protects cross-instance nested mutex order, not cross-process authorization.

Implement this parameterized real inversion detector in `tests/integration/test_artifact_recovery_lock_order.py`; its RECOVERY_OPERATIONS and exact adapter are in R3-C. Imports: multiprocessing, contextlib.contextmanager, types.SimpleNamespace, datetime.datetime, ControlPlaneApplication, ConcurrencyLimits and the support builder. Each child owns its own application/SQLite connection, opened before the lock rendezvous. The first child really holds the OS digest lock and mutex; the second reports its actual connection state immediately before acquiring that same lock.

```python
def lock_order_process(database, root, identity, now, mode, pipe, request_json, decision_json, contract_json, proposal_hash):
    from orchestrator.routing import RoutingRequest, RoutingDecision, PlanningNodeContract
    from orchestrator.scheduler import SchedulerError
    request = RoutingRequest.model_validate(request_json)
    decision = RoutingDecision.model_validate(decision_json)
    contract = PlanningNodeContract.model_validate(contract_json)
    with ControlPlaneApplication(database, artifact_root=root,
            limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4,
                                     provider_active_attempts=8, tool_active_attempts=4)) as app:
        store = app._artifact_store
        if mode == "publish":
            original = store._record_intent
            def pause(record):
                pipe.send("digest-held")
                assert pipe.poll(10) and pipe.recv() == "release"
                return original(record)
            store._record_intent = pause
        else:
            original = store._digest_lock
            announced = False
            @contextmanager
            def observed(digest):
                nonlocal announced
                if not announced:
                    announced = True
                    pipe.send(("before-digest", app._event_store.in_transaction))
                with original(digest):
                    yield
            store._digest_lock = observed
        pipe.send("ready")
        assert pipe.poll(10) and pipe.recv() == "start"
        if mode == "publish":
            store.publish_bytes(b"same", source={"run_id": "run-1"}, readable_scope=("run-1",))
        else:
            try:
                recovery_operation(app, mode, identity, datetime.fromisoformat(now),
                                   request, decision, contract, proposal_hash)
            except SchedulerError:
                assert mode == "recovery_plan" and announced
        pipe.send("done")

@pytest.mark.parametrize("operation", RECOVERY_OPERATIONS)
def test_active_resolution_does_not_invert_publication_lock(tmp_path, operation):
    case = candidate_case(tmp_path)
    case.artifacts.publish_bytes(b"same", source={"run_id": "run-1"}, readable_scope=("run-1",))
    from tests.support.attempt_acceptance import proposal_for_attempt
    proposal = proposal_for_attempt(case.app._scheduler, run_id=case.identity["run_id"],
        node_id=case.identity["node_id"], attempt_id=case.identity["attempt_id"])
    serialized = (case.request.model_dump(mode="json"), case.decision.model_dump(mode="json"),
                  case.contract.model_dump(mode="json"), proposal.task_sha256)
    ctx = multiprocessing.get_context("spawn")
    pa, ca = ctx.Pipe()
    pb, cb = ctx.Pipe()
    args = (case.database, case.root, case.identity, case.clock.now.isoformat())
    a = ctx.Process(target=lock_order_process, args=(*args, "publish", ca, *serialized))
    b = ctx.Process(target=lock_order_process, args=(*args, operation, cb, *serialized))
    try:
        a.start()
        b.start()
        assert pa.poll(10) and pa.recv() == "ready"
        assert pb.poll(10) and pb.recv() == "ready"
        pa.send("start")
        assert pa.poll(10) and pa.recv() == "digest-held"
        pb.send("start")
        assert pb.poll(10)
        phase, owns_sqlite = pb.recv()
        assert phase == "before-digest"
        pa.send("release")
        assert pa.poll(10) and pa.recv() == "done"
        assert pb.poll(10) and pb.recv() == "done"
        a.join(15)
        b.join(15)
        assert a.exitcode == b.exitcode == 0
        assert owns_sqlite is False
    finally:
        for process in (a, b):
            if process.pid is not None and process.is_alive():
                process.terminate()
                process.join(15)
        for pipe in (pa, ca, pb, cb):
            pipe.close()
        case.app.close()
```

The preexisting same-digest publication ensures the second process actually discovers and attempts that digest lock before the first releases it. A cold empty inventory would not exercise the inversion. Existing code is expected to time out or report SQLite ownership at this rendezvous; fixed code completes with ownership False. Startup variants install the observation wrapper on the ArtifactStore class before opening the reader application, after the writer is already paused, and retain the same release ordering. They do not wait for application readiness before releasing a lock that startup needs.

### Task 2: exact accounting and durable fixture bodies

Add pure `candidate_schema.account_candidate_bytes(current: int, *, preparation_size: int = 0, committed_size: int | None = None) -> int`. `current` is already validated replay accounting; this helper performs one preparation addition or one reserved-commit replacement. A preparation-size call requires 1..EVENT_CAP and committed_size None; a commit call requires preparation_size zero and 1..COMMIT_RESERVE. Count enforcement remains separate under the same journal transaction.

```python
def account_candidate_bytes(current, *, preparation_size=0, committed_size=None):
    if type(current) is not int or not 0 <= current <= RUN_CAP:
        raise ValueError("candidate journal capacity invalid")
    if committed_size is None:
        if type(preparation_size) is not int or not 1 <= preparation_size <= EVENT_CAP:
            raise ValueError("candidate journal payload size invalid")
        result = current + preparation_size + COMMIT_RESERVE
    else:
        if (type(preparation_size) is not int or preparation_size != 0 or
            type(committed_size) is not int or not 1 <= committed_size <= COMMIT_RESERVE or
            current < COMMIT_RESERVE):
            raise ValueError("candidate journal commit size invalid")
        result = current - COMMIT_RESERVE + committed_size
    if result > RUN_CAP:
        raise ValueError("candidate journal capacity exceeded")
    return result

def test_reserved_commit_fits_exact_accounting_boundary():
    before = RUN_CAP - COMMIT_RESERVE - 17
    assert account_candidate_bytes(before, preparation_size=17) == RUN_CAP
    assert account_candidate_bytes(before - 1, preparation_size=17) == RUN_CAP - 1
    with pytest.raises(ValueError):
        account_candidate_bytes(before + 1, preparation_size=17)
    assert account_candidate_bytes(RUN_CAP, committed_size=COMMIT_RESERVE) == RUN_CAP
    assert account_candidate_bytes(RUN_CAP, committed_size=17) == RUN_CAP - COMMIT_RESERVE + 17
    with pytest.raises(ValueError):
        account_candidate_bytes(RUN_CAP, committed_size=COMMIT_RESERVE + 1)
```

The journal reducer computes canonical payload length before each call, applies it incrementally, and has a per-request uncommitted set so a commit cannot release somebody else's reservation. A repeated event is corruption; an exact API retry does not append an event and leaves accounting unchanged. A cap error from an attempted new request maps to too_large; a cap violation found in durable history is unavailable/corruption, not permission to truncate replay.

Define `fixture_artifact_draft(event_type: str, record: dict, context: AttemptContext, set_id: str) -> EventDraft`, `finish_candidate_fixture(case, contents: tuple[bytes, ...]) -> CandidateSet` and the previously specified `append_candidate_fixture` in `tests/support/candidate_publication.py`. Add imports for `ArtifactRecord`, `EventDraft`, `decode_worker_output`, `CandidatePublicationJournal`, and the schema/authority helpers. These are test-only construction routines; the product never calls them.

```python
def fixture_artifact_draft(event_type, record, context, set_id):
    return EventDraft(event_type, record, run_id=context.run_id, node_id=context.node_id,
        attempt_id=context.attempt_id, fencing_generation=context.fencing_generation,
        correlation_id=context.run_id, causation_id=set_id)

def append_candidate_fixture(case, contents, *, commit):
    decoded = decode_worker_output(candidate_frame(case.task, contents), case.task)
    context = case.task.context
    members = sorted([blob.artifact.model_dump(mode="json") for blob in decoded.blobs], key=lambda r: r["digest"])
    request_id = candidate_hash("maestro.candidate-request.v1", case.identity)
    result_id_hash = candidate_hash("maestro.candidate-result-id.v1", decoded.result.result_id)
    identity = dict(request_id=request_id, context=context.model_dump(mode="json"),
        result_id_hash=result_id_hash, policy_id=POLICY, output_byte_limit=CONTENT_CAP,
        members=members)
    set_id = candidate_hash("maestro.candidate-set.v1", identity)
    def prepare(events, version):
        binding = case.app._scheduler.resolve_verification_binding(
            **case.identity, as_of=case.clock.now)
        source = dict(run_id=context.run_id, node_id=context.node_id,
            attempt_id=context.attempt_id, fencing_generation=str(context.fencing_generation),
            agent_instance_id=context.agent_instance_id,
            publication_protocol=SCHEMA, candidate_set_id=set_id)
        records = [ArtifactRecord.from_metadata(member["digest"], size=member["size_bytes"],
            artifact_type="worker-candidate", media_type="application/octet-stream",
            source=source, schema_version=1, redaction_state="unknown",
            readable_scope=(context.run_id,), references=(), lifecycle_state="temporary",
            created_at=case.clock.now, publication_id="candidate-v1:" + candidate_hash(
                "maestro.candidate-member.v1", dict(set_id=set_id, digest=member["digest"]))[7:]
            ).to_event_payload() for member in members]
        payload = dict(identity, schema=SCHEMA, set_id=set_id,
            reservation_id=binding.active_attempt.reservation.reservation_id,
            accepted_route_id=binding.active_attempt.accepted_route.decision_id,
            accepted_at=utc_text(binding.active_attempt.accepted_at),
            lease_expires_at=utc_text(binding.active_attempt.lease_expires_at),
            prepared_at=utc_text(case.clock.now),
            authority_heads=list(capture_authority_heads(case.store, context.run_id)), records=records)
        for record in records:
            case.store.append("artifact", record["digest"],
                case.store.current_version("artifact", record["digest"]),
                [fixture_artifact_draft("ArtifactPublicationIntent", record, context, set_id)],
                "artifact-intent:" + record["publication_id"])
        return [fixture_artifact_draft("CandidatePublicationPrepared", payload, context, set_id)]
    case.app._scheduler.recovery._checked((context.run_id,), "candidate_publications",
        context.run_id, "candidate-prepare:" + request_id[7:], prepare)
    if commit:
        return finish_candidate_fixture(case, contents)
    return CandidatePublicationJournal(case.store).lookup(**case.identity)

def finish_candidate_fixture(case, contents):
    journal = CandidatePublicationJournal(case.store)
    prepared = journal.lookup(**case.identity)
    assert prepared is not None and prepared.committed is None
    for body in contents:
        case.artifacts.publish_bytes(body, source={"run_id": "fixture-storage"},
                                     readable_scope=("fixture-storage",))
    def finalize(events, version):
        fresh = journal.lookup(**case.identity)
        assert fresh is not None and fresh.committed is None
        for record in fresh.prepared["records"]:
            case.store.append("artifact", record["digest"],
                case.store.current_version("artifact", record["digest"]),
                [fixture_artifact_draft("ArtifactPublished", record, fresh.context, fresh.set_id)],
                "artifact-publication:" + record["publication_id"])
        payload = dict(schema=SCHEMA, request_id=fresh.request_id, set_id=fresh.set_id,
            prepared_hash=candidate_hash("maestro.candidate-prepared.v1", fresh.prepared),
            authority_heads=list(capture_authority_heads(case.store, fresh.context.run_id)),
            committed_at=utc_text(case.clock.now))
        return [fixture_artifact_draft("CandidatePublicationCommitted", payload, fresh.context, fresh.set_id)]
    case.app._scheduler.recovery._checked((prepared.context.run_id,), "candidate_publications",
        prepared.context.run_id, "candidate-commit:" + prepared.request_id[7:], finalize)
    return journal.lookup(**case.identity)
```

Before using those fixtures to exercise invalid history, prove the valid fixture passes `audit_all` and matches the independent tagged-hash oracle. Fixture-installed generic records do not grant candidate eligibility; every candidate gate still requires the real candidate marker and all exact links. No fixture imports or helper calls enter product modules.

The field-rewrite helper is concrete and confined to disposable test databases:

```python
def rewrite_event(store, event_id, *, payload=None, headers=None):
    allowed = {"run_id", "node_id", "attempt_id", "fencing_generation", "correlation_id",
               "causation_id", "idempotency_key", "stream_version", "schema_version", "event_type"}
    changes = dict(headers or {})
    if not set(changes) <= allowed:
        raise ValueError("unsupported fixture header")
    if payload is not None:
        encoded = canonical_bytes(payload)
        changes.update(payload_json=encoded.decode("utf-8"), payload_hash=hashlib.sha256(encoded).hexdigest())
    if not changes:
        raise ValueError("empty fixture change")
    trigger = store._connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='events_immutable_update'").fetchone()[0]
    store._connection.execute("DROP TRIGGER events_immutable_update")
    try:
        columns = tuple(sorted(changes))
        store._connection.execute("UPDATE events SET " + ", ".join(name + "=?" for name in columns)
            + " WHERE event_id=?", tuple(changes[name] for name in columns) + (event_id,))
    finally:
        store._connection.execute(trigger)
```

### Task 2: executable unsafe-model and coherent-read tests

Add these imports to `test_candidate_set_gates.py`: pytest, types.SimpleNamespace, CandidateJournalError/CandidatePublicationJournal, ArtifactRef, and the public test support functions. These cases use a valid committed fixture before changing only the caller value:

```python
@pytest.mark.parametrize("mutation", ["extra", "extra_storage", "bool_size", "subclass", "duplicate", "subset"])
def test_constructed_models_cannot_bypass_set_gate(tmp_path, mutation):
    case = candidate_case(tmp_path)
    extra_evaluations = []
    try:
        committed = append_candidate_fixture(case, (b"first", b"second"), commit=True)
        refs = committed.members
        if mutation == "extra":
            refs = (refs[0].model_copy(update={"untrusted_extra": "bad"}), refs[1])
        elif mutation == "extra_storage":
            class FalseyExtra:
                def __bool__(self):
                    extra_evaluations.append(True)
                    return False
            altered = refs[0].model_copy()
            object.__setattr__(altered, "__pydantic_extra__", FalseyExtra())
            refs = (altered, refs[1])
        elif mutation == "bool_size":
            refs = (refs[0].model_copy(update={"size_bytes": True}), refs[1])
        elif mutation == "subclass":
            class OverriddenRef(ArtifactRef):
                size_bytes: str
            refs = (OverriddenRef.model_construct(**dict(refs[0].__dict__, size_bytes="bad")), refs[1])
        elif mutation == "duplicate":
            refs = (refs[0], refs[0])
        else:
            refs = refs[:1]
        counts = event_counts(case.store)
        with pytest.raises(CandidateJournalError):
            CandidatePublicationJournal(case.store).require_set(case.task.context, refs)
        assert extra_evaluations == []
        assert event_counts(case.store) == counts
    finally:
        case.app.close()
```

In `test_candidate_read_snapshots.py`, implement this actual separate-process journal-first race as the base for the full Step-7 parameter matrix. Imports: `import multiprocessing`, `from types import SimpleNamespace`, `from datetime import datetime`, ControlPlaneApplication, ConcurrencyLimits, EventDraft, CandidatePublicationJournal, `candidate_case`, `append_candidate_fixture`, `finish_candidate_fixture`. `commit_fixture_process(database, root, identity, now, pipe) -> None` is a top-level spawn target defined below, not a subprocess string.

```python
def commit_fixture_process(database, root, identity, now, pipe):
    with ControlPlaneApplication(database, artifact_root=root,
            limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4,
                                     provider_active_attempts=8, tool_active_attempts=4)) as app:
        case = SimpleNamespace(app=app, store=app._event_store, artifacts=app._artifact_store,
            identity=identity, clock=SimpleNamespace(now=datetime.fromisoformat(now)))
        pipe.send("ready")
        assert pipe.poll(10) and pipe.recv() == "commit"
        pipe.send("attempting")
        finish_candidate_fixture(case, (b"first", b"second"))
        pipe.send("committed")

def test_candidate_read_race_is_one_view(tmp_path, monkeypatch):
    case = candidate_case(tmp_path)
    parent, child = multiprocessing.get_context("spawn").Pipe()
    process = multiprocessing.get_context("spawn").Process(target=commit_fixture_process,
        args=(case.database, case.root, case.identity, case.clock.now.isoformat(), child))
    try:
        append_candidate_fixture(case, (b"first", b"second"), commit=False)
        process.start()
        assert parent.poll(10) and parent.recv() == "ready"
        original = case.store.validated_stream
        seen = False
        def read(stream_type, stream_id):
            nonlocal seen
            rows = original(stream_type, stream_id)
            if stream_type == "candidate_publications" and not seen:
                seen = True
                assert case.store.in_transaction
                parent.send("commit")
                assert parent.poll(10) and parent.recv() == "attempting"
                assert parent.poll(10) and parent.recv() == "committed"
            return rows
        monkeypatch.setattr(case.store, "validated_stream", read)
        result = CandidatePublicationJournal(case.store).lookup(**case.identity)
        assert seen and result.committed is None
        assert not case.store.in_transaction
        monkeypatch.setattr(case.store, "validated_stream", original)
        assert CandidatePublicationJournal(case.store).lookup(**case.identity).committed is not None
        process.join(15)
        assert process.exitcode == 0
    finally:
        if process.pid is not None and process.is_alive():
            process.terminate()
            process.join(15)
        parent.close()
        child.close()
        case.app.close()
```

This base journal-first test is retained as a focused regression; R3-D supplies the complete real public-reader/outer-ownership matrix. Its write-owner branch waits for committed only after owner release.

### Task 3: exact final-sample, forged installation, and historical retry assertions

In `test_candidate_publication_transactions.py`, import pytest, `datetime, timedelta` from datetime, CandidatePublicationError, CandidatePublicationJournal and the shared helpers. This test pins metadata-time advancement and actual rollback; it runs against the product publisher, not the test event fixture:

```python
@pytest.mark.parametrize("anchor, final_offset, committed", [
    ("expiry", -1, True), ("expiry", 0, False), ("expiry", 1, False),
    ("preparation", -1, False), ("after_lock", -1, False),
])
def test_final_clock_sample_controls_whole_commit(tmp_path, monkeypatch, anchor, final_offset, committed):
    case = candidate_case(tmp_path)
    try:
        service = case.app._candidate_publications
        service._clock = lambda: case.clock.now
        before = case.clock.now
        samples = {}
        finalize = service._finalize
        def enter_final(prepared, operation, installations, task_text):
            samples["preparation"] = datetime.fromisoformat(prepared.prepared["prepared_at"])
            samples["expiry"] = case.accepted.lease_expires_at
            samples["after_lock"] = samples["expiry"] - timedelta(seconds=2)
            assert samples["preparation"] < samples["after_lock"] - timedelta(seconds=1)
            case.clock.now = samples["after_lock"]
            return finalize(prepared, operation, installations, task_text)
        monkeypatch.setattr(service, "_finalize", enter_final)
        original = case.artifacts._record_candidate_metadata
        def append_and_advance(record, context, set_id):
            original(record, context, set_id)
            case.clock.now = samples[anchor] + timedelta(seconds=final_offset)
        monkeypatch.setattr(case.artifacts, "_record_candidate_metadata", append_and_advance)
        raw = candidate_frame(case.task, (b"first", b"second"))
        if committed:
            case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
        else:
            with pytest.raises(CandidatePublicationError) as caught:
                case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
            assert caught.value.code == "candidate_publication_inactive"
        view = CandidatePublicationJournal(case.store).lookup(**case.identity)
        assert view.prepared["prepared_at"] == before.isoformat(timespec="microseconds")
        assert (view.committed is not None) is committed
        for record in view.records:
            rows = case.store.read_stream("artifact", record.digest)
            published = [row for row in rows if row.event_type == "ArtifactPublished"
                         and row.payload["publication_id"] == record.publication_id]
            assert len(published) == int(committed)
            assert case.artifacts._path_for_digest(record.digest).is_file()
        if committed:
            assert view.committed["committed_at"] == case.clock.now.isoformat(timespec="microseconds")
            assert view.committed["committed_at"] != view.prepared["prepared_at"]
            assert case.clock.now != samples["after_lock"]
        assert_held(case)
    finally:
        case.app.close()
```

The preparation-relative case derives its backward sample from the actual durable `prepared_at`; the separate after-lock-relative case remains above preparation but below the after-lock sample. The `_finalize` wrapper forwards the operation token and installation tuple unchanged and sets the trusted clock to expiry minus two seconds before the real callback samples it. Thus the negative cases do not depend on a guessed lease duration. For a valid final guard whose outer commit/response happens after expiry, wrap the real append_checked and change clock only after its callback has returned the marker; do not resample/rewrite the marker. Assert original committed_at survives.

In `test_candidate_publication.py`, the injected decoder check must use the actual module global `decode_worker_output` seam. Store-issued installation checks must reject a constructed replacement even when all visible fields match:

```python
def test_decoder_and_private_result_are_not_authority(tmp_path, monkeypatch):
    from dataclasses import replace
    import orchestrator.runtime.candidate_publication as publication
    from orchestrator.runtime.worker_output import decode_worker_output
    case = candidate_case(tmp_path)
    try:
        service = case.app._candidate_publications
        service._clock = lambda: case.clock.now
        raw = candidate_frame(case.task, (b"first",))
        decoded = decode_worker_output(raw, case.task)
        bad_blob = replace(decoded.blobs[0], content=bytearray(b"first"))
        monkeypatch.setattr(publication, "decode_worker_output", lambda payload, task:
                            replace(decoded, blobs=(bad_blob,)))
        with pytest.raises(CandidatePublicationError) as caught:
            case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
        assert caught.value.code == "candidate_publication_invalid"
        assert case.store.read_stream("candidate_publications", "run-1") == []
        monkeypatch.setattr(publication, "decode_worker_output", decode_worker_output)
        stage = case.artifacts._stage_candidate_blob
        def forged(**kwargs):
            installed = stage(**kwargs)
            return replace(installed)
        monkeypatch.setattr(case.artifacts, "_stage_candidate_blob", forged)
        with pytest.raises(CandidatePublicationError):
            case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
        view = CandidatePublicationJournal(case.store).lookup(**case.identity)
        assert view.committed is None
        assert all(case.artifacts._path_for_digest(record.digest).is_file() for record in view.records)
    finally:
        case.app.close()

def test_committed_retry_after_expiry_is_read_only(tmp_path, monkeypatch):
    case = candidate_case(tmp_path)
    try:
        service = case.app._candidate_publications
        service._clock = lambda: case.clock.now
        raw = candidate_frame(case.task, (b"first", b"second"))
        receipt = case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
        case.app._scheduler.lifecycle.request_cancel("run-1", reason_code="fixture_cancel")
        case.clock.now = case.accepted.lease_expires_at + timedelta(days=1)
        counts = event_counts(case.store)
        def forbidden(*args, **kwargs):
            raise AssertionError("historical retry must not perform this action")
        monkeypatch.setattr(service, "_clock", forbidden)
        monkeypatch.setattr(case.app._worker_inputs, "prepare_worker_task", forbidden)
        monkeypatch.setattr(case.artifacts, "_stage_candidate_blob", forbidden)
        monkeypatch.setattr(case.app._artifact_grants, "issue", forbidden)
        monkeypatch.setattr(case.store, "append_checked", forbidden)
        assert case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw) == receipt
        assert event_counts(case.store) == counts
        assert case.app.candidate_publication_status(**case.identity).state == "committed"
    finally:
        case.app.close()
```

Because a hash/model/receipt is not an origin guarantee, these tests assert durable effects and actual call order, not a synthetic capability Boolean. A malicious same-UID host or arbitrary Python execution inside the control plane remains outside the threat model.

## Final self-review checklist for execution handoff

### Revision R3-G: shared candidate Runs and corrupted-file restart

In test support, define `another_run_case` using public run_setup/routed_pair on the **same** application/database/root. This is the second candidate Run, not a generic publication relabelled as a candidate. Both use the same trusted current UTC window and real persisted input admission.

```python
def another_run_case(case, run_id):
    from tests.unit.lifecycle.test_scheduler import run_setup, routed_pair
    reg, config, lifecycle, policy = run_setup(case.store, run_id=run_id, freeze_contracts=True)
    node = lifecycle.replay(run_id).node("node-1")
    request, decision = routed_pair(reg, config, policy, run_id=run_id,
                                    contract_hash=node.spec.planning_contract_hash)
    accepted = case.app.accept_routing_with_input(request, decision, task_text=case.text,
        accepted_at=case.clock.now - timedelta(seconds=1), lease_expires_at=case.clock.now + timedelta(hours=1))
    identity = dict(run_id=run_id, node_id="node-1", attempt_id=request.attempt_id, fencing_generation=1)
    task = case.app.prepare_worker_task(**identity, task_text=case.text)
    return SimpleNamespace(**dict(vars(case), identity=identity, task=task, accepted=accepted,
        request=request, decision=decision, contract=node.spec.planning_contract, policy=policy,
        registry=reg, config=config))

def test_shared_candidate_bytes_keep_distinct_run_authority(tmp_path):
    case = candidate_case(tmp_path)
    try:
        other = another_run_case(case, "run-2")
        case.app._candidate_publications._clock = lambda: case.clock.now
        a = case.app.publish_worker_candidate(**case.identity, task_text=case.text,
                                              payload=candidate_frame(case.task, (b"same",)))
        b = case.app.publish_worker_candidate(**other.identity, task_text=other.text,
                                              payload=candidate_frame(other.task, (b"same",)))
        assert a.request_id != b.request_id and a.set_id != b.set_id
        journal = CandidatePublicationJournal(case.store)
        first, second = journal.lookup(**case.identity), journal.lookup(**other.identity)
        assert first.records[0].digest == second.records[0].digest
        assert first.records[0].publication_id != second.records[0].publication_id
        authority = EphemeralArtifactGrantAuthority()
        case.artifacts._grant_verifier = authority
        for run_id in ("run-1", "run-2"):
            grant = authority.issue(digest=first.members[0].digest, run_id=run_id,
                                    expires_at=datetime.now(timezone.utc) + timedelta(minutes=5))
            records = case.artifacts.list_publications(first.members[0].digest, grant=grant)
            assert records and {r.source["run_id"] for r in records} == {run_id}
        with pytest.raises(CandidateJournalError):
            journal.require_set(other.task.context, first.members + second.members)
        assert case.artifacts._path_for_digest(first.members[0].digest).read_bytes() == b"same"
    finally:
        case.app.close()

@pytest.mark.parametrize("damage", ["missing", "wrong_bytes", "symlink"])
def test_committed_content_damage_blocks_restart_without_repair(tmp_path, damage):
    case = candidate_case(tmp_path)
    saved = append_candidate_fixture(case, (b"first",), commit=True)
    path = case.artifacts._path_for_digest(saved.members[0].digest)
    before = event_counts(case.store)
    case.app.close()
    if damage == "missing":
        path.unlink()
    elif damage == "wrong_bytes":
        path.write_bytes(b"wrong")
    else:
        target = tmp_path / "symlink-target"
        target.write_bytes(b"first")
        path.unlink()
        path.symlink_to(target)
    with pytest.raises(StartupRecoveryFailed):
        ControlPlaneApplication(case.database, artifact_root=case.root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
    store = SQLiteEventStore(case.database)
    try:
        assert event_counts(store) == before
        assert CandidatePublicationJournal(store).lookup(**case.identity).committed is not None
    finally:
        store.close()
    if damage == "missing":
        assert not path.exists()
    elif damage == "wrong_bytes":
        assert path.read_bytes() == b"wrong"
    else:
        assert path.is_symlink() and target.read_bytes() == b"first"
```

### Revision R3-F: witnessed corruption, real thread inversion and bounded inventories

Add these artifact/head mutation cases to `test_candidate_journal.py`; they supplement (not replace) all-field candidate payload deletion and event header matrices. No fixture catches its own corruption-injection error as a passing domain rejection.

```python
@pytest.mark.parametrize("kind", ["ArtifactPublicationIntent", "ArtifactPublished"])
def test_every_raw_record_and_source_field_is_bound(tmp_path, kind):
    case = candidate_case(tmp_path)
    try:
        saved = append_candidate_fixture(case, (b"a", b"b"), commit=True)
        event = next(e for e in case.store.read_stream("artifact", saved.members[0].digest)
                     if e.event_type == kind and reserved(e.payload))
        original = plain(event.payload)
        for path in tuple(leaf_paths(original)):
            rewrite_event(case.store, event.event_id, payload=removed(original, path))
            with pytest.raises(CandidateJournalError):
                CandidatePublicationJournal(case.store).lookup(**case.identity)
            rewrite_event(case.store, event.event_id, payload=original)
        assert CandidatePublicationJournal(case.store).lookup(**case.identity) == saved
    finally:
        case.app.close()

@pytest.mark.parametrize("stream", ["run", "run_lifecycle", "agent_registry", "budget", "scheduler"])
@pytest.mark.parametrize("alteration", ["head_id", "head_hash", "head_version", "source_hash"])
def test_each_historical_head_is_an_exact_prefix(tmp_path, stream, alteration):
    case = candidate_case(tmp_path)
    try:
        saved = append_candidate_fixture(case, (b"a",), commit=True)
        event = case.store.read_stream("candidate_publications", "run-1")[0]
        payload = plain(event.payload)
        head = next(h for h in payload["authority_heads"] if h["stream_type"] == stream)
        if alteration == "source_hash":
            corrupt_events(case.store, lambda c: c.execute(
                "UPDATE events SET payload_hash=? WHERE event_id=?", ("0" * 64, head["event_id"])))
        else:
            if alteration == "head_id":
                head["event_id"] = "x" * 129
            elif alteration == "head_hash":
                head["payload_hash"] = "0" * 64
            else:
                head["version"] += 1
            rewrite_event(case.store, event.event_id, payload=payload)
        with pytest.raises(CandidateJournalError):
            CandidatePublicationJournal(case.store).lookup(**case.identity)
    finally:
        case.app.close()

@pytest.mark.parametrize("kind", ["fence", "agent", "reservation"])
def test_rehashed_authority_corruption_never_finalizes(tmp_path, monkeypatch, kind):
    case = candidate_case(tmp_path)
    fired = []
    def hit(point):
        if point != "after_prepare_commit" or fired:
            return
        fired.append(point)
        stream, name, event_type = {
            "fence": ("scheduler", "global", "RoutingDecisionAccepted"),
            "agent": ("agent_registry", "run-1", "AgentInstanceCreated"),
            "reservation": ("budget", "run-1", "BudgetReserved")}[kind]
        event = next(e for e in case.store.read_stream(stream, name) if e.event_type == event_type)
        payload = plain(event.payload)
        if kind == "fence":
            payload["accepted_route"]["fencing_generation"] = 2
        elif kind == "agent":
            payload["instance"]["status"] = "completed"
        else:
            payload["reservation_id"] = "wrong-reservation"
        rewrite_event(case.store, event.event_id, payload=payload)
    try:
        case.app._candidate_publications._clock = lambda: case.clock.now
        install_candidate_hooks(case.app, monkeypatch, hit)
        with pytest.raises(CandidatePublicationError) as caught:
            case.app.publish_worker_candidate(**case.identity, task_text=case.text,
                                              payload=candidate_frame(case.task, (b"a",)))
        assert caught.value.code in {"candidate_publication_unavailable", "candidate_publication_uncertain"}
        assert fired == ["after_prepare_commit"]
        assert case.store._connection.execute("SELECT COUNT(*) FROM events WHERE event_type='CandidatePublicationCommitted'").fetchone()[0] == 0
        assert case.store._connection.execute("SELECT COUNT(*) FROM events WHERE event_type='CandidatePublicationPrepared'").fetchone()[0] == 1
    finally:
        case.app.close()
```

Place the following real concurrent witness test in Task-1 `test_artifact_inventory.py`. Corruption is not used: a second genuine ArtifactStore adds a new generic stream after each observed capture. Three failed inventories must be collected outside SQLite and append no requested probe. The successful one-change variant demonstrates actual rollback/recapture, not just monkeypatched InventoryChanged.

```python
@pytest.mark.parametrize("changes,success", [(1, True), (3, False)])
def test_new_streams_retry_only_outside_sqlite(tmp_path, monkeypatch, changes, success):
    case = candidate_case(tmp_path)
    other = ControlPlaneApplication(case.database, artifact_root=case.root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
    recovery = case.app._scheduler.recovery
    original = recovery._capture_inventory
    captures = []
    def capture(runs):
        assert not case.store.in_transaction
        inventory = original(runs)
        captures.append(inventory)
        if len(captures) <= changes:
            other._artifact_store.publish_bytes(f"new-{len(captures)}".encode(),
                source={"run_id": "foreign-fixture"}, readable_scope=("foreign-fixture",))
        return inventory
    monkeypatch.setattr(recovery, "_capture_inventory", capture)
    try:
        def run():
            return recovery._checked(("run-1",), "probe", "witness", "witness",
                lambda events, version: [EventDraft("Probe", {"value": 1})])
        if success:
            run()
            assert len(captures) == 2
            assert len(case.store.read_stream("probe", "witness")) == 1
        else:
            with pytest.raises(RunRecoveryError, match="inventory unavailable"):
                run()
            assert len(captures) == 3
            assert case.store.read_stream("probe", "witness") == []
        assert recovery._captured is recovery._scope is None
        assert not case.store.in_transaction
    finally:
        other.close()
        case.app.close()
```

For all-run startup capture membership, use this callback on its first post-capture boundary instead of publishing a foreign artifact; it creates a real frozen Run, not a fabricated empty inventory. Parameterize selected membership over complete Run setup and run_lifecycle discovery (the same public setup necessarily creates both):

```python
from tests.unit.lifecycle.test_scheduler import run_setup
run_setup(other._event_store, run_id="new-run", freeze_contracts=True)
```

The thread inversion detector is isolated in a child so a broken old lock order cannot leave pytest-owned threads hung. B really acquires its OS digest lock while A holds the real process mutex; A's nested second-digest request must fail before waiting. Parent kills/reaps only this test child on a bounded timeout. Put both functions in `test_artifact_recovery_lock_order.py` with `threading`, `fcntl`, `ArtifactStore`, `SQLiteEventStore`, and artifact store module imports.

```python
def crossed_thread_child(database, root, pipe):
    import fcntl
    import threading
    from orchestrator.artifacts import ArtifactStore
    from orchestrator.artifacts.store import ArtifactMetadataError
    a_held, b_os_locked = threading.Event(), threading.Event()
    ready = threading.Barrier(2, timeout=10)
    failures = []
    actual_flock = fcntl.flock
    def flock(fd, operation):
        result = actual_flock(fd, operation)
        if threading.current_thread().name == "B" and operation == fcntl.LOCK_EX:
            b_os_locked.set()
        return result
    def work(name):
        events = SQLiteEventStore(database)
        artifacts = ArtifactStore(root, event_store=events)
        try:
            ready.wait()
            if name == "A":
                with artifacts._digest_lock("sha256:" + "a" * 64):
                    a_held.set()
                    assert b_os_locked.wait(10)
                    with pytest.raises(ArtifactMetadataError):
                        with artifacts._digest_lock("sha256:" + "b" * 64):
                            raise AssertionError("second digest admitted under mutex")
            else:
                assert a_held.wait(10)
                with artifacts._digest_lock("sha256:" + "b" * 64):
                    assert not events.in_transaction
        except BaseException as error:
            failures.append(type(error).__name__)
        finally:
            events.close()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(fcntl, "flock", flock)
        threads = [threading.Thread(target=work, args=(name,), name=name, daemon=True) for name in ("A", "B")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(12)
        assert all(not t.is_alive() for t in threads) and failures == []
    pipe.send("complete")

def test_two_threads_cannot_wait_for_second_digest_under_mutex(tmp_path):
    case = candidate_case(tmp_path)
    mp = multiprocessing.get_context("spawn")
    parent, child = mp.Pipe()
    process = mp.Process(target=crossed_thread_child, args=(case.database, case.root, child))
    try:
        process.start()
        assert parent.poll(15) and parent.recv() == "complete"
        process.join(15)
        assert process.exitcode == 0
    finally:
        if process.pid is not None and process.is_alive():
            process.terminate()
            process.join(15)
        parent.close()
        child.close()
        case.app.close()
```

Pin byte/count extremes with real public frames and no lowered constants:

```python
@pytest.mark.parametrize("contents,allowed", [((b"",), True), ((b"x" * 16_384,), True),
    ((b"x" * 16_385,), False), (tuple(str(i).encode() for i in range(128)), True),
    (tuple(str(i).encode() for i in range(129)), False)])
def test_actual_content_and_member_limits(tmp_path, contents, allowed):
    case = candidate_case(tmp_path)
    try:
        case.app._candidate_publications._clock = lambda: case.clock.now
        # 129 is intentionally made through raw JSON because WorkerResult rejects it itself.
        value = json.loads(candidate_frame(case.task, contents[:128]))
        if len(contents) == 129:
            body = contents[-1]
            digest = "sha256:" + hashlib.sha256(body).hexdigest()
            value["result"]["artifacts"].append(dict(digest=digest, size_bytes=len(body),
                artifact_type="worker-candidate", media_type="application/octet-stream"))
            value["blobs"].append(dict(digest=digest, content_base64=base64.b64encode(body).decode()))
        raw = json.dumps(value).encode()
        if allowed:
            assert case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw).state == "committed"
        else:
            with pytest.raises(CandidatePublicationError):
                case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
            assert case.app.candidate_publication_status(**case.identity).state == "absent"
    finally:
        case.app.close()

def test_raw_frame_cap_includes_whitespace_and_retry_is_noop(tmp_path):
    case = candidate_case(tmp_path)
    try:
        case.app._candidate_publications._clock = lambda: case.clock.now
        raw = candidate_frame(case.task, (b"",))
        exact = raw + b" " * (1_048_576 - len(raw))
        receipt = case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=exact)
        before = event_counts(case.store)
        assert case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw) == receipt
        with pytest.raises(CandidatePublicationError) as caught:
            case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=exact + b" ")
        assert caught.value.code == "candidate_publication_too_large"
        assert event_counts(case.store) == before
    finally:
        case.app.close()
```

### Revision R3-E: remaining executable race, hostile-value and retention tables

In test support add this exact spawned caller for simultaneous publication. Each child opens its own application/store before announcing ready; the parent releases both only after both are ready. Parameterize identical versus changed result ID, and opposite body order for the identical case. No sleeps or artificial Boolean lock bypass is used.

```python
def simultaneous_publisher(database, root, identity, text, raw, now, pipe):
    app = ControlPlaneApplication(database, artifact_root=root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
    app._candidate_publications._clock = lambda: datetime.fromisoformat(now)
    pipe.send("ready")
    assert pipe.poll(10) and pipe.recv() == "start"
    try:
        receipt = app.publish_worker_candidate(**identity, task_text=text, payload=raw)
        pipe.send(("committed", receipt.request_id, receipt.set_id))
    except CandidatePublicationError as error:
        pipe.send((error.code,))
    finally:
        app.close()

@pytest.mark.parametrize("changed", [False, True])
def test_two_real_publishers_converge_or_conflict(tmp_path, changed):
    case = candidate_case(tmp_path)
    mp = multiprocessing.get_context("spawn")
    pipes, processes = [], []
    try:
        for index in range(2):
            parent, child = mp.Pipe()
            raw = candidate_frame(case.task, (b"first", b"second"), reverse=index == 1,
                                  result_id="changed" if changed and index == 1 else "result-1")
            process = mp.Process(target=simultaneous_publisher, args=(case.database, case.root,
                case.identity, case.text, raw, case.clock.now.isoformat(), child))
            pipes.append((parent, child))
            processes.append(process)
            process.start()
        for parent, child in pipes:
            assert parent.poll(10) and parent.recv() == "ready"
        for parent, child in pipes:
            parent.send("start")
        results = []
        for parent, child in pipes:
            assert parent.poll(15)
            results.append(parent.recv())
        if changed:
            assert sorted(r[0] for r in results) == ["candidate_publication_conflict", "committed"]
        else:
            assert results[0] == results[1] and results[0][0] == "committed"
        for process in processes:
            process.join(15)
            assert process.exitcode == 0
        saved = CandidatePublicationJournal(case.store).lookup(**case.identity)
        assert len(case.store.read_stream("candidate_publications", "run-1")) == 2
        for record in saved.records:
            assert sum(e.payload.get("publication_id") == record.publication_id
                       for e in case.store.read_stream("artifact", record.digest)) == 2
        assert_held(case)
    finally:
        for process in processes:
            if process.pid is not None and process.is_alive():
                process.terminate()
                process.join(15)
        for parent, child in pipes:
            parent.close()
            child.close()
        case.app.close()
```

Use the existing lock-order publisher child for the ordinary-publication versus candidate-stage inversion: point its reader at `_stage_candidate_blob` only after a real complete preparation, using a store-issued installation session and the exact durable digest/content. The concrete child branch is:

```python
elif mode == "candidate_stage":
    saved = CandidatePublicationJournal(app._event_store).lookup(**identity)
    with app._artifact_store._candidate_installation_session() as operation:
        app._artifact_store._stage_candidate_blob(operation=operation, run_id=identity["run_id"],
            request_id=saved.request_id, set_id=saved.set_id,
            digest="sha256:" + hashlib.sha256(b"same").hexdigest(), content=b"same")
```

For this branch the parent first prepares exactly `(b"same",)` through the Stage-2 fixture and uses the same digest-held/before-digest/release/done barrier. The parent asserts pending remains pending: staging plus an installation object cannot create commitment by itself.

The startup inversion variant runs this child after publisher A has announced digest-held. The parent receives before-digest, releases A, waits for A done, then receives reader ready. It uses the same 10-second/15-second finally terminate/reap pattern as the all-owner parent test, without waiting for startup before releasing its prerequisite lock.

```python
def startup_lock_reader(database, root, pipe):
    from contextlib import contextmanager
    from orchestrator.artifacts.store import ArtifactStore
    original = ArtifactStore._digest_lock
    seen = False
    @contextmanager
    def observed(self, digest):
        nonlocal seen
        if not seen:
            seen = True
            pipe.send(("before-digest", self._event_store.in_transaction))
        with original(self, digest):
            yield
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ArtifactStore, "_digest_lock", observed)
        app = ControlPlaneApplication(database, artifact_root=root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
        pipe.send("ready")
        app.close()
```

Hostile values are executable functions, not names to interpret later. Add these factories/test in `test_candidate_publication.py`; invalid calls occur before any candidate stream/file mutation. An `ArtifactRecord`/receipt is never accepted where the public API asks for bytes.

```python
class BytesSubclass(bytes):
    pass
class TextSubclass(str):
    pass

BAD_INPUTS = (
    ("payload", lambda case, raw: BytesSubclass(raw)),
    ("payload", lambda case, raw: bytearray(raw)),
    ("payload", lambda case, raw: memoryview(raw)),
    ("payload", lambda case, raw: b""),
    ("payload", lambda case, raw: b"x" * 1_048_577),
    ("task_text", lambda case, raw: TextSubclass(case.text)),
    ("task_text", lambda case, raw: " "),
    ("task_text", lambda case, raw: "x" * 100_001),
    ("task_text", lambda case, raw: "\ud800"),
    ("fencing_generation", lambda case, raw: True),
    ("fencing_generation", lambda case, raw: 0),
    ("run_id", lambda case, raw: "r" * 129),
    ("node_id", lambda case, raw: TextSubclass(case.identity["node_id"])),
)

@pytest.mark.parametrize("field,make", BAD_INPUTS)
def test_invalid_public_values_have_no_candidate_effect(tmp_path, field, make):
    case = candidate_case(tmp_path)
    try:
        raw = candidate_frame(case.task, (b"first",))
        request = dict(case.identity, task_text=case.text, payload=raw)
        request[field] = make(case, raw)
        before, files = event_counts(case.store), sorted(p.name for p in case.root.iterdir())
        with pytest.raises(CandidatePublicationError) as caught:
            case.app.publish_worker_candidate(**request)
        assert caught.value.code in {"candidate_publication_invalid", "candidate_publication_too_large"}
        assert caught.value.__context__ is caught.value.__cause__ is None
        assert event_counts(case.store) == before
        assert sorted(p.name for p in case.root.iterdir()) == files
    finally:
        case.app.close()

@pytest.mark.parametrize("point", ["decode", "clock", "stage", "metadata", "before_prepare_commit", "after_final_commit"])
def test_sensitive_backend_failures_are_detached(tmp_path, monkeypatch, caplog, point):
    case = candidate_case(tmp_path)
    sentinel = "CANDIDATE-SECRET-TEST-96a2"
    fired = []
    def fail(*args, **kwargs):
        raise RuntimeError(sentinel)
    def hit(name):
        if name == point and not fired:
            fired.append(name)
            fail()
    try:
        service = case.app._candidate_publications
        service._clock = lambda: case.clock.now
        if point == "decode":
            monkeypatch.setattr(publication, "decode_worker_output", fail)
        elif point == "clock":
            monkeypatch.setattr(service, "_clock", fail)
        elif point == "stage":
            monkeypatch.setattr(case.artifacts, "_stage_candidate_blob", fail)
        elif point == "metadata":
            monkeypatch.setattr(case.artifacts, "_record_candidate_metadata", fail)
        else:
            install_candidate_hooks(case.app, monkeypatch, hit)
        try:
            result = case.app.publish_worker_candidate(**case.identity, task_text=case.text,
                payload=candidate_frame(case.task, (sentinel.encode(),)))
        except CandidatePublicationError as error:
            assert error.args == (error.code,)
            assert error.__cause__ is error.__context__ is None
            assert sentinel not in str(error) + repr(error)
        else:
            assert point == "after_final_commit" and result.state == "committed"
        assert sentinel not in caplog.text
        assert all(sentinel not in row[0] for row in case.store._connection.execute("SELECT payload_json FROM events"))
    finally:
        case.app.close()

@pytest.mark.parametrize("point", ["after_link_0", "after_link_1", "after_dir_fsync_0", "after_dir_fsync_1"])
@pytest.mark.parametrize("shared", [False, True])
def test_install_failure_retains_every_final_object(tmp_path, monkeypatch, point, shared):
    case = candidate_case(tmp_path)
    fired = []
    bodies = (b"first", b"second")
    try:
        if shared:
            for body in bodies:
                case.artifacts.publish_bytes(body, source={"run_id": "run-A"}, readable_scope=("run-A",))
        def hit(name):
            if name == point and not fired:
                fired.append(name)
                raise OSError("fixture filesystem error")
        case.app._candidate_publications._clock = lambda: case.clock.now
        install_candidate_hooks(case.app, monkeypatch, hit)
        with pytest.raises(CandidatePublicationError) as caught:
            case.app.publish_worker_candidate(**case.identity, task_text=case.text,
                                              payload=candidate_frame(case.task, bodies))
        assert caught.value.code == "candidate_publication_uncertain"
        assert fired == [point]
        assert case.app.candidate_publication_status(**case.identity).state == "prepared"
        saved = CandidatePublicationJournal(case.store).lookup(**case.identity)
        assert all(len([e for e in case.store.read_stream("artifact", r.digest)
                        if e.payload.get("publication_id") == r.publication_id]) == 1 for r in saved.records)
        for ref in saved.members:
            path = case.artifacts._path_for_digest(ref.digest)
            if path.exists():
                assert "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest() == ref.digest
            if shared:
                authority = EphemeralArtifactGrantAuthority()
                case.artifacts._grant_verifier = authority
                grant = authority.issue(digest=ref.digest, run_id="run-A",
                                        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5))
                assert len(case.artifacts.read_bytes(ref.digest, grant=grant)) == ref.size_bytes
    finally:
        case.app.close()
```

### Revision R3-D: parameterized snapshot readers and real positive acceptance

Replace the single journal-first test with this full owning-module matrix in `test_candidate_read_snapshots.py`. It uses the actual public adapters from R3-A and the real `commit_fixture_process`. Enhance that writer to `pipe.send("attempting")` immediately before `finish_candidate_fixture`, then send committed afterward. The reader hook triggers after the **first real validated stream read**, regardless of whether that consumer discovers the candidate journal or an artifact first. A Run-A generic publication on the same first digest provides a valid old publication to read while the candidate remains prepared.

```python
READ_CASES = ("lookup", "require_set", "publication_view", "candidate_records", "read_bytes",
              "get_record", "list_publications", "get_records", "inventory", "pending_inventory")
DB_ONLY = {"lookup", "require_set", "publication_view"}

@pytest.mark.parametrize("consumer", READ_CASES)
@pytest.mark.parametrize("outer", [None, "BEGIN", "BEGIN IMMEDIATE"])
def test_every_reader_keeps_discovery_and_links_in_one_view(tmp_path, monkeypatch, consumer, outer):
    case = candidate_case(tmp_path)
    pending = append_candidate_fixture(case, (b"first", b"second"), commit=False)
    for body in (b"first", b"second"):
        case.artifacts.publish_bytes(body, source={"run_id": "run-A"}, readable_scope=("run-A",))
    authority = EphemeralArtifactGrantAuthority()
    case.artifacts._grant_verifier = authority
    grant = lambda digest: authority.issue(digest=digest, run_id="run-A",
                                           expires_at=datetime.now(timezone.utc) + timedelta(minutes=5))
    if outer is not None and consumer not in DB_ONLY:
        case.store._connection.execute(outer)
        try:
            def forbidden(*args, **kwargs):
                pytest.fail("outer owner reached filesystem or grant verification")
            monkeypatch.setattr(case.artifacts, "_authorize", forbidden)
            monkeypatch.setattr(case.artifacts, "_path_for_digest", forbidden)
            monkeypatch.setattr(case.artifacts, "_digest_lock", forbidden)
            with pytest.raises(ArtifactMetadataError):
                public_consumer(case, consumer, pending.members, None, grant)
            assert case.store.in_transaction
        finally:
            case.store._connection.rollback()
            case.app.close()
        return
    mp = multiprocessing.get_context("spawn")
    parent, child = mp.Pipe()
    process = mp.Process(target=commit_fixture_process,
        args=(case.database, case.root, case.identity, case.clock.now.isoformat(), child))
    seen = []
    original = case.store.validated_stream
    def read(kind, name):
        rows = original(kind, name)
        if not seen:
            seen.append((kind, case.store.in_transaction))
            parent.send("commit")
            assert parent.poll(10) and parent.recv() == "attempting"
            if outer != "BEGIN IMMEDIATE":
                assert parent.poll(10) and parent.recv() == "committed"
        return rows
    try:
        process.start()
        assert parent.poll(10) and parent.recv() == "ready"
        if outer:
            case.store._connection.execute(outer)
        monkeypatch.setattr(case.store, "validated_stream", read)
        if consumer in {"require_set", "candidate_records"}:
            with pytest.raises(CandidateJournalError if consumer == "require_set" else ArtifactMetadataError):
                public_consumer(case, consumer, pending.members, None, grant)
        else:
            result = public_consumer(case, consumer, pending.members, None, grant)
            if consumer == "lookup":
                assert result.committed is None
            elif consumer in {"publication_view", "list_publications", "get_records", "inventory"}:
                assert not any(reserved(r.to_event_payload()) for r in result)
        assert seen and seen[0][1] is True
        assert case.store.in_transaction is (outer is not None)
        if outer:
            case.store._connection.rollback()
        if outer == "BEGIN IMMEDIATE":
            assert parent.poll(10) and parent.recv() == "committed"
        monkeypatch.setattr(case.store, "validated_stream", original)
        assert CandidatePublicationJournal(case.store).lookup(**case.identity).committed is not None
        process.join(15)
        assert process.exitcode == 0
    finally:
        if case.store.in_transaction:
            case.store._connection.rollback()
        if process.pid is not None and process.is_alive():
            process.terminate()
            process.join(15)
        parent.close()
        child.close()
        case.app.close()
```

Task 3 extends `READ_CASES` with public status and historical retry by adding these exact public adapter branches. Pending status returns prepared in the raced old view. Historical retry is tested starting from a **committed** set against concurrent graph/cancellation, not falsely described as a read-only retry of a pending set. Its forbidden clock/write/grant/install assertions remain the concrete historical retry test already supplied.

```python
if name == "status":
    return case.app.candidate_publication_status(**case.identity)
if name == "historical_retry":
    return case.app.publish_worker_candidate(**case.identity, task_text=case.text,
        payload=candidate_frame(case.task, (b"first", b"second")))
```

Add this positive live acceptance test in `tests/integration/test_candidate_history.py` (Task 2 fixture producer) and reuse it with the public publisher in Task 3. No grant-clock mock is used: current UTC leases from candidate_case and the real ephemeral authority cover the actual run. This is a required live isolation test with no skip/fallback.

```python
def test_complete_candidate_still_requires_independent_acceptance(tmp_path):
    from orchestrator.runtime.acceptance_coordinator import AttemptExecutionCoordinator
    from orchestrator.runtime.verification_journal import VerifierProposalJournal
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    case = candidate_case(tmp_path)
    try:
        saved = append_candidate_fixture(case, (b"first", b"second"), commit=True)
        assert_held(case)
        proposal = candidate_proposal(case, saved.members)
        authority = EphemeralArtifactGrantAuthority()
        case.artifacts._grant_verifier = authority
        class Usage:
            def usage_for(self, snapshot):
                return candidate_usage(case)
        coordinator = AttemptExecutionCoordinator(scheduler=case.app._scheduler,
            artifact_store=case.artifacts, verifier=IsolatedVerifierProcess(),
            journal=VerifierProposalJournal(case.store), grants=authority,
            usage_source=Usage(), clock=lambda: case.clock.now)
        accepted = coordinator.accept_proposal(**case.identity, task_sha256=proposal.task_sha256)
        assert accepted == proposal
        assert case.app._scheduler.recovery.recover("run-1").active_attempts == ()
        case.app._scheduler.validate_success_proofs()
        reopened = ControlPlaneApplication(case.database, artifact_root=case.root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
        reopened.close()
    finally:
        case.app.close()
```

### Definitive serialized RED-first execution schedule

This schedule is the execution order; the earlier numbered sections organize implementation reference material. Each checkbox is one action; each test module is written before the production group it constrains. Re-run the exact same focused command for GREEN after the named group, retaining the original RED output. An import/setup exception is not a behavioral RED. Where a new API does not yet exist, the first missing-API RED is recorded separately and the behavioral tests run again as soon as its minimal callable boundary exists, before the corresponding guard/replay/transaction implementation. Do not deliberately remove working safeguards to manufacture a RED; tests that already pass are recorded as preserved regressions, never mislabeled failures.

**Task 1 (one delivered commit boundary):**

- [ ] Write support candidate_case/frame, dynamic-snapshot tests, all-owner process inversion test and raw-outer-transaction fail-before-I/O tests.
- [ ] Run `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/persistence/test_dynamic_snapshot.py tests/integration/test_artifact_recovery_lock_order.py -q -ra -W error`. Snapshot API absence is structural RED; inversion must report `owns_sqlite=True` or bounded deadlock failure on the old existing active/binding/Worker paths.
- [ ] Implement read_snapshot/validated_stream and transaction ownership depth only (Task 1 Step 3 and R2-A persistence prerequisites).
- [ ] Write inventory capture/foreign-object/new-membership/three-retry tests and prefix reducer comparisons before moving recovery/artifact code.
- [ ] Run `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/recovery/test_artifact_inventory.py tests/integration/test_artifact_recovery_lock_order.py -q -ra -W error`. Existing inversion/outer-I/O assertions still fail behaviorally; missing capture API is separately reported.
- [ ] Implement R2-A metadata observation/scope/recovery substitutions and all outer-owner calls; factor unchanged prefix reducers.
- [ ] Run both focused commands again; all assertions must pass without weakening rendezvous/owner checks.
- [ ] Update task documentation, run the common frozen-byte full gate, self-review, make the tested local Task-1 commit, submit committed range for independent review, resolve/reaccept/local-commit scoped fixes, then controller-only reviewed push/equality check.

**Task 2 (one delivered commit boundary, every consumer before producer):**

- [ ] Write R1 raw-bound regression first using only Task-1 APIs and actual SQL corruption.
- [ ] Run `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/persistence/test_candidate_read_bounds.py -q -ra -W error`; expected behavioral RED is `decoded before bounds` before the callback/filesystem guard, for event/aggregate/count through checked/append/read/versioned/validated/fresh-inventory entries, including versioned/validated outer read/write ownership. Draft construction occurs outside the expected loader-error assertion; setup/model errors are not a behavioral RED or GREEN.
- [ ] Implement the R1 wrapper/read-route, same-snapshot versioned-loader preflight and whole-stream count check, then rerun that command GREEN (all 30 bound/entry/ownership combinations).
- [ ] Write schema/accounting/field/head/header corruption tests plus durable fixture builders and historical prefix tests.
- [ ] Run `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/artifacts/test_candidate_schema.py tests/unit/artifacts/test_candidate_journal.py tests/integration/test_candidate_history.py -q -ra -W error`; record structural module RED separately, then at each canonical-schema/replay/link substep rerun the same tests. Before link/index implementation the valid fixture loads but omitted/changed member links must demonstrate behavioral rejection failures.
- [ ] Implement R2-B schema and R2-C prefix authority, then R2-D call-local link/index journal. Preserve exact cap/accounting and canonical field tests.
- [ ] Write R3-A whole consumer and legacy-bypass tests and R3-D reader races/outer ownership before editing runtime consumers/store readers/startup.
- [ ] Run `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/runtime/test_candidate_set_gates.py tests/integration/test_candidate_read_snapshots.py -q -ra -W error`; expected behavioral RED includes subset/selected-legacy admission returning instead of rejecting and old preloaded/inconsistent reader behavior. The journal itself is already present, so a missing journal import is not this RED.
- [ ] Implement every R2-D/F consumer/store/startup insertion in one working stage; update exact recursive trust crossings and candidate-only timestamp serialization.
- [ ] Run all Task-2 focused commands GREEN, including real isolated positive acceptance; then common full gate, documentation, self-review, tested local commit, independent committed review/fix cycle and controller-only reviewed push.

**Task 3 (one delivered commit boundary):**

- [ ] Write public input/idempotency tests, all hostile decoder/value/error tables, and R3-C real deadline/authority/count tests before service code.
- [ ] Run `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/runtime/test_candidate_publication.py tests/integration/test_candidate_publication_transactions.py -q -ra -W error`; record missing API as structural RED only.
- [ ] Implement R2-E composition/boundary/task/decoded/preparation functions; keep the public publisher unavailable until retained install/finalization are both integrated. Run preparation-only tests at the real `_prepare` API: counts, conflicting identity, response fault and durable replay now have behavioral assertions before install/finalize exist.
- [ ] Write the complete R3-B process target/hook table and retained-stage tests before adding `_install_candidate_bytes`, issued-result registry or finalization.
- [ ] Run `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/integration/test_candidate_publication_crashes.py tests/integration/test_candidate_publication_transactions.py -q -ra -W error`; report unsupported stage seams as structural, not falsely as a filesystem regression. Add retained install and issue registry; re-run OSError/reuse/fsync/forged-result tests before finalization, so premature metadata visibility/cleanup errors are behavioral failures.
- [ ] Add `_finalize` and application API integration using the complete body. Run the final-clock/deadline/metadata-fault tests immediately while integrating the guard: exact recorded final sample, whole-set rollback, no late publication and no grant/resource side effects must be proven; never accept a merely successful happy-path call as this gate.
- [ ] Run full R3-B real kill/restart/response-fault matrix, R3-C concurrent authority/count cases, R3-D public status/reader/historical retry extensions and every existing focused regression GREEN.
- [ ] Update documentation, run common frozen-byte full gate, self-review, tested local Task-3 commit, independent committed-range task review and independent BASE..HEAD whole-slice review. Any code fix repeats full gate/local commit/scoped review. Controller alone pushes the reviewed final SHA after manifest/upstream/remote/clean verification. No transport work follows automatically.

### Revision R2-F: concrete composition and consumer insertions

The following constructor is the whole service composition gate, not an assertion left to callers. ArtifactStore internals use local schema/journal imports to avoid the runtime→artifacts→runtime cycle. CandidatePublicationJournal constructor similarly checks exact SQLiteEventStore type/open readable connection, but permits an existing transaction because its methods are database-only.

```python
@dataclass(frozen=True, slots=True)
class CandidatePublicationReceipt:
    request_id: str
    set_id: str
    state: Literal["committed"] = "committed"

@dataclass(frozen=True, slots=True)
class CandidatePublicationStatus:
    state: Literal["absent", "prepared", "committed"]
    request_id: str | None
    set_id: str | None

def __init__(self, *, event_store, scheduler, artifact_store, clock=_utc_now):
    valid = False
    try:
        valid = (type(event_store) is SQLiteEventStore and type(scheduler) is Scheduler and
            type(artifact_store) is ArtifactStore and scheduler.event_store is event_store and
            artifact_store._event_store is event_store and scheduler.recovery.event_store is event_store and
            scheduler.recovery.artifact_store is artifact_store and callable(clock) and
            not event_store.in_transaction)
        if valid:
            event_store._connection.execute("SELECT 1").fetchone()
    except Exception:
        valid = False
    if not valid:
        raise CandidatePublicationError("candidate_publication_unavailable")
    self._store, self._scheduler, self._artifacts, self._clock = event_store, scheduler, artifact_store, clock
    self._journal = CandidatePublicationJournal(event_store)

# CandidatePublicationJournal.__init__ (different owning class)
def __init__(self, event_store):
    valid = False
    try:
        if type(event_store) is SQLiteEventStore:
            event_store._connection.execute("SELECT 1").fetchone()
            valid = True
    except Exception:
        pass
    if not valid:
        raise CandidateJournalError("candidate journal invalid")
    self._event_store = event_store
```

Rename the concrete candidate_records selection body above to `_candidate_records_checked`; expose this wrapper so callers never inherit raw schema/backend exceptions. The entry guard is inside the wrapper but still precedes any grant/path/lock work.

```python
def candidate_records(self, context, members):
    failed = False
    try:
        return self._candidate_records_checked(context, members)
    except Exception:
        failed = True
    if failed:
        raise ArtifactMetadataError("candidate artifacts unavailable")
```

At the beginning of `_load_records`, add this durable SQLite branch; leave the existing adapter/cache path exclusively for non-SQLite generic adapters. Public read_bytes/get_record/list_publications entry first calls `_require_no_transaction`, then resolves metadata by `_load_records`, **then** `_require_published_path`, `_authorize`, and `_read_and_verify`. get_records delegates to list_publications. No earlier list is passed to a later journal snapshot.

```python
if type(self._event_store) is SQLiteEventStore:
    from orchestrator.artifacts.candidate_journal import CandidatePublicationJournal
    failed = False
    try:
        return list(CandidatePublicationJournal(self._event_store).publication_view(digest))
    except Exception:
        failed = True
    if failed:
        raise ArtifactMetadataError("artifact publication history unavailable")
```

Replace admission's current function body with the following, retaining its signature/exception declaration. Imports are local to avoid cycles. A real generic ArtifactStore continues to work through the proven-legacy branch; an inventory-only fake cannot prove absence of preparations.

```python
from orchestrator.artifacts.store import ArtifactStore
from orchestrator.artifacts.candidate_schema import exact_model
failed = False
try:
    if type(artifact_store) is not ArtifactStore:
        raise ValueError("candidate store unavailable")
    task = exact_model(WorkerTask, task)
    result = exact_model(WorkerResult, result)
    validate_worker_result(task, result)
    if result.outcome != "candidate":
        raise ValueError("candidate outcome required")
    return artifact_store.candidate_records(task.context, result.artifacts)
except Exception:
    failed = True
if failed:
    raise CandidateArtifactError("candidate artifacts unavailable")
```

In IsolatedVerifierProcess.verify replace its model_dump revalidation with exact_model(VerificationTask,task), and replace `expected_source` plus `verify_run_artifacts` and per-ref matches with this block. Keep the independent byte-size/grant/read/isolation/evidence code unchanged. Store `by_digest` from the materialized tuple and replace the old `record = matches[-1]` with `record = by_digest[reference.digest]`.

```python
failed = False
try:
    task = exact_model(VerificationTask, task)
    validate_verification_task(task)
    records = artifact_store.candidate_records(task.context, task.candidate_artifacts)
    by_digest = {record.digest: record for record in records}
except Exception:
    failed = True
if failed:
    raise VerifierProcessError("candidate artifacts unavailable")
```

In coordinator.accept_proposal insert this exact call after its existing proposal/binding comparison and before `evidence = self._verifier.verify(...)`:

```python
self._artifact_store.candidate_records(proposal.task.context, proposal.task.candidate_artifacts)
```

In Scheduler.finish_verified_attempt.decide insert the following after the missing/accepted proposal check, before constructing payload or checking prior. In `_validate_success_proofs`, insert the same block after its `proposal` binding check, using the existing `completed_at`; append the two head/version comparisons shown only in that historical loop where `release`/`terminal` already exist.

```python
candidate = CandidatePublicationJournal(self.event_store).require_set(
    proposal.task.context, proposal.task.candidate_artifacts)
if candidate is not None and parse_utc_text(candidate.committed["committed_at"]) > completed_at:
    raise SchedulerError("candidate publication postdates completion")
# Historical _validate_success_proofs loop only:
if candidate is not None:
    heads = {h["stream_type"]: h["version"] for h in candidate.committed["authority_heads"]}
    if heads["scheduler"] >= release.stream_version or heads["run_lifecycle"] >= terminal.stream_version:
        raise SchedulerError("candidate publication postdates terminal authority")
```

In application._validate_verifier_proposals replace the standalone `journal.read_run(run_id)` with the following, preserving the existing `run_id not in runs` rejection afterward. In `_bootstrap`'s checked callback insert `CandidatePublicationJournal(self._event_store).audit_all()` before the first success/provider audit. Add the same audit to ArtifactStore._inventory_metadata inside its owned/reused snapshot before artifact discovery. There is no recursion back to ArtifactStore from that journal audit.

```python
for proposal in journal.read_run(run_id):
    CandidatePublicationJournal(self._event_store).require_set(
        proposal.task.context, proposal.task.candidate_artifacts)
```

Application constructor creates the service immediately before bootstrap when `_artifact_store is not None`; otherwise sets it to None. These public keyword-only wrappers preserve ready/closed semantics without accepting caller models or times:

```python
def publish_worker_candidate(self, *, run_id, node_id, attempt_id, fencing_generation, task_text, payload):
    self._require_ready()
    if self._candidate_publications is None:
        raise CandidatePublicationError("candidate_publication_unavailable")
    return self._candidate_publications.publish(run_id=run_id, node_id=node_id,
        attempt_id=attempt_id, fencing_generation=fencing_generation, task_text=task_text, payload=payload)

def candidate_publication_status(self, *, run_id, node_id, attempt_id, fencing_generation):
    self._require_ready()
    if self._candidate_publications is None:
        raise CandidatePublicationError("candidate_publication_unavailable")
    return self._candidate_publications.status(run_id=run_id, node_id=node_id,
        attempt_id=attempt_id, fencing_generation=fencing_generation)
```

### Revision R3-C: real lock deadline, all recovery owners, capacity and authority changes

`test_lock_wait_crosses_lease_deadline` belongs to `tests/integration/test_candidate_publication_transactions.py`. Both connections are opened before the holder takes its lock. Parent waits for the publisher's **actual append_checked entry** and then advances its shared trusted clock before releasing the real writer. No sleep establishes order and no process waits for a blocked writer's completion while retaining its own writer lock.

```python
def deadline_publisher(database, root, identity, text, raw, expiry, clock, phase, pipe):
    app = ControlPlaneApplication(database, artifact_root=root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
    app._candidate_publications._clock = lambda: datetime.fromtimestamp(clock.value, timezone.utc)
    original = app._event_store.append_checked
    announced = False
    def append_checked(kind, name, key, decide):
        nonlocal announced
        target = "candidate-input-snapshot" if phase == "prepare" else "candidate-commit:"
        if key.startswith(target) and not announced:
            announced = True
            pipe.send("before-writer-lock")
        return original(kind, name, key, decide)
    app._event_store.append_checked = append_checked
    if phase == "final":
        stage = app._artifact_store._stage_candidate_blob
        def staging(**kwargs):
            result = stage(**kwargs)
            pipe.send("installed")
            assert pipe.poll(10) and pipe.recv() == "continue"
            return result
        app._artifact_store._stage_candidate_blob = staging
    pipe.send("ready")
    assert pipe.poll(10) and pipe.recv() == "start"
    try:
        app.publish_worker_candidate(**identity, task_text=text, payload=raw)
        pipe.send("unexpected-success")
    except CandidatePublicationError as error:
        pipe.send(error.code)
    finally:
        app.close()

@pytest.mark.parametrize("phase", ["prepare", "final"])
def test_lock_wait_crosses_lease_deadline(tmp_path, phase):
    case = candidate_case(tmp_path)
    mp = multiprocessing.get_context("spawn")
    clock = mp.Value("d", case.clock.now.timestamp())
    parent, child = mp.Pipe()
    holder = sqlite3.connect(case.database, isolation_level=None)
    process = mp.Process(target=deadline_publisher, args=(case.database, case.root,
        case.identity, case.text, candidate_frame(case.task, (b"one",)),
        case.accepted.lease_expires_at.isoformat(), clock, phase, child))
    try:
        process.start()
        assert parent.poll(10) and parent.recv() == "ready"
        if phase == "prepare":
            holder.execute("BEGIN IMMEDIATE")
            parent.send("start")
        else:
            parent.send("start")
            assert parent.poll(10) and parent.recv() == "installed"
            holder.execute("BEGIN IMMEDIATE")
            parent.send("continue")
        assert parent.poll(10) and parent.recv() == "before-writer-lock"
        clock.value = case.accepted.lease_expires_at.timestamp()
        holder.commit()
        assert parent.poll(10) and parent.recv() == "candidate_publication_inactive"
        process.join(15)
        assert process.exitcode == 0
        state = case.app.candidate_publication_status(**case.identity)
        assert state.state == ("absent" if phase == "prepare" else "prepared")
        assert_held(case)
    finally:
        holder.rollback()
        holder.close()
        if process.pid is not None and process.is_alive():
            process.terminate()
            process.join(15)
        parent.close()
        child.close()
        case.app.close()
```

The prepare variant rendezvous is on the **first** writer (`candidate-input-snapshot`), so the expired sample is read after the lock the operation actually waits for; the final variant selects candidate-commit after durable installation.

Extend the existing Task-1 `lock_order_process` with the following `recovery_operation` adapter. Pass serialized request/decision/contract into the child; rebuild with their real model types before the rendezvous. Add `operation` to its arguments (default `"active"`), invoke this adapter in its nonpublisher branch, and parameterize the existing parent test over the keys below. `plan_attempt_recovery` on a still-active fixture intentionally raises SchedulerError only **after** its real recovery callback has run; assert the observation hook was reached before accepting that result.

```python
RECOVERY_OPERATIONS = ("active", "binding", "route", "recovery_plan", "worker_prepare", "worker_admit", "recover", "terminal")

def recovery_operation(app, operation, identity, now, request, decision, contract, proposal_hash):
    from orchestrator.models import ModelGatewayFailure
    scheduler = app._scheduler
    accepted = next(e for e in scheduler.event_store.read_stream("scheduler", "global")
                    if e.event_type == "RoutingDecisionAccepted" and e.attempt_id == identity["attempt_id"])
    accepted_at = datetime.fromisoformat(accepted.payload["accepted_at"])
    expires_at = datetime.fromisoformat(accepted.payload["lease_expires_at"])
    admission = dict(accepted.payload["verification_admission"])
    if operation == "active":
        return scheduler.resolve_active_attempt(**identity, as_of=now)
    if operation == "binding":
        return scheduler.resolve_verification_binding(**identity, as_of=now)
    if operation == "route":
        return scheduler.accept_routing(request, decision, accepted_at=accepted_at, lease_expires_at=expires_at,
            input_manifest_hash=admission["input_manifest_hash"], verification_contract=admission["verification_contract"],
            required_check_ids=tuple(admission["required_check_ids"]))
    if operation == "recovery_plan":
        return scheduler.plan_attempt_recovery(**identity, source_decision=decision, contract=contract,
            failure=ModelGatewayFailure(code="rate_limited", phase="provider",
                outcome="known_failure", retryable=True, http_status=429))
    if operation == "worker_prepare":
        app._worker_inputs._clock = lambda: now
        return app.prepare_worker_task(**identity, task_text="implement a small change")
    if operation == "worker_admit":
        return app.accept_routing_with_input(request, decision, task_text="implement a small change",
            accepted_at=accepted_at, lease_expires_at=expires_at)
    if operation == "recover":
        return scheduler.recovery.recover(identity["run_id"])
    if operation == "terminal":
        reservation = scheduler.resolve_active_attempt(**identity, as_of=now).reservation
        return scheduler.finish_verified_attempt(**identity, task_sha256=proposal_hash, completed_at=now,
            usage=UsageRecord(run_id=identity["run_id"], reservation_id=reservation.reservation_id,
                settlement_key="lock-order-fixture", currency=reservation.currency, cost_minor=0, status="committed"))
    raise AssertionError(operation)
```

Route/admit probes use the actual stored acceptance times/admission, so they reach recovery rather than fail an unrelated request comparison. The observation assertion proves each tested operation attempted the real digest lock.

Use Task-2 committed fixture/proposal only for the Task-3 terminal extension; Task-1 terminal probe uses the already existing `tests.support.attempt_acceptance.proposal_for_attempt` with a real matching generic publication. Startup is a separate child variant: monkeypatch ArtifactStore._digest_lock at class level **before** constructing the application, announce before-digest from that wrapper, and release the existing writer before waiting for startup readiness. Do not construct the startup reader first, which would miss the target path.

For the exact 1024 request cap, add this slow real-database helper to `tests/support/candidate_publication.py`; the two test wrappers below call it from their owning stages. It uses public run_setup/routed_pair, not reduced constants or forged prepared rows. One shared zero-byte object bounds disk use while each Attempt has distinct deterministic metadata.

```python
def run_capacity_case(tmp_path, *, publisher):
    from tests.unit.lifecycle.test_scheduler import run_setup, routed_pair
    from orchestrator.lifecycle import NodeSpec
    from types import SimpleNamespace
    app = ControlPlaneApplication(tmp_path / "cap.db", artifact_root=tmp_path / "objects",
        limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4,
                                 provider_active_attempts=8, tool_active_attempts=4))
    try:
        nodes = tuple(NodeSpec(node_id=f"node-{i}", role="coder", max_attempts=1,
                              planning_contract_hash="sha256:" + "e" * 64) for i in range(1025))
        reg, config, lifecycle, policy = run_setup(app._event_store, nodes=nodes,
                                                 max_agents=1025, freeze_contracts=True)
        start = datetime.now(timezone.utc)
        app._worker_inputs._clock = lambda: start + timedelta(seconds=1)
        if publisher:
            app._candidate_publications._clock = lambda: start + timedelta(seconds=1)
        first = None
        def publish(identity, task, accepted, raw):
            if publisher:
                return app.publish_worker_candidate(**identity, task_text="implement a small change", payload=raw)
            fixture = SimpleNamespace(app=app, store=app._event_store, artifacts=app._artifact_store,
                identity=identity, task=task, accepted=accepted,
                clock=SimpleNamespace(now=start + timedelta(seconds=1)))
            return append_candidate_fixture(fixture, (b"",), commit=True)
        for i in range(1025):
            node = lifecycle.replay("run-1").node(f"node-{i}")
            request, decision = routed_pair(reg, config, policy, node_id=node.spec.node_id,
                                            contract_hash=node.spec.planning_contract_hash)
            accepted = app.accept_routing_with_input(request, decision, task_text="implement a small change",
                accepted_at=start, lease_expires_at=start + timedelta(hours=1))
            identity = dict(run_id="run-1", node_id=node.spec.node_id, attempt_id=request.attempt_id,
                            fencing_generation=1)
            task = app.prepare_worker_task(**identity, task_text="implement a small change")
            raw = candidate_frame(task, (b"",))
            if i == 1024:
                before = event_counts(app._event_store)
                with pytest.raises(CandidatePublicationError if publisher else EventContractError) as caught:
                    publish(identity, task, accepted, raw)
                if publisher:
                    assert caught.value.code == "candidate_publication_too_large"
                assert event_counts(app._event_store) == before
                break
            receipt = publish(identity, task, accepted, raw)
            if first is None:
                first = (identity, raw, receipt)
            app._scheduler.finish_attempt(**identity, completed_at=start + timedelta(seconds=2),
                                          outcome="failed", known_no_effect=True)
        before = event_counts(app._event_store)
        if publisher:
            assert app.publish_worker_candidate(**first[0], task_text="implement a small change", payload=first[1]) == first[2]
        else:
            assert CandidatePublicationJournal(app._event_store).lookup(**first[0]) == first[2]
        assert event_counts(app._event_store) == before
        assert before[("candidate_publications", "run-1")] == 2048
    finally:
        app.close()
```

Import EventContractError from persistence.events in the support module. These exact wrappers ensure each stage actually runs its owned path; the fixture branch claims durable replay/count rejection, not a product API retry:

```python
# tests/unit/artifacts/test_candidate_journal.py (Task 2)
def test_journal_request_count_cap_with_real_history(tmp_path):
    run_capacity_case(tmp_path, publisher=False)

# tests/integration/test_candidate_publication_transactions.py (Task 3)
def test_1024_committed_requests_allow_retry_but_not_1025(tmp_path):
    run_capacity_case(tmp_path, publisher=True)
```

Authority-change adapters are actual persisted changes. In `test_candidate_publication_transactions.py`, insert them via the existing hook installer's `after_prepare_commit` point (before staging/finalization) and via `after_final_commit` point (history remains). Mutations must use a **second** open connection/application so they are not rolled back with the candidate transaction; final-commit hook must run after actual commit, never while its write lock is held.

```python
def change_authority(case, other, kind):
    if kind == "cancel":
        other._scheduler.lifecycle.request_cancel("run-1", reason_code="candidate-fixture-cancel")
    elif kind == "expire":
        case.clock.now = case.accepted.lease_expires_at
    elif kind == "graph":
        from orchestrator.lifecycle import NodeSpec
        from orchestrator.routing import compile_node_contract
        state = other._scheduler.lifecycle.replay("run-1")
        contract = compile_node_contract(run_id="run-1", node_id="later-node", role="coder",
            task_text=case.text, config=case.config, registry=case.registry, policy_manifest=case.policy,
            context_tokens=1000, max_output_tokens=500, required_capabilities=("text",))
        other._scheduler.lifecycle.append_nodes("run-1", (NodeSpec(node_id="later-node", role="coder",
            planning_contract_hash=contract.contract_hash, planning_contract=contract, max_attempts=1),),
            expected_graph_version=state.graph_version, idempotency_key="candidate-fixture-graph",
            policy_manifest=case.policy)
    elif kind in ("release", "replacement"):
        other._scheduler.finish_attempt(**case.identity, completed_at=case.clock.now,
                                        outcome="failed", known_no_effect=True)
        if kind == "replacement":
            from orchestrator.models import ModelGatewayFailure
            plan = other._scheduler.plan_attempt_recovery(**case.identity,
                source_decision=case.decision, contract=case.contract,
                failure=ModelGatewayFailure(code="rate_limited", phase="provider",
                    outcome="known_failure", retryable=True, http_status=429))
            assert plan.authorization is not None
            request, decision = routed_pair(case.registry, case.config, case.policy,
                node_id=case.identity["node_id"], attempt=2, contract_hash=case.contract.contract_hash,
                recovery_action="same_model_retry", recovery_authorization=plan.authorization,
                prior_decision_hash=case.decision.decision_hash, previous_model_id="model-1",
                failure_category="transient", retry_level=1)
            other.accept_routing_with_input(request, decision, task_text=case.text,
                accepted_at=case.clock.now, lease_expires_at=case.accepted.lease_expires_at)
    else:
        raise AssertionError(kind)

@pytest.mark.parametrize("kind", ["cancel", "expire", "graph", "release", "replacement"])
@pytest.mark.parametrize("when", ["before_prepare", "after_prepare_commit", "after_final_commit"])
def test_authority_change_cannot_partially_publish(tmp_path, monkeypatch, kind, when):
    case = candidate_case(tmp_path)
    other = ControlPlaneApplication(case.database, artifact_root=case.root, limits=ConcurrencyLimits(system_active_attempts=8, run_active_attempts=4, provider_active_attempts=8, tool_active_attempts=4))
    fired = []
    def hit(name):
        if name == when and not fired:
            fired.append(name)
            change_authority(case, other, kind)
    try:
        case.app._candidate_publications._clock = lambda: case.clock.now
        install_candidate_hooks(case.app, monkeypatch, hit)
        raw = candidate_frame(case.task, (b"first", b"second"))
        if when != "after_final_commit":
            with pytest.raises(CandidatePublicationError) as caught:
                case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
            assert caught.value.code == "candidate_publication_inactive"
            assert case.app.candidate_publication_status(**case.identity).state == (
                "absent" if when == "before_prepare" else "prepared")
        else:
            receipt = case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw)
            assert receipt.state == "committed"
            before = event_counts(case.store)
            assert case.app.publish_worker_candidate(**case.identity, task_text=case.text, payload=raw) == receipt
            assert event_counts(case.store) == before
        assert fired == [when]
    finally:
        other.close()
        case.app.close()
```

Generation/Agent/reservation isolated corruption is not a valid authority transition; R3-F below alters raw witnessed history and expects unavailable/integrity. The replacement branch above uses a real failed Attempt, persisted recovery plan and a newly accepted generation, so it tests actual fence replacement on both sides of publication instead of labelling a corrupted row a valid transition.

- [ ] All three stages' source/test/doc scopes are explicit; Stage 2's gates precede Stage 3's producer.
- [ ] Every spec section is mapped in planning-report.md and every Review Focus has concrete test code plus its owning task.
- [ ] All authority checks use the exact same store/connection and full original admission; no private result or receipt creates durable authority.
- [ ] Dynamic discovery and all links are in one database snapshot; metadata materializes before snapshot closure and filesystem/grants; caller transaction ownership is preserved.
- [ ] The five historical prefixes use actual reducers and within-stream ordering; there is no inferred cross-stream event order.
- [ ] Recovery inventory retries are bounded at three and occur outside SQLite; current pending content observations do not authorize deletion/read/success.
- [ ] All source gates, independent task and final whole-slice review, source-fix reacceptance, committed-byte binding and controller push verification remain required.
- [ ] No implementation/test/build/acceptance result is claimed by this planning document. Controller review is next; the selected execution method remains Subagent-driven.
