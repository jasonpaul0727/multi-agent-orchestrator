# Persistence, Cost, Observability, and Testing Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Build the local SQLite foundation that persists orchestration facts, accounts for model and tool cost, exposes safe operational observations, and can recover and verify state after concurrency and crash scenarios.

**Architecture:** Keep the control plane behind small Python interfaces. SQLite WAL is the V1 EventStore implementation; append-only event streams are authoritative, while snapshots, budget ledgers, Artifact Store metadata, projections, logs, and metrics are derived views. Workers and gateways receive interfaces, never database handles, so later lifecycle, routing, and security work can depend on stable contracts.

**Tech Stack:** Python 3.12+, standard-library sqlite3 and hashlib, Pydantic v2 for validated boundary objects, pytest, pytest-asyncio, and Hypothesis. The persistence interfaces must not depend on a specific web framework, ORM, or telemetry SDK.

---

## Scope and file map

This plan implements only the persistence, cost, observability, and testing foundation. Lifecycle, model routing, security policy, isolation, and CLI/MCP behavior remain separate implementation slices that consume the interfaces created here.

Files created by this plan:

- Create: pyproject.toml
- Create: src/orchestrator/__init__.py
- Create: src/orchestrator/identifiers.py
- Create: src/orchestrator/persistence/events.py
- Create: src/orchestrator/persistence/sqlite_event_store.py
- Create: src/orchestrator/persistence/snapshots.py
- Create: src/orchestrator/artifacts/store.py
- Create: src/orchestrator/budget/models.py
- Create: src/orchestrator/budget/ledger.py
- Create: src/orchestrator/observability/models.py
- Create: src/orchestrator/observability/redaction.py
- Create: src/orchestrator/observability/sink.py
- Create: src/orchestrator/observability/projections.py
- Create: src/orchestrator/recovery/bootstrap.py
- Create: tests/conftest.py
- Create: tests/unit/persistence/test_event_store.py
- Create: tests/unit/persistence/test_snapshots.py
- Create: tests/unit/artifacts/test_store.py
- Create: tests/unit/budget/test_ledger.py
- Create: tests/unit/observability/test_redaction.py
- Create: tests/unit/observability/test_projections.py
- Create: tests/integration/test_recovery.py
- Create: tests/integration/test_crash_matrix.py
- Create: tests/contract/test_cross_spec_events.py

The implementation must preserve the already approved event, security, routing, and lifecycle names: Run, Node, Attempt, PolicyDecision, CapabilityGrant, ApprovalGrant, EffectIntentRecorded, RoutingRequest, RoutingDecision, OutcomeUnknown, and AwaitingReconciliation.

### Task 1: Bootstrap the package and test harness

**Files:**

- Create: pyproject.toml
- Create: src/orchestrator/__init__.py
- Create: src/orchestrator/identifiers.py
- Create: tests/conftest.py
- Create: tests/unit/test_bootstrap.py

- [ ] **Step 1: Write the failing package test**

~~~python
from orchestrator.identifiers import new_id


def test_new_id_is_non_empty_and_stable_as_text():
    value = new_id()
    assert isinstance(value, str)
    assert len(value) == 36
~~~

- [ ] **Step 2: Run the test and verify the failure**

Run: python -m pytest tests/unit/test_bootstrap.py -q

Expected: FAIL because the orchestrator package and identifiers module do not exist.

- [ ] **Step 3: Add the minimal package metadata and identifier helper**

~~~toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "multi-agent-orchestrator"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "pydantic>=2.7,<3",
]

[project.optional-dependencies]
dev = [
  "build>=1.2,<2",
  "coverage>=7.5,<8",
  "hypothesis>=6.100,<7",
  "pytest>=8,<9",
  "pytest-asyncio>=0.23,<1",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
addopts = "-q"
~~~

~~~python
# src/orchestrator/identifiers.py
from uuid import UUID, uuid4


def new_id() -> str:
    value: UUID = uuid4()
    return str(value)
~~~

~~~python
# tests/conftest.py
from pathlib import Path
import pytest


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "control.db"
~~~

- [ ] **Step 4: Run the bootstrap test**

Run: python -m pytest tests/unit/test_bootstrap.py -q

Expected: PASS.

- [ ] **Step 5: Commit the bootstrap**

~~~bash
git add pyproject.toml src/orchestrator tests/unit/test_bootstrap.py
git commit -m "build: bootstrap orchestrator package"
~~~

### Task 2: Define event objects and the SQLite EventStore

**Files:**

- Create: src/orchestrator/persistence/__init__.py
- Create: src/orchestrator/persistence/events.py
- Create: src/orchestrator/persistence/sqlite_event_store.py
- Create: tests/unit/persistence/test_event_store.py

- [ ] **Step 1: Write tests for append, replay, CAS, and idempotency**

~~~python
from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore, StaleStream


def test_append_assigns_monotonic_stream_versions(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    first = store.append(
        "run", "run-1", expected_version=0,
        events=[EventDraft("RunCreated", {"run_id": "run-1"})],
        idempotency_key="create-run-1",
    )
    second = store.append(
        "run", "run-1", expected_version=1,
        events=[EventDraft("InputAccepted", {"run_id": "run-1"})],
        idempotency_key="accept-run-1",
    )
    assert [event.stream_version for event in first + second] == [1, 2]


def test_stale_expected_version_does_not_append(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    store.append(
        "run", "run-1", 0,
        [EventDraft("RunCreated", {})],
        "create-run-1",
    )
    try:
        store.append("run", "run-1", 0, [EventDraft("InputAccepted", {})], "stale")
    except StaleStream:
        pass
    else:
        raise AssertionError("stale write was accepted")
    assert len(store.read_stream("run", "run-1")) == 1


def test_repeating_an_idempotency_key_returns_original_events(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    draft = [EventDraft("RunCreated", {"run_id": "run-1"})]
    first = store.append("run", "run-1", 0, draft, "same-key")
    repeated = store.append("run", "run-1", 0, draft, "same-key")
    assert repeated == first
    assert len(store.read_stream("run", "run-1")) == 1
~~~

- [ ] **Step 2: Run the tests and verify the expected failures**

Run: python -m pytest tests/unit/persistence/test_event_store.py -q

Expected: FAIL because EventDraft, SQLiteEventStore, and StaleStream are not defined.

- [ ] **Step 3: Implement validated event models**

~~~python
# src/orchestrator/persistence/events.py
from datetime import datetime, timezone
from pydantic import BaseModel, ConfigDict, Field
from orchestrator.identifiers import new_id


class EventDraft(BaseModel):
    model_config = ConfigDict(frozen=True)
    event_type: str = Field(min_length=1)
    payload: dict


class StoredEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    event_id: str
    stream_type: str
    stream_id: str
    stream_version: int
    event_type: str
    schema_version: int
    occurred_at: datetime
    payload: dict
    payload_hash: str
    idempotency_key: str
    correlation_id: str | None = None
    causation_id: str | None = None
~~~

- [ ] **Step 4: Implement SQLite schema and transactional append**

The schema must create events, stream_versions, and idempotency_records with unique constraints on event ID, stream/version, and stream/idempotency key. Use sqlite3.Row, PRAGMA journal_mode=WAL, PRAGMA foreign_keys=ON, and BEGIN IMMEDIATE for append. Canonicalize JSON with sorted keys before hashing.

~~~python
# src/orchestrator/persistence/sqlite_event_store.py
class StaleStream(RuntimeError):
    pass


class SQLiteEventStore:
    def __init__(self, path):
        self._connection = open_connection(path)
        initialize_schema(self._connection)

    def append(self, stream_type, stream_id, expected_version, events, idempotency_key):
        raise NotImplementedError

    def read_stream(self, stream_type, stream_id, after_version=0):
        raise NotImplementedError

    def current_version(self, stream_type, stream_id):
        raise NotImplementedError
~~~

The implementation must return the original StoredEvent objects for an idempotency retry, reject different payloads with an existing key, and raise StaleStream before inserting any event when the expected version is wrong.

- [ ] **Step 5: Run the event store tests**

Run: python -m pytest tests/unit/persistence/test_event_store.py -q

Expected: PASS with three tests.

- [ ] **Step 6: Commit the event store**

~~~bash
git add src/orchestrator/persistence tests/unit/persistence/test_event_store.py
git commit -m "feat: add sqlite event store"
~~~

### Task 3: Add snapshots and deterministic recovery

**Files:**

- Create: src/orchestrator/persistence/snapshots.py
- Create: src/orchestrator/recovery/__init__.py
- Create: src/orchestrator/recovery/bootstrap.py
- Create: tests/unit/persistence/test_snapshots.py
- Create: tests/integration/test_recovery.py

- [ ] **Step 1: Write snapshot and replay tests**

~~~python
def test_snapshot_is_rejected_when_event_version_or_hash_is_wrong(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    snapshot_store = SnapshotStore(store)
    snapshot_store.save("run", "run-1", version=2, state={"status": "Running"})
    snapshot_store.tamper_for_test("run", "run-1", version=2, state={"status": "Delivered"})
    assert snapshot_store.load_valid("run", "run-1") is None


def test_restart_replays_events_after_latest_valid_snapshot(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    store.append("run", "run-1", 0, [EventDraft("RunCreated", {"run_id": "run-1"})], "create")
    SnapshotStore(store).save("run", "run-1", version=1, state={"status": "Intake"})
    store.append("run", "run-1", 1, [EventDraft("InputAccepted", {"run_id": "run-1"})], "accept")
    recovered = recover_control_plane(tmp_path / "events.db", reducers={"run": apply_run_event}, projections=[])
    assert recovered["run"]["run-1"]["status"] == "Planning"


def apply_run_event(state, event):
    next_state = dict(state)
    if event.event_type == "RunCreated":
        next_state["status"] = "Intake"
    if event.event_type == "InputAccepted":
        next_state["status"] = "Planning"
    return next_state
~~~

- [ ] **Step 2: Implement snapshot hashing and recovery ordering**

SnapshotStore must persist aggregate type, aggregate ID, event version, state hash, schema version, and source event ID. Recovery must validate the snapshot, replay the tail, rebuild projections, mark expired leases and unknown effects, and stop new scheduling if event or budget invariants fail.

The tamper_for_test helper used by the test changes only a temporary test row; it is test-only and is not part of the production SnapshotStore API.

~~~python
class RecoveryFailure(RuntimeError):
    pass


def recover_control_plane(database_path, reducers, projections):
    store = SQLiteEventStore(database_path)
    verify_event_streams(store)
    snapshots = SnapshotStore(store)
    state = load_verified_snapshots(snapshots, reducers)
    replay_into_state(store, state, reducers)
    rebuild_projections(state, projections)
    verify_budget_and_security_invariants(state)
    return state
~~~

- [ ] **Step 3: Run recovery tests**

Run: python -m pytest tests/unit/persistence/test_snapshots.py tests/integration/test_recovery.py -q

Expected: PASS; corrupt snapshots must be ignored and event-chain or budget invariant failures must raise RecoveryFailure.

- [ ] **Step 4: Commit snapshots and recovery**

~~~bash
git add src/orchestrator/persistence/snapshots.py src/orchestrator/recovery tests/unit/persistence/test_snapshots.py tests/integration/test_recovery.py
git commit -m "feat: add snapshot validation and recovery"
~~~

### Task 4: Implement the content-addressed Artifact Store

**Files:**

- Create: src/orchestrator/artifacts/__init__.py
- Create: src/orchestrator/artifacts/store.py
- Create: tests/unit/artifacts/test_store.py

- [ ] **Step 1: Write hash, atomic publish, and access-scope tests**

~~~python
def test_publish_returns_content_hash_and_is_atomic(tmp_path):
    artifacts = ArtifactStore(tmp_path / "artifacts")
    record = artifacts.publish_bytes(b"result", source={"run_id": "run-1"})
    assert record.digest.startswith("sha256:")
    assert artifacts.read_bytes(record.digest) == b"result"


def test_publish_rejects_modified_content_after_hashing(tmp_path):
    artifacts = ArtifactStore(tmp_path / "artifacts")
    record = artifacts.publish_bytes(b"result", source={"run_id": "run-1"})
    artifacts.corrupt_for_test(record.digest)
    try:
        artifacts.read_bytes(record.digest)
    except ArtifactIntegrityError:
        pass
    else:
        raise AssertionError("corrupt artifact was returned")
~~~

- [ ] **Step 2: Implement private staging and atomic publication**

ArtifactStore must write to a private temporary path, fsync the content, compute SHA-256, atomically rename into the content-addressed location, then append metadata through EventStore. Reads must re-hash content and enforce the caller’s scope before returning bytes. The store must never expose the database or worker temporary directory.

The corrupt_for_test helper used by the test mutates only a temporary test artifact; it is test-only and is not part of the production ArtifactStore API.

- [ ] **Step 3: Run artifact tests**

Run: python -m pytest tests/unit/artifacts/test_store.py -q

Expected: PASS.

- [ ] **Step 4: Commit the Artifact Store**

~~~bash
git add src/orchestrator/artifacts tests/unit/artifacts/test_store.py
git commit -m "feat: add content addressed artifact store"
~~~

### Task 5: Implement budget reservations and cost settlement

**Files:**

- Create: src/orchestrator/budget/__init__.py
- Create: src/orchestrator/budget/models.py
- Create: src/orchestrator/budget/ledger.py
- Create: tests/unit/budget/test_ledger.py

- [ ] **Step 1: Write integer-cost and reservation tests**

~~~python
def test_cost_rounds_up_and_reservation_is_atomic(tmp_path):
    ledger = BudgetLedger(SQLiteEventStore(tmp_path / "events.db"), run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=200)})
    estimate = ledger.estimate(
        input_tokens=101, output_tokens=9,
        input_price_minor_per_million=1,
        output_price_minor_per_million=3,
        currency="USD",
    )
    assert estimate.amount_minor == 1
    reservation = ledger.reserve("run-1", estimate, token_limit=200)
    assert reservation.reserved_minor == 1


def test_unknown_result_keeps_worst_case_reservation(tmp_path):
    ledger = BudgetLedger(SQLiteEventStore(tmp_path / "events.db"), run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)})
    reservation = ledger.reserve("run-1", estimate_with_worst_case(10), token_limit=100)
    ledger.mark_unknown(reservation.reservation_id)
    assert ledger.available("run-1").unknown_minor == reservation.reserved_minor


def test_settlement_is_idempotent(tmp_path):
    ledger = BudgetLedger(SQLiteEventStore(tmp_path / "events.db"), run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)})
    reservation = ledger.reserve("run-1", estimate_with_worst_case(10), token_limit=100)
    first = ledger.commit_usage(reservation.reservation_id, usage={"input": 2, "output": 3})
    second = ledger.commit_usage(reservation.reservation_id, usage={"input": 2, "output": 3})
    assert second == first


def estimate_with_worst_case(tokens):
    return CostEstimate(amount_minor=tokens, currency="USD", token_limit=tokens, snapshot_id="test")
~~~

- [ ] **Step 2: Implement typed cost models**

Define RunLimit, CostEstimate, BudgetReservation, UsageRecord, and BudgetBalance with integer minor units, currency, tokenizer/price snapshot IDs, reservation version, and status values reserved, committed, released, or unknown. Reject negative values and currency mismatches.

- [ ] **Step 3: Implement ledger transactions**

BudgetLedger must append BudgetReserved, UsageObserved, CostCommitted, BudgetReleased, and CostAdjusted events through one EventStore transaction. A reservation cannot exceed the current Run envelope; unknown results keep the worst-case reservation until reconciliation; repeated settlement keys return the first settlement.

- [ ] **Step 4: Run budget tests**

Run: python -m pytest tests/unit/budget/test_ledger.py -q

Expected: PASS with rounding, unknown-result, duplicate-settlement, and budget-exhaustion coverage.

- [ ] **Step 5: Commit budget and cost accounting**

~~~bash
git add src/orchestrator/budget tests/unit/budget/test_ledger.py
git commit -m "feat: add budget and cost ledger"
~~~

### Task 6: Add redacted observations and projections

**Files:**

- Create: src/orchestrator/observability/__init__.py
- Create: src/orchestrator/observability/models.py
- Create: src/orchestrator/observability/redaction.py
- Create: src/orchestrator/observability/sink.py
- Create: src/orchestrator/observability/projections.py
- Create: tests/unit/observability/test_redaction.py
- Create: tests/unit/observability/test_projections.py

- [ ] **Step 1: Write redaction and projection tests**

~~~python
def test_redactor_removes_secret_values_and_protected_paths():
    redacted = Redactor(secret_values=["sk-secret"], protected_paths=["/workspace/private"])
    result = redacted.clean({"token": "sk-secret", "path": "/workspace/private/a.txt"})
    assert result == {"token": "[REDACTED]", "path": "[PROTECTED_PATH]"}


def test_projection_reports_event_version_and_lag():
    projection = RunProjection()
    projection.apply(event("RunCreated", version=1, payload={"run_id": "run-1"}))
    assert projection.read("run-1").event_version == 1
    assert projection.lag(current_stream_version=3, stream_id="run-1") == 2
~~~

- [ ] **Step 2: Implement deterministic redaction**

Redactor must recursively handle dicts, lists, strings, and exception text; replace known secret values, secret-like headers, authorization fields, and protected path segments without changing event IDs or hashes already committed. Redaction failures must fail the observation write and emit no raw fallback.

- [ ] **Step 3: Implement ObservationSink and projections**

ObservationSink accepts structured LogRecord, TraceSpan, and MetricSample objects. Run, budget, cost, approval, and audit projections consume StoredEvent objects in order and expose the last applied event version. A projection cannot mutate EventStore or make scheduling decisions.

- [ ] **Step 4: Run observation tests**

Run: python -m pytest tests/unit/observability -q

Expected: PASS; raw secrets and protected paths must not appear in logs, spans, metrics, or projection explanations.

- [ ] **Step 5: Commit observations**

~~~bash
git add src/orchestrator/observability tests/unit/observability
git commit -m "feat: add redacted observations and projections"
~~~

### Task 7: Connect persistence contracts to lifecycle and security events

**Files:**

- Create: tests/contract/test_cross_spec_events.py
- Modify: src/orchestrator/persistence/events.py
- Modify: src/orchestrator/budget/ledger.py
- Modify: src/orchestrator/observability/projections.py

- [ ] **Step 1: Write cross-spec contract tests**

~~~python
from types import SimpleNamespace


def test_effect_intent_precedes_external_receipt():
    events = recorded_events_for_external_mutation()
    types = [item.event_type for item in events]
    assert types.index("EffectIntentRecorded") < types.index("EffectReceiptRecorded")


def recorded_events_for_external_mutation():
    return [event("EffectIntentRecorded"), event("EffectReceiptRecorded")]


def test_approval_consumption_and_budget_reservation_share_attempt_identity():
    events = recorded_events_for_approved_attempt()
    attempt_ids = {item.attempt_id for item in events if item.attempt_id}
    assert len(attempt_ids) == 1
    assert has_event_pair(events, "ApprovalGrantConsumed", "BudgetReserved")


def recorded_events_for_approved_attempt():
    return [
        event("ApprovalGrantConsumed", attempt_id="attempt-1"),
        event("BudgetReserved", attempt_id="attempt-1"),
    ]


def event(event_type, attempt_id=None):
    return SimpleNamespace(event_type=event_type, attempt_id=attempt_id)


def has_event_pair(events, first_type, second_type):
    types = [item.event_type for item in events]
    return types.index(first_type) < types.index(second_type)
~~~

- [ ] **Step 2: Add event validation for required security and lifecycle fields**

StoredEvent validation must require run_id, node_id, attempt_id, fencing_generation, and causation metadata on security, budget, routing, and side-effect events. It must reject a receipt without a prior intent in the same causal stream and reject an ApprovalGrantConsumed event whose attempt identity differs from the reservation.

- [ ] **Step 3: Run the contract tests**

Run: python -m pytest tests/contract/test_cross_spec_events.py -q

Expected: PASS; invalid ordering and mismatched identities must be rejected before event append.

- [ ] **Step 4: Commit cross-spec contracts**

~~~bash
git add src/orchestrator/persistence/events.py src/orchestrator/budget/ledger.py src/orchestrator/observability/projections.py tests/contract/test_cross_spec_events.py
git commit -m "test: enforce lifecycle and security event contracts"
~~~

### Task 8: Exercise concurrency, crash recovery, and the acceptance matrix

**Files:**

- Create: tests/integration/test_crash_matrix.py
- Modify: tests/integration/test_recovery.py
- Create: tests/integration/test_concurrency.py
- Create: tests/acceptance/test_persistence_foundation.py

- [ ] **Step 1: Write fault-injection tests**

Inject failures at event append, after budget reservation, before and after EffectIntentRecorded, before receipt, after receipt, before settlement, and after ApprovalGrant consumption. For each injection, restart from the same database and assert no duplicate side effect, duplicate charge, lost evidence, or illegal state.

- [ ] **Step 2: Write concurrency tests**

Use two SQLite connections and barriers to race the same expected stream version, budget reservation, idempotency key, and approval consumption. Assert exactly one winner, one stale/idempotent result, and no negative balance.

- [ ] **Step 3: Add the acceptance flow**

The acceptance test must create a Run, append a planning event, reserve a model call, publish an artifact, record an approval-gated effect, settle usage, rebuild projections, and recover after a forced process interruption. It must inspect the event chain and budget totals rather than only checking a final boolean.

- [ ] **Step 4: Run the full foundation suite**

Run: python -m pytest tests/unit tests/contract tests/integration tests/acceptance -q

Expected: PASS with zero failures and no unclosed SQLite connections.

- [ ] **Step 5: Commit the acceptance suite**

~~~bash
git add tests/integration tests/acceptance
git commit -m "test: cover persistence crash and concurrency matrix"
~~~

### Task 9: Package checks and implementation handoff

**Files:**

- Modify: pyproject.toml
- Modify: README.md
- Create: .gitignore

- [ ] **Step 1: Add reproducible developer commands**

Add pytest, coverage, and package build commands to pyproject.toml. The required commands are:

~~~bash
python -m coverage run -m pytest -q
python -m coverage report --fail-under=90
python -m compileall src
python -m build
~~~

- [ ] **Step 2: Document the foundation**

Update README.md with the implemented module boundaries, SQLite control-directory requirement, event-source-of-truth rule, and the command used to run the foundation tests. Do not document model provider credentials or claim the full orchestrator is production ready.

- [ ] **Step 3: Run final checks**

Run: python -m coverage run -m pytest -q

Expected: PASS.

Run: python -m coverage report --fail-under=90

Expected: exit code 0 with at least 90 percent coverage.

Run: python -m compileall src

Expected: exit code 0.

Run: python -m build

Expected: wheel and source archive are produced in dist/.

- [ ] **Step 4: Commit packaging and documentation**

~~~bash
git add pyproject.toml README.md .gitignore
git commit -m "chore: package persistence foundation"
~~~

## Verification checklist

Before handing the implementation to the next plan, verify:

- EventStore is the only source of truth; projections and snapshots replay correctly.
- SQLite uses WAL, explicit transactions, unique stream versions, and idempotency keys.
- Cost uses integer minor units and frozen price/estimator snapshots.
- Unknown results retain worst-case reservations and never auto-retry side effects.
- Artifact content is hash-verified and scoped through Gateway interfaces.
- Logs, traces, metrics, and projections are redacted and version-aware.
- Cross-spec events preserve attempt, fencing, causation, EffectIntent, approval, and budget identities.
- Crash and concurrency tests prove no duplicate side effects or charges.
- Full pytest, compileall, and package build commands pass from a clean checkout.
