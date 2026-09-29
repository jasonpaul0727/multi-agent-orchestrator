# Provider Call Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an append-only, host-only, evidence-verified path that resolves eligible unknown Provider calls and settles their held budget/Attempt exactly once without replaying the original call.

**Architecture:** The Gateway persists and transmits a per-dispatch OpenAI Responses correlation ID. The Provider journal validates a bounded reconciliation event chain; a host service accepts raw evidence only through an injected Provider verifier and requires host-verified Attempt termination before it records proof. The service applies the proof through the existing Scheduler transaction, then records a replay marker so process death across the two streams is recoverable. No production-grade exact OpenAI evidence source exists in this repository today, so the default verifier remains unavailable and production reconciliation stays fail-closed until one is configured.

**Tech Stack:** Python 3.12, Pydantic 2, SQLite event store with `append_checked`/stream-version CAS, `UsageRecord`/`BudgetLedger`, existing `Scheduler.reconcile_attempt`, `SandboxTerminationReceipt`, pytest, coverage.

**Spec:** `docs/superpowers/specs/2026-09-29-provider-call-reconciliation-design.md`

## Global Constraints

- Missing, unsupported, ambiguous, stale, or mismatched evidence remains unresolved; its budget and scheduler slot stay held and the original Provider call is never automatically replayed.
- `X-Client-Request-Id` is a correlation aid only; it is never accepted as outcome/usage proof by itself.
- Do not persist prompts, raw request/response bodies, provider credentials, completion output, support transcripts, email contents, or raw evidence payloads.
- Keep OpenAI Responses `store: false`; do not retrieve or synthesize a lost output.
- A verified charged Provider request whose output is lost settles exact usage and marks the Agent Attempt `failed`, not `succeeded`.
- Release an Attempt only after host-verified termination evidence is bound to that exact Attempt/fencing generation; the current generic `SandboxTerminationReceipt` alone lacks this binding and is not sufficient.
- Unit, integration, and crash tests use fakes/local SQLite only; do not make paid Provider calls.
- Push every task commit to `origin/codex/p3-systemd-termination-receipts`.

## Review Focus

1. **Ambiguous or forged source evidence:** a correlation ID, aggregate usage bucket, partial receipt, or untrusted assertion must not settle the budget. Pin this to Task 3 verifier tests.
2. **Cross-provider/header leakage:** `X-Client-Request-Id` must be sent only to the fixed first-party `openai_responses` route, never a generic compatibility endpoint. Pin this to Task 1 transport tests.
3. **Live/stale Attempt termination:** no release is allowed if the supervisor cannot prove the matching Attempt/fence stopped. Pin this to Task 4 termination and stale-fence tests.
4. **Provider-response/reconciliation race:** the late Gateway outcome and reconciliation must have one journal CAS winner; the loser cannot overwrite the winner or dispatch again. Pin this to Task 5 concurrency tests.
5. **Cross-stream crash windows:** proof persistence, Scheduler settlement, and the settlement marker must replay idempotently after each process-death point. Pin this to Task 5 subprocess tests.

---

## File Map

- Modify `src/orchestrator/models/transport.py`: generate the correlation ID after prior-call rejection, persist it with intent, and attach it only to the supported first-party OpenAI Responses request.
- Modify `src/orchestrator/models/provider_calls.py`: add correlation/reconciliation projection fields and journal APIs that can recover a stream without reconstructing prompt-bearing `ModelRequest` data.
- Modify `src/orchestrator/persistence/events.py`: strictly validate the new append-only Provider reconciliation and Scheduler-applied marker event sequences.
- Create `src/orchestrator/provider_reconciliation.py`: define the host-only `ProviderEvidenceVerifier`, `AttemptTerminationVerifier`, and `ProviderReconciliationService`; default evidence verification is unavailable/fail-closed.
- Use `src/orchestrator/scheduler/core.py` unchanged for the atomic budget/lifecycle/Agent/slot operation through `Scheduler.reconcile_attempt()`; do not add a parallel settlement implementation.
- Modify `tests/unit/models/test_provider_call_journal.py` and `tests/unit/models/test_provider_adapters_transport.py` for correlation, event validation, bounds, and CAS behavior.
- Create `tests/unit/test_provider_reconciliation.py` for verifier and host-service boundaries.
- Modify `tests/unit/lifecycle/test_scheduler.py` for real SQLite/Scheduler settlement and process-death replay with fake Provider evidence; this file already owns the Run/route setup helpers and crash-injection patterns.
- Update `docs/security/provider-call-journal.md` and the P3 status/checklist in `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`; keep automatic OpenAI reconciliation marked unavailable until an exact production verifier is configured.

## Implementation Tasks

### Task 1: Persist and transmit opaque OpenAI correlation IDs

**Files:**
- Modify: `src/orchestrator/models/provider_calls.py`
- Modify: `src/orchestrator/models/transport.py`
- Test: `tests/unit/models/test_provider_call_journal.py`
- Test: `tests/unit/models/test_provider_adapters_transport.py`

**Interfaces:**
- `ProviderCallJournal.record_intent(request, *, provider_id, provider_adapter, request_body, provider_correlation_id=None) -> str` stores the route's adapter capability and a nullable, bounded ID in the same intent event.
- `ProviderCallSnapshot.provider_adapter: ProviderAdapter` and `provider_correlation_id: str | None` return the persisted values after reopening SQLite; reconciliation is enabled only for a supported first-party adapter.
- `ProviderModelGateway.invoke()` creates one `maestro-<uuid>` ASCII ID only when the validated manifest route has `adapter == "openai_responses"` (whose endpoint is already constrained to `https://api.openai.com/v1`). It sends that same value as `X-Client-Request-Id`; all other adapters send no such header.

- [ ] **Step 1: Write the failing correlation persistence and header test** in `tests/unit/models/test_provider_call_journal.py` using the existing `model_registry()`, `model_request()`, `_FakeTransport`, `_Broker`, `_Verifier`, and `_success_response()` helpers.

```python
def test_gateway_persists_and_sends_one_openai_correlation_id(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    store = SQLiteEventStore(tmp_path / "correlation.db")
    journal = SQLiteProviderCallJournal(store)
    transport = _FakeTransport(_success_response())
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=transport,
        provider_call_journal=journal,
    )

    asyncio.run(gateway.invoke(request))

    pending = journal.read(request)
    assert pending is not None
    assert pending.provider_correlation_id is not None
    headers = dict(transport.calls[0]["headers"])
    assert headers["X-Client-Request-Id"] == pending.provider_correlation_id
    assert pending.provider_correlation_id.isascii()
    assert 0 < len(pending.provider_correlation_id) <= 512
```

- [ ] **Step 2: Run the focused test and confirm it fails** because the snapshot has no persisted correlation field/header yet.

Run: `python -m pytest tests/unit/models/test_provider_call_journal.py::test_gateway_persists_and_sends_one_openai_correlation_id -q`

Expected: FAIL with the missing correlation field or header assertion.

- [ ] **Step 3: Add the optional journal field and one-time Gateway generation.** Extend the `record_intent` protocol/implementation and snapshot. In `transport.py`, generate only after the existing prior-call check, pass the same value to `record_intent`, and add a header only when non-`None`:

```python
from orchestrator.identifiers import new_id

provider_correlation_id = (
    "maestro-" + new_id()
    if provider.adapter == "openai_responses"
    else None
)
self.provider_call_journal.record_intent(
    request,
    provider_id=provider.id,
    provider_adapter=provider.adapter,
    request_body=encoded.body_json.encode("utf-8"),
    provider_correlation_id=provider_correlation_id,
)
headers = [
    ("Content-Type", "application/json"),
    ("Accept", "application/json"),
    (credential.header_name, _auth_value(provider.adapter, credential.value)),
    ("Idempotency-Key", request.idempotency_key),
    ("X-Request-Id", request.request_id),
]
if provider_correlation_id is not None:
    headers.append(("X-Client-Request-Id", provider_correlation_id))
```

- [ ] **Step 4: Add restart and endpoint-scope assertions.** Record an intent-only call in a first journal, close/reopen the SQLite store, and assert `unresolved()[0].provider_correlation_id` is byte-for-byte unchanged. Add a `openai_compatible` Gateway test with `endpoint="https://compat.example/v1"` and assert the request contains no `X-Client-Request-Id` header.
- [ ] **Step 5: Run both focused model test modules.**

Run: `python -m pytest tests/unit/models/test_provider_call_journal.py tests/unit/models/test_provider_adapters_transport.py -q`

Expected: PASS; existing replay blocking and secret redaction assertions remain unchanged.

- [ ] **Step 6: Commit and push Task 1.**

```bash
git add src/orchestrator/models/provider_calls.py src/orchestrator/models/transport.py tests/unit/models/test_provider_call_journal.py tests/unit/models/test_provider_adapters_transport.py
git commit -m "feat: correlate OpenAI provider calls"
git push origin codex/p3-systemd-termination-receipts
```

### Task 2: Add validated append-only reconciliation journal events

**Files:**
- Modify: `src/orchestrator/models/provider_calls.py`
- Modify: `src/orchestrator/persistence/events.py`
- Test: `tests/unit/models/test_provider_call_journal.py`
- Test: `tests/contract/test_cross_spec_events.py`

**Interfaces:**
- Add immutable `ProviderCallReconciliation` with the complete call binding (`provider_call_stream_id`, `provider_adapter`, correlation ID, Run/Node/Attempt/fence, Provider/model/route/reservation, registry hash, and request hash), plus `effect: Literal["not_received", "received_and_charged"]`, optional exact `UsageRecord`, optional `provider_request_id`, `evidence_source: Literal["provider_signed_receipt", "provider_authoritative_api"]`, SHA-256 `evidence_digest`, required SHA-256 `termination_receipt_hash`, and timezone-aware `observed_at`. Neither correlation IDs, aggregate usage buckets, nor operator assertions qualify as exact evidence sources. Reconciliation supports only `provider_adapter == "openai_responses"` with a non-null generated correlation ID. `not_received` requires no usage; `received_and_charged` requires exact usage.
- Extend `ProviderCallStatus` with `settlement_pending` and `reconciled`; add immutable snapshot fields `reconciliation_event_id: str | None`, `reconciled_at: datetime | None`, and `settlement_applied: bool`.
- Add internal `SQLiteProviderCallJournal._append_reconciliation(stream_id: str, proof: ProviderCallReconciliation, reconciled_at: datetime) -> str` and `_record_scheduler_settlement(stream_id: str, reconciliation_event_id: str) -> None`; expose `read_call(stream_id: str) -> ProviderCallSnapshot | None` and `pending_settlements() -> tuple[ProviderCallSnapshot, ...]` for the host service. Persist `reconciled_at` separately from the Provider evidence's `observed_at`, so Scheduler replay uses the same timestamp after restart. No CLI/MCP or Worker surface calls the internal append methods.
- A legal provider stream is `intent`, optionally followed by `outcome(unknown)`, then `reconciliation`, then `scheduler_settlement_applied`. A terminal known outcome cannot be reconciled or overwritten. Both append operations use the exact expected stream version and deterministic idempotency binding.

- [ ] **Step 1: Write a journal test for the two permitted evidence entry states.** In `test_provider_call_journal.py`, import `datetime`, `timezone`, and `ProviderCallReconciliation`; record one intent-only call and one intent+unknown call; for each, append the same valid proof at an explicit `reconciled_at` and assert exactly one reconciliation event and a `settlement_pending` snapshot.

```python
def test_provider_reconciliation_accepts_dispatching_or_unknown_only(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    def proof_for(call):
        return ProviderCallReconciliation(
            provider_call_stream_id=call.stream_id,
            provider_adapter=call.provider_adapter,
            provider_correlation_id=call.provider_correlation_id,
            run_id=call.run_id,
            node_id=call.node_id,
            attempt_id=call.attempt_id,
            fencing_generation=call.fencing_generation,
            provider_id=call.provider_id,
            model_id=call.model_id,
            accepted_route_id=call.accepted_route_id,
            budget_reservation_id=call.budget_reservation_id,
            registry_manifest_hash=call.registry_manifest_hash,
            request_hash=call.request_hash,
            effect="not_received",
            usage=None,
            provider_request_id=None,
            evidence_source="provider_signed_receipt",
            evidence_digest="sha256:" + "b" * 64,
            termination_receipt_hash="sha256:" + "c" * 64,
            observed_at=datetime(2026, 9, 29, 12, tzinfo=timezone.utc),
        )

    dispatch_store = SQLiteEventStore(tmp_path / "dispatching.db")
    dispatch_journal = SQLiteProviderCallJournal(dispatch_store)
    dispatch_journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-a"
    )
    dispatch_id = dispatch_journal.unresolved()[0].stream_id
    dispatch_call = dispatch_journal.read_call(dispatch_id)
    assert dispatch_call is not None
    reconciled_at = datetime(2026, 9, 29, 13, tzinfo=timezone.utc)
    dispatch_journal._append_reconciliation(dispatch_id, proof_for(dispatch_call), reconciled_at)
    dispatch_result = dispatch_journal.read_call(dispatch_id)
    assert dispatch_result is not None
    assert dispatch_result.status == "settlement_pending"

    unknown_store = SQLiteEventStore(tmp_path / "unknown.db")
    unknown_journal = SQLiteProviderCallJournal(unknown_store)
    unknown_journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-a"
    )
    unknown_journal.record_outcome(request, outcome="unknown", failure_code="timeout")
    unknown_id = unknown_journal.unresolved()[0].stream_id
    unknown_call = unknown_journal.read_call(unknown_id)
    assert unknown_call is not None
    unknown_journal._append_reconciliation(unknown_id, proof_for(unknown_call), reconciled_at)
    unknown_result = unknown_journal.read_call(unknown_id)
    assert unknown_result is not None
    assert unknown_result.status == "settlement_pending"

    terminal_store = SQLiteEventStore(tmp_path / "known-success.db")
    terminal_journal = SQLiteProviderCallJournal(terminal_store)
    terminal_journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-terminal"
    )
    terminal_journal.record_outcome(request, outcome="known_success", http_status=200)
    terminal_id = terminal_journal.read(request).stream_id
    with pytest.raises(ValueError, match="terminal"):
        terminal_call = terminal_journal.read_call(terminal_id)
        assert terminal_call is not None
        terminal_journal._append_reconciliation(
            terminal_id, proof_for(terminal_call), reconciled_at
        )
```

Import `ProviderCallReconciliation` from `orchestrator.models.provider_calls`, `datetime`/`timezone` from `datetime`, and `pytest` from the existing test module imports.

- [ ] **Step 2: Run the new journal test and confirm the missing API/state rejects it.**

Run: `python -m pytest tests/unit/models/test_provider_call_journal.py::test_provider_reconciliation_accepts_dispatching_or_unknown_only -q`

Expected: FAIL because the reconciliation model/API and event transition are not implemented.

- [ ] **Step 3: Extend event contract validation with exact schemas and identity binding.** Add exact event-key sets and validate that each event repeats the intent's provider-call stream ID, adapter, correlation ID, Run/Node/Attempt/fence, Provider/model/route/reservation/registry/request hashes; require reconciliation causation to match the intent route; allow only the two sequences listed above; bind the settlement marker's causation to the reconciliation event ID.

```python
_PROVIDER_CALL_BINDING_FIELDS = frozenset({
    "provider_call_stream_id", "provider_adapter", "run_id", "node_id",
    "attempt_id", "fencing_generation", "provider_id", "model_id",
    "accepted_route_id", "budget_reservation_id",
    "registry_manifest_hash", "request_hash", "provider_correlation_id",
})
_PROVIDER_RECONCILIATION_FIELDS = _PROVIDER_CALL_BINDING_FIELDS | frozenset({
    "provider_request_id", "effect", "usage", "evidence_source",
    "evidence_digest", "termination_receipt_hash", "observed_at",
    "reconciled_at",
})
_PROVIDER_SETTLEMENT_FIELDS = _PROVIDER_CALL_BINDING_FIELDS | frozenset({
    "reconciliation_event_id",
})
```

Register `ProviderCallReconciliationRecorded` and `ProviderCallSchedulerSettlementApplied` in `_CAUSAL_EVENT_TYPES`; the event-contract tests must assert their causal envelope and exact payload fields as well as their stream-local order.

- [ ] **Step 4: Implement the journal projection without reconstructing prompts.** `read_call()` reads intent/outcome/reconciliation/marker events and derives state in this order:

```python
if reconciliation is not None:
    status = "reconciled" if settlement_applied else "settlement_pending"
else:
    status = terminal_outcome if terminal_outcome is not None else "dispatching"
```

- [ ] **Step 5: Implement internal event appends using SQLite stream-version CAS.** `_append_reconciliation()` appends at expected version 1 for intent-only or 2 for intent+unknown and persists the host `reconciled_at`; `_record_scheduler_settlement()` appends at the version after proof. An exact replay returns the existing event; a different proof, timestamp, or marker target raises a typed conflict. Keep these low-level append methods out of application-facing exports; `ProviderReconciliationService` is the only production caller.
- [ ] **Step 6: Add malformed-chain and concurrent-CAS tests.** Assert an orphan reconciliation, invalid event order, different Attempt/fence, second conflicting receipt, and marker for another reconciliation ID all raise `EventContractError` or a typed journal error. Use two `SQLiteEventStore` connections and `ThreadPoolExecutor(max_workers=2)` to prove only one concurrent reconciliation append wins.
- [ ] **Step 7: Run event and journal suites, then commit and push Task 2.**

Run: `python -m pytest tests/unit/models/test_provider_call_journal.py tests/contract/test_cross_spec_events.py -q`

Expected: PASS, with exactly one durable reconciliation winner per call stream.

```bash
git add src/orchestrator/models/provider_calls.py src/orchestrator/persistence/events.py tests/unit/models/test_provider_call_journal.py tests/contract/test_cross_spec_events.py
git commit -m "feat: journal provider reconciliation evidence"
git push origin codex/p3-systemd-termination-receipts
```

### Task 3: Add fail-closed Provider and termination verifier contracts

**Files:**
- Create: `src/orchestrator/provider_reconciliation.py`
- Modify: `tests/unit/test_provider_reconciliation.py`
- Test: `tests/unit/models/test_provider_call_journal.py`

**Interfaces:**
- `ProviderEvidenceVerifier.verify(call: ProviderCallSnapshot, raw_evidence: bytes) -> ProviderCallReconciliation` is the only input-to-proof boundary. It receives at most 64 KiB of evidence bytes and must validate the provider-specific source before returning a proof.
- `AttemptTerminationVerifier.verify_stopped(call: ProviderCallSnapshot, receipt: object) -> str` returns a SHA-256 digest only for a host-created receipt bound to the exact Attempt/fencing generation; it raises `ReconciliationRejected` for absent, wrong-unit, non-empty-cgroup, stale, or caller-forged evidence. The current generic `SandboxTerminationReceipt` does not carry that identity, so no production implementation may accept it alone.
- `UnavailableProviderEvidenceVerifier.verify(...)` always raises `ProviderEvidenceUnsupported`; do not implement an OpenAI support/Usage shortcut as a verifier.
- `UnavailableAttemptTerminationVerifier.verify_stopped(...)` always raises `ReconciliationRejected` until a real host supervisor can prove the Attempt/fence binding.
- `ProviderReconciliationService.reconcile(stream_id: str, raw_evidence: bytes, termination_receipt: object, reconciled_at: datetime) -> ProviderCallSnapshot` accepts raw evidence, never a caller-built proof object.

- [ ] **Step 1: Write tests for missing verifier, oversized bytes, unavailable/ambiguous source, and caller-supplied proof objects.** The service must not append a reconciliation event or change the budget if any case is rejected.

```python
def test_unavailable_provider_evidence_verifier_leaves_call_unknown():
    call = ProviderCallSnapshot(
        stream_id="call-" + "a" * 64,
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=1,
        request_id="request-1",
        idempotency_key_hash="sha256:" + "b" * 64,
        accepted_route_id="decision-1",
        budget_reservation_id="reservation-1",
        provider_id="primary",
        provider_adapter="openai_responses",
        model_id="model-1",
        registry_manifest_hash="sha256:" + "c" * 64,
        request_hash="sha256:" + "d" * 64,
        provider_correlation_id="maestro-a",
        status="unknown",
    )

    class Journal:
        recorded = False

        def read_call(self, stream_id):
            assert stream_id == call.stream_id
            return call

        def _append_reconciliation(self, *_args, **_kwargs):
            self.recorded = True
            raise AssertionError("unverified evidence reached the journal")

    class TerminationVerifier:
        def verify_stopped(self, _call, _receipt):
            return "sha256:" + "e" * 64

    journal = Journal()
    service = ProviderReconciliationService(
        journal=journal,
        scheduler=object(),
        evidence_verifier=UnavailableProviderEvidenceVerifier(),
        termination_verifier=TerminationVerifier(),
    )

    with pytest.raises(ProviderEvidenceUnsupported):
        service.reconcile(
            call.stream_id,
            raw_evidence=b"{}",
            termination_receipt=object(),
            reconciled_at=datetime(2026, 9, 29, 12, tzinfo=timezone.utc),
        )

    assert journal.recorded is False
```

- [ ] **Step 2: Run the verifier boundary test and confirm it fails because the service/contracts are absent.**

Run: `python -m pytest tests/unit/test_provider_reconciliation.py::test_unavailable_provider_evidence_verifier_leaves_call_unknown -q`

Expected: FAIL because `ProviderReconciliationService` and fail-closed verifier types do not exist.

- [ ] **Step 3: Define the evidence-verifier protocol and fail-closed default.** Add typed errors and a Protocol; reject non-bytes or evidence larger than 65,536 bytes before calling the verifier; make the default verifier report unsupported instead of accepting correlation IDs or aggregated Usage data.

```python
class ProviderEvidenceVerifier(Protocol):
    def verify(
        self, call: ProviderCallSnapshot, raw_evidence: bytes
    ) -> ProviderCallReconciliation: ...


class UnavailableProviderEvidenceVerifier:
    def verify(self, call: ProviderCallSnapshot, raw_evidence: bytes) -> ProviderCallReconciliation:
        raise ProviderEvidenceUnsupported("no authoritative Provider evidence source is configured")
```

- [ ] **Step 4: Validate verifier output before it can reach the journal.** Revalidate the snapshot and returned immutable proof; require `call.provider_adapter == "openai_responses"` and a non-null correlation ID, then require equality for every binding field, including provider-call stream ID, adapter, correlation ID, request-body hash, route, registry, reservation, Run/Node/Attempt/fence; accept only `not_received` without usage or `received_and_charged` with exact UsageRecord; reject an object supplied in place of raw evidence bytes so callers cannot bypass the verifier with a constructed proof; never log or persist raw evidence.
- [ ] **Step 5: Define the Attempt-termination protocol and fail-closed default.** Require a host-created supervisor result bound to the full Run/Node/Attempt/fence. The current Sandbox receipt can be consumed only after a supervisor has associated its unit with this exact context; it does not prove this association on its own.

```python
class AttemptTerminationVerifier(Protocol):
    def verify_stopped(self, call: ProviderCallSnapshot, receipt: object) -> str: ...


class UnavailableAttemptTerminationVerifier:
    def verify_stopped(self, call: ProviderCallSnapshot, receipt: object) -> str:
        raise ReconciliationRejected("no Attempt-bound termination witness is configured")
```

- [ ] **Step 6: Add fakes only in tests.** Implement `FakeProviderEvidenceVerifier(proof)` and `FakeAttemptTerminationVerifier(expected_digest)` in the test module; test unknown status, wrong evidence digest/provider/request hash/fencing, caller-supplied proof objects, and no-effect/charged usage shape. Do not register the fake in package runtime exports.
- [ ] **Step 7: Run focused verifier tests and commit/push Task 3.**

Run: `python -m pytest tests/unit/test_provider_reconciliation.py tests/unit/models/test_provider_call_journal.py -q`

Expected: PASS; no production verifier is registered and default behavior remains unresolved/fail-closed.

```bash
git add src/orchestrator/provider_reconciliation.py tests/unit/test_provider_reconciliation.py tests/unit/models/test_provider_call_journal.py
git commit -m "feat: define trusted provider evidence boundary"
git push origin codex/p3-systemd-termination-receipts
```

### Task 4: Apply verified proof through Scheduler and recover pending settlement

**Files:**
- Modify: `src/orchestrator/provider_reconciliation.py`
- Modify: `src/orchestrator/models/provider_calls.py`
- Test: `tests/unit/lifecycle/test_scheduler.py`

**Interfaces:**
- `ProviderReconciliationService.reconcile(...)` must require an existing journal state of `dispatching` or `unknown`, verified Provider proof, verified termination, and a Scheduler Attempt already classified `OutcomeUnknown`. It appends proof before calling Scheduler.
- `ProviderReconciliationService.apply_pending_settlements() -> tuple[str, ...]` replays only proof events already persisted and not yet marked applied; it reads each persisted `reconciled_at`, never re-queries the Provider, and takes no raw evidence.
- No-effect proof calls `Scheduler.reconcile_attempt(..., outcome="failed", known_no_effect=True)`. Charged proof calls `Scheduler.reconcile_attempt(..., outcome="failed", usage=exact_usage)`. The call's lost output is never treated as success.
- After the Scheduler transaction succeeds, the service appends `ProviderCallSchedulerSettlementApplied`; scheduler idempotency plus the Provider journal marker make the cross-stream operation replay-safe.

- [ ] **Step 1: Write a scheduler integration test for both definitive results.** Add the test to `tests/unit/lifecycle/test_scheduler.py` so it can use `run_setup()`, `routed_pair()`, `scheduler()`, and `accept()` without importing another test module. For each parameter, create the SQLite store, call `run_setup(store)`, build `(request, decision) = routed_pair(registry, config, manifest)`, set `control = scheduler(store)`, and call `accepted = accept(control, request, decision)`. Mark the Attempt unknown with `control.finish_attempt(run_id=request.run_id, node_id=request.node_id, attempt_id=request.attempt_id, fencing_generation=request.fencing_generation, completed_at=NOW + timedelta(minutes=1), outcome="outcome_unknown")`. Construct a `ModelRequest` from the accepted route and Run Registry with the helper below; persist its Provider intent and `unknown` outcome; inject fake Provider/termination verifiers; then parameterize over `not_received` and `received_and_charged`. Use the actual accepted reservation ID/run ID in any `UsageRecord`.

```python
from orchestrator.models.gateway import CostSnapshotRefs, ModelMessage, ModelRequest

def provider_request_for(accepted, registry):
    route = accepted.accepted_route
    return ModelRequest(
        request_id=f"provider-{route.attempt_id}",
        idempotency_key=f"provider-idem-{route.attempt_id}",
        run_id=route.run_id,
        node_id=route.node_id,
        attempt_id=route.attempt_id,
        fencing_generation=route.fencing_generation,
        budget_reservation_id=route.budget_reservation_id,
        model_id=route.model_id,
        accepted_route=route,
        messages=(ModelMessage(role="user", content="test prompt"),),
        max_output_tokens=32,
        reasoning_effort=route.reasoning_effort,
        timeout_ms=5_000,
        cost_snapshots=CostSnapshotRefs(
            registry_manifest_hash=registry.content_hash,
            tokenizer_snapshot_id=HASH,
            fx_snapshot_id=HASH,
            price_snapshot_id=registry.content_hash,
            estimator_snapshot_id=HASH,
        ),
    )
```

Use a test fake that returns one of two immutable proofs selected by the test parameter. For the charged case, construct exact usage after reading `call` from the journal:

```python
from orchestrator.budget.models import UsageRecord
from orchestrator.models.provider_calls import ProviderCallReconciliation

binding = {
    "provider_call_stream_id": call.stream_id,
    "provider_adapter": call.provider_adapter,
    "provider_correlation_id": call.provider_correlation_id,
    "run_id": call.run_id,
    "node_id": call.node_id,
    "attempt_id": call.attempt_id,
    "fencing_generation": call.fencing_generation,
    "provider_id": call.provider_id,
    "model_id": call.model_id,
    "accepted_route_id": call.accepted_route_id,
    "budget_reservation_id": call.budget_reservation_id,
    "registry_manifest_hash": call.registry_manifest_hash,
    "request_hash": call.request_hash,
}

usage = UsageRecord(
    reservation_id=accepted.reservation.reservation_id,
    run_id="run-1",
    settlement_key=f"provider-reconcile-{call.stream_id}",
    currency="USD",
    input_tokens=12,
    output_tokens=4,
    provider_fee_minor=3,
    cost_minor=3,
)
charged_proof = ProviderCallReconciliation(
    **binding,
    effect="received_and_charged",
    usage=usage,
    provider_request_id="provider-request-1",
    evidence_source="provider_signed_receipt",
    evidence_digest="sha256:" + "b" * 64,
    termination_receipt_hash="sha256:" + "c" * 64,
    observed_at=NOW + timedelta(minutes=2),
)
not_received_proof = ProviderCallReconciliation(
    **binding,
    effect="not_received",
    usage=None,
    provider_request_id=None,
    evidence_source="provider_signed_receipt",
    evidence_digest="sha256:" + "d" * 64,
    termination_receipt_hash="sha256:" + "c" * 64,
    observed_at=NOW + timedelta(minutes=2),
)
```

```python
@pytest.mark.parametrize(
    ("effect", "expected_reservation_status"),
    [("not_received", "released"), ("received_and_charged", "committed")],
)
def test_scheduler_reconciles_provider_no_delivery_and_charged_usage(
    tmp_path, effect, expected_reservation_status
):
    store = SQLiteEventStore(tmp_path / f"reconcile-{effect}.db")
    reg, config, _lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    route_request, decision = routed_pair(reg, config, manifest)
    accepted = accept(control, route_request, decision)
    control.finish_attempt(
        run_id=route_request.run_id,
        node_id=route_request.node_id,
        attempt_id=route_request.attempt_id,
        fencing_generation=route_request.fencing_generation,
        completed_at=NOW + timedelta(minutes=1),
        outcome="outcome_unknown",
    )
    request = provider_request_for(accepted, reg)
    journal = SQLiteProviderCallJournal(store)
    journal.record_intent(
        request,
        provider_id=accepted.accepted_route.provider_id,
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-test",
    )
    journal.record_outcome(request, outcome="unknown", failure_code="timeout")
    intent = journal.read(request)
    assert intent is not None
    call = journal.read_call(intent.stream_id)
    assert call is not None
    proof = not_received_proof if effect == "not_received" else charged_proof
    service = ProviderReconciliationService(
        journal=journal,
        scheduler=control,
        evidence_verifier=FakeProviderEvidenceVerifier(proof),
        termination_verifier=FakeAttemptTerminationVerifier(
            proof.termination_receipt_hash
        ),
    )
    reconciled = service.reconcile(
        call.stream_id,
        raw_evidence=b"fake-signed-provider-result",
        # This opaque object is consumed only by the injected test verifier.
        termination_receipt=object(),
        reconciled_at=NOW + timedelta(minutes=3),
    )

    assert reconciled.status == "reconciled"
    assert BudgetLedger(store).get_reservation(
        accepted.reservation.reservation_id, run_id="run-1"
    ).status == expected_reservation_status
    attempt = control.lifecycle.replay("run-1").node("node-1").attempts[-1]
    assert attempt.status == "failed"
    assert control.recovery.recover("run-1").active_attempts == ()
```

Keep the test-only `service.reconcile()` call inside the parametrized test body. The fake termination verifier must return the exact proof's `termination_receipt_hash`; assert the expected reservation state rather than treating release and charge as interchangeable.

- [ ] **Step 2: Run the integration test and confirm the missing service orchestration fails.**

Run: `python -m pytest tests/unit/lifecycle/test_scheduler.py::test_scheduler_reconciles_provider_no_delivery_and_charged_usage -q`

Expected: FAIL until proof append, Scheduler call, and settlement marker are wired.

- [ ] **Step 3: Implement the unknown-Attempt precondition and proof-first append.** Read `scheduler.lifecycle.replay(call.run_id)` and require this exact Node/Attempt to be `outcome_unknown` at `call.fencing_generation`; reject an active, succeeded, cancelled, or stale-fence Attempt before recording evidence. Append the validated proof and its stable event ID before changing budget state.
- [ ] **Step 4: Apply the proof through `Scheduler.reconcile_attempt()`.** Derive `settlement_key` deterministically from the Provider call stream (`provider-reconcile-{stream_id}`), so it is available before proof append and identical on replay. Map exact provider amounts, currency, and token fields to `UsageRecord`. For `not_received`, pass `known_no_effect=True` and `usage=None`; for `received_and_charged`, pass exact usage and `known_no_effect=False`. Always use `outcome="failed"`; do not duplicate BudgetLedger/lifecycle/Agent mutation logic.
- [ ] **Step 5: Implement `apply_pending_settlements()`.** Read `pending_settlements()`, use only the persisted proof and its persisted `reconciled_at`, call Scheduler idempotently with the exact same timestamp/evidence-derived usage, then append the applied marker. A marker is written only after `Scheduler.reconcile_attempt()` returns successfully.
- [ ] **Step 6: Test rejection and idempotency.** Reject an Attempt not yet unknown, wrong fence, stale timestamp, mismatched reservation/run/currency, missing usage on charged outcome, or usage on no-effect. Call `reconcile()` twice with identical evidence and prove budget events/slot release do not duplicate; conflicting evidence must fail closed.
- [ ] **Step 7: Run the Scheduler suite, then commit/push Task 4.**

Run: `python -m pytest tests/unit/lifecycle/test_scheduler.py -q`

Expected: PASS; all existing scheduler reconciliation invariants still hold.

```bash
git add src/orchestrator/provider_reconciliation.py src/orchestrator/models/provider_calls.py tests/unit/lifecycle/test_scheduler.py
git commit -m "feat: settle verified provider outcomes"
git push origin codex/p3-systemd-termination-receipts
```

### Task 5: Prove response races, restart replay, and update security status

**Files:**
- Modify: `tests/unit/lifecycle/test_scheduler.py`
- Modify: `tests/unit/models/test_provider_call_journal.py`
- Modify: `docs/security/provider-call-journal.md`
- Modify: `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`

**Interfaces:**
- Gateway late-outcome writes, Provider reconciliation writes, and Scheduler settlement replay all operate on their durable compare-and-swap/idempotency contracts from Tasks 1–4.
- `unresolved()` reports only `dispatching`/`unknown` external outcomes; `pending_settlements()` reports verified proof waiting for Scheduler application; neither list authorizes Provider replay.

- [ ] **Step 1: Add a two-connection race test for a late known-success receipt.** Start from an intent-only stream, then race `record_outcome(..., outcome="known_success", http_status=200)` against `_append_reconciliation(...)` using two SQLite connections. The sole second-event winner is either known-success or proof; the loser gets a typed conflict, no legal history overwrites the winner, and the existing Gateway replay test still proves no second transport call.

```python
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

barrier = Barrier(2)

def try_write(write):
    barrier.wait()
    try:
        write()
        return "written"
    except (IdempotencyConflict, StaleStream, ValueError):
        return "conflict"

with ThreadPoolExecutor(max_workers=2) as pool:
    futures = (
        pool.submit(try_write, lambda: first_journal.record_outcome(
            request, outcome="known_success", http_status=200
        )),
        pool.submit(try_write, lambda: second_journal._append_reconciliation(
            stream_id, proof, reconciled_at
        )),
    )
    results = [future.result() for future in futures]

assert results.count("written") == 1
assert results.count("conflict") == 1
snapshot = first_journal.read_call(stream_id)
assert snapshot.status in {"known_success", "settlement_pending"}
```

Seed `request`, `proof`, `reconciled_at`, and `stream_id` with the intent-only fixture; open `first_journal` and `second_journal` over separate SQLite connections to the same temporary database. The connection setup must enable the repository's normal SQLite busy timeout so the second append observes the winning stream version rather than an unrelated lock error.

- [ ] **Step 2: Add process-death injection for every durable boundary.** Use a `multiprocessing` child created with the `spawn` context, a temporary SQLite database, and `os._exit()` after (a) proof event append, (b) Scheduler multi-stream settlement commit but before the Provider marker, and (c) marker commit. The child must open fresh SQLite objects from the seeded database; the parent waits for exit code 78, reopens the DB, and calls `apply_pending_settlements()`. Assert one final Scheduler settlement, one slot release, and no repeated Provider call.

Add a module-level `_run_reconciliation_crash_child(database_path, crash_point)` target so it is importable by the `spawn` process. It opens all SQLite objects after process start, installs the selected test-only fault hook, and either calls `reconcile()` (proof-append boundary) or `apply_pending_settlements()` (settlement/marker boundaries).

```python
import multiprocessing
import os

child_store = SQLiteEventStore(database_path)
child_journal = SQLiteProviderCallJournal(child_store)
child_scheduler = scheduler(child_store)
child_service = ProviderReconciliationService(
    journal=child_journal,
    scheduler=child_scheduler,
    evidence_verifier=UnavailableProviderEvidenceVerifier(),
    termination_verifier=UnavailableAttemptTerminationVerifier(),
)

original_append_marker = child_journal._record_scheduler_settlement
original_append_proof = child_journal._append_reconciliation

def exit_after_proof_append(*args, **kwargs):
    original_append_proof(*args, **kwargs)
    os._exit(78)

def exit_after_scheduler_commit_before_marker(*args, **kwargs):
    os._exit(78)

def exit_after_marker_commit(*args, **kwargs):
    original_append_marker(*args, **kwargs)
    os._exit(78)

# The child already reopened the seeded DB above. The service calls this only
# after Scheduler.reconcile_attempt commits; exit before the marker is appended.
child_service.journal._record_scheduler_settlement = exit_after_scheduler_commit_before_marker
child_service.apply_pending_settlements()
```

The parent seeds the required precondition for each boundary: an `unknown` journal/Attempt before the proof-append crash, and a `settlement_pending` journal before the later two crash points. Run each boundary with:

```python
context = multiprocessing.get_context("spawn")
child = context.Process(
    target=_run_reconciliation_crash_child,
    args=(database_path, crash_point),
)
child.start()
child.join(timeout=10)
assert child.exitcode == 78
```

Run three independent spawned children, each with the fresh service setup above. For the proof-append crash, replace `_append_reconciliation` with `exit_after_proof_append`, inject the test-only fake evidence and termination verifiers, and call `child_service.reconcile()` with the same fake evidence and receipt as Task 4; after parent restart, assert the persisted proof is applied without asking for evidence again. For the pre-marker crash, replace `_record_scheduler_settlement` with `exit_after_scheduler_commit_before_marker` and call `apply_pending_settlements()`. For the marker-durable crash, replace it with `exit_after_marker_commit` and call `apply_pending_settlements()`; after parent restart, assert the call is already reconciled and no extra Scheduler/Budget event is appended. The child setup uses fresh SQLite objects; replay-only cases use unavailable verifiers because `apply_pending_settlements()` must need neither raw evidence nor verifier calls.
- [ ] **Step 3: Assert privacy and projection recovery.** Serialize all Provider event payloads and assert no prompt/output/credential/raw evidence appears. Reopen the journal and assert `unknown` stays held, proof-without-marker is `settlement_pending`, and fully marked proof is settled exactly once.
- [ ] **Step 4: Update `docs/security/provider-call-journal.md`.** Replace the “no evidence/reconciliation path” status with the implemented event flow, explicitly state that the default verifier has no production OpenAI source and leaves calls unresolved, distinguish Provider charges from Attempt success, and document the manual `apply_pending_settlements()` host API without advertising a CLI/MCP surface.
- [ ] **Step 5: Update the P3 status and checkboxes in the full V1 plan.** Mark only implemented correlation/evidence-contract/replay behavior complete. Keep “production Provider authoritative lookup”, complete Worker termination binding, Worker/application automatic startup wiring, CLI/MCP, E2E security, and benchmarks open. Do not label P3 or V1 delivered.
- [ ] **Step 6: Run acceptance verification before the final commit.**

Run: `python -m pytest -q`

Expected: PASS with no skipped deterministic unit/contract tests.

Run: `python -m coverage run -m pytest -q && python -m coverage report --fail-under=90`

Expected: PASS at `>= 90%` total project coverage.

Run: `python -m compileall -q src tests && python -m pip check && python -m build --wheel`

Expected: all commands exit 0 and the wheel is produced.

- [ ] **Step 7: Inspect staged diff, commit, push, and verify synchronization.**

```bash
git diff --check
git diff --stat
git status --short --branch
git add tests/unit/lifecycle/test_scheduler.py tests/unit/models/test_provider_call_journal.py docs/security/provider-call-journal.md docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md
git commit -m "test: verify provider reconciliation recovery"
git push origin codex/p3-systemd-termination-receipts
git status --short --branch
git rev-parse --short HEAD
git rev-parse --short origin/codex/p3-systemd-termination-receipts
```

Expected: clean worktree and identical local/remote commit IDs. If the live provider verification source or a context-bound production termination supervisor is still unavailable, report those as remaining V1 gates; do not claim real Provider reconciliation is production-enabled.

## Plan Self-Review

- **Spec coverage:** correlation/header scoping is Task 1; immutable evidence/event binding and privacy are Tasks 2–3; termination proof and the failed-not-succeeded Attempt rule are Tasks 3–4; exact usage/no-effect settlement and crash replay are Tasks 4–5; unsupported production source and V1 status are Task 5.
- **Placeholder scan:** no task is delegated to an unspecified “later” implementation; the production-grade Provider evidence source, Attempt-bound termination supervisor, and automatic application startup composition are explicit prerequisites, stay unavailable/fail-closed, and remain V1 blockers. Until startup composition exists, the host service is an internal explicit API only.
- **Type consistency:** `ProviderCallReconciliation` is introduced in Task 2 and consumed by the verifier in Task 3; `ProviderCallSnapshot` carries persisted call/proof state into the service; `reconcile()` writes proof and delegates to `apply_pending_settlements()`; the latter calls existing `Scheduler.reconcile_attempt()` and then writes the journal marker.
- **Review Focus mapping:** all five risks have explicit tests in their owning tasks: Task 3 source evidence, Task 1 header scope, Task 4 termination, Task 5 response race, and Task 5 crash matrix.

## Execution Gate

This plan is ready for review, not execution. Even if this plan is approved, implementation must not begin until the user chooses the execution method and explicitly approves execution. The repository's previous choice of an isolated `codex/` worktree is retained; every implementation task commit must be pushed to `origin/codex/p3-systemd-termination-receipts`.
