# Provider Sender Supervision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Run each Gateway HTTPS request in a host-supervised, one-request systemd service and persist a host-verified, exact-Attempt stop receipt before returning its outcome.

**Architecture:** A fixed standard-library child accepts one bounded JSON request on stdin, sends one HTTPS POST without redirects or proxies, and returns a bounded response frame. A dedicated systemd launcher gives that sender a unique service/cgroup and its own network-enabled hardening profile; the existing Worker/Tool profiles keep network disabled. The host alone verifies the exact inactive unit and empty cgroup, constructs the receipt, and records it with the call outcome.

**Tech Stack:** Python 3.12, asyncio, urllib, SQLite event store, systemd user transient services, cgroup v2, pytest.

**Spec:** `docs/superpowers/specs/2026-10-01-provider-sender-supervision-design.md`

## Global Constraints

- This work remains the single-user local Linux V1 profile measured on Ubuntu 24.04 / WSL2 / systemd.
- It must not run task text or arbitrary child commands, weaken Tool/Worker isolation, dispatch or replay unknown calls, record API credentials or request/response bodies in events, or claim measured provider cost/token savings.
- No paid Provider credentials are required for tests.
- A missing or unverified stop receipt leaves an unknown Provider outcome unresolved; it never frees Scheduler budget or concurrency holds.
- Preserve backward replay of existing ProviderCallOutcomeRecorded events without sender receipts.
- Only a host-verified exact sender unit/cgroup may produce ProviderSenderTerminationReceipt. Child IPC never carries a trusted receipt.
- Do not fall back to a host urllib thread or untracked child when systemd/cgroup verification is unavailable.

## Review Focus

- Cancellation while the launcher is still creating the service: no detached late request may escape and Gateway must not return before stop is proved.
- A sender with a different call, Attempt fence, route, reservation, model, Registry or request hash: reject its receipt before reconciliation.
- The service exits while a descendant remains, or the cgroup path differs from the expected unit: withhold the receipt and retain unknown holds.
- Malformed, duplicate-key, oversized, credential-bearing or child-forged receipt IPC: reject it without logging or persisting request data.
- Journal failure after a verified stop: preserve the unresolved call and never claim that a replay is safe.

## Task 1: Persist call-bound sender receipts

**Files:**
- Create: `src/orchestrator/models/provider_sender.py`
- Modify: `src/orchestrator/models/provider_calls.py`
- Modify: `src/orchestrator/models/__init__.py`
- Modify: `src/orchestrator/persistence/events.py`
- Test: `tests/unit/models/test_provider_call_journal.py`
- Test: `tests/contract/test_cross_spec_events.py`

**Interfaces:**
- Produces an immutable `ProviderSenderTerminationReceipt` with full ProviderCall/Attempt binding, a random systemd unit name, a SHA-256 digest of the exact cgroup path, inactive/failed unit state, `cgroup_empty=True`, and an aware observation time. It persists no absolute path or credential.
- `SQLiteProviderCallJournal.record_outcome(..., termination_receipt=None)` revalidates the optional receipt against its prior intent. `ProviderCallSnapshot.termination_receipt` exposes that durable proof after replay. Older outcomes without the optional field remain readable.

- [x] **Step 1: Write failing journal and contract tests.**

```python
def test_provider_call_journal_replays_attempt_bound_sender_receipt(tmp_path):
    store_path = tmp_path / "sender-receipt.db"
    store = SQLiteEventStore(store_path)
    request = model_request(model_registry())
    journal = SQLiteProviderCallJournal(store)
    journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-test",
    )
    call = journal.read(request)
    assert call is not None
    receipt = ProviderSenderTerminationReceipt(
        provider_call_stream_id=call.stream_id, run_id=call.run_id,
        node_id=call.node_id, attempt_id=call.attempt_id,
        fencing_generation=call.fencing_generation,
        accepted_route_id=call.accepted_route_id,
        budget_reservation_id=call.budget_reservation_id,
        provider_id=call.provider_id, model_id=call.model_id,
        registry_manifest_hash=call.registry_manifest_hash,
        request_hash=call.request_hash,
        unit_name="maestro-provider-" + "a" * 32 + ".service",
        cgroup_path_hash="sha256:" + "b" * 64,
        active_state="inactive", cgroup_empty=True,
        observed_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    journal.record_outcome(
        request, outcome="unknown", failure_code="timeout",
        termination_receipt=receipt,
    )
    store.close()
    with SQLiteEventStore(store_path) as reopened:
        recovered = SQLiteProviderCallJournal(reopened).read(request)
        assert recovered is not None and recovered.termination_receipt == receipt
```

Also reject a receipt copied from another call or fencing generation and a
malformed cgroup digest. Keep a legacy no-receipt outcome fixture and assert it
replays with `termination_receipt is None`.

- [x] **Step 2: Run the focused tests and verify they fail for the missing receipt contract.**

Run: `python3 -m pytest tests/unit/models/test_provider_call_journal.py tests/contract/test_cross_spec_events.py -k sender_receipt -q`

Expected: FAIL because receipt schema/persistence is absent; any import, fixture,
or unrelated test error must be corrected before implementation.

- [x] **Step 3: Implement the receipt model, snapshot replay, journal validation, and optional strict event payload.**

```python
def _validate_sender_receipt(call, receipt):
    receipt = revalidate_model(ProviderSenderTerminationReceipt, receipt)
    bindings = (
        (receipt.provider_call_stream_id, call.stream_id),
        (receipt.run_id, call.run_id),
        (receipt.node_id, call.node_id),
        (receipt.attempt_id, call.attempt_id),
        (receipt.fencing_generation, call.fencing_generation),
        (receipt.accepted_route_id, call.accepted_route_id),
        (receipt.budget_reservation_id, call.budget_reservation_id),
        (receipt.provider_id, call.provider_id),
        (receipt.model_id, call.model_id),
        (receipt.registry_manifest_hash, call.registry_manifest_hash),
        (receipt.request_hash, call.request_hash),
    )
    if any(receipt_value != call_value for receipt_value, call_value in bindings):
        raise ValueError("sender receipt does not match ProviderCall intent")
    return receipt
```

Reject every mismatched Attempt/call binding before append. Keep old event
payloads valid and do not include request/response bytes or raw cgroup paths.

- [x] **Step 4: Run the focused journal and cross-spec tests; verify receipt replay and legacy compatibility pass.**

Run: `python3 -m pytest tests/unit/models/test_provider_call_journal.py tests/contract/test_cross_spec_events.py -q`

Expected: PASS, including the pre-existing event-contract and replay cases.

- [ ] **Step 5: Commit and push Task 1.**

```bash
git add src/orchestrator/models/provider_sender.py src/orchestrator/models/provider_calls.py src/orchestrator/models/__init__.py src/orchestrator/persistence/events.py tests/unit/models/test_provider_call_journal.py tests/contract/test_cross_spec_events.py
git commit -m "feat: persist Provider sender termination proofs"
git push origin codex/p3-systemd-termination-receipts
```

## Task 2: Add a bounded systemd Provider sender

**Files:**
- Create: `src/orchestrator/runtime/provider_sender_process.py`
- Create: `src/orchestrator/isolation/provider_sender.py`
- Modify: `src/orchestrator/isolation/launcher.py`
- Modify: `src/orchestrator/isolation/__init__.py`
- Test: `tests/unit/runtime/test_provider_sender_process.py`
- Test: `tests/unit/isolation/test_provider_sender.py`
- Test: `tests/integration/test_systemd_provider_sender.py`

**Interfaces:**
- `decode_provider_sender_request(frame: bytes) -> ProviderSenderRequest` accepts only the versioned, bounded, unique-key child request schema. `decode_provider_sender_response(frame: bytes, *, max_response_bytes: int)` rejects extra fields, including any child-claimed receipt.
- `SystemdProviderSenderLauncher.launch(frame: bytes, *, timeout_seconds: int, output_bytes: int) -> SystemdProviderSenderSession` starts only the fixed trusted helper, with a unique `maestro-provider-<32 hex>.service` in `app.slice`, a dedicated network-enabled hardening profile, bounded stdin/stdout/stderr, private temporary storage, no request data in argv/environment, and no workspace mount.
- `SystemdProviderSenderSession.cancel() -> bool` signals only its unit; `wait() -> ProviderSenderResult` returns the child response only with a host-created stop receipt. The host reuses the existing inactive-unit/empty-cgroup verifier and retained-staging behavior.
- Child IPC accepts one bounded, unique-key JSON frame, one credential-free HTTPS URL plus bounded request headers/body/response limit, and emits only status, approved response headers and base64 response bytes. Extra fields, including receipt-shaped data, are errors.

- [ ] **Step 1: Write failing frame and launcher tests.**

```python
def test_provider_sender_child_rejects_duplicate_keys_and_child_receipts():
    duplicate_keys = b'{"url":"https://api.example","url":"https://evil.example"}'
    child_receipt = b'{"status":200,"termination_receipt":{"cgroup_empty":true}}'
    with pytest.raises(ValueError, match="duplicate"):
        decode_provider_sender_request(duplicate_keys)
    with pytest.raises(ValueError, match="unexpected"):
        decode_provider_sender_response(child_receipt)

```

Add deterministic launcher tests for HTTPS-only URL validation and bounded
frames, plus systemd property fixtures proving wrong/missing unit or cgroup
state withholds the receipt and retains staging. The exact empty-cgroup fixture
must produce a host receipt; an existing Worker profile test must continue to
assert `PrivateNetwork=yes`.

- [ ] **Step 2: Run the focused tests and verify they fail because the sender components are missing.**

Run: `python3 -m pytest tests/unit/runtime/test_provider_sender_process.py tests/unit/isolation/test_provider_sender.py -q`

Expected: FAIL on missing provider sender APIs, not test collection errors.

- [ ] **Step 3: Implement the fixed child protocol and separate network-enabled systemd sender launcher.**

```python
class SystemdProviderSenderLauncher:
    def _new_unit_name(self) -> str:
        return f"maestro-provider-{uuid.uuid4().hex}.service"
```

Do not add a public arbitrary command, inherit ambient environment, weaken the
existing Worker/Tool profile, use shell invocation, or trust child stop data.
The host derives the expected cgroup before start and checks the exact unit and
strict `cgroup.events` `populated 0` after exit or kill.

- [ ] **Step 4: Run unit and live systemd sender tests.**

Run: `python3 -m pytest tests/unit/runtime/test_provider_sender_process.py tests/unit/isolation/test_provider_sender.py tests/integration/test_systemd_provider_sender.py -q`

Expected: PASS on measured Ubuntu 24.04 / WSL2 / systemd. Environments without
systemd skip only the live integration test; deterministic tests still pass.

- [ ] **Step 5: Commit and push Task 2.**

```bash
git add src/orchestrator/runtime/provider_sender_process.py src/orchestrator/isolation/provider_sender.py src/orchestrator/isolation/launcher.py src/orchestrator/isolation/__init__.py tests/unit/runtime/test_provider_sender_process.py tests/unit/isolation/test_provider_sender.py tests/integration/test_systemd_provider_sender.py
git commit -m "feat: supervise Provider requests in systemd"
git push origin codex/p3-systemd-termination-receipts
```

## Task 3: Bind async Gateway cancellation and durable outcomes

**Files:**
- Modify: `src/orchestrator/models/transport.py`
- Modify: `src/orchestrator/models/gateway.py`
- Modify: `src/orchestrator/models/provider_calls.py`
- Modify: `src/orchestrator/provider_reconciliation.py`
- Test: `tests/unit/models/test_provider_adapters_transport.py`
- Test: `tests/unit/models/test_provider_call_journal.py`
- Test: `tests/unit/test_provider_reconciliation.py`
- Test: `tests/integration/test_systemd_provider_sender.py`

**Interfaces:**
- `SystemdProviderHTTPSTransport.post_json(...) -> HTTPTransportResponse` uses Task 2's session handle and includes `termination_receipt` only after the host verifies stop.
- The transport receives `call_binding: ProviderCallSnapshot` from Gateway. Gateway obtains it by reading the intent it just appended; the sender request frame contains no receipt authority or event-store context.
- `HTTPTransportCancelled`, `HTTPTransportTimedOut`, and `HTTPTransportFailed` carry `may_have_been_sent` plus an optional host receipt. `ProviderModelGateway` journals those fields on every terminal transport path and response/decode path.
- A `ProviderSenderTerminationVerifier` validates that an offered receipt is the exact one persisted for the unresolved `ProviderCallSnapshot` and returns its stable digest. Provider-authoritative evidence stays independently required; the general fail-closed reconciliation default remains unavailable.

- [ ] **Step 1: Write failing Gateway transport and cancellation tests.**

```python
async def test_cancelled_gateway_waits_for_sender_cgroup_before_recording_unknown():
    invocation = asyncio.create_task(gateway.invoke(request))
    await https_server.request_started.wait()
    invocation.cancel()
    with pytest.raises(ModelGatewayError, match="cancelled"):
        await invocation
    assert https_server.response_released.is_set()
    with SQLiteEventStore(database) as reopened:
        call = SQLiteProviderCallJournal(reopened).read(request)
        assert call is not None and call.status == "unknown"
        assert call.termination_receipt.attempt_id == request.attempt_id
        assert call.termination_receipt.cgroup_empty is True
```

Also cover real successful response + receipt, timeout, direct asyncio task
cancellation during service startup, a repeated cancel signal, unverifiable
stop (no receipt and unresolved hold), ProviderCallJournal write failure, and
legacy injected transport responses without receipts.

- [ ] **Step 2: Run the focused Gateway/systemd tests and verify the existing host-thread behavior cannot satisfy the new assertions.**

Run: `python3 -m pytest tests/unit/models/test_provider_adapters_transport.py tests/unit/models/test_provider_call_journal.py tests/unit/test_provider_reconciliation.py tests/integration/test_systemd_provider_sender.py -k 'sender or receipt or cancellation' -q`

Expected: FAIL because the current host-thread transport returns before the
actual request sender is stopped and does not persist a receipt.

- [ ] **Step 3: Implement the async supervisor bridge and receipt-aware journal flow.**

```python
async def _stop_and_wait(session, result_task):
    session.cancel()
    result = await asyncio.shield(result_task)
    if result.termination_receipt is None:
        raise HTTPTransportFailed(may_have_been_sent=True)
    return result
```

Shield sender creation until its handle is available; if the caller cancels
during startup, wait for that handle, kill its exact unit, and await this stop
helper. Monitor the cancellation signal and monotonic request deadline while
the sender wait runs. Never return on cancellation/timeout while a sender may
still be alive. A second cancel during proof, an unverified stop, or outcome
journal failure must remain unknown with no fabricated receipt. Do not persist
raw request, response or credential bytes.

- [ ] **Step 4: Run all transport, Gateway, journal, reconciliation, and systemd sender tests.**

Run: `python3 -m pytest tests/unit/models/test_provider_adapters_transport.py tests/unit/models/test_provider_call_journal.py tests/unit/test_provider_reconciliation.py tests/integration/test_systemd_provider_sender.py -q`

Expected: PASS with a persisted exact sender receipt for completed units;
unverified stops produce no receipt and no replay eligibility.

- [ ] **Step 5: Commit and push Task 3.**

```bash
git add src/orchestrator/models/transport.py src/orchestrator/models/gateway.py src/orchestrator/models/provider_calls.py src/orchestrator/provider_reconciliation.py tests/unit/models/test_provider_adapters_transport.py tests/unit/models/test_provider_call_journal.py tests/unit/test_provider_reconciliation.py tests/integration/test_systemd_provider_sender.py
git commit -m "feat: bind Gateway outcomes to sender stop proofs"
git push origin codex/p3-systemd-termination-receipts
```

## Task 4: Full acceptance, documentation, and review

**Files:**
- Modify: `README.md`
- Modify: `docs/security/provider-call-journal.md`
- Modify: `docs/security/platform-support.md`
- Modify: `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`
- Test: the full test suite, including live systemd sender integration on the measured host.

**Interfaces:** Task 1's optional replayable receipt, Task 2's host-observed
systemd cgroup receipt, and Task 3's async Gateway mapping must be connected and
covered together here. No functional Worker or task-dispatch interface is
introduced by this plan.

- [ ] **Step 1: Document measured behavior and remaining V1 limitations.**

Update the Provider journal/README/platform docs to state the fixed helper,
credential delivery over stdin, fail-closed cgroup proof, no-fallback behavior,
measured platform, tests performed, and the still-unavailable live Provider
authoritative lookup. Do not claim paid Provider benchmarks or V1 completion.

- [ ] **Step 2: Run the complete acceptance suite.**

Run: `python3 -m coverage run --source=src/orchestrator -m pytest -q`

Expected: PASS for the complete suite. Then run
`python3 -m coverage report --skip-covered --fail-under=90`, `python3 -m compileall -q src tests`,
`python3 -m pip check`, `python3 -m build --wheel`, and `git diff --check`.

- [ ] **Step 3: Prepare and complete one independent whole-branch review.**

Use `superpowers:requesting-code-review/code-reviewer.md` with merge base
`5bb9e58699b8f86ddb54a497fde273a19af35bac`, this plan/spec and its Review Focus.
Critical/Important findings get one failing-test-first fix pass and a green full
suite; Minor findings are recorded without widening this slice.

- [ ] **Step 4: Record verification, commit the docs/fixes, push, and verify the remote ref.**

```bash
git add README.md docs/security/provider-call-journal.md docs/security/platform-support.md docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md
git commit -m "docs: record supervised Provider sender contract"
git push origin codex/p3-systemd-termination-receipts
git ls-remote origin refs/heads/codex/p3-systemd-termination-receipts
```

## Acceptance

- Actual Provider HTTPS bytes are sent only by the fixed child in its own unique systemd service/cgroup; no host urllib send thread remains in the default path.
- Every completed response/transport failure is returned only after the exact service is inactive and its cgroup is empty, or it fails closed without a receipt.
- The durable outcome receipt matches the intent's full route/Attempt/fence and remains replayable after a process restart without disclosing absolute cgroup paths or request/response/credential data.
- Unknown Provider calls are never replayed and cannot release Scheduler holds without separately verified Provider evidence.
- The existing Worker/Tool network-denial, receipt, and isolation acceptance tests still pass; strict two-decimal total coverage is at least 90%.

Functional Worker execution and task dispatch remain a separate next slice.
