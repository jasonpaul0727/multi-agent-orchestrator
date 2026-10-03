# Provider Call Reconciliation Design

**Status:** Draft for user review
**Date:** 2026-09-29
**Target slice:** P3 — authoritative Provider evidence and replay-safe budget reconciliation

## 1. Problem and goal

The current Provider journal persists an intent before dispatch and a sanitized terminal outcome afterward. If the process dies after the Provider may have received a request but before the terminal event is durable, the call remains `dispatching` or `unknown`. The Gateway correctly blocks the same call identity from being dispatched again, but there is no authoritative evidence path that can resolve the held budget or feed the existing Scheduler reconciliation operation.

This slice adds a durable, host-only path to correlate an ambiguous OpenAI Responses request with out-of-band Provider evidence, validate that evidence against the exact original call, and idempotently reconcile its budget and Attempt. The contract must keep unresolved calls fail-closed when evidence is missing, unsupported, ambiguous, stale, or mismatched.

The slice is not a generic “query every Provider” feature. No general API can be assumed to retrieve an exact result for every Provider or endpoint. In particular, OpenAI documents `X-Client-Request-Id` as a request-correlation aid that support can use to look up whether a request was received and when; it is not itself an outcome or usage receipt. OpenAI organization Usage APIs return time-bucketed, grouped aggregates, not a per-Attempt receipt. Treating either as exact evidence would be an unsafe inference. See [OpenAI request debugging](https://developers.openai.com/api/reference/overview) and [OpenAI Completions usage](https://developers.openai.com/api/reference/resources/admin/subresources/organization/subresources/usage/methods/completions).

## 2. Existing behavior and boundary

- `SQLiteProviderCallJournal` writes `ProviderCallIntentRecorded` before network dispatch and one `ProviderCallOutcomeRecorded` afterward. It stores a request-body hash and accepted route/budget/registry bindings, not prompts, credentials, raw request/response bodies, or completion text.
- `unresolved()` currently returns `dispatching` and `unknown` metadata. It does not query a Provider, verify evidence, settle a budget, release an Attempt slot, resume a Worker, or permit replay.
- `Scheduler.reconcile_attempt()` can settle an `OutcomeUnknown` reservation from validated usage or a known-no-effect proof, then mark the Attempt reconciled and release its slot. It is currently a trusted host-side operation; it does not verify Provider evidence itself.
- The OpenAI Responses adapter currently sends `store: false`. This design does not change retention or retrieve a lost response body.

## 3. Goals and non-goals

### Goals

1. Persist one host-generated, opaque correlation ID with each eligible OpenAI Responses intent and send it on that exact outbound request.
2. Define a bounded, provider-specific evidence-verifier contract. Only a verifier result bound to the complete original call can resolve an unknown Provider effect.
3. Persist reconciliation proof metadata append-only and make concurrent reconciliation, late Gateway outcomes, and crash/restart recovery deterministic and idempotent.
4. Apply validated usage or no-effect evidence through the existing budget/Scheduler reconciliation invariants without double settlement or premature slot release.
5. Keep unsupported providers and unverifiable evidence held, with no automatic replay and no paid Provider calls in CI.

### Explicit non-goals

- Implementing a universal Provider lookup API or claiming that support correlation is machine-verifiable evidence.
- Treating an OpenAI Usage aggregation, request ID, HTTP status, timeout, or operator-supplied assertion as proof by itself.
- Retrying the original Provider request, automatically creating a replacement Attempt, or changing idempotency semantics.
- Retrieving or persisting the lost prompt, request body, response body, or model output; changing `store: false`; or upgrading an Attempt to success without its durable output and normal verification.
- Reconciling already-known terminal Provider outcomes or invoice corrections in this first slice.
- Live paid-provider integration tests or validating the resume/recovery behavior of the whole Worker runtime.

## 4. Design decisions

### 4.1 Correlation ID is an aid, not authority

For an explicitly enabled first-party OpenAI Responses route, the Gateway generates a fresh opaque ASCII correlation ID before recording intent, persists that value in the intent event, and sends it as `X-Client-Request-Id`. It must not be derived from a prompt, secret, user-supplied value, or unbounded internal identifier. It is unique per actual dispatch and is never regenerated during recovery. The implementation should use a bounded value far below OpenAI's documented 512-character ASCII limit.

Only a route explicitly marked as the supported OpenAI Responses capability may receive this header. Do not infer permission from a generic “OpenAI-compatible” adapter or send it to arbitrary custom endpoints. The existing `X-Request-Id` header remains an internal/request tracing value and is not substituted for the Provider correlation ID.

The Provider's returned request ID is retained as sanitized correlation metadata when available. Neither ID alone changes `dispatching`/`unknown` or authorizes budget settlement.

### 4.2 Evidence contract and trusted boundary

Add a typed, size-bounded reconciliation evidence envelope accepted only by a host-side reconciliation service. Raw evidence is presented to an injected provider-specific `ProviderEvidenceVerifier`; the service does not accept a Worker, CLI, or MCP self-assertion as Provider proof. A verifier returns a validated immutable result only after checking the evidence source and all call bindings.

The validated result must bind at least:

- Provider and supported adapter/capability;
- correlation ID and Provider request ID where available;
- Provider call stream ID and exact request-body hash;
- Run, Node, Attempt, fencing generation, accepted route ID, model/registry hash, and budget reservation ID;
- a definitive effect classification (`not_received` or `received_and_charged`), exact normalized usage/cost evidence for the latter, evidence source class, evidence timestamp, and a digest/reference for audit.

`pending`, `not_found_but_not_final`, partial aggregate usage, mismatched identity, unsupported source, invalid signature/authority, missing exact usage, and stale or malformed evidence all remain unresolved. A “not found” lookup only qualifies as `not_received` if the provider-specific verifier can prove the lookup is authoritative and final for this unique call; otherwise it is ambiguous.

For the first OpenAI slice, the verifier interface may be exercised with deterministic fake receipts in tests. Until a production-grade exact-evidence source is implemented and configured, OpenAI support lookup/Usage aggregation can help an operator investigate but cannot be automatically accepted as a verified receipt. No production caller may manufacture a `VerifiedProviderEvidence` object by directly setting its fields; construction is owned by the verifier boundary.

### 4.3 Separate Provider billing fact from Agent Attempt success

A Provider receipt that a request was processed or billed does not prove that Maestro durably received, admitted, and verified the model output. Because this path exists specifically for calls whose terminal result was lost, a verified `received_and_charged` receipt settles actual usage but reconciles the Agent Attempt as `failed` (output unavailable), not `succeeded`. A verified `not_received` receipt reconciles it as failed with known no effect. A Provider receipt alone can never synthesize a Worker artifact or pass the independent Verifier.

If the Attempt may still have a live sender/Worker, do not release its scheduler slot. Reconciliation requires host-verified termination evidence bound to the same Attempt and fencing generation, or another supervisor guarantee that the sender can no longer mutate state. If that proof is unavailable, evidence may be inspected but the Attempt/budget stay held.

### 4.4 Append-only and crash-replayable settlement

The Provider stream gains an immutable `ProviderCallReconciliationRecorded` event after either an intent-only `dispatching` stream or an `unknown` terminal outcome. Known terminal outcomes cannot be overwritten. The append uses the exact current stream version as a SQLite CAS; concurrent Provider response and reconciliation races therefore have one durable winner. A late Gateway outcome that loses the CAS is treated as a conflict and cannot overwrite reconciliation.

Provider proof persistence and Scheduler settlement span streams and cannot be treated as one atomic write. Use an idempotent three-step operation:

1. Verify all evidence and Attempt-termination bindings, then append `ProviderCallReconciliationRecorded` with only sanitized evidence metadata and the exact normalized settlement fact.
2. Invoke `Scheduler.reconcile_attempt()` with the same evidence-derived usage or known-no-effect result and `outcome="failed"`.
3. After Scheduler reconciliation succeeds, append `ProviderCallSchedulerSettlementApplied` bound to the reconciliation event.

If the process dies between steps 1 and 2, startup recovery finds a verified reconciliation without a settlement-applied marker and retries step 2. Scheduler idempotency makes repeating step 2 safe if the Scheduler committed but the caller died before step 3. The budget reservation remains held until Scheduler reconciliation commits. An event with verified proof but no settlement marker is `settlement_pending`, not fully reconciled.

The provider journal projection must validate event order, full stream identity, causation, and allowed state transitions, including:

```text
intent -> outcome(unknown) -> reconciliation -> scheduler_settlement_applied
intent -> reconciliation -> scheduler_settlement_applied
```

Known terminal outcomes (`not_sent`, `known_failure`, `known_success`) do not accept a reconciliation event in this slice. A reconciliation event is one-shot; conflicting second evidence is rejected. An exact idempotent replay returns the prior result.

### 4.5 Privacy and boundedness

Journal only opaque IDs, enums, timestamps, usage/cost primitives, hashes, and bounded evidence references. Do not persist provider support transcripts, email contents, raw signed bodies, prompts, outputs, credentials, or raw Provider payloads. Hash evidence input before discarding it when policy permits. Redaction must cover the new event and error paths. Verify strict schemas, duplicate-key rejection where JSON is used, maximum size, safe identifiers, and exact `UsageRecord` currency/reservation/run binding.

## 5. State/result semantics

| Provider evidence | Provider journal state | Budget | Attempt |
| --- | --- | --- | --- |
| Missing, pending, ambiguous, aggregate-only, unsupported, stale, or mismatched | `unknown` / unresolved | Held | `OutcomeUnknown`; slot held |
| Exact authoritative proof request was not received, plus verified sender termination | `reconciled` | Release as known-no-effect | `failed`; slot released once |
| Exact authoritative proof request was received and exact usage is known, plus verified sender termination | `reconciled` | Commit actual usage/cost | `failed` because durable output is unavailable; slot released once |
| Exact receipt says processed but exact usage is unavailable | unresolved | Held | `OutcomeUnknown`; slot held |
| Exact proof persisted, process died before Scheduler settlement | `settlement_pending` | Held until replay | `OutcomeUnknown` until idempotent Scheduler operation completes |

No row authorizes a retry of the same Provider call. A later retry, if a future policy allows one, is a distinct Attempt with a new call identity and its own budget reservation and approval checks.

## 6. Testing and acceptance criteria

All tests use a fake transport, fake provider evidence verifier, temporary SQLite stores, and controlled subprocess termination; no test requires a real credential or paid Provider call.

1. Correlation ID is opaque, ASCII, bounded, unique per dispatch, durably stable through restart, present only for the explicitly enabled OpenAI Responses route, and absent from logs/errors if redaction rules require it.
2. Evidence is rejected for every mismatch in Provider, correlation/request ID, stream/request hash, Run/Node/Attempt/fence, route, model/registry, reservation, currency, or stale termination proof.
3. Ambiguous, unsupported, partial/aggregate-only, malformed, duplicate-key, oversized, or verifier-failed evidence does not append a reconciliation event, settle budget, free a slot, or permit replay.
4. A definitive no-delivery receipt releases the unknown reservation exactly once and fails/releases the matching Attempt exactly once.
5. Exact accepted usage commits the reservation exactly once and fails/releases the matching Attempt without manufacturing success output.
6. Concurrent reconcilers, concurrent late Gateway outcomes, and duplicate identical requests produce one valid journal winner; conflicting second receipts fail closed.
7. Crash injection after proof persistence, after Scheduler settlement but before settlement marker, and after settlement marker proves restart replay is idempotent and no reservation is double-settled or slot double-released.
8. Reconciliation before verified sender termination is rejected; a stale or wrong-fence termination receipt is rejected.
9. Provider event privacy, redaction, schema bounds, event-chain validation, full unit/integration suite, coverage gate, compile/package checks all pass.

## 7. Rollout and completion boundary

Deliver the slice behind an explicit provider capability and verifier registration. Unsupported routes remain unchanged and fail closed. The rollout must not imply that OpenAI support correlation or Usage aggregates have become automated proof. A provider is reconciliation-capable only when its configured verifier can provide exact, authoritative evidence for the exact call.

Completion of this spec/implementation closes only the Provider unknown-outcome reconciliation gap. It does not by itself complete V1: independent Worker/Verifier lifecycle wiring, CLI/MCP production identity, end-to-end security acceptance, and real measured cost/token/rework benchmarks remain separately tracked.

## 8. Self-review checklist

- Does correlation ever masquerade as outcome proof? **No** — only a configured evidence verifier can produce the validated envelope.
- Can a missing/unsupported/mismatched receipt spend or release budget? **No** — unresolved paths stay held.
- Can evidence mark a lost-output Attempt successful? **No** — Provider settlement and artifact/Verifier success remain distinct.
- Can a live or stale-fence Worker be released? **No** — same-Attempt host termination evidence is required.
- Can a crash between streams double-charge, double-release, or lose the proof? **No** — append-only proof plus idempotent Scheduler settlement and a replay marker cover each crash window.
- Does this claim an available automatic OpenAI lookup that the official contract does not provide? **No** — the exact-evidence production adapter remains an explicit prerequisite; support correlation is only a lookup aid.

## References

- [Current Provider call journal contract](../../security/provider-call-journal.md)
- [V1 control-plane architecture decisions](2026-09-23-v1-control-plane-architecture-decisions.md)
- [P3 systemd termination-receipts slice](../plans/2026-09-28-p3-systemd-termination-receipts.md)
- [OpenAI API request debugging and `X-Client-Request-Id`](https://developers.openai.com/api/reference/overview)
- [OpenAI organization Completions Usage API](https://developers.openai.com/api/reference/resources/admin/subresources/organization/subresources/usage/methods/completions)
