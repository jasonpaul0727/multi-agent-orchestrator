# Provider Call Intent, Evidence, and Settlement

`orchestrator.models.SQLiteProviderCallJournal` is the durable boundary used by
`ProviderModelGateway` before it dispatches a model request. A Gateway with no
journal fails closed before asking the Secret Broker for a credential or
calling the HTTP transport.

Each call uses a deterministic stream identity derived from Run, Node, Attempt,
fencing generation, and request ID. The host appends one
`ProviderCallIntentRecorded` event before network dispatch. The event binds the
accepted route, budget reservation, registry hash, and hashes of the provider
request body and idempotency key; it never stores prompts, raw request bodies,
credentials, provider response bodies, or output text. A SQLite version CAS
allows only one process to claim a call scope.

After dispatch, the Gateway appends one `ProviderCallOutcomeRecorded` fact:
`not_sent`, `known_failure`, `known_success`, or `unknown`. A known-success
receipt can retain sanitized token/fee usage, HTTP status, and provider request
ID. The event contract rejects orphaned/duplicate terminal events, mismatched
Attempt identity, malformed hashes, and unrecognized usage fields. If terminal
receipt persistence fails, the caller receives `outcome_unknown`; the durable
intent remains unresolved. A second dispatch using the same call identity is
blocked, even after process restart. If a late terminal receipt races with a
reconciliation proof, the provider-call stream CAS allows only one event to
follow the intent; the loser receives a typed journal conflict.

## Reconciliation contract

`SQLiteProviderCallJournal.unresolved()` exposes only calls in `dispatching` or
`unknown`. Task-scoped correlation IDs are currently emitted for the
first-party OpenAI Responses adapter. `ProviderReconciliationService` accepts
only bounded raw evidence from an injected `ProviderEvidenceVerifier`; it
checks the evidence digest against those exact bytes and revalidates the full
Attempt, fence, route, reservation, Registry, and request-hash binding. A
separate injected `AttemptTerminationVerifier` must return a hash for the
matching stopped sender. Provider evidence alone can never assert that the
Worker stopped, and a local stop receipt alone can never establish Provider
charges.

Before appending proof, the service requires the corresponding Scheduler
Attempt to be `OutcomeUnknown` at the exact fence and rejects a reconciliation
timestamp older than that event. It then appends
`ProviderCallReconciliationRecorded` before asking
`Scheduler.reconcile_attempt()` to settle the budget and release the slot. A
`not_received` proof resolves the unknown hold as a committed zero-cost
settlement; a `received_and_charged` proof uses the exact committed `UsageRecord`
and Provider-reported currency/token/cost values. In both cases, the Attempt is
recorded as **failed**, because the lost model output is not available to the
orchestrator and Provider charge evidence does not prove task success. The
service appends `ProviderCallSchedulerSettlementApplied` only after the
Scheduler transaction succeeds.

If the process stops after the proof but before Scheduler settlement, or after
Scheduler settlement but before the journal marker, the call remains
`settlement_pending`. The host can explicitly call
`ProviderReconciliationService.apply_pending_settlements()` after reopening the
database. It reuses only the persisted proof and its `reconciled_at`, relies on
Scheduler idempotency for the cross-stream crash window, and never asks the
Provider verifier again. A durable settlement marker projects the call as
`reconciled`; replay does not duplicate budget events or release the slot.
`pending_settlements()` reports proof waiting for Scheduler application, but
neither projection authorizes Gateway replay. `ControlPlaneApplication` now
applies pending proofs during its atomic startup transaction, before admission
becomes available. Startup checks all Run/call bindings and rolls back the
entire settlement batch on a failure. See [startup recovery](control-plane-startup.md).
The APIs are not yet exposed through CLI or MCP.

## Production status and limits

The default Gateway transport is now `SystemdProviderHTTPSTransport`. It sends
one bounded HTTPS request through the fixed helper in a dedicated transient
systemd service. Credential and request bytes cross a bounded stdin frame;
they are not stored in the journal. The Gateway maps a host-verified exact
unit/cgroup stop result to a receipt bound to the persisted call, full Attempt,
route, reservation, Registry, and request hash. On cancellation or timeout it
waits for that stop result before returning. If systemd cannot prove the exact
sender stopped, the Gateway supplies no receipt and leaves the call unresolved;
it does not fall back to an urllib thread or another untracked sender.

`ProviderSenderTerminationVerifier` can validate that a supplied receipt is
the exact receipt already persisted for the unresolved call. It is opt-in:
`ProviderReconciliationService` still defaults to unavailable Provider and
Attempt verifiers. There is no production OpenAI authoritative lookup or
signed-receipt verifier wired to the service. The local live integration test
uses a loopback TLS sink to prove sender execution and cancellation/stop
ordering; it makes no Provider request and incurs no Provider charge. A sender
stop receipt does not prove that the Provider did not receive or charge the
request, nor that the entire Worker stopped. Therefore this slice does **not**
enable production Provider reconciliation. Without configured authoritative
verifiers, unknown calls remain held and unresolved. The service does not
automatically retry/replay a Provider call, resume a Worker, publish artifacts,
or decide that a task succeeded.

The deterministic tests cover concurrent call claims, late-success versus
reconciliation CAS, restart recovery, privacy of prompts/output/credentials
and raw evidence, malformed event rejection, exact usage/no-effect settlement,
and spawned-process death after proof append, Scheduler settlement, and marker
commit. Live authoritative Provider verification, production Worker recovery,
and CLI/MCP remain open P3/V1 gates. Pending-proof startup recovery is now
implemented by the host application entry point.
