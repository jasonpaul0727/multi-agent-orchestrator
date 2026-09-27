# Provider Call Intent and Receipt Journal

`orchestrator.models.SQLiteProviderCallJournal` is the durable boundary used
by `ProviderModelGateway` before it dispatches a model request. A Gateway with
no journal fails closed before asking the Secret Broker for a credential or
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
blocked, even after process restart.

`SQLiteProviderCallJournal.unresolved()` exposes only metadata for calls in
`dispatching` or `unknown`, so a future recovery service can reconcile them
without reconstructing prompt contents. It does **not** query a Provider,
verify an external invoice, settle the Scheduler budget, resume a Worker, or
authorize a retry. No generic Provider API can prove the result of every lost
response; a provider-specific authoritative lookup/evidence adapter is still
required. Until then unknown calls remain held and are never automatically
replayed. This slice is a recovery prerequisite, not completion of P3 or a
claim of Provider reconciliation.

The deterministic tests cover concurrent claims across SQLite connections,
restart recovery of unresolved calls, sensitive-payload exclusion, scope
binding, malformed event rejection, successful receipts, unknown outcomes,
missing-journal fail-closed behavior, and receipt-write failure after a
successful transport response.
