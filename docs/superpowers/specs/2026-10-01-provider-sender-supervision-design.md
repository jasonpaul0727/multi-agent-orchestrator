# Provider sender process supervision

## Outcome

Every HTTP request sent by `ProviderModelGateway` runs in its own fixed,
host-created systemd service. The host returns a response or a timeout/cancel
failure only after it observes that exact unit inactive and its attempt cgroup
empty. If it cannot prove the sender stopped, the Gateway records an unknown
Provider outcome without a stop receipt, so Scheduler budget and concurrency
holds remain in place. A stop receipt is retained in the Provider-call journal
with its exact Attempt binding, allowing a trusted reconciliation verifier to
use it after process restart.

## Context and constraints

`UrllibHTTPSTransport` currently sends through `asyncio.to_thread`. Cancelling
the asyncio task does not stop the blocking `urllib` call in its host thread.
`SystemdReadOnlyLauncher` cannot run the sender because its profile deliberately
blocks networking. Its host-observed receipt is therefore valid only for that
launcher’s process tree, not for the Gateway's current HTTP request.

This work remains the single-user local Linux V1 profile measured on Ubuntu
24.04 / WSL2 / systemd. It must not run task text or arbitrary child commands,
weaken Tool/Worker isolation, dispatch or replay unknown calls, record API
credentials or request/response bodies in events, or claim measured provider
cost/token savings. No paid Provider credentials are required for tests.

## Approaches considered

1. Keep urllib in a thread and attempt to cancel its socket. urllib does not
   expose a stable cancellation handle for the blocking operation, so this
   cannot prove the actual sender stopped.
2. Start a normal child process and kill its PID/process group. Descendants can
   create another session and escape process-group checks, so an empty process
   group is weaker than the existing cgroup termination contract.
3. Start a one-request, network-enabled systemd transient service under a
   unique unit and cgroup. The child receives one bounded request over stdin,
   runs only a fixed trusted HTTP helper, and returns one bounded response.
   The host owns the unit handle and creates the receipt from systemd and
   cgroup observations. This reuses the measured Linux supervisor and is the
   selected approach.

The provider service must use a separate profile from read-only Worker/Tool
services: outbound networking is required, while task code, workspace mounts,
ambient environment variables, redirects, proxies, shell commands, and
unbounded output remain unavailable. URL and authentication headers come only
from the host Gateway after frozen-Registry and Secret Broker checks. The
service sends exactly one HTTPS POST and exits. Its command line contains no
credential or request data; `LimitCORE=0`, private temporary storage and a
sanitized environment prevent accidental disclosure.

## Components and data flow

1. Gateway validates the accepted route and obtains the credential as it does
   today, then journals its intent.
2. A Provider sender launcher creates an attempt-specific systemd service,
   writes a bounded request frame to its stdin, and drains bounded stdout and
   stderr. A standard-library child helper validates the frame, sends one
   HTTPS POST with redirects and ambient proxies disabled, and emits a bounded
   response frame. It has no EventStore, Workspace, Tool, Approval, or Secret
   Broker handle.
3. The host validates the response and verifies the exact unit is inactive and
   the exact cgroup reports `populated 0`. It then creates an immutable
   `ProviderSenderTerminationReceipt` binding call stream, Run, Node, Attempt,
   fencing generation, accepted route, reservation, model, Registry hash,
   request hash, unique unit name, a digest of the exact cgroup path, observed
   unit state, and observation time. The absolute cgroup path is not persisted.
   Child stdout cannot supply or override this receipt.
4. Gateway attaches the receipt to both normal transport responses and typed
   timeout/cancellation failures. `SQLiteProviderCallJournal.record_outcome`
   verifies it against the recorded intent and persists its sanitized form in
   `ProviderCallOutcomeRecorded`. The schema remains backward-compatible with
   legacy outcome events which have no sender receipt.
5. Reconciliation can compare the persisted receipt to the call and Attempt.
   Provider evidence remains independently required. A missing/mismatched
   receipt, malformed IPC, unbounded output, unknown unit, populated cgroup,
   failed timeout/cancellation stop, or journal failure cannot authorize a
   replay or release unknown holds.

## Failure semantics

- Cancellation before a sender unit is created remains `not_sent`.
- Once a unit may have received the request, timeout/cancellation is `unknown`;
  the host waits for stop verification before returning. A confirmed stop receipt
  is journaled with the unknown outcome; no stop proof means the receipt is
  absent and the outcome remains held for manual/retry-safe recovery.
- A normal HTTP result is not returned before the service is inactive and its
  cgroup is empty. Failure to prove termination becomes an unknown transport
  failure, even if a response frame was received.
- Redirects, proxies, non-HTTPS URLs, credential-bearing URLs, malformed or
  duplicate JSON keys, oversized frames/responses, and child-supplied receipt
  fields fail closed. Verifier and child exception text is not surfaced or
  persisted.
- Service-manager absence is an isolation failure. It does not fall back to a
  host thread or untracked process.

## Validation

- Unit tests exercise frame limits/duplicate keys, route-bound receipt
  validation, journal persistence/replay, no receipt on failed cgroup proof,
  and Gateway mapping for success, timeout, cancellation, and journal failure.
- A local self-signed HTTPS server and a test-only trusted CA exercise the
  complete Gateway-to-child transport without any paid Provider. Live systemd
  tests inspect the sender's exact unit/cgroup while it is running, then prove
  normal exit and cancellation leave no live process before result return.
- Existing Worker/Tool network-denial and termination tests must remain green;
  provider-send network access applies only to the dedicated fixed helper.
- Full tests, two-decimal 90% coverage gate, compilation, dependency check,
  wheel build, and independent review must pass before pushing.

## Out of scope

This adds neither a model-executing Worker nor task dispatch, verifier,
Publisher, CLI/MCP, provider billing lookup, nor claims of measured cost/token
improvement. Functional Worker integration is the next separate slice and must
consume the Gateway's persisted sender outcome/receipt contract.
