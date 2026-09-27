# Worker and Verifier Boundary Contracts

`orchestrator.runtime.contracts` defines bounded host-to-Worker inputs and
Worker/Verifier proposals. `IsolatedWorkerProcess` now exercises that input
and result contract through a real systemd-isolated child process, but the
child deliberately returns only `blocked`: it never executes task text,
calls a model or tool, or authorizes a state transition.

## Contract guarantees

- Every message is bound to the exact Run, Node, Attempt, Agent instance,
  fencing generation, graph version, input manifest, EffectiveConfig,
  Registry, policy, routing decision, and frozen planning contract hashes.
- Worker inputs contain task text, opaque content digests, output byte limits,
  and scoped Tool capability references only. They contain no filesystem path,
  EventStore handle, control-directory handle, provider credential, or durable
  state mutation capability.
- A Worker response is a candidate, failure, or blocked proposal. A candidate
  must name at least one artifact, cannot relabel an input digest as a new
  output, and the aggregate declared size must fit the frozen byte limit.
- A Verifier receives an exact candidate artifact set and required check IDs.
  Its result must echo the same attempt, inspect exactly that artifact set,
  report every required check once, and cite only candidate digests. A passing
  proposal is not itself accepted execution state; each passing check must
  cite at least one candidate digest, while an inconclusive result may report
  only the candidate subset it managed to inspect.
- Inbound JSON is size-bounded, UTF-8, rejects duplicate object keys and
  non-finite constants, then validates against strict schemas with unknown
  fields forbidden. Errors do not echo payload values.

These checks establish message shape and causal binding, not truth.
`admit_candidate_artifacts` is a host-only gate that re-hashes ArtifactStore
bytes and requires each candidate digest, type, size, media type, Run, Node,
Attempt, fencing generation, and Agent ID to match a host publication. It
does not promote a candidate to success or attest to an independent check.

The blocked-only child receives at most 1 MiB through stdin, never via a
command argument or environment variable. It runs from a read-only trusted
runtime bind under the measured systemd profile; the host rejects incomplete
input writes, malformed output, child errors, unconfirmed termination, and
any unauthorized candidate. This is a process-boundary smoke test, not a
functional Agent. Artifact upload, Gateway/Approval/Secret Broker mediation,
independent Verifier execution, durable evidence acceptance, crash recovery,
and CLI/MCP application services remain unimplemented. No production Worker
profile is enabled by this slice.

## Local tests

`tests/unit/runtime/test_contracts.py` covers schema bounds, duplicate and
stale attempt bindings, input/output aliasing, aggregate size limits, exact
Verifier check/artifact sets, contradictory verdicts, duplicate JSON keys,
malformed UTF-8/JSON, and oversized frames. These are contract tests, not a
security proof for a process boundary. `test_worker_process.py`,
`test_artifact_admission.py`, and the live
`tests/integration/test_isolated_worker_process.py` cover the new blocked-only
IPC and host artifact admission boundaries.
