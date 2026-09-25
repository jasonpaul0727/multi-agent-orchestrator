# Worker and Verifier Boundary Contracts

`orchestrator.runtime.contracts` is the first P5 interface slice. The models
define bounded host-to-Worker inputs and Worker/Verifier proposals, but they do
not launch a process or authorize a state transition.

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

These checks establish message shape and causal binding, not truth. The host
must resolve each artifact digest through the trusted ArtifactStore, recheck
stored bytes and source provenance, independently run the required checks, and
persist accepted evidence through the control plane. Worker and Verifier
processes, IPC transport, capability consumption, crash recovery, and CLI/MCP
application services are not implemented by this contract slice. No execution
profile is enabled by adding these models.

## Local tests

`tests/unit/runtime/test_contracts.py` covers schema bounds, duplicate and
stale attempt bindings, input/output aliasing, aggregate size limits, exact
Verifier check/artifact sets, contradictory verdicts, duplicate JSON keys,
malformed UTF-8/JSON, and oversized frames. These are contract tests, not a
security proof for a process boundary.
