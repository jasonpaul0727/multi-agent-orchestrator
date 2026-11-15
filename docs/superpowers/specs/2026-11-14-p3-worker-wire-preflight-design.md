# P3 Worker wire encoder preflight: existing-limit enforcement before copying

Status: draft for independent review, then development under the user's continuous P3 instruction. This is a narrow product-code correction independent of deployment and the separate tests-only SQLite prototype.

## Problem and boundaries

`general_worker_protocol._turn_value` copies contract dictionaries, iterates all artifact/capability items and calls recursive `_plain` before `_validate_turn` enforces schema counts/lengths. Completion response is fully UTF-8 encoded before its byte cap is checked. Constructed/copied Pydantic models can bypass ordinary validation while retaining the exact contract type; existing tests deliberately require defensive rejection. Rejected oversized or structurally invalid input can therefore trigger unnecessary traversal/copy/encoding. This is not proof an untrusted Worker can construct such host objects or cause production OOM.

Correct only work done by the encoder on already-existing invalid input. No claim bounds whole RAM, existing object size, arbitrary integer serialization, FDs/staging or the service's pre-prepare admission. The execution service persists TurnPrepared before start_turn encodes; prepared/launch_unknown holds cannot be released by this change.

## Exact compatibility

Preserve v1 schema strings, canonical bytes for accepted values, task input commitment, UUID requirements, capability sorting/duplicate rules, artifact duplicate rules, exact built-in types and fixed `general_worker_protocol_invalid` public error. Preserve currently accepted builtin list or tuple artifact/capability collections. Do not add upper limits to currently unbounded valid fencing_generation, graph_version or artifact size integers. Preserve empty completion response acceptance and strict UTF-8/surrogate rejection. Do not change decode schema or Provider/stop/unknown rules.

## Implementation

Only modify `src/orchestrator/runtime/general_worker_protocol.py` and protocol/process test files, plus docs. Existing `_plain` remains available to decode/other paths; do not weaken validation or change canonical encoding.

Perform schema-aware bounded snapshot construction instead of running generic recursive `_plain` over outbound arbitrary input:

1. Validate exact GeneralWorkerTurn, phase shape/value, bounded canonical UUID4 launch ID, task exact WorkerTask type and task dictionary exact field shape. `_model_fields` must reject wrong dictionary size before enumerating unknown field names; use fixed trusted field names/expected count, not an unbounded scan of injected extras.
2. Snapshot required fields by fixed keys. Validate scalar exact type and existing character limits before UTF-8 work; validate context and nested model field shapes before copying arbitrary values. Fixed schema strings/integers go directly into a new bounded structure; wrong scalar builtin list/dict is rejected without recursive traversal.
3. Check exact list/tuple collection and existing artifact128/capability64 count before touching any element. Read at most that bounded count (fixed indices, no list iterator that can grow indefinitely), validate each exact nested contract and scalar field before adding its plain snapshot. Length/shape mutation during observation causes fixed rejection where detected; no linearizable or authenticated host-object snapshot is claimed. Never iterate a collection merely to decide it is too long.
4. Completion response exact string type and cheap character upper bound derived from `min(MAX_OUTPUT_BYTES, output_byte_limit)` precede exact UTF-8 byte check. Every Unicode code point needs at least one byte, so over-character-cap strings are already invalid; candidate encoding is bounded by the existing cap and Python UTF-8 maximum width. Request response must remain None. No substitute boolean/no-effect statement.
5. Reuse existing schema validators on the bounded copied output; canonical encoding/frame limit are still final gates. Any invalid model/subclass/constructed extra/surrogate/concurrent shape failure remains a detached fixed public error. Controls BaseException should retain existing propagation behavior.

## TDD and evidence

Use small finite inputs and call-count sentinels, not timing/RSS claims. Proposed tests first reproduce existing behavior:
- 129 real artifact refs or65 real capability refs: reject before any nested `_model_fields` element conversion.
- task_text of100001 chars, or a bounded nested builtin container in a scalar position: reject before `_plain`/UTF-8 traversal of the invalid value.
- contract dictionary with extra fields: reject by size before scanning/converting injected values; exact field type/name rules retained.
- response over character cap: reject before generic outbound copying; multibyte exact byte-cap, one-byte-over, empty string, surrogate and output limit cases retain precise acceptance/rejection.
- bounded mutable list behavior is tested with controlled source changes during nested conversion; no arbitrary subclass callbacks are invoked, no unbounded growth iterator is used.
- compare encoded bytes and input commitment for existing accepted fixtures, list/tuple, UUIDs and all field boundaries; preserve existing rejection matrix and unbounded valid integer policy.
- via real GeneralWorkerProcess.start_turn with fake owned launcher, invalid input leads to zero launcher calls. This proves no launcher invocation in that path, not no-start/stop authority or the earlier service prepare state.

Independent review must distinguish actual preflight work avoided from eventual rejection. TDD per change, independent stage and final review, >=3 substantive feature-branch pushes per task, usage checks and daily caps. Run focused protocol/process tests and required final suite. No source edits while a full suite is active; if combined verification with the SQLite task is used, report exact source SHA and each task's review separately.

Independent design review PASS. For model dictionaries with correct field count, snapshot at most the fixed expected item count plus an exhaustion check using exact builtin dict iteration; verify exact-str keys before any caller-key lookup/equality or output insertion. This avoids invoking injected key callbacks and limits work if the source mutates; reject detectable key/count/iterator inconsistencies. Tests must cover forged same-count keys and finite mutations; no hostile same-process snapshot authenticity is claimed.
