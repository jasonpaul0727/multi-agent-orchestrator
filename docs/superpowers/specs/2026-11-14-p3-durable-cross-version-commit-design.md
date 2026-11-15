# P3 cross-version unique commit and durable replay: private SQLite prototype

Status: design for independent review, then development under the user's explicit instruction. This is deployment-independent tests-only storage, not production admission. Existing full v1 validation, actual Router exclusions, unknown reconciliation and Provider behavior remain unchanged.

## Purpose and choice

The reviewed finite model establishes transition rules but not real storage or process atomicity. Implement a sibling tests-only SQLite WAL store using that model as the semantic oracle. This yields actual independent-process contention and SIGKILL/reopen evidence on owned local databases without inventing production trust roots. Do not add an adapter to Scheduler, EventStore, Worker or Provider. A production schema migration or multiple-database adapter is deferred because its atomicity and writer exclusion have not been demonstrated.

Existing Scheduler and append_checked use the same connection/BEGIN IMMEDIATE for their existing acceptance operations; that fact does not make a new separate database atomic with them. The new prototype is intentionally a separate database containing only finite synthetic identities, facts, historical exclusions and eligibility.

## Files and interfaces

- `tests/support/dual_view_sqlite_codec.py`: exact whitelist canonical JSON encoding/decoding of existing model enums and frozen dataclasses; no production-compatible authorization format.
- `tests/support/dual_view_sqlite_store.py`: `create_store(path: Path) -> None`; `SqliteDualViewStore(path)` context manager for an existing store; `apply(command) -> (ModelState, Observation)`; `commit(envelope, *, hook=None) -> (ModelState, Observation)`; `read() -> ModelState`.
- `tests/unit/runtime/test_dual_view_sqlite_store.py`: codec, lifecycle, transactions, idempotency, strict replay/projection rejection.
- `tests/integration/test_dual_view_sqlite_processes.py`: real process races, controlled SIGKILL and fresh-process recovery. A separate tests/support harness is allowed if needed.

Keep `dual_view_simulation.py` unchanged and free of codec/I/O. All returned values retain simulation_only/non_authoritative labels; local WINNER is a synthetic stored decision, not a task acceptance or permission to send.

## Fixed domain and codec

Reuse exactly the model's finite two sources, two requests per source, two plan generations, source fence 2/successor fence 3, 64 events and synthetic resource fixtures. Encode only exact registered enum/dataclass types and immutable tuples plus field-appropriate None/bool/int. Fixed type tags and exact required field sets; unknown tags/fields/versions, duplicate JSON keys, floats, non-finite values, bool-as-int, excessive depth/size, invalid enum names and noncanonical representation are invalid. No pickle, eval, dynamic type import, arbitrary object constructors or production ID input. Encode/decode must defensively isolate caller-owned objects. Roundtrip does not authenticate facts.

Limit each event text to 16 KiB, all loaded event text to 1 MiB and projection text to 2 MiB; validate SQL lengths and row count before deserializing. State projection includes complete event history and all persistent model state, with pending empty. These are input/workload bounds, not an RSS/disk guarantee.

## Database lifecycle and layout

Only create an absent file inside a trusted test-owned local directory with exclusive create (0600). Reject existing destinations; no overwrites or migrations. Open-existing uses mode=rw to avoid creating a missing path. Reject final symlink; trusted parent namespace is an explicit test assumption, not hostile path/DB-WAL handle proof. Never delete database/WAL files to recover.

Own schema version/application marker plus exact expected sqlite_schema definitions. Tables: `prototype_meta` singleton version marker, `events(sequence INTEGER PRIMARY KEY, payload TEXT NOT NULL)`, singleton `projection(payload TEXT NOT NULL)`, `winners(source TEXT PRIMARY KEY,payload TEXT NOT NULL)`, `resources(source TEXT PRIMARY KEY,payload TEXT NOT NULL)`. Source keys here are the fixed synthetic source identities, each binding the model's fixed full source tuple/fence. Strict replay must cross-check every row against the complete synthetic identity.

On opening, verify marker and exact schema including indexes/triggers before any mutating PRAGMA, then require WAL. Unknown schema/version/extra table/index/trigger, absent projection, event gap/duplicate/noncanonical payload, malformed or conflicting materialized rows fail closed. No repair/rebuild-on-error. A writable SQLite read can touch its sidecars: rejection is not a guarantee of zero physical file writes. Restrict observed application writes to this own database.

Record sqlite3 runtime, compile_options, journal_mode and synchronous mode. Set synchronous=FULL and wal_autocheckpoint=0 on each prototype connection; bounded busy timeout, no application-level retry loop. If WAL is unavailable, report unsupported rather than silently fallback. Keep the connection private; callers do not supply an ambient connection/transaction.

## One transaction and unique winner

For every apply or commit, BEGIN IMMEDIATE before loading current state. Read a consistent complete event sequence; use strict model Replay with the stored events as the self-consistency anchor, then compare canonical complete persistent projection and independently derived winners/resources indexes. This anchor checks internal consistency only, not authenticity, completeness or rollback against an external authority. A coherent rewrite/truncation of all state is outside the guarantee.

`apply` accepts only RegisterSource, SetFacts, Propose, CloseGeneration and Invalidate. It rejects external PrepareCommit, Commit, Crash, Replay, arbitrary ModelState or pending. `commit(envelope)` revalidates and performs local model PrepareCommit+Commit within this same protected operation; it never trusts caller-supplied volatile state. Exact winner replay returns its existing result without creating another event/reservation/debit. Same request/different frozen envelope conflicts; a different request/version/entry for the source returns superseded only when the model's existing proposal rules permit. No new proposal/winner or resource change on unknown/held/conflict.

If model transitions append events, insert their exact next ordinals and replace projection plus derived winner/resource rows within the same SQL transaction. The source primary key gives a second unique-winner constraint. All records, reservation binding and resource decrement commit together or roll back. Unknown/expiry/source stale/revocation/generation closure are rechecked after acquiring write lock; no pre-lock decision can authorize commit. Read/history repeat preserves past winner, and current unknown/revocation still blocks any simulated dispatch check in the original model. No external dispatch exists here.

Any decoding, validation, hook or SQLite error rolls back the owned transaction; re-raise control-flow errors after attempting cleanup, preserve primary error if rollback/close also fails. No catch-and-succeed, partial projection update, automatic replay from a valid prefix, automatic overwrite or retry. read() uses one read transaction for the same full validation. No transaction remains open after any normal result.

## Crash hooks and local evidence

Optional test-only hook receives fixed phase labels: after_begin, after_events, after_projection, after_indexes, before_commit, after_commit. It has no production meaning. A raised precommit hook error rolls back. An after_commit error means the response is lost while data may already be committed; caller must reopen and query/retry exact request, never assume rollback.

Tests use private /tmp local SQLite, spawn actual independent Python processes and communicate bounded ready/phase/release messages. Parent kills only its own verified child after receiving an exact phase acknowledgement; no sleep-based cut point. Always join/reap owned children and preserve primary failures while attempting bounded cleanup. Do not remove evidence/database files as a recovery mechanism.

Race cases: v1 fallback vs B same-model with source historically excluded, both winner orders; v1 vs B same-model when not excluded, exact duplicate, different envelopes, two independent sources, invalidation/generation update winning the lock first. At most one winner/reservation and one synthetic debit per source after fresh connection and process reopen.

SIGKILL cuts: after_begin, after_events, after_projection, after_indexes, before_commit, after_commit. Every precommit cut yields the old complete state and no winner/debit; after_commit yields all committed state and exact retry idempotency. Close all worker connections before final fresh-process replay. Also verify unknown persisted before/after winner, stale/expired eligibility, cross-source wrong binding, invalid requests, source/successor fence and v1 exclusion. Negative SQL-tamper fixtures cover missing/gapped/invalid events, changed projection/resources/winner, extra schema objects, cross-source rows and malformed codec; refusal must not repair or change logical rows.

SQLite test assumptions include this host's filesystem/VFS/process semantics; SIGKILL is not power-loss/hardware durability. Runtime currently reports SQLite 3.45.1. The official WAL documentation describes a WAL-reset race affecting older releases (https://www.sqlite.org/wal.html#the_wal_reset_bug). Do not install/upgrade shared dependencies. In controlled races keep an open parent keeper connection, disable auto-checkpoints on every connection and run no explicit checkpoint while workers exist; only close keeper after all workers have exited. This restricts the workload; it does not prove the runtime bug absent or generally qualify concurrent checkpoint safety.

## Acceptance and rollout boundary

TDD per meaningful stage; independent stage and whole-slice reviews; at least three substantive ordinary feature-branch pushes, quota check before dispatch and after each push, current daily completion limit. Stage A strict codec/create/open/read; B transactional unique commit/projections/negative cases; C independent process race/SIGKILL/reopen. Run relevant focused tests and a final full suite; preserve any red reports without inferring flaky causes. Do not change source during an active full suite.

Keep full v1 verification; no schema epoch bypass, production event acceptance, old-writer/DB-WAL fencing, effect/no-charge certificate, credentials, paid call, deployment, main merge, production trust root or external exactly-once claim. Completed local SQLite evidence advances a storage prerequisite; P3 remains incomplete until its actual remaining gates are met.

## Independent review clarification

Design review PASS. Rollback-on-error applies only before COMMIT has succeeded; after_commit errors preserve the committed database and represent lost local response. Regardless of any failed model Prepare/Commit intermediate result (including the 64-event cap), all persisted projections and public returned ModelState values must have pending=(). The database never stores a volatile pending or treats it as independent permission. Full source keys are the fixed schema-domain mapping of S0/S1 to their synthetic run/node/attempt/fence/decision identities; version, entry and plan generation never extend the winner primary key.
