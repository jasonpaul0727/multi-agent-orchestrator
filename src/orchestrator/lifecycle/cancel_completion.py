"""Fixed, transaction-owned materialization budgets for cancellation proofs.

The budget limits returned rows, raw bytes and JSON decoding; it does not
claim an upper bound on SQLite page I/O or query CPU.
"""

from dataclasses import dataclass, field

from orchestrator.persistence import EventContractError, EventIntegrityError
from orchestrator.artifacts.store import ArtifactError
from orchestrator.budget.ledger import BudgetError
from orchestrator.runtime.verification_journal import VerifierProposalJournalError


class CancellationHeld(ValueError):
    def __init__(self, reason):
        if reason not in {"budget", "integrity", "unsupported"}:
            raise ValueError("invalid cancellation hold reason")
        self.reason = reason
        super().__init__("cancellation_held_" + reason)


@dataclass(slots=True)
class CancellationBudget:
    max_rows: int
    max_bytes: int
    max_row_bytes: int
    max_streams: int
    max_nodes: int
    rows: int = 0
    bytes: int = 0
    streams: int = 0
    _store: object = field(default=None, init=False, repr=False)
    _token: object = field(default=None, init=False, repr=False)
    _windows: dict = field(default_factory=dict, init=False, repr=False)
    _cache: dict = field(default_factory=dict, init=False, repr=False)
    _state: object = field(default=None, init=False, repr=False)

    def __post_init__(self):
        for value in (self.max_rows, self.max_bytes, self.max_row_bytes, self.max_streams, self.max_nodes):
            if type(value) is not int or value < 1:
                raise ValueError("invalid cancellation proof budget")

    def bind(self, store):
        if not store._has_owned_write():
            raise ValueError("owned write required")
        token = store._owned_write_token()
        if self._store is None:
            self._store, self._token = store, token
        elif self._store is not store or self._token is not token:
            raise ValueError("cancellation proof transaction changed")


_COLUMNS = ("event_id", "stream_type", "stream_id", "event_type", "occurred_at",
            "payload_json", "payload_hash", "idempotency_key", "run_id", "node_id",
            "attempt_id", "correlation_id", "causation_id", "stream_version",
            "schema_version", "fencing_generation")


def _event_storage_types(row):
    integer_fields = {"stream_version", "schema_version", "fencing_generation"}
    optional_fields = {"run_id", "node_id", "attempt_id", "fencing_generation", "correlation_id", "causation_id"}
    for index, name in enumerate(_COLUMNS):
        kind = row["t" + str(index)]
        expected = "integer" if name in integer_fields else "text"
        if kind != expected and not (name in optional_fields and kind == "null"):
            raise CancellationHeld("integrity")

def read_version(store, stream_type, stream_id, budget):
    """Read an integer version without materializing malformed large values."""
    budget.bind(store)
    if budget.rows + 1 > budget.max_rows or budget.bytes + 32 > budget.max_bytes:
        raise CancellationHeld("budget")
    row = store._connection.execute("""
        SELECT typeof(current_version) AS kind, length(CAST(current_version AS BLOB)) AS n,
               CASE WHEN typeof(current_version)='integer' THEN current_version ELSE NULL END AS value
          FROM stream_versions WHERE stream_type=? AND stream_id=?
    """, (stream_type, stream_id)).fetchone()
    budget.rows += 1
    budget.bytes += 32
    if row is None:
        return None
    if type(row["n"]) is not int or row["n"] > budget.max_row_bytes:
        raise CancellationHeld("budget")
    if row["kind"] != "integer" or type(row["value"]) is not int or row["value"] < 1:
        raise CancellationHeld("integrity")
    return row["value"]

def read_event_window(store, stream_type, stream_id, budget, *, max_events=None,
                      max_payload_bytes=None, max_payload_total=None):
    """Materialize a full physical stream only after bounded metadata preflight."""
    budget.bind(store)
    cache_key = (stream_type, stream_id)
    if (cache_key in budget._cache and max_events is None
            and max_payload_bytes is None and max_payload_total is None):
        cached, changes = budget._cache[cache_key]
        if changes == store._connection.total_changes:
            return cached
        budget._cache.clear()
        budget._windows.clear()
    if budget.streams >= budget.max_streams:
        raise CancellationHeld("budget")
    budget.streams += 1
    remaining = budget.max_rows - budget.rows
    if remaining < 1:
        raise CancellationHeld("budget")
    lengths = ", ".join(
        "COALESCE(length(CAST(" + name + " AS BLOB)), 0) AS n" + str(index)
        for index, name in enumerate(_COLUMNS)
    )
    types = ", ".join("typeof(" + name + ") AS t" + str(i) for i, name in enumerate(_COLUMNS))
    metadata = store._connection.execute(
        "SELECT rowid AS r, " + lengths + ", " + types + " FROM events "
        "WHERE stream_type=? AND stream_id=? ORDER BY stream_version LIMIT ?",
        (stream_type, stream_id, remaining + 1),
    ).fetchall()
    count = len(metadata)
    if max_events is not None and count > max_events:
        raise CancellationHeld("budget")
    payload_sizes = [row["n5"] for row in metadata]
    if ((max_payload_bytes is not None and any(n > max_payload_bytes for n in payload_sizes))
            or (max_payload_total is not None and sum(payload_sizes) > max_payload_total)):
        raise CancellationHeld("budget")
    # Metadata and payload queries each return one row per event.
    if count > remaining or count * 2 + 1 > remaining:
        raise CancellationHeld("budget")
    byte_count = 0
    for row in metadata:
        lengths_for_row = [row["n" + str(i)] for i in range(len(_COLUMNS))]
        if any(type(n) is not int or n < 0 or n > budget.max_row_bytes for n in lengths_for_row):
            raise CancellationHeld("budget")
        row_bytes = sum(lengths_for_row)
        if row_bytes > budget.max_row_bytes:
            raise CancellationHeld("budget")
        _event_storage_types(row)
        byte_count += row_bytes + 16 * len(_COLUMNS) + 16
    if budget.bytes + byte_count + 32 > budget.max_bytes:
        raise CancellationHeld("budget")
    version_value = read_version(store, stream_type, stream_id, budget)
    version = 0 if version_value is None else version_value
    if version != count:
        raise CancellationHeld("integrity")
    budget.bind(store)
    budget.rows += count * 2
    budget.bytes += byte_count
    rows = store._connection.execute(
        """SELECT rowid AS r, event_id, stream_type, stream_id, stream_version, event_type, schema_version,
                  occurred_at, payload_json, payload_hash, idempotency_key, run_id, node_id,
                  attempt_id, fencing_generation, correlation_id, causation_id
             FROM events WHERE stream_type=? AND stream_id=? ORDER BY stream_version LIMIT ?""",
        (stream_type, stream_id, count + 1),
    ).fetchall()
    if stream_type in {"run_lifecycle", "budget", "artifact", "verification_proposals", "general_worker", "mediation_v2"} and any(row["schema_version"] != 1 for row in rows):
        raise CancellationHeld("integrity")
    if ([row["r"] for row in rows] != [row["r"] for row in metadata]
            or any(type(row["stream_version"]) is not int or row["stream_version"] != index
                   for index, row in enumerate(rows, 1))):
        raise CancellationHeld("integrity")
    budget._windows[(stream_type, stream_id)] = (count, byte_count)
    try:
        result = ([store._row_to_event(row) for row in rows], version)
    except (ValueError, TypeError, KeyError, EventIntegrityError, EventContractError,
            VerifierProposalJournalError, ArtifactError, BudgetError):
        raise CancellationHeld("integrity") from None
    budget._cache[cache_key] = (result, store._connection.total_changes)
    return result


def read_run_catalog(store, run_id, budget, *, node_id=None, stream_types=None):
    """Collect bounded v1/v2 scope and physical metadata without JSON decoding."""
    budget.bind(store)
    scopes, physical = [], []
    scope_columns = ("stream_id", "run_id", "node_id", "attempt_id", "fencing_generation")

    def window(table, index_name, where, args, length_columns, selected):
        budget.bind(store)
        remaining = budget.max_rows - budget.rows
        if remaining < 1:
            raise CancellationHeld("budget")
        lengths = ", ".join(
            "COALESCE(length(CAST(" + name + " AS BLOB)), 0) AS n" + str(i)
            for i, name in enumerate(length_columns)
        )
        prefix = " FROM " + table + " INDEXED BY " + index_name + " WHERE " + where
        types = ", ".join("typeof(" + name + ") AS t" + str(i) for i, name in enumerate(length_columns))
        metadata = store._connection.execute(
            "SELECT rowid AS r, " + lengths + ", " + types + prefix + " LIMIT ?", (*args, remaining + 1)
        ).fetchall()
        count = len(metadata)
        if count * 2 > remaining:
            raise CancellationHeld("budget")
        size = 0
        for row in metadata:
            lengths_for_row = [row["n" + str(i)] for i in range(len(length_columns))]
            if any(type(n) is not int or n < 0 or n > budget.max_row_bytes for n in lengths_for_row):
                raise CancellationHeld("budget")
            row_bytes = sum(lengths_for_row)
            if row_bytes > budget.max_row_bytes:
                raise CancellationHeld("budget")
            if table == "events":
                _event_storage_types(row)
            else:
                for index, name in enumerate(length_columns):
                    expected = "integer" if name == "fencing_generation" else "text"
                    if row["t" + str(index)] != expected:
                        raise CancellationHeld("integrity")
            size += row_bytes + 16 * len(length_columns) + 8
        if budget.bytes + size > budget.max_bytes:
            raise CancellationHeld("budget")
        budget.bind(store)
        budget.rows += count * 2
        budget.bytes += size
        rows = store._connection.execute(
            "SELECT rowid AS r, " + ", ".join(selected) + prefix + " LIMIT ?", (*args, count + 1)
        ).fetchall()
        if [row["r"] for row in rows] != [row["r"] for row in metadata]:
            raise CancellationHeld("integrity")
        return rows

    for stream_type, table, index_name in (
        ("general_worker", "general_worker_scope", "ix_cancel_v1_scope_run"),
        ("mediation_v2", "mediation_scope", "ix_cancel_v2_scope_run"),
    ):
        if stream_types is not None and stream_type not in stream_types:
            continue
        if node_id is None:
            where, args = "run_id=?", (run_id,)
        else:
            where, args = "run_id=? AND node_id=?", (run_id, node_id)
            index_name = "ix_general_worker_scope_node" if stream_type == "general_worker" else "ix_mediation_scope_node"
        rows = window(table, index_name, where, args, scope_columns, scope_columns)
        for row in rows:
            if (any(type(row[name]) is not str or not row[name] for name in scope_columns[:-1])
                    or type(row["fencing_generation"]) is not int or row["fencing_generation"] < 1):
                raise CancellationHeld("integrity")
            scopes.append((stream_type, row["stream_id"], row["node_id"],
                           row["attempt_id"], row["fencing_generation"]))
        event_index = "ix_cancel_events_run" if node_id is None else "ix_mediation_event_node"
        event_where = "stream_type=? AND run_id=?" if node_id is None else "stream_type=? AND run_id=? AND node_id=?"
        event_args = (stream_type, run_id) if node_id is None else (stream_type, run_id, node_id)
        rows = window("events", event_index, event_where,
                      event_args, _COLUMNS,
                      ("stream_id", "node_id", "attempt_id", "fencing_generation", "stream_version"))
        for row in rows:
            if (any(type(row[name]) is not str or not row[name] for name in ("stream_id", "node_id", "attempt_id"))
                    or type(row["fencing_generation"]) is not int or row["fencing_generation"] < 1
                    or type(row["stream_version"]) is not int or row["stream_version"] < 1):
                raise CancellationHeld("integrity")
            physical.append((stream_type, row["stream_id"], row["node_id"], row["attempt_id"],
                             row["fencing_generation"], row["stream_version"]))
    return scopes, physical


def read_scheduler_release(store, run_id, node_id, attempt_id, generation, budget):
    """Read at most one exact attempt release after per-field length preflight."""
    budget.bind(store)
    remaining = budget.max_rows - budget.rows
    if remaining < 2 or budget.streams >= budget.max_streams:
        raise CancellationHeld("budget")
    budget.streams += 1
    lengths = ", ".join(
        "COALESCE(length(CAST(" + name + " AS BLOB)), 0) AS n" + str(i)
        for i, name in enumerate(_COLUMNS)
    )
    where = """ FROM events INDEXED BY ix_scheduler_release_context
        WHERE stream_type='scheduler' AND stream_id='global' AND event_type='AttemptSlotReleased'
          AND run_id=? AND node_id=? AND attempt_id=? AND fencing_generation=?"""
    args = (run_id, node_id, attempt_id, generation)
    types = ", ".join("typeof(" + name + ") AS t" + str(i) for i, name in enumerate(_COLUMNS))
    metadata = store._connection.execute(
        "SELECT rowid AS r, " + lengths + ", " + types + where + " LIMIT 2", args).fetchall()
    if len(metadata) > 1:
        raise CancellationHeld("integrity")
    if len(metadata) * 2 > remaining:
        raise CancellationHeld("budget")
    size = 0
    for row in metadata:
        field_sizes = [row["n" + str(i)] for i in range(len(_COLUMNS))]
        if any(type(n) is not int or n < 0 or n > budget.max_row_bytes for n in field_sizes):
            raise CancellationHeld("budget")
        size += sum(field_sizes) + 16 * len(_COLUMNS) + 8
        if sum(field_sizes) > budget.max_row_bytes:
            raise CancellationHeld("budget")
        _event_storage_types(row)
    if budget.bytes + size > budget.max_bytes:
        raise CancellationHeld("budget")
    budget.bind(store)
    budget.rows += len(metadata) * 2
    budget.bytes += size
    rows = store._connection.execute(
        """SELECT rowid AS r, event_id, stream_type, stream_id, stream_version, event_type,
                  schema_version, occurred_at, payload_json, payload_hash, idempotency_key,
                  run_id, node_id, attempt_id, fencing_generation, correlation_id, causation_id"""
        + where + " LIMIT 2", args).fetchall()
    if ([row["r"] for row in rows] != [row["r"] for row in metadata]
            or any(row["schema_version"] != 1 for row in rows)):
        raise CancellationHeld("integrity")
    try:
        return [store._row_to_event(row) for row in rows]
    except (ValueError, TypeError, KeyError, EventIntegrityError, EventContractError,
            VerifierProposalJournalError, ArtifactError, BudgetError):
        raise CancellationHeld("integrity") from None


@dataclass(frozen=True, slots=True)
class CancellationResult:
    outcome: str
    reason: str | None = None
    state: object | None = None

    @property
    def status(self):
        return self.state.status if self.state is not None else "held"


def prove_basic_cancellation(store, run_id, budget):
    """Prove empty or bounded v1 history; unsupported histories are held."""
    from .controller import LifecycleError, reduce_lifecycle
    from orchestrator.persistence.mediation_identity import mediation_stream_id
    from orchestrator.runtime.general_worker_journal import (
        GeneralWorkerIdentity, _replay as replay_v1,
    )

    events, version = read_event_window(store, "run_lifecycle", run_id, budget)
    if not events:
        raise CancellationHeld("integrity")
    try:
        state = reduce_lifecycle(run_id, events)
    except (LifecycleError, ValueError, TypeError, KeyError):
        raise CancellationHeld("integrity") from None
    if state.event_version != version:
        raise CancellationHeld("integrity")
    budget._state = state
    if state.status != "cancelling":
        return CancellationResult("not_needed", state=state), events, version
    if any(node.status in {"running", "awaiting_reconciliation"} for node in state.nodes):
        return CancellationResult("held", "active", state), events, version
    attempts_count = sum(len(node.attempts) for node in state.nodes)
    if len(state.nodes) + attempts_count > budget.max_nodes:
        raise CancellationHeld("budget")

    scopes, physical = read_run_catalog(store, run_id, budget)
    if len(scopes) > budget.max_streams:
        raise CancellationHeld("budget")
    expected = {}
    attempts = {}
    for node in state.nodes:
        for attempt in node.attempts:
            identity = GeneralWorkerIdentity(run_id, node.spec.node_id, attempt.attempt_id,
                                             attempt.fencing_generation)
            v2_id = mediation_stream_id(run_id, node.spec.node_id, attempt.attempt_id,
                                         attempt.fencing_generation)
            expected[("general_worker", identity.stream_id)] = (
                node.spec.node_id, attempt.attempt_id, attempt.fencing_generation)
            expected[("mediation_v2", v2_id)] = (
                node.spec.node_id, attempt.attempt_id, attempt.fencing_generation)
            attempts[("general_worker", identity.stream_id)] = (identity, attempt)
    if len(expected) > budget.max_streams:
        raise CancellationHeld("budget")
    if len(set(scopes)) != len(scopes):
        raise CancellationHeld("integrity")
    scope_map = {}
    for kind, stream_id, node_id, attempt_id, generation in scopes:
        key = (kind, stream_id)
        if expected.get(key) != (node_id, attempt_id, generation) or key in scope_map:
            raise CancellationHeld("integrity")
        scope_map[key] = (node_id, attempt_id, generation)
    physical_map = {}
    for kind, stream_id, node_id, attempt_id, generation, stream_version in physical:
        key = (kind, stream_id)
        if scope_map.get(key) != (node_id, attempt_id, generation):
            raise CancellationHeld("integrity")
        physical_map.setdefault(key, []).append(stream_version)
    if set(scope_map) != set(physical_map):
        raise CancellationHeld("integrity")

    for kind, stream_id in expected:
        budget.bind(store)
        # Two indexed singleton probes per admitted stream; charge even absent rows.
        version_value = read_version(store, kind, stream_id, budget)
        if budget.rows + 1 > budget.max_rows or budget.bytes + 8 > budget.max_bytes:
            raise CancellationHeld("budget")
        budget.rows += 1
        budget.bytes += 8
        exists = store._connection.execute(
            "SELECT 1 FROM events WHERE stream_type=? AND stream_id=? LIMIT 1",
            (kind, stream_id)).fetchone()
        key = (kind, stream_id)
        if (version_value is not None or exists is not None) != (key in scope_map):
            raise CancellationHeld("integrity")
        if key in scope_map:
            versions = sorted(physical_map[key])
            if (type(version_value) is not int or versions != list(range(1, len(versions) + 1))
                    or version_value != len(versions)):
                raise CancellationHeld("integrity")
    # The same bounded budget stream must prove each admitted reservation.
    from orchestrator.budget import BudgetLedger
    from orchestrator.runtime.general_worker_journal import metadata_hash
    budget_events, _ = read_event_window(store, "budget", run_id, budget)
    try:
        reservation_states = BudgetLedger._replay_states(run_id, budget_events)
    except (ValueError, TypeError, KeyError, BudgetError):
        raise CancellationHeld("integrity") from None
    # Lifecycle-only terminal records do not prove the Scheduler released its
    # slot. Every admitted attempt needs its own exact, bounded release record.
    for node in state.nodes:
        for attempt in node.attempts:
            releases = read_scheduler_release(store, run_id, node.spec.node_id, attempt.attempt_id,
                                              attempt.fencing_generation, budget)
            if len(releases) != 1:
                raise CancellationHeld("unsupported")
            release = releases[0]
            identity = dict(run_id=run_id, node_id=node.spec.node_id, attempt_id=attempt.attempt_id,
                            fencing_generation=attempt.fencing_generation)
            if (any(getattr(release, key) != value for key, value in identity.items())
                    or any(release.payload.get(key) != value for key, value in identity.items())
                    or release.correlation_id != run_id or release.payload.get("outcome") != attempt.status):
                raise CancellationHeld("integrity")
            reservation_state = reservation_states.get(attempt.reservation_id)
            if reservation_state is None:
                raise CancellationHeld("integrity")
            reservation = reservation_state.reservation
            if (reservation.reservation_id != attempt.reservation_id or
                    any(getattr(reservation, key) != value for key, value in identity.items()) or
                    reservation.status not in {"released", "committed"}):
                raise CancellationHeld("integrity")
            related = [event for event in budget_events if event.payload.get("reservation_id") == attempt.reservation_id]
            if any(any(getattr(event, key) != value for key, value in identity.items())
                   or event.payload.get("run_id") != run_id for event in related):
                raise CancellationHeld("integrity")
            reservations = [e for e in related if e.event_type == "BudgetReserved"]
            budget_releases = [e for e in related if e.event_type == "BudgetReleased"]
            if len(reservations) != 1 or len(budget_releases) != 1:
                raise CancellationHeld("integrity")
            budget_release = budget_releases[0].payload
            if (budget_release.get("currency") != reservation.currency
                    or budget_release.get("status") != reservation.status):
                raise CancellationHeld("integrity")
            if reservation.status == "committed":
                if (sum(e.event_type == "UsageObserved" for e in related) != 1
                        or sum(e.event_type == "CostCommitted" for e in related) != 1):
                    raise CancellationHeld("integrity")
                expected_minor = max(0, reservation.reserved_minor - reservation_state.cost_minor)
                expected_tokens = max(0, reservation.reserved_tokens - reservation_state.used_tokens)
            else:
                expected_minor, expected_tokens = reservation.reserved_minor, reservation.reserved_tokens
            if (type(budget_release.get("released_minor")) is not int
                    or type(budget_release.get("released_tokens")) is not int
                    or budget_release["released_minor"] != expected_minor
                    or budget_release["released_tokens"] != expected_tokens):
                raise CancellationHeld("integrity")
            usage_hash = release.payload.get("usage_hash")
            import re
            no_effect = (release.payload.get("known_no_effect") is True or
                type(release.payload.get("no_effect_receipt_hash")) is str and
                re.fullmatch(r"sha256:[0-9a-f]{64}", release.payload["no_effect_receipt_hash"]) is not None)
            if reservation.status == "committed":
                usage = reservation_state.usage
                committed = next(e for e in related if e.event_type == "CostCommitted").payload
                observed = next(e for e in related if e.event_type == "UsageObserved").payload
                if (usage is None or reservation_state.cost_minor != usage.cost_minor
                        or reservation_state.used_tokens != usage.total_tokens
                        or metadata_hash(dict(committed)) != metadata_hash(BudgetLedger._committed_payload(usage))
                        or metadata_hash(dict(observed)) != metadata_hash(BudgetLedger._usage_payload(usage))):
                    raise CancellationHeld("integrity")
                zero_no_effect = (usage_hash is None and no_effect and usage is not None
                    and usage.cost_minor == 0 and usage.total_tokens == 0
                    and usage.provider_fee_minor == 0 and usage.tool_fee_minor == 0)
                if not zero_no_effect and (usage is None or metadata_hash(usage.model_dump(mode="json")) != usage_hash):
                    raise CancellationHeld("integrity")
            elif usage_hash is not None or not no_effect:
                raise CancellationHeld("integrity")
    for (kind, stream_id), metadata in scope_map.items():
        if kind == "mediation_v2":
            raise CancellationHeld("unsupported")
        identity, attempt = attempts[(kind, stream_id)]
        worker_events, worker_version = read_event_window(store, kind, stream_id, budget)
        try:
            op = replay_v1(identity, worker_events, worker_version)
        except (ValueError, TypeError, KeyError, EventIntegrityError, EventContractError,
                VerifierProposalJournalError, ArtifactError, BudgetError):
            raise CancellationHeld("integrity") from None
        if op is None:
            raise CancellationHeld("integrity")
        validate_v1_links(store, op, budget)
        release = validate_v1_terminal(store, op, budget)
        payload = release.payload
        if payload.get("outcome") != attempt.status:
            raise CancellationHeld("integrity")
        if op.call is None:
            import re
            no_effect_hash = payload.get("no_effect_receipt_hash")
            if not (
                    payload.get("outcome") == "failed" and payload.get("known_no_effect") is True
                    and payload.get("usage_hash") is None
                    or payload.get("outcome") == "cancelled" and type(no_effect_hash) is str
                    and re.fullmatch(r"sha256:[0-9a-f]{64}", no_effect_hash)
                    and payload.get("usage_hash") is None):
                raise CancellationHeld("unsupported")
    return CancellationResult("ready", state=state), events, version


def validate_v1_links(store, operation, budget, *, allow_uncaptured_call=False):
    """Validate existing v1 capture/proposal links using only bounded cached windows."""
    from orchestrator.models.provider_calls import SQLiteProviderCallJournal
    from orchestrator.runtime.contracts import ArtifactRef
    from orchestrator.runtime.general_worker_journal import _identity_value, metadata_hash
    from orchestrator.runtime.verification_journal import (
        _decode_stream, MAX_PROPOSALS_PER_RUN, MAX_RUN_PAYLOAD_BYTES, VERIFICATION_PROPOSAL_STREAM_TYPE,
    )

    if operation.capture is not None:
        if operation.call is None:
            raise CancellationHeld("integrity")
        events, _ = read_event_window(store, "provider_call", operation.call.provider_call_stream_id, budget)
        try:
            call = SQLiteProviderCallJournal(store)._project_call(events)
            if (call is None or any(getattr(call, name) != value
                                    for name, value in _identity_value(operation.identity).items())
                    or call.request_id != operation.request_id
                    or call.registry_manifest_hash != operation.context.registry_hash
                    or call.request_hash != operation.call.request_body_sha256
                    or any(getattr(call, name) != getattr(operation.call, name) for name in (
                        "accepted_route_id", "budget_reservation_id", "provider_id", "model_id",
                        "idempotency_key_hash"))
                    or call.status != "known_success" or call.usage is None
                    or call.usage.get("status") not in {"reported", "estimated"}
                    or call.termination_receipt is None):
                raise ValueError()
            capture = operation.capture
            if (capture.request_body_sha256 != call.request_hash
                    or capture.usage_sha256 != metadata_hash(dict(call.usage))
                    or capture.sender_receipt_sha256 != call.termination_receipt.receipt_hash):
                raise ValueError()
        except (ValueError, TypeError, KeyError, EventIntegrityError, EventContractError,
                VerifierProposalJournalError, ArtifactError, BudgetError):
            raise CancellationHeld("integrity") from None
    elif operation.call is not None and not allow_uncaptured_call:
        # No captured success: outcome/settlement proof needs a distinct protocol.
        raise CancellationHeld("unsupported")

    if operation.task_sha256 is not None:
        if operation.capture is None:
            raise CancellationHeld("integrity")
        events, _ = read_event_window(store, VERIFICATION_PROPOSAL_STREAM_TYPE,
                                      operation.identity.run_id, budget)
        try:
            records, sizes = _decode_stream(operation.identity.run_id, events)
            if len(records) > MAX_PROPOSALS_PER_RUN or sum(sizes) > MAX_RUN_PAYLOAD_BYTES:
                raise ValueError()
            matches = [item for item in records if item.task_sha256 == operation.task_sha256]
            expected = ArtifactRef(digest=operation.capture.output_digest,
                size_bytes=operation.capture.output_byte_count, artifact_type="text",
                media_type="text/plain; charset=utf-8")
            if (len(matches) != 1 or matches[0].evidence_sha256 != operation.evidence_sha256
                    or matches[0].task.context != operation.context
                    or matches[0].task.candidate_artifacts != (expected,)):
                raise ValueError()
        except (ValueError, TypeError, KeyError, EventIntegrityError, EventContractError,
                VerifierProposalJournalError, ArtifactError, BudgetError):
            raise CancellationHeld("integrity") from None


def validate_v1_terminal(store, operation, budget):
    """Validate v1 terminal, usage and publication links from bounded windows."""
    from orchestrator.artifacts import ArtifactStore
    from orchestrator.budget import BudgetLedger
    from orchestrator.runtime.general_worker_journal import (
        _identity_value, _require_worker_stopped, metadata_hash,
    )

    identity = operation.identity
    releases = read_scheduler_release(store, identity.run_id, identity.node_id, identity.attempt_id,
                                      identity.fencing_generation, budget)
    if not releases:
        raise CancellationHeld("unsupported")
    release = releases[0]
    payload = release.payload
    identity_fields = _identity_value(identity)
    if (any(getattr(release, name) != value for name, value in identity_fields.items())
            or any(payload.get(name) != value for name, value in identity_fields.items())
            or release.correlation_id != identity.run_id):
        raise CancellationHeld("integrity")
    try:
        _require_worker_stopped(operation)
    except ValueError:
        raise CancellationHeld("unsupported") from None

    if operation.phase != "accepted":
        if payload.get("outcome") == "succeeded":
            raise CancellationHeld("integrity")
        if operation.call is not None:
            budget_events, _ = read_event_window(store, "budget", identity.run_id, budget)
            try:
                states = BudgetLedger._replay_states(identity.run_id, budget_events)
                reservation = states[operation.call.budget_reservation_id]
                if (reservation.reservation.status != "committed" or reservation.usage is None
                        or metadata_hash(reservation.usage.model_dump(mode="json")) != payload.get("usage_hash")):
                    raise ValueError()
            except (ValueError, TypeError, KeyError, EventIntegrityError, EventContractError,
                    VerifierProposalJournalError, ArtifactError, BudgetError):
                raise CancellationHeld("integrity") from None
        return release

    accepted = operation.acceptance
    if (accepted is None or operation.call is None or operation.capture is None
            or payload.get("outcome") != "succeeded"
            or payload.get("completed_at") != accepted.completed_at
            or payload.get("usage_hash") != accepted.usage_sha256
            or payload.get("task_sha256") != accepted.task_sha256
            or payload.get("evidence_sha256") != accepted.evidence_sha256):
        raise CancellationHeld("integrity")
    budget_events, _ = read_event_window(store, "budget", identity.run_id, budget)
    try:
        states = BudgetLedger._replay_states(identity.run_id, budget_events)
        reservation = states[operation.call.budget_reservation_id]
        if (reservation.reservation.status != "committed" or reservation.usage is None
                or reservation.usage.settlement_key != accepted.settlement_key
                or metadata_hash(reservation.usage.model_dump(mode="json")) != accepted.usage_sha256):
            raise ValueError()
    except (ValueError, TypeError, KeyError, EventIntegrityError, EventContractError,
            VerifierProposalJournalError, ArtifactError, BudgetError):
        raise CancellationHeld("integrity") from None

    source = {**{key: str(value) for key, value in identity_fields.items()},
              "agent_instance_id": operation.context.agent_instance_id,
              "general_worker_operation_id": operation.operation_id}
    for ref, kind, publication_id in (
        (operation.capture.response_artifact, "response", accepted.response_publication_id),
        (accepted.artifacts[0], "output", accepted.output_publication_id),
    ):
        events, _ = read_event_window(store, "artifact", ref.digest, budget)
        scope = (operation.operation_id,) if kind == "response" else (identity.run_id,)
        found = False
        try:
            for event in events:
                if event.event_type != "ArtifactPublished":
                    continue
                record = ArtifactStore._record_from_event(event, ref.digest)
                if (record.source == {**source, "general_worker_kind": kind}
                        and record.readable_scope == scope and record.size == ref.size_bytes
                        and record.artifact_type == ref.artifact_type and record.media_type == ref.media_type
                        and record.publication_id == publication_id):
                    found = True
                    break
        except (ValueError, TypeError, KeyError, EventIntegrityError, EventContractError,
                VerifierProposalJournalError, ArtifactError, BudgetError):
            raise CancellationHeld("integrity") from None
        if not found:
            raise CancellationHeld("integrity")
    return release
