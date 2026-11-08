"""SQLite implementation of the append-only event store."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
from pathlib import Path
import sqlite3
import time
import threading
from typing import Any, Callable
from urllib.parse import quote
from weakref import WeakKeyDictionary, ref

from orchestrator.identifiers import new_id

from .events import (
    EventContractError,
    EventDraft,
    StoredEvent,
    _validate_sha256_hex,
    validate_event_contract,
)


class StaleStream(RuntimeError):
    """Raised when a compare-and-swap stream version is no longer current."""

    def __init__(self, expected_version: int, current_version: int) -> None:
        self.expected_version = expected_version
        self.current_version = current_version
        super().__init__(
            f"expected stream version {expected_version}, current version is {current_version}"
        )


class IdempotencyConflict(ValueError):
    """Raised when an idempotency key is reused for a different append."""


class EventIntegrityError(RuntimeError):
    """Raised when a persisted event fails payload or boundary validation."""


def canonical_json(value: Any) -> str:
    """Encode JSON with stable key ordering and no insignificant whitespace."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("value must be JSON serializable") from exc


def _sha256_json(value: Any) -> str:
    encoded = canonical_json(value)
    try:
        encoded_bytes = encoded.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("value must not contain lone surrogate characters") from exc
    return hashlib.sha256(encoded_bytes).hexdigest()


def _validate_identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError(f"{field_name} must not contain lone surrogate characters")
    return value


def _validate_version(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


class _OwnedConnection(sqlite3.Connection):
    """Keep the store's transaction observer installed for its lifetime."""

    def set_trace_callback(self, callback):
        if getattr(self, "_ownership_trace_installed", False):
            raise RuntimeError("the store transaction observer cannot be replaced")
        super().set_trace_callback(callback)
        self._ownership_trace_installed = True


def _open_connection(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        str(path),
        timeout=5.0,
        isolation_level=None,
        check_same_thread=True,
        factory=_OwnedConnection,
    )
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        _enable_wal_with_retry(connection)
        return connection
    except Exception:
        connection.close()
        raise


def _enable_wal_with_retry(connection: sqlite3.Connection) -> None:
    """Enable WAL despite simultaneous first-open journal-mode transitions.

    SQLite's busy timeout does not consistently wait for locks taken while
    changing ``journal_mode``. Opening several store instances together can
    therefore produce ``database is locked`` before schema initialization.
    Retry only that transient condition, bounded by the normal connection
    timeout; all other SQLite errors remain visible to the caller.
    """

    deadline = time.monotonic() + 5.0
    delay = 0.01
    while True:
        try:
            mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            if str(mode).lower() != "wal":
                mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise sqlite3.OperationalError("SQLite refused WAL journal mode")
            return
        except sqlite3.OperationalError as exc:
            if not any(marker in str(exc).lower() for marker in ("locked", "busy")):
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, 0.1)


def initialize_schema(connection: sqlite3.Connection) -> None:
    # ``executescript`` implicitly commits around DDL when autocommit is on,
    # leaving partial schemas behind after a migration failure.  Keep every
    # schema change (including the snapshot table added later) in one explicit
    # transaction.
    statements = (
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS stream_versions (
            stream_type TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            current_version INTEGER NOT NULL CHECK (current_version >= 0),
            PRIMARY KEY (stream_type, stream_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT NOT NULL UNIQUE,
            stream_type TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            stream_version INTEGER NOT NULL CHECK (stream_version > 0),
            event_type TEXT NOT NULL,
            schema_version INTEGER NOT NULL CHECK (schema_version > 0),
            occurred_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            run_id TEXT,
            node_id TEXT,
            attempt_id TEXT,
            fencing_generation INTEGER,
            correlation_id TEXT,
            causation_id TEXT,
            PRIMARY KEY (event_id),
            UNIQUE (stream_type, stream_id, stream_version),
            FOREIGN KEY (stream_type, stream_id)
                REFERENCES stream_versions (stream_type, stream_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS idempotency_records (
            stream_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            stream_type TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            first_version INTEGER NOT NULL CHECK (first_version > 0),
            last_version INTEGER NOT NULL CHECK (last_version >= first_version),
            PRIMARY KEY (stream_id, idempotency_key),
            FOREIGN KEY (stream_type, stream_id)
                REFERENCES stream_versions (stream_type, stream_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            event_version INTEGER NOT NULL,
            state_json TEXT NOT NULL,
            state_hash TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            source_event_id TEXT,
            created_at TEXT NOT NULL,
            metadata_hash TEXT,
            PRIMARY KEY (aggregate_type, aggregate_id)
        )
        """,
        """
        CREATE TRIGGER IF NOT EXISTS events_immutable_update
        BEFORE UPDATE ON events
        BEGIN
            SELECT RAISE(ABORT, 'event rows are immutable');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS events_immutable_delete
        BEFORE DELETE ON events
        BEGIN
            SELECT RAISE(ABORT, 'event rows are immutable');
        END
        """,
    )
    if connection.in_transaction:
        # SQLite has no nested BEGIN.  A savepoint keeps a caller-owned
        # transaction open while making migration DDL atomic.
        savepoint = "orchestrator_schema_migration"
        connection.execute(f"SAVEPOINT {savepoint}")
        try:
            for statement in statements:
                connection.execute(statement)
            _migrate_snapshot_metadata(connection)
            _migrate_event_context(connection)
            _ensure_events_attempt_context_index(connection)
            _migrate_general_worker_scope(connection)
            connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (1)")
            connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (2)")
            connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (3)")
            connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (4)")
            connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        except BaseException:
            try:
                connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            finally:
                connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        return

    connection.execute("BEGIN IMMEDIATE")
    try:
        for statement in statements:
            connection.execute(statement)
        _migrate_snapshot_metadata(connection)
        _migrate_event_context(connection)
        _ensure_events_attempt_context_index(connection)
        _migrate_general_worker_scope(connection)
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (1)")
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (2)")
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (3)")
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (4)")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _migrate_general_worker_scope(connection: sqlite3.Connection) -> None:
    """Build one immutable scope row per Worker stream in the migration transaction."""
    migrated = connection.execute("SELECT 1 FROM schema_migrations WHERE version = 5").fetchone() is not None
    exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='general_worker_scope'").fetchone() is not None
    if migrated and not exists:
        raise ValueError("general worker scope index is missing")
    connection.execute("""CREATE TABLE IF NOT EXISTS general_worker_scope (
        stream_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_id TEXT NOT NULL,
        attempt_id TEXT NOT NULL, fencing_generation INTEGER NOT NULL CHECK (fencing_generation > 0))""")
    if not migrated:
        invalid = connection.execute("""
            SELECT 1 FROM stream_versions s
            LEFT JOIN events e ON e.stream_type=s.stream_type AND e.stream_id=s.stream_id AND e.stream_version=1
            WHERE s.stream_type='general_worker' AND
              (e.stream_id IS NULL OR typeof(e.run_id) != 'text' OR length(trim(e.run_id, char(9)||char(10)||char(13)||' '))=0
               OR typeof(e.node_id) != 'text' OR length(trim(e.node_id, char(9)||char(10)||char(13)||' '))=0
               OR typeof(e.attempt_id) != 'text' OR length(trim(e.attempt_id, char(9)||char(10)||char(13)||' '))=0
               OR typeof(e.fencing_generation) != 'integer' OR e.fencing_generation <= 0)
            LIMIT 1
        """).fetchone()
        inconsistent = connection.execute("""
            SELECT 1 FROM events e JOIN events f ON f.stream_type=e.stream_type AND f.stream_id=e.stream_id
                AND f.stream_version=1
            WHERE e.stream_type='general_worker' AND (e.run_id IS NOT f.run_id OR e.node_id IS NOT f.node_id
                OR e.attempt_id IS NOT f.attempt_id OR e.fencing_generation IS NOT f.fencing_generation)
            LIMIT 1
        """).fetchone()
        if invalid or inconsistent:
            raise ValueError("general worker scope contains unattributable history")
        connection.execute("""INSERT INTO general_worker_scope
            SELECT stream_id, run_id, node_id, attempt_id, fencing_generation FROM events
            WHERE stream_type='general_worker' AND stream_version=1""")
        connection.execute("INSERT INTO schema_migrations (version) VALUES (5)")
    connection.execute("CREATE INDEX IF NOT EXISTS ix_general_worker_scope_node ON general_worker_scope(run_id, node_id, stream_id)")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS gw_scope_check BEFORE INSERT ON events
        WHEN NEW.stream_type='general_worker' BEGIN
        SELECT CASE WHEN typeof(NEW.run_id) != 'text' OR length(trim(NEW.run_id, char(9)||char(10)||char(13)||' '))=0
            OR typeof(NEW.node_id) != 'text' OR length(trim(NEW.node_id, char(9)||char(10)||char(13)||' '))=0
            OR typeof(NEW.attempt_id) != 'text' OR length(trim(NEW.attempt_id, char(9)||char(10)||char(13)||' '))=0
            OR typeof(NEW.fencing_generation) != 'integer' OR NEW.fencing_generation <= 0
            THEN RAISE(ABORT, 'general worker scope requires identity') END;
        SELECT CASE WHEN NEW.stream_version > 1 AND NOT EXISTS (
            SELECT 1 FROM general_worker_scope s WHERE s.stream_id=NEW.stream_id
            AND s.run_id=NEW.run_id AND s.node_id=NEW.node_id AND s.attempt_id=NEW.attempt_id
            AND s.fencing_generation=NEW.fencing_generation)
            THEN RAISE(ABORT, 'general worker scope mismatch') END;
        END""")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS gw_scope_insert AFTER INSERT ON events
        WHEN NEW.stream_type='general_worker' AND NEW.stream_version=1 BEGIN
        INSERT INTO general_worker_scope VALUES (NEW.stream_id, NEW.run_id, NEW.node_id,
            NEW.attempt_id, NEW.fencing_generation); END""")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS gw_scope_immutable_update BEFORE UPDATE ON general_worker_scope
        BEGIN SELECT RAISE(ABORT, 'general worker scope is immutable'); END""")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS gw_scope_immutable_delete BEFORE DELETE ON general_worker_scope
        BEGIN SELECT RAISE(ABORT, 'general worker scope is immutable'); END""")

def _migrate_snapshot_metadata(connection: sqlite3.Connection) -> None:
    """Add authenticated snapshot metadata to databases from Task 3."""

    migration_versions = {
        row[0]
        for row in connection.execute(
            "SELECT version FROM schema_migrations"
        ).fetchall()
    }
    columns = connection.execute("PRAGMA table_info(snapshots)").fetchall()
    names = {row[1] for row in columns}
    source_is_required = any(row[1] == "source_event_id" and row[3] for row in columns)
    if "metadata_hash" not in names or source_is_required:
        connection.execute(
            """
            CREATE TABLE snapshots_migration (
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                event_version INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                state_hash TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                source_event_id TEXT,
                created_at TEXT NOT NULL,
                metadata_hash TEXT,
                PRIMARY KEY (aggregate_type, aggregate_id)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO snapshots_migration (
                aggregate_type, aggregate_id, event_version, state_json,
                state_hash, schema_version, source_event_id, created_at,
                metadata_hash
            )
            SELECT aggregate_type, aggregate_id, event_version, state_json,
                   state_hash, schema_version, source_event_id, created_at,
                   NULL
            FROM snapshots
            """
        )
        connection.execute("DROP TABLE snapshots")
        connection.execute("ALTER TABLE snapshots_migration RENAME TO snapshots")

        # The old table did not authenticate any metadata.  In particular,
        # event_version, source_event_id, and created_at came from an
        # untrusted snapshot row, so there is no safe v3 hash to synthesize.
        # Leave metadata_hash NULL and force recovery to replay the stream.
        return

    # Version 3 authenticates ``created_at`` as part of the snapshot
    # metadata.  Databases written by the preceding Task 3 implementation
    # have versions 1 and 2 but no version 3.  Their metadata hash did not
    # authenticate ``created_at``, so there is no trusted value with which to
    # produce a v3 hash.  Mark matching legacy rows unusable and require event
    # replay instead of silently trusting or rehashing their metadata.  Rows
    # that do not match the legacy format are left untouched; v3 validation
    # will accept an already-v3 hash or reject arbitrary tampering.
    if 2 in migration_versions and 3 not in migration_versions:
        rows = connection.execute(
            """
            SELECT aggregate_type, aggregate_id, event_version, schema_version,
                   source_event_id, metadata_hash
            FROM snapshots
            WHERE metadata_hash IS NOT NULL
            """
        ).fetchall()
        for row in rows:
            legacy_hash = row[5]
            expected_legacy_hash = _legacy_snapshot_metadata_hash(
                row[0], row[1], row[2], row[3], row[4]
            )
            if not isinstance(legacy_hash, str) or not hmac.compare_digest(
                legacy_hash, expected_legacy_hash
            ):
                continue
            connection.execute(
                """
                UPDATE snapshots
                SET metadata_hash = ?
                WHERE aggregate_type = ? AND aggregate_id = ?
                """,
                (None, row[0], row[1]),
            )


def _migrate_event_context(connection: sqlite3.Connection) -> None:
    """Add execution identity columns while preserving earlier event rows."""

    columns = {
        row[1] for row in connection.execute("PRAGMA table_info(events)").fetchall()
    }
    for name, sql_type in (
        ("run_id", "TEXT"),
        ("node_id", "TEXT"),
        ("attempt_id", "TEXT"),
        ("fencing_generation", "INTEGER"),
    ):
        if name not in columns:
            connection.execute(f"ALTER TABLE events ADD COLUMN {name} {sql_type}")


def _ensure_events_attempt_context_index(connection: sqlite3.Connection) -> None:
    """Create the contextual lookup index after legacy event columns are migrated."""

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_events_stream_attempt_context
        ON events(stream_type, run_id, node_id, attempt_id)
        """
    )


def _legacy_snapshot_metadata_hash(
    aggregate_type: str,
    aggregate_id: str,
    event_version: int,
    schema_version: int,
    source_event_id: str | None,
) -> str:
    """Hash format emitted before migration 3 authenticated ``created_at``."""

    return hashlib.sha256(
        canonical_json(
            {
                "aggregate_id": aggregate_id,
                "aggregate_type": aggregate_type,
                "event_version": event_version,
                "schema_version": schema_version,
                "source_event_id": source_event_id,
            }
        ).encode("utf-8")
    ).hexdigest()


def _snapshot_metadata_hash(
    aggregate_type: str,
    aggregate_id: str,
    event_version: int,
    schema_version: int,
    source_event_id: str | None,
    created_at: str,
) -> str:
    return hashlib.sha256(
        canonical_json(
            {
                "aggregate_id": aggregate_id,
                "aggregate_type": aggregate_type,
                "event_version": event_version,
                "schema_version": schema_version,
                "source_event_id": source_event_id,
                "created_at": created_at,
            }
        ).encode("utf-8")
    ).hexdigest()


def _draft_request_hash(stream_type: str, stream_id: str, drafts: list[EventDraft]) -> str:
    serialized_events = []
    for draft in drafts:
        item = {"event_type": draft.event_type, "payload": draft.payload}
        context = {
            "run_id": draft.run_id,
            "node_id": draft.node_id,
            "attempt_id": draft.attempt_id,
            "fencing_generation": draft.fencing_generation,
            "correlation_id": draft.correlation_id,
            "causation_id": draft.causation_id,
        }
        if any(value is not None for value in context.values()):
            item["context"] = context
        serialized_events.append(item)
    return _sha256_json(
        {
            "stream_type": stream_type,
            "stream_id": stream_id,
            "events": serialized_events,
        }
    )


_OWNED_WRITES = WeakKeyDictionary()


def _watch_transaction(store_ref, statement):
    """Invalidate on transaction replacement, retaining only actual ROLLBACK TO."""
    # Most traced statements are ordinary reads/writes. Avoid tokenizing their
    # potentially large bound payloads; only ROLLBACK needs detailed parsing.
    prefix = re.sub(r"\A(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*", "", statement, flags=re.S)
    command = re.match(r"([A-Za-z_]+)", prefix)
    if command is None:
        return
    command = command.group(1).upper()
    rollback_to = False
    if command == "ROLLBACK":
        # Preserve quoted identifiers while removing comments everywhere.
        token = re.compile(r"(?P<quoted>'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`|\[[^\]]*\])|(?P<comment>--[^\n]*|/\*.*?\*/)", re.S)
        clean = token.sub(lambda match: match.group("quoted") or " ", prefix)
        identifier = r"(?:\"(?:\"\"|[^\"])*\"|'(?:''|[^'])*'|`(?:``|[^`])*`|\[[^\]]*\]|(?!SAVEPOINT\b)[A-Za-z_\u0080-\uffff][\w$\u0080-\uffff]*)"
        rollback_to = re.fullmatch(
            r"\s*ROLLBACK(?:\s+TRANSACTION)?\s+TO\s+(?:SAVEPOINT\s+)?" + identifier + r"\s*;?\s*",
            clean, flags=re.I | re.S,
        ) is not None
    if command in {"BEGIN", "COMMIT", "END"} or (command == "ROLLBACK" and not rollback_to):
        store = store_ref()
        if store is not None:
            _OWNED_WRITES.pop(store, None)


class SQLiteEventStore:
    """Append-only event storage with stream-version CAS and idempotency.

    A store instance is thread-affine: SQLite's ``check_same_thread=True``
    contract is enforced, so callers must create one store per worker thread.
    Separate instances may safely point at the same database path.
    """

    def __init__(self, path: str | Path) -> None:
        self._connection = _open_connection(path)
        initialize_schema(self._connection)
        self._post_commit_actions = []
        store_ref = ref(self)
        self._connection.set_trace_callback(lambda sql: _watch_transaction(store_ref, sql))

    @classmethod
    def open_read_only(cls, path: str | Path) -> "SQLiteEventStore":
        """Open an existing store without schema writes or a writer lock.

        Independent watcher threads can use this while another connection
        holds a Tool Gateway write transaction. SQLite still validates every
        decoded event through the normal read methods.
        """

        database = Path(path)
        if not database.is_absolute() or not database.is_file() or database.is_symlink():
            raise ValueError("read-only event store requires an existing absolute regular file")
        uri = "file:" + quote(str(database), safe="/") + "?mode=ro"
        connection = sqlite3.connect(
            uri, uri=True, timeout=5.0, isolation_level=None, check_same_thread=True,
            factory=_OwnedConnection,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 5000")
            instance = cls.__new__(cls)
            instance._connection = connection
            instance._post_commit_actions = []
            store_ref = ref(instance)
            connection.set_trace_callback(lambda sql: _watch_transaction(store_ref, sql))
            return instance
        except BaseException:
            connection.close()
            raise

    def _defer_until_commit(self, action):
        if not self._has_owned_write() or not callable(action):
            raise ValueError("deferred action requires an owned write transaction")
        self._post_commit_actions.append(action)

    def _publish_post_commit(self):
        actions, self._post_commit_actions = self._post_commit_actions, []
        for action in actions:
            try:
                action()
            except Exception:
                # A failed publication never changes the durable commit.
                pass
    def _has_owned_write(self):
        marker = _OWNED_WRITES.get(self)
        return (self._connection.in_transaction and marker is not None
            and marker[0] is self._connection and marker[1] == threading.get_ident())

    def _owned_write_token(self):
        return _OWNED_WRITES[self][2] if self._has_owned_write() else None

    def append(
        self,
        stream_type: str,
        stream_id: str,
        expected_version: int,
        events: Iterable[EventDraft],
        idempotency_key: str,
    ) -> list[StoredEvent]:
        stream_type = _validate_identifier(stream_type, "stream_type")
        stream_id = _validate_identifier(stream_id, "stream_id")
        expected_version = _validate_version(expected_version, "expected_version")
        idempotency_key = _validate_identifier(idempotency_key, "idempotency_key")

        try:
            drafts = list(events)
        except TypeError as exc:
            raise ValueError("events must be an iterable of EventDraft objects") from exc
        if not drafts:
            raise ValueError("events must contain at least one EventDraft")
        if not all(isinstance(draft, EventDraft) for draft in drafts):
            raise ValueError("events must contain only EventDraft objects")

        request_hash = _draft_request_hash(stream_type, stream_id, drafts)
        connection = self._connection
        started_transaction = not connection.in_transaction
        savepoint: str | None = None
        if started_transaction:
            connection.execute("BEGIN IMMEDIATE")
            _OWNED_WRITES[self] = (connection, threading.get_ident(), object())
            self._post_commit_actions = []
        else:
            savepoint = "orchestrator_append"
            connection.execute(f"SAVEPOINT {savepoint}")
        try:
            idempotency_row = connection.execute(
                """
                SELECT stream_type, request_hash, first_version, last_version
                FROM idempotency_records
                WHERE stream_id = ? AND idempotency_key = ?
                """,
                (stream_id, idempotency_key),
            ).fetchone()
            if idempotency_row is not None:
                if (
                    idempotency_row["stream_type"] != stream_type
                    or idempotency_row["request_hash"] != request_hash
                ):
                    raise IdempotencyConflict(
                        f"idempotency key {idempotency_key!r} was already used for a different append"
                    )
                original_events = self._read_rows(
                    stream_type,
                    stream_id,
                    first_version=idempotency_row["first_version"],
                    last_version=idempotency_row["last_version"],
                )
                if len(original_events) != (
                    idempotency_row["last_version"] - idempotency_row["first_version"] + 1
                ):
                    raise RuntimeError("idempotency record points to missing events")
                if started_transaction:
                    connection.commit()
                    self._publish_post_commit()
                else:
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                return original_events

            version_row = connection.execute(
                """
                SELECT current_version
                FROM stream_versions
                WHERE stream_type = ? AND stream_id = ?
                """,
                (stream_type, stream_id),
            ).fetchone()
            current_version = 0 if version_row is None else version_row["current_version"]
            if current_version != expected_version:
                raise StaleStream(expected_version, current_version)

            existing_events = self._read_rows(
                stream_type,
                stream_id,
                first_version=1,
                last_version=current_version,
            )
            first_version = current_version + 1
            last_version = current_version + len(drafts)
            if version_row is None:
                connection.execute(
                    """
                    INSERT INTO stream_versions (stream_type, stream_id, current_version)
                    VALUES (?, ?, ?)
                    """,
                    (stream_type, stream_id, last_version),
                )
            else:
                connection.execute(
                    """
                    UPDATE stream_versions
                    SET current_version = ?
                    WHERE stream_type = ? AND stream_id = ?
                    """,
                    (last_version, stream_type, stream_id),
                )

            stored_events: list[StoredEvent] = []
            for offset, draft in enumerate(drafts):
                occurred_at = datetime.now(timezone.utc)
                stored_event = StoredEvent(
                    event_id=new_id(),
                    stream_type=stream_type,
                    stream_id=stream_id,
                    stream_version=first_version + offset,
                    event_type=draft.event_type,
                    schema_version=2 if stream_type == "provider_call" else 1,
                    occurred_at=occurred_at,
                    payload=draft.payload,
                    payload_hash=_sha256_json(draft.payload),
                    idempotency_key=idempotency_key,
                    run_id=draft.run_id,
                    node_id=draft.node_id,
                    attempt_id=draft.attempt_id,
                    fencing_generation=draft.fencing_generation,
                    correlation_id=draft.correlation_id,
                    causation_id=draft.causation_id,
                )
                stored_events.append(stored_event)

            try:
                validate_event_contract(existing_events + stored_events)
            except EventContractError:
                raise

            for stored_event in stored_events:
                connection.execute(
                    """
                    INSERT INTO events (
                        event_id, stream_type, stream_id, stream_version,
                        event_type, schema_version, occurred_at, payload_json,
                        payload_hash, idempotency_key, run_id, node_id,
                        attempt_id, fencing_generation, correlation_id, causation_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        stored_event.event_id,
                        stored_event.stream_type,
                        stored_event.stream_id,
                        stored_event.stream_version,
                        stored_event.event_type,
                        stored_event.schema_version,
                        stored_event.occurred_at.isoformat(),
                        canonical_json(stored_event.payload),
                        stored_event.payload_hash,
                        stored_event.idempotency_key,
                        stored_event.run_id,
                        stored_event.node_id,
                        stored_event.attempt_id,
                        stored_event.fencing_generation,
                        stored_event.correlation_id,
                        stored_event.causation_id,
                    ),
                )

            connection.execute(
                """
                INSERT INTO idempotency_records (
                    stream_id, idempotency_key, stream_type, request_hash,
                    first_version, last_version
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    stream_id,
                    idempotency_key,
                    stream_type,
                    request_hash,
                    first_version,
                    last_version,
                ),
            )
            if started_transaction:
                connection.commit()
                self._publish_post_commit()
            else:
                connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            return stored_events
        except BaseException:
            if started_transaction:
                connection.rollback()
            elif savepoint is not None:
                try:
                    connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                finally:
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        finally:
            if started_transaction:
                _OWNED_WRITES.pop(self, None)
                self._post_commit_actions = []

    def append_checked(
        self,
        stream_type: str,
        stream_id: str,
        idempotency_key: str,
        decide: Callable[[list[StoredEvent], int], Iterable[EventDraft] | None],
    ) -> list[StoredEvent]:
        """Run a locked read-modify-append operation atomically.

        ``decide`` executes while ``BEGIN IMMEDIATE`` holds the database
        write lock. Nested calls use savepoints and remain part of the outer
        transaction. The callback receives the complete validated stream and
        its current version, and returns the drafts to append. Returning
        ``None`` is a no-op and is useful for an operation whose idempotency
        key was already observed by the caller. The regular append
        implementation is savepoint-aware, so each append remains subject to
        its normal CAS and idempotency checks while sharing this transaction.
        """

        stream_type = _validate_identifier(stream_type, "stream_type")
        stream_id = _validate_identifier(stream_id, "stream_id")
        idempotency_key = _validate_identifier(idempotency_key, "idempotency_key")
        if not callable(decide):
            raise TypeError("decide must be callable")
        connection = self._connection
        started_transaction = not connection.in_transaction
        savepoint: str | None = None
        if started_transaction:
            connection.execute("BEGIN IMMEDIATE")
            _OWNED_WRITES[self] = (connection, threading.get_ident(), object())
            self._post_commit_actions = []
        else:
            savepoint = "orchestrator_append_checked"
            connection.execute(f"SAVEPOINT {savepoint}")
        try:
            events = self._read_rows(
                stream_type,
                stream_id,
                first_version=1,
                last_version=self.current_version(stream_type, stream_id),
            )
            current_version = self.current_version(stream_type, stream_id)
            drafts = decide(events, current_version)
            if drafts is None:
                existing = [
                    event for event in events if event.idempotency_key == idempotency_key
                ]
                if started_transaction:
                    connection.commit()
                    self._publish_post_commit()
                else:
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                return existing
            appended = self.append(
                stream_type,
                stream_id,
                current_version,
                drafts,
                idempotency_key,
            )
            if started_transaction:
                connection.commit()
                self._publish_post_commit()
            else:
                connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            return appended
        except BaseException:
            if started_transaction:
                connection.rollback()
            elif savepoint is not None:
                try:
                    connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                finally:
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        finally:
            if started_transaction:
                _OWNED_WRITES.pop(self, None)
                self._post_commit_actions = []

    def read_stream(
        self,
        stream_type: str,
        stream_id: str,
        after_version: int = 0,
    ) -> list[StoredEvent]:
        stream_type = _validate_identifier(stream_type, "stream_type")
        stream_id = _validate_identifier(stream_id, "stream_id")
        after_version = _validate_version(after_version, "after_version")
        rows = self._connection.execute(
            """
            SELECT event_id, stream_type, stream_id, stream_version,
                   event_type, schema_version, occurred_at, payload_json,
                   payload_hash, idempotency_key, run_id, node_id, attempt_id,
                   fencing_generation, correlation_id, causation_id
            FROM events
            WHERE stream_type = ? AND stream_id = ? AND stream_version > ?
            ORDER BY stream_version ASC
            """,
            (stream_type, stream_id, after_version),
        ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def read_streams_consistent(
        self,
        streams: Iterable[tuple[str, str]],
    ) -> dict[tuple[str, str], list[StoredEvent]]:
        """Read several streams from one SQLite snapshot.

        Security decisions that join lifecycle, scheduler, and agent state
        must not combine events from different committed instants. The
        transaction also keeps nested calls safe when a trusted caller is
        already holding a write transaction on this store instance.
        """

        keys = tuple(streams)
        if not keys or len(keys) != len(set(keys)):
            raise ValueError("streams must be a non-empty set of unique stream keys")
        for key in keys:
            if not isinstance(key, tuple) or len(key) != 2:
                raise ValueError("each stream key must contain type and id")
            _validate_identifier(key[0], "stream_type")
            _validate_identifier(key[1], "stream_id")
        connection = self._connection
        started_transaction = not connection.in_transaction
        savepoint = "orchestrator_multi_stream_read"
        if started_transaction:
            connection.execute("BEGIN")
        else:
            connection.execute(f"SAVEPOINT {savepoint}")
        try:
            result = {key: self.read_stream(*key) for key in keys}
            if started_transaction:
                connection.commit()
            else:
                connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            return result
        except BaseException:
            if started_transaction:
                connection.rollback()
            else:
                try:
                    connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                finally:
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise

    def read_stream_with_version(
        self,
        stream_type: str,
        stream_id: str,
        after_version: int = 0,
        *, max_events: int | None = None,
    ) -> tuple[list[StoredEvent], int]:
        """Read a stream and its version from one SQLite read transaction.

        Keeping both reads in one transaction prevents recovery from seeing a
        stream's rows before a concurrent append and its version afterwards.
        """

        stream_type = _validate_identifier(stream_type, "stream_type")
        stream_id = _validate_identifier(stream_id, "stream_id")
        after_version = _validate_version(after_version, "after_version")
        if max_events is not None and (type(max_events) is not int or max_events < 1):
            raise ValueError("max_events must be positive")
        connection = self._connection
        savepoint: str | None = None
        started_transaction = not connection.in_transaction
        if started_transaction:
            connection.execute("BEGIN")
        else:
            savepoint = "orchestrator_stream_read"
            connection.execute(f"SAVEPOINT {savepoint}")
        try:
            rows = connection.execute(
                """
                SELECT event_id, stream_type, stream_id, stream_version,
                       event_type, schema_version, occurred_at, payload_json,
                       payload_hash, idempotency_key, run_id, node_id, attempt_id,
                       fencing_generation, correlation_id, causation_id
                FROM events
                WHERE stream_type = ? AND stream_id = ? AND stream_version > ?
                ORDER BY stream_version ASC LIMIT ?
                """,
                (stream_type, stream_id, after_version, -1 if max_events is None else max_events + 1),
            ).fetchall()
            if max_events is not None and len(rows) > max_events:
                raise ValueError("stream exceeds event bound")
            version_row = connection.execute(
                """
                SELECT current_version
                FROM stream_versions
                WHERE stream_type = ? AND stream_id = ?
                """,
                (stream_type, stream_id),
            ).fetchone()
            events = [self._row_to_event(row) for row in rows]
            current_version = 0 if version_row is None else version_row["current_version"]
            if started_transaction:
                connection.commit()
            else:
                connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            return events, current_version
        except BaseException:
            if started_transaction:
                connection.rollback()
            elif savepoint is not None:
                try:
                    connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                finally:
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise

    # Name the operation after the consistent point-in-time view as well;
    # both spellings are kept as public compatibility aliases.
    read_stream_snapshot = read_stream_with_version

    def current_version(self, stream_type: str, stream_id: str) -> int:
        stream_type = _validate_identifier(stream_type, "stream_type")
        stream_id = _validate_identifier(stream_id, "stream_id")
        row = self._connection.execute(
            """
            SELECT current_version
            FROM stream_versions
            WHERE stream_type = ? AND stream_id = ?
            """,
            (stream_type, stream_id),
        ).fetchone()
        return 0 if row is None else row["current_version"]

    def stream_ids(self, stream_type: str) -> list[str]:
        """Return stream IDs for a type without exposing the SQLite handle."""

        stream_type = _validate_identifier(stream_type, "stream_type")
        rows = self._connection.execute(
            """
            SELECT stream_id
            FROM stream_versions
            WHERE stream_type = ?
            ORDER BY stream_id ASC
            """,
            (stream_type,),
        ).fetchall()
        return [row["stream_id"] for row in rows]

    def stream_ids_for_worker_node(self, run_id: str, node_id: str, *, limit: int) -> list[str]:
        run_id = _validate_identifier(run_id, "run_id")
        node_id = _validate_identifier(node_id, "node_id")
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be positive")
        rows = self._connection.execute(
            "SELECT stream_id FROM general_worker_scope WHERE run_id=? AND node_id=? ORDER BY stream_id LIMIT ?",
            (run_id, node_id, limit)).fetchall()
        return [row["stream_id"] for row in rows]
    def stream_ids_for_context(
        self, stream_type: str, run_id: str, node_id: str, attempt_id: str
    ) -> list[str]:
        """Return streams containing this operation tuple, regardless of fencing generation."""

        stream_type = _validate_identifier(stream_type, "stream_type")
        run_id = _validate_identifier(run_id, "run_id")
        node_id = _validate_identifier(node_id, "node_id")
        attempt_id = _validate_identifier(attempt_id, "attempt_id")
        rows = self._connection.execute(
            """
            SELECT DISTINCT stream_id
            FROM events
            WHERE stream_type = ? AND run_id = ? AND node_id = ? AND attempt_id = ?
            ORDER BY stream_id ASC
            """,
            (stream_type, run_id, node_id, attempt_id),
        ).fetchall()
        return [row["stream_id"] for row in rows]

    def _read_rows(
        self,
        stream_type: str,
        stream_id: str,
        *,
        first_version: int,
        last_version: int,
    ) -> list[StoredEvent]:
        rows = self._connection.execute(
            """
            SELECT event_id, stream_type, stream_id, stream_version,
                   event_type, schema_version, occurred_at, payload_json,
                   payload_hash, idempotency_key, run_id, node_id, attempt_id,
                   fencing_generation, correlation_id, causation_id
            FROM events
            WHERE stream_type = ? AND stream_id = ?
              AND stream_version BETWEEN ? AND ?
            ORDER BY stream_version ASC
            """,
            (stream_type, stream_id, first_version, last_version),
        ).fetchall()
        return [self._row_to_event(row) for row in rows]

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> StoredEvent:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise EventIntegrityError(
                f"event {row['event_id']!r} contains invalid payload JSON"
            ) from exc
        try:
            computed_hash = _sha256_json(payload)
        except ValueError as exc:
            raise EventIntegrityError(
                f"event {row['event_id']!r} contains an invalid payload"
            ) from exc
        try:
            stored_hash = _validate_sha256_hex(row["payload_hash"])
        except ValueError as exc:
            raise EventIntegrityError(
                f"event {row['event_id']!r} has a malformed payload_hash"
            ) from exc
        if not hmac.compare_digest(computed_hash, stored_hash):
            raise EventIntegrityError(
                f"event {row['event_id']!r} payload_hash does not match payload"
            )
        try:
            return StoredEvent(
                event_id=row["event_id"],
                stream_type=row["stream_type"],
                stream_id=row["stream_id"],
                stream_version=row["stream_version"],
                event_type=row["event_type"],
                schema_version=row["schema_version"],
                occurred_at=datetime.fromisoformat(row["occurred_at"]),
                payload=payload,
                payload_hash=row["payload_hash"],
                idempotency_key=row["idempotency_key"],
                run_id=row["run_id"],
                node_id=row["node_id"],
                attempt_id=row["attempt_id"],
                fencing_generation=row["fencing_generation"],
                correlation_id=row["correlation_id"],
                causation_id=row["causation_id"],
            )
        except (TypeError, ValueError) as exc:
            raise EventIntegrityError(
                f"event {row['event_id']!r} fails persisted field validation"
            ) from exc

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "SQLiteEventStore":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
