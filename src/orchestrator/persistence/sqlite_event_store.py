"""SQLite implementation of the append-only event store."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable

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


def _open_connection(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        str(path),
        timeout=5.0,
        isolation_level=None,
        check_same_thread=True,
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
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (1)")
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (2)")
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (3)")
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (4)")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


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


class SQLiteEventStore:
    """Append-only event storage with stream-version CAS and idempotency.

    A store instance is thread-affine: SQLite's ``check_same_thread=True``
    contract is enforced, so callers must create one store per worker thread.
    Separate instances may safely point at the same database path.
    """

    def __init__(self, path: str | Path) -> None:
        self._connection = _open_connection(path)
        initialize_schema(self._connection)

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
                    schema_version=1,
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

    def read_stream_with_version(
        self,
        stream_type: str,
        stream_id: str,
        after_version: int = 0,
    ) -> tuple[list[StoredEvent], int]:
        """Read a stream and its version from one SQLite read transaction.

        Keeping both reads in one transaction prevents recovery from seeing a
        stream's rows before a concurrent append and its version afterwards.
        """

        stream_type = _validate_identifier(stream_type, "stream_type")
        stream_id = _validate_identifier(stream_id, "stream_id")
        after_version = _validate_version(after_version, "after_version")
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
                ORDER BY stream_version ASC
                """,
                (stream_type, stream_id, after_version),
            ).fetchall()
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
