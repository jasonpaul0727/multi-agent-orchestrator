"""SQLite implementation of the append-only event store."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import sqlite3
from typing import Any

from orchestrator.identifiers import new_id

from .events import EventDraft, StoredEvent, _validate_sha256_hex


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
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


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
            connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (1)")
            connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (2)")
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
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (1)")
        connection.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (2)")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _migrate_snapshot_metadata(connection: sqlite3.Connection) -> None:
    """Add authenticated snapshot metadata to databases from Task 3."""

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

        # Only rows copied out of the pre-v2 table are missing authenticated
        # metadata.  Current-schema NULLs are tampering/corruption and must
        # remain unusable rather than being silently repaired at open time.
        rows = connection.execute(
            """
            SELECT aggregate_type, aggregate_id, event_version, schema_version,
                   source_event_id, created_at
            FROM snapshots
            WHERE metadata_hash IS NULL
            """
        ).fetchall()
        for row in rows:
            metadata_hash = _snapshot_metadata_hash(
                row[0], row[1], row[2], row[3], row[4], row[5]
            )
            connection.execute(
                """
                UPDATE snapshots
                SET metadata_hash = ?
                WHERE aggregate_type = ? AND aggregate_id = ?
                """,
                (metadata_hash, row[0], row[1]),
            )


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
    return _sha256_json(
        {
            "stream_type": stream_type,
            "stream_id": stream_id,
            "events": [
                {"event_type": draft.event_type, "payload": draft.payload}
                for draft in drafts
            ],
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
        connection.execute("BEGIN IMMEDIATE")
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
                connection.commit()
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
                )
                connection.execute(
                    """
                    INSERT INTO events (
                        event_id, stream_type, stream_id, stream_version,
                        event_type, schema_version, occurred_at, payload_json,
                        payload_hash, idempotency_key, correlation_id, causation_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        stored_event.correlation_id,
                        stored_event.causation_id,
                    ),
                )
                stored_events.append(stored_event)

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
            connection.commit()
            return stored_events
        except BaseException:
            connection.rollback()
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
                   payload_hash, idempotency_key, correlation_id, causation_id
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
                       payload_hash, idempotency_key, correlation_id, causation_id
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
                   payload_hash, idempotency_key, correlation_id, causation_id
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
