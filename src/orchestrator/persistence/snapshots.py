"""Validated, replaceable aggregate snapshots for deterministic recovery."""

from __future__ import annotations

from datetime import datetime, timezone
import hmac
import json
from pathlib import Path
import sqlite3
from typing import Any
import warnings

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

from .events import (
    _freeze_json_value,
    _validate_json_value,
    _validate_non_blank,
    _validate_sha256_hex,
)
from .sqlite_event_store import (
    _open_connection,
    _sha256_json,
    _validate_version,
    initialize_schema,
)


_MISSING = object()


class StaleSnapshot(RuntimeError):
    """Raised when a checkpoint would move an aggregate back in time."""

    def __init__(self, attempted_version: int, current_version: int) -> None:
        self.attempted_version = attempted_version
        self.current_version = current_version
        super().__init__(
            "snapshot version is stale: "
            f"attempted {attempted_version}, current {current_version}"
        )


class Snapshot(BaseModel):
    """An immutable in-memory representation of a persisted snapshot."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    aggregate_type: StrictStr = Field(min_length=1)
    aggregate_id: StrictStr = Field(min_length=1)
    event_version: StrictInt = Field(gt=0)
    state: Any
    state_hash: StrictStr = Field(min_length=64, max_length=64)
    schema_version: StrictInt = Field(gt=0)
    source_event_id: StrictStr | None = None
    created_at: datetime
    metadata_hash: StrictStr | None = None

    @property
    def version(self) -> int:
        return self.event_version

    @property
    def canonical_state_hash(self) -> str:
        return self.state_hash

    @property
    def source_version(self) -> int:
        return self.event_version

    @field_validator("aggregate_type", "aggregate_id", mode="before")
    @classmethod
    def validate_identifiers(cls, value: Any, info: Any) -> Any:
        return _validate_non_blank(value, info.field_name)

    @field_validator("source_event_id", mode="before")
    @classmethod
    def validate_source_event_id(cls, value: Any) -> Any:
        if value is None:
            return None
        return _validate_non_blank(value, "source_event_id")

    @field_validator("state", mode="before")
    @classmethod
    def validate_state(cls, value: Any) -> Any:
        _validate_json_value(value, path="state")
        return _freeze_json_value(value)

    @field_validator("state_hash", mode="before")
    @classmethod
    def validate_state_hash(cls, value: Any) -> Any:
        return _validate_sha256_hex(value, "state_hash")

    @field_validator("metadata_hash", mode="before")
    @classmethod
    def validate_metadata_hash(cls, value: Any) -> Any:
        if value is None:
            return None
        return _validate_sha256_hex(value, "metadata_hash")

    @model_validator(mode="after")
    def validate_hash(self) -> "Snapshot":
        try:
            computed = _sha256_json(self.state)
        except ValueError as exc:
            raise ValueError("state is not canonically serializable") from exc
        if not hmac.compare_digest(computed, self.state_hash):
            raise ValueError("state_hash does not match state")
        if self.metadata_hash is not None:
            expected_metadata_hash = _metadata_hash(
                self.aggregate_type,
                self.aggregate_id,
                self.event_version,
                self.schema_version,
                self.source_event_id,
                self.created_at.isoformat(),
            )
            if not hmac.compare_digest(expected_metadata_hash, self.metadata_hash):
                raise ValueError("metadata_hash does not match snapshot metadata")
        return self


# This name makes the storage boundary explicit for callers that prefer the
# record terminology.  Snapshot remains the canonical public type.
SnapshotRecord = Snapshot


class SnapshotStore:
    """SQLite-backed snapshots with atomic replacement and validation.

    Snapshots are intentionally replaceable: a newer checkpoint supersedes an
    older checkpoint for the same aggregate.  Event rows remain protected by
    the append-only triggers owned by :class:`SQLiteEventStore`.

    ``source_event_id`` is optional for generic checkpointing.  Recovery treats
    a source-less snapshot as anchored at its validated stream version and
    replays the event tail from that anchor; callers that need an explicit
    identity can persist the source event ID and have it checked as well.
    """

    def __init__(self, database: str | Path | sqlite3.Connection | Any) -> None:
        self._owns_connection = True
        if isinstance(database, sqlite3.Connection):
            self._connection = database
            self._owns_connection = False
        elif hasattr(database, "_connection"):
            # Sharing a SQLiteEventStore connection keeps both stores thread
            # affine and is useful for an atomic caller-managed workflow.
            self._connection = database._connection
            self._owns_connection = False
        else:
            self._connection = _open_connection(database)
        # Raw sqlite3.connect() uses tuple rows by default.  Use named rows
        # when possible; _row_value below still accepts tuple rows from
        # caller-managed connections and older adapters.
        if self._connection.row_factory is None:
            self._connection.row_factory = sqlite3.Row
        initialize_schema(self._connection)

    def save_snapshot(
        self,
        aggregate_type: str,
        aggregate_id: str,
        event_version: int,
        state: Any,
        schema_version: int = 1,
        source_event_id: str | None = None,
        state_hash: str | None = None,
    ) -> Snapshot:
        """Persist a checkpoint and return its validated representation."""

        aggregate_type = _validate_non_blank(aggregate_type, "aggregate_type")
        aggregate_id = _validate_non_blank(aggregate_id, "aggregate_id")
        event_version = _validate_positive_version(event_version, "event_version")
        schema_version = _validate_positive_version(schema_version, "schema_version")
        if source_event_id is not None:
            source_event_id = _validate_non_blank(source_event_id, "source_event_id")

        state_json = _canonical_state_json(state)
        computed_hash = _sha256_json(state)
        if state_hash is not None:
            state_hash = _validate_sha256_hex(state_hash, "state_hash")
            if not hmac.compare_digest(computed_hash, state_hash):
                raise ValueError("state_hash does not match state")
        else:
            state_hash = computed_hash
        created_at = datetime.now(timezone.utc)
        metadata_hash = _metadata_hash(
            aggregate_type,
            aggregate_id,
            event_version,
            schema_version,
            source_event_id,
            created_at.isoformat(),
        )
        snapshot = Snapshot(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_version=event_version,
            state=_decode_state(state_json),
            state_hash=state_hash,
            schema_version=schema_version,
            source_event_id=source_event_id,
            created_at=created_at,
            metadata_hash=metadata_hash,
        )

        connection = self._connection
        started_transaction = not connection.in_transaction
        savepoint: str | None = None
        if started_transaction:
            connection.execute("BEGIN IMMEDIATE")
        else:
            savepoint = "orchestrator_snapshot_write"
            connection.execute(f"SAVEPOINT {savepoint}")
        try:
            current = connection.execute(
                """
                SELECT event_version
                FROM snapshots
                WHERE aggregate_type = ? AND aggregate_id = ?
                """,
                (snapshot.aggregate_type, snapshot.aggregate_id),
            ).fetchone()
            if current is not None:
                current_version = _row_value(current, "event_version", 0)
                if current_version > snapshot.event_version:
                    raise StaleSnapshot(snapshot.event_version, current_version)
            connection.execute(
                """
                INSERT INTO snapshots (
                    aggregate_type, aggregate_id, event_version, state_json,
                    state_hash, schema_version, source_event_id, created_at,
                    metadata_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET
                    event_version = excluded.event_version,
                    state_json = excluded.state_json,
                    state_hash = excluded.state_hash,
                    schema_version = excluded.schema_version,
                    source_event_id = excluded.source_event_id,
                    created_at = excluded.created_at,
                    metadata_hash = excluded.metadata_hash
                WHERE snapshots.event_version <= excluded.event_version
                """,
                (
                    snapshot.aggregate_type,
                    snapshot.aggregate_id,
                    snapshot.event_version,
                    state_json,
                    snapshot.state_hash,
                    snapshot.schema_version,
                    snapshot.source_event_id,
                    snapshot.created_at.isoformat(),
                    snapshot.metadata_hash,
                ),
            )
            current_after = connection.execute(
                """
                SELECT event_version
                FROM snapshots
                WHERE aggregate_type = ? AND aggregate_id = ?
                """,
                (snapshot.aggregate_type, snapshot.aggregate_id),
            ).fetchone()
            if current_after is not None:
                current_after_version = _row_value(current_after, "event_version", 0)
                if current_after_version > snapshot.event_version:
                    raise StaleSnapshot(snapshot.event_version, current_after_version)
            if started_transaction:
                connection.commit()
            else:
                connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        except BaseException:
            if started_transaction:
                connection.rollback()
            elif savepoint is not None:
                try:
                    connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                finally:
                    connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        return snapshot

    def save(
        self,
        aggregate_type: str,
        aggregate_id: str,
        state: Any = _MISSING,
        version: Any = None,
        *legacy_args: Any,
        event_version: int | None = None,
        schema_version: int = 1,
        source_event_id: str | None = None,
        state_hash: str | None = None,
    ) -> Snapshot:
        """Persist ``state`` at ``version`` using a deterministic signature.

        ``save_snapshot`` remains the lower-level API with event-version-first
        arguments.  The historical positional form with trailing metadata
        arguments remains supported with a deprecation warning; its
        four-position prefix is intentionally rejected as ambiguous.  New
        callers should use the deterministic state-first form, while legacy
        callers can use :meth:`save_legacy` or ``save_snapshot`` explicitly.
        """

        if state is _MISSING:
            raise TypeError("save requires state as its third argument")
        if legacy_args:
            if isinstance(version, int) and not isinstance(version, bool):
                # Unambiguous state-first positional metadata, retained for
                # callers of the original convenience wrapper.
                if isinstance(state, int) and not isinstance(state, bool):
                    raise TypeError(
                        "save cannot disambiguate integer state and legacy "
                        "event-version order; use keyword version or save_snapshot"
                    )
                if event_version is not None:
                    raise TypeError("version and event_version disagree")
                if len(legacy_args) > 3:
                    raise TypeError(
                        "state-first save accepts at most three trailing arguments"
                    )
                schema_version = legacy_args[0] if len(legacy_args) >= 1 else schema_version
                source_event_id = (
                    legacy_args[1] if len(legacy_args) >= 2 else source_event_id
                )
                state_hash = legacy_args[2] if len(legacy_args) >= 3 else state_hash
                event_version = version
                return self.save_snapshot(
                    aggregate_type,
                    aggregate_id,
                    event_version,
                    state,
                    schema_version,
                    source_event_id,
                    state_hash,
                )
            if (
                isinstance(state, bool)
                or not isinstance(state, int)
                or isinstance(version, (type(None), int, bool))
            ):
                raise TypeError(
                    "save expects state first and an integer version; use "
                    "save_snapshot for legacy order"
                )
            if event_version is not None:
                raise TypeError("legacy positional save cannot combine event_version")
            if len(legacy_args) > 3:
                raise TypeError("legacy positional save accepts at most three trailing arguments")
            warnings.warn(
                "legacy positional save is deprecated; use save_snapshot",
                DeprecationWarning,
                stacklevel=2,
            )
            legacy_schema_version = legacy_args[0] if len(legacy_args) >= 1 else 1
            legacy_source_event_id = legacy_args[1] if len(legacy_args) >= 2 else None
            legacy_state_hash = legacy_args[2] if len(legacy_args) >= 3 else None
            return self.save_snapshot(
                aggregate_type,
                aggregate_id,
                state,
                version,
                legacy_schema_version,
                legacy_source_event_id,
                legacy_state_hash,
            )
        if version is not None and (
            isinstance(version, bool) or not isinstance(version, int)
        ):
            raise TypeError(
                "save expects state first and an integer version; use save_snapshot for legacy order"
            )
        if event_version is None:
            event_version = version
        elif version is not None and event_version != version:
            raise TypeError("version and event_version disagree")
        if event_version is None:
            raise TypeError("save requires version or event_version")
        return self.save_snapshot(
            aggregate_type,
            aggregate_id,
            event_version,
            state,
            schema_version,
            source_event_id,
            state_hash,
        )

    def save_legacy(
        self,
        aggregate_type: str,
        aggregate_id: str,
        event_version: int,
        state: Any,
        schema_version: int = 1,
        source_event_id: str | None = None,
        state_hash: str | None = None,
    ) -> Snapshot:
        """Explicit compatibility entry point for event-version-first callers.

        The four-position form cannot be distinguished from an invalid
        state-first call, so callers using the historical order should use
        this named, deprecated entry point (or :meth:`save_snapshot`).
        """

        warnings.warn(
            "save_legacy is deprecated; use save_snapshot or state-first save",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.save_snapshot(
            aggregate_type,
            aggregate_id,
            event_version,
            state,
            schema_version,
            source_event_id,
            state_hash,
        )

    def load_valid(
        self,
        aggregate_type: str,
        aggregate_id: str,
        *,
        expected_schema_version: int | None = None,
        expected_source_version: int | None = None,
        expected_event_version: int | None = None,
        expected_source_event_id: str | None = None,
        schema_version: int | None = None,
        source_version: int | None = None,
        source_event_id: str | None = None,
        expected_version: int | None = None,
        version: int | None = None,
    ) -> Snapshot | None:
        """Load a snapshot only when all persisted and expected facts agree.

        Invalid JSON, malformed fields, hash mismatches, schema mismatches, or
        source-version mismatches are treated as an unusable checkpoint.  The
        recovery layer can then replay the complete event stream.
        """

        aggregate_type = _validate_non_blank(aggregate_type, "aggregate_type")
        aggregate_id = _validate_non_blank(aggregate_id, "aggregate_id")
        if schema_version is not None:
            if expected_schema_version is not None and expected_schema_version != schema_version:
                return None
            expected_schema_version = schema_version
        if source_version is not None:
            if expected_source_version is not None and expected_source_version != source_version:
                return None
            expected_source_version = source_version
        if source_event_id is not None:
            if (
                expected_source_event_id is not None
                and expected_source_event_id != source_event_id
            ):
                return None
            expected_source_event_id = source_event_id
        if version is not None:
            if expected_version is not None and expected_version != version:
                return None
            expected_version = version
        if expected_version is not None:
            if expected_event_version is not None and expected_event_version != expected_version:
                return None
            expected_event_version = expected_version
        for value, name in (
            (expected_schema_version, "expected_schema_version"),
            (expected_source_version, "expected_source_version"),
            (expected_event_version, "expected_event_version"),
        ):
            if value is not None:
                _validate_positive_version(value, name)
        if expected_source_event_id is not None:
            expected_source_event_id = _validate_non_blank(
                expected_source_event_id, "expected_source_event_id"
            )

        row = self._connection.execute(
            """
            SELECT aggregate_type, aggregate_id, event_version, state_json,
                   state_hash, schema_version, source_event_id, created_at,
                   metadata_hash
            FROM snapshots
            WHERE aggregate_type = ? AND aggregate_id = ?
            """,
            (aggregate_type, aggregate_id),
        ).fetchone()
        if row is None:
            return None
        snapshot = self._row_to_snapshot(row)
        if snapshot is None:
            return None
        if expected_schema_version is not None and snapshot.schema_version != expected_schema_version:
            return None
        expected_version = expected_event_version
        if expected_version is not None and snapshot.event_version != expected_version:
            return None
        if (
            expected_source_version is not None
            and snapshot.event_version != expected_source_version
        ):
            return None
        if (
            expected_source_event_id is not None
            and snapshot.source_event_id != expected_source_event_id
        ):
            return None
        return snapshot

    load = load_valid

    @staticmethod
    def _row_to_snapshot(row: sqlite3.Row) -> Snapshot | None:
        try:
            state = _decode_state(_row_value(row, "state_json", 3))
            # Validate the stored hash before constructing the model.  This
            # ensures malformed rows never expose their payload to callers.
            stored_hash = _validate_sha256_hex(
                _row_value(row, "state_hash", 4), "state_hash"
            )
            if not hmac.compare_digest(_sha256_json(state), stored_hash):
                return None
            metadata_hash = _row_value(row, "metadata_hash", 8, default=None)
            if metadata_hash is None:
                return None
            metadata_hash = _validate_sha256_hex(metadata_hash, "metadata_hash")
            aggregate_type = _row_value(row, "aggregate_type", 0)
            aggregate_id = _row_value(row, "aggregate_id", 1)
            event_version = _row_value(row, "event_version", 2)
            schema_version = _row_value(row, "schema_version", 5)
            source_event_id = _row_value(row, "source_event_id", 6)
            created_at = datetime.fromisoformat(_row_value(row, "created_at", 7))
            expected_metadata_hash = _metadata_hash(
                aggregate_type,
                aggregate_id,
                event_version,
                schema_version,
                source_event_id,
                created_at.isoformat(),
            )
            if not hmac.compare_digest(expected_metadata_hash, metadata_hash):
                return None
            return Snapshot(
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                event_version=event_version,
                state=state,
                state_hash=stored_hash,
                schema_version=schema_version,
                source_event_id=source_event_id,
                created_at=created_at,
                metadata_hash=metadata_hash,
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    def close(self) -> None:
        if self._owns_connection:
            self._connection.close()

    def __enter__(self) -> "SnapshotStore":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()


def _validate_positive_version(value: Any, field_name: str) -> int:
    # Reuse the event store's strict integer validation while changing the
    # non-negative rule to the positive rule required by persisted records.
    value = _validate_version(value, field_name)
    if value == 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _canonical_state_json(state: Any) -> str:
    try:
        encoded = json.dumps(
            state,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        encoded.encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError("state must be canonically JSON serializable") from exc
    _validate_json_value(state, path="state")
    return encoded


def _decode_state(value: Any) -> Any:
    if not isinstance(value, str):
        raise ValueError("state_json must be text")
    return json.loads(value)


def _metadata_hash(
    aggregate_type: str,
    aggregate_id: str,
    event_version: int,
    schema_version: int,
    source_event_id: str | None,
    created_at: str,
) -> str:
    return _sha256_json(
        {
            "aggregate_id": aggregate_id,
            "aggregate_type": aggregate_type,
            "event_version": event_version,
            "schema_version": schema_version,
            "source_event_id": source_event_id,
            "created_at": created_at,
        }
    )


def _row_value(
    row: sqlite3.Row | tuple[Any, ...],
    name: str,
    index: int,
    *,
    default: Any = _MISSING,
) -> Any:
    try:
        return row[name]  # type: ignore[index]
    except (IndexError, KeyError, TypeError):
        try:
            return row[index]
        except (IndexError, TypeError):
            if default is not _MISSING:
                return default
            raise


__all__ = ["Snapshot", "SnapshotRecord", "SnapshotStore", "StaleSnapshot"]
