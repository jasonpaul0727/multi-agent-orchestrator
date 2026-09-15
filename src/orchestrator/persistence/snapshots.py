"""Validated, replaceable aggregate snapshots for deterministic recovery."""

from __future__ import annotations

from datetime import datetime, timezone
import hmac
import json
from pathlib import Path
import sqlite3
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

from .events import _validate_json_value, _validate_non_blank, _validate_sha256_hex
from .sqlite_event_store import (
    _open_connection,
    _sha256_json,
    _validate_version,
    initialize_schema,
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
    source_event_id: StrictStr = Field(min_length=1)
    created_at: datetime

    @property
    def version(self) -> int:
        return self.event_version

    @property
    def canonical_state_hash(self) -> str:
        return self.state_hash

    @property
    def source_version(self) -> int:
        return self.event_version

    @field_validator("aggregate_type", "aggregate_id", "source_event_id", mode="before")
    @classmethod
    def validate_identifiers(cls, value: Any, info: Any) -> Any:
        return _validate_non_blank(value, info.field_name)

    @field_validator("state", mode="before")
    @classmethod
    def validate_state(cls, value: Any) -> Any:
        _validate_json_value(value, path="state")
        return value

    @field_validator("state_hash", mode="before")
    @classmethod
    def validate_state_hash(cls, value: Any) -> Any:
        return _validate_sha256_hex(value, "state_hash")

    @model_validator(mode="after")
    def validate_hash(self) -> "Snapshot":
        try:
            computed = _sha256_json(self.state)
        except ValueError as exc:
            raise ValueError("state is not canonically serializable") from exc
        if not hmac.compare_digest(computed, self.state_hash):
            raise ValueError("state_hash does not match state")
        return self


# This name makes the storage boundary explicit for callers that prefer the
# record terminology.  Snapshot remains the canonical public type.
SnapshotRecord = Snapshot


class SnapshotStore:
    """SQLite-backed snapshots with atomic replacement and validation.

    Snapshots are intentionally replaceable: a newer checkpoint supersedes an
    older checkpoint for the same aggregate.  Event rows remain protected by
    the append-only triggers owned by :class:`SQLiteEventStore`.
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
        if source_event_id is None:
            raise ValueError("source_event_id must be a non-blank string")
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
        snapshot = Snapshot(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_version=event_version,
            state=_decode_state(state_json),
            state_hash=state_hash,
            schema_version=schema_version,
            source_event_id=source_event_id,
            created_at=created_at,
        )

        connection = self._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                INSERT INTO snapshots (
                    aggregate_type, aggregate_id, event_version, state_json,
                    state_hash, schema_version, source_event_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET
                    event_version = excluded.event_version,
                    state_json = excluded.state_json,
                    state_hash = excluded.state_hash,
                    schema_version = excluded.schema_version,
                    source_event_id = excluded.source_event_id,
                    created_at = excluded.created_at
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
                ),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return snapshot

    # Concise alias for clients that use ``save`` as their persistence verb.
    save = save_snapshot

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
                   state_hash, schema_version, source_event_id, created_at
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
            state = _decode_state(row["state_json"])
            # Validate the stored hash before constructing the model.  This
            # ensures malformed rows never expose their payload to callers.
            stored_hash = _validate_sha256_hex(row["state_hash"], "state_hash")
            if not hmac.compare_digest(_sha256_json(state), stored_hash):
                return None
            return Snapshot(
                aggregate_type=row["aggregate_type"],
                aggregate_id=row["aggregate_id"],
                event_version=row["event_version"],
                state=state,
                state_hash=stored_hash,
                schema_version=row["schema_version"],
                source_event_id=row["source_event_id"],
                created_at=datetime.fromisoformat(row["created_at"]),
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


__all__ = ["Snapshot", "SnapshotRecord", "SnapshotStore"]
