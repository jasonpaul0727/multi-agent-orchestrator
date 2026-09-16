"""Private, content-addressed storage for orchestration artifacts.

Artifact bytes are deliberately kept outside the event payload.  The event
store receives a small, validated metadata event only after the bytes have
been fsync'd and atomically installed in the artifact directory.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
import hashlib
import hmac
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    model_validator,
)

from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.sqlite_event_store import (
    EventIntegrityError,
    SQLiteEventStore,
    StaleStream,
)


_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}\Z", re.ASCII)
_DEFAULT_MEDIA_TYPE = "application/octet-stream"


class ArtifactError(RuntimeError):
    """Base class for typed artifact-store failures."""


class ArtifactIntegrityError(ArtifactError):
    """Raised when bytes or metadata do not match their authenticated hash."""

    def __init__(self, digest: str, message: str = "artifact integrity check failed") -> None:
        self.digest = digest
        super().__init__(f"{message}: {digest}")


class ArtifactNotFound(ArtifactError, FileNotFoundError):
    """Raised when an artifact or its metadata is not present."""

    def __init__(self, digest: str) -> None:
        self.digest = digest
        # Do not include the private filesystem path in the error.  Callers
        # should only need the content address.
        super().__init__(f"artifact not found: {digest}")


class ArtifactAccessDenied(ArtifactError, PermissionError):
    """Raised when a caller's scope is not allowed to read an artifact."""

    def __init__(self, digest: str) -> None:
        self.digest = digest
        super().__init__(f"artifact access denied: {digest}")


class _EventStore(Protocol):
    """The narrow EventStore surface needed by ArtifactStore.

    The protocol is intentional: a worker gets this capability, not a SQLite
    connection or a control-directory path.
    """

    def append(
        self,
        stream_type: str,
        stream_id: str,
        expected_version: int,
        events: Iterable[EventDraft],
        idempotency_key: str,
    ) -> list[Any]: ...

    def current_version(self, stream_type: str, stream_id: str) -> int: ...

    def read_stream(self, stream_type: str, stream_id: str) -> list[Any]: ...


def _validate_digest(value: Any) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ValueError("digest must be sha256:<64 lowercase hexadecimal characters>")
    return value


def _validate_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError(f"{field_name} must not contain lone surrogate characters")
    return value


def _normalize_source(value: Mapping[str, Any] | None) -> dict[str, str | None]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("source must be a mapping")
    normalized: dict[str, str | None] = {}
    for key, item in value.items():
        key = _validate_text(key, "source key")
        if item is not None:
            item = _validate_text(item, f"source[{key!r}]")
        normalized[key] = item
    return normalized


def _normalize_labels(value: Any, field_name: str, *, none_is_wildcard: bool) -> tuple[str, ...]:
    if value is None:
        return ("*",) if none_is_wildcard else ()
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, Mapping):
        # Mapping scopes are accepted as a convenience for gateway callers;
        # both ``run-1`` and ``run_id:run-1`` are useful matching labels.
        values_list: list[str] = []
        for key, item in value.items():
            key = _validate_text(key, f"{field_name} key")
            if isinstance(item, str):
                item = _validate_text(item, f"{field_name}[{key!r}]")
                values_list.extend((item, f"{key}:{item}"))
            elif isinstance(item, Iterable) and not isinstance(item, (bytes, bytearray)):
                for nested in item:
                    nested = _validate_text(nested, f"{field_name}[{key!r}]")
                    values_list.extend((nested, f"{key}:{nested}"))
            else:
                raise TypeError(f"{field_name} values must be strings or iterables of strings")
        values = tuple(values_list)
    else:
        if isinstance(value, (bytes, bytearray)):
            raise TypeError(f"{field_name} must be a string or iterable of strings")
        try:
            values = tuple(value)
        except TypeError as exc:
            raise TypeError(f"{field_name} must be a string or iterable of strings") from exc
    return tuple(_validate_text(item, field_name) for item in values)


def _validate_scope_labels(value: Any) -> tuple[str, ...]:
    return _normalize_labels(value, "readable_scope", none_is_wildcard=True)


def _validate_references(value: Any) -> tuple[str, ...]:
    return _normalize_labels(value, "references", none_is_wildcard=False)


class _FrozenDict(dict[str, str | None]):
    """Dict-compatible source metadata that cannot be mutated in place."""

    @staticmethod
    def _immutable(*args: Any, **kwargs: Any) -> None:
        raise TypeError("artifact metadata is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class ArtifactRecord(BaseModel):
    """Immutable metadata for a published content-addressed artifact."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    digest: StrictStr = Field(min_length=71, max_length=71)
    artifact_type: StrictStr = Field(min_length=1)
    size: StrictInt = Field(ge=0)
    media_type: StrictStr = Field(min_length=1)
    source: dict[str, str | None]
    schema_version: StrictInt = Field(gt=0)
    redaction_state: StrictStr = Field(min_length=1)
    readable_scope: tuple[StrictStr, ...]
    references: tuple[StrictStr, ...]
    lifecycle_state: StrictStr = Field(min_length=1)
    created_at: datetime

    @model_validator(mode="after")
    def freeze_metadata(self) -> "ArtifactRecord":
        object.__setattr__(self, "source", _FrozenDict(self.source))
        return self

    @classmethod
    def from_metadata(
        cls,
        digest: str,
        *,
        size: int,
        artifact_type: str,
        media_type: str,
        source: Mapping[str, Any] | None,
        schema_version: int,
        redaction_state: str,
        readable_scope: Any,
        references: Any,
        lifecycle_state: str,
        created_at: datetime | None = None,
    ) -> "ArtifactRecord":
        try:
            return cls(
                digest=_validate_digest(digest),
                artifact_type=_validate_text(artifact_type, "artifact_type"),
                size=size,
                media_type=_validate_text(media_type, "media_type"),
                source=_normalize_source(source),
                schema_version=schema_version,
                redaction_state=_validate_text(redaction_state, "redaction_state"),
                readable_scope=_validate_scope_labels(readable_scope),
                references=_validate_references(references),
                lifecycle_state=_validate_text(lifecycle_state, "lifecycle_state"),
                created_at=created_at or datetime.now(timezone.utc),
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError(f"invalid artifact metadata: {exc}") from exc

    @property
    def type(self) -> str:
        """Compatibility spelling for the metadata field named ``type``."""

        return self.artifact_type

    @property
    def content_hash(self) -> str:
        return self.digest

    @property
    def source_run_id(self) -> str | None:
        return self.source.get("run_id")

    @property
    def source_node_id(self) -> str | None:
        return self.source.get("node_id")

    @property
    def source_attempt_id(self) -> str | None:
        return self.source.get("attempt_id")

    @property
    def source_tool(self) -> str | None:
        return self.source.get("tool")

    @property
    def source_model(self) -> str | None:
        return self.source.get("model")

    def to_event_payload(self) -> dict[str, Any]:
        return {
            "digest": self.digest,
            "type": self.artifact_type,
            "size": self.size,
            "media_type": self.media_type,
            "source": dict(self.source),
            "schema_version": self.schema_version,
            "redaction_state": self.redaction_state,
            "readable_scope": list(self.readable_scope),
            "references": list(self.references),
            "lifecycle_state": self.lifecycle_state,
            "created_at": self.created_at.isoformat(),
        }


class ArtifactStore:
    """Private content-addressed artifact storage.

    ``event_store`` is an injected capability rather than a database handle.
    For the standalone convenience constructor an internal private SQLite
    metadata store is created beneath ``root``; it is never exposed to the
    caller or to workers.
    """

    def __init__(self, root: str | Path, event_store: _EventStore | None = None) -> None:
        if not isinstance(root, (str, Path)):
            raise TypeError("root must be an explicit directory path")
        root_path = Path(root)
        if isinstance(root, str) and not root.strip():
            raise ValueError("artifact root must be an explicit directory path")
        # Refuse ambiguous broad locations.  In particular, ``ArtifactStore("")``
        # must never silently chmod or populate the caller's working tree.
        unresolved_root = root_path.absolute()
        if unresolved_root == Path(unresolved_root.anchor):
            raise ValueError("artifact root must be a dedicated private directory")
        if root_path.exists() and root_path.is_symlink():
            raise ValueError("artifact root must not be a symlink")
        root_path.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not root_path.is_dir():
            raise ValueError("artifact root must be a directory")
        try:
            os.chmod(root_path, 0o700)
        except OSError as exc:
            raise ValueError("artifact root must be a private directory") from exc
        self._root = root_path.resolve()
        self._owned_event_store = event_store is None
        if event_store is None:
            # This private fallback makes ``ArtifactStore(root)`` durable while
            # keeping the normal worker API independent of database paths.
            metadata_db = self._root / ".metadata.db"
            self._event_store: _EventStore = SQLiteEventStore(metadata_db)
            try:
                os.chmod(metadata_db, 0o600)
            except OSError:
                pass
        else:
            self._event_store = event_store
        self._records: dict[str, ArtifactRecord] = {}

    def publish_bytes(
        self,
        content: bytes | bytearray | memoryview,
        *,
        source: Mapping[str, Any] | None = None,
        artifact_type: str = "artifact",
        type: str | None = None,
        media_type: str = _DEFAULT_MEDIA_TYPE,
        schema_version: int = 1,
        redaction_state: str = "unknown",
        readable_scope: Any = None,
        references: Any = None,
        lifecycle_state: str = "temporary",
    ) -> ArtifactRecord:
        """Durably publish bytes and return their immutable metadata record."""

        if not isinstance(content, (bytes, bytearray, memoryview)):
            raise TypeError("content must be bytes-like")
        payload = bytes(content)
        if type is not None:
            artifact_type = type

        temp_path: Path | None = None
        digest: str
        try:
            fd, temp_name = tempfile.mkstemp(prefix=".tmp-", dir=self._root)
            temp_path = Path(temp_name)
            try:
                os.chmod(temp_path, 0o600)
            except OSError:
                pass
            hasher = hashlib.sha256()
            with os.fdopen(fd, "wb") as staged:
                # A single bytes object is accepted at the boundary, but hash
                # and write in chunks so a large result does not duplicate in
                # temporary buffers.
                for offset in range(0, len(payload), 1024 * 1024):
                    chunk = payload[offset : offset + 1024 * 1024]
                    staged.write(chunk)
                    hasher.update(chunk)
                staged.flush()
                os.fsync(staged.fileno())
            digest = f"sha256:{hasher.hexdigest()}"
            final_path = self._path_for_digest(digest)
            if final_path.exists() or final_path.is_symlink():
                self._verify_file(final_path, digest)
                temp_path.unlink(missing_ok=True)
                temp_path = None
            else:
                # ``replace`` is atomic.  A racing publisher writes the exact
                # same digest, so replacing it cannot create mixed content.
                os.replace(temp_path, final_path)
                temp_path = None
                self._fsync_directory()
            candidate = ArtifactRecord.from_metadata(
                digest,
                size=len(payload),
                artifact_type=artifact_type,
                media_type=media_type,
                source=source,
                schema_version=schema_version,
                redaction_state=redaction_state,
                readable_scope=readable_scope,
                references=references,
                lifecycle_state=lifecycle_state,
            )
            existing = self._load_record(digest)
            if existing is not None:
                return existing
            return self._record_metadata(candidate)
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def read_bytes(self, digest: str, *, caller_scope: Any = None, scope: Any = None) -> bytes:
        """Read and hash-verify an artifact after enforcing its read scope."""

        digest = _validate_digest(digest)
        if scope is not None:
            if caller_scope is not None:
                raise TypeError("caller_scope and scope are mutually exclusive")
            caller_scope = scope
        path = self._path_for_digest(digest)
        if not path.exists() or path.is_symlink():
            raise ArtifactNotFound(digest)
        record = self._load_record(digest)
        if record is None:
            raise ArtifactNotFound(digest)
        if not self._scope_allows(record.readable_scope, caller_scope):
            raise ArtifactAccessDenied(digest)
        return self._read_and_verify(path, digest, expected_size=record.size)

    def get_record(self, digest: str) -> ArtifactRecord:
        """Return authenticated metadata for an existing artifact."""

        digest = _validate_digest(digest)
        path = self._path_for_digest(digest)
        if not path.exists() or path.is_symlink():
            raise ArtifactNotFound(digest)
        record = self._load_record(digest)
        if record is None:
            raise ArtifactNotFound(digest)
        return record

    # This helper exists solely for the artifact integrity test.  It is not a
    # production mutation API and intentionally does not expose the path.
    def corrupt_for_test(self, digest: str) -> None:
        """TEST-ONLY: append a byte to a temporary test artifact."""

        digest = _validate_digest(digest)
        path = self._path_for_digest(digest)
        if not path.exists() or path.is_symlink():
            raise ArtifactNotFound(digest)
        try:
            with path.open("ab") as artifact:
                artifact.write(b"\x00")
                artifact.flush()
                os.fsync(artifact.fileno())
        except OSError as exc:
            raise ArtifactIntegrityError(digest, "unable to mutate test artifact") from exc

    def _record_metadata(self, record: ArtifactRecord) -> ArtifactRecord:
        """Append exactly one idempotent metadata event after publication."""

        payload = record.to_event_payload()
        key = f"artifact-published:{record.digest}"
        try:
            expected = self._event_store.current_version("artifact", record.digest)
        except AttributeError as exc:
            raise TypeError("event_store must expose current_version") from exc
        try:
            stored_events = self._event_store.append(
                "artifact",
                record.digest,
                expected,
                [EventDraft("ArtifactPublished", payload)],
                key,
            )
        except StaleStream:
            # Another publisher won the stream CAS.  Its idempotency record is
            # authoritative; read it back rather than appending conflicting
            # metadata.
            existing = self._load_record(record.digest)
            if existing is not None:
                return existing
            raise
        if stored_events:
            loaded = self._record_from_event(stored_events[0], record.digest)
            self._records[record.digest] = loaded
            return loaded
        # A minimal EventStore adapter may not return events.  The metadata
        # was accepted, so retain the validated record for this store instance.
        self._records[record.digest] = record
        return record

    def _load_record(self, digest: str) -> ArtifactRecord | None:
        try:
            events = self._event_store.read_stream("artifact", digest)
        except AttributeError:
            # A tiny in-memory adapter may intentionally omit read_stream;
            # publication still works for the lifetime of this instance.
            return self._records.get(digest)
        except EventIntegrityError as exc:
            raise ArtifactIntegrityError(digest, "metadata event integrity failure") from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactIntegrityError(digest, "metadata event could not be read") from exc
        published = [event for event in events if getattr(event, "event_type", None) == "ArtifactPublished"]
        if not published:
            return None
        record = self._record_from_event(published[0], digest)
        for duplicate in published[1:]:
            repeated = self._record_from_event(duplicate, digest)
            if repeated != record:
                raise ArtifactIntegrityError(digest, "conflicting metadata events")
        self._records[digest] = record
        return record

    @staticmethod
    def _record_from_event(event: Any, digest: str) -> ArtifactRecord:
        payload = getattr(event, "payload", None)
        if not isinstance(payload, Mapping):
            raise ArtifactIntegrityError(digest, "metadata event payload is not an object")
        if payload.get("digest") != digest:
            raise ArtifactIntegrityError(digest, "metadata event digest mismatch")
        try:
            created_at = datetime.fromisoformat(payload["created_at"])
            return ArtifactRecord.from_metadata(
                digest,
                size=payload["size"],
                artifact_type=payload.get("type", payload.get("artifact_type")),
                media_type=payload["media_type"],
                source=payload["source"],
                schema_version=payload["schema_version"],
                redaction_state=payload["redaction_state"],
                readable_scope=payload["readable_scope"],
                references=payload["references"],
                lifecycle_state=payload["lifecycle_state"],
                created_at=created_at,
            )
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise ArtifactIntegrityError(digest, "metadata event fields are invalid") from exc

    def _path_for_digest(self, digest: str) -> Path:
        digest = _validate_digest(digest)
        path = self._root / digest.removeprefix("sha256:")
        # Digest validation makes traversal impossible; keep this check as a
        # defense in depth if the on-disk layout is ever changed.
        try:
            path.relative_to(self._root)
        except ValueError as exc:
            raise ValueError("digest resolves outside artifact root") from exc
        return path

    @staticmethod
    def _scope_allows(allowed: tuple[str, ...], caller_scope: Any) -> bool:
        if caller_scope is None:
            return "*" in allowed
        if "*" in allowed:
            return True
        requested = set(_normalize_labels(caller_scope, "caller_scope", none_is_wildcard=False))
        return bool(requested.intersection(allowed))

    @staticmethod
    def _read_and_verify(
        path: Path,
        digest: str,
        *,
        expected_size: int | None = None,
    ) -> bytes:
        try:
            # lstat prevents a symlink in the private directory from turning
            # an otherwise valid external file into an artifact.
            mode = path.lstat().st_mode
            if not stat.S_ISREG(mode):
                raise ArtifactIntegrityError(digest, "artifact is not a regular file")
            with path.open("rb") as artifact:
                content = artifact.read()
        except ArtifactIntegrityError:
            raise
        except FileNotFoundError as exc:
            raise ArtifactNotFound(digest) from exc
        except OSError as exc:
            raise ArtifactIntegrityError(digest, "artifact could not be read") from exc
        actual = hashlib.sha256(content).hexdigest()
        expected = digest.removeprefix("sha256:")
        if not hmac.compare_digest(actual, expected):
            raise ArtifactIntegrityError(digest)
        if expected_size is not None and len(content) != expected_size:
            raise ArtifactIntegrityError(digest, "artifact metadata size mismatch")
        return content

    @classmethod
    def _verify_file(cls, path: Path, digest: str) -> None:
        cls._read_and_verify(path, digest)

    def _fsync_directory(self) -> None:
        try:
            flags = os.O_RDONLY
            if hasattr(os, "O_DIRECTORY"):
                flags |= os.O_DIRECTORY
            directory_fd = os.open(self._root, flags)
        except OSError:
            return
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def close(self) -> None:
        if self._owned_event_store:
            close = getattr(self._event_store, "close", None)
            if close is not None:
                close()

    def __enter__(self) -> "ArtifactStore":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()


__all__ = [
    "ArtifactAccessDenied",
    "ArtifactError",
    "ArtifactIntegrityError",
    "ArtifactNotFound",
    "ArtifactRecord",
    "ArtifactStore",
]
