"""Private, content-addressed storage for orchestration artifacts.

Artifact bytes are kept outside the event payload.  The event store receives
small, validated publication metadata only after bytes have been fsync'd and
atomically installed in the artifact directory.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
import hashlib
import hmac
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
import threading
from typing import Any, Protocol

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
    model_validator,
)

from orchestrator.identifiers import new_id
from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.sqlite_event_store import (
    EventIntegrityError,
    SQLiteEventStore,
    StaleStream,
)


_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}\Z", re.ASCII)
_DEFAULT_MEDIA_TYPE = "application/octet-stream"
_PUBLICATION_LOCK = threading.RLock()


class ArtifactError(RuntimeError):
    """Base class for typed artifact-store failures."""


class ArtifactFilesystemError(ArtifactError):
    """Raised when artifact-directory I/O cannot complete safely."""


class ArtifactMetadataError(ArtifactError):
    """Raised when publication metadata cannot be durably recorded."""


class ArtifactIntegrityError(ArtifactError):
    """Raised when bytes or authenticated metadata fail validation."""

    def __init__(self, digest: str, message: str = "artifact integrity check failed") -> None:
        self.digest = digest
        super().__init__(f"{message}: {digest}")


class ArtifactNotFound(ArtifactError, FileNotFoundError):
    """Raised when an artifact or its metadata is not present."""

    def __init__(self, digest: str) -> None:
        self.digest = digest
        # Do not include the private filesystem path in the error.
        super().__init__(f"artifact not found: {digest}")


class ArtifactAccessDenied(ArtifactError, PermissionError):
    """Raised when a caller's authenticated grant cannot read an artifact."""

    def __init__(self, digest: str) -> None:
        self.digest = digest
        super().__init__(f"artifact access denied: {digest}")


class _EventStore(Protocol):
    """Narrow EventStore capability needed by ArtifactStore."""

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
    if value != value.strip():
        raise ValueError(f"{field_name} must not have leading or trailing whitespace")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError(f"{field_name} must not contain lone surrogate characters")
    if any(character in "\r\n\x00" for character in value):
        raise ValueError(f"{field_name} must not contain control characters")
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


def _normalize_labels(
    value: Any,
    field_name: str,
    *,
    none_is_wildcard: bool,
    require_nonempty: bool = False,
) -> tuple[str, ...]:
    if value is None:
        values: tuple[Any, ...] = ("*",) if none_is_wildcard else ()
    elif isinstance(value, str):
        values = (value,)
    elif isinstance(value, Mapping):
        # Mapping scopes are accepted as a convenience for control-plane
        # callers; both ``run-1`` and ``run_id:run-1`` match.
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
    normalized = tuple(_validate_text(item, field_name) for item in values)
    if require_nonempty and not normalized:
        raise ValueError(f"{field_name} must contain at least one scope")
    return normalized


def _validate_scope_labels(value: Any) -> tuple[str, ...]:
    return _normalize_labels(
        value,
        "readable_scope",
        none_is_wildcard=True,
        require_nonempty=True,
    )


def _validate_grant_scope(value: Any) -> tuple[str, ...]:
    return _normalize_labels(
        value,
        "scope",
        none_is_wildcard=False,
        require_nonempty=True,
    )


def _validate_references(value: Any) -> tuple[str, ...]:
    return _normalize_labels(value, "references", none_is_wildcard=False)


class _FrozenDict(dict[str, str | None]):
    """Dict-compatible metadata that cannot be mutated in place."""

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


class ArtifactAccessGrant(BaseModel):
    """A control-plane-issued, authenticated capability to read an artifact.

    The signature is intentionally opaque to this module.  The trusted
    control plane supplies a verifier to :class:`ArtifactStore`; callers
    cannot authorize themselves by passing an arbitrary scope label.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    digest: StrictStr = Field(min_length=71, max_length=71)
    scope: tuple[StrictStr, ...]
    expires_at: datetime = Field(validation_alias=AliasChoices("expires_at", "expires"))
    issuer: StrictStr = Field(min_length=1)
    signature: StrictStr = Field(min_length=1)

    @field_validator("digest", mode="before")
    @classmethod
    def validate_digest(cls, value: Any) -> str:
        return _validate_digest(value)

    @field_validator("scope", mode="before")
    @classmethod
    def validate_scope(cls, value: Any) -> tuple[str, ...]:
        return _validate_grant_scope(value)

    @field_validator("issuer", "signature", mode="before")
    @classmethod
    def validate_strings(cls, value: Any, info: Any) -> str:
        return _validate_text(value, info.field_name)

    @field_validator("expires_at")
    @classmethod
    def validate_expiry_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")
        return value


class ArtifactRecord(BaseModel):
    """Immutable metadata for one artifact publication."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    digest: StrictStr = Field(min_length=71, max_length=71)
    artifact_type: StrictStr = Field(
        default="artifact",
        min_length=1,
        validation_alias=AliasChoices("artifact_type", "type"),
    )
    size: StrictInt = Field(default=0, ge=0)
    media_type: StrictStr = Field(default=_DEFAULT_MEDIA_TYPE, min_length=1)
    source: dict[str, str | None] = Field(default_factory=dict)
    schema_version: StrictInt = Field(default=1, gt=0)
    redaction_state: StrictStr = Field(default="unknown", min_length=1)
    readable_scope: tuple[StrictStr, ...] = ("*",)
    references: tuple[StrictStr, ...] = ()
    lifecycle_state: StrictStr = Field(default="temporary", min_length=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    publication_id: StrictStr = Field(default_factory=new_id, min_length=1)

    @field_validator("digest", mode="before")
    @classmethod
    def validate_digest(cls, value: Any) -> str:
        return _validate_digest(value)

    @field_validator(
        "artifact_type",
        "media_type",
        "redaction_state",
        "lifecycle_state",
        "publication_id",
        mode="before",
    )
    @classmethod
    def validate_metadata_strings(cls, value: Any, info: Any) -> str:
        return _validate_text(value, info.field_name)

    @field_validator("source", mode="before")
    @classmethod
    def validate_source(cls, value: Any) -> dict[str, str | None]:
        return _normalize_source(value)

    @field_validator("readable_scope", mode="before")
    @classmethod
    def validate_readable_scope(cls, value: Any) -> tuple[str, ...]:
        return _validate_scope_labels(value)

    @field_validator("references", mode="before")
    @classmethod
    def validate_reference_labels(cls, value: Any) -> tuple[str, ...]:
        return _validate_references(value)

    @field_validator("created_at")
    @classmethod
    def validate_created_at_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value

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
        publication_id: str | None = None,
    ) -> "ArtifactRecord":
        try:
            kwargs: dict[str, Any] = {
                "digest": _validate_digest(digest),
                "artifact_type": _validate_text(artifact_type, "artifact_type"),
                "size": size,
                "media_type": _validate_text(media_type, "media_type"),
                "source": _normalize_source(source),
                "schema_version": schema_version,
                "redaction_state": _validate_text(redaction_state, "redaction_state"),
                "readable_scope": _validate_scope_labels(readable_scope),
                "references": _validate_references(references),
                "lifecycle_state": _validate_text(lifecycle_state, "lifecycle_state"),
                "created_at": created_at or datetime.now(timezone.utc),
            }
            if publication_id is not None:
                kwargs["publication_id"] = publication_id
            return cls(**kwargs)
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError(f"invalid artifact metadata: {exc}") from exc

    @property
    def type(self) -> str:
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
            "publication_id": self.publication_id,
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

    ``event_store`` and ``grant_verifier`` are capabilities supplied by the
    trusted control plane.  No database handle or control-directory path is
    exposed to a worker.  The standalone constructor creates an internal
    private metadata store, but reads still require a configured verifier and
    a signed :class:`ArtifactAccessGrant`.
    """

    def __init__(
        self,
        root: str | Path,
        event_store: _EventStore | None = None,
        grant_verifier: Callable[[ArtifactAccessGrant], bool] | Any | None = None,
        *,
        verifier: Callable[[ArtifactAccessGrant], bool] | Any | None = None,
        access_grant_verifier: Callable[[ArtifactAccessGrant], bool] | Any | None = None,
        access_verifier: Callable[[ArtifactAccessGrant], bool] | Any | None = None,
    ) -> None:
        canonical_root = self._canonicalize_root(root)
        try:
            canonical_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError as exc:
            raise ArtifactFilesystemError("unable to create private artifact directory") from exc
        if not canonical_root.is_dir() or canonical_root.is_symlink():
            raise ArtifactFilesystemError("artifact root is not a private directory")
        try:
            os.chmod(canonical_root, 0o700)
        except OSError as exc:
            raise ArtifactFilesystemError("unable to secure private artifact directory") from exc
        self._root = canonical_root

        verifiers = [
            item
            for item in (grant_verifier, verifier, access_grant_verifier, access_verifier)
            if item is not None
        ]
        if len(verifiers) > 1:
            raise TypeError("provide only one grant verifier")
        if isinstance(event_store, (str, Path, sqlite3.Connection)):
            raise TypeError("event_store must be a capability, not a database handle or path")
        self._grant_verifier = verifiers[0] if verifiers else None
        self._owned_event_store = event_store is None
        if event_store is None:
            metadata_db = self._root / ".metadata.db"
            try:
                self._event_store: _EventStore = SQLiteEventStore(metadata_db)
                os.chmod(metadata_db, 0o600)
            except OSError as exc:
                raise ArtifactFilesystemError("unable to initialize artifact metadata store") from exc
        else:
            self._event_store = event_store
        self._records: dict[str, list[ArtifactRecord]] = {}

    @staticmethod
    def _canonicalize_root(root: str | Path) -> Path:
        if not isinstance(root, (str, Path)):
            raise TypeError("root must be an explicit absolute directory path")
        raw_root = os.fspath(root)
        if isinstance(raw_root, bytes):
            raise TypeError("root must be an explicit absolute directory path")
        if not raw_root.strip():
            raise ValueError("artifact root must be an explicit absolute directory path")
        # Path() removes dot components before validation, so inspect the raw
        # spelling as well.  This prevents an apparently canonical path from
        # concealing traversal or an accidental cwd selection.
        if any(component in {".", ".."} for component in re.split(r"[\\/]", raw_root)):
            raise ValueError("artifact root must not contain . or .. components")
        candidate = Path(raw_root)
        if not candidate.is_absolute():
            raise ValueError("artifact root must be an absolute path")
        # Inspect the spelling before resolve(); otherwise resolve() would
        # erase the evidence that a caller supplied a symlink component.
        current_candidate = Path(candidate.anchor)
        for component in candidate.parts[1:]:
            current_candidate /= component
            if current_candidate.is_symlink():
                raise ValueError("artifact root must not contain symlink components")
        try:
            canonical = candidate.resolve(strict=False)
        except OSError as exc:
            raise ArtifactFilesystemError("unable to canonicalize artifact root") from exc
        if canonical == Path(canonical.anchor):
            raise ValueError("artifact root must be a dedicated private directory")
        if canonical in {
            Path("/tmp"),
            Path("/var"),
            Path("/home"),
            Path("/root"),
            Path("/usr"),
            Path("/etc"),
            Path("/opt"),
        }:
            raise ValueError("artifact root must be a dedicated private directory")
        cwd = Path.cwd().resolve()
        # A process cwd or any of its ancestors is a broad workspace location.
        # A repository root is also rejected if cwd has changed into a child.
        if canonical == cwd or canonical in cwd.parents:
            raise ValueError("artifact root must not be the cwd or a workspace ancestor")
        if (canonical / ".git").exists():
            raise ValueError("artifact root must not be a workspace or repository root")
        # Never follow a symlink in an existing component.  This check happens
        # before mkdir/chmod and also covers a symlink that resolves to cwd.
        current = Path(canonical.anchor)
        for component in canonical.parts[1:]:
            current /= component
            if current.is_symlink():
                raise ValueError("artifact root must not contain symlink components")
        return canonical

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
        """Write bytes privately, atomically publish, then append provenance."""

        if not isinstance(content, (bytes, bytearray, memoryview)):
            raise TypeError("content must be bytes-like")
        payload = bytes(content)
        if type is not None:
            artifact_type = type

        temp_path: Path | None = None
        installed_new = False
        installed_identity: tuple[int, int] | None = None
        digest: str | None = None
        try:
            try:
                fd, temp_name = tempfile.mkstemp(prefix=".tmp-", dir=self._root)
                temp_path = Path(temp_name)
                os.chmod(temp_path, 0o600)
                hasher = hashlib.sha256()
                with os.fdopen(fd, "wb") as staged:
                    for offset in range(0, len(payload), 1024 * 1024):
                        chunk = payload[offset : offset + 1024 * 1024]
                        staged.write(chunk)
                        hasher.update(chunk)
                    staged.flush()
                    os.fsync(staged.fileno())
                digest = f"sha256:{hasher.hexdigest()}"
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
            except ArtifactError:
                raise
            except (ValueError, TypeError):
                # Metadata is a caller-boundary error, not a disk failure.
                # The finally block removes the private staging file.
                raise
            except OSError as exc:
                raise ArtifactFilesystemError("unable to stage artifact bytes") from exc

            assert digest is not None
            final_path = self._path_for_digest(digest)
            with _PUBLICATION_LOCK:
                try:
                    if final_path.exists() or final_path.is_symlink():
                        self._verify_file(final_path, digest)
                        temp_path.unlink(missing_ok=True)
                        temp_path = None
                    else:
                        try:
                            staged_stat = temp_path.stat()
                            installed_identity = (staged_stat.st_dev, staged_stat.st_ino)
                            os.replace(temp_path, final_path)
                        except OSError as exc:
                            raise ArtifactFilesystemError("unable to atomically publish artifact") from exc
                        temp_path = None
                        installed_new = True
                        self._fsync_directory()
                    return self._record_metadata(candidate)
                except ArtifactError:
                    if installed_new:
                        self._remove_new_object(final_path, digest, installed_identity)
                    raise
                except OSError as exc:
                    if installed_new:
                        self._remove_new_object(final_path, digest, installed_identity)
                    raise ArtifactFilesystemError("artifact publication filesystem failure") from exc
                except Exception as exc:
                    if installed_new:
                        self._remove_new_object(final_path, digest, installed_identity)
                    raise ArtifactMetadataError("unable to record artifact publication") from exc
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def read_bytes(
        self,
        digest: str,
        grant: ArtifactAccessGrant | None = None,
        *,
        access_grant: ArtifactAccessGrant | None = None,
        caller_scope: Any = None,
        scope: Any = None,
    ) -> bytes:
        """Authorize with a signed grant, then re-hash bytes before returning."""

        digest = _validate_digest(digest)
        if caller_scope is not None or scope is not None:
            raise TypeError("caller_scope/scope are deprecated; use an authenticated grant")
        if grant is not None and access_grant is not None:
            raise TypeError("grant and access_grant are mutually exclusive")
        if grant is None:
            grant = access_grant
        path = self._path_for_digest(digest)
        if path.is_symlink():
            raise ArtifactIntegrityError(digest, "artifact path must not be a symlink")
        if not path.exists():
            raise ArtifactNotFound(digest)
        publications = self._load_records(digest)
        if not publications:
            raise ArtifactNotFound(digest)
        self._authorize(digest, grant, publications)
        content = self._read_and_verify(path, digest)
        if any(len(content) != record.size for record in publications):
            raise ArtifactIntegrityError(digest, "artifact metadata size mismatch")
        return content

    def get_record(self, digest: str) -> ArtifactRecord:
        """Return the latest publication only after re-hashing its bytes."""

        digest = _validate_digest(digest)
        path = self._path_for_digest(digest)
        if path.is_symlink():
            raise ArtifactIntegrityError(digest, "artifact path must not be a symlink")
        if not path.exists():
            raise ArtifactNotFound(digest)
        publications = self._load_records(digest)
        if not publications:
            raise ArtifactNotFound(digest)
        content = self._read_and_verify(path, digest)
        latest = publications[-1]
        if any(len(content) != record.size for record in publications):
            raise ArtifactIntegrityError(digest, "artifact metadata size mismatch")
        return latest

    def list_publications(self, digest: str) -> list[ArtifactRecord]:
        """List all authenticated provenance publications for one digest."""

        digest = _validate_digest(digest)
        path = self._path_for_digest(digest)
        if path.is_symlink():
            raise ArtifactIntegrityError(digest, "artifact path must not be a symlink")
        if not path.exists():
            raise ArtifactNotFound(digest)
        publications = self._load_records(digest)
        if not publications:
            raise ArtifactNotFound(digest)
        content = self._read_and_verify(path, digest)
        if any(len(content) != record.size for record in publications):
            raise ArtifactIntegrityError(digest, "artifact metadata size mismatch")
        return publications

    def get_records(self, digest: str | None = None) -> list[ArtifactRecord]:
        """Return publications for one digest, or all discoverable artifacts."""

        if digest is not None:
            return self.list_publications(digest)
        try:
            children = list(self._root.iterdir())
        except OSError as exc:
            raise ArtifactFilesystemError("unable to enumerate artifact directory") from exc
        records: list[ArtifactRecord] = []
        for child in children:
            if child.name.startswith(".") or not child.is_file():
                continue
            candidate = f"sha256:{child.name}"
            if _DIGEST_RE.fullmatch(candidate) is None:
                continue
            try:
                records.extend(self.list_publications(candidate))
            except ArtifactNotFound:
                continue
        return records

    def _record_metadata(self, record: ArtifactRecord) -> ArtifactRecord:
        """Append a unique provenance event; never use digest as idempotency key."""

        key = f"artifact-publication:{record.publication_id}"
        payload = record.to_event_payload()
        try:
            for _ in range(8):
                expected = self._event_store.current_version("artifact", record.digest)
                try:
                    stored_events = self._event_store.append(
                        "artifact",
                        record.digest,
                        expected,
                        [EventDraft("ArtifactPublished", payload)],
                        key,
                    )
                    if stored_events:
                        loaded = self._record_from_event(stored_events[0], record.digest)
                        self._records.setdefault(record.digest, []).append(loaded)
                        return loaded
                    self._records.setdefault(record.digest, []).append(record)
                    return record
                except StaleStream:
                    continue
            raise ArtifactMetadataError("concurrent artifact publication did not converge")
        except ArtifactMetadataError:
            raise
        except Exception as exc:
            raise ArtifactMetadataError("unable to record artifact publication") from exc

    def _load_records(self, digest: str) -> list[ArtifactRecord]:
        try:
            events = self._event_store.read_stream("artifact", digest)
        except AttributeError:
            return list(self._records.get(digest, ()))
        except EventIntegrityError as exc:
            raise ArtifactIntegrityError(digest, "metadata event integrity failure") from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactIntegrityError(digest, "metadata event could not be read") from exc
        publications: list[ArtifactRecord] = []
        for event in events:
            if getattr(event, "event_type", None) == "ArtifactPublished":
                publications.append(self._record_from_event(event, digest))
        if publications:
            self._records[digest] = publications
        return publications

    @staticmethod
    def _record_from_event(event: Any, digest: str) -> ArtifactRecord:
        payload = getattr(event, "payload", None)
        if not isinstance(payload, Mapping):
            raise ArtifactIntegrityError(digest, "metadata event payload is not an object")
        if payload.get("digest") != digest:
            raise ArtifactIntegrityError(digest, "metadata event digest mismatch")
        try:
            created_at = datetime.fromisoformat(payload["created_at"])
            publication_id = payload.get("publication_id") or getattr(event, "event_id", None)
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
                publication_id=publication_id,
            )
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise ArtifactIntegrityError(digest, "metadata event fields are invalid") from exc

    def _authorize(
        self,
        digest: str,
        grant: ArtifactAccessGrant | None,
        publications: list[ArtifactRecord],
    ) -> None:
        if grant is None or not isinstance(grant, ArtifactAccessGrant):
            raise ArtifactAccessDenied(digest)
        if grant.digest != digest:
            raise ArtifactAccessDenied(digest)
        now = datetime.now(timezone.utc)
        if grant.expires_at <= now:
            raise ArtifactAccessDenied(digest)
        verifier = self._grant_verifier
        try:
            if verifier is None:
                verified = False
            elif callable(verifier):
                verified = bool(verifier(grant))
            else:
                verify = getattr(verifier, "verify", None)
                verified = bool(verify(grant)) if callable(verify) else False
        except Exception:
            verified = False
        if not verified:
            raise ArtifactAccessDenied(digest)
        if not any(self._scope_allows(record.readable_scope, grant.scope) for record in publications):
            raise ArtifactAccessDenied(digest)

    @staticmethod
    def _scope_allows(allowed: tuple[str, ...], requested: tuple[str, ...]) -> bool:
        return "*" in allowed or "*" in requested or bool(set(allowed).intersection(requested))

    def _path_for_digest(self, digest: str) -> Path:
        digest = _validate_digest(digest)
        path = self._root / digest.removeprefix("sha256:")
        try:
            path.relative_to(self._root)
        except ValueError as exc:
            raise ValueError("digest resolves outside artifact root") from exc
        return path

    @staticmethod
    def _read_and_verify(path: Path, digest: str) -> bytes:
        try:
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
            raise ArtifactFilesystemError("unable to read artifact") from exc
        actual = hashlib.sha256(content).hexdigest()
        expected = digest.removeprefix("sha256:")
        if not hmac.compare_digest(actual, expected):
            raise ArtifactIntegrityError(digest)
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
        except OSError as exc:
            raise ArtifactFilesystemError("unable to open artifact directory for sync") from exc
        try:
            try:
                os.fsync(directory_fd)
            except OSError as exc:
                raise ArtifactFilesystemError("unable to fsync artifact directory") from exc
        finally:
            os.close(directory_fd)

    def _remove_new_object(
        self,
        path: Path,
        digest: str,
        identity: tuple[int, int] | None,
    ) -> None:
        if identity is None:
            return
        try:
            current = path.lstat()
            if (current.st_dev, current.st_ino) != identity or not stat.S_ISREG(current.st_mode):
                return
            path.unlink()
            self._fsync_directory()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise ArtifactFilesystemError("unable to remove failed artifact publication") from exc

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
    "ArtifactAccessGrant",
    "ArtifactError",
    "ArtifactFilesystemError",
    "ArtifactIntegrityError",
    "ArtifactMetadataError",
    "ArtifactNotFound",
    "ArtifactRecord",
    "ArtifactStore",
]
