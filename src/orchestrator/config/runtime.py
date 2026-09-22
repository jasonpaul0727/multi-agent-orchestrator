"""Atomic configuration reload and immutable per-Run configuration snapshots."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
import threading
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.config.effective import FrozenDict, ResolvedConfig
from orchestrator.config.models import ModelRegistryManifest
from orchestrator.persistence.events import EventDraft, StoredEvent
from orchestrator.persistence.snapshots import SnapshotStore
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore


_CONTENT_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_RUN_STREAM = "run"
_RUN_CREATED = "RunCreated"
_RUN_CONFIG_AGGREGATE = "run_config"


class ConfigCandidate(BaseModel):
    """A fully resolved and validated config/registry pair ready to activate."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    resolved: ResolvedConfig
    registry: ModelRegistryManifest
    source_details: FrozenDict[StrictStr, StrictStr] = Field(default_factory=FrozenDict)

    @model_validator(mode="after")
    def validate_registry_binding(self) -> "ConfigCandidate":
        if self.resolved.config.registry_manifest_ref != self.registry.content_hash:
            raise ValueError("effective config does not reference the candidate registry")
        return self


class RunConfigSnapshot(BaseModel):
    """Self-contained, immutable configuration state captured when a Run starts."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    schema_version: Literal[1] = 1
    run_id: StrictStr = Field(min_length=1)
    resolved_config: ResolvedConfig
    effective_config_hash: StrictStr = Field(min_length=71, max_length=71)
    registry_manifest: ModelRegistryManifest
    registry_manifest_hash: StrictStr = Field(min_length=71, max_length=71)
    source_details: FrozenDict[StrictStr, StrictStr] = Field(default_factory=FrozenDict)
    manager_generation: StrictInt = Field(gt=0)
    captured_at: StrictStr = Field(min_length=1)

    @field_validator("run_id", "captured_at")
    @classmethod
    def validate_nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must not be blank")
        return value

    @field_validator("effective_config_hash", "registry_manifest_hash")
    @classmethod
    def validate_hash_format(cls, value: str) -> str:
        if not _CONTENT_HASH.fullmatch(value):
            raise ValueError("hash must be a sha256 content hash")
        return value

    @model_validator(mode="after")
    def validate_snapshot_hashes(self) -> "RunConfigSnapshot":
        if self.effective_config_hash != self.resolved_config.config.content_hash:
            raise ValueError("effective_config_hash does not match the frozen config")
        if self.registry_manifest_hash != self.registry_manifest.content_hash:
            raise ValueError("registry_manifest_hash does not match the frozen registry")
        if self.resolved_config.config.registry_manifest_ref != self.registry_manifest_hash:
            raise ValueError("frozen config references a different registry")
        return self


@dataclass(frozen=True)
class ConfigReloadResult:
    """Sanitized result of an atomic candidate reload attempt."""

    applied: bool
    generation: int
    effective_config_hash: str
    registry_manifest_hash: str
    error_code: str | None = None


class RunConfigSnapshotError(RuntimeError):
    """Raised when a Run event stream cannot supply a valid config snapshot."""


class ConfigManager:
    """Manage a validated config pointer and freeze it at the Run start boundary.

    Reload candidates are completely built before the active pointer is changed.
    A rejected candidate leaves both the active object and generation untouched.
    Run creation holds the same short-lived state lock through durable event and
    checkpoint persistence, so a concurrent reload is ordered wholly before or
    after that Run's immutable snapshot.
    """

    def __init__(self, initial: ConfigCandidate) -> None:
        if not isinstance(initial, ConfigCandidate):
            raise TypeError("initial must be a validated ConfigCandidate")
        self._active = initial
        self._generation = 1
        self._state_lock = threading.RLock()
        self._reload_lock = threading.Lock()

    @property
    def active(self) -> ConfigCandidate:
        with self._state_lock:
            return self._active

    @property
    def generation(self) -> int:
        with self._state_lock:
            return self._generation

    def reload(self, candidate_loader: Callable[[], ConfigCandidate]) -> ConfigReloadResult:
        """Build/validate then atomically activate one candidate.

        The reload lock serializes builders as well as swaps, preventing a slow
        earlier reload from overwriting a later candidate after it completes.
        Exceptions are represented by a generic error code; exception text may
        contain user configuration and is deliberately not returned.
        """

        if not callable(candidate_loader):
            raise TypeError("candidate_loader must be callable")
        with self._reload_lock:
            try:
                candidate = candidate_loader()
                if not isinstance(candidate, ConfigCandidate):
                    raise TypeError("candidate loader did not return ConfigCandidate")
            except Exception:
                return self._reload_result(applied=False, error_code="candidate_rejected")

            with self._state_lock:
                self._active = candidate
                self._generation += 1
                return self._reload_result(applied=True)

    def start_run(
        self,
        run_id: str,
        event_store: SQLiteEventStore,
        snapshot_store: SnapshotStore | None = None,
    ) -> RunConfigSnapshot:
        """Persist or return the original RunCreated config snapshot.

        The append-only RunCreated event is the source of truth. The separately
        validated SnapshotStore checkpoint accelerates later restart recovery;
        a crash between those writes is repaired by replaying the event on retry.
        """

        if not isinstance(event_store, SQLiteEventStore):
            raise TypeError("event_store must be a SQLiteEventStore")
        if snapshot_store is None:
            snapshot_store = SnapshotStore(event_store)
        with self._state_lock:
            if self._generation < 1:
                raise RuntimeError("configuration manager has no active generation")
            candidate = self._active
            now = datetime.now(timezone.utc)
            proposed = RunConfigSnapshot(
                run_id=run_id,
                resolved_config=candidate.resolved,
                effective_config_hash=candidate.resolved.config.content_hash,
                registry_manifest=candidate.registry,
                registry_manifest_hash=candidate.registry.content_hash,
                source_details=candidate.source_details,
                manager_generation=self._generation,
                captured_at=now.isoformat(),
            )

            def decide(events: list[StoredEvent], current_version: int) -> list[EventDraft] | None:
                if events:
                    if events[0].event_type != _RUN_CREATED or events[0].stream_version != 1:
                        raise RunConfigSnapshotError("Run stream does not begin with RunCreated")
                    return None
                if current_version != 0:
                    raise RunConfigSnapshotError("Run stream version has no corresponding event")
                return [
                    EventDraft(
                        _RUN_CREATED,
                        {
                            "run_id": run_id,
                            "config_snapshot": proposed.model_dump(mode="json"),
                        },
                    )
                ]

            event_store.append_checked(
                _RUN_STREAM,
                run_id,
                f"run-created:{run_id}",
                decide,
            )
            events, _ = event_store.read_stream_with_version(_RUN_STREAM, run_id)
            frozen = _snapshot_from_events(run_id, events)
            _ensure_checkpoint(frozen, events[0], snapshot_store)
            return frozen

    def restore_run(
        self,
        run_id: str,
        event_store: SQLiteEventStore,
        snapshot_store: SnapshotStore | None = None,
    ) -> RunConfigSnapshot:
        """Rebuild a Run's original config from its event after a process restart."""

        if not isinstance(event_store, SQLiteEventStore):
            raise TypeError("event_store must be a SQLiteEventStore")
        if snapshot_store is None:
            snapshot_store = SnapshotStore(event_store)
        events, _ = event_store.read_stream_with_version(_RUN_STREAM, run_id)
        frozen = _snapshot_from_events(run_id, events)
        _ensure_checkpoint(frozen, events[0], snapshot_store)
        return frozen

    def _reload_result(self, *, applied: bool, error_code: str | None = None) -> ConfigReloadResult:
        with self._state_lock:
            return ConfigReloadResult(
                applied=applied,
                generation=self._generation,
                effective_config_hash=self._active.resolved.config.content_hash,
                registry_manifest_hash=self._active.registry.content_hash,
                error_code=error_code,
            )


def _snapshot_from_events(run_id: str, events: list[StoredEvent]) -> RunConfigSnapshot:
    if not events or events[0].event_type != _RUN_CREATED or events[0].stream_version != 1:
        raise RunConfigSnapshotError("RunCreated config snapshot is missing")
    payload = events[0].payload
    if payload.get("run_id") != run_id:
        raise RunConfigSnapshotError("RunCreated identity does not match the stream")
    try:
        snapshot_payload = payload["config_snapshot"]
        snapshot = RunConfigSnapshot.model_validate_json(json.dumps(snapshot_payload))
    except (KeyError, TypeError, ValueError) as exc:
        raise RunConfigSnapshotError("RunCreated config snapshot failed validation") from exc
    if snapshot.run_id != run_id:
        raise RunConfigSnapshotError("RunCreated config snapshot has a different Run identity")
    return snapshot


def _ensure_checkpoint(
    snapshot: RunConfigSnapshot,
    source_event: StoredEvent,
    snapshot_store: SnapshotStore,
) -> None:
    state = snapshot.model_dump(mode="json")
    checkpoint = snapshot_store.load_valid(
        _RUN_CONFIG_AGGREGATE,
        snapshot.run_id,
        expected_schema_version=1,
        expected_event_version=1,
        expected_source_event_id=source_event.event_id,
    )
    if checkpoint is not None:
        if checkpoint.state != state:
            raise RunConfigSnapshotError("config checkpoint disagrees with RunCreated")
        return
    snapshot_store.save_snapshot(
        _RUN_CONFIG_AGGREGATE,
        snapshot.run_id,
        1,
        state,
        schema_version=1,
        source_event_id=source_event.event_id,
    )
