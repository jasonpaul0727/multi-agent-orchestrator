"""Versioned, in-memory Worker input commitment data."""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator

from orchestrator.config.effective import RoleName
from orchestrator.validation import revalidate_model


MAX_WORKER_INPUT_FRAME_BYTES = 1_048_576
WORKER_INPUT_OUTPUT_BYTES = 16_384
_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$"


class WorkerInputError(ValueError):
    """A stable, non-sensitive input contract failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class WorkerInputManifest(BaseModel):
    """Exact caller text and host-selected identities, without execution authority."""

    model_config = ConfigDict(
        frozen=True, strict=True, extra="forbid", hide_input_in_errors=True,
        populate_by_name=True,
    )

    schema_id: Literal["maestro.worker-input.v1"] = Field(
        default="maestro.worker-input.v1", alias="schema"
    )
    run_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER_PATTERN)
    node_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER_PATTERN)
    role: RoleName
    task_text: StrictStr = Field(min_length=1, max_length=100_000, repr=False)
    input_artifacts: tuple[()] = ()

    @field_validator("task_text")
    @classmethod
    def nonblank_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("task_text must not be blank")
        return value

    @field_validator("input_artifacts", mode="before")
    @classmethod
    def artifact_array(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("input_artifacts must be an empty array")
        return tuple(value)

    @property
    def canonical_bytes(self) -> bytes:
        try:
            # The shared helper rebuilds declared fields only. Check surviving
            # unsafe-copy extras first so that rebuilding cannot discard them.
            if not set(self.__dict__) <= set(WorkerInputManifest.model_fields):
                raise ValueError("unexpected model fields")
            extra = self.model_extra
            if extra is not None and (not isinstance(extra, dict) or extra):
                raise ValueError("unexpected model extras")
            value = revalidate_model(WorkerInputManifest, self)
            payload = value.model_dump(mode="json", by_alias=True)
            encoded = json.dumps(
                payload, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            ).encode("utf-8", errors="strict")
            if len(encoded) > MAX_WORKER_INPUT_FRAME_BYTES:
                raise WorkerInputError("worker_input_too_large")
            return encoded
        except WorkerInputError as exc:
            raise WorkerInputError(exc.code) from None
        except Exception:
            raise WorkerInputError("worker_input_invalid") from None

    @property
    def content_hash(self) -> str:
        try:
            return "sha256:" + hashlib.sha256(
                b"maestro.worker-input.v1\x00" + self.canonical_bytes
            ).hexdigest()
        except WorkerInputError as exc:
            raise WorkerInputError(exc.code) from None
        except Exception:
            raise WorkerInputError("worker_input_invalid") from None


def build_worker_input_manifest(
    *, run_id: str, node_id: str, role: RoleName, task_text: str,
) -> WorkerInputManifest:
    """Validate and commit supplied input without persisting or executing it."""

    try:
        value = WorkerInputManifest(
            run_id=run_id, node_id=node_id, role=role, task_text=task_text,
        )
        value.canonical_bytes
        return value
    except WorkerInputError as exc:
        raise WorkerInputError(exc.code) from None
    except Exception:
        raise WorkerInputError("worker_input_invalid") from None


__all__ = [
    "MAX_WORKER_INPUT_FRAME_BYTES", "WORKER_INPUT_OUTPUT_BYTES",
    "WorkerInputError", "WorkerInputManifest", "build_worker_input_manifest",
]
