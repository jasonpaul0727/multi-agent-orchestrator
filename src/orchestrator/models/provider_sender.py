"""Sanitized host proof that a Provider sender process has stopped."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator


_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$", re.ASCII)
_UNIT_NAME = re.compile(r"^maestro-provider-[0-9a-f]{32}\.service$", re.ASCII)


class ProviderSenderTerminationReceipt(BaseModel):
    """Immutable, call-bound and secret-free evidence of a stopped sender unit.

    This object is created by the host only after verifying the exact systemd
    unit and its cgroup. It deliberately contains a digest rather than the
    absolute cgroup path, and never contains request, response, or credential
    material.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_call_stream_id: StrictStr = Field(min_length=1, max_length=256)
    run_id: StrictStr = Field(min_length=1, max_length=128)
    node_id: StrictStr = Field(min_length=1, max_length=128)
    attempt_id: StrictStr = Field(min_length=1, max_length=128)
    fencing_generation: StrictInt = Field(ge=1)
    accepted_route_id: StrictStr = Field(min_length=1, max_length=256)
    budget_reservation_id: StrictStr = Field(min_length=1, max_length=256)
    provider_id: StrictStr = Field(min_length=1, max_length=128)
    model_id: StrictStr = Field(min_length=1, max_length=128)
    registry_manifest_hash: StrictStr
    request_hash: StrictStr
    unit_name: StrictStr
    cgroup_path_hash: StrictStr
    active_state: Literal["inactive", "failed"]
    cgroup_empty: StrictBool
    observed_at: datetime

    @field_validator("registry_manifest_hash", "request_hash", "cgroup_path_hash")
    @classmethod
    def require_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("value must be a SHA-256 content hash")
        return value

    @field_validator("unit_name")
    @classmethod
    def require_provider_unit_name(cls, value: str) -> str:
        if not _UNIT_NAME.fullmatch(value):
            raise ValueError("unit_name must be a unique Maestro Provider service")
        return value

    @field_validator("observed_at")
    @classmethod
    def require_aware_observation_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return value

    @field_validator("cgroup_empty")
    @classmethod
    def require_empty_cgroup(cls, value: bool) -> bool:
        if value is not True:
            raise ValueError("sender termination requires an empty cgroup")
        return value

    @property
    def receipt_hash(self) -> str:
        payload = self.model_dump(mode="json")
        canonical = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


__all__ = ["ProviderSenderTerminationReceipt"]
