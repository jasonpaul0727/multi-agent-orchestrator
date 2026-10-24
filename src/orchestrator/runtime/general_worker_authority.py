"""Owned host scope and frozen control bindings for general Worker authority."""

from __future__ import annotations

import os
from pathlib import Path
import stat
from uuid import UUID

from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.general_worker_journal import (
    GeneralWorkerOperation, _guard, _safe, metadata_hash,
)
from orchestrator.runtime.general_worker_protocol import _model_fields, _text, _DIGEST


@_guard
def general_worker_host_scope(event_store: SQLiteEventStore) -> str:
    """Read trusted Linux/file authority before locks; persist only its digest.

    This is deliberately re-sampled by the execution owner before external
    work, never during a checked callback. A copied DB is a different owner.
    """
    if type(event_store) is not SQLiteEventStore or event_store._connection.in_transaction:
        raise ValueError()
    databases = event_store._connection.execute("PRAGMA database_list").fetchall()
    main = [row for row in databases if row[1] == "main"]
    if len(main) != 1 or not main[0][2]:
        raise ValueError()
    path = Path(main[0][2]).resolve(strict=True)
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError()
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    if str(UUID(boot_id)) != boot_id:
        raise ValueError()
    return metadata_hash({"domain": "maestro.general-worker-host-scope.v1", "boot_id": boot_id,
        "effective_uid": os.geteuid(), "database_path_sha256": metadata_hash(str(path)),
        "device": info.st_dev, "inode": info.st_ino})


@_guard
def validate_operation_binding(operation: GeneralWorkerOperation, binding, *,
    workspace_identity_hash: str, host_scope_hash: str) -> None:
    """Compare a persisted operation with the existing frozen VerificationBinding.

    No lease clock is sampled here: the owned Scheduler resolver must obtain
    this binding using trusted UTC after acquiring its authority transaction.
    """
    from orchestrator.scheduler.core import ActiveAttemptSnapshot, VerificationBinding

    if type(operation) is not GeneralWorkerOperation or type(binding) is not VerificationBinding:
        raise ValueError()
    _safe(operation)
    binding_fields = _model_fields(binding, VerificationBinding)
    active_fields = _model_fields(binding_fields["active_attempt"], ActiveAttemptSnapshot)
    context = _safe(binding_fields["context"])
    route = _safe(active_fields["accepted_route"])
    _text(workspace_identity_hash, maximum=71, pattern=_DIGEST)
    _text(host_scope_hash, maximum=71, pattern=_DIGEST)
    if (_safe(operation.context) != context
        or operation.accepted_route_sha256 != metadata_hash(route)
        or operation.workspace_identity_hash != workspace_identity_hash or operation.host_scope_hash != host_scope_hash):
        raise ValueError()
