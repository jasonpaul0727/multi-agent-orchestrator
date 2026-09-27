"""Blocked-by-default Worker process boundary over bounded systemd stdin/stdout.

This module intentionally has only standard-library imports so its child entry
can run under the isolated system Python. It does not execute task text, invoke
tools or providers, publish artifacts, or claim successful Agent execution.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orchestrator.isolation import SandboxLimits, SystemdReadOnlyLauncher
    from orchestrator.runtime.contracts import WorkerResult, WorkerTask


_MAX_FRAME_BYTES = 1_048_576
_CHILD_PATH = "@maestro-runtime@/orchestrator/runtime/worker_process.py"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$", re.ASCII)
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$", re.ASCII)
_CONTEXT_TEXT_FIELDS = frozenset({"run_id", "node_id", "attempt_id", "agent_instance_id"})
_CONTEXT_HASH_FIELDS = frozenset({
    "input_manifest_hash", "effective_config_hash", "registry_hash",
    "policy_manifest_hash", "routing_decision_hash", "planning_contract_hash",
})
_CONTEXT_INTEGER_FIELDS = frozenset({"fencing_generation", "graph_version"})
_TASK_FIELDS = frozenset({
    "context", "role", "task_text", "input_artifacts", "tool_capabilities", "output_byte_limit",
})


class WorkerProcessError(RuntimeError):
    """The isolated child did not produce a valid blocked-only proposal."""


class IsolatedWorkerProcess:
    """Prove an isolated IPC round trip without enabling Agent execution.

    All requests currently return ``blocked``. A future execution adapter
    must add trusted Tool/Provider mediation and artifact attestation before
    candidate results can be admitted. This boundary never passes a control
    plane handle or credential into the child.
    """

    def __init__(
        self,
        *,
        launcher: SystemdReadOnlyLauncher | None = None,
        limits: SandboxLimits | None = None,
    ) -> None:
        from orchestrator.isolation import SandboxLimits, SystemdReadOnlyLauncher

        self._launcher = launcher if launcher is not None else SystemdReadOnlyLauncher()
        self._limits = limits if limits is not None else SandboxLimits(
            timeout_seconds=10,
            output_bytes=16 * 1024,
        )

    def execute(self, workspace: str | Path, task: WorkerTask) -> WorkerResult:
        """Return only a bound blocked proposal, or fail closed on transport error."""

        from orchestrator.runtime.contracts import (
            RuntimeContractError,
            WorkerTask,
            decode_worker_result,
            validate_worker_result,
        )

        if not isinstance(task, WorkerTask):
            raise TypeError("task must be a validated WorkerTask")
        payload = task.model_dump_json().encode("utf-8")
        if len(payload) > _MAX_FRAME_BYTES:
            raise WorkerProcessError("Worker input exceeds the IPC frame limit")
        session = self._launcher.launch(
            workspace,
            ["/usr/bin/python3", _CHILD_PATH, "--child"],
            limits=self._limits,
            input_bytes=payload,
        )
        result = session.wait()
        if (
            not result.termination_confirmed
            or result.returncode != 0
            or result.cancelled
            or result.timed_out
            or result.output_limited
            or not result.input_written
            or result.stderr
        ):
            raise WorkerProcessError(
                "isolated Worker transport did not complete cleanly "
                f"(exit={result.returncode}, terminated={result.termination_confirmed}, "
                f"cancelled={result.cancelled}, timed_out={result.timed_out}, "
                f"output_limited={result.output_limited}, input_written={result.input_written})"
            )
        try:
            proposal = decode_worker_result(result.stdout)
            validate_worker_result(task, proposal)
        except RuntimeContractError as exc:
            raise WorkerProcessError("isolated Worker returned an invalid proposal") from exc
        if proposal.outcome != "blocked" or proposal.artifacts:
            raise WorkerProcessError("blocked-only Worker returned an unauthorized candidate")
        return proposal


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("non-finite JSON number")


def _child_main() -> int:
    """Read a single bounded task and emit a non-authoritative blocked proposal."""

    try:
        raw = sys.stdin.buffer.read(_MAX_FRAME_BYTES + 1)
        if not raw or len(raw) > _MAX_FRAME_BYTES:
            return 64
        task = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_reject_constant,
        )
        if not isinstance(task, dict) or task.keys() != _TASK_FIELDS:
            return 64
        context = task["context"]
        if not isinstance(context, dict) or context.keys() != (
            _CONTEXT_TEXT_FIELDS | _CONTEXT_HASH_FIELDS | _CONTEXT_INTEGER_FIELDS
        ):
            return 64
        if any(not isinstance(context[key], str) or not _IDENTIFIER.fullmatch(context[key])
               for key in _CONTEXT_TEXT_FIELDS):
            return 64
        if any(not isinstance(context[key], str) or not _DIGEST.fullmatch(context[key])
               for key in _CONTEXT_HASH_FIELDS):
            return 64
        if type(context["fencing_generation"]) is not int or context["fencing_generation"] <= 0:
            return 64
        if type(context["graph_version"]) is not int or context["graph_version"] < 0:
            return 64
        if not isinstance(task["role"], str) or not _IDENTIFIER.fullmatch(task["role"]):
            return 64
        if not isinstance(task["task_text"], str) or not task["task_text"].strip():
            return 64
        if not isinstance(task["input_artifacts"], list) or not isinstance(task["tool_capabilities"], list):
            return 64
        if type(task["output_byte_limit"]) is not int or task["output_byte_limit"] <= 0:
            return 64
        result_id = "blocked-" + hashlib.sha256(
            json.dumps(context, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:32]
        proposal = {"context": context, "result_id": result_id, "outcome": "blocked", "artifacts": []}
        sys.stdout.write(json.dumps(proposal, sort_keys=True, separators=(",", ":")))
        sys.stdout.flush()
        return 0
    except (OSError, ValueError, TypeError, UnicodeDecodeError, RecursionError):
        return 64


if __name__ == "__main__":
    raise SystemExit(_child_main() if sys.argv[1:] == ["--child"] else 64)
