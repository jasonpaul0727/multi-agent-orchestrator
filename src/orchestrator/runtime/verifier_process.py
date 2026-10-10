"""Attempt-bound, isolated verifier for host-published candidate artifacts.

The child is intentionally dependency-free. It runs only deterministic
read-only checks in the systemd boundary and never writes lifecycle state.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from orchestrator.artifacts import ArtifactAccessGrant, ArtifactStore
    from orchestrator.isolation import SandboxLimits, SystemdReadOnlyLauncher
    from orchestrator.runtime.contracts import VerificationEvidence, VerificationTask


_MAX_FRAME_BYTES = 1_048_576
_DEFAULT_ARTIFACT_BYTES = 8 * 1024 * 1024
_CHILD_PATH = "@maestro-runtime@/orchestrator/runtime/verifier_process.py"
BUILTIN_VERIFIER_ID = "builtin.readonly-v1"
BUILTIN_VERIFICATION_CONTRACT_ID = "maestro.artifact-verification/v1"
SUPPORTED_VERIFICATION_CHECK_IDS = frozenset(
    {"artifact-integrity", "utf8-text", "json", "python-syntax"}
)
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$", re.ASCII)
_STAGED_NAME = re.compile(r"^candidate-[0-9]{3}\.bin$", re.ASCII)


class VerifierProcessError(RuntimeError):
    """The candidate, isolation transport, or independent evidence is invalid."""


class IsolatedVerifierProcess:
    """Verify exact Attempt artifacts in an isolated read-only child process.

    This built-in verifier checks byte integrity and selected deterministic
    formats. It is not a semantic code reviewer and does not accept a Run or
    Node; trusted control-plane logic must decide what the evidence means.
    """

    def __init__(
        self,
        *,
        launcher: SystemdReadOnlyLauncher | None = None,
        limits: SandboxLimits | None = None,
        max_artifact_bytes: int = _DEFAULT_ARTIFACT_BYTES,
    ) -> None:
        from orchestrator.isolation import SandboxLimits, SystemdReadOnlyLauncher

        if (
            isinstance(max_artifact_bytes, bool)
            or not isinstance(max_artifact_bytes, int)
            or not 1 <= max_artifact_bytes <= 64 * 1024 * 1024
        ):
            raise ValueError("max_artifact_bytes must be between 1 byte and 64 MiB")
        self._launcher = launcher if launcher is not None else SystemdReadOnlyLauncher()
        self._max_artifact_bytes = max_artifact_bytes
        self._limits = limits if limits is not None else SandboxLimits(
            timeout_seconds=15,
            output_bytes=256 * 1024,
            file_bytes=max_artifact_bytes,
        )

    def verify(
        self,
        task: VerificationTask,
        artifact_store: ArtifactStore,
        *,
        grant_for_digest: Callable[[str], ArtifactAccessGrant],
    ) -> VerificationEvidence:
        """Re-read verified ArtifactStore bytes and return a non-authoritative proposal."""

        from orchestrator.artifacts import ArtifactAccessGrant
        from orchestrator.runtime.contracts import (
            RuntimeContractError,
            VerificationTask,
            decode_verification_evidence,
            validate_verification_evidence,
            validate_verification_task,
        )

        if not isinstance(task, VerificationTask):
            raise TypeError("task must be a validated VerificationTask")
        try:
            task = VerificationTask.model_validate(task.model_dump(mode="json"))
            validate_verification_task(task)
        except (RuntimeContractError, ValueError, TypeError) as exc:
            raise VerifierProcessError("verification task is invalid") from exc
        if not callable(grant_for_digest):
            raise TypeError("grant_for_digest must be a trusted ArtifactAccessGrant provider")

        context = task.context
        expected_source = {
            "run_id": context.run_id,
            "node_id": context.node_id,
            "attempt_id": context.attempt_id,
            "fencing_generation": str(context.fencing_generation),
            "agent_instance_id": context.agent_instance_id,
        }
        try:
            inventory = artifact_store.verify_run_artifacts(context.run_id)
        except Exception as exc:
            raise VerifierProcessError("candidate ArtifactStore inventory is unavailable") from exc

        files: list[dict[str, object]] = []
        staged_bytes: list[tuple[str, bytes]] = []
        seen_bytes = 0
        for index, reference in enumerate(task.candidate_artifacts):
            matches = [
                record for record in inventory
                if record.digest == reference.digest
                and all(record.source.get(key) == value for key, value in expected_source.items())
            ]
            if not matches:
                raise VerifierProcessError("candidate artifact has no exact Attempt publication")
            record = matches[-1]
            if (
                record.size != reference.size_bytes
                or record.artifact_type != reference.artifact_type
                or record.media_type != reference.media_type
            ):
                raise VerifierProcessError("candidate artifact metadata conflicts with its publication")
            seen_bytes += reference.size_bytes
            if seen_bytes > self._max_artifact_bytes or seen_bytes > self._limits.file_bytes:
                raise VerifierProcessError("candidate artifacts exceed the verifier byte limit")
            try:
                grant = grant_for_digest(reference.digest)
                if not isinstance(grant, ArtifactAccessGrant) or grant.digest != reference.digest:
                    raise ValueError("grant does not bind to the candidate digest")
                content = artifact_store.read_bytes(reference.digest, grant=grant)
            except Exception as exc:
                raise VerifierProcessError("candidate artifact read was not authorized or intact") from exc
            if len(content) != reference.size_bytes:
                raise VerifierProcessError("candidate artifact size changed during verification")
            name = f"candidate-{index:03d}.bin"
            files.append({"digest": reference.digest, "name": name, "size_bytes": len(content)})
            staged_bytes.append((name, content))

        envelope = {
            "task": task.model_dump(mode="json"),
            "artifact_files": files,
        }
        payload = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if len(payload) > _MAX_FRAME_BYTES:
            raise VerifierProcessError("Verifier input exceeds the IPC frame limit")

        try:
            with tempfile.TemporaryDirectory(prefix="maestro-verifier-") as temporary:
                root = Path(temporary)
                os.chmod(root, 0o700)
                for name, content in staged_bytes:
                    if not _STAGED_NAME.fullmatch(name):
                        raise VerifierProcessError("host generated an invalid staged artifact name")
                    descriptor = os.open(
                        root / name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                        0o400,
                    )
                    try:
                        offset = 0
                        while offset < len(content):
                            offset += os.write(descriptor, content[offset:])
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                session = self._launcher.launch(
                    root,
                    ["/usr/bin/python3", _CHILD_PATH, "--child"],
                    limits=self._limits,
                    input_bytes=payload,
                )
                result = session.wait()
        except VerifierProcessError:
            raise
        except Exception as exc:
            raise VerifierProcessError("isolated Verifier could not be launched") from exc

        if (
            not result.termination_confirmed
            or result.returncode != 0
            or result.cancelled
            or result.timed_out
            or result.output_limited
            or not result.input_written
            or result.stderr
        ):
            raise VerifierProcessError("isolated Verifier transport did not complete cleanly")
        try:
            evidence = decode_verification_evidence(result.stdout)
            if evidence.verifier_id != BUILTIN_VERIFIER_ID:
                raise RuntimeContractError("unsupported verifier")
            validate_verification_evidence(task, evidence)
        except RuntimeContractError as exc:
            raise VerifierProcessError("isolated Verifier returned invalid evidence") from exc
        return evidence


def _verification_child_result(workspace: Path, raw: bytes) -> bytes:
    """Validate one child frame and run only fixed, deterministic checks."""

    try:
        if not isinstance(raw, bytes) or not raw or len(raw) > _MAX_FRAME_BYTES:
            raise ValueError
        envelope = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_reject_constant,
        )
        if not isinstance(envelope, dict) or set(envelope) != {"task", "artifact_files"}:
            raise ValueError
        task = envelope["task"]
        if not isinstance(task, dict) or set(task) != {
            "context", "candidate_artifacts", "required_check_ids", "acceptance_contract",
        }:
            raise ValueError
        context = task["context"]
        if not isinstance(context, dict) or not isinstance(task["candidate_artifacts"], list):
            raise ValueError
        if task["acceptance_contract"] != BUILTIN_VERIFICATION_CONTRACT_ID:
            raise ValueError
        required = task["required_check_ids"]
        if (
            not isinstance(required, list)
            or len(required) != len(set(required))
            or "artifact-integrity" not in required
            or any(check not in SUPPORTED_VERIFICATION_CHECK_IDS for check in required)
        ):
            raise ValueError
        references = task["candidate_artifacts"]
        files = envelope["artifact_files"]
        if not isinstance(files, list) or len(files) != len(references) or not references:
            raise ValueError
        digests: list[str] = []
        contents: list[bytes] = []
        for index, (reference, entry) in enumerate(zip(references, files)):
            if not isinstance(reference, dict) or not isinstance(entry, dict):
                raise ValueError
            if set(reference) != {"digest", "size_bytes", "artifact_type", "media_type"}:
                raise ValueError
            if set(entry) != {"digest", "name", "size_bytes"}:
                raise ValueError
            digest = reference["digest"]
            name = entry["name"]
            if (
                not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
                or digest != entry["digest"]
                or type(entry["size_bytes"]) is not int
                or entry["size_bytes"] != reference["size_bytes"]
                or name != f"candidate-{index:03d}.bin"
                or not isinstance(name, str) or not _STAGED_NAME.fullmatch(name)
            ):
                raise ValueError
            path = workspace / name
            if path.is_symlink() or not path.is_file():
                raise ValueError
            content = path.read_bytes()
            digests.append(digest)
            contents.append(content)

        all_digests = sorted(digests)
        checks: list[dict[str, object]] = []
        for check_id in required:
            passed = True
            for digest, content, reference in zip(digests, contents, references):
                if check_id == "artifact-integrity":
                    passed = passed and len(content) == reference["size_bytes"]
                    passed = passed and f"sha256:{hashlib.sha256(content).hexdigest()}" == digest
                elif check_id == "utf8-text":
                    try:
                        content.decode("utf-8", errors="strict")
                    except UnicodeDecodeError:
                        passed = False
                elif check_id == "json":
                    try:
                        json.loads(
                            content.decode("utf-8", errors="strict"),
                            object_pairs_hook=_unique_pairs,
                            parse_constant=_reject_constant,
                        )
                    except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
                        passed = False
                elif check_id == "python-syntax":
                    try:
                        source = content.decode("utf-8", errors="strict")
                        ast.parse(source, filename="candidate.py", mode="exec")
                    except (UnicodeDecodeError, SyntaxError, ValueError, RecursionError):
                        passed = False
            checks.append({
                "check_id": check_id,
                "passed": passed,
                "evidence_digests": all_digests,
            })
        evidence = {
            "context": context,
            "verifier_id": BUILTIN_VERIFIER_ID,
            "outcome": "accepted" if all(item["passed"] for item in checks) else "rejected",
            "checks": checks,
            "inspected_digests": all_digests,
        }
        return json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (OSError, ValueError, TypeError, UnicodeDecodeError, RecursionError) as exc:
        raise VerifierProcessError("Verifier input or candidate is malformed") from exc


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("non-finite JSON number")


def _child_main() -> int:
    try:
        raw = sys.stdin.buffer.read(_MAX_FRAME_BYTES + 1)
        result = _verification_child_result(Path.cwd(), raw)
        sys.stdout.buffer.write(result)
        sys.stdout.buffer.flush()
        return 0
    except (OSError, ValueError, TypeError, RecursionError, VerifierProcessError):
        return 64


if __name__ == "__main__":
    raise SystemExit(_child_main() if sys.argv[1:] == ["--child"] else 64)


__all__ = [
    "BUILTIN_VERIFICATION_CONTRACT_ID",
    "BUILTIN_VERIFIER_ID",
    "SUPPORTED_VERIFICATION_CHECK_IDS",
    "IsolatedVerifierProcess",
    "VerifierProcessError",
]
