"""Fixed useful Worker child: bounded private pipes, no execution authority."""

from __future__ import annotations

from pathlib import Path
import sys


def _child_main() -> int:
    # The fixed child is invoked by absolute path under isolated system Python.
    # Import its trusted sibling directly, bypassing package/host startup imports.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from general_worker_protocol import MAX_FRAME_BYTES, _proposal_bytes

    try:
        raw = sys.stdin.buffer.read(MAX_FRAME_BYTES + 1)
        proposal = _proposal_bytes(raw)
        sys.stdout.buffer.write(proposal)
        sys.stdout.buffer.flush()
        return 0
    except Exception:
        return 64


if __name__ == "__main__":
    raise SystemExit(_child_main() if sys.argv[1:] == ["--child"] else 64)


from dataclasses import dataclass, field, replace
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orchestrator.isolation import SandboxLaunchReceipt, SandboxTerminationReceipt
    from .general_worker_protocol import GeneralWorkerRequest, GeneralWorkerTurn
    from .worker_output import DecodedWorkerOutput


@dataclass(frozen=True, slots=True)
class GeneralWorkerTurnResult:
    proposal: GeneralWorkerRequest | DecodedWorkerOutput | None = field(repr=False)
    launch_receipt: SandboxLaunchReceipt
    termination_receipt: SandboxTerminationReceipt
    returncode: int
    cancelled: bool
    timed_out: bool
    output_limited: bool
    input_written: bool


class GeneralWorkerSession:
    """Host supervision with a launch acknowledgement available before wait."""

    def __init__(self, sandbox, turn, *, cancellation=None):
        self._sandbox = sandbox
        self._turn = turn
        self._cancellation = cancellation
        self.launch_receipt = sandbox.launch_receipt
        self._result = None
        self._wait_lock = threading.Lock()

    def cancel(self) -> bool:
        return self._sandbox.cancel()

    def wait(self) -> GeneralWorkerTurnResult:
        from orchestrator.isolation import IsolationStopUnknown, SandboxTerminationReceipt
        from .general_worker_protocol import decode_general_worker_proposal

        with self._wait_lock:
            if self._result is not None:
                return self._result
            finished = threading.Event()
            monitor = None
            if self._cancellation is not None:
                def supervise():
                    while not finished.is_set():
                        if self._cancellation.is_set():
                            self._sandbox.cancel()
                            return
                        finished.wait(0.02)
                if self._cancellation.is_set():
                    self._sandbox.cancel()
                else:
                    monitor = threading.Thread(target=supervise, daemon=True)
                    monitor.start()
            execution = None
            try:
                execution = self._sandbox.wait()
            except Exception:
                pass
            finally:
                finished.set()
                if monitor is not None:
                    monitor.join(timeout=4)
            valid_stop = False
            try:
                receipt = execution.termination_receipt
                valid_stop = (type(receipt) is SandboxTerminationReceipt
                    and receipt.unit_name == self.launch_receipt.unit_name == execution.unit_name
                    and receipt.control_group == self.launch_receipt.control_group
                    and receipt.cgroup_empty is True and receipt.active_state in {"inactive", "failed"})
            except Exception:
                pass
            if not valid_stop:
                raise IsolationStopUnknown("general_worker_stop_unknown")
            cancelled = execution.cancelled or (self._cancellation is not None and self._cancellation.is_set())
            proposal = None
            if (execution.returncode == 0 and execution.input_written and not cancelled
                and not execution.timed_out and not execution.output_limited and not execution.stderr):
                try:
                    proposal = decode_general_worker_proposal(execution.stdout, self._turn)
                except Exception:
                    pass
            self._result = GeneralWorkerTurnResult(proposal, self.launch_receipt, receipt,
                execution.returncode, cancelled, execution.timed_out, execution.output_limited,
                execution.input_written)
            return self._result


class GeneralWorkerProcess:
    """Launch fixed useful turns; neither child output nor exit grants authority."""

    def __init__(self, *, launcher=None, limits=None):
        from orchestrator.isolation import SandboxLimits, SystemdReadOnlyLauncher
        from .general_worker_protocol import MAX_FRAME_BYTES

        self._launcher = launcher if launcher is not None else SystemdReadOnlyLauncher()
        limits = limits if limits is not None else SandboxLimits(timeout_seconds=10, output_bytes=MAX_FRAME_BYTES)
        self._limits = replace(limits, output_bytes=min(limits.output_bytes, MAX_FRAME_BYTES))

    def prepare_launch(self):
        return self._launcher.prepare_launch()

    def stop_prepared_launch(self, prepared_launch, launch_receipt):
        return self._launcher.stop_prepared_launch(prepared_launch, launch_receipt)

    def start_turn(self, workspace: Path, turn: GeneralWorkerTurn, *, prepared_launch, cancellation=None,
        expected_workspace_identity_hash: str | None = None) -> GeneralWorkerSession:
        from orchestrator.isolation import IsolationNotStarted, IsolationStartUnknown
        from orchestrator.isolation.launcher import _validate_prepared, _validate_launch_receipt
        from .general_worker_protocol import encode_general_worker_turn

        raw = None
        try:
            _validate_prepared(prepared_launch)
            raw = encode_general_worker_turn(turn)
            if turn.launch_id != prepared_launch.launch_id or (
                cancellation is not None and not isinstance(cancellation, threading.Event)
            ):
                raw = None
        except Exception:
            pass
        if raw is None:
            raise IsolationNotStarted("general_worker_not_started")
        failure = None
        session = None
        try:
            options = {} if expected_workspace_identity_hash is None else {
                "expected_workspace_identity_hash": expected_workspace_identity_hash}
            session = self._launcher.launch(workspace, ["/usr/bin/python3", "-I", "-S",
                "@maestro-runtime@/orchestrator/runtime/general_worker_process.py", "--child"],
                limits=self._limits, input_bytes=raw, prepared_launch=prepared_launch, **options)
            _validate_launch_receipt(prepared_launch, session.launch_receipt)
        except IsolationNotStarted:
            failure = IsolationNotStarted
        except Exception:
            failure = IsolationStartUnknown
        if failure is not None:
            raise failure("general_worker_not_started" if failure is IsolationNotStarted else "general_worker_start_unknown")
        return GeneralWorkerSession(session, turn, cancellation=cancellation)

    def run_turn(self, workspace: Path, turn: GeneralWorkerTurn, *, prepared_launch, cancellation=None,
        expected_workspace_identity_hash: str | None = None) -> GeneralWorkerTurnResult:
        return self.start_turn(workspace, turn, prepared_launch=prepared_launch, cancellation=cancellation,
            expected_workspace_identity_hash=expected_workspace_identity_hash).wait()


__all__ = ["GeneralWorkerProcess", "GeneralWorkerSession", "GeneralWorkerTurnResult"]
