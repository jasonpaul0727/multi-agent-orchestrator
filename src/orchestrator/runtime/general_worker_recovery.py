"""Private single-owner recovery decisions; ownership never proves unit stop."""

import os
import threading

from .general_worker_journal import _request_retry_allowed


class GeneralWorkerOwnership:
    """Nonblocking advisory lease under the canonical database authority."""

    def __init__(self, root, operation_id):
        self._root, self._operation_id, self._descriptor = root.resolve(), operation_id, None
        self._state_lock = threading.Lock()
        self._closed = False

    def acquire(self):
        import fcntl
        with self._state_lock:
            if self._closed or self._descriptor is not None:
                return False
        self._root.mkdir(mode=0o700, exist_ok=True)
        if self._root.resolve(strict=True) != self._root or self._root.is_symlink():
            raise ValueError()
        descriptor = os.open(self._root / (self._operation_id + ".lock"),
            os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            # close() can win while filesystem/flock work is pending. Transfer
            # the FD only to a still-live claim; otherwise this worker owns its
            # cleanup, independently of any asyncio waiter or event loop.
            with self._state_lock:
                if self._closed or self._descriptor is not None:
                    return False
                self._descriptor = descriptor
                descriptor = None
            return True
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def close(self):
        with self._state_lock:
            self._closed = True
            descriptor = self._descriptor
            self._descriptor = None
        if descriptor is not None:
            os.close(descriptor)


def recovery_action(operation):
    """Classify validated durable metadata; mint no launch or dispatch authority."""
    if operation.accepted_result is not None:
        return "accepted"
    if operation.phase in {"worker_requested", "completion_requested"}:
        return "launch_unknown" if operation.turns[-1].launch_receipt is None else "stop"
    if operation.phase == "requested" or _request_retry_allowed(operation):
        return "request"
    if operation.phase == "worker_stopped" and operation.call is None and operation.capture is None:
        return "dispatch" if operation.turns[-1].request_observed else "held"
    if operation.phase == "call_reserved":
        return "call_held"
    if operation.phase in {"response_captured", "completion_stopped"}:
        return "complete"
    if operation.phase == "proposed":
        return "accept"
    return "held"
