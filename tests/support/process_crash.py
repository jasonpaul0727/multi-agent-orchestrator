"""Bounded test-only rendezvous for parent-issued process death."""

import math
import os
import signal
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess


def _point_bytes(point: str) -> bytes:
    if not isinstance(point, str):
        raise ValueError("crash point must be a non-empty ASCII name up to 96 bytes")
    try:
        encoded = point.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError("crash point must be a non-empty ASCII name up to 96 bytes") from None
    if not 1 <= len(encoded) <= 96:
        raise ValueError("crash point must be a non-empty ASCII name up to 96 bytes")
    return encoded


def block_at_crash_point(pipe: Connection, point: str) -> None:
    """Announce only the point name, then block until the parent kills us."""
    pipe.send_bytes(_point_bytes(point))
    pipe.recv_bytes(1)


def kill_at_crash_point(
    process: BaseProcess,
    pipe: Connection,
    *,
    expected_point: str,
    timeout_seconds: float = 10.0,
) -> None:
    """Kill and reap a child at the exact point, including on barrier errors."""
    # Keep cleanup finite even if a caller supplies an invalid timeout.
    cleanup_timeout = 10.0
    try:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive")
        cleanup_timeout = timeout_seconds
        expected = _point_bytes(expected_point)
        assert pipe.poll(timeout_seconds), "child never reached crash point"
        assert pipe.recv_bytes(96) == expected, "child reached an unexpected crash point"
        os.kill(process.pid, signal.SIGKILL)
        process.join(timeout=timeout_seconds)
        assert process.exitcode == -signal.SIGKILL, "child was not reaped after SIGKILL"
    finally:
        if process.is_alive():
            os.kill(process.pid, signal.SIGKILL)
            process.join(timeout=cleanup_timeout)
            assert not process.is_alive(), "child survived SIGKILL cleanup"
