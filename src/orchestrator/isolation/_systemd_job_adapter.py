"""Unwired connection-bound job samples; never launch, stop, or authorization proof.

Each returned observation is a snapshot and can become stale immediately. Call
poll again to sample continuity. The caller-supplied reply is only an external
claim of correlation, not proof of who initiated a job or of host identity.
"""
import threading

from ._systemd_job_correlation import JobCorrelation
from ._systemd_job_monitor import JobMonitor


class JobMonitorCorrelation:
    def __init__(self, monitor, unit, *, max_events, max_bytes):
        self._lock = threading.RLock()
        self._monitor = monitor
        self._owner, self._epoch = monitor.owner, monitor.epoch
        self._correlation = JobCorrelation(unit, max_events=max_events, max_bytes=max_bytes)
        self._unit = unit
        self._closed = False
        self._correlation.subscribe(self._owner, self._epoch)
        # Marks the start of a correlation window, not an actual dispatch.
        self._correlation.dispatch(self._owner, self._epoch)
        self._check_connection()

    @classmethod
    def open(cls, unit, *, max_events=64, max_bytes=16384):
        # Validate limits before acquiring native resources.
        JobCorrelation(unit, max_events=max_events, max_bytes=max_bytes)
        monitor = JobMonitor.open(max_events=max_events, max_bytes=max_bytes)
        if monitor is None:
            return None
        try:
            return cls(monitor, unit, max_events=max_events, max_bytes=max_bytes)
        except BaseException as exc:
            try:
                monitor.close()
            except BaseException as cleanup:
                if isinstance(exc, Exception) and not isinstance(cleanup, Exception):
                    cleanup.add_note("original open failure: " + repr(exc))
                    raise cleanup from exc
                exc.add_note("monitor cleanup failure: " + repr(cleanup))
            raise

    def _check_connection(self):
        try:
            if (self._closed or self._monitor.unknown or
                (self._monitor.owner, self._monitor.epoch) != (self._owner, self._epoch)):
                self._correlation.disconnect()
        except BaseException as exc:
            self._correlation.disconnect()
            if not isinstance(exc, Exception):
                raise
        return not self._correlation.unknown

    @property
    def unknown(self):
        with self._lock:
            return not self._check_connection()

    def reply(self, job_id, path):
        with self._lock:
            if self._check_connection():
                self._correlation.reply(self._owner, self._epoch, job_id, path, self._unit)

    def timeout(self):
        with self._lock:
            self._correlation.timeout()

    def poll(self, timeout_ms=0, *, operation_timeout_ms=1000):
        with self._lock:
            if not self._check_connection():
                return None
            try:
                batch = self._monitor.poll(timeout_ms, operation_timeout_ms=operation_timeout_ms)
                if batch is None or not self._check_connection():
                    self._correlation.disconnect()
                    return None
                for event in batch:
                    if (event.sender, event.epoch) != (self._owner, self._epoch):
                        self._correlation.disconnect()
                        break
                    self._correlation.signal(self._owner, self._epoch, event.job_id,
                                             event.path, event.unit, event.result)
                if not self._check_connection():
                    return None
                return self._correlation.observation
            except BaseException as exc:
                self._correlation.disconnect()
                if isinstance(exc, Exception):
                    return None
                raise

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._correlation.disconnect()
            self._monitor.close()
