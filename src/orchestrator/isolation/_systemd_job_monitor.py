"""Read-only sd-bus JobRemoved samples, never an authorization or stop proof.

Owner continuity is observed only while this dedicated connection is alive.
Same-UID adversaries, missed events before subscription, and manager restart
recovery are outside this monitor. No method can start or stop a unit.
"""
import ctypes as c
from dataclasses import dataclass
import secrets
import threading
import time

from ._systemd_job_correlation import _identity, _owner, _unit, _RESULT


@dataclass(frozen=True, slots=True)
class JobRemovedEvent:
    sender: str
    epoch: int
    job_id: int
    path: str
    unit: str
    result: str


class _EventBuffer:
    def __init__(self, owner, max_events, max_bytes, epoch=1):
        if (not _owner(owner, epoch) or type(max_events) is not int or not 1 <= max_events <= 256
            or type(max_bytes) is not int or not 1 <= max_bytes <= 65536):
            raise ValueError("invalid monitor limits")
        self.owner, self.epoch = owner, epoch
        self.max_events, self.max_bytes = max_events, max_bytes
        self.events = []
        self.size = 0
        self.unknown = False

    def fail(self):
        self.unknown = True
        self.events.clear()
        self.size = 0

    def owner_changed(self, owner):
        # Any transition for the watched well-known name breaks continuity.
        self.fail()

    def add(self, sender, job_id, path, unit, result):
        if self.unknown: return
        if (type(sender) is not str or sender != self.owner or not _identity(job_id, path)
            or not _unit(unit) or type(result) is not str or not _RESULT.fullmatch(result)):
            self.fail()
            return
        size = len(sender) + len(path) + len(unit) + len(result) + 8
        if len(self.events) >= self.max_events or self.size + size > self.max_bytes:
            self.fail()
            return
        self.events.append(JobRemovedEvent(sender, self.epoch, job_id, path, unit, result))
        self.size += size

    def drain(self):
        if self.unknown: return None
        result = tuple(self.events)
        self.events.clear()
        self.size = 0
        return result


_P = c.c_void_p
_CALLBACK = c.CFUNCTYPE(c.c_int, _P, _P, _P)
# sd-bus supplies NUL-terminated borrowed strings valid while the message lives.
# This does not protect against a broken native library returning an invalid pointer.
_strnlen = c.CDLL(None).strnlen
_strnlen.argtypes = [_P, c.c_size_t]
_strnlen.restype = c.c_size_t


def _decode_cstring(pointer, limit):
    if not pointer:
        raise ValueError("missing string")
    size = _strnlen(pointer, limit + 1)
    if size > limit:
        raise ValueError("string limit exceeded")
    return c.string_at(pointer, size).decode("ascii")
# Retain callbacks if interrupted native cleanup leaves ownership uncertain.
_KEEPERS = []
_KEEPER_LOCK = threading.Lock()
_KEEPER_LIMIT = 32


class _BusError(c.Structure):
    _fields_ = [("name", c.c_char_p), ("message", c.c_char_p), ("need_free", c.c_int)]


class _ABI:
    def __init__(self):
        lib = c.CDLL("libsystemd.so.0")
        self._lib = lib  # Keep native function pointers valid for this ABI.
        def bind(name, restype, argtypes):
            fn = getattr(lib, name)
            fn.restype, fn.argtypes = restype, argtypes
            return fn
        self.open = bind("sd_bus_open_user", c.c_int, [c.POINTER(_P)])
        self.new = bind("sd_bus_message_new_method_call", c.c_int,
            [_P, c.POINTER(_P), c.c_char_p, c.c_char_p, c.c_char_p, c.c_char_p])
        self.append = bind("sd_bus_message_append_basic", c.c_int, [_P, c.c_char, _P])
        self.call = bind("sd_bus_call", c.c_int, [_P, _P, c.c_uint64, c.POINTER(_BusError), c.POINTER(_P)])
        self.read = bind("sd_bus_message_read_basic", c.c_int, [_P, c.c_char, _P])
        self.at_end = bind("sd_bus_message_at_end", c.c_int, [_P, c.c_int])
        self.sender = bind("sd_bus_message_get_sender", _P, [_P])
        self.match = bind("sd_bus_add_match", c.c_int, [_P, c.POINTER(_P), c.c_char_p, _CALLBACK, _P])
        self.process = bind("sd_bus_process", c.c_int, [_P, c.POINTER(_P)])
        self.wait = bind("sd_bus_wait", c.c_int, [_P, c.c_uint64])
        self.message_unref = bind("sd_bus_message_unref", _P, [_P])
        self.slot_unref = bind("sd_bus_slot_unref", _P, [_P])
        self.bus_close = bind("sd_bus_close", None, [_P])
        self.bus_unref = bind("sd_bus_unref", _P, [_P])
        self.error_free = bind("sd_bus_error_free", None, [c.POINTER(_BusError)])


def _check(code):
    if code < 0: raise OSError(-code, "sd-bus operation failed")


class JobMonitor:
    """A single serialized connection with bounded in-memory event delivery."""
    def __init__(self, abi, max_events, max_bytes):
        self._abi = abi
        self._lock = threading.RLock()
        self._bus = _P()
        self._slots = []
        self._callbacks = []
        self._buffer = None
        self._failed = False
        self._max_events, self._max_bytes = max_events, max_bytes
        self._owner_name = None
        self._owner_changed = False
        self._callback_error = None
        self._installation_uncertain = False
        self._teardown_uncertain = False
        self._preserved_bus = None
        self._preserved_slots = None
        self._epoch = secrets.randbits(63) or 1

    @classmethod
    def open(cls, *, max_events=64, max_bytes=16384):
        if (type(max_events) is not int or not 1 <= max_events <= 256
            or type(max_bytes) is not int or not 1 <= max_bytes <= 65536):
            raise ValueError("invalid monitor limits")
        monitor = None
        try:
            monitor = cls(_ABI(), max_events, max_bytes)
            with _KEEPER_LOCK:
                if len(_KEEPERS) >= _KEEPER_LIMIT:
                    return None
                _KEEPERS.append(monitor)
            _check(monitor._abi.open(c.byref(monitor._bus)))
            monitor._establish()
            return monitor
        except BaseException as exc:
            if monitor is not None:
                try: monitor.close()
                except BaseException as cleanup:
                    if isinstance(exc, Exception) and not isinstance(cleanup, Exception):
                        cleanup.add_note("original open failure: " + repr(exc))
                        raise cleanup from exc
                    exc.add_note("monitor cleanup failure: " + repr(cleanup))
            if isinstance(exc, Exception): return None
            raise

    def _request(self, destination, path, interface, member, argument=None, timeout_us=1000000):
        msg, reply, error = _P(), _P(), _BusError()
        _check(self._abi.new(self._bus, c.byref(msg), destination, path, interface, member))
        try:
            if argument is not None:
                _check(self._abi.append(msg, b"s", c.c_char_p(argument)))
            try:
                result = self._abi.call(self._bus, msg, timeout_us, c.byref(error), c.byref(reply))
                _check(result)
            except BaseException:
                if reply:
                    self._abi.message_unref(reply)
                raise
            finally:
                self._abi.error_free(c.byref(error))
            if not reply: raise ValueError("missing reply")
            return reply
        finally:
            self._abi.message_unref(msg)

    def _get_owner(self, timeout_us=1000000):
        reply = self._request(b"org.freedesktop.DBus", b"/org/freedesktop/DBus",
                              b"org.freedesktop.DBus", b"GetNameOwner", b"org.freedesktop.systemd1", timeout_us)
        try:
            value = _P()
            if self._abi.read(reply, b"s", c.byref(value)) <= 0 or self._abi.at_end(reply, 1) != 1:
                raise ValueError("invalid owner reply")
            owner = _decode_cstring(value.value, 255)
            if not _owner(owner, self.epoch): raise ValueError("invalid owner")
            return owner
        finally:
            self._abi.message_unref(reply)

    def _make_callbacks(self):
        @_CALLBACK
        def job(msg, userdata, error):
            try:
                sender = self._abi.sender(msg)
                number, path, unit, result = c.c_uint32(), _P(), _P(), _P()
                for kind, value in ((b"u", number), (b"o", path), (b"s", unit), (b"s", result)):
                    if self._abi.read(msg, kind, c.byref(value)) <= 0: raise ValueError("invalid job signal")
                if self._abi.at_end(msg, 1) != 1:
                    raise ValueError("invalid job signal")
                self._buffer.add(_decode_cstring(sender, 255), number.value,
                                 _decode_cstring(path.value, 128), _decode_cstring(unit.value, 255),
                                 _decode_cstring(result.value, 32))
            except BaseException as exc:
                self._record_callback_error(exc)
            return 0
        @_CALLBACK
        def changed(msg, userdata, error):
            try:
                name, old, new = _P(), _P(), _P()
                for value in (name, old, new):
                    if self._abi.read(msg, b"s", c.byref(value)) <= 0: raise ValueError("invalid owner signal")
                if self._abi.at_end(msg, 1) != 1:
                    raise ValueError("invalid owner signal")
                name_text = _decode_cstring(name.value, 255)
                _decode_cstring(old.value, 255)
                new_text = _decode_cstring(new.value, 255)
                if name_text == "org.freedesktop.systemd1":
                    self._owner_changed = True
                    if self._buffer is not None: self._buffer.owner_changed(new_text)
            except BaseException as exc:
                self._record_callback_error(exc)
            return 0
        self._job_callback, self._owner_callback = job, changed

    def _record_callback_error(self, exc):
        self._failed = True
        if self._buffer is not None: self._buffer.fail()
        if not isinstance(exc, Exception) and self._callback_error is None:
            self._callback_error = exc

    def _raise_callback_error(self):
        if self._callback_error is not None:
            exc, self._callback_error = self._callback_error, None
            raise exc

    def _register(self, rule, callback):
        # Both objects are owned by the globally retained monitor before C sees them.
        self._callbacks.append(callback)
        slot = _P()
        self._slots.append(slot)
        self._installation_uncertain = True
        _check(self._abi.match(self._bus, c.byref(slot), rule.encode("ascii"), callback, None))
        self._installation_uncertain = False

    def _install_owner_match(self):
        self._make_callbacks()
        q = chr(39)
        rule = ("type="+q+"signal"+q+",sender="+q+"org.freedesktop.DBus"+q+",path="+q+"/org/freedesktop/DBus"+q+",interface="+q+"org.freedesktop.DBus"+q+",member="+q+"NameOwnerChanged"+q+",arg0="+q+"org.freedesktop.systemd1"+q)
        self._register(rule, self._owner_callback)

    def _install_job_match(self):
        q = chr(39)
        rule = ("type="+q+"signal"+q+",sender="+q+self.owner+q+",path="+q+"/org/freedesktop/systemd1"+q+",interface="+q+"org.freedesktop.systemd1.Manager"+q+",member="+q+"JobRemoved"+q)
        self._register(rule, self._job_callback)

    def _subscribe(self):
        reply = self._request(b"org.freedesktop.systemd1", b"/org/freedesktop/systemd1",
                              b"org.freedesktop.systemd1.Manager", b"Subscribe")
        try:
            if _decode_cstring(self._abi.sender(reply), 255) != self.owner:
                raise ValueError("unexpected subscribe sender")
        finally:
            self._abi.message_unref(reply)

    def _establish(self):
        self._install_owner_match()
        self._owner_name = self._get_owner()
        self._buffer = _EventBuffer(self.owner, self._max_events, self._max_bytes, self.epoch)
        self._install_job_match()
        self._subscribe()
        if self._get_owner() != self.owner:
            raise ValueError("manager owner changed")
        self._drain_ready(time.monotonic() + 1, 0)
        self._raise_callback_error()
        if self._owner_changed or self._failed or self._buffer.unknown:
            raise ValueError("manager owner continuity unknown")

    def _drain_ready(self, deadline, count):
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError("operation budget exceeded")
            result = self._abi.process(self._bus, None)
            if time.monotonic() >= deadline:
                raise TimeoutError("operation budget exceeded")
            _check(result)
            self._raise_callback_error()
            if self._owner_changed or self._failed or self._buffer.unknown:
                raise ValueError("connection continuity unknown")
            if not result: return count
            count += 1
            if count > 256: raise ValueError("poll budget exceeded")

    @property
    def owner(self):
        with self._lock:
            return self._owner_name

    @property
    def epoch(self):
        with self._lock:
            return self._epoch

    @property
    def unknown(self):
        with self._lock:
            return self._failed or self._buffer is None or self._buffer.unknown

    def poll(self, timeout_ms=0, *, operation_timeout_ms=1000):
        # timeout_ms controls wait for events; operation_timeout_ms bounds the
        # poll, including the ordering barrier and owner query. Native calls
        # must return before an overrun can be detected; this is not a hard
        # wall-clock bound on a stalled native library.
        with self._lock:
            self._raise_callback_error()
            if self.unknown: return None
            if (type(timeout_ms) is not int or not 0 <= timeout_ms <= 1000
                or type(operation_timeout_ms) is not int or not 1 <= operation_timeout_ms <= 5000
                or timeout_ms > operation_timeout_ms):
                self._buffer.fail()
                return None
            try:
                operation_deadline = time.monotonic() + operation_timeout_ms / 1000
                wait_deadline = time.monotonic() + timeout_ms / 1000
                count = 0
                while True:
                    count = self._drain_ready(operation_deadline, count)
                    if self._buffer.events or time.monotonic() >= wait_deadline: break
                    remaining = min(wait_deadline, operation_deadline) - time.monotonic()
                    if remaining <= 0: raise TimeoutError("poll deadline")
                    _check(self._abi.wait(self._bus, max(1, int(remaining * 1000000))))
                remaining = operation_deadline - time.monotonic()
                if remaining <= 0: raise TimeoutError("owner deadline")
                if self._get_owner(max(1, int(remaining * 1000000))) != self.owner:
                    raise ValueError("owner changed")
                # The owner query is an ordering barrier. Deliver all already
                # queued callbacks before returning its associated batch.
                self._drain_ready(operation_deadline, count)
                return self._buffer.drain()
            except BaseException as exc:
                self._buffer.fail()
                if isinstance(exc, Exception): return None
                raise

    def close(self):
        with self._lock:
            self._failed = True
            if self._buffer is not None: self._buffer.fail()
            if self._installation_uncertain or self._teardown_uncertain:
                # Native registration may hold these exact callback pointers.
                # Keep the whole connection and owner in its bounded keeper.
                return
            self._teardown_uncertain = True
            self._preserved_bus = self._bus
            self._preserved_slots = self._slots
            slots, self._slots = self._slots, []
            bus, self._bus = self._bus, _P()
            callbacks = self._callbacks
            failures = []
            for slot in slots:
                try: self._abi.slot_unref(slot)
                except BaseException as exc: failures.append(exc)
            if bus:
                try: self._abi.bus_close(bus)
                except BaseException as exc: failures.append(exc)
                try: self._abi.bus_unref(bus)
                except BaseException as exc: failures.append(exc)
            if failures:
                self._preserved_bus = bus
                self._callbacks = callbacks
            else:
                self._callbacks = []
                self._preserved_bus = self._preserved_slots = None
                self._teardown_uncertain = False
                with _KEEPER_LOCK:
                    if self in _KEEPERS: _KEEPERS.remove(self)
            if failures:
                primary = next((exc for exc in failures if not isinstance(exc, Exception)), failures[0])
                for exc in failures:
                    if exc is not primary: primary.add_note("additional close failure: " + repr(exc))
                raise primary
