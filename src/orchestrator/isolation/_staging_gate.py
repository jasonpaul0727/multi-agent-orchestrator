"""Canonical, metadata-only host root. No helper or submission surface exists."""
# Reload must refuse before rebinding ANY definition or the strong root.
if '_BOOT_SENTINEL' in globals():
    raise RuntimeError('native_host_reload_refused')

# All three are fixed CPython builtins; no controlled import/acquisition yet.
import _thread as _threads
import posix as _posix
import sys as _sys

if __name__ != 'orchestrator.isolation._staging_gate':
    raise RuntimeError('native_host_alias_refused')
# Anchor to the already loaded canonical package, independent of this module's
# self-reported file/origin. Only builtin string/module operations run here.
_package = _sys.modules.get('orchestrator.isolation')
_package_origin = getattr(getattr(_package, '__spec__', None), 'origin', None)
if (type(_package_origin) is not str or not _package_origin.endswith('/orchestrator/isolation/__init__.py')
        or getattr(_package, '__file__', None) != _package_origin):
    raise RuntimeError('native_host_package_origin_refused')
_TRUSTED_SOURCE = _package_origin[:-len('__init__.py')] + '_staging_gate.py'
if (__file__ != _TRUSTED_SOURCE or __spec__ is None or __spec__.origin != _TRUSTED_SOURCE
        or _threads.get_native_id() != _posix.getpid()):
    raise RuntimeError('native_host_installation_refused')
_BOOT_SENTINEL = object()
_PRESERVED_PARENT = '/tmp/maestro-staging-a2-native-20261115'
_SUBINTERPRETERS = '/usr/lib/python3.12/lib-dynload/_xxsubinterpreters.cpython-312-x86_64-linux-gnu.so'


class CloseOutcome:
    """Built-in-only finite statuses, available even if bootstrap imports fail."""
    __slots__ = ('value',)

    def __init__(self, value):
        if value not in ('CLOSED', 'RETAINED', 'POISONED'):
            raise ValueError('invalid_close_outcome')
        object.__setattr__(self, 'value', value)

    def __setattr__(self, name, value):
        raise AttributeError('immutable_close_outcome')


CloseOutcome.CLOSED = CloseOutcome('CLOSED')
CloseOutcome.RETAINED = CloseOutcome('RETAINED')
CloseOutcome.POISONED = CloseOutcome('POISONED')


class NativeHostRoot:
    def __new__(cls):
        if globals().get('_HOST_ROOT') is not None:
            raise RuntimeError('native_host_second_install_refused')
        return super().__new__(cls)

    def __init__(self):
        self._lock = _threads.allocate_lock()
        self._pid = _posix.getpid()
        self._thread_id = _threads.get_ident()
        self._lock.acquire()
        try:
            self._bootstrap = {'owner': None, 'roots': None, 'arena': None}
            self._entries = [self._bootstrap, None]
            self._busy = False
            self._debt = False
            self._ready = False
            self._closed = False
            self._close_poisoned = False
            self._state = None
            self._sub = None
            self._interpreter = None
            self._deps = {}
            self._phase = 'VERIFY_MANIFEST'
        finally:
            self._lock.release()

    def _builtin_context(self):
        if (_posix.getpid() != self._pid or _threads.get_ident() != self._thread_id
                or __name__ != 'orchestrator.isolation._staging_gate'
                or getattr(_sys.modules.get(__name__), '__dict__', None) is not globals()
                or __file__ != _TRUSTED_SOURCE or __spec__ is None or __spec__.origin != _TRUSTED_SOURCE
                or _threads.get_native_id() != _posix.getpid()):
            raise RuntimeError('native_host_context_refused')

    def _composition_context(self):
        self._builtin_context()
        if self._sub is None:
            return
        origin = getattr(getattr(self._sub, '__spec__', None), 'origin', None)
        if (origin not in ('built-in', _SUBINTERPRETERS)
                or self._sub.get_current() != self._sub.get_main()
                or self._sub.get_current() != self._interpreter
                or self._deps['threading'].main_thread().ident != self._thread_id):
            raise RuntimeError('native_host_interpreter_refused')

    def _load_main_interpreter(self):
        # This runs AFTER the built-in strong placeholder and busy claim exist.
        import importlib.machinery
        import importlib.util
        import threading
        self._deps['threading'] = threading
        if threading.main_thread().ident != self._thread_id:
            raise RuntimeError('native_host_main_thread_required')
        module = _sys.modules.get('_xxsubinterpreters')
        if module is None:
            spec = importlib.machinery.BuiltinImporter.find_spec('_xxsubinterpreters')
            if spec is None:
                spec = importlib.util.spec_from_file_location('_xxsubinterpreters', _SUBINTERPRETERS)
            if spec is None or spec.origin not in ('built-in', _SUBINTERPRETERS):
                raise RuntimeError('native_host_interpreter_origin_refused')
            module = importlib.util.module_from_spec(spec)
            self._deps['_xxsubinterpreters_loading'] = module
            _sys.modules['_xxsubinterpreters'] = module
            spec.loader.exec_module(module)
        self._sub = module
        origin = getattr(getattr(module, '__spec__', None), 'origin', None)
        if origin not in ('built-in', _SUBINTERPRETERS) or module.get_current() != module.get_main():
            raise RuntimeError('native_host_main_interpreter_required')
        self._interpreter = module.get_current()
        self._composition_context()

    @classmethod
    def open_preserved(cls):
        root = _HOST_ROOT
        root._builtin_context()
        if not root._lock.acquire(False):
            raise RuntimeError('native_host_busy')
        try:
            owner = root._bootstrap.get('owner')
            if owner is not None and owner.has_debt:
                root._debt = True
            if root._busy or root._debt or root._closed:
                raise RuntimeError('native_host_unavailable')
            root._busy = True
        finally:
            root._lock.release()
        try:
            if root._ready:
                root._composition_context()
                return root
            # Nothing above this point can perform a controlled FD acquisition.
            root._load_main_interpreter()
            from ._staging_ownership import AcquisitionLedger, HostState, HostCloseState, NativePhase
            root._deps['NativePhase'] = NativePhase
            owner = AcquisitionLedger()
            root._bootstrap['owner'] = owner
            state = HostState()
            root._state = state
            state.install_bootstrap(root._bootstrap)
            root._entries = state._entries
            from . import _staging_contract as contract
            from . import _staging_arena as arena_module
            from . import _staging_reopen as reopen
            from . import _staging_audit as audit
            from ._staging_a2_contract import NativePhase
            root._deps.update(contract=contract, arena=arena_module, reopen=reopen, audit=audit,
                              HostCloseState=HostCloseState, NativePhase=NativePhase)
            trusted = str(contract._CODE_ROOT / '_staging_gate.py')
            if __file__ != trusted or __spec__.origin != trusted:
                raise RuntimeError('native_host_origin_refused')
            root._phase = 'VERIFY_MANIFEST'
            # Accepted A1 helpers are opaque. A failure latches unreserved debt.
            root._manifest = contract.build_profile_manifest()
            contract.verify_profile_manifest(root._manifest)
            root._phase = 'REOPEN_ROOTS'
            roots = reopen.open_fixture_roots_existing(contract.Path(_PRESERVED_PARENT), owner=owner)
            root._bootstrap['roots'] = roots
            arena = arena_module.SlotArena.open_existing(roots.arena_fd)
            root._bootstrap['arena'] = arena
            owner.adopt_returned_arena(arena)
            contract._require(len(owner.open_fds) == 8)
            audit.verify_ext4_metadata(owner.parent, owner=owner)
            arena._enter()
            primary = None
            try:
                root._baseline = audit.capture_slot0_locked(arena, roots)
            except BaseException as exc:
                primary = exc
                raise
            finally:
                arena._leave(primary)
            root._composition_context()
            with root._lock:
                state.finish_bootstrap(root._bootstrap)
                root._ready = True
            return root
        except BaseException:
            with root._lock:
                root._debt = True
                state = root._state
                phase_enum = root._deps.get('NativePhase')
                if phase_enum is not None:
                    phase = phase_enum[root._phase]
                    owner = root._bootstrap.get('owner')
                    if owner is not None:
                        try:
                            owner.poison_debt(phase)
                        except BaseException:
                            pass
                    if state is not None:
                        try:
                            state.poison_debt(phase)
                        except BaseException:
                            pass
            raise
        finally:
            with root._lock:
                root._busy = False

    def close(self):
        self._composition_context()
        # Import-free statuses cover debt before the enum could even be imported.
        if not self._lock.acquire(False):
            return self._outcome('RETAINED')
        try:
            if self._close_poisoned:
                return self._outcome('POISONED')
            if self._closed:
                return self._outcome('CLOSED')
            owner = self._bootstrap.get('owner')
            if owner is not None and owner.has_debt:
                self._debt = True
            if self._busy or self._debt or not self._ready or any(self._entries):
                return self._outcome('RETAINED')
            self._busy = True
        finally:
            self._lock.release()
        try:
            self._bootstrap['owner'].close_all()
            if not self._bootstrap['owner'].all_closed:
                raise RuntimeError('native_host_closure_unconfirmed')
        except BaseException as exc:
            with self._lock:
                self._debt = True
                self._close_poisoned = True
                self._state.poison_debt(self._deps['NativePhase'].FD_CLOSE)
            if not isinstance(exc, Exception):
                raise
            return self._outcome('POISONED')
        else:
            with self._lock:
                self._closed = True
                self._state.lifecycle = type(self._state.lifecycle).CLOSED

            return self._outcome('CLOSED')
        finally:
            with self._lock:
                self._busy = False

    def _outcome(self, name):
        # Fixed mapping; no imports, FD work or admission capability.
        return {'CLOSED': CloseOutcome.CLOSED, 'RETAINED': CloseOutcome.RETAINED,
                'POISONED': CloseOutcome.POISONED}[name]

_HOST_ROOT = NativeHostRoot()
