"""Fixed production child entrypoint; no configured credential value backend."""
from __future__ import annotations

import ctypes
import os
from pathlib import Path
import signal
import sys
import threading
import time

from orchestrator.config.models import ProviderSpec
from .broker import SecretAccessRule, UnavailableSecretValueStore
from .process import MAX_BOOTSTRAP_BYTES, _read_frame
from .server import UnixSecretBrokerServer


def _decode_bootstrap(record):
    if set(record) != {"version", "expected_parent_pid", "event_store_path", "socket_path",
                       "session_nonce", "rules", "providers"} or type(record["version"]) is not int or record["version"] != 1:
        raise ValueError("Invalid bootstrap")
    if type(record["expected_parent_pid"]) is not int or record["expected_parent_pid"] <= 0:
        raise ValueError("Invalid parent")
    if not isinstance(record["rules"], list) or not isinstance(record["providers"], list):
        raise ValueError("Invalid descriptors")
    rules = []
    for rule in record["rules"]:
        if not isinstance(rule, dict) or set(rule) != {"provider_id", "secret_ref", "endpoint", "purpose", "allowed_run_ids"}:
            raise ValueError("Invalid rule")
        if not isinstance(rule["allowed_run_ids"], list) or any(not isinstance(run, str) for run in rule["allowed_run_ids"]):
            raise ValueError("Invalid Runs")
        rules.append(SecretAccessRule(**{**rule, "allowed_run_ids": frozenset(rule["allowed_run_ids"])}))
    providers = {}
    for item in record["providers"]:
        provider = ProviderSpec.model_validate(item)
        if provider.id in providers:
            raise ValueError("Duplicate Provider")
        providers[provider.id] = provider
    for key in ("event_store_path", "socket_path"):
        if not isinstance(record[key], str) or not Path(record[key]).is_absolute():
            raise ValueError("Invalid path")
    return rules, providers


def main() -> int:
    try:
        if sys.platform != "linux":
            return 1
        # Install before blocking on bootstrap; recheck after reading parent ID
        # to close the fork-to-prctl parent-death race.
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
            return 1
        record = _read_frame(0, deadline=time.monotonic() + 10, max_bytes=MAX_BOOTSTRAP_BYTES)
        os.close(0)
        rules, providers = _decode_bootstrap(record)
        if os.getppid() != record["expected_parent_pid"]:
            return 1
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        # A death arriving between parent check and handler installation exits
        # via the default TERM action. Recheck after installation as well.
        if os.getppid() != record["expected_parent_pid"]:
            return 1
        server = UnixSecretBrokerServer(socket_path=Path(record["socket_path"]),
            session_nonce=record["session_nonce"], event_store_path=Path(record["event_store_path"]),
            rules=rules, providers=providers, value_store=UnavailableSecretValueStore(),
            expected_uid=os.getuid(), unlink_on_close=False)
        def serve():
            try:
                server.serve_forever(readiness_fd=1)
            except Exception:
                pass
            finally:
                stop.set()
        thread = threading.Thread(target=serve, name="secret-broker-listener", daemon=True)
        thread.start()
        stop.wait()
        server.close()
        thread.join(timeout=2)
        return 0
    except Exception:
        # Never print bootstrap, exceptions, descriptors, or credentials.
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
