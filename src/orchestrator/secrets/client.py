"""One-request Unix packet client used exclusively by the trusted Gateway."""
from __future__ import annotations

import asyncio
import math
from pathlib import Path
import socket
import sys

from orchestrator.config.models import ProviderAdapter, ProviderSpec
from orchestrator.models.gateway import SecretAccessContext
from orchestrator.models.transport import ProviderCredential
from .broker import SecretBrokerUnavailable, _HEADER_BY_ADAPTER
from .ipc import (
    MAX_SECRET_BROKER_FRAME_BYTES, SecretBrokerRequest, SecretBrokerResponse,
    decode_secret_broker_response, encode_secret_broker_request, _nonce,
)


def _validate_response_binding(response: SecretBrokerResponse, request: SecretBrokerRequest,
                               adapter: ProviderAdapter) -> None:
    if (response.version != 1 or response.request_id != request.context.request_id
        or response.provider_id != request.provider_id or response.endpoint != request.endpoint
        or response.purpose != request.purpose
        or response.status not in {"credential", "denied", "unavailable"}
        or adapter not in _HEADER_BY_ADAPTER
        or (response.status == "credential" and response.header_name != _HEADER_BY_ADAPTER[adapter])):
        raise SecretBrokerUnavailable("Secret Broker response binding is invalid")


def _credential_or_none(response: SecretBrokerResponse) -> ProviderCredential | None:
    if response.status != "credential":
        return None
    assert response.header_name is not None and response.credential_value is not None
    return ProviderCredential(header_name=response.header_name, value=response.credential_value,
                              provider_id=response.provider_id, endpoint=response.endpoint,
                              purpose=response.purpose)


class UnixSocketSecretBroker:
    def __init__(self, *, socket_path: Path, session_nonce: str, timeout_seconds: float = 10.0):
        if sys.platform != "linux":
            raise RuntimeError("Secret Broker requires Linux")
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 3600):
            raise ValueError("Secret Broker deadline is invalid")
        self._socket_path = Path(socket_path)
        self._session_nonce = _nonce(session_nonce)
        self._timeout_seconds = float(timeout_seconds)
        self._available = True

    async def acquire_provider_credential(self, *, secret_ref: str, provider: ProviderSpec,
        endpoint: str, purpose: str, context: SecretAccessContext) -> ProviderCredential | None:
        if not self._available:
            raise SecretBrokerUnavailable("Secret Broker session is closed")
        request = SecretBrokerRequest(1, self._session_nonce, secret_ref, provider.id,
                                      endpoint, purpose, context)
        response = await asyncio.to_thread(self._exchange_one, request)
        _validate_response_binding(response, request, provider.adapter)
        if not self._available:
            raise SecretBrokerUnavailable("Secret Broker session is closed")
        return _credential_or_none(response)

    def _exchange_one(self, request: SecretBrokerRequest) -> SecretBrokerResponse:
        try:
            frame = encode_secret_broker_request(request)
            with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as connection:
                connection.settimeout(self._timeout_seconds)
                connection.connect(str(self._socket_path))
                if connection.send(frame) != len(frame):
                    raise OSError()
                result, _ancillary, flags, _address = connection.recvmsg(MAX_SECRET_BROKER_FRAME_BYTES)
                if not result or flags & socket.MSG_TRUNC:
                    raise ValueError()
                return decode_secret_broker_response(result)
        except (OSError, ValueError):
            raise SecretBrokerUnavailable("Secret Broker exchange failed") from None
