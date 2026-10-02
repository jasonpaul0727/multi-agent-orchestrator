"""Fail-closed secret use broker for trusted Gateway calls."""

from .broker import (
    AuditedSecretBroker,
    EnvironmentSecretStore,
    SecretAccessDenied,
    SecretAccessRule,
    SecretBrokerUnavailable,
    SecretValueStore,
    UnavailableSecretValueStore,
)
from .server import UnixSecretBrokerServer
from .ipc import (
    MAX_SECRET_BROKER_FRAME_BYTES,
    SecretBrokerRequest,
    SecretBrokerResponse,
    decode_secret_broker_request,
    decode_secret_broker_response,
    encode_secret_broker_request,
    encode_secret_broker_response,
)

__all__ = [
    "AuditedSecretBroker",
    "EnvironmentSecretStore",
    "MAX_SECRET_BROKER_FRAME_BYTES",
    "SecretAccessDenied",
    "SecretAccessRule",
    "SecretBrokerRequest",
    "SecretBrokerResponse",
    "SecretBrokerUnavailable",
    "SecretValueStore",
    "UnavailableSecretValueStore",
    "UnixSecretBrokerServer",
    "decode_secret_broker_request",
    "decode_secret_broker_response",
    "encode_secret_broker_request",
    "encode_secret_broker_response",
]
