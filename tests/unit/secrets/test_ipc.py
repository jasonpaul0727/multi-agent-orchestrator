from __future__ import annotations

import pytest

from orchestrator.models import SecretAccessContext


def _context() -> SecretAccessContext:
    return SecretAccessContext(
        request_id="request-1",
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=1,
        accepted_route_id="decision-1",
        budget_reservation_id="reservation-1",
    )


def test_secret_broker_request_and_response_round_trip_redacts_repr():
    from orchestrator.secrets.ipc import (
        SecretBrokerRequest,
        SecretBrokerResponse,
        decode_secret_broker_request,
        decode_secret_broker_response,
        encode_secret_broker_request,
        encode_secret_broker_response,
    )

    request = SecretBrokerRequest(1, "nonce-9a2c", "env:MODEL_KEY", "primary", "https://api.openai.com/v1", "model_inference", _context())
    response = SecretBrokerResponse(1, "request-1", "primary", "https://api.openai.com/v1", "model_inference", "credential", "authorization", "credential-generated-for-this-test")

    assert decode_secret_broker_request(encode_secret_broker_request(request)) == request
    assert decode_secret_broker_response(encode_secret_broker_response(response)) == response
    assert "nonce-9a2c" not in repr(request)
    assert "credential-generated-for-this-test" not in repr(response)


def test_secret_broker_codec_rejects_duplicate_keys_unknown_fields_and_oversize():
    from orchestrator.secrets.ipc import MAX_SECRET_BROKER_FRAME_BYTES, decode_secret_broker_request

    with pytest.raises(ValueError, match="duplicate"):
        decode_secret_broker_request(b'{"version":1,"version":1}')
    with pytest.raises(ValueError, match="unexpected"):
        decode_secret_broker_request(b'{"version":1,"extra":true}')
    with pytest.raises(ValueError, match="bound"):
        decode_secret_broker_request(b" " * (MAX_SECRET_BROKER_FRAME_BYTES + 1))


def test_secret_broker_codec_rejects_unsupported_versions_and_invalid_response_fields():
    from orchestrator.secrets.ipc import SecretBrokerResponse, decode_secret_broker_request, decode_secret_broker_response, encode_secret_broker_response

    with pytest.raises(ValueError):
        decode_secret_broker_request(b'{"version":2}')
    with pytest.raises(ValueError):
        decode_secret_broker_response(b'{"version":2}')
    invalid_response = SecretBrokerResponse.__new__(SecretBrokerResponse)
    for name, value in {
        "version": 1,
        "request_id": "request-1",
        "provider_id": "primary",
        "endpoint": "https://api.openai.com/v1",
        "purpose": "model_inference",
        "status": "credential",
        "header_name": "bad header\r\nX",
        "credential_value": "credential-value",
    }.items():
        object.__setattr__(invalid_response, name, value)
    with pytest.raises(ValueError):
        encode_secret_broker_response(invalid_response)


def test_secret_broker_request_decoder_rejects_non_exact_json_objects():
    from orchestrator.secrets.ipc import decode_secret_broker_request

    invalid_frames = (
        b'{"version":1,"version":1}',
        b'{"version":1,"extra":true}',
        b'{"version":1} trailing',
        b'{"version":NaN}',
        b"\xff",
    )
    for frame in invalid_frames:
        with pytest.raises(ValueError):
            decode_secret_broker_request(frame)


def test_secret_broker_encoders_emit_the_complete_fixed_wire_schema():
    import json

    from orchestrator.secrets.ipc import (
        SecretBrokerRequest,
        SecretBrokerResponse,
        encode_secret_broker_request,
        encode_secret_broker_response,
    )

    request = SecretBrokerRequest(
        1,
        "nonce-9a2c",
        "env:MODEL_KEY",
        "primary",
        "https://api.openai.com/v1",
        "model_inference",
        _context(),
    )
    response = SecretBrokerResponse(
        1,
        "request-1",
        "primary",
        "https://api.openai.com/v1",
        "model_inference",
        "credential",
        "authorization",
        "credential-generated-for-this-test",
    )

    request_payload = json.loads(encode_secret_broker_request(request))
    response_payload = json.loads(encode_secret_broker_response(response))
    assert set(request_payload) == {
        "version", "session_nonce", "secret_ref", "provider_id", "endpoint", "purpose", "context"
    }
    assert set(request_payload["context"]) == {
        "request_id", "run_id", "node_id", "attempt_id", "fencing_generation",
        "accepted_route_id", "budget_reservation_id",
    }
    assert set(response_payload) == {
        "version", "request_id", "provider_id", "endpoint", "purpose", "status",
        "header_name", "credential_value",
    }
def test_secret_broker_nonce_accepts_urlsafe_leading_punctuation():
    from orchestrator.secrets.ipc import SecretBrokerRequest, encode_secret_broker_request

    request = SecretBrokerRequest(
        1,
        "_nonce-start",
        "env:MODEL_KEY",
        "primary",
        "https://api.openai.com/v1",
        "model_inference",
        _context(),
    )

    assert encode_secret_broker_request(request)
