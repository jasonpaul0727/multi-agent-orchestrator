from __future__ import annotations

import base64
import importlib
import importlib.util
import io
import json

import pytest


_MODULE = "orchestrator.runtime.provider_sender_process"


def _process_module():
    assert importlib.util.find_spec(_MODULE) is not None, (
        "the fixed Provider sender helper module is not implemented"
    )
    return importlib.import_module(_MODULE)


def _request_payload(**overrides):
    return {
        "version": 1,
        "url": "https://api.example.test/v1/responses",
        "headers": [
            ["accept", "application/json"],
            ["authorization", "Bearer private-token"],
            ["content-type", "application/json"],
        ],
        "body_b64": base64.b64encode(b'{"model":"test"}').decode("ascii"),
        "timeout_ms": 1_000,
        "max_response_bytes": 4_096,
        **overrides,
    }


def test_provider_sender_child_rejects_duplicate_keys_and_child_receipts():
    module = _process_module()
    duplicate_keys = json.dumps(_request_payload(), separators=(",", ":")).replace(
        '"version":1', '"version":1,"version":1', 1
    ).encode()
    child_receipt = json.dumps(
        {
            "version": 1,
            "status": 200,
            "headers": [],
            "body_b64": base64.b64encode(b"{}").decode("ascii"),
            "termination_receipt": {"cgroup_empty": True},
        },
        separators=(",", ":"),
    ).encode()

    with pytest.raises(ValueError, match="duplicate"):
        module.decode_provider_sender_request(duplicate_keys)
    with pytest.raises(ValueError, match="unexpected"):
        module.decode_provider_sender_response(child_receipt, max_response_bytes=128)


@pytest.mark.parametrize(
    "url",
    [
        "http://api.example.test/v1/responses",
        "https://user:secret@api.example.test/v1/responses",
        "https://api.example.test/v1/responses?api_key=secret",
        "https://api.example.test/v1/responses#fragment",
    ],
)
def test_provider_sender_request_rejects_noncanonical_or_credentialed_urls(url):
    module = _process_module()
    frame = json.dumps(_request_payload(url=url), separators=(",", ":")).encode()

    with pytest.raises(ValueError, match="HTTPS|credential|query|fragment|URL"):
        module.decode_provider_sender_request(frame)


def test_provider_sender_request_rejects_duplicate_headers_and_unbounded_frames():
    module = _process_module()
    duplicate_headers = json.dumps(
        _request_payload(headers=[["accept", "application/json"], ["Accept", "text/plain"]]),
        separators=(",", ":"),
    ).encode()

    with pytest.raises(ValueError, match="duplicate"):
        module.decode_provider_sender_request(duplicate_headers)
    with pytest.raises(ValueError, match="bound|size|large"):
        module.decode_provider_sender_request(b" " * (16 * 1024 * 1024 + 1))


def test_provider_sender_request_roundtrips_bounded_bytes_without_text_coercion():
    module = _process_module()
    body = b'{"text":"provider-body"}'
    frame = module.encode_provider_sender_request(
        url="https://api.example.test/v1/responses",
        headers=(("content-type", "application/json"), ("authorization", "Bearer test")),
        body=body,
        timeout_ms=500,
        max_response_bytes=128,
    )

    decoded = module.decode_provider_sender_request(frame)

    assert decoded.body == body
    assert decoded.url == "https://api.example.test/v1/responses"
    assert decoded.timeout_ms == 500


def test_provider_sender_request_repr_never_exposes_credentials_or_body():
    module = _process_module()
    request = module.decode_provider_sender_request(_request_frame(_request_payload()))

    rendered = repr(request)

    assert "private-token" not in rendered
    assert "Bearer" not in rendered
    assert "test" not in rendered
    assert "body_bytes=" in rendered


def test_provider_sender_performs_one_https_post_and_does_not_follow_redirects(monkeypatch):
    module = _process_module()
    request = module.decode_provider_sender_request(
        module.encode_provider_sender_request(
            url="https://api.example.test/v1/responses",
            headers=(
                ("accept", "application/json"),
                ("authorization", "Bearer private-token"),
                ("content-type", "application/json"),
            ),
            body=b"{}",
            timeout_ms=1_000,
            max_response_bytes=128,
        )
    )
    calls = []

    class Response:
        status = 302

        def read(self, maximum):
            assert maximum == 129
            return b"redirect response"

        def getheader(self, name):
            return {"location": "https://attacker.invalid/", "x-request-id": "req-1"}.get(name)

    class Connection:
        def __init__(self, host, port, *, timeout, context):
            calls.append(("connection", host, port, timeout, context))

        def request(self, method, path, *, body, headers):
            calls.append(("request", method, path, body, headers))

        def getresponse(self):
            return Response()

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(module.http.client, "HTTPSConnection", Connection)

    response = module._perform_https_post(request)

    assert response.status == 302
    assert response.headers == (("x-request-id", "req-1"),)
    assert [call[0] for call in calls] == ["connection", "request", "close"]
    assert calls[1][1:4] == ("POST", "/v1/responses", b"{}")


def test_provider_sender_rejects_response_larger_than_declared_limit(monkeypatch):
    module = _process_module()
    request = module.decode_provider_sender_request(
        module.encode_provider_sender_request(
            url="https://api.example.test/v1/responses",
            headers=(("authorization", "Bearer test"), ("content-type", "application/json")),
            body=b"{}",
            timeout_ms=1_000,
            max_response_bytes=2,
        )
    )

    class Response:
        status = 200

        def read(self, maximum):
            assert maximum == 3
            return b"abc"

        def getheader(self, _name):
            return None

    class Connection:
        closed = False

        def __init__(self, *_args, **_kwargs):
            pass

        def request(self, *_args, **_kwargs):
            pass

        def getresponse(self):
            return Response()

        def close(self):
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(module.http.client, "HTTPSConnection", lambda *_args, **_kwargs: connection)

    with pytest.raises(ValueError, match="byte bound"):
        module._perform_https_post(request)

    assert connection.closed


def _request_frame(payload):
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


@pytest.mark.parametrize(
    "frame",
    [
        b"",
        b"\xff",
        b"not-json",
        b"[]",
        b"NaN",
    ],
)
def test_provider_sender_request_rejects_invalid_json_frames(frame):
    module = _process_module()

    with pytest.raises(ValueError):
        module.decode_provider_sender_request(frame)


@pytest.mark.parametrize(
    "payload",
    [
        _request_payload(extra=True),
        {key: value for key, value in _request_payload().items() if key != "version"},
        _request_payload(version=True),
        _request_payload(version=2),
        _request_payload(url=7),
        _request_payload(url=""),
        _request_payload(url="https://api.example.test:bad/path"),
        _request_payload(url="https://éxample.test/path"),
        _request_payload(url="https://api%2eexample.test/path"),
        _request_payload(url="https://api.example.test/path\n"),
        _request_payload(headers="authorization"),
        _request_payload(headers=[]),
        _request_payload(headers=[["authorization", "token"]] * 33),
        _request_payload(headers=[["authorization"]]),
        _request_payload(headers=[["bad name", "token"]]),
        _request_payload(headers=[["host", "api.example.test"]]),
        _request_payload(headers=[["authorization", "é"]]),
        _request_payload(headers=[["authorization", "token\r\nInjected: true"]]),
        _request_payload(headers=[["authorization", "x" * 4097]]),
        _request_payload(headers=[["authorization", "one"], ["x-api-key", "two"]]),
        _request_payload(headers=[["content-type", "application/json"]]),
        _request_payload(headers=[["authorization", "token"], ["content-type", "text/plain"]]),
        _request_payload(body_b64="not base64!"),
        _request_payload(body_b64=7),
        _request_payload(body_b64="é"),
        _request_payload(body_b64=""),
        _request_payload(timeout_ms=True),
        _request_payload(timeout_ms=0),
        _request_payload(max_response_bytes=False),
        _request_payload(max_response_bytes=0),
    ],
)
def test_provider_sender_request_rejects_invalid_fields(payload):
    module = _process_module()

    with pytest.raises(ValueError):
        module.decode_provider_sender_request(_request_frame(payload))


@pytest.mark.parametrize(
    "frame",
    [b"\xff", b"not-json", b"[]", b"NaN"],
)
def test_provider_sender_response_rejects_invalid_json_frames(frame):
    module = _process_module()

    with pytest.raises(ValueError):
        module.decode_provider_sender_response(frame, max_response_bytes=128)


def _response_payload(**overrides):
    return {
        "version": 1,
        "status": 200,
        "headers": [["request-id", "req-1"]],
        "body_b64": base64.b64encode(b"{}").decode("ascii"),
        **overrides,
    }


@pytest.mark.parametrize(
    "payload",
    [
        {key: value for key, value in _response_payload().items() if key != "version"},
        _response_payload(extra=True),
        _response_payload(version=True),
        _response_payload(version=2),
        _response_payload(status=True),
        _response_payload(status=99),
        _response_payload(headers="request-id"),
        _response_payload(headers=[["request-id", "req"]] * 4),
        _response_payload(headers=[["request-id"]]),
        _response_payload(headers=[["location", "https://elsewhere.test"]]),
        _response_payload(headers=[["request-id", "x"], ["Request-ID", "y"]]),
        _response_payload(headers=[["request-id", "é"]]),
        _response_payload(headers=[["request-id", "x\nInjected: true"]]),
        _response_payload(headers=[["request-id", "x" * 257]]),
        _response_payload(body_b64=7),
        _response_payload(body_b64="not base64!"),
        _response_payload(body_b64="é"),
        _response_payload(body_b64=base64.b64encode(b"toolong").decode("ascii")),
        _response_payload(body_b64=base64.b64encode(b"12345").decode("ascii")),
    ],
)
def test_provider_sender_response_rejects_invalid_fields(payload):
    module = _process_module()

    with pytest.raises(ValueError):
        module.decode_provider_sender_response(_request_frame(payload), max_response_bytes=4)


def test_provider_sender_rejects_nonbytes_and_non_json_safe_encoder_values():
    module = _process_module()

    with pytest.raises(ValueError, match="must be bytes"):
        module.encode_provider_sender_request(
            url="https://api.example.test/v1/responses",
            headers=(("authorization", "Bearer test"), ("content-type", "application/json")),
            body="{}",
            timeout_ms=100,
            max_response_bytes=128,
        )
    with pytest.raises(ValueError, match="not JSON-safe"):
        module.encode_provider_sender_request(
            url="https://api.example.test/v1/responses",
            headers=(("authorization", "Bearer test"), ("content-type", "application/json")),
            body=b"{}",
            timeout_ms=float("nan"),
            max_response_bytes=128,
        )


def test_provider_sender_response_bounds_frame_and_rejects_invalid_maximum():
    module = _process_module()

    with pytest.raises(ValueError, match="bound"):
        module.decode_provider_sender_response(b"{}", max_response_bytes=True)
    with pytest.raises(ValueError, match="bound"):
        module.decode_provider_sender_response(b"{}", max_response_bytes=0)
    with pytest.raises(ValueError, match="bound"):
        module.decode_provider_sender_response(b" " * 17_000, max_response_bytes=1)


def test_provider_sender_child_main_emits_bounded_response_and_hides_errors(monkeypatch):
    module = _process_module()
    frame = _request_frame(_request_payload())
    stdout = io.BytesIO()
    monkeypatch.setattr(module.sys, "stdin", type("Input", (), {"buffer": io.BytesIO(frame)})())
    monkeypatch.setattr(module.sys, "stdout", type("Output", (), {"buffer": stdout})())
    monkeypatch.setattr(
        module,
        "_perform_https_post",
        lambda _request: module.ProviderSenderResponse(status=200, headers=(), body=b"ok"),
    )

    assert module.main() == 0
    assert module.decode_provider_sender_response(stdout.getvalue(), max_response_bytes=16).body == b"ok"

    monkeypatch.setattr(
        module,
        "_perform_https_post",
        lambda _request: (_ for _ in ()).throw(RuntimeError("secret token must not appear")),
    )
    monkeypatch.setattr(module.sys, "stdin", type("Input", (), {"buffer": io.BytesIO(frame)})())
    assert module.main() == 1
