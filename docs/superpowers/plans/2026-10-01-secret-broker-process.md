# Secret Broker Process Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the in-process-only credential lookup path with a separately supervised local Secret Broker process that returns one exact-scope credential only to the trusted Model Gateway.

**Architecture:** Keep `AuditedSecretBroker` as the policy and audit authority, and put a strict, one-request-per-connection `AF_UNIX/SOCK_SEQPACKET` boundary around it. A host-owned process manager sends frozen non-secret configuration through an inherited pipe, creates a private per-session runtime directory, and exposes a client only after the child proves readiness. The production child has an unavailable value store by default; an in-memory sentinel store exists only in test setup. The existing Gateway and fixed systemd Provider sender remain responsible for Provider intent and HTTPS respectively.

**Tech Stack:** Python 3.12, stdlib `socket`/`subprocess`/`ssl`/`sqlite3`, existing Pydantic models and SQLite EventStore, Linux `SO_PEERCRED`, systemd user transient services, pytest.

**Spec:** `docs/superpowers/specs/2026-10-01-secret-broker-process-design.md`

## Global Constraints

- 本切片不发起公共或付费 Provider 调用，不接入真实生产凭证后端，也不实现功能 Worker。
- 默认凭证后端不可用；集成测试使用仅在 Broker 测试进程内存中的哨兵值。
- 所有不支持该边界的平台都 fail-closed。
- Broker 不执行 Provider HTTP 请求；只允许固定 `SystemdProviderSenderLauncher` 执行一次 HTTPS 请求。
- 每个 socket request/response 都有 16 KiB 帧上限和有限读写期限。
- Broker 使用固定上限 8 个活动请求处理器和 16 个 pending connection backlog。
- Broker 目录模式必须为 `0700`、socket 模式必须为 `0600`；不得盲目删除或复用 stale 路径。
- sender CA bundle 默认未设置；默认系统 TLS trust roots 不变；配置路径只能来自受信任宿主配置并只读提供。
- 凭证不得进入 Worker、Tool、Verifier、候选代码、Shell、argv、环境、日志、审计或 Artifact；不得声称 Python 字符串可可靠清零。
- 不得声称本边界抵御同 UID 宿主进程失陷、宿主账户失陷或有权读取宿主进程内存的攻击者。
- 现仓库没有生产 Gateway composition root；本切片交付可由受信任宿主持有的 manager/session API 与集成验收，不新增 CLI、Worker 或完整 Run 启动器。
- 全量测试覆盖率门槛至少为 90%；必须完成 `compileall`、`pip check`、wheel 构建与 diff 检查。

## Review Focus

- 同一 Run/request ID 的并发重放，或 Broker 重启后的重放：最多一次返回凭证；将并发与重启回放测试放入 Task 2。
- runtime 目录/socket 不安全、stale 路径、启动握手失败或 stop 未确认：不得暴露 client、盲删路径或复用 inode；将生命周期失败测试放入 Task 3。
- 错 peer UID/session nonce、Provider/ref/endpoint/purpose/Run 或 response request binding：不得读取 value store 或返回凭证；将授权失败矩阵放入 Task 1、Task 2 和 Task 3。Attempt/route/reservation 在 IPC context 与脱敏 audit event 中保留原绑定；同 UID 宿主攻击者不在此服务端身份模型内。
- 请求、凭证、异常或进程退出过程中意外泄漏 sentinel：任何 argv/environment/log/audit/Artifact 都不得包含 sentinel；将泄漏扫描放入 Task 6。
- 缺失或错误 CA bundle、非可信输入试图改变 CA 路径，以及 sender 默认 TLS：只允许宿主显式 CA 配置改变该次 trust roots，默认行为保持不变；将失败和 TLS sink 测试放入 Task 5。

---

## 文件职责图

- `src/orchestrator/secrets/ipc.py`：固定版本的 request/response 数据结构、严格 JSON codec、16 KiB 帧边界和脱敏 repr；不做授权或 I/O。
- `src/orchestrator/secrets/server.py`：SOCK_SEQPACKET listener、peer UID 与 nonce 检查、有界并发、冻结 Provider/rule 校验、逐请求 EventStore 与既有 `AuditedSecretBroker` 调用。
- `src/orchestrator/secrets/_broker_child.py`：唯一生产 Broker 子进程入口；从继承的 bootstrap fd 读配置、安装 parent-death signal、启动 server 并通过 readiness fd 报告成功；不接受 key 或任意模块/命令。
- `src/orchestrator/secrets/process.py`：宿主 runtime 目录校验、bootstrap IPC、固定 child 启停与 inode 清理、对 Gateway 返回 `SecretBrokerProcessSession`。
- `src/orchestrator/secrets/client.py`：`UnixSocketSecretBroker`，实现 Gateway 现有异步 `SecretBroker` 协议并验证响应绑定与认证 header。
- `src/orchestrator/secrets/broker.py`：新增显式 unavailable store；保留现有审计先于读取与规则行为。
- `src/orchestrator/secrets/__init__.py`：只导出 Gateway 宿主组合所需的 public manager/session/client/rule API，不导出 child 内部函数。
- `src/orchestrator/isolation/overlay_launcher.py`：候选 unit 增加 `InaccessiblePaths=-/run/user`，不放宽现有 profile。
- `src/orchestrator/isolation/provider_sender.py`：可选、宿主配置的只读 CA bundle 输入和对应 BindReadOnlyPaths；默认不改变 profile。
- `src/orchestrator/runtime/provider_sender_process.py`：接收仅由固定 launcher 添加的 CA 路径，并在未配置时保持 `ssl.create_default_context()`。
- `tests/unit/secrets/test_ipc.py`、`test_server.py`、`test_process.py`：契约、授权和生命周期边界。
- `tests/integration/test_secret_broker_process.py`：真实子进程、Gateway、sender 与本机 TLS sink 的端到端验收和 sentinel 扫描。
- `tests/support/secret_broker_child.py`：仅供集成测试启动的 child，自己生成内存 sentinel；生产 manager 不导入或调用它。
- `tests/integration/test_overlay_candidate_launcher.py`、`tests/unit/isolation/test_overlay_launcher.py`：候选层的 runtime 隐藏 profile 回归与实测。
- `tests/unit/isolation/test_provider_sender.py`、`tests/unit/runtime/test_provider_sender_process.py`、`tests/integration/test_systemd_provider_sender.py`：CA 配置验证与 sender TLS 行为。
- `docs/security/secrets.md`：记录已实现进程边界和仍未支持的生产 secret backend/威胁模型。
- `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`：只更新此 P4 slice 的状态与证据，不把 P4 等同于完整 V1。

## Task 1: Freeze the Broker IPC contract

**Files:**
- Create: `src/orchestrator/secrets/ipc.py`
- Modify: `src/orchestrator/secrets/__init__.py`
- Test: `tests/unit/secrets/test_ipc.py`

**Interfaces:**
- `SecretBrokerRequest` is immutable and contains `version: int`, `session_nonce: str`, `secret_ref: str`, `provider_id: str`, `endpoint: str`, `purpose: str`, and `context: SecretAccessContext`. Provider adapter/configuration is not caller data; it is frozen in the server bootstrap map.
- `SecretBrokerResponse` is immutable and contains `version`, `request_id`, `provider_id`, `endpoint`, `purpose`, `status: Literal["credential", "denied", "unavailable"]`, `header_name: str | None`, and `credential_value: str | None`. Request reprs redact the session nonce; credential-bearing response reprs redact the header value.
- `encode_secret_broker_request(request) -> bytes`, `decode_secret_broker_request(frame: bytes) -> SecretBrokerRequest`, `encode_secret_broker_response(response, *, adapter: ProviderAdapter | None = None) -> bytes`, and `decode_secret_broker_response(frame: bytes) -> SecretBrokerResponse` accept one strict JSON object of at most 16,384 bytes. Credential responses require the trusted frozen `ProviderAdapter`; the encoder checks the exact adapter/header mapping without adding adapter data to the wire. Non-credential responses with no header or credential may omit it. Every decoder rejects duplicate/unknown/missing keys, invalid UTF-8, non-finite numbers, unsupported versions, trailing JSON content, invalid identifiers, and values outside field bounds.
- `MAX_SECRET_BROKER_FRAME_BYTES == 16_384`; codec errors are generic and never include the frame or field values.
- `_REQUEST_FIELDS: frozenset[str]` and `_RESPONSE_FIELDS: frozenset[str]` define the complete version-1 wire keys; no decoder accepts an extension field.
- Internal codec helpers are `_decode_exact_json_object(frame: bytes, *, max_bytes: int) -> dict[str, object]`, `_validate_request_payload(payload: dict[str, object]) -> SecretBrokerRequest`, and `_validate_response_payload(payload: dict[str, object]) -> SecretBrokerResponse`.

- [ ] **Step 1: Write failing codec contract tests.**

```python
def test_secret_broker_codec_rejects_duplicate_keys_unknown_fields_and_oversize():
    from orchestrator.secrets.ipc import (
        MAX_SECRET_BROKER_FRAME_BYTES,
        decode_secret_broker_request,
    )

    with pytest.raises(ValueError, match="duplicate"):
        decode_secret_broker_request(b'{"version":1,"version":1}')
    with pytest.raises(ValueError, match="unexpected"):
        decode_secret_broker_request(b'{"version":1,"extra":true}')
    with pytest.raises(ValueError, match="bound"):
        decode_secret_broker_request(b" " * (MAX_SECRET_BROKER_FRAME_BYTES + 1))
```

Also round-trip one fully bound request/response, supplying the trusted `adapter="openai_responses"` when encoding its credential response; reject unsupported version and invalid response fields, and assert `repr(response)` does not contain a generated credential value. Include a valid but adapter-mismatched header case. The client-level mismatch of response request/provider/endpoint/purpose is tested in Task 3 where the original request is available for comparison.

- [ ] **Step 2: Run the codec tests and verify they fail on missing symbols.**

Run: `python3 -m pytest tests/unit/secrets/test_ipc.py -q`

Expected: collection or assertion failure only because the new IPC module/API is absent. Fix test fixture/import errors before implementing.

- [ ] **Step 3: Implement immutable DTOs and strict bounded codecs.**

```python
MAX_SECRET_BROKER_FRAME_BYTES = 16_384

def decode_secret_broker_request(frame: bytes) -> SecretBrokerRequest:
    payload = _decode_exact_json_object(frame, max_bytes=MAX_SECRET_BROKER_FRAME_BYTES)
    version = payload.get("version")
    if set(payload) != _REQUEST_FIELDS or isinstance(version, bool) or version != 1:
        raise ValueError("Secret Broker request schema is invalid")
    return _validate_request_payload(payload)
```

Use `object_pairs_hook` to reject duplicate keys; catch JSON/Unicode/recursion failures without echoing raw input; revalidate `SecretAccessContext` through its Pydantic model. Credential response encoding requires the trusted keyword-only `ProviderAdapter` and checks the exact adapter/header mapping before JSON serialization. The adapter is not a response DTO field and is never serialized.

- [ ] **Step 4: Run codec tests and verify schema, bounds and repr checks pass.**

Run: `python3 -m pytest tests/unit/secrets/test_ipc.py -q`

Expected: PASS; no test may print frame bytes, nonce, or credential values.

- [ ] **Step 5: Commit and push Task 1.**

```bash
git add src/orchestrator/secrets/ipc.py src/orchestrator/secrets/__init__.py tests/unit/secrets/test_ipc.py
git commit -m "feat: define Secret Broker IPC contract"
git push origin codex/p3-systemd-termination-receipts
```

## Task 2: Add the bounded Unix socket Broker server

**Files:**
- Create: `src/orchestrator/secrets/server.py`
- Modify: `src/orchestrator/secrets/broker.py`
- Modify: `src/orchestrator/secrets/__init__.py`
- Test: `tests/unit/secrets/test_server.py`

**Interfaces:**
- `UnavailableSecretValueStore.read(secret_ref: str) -> None` is the production default and never consults process environment.
- `UnixSecretBrokerServer(*, socket_path: Path, session_nonce: str, event_store_path: Path, rules: Sequence[SecretAccessRule], providers: Mapping[str, ProviderSpec], value_store: SecretValueStore, expected_uid: int, max_handlers: int = 8, backlog: int = 16, io_timeout_seconds: float = 10.0)` accepts only Linux `AF_UNIX/SOCK_SEQPACKET`; constructor validation rejects `max_handlers > 8` or `backlog > 16`. Production construction supplies `UnavailableSecretValueStore`; only test setup injects an in-memory store.
- `UnixSecretBrokerServer.serve_forever(*, readiness_fd: int | None = None) -> None` creates the socket at mode `0600`, checks `SO_PEERCRED` UID and session nonce, reads exactly one bounded frame per connection, and schedules work on at most 8 handlers. The listener backlog is 16; read/write deadlines are finite.
- Admission uses a 24-slot bound (8 running handlers plus at most 16 queued accepted sockets); if full, close/reject the new connection instead of growing an executor queue or thread count.
- `UnixSecretBrokerServer.socket_path -> Path` exposes only the non-secret host pathname. `_recv_one_frame(connection: socket.socket) -> bytes`, `_peer_uid(connection: socket.socket) -> int`, `_handle_one_connection(connection: socket.socket) -> None`, and `_send_result(connection: socket.socket, response: SecretBrokerResponse, *, adapter: ProviderAdapter | None = None) -> None` are the bounded one-connection operations. Credential responses pass `provider.adapter` from the frozen Provider map to the Task 1 encoder; denied/unavailable responses without credentials may omit it. The readiness fd receives a four-byte big-endian length plus one JSON record of at most 1 KiB only after bind/chmod/self-check.
- `_denied_response(request: SecretBrokerRequest) -> SecretBrokerResponse` and `_bound_response(request: SecretBrokerRequest, provider: ProviderSpec, credential: ProviderCredential | None) -> SecretBrokerResponse` construct exact-scope results; neither includes raw secret references in denial text or events.
- Each request resolves `provider_id` only from the frozen non-secret `providers` map, opens a new thread-local `SQLiteEventStore(event_store_path)`, calls `AuditedSecretBroker.acquire_provider_credential(...)`, closes the store, and returns only a bound IPC response. Bootstrap contains only rules and public Provider descriptors (`id`, `adapter`, `endpoint`, `secret_ref`, `enabled`), never a secret value. Audit failure, store failure, request replay, or validation error returns no credential.
- `close() -> None` stops accepting new work and drains active handlers for a finite grace period. The request ID is consumed durably by the existing `secret-access:<request_id>` Run-stream idempotency key.

- [ ] **Step 1: Write failing server authorization and audit-order tests.**

```python
def test_server_audits_before_reading_and_rejects_wrong_nonce(tmp_path):
    store_path = tmp_path / "events.db"
    with SQLiteEventStore(store_path):
        pass
    provider = ProviderSpec(
        id="test-provider", adapter="openai_compatible",
        endpoint="https://provider.example.test/v1",
        secret_ref="env:MAESTRO_TEST_KEY", enabled=True,
    )
    rule = SecretAccessRule(
        provider_id=provider.id, secret_ref=provider.secret_ref,
        endpoint=provider.effective_endpoint, purpose="model_inference",
        allowed_run_ids=frozenset({"run-1"}),
    )
    observed = []

    class AuditObservingStore:
        def read(self, secret_ref):
            reader = SQLiteEventStore(store_path)
            try:
                observed.extend(reader.read_stream("security", "run-1"))
            finally:
                reader.close()
            return "sentinel-" + "x" * 16

    server = UnixSecretBrokerServer(
        socket_path=tmp_path / "broker.sock", session_nonce="a" * 64,
        event_store_path=store_path, rules=(rule,), providers={provider.id: provider},
        value_store=AuditObservingStore(), expected_uid=os.getuid(),
    )
    ready_read, ready_write = os.pipe()
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"readiness_fd": ready_write}, daemon=True,
    )
    thread.start()
    os.close(ready_write)
    assert json.loads(os.read(ready_read, 1_024))["status"] == "ready"
    os.close(ready_read)
    context = SecretAccessContext(
        request_id="request-1", run_id="run-1", node_id="node-1",
        attempt_id="attempt-1", fencing_generation=1,
        accepted_route_id="route-1", budget_reservation_id="reservation-1",
    )
    request = SecretBrokerRequest(
        version=1, session_nonce="a" * 64, secret_ref=provider.secret_ref,
        provider_id=provider.id, endpoint=provider.effective_endpoint,
        purpose="model_inference", context=context,
    )
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
        client.connect(str(server.socket_path))
        client.sendall(encode_secret_broker_request(request))
        assert decode_secret_broker_response(client.recv(16_385)).status == "credential"
    assert observed[-1].event_type == "SecretAccessGranted"
    assert "sentinel-" not in repr(observed[-1].payload)
    denied = replace(request, session_nonce="0" * 64)
    assert _exchange_one_frame(server.socket_path, denied).status == "denied"
    assert len(observed) == 1
    server.close()
    thread.join(timeout=2)
    assert not thread.is_alive()
```

In `test_server.py`, define `_exchange_one_frame(socket_path: Path, request: SecretBrokerRequest) -> SecretBrokerResponse` using one fresh SOCK_SEQPACKET connection and the Task 1 codec. Add cases for wrong peer UID, Provider/ref/endpoint/purpose/Run mismatch, malformed Attempt context, audit write failure (store `read` is never called), unavailable/invalid value, request replay, and two simultaneous connections with the same request ID (at most one credential response).
Also hold all 8 handler slots and 16 pending slots, then assert the next connection is rejected without increasing handler/future counts.

- [ ] **Step 2: Run the new server tests and confirm missing server behavior fails.**

Run: `python3 -m pytest tests/unit/secrets/test_server.py -q`

Expected: FAIL because `UnixSecretBrokerServer` and `UnavailableSecretValueStore` do not exist; there must be no unrelated fixture/import failures.

- [ ] **Step 3: Implement request handling with exact scope and audit-before-read.**

```python
def _handle_one_connection(self, connection: socket.socket) -> None:
    peer_uid = _peer_uid(connection)
    request = decode_secret_broker_request(_recv_one_frame(connection))
    if peer_uid != self.expected_uid or request.session_nonce != self.session_nonce:
        self._send_result(connection, _denied_response(request))
        return
    provider = self.providers.get(request.provider_id)
    if provider is None:
        self._send_result(connection, _denied_response(request))
        return
    events = SQLiteEventStore(self.event_store_path)
    try:
        broker = AuditedSecretBroker(
            event_store=events, value_store=self.value_store, rules=self.rules,
        )
        credential = asyncio.run(broker.acquire_provider_credential(
            secret_ref=request.secret_ref, provider=provider, endpoint=request.endpoint,
            purpose=request.purpose, context=request.context,
        ))
    finally:
        events.close()
    self._send_result(
        connection,
        _bound_response(request, provider, credential),
        adapter=provider.adapter,
    )
```

The real implementation must remain synchronous inside bounded worker threads; do not share a thread-affine SQLite store across handlers. Use `recvmsg`/`MSG_TRUNC` or equivalent to detect oversized SOCK_SEQPACKET frames, set socket deadlines, close each accepted socket after one response, and never log request/response data or exception text. Keep `AuditedSecretBroker` as the only scope/audit policy implementation.

- [ ] **Step 4: Run server, existing Broker, and EventStore tests.**

Run: `python3 -m pytest tests/unit/secrets tests/unit/persistence/test_sqlite_event_store.py -q`

Expected: PASS, including one-shot concurrency, audit-before-read, and every incorrect-scope case.

- [ ] **Step 5: Commit and push Task 2.**

```bash
git add src/orchestrator/secrets/server.py src/orchestrator/secrets/broker.py src/orchestrator/secrets/__init__.py tests/unit/secrets/test_server.py
git commit -m "feat: serve audited secrets over bounded Unix IPC"
git push origin codex/p3-systemd-termination-receipts
```

## Task 3: Add the Gateway client and supervised Broker process lifecycle

**Files:**
- Create: `src/orchestrator/secrets/client.py`
- Create: `src/orchestrator/secrets/process.py`
- Create: `src/orchestrator/secrets/_broker_child.py`
- Modify: `src/orchestrator/secrets/__init__.py`
- Test: `tests/unit/secrets/test_process.py`
- Test: `tests/integration/test_secret_broker_process.py`

**Interfaces:**
- `UnixSocketSecretBroker(*, socket_path: Path, session_nonce: str, timeout_seconds: float = 10.0)` implements the existing async `SecretBroker.acquire_provider_credential(*, secret_ref, provider, endpoint, purpose, context) -> ProviderCredential | None` contract. It performs blocking socket work with `asyncio.to_thread`, sends one request, and verifies request ID, Provider, endpoint, purpose, status and adapter-specific header before constructing `ProviderCredential`.
- `SecretBrokerProcessManager(*, runtime_root: Path, event_store_path: Path, python_executable: Path | None = None)` has `start(*, rules: Sequence[SecretAccessRule], providers: Mapping[str, ProviderSpec]) -> SecretBrokerProcessSession`.
- Bootstrap is a four-byte big-endian length plus one duplicate-safe JSON record capped at 256 KiB; an oversized rules/provider set fails before child start. No secret value is part of the bootstrap schema.
- `SecretBrokerProcessSession.client -> UnixSocketSecretBroker`, `.process_pid -> int`, and `.close(timeout_seconds: float = 5.0) -> None` expose the client only after readiness. Calling `close` twice is safe.
- `SecretBrokerProcessSession.socket_path -> Path` and `.is_alive -> bool` expose non-secret host lifecycle state for owner diagnostics/tests, never as Worker context.
- Client internals are `_exchange_one(request: SecretBrokerRequest) -> SecretBrokerResponse`, `_validate_response_binding(response: SecretBrokerResponse, request: SecretBrokerRequest, adapter: ProviderAdapter) -> None`, and `_credential_or_none(response: SecretBrokerResponse) -> ProviderCredential | None`.
- Manager validates Linux, `runtime_root` owner/mode, an existing absolute regular EventStore file, and Unix path length. It creates a random private `0700` child directory, passes rules/provider descriptors/event path/nonce/expected parent PID only through a bounded inherited pipe, and launches one fixed child entrypoint with an explicit clean environment. No key or EnvironmentSecretStore crosses this channel.
- `_broker_child.py` installs `PR_SET_PDEATHSIG=SIGTERM`, checks the configured parent PID, starts `UnixSecretBrokerServer` with `UnavailableSecretValueStore`, and emits a readiness frame only after socket bind, chmod and self-check. On close, the manager stops accepting requests, sends `SIGTERM`, waits at most `timeout_seconds`, escalates once to `SIGKILL`, and waits for process exit. It removes only the socket/directory whose captured inode still matches; unconfirmed stop or mismatched inode leaves the path in place and marks the session unavailable.

- [ ] **Step 1: Write failing manager/client lifecycle tests.**

```python
def test_manager_exposes_client_only_after_ready_and_closes_exact_child(tmp_path):
    events_path = tmp_path / "events.db"
    with SQLiteEventStore(events_path):
        pass
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    provider, rule = _provider_and_rule("run-1")
    session = SecretBrokerProcessManager(
        runtime_root=runtime, event_store_path=events_path,
    ).start(rules=(rule,), providers={provider.id: provider})
    try:
        assert session.client is not None
        assert session.process_pid > 0
        assert asyncio.run(session.client.acquire_provider_credential(
            secret_ref=provider.secret_ref, provider=provider,
            endpoint=provider.effective_endpoint, purpose="model_inference",
            context=_context_for("run-1"),
        )) is None  # production child has no configured value backend
    finally:
        session.close()
    assert not session.is_alive
    assert not session.socket_path.exists()
```

Define `_provider_and_rule(run_id: str) -> tuple[ProviderSpec, SecretAccessRule]` and `_context_for(run_id: str, request_id: str = "request-1") -> SecretAccessContext` in `test_process.py`. Add `test_client_rejects_mismatched_response_binding` using a fake SOCK_SEQPACKET peer that returns a wrong request ID, Provider ID, endpoint, purpose or adapter header in separate cases. Also add deterministic failures for non-Linux, unsafe runtime permissions, missing/symlink EventStore path, socket path too long, readiness timeout, child exit during startup, wrong socket mode/owner, changed socket inode, repeated close, and stop timeout retaining the private directory. Add an integration case with a disposable supervisor process that starts the child and exits without calling `close`; assert the child exits from `PR_SET_PDEATHSIG` within the bounded wait.

- [ ] **Step 2: Run process tests and verify they fail on absent lifecycle APIs.**

Run: `python3 -m pytest tests/unit/secrets/test_process.py -q`

Expected: FAIL only because manager/session/client APIs are absent; use deterministic fake process and socket fixtures here, not a mock in the later live isolation test.

- [ ] **Step 3: Implement strict client exchange and readiness-gated manager.**

```python
async def acquire_provider_credential(self, *, secret_ref, provider, endpoint, purpose, context):
    request = SecretBrokerRequest(
        version=1, session_nonce=self._session_nonce, secret_ref=secret_ref,
        provider_id=provider.id, endpoint=endpoint, purpose=purpose, context=context,
    )
    response = await asyncio.to_thread(self._exchange_one, request)
    _validate_response_binding(response, request, provider.adapter)
    return _credential_or_none(response)
```

Use a fixed `python_executable` and child module, pass a minimal explicit environment (locale, PATH, and trusted code root needed for imports), and pass configuration via a length-limited inherited pipe fd (never argv/environment). In the test, put an environment canary in the parent process and assert it is absent from `/proc/<broker-pid>/environ`. Generate unpredictable nonces and directory names in the host. Read the readiness frame with a finite deadline and withhold `session.client` until path mode, owner, socket type and child liveness are verified.

- [ ] **Step 4: Run manager/client tests plus a real child round-trip and restart replay test.**

Run: `python3 -m pytest tests/unit/secrets/test_process.py tests/integration/test_secret_broker_process.py -k 'manager or restart_replay or response_binding' -q`

Run also: `python3 -m pytest tests/unit/models/test_provider_adapters_transport.py tests/unit/models/test_provider_call_journal.py -q`

Expected: PASS on Linux; `test_manager_exposes_client_only_after_ready_and_closes_exact_child`, `test_restart_replay_is_denied_after_child_restart`, and `test_client_rejects_mismatched_response_binding` pass. The production child returns unavailable, stale static paths are never reused, a durable request ID remains consumed across Broker process restart, and existing Gateway tests preserve the no-intent/no-sender behavior on credential failure.

- [ ] **Step 5: Commit and push Task 3.**

```bash
git add src/orchestrator/secrets/client.py src/orchestrator/secrets/process.py src/orchestrator/secrets/_broker_child.py src/orchestrator/secrets/__init__.py tests/unit/secrets/test_process.py tests/integration/test_secret_broker_process.py
git commit -m "feat: supervise a local Secret Broker process"
git push origin codex/p3-systemd-termination-receipts
```

## Task 4: Hide the Broker runtime from overlay candidates

**Files:**
- Modify: `src/orchestrator/isolation/overlay_launcher.py`
- Modify: `tests/unit/isolation/test_overlay_launcher.py`
- Modify: `tests/integration/test_overlay_candidate_launcher.py`

**Interfaces:**
- Extract `_candidate_service_properties(limits: SandboxLimits) -> tuple[str, ...]` from the inline unit argument construction. It includes `InaccessiblePaths=-/run/user` and preserves all existing properties.
- Worker/Tool/Verifier and Provider sender retain their existing `InaccessiblePaths=-/run/user`; this task does not loosen their profiles or add the Broker socket to any untrusted mount.

- [ ] **Step 1: Write failing profile and live candidate visibility tests.**

```python
def test_overlay_candidate_hides_user_runtime_directory():
    properties = module._candidate_service_properties(SandboxLimits())
    assert "InaccessiblePaths=-/run/user" in properties
```

Extend the existing live candidate test on a host whose `XDG_RUNTIME_DIR` is under `/run/user`:

```python
socket_path = Path(os.environ["XDG_RUNTIME_DIR"]) / f"maestro-test-{uuid.uuid4().hex}.sock"
with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as host_listener:
    host_listener.bind(str(socket_path))
    host_listener.listen(1)
    code = r'''
import json, socket, sys
from pathlib import Path
p = Path(sys.argv[1])
try:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    s.connect(str(p))
    connect_denied = False
except OSError:
    connect_denied = True
print(json.dumps({"broker_socket_hidden": not p.exists(), "broker_connect_denied": connect_denied}))
'''
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    launcher = isolation.SystemdOverlayCandidateLauncher()
    with launcher.launch(workspace, ["/usr/bin/python3", "-c", code, str(socket_path)]) as session:
        result = session.wait()
        assert result.execution.returncode == 0
        assert json.loads(result.execution.stdout) == {
            "broker_connect_denied": True,
            "broker_socket_hidden": True,
        }
```

Keep the existing `unix_denied=True` assertion as a separate regression. Skip only when the host has no `/run/user` runtime path; a skipped test does not satisfy the live acceptance gate.

- [ ] **Step 2: Run the unit test to verify the missing systemd property fails.**

Run: `python3 -m pytest tests/unit/isolation/test_overlay_launcher.py -q`

Expected: FAIL because the candidate profile does not explicitly hide `/run/user` yet.

- [ ] **Step 3: Add the defense-in-depth inaccessible path.**

```python
properties = _candidate_service_properties(limits)
args = [
    "systemd-run", "--user", "--scope", "--slice=app.slice", "--quiet", "--collect",
    *(f"--property={value}" for value in properties),
    "--", "/usr/bin/env", "-i", *env_args,
    "/usr/bin/unshare", "--user", "--map-root-user", "--mount", "--net",
    "--pid", "--fork", "--mount-proc=/proc", "/usr/bin/python3", "-P", "-S", "-m",
    "orchestrator.isolation._overlay_bootstrap", str(stage),
]
```

Add the property to the central candidate profile builder rather than a test-only branch. Keep candidate seccomp's AF_UNIX denial and do not grant any socket FD.

- [ ] **Step 4: Run both deterministic and live overlay tests.**

Run: `python3 -m pytest tests/unit/isolation/test_overlay_launcher.py tests/integration/test_overlay_candidate_launcher.py -q`

Expected: PASS on the configured Ubuntu 24.04 / WSL2 / systemd host. If systemd is unavailable, only the live integration may skip; record that P4 isolation acceptance remains unverified rather than substituting a mock.

- [ ] **Step 5: Commit and push Task 4.**

```bash
git add src/orchestrator/isolation/overlay_launcher.py tests/unit/isolation/test_overlay_launcher.py tests/integration/test_overlay_candidate_launcher.py
git commit -m "security: hide host runtime from overlay candidates"
git push origin codex/p3-systemd-termination-receipts
```

## Task 5: Allow a host-only CA bundle for local sender TLS tests

**Files:**
- Modify: `src/orchestrator/isolation/provider_sender.py`
- Modify: `src/orchestrator/runtime/provider_sender_process.py`
- Modify: `tests/unit/isolation/test_provider_sender.py`
- Modify: `tests/unit/runtime/test_provider_sender_process.py`
- Modify: `tests/integration/test_systemd_provider_sender.py`

**Interfaces:**
- `SystemdProviderSenderLauncher(*, systemd_run: str | None = None, systemctl: str | None = None, ca_bundle_path: str | Path | None = None)` accepts a CA bundle only from the host's explicit constructor configuration. If configured, resolve it strictly, require a supported regular file, stage it read-only at a fixed path, and bind that staged copy into the sender unit. Do not add it to any `ModelRequest`, Provider JSON body, secret IPC, ambient environment, or Worker config.
- The fixed child accepts a launcher-added `--ca-bundle <staged-path>` option that never originates in the Provider request frame. With no option `_perform_https_post` uses exactly `ssl.create_default_context()`; with an option it uses `ssl.create_default_context(cafile=staged_path)` for that request only.
- An unavailable, symlinked, unsupported, non-regular or unreadable configured bundle fails before sender launch; a malformed bundle fails before any network request. The optional public CA certificate is not treated as a credential.

- [ ] **Step 1: Write failing default/configured CA behavior and validation tests.**

```python
def test_sender_tls_context_keeps_system_defaults_unless_host_ca_is_configured(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(ssl, "create_default_context", lambda **kwargs: calls.append(kwargs) or object())
    _make_sender_tls_context(None)
    assert calls[-1] == {}
    ca_bundle = tmp_path / "test-ca.pem"
    ca_bundle.write_text("not parsed by launcher")
    _make_sender_tls_context(ca_bundle)
    assert calls[-1] == {"cafile": str(ca_bundle)}
```

Add launcher tests that reject directories/symlinks/missing paths, assert the staged CA is a read-only bind, and assert `decode_provider_sender_request` cannot accept a request-supplied CA path. Add a child test with a malformed PEM and assert TLS context setup fails before the child calls `HTTPSConnection`.

- [ ] **Step 2: Run the tests and verify the missing CA configuration fails.**

Run: `python3 -m pytest tests/unit/isolation/test_provider_sender.py tests/unit/runtime/test_provider_sender_process.py -k 'ca_bundle or tls_context' -q`

Expected: FAIL because the sender has no host CA option; no test should use a real network endpoint.

- [ ] **Step 3: Implement host-selected CA staging and isolated child TLS context.**

```python
def _make_sender_tls_context(ca_bundle_path: str | None) -> ssl.SSLContext:
    if ca_bundle_path is None:
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=ca_bundle_path)
```

Make the sender launcher append only its validated staged path to its fixed child command, add a `BindReadOnlyPaths` entry for that copy, and reject any CA field in the Gateway-to-launcher frame. Do not alter the default command/environment/profile when `ca_bundle_path is None`.

- [ ] **Step 4: Run unit sender tests and a live loopback TLS sink test.**

Run: `python3 -m pytest tests/unit/isolation/test_provider_sender.py tests/unit/runtime/test_provider_sender_process.py tests/integration/test_systemd_provider_sender.py -q`

Expected: PASS on systemd. The live test creates a temporary local CA/server certificate, trusts it only through the host option, observes exactly one Authorization header at the TLS sink, and makes no public Provider call.

- [ ] **Step 5: Commit and push Task 5.**

```bash
git add src/orchestrator/isolation/provider_sender.py src/orchestrator/runtime/provider_sender_process.py tests/unit/isolation/test_provider_sender.py tests/unit/runtime/test_provider_sender_process.py tests/integration/test_systemd_provider_sender.py
git commit -m "test: support host-only CA for sender TLS sink"
git push origin codex/p3-systemd-termination-receipts
```

## Task 6: Prove cross-process Gateway-to-sender behavior and update security evidence

**Files:**
- Create: `tests/support/secret_broker_child.py` (test-only Broker child that generates its own random sentinel in memory)
- Modify: `tests/integration/test_secret_broker_process.py` (TLS sink, fixtures, complete process/Gateway cases)
- Modify: `docs/security/secrets.md`
- Modify: `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`
- Test: `tests/unit/secrets/`, `tests/integration/test_secret_broker_process.py`, `tests/integration/test_overlay_candidate_launcher.py`, `tests/integration/test_systemd_provider_sender.py`

**Interfaces:**
- The integration fixture starts an independent Broker child with a test-only `InMemorySecretValueStore`, a real `UnixSocketSecretBroker`, the existing `ProviderModelGateway`, the real systemd sender, and a loopback TLS sink. Production `SecretBrokerProcessManager.start` continues to use `UnavailableSecretValueStore` and exposes no test value-store parameter.
- `tests/support/secret_broker_child.py` generates `"maestro-test-secret-" + secrets.token_hex(16)` inside the child and keeps it only in its in-memory value store; it returns the value only through the ordinary scoped Broker response. It never accepts a secret in argv, environment, or bootstrap config. `start_test_broker_process(*, runtime_root: Path, event_store_path: Path, provider: ProviderSpec, rule: SecretAccessRule) -> TestBrokerProcessSession` creates/chmods the test runtime directory to `0700` and returns `.client`, `.process_pid`, `.close()`, `.terminate_for_test()`, and `.is_alive`.
- `LoopbackTLSSink(tmp_path: Path)` is a context manager around a standard-library `ThreadingHTTPServer` bound to `127.0.0.1`; it creates a temporary self-signed cert with SAN `localhost` via `openssl`, exposes `.endpoint`, `.ca_bundle`, `.authorization_headers`, `.request_started`, `.release_response`, and a host-only `.intent_checker: Callable[[], bool]`. The handler records `.intent_was_durable = intent_checker()` before responding and returns the existing OpenAI-compatible response shape `{"id":"chatcmpl_1","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1}}`.
- `_intent_visible_from_new_connection(db_path: Path, request: ModelRequest) -> bool` opens/closes a fresh thread-local `SQLiteEventStore` and checks `SQLiteProviderCallJournal.read(request) is not None`; the TLS handler uses it so the test proves intent durability before accepting the HTTP request.
- `local_provider_request(endpoint: str) -> tuple[ModelRegistryManifest, ModelRequest, ProviderSpec, SecretAccessRule]` builds a valid `openai_compatible` Provider with `secret_ref="env:MAESTRO_TEST_KEY"`, a Run-scoped rule, matching registry hash, and a complete `ModelRequest`; `_AcceptedRoutes.is_accepted(request) -> bool` checks equality with the one request created by this builder.
- `_provider_sender_pids() -> tuple[int, ...]` inspects live `/proc/*/cmdline` entries for the fixed `provider_sender_process.py` helper while the TLS sink deliberately holds its response open.
- A successful sink request requires exactly one test authorization header and an already-persisted Provider intent. A denial, audit error, Broker death, invalid response binding or sender stop failure never returns a usable Gateway success.
- Process evidence exposes only sanitized identifiers/pids/path modes needed by tests; it never exposes socket request payloads, nonce or credential reprs.

- [ ] **Step 1: Write the integrated sentinel and crash/restart acceptance test.**

```python
@pytest.mark.asyncio
async def test_gateway_uses_process_broker_and_systemd_sender_without_secret_leaks(tmp_path):
    with LoopbackTLSSink(tmp_path) as sink:
        registry, request, provider, rule = local_provider_request(sink.endpoint)
        with SQLiteEventStore(tmp_path / "events.db"):
            pass
        session = start_test_broker_process(
            runtime_root=tmp_path / "runtime", event_store_path=tmp_path / "events.db",
            provider=provider, rule=rule,
        )
        journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "e2e-events.db"))
        gateway = ProviderModelGateway(
            registry=registry, accepted_route_verifier=_AcceptedRoutes(),
            secret_broker=session.client,
            transport=SystemdProviderHTTPSTransport(
                launcher=SystemdProviderSenderLauncher(ca_bundle_path=sink.ca_bundle),
            ),
            provider_call_journal=journal,
        )
        sink.intent_checker = lambda: _intent_visible_from_new_connection(
            tmp_path / "e2e-events.db", request,
        )
        try:
            invocation = asyncio.create_task(gateway.invoke(request))
            assert await asyncio.to_thread(sink.request_started.wait, 5)
            sentinel = sink.authorization_headers[0].removeprefix("Bearer ")
            assert sentinel.startswith("maestro-test-secret-")
            assert sink.intent_was_durable is True
            assert_no_value_in_process_metadata(
                sentinel, (os.getpid(), session.process_pid, *_provider_sender_pids()),
            )
            sink.release_response.set()
            assert (await invocation).output_text == "ok"
            call = journal.read(request)
            assert call is not None and call.status == "known_success"
            assert call.termination_receipt is not None
            assert len(sink.authorization_headers) == 1
            assert_no_value_in_event_db(sentinel, tmp_path / "events.db")
            assert_no_value_in_event_db(sentinel, tmp_path / "e2e-events.db")
            assert_no_value_in_test_artifacts(sentinel, tmp_path)
        finally:
            sink.release_response.set()
            session.close()
```

Define `assert_no_value_in_process_metadata(sentinel: str, pids: Sequence[int])`, `assert_no_value_in_event_db(sentinel: str, db_path: Path)`, and `assert_no_value_in_test_artifacts(sentinel: str, root: Path)` in the integration test module. Scan only `/proc/<pid>/cmdline` and `/proc/<pid>/environ`, raw SQLite main/WAL/SHM bytes, test logs, and generated artifacts; do not claim to inspect or clear process memory. Add a `terminate_for_test()` case where the Broker child dies while the Gateway waits; separately test invalid nonce, wrong Provider endpoint, audit failure, and replay after restart against the real socket. Reuse the existing sender malformed-response, cancellation, and unconfirmed-stop tests rather than inventing a second sender fault-injection path. Each negative case must prove no unauthorized second call, no leaked value, and no false-success event.

- [ ] **Step 2: Run the focused complete process boundary suite.**

Run: `python3 -m pytest tests/unit/secrets tests/integration/test_secret_broker_process.py tests/integration/test_overlay_candidate_launcher.py tests/integration/test_systemd_provider_sender.py -q`

Expected: PASS on the designated Linux/systemd host, including the real overlay deny and Gateway → Broker → sender → TLS sink path. Any skipped live-systemd check must be recorded as an unmet acceptance item.

- [ ] **Step 3: Update security docs with only proven claims.**

Document the implemented default-unavailable backend, process/session flow, IPC bounds, audit-before-read rule, runtime permissions, candidate denial, optional host-only test CA behavior, no same-UID compromise guarantee, and tests actually run. Keep production keyring/plugin integration and full V1 explicitly incomplete.

- [ ] **Step 4: Run full project acceptance and inspect the final diff.**

Run:

```bash
python3 -m pytest --cov=orchestrator --cov-report=term-missing --cov-fail-under=90
python3 -m compileall -q src
python3 -m pip check
python3 -m pip wheel . --no-deps --wheel-dir /tmp/maestro-secret-broker-wheel
git diff --check
```

Expected: all commands exit successfully, total coverage is at least 90%, and the final diff contains no real credentials, TLS private keys outside temporary test directories, unrelated files, or statements that P4 equals complete V1.

- [ ] **Step 5: Commit and push Task 6; report remaining V1 gaps.**

```bash
git add tests/support/secret_broker_child.py tests/integration/test_secret_broker_process.py docs/security/secrets.md docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md
git commit -m "test: verify Secret Broker process boundary end to end"
git push origin codex/p3-systemd-termination-receipts
```

Before handoff, verify `git status --short --branch` is clean and `git rev-parse HEAD` matches the pushed branch tip. Report any unrun/unsupported live acceptance honestly; do not claim production secret-store support or full Maestro V1 completion.
