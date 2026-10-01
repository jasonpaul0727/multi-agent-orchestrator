# Secret Broker 独立进程边界设计

状态：待用户审阅；本文件是设计规格，不代表功能已实现或 P4/V1 已完成。

## 目标

将当前进程内 `AuditedSecretBroker` 接入独立的宿主 Broker 进程，使可信 Model Gateway 可经本机有界 IPC 请求一次性、尝试范围明确的凭证。隔离 Worker、Tool、Verifier 和候选代码不能访问 Broker socket 或 Broker 客户端；Provider 凭证只允许短暂出现在 Broker、可信 Gateway 和固定 Provider sender 的进程内存及明确的 Broker→Gateway→sender 有界 IPC 中。

本切片不发起公共或付费 Provider 调用，不接入真实生产凭证后端，也不实现功能 Worker。默认凭证后端不可用；集成测试使用仅在 Broker 进程内存中的哨兵值。所有不支持该边界的平台都 fail-closed。

## 信任边界与威胁模型

- Worker、工具进程、Verifier 与候选代码视为不可信；它们不得取得 Broker 对象、socket FD、会话 nonce、secret store 或控制面句柄。
- Broker 与 Model Gateway 是宿主可信进程，运行在同一账户下。Broker 使用宿主私有 runtime 目录内的 Unix socket；peer UID 和一次性会话 nonce 用于限定本宿主会话。Unix socket 与这些校验**不**防御同 UID 的恶意宿主进程、宿主账户失陷或有权读取宿主进程内存的攻击者。
- 当前 `SystemdReadOnlyLauncher` 已通过 `InaccessiblePaths=/run/user` 隐藏宿主 runtime socket。Provider sender 也隐藏该路径。`SystemdOverlayCandidateLauncher` 尚无此明确属性，必须在其 profile 加入同等屏蔽并通过实机测试后，才可声称候选代码不可访问 Broker。
- Broker 不执行 Provider HTTP 请求。默认 Model Gateway transport 将已经校验的凭证绑定到 Provider HTTP 认证头，再交给固定 `SystemdProviderSenderLauncher` 子进程执行一次 HTTPS 请求；此 Broker IPC 路径不增加 urllib/thread fallback。该 sender 可接收必要的认证头，但不接收环境变量或 shell 命令；其输入限于现有有界 stdin IPC。
- Python 字符串不能可靠清零。关闭 socket 和释放对象只能缩短凭证驻留时间，不构成内存擦除证明。

## 架构与生命周期

1. 宿主 Gateway 会话通过 `SecretBrokerProcessManager` 启动一个独立 Broker 子进程，并持有 `UnixSocketSecretBroker` client。Broker 的入口固定，不使用 shell；环境采用 allowlist，不继承 Provider key。规则、EventStore 位置及随机会话 nonce 通过受限启动通道传递，不放入命令行或环境变量；nonce 仅驻留于 Gateway/Broker 内存。子进程安装 parent-death signal 并校验启动时记录的 parent PID，避免宿主异常退出后 Broker 留存服务。
2. Broker 在 `$XDG_RUNTIME_DIR` 下创建唯一私有目录（目录模式 `0700`、socket 模式 `0600`），只接受 `AF_UNIX/SOCK_SEQPACKET`。启动前不得盲目删除同名路径；必须检查父目录、所有者、socket 类型和实例身份，遇到 stale/不匹配状态时拒绝启动。
3. Broker 使用固定上限 8 个活动请求处理器和 16 个 pending connection backlog，不为每个连接无界创建线程。每个 socket request/response 都有 16 KiB 帧上限和有限读写期限。管理器只在 Broker 完成 socket bind、模式设置和 readiness handshake 后向 Gateway 暴露 client。系统缺少 Linux 所需 socket/peer-credential 能力、runtime dir 不安全或启动验证失败时，Gateway 不发送 Provider 请求。
4. 每个会话使用不可预测且不复用的目录名，避免跨会话争抢静态 socket 路径。正常关闭时先拒绝新请求、停止监听、等待有限时间内的活动请求结束，再停止并等待 Broker 进程。仅在确认进程退出且 socket 仍是本实例创建的 inode 后删除 socket/私有目录。若停止无法确认，保留路径并将 Broker 标为不可用；不复用或盲删遗留路径。父进程异常退出时子进程必须退出；未能正常清理的旧 runtime 目录不再复用，并在用户 runtime 清理时回收。
5. Broker 对每次凭证读取只使用启动时冻结的规则。规则变化需停止并重建 Broker；本切片不提供动态撤销或规则热更新。

## IPC 契约与授权顺序

请求和响应均是版本化 JSON 单帧，最大 16 KiB，严格拒绝重复 JSON 键、未知/缺失字段、非有限数值、无效 UTF-8、超限字符串和错误版本。单连接恰好处理一个 request/response 后关闭。服务端读取 Linux `SO_PEERCRED` 并校验对端 UID；请求还必须携带当前内存会话 nonce。它们是本地会话约束，不是多用户认证，也不把 caller-supplied `SecretAccessContext` 当作独立身份凭证。

请求只包含非秘密的 `request_id`、Run/Node/Attempt/fencing generation、accepted route、budget reservation、Provider ID/adapter、secret reference、精确 endpoint 和 `model_inference` purpose。Broker 重新验证完整 schema、nonce、peer UID、Provider 配置与冻结 `SecretAccessRule`。Worker 不负责构造或发送该请求。

处理顺序固定为：

1. 校验帧、socket peer、会话 nonce、Run/Provider/ref/endpoint/purpose 规则。
2. 在现有 SQLite EventStore 的 `security` Run stream 原子写入脱敏 `SecretAccessGranted` 或 `SecretAccessDenied`；`request_id` 在 Run 范围内一次性消费。事件只存 Provider ID、route/reservation、reason code 及 secret ref/endpoint 哈希，不存原文或 key。
3. 仅在授权审计成功后读取 `SecretValueStore`。缺失、不可用或格式不合法时写 `SecretCredentialUnavailable`（可写时）并不给凭证。
4. Broker 通过同一 socket 返回绑定 `request_id`、Provider、endpoint、purpose 和合法认证 header name 的凭证响应。Gateway 严格复验响应，再持久化 Provider intent；只有 intent 持久化成功后，才把必要认证头放入现有 sender 的有界 stdin frame。

现有 `EnvironmentSecretStore` 保留为显式进程内原型，但不自动传入 Broker 子进程；新跨进程路径默认使用 unavailable store。只有未来经单独安全评估并明确配置的 keyring/plugin backend 才能提供真实凭证。本切片不得为了测试把哨兵值放到进程环境、argv、启动配置、日志、审计或 artifact；测试专用值只在 Broker 测试进程内存中创建和读取。

## 失败语义

任何 socket 无法安全创建/连接、peer/nonce/scope 不匹配、request 重放、IPC 超时、帧非法或超限、EventStore 审计失败、secret store 不可用或凭证不符合 adapter/header 约束时，Broker 不返回凭证，Gateway 不启动 sender。Gateway 对外只返回脱敏的凭证不可用/交付失败类别，不传播异常文本、key、secret ref 原文、HTTP body 或本机路径。

授权审计已成功而 Broker 在返回凭证前崩溃时，该 `request_id` 视为已消费；同一 ID 重试被拒绝，必须由控制面产生新鲜 Attempt/request ID。审计不可写时不读取 secret store。日志和错误不含请求帧、认证头、Provider body、环境内容或凭证 repr。

## 验收测试

- 单元/契约：合法请求完整字段绑定；错误 peer/nonce、错 Provider/ref/endpoint/purpose、重复 ID、并发同 ID 仅一个成功、重复 JSON 键、未知字段、损坏/超限帧、超时、无效/不可用 secret store、审计失败都拒绝且不返回 key。
- 独立进程：真实 Unix socket client/server 跨进程往返；哨兵值只由 Broker 测试进程内存 store 提供；扫描 Broker/Gateway/Worker/sender `/proc/<pid>/cmdline` 与 `environ`、审计事件、错误、日志和 Artifact，确认均无哨兵值。测试不得声称能检查或清除 Python 进程内存。
- 隔离实测：使用实际 systemd Worker/Tool/Verifier profile 和 overlay candidate profile 尝试访问 Broker socket，必须失败；专门回归验证 overlay 的 `/run/user` 不可见。不能用 mock 替代这项实测。
- 端到端：Gateway → 独立 Broker → 现有 Provider sender → 本机 loopback TLS sink；sink 只接收测试认证头，断言一次请求、正确 endpoint/Attempt binding、sender stop receipt 有效、key 不进入 Worker/Shell/argv/environment/log/audit/Artifact。测试不得连接公共 Provider。
- 全量项目验收继续执行项目覆盖率门（至少 90%）、`compileall`、`pip check`、wheel 构建和 diff 检查；本切片的新增边界必须有针对性覆盖。

## 明确不包含

- 生产 keyring/plugin/HSM backend、密钥轮换/动态撤销、多用户/MFA 身份系统，或抵御同 UID 宿主进程失陷。
- Provider egress 主机 allowlist、DNS rebinding 防护、Provider 权威回执/usage 对账或公共 Provider 实测。
- Worker 的实际 Agent 执行、Tool/Approval/Artifact 发布全链路、自动恢复派发、CLI/MCP 或完整 P4/P5/P7/V1 验收。

## 方案选择与权衡

本方案选择独立本地 Broker daemon、Unix socket 有界 IPC、凭证只返回可信 Gateway，并复用当前固定 Provider sender。相比让 Broker 自己承担 HTTPS，此方案不把 secret policy 与 transport/Gateway 职责合并；相比将 Broker/Gateway 同放一个服务进程，它明确增加了 Broker/Gateway 进程间凭证传递，但 Worker 仍无法直接取得 Broker 接口。其保证范围是隔离 Worker 威胁模型，不是完整宿主账户隔离。
