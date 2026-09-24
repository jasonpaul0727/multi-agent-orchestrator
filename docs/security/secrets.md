# Secret Broker 当前边界

更新时间：2026-09-23

仓库现有 `orchestrator.models.SecretBroker` 是 fail-closed 协议，默认实现 `UnavailableSecretBroker` 不返回凭据。本切片新增 `orchestrator.secrets.AuditedSecretBroker`：对**显式配置**的 Provider secret reference 进行尝试级授权，在同一 Run 的安全事件流中持久化脱敏决定后，才把绑定端点/用途的临时 `ProviderCredential` 返回给可信的 `ProviderModelGateway`。

## 已实现并由测试覆盖

- 每条 `SecretAccessRule` 固定 Provider ID、secret reference、精确 HTTPS endpoint、`model_inference` 用途和允许的 Run ID 集合；关闭的 Provider、其他 Run、其他引用/端点/用途均拒绝。
- broker 要求 Model Gateway 提供 `SecretAccessContext`，绑定 request、Run、Node、Attempt、fencing generation、accepted route 和 budget reservation。相同 Run 内的 request ID 只能使用一次；并发/重放不能再次读取或返回同一授权凭据。
- 对准许和拒绝都写 `SecretAccessGranted`/`SecretAccessDenied`，只含引用与 endpoint 哈希、用途、decision/request ID 和稳定 reason code；引用合法但读取失败时再写 `SecretCredentialUnavailable`。事件不含 API key 或原始引用。授权审计无法提交时不会读取/返回 secret；凭据读取失败或读取结果无效时也不会返回 secret。
- `EnvironmentSecretStore` 只解析 host 显式 allowlist 中的 `env:NAME` 引用；不会遍历/复制环境，也不接受明文配置。keyring/plugin reference 必须由宿主注入另一个 `SecretValueStore` adapter；本仓库不假装这些后端已接入。
- 返回的 `ProviderCredential` 将 provider、endpoint、purpose 与 header 一起绑定；Gateway 再验证 audience、header 类型/名称与值，再发往 Registry 对应 endpoint。repr 会隐藏 key 值。Gateway 不把凭据放入 `ModelRequest`、提示、命令行或模型响应。

## 不能宣称已完成的部分

- 这是**同进程接口与本地 SQLite 审计切片**，Python 对象/私有字段不是进程安全边界。当前 Worker 尚未实现；后续必须让 Worker 子进程拿不到 broker、secret store、credential 和控制目录句柄，再以实测验证。
- Host composition root 必须保护 broker/rule/value-store 注入；`SecretAccessContext` 是内部 API 结构，不是独立认证证明。broker 不能替代已接受路由 authority、Policy Engine 或网络隔离。
- `EnvironmentSecretStore` 读取宿主进程环境中的 API key，Python 字符串无法可靠清零。规则更新目前需由可信宿主重建 broker；没有动态撤销服务、keyring 插件、分布式审计、HSM、MFA 或多租户身份。
- 没有真实 Provider 网络调用验收；离线测试证明引用/用途/Run 范围和审计行为，不能证明 TLS/DNS 重绑定、目标服务器安全、供应商密钥轮换或远端服务可用。
- 未连接 ToolGateway、Worker、Approval UI、CLI/MCP 或完整 Run application service。默认 broker 仍 unavailable；没有显式 host 初始化与规则时，在线调用 fail-closed。

本地测试：`pytest tests/unit/secrets tests/unit/models/test_provider_adapters_transport.py -q`。这些测试不等同于端到端安全验收。
