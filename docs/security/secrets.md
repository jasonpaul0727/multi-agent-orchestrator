# Secret Broker 当前边界

更新时间：2026-10-02

`SecretBrokerProcessManager` 现在启动独立的 Linux Broker 子进程，经私有 Unix
socket 向可信 `ProviderModelGateway` 提供尝试级凭据。生产子进程固定使用
`UnavailableSecretValueStore`：尚未配置生产 secret backend，默认请求只留下
脱敏授权/不可用记录，不返回凭据。进程内 `AuditedSecretBroker` 和显式 allowlist
的 `EnvironmentSecretStore` 仍是内部接口，不会自动成为 daemon 的后端。

## 流程与失败规则

1. 可信宿主以有界 bootstrap pipe 发送冻结的 Provider/rule、EventStore 路径及
   session nonce；bootstrap 没有 credential。子进程只继承固定干净环境，不继承
   宿主 key 环境变量。Linux parent-death signal 和精确 pidfd 约束子进程生命周期。
2. supervisor 检查 runtime root 为宿主所有的 `0700` 目录、就绪 socket 为 `0600`，
   并比对创建时的目录/socket inode。只有就绪且存活的 session 提供 client。
   关闭先禁用 client，再 TERM/KILL 精确子进程；确认退出且 inode 匹配后才清理路径。
   无法确认停止或路径被替换时保留现场，不盲目删除/复用。
3. Gateway 经 `AF_UNIX/SOCK_SEQPACKET` 发单帧严格 version-1 JSON。请求和响应
   各最多 16 KiB；重复/未知字段、坏版本、非法范围、截断帧和不安全 header 值均拒绝。
   服务端最多 8 个 active handler、16 个 pending slot，有有限 I/O deadline。
   每个 handler 独占一个 SQLite connection；校验 peer UID 和 session nonce。
4. `SecretAccessRule` 精确绑定 Provider、secret reference、HTTPS endpoint、
   `model_inference` 用途和允许 Run。`SecretAccessContext` 绑定 request、Run、Node、
   Attempt、fencing generation、accepted route 和 budget reservation。
   相同 Run 的 request ID 只能消费一次，跨连接/重启重放也不能再次返回凭据。
5. Broker 先持久化 `SecretAccessGranted`/`SecretAccessDenied`，才读取 value store。
   审计失败不读取/返回 secret；读取失败/无效记 `SecretCredentialUnavailable` 并
   fail-closed。事件仅有稳定 reason code、标识和引用/endpoint 哈希。
6. Broker encoder 和 Gateway client 核验 Provider adapter 对应的精确认证 header；
   client 再核验响应 request/Provider/endpoint/用途绑定，Gateway 重验 credential scope。
   Gateway 先持久化 Provider intent，再经固定 systemd sender 的有界 stdin 发送认证头。
   sender 以 HTTPS 发请求并返回与调用绑定的停止回执；未知结果/未确认停止不会作为成功。

临时凭据只经过 Broker、可信 Gateway 和固定 sender 的 IPC/内存，不加入 ModelRequest、
提示、一般 Worker/Shell 的 argv/environment、事件、日志或 Artifact。
Python 字符串内存不保证可靠清零；下面的实测也不声称检查/清除进程内存。

## 已有进程与网络实测

`tests/integration/test_secret_broker_process.py` 的测试专用子进程在自身内存生成随机
`maestro-test-secret-` sentinel。宿主不会通过 argv、环境或 bootstrap 注入 sentinel。
测试经过真实 Unix Broker/client、`ProviderModelGateway`、真实 systemd sender 和
绑定 `127.0.0.1` 的 TLS sink；sink 以新 SQLite connection 确认 Provider intent 已提交，
再释放响应。检查唯一 Authorization header、正确 usage、known-success 及空 cgroup 回执。
重放同一调用不会产生第二次 HTTP 请求。

持有 sink 响应期间，测试读取宿主、Broker、存活 sender 的 `/proc/<pid>/cmdline` 与
`environ`；成功后检查 SQLite main/WAL/SHM、捕获宿主日志/stdout/stderr 和测试目录的
生成文件均无 sentinel。检查范围仅为这些元数据/持久输出，不包括内存、系统日志服务、
未实现的 Worker/审批 UI 或未部署的远端服务。

真实 socket 负例覆盖错误 nonce、错误 endpoint rule、不可用审计库、Broker 在 Gateway
等待审计时死亡和 child 重启后的 request replay。另以测试专用真实 packet socket
发送 request/Provider/endpoint/用途/header 绑定错误的回复，验证 client/Gateway 拒绝。
这些路径均无 Provider intent/成功记录、无 sink Authorization，也不因重试启动第二调用。
sender 的 malformed response、取消与未确认停止复用既有 sender/transport 测试。

`tests/integration/test_overlay_candidate_launcher.py` 在真实隔离候选中验证宿主
Broker runtime/socket 和 `/run/user` 不可访问。固定 read-only tool 与候选写 profile
拒绝这些宿主路径；这份证据不等于尚未实现的功能性 Worker 验收。

`SystemdProviderSenderLauncher(ca_bundle_path=...)` 是可信宿主的显式测试 CA 选项。
launcher 安全读取公共 CA 有界快照，再以只读方式挂载到固定 sender。CA/host 路径不能
来自 Worker、Provider request 或 ambient environment；TLS 私钥只在临时 host 测试目录。
未传选项时仍使用系统默认 TLS roots，未配置的自签 CA 被 live 测试拒绝。

验收命令：

```bash
python3 -m pytest tests/unit/secrets tests/integration/test_secret_broker_process.py tests/integration/test_overlay_candidate_launcher.py tests/integration/test_systemd_provider_sender.py -q
python3 -m pytest --cov=orchestrator --cov-report=term-missing --cov-fail-under=90
```

2026-10-02 实测：完整定向进程/隔离/sender 套件 129 passed，另有新增 child contract
18 passed；最终全量 1,475 passed，总覆盖率 90.06%，无跳过的 live-systemd 检查。
首次全量覆盖率为 89.77%，补齐 descriptor/default-backend 行为测试后恢复门槛；一次
既有 overlay 时限测试的临时失败及隔离重跑/最终全量通过证据保留在 Task 6 报告。
支持环境为现有 Linux/WSL2 systemd user manager。非 Linux/缺少隔离能力时
fail-closed，没有无隔离 fallback。

## 威胁模型和仍未交付的部分

- 保护目标是被隔离的候选/Worker。私有 runtime、UID 和 nonce 不承诺抵御同 UID
  宿主进程失陷；可信 Gateway、规则/Provider 注入与 host composition root 仍属信任边界。
- context 不是独立身份认证证明，不能替代 accepted-route authority、Policy Engine 或
  网络隔离。生产身份/Authority Envelope、动态撤销、keyring/plugin、HSM、多租户均未完成。
- 生产 Broker backend 默认 unavailable；测试 store 只存在 `tests/support`，生产 manager
  不接受 value-store 参数。没有真实/付费 Provider 调用或供应商密钥轮换/权威对账验收。
- Worker/Verifier、完整 Scheduler/application service、CLI/MCP 的自动接线及崩溃恢复
  尚未完成。本切片不完成整体 P4 或 Maestro V1，不解除产品 workspace-write 禁用。
- 成本、Token 和重复工作改善目标仍无真实付费 Provider 基准证据。
