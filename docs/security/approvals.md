# ApprovalService 当前边界

更新时间：2026-09-26

`orchestrator.approvals.ApprovalService` 是 P4 的内部控制面原语。2026-09-26 起，它通过 `ToolGateway` 接入仓库现有的**固定只读命令**路径：Gateway 可生成精确审批请求，批准后只能由新的 Attempt 绑定并消费 grant，最后写入 EffectReceipt。它仍不是已集成的用户审批产品：请求者身份、认证器、可信 attempt/profile 和 CLI/MCP/UI 都由 host/application 层提供。

## 已实现并由测试覆盖

- 请求固定绑定 Run、Node、发起 attempt/fencing、动作类别、工具、目标/参数哈希、effect-intent 哈希、Policy manifest 哈希、撤销版本及失效时间；请求身份不可复用为不同 scope。
- 审批身份由调用方注入的 authenticator 返回，且必须有对应 `approval:approve`、`approval:deny` 或 `approval:revoke` 权限。服务不接受 agent 文本作为身份凭证。
- Grant 只允许绑定到相同 Run/Node 上、拥有更高 fencing generation 且当前有效的**新 attempt**；不能恢复审批请求来源的旧 attempt。
- 单次消费把 `EffectIntentRecorded`、`ApprovalGrantConsumed` 与 `BudgetReserved` 通过一个共享 SQLite 事务原子追加；重复/并发消费至多有一方成功。估算不得超过审批上限，EffectIntent 的完整内容哈希必须匹配。
- 过期、Policy 版本变化、撤销、失败的 attempt authority、缺失的因果/attempt 上下文均 fail-closed。结果回执绑定到已消费的 effect/attempt，完全相同的回执幂等；不同结果拒绝覆盖。
- 持久化只写审批理由哈希和 Provider idempotency key 哈希；测试断言这些原始值不出现在事件内容中。
- ToolGateway 只在 ApprovalService 与自己共享同一个 SQLite event store、且 host 提供认证请求者和 profile hash 时启用接线。审批 scope 哈希绑定固定只读工具、workspace identity、命令参数、运行限制与冻结 Policy；新 Attempt 必须逐项匹配后才绑定 grant。grant 消费、effect intent、零费用预算预留、Gateway start 和终态 receipt 均有集成测试。
- 这条垂直路径仅适用于 `system.readonly-command`，不是 workspace-write/发布/外部 Provider 的审批接线。调用者需另行通过已认证审批 API批准，并为相同 scope 调度新的已接受 Attempt；Gateway 不恢复旧 Attempt。

## 尚未实现（不能据此宣称安全审批闭环）

- 未配置真实身份提供方、密钥存储、角色目录、MFA 或用户界面。传入的 authenticator 是可信系统边界；接入方必须自行保证不可伪造且能区分调用者。
- 请求创建仍是内部 API：Gateway 的只读命令适配器会从注入的 host 身份回调取得 requester，但请求者对来源 attempt、目标或参数的产品级权限仍要由真实身份/Authority Envelope 校验。
- 尚未被 Worker/Scheduler application service 消费；CLI/MCP 入口、批准后的通知/队列/自动生成新 Attempt 流程仍缺。Gateway 只接受 host 显式传入 grant 与新的当前 Attempt，不恢复旧 `RoutingDecision` 或旧 attempt。
- 尚未集成 Secret Broker、workspace-write/Overlay、CLI/MCP `Authority Envelope`、通知/审批队列或产品级审计查看器。
- SQLite 本地事件事务不是跨主机一致性协议；此实现不构成分布式授权服务或多租户身份系统。

## 本地验证

运行 `pytest tests/unit/approvals/test_service.py tests/unit/budget/test_ledger.py tests/unit/tools/test_gateway.py tests/integration/test_approval_tool_gateway.py tests/contract/test_cross_spec_events.py -q`。这些测试覆盖内部事件/账本契约、并发单次消费，以及固定只读命令的 ApprovalService→ToolGateway 路径；不证明真实身份验证、Worker 隔离、workspace-write/外部 Provider 副作用、跨进程恢复或端到端 UI 流程。
