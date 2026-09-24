# ApprovalService 当前边界

更新时间：2026-09-23

`orchestrator.approvals.ApprovalService` 是 P4 的内部控制面原语，不是已集成的用户审批功能。它以共享 `SQLiteEventStore` 作为安全事件与预算账本的单一事务边界，记录 hash-scoped 请求、审批/拒绝/撤销、grant 到新 attempt 的一次性绑定，以及外部 effect 的意图、授权消费、预算预留和结果回执。

## 已实现并由测试覆盖

- 请求固定绑定 Run、Node、发起 attempt/fencing、动作类别、工具、目标/参数哈希、effect-intent 哈希、Policy manifest 哈希、撤销版本及失效时间；请求身份不可复用为不同 scope。
- 审批身份由调用方注入的 authenticator 返回，且必须有对应 `approval:approve`、`approval:deny` 或 `approval:revoke` 权限。服务不接受 agent 文本作为身份凭证。
- Grant 只允许绑定到相同 Run/Node 上、拥有更高 fencing generation 且当前有效的**新 attempt**；不能恢复审批请求来源的旧 attempt。
- 单次消费把 `EffectIntentRecorded`、`ApprovalGrantConsumed` 与 `BudgetReserved` 通过一个共享 SQLite 事务原子追加；重复/并发消费至多有一方成功。估算不得超过审批上限，EffectIntent 的完整内容哈希必须匹配。
- 过期、Policy 版本变化、撤销、失败的 attempt authority、缺失的因果/attempt 上下文均 fail-closed。结果回执绑定到已消费的 effect/attempt，完全相同的回执幂等；不同结果拒绝覆盖。
- 持久化只写审批理由哈希和 Provider idempotency key 哈希；测试断言这些原始值不出现在事件内容中。

## 尚未实现（不能据此宣称安全审批闭环）

- 未配置真实身份提供方、密钥存储、角色目录、MFA 或用户界面。传入的 authenticator 是可信系统边界；接入方必须自行保证不可伪造且能区分调用者。
- 请求创建仍是内部 API：尚未验证请求者对来源 attempt、目标或参数的权限，也没有与 Policy Engine/ToolGateway 自动生成待审批项的接线。
- `ApprovalService` 尚未被 `ToolGateway` 或 Worker 消费；没有 Gateway 侧批准后安全恢复/重新调度流程。它不恢复旧 `RoutingDecision` 或旧 attempt，也不执行任何 effect。
- 尚未集成 Secret Broker、workspace-write/Overlay、CLI/MCP `Authority Envelope`、通知/审批队列或产品级审计查看器。
- SQLite 本地事件事务不是跨主机一致性协议；此实现不构成分布式授权服务或多租户身份系统。

## 本地验证

运行 `pytest tests/unit/approvals/test_service.py tests/unit/budget/test_ledger.py tests/contract/test_cross_spec_events.py -q`。该测试证明内部事件/账本契约和并发单次消费性质，不证明真实身份验证、Worker 隔离、外部 Provider 副作用、跨进程恢复或端到端 UI 流程。
