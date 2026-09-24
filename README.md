# Multi-Agent Orchestrator

一个本地运行的多模型、多 Agent 编排系统。项目以 GPT/Codex 为主要模型，同时支持通过 API Key 接入其他模型厂商；系统会根据任务角色、成本、风险和失败情况选择模型，并在必要时升级到更高能力的模型。

> **当前状态：P1/P2 已实现，P3 首个生命周期与调度控制平面垂直切片已实现；完整编排系统仍不可运行。**
> 实施计划 Task 1–9 已完成，涵盖事件存储、快照恢复、Artifact Store、预算账本、脱敏投影、跨规格事件契约、崩溃/并发测试及打包验收。
> 已完成严格四层配置及 Run 快照、Policy Engine、确定性分类/Planning 冻结、候选成本路由、事件存储驱动的健康熔断/ProbeLease、Recovery Controller，以及三种 Provider codec 和受限 HTTPS transport。
> P3 尚未完整：Run 等待用户和取消门控已实现，但检查点/完整崩溃恢复、失败指纹与恢复阶梯仍待实现；当前没有 Worker/隔离运行时，因此取消 API 只能在收到可信停止回执并完成预算核算后终结 Attempt，不能自行终止 OS 进程。P4 Tool/Isolation/Secret Broker/Approval、P5 Worker/Verifier 和 CLI/MCP 尚未实现；Provider 在线请求默认因无 Secret Broker 而拒绝。
> 本项目**不具备生产就绪状态**。

## V1 目标

- 使用 Python 构建共享编排核心。
- 提供独立的本地 CLI 和 MCP Server，不建设 Web 服务。
- 支持软件开发与文档分析两类任务。
- 支持 Root/Planner、Coder、Document Analyst、Researcher、Tester、Reviewer 和 Director 等 Agent 角色协作。
- 以角色 × 能力 × 预算进行确定性路由；模型通过单一 Gateway adapter 接入，复杂决策与任务执行可选择不同后端。
- 允许 Agent 动态创建子 Agent，同时严格限制并发数、总数量、调用深度、Token 和金额预算。
- 提供 `economic`、`balanced`、`quality` 三个内置预设，以及可完全自定义的 `custom` 预设。
- 支持 OpenAI Responses、Anthropic Messages 和通用 OpenAI-compatible 模型适配器。

Maestro 的性能数字仍是待基准验证的目标，不代表当前实现或生产结果：以可重放工作集测量单功能成本约 `$18 → $7`、Token 约 `90M → 40M`、失败后重复工作减少约 `65%`，并评估约 `15%` 决策任务使用高能力模型、约 `85%` 执行任务使用低成本模型的路由组合。

## 架构原则

系统采用「受控的去中心化 Agent 网络」：Agent 可以协作、委派和细化任务，但不能绕过确定性控制层。

- **编排核心**：CLI 与 MCP Server 共用相同的任务编排能力。
- **确定性控制层**：统一管理运行状态、预算、权限、并发、检查点和审计。
- **Model Gateway**：封装不同模型厂商的调用、重试、路由和升级策略。
- **Tool Gateway**：统一执行权限检查、工作区隔离和高风险动作审批。
- **事件存储**：记录任务轨迹、模型调用、工具调用、Token、费用和证据。
- **任务模型**：V1 使用事件驱动 DAG，并允许在执行期间仅追加或细化任务节点。

## 核心规则

以下两条规则约束所有已实现和后续模块，实现时不得绕过。

### 事件为唯一真源

追加式事件流是系统中唯一的权威事实来源。快照、预算余额、Artifact 元数据、投影、日志和指标**全部是派生视图**，必须能够从事件流重放重建。

因此：

- 任何组件都不得直接修改派生视图来表达状态变化，状态变化只能通过追加事件表达。
- 投影（projection）是只读的，不得回写事件存储，也不得参与调度决策。
- 恢复流程从最近一个合法快照开始重放事件，而不是信任任何已落盘的派生状态。

### SQLite 控制目录

V1 的事件存储实现基于 SQLite WAL，要求一个专用的**控制目录**：

- 该目录由编排器独占，不与 Agent 工作区共享，也不在任何 `workspace-write` 权限边界内。
- 目录内持有事件数据库、快照和内容寻址的 Artifact blob；Agent 无法通过工具网关访问这些路径。
- 事件库必须以 WAL 模式打开，使用显式事务、唯一 stream 版本和幂等键。
- 该目录的完整性即系统的可恢复性边界：目录丢失等同于运行历史丢失。

## 已实现的模块边界

所有模块只对外暴露经 Pydantic 校验的边界对象和小接口，**上层不会拿到数据库句柄**，因此生命周期、路由和权限等后续切片可以依赖稳定契约。

| 模块 | 职责 | 对外契约 |
| --- | --- | --- |
| `orchestrator.config` | 严格有效配置、四层覆盖/来源追踪、原子 reload 与 Run 配置快照 | `EffectiveConfig` · `ResolvedConfig` · `ConfigManager` · `RunConfigSnapshot` · `ModelRegistryManifest` |
| `orchestrator.models` | Tokenizer/FX 成本快照、确定性成本估算、三种 Provider codec 和 fail-closed HTTPS Gateway | `TokenizerSnapshot` · `FXSnapshot` · `ProviderModelGateway` · `OpenAIResponsesAdapter` · `AnthropicMessagesAdapter` · `OpenAICompatibleAdapter` |
| `orchestrator.security` | Policy manifest 与确定性权限判定 | `PolicyManifest` · `PolicyEngine` · `PolicyDecision` |
| `orchestrator.routing` | TaskClassifier、Planning 节点契约、确定性路由、事件驱动健康熔断和恢复授权 | `TaskClassifier` · `PlanningNodeContract` · `ModelRouter` · `HealthController` · `RecoveryController` |
| `orchestrator.identifiers` | 稳定标识符生成 | `new_id()` |
| `orchestrator.persistence` | 追加式事件存储、快照 | `EventDraft` · `StoredEvent` · `SQLiteEventStore` · `SnapshotStore` |
| `orchestrator.artifacts` | 内容寻址的 Artifact 存储与访问控制 | `ArtifactStore` · `ArtifactRecord` · `ArtifactAccessGrant` |
| `orchestrator.budget` | Token 与费用的预留、结算、对账 | `BudgetLedger` · `RunLimit` · `CostEstimate` · `BudgetReservation` · `UsageRecord` · `BudgetBalance` |
| `orchestrator.lifecycle` | Run/Node/Attempt 状态投影、事件重放、追加式 DAG 与图版本校验 | `LifecycleController` · `RunLifecycleState` · `NodeSpec` · `AttemptState` |
| `orchestrator.scheduler` | 路由接纳、最坏成本预留、并发 slot、fencing lease 与未知结果对账 | `Scheduler` · `AcceptedAttempt` · `ConcurrencyLimits` |
| `orchestrator.agents` | 每个模型 attempt 的事件化 Agent 实例、累计数量/深度/活动上限和状态重放 | `AgentRegistry` · `AgentInstance` · `AgentRegistryState` |
| `orchestrator.recovery` | 确定性恢复与不变式校验 | `bootstrap_recovery()` · `recover()` · `recover_aggregate()` |
| `orchestrator.observability` | fail-closed 脱敏观测与只读投影 | `ObservationSink` · `Redactor` · Run/Budget/Cost/Approval/Audit projections |

`orchestrator.config` 已实现完整配置 schema、system default → user global → project → Run 四层解析、逐字段来源追踪、Policy Envelope 单调收紧、selector tombstone/guard 累加、安全 YAML loaders 和 JSON Schema。`ConfigManager` 会先完整构造并验证候选，再以单次原子切换替换活动配置；校验失败保留原配置。Run 启动时将有效配置、注册表及哈希、字段来源和来源标签冻结到 `RunCreated` 事件，并以校验快照作恢复加速；重启时从事件重放原始快照，不受配置文件后续变化影响。并发 reload、Run 启动竞争、重复 Run 创建、失败重试和重启恢复均有测试。当前尚未接入完整生命周期执行。已建立跨规格事件契约，要求因果事件具有运行/节点/尝试/fencing/causation 上下文，校验外部副作用的 intent/receipt 顺序及审批消费与预算预留的一致性。预算独立使用时仍可省略执行上下文。

`orchestrator.models` 增加版本化 Tokenizer/FX snapshots：都以规范化内容生成稳定 SHA-256 ID，并在调用事件时间验证有效期；汇率用整数有理数表示，转换与费用估算均向上取整。三种 codec 已实现 Responses、Anthropic Messages 和 OpenAI-compatible Chat Completions 的请求/响应转换、工具提案、usage 和失败归一化。`UrllibHTTPSTransport` 使用注册 endpoint、关闭环境代理、拒绝重定向并限制响应大小；`ProviderModelGateway` 要求已接受路由验证器与凭据 Broker。当前未实现 P4 Secret Broker，因此默认 Broker fail-closed，不能读取环境变量或发出在线 Provider 请求；mock transport 契约测试不代表在线验收。P3 已把模型预算预留/结算接入 attempt 生命周期；Tool Gateway 与 Worker 执行期集成仍未实现。

`orchestrator.security` 的 Policy Engine 对冻结策略版本作 deny 优先判定，按权限/工具 allowlist 交集处理并返回 scope-bound `PolicyDecision`。`orchestrator.routing` 以固定本地分类器生成不含原文的标签/哈希，Planning 阶段将 GuardRule、预算和角色限制冻结进节点契约；Router 对候选执行授权、能力、健康、凭据和最坏成本过滤，并按成本/配置顺位/模型 ID 稳定排序。Health Controller 将 provider/model 健康变化写入独立 SQLite event streams；跨聚合 ProbeLease 使用控制器 stream 与 aggregate stream 的同一 SQLite 事务完成版本 CAS，可按记录的事件时间重放/过期。Recovery Controller 仅生成有界的重试、同层 fallback、升级或独立角色节点计划；未知调用结果会要求先对账。

P3 首个控制平面切片已实现：`LifecycleController` 将 Run 配置/Registry 哈希冻结到生命周期流，确定性重放 Run/Node/Attempt 状态，校验有界 append-only DAG 和图版本；`Scheduler` 重验路由与 Policy scope，在同一 SQLite 事务里接纳决策、预留预算、占用系统/Run/provider/tool 并发 slot、创建 attempt/fencing lease 和对应 Agent 实例。Agent 累计计数以 `AgentInstanceCreated` 事件重放，结束/重试不扣减；子 Agent 深度由冻结节点上的父实例 ID 派生，创建来源、父关系与深度也在重放时复核；`Active`/`OutcomeUnknown` 均占活动上限。确定性路由选出的 reasoning effort 会绑定到 Attempt、Agent 实例和 Gateway accepted route，避免 Gateway 请求改变已批准等级。租约过期转成 `OutcomeUnknown` 但不释放资源；仅显式 reconciliation 结算/释放并允许有界重试。原子回滚、多连接竞争、迟到结果、Agent 数/深度/活动上限与状态重放均有测试；取消/等待用户/检查点、完整崩溃恢复及 Worker 集成仍未实现。

Run 可事件化进入 `awaiting_user`，只有带请求 ID 和响应哈希的显式回应才能恢复调度；取消先进入 `cancelling`，立即阻止新节点/Attempt 接纳。调度中的 Attempt 在外部运行时出具停止回执、并完成 usage 结算或提供无副作用回执哈希后才能标记 cancelled 和释放 slot；未知结果必须 reconciliation，不能用取消绕过不确定副作用。取消/等待用户目前只有控制平面状态机，不能替代尚未实现的隔离 Worker 进程终止。

Run lifecycle 在初始化、图变更、Run 状态和 Attempt 变化后写入带版本/源事件锚点的快照；重放使用通过 schema/hash/version/anchor 校验的快照并应用后续事件。快照失效或锚点不符会被忽略并从完整 lifecycle 事件流重建。Agent Registry、预算账本、租约与副作用账本的跨流恢复协调仍待实现，所以当前快照能力不是完整 Run crash recovery。

仍未实现：Policy 执行入口、OS 隔离、Tool Gateway、Secret Broker、Approval、Worker、Verifier、CLI 与 MCP Server。因此当前交付仍是可验证的控制与基础库切片，不是可执行的多 Agent 产品。

### 预算生命周期

每次模型或工具调用都经过同一套四态账本，状态变化全部以事件形式落入事件流：

```
reserve ──┬──▶ commit   结算真实用量，按 settlement_key 幂等
          ├──▶ release  调用失败，预留额度归还
          └──▶ unknown  结果不明，保留最坏情况占用，等待对账
```

金额一律使用整数最小货币单位（如美分），并附带 tokenizer、价格与估算器的 snapshot ID，保证可追溯。

## 权限与安全

项目规划提供 `read-only`、`workspace-write` 和 `full-trust` 三档权限。文件访问以声明的工作区边界为基础；删除、安装系统软件、发布、推送和密钥访问等高风险动作可分别配置策略，并由控制层强制执行与审计。

上述权限模型的**执行层尚未实现**，当前仓库中只有对应的事件名称与持久化契约。

## 开发

需要 Python 3.12 或更高版本。

```bash
# 安装开发依赖
python -m pip install -e ".[dev]"

# 运行底座测试
python -m pytest tests/unit tests/contract tests/integration tests/acceptance -q

# 覆盖率检查
python -m coverage run --source=orchestrator -m pytest tests/unit tests/contract tests/integration tests/acceptance -q
python -m coverage report --fail-under=90

# 语法检查与打包
python -m compileall src
python -m build
```

## 设计文档

- [项目上下文](PROJECT_CONTEXT.md)：已确认范围、总体架构和当前进度。
- [任务生命周期与失败恢复](docs/superpowers/specs/2026-09-09-task-lifecycle-design.md)：已于 2026-09-10 批准。
- [预设、DIY 配置与模型路由](docs/superpowers/specs/2026-09-11-presets-routing-design.md)：已于 2026-09-12 批准。
- [权限、安全、隔离与审批](docs/superpowers/specs/2026-09-13-permissions-security-isolation-approval-design.md)：已于 2026-09-22 获书面批准；执行隔离层仍未实现。
- [持久化、成本统计、可观测性与测试](docs/superpowers/specs/2026-09-14-persistence-cost-observability-testing-design.md)：已于 2026-09-14 确认并完成书面审阅。
- [持久化底座实施计划](docs/superpowers/plans/2026-09-14-persistence-cost-observability-testing-implementation-plan.md)：Task 1–9 已完成。
- [完整 V1 产品实施计划](docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md)：P0–P7 分阶段实施与验收路线；当前执行 P3 首个控制平面切片，P0 平台隔离验证仍是 Worker 执行硬门。

## 下一步

1. 继续完成 P3：Agent Registry 累计数量/深度限制、取消/等待用户/检查点与重启崩溃恢复，并把控制平面故障矩阵补齐。
2. P0 隔离验证仍是任何 Worker 执行的硬门；P4 必须完成 Secret Broker、Tool Gateway、Approval 和真实 OS 隔离后，才可启用 live Provider/Worker 调用。
