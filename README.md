# Multi-Agent Orchestrator

一个本地运行的多模型、多 Agent 编排系统。项目以 GPT/Codex 为主要模型，同时支持通过 API Key 接入其他模型厂商；系统会根据任务角色、成本、风险和失败情况选择模型，并在必要时升级到更高能力的模型。

> **当前状态：持久化、预算、观测与验证底座已完成；整体编排系统尚不可运行。**
> 实施计划 Task 1–9 已完成，涵盖事件存储、快照恢复、Artifact Store、预算账本、脱敏投影、跨规格事件契约、崩溃/并发测试及打包验收。
> 完整 V1 实施已启动：`orchestrator.config` 首批严格策略/Model Registry 契约和 YAML 校验已加入，尚未接入运行时。
> 任务生命周期、模型路由、权限执行和 CLI/MCP 均尚未实现。
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
| `orchestrator.identifiers` | 稳定标识符生成 | `new_id()` |
| `orchestrator.persistence` | 追加式事件存储、快照 | `EventDraft` · `StoredEvent` · `SQLiteEventStore` · `SnapshotStore` |
| `orchestrator.artifacts` | 内容寻址的 Artifact 存储与访问控制 | `ArtifactStore` · `ArtifactRecord` · `ArtifactAccessGrant` |
| `orchestrator.budget` | Token 与费用的预留、结算、对账 | `BudgetLedger` · `RunLimit` · `CostEstimate` · `BudgetReservation` · `UsageRecord` · `BudgetBalance` |
| `orchestrator.recovery` | 确定性恢复与不变式校验 | `bootstrap_recovery()` · `recover()` · `recover_aggregate()` |
| `orchestrator.observability` | fail-closed 脱敏观测与只读投影 | `ObservationSink` · `Redactor` · Run/Budget/Cost/Approval/Audit projections |

`orchestrator.config` 已实现完整配置 schema、system default → user global → project → Run 四层解析、逐字段来源追踪、Policy Envelope 单调收紧、selector tombstone/guard 累加、安全 YAML loaders 和 JSON Schema。`ConfigManager` 会先完整构造并验证候选，再以单次原子切换替换活动配置；校验失败保留原配置。Run 启动时将有效配置、注册表及哈希、字段来源和来源标签冻结到 `RunCreated` 事件，并以校验快照作恢复加速；重启时从事件重放原始快照，不受配置文件后续变化影响。并发 reload、Run 启动竞争、重复 Run 创建、失败重试和重启恢复均有测试。该模块尚未接入 Router 或生命周期执行。已建立跨规格事件契约，要求因果事件具有运行/节点/尝试/fencing/causation 上下文，校验外部副作用的 intent/receipt 顺序及审批消费与预算预留的一致性。预算独立使用时仍可省略执行上下文。

尚未实现：任务生命周期、模型路由、权限执行、CLI 与 MCP Server。因此当前交付是可验证的基础库，不是可执行的多 Agent 产品。

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
- [完整 V1 产品实施计划](docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md)：P0–P7 分阶段实施与验收路线；当前执行 P0 平台隔离能力验证。

## 下一步

1. 继续 P1：完成 tokenizer/FX 估算快照及 Gateway 契约，并补齐 Provider/Model/Price manifest 校验。
2. P0 隔离能力验证仍是任何 Worker 执行的硬门；无法证明的能力必须失败关闭。之后按计划推进路由、生命周期、权限网关和 CLI/MCP。
