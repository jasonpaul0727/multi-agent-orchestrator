# Multi-Agent Orchestrator — 项目上下文

## 当前状态

持久化、预算、观测与验证底座及 P1 配置阶段已完成；P2 Policy Engine、确定性分类/Planning 冻结、候选成本路由、事件驱动健康熔断/ProbeLease CAS、Recovery Controller、三种 Provider codec 与受限 HTTPS transport 已实现。P3 已有 Run/Node/Attempt、追加式 DAG、Agent Registry 累计数量/深度与原子路由接纳；Run 等待用户/取消门控和生命周期检查点/尾部重放也已实现。新增只读 Run Recovery Coordinator，重放并核对生命周期、Agent Registry、预算预留和 scheduler lease；新路由接纳前进行一致性检查，分裂状态 fail-closed。恢复器保留活动/未知 lease，不猜测结果、不释放占用、不重派工作；effect/artifact 跨流恢复和多进程中断矩阵仍未完成，取消回执也尚无真实 Worker/OS 终止器验证。完整产品目前仍不可交付：P3 失败分类/有界重试与全恢复验收缺失；P4 OS 隔离/Tool Gateway/Secret Broker/Approval、P5 Worker/Verifier、P6 CLI/MCP、P7 E2E/安全验收与基准证据均未完成。Provider Gateway 默认没有凭据 Broker，在线调用会 fail-closed。本次 P3 一致性恢复切片：全量 416 项测试通过；总覆盖率 90.02%（项目门槛 90%）通过；`compileall`、`pip check`、sdist/wheel 构建及 `git diff --check` 通过。Maestro 成本、Token 和重复工作改善目标仍未有基准证据。

## 产品目标

构建一个本地运行的多模型 Multi-Agent 编排系统，用 GPT/Codex 作为主要模型，同时通过 API Key 接入其他厂商模型，将低成本任务路由给低价模型，并只在复杂、高风险或多轮失败时升级到 GPT-6 Astra 等高能力模型。

## 已确认范围

- 使用 Python 开发。
- 产品入口为独立本地 CLI 和 MCP Server，不建设 Web 服务。
- 第一版支持软件开发和文档分析。
- 软件开发 Agent 可以修改工作区并运行终端命令。
- 权限参考 Codex，提供 `read-only`、`workspace-write`、`full-trust` 三档；删除、安装系统软件、发布、推送和密钥访问等动作可独立配置策略。
- Agent 可以动态创建子 Agent，但必须受并发数、总数量、调用深度、Token 和金额预算限制。
- 提供 `economic`、`balanced`、`quality` 三个内置预设，以及可完全 DIY 的 `custom` 预设。
- 支持按角色配置厂商、模型、reasoning effort、最大 Token、重试次数和升级目标。
- 模型接入采用 OpenAI Responses 原生适配器、Anthropic Messages 原生适配器和通用 OpenAI-compatible 适配器。
- 路由维度为角色 × 能力 × 预算，统一经 Model Gateway adapter 接入；通过事件/checkpoint 做故障恢复，确定性控制层负责权限和每 Run 硬预算。

### Maestro 性能验收目标（尚待基准验证）

以下是目标与待验证假设，不是已实现能力或生产数据：用固定、可重放工作集评估单功能成本约 `$18 → $7`、Token 约 `90M → 40M`、模型故障后的重复工作减少约 `65%`；同时测量约 `15%` 决策节点采用高能力模型、约 `85%` 执行节点采用低成本模型的组合。模型比例由真实任务分布和质量门槛验证，不作为写死的配置默认值。

## 已选择的总体架构

选择“受控的去中心化 Agent 网络”：Root/Planner、Coder、Document Analyst、Researcher、Tester、Reviewer 和 Director 可以协作并动态创建子 Agent；确定性控制层负责运行状态、预算、权限、并发、检查点和审计，模型不能绕过这些硬限制。

CLI 与 MCP 共用同一 Python 编排核心。核心下方分为 Model Gateway 和 Tool Gateway；运行轨迹、模型调用、工具调用、Token、费用和证据写入持久化事件存储。

## 已完成的实现（Task 1–9）

- `orchestrator.identifiers`、包骨架与测试夹具（Task 1）。
- `orchestrator.persistence`：追加式 SQLite 事件存储、CAS、幂等（Task 2）。
- `orchestrator.persistence.snapshots` 与 `orchestrator.recovery`：快照校验与确定性恢复（Task 3）。
- `orchestrator.artifacts`：内容寻址、原子发布、访问隔离（Task 4）。
- `orchestrator.budget`：整数最小货币单位的预留、结算、释放与结果不明（Task 5）。
- `orchestrator.observability`：脱敏 Redactor、ObservationSink（LogRecord/TraceSpan/MetricSample）、Run/Budget/Cost/Approval/Audit 五个只读版本感知投影（Task 6）。投影不回写事件存储、不参与调度；脱敏失败即观测写入失败，无原始回落。
- 跨规格事件契约：为安全、路由、副作用等事件校验完整执行上下文；校验 EffectIntent/Receipt 因果次序、fencing/attempt 一致性，以及审批消费和预算预留的配对（Task 7）。
- 并发与崩溃矩阵：覆盖多连接 SQLite CAS/幂等、并发预算预留、重启恢复及副作用 intent/receipt 故障窗口（Task 8）。
- 验收及交付：端到端持久化流程测试、覆盖率/编译/打包命令与忽略构建产物的 `.gitignore`（Task 9）。

测试：`python -m pytest tests/unit tests/contract tests/integration tests/acceptance -q` 完整 307 项通过（覆盖率 91%）。

## V1 后续实施进度

- `orchestrator.config` 已实现：不可变 `EffectiveConfig`、七种角色 schema、经济/平衡/质量/自定义预设、selector/guard/health policy 校验、Provider/Model/Price/Registry manifest、SHA-256 内容哈希、安全 YAML loaders、配置/Overlay JSON Schema，以及 system default → user global → project → Run 四层合并和逐字段来源追踪。候选 reload 经完整验证后原子切换，Run 创建冻结有效配置、Registry、哈希和来源；Run 快照可从事件重放恢复。`PolicyEnvelope` 只单调收紧；registry 引用和强制 guards 不能被普通覆盖修改。
- `orchestrator.security` / `orchestrator.routing` 已实现确定性策略判定、TaskClassifier、Planning 节点契约冻结、按资格/策略/健康/秘密/预算过滤并稳定排序的 ModelRouter。模型输入任务原文不进入分类结果，只保留规范化 hash 与稳定证据码。
- Recovery Controller 只授权有限策略：瞬时且安全失败优先同模型有限重试；已不可用/耗尽才同 tier fallback；高层级能力升级必须由 `escalate_to` 和 failure evidence 支持；输出/任务失败要求独立修复子节点；未知副作用结果不自动重试。
- Health Controller 将 provider/model aggregate 变化写入 event streams，基于持久化事件时间重放滑动失败窗口、degraded/open/half-open 状态；ProbeLease 覆盖 provider/model 所有 open aggregate，通过 SQLite 单事务 CAS，重启后可复原，过期探测会被记录为失败并重新打开熔断。
- `orchestrator.models` 已实现三种协议 codec 和 bounded HTTPS transport。Gateway 强制检查 Registry/provider/model/accepted-route verifier，再向注入的 Secret Broker 获取凭据；默认 Broker 无法交付凭据，避免环境变量回退。真实 Provider 在线请求和凭据管理仍由 P4 Secret Broker 前置阻挡；P3 已把模型路由预算预留/结算接入 attempt lifecycle，但 Tool Gateway、Worker 执行期集成尚未实现。
- `orchestrator.lifecycle` / `orchestrator.scheduler` 的 P3 控制面：冻结 Run 配置与 Registry 哈希，事件重放 Run/Node/Attempt；append-only DAG 以 graph version CAS、节点数和深度界限校验。Scheduler 在共享 SQLiteEventStore 的事务内校验路由与 Policy scope，原子写入预算预留、生命周期 Attempt、Agent 实例、全局并发 slot 和 lease；用 fencing generation 拒绝迟到结果，未知结果保留资源直到 reconciliation。等待用户、取消门控、生命周期 checkpoint/replay 已有实现；只读 Run Recovery Coordinator 会校验 lifecycle/Agent/budget/scheduler 四类记录并作为新调度的 fail-closed admission barrier。它不恢复外部副作用或 artifact，也不重启/杀死真实进程；P3 跨进程故障矩阵、失败分类及有界重试尚缺。
- `orchestrator.agents` 已接入 P3 Scheduler：每个模型 Attempt 有确定性 AgentInstance ID；创建与 Attempt/预算/slot 同事务，记录 parent、深度、角色、模型、provider、决策和策略哈希。累计数量按创建事件计数且永不退款；深度从已完成父实例派生；活动与未知结果均占 Agent 并发上限；终结/对账和状态重放也与预算及 slot 更新原子提交。
- 权限、安全规格已于 2026-09-22 获用户书面批准。当前 WSL2 只列为隔离后端候选；namespace、Landlock 与 `prlimit` 原语探测通过，但 cgroup 未委派，完整执行隔离尚未验证，不能宣称平台已支持。
- 本次 Agent Registry 验收：全量 402 项测试通过；`coverage report --precision=2 --fail-under=90` 精确覆盖率 90.19% 通过；编译、`pip check` 与 sdist/wheel 打包通过。Maestro 的 `$18 → $7` 单功能费用、`90M → 40M` Token 和约 65% 重复工作下降均只是待固定工作集测量的目标，尚非项目结果。

## 已确认的设计部分

整体架构和分层职责已获用户确认，不需要调整。

任务生命周期与失败恢复设计已于 2026-09-10 获用户批准。V1 采用事件驱动 DAG，并允许执行期间仅追加和细化节点；优先保证可控可靠。正式规格位于 `docs/superpowers/specs/2026-09-09-task-lifecycle-design.md`。

预设、DIY 配置与模型路由设计已于 2026-09-12 获用户批准；正式规格位于 `docs/superpowers/specs/2026-09-11-presets-routing-design.md`。

权限、安全、隔离与审批设计正式规格位于 `docs/superpowers/specs/2026-09-13-permissions-security-isolation-approval-design.md`，已于 2026-09-22 获用户书面批准。规格批准不代表执行隔离已实现；平台隔离能力仍须实测并失败关闭。

持久化、成本统计、可观测性和测试方案的 SQLite 本地优先方向已于 2026-09-14 获用户确认；正式规格位于 `docs/superpowers/specs/2026-09-14-persistence-cost-observability-testing-design.md`，已完成书面审阅。

持久化、成本统计、可观测性和测试方案书面规格已获用户审阅确认；对应实施计划位于 `docs/superpowers/plans/2026-09-14-persistence-cost-observability-testing-implementation-plan.md`，Task 1–9 已完成。

## 实施前置门与待解决技术风险

权限、安全、隔离与审批书面规格已获批准。尽管持久化底座包含审批和副作用的事件契约，这并不代表权限签发/验证、策略执行或工具隔离已实现。

2026-09-22 对当前 Ubuntu 24.04 / WSL2（Microsoft Linux kernel 6.6.87）进行只读能力探测：用户、挂载、PID、网络 namespace 组合可创建；namespace 内仅有 loopback 且无路由；私有挂载 namespace 中 tmpfs 挂载成功；Landlock ABI 3 可用，并实测允许白名单文件、拒绝目录外读取；`prlimit` 可为进程设置 CPU、地址空间、进程数、文件大小和文件描述符上限。当前环境未安装 bubblewrap；cgroup v2 未向当前用户提供可写委派，因此不能依赖它落实每 attempt 的内存/PID 控制。结论：WSL2 隔离后端仍是待验证/待决策，不列为已支持平台；需要证明资源上限、进程树终止和控制目录隔离的实现，或对不满足的执行模式失败关闭。

## 后续设计顺序

1. 完成 P0 隔离后端可行性验证并形成明确的平台支持矩阵。
2. 按 `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md` 继续 P3：补齐跨 Run/预算/Registry/租约/副作用的恢复协调及多进程故障矩阵。
3. P0/P4 实现并实测 OS 隔离、Tool Gateway、Secret Broker 和 Approval 后，再启用真实 Provider 调用及 Worker/Verifier。
4. 完成 CLI/MCP 与端到端、安全验收，形成可运行且有证据链的 V1 闭环。
