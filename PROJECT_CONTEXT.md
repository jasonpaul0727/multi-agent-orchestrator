# Multi-Agent Orchestrator — 项目上下文

## 当前状态

2026-09-23 P3 Planning 更新：新增 `GraphPlanningService`/`NodeProposal` 可信 host-side 路径，以 Run 冻结的 EffectiveConfig/Registry 和首次图追加冻结的完整 `PolicyManifest` 编译并追加 DAG；事件持久化完整 `PlanningNodeContract`（分类 hash/证据、策略约束、候选模型、Token 和工具声明），重放校验 Run/config/Registry/policy/node/role/hash 绑定，Scheduler 接纳还会将请求策略哈希比对该已冻结节点策略。动态追加不能改变 Run policy；任务原文不进入事件。计划文件的 P3 Intake/Planning 与完整节点契约项现完成。此服务尚未接入非可信 Planner Worker 或面向用户的 application service，不改变 P5/P6 缺口。本轮最终验收：629 项全量测试通过、覆盖率 90.06%（门槛 90%），compileall、pip check、wheel build、diff check 通过。

2026-09-23 本轮 P4 更新：新增内部 ApprovalService 原语，hash-scoped ApprovalRequest、带权限的注入式身份认证、到新 attempt 的 grant 绑定、过期/撤销、一次性 effect intent + grant 消费 + budget reservation 原子追加，以及 attempt-bound receipt 均有定向测试。它尚未接入 ToolGateway/Worker，也没有生产身份提供方或 CLI/MCP 审批入口。另新增 `AuditedSecretBroker`：对显式按 Run/provider/ref/endpoint/purpose 配置的请求写脱敏 SQLite 事件后才解析环境/插件 SecretValueStore；Worker 进程边界、keyring/plugin 后端及真实 Provider egress 仍未验收。边界见 `docs/security/approvals.md` 与 `docs/security/secrets.md`；均不代表端到端安全功能已交付。

2026-09-23 本轮前序 P4 更新：新增只读命令 Tool Gateway，已在 Ubuntu 24.04/WSL2 systemd 上以真实 `ToolGateway → SQLite 安全事件 → SystemdReadOnlyLauncher` 路径集成验证。它只开放固定的 `system.readonly-command`，基于冻结策略、一次性 CapabilityGrant、启动前 fencing/策略版本重验和运行中撤销；deny/approval/audit failure/重复请求均不启动或不重放，权限丢失/终止未确认时丢弃输出并记录取消/unknown。细节与边界见 `docs/security/tool-gateway.md`。ToolGateway、ApprovalService 与 Secret Broker 都是内部候选 API，不代表 workspace-write、Worker 或产品级恢复已完成。

2026-09-23 P0/P4 隔离更新：WSL2 的 systemd transient service/cgroup 实际资源限制与超限终止、进程树 kill、user/mount/network namespace、PrivateTmp/ProtectHome、workspace bind、`.git`/控制目录只读、Git worktree host metadata 隐藏、Landlock 越界拒绝和 4 MiB tmpfs Overlay 写满/lower 不变已实测。`SystemdReadOnlyLauncher` 接入 cgroup/rlimit 校验、descriptor-anchored no-follow 工作区快照、只读 runtime/workspace bind、私有网络/tmp/Home、敏感路径隐藏、Landlock 与有界输出/超时/取消。新 `orchestrator.tools.ToolGateway` 固定只读命令已通过真实 Gateway→SQLite 安全事件→systemd 集成；包括现有 launcher 用例共 6 个 live integration tests。ToolGateway/launcher 仍是内部只读候选切片：race-safe workspace-write、Overlay diff 发布、ApprovalService/Secret Broker 的进程集成、Worker 接线和完整安全验收仍缺；平台矩阵见 `docs/security/platform-support.md`，WSL2 不是完整 V1 支持平台，V1 仍不可交付。

持久化/预算/观测基础、P1 四层配置及 P2 策略/确定性路由均已实现。P3 包含 Agent Registry 累计数量/深度/活动限制、等待用户/取消门控、生命周期 checkpoint/replay、只读一致性恢复，以及 Effect/Artifact/Gateway failure 记录与受限恢复授权。Artifact publication intent 可跨进程重启归属 Run 并检查 bytes/provenance；候选不会自动采纳或删除，旧/裸 orphan 仍不可归属。Scheduler admission/reconciliation 的子进程测试证明 SQLite 原子性和幂等重放，但不证明 Provider 查询/回执真实性；完整跨进程中断矩阵、真实 Worker 终止回执、Provider reconciliation 和自动恢复循环仍缺，协调器目前不启动/终止 Worker。

P4 只有经实测的 Linux/systemd 只读隔离 profile、固定只读 ToolGateway 切片、未接 Worker 的 ApprovalService 原语和进程内 Secret Broker 原型。workspace-write/Overlay 安全发布、独立 Worker 边界、生产身份/keyring、Gateway/Worker 审批接线未完成。P5 Worker/Verifier、P6 CLI/MCP、P7 完整 E2E/安全验收及性能基准均未完成；默认 Provider broker fail-closed，项目不具备完整产品或生产系统交付条件。Maestro 成本、Token 和重复工作目标尚无可复现基准证据；边界见 `docs/security/run-recovery.md`。

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
- Recovery Controller 只授权有限策略：瞬时且安全失败优先同模型有限重试；已不可用/耗尽才同 tier fallback；高层级能力升级必须由 `escalate_to` 和 failure evidence 支持；输出/任务失败要求独立修复子节点。现在将 `outcome_unknown` 明确作为不可重试门：即使重试预算耗尽且模型已不可用，也必须先 reconciliation，不得直接 fallback。
- Health Controller 将 provider/model aggregate 变化写入 event streams，基于持久化事件时间重放滑动失败窗口、degraded/open/half-open 状态；ProbeLease 覆盖 provider/model 所有 open aggregate，通过 SQLite 单事务 CAS，重启后可复原，过期探测会被记录为失败并重新打开熔断。
- `orchestrator.models` 已实现三种协议 codec 和 bounded HTTPS transport。Gateway 强制检查 Registry/provider/model/accepted-route verifier，再向注入的 Secret Broker 获取凭据；默认 Broker 无法交付凭据，避免环境变量回退。真实 Provider 在线请求和凭据管理仍由 P4 Secret Broker 前置阻挡；P3 已把模型路由预算预留/结算接入 attempt lifecycle；`orchestrator.tools.ToolGateway` 现有只读命令切片尚未接入 Worker/Scheduler application service。
- `orchestrator.lifecycle` / `orchestrator.scheduler` 的 P3 控制面：冻结 Run 配置与 Registry 哈希，事件重放 Run/Node/Attempt；append-only DAG 以 graph version CAS、节点数和深度界限校验。Scheduler 在共享 SQLiteEventStore 的事务内校验路由与 Policy scope，原子写入预算预留、生命周期 Attempt、Agent 实例、全局并发 slot 和 lease；用 fencing generation 拒绝迟到结果，未知结果保留资源直到 reconciliation。等待用户、取消门控、生命周期 checkpoint/replay 已有实现。只读 Run Recovery Coordinator 现在还会校验 EffectIntent/Receipt 与 ArtifactPublished 元数据/对象 hash；无回执副作用保持 unknown，终态 Attempt 存在未决 effect 或 artifact 校验失败时 fail-closed。ArtifactStore 提供全局只读 orphan blob 盘点，但不归属到 Run、不自动删除。Gateway 失败分类器与 Scheduler 现可将脱敏分类/RecoveryPlan 持久化，并只接受证据计数相符、单次消费的同节点恢复授权；自动 Worker 循环、Provider reconciliation 和跨进程故障矩阵仍缺，详见 `docs/security/run-recovery.md`。
- `orchestrator.agents` 已接入 P3 Scheduler：每个模型 Attempt 有确定性 AgentInstance ID；创建与 Attempt/预算/slot 同事务，记录 parent、深度、角色、模型、provider、决策和策略哈希。累计数量按创建事件计数且永不退款；深度从已完成父实例派生；活动与未知结果均占 Agent 并发上限；终结/对账和状态重放也与预算及 slot 更新原子提交。
- 权限、安全规格已于 2026-09-22 获用户书面批准。2026-09-23 的 `SystemdReadOnlyLauncher` 与 `ToolGateway` 已把 WSL2 live probes 接成真实、fail-closed 的只读命令路径；workspace-write、安全差异发布、Approval/Secret Broker 和 Worker/Scheduler 接入仍未完成。当前 WSL2 仅对内部只读 profile 有验证证据，不可宣称完整 V1 平台已支持。
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
3. P4 继续完成实测 OS 写隔离、Tool Gateway 剩余动作、独立进程/宿主身份边界、Secret Broker 插件后端和 ApprovalService 到 Gateway/Worker 的接线后，才启用 Provider/Worker 产品路径。
4. 完成 CLI/MCP 与端到端、安全验收，形成可运行且有证据链的 V1 闭环。
