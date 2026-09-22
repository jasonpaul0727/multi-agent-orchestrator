# Multi-Agent Orchestrator — 项目上下文

## 当前状态

持久化、预算、观测与验证底座已完成。实施计划 `docs/superpowers/plans/2026-09-14-persistence-cost-observability-testing-implementation-plan.md` 的 Task 1–9 均已实现，包括跨规格事件契约、并发/崩溃矩阵、端到端验收和打包检查。完整多 Agent 产品仍不可运行：生命周期、模型路由、权限执行和 CLI/MCP 尚未实现。V1 实施已启动，配置/Model Registry 首批边界模型与安全 YAML 加载已加入；完整配置覆盖及运行时集成仍待实现。当前完整测试集为 291 项，覆盖率 91%。

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

测试：`python -m pytest tests/unit tests/contract tests/integration tests/acceptance -q` 完整 269 项通过。

## V1 后续实施进度

- `orchestrator.config` 首批边界已实现：不可变 `PolicyEnvelope`、单调收紧合并、Provider/Model/Price 及 Registry manifest、SHA-256 内容哈希、安全 YAML loader（拒绝重复键/别名/不安全标签并脱敏校验错误）和 Model Registry JSON Schema。
- 权限、安全规格已于 2026-09-22 获用户书面批准。当前 WSL2 只列为隔离后端候选；namespace、Landlock 与 `prlimit` 原语探测通过，但 cgroup 未委派，完整执行隔离尚未验证，不能宣称平台已支持。
- 配置模型新增 22 项单元测试；完整 291 项测试通过，覆盖率 91%。编译、打包和 `pip check` 通过。

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
2. 按 `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md` 的依赖顺序，先建立跨模块契约、配置和注册表，再实现策略/路由及生命周期控制平面。
3. 实现并实测 OS 隔离、Tool/Model Gateway、密钥和审批服务后，再接入 Worker/Verifier。
4. 完成 CLI/MCP 与端到端、安全验收，形成可运行且有证据链的 V1 闭环。
