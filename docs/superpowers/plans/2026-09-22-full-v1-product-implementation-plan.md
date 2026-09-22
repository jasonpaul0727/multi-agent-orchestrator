# Multi-Agent Orchestrator V1 完整产品实施计划

日期：2026-09-22

状态：实施中；安全规格已获书面批准；P1 纯配置/契约工作可推进，Worker 执行仍受隔离验收硬门约束
基线：`fd567ba feat: complete persistence foundation validation`（`main` 与 `origin/main` 同步）

## 目标与交付定义

在已完成的持久化、预算、Artifact、恢复和可观测性基础库之上，实现本地可运行的 V1：用户能通过 CLI 或 MCP 创建软件开发/文档分析 Run；系统能规划并执行有界 DAG、多 Agent 委派、模型与工具调用、独立验证和 Final Review；进程重启后能从事件流安全恢复；所有预算、权限、隔离、审批和审计约束由确定性控制层强制执行。

本计划以仓库已批准的任务生命周期和预设/路由规格为行为依据，以权限/安全规格完成书面终审为安全实施前提。此计划不授权跳过终审，也不把“本机进程能运行”视为隔离已实现。

V1 初始交付范围：

- 本地单用户运行；共享编排核心、CLI 和 MCP Server，不建设 Web 服务。
- 软件开发与文档分析两类任务；Agent 角色、动态子 Agent、预算/深度/并发硬上限按已批准规格实施。
- OpenAI Responses、Anthropic Messages、OpenAI-compatible 三种 Model Adapter；测试默认使用确定性 fake/recorded transport，不要求在线密钥。
- `economic`、`balanced`、`quality`、`custom` 配置；严格解析、版本快照和可解释的确定性路由。
- `read-only`、`workspace-write`、`full-trust` 权限语义，精确的一次性危险动作审批；平台无法证明所需隔离时失败关闭。
- 事件流是唯一真源；外部副作用先记录 intent，结果不明进入 reconciliation，不自动重发。

## 非目标与交付边界

- 不在 V1 承诺多租户服务、远程执行集群、Web UI、完整 DLP 或绕过操作系统权限的“纯 Python 沙箱”。
- 不因 `full-trust` 关闭策略硬拒绝、预算、审批、事件审计、fencing 或结果核对。
- 不把模型 API 在线可用性作为核心 CI 测试前提；在线冒烟需单独启用并由操作者提供密钥。
- 不宣称生产就绪，除非受支持平台矩阵、隔离/密钥/网络安全验收和发布复核全部通过。

## 依赖顺序

```text
P0 规格终审与平台能力候选决策
  -> P1 跨模块契约、配置与注册表（不启用执行）
  -> P2 策略判定与模型路由
  -> P3 Run/DAG/调度/恢复控制平面
  -> P4 OS 隔离、Tool/Model Gateway、Secret/Approval 服务
  -> P5 Worker 与 Agent 执行和验证
  -> P6 CLI/MCP 用户入口
  -> P7 端到端、安全验收与发布
```

实现中可并行编写互不冲突的单元测试、fixture 和文档，但集成必须遵守上述依赖；不得用 mock 通过来代替 P4 的真实隔离验收。

P1/P2 的纯数据模型、配置解析和确定性决策逻辑不运行 Agent 代码，可在 P0 平台探测后继续；任何 Worker、Shell 或有外部副作用的入口都必须等到 P4/P7 对目标平台的实际隔离与安全测试通过。

## P0：解除设计和安全前置阻塞

- [x] 用户于 2026-09-22 书面批准 `docs/superpowers/specs/2026-09-13-permissions-security-isolation-approval-design.md`；如后续实现发现规格冲突，先更新规格和接口，再开始相关安全实现。
- [ ] 建立完整 V1 的接口/架构决策记录：Run/节点/attempt 标识，事件类型归属，stream/CAS 边界，GraphExpansion、PolicyDecision、RoutingDecision、ToolRequest、候选结果、验证证据及统一错误分类。
- [ ] 做隔离后端可行性 spike，逐项验证声明工作区文件边界、符号链接/挂载逃逸防护、进程树终止、资源限制、原始网络阻断、私有临时目录和 Git worktree。给出明确支持的平台/版本矩阵及可重复测试。
- [ ] 若某平台没有满足规格的可验证后端，将该平台/能力标记为不支持并返回 `Blocked(isolation_unavailable)`；禁止无隔离 fallback。记录 Secret Broker 的密钥来源、生命周期、端点绑定方式和 MCP 调用者身份信任边界。
- [ ] 将 V1 最小交付平台及必要用户决策写入 README/ADR；没有获得书面终审或安全能力证据时，停止安全执行层和 Worker 的合并。

2026-09-22 探测记录：当前 Ubuntu 24.04 / WSL2（kernel `6.6.87.2-microsoft-standard-WSL2`）支持组合 user/mount/PID/network namespace；新 network namespace 仅有 loopback 且无路由；私有 mount namespace 内 tmpfs mount 成功；Landlock ABI 3 的白名单读取/越界拒绝已通过一次性实测；`prlimit` 的 CPU、地址空间、NPROC、文件大小和打开文件数限制在子进程中可见，`NPROC=1` 时 fork 实测被内核拒绝。当前未安装 bubblewrap；cgroup v2 对当前用户不可写且没有 delegated cgroup，`NPROC` 仍是 UID 级而非独立 per-attempt controller。WSL2 **仅列为隔离后端候选，不能据此宣布受支持**；P0 仍需验证进程树终止、控制目录隐藏、网络/文件边界和资源限制的组合执行路径，或明确将相关执行模式标记不支持并失败关闭。

验收：所有跨模块边界有类型化接口和因果/版本语义；安全规格状态明确；隔离 spike 有可重复的通过/失败证据、支持矩阵和失败关闭方案。

## P1：跨模块契约、配置和 Model Registry

新增包：`orchestrator.config`；Model Gateway/Adapter 实现仍待后续阶段。

- [x] 为 Policy Envelope、Provider/Model/Price 与 Model Registry manifest 建立严格 Pydantic schema；拒绝未知字段、未知版本、明文秘密和不合法引用。
- [x] 完成 EffectiveConfig、内置预设/角色/selector/guard/health policy 的严格 schema；校验注册表引用、SelectorRule 同优先级交集、角色升级环、fallback tier/能力和健康策略引用。
- [x] 实现 system default → user global → project → Run 四层解析、字段来源追踪和只收紧的 Policy Envelope 合并；`null` 回退、普通列表替换、SelectorRule tombstone 和 GuardRule 累加均有测试。
- [ ] 将候选配置验证接入原子 reload，并把完整有效 manifest/哈希冻结到 Run 快照。
- [x] 导出 Model Registry JSON Schema；安全加载拒绝重复键、别名、不安全标签和过大文档，错误诊断不回显输入值。
- [x] 为完整 EffectiveConfig 与 Overlay 导出 JSON Schema 并提供无密钥 Run overlay 示例；配置预算需显式声明，Model Registry 的秘密字段只接受允许的引用格式。
- [ ] 定义 provider/model/price/tokenizer manifest、适配器协议、请求/响应/usage/error 类型；固定费用按最小货币单位向上取整。
- [ ] 增加 manifest 校验：角色引用、能力、上下文、输出上限、reasoning effort、价格有效期、fallback 和升级图均完整且无环。

2026-09-22 已落地 `PolicyEnvelope`、Provider/Model/Price/Registry 和完整 EffectiveConfig 契约；增加四层解析、来源追踪、安全 YAML loaders、完整配置/Overlay JSON Schema、角色/selector/guard/health 校验与无密钥示例。配置专属测试 38 项通过；完整测试集 307 项通过，覆盖率 91%。原子 reload、Run 快照持久化、tokenizer/FX 快照、Gateway 和运行时路由仍未完成。

验收：配置合并与 schema 测试通过；任意覆盖无法放宽硬限制；相同输入 manifest 生成稳定哈希；任何错误配置不替换活动有效配置；测试输出和事件中无秘密值。

## P2：权限策略核心与确定性路由/健康

建议新增包：`orchestrator/security`、`orchestrator/routing`、`orchestrator/models` 下的 adapter/gateway 接口。

- [ ] 实现纯确定性 `Policy Engine`：规则规范化、deny 优先、权限/allowlist 交集、收紧型约束、`PolicyDecision` 和可解释原因；校验请求绑定 Run/节点/attempt/generation/策略版本。
- [ ] 实现确定性 TaskClassifier、GuardRule、`economic`/`balanced`/`quality`/`custom` 的完整配置语义，以及 Planning 时冻结节点约束。
- [ ] 实现 `RoutingRequest`、资格快照、候选过滤/稳定排序、候选排除原因、最坏费用估算、`RoutingDecision` 和精确重放/预览解释。
- [ ] 实现 Recovery Controller 与 Router 的职责边界：Router 只执行被授权的 retry/fallback/escalation/reviewer/director 动作；升级严格遵守 tier 和证据要求。
- [ ] 实现版本化 provider/model Health aggregate、退避/熔断和完整 ProbeLease CAS；不能仅以本地计数器或墙钟做不可重放的健康判定。
- [ ] 实现三种 provider adapter 的协议转换、错误归一化、usage 提取和取消/超时边界；用 mock HTTP/recorded fixtures 做契约测试，密钥只通过后续 Secret Broker 使用。

验收：相同配置、RoutingRequest、EligibilitySnapshot 与健康版本必得相同结果；所有预算/权限/能力/密钥/健康排除有证据；Decision 接纳必须等调度阶段与账本、租约进行原子 CAS；没有合格模型时返回明确阻塞，不调用模型。

## P3：生命周期状态机、事件驱动 DAG 与控制平面

建议新增包：`orchestrator/lifecycle`、`orchestrator/graph`、`orchestrator/scheduler`、`orchestrator/agents`。

- [ ] 建立 Run、Node、Attempt、Agent 实例和 Graph aggregate；实现已批准规格中的 Run/节点/attempt 状态机和非法转换拒绝。
- [ ] 实现 Intake/Planning、节点契约、DAG/依赖无环校验、追加式扩展和图版本；执行历史不可被模型改写，修复以新节点表达。
- [ ] 实现 Scheduler/Lease Manager：系统、Run、provider、tool 并发额度；单调 fencing generation；过期租约不等于旧 Worker 已停止；迟到候选只能作为证据。
- [ ] 在一个确定事务边界内接纳 RoutingDecision、预算最坏情况预留、并发额度、attempt 租约和节点运行转换；CAS 失败须释放未取得资源并重建请求。
- [ ] 实现 Agent Registry 的累计 Agent 数、父子深度、活动/未知结果计数和上限；重试、改名或结束实例不能重置累计限制。
- [ ] 实现暂停、取消、等待用户、检查点、事件重放、快照失效回退、terminal Run 限制及崩溃恢复；所有调度动作发生在事件持久化之后。
- [ ] 实现失败分类、失败指纹、有限重试/恢复阶梯/熔断及 `OutcomeUnknown`/`AwaitingReconciliation` 行为。

验收：状态机/property tests、并发 CAS tests、多连接测试和进程中断矩阵通过；永不突破预算、并发、深度和 Agent 数上限；相同事件流确定性重建 Run/图/账本；迟到/重复结果不能覆盖被接受结果或触发重复副作用。

## P4：OS 隔离、Tool Gateway、密钥与审批

建议新增包：`orchestrator/isolation`、`orchestrator/tools`、`orchestrator/approvals`、`orchestrator/secrets`。

- [ ] 实现平台隔离适配器和能力指纹；工作区、控制目录、`.git`、策略、密钥和 Artifact 路径之间有明确边界。无法达到节点安全契约时失败关闭。
- [ ] 实现安全路径解析及打开时二次校验；覆盖 traversal、symlink/junction/reparse point、挂载点、硬链接、大小写别名和 TOCTOU 风险；记录可恢复写入清单/快照。
- [ ] 实现受限进程树、CPU/内存/时长/进程/输出限制、私有临时目录、取消与清理；取消/租约丢失先关 Gateway 动作再停止进程，不能确认停止时保留未知占用。
- [ ] 实现 Tool Gateway，作为文件、受限命令、Git、managed web read 和外部工具的唯一入口；执行前重验 Grant、目标/参数哈希、fencing、隔离指纹、撤销版本及策略版本。
- [ ] 实现一次性精确 ApprovalRequest/ApprovalGrant：身份验证、绑定 effect intent、原子单次消费、过期/撤销；批准只令阻塞节点重新就绪，不恢复旧 attempt 或复用旧 RoutingDecision。
- [ ] 实现 Secret Broker：仅 Gateway 在指定端点/用途内使用 secret ref；密钥不进入 Agent/Worker/Shell、环境变量、命令行、提示、事件、日志、Artifact 或审批预览。
- [ ] 外部副作用严格按 intent -> grant consume -> execute -> receipt/reconcile；结果不明禁止自动重试。managed web 代理阻止认证信息、上传、私网/localhost/metadata、重定向绕过和 DNS 重绑定。
- [ ] Git/worktree 写入采用串行共享工作区写租约或独立 worktree 并行；合并作为独立验证节点。

验收：实际运行的隔离后端通过越界/网络/进程终止测试；没有仅靠字符串过滤声称沙箱安全；Grant 并发消费只有一个成功者；EffectIntent 崩溃矩阵可恢复且不重复外部动作；审计不可写时停止授权、调度和副作用。

## P5：Worker Runtime、Model Gateway、Verifier 和 Agent 协作

建议新增包：`orchestrator/runtime`、`orchestrator/verification`。

- [ ] Worker 仅获得当前 attempt 的最小输入、CapabilityGrant 引用和工具请求接口；不得拿到 EventStore/控制目录句柄或写最终状态。
- [ ] 实现 Model Gateway：按 accepted RoutingDecision 调用 adapter，通过 Secret Broker 请求凭据，做超时/取消/有限重试、usage 采集、预算结算和响应脱敏。
- [ ] 实现 Planner 生成初始 DAG、Worker 候选结果、Reviewer、Director 和文档分析 Agent 契约；模型输出只能成为提案/候选，由控制层验证后追加事件。
- [ ] 实现 Agent 动态拆分：Graph Manager 校验依赖/契约/权限/预算/深度/累计数量后追加子图；新增能力必须形成具有新安全契约的节点。
- [ ] 实现独立 Verifier：按节点验收契约检查代码/文档产物、测试证据、Artifact hash 和版本；Worker 不能自我宣布成功。
- [ ] 实现 Final Review：冻结 review graph version 与 input manifest，核验原始目标、必需产物、证据、风险、账本；repair 必须有界返工并重新审查最新 generation。

验收：离线 fake-model + fake-tool 模式跑通成功、失败、重试、升级、拆分、审批等待和恢复流程；Worker 无法伪造状态/访问控制面/绕过 Gateway；Verifier 与 Final Review 的每个判定可追溯到事件和产物哈希。

## P6：CLI 与 MCP Server

建议新增包：`orchestrator/cli`、`orchestrator/mcp`，对外命令在架构决策后固定并写文档。

- [ ] CLI：初始化/校验配置、注册模型、创建 Run、查询 Run/节点/预算/事件摘要、查看 artifact/审计、解释路由、审批/拒绝/撤销、暂停/恢复/取消。
- [ ] MCP：暴露与 CLI 共用的 application service，不复制状态机/权限逻辑；为创建/查询/控制 Run、审批和只读诊断定义严格输入输出 schema。
- [ ] 对 MCP caller 建立调用方身份和 Authority Envelope；未认证或没有审批权的调用方不能授权高风险动作。
- [ ] 长任务工具调用返回 run id/状态或结构化 awaiting-user 请求；重复请求须有幂等键；查询操作只读且不执行隐式恢复副作用。
- [ ] 所有接口输出经过脱敏；错误返回稳定类型和可操作原因，不显示密钥、受保护路径内容或未授权请求数据。

验收：干净环境 `--help`/安装 smoke test、CLI 创建/跟踪/等待审批/恢复/取消全流程、MCP initialize/call/error/cancellation/身份授权协议测试通过；CLI 与 MCP 对同一 Run 展示一致状态和预算。

## P7：产品级端到端、安全验收和发布准备

- [ ] 保留并通过已有 269 项基础库测试；新增 schema/属性/契约/多进程并发/故障注入/平台安全测试。覆盖率总门槛不低于 90%，并单独审查 Artifact Store、Budget Ledger 等关键模块未覆盖分支。
- [ ] 建立离线端到端验收矩阵：单节点成功、多分支并行、动态细化、重试/修复/升级、Agent 上限、预算阻塞、审批等待、Final Review repair、暂停/取消、crash restart、副作用 reconciliation。
- [ ] 建立安全负向验收：工作区逃逸、秘密泄漏、原始网络、SSRF/重定向、提示注入越权、伪造/过期/重复 Grant、审计不可写和旧 fencing 输出。
- [ ] 提供可重现的本地 sandbox smoke suite；跨支持平台逐一验证进程树终止、隔离、网络规则和工作区并发。安全门失败的平台不得被标为支持。
- [ ] 验证 `pip install`/wheel 安装后 CLI 和 MCP 均可启动；编译、格式/类型（若已采纳）、依赖审计、build、pip check 与无密钥 CI 通过。
- [ ] 更新 README、PROJECT_CONTEXT、配置样例、CLI/MCP 使用说明、平台支持矩阵、威胁模型、故障排查和明确的 V1 限制；清除“产品已完成/生产就绪”等未经验证的表述。
- [ ] 发布前做一次交付复核：从干净 checkout 按文档安装、运行离线 E2E、查看审计/恢复、生成制品并检查内容；列出已知风险与升级路径。

V1 完成门槛：以上全部通过；至少一种明确平台的隔离能力被实机证明；从用户输入到有证据的 Final Review 交付形成真实可运行闭环；CLI 和 MCP 均调用同一个核心；预算、权限、审批、审计、失败关闭与崩溃恢复没有绕过路径。只有完成此门槛后，才可将 README 中“整体编排系统尚不可运行”改为可运行状态；生产就绪仍需单独的威胁建模和发布批准。

## 全局实现约束

- 每一阶段先为状态变化写测试，再实现最小垂直切片；每个 PR/提交保持可验证，不一次性堆出不可审查的大改动。
- Event Store 是唯一真源。状态变更先追加事件，投影/日志/快照不能回写或决定调度。
- Worker/模型输出一律视为不可信输入；只提交提案和候选，控制层负责校验、授权、接受及持久化。
- 所有重试、并发、预算、深度和恢复都有明确上限；任何“结果不明”均不得被当作“确定失败”。
- 测试不得要求真实密钥或外网；在线 provider smoke 为显式 opt-in，fixture 不得含真实凭据。
- 每完成一个 Phase，同步 README/PROJECT_CONTEXT 与测试证据，记录没有完成或被平台阻塞的验收项，不通过删减安全断言来消除阻塞。

## 交付记录

- 现有持久化基础提交已执行 `git push`；分支当时与 `origin/main` 同步，远端返回 `Everything up-to-date`。
- 本计划为后续实施路线，不表示 P0–P7 已实现或安全规格已获书面终审。
