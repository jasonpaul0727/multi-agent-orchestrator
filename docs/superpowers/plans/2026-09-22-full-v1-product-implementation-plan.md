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
- [x] 做隔离原语可行性 spike，逐项以 live tests 验证工作区文件边界、符号链接/挂载逃逸、进程树终止、资源限制、原始网络阻断、私有临时目录和 Git worktree。
- [x] 将已测原语接入真实 `SystemdReadOnlyLauncher`：每次启动核验 cgroup/rlimit，bind 只读 workspace/runtime，隐藏 Home、Git/控制和主机凭证路径，限制网络/临时目录/filesystem、资源、时长和输出；停止通过 systemd unit wait 收据确认。
- [x] 记录隔离平台矩阵与边界：`docs/security/platform-support.md` 只把 Ubuntu 24.04/WSL2/systemd 255 的只读 profile 列为已测候选，不宣称完整 V1 支持。
- [ ] 完成 workspace-write 的安全路径操作、Overlay 差异导出/验证/原子应用及 Worker/Gateway 接入；在这些闭环前 WSL2 仍不是完整 V1 支持平台。
- [ ] 若某平台没有满足规格的可验证后端，将该平台/能力标记为不支持并返回 `Blocked(isolation_unavailable)`；禁止无隔离 fallback。记录 Secret Broker 的密钥来源、生命周期、端点绑定方式和 MCP 调用者身份信任边界。
- [ ] 将 V1 最小交付平台及必要用户决策写入 README/ADR；没有获得书面终审或安全能力证据时，停止安全执行层和 Worker 的合并。

2026-09-22 首轮探测记录：当前 Ubuntu 24.04 / WSL2（kernel `6.6.87.2-microsoft-standard-WSL2`）支持组合 user/mount/PID/network namespace；新 network namespace 仅有 loopback 且无路由；私有 mount namespace 内 tmpfs mount 成功；Landlock ABI 3 的白名单读取/越界拒绝已通过一次性实测；`prlimit` 的 CPU、地址空间、NPROC、文件大小和打开文件数限制在子进程中可见，`NPROC=1` 时 fork 实测被内核拒绝。当时未安装 bubblewrap，且用户不能直接写 cgroup v2 根目录。

2026-09-23 后续 live spike：发现 `systemd --user` transient service 能建立实际 task cgroup；MemoryMax、TasksMax、CPUQuota 和文件 rlimit 按请求施加，真实内存超限服务终止、TasksMax 阻止额外 fork，systemd kill-all 终止服务进程树。组合 probes 还通过 network namespace 外连阻断、PrivateTmp/ProtectHome、workspace bind、`.git`/`.maestro` 只读、Git worktree host metadata 隐藏、Landlock 越界和 symlink 读取拒绝，以及 4 MiB tmpfs Overlay 写满拒绝/lower 不变。此后新增 `SystemdReadOnlyLauncher` 真实垂直切片：host 使用固定根目录 FD、`openat`/`O_NOFOLLOW` 与 mount-id 检查制成无写权限快照（忽略 `.git`/`.maestro`）；child 在 exec 前核验 cgroup/rlimit、设置最小 Landlock grants、运行代码只得到干净环境；systemd 属性配置包含网络/敏感路径隔离、资源/时长上限；host 对 stdout/stderr 限流，取消发 SIGKILL 至整个 unit 并等待 `systemd-run --wait` 退出才返回 `termination_confirmed`。`tests/integration/test_systemd_launcher.py` 实测读写、控制目录、home/凭证路径、网络、超时、输出洪泛和进程树取消；WSL2/systemd 具体版本与限制记录在 `docs/security/platform-support.md`。P4 尚缺 workspace-write 的安全修改、Overlay 差异导出/应用、Tool Gateway/Approval/Secret Broker/Worker 接入；故这只是只读 profile，不是完整受支持 V1 平台。

验收：所有跨模块边界有类型化接口和因果/版本语义；安全规格状态明确；隔离 spike 有可重复的通过/失败证据、支持矩阵和失败关闭方案。

## P1：跨模块契约、配置和 Model Registry

新增包：`orchestrator.config`；Model Gateway/Adapter 实现仍待后续阶段。

- [x] 为 Policy Envelope、Provider/Model/Price 与 Model Registry manifest 建立严格 Pydantic schema；拒绝未知字段、未知版本、明文秘密和不合法引用。
- [x] 完成 EffectiveConfig、内置预设/角色/selector/guard/health policy 的严格 schema；校验注册表引用、SelectorRule 同优先级交集、角色升级环、fallback tier/能力和健康策略引用。
- [x] 实现 system default → user global → project → Run 四层解析、字段来源追踪和只收紧的 Policy Envelope 合并；`null` 回退、普通列表替换、SelectorRule tombstone 和 GuardRule 累加均有测试。
- [x] 将候选配置验证接入原子 reload，并把完整有效 manifest/哈希冻结到 Run 快照。
- [x] 导出 Model Registry JSON Schema；安全加载拒绝重复键、别名、不安全标签和过大文档，错误诊断不回显输入值。
- [x] 为完整 EffectiveConfig 与 Overlay 导出 JSON Schema 并提供无密钥 Run overlay 示例；配置预算需显式声明，Model Registry 的秘密字段只接受允许的引用格式。
- [x] 定义 provider/model/price/tokenizer manifest、适配器协议、请求/响应/usage/error 类型；固定费用按最小货币单位向上取整。
- [x] 增加 manifest 校验：角色引用、能力、上下文、输出上限、reasoning effort、价格有效期、fallback 和升级图均完整且无环。

2026-09-22 已落地 `PolicyEnvelope`、Provider/Model/Price/Registry 和完整 EffectiveConfig 契约；增加四层解析、来源追踪、安全 YAML loaders、完整配置/Overlay JSON Schema、角色/selector/guard/health 校验与无密钥示例。`ConfigManager` 仅在候选完整验证后原子切换；失败时保持活动对象与 generation 不变。`RunCreated` 事件持久化完整有效配置、Registry、双哈希、来源追踪/标签及代次；SnapshotStore checkpoint 可从事件重放修复，Run 重试返回原快照。新增 Tokenizer/FX 版本化快照、事件时间有效性校验、整数有理汇率换算/向上舍入、成本快照 ID 写入估算与预算预留事件；新增 provider endpoint 约束及 Model Gateway/Adapter 请求、响应、usage、失败类型与 accepted route/attempt/reservation 绑定校验。Gateway 当前是纯契约层，真实 provider codecs/HTTP transport 尚未实现。测试覆盖并发 reload、Run/reload 竞争、同 Run ID 并发启动、成本快照过期/错配、汇率舍入、Adapter endpoint/auth 约束和 checkpoint 失败重放。

验收：配置合并与 schema 测试通过；任意覆盖无法放宽硬限制；相同输入 manifest 生成稳定哈希；任何错误配置不替换活动有效配置；测试输出和事件中无秘密值。

## P2：权限策略核心与确定性路由/健康

建议新增包：`orchestrator/security`、`orchestrator/routing`、`orchestrator/models` 下的 adapter/gateway 接口。

- [x] 实现纯确定性 `Policy Engine`：规则规范化、deny 优先、权限/allowlist 交集、收紧型约束、`PolicyDecision` 和可解释原因；校验请求绑定 Run/节点/attempt/generation/策略版本。
- [x] 实现确定性 TaskClassifier、GuardRule、`economic`/`balanced`/`quality`/`custom` 的完整配置语义，以及 Planning 时冻结节点约束。
- [x] 实现 `RoutingRequest`、资格快照、候选过滤/稳定排序、候选排除原因、最坏费用估算、`RoutingDecision` 和精确重放/预览解释。
- [x] 实现 Recovery Controller 与 Router 的职责边界：Router 只执行被授权的 retry/fallback/escalation/reviewer/director 动作；升级严格遵守 tier 和证据要求。
- [x] 实现版本化 provider/model Health aggregate、退避/熔断和完整 ProbeLease CAS；不能仅以本地计数器或墙钟做不可重放的健康判定。
- [x] 实现三种 provider adapter 的协议转换、错误归一化、usage 提取和取消/超时边界；以 mock transport/recorded fixtures 做契约测试；Gateway 不直接读取密钥，后续 P4 Secret Broker 是 live 调用前置。

验收：P2 单元/契约测试证明相同配置、RoutingRequest、EligibilitySnapshot 与健康版本必得相同结果；所有预算/权限/能力/密钥/健康排除有证据；没有合格模型时 Router 返回明确阻塞，不调用模型。Decision 原子接纳仍须由 P3 Scheduler 与账本、租约 CAS 实现；未有 P4 Secret Broker 时 Gateway fail-closed，不发在线请求。

## P3：生命周期状态机、事件驱动 DAG 与控制平面

状态：Run/Node/Attempt + Scheduler + Agent Registry 首个垂直切片已实现；生命周期检查点/尾部重放和只读跨流一致性协调已加入。2026-09-23 又将 EffectIntent/Receipt 与 ArtifactPublished 元数据/对象完整性检查接入恢复 admission；无回执副作用保持 outcome_unknown，终态 Attempt 仍有未决副作用时 fail-closed。本 Phase 仍未完成：跨进程 Worker 中断矩阵、Provider reconciliation、真实 Worker/OS 终止回执、孤儿 artifact 清点、失败分类/有界重试及 Worker 对接仍缺失，不得视作 P3 完成或可交付产品。Agent 记录冻结路由的 reasoning effort，Gateway accepted route 校验请求与所选 effort 一致。

建议新增包：`orchestrator/lifecycle`、`orchestrator/graph`、`orchestrator/scheduler`、`orchestrator/agents`。

- [x] 建立 Run/Node/Attempt/Graph 生命周期投影；实现基础状态机与非法转换拒绝。
- [x] 建立 Agent 实例 aggregate 和 Agent Registry；每个模型 attempt 在调度接纳时创建事件化实例。
- [ ] 实现 Intake/Planning 与完整节点契约集成。
- [x] 实现 DAG/依赖无环校验、有界追加式扩展和图版本；执行历史不可改写，修复须作为新节点表达。
- [x] 实现 Scheduler 的系统、Run、provider、tool 并发额度与 fencing generation；过期租约转未知结果并保留 slot，不假定旧 Worker 已停止。
- [x] 在一个 SQLite 事务边界内接纳 RoutingDecision、预算最坏情况预留、并发 slot、attempt lease 与节点运行转换；失败注入验证回滚，多连接 CAS 验证互斥。
- [x] 实现 Agent Registry 的累计 Agent 数、父子深度、活动/未知结果计数和上限；重试/改名/结束不得重置累计限制。
- [x] 将所选 reasoning effort 纳入路由决策、Attempt、Agent 实例和 Gateway accepted route 的完整性绑定。
- [x] 实现 Run 暂停/恢复/等待用户，显式响应哈希解除等待。
- [x] 实现 Run 取消门控：先拒绝新调度；只有活动 Attempt 收到停止回执且预算已结算/证明无副作用后才可释放；OutcomeUnknown 仍要求核对。
- [x] lifecycle 初始化/图/Run/Attempt 边界自动检查点；重启优先校验快照 hash/schema/version/source-event anchor，再重放尾部；失效快照回退完整事件流。
- [x] 增加只读 Run Recovery Coordinator，重放并交叉核对生命周期、Agent Registry、预算预留、scheduler lease/结果；新路由接纳前先运行一致性检查，发现分裂状态即 fail-closed。真实子进程中分别在接纳事务提交前、提交后丢响应并重开数据库，验证完整回滚、确定性重建和幂等重放。恢复器只返回活动/未知 lease，不猜测结果、不释放资源、不重派工作。
- [x] 将 effect intent/receipt 与 ArtifactPublished stream 纳入统一 Run Recovery Coordinator；恢复时将缺回执的外部 effect 保持为 outcome_unknown、拒绝终态 Attempt 上的未决 effect，并验证有发布事件的 ArtifactStore 对象 digest/size 与可用 attempt provenance。只读恢复结果不重放副作用或暴露 artifact bytes。
- [ ] 扩展多进程中断矩阵覆盖每个跨流事务/副作用窗口；接入能验证进程终止回执的真实 Worker/OS 终止器，并实现 Provider reconciliation 与 orphan-artifact 清点。当前恢复器是重建/完整性门，不是完整自动恢复执行器。
- [ ] 实现失败分类/指纹、与 Recovery Controller 集成的有界重试阶梯和熔断。
- [x] 实现 `OutcomeUnknown`/`AwaitingReconciliation`，显式对账前不释放预算与并发资源。

验收：状态机/property tests、并发 CAS tests、多连接测试和进程中断矩阵通过；永不突破预算、并发、深度和 Agent 数上限；相同事件流确定性重建 Run/图/账本；迟到/重复结果不能覆盖被接受结果或触发重复副作用。

## P4：OS 隔离、Tool Gateway、密钥与审批

建议新增包：`orchestrator/isolation`、`orchestrator/tools`、`orchestrator/approvals`、`orchestrator/secrets`。

2026-09-23 部分进展：已通过实测 `SystemdReadOnlyLauncher` 将 `orchestrator.tools.ToolGateway` 的固定只读命令接入候选安全事件流。Gateway 只在冻结 `PolicyManifest` 判定 allow 后消费一次性 capability；启动前重验 fencing/策略版本，运行中监视权限并取消，审计只记输入/输出摘要；真机集成测试覆盖 SQLite 审计→systemd 执行。该切片不能勾销后面列出的整体 P4 验收项：没有 workspace-write/原子变更应用、Approval Service、Secret Broker、Worker/Scheduler 接线，也没有 Tool effect 的崩溃对账。

2026-09-23 ApprovalService 切片：新增内部 `orchestrator.approvals` API，以请求/动作/目标哈希约束 grant；注入式认证和权限检查；grant 只能绑定到新 attempt；在一个共享 SQLite 事务中记录 EffectIntent、单次 grant 消费和预算预留，并要求 effect receipt 绑定同一 attempt。定向测试覆盖过期、撤销、scope 变化、失败认证、并发双消费和回执重放。该切片**未**接入 ToolGateway/Worker，不含真实身份服务、CLI/MCP 入口、Secret Broker，也不触发外部动作；它不解除 P4/P5 硬门。细节：`docs/security/approvals.md`。

2026-09-23 Secret Broker 切片：新增 `AuditedSecretBroker` 与 `EnvironmentSecretStore`，由宿主显式 allowlist 精确绑定 Run/provider/secret ref/HTTPS endpoint/用途；授权/拒绝决定先以哈希字段写 SQLite security stream，再按一次性 request ID 解析凭据并交给 ProviderModelGateway；secret store 不可用另记 `SecretCredentialUnavailable`。Gateway 核验凭据 provider/endpoint/purpose/header scope。环境变量引用仅由显式 store 读取，默认 `UnavailableSecretBroker` 仍 fail-closed。此为进程内原型：尚无独立 Worker/Gateway 进程边界、keyring/plugin 实际实现、动态撤销、在线 Provider E2E 或运行平台安全验收；细节：`docs/security/secrets.md`。

- [ ] 实现平台隔离适配器和能力指纹；工作区、控制目录、`.git`、策略、密钥和 Artifact 路径之间有明确边界。无法达到节点安全契约时失败关闭。
- [ ] 实现安全路径解析及打开时二次校验；覆盖 traversal、symlink/junction/reparse point、挂载点、硬链接、大小写别名和 TOCTOU 风险；记录可恢复写入清单/快照。
- [ ] 实现受限进程树、CPU/内存/时长/进程/输出限制、私有临时目录、取消与清理；取消/租约丢失先关 Gateway 动作再停止进程，不能确认停止时保留未知占用。
- [ ] 实现 Tool Gateway，作为文件、受限命令、Git、managed web read 和外部工具的唯一入口；执行前重验 Grant、目标/参数哈希、fencing、隔离指纹、撤销版本及策略版本。
- [ ] 完成端到端一次性精确 ApprovalRequest/ApprovalGrant：将已有内部 ApprovalService 原语接入可信身份/Authority Envelope、ToolGateway 和 Worker；覆盖绑定 effect intent、原子单次消费、过期/撤销及授权后创建新 attempt；批准只令阻塞节点重新就绪，不恢复旧 attempt 或复用旧 RoutingDecision。
- [ ] 完成 Secret Broker 端到端安全边界：将现有进程内 allowlist/audit 原型接入独立可信 Gateway/Worker 进程布局、宿主身份/密钥后端与策略撤销；实测密钥不进入 Worker/Shell、环境变量继承、命令行、提示、事件、日志、Artifact 或审批预览。
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
