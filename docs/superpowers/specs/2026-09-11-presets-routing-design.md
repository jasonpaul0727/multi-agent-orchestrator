# Multi-Agent Orchestrator：预设、DIY 配置与模型路由设计

日期：2026-09-11
状态：已于 2026-09-12 获用户批准

## 1. 目标

本规格定义 V1 的三个内置预设、`custom` DIY 配置、分层覆盖、模型注册、确定性模型路由、升级与健康切换，以及可解释性和验收测试。

目标是在不削弱前述任务生命周期中的预算、权限、并发、审计和验证硬限制的前提下，把简单工作稳定地交给低成本模型，并在复杂、高风险或有明确失败证据时升级到更高能力的模型或独立 Reviewer。

V1 优先可控可靠。因此模型不能自行改变配置、扩大权限、提高预算，或绕过确定性 Router 直接选择模型。

## 2. 范围与非目标

本规格包含：

- `economic`、`balanced`、`quality` 三个内置预设的行为语义；
- `custom` 的完整 DIY 配置格式；
- 系统、用户、项目和单次 Run 四层配置覆盖；
- 提供商、模型、能力层级、价格来源和角色策略的声明；
- 路由规则、候选筛选、稳定排序、fallback 与决策审计；
- 失败升级、模型健康状态与熔断；
- 配置热更新、Run 快照、解释路由和测试要求。

本规格不定义：

- 实际密钥存储实现、具体权限政策和审批界面；
- 内置模型清单、实时价格获取和厂商账号管理；
- CLI 与 MCP 的最终命令或协议名称；
- 成本账本、事件存储、可观测性后端的物理实现；
- 模型厂商适配器的请求/响应细节。

这些后续模块必须满足本规格的接口和约束，但其实现细节不在此文档中锁定。

## 3. 术语与不变量

- **能力层级（tier）**：V1 使用固定的有序枚举 `economy < standard < high`，不等同于具体模型名称。
- **模型注册表（Model Registry）**：将稳定模型引用映射到厂商、实际模型 ID、能力、上下文、输出上限和计价信息的配置数据。
- **预设（preset）**：一组路由、角色、恢复和资源默认值。
- **有效配置（resolved configuration）**：四层来源合并并校验后的不可变配置。
- **策略信封（Policy Envelope）**：只能收紧硬限制的独立约束集合，不属于可自由覆盖的预设偏好。
- **RoutingRequest**：为一个节点或 attempt 选择模型时的结构化输入。
- **RoutingDecision**：Router 的可重放、可审计输出。
- **EligibilitySnapshot**：一次路由所依赖的动态准入事实，包括秘密可用性、授权、预算账本、熔断和健康版本。
- **ProbeLease**：一个带到期时间的唯一探测租约；它以单个 `probe_lease_id` 标识一个 probe group，并持有该组所有 Health aggregate 的 ID、版本和 generation。它只可由一次跨 aggregate CAS 将一个或多个 aggregate 从 `open` 转为 `half_open` 时创建。

以下不变量适用于所有预设和覆盖：

1. 权限、预算、并发、调用深度和必需验证条件是硬限制，不能由预设、覆盖或选择规则放宽。
2. 相同的有效配置 manifest、模型注册表 manifest、RoutingRequest、EligibilitySnapshot 和健康聚合版本，必须产生同一 RoutingDecision。
3. 每次实际模型调用恰好绑定一个已接受的 RoutingDecision、一个 attempt 和一次完整预算预留。
4. Run 一旦创建，保存其路由偏好和去密钥模型注册表 manifest；实时安全策略、紧急 deny 和用户授权只能以版本化事件收紧或在不可突破的上限内授权，不能静默改写该快照。
5. API Key、令牌和密钥值不得进入配置快照、日志、解释输出或事件存储。
6. 成本计算使用版本化 tokenizer/估算器、价格表、汇率和舍入规则，以最小货币单位向上取整；价格或汇率过期且没有受信任的保守上界时，候选必须被阻塞。

## 4. 配置来源、策略信封与合并规则

### 4.1 两类配置

V1 将可调整的**路由偏好**与不可放宽的**策略信封**分开处理。

- 路由偏好包括活动预设、角色候选池、模型排序、输出 Token 偏好、恢复偏好和选择规则。它们可按四层覆盖。
- 策略信封包括最大费用、最大 Token、最大 Agent 数、最大深度、最大并发、允许的提供商/模型/能力、deny 列表、最小能力层级、独立 Review、审批和最大并行候选数。它们只能收紧，不能被任何高优先级层放宽。

运行时有效上限为已解析预设请求值与所有适用策略信封的单调组合：

- 数值上限取最小值；
- allowlist 取交集；
- denylist 取并集；
- 最低 tier 取最高值；
- `require_independent_review` 与 `require_approval` 取逻辑 OR；
- 最大并行候选数取最小值。

因此，项目或 Run 可以把预设的“期望预算”从较小值调高，但绝不能超过系统、调用方或已批准授权设置的硬上限。用户若希望施加自己的硬限额，应在 `policy_envelope` 中设置，而不是只修改预设偏好。

`mandatory_guard_rules` 属于系统/调用方策略信封，始终与活动预设的 `guard_rules` 一起累加。项目、Run 和 `custom` 预设都不能删除、覆盖或停用它们。

### 4.2 优先级与来源权限

路由偏好从低到高按以下顺序解析：

```text
系统默认 -> 用户全局 -> 当前项目 -> 单次运行
```

- **系统默认**：随产品发布的版本化预设和系统策略信封。
- **用户全局**：当前用户在本机的偏好覆盖，也可额外收紧策略信封。
- **当前项目**：仓库范围的偏好覆盖，也可额外收紧策略信封。
- **单次运行**：CLI/MCP 请求内的临时偏好和收紧约束，仅作用于该 Run。

系统、调用方和交互式授权是唯一能定义其各自权威上限的来源。交互式授权只能在系统/调用方不可突破的信封内提高本 Run 的请求预算或授予特定动作，并作为新的授权事件保存；它不是普通配置覆盖。

用户可以在任意后三层直接覆盖 `economic`、`balanced`、`quality` 的偏好字段。该“覆盖”影响有效配置，不修改系统默认文件；官方基线仍可升级和恢复。

### 4.3 合并语义

1. 普通偏好映射按字段递归合并，高优先级标量覆盖低优先级标量；普通列表整体替换。
2. `selector_rules` 按稳定 `id` 管理：同 ID 的高优先级完整规则替换低优先级完整规则；新 ID 按该层显式顺序追加。规则对象内禁止 `null` 字段。
3. 要停用继承的 selector 规则，使用只含 `id` 与 `disabled: true` 的 tombstone；tombstone 不得包含 `when`、`select` 或其他动作字段。同层重复 ID 是错误。
4. `mandatory_guard_rules` 与预设 `guard_rules` 是策略信封的一部分，按全部匹配规则单调累加，不能用 `disabled` 停用，也不能被 selector 规则遮蔽。
5. 高优先级层的非规则字段写为 `null` 时，表示移除该层对该字段的覆盖并回退到较低层；运行时业务字段不接受真实空值。
6. 合并完成后才执行 schema、引用、能力、预算、规则交集和策略单调性校验；任一校验失败则整份候选配置无效。

每个有效配置记录 `schema_version`、内容哈希、逐字段来源层和策略信封来源。Run 快照保存完整的配置 manifest 及其内容哈希，并引用不可变、内容寻址的去密钥模型注册表 manifest，而不重新读取活动配置文件。

## 5. 规范性配置格式

V1 的主配置格式为 YAML。解析器使用严格类型模型，并发布等价 JSON Schema。未知字段、未知枚举值和不支持的 schema 版本均为错误，不能静默忽略。本节定义规范性字段；代码示例仅说明结构，不构成未列出字段的默认值。

### 5.1 有效配置 manifest

完整有效配置必须包含下列字段：

| 字段 | 类型 | 要求 | 语义 |
| --- | --- | --- | --- |
| `schema_version` | 整数 | 必填，值为 `1` | 配置 schema 版本 |
| `active_preset` | 枚举 | 必填 | `economic`、`balanced`、`quality` 或 `custom` |
| `registry_manifest_ref` | 内容哈希引用 | 必填 | 不可变、去密钥 Model Registry manifest |
| `policy_envelope` | Envelope 对象 | 必填 | 见第 5.2 节；只可收紧 |
| `mandatory_guard_rules` | GuardRule 数组 | 必填 | 系统/调用方强制规则，独立于活动预设且不可停用 |
| `presets` | 预设映射 | 必填 | 至少含三个内置预设和活动预设 |
| `classifier` | Classifier 对象 | 必填 | V1 固定为确定性分类器及其版本 |
| `health_policies` | HealthPolicy 映射 | 必填 | 至少一个阈值、冷却和探测限制对象 |

覆盖文件可省略任何字段；合并后的 manifest 必须完整。项目与单次 Run 覆盖不得重定义 `registry_manifest_ref` 指向的提供商、模型、价格、能力或秘密引用，只能引用已注册模型。新增提供商或模型由系统/用户级 Model Registry 更新产生新的内容哈希 manifest。

### 5.2 策略信封

`policy_envelope` 的所有字段均可省略；缺失表示该来源不额外收紧。完整有效信封必须从系统/调用方获得所有适用的硬上限。

| 字段 | 类型 | 合并方式 |
| --- | --- | --- |
| `currency` | ISO 4217 字符串 | 系统/调用方固定；不同币种必须先使用快照汇率换算 |
| `max_cost_minor` | 非负整数 | 最小值 |
| `max_total_tokens`、`max_agents`、`max_depth`、`max_concurrency` | 非负整数 | 最小值 |
| `allowed_models`、`allowed_providers`、`allowed_capabilities` | 字符串集合 | 交集 |
| `denied_models`、`denied_providers` | 字符串集合 | 并集 |
| `min_tier` | `economy`/`standard`/`high` | 最高 tier |
| `require_independent_review`、`require_approval` | 布尔值 | 逻辑 OR |
| `max_parallel_candidates` | 非负整数 | 最小值 |

### 5.3 Model Registry manifest

Model Registry 是独立、内容寻址、去密钥的 manifest。每个 provider 条目必须包含 `adapter`（`openai_responses`、`anthropic_messages` 或 `openai_compatible`）、`secret_ref`（`env:NAME`、系统密钥库引用或允许的插件引用）和启用状态。

每个模型条目必须包含：

| 字段 | 类型 | 要求 |
| --- | --- | --- |
| `id`、`provider`、`remote_model` | 非空字符串 | 必填且在 manifest 内唯一 |
| `tier` | `economy`/`standard`/`high` | 必填 |
| `capabilities` | 非空集合 | 必填 |
| `context_window`、`max_output_tokens` | 正整数 | 必填 |
| `supported_reasoning_efforts` | `none`/`minimal`/`low`/`medium`/`high`/`xhigh`/`max`/`ultra` 的集合 | 必填 |
| `price` | Price 对象 | 付费模型必填 |
| `local_zero_cost` | 布尔值 | 仅明确的本地零成本模型可为真 |

`Price` 必须含币种、每百万输入/输出 Token 的整数最小货币单位价格、已知单次工具成本上限、`estimator_id`、`effective_from` 和 `expires_at`。所有估算使用 registry 固定的 tokenizer/估算器版本、快照汇率和向上舍入规则。缺少、过期或无法保守上界的价格使付费模型不可路由。

Router 不推断未声明能力。模型缺少能力、上下文、输出限制、reasoning effort 或合格价格时，不能成为候选。

### 5.4 预设、角色与规则对象

`Classifier` 必须包含 `id`、`version`、`normalization_version` 和固定 taxonomy 版本。V1 只接受内置确定性分类器 ID；模型驱动分类器不在 V1 范围内。

每个 `HealthPolicy` 必须包含 `id`、`failure_window_ms`、`degrade_after`、`open_after`、`recovery_successes`、`cooldown_ms` 和 `max_probe_permits`；其数值约束见第 9.2 节。

每个完整预设对象包含：

| 字段 | 类型 | 要求 | 语义 |
| --- | --- | --- | --- |
| `requested_budget` | RequestedBudget 对象 | 必填 | 期望上限；最终仍受策略信封限制 |
| `roles` | RoleProfile 映射 | 必填 | 启用的模型角色配置 |
| `selector_rules` | SelectorRule 数组 | 必填，可为空 | 只决定模型偏好 |
| `guard_rules` | GuardRule 数组 | 必填，可为空 | 单调增加安全/验证约束 |
| `health_policy_ref` | 字符串 | 必填 | 指向有效 `health_policies` 条目 |

`RequestedBudget` 使用与策略信封相同的数值字段，并使用策略信封的 `currency`，但仅表示预设请求值。其有效值始终与策略信封取最小值。

V1 的模型角色为 `planner`、`coder`、`document_analyst`、`researcher`、`tester`、`reviewer` 和 `director`。`RoleProfile` 必须含 `candidates`（非空模型 ID 或 tier 引用数组）、`reasoning_effort`、`max_output_tokens`、`retries` 和 `escalate_to`。`escalate_to` 只能是更高 tier 或独立 `reviewer`/`director` 角色；验证器必须检查完整角色图无环。

`SelectorRule` 是完整对象，包含 `id`、`priority`、有限条件 `when`、一个 `select` 判别联合（恰好一个 `model` 或 `tier`）、可选 `reasoning_effort`、`fallback` 数组和 `allow_degraded`。它不得含 Review、审批、并行或权限字段。`fallback` 只包含同 tier 且不减少能力集合的模型或 tier 引用；更高 tier 只能通过角色的 `escalate_to` 和经授权的升级动作选择。

`GuardRule` 包含 `id`、有限条件 `when` 和 `constraints` 对象。`constraints` 仅允许第 5.2 节中的单调收紧字段。所有匹配 GuardRule 的约束都累加到节点契约；GuardRule 不支持 `disabled`。

SelectorRule 的 `when` 是条件字段的 AND。允许字段及类型为：`role`、`task_class`、`complexity`、`risk`、`failure_category`、`authorized_recovery_action` 和 `health_state` 的枚举集合；`required_capabilities_all` 的字符串集合；以及 `context_tokens`、`output_tokens`、`retry_level` 的闭区间整数范围。GuardRule 的 `when` 只允许节点首次进入 `Ready` 前已固定的字段：`role`、`task_class`、`complexity`、`risk`、`required_capabilities_all`、`context_tokens` 和 `output_tokens`。不存在否定、正则、脚本、动态函数或跨字段表达式。动态恢复/健康条件需要由 Recovery Controller 创建新的子节点，并在该节点首次进入 `Ready` 前重新解析 GuardRule。

`economic`、`balanced`、`quality` 是系统默认提供的预设名字，所有层均可覆盖其偏好字段。`custom` 使用同一对象结构，但在合并后必须为全部启用角色、预算、selector/guard 规则和健康策略提供完整值；它不隐式继承其他预设。

单次运行覆盖使用同一 overlay schema。它在创建 Run 前合并和验证，成功后只存入该 Run 快照，不写回用户或项目配置。

以下为非规范性结构示例：

```yaml
schema_version: 1
active_preset: balanced
registry_manifest_ref: sha256:immutable-registry-manifest
policy_envelope:
  max_cost_minor: 500
  max_concurrency: 4
presets:
  balanced:
    requested_budget:
      max_cost_minor: 500
    roles:
      planner:
        candidates: [tier:standard]
        reasoning_effort: high
        max_output_tokens: 12000
        retries: 2
        escalate_to: tier:high
    selector_rules: []
    guard_rules:
      - id: high-risk-review
        when: {risk: [high, critical]}
        constraints:
          require_independent_review: true
          min_tier: standard
```

## 6. 三种内置预设

预设调整模型选择偏好、并行候选上限、验证投入和升级时机，不影响确定性 schema 校验、测试、权限检查、策略信封或最终验收。系统分发必须为三个内置预设提供符合第 5 节的完整、版本化配置；具体货币预算是部署策略值，不能作为未声明的代码默认值。

V1 的确定性分类器输出 `task_class`（`mechanical`、`analysis`、`code_change`、`document_analysis`、`research`、`review`、`planning`、`unknown`）、`complexity`（`low`、`medium`、`high`、`unknown`）和 `risk`（`low`、`medium`、`high`、`critical`）。`mechanical` 仅指格式化、结构化提取、枚举、确定性命令执行和 schema 转换。分类器版本、规则和输出均写入 RoutingRequest；它不调用模型。

系统不可停用的 GuardRule 位于顶层 `mandatory_guard_rules`，不属于任一预设：`risk=high` 或 `complexity=unknown` 时要求至少 `standard` tier 和独立 Review；`risk=critical` 时要求 `high` tier 和独立 Review。分类失败产生 `risk=high`、`complexity=unknown`，因此不会路由到 `economy`，包括选择 `custom` 时。

| 行为 | `economic` | `balanced` | `quality` |
| --- | --- | --- | --- |
| Planner tier | `standard` | `standard` | `high` |
| 普通工作默认 tier | `economy` | `standard` | `high` |
| 低风险低复杂度 `mechanical` | `economy` | `economy` | `standard` |
| Reviewer tier | `standard` | `standard` | `high` |
| Director tier | `high` | `high` | `high` |
| 最大并行候选数 | 1 | 2 | 3 |
| 同层恢复次数 | 1 | 2 | 2 |
| 额外模型 Review | 仅不可机器验证且非 GuardRule 已覆盖的关键结果 | `code_change`、高风险结果及节点 ID 哈希模 10 为 0 的抽样结果 | 除低风险 `mechanical` 结果外的所有重要产物 |
| 高 tier 使用 | 仅 GuardRule、角色要求或经恢复控制器确认的 `capability_failure` | 同左 | 高复杂度/临界风险可在初始路由直接选择；后续升级仍需恢复控制器授权 |
| 停止偏好 | 在许可恢复耗尽后较早请求用户 | 在硬限制内平衡恢复与停止 | 在硬限制内优先执行许可的恢复路径 |

“并行候选数”不是同一节点上的多个 attempt。Graph Manager 必须在 Planning 阶段创建独立的 sibling 节点，各自拥有 RoutingRequest、RoutingDecision、预算预留和租约，再由聚合节点验证；单个节点任一时刻仍只允许一个有效 attempt。

表中的额外模型 Review 同样在 Planning 阶段编译为版本化 GuardRule，再由 Graph Manager 创建必需 Review 节点；它不是 Dispatch Router 的可选动作。

能力层级由 Model Registry 映射到实际厂商和模型，因此预设不硬编码会过时的模型名称。用户覆盖任一偏好字段时，解释输出必须显示官方基线和覆盖来源。

## 7. 两阶段约束解析与 RoutingRequest

### 7.1 Planning 阶段：冻结节点约束

Graph Manager 在节点首次进入 `Ready` 前运行确定性的 TaskClassifier 和全部匹配 GuardRule。其输出形成节点契约的一部分，至少冻结：最低 tier、允许模型/能力集合、独立 Review/审批义务、最大并行候选数、所需工具/隔离和验收条件。

如果 GuardRule 要求独立 Review，Graph Manager 在 Planning 阶段创建独立 Review 节点；Router 不得在 Dispatch 阶段添加或移除该义务。如果最大并行候选数大于 1，Graph Manager 创建 sibling 节点而不是向同一节点发放多个 attempt。节点进入 `Ready` 后，Dispatch Router 只能在已冻结契约内选择一个模型，不能改变图、权限、审批、Review 或并行结构。

V1 的 TaskClassifier 是固定的、无模型调用的确定性组件。它记录 `classifier_id`、版本、输入规范化版本、标签和置信度。分类失败、置信度不足或标签未知时，输出 `risk=high`、`complexity=unknown`，触发第 6 节的不可停用 GuardRule。

### 7.2 Dispatch 阶段：RoutingRequest

每个节点首次调度、每次同模型重试、每次 fallback 或每次经 Recovery Controller 授权的升级前，创建一个新的不可变 `RoutingRequest`。它至少包含：

- `request_id`、Run、节点、预期 attempt、父节点和角色标识；
- `routing_as_of_event_time`：创建请求的 Event Store 单调事件时间；
- 分类器输出及版本、冻结节点契约哈希、所需能力、上下文/输出 Token 需求、工具和隔离要求；
- `authorized_recovery_action`、授权的升级目标、失败指纹、重试层级、前序决策链、已耗尽候选和 fallback cursor；
- 有效配置 manifest 哈希、去密钥 Model Registry manifest 哈希、价格/汇率/估算器版本；
- `EligibilitySnapshot`：秘密引用可用性版本及布尔结果、PolicyDecision/授权信封哈希、预算账本版本和可用预留容量、生命周期熔断版本、Health aggregate 版本及有效状态；
- 是否为专用健康探测请求，以及目标 provider/model 的 `probe_lease_id` 和该 Lease 覆盖的全部 half-open aggregate ID、版本与 generation；普通工作请求不得声明自己为探测。

秘密可用性只以引用 ID、可用/不可用布尔值和版本形式保存，不保存秘密值。动态准入事实变化后，旧 RoutingRequest 不能复用，必须创建新请求。

### 7.3 规则匹配

SelectorRule 只匹配角色、任务分类、冻结能力需求、失败/恢复上下文和健康状态，并只能选择模型偏好。V1 的 `when` 只允许本规格列出的有限枚举或数值范围条件，不支持任意脚本、正则或动态表达式，因此校验器可以确定规则交集。

同一优先级的两个启用 SelectorRule 只要可能同时匹配即为配置错误，即使其模型选择恰好相同；文件顺序不是冲突解决器。GuardRule 不参与“唯一胜者”选择，所有匹配 GuardRule 已在 Planning 阶段单调累加。

## 8. 确定性路由与原子接纳

Router 按以下顺序工作：

1. 验证 RoutingRequest 引用的节点契约、配置 manifest、注册表 manifest 和 EligibilitySnapshot 均存在；价格和 registry 有效期只按 `routing_as_of_event_time` 判断，以保证精确重放。
2. 匹配唯一最高优先级 SelectorRule；未匹配时使用角色的冻结默认候选池。
3. 展开候选池，并基于固定 tokenizer/估算器、价格、汇率、附加费和向上舍入规则计算每个候选的最坏成本。
4. 按 `authorized_recovery_action` 进行强制候选过滤：`same_model_retry` 只允许前序决策的原模型；`same_tier_fallback` 只允许原 tier、未耗尽的 fallback；`capability_escalation` 只允许严格更高 tier 且匹配授权 `escalate_to`；`reviewer_node` 与 `director_node` 分别只允许独立 Reviewer 或 Director 节点的角色候选；`health_probe` 只允许持有完整 ProbeLease 的探测目标。初始路由仅受冻结节点契约约束。
5. 使用冻结节点契约和 EligibilitySnapshot 排除不满足能力、上下文、输出、reasoning effort、允许模型/提供商、秘密可用性、权限、生命周期熔断或最坏成本预留条件的候选。
6. 对常规请求排除 `open` 与 `half_open` 候选；`half_open` 只接受持有未过期、且覆盖目标 provider/model 全部当前 half-open aggregate 的完整 ProbeLease 的健康探测请求。若存在合格 `healthy` 候选，排除 `degraded` 候选，除非 SelectorRule 显式允许；没有合格 `healthy` 候选时才考虑 `degraded`。
7. 在剩余候选中按“预计最坏成本、配置顺位、稳定模型 ID”升序排序，生成候选选择。
8. 持久化 `RoutingDecisionProposed`，其中包含匹配规则、候选池、全部排除原因、成本估算、健康与准入版本、选择结果、前序决策链和 fallback cursor。
9. Scheduler 以预算账本、授权、熔断、健康、秘密可用性版本，以及完整 ProbeLease 所覆盖的 aggregate ID、版本和 generation 为 CAS 前提，原子提交“RoutingDecision 已接受 + 预算预留 + 并发额度占用 + attempt 租约”。CAS 失败时将该决策标记为 `stale`，释放未取得的资源，并基于新快照创建新的 RoutingRequest；不得用旧决策调用模型。

没有合格候选时，Router 返回带类型化原因的 `blocked`：`credential_unavailable`、`policy_denied`、`budget_unavailable`、`all_candidates_unhealthy`、`capability_unavailable` 或 `limit_exhausted`。Coordinator 订阅对应的秘密、策略、账本、健康或熔断版本变化；唤醒采用退避和次数上限，并且每次唤醒都创建新的 RoutingRequest，避免忙循环。

## 9. 恢复、fallback 与健康状态机

### 9.1 与任务生命周期的衔接

Router 不是恢复策略的决策者。它只能执行 RoutingRequest 中由 Recovery Controller 明确给出的 `authorized_recovery_action`：`initial`、`same_model_retry`、`same_tier_fallback`、`capability_escalation`、`reviewer_node`、`director_node` 或 `health_probe`。

恢复顺序与已批准的任务生命周期一致：

```text
同模型瞬时重试
  -> 同层定向修复
  -> 独立 Reviewer 或更高能力层级
  -> 重新拆分
  -> Director 或用户
```

- `transient` 先执行有界的 `same_model_retry`。当前模型被熔断、不可用或同模型重试耗尽后，Recovery Controller 才可授权 `same_tier_fallback`。
- `output_invalid` 和 `task_failure` 先创建定向修复子节点或重新聚合；只有恢复控制器基于新的失败证据把问题确认重分类为 `capability_failure` 后，才可授权模型升级。
- `capability_escalation` 的目标必须严格高于原 tier；`reviewer_node` 和 `director_node` 必须创建独立角色节点，不能把当前执行节点原地改变角色。
- 每次调用后的 fallback 都先按生命周期结算或保留旧 attempt 的费用和在途额度，再创建新的 RoutingRequest、RoutingDecision、attempt 和完整预留。调用前的候选失效同样创建新请求和新决策，不能就地改写旧决策。
- 已有结果失败后不得静默降级到较弱模型。降级只适用于尚未执行的新低风险子节点，且必须经 GuardRule 与恢复控制器共同允许。

### 9.2 Health aggregate

健康状态是独立于 Run 的、版本化的 Health aggregate/event stream。其键为 `(registry_manifest_hash, scope, provider_id, model_id?)`，其中 `scope` 为 `provider` 或 `model`。Run 事件只引用造成或观察到的健康事件，不能充当全局健康事实来源。

`HealthPolicy` 必须包含：`failure_window_ms`、`degrade_after`、`open_after`、`recovery_successes`、`cooldown_ms` 和 `max_probe_permits`。所有值为正整数，`open_after > degrade_after`，V1 要求 `max_probe_permits = 1`。时间一律使用写入 Health event stream 的单调事件时间；重放时只读取记录的时间，不读取当前墙钟。

| 当前状态 | 事件与条件 | 下一状态 |
| --- | --- | --- |
| `healthy` | 窗口内可重试瞬时失败数达到 `degrade_after` | `degraded` |
| `healthy` 或 `degraded` | 窗口内瞬时失败数达到 `open_after`，或收到带重试期的限流事件 | `open` |
| `degraded` | 连续成功数达到 `recovery_successes` | `healthy` |
| `open` | 记录的冷却时间结束，并作为目标 probe group 的一部分通过跨 aggregate CAS 获得唯一 probe lease | `half_open` |
| `half_open` | 持完整 ProbeLease 的健康探测成功 | `healthy` |
| `half_open` | 探测失败、ProbeLease 到期或探测 Worker 失联 | `open` |

Provider 与模型状态合成为有效状态：任一 `open` 则有效状态为 `open`；否则任一 `half_open` 则为 `half_open`；否则任一 `degraded` 则为 `degraded`；其余为 `healthy`。

Health Controller 为目标 `(provider, model)` 发放探测时，先读取相关 provider/model aggregate 的同一版本快照。若任一相关 aggregate 已为 `half_open` 或带有未完成的 ProbeLease，则不得为该目标创建新 probe group；Controller 必须等待该 Lease 成功、失败或到期后重新构造快照。随后它构造集合 `O`：快照中所有当前为 `open` 的相关 aggregate（provider aggregate、model aggregate 或二者）。仅当 `O` 非空、`O` 内每个 aggregate 均已到达冷却时间、且上述快照中不存在其他探测租约时，才可通过一次跨 aggregate CAS 将 **恰好** `O` 的所有 aggregate 从 `open` 转为 `half_open`，并创建一个单一的 ProbeLease。该 Lease 具有唯一 `probe_lease_id`，并保存覆盖集合中每个 aggregate 的 ID、CAS 后版本、generation、持有人和到期事件；任一 CAS 失败则不转换任何 aggregate。探测成功将同组 aggregate 转为 `healthy`，失败或 Lease 到期将同组 aggregate 转回 `open`。只有持有当前完整 ProbeLease 的请求能调度该 health probe。

每个 RoutingDecision 必须引用确切的 provider/model Health aggregate 版本和有效状态。健康事件可关联触发它的 Run/attempt，但始终写入独立 Health stream。`open` 只能由满足相同 tier、冻结能力、权限和预算要求的 fallback 接替；如需更高 tier，必须由 Recovery Controller 另行授权 `capability_escalation`。

## 10. 配置加载、解释与审计

配置加载在内存中完成。只有候选配置完整合并并通过全部校验后，才原子替换当前活动配置；加载失败保留上一份有效配置，并返回逐字段诊断。

Run 创建时保存路由偏好 manifest、去密钥 Model Registry manifest、分类器版本、价格/汇率/估算器版本和初始策略信封。Run 执行期间，普通配置编辑不改变这些冻结偏好。以下实时输入可以影响尚未调度的 attempt，但只能收紧或以明确授权形式变化：

- 紧急 deny、权限撤销和 Policy Envelope 收紧；
- Health aggregate、秘密可用性、账本和生命周期熔断状态；
- 经用户明确批准、且仍在系统/调用方硬信封内的预算或动作授权。

预算授权以版本化 `RunAuthorization` 事件表示，包含新的请求预算上限或已批准动作、授权理由和不可突破的系统/调用方信封引用；它不是普通 YAML 覆盖。上述变化均通过新的 EligibilitySnapshot 和 RoutingRequest 生效，并写入事件。恢复或用户审批后，Coordinator 按已批准生命周期重新检查策略；旧 RoutingDecision 不会被修改或复用。

系统必须提供两种只读“解释路由”能力：

- **精确重放**：输入已持久化的完整 RoutingRequest 或 RoutingDecision ID，使用其引用的 manifest 和 EligibilitySnapshot，返回与实际调度相同的结果。
- **预览解释**：输入节点描述，使用当前活动配置和确定性分类器生成一个新的临时请求。它不调用模型、工具或网络服务，但只表示当前假设下的预览，不能声称与历史实际决策完全相同。

两种输出都至少包含：

- 有效配置及每个关键字段的来源层；
- 匹配和未匹配的规则；
- 候选模型、排除原因、健康状态和最坏成本估算；
- 最终 RoutingDecision 或 `blocked` 原因。

精确重放和实际调度使用同一配置解析器、规则匹配器、成本估算器和排序器，不能维护两套逻辑。

每次有效配置变更、Model Registry manifest 变更、Health aggregate 事件、EligibilitySnapshot 和 RoutingDecision 都必须可审计。健康与注册表事件保存在各自的全局 stream；Run 审计轨迹引用相关版本与关联 ID。审计数据只保存秘密引用和不可逆指纹，绝不保存秘密值。

## 11. 配置校验

在 Run 创建前和配置重新加载时，系统必须校验：

1. schema 版本受支持，字段类型和枚举值正确，且没有未知字段；
2. 所有 Registry manifest、provider、模型、tier、角色、规则、健康策略和升级目标引用存在且内容哈希可解析；
3. 模型声明的能力、上下文、输出限制、reasoning effort、价格有效期和成本估算信息满足其被引用的角色/规则；
4. Policy Envelope 只按第 4.1 节的单调方式合并；任何来源试图放宽已适用硬限制必须失败；
5. 预设请求预算、Token、Agent、深度和并发值为有效非负值，且最终有效值不超过策略信封；
6. 升级链、fallback 图和角色委派图均不存在循环；每次 capability 升级严格提高 tier，任何 fallback 的 tier 必须等于原 tier 且能力集合不减少；
7. SelectorRule 为完整对象或合法 tombstone，GuardRule 不含 `disabled`；同层没有重复 ID，且同优先级 SelectorRule 没有可交集；
8. GuardRule 的动作均为单调收紧字段，SelectorRule 不含 Review、审批、并行、权限或预算放宽字段；
9. `custom` 在解析后提供所有必需字段和全部启用角色；
10. 秘密字段均为允许的引用格式，没有明文秘密；项目/Run 覆盖没有重定义注册表数据；
11. HealthPolicy 的阈值、冷却、permit 数和状态转换参数有效；
12. 单次 Run 覆盖和交互式授权不会突破调用方或系统硬信封。

无法确定两条规则是否冲突时，校验器必须保守拒绝，而不是依赖运行时顺序。

## 12. 测试与验收标准

### 12.1 配置与解析

- 覆盖四层偏好合并、`null` 回退、完整 SelectorRule 替换、合法 tombstone 和普通列表替换；
- 验证 Policy Envelope 的数值最小值、allowlist 交集、denylist 并集、tier 最大值和布尔/并行约束单调收紧；
- 验证任何项目或 Run 覆盖都不能放宽系统/调用方硬信封、停用 `mandatory_guard_rules`，或因选择 `custom` 绕过强制 GuardRule；
- 验证覆盖来源链、内容哈希、完整去密钥 Registry manifest 和 Run 快照可精确重放；
- 验证配置重载失败不会替换上一份有效配置；
- 验证未知字段、未来 schema、明文密钥、缺失 `custom` 字段、无效引用、过期价格、规则交集和无效 tombstone 均被拒绝。

### 12.2 路由

- 对相同 manifest、RoutingRequest 和 EligibilitySnapshot 重复执行，必须得到完全相同的 RoutingDecision；
- 验证 `routing_as_of_event_time` 固定价格/注册表有效性，历史精确重放不会因当前时间变化而改变结果；
- 验证缺失密钥、能力、上下文、输出限制、reasoning effort、预算、权限、熔断或健康条件的模型一定被排除；
- 验证 `same_model_retry` 只能选择原模型、`same_tier_fallback` 只能选择原 tier、`capability_escalation` 只能选择授权的更高 tier，且 `reviewer_node`/`director_node` 只能路由到独立角色节点；
- 验证同优先级 SelectorRule 交集、循环 fallback、任意非同 tier fallback 和能力缩减 fallback 在启动前失败；
- 验证 RoutingDecision 接纳与预算预留、并发额度、attempt 租约、秘密可用性、授权、熔断、健康和完整 ProbeLease 覆盖集合通过 CAS 原子提交；并发竞争会使陈旧决策失效并生成新请求；
- 验证精确重放与真实调度产生相同决策，预览解释明确标示为当前假设而非历史回放；
- 验证分类失败采用保守标签，不会错误选择低能力候选。

### 12.3 升级与健康

- 验证瞬时失败先执行同模型重试，只有授权后才执行同 tier fallback，且不触发无证据能力升级；
- 验证输出/任务失败先生成修复子图，能力失败、独立 Reviewer 和升级路径严格遵守 Recovery Controller 授权；
- 验证每次 fallback 都终结或保留旧 attempt 的账本状态，并创建新的请求、决策、attempt 和预留；
- 验证 `healthy`、`degraded`、`open` 和 `half_open` 的完整状态转换、严格递增阈值、冷却、provider/model 合成和唯一 ProbeLease；
- 验证 provider 与 model 同时 `half_open` 时必须原子取得覆盖两者的单一 ProbeLease，且任一相关 aggregate 已被其他 Lease 置为 `half_open` 时不会创建第二个 probe group；
- 验证普通请求不会选择 `half_open`，健康候选优先于 degraded，且 `open` fallback 必须同 tier、不能缩减能力或突破权限/预算；
- 验证多候选策略创建独立 sibling 节点，不会向同一节点并发授予多个 attempt。

### 12.4 完成条件

本模块完成时必须满足：

1. 三个内置预设及其四层偏好覆盖能被完整解析、解释和快照化，且完整系统配置不含隐式默认值。
2. `custom` 能完全 DIY，但不会借用未声明的其他预设或放宽策略信封。
3. Router 的选择、CAS 接纳、fallback、升级与阻塞结果可精确重放并具备完整证据。
4. 任何预设、覆盖、授权或健康切换都不能改变任务生命周期的硬预算、权限、并发、深度和验证约束。
5. 精确解释与真实调度共享同一决策逻辑并输出一致；预览解释明确标注其动态假设。
6. 本节定义的配置、路由、健康和并发测试全部通过。

## 13. 与后续设计的接口

后续权限与审批规格必须定义 Router 可查询的 PolicyDecision、授权信封和紧急 deny 结果。持久化、成本与可观测性规格必须提供内容寻址配置/Registry manifest、预算账本版本、价格/汇率、Health aggregate、RoutingDecision、EligibilitySnapshot 和审计事件的存储与查询接口。

在完整产品设计获得批准前，本规格不触发实现计划或编码。后续整体实施计划至少将配置/注册表、路由/健康、生命周期集成和 CLI/MCP 表面拆为可独立验收的工作阶段。
