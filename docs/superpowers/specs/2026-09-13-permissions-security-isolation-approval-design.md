# Multi-Agent Orchestrator：权限、安全、隔离与审批设计

日期：2026-09-13
状态：已于 2026-09-22 获用户书面批准

## 1. 目标

本规格定义 V1 的权限模型、策略判定、运行时隔离、密钥代理、网络出口、危险动作审批、撤销、审计和安全验收标准。

目标是让本地开发者能够让 Agent 常规地读取、编辑和测试声明的工作区，同时确保模型、网页内容、工具输出和子 Agent 不能自行扩大权限、读取密钥、逃逸工作区、绕过审批，或抹除副作用与审计证据。

V1 采用“能力代理 + Policy Engine”的设计。操作系统隔离负责强制边界，Policy Engine 负责确定性语义判定，Gateway 是唯一能代表 attempt 执行模型调用、工具调用、网络请求、Git 操作和外部副作用的入口。

## 2. 范围与非目标

本规格包含：

- `read-only`、`workspace-write`、`full-trust` 三档权限的规范语义；
- 系统、调用方、用户、项目、Run、节点与审批的权限合并规则；
- NodeSecurityContract、PolicyDecision、CapabilityGrant、ApprovalRequest 与 ApprovalGrant；
- 文件、进程、工作区、worktree、网络、密钥和外部副作用的执行边界；
- 一次性精确审批、撤销、崩溃恢复和审计证据；
- 与任务生命周期及模型路由的接口和测试标准。

本规格不定义：

- 密钥库、操作系统沙箱或 Git worktree 的具体库和供应商实现；
- 最终 CLI/MCP 命令、审批界面或身份提供商协议；
- 事件存储、Artifact Store 和长期审计保留的物理表结构；
- 对普通工作区内容的完整数据防泄漏（DLP）能力；
- 容器镜像、VM、远程执行器或企业多租户隔离。

V1 的平台后端可以不同，但不能对请求的安全边界静默降级。无法实现所需边界时必须阻塞该节点。

## 3. 威胁模型与不变量

### 3.1 信任边界

V1 信任本机已认证用户、系统/调用方策略、用户受保护的本机配置和确定性控制平面。已声明工作区按“可信开发项目”使用，但模型输出、Agent 计划、网页内容、工具输出、第三方依赖的安装脚本和来自工作区的指令文本均不拥有权限授予权。

模型调用会把节点许可的数据交给用户配置的模型提供商；启用该提供商是用户/调用方的既有策略选择。受管网页读取默认启用，但其网页内容始终是不可信输入。

密钥值是机密。普通工作区内容不是默认保密边界：启用受管公开网页读取时，恶意或受提示注入影响的 Agent 仍可能尝试把内容编码到 URL 或查询词中。V1 不把“GET/HEAD”误称为完整 DLP；敏感项目必须在受保护策略中关闭网页出口或设置目标 allowlist。

### 3.2 不变量

1. Agent、Worker、Reviewer、Director 和子 Agent 都不能授予、扩大、转让或复用权限。
2. 每个工具或模型动作都必须绑定一个当前有效的 PolicyDecision、CapabilityGrant、attempt、fencing generation 和审计因果链。
3. 子节点与子 Agent 的有效能力只能等于或小于父节点授权和系统/调用方上限。
4. 节点首次进入 `Ready` 后，其最大安全契约不可改变。审批只能激活该契约内的精确动作；契约外能力必须由新增节点声明。
5. 原始密钥值不得进入 Agent 上下文、Worker 环境、命令行、日志、事件、Artifact、解释输出或审批预览。
6. 策略拒绝、审批过期、隔离能力缺失、审计不可写或安全边界无法验证时，系统必须失败关闭，不能以更宽权限重试。
7. 每个外部副作用在真正发送前必须持久化 `EffectIntentRecorded`；结果不明时只能核对或补偿，不能自动重发。
8. `full-trust` 不是绕过系统硬 deny、预算、审计、密钥代理、fencing、外部副作用核对或一次性审批的开关。

## 4. 总体架构与授权数据流

### 4.1 组件职责

- **Policy Engine**：解析策略、规范化动作、计算 PolicyDecision，维护撤销与紧急 deny 版本。
- **Isolation Manager**：将 NodeSecurityContract 转为经验证的平台隔离配置，并报告实际能力指纹。
- **Tool Gateway**：唯一执行文件、终端、Git、网页获取和外部工具动作的入口。
- **Model Gateway**：唯一代表 Run 调用已注册模型提供商的入口；仅它可通过 Secret Broker 使用模型密钥。
- **Secret Broker**：按引用、受众、端点和操作约束临时使用密钥，绝不向 Worker 返回密钥值。
- **Approval Service**：保存审批请求、验证审批者身份，签发、撤销和原子消费一次性 ApprovalGrant。
- **Audit Writer**：将安全事件追加到事件存储，并提供去敏解释输出。

这些组件是确定性控制层的一部分。Worker Runtime 只能提交 ToolRequest、GraphExpansionProposal、检查点和候选结果，不能直接访问控制面状态或持久化最终授权结果。

### 4.2 有效能力

对一个 attempt，Policy Engine 计算：

```text
CapabilityGrant =
  NodeSecurityContract
  ∩ SystemAuthorityEnvelope
  ∩ CallerAuthorityEnvelope
  ∩ UserProtectedPolicy
  ∩ CurrentRevocationsAndEmergencyDenies
  ∩ PlatformIsolationCapabilities
  ∩ ApplicableOneShotApprovalOrUniverse
```

不需要互动审批的动作中，`ApplicableOneShotApprovalOrUniverse` 为不施加限制的全集；需要审批的动作则为相应的一次性 ApprovalGrant。项目策略和 Run 覆盖只能进一步收窄上述集合或提出审批请求，不能向其加入能力。Budget、模型、并发、深度和验证要求仍由已批准的生命周期与路由规格独立限制；安全 Grant 不会替代这些限制。

### 4.3 动作链

```text
Agent 提交 ToolRequest
  -> Gateway 规范化路径、目标和参数
  -> Policy Engine 产生 PolicyDecision
  -> deny / needs_approval / allow
  -> allow 时签发短期、不可转让 CapabilityGrant
  -> Gateway 原子验证 Grant、attempt 和 generation
  -> 执行动作，写入回执、产物或失败证据
```

Gateway 在动作执行前再次比较撤销版本、紧急 deny 版本、Grant 期限、目标、参数哈希、隔离配置指纹和 fencing generation。任一变化都会使旧决策失效；Coordinator 必须创建新的请求或让节点进入阻塞状态，不能使用陈旧决定。

## 5. 权限档位与动作类别

### 5.1 权限档位

| 档位 | 默认允许 | 仍需审批或禁止 |
| --- | --- | --- |
| `read-only` | 读取声明工作区与显式只读输入；写入系统管理的私有临时目录和 Artifact Store；受管公开网页读取 | 修改工作区、持久副作用、越界访问、密钥使用和原始网络均不允许 |
| `workspace-write` | 在声明工作区内创建和修改文件；运行受隔离的构建、测试、格式化及本地开发命令；受管公开网页读取 | 不可恢复删除、工作区外访问、系统安装、密钥使用、推送、发布、上传与其他外部写操作需要一次性审批 |
| `full-trust` | 使节点有资格请求工作区外、受管网络或主机特权能力 | 每个危险动作仍须同时通过策略、一次性审批、隔离与审计检查；Worker 仍无原始网络或控制面直连能力 |

`read-only` attempt 的系统管理临时目录不属于工作区，也不会自动成为持久产物。Worker 只能经 Artifact Store 接受后才可将结果公开给下游节点。

### 5.2 动作类别

| 类别 | 示例 | 默认处置 |
| --- | --- | --- |
| `safe_read` | 工作区读取、元数据查询、状态检查 | 在允许根内按档位允许 |
| `managed_web_read` | 受管 HTTPS `GET`/`HEAD` | 在受管代理内允许 |
| `reversible_workspace_write` | 可由 Git、快照或写前备份恢复的工作区创建、修改与删除 | `workspace-write` 允许，必须产生变更清单 |
| `irreversible_delete` | 无可靠恢复点、范围过宽或涉及重要未跟踪数据的删除 | 一次性审批 |
| `secret_use` | Gateway 使用指定密钥引用调用注册服务 | 仅在精确策略与用途约束允许时执行，值不暴露；规则要求时另行一次性审批 |
| `external_mutation` | push、发布、消息发送、提交表单、创建云资源 | 一次性审批与 EffectIntent |
| `host_privileged` | 系统安装、服务管理、设备访问、工作区外写入 | `full-trust` + 一次性审批 |
| `always_deny` | 读取原始密钥、篡改审计、绕过 Gateway、扩大 Grant、关闭沙箱 | 永久拒绝 |

动作类别由 Gateway 根据规范化工具操作和实际目标确定，而不是由模型的自然语言说明或 Shell 命令名称决定。命令中可观察的文件差异、进程意图和网络目标必须与申请动作一致；不一致时拒绝并记录安全事件。无法在执行前证明安全类别的 Shell 请求，必须按其所有可能副作用的最严格类别处理。

## 6. 安全策略格式、来源与合并

### 6.1 来源权威

安全策略自低到高可以带来默认或限制，但授权上限由下列来源固定：

1. **系统 Authority Envelope**：不可突破的产品级硬 deny、支持的权限档位和最小隔离能力。
2. **调用方 Authority Envelope**：CLI/MCP 调用者的上限和身份范围，不能高于系统上限。
3. **用户受保护策略**：工作区外、由本机用户修改的全局默认值、允许的项目身份和可委派范围。
4. **项目策略文件**：只能收窄、声明工作区需求或提出审批请求；它不是授权来源，因为 Agent 可修改仓库内容。
5. **Run 覆盖与节点契约**：只能收窄、请求或冻结已允许的最大能力。
6. **ApprovalGrant 与 RunAuthorization 事件**：只在已存在上限内激活一次性精确动作。
7. **实时撤销与紧急 deny**：只能收紧，立即影响未执行动作。

用户选择的持久策略属于第 3 层；V1 的交互式批准一律是一次性精确授权，不会把“本次同意”写为项目的长期规则。

### 6.2 规则与合并语义

有效安全 manifest 必含 `schema_version=1`、内容哈希、来源 manifest 哈希、权限档位定义、Authority Envelope、规则数组、隔离 profile 引用、密钥用途约束和规范化版本。未知字段、未知枚举、重复规则 ID、明文秘密、未解析引用或无法证明单调收紧的覆盖均为错误。

每条 `SecurityRule` 具有：

- `id`、来源层和规则版本；
- `match`：权限档位、Agent 角色、动作类别、工具 ID、attempt kind、路径根引用、网络主机、密钥引用和隔离 profile 的有限集合；
- `effect`：`allow`、`needs_approval` 或 `deny`；
- `constraints`：最小隔离能力、最大资源、必须产生 EffectIntent、审批过期时间和允许使用次数。

规则不支持脚本、任意正则、动态函数或“由模型判断”。路径匹配基于已验证的根引用，不能以字符串前缀匹配。域名仅支持精确主机和按 DNS 标签边界匹配的 `*.example.com`；IP、重定向和解析后的地址仍须单独验证。

所有匹配规则共同生效，按以下偏序决议：

```text
deny > needs_approval > allow
```

deny 集合取并集；允许工具、路径、主机、密钥受众和资源上限取交集；最小隔离能力取更强要求。无法证明两项规则兼容时，解析器保守拒绝。项目或 Run 文件不能用规则顺序、同名覆盖或 `null` 值移除高层 deny。

### 6.3 核心数据对象

`NodeSecurityContract` 在节点首次 `Ready` 前冻结，至少包含：

- `node_id`、Run、图版本、角色、权限档位和父节点契约哈希；
- 允许工作区根、只读输入、允许工具和最大能力；
- 动作类别、网络和密钥用途约束；
- 隔离 profile、资源限制、外部副作用与审批要求；
- 生效安全 manifest、策略上限和规范化版本哈希。

`PolicyDecision` 至少包含 Run、节点、预期 attempt、fencing generation、规范化 ToolRequest 哈希、`allow/deny/needs_approval`、命中规则、逐项原因、可签发 Grant 的最大范围、审批结构、安全 manifest 哈希、授权信封哈希、撤销/紧急 deny 版本和决策自身哈希。它只能由控制层签发，Worker 不能构造或伪造。

`CapabilityGrant` 绑定 `run_id`、`node_id`、`attempt_id`、`fencing_generation`、工具、动作类别、目标、参数哈希、隔离配置哈希、策略版本、签发时间和到期时间。Grant 不可转让、不可扩大，且不含秘密值。

`RunAuthorization` 是版本化事件，不是 YAML 覆盖。它记录审批者/调用方、理由、不可突破的系统/调用方信封、精确动作或 `effect_intent_hash`、作用域、最大费用或使用次数、失效时间与撤销状态。ApprovalGrant 是由该事件派生的、仅可消费一次的执行级令牌；任何两者都不能放宽 NodeSecurityContract。

## 7. 执行隔离与工作区并发

### 7.1 后端能力与失败关闭

Isolation Manager 必须向 Policy Engine 报告经验证的平台能力：工作区文件边界、只读挂载、子进程树约束、原始网络阻断、资源限制、临时目录隔离和工作树隔离。若节点契约所需能力缺失，产生 `Blocked(isolation_unavailable)`；禁止用无隔离的本地进程替代。

文件授权必须由操作系统级的挂载、句柄或访问控制落实。Gateway 和后端在打开目标时都要重新规范化路径并防止符号链接、junction、reparse point、挂载点、硬链接和大小写别名逃逸。`.git`、事件存储、策略文件、密钥配置和 Orchestrator 控制目录默认受保护，不能由普通 Worker 原始修改。

### 7.2 进程、临时文件和网络

每个 attempt 运行在独立的受限进程树和私有临时目录中，并受 CPU、内存、执行时间、进程数、文件大小、输出量和打开文件数上限约束。Worker 不拥有原始网络套接字；模型调用、网页读取、Git 和外部工具动作均通过 Gateway IPC 执行。

暂停、取消、租约失效或撤销采用先关闸、后终止的顺序：先阻止新工具动作，再在安全检查点结束进程树。无法证明已停止的 attempt 进入既定的 `OutcomeUnknown` 路径，继续占用预算和并发额度。

### 7.3 共享工作区与并行修改

同一共享工作区一次只授予一个写租约。`workspace-write` attempt 在写前固定基线 manifest，并为可恢复目标建立写前备份或写时隔离层，持续产生变更清单和可恢复证据。若无法在动作前证明删除具备这种恢复点，Gateway 必须按 `irreversible_delete` 处理并在执行前请求审批。可恢复变更可在策略允许下合并到工作区。

并行修改只能在独立 Git worktree 或等效写时隔离目录中进行。各 worktree 的 Worker 只可修改其工作树文件；受管 Git Gateway 负责需要访问仓库控制元数据的操作。合并是一个独立、串行且受验证的节点，冲突不得静默覆盖。隔离目录只能在 attempt、预算、租约、审计和外部副作用状态均已终结后清理。

工作区外写入必须由在 `Ready` 前已声明最大能力的 `full-trust` 节点，在取得一次性精确 ApprovalGrant 后通过新 attempt 执行。

## 8. 密钥、模型与网络能力

### 8.1 Secret Broker

Model Registry 和安全策略只保存 `secret_ref`。Secret Broker 按引用、Gateway、目标端点、操作类型、Run/项目范围和期限验证用途后，在自身进程内使用解密值。Worker、Agent、Shell 和审批者都不会获得原始值。

Model Gateway 可用已注册的模型密钥调用固定 HTTPS 提供商端点；Git Gateway 可用已批准的凭据执行受管 Git 操作。若第三方工具只能通过向子进程注入密钥运行，V1 返回 `credential_delivery_unsupported`，不能自动退化为环境变量、命令行参数或文件注入。

日志、错误、模型响应和 Artifact 在写入前执行秘密指纹与已知值脱敏；审计只记录引用 ID、用途、受众、时间、策略与结果。脱敏是补充控制，秘密不进入 Worker 才是主控制。

### 8.2 网络通道

V1 只存在三类网络通道：

1. **Model Gateway**：仅连接 Model Registry 声明的 HTTPS 端点；每次重定向、DNS 解析和连接目标都重新校验。
2. **Managed Web Read**：仅无凭据 HTTPS `GET/HEAD`；禁止请求体、Cookie、认证头、代理覆盖、上传、私网/localhost/链路本地/云元数据地址，以及未经检查的重定向和 DNS 重绑定。代理限制主机、响应类型、大小、跳转数、速率和缓存生存期。
3. **External Mutation**：上传、POST、PUT、PATCH、DELETE、消息发送、push、发布、表单提交和云资源变更；必须具有一次性 ApprovalGrant 和 EffectIntent。

网页内容、重定向地址和下载文件都视为不可信输入。它们不能成为策略、系统提示、工具指令或密钥用途的权威来源。敏感项目可通过用户受保护策略关闭 Managed Web Read 或使用域名 allowlist。

每项 External Mutation 还必须声明可恢复性：支持供应商幂等键、非幂等但可查询结果，或既不可幂等也不可查询。最后一类默认禁止自动执行，只能在用户精确批准后执行一次；三类动作都必须在审计可安全追加时先写入 EffectIntent，并在回执缺失时进入核对路径。

## 9. 审批、消费、撤销与恢复

### 9.1 审批状态机

当 PolicyDecision 为 `needs_approval` 时，Gateway 持久化 `ApprovalRequested`，其中包含动作类别、规范化目标、可读参数预览、参数哈希、预期副作用、恢复方式、关联密钥引用、来源 Run/节点/attempt、策略版本和到期时间。

- 调度前发现缺权时，节点直接从 `Proposed/Ready` 进入 `AwaitingApproval`，不创建 attempt。
- 运行中发现缺权时，Worker 先保存检查点并以 `Yielded` 或 `Rejected` 结束；节点进入 `AwaitingApproval`。
- Coordinator 以已批准生命周期的 `UserDecisionRequired` 语义关闭 Run 新调度，安全处置在途 attempt 后将 Run 置为 `AwaitingUser`。
- 本机用户或经认证、具备批准权的 MCP 调用方可批准、拒绝或撤销。Agent、Worker、Reviewer、Director 和被请求工具都不能审批。
- 无交互通道时，CLI/MCP 返回结构化 `approval_request_id`；Run 保持等待，直到调用方明确决定。

批准不会直接启动旧 attempt。它只能触发 `BlockingConditionCleared -> Ready`；Coordinator 必须重新检查权限、预算、图版本、隔离能力和撤销状态，并为下一次调度创建新的 RoutingRequest、RoutingDecision、PolicyDecision 和 attempt。

批准只能覆盖节点契约中已经声明的动作。若要访问契约外能力，Graph Manager 必须接受一个具有新安全契约的子节点；用户对旧节点的批准不能隐式迁移到该子节点。

### 9.2 一次性 ApprovalGrant

用户批准生成带单一使用次数的 `ApprovalGrant`，绑定 `approval_request_id`、`effect_intent_hash`、Run、节点、预期 attempt、动作、目标、参数哈希、策略/撤销版本和到期时间。它不是可泛化的“本 Run 都允许”或“项目永久允许”令牌。

真正执行外部或特权动作前，Gateway 必须原子完成：

1. 验证 Grant 未过期、未撤销、未消费，且节点、attempt、generation、目标、参数、隔离与策略版本仍匹配；
2. 记录 `EffectIntentRecorded`，其中包含供应商幂等键、请求摘要和最坏费用；
3. 将 ApprovalGrant 标记为 `Consumed`；
4. 执行调用并记录回执或结果不明。

消费后崩溃或超时不得重新签发或自动重用 Grant。它进入 `OutcomeUnknown/AwaitingReconciliation`；仅可查询的效果须核对，不可查询且非幂等的效果等待人工处置，完全遵守已批准的生命周期规则。

### 9.3 撤销与策略变更

紧急 deny、策略收紧、密钥撤销和用户撤销都以版本化事件写入。它们立即阻止尚未执行的动作、Grant 签发和新工具调用。正在进行的可取消进程停止在安全点；已经发出的外部动作不被误报为“已撤销”，只能核对、补偿或进入人工处置。

普通配置更新不会改写 Run 的冻结安全 manifest。只有撤销、紧急 deny 和在上限内的批准事件可影响尚未执行的动作；每次影响都要求新的 PolicyDecision 和 CAS 重检。

## 10. 故障处置与审计

### 10.1 安全故障

| 条件 | 处置 |
| --- | --- |
| `policy_denied`、审批拒绝或过期 | 节点进入 `Blocked` 或保持 `AwaitingApproval`；不进入自动恢复阶梯 |
| `credential_unavailable` 或 `credential_delivery_unsupported` | 节点 `Blocked`，等待受支持配置或用户决定 |
| `isolation_unavailable` | 节点 `Blocked`，不得降级执行 |
| Gateway/沙箱发现越界、Grant 伪造或绕过尝试 | 停止进程树、保存证据、节点安全阻塞；不得自动重试同一动作 |
| Effect 结果不明 | `OutcomeUnknown/AwaitingReconciliation`，不得自动重发 |
| 审计或事件存储不可安全追加 | 停止签发新 Grant、审批、attempt 租约和副作用；按生命周期进入 `Fatal` 或控制平面 `Quarantined` |

安全边界违例本身不允许模型通过新节点、换模型、改变角色或降低隔离来绕过。若控制平面完整性仍可信，用户可以创建符合策略的替代节点；若完整性不可信，只有人工诊断和新 Run 可以继续。

### 10.2 审计与解释

安全审计事件至少包括：

- `PolicyEvaluated`、`ToolRequestReceived`、`CapabilityGrantIssued/Expired/Revoked`；
- `ApprovalRequested/Granted/Denied/Expired/Revoked/Consumed`；
- `RunAuthorizationRecorded` 及其授权信封版本；
- `IsolationConfigured`、`IsolationViolationDetected`、进程树启动/终止；
- 秘密引用用途、网络请求去敏摘要、`EffectIntentRecorded` 与 `EffectReceiptRecorded`；
- 策略、撤销、紧急 deny 和隔离能力版本变化。

所有事件记录主体、因果事件、Run/节点/attempt/Agent、图版本、fencing generation、Run 内单调序号、策略/配置/注册表/授权版本、时间和不可逆摘要。审计流只追加；模型和 Agent 无法删除、修改或伪造其顺序。用户可请求只读解释：它必须显示允许/拒绝原因、命中规则、审批要求和安全版本，但不泄漏密钥、被保护路径内容或敏感请求正文。

## 11. 配置校验、测试与验收

### 11.1 配置与判定测试

- 验证来源权威、deny 优先、集合交集、不可放宽性、内容哈希和 Run 快照重放；
- 验证项目文件、Run 覆盖、模型输出和子 Agent 均不能自我扩权；
- 验证未知字段、动态脚本、明文秘密、模糊路径、非法域名通配和冲突规则均被拒绝；
- 对相同规范化请求和安全快照重复判定，必须得到相同 PolicyDecision；Router、Scheduler 和 Gateway 必须引用同一决策/版本并在执行前 CAS 重检。
- 验证撤销、审批过期、并发批准或并发消费发生在 CAS 前后时，不会产生未经授权的 attempt 或副作用；旧 RoutingDecision 被标为 `stale`，系统创建新请求。

### 11.2 隔离与网络测试

- 覆盖 `..`、符号链接、junction、reparse point、挂载点、硬链接、大小写别名和 Git 控制目录逃逸；
- 验证进程树、临时目录、资源限制、租约失效和取消不能留下未管理子进程；
- 验证共享工作区写租约、隔离 worktree 并行、合并冲突、清理时机和不可恢复删除审批；
- 验证 Worker 无原始套接字，且 Managed Web Read 阻止 SSRF、私网、重定向绕过、DNS 重绑定、认证材料和上传；
- 验证网页提示注入不能改变策略、Grant、工具或密钥用途。

### 11.3 密钥、审批与恢复测试

- 验证密钥不会出现在环境、子进程、提示、日志、事件、错误或 Artifact；
- 验证同一 ApprovalGrant 在并发调用中只会被一次原子消费，参数/目标改变、过期、撤销和 generation 变化均失败；
- 验证审批请求、Grant 消费、EffectIntent、回执和崩溃恢复在每个写入边界都可重放；
- 验证消费后结果不明只能核对，不能自动重发；
- 覆盖 EffectIntent 前、Intent 后发送前、发送后回执前、回执后状态提交前、取消、暂停和重复投递的崩溃矩阵；逐项验证幂等、预算/并发保留和 reconciliation-validation attempt 限制；
- 验证审计篡改、事件链损坏或审计不可写时停止新授权与副作用并进入正确的阻塞、`Fatal` 或 `Quarantined` 状态。

### 11.4 完成条件

本模块完成时必须满足：

1. 三档权限、每个动作类别和一次性审批均有确定、可重放的语义。
2. 任何 Agent、预设、路由升级、子 Agent、项目文件或 Run 覆盖都无法突破系统/调用方安全上限。
3. 请求的文件、进程、网络、密钥和工作区隔离要么被平台后端强制执行，要么节点失败关闭。
4. 外部副作用始终具有不可变意图、一次性授权、回执或结果不明的核对路径。
5. 审计在不泄漏秘密的前提下能够解释每次安全判定、授权、拒绝、撤销和副作用。
6. 本节的配置、隔离、网络、密钥、审批、恢复和并发测试全部通过。

## 12. 与已批准规格和后续设计的接口

本规格为任务生命周期提供节点/工具权限判定、审批要求、隔离配置、危害事件和安全阻塞原因。它严格遵守生命周期中的 `AwaitingApproval`、`AwaitingUser`、fencing、EffectIntent、OutcomeUnknown、AwaitingReconciliation、并发租约和 worktree 并行规则。

本规格为预设与模型路由提供版本化 `PolicyDecision`、Authority Envelope、RunAuthorization、秘密可用性布尔结果、紧急 deny 版本和可用于 CAS 的撤销版本。路由的 `require_approval` 只能使节点在 Planning 阶段形成审批义务；Dispatch Router 不能自行增加、移除或绕过它。用户批准的预算或动作仍不得突破系统/调用方信封，且旧 RoutingDecision 不得复用。

后续持久化、成本、可观测性和测试设计必须定义安全事件、策略 manifest、审批、Grant、EffectIntent、密钥用途、隔离能力、审计查询和保留策略的存储与恢复接口。

在完整产品设计获得用户批准前，本规格不触发实施计划或编码。
