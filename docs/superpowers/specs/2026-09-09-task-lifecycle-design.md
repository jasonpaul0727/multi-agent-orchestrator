# Multi-Agent Orchestrator：任务生命周期与失败恢复设计

日期：2026-09-09
状态：设计内容已确认并完成自审，等待用户审阅书面规格

## 1. 目标

本设计定义 Multi-Agent Orchestrator V1 的任务生命周期、动态任务图、调度边界、失败恢复、完成判定和测试要求。V1 的首要目标是可控可靠，其次才是成本和自治程度。

系统采用事件驱动 DAG，而不是固定的全局线性流水线。Agent 可以在执行期间继续拆分任务，但所有变更都必须经过确定性控制层；模型不能直接修改运行状态、突破权限或绕过预算。

## 2. 范围

本规格涵盖：

- 一个用户任务从接收到交付的完整生命周期；
- DAG 节点、依赖、运行状态和追加式扩展；
- 调度、并发租约、检查点和崩溃恢复；
- 失败分类、重试、升级、熔断和人工介入；
- 节点输入、输出、验证证据和完成语义；
- 生命周期引擎的测试与验收标准。

以下内容不在本规格中定义，将在后续设计阶段完成：

- `economic`、`balanced`、`quality` 和 `custom` 的具体数值；
- 厂商与模型的实际路由表、价格表和 API 适配细节；
- 三档权限及单项危险动作的完整策略格式；
- 事件存储和 Artifact Store 的具体数据库、表结构及保留周期；
- CLI 与 MCP Server 的命令和协议表面。

上述后续设计可以调整策略参数，但不能削弱本规格中的状态一致性、审计、权限和预算硬约束。

## 3. 核心原则

1. **事件是唯一事实来源。** 所有有效状态变化先持久化为事件，再产生可调度行为。
2. **图只追加和细化。** 已进入执行的历史不会被模型重写；修复和替代执行路径以新增子节点表达。
3. **执行与判定分离。** Worker 只能提交候选结果，Verifier 决定节点是否满足验收条件。
4. **硬限制不可升级绕过。** Director 和高能力模型同样受权限、预算、并发和深度上限约束。
5. **失败必须收敛。** 所有重试和升级都有上限；相同失败反复出现时必须熔断。
6. **恢复不丢证据。** 崩溃恢复保留已接受的节点结果、产物、费用和验证记录。
7. **最终结果对齐原始目标。** Final Review 不以“所有节点变绿”代替对用户目标的复核。

## 4. 总体生命周期

每次用户请求创建一个 `Run`。正常主路径为：

```text
Intake -> Planning -> Running -> FinalReview -> Delivered
```

`Run` 还可以进入以下控制状态：

- `AwaitingUser`：等待用户批准权限、预算、降级交付或补充必要信息；
- `Paused`：由控制层安全暂停，可从事件存储恢复；
- `Failed`：恢复阶梯已用尽，且不能形成可接受交付；
- `Cancelled`：用户或上层调用方明确取消；
- `Fatal`：控制层一致性、事件存储或任务图发生不可安全继续的错误。

`Delivered`、`Failed`、`Cancelled` 和 `Fatal` 是终态。终态不可原地恢复；如需继续工作，必须基于原 Run 的证据创建一个新 Run，并记录来源关系。终态 Run 仍可追加费用结算、外部副作用核对和审计事件，但不得新增工作节点、发出租约或返回活动态。

### 4.1 阶段职责

- `Intake`：保存原始目标、输入引用、工作区快照、调用方约束和默认策略版本。
- `Planning`：生成最小可执行初始 DAG，为每个节点补齐契约并进行首次预算估算。
- `Running`：按依赖和策略并行调度节点，允许受控地追加和细化未完成工作。
- `FinalReview`：独立核对原始目标、必需产物、验证证据、未解决风险和预算记录。
- `Delivered`：生成结构化交付清单和面向用户的结果，不再接受新节点。

### 4.2 Run 状态转换

下表是 Run 的权威转换规则；表中“活动态”指 `Intake`、`Planning`、`Running` 和 `FinalReview`。

| 当前状态 | 触发事件 | 下一状态 | 约束 |
| --- | --- | --- | --- |
| 新建 | `RunCreated` | `Intake` | 原始请求与策略版本必须在同一事务中保存 |
| `Intake` | `InputAccepted` | `Planning` | 输入引用与工作区快照完整 |
| `Planning` | `InitialGraphCommitted` | `Running` | 初始图已校验并持久化 |
| `Running` | `GraphQuiesced` | `FinalReview` | 固定本次 `review_graph_version` |
| `FinalReview` | `FinalReviewPassed` | `Delivered` | Review 节点已成功，交付清单已持久化 |
| `FinalReview` | `ReviewRepairExpansionCommitted` | `Running` | 只允许 Final Reviewer 发起有界修复扩展 |
| 活动态 | `UserDecisionRequired` | `AwaitingUser` | 保存唯一的 `resume_state` 并停止新调度 |
| 活动态 | `PauseCommitted` | `Paused` | 保存唯一的 `resume_state`；活动 attempt 按第 8 节处置 |
| `AwaitingUser` | `UserDecisionAccepted` | `resume_state` | 重新执行策略、预算与图版本校验 |
| `Paused` | `ResumeCommitted` | `resume_state` | 重建状态后重新执行就绪检查 |
| 任意非终态 | `RecoveryExhausted` | `Failed` | 没有获准的降级交付或可行恢复路径 |
| 任意非终态 | `CancelCommitted` | `Cancelled` | 停止新调度，并确认或隔离在途副作用 |
| 任意非终态 | `InvariantViolation` 或 `EventChainCorrupt` | `Fatal` | 仅在该终态能够可靠持久化时写入 |

`AwaitingUser` 和 `Paused` 不是可任意跳转的中转站；它们只能返回进入时记录的 `resume_state`。恢复前策略版本发生变化时，必须记录新版本并重新检查所有尚未执行的节点。

## 5. DAG、节点与 attempt 状态

一个 `Run` 包含一个有向无环图。边 `A -> B` 表示 B 必须等待 A 满足依赖条件。节点表示可独立调度、产出和验证的工作单元。

节点代表需要被满足的逻辑工作单元，attempt 代表对该节点的一次具体执行。一次 attempt 失败不等于节点立即进入终态 `Failed`。

节点正常路径为：

```text
Proposed -> Ready -> Running -> Verifying -> Succeeded
```

节点还可进入：

- `RetryPending`：一次 attempt 已结束，正在等待退避时间或下一恢复动作；
- `WaitingChildren`：当前 attempt 已结束或让出，节点正在等待必需子节点；
- `AwaitingApproval`：所需动作超过当前授权，但允许用户批准后继续；
- `Blocked`：预算、密钥、工具或环境条件不满足，当前不可执行；
- `AwaitingReconciliation`：外部副作用结果不明，禁止自动重试；
- `Failed`：节点的有界恢复路径已耗尽，无法满足验收条件；
- `Skipped`：仅适用于被明确标记为可选的节点；
- `Superseded`：仅适用于要求返修、已被下一轮审查取代的 Final Review 节点；
- `Cancelled`：Run 取消，或尚未执行的可选节点被明确移除。

`Succeeded`、`Failed`、`Skipped`、`Superseded` 和 `Cancelled` 是节点终态。终态节点不会改回 `Ready`。

### 5.1 Node 状态转换

| 当前状态 | 触发事件 | 下一状态 | 约束 |
| --- | --- | --- | --- |
| `Proposed` | `NodeBecameEligible` | `Ready` | 基线依赖、契约和策略检查通过，预算上界可容纳 |
| `Proposed` 或 `Ready` | `ApprovalRequired` | `AwaitingApproval` | 不创建 attempt |
| `Proposed` 或 `Ready` | `ExecutionBlocked` | `Blocked` | 记录可解除或不可解除的阻塞原因 |
| `Ready` | `AttemptDispatched` | `Running` | 与预算预留、额度占用和租约授予原子提交 |
| `Running` | `CandidateSubmitted` | `Verifying` | 仅接受当前有效 fencing generation 的候选结果 |
| `Running` | `AttemptRetryScheduled` | `RetryPending` | 当前 attempt 已确定结束且允许重试 |
| `Running` | `SideEffectFreeAttemptOrphaned` | `RetryPending` | 旧 attempt 保持 `OutcomeUnknown` 并继续计入预算与并发 |
| `Running` | `ExpansionCommitted` | `WaitingChildren` | 当前 attempt 以 `Yielded` 结束 |
| `Running` 或 `Verifying` | `AttemptYieldedForApproval` | `AwaitingApproval` | 当前 attempt 先以 `Yielded` 或 `Rejected` 结束 |
| `Running` 或 `Verifying` | `AttemptYieldedForBlocker` | `Blocked` | 当前 attempt 先结束并保存检查点 |
| `Running` | `EffectOutcomeUnknown` | `AwaitingReconciliation` | 保留预算和并发占用，禁止自动重试 |
| `Verifying` | `ValidationPassed` | `Succeeded` | 全部必需验收条件通过 |
| `Verifying` | `RetryScheduled` | `RetryPending` | 验证拒绝，但仍有同节点有界重试策略 |
| `Verifying` | `RecoveryChildrenCommitted` | `WaitingChildren` | 修复或重新拆分以新增子图表达 |
| `Verifying` | `ReviewRepairExpansionCommitted` | `Superseded` | 仅限 Final Review 节点，并与 Run 回到 `Running` 原子提交 |
| `RetryPending` | `BackoffElapsed` | `Ready` | 重新检查策略、图版本和预算容量 |
| `WaitingChildren` | `RequiredChildrenSucceeded` | `Ready` | 父节点必须运行新的聚合 attempt，不能直接成功 |
| `AwaitingApproval` 或 `Blocked` | `BlockingConditionCleared` | `Ready` | 重新执行全部就绪检查 |
| `AwaitingReconciliation` | `EffectConfirmed` | `Ready` | 下一 attempt 被限制为只读取回执并验证，不得重放原副作用 |
| `AwaitingReconciliation` | `NoEffectConfirmed` | `RetryPending` | 只有确认副作用未发生后才能重试 |
| 任意非终态 | `NodeRecoveryExhausted` | `Failed` | 不存在可执行恢复分支 |
| `Proposed` | `OptionalNodeSkipped` | `Skipped` | 仅限契约明确标记为可选的节点 |
| 任意非终态 | `NodeCancelled` | `Cancelled` | Run 已取消，或节点是尚未执行且不再需要的可选节点 |

### 5.2 Attempt 状态转换

每次调度生成不可复用的 `attempt_id` 和单调递增的 `fencing_generation`。

| 当前状态 | 触发事件 | 下一状态 | 约束 |
| --- | --- | --- | --- |
| 新建 | `AttemptLeaseGranted` | `Leased` | 与节点、预算和并发事件原子提交 |
| `Leased` | `WorkerStarted` | `Running` | Worker 确认 fencing generation |
| `Leased` 或 `Running` | `CandidateDurablyStored` | `CandidateSubmitted` | 提交时租约必须有效且 generation 最新 |
| `Running` | `ExpansionAccepted` | `Yielded` | Worker 让出控制，父节点等待新增子图 |
| `CandidateSubmitted` | `CandidateAccepted` | `Accepted` | 节点结果 CAS 的唯一胜者 |
| `CandidateSubmitted` | `CandidateRejected` | `Rejected` | 验证证据持久化 |
| `Leased` 或 `Running` | `ExecutionFailureConfirmed` | `Rejected` | 已确认没有未决外部副作用 |
| `Leased` 或 `Running` | `OutcomeCannotBeDetermined` | `OutcomeUnknown` | 保留额度与预算，进入核对流程 |
| `OutcomeUnknown` | `OutcomeReconciled` | `Reconciled` | 根据节点当前状态和副作用类别驱动 Node 转换或仅完成结算；不得覆盖已接受结果 |
| 任意非终态 | `CancellationConfirmed` | `Cancelled` | 必须确认本地执行停止或外部请求已撤销 |

`Yielded`、`Accepted`、`Rejected`、`Reconciled` 和 `Cancelled` 是 attempt 终态。租约过期会使 generation 失效，但不会凭空证明执行已停止；控制层必须将 attempt 确认为 `Rejected`/`Cancelled`，或转入 `OutcomeUnknown`。

### 5.3 就绪条件

节点只有同时满足以下条件才可进入 `Ready`：

1. 所有必需依赖均已 `Succeeded`；
2. 节点契约完整且图通过无环校验；
3. attempt 的最坏情况费用能够在剩余预算中完整预留；
4. 权限、模型、工具和隔离策略允许执行；
5. Run 未处于暂停、人工等待或终态；
6. 没有同一节点的有效执行租约或尚未核对的非幂等副作用。

可选依赖被跳过或失败时，Graph Manager 必须根据节点契约明确判断下游是否仍可运行，不能隐式视为成功。

## 6. 追加式图扩展

V1 采用“仅允许追加和细化节点”的动态图策略。

Agent 可以提交 `GraphExpansionProposal`，但只有 Graph Manager 可以接受并写入图。提案必须包含新增节点、依赖边、父节点、理由、验收条件和预算估算。

Graph Manager 接受提案前必须验证：

- 新图保持无环；
- 新节点 ID 在 Run 内唯一；
- 已进入 `Ready`、已开始或已终结节点的基线依赖和验收条件不可改写；
- 父节点尚未 `Succeeded`；
- 调用深度、Agent 总量和并发上限未被突破；
- 额外 Token、金额和时间预算可被预留；
- 新节点所需权限与工具符合策略；
- 新节点不会重复一个已成功且产物仍有效的工作单元。

父子关系表示任务分解，不等同于依赖边。一个运行中的节点需要细化或修复时，当前 attempt 必须先以 `Yielded` 或 `Rejected` 结束；Graph Manager 随后只追加子节点、子图和控制层管理的 completion links，并把父节点置为 `WaitingChildren`。新增子节点可以继承父节点的已满足依赖，但不能依赖尚未成功的父节点本身。

completion link 是追加式恢复关系，不会改变父节点原有目标、输入、基线依赖或验收条件。所有必需子节点成功后，父节点重新进入 `Ready`，通过一个新的聚合 attempt 生成候选结果，然后才进入 `Verifying`。因此修复 attempt 的失败证据保持不变，下游仍只依赖最终成功的逻辑父节点，不需要把终态失败节点“复活”。

completion-link 产物属于追加式恢复上下文，不会覆盖节点冻结的基线输入。每个聚合 attempt 在调度时固定一个不可变的 `attempt_input_manifest`，其中列出基线输入、选用的子节点产物、内容哈希和 `graph_version`；Verifier 只验证该 manifest 对应的候选结果。

图的历史版本不可覆盖。每次成功扩展产生递增的 `graph_version`，并记录提出者、验证结果和对应事件序号。读侧通过事件重放或已验证快照得到当前图。

普通扩展只在 Run 为 `Running` 时接受。Scheduler 授予租约时必须针对当前 `graph_version` 做 CAS；版本变化时重新计算节点就绪状态。进入 `FinalReview` 时冻结一个 `review_graph_version`，拒绝普通 Agent 的扩展。只有 Final Reviewer 可以发起受修复次数上限约束的 repair expansion；接受后 Run 回到 `Running` 并生成新图版本。

## 7. 核心组件与数据流

### 7.1 组件

- `Run Coordinator`：管理 Run 状态、终止条件、暂停和恢复。
- `Graph Manager`：维护 DAG、校验扩展提案并生成图版本。
- `Scheduler`：根据就绪条件、优先级、关键路径和资源额度分派节点。
- `Policy Engine`：执行权限、预算、模型、工具、深度和危险动作策略。
- `Budget Ledger`：按 Run、节点和 attempt 原子预留、结算与释放费用。
- `Lease Manager`：分配 fencing generation，管理续租、失效和在途额度。
- `Recovery Controller`：分类失败、计算失败指纹并选择下一层有界恢复动作。
- `Agent Registry`：记录 Agent 实例、父子关系、深度、状态和累计数量。
- `Worker Runtime`：承载 Agent、模型和工具调用，不直接写全局状态。
- `Verifier`：根据节点契约检查产物和证据，提交状态判定。
- `Event Store`：保存事件序列，是运行状态的唯一事实来源。
- `Artifact Store`：保存补丁、文件、分析结果、测试报告、原始响应和内容哈希。

### 7.2 执行数据流

1. Coordinator 创建 Run，并将输入与约束写入事件存储。
2. Planner 提交初始图；Graph Manager 校验并持久化图版本。
3. Scheduler 计算可运行节点，并通过 Policy Engine 检查是否容得下最坏情况费用。
4. Scheduler 在一个原子事件批次中完成 attempt 预算预留、并发额度占用、租约授予和节点 `Running` 转换。
5. Worker Runtime 获取带 fencing generation 的 attempt，运行模型和工具，将中间事件与产物写入存储。
6. Worker 提交候选结果；Verifier 根据验收条件接受或拒绝。
7. 接受结果后，Coordinator 更新可运行集合；拒绝结果后进入失败分类与恢复流程。
8. 必需工作完成后创建独立 Final Review 节点并固定审查图版本。
9. Final Review 通过后，Coordinator 生成交付事件与证据清单。

Worker 提交的是请求或候选事件，最终状态事件只能由确定性控制组件在完成校验后追加。

## 8. 调度、并发和一致性

并发额度分为四层：系统全局、单个 Run、单个模型厂商、单类工具。一个节点必须同时取得所需额度才能开始执行。

Scheduler 使用有期限的执行租约：

- 租约包含 `run_id`、`node_id`、`attempt_id`、所有者、`fencing_generation` 和过期时间；
- 有效租约期间，Scheduler 不会为同一节点分配另一个有效 generation；
- Worker 必须续租，失联后租约自然过期；
- 只有提交时仍持有当前有效 generation 的 attempt 才能进入验证；迟到输出作为审计证据保存，不能成为节点结果；
- 旧 attempt 被确认停止或继续占用预算与并发额度后，Scheduler 才能创建替代 attempt。

系统承诺的是“单一节点结果接受”，而不是候选只能提交一次，也不是底层副作用的绝对 exactly-once 执行。模型或工具调用在崩溃边界上可能重发，因此：

- 所有内部写入使用 `attempt_id` 或专用幂等键；
- 接受节点结果以 `node_id`、当前 generation 和节点状态执行比较并交换，只有一个 attempt 能胜出；
- 外部调用前先持久化 `EffectIntentRecorded`，其中包含供应商幂等键、请求哈希和最坏费用；返回后持久化回执；
- 不支持幂等键但可以查询结果的操作必须串行执行；结果不明时进入 `AwaitingReconciliation`，不得自动重试；
- 既不支持幂等也无法核对结果的外部副作用默认禁止自动执行，只能在用户明确批准后执行一次，结果不明时等待人工核对；
- Artifact Store 以内容哈希去重，但保留各 attempt 的来源记录。

硬预算按 attempt 记账。调度事务必须保证 `已结算费用 + 所有未释放预留 <= Run 硬预算`。租约过期、Worker 失联或迟到结果不会自动释放预留；所有实际费用都从原 attempt 的预留中结算。只有获得明确终止或账单回执后才能释放余额。

结果不明的 attempt 继续占用相应并发额度。对于已确认不包含非幂等副作用的孤立 attempt，可以在旧 attempt 仍计入额度和预算的前提下创建替代 attempt；旧、新 attempt 必须同时计入系统、Run、厂商及工具额度，没有剩余额度时必须等待核对。包含未决非幂等副作用的节点保持 `AwaitingReconciliation`，无论是否有剩余额度都禁止替代 attempt。租约失效不能用于规避硬并发上限。

暂停或等待用户决定采用先关闸、后静止的顺序：Coordinator 先持久化调度禁令，不再发出新租约，再要求可取消的活动 attempt 在安全检查点结束。不能确认已经停止的 attempt 转为 `OutcomeUnknown`，继续占用预算和并发额度。全部在途 attempt 均已终结或进入核对状态后，才提交 `PauseCommitted` 或 `UserDecisionRequired`。暂停期间到达的迟到输出只保存为证据，不绕过 fencing 规则。

对同一工作区的写操作，默认策略为串行执行。只有在独立 Git worktree 或等效隔离目录中，修改节点才可并行。合并动作本身是单独节点，必须处理冲突并接受验证。

## 9. 节点与 Agent 契约

### 9.1 节点契约

节点进入 `Ready` 前必须具备以下字段：

- `node_id`、`parent_id`、节点类型和依赖列表；
- 清晰、单一的目标；
- 输入引用及其版本或内容哈希；
- 预期产物和验收条件；
- 必需或可选标记；
- Agent 角色、模型策略和 reasoning effort；
- 工具白名单、权限档位、危险动作策略和隔离方式；
- Token、金额、执行时间、重试和调用深度预算；
- 单次 attempt 的最坏费用估算及计价版本；
- 失败时的升级目标；
- 优先级及调度标签。

目标、基线输入版本、基线依赖、验收条件、权限需求和预算上限在节点第一次进入 `Ready` 后不可修改。恢复时可以追加 completion links 和子图，并为新 attempt 生成包含子产物的 input manifest，但不能改变上述契约。

V1 不允许在仍要继续交付同一 Run 时，以“新节点替换并取消旧必需节点”的方式改变目标或验收条件。执行方法变化必须通过同一逻辑父节点下的修复子图表达；若用户目标或必需验收条件实质变化，当前 Run 进入 `AwaitingUser`，经用户确认后结束或创建关联的新 Run。只要 Run 继续交付，所有必需业务节点最终都必须 `Succeeded`。

### 9.2 Agent 实例与子 Agent

节点不等于 Agent。确定性校验、文件合并和纯控制节点可以不创建 Agent；需要模型推理的 attempt 在调度时创建一个 `AgentInstance`。

Agent 不能直接生成隐藏的后台 Agent。动态委派必须先提交新增 DAG 节点的提案；Graph Manager 接受节点后，Scheduler 只有在该节点真正获得 attempt 时才创建 Agent 实例。

每个实例记录：

- `agent_instance_id`、`run_id`、`node_id` 和 `attempt_id`；
- `parent_agent_instance_id` 与 `created_by_attempt_id`；
- 根实例深度为 0，子实例深度为父实例深度加 1；
- 角色、厂商、模型、reasoning effort 和策略版本；
- `Created`、`Active`、`OutcomeUnknown`、`Completed`、`Failed` 或 `Cancelled` 状态；
- Token、费用、工具调用和产物归属。

计数口径如下：

- Run 的 Agent 总数是该 Run 已接受的全部 `AgentInstanceCreated` 事件数，已结束实例也不从累计值中扣除；
- 并发 Agent 数是 `Active` 与 `OutcomeUnknown` 实例数之和；
- 深度在接受扩展提案时检查，不能通过换模型、重试或重新命名节点重置；
- attempt 终结时必须终结其 Agent 实例；结果不明时实例保持 `OutcomeUnknown` 并继续占用并发额度；
- Run 进入终态后不再创建实例，所有未终结实例进入取消或核对流程。

至少记录 `AgentSpawnRequested`、`AgentInstanceCreated`、`AgentStarted`、`AgentCompleted`、`AgentFailed`、`AgentOutcomeUnknown` 和 `AgentCancelled` 事件，以便审计总量、深度和并发限制。

## 10. 执行结果与验证

每个 attempt 的结果必须包含：

- 执行状态和结构化摘要；
- 产物引用、内容哈希和生成 attempt；
- 实际使用的模型、reasoning effort、工具和权限；
- Token、费用和耗时；
- 验证证据或失败信息；
- 建议的失败类别和后续动作。

Worker 只能标记候选完成。Verifier 必须针对每条验收条件生成通过、失败或不可判定的记录：

- 全部必需验收条件通过，节点才可 `Succeeded`；
- 任一必需条件失败时，该 attempt 进入 `Rejected`，节点由 Recovery Controller 转为 `RetryPending`、`WaitingChildren`、`AwaitingApproval`、`Blocked` 或在恢复耗尽后转为 `Failed`；
- 条件不可判定时，创建补充验证节点，或在无法获得证据时升级处理；
- 可选节点可以 `Skipped`，必需节点不能通过跳过满足依赖；
- 父节点需要对子节点聚合结果再次验证。

Verifier 默认与产生候选结果的 Worker 分离。低风险、纯机械且可完全机器检查的节点可以使用确定性验证器，不必额外调用模型。

当外部副作用从 `OutcomeUnknown` 核对为“已经发生”时，原执行 attempt 以 `Reconciled` 终结，不能恢复其过期 generation。控制层把节点送回 `Ready`，并强制下一次调度使用 `attempt_kind=reconciliation_validation`：该 attempt 获得新的租约和 generation，只能读取持久化回执、查询结果及现有 Artifact，不能再次调用原副作用工具。它随后通过标准的 `Running -> Verifying -> Succeeded/恢复` 路径提交结果。若核对证明副作用未发生，节点进入 `RetryPending`，后续才可创建普通执行 attempt。

## 11. 失败分类

失败在采取恢复动作前必须归为以下一种主类别，并保存可比较的失败指纹：

- `transient`：网络超时、限流、服务临时不可用；
- `output_invalid`：响应不符合结构、协议或格式要求；
- `task_failure`：代码、分析、测试或产物不满足验收条件；
- `capability_failure`：当前模型经过有限修复仍无法胜任；
- `plan_failure`：任务拆分、依赖或输入假设错误；
- `policy_blocked`：权限、预算、密钥、工具或环境硬条件不满足；
- `fatal`：控制层一致性损坏、非法循环、事件存储不可安全使用等系统级错误。

失败指纹至少由类别、节点类型、规范化错误码、验收条件和关键上下文哈希组成。它用于识别没有进展的重复尝试。

## 12. 恢复阶梯

默认恢复顺序为：

```text
同模型瞬时重试
  -> 定向修复节点
  -> 更换模型或交给独立 Reviewer
  -> 重新拆分未完成子图
  -> Director 裁决
  -> 请求用户介入或结束 Run
```

具体规则如下：

1. `transient` 可以在同一节点下创建新的 attempt，使用带抖动的指数退避；旧 attempt 必须已经确定结束或仍被完整计入预算和并发额度，次数受节点和 Run 双重上限约束。
2. `output_invalid` 允许一次携带明确验证错误的定向修复；再次出现同一指纹时不得继续格式修复循环。
3. `task_failure` 为当前逻辑节点追加修复子节点，原失败 attempt 和证据保持不变；父节点进入 `WaitingChildren`，修复完成后必须运行新的聚合 attempt。
4. `capability_failure` 可以升级模型或创建独立 Reviewer 诊断节点；升级前重新执行预算预留。
5. `plan_failure` 由 Planner 为尚未完成的逻辑节点追加替代子图；原节点在子图完成后重新聚合，不能改写已执行历史。
6. `policy_blocked` 不进入自动恢复阶梯；相关分支进入 `AwaitingApproval` 或 `Blocked`，并向用户给出所需操作。
7. `fatal` 立即停止新的调度；若事件流仍可写，则将 Run 置为 `Fatal` 并保存诊断证据，否则按第 13 节进入隔离处置。

Director 只能作出以下决定：继续指定的恢复层级、接受明确标注的降级交付、停止为 `Failed`、或请求用户决定。Director 不能增加硬预算、扩大权限、解除熔断或修改审计记录。

### 12.1 熔断与收敛

出现以下任一条件时，当前分支必须熔断：

- 相同失败指纹达到策略上限；
- 节点、分支或 Run 的重试次数耗尽；
- 继续尝试无法满足最低预算预留；
- 恢复层级已到 Director 且没有新的证据或可行策略；
- 新拆分与已有失败分支实质等价。

熔断后只能转向已存在的替代分支、请求用户介入，或明确结束任务。不能通过创建语义相同但 ID 不同的节点规避计数。

## 13. 检查点与崩溃恢复

事件在被调度器观察之前必须持久化。事件包含单调递增的 Run 内序号、事件类型、主体 ID、因果事件、策略版本和时间戳。

系统至少在以下位置形成可恢复检查点：

- 初始输入和约束已保存；
- 初始图或图扩展已接受；
- 节点执行租约已授予、续租或过期；
- 预算已预留、结算或释放；
- 产物已写入并取得内容哈希；
- 验证结果已接受；
- Run 状态发生变化。

恢复流程为：读取最近的已验证快照，按事件序号重放后续事件，重建 Run、图、预算账本、Agent Registry 和租约状态，然后仅重新调度满足就绪条件且没有有效租约或未决副作用的节点。

快照是事件流的派生缓存，不是事实来源。快照哈希或序号不匹配时，系统丢弃该快照，尝试更早的已验证快照，最后可以从完整事件流重放；只要事件流完整可验证，损坏快照不会令 Run 进入 `Fatal`。

事件链本身缺失、顺序冲突或校验失败时，系统停止该 Run 的全部调度。若事件存储仍能安全追加，则记录 `EventChainCorrupt` 并进入 `Fatal`；若事件存储不可写，则把 Run 标记为控制平面 `Quarantined`，通过独立的本地诊断日志和进程健康状态报告故障。此时事件流中的 Run 保持最后一个已持久化状态，不能谎称 `Fatal` 已写入。存储恢复后必须先完成一致性核对，再追加诊断事件或由用户创建恢复 Run。

## 14. Final Review 与交付

所有必需业务节点成功只代表可以开始 Final Review，不代表 Run 已完成。Coordinator 为每轮审查递增 `review_generation`，原子记录不可变的 `review_graph_version` 和 `review_input_manifest`，并创建一个新的独立 Final Review 控制节点。该控制节点不属于被审查的业务 DAG，因此它的创建不会改变 `review_graph_version`。manifest 固定原始目标、该图版本的必需节点结果、产物哈希、验证证据和账本快照。该节点必须检查：

1. 原始用户目标和明确约束是否全部覆盖；
2. 必需产物是否存在且哈希匹配；
3. 验证证据是否充分、最新且来自正确输入版本；
4. 是否仍有未解决失败、冲突、权限风险或降级项；
5. 总 Token、费用、工具调用和 Agent 数量是否与账本一致；
6. 用户是否需要执行后续动作。

审查期间普通图扩展被冻结。Final Review 的结论只能是：

- `pass`：Review attempt 和节点均成功，Run 进入 `Delivered`；
- `repair`：当前 Review attempt 以 `Rejected` 结束，Review 节点进入终态 `Superseded`；系统原子追加有界修复子图并把 Run 退回 `Running`。修复完成且图再次静止后，创建绑定新图版本和新 manifest 的下一代 Final Review 节点；
- `degraded`：Review 节点进入 `AwaitingApproval`，Run 进入 `AwaitingUser`；仅在策略允许且用户明确授权后，节点才回到 `Ready`，通过包含授权记录的新验证 attempt 后完成带缺失项的交付；
- `blocked`：Review 节点进入 `AwaitingApproval` 或 `Blocked`，Run 进入 `AwaitingUser`；
- `fail`：Review 节点和 Run 在恢复无合理路径后进入 `Failed`。

历史 `Superseded` Review 节点不属于必需业务节点，也不满足交付条件；只有最新 `review_generation` 的 Review 节点成功才能交付。交付记录必须能从用户目标追溯到节点结果、产物、验证证据和策略决策。

## 15. 测试策略

### 15.1 单元与属性测试

- 覆盖第 4、5 节全部合法 Run、Node 和 Attempt 状态转换，并拒绝越级、非法回退和终态改写；
- 随机生成 DAG，验证扩展后始终无环；
- 验证节点只在必需依赖成功和所有策略门通过后进入 `Ready`；
- 验证必需节点不能被 `Skipped`；
- 验证继续交付的 Run 不能取消必需节点或用新 ID 绕过其验收条件；
- 验证 fencing generation 和结果 CAS 只允许一个胜者，迟到 attempt 不能进入验证或覆盖结果；
- 验证修复子图成功后父节点回到 `Ready` 运行聚合 attempt，而不是从 `Failed` 复活或直接成功；
- 验证每一代 Final Review 固定图版本和输入 manifest，返修后必须创建新一代 Review 节点；
- 验证 Final Review 期间普通扩展无法与审查并发提交；
- 验证所有恢复路径受次数、预算、深度和权限限制。

### 15.2 故障注入与恢复测试

- 在任意事件写入后终止进程，并验证重放后的状态等同于不中断执行；
- 模拟超时、限流、无效输出、Worker 失联、租约过期和事件重复投递；
- 验证已接受节点不会被重新提交；只有在旧 attempt 已终结或继续计入额度时，未完成 attempt 才能安全替换；
- 验证预算预留、结算和释放在重放后守恒，迟到费用仍从原预留结算；
- 模拟非幂等外部操作在“副作用发生、回执未写入”窗口崩溃，验证节点进入 `AwaitingReconciliation` 且不会自动重试；
- 验证确认副作用已发生后只创建新的 reconciliation-validation attempt，且该 attempt 无权再次调用原副作用工具；
- 模拟损坏快照但事件流完整，验证系统丢弃快照并成功重放；
- 模拟事件链损坏，验证系统进入 `Fatal` 或在存储不可写时进入控制平面 `Quarantined`，而不是带病运行。

### 15.3 并发与隔离测试

- 压测全局、Run、厂商和工具四级额度；
- 验证同一节点不会同时持有两个有效租约；
- 验证 outcome-unknown attempt 继续占用额度，替代 attempt 不会令物理在途数突破硬并发上限；
- 验证并发或迟到 attempt 只能有一个结果被接受；
- 验证 attempt 的最坏费用预留与租约授予原子提交，任意时刻已结算费用和未释放预留之和不超过硬预算；
- 验证同一工作区默认串行写入；
- 验证隔离工作树可以并行，合并冲突会生成失败证据而非静默覆盖。

### 15.4 Agent 限制测试

- 验证每个模型驱动 attempt 都有唯一 Agent 实例，确定性节点不虚增实例；
- 验证累计 Agent 总数不会因实例结束、重试或重命名节点而减少；
- 验证子 Agent 深度由父实例单调增加，不能通过重新拆分重置；
- 验证 `Active` 和 `OutcomeUnknown` 实例均计入并发；
- 验证达到总数、深度或并发上限时，扩展或调度被确定性拒绝并留下事件。

### 15.5 端到端场景

至少覆盖以下路径：

1. 单分支一次成功并交付；
2. 多分支并行后汇总成功；
3. 执行期间追加并细化节点；
4. 瞬时失败后重试成功；
5. 定向修复成功；
6. 模型升级或 Reviewer 介入；
7. 重新拆分未完成子图；
8. Director 决定停止或降级；
9. 权限或预算阻塞后等待用户；
10. 重复失败触发熔断；
11. 崩溃重启并从检查点恢复；
12. Final Review 发现遗漏并退回有界修复；
13. 非幂等副作用结果不明并等待核对；
14. 暂停或等待用户前先关闸并静止在途执行；
15. 动态创建子 Agent 时触发累计数量或深度限制。

模型和工具响应使用录制夹具进行确定性回放；在线集成测试单独运行，不作为核心状态机测试的稳定性依赖。

## 16. 验收标准

实现只有同时满足以下条件，生命周期模块才可视为完成：

1. 每次有效状态转换均有持久化事件、原因和因果关系。
2. 图扩展只能追加合法节点和依赖，无法修改执行历史或形成环。
3. 崩溃恢复不丢失已接受结果，也不接受重复或迟到提交覆盖结果。
4. 任意模型和 Agent 都无法突破硬权限、预算、并发和深度上限。
5. 重复失败能够熔断，任务不存在无限重试或无限创建子 Agent 的路径。
6. 必需节点均经过独立或确定性验证，Final Review 对齐原始用户目标。
7. 最终交付能够追溯到输入、图版本、执行 attempt、产物和验证证据。
8. 第 15 节定义的状态、属性、故障注入、并发及端到端测试全部通过。

## 17. 后续设计接口

后续预设、路由、权限和持久化规格必须为本模块提供以下配置或实现：

- 各层并发、重试、深度、Token、金额和时间上限；
- 不同失败类别的模型升级和 Reviewer 路由；
- 节点与工具的权限判定结果及审批要求；
- 事件追加、快照、幂等提交和 Artifact 内容寻址接口；
- 可供 CLI 与 MCP 查询的 Run、节点、预算和审计视图。

这些接口的具体格式将在下一设计阶段确定，本规格定义的行为语义保持不变。

本规格尚不触发编码或单独的实施计划。权限、路由、存储和可观测性规格全部批准后，再编写总体实施计划。该计划至少拆为四个可独立验收的实施阶段：

1. 状态机、事件流、快照与重放内核；
2. DAG、调度、租约、预算账本与 Agent Registry；
3. Worker、Verifier、外部副作用核对与失败恢复；
4. CLI/MCP 集成、故障注入和端到端验收。
