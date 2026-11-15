# B 双视图纯离线模型设计

用户要求完善 B 双视图设计，独立审阅通过后直接开发。本设计限定于独立审阅通过的纯离线模型；不批准或启用生产协议、停机证明、凭据、线上调用或 Worker v2。完整 v1 校验和 outcome_unknown 对账保持不变。

## 目的与边界

用纯函数有限状态机探索同一源 attempt 的合成 v1/B 请求、不同入口竞争、双视图、撤销、unknown 及 crash/replay：提供反例，检验候选规则是否自洽。不声称现有 Scheduler/Router 满足新规则，不能用模型改变现有 blocked、对账、准入或发送。

只新增独立测试支持模块与 unit tests；不更改生产包、旧事件或 schema。仅标准库及不可变内存值，禁止导入或调用 orchestrator 生产模块、Worker、Provider、服务、网络、凭据；禁止子进程、数据库和文件 I/O，不提供 CLI、注册或持久化入口。模型不定义或输出生产 request ID、authorization、签名、hash、权限 token 或生产兼容反序列化格式，也不提供 JSON/bytes 编解码器。Python 合成值仍可能被人为转换；不宣称技术上不可能序列化或伪装成生产输入。输入只允许固定 synthetic 类型；全部返回对象固定标记 simulation_only=True、non_authoritative=True，按设计不被运行时消费；这不是运行时访问控制，不能阻止人为编写转换器。调试输出仅固定合成枚举，不收用户文本。

YES、winner 和模拟通过仅为假设，不是实际 stop/no-charge、签发权威、原子存储、旧 writer 屏障、真实结算或送达证据。模型通过也不构成实际授权，不保证可用性或外部 exactly-once。

## 有限域与双视图

固定最多两个合成源 S0/S1；每源对应唯一固定五元组 (run,node,source_attempt,generation,source_decision) 与模型、provider、合成 evidence。完整身份和关联数据必须一致；缺失、重复冲突、未知符号或错误类型一律 SIM_HELD_INVALID 且状态不变。不得只依赖摘要或 caller 声称前驱。所有标识仅使用本模块固定枚举，不接受任意字符串或真实事件。

历史视图是 M0/M1 有序无重复 tuple，精确保留原模拟 v1 evidence 值和顺序；不自动附加当前源模型，不删除 ID。资格视图不可变绑定完整 source、source model/provider、合成 evidence、policy、retry level 和逻辑过期步。仅 SIM_B 同模型 action 可对确切源模型做一次排除集合例外；v1、fallback、escalation 始终用原历史集合，资格不能跨 source/model/provider/action。两视图不合并、不互相改写，也不代表可信签发。

独立模拟前提为：已知 failed、source 最新、输入完整、旧发送能力封闭、全部 effects no-effect AND no-charge、policy/health 可用、预算及 attempt 可用。每项初始 UNSET，随后可观测 YES/NO/UNKNOWN；只有全部明确 YES 才可模拟 B，实际观测 UNKNOWN outcome 必须 SIM_RECONCILIATION。精确转移矩阵见下方评审修正。撤销、source stale、输入失效、unknown、费用/effect 和旧发送能力恢复为单调阻断，后续 YES 不得清除；重新取证/对账在模型外 held。逻辑时刻仅整数 0..8，回退或过期 held，不是可信时钟。

版本 SIM_V1/SIM_B、入口 INITIAL/PROBE/RECOVERY/IMPORT、action SAME/FALLBACK/ESCALATION 仅合成枚举；每源最多两个 request R0/R1、generation 0/1、两个 pending。首次和后续 attempt 分开：统一候选边界从模型历史解析实际前驱，有前驱时不得仅凭 caller 声称 initial 绕开 source winner；此为拟议规则，不声称现有 v1 行为。抽象 attempt 上限 1..3、retry 上限 0..2、预算 0..3；不是 tokens、金钱或真实 reservation。事件上限 64，超限 held 且不改状态；不声称真实 RSS/磁盘限制。

## 接口与转移

initial_state() 返回不可变 ModelState；唯一入口 step(state, command) 返回新状态和有两个非权威标记的固定枚举 observation。边界只接受本模块精确 frozen 类型与 enums，拒绝 dict、json、bytes、未知字段、bool 冒充 int、越界及生产对象；嵌套不可变并验证无外部可变别名。

- RegisterSource 登记固定表中的完整身份及历史视图，精确重复幂等；同键不同属性 held。
- SetFacts 在有限域设置各独立前提及逻辑步；非法、时钟回退或试图撤销单调失效 held，不把标记解释为真实证明。
- Propose 核对 source、版本、入口、action、目标、视图、facts 和配额；仅返回 SIM_PROPOSED。相同 request 的冻结 envelope 完全相同才重放原历史观察；不同 envelope 返回 SIM_CONFLICT。当前动态事实与时钟不属于 envelope，后续失效影响 commit/dispatch，不改变历史记录。
- PrepareCommit 仅添加易失 pending、不创建 winner 或扣预算；用于模拟两个调用先后观察，不代表真实锁或事务。
- Commit 在单次假定原子模型转移内重新验证 source、pending、前提、generation、预算及 winner。无 winner 且条件满足时记录一个合成 winner、消耗一次预算并推进 attempt/fencing；另一个版本/入口后到返回 SIM_SUPERSEDED，不覆盖。赢家相同 request/相同内容重放返回原结果，不再次扣减；不同内容固定冲突。
- CloseGeneration 仅无 winner 且旧代次可明确关闭时显式关闭 generation 0；所有条件重新成立才能探索 generation 1，旧代次不得迟到提交。它只是显式探索分支，不决定生产自动重规划；unknown、winner 或条件不明 held。
- Invalidate 单调记录撤销、stale、输入失效、unknown、效果/费用或旧发送能力恢复。commit 前阻止新 winner；winner 后保留其历史与抽象预算，后续发送检查 held/reconciliation，不假装未发生。
- Crash 只清掉易失 pending，保留已提交模型事件；Replay 通过唯一 step 命令接收不超过 64 个不可变合成事件，并按下方评审修正定义的自有可信合成锚点和事件前态规则从空态重建；不一致或非法时 held 且输入 state 不变。提交后响应丢失、同 request 重放只返回原记录。这些是模型抽象，不是 SIGKILL、SQLite/WAL 或物理原子性证据。
- SimulateDispatchCheck 在模拟 winner 后重验 source、目标、事实和撤销，仅返回 SIM_HELD、SIM_RECONCILIATION 或 SIM_PRECONDITIONS_MET_NON_AUTHORITY；不发送、不发能力、不可连接运行时。

内存事件仅 SOURCE_REGISTERED、FACTS_UPDATED、PROPOSAL_RECORDED、GENERATION_CLOSED、WINNER_RECORDED、INVALIDATED；事件为固定冻结合成值。PrepareCommit/Crash 不生成模型 durable event。对 v1 模拟赢家，B 显示 superseded，反向亦然；不得称另一个授权已消费。模型中 source key、winner、预算和接受的单步提交是明确假设，非生产实现声明。

## 不变量与测试验收

1. 一个完整 source 跨所有模拟版本、入口和 action 至多一个不可修改 winner；两个 pending 任一提交顺序最多扣一次预算，另一 source 独立。
2. 历史集合原顺序/值恒定，B 例外仅绑定 source model；不得泄漏到其他 source/model/provider/action 或 v1/fallback/escalation。
3. 未知、撤销、过期、费用/effect、旧源发送恢复、身份不符或任何前提非 YES 时，不得产生新 winner 或模拟发送；已发生 winner 不能消除。
4. 同 ID 同输入及响应丢失重放幂等；不同内容冲突；预算非负且每个 winner 恰扣一次，attempt/retry/fencing 不超过边界。
5. generation 关闭后旧提交失败；无 winner 的新代次仍须重验。crash 只丢 pending，已提交 winner 与抽象扣减同在；损坏历史重放 held。
6. 全部输出标识 simulation_only/non_authoritative，既不提供 production token，也不读取真实输入或调用外部边界。

按 TDD 对每个尚未实现的行为先真实 RED 再最小 GREEN，独立审阅；不造 RED。枚举固定域两个请求的 prepare/commit 交错及两个 winner 顺序、各入口/版本、同/不同 source、model/provider/action、历史视图含/不含源模型；逐项测 UNSET 和每个已观测三值事实、期限、预算/retry/attempt 边界。将撤销、unknown、输入失效、旧发送恢复分别放在 proposal、prepare、commit、winner 与发送检查前后；测冲突、generation 关闭与迟到、crash/response 丢失、缺失/乱序/矛盾合成事件和重新 replay。断言拒绝不改变资源或伪造历史。静态审查依赖及合成边界 guard，验证无生产导入/I/O/凭证；guard 本身不是生产隔离证据。报告实际穷举范围与未覆盖情况，出现反例先修订规则，不放宽测试掩盖。

## 用户决定与主设计分界

此模型固定为 2 source、2 request、2 generation、64 事件的简化范围；需要扩展时先修改并审阅。独立审阅和开发仅覆盖该模型及必要测试。

主 B 设计仍须单独决定：真实停止与 no-effect/no-charge 签发、已结算有效果政策、历史迁移/旧客户端、fallback、健康/撤销/价格权威与时效、生产 replan/事务/schema、所有 admission 入口及 v1 行为变化、dispatch gate、实际 writer/DB/WAL 屏障、密钥和回滚。缺少真实信任根与证明时生产仍 held；本模型不决定或替代任何线上证据。
同 request 已有 winner 的重放顺序：先核对已存 winner 和完整 request 一致性，再返回固定历史观察；此分支不要求 crash 前的 pending 仍存在，也不得再次扣减。它只报告历史；如之后 facts 已失效，SimulateDispatchCheck 仍须 held/reconciliation。不同内容始终冲突。

## 评审修正：事实、请求、重放和资源的精确定义

本节优先于任何较宽或不一致的前文，均为受限纯离线模型候选，不是生产策略。

### 事实转移矩阵

每个独立事实初始为 UNSET，即尚未观测；UNSET 不是已观测 UNKNOWN。UNSET 可首次设置 YES、NO 或 UNKNOWN，命令/时钟须合法；因此首个 YES 可达。未经明确 YES 时不能新 winner/模拟发送。

已知 failed、source 最新、输入完整、旧发送入口封闭、完整 no-effect 且 no-charge 等安全事实：YES 可保持 YES 或转 NO/UNKNOWN；一旦观测 NO 或 UNKNOWN，在此 source 的模型寿命内只能保持原阻断值；尝试转 YES 或互转 held 且状态不变。相应独立撤销/费用/效果/旧入口恢复也单调失效。重新对账、取证、source 换代不在模型内。UNKNOWN 为已发生的不确定性，永不被后续 YES 清除；unknown outcome 持续 reconciliation，其他未知事实 held。

policy/health 与抽象预算/attempt 可用是独立动态事实：UNSET 可转 YES/NO/UNKNOWN；YES 可转 YES/NO/UNKNOWN；NO 可保持 NO、转 YES 或 UNKNOWN；一旦观测 UNKNOWN 只能保持 UNKNOWN。动态 NO 恢复不清除任何安全事实阻断或 unknown outcome。预算数值由模型状态控制，availability=YES 不可透支或抵消已消费量。独立 Invalidate 撤销为单调状态，不与暂时 policy/health NO 混同。其余转移 held，不更新时钟、事件或资源。测试覆盖每个起点/终点、UNSET 首次 YES、已观测 UNKNOWN 不可恢复、动态 NO 恢复；不暗示生产策略。

### 请求冻结与动态观察

immutable envelope 保存完整 source、request/version、入口/action、model/provider、历史视图、资格合成标签、plan generation、创建步/expiry、冻结的 retry/attempt 上限和固定模拟配置；字段仅有限枚举和小整数。同 source/request envelope 逐项相同才幂等，任一差异 SIM_CONFLICT 且不改变历史/资源；不同 source 的 R0/R1 不混淆。

当前 facts、当前逻辑时间、预算余额及事件位置是动态状态，不纳入 envelope 相等性。首次 Propose 校验当时事实；之后精确重放只返回保存的历史 proposal，不因动态变化误报冲突，但不授予资格。Prepare/Commit 和 SimulateDispatchCheck 分别重验当前事实、时间与余额。已有 winner 的同 envelope 重放只读返回历史赢家，不要求已丢弃的 pending，也不依据现时事实重算；之后的发送检查仍因撤销/unknown 等 held。不同 envelope 始终冲突。

### 唯一 step 下的完整 replay

Replay 是唯一 step 接受的白名单 frozen command，携带最多 64 个不可变合成事件 tuple 和测试 harness 分开保存的合成锚点；event 不允许 dict、bytes、可变成员、生产对象或未知枚举。每个提交事件有从 1 连续单调的合成 ordinal 和固定类型字段；pending 不在事件流中。Crash 丢 pending，保留模型事件和测试锚点。

锚点仅为 harness 保存的相同有限模拟历史的精确不可变事件 tuple 和计数；Replay 先要求提交的历史完整等于锚点和计数，再从空状态逐事件重建，每项用该事件发生前的状态和当时逻辑步核查前提、资源及合法顺序。后续撤销不能倒推先前已合法 winner 不存在。在线 Commit 仍必须验证当前易失 pending；但 replay 从事件流还原 WINNER_RECORDED 时只验证可从 durable 模型事件重建的前置条件：同一 source 的已记录合法 proposal 及其 envelope、该事件发生时 facts/逻辑时钟、未关闭代次、预算与 attempt/retry/fencing、没有既有 winner，以及匹配的来源/目标。Replay 不要求也不声称证明此前存在 PrepareCommit 或 pending：它们是易失态且不进入事件。这是对模拟 durable 历史的验算，不等同调用在线 Commit。此 oracle 明确依赖 harness 自有历史，不能证明独立生产完整性或抗恶意篡改，也不模拟持久化或可靠存储。

原始合法序列重建相同已提交状态、pending 为空；空历史仅锚点也为空时接受。缺尾/缺中、重复 ordinal、乱序、内容与锚点不符、未知类型/超限、非法转移或第二 winner 均 SIM_HELD_INVALID，调用前输入 state 完全不变，不可按合法前缀部分更新。人为同时篡改事件和锚点超出防篡改证明范围；没有可信锚点的恢复 held。除原锚点比对测试外，测试 harness 可有意构造坏序、无 proposal、提前 close/撤销及双 winner 的 frozen 合成 event tuple 并提供与之相同的合成锚点，以穿过相等性检查后验证事件前态规则会 held、输入 state 不变；这不证明能防御生产事件与锚点被同时篡改。另对删尾、删中、重复、置换、字段不符和后续撤销检查锚点及事件前态。必须覆盖合法 Propose→PrepareCommit→Commit→Crash→Replay：重放时 pending 为空但合法 winner、一次预算消耗保留；追加有效 Invalidate 后重放仍保留历史 winner，发送检查 held/reconciliation。两者均只是模型结果，不代表真实 SQLite。

### source 资源作用域与实例

S0/S1 明确来自两个不同合成 run 和 node，各有专属独立抽象预算、attempt 和 retry。跨 source 独立性仅在此限定域成立；同 run 共享预算、共享 node、跨源资源竞争、多源链和真实全局资源不在模型内，不能推广。

R0/R1、plan_generation 0/1、两个 pending 均按 source 命名；S0 与 S1 的 R0 是不同完整请求。source_fencing_generation 是五元组固有字段，plan_generation 0/1 是该 source 的规划代次，二者不同。默认源 fencing=2、已完成 attempts=2、max_attempts=3、evidence retry_level=1、profile retries=2、抽象预算初始=1；winner 后 attempt fencing=3、attempts=3、request retry_level=2、预算=0，重放不改变。generation 关闭/重新规划仅变更 plan_generation，不改变 source fencing 或预先消耗 attempt/预算；每 source 最多一个 winner。

固定合成 fixture 枚举：NORMAL 如上；ATTEMPTS_FULL 为 attempts=2/max=2；RETRY_FULL 为 evidence retry=2/retries=2；BUDGET_EMPTY 为预算=0。各 fixture 固定小整数，caller 不可注入任意生产数据；source 登记后不可切换 fixture。各失败 fixture 不得产生 winner。不能声称穷举任意 64 步组合；报告实际穷举的短序列及交错。

### 模型隔离真实限度

tests-only、禁止生产导入/adapter/生产 ID/hash、依赖方向检查及 I/O guards 是开发和可检验约束，不是 OS 安全边界或生产权限隔离。Python 值可被人为翻译，不能声称本模型使任意人无法序列化、伪造或接入 admission。任何生产 adapter/绑定/真实权限必须另行设计、审阅并批准；此设计不提供或授权它。恶意手写转换或绕过 tests-only 不在本模型防护证明内。

## 2026-11-15 审阅修正：模型中的资源占用

用户已要求完善双视图设计，经独立审阅后直接开发；本段与该要求取代此前本文件中等待此离线模型另行批准的状态。生产部署与线上启用不由此授权，完整 v1 校验及 outcome_unknown 对账持续生效。

每个模拟 winner 有一个不可变的 synthetic reservation，完整绑定 source、获胜 request/envelope、目标 model/provider 和一次模型资源扣减。winner、reservation 和扣减只在同一假定原子 Commit 产生；模型不模拟真实资金或持久化原子性。保留 reservation 身份及其单调撤销状态于合成事件，并在 Replay 中重建；缺失、重复、错源或错赢家均 held。Invalidate 可撤销 reservation，但不得删除 winner、返还或再次扣减历史资源。

SimulateDispatchCheck 核验该 winner 对应 reservation 的完整绑定、存在及未撤销，同时重新验证其他事实与有效期；它不再要求余额至少为 1，也不再次扣减。模拟余额因 winner 从 1 降至 0 时仍可通过有效 reservation 检查。对其他 source/winner 的 reservation、撤销、未登记或不一致必须 held；重复检查不改变余额。包括 crash/replay 后的成功及上述负例均需测试；该 reservation 只是模型占位，不构成真实预算、结算、发送许可或生产可信证明。

## 2026-11-15 审阅修正：封口与效果观察的先后

对每个 source，记录旧发送入口封闭 YES 与完整 no-effect AND no-charge YES 两次合成观察各自提交事件的 ordinal；资格要求前者严格早于后者，不能仅比较最终布尔值或相同逻辑时钟。两者由独立 SetFacts 命令记录；同一命令试图同时将两者设为 YES，一律 held 且状态不变。若 no-effect YES 先于封口 YES，即使后续封口，也不能凭此前观察产生 winner 或模拟发送；本模型不包含重新取证，保持 held。封口后旧发送能力恢复仍永久阻断。Replay 以事件发生时的顺序验证相同约束，不能倒推或更改已发生 winner。

测试覆盖效果先于封口 held、封口后效果可以进入其余前提判断、同命令同时 YES held、重放顺序损坏 held，以及封口后旧发送恢复使新 winner 和模拟发送 blocked。这仅验证合成排序，并不证明真实封口或 Provider 结算。

## 2026-11-15 审阅修正：v1 对照与竞争

此模型中的统一前驱规则是候选设计，并非现有所有 v1 入口的行为。明确模拟 v1 same-model 的目标出现在历史排除视图时必须 held，保留原视图；B 仅在全部模拟前提成立时对严格绑定的源模型适用例外。竞争验收须分别覆盖：源模型已在历史视图时，v1 fallback 与 B same-model 两种提交次序；源模型不在历史视图时，v1 和 B same-model 两种次序。测试须验证同源至多一个 winner，而不声称模型实现了源码的全部 v1 行为。源码完整 v1 校验及 outcome_unknown 对账不能由模型断言替代。

重复设置安全事实 YES 不得刷新该事实首次 YES 的 ordinal。效果 YES → 封口 YES → 效果 YES 仍 held；Replay 须以首次观察验证同一结果。此规则和对应测试是独立评审 PASS 的必要条件。

2026-11-15 B 审阅范围澄清：固定 source 均有两次已完成 attempt，本模型只探索有前驱的后续 attempt；INITIAL 仅用于测试有前驱时不能伪称首次，不构造无前驱首次请求，也不验证生产初始 admission 或完整入口语义。FALLBACK 与 ESCALATION 均不得选 source model；本模型不表示完整 tier/capability 规则。

## 2026-11-15 C 审阅：测试模型的输入别名与锚点边界

所有合成 dataclass 使用 frozen slots，并对外来白名单命令及嵌套 envelope、eligibility、tuple 防御性复制后记录；测试 caller 后续修改原对象不能改变事件、proposal、winner 或独立复制的锚点。未知字段和不符合精确类型者拒绝。测试 harness 独立复制合成历史作为锚点，假设它始终正确且最新；同时回滚历史与锚点或缺少可信锚点不能由模型检出。对已返回 ModelState/anchor 对象使用 object.__setattr__、恶意 Python 反射或同进程对抗不属于模型安全边界；这些措施不是进程隔离或生产防篡改证明。生产源码不依赖测试模型须通过静态方向检查，动态计算/反射或外部加载超出静态证明。

## 2026-11-15 整体复审：易失状态来源假设

公共 step 对持久事件及其投影进行一致性验算，但无法仅由 durable events 证明 pending 曾由 PrepareCommit 产生，因为 pending 有意不写入事件。测试 harness 必须可信地传递先前 step 返回的易失 pending；任何人可使用 dataclasses.replace 构造与已记录 proposal 静态一致的 pending，令模型后续模拟 commit，模型不提供该 pending 的真实性、防伪或安全边界。必须有表征测试展示这一限制，不能称为对任意 caller state 的完整来源认证或生产可用性证明。生产真实 pending/锁/身份和原子性仍需独立设计及验证。状态与持久事件不一致仍须拒绝；一旦对已知事件写入 UNKNOWN，仅篡改持久投影不能清除之。
