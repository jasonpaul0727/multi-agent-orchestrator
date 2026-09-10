# Multi-Agent Orchestrator — 项目上下文

## 当前状态

项目处于设计阶段，尚未开始实现。后续工作应在本目录进行。

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

## 已确认的设计部分

整体架构和分层职责已获用户确认，不需要调整。

任务生命周期与失败恢复设计已于 2026-09-10 获用户批准。V1 采用事件驱动 DAG，并允许执行期间仅追加和细化节点；优先保证可控可靠。正式规格位于 `docs/superpowers/specs/2026-09-09-task-lifecycle-design.md`。

## 待确认的设计部分

三种内置预设和 DIY 配置格式、模型路由规则、权限与审批、持久化和可观测性等后续设计尚未开始。

## 后续设计顺序

1. 设计三种预设和 DIY 配置格式及路由规则。
2. 设计权限、安全、隔离和审批模型。
3. 设计持久化、成本统计、可观测性和测试方案。
4. 汇总并审阅完整产品设计文档。
5. 用户批准完整设计文档后，编写实施计划，再开始编码。
