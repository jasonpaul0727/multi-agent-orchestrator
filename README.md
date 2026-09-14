# Multi-Agent Orchestrator

一个本地运行的多模型、多 Agent 编排系统。项目以 GPT/Codex 为主要模型，同时支持通过 API Key 接入其他模型厂商；系统会根据任务角色、成本、风险和失败情况选择模型，并在必要时升级到更高能力的模型。

> 当前状态：设计阶段，尚无可运行版本。完整设计获批后，项目才会进入实施规划和编码阶段。

## V1 目标

- 使用 Python 构建共享编排核心。
- 提供独立的本地 CLI 和 MCP Server，不建设 Web 服务。
- 支持软件开发与文档分析两类任务。
- 支持 Root/Planner、Coder、Document Analyst、Researcher、Tester、Reviewer 和 Director 等 Agent 角色协作。
- 允许 Agent 动态创建子 Agent，同时严格限制并发数、总数量、调用深度、Token 和金额预算。
- 提供 `economic`、`balanced`、`quality` 三个内置预设，以及可完全自定义的 `custom` 预设。
- 支持 OpenAI Responses、Anthropic Messages 和通用 OpenAI-compatible 模型适配器。

## 架构原则

系统采用“受控的去中心化 Agent 网络”：Agent 可以协作、委派和细化任务，但不能绕过确定性控制层。

- **编排核心**：CLI 与 MCP Server 共用相同的任务编排能力。
- **确定性控制层**：统一管理运行状态、预算、权限、并发、检查点和审计。
- **Model Gateway**：封装不同模型厂商的调用、重试、路由和升级策略。
- **Tool Gateway**：统一执行权限检查、工作区隔离和高风险动作审批。
- **事件存储**：记录任务轨迹、模型调用、工具调用、Token、费用和证据。
- **任务模型**：V1 使用事件驱动 DAG，并允许在执行期间仅追加或细化任务节点。

## 权限与安全

项目规划提供 `read-only`、`workspace-write` 和 `full-trust` 三档权限。文件访问以声明的工作区边界为基础；删除、安装系统软件、发布、推送和密钥访问等高风险动作可分别配置策略，并由控制层强制执行与审计。

## 设计文档

- [项目上下文](PROJECT_CONTEXT.md)：已确认范围、总体架构和当前进度。
- [任务生命周期与失败恢复](docs/superpowers/specs/2026-09-09-task-lifecycle-design.md)：已于 2026-09-10 批准。
- [预设、DIY 配置与模型路由](docs/superpowers/specs/2026-09-11-presets-routing-design.md)：已于 2026-09-12 批准。
- [权限、安全、隔离与审批](docs/superpowers/specs/2026-09-13-permissions-security-isolation-approval-design.md)：各章节已确认，等待书面规格终审。

## 下一步

1. 完成权限、安全、隔离与审批书面规格的终审。
2. 设计持久化、成本统计、可观测性和测试方案。
3. 汇总并审阅完整产品设计。
4. 完整设计获批后编写实施计划，再开始编码。
