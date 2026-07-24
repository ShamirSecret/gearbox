# Gearbox Gear — 吸收自 oh-my-openagent 的功能参考

> 基于对 omo（oh-my-openagent v4.16.0）源码的深度逆向分析。
> 目标：将 omo 的核心编排、质量门禁、token 优化机制提取为 Gear 可直接复用的设计。
>
> 2026-07-24 路由政策覆盖：omo 的多模型 fallback 仅保留为历史参考，不能作为 Gear 实现目标。Gear 的 canonical ACP 路由固定为 Codex ACP `openai/gpt-5.6-luna` 只读规划/批评/审查，以及 OpenCode ACP `opencode-go/deepseek-v4-flash` 执行/修复；禁止 fallback、命令覆盖、模型切换与 native substitute。

## ACP-only 运行契约

- 生产 worker 只能经 ACP；不得用 `opencode run`、`codex exec`、`claude -p` 或 native backend 代替成功路径。
- Provider 持有 context 和响应生命周期。Gear 不向 ACP 请求继承 goal lease 或 stale watchdog；瞬态流错误只在同一 provider/model/command 上重试，不能升级或替换模型。
- Luna 的只读 ACP session 使用隔离配置，不能继承全局 MCP、plugin 或 LSP；OpenCode OMO 内部保持 background concurrency 为 1、team 和 LSP 关闭。
- `.gear/**` 的 receipt 由 Gear 写入。worker 只报告变更、命令与结果，不能被要求写 marker；ACP session 结束后只清理已记录的精确 process-group/descendants，并保存 cleanup receipt。

### ACP 资源预算归属

- OpenCode/Codex ACP worker/provider 自己控制调用次数、context/token、费用、usage-unknown 和 premium 尝试策略。
- Gear 的 `Budget`、`ObjectivePolicy` 和 reservation ledger 只保存 lineage、实际 usage 与审计 receipt；不得用本地计数阻断 worker dispatch 或 objective continuation。
- Gear 仍可执行编排安全与验收门禁（迭代/文件范围/运行租约、重试、review、scope、verification），这些不是 provider 资源预算，也不能替代 provider 的预算策略。

## 当前实现状态（2026-07-24）

以下状态以当前 Gear 源码为准；本文件后文的“Gear 实现要点”是设计参考，不再代表未实现清单。

| 能力 | 当前状态 | 代码证据 |
|---|---|---|
| Model fallback / category route | canonical 路由已收敛，fallback 禁止 | `phase_routing.rs`、`cli.rs`、`workers.rs` |
| TaskManager steering / terminal revive | 已完成主路径 | `task_manager.rs` 的 typed outcome、resident epoch revive |
| Completion parent wake | 已完成基础缓冲与串行 flush | `task_manager.rs`、`runtime.rs` |
| Review Gate | 部分完成 | 四维 hard gate；comment checker 为环境开关 |
| Category tool policy / model variant | 仅保留工具策略 | category 不再选择 provider/model/variant；固定 canonical route |
| Stop Continuation Guard | 部分完成 | continuation state、lifecycle event、GUI stop、GoalLoop guard |
| Keyword mode / task reminder | 未完成 | 另立下一批计划 |
| `init-deep` / handoff / plan validator | 未完成 | 另立下一批计划 |
| 完整事件驱动 Ralph Loop | 未完成 | 当前仍保留 GoalLoop 主循环 |

---

## 一、核心架构：omo 是如何工作的

### 1.1 分层架构

```
┌──────────────────────────────────────────────┐
│                  OpenCode TUI                  │
│  (事件循环: session.idle / session.error /     │
│   chat.message / tool.execute.before/after)    │
└──────────────┬───────────────────────────────┘
               │ 54+ lifecycle hooks
┌──────────────▼───────────────────────────────┐
│            omo Plugin (hooks/)                 │
│  ┌─────────┐ ┌──────────┐ ┌───────────────┐  │
│  │Ralph    │ │Keyword   │ │Comment        │  │
│  │Loop     │ │Detector  │ │Checker        │  │
│  ├─────────┤ ├──────────┤ ├───────────────┤  │
│  │Model    │ │Task      │ │Stop           │  │
│  │Fallback │ │Reminder  │ │Continuation   │  │
│  └─────────┘ └──────────┘ └───────────────┘  │
└──────────────┬───────────────────────────────┘
               │ 注册 agent + 注入 prompt
┌──────────────▼───────────────────────────────┐
│           OpenCode Agent Runtime               │
│  (Sisyphus / Hephaestus / Prometheus / Atlas)  │
└──────────────────────────────────────────────┘
```

### 1.2 关键设计原则

| 原则 | 说明 |
|---|---|
| **事件驱动** | 所有循环和门禁都基于 opencode 的 lifecycle events，非轮询 |
| **Hook 即能力** | 每个功能模块就是一个 hook，可独立开关和配置 |
| **Prompt 即方法论** | 代理的行为差异不靠代码，靠 prompt 模板的差异 |
| **状态持久化** | Ralph Loop 的任务状态、fallback 链都持久化到磁盘 |

---

## 二、Ralph Loop（无限循环机制）— P2

### 2.1 架构

omo 的 Ralph Loop 不是"在 agent 里写一个 for 循环"，而是**寄生在 TUI 的事件循环上**：

```
用户输入 → Agent 工作 → 会话闲置(session.idle)
                              │
                    ┌─────────▼─────────┐
                    │  Ralph Loop Hook   │
                    │  (event-handler-   │
                    │   idle.ts)         │
                    │                    │
                    │ 1. 检查循环状态    │
                    │ 2. 检测完成条件    │
                    │ 3. 检测停滞        │
                    │ 4. 检测最大迭代    │
                    │ 5. 否则继续        │
                    └─────────┬─────────┘
                              │ injectContinuationPrompt()
                              ▼
                    Agent 继续工作 → 再次闲置...
```

**核心文件**：`hooks/ralph-loop/`
- `ralph-loop-hook.ts` — 循环入口，start/stop/resume
- `event-handler-impl.ts` — 事件分发
- `event-handler-idle.ts` — 闲置事件处理（核心）
- `event-handler-continuation.ts` — 续跑注入
- `continuation-prompt-injector.ts` — 向 session 注入续跑 prompt
- `completion-promise-detector.ts` — 检测 `<promise>DONE</promise>` 完成标记
- `loop-state-controller.ts` — 状态管理（持久化到磁盘）
- `storage.ts` — 状态存储

### 2.2 续跑流程（continuation-prompt-injector.ts）

1. 从上一个消息获取 agent/model/tools 信息（inheritFromSessionID）
2. 构建续跑 prompt：`{iteration N/N, 当前状态, 下一步目标}`
3. 通过 `ctx.client.session.sendMessage()` 注入为新消息
4. Agent 自动响应 → 工作 → 闲置 → 下一轮

### 2.3 完成检测（completion-promise-detector.ts）

检测 transcript 中是否包含 `<promise>DONE</promise>` 标记：
- Agent 发送包含 `DONE` 的消息 → 循环终止
- 支持多种格式：`<promise>DONE</promise>`、`DONE`（独立行）

### 2.4 停滞检测（no-progress-turn-detector.ts）

| 检测项 | 阈值 | 动作 |
|---|---|---|
| 连续无文件变更 | 2 轮 | 触发 "deep" 路线提示的 replan |
| 相同验证失败 | 2 轮 | 标记停滞 |
| 重复修复请求 | 2 轮 | 标记停滞 |
| 重复 worker 输出 | 2 轮 | 标记停滞 |

### 2.5 状态持久化（storage.ts）

```
.omo/ralph-loop/<session_id>.json
{
  "session_id": "ses_xxx",
  "active": true,
  "iteration": 5,
  "max_iterations": 100,
  "started_at": "...",
  "original_prompt": "...",
  "completion_promise": "...",
  "verification_pending": false,
  "strategy": "reset" | "continue"
}
```

### 2.6 Gear 实现要点

```rust
// Gear 目前是同步 for 循环，要改成事件驱动需要：
// 1. 注册 session.idle 事件监听
// 2. 在闲置时检查 state.goal 是否还需要继续
// 3. 调用 worker.send_follow_up() 传入续跑 prompt
// 4. 检测 worker 输出的 DONE 标记
// 5. 持久化循环状态到 Gear runtime-owned `.gear/`

// 关键：不在 Orchestrator::run() 里做循环，
// 而是在 GUI/Server 层监听 session 事件做循环
```

---

## 三、Keyword Detector（关键词检测）— P0

### 3.1 架构

```
chat.message hook
      │
      ▼
extractPromptText(output.parts)
      │
      ▼
detectKeywordsWithType(text, agent, model, disabledKeywords, enabledExpansions)
      │
      ▼
匹配到关键词 → 注入对应 system prompt → agent 按新模式工作
```

**核心文件**：`hooks/keyword-detector/`
- `hook.ts` — 入口，关键词检测+注入
- `detector.ts` — 关键词匹配算法

### 3.2 关键词列表

| 关键词 | 触发模式 | 注入内容 |
|---|---|---|
| `ultrawork` / `ulw` | ultrawork | 激活所有 agent，最大火力执行 |
| `hyperplan` / `n` | hyperplan | 启动 5 个敌对评审 |
| 组合 `n ultrawork` | hyperplan-ultrawork | 先评审后全力执行 |
| `deep` / `deep research` | deep research | 最大饱和度研究 |

### 3.3 注入机制

```typescript
// 在 chat.message hook 中，将检测到的关键词消息注入到用户消息之前
output.parts[textPartIndex].text = `${allMessages}\n\n---\n\n${originalText}`
```

### 3.4 Gear 实现要点

```rust
// 不需要事件 hook，可以直接在 Orchestrator::run() 的 prompt 解析阶段做：
// 1. 解析用户输入的 goal prompt
// 2. 匹配关键词（简单 regex 或字符串匹配）
// 3. 如果是 "ulw" → 设置 budget.max_iterations = MAX（如 100）
// 4. 如果是 "n" / "hyperplan" → 启动 review gate 前置
```

---

## 四、Comment Checker（注释检查）— P0

### 4.1 架构

```
tool.execute.before → 记录文件路径和内容（pending call）
       │
       ▼
tool.execute.after  → 读取实际写入的文件
       │
       ▼
调用 comment-checker CLI → 扫描新增的 .ts/.rs/.py 等文件
       │
       ▼
检测到 AI 废话注释 → 发出警告 / 自动移除
```

**核心文件**：`hooks/comment-checker/`
- `hook.ts` — 入口，before/after 钩子
- `cli-runner.ts` — CLI 调用
- `pending-calls.ts` — 待处理请求管理

### 4.2 检测规则（comment-checker CLI 内置）

| 规则 | 示例 |
|---|---|
| 显而易见的注释 | `// This function does X` |
| 过度错误处理 | `try { ... } catch (e) { console.error(e) }` 无后续处理 |
| AI 风格表述 | `// Let's ...` / `// First, ...` / `// Note that ...` |
| 冗余参数文档 | 类型已明确时的多余 `@param` 注释 |
| 样板文件注释 | `// Created by ...` / `// Author: ...` |

### 4.3 Gear 实现要点

```rust
// Gear 作为编排层不直接写文件（worker 写），
// 所以 comment-checker 适合作为 Review Gate 的一个 check：
// 1. Worker 完成 → 读取产出文件
// 2. 运行 comment-checker（可嵌入 Rust 或用 CLI）
// 3. 检测到违规 → Review 不通过 → 通知 worker 修复
```

---

## 五、Model Fallback（模型降级）— 历史设计，当前禁止实现

> 本节描述 omo 的历史机制，不能转化为 Gear 的 `WorkerConfig`、`TaskManager` 或 provider-recovery 实现。Gear 遇到 provider error、429、配置回执漂移或模型不可用时必须保存证据并以原路由 fail closed/retry，不能改用其他模型。

### 5.1 架构

```
配置 fallback chain:
  agents.sisyphus.model = "opencode-go/deepseek-v4-flash"
  agents.sisyphus.fallback_models = [
    "opencode-go/qwen3.7-max",
    "opencode-go/glm-5.2"
  ]

session.error → 检测模型错误 → setPendingModelFallback()
       │
       ▼
chat.message hook → getNextFallback() → 替换模型
```

**核心文件**：`hooks/model-fallback/`
- `hook.ts` — 入口
- `fallback-state-controller.ts` — fallback 状态管理
- `chat-message-fallback-handler.ts` — 消息中注入 fallback

### 5.2 Fallback 链配置

```jsonc
{
  "agents": {
    "sisyphus": {
      "model": "opencode-go/deepseek-v4-flash",
      "fallback_models": [
        "opencode-go/deepseek-v4-pro",
        "opencode-go/qwen3.7-max",
        { "model": "opencode-go/glm-5.2", "variant": "medium" }
      ]
    }
  }
}
```

### 5.3 Gear 的处置

Gear 不实现 `fallback_models`、模型升级或同任务模型切换。`ModelUnavailable` 结算为可见 blocker；`Streaming response failed` 等瞬态 ACP 失败仅允许保持同一 `opencode-go/deepseek-v4-flash` route 重试，并持久化原始错误与 attempt。

---

## 六、Category → Model 映射（类别模型路由）— 历史设计，按 canonical ACP 收敛

> 当前 Gear 不按类别自由选择模型：Luna 仅用于只读规划/批评/审查，Flash 仅用于执行/修复。下文的多模型 category 映射与 fallback 字段仅作 omo 对照，不得作为配置入口。

### 6.1 omo 的实现

omo 的 category 系统将任务类型映射到模型+prompt：

```typescript
// 内置 categories（从 schema 和 features.md 提取）
const CATEGORIES = {
  "visual-engineering": { model: "google/gemini-3.1-pro", variant: "high" },
  "ultrabrain":         { model: "openai/gpt-5.5", variant: "xhigh" },
  "deep":               { model: "openai/gpt-5.5", variant: "medium" },
  "artistry":           { model: "google/gemini-3.1-pro", variant: "high" },
  "quick":              { model: "openai/gpt-5.4-mini" },
  "unspecified-low":    { model: "anthropic/claude-sonnet-4-6" },
  "unspecified-high":   { model: "anthropic/claude-opus-4-7", variant: "max" },
  "writing":            { model: "kimi-for-coding/k2p5" },
}
```

### 6.2 Gear 已有基础

Gear 的 `WorkerCategory` 已有 Quick/Deep/Repair/Review/Explore/Librarian/Visual/ZedNative/Custom，用于 scope、工具策略和 prompt；它们不再拥有模型选择权。

### 6.3 Gear 固定角色映射

| 角色 | 固定 ACP route | 写权限 |
|---|---|---|
| Planner / PlanCritic / Oracle / Reviewer | Codex `openai/gpt-5.6-luna` | 只读 |
| Executor / Repair | OpenCode `opencode-go/deepseek-v4-flash` | 按任务 scope |
| Orchestrator / Summarizer | Gear deterministic | 无 |

不得新增 category model、variant 或 fallback 配置入口。

---

## 七、Review Gate（质量门禁）— P0

### 7.1 omo 的实现

`review-work` skill 历史上启动 5 个并行审查 agent：

| 审查维度 | Agent 类型 | 职责 |
|---|---|---|
| Goal/Constraint | oracle（只读） | 是否满足需求 |
| Code Quality | oracle（只读） | 代码质量、模式 |
| Security | oracle（只读） | 安全漏洞 |
| Hands-on QA | unspecified-high | 实际执行验证 |
| Context Mining | unspecified-high | 从 git/文档检查上下文 |

**全部通过才放行**。

### 7.2 Gear 已有基础

Gear 有 `ROUTE_HINT=review`、四维 hard gate 和 `CoordinatorReview`。strict ACP 模式以串行独立 session 执行审查、修复和复审，不把多 worker 并行当成缺失能力。

### 7.3 Gear 实现要点

```rust
// 在 runtime.rs 的 evaluate() 中扩展：
struct ReviewGate {
    dimensions: Vec<ReviewDimension>,
    require_all_pass: bool,  // 默认 true
}

enum ReviewDimension {
    GoalVerification,   // 需求满足
    CodeQuality,        // 代码质量
    Security,           // 安全审查
    QaExecution,        // 实际验证
    ContextMining,      // 上下文完整性
}

// 审查不通过时 → coordinator_review() → 串行通知 Flash worker 修复，再由新 Luna session 复审
```

---

## 八、Tool Permission per Category（工具权限）— P0

### 8.1 omo 的实现

```typescript
// 每个 agent 有独立的工具限制（从 features.md 提取）
const TOOL_RESTRICTIONS = {
  oracle:     { write: false, edit: false, task: false, call_omo_agent: false },
  librarian:  { write: false, edit: false, task: false, call_omo_agent: false },
  explore:    { write: false, edit: false, task: false, call_omo_agent: false },
  "multimodal-looker":  { read: true },  // allowlist
  atlas:      { task: false, call_omo_agent: false },
  momus:      { write: false, edit: false, task: false },
}
```

### 8.2 Gear 已有基础

Gear 的 `WorkerToolPolicy` 已有 `can_write` / `can_review` / `can_explore` / `question`，但缺少：
- 对外暴露给配置层
- 用户自定义覆盖

### 8.3 Gear 实现要点

```rust
// WorkerToolPolicy 已存在，只需：
// 1. 序列化到配置文件
// 2. 支持用户自定义覆盖每个类别的策略
// 3. worker_prompt() 中渲染策略到 prompt
```

---

## 九、Task Reminder（任务提醒）— P1

### 9.1 omo 实现

```typescript
// hooks/task-reminder/hook.ts
// 10 轮工具调用未使用 task 工具 → 追加提醒消息
const TURN_THRESHOLD = 10
const REMINDER_MESSAGE = `
The task tools haven't been used recently. If you're using task tools,
record progress with task(action=create/update).`
```

### 9.2 Gear 实现要点

```rust
// 在 worker 输出中检测是否长时间无 task 记录
// 追加到 follow-up prompt 中
```

---

## 十、Stop Continuation Guard（停止续跑）— P1

### 10.1 omo 实现

```typescript
// hooks/stop-continuation-guard/hook.ts
// 状态持久化：/"stop"/"stopped" + 级联取消后台任务
// 只通过 /start-work /ulw-loop /ralph-loop 或 session.deleted 清除
```

### 10.2 Gear 实现要点

```rust
// Gear 的 GoalLoop 需要支持用户主动终止：
// 1. CLI: gear run --no-loop
// 2. GUI: "Stop Continuation" 按钮
// 3. 持久化 stop marker 到 Gear runtime-owned `.gear/`
```

---

## 十一、层级 AGENTS.md（/init-deep）— P1

### 11.1 omo 实现

`/init-deep` 按目录生成 AGENTS.md，让 agent 只加载相关上下文：

```
project/
├── AGENTS.md                        # 全局
├── src/
│   ├── AGENTS.md                    # src 上下文
│   └── components/
│       └── AGENTS.md                # 组件上下文
```

### 11.2 Gear 实现要点

```rust
// 作为 Gear CLI 的一个子命令：
// gear init-deep [--max-depth=3]
// 扫描项目目录结构，为每个子目录生成 AGENTS.md
```

---

## 十二、/handoff（Session 交接）— P1

### 12.1 omo 实现

生成结构化 handoff 文档：

```markdown
# Handoff: <session_title>

## Current State
- What was done
- What remains
- Decisions made

## File Map
- path/to/file.rs:42 — 修改点
- path/to/new.rs — 新文件

## Next Steps
1. ...
2. ...
```

### 12.2 Gear 实现要点

```rust
// Gear 的 product.rs 已经有 final_report() 和 artifacts，
// 可以扩展为 handoff 格式输出。
// CLI: gear handoff --session <id>
```

---

## 十三、完整 Hook 清单（omo 54+ hooks）

以下按**功能域**分类，标记 Gear 是否需要：

### 循环/续跑

| Hook | 触发事件 | 功能 | Gear 需要 |
|---|---|---|---|
| `ralph-loop` | `session.idle` | 闲置时自动续跑 | P2 |
| `stop-continuation-guard` | `session.deleted` / `chat.message` | 停止续跑 | P1 |
| `keyword-detector` | `chat.message` | 关键词→模式注入 | P0 |
| `task-reminder` | `tool.execute.after` | 提醒记录进度 | P2 |

### 质量门禁

| Hook | 触发事件 | 功能 | Gear 需要 |
|---|---|---|---|
| `comment-checker` | `tool.execute.before/after` | 注释质量检查 | P0 |
| `plan-format-validator` | `chat.message` | 计划格式验证 | P2 |
| `tool-pair-validator` | `tool.execute.before` | 工具参数校验 | P2 |

### 模型/Token

| Hook | 触发事件 | 功能 | Gear 需要 |
|---|---|---|---|
| `model-fallback` | `session.error` / `chat.message` | 模型降级 | 不吸收：strict ACP 禁止 |
| `runtime-fallback` | `session.error` | 运行时容错 | 仅同 route 瞬态重试 / blocker receipt |
| `compaction-todo-preserver` | `experimental.session.compacting` | 压缩时保留 todo | P2 |
| `read-image-resizer` | `tool.execute.before` | 压缩图片 token | P3 |

### Agent 编排

| Hook | 触发事件 | 功能 | Gear 需要 |
|---|---|---|---|
| `hephaestus-agents-md-injector` | `chat.message` | 注入 AGENTS.md | P1 |
| `directory-agents-injector` | `chat.message` | 注入目录规则 | P1 |
| `sisyphus-junior-notepad` | `chat.message` | 轻量 agent 上下文 | P3 |
| `prometheus-md-only` | `chat.message` | 限制 Prometheus 只读 | P3 |
| `no-sisyphus-gpt` | `chat.message` | 防止 Sisyphus 用非 GPT 模型 | P3 |
| `no-hephaestus-non-gpt` | `chat.message` | 防止 Hephaestus 用非 GPT | P3 |

### 后台

| Hook | 触发事件 | 功能 | Gear 需要 |
|---|---|---|---|
| `background-notification` | `event` | 后台任务完成通知 | P3 |
| `delegate-task-retry` | `tool.execute.after` | 委派任务重试 | P2 |

### 其他

| Hook | 功能 | Gear 需要 |
|---|---|---|
| `hashline-read-enhancer` | 增强行号读取 | P3 |
| `hashline-edit-diff-enhancer` | 增强 diff 显示 | P3 |
| `question-label-truncator` | 截断过长标签 | P3 |
| `think-mode` | 思考模式 | P3 |
| `auto-slash-command` | 自动触发 slash 命令 | P2 |
| `rules-injector` | 注入项目规则 | P1 |

---

## 十四、Gear 吸收优先级总表

| 优先级 | 功能 | 实现方式 | 预估工作量 |
|---|---|---|---|
| — | Category → Model 映射 | strict ACP 固定角色 route，禁止配置入口 | 不实现 |
| — | Fallback Model Chain | 仅同 route 瞬态重试，禁止模型替换 | 不实现 |
| **P0** | Review Gate（多维度） | 扩展 `CoordinatorReview` 为多维度 | ~200 行 |
| **P0** | Comment Checker（Review 阶段） | 在 `CoordinatorReview` 中加一个 check | ~100 行 |
| **P0** | Keyword → Mode 映射 | 在 `Goal` 解析时检测关键词 | ~80 行 |
| **P1** | Tool Permission 可配置 | 序列化 `WorkerToolPolicy` 到配置 | ~50 行 |
| **P1** | 层级 AGENTS.md | `gear init-deep` CLI 子命令 | ~150 行 |
| **P1** | /handoff | 扩展 `product.rs` 的 `final_report()` | ~100 行 |
| **P1** | Stop Continuation Guard | 持久化 stop marker + GUI 按钮 | ~80 行 |
| **P2** | Ralph Loop 事件驱动 | 在 gearbox 应用层加 `session.idle` 监听 | ~300 行 |
| **P2** | Task Reminder | 检测 worker 输出中的 task 使用 | ~50 行 |
| **P2** | Plan Format Validator | 验证 Prometheus 输出的计划格式 | ~100 行 |
| **P3** | 后台任务通知 | 通知缓冲 + 闲置时刷新 | ~80 行 |
| — | 模型 variant | strict ACP 不暴露 variant 配置 | 不实现 |
