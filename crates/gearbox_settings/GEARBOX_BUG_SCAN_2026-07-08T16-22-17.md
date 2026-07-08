# Gearbox Bug Scan（2026-07-08）

静态检查结果（仅读代码，不运行测试）。

## 1）before_diff 只在循环外初始化，未在迭代间更新（P0）
**位置：** `crates/gearbox_agent/src/runtime.rs:190`  
`before_diff` 在 `for iteration in 1..=max_iterations` 之前初始化一次，循环中只更新 `after_diff`（`runtime.rs:311`），后续 `WorkerStarted`（`runtime.rs:247`）与 `DiffDetected`（`runtime.rs:325`）仍使用同一份快照。  

影响：每轮 repair 的“本轮前状态”不是实际轮次前状态，可能导致 worker 复盘与修复依据偏移。  
建议：每轮开始前将 `before_diff` 置为上轮 `after_diff`，并在下一轮结束前更新该变量。

---

## 2）`NeedUser` 语义被最终事件压平为 `GoalBlocked`（P1）
**位置：** `crates/gearbox_agent/src/state.rs:81`, `runtime.rs:76`, `runtime.rs:488`  
`GoalStatus` 有 `NeedsUser`（`state.rs:81`），但终态映射仅区分 `Complete/Limited` 之外统一写成 `GoalBlocked`（`runtime.rs:488-491`）。  

影响：`evaluate_goal` 返回 `NeedsUser` 后，前端/埋点只能看到 `GoalBlocked`，无法区分“需人工介入”。  
建议：增加 `GoalNeedsUser` 事件类型并映射 `NeedUser`，或在现有 `GoalBlocked` 下补字段保留原因码。

---

## 3）`coordinator_brief` 只生成一次且未按每轮 review 重算（P1）
**位置：** `crates/agent/src/agent.rs:2441`, `runtime.rs:278`  
GUI 只在启动 `Orchestrator::run` 前生成一次 `coordinator_brief`（`agent.rs:2441-2444`），运行期每轮都复用同一段 brief 传给 `WorkerRunRequest`（`runtime.rs:277-279`）。  

影响：第一次执行失败后，后续修复迭代缺少“基于新验证结果的二次指导”。  
建议：在每轮 review 后按最新 `goal_review` 与验证结果重新生成/更新 brief。

---

## 4）ACP stdio server 路径未接入（P1）
**位置：** `crates/gearbox_agent/src/cli.rs:17-20`, `crates/gearbox_agent/src/main.rs:1-3`, `crates/gearbox_agent/src/product.rs:292`  
`gearbox_agent` CLI 仅提供 `gear run` 子命令，文档注释与 final report 也明确写“ACP server integration is intentionally deferred”。  

影响：当前运行时无法以标准 ACP stdio 外部协议接入（只能走本地 CLI / GUI）。  
建议：按现有运行入口新增 stdio server 命令/handler，补齐与 ACP 生态的最小对接层。

---

## 5）`WorkerRegistry` 未按 kind 做分发，全部走同一 `CommandWorker`（P2）
**位置：** `crates/gearbox_agent/src/workers.rs:126-133`  
`WorkerRegistry::run` 总是 `CommandWorker { kind: request.config.worker_kind }`，未有 `match` 或注册表分流。`WorkerKind` 枚举存在但没有形成动态路由。  

影响：`WorkerKind::Codex/Claude/ZedAgent/Custom` 与实际运行行为未解耦，无法验证不同 worker adapter。  
建议：在 Registry 中按 kind 实例化/路由不同 adapter，并保留统一接口。

---

## 6）Workspace 选择路径来源未统一排序，存在非确定性（P2）
**位置：** `crates/agent/src/agent.rs:2713-2720`, `agent.rs:2726-2729`  
`session.work_dirs.paths()` 与 `state.project.visible_worktrees(cx).next()` 两个来源都可能返回多个路径，但无统一排序或稳定优先级。  

影响：在多 worktree/路径场景下，启动 session 时不同调用可能选择不同 workspace，影响可复现性。  
建议：统一路径来源并固定排序规则（例如按 path/canonicalize 后排序），保证会话级路径稳定。

---

## 7）非阻塞：Gear UI/消息层仍有遗留英文（P2）
**位置：** `crates/ui/src/components/collab/update_button.rs:98/111/125`, `crates/collab_ui/src/notifications/incoming_call_notification.rs:131`, `crates/collab/src/auth.rs:36`  
有若干可见文案仍是英文（更新提示、Collab 提示、错误提示），当前缺乏统一到 Gearbox 品牌/中文文本的闭环。  

影响：品牌/本地化一致性不完整，不影响运行逻辑但影响最终交付质量。  
建议：沿 `GEARBOX_L10N_AUDIT.md` 继续清单化替换，并把残留点收敛到同一命名规范。

---

## 附：已确认不再成立的旧条目
- `runtime.rs` 里的事件 JSON 已不再出现重复 `"iteration"` 字段；该条可从旧文档移除。

