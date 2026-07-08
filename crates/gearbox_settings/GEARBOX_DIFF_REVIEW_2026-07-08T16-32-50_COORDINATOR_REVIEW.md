# Gear 运行时差异审阅（2026-07-08）

范围：`git diff` 当前工作区未提交改动（不执行测试）

## 变更文件
- `crates/agent/src/agent.rs`
- `crates/gearbox_agent/src/cli.rs`
- `crates/gearbox_agent/src/runtime.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`

## 审阅结论

1. **协调器复核链路已打通**
- 在 `agent.rs` 新增 `CoordinatorReview` 数据模型与 `generate_gear_coordinator_review`，并在 `send_gear_prompt` 里建立 review 异步线程；
- `RunOptions` 新增 `coordinator_review_hook`，`cli.rs` 与运行时调用路径默认透传为 `None`；
- 在 `runtime.rs` 中新增：
  - `run_coordinator_review`：每轮运行后调用 hook，输出 `coordinator-review-iteration-{n}.md`；
  - `evaluate_goal` 增加 `coordinator_review` 输入，支持“验证通过但 LLM 认为不满足则继续修复”；
  - `repair_request` 与 `goal_review_artifact` 引入 `coordinator_review` 信息。
- 已修复 `runtime.rs` 主循环未接入该 hook 的空转问题（此前新增函数存在但未调用）。

2. **多 worker 路由更完整**
- `workers.rs` 的测试覆盖增加“多轮 route 切换”；
- `agent.rs` 新增 `GEARBOX_GEAR_WORKER_SEQUENCE` 与各类 worker 命令 env（opencode/codex/claude/zed-agent/custom）映射；
- `run` 的最大迭代与最大文件数从 env 读取：
  - `GEARBOX_GEAR_MAX_ITERATIONS`
  - `GEARBOX_GEAR_MAX_FILES_CHANGED`

3. **上游同步文档更新**
- `UPSTREAM_SYNC_NOTES.md` 同步补充 merge 标注和本次变更说明，保持 Gearbox 与上游差异记录可追踪。

## 风险与注意点（无需修复）

- `run_coordinator_review` 使用 `EventKind::TaskStarted` 记录完成与失败事件，类型语义偏通用，事件消费方若按事件种类过滤需要注意。
- 本次不跑 `cargo test`，仅做差异静态检查；需在后续执行回归命令确认：
  - `cargo check` 能过；
  - 包含 coordinator review 的端到端流程无阻塞；
  - `GEARBOX_GEAR_WORKER_SEQUENCE` + `repair` 场景下任务切换行为与预期一致。

## 备注

- 修改按当前工作区状态保存，包含新增/更新的实现代码、测试和 upstream notes。
