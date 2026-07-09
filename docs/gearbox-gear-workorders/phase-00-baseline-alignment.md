# Phase 00：基线冻结与计划对齐

## 目标

把现有已完成的 Gear MVP 状态固定下来，保证后续重构不会回退：`Gear` session 能从 GUI 进入 `gearbox_agent::runtime::Orchestrator`，`Agent` 与 `Gear` 两个 agent 都能显示，默认 loop budget 与计划保持一致。

## 当前状态

- `crates/gearbox_agent/src/runtime.rs` 的 `DEFAULT_MAX_ITERATIONS` 已对齐为 `5`。
- GUI 已能区分 `Agent` 与 `Gear`，Gear session 已接入 orchestrator。
- `TaskManager`、`WorkerSessionHandle`、`CategoryRouter`、attempt/fallback MVP 已存在。

## 修改范围

- `docs/gearbox-gear-agent-plan.md`
- `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`
- `crates/gearbox_agent/src/runtime.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`

## 具体工单

1. 在主计划中增加本工单索引，说明后续实现以 `docs/gearbox-gear-workorders/` 为执行清单。
2. 检查 `DEFAULT_MAX_ITERATIONS`、计划 Budget、UPSTREAM_SYNC_NOTES 三处是否都写 `5`。
3. 检查 GUI agent 名称：
   - Zed 原生 agent 对用户显示为 `Agent`。
   - Gear coordinator 对用户显示为 `Gear`。
   - 不把上游内部类型名、action name、协议名为了品牌替换而重命名。
4. 固定已有 MVP 回归测试：
   - Gear prompt 进入 orchestrator。
   - greeting 不误触发 orchestrator。
   - native Zed worker follow-up/steer 复用同一个 worker session。
5. 如果后续工单需要改共享源码，先确认 `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md` 已记录 Gear 覆盖层原因。

## 验收

- `cargo test -p gearbox_agent -- --nocapture` 通过。
- `cargo test -p agent test_gear_prompt_runs_gearbox_orchestrator -- --nocapture` 通过。
- `cargo test -p agent test_gear_prompt_greeting_does_not_start_orchestrator -- --nocapture` 通过。
- `docs/gearbox-gear-agent-plan.md` 能链接到每个阶段工单。
