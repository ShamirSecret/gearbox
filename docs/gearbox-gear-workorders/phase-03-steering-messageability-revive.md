# Phase 03：Steering、messageability、queued delivery 与 revive

## 目标

实现 OMO steering 的核心语义：先转换再 abort、任务可消息性矩阵、pending 消息队列、terminal resident revive、scope 检查和类型化返回。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`

## 具体工单

1. 新增 `Messageability`：
   - `Steer`
   - `Revive`
   - `NotContinuable { reason }`
2. 实现 `messageability(record) -> Messageability`：
   - `Pending` / `Running` + `Resident` -> `Steer`
   - `Completed` / `Failed` / `Interrupted` + `Resident` -> `Revive`
   - `Cancelled` / `Lost` -> `NotContinuable`
   - `Evicted` / `Disposed` -> `NotContinuable`
3. 改造 `cancel_task`：
   - 第一步调用 `transition_task_record(... Cancel ...)`。
   - 只有 transition applied 后才 best-effort 调 `handle.cancel()` 或 `handle.abort()`。
   - abort/cancel 错误只记录 lifecycle event，不阻止后续 destroy。
   - queued task cancel 走同一 transition，再从 queue 移除。
4. 改造 `interrupt_task`：
   - 第一步 transition `Running -> Interrupted`。
   - 再 best-effort 调 `handle.interrupt()`。
   - 调用 `handle.last_output()` 捕获 partial output，写入 record summary 或 interrupt artifact。
   - late completion 被 Phase 01 守卫忽略。
5. 新增 pending message queue：
   - `pending_messages: HashMap<TaskId, VecDeque<QueuedMessage>>`
   - `QueuedMessage { message, deliver_as: FollowUp | Steer, caller_session_id, created_at }`
   - 对 pending task 调 send/steer 时返回 `queued`。
   - `start_queued_task` 成功后调用 `drain_pending_messages(task_id)`。
   - drain FIFO，一个失败写 event 但继续投递后续消息。
6. 新增 `revive_terminal_task`：
   - 仅允许 `Messageability::Revive`。
   - 调 `handle.send_follow_up(message)`，复用同一 resident session。
   - 清空 terminal 字段：`error`、terminal summary 中的错误、旧 result/outcome 指针按需要移入 attempt。
   - `run_epoch += 1`，`notified_epoch` 不变。
   - 重新 acquire concurrency slot。
   - 状态写回 `Running`。
7. 新增 scope check：
   - control input 带 `caller_session_id` 和 `all_scope`。
   - caller 必须等于 `parent_session_id` 或 `root_session_id`，否则返回 `scope_denied`。
   - GUI current-task 快捷路径可自动传当前 Gear session id。
8. 把 `Result<bool>` 改成类型化返回：
   - `CancelOutcome::{Cancelled, Noop, NotFound, ScopeDenied}`
   - `InterruptOutcome::{Interrupted, Noop, NotFound, ScopeDenied}`
   - `SendOutcome::{Steered, Revived, Queued, NotContinuable, NotFound, ScopeDenied}`
   - GUI 层再把 outcome 转为用户可读文案。

## 测试

1. `cancel_transitions_before_abort_and_ignores_late_completion`
2. `interrupt_captures_partial_output`
3. `pending_follow_up_is_drained_after_start`
4. `pending_steer_delivery_failure_does_not_block_next_message`
5. `completed_resident_follow_up_revives_same_task_and_increments_epoch`
6. `cancelled_task_is_not_continuable`
7. `scope_denied_for_unrelated_session`

## 验收

- cancel/interrupt 不再先操作 handle 后写状态。
- pending task 可以接收 queued follow-up/steer。
- completed/failed/interrupted resident task 可以在同一 session revive。
- cancelled/lost/evicted/disposed 明确拒绝消息。
