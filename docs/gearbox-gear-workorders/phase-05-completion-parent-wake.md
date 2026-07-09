# Phase 05：Completion notification 与 GUI parent wake

## 目标

实现 OMO completion notification 的关键守卫：只通知 externally-caused terminal、epoch 去重、缓冲、GUI parent session 忙碌检测，避免 worker 完成消息打断用户正在进行的 Agent/Gear 对话。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/runtime.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`

## 具体工单

1. 新增 `CompletionNotifier`：
   - 输入：`TaskRecord`、parent session state、event sink。
   - 输出：`NotificationResult::{Skipped, Sent, Buffered, Dropped, Failed}`
2. 新增通知触发规则：
   - 只通知 `Completed` / `Failed` / `Lost`。
   - 不通知 `Cancelled` / `Interrupted`，因为它们由用户同步控制路径返回。
   - 非 terminal 不通知。
   - `notified_epoch >= run_epoch` 不重复通知。
3. 新增运行时去重缓冲：
   - buffer key: `(task_id, run_epoch)`
   - 同一 key 多次进入 buffer 只保留一条。
   - flush 前重新加载最新 record，再写 `notified_epoch`。
4. 新增 parent session state：
   - `Idle`
   - `Streaming`
   - `Compacting`
   - `SessionSwitching`
   - `SessionShutdown`
   - MVP 如果 GUI 侧无法精确判断，先以保守 `Streaming` / `Idle` 两态接入。
5. 路由策略：
   - `Idle` -> wake / append completion message。
   - `Streaming` -> 延迟到下一个 tool boundary 或当前 turn 结束。
   - `Compacting` / `SessionSwitching` / `SessionShutdown` -> buffer。
6. 新增 debounce：
   - 默认 100ms 合并连续 worker completion。
   - 同一 parent session 的完成通知串行化。
7. 新增 delivery retry：
   - 首次失败记录原因。
   - 用短退避重试，不做 OMO 那种无延迟同调用双重重试。
   - 超过窗口后写 `notification_failed_epoch`。
8. completion message 内容：
   - task id / name
   - status
   - duration
   - final response head
   - continuation hint
   - artifact links
9. GUI 集成：
   - 当前 `gear_task_manager_snapshot_to_markdown` 继续作为 snapshot。
   - completion notification 不直接插入正在 streaming 的 assistant 文本中间。
   - 若后续改为专门 Gear panel，则 notifier 写 panel event，不写聊天流。

## 测试

1. `cancelled_and_interrupted_do_not_emit_completion_notification`
2. `same_epoch_completion_notified_once`
3. `revived_epoch_completion_notifies_again`
4. `streaming_parent_buffers_completion`
5. `buffer_flush_deduplicates_task_epoch`
6. `delivery_failure_records_notification_failed_epoch`

## 验收

- 用户 cancel/interrupt 后不再额外刷一条异步 “cancelled” 消息。
- 同一 worker 同一 epoch 只通知一次。
- worker completion 不会插入用户正在看的 streaming 回复中间。
