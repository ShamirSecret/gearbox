# Phase 02：TaskManager control plane 与并发释放守卫

## 目标

把 `TaskManager` 从“可运行的串行 MVP”加固成真正 control plane：等待者不阻塞控制路径、并发槽释放幂等、revive epoch 可重新 acquire、任务清理有明确入口。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/runtime.rs`
- `crates/agent/src/agent.rs`

## 具体工单

1. 新增 `ReleaseGuard`：
   - 内部 key 使用 `(task_id, run_epoch)`。
   - `release_once(task_id, epoch)` 第一次返回 true，后续返回 false。
   - task 被 `forget` 后清理该 task 所有 release key。
2. 改造 `ConcurrencyManager`：
   - 从只做 `can_start()` 检查，升级为显式 `acquire` / `release`。
   - MVP 可仍然由 `tick()` 驱动启动，但 release 必须走统一函数。
   - 后续支持等待队列时，`release()` 优先移交给 waiter，无 waiter 再递减计数。
3. 引入 `TaskWaiter` 队列：
   - `wait_for(task_id)` 不长期独占 `&mut TaskManager`。
   - completion 时唤醒该 task 的所有 waiter。
   - GUI cancel/interrupt/follow-up 在 wait 期间仍可进入。
4. 明确 `current_task` API 与 task-id API：
   - MVP GUI 继续使用 `TaskManagerControl::{cancel_current_task, interrupt_current_task, send_follow_up_current_task, steer_current_task}`。
   - 内部保留 task-id 级别函数，给 Phase 09 递归取消和并行 worker 使用。
5. 新增 `forget_task(task_id)`：
   - 移除 running handle。
   - 移除 completed error/run cache。
   - 清理 waiters。
   - 清理 release guard。
   - 不删除已经写到磁盘的 task artifacts。
6. 把 `settle_finished_task` / `settle_running_task` 拆成小步骤：
   - 加载 record。
   - 通过 Phase 01 transition 写 terminal。
   - release `(task_id, run_epoch)`。
   - settle waiters。
   - 写 archive / snapshot。
7. HashMap 访问不要 `unwrap` / `expect`：
   - task 不存在返回 `not_found` 风格结果。
   - record 丢失但 running handle 存在时写 lifecycle audit，然后清理孤儿内存状态。

## 测试

1. `release_guard_is_epoch_scoped`
   - epoch 0 release 两次只计一次。
   - revive 到 epoch 1 后 release 可再次计一次。
2. `wait_for_does_not_block_cancel`
   - wait_for 正在等待时，另一路 control cancel 能生效。
3. `late_finished_message_after_forget_is_ignored`
   - forget 后收到旧 completion message，不 panic，不复活旧 task。
4. `concurrency_slot_is_released_once_on_cancel_completion_race`
   - cancel 路径和 completion dispatcher 同时到达，只释放一次。
5. `snapshot_counts_include_interrupted_and_lost`
   - Phase 01 新状态在 snapshot counts 中正确统计。

## 验收

- `TaskManager` 的状态写入、释放、等待者唤醒都有单一入口。
- 重复 completion / cancel / interrupt 不会导致并发槽重复释放。
- wait_for 期间 GUI control path 不被 manager 锁阻塞。
