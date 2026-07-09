# Phase 01：TaskRecord 状态机与驻留语义

## 目标

先补齐 OMO 最核心的状态机边界：终端状态幂等、驻留状态独立、run epoch 去重、lost/interrupted 区分。后续 steering、revive、completion notification 都依赖这一层。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`
- `crates/gearbox_agent/src/runtime.rs`

## 数据结构修改

1. 扩展 `ManagedTaskStatus`：
   - 保留：`Pending`、`Running`、`Completed`、`Failed`、`Cancelled`、`Skipped`
   - 新增：`Interrupted`、`Lost`
   - 后续考虑把 `Failed` 序列化别名映射到 OMO 语义的 `error`，但第一步不做破坏性改名。
2. 新增 `ResidencyState`：
   - `Resident`
   - `Evicted`
   - `Disposed`
   - `PersistedOnly`
   - `RpcDetached`
3. 扩展 `TaskRecord`：
   - `residency_state: ResidencyState`
   - `run_epoch: u64`
   - `notified_epoch: i64`
   - `notification_failed_epoch: Option<u64>`
   - `killed: bool`
   - `parent_session_id: Option<String>`
   - `root_session_id: Option<String>`
4. 扩展 `TaskSnapshot` / GUI snapshot：
   - 暴露 `residency_state`
   - 暴露 `run_epoch`
   - 暴露 `notified_epoch`
   - `TaskManagerSnapshotCounts` 增加 `interrupted`、`lost`

## 状态转换修改

1. 新增 `TaskTransition` enum，至少包含：
   - `Start`
   - `Complete { summary, result_path, outcome_path }`
   - `Fail { failure_kind, error }`
   - `Cancel { reason }`
   - `Interrupt { reason, partial_output }`
   - `MarkLost { reason, killed }`
   - `Revive { prompt_path }`
   - `MarkResident`
   - `Evict`
   - `Dispose`
   - `PersistOnly`
   - `DetachRpc`
2. 新增 `transition_task_record(record, transition, timestamp) -> TaskTransitionResult`：
   - terminal 状态上只允许 residency-only transition。
   - 非法转换返回 `applied: false`，写 audit reason，不 panic。
   - `Revive` 只允许 `Completed` / `Failed` / `Interrupted` 且 `ResidencyState::Resident`。
   - `Cancelled` / `Lost` 不允许 revive。
3. 把以下直接写 `record.status = ...` 的路径改为调用转换函数：
   - `TaskManager::start`
   - `TaskManager::start_queued_task`
   - `TaskManager::settle_running_task`
   - `TaskManager::cancel_task`
   - `TaskManager::interrupt_task`
   - `TaskManager::recover_orphaned_records`
   - stale sweep 路径
4. `recover_orphaned_records` 和 stale timeout 不再写 `Failed`，改写 `Lost`，并记录 `TaskFailureKind`。
5. `append_task_lifecycle_event` 增加 transition audit 字段：
   - `transition_type`
   - `applied`
   - `previous_status`
   - `previous_residency_state`
   - `run_epoch`

## 兼容性要求

- 老的 `task-record.json` 缺少新字段时必须能反序列化：
  - `residency_state` 默认 `Resident`
  - `run_epoch` 默认 `0`
  - `notified_epoch` 默认 `-1`
  - `killed` 默认 `false`
- GUI status label 要补 `interrupted`、`lost`，不能 panic 或显示空串。

## 测试

1. `cancelled_task_is_not_overwritten_by_late_completion`
   - 先把 running task 转 `Cancelled`
   - 再模拟 completion dispatcher 返回 completed
   - 验证状态仍为 `Cancelled`，transition audit 为 late ignored
2. `interrupted_task_keeps_partial_output_and_allows_revival`
   - running -> interrupt
   - 保存 partial output
   - 后续 `messageability` 阶段才能 revive，本阶段先验证状态/字段。
3. `lost_task_is_terminal_and_not_revivable`
   - stale/recover 路径写 `Lost`
   - late completion 不覆盖 `Lost`
4. `terminal_status_allows_only_residency_transition`
   - completed -> evicted 可以
   - completed -> failed 不可以
5. `old_task_record_defaults_new_fields`
   - 用缺少新字段的 JSON 反序列化。

## 验收

- 所有 task 状态修改都集中在 `transition_task_record`。
- `Cancelled` / `Interrupted` 不会被 late completion 覆盖。
- `Lost` 能表示 timeout/reconciliation，不再和确定性 `Failed` 混在一起。
- GUI snapshot 能显示新状态和 epoch，不崩溃。
