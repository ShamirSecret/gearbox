# Phase 06：Lifecycle、residency、reconciliation 与 TTL

## 目标

把销毁、驱逐、启动恢复、TTL 清理统一到生命周期模块，避免 resident handle 泄漏、旧 running record 残留、cancelled/lost 被错误 FIFO 清掉。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/runtime.rs`

## 具体工单

1. 新增统一销毁入口：
   - `destroy_resident_task(task_id, cause) -> DestroyResult`
   - 所有 cancel、interrupt dispose、LRU eviction、TTL cleanup、session shutdown、reconciliation 都路由到这里。
2. destroy 保证：
   - best-effort `abort` / `cancel` / `terminate`
   - 无论 abort 是否失败，都必须调用 `dispose`
   - dispose 错误写 lifecycle event，但不能阻止 residency transition
3. residency admission：
   - 默认 `residency_max_children = 8`
   - 按 parent/root session 统计 resident task。
   - 超过上限时找 LRU evictable task。
4. evictable 状态：
   - 可驱逐：`Completed`、`Failed`、`Interrupted`
   - 不自动驱逐：`Cancelled`、`Lost`
   - 有 pending send 的 task 不驱逐。
5. LRU 失败处理：
   - 找不到可驱逐 task 时返回 `AgentLimitReached` 风格错误。
   - 错误中列出 resident task name/status，便于用户或 Gear 做下一步。
6. 启动 reconciliation：
   - runtime 启动扫描 `.gearbox-agent/workers/*/task-record.json`。
   - 非 terminal 的 `Pending` / `Running` 标记为 `Lost`，而不是 `Failed`。
   - 如果后续记录 pid/session id：
     - pid 缺失：mark lost
     - pid dead：mark lost
     - pid alive orphan：mark lost + terminate orphan
7. TTL cleanup：
   - 默认 `ttl_ms = 24h`。
   - 只删除 terminal 且超过 TTL 的 records。
   - `Lost` + process 记录必须确认 pid dead 后才删除。
8. archive 改造：
   - `completed_archive` 的容量 cap 不能 FIFO 清掉 `Cancelled` / `Lost`。
   - archive 清理与 residency/TTL 的语义分开。

## 测试

1. `destroy_disposes_even_when_abort_fails`
2. `cancel_routes_through_destroy_resident_task`
3. `lru_evicts_oldest_completed_not_cancelled`
4. `lost_record_is_not_ttl_deleted_until_process_dead`
5. `reconcile_marks_running_record_lost`
6. `residency_limit_reports_current_residents`

## 验收

- 所有释放 resident handle 的路径都经过一个 destroy 函数。
- restart 后旧 pending/running 不再显示为假运行态。
- cancelled/lost 不会被普通 archive cap 自动挤掉。
