# Phase 09：GUI 原生 worker 池、小规模并行与级联取消

## 目标

在前面状态机、lifecycle、notification 都稳定后，开放小规模并行与真正 worker 池：opencode 默认执行，Codex/Claude/Zed Agent 作为可调度 worker，GUI 里能观察和控制多个 task，同时保证 write task 不并行改同一 scope。

## 主要文件

- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/runtime.rs`
- `crates/agent/src/agent.rs`
- `crates/agent_ui/src/conversation_view/thread_view.rs`

## 具体工单

1. `max_parallel_workers > 1`：
   - 默认仍为 1。
   - 仅 read-only explore/review task 可并行。
   - write/repair task 默认串行。
2. dependency model：
   - `TaskRecord.parent_task_id`
   - `root_session_id`
   - `depends_on: Vec<TaskId>`
   - `scope_key` / `write_scope`
3. descendant cancel：
   - 新增 `get_descendant_tasks(session_or_task_id)`。
   - Gear session cancel 时递归取消所有 pending/running 子孙 task。
   - 使用 `skip_notification` 防止通知风暴。
   - pending task 只从 queue 移除；running task 才 abort session。
4. scope guard：
   - read-only tasks 可并行。
   - 两个 write tasks 如果 scope overlap，后者必须等待或被拒绝。
   - worker packet 里写 scope assumption。
5. GUI panel：
   - 从 `thread_view` 内嵌 markdown 区块升级为独立 Gear task panel。
   - 支持筛选：pending/running/terminal/category/worker。
   - 支持排序：updated time、status、category。
   - 支持打开 artifact：packet、transcript、result、outcome、review。
   - 支持 task control：cancel、interrupt、follow-up、steer。
6. Worker pool：
   - opencode：默认 write worker。
   - Zed Agent：native sibling/subagent worker，不递归 Gear。
   - Codex：先 CLI adapter，后 session adapter。
   - Claude：先 CLI adapter，后 session adapter。
   - custom：显式配置命令。
7. worker routing：
   - review/explore 可优先 Zed Agent/Codex/Claude。
   - repair/write 默认 opencode，失败后按 policy 升级。
   - premium worker 调度必须消耗 budget。
8. concurrency fairness：
   - release 时优先唤醒等待队列。
   - per provider/model/category 限流。
   - 防止一个 category 独占所有 worker slot。
9. team/mailbox 边界：
   - 不复制 OMO tmux/team mailbox 全量能力。
   - MVP 只做 task artifact 和 GUI panel，不做 worker 之间自由聊天。

## 测试

1. `read_only_review_tasks_can_run_in_parallel`
2. `write_tasks_with_overlapping_scope_are_serialized`
3. `session_cancel_cascades_to_descendant_tasks`
4. `skip_notification_prevents_cancel_notification_storm`
5. `worker_pool_routes_review_to_non_writer_worker`
6. GPUI：Gear panel 同时显示两个 running read-only task，且 control 按钮作用到正确 task。

## 验收

- Gear 可以统管 opencode、Codex、Claude、Zed Agent/custom worker。
- 小规模并行只开放给安全的 read-only 类任务。
- 用户 cancel Gear session 时不会留下后台子 task。
- GUI 能清楚显示多个 task 的状态、artifact 和控制入口。
