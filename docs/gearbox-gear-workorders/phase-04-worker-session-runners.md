# Phase 04：WorkerSessionHandle 与 runner 生命周期

## 目标

把 `WorkerSessionHandle` 从“最终结果等待器”升级为 OMO `ChildHandle` 风格的 session 抽象：可订阅中间事件、可显式 dispose、可 abort、可 revive 新 turn，并能承载 opencode / Zed Agent / Codex / Claude 的 session worker。

## 主要文件

- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/runtime.rs`
- `crates/agent/src/agent.rs`

## 接口修改

1. 扩展 `WorkerSessionHandle`：
   - `fn abort(&self) -> Result<()>`
   - `fn dispose(&self) -> Result<()>`
   - `fn subscribe(&self, listener: WorkerEventListener) -> Result<WorkerSubscription>`
   - `fn wait_for_idle(&self) -> Result<WorkerTurnOutcome>`
2. 保留兼容层：
   - `wait_for_outcome` / `wait_for_result` 可先保留，但内部应逐步收敛到 `wait_for_idle`。
   - command worker 不支持 subscribe 时必须返回明确 unsupported，不得静默成功。
3. 新增 worker event 类型：
   - `AssistantTextDelta`
   - `ToolCallStarted`
   - `ToolCallFinished`
   - `WorkerStdout`
   - `WorkerStderr`
   - `TurnStarted`
   - `TurnFinished`
   - `Error`
4. 新增 transcript artifact：
   - `transcript.jsonl`
   - `tool-events.jsonl`
   - `partial-output.md`
   - `turn-<epoch>-result.json`
5. runner 内部使用 `turn_active` / `current_turn` 语义：
   - follow-up during active turn：进入 session 队列。
   - follow-up while idle and resident：`begin_turn(prompt)`，替换 current turn。
   - `wait_for_idle` 总是等待当前 turn。
6. command-backed worker：
   - 支持 `abort` 映射到进程 kill/cancel token。
   - `dispose` 幂等。
   - 不具备真正 session revive 时，必须在 `WorkerOutcome` 中标明 `session_capability=command_resident_mvp`。
7. opencode worker：
   - 继续保留 resident-command MVP。
   - 新增 opencode-native backend 适配点，但不要把 TaskManager 绑定到 opencode API。
8. Zed Agent native worker：
   - 已有 native dispatcher 基础上补 event subscribe。
   - follow-up/steer 的中间输出写 transcript，而不是只在 turn 结束写 last message。
9. Codex / Claude worker：
   - CLI adapter 第一阶段继续作为 command worker。
   - 预留 session adapter trait，后续接入真正长驻会话。

## 测试

1. `worker_subscribe_writes_transcript_events`
2. `dispose_is_idempotent`
3. `abort_after_cancel_does_not_prevent_dispose`
4. `follow_up_while_idle_begins_new_turn`
5. `wait_for_idle_waits_for_latest_revived_turn`
6. `command_worker_unsupported_subscribe_is_explicit`
7. GPUI：native Zed worker follow-up/steer 仍复用同一 subagent session，并产出 transcript。

## 验收

- `TaskManager` 能订阅 worker 中间事件。
- review 不再只能依赖最终 stdout/outcome。
- session worker 具备显式 dispose，取消/驱逐/TTL 都能统一释放资源。
