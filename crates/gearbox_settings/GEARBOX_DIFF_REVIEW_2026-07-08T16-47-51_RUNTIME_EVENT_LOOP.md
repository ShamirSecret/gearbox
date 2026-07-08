# Gear 运行链路审阅（2026-07-08T16-47-51）

范围：`git diff` 当前未提交工作区（不跑测试）

## 变更文件
- `crates/agent/src/agent.rs`

## 文件顺序说明
已有审阅文档时间顺序为：
- `...16-22-17`（Bug Scan）
- `...16-26-31`（Diff Review）
- `...16-32-50`（Coordinator Review）
- `...16-36-23`（Coordinator Review Hook）
- `...16-47-51`（本次）

## 结论

1. **事件循环已改为“可提前返回”模式**
- `agent.rs` 中把 `run_task.await` 改为 `futures::select!`，在 `event_rx.recv()` 与 `run_task` 之间竞态等待；
- 当运行任务完成后立即退出并处理运行结果，避免了之前事件通道关闭前空转等待的可能。

2. **剩余事件清理逻辑保持**
- 运行任务完成后，用 `while let Ok(message) = event_rx.try_recv()` 再清空通道中的已到达事件；
- 与 `gear run` 的终态 markdown 追加时序一致，属于兼容性增强，不引入额外语义变化。

3. **并发任务句柄处理**
- `coordinator_review` 的后台任务在 `send_gear_prompt` 末尾通过 `drop(review_task)` 释放；
- 当前逻辑下 `review_tx` 在 `generate_gear_coordinator_brief` 之后就已 `drop(review_tx)`，主流程不再继续提交新 review 请求，`drop` 本身不会影响既有最终汇总的事件消费顺序。

## 风险与建议（非阻塞）
- 尽管未执行测试，建议后续在一次包含协调器复核的场景下执行一次端到端：
  - 运行包含 `GEARBOX_COORDINATOR_REVIEW` 场景的会话；
  - 观察一次 `run_task` 成功/失败下是否都能持续产出并清理 coordinator review 事件。

## 备注
- 本审阅仅覆盖当前工作区 diff；与 `docs/gearbox-gear-agent-plan.md` 的对齐状态需要下一步在全量 diff 上持续跟进。
