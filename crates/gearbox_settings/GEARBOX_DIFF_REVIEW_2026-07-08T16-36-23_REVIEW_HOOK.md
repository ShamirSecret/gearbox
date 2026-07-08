# Gear 协调器 Review Hook 变更审阅（2026-07-08）

范围：`git diff` 当前未提交改动（不执行测试）

## 变更文件
- `crates/agent/src/agent.rs`

## 变更内容
- 将 `coordinator_review_hook` 回调中的异步调用路径从 `smol::block_on(async { ... })` 改为同步阻塞：
  - `review_tx.send_blocking(...)`
  - `response_rx.recv_blocking()`

## 审阅结论
- 该变更是兼容的：当前 `coordinator_review_hook` 仅在 `Orchestrator::run` 的 `background_spawn` 流程中触发，回调通过 `unbounded` 通道与前台任务交换 review 请求/响应，阻塞等待可避免额外的异步包装。
- 与现有 `RunOptions`、`Event`、`Orchestrator` 流程保持一致，未引入新的接口更改。

## 风险与未确认点（后续建议）
- 未运行测试（按要求跳过）：建议后续在真实执行链路上做一次带 coordinator review 的端到端验证，确认多轮 review 场景不产生阻塞超时。

