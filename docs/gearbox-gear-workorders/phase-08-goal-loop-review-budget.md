# Phase 08：GoalLoop、ReviewEngine、budget 与 stagnation guard

## 目标

把 Gear runtime 的核心特色落到循环策略：plan-code-review-repair 自问自答，provider-backed 自审/重规划，独立 reviewer gate，no-progress/stagnation 检测，token/context guard 和统一 budget policy。

## 主要文件

- `crates/gearbox_agent/src/runtime.rs`
- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/gearbox_agent.rs`

## 具体工单

1. Review parser 容错：
   - `GOAL_SATISFIED` / `ROUTE_HINT` / `STOP_REASON` 大小写不敏感。
   - 缺 key 时使用 `unknown` / `none` 默认值。
   - malformed 输出写 parser warning artifact。
   - raw response 永远保留。
2. Review input 补齐：
   - category resolution result
   - fallback history
   - provider/model transform
   - worker transcript head/tail
   - verification result
   - changed files/diff summary
   - no-progress signals
   - budget remaining
3. independent reviewer gate：
   - 高风险任务、用户要求审查、`ROUTE_HINT=review`、连续 unknown、verification 与 provider review 冲突时触发。
   - reviewer 必须是独立 worker/session，不能直接复用刚写代码的 worker claim。
   - reviewer 输出必须进入 Evidence Chain。
4. complete 判定规则：
   - verification failed 永远阻止 complete。
   - provider `STOP_REASON=complete` 只在 deterministic checks 通过时生效。
   - independent reviewer veto 会进入 repair/replan。
5. unified policy：
   - provider-backed review
   - independent reviewer
   - fallback retry
   - premium/depth budget
   - no-progress detector
   - 统一成一个 `GoalDecisionPolicy`，减少逻辑散落。
6. no-progress/stagnation detector：
   - 连续无 diff。
   - 连续相同 verification failure。
   - worker 输出重复或只解释不改代码。
   - review repair request 与上一轮高度相同。
   - transcript 无 tool/output 进展。
7. token/context guard：
   - 检测 token limit、context compaction、session agent 信息不可靠。
   - 状态不可判定时进入 replan/needs_user，不盲目继续 worker。
8. BudgetController 改造：
   - `max_iterations = 5` 保持默认。
   - `max_worker_calls`、`max_premium_worker_calls`、`max_same_failure_retries`、`max_runtime_minutes` 都进入决策 artifact。
   - fallback chain 长度与 retry 机会绑定，但 premium budget 仍可提前截断。
9. final report：
   - `Evidence Chain` 必须列出 spec/plan/worker packet/transcript/result/outcome/verification/review。
   - `limited` 报告必须写“已完成、未完成、为什么停止、下一步建议”。

## 测试

1. `malformed_review_response_falls_back_to_unknown`
2. `verification_failed_blocks_provider_complete`
3. `review_route_hint_triggers_independent_reviewer`
4. `independent_reviewer_veto_forces_repair`
5. `consecutive_unknown_escalates_to_review_then_needs_user`
6. `no_progress_detector_stops_or_upgrades_route`
7. `premium_budget_exhaustion_returns_limited`
8. `final_report_includes_evidence_chain`

## 验收

- Gear 能持续 plan-code-review-repair，直到 complete/limited/blocked/needs_user/cancelled。
- 自审不是一句 provider 文本，而是有 verification、review、evidence 和预算共同参与。
- 没有实质进展时不会无限循环。
