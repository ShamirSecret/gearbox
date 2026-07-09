# Gear Runtime 阶段工单索引

这些工单把 `docs/gearbox-gear-agent-plan.md` 的阶段计划和 OMO 细粒度对比报告落成可执行修改清单。每个阶段一个文件，按依赖顺序推进；如果实现时发现代码已提前完成，应在对应工单中把状态改为“已完成”并补上证据测试。

## 执行顺序

1. [Phase 00：基线冻结与计划对齐](phase-00-baseline-alignment.md)
2. [Phase 01：TaskRecord 状态机与驻留语义](phase-01-state-machine.md)
3. [Phase 02：TaskManager control plane 与并发释放守卫](phase-02-task-manager-control-plane.md)
4. [Phase 03：Steering、messageability、queued delivery 与 revive](phase-03-steering-messageability-revive.md)
5. [Phase 04：WorkerSessionHandle 与 runner 生命周期](phase-04-worker-session-runners.md)
6. [Phase 05：Completion notification 与 GUI parent wake](phase-05-completion-parent-wake.md)
7. [Phase 06：Lifecycle、residency、reconciliation 与 TTL](phase-06-lifecycle-residency-cleanup.md)
8. [Phase 07：Category、fallback、provider/model policy](phase-07-category-fallback-model-policy.md)
9. [Phase 08：GoalLoop、ReviewEngine、budget 与 stagnation guard](phase-08-goal-loop-review-budget.md)
10. [Phase 09：GUI 原生 worker 池、小规模并行与级联取消](phase-09-gui-parallel-worker-pool.md)

## 共同约束

- Gear runtime 的 goal 完成权只属于 `GoalLoop`，worker 不得直接把 goal 标为 complete。
- 所有 task 状态变更必须走 `TaskManager` 的统一转换函数，禁止在不同路径直接覆盖 `record.status`。
- 取消、中断、失败、丢失、完成必须保留可审计 artifact，不能只在内存里更新状态。
- GUI 中 `Agent` 和 `Gear` 必须继续作为两个原生 agent 分开显示。
- MVP 默认 worker 仍是 opencode，但数据结构不得把 runtime 绑定死到 opencode。
- 每个阶段完成后至少运行：
  - `cargo fmt -p gearbox_agent`
  - 与修改点对应的 `cargo test -p gearbox_agent ...`
  - 如果触碰 GUI 侧 agent/task panel，再运行对应 `cargo test -p agent ...` 或 GPUI 测试。
