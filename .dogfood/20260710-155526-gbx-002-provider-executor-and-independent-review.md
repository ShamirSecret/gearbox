# Gearbox Provider 执行边界与独立审查证据修复计划

```yaml
plan_id: GBX-002
artifact_kind: repair_plan
status: draft
created_at: "2026-07-10 15:55:26 +0800"
slug: provider-executor-and-independent-review
canonical_path: .dogfood/20260710-155526-gbx-002-provider-executor-and-independent-review.md
target_id: GBX-001
related_ids:
  - GBX-001
```

## Summary

修复 GBX-001 审查后仍未闭环的两个运行时边界：provider variant/tool policy 必须通过真实执行器生效或明确拒绝；Review Gate 的每个必需维度必须引用真实、可定位且不可复用的 reviewer execution artifact。GBX-001 后续补丁已使 ACP session ID 成为 continuation 的唯一键，并删除单槽 fallback；本计划不重复该修复，只补齐可验证的 dispatch 与 review authority。

所有工单串行执行。不得把 packet、prompt、环境变量或合成 execution ID 当作已经执行的 provider/reviewer 证据。

## Evidence / Current State

- `WorkerRegistry::start()` 已在 dispatch 前校验 variant，并把 command-worker variant 与 policy 写入 `GEARBOX_WORKER_MODEL_VARIANT` / `GEARBOX_WORKER_TOOL_POLICY`；native Zed worker 无 provider-selection contract 时会 fail closed。
- 外部 command worker 的子工具仍在其进程内部执行，当前 host 只能拒绝 category 必需能力，不能拦截单个工具调用。
- `ReviewGate::from_inputs()` 为四个维度填充固定字符串 execution ID，且 `artifact_path` 为 `None`；`validate_independent_reviewers()` 没有生产调用。
- `OutcomeContext` 已保存 task/epoch/queue 字段，但 Gear UI 仍主要显示 TaskManager snapshot，未对每种命令回执做端到端断言。

## Root Cause Analysis

`observed_symptoms`:

- provider variant 被命名为 applied，却没有统一的 backend capability contract；原生与 command 后端的真实请求面不同。
- tool policy 字段在 worker packet 中可见，但 external command 的工具调用不经过 Gear host。
- Review Gate 用合成 ID 满足唯一性单元测试，未连接实际 review worker 的 task record/result/outcome artifact。

`likely_root_cause`:

- route resolution、provider request construction 和 backend dispatch 没有共享的 typed capability/dispatch object。
- reviewer 只是 category route，而不是有稳定 execution identity 与 verdict 的一等运行时对象。

`code_paths_to_verify`:

- `crates/gearbox_agent/src/workers.rs::{ProviderAdapter,WorkerRegistry::start,start_command_backed_worker,CommandWorkerSessionHandle::execute_command_with_prompt}`
- `crates/agent/src/agent.rs::{GearZedWorkerBackend::start_zed_agent,run_native_zed_worker}`
- `crates/gearbox_agent/src/runtime.rs::{ReviewGate,run_coordinator_review,Orchestrator::run}`
- `crates/gearbox_agent/src/task_manager.rs::{TaskRecord,TaskAttempt,OutcomeContext}`
- `crates/agent_ui/src/conversation_view/thread_view.rs`

`failure_chain`:

1. route 选择 variant/tool policy。
2. adapter 只校验或写 metadata，backend 无可审计的 applied request。
3. external worker 在 host 之外执行工具，host 无法证明 deny 在执行器前发生。
4. Review Gate 用字符串构造“独立”证据，测试只验证字符串不重复。
5. final report 把这些元数据当作闭环证据，造成假完成。

`why_previous_fix_failed`:

GBX-001 把结构、artifact 和单元测试先补齐，但没有把能力选择、实际 dispatch、worker execution 和 gate verdict 组成同一条数据链。

`validation_gap`:

缺少捕获真实 command/native dispatch 参数的 fake backend，缺少 denied tool 到 executor 零调用断言，缺少 reviewer task record/artifact 的生产绑定，以及 Gear UI/API 回执回归测试。

`uncertainty`:

原生 Zed 子线程当前是否能在不改 ACP schema 的前提下选择指定 provider/model 需要先证明；若不能，必须保留 fail-closed 并停止该路径的 variant 支持。

## Non-Goals

- 不修改 ACP schema、通用 Agent 协议或非 Gear cancellation 语义。
- 不为旧 `.gearbox-agent/continuation/state.json` 数据增加双读迁移层。
- 不增加未经验证的 provider 名称、模型映射或 prompt-only 工具绕过。
- 不重构无关的 TaskManager 调度、品牌资源或上游 UI。

## Chief Prompt

按 GBX-002-001 至 GBX-002-005 串行执行。每个工单先提交 `WORK_ORDER_ROUTING_BRIEF`，再提交 `WORK_ORDER_EVIDENCE`；两者都必须包含 `scope_check`、`forbidden_check`、`acceptance_check`、`validation_check`。没有真实 backend/reviewer artifact 的字段必须标记 unavailable 或拒绝，不能写合成值。任何需要 ACP schema、新凭据或未授权网络服务的步骤立即停止并报告。

## Preflight

1. 工作目录必须是 `/home/donald/文档/github/zed`。
2. 阅读 `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`、GBX-001 计划和 result summary。
3. 记录 `git status --short --branch`，不覆盖已有 `.omo/boulder.json` 或其他用户修改。
4. 用 CodeGraph 复核 ProviderAdapter、WorkerRegistry、GearZedWorkerBackend、ReviewGate 和 TaskRecord 调用链。
5. 若存在活动 run，只使用 `.machine/sessions/<session-id>/runs/run-N/` 下的证据。

## Work Orders

### GBX-002-001 PROVIDER-CAPABILITY-CONTRACT

- `role`: plan
- `objective`: 为 command 与 native backend 写出最小 typed capability contract，并证明每种 backend 的 variant 参数实际入口。
- `allowed_files`: `crates/gearbox_agent/src/workers.rs`, `crates/agent/src/agent.rs`, `crates/gearbox_agent/tests/**`
- `forbidden_files`: ACP schema、第三方 provider 实现、无关 UI、prompt-only policy。
- `inputs`: GBX-001 summary、ProviderAdapter、command env dispatch、GearZedWorkerBackend。
- `steps`: 添加 fake command/native backend 捕获最终 dispatch options；区分 supported、unsupported、unavailable；若 native 无 model-selection API，记录 fail-closed evidence 并禁止该 capability。
- `acceptance`: 至少一个 command 或 native backend 捕获到 variant 改变的真实 dispatch 参数；unsupported 与 unavailable 有结构化原因；不能证明入口的 backend 不接受 variant。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent workers -- --nocapture`
- `rollback`: 回滚 capability object 与所有 backend 调用方为同一提交；不得留下 metadata-only applied 字段。

### GBX-002-002 TOOL-EXECUTOR-GATE

- `role`: build
- `objective`: 让 disabled tool 在真实 executor 调用前被拒绝，而非只限制 category 或 prompt。
- `allowed_files`: `crates/gearbox_agent/src/workers.rs`, `crates/agent/src/agent.rs`, `crates/agent/src/tool_permissions.rs`, 相关 Gear-only 测试, `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`
- `forbidden_files`: ACP schema、非 Gear tool permission 行为、全局 sandbox 默认值。
- `inputs`: GBX-002-001 capability contract、native tool dispatch、command worker adapter。
- `steps`: 为可拦截的 native executor 接入 typed allow/deny gate；为不可拦截 external command 定义明确 capability boundary并拒绝声称 host enforcement；增加 executor invocation counter 测试。
- `acceptance`: denied native tool 的 executor counter 为零；command backend 清楚声明其 boundary；review/explore/write 策略差异有测试；无未知 tool 默认允许。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent workers -- --nocapture`; `cd /home/donald/文档/github/zed && cargo test -p agent gear -- --nocapture`
- `rollback`: 原子回滚 gate 与测试；不得把拒绝回退为 prompt 文案。

### GBX-002-003 REVIEWER-EXECUTION-EVIDENCE

- `role`: build
- `objective`: 让每个 Review Gate dimension 引用真实 reviewer execution、route、artifact path 和 verdict。
- `allowed_files`: `crates/gearbox_agent/src/runtime.rs`, `crates/gearbox_agent/src/task_manager.rs`, `crates/gearbox_agent/src/state.rs`, `crates/gearbox_agent/tests/**`
- `forbidden_files`: ACP schema、第二套 TaskManager、合成 execution ID、无关 UI。
- `inputs`: ReviewGate、review route、TaskRecord attempts、worker result/outcome artifacts。
- `steps`: materialize reviewer execution identity from TaskAttempt/task-record; 在 gate construction 前验证 required dimension 的 artifact 与 verdict；重复 execution 或空 artifact hard fail；将 validation/scope 等非 reviewer 检查标为其真实 execution type，不伪装成 reviewer。
- `acceptance`: 一个真实 review worker artifact 可被 UI/API 追溯；同一 execution 不能填充多个要求独立的 reviewer dimension；缺失 artifact/inconclusive verdict 阻止 completion。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent runtime -- --nocapture`
- `rollback`: 整体回滚 evidence schema 与 gate 调用方；不得保留 synthetic fallback。

### GBX-002-004 ACP-SESSION-AND-OUTCOME-INTEGRATION

- `role`: build
- `objective`: 用生产 Gear ACP 路径验证 session-scoped continuation 与自描述 outcome。
- `allowed_files`: `crates/agent/src/agent.rs`, `crates/agent/src/tests/**`, `crates/agent_ui/src/conversation_view/thread_view.rs`, `crates/gearbox_agent/src/task_manager.rs`, `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`
- `forbidden_files`: ACP schema、非 Gear UI、旧 continuation fallback。
- `inputs`: GBX-001 repair diff、NativeAgentConnection stop/restart、TaskManager command methods。
- `steps`: 添加 GPUI integration test：同 workspace 的 session A/B 启动、停止 A、确认 B 未变、重启 A 后只读取 A；断言 accepted/rejected/queued outcome 的 task/epoch/queue 或 unavailable reason 被 Gear UI/API 消费。
- `acceptance`: 测试经过 `NativeAgentConnection` 而非直接 StateStore；无 outcome 回退到猜测 current task；默认 Zed 行为未变。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo test -p agent gear -- --nocapture`; `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent task_manager -- --nocapture`
- `rollback`: 回滚 Gear-only bridge 与测试；保留 session-key runtime repair。

### GBX-002-005 FINAL-REVIEW

- `role`: review
- `objective`: 确认每一条 completion claim 都有真实 dispatch/reviewer artifact 支撑。
- `allowed_files`: GBX-002 允许文件、测试、当前 run reports、`crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`
- `forbidden_files`: 新功能、范围外重构、force pass、报告掩盖失败。
- `inputs`: 所有 GBX-002 evidence、git diff、fake backend capture、reviewer artifacts、ACP integration test。
- `steps`: 复核 scope；抽查 supported variant、unsupported variant、denied tool、inconclusive reviewer、A/B continuation 五条路径；确认 upstream sync note 覆盖所有 shared file；写结果和 harness retrospective。
- `acceptance`: 没有 metadata-only applied/evidence 字段；失败路径有结构化原因；完整验证通过；报告不把未来工作写成 completed。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo fmt --all -- --check`; `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent -- --nocapture`; `cd /home/donald/文档/github/zed && cargo test -p agent gear -- --nocapture`; `cd /home/donald/文档/github/zed && ./script/clippy -p gearbox_agent`; `cd /home/donald/文档/github/zed && cargo check -p gearbox_agent -p agent -p agent_ui`; `cd /home/donald/文档/github/zed && git diff --check`
- `rollback`: 仅允许按工单提交逆序回滚；基础设施失败与业务失败分开记录。

## Validation Plan

- provider：捕获最终 command/native dispatch 参数；unsupported/unavailable 不能静默降级。
- tools：拒绝发生在 executor invocation 前，并有零调用断言。
- review：每个必需 reviewer dimension 都有真实 execution ID、route、artifact path、verdict；重复或缺失 hard fail。
- continuation/outcome：走 ACP/Gear bridge 的 A/B 隔离、restart、accepted/rejected/queued 回执。
- final：GBX-002-005 的六条命令不得用筛选测试替代。

## Rollback Plan

每个 build 工单独立提交。capability contract、executor gate、review evidence、ACP integration test 不跨工单混合。任一工单发现需要改 ACP schema 或不可证明的 native provider API 时，停止该工单并保留失败测试/证据，不增加兼容 fallback。

## Acceptance Criteria

- variant 只在真实 backend dispatch 参数发生改变时标记 applied；其余情况明确 rejected/unavailable。
- disabled tool 无法进入可拦截 executor；无法拦截的 backend 明确拒绝 host-enforced claim。
- Review Gate 不能由 synthetic ID 或空 artifact 通过。
- ACP session A/B continuation 及 outcome context 有端到端回归覆盖。
- Gear-only shared changes 已写入 `UPSTREAM_SYNC_NOTES.md`，默认 Zed 行为保持不变。
- 完整验证通过且 `git diff --check` 无错误。

## Completion Reports

若存在活动 run，写入 `.machine/sessions/<session-id>/runs/run-N/reports/result-summary.md` 与 `.machine/sessions/<session-id>/runs/run-N/reports/harness-retrospective.md`。否则写入 `.dogfood/reports/gbx-002-provider-executor-and-independent-review-summary.md` 及对应 retrospective。两者必须列出工单、提交、命令、artifact、失败、force-advance 决定、剩余风险；retrospective 必须有 `## Harness 问题与使用感受`。

## Risks / Stop Conditions

- 原生 Zed provider 选择需要 ACP schema 或非 Gear Agent 行为时停止。
- external command worker 无法拦截单个工具调用时，不得声称 host enforcement，记录边界并转为 capability 设计。
- reviewer 需要新凭据、网络服务或未授权模型时停止。
- 发现旧 continuation 数据需要迁移时停止，另写迁移计划。
- no-match、runner 或环境错误必须标为基础设施失败，不得 force pass。
