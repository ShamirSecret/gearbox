# Gearbox 会话级续跑与 Provider 策略修复计划

~~~yaml
plan_id: GBX-001
artifact_kind: repair_plan
status: draft
created_at: "2026-07-10 12:12:16 +0800"
slug: session-scoped-continuation-and-provider-policy
canonical_path: .dogfood/20260710-121216-gbx-001-session-scoped-continuation-and-provider-policy.md
target_id: null
related_ids: []
~~~

## Summary

在当前 TaskManager 控制面、terminal revive、Review Gate 和 Stop Continuation 已可用的基础上，修复仍会破坏多会话正确性的续跑状态存储边界，并把 OMO 参考中的 model variant、tool policy、typed outcome 和独立审查从描述性元数据推进到可验证的执行约束。

所有工单串行执行。不得用 prompt 文案、报告字段或双读兼容层冒充运行时约束。

## Evidence / Current State

- `crates/gearbox_agent/src/state.rs::continuation_state_path` 固定返回工作区级 `.gearbox-agent/continuation/state.json`，但文件内容带 `session_id`。同一工作区的两个会话会争用一个权威槽位。
- `crates/agent/src/agent.rs::stop_gear_continuation` 和 `restart_gear_continuation` 通过该单文件状态控制续跑，ACP session id 与 Gear runtime session id 的映射尚无独立持久化契约。
- `crates/gearbox_agent/src/workers.rs` 已计算 `WorkerToolPolicy` 和 model variant，但 variant 主要进入 packet/artifact；尚未证明 native/command provider 的实际请求参数发生变化。
- `crates/gearbox_agent/src/runtime.rs` 已有硬 Review Gate，但多个审查维度仍可能来自同一份合并证据，不能证明独立 reviewer 路径真的执行。
- `SendOutcome`、`SteerOutcome` 和 `ActionOutcome` 已保留结果类别与 reason，但仍未稳定携带 `task_id`、`run_epoch`、`queue_position` 等调用方需要的上下文。
- 本计划创建前，`cargo test -p gearbox_agent -- --nocapture` 通过 181 个测试。

## Root Cause Analysis

`observed_symptoms`:

- Stop/Restart Continuation 的持久化路径是工作区单例，而控制 API 是按 session 调用。
- model variant 和 tool policy 可在 artifact 中观察，但没有端到端测试证明 provider dispatch 使用了这些值。
- UI/API 仍需额外查询 snapshot 才能把命令结果关联到 epoch 和队列位置。

`likely_root_cause`:

- 状态存储最初按单活跃 Gear run 设计，session identity 后加入结构体但没有进入路径键。
- worker packet 被当作策略真相源，provider adapter 没有成为可验证的策略执行边界。
- outcome enum 先解决了 bool 语义丢失，尚未统一成自描述命令回执。

`code_paths_to_verify`:

- `crates/gearbox_agent/src/state.rs::{continuation_state_path,read_continuation_state,write_continuation_state,clear_continuation_state}`
- `crates/agent/src/agent.rs::{stop_gear_continuation,restart_gear_continuation}`
- `crates/gearbox_agent/src/runtime.rs::Orchestrator::run`
- `crates/gearbox_agent/src/workers.rs::{tool_policy_for_category,build_worker_packet}` 及 native/command worker dispatch
- `crates/gearbox_agent/src/task_manager.rs::{SendOutcome,SteerOutcome,ActionOutcome}`

`failure_chain`:

1. 会话 A 写入 Running 或 Stopped。
2. 同工作区会话 B 覆盖同一 `state.json`。
3. A 的 stop/restart 读取到 B 的身份或状态，操作被拒绝或影响错误的后续续跑判断。
4. packet 中的策略字段即使正确，provider dispatch 若忽略它，artifact 与真实执行能力仍会分叉。

`why_previous_fix_failed`:

前一轮优先补齐了控制入口、生命周期事件和 GUI 操作，没有改变 `StateStore` 的单槽路径模型，也没有把 provider 请求作为策略验收点。

`validation_gap`:

缺少同一工作区双 session 的交错写入测试、ACP/Gear session 映射测试、provider 请求捕获测试，以及 outcome 自包含字段断言。

`uncertainty`:

ACP session id 是否可直接作为长期稳定目录键尚未证明。`GBX-001-001` 必须先确认映射和生命周期；若必须修改 ACP 协议 schema，立即停止并报告，不得扩大共享协议改动。

## Non-Goals

- 不重命名 Zed 内部类型、action、协议、keymap context 或 fixture。
- 不引入旧路径双读、自动迁移或静默 fallback。
- 不重写 GoalLoop、TaskManager 或 ACP 协议。
- 不处理与 Gear runtime 无关的汉化、品牌和编辑器功能。
- 不以 prompt 声明替代 tool/provider dispatch 强制。

## Chief Prompt

按 `GBX-001-001` 至 `GBX-001-005` 串行执行。先用失败测试证明会话单槽冲突和身份映射，再做最小状态布局修复；随后让 model variant 与 tool policy 在 provider dispatch 层可观察、可拒绝；最后收口 typed outcome 和独立审查证据。每个工单必须提交 `WORK_ORDER_ROUTING_BRIEF` 与 `WORK_ORDER_EVIDENCE`，其中包含 `scope_check`、`forbidden_check`、`acceptance_check`、`validation_check`。遇到 Stop Conditions 时停止，不得自行修改 ACP schema 或增加兼容层。

## Preflight

1. 工作目录必须是 `/home/donald/文档/github/zed`。
2. 阅读 `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`、`docs/gearbox-omo-reference.md` 和本计划。
3. 记录 `git status --short --branch`，不得覆盖用户已有修改。
4. 确认 `.codegraph/` 存在时优先用 CodeGraph 定位调用链。
5. 若存在活动 ACM run，只把 `.machine/sessions/<session-id>/runs/run-N/` 视为当前运行事实。

## Work Orders

### GBX-001-001 ROOT-CAUSE-REPRO

- `role`: plan
- `objective`: 用测试证明或否定同工作区多会话争用 continuation 单槽，并确定 ACP session id 到 Gear runtime session id 的稳定映射边界。
- `allowed_files`: `crates/gearbox_agent/src/state.rs`, `crates/gearbox_agent/src/runtime.rs`, `crates/agent/src/agent.rs`, `crates/gearbox_agent/tests/**`
- `forbidden_files`: ACP schema、非 Gear agent 实现、打包资源、上游内部标识。
- `inputs`: 本计划、`docs/gearbox-omo-reference.md`、上述 continuation 函数及现有测试。
- `steps`: 添加同一临时 workspace 下 session A/B 交错写停读启测试；跟踪 GUI API session id 到 runtime state 的构造路径；记录可作为路径键的稳定 id；旧行为必须使至少一个断言失败，若无法安全运行旧行为则给出静态调用链证据。
- `acceptance`: 证据明确指出覆盖发生的路径和错误 session；映射结论有代码引用；没有生产代码修复混入本工单。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent continuation -- --nocapture`
- `rollback`: 只回滚本工单新增的复现测试和临时诊断，不改已有用户修改。

### GBX-001-002 SESSION-SCOPED-CONTINUATION

- `role`: build
- `objective`: 恢复不变量：一个 session 的 continuation 状态不能覆盖、停止或重启同工作区的另一个 session。
- `allowed_files`: `crates/gearbox_agent/src/state.rs`, `crates/gearbox_agent/src/runtime.rs`, `crates/agent/src/agent.rs`, `crates/agent_ui/src/conversation_view/thread_view.rs`, `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`
- `forbidden_files`: ACP schema、通用 Agent 取消语义、旧状态迁移层、无关 UI。
- `inputs`: `GBX-001-001` 证据与测试。
- `steps`: 选择已证明的稳定 session key；将 continuation 状态改为会话级权威路径或等价的显式索引；所有 read/write/clear 必须要求 session；让 Stop/Restart 只操作目标 session；更新生命周期事件和 Gear-only UI；共享源码变化同步到 `UPSTREAM_SYNC_NOTES.md`。
- `acceptance`: A/B 交错测试通过；停止 A 不改变 B；重启 A 不读取 B；不存在旧单槽静默 fallback；重启进程后行为一致。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent continuation -- --nocapture`
- `rollback`: 回滚会话路径和调用方为同一提交；若发现已有数据必须迁移，停止并另写迁移计划。

### GBX-001-003 PROVIDER-POLICY-DISPATCH

- `role`: build
- `objective`: 恢复不变量：选中的 model variant 和 tool policy 必须改变真实 provider 请求或在 dispatch 前明确拒绝。
- `allowed_files`: `crates/gearbox_agent/src/workers.rs`, `crates/gearbox_agent/src/runtime.rs`, `crates/gearbox_agent/src/cli.rs`, 相关 Gear-only 测试。
- `forbidden_files`: provider 公共协议、第三方 provider 实现、prompt-only 绕过、无关模型设置 UI。
- `inputs`: `docs/gearbox-omo-reference.md` 中 model variant/tool policy 章节、worker packet 和 native/command dispatch 代码。
- `steps`: 为 native 与 command 路径确定单一 adapter 边界；把 variant 转为 provider 支持的参数或返回结构化 unsupported；在工具调用 dispatch 前强制 allow/deny；用 fake provider 捕获最终请求和拒绝事件；artifact 记录最终应用值而非期望值。
- `acceptance`: 至少一个 variant 改变捕获到的 provider 请求；unsupported 不静默降级；禁用工具无法到达执行器；review/explore 策略测试覆盖差异。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent workers -- --nocapture`
- `rollback`: adapter 与测试作为一个原子变更回滚；不得保留只写 artifact 的半成品字段。

### GBX-001-004 OUTCOME-AND-REVIEW-EVIDENCE

- `role`: build
- `objective`: 让 task 命令回执自描述，并证明 Review Gate 的各维度来自实际执行的独立 reviewer 证据。
- `allowed_files`: `crates/gearbox_agent/src/task_manager.rs`, `crates/gearbox_agent/src/runtime.rs`, `crates/agent/src/agent.rs`, `crates/agent_ui/src/conversation_view/thread_view.rs`, `crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`
- `forbidden_files`: 第二套 TaskManager 状态机、ACP schema、非 Gear UI 行为。
- `inputs`: 当前 typed outcomes、task command events、ReviewGate 判定和 reviewer worker artifact。
- `steps`: 为 accepted/rejected outcome 统一加入 `task_id`、`run_epoch` 和适用时的 `queue_position`；Agent/UI 不再通过猜测 current task 补字段；把每个 gate dimension 绑定到 reviewer execution id、route、artifact path 和 verdict；缺失或同源冒充独立证据时 hard fail；保持审计写失败不反向改变命令结果。
- `acceptance`: UI/API 单凭 outcome 可关联目标 epoch；queued 回执有稳定位置或明确 unavailable reason；Review Gate 可追溯到独立 reviewer artifact；同一证据重复填充多个独立维度的测试失败。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent -- --nocapture`
- `rollback`: outcome 枚举与所有调用方必须整体回滚；不得临时保留 bool 与 typed 双路径。

### GBX-001-005 FINAL-REVIEW

- `role`: review
- `objective`: 审核根因闭环、共享边界和 OMO 功能对齐，不以测试总绿替代行为证据。
- `allowed_files`: 本计划允许文件、测试、当前 run reports、`crates/gearbox_settings/UPSTREAM_SYNC_NOTES.md`
- `forbidden_files`: 新功能实现、范围外重构、force pass。
- `inputs`: 所有工单 evidence、git diff、provider 捕获 artifact、continuation 双 session 测试、reviewer artifact。
- `steps`: 检查 `scope_check`/`forbidden_check`；确认没有旧路径 fallback；确认 shared changes 都有 sync note；运行完整验证；抽查 A/B session、unsupported variant、denied tool、inconclusive review 四条失败路径；写完成报告和 harness retrospective。
- `acceptance`: 所有失败路径产生结构化原因且不污染其他 session；完整验证通过；报告列出提交、命令、artifact 和剩余风险。
- `validation_commands`: `cd /home/donald/文档/github/zed && cargo fmt --all -- --check`; `cd /home/donald/文档/github/zed && cargo test -p gearbox_agent -- --nocapture`; `cd /home/donald/文档/github/zed && ./script/clippy -p gearbox_agent`; `cd /home/donald/文档/github/zed && cargo check -p gearbox_agent -p agent -p agent_ui`; `cd /home/donald/文档/github/zed && git diff --check`
- `rollback`: 只允许按工单提交逆序回滚；验证基础设施失败与代码失败必须分开记录。

## Validation Plan

- continuation：同 workspace 双 session 隔离、停止/重启、进程恢复和错误 session 拒绝。
- provider：捕获最终请求参数、unsupported variant、禁用工具 dispatch 拒绝。
- outcome：每种 accepted/rejected 类别的 task/epoch/reason/queue context。
- review：独立 execution id、artifact path、verdict 和 hard gate 失败路径。
- 全量：执行 `GBX-001-005` 的五条命令，不允许以筛选测试替代。

## Rollback Plan

每个 build 工单独立提交。状态布局、provider adapter、outcome schema 不跨工单混合提交。若某工单失败，保留复现测试与 evidence，逆序回滚该工单生产代码；不得删除用户原有 dirty diff，不得用旧路径 fallback 掩盖失败。

## Acceptance Criteria

- 同工作区任意两个 Gear session 的 continuation 权威状态互不覆盖。
- model variant 和 tool policy 对真实 dispatch 有可观测影响。
- task command outcome 自包含目标、epoch、结果和必要队列上下文。
- Review Gate 的独立维度可追溯到真实 reviewer execution artifact。
- Gear-only shared changes 已记录在 `UPSTREAM_SYNC_NOTES.md`，Zed 默认行为不变。
- 完整验证全部通过且没有 `git diff --check` 错误。

## Completion Reports

若有活动 run，必须写：

- `.machine/sessions/<session-id>/runs/run-N/reports/result-summary.md`
- `.machine/sessions/<session-id>/runs/run-N/reports/harness-retrospective.md`

`result-summary.md` 必须列出工单、提交、验证命令、失败、force-advance 决定和剩余风险。`harness-retrospective.md` 必须包含 `## Harness 问题与使用感受`，记录权限提示、路由异常、验证摩擦、陈旧 artifact、执行行为和改进建议。无活动 run 时，在 `.dogfood/reports/gbx-001-session-scoped-continuation-and-provider-policy-summary.md` 与对应 `-harness-retrospective.md` 写同等内容。

## Risks / Stop Conditions

- 无法证明 ACP session id 与 Gear runtime session id 的稳定映射时停止。
- 修复需要改变 ACP schema 或非 Gear agent 会话语义时停止。
- 发现已有 continuation 数据必须迁移时停止，另写显式迁移计划。
- provider 不支持 variant 且没有明确 capability contract 时停止，不得静默降级。
- tool policy 只能靠 prompt 约束、无法在 dispatch 层执行时停止并报告边界。
- 独立 reviewer 需要新增外部凭据、网络服务或未授权模型时停止。
- validation runner/no-match/环境失败必须标记为基础设施失败，不得 force pass。
