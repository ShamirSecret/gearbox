# Phase 07：Category、fallback、provider/model policy

## 目标

把 route policy 从 worker-kind MVP 升级为 provider/model-aware policy：category 解析有类型化失败、fallback 链按配置长度重试、跳过 no-op/unreachable，模型元数据安全扫描，category prompt_append 可注入。

## 主要文件

- `crates/gearbox_agent/src/runtime.rs`
- `crates/gearbox_agent/src/task_manager.rs`
- `crates/gearbox_agent/src/workers.rs`
- `crates/gearbox_agent/src/cli.rs`
- `crates/agent/src/agent.rs`

## 具体工单

1. 扩展 `CategoryResolution`：
   - `prompt_append: Option<String>`
   - `available_categories: Vec<String>`
   - `nearest_fallback: Option<FallbackRoute>`
   - `fallback_chain: Vec<FallbackRoute>`
   - `tools: WorkerToolPolicy`
2. 新增 `CategoryResolutionResult`：
   - `Resolved`
   - `Disabled`
   - `NotFound`
   - `ModelUnavailable`
3. 错误结果必须包含：
   - requested category
   - available categories
   - attempted provider/model
   - nearest fallback
4. 新增 secret-like model field scan：
   - 字段名标准化：去掉非字母数字，转小写。
   - 阻止字段名包含 `apikey`、`authorization`、`bearertoken`、`clientsecret`、`password`、`privatekey`、`secret`、`secretkey`、`token` 等。
   - 扫描发生在写 ledger/worker packet/review prompt 前。
5. fallback chain 改造：
   - chain item 是 `{ providers: Vec<String>, model: String, variant: Option<String> }`。
   - `has_more_fallbacks` 使用 chain 长度，不再只用 `MAX_SAME_FAILURE_RETRIES=2`。
   - budget 仍可单独限制 premium call。
6. no-op fallback 检测：
   - provider id case-insensitive 比较。
   - model id canonicalize：`.` 和 `-` 等价，转小写。
   - fallback 候选与当前 provider/model 相同则跳过并写 artifact。
7. unreachable provider skip：
   - provider registry snapshot 中不可用的 provider/model 不启动 worker。
   - 写 `skipped_unreachable_provider` attempt。
8. provider/model transform artifact：
   - 记录 previous provider/model/session。
   - 记录 failed provider/model。
   - 记录 next provider/model。
   - 输入 ReviewEngine。
9. category prompt_append：
   - 支持静态 append。
   - 支持根据 model/provider 动态 append。
   - 用户配置 append 与内置 append 拼接。
10. tool policy：
   - worker 默认 `question: false`。
   - worker 默认不能递归创建 Gear task，除非显式允许。
   - write worker 与 review/explore worker 使用不同工具策略。

## 测试

1. `category_not_found_lists_available_categories`
2. `disabled_category_returns_disabled_result`
3. `model_unavailable_returns_nearest_fallback`
4. `secret_like_model_field_is_rejected_before_packet_write`
5. `fallback_skips_noop_provider_model`
6. `fallback_attempt_count_follows_chain_length`
7. `prompt_append_combines_builtin_and_user_append`
8. `worker_tool_policy_disables_question_by_default`

## 验收

- route 失败不再只有泛化 `Unavailable`。
- fallback artifact 能说明跳过了哪些 provider/model 以及为什么。
- category prompt 能根据任务类别改变 worker 指令。
- 不会把疑似 API key 的 model metadata 写进 artifacts 或 review prompt。
