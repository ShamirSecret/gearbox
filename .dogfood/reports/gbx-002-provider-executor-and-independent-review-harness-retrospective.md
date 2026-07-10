# GXB-002 Harness Retrospective

## Harness 问题与使用感受

### 权限提示
- 无异常权限提示。

### 路由异常
- 无路由异常。所有 Work Order 串行执行成功。

### 验证摩擦
- **clippy 严格模式** 再次出现 redundant clone 问题（在 test code 和 lib code 中均有）。需要在每次 subagent 提交后手动修复。
- **GBX-002-004 的 GPUI 测试要求** 难以满足：agent 的 GPUI 测试需要完整的 `TestAppContext`、`FakeLanguageModel`、`Project` 等基础设施，测试模块本身有 8000+ 行。实际的 session_id 流通过程已在 gearbox_agent 层的 `Orchestrator::run()` 测试中验证，但"完整 ACP bridge 测试"仍缺失。

### 执行行为
- Subagent 对 GBX-002-003（ReviewGate 改造）处理良好，正确修改了 `from_inputs()` 签名并更新了所有 7 个调用点。
- GBX-001 遗留的 dirty diff（deprecated 方法删除、check_tool_allowed 实现、session_id 参数）被成功继承到 GBX-002-001 的提交中。

### 改进建议
1. 对于 GPUI 集成测试的要求，应在计划阶段明确评估测试基础设施成本。如果 agent 测试需要 8000+ 行 setup，则应允许在 gearbox_agent 层做等效验证。
2. 继续使用 category 特定的环境变量（`GEARBOX_GEAR_CATEGORY_*`）来避免并发测试的 env 污染。
