# GXB-001 Harness Retrospective

## Harness 问题与使用感受

### 权限提示
- 无异常权限提示。CodeGraph 工具在本仓库索引良好，查询快速准确。

### 路由异常
- 无路由异常。Work Order 均按预期串行执行。

### 验证摩擦
- **clippy 严格模式**：`./script/clippy` 使用 `--release --all-targets --all-features -- --deny warnings`，比普通 `cargo clippy` 严格得多。需要在最终验证阶段特别处理 deprecated method 的 `#[allow(deprecated)]` 注解和 test code 中的 redundant clone。
- **WO-004 的 breakage 影响**：修改 `ActionOutcome`/`SendOutcome`/`SteerOutcome` 枚举变体的结构（添加 context 字段）波及了大量 match 表达式和 test 断言。应优选向后兼容的方式（如添加新变体而非修改现有变体结构）以减少 ripple effect。

### 陈旧 Artifact
- 无陈旧 artifact 问题。

### 执行行为
- Subagent（deep category）对复杂、多步骤的工单处理质量良好，但 clippy 和 fmt 的细节需要最终 review 时手动修补。建议在 prompt 中明确要求 subagent 运行 `cargo fmt` 和 `cargo clippy` 并修复结果。

### 改进建议
1. 在 `deep` category 的 prompt 中增加一条强制要求：`"After implementation, run 'cargo fmt --all' and fix all clippy warnings with --deny warnings level"`。
2. 对于涉及共享 enum 变体修改的工单，提示 subagent 使用 `#[non_exhaustive]` 或向后兼容的方式。
3. WO-001 的复现测试在 WO-002 中需要重写（从证明 bug 变为验证修复），这是预期的计划内开销。
