# OMO vs Gear 细粒度对比 - Parts 8-11: 剩余模块

> 文件：fallback-retry-handler.ts、stop-continuation-guard/hook.ts

---

## Part 8: Fallback Retry Handler

**文件：** `packages/omo-opencode/src/features/background-agent/fallback-retry-handler.ts`

### 关键模式：no-op fallback 检测

```typescript
// fallback-retry-handler.ts:114-125
const isNoOpFallback =
    candidateProviderID.toLowerCase() === task.model.providerID.toLowerCase() &&
    canonicalizeModelID(candidateModelID) === canonicalizeModelID(task.model.modelID);
```

当 fallback 候选与当前 model 相同时跳过。`canonicalizeModelID` 将 `.` 替换为 `-` 并转小写，所以 `gpt-4.1` 和 `gpt-4-1` 被视为同一模型。

**Gear 现状：** `WorkerSequence` 不会检测 no-op route（同一个 worker_kind 的连续 route 不会自动跳过）。

### 关键模式：team-mode guard

```typescript
// fallback-retry-handler.ts:198-206
if (task.teamRunId && !task.onSessionCreated) {
    throw new TeamModeFallbackError(...);
}
```

**Gear 相关性：** 无（Gear 没有 team mode）。但 principle 可借鉴——在关键状态不完整时抛出结构化错误，而不是静默进入不可恢复状态。

---

## Part 9: Stop-Continuation-Guard

**文件：** `packages/omo-opencode/src/hooks/stop-continuation-guard/hook.ts`

### 关键模式：级联取消 + skipNotification

```typescript
// hook.ts:44-69
const cancellableTasks = backgroundManager
    .getAllDescendantTasks(sessionID)    // 递归找到所有子/孙 task
    .filter(t => t.status === "running" || t.status === "pending");

void Promise.allSettled(
    cancellableTasks.map(task => backgroundManager.cancelTask(task.id, {
        source: "stop-continuation",
        skipNotification: true,  // 防止通知风暴
        abortSession: task.status === "running",
    }))
);
```

**关键设计：**
- `getAllDescendantTasks()` — 递归遍历所有层级
- `skipNotification: true` — 父 session 不需要为每个子 task 收到独立通知
- `abortSession: task.status === "running"` — 只有 running 的 task 需要 abort session；pending 的只需从 queue 移除

**Gear 现状：** `cancel()` 只取消 `current_task`。没有递归后代查找，没有 `skipNotification` 概念。

---

## Parts 10-11: Delegate-Core + Config Schema

这两个模块对 Gear 的直接影响较低：

| 模块 | 内容 | Gear 相关性 |
|------|------|------------|
| `delegate-core/model-selection.ts` | 多层模型回退解析算法 | 低 — Gear 的 model selection 在 coordinator_brief 层 |
| `delegate-core/retry-patterns.ts` | 错误模式检测（run_in_background 缺失、unknown category 等） | 低 — Gear 不通过 opencode tool call 调用 |
| `omo-config-core/schema/task.ts` | Task settings schema（concurrency、depth、residency、TTL） | 中 — Gear 通过 `WorkerConfig` 实现相似配置 |
| `omo-config-core/schema/category.ts` | Category 配置 schema | 低 — Gear 的 CategoryRouter 有内置 policy |

**唯一值得借鉴的细节：** `omo-config-core/schema/task.ts` 中的 `residency_max_children`（默认 8）、`ttl_ms`（默认 24h）、`default_concurrency`（默认 5）等默认值可以作为 Gear 未来配置的参考基线。
