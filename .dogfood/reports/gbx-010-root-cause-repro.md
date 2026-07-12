# GBX-010 Root-Cause Repro Report

## Summary

This report documents the baseline freeze and root-cause reproduction for Objective production gaps identified in GBX-009. Four repro tests were added to `crates/gearbox_agent/src/runtime.rs` with test-only observation/fault seams. All tests compile and pass (329/329 in `gearbox_agent`).

**Key finding: GBX-010-003 IS needed.** The crash gap between Goal Settled and ObjectiveGraph commit is confirmed by repro test 2. The existing runtime does NOT correctly reuse a settled Goal when the process restarts after this gap.

## Baseline Freeze — Write Order of Key Functions

### `run_objective_controller()` (runtime.rs:2771)

1. Acquire `objective_lease` for (`objective_id`, `root_session_id`)
2. Read or create `ObjectiveGraph`
3. `reconcile_objective_frontier()` — recover child from Continue event if graph has no active frontier
4. If graph is terminal, release lease and return
5. If graph is empty, create root `GoalGraphNode` and `graph.add_root_node()`
6. **Loop:**
   a. `graph.active_node()` → get active frontier
   b. `Orchestrator::run_single_goal_with_phase_runtime(...)` — runs the active Goal to completion
   c. **CRASH GAP #1:** Goal has settled, but `graph` is NOT yet updated
   d. `graph.update_active_node(...)` — write Goal outcome into graph
   e. `store.write_objective_graph(&graph)` — persist updated graph
   f. If Goal status != Complete, set terminal and break
   g. If no `strategist_receipt`, set terminal Complete and break
   h. If Strategist decision = Complete/NeedsUser/Stop, set terminal and break
   i. If Strategist decision = Continue:
      - `store.append_objective_event(..., StrategistContinueAccepted, ...)`
      - Check policy gates (max_epochs, budget, cooldown)
      - **CRASH GAP #2:** Continue event written, but child not yet attached
      - `graph.attach_child(child_node)`
      - `store.write_objective_graph(&graph)`
      - `store.append_objective_event(..., GoalAttached, ...)`
      - `store.append_objective_event(..., FrontierAdvanced, ...)`
7. Final `store.write_objective_graph(&graph)`
8. Release `objective_lease`
9. Return `ObjectiveRunOutcome`

### `run_single_goal_with_phase_runtime()` (runtime.rs:929)

1. Create `StateStore`, `Goal`, `Session`
2. Acquire `goal_run_lease` for (`goal_id`, `epoch_id`, `session_id`)
3. `store.abort_incomplete_goal_epoch(...)` — abort previous epoch if lease was released without terminal event
4. Append `GoalEpochEventKind::Started`
5. Build and write `PlanGraph`
6. Worker dispatch loop (iterations):
   a. `task_manager.start(WorkerStartRequest {...})` — dispatch worker
   b. Wait for worker completion
   c. Settle budget reservation
   d. Update lineage and plan node runs
7. Build final verification wave
8. Write final report artifact
9. `run_strategist_next_goal(...)` — produce `StrategistNextGoalReceipt` if hook present
10. Update `Goal` status, write goal, write continuation state
11. Append `GoalEpochEventKind::Settled`
12. **Release `goal_run_lease`**
13. Return `RunOutcome` (includes `strategist_receipt`)

**Settled event + lease release order:** The Goal epoch `Settled` event is written BEFORE the `goal_run_lease` is released. This means a crashed process can observe a Settled epoch but an unreleased lease (if crash happens between 11 and 12), or a Settled epoch with a released lease (if crash happens after 12).

### `reconcile_objective_frontier()` (runtime.rs:3184)

1. Read objective events; append `Started` if empty
2. Replay `GoalAttached` events for all existing graph nodes
3. If graph is terminal, append terminal event if missing, then return
4. If graph has active frontier or is empty, return early
5. Find last `StrategistContinueAccepted` event
6. Validate parent goal binding (status Complete, epoch match, receipt hash match, final wave present)
7. `graph.attach_child(child_node)`
8. `store.write_objective_graph(graph)`
9. Append `GoalAttached` and `FrontierAdvanced` events

**Recovery limitation:** `reconcile_objective_frontier` only recovers a child from a `StrategistContinueAccepted` event. It does NOT handle the case where the Goal settled but the graph was never updated (CRASH GAP #1).

### `objective_budget_totals()` (runtime.rs:3498)

1. Iterate all `graph.nodes`
2. For each node, `store.read_goal_budget_ledger(&node.goal_id)`
3. Sum reservations that are NOT `Released`
4. Return `(calls, tokens, cost, unknown_calls)`

**Aggregation behavior:** This is a post-hoc aggregation from per-goal settled ledgers. There is NO objective-wide reservation or settle ledger. Budget gating happens at child dispatch time by calling `objective_budget_totals` and comparing against `policy.max_calls/max_tokens/max_cost_micros/max_unknown_usage_calls`.

### `GearOpenCodePhaseRunner` entry points (agent.rs:3511)

- `run()` — core phase execution via `broker_factory.execute_worker_phase()`
- `fold_intent()` — IntentFold phase
- `plan()` — Planner phase
- `critique()` — PlanCritic phase
- `revise()` — PlanRevision phase
- `strategize()` — StrategistNextGoal phase

All entry points require a `PhaseRouteDecision` with `PhaseBackend::Worker(WorkerKind::OpencodeSession)`.

### `PhaseRouteTable::opencode_only()` (phase_routing.rs:281)

- Maps all phase profiles to `WorkerKind::OpencodeSession` backend
- Uses `PhaseModelBinding::BackendDeclared(model)` for Planner, Executor, Reviewer
- Sets `PhaseRouteSource::BuiltIn`
- Requires validated `OpenCodeModelProfiles`

### CLI objective branch (cli.rs:169)

```rust
if command.objective {
    Orchestrator::run_objective_with_phase_runtime(
        options,
        PhaseRuntime::legacy(),  // <-- NOT production
        ObjectivePolicy { ... },
    )?
}
```

`PhaseRuntime::legacy()` has:
- `routes: PhaseRouteTable::legacy_defaults()` (DirectModel/LegacyCategory, NOT OpencodeSession)
- `broker: None`
- `broker_factory: None`

## Repro Tests and Failure Points

### Test 1: `objective_production_gap_repro`

**What it proves:** CLI `PhaseRuntime::legacy()` cannot construct a GUI-equivalent production `PhaseRuntime`.

**Assertions:**
- `cli_runtime.broker_factory.is_none()`
- `cli_runtime.routes.hash() == PhaseRouteTable::legacy_defaults().hash()`
- `cli_routes_hash != gui_routes_hash` (opencode_only)
- `gui_runtime.broker_factory.is_some()`

**Failure point:** The CLI objective path uses `PhaseRuntime::legacy()`, which lacks `broker_factory` and uses `legacy_defaults` routes. A production OpenCode objective requires `PhaseRouteTable::opencode_only(...)` and a `PhaseBrokerFactory`.

### Test 2: `objective_crash_after_goal_settle_repro`

**What it proves:** After Goal Settled → ObjectiveGraph commit, a process crash leaves the objective graph in an inconsistent state. On resume, the controller re-enters the settled Goal and fails.

**Method:**
1. Install test seam with `intercept_settled_to_graph_commit` returning `true`
2. Run objective controller with Continue strategist verdict
3. Seam intercepts after `run_single_goal_with_phase_runtime` returns but before `graph.update_active_node`
4. Simulated crash: `run_objective_controller` returns error
5. Assert graph still shows active node as non-terminal (Running/Planning)
6. Assert goal epoch has `Settled` event
7. Call `reconcile_objective_frontier` — it returns early because `graph.active_goal_id.is_some()`
8. Resume controller with same options
9. Assert resumed run fails with epoch state conflict ("idempotency key conflicts with an existing event")

**Failure point:** The existing runtime does NOT correctly handle a settled Goal on resume. `reconcile_objective_frontier` has no code path to update an active node from a settled goal epoch. The controller tries to re-run the same goal_id/epoch_id, which produces duplicate epoch events and fails.

**GBX-010-003 disposition:** NEEDED. The crash gap exists and is not handled.

### Test 3: `objective_budget_reservation_repro`

**What it proves:** No objective-wide reservation ledger exists before child dispatch. Budget aggregation is purely post-hoc from per-goal ledgers.

**Method:**
1. Run objective with auto_continue and Continue strategist verdict
2. Count child attaches via `on_child_attach` seam
3. After run completes, check `store.objectives_dir().join("{objective_id}.reservations.json")`
4. Assert file does NOT exist
5. Assert `objective_budget_totals` returns `calls > 0` (proving aggregation from goal ledgers works)

**Failure point:** There is no durable objective-wide reservation/settle ledger. If a child dispatch needs to be gated on objective-wide budget, the gate reads aggregated settled usage from past goals. There is no reservation before the child dispatch that could be rolled back if the child fails.

### Test 4: `objective_cli_profile_assertion`

**What it proves:** `--objective` + OpenCode profile requires Gear-owned production phase factory; `PhaseRuntime::legacy()` is NOT production.

**Method:**
1. Assert `PhaseRuntime::legacy()` lacks `broker_factory` and `broker`
2. Assert legacy routes differ from `PhaseRouteTable::opencode_only(...)`
3. Construct production `PhaseRuntime` with `broker_factory: Some(...)`
4. Assert production runtime has `broker_factory`

**Failure point:** The CLI objective branch hardcodes `PhaseRuntime::legacy()`. For an OpenCode profile, this is not a production-equivalent runtime.

## Worker Dispatch Count and Artifact Paths

- **Worker dispatch count:** Tracked via `test_seams::increment_worker_dispatch()` in `run_single_goal_with_phase_runtime`. Each successful `task_manager.start(...)` increments the counter.
- **Per-goal artifacts:** `.gearbox-agent/goals/{goal_id}/` — spec.md, plan.md, final-report.md, final-verification-wave.json, strategist-next-goal-receipt.json, budget-ledger.json
- **Per-epoch artifacts:** `.gearbox-agent/goals/{goal_id}/epochs/{epoch_id}.jsonl` — Started, BudgetReserved, BudgetSettled, PhaseCompleted, NextGoalSelected, Settled
- **Objective artifacts:** `.gearbox-agent/objectives/{objective_id}.graph.json`, `{objective_id}.jsonl`, `{objective_id}.lease.json`
- **Missing artifact:** `.gearbox-agent/objectives/{objective_id}.reservations.json` — does NOT exist (confirmed by test 3)

## Additional Findings

1. **Test seam design:** Thread-local `test_seams` module in `runtime.rs` provides non-interfering per-test observation. Uses `RefCell<Option<ObjectiveControllerTestSeam>>` scoped to each test thread.

2. **Write order observation:** The seam captures:
   - `goal_settled` → `goal_lease_released` → `objective_graph_commit` (normal flow)
   - `intercept_settled_to_graph_commit` can simulate a crash between goal settlement and graph commit

3. **Reconcile limitation:** `reconcile_objective_frontier` only handles child recovery from `StrategistContinueAccepted` events. It does NOT handle:
   - Active node update from a settled goal epoch
   - Goal re-run after a settled epoch
   - Duplicate epoch idempotency conflicts

4. **Budget aggregation timing:** `objective_budget_totals` is called at child dispatch time (in the Continue branch of `run_objective_controller`). It aggregates settled usage from ALL graph nodes. There is no pre-dispatch reservation that would prevent overshoot in a crash window.

## Conclusion

- **GBX-010-003 is needed:** The crash gap between Goal Settled and ObjectiveGraph commit is real and unhandled.
- **CLI production gap is real:** `PhaseRuntime::legacy()` is not equivalent to the GUI production path.
- **No objective-wide reservation ledger exists:** Budget gating relies on post-hoc aggregation.
- **All four repro tests pass** and serve as regression guards for these findings.
