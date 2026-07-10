# GBX-001 Result Summary

**Plan:** Session-Scoped Continuation & Provider Policy Repair
**Status:** Partially complete; follow-up required
**Created:** 2026-07-10
**Completed:** 2026-07-10

## Work Orders

| WO | Status | Commits |
|---|---|---|
| WO-001: Root Cause Repro | Partial | `1978e2073a` — evidence was limited to StateStore tests |
| WO-002: Session-Scoped Continuation | Repaired after review | ACP session ID now reaches `RunOptions`; legacy singleton APIs removed |
| WO-003: Provider Policy Dispatch | Partial | command dispatch receives variant/policy environment; no provider capability contract |
| WO-004: Outcome & Review Evidence | Partial | typed outcomes exist; reviewer evidence remains synthetic |
| WO-005: Final Review | Invalidated | original final review accepted known unmet acceptance criteria |

## Validation Commands

| Command | Result |
|---|---|
| Original five commands | Passed on 2026-07-10, but did not exercise the missing runtime contracts |

## Remaining Risks

1. **Continuation mapping is repaired but needs GUI integration coverage**: the runtime now receives the ACP session ID and the singleton APIs are removed; add a GPUI Stop/Restart A/B test before closing the follow-up.
2. **Provider capability contract is still missing**: command workers now receive `GEARBOX_WORKER_MODEL_VARIANT` and `GEARBOX_WORKER_TOOL_POLICY`, while native Zed variant dispatch fails closed until a real provider-selection API is wired.
3. **Review evidence remains synthetic**: `ReviewerEvidence.execution_id` is not a reviewer worker execution ID and its artifact path is empty.
4. **Tool policy remains boundary-limited**: the adapter now rejects a denied required category capability before worker dispatch, but it cannot yet intercept individual tools inside external command workers.

The remaining provider and independent-review work is tracked by `GBX-002`.
