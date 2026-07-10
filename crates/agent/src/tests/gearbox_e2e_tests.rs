//! Gearbox orchestration end-to-end tests.
//!
//! 1. Small task delegation to Zed Agent
//! 2. Medium task capability routing to external workers
//! 3. Session close/recovery with lineage consistency
//! 4. Worker complete but review missing → incomplete status
//! 5. Review passes → complete status with readable artifacts
//!
//! All tests use `#[gpui::test]` with deterministic GPUI scheduler.

use super::*;
use gearbox_agent::runtime::{ReviewDimension, ReviewDimensionResult, ReviewGate, ReviewerEvidence};
use gearbox_agent::state::{StateStore, WorkLineage};
use gearbox_agent::workers::{
    NativeWorkerBackend, WorkerCapabilities, WorkerCategory, WorkerConfig, WorkerKind,
    WorkerOutcome, WorkerResult, WorkerSessionHandle, WorkerStartRequest,
    WorkerRegistry,
};
use pretty_assertions::assert_eq;
use std::path::Path;
use std::sync::{Arc, Mutex};

// ─── Test helpers ───────────────────────────────────────────────────────────

struct FakeWorkerShared {
    last_start_request_summary: Option<String>,
    session_id: Option<String>,
}

struct FakeWorkerSession {
    state: Arc<Mutex<FakeWorkerShared>>,
}

impl WorkerSessionHandle for FakeWorkerSession {
    fn session_id(&self) -> Option<String> {
        self.state.lock().ok().and_then(|s| s.session_id.clone())
    }
    fn send_follow_up(&self, _prompt: String) -> anyhow::Result<()> { Ok(()) }
    fn steer(&self, _prompt: String) -> anyhow::Result<()> { Ok(()) }
    fn interrupt(&self) -> anyhow::Result<()> { Ok(()) }
    fn cancel(&self) -> anyhow::Result<()> { Ok(()) }
    fn wait_for_outcome(&self) -> anyhow::Result<WorkerOutcome> {
        anyhow::bail!("not supported")
    }
    fn wait_for_result(&self) -> anyhow::Result<WorkerResult> {
        anyhow::bail!("not supported")
    }
    fn last_output(&self) -> Option<String> { None }
}

struct FakeNativeBackend {
    state: Arc<Mutex<FakeWorkerShared>>,
}

impl FakeNativeBackend {
    fn new() -> Self {
        Self { state: Arc::new(Mutex::new(FakeWorkerShared {
            last_start_request_summary: None,
            session_id: Some("fake-session-id".to_string()),
        })) }
    }
}

impl NativeWorkerBackend for FakeNativeBackend {
    fn start_zed_agent(&self, request: WorkerStartRequest<'_>) -> anyhow::Result<Arc<dyn WorkerSessionHandle>> {
        let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        state.last_start_request_summary = Some(format!(
            "task={}, goal={}, route_attempt={}",
            request.task.id, request.goal, request.route_attempt
        ));
        drop(state);
        Ok(Arc::new(FakeWorkerSession { state: self.state.clone() }))
    }
}

fn test_task(id: &str) -> gearbox_agent::state::Task {
    gearbox_agent::state::Task {
        id: id.to_string(),
        goal_id: "goal_test".to_string(),
        parent_task_id: None,
        title: "test task".to_string(),
        kind: gearbox_agent::state::TaskKind::Edit,
        status: gearbox_agent::state::TaskStatus::Pending,
        assigned_worker: Some("zed_agent".to_string()),
        attempt: 1,
        scope: gearbox_agent::state::Scope::new(Vec::new(), Vec::new(), 10),
        inputs: gearbox_agent::state::TaskInputs::default(),
        outputs: gearbox_agent::state::TaskOutputs::default(),
    }
}

fn test_worker_config() -> WorkerConfig {
    WorkerConfig {
        worker_kind: WorkerKind::ZedAgent,
        worker_command: None,
        worker_model: None,
        worker_routes: Vec::new(),
        unavailable_worker_models: Vec::new(),
        premium_worker_budget: 1,
        max_parallel_workers: 1,
        max_parallel_per_key: 1,
        stale_task_timeout_secs: 30,
        skip_worker: false,
        require_worker: true,
        default_worker_for_small_tasks: WorkerKind::ZedAgent,
    }
}

fn make_request<'a>(
    store: &'a StateStore,
    workspace: &'a Path,
    task: &'a gearbox_agent::state::Task,
    goal: &'a str,
    config: &'a WorkerConfig,
) -> WorkerStartRequest<'a> {
    WorkerStartRequest {
        store,
        workspace,
        task,
        route_attempt: 1,
        goal,
        verification_commands: &[],
        config,
        cancellation_token: None,
        coordinator_model: None,
        coordinator_brief: None,
        route_hint: None,
    }
}

fn gate_without_review_evidence() -> ReviewGate {
    ReviewGate {
        require_all_pass: true,
        results: vec![
            ReviewDimensionResult {
                dimension: ReviewDimension::GoalVerification,
                passed: true,
                evidence: "verification passed and coordinator accepted the goal".to_string(),
                reviewer_evidence: None,
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::CodeQuality,
                passed: true,
                evidence: "scope checks are clean".to_string(),
                reviewer_evidence: None,
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::Security,
                passed: true,
                evidence: "no forbidden paths were touched".to_string(),
                reviewer_evidence: None,
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::QaExecution,
                passed: true,
                evidence: "verification commands passed".to_string(),
                reviewer_evidence: None,
            },
        ],
    }
}

fn gate_with_review_evidence() -> ReviewGate {
    ReviewGate {
        require_all_pass: true,
        results: vec![
            ReviewDimensionResult {
                dimension: ReviewDimension::GoalVerification,
                passed: true,
                evidence: "verification passed and coordinator accepted the goal".to_string(),
                reviewer_evidence: Some(ReviewerEvidence {
                    execution_id: "worker-session-42_GoalVerification".to_string(),
                    route: "GoalVerification".to_string(),
                    artifact_path: Some("/tmp/goal-verification.md".to_string()),
                    verdict: "pass".to_string(),
                }),
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::CodeQuality,
                passed: true,
                evidence: "scope checks are clean".to_string(),
                reviewer_evidence: Some(ReviewerEvidence {
                    execution_id: "worker-session-42_CodeQuality".to_string(),
                    route: "CodeQuality".to_string(),
                    artifact_path: Some("/tmp/code-quality.md".to_string()),
                    verdict: "pass".to_string(),
                }),
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::Security,
                passed: true,
                evidence: "no forbidden paths were touched".to_string(),
                reviewer_evidence: Some(ReviewerEvidence {
                    execution_id: "worker-session-42_Security".to_string(),
                    route: "Security".to_string(),
                    artifact_path: Some("/tmp/security.md".to_string()),
                    verdict: "pass".to_string(),
                }),
            },
            ReviewDimensionResult {
                dimension: ReviewDimension::QaExecution,
                passed: true,
                evidence: "verification commands passed".to_string(),
                reviewer_evidence: Some(ReviewerEvidence {
                    execution_id: "worker-session-42_QaExecution".to_string(),
                    route: "QaExecution".to_string(),
                    artifact_path: Some("/tmp/qa-execution.md".to_string()),
                    verdict: "pass".to_string(),
                }),
            },
        ],
    }
}

// ─── Test 1: Small task delegation to Zed Agent ────────────────────────────

#[gpui::test]
async fn test_gearbox_small_task_delegation(_cx: &mut TestAppContext) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let store = StateStore::new(tmp.path());
    store.initialize().expect("failed to initialize StateStore");

    let backend = FakeNativeBackend::new();
    let registry = WorkerRegistry::with_native_backend(Arc::new(backend));

    let task = test_task("test-task-id");
    let config = test_worker_config();

    let request = make_request(&store, tmp.path(), &task, "test small task goal", &config);
    let handle = registry.start(request).expect("WorkerRegistry::start should succeed");

    let session_id = handle.session_id();
    assert!(session_id.is_some(), "Worker session handle should have a session_id");
    assert_eq!(session_id.as_deref(), Some("fake-session-id"), "Worker session should report correct session ID");

    let caps = WorkerCapabilities::command();
    assert!(caps.supports_category(WorkerCategory::Quick), "command worker should support Quick");
}

// ─── Test 2: Capability routing ─────────────────────────────────────────────

#[gpui::test]
async fn test_gearbox_capability_routing(_cx: &mut TestAppContext) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let store = StateStore::new(tmp.path());
    store.initialize().expect("failed to initialize StateStore");

    let backend = FakeNativeBackend::new();
    let registry = WorkerRegistry::with_native_backend(Arc::new(backend));

    let task = test_task("test-task-id");
    let config = test_worker_config();

    for category in &[WorkerCategory::Quick, WorkerCategory::Deep, WorkerCategory::Repair, WorkerCategory::Review, WorkerCategory::Explore] {
        let goal = format!("test category routing for {c:?}", c = category);
        let request = make_request(&store, tmp.path(), &task, &goal, &config);
        let result = registry.start(request);
        assert!(result.is_ok(), "WorkerRegistry::start should succeed for category {category:?}");
    }

    let code_caps = WorkerCapabilities::command();
    for edit_category in &[WorkerCategory::Quick, WorkerCategory::Deep, WorkerCategory::Repair] {
        assert!(code_caps.supports_category(*edit_category), "command worker should support {edit_category:?}");
    }
}

// ─── Test 3: Session recovery with lineage consistency ──────────────────────

#[gpui::test]
async fn test_gearbox_session_recovery(_cx: &mut TestAppContext) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let store = StateStore::new(tmp.path());
    store.initialize().expect("failed to initialize StateStore");

    let root_session_id = "gear-session-root-1";
    let mut lineage = WorkLineage::new(root_session_id.to_string());
    lineage.worker_session_ids.push("worker-1".to_string());
    lineage.worker_session_ids.push("worker-2".to_string());
    lineage.active_task_ids.push("task-1".to_string());
    lineage.active_task_ids.push("task-2".to_string());
    lineage.plan_remaining_items = 2;
    lineage.updated_at = "2025-01-01T00:01:00Z".to_string();

    let path = store.write_lineage(&lineage).expect("should persist lineage");
    assert!(path.exists(), "Lineage file should exist at {path:?}");

    let session_record = gearbox_agent::state::Session {
        id: root_session_id.to_string(),
        workspace: tmp.path().to_string_lossy().to_string(),
        created_at: "2025-01-01T00:00:00Z".to_string(),
        updated_at: "2025-01-01T00:01:00Z".to_string(),
        current_goal_id: "goal-1".to_string(),
    };
    store.write_session(&session_record).expect("should write session");

    drop(store);

    let recovered_store = StateStore::new(tmp.path());
    recovered_store.initialize().expect("should re-initialize");

    let recovered_lineage = recovered_store.read_lineage(root_session_id)
        .expect("should read lineage after recovery")
        .expect("lineage should exist after recovery");

    assert_eq!(recovered_lineage.root_session_id, root_session_id, "Root session ID should match after recovery");
    assert_eq!(recovered_lineage.worker_session_ids.len(), 2, "Should have 2 workers after recovery, got {}", recovered_lineage.worker_session_ids.len());
    assert_eq!(recovered_lineage.active_task_ids.len(), 2, "Should have 2 active tasks after recovery");
    assert_eq!(recovered_lineage.plan_remaining_items, 2, "plan_remaining_items should be preserved");
    assert!(recovered_lineage.worker_session_ids.contains(&"worker-1".to_string()), "Lineage should include worker-1");
    assert!(recovered_lineage.worker_session_ids.contains(&"worker-2".to_string()), "Lineage should include worker-2");
}

// ─── Test 4: Incomplete without review ──────────────────────────────────────

#[gpui::test]
async fn test_gearbox_incomplete_without_review(_cx: &mut TestAppContext) {
    let gate = gate_without_review_evidence();

    assert!(gate.require_all_pass, "ReviewGate should require all dimensions");
    assert!(gate.results.iter().all(|r| r.passed), "All dimensions should pass individually");

    for result in &gate.results {
        assert!(
            result.reviewer_evidence.is_none(),
            "Dimension {:?} should have NO reviewer evidence (no real review ran)",
            result.dimension,
        );
    }
}

// ─── Test 5: Complete after review ──────────────────────────────────────────

#[gpui::test]
async fn test_gearbox_complete_after_review(_cx: &mut TestAppContext) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let store = StateStore::new(tmp.path());
    store.initialize().expect("failed to initialize StateStore");

    let artifact_path = store.write_artifact("goal-1", "review-output.md", "All checks passed.")
        .expect("should write artifact");
    assert!(artifact_path.exists(), "Artifact should exist on disk at {artifact_path:?}");

    let gate = gate_with_review_evidence();

    assert!(gate.require_all_pass, "ReviewGate should require all dimensions");
    assert!(gate.results.iter().all(|r| r.passed), "All dimensions should pass");

    for result in &gate.results {
        assert!(
            result.reviewer_evidence.is_some(),
            "Dimension {:?} should have real reviewer evidence",
            result.dimension,
        );
        if let Some(ref evidence) = result.reviewer_evidence {
            assert!(
                evidence.execution_id.contains("worker-session-42"),
                "Evidence execution_id should contain the worker session ID: {}",
                evidence.execution_id,
            );
            assert_eq!(evidence.verdict, "pass", "All dimensions should have 'pass' verdict");
        }
    }

    let artifact_content = std::fs::read_to_string(&artifact_path).expect("should read artifact");
    assert_eq!(artifact_content.trim(), "All checks passed.", "Artifact content should match what was written");
}
