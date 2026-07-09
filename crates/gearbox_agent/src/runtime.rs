use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as _, Result, bail};
use serde_json::json;

use crate::languages::{LanguageDetection, detect_with_request};
use crate::product;
use crate::state::{
    Budget, CoordinatorModel, Event, EventKind, Goal, GoalStatus, Scope, Session, StateStore, Task,
    TaskInputs, TaskKind, TaskOutputs, TaskStatus, event, id_timestamp, timestamp,
};
use crate::task_manager::{
    SharedTaskManager, TaskFailureKind, TaskManager, TaskManagerControl, TaskManagerTickLoop,
    TaskRecord,
};
use crate::tools::{
    CancellationToken, DiffSnapshot, ShellCommandResult, check_scope, git_snapshot,
    run_shell_command_with_env_and_cancellation,
};
use crate::workers::{
    WorkerCategory, WorkerConfig, WorkerKind, WorkerOutcome, WorkerStartRequest, WorkerStatus,
};

pub type EventSink = Arc<dyn Fn(&Event) + Send + Sync + 'static>;
pub type CoordinatorReviewHook = Arc<
    dyn Fn(CoordinatorReviewInput) -> Result<Option<CoordinatorReview>> + Send + Sync + 'static,
>;
pub const DEFAULT_MAX_ITERATIONS: usize = 5;

#[derive(Clone)]
pub struct RunOptions {
    pub request: String,
    pub workspace: PathBuf,
    pub verification_commands: Vec<String>,
    pub worker: WorkerConfig,
    pub allowed_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub max_files_changed: usize,
    pub install_dependencies: bool,
    pub event_sink: Option<EventSink>,
    pub cancellation_token: Option<CancellationToken>,
    pub max_iterations: usize,
    pub coordinator_model: Option<CoordinatorModel>,
    pub coordinator_brief: Option<String>,
    pub coordinator_review_hook: Option<CoordinatorReviewHook>,
    pub task_manager_control: Option<TaskManagerControl>,
    pub task_manager: Option<SharedTaskManager>,
}

#[derive(Clone, Debug)]
pub struct CoordinatorReviewInput {
    pub goal_id: String,
    pub task_id: String,
    pub iteration: usize,
    pub max_iterations: usize,
    pub request: String,
    pub worker_kind: String,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub route_reason: String,
    pub worker_attempt: usize,
    pub worker_attempt_count: usize,
    pub worker_failure_kind: Option<String>,
    pub worker_retry_reason: Option<String>,
    pub worker_fallback_summary: String,
    pub worker_status: String,
    pub worker_summary: String,
    pub worker_outcome_summary: String,
    pub worker_commands_run: Vec<String>,
    pub worker_known_failures: Vec<String>,
    pub worker_outcome_path: Option<String>,
    pub budget_summary: String,
    pub verification_passed: bool,
    pub verification_summary: String,
    pub scope_summary: String,
    pub diff_summary: String,
}

#[derive(Clone, Debug)]
pub struct CoordinatorReview {
    pub goal_satisfied: Option<bool>,
    pub summary: String,
    pub repair_request: Option<String>,
    pub route_hint: Option<String>,
    pub stop_reason: Option<String>,
    pub raw_response: String,
}

#[derive(Clone, Debug)]
pub struct RunOutcome {
    pub goal_id: String,
    pub session_id: String,
    pub status: GoalStatus,
    pub artifacts_root: PathBuf,
    pub final_report_path: PathBuf,
    pub events_path: PathBuf,
}

pub struct Orchestrator;

impl Orchestrator {
    pub fn run(options: RunOptions) -> Result<RunOutcome> {
        if options.request.trim().is_empty() {
            bail!("prompt cannot be empty");
        }
        check_run_cancelled(options.cancellation_token.as_ref())?;

        let workspace = options.workspace.canonicalize().with_context(|| {
            format!(
                "failed to resolve workspace {}",
                options.workspace.display()
            )
        })?;
        if !workspace.is_dir() {
            bail!("workspace is not a directory: {}", workspace.display());
        }

        let store = StateStore::new(&workspace);
        store.initialize()?;
        check_run_cancelled(options.cancellation_token.as_ref())?;

        let id_suffix = id_timestamp();
        let session_id = format!("ses_{id_suffix}");
        let goal_id = format!("goal_{id_suffix}");
        let scope = Scope::new(
            options.allowed_paths.clone(),
            options.forbidden_paths.clone(),
            options.max_files_changed,
        );
        let max_iterations = options.max_iterations.max(1);
        let detection = detect_with_request(
            &workspace,
            &options.verification_commands,
            options.install_dependencies,
            &options.request,
        )?;
        let now = timestamp();

        let mut goal = Goal {
            id: goal_id.clone(),
            title: title_from_request(&options.request),
            status: GoalStatus::Planning,
            workspace: workspace.to_string_lossy().to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
            request: options.request.clone(),
            product_type: detection.product_type.clone(),
            language_profile: detection.profile.as_str().to_string(),
            success_criteria: success_criteria(&detection),
            budget: Budget::default(),
            current_task_id: None,
            coordinator_model: options.coordinator_model.clone(),
            coordinator_brief: options.coordinator_brief.clone(),
            summary: String::new(),
        };

        let session = Session {
            id: session_id.clone(),
            workspace: workspace.to_string_lossy().to_string(),
            created_at: now.clone(),
            updated_at: now,
            current_goal_id: goal_id.clone(),
        };

        store.write_session(&session)?;
        store.write_goal(&goal)?;
        append_event(
            &store,
            &options.event_sink,
            event(
                &session_id,
                Some(&goal_id),
                None,
                EventKind::GoalCreated,
                format!("Created {}", goal.id),
                json!({
                    "workspace": workspace.to_string_lossy(),
                    "language_profile": detection.profile.as_str(),
                    "evidence": &detection.evidence,
                    "coordinator_model": &goal.coordinator_model,
                    "coordinator_brief": &goal.coordinator_brief,
                }),
            ),
        )?;

        let mut tasks = initial_tasks(
            &goal_id,
            &scope,
            options.worker.selected_route(1).worker_kind,
        );
        store.write_tasks(&goal_id, &tasks)?;

        let spec_path =
            store.write_artifact(&goal_id, "spec.md", &product::spec(&goal, &detection))?;
        complete_task(&mut tasks, "task_001", |task| {
            task.outputs.summary = "Spec artifact created.".to_string();
            task.outputs
                .evidence
                .push(spec_path.to_string_lossy().to_string());
        });
        append_event(
            &store,
            &options.event_sink,
            event(
                &session_id,
                Some(&goal_id),
                Some("task_001"),
                EventKind::SpecCreated,
                "Spec artifact created",
                json!({ "path": spec_path.to_string_lossy() }),
            ),
        )?;

        set_task_inputs(&mut tasks, spec_path.to_string_lossy().to_string(), None);
        let plan_path = store.write_artifact(
            &goal_id,
            "plan.md",
            &product::plan(&goal, &tasks, &detection),
        )?;
        complete_task(&mut tasks, "task_002", |task| {
            task.outputs.summary = "Plan artifact created.".to_string();
            task.outputs
                .evidence
                .push(plan_path.to_string_lossy().to_string());
        });
        set_task_inputs(
            &mut tasks,
            spec_path.to_string_lossy().to_string(),
            Some(plan_path.to_string_lossy().to_string()),
        );
        store.write_tasks(&goal_id, &tasks)?;
        append_event(
            &store,
            &options.event_sink,
            event(
                &session_id,
                Some(&goal_id),
                Some("task_002"),
                EventKind::PlanCreated,
                "Plan artifact created",
                json!({ "path": plan_path.to_string_lossy() }),
            ),
        )?;

        let mut before_diff = git_snapshot(&workspace)?;
        let mut after_diff = before_diff.clone();
        let mut scope_check = check_scope(&after_diff, &scope);
        let mut worker_result = None;
        let mut verification_results = Vec::new();
        let mut last_verification_path = None;
        let mut final_evaluation = None;
        let mut last_coordinator_review: Option<CoordinatorReview> = None;
        let mut next_route_hint_override: Option<String> = None;
        let mut provider_unknown_streak = 0usize;
        let mut repeated_failure_streak = 0usize;
        let mut last_failure_kind: Option<TaskFailureKind> = None;
        let task_manager = options.task_manager.clone().unwrap_or_else(|| {
            options
                .task_manager_control
                .clone()
                .map(TaskManager::with_control)
                .unwrap_or_else(TaskManager::new)
                .into_shared()
        });
        task_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
            .recover_orphaned_records(&store)?;
        task_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
            .apply_worker_config(&options.worker);
        let task_manager_tick_loop =
            TaskManagerTickLoop::start(task_manager.clone(), Duration::from_millis(50));

        for iteration in 1..=max_iterations {
            check_run_cancelled(options.cancellation_token.as_ref())?;
            let worker_route_hint = next_route_hint_override.as_deref().or_else(|| {
                last_coordinator_review
                    .as_ref()
                    .and_then(|review| review.route_hint.as_deref())
            });
            let selected_route = options.worker.selected_route_for_hint(1, worker_route_hint);
            let worker_task_id = if iteration == 1 {
                "task_003".to_string()
            } else {
                let verification_path = last_verification_path
                    .as_deref()
                    .context("missing verification artifact for repair iteration")?;
                let repair_task_id = add_repair_task(
                    &mut tasks,
                    &goal_id,
                    &scope,
                    iteration,
                    verification_path,
                    selected_route.worker_kind,
                );
                store.write_tasks(&goal_id, &tasks)?;
                append_event(
                    &store,
                    &options.event_sink,
                    event(
                        &session_id,
                        Some(&goal_id),
                        Some(&repair_task_id),
                        EventKind::RepairStarted,
                        format!("Repair iteration {iteration} started"),
                        json!({
                            "iteration": iteration,
                            "verification_path": verification_path.to_string_lossy(),
                            "route_hint": worker_route_hint,
                            "worker_kind": selected_route.worker_kind.as_str(),
                            "worker_model": selected_route.worker_model,
                            "worker_category": selected_route.category.as_str(),
                            "route_reason": &selected_route.route_reason,
                        }),
                    ),
                )?;
                repair_task_id
            };

            start_task(&mut tasks, &worker_task_id);
            goal.status = GoalStatus::Running;
            goal.current_task_id = Some(worker_task_id.clone());
            goal.updated_at = timestamp();
            store.write_goal(&goal)?;
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&worker_task_id),
                    EventKind::WorkerStarted,
                    if iteration == 1 {
                        "Prepared implementation worker packet".to_string()
                    } else {
                        "Prepared repair worker packet".to_string()
                    },
                    json!({
                        "iteration": iteration,
                        "before": &before_diff,
                        "current": &after_diff,
                        "route_hint": worker_route_hint,
                        "worker_kind": selected_route.worker_kind.as_str(),
                        "worker_model": selected_route.worker_model,
                        "worker_category": selected_route.category.as_str(),
                        "route_reason": &selected_route.route_reason,
                    }),
                ),
            )?;

            let worker_task = tasks
                .iter()
                .find(|task| task.id == worker_task_id)
                .context("missing worker task")?
                .clone();
            let worker_request = if iteration == 1 {
                options.request.clone()
            } else {
                repair_request(
                    &options.request,
                    iteration,
                    last_verification_path.as_deref(),
                    last_coordinator_review.as_ref(),
                )
            };
            let managed_worker_task_id = task_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
                .start(WorkerStartRequest {
                    store: &store,
                    workspace: &workspace,
                    task: &worker_task,
                    route_attempt: worker_task.attempt,
                    goal: &worker_request,
                    verification_commands: &detection.verification_commands,
                    config: &options.worker,
                    cancellation_token: options.cancellation_token.clone(),
                    coordinator_model: goal.coordinator_model.as_ref(),
                    coordinator_brief: goal.coordinator_brief.as_deref(),
                    route_hint: worker_route_hint,
                })?;
            if options
                .cancellation_token
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                task_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
                    .cancel_task(&managed_worker_task_id)?;
                check_run_cancelled(options.cancellation_token.as_ref())?;
            }
            let managed_worker_run = loop {
                check_run_cancelled(options.cancellation_token.as_ref())?;
                if let Some(run) = task_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
                    .try_wait_for(&managed_worker_task_id)?
                {
                    break run;
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            let worker_session_id = managed_worker_run.record.session_id.clone();
            let worker_task_record = managed_worker_run.record;
            let iteration_worker_outcome = managed_worker_run.outcome;
            let iteration_worker_result = managed_worker_run.result;

            update_worker_task(
                &mut tasks,
                &worker_task_id,
                &iteration_worker_result.status,
                &iteration_worker_result.summary,
            );
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&worker_task_id),
                    match iteration_worker_result.status {
                        WorkerStatus::Succeeded => EventKind::WorkerFinished,
                        WorkerStatus::Skipped => EventKind::WorkerWaiting,
                        WorkerStatus::Failed => EventKind::WorkerFailed,
                    },
                    iteration_worker_result.summary.clone(),
                    json!({
                        "iteration": iteration,
                        "status": iteration_worker_result.status.as_str(),
                        "session_id": worker_session_id,
                        "route_hint": worker_route_hint,
                        "worker_kind": selected_route.worker_kind.as_str(),
                        "worker_model": selected_route.worker_model,
                        "worker_category": selected_route.category.as_str(),
                        "route_reason": &selected_route.route_reason,
                        "packet_path": iteration_worker_result.packet_path.to_string_lossy(),
                        "prompt_path": iteration_worker_result.prompt_path.to_string_lossy(),
                        "outcome_path": iteration_worker_result.outcome_path.to_string_lossy(),
                        "task_record_path": store.worker_dir(&worker_task_id).join("task-record.json").to_string_lossy(),
                        "managed_status": format!("{:?}", worker_task_record.status),
                        "failure_kind": worker_task_record.failure_kind.as_ref().map(|kind| format!("{kind:?}")),
                        "retry_reason": &worker_task_record.retry_reason,
                        "commands_run": &iteration_worker_outcome.commands_run,
                        "known_failures": &iteration_worker_outcome.known_failures,
                    }),
                ),
            )?;
            worker_result = Some(iteration_worker_result);

            after_diff = git_snapshot(&workspace)?;
            scope_check = check_scope(&after_diff, &scope);
            check_run_cancelled(options.cancellation_token.as_ref())?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&worker_task_id),
                    EventKind::DiffDetected,
                    "Diff snapshot captured",
                    json!({
                        "iteration": iteration,
                        "before": &before_diff,
                        "after": &after_diff,
                        "scope_check": &scope_check,
                    }),
                ),
            )?;

            start_task(&mut tasks, "task_004");
            goal.status = GoalStatus::Verifying;
            goal.current_task_id = Some("task_004".to_string());
            goal.updated_at = timestamp();
            store.write_goal(&goal)?;
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some("task_004"),
                    EventKind::VerificationStarted,
                    "Verification started",
                    json!({
                        "iteration": iteration,
                        "commands": detection.verification_commands,
                    }),
                ),
            )?;

            verification_results = run_verification(
                &workspace,
                &detection.verification_commands,
                options.cancellation_token.as_ref(),
            )?;
            let verification_artifact = if iteration == 1 {
                "verification.md".to_string()
            } else {
                format!("verification-iteration-{iteration}.md")
            };
            let verification_path = store.write_artifact(
                &goal_id,
                &verification_artifact,
                &product::verification(&verification_results),
            )?;

            let verification_passed = !verification_results.is_empty()
                && verification_results.iter().all(|result| result.success);
            update_verification_task(
                &mut tasks,
                &verification_results,
                verification_path.to_string_lossy().to_string(),
                verification_passed,
            );

            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some("task_004"),
                    if verification_passed {
                        EventKind::VerificationPassed
                    } else {
                        EventKind::VerificationFailed
                    },
                    if verification_passed {
                        "Verification passed".to_string()
                    } else {
                        "Verification failed or was unavailable".to_string()
                    },
                    json!({
                        "iteration": iteration,
                        "verification_path": verification_path.to_string_lossy(),
                    }),
                ),
            )?;

            last_verification_path = Some(verification_path.clone());
            let coordinator_review = run_coordinator_review(
                &store,
                &options.event_sink,
                &options.coordinator_review_hook,
                &session_id,
                &goal_id,
                iteration,
                max_iterations,
                &options.request,
                &worker_task_id,
                &worker_task_record,
                worker_result
                    .as_ref()
                    .context("missing worker result for coordinator review")?,
                &iteration_worker_outcome,
                verification_passed,
                &verification_results,
                &scope_check,
                &before_diff,
                &after_diff,
            )?;
            last_coordinator_review = coordinator_review.clone();
            let coordinator_review = coordinator_review.as_ref();
            if verification_passed
                && coordinator_review.is_some_and(|review| {
                    review.goal_satisfied.is_none()
                        && review
                            .stop_reason
                            .as_deref()
                            .and_then(normalized_stop_reason)
                            .is_none()
                })
            {
                provider_unknown_streak += 1;
            } else {
                provider_unknown_streak = 0;
            }
            if let Some(current_failure_kind) = worker_task_record.failure_kind.clone() {
                if last_failure_kind.as_ref() == Some(&current_failure_kind) {
                    repeated_failure_streak += 1;
                } else {
                    repeated_failure_streak = 1;
                }
                last_failure_kind = Some(current_failure_kind);
            } else {
                repeated_failure_streak = 0;
                last_failure_kind = None;
            }

            let evaluation = evaluate_goal(
                verification_passed,
                &worker_result
                    .as_ref()
                    .context("missing worker result for goal evaluation")?
                    .status,
                selected_route.category,
                selected_route.require_worker,
                worker_task_record.failure_kind.as_ref(),
                worker_task_record.retry_reason.as_deref(),
                &scope_check,
                coordinator_review,
                provider_unknown_streak,
                repeated_failure_streak,
                iteration,
                max_iterations,
            );
            next_route_hint_override = evaluation.route_hint_override.clone();
            let review_path = store.write_artifact(
                &goal_id,
                &format!("goal-review-iteration-{iteration}.md"),
                &goal_review_artifact(
                    iteration,
                    max_iterations,
                    &evaluation,
                    worker_result
                        .as_ref()
                        .context("missing worker result for goal review")?,
                    selected_route.category,
                    selected_route.worker_model,
                    &selected_route.route_reason,
                    worker_task_record.failure_kind.as_ref(),
                    worker_task_record.retry_reason.as_deref(),
                    &iteration_worker_outcome,
                    &scope_check,
                    &verification_results,
                    coordinator_review,
                ),
            )?;
            add_review_task(
                &mut tasks,
                &goal_id,
                &scope,
                iteration,
                &review_path,
                &evaluation.summary,
            );
            store.write_tasks(&goal_id, &tasks)?;
            append_event(
                &store,
                &options.event_sink,
                event(
                    &session_id,
                    Some(&goal_id),
                    Some(&review_task_id(iteration)),
                    EventKind::TaskStarted,
                    "Goal check completed",
                    json!({
                        "iteration": iteration,
                        "status": evaluation.status.as_str(),
                        "should_continue": evaluation.should_continue,
                        "review_path": review_path.to_string_lossy(),
                    }),
                ),
            )?;

            let should_continue = evaluation.should_continue;
            final_evaluation = Some(evaluation);
            if !should_continue {
                break;
            }

            before_diff = after_diff.clone();
        }

        let final_evaluation = final_evaluation.context("Gear loop did not evaluate the goal")?;
        let worker_result = worker_result.context("Gear loop did not produce a worker result")?;
        goal.status = final_evaluation.status;
        goal.current_task_id = None;
        goal.updated_at = timestamp();
        goal.summary = final_evaluation.summary;

        let final_report = product::final_report(
            &goal,
            &tasks,
            &worker_result,
            &after_diff,
            &scope_check,
            &verification_results,
        );
        let final_report_path = store.write_artifact(&goal_id, "final-report.md", &final_report)?;
        complete_task(&mut tasks, "task_006", |task| {
            task.outputs.summary = "Final report artifact created.".to_string();
            task.outputs
                .evidence
                .push(final_report_path.to_string_lossy().to_string());
        });
        store.write_goal(&goal)?;
        store.write_tasks(&goal_id, &tasks)?;

        let final_event_kind = match goal.status {
            GoalStatus::Complete => EventKind::GoalCompleted,
            GoalStatus::Limited => EventKind::GoalLimited,
            _ => EventKind::GoalBlocked,
        };
        append_event(
            &store,
            &options.event_sink,
            event(
                &session_id,
                Some(&goal_id),
                None,
                final_event_kind,
                goal.summary.clone(),
                json!({
                    "status": goal.status.as_str(),
                    "final_report_path": final_report_path.to_string_lossy(),
                }),
            ),
        )?;

        if let Some(error) = task_manager_tick_loop.last_error()? {
            bail!("{error}");
        }
        task_manager_tick_loop.stop()?;

        let status = goal.status.clone();
        let artifacts_root = store.artifact_dir(&goal.id);
        Ok(RunOutcome {
            goal_id,
            session_id: session_id.clone(),
            status,
            artifacts_root,
            final_report_path,
            events_path: store.events_path(&session_id),
        })
    }
}

fn title_from_request(request: &str) -> String {
    let trimmed = request.trim();
    let mut title = String::new();
    for character in trimmed.chars().take(60) {
        title.push(character);
    }
    if title.is_empty() {
        "Gear goal".to_string()
    } else {
        title
    }
}

fn success_criteria(detection: &LanguageDetection) -> Vec<String> {
    let mut criteria = vec![
        "Artifacts include spec, plan, verification, and final report.".to_string(),
        "Diff is checked against the task scope.".to_string(),
        "Known failures are recorded instead of hidden.".to_string(),
    ];
    match detection.profile {
        crate::languages::LanguageProfile::TypeScript => {
            criteria.push("TypeScript project verification is recorded.".to_string());
        }
        crate::languages::LanguageProfile::Python => {
            criteria.push("Python project verification is recorded.".to_string());
        }
        crate::languages::LanguageProfile::Rust => {
            criteria.push("Rust project verification is recorded.".to_string());
        }
        crate::languages::LanguageProfile::Unknown => {
            criteria.push(
                "A verification command is supplied or the goal asks for user input.".to_string(),
            );
        }
    }
    criteria
}

fn initial_tasks(goal_id: &str, scope: &Scope, worker_kind: WorkerKind) -> Vec<Task> {
    [
        ("task_001", "Generate minimal spec", TaskKind::Spec, None),
        ("task_002", "Generate executable plan", TaskKind::Plan, None),
        (
            "task_003",
            "Dispatch bounded implementation packet",
            TaskKind::Edit,
            Some(worker_kind.as_str().to_string()),
        ),
        (
            "task_004",
            "Run Gear-owned verification",
            TaskKind::Verify,
            None,
        ),
        (
            "task_006",
            "Write delivery report",
            TaskKind::Document,
            None,
        ),
    ]
    .into_iter()
    .map(|(id, title, kind, assigned_worker)| Task {
        id: id.to_string(),
        goal_id: goal_id.to_string(),
        title: title.to_string(),
        kind,
        status: TaskStatus::Pending,
        assigned_worker,
        attempt: 1,
        scope: scope.clone(),
        inputs: TaskInputs::default(),
        outputs: TaskOutputs::default(),
    })
    .collect()
}

fn start_task(tasks: &mut [Task], task_id: &str) {
    if let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) {
        task.status = TaskStatus::Running;
    }
}

fn complete_task(tasks: &mut [Task], task_id: &str, update: impl FnOnce(&mut Task)) {
    if let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) {
        update(task);
        task.status = TaskStatus::Complete;
    }
}

fn set_task_inputs(tasks: &mut [Task], spec_path: String, plan_path: Option<String>) {
    for task in tasks {
        task.inputs.spec_path = Some(spec_path.clone());
        task.inputs.plan_path = plan_path.clone();
    }
}

fn update_worker_task(tasks: &mut [Task], task_id: &str, status: &WorkerStatus, summary: &str) {
    if let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) {
        task.status = match status {
            WorkerStatus::Succeeded => TaskStatus::Complete,
            WorkerStatus::Skipped => TaskStatus::Skipped,
            WorkerStatus::Failed => TaskStatus::Failed,
        };
        task.outputs.summary = summary.to_string();
    }
}

fn run_verification(
    workspace: &std::path::Path,
    commands: &[String],
    cancellation_token: Option<&CancellationToken>,
) -> Result<Vec<ShellCommandResult>> {
    let env = std::collections::HashMap::new();
    commands
        .iter()
        .map(|command| {
            run_shell_command_with_env_and_cancellation(
                workspace,
                command,
                &env,
                cancellation_token,
            )
        })
        .collect()
}

fn run_coordinator_review(
    store: &StateStore,
    event_sink: &Option<EventSink>,
    hook: &Option<CoordinatorReviewHook>,
    session_id: &str,
    goal_id: &str,
    iteration: usize,
    max_iterations: usize,
    request: &str,
    task_id: &str,
    worker_task_record: &TaskRecord,
    worker_result: &crate::workers::WorkerResult,
    worker_outcome: &WorkerOutcome,
    verification_passed: bool,
    verification_results: &[ShellCommandResult],
    scope_check: &crate::tools::ScopeCheck,
    before_diff: &DiffSnapshot,
    after_diff: &DiffSnapshot,
) -> Result<Option<CoordinatorReview>> {
    let Some(hook) = hook else {
        return Ok(None);
    };

    let input = CoordinatorReviewInput {
        goal_id: goal_id.to_string(),
        task_id: task_id.to_string(),
        iteration,
        max_iterations,
        request: request.to_string(),
        worker_kind: worker_task_record.worker_kind.clone(),
        worker_model: worker_task_record.worker_model.clone(),
        worker_category: worker_task_record.worker_category.clone(),
        route_reason: worker_task_record.route_reason.clone(),
        worker_attempt: worker_task_record
            .attempts
            .last()
            .map(|attempt| attempt.attempt)
            .unwrap_or(1),
        worker_attempt_count: worker_task_record.attempts.len(),
        worker_failure_kind: worker_task_record
            .failure_kind
            .as_ref()
            .map(|kind| format!("{kind:?}")),
        worker_retry_reason: worker_task_record.retry_reason.clone(),
        worker_fallback_summary: worker_fallback_summary(worker_task_record),
        worker_status: worker_result.status.as_str().to_string(),
        worker_summary: worker_result.summary.clone(),
        worker_outcome_summary: worker_outcome.summary.clone(),
        worker_commands_run: worker_outcome.commands_run.clone(),
        worker_known_failures: worker_outcome.known_failures.clone(),
        worker_outcome_path: Some(worker_result.outcome_path.to_string_lossy().to_string()),
        budget_summary: format!("iteration {iteration} of {max_iterations}"),
        verification_passed,
        verification_summary: verification_summary(verification_results),
        scope_summary: scope_summary(scope_check),
        diff_summary: diff_summary(before_diff, after_diff),
    };

    let review = match hook(input) {
        Ok(review) => review,
        Err(error) => {
            append_event(
                store,
                event_sink,
                event(
                    session_id,
                    Some(goal_id),
                    None,
                    EventKind::TaskStarted,
                    format!("Coordinator review failed: {error:#}"),
                    json!({ "iteration": iteration }),
                ),
            )?;
            return Ok(None);
        }
    };

    let Some(review) = review else {
        return Ok(None);
    };

    let review_path = store.write_artifact(
        goal_id,
        &format!("coordinator-review-iteration-{iteration}.md"),
        &coordinator_review_artifact(iteration, &review),
    )?;
    append_event(
        store,
        event_sink,
        event(
            session_id,
            Some(goal_id),
            None,
            EventKind::TaskStarted,
            "Coordinator review completed",
            json!({
                "iteration": iteration,
                "goal_satisfied": review.goal_satisfied,
                "route_hint": &review.route_hint,
                "stop_reason": &review.stop_reason,
                "review_path": review_path.to_string_lossy(),
            }),
        ),
    )?;

    Ok(Some(review))
}

fn verification_summary(results: &[ShellCommandResult]) -> String {
    if results.is_empty() {
        return "No verification command ran.".to_string();
    }

    results
        .iter()
        .map(|result| {
            format!(
                "- `{}`: {} ({:?})",
                result.command,
                if result.success { "passed" } else { "failed" },
                result.exit_code
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn scope_summary(scope_check: &crate::tools::ScopeCheck) -> String {
    format!(
        "forbidden_touches={}, outside_allowed_paths={}, changed_file_count={}, max_files_exceeded={}",
        scope_check.forbidden_touches.len(),
        scope_check.outside_allowed_paths.len(),
        scope_check.changed_file_count,
        scope_check.max_files_exceeded
    )
}

fn diff_summary(before_diff: &DiffSnapshot, after_diff: &DiffSnapshot) -> String {
    format!(
        "before_files={}, after_files={}, is_git_repo={}",
        before_diff.changed_files.len(),
        after_diff.changed_files.len(),
        after_diff.is_git_repo
    )
}

fn coordinator_review_artifact(iteration: usize, review: &CoordinatorReview) -> String {
    format!(
        r#"# Coordinator Review

Iteration: `{iteration}`

## Decision

- goal_satisfied: `{}`
- summary: {}
- route_hint: `{}`
- stop_reason: `{}`

## Repair Request

{}

## Raw Provider Review

{}
"#,
        review
            .goal_satisfied
            .map(|satisfied| if satisfied { "yes" } else { "no" })
            .unwrap_or("unknown"),
        review.summary,
        review.route_hint.as_deref().unwrap_or("none"),
        review.stop_reason.as_deref().unwrap_or("none"),
        review
            .repair_request
            .as_deref()
            .unwrap_or("No repair request supplied."),
        review.raw_response.trim(),
    )
}

fn worker_fallback_summary(task_record: &TaskRecord) -> String {
    if task_record.attempts.len() <= 1 {
        return "single-attempt run".to_string();
    }

    task_record
        .attempts
        .iter()
        .map(|attempt| {
            format!(
                "- attempt {}: {} [{}] failure={} retry={}",
                attempt.attempt,
                attempt.worker_kind,
                attempt.worker_category,
                attempt
                    .failure_kind
                    .as_ref()
                    .map(|kind| format!("{kind:?}"))
                    .unwrap_or_else(|| "none".to_string()),
                attempt.retry_reason.as_deref().unwrap_or("none"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn check_run_cancelled(cancellation_token: Option<&CancellationToken>) -> Result<()> {
    if cancellation_token.is_some_and(CancellationToken::is_cancelled) {
        bail!("Gear run cancelled");
    }
    Ok(())
}

fn update_verification_task(
    tasks: &mut [Task],
    results: &[ShellCommandResult],
    verification_path: String,
    verification_passed: bool,
) {
    if let Some(task) = tasks.iter_mut().find(|task| task.id == "task_004") {
        task.status = if verification_passed {
            TaskStatus::Complete
        } else {
            TaskStatus::Failed
        };
        task.outputs.commands_run = results.iter().map(ShellCommandResult::record).collect();
        task.outputs.evidence.push(verification_path);
        task.outputs.summary = if verification_passed {
            "Verification passed.".to_string()
        } else {
            "Verification failed or no verification command was available.".to_string()
        };
    }
}

fn append_event(store: &StateStore, event_sink: &Option<EventSink>, event: Event) -> Result<()> {
    store.append_event(&event)?;
    if let Some(event_sink) = event_sink {
        event_sink(&event);
    }
    Ok(())
}

fn add_repair_task(
    tasks: &mut Vec<Task>,
    goal_id: &str,
    scope: &Scope,
    iteration: usize,
    verification_path: &std::path::Path,
    worker_kind: WorkerKind,
) -> String {
    let task_id = repair_task_id(iteration);
    tasks.push(Task {
        id: task_id.clone(),
        goal_id: goal_id.to_string(),
        title: format!("Repair failed verification iteration {iteration}"),
        kind: TaskKind::Repair,
        status: TaskStatus::Pending,
        assigned_worker: Some(worker_kind.as_str().to_string()),
        attempt: 1,
        scope: scope.clone(),
        inputs: TaskInputs {
            spec_path: None,
            plan_path: None,
            worker_packet_path: None,
        },
        outputs: TaskOutputs {
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            evidence: vec![verification_path.to_string_lossy().to_string()],
            summary: "Repair task created from failed verification.".to_string(),
        },
    });
    task_id
}

fn repair_task_id(iteration: usize) -> String {
    if iteration == 2 {
        "task_005".to_string()
    } else {
        format!("task_repair_{iteration:03}")
    }
}

fn review_task_id(iteration: usize) -> String {
    format!("task_review_{iteration:03}")
}

fn add_review_task(
    tasks: &mut Vec<Task>,
    goal_id: &str,
    scope: &Scope,
    iteration: usize,
    review_path: &std::path::Path,
    summary: &str,
) {
    tasks.push(Task {
        id: review_task_id(iteration),
        goal_id: goal_id.to_string(),
        title: format!("Review goal after iteration {iteration}"),
        kind: TaskKind::Review,
        status: TaskStatus::Complete,
        assigned_worker: None,
        attempt: iteration,
        scope: scope.clone(),
        inputs: TaskInputs::default(),
        outputs: TaskOutputs {
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            evidence: vec![review_path.to_string_lossy().to_string()],
            summary: summary.to_string(),
        },
    });
}

#[derive(Clone, Debug)]
struct GoalEvaluation {
    status: GoalStatus,
    should_continue: bool,
    summary: String,
    route_hint_override: Option<String>,
}

fn evaluate_goal(
    verification_passed: bool,
    worker_status: &WorkerStatus,
    worker_category: WorkerCategory,
    require_worker: bool,
    worker_failure_kind: Option<&TaskFailureKind>,
    worker_retry_reason: Option<&str>,
    scope_check: &crate::tools::ScopeCheck,
    coordinator_review: Option<&CoordinatorReview>,
    provider_unknown_streak: usize,
    repeated_failure_streak: usize,
    iteration: usize,
    max_iterations: usize,
) -> GoalEvaluation {
    let independent_review_requested = coordinator_review.is_some_and(|review| {
        review.goal_satisfied != Some(true)
            && review.route_hint.as_deref().and_then(WorkerCategory::parse)
                == Some(WorkerCategory::Review)
    });
    if !scope_check.forbidden_touches.is_empty()
        || !scope_check.outside_allowed_paths.is_empty()
        || scope_check.max_files_exceeded
    {
        return GoalEvaluation {
            status: GoalStatus::Blocked,
            should_continue: false,
            summary: "Goal blocked by scope checks.".to_string(),
            route_hint_override: None,
        };
    }
    if !verification_passed {
        if repeated_failure_streak >= 2 {
            let upgrade_hint = match worker_category {
                WorkerCategory::Quick | WorkerCategory::Repair | WorkerCategory::Explore => {
                    Some("deep")
                }
                WorkerCategory::Deep => Some("review"),
                WorkerCategory::Review => None,
                _ => Some("deep"),
            };
            if let Some(route_hint_override) = upgrade_hint
                && iteration < max_iterations
            {
                return GoalEvaluation {
                    status: GoalStatus::Running,
                    should_continue: true,
                    summary: format!(
                        "Gear detected repeated `{}` failures and will escalate to `{route_hint_override}`.",
                        worker_failure_kind
                            .map(|kind| format!("{kind:?}"))
                            .unwrap_or_else(|| "worker".to_string())
                    ),
                    route_hint_override: Some(route_hint_override.to_string()),
                };
            }
        }
        if let Some(worker_failure_kind) = worker_failure_kind {
            match worker_failure_kind {
                TaskFailureKind::NoFallbackRoute
                | TaskFailureKind::RepeatedFailureLimit
                | TaskFailureKind::PremiumBudgetExceeded => {
                    return GoalEvaluation {
                        status: GoalStatus::Limited,
                        should_continue: false,
                        summary: format!(
                            "Goal reached a worker fallback limit: {}.",
                            worker_retry_reason.unwrap_or(match worker_failure_kind {
                                TaskFailureKind::NoFallbackRoute => {
                                    "no different fallback route is available"
                                }
                                TaskFailureKind::RepeatedFailureLimit => {
                                    "same worker failure repeated too many times"
                                }
                                TaskFailureKind::PremiumBudgetExceeded => {
                                    "premium worker budget was exhausted"
                                }
                                _ => "worker fallback stopped",
                            })
                        ),
                        route_hint_override: None,
                    };
                }
                TaskFailureKind::WorkerUnavailable | TaskFailureKind::WorkerStartFailed
                    if require_worker =>
                {
                    return GoalEvaluation {
                        status: GoalStatus::NeedsUser,
                        should_continue: false,
                        summary: format!(
                            "Goal needs user input because the required worker is unavailable: {}.",
                            worker_retry_reason.unwrap_or("configure a worker command or route")
                        ),
                        route_hint_override: None,
                    };
                }
                _ => {}
            }
        }
    }
    if require_worker && *worker_status != WorkerStatus::Succeeded {
        return GoalEvaluation {
            status: GoalStatus::NeedsUser,
            should_continue: false,
            summary: format!(
                "Goal needs user input because worker status is {}.",
                worker_status.as_str()
            ),
            route_hint_override: None,
        };
    }
    if let Some(stop_reason) = coordinator_review
        .and_then(|review| review.stop_reason.as_deref())
        .and_then(normalized_stop_reason)
    {
        match stop_reason {
            "needs_user" => {
                return GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: "Coordinator review requested user input before continuing."
                        .to_string(),
                    route_hint_override: None,
                };
            }
            "blocked" => {
                return GoalEvaluation {
                    status: GoalStatus::Blocked,
                    should_continue: false,
                    summary: "Coordinator review marked the goal blocked.".to_string(),
                    route_hint_override: None,
                };
            }
            "limited" => {
                return GoalEvaluation {
                    status: GoalStatus::Limited,
                    should_continue: false,
                    summary: "Coordinator review stopped the loop at the current budget limit."
                        .to_string(),
                    route_hint_override: None,
                };
            }
            "complete" => {}
            _ => {}
        }
    }
    if verification_passed {
        if independent_review_requested {
            if iteration < max_iterations {
                return GoalEvaluation {
                    status: GoalStatus::Running,
                    should_continue: true,
                    summary: format!(
                        "Coordinator review requested an independent review worker after iteration {iteration}."
                    ),
                    route_hint_override: Some("review".to_string()),
                };
            }

            return GoalEvaluation {
                status: GoalStatus::Limited,
                should_continue: false,
                summary: format!(
                    "Goal reached the iteration limit ({max_iterations}) before the requested independent review could complete."
                ),
                route_hint_override: None,
            };
        }
        if coordinator_review.is_some_and(|review| review.goal_satisfied.is_none()) {
            if provider_unknown_streak >= 2 {
                if worker_category != WorkerCategory::Review && iteration < max_iterations {
                    return GoalEvaluation {
                        status: GoalStatus::Running,
                        should_continue: true,
                        summary: format!(
                            "Coordinator review stayed inconclusive for {provider_unknown_streak} iterations; Gear will escalate to an independent review worker."
                        ),
                        route_hint_override: Some("review".to_string()),
                    };
                }

                return GoalEvaluation {
                    status: GoalStatus::NeedsUser,
                    should_continue: false,
                    summary: "Coordinator review remained inconclusive after repeated passes; user input is required."
                        .to_string(),
                    route_hint_override: None,
                };
            }

            if iteration < max_iterations {
                return GoalEvaluation {
                    status: GoalStatus::Running,
                    should_continue: true,
                    summary: format!(
                        "Coordinator review remained inconclusive after iteration {iteration}; Gear will continue before declaring completion."
                    ),
                    route_hint_override: None,
                };
            }

            return GoalEvaluation {
                status: GoalStatus::NeedsUser,
                should_continue: false,
                summary: format!(
                    "Goal reached the iteration limit ({max_iterations}) while coordinator review remained inconclusive."
                ),
                route_hint_override: None,
            };
        }
        if coordinator_review.is_some_and(|review| review.goal_satisfied == Some(false)) {
            if iteration < max_iterations {
                return GoalEvaluation {
                    status: GoalStatus::Running,
                    should_continue: true,
                    summary: format!(
                        "Coordinator review found remaining work after iteration {iteration}; Gear will plan a repair iteration."
                    ),
                    route_hint_override: None,
                };
            }

            return GoalEvaluation {
                status: GoalStatus::Limited,
                should_continue: false,
                summary: format!(
                    "Goal reached the iteration limit ({max_iterations}) after coordinator review found remaining work."
                ),
                route_hint_override: None,
            };
        }

        let summary = if *worker_status == WorkerStatus::Succeeded {
            format!("Goal completed after {iteration} Gear iteration(s).")
        } else {
            format!(
                "Goal completed after {iteration} Gear iteration(s); verification passed while worker status was {}.",
                worker_status.as_str()
            )
        };
        return GoalEvaluation {
            status: GoalStatus::Complete,
            should_continue: false,
            summary,
            route_hint_override: None,
        };
    }
    if iteration < max_iterations {
        GoalEvaluation {
            status: GoalStatus::Running,
            should_continue: true,
            summary: format!(
                "Goal still incomplete after iteration {iteration}; Gear will plan a repair iteration."
            ),
            route_hint_override: None,
        }
    } else {
        GoalEvaluation {
            status: GoalStatus::Limited,
            should_continue: false,
            summary: format!(
                "Goal reached the iteration limit ({max_iterations}) before verification passed."
            ),
            route_hint_override: None,
        }
    }
}

fn normalized_stop_reason(value: &str) -> Option<&'static str> {
    let value = value.trim().to_ascii_lowercase();
    match value.as_str() {
        "complete" => Some("complete"),
        "limited" => Some("limited"),
        "blocked" => Some("blocked"),
        "needs_user" | "needs-user" | "user" => Some("needs_user"),
        _ => None,
    }
}

fn repair_request(
    original_request: &str,
    iteration: usize,
    verification_path: Option<&std::path::Path>,
    coordinator_review: Option<&CoordinatorReview>,
) -> String {
    let verification_path = verification_path
        .map(|path| path.to_string_lossy().to_string())
        .unwrap_or_else(|| "missing verification artifact".to_string());
    let coordinator_guidance = coordinator_review
        .and_then(|review| review.repair_request.as_deref())
        .unwrap_or("Use the verification artifact and goal review to choose the smallest repair.");
    let requested_category = coordinator_review
        .and_then(|review| review.route_hint.as_deref())
        .and_then(WorkerCategory::parse);
    if requested_category == Some(WorkerCategory::Review) {
        return format!(
            "Independent review iteration {iteration} for Gear goal.\n\nOriginal request:\n{original_request}\n\nInspect the current workspace, the verification artifact at `{verification_path}`, and the prior worker evidence. Do not expand scope or make speculative edits. Decide whether the goal is actually complete, and if not, identify the smallest missing fix or risk.\n\nCoordinator review guidance:\n{coordinator_guidance}"
        );
    }
    format!(
        "Repair iteration {iteration} for Gear goal.\n\nOriginal request:\n{original_request}\n\nReview the failed verification artifact at `{verification_path}` and make the smallest focused repair. Do not expand scope.\n\nCoordinator repair guidance:\n{coordinator_guidance}"
    )
}

fn goal_review_artifact(
    iteration: usize,
    max_iterations: usize,
    evaluation: &GoalEvaluation,
    worker_result: &crate::workers::WorkerResult,
    worker_category: WorkerCategory,
    worker_model: Option<&str>,
    route_reason: &str,
    worker_failure_kind: Option<&TaskFailureKind>,
    worker_retry_reason: Option<&str>,
    worker_outcome: &WorkerOutcome,
    scope_check: &crate::tools::ScopeCheck,
    verification_results: &[ShellCommandResult],
    coordinator_review: Option<&CoordinatorReview>,
) -> String {
    let verification_summary = if verification_results.is_empty() {
        "No verification command ran.".to_string()
    } else if verification_results.iter().all(|result| result.success) {
        "All verification commands passed.".to_string()
    } else {
        "One or more verification commands failed.".to_string()
    };

    let coordinator_summary = coordinator_review
        .map(|review| {
            format!(
                "- goal_satisfied: `{}`\n- route_hint: `{}`\n- stop_reason: `{}`\n- summary: {}",
                review
                    .goal_satisfied
                    .map(|satisfied| if satisfied { "yes" } else { "no" })
                    .unwrap_or("unknown"),
                review.route_hint.as_deref().unwrap_or("none"),
                review.stop_reason.as_deref().unwrap_or("none"),
                review.summary
            )
        })
        .unwrap_or_else(|| "No provider-backed coordinator review ran.".to_string());

    format!(
        r#"# Goal Review

Iteration: `{iteration}` / `{max_iterations}`

## Gear Decision

- status: `{}`
- should_continue: `{}`
- summary: {}

## Worker

- status: `{}`
- category: `{}`
- model: `{}`
- route_reason: {}
- failure_kind: `{}`
- retry_reason: {}
- summary: {}
- outcome: {}
- commands_run: {}
- known_failures: {}
- outcome_path: `{}`

## Verification

{}

## Coordinator Review

{}

## Scope

- forbidden_touches: {}
- outside_allowed_paths: {}
- changed_file_count: {}
- max_files_exceeded: {}
"#,
        evaluation.status.as_str(),
        evaluation.should_continue,
        evaluation.summary,
        worker_result.status.as_str(),
        worker_category.as_str(),
        worker_model.unwrap_or("none"),
        route_reason,
        worker_failure_kind
            .map(|failure_kind| format!("{failure_kind:?}"))
            .unwrap_or_else(|| "none".to_string()),
        worker_retry_reason.unwrap_or("none"),
        worker_result.summary,
        worker_outcome.summary,
        if worker_outcome.commands_run.is_empty() {
            "none".to_string()
        } else {
            worker_outcome.commands_run.join(", ")
        },
        if worker_outcome.known_failures.is_empty() {
            "none".to_string()
        } else {
            worker_outcome.known_failures.join("; ")
        },
        worker_result.outcome_path.to_string_lossy(),
        verification_summary,
        coordinator_summary,
        scope_check.forbidden_touches.len(),
        scope_check.outside_allowed_paths.len(),
        scope_check.changed_file_count,
        scope_check.max_files_exceeded,
    )
}

#[allow(dead_code)]
fn _keep_diff_snapshot_for_docs(_: &DiffSnapshot) {}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;

    use super::*;
    use crate::workers::{WorkerKind, WorkerStatus};

    #[test]
    fn run_creates_ledger_artifacts_and_verification() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{"scripts":{"build":"echo build-ok"}}"#,
        )?;
        let events = Arc::new(Mutex::new(Vec::new()));
        let event_sink = {
            let events = events.clone();
            Arc::new(move |event: &Event| {
                events
                    .lock()
                    .expect("events mutex poisoned")
                    .push(event.message.clone());
            }) as EventSink
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: vec!["src".to_string(), "README.md".to_string()],
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: Some(event_sink),
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: Some(CoordinatorModel {
                provider_id: "openai".to_string(),
                model_id: "gpt-4.1".to_string(),
                name: "GPT-4.1".to_string(),
            }),
            coordinator_brief: Some("Prefer a compact local implementation.".to_string()),
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert!(outcome.final_report_path.exists());
        assert!(outcome.events_path.exists());
        assert!(outcome.artifacts_root.join("spec.md").exists());
        assert!(outcome.artifacts_root.join("plan.md").exists());
        let goal = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent")
                .join("goals")
                .join(format!("{}.json", outcome.goal_id)),
        )?;
        assert!(goal.contains("\"provider_id\": \"openai\""));
        assert!(goal.contains("Prefer a compact local implementation."));
        let packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent")
                .join("workers")
                .join("task_003")
                .join("packet.json"),
        )?;
        assert!(packet.contains("\"model_id\": \"gpt-4.1\""));
        assert!(packet.contains("Prefer a compact local implementation."));
        let final_report = fs::read_to_string(&outcome.final_report_path)?;
        assert!(final_report.contains("GPT-4.1 (openai/gpt-4.1)"));
        assert!(final_report.contains("Prefer a compact local implementation."));
        assert!(final_report.contains("## Evidence Chain"));
        assert!(final_report.contains("worker_outcome"));
        assert!(final_report.contains("verification.md"));
        assert!(final_report.contains("spec.md"));
        assert!(final_report.contains("plan.md"));
        let verification = fs::read_to_string(outcome.artifacts_root.join("verification.md"))?;
        assert!(verification.contains("verify-ok"));
        let events = events.lock().expect("events mutex poisoned");
        assert!(events.iter().any(|event| event == "Spec artifact created"));
        assert!(events.iter().any(|event| event == "Verification passed"));
        assert!(
            events
                .iter()
                .any(|event| event.contains("Goal completed after 1 Gear iteration(s)"))
        );
        Ok(())
    }

    #[test]
    fn evaluation_mentions_non_required_worker_failure_when_verification_passes() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal(
            true,
            &WorkerStatus::Failed,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            None,
            0,
            0,
            1,
            DEFAULT_MAX_ITERATIONS,
        );

        assert_eq!(evaluation.status, GoalStatus::Complete);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("verification passed"));
        assert!(evaluation.summary.contains("worker status was failed"));
    }

    #[test]
    fn evaluation_honors_provider_needs_user_stop_reason() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "The provider needs user input.".to_string(),
            repair_request: None,
            route_hint: None,
            stop_reason: Some("needs_user".to_string()),
            raw_response: "STOP_REASON: needs_user".to_string(),
        };

        let evaluation = evaluate_goal(
            false,
            &WorkerStatus::Succeeded,
            WorkerCategory::Quick,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            0,
            0,
            1,
            DEFAULT_MAX_ITERATIONS,
        );

        assert_eq!(evaluation.status, GoalStatus::NeedsUser);
        assert!(!evaluation.should_continue);
    }

    #[test]
    fn evaluation_continues_when_independent_review_is_requested() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "Run an independent review worker before completion.".to_string(),
            repair_request: Some("Audit the final state independently.".to_string()),
            route_hint: Some("review".to_string()),
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: unknown\nROUTE_HINT: review".to_string(),
        };

        let evaluation = evaluate_goal(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Deep,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            0,
            0,
            1,
            DEFAULT_MAX_ITERATIONS,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert!(evaluation.summary.contains("independent review worker"));
    }

    #[test]
    fn evaluation_continues_on_first_unknown_provider_review() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "Still inconclusive.".to_string(),
            repair_request: Some("Inspect the current state again.".to_string()),
            route_hint: None,
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: unknown".to_string(),
        };

        let evaluation = evaluate_goal(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Repair,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            1,
            0,
            1,
            3,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert_eq!(evaluation.route_hint_override, None);
        assert!(evaluation.summary.contains("inconclusive"));
    }

    #[test]
    fn evaluation_escalates_to_review_after_second_unknown_provider_review() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: None,
            summary: "Still inconclusive.".to_string(),
            repair_request: Some("Request independent review.".to_string()),
            route_hint: None,
            stop_reason: None,
            raw_response: "GOAL_SATISFIED: unknown".to_string(),
        };

        let evaluation = evaluate_goal(
            true,
            &WorkerStatus::Succeeded,
            WorkerCategory::Repair,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            2,
            0,
            2,
            4,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert_eq!(evaluation.route_hint_override.as_deref(), Some("review"));
    }

    #[test]
    fn evaluation_maps_worker_fallback_limit_to_limited() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Deep,
            true,
            Some(&TaskFailureKind::RepeatedFailureLimit),
            Some("same failure kind `WorkerFailed` reached retry limit 2"),
            &scope_check,
            None,
            0,
            0,
            1,
            DEFAULT_MAX_ITERATIONS,
        );

        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("retry limit"));
    }

    #[test]
    fn evaluation_maps_premium_budget_limit_to_limited() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal(
            false,
            &WorkerStatus::Skipped,
            WorkerCategory::Deep,
            false,
            Some(&TaskFailureKind::PremiumBudgetExceeded),
            Some("premium worker budget 1 exhausted before `claude` attempt 2"),
            &scope_check,
            None,
            0,
            0,
            1,
            DEFAULT_MAX_ITERATIONS,
        );

        assert_eq!(evaluation.status, GoalStatus::Limited);
        assert!(!evaluation.should_continue);
        assert!(evaluation.summary.contains("premium worker budget"));
    }

    #[test]
    fn evaluation_maps_required_worker_unavailable_to_needs_user() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal(
            false,
            &WorkerStatus::Skipped,
            WorkerCategory::Repair,
            true,
            Some(&TaskFailureKind::WorkerUnavailable),
            Some("configure a worker command"),
            &scope_check,
            None,
            0,
            0,
            1,
            DEFAULT_MAX_ITERATIONS,
        );

        assert_eq!(evaluation.status, GoalStatus::NeedsUser);
        assert!(!evaluation.should_continue);
        assert!(
            evaluation
                .summary
                .contains("required worker is unavailable")
        );
    }

    #[test]
    fn evaluation_does_not_allow_provider_complete_to_override_failed_verification() {
        let scope_check = crate::tools::ScopeCheck::default();
        let review = CoordinatorReview {
            goal_satisfied: Some(true),
            summary: "The provider thinks the goal is complete.".to_string(),
            repair_request: None,
            route_hint: None,
            stop_reason: Some("complete".to_string()),
            raw_response: "GOAL_SATISFIED: yes\nSTOP_REASON: complete".to_string(),
        };

        let evaluation = evaluate_goal(
            false,
            &WorkerStatus::Succeeded,
            WorkerCategory::Repair,
            false,
            None,
            None,
            &scope_check,
            Some(&review),
            0,
            0,
            1,
            DEFAULT_MAX_ITERATIONS,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
    }

    #[test]
    fn evaluation_escalates_repeated_failures_to_deep() {
        let scope_check = crate::tools::ScopeCheck::default();
        let evaluation = evaluate_goal(
            false,
            &WorkerStatus::Failed,
            WorkerCategory::Repair,
            false,
            Some(&TaskFailureKind::WorkerFailed),
            Some("worker failed twice"),
            &scope_check,
            None,
            0,
            2,
            2,
            4,
        );

        assert_eq!(evaluation.status, GoalStatus::Running);
        assert!(evaluation.should_continue);
        assert_eq!(evaluation.route_hint_override.as_deref(), Some("deep"));
    }

    #[test]
    fn coordinator_review_can_request_repair_after_passing_verification() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let review_calls = Arc::new(Mutex::new(0usize));
        let hook: CoordinatorReviewHook = {
            let review_calls = review_calls.clone();
            Arc::new(move |input| {
                let mut calls = review_calls.lock().expect("review mutex poisoned");
                *calls += 1;
                if input.iteration == 1 {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(false),
                        summary: "The provider review wants one more repair pass.".to_string(),
                        repair_request: Some("Re-check the minimal deliverable.".to_string()),
                        route_hint: Some("deep".to_string()),
                        stop_reason: None,
                        raw_response: "GOAL_SATISFIED: no\nSUMMARY: The provider review wants one more repair pass.\nREPAIR_REQUEST: Re-check the minimal deliverable.\nROUTE_HINT: deep".to_string(),
                    }))
                } else {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(true),
                        summary: "The goal is now satisfied.".to_string(),
                        repair_request: None,
                        route_hint: None,
                        stop_reason: Some("complete".to_string()),
                        raw_response: "GOAL_SATISFIED: yes\nSUMMARY: The goal is now satisfied.\nREPAIR_REQUEST: none".to_string(),
                    }))
                }
            })
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: vec![
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Opencode,
                        worker_command: None,
                        worker_model: None,
                    },
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Codex,
                        worker_command: None,
                        worker_model: None,
                    },
                ],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: Some(hook),
            task_manager_control: None,
            task_manager: None,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert_eq!(*review_calls.lock().expect("review mutex poisoned"), 2);
        assert!(
            outcome
                .artifacts_root
                .join("coordinator-review-iteration-1.md")
                .exists()
        );
        assert!(
            outcome
                .artifacts_root
                .join("verification-iteration-2.md")
                .exists()
        );
        let repair_packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_005/packet.json"),
        )?;
        assert!(repair_packet.contains(r#""worker": "codex""#));
        Ok(())
    }

    #[test]
    fn coordinator_review_can_request_independent_review_after_passing_verification() -> Result<()>
    {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let review_calls = Arc::new(Mutex::new(0usize));
        let hook: CoordinatorReviewHook = {
            let review_calls = review_calls.clone();
            Arc::new(move |input| {
                let mut calls = review_calls.lock().expect("review mutex poisoned");
                *calls += 1;
                if input.iteration == 1 {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: None,
                        summary: "Run an independent review worker.".to_string(),
                        repair_request: Some("Audit the current deliverable without expanding scope.".to_string()),
                        route_hint: Some("review".to_string()),
                        stop_reason: None,
                        raw_response: "GOAL_SATISFIED: unknown\nSUMMARY: Run an independent review worker.\nREPAIR_REQUEST: Audit the current deliverable without expanding scope.\nROUTE_HINT: review".to_string(),
                    }))
                } else {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(true),
                        summary: "Independent review accepted the result.".to_string(),
                        repair_request: None,
                        route_hint: None,
                        stop_reason: Some("complete".to_string()),
                        raw_response: "GOAL_SATISFIED: yes\nSUMMARY: Independent review accepted the result.\nSTOP_REASON: complete".to_string(),
                    }))
                }
            })
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: vec![
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Opencode,
                        worker_command: None,
                        worker_model: None,
                    },
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Codex,
                        worker_command: None,
                        worker_model: None,
                    },
                ],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: Some(hook),
            task_manager_control: None,
            task_manager: None,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert_eq!(*review_calls.lock().expect("review mutex poisoned"), 2);
        let review_packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_005/packet.json"),
        )?;
        assert!(review_packet.contains(r#""worker": "codex""#));
        let review_prompt = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_005/prompt.md"),
        )?;
        assert!(review_prompt.contains("Independent review iteration 2"));
        Ok(())
    }

    #[test]
    fn consecutive_unknown_reviews_escalate_to_review_worker() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let review_calls = Arc::new(Mutex::new(0usize));
        let hook: CoordinatorReviewHook = {
            let review_calls = review_calls.clone();
            Arc::new(move |input| {
                let mut calls = review_calls.lock().expect("review mutex poisoned");
                *calls += 1;
                if input.iteration < 3 {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: None,
                        summary: "Still inconclusive.".to_string(),
                        repair_request: Some("Keep checking the final state.".to_string()),
                        route_hint: None,
                        stop_reason: None,
                        raw_response: "GOAL_SATISFIED: unknown\nSUMMARY: Still inconclusive."
                            .to_string(),
                    }))
                } else {
                    Ok(Some(CoordinatorReview {
                        goal_satisfied: Some(true),
                        summary: "Independent review accepted the result.".to_string(),
                        repair_request: None,
                        route_hint: None,
                        stop_reason: Some("complete".to_string()),
                        raw_response: "GOAL_SATISFIED: yes\nSTOP_REASON: complete".to_string(),
                    }))
                }
            })
        };

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo verify-ok".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: vec![
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Opencode,
                        worker_command: None,
                        worker_model: None,
                    },
                    crate::workers::WorkerRoute {
                        worker_kind: WorkerKind::Codex,
                        worker_command: None,
                        worker_model: None,
                    },
                ],
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: 3,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: Some(hook),
            task_manager_control: None,
            task_manager: None,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert_eq!(*review_calls.lock().expect("review mutex poisoned"), 3);
        let third_packet = fs::read_to_string(
            temp_dir
                .path()
                .join(".gearbox-agent/workers/task_repair_003/packet.json"),
        )?;
        assert!(third_packet.contains(r#""worker": "codex""#));
        Ok(())
    }

    #[test]
    fn failed_verification_creates_repair_task() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["exit 7".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
        })?;

        assert_eq!(outcome.status, GoalStatus::Limited);
        let tasks_path = temp_dir
            .path()
            .join(".gearbox-agent")
            .join("tasks")
            .join(format!("{}.tasks.json", outcome.goal_id));
        let tasks = fs::read_to_string(tasks_path)?;
        assert!(tasks.contains("task_005"));
        Ok(())
    }

    #[test]
    fn failed_verification_runs_repair_iteration_until_goal_passes() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        fs::write(temp_dir.path().join("package.json"), r#"{"scripts":{}}"#)?;
        let marker_path = temp_dir.path().join("repair-marker");
        let verify_command = format!(
            "test -f {} && echo repaired || (touch {}; exit 7)",
            marker_path.display(),
            marker_path.display()
        );

        let outcome = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec![verify_command],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: vec![".git".to_string()],
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: None,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
        })?;

        assert_eq!(outcome.status, GoalStatus::Complete);
        assert!(
            outcome
                .artifacts_root
                .join("verification-iteration-2.md")
                .exists()
        );
        assert!(
            outcome
                .artifacts_root
                .join("goal-review-iteration-2.md")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn cancelled_run_stops_before_artifacts() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let cancellation_token = CancellationToken::new();
        cancellation_token.cancel();

        let error = Orchestrator::run(RunOptions {
            request: "Build a tiny task tracker".to_string(),
            workspace: temp_dir.path().to_path_buf(),
            verification_commands: vec!["echo unreachable".to_string()],
            worker: WorkerConfig {
                worker_kind: WorkerKind::Opencode,
                worker_command: None,
                worker_model: None,
                worker_routes: Vec::new(),
                unavailable_worker_models: Vec::new(),
                premium_worker_budget: 1,
                max_parallel_workers: 1,
                max_parallel_per_key: 1,
                stale_task_timeout_secs: 30,
                skip_worker: true,
                require_worker: false,
            },
            allowed_paths: Vec::new(),
            forbidden_paths: Vec::new(),
            max_files_changed: 10,
            install_dependencies: false,
            event_sink: None,
            cancellation_token: Some(cancellation_token),
            max_iterations: DEFAULT_MAX_ITERATIONS,
            coordinator_model: None,
            coordinator_brief: None,
            coordinator_review_hook: None,
            task_manager_control: None,
            task_manager: None,
        })
        .expect_err("run should be cancelled");

        assert!(
            error.to_string().contains("Gear run cancelled"),
            "{error:#}"
        );
        Ok(())
    }
}
