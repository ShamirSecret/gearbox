use std::{
    collections::{HashSet, VecDeque},
    fs,
    io::{Read, Seek},
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    state::{Event, EventKind, Goal, GoalBudgetLedger, ObjectiveGraph, StateStore},
    task_manager::{TaskManager, TaskManagerSnapshot},
};

pub const GEAR_GUI_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const GEAR_GUI_EVENT_BUFFER_CAPACITY: usize = 256;
pub const GEAR_GUI_WORKER_DISPATCH_CAPACITY: usize = 64;
pub const GEAR_GUI_REVIEW_QUEUE_CAPACITY: usize = 16;
pub const GEAR_GUI_TIMELINE_CAPACITY: usize = 500;
pub const GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES: usize = 64 * 1024;
pub const GEAR_GUI_TERMINAL_SUMMARY_BYTES: usize = 16 * 1024;
pub const GEAR_GUI_MAX_CONVERSATION_SUMMARIES_PER_EPOCH: usize = 12;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GearRuntimeEventClass {
    Critical,
    Milestone,
    Telemetry,
    ConversationSummary,
}

impl GearRuntimeEventClass {
    fn is_lossless(self) -> bool {
        matches!(self, Self::Critical | Self::ConversationSummary)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeEventEnvelope {
    pub sequence: u64,
    pub class: GearRuntimeEventClass,
    pub semantic_key: String,
    pub session_id: String,
    pub objective_id: Option<String>,
    pub goal_id: Option<String>,
    pub task_id: Option<String>,
    pub run_epoch: Option<u64>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

impl GearRuntimeEventEnvelope {
    pub fn bounded_message(message: impl Into<String>, max_bytes: usize) -> String {
        let message = message.into();
        if message.len() <= max_bytes {
            return message;
        }
        let mut end = max_bytes.saturating_sub("\n[truncated]".len());
        while end > 0 && !message.is_char_boundary(end) {
            end -= 1;
        }
        let mut bounded = message[..end].to_string();
        bounded.push_str("\n[truncated]");
        bounded
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeHealth {
    pub last_activity_at: Option<String>,
    pub dropped_telemetry: u64,
    pub coalesced_telemetry: u64,
    pub refresh_required: bool,
    pub owned_child_processes: usize,
    pub rust_work_state: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeLifecycle {
    pub objective_status: Option<String>,
    pub goal_status: Option<String>,
    pub continuation_status: Option<String>,
    pub phase: Option<String>,
    pub stop_reason: Option<String>,
    pub recovery_state: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeBudgetSummary {
    pub calls_reserved: Option<u64>,
    pub calls_used: Option<u64>,
    pub tokens_reserved: Option<u64>,
    pub tokens_used: Option<u64>,
    pub cost_micros_reserved: Option<u64>,
    pub cost_micros_used: Option<u64>,
    pub unknown_usage_calls: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeGoalSummary {
    pub id: String,
    pub title: String,
    pub status: String,
    pub current_task_id: Option<String>,
    pub summary: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeObjectiveSummary {
    pub id: String,
    pub status: String,
    pub active_goal_id: Option<String>,
    pub stop_reason: Option<String>,
    pub consecutive_failures: usize,
    pub consecutive_no_progress: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeReviewSummary {
    pub status: String,
    pub epoch_events: usize,
    pub latest_event: Option<String>,
    pub plan_revision: Option<usize>,
    pub bundle_complete: Option<bool>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeRecoverySummary {
    pub continuation_status: Option<String>,
    pub resume_count: usize,
    pub stuck_reason: Option<String>,
    pub last_progress_marker: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeFeedbackSummary {
    pub tool_calls: usize,
    pub permission_events: usize,
    pub task_events: usize,
    pub worker_errors: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GearRuntimeFeedbackEvent {
    pub task_id: String,
    pub kind: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GearRuntimeSnapshot {
    pub schema_version: u32,
    pub sequence: u64,
    pub workspace: String,
    pub session_id: String,
    pub objective_id: Option<String>,
    pub goal_id: Option<String>,
    pub epoch_id: Option<String>,
    pub objective: Option<GearRuntimeObjectiveSummary>,
    pub goal: Option<GearRuntimeGoalSummary>,
    pub request_summary: String,
    pub lifecycle: GearRuntimeLifecycle,
    pub budget: GearRuntimeBudgetSummary,
    pub review: Option<GearRuntimeReviewSummary>,
    pub recovery: GearRuntimeRecoverySummary,
    pub feedback: GearRuntimeFeedbackSummary,
    pub feedback_events: Vec<GearRuntimeFeedbackEvent>,
    pub task_manager: Option<TaskManagerSnapshot>,
    pub timeline: Vec<GearRuntimeEventEnvelope>,
    pub health: GearRuntimeHealth,
}

impl GearRuntimeSnapshot {
    pub fn from_store(
        store: &StateStore,
        workspace: impl Into<String>,
        session_id: impl Into<String>,
        task_manager: Option<TaskManagerSnapshot>,
    ) -> anyhow::Result<Self> {
        let workspace = workspace.into();
        let session_id = session_id.into();
        let continuation = store.read_continuation_state_for_session(&session_id)?;
        let goal_id = continuation.as_ref().map(|state| state.goal_id.clone());
        let goal = goal_id
            .as_deref()
            .map(|id| store.read_goal(id))
            .transpose()?
            .flatten();
        let objective = goal_id
            .as_deref()
            .and_then(|id| find_objective_graph(store.objectives_dir().as_path(), id))
            .map(|graph| {
                (
                    graph.objective_id.clone(),
                    GearRuntimeObjectiveSummary {
                        id: graph.objective_id.clone(),
                        status: format!("{:?}", graph.status),
                        active_goal_id: graph.active_goal_id.clone(),
                        stop_reason: graph.stop_reason.clone(),
                        consecutive_failures: graph.consecutive_failures,
                        consecutive_no_progress: graph.consecutive_no_progress,
                    },
                    graph,
                )
            });
        let objective_id = objective.as_ref().map(|(id, _, _)| id.clone());
        let objective_summary = objective.as_ref().map(|(_, summary, _)| summary.clone());
        let graph = objective.as_ref().map(|(_, _, graph)| graph);
        let epoch_id = graph
            .and_then(|graph| {
                graph
                    .nodes
                    .iter()
                    .find(|node| Some(&node.goal_id) == goal_id.as_ref())
            })
            .map(|node| node.epoch_id.clone());
        let goal_budget = goal_id
            .as_deref()
            .map(|id| store.read_goal_budget_ledger(id))
            .transpose()?;
        let budget = goal_budget_summary(goal_budget.as_ref());
        let task_manager = match task_manager {
            Some(task_manager) => Some(task_manager),
            None => TaskManager::durable_snapshot(store, Some(&session_id))?,
        };
        let feedback = feedback_summary(store, task_manager.as_ref());
        let feedback_events = feedback_events(store, task_manager.as_ref());
        let timeline = read_timeline(store, &session_id);
        let epoch_events = goal_id
            .as_deref()
            .map(|id| store.read_goal_epoch_events(id))
            .transpose()?
            .unwrap_or_default();
        let review = review_summary(store, goal.as_ref(), epoch_id.as_deref(), &epoch_events);
        let recovery =
            continuation
                .as_ref()
                .map_or_else(GearRuntimeRecoverySummary::default, |state| {
                    GearRuntimeRecoverySummary {
                        continuation_status: Some(format!("{:?}", state.status)),
                        resume_count: state.resume_count,
                        stuck_reason: state.stuck_reason.clone(),
                        last_progress_marker: state.last_progress_marker.clone(),
                    }
                });
        let goal_summary = goal.as_ref().map(goal_summary);
        let request_summary = goal
            .as_ref()
            .map(|goal| goal.request.clone())
            .unwrap_or_default();
        let lifecycle = GearRuntimeLifecycle {
            objective_status: objective_summary
                .as_ref()
                .map(|summary| summary.status.clone()),
            goal_status: goal_summary.as_ref().map(|summary| summary.status.clone()),
            continuation_status: recovery.continuation_status.clone(),
            phase: epoch_events.last().map(|event| format!("{:?}", event.kind)),
            stop_reason: objective_summary
                .as_ref()
                .and_then(|summary| summary.stop_reason.clone()),
            recovery_state: recovery.stuck_reason.clone(),
        };
        let mut health = runtime_health(task_manager.as_ref());
        health.last_activity_at = last_event_timestamp(store, &session_id);
        let sequence = timeline.last().map(|event| event.sequence).unwrap_or(0);
        Ok(Self {
            schema_version: GEAR_GUI_SNAPSHOT_SCHEMA_VERSION,
            sequence,
            workspace,
            session_id,
            objective_id,
            goal_id,
            epoch_id,
            objective: objective_summary,
            goal: goal_summary,
            request_summary,
            lifecycle,
            budget,
            review,
            recovery,
            feedback,
            feedback_events,
            task_manager,
            timeline,
            health,
        }
        .bounded_for_ui())
    }

    pub fn bounded_for_ui(mut self) -> Self {
        if self.timeline.len() > GEAR_GUI_TIMELINE_CAPACITY {
            let keep_from = self.timeline.len() - GEAR_GUI_TIMELINE_CAPACITY;
            self.timeline.drain(..keep_from);
        }

        if let Some(task_manager) = self.task_manager.as_mut() {
            task_manager.tasks.truncate(32);
            for task in &mut task_manager.tasks {
                task.attempts.truncate(8);
                task.summary = GearRuntimeEventEnvelope::bounded_message(
                    std::mem::take(&mut task.summary),
                    GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                );
                task.summary_head = GearRuntimeEventEnvelope::bounded_message(
                    std::mem::take(&mut task.summary_head),
                    GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                );
                task.continuation_hint = GearRuntimeEventEnvelope::bounded_message(
                    std::mem::take(&mut task.continuation_hint),
                    GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                );
                for attempt in &mut task.attempts {
                    attempt.summary = GearRuntimeEventEnvelope::bounded_message(
                        std::mem::take(&mut attempt.summary),
                        GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                    );
                    if let Some(error) = attempt.error.take() {
                        attempt.error = Some(GearRuntimeEventEnvelope::bounded_message(
                            error,
                            GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                        ));
                    }
                }
            }
            task_manager.current_output = task_manager.current_output.take().map(|output| {
                GearRuntimeEventEnvelope::bounded_message(output, GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES)
            });
        }

        self.request_summary = GearRuntimeEventEnvelope::bounded_message(
            self.request_summary,
            GEAR_GUI_TERMINAL_SUMMARY_BYTES,
        );
        self.feedback_events.truncate(32);
        for event in &mut self.feedback_events {
            event.message = GearRuntimeEventEnvelope::bounded_message(
                std::mem::take(&mut event.message),
                GEAR_GUI_TERMINAL_SUMMARY_BYTES,
            );
        }
        self
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.schema_version != GEAR_GUI_SNAPSHOT_SCHEMA_VERSION {
            anyhow::bail!(
                "unsupported Gear GUI snapshot schema {}",
                self.schema_version
            );
        }
        if self.session_id.trim().is_empty() || self.workspace.trim().is_empty() {
            anyhow::bail!("Gear GUI snapshot requires session and workspace");
        }
        if self.timeline.len() > GEAR_GUI_TIMELINE_CAPACITY {
            anyhow::bail!("Gear GUI timeline exceeds its bounded capacity");
        }
        let serialized_size = serde_json::to_vec(self)?.len();
        if serialized_size > 512 * 1024 {
            anyhow::bail!("Gear GUI snapshot exceeds 512KiB");
        }
        Ok(())
    }
}

fn runtime_health(task_manager: Option<&TaskManagerSnapshot>) -> GearRuntimeHealth {
    let Some(task_manager) = task_manager else {
        return GearRuntimeHealth::default();
    };
    let owned_child_processes = task_manager
        .tasks
        .iter()
        .filter(|task| matches!(task.status, crate::task_manager::ManagedTaskStatus::Running))
        .count();
    let last_error = task_manager
        .tasks
        .iter()
        .rev()
        .flat_map(|task| task.attempts.iter().rev())
        .find_map(|attempt| attempt.error.clone());
    GearRuntimeHealth {
        owned_child_processes,
        last_error,
        ..GearRuntimeHealth::default()
    }
}

fn last_event_timestamp(store: &StateStore, session_id: &str) -> Option<String> {
    bounded_file_tail(&store.events_path(session_id))
        .lines()
        .rev()
        .find_map(|line| {
            serde_json::from_str::<Event>(line)
                .ok()
                .filter(|event| event.session_id == session_id)
                .map(|event| event.ts)
        })
}

fn goal_summary(goal: &Goal) -> GearRuntimeGoalSummary {
    GearRuntimeGoalSummary {
        id: goal.id.clone(),
        title: goal.title.clone(),
        status: format!("{:?}", goal.status),
        current_task_id: goal.current_task_id.clone(),
        summary: GearRuntimeEventEnvelope::bounded_message(
            goal.summary.clone(),
            GEAR_GUI_TERMINAL_SUMMARY_BYTES,
        ),
    }
}

fn goal_budget_summary(ledger: Option<&GoalBudgetLedger>) -> GearRuntimeBudgetSummary {
    let Some(ledger) = ledger else {
        return GearRuntimeBudgetSummary::default();
    };
    let mut summary = GearRuntimeBudgetSummary::default();
    summary.calls_reserved = Some(ledger.reservations.len() as u64);
    summary.calls_used = Some(
        ledger
            .reservations
            .iter()
            .filter(|reservation| {
                reservation.status != crate::state::BudgetReservationStatus::Reserved
            })
            .count() as u64,
    );
    summary.tokens_reserved = Some(
        ledger
            .reservations
            .iter()
            .map(|reservation| reservation.reserved_tokens)
            .sum(),
    );
    summary.tokens_used = Some(
        ledger
            .reservations
            .iter()
            .filter_map(|reservation| reservation.usage.as_ref())
            .filter_map(|usage| usage.total_tokens())
            .sum(),
    );
    summary.cost_micros_reserved = Some(
        ledger
            .reservations
            .iter()
            .map(|reservation| reservation.reserved_cost_micros)
            .sum(),
    );
    summary.cost_micros_used = Some(
        ledger
            .reservations
            .iter()
            .filter_map(|reservation| reservation.usage.as_ref())
            .filter_map(|usage| usage.cost_micros)
            .sum(),
    );
    summary.unknown_usage_calls = ledger
        .reservations
        .iter()
        .filter(|reservation| {
            reservation
                .usage
                .as_ref()
                .is_some_and(|usage| usage.is_unknown())
        })
        .count() as u64;
    summary
}

fn find_objective_graph(objectives_dir: &Path, goal_id: &str) -> Option<ObjectiveGraph> {
    let entries = fs::read_dir(objectives_dir).ok()?;
    entries.filter_map(Result::ok).find_map(|entry| {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json")
            || !path.file_name()?.to_str()?.ends_with(".graph.json")
        {
            return None;
        }
        let graph: ObjectiveGraph = serde_json::from_reader(fs::File::open(path).ok()?).ok()?;
        (graph.active_goal_id.as_deref() == Some(goal_id)
            || graph.nodes.iter().any(|node| node.goal_id == goal_id))
        .then_some(graph)
    })
}

fn review_summary(
    store: &StateStore,
    goal: Option<&Goal>,
    epoch_id: Option<&str>,
    events: &[crate::state::GoalEpochEvent],
) -> Option<GearRuntimeReviewSummary> {
    let goal = goal?;
    let plan = store.read_plan_graph(&goal.id).ok().flatten();
    let plan_revision = plan.as_ref().map(|plan| plan.revision);
    let bundle_complete = plan_revision
        .and_then(|revision| {
            store
                .read_review_epoch_bundle(&goal.id, revision)
                .ok()
                .flatten()
        })
        .map(|bundle| bundle.complete);
    let latest_event = events.last().map(|event| format!("{:?}", event.kind));
    Some(GearRuntimeReviewSummary {
        status: if bundle_complete == Some(true) {
            "complete".to_string()
        } else {
            "pending".to_string()
        },
        epoch_events: events
            .iter()
            .filter(|event| epoch_id.is_none_or(|id| id == event.epoch_id))
            .count(),
        latest_event,
        plan_revision,
        bundle_complete,
    })
}

fn feedback_summary(
    store: &StateStore,
    task_manager: Option<&TaskManagerSnapshot>,
) -> GearRuntimeFeedbackSummary {
    let mut summary = GearRuntimeFeedbackSummary::default();
    let mut observed_task_ids = HashSet::new();
    if let Some(task_manager) = task_manager {
        for task in task_manager.tasks.iter().take(32) {
            observed_task_ids.insert(task.task_id.clone());
            add_worker_feedback(&mut summary, &store.worker_dir(&task.task_id));
        }
    }
    // Durable projections may be rendered after the live TaskManager has been
    // dropped. Include bounded worker artifacts in that case, while avoiding
    // double counting task directories already represented above.
    let Ok(entries) = fs::read_dir(store.workers_dir()) else {
        return summary;
    };
    for entry in entries.flatten().take(64) {
        let task_id = entry.file_name().to_string_lossy().into_owned();
        if !observed_task_ids.insert(task_id) {
            continue;
        }
        if entry.file_type().is_ok_and(|file_type| file_type.is_dir()) {
            add_worker_feedback(&mut summary, &entry.path());
        }
    }
    summary
}

fn add_worker_feedback(summary: &mut GearRuntimeFeedbackSummary, worker_dir: &Path) {
    let tool_events = bounded_line_count(&worker_dir.join("tool-events.jsonl"));
    let permission_events = bounded_line_count(&worker_dir.join("permission-events.jsonl"));
    let task_events = bounded_line_count(&worker_dir.join("task-events.jsonl"));
    let worker_events = bounded_line_count(&worker_dir.join("worker-events.jsonl"));
    summary.tool_calls = summary.tool_calls.saturating_add(tool_events);
    summary.permission_events = summary.permission_events.saturating_add(permission_events);
    summary.task_events = summary.task_events.saturating_add(task_events);
    summary.worker_errors = summary.worker_errors.saturating_add(worker_events);
}

fn feedback_events(
    store: &StateStore,
    task_manager: Option<&TaskManagerSnapshot>,
) -> Vec<GearRuntimeFeedbackEvent> {
    let mut task_ids = task_manager
        .into_iter()
        .flat_map(|snapshot| snapshot.tasks.iter().map(|task| task.task_id.clone()))
        .collect::<Vec<_>>();
    if task_ids.is_empty() {
        if let Ok(entries) = fs::read_dir(store.workers_dir()) {
            task_ids.extend(
                entries
                    .flatten()
                    .take(64)
                    .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_dir()))
                    .map(|entry| entry.file_name().to_string_lossy().into_owned()),
            );
        }
    }
    task_ids.sort();
    task_ids.dedup();

    let mut events = Vec::new();
    for task_id in task_ids.into_iter().take(32) {
        let worker_dir = store.worker_dir(&task_id);
        for (kind, file_name) in [
            ("tool", "tool-events.jsonl"),
            ("permission", "permission-events.jsonl"),
            ("task", "task-events.jsonl"),
            ("worker", "worker-events.jsonl"),
        ] {
            for message in bounded_file_tail(&worker_dir.join(file_name))
                .lines()
                .rev()
                .take(4)
                .map(str::to_string)
            {
                events.push(GearRuntimeFeedbackEvent {
                    task_id: task_id.clone(),
                    kind: kind.to_string(),
                    message: GearRuntimeEventEnvelope::bounded_message(
                        message,
                        GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                    ),
                });
                if events.len() >= 32 {
                    return events;
                }
            }
        }
    }
    events.reverse();
    events
}

fn bounded_line_count(path: &Path) -> usize {
    let Ok(metadata) = fs::metadata(path) else {
        return 0;
    };
    let Ok(mut file) = fs::File::open(path) else {
        return 0;
    };
    let start = metadata
        .len()
        .saturating_sub(GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES as u64);
    if file.seek(std::io::SeekFrom::Start(start)).is_err() {
        return 0;
    }
    let mut tail = String::new();
    if file.read_to_string(&mut tail).is_err() {
        return 0;
    }
    tail.lines().count()
}

fn read_timeline(store: &StateStore, session_id: &str) -> Vec<GearRuntimeEventEnvelope> {
    let path = store.events_path(session_id);
    let sequence_base = fs::metadata(&path).map(|metadata| metadata.len()).unwrap_or(0);
    let tail = bounded_file_tail(&path);
    tail.lines()
        .enumerate()
        .filter_map(|(sequence, line)| {
            serde_json::from_str::<Event>(line).ok().map(|event| {
                let class = match &event.kind {
                    EventKind::WorkerOutput => GearRuntimeEventClass::Telemetry,
                    EventKind::WorkerFailed
                    | EventKind::GoalBlocked
                    | EventKind::GoalLimited
                    | EventKind::ContinuationStopped
                    | EventKind::VerificationFailed => GearRuntimeEventClass::Critical,
                    _ => GearRuntimeEventClass::Milestone,
                };
                GearRuntimeEventEnvelope {
                    // Use the durable byte position as a monotonic cursor
                    // base. The tail is bounded, so line indexes alone would
                    // reset to zero on every refresh and hide new events.
                    sequence: sequence_base.saturating_add(sequence as u64),
                    class,
                    semantic_key: format!(
                        "{:?}:{}",
                        event.kind,
                        event.task_id.as_deref().unwrap_or("")
                    ),
                    session_id: event.session_id,
                    objective_id: None,
                    goal_id: event.goal_id,
                    task_id: event.task_id,
                    run_epoch: None,
                    message: GearRuntimeEventEnvelope::bounded_message(
                        event.message,
                        GEAR_GUI_TERMINAL_SUMMARY_BYTES,
                    ),
                    payload: Some(event.data),
                }
            })
        })
        .collect()
}

fn bounded_file_tail(path: &Path) -> String {
    let Ok(metadata) = fs::metadata(path) else {
        return String::new();
    };
    let Ok(mut file) = fs::File::open(path) else {
        return String::new();
    };
    let start = metadata
        .len()
        .saturating_sub((GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES * 4) as u64);
    if file.seek(std::io::SeekFrom::Start(start)).is_err() {
        return String::new();
    }
    let mut tail = String::new();
    if file.read_to_string(&mut tail).is_err() {
        return String::new();
    }
    if start > 0 {
        if let Some(newline) = tail.find('\n') {
            tail.drain(..=newline);
        }
    }
    tail
}

#[derive(Clone, Debug)]
pub struct GearRuntimeEventBuffer {
    events: VecDeque<GearRuntimeEventEnvelope>,
    capacity: usize,
    dropped_telemetry: u64,
    coalesced_telemetry: u64,
    refresh_required: bool,
}

impl Default for GearRuntimeEventBuffer {
    fn default() -> Self {
        Self::new(GEAR_GUI_EVENT_BUFFER_CAPACITY)
    }
}

impl GearRuntimeEventBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: VecDeque::with_capacity(capacity),
            capacity: capacity.max(1),
            dropped_telemetry: 0,
            coalesced_telemetry: 0,
            refresh_required: false,
        }
    }

    pub fn push(&mut self, event: GearRuntimeEventEnvelope) {
        if event.class == GearRuntimeEventClass::Telemetry {
            if let Some(existing) = self.events.iter_mut().rev().find(|existing| {
                existing.class == GearRuntimeEventClass::Telemetry
                    && existing.semantic_key == event.semantic_key
            }) {
                *existing = event;
                self.coalesced_telemetry = self.coalesced_telemetry.saturating_add(1);
                return;
            }
        }

        if self.events.len() >= self.capacity {
            if let Some(index) = self
                .events
                .iter()
                .position(|existing| !existing.class.is_lossless())
            {
                self.events.remove(index);
                self.dropped_telemetry = self.dropped_telemetry.saturating_add(1);
            } else if event.class.is_lossless() {
                self.refresh_required = true;
                return;
            } else {
                self.dropped_telemetry = self.dropped_telemetry.saturating_add(1);
                return;
            }
        }
        self.events.push_back(event);
    }

    pub fn drain(&mut self) -> Vec<GearRuntimeEventEnvelope> {
        self.events.drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn dropped_telemetry(&self) -> u64 {
        self.dropped_telemetry
    }

    pub fn coalesced_telemetry(&self) -> u64 {
        self.coalesced_telemetry
    }

    pub fn refresh_required(&self) -> bool {
        self.refresh_required
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        Budget, ContinuationStatus, EventKind, Goal, GoalStatus, event as state_event,
    };
    use serde_json::json;

    fn event(class: GearRuntimeEventClass, key: &str, sequence: u64) -> GearRuntimeEventEnvelope {
        GearRuntimeEventEnvelope {
            sequence,
            class,
            semantic_key: key.to_string(),
            session_id: "session".to_string(),
            objective_id: Some("objective".to_string()),
            goal_id: Some("goal".to_string()),
            task_id: Some("task".to_string()),
            run_epoch: Some(0),
            message: "event".to_string(),
            payload: None,
        }
    }

    #[test]
    fn telemetry_is_coalesced_without_growing_the_buffer() {
        let mut buffer = GearRuntimeEventBuffer::new(4);
        for sequence in 0..100_000 {
            buffer.push(event(
                GearRuntimeEventClass::Telemetry,
                "task/output",
                sequence,
            ));
        }
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.coalesced_telemetry(), 99_999);
        assert_eq!(buffer.dropped_telemetry(), 0);
    }

    #[test]
    fn critical_events_survive_telemetry_pressure() {
        let mut buffer = GearRuntimeEventBuffer::new(2);
        buffer.push(event(GearRuntimeEventClass::Telemetry, "a", 1));
        buffer.push(event(GearRuntimeEventClass::Telemetry, "b", 2));
        buffer.push(event(GearRuntimeEventClass::Critical, "terminal", 3));
        let events = buffer.drain();
        assert!(events.iter().any(|event| event.semantic_key == "terminal"));
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn bounded_message_respects_utf8_and_byte_limit() {
        let message = GearRuntimeEventEnvelope::bounded_message("你好世界".repeat(100), 32);
        assert!(message.len() <= 32);
        assert!(message.is_char_boundary(message.len()));
        assert!(message.ends_with("[truncated]"));
    }

    #[test]
    fn bounded_snapshot_keeps_recent_timeline_and_worker_tail() {
        let mut snapshot = GearRuntimeSnapshot {
            schema_version: GEAR_GUI_SNAPSHOT_SCHEMA_VERSION,
            sequence: 1,
            workspace: "workspace".to_string(),
            session_id: "session".to_string(),
            objective_id: None,
            goal_id: None,
            epoch_id: None,
            objective: None,
            goal: None,
            request_summary: "request".to_string(),
            lifecycle: GearRuntimeLifecycle::default(),
            budget: GearRuntimeBudgetSummary::default(),
            review: None,
            recovery: GearRuntimeRecoverySummary::default(),
            feedback: GearRuntimeFeedbackSummary::default(),
            feedback_events: Vec::new(),
            task_manager: None,
            timeline: Vec::new(),
            health: GearRuntimeHealth::default(),
        };
        for sequence in 0..(GEAR_GUI_TIMELINE_CAPACITY + 7) {
            snapshot.timeline.push(event(
                GearRuntimeEventClass::Milestone,
                "milestone",
                sequence as u64,
            ));
        }
        snapshot.task_manager = Some(TaskManagerSnapshot {
            counts: Default::default(),
            artifacts_root: None,
            tasks: Vec::new(),
            current_output: Some("x".repeat(GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES + 100)),
        });

        let bounded = snapshot.bounded_for_ui();
        assert_eq!(bounded.timeline.len(), GEAR_GUI_TIMELINE_CAPACITY);
        assert_eq!(bounded.timeline[0].sequence, 7);
        assert!(
            bounded
                .task_manager
                .as_ref()
                .and_then(|tasks| tasks.current_output.as_ref())
                .is_some_and(|output| output.len() <= GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES)
        );
        assert!(bounded.validate().is_ok());
    }

    #[test]
    fn long_session_snapshot_serialization_stays_bounded() {
        let mut snapshot = GearRuntimeSnapshot {
            schema_version: GEAR_GUI_SNAPSHOT_SCHEMA_VERSION,
            sequence: 0,
            workspace: "workspace".to_string(),
            session_id: "session".to_string(),
            objective_id: Some("objective".to_string()),
            goal_id: Some("goal".to_string()),
            epoch_id: Some("epoch".to_string()),
            objective: None,
            goal: None,
            request_summary: "request".to_string(),
            lifecycle: GearRuntimeLifecycle::default(),
            budget: GearRuntimeBudgetSummary::default(),
            review: None,
            recovery: GearRuntimeRecoverySummary::default(),
            feedback: GearRuntimeFeedbackSummary::default(),
            feedback_events: (0..100_000)
                .map(|index| GearRuntimeFeedbackEvent {
                    task_id: format!("task-{}", index % 32),
                    kind: "worker".to_string(),
                    message: "bounded worker output".to_string(),
                })
                .collect(),
            task_manager: None,
            timeline: (0..100_000)
                .map(|sequence| event(GearRuntimeEventClass::Telemetry, "worker/output", sequence))
                .collect(),
            health: GearRuntimeHealth::default(),
        };

        snapshot = snapshot.bounded_for_ui();
        let serialized = serde_json::to_vec(&snapshot).expect("serialize bounded snapshot");
        assert!(serialized.len() <= 512 * 1024);
        assert_eq!(snapshot.timeline.len(), GEAR_GUI_TIMELINE_CAPACITY);
        assert_eq!(snapshot.feedback_events.len(), 32);
        snapshot.validate().expect("validate bounded snapshot");
    }

    #[test]
    fn event_ledger_tail_is_bounded_and_line_aligned() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("events.jsonl");
        let content = (0..20_000)
            .map(|index| format!("{{\"sequence\":{index}}}\n"))
            .collect::<String>();
        std::fs::write(&path, content).expect("write event fixture");
        let tail = bounded_file_tail(&path);
        assert!(tail.len() <= GEAR_GUI_WORKER_OUTPUT_TAIL_BYTES * 4);
        assert!(tail.starts_with('{'));
        assert!(tail.ends_with('\n'));
    }

    #[test]
    fn durable_projection_survives_without_live_tasks() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = StateStore::new(directory.path());
        store.initialize().expect("initialize state store");
        store
            .write_goal(&Goal {
                id: "goal-gui".to_string(),
                title: "GUI projection".to_string(),
                status: GoalStatus::Running,
                workspace: directory.path().display().to_string(),
                created_at: "2026-07-14T00:00:00Z".to_string(),
                updated_at: "2026-07-14T00:00:01Z".to_string(),
                request: "show runtime state".to_string(),
                product_type: "tool".to_string(),
                language_profile: "rust".to_string(),
                success_criteria: vec!["snapshot".to_string()],
                budget: Budget::default(),
                current_task_id: None,
                coordinator_model: None,
                coordinator_brief: None,
                summary: "running without a live task".to_string(),
            })
            .expect("write goal");
        store
            .write_continuation_state("session-gui", "goal-gui", ContinuationStatus::Running)
            .expect("write continuation");
        store
            .append_event(&state_event(
                "session-gui",
                Some("goal-gui"),
                None,
                EventKind::ContinuationStarted,
                "continuation started",
                json!({"sequence": 7}),
            ))
            .expect("write timeline event");
        let task_record = serde_json::to_string(&json!({
            "task_id": "task-gui",
            "worker_kind": "opencode_session",
            "worker_command": null,
            "worker_model": "opencode/deepseek-v4-flash-free",
            "worker_category": "execute",
            "route_hint": "execute",
            "route_reason": "durable fixture",
            "status": "failed",
            "started_at": "2026-07-14T00:00:00Z",
            "finished_at": "2026-07-14T00:00:01Z",
            "residency_state": "persisted_only",
            "run_epoch": 2,
            "notified_epoch": 2,
            "notification_failed_epoch": null,
            "killed": false,
            "session_id": "worker-session",
            "parent_session_id": null,
                    "root_session_id": "session-gui",
            "parent_task_id": null,
            "result_path": null,
            "outcome_path": null,
            "summary": "worker failed durably",
            "failure_kind": null,
            "retry_reason": "retry from GUI",
            "error": "provider unavailable",
            "attempts": []
        }))
        .expect("serialize task fixture");
        store
            .write_worker_file("task-gui", "task-record.json", &format!("{task_record}\n"))
            .expect("write durable task");
        store
            .write_worker_file(
                "task-gui",
                "permission-events.jsonl",
                "{\"status\":\"approved\",\"tool\":\"write\"}\n",
            )
            .expect("write feedback event");

        let snapshot = GearRuntimeSnapshot::from_store(
            &store,
            directory.path().display().to_string(),
            "session-gui",
            None,
        )
        .expect("project durable state");
        assert_eq!(snapshot.goal_id.as_deref(), Some("goal-gui"));
        assert_eq!(
            snapshot.goal.as_ref().map(|goal| goal.status.as_str()),
            Some("Running")
        );
        assert_eq!(
            snapshot.recovery.continuation_status.as_deref(),
            Some("Running")
        );
        let task_manager = snapshot
            .task_manager
            .as_ref()
            .expect("durable task projection should be visible after restart");
        assert_eq!(task_manager.tasks.len(), 1);
        assert_eq!(task_manager.tasks[0].task_id, "task-gui");
        assert_eq!(
            task_manager.tasks[0].worker_model.as_deref(),
            Some("opencode/deepseek-v4-flash-free")
        );
        assert!(task_manager.tasks[0].messageability.is_none());
        assert_eq!(snapshot.feedback_events.len(), 1);
        assert_eq!(snapshot.feedback_events[0].kind, "permission");
        assert!(snapshot.sequence > 0);
        assert!(snapshot.health.last_activity_at.is_some());
        snapshot.validate().expect("validate snapshot");

        let first_sequence = snapshot.sequence;
        store
            .append_event(&state_event(
                "session-gui",
                Some("goal-gui"),
                Some("task-gui"),
                EventKind::WorkerOutput,
                "worker output appended",
                json!({"delta":"bounded"}),
            ))
            .expect("append second timeline event");
        let refreshed = GearRuntimeSnapshot::from_store(
            &store,
            directory.path().display().to_string(),
            "session-gui",
            None,
        )
        .expect("refresh durable projection");
        assert!(refreshed.sequence > first_sequence);
    }
}
