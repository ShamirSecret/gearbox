use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use crate::state::{CoordinatorModel, StateStore, Task, timestamp};
use crate::tools::CancellationToken;
use crate::workers::{
    WorkerConfig, WorkerKind, WorkerOutcome, WorkerRegistry, WorkerResult, WorkerSessionHandle,
    WorkerStartRequest, WorkerStatus,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedTaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
    Lost,
    Skipped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAttemptStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
    Lost,
    Skipped,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidencyState {
    #[default]
    Resident,
    Evicted,
    Disposed,
    PersistedOnly,
    RpcDetached,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskFailureKind {
    WorkerFailed,
    WorkerStartFailed,
    WorkerCancelled,
    WorkerUnavailable,
    ModelUnavailable,
    PremiumBudgetExceeded,
    NoFallbackRoute,
    RepeatedFailureLimit,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskAttempt {
    pub attempt: usize,
    pub worker_kind: String,
    pub worker_command: Option<String>,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub route_hint: Option<String>,
    pub route_reason: String,
    pub status: TaskAttemptStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub session_id: Option<String>,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub summary: String,
    pub failure_kind: Option<TaskFailureKind>,
    pub retry_reason: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub worker_kind: String,
    pub worker_command: Option<String>,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub route_hint: Option<String>,
    pub route_reason: String,
    pub status: ManagedTaskStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    #[serde(default)]
    pub residency_state: ResidencyState,
    #[serde(default)]
    pub run_epoch: u64,
    #[serde(default = "default_notified_epoch")]
    pub notified_epoch: i64,
    #[serde(default)]
    pub notification_failed_epoch: Option<u64>,
    #[serde(default)]
    pub killed: bool,
    pub session_id: Option<String>,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub root_session_id: Option<String>,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub summary: String,
    pub failure_kind: Option<TaskFailureKind>,
    pub retry_reason: Option<String>,
    pub error: Option<String>,
    pub attempts: Vec<TaskAttempt>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskManagerSnapshotCounts {
    pub pending: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub interrupted: usize,
    pub lost: usize,
    pub skipped: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAttemptSnapshot {
    pub attempt: usize,
    pub worker_kind: String,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub status: TaskAttemptStatus,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub summary: String,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub task_id: String,
    pub status: ManagedTaskStatus,
    pub residency_state: ResidencyState,
    pub run_epoch: u64,
    pub notified_epoch: i64,
    pub worker_kind: String,
    pub worker_model: Option<String>,
    pub worker_category: String,
    pub attempts: Vec<TaskAttemptSnapshot>,
    pub result_path: Option<PathBuf>,
    pub outcome_path: Option<PathBuf>,
    pub summary: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskManagerSnapshot {
    pub counts: TaskManagerSnapshotCounts,
    pub tasks: Vec<TaskSnapshot>,
    pub current_output: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ManagedWorkerRun {
    pub result: WorkerResult,
    pub outcome: WorkerOutcome,
    pub record: TaskRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskLifecycleEvent {
    pub task_id: String,
    pub status: ManagedTaskStatus,
    pub residency_state: ResidencyState,
    pub timestamp: String,
    pub transition_type: Option<String>,
    pub transition_applied: bool,
    pub previous_status: Option<ManagedTaskStatus>,
    pub previous_residency_state: Option<ResidencyState>,
    pub run_epoch: u64,
    pub summary: String,
}

#[derive(Clone, Debug)]
struct TaskTransitionResult {
    applied: bool,
    transition_type: &'static str,
    previous_status: ManagedTaskStatus,
    previous_residency_state: ResidencyState,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
enum TaskTransition {
    Start {
        session_id: Option<String>,
    },
    Skip {
        finished_at: String,
        result_path: PathBuf,
        outcome_path: PathBuf,
        summary: String,
        failure_kind: Option<TaskFailureKind>,
    },
    Complete {
        finished_at: String,
        result_path: PathBuf,
        outcome_path: PathBuf,
        summary: String,
        failure_kind: Option<TaskFailureKind>,
    },
    Fail {
        finished_at: String,
        summary: String,
        failure_kind: TaskFailureKind,
        error: Option<String>,
    },
    Cancel {
        finished_at: String,
        summary: String,
        error: Option<String>,
    },
    Interrupt {
        finished_at: String,
        summary: String,
        error: Option<String>,
    },
    MarkLost {
        finished_at: String,
        summary: String,
        failure_kind: TaskFailureKind,
        error: Option<String>,
        killed: bool,
    },
    QueueRetry {
        summary: String,
        retry_reason: String,
    },
    MarkResident,
    Evict,
    Dispose,
    PersistOnly,
    DetachRpc,
}

#[derive(Clone)]
struct QueuedTask {
    store: StateStore,
    workspace: PathBuf,
    task: Task,
    route_attempt: usize,
    goal: String,
    verification_commands: Vec<String>,
    config: WorkerConfig,
    cancellation_token: Option<CancellationToken>,
    coordinator_model: Option<CoordinatorModel>,
    coordinator_brief: Option<String>,
    route_hint: Option<String>,
}

#[derive(Clone, Debug)]
struct ConcurrencyManager {
    max_parallel_workers: usize,
    max_parallel_per_key: usize,
}

#[derive(Clone, Debug)]
struct TaskRuntimePolicy {
    stale_task_timeout: Duration,
}

#[derive(Clone, Debug, Default)]
struct ReleaseGuard {
    released: HashSet<(String, u64)>,
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self {
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
        }
    }
}

impl Default for TaskRuntimePolicy {
    fn default() -> Self {
        Self {
            stale_task_timeout: Duration::from_secs(30),
        }
    }
}

impl TaskRuntimePolicy {
    fn from_worker_config(config: &WorkerConfig) -> Self {
        Self {
            stale_task_timeout: Duration::from_secs(config.stale_task_timeout_secs.max(1) as u64),
        }
    }
}

impl ReleaseGuard {
    fn release_once(&mut self, task_id: &str, run_epoch: u64) -> bool {
        self.released.insert((task_id.to_string(), run_epoch))
    }

    fn forget_task(&mut self, task_id: &str) {
        self.released
            .retain(|(released_task_id, _)| released_task_id != task_id);
    }
}

impl ConcurrencyManager {
    fn from_worker_config(config: &WorkerConfig) -> Self {
        Self {
            max_parallel_workers: config.max_parallel_workers.max(1),
            max_parallel_per_key: config.max_parallel_per_key.max(1),
        }
    }

    fn max_parallel_workers(&self) -> usize {
        self.max_parallel_workers.max(1)
    }

    fn max_parallel_per_key(&self) -> usize {
        self.max_parallel_per_key.max(1)
    }

    fn can_start(
        &self,
        running_tasks: &HashMap<String, RunningTask>,
        queued_task: &QueuedTask,
    ) -> bool {
        if running_tasks.len() >= self.max_parallel_workers() {
            return false;
        }

        let queued_key = concurrency_key_for_task(queued_task);
        let running_for_key = running_tasks
            .values()
            .filter(|running_task| {
                concurrency_key_for_task(&running_task.queued_task) == queued_key
            })
            .count();
        running_for_key < self.max_parallel_per_key()
    }
}

#[derive(Clone)]
struct RunningTask {
    store: StateStore,
    handle: Arc<dyn WorkerSessionHandle>,
    queued_task: QueuedTask,
    started_at: Instant,
}

struct FinishedTaskMessage {
    task_id: String,
    running_task: RunningTask,
    run_result: Result<(WorkerOutcome, WorkerResult)>,
}

#[derive(Clone)]
struct CurrentManagedTask {
    task_id: String,
    handle: Arc<dyn WorkerSessionHandle>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FallbackDecision {
    Queued,
    Unavailable {
        reason: String,
        failure_kind: TaskFailureKind,
    },
}

const WAIT_FOR_POLL_INTERVAL: Duration = Duration::from_millis(50);

const MAX_SAME_FAILURE_RETRIES: usize = 2;

fn default_notified_epoch() -> i64 {
    -1
}

#[derive(Clone, Default)]
pub struct TaskManagerControl {
    current_task: Arc<Mutex<Option<CurrentManagedTask>>>,
}

impl TaskManagerControl {
    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.current_task, &other.current_task)
    }

    pub fn current_task_id(&self) -> Result<Option<String>> {
        Ok(self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .as_ref()
            .map(|task| task.task_id.clone()))
    }

    pub fn current_last_output(&self) -> Result<Option<String>> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(None);
        };

        Ok(current_task.handle.last_output())
    }

    pub fn send_follow_up_current_task(&self, prompt: String) -> Result<bool> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(false);
        };

        current_task.handle.send_follow_up(prompt)?;
        Ok(true)
    }

    pub fn steer_current_task(&self, prompt: String) -> Result<bool> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(false);
        };

        current_task.handle.steer(prompt)?;
        Ok(true)
    }

    pub fn cancel_current_task(&self) -> Result<bool> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(false);
        };

        current_task.handle.cancel()?;
        Ok(true)
    }

    pub fn interrupt_current_task(&self) -> Result<bool> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(false);
        };

        current_task.handle.interrupt()?;
        Ok(true)
    }

    pub fn cancel_task(&self, task_id: &str) -> Result<bool> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        current_task.handle.cancel()?;
        Ok(true)
    }

    pub fn interrupt_task(&self, task_id: &str) -> Result<bool> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        current_task.handle.interrupt()?;
        Ok(true)
    }

    pub fn send_follow_up_task(&self, task_id: &str, prompt: String) -> Result<bool> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        current_task.handle.send_follow_up(prompt)?;
        Ok(true)
    }

    pub fn steer_task(&self, task_id: &str, prompt: String) -> Result<bool> {
        let Some(current_task) = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?
            .clone()
        else {
            return Ok(false);
        };
        if current_task.task_id != task_id {
            return Ok(false);
        }

        current_task.handle.steer(prompt)?;
        Ok(true)
    }

    fn set_current(&self, task_id: String, handle: Arc<dyn WorkerSessionHandle>) -> Result<()> {
        *self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))? =
            Some(CurrentManagedTask { task_id, handle });
        Ok(())
    }

    fn clear_current(&self, task_id: &str) -> Result<()> {
        let mut current_task = self
            .current_task
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager control mutex poisoned"))?;
        if current_task
            .as_ref()
            .is_some_and(|current_task| current_task.task_id == task_id)
        {
            *current_task = None;
        }
        Ok(())
    }
}

pub struct TaskManager {
    registry: WorkerRegistry,
    records: HashMap<String, TaskRecord>,
    running_tasks: HashMap<String, RunningTask>,
    queued_tasks: VecDeque<QueuedTask>,
    completed_runs: HashMap<String, ManagedWorkerRun>,
    completed_errors: HashMap<String, String>,
    completed_archive: VecDeque<TaskRecord>,
    concurrency: ConcurrencyManager,
    release_guard: ReleaseGuard,
    runtime_policy: TaskRuntimePolicy,
    control: TaskManagerControl,
    finished_task_tx: Sender<FinishedTaskMessage>,
    finished_task_rx: Receiver<FinishedTaskMessage>,
}

pub type SharedTaskManager = Arc<Mutex<TaskManager>>;

pub struct TaskManagerTickLoop {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    last_error: Arc<Mutex<Option<String>>>,
}

impl TaskManagerTickLoop {
    pub fn start(manager: SharedTaskManager, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let last_error = Arc::new(Mutex::new(None));
        let thread = thread::spawn({
            let stop = stop.clone();
            let last_error = last_error.clone();
            move || {
                while !stop.load(Ordering::Relaxed) {
                    let tick_result = manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))
                        .and_then(|mut manager| manager.tick());
                    if let Err(error) = tick_result {
                        if let Ok(mut last_error) = last_error.lock() {
                            *last_error = Some(format!("{error:#}"));
                        }
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                    thread::sleep(interval);
                }
            }
        });
        Self {
            stop,
            thread: Some(thread),
            last_error,
        }
    }

    pub fn last_error(&self) -> Result<Option<String>> {
        Ok(self
            .last_error
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager tick loop mutex poisoned"))?
            .clone())
    }

    pub fn stop(mut self) -> Result<()> {
        self.stop_inner(true)
    }

    fn stop_inner(&mut self, report_error: bool) -> Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                bail!("task manager tick loop panicked");
            }
        }
        if report_error {
            if let Some(error) = self.last_error()? {
                bail!("{error}");
            }
        }
        Ok(())
    }
}

impl Drop for TaskManagerTickLoop {
    fn drop(&mut self) {
        self.stop_inner(false).ok();
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        let (finished_task_tx, finished_task_rx) = std::sync::mpsc::channel();
        Self {
            registry: WorkerRegistry::default(),
            records: HashMap::new(),
            running_tasks: HashMap::new(),
            queued_tasks: VecDeque::new(),
            completed_runs: HashMap::new(),
            completed_errors: HashMap::new(),
            completed_archive: VecDeque::new(),
            concurrency: ConcurrencyManager::default(),
            release_guard: ReleaseGuard::default(),
            runtime_policy: TaskRuntimePolicy::default(),
            control: TaskManagerControl::default(),
            finished_task_tx,
            finished_task_rx,
        }
    }
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_shared(self) -> SharedTaskManager {
        Arc::new(Mutex::new(self))
    }

    pub fn with_control(control: TaskManagerControl) -> Self {
        Self {
            control,
            ..Self::default()
        }
    }

    pub fn control(&self) -> TaskManagerControl {
        self.control.clone()
    }

    pub fn set_worker_registry(&mut self, registry: WorkerRegistry) {
        self.registry = registry;
    }

    pub fn max_parallel_workers(&self) -> usize {
        self.concurrency.max_parallel_workers()
    }

    pub fn max_parallel_per_key(&self) -> usize {
        self.concurrency.max_parallel_per_key()
    }

    pub fn apply_worker_config(&mut self, config: &WorkerConfig) {
        self.concurrency = ConcurrencyManager::from_worker_config(config);
        self.runtime_policy = TaskRuntimePolicy::from_worker_config(config);
    }

    pub fn recover_orphaned_records(&mut self, store: &StateStore) -> Result<usize> {
        let workers_dir = store.workers_dir();
        if !workers_dir.exists() {
            return Ok(0);
        }

        let mut recovered = 0;
        for entry in fs::read_dir(&workers_dir)
            .with_context(|| format!("failed to read {}", workers_dir.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read {}", workers_dir.display()))?;
            let task_record_path = entry.path().join("task-record.json");
            if !task_record_path.is_file() {
                continue;
            }

            let json = fs::read_to_string(&task_record_path)
                .with_context(|| format!("failed to read {}", task_record_path.display()))?;
            let mut record: TaskRecord = serde_json::from_str(&json)
                .with_context(|| format!("failed to parse {}", task_record_path.display()))?;
            if !matches!(
                record.status,
                ManagedTaskStatus::Pending | ManagedTaskStatus::Running
            ) {
                continue;
            }

            let finished_at = timestamp();
            let summary =
                "Recovered orphaned Gear worker task after previous runtime exited.".to_string();
            record.retry_reason =
                Some("Recovered orphaned task record from previous runtime".into());
            let transition = transition_task_record(
                &mut record,
                TaskTransition::MarkLost {
                    finished_at: finished_at.clone(),
                    summary: summary.clone(),
                    failure_kind: TaskFailureKind::WorkerStartFailed,
                    error: Some(
                        "Task record was still pending/running on disk, but no live worker handle exists."
                            .into(),
                    ),
                    killed: false,
                },
            );
            record.error = Some(
                "Task record was still pending/running on disk, but no live worker handle exists."
                    .into(),
            );
            update_latest_attempt(&mut record, |attempt| {
                if matches!(
                    attempt.status,
                    TaskAttemptStatus::Pending | TaskAttemptStatus::Running
                ) {
                    attempt.status = TaskAttemptStatus::Lost;
                    attempt.finished_at = Some(finished_at);
                    attempt.summary = summary;
                    attempt.failure_kind = Some(TaskFailureKind::WorkerStartFailed);
                    attempt.retry_reason =
                        Some("Recovered orphaned task record from previous runtime".into());
                    attempt.error = Some(
                        "Task attempt was still pending/running on disk, but no live worker handle exists."
                            .into(),
                    );
                }
            });
            write_task_record(store, &record)?;
            append_task_lifecycle_event(store, &record, Some(&transition))?;
            self.records.insert(record.task_id.clone(), record);
            recovered += 1;
        }

        Ok(recovered)
    }

    pub fn start(&mut self, request: WorkerStartRequest<'_>) -> Result<String> {
        let task_id = request.task.id.clone();
        let queued_task = queued_task_from_request(request);
        let selected_route = queued_task
            .config
            .selected_route_for_hint(queued_task.route_attempt, queued_task.route_hint.as_deref());
        let worker_kind = selected_route.worker_kind.as_str().to_string();
        let worker_command = selected_route.worker_command.map(ToString::to_string);
        let worker_model = selected_route.worker_model.map(ToString::to_string);
        let worker_category = selected_route.category.as_str().to_string();
        let route_hint = queued_task.route_hint.clone();
        let route_reason = selected_route.route_reason.clone();
        let store = queued_task.store.clone();
        let started_at = timestamp();
        let record = TaskRecord {
            task_id: task_id.clone(),
            worker_kind: worker_kind.clone(),
            worker_command: worker_command.clone(),
            worker_model: worker_model.clone(),
            worker_category: worker_category.clone(),
            route_hint: route_hint.clone(),
            route_reason: route_reason.clone(),
            status: ManagedTaskStatus::Pending,
            started_at: started_at.clone(),
            finished_at: None,
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            result_path: None,
            outcome_path: None,
            summary: "Worker task queued.".to_string(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: queued_task.task.attempt,
                worker_kind,
                worker_command,
                worker_model,
                worker_category,
                route_hint,
                route_reason,
                status: TaskAttemptStatus::Pending,
                started_at,
                finished_at: None,
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "Worker task queued.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        };
        write_task_record(&store, &record)?;
        append_task_lifecycle_event(&store, &record, None)?;
        self.records.insert(task_id.clone(), record.clone());

        self.queued_tasks.push_back(queued_task);
        self.process_queue()?;
        Ok(task_id)
    }

    pub fn wait_for(&mut self, task_id: &str) -> Result<ManagedWorkerRun> {
        loop {
            if let Some(run) = self.try_wait_for(task_id)? {
                return Ok(run);
            }
            match self.finished_task_rx.recv_timeout(WAIT_FOR_POLL_INTERVAL) {
                Ok(finished_task) => self.settle_finished_task(finished_task)?,
                Err(RecvTimeoutError::Timeout) => {
                    self.tick()?;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    bail!("failed to receive finished worker task: channel disconnected");
                }
            }
        }
    }

    pub fn try_wait_for(&mut self, task_id: &str) -> Result<Option<ManagedWorkerRun>> {
        self.tick()?;
        if let Some(run) = self.completed_runs.remove(task_id) {
            return Ok(Some(run));
        }
        if let Some(error) = self.completed_errors.remove(task_id) {
            bail!("{error}");
        }

        if !self.running_tasks.contains_key(task_id)
            && !self
                .queued_tasks
                .iter()
                .any(|queued_task| queued_task.task.id == task_id)
        {
            bail!("managed task is not running or complete: {task_id}");
        }

        Ok(None)
    }

    pub fn tick(&mut self) -> Result<usize> {
        let mut settled_count = 0;
        loop {
            match self.finished_task_rx.try_recv() {
                Ok(finished_task) => {
                    self.settle_finished_task(finished_task)?;
                    settled_count += 1;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    bail!("finished worker task channel disconnected");
                }
            }
        }
        settled_count += self.sweep_orphaned_task_state()?;
        settled_count += self.sweep_stale_running_tasks()?;
        self.process_queue()?;
        Ok(settled_count)
    }

    fn sweep_orphaned_task_state(&mut self) -> Result<usize> {
        let orphaned_running_ids = self
            .running_tasks
            .keys()
            .filter(|task_id| !self.records.contains_key(*task_id))
            .cloned()
            .collect::<Vec<_>>();
        let orphaned_queued_len_before = self.queued_tasks.len();
        self.queued_tasks
            .retain(|queued_task| self.records.contains_key(&queued_task.task.id));

        for task_id in &orphaned_running_ids {
            if let Some(running_task) = self.running_tasks.get(task_id) {
                if let Err(error) = running_task.handle.cancel() {
                    eprintln!("failed to cancel orphaned Gear worker task `{task_id}`: {error:#}");
                }
            }
            self.forget_task(task_id)?;
        }

        Ok(orphaned_running_ids.len() + orphaned_queued_len_before - self.queued_tasks.len())
    }

    fn release_running_task_once(&mut self, task_id: &str, run_epoch: u64) -> Result<bool> {
        if !self.release_guard.release_once(task_id, run_epoch) {
            return Ok(false);
        }

        self.running_tasks.remove(task_id);
        self.control.clear_current(task_id)?;
        Ok(true)
    }

    fn forget_task(&mut self, task_id: &str) -> Result<()> {
        self.running_tasks.remove(task_id);
        self.queued_tasks
            .retain(|queued_task| queued_task.task.id != task_id);
        self.completed_runs.remove(task_id);
        self.completed_errors.remove(task_id);
        self.records.remove(task_id);
        self.completed_archive
            .retain(|record| record.task_id != task_id);
        self.release_guard.forget_task(task_id);
        self.control.clear_current(task_id)?;
        Ok(())
    }

    fn sweep_stale_running_tasks(&mut self) -> Result<usize> {
        let stale_task_timeout = self.runtime_policy.stale_task_timeout;
        let now = Instant::now();
        let stale_tasks = self
            .running_tasks
            .iter()
            .filter(|(_, running_task)| {
                now.duration_since(running_task.started_at) > stale_task_timeout
            })
            .map(|(task_id, running_task)| (task_id.clone(), running_task.clone()))
            .collect::<Vec<_>>();
        let stale_count = stale_tasks.len();

        for (task_id, running_task) in stale_tasks {
            if let Err(error) = running_task.handle.cancel() {
                eprintln!("failed to cancel stale Gear worker task `{task_id}`: {error:#}");
            }
            match self.settle_running_task(
                &task_id,
                running_task,
                Err(anyhow::anyhow!(
                    "worker task timed out waiting for outcome after {:?}",
                    stale_task_timeout
                )),
            ) {
                Ok(Some(run)) => {
                    self.completed_runs.insert(task_id, run);
                }
                Ok(None) => {}
                Err(error) => {
                    self.completed_errors
                        .insert(task_id, format!("Worker task failed: {error:#}"));
                }
            }
        }

        Ok(stale_count)
    }

    fn settle_finished_task(&mut self, finished_task: FinishedTaskMessage) -> Result<()> {
        let task_id = finished_task.task_id.clone();
        match self.settle_running_task(
            &finished_task.task_id,
            finished_task.running_task,
            finished_task.run_result,
        ) {
            Ok(Some(run)) => {
                self.completed_runs.insert(task_id, run);
            }
            Ok(None) => {}
            Err(error) => {
                self.completed_errors
                    .insert(task_id, format!("Worker task failed: {error:#}"));
            }
        }
        Ok(())
    }

    fn settle_running_task(
        &mut self,
        task_id: &str,
        running_task: RunningTask,
        run_result: Result<(WorkerOutcome, WorkerResult)>,
    ) -> Result<Option<ManagedWorkerRun>> {
        match run_result {
            Ok((outcome, result)) => {
                let Some(mut record) = self.records.remove(task_id) else {
                    self.forget_task(task_id)?;
                    return Ok(None);
                };
                let transition = match result.status {
                    WorkerStatus::Succeeded => transition_task_record(
                        &mut record,
                        TaskTransition::Complete {
                            finished_at: timestamp(),
                            result_path: result.result_path.clone(),
                            outcome_path: result.outcome_path.clone(),
                            summary: outcome.summary.clone(),
                            failure_kind: failure_kind_from_worker_result(&result, &outcome),
                        },
                    ),
                    WorkerStatus::Skipped => transition_task_record(
                        &mut record,
                        TaskTransition::Skip {
                            finished_at: timestamp(),
                            result_path: result.result_path.clone(),
                            outcome_path: result.outcome_path.clone(),
                            summary: outcome.summary.clone(),
                            failure_kind: failure_kind_from_worker_result(&result, &outcome),
                        },
                    ),
                    WorkerStatus::Failed => {
                        let cancelled = outcome.known_failures.iter().any(|failure| {
                            let failure = failure.to_ascii_lowercase();
                            failure.contains("cancelled") || failure.contains("canceled")
                        });
                        if cancelled {
                            transition_task_record(
                                &mut record,
                                TaskTransition::Cancel {
                                    finished_at: timestamp(),
                                    summary: "Worker task cancelled.".to_string(),
                                    error: None,
                                },
                            )
                        } else {
                            transition_task_record(
                                &mut record,
                                TaskTransition::Fail {
                                    finished_at: timestamp(),
                                    summary: outcome.summary.clone(),
                                    failure_kind: failure_kind_from_worker_result(
                                        &result, &outcome,
                                    )
                                    .unwrap_or(TaskFailureKind::WorkerFailed),
                                    error: None,
                                },
                            )
                        }
                    }
                };
                if transition.applied {
                    record.result_path = Some(result.result_path.clone());
                    record.outcome_path = Some(result.outcome_path.clone());
                }
                write_task_record(&running_task.store, &record)?;
                append_task_lifecycle_event(&running_task.store, &record, Some(&transition))?;

                if should_retry_worker_result(&record, &running_task.queued_task, &result) {
                    let mut retry_task = running_task.queued_task.clone();
                    match queue_next_attempt(&mut record, &mut retry_task) {
                        FallbackDecision::Queued => {
                            write_task_record(&running_task.store, &record)?;
                            append_task_lifecycle_event(&running_task.store, &record, None)?;
                            let run_epoch = record.run_epoch;
                            self.release_running_task_once(task_id, run_epoch)?;
                            self.records.insert(task_id.to_string(), record);
                            self.start_queued_task(retry_task)?;
                            return Ok(None);
                        }
                        FallbackDecision::Unavailable {
                            reason,
                            failure_kind,
                        } => {
                            record.failure_kind = Some(failure_kind);
                            record.retry_reason = Some(reason.clone());
                            if let Some(attempt) = record.attempts.last_mut() {
                                attempt.retry_reason = Some(reason);
                            }
                            write_task_record(&running_task.store, &record)?;
                            append_task_lifecycle_event(&running_task.store, &record, None)?;
                        }
                    }
                }

                let run = ManagedWorkerRun {
                    result,
                    outcome,
                    record,
                };
                let run_epoch = run.record.run_epoch;
                self.release_running_task_once(task_id, run_epoch)?;
                self.records.insert(task_id.to_string(), run.record.clone());
                self.completed_runs.insert(task_id.to_string(), run.clone());
                self.completed_archive.push_back(run.record.clone());
                while self.completed_archive.len() > 100 {
                    self.completed_archive.pop_front();
                }
                self.process_queue()?;
                Ok(Some(run))
            }
            Err(error) => {
                let Some(mut record) = self.records.remove(task_id) else {
                    self.forget_task(task_id)?;
                    return Ok(None);
                };
                let error_text = format!("{error:#}");
                let transition = if record.status == ManagedTaskStatus::Interrupted {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Fail {
                            finished_at: timestamp(),
                            summary: "Worker task interrupted.".to_string(),
                            failure_kind: TaskFailureKind::WorkerCancelled,
                            error: Some(error_text.clone()),
                        },
                    )
                } else if error_text.contains("timed out waiting for outcome") {
                    transition_task_record(
                        &mut record,
                        TaskTransition::MarkLost {
                            finished_at: timestamp(),
                            summary: "Worker task timed out waiting for outcome.".to_string(),
                            failure_kind: TaskFailureKind::WorkerFailed,
                            error: Some(error_text.clone()),
                            killed: false,
                        },
                    )
                } else if record.status != ManagedTaskStatus::Cancelled
                    && !error_text.contains("cancelled")
                    && !error_text.contains("canceled")
                {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Fail {
                            finished_at: timestamp(),
                            summary: "Worker task failed before producing an outcome.".to_string(),
                            failure_kind: TaskFailureKind::WorkerFailed,
                            error: Some(error_text.clone()),
                        },
                    )
                } else {
                    transition_task_record(
                        &mut record,
                        TaskTransition::Cancel {
                            finished_at: timestamp(),
                            summary: "Worker task cancelled.".to_string(),
                            error: Some(error_text.clone()),
                        },
                    )
                };
                write_task_record(&running_task.store, &record)?;
                append_task_lifecycle_event(&running_task.store, &record, Some(&transition))?;
                if record.status == ManagedTaskStatus::Failed {
                    let mut retry_task = running_task.queued_task.clone();
                    match queue_next_attempt(&mut record, &mut retry_task) {
                        FallbackDecision::Queued => {
                            write_task_record(&running_task.store, &record)?;
                            append_task_lifecycle_event(&running_task.store, &record, None)?;
                            let run_epoch = record.run_epoch;
                            self.release_running_task_once(task_id, run_epoch)?;
                            self.records.insert(task_id.to_string(), record);
                            self.start_queued_task(retry_task)?;
                            return Ok(None);
                        }
                        FallbackDecision::Unavailable {
                            reason,
                            failure_kind,
                        } => {
                            record.failure_kind = Some(failure_kind);
                            record.retry_reason = Some(reason.clone());
                            if let Some(attempt) = record.attempts.last_mut() {
                                attempt.retry_reason = Some(reason);
                            }
                            write_task_record(&running_task.store, &record)?;
                            append_task_lifecycle_event(&running_task.store, &record, None)?;
                        }
                    }
                }
                let run_epoch = record.run_epoch;
                self.release_running_task_once(task_id, run_epoch)?;
                self.records.insert(task_id.to_string(), record);
                self.process_queue()?;
                Err(error)
            }
        }
    }

    pub fn run_worker_task(&mut self, request: WorkerStartRequest<'_>) -> Result<ManagedWorkerRun> {
        let task_id = self.start(request)?;
        self.wait_for(&task_id)
    }

    pub fn cancel_task(&mut self, task_id: &str) -> Result<()> {
        let mut queued_store = None;
        if let Some(index) = self
            .queued_tasks
            .iter()
            .position(|queued_task| queued_task.task.id == task_id)
        {
            queued_store = Some(
                self.queued_tasks
                    .remove(index)
                    .context("queued task disappeared during cancellation")?
                    .store,
            );
        }
        let Some(record) = self.records.get_mut(task_id) else {
            bail!("unknown managed task: {task_id}");
        };
        let transition = transition_task_record(
            record,
            TaskTransition::Cancel {
                finished_at: timestamp(),
                summary: "Worker task cancelled.".to_string(),
                error: None,
            },
        );
        let store = if let Some(running_task) = self.running_tasks.get(task_id) {
            if transition.applied {
                running_task.handle.cancel()?;
            }
            Some(running_task.store.clone())
        } else {
            queued_store
        };
        if let Some(store) = store {
            write_task_record(&store, record)?;
            append_task_lifecycle_event(&store, record, Some(&transition))?;
        }
        Ok(())
    }

    pub fn interrupt_task(&mut self, task_id: &str) -> Result<()> {
        let Some(record) = self.records.get_mut(task_id) else {
            bail!("unknown managed task: {task_id}");
        };
        let transition = transition_task_record(
            record,
            TaskTransition::Interrupt {
                finished_at: timestamp(),
                summary: "Worker task interrupted.".to_string(),
                error: None,
            },
        );
        let store = if let Some(running_task) = self.running_tasks.get(task_id) {
            if transition.applied {
                running_task.handle.interrupt()?;
                if let Some(output) = running_task.handle.last_output() {
                    record.summary = output;
                    if let Some(attempt) = record.attempts.last_mut() {
                        attempt.summary = record.summary.clone();
                    }
                }
            }
            Some(running_task.store.clone())
        } else {
            None
        };
        if let Some(store) = store {
            write_task_record(&store, record)?;
            append_task_lifecycle_event(&store, record, Some(&transition))?;
        }
        Ok(())
    }

    pub fn list(&self) -> Vec<TaskRecord> {
        let mut records = self.records.values().cloned().collect::<Vec<_>>();
        records.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        records
    }

    pub fn snapshot(&self) -> Result<TaskManagerSnapshot> {
        let records = self.list();
        let mut counts = TaskManagerSnapshotCounts::default();
        for record in &records {
            match &record.status {
                ManagedTaskStatus::Pending => counts.pending += 1,
                ManagedTaskStatus::Running => counts.running += 1,
                ManagedTaskStatus::Completed => counts.completed += 1,
                ManagedTaskStatus::Failed => counts.failed += 1,
                ManagedTaskStatus::Cancelled => counts.cancelled += 1,
                ManagedTaskStatus::Interrupted => counts.interrupted += 1,
                ManagedTaskStatus::Lost => counts.lost += 1,
                ManagedTaskStatus::Skipped => counts.skipped += 1,
            }
        }

        let tasks = records
            .into_iter()
            .map(|record| TaskSnapshot {
                task_id: record.task_id,
                status: record.status,
                residency_state: record.residency_state,
                run_epoch: record.run_epoch,
                notified_epoch: record.notified_epoch,
                worker_kind: record.worker_kind,
                worker_model: record.worker_model,
                worker_category: record.worker_category,
                attempts: record
                    .attempts
                    .into_iter()
                    .map(|attempt| TaskAttemptSnapshot {
                        attempt: attempt.attempt,
                        worker_kind: attempt.worker_kind,
                        worker_model: attempt.worker_model,
                        worker_category: attempt.worker_category,
                        status: attempt.status,
                        result_path: attempt.result_path,
                        outcome_path: attempt.outcome_path,
                        summary: attempt.summary,
                        error: attempt.error,
                    })
                    .collect(),
                result_path: record.result_path,
                outcome_path: record.outcome_path,
                summary: record.summary,
            })
            .collect();

        Ok(TaskManagerSnapshot {
            counts,
            tasks,
            current_output: self.control.current_last_output()?,
        })
    }

    fn process_queue(&mut self) -> Result<()> {
        while self.running_tasks.len() < self.concurrency.max_parallel_workers() {
            let Some(index) = self.queued_tasks.iter().position(|queued_task| {
                self.concurrency.can_start(&self.running_tasks, queued_task)
            }) else {
                break;
            };
            let queued_task = self
                .queued_tasks
                .remove(index)
                .context("queued task disappeared while starting worker")?;
            self.start_queued_task(queued_task)?;
        }
        Ok(())
    }

    fn start_queued_task(&mut self, mut queued_task: QueuedTask) -> Result<()> {
        let task_id = queued_task.task.id.clone();
        loop {
            if let Some(model_unavailable_error) =
                model_unavailable_error_for_task(&queued_task.config, &queued_task)
            {
                let mut failed_record = self
                    .records
                    .remove(&task_id)
                    .context("missing task manager record for unavailable worker model")?;
                let transition = transition_task_record(
                    &mut failed_record,
                    TaskTransition::Skip {
                        finished_at: timestamp(),
                        result_path: queued_task.store.worker_dir(&task_id).join("result.json"),
                        outcome_path: queued_task.store.worker_dir(&task_id).join("outcome.json"),
                        summary: model_unavailable_error.clone(),
                        failure_kind: Some(TaskFailureKind::ModelUnavailable),
                    },
                );
                write_task_record(&queued_task.store, &failed_record)?;
                append_task_lifecycle_event(&queued_task.store, &failed_record, Some(&transition))?;

                match queue_next_attempt(&mut failed_record, &mut queued_task) {
                    FallbackDecision::Queued => {
                        write_task_record(&queued_task.store, &failed_record)?;
                        append_task_lifecycle_event(&queued_task.store, &failed_record, None)?;
                        self.records.insert(task_id.clone(), failed_record);
                        continue;
                    }
                    FallbackDecision::Unavailable {
                        reason,
                        failure_kind,
                    } => {
                        failed_record.failure_kind = Some(failure_kind);
                        failed_record.retry_reason = Some(reason.clone());
                        if let Some(attempt) = failed_record.attempts.last_mut() {
                            attempt.retry_reason = Some(reason);
                        }
                        let (result, outcome) = write_model_unavailable_artifacts(
                            &queued_task.store,
                            &task_id,
                            &model_unavailable_error,
                        )?;
                        failed_record.result_path = Some(result.result_path.clone());
                        failed_record.outcome_path = Some(result.outcome_path.clone());
                        write_task_record(&queued_task.store, &failed_record)?;
                        append_task_lifecycle_event(&queued_task.store, &failed_record, None)?;
                        let run = ManagedWorkerRun {
                            result,
                            outcome,
                            record: failed_record.clone(),
                        };
                        self.completed_runs.insert(task_id.clone(), run);
                        self.records.insert(task_id.clone(), failed_record);
                        return Ok(());
                    }
                }
            }

            let handle = match self.registry.start(WorkerStartRequest {
                store: &queued_task.store,
                workspace: &queued_task.workspace,
                task: &queued_task.task,
                route_attempt: queued_task.route_attempt,
                goal: &queued_task.goal,
                verification_commands: &queued_task.verification_commands,
                config: &queued_task.config,
                cancellation_token: queued_task.cancellation_token.clone(),
                coordinator_model: queued_task.coordinator_model.as_ref(),
                coordinator_brief: queued_task.coordinator_brief.as_deref(),
                route_hint: queued_task.route_hint.as_deref(),
            }) {
                Ok(handle) => handle,
                Err(error) => {
                    let mut failed_record = self
                        .records
                        .remove(&task_id)
                        .context("missing task manager record for failed worker start")?;
                    let transition = transition_task_record(
                        &mut failed_record,
                        TaskTransition::Fail {
                            finished_at: timestamp(),
                            summary: "Worker task failed before producing an outcome.".to_string(),
                            failure_kind: TaskFailureKind::WorkerStartFailed,
                            error: Some(format!("{error:#}")),
                        },
                    );
                    write_task_record(&queued_task.store, &failed_record)?;
                    append_task_lifecycle_event(
                        &queued_task.store,
                        &failed_record,
                        Some(&transition),
                    )?;

                    match queue_next_attempt(&mut failed_record, &mut queued_task) {
                        FallbackDecision::Queued => {
                            write_task_record(&queued_task.store, &failed_record)?;
                            append_task_lifecycle_event(&queued_task.store, &failed_record, None)?;
                            self.records.insert(task_id.clone(), failed_record);
                            continue;
                        }
                        FallbackDecision::Unavailable {
                            reason,
                            failure_kind,
                        } => {
                            failed_record.failure_kind = Some(failure_kind);
                            failed_record.retry_reason = Some(reason.clone());
                            if let Some(attempt) = failed_record.attempts.last_mut() {
                                attempt.retry_reason = Some(reason);
                            }
                            write_task_record(&queued_task.store, &failed_record)?;
                            append_task_lifecycle_event(&queued_task.store, &failed_record, None)?;
                        }
                    }

                    self.records.insert(task_id.clone(), failed_record);
                    return Err(error);
                }
            };
            if let Some(record) = self.records.get_mut(&task_id) {
                let transition = transition_task_record(
                    record,
                    TaskTransition::Start {
                        session_id: handle.session_id(),
                    },
                );
                write_task_record(&queued_task.store, record)?;
                append_task_lifecycle_event(&queued_task.store, record, Some(&transition))?;
            }
            self.control
                .set_current(task_id.clone(), Arc::clone(&handle))?;
            let running_task = RunningTask {
                store: queued_task.store.clone(),
                handle,
                queued_task,
                started_at: Instant::now(),
            };
            self.running_tasks
                .insert(task_id.clone(), running_task.clone());
            self.dispatch_running_task(task_id, running_task);
            return Ok(());
        }
    }

    fn dispatch_running_task(&self, task_id: String, running_task: RunningTask) {
        let finished_task_tx = self.finished_task_tx.clone();
        std::thread::spawn(move || {
            let run_result = (|| -> Result<(WorkerOutcome, WorkerResult)> {
                let outcome = running_task.handle.wait_for_outcome()?;
                let result = running_task.handle.wait_for_result()?;
                Ok((outcome, result))
            })();
            if let Err(error) = finished_task_tx.send(FinishedTaskMessage {
                task_id,
                running_task,
                run_result,
            }) {
                eprintln!("failed to dispatch finished Gear worker task: {error}");
            }
        });
    }
}

fn is_terminal_status(status: &ManagedTaskStatus) -> bool {
    matches!(
        status,
        ManagedTaskStatus::Completed
            | ManagedTaskStatus::Failed
            | ManagedTaskStatus::Cancelled
            | ManagedTaskStatus::Interrupted
            | ManagedTaskStatus::Lost
            | ManagedTaskStatus::Skipped
    )
}

fn is_residency_transition(transition: &TaskTransition) -> bool {
    matches!(
        transition,
        TaskTransition::MarkResident
            | TaskTransition::Evict
            | TaskTransition::Dispose
            | TaskTransition::PersistOnly
            | TaskTransition::DetachRpc
    )
}

fn is_terminal_safe_transition(transition: &TaskTransition) -> bool {
    is_residency_transition(transition) || matches!(transition, TaskTransition::QueueRetry { .. })
}

fn apply_attempt_status(record: &mut TaskRecord, status: TaskAttemptStatus) {
    let finished_at = record.finished_at.clone();
    let session_id = record.session_id.clone();
    let result_path = record.result_path.clone();
    let outcome_path = record.outcome_path.clone();
    let summary = record.summary.clone();
    let failure_kind = record.failure_kind.clone();
    let error = record.error.clone();
    update_latest_attempt(record, |attempt| {
        attempt.status = status;
        attempt.finished_at = finished_at;
        attempt.session_id = session_id;
        attempt.result_path = result_path;
        attempt.outcome_path = outcome_path;
        attempt.summary = summary;
        attempt.failure_kind = failure_kind;
        attempt.error = error;
    });
}

fn transition_task_record(
    record: &mut TaskRecord,
    transition: TaskTransition,
) -> TaskTransitionResult {
    let previous_status = record.status.clone();
    let previous_residency_state = record.residency_state.clone();
    let transition_type = match &transition {
        TaskTransition::Start { .. } => "start",
        TaskTransition::Skip { .. } => "skip",
        TaskTransition::Complete { .. } => "complete",
        TaskTransition::Fail { .. } => "fail",
        TaskTransition::Cancel { .. } => "cancel",
        TaskTransition::Interrupt { .. } => "interrupt",
        TaskTransition::MarkLost { .. } => "mark_lost",
        TaskTransition::QueueRetry { .. } => "queue_retry",
        TaskTransition::MarkResident => "mark_resident",
        TaskTransition::Evict => "evict",
        TaskTransition::Dispose => "dispose",
        TaskTransition::PersistOnly => "persist_only",
        TaskTransition::DetachRpc => "detach_rpc",
    };

    if is_terminal_status(&record.status) && !is_terminal_safe_transition(&transition) {
        return TaskTransitionResult {
            applied: false,
            transition_type,
            previous_status,
            previous_residency_state,
        };
    }

    let applied = match transition {
        TaskTransition::Start { session_id } => {
            if record.status != ManagedTaskStatus::Pending {
                false
            } else {
                record.status = ManagedTaskStatus::Running;
                record.summary = "Worker task started.".to_string();
                record.failure_kind = None;
                record.retry_reason = None;
                record.error = None;
                record.session_id = session_id;
                apply_attempt_status(record, TaskAttemptStatus::Running);
                true
            }
        }
        TaskTransition::Skip {
            finished_at,
            result_path,
            outcome_path,
            summary,
            failure_kind,
        } => {
            record.status = ManagedTaskStatus::Skipped;
            record.finished_at = Some(finished_at);
            record.result_path = Some(result_path);
            record.outcome_path = Some(outcome_path);
            record.summary = summary;
            record.failure_kind = failure_kind;
            record.retry_reason = None;
            record.error = None;
            apply_attempt_status(record, TaskAttemptStatus::Skipped);
            true
        }
        TaskTransition::Complete {
            finished_at,
            result_path,
            outcome_path,
            summary,
            failure_kind,
        } => {
            record.status = ManagedTaskStatus::Completed;
            record.finished_at = Some(finished_at);
            record.result_path = Some(result_path);
            record.outcome_path = Some(outcome_path);
            record.summary = summary;
            record.failure_kind = failure_kind;
            record.retry_reason = None;
            record.error = None;
            apply_attempt_status(record, TaskAttemptStatus::Completed);
            true
        }
        TaskTransition::Fail {
            finished_at,
            summary,
            failure_kind,
            error,
        } => {
            record.status = ManagedTaskStatus::Failed;
            record.finished_at = Some(finished_at);
            record.summary = summary;
            record.failure_kind = Some(failure_kind);
            record.retry_reason = None;
            record.error = error;
            apply_attempt_status(record, TaskAttemptStatus::Failed);
            true
        }
        TaskTransition::Cancel {
            finished_at,
            summary,
            error,
        } => match record.status {
            ManagedTaskStatus::Pending | ManagedTaskStatus::Running => {
                record.status = ManagedTaskStatus::Cancelled;
                record.finished_at = Some(finished_at);
                record.summary = summary;
                record.failure_kind = Some(TaskFailureKind::WorkerCancelled);
                record.retry_reason = None;
                record.error = error;
                apply_attempt_status(record, TaskAttemptStatus::Cancelled);
                true
            }
            _ => false,
        },
        TaskTransition::Interrupt {
            finished_at,
            summary,
            error,
        } => {
            if record.status != ManagedTaskStatus::Running {
                false
            } else {
                record.status = ManagedTaskStatus::Interrupted;
                record.finished_at = Some(finished_at);
                record.summary = summary;
                record.failure_kind = Some(TaskFailureKind::WorkerCancelled);
                record.retry_reason = None;
                record.error = error;
                apply_attempt_status(record, TaskAttemptStatus::Interrupted);
                true
            }
        }
        TaskTransition::MarkLost {
            finished_at,
            summary,
            failure_kind,
            error,
            killed,
        } => match record.status {
            ManagedTaskStatus::Pending | ManagedTaskStatus::Running => {
                record.status = ManagedTaskStatus::Lost;
                record.finished_at = Some(finished_at);
                record.summary = summary;
                record.failure_kind = Some(failure_kind);
                record.retry_reason = None;
                record.error = error;
                record.killed = killed;
                apply_attempt_status(record, TaskAttemptStatus::Lost);
                true
            }
            _ => false,
        },
        TaskTransition::QueueRetry {
            summary,
            retry_reason,
        } => {
            record.status = ManagedTaskStatus::Pending;
            record.finished_at = None;
            record.session_id = None;
            record.result_path = None;
            record.outcome_path = None;
            record.summary = summary;
            record.failure_kind = None;
            record.retry_reason = Some(retry_reason.clone());
            record.error = None;
            true
        }
        TaskTransition::MarkResident => {
            record.residency_state = ResidencyState::Resident;
            true
        }
        TaskTransition::Evict => {
            record.residency_state = ResidencyState::Evicted;
            true
        }
        TaskTransition::Dispose => {
            record.residency_state = ResidencyState::Disposed;
            true
        }
        TaskTransition::PersistOnly => {
            record.residency_state = ResidencyState::PersistedOnly;
            true
        }
        TaskTransition::DetachRpc => {
            record.residency_state = ResidencyState::RpcDetached;
            true
        }
    };

    TaskTransitionResult {
        applied,
        transition_type,
        previous_status,
        previous_residency_state,
    }
}

fn model_unavailable_error_for_task(
    config: &WorkerConfig,
    queued_task: &QueuedTask,
) -> Option<String> {
    let selected_route = config
        .selected_route_for_hint(queued_task.route_attempt, queued_task.route_hint.as_deref());
    let worker_model = selected_route.worker_model?;
    let provider_qualified_model = selected_route
        .worker_kind
        .provider_id_hint()
        .map(|provider_id| format!("{provider_id}/{worker_model}"));
    config
        .unavailable_worker_models
        .iter()
        .any(|unavailable_model| {
            unavailable_model.eq_ignore_ascii_case(worker_model.trim())
                || provider_qualified_model
                    .as_ref()
                    .is_some_and(|qualified_model| {
                        unavailable_model.eq_ignore_ascii_case(qualified_model)
                    })
        })
        .then(|| {
            format!(
                "Worker model `{worker_model}` is unavailable for `{}`.",
                selected_route.worker_kind.as_str()
            )
        })
}

fn write_model_unavailable_artifacts(
    store: &StateStore,
    task_id: &str,
    summary: &str,
) -> Result<(WorkerResult, WorkerOutcome)> {
    let packet_path = store.write_worker_file(
        task_id,
        "packet.json",
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&serde_json::json!({
                "task_id": task_id,
                "status": "skipped",
                "summary": summary,
            }))?
        ),
    )?;
    let prompt_path = store.write_worker_file(task_id, "prompt.md", summary)?;
    let result_path = store.worker_dir(task_id).join("result.json");
    let outcome_path = store.worker_dir(task_id).join("outcome.json");
    let result = WorkerResult {
        status: WorkerStatus::Skipped,
        command: None,
        exit_code: None,
        summary: summary.to_string(),
        packet_path,
        prompt_path,
        stdout_path: None,
        stderr_path: None,
        last_message_path: None,
        result_path,
        outcome_path: outcome_path.clone(),
    };
    let outcome = WorkerOutcome {
        status: WorkerStatus::Skipped,
        session_id: None,
        summary: summary.to_string(),
        changed_files: Vec::new(),
        commands_run: Vec::new(),
        known_failures: vec![summary.to_string()],
        raw_output_path: None,
        command: None,
        exit_code: None,
    };
    store.write_worker_file(
        task_id,
        "result.json",
        &format!("{}\n", serde_json::to_string_pretty(&result)?),
    )?;
    store.write_worker_file(
        task_id,
        "outcome.json",
        &format!("{}\n", serde_json::to_string_pretty(&outcome)?),
    )?;
    Ok((result, outcome))
}

fn queued_task_from_request(request: WorkerStartRequest<'_>) -> QueuedTask {
    let mut task = request.task.clone();
    task.attempt = 1;
    QueuedTask {
        store: request.store.clone(),
        workspace: request.workspace.to_path_buf(),
        task,
        route_attempt: 1,
        goal: request.goal.to_string(),
        verification_commands: request.verification_commands.to_vec(),
        config: request.config.clone(),
        cancellation_token: request.cancellation_token,
        coordinator_model: request.coordinator_model.cloned(),
        coordinator_brief: request.coordinator_brief.map(ToString::to_string),
        route_hint: request.route_hint.map(ToString::to_string),
    }
}

fn queue_next_attempt(record: &mut TaskRecord, queued_task: &mut QueuedTask) -> FallbackDecision {
    if let Some(failure_kind) = record
        .attempts
        .last()
        .and_then(|attempt| attempt.failure_kind.clone())
    {
        let same_failure_count = record
            .attempts
            .iter()
            .filter(|attempt| attempt.failure_kind.as_ref() == Some(&failure_kind))
            .count();
        if same_failure_count >= MAX_SAME_FAILURE_RETRIES {
            return FallbackDecision::Unavailable {
                reason: format!(
                    "same failure kind `{failure_kind:?}` reached retry limit {MAX_SAME_FAILURE_RETRIES}"
                ),
                failure_kind: TaskFailureKind::RepeatedFailureLimit,
            };
        }
    }

    maybe_append_failure_upgrade_route(record, queued_task);

    let Some(previous_attempt) = record.attempts.last() else {
        return FallbackDecision::Unavailable {
            reason: "missing previous attempt".to_string(),
            failure_kind: TaskFailureKind::NoFallbackRoute,
        };
    };

    let next_attempt = queued_task.task.attempt.saturating_add(1);
    let route_selection_attempt = queued_task
        .route_hint
        .as_deref()
        .filter(|route_hint| *route_hint != previous_attempt.worker_category)
        .map(|_| 1)
        .unwrap_or(next_attempt);
    let selected_route = queued_task
        .config
        .selected_route_for_hint(route_selection_attempt, queued_task.route_hint.as_deref());
    let worker_kind = selected_route.worker_kind.as_str().to_string();
    let worker_command = selected_route.worker_command.map(ToString::to_string);
    let worker_model = selected_route.worker_model.map(ToString::to_string);
    if previous_attempt.worker_kind == worker_kind
        && previous_attempt.worker_command == worker_command
        && previous_attempt.worker_model == worker_model
    {
        return FallbackDecision::Unavailable {
            reason: format!(
                "no different fallback route after `{}` attempt {}",
                previous_attempt.worker_kind, previous_attempt.attempt
            ),
            failure_kind: TaskFailureKind::NoFallbackRoute,
        };
    }
    if selected_route.worker_kind.is_premium() {
        let used_premium_attempts = record
            .attempts
            .iter()
            .filter(|attempt| {
                WorkerKind::parse(&attempt.worker_kind).is_some_and(|worker_kind| {
                    worker_kind.is_premium() && attempt.status != TaskAttemptStatus::Pending
                })
            })
            .count();
        if used_premium_attempts >= queued_task.config.premium_worker_budget {
            return FallbackDecision::Unavailable {
                reason: format!(
                    "premium worker budget {} exhausted before `{}` attempt {}",
                    queued_task.config.premium_worker_budget,
                    selected_route.worker_kind.as_str(),
                    next_attempt
                ),
                failure_kind: TaskFailureKind::PremiumBudgetExceeded,
            };
        }
    }

    queued_task.task.attempt = next_attempt;
    queued_task.route_attempt = route_selection_attempt;
    let worker_category = selected_route.category.as_str().to_string();
    let route_reason = selected_route.route_reason.clone();
    let route_hint = queued_task.route_hint.clone();
    let started_at = timestamp();
    let retry_reason = format!(
        "retrying after {:?} with `{}` via {}",
        previous_attempt
            .failure_kind
            .clone()
            .unwrap_or(TaskFailureKind::WorkerFailed),
        worker_kind,
        route_reason
    );
    record.worker_kind = worker_kind.clone();
    record.worker_command = worker_command.clone();
    record.worker_model = worker_model.clone();
    record.worker_category = worker_category.clone();
    record.route_hint = route_hint.clone();
    record.route_reason = route_reason.clone();
    let _ = transition_task_record(
        record,
        TaskTransition::QueueRetry {
            summary: format!("Worker fallback attempt {next_attempt} queued."),
            retry_reason: retry_reason.clone(),
        },
    );
    record.attempts.push(TaskAttempt {
        attempt: next_attempt,
        worker_kind,
        worker_command,
        worker_model,
        worker_category,
        route_hint,
        route_reason,
        status: TaskAttemptStatus::Pending,
        started_at,
        finished_at: None,
        session_id: None,
        result_path: None,
        outcome_path: None,
        summary: format!("Worker fallback attempt {next_attempt} queued."),
        failure_kind: None,
        retry_reason: Some(retry_reason),
        error: None,
    });
    FallbackDecision::Queued
}

fn maybe_append_failure_upgrade_route(record: &TaskRecord, queued_task: &mut QueuedTask) {
    if queued_task.route_hint.is_none() {
        return;
    }
    let Some(previous_attempt) = record.attempts.last() else {
        return;
    };
    let Some(failure_kind) = previous_attempt.failure_kind.as_ref() else {
        return;
    };
    if !matches!(
        failure_kind,
        TaskFailureKind::WorkerFailed
            | TaskFailureKind::WorkerStartFailed
            | TaskFailureKind::WorkerUnavailable
            | TaskFailureKind::ModelUnavailable
    ) {
        return;
    }

    let candidate_worker_kind = match WorkerKind::parse(&previous_attempt.worker_kind) {
        Some(WorkerKind::Opencode | WorkerKind::OpencodeSession) => WorkerKind::Codex,
        _ => return,
    };
    if queued_task
        .config
        .worker_routes
        .iter()
        .any(|route| route.worker_kind == candidate_worker_kind)
    {
        queued_task.route_hint = Some("deep".to_string());
        return;
    }
    let Some(worker_command) = candidate_worker_kind.default_command(None) else {
        return;
    };
    queued_task
        .config
        .worker_routes
        .push(crate::workers::WorkerRoute {
            worker_kind: candidate_worker_kind,
            worker_command: Some(worker_command),
            worker_model: None,
        });
    queued_task.route_hint = Some("deep".to_string());
}

fn failure_kind_from_worker_result(
    result: &WorkerResult,
    outcome: &WorkerOutcome,
) -> Option<TaskFailureKind> {
    match result.status {
        WorkerStatus::Succeeded => None,
        WorkerStatus::Skipped => {
            if result
                .summary
                .to_ascii_lowercase()
                .contains("no worker command")
            {
                Some(TaskFailureKind::WorkerUnavailable)
            } else {
                None
            }
        }
        WorkerStatus::Failed => {
            if outcome.known_failures.iter().any(|failure| {
                failure.to_ascii_lowercase().contains("cancelled")
                    || failure.to_ascii_lowercase().contains("canceled")
            }) {
                Some(TaskFailureKind::WorkerCancelled)
            } else {
                Some(TaskFailureKind::WorkerFailed)
            }
        }
    }
}

fn should_retry_worker_result(
    record: &TaskRecord,
    queued_task: &QueuedTask,
    result: &WorkerResult,
) -> bool {
    if result.status == WorkerStatus::Failed {
        return true;
    }

    record.failure_kind == Some(TaskFailureKind::WorkerUnavailable)
        && (!queued_task.config.worker_routes.is_empty() || queued_task.config.require_worker)
}

fn concurrency_key_for_task(queued_task: &QueuedTask) -> String {
    let selected_route = queued_task
        .config
        .selected_route_for_hint(queued_task.route_attempt, queued_task.route_hint.as_deref());
    let model_key = queued_task
        .coordinator_model
        .as_ref()
        .map(|model| format!("{}:{}", model.provider_id, model.model_id))
        .unwrap_or_else(|| "unconfigured".to_string());
    format!("{}:{model_key}", selected_route.worker_kind.as_str())
}

fn update_latest_attempt(record: &mut TaskRecord, update: impl FnOnce(&mut TaskAttempt)) {
    if let Some(attempt) = record.attempts.last_mut() {
        update(attempt);
    }
}

fn write_task_record(store: &StateStore, task_record: &TaskRecord) -> Result<PathBuf> {
    let json =
        serde_json::to_string_pretty(task_record).context("failed to serialize task record")?;
    store.write_worker_file(
        &task_record.task_id,
        "task-record.json",
        &format!("{json}\n"),
    )
}

fn append_task_lifecycle_event(
    store: &StateStore,
    task_record: &TaskRecord,
    transition: Option<&TaskTransitionResult>,
) -> Result<PathBuf> {
    let event = TaskLifecycleEvent {
        task_id: task_record.task_id.clone(),
        status: task_record.status.clone(),
        residency_state: task_record.residency_state.clone(),
        timestamp: timestamp(),
        transition_type: transition.map(|transition| transition.transition_type.to_string()),
        transition_applied: transition
            .map(|transition| transition.applied)
            .unwrap_or(true),
        previous_status: transition.map(|transition| transition.previous_status.clone()),
        previous_residency_state: transition
            .map(|transition| transition.previous_residency_state.clone()),
        run_epoch: task_record.run_epoch,
        summary: task_record.summary.clone(),
    };
    let json = serde_json::to_string(&event).context("failed to serialize task lifecycle event")?;
    let worker_dir = store.worker_dir(&task_record.task_id);
    fs::create_dir_all(&worker_dir)
        .with_context(|| format!("failed to create {}", worker_dir.display()))?;
    let path = worker_dir.join("task-events.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    writeln!(file, "{json}").with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;

    use super::*;
    use crate::state::{Scope, Task, TaskInputs, TaskKind, TaskOutputs, TaskStatus};
    use crate::workers::{WorkerConfig, WorkerKind, WorkerRoute};

    fn test_task(id: &str) -> Task {
        Task {
            id: id.to_string(),
            goal_id: "goal_test".to_string(),
            title: "test managed task".to_string(),
            kind: TaskKind::Edit,
            status: TaskStatus::Pending,
            assigned_worker: Some("opencode".to_string()),
            attempt: 1,
            scope: Scope::new(Vec::new(), Vec::new(), 10),
            inputs: TaskInputs::default(),
            outputs: TaskOutputs::default(),
        }
    }

    fn test_task_record(
        task_id: &str,
        status: ManagedTaskStatus,
        attempt_status: TaskAttemptStatus,
    ) -> TaskRecord {
        TaskRecord {
            task_id: task_id.to_string(),
            worker_kind: "opencode".to_string(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".to_string(),
            route_hint: None,
            route_reason: "test route".to_string(),
            status,
            started_at: timestamp(),
            finished_at: None,
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            result_path: None,
            outcome_path: None,
            summary: "Worker task started.".to_string(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".to_string(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".to_string(),
                route_hint: None,
                route_reason: "test route".to_string(),
                status: attempt_status,
                started_at: timestamp(),
                finished_at: None,
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "Worker task started.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        }
    }

    #[test]
    fn task_manager_records_skipped_worker_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_skipped");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: true,
            require_worker: false,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Skipped);
        assert_eq!(run.result.status, WorkerStatus::Skipped);
        assert!(store.worker_dir(&task.id).join("task-record.json").exists());
        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "skipped""#));
        assert!(record.contains(r#""attempts""#));
        assert!(record.contains(r#""worker_category": "quick""#));
        Ok(())
    }

    #[test]
    fn task_manager_records_failed_worker_outcome() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_failed");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'exit 2'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Failed);
        assert_eq!(run.result.status, WorkerStatus::Failed);
        assert!(run.record.finished_at.is_some());
        assert_eq!(
            run.record.failure_kind,
            Some(TaskFailureKind::NoFallbackRoute)
        );
        assert_eq!(run.record.attempts.len(), 1);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Failed);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerFailed)
        );
        assert!(run.record.attempts[0].retry_reason.is_some());
        Ok(())
    }

    #[test]
    fn task_manager_fallback_retries_failed_worker_result() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_fallback_result");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("sh -c 'exit 2'".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("printf fallback-ok".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.worker_kind, "codex");
        assert_eq!(run.record.failure_kind, None);
        assert_eq!(run.record.retry_reason, None);
        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(run.record.attempts[0].worker_kind, "opencode");
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Failed);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerFailed)
        );
        assert!(run.record.attempts[0].retry_reason.is_none());
        assert_eq!(run.record.attempts[1].worker_kind, "codex");
        assert_eq!(run.record.attempts[1].status, TaskAttemptStatus::Completed);
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("retrying after WorkerFailed"))
        );
        let events = fs::read_to_string(store.worker_dir(&task.id).join("task-events.jsonl"))?;
        assert!(events.contains(r#""status":"failed""#));
        assert!(events.contains(r#"Worker fallback attempt 2 queued."#));
        Ok(())
    }

    #[test]
    fn queue_next_attempt_upgrades_non_premium_failure_to_codex_route() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_upgrade_to_codex");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'exit 2'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair".to_string()),
        };
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id: task.id.clone(),
            worker_kind: "opencode".to_string(),
            worker_command: Some("sh -c 'exit 2'".to_string()),
            worker_model: None,
            worker_category: "repair".to_string(),
            route_hint: Some("repair".to_string()),
            route_reason: "initial route".to_string(),
            status: ManagedTaskStatus::Failed,
            started_at: started_at.clone(),
            finished_at: Some(timestamp()),
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            result_path: None,
            outcome_path: None,
            summary: "failed".to_string(),
            failure_kind: Some(TaskFailureKind::WorkerFailed),
            retry_reason: None,
            error: Some("exit 2".to_string()),
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".to_string(),
                worker_command: Some("sh -c 'exit 2'".to_string()),
                worker_model: None,
                worker_category: "repair".to_string(),
                route_hint: Some("repair".to_string()),
                route_reason: "initial route".to_string(),
                status: TaskAttemptStatus::Failed,
                started_at,
                finished_at: Some(timestamp()),
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "failed".to_string(),
                failure_kind: Some(TaskFailureKind::WorkerFailed),
                retry_reason: None,
                error: Some("exit 2".to_string()),
            }],
        };

        let decision = queue_next_attempt(&mut record, &mut queued_task);

        assert_eq!(decision, FallbackDecision::Queued);
        assert_eq!(queued_task.route_hint.as_deref(), Some("deep"));
        assert_eq!(record.attempts.len(), 2);
        assert_eq!(record.attempts[1].worker_kind, "codex");
        assert_eq!(record.attempts[1].worker_category, "deep");
        assert!(
            record.attempts[1]
                .worker_command
                .as_deref()
                .is_some_and(|command| command.contains("codex exec"))
        );
        Ok(())
    }

    #[test]
    fn task_manager_fallback_retries_unavailable_worker() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_fallback_unavailable");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: None,
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("printf fallback-ok".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.worker_kind, "codex");
        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Skipped);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerUnavailable)
        );
        assert_eq!(run.record.attempts[1].status, TaskAttemptStatus::Completed);
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("WorkerUnavailable"))
        );
        Ok(())
    }

    #[test]
    fn queue_next_attempt_stops_when_premium_budget_is_exhausted() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_premium_budget_exhausted");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf opencode".to_string()),
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("printf codex".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Claude,
                    worker_command: Some("printf claude".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut queued_task = QueuedTask {
            store,
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        queued_task.task.attempt = 1;
        let started_at = timestamp();
        let mut record = TaskRecord {
            task_id: task.id.clone(),
            worker_kind: "codex".to_string(),
            worker_command: Some("printf codex".to_string()),
            worker_model: None,
            worker_category: "deep".to_string(),
            route_hint: None,
            route_reason: "attempt 1 selected sequence route `codex`".to_string(),
            status: ManagedTaskStatus::Failed,
            started_at: started_at.clone(),
            finished_at: Some(timestamp()),
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            result_path: None,
            outcome_path: None,
            summary: "failed".to_string(),
            failure_kind: Some(TaskFailureKind::WorkerFailed),
            retry_reason: None,
            error: Some("exit 2".to_string()),
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "codex".to_string(),
                worker_command: Some("printf codex".to_string()),
                worker_model: None,
                worker_category: "deep".to_string(),
                route_hint: None,
                route_reason: "attempt 1 selected sequence route `codex`".to_string(),
                status: TaskAttemptStatus::Failed,
                started_at,
                finished_at: Some(timestamp()),
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "failed".to_string(),
                failure_kind: Some(TaskFailureKind::WorkerFailed),
                retry_reason: None,
                error: Some("exit 2".to_string()),
            }],
        };

        let decision = queue_next_attempt(&mut record, &mut queued_task);

        assert_eq!(
            decision,
            FallbackDecision::Unavailable {
                reason: "premium worker budget 1 exhausted before `claude` attempt 2".to_string(),
                failure_kind: TaskFailureKind::PremiumBudgetExceeded,
            }
        );
        Ok(())
    }

    #[test]
    fn task_manager_marks_missing_worker_binary_as_unavailable() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_missing_binary");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some("__gearbox_missing_worker_command__ exec".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(run.record.attempts.len(), 1);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Skipped);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::WorkerUnavailable)
        );
        assert_eq!(
            run.record.failure_kind,
            Some(TaskFailureKind::NoFallbackRoute)
        );
        Ok(())
    }

    #[test]
    fn task_manager_fallback_retries_unavailable_worker_model() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_fallback_model_unavailable");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("printf should-not-run".to_string()),
                    worker_model: Some("slow-model".to_string()),
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("printf model-fallback-ok".to_string()),
                    worker_model: Some("fast-model".to_string()),
                },
            ],
            unavailable_worker_models: vec!["slow-model".to_string()],
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("repair"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.worker_kind, "codex");
        assert_eq!(run.record.worker_model.as_deref(), Some("fast-model"));
        assert_eq!(run.record.attempts.len(), 2);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Skipped);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::ModelUnavailable)
        );
        assert_eq!(
            run.record.attempts[0].worker_model.as_deref(),
            Some("slow-model")
        );
        assert_eq!(run.record.attempts[1].status, TaskAttemptStatus::Completed);
        assert_eq!(
            run.record.attempts[1].worker_model.as_deref(),
            Some("fast-model")
        );
        assert!(
            run.record.attempts[1]
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("ModelUnavailable"))
        );
        Ok(())
    }

    #[test]
    fn task_manager_treats_provider_qualified_model_as_unavailable() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_provider_qualified_model_unavailable");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some("printf should-not-run".to_string()),
            worker_model: Some("gpt-5".to_string()),
            worker_routes: Vec::new(),
            unavailable_worker_models: vec!["openai/gpt-5".to_string()],
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("deep"),
        })?;

        assert_eq!(run.record.attempts.len(), 1);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Skipped);
        assert_eq!(
            run.record.attempts[0].failure_kind,
            Some(TaskFailureKind::ModelUnavailable)
        );
        assert_eq!(
            run.record.failure_kind,
            Some(TaskFailureKind::NoFallbackRoute)
        );
        Ok(())
    }

    #[test]
    fn task_manager_stops_after_repeated_same_failure_limit() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_repeated_failure_limit");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: vec![
                WorkerRoute {
                    worker_kind: WorkerKind::Codex,
                    worker_command: Some("sh -c 'exit 2'".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Claude,
                    worker_command: Some("sh -c 'exit 3'".to_string()),
                    worker_model: None,
                },
                WorkerRoute {
                    worker_kind: WorkerKind::Opencode,
                    worker_command: Some("printf should-not-run".to_string()),
                    worker_model: None,
                },
            ],
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 2,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        let run = manager.run_worker_task(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: Some("deep"),
        })?;

        assert_eq!(run.record.status, ManagedTaskStatus::Failed);
        assert_eq!(
            run.record.failure_kind,
            Some(TaskFailureKind::RepeatedFailureLimit)
        );
        assert_eq!(run.record.attempts.len(), MAX_SAME_FAILURE_RETRIES);
        assert_eq!(run.record.attempts[0].worker_kind, "codex");
        assert_eq!(run.record.attempts[1].worker_kind, "claude");
        assert!(
            run.record
                .retry_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("retry limit"))
        );
        Ok(())
    }

    #[test]
    fn task_manager_start_dispatches_worker_in_background_until_wait() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_deferred");
        let release_path = temp_dir.path().join("release-worker");
        let worker_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo worker-ok'",
            release_path.display()
        );
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(worker_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        let task_id = manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert_eq!(task_id, task.id);
        assert!(store.worker_dir(&task.id).join("packet.json").exists());
        assert!(store.worker_dir(&task.id).join("prompt.md").exists());
        assert!(!store.worker_dir(&task.id).join("result.json").exists());

        fs::write(&release_path, "go")?;
        let run = manager.wait_for(&task.id)?;
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.record.attempts[0].status, TaskAttemptStatus::Completed);
        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        assert!(store.worker_dir(&task.id).join("result.json").exists());
        let events = fs::read_to_string(store.worker_dir(&task.id).join("task-events.jsonl"))?;
        assert!(events.contains(r#""status":"pending""#));
        assert!(events.contains(r#""status":"running""#));
        assert!(events.contains(r#""status":"completed""#));
        Ok(())
    }

    #[test]
    fn task_manager_tick_settles_finished_worker_without_wait_for() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_tick_settles");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo tick-ok".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        let mut settled = 0;
        for _ in 0..50 {
            settled += manager.tick()?;
            if settled > 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(settled > 0);
        let record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == task.id)
            .context("missing settled task record")?;
        assert_eq!(record.status, ManagedTaskStatus::Completed);

        let run = manager.wait_for(&task.id)?;
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        Ok(())
    }

    #[test]
    fn task_manager_tick_loop_settles_finished_worker_in_background() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_tick_loop_settles");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo loop-ok".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let manager = TaskManager::new().into_shared();
        let tick_loop = TaskManagerTickLoop::start(manager.clone(), Duration::from_millis(10));

        manager
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
            .start(WorkerStartRequest {
                store: &store,
                workspace: temp_dir.path(),
                task: &task,
                route_attempt: 1,
                goal: "test goal",
                verification_commands: &[],
                config: &config,
                cancellation_token: None,
                coordinator_model: None,
                coordinator_brief: None,
                route_hint: None,
            })?;

        let mut completed = false;
        for _ in 0..50 {
            completed = manager
                .lock()
                .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
                .list()
                .into_iter()
                .any(|record| {
                    record.task_id == task.id && record.status == ManagedTaskStatus::Completed
                });
            if completed {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        tick_loop.stop()?;
        assert!(completed);

        let run = manager
            .lock()
            .map_err(|_| anyhow::anyhow!("task manager mutex poisoned"))?
            .wait_for(&task.id)?;
        assert_eq!(run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(run.result.status, WorkerStatus::Succeeded);
        Ok(())
    }

    struct FakeOutputHandle;

    impl WorkerSessionHandle for FakeOutputHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_fake".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            bail!("not supported")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            bail!("not supported")
        }

        fn last_output(&self) -> Option<String> {
            Some("control-output".to_string())
        }
    }

    struct FakeInterruptHandle {
        interrupted: Arc<AtomicUsize>,
        follow_ups: Arc<Mutex<Vec<String>>>,
        steers: Arc<Mutex<Vec<String>>>,
    }

    impl WorkerSessionHandle for FakeInterruptHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_interrupt".to_string())
        }

        fn send_follow_up(&self, prompt: String) -> Result<()> {
            self.follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("follow-up mutex poisoned"))?
                .push(prompt);
            Ok(())
        }

        fn steer(&self, prompt: String) -> Result<()> {
            self.steers
                .lock()
                .map_err(|_| anyhow::anyhow!("steer mutex poisoned"))?
                .push(prompt);
            Ok(())
        }

        fn interrupt(&self) -> Result<()> {
            self.interrupted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            bail!("not supported")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            bail!("not supported")
        }

        fn last_output(&self) -> Option<String> {
            None
        }
    }

    struct FakeHangingHandle;

    impl WorkerSessionHandle for FakeHangingHandle {
        fn session_id(&self) -> Option<String> {
            Some("session_hanging".to_string())
        }

        fn send_follow_up(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn steer(&self, _prompt: String) -> Result<()> {
            bail!("not supported")
        }

        fn interrupt(&self) -> Result<()> {
            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            Ok(())
        }

        fn wait_for_outcome(&self) -> Result<WorkerOutcome> {
            std::thread::sleep(Duration::from_secs(60));
            bail!("timed out")
        }

        fn wait_for_result(&self) -> Result<WorkerResult> {
            std::thread::sleep(Duration::from_secs(60));
            bail!("timed out")
        }

        fn last_output(&self) -> Option<String> {
            None
        }
    }

    #[test]
    fn task_manager_control_reads_current_worker_last_output() -> Result<()> {
        let control = TaskManagerControl::default();
        control.set_current("task_fake".to_string(), Arc::new(FakeOutputHandle))?;

        assert_eq!(control.current_task_id()?.as_deref(), Some("task_fake"));
        assert_eq!(
            control.current_last_output()?.as_deref(),
            Some("control-output")
        );
        control.clear_current("task_fake")?;
        assert_eq!(control.current_last_output()?, None);
        Ok(())
    }

    #[test]
    fn task_manager_control_interrupts_current_worker() -> Result<()> {
        let control = TaskManagerControl::default();
        let interrupted = Arc::new(AtomicUsize::new(0));
        let follow_ups = Arc::new(Mutex::new(Vec::new()));
        let steers = Arc::new(Mutex::new(Vec::new()));
        control.set_current(
            "task_interrupt".to_string(),
            Arc::new(FakeInterruptHandle {
                interrupted: interrupted.clone(),
                follow_ups: follow_ups.clone(),
                steers: steers.clone(),
            }),
        )?;

        assert!(control.send_follow_up_current_task("continue".to_string())?);
        assert!(control.steer_current_task("adjust".to_string())?);
        assert!(control.interrupt_current_task()?);
        assert_eq!(interrupted.load(Ordering::SeqCst), 1);
        assert_eq!(
            follow_ups
                .lock()
                .map_err(|_| anyhow::anyhow!("follow-up mutex poisoned"))?
                .as_slice(),
            ["continue"]
        );
        assert_eq!(
            steers
                .lock()
                .map_err(|_| anyhow::anyhow!("steer mutex poisoned"))?
                .as_slice(),
            ["adjust"]
        );
        assert!(control.send_follow_up_task("task_interrupt", "continue 2".to_string())?);
        assert!(control.steer_task("task_interrupt", "adjust 2".to_string())?);
        assert!(control.interrupt_task("task_interrupt")?);
        assert_eq!(interrupted.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn release_guard_is_epoch_scoped() {
        let mut release_guard = ReleaseGuard::default();

        assert!(release_guard.release_once("task_epoch", 0));
        assert!(!release_guard.release_once("task_epoch", 0));
        assert!(release_guard.release_once("task_epoch", 1));
        release_guard.forget_task("task_epoch");
        assert!(release_guard.release_once("task_epoch", 0));
    }

    #[test]
    fn task_manager_snapshot_exposes_queue_state_for_gui_observers() -> Result<()> {
        let control = TaskManagerControl::default();
        control.set_current("task_snapshot".to_string(), Arc::new(FakeOutputHandle))?;
        let mut manager = TaskManager::with_control(control);
        manager.records.insert(
            "task_snapshot".to_string(),
            TaskRecord {
                task_id: "task_snapshot".to_string(),
                worker_kind: "opencode".to_string(),
                worker_command: None,
                worker_model: None,
                worker_category: "repair".to_string(),
                route_hint: Some("repair".to_string()),
                route_reason: "test route".to_string(),
                status: ManagedTaskStatus::Running,
                started_at: timestamp(),
                finished_at: None,
                residency_state: ResidencyState::Resident,
                run_epoch: 0,
                notified_epoch: default_notified_epoch(),
                notification_failed_epoch: None,
                killed: false,
                session_id: Some("session_fake".to_string()),
                parent_session_id: None,
                root_session_id: None,
                result_path: Some(PathBuf::from("/tmp/task-result.json")),
                outcome_path: Some(PathBuf::from("/tmp/task-outcome.json")),
                summary: "Worker task started.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
                attempts: vec![TaskAttempt {
                    attempt: 1,
                    worker_kind: "opencode".to_string(),
                    worker_command: None,
                    worker_model: None,
                    worker_category: "repair".to_string(),
                    route_hint: Some("repair".to_string()),
                    route_reason: "test route".to_string(),
                    status: TaskAttemptStatus::Running,
                    started_at: timestamp(),
                    finished_at: None,
                    session_id: Some("session_fake".to_string()),
                    result_path: Some(PathBuf::from("/tmp/attempt-result.json")),
                    outcome_path: Some(PathBuf::from("/tmp/attempt-outcome.json")),
                    summary: "Worker task started.".to_string(),
                    failure_kind: None,
                    retry_reason: None,
                    error: None,
                }],
            },
        );

        let snapshot = manager.snapshot()?;

        assert_eq!(snapshot.counts.running, 1);
        assert_eq!(snapshot.counts.pending, 0);
        assert_eq!(snapshot.current_output.as_deref(), Some("control-output"));
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].task_id, "task_snapshot");
        assert_eq!(snapshot.tasks[0].attempts.len(), 1);
        assert_eq!(
            snapshot.tasks[0].attempts[0].outcome_path.as_deref(),
            Some(std::path::Path::new("/tmp/attempt-outcome.json"))
        );
        Ok(())
    }

    #[test]
    fn task_manager_snapshot_counts_interrupted_and_lost_tasks() -> Result<()> {
        let mut manager = TaskManager::new();
        manager.records.insert(
            "task_interrupted".to_string(),
            test_task_record(
                "task_interrupted",
                ManagedTaskStatus::Interrupted,
                TaskAttemptStatus::Interrupted,
            ),
        );
        manager.records.insert(
            "task_lost".to_string(),
            test_task_record(
                "task_lost",
                ManagedTaskStatus::Lost,
                TaskAttemptStatus::Lost,
            ),
        );

        let snapshot = manager.snapshot()?;

        assert_eq!(snapshot.counts.interrupted, 1);
        assert_eq!(snapshot.counts.lost, 1);
        assert_eq!(snapshot.tasks.len(), 2);
        Ok(())
    }

    #[test]
    fn late_finished_message_after_forget_is_ignored() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_forgotten");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf ignored".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let running_task = RunningTask {
            store: store.clone(),
            handle: Arc::new(FakeOutputHandle),
            queued_task,
            started_at: Instant::now(),
        };
        let result = WorkerResult {
            status: WorkerStatus::Succeeded,
            command: None,
            exit_code: Some(0),
            summary: "late result".to_string(),
            packet_path: store.worker_dir(&task.id).join("packet.json"),
            prompt_path: store.worker_dir(&task.id).join("prompt.md"),
            stdout_path: None,
            stderr_path: None,
            last_message_path: None,
            result_path: store.worker_dir(&task.id).join("result.json"),
            outcome_path: store.worker_dir(&task.id).join("outcome.json"),
        };
        let outcome = WorkerOutcome {
            status: WorkerStatus::Succeeded,
            session_id: None,
            summary: "late outcome".to_string(),
            changed_files: Vec::new(),
            commands_run: Vec::new(),
            known_failures: Vec::new(),
            raw_output_path: None,
            command: None,
            exit_code: Some(0),
        };
        let mut manager = TaskManager::new();
        manager.records.insert(
            task.id.clone(),
            test_task_record(
                &task.id,
                ManagedTaskStatus::Running,
                TaskAttemptStatus::Running,
            ),
        );
        manager
            .running_tasks
            .insert(task.id.clone(), running_task.clone());

        manager.forget_task(&task.id)?;
        manager.settle_finished_task(FinishedTaskMessage {
            task_id: task.id.clone(),
            running_task,
            run_result: Ok((outcome, result)),
        })?;

        assert!(!manager.records.contains_key(&task.id));
        assert!(!manager.completed_runs.contains_key(&task.id));
        assert!(!manager.completed_errors.contains_key(&task.id));
        assert!(manager.running_tasks.is_empty());
        Ok(())
    }

    #[test]
    fn task_manager_wait_for_does_not_hang_on_stale_running_task() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_stale_running");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf noop".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let started_at = timestamp();
        let queued_task = QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: task.clone(),
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        };
        let mut manager = TaskManager::new();
        manager.records.insert(
            task.id.clone(),
            TaskRecord {
                task_id: task.id.clone(),
                worker_kind: "opencode".to_string(),
                worker_command: Some("printf noop".to_string()),
                worker_model: None,
                worker_category: "quick".to_string(),
                route_hint: None,
                route_reason: "test route".to_string(),
                status: ManagedTaskStatus::Running,
                started_at: started_at.clone(),
                finished_at: None,
                residency_state: ResidencyState::Resident,
                run_epoch: 0,
                notified_epoch: default_notified_epoch(),
                notification_failed_epoch: None,
                killed: false,
                session_id: Some("session_hanging".to_string()),
                parent_session_id: None,
                root_session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "Worker task started.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
                attempts: vec![TaskAttempt {
                    attempt: 1,
                    worker_kind: "opencode".to_string(),
                    worker_command: Some("printf noop".to_string()),
                    worker_model: None,
                    worker_category: "quick".to_string(),
                    route_hint: None,
                    route_reason: "test route".to_string(),
                    status: TaskAttemptStatus::Running,
                    started_at,
                    finished_at: None,
                    session_id: Some("session_hanging".to_string()),
                    result_path: None,
                    outcome_path: None,
                    summary: "Worker task started.".to_string(),
                    failure_kind: None,
                    retry_reason: None,
                    error: None,
                }],
            },
        );
        manager.running_tasks.insert(
            task.id.clone(),
            RunningTask {
                store: store.clone(),
                handle: Arc::new(FakeHangingHandle),
                queued_task,
                started_at: Instant::now() - Duration::from_secs(30) - Duration::from_millis(1),
            },
        );

        let error = manager
            .wait_for(&task.id)
            .expect_err("stale worker should not hang forever");
        assert!(format!("{error:#}").contains("timed out waiting for outcome"));
        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "lost""#));
        assert!(record.contains("timed out waiting for outcome"));
        Ok(())
    }

    #[test]
    fn task_manager_tick_cleans_orphaned_running_and_queued_state() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let queued_task = test_task("task_orphan_queued");
        let running_task = test_task("task_orphan_running");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("printf noop".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();
        manager.queued_tasks.push_back(QueuedTask {
            store: store.clone(),
            workspace: temp_dir.path().to_path_buf(),
            task: queued_task,
            route_attempt: 1,
            goal: "test goal".to_string(),
            verification_commands: Vec::new(),
            config: config.clone(),
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        });
        manager.running_tasks.insert(
            running_task.id.clone(),
            RunningTask {
                store,
                handle: Arc::new(FakeHangingHandle),
                queued_task: QueuedTask {
                    store: StateStore::new(temp_dir.path()),
                    workspace: temp_dir.path().to_path_buf(),
                    task: running_task.clone(),
                    route_attempt: 1,
                    goal: "test goal".to_string(),
                    verification_commands: Vec::new(),
                    config,
                    cancellation_token: None,
                    coordinator_model: None,
                    coordinator_brief: None,
                    route_hint: None,
                },
                started_at: Instant::now(),
            },
        );

        let cleaned = manager.tick()?;

        assert_eq!(cleaned, 2);
        assert!(manager.queued_tasks.is_empty());
        assert!(manager.running_tasks.is_empty());
        Ok(())
    }

    #[test]
    fn task_manager_queues_when_concurrency_slot_is_busy() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("task_first");
        let second_task = test_task("task_second");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo worker-ok".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        let queued_record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == second_task.id)
            .context("missing queued task record")?;
        assert_eq!(queued_record.status, ManagedTaskStatus::Pending);
        assert!(
            !store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );

        let first_run = manager.wait_for(&first_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        let second_record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == second_task.id)
            .context("missing second task record")?;
        assert_eq!(second_record.status, ManagedTaskStatus::Running);
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );

        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn task_manager_serializes_tasks_with_same_concurrency_key() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("task_same_key_first");
        let second_task = test_task("task_same_key_second");
        let first_release = temp_dir.path().join("release-first");
        let second_release = temp_dir.path().join("release-second");
        let first_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo first-ok'",
            first_release.display()
        );
        let second_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo second-ok'",
            second_release.display()
        );
        let first_config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(first_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut second_config = first_config.clone();
        second_config.worker_command = Some(second_command);
        let mut manager = TaskManager::new();
        manager.concurrency.max_parallel_workers = 2;

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first goal",
            verification_commands: &[],
            config: &first_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second goal",
            verification_commands: &[],
            config: &second_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert!(
            store
                .worker_dir(&first_task.id)
                .join("packet.json")
                .exists()
        );
        assert!(
            !store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );
        fs::write(&first_release, "go")?;
        let first_run = manager.wait_for(&first_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );
        fs::write(&second_release, "go")?;
        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn task_manager_runs_different_concurrency_keys_in_parallel() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("task_keyed_first");
        let second_task = test_task("task_keyed_second");
        let first_release = temp_dir.path().join("release-keyed-first");
        let second_release = temp_dir.path().join("release-keyed-second");
        let first_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo first-ok'",
            first_release.display()
        );
        let second_command = format!(
            "sh -c 'while [ ! -f \"{}\" ]; do sleep 0.01; done; echo second-ok'",
            second_release.display()
        );
        let first_config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some(first_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let second_config = WorkerConfig {
            worker_kind: WorkerKind::Codex,
            worker_command: Some(second_command),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 2,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();
        manager.concurrency.max_parallel_workers = 2;

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first goal",
            verification_commands: &[],
            config: &first_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &second_task,
            route_attempt: 1,
            goal: "second goal",
            verification_commands: &[],
            config: &second_config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        assert!(
            store
                .worker_dir(&first_task.id)
                .join("packet.json")
                .exists()
        );
        assert!(
            store
                .worker_dir(&second_task.id)
                .join("packet.json")
                .exists()
        );
        fs::write(&first_release, "go")?;
        fs::write(&second_release, "go")?;
        let first_run = manager.wait_for(&first_task.id)?;
        let second_run = manager.wait_for(&second_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert_eq!(second_run.record.status, ManagedTaskStatus::Completed);
        Ok(())
    }

    #[test]
    fn task_manager_cancel_task_removes_pending_task_from_queue() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let first_task = test_task("task_running_slot");
        let pending_task = test_task("task_pending_cancel");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo worker-ok".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &first_task,
            route_attempt: 1,
            goal: "first goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &pending_task,
            route_attempt: 1,
            goal: "pending goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        manager.cancel_task(&pending_task.id)?;

        let pending_record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == pending_task.id)
            .context("missing pending task record")?;
        assert_eq!(pending_record.status, ManagedTaskStatus::Cancelled);
        let events =
            fs::read_to_string(store.worker_dir(&pending_task.id).join("task-events.jsonl"))?;
        assert!(events.contains(r#""status":"pending""#));
        assert!(events.contains(r#""status":"cancelled""#));

        let first_run = manager.wait_for(&first_task.id)?;
        assert_eq!(first_run.record.status, ManagedTaskStatus::Completed);
        assert!(
            !store
                .worker_dir(&pending_task.id)
                .join("packet.json")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn task_manager_cancel_task_cancels_running_worker_handle() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_cancel_handle");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("echo unreachable".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let mut manager = TaskManager::new();

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;
        manager.cancel_task(&task.id)?;

        let error = manager
            .wait_for(&task.id)
            .expect_err("cancelled worker should not produce a run");
        assert!(format!("{error:#}").contains("cancelled"));
        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "cancelled""#));
        Ok(())
    }

    #[test]
    fn task_manager_control_cancels_worker_while_waiting() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;
        let task = test_task("task_control_cancel");
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: Some("sh -c 'sleep 5'".to_string()),
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: true,
        };
        let control = TaskManagerControl::default();
        let mut manager = TaskManager::with_control(control.clone());

        manager.start(WorkerStartRequest {
            store: &store,
            workspace: temp_dir.path(),
            task: &task,
            route_attempt: 1,
            goal: "test goal",
            verification_commands: &[],
            config: &config,
            cancellation_token: None,
            coordinator_model: None,
            coordinator_brief: None,
            route_hint: None,
        })?;

        let wait_task_id = task.id.clone();
        let waiter = std::thread::spawn(move || manager.wait_for(&wait_task_id));

        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(
            control.current_task_id()?.as_deref(),
            Some(task.id.as_str())
        );
        assert!(control.cancel_current_task()?);

        let error = waiter
            .join()
            .expect("wait thread should not panic")
            .expect_err("cancelled worker should not complete");
        assert!(format!("{error:#}").contains("cancelled"));
        assert_eq!(control.current_task_id()?, None);

        let record = fs::read_to_string(store.worker_dir(&task.id).join("task-record.json"))?;
        assert!(record.contains(r#""status": "cancelled""#));
        Ok(())
    }

    #[test]
    fn task_manager_marks_running_task_cancelled() -> Result<()> {
        let mut manager = TaskManager::new();
        manager.records.insert(
            "task_running".to_string(),
            TaskRecord {
                task_id: "task_running".to_string(),
                worker_kind: "opencode".to_string(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".to_string(),
                route_hint: None,
                route_reason: "test route".to_string(),
                status: ManagedTaskStatus::Running,
                started_at: timestamp(),
                finished_at: None,
                residency_state: ResidencyState::Resident,
                run_epoch: 0,
                notified_epoch: default_notified_epoch(),
                notification_failed_epoch: None,
                killed: false,
                session_id: None,
                parent_session_id: None,
                root_session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "Worker task started.".to_string(),
                failure_kind: None,
                retry_reason: None,
                error: None,
                attempts: vec![TaskAttempt {
                    attempt: 1,
                    worker_kind: "opencode".to_string(),
                    worker_command: None,
                    worker_model: None,
                    worker_category: "quick".to_string(),
                    route_hint: None,
                    route_reason: "test route".to_string(),
                    status: TaskAttemptStatus::Running,
                    started_at: timestamp(),
                    finished_at: None,
                    session_id: None,
                    result_path: None,
                    outcome_path: None,
                    summary: "Worker task started.".to_string(),
                    failure_kind: None,
                    retry_reason: None,
                    error: None,
                }],
            },
        );

        manager.cancel_task("task_running")?;

        let record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == "task_running")
            .context("missing task record")?;
        assert_eq!(record.status, ManagedTaskStatus::Cancelled);
        assert!(record.finished_at.is_some());
        Ok(())
    }

    #[test]
    fn transition_task_record_rejects_late_complete_after_cancel() {
        let mut record = test_task_record(
            "task_cancelled",
            ManagedTaskStatus::Running,
            TaskAttemptStatus::Running,
        );
        let cancel = transition_task_record(
            &mut record,
            TaskTransition::Cancel {
                finished_at: timestamp(),
                summary: "Worker task cancelled.".to_string(),
                error: None,
            },
        );
        assert!(cancel.applied);
        assert_eq!(record.status, ManagedTaskStatus::Cancelled);

        let complete = transition_task_record(
            &mut record,
            TaskTransition::Complete {
                finished_at: timestamp(),
                result_path: PathBuf::from("/tmp/result.json"),
                outcome_path: PathBuf::from("/tmp/outcome.json"),
                summary: "late completion".to_string(),
                failure_kind: None,
            },
        );
        assert!(!complete.applied);
        assert_eq!(record.status, ManagedTaskStatus::Cancelled);
    }

    #[test]
    fn task_manager_interrupt_task_marks_running_task_interrupted() -> Result<()> {
        let mut manager = TaskManager::new();
        manager.records.insert(
            "task_interrupt".to_string(),
            test_task_record(
                "task_interrupt",
                ManagedTaskStatus::Running,
                TaskAttemptStatus::Running,
            ),
        );

        manager.interrupt_task("task_interrupt")?;

        let record = manager
            .list()
            .into_iter()
            .find(|record| record.task_id == "task_interrupt")
            .context("missing task record")?;
        assert_eq!(record.status, ManagedTaskStatus::Interrupted);
        assert_eq!(
            record.attempts.last().map(|attempt| &attempt.status),
            Some(&TaskAttemptStatus::Interrupted)
        );
        Ok(())
    }

    #[test]
    fn task_manager_applies_parallelism_limits_from_worker_config() {
        let mut manager = TaskManager::new();
        let config = WorkerConfig {
            worker_kind: WorkerKind::Opencode,
            worker_command: None,
            worker_model: None,
            worker_routes: Vec::new(),
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 3,
            max_parallel_per_key: 2,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: false,
        };

        manager.apply_worker_config(&config);

        assert_eq!(manager.max_parallel_workers(), 3);
        assert_eq!(manager.max_parallel_per_key(), 2);
    }

    #[test]
    fn task_manager_recovers_orphaned_pending_and_running_records_from_disk() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = StateStore::new(temp_dir.path());
        store.initialize()?;

        let pending_record = TaskRecord {
            task_id: "task_pending".into(),
            worker_kind: "opencode".into(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".into(),
            route_hint: None,
            route_reason: "test".into(),
            status: ManagedTaskStatus::Pending,
            started_at: timestamp(),
            finished_at: None,
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            result_path: None,
            outcome_path: None,
            summary: "queued".into(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".into(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".into(),
                route_hint: None,
                route_reason: "test".into(),
                status: TaskAttemptStatus::Pending,
                started_at: timestamp(),
                finished_at: None,
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "queued".into(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        };
        let running_record = TaskRecord {
            task_id: "task_running".into(),
            worker_kind: "opencode".into(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".into(),
            route_hint: None,
            route_reason: "test".into(),
            status: ManagedTaskStatus::Running,
            started_at: timestamp(),
            finished_at: None,
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: Some("session_running".into()),
            parent_session_id: None,
            root_session_id: None,
            result_path: None,
            outcome_path: None,
            summary: "running".into(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".into(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".into(),
                route_hint: None,
                route_reason: "test".into(),
                status: TaskAttemptStatus::Running,
                started_at: timestamp(),
                finished_at: None,
                session_id: Some("session_running".into()),
                result_path: None,
                outcome_path: None,
                summary: "running".into(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        };
        let completed_record = TaskRecord {
            task_id: "task_completed".into(),
            worker_kind: "opencode".into(),
            worker_command: None,
            worker_model: None,
            worker_category: "quick".into(),
            route_hint: None,
            route_reason: "test".into(),
            status: ManagedTaskStatus::Completed,
            started_at: timestamp(),
            finished_at: Some(timestamp()),
            residency_state: ResidencyState::Resident,
            run_epoch: 0,
            notified_epoch: default_notified_epoch(),
            notification_failed_epoch: None,
            killed: false,
            session_id: None,
            parent_session_id: None,
            root_session_id: None,
            result_path: None,
            outcome_path: None,
            summary: "completed".into(),
            failure_kind: None,
            retry_reason: None,
            error: None,
            attempts: vec![TaskAttempt {
                attempt: 1,
                worker_kind: "opencode".into(),
                worker_command: None,
                worker_model: None,
                worker_category: "quick".into(),
                route_hint: None,
                route_reason: "test".into(),
                status: TaskAttemptStatus::Completed,
                started_at: timestamp(),
                finished_at: Some(timestamp()),
                session_id: None,
                result_path: None,
                outcome_path: None,
                summary: "completed".into(),
                failure_kind: None,
                retry_reason: None,
                error: None,
            }],
        };

        write_task_record(&store, &pending_record)?;
        write_task_record(&store, &running_record)?;
        write_task_record(&store, &completed_record)?;

        let mut manager = TaskManager::new();
        let recovered = manager.recover_orphaned_records(&store)?;
        assert_eq!(recovered, 2);

        let pending_json =
            fs::read_to_string(store.worker_dir("task_pending").join("task-record.json"))?;
        let pending_after: TaskRecord = serde_json::from_str(&pending_json)?;
        assert_eq!(pending_after.status, ManagedTaskStatus::Lost);
        assert_eq!(
            pending_after.failure_kind,
            Some(TaskFailureKind::WorkerStartFailed)
        );
        assert_eq!(
            pending_after.attempts.last().map(|attempt| &attempt.status),
            Some(&TaskAttemptStatus::Lost)
        );

        let running_json =
            fs::read_to_string(store.worker_dir("task_running").join("task-record.json"))?;
        let running_after: TaskRecord = serde_json::from_str(&running_json)?;
        assert_eq!(running_after.status, ManagedTaskStatus::Lost);
        assert_eq!(
            running_after.failure_kind,
            Some(TaskFailureKind::WorkerStartFailed)
        );

        let completed_json =
            fs::read_to_string(store.worker_dir("task_completed").join("task-record.json"))?;
        let completed_after: TaskRecord = serde_json::from_str(&completed_json)?;
        assert_eq!(completed_after.status, ManagedTaskStatus::Completed);

        let pending_events =
            fs::read_to_string(store.worker_dir("task_pending").join("task-events.jsonl"))?;
        assert!(pending_events.contains("Recovered orphaned Gear worker task"));
        let running_events =
            fs::read_to_string(store.worker_dir("task_running").join("task-events.jsonl"))?;
        assert!(running_events.contains("Recovered orphaned Gear worker task"));
        assert!(
            !store
                .worker_dir("task_completed")
                .join("task-events.jsonl")
                .exists()
        );

        Ok(())
    }
}
