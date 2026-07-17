use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, Stdio as StdStdio};
use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use smol::process::{Command, Stdio};

use crate::state::{CommandRecord, Scope};

const OUTPUT_LIMIT: usize = 12_000;
// Structured OpenCode worker events need a larger bounded envelope: cutting
// a JSON event in the middle makes planner/executor responses unparsable. The
// limit remains finite to keep long worker sessions from retaining unbounded
// output, while ordinary shell command excerpts keep the smaller limit.
const WORKER_OUTPUT_LIMIT: usize = 64_000;
static COMMAND_OUTPUT_COUNTER: AtomicU64 = AtomicU64::new(1);
static GEAR_RUST_COMMAND_GATE: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    is_cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::SeqCst)
    }

    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.is_cancelled, &other.is_cancelled)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShellCommandResult {
    pub command: String,
    pub exit_code: Option<i32>,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u128,
}

impl ShellCommandResult {
    pub fn record(&self) -> CommandRecord {
        CommandRecord {
            command: self.command.clone(),
            exit_code: self.exit_code,
            success: self.success,
            duration_ms: self.duration_ms,
            stdout_excerpt: truncate(&self.stdout, OUTPUT_LIMIT),
            stderr_excerpt: truncate(&self.stderr, OUTPUT_LIMIT),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DiffSnapshot {
    pub is_git_repo: bool,
    pub status: String,
    pub changed_files: Vec<String>,
    pub diff_hash: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScopeCheck {
    pub forbidden_touches: Vec<String>,
    pub outside_allowed_paths: Vec<String>,
    pub max_files_exceeded: bool,
    pub changed_file_count: usize,
}

/// Structured scope drift information for reviewable soft-scope signals.
///
/// Unlike `ScopeCheck` (which aggregates all violations), `ScopeDrift`
/// records only the worker-relative drift that the runtime should
/// escalate to a review instead of immediately blocking.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScopeDrift {
    /// Paths outside the allowed scope that are new (not in baseline diff).
    pub drifted_paths: Vec<String>,
    /// Human-readable explanation of the drift.
    pub drift_reason: String,
}

/// Baseline-aware scope check.
///
/// Files already present in the baseline (`before_diff`) are excluded from
/// scope drift computation so that pre-existing user dirty diffs are not
/// mis-counted as worker scope violations.
///
/// Hard `forbidden_paths` are still enforced on **all** files (including
/// baseline and worker additions) to preserve the hard safety boundary.
pub fn compute_baseline_aware_scope(
    before_diff: &DiffSnapshot,
    after_diff: &DiffSnapshot,
    scope: &Scope,
) -> (ScopeCheck, ScopeDrift) {
    // Build a set of files that were already changed before the worker started.
    let baseline_set: HashSet<&str> = before_diff
        .changed_files
        .iter()
        .map(String::as_str)
        .collect();

    // Forbidden touches checked against ALL files (hard boundary).
    let forbidden_touches: Vec<String> = after_diff
        .changed_files
        .iter()
        .filter(|path| {
            scope.forbidden_paths.iter().any(|forbidden_path| {
                path == &forbidden_path || path.starts_with(&format!("{forbidden_path}/"))
            })
        })
        .cloned()
        .collect();

    // New files: present in after_diff but not in baseline.
    let new_files: Vec<&str> = after_diff
        .changed_files
        .iter()
        .filter(|path| !baseline_set.contains(path.as_str()))
        .map(String::as_str)
        .collect();

    // Outside-allowed check only on new files (soft drift).
    // Files that already hit a forbidden path are excluded from drift
    // because they are handled by the hard-boundary block.
    let forbidden_set: HashSet<&str> = forbidden_touches.iter().map(String::as_str).collect();
    let outside_allowed_paths: Vec<String> = if scope.allowed_paths.is_empty() {
        Vec::new()
    } else {
        new_files
            .iter()
            .filter(|path| !forbidden_set.contains(*path))
            .filter(|path| {
                !scope
                    .allowed_paths
                    .iter()
                    .any(|allowed_path| path_is_within_allowed_scope(path, allowed_path))
            })
            .map(ToString::to_string)
            .collect()
    };

    let new_file_count = new_files.len();
    let max_files_exceeded = new_file_count > scope.max_files_changed;

    let scope_check = ScopeCheck {
        forbidden_touches,
        outside_allowed_paths: outside_allowed_paths.clone(),
        max_files_exceeded,
        changed_file_count: new_file_count,
    };

    let drift_parts: Vec<String> = {
        let mut parts = Vec::new();
        if !outside_allowed_paths.is_empty() {
            parts.push(format!(
                "{} file(s) outside scope: [{}]",
                outside_allowed_paths.len(),
                outside_allowed_paths.join(", ")
            ));
        }
        if max_files_exceeded {
            parts.push(format!(
                "new file count {} exceeds budget {}",
                new_file_count, scope.max_files_changed
            ));
        }
        parts
    };

    let drift = if drift_parts.is_empty() {
        ScopeDrift::default()
    } else {
        ScopeDrift {
            drifted_paths: outside_allowed_paths,
            drift_reason: drift_parts.join("; "),
        }
    };

    (scope_check, drift)
}

fn path_is_within_allowed_scope(path: &str, allowed_path: &str) -> bool {
    let normalized_allowed_path = allowed_path.trim_end_matches('/');
    normalized_allowed_path.is_empty()
        || normalized_allowed_path == "."
        || path == normalized_allowed_path
        || path.starts_with(&format!("{normalized_allowed_path}/"))
}

pub fn run_shell_command(workspace: &Path, command: &str) -> Result<ShellCommandResult> {
    run_shell_command_with_env(workspace, command, &HashMap::new())
}

pub fn run_shell_command_with_env(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
) -> Result<ShellCommandResult> {
    run_shell_command_with_env_and_cancellation(workspace, command, env, None)
}

pub fn run_shell_command_with_env_and_cancellation(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
    cancellation_token: Option<&CancellationToken>,
) -> Result<ShellCommandResult> {
    run_shell_command_with_env_and_cancellation_and_timeout(
        workspace,
        command,
        env,
        cancellation_token,
        None,
    )
}

pub fn run_shell_command_with_env_and_cancellation_and_timeout(
    workspace: &Path,
    command: &str,
    env: &HashMap<String, String>,
    cancellation_token: Option<&CancellationToken>,
    timeout: Option<Duration>,
) -> Result<ShellCommandResult> {
    let started_at = Instant::now();
    check_cancelled(cancellation_token, command)?;

    // Gear owns this admission gate. It serializes its own Cargo/Rust
    // commands without inspecting or terminating unrelated IDE processes.
    // The file lease extends that protection to another Gear process using
    // the same workspace, while the in-process mutex avoids needless polling
    // when two workers belong to this process.
    let _rust_command_lease = is_rust_build_command(command)
        .then(|| acquire_rust_command_lease(workspace, cancellation_token, timeout, started_at))
        .transpose()?;

    let stdout_path = command_output_path(workspace, "stdout")?;
    let stderr_path = command_output_path(workspace, "stderr")?;
    let stdout = fs::File::create(&stdout_path)
        .with_context(|| format!("failed to create {}", stdout_path.display()))?;
    let stderr = fs::File::create(&stderr_path)
        .with_context(|| format!("failed to create {}", stderr_path.display()))?;

    let mut process = cancellable_shell_command(command);
    process
        .current_dir(workspace)
        .stdout(StdStdio::from(stdout))
        .stderr(StdStdio::from(stderr));
    for (key, value) in env {
        process.env(key, value);
    }

    let mut child = process
        .spawn()
        .with_context(|| format!("failed to run command `{command}`"))?;
    let mut owned_processes = OwnedProcessTree::new(child.id());
    let cleanup_artifact_path = worker_cleanup_artifact_path(env);
    let monitor_provider_errors = env
        .get("GEARBOX_WORKER_PROVIDER_ERROR_RECOVERY")
        .is_some_and(|value| value == "1");
    let status = loop {
        owned_processes.observe(child.id());
        if cancellation_token.is_some_and(CancellationToken::is_cancelled) {
            terminate_command_process_group(
                &mut child,
                &owned_processes,
                cleanup_artifact_path.as_deref(),
                "cancelled",
            )?;
            cleanup_command_output(&stdout_path);
            cleanup_command_output(&stderr_path);
            bail!("Gear run cancelled while running `{command}`");
        }

        if let Some(timeout) = timeout.filter(|timeout| started_at.elapsed() >= *timeout) {
            terminate_command_process_group(
                &mut child,
                &owned_processes,
                cleanup_artifact_path.as_deref(),
                "explicit_timeout",
            )?;
            cleanup_command_output(&stdout_path);
            cleanup_command_output(&stderr_path);
            bail!(
                "Gear worker command timed out after {} seconds",
                timeout.as_secs()
            );
        }

        if monitor_provider_errors
            && command_output_indicates_provider_error(&stdout_path, &stderr_path)
        {
            owned_processes.observe(child.id());
            terminate_command_process_group(
                &mut child,
                &owned_processes,
                cleanup_artifact_path.as_deref(),
                "provider_error",
            )?;
            break child
                .wait()
                .with_context(|| format!("failed to reap provider-error command `{command}`"))?;
        }

        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("failed to poll command `{command}`"))?
        {
            break status;
        }

        std::thread::sleep(Duration::from_millis(50));
    };

    let stdout = fs::read_to_string(&stdout_path)
        .with_context(|| format!("failed to read {}", stdout_path.display()))?;
    let stderr = fs::read_to_string(&stderr_path)
        .with_context(|| format!("failed to read {}", stderr_path.display()))?;
    cleanup_command_output(&stdout_path);
    cleanup_command_output(&stderr_path);

    let preserve_worker_tail = env.contains_key("GEARBOX_WORKER_SESSION_ID");
    Ok(ShellCommandResult {
        command: command.to_string(),
        exit_code: status.code(),
        success: status.success(),
        stdout: if preserve_worker_tail {
            truncate_with_tail(&stdout, WORKER_OUTPUT_LIMIT)
        } else {
            truncate(&stdout, OUTPUT_LIMIT)
        },
        stderr: if preserve_worker_tail {
            truncate_with_tail(&stderr, WORKER_OUTPUT_LIMIT)
        } else {
            truncate(&stderr, OUTPUT_LIMIT)
        },
        duration_ms: started_at.elapsed().as_millis(),
    })
}

fn acquire_rust_command_lease(
    workspace: &Path,
    cancellation_token: Option<&CancellationToken>,
    timeout: Option<Duration>,
    started_at: Instant,
) -> Result<RustCommandLease> {
    let lock_directory = workspace.join(".gear").join("locks");
    fs::create_dir_all(&lock_directory).with_context(|| {
        format!(
            "failed to create Rust command lock directory {}",
            lock_directory.display()
        )
    })?;
    let lock_path = lock_directory.join("rust-build.lock");
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open Rust command lock {}", lock_path.display()))?;
    let process_guard = GEAR_RUST_COMMAND_GATE.get_or_init(|| Mutex::new(()));

    loop {
        check_cancelled(cancellation_token, "Rust command admission")?;
        if let Some(timeout) = timeout {
            if started_at.elapsed() >= timeout {
                bail!(
                    "Gear Rust command admission timed out after {} seconds",
                    timeout.as_secs()
                );
            }
        }

        let process_guard = match process_guard.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::sleep(Duration::from_millis(25));
                continue;
            }
        };

        if try_lock_rust_command_file(&lock_file)? {
            return Ok(RustCommandLease {
                _process_guard: process_guard,
                _lock_file: lock_file,
            });
        }

        drop(process_guard);
        std::thread::sleep(Duration::from_millis(25));
    }
}

struct RustCommandLease {
    _process_guard: MutexGuard<'static, ()>,
    _lock_file: fs::File,
}

#[cfg(unix)]
fn try_lock_rust_command_file(lock_file: &fs::File) -> Result<bool> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(true)
    } else if std::io::Error::last_os_error().kind() == std::io::ErrorKind::WouldBlock {
        Ok(false)
    } else {
        Err(std::io::Error::last_os_error()).context("failed to acquire Rust command lock")
    }
}

#[cfg(not(unix))]
fn try_lock_rust_command_file(_lock_file: &fs::File) -> Result<bool> {
    Ok(true)
}

fn is_rust_build_command(command: &str) -> bool {
    let mut tokens = command.split_whitespace();
    let mut token = tokens.next();
    while token.is_some_and(|value| value == "env" || value.contains('=')) {
        token = tokens.next();
    }
    token
        .and_then(|value| value.rsplit('/').next())
        .is_some_and(|value| matches!(value, "cargo" | "rustc" | "rust-analyzer"))
}

pub fn git_snapshot(workspace: &Path) -> Result<DiffSnapshot> {
    let rev_parse = run_raw_git(workspace, &["rev-parse", "--is-inside-work-tree"])?;
    if !rev_parse.success {
        return Ok(DiffSnapshot {
            is_git_repo: false,
            status: rev_parse.stderr,
            changed_files: Vec::new(),
            diff_hash: None,
        });
    }

    let status = run_raw_git(workspace, &["status", "--short"])?;
    let changed_files = parse_status_paths(&status.stdout);

    let diff_hash = {
        let diff_result = run_raw_git(workspace, &["diff"])?;
        if diff_result.success && !diff_result.stdout.trim().is_empty() {
            let normalized = normalize_diff_patch(&diff_result.stdout);
            let mut hasher = Sha256::new();
            hasher.update(normalized.as_bytes());
            Some(format!("{:x}", hasher.finalize()))
        } else {
            None
        }
    };

    Ok(DiffSnapshot {
        is_git_repo: true,
        status: status.stdout,
        changed_files,
        diff_hash,
    })
}

/// Return the repository HEAD used to bind evidence captured in this workspace.
/// Non-Git directories return `None`; callers decide whether that is compatible
/// with the evidence gate they are enforcing.
pub fn git_head_commit(workspace: &Path) -> Result<Option<String>> {
    let repository_check = run_raw_git(workspace, &["rev-parse", "--is-inside-work-tree"])?;
    if !repository_check.success {
        let diagnostic = format!(
            "{}{}",
            repository_check.stdout.trim(),
            repository_check.stderr.trim()
        );
        if diagnostic
            .to_ascii_lowercase()
            .contains("not a git repository")
        {
            return Ok(None);
        }
        bail!(
            "failed to determine whether {} is a Git workspace: {}",
            workspace.display(),
            diagnostic.trim()
        );
    }
    let result = run_raw_git(workspace, &["rev-parse", "HEAD"])?;
    if !result.success {
        bail!(
            "failed to resolve Git HEAD in {}: {}",
            workspace.display(),
            result.stderr.trim()
        );
    }
    if result.stdout.trim().is_empty() {
        return Ok(None);
    }
    let commit = result.stdout.trim();
    if commit.is_empty() {
        return Ok(None);
    }
    Ok(Some(commit.to_string()))
}

/// Strip timestamp noise from `---`/`+++` header lines so that semantically
/// identical diffs produced at different times hash to the same value.
pub fn normalize_diff_patch(patch: &str) -> String {
    patch
        .lines()
        .map(|line| {
            if line.starts_with("--- ") || line.starts_with("+++ ") {
                // Drop everything after the first tab, which is where git puts
                // the timestamp (e.g. "--- a/foo.rs\t2024-01-01 12:00:00.000000000 +0000").
                if let Some(tab_idx) = line.find('\t') {
                    return line[..tab_idx].to_string();
                }
            }
            line.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn check_scope(snapshot: &DiffSnapshot, scope: &Scope) -> ScopeCheck {
    let forbidden_touches = snapshot
        .changed_files
        .iter()
        .filter(|path| {
            scope.forbidden_paths.iter().any(|forbidden_path| {
                path == &forbidden_path || path.starts_with(&format!("{forbidden_path}/"))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let outside_allowed_paths = if scope.allowed_paths.is_empty() {
        Vec::new()
    } else {
        snapshot
            .changed_files
            .iter()
            .filter(|path| {
                !scope
                    .allowed_paths
                    .iter()
                    .any(|allowed_path| path_is_within_allowed_scope(path, allowed_path))
            })
            .cloned()
            .collect()
    };

    ScopeCheck {
        forbidden_touches,
        outside_allowed_paths,
        max_files_exceeded: snapshot.changed_files.len() > scope.max_files_changed,
        changed_file_count: snapshot.changed_files.len(),
    }
}

fn run_raw_git(workspace: &Path, args: &[&str]) -> Result<ShellCommandResult> {
    let command = format!("git {}", args.join(" "));
    let started_at = Instant::now();
    let output = Command::new("git")
        .args(args)
        .current_dir(workspace)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();
    let output = smol::block_on(output).with_context(|| format!("failed to run `{command}`"))?;

    Ok(ShellCommandResult {
        command,
        exit_code: output.status.code(),
        success: output.status.success(),
        stdout: truncate(&String::from_utf8_lossy(&output.stdout), OUTPUT_LIMIT),
        stderr: truncate(&String::from_utf8_lossy(&output.stderr), OUTPUT_LIMIT),
        duration_ms: started_at.elapsed().as_millis(),
    })
}

fn parse_status_paths(status: &str) -> Vec<String> {
    status
        .lines()
        .filter_map(|line| {
            let path = line.get(3..)?.trim();
            let path = path
                .split(" -> ")
                .last()
                .map(str::trim)
                .unwrap_or(path)
                .trim_matches('"');
            if path.is_empty() || path.starts_with(".gear/") || path.starts_with(".gearbox-agent/")
            {
                None
            } else {
                Some(path.to_string())
            }
        })
        .collect()
}

fn cancellable_shell_command(command: &str) -> StdCommand {
    if cfg!(windows) {
        let mut process = StdCommand::new("cmd");
        process.args(["/C", command]);
        process
    } else {
        let mut process = StdCommand::new("sh");
        process.args(["-lc", command]);
        #[cfg(unix)]
        unsafe {
            use std::os::unix::process::CommandExt as _;

            process.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        process
    }
}

fn terminate_command_process_group(
    child: &mut Child,
    owned_processes: &OwnedProcessTree,
    cleanup_artifact_path: Option<&Path>,
    reason: &str,
) -> Result<()> {
    #[cfg(unix)]
    {
        let process_group = child.id() as libc::pid_t;
        signal_command_process_group(process_group, libc::SIGTERM)?;
        owned_processes.signal(libc::SIGTERM)?;
        let mut signals = vec!["SIGTERM".to_string()];
        let graceful_deadline = Instant::now() + Duration::from_millis(100);
        let mut root_reaped = false;
        while Instant::now() < graceful_deadline {
            if child.try_wait()?.is_some() {
                root_reaped = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        if !root_reaped {
            signal_command_process_group(process_group, libc::SIGKILL)?;
            signals.push("SIGKILL".to_string());
            child.wait()?;
            root_reaped = true;
        }
        owned_processes.signal(libc::SIGKILL)?;
        if !signals.iter().any(|signal| signal == "SIGKILL") {
            signals.push("SIGKILL".to_string());
        }
        if let Some(path) = cleanup_artifact_path {
            write_process_cleanup_evidence(
                path,
                owned_processes,
                process_group,
                reason,
                &signals,
                root_reaped,
            )?;
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        if let Err(error) = child.kill()
            && error.kind() != std::io::ErrorKind::InvalidInput
        {
            return Err(error).context("failed to stop worker command");
        }
        owned_processes.signal(0)?;
        if let Err(error) = child.wait()
            && error.kind() != std::io::ErrorKind::InvalidInput
        {
            return Err(error).context("failed to wait for worker command shutdown");
        }
        if let Some(path) = cleanup_artifact_path {
            write_process_cleanup_evidence(
                path,
                owned_processes,
                child.id() as libc::pid_t,
                reason,
                &["kill".to_string()],
                true,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct OwnedProcessTree {
    root_pid: u32,
    #[cfg(target_os = "linux")]
    root_identity: Option<LinuxProcessIdentity>,
    #[cfg(target_os = "linux")]
    descendants: HashMap<libc::pid_t, LinuxProcessIdentity>,
}

impl OwnedProcessTree {
    fn new(root_pid: u32) -> Self {
        let mut tree = Self {
            root_pid,
            ..Default::default()
        };
        tree.observe(root_pid);
        tree
    }

    fn observe(&mut self, root_pid: u32) {
        #[cfg(target_os = "linux")]
        {
            let root_pid = root_pid as libc::pid_t;
            let snapshot = linux_process_snapshot();
            if let Some(identity) = snapshot.get(&root_pid) {
                self.root_identity = Some(identity.clone());
            }
            let mut children_by_parent: HashMap<
                libc::pid_t,
                Vec<(libc::pid_t, LinuxProcessIdentity)>,
            > = HashMap::new();
            for (pid, identity) in snapshot {
                children_by_parent
                    .entry(identity.parent_pid)
                    .or_default()
                    .push((pid, identity));
            }

            let mut pending = vec![root_pid];
            let mut visited = HashSet::from([root_pid]);
            while let Some(parent_pid) = pending.pop() {
                for (pid, identity) in children_by_parent
                    .get(&parent_pid)
                    .into_iter()
                    .flatten()
                    .cloned()
                {
                    if visited.insert(pid) {
                        self.descendants.insert(pid, identity);
                        pending.push(pid);
                    }
                }
            }
        }
    }

    fn signal(&self, signal: libc::c_int) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            if signal == 0 {
                return Ok(());
            }
            for (&pid, identity) in &self.descendants {
                if linux_process_start_time(pid) != Some(identity.start_time) {
                    continue;
                }
                if unsafe { libc::kill(pid, signal) } == -1 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error).with_context(|| {
                            format!("failed to signal owned worker process {pid}")
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
struct ProcessCleanupEvidence {
    schema_version: u32,
    reason: String,
    root_pid: u32,
    process_group: i32,
    session_id: Option<i32>,
    root_start_time: Option<u64>,
    owned_descendants: Vec<ProcessIdentityEvidence>,
    signals: Vec<String>,
    root_reaped: bool,
    remaining_owned_pids: Vec<u32>,
    platform: String,
    recorded_at: String,
}

#[derive(Clone, Debug, Serialize)]
struct ProcessIdentityEvidence {
    pid: u32,
    start_time: u64,
    process_group: Option<i32>,
    session_id: Option<i32>,
}

fn write_process_cleanup_evidence(
    path: &Path,
    owned_processes: &OwnedProcessTree,
    process_group: libc::pid_t,
    reason: &str,
    signals: &[String],
    root_reaped: bool,
) -> Result<()> {
    #[cfg(target_os = "linux")]
    let owned_descendants = owned_processes
        .descendants
        .iter()
        .map(|(&pid, identity)| ProcessIdentityEvidence {
            pid: pid as u32,
            start_time: identity.start_time,
            process_group: Some(identity.process_group),
            session_id: Some(identity.session_id),
        })
        .collect::<Vec<_>>();
    #[cfg(not(target_os = "linux"))]
    let owned_descendants = Vec::new();

    #[cfg(target_os = "linux")]
    let remaining_owned_pids = {
        // SIGKILL is asynchronous for detached descendants. Give the kernel
        // a short bounded window to reap them before freezing the receipt, so
        // a process that disappears immediately after the first snapshot is
        // not falsely reported as an orphan.
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            let remaining = owned_processes
                .descendants
                .iter()
                .filter_map(|(&pid, identity)| {
                    (linux_process_start_time(pid) == Some(identity.start_time))
                        .then_some(pid as u32)
                })
                .collect::<Vec<_>>();
            if remaining.is_empty() || Instant::now() >= deadline {
                break remaining;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    #[cfg(not(target_os = "linux"))]
    let remaining_owned_pids = Vec::new();

    let evidence = ProcessCleanupEvidence {
        schema_version: 1,
        reason: reason.to_string(),
        root_pid: owned_processes.root_pid,
        process_group: process_group as i32,
        #[cfg(target_os = "linux")]
        session_id: owned_processes.root_identity.as_ref().map(|identity| identity.session_id),
        #[cfg(not(target_os = "linux"))]
        session_id: None,
        #[cfg(target_os = "linux")]
        root_start_time: owned_processes
            .root_identity
            .as_ref()
            .map(|identity| identity.start_time),
        #[cfg(not(target_os = "linux"))]
        root_start_time: None,
        owned_descendants,
        signals: signals.to_vec(),
        root_reaped,
        remaining_owned_pids,
        platform: std::env::consts::OS.to_string(),
        recorded_at: crate::state::timestamp(),
    };
    let contents = format!(
        "{}\n",
        serde_json::to_string_pretty(&evidence).context("failed to serialize process cleanup evidence")?
    );
    fs::write(path, contents)
        .with_context(|| format!("failed to write process cleanup evidence {}", path.display()))?;
    Ok(())
}

fn worker_cleanup_artifact_path(env: &HashMap<String, String>) -> Option<PathBuf> {
    let packet_path = env.get("GEARBOX_WORKER_PACKET")?;
    Path::new(packet_path)
        .parent()
        .map(|worker_directory| worker_directory.join("process-cleanup.json"))
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct LinuxProcessIdentity {
    parent_pid: libc::pid_t,
    process_group: libc::pid_t,
    session_id: libc::pid_t,
    start_time: u64,
}

#[cfg(target_os = "linux")]
fn linux_process_snapshot() -> HashMap<libc::pid_t, LinuxProcessIdentity> {
    let mut snapshot = HashMap::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return snapshot;
    };
    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<libc::pid_t>() else {
            continue;
        };
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let Some(close_paren) = stat.rfind(')') else {
            continue;
        };
        let fields = stat[close_paren + 1..].split_whitespace().collect::<Vec<_>>();
        let (Some(parent_pid), Some(process_group), Some(session_id), Some(start_time)) =
            (fields.get(1), fields.get(2), fields.get(3), fields.get(19))
        else {
            continue;
        };
        let (Ok(parent_pid), Ok(process_group), Ok(session_id), Ok(start_time)) = (
            parent_pid.parse::<libc::pid_t>(),
            process_group.parse::<libc::pid_t>(),
            session_id.parse::<libc::pid_t>(),
            start_time.parse::<u64>(),
        ) else {
            continue;
        };
        snapshot.insert(
            pid,
            LinuxProcessIdentity {
                parent_pid,
                process_group,
                session_id,
                start_time,
            },
        );
    }
    snapshot
}

#[cfg(target_os = "linux")]
fn linux_process_start_time(pid: libc::pid_t) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close_paren = stat.rfind(')')?;
    stat[close_paren + 1..]
        .split_whitespace()
        .nth(19)
        .and_then(|value| value.parse::<u64>().ok())
}

#[cfg(unix)]
fn signal_command_process_group(process_group: libc::pid_t, signal: libc::c_int) -> Result<()> {
    if unsafe { libc::killpg(process_group, signal) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(error).context("failed to signal worker command process group")
}

fn command_output_path(workspace: &Path, stream: &str) -> Result<PathBuf> {
    let output_dir = workspace.join(".gear").join("tmp");
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;
    let sequence = COMMAND_OUTPUT_COUNTER.fetch_add(1, Ordering::SeqCst);
    Ok(output_dir.join(format!(
        "command-{}-{sequence}-{stream}.log",
        std::process::id()
    )))
}

fn check_cancelled(cancellation_token: Option<&CancellationToken>, command: &str) -> Result<()> {
    if cancellation_token.is_some_and(CancellationToken::is_cancelled) {
        bail!("Gear run cancelled before running `{command}`");
    }
    Ok(())
}

fn command_output_indicates_provider_error(stdout_path: &Path, stderr_path: &Path) -> bool {
    let patterns = [
        "rate limit",
        "rate-limit",
        "too many requests",
        "429",
        "quota exceeded",
        "usage quota",
        "free usage",
        "limit exhausted",
        "cooling down",
        "service unavailable",
        "temporarily unavailable",
        "overloaded",
        "model not found",
        "model unavailable",
        "provider error",
        "upstream error",
        "connection reset",
        "deadline exceeded",
        "context length",
    ];

    // OpenCode streams model text as JSON events on stdout.  The model may
    // quote these phrases in a plan's constraints or risks, which is not a
    // provider failure.  Stderr remains the process/provider diagnostic
    // channel and can be scanned as a whole; stdout is limited to explicit
    // error-shaped lines or structured error events.
    let stderr = fs::read_to_string(stderr_path).unwrap_or_default();
    if patterns
        .iter()
        .any(|pattern| stderr.to_ascii_lowercase().contains(pattern))
    {
        return true;
    }

    let stdout = fs::read_to_string(stdout_path).unwrap_or_default();
    stdout.lines().map(str::trim).filter(|line| !line.is_empty()).any(|line| {
        let is_json_error = serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .is_some_and(|value| {
                value
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|kind| kind.eq_ignore_ascii_case("error"))
                    || value.get("error").is_some()
            });
        let normalized = line.to_ascii_lowercase();
        let is_explicit_text_error = normalized.starts_with("error:")
            || normalized.starts_with("error ")
            || normalized.starts_with("provider error")
            || normalized.starts_with("http ")
            || normalized.starts_with("status ");
        (is_json_error || is_explicit_text_error)
            && patterns.iter().any(|pattern| normalized.contains(pattern))
    })
}

fn cleanup_command_output(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => eprintln!("failed to remove {}: {error}", path.display()),
    }
}

pub fn truncate(input: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for character in input.chars().take(max_chars) {
        output.push(character);
    }
    if output.len() < input.len() {
        output.push_str("\n[gearbox-agent output truncated]");
    }
    output
}

pub fn truncate_with_tail(input: &str, max_chars: usize) -> String {
    let character_count = input.chars().count();
    if character_count <= max_chars {
        return input.to_string();
    }

    const MARKER: &str = "\n[gearbox-agent output truncated]\n";
    let marker_length = MARKER.chars().count();
    if max_chars <= marker_length {
        return input.chars().take(max_chars).collect();
    }

    let retained = max_chars - marker_length;
    // Worker protocols put the final assistant receipt at the end of stdout.
    // Keep a small head for diagnostics and most of the bounded budget for
    // that final event so a JSON line is less likely to be cut in half.
    let prefix_length = (retained / 4).max(6).min(retained);
    let suffix_length = retained - prefix_length;
    let prefix = input.chars().take(prefix_length).collect::<String>();
    let suffix = input
        .chars()
        .rev()
        .take(suffix_length)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{prefix}{MARKER}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_git_status_paths() {
        let paths = parse_status_paths(
            " M src/main.rs\n?? .gearbox-agent/events/x.jsonl\nR  old.rs -> new.rs\n",
        );

        assert_eq!(paths, vec!["src/main.rs".to_string(), "new.rs".to_string()]);
    }

    #[test]
    fn truncate_with_tail_preserves_bounded_head_and_tail() {
        let output = truncate_with_tail(
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
            50,
        );
        assert!(output.contains("012345"), "head should remain available");
        assert!(output.contains("UVWXYZ"), "tail should remain available");
        assert!(output.contains("[gearbox-agent output truncated]"));
        assert!(output.chars().count() <= 50);
    }

    #[test]
    fn git_head_commit_distinguishes_repository_and_non_repository() {
        let repository = git_head_commit(Path::new(env!("CARGO_MANIFEST_DIR")))
            .expect("repository HEAD lookup should succeed")
            .expect("gearbox_agent should be inside a Git repository");
        assert!(repository.len() >= 7);
        assert!(
            repository
                .chars()
                .all(|character| character.is_ascii_hexdigit())
        );

        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        assert_eq!(
            git_head_commit(temp_dir.path()).expect("non-Git lookup should not error"),
            None
        );
    }

    #[test]
    fn checks_allowed_paths() {
        let snapshot = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["src/main.rs".to_string(), "README.md".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(vec!["src".to_string()], vec![".git".to_string()], 10);

        let check = check_scope(&snapshot, &scope);

        assert_eq!(check.outside_allowed_paths, vec!["README.md".to_string()]);
    }

    #[test]
    fn allowed_dot_scope_includes_workspace_root_and_runtime_artifacts() {
        let snapshot = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["LIVE_MARKER.txt".to_string(), ".gear/events/run.json".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(vec![".".to_string()], vec![".omo".to_string()], 10);

        let check = check_scope(&snapshot, &scope);

        assert!(check.outside_allowed_paths.is_empty());
        assert!(check.forbidden_touches.is_empty());
    }

    #[test]
    fn baseline_aware_scope_ignores_dirty_baseline_files() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["Cargo.lock".to_string(), "README.md".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec![
                "Cargo.lock".to_string(),
                "README.md".to_string(),
                "src/main.rs".to_string(),
            ],
            diff_hash: None,
        };
        let scope = Scope::new(vec!["src".to_string()], vec![".omo".to_string()], 10);
        let (check, drift) = compute_baseline_aware_scope(&before, &after, &scope);

        // Baseline files (Cargo.lock, README.md) should not count as drift.
        assert!(
            check.outside_allowed_paths.is_empty(),
            "baseline files should not appear in outside_allowed_paths: {:?}",
            check.outside_allowed_paths
        );
        // Only new file (src/main.rs) is counted.
        assert_eq!(check.changed_file_count, 1);
        // No forbidden touches.
        assert!(check.forbidden_touches.is_empty());
        // No drift because new file is inside allowed paths.
        assert!(drift.drifted_paths.is_empty());
        assert!(drift.drift_reason.is_empty());
    }

    #[test]
    fn baseline_aware_scope_detects_drift_on_new_outside_files() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["README.md".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec![
                "README.md".to_string(),
                "new_file.py".to_string(),
                "Cargo.toml".to_string(),
            ],
            diff_hash: None,
        };
        let scope = Scope::new(
            vec!["src".to_string(), "Cargo.toml".to_string()],
            vec![".omo".to_string()],
            10,
        );
        let (check, drift) = compute_baseline_aware_scope(&before, &after, &scope);

        // new_file.py is new and outside allowed paths.
        assert_eq!(check.outside_allowed_paths, vec!["new_file.py".to_string()]);
        assert_eq!(check.changed_file_count, 2);
        assert!(!check.max_files_exceeded);
        // Drift should have the outside path.
        assert_eq!(drift.drifted_paths, vec!["new_file.py".to_string()]);
        assert!(!drift.drift_reason.is_empty());
    }

    #[test]
    fn baseline_aware_scope_hard_boundary_still_blocks() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["src/lib.rs".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["src/lib.rs".to_string(), ".omo/config.json".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(vec!["src".to_string()], vec![".omo".to_string()], 10);
        let (check, _) = compute_baseline_aware_scope(&before, &after, &scope);

        // .omo/config.json is a forbidden path touch (hard boundary).
        assert_eq!(
            check.forbidden_touches,
            vec![".omo/config.json".to_string()]
        );
        // outside_allowed_paths should NOT include baseline file src/lib.rs.
        assert!(check.outside_allowed_paths.is_empty());
    }

    #[test]
    fn baseline_aware_scope_exceeded_file_budget() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["existing.rs".to_string()],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec![
                "existing.rs".to_string(),
                "a.rs".to_string(),
                "b.rs".to_string(),
                "c.rs".to_string(),
            ],
            diff_hash: None,
        };
        // max_files_changed = 2, but only 3 new files from baseline.
        let scope = Scope::new(Vec::new(), Vec::new(), 2);
        let (check, drift) = compute_baseline_aware_scope(&before, &after, &scope);

        assert!(check.max_files_exceeded);
        assert_eq!(check.changed_file_count, 3);
        assert!(drift.drift_reason.contains("exceeds budget"));
    }

    #[test]
    fn baseline_aware_scope_no_baseline_no_change() {
        let before = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec![],
            diff_hash: None,
        };
        let after = DiffSnapshot {
            is_git_repo: true,
            status: String::new(),
            changed_files: vec!["src/main.rs".to_string()],
            diff_hash: None,
        };
        let scope = Scope::new(vec!["src".to_string()], vec![".omo".to_string()], 10);
        let (check, drift) = compute_baseline_aware_scope(&before, &after, &scope);

        assert!(check.forbidden_touches.is_empty());
        assert!(check.outside_allowed_paths.is_empty());
        assert!(!check.max_files_exceeded);
        assert_eq!(check.changed_file_count, 1);
        assert!(drift.drifted_paths.is_empty());
    }

    #[test]
    fn rust_command_gate_only_matches_owned_rust_build_tokens() {
        assert!(is_rust_build_command("cargo test -p gearbox_agent"));
        assert!(is_rust_build_command(
            "env CARGO_BUILD_JOBS=1 rustc src/main.rs"
        ));
        assert!(!is_rust_build_command("echo cargo"));
        assert!(!is_rust_build_command("python build.py"));
    }

    #[test]
    fn cancelled_command_returns_error_before_spawn() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let cancellation_token = CancellationToken::new();
        cancellation_token.cancel();

        let error = run_shell_command_with_env_and_cancellation(
            temp_dir.path(),
            "echo unreachable",
            &HashMap::new(),
            Some(&cancellation_token),
        )
        .expect_err("command should be cancelled");

        assert!(
            error.to_string().contains("Gear run cancelled"),
            "{error:#}"
        );
    }

    #[test]
    fn timed_out_command_returns_a_stable_error() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let error = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "sleep 5",
            &HashMap::new(),
            None,
            Some(Duration::from_millis(20)),
        )
        .expect_err("command should time out");

        assert_eq!(
            error.to_string(),
            "Gear worker command timed out after 0 seconds"
        );
    }

    #[cfg(unix)]
    #[test]
    fn provider_error_output_terminates_worker_process_group_without_timeout() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let worker_dir = temp_dir.path().join(".gear").join("workers").join("task");
        fs::create_dir_all(&worker_dir).expect("worker evidence directory should exist");
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_PROVIDER_ERROR_RECOVERY".to_string(),
            "1".to_string(),
        );
        env.insert(
            "GEARBOX_WORKER_PACKET".to_string(),
            worker_dir.join("packet.json").to_string_lossy().to_string(),
        );
        let started_at = Instant::now();
        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "printf 'rate limit exceeded' >&2; sleep 5",
            &env,
            None,
            None,
        )
        .expect("provider error should return a failed result, not a timeout");

        assert!(!result.success);
        assert!(result.stderr.contains("rate limit exceeded"));
        assert!(
            started_at.elapsed() < Duration::from_secs(2),
            "provider error should reap the child promptly"
        );
        let cleanup: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(worker_dir.join("process-cleanup.json"))
                .expect("provider cleanup evidence should be persisted"),
        )
        .expect("provider cleanup evidence should be valid JSON");
        assert_eq!(cleanup["reason"], "provider_error");
        assert_eq!(cleanup["root_reaped"], true);
        assert!(cleanup["remaining_owned_pids"].as_array().is_some_and(Vec::is_empty));
    }

    #[test]
    fn model_text_that_mentions_provider_errors_does_not_trigger_recovery() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let stdout_path = temp_dir.path().join("stdout.log");
        let stderr_path = temp_dir.path().join("stderr.log");
        fs::write(
            &stdout_path,
            r#"{"type":"text","part":{"text":"{\"constraints\":[\"Provider errors must halt the current attempt\"],\"risks\":[{\"description\":\"context length remains a future concern\"}]}"}}"#,
        )
        .expect("stdout should be writable");
        fs::write(&stderr_path, "timestamp=... level=INFO message=streaming\n")
            .expect("stderr should be writable");

        assert!(!command_output_indicates_provider_error(
            &stdout_path,
            &stderr_path
        ));
    }

    #[test]
    fn structured_provider_error_event_still_triggers_recovery() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let stdout_path = temp_dir.path().join("stdout.log");
        let stderr_path = temp_dir.path().join("stderr.log");
        fs::write(
            &stdout_path,
            r#"{"type":"error","error":{"message":"429 too many requests"}}"#,
        )
        .expect("stdout should be writable");
        fs::write(&stderr_path, "").expect("stderr should be writable");

        assert!(command_output_indicates_provider_error(
            &stdout_path,
            &stderr_path
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn provider_error_terminates_owned_detached_child_without_touching_unrelated_process() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let detached_pid_path = temp_dir.path().join("detached.pid");
        let command = format!(
            "setsid sh -c 'sleep 5' >/dev/null 2>&1 & printf '%s' \"$!\" > {}; sleep 0.2; printf '%s' 'rate limit exceeded' >&2; sleep 5",
            detached_pid_path.display(),
        );
        let mut env = HashMap::new();
        env.insert(
            "GEARBOX_WORKER_PROVIDER_ERROR_RECOVERY".to_string(),
            "1".to_string(),
        );
        let mut unrelated = StdCommand::new("sleep")
            .arg("5")
            .spawn()
            .expect("unrelated process should start");
        let unrelated_pid = unrelated.id() as libc::pid_t;

        let result = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            &command,
            &env,
            None,
            None,
        )
        .expect("provider error should return a failed result");
        assert!(!result.success);

        let detached_pid = fs::read_to_string(&detached_pid_path)
            .expect("detached child pid should be recorded")
            .trim()
            .parse::<libc::pid_t>()
            .expect("detached child pid should be numeric");
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let detached_exists = fs::metadata(format!("/proc/{detached_pid}")).is_ok();
            let unrelated_exists = fs::metadata(format!("/proc/{unrelated_pid}")).is_ok();
            if !detached_exists && unrelated_exists {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let detached_survived = fs::metadata(format!("/proc/{detached_pid}")).is_ok();
        let unrelated_survived = fs::metadata(format!("/proc/{unrelated_pid}")).is_ok();
        unrelated.kill().expect("test-owned unrelated process should stop");
        unrelated.wait().expect("test-owned unrelated process should reap");
        assert!(
            !detached_survived,
            "detached worker child {detached_pid} survived owned cleanup"
        );
        assert!(
            unrelated_survived,
            "unrelated process {unrelated_pid} should survive owned cleanup"
        );
    }

    #[cfg(unix)]
    #[test]
    fn timed_out_command_terminates_its_process_group() {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let child_pid_path = temp_dir.path().join("child.pid");
        let command = format!(
            "sleep 5 & printf '%s' \"$!\" > {}; wait",
            child_pid_path.display()
        );

        run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            &command,
            &HashMap::new(),
            None,
            Some(Duration::from_millis(500)),
        )
        .expect_err("command should time out");

        let child_pid = fs::read_to_string(&child_pid_path)
            .expect("background child pid should be recorded")
            .trim()
            .parse::<libc::pid_t>()
            .expect("background child pid should be numeric");
        std::thread::sleep(Duration::from_millis(20));
        let process_exists = unsafe { libc::kill(child_pid, 0) == 0 };
        assert!(
            !process_exists,
            "background command child {child_pid} survived"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rust_command_admission_times_out_when_another_process_holds_workspace_lease() {
        use std::os::fd::AsRawFd;

        let temp_dir = tempfile::tempdir().expect("failed to create temp directory");
        let lock_directory = temp_dir.path().join(".gear").join("locks");
        fs::create_dir_all(&lock_directory).expect("failed to create lock directory");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(lock_directory.join("rust-build.lock"))
            .expect("failed to open lock file");
        let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(result, 0, "test must own the workspace lease");

        let error = run_shell_command_with_env_and_cancellation_and_timeout(
            temp_dir.path(),
            "cargo --version",
            &HashMap::new(),
            None,
            Some(Duration::from_millis(40)),
        )
        .expect_err("held workspace lease should prevent command admission");

        assert_eq!(
            error.to_string(),
            "Gear Rust command admission timed out after 0 seconds"
        );
    }
}
