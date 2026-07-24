use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};

use crate::open_code_phase_runtime::OpenCodePhaseRuntimeFactory;
#[cfg(test)]
use crate::phase_routing::GEAR_LUNA_MODEL;
use crate::phase_routing::{GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL, PhaseRouteTable};
use crate::runtime::{
    DEFAULT_MAX_ITERATIONS, DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK, DEFAULT_MAX_RUNTIME_MINUTES,
    Orchestrator, PhaseRuntime, RunOptions,
};
use crate::state::ObjectivePolicy;
use crate::tools::CancellationToken;
use crate::worker_broker::PhaseBrokerFactory;
use crate::workers::{
    DEFAULT_OPENCODE_SESSION_COMMAND, Intensity, WorkerConfig, WorkerKind, WorkerRegistry,
    WorkerRoute,
};

#[derive(Debug, Parser)]
#[command(name = "gear")]
#[command(about = "Gearbox Gear local orchestration runtime")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run(RunCommand),
}

#[derive(Debug, Args)]
struct RunCommand {
    prompt: String,

    #[arg(long)]
    objective: bool,

    #[arg(long)]
    session_id: Option<String>,

    #[arg(long)]
    auto_continue: bool,

    #[arg(long, default_value_t = 3)]
    max_epochs: usize,

    #[arg(long, default_value_t = usize::MAX)]
    max_objective_calls: usize,

    #[arg(long, default_value_t = u64::MAX)]
    max_objective_tokens: u64,

    #[arg(long, default_value_t = u64::MAX)]
    max_objective_cost_micros: u64,

    #[arg(long, default_value_t = usize::MAX)]
    max_unknown_usage_calls: usize,

    #[arg(long, default_value_t = 2)]
    max_consecutive_no_progress: usize,

    #[arg(long, default_value_t = 3)]
    max_consecutive_failures: usize,

    #[arg(long, default_value_t = 0)]
    objective_cooldown_seconds: u64,

    #[arg(long, default_value = ".")]
    workspace: PathBuf,

    #[arg(long = "verify-command")]
    verification_commands: Vec<String>,

    #[arg(long)]
    opencode_command: Option<String>,

    #[arg(long)]
    codex_command: Option<String>,

    #[arg(long)]
    claude_command: Option<String>,

    #[arg(long)]
    zed_agent_command: Option<String>,

    #[arg(long)]
    custom_command: Option<String>,

    #[arg(long, default_value = "opencode_session")]
    worker: String,

    #[arg(long)]
    worker_command: Option<String>,

    #[arg(long)]
    worker_model: Option<String>,

    #[arg(long)]
    intensity: Option<String>,

    #[arg(long = "worker-sequence")]
    worker_sequence: Option<String>,

    #[arg(long)]
    opencode_free_fallbacks: bool,

    #[arg(long = "unavailable-worker-model")]
    unavailable_worker_models: Vec<String>,

    #[arg(long, default_value_t = usize::MAX)]
    premium_worker_budget: usize,

    #[arg(long, default_value_t = 1)]
    max_parallel_workers: usize,

    #[arg(long, default_value_t = 1)]
    max_parallel_per_key: usize,

    #[arg(long, default_value_t = 30)]
    stale_task_timeout_secs: usize,

    #[arg(long)]
    skip_worker: bool,

    #[arg(long = "allowed-path")]
    allowed_paths: Vec<String>,

    #[arg(long = "forbidden-path")]
    forbidden_paths: Vec<String>,

    #[arg(long, default_value_t = 40)]
    max_files_changed: usize,

    #[arg(long)]
    install_dependencies: bool,

    #[arg(long, default_value_t = DEFAULT_MAX_ITERATIONS)]
    max_iterations: usize,

    #[arg(long, default_value_t = DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK)]
    max_provider_unknown_streak: usize,

    #[arg(long, default_value_t = usize::MAX)]
    max_child_depth: usize,

    #[arg(long, default_value_t = DEFAULT_MAX_RUNTIME_MINUTES)]
    max_runtime_minutes: usize,

    #[arg(long)]
    opencode_phases: bool,

    #[arg(long)]
    opencode_planner_model: Option<String>,

    #[arg(long)]
    opencode_executor_model: Option<String>,

    #[arg(long)]
    opencode_reviewer_model: Option<String>,

    #[arg(long)]
    codex_acp_planner_model: Option<String>,

    #[arg(long)]
    codex_acp_executor_model: Option<String>,

    #[arg(long)]
    codex_acp_reviewer_model: Option<String>,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Run(command) => {
            validate_canonical_acp_command(&command)?;
            let worker = worker_config_from_command(&command)?;
            let phase_runtime = phase_runtime_from_command(&command, worker.clone())?;
            let options = RunOptions {
                request: command.prompt,
                workspace: command.workspace,
                verification_commands: command.verification_commands,
                worker,
                allowed_paths: command.allowed_paths,
                forbidden_paths: command.forbidden_paths,
                max_files_changed: command.max_files_changed,
                install_dependencies: command.install_dependencies,
                event_sink: None,
                cancellation_token: None,
                max_iterations: command.max_iterations,
                max_provider_unknown_streak: command.max_provider_unknown_streak,
                max_child_depth: command.max_child_depth,
                max_runtime_minutes: command.max_runtime_minutes,
                budget: None,
                coordinator_model: None,
                coordinator_brief: None,
                coordinator_review_hook: None,
                task_manager_control: None,
                task_manager: None,
                session_id: command.session_id,
                continuation: command.objective,
                intensity: {
                    let parsed = match command.intensity.as_deref() {
                        Some(value) => Some(
                            Intensity::parse_or_fail(value)
                                .map_err(|error| anyhow!("--intensity: {error}"))?,
                        ),
                        None => std::env::var("GEARBOX_GEAR_WORKER_INTENSITY")
                            .ok()
                            .and_then(|value| {
                                Intensity::parse_or_fail(&value)
                                    .map_err(|error| {
                                        eprintln!("warning: GEARBOX_GEAR_WORKER_INTENSITY: {error}")
                                    })
                                    .ok()
                            }),
                    };
                    parsed.flatten()
                },
            };
            let outcome = if command.objective {
                Orchestrator::run_objective_with_phase_runtime(
                    options,
                    phase_runtime,
                    ObjectivePolicy {
                        auto_continue: command.auto_continue,
                        max_epochs: command.max_epochs,
                        max_calls: command.max_objective_calls,
                        max_tokens: command.max_objective_tokens,
                        max_cost_micros: command.max_objective_cost_micros,
                        max_unknown_usage_calls: command.max_unknown_usage_calls,
                        max_consecutive_no_progress: command.max_consecutive_no_progress,
                        max_consecutive_failures: command.max_consecutive_failures,
                        cooldown_seconds: command.objective_cooldown_seconds,
                    },
                )?
                .into_last_goal_outcome()?
            } else {
                Orchestrator::run_with_phase_runtime(options, phase_runtime)?
            };

            println!("Gear goal: {}", outcome.goal_id);
            println!("Status: {}", outcome.status.as_str());
            println!("Artifacts: {}", outcome.artifacts_root.display());
            println!("Final report: {}", outcome.final_report_path.display());
            println!("Events: {}", outcome.events_path.display());
        }
    }

    Ok(())
}

fn validate_canonical_acp_command(command: &RunCommand) -> Result<()> {
    if command.max_parallel_workers != 1 || command.max_parallel_per_key != 1 {
        bail!(
            "Gear canonical Luna/Flash ACP mode requires --max-parallel-workers=1 and --max-parallel-per-key=1"
        );
    }
    if command.opencode_free_fallbacks {
        bail!("Gear canonical Luna/Flash ACP mode does not allow --opencode-free-fallbacks");
    }
    if command.skip_worker {
        bail!("Gear canonical Luna/Flash ACP mode cannot use --skip-worker");
    }
    for (flag, configured) in [
        ("--worker-sequence", command.worker_sequence.as_ref()),
        ("--worker-command", command.worker_command.as_ref()),
        ("--opencode-command", command.opencode_command.as_ref()),
        ("--codex-command", command.codex_command.as_ref()),
        ("--claude-command", command.claude_command.as_ref()),
        ("--zed-agent-command", command.zed_agent_command.as_ref()),
        ("--custom-command", command.custom_command.as_ref()),
    ] {
        if configured.is_some() {
            bail!("Gear canonical Luna/Flash ACP mode does not allow {flag}");
        }
    }
    let worker_kind = WorkerKind::parse(&command.worker)
        .ok_or_else(|| anyhow!("unknown Gear worker kind `{}`", command.worker))?;
    if worker_kind != WorkerKind::OpencodeSession {
        bail!(
            "Gear canonical Luna/Flash ACP mode uses `opencode_session` as its Flash executor base, not `{}`",
            worker_kind.as_str()
        );
    }
    if command.opencode_phases
        || command.opencode_planner_model.is_some()
        || command.opencode_executor_model.is_some()
        || command.opencode_reviewer_model.is_some()
        || command.codex_acp_planner_model.is_some()
        || command.codex_acp_executor_model.is_some()
        || command.codex_acp_reviewer_model.is_some()
    {
        bail!(
            "Gear canonical Luna/Flash ACP mode fixes Luna planning/review and `{GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL}` execution; model overrides are disabled"
        );
    }
    if let Some(model) = command.worker_model.as_deref().map(str::trim)
        && !model.is_empty()
        && model != GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL
    {
        bail!(
            "Gear canonical Luna/Flash ACP mode requires --worker-model `{GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL}` for execution"
        );
    }
    Ok(())
}

fn phase_runtime_from_command(command: &RunCommand, worker: WorkerConfig) -> Result<PhaseRuntime> {
    let routes = PhaseRouteTable::canonical_luna_flash()?;
    let workspace = command.workspace.clone();
    let broker_registry = Arc::new(WorkerRegistry::default());
    let broker_factory = Arc::new(PhaseBrokerFactory::new(
        broker_registry,
        workspace.join(".gear"),
    ));
    OpenCodePhaseRuntimeFactory::new(
        workspace,
        worker,
        broker_factory,
        CancellationToken::new(),
        routes,
        crate::phase_routing::LiveModelInventory::default(),
    )
    .build()
}

fn worker_config_from_command(command: &RunCommand) -> Result<WorkerConfig> {
    let worker_command = DEFAULT_OPENCODE_SESSION_COMMAND.to_string();

    Ok(WorkerConfig {
        worker_kind: WorkerKind::OpencodeSession,
        worker_command: Some(worker_command.clone()),
        worker_model: Some(GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL.to_string()),
        worker_routes: vec![WorkerRoute {
            worker_kind: WorkerKind::OpencodeSession,
            worker_command: Some(worker_command),
            worker_model: Some(GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL.to_string()),
        }],
        unavailable_worker_models: command
            .unavailable_worker_models
            .iter()
            .map(|model| model.trim().to_string())
            .filter(|model| !model.is_empty())
            .collect(),
        premium_worker_budget: usize::MAX,
        max_parallel_workers: 1,
        max_parallel_per_key: 1,
        stale_task_timeout_secs: command.stale_task_timeout_secs.max(1),
        skip_worker: false,
        require_worker: true,
        default_worker_for_small_tasks: WorkerKind::OpencodeSession,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_command() -> RunCommand {
        RunCommand {
            prompt: "build a test app".to_string(),
            objective: false,
            session_id: None,
            auto_continue: false,
            max_epochs: 3,
            max_objective_calls: 96,
            max_objective_tokens: 12_288_000,
            max_objective_cost_micros: 10_000_000,
            max_unknown_usage_calls: 32,
            max_consecutive_no_progress: 2,
            max_consecutive_failures: 3,
            objective_cooldown_seconds: 0,
            workspace: PathBuf::from("."),
            verification_commands: Vec::new(),
            opencode_command: None,
            codex_command: None,
            claude_command: None,
            zed_agent_command: None,
            custom_command: None,
            worker: "opencode_session".to_string(),
            worker_command: None,
            worker_model: None,
            worker_sequence: None,
            opencode_free_fallbacks: false,
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 0,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            allowed_paths: Vec::new(),
            forbidden_paths: Vec::new(),
            max_files_changed: 40,
            max_provider_unknown_streak: DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK,
            max_child_depth: usize::MAX,
            max_runtime_minutes: DEFAULT_MAX_RUNTIME_MINUTES,
            install_dependencies: false,
            max_iterations: DEFAULT_MAX_ITERATIONS,
            opencode_phases: false,
            opencode_planner_model: None,
            opencode_executor_model: None,
            opencode_reviewer_model: None,
            codex_acp_planner_model: None,
            codex_acp_executor_model: None,
            codex_acp_reviewer_model: None,
            intensity: None,
        }
    }

    #[test]
    fn canonical_cli_rejects_parallelism_fallbacks_and_command_overrides() {
        let mut command = run_command();
        command.max_parallel_workers = 2;
        assert!(validate_canonical_acp_command(&command).is_err());

        command.max_parallel_workers = 1;
        command.opencode_free_fallbacks = true;
        assert!(validate_canonical_acp_command(&command).is_err());

        command.opencode_free_fallbacks = false;
        command.codex_command = Some("codex exec".to_string());
        assert!(validate_canonical_acp_command(&command).is_err());

        command.codex_command = None;
        command.skip_worker = true;
        assert!(validate_canonical_acp_command(&command).is_err());

        command.skip_worker = false;
        command.codex_acp_planner_model = Some(GEAR_LUNA_MODEL.to_string());
        assert!(validate_canonical_acp_command(&command).is_err());

        command.codex_acp_planner_model = None;
        command.worker_sequence = Some("opencode_session".to_string());
        assert!(validate_canonical_acp_command(&command).is_err());

        command.worker_sequence = None;
        command.worker = "zed_agent".to_string();
        assert!(validate_canonical_acp_command(&command).is_err());
    }

    #[test]
    fn default_opencode_worker_has_a_session_command() -> Result<()> {
        let config = worker_config_from_command(&run_command())?;
        assert_eq!(config.worker_kind, WorkerKind::OpencodeSession);
        assert!(config.require_worker);
        assert_eq!(
            config.worker_command.as_deref(),
            Some(DEFAULT_OPENCODE_SESSION_COMMAND)
        );
        assert_eq!(
            config.worker_model.as_deref(),
            Some(GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL)
        );
        assert_eq!(config.worker_routes.len(), 1);
        assert_eq!(
            config.worker_routes[0].worker_model.as_deref(),
            Some(GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL)
        );
        Ok(())
    }

    #[test]
    fn canonical_cli_rejects_model_routing_knobs() {
        let mut command = run_command();
        command.worker_model = Some("opencode/mimo-v2.5-free".to_string());
        assert!(validate_canonical_acp_command(&command).is_err());

        command.worker_model = None;
        command.opencode_phases = true;
        assert!(validate_canonical_acp_command(&command).is_err());
    }

    #[test]
    fn canonical_cli_builds_provider_backed_phase_runtime() -> Result<()> {
        let mut command = run_command();
        command.workspace = PathBuf::from(".");
        let config = worker_config_from_command(&command)?;
        let runtime = phase_runtime_from_command(&command, config)?;

        assert!(runtime.broker_factory.is_some());
        for profile in &runtime.routes.profiles {
            assert_eq!(profile.candidates.len(), 1);
            let candidate = &profile.candidates[0];
            match profile.phase {
                crate::plan_graph::PhaseProfile::Planner
                | crate::plan_graph::PhaseProfile::PlanCritic
                | crate::plan_graph::PhaseProfile::ReviewerTask
                | crate::plan_graph::PhaseProfile::ReviewerFinal
                | crate::plan_graph::PhaseProfile::StrategistNextGoal => {
                    assert_eq!(
                        candidate.backend,
                        crate::phase_routing::PhaseBackend::CodexAcp
                    );
                    assert_eq!(
                        candidate.model,
                        crate::phase_routing::PhaseModelBinding::BackendDeclared(
                            GEAR_LUNA_MODEL.to_string()
                        )
                    );
                }
                crate::plan_graph::PhaseProfile::ExecutorQuick
                | crate::plan_graph::PhaseProfile::ExecutorDeep => {
                    assert_eq!(
                        candidate.backend,
                        crate::phase_routing::PhaseBackend::Worker(WorkerKind::OpencodeSession)
                    );
                    assert_eq!(
                        candidate.model,
                        crate::phase_routing::PhaseModelBinding::BackendDeclared(
                            GEAR_OPENCODE_DEEPSEEK_FLASH_MODEL.to_string()
                        )
                    );
                }
                crate::plan_graph::PhaseProfile::Orchestrator
                | crate::plan_graph::PhaseProfile::Summarizer => {
                    assert_eq!(
                        candidate.backend,
                        crate::phase_routing::PhaseBackend::Deterministic
                    );
                }
            }
        }
        Ok(())
    }
}
