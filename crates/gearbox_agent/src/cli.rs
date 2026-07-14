use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow};
use clap::{Args, Parser, Subcommand};

use crate::open_code_phase_runtime::{
    OpenCodePhaseRuntimeFactory, open_code_model_profiles_from_env,
    open_code_model_profiles_from_values,
};
use crate::phase_routing::{CodexAcpModelProfiles, PhaseRouteTable};
use crate::runtime::{
    DEFAULT_MAX_ITERATIONS, DEFAULT_MAX_PROVIDER_UNKNOWN_STREAK, DEFAULT_MAX_RUNTIME_MINUTES,
    Orchestrator, PhaseRuntime, RunOptions,
};
use crate::state::ObjectivePolicy;
use crate::tools::CancellationToken;
use crate::worker_broker::PhaseBrokerFactory;
use crate::workers::{Intensity, WorkerConfig, WorkerKind, WorkerRegistry, WorkerRoute};

const DEFAULT_OPENCODE_SESSION_COMMAND: &str = r#"sh -c 'if [ "$GEARBOX_WORKER_RESUME" = "true" ]; then opencode run --pure --format json --session "$GEARBOX_WORKER_SESSION_ID" --model "${GEARBOX_WORKER_MODEL:-opencode/mimo-v2.5-free}" < "$GEARBOX_WORKER_PROMPT"; else opencode run --pure --format json --model "${GEARBOX_WORKER_MODEL:-opencode/mimo-v2.5-free}" < "$GEARBOX_WORKER_PROMPT"; fi'"#;

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
    auto_continue: bool,

    #[arg(long, default_value_t = 3)]
    max_epochs: usize,

    #[arg(long, default_value_t = 96)]
    max_objective_calls: usize,

    #[arg(long, default_value_t = 12_288_000)]
    max_objective_tokens: u64,

    #[arg(long, default_value_t = 10_000_000)]
    max_objective_cost_micros: u64,

    #[arg(long, default_value_t = 32)]
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

    #[arg(long, default_value = "opencode")]
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

    #[arg(long, default_value_t = 1)]
    premium_worker_budget: usize,

    #[arg(long, default_value_t = 1)]
    max_parallel_workers: usize,

    #[arg(long, default_value_t = 1)]
    max_parallel_per_key: usize,

    #[arg(long, default_value_t = 30)]
    stale_task_timeout_secs: usize,

    #[arg(long)]
    skip_worker: bool,

    #[arg(long)]
    require_worker: bool,

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
    codex_acp: bool,

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
            let worker = worker_config_from_command(&command)?;
            let workspace = command.workspace.clone();
            let phase_runtime = if command.objective && command.codex_acp {
                let planner = command
                    .codex_acp_planner_model
                    .clone()
                    .or_else(|| trimmed_env_value("GEARBOX_GEAR_CODEX_ACP_PLANNER_MODEL"))
                    .context(
                        "--codex-acp requires --codex-acp-planner-model or GEARBOX_GEAR_CODEX_ACP_PLANNER_MODEL",
                    )?;
                let executor = command
                    .codex_acp_executor_model
                    .clone()
                    .or_else(|| trimmed_env_value("GEARBOX_GEAR_CODEX_ACP_EXECUTOR_MODEL"))
                    .unwrap_or_else(|| planner.clone());
                let reviewer = command
                    .codex_acp_reviewer_model
                    .clone()
                    .or_else(|| trimmed_env_value("GEARBOX_GEAR_CODEX_ACP_REVIEWER_MODEL"))
                    .unwrap_or_else(|| planner.clone());
                let profiles = CodexAcpModelProfiles {
                    codex_planner: planner,
                    opencode_executor: executor,
                    codex_reviewer: reviewer,
                };
                let routes = PhaseRouteTable::codex_acp_opencode(profiles)?;
                let broker_registry = Arc::new(WorkerRegistry::default());
                let broker_factory = Arc::new(PhaseBrokerFactory::new(
                    broker_registry,
                    workspace.join(".gear"),
                ));
                OpenCodePhaseRuntimeFactory::new(
                    workspace,
                    worker.clone(),
                    broker_factory,
                    CancellationToken::new(),
                    routes,
                    crate::phase_routing::LiveModelInventory::default(),
                )
                .build()?
            } else if command.objective && command.opencode_phases {
                let profiles = open_code_model_profiles_from_values(
                    true,
                    command.opencode_planner_model.clone(),
                    command.opencode_executor_model.clone(),
                    command.opencode_reviewer_model.clone(),
                    None,
                )?
                .or_else(|| open_code_model_profiles_from_env().ok().flatten())
                .context(
                    "--opencode-phases requires --opencode-planner-model or GEARBOX_GEAR_OPENCODE_PLANNER_MODEL",
                )?;
                let routes = PhaseRouteTable::opencode_only(profiles)?;
                let broker_registry = Arc::new(WorkerRegistry::default());
                let broker_factory = Arc::new(PhaseBrokerFactory::new(
                    broker_registry,
                    workspace.join(".gear"),
                ));
                OpenCodePhaseRuntimeFactory::new(
                    workspace,
                    worker.clone(),
                    broker_factory,
                    CancellationToken::new(),
                    routes,
                    crate::phase_routing::LiveModelInventory::default(),
                )
                .build()?
            } else {
                PhaseRuntime::legacy()
            };
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
                session_id: None,
                continuation: false,
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
                Orchestrator::run(options)?
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

fn trimmed_env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn worker_config_from_command(command: &RunCommand) -> Result<WorkerConfig> {
    let worker_kind = WorkerKind::parse(&command.worker)
        .ok_or_else(|| anyhow!("unknown Gear worker kind `{}`", command.worker))?;
    let worker_model = command
        .worker_model
        .clone()
        .filter(|model| !model.trim().is_empty());
    let worker_command = command
        .worker_command
        .clone()
        .or_else(|| worker_command_for_kind(worker_kind, command))
        .or_else(|| {
            (worker_kind == WorkerKind::OpencodeSession)
                .then(|| DEFAULT_OPENCODE_SESSION_COMMAND.to_string())
        })
        .or_else(|| worker_kind.default_command(worker_model.as_deref()))
        .filter(|command| !command.trim().is_empty());
    let worker_routes = if command.worker_sequence.is_some() {
        worker_routes_from_sequence(
            command.worker_sequence.as_deref(),
            worker_kind,
            &worker_command,
            &worker_model,
            command,
        )?
    } else if command.opencode_free_fallbacks {
        opencode_free_fallback_routes()
    } else {
        Vec::new()
    };
    let require_worker = command.require_worker
        || worker_command.is_some()
        || worker_routes
            .iter()
            .any(|route| route.worker_command.is_some());

    Ok(WorkerConfig {
        worker_kind,
        worker_command,
        worker_model,
        worker_routes,
        unavailable_worker_models: command
            .unavailable_worker_models
            .iter()
            .map(|model| model.trim().to_string())
            .filter(|model| !model.is_empty())
            .collect(),
        premium_worker_budget: command.premium_worker_budget,
        max_parallel_workers: command.max_parallel_workers.max(1),
        max_parallel_per_key: command.max_parallel_per_key.max(1),
        stale_task_timeout_secs: command.stale_task_timeout_secs.max(1),
        skip_worker: command.skip_worker,
        require_worker,
        default_worker_for_small_tasks: WorkerKind::ZedAgent,
    })
}

fn opencode_free_fallback_routes() -> Vec<WorkerRoute> {
    [
        "opencode/hy3-free",
        "opencode/mimo-v2.5-free",
        "opencode/deepseek-v4-flash-free",
    ]
    .into_iter()
    .map(|worker_model| WorkerRoute {
        worker_kind: WorkerKind::OpencodeSession,
        worker_command: Some(DEFAULT_OPENCODE_SESSION_COMMAND.to_string()),
        worker_model: Some(worker_model.to_string()),
    })
    .collect()
}

fn worker_routes_from_sequence(
    worker_sequence: Option<&str>,
    default_worker_kind: WorkerKind,
    default_worker_command: &Option<String>,
    default_worker_model: &Option<String>,
    command: &RunCommand,
) -> Result<Vec<WorkerRoute>> {
    let Some(worker_sequence) = worker_sequence else {
        return Ok(Vec::new());
    };

    worker_sequence
        .split(',')
        .filter_map(|worker| {
            let worker = worker.trim();
            (!worker.is_empty()).then_some(worker)
        })
        .map(|worker| {
            let (worker_kind, worker_model) = worker_route_from_sequence_entry(worker)?;
            let worker_command = worker_command_for_kind(worker_kind, command)
                .or_else(|| {
                    (worker_kind == default_worker_kind)
                        .then(|| default_worker_command.clone())
                        .flatten()
                })
                .or_else(|| worker_kind.default_command(worker_model.as_deref()));
            let worker_model = worker_model.or_else(|| {
                (worker_kind == default_worker_kind)
                    .then(|| default_worker_model.clone())
                    .flatten()
            });
            Ok(WorkerRoute {
                worker_kind,
                worker_command,
                worker_model,
            })
        })
        .collect()
}

fn worker_route_from_sequence_entry(worker: &str) -> Result<(WorkerKind, Option<String>)> {
    let (worker, worker_model) = worker
        .split_once(':')
        .map(|(worker, worker_model)| {
            (
                worker.trim(),
                Some(worker_model.trim().to_string()).filter(|model| !model.is_empty()),
            )
        })
        .unwrap_or((worker.trim(), None));
    let worker_kind = WorkerKind::parse(worker)
        .ok_or_else(|| anyhow!("unknown Gear worker kind in sequence `{worker}`"))?;
    Ok((worker_kind, worker_model))
}

fn worker_command_for_kind(worker_kind: WorkerKind, command: &RunCommand) -> Option<String> {
    match worker_kind {
        WorkerKind::Opencode | WorkerKind::OpencodeSession => command.opencode_command.clone(),
        WorkerKind::Codex => command.codex_command.clone(),
        WorkerKind::Claude => command.claude_command.clone(),
        WorkerKind::ZedAgent => command.zed_agent_command.clone(),
        WorkerKind::Custom => command.custom_command.clone(),
    }
    .filter(|command| !command.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_command() -> RunCommand {
        RunCommand {
            prompt: "build a test app".to_string(),
            objective: false,
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
            worker: "opencode".to_string(),
            worker_command: None,
            worker_model: None,
            worker_sequence: None,
            opencode_free_fallbacks: false,
            unavailable_worker_models: Vec::new(),
            premium_worker_budget: 1,
            max_parallel_workers: 1,
            max_parallel_per_key: 1,
            stale_task_timeout_secs: 30,
            skip_worker: false,
            require_worker: false,
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
            codex_acp: false,
            codex_acp_planner_model: None,
            codex_acp_executor_model: None,
            codex_acp_reviewer_model: None,
            intensity: None,
        }
    }

    #[test]
    fn cli_worker_config_builds_sequence_with_kind_commands() -> Result<()> {
        let mut command = run_command();
        command.worker_sequence = Some("opencode,codex:gpt-5.4,claude".to_string());
        command.opencode_command = Some("opencode run".to_string());
        command.codex_command = Some("codex exec".to_string());
        command.claude_command = Some("claude -p".to_string());

        let config = worker_config_from_command(&command)?;

        assert_eq!(config.worker_routes.len(), 3);
        assert_eq!(config.worker_routes[0].worker_kind, WorkerKind::Opencode);
        assert_eq!(
            config.worker_routes[1].worker_command.as_deref(),
            Some("codex exec")
        );
        assert_eq!(
            config.worker_routes[1].worker_model.as_deref(),
            Some("gpt-5.4")
        );
        assert_eq!(
            config.worker_routes[2].worker_command.as_deref(),
            Some("claude -p")
        );
        assert_eq!(config.max_parallel_workers, 1);
        assert_eq!(config.max_parallel_per_key, 1);
        assert_eq!(config.premium_worker_budget, 1);
        assert!(config.require_worker);
        Ok(())
    }

    #[test]
    fn cli_worker_config_enables_verified_opencode_free_fallbacks_without_overriding_sequence()
    -> Result<()> {
        let mut command = run_command();
        command.opencode_free_fallbacks = true;

        let config = worker_config_from_command(&command)?;

        assert_eq!(
            config
                .worker_routes
                .iter()
                .map(|route| route.worker_model.as_deref())
                .collect::<Vec<_>>(),
            vec![
                Some("opencode/hy3-free"),
                Some("opencode/mimo-v2.5-free"),
                Some("opencode/deepseek-v4-flash-free"),
            ]
        );
        assert!(config.worker_routes.iter().all(|route| {
            route.worker_kind == WorkerKind::OpencodeSession
                && route.worker_command.as_deref().is_some_and(|command| {
                    command.contains("--session \"$GEARBOX_WORKER_SESSION_ID\"")
                        && command.contains("< \"$GEARBOX_WORKER_PROMPT\"")
                })
        }));

        command.worker_sequence = Some("opencode:opencode/custom-free".to_string());
        let explicit_config = worker_config_from_command(&command)?;
        assert_eq!(explicit_config.worker_routes.len(), 1);
        assert_eq!(
            explicit_config.worker_routes[0].worker_model.as_deref(),
            Some("opencode/custom-free")
        );
        Ok(())
    }

    #[test]
    fn cli_worker_config_keeps_parallelism_limits() -> Result<()> {
        let mut command = run_command();
        command.max_parallel_workers = 3;
        command.max_parallel_per_key = 2;
        command.premium_worker_budget = 4;

        let config = worker_config_from_command(&command)?;

        assert_eq!(config.premium_worker_budget, 4);
        assert_eq!(config.max_parallel_workers, 3);
        assert_eq!(config.max_parallel_per_key, 2);
        Ok(())
    }

    #[test]
    fn cli_worker_config_uses_default_codex_command_when_unspecified() -> Result<()> {
        let mut command = run_command();
        command.worker = "codex".to_string();
        command.worker_model = Some("gpt-5".to_string());

        let config = worker_config_from_command(&command)?;

        assert_eq!(config.worker_kind, WorkerKind::Codex);
        assert!(
            config
                .worker_command
                .as_deref()
                .is_some_and(|command| command.contains("codex exec"))
        );
        assert!(
            config
                .worker_command
                .as_deref()
                .is_some_and(|command| command.contains("-m 'gpt-5'"))
        );
        Ok(())
    }

    #[test]
    fn cli_worker_config_uses_primary_kind_command_for_non_opencode_worker() -> Result<()> {
        let mut command = run_command();
        command.worker = "codex".to_string();
        command.codex_command = Some("codex exec".to_string());

        let config = worker_config_from_command(&command)?;

        assert_eq!(config.worker_kind, WorkerKind::Codex);
        assert_eq!(config.worker_command.as_deref(), Some("codex exec"));
        assert!(config.require_worker);
        Ok(())
    }

    #[test]
    fn cli_worker_config_rejects_unknown_sequence_worker() {
        let mut command = run_command();
        command.worker_sequence = Some("opencode,unknown".to_string());

        assert!(worker_config_from_command(&command).is_err());
    }
}
