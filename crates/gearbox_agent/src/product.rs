use crate::languages::{LanguageDetection, LanguageProfile};
use crate::state::{Goal, Task};
use crate::tools::{DiffSnapshot, ScopeCheck, ShellCommandResult};
use crate::workers::WorkerResult;

pub fn spec(goal: &Goal, detection: &LanguageDetection) -> String {
    let generation_guidance = generation_guidance(detection);
    format!(
        r#"# Spec

## Original Request

{}

## Gear Assumptions

- Product type: {}
- Language profile: {}
- Coordinator model: {}
- Evidence: {}
- Prefer reversible defaults when the prompt leaves details open.
- Keep the first implementation local and runnable.

## Features

- Create the smallest useful product that satisfies the request.
- Include local run instructions.
- Include verification commands and known limits.

## Non-goals

- No paid cloud dependency unless the user explicitly asks.
- No global dependency installation.
- No automatic git commit or push.

## Acceptance Criteria

{}

## Generation Guidance

{}

## Coordinator Brief

{}
"#,
        goal.request,
        goal.product_type,
        detection.profile.as_str(),
        coordinator_model_summary(goal),
        if detection.evidence.is_empty() {
            "none".to_string()
        } else {
            detection.evidence.join(", ")
        },
        goal.success_criteria
            .iter()
            .map(|criterion| format!("- {criterion}"))
            .collect::<Vec<_>>()
            .join("\n"),
        generation_guidance,
        coordinator_brief_summary(goal)
    )
}

pub fn plan(goal: &Goal, tasks: &[Task], detection: &LanguageDetection) -> String {
    let generation_guidance = generation_guidance(detection);
    let task_lines = tasks
        .iter()
        .map(|task| format!("- `{}`: {} ({:?})", task.id, task.title, task.kind))
        .collect::<Vec<_>>()
        .join("\n");
    let commands = if detection.verification_commands.is_empty() {
        "- No verification command detected yet.".to_string()
    } else {
        detection
            .verification_commands
            .iter()
            .map(|command| format!("- `{command}`"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        r#"# Plan

Goal: `{}`

## Execution Tasks

{}

## Default Build Path

- Use the coordinator model recorded below for Gear planning and review context when available.
- Confirm the workspace facts with deterministic tools.
- Follow the generation guidance below before writing code.
- Send bounded implementation work to the configured worker adapter.
- Inspect diff after the worker returns.
- Run Gear-owned verification commands.
- Create a repair task if verification fails.
- Produce final delivery notes.

## Generation Guidance

{}

## Coordinator Model

{}

## Coordinator Brief

{}

## Verification Commands

{}
"#,
        goal.id,
        task_lines,
        generation_guidance,
        coordinator_model_summary(goal),
        coordinator_brief_summary(goal),
        commands
    )
}

fn coordinator_model_summary(goal: &Goal) -> String {
    goal.coordinator_model
        .as_ref()
        .map(|model| format!("{} ({}/{})", model.name, model.provider_id, model.model_id))
        .unwrap_or_else(|| "not configured".to_string())
}

fn coordinator_brief_summary(goal: &Goal) -> String {
    goal.coordinator_brief
        .as_deref()
        .filter(|brief| !brief.trim().is_empty())
        .unwrap_or("not generated")
        .to_string()
}

fn generation_guidance(detection: &LanguageDetection) -> String {
    if detection.profile == LanguageProfile::TypeScript && detection.product_type == "web_app" {
        let existing_project = detection
            .evidence
            .iter()
            .any(|evidence| evidence == "package.json");
        if existing_project {
            return [
                "- Preserve the existing TypeScript/Web stack detected from the workspace.",
                "- Prefer existing package scripts and project layout.",
                "- Ensure README.md documents install, run, build, and test commands.",
            ]
            .join("\n");
        }

        return [
            "- Default stack: Vite + React + TypeScript with npm scripts.",
            "- Use plain CSS unless the prompt explicitly asks for another styling system.",
            "- Scaffold at minimum: package.json, index.html, src/main.tsx, src/App.tsx, src/styles.css, tsconfig.json, vite.config.ts, README.md.",
            "- package.json must include dev, build, and preview scripts.",
            "- README.md must document install, local run, build, and known limits.",
        ]
        .join("\n");
    }

    "- Use the smallest local runnable implementation that matches the detected language profile."
        .to_string()
}

pub fn verification(results: &[ShellCommandResult]) -> String {
    if results.is_empty() {
        return "# Verification\n\nNo verification commands were available.\n".to_string();
    }

    let mut contents = String::from("# Verification\n\n");
    for result in results {
        contents.push_str(&format!(
            "## `{}`\n\n- success: {}\n- exit_code: {}\n- duration_ms: {}\n\n",
            result.command,
            result.success,
            result
                .exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            result.duration_ms
        ));
        if !result.stdout.trim().is_empty() {
            contents.push_str("### stdout\n\n```text\n");
            contents.push_str(&result.stdout);
            contents.push_str("\n```\n\n");
        }
        if !result.stderr.trim().is_empty() {
            contents.push_str("### stderr\n\n```text\n");
            contents.push_str(&result.stderr);
            contents.push_str("\n```\n\n");
        }
    }
    contents
}

pub fn final_report(
    goal: &Goal,
    tasks: &[Task],
    worker_result: &WorkerResult,
    diff: &DiffSnapshot,
    scope_check: &ScopeCheck,
    verification_results: &[ShellCommandResult],
) -> String {
    let verification_summary = if verification_results.is_empty() {
        "No verification commands were available.".to_string()
    } else if verification_results.iter().all(|result| result.success) {
        "All verification commands passed.".to_string()
    } else {
        "One or more verification commands failed.".to_string()
    };

    let changed_files = if diff.changed_files.is_empty() {
        "- No code file changes detected outside `.gearbox-agent/`.".to_string()
    } else {
        diff.changed_files
            .iter()
            .map(|path| format!("- `{path}`"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let scope_summary = if scope_check.forbidden_touches.is_empty()
        && scope_check.outside_allowed_paths.is_empty()
        && !scope_check.max_files_exceeded
    {
        "Scope check passed.".to_string()
    } else {
        format!(
            "Scope check failed. forbidden_touches={}, outside_allowed_paths={}, changed_file_count={}, max_files_exceeded={}",
            scope_check.forbidden_touches.len(),
            scope_check.outside_allowed_paths.len(),
            scope_check.changed_file_count,
            scope_check.max_files_exceeded
        )
    };

    let task_lines = tasks
        .iter()
        .map(|task| format!("- `{}`: {:?} / {:?}", task.id, task.kind, task.status))
        .collect::<Vec<_>>()
        .join("\n");
    let evidence_chain = final_report_evidence(tasks, worker_result);

    format!(
        r#"# Final Report

Goal: `{}`

Status: `{}`

## Worker

- status: `{}`
- summary: {}
- packet: `{}`
- prompt: `{}`

## Coordinator Model

{}

## Coordinator Brief

{}

## Verification

{}

## Diff

{}

## Scope

{}

## Tasks

{}

## Evidence Chain

{}

## Known Limits

- ACP server integration is intentionally deferred until the local CLI runtime is stable.
- Codex, Claude Code, CodeGraph, and context-mode workers are not hard dependencies in this MVP.
"#,
        goal.id,
        goal.status.as_str(),
        worker_result.status.as_str(),
        worker_result.summary,
        worker_result.packet_path.display(),
        worker_result.prompt_path.display(),
        coordinator_model_summary(goal),
        coordinator_brief_summary(goal),
        verification_summary,
        changed_files,
        scope_summary,
        task_lines,
        evidence_chain
    )
}

fn final_report_evidence(tasks: &[Task], worker_result: &WorkerResult) -> String {
    let worker_evidence = [
        (
            "packet",
            worker_result.packet_path.to_string_lossy().to_string(),
        ),
        (
            "prompt",
            worker_result.prompt_path.to_string_lossy().to_string(),
        ),
        (
            "result",
            worker_result.result_path.to_string_lossy().to_string(),
        ),
        (
            "outcome",
            worker_result.outcome_path.to_string_lossy().to_string(),
        ),
    ]
    .into_iter()
    .map(|(label, path)| format!("- worker_{label}: `{path}`"))
    .collect::<Vec<_>>();

    let task_evidence = tasks
        .iter()
        .filter(|task| !task.outputs.evidence.is_empty())
        .flat_map(|task| {
            task.outputs
                .evidence
                .iter()
                .map(move |path| format!("- {} / {:?}: `{path}`", task.id, task.kind))
        })
        .collect::<Vec<_>>();

    worker_evidence
        .into_iter()
        .chain(task_evidence)
        .collect::<Vec<_>>()
        .join("\n")
}
