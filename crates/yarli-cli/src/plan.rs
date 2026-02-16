use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config;
use crate::config::{
    ensure_write_backend_guard, AutoAdvancePolicy, BackendSelection, LoadedConfig,
    RunPlanGuardModeConfig,
};

use yarli_cli::mode::RenderMode;
use yarli_cli::{self, prompt};
use yarli_core::domain::CommandClass;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_queue::{InMemoryTaskQueue, PostgresTaskQueue};
use yarli_store::{InMemoryEventStore, PostgresEventStore};

/// `yarli run start` — create a run, submit tasks, drive scheduler with stream output.
#[derive(Debug, Clone)]
pub(crate) struct PlannedTask {
    pub(crate) task_key: String,
    pub(crate) command: String,
    pub(crate) command_class: CommandClass,
    pub(crate) tranche_key: Option<String>,
    pub(crate) tranche_group: Option<String>,
    pub(crate) allowed_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PlannedTranche {
    pub(crate) key: String,
    pub(crate) objective: String,
    pub(crate) task_keys: Vec<String>,
    #[serde(default)]
    pub(crate) tranche_group: Option<String>,
}

/// Fully resolved run execution plan.
#[derive(Debug, Clone)]
pub(crate) struct RunPlan {
    pub(crate) objective: String,
    pub(crate) tasks: Vec<PlannedTask>,
    pub(crate) task_catalog: Vec<PlannedTask>,
    pub(crate) workdir: String,
    pub(crate) timeout_secs: u64,
    pub(crate) pace: Option<String>,
    pub(crate) prompt_snapshot: Option<yarli_cli::prompt::PromptSnapshot>,
    pub(crate) run_spec: Option<yarli_cli::prompt::RunSpec>,
    pub(crate) tranche_plan: Vec<PlannedTranche>,
    pub(crate) current_tranche_index: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct RunExecutionOutcome {
    pub(crate) run_id: Uuid,
    pub(crate) run_state: RunState,
    pub(crate) continuation_payload: yarli_core::entities::ContinuationPayload,
    pub(crate) token_totals: RunTokenTotals,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RunTokenTotals {
    pub(crate) prompt_tokens: u64,
    pub(crate) completion_tokens: u64,
    pub(crate) total_tokens: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct IterationMetrics {
    pub(crate) iteration: usize,
    pub(crate) run_id: Uuid,
    pub(crate) duration: Duration,
    pub(crate) token_totals: RunTokenTotals,
}

pub(crate) use crate::tranche::{
    cmd_plan_tranche_add, cmd_plan_tranche_complete, cmd_plan_tranche_list,
    cmd_plan_tranche_remove, cmd_plan_validate, discover_plan_dispatch_entries,
    enforce_plan_guard_post_run, maybe_mark_current_structured_tranche_complete,
    plan_path_for_prompt_entry, read_tranches_file_in, run_spec_plan_guard_preflight,
    tranches_file_path,
};
#[cfg(test)]
pub(crate) use crate::tranche::{
    plan_target_completion_state, run_spec_plan_guard_preflight_with_override,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImplementationPlanEntry {
    pub(crate) key: String,
    pub(crate) summary: String,
    pub(crate) is_complete: bool,
    pub(crate) tranche_group: Option<String>,
    pub(crate) allowed_paths: Vec<String>,
}

// ---------------------------------------------------------------------------
// Structured tranches file (.yarli/tranches.toml)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TrancheStatus {
    Incomplete,
    Complete,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TrancheDefinition {
    pub(crate) key: String,
    pub(crate) summary: String,
    pub(crate) status: TrancheStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) group: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) allowed_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TranchesFile {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) tranches: Vec<TrancheDefinition>,
}

impl TranchesFile {
    pub(crate) fn to_entries(&self) -> Vec<ImplementationPlanEntry> {
        self.tranches
            .iter()
            .map(|def| ImplementationPlanEntry {
                key: def.key.clone(),
                summary: def.summary.clone(),
                is_complete: def.status == TrancheStatus::Complete,
                tranche_group: def.group.clone(),
                allowed_paths: def.allowed_paths.clone(),
            })
            .collect()
    }
}

pub(crate) use crate::workspace::{
    merge_parallel_workspace_results, prepare_parallel_workspace_layout,
};

pub(crate) fn resolve_run_plan(
    loaded_config: &LoadedConfig,
    objective: String,
    commands: Vec<String>,
    pace: Option<String>,
    workdir: Option<String>,
    timeout_secs: Option<u64>,
    default_pace_fallback: Option<&str>,
) -> Result<RunPlan> {
    if !commands.is_empty() && pace.is_some() {
        bail!("--cmd and --pace are mutually exclusive");
    }

    let mut selected_pace = pace
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    if commands.is_empty() && selected_pace.is_none() {
        if let Some(fallback) = default_pace_fallback {
            if loaded_config.config().run.paces.contains_key(fallback) {
                selected_pace = Some(fallback.to_string());
            } else {
                selected_pace = loaded_config
                    .config()
                    .run
                    .default_pace
                    .clone()
                    .filter(|value| !value.trim().is_empty());
            }
        } else {
            selected_pace = loaded_config
                .config()
                .run
                .default_pace
                .clone()
                .filter(|value| !value.trim().is_empty());
        }
    }

    let (commands, pace_name, pace_workdir, pace_timeout) = if commands.is_empty() {
        let pace_name = selected_pace.ok_or_else(|| {
            anyhow::anyhow!(
                "at least one --cmd is required (or configure [run] paces in {})",
                loaded_config.path().display()
            )
        })?;

        let pace = loaded_config
            .config()
            .run
            .paces
            .get(&pace_name)
            .ok_or_else(|| {
                let mut available: Vec<&str> = loaded_config
                    .config()
                    .run
                    .paces
                    .keys()
                    .map(|s| s.as_str())
                    .collect();
                available.sort_unstable();
                if available.is_empty() {
                    anyhow::anyhow!(
                        "unknown pace {pace_name:?} (no paces configured in {})",
                        loaded_config.path().display()
                    )
                } else {
                    anyhow::anyhow!(
                        "unknown pace {pace_name:?} (available: {})",
                        available.join(", ")
                    )
                }
            })?;

        if pace.cmds.is_empty() {
            bail!("pace {pace_name:?} has no cmds configured");
        }

        (
            pace.cmds.clone(),
            Some(pace_name),
            pace.working_dir.clone(),
            pace.command_timeout_seconds,
        )
    } else {
        (commands, None, None, None)
    };

    let tasks = commands
        .into_iter()
        .enumerate()
        .map(|(idx, cmd)| PlannedTask {
            task_key: format!("task-{}", idx + 1),
            command: cmd,
            command_class: CommandClass::Io,
            tranche_key: None,
            tranche_group: None,
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();

    let selected_workdir = workdir
        .or(pace_workdir)
        .unwrap_or_else(|| loaded_config.config().execution.working_dir.clone());
    let selected_timeout_secs = timeout_secs
        .or(pace_timeout)
        .unwrap_or(loaded_config.config().execution.command_timeout_seconds);

    let task_catalog = tasks.clone();
    Ok(RunPlan {
        objective,
        tasks,
        task_catalog,
        workdir: selected_workdir,
        timeout_secs: selected_timeout_secs,
        pace: pace_name,
        prompt_snapshot: None,
        run_spec: None,
        tranche_plan: Vec::new(),
        current_tranche_index: None,
    })
}

pub(crate) fn sanitize_task_key_component(raw: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in raw.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' {
            if prev_dash {
                continue;
            }
            prev_dash = true;
        } else {
            prev_dash = false;
        }
        out.push(mapped);
    }
    out.trim_matches('-').to_string()
}

pub(crate) fn build_tranche_task_prompt(
    loaded_prompt: &prompt::LoadedPrompt,
    plan_path: &Path,
    objective: &str,
    tranche: &ImplementationPlanEntry,
    index: usize,
    total: usize,
    enforce_allowed_paths: bool,
) -> String {
    let tranche_group_line = tranche
        .tranche_group
        .as_ref()
        .map(|group| format!("Tranche group: {}.\n", group))
        .unwrap_or_default();
    let allowed_paths_line = if enforce_allowed_paths && !tranche.allowed_paths.is_empty() {
        format!(
            "Allowed file scope: {}.\n",
            tranche.allowed_paths.join(", ")
        )
    } else {
        String::new()
    };
    let allowed_paths_instruction = if enforce_allowed_paths && !tranche.allowed_paths.is_empty() {
        "5. Restrict edits to the allowed file scope above.\n6. Keep output concise and concrete."
    } else {
        "5. Keep output concise and concrete."
    };
    format!(
        "YARLI tranche task {}/{}.\nObjective: {}\nPrompt file: {}\nPlan file: {}\nTarget tranche: {}.\nTarget summary: {}\n{}{}Mode: implementation.\n\nInstructions:\n1. Read PROMPT and plan context from the workspace paths above.\n2. Implement only the target tranche if it is still incomplete.\n3. Update IMPLEMENTATION_PLAN.md and evidence in-repo.\n4. Run the tranche's required verification commands before finishing.\n{}",
        index + 1,
        total,
        objective,
        loaded_prompt.entry_path.display(),
        plan_path.display(),
        tranche.key,
        tranche.summary,
        tranche_group_line,
        allowed_paths_line,
        allowed_paths_instruction
    )
}

pub(crate) fn build_verification_task_prompt(
    loaded_prompt: &prompt::LoadedPrompt,
    plan_path: &Path,
    objective: &str,
    open_tranche_count: usize,
) -> String {
    format!(
        "YARLI verification task.\nObjective: {}\nPrompt file: {}\nPlan file: {}\nOpen tranche count seen at dispatch: {}.\nMode: verification-only.\n\nInstructions:\n1. Verify current workspace state against PROMPT.md and IMPLEMENTATION_PLAN.md.\n2. Run verification commands and capture concrete results.\n3. Update evidence/status text in-repo only if needed.\n4. Do not invent completion claims.",
        objective,
        loaded_prompt.entry_path.display(),
        plan_path.display(),
        open_tranche_count
    )
}

pub(crate) fn build_grouped_plan_tranches(
    objective: &str,
    entry_task_pairs: &[(ImplementationPlanEntry, String)],
    enable_grouping: bool,
    max_grouped_tasks_per_tranche: u32,
) -> Vec<PlannedTranche> {
    let grouped_task_cap = if max_grouped_tasks_per_tranche == 0 {
        usize::MAX
    } else {
        max_grouped_tasks_per_tranche as usize
    };
    let mut tranches: Vec<PlannedTranche> = Vec::new();

    for (idx, (entry, task_key)) in entry_task_pairs.iter().enumerate() {
        if enable_grouping {
            if let Some(group) = entry.tranche_group.as_deref() {
                if let Some(last) = tranches.last_mut() {
                    if last.tranche_group.as_deref() == Some(group)
                        && last.task_keys.len() < grouped_task_cap
                    {
                        last.task_keys.push(task_key.clone());
                        continue;
                    }
                }

                tranches.push(PlannedTranche {
                    key: format!("group-{:03}-{}", idx + 1, group),
                    objective: format!("{objective} [group:{group}]"),
                    task_keys: vec![task_key.clone()],
                    tranche_group: Some(group.to_string()),
                });
                continue;
            }
        }

        tranches.push(PlannedTranche {
            key: entry.key.clone(),
            objective: format!("{objective} [{}]", entry.key),
            task_keys: vec![task_key.clone()],
            tranche_group: entry.tranche_group.clone(),
        });
    }

    tranches
}

pub(crate) fn build_plan_driven_run_sequence(
    loaded_config: &LoadedConfig,
    loaded_prompt: &prompt::LoadedPrompt,
    objective: &str,
) -> Result<(Vec<PlannedTask>, Vec<PlannedTranche>)> {
    let plan_path = plan_path_for_prompt_entry(&loaded_prompt.entry_path)?;
    let tranches_base_dir = plan_path
        .parent()
        .context("plan file has no parent directory")?;
    let plan_text = fs::read_to_string(&plan_path)
        .with_context(|| format!("failed to read plan file {}", plan_path.display()))?;

    let open_tranches: Vec<ImplementationPlanEntry> = match read_tranches_file_in(tranches_base_dir)
    {
        Ok(Some(tf)) => {
            info!(
                count = tf.tranches.len(),
                tranches_file = %tranches_file_path(tranches_base_dir).display(),
                "using structured tranches file"
            );
            tf.to_entries()
                .into_iter()
                .filter(|e| !e.is_complete)
                .collect()
        }
        Ok(None) => {
            debug!(
                tranches_file = %tranches_file_path(tranches_base_dir).display(),
                "no structured tranches file, falling back to markdown parsing"
            );
            discover_plan_dispatch_entries(&plan_text)
                .into_iter()
                .filter(|e| !e.is_complete)
                .collect()
        }
        Err(e) => {
            warn!(
                error = %e,
                tranches_file = %tranches_file_path(tranches_base_dir).display(),
                "malformed tranches file, falling back to markdown parsing"
            );
            discover_plan_dispatch_entries(&plan_text)
                .into_iter()
                .filter(|e| !e.is_complete)
                .collect()
        }
    };

    if open_tranches.is_empty() && plan_text.len() > 50 {
        let total_parsed = discover_plan_dispatch_entries(&plan_text).len();
        warn!(
            plan_lines = plan_text.lines().count(),
            parsed_entries = total_parsed,
            "no open tranches found in plan — dispatch will be verification-only"
        );
    }

    let cli_invocation = config::resolve_cli_invocation_config(loaded_config)?;
    let mut task_catalog = Vec::new();
    let mut entry_task_pairs = Vec::new();

    for (idx, tranche) in open_tranches.iter().enumerate() {
        let component = sanitize_task_key_component(&tranche.key);
        let suffix = if component.is_empty() {
            format!("tranche-{}", idx + 1)
        } else {
            component
        };
        let task_key = format!("tranche-{:03}-{suffix}", idx + 1);
        let prompt_text = build_tranche_task_prompt(
            loaded_prompt,
            &plan_path,
            objective,
            tranche,
            idx,
            open_tranches.len(),
            loaded_config
                .config()
                .run
                .enforce_plan_tranche_allowed_paths,
        );
        let command = config::build_cli_command(&cli_invocation, &prompt_text);
        task_catalog.push(PlannedTask {
            task_key: task_key.clone(),
            command,
            command_class: CommandClass::Tool,
            tranche_key: Some(tranche.key.clone()),
            tranche_group: tranche.tranche_group.clone(),
            allowed_paths: tranche.allowed_paths.clone(),
        });
        entry_task_pairs.push((tranche.clone(), task_key));
    }

    let mut tranche_plan = build_grouped_plan_tranches(
        objective,
        &entry_task_pairs,
        loaded_config.config().run.enable_plan_tranche_grouping,
        loaded_config.config().run.max_grouped_tasks_per_tranche,
    );

    let verification_key = format!("verify-{:03}", open_tranches.len() + 1);
    let verification_prompt =
        build_verification_task_prompt(loaded_prompt, &plan_path, objective, open_tranches.len());
    let verification_command = config::build_cli_command(&cli_invocation, &verification_prompt);
    task_catalog.push(PlannedTask {
        task_key: verification_key.clone(),
        command: verification_command,
        command_class: CommandClass::Tool,
        tranche_key: Some("verification".to_string()),
        tranche_group: None,
        allowed_paths: Vec::new(),
    });
    tranche_plan.push(PlannedTranche {
        key: "verification".to_string(),
        objective: format!("{objective} [verification]"),
        task_keys: vec![verification_key],
        tranche_group: None,
    });

    Ok((task_catalog, tranche_plan))
}

pub(crate) fn build_plain_prompt_run_sequence(
    loaded_config: &LoadedConfig,
    loaded_prompt: &prompt::LoadedPrompt,
    objective: &str,
) -> Result<(Vec<PlannedTask>, Vec<PlannedTranche>)> {
    let cli_invocation = config::resolve_cli_invocation_config(loaded_config)?;
    let command = config::build_cli_command(&cli_invocation, &loaded_prompt.expanded_text);
    let task_key = "prompt-001".to_string();
    let task_catalog = vec![PlannedTask {
        task_key: task_key.clone(),
        command,
        command_class: CommandClass::Tool,
        tranche_key: Some("prompt".to_string()),
        tranche_group: None,
        allowed_paths: Vec::new(),
    }];
    let tranche_plan = vec![PlannedTranche {
        key: "prompt".to_string(),
        objective: format!("{objective} [prompt]"),
        task_keys: vec![task_key],
        tranche_group: None,
    }];

    Ok((task_catalog, tranche_plan))
}

pub(crate) fn parse_command_class(label: &str) -> Result<CommandClass> {
    match label {
        "io" => Ok(CommandClass::Io),
        "cpu" => Ok(CommandClass::Cpu),
        "git" => Ok(CommandClass::Git),
        "tool" => Ok(CommandClass::Tool),
        other => bail!("unknown command class {other:?}"),
    }
}

pub(crate) fn run_config_has_run_spec_data(run_config: &config::RunConfig) -> bool {
    run_config
        .objective
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || !run_config.tasks.is_empty()
        || !run_config.tranches.is_empty()
        || run_config.plan_guard.is_some()
}

pub(crate) fn run_spec_from_run_config(run_config: &config::RunConfig) -> prompt::RunSpec {
    let objective = run_config
        .objective
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());
    let tasks = prompt::RunSpecTasks {
        items: run_config
            .tasks
            .iter()
            .map(|task| prompt::RunSpecTask {
                key: task.key.clone(),
                cmd: task.cmd.clone(),
                class: task.class.clone(),
            })
            .collect(),
    };
    let tranches = if run_config.tranches.is_empty() {
        None
    } else {
        Some(prompt::RunSpecTranches {
            items: run_config
                .tranches
                .iter()
                .map(|tranche| prompt::RunSpecTranche {
                    key: tranche.key.clone(),
                    objective: tranche.objective.clone(),
                    task_keys: tranche.task_keys.clone(),
                })
                .collect(),
        })
    };
    let plan_guard = run_config
        .plan_guard
        .as_ref()
        .map(|guard| prompt::RunSpecPlanGuard {
            target: guard.target.clone(),
            mode: match guard.mode {
                RunPlanGuardModeConfig::Implement => prompt::RunSpecPlanGuardMode::Implement,
                RunPlanGuardModeConfig::VerifyOnly => prompt::RunSpecPlanGuardMode::VerifyOnly,
            },
        });

    prompt::RunSpec {
        version: 1,
        objective,
        tasks,
        tranches,
        plan_guard,
    }
}

pub(crate) fn merge_run_specs(
    base_spec: &prompt::RunSpec,
    prompt_override: Option<&prompt::RunSpec>,
) -> prompt::RunSpec {
    let Some(prompt_spec) = prompt_override else {
        return base_spec.clone();
    };

    let mut merged = base_spec.clone();
    merged.version = prompt_spec.version;

    if let Some(prompt_objective) = prompt_spec
        .objective
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        merged.objective = Some(prompt_objective.to_string());
    }

    let mut task_indexes: HashMap<String, usize> = merged
        .tasks
        .items
        .iter()
        .enumerate()
        .map(|(idx, task)| (task.key.clone(), idx))
        .collect();
    for task in &prompt_spec.tasks.items {
        if let Some(idx) = task_indexes.get(&task.key).copied() {
            merged.tasks.items[idx] = task.clone();
            continue;
        }
        let next_idx = merged.tasks.items.len();
        merged.tasks.items.push(task.clone());
        task_indexes.insert(task.key.clone(), next_idx);
    }

    if prompt_spec.tranches.is_some() {
        merged.tranches = prompt_spec.tranches.clone();
    }

    if prompt_spec.plan_guard.is_some() {
        merged.plan_guard = prompt_spec.plan_guard.clone();
    }

    merged
}

pub(crate) fn build_task_catalog_from_run_spec(
    run_spec: &yarli_cli::prompt::RunSpec,
) -> Result<Vec<PlannedTask>> {
    run_spec
        .tasks
        .items
        .iter()
        .map(|t| {
            let class = parse_command_class(t.class.as_deref().unwrap_or("io"))
                .with_context(|| format!("for task key {}", t.key))?;
            Ok(PlannedTask {
                task_key: t.key.clone(),
                command: t.cmd.clone(),
                command_class: class,
                tranche_key: None,
                tranche_group: None,
                allowed_paths: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>>>()
}

pub(crate) fn build_tranche_plan_from_run_spec(
    run_spec: &yarli_cli::prompt::RunSpec,
    default_objective: &str,
) -> Result<Vec<PlannedTranche>> {
    let task_keys: Vec<String> = run_spec
        .tasks
        .items
        .iter()
        .map(|task| task.key.clone())
        .collect();

    let tranches = if let Some(explicit) = run_spec.tranches.as_ref() {
        explicit
            .items
            .iter()
            .map(|tranche| PlannedTranche {
                key: tranche.key.clone(),
                objective: tranche
                    .objective
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| format!("{default_objective} [{}]", tranche.key)),
                task_keys: tranche.task_keys.clone(),
                tranche_group: None,
            })
            .collect()
    } else {
        vec![PlannedTranche {
            key: "default".to_string(),
            objective: default_objective.to_string(),
            task_keys,
            tranche_group: None,
        }]
    };

    if tranches.is_empty() {
        bail!("run spec must resolve to at least one tranche");
    }

    validate_tranche_keys(&tranches)?;

    Ok(tranches)
}

/// Reject tranche keys that look like stale legacy artifacts leaked from handoff docs.
///
/// Stale patterns:
/// - UUID-formatted keys (e.g. `550e8400-e29b-41d4-a716-446655440000`)
/// - `run-*`, `app-*`, `Target-*` prefixed keys
/// - Empty or whitespace-only keys
pub(crate) fn validate_tranche_keys(tranches: &[PlannedTranche]) -> Result<()> {
    for tranche in tranches {
        let key = tranche.key.trim();
        if key.is_empty() {
            bail!("tranche key must not be empty");
        }
        if looks_like_uuid(key) {
            bail!(
                "tranche key '{}' looks like a stale UUID — use a descriptive key instead",
                key
            );
        }
        let lower = key.to_ascii_lowercase();
        for prefix in &["run-", "app-", "target-"] {
            if lower.starts_with(prefix) {
                bail!(
                    "tranche key '{}' uses reserved prefix '{}' — use a descriptive key instead",
                    key,
                    prefix.trim_end_matches('-')
                );
            }
        }
    }
    Ok(())
}

/// Check if a string looks like a UUID (8-4-4-4-12 hex pattern).
pub(crate) fn looks_like_uuid(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 5 {
        return false;
    }
    let expected_lens = [8, 4, 4, 4, 12];
    parts
        .iter()
        .zip(expected_lens.iter())
        .all(|(part, &len)| part.len() == len && part.chars().all(|c| c.is_ascii_hexdigit()))
}

pub(crate) fn tasks_for_tranche(
    task_catalog: &[PlannedTask],
    tranche: &PlannedTranche,
) -> Result<Vec<PlannedTask>> {
    let catalog_by_key: HashMap<&str, &PlannedTask> = task_catalog
        .iter()
        .map(|task| (task.task_key.as_str(), task))
        .collect();
    let mut tasks = Vec::new();
    for task_key in &tranche.task_keys {
        let task = catalog_by_key.get(task_key.as_str()).ok_or_else(|| {
            anyhow::anyhow!(
                "tranche {} references unknown task key {}",
                tranche.key,
                task_key
            )
        })?;
        tasks.push((*task).clone());
    }
    Ok(tasks)
}

pub(crate) fn parse_task_catalog_from_snapshot(
    config_snapshot: &serde_json::Value,
) -> Vec<PlannedTask> {
    let from_entries = |entries: &Vec<serde_json::Value>| {
        entries
            .iter()
            .filter_map(|task| {
                let task_key = task.get("task_key")?.as_str()?.to_string();
                let command = task.get("command")?.as_str()?.to_string();
                let class_str = task
                    .get("command_class")
                    .and_then(|value| value.as_str())
                    .unwrap_or("Io")
                    .to_ascii_lowercase();
                let command_class = match class_str.as_str() {
                    "io" => CommandClass::Io,
                    "cpu" => CommandClass::Cpu,
                    "git" => CommandClass::Git,
                    "tool" => CommandClass::Tool,
                    _ => CommandClass::Io,
                };
                Some(PlannedTask {
                    task_key,
                    command,
                    command_class,
                    tranche_key: task
                        .get("tranche_key")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string)
                        .filter(|value| !value.trim().is_empty()),
                    tranche_group: task
                        .get("tranche_group")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string)
                        .filter(|value| !value.trim().is_empty()),
                    allowed_paths: task
                        .get("allowed_paths")
                        .and_then(|value| value.as_array())
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item.as_str().map(ToString::to_string))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                })
            })
            .collect::<Vec<_>>()
    };

    let runtime = config_snapshot.get("runtime");
    if let Some(entries) = runtime
        .and_then(|runtime| runtime.get("task_catalog"))
        .and_then(|tasks| tasks.as_array())
    {
        let parsed = from_entries(entries);
        if !parsed.is_empty() {
            return parsed;
        }
    }

    runtime
        .and_then(|runtime| runtime.get("tasks"))
        .and_then(|tasks| tasks.as_array())
        .map(from_entries)
        .unwrap_or_default()
}

pub(crate) fn parse_tranche_plan_from_snapshot(
    config_snapshot: &serde_json::Value,
) -> Vec<PlannedTranche> {
    config_snapshot
        .get("runtime")
        .and_then(|runtime| runtime.get("tranche_plan"))
        .and_then(|tranches| tranches.as_array())
        .map(|tranches| {
            tranches
                .iter()
                .filter_map(|tranche| {
                    let key = tranche.get("key")?.as_str()?.to_string();
                    let objective = tranche
                        .get("objective")
                        .and_then(|value| value.as_str())
                        .unwrap_or("yarli run")
                        .to_string();
                    let task_keys = tranche
                        .get("task_keys")?
                        .as_array()?
                        .iter()
                        .filter_map(|value| value.as_str().map(ToString::to_string))
                        .collect::<Vec<_>>();
                    let tranche_group = tranche
                        .get("tranche_group")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string)
                        .filter(|value| !value.trim().is_empty());
                    if task_keys.is_empty() {
                        return None;
                    }
                    Some(PlannedTranche {
                        key,
                        objective,
                        task_keys,
                        tranche_group,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub(crate) fn parse_current_tranche_index_from_snapshot(
    config_snapshot: &serde_json::Value,
) -> Option<usize> {
    let value = config_snapshot
        .get("runtime")
        .and_then(|runtime| runtime.get("current_tranche_index"))?
        .as_u64()?;
    usize::try_from(value).ok()
}

pub(crate) fn parse_prompt_snapshot_from_snapshot(
    config_snapshot: &serde_json::Value,
) -> Option<prompt::PromptSnapshot> {
    let prompt_value = config_snapshot.get("runtime")?.get("prompt")?;
    if prompt_value.is_null() {
        return None;
    }
    serde_json::from_value(prompt_value.clone()).ok()
}

pub(crate) async fn cmd_run_start(
    plan: RunPlan,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
    allow_recursive_run_override: bool,
) -> Result<()> {
    let outcome = execute_run_plan(
        plan,
        render_mode,
        loaded_config,
        allow_recursive_run_override,
    )
    .await?;
    finalize_run_outcome(&outcome)
}

pub(crate) async fn execute_run_plan(
    plan: RunPlan,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
    allow_recursive_run_override: bool,
) -> Result<RunExecutionOutcome> {
    ensure_write_backend_guard(loaded_config, "run start")?;
    config::ensure_parallel_workspace_contract(loaded_config)?;

    match loaded_config.backend_selection()? {
        BackendSelection::InMemory => {
            println!("Using backend: in-memory");
            let store = Arc::new(InMemoryEventStore::new());
            let queue = Arc::new(InMemoryTaskQueue::new());
            crate::cmd_run_start_with_backend(
                plan.clone(),
                render_mode,
                loaded_config,
                store,
                queue,
                allow_recursive_run_override,
            )
            .await
        }
        BackendSelection::Postgres { database_url } => {
            println!("Using backend: postgres");
            let store =
                Arc::new(PostgresEventStore::new(&database_url).map_err(|e| {
                    anyhow::anyhow!("failed to initialize postgres event store: {e}")
                })?);
            let queue =
                Arc::new(PostgresTaskQueue::new(&database_url).map_err(|e| {
                    anyhow::anyhow!("failed to initialize postgres task queue: {e}")
                })?);
            crate::cmd_run_start_with_backend(
                plan,
                render_mode,
                loaded_config,
                store,
                queue,
                allow_recursive_run_override,
            )
            .await
        }
    }
}

pub(crate) fn finalize_run_outcome(outcome: &RunExecutionOutcome) -> Result<()> {
    print_run_summary(outcome);

    match outcome.run_state {
        RunState::RunCompleted => {
            println!("Run {} completed successfully.", outcome.run_id);
            Ok(())
        }
        RunState::RunFailed => {
            if outcome
                .continuation_payload
                .next_tranche
                .as_ref()
                .is_some_and(|t| {
                    t.kind == yarli_core::entities::continuation::TrancheKind::GateRetry
                })
            {
                eprintln!(
                    "Hint: all tasks completed but run-level gates failed. \
                     Use `yarli run continue` to re-verify after addressing the gate issue."
                );
            }
            bail!("Run {} failed.", outcome.run_id)
        }
        RunState::RunCancelled => {
            println!("Run {} cancelled.", outcome.run_id);
            process::exit(130);
        }
        other => bail!(
            "Run {} ended in unexpected state: {other:?}",
            outcome.run_id
        ),
    }
}

pub(crate) fn print_run_summary(outcome: &RunExecutionOutcome) {
    let payload = &outcome.continuation_payload;
    let summary = &payload.summary;

    // Compute duration from UUIDv7 timestamp to completed_at
    let duration_str = uuid_v7_timestamp(outcome.run_id)
        .map(|start| {
            let dur = payload.completed_at.signed_duration_since(start);
            let total_secs = dur.num_seconds().max(0);
            let mins = total_secs / 60;
            let secs = total_secs % 60;
            if mins > 0 {
                format!("{mins}m {secs:02}s")
            } else {
                format!("{secs}s")
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    let exit_reason_str = payload
        .exit_reason
        .map(|r| r.to_string())
        .unwrap_or_else(|| "none".to_string());
    let cancellation_source_str = payload
        .cancellation_source
        .map(|source| source.to_string())
        .unwrap_or_else(|| "none".to_string());

    let bar: String = "═".repeat(75);
    let suffix: String = "═".repeat(59);

    println!();
    println!("═══ Run Summary {suffix}");
    println!("  Run ID:      {}", outcome.run_id);
    println!("  Objective:   {}", payload.objective);
    println!("  Exit state:  {:?}", outcome.run_state);
    println!("  Exit reason: {exit_reason_str}");
    println!("  Cancel src:  {cancellation_source_str}");
    if outcome.run_state == RunState::RunCancelled || payload.cancellation_provenance.is_some() {
        println!(
            "  Cancel prov: {}",
            format_cancel_provenance_summary(payload.cancellation_provenance.as_ref())
        );
    }
    println!(
        "  Tasks:       {} completed, {} failed, {} pending",
        summary.completed, summary.failed, summary.pending
    );
    println!(
        "  Tokens:      total={} (prompt={} completion={})",
        outcome.token_totals.total_tokens,
        outcome.token_totals.prompt_tokens,
        outcome.token_totals.completion_tokens
    );
    println!("  Duration:    {duration_str}");

    // Show failed tasks with their blocker/error
    if summary.failed > 0 {
        for task in &payload.tasks {
            if task.state == TaskState::TaskFailed {
                let detail = task
                    .blocker
                    .as_ref()
                    .map(|b| format!("{b:?}"))
                    .or_else(|| task.last_error.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                println!("  Failed:      {} ({})", task.task_key, detail);
            }
        }
    }

    println!("{bar}");
    println!();
}

pub(crate) fn format_cancel_provenance_summary(
    provenance: Option<&yarli_core::domain::CancellationProvenance>,
) -> String {
    let signal = provenance
        .and_then(|p| p.signal_name.as_deref())
        .unwrap_or("unknown");
    let sender = provenance
        .and_then(|p| p.sender_pid)
        .map(|pid| pid.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let receiver = provenance
        .and_then(|p| p.receiver_pid)
        .map(|pid| format!("yarli({pid})"))
        .unwrap_or_else(|| "unknown".to_string());
    let actor = provenance
        .and_then(|p| p.actor_kind)
        .map(|kind| kind.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let stage = provenance
        .and_then(|p| p.stage)
        .map(|stage| stage.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    format!("signal={signal} sender={sender} receiver={receiver} actor={actor} stage={stage}")
}

pub(crate) fn format_wall_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let mins = total_secs / 60;
    let secs = total_secs % 60;
    if mins > 0 {
        format!("{mins}m {secs:02}s")
    } else {
        format!("{secs}s")
    }
}

pub(crate) fn print_iteration_metrics(
    iteration: usize,
    outcome: &RunExecutionOutcome,
    duration: Duration,
) {
    let tokens = outcome.token_totals;
    println!(
        "Iteration {iteration} metrics: run_id={} tokens total={} (prompt={} completion={}), duration={}",
        outcome.run_id,
        tokens.total_tokens,
        tokens.prompt_tokens,
        tokens.completion_tokens,
        format_wall_duration(duration)
    );
}

pub(crate) fn print_invocation_summary(iterations: &[IterationMetrics]) {
    if iterations.is_empty() {
        return;
    }

    let tranche_count = iterations.len();
    let first_iteration = iterations.first().map(|item| item.iteration).unwrap_or(1);
    let last_iteration = iterations.last().map(|item| item.iteration).unwrap_or(1);
    let last_run_id = iterations
        .last()
        .map(|item| item.run_id.to_string())
        .unwrap_or_else(|| "-".to_string());
    let total_prompt_tokens: u64 = iterations
        .iter()
        .map(|item| item.token_totals.prompt_tokens)
        .sum();
    let total_completion_tokens: u64 = iterations
        .iter()
        .map(|item| item.token_totals.completion_tokens)
        .sum();
    let total_tokens: u64 = iterations
        .iter()
        .map(|item| item.token_totals.total_tokens)
        .sum();
    let total_duration = iterations.iter().fold(Duration::ZERO, |acc, item| {
        acc.saturating_add(item.duration)
    });
    let avg_duration = if tranche_count == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(total_duration.as_secs_f64() / tranche_count as f64)
    };

    println!(
        "Final invocation summary: iterations={}..{} tranches={} total_tokens={} (prompt={} completion={}) total_duration={} avg_per_tranche={} last_run={}",
        first_iteration,
        last_iteration,
        tranche_count,
        total_tokens,
        total_prompt_tokens,
        total_completion_tokens,
        format_wall_duration(total_duration),
        format_wall_duration(avg_duration),
        last_run_id,
    );
}

pub(crate) fn uuid_v7_timestamp(id: Uuid) -> Option<chrono::DateTime<chrono::Utc>> {
    // UUIDv7: first 48 bits are unix timestamp in milliseconds
    let bytes = id.as_bytes();
    let version = (bytes[6] >> 4) & 0x0F;
    if version != 7 {
        return None;
    }
    let ms = ((bytes[0] as u64) << 40)
        | ((bytes[1] as u64) << 32)
        | ((bytes[2] as u64) << 24)
        | ((bytes[3] as u64) << 16)
        | ((bytes[4] as u64) << 8)
        | (bytes[5] as u64);
    chrono::DateTime::from_timestamp_millis(ms as i64).map(|dt| dt.to_utc())
}

pub(crate) fn build_plan_from_continuation_tranche(
    tranche: &yarli_core::entities::continuation::TrancheSpec,
    loaded_config: &LoadedConfig,
) -> Result<RunPlan> {
    let mut task_keys: Vec<String> = match tranche.kind {
        yarli_core::entities::continuation::TrancheKind::PlannedNext
            if !tranche.planned_task_keys.is_empty() =>
        {
            tranche.planned_task_keys.clone()
        }
        _ => {
            let mut keys = tranche.retry_task_keys.clone();
            keys.extend(tranche.unfinished_task_keys.iter().cloned());
            if keys.is_empty() {
                keys = tranche.planned_task_keys.clone();
            }
            keys
        }
    };

    let mut seen = HashSet::new();
    task_keys.retain(|key| seen.insert(key.clone()));

    if task_keys.is_empty() {
        bail!("continuation spec has no tasks to dispatch");
    }

    let catalog = parse_task_catalog_from_snapshot(&tranche.config_snapshot);
    let catalog_by_key: HashMap<&str, &PlannedTask> = catalog
        .iter()
        .map(|task| (task.task_key.as_str(), task))
        .collect();

    let tasks: Vec<PlannedTask> = task_keys
        .into_iter()
        .map(|key| {
            if let Some(task) = catalog_by_key.get(key.as_str()) {
                (*task).clone()
            } else {
                PlannedTask {
                    task_key: key.clone(),
                    command: key,
                    command_class: CommandClass::Io,
                    tranche_key: None,
                    tranche_group: None,
                    allowed_paths: Vec::new(),
                }
            }
        })
        .collect();

    let current_tranche_index = tranche
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.next_tranche_index.or(cursor.current_tranche_index))
        .or_else(|| parse_current_tranche_index_from_snapshot(&tranche.config_snapshot));

    let workdir = tranche
        .config_snapshot
        .get("runtime")
        .and_then(|runtime| runtime.get("working_dir"))
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .unwrap_or_else(|| loaded_config.config().execution.working_dir.clone());
    let timeout_secs = tranche
        .config_snapshot
        .get("runtime")
        .and_then(|runtime| runtime.get("timeout_secs"))
        .and_then(|value| value.as_u64())
        .unwrap_or(loaded_config.config().execution.command_timeout_seconds);

    let task_catalog = if catalog.is_empty() {
        tasks.clone()
    } else {
        catalog
    };
    Ok(RunPlan {
        objective: tranche.suggested_objective.clone(),
        tasks,
        task_catalog,
        workdir,
        timeout_secs,
        pace: None,
        prompt_snapshot: parse_prompt_snapshot_from_snapshot(&tranche.config_snapshot),
        run_spec: None,
        tranche_plan: {
            let parsed = parse_tranche_plan_from_snapshot(&tranche.config_snapshot);
            if let Err(e) = validate_tranche_keys(&parsed) {
                warn!(error = %e, "continuation snapshot contains invalid tranche keys; filtering");
                parsed
                    .into_iter()
                    .filter(|t| !t.key.trim().is_empty())
                    .collect()
            } else {
                parsed
            }
        },
        current_tranche_index,
    })
}

pub(crate) fn should_auto_advance_planned_tranche(
    payload: &yarli_core::entities::ContinuationPayload,
    auto_advance: config::AutoAdvanceConfig,
    advances_taken: usize,
) -> (bool, String) {
    if auto_advance.max_reached(advances_taken) {
        return (
            false,
            format!(
                "auto-advance tranche cap reached (max_auto_advance_tranches={})",
                auto_advance.max_tranches
            ),
        );
    }

    let Some(tranche) = payload.next_tranche.as_ref() else {
        return (false, "no next tranche available".to_string());
    };
    if tranche.kind != yarli_core::entities::continuation::TrancheKind::PlannedNext {
        return (
            false,
            "next tranche is retry/unfinished, not planned-next".to_string(),
        );
    }

    if auto_advance.policy == AutoAdvancePolicy::Always {
        return (true, "auto_advance_policy=always".to_string());
    }

    let Some(gate) = payload.quality_gate.as_ref() else {
        return (
            false,
            "quality gate result missing in continuation payload".to_string(),
        );
    };
    if gate.allow_auto_advance {
        (true, gate.reason.clone())
    } else {
        (false, gate.reason.clone())
    }
}

pub(crate) fn is_verification_only_dispatch(tranche_plan: &[PlannedTranche]) -> bool {
    tranche_plan.len() == 1
        && tranche_plan
            .first()
            .map(|tranche| tranche.key.eq_ignore_ascii_case("verification"))
            .unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromptSource {
    Cli,
    Config,
    Default,
}

impl PromptSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Config => "config",
            Self::Default => "default",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedPromptPath {
    pub(crate) entry_path: PathBuf,
    pub(crate) source: PromptSource,
}

pub(crate) fn find_repo_root(start_dir: &Path) -> Option<PathBuf> {
    let mut current = start_dir.to_path_buf();
    loop {
        if current.join(".git").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

pub(crate) fn config_dir_from_cwd(loaded_config: &LoadedConfig, cwd: &Path) -> PathBuf {
    let config_path = loaded_config.path();
    let absolute = if config_path.is_absolute() {
        config_path.to_path_buf()
    } else {
        cwd.join(config_path)
    };
    absolute
        .parent()
        .map(|path| path.to_path_buf())
        .unwrap_or_else(|| cwd.to_path_buf())
}

pub(crate) fn resolve_prompt_entry_path_with_cwd(
    loaded_config: &LoadedConfig,
    prompt_file_override: Option<&Path>,
    cwd: &Path,
) -> Result<ResolvedPromptPath> {
    let cli_prompt = prompt_file_override
        .map(|path| path.to_path_buf())
        .filter(|path| !path.as_os_str().is_empty());
    if prompt_file_override.is_some() && cli_prompt.is_none() {
        bail!("--prompt-file must not be empty");
    }

    if let Some(path) = cli_prompt {
        return resolve_explicit_prompt_path(loaded_config, cwd, path, PromptSource::Cli);
    }

    if let Some(configured) = loaded_config.config().run.prompt_file.as_ref() {
        let trimmed = configured.trim();
        if trimmed.is_empty() {
            bail!(
                "invalid config: run.prompt_file in {} must not be empty",
                loaded_config.path().display()
            );
        }
        return resolve_explicit_prompt_path(
            loaded_config,
            cwd,
            PathBuf::from(trimmed),
            PromptSource::Config,
        );
    }

    let discovered = prompt::find_prompt_upwards(cwd.to_path_buf()).with_context(|| {
        format!(
            "failed to resolve default PROMPT.md from {} (set [run].prompt_file in {} or pass --prompt-file)",
            cwd.display(),
            loaded_config.path().display()
        )
    })?;
    Ok(ResolvedPromptPath {
        entry_path: discovered,
        source: PromptSource::Default,
    })
}

pub(crate) fn resolve_explicit_prompt_path(
    loaded_config: &LoadedConfig,
    cwd: &Path,
    candidate: PathBuf,
    source: PromptSource,
) -> Result<ResolvedPromptPath> {
    let resolved = if candidate.is_absolute() {
        candidate.clone()
    } else {
        let base_dir =
            find_repo_root(cwd).unwrap_or_else(|| config_dir_from_cwd(loaded_config, cwd));
        base_dir.join(candidate.as_path())
    };

    if !resolved.exists() {
        match source {
            PromptSource::Cli => bail!(
                "prompt file not found: {} (from --prompt-file). Remove --prompt-file to use config/default resolution",
                resolved.display()
            ),
            PromptSource::Config => bail!(
                "configured prompt file not found: {} (from [run].prompt_file in {}). Fix run.prompt_file or unset it to fall back to PROMPT.md",
                resolved.display(),
                loaded_config.path().display()
            ),
            PromptSource::Default => {
                bail!("default prompt file not found: {}", resolved.display());
            }
        }
    }

    if !resolved.is_file() {
        bail!(
            "prompt path is not a file: {}",
            resolved.as_path().display()
        );
    }

    fs::File::open(&resolved)
        .with_context(|| format!("prompt file is not readable: {}", resolved.display()))?;

    Ok(ResolvedPromptPath {
        entry_path: resolved,
        source,
    })
}

pub(crate) fn resolve_prompt_entry_path(
    loaded_config: &LoadedConfig,
    prompt_file_override: Option<&Path>,
) -> Result<ResolvedPromptPath> {
    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    resolve_prompt_entry_path_with_cwd(loaded_config, prompt_file_override, &cwd)
}

pub(crate) fn verify_only_override_enabled() -> bool {
    std::env::var("VERIFY_ONLY")
        .map(|raw| is_truthy_env(raw.trim()))
        .unwrap_or(false)
}

pub(crate) fn is_truthy_env(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

pub(crate) fn recursive_run_execution_enabled(
    loaded_config: &LoadedConfig,
    allow_recursive_run_override: bool,
) -> bool {
    let env_override = std::env::var("YARLI_ALLOW_RECURSIVE_RUN")
        .map(|raw| is_truthy_env(raw.trim()))
        .unwrap_or(false);
    loaded_config.config().run.allow_recursive_run && (allow_recursive_run_override || env_override)
}

pub(crate) fn parse_plan_checkbox_status(line: &str) -> Option<(bool, &str)> {
    let trimmed = line.trim_start();
    if let Some(rest) = trimmed.strip_prefix("- [x]") {
        return Some((true, rest.trim_start()));
    }
    if let Some(rest) = trimmed.strip_prefix("- [X]") {
        return Some((true, rest.trim_start()));
    }
    if let Some(rest) = trimmed.strip_prefix("- [ ]") {
        return Some((false, rest.trim_start()));
    }
    None
}

pub(crate) fn strip_markdown_list_marker(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if let Some(rest) = trimmed.strip_prefix("- ") {
        return Some(rest.trim_start());
    }
    if let Some(rest) = trimmed.strip_prefix("* ") {
        return Some(rest.trim_start());
    }

    let bytes = trimmed.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx > 0 && idx < bytes.len() && (bytes[idx] == b'.' || bytes[idx] == b')') {
        idx += 1;
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        return Some(trimmed[idx..].trim_start());
    }

    None
}

pub(crate) fn parse_plan_status_keywords(text: &str) -> Option<bool> {
    let lower = text.to_ascii_lowercase();
    if lower.contains("incomplete")
        || lower.contains("not complete")
        || lower.contains("todo")
        || lower.contains("to do")
        || lower.contains("pending")
        || lower.contains("open")
    {
        return Some(false);
    }
    if lower.contains("complete") || lower.contains("completed") || lower.contains("done") {
        return Some(true);
    }
    None
}

pub(crate) fn parse_tranche_group_token(token: &str) -> Option<String> {
    let trimmed = token.trim_matches(|ch: char| {
        matches!(
            ch,
            '[' | ']' | '(' | ')' | '{' | '}' | ',' | ';' | '"' | '\''
        )
    });
    let (key, value) = trimmed.split_once('=')?;
    if !key.eq_ignore_ascii_case("tranche_group") {
        return None;
    }
    let normalized = sanitize_task_key_component(value.trim());
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub(crate) fn normalize_allowed_path(raw: &str) -> Option<String> {
    let normalized = raw.trim_matches(|ch: char| {
        matches!(
            ch,
            '[' | ']' | '(' | ')' | '{' | '}' | ',' | ';' | '"' | '\''
        )
    });
    if normalized.is_empty() {
        return None;
    }
    let path = Path::new(normalized);
    if path.is_absolute() {
        return None;
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return None;
    }
    Some(normalized.to_string())
}

pub(crate) fn parse_allowed_paths_token(token: &str) -> Option<Vec<String>> {
    let trimmed = token.trim_matches(|ch: char| {
        matches!(
            ch,
            '[' | ']' | '(' | ')' | '{' | '}' | ',' | ';' | '"' | '\''
        )
    });
    let (key, value) = trimmed.split_once('=')?;
    if !key.eq_ignore_ascii_case("allowed_paths") {
        return None;
    }
    Some(
        value
            .split(',')
            .filter_map(normalize_allowed_path)
            .collect::<Vec<_>>(),
    )
}

pub(crate) fn parse_plan_tranche_group(text: &str) -> Option<String> {
    text.split_whitespace().find_map(parse_tranche_group_token)
}

pub(crate) fn parse_plan_allowed_paths(text: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    text.split_whitespace()
        .filter_map(parse_allowed_paths_token)
        .flat_map(|items| items.into_iter())
        .filter(|path| seen.insert(path.to_ascii_lowercase()))
        .collect()
}

pub(crate) fn strip_plan_metadata_tokens(text: &str) -> String {
    let filtered = text
        .split_whitespace()
        .filter(|token| {
            parse_tranche_group_token(token).is_none() && parse_allowed_paths_token(token).is_none()
        })
        .collect::<Vec<_>>()
        .join(" ");
    if filtered.trim().is_empty() {
        text.trim().to_string()
    } else {
        filtered.trim().to_string()
    }
}

pub(crate) fn extract_plan_target_key(text: &str) -> Option<String> {
    for raw in text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')) {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        if token_has_alpha_and_digit(token) {
            return Some(token.to_string());
        }
    }
    None
}

pub(crate) fn token_has_alpha_and_digit(token: &str) -> bool {
    let has_alpha = token.chars().any(|ch| ch.is_ascii_alphabetic());
    let has_digit = token.chars().any(|ch| ch.is_ascii_digit());
    has_alpha && has_digit
}

pub(crate) fn parse_plan_entry_line(line: &str) -> Option<ImplementationPlanEntry> {
    if let Some((is_complete, entry)) = parse_plan_checkbox_status(line) {
        let normalized_summary = strip_plan_metadata_tokens(entry);
        let key = extract_plan_target_key(&normalized_summary)?;
        return Some(ImplementationPlanEntry {
            key,
            summary: normalized_summary,
            is_complete,
            tranche_group: parse_plan_tranche_group(entry),
            allowed_paths: parse_plan_allowed_paths(entry),
        });
    }

    let candidate = if let Some(item) = strip_markdown_list_marker(line) {
        item
    } else {
        let trimmed = line.trim_start();
        let first = trimmed
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_');
        if !token_has_alpha_and_digit(first) {
            return None;
        }
        trimmed
    };
    let is_complete = parse_plan_status_keywords(candidate)?;
    let normalized_summary = strip_plan_metadata_tokens(candidate);
    let key = extract_plan_target_key(&normalized_summary)?;
    Some(ImplementationPlanEntry {
        key,
        summary: normalized_summary,
        is_complete,
        tranche_group: parse_plan_tranche_group(candidate),
        allowed_paths: parse_plan_allowed_paths(candidate),
    })
}

pub(crate) fn discover_plan_entries(plan_text: &str) -> Vec<ImplementationPlanEntry> {
    let mut parsed = Vec::new();
    for line in plan_text.lines() {
        if let Some(entry) = parse_plan_entry_line(line) {
            parsed.push(entry);
        }
    }

    // Keep only the latest explicit state per key, preserving forward order.
    let mut seen = HashSet::new();
    let mut dedup_reversed = Vec::new();
    for entry in parsed.into_iter().rev() {
        if seen.insert(entry.key.to_ascii_lowercase()) {
            dedup_reversed.push(entry);
        }
    }
    dedup_reversed.reverse();
    dedup_reversed
}

pub(crate) fn parse_plan_tranche_header_line(line: &str) -> Option<ImplementationPlanEntry> {
    // Tranche headers are top-level lines inside `## Next Work Tranches`.
    if line
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_whitespace())
    {
        return None;
    }
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }

    let bytes = trimmed.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == 0 || idx >= bytes.len() || bytes[idx] != b'.' {
        return None;
    }
    idx += 1;
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    if idx >= bytes.len() {
        return None;
    }

    let candidate = &trimmed[idx..];
    let split_at = candidate.rfind(':')?;
    let summary_text = candidate[..split_at].trim();
    let status_segment = candidate[split_at + 1..].trim();
    let status_word = status_segment.split_whitespace().next().map(|token| {
        token
            .trim_matches(|ch: char| {
                matches!(
                    ch,
                    '[' | ']' | '(' | ')' | '{' | '}' | ',' | ';' | '.' | ':' | '"' | '\''
                )
            })
            .to_ascii_lowercase()
    })?;
    let is_complete = match status_word.as_str() {
        "complete" => true,
        "incomplete" | "blocked" => false,
        _ => return None,
    };

    let normalized_summary = strip_plan_metadata_tokens(summary_text);
    let key_token = normalized_summary
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_');
    if key_token.is_empty() || !token_has_alpha_and_digit(key_token) || looks_like_uuid(key_token) {
        return None;
    }

    Some(ImplementationPlanEntry {
        key: key_token.to_string(),
        summary: normalized_summary,
        is_complete,
        tranche_group: parse_plan_tranche_group(candidate),
        allowed_paths: parse_plan_allowed_paths(candidate),
    })
}

/*
// ---------------------------------------------------------------------------
// Structured tranches file I/O
// ---------------------------------------------------------------------------

pub(crate) const TRANCHES_FILE: &str = ".yarli/tranches.toml";

pub(crate) fn tranches_file_path(base_dir: &Path) -> PathBuf {
    base_dir.join(TRANCHES_FILE)
}

pub(crate) fn read_tranches_file_in(base_dir: &Path) -> Result<Option<TranchesFile>> {
    let path = tranches_file_path(base_dir);
    if !path.exists() {
        return Ok(None);
    }
    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let tf: TranchesFile =
        toml::from_str(&content).with_context(|| format!("failed to parse {}", path.display()))?;
    if tf.version != 1 {
        bail!(
            "{}: unsupported version {} (expected 1)",
            path.display(),
            tf.version
        );
    }
    Ok(Some(tf))
}

pub(crate) fn read_tranches_file() -> Result<Option<TranchesFile>> {
    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    read_tranches_file_in(&cwd)
}

pub(crate) fn write_tranches_file_in(base_dir: &Path, tf: &TranchesFile) -> Result<()> {
    let path = tranches_file_path(base_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let content = toml::to_string_pretty(tf).context("failed to serialize tranches file")?;
    fs::write(&path, content).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub(crate) fn write_tranches_file(tf: &TranchesFile) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    write_tranches_file_in(&cwd, tf)
}

pub(crate) fn maybe_mark_current_structured_tranche_complete(plan: &RunPlan) -> Result<bool> {
    let Some(current_index) = plan.current_tranche_index else {
        return Ok(false);
    };
    let Some(current_tranche) = plan.tranche_plan.get(current_index) else {
        return Ok(false);
    };
    if current_tranche.key.eq_ignore_ascii_case("verification") {
        return Ok(false);
    }

    let Some(prompt_snapshot) = plan.prompt_snapshot.as_ref() else {
        return Ok(false);
    };
    let prompt_entry_path = PathBuf::from(&prompt_snapshot.entry_path);
    let plan_path = plan_path_for_prompt_entry(&prompt_entry_path)?;
    let tranches_base_dir = plan_path
        .parent()
        .context("plan file has no parent directory")?;
    let tranches_path = tranches_file_path(tranches_base_dir);

    let Some(mut tf) = read_tranches_file_in(tranches_base_dir)? else {
        return Ok(false);
    };
    let Some(def) = tf
        .tranches
        .iter_mut()
        .find(|def| def.key.eq_ignore_ascii_case(&current_tranche.key))
    else {
        debug!(
            tranche_key = %current_tranche.key,
            tranches_file = %tranches_path.display(),
            "current tranche key not found in structured tranches file"
        );
        return Ok(false);
    };
    if def.status == TrancheStatus::Complete {
        return Ok(false);
    }

    def.status = TrancheStatus::Complete;
    let completed_key = def.key.clone();
    write_tranches_file_in(tranches_base_dir, &tf)?;
    info!(
        tranche_key = %completed_key,
        tranches_file = %tranches_path.display(),
        "auto-marked structured tranche as complete"
    );
    Ok(true)
}

pub(crate) fn validate_tranche_definition(def: &TrancheDefinition) -> Result<()> {
    let key = def.key.trim();
    if key.is_empty() {
        bail!("tranche key must not be empty");
    }
    if looks_like_uuid(key) {
        bail!(
            "tranche key '{}' looks like a stale UUID — use a descriptive key instead",
            key
        );
    }
    let lower = key.to_ascii_lowercase();
    for prefix in &["run-", "app-", "target-"] {
        if lower.starts_with(prefix) {
            bail!(
                "tranche key '{}' uses reserved prefix '{}' — use a descriptive key instead",
                key,
                prefix.trim_end_matches('-')
            );
        }
    }
    if !token_has_alpha_and_digit(key) {
        bail!("tranche key '{}' must contain both letters and digits", key);
    }
    if def.summary.trim().is_empty() {
        bail!("tranche summary must not be empty");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `yarli plan` command handlers
// ---------------------------------------------------------------------------

pub(crate) fn cmd_plan_tranche_add(
    key: &str,
    summary: &str,
    group: Option<&str>,
    allowed_paths: &[String],
) -> Result<()> {
    let def = TrancheDefinition {
        key: key.to_string(),
        summary: summary.to_string(),
        status: TrancheStatus::Incomplete,
        group: group.map(|g| g.to_string()),
        allowed_paths: allowed_paths.to_vec(),
    };
    validate_tranche_definition(&def)?;

    let mut tf = read_tranches_file()?.unwrap_or(TranchesFile {
        version: 1,
        tranches: Vec::new(),
    });

    if tf.tranches.iter().any(|t| t.key == key) {
        bail!("tranche with key '{}' already exists", key);
    }

    tf.tranches.push(def);
    write_tranches_file(&tf)?;
    println!("Added tranche '{key}'");
    Ok(())
}

pub(crate) fn cmd_plan_tranche_complete(key: &str) -> Result<()> {
    let mut tf = read_tranches_file()?
        .ok_or_else(|| anyhow::anyhow!("no tranches file found at {TRANCHES_FILE}"))?;

    let entry = tf
        .tranches
        .iter_mut()
        .find(|t| t.key == key)
        .ok_or_else(|| anyhow::anyhow!("tranche with key '{}' not found", key))?;

    entry.status = TrancheStatus::Complete;
    write_tranches_file(&tf)?;
    println!("Marked tranche '{key}' as complete");
    Ok(())
}

pub(crate) fn cmd_plan_tranche_list() -> Result<()> {
    let tf = read_tranches_file()?
        .ok_or_else(|| anyhow::anyhow!("no tranches file found at {TRANCHES_FILE}"))?;

    if tf.tranches.is_empty() {
        println!("No tranches defined.");
        return Ok(());
    }

    for def in &tf.tranches {
        let marker = match def.status {
            TrancheStatus::Complete => "[x]",
            TrancheStatus::Incomplete => "[ ]",
            TrancheStatus::Blocked => "[~]",
        };
        let group_suffix = def
            .group
            .as_ref()
            .map(|g| format!("  (group: {g})"))
            .unwrap_or_default();
        println!("{marker} {}: {}{group_suffix}", def.key, def.summary);
    }
    Ok(())
}

pub(crate) fn cmd_plan_tranche_remove(key: &str) -> Result<()> {
    let mut tf = read_tranches_file()?
        .ok_or_else(|| anyhow::anyhow!("no tranches file found at {TRANCHES_FILE}"))?;

    let before = tf.tranches.len();
    tf.tranches.retain(|t| t.key != key);
    if tf.tranches.len() == before {
        bail!("tranche with key '{}' not found", key);
    }

    write_tranches_file(&tf)?;
    println!("Removed tranche '{key}'");
    Ok(())
}

pub(crate) fn cmd_plan_validate() -> Result<()> {
    let tf = read_tranches_file()?
        .ok_or_else(|| anyhow::anyhow!("no tranches file found at {TRANCHES_FILE}"))?;

    let mut errors = Vec::new();
    let mut seen_keys = HashSet::new();

    for (i, def) in tf.tranches.iter().enumerate() {
        if let Err(e) = validate_tranche_definition(def) {
            errors.push(format!("tranches[{}] (key='{}'): {}", i, def.key, e));
        }
        if !seen_keys.insert(&def.key) {
            errors.push(format!("tranches[{}]: duplicate key '{}'", i, def.key));
        }
    }

    if errors.is_empty() {
        println!(
            "Tranches file is valid ({} tranches defined).",
            tf.tranches.len()
        );
        Ok(())
    } else {
        for err in &errors {
            eprintln!("  error: {err}");
        }
        bail!("{TRANCHES_FILE} has {} validation error(s)", errors.len());
    }
}

pub(crate) fn discover_plan_dispatch_entries(plan_text: &str) -> Vec<ImplementationPlanEntry> {
    let mut in_next_work_tranches = false;
    let mut saw_next_work_tranches_header = false;
    let mut entries = Vec::new();

    for line in plan_text.lines() {
        let trimmed = line.trim();
        if !in_next_work_tranches {
            let header_text = trimmed.trim_start_matches('#').trim();
            if header_text.eq_ignore_ascii_case("Next Work Tranches")
                || header_text.eq_ignore_ascii_case("Next Priority Actions")
            {
                in_next_work_tranches = true;
                saw_next_work_tranches_header = true;
            }
            continue;
        }

        if trimmed.starts_with("## ") {
            break;
        }
        if let Some(entry) = parse_plan_tranche_header_line(line) {
            entries.push(entry);
        }
    }

    if saw_next_work_tranches_header {
        entries
    } else {
        discover_plan_entries(plan_text)
    }
}

pub(crate) fn target_matches_entry_key(entry_key: &str, target: &str) -> bool {
    if entry_key.eq_ignore_ascii_case(target) {
        return true;
    }
    let Some(remainder) = entry_key.strip_prefix(target) else {
        return false;
    };
    match remainder.chars().next() {
        None => true,
        Some(next) => !(next.is_ascii_alphanumeric() || next == '_' || next == '-'),
    }
}

pub(crate) fn plan_target_completion_state(plan_text: &str, target: &str) -> Result<Option<bool>> {
    Ok(discover_plan_entries(plan_text)
        .into_iter()
        .find(|entry| target_matches_entry_key(&entry.key, target))
        .map(|entry| entry.is_complete))
}

pub(crate) fn plan_path_for_prompt_entry(entry_prompt_path: &Path) -> Result<PathBuf> {
    let mut current = entry_prompt_path
        .parent()
        .context("prompt file has no parent directory")?
        .to_path_buf();
    loop {
        let candidate = current.join("IMPLEMENTATION_PLAN.md");
        if candidate.exists() {
            return Ok(candidate);
        }
        if !current.pop() {
            bail!(
                "failed to resolve IMPLEMENTATION_PLAN.md for prompt {}",
                entry_prompt_path.display()
            );
        }
    }
}

pub(crate) fn run_spec_mentions_verification(run_spec: &prompt::RunSpec) -> bool {
    if run_spec
        .objective
        .as_ref()
        .map(|objective| objective.to_ascii_lowercase().contains("verif"))
        .unwrap_or(false)
    {
        return true;
    }

    run_spec
        .tranches
        .as_ref()
        .map(|tranches| {
            tranches.items.iter().any(|tranche| {
                tranche
                    .objective
                    .as_ref()
                    .map(|objective| objective.to_ascii_lowercase().contains("verif"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

pub(crate) fn run_spec_plan_guard_preflight_with_override(
    loaded: &prompt::LoadedPrompt,
    verify_only_override: bool,
) -> Result<Option<PlanGuardContext>> {
    let Some(plan_guard) = loaded.run_spec.plan_guard.as_ref() else {
        return Ok(None);
    };

    let plan_path = plan_path_for_prompt_entry(&loaded.entry_path)?;
    let plan_text = fs::read_to_string(&plan_path).with_context(|| {
        format!(
            "failed to read plan guard file at {}",
            plan_path.as_path().display()
        )
    })?;
    let is_complete =
        plan_target_completion_state(&plan_text, &plan_guard.target)?.ok_or_else(|| {
            anyhow::anyhow!(
                "plan guard target {} not found in IMPLEMENTATION_PLAN.md",
                plan_guard.target
            )
        })?;

    match plan_guard.mode {
        prompt::RunSpecPlanGuardMode::Implement => {
            if is_complete && !verify_only_override {
                info!(
                    target = %plan_guard.target,
                    "plan guard target already complete; proceeding with verification-only routing"
                );
            }
        }
        prompt::RunSpecPlanGuardMode::VerifyOnly => {
            if !is_complete {
                bail!(
                    "plan guard blocked run: target {} is not complete; verify-only mode is only allowed after completion",
                    plan_guard.target
                );
            }
            if !run_spec_mentions_verification(&loaded.run_spec) {
                bail!(
                    "plan guard blocked run: verify-only mode requires objective text to clearly indicate verification"
                );
            }
        }
    }

    if verify_only_override && plan_guard.mode == prompt::RunSpecPlanGuardMode::Implement {
        info!(
            target = %plan_guard.target,
            "VERIFY_ONLY override enabled for completed implement target"
        );
    }

    Ok(Some(PlanGuardContext {
        target: plan_guard.target.clone(),
        mode: plan_guard.mode,
        was_complete: is_complete,
        plan_path,
    }))
}

pub(crate) fn run_spec_plan_guard_preflight(
    loaded: &prompt::LoadedPrompt,
) -> Result<Option<PlanGuardContext>> {
    run_spec_plan_guard_preflight_with_override(loaded, verify_only_override_enabled())
}

pub(crate) fn enforce_plan_guard_post_run(
    _loaded: &prompt::LoadedPrompt,
    context: &PlanGuardContext,
) -> Result<()> {
    if context.mode != prompt::RunSpecPlanGuardMode::Implement {
        return Ok(());
    }

    let plan_text = fs::read_to_string(&context.plan_path).with_context(|| {
        format!(
            "failed to read plan guard file at {}",
            context.plan_path.as_path().display()
        )
    })?;
    let is_complete_now =
        plan_target_completion_state(&plan_text, &context.target)?.ok_or_else(|| {
            anyhow::anyhow!(
                "plan guard target {} disappeared from IMPLEMENTATION_PLAN.md",
                context.target
            )
        })?;

    if is_complete_now && !context.was_complete {
        info!(
            target = %context.target,
            "plan guard target transitioned to complete during run"
        );
    }

    Ok(())
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{build_run_config_snapshot, compute_quality_gate};
    use crate::test_helpers::{write_test_config, write_test_config_at};
    use chrono::Utc;
    use tempfile::TempDir;
    use uuid::Uuid;
    use yarli_cli::prompt;
    use yarli_core::domain::{CommandClass, SafeMode};
    use yarli_core::entities::run::Run;
    use yarli_core::entities::task::Task;
    use yarli_core::explain::{DeteriorationReport, DeteriorationTrend};
    use yarli_core::fsm::run::RunState;
    use yarli_core::fsm::task::TaskState;

    #[test]
    fn run_config_has_run_spec_data_detects_configured_sections() {
        let loaded_empty = write_test_config(
            r#"
[run]
continue_wait_timeout_seconds = 0
"#,
        );
        assert!(!run_config_has_run_spec_data(&loaded_empty.config().run));

        let loaded_objective = write_test_config(
            r#"
[run]
objective = "verify config"
"#,
        );
        assert!(run_config_has_run_spec_data(&loaded_objective.config().run));

        let loaded_tasks = write_test_config(
            r#"
[[run.tasks]]
key = "lint"
cmd = "cargo clippy --workspace -- -D warnings"
"#,
        );
        assert!(run_config_has_run_spec_data(&loaded_tasks.config().run));
    }

    #[test]
    fn run_spec_from_run_config_maps_tasks_tranches_and_plan_guard() {
        let loaded = write_test_config(
            r#"
[run]
objective = "verify all"

[[run.tasks]]
key = "lint"
cmd = "cargo clippy --workspace -- -D warnings"
class = "cpu"

[[run.tasks]]
key = "test"
cmd = "cargo test --workspace"

[[run.tranches]]
key = "verify"
objective = "verification tranche"
task_keys = ["lint", "test"]

[run.plan_guard]
target = "I8B"
mode = "verify-only"
"#,
        );

        let run_spec = run_spec_from_run_config(&loaded.config().run);
        assert_eq!(run_spec.objective.as_deref(), Some("verify all"));
        assert_eq!(run_spec.tasks.items.len(), 2);
        assert_eq!(run_spec.tasks.items[0].key, "lint");
        assert_eq!(run_spec.tasks.items[0].class.as_deref(), Some("cpu"));
        assert_eq!(
            run_spec
                .tranches
                .as_ref()
                .map(|tranches| tranches.items.len()),
            Some(1)
        );
        let guard = run_spec.plan_guard.as_ref().expect("plan guard expected");
        assert_eq!(guard.target, "I8B");
        assert_eq!(guard.mode, prompt::RunSpecPlanGuardMode::VerifyOnly);
    }

    #[test]
    fn merge_run_specs_applies_prompt_overrides_on_top_of_config_defaults() {
        let base = prompt::RunSpec {
            version: 1,
            objective: Some("config objective".to_string()),
            tasks: prompt::RunSpecTasks {
                items: vec![
                    prompt::RunSpecTask {
                        key: "lint".to_string(),
                        cmd: "cargo clippy --workspace -- -D warnings".to_string(),
                        class: Some("cpu".to_string()),
                    },
                    prompt::RunSpecTask {
                        key: "test".to_string(),
                        cmd: "cargo test --workspace".to_string(),
                        class: Some("io".to_string()),
                    },
                ],
            },
            tranches: Some(prompt::RunSpecTranches {
                items: vec![prompt::RunSpecTranche {
                    key: "verify".to_string(),
                    objective: Some("config tranche".to_string()),
                    task_keys: vec!["lint".to_string(), "test".to_string()],
                }],
            }),
            plan_guard: Some(prompt::RunSpecPlanGuard {
                target: "I8A".to_string(),
                mode: prompt::RunSpecPlanGuardMode::Implement,
            }),
        };
        let prompt_override = prompt::RunSpec {
            version: 1,
            objective: Some("prompt objective".to_string()),
            tasks: prompt::RunSpecTasks {
                items: vec![
                    prompt::RunSpecTask {
                        key: "lint".to_string(),
                        cmd: "cargo clippy --workspace --all-targets -- -D warnings".to_string(),
                        class: Some("cpu".to_string()),
                    },
                    prompt::RunSpecTask {
                        key: "docs".to_string(),
                        cmd: "make docs-build".to_string(),
                        class: Some("io".to_string()),
                    },
                ],
            },
            tranches: Some(prompt::RunSpecTranches {
                items: vec![prompt::RunSpecTranche {
                    key: "prompt-verify".to_string(),
                    objective: Some("prompt tranche".to_string()),
                    task_keys: vec!["lint".to_string(), "docs".to_string()],
                }],
            }),
            plan_guard: Some(prompt::RunSpecPlanGuard {
                target: "I8B".to_string(),
                mode: prompt::RunSpecPlanGuardMode::VerifyOnly,
            }),
        };

        let merged = merge_run_specs(&base, Some(&prompt_override));
        assert_eq!(merged.objective.as_deref(), Some("prompt objective"));
        assert_eq!(merged.tasks.items.len(), 3);
        assert_eq!(merged.tasks.items[0].key, "lint");
        assert!(merged.tasks.items[0]
            .cmd
            .contains("--all-targets -- -D warnings"));
        assert_eq!(merged.tasks.items[1].key, "test");
        assert_eq!(merged.tasks.items[2].key, "docs");
        assert_eq!(
            merged
                .tranches
                .as_ref()
                .map(|tranches| tranches.items[0].key.as_str()),
            Some("prompt-verify")
        );
        assert_eq!(
            merged
                .plan_guard
                .as_ref()
                .map(|guard| guard.target.as_str()),
            Some("I8B")
        );
    }

    #[test]
    fn resolve_prompt_uses_config_prompt_file_when_set() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        std::fs::create_dir_all(temp.path().join("prompts")).unwrap();
        std::fs::write(temp.path().join("prompts/I8B.md"), "# prompt").unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[run]
prompt_file = "prompts/I8B.md"
"#,
        );

        let resolved = resolve_prompt_entry_path_with_cwd(&loaded, None, temp.path()).unwrap();
        assert_eq!(resolved.source, PromptSource::Config);
        assert_eq!(resolved.entry_path, temp.path().join("prompts/I8B.md"));
    }

    #[test]
    fn resolve_prompt_cli_override_wins_over_config() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        std::fs::create_dir_all(temp.path().join("prompts")).unwrap();
        std::fs::write(temp.path().join("prompts/I8B.md"), "# prompt").unwrap();
        std::fs::write(temp.path().join("prompts/I8C.md"), "# prompt").unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[run]
prompt_file = "prompts/I8B.md"
"#,
        );

        let resolved = resolve_prompt_entry_path_with_cwd(
            &loaded,
            Some(Path::new("prompts/I8C.md")),
            temp.path(),
        )
        .unwrap();
        assert_eq!(resolved.source, PromptSource::Cli);
        assert_eq!(resolved.entry_path, temp.path().join("prompts/I8C.md"));
    }

    #[test]
    fn resolve_prompt_defaults_to_prompt_md_lookup() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        std::fs::create_dir_all(temp.path().join("nested/work")).unwrap();
        std::fs::write(temp.path().join("PROMPT.md"), "# prompt").unwrap();
        let loaded = LoadedConfig::load(temp.path().join("yarli.toml")).unwrap();

        let resolved =
            resolve_prompt_entry_path_with_cwd(&loaded, None, &temp.path().join("nested/work"))
                .unwrap();
        assert_eq!(resolved.source, PromptSource::Default);
        assert_eq!(resolved.entry_path, temp.path().join("PROMPT.md"));
    }

    #[test]
    fn resolve_prompt_relative_paths_use_repo_root_before_config_dir() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        std::fs::create_dir_all(temp.path().join("prompts")).unwrap();
        std::fs::create_dir_all(temp.path().join("config")).unwrap();
        std::fs::write(temp.path().join("prompts/I8B.md"), "# prompt").unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("config/yarli.toml"),
            r#"
[run]
prompt_file = "prompts/I8B.md"
"#,
        );

        let resolved =
            resolve_prompt_entry_path_with_cwd(&loaded, None, &temp.path().join("config")).unwrap();
        assert_eq!(resolved.source, PromptSource::Config);
        assert_eq!(resolved.entry_path, temp.path().join("prompts/I8B.md"));
    }

    #[test]
    fn resolve_prompt_relative_paths_fallback_to_config_dir_without_repo_root() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join("config/prompts")).unwrap();
        std::fs::write(temp.path().join("config/prompts/I8B.md"), "# prompt").unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("config/yarli.toml"),
            r#"
[run]
prompt_file = "prompts/I8B.md"
"#,
        );

        let resolved =
            resolve_prompt_entry_path_with_cwd(&loaded, None, &temp.path().join("somewhere"))
                .unwrap();
        assert_eq!(resolved.source, PromptSource::Config);
        assert_eq!(
            resolved.entry_path,
            temp.path().join("config/prompts/I8B.md")
        );
    }

    #[test]
    fn resolve_prompt_missing_configured_file_error_includes_resolved_path() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[run]
prompt_file = "prompts/missing.md"
"#,
        );

        let err = resolve_prompt_entry_path_with_cwd(&loaded, None, temp.path()).unwrap_err();
        assert!(err
            .to_string()
            .contains(&temp.path().join("prompts/missing.md").display().to_string()));
        assert!(err.to_string().contains("run.prompt_file"));
    }

    #[test]
    fn resolve_prompt_rejects_empty_config_prompt_file() {
        let temp = TempDir::new().unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[run]
prompt_file = "   "
"#,
        );

        let err = resolve_prompt_entry_path_with_cwd(&loaded, None, temp.path()).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn run_config_snapshot_records_resolved_prompt_entry_path() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join("prompts")).unwrap();
        let prompt_path = temp.path().join("prompts/I8B.md");
        std::fs::write(
            &prompt_path,
            r#"
```yarli-run
version = 1
objective = "verify"
[tasks]
items = [{ key = "fmt", cmd = "cargo fmt --all -- --check" }]
```
"#,
        )
        .unwrap();

        let loaded_prompt = prompt::load_prompt_and_run_spec(&prompt_path).unwrap();
        let loaded_config = LoadedConfig::load(temp.path().join("yarli.toml")).unwrap();
        let task_catalog = build_task_catalog_from_run_spec(&loaded_prompt.run_spec).unwrap();
        let tranche_plan =
            build_tranche_plan_from_run_spec(&loaded_prompt.run_spec, "verify").unwrap();
        let first_tasks = tasks_for_tranche(&task_catalog, tranche_plan.first().unwrap()).unwrap();
        let snapshot = build_run_config_snapshot(
            &loaded_config,
            ".",
            300,
            &first_tasks,
            &task_catalog,
            None,
            Some(&loaded_prompt.snapshot),
            Some(&loaded_prompt.run_spec),
            &tranche_plan,
            Some(0),
        )
        .unwrap();

        assert_eq!(
            snapshot["runtime"]["prompt"]["entry_path"].as_str(),
            Some(loaded_prompt.snapshot.entry_path.as_str())
        );
    }

    #[test]
    fn plan_driven_sequence_builds_open_tranches_plus_verification() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] I8A first tranche\n- [ ] I8B second tranche\n- [x] I8C done\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement active plan")
                .unwrap();

        assert_eq!(tasks.len(), 3);
        assert_eq!(tranches.len(), 3);
        assert_eq!(tranches[0].key, "I8A");
        assert_eq!(tranches[1].key, "I8B");
        assert_eq!(tranches[2].key, "verification");
        assert!(tasks[0].command.contains("codex"));
        assert!(tasks[0].command.contains("I8A"));
        assert!(tasks[2].command.contains("verification"));
    }

    #[test]
    fn plan_driven_sequence_uses_tranches_file_near_resolved_plan_not_cwd() {
        let repo = TempDir::new().unwrap();
        std::fs::write(
            repo.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] MD-99 markdown tranche\n",
        )
        .unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "ST-01"
summary = "Structured tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let decoy_cwd = TempDir::new().unwrap();
        std::fs::create_dir_all(decoy_cwd.path().join(".yarli")).unwrap();
        std::fs::write(
            decoy_cwd.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "WRONG-99"
summary = "Decoy tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &repo.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&repo.path().join("PROMPT.md")).unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(decoy_cwd.path()).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement active plan")
                .unwrap();
        std::env::set_current_dir(original_dir).unwrap();

        assert_eq!(tasks.len(), 2);
        assert_eq!(tranches.len(), 2);
        assert_eq!(tranches[0].key, "ST-01");
        assert_eq!(tranches[1].key, "verification");
    }

    #[test]
    fn plan_driven_sequence_runs_verification_only_when_no_open_tranches() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] I8A first tranche\n- [x] I8B second tranche\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement active plan")
                .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tranches.len(), 1);
        assert_eq!(tranches[0].key, "verification");
        assert!(tasks[0].command.contains("verification"));
    }

    #[test]
    fn plain_prompt_sequence_dispatches_expanded_prompt_text() {
        let temp = TempDir::new().unwrap();
        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt = prompt::LoadedPrompt {
            entry_path: temp.path().join("PROMPT.md"),
            expanded_text: "# plain prompt\nImplement step 2.3.\n".to_string(),
            snapshot: prompt::PromptSnapshot {
                entry_path: temp.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            },
            run_spec: prompt::RunSpec {
                version: 1,
                objective: None,
                tasks: prompt::RunSpecTasks::default(),
                tranches: None,
                plan_guard: None,
            },
        };

        let (tasks, tranches) =
            build_plain_prompt_run_sequence(&loaded_config, &loaded_prompt, "yarli run").unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tranches.len(), 1);
        assert_eq!(tranches[0].key, "prompt");
        assert_eq!(tasks[0].task_key, "prompt-001");
        assert!(tasks[0].command.contains("plain prompt"));
        assert!(tasks[0].command.contains("Implement step 2.3."));
    }

    #[test]
    fn plan_driven_sequence_ignores_stale_keys_in_non_header_evidence_lines() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            r#"
## Next Work Tranches
11. I9 `Runtime Contract`: complete. tranche_group=runtime-contract
    Verification evidence:
    1. Open-tranche dispatch evidence remains intact: `yarli run status 019c5308-e73b-7a23-8b7a-c4acc8b95e52` includes `I11` and `YARLI_DETERIORATION_REPORT_V1`.
12. I10 `Follow-up`: complete. tranche_group=runtime-contract

## Notes
1. YARLI_DETERIORATION_REPORT_V1 incomplete in historical notes.
"#,
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement active plan")
                .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tranches.len(), 1);
        assert_eq!(tranches[0].key, "verification");
        assert!(tasks[0].command.contains("verification"));
    }

    #[test]
    fn plan_driven_sequence_groups_adjacent_entries_by_tranche_group_when_enabled() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement grouped plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] I8A first tranche_group=core\n- [ ] I8B second tranche_group=core\n- [ ] I8C third tranche_group=ui\n- [ ] I8D fourth\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"

[run]
enable_plan_tranche_grouping = true
max_grouped_tasks_per_tranche = 0
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) = build_plan_driven_run_sequence(
            &loaded_config,
            &loaded_prompt,
            "implement grouped plan",
        )
        .unwrap();

        assert_eq!(tasks.len(), 5);
        assert_eq!(tranches.len(), 4);
        assert_eq!(tranches[0].key, "group-001-core");
        assert_eq!(tranches[0].tranche_group.as_deref(), Some("core"));
        assert_eq!(tranches[0].task_keys.len(), 2);
        assert_eq!(tranches[1].key, "group-003-ui");
        assert_eq!(tranches[1].tranche_group.as_deref(), Some("ui"));
        assert_eq!(tranches[1].task_keys.len(), 1);
        assert_eq!(tranches[2].key, "I8D");
        assert_eq!(tranches[2].task_keys.len(), 1);
        assert_eq!(tranches[3].key, "verification");
        assert!(tasks[0].command.contains("Tranche group: core."));
        assert!(tasks[2].command.contains("Tranche group: ui."));
    }

    #[test]
    fn plan_driven_sequence_grouping_respects_max_grouped_tasks_per_tranche_cap() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement grouped plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] I8A first tranche_group=core\n- [ ] I8B second tranche_group=core\n- [ ] I8C third tranche_group=core\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"

[run]
enable_plan_tranche_grouping = true
max_grouped_tasks_per_tranche = 2
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (_tasks, tranches) = build_plan_driven_run_sequence(
            &loaded_config,
            &loaded_prompt,
            "implement grouped plan",
        )
        .unwrap();

        assert_eq!(tranches.len(), 3);
        assert_eq!(tranches[0].key, "group-001-core");
        assert_eq!(tranches[0].task_keys.len(), 2);
        assert_eq!(tranches[1].key, "group-003-core");
        assert_eq!(tranches[1].task_keys.len(), 1);
        assert_eq!(tranches[2].key, "verification");
    }

    #[test]
    fn plan_driven_sequence_surfaces_allowed_paths_scope_when_enforced() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement scoped plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] I8A scoped tranche allowed_paths=src/main.rs,docs/CLI.md,../reject\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"

[run]
enforce_plan_tranche_allowed_paths = true
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement scoped plan")
                .unwrap();

        assert_eq!(tasks.len(), 2);
        assert_eq!(tranches.len(), 2);
        assert_eq!(
            tasks[0].allowed_paths,
            vec!["src/main.rs".to_string(), "docs/CLI.md".to_string()]
        );
        assert!(tasks[0]
            .command
            .contains("Allowed file scope: src/main.rs, docs/CLI.md."));
        assert!(tasks[0]
            .command
            .contains("Restrict edits to the allowed file scope above."));
        assert!(!tasks[0].command.contains("../reject"));
    }

    #[test]
    fn resolve_run_plan_rejects_cmd_and_pace() {
        let loaded = write_test_config("");

        let err = resolve_run_plan(
            &loaded,
            "obj".to_string(),
            vec!["echo hi".to_string()],
            Some("batch".to_string()),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn resolve_run_plan_uses_named_pace_for_commands_and_overrides() {
        let loaded = write_test_config(
            r#"
[execution]
working_dir = "/default"
command_timeout_seconds = 111

[run]
default_pace = "batch"

[run.paces.batch]
cmds = ["echo one", "echo two"]
working_dir = "/pace"
command_timeout_seconds = 222
"#,
        );

        let plan = resolve_run_plan(
            &loaded,
            "obj".to_string(),
            Vec::new(),
            Some("batch".to_string()),
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(plan.tasks.len(), 2);
        assert_eq!(plan.tasks[0].task_key, "task-1");
        assert_eq!(plan.tasks[0].command, "echo one");
        assert_eq!(plan.tasks[1].task_key, "task-2");
        assert_eq!(plan.tasks[1].command, "echo two");
        assert_eq!(plan.workdir, "/pace");
        assert_eq!(plan.timeout_secs, 222);
        assert_eq!(plan.pace.as_deref(), Some("batch"));
    }

    #[test]
    fn resolve_run_plan_start_without_cmd_uses_run_default_pace() {
        let loaded = write_test_config(
            r#"
[run]
default_pace = "batch"

[run.paces.batch]
cmds = ["echo ok"]
"#,
        );

        let plan = resolve_run_plan(
            &loaded,
            "obj".to_string(),
            Vec::new(),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].command, "echo ok");
        assert_eq!(plan.pace.as_deref(), Some("batch"));
    }

    #[test]
    fn resolve_run_plan_batch_defaults_to_batch_pace_name() {
        let loaded = write_test_config(
            r#"
[run.paces.batch]
cmds = ["echo ok"]
"#,
        );

        let plan = resolve_run_plan(
            &loaded,
            "batch".to_string(),
            Vec::new(),
            None,
            None,
            None,
            Some("batch"),
        )
        .unwrap();
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].command, "echo ok");
        assert_eq!(plan.pace.as_deref(), Some("batch"));
    }

    #[test]
    fn resolve_run_plan_batch_falls_back_to_default_pace_when_batch_not_defined() {
        let loaded = write_test_config(
            r#"
[run]
default_pace = "ci"

[run.paces.ci]
cmds = ["echo ok"]
"#,
        );

        let plan = resolve_run_plan(
            &loaded,
            "batch".to_string(),
            Vec::new(),
            None,
            None,
            None,
            Some("batch"),
        )
        .unwrap();
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].command, "echo ok");
        assert_eq!(plan.pace.as_deref(), Some("ci"));
    }

    #[test]
    fn continuation_no_tranche_when_all_complete() {
        use yarli_core::entities::continuation::ContinuationPayload;

        let run = Run::new("all done", SafeMode::Execute);
        let mut t1 = Task::new(
            run.id,
            "build",
            "cargo build",
            CommandClass::Io,
            run.correlation_id,
        );
        t1.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1]);
        assert!(payload.next_tranche.is_none());
    }

    #[test]
    fn continuation_tranche_includes_failed_and_unfinished() {
        use yarli_core::entities::continuation::ContinuationPayload;

        let run = Run::new("mixed", SafeMode::Execute);
        let mut t1 = Task::new(
            run.id,
            "lint",
            "cargo clippy",
            CommandClass::Io,
            run.correlation_id,
        );
        t1.state = TaskState::TaskFailed;
        let mut t2 = Task::new(
            run.id,
            "deploy",
            "deploy.sh",
            CommandClass::Io,
            run.correlation_id,
        );
        t2.state = TaskState::TaskOpen;
        let mut t3 = Task::new(
            run.id,
            "build",
            "cargo build",
            CommandClass::Io,
            run.correlation_id,
        );
        t3.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1, &t2, &t3]);
        let tranche = payload.next_tranche.as_ref().unwrap();
        assert_eq!(tranche.retry_task_keys, vec!["lint"]);
        assert_eq!(tranche.unfinished_task_keys, vec!["deploy"]);
    }

    #[test]
    fn continuation_planned_next_resolves_command_from_task_catalog() {
        use yarli_core::entities::continuation::{TrancheKind, TrancheSpec};

        let loaded = write_test_config("");
        let prompt_entry_path = "/tmp/project/PROMPT.md";
        let tranche = TrancheSpec {
            suggested_objective: "planned-next".into(),
            kind: TrancheKind::PlannedNext,
            retry_task_keys: vec![],
            unfinished_task_keys: vec![],
            planned_task_keys: vec!["two_task".into(), "three_task".into()],
            planned_tranche_key: Some("two".into()),
            cursor: Some(yarli_core::entities::continuation::TrancheCursor {
                current_tranche_index: Some(0),
                next_tranche_index: Some(1),
            }),
            config_snapshot: serde_json::json!({
                "runtime": {
                    "working_dir": ".",
                    "timeout_secs": 300,
                    "tasks": [
                        {"task_key": "one_task", "command": "true", "command_class": "Io"}
                    ],
                    "task_catalog": [
                        {"task_key": "one_task", "command": "true", "command_class": "Io"},
                        {"task_key": "two_task", "command": "echo second", "command_class": "Io"},
                        {"task_key": "three_task", "command": "echo third", "command_class": "Io"}
                    ],
                    "tranche_plan": [
                        {"key": "one", "objective": "first", "task_keys": ["one_task"]},
                        {"key": "two", "objective": "second", "task_keys": ["two_task", "three_task"], "tranche_group": "core"}
                    ],
                    "current_tranche_index": 0,
                    "prompt": {
                        "entry_path": prompt_entry_path,
                        "expanded_sha256": "abc123",
                        "included_files": []
                    }
                }
            }),
        };

        let plan = build_plan_from_continuation_tranche(&tranche, &loaded).unwrap();
        assert_eq!(plan.tasks.len(), 2);
        assert_eq!(plan.tasks[0].task_key, "two_task");
        assert_eq!(plan.tasks[0].command, "echo second");
        assert_eq!(plan.tasks[1].task_key, "three_task");
        assert_eq!(plan.tasks[1].command, "echo third");
        assert_eq!(plan.tranche_plan[1].tranche_group.as_deref(), Some("core"));
        assert_eq!(plan.current_tranche_index, Some(1));
        assert_eq!(
            plan.prompt_snapshot
                .as_ref()
                .map(|snapshot| snapshot.entry_path.as_str()),
            Some(prompt_entry_path)
        );
    }

    #[test]
    fn compute_quality_gate_blocks_stable_when_policy_disabled() {
        let report = DeteriorationReport {
            score: 22.0,
            window_size: 8,
            factors: Vec::new(),
            trend: DeteriorationTrend::Stable,
        };

        let gate = compute_quality_gate(Some(&report), AutoAdvancePolicy::ImprovingOnly);
        assert!(!gate.allow_auto_advance);
        assert_eq!(
            gate.reason,
            "deterioration trend stable (stagnation blocked)"
        );
    }

    #[test]
    fn compute_quality_gate_allows_stable_when_policy_enabled() {
        let report = DeteriorationReport {
            score: 22.0,
            window_size: 8,
            factors: Vec::new(),
            trend: DeteriorationTrend::Stable,
        };

        let gate = compute_quality_gate(Some(&report), AutoAdvancePolicy::StableOk);
        assert!(gate.allow_auto_advance);
        assert_eq!(
            gate.reason,
            "deterioration trend stable (policy allows auto-advance)"
        );
    }

    #[test]
    fn auto_advance_requires_planned_next_and_allowed_quality_gate() {
        use yarli_core::entities::continuation::{
            ContinuationPayload, ContinuationQualityGate, RunSummary, TrancheKind, TrancheSpec,
        };

        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: Vec::new(),
            summary: RunSummary {
                total: 1,
                completed: 1,
                failed: 0,
                cancelled: 0,
                pending: 0,
            },
            next_tranche: Some(TrancheSpec {
                suggested_objective: "next".into(),
                kind: TrancheKind::PlannedNext,
                retry_task_keys: Vec::new(),
                unfinished_task_keys: Vec::new(),
                planned_task_keys: vec!["test".into()],
                planned_tranche_key: Some("full".into()),
                cursor: None,
                config_snapshot: serde_json::json!({}),
            }),
            quality_gate: Some(ContinuationQualityGate {
                allow_auto_advance: true,
                reason: "improving".into(),
                trend: Some(DeteriorationTrend::Improving),
                score: Some(10.0),
            }),
        };

        let (allow, _) = should_auto_advance_planned_tranche(
            &payload,
            config::AutoAdvanceConfig {
                policy: AutoAdvancePolicy::ImprovingOnly,
                max_tranches: 0,
            },
            0,
        );
        assert!(allow);
    }
}
