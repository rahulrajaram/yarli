//! Command handlers extracted from `main.rs`.

use anyhow::anyhow;
use yarli_exec::{
    CommandRequest, CommandResult, CommandRunner, LocalCommandRunner, OverwatchCommandRunner,
    OverwatchRunnerConfig,
};
use yarli_observability::{run_analyzer, AuditCategory, YarliMetrics};
use yarli_queue::scheduler::LiveOutputEvent;
use yarli_queue::{QueueEntry, QueueStatus};

use super::*;
use crate::cli::AuditOutputFormat;
use crate::events::*;
use crate::persistence::RUN_CONTINUATION_EVENT_TYPE;
use crate::render::*;
use crate::workspace::{
    cleanup_parallel_workspace, merge_parallel_workspace_results_with_resolution_with_events,
    merge_worktree_workspace_results, prepare_parallel_workspace_layout, MergeApplyTelemetryEvent,
    MergeResolutionConfig, ParallelWorkspaceMergeApplyError, ParallelWorkspaceMergeFailureKind,
    ParallelWorkspaceMergeReport, ParallelWorkspaceMode,
};
use yarli_core::domain::ExitReason;
use yarli_core::entities::continuation::{
    ContinuationIntervention, ContinuationInterventionKind, RetryScope, TaskHealthAction,
};
use yarli_store::{
    MIGRATION_0001_DOWN, MIGRATION_0001_INIT, MIGRATION_0002_DOWN, MIGRATION_0002_INDEXES,
};

fn collect_telemetry_string_values(metadata: Option<&serde_json::Value>) -> Vec<String> {
    let Some(values) = metadata.and_then(|value| value.as_array()) else {
        return Vec::new();
    };
    let mut output = Vec::new();
    for value in values {
        if let Some(text) = value.as_str() {
            output.push(text.to_string());
        }
    }
    output.sort_unstable();
    output.dedup();
    output
}

fn parse_conflicted_paths_from_repo_status(repo_status: &str) -> Vec<String> {
    let mut paths = Vec::new();
    for raw_line in repo_status.lines() {
        let line = raw_line.trim_end();
        if line.len() < 3 {
            continue;
        }
        let status = &line[..2];
        let is_unmerged = matches!(status, "DD" | "AU" | "UD" | "UA" | "DU" | "AA" | "UU");
        if !is_unmerged {
            continue;
        }
        let remainder = line[2..].trim();
        if remainder.is_empty() {
            continue;
        }
        // Porcelain rename/copy format uses `old -> new`; keep the destination path.
        let candidate = remainder
            .split(" -> ")
            .last()
            .unwrap_or(remainder)
            .trim()
            .trim_matches('"')
            .trim_matches('\'');
        if !candidate.is_empty() {
            paths.push(candidate.to_string());
        }
    }
    paths.sort_unstable();
    paths.dedup();
    paths
}

fn parse_conflicted_paths_from_reason(reason: &str) -> Vec<String> {
    let mut paths = Vec::new();
    for raw_line in reason.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((_, tail)) = line.split_once("patch failed:") {
            let candidate = tail
                .trim()
                .split(':')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .trim_matches('\'');
            if !candidate.is_empty() {
                paths.push(candidate.to_string());
                continue;
            }
        }
        if let Some(prefix) = line.strip_suffix(": patch does not apply") {
            let candidate = prefix
                .trim_start_matches("error:")
                .trim()
                .trim_matches('"')
                .trim_matches('\'');
            if !candidate.is_empty() {
                paths.push(candidate.to_string());
            }
        }
    }
    paths.sort_unstable();
    paths.dedup();
    paths
}

#[derive(Debug, Clone, Default)]
struct RunResourceBudgetLimits {
    max_run_cpu_user_ticks: Option<u64>,
    max_run_cpu_system_ticks: Option<u64>,
    max_run_io_read_bytes: Option<u64>,
    max_run_io_write_bytes: Option<u64>,
    max_run_total_tokens: Option<u64>,
    max_run_peak_rss_bytes: Option<u64>,
}

fn collect_run_resource_limits(
    store: &dyn EventStore,
    correlation_id: Uuid,
) -> Result<RunResourceBudgetLimits> {
    let mut budget = RunResourceBudgetLimits::default();
    let mut latest_snapshot: Option<serde_json::Value> = None;

    for event in query_events(store, &EventQuery::by_correlation(correlation_id))? {
        if event.event_type == "run.config_snapshot" {
            latest_snapshot = event.payload.get("config_snapshot").cloned();
        }
    }

    let Some(snapshot) = latest_snapshot else {
        return Ok(budget);
    };
    let budgets = snapshot
        .get("config")
        .and_then(|value| value.get("budgets"));
    if budgets.is_none() {
        return Ok(budget);
    }
    let budgets = budgets.expect("checked above");
    budget.max_run_cpu_user_ticks = budgets
        .get("max_run_cpu_user_ticks")
        .and_then(serde_json::Value::as_u64);
    budget.max_run_cpu_system_ticks = budgets
        .get("max_run_cpu_system_ticks")
        .and_then(serde_json::Value::as_u64);
    budget.max_run_io_read_bytes = budgets
        .get("max_run_io_read_bytes")
        .and_then(serde_json::Value::as_u64);
    budget.max_run_io_write_bytes = budgets
        .get("max_run_io_write_bytes")
        .and_then(serde_json::Value::as_u64);
    budget.max_run_total_tokens = budgets
        .get("max_run_total_tokens")
        .and_then(serde_json::Value::as_u64);
    budget.max_run_peak_rss_bytes = budgets
        .get("max_run_peak_rss_bytes")
        .and_then(serde_json::Value::as_u64);
    Ok(budget)
}

fn format_budget_value(current: u64, limit: Option<u64>) -> String {
    match limit {
        Some(limit) => {
            let ratio = if limit == 0 {
                "n/a".to_string()
            } else {
                format!(" ({:.1}%)", (current as f64 / limit as f64) * 100.0)
            };
            format!("{current} / {limit}{ratio}")
        }
        None => current.to_string(),
    }
}

#[derive(Debug, Default)]
struct QueueDepthTotals {
    pending: usize,
    leased: usize,
    completed: usize,
    failed: usize,
    cancelled: usize,
}

impl QueueDepthTotals {
    fn add(&mut self, status: QueueStatus) {
        match status {
            QueueStatus::Pending => self.pending = self.pending.saturating_add(1),
            QueueStatus::Leased => self.leased = self.leased.saturating_add(1),
            QueueStatus::Completed => self.completed = self.completed.saturating_add(1),
            QueueStatus::Failed => self.failed = self.failed.saturating_add(1),
            QueueStatus::Cancelled => self.cancelled = self.cancelled.saturating_add(1),
        }
    }

    fn total(&self) -> usize {
        self.pending
            .saturating_add(self.leased)
            .saturating_add(self.completed)
            .saturating_add(self.failed)
            .saturating_add(self.cancelled)
    }
}

fn class_label(class: yarli_core::domain::CommandClass) -> &'static str {
    match class {
        yarli_core::domain::CommandClass::Io => "io",
        yarli_core::domain::CommandClass::Cpu => "cpu",
        yarli_core::domain::CommandClass::Git => "git",
        yarli_core::domain::CommandClass::Tool => "tool",
    }
}

fn render_queue_depth(entries: &[QueueEntry]) -> String {
    let mut total = QueueDepthTotals::default();
    let mut by_run: BTreeMap<Uuid, QueueDepthTotals> = BTreeMap::new();
    let mut by_class: BTreeMap<&'static str, QueueDepthTotals> = BTreeMap::new();

    for entry in entries {
        total.add(entry.status);
        by_run.entry(entry.run_id).or_default().add(entry.status);
        by_class
            .entry(class_label(entry.command_class))
            .or_default()
            .add(entry.status);
    }

    let mut output = String::new();
    output.push_str("Queue depth report\n");
    output.push_str("----------------\n");
    output.push_str(&format!(
        "overall: total={} pending={} leased={} completed={} failed={} cancelled={}\n",
        total.total(),
        total.pending,
        total.leased,
        total.completed,
        total.failed,
        total.cancelled
    ));

    output.push('\n');
    output.push_str("By run:\n");
    if by_run.is_empty() {
        output.push_str("  (no queue entries)\n");
    } else {
        for (run_id, tally) in by_run {
            output.push_str(&format!(
                "  {run_id}: total={} pending={} leased={} completed={} failed={} cancelled={}\n",
                tally.total(),
                tally.pending,
                tally.leased,
                tally.completed,
                tally.failed,
                tally.cancelled
            ));
        }
    }

    output.push('\n');
    output.push_str("By command class:\n");
    if by_class.is_empty() {
        output.push_str("  (no queue entries)\n");
    } else {
        for (class, tally) in by_class {
            output.push_str(&format!(
                "  {class}: total={} pending={} leased={} completed={} failed={} cancelled={}\n",
                tally.total(),
                tally.pending,
                tally.leased,
                tally.completed,
                tally.failed,
                tally.cancelled
            ));
        }
    }

    output
}

fn render_active_leases(entries: &[QueueEntry], now: chrono::DateTime<chrono::Utc>) -> String {
    let mut leases = Vec::new();
    for entry in entries
        .iter()
        .filter(|entry| entry.status == QueueStatus::Leased)
    {
        leases.push(entry);
    }
    leases.sort_by(|left, right| left.lease_expires_at.cmp(&right.lease_expires_at));

    let mut output = String::new();
    output.push_str("Active leases\n");
    output.push_str("-------------\n");

    if leases.is_empty() {
        output.push_str("No active leases.\n");
        return output;
    }

    for lease in leases {
        let expires_at = lease
            .lease_expires_at
            .map(|expires| {
                let ttl = expires.signed_duration_since(now);
                let secs = ttl.num_seconds();
                if secs <= 0 {
                    format!("expired ({secs}s ago)")
                } else {
                    format!("{}s", secs)
                }
            })
            .unwrap_or_else(|| "unknown".to_string());

        output.push_str(&format!(
            "{:<36} run={:<36} class={:<4} owner={:<16} task={} attempt={} ttl={}\n",
            lease.queue_id,
            lease.run_id,
            class_label(lease.command_class),
            lease.lease_owner.clone().unwrap_or_else(|| "-".to_string()),
            lease.task_id,
            lease.attempt_no,
            expires_at
        ));
    }

    output
}

fn render_resource_usage_summary(
    run_id: Uuid,
    totals: &RunResourceTotals,
    budgets: &RunResourceBudgetLimits,
) -> String {
    let mut output = String::new();
    output.push_str(&format!("Run resource usage for {run_id}\n"));
    output.push_str("--------------------------\n");
    output.push_str(&format!(
        "CPU user:       {}\n",
        format_budget_value(totals.total_cpu_user_ticks, budgets.max_run_cpu_user_ticks,)
    ));
    output.push_str(&format!(
        "CPU system:     {}\n",
        format_budget_value(
            totals.total_cpu_system_ticks,
            budgets.max_run_cpu_system_ticks,
        )
    ));
    output.push_str(&format!(
        "IO read:        {}\n",
        format_budget_value(totals.total_io_read_bytes, budgets.max_run_io_read_bytes)
    ));
    output.push_str(&format!(
        "IO write:       {}\n",
        format_budget_value(totals.total_io_write_bytes, budgets.max_run_io_write_bytes)
    ));
    output.push_str(&format!(
        "Peak RSS bytes: {}\n",
        format_budget_value(totals.peak_rss_bytes, budgets.max_run_peak_rss_bytes)
    ));
    output.push_str(&format!(
        "Tokens:         {}\n",
        format_budget_value(totals.total_tokens, budgets.max_run_total_tokens)
    ));
    output
}

#[allow(clippy::type_complexity)]
fn merge_apply_conflict_metadata(
    telemetry: &[MergeApplyTelemetryEvent],
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Vec<String>,
    Vec<String>,
    Option<String>,
) {
    telemetry
        .iter()
        .rev()
        .find(|event| {
            event.event_type == "merge.apply.conflict"
                || (event.event_type == "merge.apply.finalized"
                    && event
                        .metadata
                        .get("status")
                        .and_then(|value| value.as_str())
                        == Some("failed"))
        })
        .map_or_else(
            || (None, None, None, Vec::new(), Vec::new(), None),
            |event| {
                let task_key = event
                    .metadata
                    .get("task_key")
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string);
                let patch_path = event
                    .metadata
                    .get("patch_path")
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string);
                let workspace_path = event
                    .metadata
                    .get("workspace_path")
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string);
                let reason = event
                    .metadata
                    .get("reason")
                    .and_then(|value| value.as_str());
                let recovery_hints =
                    collect_telemetry_string_values(event.metadata.get("recovery_hints"));
                let repo_status = event
                    .metadata
                    .get("repo_status")
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string);
                let mut conflicted_files =
                    collect_telemetry_string_values(event.metadata.get("conflicted_files"));
                if conflicted_files.is_empty() {
                    if let Some(status) = repo_status.as_deref() {
                        conflicted_files = parse_conflicted_paths_from_repo_status(status);
                    }
                }
                if conflicted_files.is_empty() {
                    if let Some(reason) = reason {
                        conflicted_files = parse_conflicted_paths_from_reason(reason);
                    }
                }
                (
                    task_key,
                    patch_path,
                    workspace_path,
                    conflicted_files,
                    recovery_hints,
                    repo_status,
                )
            },
        )
}

#[derive(Clone)]
enum SelectedCommandRunner {
    Native(LocalCommandRunner),
    Overwatch(OverwatchCommandRunner),
}

impl CommandRunner for SelectedCommandRunner {
    async fn run(
        &self,
        request: CommandRequest,
        cancel: CancellationToken,
    ) -> Result<CommandResult, yarli_exec::ExecError> {
        match self {
            Self::Native(runner) => runner.run(request, cancel).await,
            Self::Overwatch(runner) => runner.run(request, cancel).await,
        }
    }
}

pub(crate) fn resolve_render_mode(
    term_info: &TerminalInfo,
    cli_force_stream: bool,
    cli_force_tui: bool,
    configured_ui_mode: UiMode,
) -> Result<RenderMode> {
    let (force_stream, force_tui) = if cli_force_stream || cli_force_tui {
        (cli_force_stream, cli_force_tui)
    } else {
        match configured_ui_mode {
            UiMode::Auto => (false, false),
            UiMode::Stream => (true, false),
            UiMode::Tui => (false, true),
        }
    };
    mode::select_render_mode(term_info, force_stream, force_tui).map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(feature = "sw4rm")]
fn sw4rm_verification_runner(loaded_config: &LoadedConfig) -> Result<SelectedCommandRunner> {
    match loaded_config.config().execution.runner {
        ExecutionRunner::Native => Ok(SelectedCommandRunner::Native(LocalCommandRunner::new())),
        ExecutionRunner::Overwatch => {
            let overwatch = &loaded_config.config().execution.overwatch;
            let runner = OverwatchCommandRunner::new(OverwatchRunnerConfig {
                service_url: overwatch.service_url.clone(),
                profile: overwatch
                    .profile
                    .clone()
                    .filter(|value| !value.trim().is_empty()),
                soft_timeout_seconds: overwatch.soft_timeout_seconds,
                silent_timeout_seconds: overwatch.silent_timeout_seconds,
                max_log_bytes: overwatch.max_log_bytes,
                ..OverwatchRunnerConfig::default()
            })
            .context("failed to initialize sw4rm verification runner")?;
            Ok(SelectedCommandRunner::Overwatch(runner))
        }
    }
}

async fn load_continuation_payload_for_continue(
    file: &Path,
    loaded_config: &LoadedConfig,
) -> Result<yarli_core::entities::ContinuationPayload> {
    let wait_timeout_secs = loaded_config.config().run.continue_wait_timeout_seconds;
    let deadline = (wait_timeout_secs > 0)
        .then(|| tokio::time::Instant::now() + Duration::from_secs(wait_timeout_secs));
    let mut waiting_logged = false;

    loop {
        if let Some(payload) =
            crate::persistence::try_load_continuation_payload_for_continue(file, loaded_config)?
        {
            if waiting_logged {
                info!(
                    run_id = %payload.run_id,
                    "continuation payload became available while waiting"
                );
            } else if file == Path::new(crate::persistence::DEFAULT_CONTINUATION_FILE) {
                info!(
                    run_id = %payload.run_id,
                    "loaded continuation payload from event store or file artifact"
                );
            }
            return Ok(payload);
        }

        let Some(deadline) = deadline else {
            bail!(
                "no continuation payload available at {} (set [run] continue_wait_timeout_seconds > 0 to wait)",
                file.display()
            );
        };
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "no continuation payload became available within {}s at {}",
                wait_timeout_secs,
                file.display()
            );
        }
        if !waiting_logged {
            info!(
                timeout_secs = wait_timeout_secs,
                poll_interval_ms = CONTINUATION_WAIT_POLL_INTERVAL_MS,
                "waiting for continuation payload availability"
            );
            waiting_logged = true;
        }
        tokio::time::sleep(Duration::from_millis(CONTINUATION_WAIT_POLL_INTERVAL_MS)).await;
    }
}

/// `yarli run continue` — resume from a previous run's continuation payload.
///
/// When using the default file path, this prefers event-store-backed
/// continuation (`run.continuation`) and falls back to `.yarli/continuation.json`.
/// If continuation is temporarily unavailable, this polls until
/// `[run] continue_wait_timeout_seconds` elapses.
/// If subsequent runs complete and the continuation quality gate allows it,
/// planned-next tranches auto-advance.
pub(crate) async fn cmd_run_continue(
    file: PathBuf,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
    allow_recursive_run_override: bool,
) -> Result<()> {
    let payload = load_continuation_payload_for_continue(&file, loaded_config).await?;

    let Some(tranche) = payload.next_tranche.clone() else {
        let exit_reason = payload
            .exit_reason
            .map(|reason| format!("{reason:?}"))
            .unwrap_or_else(|| "none".to_string());
        match payload.exit_state {
            RunState::RunFailed => {
                bail!(
                    "run {} is RunFailed (reason: {}) with no recovery tranche; inspect `yarli run explain-exit {}` and PARALLEL_MERGE_RECOVERY.txt",
                    payload.run_id,
                    exit_reason,
                    payload.run_id
                );
            }
            RunState::RunCancelled | RunState::RunBlocked => {
                bail!(
                    "run {} is {:?} (reason: {}) with no continuation tranche",
                    payload.run_id,
                    payload.exit_state,
                    exit_reason
                );
            }
            _ => {
                bail!("nothing to continue — all tasks completed successfully");
            }
        }
    };

    let auto_advance = config::AutoAdvanceConfig::from_loaded(loaded_config);
    let mut plan = build_plan_from_continuation_tranche(&tranche, loaded_config)?;
    let mut iteration = 1usize;
    let mut advances_taken = 0usize;
    let mut iteration_metrics: Vec<IterationMetrics> = Vec::new();
    loop {
        info!(
            objective = %plan.objective,
            task_count = plan.tasks.len(),
            tranche_index = ?plan.current_tranche_index,
            iteration,
            "continuing from previous run"
        );
        let iteration_started_at = Instant::now();
        let outcome = execute_run_plan(
            plan.clone(),
            render_mode,
            loaded_config,
            allow_recursive_run_override,
        )
        .await?;
        let duration = iteration_started_at.elapsed();
        print_iteration_metrics(iteration, &outcome, duration);
        iteration_metrics.push(IterationMetrics {
            iteration,
            run_id: outcome.run_id,
            duration,
            token_totals: outcome.token_totals,
        });
        if outcome.run_state != RunState::RunCompleted {
            print_invocation_summary(&iteration_metrics);
            return finalize_run_outcome(&outcome);
        }

        let (allow, reason) = should_auto_advance_planned_tranche(
            &outcome.continuation_payload,
            auto_advance,
            advances_taken,
        );
        if !allow {
            info!(reason = %reason, "stopping auto-advance");
            if reason != "no next tranche available" {
                println!("Auto-advance stopped: {reason}");
            }
            print_invocation_summary(&iteration_metrics);
            return finalize_run_outcome(&outcome);
        }

        let Some(next) = outcome.continuation_payload.next_tranche.as_ref() else {
            print_invocation_summary(&iteration_metrics);
            return finalize_run_outcome(&outcome);
        };

        println!(
            "Auto-advancing to planned tranche (iteration {}): {}",
            iteration + 1,
            next.suggested_objective
        );
        plan = build_plan_from_continuation_tranche(next, loaded_config)?;
        iteration += 1;
        advances_taken += 1;
    }
}

/// `yarli run` — config-first entrypoint: resolve prompt context and execute plan-driven tranches.
pub(crate) async fn cmd_run_default(
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
    prompt_file_override: Option<PathBuf>,
    allow_recursive_run_override: bool,
) -> Result<()> {
    let resolved_prompt =
        resolve_prompt_entry_path(loaded_config, prompt_file_override.as_deref())?;
    info!(
        prompt_entry_path = %resolved_prompt.entry_path.display(),
        prompt_source = resolved_prompt.source.as_str(),
        "resolved prompt file for yarli run"
    );
    let loaded_optional = prompt::load_prompt_with_optional_run_spec(&resolved_prompt.entry_path)
        .with_context(|| {
        format!(
            "failed to load prompt context from {}",
            resolved_prompt.entry_path.display()
        )
    })?;
    let has_prompt_run_spec = loaded_optional.run_spec.is_some();
    let has_config_run_spec = run_config_has_run_spec_data(&loaded_config.config().run);
    let config_run_spec = run_spec_from_run_config(&loaded_config.config().run);
    if has_config_run_spec {
        prompt::validate_run_spec(&config_run_spec)
            .context("invalid [run] run-spec configuration in yarli.toml")?;
    }
    let run_spec = merge_run_specs(&config_run_spec, loaded_optional.run_spec.as_ref());
    prompt::validate_run_spec(&run_spec).context(
        "invalid effective run spec after merging yarli.toml [run] with optional PROMPT.md overrides",
    )?;
    let has_effective_run_spec = has_prompt_run_spec || has_config_run_spec;
    let loaded = prompt::LoadedPrompt {
        entry_path: loaded_optional.entry_path.clone(),
        expanded_text: loaded_optional.expanded_text.clone(),
        snapshot: loaded_optional.snapshot.clone(),
        run_spec: run_spec.clone(),
    };
    let plan_guard_context = run_spec_plan_guard_preflight(&loaded)?;
    if let Some(validated_tranches_path) =
        validate_structured_tranches_preflight_for_prompt(&loaded.entry_path)?
    {
        info!(
            tranches_file = %validated_tranches_path.display(),
            "validated structured tranches file preflight"
        );
    }

    // Preflight: verify the CLI backend is functional before dispatching real work.
    let cli_invocation = config::resolve_cli_invocation_config(loaded_config)?;
    config::preflight_cli_backend(&cli_invocation).context(
        "CLI backend preflight check failed — fix the [cli] section in yarli.toml before running",
    )?;
    info!("CLI backend preflight passed");

    let objective = run_spec
        .objective
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "yarli run".to_string());
    let auto_advance = config::AutoAdvanceConfig::from_loaded(loaded_config);

    let (task_catalog, tranche_plan, execution_mode, plan_run_spec): (
        Vec<PlannedTask>,
        Vec<PlannedTranche>,
        &'static str,
        Option<prompt::RunSpec>,
    ) = match build_plan_driven_run_sequence(loaded_config, &loaded, &objective) {
        Ok((_tasks, tranches))
            if !has_effective_run_spec && is_verification_only_dispatch(&tranches) =>
        {
            info!(
                "no configured run spec and no open implementation tranches; dispatching plain prompt"
            );
            let (tasks, tranches) =
                build_plain_prompt_run_sequence(loaded_config, &loaded, &objective)?;
            (tasks, tranches, "plain-prompt", None)
        }
        Ok((tasks, tranches)) => (
            tasks,
            tranches,
            "config-first-plan",
            has_effective_run_spec.then_some(run_spec.clone()),
        ),
        Err(err) if !run_spec.tasks.items.is_empty() => {
            warn!(
                error = %err,
                "falling back to legacy prompt-defined task orchestration"
            );
            let tasks = build_task_catalog_from_run_spec(&run_spec)?;
            let tranches = build_tranche_plan_from_run_spec(&run_spec, &objective)?;
            (tasks, tranches, "legacy-prompt", Some(run_spec.clone()))
        }
        Err(err) if !has_effective_run_spec => {
            warn!(
                error = %err,
                "plan-driven dispatch unavailable for plain prompt; dispatching prompt text directly"
            );
            let (tasks, tranches) =
                build_plain_prompt_run_sequence(loaded_config, &loaded, &objective)?;
            (tasks, tranches, "plain-prompt", None)
        }
        Err(err) => {
            return Err(err.context(
                "failed to build plan-driven tranche execution (configure [cli] command/backend)",
            ));
        }
    };

    info!(mode = execution_mode, "resolved yarli run execution mode");

    let first_tranche = tranche_plan
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("run spec resolved to no tranches"))?;
    let first_tasks = tasks_for_tranche(&task_catalog, &first_tranche)?;

    let mut plan = RunPlan {
        objective: first_tranche.objective.clone(),
        tasks: first_tasks,
        task_catalog: task_catalog.clone(),
        workdir: loaded_config.config().execution.working_dir.clone(),
        timeout_secs: loaded_config.config().execution.command_timeout_seconds,
        pace: None,
        prompt_snapshot: Some(loaded.snapshot.clone()),
        run_spec: plan_run_spec,
        tranche_plan: tranche_plan.clone(),
        current_tranche_index: Some(0),
    };
    let verification_only_dispatch =
        execution_mode == "config-first-plan" && is_verification_only_dispatch(&plan.tranche_plan);

    let mut iteration = 1usize;
    let mut advances_taken = 0usize;
    let mut iteration_metrics: Vec<IterationMetrics> = Vec::new();
    loop {
        info!(
            objective = %plan.objective,
            task_count = plan.tasks.len(),
            tranche_index = ?plan.current_tranche_index,
            iteration,
            "running prompt-defined tranche"
        );
        let iteration_started_at = Instant::now();
        let outcome = execute_run_plan(
            plan.clone(),
            render_mode,
            loaded_config,
            allow_recursive_run_override,
        )
        .await?;
        let duration = iteration_started_at.elapsed();
        print_iteration_metrics(iteration, &outcome, duration);
        iteration_metrics.push(IterationMetrics {
            iteration,
            run_id: outcome.run_id,
            duration,
            token_totals: outcome.token_totals,
        });
        if outcome.run_state != RunState::RunCompleted {
            print_invocation_summary(&iteration_metrics);
            return finalize_run_outcome(&outcome);
        }
        if verification_only_dispatch && advances_taken == 0 {
            if outcome.continuation_payload.next_tranche.is_some() {
                warn!(
                    run_id = %outcome.run_id,
                    "ignoring continuation next_tranche for verification-only dispatch"
                );
            }
            if let Some(context) = plan_guard_context.as_ref() {
                enforce_plan_guard_post_run(&loaded, context)?;
            }
            print_invocation_summary(&iteration_metrics);
            return finalize_run_outcome(&outcome);
        }

        let (allow, reason) = should_auto_advance_planned_tranche(
            &outcome.continuation_payload,
            auto_advance,
            advances_taken,
        );
        if !allow {
            info!(reason = %reason, "stopping auto-advance");
            if reason != "no next tranche available" {
                println!("Auto-advance stopped: {reason}");
            }
            if let Some(context) = plan_guard_context.as_ref() {
                enforce_plan_guard_post_run(&loaded, context)?;
            }
            print_invocation_summary(&iteration_metrics);
            return finalize_run_outcome(&outcome);
        }

        let Some(next) = outcome.continuation_payload.next_tranche.as_ref() else {
            if let Some(context) = plan_guard_context.as_ref() {
                enforce_plan_guard_post_run(&loaded, context)?;
            }
            print_invocation_summary(&iteration_metrics);
            return finalize_run_outcome(&outcome);
        };
        println!(
            "Auto-advancing to planned tranche (iteration {}): {}",
            iteration + 1,
            next.suggested_objective
        );
        plan = build_plan_from_continuation_tranche(next, loaded_config)?;
        iteration += 1;
        advances_taken += 1;
    }
}

pub(crate) async fn cmd_run_start_with_backend<Q, S>(
    plan: RunPlan,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
    store: Arc<S>,
    queue: Arc<Q>,
    allow_recursive_run_override: bool,
    metrics: Arc<YarliMetrics>,
    #[cfg(feature = "chaos")] chaos: Option<Arc<yarli_chaos::ChaosController>>,
) -> Result<RunExecutionOutcome>
where
    Q: TaskQueue + 'static,
    S: EventStore + 'static,
{
    if plan.tasks.is_empty() {
        bail!("at least one task is required");
    }

    let parallel_workspace_layout = prepare_parallel_workspace_layout(&plan, loaded_config)?;
    if let Some(layout) = parallel_workspace_layout.as_ref() {
        println!(
            "Parallel workspaces prepared: {} task workspace(s) at {}",
            layout.task_workspace_dirs.len(),
            layout.run_workspace_root.display()
        );
    }

    // Create shutdown controller early so the runner can track child PIDs.
    // This ensures `terminate_children()` can clean up zombie processes when
    // the run reaches a terminal state programmatically (not just on Ctrl+C).
    let shutdown = ShutdownController::new();
    shutdown.install_signal_handler();

    let idle_kill_timeout = if loaded_config.config().event_loop.idle_timeout_secs > 0 {
        Some(Duration::from_secs(
            loaded_config.config().event_loop.idle_timeout_secs,
        ))
    } else {
        None
    };
    let runner = match loaded_config.config().execution.runner {
        ExecutionRunner::Native => {
            let lcr = LocalCommandRunner::new().with_shutdown(shutdown.clone());
            let mut lcr = lcr;
            lcr.idle_kill_timeout = idle_kill_timeout;
            Arc::new(SelectedCommandRunner::Native(lcr))
        }
        ExecutionRunner::Overwatch => {
            let overwatch = &loaded_config.config().execution.overwatch;
            let runner = OverwatchCommandRunner::new(OverwatchRunnerConfig {
                service_url: overwatch.service_url.clone(),
                profile: overwatch
                    .profile
                    .clone()
                    .filter(|value| !value.trim().is_empty()),
                soft_timeout_seconds: overwatch.soft_timeout_seconds,
                silent_timeout_seconds: overwatch.silent_timeout_seconds,
                max_log_bytes: overwatch.max_log_bytes,
                ..OverwatchRunnerConfig::default()
            })
            .map_err(|e| anyhow::anyhow!("failed to initialize overwatch runner: {e}"))?;
            Arc::new(SelectedCommandRunner::Overwatch(runner))
        }
    };

    let command_timeout = if plan.timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(plan.timeout_secs))
    };

    let scheduler_workdir =
        config::resolve_execution_path_from_cwd(&plan.workdir, "execution.working_dir")?;
    let mut config = SchedulerConfig {
        working_dir: scheduler_workdir.display().to_string(),
        command_timeout,
        ..SchedulerConfig::default()
    };
    config.claim_batch_size = loaded_config.config().queue.claim_batch_size;
    config.lease_ttl = chrono::Duration::seconds(loaded_config.config().queue.lease_ttl_seconds);
    config.heartbeat_interval =
        Duration::from_secs(loaded_config.config().queue.heartbeat_interval_seconds);
    config.reclaim_interval =
        Duration::from_secs(loaded_config.config().queue.reclaim_interval_seconds);
    config.reclaim_grace =
        chrono::Duration::seconds(loaded_config.config().queue.reclaim_grace_seconds);
    config.tick_interval = Duration::from_millis(loaded_config.config().execution.tick_interval_ms);
    config.concurrency.per_run_cap = loaded_config.config().queue.per_run_cap;
    config.concurrency.io_cap = loaded_config.config().queue.io_cap;
    config.concurrency.cpu_cap = loaded_config.config().queue.cpu_cap;
    config.concurrency.git_cap = loaded_config.config().queue.git_cap;
    config.concurrency.tool_cap = loaded_config.config().queue.tool_cap;
    if !loaded_config.config().features.parallel {
        config.claim_batch_size = 1;
        config.concurrency.per_run_cap = 1;
        config.concurrency.io_cap = 1;
        config.concurrency.cpu_cap = 1;
        config.concurrency.git_cap = 1;
        config.concurrency.tool_cap = 1;
    }
    if let Some(worker_id) = loaded_config.config().core.worker_id.as_ref() {
        config.worker_id = worker_id.clone();
    }
    config.enforce_policies = loaded_config.config().policy.enforce_policies;
    config.audit_decisions = loaded_config.config().policy.audit_decisions;
    config.allow_recursive_run =
        recursive_run_execution_enabled(loaded_config, allow_recursive_run_override);
    let el = &loaded_config.config().event_loop;
    config.max_runtime = if el.max_runtime_seconds > 0 {
        Some(Duration::from_secs(el.max_runtime_seconds))
    } else {
        None
    };
    config.idle_timeout = if el.idle_timeout_secs > 0 {
        Some(Duration::from_secs(el.idle_timeout_secs))
    } else {
        None
    };
    config.budgets = ResourceBudgetConfig {
        max_task_rss_bytes: loaded_config.config().budgets.max_task_rss_bytes,
        max_task_cpu_user_ticks: loaded_config.config().budgets.max_task_cpu_user_ticks,
        max_task_cpu_system_ticks: loaded_config.config().budgets.max_task_cpu_system_ticks,
        max_task_io_read_bytes: loaded_config.config().budgets.max_task_io_read_bytes,
        max_task_io_write_bytes: loaded_config.config().budgets.max_task_io_write_bytes,
        max_task_total_tokens: loaded_config.config().budgets.max_task_total_tokens,
        max_run_total_tokens: loaded_config.config().budgets.max_run_total_tokens,
        max_run_peak_rss_bytes: loaded_config.config().budgets.max_run_peak_rss_bytes,
        max_run_cpu_user_ticks: loaded_config.config().budgets.max_run_cpu_user_ticks,
        max_run_cpu_system_ticks: loaded_config.config().budgets.max_run_cpu_system_ticks,
        max_run_io_read_bytes: loaded_config.config().budgets.max_run_io_read_bytes,
        max_run_io_write_bytes: loaded_config.config().budgets.max_run_io_write_bytes,
    };

    let mut scheduler = Scheduler::new(queue, store.clone(), runner, config).with_metrics(metrics);

    #[cfg(feature = "chaos")]
    if let Some(c) = chaos {
        scheduler = scheduler.with_chaos(c);
    }

    if loaded_config.config().policy.audit_decisions {
        let audit_path = PathBuf::from(&loaded_config.config().observability.audit_file);
        if let Some(parent) = audit_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create audit directory {}", parent.display())
                })?;
            }
        }
        scheduler = scheduler.with_audit_sink(Arc::new(JsonlAuditSink::new(&audit_path)));
    }

    // Build run and tasks.
    let run_snapshot = build_run_config_snapshot(
        loaded_config,
        &plan.workdir,
        plan.timeout_secs,
        &plan.tasks,
        &plan.task_catalog,
        plan.pace.as_deref(),
        plan.prompt_snapshot.as_ref(),
        plan.run_spec.as_ref(),
        &plan.tranche_plan,
        plan.current_tranche_index,
    )
    .context("failed to build run config snapshot")?;
    let run = Run::with_config(
        &plan.objective,
        loaded_config.config().core.safe_mode,
        run_snapshot.clone(),
    );
    let run_id = run.id;
    let correlation_id = run.correlation_id;

    // Start a run-level span now that we have an ID.
    let run_span = tracing::info_span!("run_execution", run_id = %run_id);
    let _enter = run_span.enter();

    let mut tasks: Vec<Task> = Vec::with_capacity(plan.tasks.len());
    let mut task_key_to_id: HashMap<String, Uuid> = HashMap::with_capacity(plan.tasks.len());
    for planned in &plan.tasks {
        let task = Task::new(
            run_id,
            planned.task_key.clone(),
            &planned.command,
            planned.command_class,
            correlation_id,
        );
        task_key_to_id.insert(planned.task_key.clone(), task.id);
        tasks.push(task);
    }

    let mut indegree = HashMap::with_capacity(plan.tasks.len());
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    for planned in &plan.tasks {
        indegree.entry(planned.task_key.clone()).or_insert(0usize);
        for depends_on_key in &planned.depends_on {
            if !task_key_to_id.contains_key(depends_on_key) {
                bail!(
                    "task {} depends_on unknown task key {}",
                    planned.task_key,
                    depends_on_key
                );
            }
            indegree
                .entry(planned.task_key.clone())
                .and_modify(|degree| *degree += 1)
                .or_insert(1);
            dependents
                .entry(depends_on_key.clone())
                .or_default()
                .push(planned.task_key.clone());
        }
    }

    let mut ready: std::collections::VecDeque<String> = indegree
        .iter()
        .filter_map(|(key, degree)| (*degree == 0).then_some(key.clone()))
        .collect();
    let mut resolved_count = 0usize;
    while let Some(key) = ready.pop_front() {
        resolved_count += 1;
        if let Some(children) = dependents.get(&key) {
            for child in children {
                let degree = indegree
                    .get_mut(child)
                    .ok_or_else(|| anyhow!("internal error: missing task in dependency index"))?;
                *degree = degree.saturating_sub(1);
                if *degree == 0 {
                    ready.push_back(child.clone());
                }
            }
        }
    }
    if resolved_count != plan.tasks.len() {
        bail!("run has cyclic task dependency graph");
    }

    for planned in &plan.tasks {
        let task_id = *task_key_to_id
            .get(&planned.task_key)
            .ok_or_else(|| anyhow!("failed to resolve task id for {}", planned.task_key))?;
        let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) else {
            continue;
        };
        for depends_on_key in &planned.depends_on {
            let dependency_id = *task_key_to_id.get(depends_on_key).ok_or_else(|| {
                anyhow!(
                    "task {} depends_on unknown task key {}",
                    planned.task_key,
                    depends_on_key
                )
            })?;
            task.depends_on(dependency_id);
        }
    }

    let mut task_workspace_by_id: HashMap<Uuid, String> = HashMap::new();
    if let Some(layout) = parallel_workspace_layout.as_ref() {
        if layout.task_workspace_dirs.len() != tasks.len() {
            bail!(
                "parallel workspace provisioning mismatch: {} workspaces for {} tasks",
                layout.task_workspace_dirs.len(),
                tasks.len()
            );
        }
        for (task, workspace_dir) in tasks.iter().zip(layout.task_workspace_dirs.iter()) {
            let workspace_dir = workspace_dir.display().to_string();
            scheduler
                .bind_task_working_dir(task.id, workspace_dir.clone())
                .await;
            task_workspace_by_id.insert(task.id, workspace_dir);
        }
    }

    let task_names: Vec<(Uuid, String)> =
        tasks.iter().map(|t| (t.id, t.task_key.clone())).collect();

    seed_postgres_run_state_if_needed(loaded_config, &run, &tasks).await?;

    store.append(Event {
        event_id: Uuid::now_v7(),
        occurred_at: chrono::Utc::now(),
        entity_type: EntityType::Run,
        entity_id: run_id.to_string(),
        event_type: "run.config_snapshot".to_string(),
        payload: serde_json::json!({
            "objective": plan.objective.clone(),
            "safe_mode": loaded_config.config().core.safe_mode,
            "config_snapshot": run_snapshot.clone(),
        }),
        correlation_id,
        causation_id: None,
        actor: "cli".to_string(),
        idempotency_key: Some(format!("{run_id}:config_snapshot")),
    })?;

    store.append(Event {
        event_id: Uuid::now_v7(),
        occurred_at: chrono::Utc::now(),
        entity_type: EntityType::Run,
        entity_id: run_id.to_string(),
        event_type: "run.task_catalog".to_string(),
        payload: serde_json::json!({
            "tasks": plan
                .tasks
                .iter()
                .zip(tasks.iter())
                .map(|(planned, task)| serde_json::json!({
                    "task_id": task.id,
                    "task_key": planned.task_key,
                    "tranche_key": planned.tranche_key,
                    "tranche_group": planned.tranche_group,
                    "depends_on": planned.depends_on,
                    "allowed_paths": planned.allowed_paths,
                    "workspace_dir": task_workspace_by_id.get(&task.id),
                }))
                .collect::<Vec<_>>(),
        }),
        correlation_id,
        causation_id: None,
        actor: "cli".to_string(),
        idempotency_key: Some(format!("{run_id}:task_catalog")),
    })?;

    // Submit run.
    scheduler
        .submit_run(run, tasks)
        .await
        .context("failed to submit run")?;

    info!(run_id = %run_id, objective = %plan.objective, "run started");

    let cancel = shutdown.token();

    // Set up event channel for renderer.
    let (tx, rx) = mpsc::unbounded_channel::<StreamEvent>();

    // Emit known run/task identity immediately so the UI has deterministic
    // startup state before the first scheduler tick/event-store poll.
    emit_initial_stream_state(&tx, run_id, &plan.objective, &task_names);

    // Spawn renderer task.
    let renderer_shutdown = shutdown.clone();
    let verbose_output = loaded_config.config().ui.verbose_output;
    let audit_file = Some(PathBuf::from(
        loaded_config.config().observability.audit_file.clone(),
    ));
    let renderer_handle = tokio::task::spawn_blocking(move || {
        run_renderer(
            rx,
            render_mode,
            renderer_shutdown,
            verbose_output,
            audit_file,
        )
    });

    // Drive scheduler loop, emitting events to renderer channel.
    let scheduler_tx = tx.clone();
    let mut continuation_payload = drive_scheduler(
        &mut scheduler,
        &store,
        shutdown.clone(),
        cancel,
        scheduler_tx,
        run_id,
        correlation_id,
        &task_names,
        loaded_config.config().run.effective_auto_advance_policy(),
        loaded_config.config().run.task_health,
        loaded_config.config().budgets.max_run_total_tokens,
        loaded_config.config().run.soft_token_cap_ratio,
        loaded_config.config().ui.cancellation_diagnostics,
        observers::build_task_health_observer(run_id, correlation_id, &task_names),
        observers::build_memory_observer(
            loaded_config,
            run_id,
            correlation_id,
            &plan,
            &task_names,
        )?,
        Some(loaded_config),
    )
    .await?;

    // Terminate any tracked child processes that survived the scheduler loop.
    // This prevents zombie processes when a run reaches RunFailed programmatically
    // (not via Ctrl+C signal, which has its own signal-handler path).
    #[cfg(unix)]
    shutdown.terminate_children().await;

    let mut parallel_merge_error: Option<anyhow::Error> = None;
    let mut parallel_merge_error_reason: Option<ParallelWorkspaceMergeFailureKind> = None;
    let mut parallel_merge_report: Option<ParallelWorkspaceMergeReport> = None;
    let mut parallel_merge_retry_task_keys: Vec<String> = Vec::new();
    if continuation_payload.exit_state == RunState::RunCompleted {
        if let Some(layout) = parallel_workspace_layout.as_ref() {
            let source_workdir =
                config::resolve_execution_path_from_cwd(&plan.workdir, "execution.working_dir")?;
            let source_workdir = source_workdir.canonicalize().with_context(|| {
                format!(
                    "failed to canonicalize execution working_dir {}",
                    source_workdir.display()
                )
            })?;
            loaded_config.config().run.validate_merge_repair_config()?;
            let resolution_config =
                MergeResolutionConfig::from_run_config(&loaded_config.config().run);
            let mut merge_apply_telemetry = Vec::new();
            let task_workspaces = plan
                .tasks
                .iter()
                .zip(layout.task_workspace_dirs.iter())
                .map(|(task, workspace_dir)| (task.task_key.clone(), workspace_dir.clone()))
                .collect::<Vec<_>>();
            let merge_result = if layout.mode == ParallelWorkspaceMode::GitWorktree {
                merge_worktree_workspace_results(
                    &source_workdir,
                    run_id,
                    &task_workspaces,
                    &layout.worktree_branches,
                    &resolution_config,
                    &mut merge_apply_telemetry,
                )
            } else {
                merge_parallel_workspace_results_with_resolution_with_events(
                    &source_workdir,
                    run_id,
                    &layout.run_workspace_root,
                    &task_workspaces,
                    &resolution_config,
                    layout.source_head_at_creation.as_deref(),
                    &mut merge_apply_telemetry,
                )
            };
            match merge_result {
                Ok(report) => {
                    let merged_count = report.merged_task_keys.len();
                    let skipped_count = report.skipped_task_keys.len();
                    let preserved_workspace_root = report.preserve_workspace_root;
                    println!(
                        "Parallel workspace merge: merged {merged_count} task workspace(s), skipped {skipped_count} with no changes."
                    );
                    if preserved_workspace_root {
                        println!(
                            "Parallel workspace merge retained task workspaces for operator review: {}",
                            layout.run_workspace_root.display()
                        );
                    }
                    let merged_task_keys = report.merged_task_keys.clone();
                    let skipped_task_keys = report.skipped_task_keys.clone();
                    let task_outcomes = report.task_outcomes.clone();
                    if let Err(err) = store.append(Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Run,
                        entity_id: run_id.to_string(),
                        event_type: "run.parallel_merge_succeeded".to_string(),
                        payload: serde_json::json!({
                            "merged_task_keys": merged_task_keys,
                            "skipped_task_keys": skipped_task_keys,
                            "task_outcomes": task_outcomes,
                            "preserve_workspace_root": preserved_workspace_root,
                            "preserved_workspace_root": preserved_workspace_root.then(|| layout.run_workspace_root.display().to_string()),
                            "source_workdir": source_workdir.display().to_string(),
                            "workspace_root": layout.run_workspace_root.display().to_string(),
                        }),
                        correlation_id,
                        causation_id: None,
                        actor: "cli".to_string(),
                        idempotency_key: Some(format!("{run_id}:parallel_merge_succeeded")),
                    }) {
                        warn!(error = %err, "failed to persist parallel merge success event");
                    }
                    if let Err(err) = append_merge_apply_telemetry_events(
                        store.as_ref(),
                        run_id,
                        correlation_id,
                        &merge_apply_telemetry,
                        None,
                        scheduler.audit_sink().as_deref(),
                    ) {
                        warn!(error = %err, "failed to append merge apply telemetry events");
                    }
                    parallel_merge_report = Some(report);

                    // Auto-commit YARLI state files after successful merge.
                    let auto_commit_interval = loaded_config.config().run.auto_commit_interval;
                    if auto_commit_interval > 0 {
                        let tranche_key = plan
                            .tasks
                            .first()
                            .and_then(|t| t.tranche_key.as_deref())
                            .unwrap_or("unknown");
                        let auto_commit_message =
                            loaded_config.config().run.auto_commit_message.as_deref();
                        if let Err(err) = crate::workspace::auto_commit_state_files(
                            &source_workdir,
                            tranche_key,
                            &run_id.to_string(),
                            1,
                            1,
                            auto_commit_message,
                        ) {
                            warn!(error = %err, "auto-commit of state files failed");
                        }
                    }
                }
                Err(err) => {
                    let merge_failure_kind = err
                        .root_cause()
                        .downcast_ref::<ParallelWorkspaceMergeApplyError>()
                        .map_or(
                            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                            |merge_error| merge_error.kind,
                        );
                    let (
                        conflict_task_key,
                        conflict_patch_path,
                        conflict_workspace_path,
                        conflict_files,
                        recovery_hints,
                        conflict_repo_status,
                    ) = merge_apply_conflict_metadata(&merge_apply_telemetry);
                    if let Some(task_key) = conflict_task_key.as_ref() {
                        parallel_merge_retry_task_keys.push(task_key.clone());
                    } else if let Some(task_key) = merge_apply_telemetry
                        .iter()
                        .rev()
                        .filter_map(|event| event.task_key.as_ref())
                        .next()
                    {
                        parallel_merge_retry_task_keys.push(task_key.clone());
                    }
                    if let Err(append_err) = store.append(Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Run,
                        entity_id: run_id.to_string(),
                        event_type: "run.parallel_merge_failed".to_string(),
                        payload: serde_json::json!({
                            "reason": err.to_string(),
                            "source_workdir": source_workdir.display().to_string(),
                            "workspace_root": layout.run_workspace_root.display().to_string(),
                            "task_key": conflict_task_key,
                            "patch_path": conflict_patch_path,
                            "workspace_path": conflict_workspace_path,
                            "repo_status": conflict_repo_status,
                            "conflicted_files": conflict_files,
                            "recovery_hints": recovery_hints,
                        }),
                        correlation_id,
                        causation_id: None,
                        actor: "cli".to_string(),
                        idempotency_key: Some(format!("{run_id}:parallel_merge_failed")),
                    }) {
                        warn!(error = %append_err, "failed to persist parallel merge failure event");
                    }
                    if let Err(err) = append_merge_apply_telemetry_events(
                        store.as_ref(),
                        run_id,
                        correlation_id,
                        &merge_apply_telemetry,
                        None,
                        scheduler.audit_sink().as_deref(),
                    ) {
                        warn!(error = %err, "failed to append merge apply telemetry events");
                    }
                    parallel_merge_error = Some(
                        err.context(format!("parallel workspace merge failed for run {run_id}")),
                    );
                    parallel_merge_error_reason = Some(merge_failure_kind);
                }
            }
        }
    }

    if let Some(layout) = parallel_workspace_layout.as_ref() {
        if parallel_merge_error.is_none() {
            let preserve_due_to_skipped_merge = parallel_merge_report
                .as_ref()
                .map(|report| report.preserve_workspace_root)
                .unwrap_or(false);
            let preserve_due_to_non_completed_run =
                continuation_payload.exit_state != RunState::RunCompleted;
            if preserve_due_to_non_completed_run {
                warn!(
                    workspace_root = %layout.run_workspace_root.display(),
                    exit_state = ?continuation_payload.exit_state,
                    "preserving parallel run workspace root because run did not complete"
                );
                println!(
                    "Parallel workspace root preserved for recovery: {}",
                    layout.run_workspace_root.display()
                );
                let mut failed_task_workspaces = continuation_payload
                    .tasks
                    .iter()
                    .filter_map(|task| {
                        if task.state == TaskState::TaskComplete {
                            return None;
                        }
                        task_workspace_by_id.get(&task.task_id).map(|workspace| {
                            (task.task_key.as_str(), task.state, workspace.as_str())
                        })
                    })
                    .map(|(task_key, state, workspace)| {
                        (
                            task_key.to_owned(),
                            format!("{state:?}"),
                            workspace.to_owned(),
                        )
                    })
                    .collect::<Vec<_>>();
                failed_task_workspaces.sort_by(|a, b| a.0.cmp(&b.0));
                if failed_task_workspaces.is_empty() {
                    println!("Failed/unfinished task workspaces were not tracked for this run.");
                } else {
                    println!("Failed/unfinished task workspace paths:");
                    for (task_key, state, workspace) in failed_task_workspaces {
                        println!("  {} [{}] -> {}", task_key, state, workspace);
                    }
                }
            } else if preserve_due_to_skipped_merge {
                warn!(
                    workspace_root = %layout.run_workspace_root.display(),
                    "preserving parallel run workspace root after skipped task workspace merge for inspection"
                );
            } else {
                cleanup_parallel_workspace(layout);
            }
        } else {
            warn!(
                workspace_root = %layout.run_workspace_root.display(),
                "preserving parallel run workspace root after merge failure for inspection"
            );
        }
    }

    if let Some(err) = parallel_merge_error.as_ref() {
        warn!(run_id = %run_id, error = %err, "parallel workspace merge finalization failed");
        if continuation_payload.exit_state == RunState::RunCompleted {
            continuation_payload.exit_state = RunState::RunFailed;
            continuation_payload.exit_reason = Some(match parallel_merge_error_reason {
                Some(ParallelWorkspaceMergeFailureKind::MergeConflict) => ExitReason::MergeConflict,
                _ => ExitReason::FailedRuntimeError,
            });
            if parallel_merge_retry_task_keys.is_empty() {
                parallel_merge_retry_task_keys = plan
                    .tasks
                    .iter()
                    .map(|task| task.task_key.clone())
                    .collect();
            }
            parallel_merge_retry_task_keys.sort_unstable();
            parallel_merge_retry_task_keys.dedup();
            continuation_payload.next_tranche =
                (!parallel_merge_retry_task_keys.is_empty()).then(|| {
                    yarli_core::entities::continuation::TrancheSpec {
                        suggested_objective: format!(
                            "Recover merge finalization by re-running tasks: {}",
                            parallel_merge_retry_task_keys.join(", ")
                        ),
                        kind: yarli_core::entities::continuation::TrancheKind::RetryUnfinished,
                        retry_task_keys: parallel_merge_retry_task_keys.clone(),
                        unfinished_task_keys: Vec::new(),
                        planned_task_keys: Vec::new(),
                        planned_tranche_key: None,
                        cursor: None,
                        config_snapshot: run_snapshot.clone(),
                        interventions: Vec::new(),
                    }
                });
            eprintln!("Run {run_id} completed core tasks, but parallel merge did not finalize;");
            if continuation_payload.next_tranche.is_some() {
                eprintln!("Continuation prepared a recovery tranche for explicit operator retry.");
            } else {
                eprintln!(
                    "Continuation state set to RunFailed with no recovery tranche available."
                );
            }
        }
    }

    // Sync materialized state columns in Postgres so `yarli run status` and
    // direct DB queries reflect the actual terminal state, not the initial RUN_OPEN.
    if let Err(e) = sync_postgres_state(&scheduler, loaded_config, run_id).await {
        warn!(error = %e, "failed to sync postgres state on exit");
    }

    if let Err(e) =
        persist_continuation_payload_event(store.as_ref(), &continuation_payload, correlation_id)
    {
        warn!(error = %e, "failed to persist continuation payload event");
    }

    let cont_dir = PathBuf::from(".yarli");
    if let Err(e) = fs::create_dir_all(&cont_dir) {
        warn!(error = %e, "failed to create .yarli directory");
    } else {
        let cont_path = cont_dir.join("continuation.json");
        match serde_json::to_string_pretty(&continuation_payload) {
            Ok(json) => {
                if let Err(e) = fs::write(&cont_path, json) {
                    warn!(error = %e, "failed to write continuation file");
                } else {
                    info!(path = %cont_path.display(), "wrote continuation file");
                }
            }
            Err(e) => warn!(error = %e, "failed to serialize continuation payload"),
        }
    }

    if continuation_payload.exit_state == RunState::RunCompleted {
        if let Err(err) = maybe_mark_current_structured_tranche_complete(&plan) {
            warn!(
                run_id = %run_id,
                error = %err,
                error_chain = %format!("{err:#}"),
                "failed to auto-complete structured tranche status"
            );
        }
    }

    let token_totals = match collect_run_token_totals(store.as_ref(), correlation_id) {
        Ok(totals) => totals,
        Err(err) => {
            warn!(
                run_id = %run_id,
                error = %err,
                "failed to collect run token totals"
            );
            RunTokenTotals::default()
        }
    };

    let _ = tx.send(StreamEvent::RunExited {
        payload: continuation_payload.clone(),
    });

    // Drop the sender BEFORE awaiting the renderer so the channel closes
    // and the renderer's `blocking_recv` loop can terminate.
    drop(tx);

    // Wait for renderer to finish.
    renderer_handle
        .await
        .context("renderer task panicked")?
        .context("renderer error")?;

    Ok(RunExecutionOutcome {
        run_id,
        run_state: continuation_payload.exit_state,
        continuation_payload,
        token_totals,
    })
}

/// `yarli run sw4rm` — boot as a sw4rm orchestrator agent.
///
/// Reads `[sw4rm]` config from `yarli.toml`, creates the agent infrastructure,
/// connects to the sw4rm runtime, and enters the agent message loop.
#[cfg(feature = "sw4rm")]
pub(crate) async fn cmd_run_sw4rm(loaded_config: &LoadedConfig) -> Result<()> {
    use yarli_sw4rm::{
        orchestrator::{VerificationCommand, VerificationSpec},
        OrchestratorLoop, ShutdownBridge, YarliAgent,
    };

    let sw4rm_config = loaded_config.config().sw4rm.clone();
    println!("Booting sw4rm agent: {}", sw4rm_config.agent_name);

    // Build verification spec from merged run-spec configuration:
    // yarli.toml [run] defaults + optional PROMPT.md override block.
    let has_config_run_spec = run_config_has_run_spec_data(&loaded_config.config().run);
    let mut base_run_spec = run_spec_from_run_config(&loaded_config.config().run);
    if has_config_run_spec {
        if let Err(err) = prompt::validate_run_spec(&base_run_spec) {
            warn!(
                error = %err,
                "invalid [run] run-spec configuration for sw4rm; using fallback verification defaults"
            );
            base_run_spec = prompt::RunSpec {
                version: 1,
                objective: None,
                tasks: prompt::RunSpecTasks::default(),
                tranches: None,
                plan_guard: None,
            };
        }
    }
    if let Ok(resolved) = resolve_prompt_entry_path(loaded_config, None) {
        if let Ok(loaded_prompt) = prompt::load_prompt_with_optional_run_spec(&resolved.entry_path)
        {
            if let Some(prompt_run_spec) = loaded_prompt.run_spec.as_ref() {
                base_run_spec = merge_run_specs(&base_run_spec, Some(prompt_run_spec));
            }
        }
    }
    if let Err(err) = prompt::validate_run_spec(&base_run_spec) {
        warn!(
            error = %err,
            "invalid effective sw4rm run-spec after merge; using fallback verification defaults"
        );
        base_run_spec = prompt::RunSpec {
            version: 1,
            objective: None,
            tasks: prompt::RunSpecTasks::default(),
            tranches: None,
            plan_guard: None,
        };
    }
    let verification_tasks = base_run_spec.tasks.items;
    let verification = if !verification_tasks.is_empty() {
        let commands: Vec<VerificationCommand> = verification_tasks
            .iter()
            .map(|t| {
                let class = match t.class.as_deref().unwrap_or("io") {
                    "cpu" => yarli_core::domain::CommandClass::Cpu,
                    "git" => yarli_core::domain::CommandClass::Git,
                    "tool" => yarli_core::domain::CommandClass::Tool,
                    _ => yarli_core::domain::CommandClass::Io,
                };
                VerificationCommand {
                    task_key: t.key.clone(),
                    command: t.cmd.clone(),
                    class,
                }
            })
            .collect();
        VerificationSpec {
            commands,
            working_dir: loaded_config.config().execution.working_dir.clone(),
            task_gates: None, // use defaults from yarli-gates
            run_gates: None,
        }
    } else {
        // No usable run-spec tasks — use a minimal verification spec.
        VerificationSpec {
            commands: vec![VerificationCommand {
                task_key: "build".to_string(),
                command: "cargo build".to_string(),
                class: yarli_core::domain::CommandClass::Cpu,
            }],
            working_dir: loaded_config.config().execution.working_dir.clone(),
            task_gates: None,
            run_gates: None,
        }
    };

    // NOTE: Using MockRouterSender — real RouterClient transport not yet implemented.
    // The agent will boot and register but LLM dispatch will not function.
    eprintln!("WARNING: using mock router sender — real sw4rm transport not yet implemented");
    let router = std::sync::Arc::new(yarli_sw4rm::mock::MockRouterSender::new());
    let command_runner = sw4rm_verification_runner(loaded_config)?;
    let orchestrator = std::sync::Arc::new(
        OrchestratorLoop::new(router, sw4rm_config.clone(), verification)
            .with_verification_runner(command_runner),
    );

    // Build sw4rm AgentConfig
    let agent_config = sw4rm_sdk::AgentConfig::new(
        sw4rm_config.agent_name.clone(),
        format!("yarli-orchestrator/{}", env!("CARGO_PKG_VERSION")),
    )
    .with_capabilities(sw4rm_config.capabilities.clone())
    .with_endpoints(sw4rm_sdk::Endpoints {
        registry: sw4rm_config.registry_url.clone(),
        router: sw4rm_config.router_url.clone(),
        scheduler: sw4rm_config.scheduler_url.clone(),
        ..sw4rm_sdk::Endpoints::default()
    });

    // Create shutdown bridge
    let shutdown = ShutdownController::new();
    shutdown.install_signal_handler();
    let bridge = ShutdownBridge::new(shutdown.clone());

    // Create agent
    let agent = YarliAgent::new(agent_config.clone(), sw4rm_config, orchestrator)
        .with_shutdown_bridge(bridge);

    println!("Connecting to sw4rm services...");

    // Boot the runtime
    let mut runtime = sw4rm_sdk::AgentRuntime::new(agent_config);
    runtime
        .init()
        .await
        .context("failed to initialize sw4rm runtime")?;
    runtime
        .register()
        .await
        .context("failed to register with sw4rm registry")?;

    println!("Agent registered. Entering message loop...");

    runtime
        .run(agent)
        .await
        .map_err(|e| anyhow::anyhow!("sw4rm agent runtime error: {e}"))?;

    Ok(())
}

pub(crate) async fn seed_postgres_run_state_if_needed(
    loaded_config: &LoadedConfig,
    run: &Run,
    tasks: &[Task],
) -> Result<()> {
    let BackendSelection::Postgres { database_url } = loaded_config.backend_selection()? else {
        return Ok(());
    };

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .with_context(|| {
            format!(
                "failed to connect to postgres for run/task seeding at {}",
                loaded_config.path().display()
            )
        })?;

    let mut tx = pool
        .begin()
        .await
        .context("failed to begin run/task seed transaction")?;

    sqlx::query(
        r#"
        INSERT INTO runs (
            run_id, objective, state, safe_mode, exit_reason, correlation_id, config_snapshot, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (run_id) DO NOTHING
        "#,
    )
    .bind(run.id)
    .bind(&run.objective)
    .bind(run_state_db(run.state))
    .bind(safe_mode_db(run.safe_mode))
    .bind(run.exit_reason.map(exit_reason_db))
    .bind(run.correlation_id)
    .bind(&run.config_snapshot)
    .bind(run.created_at)
    .bind(run.updated_at)
    .execute(tx.as_mut())
    .await
    .context("failed to seed runs table row for scheduler submission")?;

    for task in tasks {
        sqlx::query(
            r#"
            INSERT INTO tasks (
                task_id, run_id, task_key, description, state, command_class, attempt_no, max_attempts,
                blocker_code, correlation_id, priority, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(task.id)
        .bind(task.run_id)
        .bind(&task.task_key)
        .bind(&task.description)
        .bind(task_state_db(task.state))
        .bind(command_class_db(task.command_class))
        .bind(task.attempt_no as i32)
        .bind(task.max_attempts as i32)
        .bind(task.blocker.as_ref().map(task_blocker_db))
        .bind(task.correlation_id)
        .bind(task.priority as i32)
        .bind(task.created_at)
        .bind(task.updated_at)
        .execute(tx.as_mut())
        .await
        .with_context(|| format!("failed to seed task row {}", task.id))?;
    }

    tx.commit()
        .await
        .context("failed to commit run/task seed transaction")?;
    Ok(())
}

pub(crate) fn run_state_db(state: RunState) -> &'static str {
    match state {
        RunState::RunOpen => "RUN_OPEN",
        RunState::RunActive => "RUN_ACTIVE",
        RunState::RunBlocked => "RUN_BLOCKED",
        RunState::RunVerifying => "RUN_VERIFYING",
        RunState::RunCompleted => "RUN_COMPLETED",
        RunState::RunFailed => "RUN_FAILED",
        RunState::RunCancelled => "RUN_CANCELLED",
    }
}

pub(crate) fn task_state_db(state: TaskState) -> &'static str {
    match state {
        TaskState::TaskOpen => "TASK_OPEN",
        TaskState::TaskReady => "TASK_READY",
        TaskState::TaskExecuting => "TASK_EXECUTING",
        TaskState::TaskWaiting => "TASK_WAITING",
        TaskState::TaskBlocked => "TASK_BLOCKED",
        TaskState::TaskVerifying => "TASK_VERIFYING",
        TaskState::TaskComplete => "TASK_COMPLETE",
        TaskState::TaskFailed => "TASK_FAILED",
        TaskState::TaskCancelled => "TASK_CANCELLED",
    }
}

pub(crate) fn command_class_db(class: CommandClass) -> &'static str {
    match class {
        CommandClass::Io => "io",
        CommandClass::Cpu => "cpu",
        CommandClass::Git => "git",
        CommandClass::Tool => "tool",
    }
}

pub(crate) fn safe_mode_db(mode: SafeMode) -> &'static str {
    match mode {
        SafeMode::Observe => "observe",
        SafeMode::Execute => "execute",
        SafeMode::Restricted => "restricted",
        SafeMode::Breakglass => "breakglass",
    }
}

pub(crate) fn exit_reason_db(reason: yarli_core::domain::ExitReason) -> &'static str {
    match reason {
        yarli_core::domain::ExitReason::CompletedAllGates => "completed_all_gates",
        yarli_core::domain::ExitReason::BlockedOpenTasks => "blocked_open_tasks",
        yarli_core::domain::ExitReason::BlockedGateFailure => "blocked_gate_failure",
        yarli_core::domain::ExitReason::FailedPolicyDenial => "failed_policy_denial",
        yarli_core::domain::ExitReason::FailedRuntimeError => "failed_runtime_error",
        yarli_core::domain::ExitReason::MergeConflict => "merge_conflict",
        yarli_core::domain::ExitReason::CancelledByOperator => "cancelled_by_operator",
        yarli_core::domain::ExitReason::TimedOut => "timed_out",
        yarli_core::domain::ExitReason::StalledNoProgress => "stalled_no_progress",
    }
}

pub(crate) fn task_blocker_db(blocker: &yarli_core::entities::task::BlockerCode) -> String {
    match blocker {
        yarli_core::entities::task::BlockerCode::DependencyPending => "dependency_pending".into(),
        yarli_core::entities::task::BlockerCode::MergeConflict => "merge_conflict".into(),
        yarli_core::entities::task::BlockerCode::PolicyDenial => "policy_denial".into(),
        yarli_core::entities::task::BlockerCode::GateFailure => "gate_failure".into(),
        yarli_core::entities::task::BlockerCode::ManualHold => "manual_hold".into(),
        yarli_core::entities::task::BlockerCode::Custom(value) => value.clone(),
    }
}

#[derive(Debug, Default)]
struct PostgresSyncState {
    pool: Option<sqlx::PgPool>,
    last_signature: Option<String>,
}

#[derive(Debug)]
struct PostgresSyncSnapshot {
    run_state: &'static str,
    exit_reason: Option<&'static str>,
    task_states: Vec<(Uuid, &'static str)>,
}

async fn initialize_postgres_sync_state(
    loaded_config: Option<&LoadedConfig>,
) -> Result<PostgresSyncState> {
    let Some(loaded_config) = loaded_config else {
        return Ok(PostgresSyncState::default());
    };
    let BackendSelection::Postgres { database_url } = loaded_config.backend_selection()? else {
        return Ok(PostgresSyncState::default());
    };

    match PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
    {
        Ok(pool) => Ok(PostgresSyncState {
            pool: Some(pool),
            last_signature: None,
        }),
        Err(error) => {
            warn!(
                config_path = %loaded_config.path().display(),
                error = %error,
                "failed to initialize postgres sync pool; disabling in-loop projection sync"
            );
            Ok(PostgresSyncState::default())
        }
    }
}

async fn build_postgres_sync_signature<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    run_id: Uuid,
) -> Option<String>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let reg = scheduler.registry().read().await;
    let run = reg.get_run(&run_id)?;
    let run_state = run.state;
    let run_exit_reason = run.exit_reason;
    let mut task_states: Vec<(Uuid, TaskState)> = reg
        .tasks_for_run(&run_id)
        .iter()
        .map(|task| (task.id, task.state))
        .collect();
    drop(reg);

    task_states.sort_by(|left, right| left.0.cmp(&right.0));
    let mut signature = format!("run={run_state:?};exit={run_exit_reason:?};");
    for (task_id, state) in task_states {
        let _ = write!(signature, "{task_id}:{state:?};");
    }
    Some(signature)
}

async fn collect_postgres_sync_snapshot<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    run_id: Uuid,
) -> Option<PostgresSyncSnapshot>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let reg = scheduler.registry().read().await;
    let run = reg.get_run(&run_id)?;
    let run_state = run_state_db(run.state);
    let exit_reason = run.exit_reason.map(exit_reason_db);
    let task_states: Vec<(Uuid, &'static str)> = reg
        .tasks_for_run(&run_id)
        .iter()
        .map(|task| (task.id, task_state_db(task.state)))
        .collect();
    drop(reg);

    Some(PostgresSyncSnapshot {
        run_state,
        exit_reason,
        task_states,
    })
}

async fn sync_postgres_state_if_changed<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    run_id: Uuid,
    sync_state: &mut PostgresSyncState,
) -> Result<bool>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let Some(pool) = sync_state.pool.as_ref() else {
        return Ok(false);
    };
    let Some(signature) = build_postgres_sync_signature(scheduler, run_id).await else {
        return Ok(false);
    };
    if sync_state.last_signature.as_ref() == Some(&signature) {
        return Ok(false);
    }

    sync_postgres_state_with_pool(scheduler, pool, run_id).await?;
    sync_state.last_signature = Some(signature);
    Ok(true)
}

/// Sync the materialized run and task states in Postgres to match in-memory registry.
///
/// The `runs.state` and `tasks.state` columns are seeded once at INSERT and never updated,
/// leaving them stale (e.g. RUN_OPEN) while the event-sourced in-memory registry advances.
/// This function writes the current in-memory state back to Postgres for observability.
pub(crate) async fn sync_postgres_state<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    loaded_config: &LoadedConfig,
    run_id: Uuid,
) -> Result<()>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let BackendSelection::Postgres { database_url } = loaded_config.backend_selection()? else {
        return Ok(());
    };

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .context("sync_postgres_state: failed to connect")?;

    sync_postgres_state_with_pool(scheduler, &pool, run_id).await
}

async fn sync_postgres_state_with_pool<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    pool: &sqlx::PgPool,
    run_id: Uuid,
) -> Result<()>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let Some(snapshot) = collect_postgres_sync_snapshot(scheduler, run_id).await else {
        return Ok(());
    };

    let mut tx = pool
        .begin()
        .await
        .context("sync_postgres_state: begin tx")?;

    sqlx::query(
        r#"
        UPDATE runs
        SET state = $1,
            exit_reason = $2,
            updated_at = now()
        WHERE run_id = $3
        "#,
    )
    .bind(snapshot.run_state)
    .bind(snapshot.exit_reason)
    .bind(run_id)
    .execute(tx.as_mut())
    .await
    .context("sync_postgres_state: update runs")?;

    for (task_id, state) in &snapshot.task_states {
        sqlx::query(
            r#"
            UPDATE tasks
            SET state = $1,
                updated_at = now()
            WHERE task_id = $2
            "#,
        )
        .bind(*state)
        .bind(*task_id)
        .execute(tx.as_mut())
        .await
        .with_context(|| format!("sync_postgres_state: update task {task_id}"))?;
    }

    tx.commit().await.context("sync_postgres_state: commit")?;

    debug!(
        run_id = %run_id,
        run_state = snapshot.run_state,
        tasks = snapshot.task_states.len(),
        "synced postgres state"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct MigrationDefinition {
    version: i32,
    name: &'static str,
    up_sql: &'static str,
    down_sql: &'static str,
}

const MIGRATION_DATA_TABLES: &[&str] = &[
    "runs",
    "tasks",
    "task_dependencies",
    "worktrees",
    "merge_intents",
    "commands",
    "command_stream_chunks",
    "events",
    "evidence",
    "gates",
    "gate_results",
    "policy_decisions",
    "leases",
    "task_queue",
    "yarli_schema_migrations",
];

const MIGRATION_TABLE_NAME: &str = "yarli_schema_migrations";
const MIGRATION_BACKUP_SCHEMA: &str = "yarli_migration_backups";
const MIGRATION_LOCK_KEY: i64 = 6_021_202_602;

const MIGRATIONS: [MigrationDefinition; 2] = [
    MigrationDefinition {
        version: 1,
        name: "0001_init",
        up_sql: MIGRATION_0001_INIT,
        down_sql: MIGRATION_0001_DOWN,
    },
    MigrationDefinition {
        version: 2,
        name: "0002_indexes",
        up_sql: MIGRATION_0002_INDEXES,
        down_sql: MIGRATION_0002_DOWN,
    },
];

pub(crate) async fn cmd_migrate_status(loaded_config: &LoadedConfig) -> Result<()> {
    let database_url = postgres_database_url(loaded_config, "status")?;
    let pool =
        connect_postgres_for_migration(&database_url, "yarli migrate status failed to connect")
            .await?;
    ensure_migration_metadata(&pool).await?;

    let applied = load_applied_migrations(&pool).await?;
    let unknown_count = applied
        .iter()
        .filter(|migration| find_migration_definition(migration.version).is_none())
        .count();

    let mut lines = String::new();
    lines.push_str("Migration status:\n");
    lines.push_str("version | name          | state   | applied\n");
    lines.push_str("--------+---------------+---------+-------------------------------\n");

    for migration in MIGRATIONS.iter() {
        let status = applied
            .iter()
            .find(|applied| applied.version == migration.version)
            .map(|applied| format!("applied @ {}", applied.applied_at))
            .unwrap_or_else(|| "pending".into());
        lines.push_str(&format!(
            "{:<7} | {:<13} | {:<7} | {}\n",
            migration.version,
            migration.name,
            if status.starts_with("applied") {
                "ok"
            } else {
                "pending"
            },
            status
        ));
    }

    if unknown_count > 0 {
        lines.push_str(&format!(
            "\nwarning: {unknown_count} migrations are recorded in {MIGRATION_TABLE_NAME} but not known to this binary\n"
        ));
    }

    if lines.ends_with('\n') {
        lines.pop();
    }
    println!("{lines}");
    Ok(())
}

pub(crate) async fn cmd_migrate_up(
    loaded_config: &LoadedConfig,
    target: Option<&str>,
) -> Result<()> {
    let target_version = parse_migration_target(target, false)?;
    let target_version =
        target_version.unwrap_or_else(|| MIGRATIONS.last().expect("known migrations").version);
    let database_url = postgres_database_url(loaded_config, "up")?;
    let pool =
        connect_postgres_for_migration(&database_url, "yarli migrate up failed to connect").await?;
    let mut tx = pool
        .begin()
        .await
        .context("failed to start migration transaction")?;

    ensure_migration_metadata_tx(&mut tx).await?;
    acquire_migration_lock(&mut tx).await?;

    let applied = load_applied_migrations_tx(&mut tx).await?;
    let applied_versions: Vec<i32> = applied.iter().map(|row| row.version).collect();
    let pending: Vec<&MigrationDefinition> = MIGRATIONS
        .iter()
        .filter(|definition| {
            definition.version <= target_version && !applied_versions.contains(&definition.version)
        })
        .collect();

    if pending.is_empty() {
        println!("No pending migrations to apply.");
        return Ok(());
    }

    for migration in pending {
        execute_migration_sql(&mut tx, migration.version, migration.name, migration.up_sql).await?;
        sqlx::query(
            "INSERT INTO yarli_schema_migrations (version, name) VALUES ($1, $2) \
             ON CONFLICT (version) DO UPDATE SET name = EXCLUDED.name, applied_at = now()",
        )
        .bind(migration.version)
        .bind(migration.name)
        .execute(tx.as_mut())
        .await
        .context("failed to record applied migration metadata")?;
    }

    tx.commit()
        .await
        .context("failed to commit migration transaction")?;

    println!(
        "Applied migration(s) up to {}.",
        migration_name(target_version)
    );
    Ok(())
}

pub(crate) async fn cmd_migrate_down(
    loaded_config: &LoadedConfig,
    target: Option<&str>,
    backup_label: Option<&str>,
) -> Result<()> {
    let target_version = parse_migration_target(target, true)?;
    let backup_label = sanitize_backup_label(backup_label.unwrap_or(""));

    let database_url = postgres_database_url(loaded_config, "down")?;
    let pool =
        connect_postgres_for_migration(&database_url, "yarli migrate down failed to connect")
            .await?;

    let mut tx = pool
        .begin()
        .await
        .context("failed to start migration transaction")?;
    ensure_migration_metadata_tx(&mut tx).await?;
    acquire_migration_lock(&mut tx).await?;

    let applied = load_applied_migrations_tx(&mut tx).await?;
    let current_version = applied.iter().map(|row| row.version).max().unwrap_or(0);
    let target_version = target_version.unwrap_or_else(|| current_version.saturating_sub(1));

    if current_version == 0 {
        println!("No migrations are applied.");
        return Ok(());
    }

    if target_version >= current_version {
        println!(
            "Already at or above target version {}.",
            migration_name(target_version)
        );
        return Ok(());
    }

    create_migration_backup(&mut tx, &backup_label, current_version).await?;
    let rollback_plan: Vec<&MigrationDefinition> = MIGRATIONS
        .iter()
        .filter(|definition| {
            definition.version > target_version && definition.version <= current_version
        })
        .rev()
        .collect();

    if rollback_plan.is_empty() {
        println!(
            "No migrations to rollback for target {}.",
            migration_name(target_version)
        );
        return Ok(());
    }

    for migration in rollback_plan {
        execute_migration_sql(
            &mut tx,
            migration.version,
            migration.name,
            migration.down_sql,
        )
        .await?;
        sqlx::query("DELETE FROM yarli_schema_migrations WHERE version = $1")
            .bind(migration.version)
            .execute(tx.as_mut())
            .await
            .with_context(|| {
                format!("failed to remove migration metadata {}", migration.version)
            })?;
    }

    tx.commit()
        .await
        .context("failed to commit rollback migration transaction")?;

    println!(
        "Rolled back migration(s) down to {}. Backup captured as {}.",
        migration_name(target_version),
        backup_label
    );
    Ok(())
}

pub(crate) async fn cmd_migrate_backup(
    loaded_config: &LoadedConfig,
    label: Option<&str>,
) -> Result<()> {
    let database_url = postgres_database_url(loaded_config, "backup")?;
    let pool =
        connect_postgres_for_migration(&database_url, "yarli migrate backup failed to connect")
            .await?;

    let mut tx = pool
        .begin()
        .await
        .context("failed to start migration transaction")?;
    ensure_migration_metadata_tx(&mut tx).await?;
    acquire_migration_lock(&mut tx).await?;

    let applied = load_applied_migrations_tx(&mut tx).await?;
    let current_version = applied.iter().map(|row| row.version).max().unwrap_or(0);
    let label = sanitize_backup_label(label.unwrap_or(""));
    create_migration_backup(&mut tx, &label, current_version).await?;

    tx.commit()
        .await
        .context("failed to commit migration backup transaction")?;

    println!("Created migration backup snapshot '{label}'.");
    Ok(())
}

pub(crate) async fn cmd_migrate_restore(loaded_config: &LoadedConfig, label: &str) -> Result<()> {
    let label = sanitize_backup_label(label);
    let database_url = postgres_database_url(loaded_config, "restore")?;
    let pool =
        connect_postgres_for_migration(&database_url, "yarli migrate restore failed to connect")
            .await?;

    let mut tx = pool
        .begin()
        .await
        .context("failed to start migration transaction")?;
    ensure_migration_metadata_tx(&mut tx).await?;
    acquire_migration_lock(&mut tx).await?;

    verify_backup_label(&mut tx, &label).await?;
    restore_from_migration_backup(&mut tx, &label).await?;

    tx.commit()
        .await
        .context("failed to commit migration restore transaction")?;

    println!("Restored migration snapshot '{label}'.");
    Ok(())
}

fn migration_name(version: i32) -> String {
    if let Some(definition) = find_migration_definition(version) {
        definition.name.to_string()
    } else if version == 0 {
        "clean".to_string()
    } else {
        format!("unknown ({version})")
    }
}

fn find_migration_definition(version: i32) -> Option<&'static MigrationDefinition> {
    MIGRATIONS.iter().find(|entry| entry.version == version)
}

fn parse_migration_target(value: Option<&str>, allow_zero: bool) -> Result<Option<i32>> {
    let Some(raw) = value else {
        return Ok(None);
    };

    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }

    let normalized = raw.split('_').next().unwrap_or("").trim_start_matches("v");
    let version = normalized
        .parse::<i32>()
        .with_context(|| format!("invalid migration target '{raw}'"))?;

    if version == 0 && allow_zero {
        return Ok(Some(0));
    }

    if version <= 0 || find_migration_definition(version).is_none() {
        let known = MIGRATIONS
            .iter()
            .map(|definition| definition.name)
            .collect::<Vec<_>>()
            .join(", ");
        bail!("unknown migration target '{raw}'; expected one of: {known}");
    }

    Ok(Some(version))
}

fn postgres_database_url(loaded_config: &LoadedConfig, command_name: &str) -> Result<String> {
    let BackendSelection::Postgres { database_url } = loaded_config.backend_selection()? else {
        bail!("`migrate {command_name}` requires core.backend = \"postgres\"");
    };
    Ok(database_url)
}

#[derive(Debug)]
struct AppliedMigration {
    version: i32,
    applied_at: chrono::DateTime<chrono::Utc>,
}

async fn load_applied_migrations(pool: &sqlx::PgPool) -> Result<Vec<AppliedMigration>> {
    sqlx::query_as::<_, (i32, String, chrono::DateTime<chrono::Utc>)>(
        "SELECT version, name, applied_at FROM yarli_schema_migrations ORDER BY version ASC",
    )
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|(version, _, applied_at)| AppliedMigration {
                version,
                applied_at,
            })
            .collect()
    })
    .context("failed to read migration metadata")
}

async fn load_applied_migrations_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<AppliedMigration>> {
    sqlx::query_as::<_, (i32, String, chrono::DateTime<chrono::Utc>)>(
        "SELECT version, name, applied_at FROM yarli_schema_migrations ORDER BY version ASC",
    )
    .fetch_all(tx.as_mut())
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|(version, _, applied_at)| AppliedMigration {
                version,
                applied_at,
            })
            .collect()
    })
    .context("failed to read migration metadata")
}

async fn ensure_migration_metadata(pool: &sqlx::PgPool) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .context("failed to start metadata transaction")?;
    ensure_migration_metadata_tx(&mut tx).await?;
    tx.commit()
        .await
        .context("failed to commit migration metadata initialization")?;
    Ok(())
}

async fn ensure_migration_metadata_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS yarli_schema_migrations (
            version integer PRIMARY KEY,
            name text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now()
        )",
    )
    .execute(tx.as_mut())
    .await
    .context("failed to ensure migration metadata table")?;

    sqlx::query("CREATE SCHEMA IF NOT EXISTS yarli_migration_backups")
        .execute(tx.as_mut())
        .await
        .context("failed to ensure migration backup schema")?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS yarli_migration_backups.labels (
            label text PRIMARY KEY,
            source_version integer NOT NULL,
            source_version_name text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )",
    )
    .execute(tx.as_mut())
    .await
    .context("failed to ensure migration backup label metadata table")?;

    Ok(())
}

async fn acquire_migration_lock(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<()> {
    let (acquired,) = sqlx::query_as::<_, (bool,)>("SELECT pg_try_advisory_xact_lock($1)")
        .bind(MIGRATION_LOCK_KEY)
        .fetch_one(tx.as_mut())
        .await
        .context("failed to acquire migration lock")?;

    if !acquired {
        bail!("another migration operation is already in progress");
    }

    Ok(())
}

async fn execute_migration_sql(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    version: i32,
    migration_name: &str,
    sql: &str,
) -> Result<()> {
    for statement in split_sql_statements(sql) {
        sqlx::query(statement)
            .execute(tx.as_mut())
            .await
            .with_context(|| {
                format!("failed to apply migration {version} ({migration_name}) statement")
            })?;
    }
    Ok(())
}

fn split_sql_statements(sql: &str) -> Vec<&str> {
    sql.split(';')
        .map(|statement| statement.trim())
        .filter(|statement| !statement.is_empty())
        .collect()
}

async fn create_migration_backup(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    label: &str,
    source_version: i32,
) -> Result<()> {
    let source_version_name = migration_name(source_version);

    let source_version_row = load_single_row(
        "SELECT COUNT(*)::bigint FROM yarli_migration_backups.labels WHERE label = $1",
        label,
        tx,
    )
    .await?;
    if source_version_row > 0 {
        bail!("backup label '{label}' already exists");
    }

    sqlx::query(
        "INSERT INTO yarli_migration_backups.labels (label, source_version, source_version_name)
         VALUES ($1, $2, $3)",
    )
    .bind(label)
    .bind(source_version)
    .bind(source_version_name)
    .execute(tx.as_mut())
    .await
    .context("failed to register migration backup metadata")?;

    for table in MIGRATION_DATA_TABLES {
        let backup_table = format!(
            "{}.{}",
            quote_identifier(MIGRATION_BACKUP_SCHEMA),
            quote_identifier(&format!("{label}_{}", table))
        );
        let create_statement = format!(
            "CREATE TABLE IF NOT EXISTS {backup_table} AS TABLE {}",
            quote_identifier(table)
        );
        sqlx::query(&create_statement)
            .execute(tx.as_mut())
            .await
            .with_context(|| format!("failed to back up table {table}"))?;
    }

    Ok(())
}

async fn verify_backup_label(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    label: &str,
) -> Result<()> {
    let exists = load_scalar_bool(
        "SELECT EXISTS(SELECT 1 FROM yarli_migration_backups.labels WHERE label = $1)",
        label,
        tx,
    )
    .await?;
    if !exists {
        bail!("backup label '{label}' not found");
    }

    Ok(())
}

async fn restore_from_migration_backup(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    label: &str,
) -> Result<()> {
    let mut restore_list = String::new();
    for (index, table) in MIGRATION_DATA_TABLES.iter().enumerate() {
        let backup_table_name = quote_identifier(&format!("{label}_{}", table));
        let qualified = format!("{MIGRATION_BACKUP_SCHEMA}.{backup_table_name}");
        let backup_exists =
            load_scalar_bool("SELECT to_regclass($1)::text IS NOT NULL", &qualified, tx).await?;
        if !backup_exists {
            bail!("backup table for {table} is missing in snapshot '{label}'");
        }

        if index > 0 {
            restore_list.push_str(", ");
        }
        restore_list.push_str(&quote_identifier(table));
    }

    sqlx::query(&format!("TRUNCATE TABLE {restore_list};"))
        .execute(tx.as_mut())
        .await
        .context("failed to clear tables before restore")?;

    for table in MIGRATION_DATA_TABLES.iter().rev() {
        let backup_table = quote_identifier(&format!("{label}_{}", table));
        let qualified_source = format!(
            "{}.{}",
            quote_identifier(MIGRATION_BACKUP_SCHEMA),
            backup_table
        );
        let statement = format!(
            "INSERT INTO {} SELECT * FROM {}",
            quote_identifier(table),
            qualified_source
        );
        sqlx::query(&statement)
            .execute(tx.as_mut())
            .await
            .with_context(|| format!("failed to restore table {table} from snapshot '{label}'"))?;
    }

    Ok(())
}

async fn connect_postgres_for_migration(database_url: &str, context: &str) -> Result<sqlx::PgPool> {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await
        .with_context(|| format!("{context}: {database_url}"))
}

async fn load_scalar_bool(
    query: &str,
    value: &str,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<bool> {
    let (result,) = sqlx::query_as::<_, (bool,)>(query)
        .bind(value)
        .fetch_one(tx.as_mut())
        .await
        .context("failed to evaluate migration backup metadata query")?;
    Ok(result)
}

async fn load_single_row(
    query: &str,
    value: &str,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<i64> {
    let (value,) = sqlx::query_as::<_, (i64,)>(query)
        .bind(value)
        .fetch_one(tx.as_mut())
        .await
        .context("failed to inspect migration backup metadata")?;
    Ok(value)
}

fn sanitize_backup_label(label: &str) -> String {
    let filtered = label
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .collect::<String>()
        .to_lowercase();

    let trimmed = filtered.trim_matches('_');
    let mut result = if trimmed.is_empty() {
        chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string()
    } else {
        trimmed.to_string()
    };

    if result
        .chars()
        .next()
        .is_some_and(|value| value.is_ascii_digit())
    {
        result.insert(0, 'b');
    }

    result
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_run_config_snapshot(
    loaded_config: &LoadedConfig,
    working_dir: &str,
    timeout_secs: u64,
    tasks: &[PlannedTask],
    task_catalog: &[PlannedTask],
    pace: Option<&str>,
    prompt_snapshot: Option<&yarli_cli::prompt::PromptSnapshot>,
    run_spec: Option<&yarli_cli::prompt::RunSpec>,
    tranche_plan: &[PlannedTranche],
    current_tranche_index: Option<usize>,
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "config_source": loaded_config.source().label(),
        "config_path": loaded_config.path().display().to_string(),
        "backend": loaded_config.config().core.backend.as_str(),
        "config": loaded_config.snapshot()?,
        "runtime": {
            "pace": pace,
            "working_dir": working_dir,
            "timeout_secs": timeout_secs,
            "task_count": tasks.len(),
            "tasks": tasks.iter().map(|t| serde_json::json!({
                "task_key": &t.task_key,
                "command": &t.command,
                "command_class": format!("{:?}", t.command_class),
                "tranche_key": &t.tranche_key,
                "tranche_group": &t.tranche_group,
                "depends_on": &t.depends_on,
                "allowed_paths": &t.allowed_paths,
            })).collect::<Vec<_>>(),
            "task_catalog": task_catalog.iter().map(|t| serde_json::json!({
                "task_key": &t.task_key,
                "command": &t.command,
                "command_class": format!("{:?}", t.command_class),
                "tranche_key": &t.tranche_key,
                "tranche_group": &t.tranche_group,
                "depends_on": &t.depends_on,
                "allowed_paths": &t.allowed_paths,
            })).collect::<Vec<_>>(),
            "tranche_plan": tranche_plan.iter().map(|t| serde_json::json!({
                "key": &t.key,
                "objective": &t.objective,
                "task_keys": &t.task_keys,
                "tranche_group": &t.tranche_group,
            })).collect::<Vec<_>>(),
            "current_tranche_index": current_tranche_index,
            "prompt": prompt_snapshot,
            "run_spec": run_spec,
        },
    }))
}

pub(crate) fn compute_quality_gate(
    report: Option<&DeteriorationReport>,
    auto_advance_policy: AutoAdvancePolicy,
    task_health: config::RunTaskHealthConfig,
    run_total_tokens: u64,
    max_run_total_tokens: Option<u64>,
    soft_token_cap_ratio: f64,
) -> yarli_core::entities::continuation::ContinuationQualityGate {
    let soft_cap_triggered = match max_run_total_tokens {
        Some(max_tokens) if max_tokens > 0 => {
            soft_token_cap_ratio.is_finite()
                && soft_token_cap_ratio > 0.0
                && (run_total_tokens as f64) >= (max_tokens as f64 * soft_token_cap_ratio)
        }
        _ => false,
    };

    let mut task_health_action = match report {
        Some(report) => match report.trend {
            DeteriorationTrend::Improving => task_health.improving,
            DeteriorationTrend::Stable => task_health.stable,
            DeteriorationTrend::Deteriorating => task_health.deteriorating,
        },
        None => task_health.improving,
    };
    if soft_cap_triggered {
        task_health_action = TaskHealthAction::CheckpointNow;
    }

    let allow_auto_advance = if soft_cap_triggered {
        false
    } else if auto_advance_policy == AutoAdvancePolicy::Always {
        true
    } else {
        match (report, task_health_action) {
            (Some(report), TaskHealthAction::Continue) => match report.trend {
                DeteriorationTrend::Improving => true,
                DeteriorationTrend::Stable => auto_advance_policy == AutoAdvancePolicy::StableOk,
                DeteriorationTrend::Deteriorating => false,
            },
            (None, TaskHealthAction::Continue) => false,
            _ => false,
        }
    };

    let reason = match (report, task_health_action) {
        (_, TaskHealthAction::CheckpointNow) if soft_cap_triggered => {
            format!(
                "soft token cap reached at {} / {} ({}% hard cap)",
                run_total_tokens,
                max_run_total_tokens.unwrap_or(0),
                (soft_token_cap_ratio * 100.0).round()
            )
        }
        (Some(report), TaskHealthAction::Continue) => match report.trend {
            DeteriorationTrend::Improving => "deterioration trend improving".to_string(),
            DeteriorationTrend::Stable if auto_advance_policy == AutoAdvancePolicy::StableOk => {
                "deterioration trend stable (policy allows auto-advance)".to_string()
            }
            DeteriorationTrend::Stable => {
                "deterioration trend stable (stagnation blocked)".to_string()
            }
            DeteriorationTrend::Deteriorating
                if auto_advance_policy == AutoAdvancePolicy::Always =>
            {
                "deterioration trend deteriorating (always policy overrides gate)".to_string()
            }
            DeteriorationTrend::Deteriorating => "deterioration trend deteriorating".to_string(),
        },
        (Some(_), _) => "deterioration trend blocked by task-health action".to_string(),
        (None, TaskHealthAction::Continue) => {
            if auto_advance_policy == AutoAdvancePolicy::Always {
                "no deterioration signal emitted (always policy overrides gate)".to_string()
            } else {
                "no deterioration signal emitted".to_string()
            }
        }
        (None, _) => "no deterioration signal emitted".to_string(),
    };

    let (trend, score) = report
        .map(|report| (Some(report.trend), Some(report.score)))
        .unwrap_or_default();

    yarli_core::entities::continuation::ContinuationQualityGate {
        allow_auto_advance,
        reason,
        trend,
        score,
        task_health_action,
    }
}

pub(crate) fn persist_continuation_payload_event(
    store: &dyn EventStore,
    payload: &yarli_core::entities::ContinuationPayload,
    correlation_id: Uuid,
) -> Result<()> {
    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Run,
            entity_id: payload.run_id.to_string(),
            event_type: RUN_CONTINUATION_EVENT_TYPE.to_string(),
            payload: serde_json::json!({
                "continuation_payload": payload,
            }),
            correlation_id,
            causation_id: None,
            actor: "yarli-cli".to_string(),
            idempotency_key: Some(format!("{}:continuation_payload", payload.run_id)),
        },
    )
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) fn build_continuation_payload(
    run: &Run,
    tasks: &[&yarli_core::entities::Task],
    report: Option<&DeteriorationReport>,
    auto_advance_policy: AutoAdvancePolicy,
    task_health: config::RunTaskHealthConfig,
    run_total_tokens: u64,
    max_run_total_tokens: Option<u64>,
    soft_token_cap_ratio: f64,
    deterioration_cycle_detected: bool,
) -> yarli_core::entities::ContinuationPayload {
    build_continuation_payload_with_gate_failures(
        run,
        tasks,
        report,
        auto_advance_policy,
        task_health,
        run_total_tokens,
        max_run_total_tokens,
        soft_token_cap_ratio,
        deterioration_cycle_detected,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_continuation_payload_with_gate_failures(
    run: &Run,
    tasks: &[&yarli_core::entities::Task],
    report: Option<&DeteriorationReport>,
    auto_advance_policy: AutoAdvancePolicy,
    task_health: config::RunTaskHealthConfig,
    run_total_tokens: u64,
    max_run_total_tokens: Option<u64>,
    soft_token_cap_ratio: f64,
    deterioration_cycle_detected: bool,
    gate_failures: &[String],
) -> yarli_core::entities::ContinuationPayload {
    let mut payload = yarli_core::entities::ContinuationPayload::build(run, tasks);
    let mut quality_gate = compute_quality_gate(
        report,
        auto_advance_policy,
        task_health,
        run_total_tokens,
        max_run_total_tokens,
        soft_token_cap_ratio,
    );
    let had_strategy_pivot_checkpoint = tasks
        .iter()
        .any(|task| task.description.contains("Strategy-pivot checkpoint:"));
    if quality_gate.task_health_action == TaskHealthAction::ForcePivot
        && deterioration_cycle_detected
        && had_strategy_pivot_checkpoint
    {
        quality_gate = yarli_core::entities::continuation::ContinuationQualityGate {
            allow_auto_advance: false,
            reason: "deterioration cycle persisted after strategy pivot; summarize and schedule follow-up tranche".to_string(),
            trend: quality_gate.trend,
            score: quality_gate.score,
            task_health_action: TaskHealthAction::StopAndSummarize,
        };
    }
    if quality_gate.task_health_action == TaskHealthAction::ForcePivot {
        if let Some(next_tranche) = payload.next_tranche.as_mut() {
            let already_recorded = next_tranche.interventions.iter().any(|intervention| {
                intervention.kind == ContinuationInterventionKind::StrategyPivotCheckpoint
            });
            if !already_recorded {
                next_tranche.interventions.push(ContinuationIntervention {
                    kind: ContinuationInterventionKind::StrategyPivotCheckpoint,
                    reason: format!(
                        "strategy-pivot checkpoint requested before continuing: {}",
                        quality_gate.reason
                    ),
                });
            }
        }
    }
    payload.quality_gate = Some(quality_gate);
    payload.retry_recommendation = Some(analyze_retry_recommendation(run, tasks, gate_failures));
    payload
}

fn analyze_retry_recommendation(
    run: &Run,
    tasks: &[&yarli_core::entities::Task],
    gate_failures: &[String],
) -> RetryScope {
    let analyzer_tasks: Vec<run_analyzer::TaskOutcome> = tasks
        .iter()
        .map(|task| run_analyzer::TaskOutcome {
            task_key: task.task_key.clone(),
            state: task.state,
            last_error: task.last_error.clone(),
            blocker: task.blocker.clone(),
        })
        .collect();
    let analysis = run_analyzer::analyze_run(
        run.state,
        run.exit_reason,
        &analyzer_tasks,
        gate_failures,
    );
    match analysis.retry_recommendation {
        run_analyzer::RetryScope::Full => RetryScope::Full,
        run_analyzer::RetryScope::Subset { keys } => RetryScope::Subset { keys },
        run_analyzer::RetryScope::None => RetryScope::None,
    }
}

fn extract_gate_failure_names_for_run(store: &dyn EventStore, run_id: Uuid) -> Vec<String> {
    let Ok(Some(run_projection)) = load_run_projection(store, run_id) else {
        return Vec::new();
    };

    let mut names = run_projection
        .failed_gates
        .iter()
        .map(|(gate_type, _)| {
            gate_type
                .label()
                .trim_start_matches("gate.")
                .to_string()
        })
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
}

pub(crate) fn cancellation_reason_for_source(source: CancellationSource) -> &'static str {
    match source {
        CancellationSource::Operator => "cancelled by operator",
        CancellationSource::Sigint => "cancelled by SIGINT",
        CancellationSource::Sigterm => "cancelled by SIGTERM",
        CancellationSource::Sw4rmPreemption => "cancelled by sw4rm preemption",
        CancellationSource::Unknown => "cancelled by shutdown token",
    }
}

pub(crate) fn actor_kind_for_source(source: CancellationSource) -> CancellationActorKind {
    match source {
        CancellationSource::Operator | CancellationSource::Sigint => {
            CancellationActorKind::Operator
        }
        CancellationSource::Sw4rmPreemption => CancellationActorKind::Supervisor,
        CancellationSource::Sigterm => CancellationActorKind::System,
        CancellationSource::Unknown => CancellationActorKind::Unknown,
    }
}

pub(crate) fn default_cancellation_provenance(
    source: CancellationSource,
    actor_detail: Option<String>,
) -> CancellationProvenance {
    CancellationProvenance {
        cancellation_source: source,
        signal_name: None,
        signal_number: None,
        sender_pid: None,
        receiver_pid: Some(std::process::id()),
        parent_pid: None,
        process_group_id: None,
        session_id: None,
        tty: None,
        actor_kind: Some(actor_kind_for_source(source)),
        actor_detail,
        stage: Some(CancellationStage::Unknown),
    }
}

pub(crate) fn cancellation_stage_for_task(task: &Task) -> CancellationStage {
    match task.state {
        TaskState::TaskExecuting => CancellationStage::Executing,
        TaskState::TaskVerifying => CancellationStage::Verifying,
        TaskState::TaskReady if task.attempt_no > 1 => CancellationStage::Retrying,
        _ => CancellationStage::Unknown,
    }
}

pub(crate) fn format_cancel_provenance_summary(
    provenance: Option<&CancellationProvenance>,
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

pub(crate) fn with_cancellation_diagnostics(
    mut provenance: CancellationProvenance,
    run_id: Uuid,
    correlation_id: Uuid,
    tick_count: u64,
    note: &str,
) -> CancellationProvenance {
    let diagnostic = format!(
        "diag(run_id={run_id}, correlation_id={correlation_id}, tick={tick_count}, note={note})"
    );
    provenance.actor_detail = Some(match provenance.actor_detail {
        Some(existing) => format!("{existing}; {diagnostic}"),
        None => diagnostic,
    });
    provenance
}

fn emit_task_health_transition_audits(
    task_health_controllers: &mut std::collections::HashMap<
        Uuid,
        yarli_exec::introspect::IntrospectionController,
    >,
    events: &[Event],
    run_id: Uuid,
    audit_sink: Option<&dyn yarli_observability::AuditSink>,
) {
    for event in events {
        if event.event_type != "run.observer.task_health" {
            continue;
        }

        let Some(task_id) = event
            .payload
            .get("task_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            continue;
        };

        let Some(report) = task_health_report_from_payload(&event.payload) else {
            continue;
        };
        let Some(controller) = task_health_controllers.get_mut(&task_id) else {
            continue;
        };
        if !controller.set_health_report(report) {
            warn!(
                task_id = %task_id,
                run_id = %run_id,
                "failed to update task health report"
            );
            continue;
        }

        if let Some(transition) = controller.check_health_transition() {
            let Some(sink) = audit_sink else {
                continue;
            };
            let factors = serde_json::to_value(transition.factors)
                .unwrap_or_else(|_| serde_json::json!({}));
            let entry = AuditEntry::process_health_transition(
                transition.previous_level.to_string(),
                transition.new_level.to_string(),
                transition.score,
                factors,
                Some(run_id),
                Some(task_id),
            );
            if let Err(err) = sink.append(&entry) {
                warn!(
                    task_id = %task_id,
                    run_id = %run_id,
                    error = %err,
                    "failed to append process health transition audit entry"
                );
            }
        }
    }
}

fn task_health_report_from_payload(
    payload: &serde_json::Value,
) -> Option<yarli_exec::introspect::HealthReport> {
    let score = payload
        .get("score")
        .and_then(serde_json::Value::as_f64)
        .or_else(|| {
            payload
                .get("outcome")
                .and_then(|value| value.get("score"))
                .and_then(serde_json::Value::as_f64)
        })
        .filter(|value| value.is_finite());

    let status = payload
        .get("status")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            payload
                .get("outcome")
                .and_then(|value| value.get("status"))
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            payload
                .get("outcome")
                .and_then(|value| value.get("result"))
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or("");

    let level = task_health_level_from_status(status, score)?;

    Some(yarli_exec::introspect::HealthReport {
        score: score.unwrap_or(match level {
            yarli_exec::introspect::HealthLevel::Healthy => 1.0,
            yarli_exec::introspect::HealthLevel::Degraded => 0.5,
            yarli_exec::introspect::HealthLevel::Stuck => 0.1,
        }),
        level,
        factors: task_health_factors_from_payload(payload),
    })
}

fn task_health_level_from_status(
    status: &str,
    score: Option<f64>,
) -> Option<yarli_exec::introspect::HealthLevel> {
    if let Some(score) = score {
        return Some(yarli_exec::introspect::HealthLevel::from_score(score));
    }

    let normalized = status.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized.contains("healthy")
        || normalized.contains("pass")
        || normalized.contains("ok")
        || normalized.contains("improving")
        || normalized.contains("continue")
    {
        Some(yarli_exec::introspect::HealthLevel::Healthy)
    } else if normalized.contains("degraded") || normalized.contains("deteriorating") {
        Some(yarli_exec::introspect::HealthLevel::Degraded)
    } else if normalized.contains("stuck")
        || normalized.contains("error")
        || normalized.contains("fail")
    {
        Some(yarli_exec::introspect::HealthLevel::Stuck)
    } else {
        None
    }
}

fn task_health_factors_from_payload(
    payload: &serde_json::Value,
) -> yarli_exec::introspect::HealthFactors {
    let factors = payload
        .get("factors")
        .or_else(|| payload.get("outcome").and_then(|value| value.get("factors")));

    let factor = |key: &str| -> f64 {
        factors
            .and_then(|value| value.get(key))
            .and_then(serde_json::Value::as_f64)
            .filter(|value| value.is_finite())
            .unwrap_or(1.0)
    };

    yarli_exec::introspect::HealthFactors {
        output_recency: factor("output_recency"),
        io_delta: factor("io_delta"),
        cpu_utilization: factor("cpu_utilization"),
        repetition: factor("repetition"),
        sleep_ratio: factor("sleep_ratio"),
    }
}

/// Drive the scheduler, emitting StreamEvents to the renderer channel.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drive_scheduler<Q, S, R>(
    scheduler: &mut Scheduler<Q, S, R>,
    store: &Arc<S>,
    shutdown: ShutdownController,
    cancel: CancellationToken,
    tx: mpsc::UnboundedSender<StreamEvent>,
    run_id: Uuid,
    correlation_id: Uuid,
    task_names: &[(Uuid, String)],
    auto_advance_policy: AutoAdvancePolicy,
    task_health: config::RunTaskHealthConfig,
    max_run_total_tokens: Option<u64>,
    soft_token_cap_ratio: f64,
    cancellation_diagnostics: bool,
    mut task_health_observer: Option<observers::TaskHealthArtifactObserver>,
    mut memory_observer: Option<observers::MemoryObserver>,
    postgres_sync_config: Option<&LoadedConfig>,
) -> Result<yarli_core::entities::ContinuationPayload>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let mut tick_interval = tokio::time::interval(Duration::from_millis(100));
    tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(5));
    let mut reclaim_interval = tokio::time::interval(Duration::from_secs(10));
    let mut tick_count: u64 = 0;
    let mut stream_cursor = IncrementalEventCursor::new(
        EventQuery::by_correlation(correlation_id),
        STREAM_EVENT_BATCH_LIMIT,
    );
    let mut deterioration_observer = observers::DeteriorationObserver::new(
        run_id,
        correlation_id,
        observers::OBSERVER_WINDOW_SIZE,
    );
    let mut task_health_controllers: std::collections::HashMap<
        Uuid,
        yarli_exec::introspect::IntrospectionController,
    > = task_names
        .iter()
        .map(|(task_id, task_key)| {
            (*task_id, yarli_exec::introspect::IntrospectionController::new(0, task_key.clone()))
        })
        .collect();
    let mut postgres_sync_state = initialize_postgres_sync_state(postgres_sync_config).await?;

    if let Some(observer) = memory_observer.as_mut() {
        match tokio::time::timeout(
            Duration::from_secs(15),
            observer.observe_run_start(store.as_ref()),
        )
        .await
        {
            Ok(()) => {}
            Err(_) => warn!("memory observer observe_run_start timed out after 15s, continuing"),
        }
    }

    // Drain stale queue entries from prior runs before first tick.
    if let Err(e) = scheduler.cleanup_stale_queue().await {
        warn!(error = %e, "failed to clean up stale queue entries");
    }

    // Set up live output streaming: chunks flow from runner → scheduler → renderer
    // in real time, bypassing the event store batch path.
    let (live_tx, mut live_rx) = tokio::sync::mpsc::unbounded_channel::<LiveOutputEvent>();
    scheduler.set_live_output(live_tx);
    let live_streaming_active = true;

    // Spawn an independent task that forwards live chunks to StreamEvents.
    // This runs concurrently with the select! loop so output arrives even while
    // tick_with_cancel blocks on a long-running command.
    let stream_tx_live = tx.clone();
    let task_names_vec: Vec<(Uuid, String)> = task_names.to_vec();
    let mut forwarder_handle: Option<tokio::task::JoinHandle<()>> =
        Some(tokio::spawn(async move {
            while let Some(event) = live_rx.recv().await {
                let name = task_names_vec
                    .iter()
                    .find(|(tid, _)| *tid == event.task_id)
                    .map(|(_, n)| n.clone())
                    .unwrap_or_else(|| event.task_id.to_string());
                let _ = stream_tx_live.send(StreamEvent::CommandOutput {
                    task_id: event.task_id,
                    task_name: name,
                    line: event.chunk.data,
                });
            }
        }));

    let mut zero_progress_ticks: u64 = 0;
    let mut paused = false;
    let collect_run_total_tokens = || -> u64 {
        collect_run_token_totals(store.as_ref(), correlation_id)
            .map(|totals| totals.total_tokens)
            .unwrap_or_default()
    };

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("scheduler cancelled");
                let cancellation_source = shutdown
                    .cancellation_source()
                    .unwrap_or(CancellationSource::Unknown);
                let reason = cancellation_reason_for_source(cancellation_source);
                let provenance = if cancellation_diagnostics {
                    Some(with_cancellation_diagnostics(
                        shutdown.cancellation_provenance().unwrap_or_else(|| {
                            default_cancellation_provenance(
                                cancellation_source,
                                Some("cancellation signal observed".to_string()),
                            )
                        }),
                        run_id,
                        correlation_id,
                        tick_count,
                        reason,
                    ))
                } else {
                    shutdown.cancellation_provenance()
                };
                let _ = cancel_active_run(
                    scheduler,
                    store,
                    run_id,
                    reason,
                    cancellation_source,
                    provenance,
                )
                .await?;
                if let Err(err) =
                    sync_postgres_state_if_changed(scheduler, run_id, &mut postgres_sync_state)
                        .await
                {
                    warn!(run_id = %run_id, error = %err, "failed to sync postgres state after cancellation");
                }
                let _new_events =
                    emit_new_stream_events(store, &tx, task_names, &mut stream_cursor, live_streaming_active)?;
                emit_task_health_transition_audits(
                    &mut task_health_controllers,
                    &_new_events,
                    run_id,
                    scheduler.audit_sink().as_deref(),
                );
                if let Some(observer) = task_health_observer.as_mut() {
                    observer.observe_store(store.as_ref());
                }
                deterioration_observer.observe_store(store.as_ref())?;
                if let Some(observer) = memory_observer.as_mut() {
                    match tokio::time::timeout(
                        Duration::from_secs(15),
                        observer.observe_events(store.as_ref(), &_new_events),
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(_) => warn!("memory observer observe_events timed out after 15s on cancel"),
                    }
                }
                let payload = {
                    let reg = scheduler.registry().read().await;
                    let run = reg.get_run(&run_id).ok_or_else(|| {
                        anyhow::anyhow!("run {run_id} missing from registry after cancellation")
                    })?;
                    let tasks: Vec<&yarli_core::entities::Task> = run
                        .task_ids
                        .iter()
                        .filter_map(|tid| reg.get_task(tid))
                        .collect();
                    let run_total_tokens = collect_run_total_tokens();
                    let gate_failures = extract_gate_failure_names_for_run(store.as_ref(), run_id);
                    build_continuation_payload_with_gate_failures(
                        run,
                        &tasks,
                        deterioration_observer.latest_report(),
                        auto_advance_policy,
                        task_health,
                        run_total_tokens,
                        max_run_total_tokens,
                        soft_token_cap_ratio,
                        deterioration_observer.has_deterioration_cycle(),
                        &gate_failures,
                    )
                };
                if let Some(observer) = memory_observer.as_ref() {
                    let gate_failures = extract_gate_failure_names_for_run(store.as_ref(), run_id);
                    let _ = tokio::time::timeout(
                        Duration::from_secs(15),
                        observer.observe_run_end(store.as_ref(), &payload, &gate_failures),
                    ).await;
                }
                scheduler.clear_live_output();
                if let Some(h) = forwarder_handle.take() {
                    let _ = h.await;
                }
                drop(tx);
                return Ok(payload);
            }
            _ = heartbeat_interval.tick() => {
                scheduler.heartbeat_active_leases().await;
                let stats = scheduler.queue_stats();
                let heartbeat_message = if paused {
                    format!(
                        "paused: pending={} leased={} tick={} zero_progress_ticks={}",
                        stats.pending, stats.leased, tick_count, zero_progress_ticks
                    )
                } else {
                    format!(
                        "heartbeat: pending={} leased={} tick={} zero_progress_ticks={}",
                        stats.pending, stats.leased, tick_count, zero_progress_ticks
                    )
                };
                let _ = tx.send(StreamEvent::TransientStatus {
                    message: heartbeat_message.clone(),
                });
                let _ = append_event(
                    store.as_ref(),
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Run,
                        entity_id: run_id.to_string(),
                        event_type: "run.observer.progress".to_string(),
                        payload: serde_json::json!({
                            "tick": tick_count,
                            "queue_pending": stats.pending,
                            "queue_leased": stats.leased,
                            "paused": paused,
                            "zero_progress_ticks": zero_progress_ticks,
                            "summary": heartbeat_message,
                        }),
                        correlation_id,
                        causation_id: None,
                        actor: "observer.progress".to_string(),
                        idempotency_key: None,
                    },
                );
            }
            _ = reclaim_interval.tick() => {
                scheduler.reclaim_stale_leases().await;
            }
            _ = tick_interval.tick() => {
                tick_count += 1;

                // Poll events FIRST so control signals (pause/resume/cancel)
                // are processed before any task claiming in the tick below.
                let _new_events =
                    emit_new_stream_events(store, &tx, task_names, &mut stream_cursor, live_streaming_active)?;
                emit_task_health_transition_audits(
                    &mut task_health_controllers,
                    &_new_events,
                    run_id,
                    scheduler.audit_sink().as_deref(),
                );
                let control_signal = _new_events
                    .iter()
                    .filter_map(|event| operator_control_signal_from_event(event, run_id))
                    .next_back();
                if let Some(observer) = task_health_observer.as_mut() {
                    observer.observe_store(store.as_ref());
                }
                deterioration_observer.observe_store(store.as_ref())?;
                if let Some(observer) = memory_observer.as_mut() {
                    match tokio::time::timeout(
                        Duration::from_secs(15),
                        observer.observe_events(store.as_ref(), &_new_events),
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(_) => warn!("memory observer observe_events timed out after 15s, continuing"),
                    }
                }
                match control_signal {
                    Some(OperatorControlSignal::Pause { reason }) => {
                        paused = true;
                        let _ = tx.send(StreamEvent::TransientStatus {
                            message: format!("operator pause: {reason}"),
                        });
                    }
                    Some(OperatorControlSignal::Resume { reason }) => {
                        paused = false;
                        let _ = tx.send(StreamEvent::TransientStatus {
                            message: format!("operator resume: {reason}"),
                        });
                    }
                    Some(OperatorControlSignal::Cancel { reason }) => {
                        info!(%reason, "received operator cancel signal");
                        let operator_provenance = if cancellation_diagnostics {
                            Some(with_cancellation_diagnostics(
                                default_cancellation_provenance(
                                    CancellationSource::Operator,
                                    Some("operator control-plane cancel event".to_string()),
                                ),
                                run_id,
                                correlation_id,
                                tick_count,
                                &reason,
                            ))
                        } else {
                            Some(default_cancellation_provenance(
                                CancellationSource::Operator,
                                Some("operator control-plane cancel event".to_string()),
                            ))
                        };
                        let _ = cancel_active_run(
                            scheduler,
                            store,
                            run_id,
                            &reason,
                            CancellationSource::Operator,
                            operator_provenance,
                        )
                        .await?;
                        if let Err(err) = sync_postgres_state_if_changed(
                            scheduler,
                            run_id,
                            &mut postgres_sync_state,
                        )
                        .await
                        {
                            warn!(run_id = %run_id, error = %err, "failed to sync postgres state after operator cancellation");
                        }
                        let cancel_events =
                            emit_new_stream_events(store, &tx, task_names, &mut stream_cursor, live_streaming_active)?;
                        emit_task_health_transition_audits(
                            &mut task_health_controllers,
                            &cancel_events,
                            run_id,
                            scheduler.audit_sink().as_deref(),
                        );
                        if let Some(observer) = task_health_observer.as_mut() {
                            observer.observe_store(store.as_ref());
                        }
                        deterioration_observer.observe_store(store.as_ref())?;
                        if let Some(observer) = memory_observer.as_mut() {
                            match tokio::time::timeout(
                                Duration::from_secs(15),
                                observer.observe_events(store.as_ref(), &cancel_events),
                            )
                            .await
                            {
                                Ok(()) => {}
                                Err(_) => warn!("memory observer observe_events timed out after operator cancel"),
                            }
                        }
                        let payload = {
                            let reg = scheduler.registry().read().await;
                            let run = reg.get_run(&run_id).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "run {run_id} missing from registry after operator cancel"
                                )
                            })?;
                            let tasks: Vec<&yarli_core::entities::Task> = run
                                .task_ids
                                .iter()
                                .filter_map(|tid| reg.get_task(tid))
                                .collect();
                            let run_total_tokens = collect_run_total_tokens();
                            let gate_failures = extract_gate_failure_names_for_run(store.as_ref(), run_id);
                            build_continuation_payload_with_gate_failures(
                                run,
                                &tasks,
                                deterioration_observer.latest_report(),
                                auto_advance_policy,
                                task_health,
                                run_total_tokens,
                                max_run_total_tokens,
                                soft_token_cap_ratio,
                                deterioration_observer.has_deterioration_cycle(),
                                &gate_failures,
                            )
                        };
                        scheduler.clear_live_output();
                        if let Some(h) = forwarder_handle.take() {
                            let _ = h.await;
                        }
                        drop(tx);
                        return Ok(payload);
                    }
                    None => {}
                }


                // Check run projection for pause/resume state.
                if let Ok(Some(run_projection)) = load_run_projection(store.as_ref(), run_id) {
                    if run_projection.state == RunState::RunBlocked && !paused {
                        paused = true;
                    } else if run_projection.state == RunState::RunActive {
                        paused = false;
                    }
                }

                if paused {
                    zero_progress_ticks += 1;
                } else {
                    // Run a scheduler tick.
                    let _result = scheduler
                        .tick_with_cancel(cancel.child_token())
                        .await
                        .context("scheduler tick failed")?;
                    if let Err(err) =
                        sync_postgres_state_if_changed(scheduler, run_id, &mut postgres_sync_state)
                            .await
                    {
                        warn!(run_id = %run_id, error = %err, "failed to sync postgres state after scheduler tick");
                    }

                    debug!(
                        tick = tick_count,
                        promoted = _result.promoted,
                        claimed = _result.claimed,
                        executed = _result.executed,
                        failed = _result.failed,
                        errors = _result.errors,
                        "drive_scheduler tick"
                    );

                    // Post-tick event poll: catch control signals (pause/resume/cancel)
                    // that arrived during command execution within the tick above.
                    // Without this, a missed-tick fires immediately and claims new
                    // tasks before the next pre-tick poll can see the signal.
                    let post_tick_events =
                        emit_new_stream_events(store, &tx, task_names, &mut stream_cursor, live_streaming_active)?;
                    emit_task_health_transition_audits(
                        &mut task_health_controllers,
                        &post_tick_events,
                        run_id,
                        scheduler.audit_sink().as_deref(),
                    );
                    let post_tick_signal = post_tick_events
                        .iter()
                        .filter_map(|event| operator_control_signal_from_event(event, run_id))
                        .next_back();
                    if let Some(observer) = task_health_observer.as_mut() {
                        observer.observe_store(store.as_ref());
                    }
                    deterioration_observer.observe_store(store.as_ref())?;
                    match post_tick_signal {
                        Some(OperatorControlSignal::Pause { reason }) => {
                            paused = true;
                            let _ = tx.send(StreamEvent::TransientStatus {
                                message: format!("operator pause: {reason}"),
                            });
                        }
                        Some(OperatorControlSignal::Resume { reason }) => {
                            paused = false;
                            let _ = tx.send(StreamEvent::TransientStatus {
                                message: format!("operator resume: {reason}"),
                            });
                        }
                        Some(OperatorControlSignal::Cancel { reason }) => {
                            info!(%reason, "received operator cancel signal (post-tick)");
                            let operator_provenance = if cancellation_diagnostics {
                                Some(with_cancellation_diagnostics(
                                    default_cancellation_provenance(
                                        CancellationSource::Operator,
                                        Some("operator control-plane cancel event (post-tick)".to_string()),
                                    ),
                                    run_id,
                                    correlation_id,
                                    tick_count,
                                    &reason,
                                ))
                            } else {
                                Some(default_cancellation_provenance(
                                    CancellationSource::Operator,
                                    Some("operator control-plane cancel event (post-tick)".to_string()),
                                ))
                            };
                            let _ = cancel_active_run(
                                scheduler,
                                store,
                                run_id,
                                &reason,
                                CancellationSource::Operator,
                                operator_provenance,
                            )
                            .await?;
                            if let Err(err) = sync_postgres_state_if_changed(
                                scheduler,
                                run_id,
                                &mut postgres_sync_state,
                            )
                            .await
                            {
                                warn!(run_id = %run_id, error = %err, "failed to sync postgres state after post-tick operator cancellation");
                            }
                            let _cancel_events =
                                emit_new_stream_events(store, &tx, task_names, &mut stream_cursor, live_streaming_active)?;
                            emit_task_health_transition_audits(
                                &mut task_health_controllers,
                                &_cancel_events,
                                run_id,
                                scheduler.audit_sink().as_deref(),
                            );
                            if let Some(observer) = task_health_observer.as_mut() {
                                observer.observe_store(store.as_ref());
                            }
                            deterioration_observer.observe_store(store.as_ref())?;
                            let payload = {
                                let reg = scheduler.registry().read().await;
                                let run = reg.get_run(&run_id).ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "run {run_id} missing from registry after operator cancel (post-tick)"
                                    )
                                })?;
                                let tasks: Vec<&yarli_core::entities::Task> = run
                                    .task_ids
                                    .iter()
                                    .filter_map(|tid| reg.get_task(tid))
                                    .collect();
                                let run_total_tokens = collect_run_total_tokens();
                                let gate_failures =
                                    extract_gate_failure_names_for_run(store.as_ref(), run_id);
                                build_continuation_payload_with_gate_failures(
                                    run,
                                    &tasks,
                                    deterioration_observer.latest_report(),
                                    auto_advance_policy,
                                    task_health,
                                    run_total_tokens,
                                    max_run_total_tokens,
                                    soft_token_cap_ratio,
                                    deterioration_observer.has_deterioration_cycle(),
                                    &gate_failures,
                                )
                            };
                            if let Some(observer) = memory_observer.as_ref() {
                                let gate_failures = extract_gate_failure_names_for_run(store.as_ref(), run_id);
                                let _ = tokio::time::timeout(
                                    Duration::from_secs(15),
                                    observer.observe_run_end(store.as_ref(), &payload, &gate_failures),
                                ).await;
                            }
                            scheduler.clear_live_output();
                            if let Some(h) = forwarder_handle.take() {
                                let _ = h.await;
                            }
                            drop(tx);
                            return Ok(payload);
                        }
                        None => {}
                    }
                    // Also refresh projection after the post-tick poll.
                    if let Ok(Some(run_projection)) = load_run_projection(store.as_ref(), run_id) {
                        if run_projection.state == RunState::RunBlocked && !paused {
                            paused = true;
                        } else if run_projection.state == RunState::RunActive {
                            paused = false;
                        }
                    }

                    if _result.claimed == 0 && _result.executed == 0 {
                        zero_progress_ticks += 1;
                        if zero_progress_ticks >= 10 && zero_progress_ticks % 10 == 0 {
                            let stats = scheduler.queue_stats();
                            warn!(
                                consecutive_zero_ticks = zero_progress_ticks,
                                tick = tick_count,
                                queue_pending = stats.pending,
                                queue_leased = stats.leased,
                                "no tasks claimed or executed for {zero_progress_ticks} consecutive ticks"
                            );
                        }
                    } else {
                        zero_progress_ticks = 0;
                    }
                }

                // Send tick for spinner animation.
                let _ = tx.send(StreamEvent::Tick);

                // Check if the run is terminal.
                let terminal_payload = {
                    let reg = scheduler.registry().read().await;
                    if let Some(run) = reg.get_run(&run_id) {
                        if run.state.is_terminal() {
                            info!(state = ?run.state, ticks = tick_count, "run reached terminal state");
                            let tasks: Vec<&yarli_core::entities::Task> = run
                                .task_ids
                                .iter()
                                .filter_map(|tid| reg.get_task(tid))
                                .collect();
                            let run_total_tokens = collect_run_total_tokens();
                            let gate_failures = extract_gate_failure_names_for_run(store.as_ref(), run_id);
                            Some(build_continuation_payload_with_gate_failures(
                                run,
                                &tasks,
                                deterioration_observer.latest_report(),
                                auto_advance_policy,
                                task_health,
                                run_total_tokens,
                                max_run_total_tokens,
                                soft_token_cap_ratio,
                                deterioration_observer.has_deterioration_cycle(),
                                &gate_failures,
                            ))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }; // reg dropped here

                if let Some(payload) = terminal_payload {
                    if let Some(observer) = memory_observer.as_ref() {
                        let gate_failures = extract_gate_failure_names_for_run(store.as_ref(), run_id);
                        let _ = tokio::time::timeout(
                            Duration::from_secs(15),
                            observer.observe_run_end(store.as_ref(), &payload, &gate_failures),
                        ).await;
                    }
                    scheduler.clear_live_output();
                    if let Some(h) = forwarder_handle.take() {
                        let _ = h.await;
                    }
                    drop(tx);
                    return Ok(payload);
                }

                // Safety: bail after 10000 ticks (~16 min at 100ms) to prevent infinite loops.
                if tick_count > 10_000 {
                    scheduler.clear_live_output();
                    if let Some(h) = forwarder_handle.take() {
                        let _ = h.await;
                    }
                    drop(tx);
                    bail!("scheduler exceeded max ticks (10000)");
                }
            }
        }
    }
}

pub(crate) fn emit_new_stream_events<S: EventStore>(
    store: &Arc<S>,
    tx: &mpsc::UnboundedSender<StreamEvent>,
    task_names: &[(Uuid, String)],
    cursor: &mut IncrementalEventCursor,
    suppress_command_output: bool,
) -> Result<Vec<Event>> {
    let new_events = cursor.read_new_events(store.as_ref())?;

    for event in &new_events {
        for (task_id, task_name, depends_on) in stream_task_catalog_entries(event) {
            let _ = tx.send(StreamEvent::TaskDiscovered {
                task_id,
                task_name,
                depends_on,
            });
        }
        if let (Ok(task_id), Some(worker_id)) = (
            event.entity_id.parse::<Uuid>(),
            event.payload.get("worker").and_then(|v| v.as_str()),
        ) {
            let _ = tx.send(StreamEvent::TaskWorker {
                task_id,
                worker_id: worker_id.to_string(),
            });
        }
        if let Some(se) = event_to_stream_event(event, task_names, suppress_command_output) {
            let _ = tx.send(se);
        }
    }

    Ok(new_events)
}

pub(crate) async fn cancel_active_run<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    store: &Arc<S>,
    run_id: Uuid,
    reason: &str,
    cancellation_source: CancellationSource,
    provenance: Option<CancellationProvenance>,
) -> Result<bool>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let (correlation_id, task_ids) = {
        let reg = scheduler.registry().read().await;
        let Some(run) = reg.get_run(&run_id) else {
            return Ok(false);
        };
        (run.correlation_id, run.task_ids.clone())
    };

    let mut run_provenance =
        provenance.unwrap_or_else(|| default_cancellation_provenance(cancellation_source, None));
    run_provenance.cancellation_source = cancellation_source;
    if run_provenance.signal_name.is_none() {
        run_provenance.signal_name = match cancellation_source {
            CancellationSource::Sigint => Some("SIGINT".to_string()),
            CancellationSource::Sigterm => Some("SIGTERM".to_string()),
            _ => None,
        };
    }
    if run_provenance.signal_number.is_none() {
        run_provenance.signal_number = match cancellation_source {
            CancellationSource::Sigint => Some(2),
            CancellationSource::Sigterm => Some(15),
            _ => None,
        };
    }
    if run_provenance.receiver_pid.is_none() {
        run_provenance.receiver_pid = Some(std::process::id());
    }
    if run_provenance.actor_kind.is_none() {
        run_provenance.actor_kind = Some(actor_kind_for_source(cancellation_source));
    }
    if run_provenance.stage.is_none() {
        run_provenance.stage = Some(CancellationStage::Unknown);
    }

    let mut events: Vec<Event> = Vec::new();
    let mut first_cancelled_task_id: Option<Uuid> = None;
    let mut reg = scheduler.registry().write().await;

    for task_id in task_ids {
        let Some(task) = reg.get_task_mut(&task_id) else {
            continue;
        };
        if task.state.is_terminal() {
            continue;
        }

        let stage = cancellation_stage_for_task(task);
        let mut task_provenance = run_provenance.clone();
        task_provenance.stage = Some(stage);

        let attempt_no = task.attempt_no;
        let transition = task.transition(TaskState::TaskCancelled, reason, "cli", None)?;

        if first_cancelled_task_id.is_none() {
            first_cancelled_task_id = Some(task_id);
            run_provenance.stage = Some(stage);
        }

        events.push(Event {
            event_id: transition.event_id,
            occurred_at: transition.occurred_at,
            entity_type: EntityType::Task,
            entity_id: task_id.to_string(),
            event_type: "task.cancelled".to_string(),
            payload: serde_json::json!({
                "from": transition.from_state,
                "to": transition.to_state,
                "reason": transition.reason,
                "detail": transition.reason,
                "cancellation_source": cancellation_source.to_string(),
                "cancellation_provenance": task_provenance,
            }),
            correlation_id,
            causation_id: None,
            actor: "cli".to_string(),
            idempotency_key: Some(format!("{task_id}:cancelled:{attempt_no}")),
        });

        events.push(Event {
            event_id: Uuid::now_v7(),
            occurred_at: transition.occurred_at,
            entity_type: EntityType::Task,
            entity_id: task_id.to_string(),
            event_type: "task.cancel_provenance".to_string(),
            payload: serde_json::json!({
                "run_id": run_id.to_string(),
                "task_id": task_id.to_string(),
                "timestamp": transition.occurred_at,
                "cancellation_source": cancellation_source.to_string(),
                "signal_name": task_provenance.signal_name.clone(),
                "signal_number": task_provenance.signal_number,
                "sender_pid": task_provenance.sender_pid,
                "receiver_pid": task_provenance.receiver_pid,
                "parent_pid": task_provenance.parent_pid,
                "process_group_id": task_provenance.process_group_id,
                "session_id": task_provenance.session_id,
                "tty": task_provenance.tty.clone(),
                "actor_kind": task_provenance.actor_kind.map(|kind| kind.to_string()),
                "actor_detail": task_provenance.actor_detail.clone(),
                "stage": task_provenance.stage.map(|stage| stage.to_string()),
            }),
            correlation_id,
            causation_id: Some(transition.event_id),
            actor: "cli".to_string(),
            idempotency_key: Some(format!("{task_id}:cancel_provenance:{attempt_no}")),
        });
    }

    let run_cancelled = if let Some(run) = reg.get_run_mut(&run_id) {
        if run.state.is_terminal() {
            false
        } else {
            let transition = run.transition(RunState::RunCancelled, reason, "cli", None)?;
            run.cancellation_source = Some(cancellation_source);
            run.cancellation_provenance = Some(run_provenance.clone());

            events.push(Event {
                event_id: transition.event_id,
                occurred_at: transition.occurred_at,
                entity_type: EntityType::Run,
                entity_id: run_id.to_string(),
                event_type: "run.cancelled".to_string(),
                payload: serde_json::json!({
                    "from": transition.from_state,
                    "to": transition.to_state,
                    "reason": transition.reason,
                    "detail": transition.reason,
                    "exit_reason": run.exit_reason.map(|r| r.to_string()),
                    "cancellation_source": cancellation_source.to_string(),
                    "cancellation_provenance": run_provenance.clone(),
                }),
                correlation_id,
                causation_id: None,
                actor: "cli".to_string(),
                idempotency_key: Some(format!("{run_id}:cancelled")),
            });
            events.push(Event {
                event_id: Uuid::now_v7(),
                occurred_at: transition.occurred_at,
                entity_type: EntityType::Run,
                entity_id: run_id.to_string(),
                event_type: "run.cancel_provenance".to_string(),
                payload: serde_json::json!({
                    "run_id": run_id.to_string(),
                    "task_id": first_cancelled_task_id.map(|id| id.to_string()),
                    "timestamp": transition.occurred_at,
                    "cancellation_source": cancellation_source.to_string(),
                    "signal_name": run_provenance.signal_name.clone(),
                    "signal_number": run_provenance.signal_number,
                    "sender_pid": run_provenance.sender_pid,
                    "receiver_pid": run_provenance.receiver_pid,
                    "parent_pid": run_provenance.parent_pid,
                    "process_group_id": run_provenance.process_group_id,
                    "session_id": run_provenance.session_id,
                    "tty": run_provenance.tty.clone(),
                    "actor_kind": run_provenance.actor_kind.map(|kind| kind.to_string()),
                    "actor_detail": run_provenance.actor_detail.clone(),
                    "stage": run_provenance.stage.map(|stage| stage.to_string()),
                }),
                correlation_id,
                causation_id: Some(transition.event_id),
                actor: "cli".to_string(),
                idempotency_key: Some(format!("{run_id}:cancel_provenance")),
            });
            true
        }
    } else {
        false
    };

    drop(reg);

    // Drain pending/leased queue entries for this run to prevent stalls.
    match scheduler.cancel_run_queue(run_id) {
        Ok(count) if count > 0 => {
            info!(run_id = %run_id, cancelled_queue_entries = count, "drained queue entries for cancelled run");
        }
        Err(e) => {
            warn!(run_id = %run_id, error = %e, "failed to drain queue entries for cancelled run");
        }
        _ => {}
    }

    for event in events {
        store
            .append(event)
            .map_err(|e| anyhow::anyhow!("failed to persist cancellation event: {e}"))?;
    }

    if run_cancelled {
        if let Some(sink) = scheduler.audit_sink() {
            let entry = AuditEntry::destructive_attempt(
                "yarli-cli",
                "run.cancelled",
                reason,
                Some(run_id),
                first_cancelled_task_id,
                serde_json::json!({
                    "cancellation_source": cancellation_source.to_string(),
                    "cancellation_provenance": run_provenance.clone(),
                    "summary": format_cancel_provenance_summary(Some(&run_provenance)),
                }),
            );
            if let Err(err) = sink.append(&entry) {
                warn!(
                    run_id = %run_id,
                    error = %err,
                    "failed to append cancellation provenance audit entry"
                );
            }
        }
    }

    Ok(run_cancelled)
}

/// Parse a WorktreeState from its serialized form.
pub(crate) fn parse_worktree_state(s: &str) -> Option<WorktreeState> {
    match s {
        "WtUnbound" => Some(WorktreeState::WtUnbound),
        "WtCreating" => Some(WorktreeState::WtCreating),
        "WtBoundHome" => Some(WorktreeState::WtBoundHome),
        "WtSwitchPending" => Some(WorktreeState::WtSwitchPending),
        "WtBoundNonHome" => Some(WorktreeState::WtBoundNonHome),
        "WtMerging" => Some(WorktreeState::WtMerging),
        "WtConflict" => Some(WorktreeState::WtConflict),
        "WtRecovering" => Some(WorktreeState::WtRecovering),
        "WtCleanupPending" => Some(WorktreeState::WtCleanupPending),
        "WtClosed" => Some(WorktreeState::WtClosed),
        _ => None,
    }
}

/// Parse a MergeState from its serialized form.
pub(crate) fn parse_merge_state(s: &str) -> Option<MergeState> {
    match s {
        "MergeRequested" => Some(MergeState::MergeRequested),
        "MergePrecheck" => Some(MergeState::MergePrecheck),
        "MergeDryRun" => Some(MergeState::MergeDryRun),
        "MergeApply" => Some(MergeState::MergeApply),
        "MergeVerify" => Some(MergeState::MergeVerify),
        "MergeDone" => Some(MergeState::MergeDone),
        "MergeConflict" => Some(MergeState::MergeConflict),
        "MergeAborted" => Some(MergeState::MergeAborted),
        _ => None,
    }
}

pub(crate) fn parse_submodule_mode(s: &str) -> Option<SubmoduleMode> {
    match s {
        "locked" | "Locked" => Some(SubmoduleMode::Locked),
        "allow_fast_forward" | "AllowFastForward" => Some(SubmoduleMode::AllowFastForward),
        "allow_any" | "AllowAny" => Some(SubmoduleMode::AllowAny),
        _ => None,
    }
}

pub(crate) fn parse_merge_strategy_value(value: &str) -> Option<MergeStrategy> {
    match value {
        "merge-no-ff" | "merge_no_ff" | "MergeNoFf" => Some(MergeStrategy::MergeNoFf),
        "rebase-then-ff" | "rebase_then_ff" | "RebaseThenFf" => Some(MergeStrategy::RebaseThenFf),
        "squash-merge" | "squash_merge" | "SquashMerge" => Some(MergeStrategy::SquashMerge),
        _ => None,
    }
}

pub(crate) fn parse_recovery_action(action: &str) -> RecoveryAction {
    match action {
        "abort" => RecoveryAction::Abort,
        "resume" => RecoveryAction::Resume,
        "manual-block" => RecoveryAction::ManualBlock,
        _ => unreachable!("recovery action validated by caller"),
    }
}

pub(crate) fn block_on_current_runtime<F>(future: F) -> Result<F::Output>
where
    F: Future,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        Ok(tokio::task::block_in_place(|| handle.block_on(future)))
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to build tokio runtime for git orchestration")?;
        Ok(runtime.block_on(future))
    }
}

/// Run the renderer in a blocking context (requires raw terminal access).
pub(crate) fn run_renderer(
    mut rx: mpsc::UnboundedReceiver<StreamEvent>,
    render_mode: RenderMode,
    shutdown: ShutdownController,
    verbose_output: bool,
    audit_file: Option<PathBuf>,
) -> Result<()> {
    match render_mode {
        RenderMode::Stream => {
            let config = StreamConfig {
                verbose_output,
                ..StreamConfig::default()
            };
            let mut renderer = match StreamRenderer::new(config) {
                Ok(renderer) => renderer,
                Err(error) => {
                    eprintln!(
                        "warning: stream renderer unavailable ({error}); continuing in headless mode"
                    );
                    HeadlessRenderer::new().run(rx);
                    return Ok(());
                }
            };

            while let Some(event) = rx.blocking_recv() {
                renderer
                    .handle_event(event)
                    .context("renderer handle_event failed")?;
            }

            renderer
                .restore()
                .context("failed to restore stream terminal")?;
        }
        RenderMode::Dashboard => {
            let config = DashboardConfig::default();
            let mut renderer = match DashboardRenderer::new(config, audit_file) {
                Ok(renderer) => renderer,
                Err(error) => {
                    eprintln!(
                        "warning: dashboard renderer unavailable ({error}); continuing in headless mode"
                    );
                    HeadlessRenderer::new().run(rx);
                    return Ok(());
                }
            };

            loop {
                // Process all pending stream events (non-blocking drain).
                while let Ok(event) = rx.try_recv() {
                    renderer.handle_event(event);
                }

                // Draw the dashboard.
                renderer.draw().context("dashboard draw failed")?;

                // Poll for keyboard input (blocks for tick_rate_ms).
                let quit = renderer
                    .poll_input()
                    .context("dashboard input poll failed")?;
                if quit {
                    break;
                }

                // Check if the channel is closed (run finished).
                if rx.is_closed() && rx.is_empty() {
                    if !shutdown.is_shutting_down() {
                        // Final draw to show terminal state.
                        renderer.draw().context("dashboard final draw failed")?;
                        // Wait for user to press q.
                        loop {
                            if renderer
                                .poll_input()
                                .context("dashboard input poll failed")?
                            {
                                break;
                            }
                        }
                    }
                    break;
                }
            }

            renderer.restore().context("failed to restore terminal")?;
        }
    }

    Ok(())
}

pub(crate) const STREAM_EVENT_BATCH_LIMIT: usize = 256;
pub(crate) const CONTINUATION_WAIT_POLL_INTERVAL_MS: u64 = 250;
pub(crate) const OPERATOR_CONTROL_ACTOR: &str = "cli.operator";

#[derive(Debug, Clone)]
pub(crate) enum OperatorControlSignal {
    Pause { reason: String },
    Resume { reason: String },
    Cancel { reason: String },
}

pub(crate) fn operator_control_signal_from_event(
    event: &Event,
    run_id: Uuid,
) -> Option<OperatorControlSignal> {
    if event.entity_type != EntityType::Run {
        return None;
    }
    if event.entity_id != run_id.to_string() || event.actor != OPERATOR_CONTROL_ACTOR {
        return None;
    }
    let reason = event
        .payload
        .get("reason")
        .and_then(|v| v.as_str())
        .or_else(|| event.payload.get("detail").and_then(|v| v.as_str()))
        .unwrap_or("operator control action")
        .to_string();
    match event.event_type.as_str() {
        "run.blocked" => Some(OperatorControlSignal::Pause { reason }),
        "run.activated" => Some(OperatorControlSignal::Resume { reason }),
        "run.cancelled" => Some(OperatorControlSignal::Cancel { reason }),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IncrementalEventCursor {
    query: EventQuery,
    after_event_id: Option<Uuid>,
    batch_limit: usize,
}

impl IncrementalEventCursor {
    pub(crate) fn new(query: EventQuery, batch_limit: usize) -> Self {
        Self {
            query,
            after_event_id: None,
            batch_limit,
        }
    }

    pub(crate) fn read_new_events(&mut self, store: &dyn EventStore) -> Result<Vec<Event>> {
        let mut events = Vec::new();

        loop {
            let mut query = self.query.clone();
            query.limit = Some(self.batch_limit);
            query.after_event_id = self.after_event_id;

            let batch = query_events(store, &query)?;
            if batch.is_empty() {
                break;
            }
            let batch_len = batch.len();

            self.after_event_id = batch.last().map(|event| event.event_id);
            events.extend(batch);

            if batch_len < self.batch_limit {
                break;
            }
        }

        Ok(events)
    }
}

pub(crate) fn execute_task_unblock(
    store: &dyn EventStore,
    task_id: Uuid,
    reason: &str,
) -> Result<String> {
    let task = match load_task_projection(store, task_id)? {
        Some(task) => task,
        None => return Ok(format!("Task {task_id} not found in persisted event log.")),
    };

    if task.state != TaskState::TaskBlocked {
        return Ok(format!(
            "Task {task_id} is {:?}; no unblock action taken.",
            task.state
        ));
    }

    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: chrono::Utc::now(),
        entity_type: EntityType::Task,
        entity_id: task_id.to_string(),
        event_type: "task.unblocked".to_string(),
        payload: serde_json::json!({
            "from": "TaskBlocked",
            "to": "TaskReady",
            "reason": reason,
        }),
        correlation_id: task.correlation_id,
        causation_id: Some(task.last_event_id),
        actor: "cli".to_string(),
        idempotency_key: None,
    };

    append_event(store, event)?;
    Ok(format!(
        "Task {task_id} transitioned TaskBlocked -> TaskReady (reason: {reason})."
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn append_worktree_transition_event(
    store: &dyn EventStore,
    projection: &WorktreeProjection,
    event_type: &str,
    from: WorktreeState,
    to: WorktreeState,
    action: &str,
    reason: &str,
    causation_id: Option<Uuid>,
    binding: Option<&WorktreeBinding>,
    side_effect: bool,
    git_error: Option<&str>,
) -> Result<Event> {
    let mut payload = serde_json::json!({
        "from": format!("{from:?}"),
        "to": format!("{to:?}"),
        "action": action,
        "reason": reason,
        "run_id": projection.run_id,
        "side_effect": side_effect,
    });
    if let Some(message) = git_error {
        payload["git_error"] = serde_json::Value::String(message.to_string());
    }
    if let Some(map) = payload.as_object_mut() {
        if let Some(task_id) = projection.task_id {
            map.insert(
                "task_id".to_string(),
                serde_json::Value::String(task_id.to_string()),
            );
        }
        if let Some(mode) = projection.submodule_mode {
            map.insert(
                "submodule_mode".to_string(),
                serde_json::Value::String(
                    match mode {
                        SubmoduleMode::Locked => "locked",
                        SubmoduleMode::AllowFastForward => "allow_fast_forward",
                        SubmoduleMode::AllowAny => "allow_any",
                    }
                    .to_string(),
                ),
            );
        }

        if let Some(binding) = binding {
            map.insert(
                "repo_root".to_string(),
                serde_json::Value::String(binding.repo_root.display().to_string()),
            );
            map.insert(
                "worktree_path".to_string(),
                serde_json::Value::String(binding.worktree_path.display().to_string()),
            );
            map.insert(
                "branch_name".to_string(),
                serde_json::Value::String(binding.branch_name.clone()),
            );
            map.insert(
                "base_ref".to_string(),
                serde_json::Value::String(binding.base_ref.clone()),
            );
            map.insert(
                "head_ref".to_string(),
                serde_json::Value::String(binding.head_ref.clone()),
            );
            map.insert(
                "submodule_mode".to_string(),
                serde_json::Value::String(
                    match binding.submodule_mode {
                        SubmoduleMode::Locked => "locked",
                        SubmoduleMode::AllowFastForward => "allow_fast_forward",
                        SubmoduleMode::AllowAny => "allow_any",
                    }
                    .to_string(),
                ),
            );
            if let Some(task_id) = binding.task_id {
                map.insert(
                    "task_id".to_string(),
                    serde_json::Value::String(task_id.to_string()),
                );
            }
        } else {
            if let Some(repo_root) = projection.repo_root.as_ref() {
                map.insert(
                    "repo_root".to_string(),
                    serde_json::Value::String(repo_root.display().to_string()),
                );
            }
            if let Some(worktree_path) = projection.worktree_path.as_ref() {
                map.insert(
                    "worktree_path".to_string(),
                    serde_json::Value::String(worktree_path.display().to_string()),
                );
            }
            if let Some(branch_name) = projection.branch_name.as_ref() {
                map.insert(
                    "branch_name".to_string(),
                    serde_json::Value::String(branch_name.clone()),
                );
            }
            if let Some(base_ref) = projection.base_ref.as_ref() {
                map.insert(
                    "base_ref".to_string(),
                    serde_json::Value::String(base_ref.clone()),
                );
            }
            if let Some(head_ref) = projection.head_ref.as_ref() {
                map.insert(
                    "head_ref".to_string(),
                    serde_json::Value::String(head_ref.clone()),
                );
            }
        }
    }

    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: chrono::Utc::now(),
        entity_type: EntityType::Worktree,
        entity_id: projection.worktree_id.to_string(),
        event_type: event_type.to_string(),
        payload,
        correlation_id: projection.correlation_id,
        causation_id,
        actor: "cli".to_string(),
        idempotency_key: Some(worktree_recovery_idempotency_key(
            projection.worktree_id,
            action,
            event_type,
        )),
    };
    append_event(store, event.clone())?;
    Ok(event)
}

pub(crate) fn execute_worktree_recover(
    store: &dyn EventStore,
    worktree_id: Uuid,
    action: &str,
) -> Result<String> {
    let projection = match load_worktree_projection(store, worktree_id)? {
        Some(worktree) => worktree,
        None => {
            return Ok(format!(
                "Worktree {worktree_id} not found in persisted event log."
            ))
        }
    };

    let mut current_state = projection.state;
    let mut causation_id = Some(projection.last_event_id);
    let mut persisted_events = 0usize;

    if current_state != WorktreeState::WtRecovering {
        if !current_state.can_transition_to(WorktreeState::WtRecovering) {
            return Ok(format!(
                "Worktree {worktree_id} is {current_state:?}; cannot enter WtRecovering from this state."
            ));
        }

        let reason = format!("recovery action `{action}` requested by operator");
        let started = append_worktree_transition_event(
            store,
            &projection,
            "worktree.recovery_started",
            current_state,
            WorktreeState::WtRecovering,
            action,
            &reason,
            causation_id,
            None,
            false,
            None,
        )?;
        current_state = WorktreeState::WtRecovering;
        causation_id = Some(started.event_id);
        persisted_events += 1;
    }

    let mut side_effect_status = "executed";
    let mut failure_reason = None::<String>;
    let mut binding = match build_worktree_binding(&projection) {
        Ok(binding) => binding,
        Err(err) => {
            let reason = format!("recovery orchestration context error: {err}");
            append_worktree_transition_event(
                store,
                &projection,
                "worktree.recovery_failed",
                WorktreeState::WtRecovering,
                WorktreeState::WtRecovering,
                action,
                &reason,
                causation_id,
                None,
                false,
                Some(&reason),
            )?;
            persisted_events += 1;
            let mut output = String::new();
            writeln!(&mut output, "Worktree recovery for {worktree_id}")?;
            writeln!(&mut output, "Action: {action}")?;
            writeln!(
                &mut output,
                "Resulting state: {:?}",
                WorktreeState::WtRecovering
            )?;
            writeln!(&mut output, "Persisted events: {persisted_events}")?;
            writeln!(&mut output, "Side effect: failed")?;
            writeln!(&mut output, "Failure: {reason}")?;
            return Ok(output.trim_end().to_string());
        }
    };
    binding.state = current_state;
    let manager = LocalWorktreeManager::new();
    let recover_action = parse_recovery_action(action);
    let recover_result = block_on_current_runtime(manager.recover(
        &mut binding,
        recover_action,
        CancellationToken::new(),
    ))?;

    match recover_result {
        Ok(()) => {
            current_state = binding.state;
            let recovered_reason = match action {
                "abort" => "aborted interrupted operation and restored bound home state",
                "resume" => "resumed interrupted operation and restored bound home state",
                "manual-block" => "manual intervention required before recovery can continue",
                _ => unreachable!("action validated by caller"),
            };
            append_worktree_transition_event(
                store,
                &projection,
                "worktree.recovered",
                WorktreeState::WtRecovering,
                current_state,
                action,
                recovered_reason,
                causation_id,
                Some(&binding),
                true,
                None,
            )?;
            persisted_events += 1;
        }
        Err(GitError::InterruptedOperation { operation, .. })
            if matches!(recover_action, RecoveryAction::ManualBlock) =>
        {
            current_state = WorktreeState::WtRecovering;
            side_effect_status = "blocked";
            append_worktree_transition_event(
                store,
                &projection,
                "worktree.recovery_blocked",
                WorktreeState::WtRecovering,
                WorktreeState::WtRecovering,
                action,
                &format!("manual intervention required for interrupted {operation}"),
                causation_id,
                Some(&binding),
                true,
                None,
            )?;
            persisted_events += 1;
        }
        Err(err) => {
            current_state = binding.state;
            side_effect_status = "failed";
            let reason = format!("recovery orchestration failed: {err}");
            failure_reason = Some(reason.clone());
            append_worktree_transition_event(
                store,
                &projection,
                "worktree.recovery_failed",
                WorktreeState::WtRecovering,
                current_state,
                action,
                &reason,
                causation_id,
                Some(&binding),
                true,
                Some(&reason),
            )?;
            persisted_events += 1;
        }
    }

    let mut output = String::new();
    writeln!(&mut output, "Worktree recovery for {worktree_id}")?;
    writeln!(&mut output, "Action: {action}")?;
    writeln!(&mut output, "Resulting state: {current_state:?}")?;
    writeln!(&mut output, "Persisted events: {persisted_events}")?;
    writeln!(&mut output, "Side effect: {side_effect_status}")?;
    if let Some(reason) = failure_reason.as_ref() {
        writeln!(&mut output, "Failure: {reason}")?;
    }
    writeln!(
        &mut output,
        "Previous state: {:?} (event: {}, updated: {})",
        projection.state,
        projection.last_event_type,
        projection.updated_at.format("%Y-%m-%d %H:%M:%S")
    )?;
    if let Some(reason) = projection.reason.as_ref() {
        writeln!(&mut output, "Previous reason: {reason}")?;
    }
    Ok(output.trim_end().to_string())
}

pub(crate) fn gate_result_status(result: &GateResult) -> &'static str {
    match result {
        GateResult::Passed { .. } => "passed",
        GateResult::Failed { .. } => "failed",
        GateResult::Pending => "pending",
    }
}

pub(crate) fn parse_command_class_value(value: &serde_json::Value) -> Option<CommandClass> {
    serde_json::from_value::<CommandClass>(value.clone())
        .ok()
        .or_else(|| {
            value.as_str().and_then(|raw| match raw {
                "io" | "Io" => Some(CommandClass::Io),
                "cpu" | "Cpu" => Some(CommandClass::Cpu),
                "git" | "Git" => Some(CommandClass::Git),
                "tool" | "Tool" => Some(CommandClass::Tool),
                _ => None,
            })
        })
}

pub(crate) fn is_command_event_for_task(event: &Event, task_id: Uuid) -> bool {
    if event.entity_type != EntityType::Command {
        return false;
    }
    let Some(idempotency_key) = event.idempotency_key.as_deref() else {
        return false;
    };
    idempotency_key.starts_with(&format!("{task_id}:cmd:"))
}

pub(crate) fn collect_task_command_evidence(
    events: &[Event],
    task_id: Uuid,
    run_id: Uuid,
) -> (Vec<Evidence>, Option<CommandClass>) {
    let mut started_meta: HashMap<String, (String, Option<CommandClass>)> = HashMap::new();
    let mut terminal_events: Vec<&Event> = Vec::new();

    for event in events
        .iter()
        .filter(|event| is_command_event_for_task(event, task_id))
    {
        match event.event_type.as_str() {
            "command.started" => {
                let command = event
                    .payload
                    .get("command")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let command_class = event
                    .payload
                    .get("command_class")
                    .and_then(parse_command_class_value);
                started_meta.insert(event.entity_id.clone(), (command, command_class));
            }
            "command.exited" | "command.timed_out" | "command.killed" => {
                terminal_events.push(event);
            }
            _ => {}
        }
    }

    let Some(terminal) = terminal_events.last() else {
        return (Vec::new(), None);
    };

    let (command, command_class) = started_meta
        .get(&terminal.entity_id)
        .map(|(command, class)| (command.clone(), *class))
        .unwrap_or_else(|| ("unknown".to_string(), None));
    let exit_code = terminal
        .payload
        .get("exit_code")
        .and_then(|value| value.as_i64())
        .unwrap_or(-1);
    let duration_ms = terminal
        .payload
        .get("duration_ms")
        .and_then(|value| value.as_i64())
        .unwrap_or(0)
        .max(0) as u64;

    let evidence = Evidence {
        evidence_id: terminal.event_id,
        task_id,
        run_id,
        evidence_type: "command_result".to_string(),
        payload: serde_json::json!({
            "command": command,
            "exit_code": exit_code,
            "duration_ms": duration_ms,
            "timed_out": terminal.event_type == "command.timed_out",
            "killed": terminal.event_type == "command.killed",
        }),
        created_at: terminal.occurred_at,
    };

    (vec![evidence], command_class)
}

pub(crate) fn policy_outcome_is_allowed(payload: &serde_json::Value) -> bool {
    payload
        .get("outcome")
        .and_then(|value| value.as_str())
        .map(|outcome| outcome.eq_ignore_ascii_case("ALLOW"))
        .unwrap_or(false)
}

pub(crate) fn build_task_gate_context(
    store: &dyn EventStore,
    task: &TaskProjection,
) -> Result<Option<(Uuid, GateContext)>> {
    let events = query_events(store, &EventQuery::by_correlation(task.correlation_id))?;
    let run_id = events
        .iter()
        .find(|event| event.entity_type == EntityType::Run)
        .and_then(|event| event.entity_id.parse::<Uuid>().ok());
    let Some(run_id) = run_id else {
        return Ok(None);
    };

    let task_events: Vec<Event> = events
        .iter()
        .filter(|event| event.entity_type == EntityType::Task)
        .cloned()
        .collect();
    let task_snapshots = collect_task_projections(&task_events);
    let task_states = task_snapshots
        .iter()
        .map(|task| (task.task_id, task.task_id.to_string(), task.state))
        .collect::<Vec<_>>();
    let all_tasks_complete = !task_states.is_empty()
        && task_states
            .iter()
            .all(|(_, _, state)| *state == TaskState::TaskComplete);

    let worktree_events: Vec<Event> = events
        .iter()
        .filter(|event| event.entity_type == EntityType::Worktree)
        .cloned()
        .collect();
    let worktrees = collect_entity_projections(&worktree_events);
    let has_unresolved_conflicts = worktrees.iter().any(|worktree| {
        worktree.state == "WtConflict"
            || worktree
                .last_event_type
                .to_ascii_lowercase()
                .contains("conflict")
    });

    let mut policy_violations = Vec::new();
    let mut has_unapproved_git_ops = false;
    for event in events
        .iter()
        .filter(|event| event.entity_type == EntityType::Policy)
    {
        if policy_outcome_is_allowed(&event.payload) {
            continue;
        }
        if let Some(reason) = event.payload.get("reason").and_then(|value| value.as_str()) {
            policy_violations.push(reason.to_string());
        }
        if let Some(action) = event.payload.get("action").and_then(|value| value.as_str()) {
            let action = action.to_ascii_lowercase();
            if action.contains("git")
                || action.contains("push")
                || action.contains("merge")
                || action.contains("rebase")
            {
                has_unapproved_git_ops = true;
            }
        }
    }

    let (evidence, command_class) = collect_task_command_evidence(&events, task.task_id, run_id);

    let mut gate_ctx = GateContext::for_task(run_id, task.task_id, task.task_id.to_string());
    gate_ctx.evidence = evidence;
    gate_ctx.task_states = task_states;
    gate_ctx.all_tasks_complete = all_tasks_complete;
    gate_ctx.has_unresolved_conflicts = has_unresolved_conflicts;
    gate_ctx.has_unapproved_git_ops = has_unapproved_git_ops;
    gate_ctx.worktree_consistent = !has_unresolved_conflicts;
    gate_ctx.policy_violations = policy_violations;
    gate_ctx.command_class = command_class;

    Ok(Some((run_id, gate_ctx)))
}

pub(crate) fn execute_gate_rerun(
    store: &dyn EventStore,
    task_id: Uuid,
    gate_name: Option<&str>,
) -> Result<String> {
    let task = match load_task_projection(store, task_id)? {
        Some(task) => task,
        None => return Ok(format!("Task {task_id} not found in persisted event log.")),
    };
    let selected_gates = if let Some(name) = gate_name {
        vec![parse_gate_type(name).ok_or_else(|| {
            anyhow::anyhow!(
                "unknown gate: {name}. Valid gates: {}",
                all_gate_names().join(", ")
            )
        })?]
    } else {
        default_task_gates()
    };

    let Some((run_id, gate_ctx)) = build_task_gate_context(store, &task)? else {
        return Ok(format!(
            "Task {task_id} has no associated run context in persisted event log."
        ));
    };

    let evaluations = evaluate_all(&selected_gates, &gate_ctx);
    for evaluation in &evaluations {
        append_event(
            store,
            Event {
                event_id: Uuid::now_v7(),
                occurred_at: chrono::Utc::now(),
                entity_type: EntityType::Gate,
                entity_id: task_id.to_string(),
                event_type: "gate.evaluated".to_string(),
                payload: serde_json::json!({
                    "scope": "task",
                    "run_id": run_id,
                    "task_id": task_id,
                    "gate": evaluation.gate_type.label(),
                    "status": gate_result_status(&evaluation.result),
                    "result": evaluation.result,
                    "inspected_evidence": evaluation.inspected_evidence,
                    "remediation": evaluation.remediation,
                    "rerun": true,
                }),
                correlation_id: task.correlation_id,
                causation_id: Some(task.last_event_id),
                actor: "cli".to_string(),
                idempotency_key: Some(gate_rerun_idempotency_key(
                    task_id,
                    evaluation.gate_type.label(),
                )),
            },
        )?;
    }

    render_gate_rerun_output(task_id, run_id, selected_gates.len(), &evaluations)
}

pub(crate) fn policy_outcome_label(outcome: PolicyOutcome) -> &'static str {
    match outcome {
        PolicyOutcome::Allow => "allow",
        PolicyOutcome::Deny => "deny",
        PolicyOutcome::RequireApproval => "require_approval",
    }
}

pub(crate) fn gate_rerun_idempotency_key(task_id: Uuid, gate_label: &str) -> String {
    format!("{task_id}:gate_rerun:{gate_label}")
}

pub(crate) fn worktree_recovery_idempotency_key(
    worktree_id: Uuid,
    action: &str,
    event_type: &str,
) -> String {
    format!("{worktree_id}:worktree_recover:{action}:{event_type}")
}

pub(crate) fn merge_request_idempotency_key(
    run_id: Uuid,
    source: &str,
    target: &str,
    strategy: &str,
) -> String {
    format!("{run_id}:merge_request:{source}:{target}:{strategy}")
}

pub(crate) fn merge_operation_idempotency_key(
    merge_id: Uuid,
    operation: &str,
    event_type: &str,
) -> String {
    format!("{merge_id}:{operation}:{event_type}")
}

pub(crate) fn merge_apply_idempotency_key(
    run_id: Uuid,
    event_type: &str,
    task_key: Option<&str>,
    task_index: Option<usize>,
) -> String {
    match (task_key, task_index) {
        (Some(task_key), Some(task_index)) => {
            format!("{run_id}:{event_type}:{task_index}:{task_key}")
        }
        (Some(task_key), None) => format!("{run_id}:{event_type}:{task_key}"),
        (None, Some(task_index)) => format!("{run_id}:{event_type}:{task_index}"),
        (None, None) => format!("{run_id}:{event_type}"),
    }
}

pub(crate) fn persist_merge_policy_decision(
    store: &dyn EventStore,
    merge: &MergeProjection,
    decision: &yarli_core::domain::PolicyDecision,
    operation: &str,
) -> Result<()> {
    append_event(
        store,
        Event {
            event_id: decision.decision_id,
            occurred_at: decision.decided_at,
            entity_type: EntityType::Policy,
            entity_id: decision.decision_id.to_string(),
            event_type: "policy.decision".to_string(),
            payload: serde_json::json!({
                "run_id": decision.run_id,
                "merge_id": merge.merge_id,
                "operation": operation,
                "action": decision.action,
                "outcome": decision.outcome,
                "rule_id": decision.rule_id,
                "reason": decision.reason,
            }),
            correlation_id: merge.correlation_id,
            causation_id: Some(merge.last_event_id),
            actor: decision.actor.clone(),
            idempotency_key: Some(merge_operation_idempotency_key(
                merge.merge_id,
                operation,
                "policy.decision",
            )),
        },
    )
}

pub(crate) fn append_merge_policy_audit(
    sink: Option<&dyn AuditSink>,
    decision: &yarli_core::domain::PolicyDecision,
    merge: &MergeProjection,
    operation: &str,
) -> Result<()> {
    if let Some(sink) = sink {
        let mut entry = AuditEntry::from_policy_decision(decision);
        entry.details = serde_json::json!({
            "decision_id": decision.decision_id,
            "decided_at": decision.decided_at,
            "merge_id": merge.merge_id,
            "merge_state": format!("{:?}", merge.state),
            "operation": operation,
            "source": merge.source_ref.clone(),
            "target": merge.target_ref.clone(),
            "strategy": merge.strategy.clone(),
        });
        sink.append(&entry)
            .map_err(|e| anyhow::anyhow!("failed to append policy audit entry: {e}"))?;
    }
    Ok(())
}

pub(crate) fn evaluate_merge_policy(
    merge: &MergeProjection,
    safe_mode: SafeMode,
    actor: &str,
) -> Result<yarli_core::domain::PolicyDecision> {
    let run_id = merge
        .run_id
        .ok_or_else(|| anyhow::anyhow!("merge intent {} missing run context", merge.merge_id))?;
    let request = PolicyRequest {
        actor: actor.to_string(),
        action: ActionType::Merge,
        command_class: Some(CommandClass::Git),
        repo_path: None,
        branch: merge.target_ref.clone(),
        run_id,
        task_id: None,
        safe_mode,
    };
    let mut engine = PolicyEngine::with_defaults();
    engine
        .evaluate(&request)
        .map_err(|e| anyhow::anyhow!("policy evaluation failed: {e}"))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn append_merge_execution_event(
    store: &dyn EventStore,
    merge: &MergeProjection,
    operation: &str,
    event_type: &str,
    from: MergeState,
    to: MergeState,
    reason: &str,
    causation_id: Option<Uuid>,
    worktree: Option<&WorktreeBinding>,
    merge_sha: Option<&str>,
    conflict_count: Option<usize>,
) -> Result<()> {
    let mut payload = serde_json::json!({
        "operation": operation,
        "from": format!("{from:?}"),
        "to": format!("{to:?}"),
        "state": format!("{to:?}"),
        "run_id": merge.run_id,
        "worktree_id": merge.worktree_id.or(worktree.map(|binding| binding.id)),
        "source": merge.source_ref.clone(),
        "target": merge.target_ref.clone(),
        "strategy": merge.strategy.clone(),
        "reason": reason,
    });
    if let Some(sha) = merge_sha {
        payload["merge_sha"] = serde_json::Value::String(sha.to_string());
    }
    if let Some(count) = conflict_count {
        payload["conflict_count"] = serde_json::json!(count);
    }
    if let Some(binding) = worktree {
        payload["worktree_state"] = serde_json::Value::String(format!("{:?}", binding.state));
        payload["worktree_head"] = serde_json::Value::String(binding.head_ref.clone());
        payload["repo_root"] = serde_json::Value::String(binding.repo_root.display().to_string());
        payload["worktree_path"] =
            serde_json::Value::String(binding.worktree_path.display().to_string());
    }

    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Merge,
            entity_id: merge.merge_id.to_string(),
            event_type: event_type.to_string(),
            payload,
            correlation_id: merge.correlation_id,
            causation_id,
            actor: "cli".to_string(),
            idempotency_key: Some(merge_operation_idempotency_key(
                merge.merge_id,
                operation,
                event_type,
            )),
        },
    )
}

pub(crate) fn append_merge_apply_event(
    store: &dyn EventStore,
    run_id: Uuid,
    correlation_id: Uuid,
    event: &MergeApplyTelemetryEvent,
    causation_id: Option<Uuid>,
) -> Result<()> {
    let mut payload = event.metadata.clone();
    payload["run_id"] = serde_json::json!(run_id);
    payload["event_type"] = serde_json::Value::String(event.event_type.clone());
    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Run,
            entity_id: run_id.to_string(),
            event_type: event.event_type.clone(),
            payload,
            correlation_id,
            causation_id,
            actor: "cli".to_string(),
            idempotency_key: Some(merge_apply_idempotency_key(
                run_id,
                &event.event_type,
                event.task_key.as_deref(),
                event.task_index,
            )),
        },
    )
}

pub(crate) fn append_merge_apply_audit_event(
    audit_sink: Option<&dyn AuditSink>,
    event: &MergeApplyTelemetryEvent,
    run_id: Uuid,
) -> Result<()> {
    if let Some(sink) = audit_sink {
        sink.append(&AuditEntry::destructive_attempt(
            "cli",
            event.event_type.as_str(),
            "merge apply telemetry",
            Some(run_id),
            None,
            event.metadata.clone(),
        ))
        .map_err(|e| anyhow::anyhow!("failed to append merge telemetry audit entry: {e}"))?;
    }
    Ok(())
}

pub(crate) fn append_merge_apply_telemetry_events(
    store: &dyn EventStore,
    run_id: Uuid,
    correlation_id: Uuid,
    events: &[MergeApplyTelemetryEvent],
    causation_id: Option<Uuid>,
    audit_sink: Option<&dyn AuditSink>,
) -> Result<()> {
    for event in events {
        if let Err(err) =
            append_merge_apply_event(store, run_id, correlation_id, event, causation_id)
        {
            warn!(
                run_id = %run_id,
                event = %event.event_type,
                "failed to append merge telemetry event: {err}"
            );
            continue;
        }
        if let Err(err) = append_merge_apply_audit_event(audit_sink, event, run_id) {
            warn!(
                run_id = %run_id,
                event = %event.event_type,
                "failed to append merge telemetry audit event: {err}"
            );
        }
    }
    Ok(())
}

pub(crate) fn execute_merge_request(
    store: &dyn EventStore,
    source: &str,
    target: &str,
    run_id: Uuid,
    strategy: &str,
) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };
    let worktree_id = load_latest_worktree_projection_for_run(store, run_id, run.correlation_id)?
        .map(|worktree| worktree.worktree_id);

    let merge_id = Uuid::now_v7();
    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Merge,
            entity_id: merge_id.to_string(),
            event_type: "merge.requested".to_string(),
            payload: serde_json::json!({
                "from": "MergeRequested",
                "to": "MergeRequested",
                "state": "MergeRequested",
                "run_id": run_id,
                "worktree_id": worktree_id,
                "source": source,
                "target": target,
                "strategy": strategy,
                "reason": "pending approval",
            }),
            correlation_id: run.correlation_id,
            causation_id: None,
            actor: "cli".to_string(),
            idempotency_key: Some(merge_request_idempotency_key(
                run_id, source, target, strategy,
            )),
        },
    )?;

    let mut out = String::new();
    writeln!(&mut out, "Merge intent requested")?;
    writeln!(&mut out, "Merge ID: {merge_id}")?;
    writeln!(&mut out, "Run: {run_id}")?;
    if let Some(worktree_id) = worktree_id {
        writeln!(&mut out, "Worktree: {worktree_id}")?;
    }
    writeln!(&mut out, "Source: {source}")?;
    writeln!(&mut out, "Target: {target}")?;
    writeln!(&mut out, "Strategy: {strategy}")?;
    writeln!(&mut out, "State: MergeRequested")?;
    Ok(out.trim_end().to_string())
}

pub(crate) fn execute_merge_approve(
    store: &dyn EventStore,
    merge_id: Uuid,
    safe_mode: SafeMode,
    enforce_policies: bool,
    audit_sink: Option<&dyn AuditSink>,
) -> Result<String> {
    let merge = match load_merge_projection(store, merge_id)? {
        Some(merge) => merge,
        None => {
            return Ok(format!(
                "Merge intent {merge_id} not found in persisted event log."
            ))
        }
    };

    if merge.state != MergeState::MergeRequested {
        return Ok(format!(
            "Merge intent {merge_id} is {:?}; approval allowed only from MergeRequested.",
            merge.state
        ));
    }

    let mut causation_id = Some(merge.last_event_id);
    if enforce_policies {
        let decision = evaluate_merge_policy(&merge, safe_mode, "cli")?;
        persist_merge_policy_decision(store, &merge, &decision, "merge.approve")?;
        append_merge_policy_audit(audit_sink, &decision, &merge, "merge.approve")?;

        if decision.outcome != PolicyOutcome::Allow {
            append_event(
                store,
                Event {
                    event_id: Uuid::now_v7(),
                    occurred_at: chrono::Utc::now(),
                    entity_type: EntityType::Merge,
                    entity_id: merge_id.to_string(),
                    event_type: "merge.policy_blocked".to_string(),
                    payload: serde_json::json!({
                        "from": format!("{:?}", merge.state),
                        "to": format!("{:?}", merge.state),
                        "state": format!("{:?}", merge.state),
                        "run_id": merge.run_id,
                        "reason": format!("approval blocked by policy: {}", decision.reason),
                        "policy": {
                            "action": decision.action,
                            "outcome": decision.outcome,
                            "rule_id": decision.rule_id,
                        },
                    }),
                    correlation_id: merge.correlation_id,
                    causation_id: Some(decision.decision_id),
                    actor: "cli".to_string(),
                    idempotency_key: Some(merge_operation_idempotency_key(
                        merge_id,
                        "merge.approve",
                        "merge.policy_blocked",
                    )),
                },
            )?;
            if let Some(sink) = audit_sink {
                sink.append(&AuditEntry::destructive_attempt(
                    "cli",
                    "merge.approve",
                    format!("blocked by policy: {}", decision.reason),
                    merge.run_id,
                    None,
                    serde_json::json!({
                        "merge_id": merge_id,
                        "outcome": decision.outcome,
                        "rule_id": decision.rule_id,
                    }),
                ))
                .map_err(|e| anyhow::anyhow!("failed to append policy-block audit entry: {e}"))?;
            }

            return Ok(format!(
                "Merge intent {merge_id} approval blocked by policy ({})",
                policy_outcome_label(decision.outcome)
            ));
        }

        causation_id = Some(decision.decision_id);
    }

    let approved_event_id = Uuid::now_v7();
    append_event(
        store,
        Event {
            event_id: approved_event_id,
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Merge,
            entity_id: merge_id.to_string(),
            event_type: "merge.approved".to_string(),
            payload: serde_json::json!({
                "from": "MergeRequested",
                "to": "MergePrecheck",
                "state": "MergePrecheck",
                "run_id": merge.run_id,
                "worktree_id": merge.worktree_id,
                "source": merge.source_ref,
                "target": merge.target_ref,
                "strategy": merge.strategy,
                "reason": "approved by operator",
            }),
            correlation_id: merge.correlation_id,
            causation_id,
            actor: "cli".to_string(),
            idempotency_key: Some(merge_operation_idempotency_key(
                merge_id,
                "merge.approve",
                "merge.approved",
            )),
        },
    )?;

    let worktree_projection = match resolve_merge_worktree_projection(store, &merge) {
        Ok(worktree) => worktree,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.approve",
                "merge.execution_failed",
                MergeState::MergePrecheck,
                MergeState::MergePrecheck,
                &format!("merge orchestration context error: {err}"),
                Some(approved_event_id),
                None,
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} approved but execution failed (missing git context): {err}"
            ));
        }
    };

    let mut binding = match build_worktree_binding(&worktree_projection) {
        Ok(binding) => binding,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.approve",
                "merge.execution_failed",
                MergeState::MergePrecheck,
                MergeState::MergePrecheck,
                &format!("merge orchestration context error: {err}"),
                Some(approved_event_id),
                None,
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} approved but execution failed (invalid worktree context): {err}"
            ));
        }
    };
    let mut intent = match build_merge_intent(&merge, binding.id) {
        Ok(intent) => intent,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.approve",
                "merge.execution_failed",
                MergeState::MergePrecheck,
                MergeState::MergePrecheck,
                &format!("merge orchestration context error: {err}"),
                Some(approved_event_id),
                Some(&binding),
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} approved but execution failed (invalid merge context): {err}"
            ));
        }
    };

    let orchestrator = LocalMergeOrchestrator::new(LocalWorktreeManager::new());
    let cancel = CancellationToken::new();
    let precheck = match block_on_current_runtime(orchestrator.precheck(
        &mut intent,
        &binding,
        cancel.clone(),
    ))? {
        Ok(precheck) => precheck,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.approve",
                "merge.execution_failed",
                MergeState::MergePrecheck,
                intent.state,
                &format!("merge precheck failed: {err}"),
                Some(approved_event_id),
                Some(&binding),
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} approved but precheck failed: {err}"
            ));
        }
    };

    let dry_run = match block_on_current_runtime(orchestrator.dry_run(
        &mut intent,
        &binding,
        cancel.clone(),
    ))? {
        Ok(result) => result,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.approve",
                "merge.execution_failed",
                MergeState::MergePrecheck,
                intent.state,
                &format!("merge dry-run failed: {err}"),
                Some(approved_event_id),
                Some(&binding),
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} approved but dry-run failed: {err}"
            ));
        }
    };

    if !dry_run.clean {
        append_merge_execution_event(
            store,
            &merge,
            "merge.approve",
            "merge.execution_failed",
            MergeState::MergePrecheck,
            MergeState::MergeConflict,
            "merge dry-run detected conflicts",
            Some(approved_event_id),
            Some(&binding),
            None,
            Some(dry_run.conflicts.len()),
        )?;
        return Ok(format!(
            "Merge intent {merge_id} approved but dry-run found {} conflict(s).",
            dry_run.conflicts.len()
        ));
    }

    let apply = match block_on_current_runtime(orchestrator.apply(
        &mut intent,
        &mut binding,
        cancel.clone(),
    ))? {
        Ok(apply) => apply,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.approve",
                "merge.execution_failed",
                MergeState::MergeApply,
                intent.state,
                &format!("merge apply failed: {err}"),
                Some(approved_event_id),
                Some(&binding),
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} approved but apply failed: {err}"
            ));
        }
    };

    if let Err(err) = block_on_current_runtime(orchestrator.verify(
        &mut intent,
        &binding,
        &precheck.submodule_snapshot,
        cancel,
    ))? {
        append_merge_execution_event(
            store,
            &merge,
            "merge.approve",
            "merge.execution_failed",
            MergeState::MergeVerify,
            intent.state,
            &format!("merge verify failed: {err}"),
            Some(approved_event_id),
            Some(&binding),
            apply.merge_sha.as_str().into(),
            None,
        )?;
        return Ok(format!(
            "Merge intent {merge_id} approved but verify failed: {err}"
        ));
    }

    append_merge_execution_event(
        store,
        &merge,
        "merge.approve",
        "merge.execution_succeeded",
        MergeState::MergePrecheck,
        intent.state,
        "merge orchestration completed successfully",
        Some(approved_event_id),
        Some(&binding),
        Some(apply.merge_sha.as_str()),
        None,
    )?;

    Ok(format!(
        "Merge intent {merge_id} approved and executed to {:?} (merge_sha: {}).",
        intent.state, apply.merge_sha
    ))
}

pub(crate) fn execute_merge_reject(
    store: &dyn EventStore,
    merge_id: Uuid,
    reason: &str,
    safe_mode: SafeMode,
    enforce_policies: bool,
    audit_sink: Option<&dyn AuditSink>,
) -> Result<String> {
    let merge = match load_merge_projection(store, merge_id)? {
        Some(merge) => merge,
        None => {
            return Ok(format!(
                "Merge intent {merge_id} not found in persisted event log."
            ))
        }
    };

    if merge.state != MergeState::MergeRequested {
        return Ok(format!(
            "Merge intent {merge_id} is {:?}; rejection allowed only from MergeRequested.",
            merge.state
        ));
    }

    let mut causation_id = Some(merge.last_event_id);
    if enforce_policies {
        let decision = evaluate_merge_policy(&merge, safe_mode, "cli")?;
        persist_merge_policy_decision(store, &merge, &decision, "merge.reject")?;
        append_merge_policy_audit(audit_sink, &decision, &merge, "merge.reject")?;

        if decision.outcome != PolicyOutcome::Allow {
            append_event(
                store,
                Event {
                    event_id: Uuid::now_v7(),
                    occurred_at: chrono::Utc::now(),
                    entity_type: EntityType::Merge,
                    entity_id: merge_id.to_string(),
                    event_type: "merge.policy_blocked".to_string(),
                    payload: serde_json::json!({
                        "from": format!("{:?}", merge.state),
                        "to": format!("{:?}", merge.state),
                        "state": format!("{:?}", merge.state),
                        "run_id": merge.run_id,
                        "reason": format!("rejection blocked by policy: {}", decision.reason),
                        "policy": {
                            "action": decision.action,
                            "outcome": decision.outcome,
                            "rule_id": decision.rule_id,
                        },
                    }),
                    correlation_id: merge.correlation_id,
                    causation_id: Some(decision.decision_id),
                    actor: "cli".to_string(),
                    idempotency_key: Some(merge_operation_idempotency_key(
                        merge_id,
                        "merge.reject",
                        "merge.policy_blocked",
                    )),
                },
            )?;
            if let Some(sink) = audit_sink {
                sink.append(&AuditEntry::destructive_attempt(
                    "cli",
                    "merge.reject",
                    format!("blocked by policy: {}", decision.reason),
                    merge.run_id,
                    None,
                    serde_json::json!({
                        "merge_id": merge_id,
                        "outcome": decision.outcome,
                        "rule_id": decision.rule_id,
                    }),
                ))
                .map_err(|e| anyhow::anyhow!("failed to append policy-block audit entry: {e}"))?;
            }

            return Ok(format!(
                "Merge intent {merge_id} rejection blocked by policy ({})",
                policy_outcome_label(decision.outcome)
            ));
        }

        causation_id = Some(decision.decision_id);
    }

    let rejected_event_id = Uuid::now_v7();
    append_event(
        store,
        Event {
            event_id: rejected_event_id,
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Merge,
            entity_id: merge_id.to_string(),
            event_type: "merge.rejected".to_string(),
            payload: serde_json::json!({
                "from": "MergeRequested",
                "to": "MergeAborted",
                "state": "MergeAborted",
                "run_id": merge.run_id,
                "worktree_id": merge.worktree_id,
                "source": merge.source_ref,
                "target": merge.target_ref,
                "strategy": merge.strategy,
                "reason": reason,
            }),
            correlation_id: merge.correlation_id,
            causation_id,
            actor: "cli".to_string(),
            idempotency_key: Some(merge_operation_idempotency_key(
                merge_id,
                "merge.reject",
                "merge.rejected",
            )),
        },
    )?;

    let worktree_projection = match resolve_merge_worktree_projection(store, &merge) {
        Ok(worktree) => worktree,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.reject",
                "merge.execution_failed",
                MergeState::MergeRequested,
                MergeState::MergeRequested,
                &format!("merge abort context error: {err}"),
                Some(rejected_event_id),
                None,
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} rejected but abort execution failed (missing git context): {err}"
            ));
        }
    };

    let mut binding = match build_worktree_binding(&worktree_projection) {
        Ok(binding) => binding,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.reject",
                "merge.execution_failed",
                MergeState::MergeRequested,
                MergeState::MergeRequested,
                &format!("merge abort context error: {err}"),
                Some(rejected_event_id),
                None,
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} rejected but abort execution failed (invalid worktree context): {err}"
            ));
        }
    };
    let mut intent = match build_merge_intent(&merge, binding.id) {
        Ok(intent) => intent,
        Err(err) => {
            append_merge_execution_event(
                store,
                &merge,
                "merge.reject",
                "merge.execution_failed",
                MergeState::MergeRequested,
                MergeState::MergeRequested,
                &format!("merge abort context error: {err}"),
                Some(rejected_event_id),
                Some(&binding),
                None,
                None,
            )?;
            return Ok(format!(
                "Merge intent {merge_id} rejected but abort execution failed (invalid merge context): {err}"
            ));
        }
    };

    let orchestrator = LocalMergeOrchestrator::new(LocalWorktreeManager::new());
    let cancel = CancellationToken::new();
    if let Err(err) =
        block_on_current_runtime(orchestrator.abort(&mut intent, &mut binding, reason, cancel))?
    {
        append_merge_execution_event(
            store,
            &merge,
            "merge.reject",
            "merge.execution_failed",
            MergeState::MergeRequested,
            intent.state,
            &format!("merge abort failed: {err}"),
            Some(rejected_event_id),
            Some(&binding),
            None,
            None,
        )?;
        return Ok(format!(
            "Merge intent {merge_id} rejected but abort execution failed: {err}"
        ));
    }

    append_merge_execution_event(
        store,
        &merge,
        "merge.reject",
        "merge.execution_succeeded",
        MergeState::MergeRequested,
        intent.state,
        "merge abort orchestration completed successfully",
        Some(rejected_event_id),
        Some(&binding),
        intent.result_sha.as_deref(),
        None,
    )?;

    Ok(format!(
        "Merge intent {merge_id} transitioned MergeRequested -> MergeAborted (reason: {reason})."
    ))
}

/// `yarli run status` — print current run/task state.
pub(crate) fn cmd_run_status(run_id_str: &str) -> Result<()> {
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| {
        let run_id = resolve_run_id_input(store, run_id_str)?;
        render_run_status(store, run_id)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli run list` — list all known runs.
pub(crate) fn cmd_run_list() -> Result<()> {
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, render_run_list)?;
    println!("{output}");
    Ok(())
}

pub(crate) fn list_runs_by_latest_state(
    store: &dyn EventStore,
) -> Result<BTreeMap<Uuid, RunState>> {
    let run_events = query_events(store, &EventQuery::by_entity_type(EntityType::Run))?;
    let mut runs: BTreeMap<Uuid, RunState> = BTreeMap::new();
    for event in run_events {
        let Ok(run_id) = Uuid::parse_str(&event.entity_id) else {
            continue;
        };
        let entry = runs.entry(run_id).or_insert(RunState::RunOpen);
        if let Some(state) = run_state_from_event(&event) {
            *entry = state;
        }
    }
    Ok(runs)
}

pub(crate) fn render_run_candidates(run_ids: &[Uuid]) -> String {
    let strings = run_ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    let prefixes = unique_run_id_prefixes(strings.clone(), 10);
    strings
        .iter()
        .map(|id| {
            prefixes
                .get(id)
                .cloned()
                .unwrap_or_else(|| compact_run_id(id))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn select_run_targets_for_control(
    store: &dyn EventStore,
    run_id_input: Option<&str>,
    all_selected: bool,
    eligible_states: &[RunState],
    all_flag_name: &str,
    action_name: &str,
) -> Result<Vec<Uuid>> {
    let runs = list_runs_by_latest_state(store)?;
    if let Some(raw_run_id) = run_id_input {
        let run_id = resolve_run_id_input(store, raw_run_id)?;
        let state = runs
            .get(&run_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Run {run_id} not found in persisted event log."))?;
        if !eligible_states.contains(&state) {
            bail!(
                "run {run_id} is {:?}; cannot {action_name}. Eligible states: {}",
                state,
                eligible_states
                    .iter()
                    .map(|s| format!("{s:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        return Ok(vec![run_id]);
    }

    let eligible = runs
        .iter()
        .filter_map(|(run_id, state)| eligible_states.contains(state).then_some(*run_id))
        .collect::<Vec<_>>();

    if all_selected {
        if eligible.is_empty() {
            bail!("no eligible runs found for `{action_name}`");
        }
        return Ok(eligible);
    }

    match eligible.len() {
        0 => bail!("no eligible runs found for `{action_name}`"),
        1 => Ok(eligible),
        _ => bail!(
            "multiple eligible runs found for `{action_name}`; pass <run-id> or --{all_flag_name}. Candidates: {}",
            render_run_candidates(&eligible)
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn append_run_transition_event(
    store: &dyn EventStore,
    run_id: Uuid,
    correlation_id: Uuid,
    from: RunState,
    to: RunState,
    event_type: &str,
    reason: &str,
    idempotency_key: Option<String>,
) -> Result<()> {
    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Run,
            entity_id: run_id.to_string(),
            event_type: event_type.to_string(),
            payload: serde_json::json!({
                "from": format!("{:?}", from),
                "to": format!("{:?}", to),
                "reason": reason,
                "detail": reason,
            }),
            correlation_id,
            causation_id: None,
            actor: "cli.operator".to_string(),
            idempotency_key,
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn append_run_cancelled_transition_event(
    store: &dyn EventStore,
    run_id: Uuid,
    correlation_id: Uuid,
    from: RunState,
    reason: &str,
    cancellation_source: CancellationSource,
    provenance: &CancellationProvenance,
    task_id: Option<Uuid>,
    idempotency_key: Option<String>,
) -> Result<()> {
    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Run,
            entity_id: run_id.to_string(),
            event_type: "run.cancelled".to_string(),
            payload: serde_json::json!({
                "from": format!("{:?}", from),
                "to": format!("{:?}", RunState::RunCancelled),
                "reason": reason,
                "detail": reason,
                "exit_reason": yarli_core::domain::ExitReason::CancelledByOperator.to_string(),
                "cancellation_source": cancellation_source.to_string(),
                "cancellation_provenance": provenance,
            }),
            correlation_id,
            causation_id: None,
            actor: OPERATOR_CONTROL_ACTOR.to_string(),
            idempotency_key,
        },
    )?;
    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Run,
            entity_id: run_id.to_string(),
            event_type: "run.cancel_provenance".to_string(),
            payload: serde_json::json!({
                "run_id": run_id.to_string(),
                "task_id": task_id.map(|id| id.to_string()),
                "timestamp": chrono::Utc::now(),
                "cancellation_source": cancellation_source.to_string(),
                "signal_name": provenance.signal_name.clone(),
                "signal_number": provenance.signal_number,
                "sender_pid": provenance.sender_pid,
                "receiver_pid": provenance.receiver_pid,
                "parent_pid": provenance.parent_pid,
                "process_group_id": provenance.process_group_id,
                "session_id": provenance.session_id,
                "tty": provenance.tty.clone(),
                "actor_kind": provenance.actor_kind.map(|kind| kind.to_string()),
                "actor_detail": provenance.actor_detail.clone(),
                "stage": provenance.stage.map(|stage| stage.to_string()),
            }),
            correlation_id,
            causation_id: None,
            actor: OPERATOR_CONTROL_ACTOR.to_string(),
            idempotency_key: Some(format!("{run_id}:operator_cancel_provenance")),
        },
    )
}

pub(crate) fn append_task_cancelled_event(
    store: &dyn EventStore,
    run_id: Uuid,
    task: &TaskProjection,
    reason: &str,
    cancellation_source: CancellationSource,
    provenance: &CancellationProvenance,
) -> Result<()> {
    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Task,
            entity_id: task.task_id.to_string(),
            event_type: "task.cancelled".to_string(),
            payload: serde_json::json!({
                "from": format!("{:?}", task.state),
                "to": format!("{:?}", TaskState::TaskCancelled),
                "reason": reason,
                "detail": reason,
                "attempt_no": task.attempt_no,
                "cancellation_source": cancellation_source.to_string(),
                "cancellation_provenance": provenance,
            }),
            correlation_id: task.correlation_id,
            causation_id: None,
            actor: "cli.operator".to_string(),
            idempotency_key: Some(format!(
                "{}:operator_cancel:{}",
                task.task_id,
                task.attempt_no.unwrap_or(0)
            )),
        },
    )?;
    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Task,
            entity_id: task.task_id.to_string(),
            event_type: "task.cancel_provenance".to_string(),
            payload: serde_json::json!({
                "run_id": run_id.to_string(),
                "task_id": task.task_id.to_string(),
                "timestamp": chrono::Utc::now(),
                "cancellation_source": cancellation_source.to_string(),
                "signal_name": provenance.signal_name.clone(),
                "signal_number": provenance.signal_number,
                "sender_pid": provenance.sender_pid,
                "receiver_pid": provenance.receiver_pid,
                "parent_pid": provenance.parent_pid,
                "process_group_id": provenance.process_group_id,
                "session_id": provenance.session_id,
                "tty": provenance.tty.clone(),
                "actor_kind": provenance.actor_kind.map(|kind| kind.to_string()),
                "actor_detail": provenance.actor_detail.clone(),
                "stage": provenance.stage.map(|stage| stage.to_string()),
            }),
            correlation_id: task.correlation_id,
            causation_id: None,
            actor: "cli.operator".to_string(),
            idempotency_key: Some(format!(
                "{}:operator_cancel_provenance:{}",
                task.task_id,
                task.attempt_no.unwrap_or(0)
            )),
        },
    )
}

pub(crate) fn execute_run_pause_control(
    store: &dyn EventStore,
    run_id: Uuid,
    reason: &str,
) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };
    if run.state == RunState::RunBlocked {
        return Ok(format!("Run {run_id} is already paused (RunBlocked)."));
    }
    if !run.state.can_transition_to(RunState::RunBlocked) {
        return Ok(format!(
            "Run {run_id} is {:?}; pause is not valid from this state.",
            run.state
        ));
    }
    append_run_transition_event(
        store,
        run_id,
        run.correlation_id,
        run.state,
        RunState::RunBlocked,
        "run.blocked",
        reason,
        Some(format!("{run_id}:operator_pause")),
    )?;
    Ok(format!(
        "Run {run_id} transitioned {:?} -> RunBlocked (reason: {reason}).",
        run.state
    ))
}

pub(crate) fn execute_run_resume_control(
    store: &dyn EventStore,
    run_id: Uuid,
    reason: &str,
) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };
    if run.state == RunState::RunActive {
        return Ok(format!("Run {run_id} is already active."));
    }
    if !run.state.can_transition_to(RunState::RunActive) {
        return Ok(format!(
            "Run {run_id} is {:?}; resume is not valid from this state.",
            run.state
        ));
    }
    append_run_transition_event(
        store,
        run_id,
        run.correlation_id,
        run.state,
        RunState::RunActive,
        "run.activated",
        reason,
        Some(format!("{run_id}:operator_resume")),
    )?;
    Ok(format!(
        "Run {run_id} transitioned {:?} -> RunActive (reason: {reason}).",
        run.state
    ))
}

pub(crate) fn execute_run_cancel_control(
    store: &dyn EventStore,
    queue: &dyn TaskQueue,
    run_id: Uuid,
    reason: &str,
    cancellation_diagnostics: bool,
) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };
    if run.state.is_terminal() {
        return Ok(format!(
            "Run {run_id} is already terminal ({:?}).",
            run.state
        ));
    }
    if !run.state.can_transition_to(RunState::RunCancelled) {
        return Ok(format!(
            "Run {run_id} is {:?}; cancel is not valid from this state.",
            run.state
        ));
    }

    let mut cancelled_tasks = 0usize;
    let mut first_cancelled_task_id: Option<Uuid> = None;
    let mut run_provenance = default_cancellation_provenance(
        CancellationSource::Operator,
        Some("operator control-plane cancel command".to_string()),
    );
    if cancellation_diagnostics {
        run_provenance =
            with_cancellation_diagnostics(run_provenance, run_id, run.correlation_id, 0, reason);
    }
    for task in &run.tasks {
        if task.state.is_terminal() || !task.state.can_transition_to(TaskState::TaskCancelled) {
            continue;
        }
        let stage = cancellation_stage_for_task_projection(task);
        let mut task_provenance = run_provenance.clone();
        task_provenance.stage = Some(stage);
        append_task_cancelled_event(
            store,
            run_id,
            task,
            reason,
            CancellationSource::Operator,
            &task_provenance,
        )?;
        if first_cancelled_task_id.is_none() {
            first_cancelled_task_id = Some(task.task_id);
            run_provenance.stage = Some(stage);
        }
        cancelled_tasks += 1;
    }

    append_run_cancelled_transition_event(
        store,
        run_id,
        run.correlation_id,
        run.state,
        reason,
        CancellationSource::Operator,
        &run_provenance,
        first_cancelled_task_id,
        Some(format!("{run_id}:operator_cancelled")),
    )?;
    let queue_cancelled = queue.cancel_for_run(run_id)?;
    Ok(format!(
        "Run {run_id} transitioned {:?} -> RunCancelled (reason: {reason}); cancelled {cancelled_tasks} task(s), drained {queue_cancelled} queue entry(ies).",
        run.state
    ))
}

pub(crate) fn cmd_run_pause(run_id: Option<&str>, all_active: bool, reason: &str) -> Result<()> {
    let loaded_config = load_runtime_config_for_writes("run pause")?;
    let output = with_event_store_and_queue(&loaded_config, |store, _queue| {
        let targets = select_run_targets_for_control(
            store,
            run_id,
            all_active,
            &[RunState::RunActive, RunState::RunVerifying],
            "all-active",
            "pause",
        )?;
        let mut lines = Vec::new();
        for run_id in targets {
            lines.push(execute_run_pause_control(store, run_id, reason)?);
        }
        Ok(lines.join("\n"))
    })?;
    println!("{output}");
    Ok(())
}

pub(crate) fn cmd_run_resume(run_id: Option<&str>, all_paused: bool, reason: &str) -> Result<()> {
    let loaded_config = load_runtime_config_for_writes("run resume")?;
    let output = with_event_store_and_queue(&loaded_config, |store, _queue| {
        let targets = select_run_targets_for_control(
            store,
            run_id,
            all_paused,
            &[RunState::RunBlocked],
            "all-paused",
            "resume",
        )?;
        let mut lines = Vec::new();
        for run_id in targets {
            lines.push(execute_run_resume_control(store, run_id, reason)?);
        }
        Ok(lines.join("\n"))
    })?;
    println!("{output}");
    Ok(())
}

pub(crate) fn cmd_run_cancel(run_id: Option<&str>, all_active: bool, reason: &str) -> Result<()> {
    let loaded_config = load_runtime_config_for_writes("run cancel")?;
    let output = with_event_store_and_queue(&loaded_config, |store, queue| {
        let targets = if let Some(raw_run_id) = run_id {
            vec![resolve_run_id_input(store, raw_run_id)?]
        } else {
            select_run_targets_for_control(
                store,
                None,
                all_active,
                &[RunState::RunActive, RunState::RunVerifying],
                "all-active",
                "cancel",
            )?
        };
        let mut lines = Vec::new();
        for run_id in targets {
            lines.push(execute_run_cancel_control(
                store,
                queue,
                run_id,
                reason,
                loaded_config.config().ui.cancellation_diagnostics,
            )?);
        }
        Ok(lines.join("\n"))
    })?;
    println!("{output}");
    Ok(())
}

pub(crate) fn resolve_run_id_input(store: &dyn EventStore, run_id_input: &str) -> Result<Uuid> {
    let trimmed = run_id_input.trim();
    if trimmed.is_empty() {
        bail!("invalid run ID (expected UUID or unique run-list prefix)");
    }

    if let Ok(parsed) = Uuid::parse_str(trimmed) {
        return Ok(parsed);
    }

    let compact_input = trimmed
        .chars()
        .filter(|c| *c != '-')
        .collect::<String>()
        .to_ascii_lowercase();
    if compact_input.is_empty() {
        bail!("invalid run ID (expected UUID or unique run-list prefix)");
    }

    let run_events = query_events(store, &EventQuery::by_entity_type(EntityType::Run))?;
    let mut unique = HashSet::new();
    let mut run_ids = Vec::new();
    for event in &run_events {
        let Ok(run_id) = Uuid::parse_str(&event.entity_id) else {
            continue;
        };
        if unique.insert(run_id) {
            run_ids.push(run_id);
        }
    }

    let matches: Vec<Uuid> = run_ids
        .iter()
        .copied()
        .filter(|run_id| run_id.simple().to_string().starts_with(&compact_input))
        .collect();

    match matches.len() {
        1 => Ok(matches[0]),
        0 => bail!(
            "invalid run ID {:?} (expected UUID or unique run-list prefix)",
            run_id_input
        ),
        _ => {
            let sample = matches
                .iter()
                .take(5)
                .map(|run_id| run_id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "ambiguous run ID prefix {:?}; matches multiple runs: {}",
                run_id_input,
                sample
            );
        }
    }
}

/// `yarli run explain-exit` — run the Why Not Done? engine.
pub(crate) fn cmd_run_explain(run_id_str: &str) -> Result<()> {
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| {
        let run_id = resolve_run_id_input(store, run_id_str)?;
        render_run_explain(store, run_id)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli task list` — list tasks for a run.
pub(crate) fn cmd_task_list(run_id_str: &str) -> Result<()> {
    let run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_task_list(store, run_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli task explain` — run the Why Not Done? engine for one task.
pub(crate) fn cmd_task_explain(task_id_str: &str) -> Result<()> {
    let task_id: Uuid = task_id_str
        .parse()
        .context("invalid task ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_task_explain(store, task_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli task output` — dump captured command output for a task.
pub(crate) fn cmd_task_output(task_id_str: &str) -> Result<()> {
    let task_id: Uuid = task_id_str
        .parse()
        .context("invalid task ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_task_output(store, task_id))?;
    print!("{output}");
    Ok(())
}

/// `yarli info` — show version and terminal capabilities.
pub(crate) fn cmd_info(
    info: &TerminalInfo,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
) -> Result<()> {
    println!("yarli v{}", YARLI_VERSION);
    println!("  commit: {}", BUILD_COMMIT);
    println!("  date:   {}", BUILD_DATE);
    println!("  build:  {}", BUILD_ID);
    println!();
    println!("Terminal:");
    println!("  TTY:     {}", info.is_tty);
    println!("  Size:    {}x{}", info.cols, info.rows);
    println!("  Dashboard capable: {}", info.supports_dashboard());
    println!();
    println!("Render mode: {:?}", render_mode);
    println!("Backend: {}", loaded_config.config().core.backend.as_str());
    println!(
        "Execution runner: {:?}",
        loaded_config.config().execution.runner
    );
    println!(
        "Config:  {} ({})",
        loaded_config.path().display(),
        loaded_config.source().label()
    );
    Ok(())
}

/// `yarli task unblock` — clear a task's blocker.
pub(crate) fn cmd_task_unblock(task_id_str: &str, reason: &str) -> Result<()> {
    let task_id: Uuid = task_id_str
        .parse()
        .context("invalid task ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_writes("task unblock")?;
    let output = with_event_store(&loaded_config, |store| {
        execute_task_unblock(store, task_id, reason)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli task annotate` — add blocker detail to a task.
pub(crate) fn cmd_task_annotate(task_id_str: &str, detail: &str) -> Result<()> {
    let task_id: Uuid = task_id_str
        .parse()
        .context("invalid task ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_writes("task annotate")?;
    let output = with_event_store(&loaded_config, |store| {
        execute_task_annotate(store, task_id, detail)
    })?;
    println!("{output}");
    Ok(())
}

/// Execute the task annotate operation.
pub(crate) fn execute_task_annotate(
    store: &dyn EventStore,
    task_id: Uuid,
    detail: &str,
) -> Result<String> {
    use yarli_core::domain::EntityType;

    // Verify the task exists.
    let task_events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Task, task_id.to_string()),
    )?;
    if task_events.is_empty() {
        return Ok(format!("Task {task_id} not found in persisted event log."));
    }

    // Persist the annotation event.
    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: chrono::Utc::now(),
        entity_type: EntityType::Task,
        entity_id: task_id.to_string(),
        event_type: "task.annotated".to_string(),
        payload: serde_json::json!({
            "blocker_detail": detail,
        }),
        correlation_id: task_events
            .last()
            .map(|e| e.correlation_id)
            .unwrap_or_else(Uuid::now_v7),
        causation_id: None,
        actor: "operator".to_string(),
        idempotency_key: None,
    };

    store
        .append(event)
        .context("failed to persist annotation event")?;

    Ok(format!(
        "Task {task_id} annotated with blocker detail: {detail}"
    ))
}

/// `yarli gate list` — show configured gate types.
pub(crate) fn cmd_gate_list(run_level: bool) -> Result<()> {
    if run_level {
        println!("Run-level verification gates:");
        for gate in default_run_gates() {
            println!("  {} {}", gate_status_glyph(), gate.label());
        }
    } else {
        println!("Task-level verification gates:");
        for gate in default_task_gates() {
            println!("  {} {}", gate_status_glyph(), gate.label());
        }
    }
    println!();
    println!("All gates must pass for verification to succeed.");
    println!("Use `yarli gate rerun <task-id>` to re-evaluate gates for a specific task.");
    Ok(())
}

/// `yarli gate rerun` — re-run gate evaluation for a task.
pub(crate) fn cmd_gate_rerun(task_id_str: &str, gate_name: Option<&str>) -> Result<()> {
    let task_id: Uuid = task_id_str
        .parse()
        .context("invalid task ID (expected UUID)")?;
    if let Some(name) = gate_name {
        // Validate the gate name.
        if parse_gate_type(name).is_none() {
            bail!(
                "unknown gate: {name}. Valid gates: {}",
                all_gate_names().join(", ")
            );
        }
    }

    let loaded_config = load_runtime_config_for_writes("gate rerun")?;
    let output = with_event_store(&loaded_config, |store| {
        execute_gate_rerun(store, task_id, gate_name)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli worktree status` — show worktree state.
pub(crate) fn cmd_worktree_status(run_id_str: &str) -> Result<()> {
    let run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| {
        render_worktree_status(store, run_id)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli worktree recover` — recover from interrupted git operation.
pub(crate) fn cmd_worktree_recover(worktree_id_str: &str, action: &str) -> Result<()> {
    let worktree_id: Uuid = worktree_id_str
        .parse()
        .context("invalid worktree ID (expected UUID)")?;
    match action {
        "abort" | "resume" | "manual-block" => {}
        _ => bail!("invalid recovery action: {action}. Use: abort, resume, or manual-block"),
    }
    let loaded_config = load_runtime_config_for_writes("worktree recover")?;
    let output = with_event_store(&loaded_config, |store| {
        execute_worktree_recover(store, worktree_id, action)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli merge request` — create a merge intent.
pub(crate) fn cmd_merge_request(
    source: &str,
    target: &str,
    run_id_str: &str,
    strategy: &str,
) -> Result<()> {
    let run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    let strategy = parse_merge_strategy(strategy).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid merge strategy: {strategy}. Use: merge-no-ff, rebase-then-ff, or squash-merge"
        )
    })?;

    let loaded_config = load_runtime_config_for_writes("merge request")?;
    let output = with_event_store(&loaded_config, |store| {
        execute_merge_request(store, source, target, run_id, strategy)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli merge approve` — approve a merge intent.
pub(crate) fn cmd_merge_approve(merge_id_str: &str) -> Result<()> {
    let merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_writes("merge approve")?;
    let audit_sink = prepare_audit_sink(&loaded_config)?;
    let output = with_event_store(&loaded_config, |store| {
        execute_merge_approve(
            store,
            merge_id,
            loaded_config.config().core.safe_mode,
            loaded_config.config().policy.enforce_policies,
            audit_sink.as_ref().map(|sink| sink as &dyn AuditSink),
        )
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli merge reject` — reject a merge intent.
pub(crate) fn cmd_merge_reject(merge_id_str: &str, reason: &str) -> Result<()> {
    let merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_writes("merge reject")?;
    let audit_sink = prepare_audit_sink(&loaded_config)?;
    let output = with_event_store(&loaded_config, |store| {
        execute_merge_reject(
            store,
            merge_id,
            reason,
            loaded_config.config().core.safe_mode,
            loaded_config.config().policy.enforce_policies,
            audit_sink.as_ref().map(|sink| sink as &dyn AuditSink),
        )
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli merge status` — show merge intent status.
pub(crate) fn cmd_merge_status(merge_id_str: &str) -> Result<()> {
    let merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_merge_status(store, merge_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli audit tail` — tail the JSONL audit log.
pub(crate) fn cmd_audit_tail(file: &str, lines: usize, category: Option<&str>) -> Result<()> {
    let path = PathBuf::from(file);
    let sink = JsonlAuditSink::new(&path);

    let entries = match sink.read_all() {
        Ok(entries) => entries,
        Err(e) => {
            if path.exists() {
                bail!("failed to read audit log {}: {e}", path.display());
            }
            println!("No audit log found at {}.", path.display());
            println!("Audit entries are written during `yarli run start` when policy decisions are made.");
            return Ok(());
        }
    };

    // Filter by category if requested.
    let filtered: Vec<_> = if let Some(cat) = category {
        entries
            .into_iter()
            .filter(|e| {
                let cat_str = format!("{:?}", e.category);
                cat_str.eq_ignore_ascii_case(cat)
            })
            .collect()
    } else {
        entries
    };

    if filtered.is_empty() {
        println!("No audit entries found.");
        return Ok(());
    }

    // Take the last N entries.
    let display: &[_] = if lines == 0 || lines >= filtered.len() {
        &filtered
    } else {
        &filtered[filtered.len() - lines..]
    };

    println!(
        "Audit log: {} entries (showing {})",
        filtered.len(),
        display.len()
    );
    println!();

    for entry in display {
        let ts = entry.timestamp.format("%Y-%m-%d %H:%M:%S");
        let cat = format!("{:?}", entry.category);
        let outcome_str = entry
            .outcome
            .as_ref()
            .map(|o| format!(" [{o:?}]"))
            .unwrap_or_default();
        println!("{ts}  {cat:<24}{outcome_str}");
        println!("  actor:  {}", entry.actor);
        println!("  action: {}", entry.action);
        if !entry.reason.is_empty() {
            println!("  reason: {}", entry.reason);
        }
        if let Some(ref rule_id) = entry.rule_id {
            println!("  rule:   {rule_id}");
        }
        if let Some(ref run_id) = entry.run_id {
            println!("  run:    {run_id}");
        }
        if let Some(ref task_id) = entry.task_id {
            println!("  task:   {task_id}");
        }
        if !entry.details.is_null() && entry.details != serde_json::json!({}) {
            let details = serde_json::to_string_pretty(&entry.details)
                .unwrap_or_else(|_| entry.details.to_string());
            println!("  details: {details}");
        }
        println!();
    }

    Ok(())
}

#[derive(Debug)]
struct ParsedAuditQuery {
    run_id: Option<Uuid>,
    task_id: Option<Uuid>,
    category: Option<AuditCategory>,
    actor: Option<String>,
    since: Option<chrono::DateTime<chrono::Utc>>,
    before: Option<chrono::DateTime<chrono::Utc>>,
    after: Option<Uuid>,
    offset: usize,
    limit: usize,
    format: AuditOutputFormat,
}

fn parse_audit_category(category: &str) -> Result<AuditCategory> {
    let normalized = category
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(" ", "");

    match normalized.as_str() {
        "policy_decision" => Ok(AuditCategory::PolicyDecision),
        "destructive_attempt" => Ok(AuditCategory::DestructiveAttempt),
        "token_consumed" => Ok(AuditCategory::TokenConsumed),
        "gate_evaluation" => Ok(AuditCategory::GateEvaluation),
        "command_execution" => Ok(AuditCategory::CommandExecution),
        _ => bail!("invalid category '{category}'"),
    }
}

fn parse_audit_time(value: &str, label: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(ts.with_timezone(&chrono::Utc));
    }

    if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow::anyhow!("{label}: invalid timestamp '{value}'"))?;
        return Ok(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
            midnight,
            chrono::Utc,
        ));
    }

    bail!("{label} must be RFC3339 timestamp or YYYY-MM-DD date (got '{value}')")
}

#[allow(clippy::too_many_arguments)]
fn parse_audit_filters(
    run_id: Option<&str>,
    task_id: Option<&str>,
    category: Option<&str>,
    actor: Option<&str>,
    since: Option<&str>,
    before: Option<&str>,
    after: Option<&str>,
    offset: usize,
    limit: usize,
    format: AuditOutputFormat,
) -> Result<ParsedAuditQuery> {
    let run_id = match run_id {
        Some(value) => Some(
            value
                .parse::<Uuid>()
                .with_context(|| format!("invalid run-id '{value}', expected UUID"))?,
        ),
        None => None,
    };
    let task_id = match task_id {
        Some(value) => Some(
            value
                .parse::<Uuid>()
                .with_context(|| format!("invalid task-id '{value}', expected UUID"))?,
        ),
        None => None,
    };
    let category = match category {
        Some(value) => Some(parse_audit_category(value)?),
        None => None,
    };
    let actor = actor.map(str::to_string);
    let since = since
        .map(|value| parse_audit_time(value, "since"))
        .transpose()?;
    let before = before
        .map(|value| parse_audit_time(value, "before"))
        .transpose()?;
    let after = match after {
        Some(value) => Some(
            value
                .parse::<Uuid>()
                .with_context(|| format!("invalid after '{value}', expected UUID"))?,
        ),
        None => None,
    };

    Ok(ParsedAuditQuery {
        run_id,
        task_id,
        category,
        actor,
        since,
        before,
        after,
        offset,
        limit,
        format,
    })
}

fn query_audit_entries(path: &Path, query: &ParsedAuditQuery) -> Result<Vec<AuditEntry>> {
    let sink = JsonlAuditSink::new(path);
    let entries = match sink.read_all() {
        Ok(entries) => entries,
        Err(e) => bail!("failed to read audit log {}: {e}", path.display()),
    };

    let mut filtered: Vec<AuditEntry> = entries
        .into_iter()
        .filter(|entry| {
            if let Some(run_id) = query.run_id {
                if entry.run_id != Some(run_id) {
                    return false;
                }
            }
            if let Some(task_id) = query.task_id {
                if entry.task_id != Some(task_id) {
                    return false;
                }
            }
            if let Some(category) = query.category {
                if entry.category != category {
                    return false;
                }
            }
            if let Some(actor) = query.actor.as_deref() {
                if entry.actor != actor {
                    return false;
                }
            }
            if let Some(since) = query.since {
                if entry.timestamp < since {
                    return false;
                }
            }
            if let Some(before) = query.before {
                if entry.timestamp > before {
                    return false;
                }
            }
            true
        })
        .collect();

    let start = if let Some(after) = query.after {
        filtered
            .iter()
            .position(|entry| entry.audit_id == after)
            .map(|idx| idx.saturating_add(1))
            .unwrap_or(0)
            .saturating_add(query.offset)
    } else {
        query.offset
    };
    let start = start.min(filtered.len());
    let end = if query.limit == 0 {
        filtered.len()
    } else {
        (start + query.limit).min(filtered.len())
    };

    filtered.truncate(end);
    if start > 0 {
        filtered.drain(0..start);
    }

    Ok(filtered)
}

fn csv_escape(value: &str) -> String {
    let mut out = value.replace('\"', "\"\"");
    if out.contains(',') || out.contains('\n') || out.contains('\r') || out.contains('\"') {
        out = format!("\"{out}\"");
    }
    out
}

fn render_audit_json_lines(entries: &[AuditEntry]) -> Result<()> {
    for entry in entries {
        let line = serde_json::to_string(entry)?;
        println!("{line}");
    }
    Ok(())
}

fn render_audit_csv(entries: &[AuditEntry]) -> String {
    let mut out = String::new();
    out.push_str(
        "audit_id,timestamp,category,actor,action,outcome,rule_id,run_id,task_id,reason\n",
    );
    for entry in entries {
        let line = format!(
            "{},{},{},{},{},{},{},{},{},{}\n",
            csv_escape(&entry.audit_id.to_string()),
            csv_escape(&entry.timestamp.to_rfc3339()),
            csv_escape(&format!("{:?}", entry.category)),
            csv_escape(&entry.actor),
            csv_escape(&entry.action),
            csv_escape(&entry.outcome.map(|o| format!("{o:?}")).unwrap_or_default()),
            csv_escape(entry.rule_id.as_deref().unwrap_or("")),
            csv_escape(
                &entry
                    .run_id
                    .map(|run_id| run_id.to_string())
                    .unwrap_or_else(String::new),
            ),
            csv_escape(
                &entry
                    .task_id
                    .map(|task_id| task_id.to_string())
                    .unwrap_or_else(String::new),
            ),
            csv_escape(&entry.reason),
        );
        out.push_str(&line);
    }
    out
}

fn render_audit_table(entries: &[AuditEntry]) {
    if entries.is_empty() {
        println!("No audit entries found.");
        return;
    }

    println!(
        "{:<20} {:<20} {:<16} {:<24} {:<8} {:<36} {:<36} REASON",
        "TIMESTAMP", "CATEGORY", "ACTOR", "ACTION", "OUTCOME", "RUN ID", "TASK ID",
    );
    for entry in entries {
        let outcome = entry.outcome.map(|o| format!("{o:?}")).unwrap_or_default();
        let run_id = entry
            .run_id
            .map(|value| value.to_string())
            .unwrap_or_else(String::new);
        let task_id = entry
            .task_id
            .map(|value| value.to_string())
            .unwrap_or_else(String::new);
        println!(
            "{:<20} {:<20} {:<16} {:<24} {:<8} {:<36} {:<36} {}",
            entry.timestamp.format("%Y-%m-%d %H:%M:%S"),
            format!("{:?}", entry.category),
            entry.actor,
            entry.action,
            outcome,
            run_id,
            task_id,
            entry.reason
        );
    }
}

/// `yarli audit query` — query the JSONL audit log with filters.
#[allow(clippy::too_many_arguments)]
pub(crate) fn cmd_audit_query(
    file: &str,
    run_id: Option<&str>,
    task_id: Option<&str>,
    category: Option<&str>,
    actor: Option<&str>,
    since: Option<&str>,
    before: Option<&str>,
    after: Option<&str>,
    offset: usize,
    limit: usize,
    format: AuditOutputFormat,
) -> Result<()> {
    let query = parse_audit_filters(
        run_id, task_id, category, actor, since, before, after, offset, limit, format,
    )?;
    let path = PathBuf::from(file);
    let entries = query_audit_entries(&path, &query)?;

    if entries.is_empty() {
        println!("No audit entries found.");
        return Ok(());
    }

    match query.format {
        AuditOutputFormat::Json => render_audit_json_lines(&entries),
        AuditOutputFormat::Csv => {
            let output = render_audit_csv(&entries);
            print!("{output}");
            Ok(())
        }
        AuditOutputFormat::Table => {
            render_audit_table(&entries);
            Ok(())
        }
    }
}

/// `yarli debug queue-depth` — show queue depth grouped by run/class.
pub(crate) fn cmd_debug_queue_depth() -> Result<()> {
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store_and_queue(&loaded_config, |_, queue| {
        let entries = queue.entries();
        Ok::<_, anyhow::Error>(render_queue_depth(&entries))
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli debug active-leases` — show currently leased tasks.
pub(crate) fn cmd_debug_active_leases() -> Result<()> {
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store_and_queue(&loaded_config, |_, queue| {
        let entries = queue.entries();
        let now = chrono::Utc::now();
        Ok::<_, anyhow::Error>(render_active_leases(&entries, now))
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli debug resource-usage` — show run resource usage totals.
pub(crate) fn cmd_debug_resource_usage(run_id_str: &str) -> Result<()> {
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| {
        let run_id = resolve_run_id_input(store, run_id_str)?;
        let run = match load_run_projection(store, run_id)? {
            Some(run) => run,
            None => return Ok(format!("run {run_id} not found in persisted event log")),
        };
        let totals = collect_run_resource_totals(store, run.correlation_id)?;
        let budgets = collect_run_resource_limits(store, run.correlation_id)?;
        Ok::<_, anyhow::Error>(render_resource_usage_summary(run_id, &totals, &budgets))
    })?;
    println!("{output}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn gate_status_glyph() -> &'static str {
    "○"
}

pub(crate) fn all_gate_names() -> Vec<&'static str> {
    vec![
        "required_tasks_closed",
        "required_evidence_present",
        "tests_passed",
        "no_unapproved_git_ops",
        "no_unresolved_conflicts",
        "worktree_consistent",
        "policy_clean",
    ]
}

pub(crate) fn parse_gate_type(name: &str) -> Option<GateType> {
    match name {
        "required_tasks_closed" => Some(GateType::RequiredTasksClosed),
        "required_evidence_present" => Some(GateType::RequiredEvidencePresent),
        "tests_passed" => Some(GateType::TestsPassed),
        "no_unapproved_git_ops" => Some(GateType::NoUnapprovedGitOps),
        "no_unresolved_conflicts" => Some(GateType::NoUnresolvedConflicts),
        "worktree_consistent" => Some(GateType::WorktreeConsistent),
        "policy_clean" => Some(GateType::PolicyClean),
        _ => None,
    }
}

pub(crate) fn parse_merge_strategy(name: &str) -> Option<&'static str> {
    match name {
        "merge-no-ff" => Some("merge-no-ff"),
        "rebase-then-ff" => Some("rebase-then-ff"),
        "squash-merge" => Some("squash-merge"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use crate::config::{AutoAdvancePolicy, UiMode};
    use crate::projection::*;
    use crate::test_helpers::*;
    use chrono::Utc;
    use clap::CommandFactory;
    use std::path::Path;
    use std::process::Command;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::{NamedTempFile, TempDir};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;
    use yarli_cli::mode::{RenderMode, TerminalInfo};
    use yarli_core::domain::{
        CancellationActorKind, CancellationSource, CommandClass, EntityType, Event, SafeMode,
    };
    use yarli_core::entities::run::Run;
    use yarli_core::entities::task::Task;
    use yarli_core::entities::worktree_binding::WorktreeBinding;
    use yarli_core::explain::GateType;
    use yarli_core::fsm::run::RunState;
    use yarli_core::fsm::task::TaskState;
    use yarli_core::shutdown::ShutdownController;
    use yarli_exec::LocalCommandRunner;
    use yarli_gates::default_task_gates;
    use yarli_git::{LocalWorktreeManager, WorktreeManager};
    use yarli_observability::{AuditCategory, AuditEntry, InMemoryAuditSink, JsonlAuditSink};
    use yarli_queue::{InMemoryTaskQueue, SchedulerConfig, TaskQueue};
    use yarli_store::event_store::EventQuery;
    use yarli_store::InMemoryEventStore;

    fn run_git(repo: &Path, args: &[&str]) -> (bool, String, String) {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .expect("git command should run");
        (
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    }

    fn run_git_expect_ok(repo: &Path, args: &[&str]) {
        let (ok, _stdout, stderr) = run_git(repo, args);
        assert!(ok, "git {:?} failed: {stderr}", args);
    }

    fn seed_worktree_event_payload(
        binding: &WorktreeBinding,
        from: &str,
        to: &str,
        reason: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "from": from,
            "to": to,
            "run_id": binding.run_id,
            "task_id": binding.task_id,
            "repo_root": binding.repo_root.display().to_string(),
            "worktree_path": binding.worktree_path.display().to_string(),
            "branch_name": binding.branch_name.clone(),
            "base_ref": binding.base_ref.clone(),
            "head_ref": binding.head_ref.clone(),
            "submodule_mode": "locked",
            "reason": reason,
        })
    }

    fn create_merge_fixture(
        conflict: bool,
    ) -> (TempDir, Uuid, Uuid, String, String, WorktreeBinding) {
        let temp_dir = TempDir::new().unwrap();
        let repo = temp_dir.path();
        run_git_expect_ok(repo, &["init"]);
        run_git_expect_ok(repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(repo.join("shared.txt"), "base\n").unwrap();
        run_git_expect_ok(repo, &["add", "."]);
        run_git_expect_ok(repo, &["commit", "-m", "initial commit"]);

        let source_branch = "feature/test-merge".to_string();
        let target_branch = "main".to_string();
        run_git_expect_ok(repo, &["checkout", "-b", &source_branch]);
        std::fs::write(repo.join("shared.txt"), "feature change\n").unwrap();
        run_git_expect_ok(repo, &["add", "shared.txt"]);
        run_git_expect_ok(repo, &["commit", "-m", "feature change"]);
        run_git_expect_ok(repo, &["checkout", &target_branch]);

        if conflict {
            std::fs::write(repo.join("shared.txt"), "main conflicting change\n").unwrap();
            run_git_expect_ok(repo, &["add", "shared.txt"]);
            run_git_expect_ok(repo, &["commit", "-m", "main conflict change"]);
        } else {
            std::fs::write(repo.join("main-only.txt"), "main only\n").unwrap();
            run_git_expect_ok(repo, &["add", "main-only.txt"]);
            run_git_expect_ok(repo, &["commit", "-m", "main baseline change"]);
        }

        let run_id = Uuid::now_v7();
        let correlation_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let mut binding = WorktreeBinding::new(
            run_id,
            repo,
            format!("yarl/{}/merge-task", &run_id.to_string()[..8]),
            "main",
            correlation_id,
        )
        .with_task(task_id);
        let manager = LocalWorktreeManager::new();
        block_on_current_runtime(manager.create(&mut binding, CancellationToken::new()))
            .unwrap()
            .unwrap();

        if conflict {
            let (ok, _stdout, _stderr) =
                run_git(&binding.worktree_path, &["merge", &source_branch]);
            assert!(!ok, "expected merge conflict to produce interrupted state");
        }

        (
            temp_dir,
            run_id,
            correlation_id,
            source_branch,
            target_branch,
            binding,
        )
    }

    fn merge_id_from_output(output: &str) -> Uuid {
        output
            .lines()
            .find_map(|line| line.strip_prefix("Merge ID: "))
            .and_then(|raw| raw.trim().parse::<Uuid>().ok())
            .expect("expected merge ID line in output")
    }

    async fn setup_drive_scheduler_fixture(
        command: &str,
    ) -> (
        Scheduler<InMemoryTaskQueue, InMemoryEventStore, LocalCommandRunner>,
        Arc<InMemoryEventStore>,
        Uuid,
        Uuid,
        Vec<(Uuid, String)>,
    ) {
        let store = Arc::new(InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
        let runner = Arc::new(LocalCommandRunner::new());
        let scheduler = Scheduler::new(queue, store.clone(), runner, SchedulerConfig::default());

        let run = Run::new("drive cancel", yarli_core::domain::SafeMode::Execute);
        let run_id = run.id;
        let correlation_id = run.correlation_id;
        let task = Task::new(run_id, "task-1", command, CommandClass::Io, correlation_id);
        let task_names = vec![(task.id, "task-1".to_string())];
        scheduler.submit_run(run, vec![task]).await.unwrap();

        (scheduler, store, run_id, correlation_id, task_names)
    }

    #[tokio::test]
    async fn postgres_sync_signature_changes_only_on_state_transitions() {
        let (scheduler, _store, run_id, _correlation_id, _task_names) =
            setup_drive_scheduler_fixture("true").await;

        let initial = build_postgres_sync_signature(&scheduler, run_id)
            .await
            .expect("initial signature should exist");
        let repeated = build_postgres_sync_signature(&scheduler, run_id)
            .await
            .expect("signature should remain available");
        assert_eq!(
            initial, repeated,
            "signature should be stable when state is unchanged"
        );

        scheduler.tick().await.unwrap();

        let after_tick = build_postgres_sync_signature(&scheduler, run_id)
            .await
            .expect("signature should exist after tick");
        assert_ne!(
            initial, after_tick,
            "signature should change after run/task state transitions"
        );
    }

    #[test]
    fn run_help_mentions_prompt_resolution_precedence() {
        let mut cmd = Cli::command();
        let run = cmd
            .find_subcommand_mut("run")
            .expect("run subcommand should exist");
        let mut help = Vec::new();
        run.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();
        let help_lower = help.to_ascii_lowercase();
        assert!(
            help.contains("--prompt-file")
                && help.contains("[run].prompt_file")
                && help.contains("PROMPT.md")
                && help.contains("yarli run")
                && help.contains("no subcommand")
                && help_lower.contains("built-in yarli policy gates")
                && help_lower.contains("verification command chain")
                && help_lower.contains("observer events are telemetry only")
                && help_lower.contains("pause|resume|cancel"),
            "run --help should mention prompt resolution precedence"
        );
    }

    #[test]
    fn resolve_render_mode_uses_configured_ui_mode_when_flags_absent() {
        let tty_large = TerminalInfo {
            is_tty: true,
            cols: 120,
            rows: 40,
        };
        assert_eq!(
            resolve_render_mode(&tty_large, false, false, UiMode::Stream).unwrap(),
            RenderMode::Stream
        );
        assert_eq!(
            resolve_render_mode(&tty_large, false, false, UiMode::Tui).unwrap(),
            RenderMode::Dashboard
        );
    }

    #[test]
    fn resolve_render_mode_cli_flags_override_configured_ui_mode() {
        let tty_large = TerminalInfo {
            is_tty: true,
            cols: 120,
            rows: 40,
        };
        assert_eq!(
            resolve_render_mode(&tty_large, true, false, UiMode::Tui).unwrap(),
            RenderMode::Stream
        );
        assert_eq!(
            resolve_render_mode(&tty_large, false, true, UiMode::Stream).unwrap(),
            RenderMode::Dashboard
        );
    }

    #[test]
    fn root_help_mentions_prompt_default_behavior() {
        let mut cmd = Cli::command();
        let mut help = Vec::new();
        cmd.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();
        assert!(
            help.contains("Default workflow")
                && help.contains("PROMPT.md")
                && help.contains("yarli run"),
            "yarli --help should mention PROMPT.md default execution behavior"
        );
    }

    #[test]
    fn version_includes_build_provenance_fields() {
        let cmd = Cli::command();
        let version = cmd
            .get_version()
            .expect("version should be set on root command");
        assert!(version.contains("commit "));
        assert!(version.contains("date "));
        assert!(version.contains("build "));
    }

    #[test]
    fn incremental_event_cursor_reads_only_new_events() {
        let store = InMemoryEventStore::new();
        let corr = Uuid::now_v7();
        let mut first_batch_ids = Vec::new();
        for i in 0..5 {
            let event = make_event(
                EntityType::Run,
                format!("run-{i}"),
                "run.activated",
                corr,
                serde_json::json!({"to":"RunActive"}),
            );
            first_batch_ids.push(event.event_id);
            store.append(event).unwrap();
        }

        let mut cursor = IncrementalEventCursor::new(EventQuery::by_correlation(corr), 2);
        let first = cursor.read_new_events(&store).unwrap();
        assert_eq!(first.len(), 5);
        assert_eq!(first[0].event_id, first_batch_ids[0]);
        assert_eq!(first[4].event_id, first_batch_ids[4]);

        let second = cursor.read_new_events(&store).unwrap();
        assert!(second.is_empty());

        let mut second_batch_ids = Vec::new();
        for i in 0..2 {
            let event = make_event(
                EntityType::Run,
                format!("run-late-{i}"),
                "run.verifying",
                corr,
                serde_json::json!({"to":"RunVerifying"}),
            );
            second_batch_ids.push(event.event_id);
            store.append(event).unwrap();
        }

        let third = cursor.read_new_events(&store).unwrap();
        assert_eq!(third.len(), 2);
        assert_eq!(third[0].event_id, second_batch_ids[0]);
        assert_eq!(third[1].event_id, second_batch_ids[1]);
    }

    #[test]
    fn execute_task_unblock_appends_unblocked_event() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.blocked",
                corr,
                serde_json::json!({ "from": "TaskReady", "to": "TaskBlocked", "reason": "dependency pending" }),
            ))
            .unwrap();

        let output = execute_task_unblock(&store, task_id, "manual override").unwrap();
        assert!(output.contains("TaskBlocked -> TaskReady"));

        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Task,
                task_id.to_string(),
            ))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "task.unblocked"),
            "expected task.unblocked event to be persisted"
        );
    }

    #[test]
    fn parse_gate_type_valid_names() {
        assert_eq!(
            parse_gate_type("required_tasks_closed"),
            Some(GateType::RequiredTasksClosed)
        );
        assert_eq!(parse_gate_type("tests_passed"), Some(GateType::TestsPassed));
        assert_eq!(parse_gate_type("policy_clean"), Some(GateType::PolicyClean));
    }

    #[test]
    fn parse_gate_type_unknown_returns_none() {
        assert_eq!(parse_gate_type("nonexistent"), None);
        assert_eq!(parse_gate_type(""), None);
    }

    #[test]
    fn all_gate_names_covers_all_gate_types() {
        let names = all_gate_names();
        assert_eq!(names.len(), 7);
        for name in &names {
            assert!(
                parse_gate_type(name).is_some(),
                "gate name {name} should parse to a GateType"
            );
        }
    }

    #[test]
    fn cmd_task_unblock_rejects_invalid_uuid() {
        let result = cmd_task_unblock("not-a-uuid", "test reason");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_task_unblock_blocks_in_memory_writes_by_default() {
        let result = cmd_task_unblock(VALID_UUID, "test reason");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn cmd_run_status_rejects_invalid_uuid() {
        let result = cmd_run_status("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_run_explain_rejects_invalid_uuid() {
        let result = cmd_run_explain("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_task_list_rejects_invalid_uuid() {
        let result = cmd_task_list("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_task_explain_rejects_invalid_uuid() {
        let result = cmd_task_explain("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_worktree_status_rejects_invalid_uuid() {
        let result = cmd_worktree_status("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_merge_approve_rejects_invalid_uuid() {
        let result = cmd_merge_approve("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_merge_reject_rejects_invalid_uuid() {
        let result = cmd_merge_reject("bad", "reason");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_merge_status_rejects_invalid_uuid() {
        let result = cmd_merge_status("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_gate_list_task_level() {
        assert!(cmd_gate_list(false).is_ok());
    }

    #[test]
    fn cmd_gate_list_run_level() {
        assert!(cmd_gate_list(true).is_ok());
    }

    #[test]
    fn cmd_gate_rerun_rejects_invalid_uuid() {
        let result = cmd_gate_rerun("not-uuid", None);
        assert!(result.is_err());
    }

    #[test]
    fn cmd_gate_rerun_rejects_unknown_gate() {
        let result = cmd_gate_rerun(VALID_UUID, Some("nonexistent_gate"));
        assert!(result.is_err());
    }

    #[test]
    fn cmd_gate_rerun_accepts_valid_gate() {
        let result = cmd_gate_rerun(VALID_UUID, Some("tests_passed"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn cmd_gate_rerun_blocks_in_memory_writes_by_default() {
        let result = cmd_gate_rerun(VALID_UUID, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn execute_gate_rerun_persists_single_gate_evaluation() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let command_id = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.verifying",
                corr,
                serde_json::json!({ "from": "TaskExecuting", "to": "TaskVerifying" }),
            ))
            .unwrap();
        store
            .append(Event {
                event_id: Uuid::now_v7(),
                occurred_at: Utc::now(),
                entity_type: EntityType::Command,
                entity_id: command_id.to_string(),
                event_type: "command.started".to_string(),
                payload: serde_json::json!({
                    "command": "cargo test",
                    "working_dir": "/tmp",
                    "command_class": "io",
                }),
                correlation_id: corr,
                causation_id: None,
                actor: "test".to_string(),
                idempotency_key: Some(format!("{task_id}:cmd:1:started")),
            })
            .unwrap();
        store
            .append(Event {
                event_id: Uuid::now_v7(),
                occurred_at: Utc::now(),
                entity_type: EntityType::Command,
                entity_id: command_id.to_string(),
                event_type: "command.exited".to_string(),
                payload: serde_json::json!({
                    "exit_code": 0,
                    "duration_ms": 42,
                    "state": "CmdExited",
                }),
                correlation_id: corr,
                causation_id: None,
                actor: "test".to_string(),
                idempotency_key: Some(format!("{task_id}:cmd:1:terminal")),
            })
            .unwrap();

        let output = execute_gate_rerun(&store, task_id, Some("tests_passed")).unwrap();
        assert!(output.contains("gate.tests_passed: PASS"));
        assert!(!output.contains("requires a persistent store"));

        let gate_events = store
            .query(&EventQuery::by_entity(
                EntityType::Gate,
                task_id.to_string(),
            ))
            .unwrap();
        assert_eq!(gate_events.len(), 1);
        assert_eq!(gate_events[0].event_type, "gate.evaluated");
        assert_eq!(gate_events[0].payload["gate"], "gate.tests_passed");
        assert_eq!(gate_events[0].payload["status"], "passed");
        let expected_gate_key = format!("{task_id}:gate_rerun:gate.tests_passed");
        assert_eq!(
            gate_events[0].idempotency_key.as_deref(),
            Some(expected_gate_key.as_str())
        );
    }

    #[test]
    fn execute_gate_rerun_persists_all_default_task_gates() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.verifying",
                corr,
                serde_json::json!({ "from": "TaskExecuting", "to": "TaskVerifying" }),
            ))
            .unwrap();

        let output = execute_gate_rerun(&store, task_id, None).unwrap();
        assert!(output.contains("Evaluated 5 gate(s):"));
        assert!(!output.contains("requires a persistent store"));

        let gate_events = store
            .query(&EventQuery::by_entity(
                EntityType::Gate,
                task_id.to_string(),
            ))
            .unwrap();
        assert_eq!(gate_events.len(), default_task_gates().len());
        assert!(gate_events
            .iter()
            .all(|event| event.event_type == "gate.evaluated"));
        assert!(gate_events.iter().all(|event| event
            .idempotency_key
            .as_deref()
            .is_some_and(|key| key.starts_with(&format!("{task_id}:gate_rerun:gate.")))));
    }

    #[test]
    fn cmd_worktree_recover_blocks_in_memory_writes_by_default() {
        let abort = cmd_worktree_recover(VALID_UUID, "abort");
        let resume = cmd_worktree_recover(VALID_UUID, "resume");
        let manual_block = cmd_worktree_recover(VALID_UUID, "manual-block");
        assert!(abort.is_err());
        assert!(resume.is_err());
        assert!(manual_block.is_err());
        assert!(abort
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn cmd_worktree_recover_rejects_invalid_action() {
        let result = cmd_worktree_recover(VALID_UUID, "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn execute_worktree_recover_abort_persists_state_and_updates_status_projection() {
        let store = InMemoryEventStore::new();
        let (_temp_dir, run_id, corr, source_branch, _target_branch, binding) =
            create_merge_fixture(true);
        let worktree_id = binding.id;

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                worktree_id.to_string(),
                "worktree.conflict_detected",
                corr,
                seed_worktree_event_payload(
                    &binding,
                    "WtMerging",
                    "WtConflict",
                    "merge conflict requires recovery",
                ),
            ))
            .unwrap();

        let output = execute_worktree_recover(&store, worktree_id, "abort").unwrap();
        assert!(output.contains("Resulting state: WtBoundHome"));
        assert!(output.contains("Side effect: executed"));
        assert!(!output.contains("requires a persistent store"));

        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Worktree,
                worktree_id.to_string(),
            ))
            .unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "worktree.recovery_started"));
        assert!(events
            .iter()
            .any(|event| event.event_type == "worktree.recovered"));
        let started = events
            .iter()
            .find(|event| event.event_type == "worktree.recovery_started")
            .expect("expected worktree.recovery_started event");
        let expected_started_key =
            format!("{worktree_id}:worktree_recover:abort:worktree.recovery_started");
        assert_eq!(
            started.idempotency_key.as_deref(),
            Some(expected_started_key.as_str())
        );
        let recovered = events
            .iter()
            .find(|event| event.event_type == "worktree.recovered")
            .expect("expected worktree.recovered event");
        let expected_recovered_key =
            format!("{worktree_id}:worktree_recover:abort:worktree.recovered");
        assert_eq!(
            recovered.idempotency_key.as_deref(),
            Some(expected_recovered_key.as_str())
        );
        assert_eq!(recovered.payload["side_effect"].as_bool(), Some(true));

        let (merge_head_ok, _stdout, _stderr) = run_git(
            &binding.worktree_path,
            &["rev-parse", "--verify", "MERGE_HEAD"],
        );
        assert!(
            !merge_head_ok,
            "MERGE_HEAD should be cleared after abort recovery on branch {source_branch}"
        );

        let status_output = render_worktree_status(&store, run_id).unwrap();
        assert!(status_output.contains(&worktree_id.to_string()));
        assert!(status_output.contains("WtBoundHome"));
    }

    #[test]
    fn execute_worktree_recover_manual_block_persists_recovering_state() {
        let store = InMemoryEventStore::new();
        let (_temp_dir, run_id, corr, _source_branch, _target_branch, binding) =
            create_merge_fixture(true);
        let worktree_id = binding.id;

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                worktree_id.to_string(),
                "worktree.conflict_detected",
                corr,
                seed_worktree_event_payload(
                    &binding,
                    "WtMerging",
                    "WtConflict",
                    "manual intervention required",
                ),
            ))
            .unwrap();

        let output = execute_worktree_recover(&store, worktree_id, "manual-block").unwrap();
        assert!(output.contains("Resulting state: WtRecovering"));
        assert!(output.contains("Side effect: blocked"));
        assert!(!output.contains("requires a persistent store"));

        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Worktree,
                worktree_id.to_string(),
            ))
            .unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "worktree.recovery_blocked"));
        let blocked = events
            .iter()
            .find(|event| event.event_type == "worktree.recovery_blocked")
            .expect("expected worktree.recovery_blocked event");
        let expected_blocked_key =
            format!("{worktree_id}:worktree_recover:manual-block:worktree.recovery_blocked");
        assert_eq!(
            blocked.idempotency_key.as_deref(),
            Some(expected_blocked_key.as_str())
        );
        assert_eq!(blocked.payload["side_effect"].as_bool(), Some(true));

        let status_output = render_worktree_status(&store, run_id).unwrap();
        assert!(status_output.contains("WtRecovering"));
    }

    #[test]
    fn cmd_merge_request_blocks_in_memory_writes_by_default() {
        let merge_no_ff = cmd_merge_request("feat", "main", VALID_UUID, "merge-no-ff");
        let rebase_then_ff = cmd_merge_request("feat", "main", VALID_UUID, "rebase-then-ff");
        let squash_merge = cmd_merge_request("feat", "main", VALID_UUID, "squash-merge");
        assert!(merge_no_ff.is_err());
        assert!(rebase_then_ff.is_err());
        assert!(squash_merge.is_err());
        assert!(merge_no_ff
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn cmd_merge_request_rejects_invalid_strategy() {
        let result = cmd_merge_request("feat", "main", VALID_UUID, "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_merge_request_rejects_invalid_uuid() {
        let result = cmd_merge_request("feat", "main", "bad", "merge-no-ff");
        assert!(result.is_err());
    }

    #[test]
    fn execute_merge_request_persists_requested_event() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let output =
            execute_merge_request(&store, "feat/login", "main", run_id, "merge-no-ff").unwrap();
        assert!(output.contains("Merge intent requested"));
        assert!(!output.contains("requires a persistent store"));

        let merge_id = merge_id_from_output(&output);
        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Merge,
                merge_id.to_string(),
            ))
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "merge.requested");
        assert_eq!(events[0].payload["state"], "MergeRequested");
        assert_eq!(events[0].payload["source"], "feat/login");
        assert_eq!(events[0].payload["target"], "main");
        assert_eq!(events[0].payload["strategy"], "merge-no-ff");
        let expected_request_key = format!("{run_id}:merge_request:feat/login:main:merge-no-ff");
        assert_eq!(
            events[0].idempotency_key.as_deref(),
            Some(expected_request_key.as_str())
        );
    }

    #[test]
    fn execute_merge_approve_persists_policy_and_transition_events() {
        let store = InMemoryEventStore::new();
        let merge_id = Uuid::now_v7();
        let audit = InMemoryAuditSink::new();
        let (_temp_dir, run_id, corr, source_branch, target_branch, binding) =
            create_merge_fixture(false);

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                binding.id.to_string(),
                "worktree.bound",
                corr,
                seed_worktree_event_payload(
                    &binding,
                    "WtCreating",
                    "WtBoundHome",
                    "worktree created for merge",
                ),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({
                    "to": "MergeRequested",
                    "state": "MergeRequested",
                    "run_id": run_id,
                    "worktree_id": binding.id,
                    "source": source_branch,
                    "target": target_branch,
                    "strategy": "merge-no-ff",
                    "reason": "pending approval",
                }),
            ))
            .unwrap();

        let output =
            execute_merge_approve(&store, merge_id, SafeMode::Execute, true, Some(&audit)).unwrap();
        assert!(output.contains("approved and executed to MergeDone"));
        assert!(!output.contains("requires a persistent store"));

        let merge_events = store
            .query(&EventQuery::by_entity(
                EntityType::Merge,
                merge_id.to_string(),
            ))
            .unwrap();
        assert!(merge_events
            .iter()
            .any(|event| event.event_type == "merge.approved"));
        let approved = merge_events
            .iter()
            .find(|event| event.event_type == "merge.approved")
            .expect("expected merge.approved event");
        let expected_approved_key = format!("{merge_id}:merge.approve:merge.approved");
        assert_eq!(
            approved.idempotency_key.as_deref(),
            Some(expected_approved_key.as_str())
        );
        let execution = merge_events
            .iter()
            .find(|event| event.event_type == "merge.execution_succeeded")
            .expect("expected merge.execution_succeeded event");
        assert_eq!(execution.payload["state"], "MergeDone");
        let merge_sha = execution
            .payload
            .get("merge_sha")
            .and_then(|value| value.as_str())
            .expect("expected merge_sha");
        assert_eq!(merge_sha.len(), 40);

        let all_events = store.query(&EventQuery::by_correlation(corr)).unwrap();
        let policy = all_events
            .iter()
            .find(|event| event.event_type == "policy.decision")
            .expect("expected policy.decision event");
        let expected_policy_key = format!("{merge_id}:merge.approve:policy.decision");
        assert_eq!(
            policy.idempotency_key.as_deref(),
            Some(expected_policy_key.as_str())
        );

        let audit_entries = audit.read_all().unwrap();
        assert!(audit_entries
            .iter()
            .any(|entry| entry.category == AuditCategory::PolicyDecision));
    }

    #[test]
    fn execute_merge_approve_in_observe_mode_blocks_and_audits() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let merge_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let audit = InMemoryAuditSink::new();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({
                    "to": "MergeRequested",
                    "state": "MergeRequested",
                    "run_id": run_id,
                    "source": "feat/login",
                    "target": "main",
                    "strategy": "merge-no-ff",
                }),
            ))
            .unwrap();

        let output =
            execute_merge_approve(&store, merge_id, SafeMode::Observe, true, Some(&audit)).unwrap();
        assert!(output.contains("blocked by policy"));
        assert!(!output.contains("requires a persistent store"));

        let merge_events = store
            .query(&EventQuery::by_entity(
                EntityType::Merge,
                merge_id.to_string(),
            ))
            .unwrap();
        assert!(!merge_events
            .iter()
            .any(|event| event.event_type == "merge.approved"));
        assert!(merge_events
            .iter()
            .any(|event| event.event_type == "merge.policy_blocked"));

        let policy_event = store
            .query(&EventQuery::by_correlation(corr))
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == "policy.decision")
            .expect("expected policy decision event");
        assert_eq!(policy_event.payload["outcome"].as_str(), Some("DENY"));
        let expected_policy_key = format!("{merge_id}:merge.approve:policy.decision");
        assert_eq!(
            policy_event.idempotency_key.as_deref(),
            Some(expected_policy_key.as_str())
        );
        let blocked_event = merge_events
            .iter()
            .find(|event| event.event_type == "merge.policy_blocked")
            .expect("expected merge.policy_blocked event");
        let expected_blocked_key = format!("{merge_id}:merge.approve:merge.policy_blocked");
        assert_eq!(
            blocked_event.idempotency_key.as_deref(),
            Some(expected_blocked_key.as_str())
        );

        let audit_entries = audit.read_all().unwrap();
        assert!(audit_entries
            .iter()
            .any(|entry| entry.category == AuditCategory::DestructiveAttempt));
    }

    #[test]
    fn execute_merge_reject_persists_rejected_transition() {
        let store = InMemoryEventStore::new();
        let merge_id = Uuid::now_v7();
        let (_temp_dir, run_id, corr, source_branch, target_branch, binding) =
            create_merge_fixture(true);

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                binding.id.to_string(),
                "worktree.conflict_detected",
                corr,
                seed_worktree_event_payload(
                    &binding,
                    "WtMerging",
                    "WtConflict",
                    "interrupted merge pending rejection",
                ),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({
                    "to": "MergeRequested",
                    "state": "MergeRequested",
                    "run_id": run_id,
                    "worktree_id": binding.id,
                    "source": source_branch,
                    "target": target_branch,
                    "strategy": "merge-no-ff",
                    "reason": "pending approval",
                }),
            ))
            .unwrap();

        let output = execute_merge_reject(
            &store,
            merge_id,
            "manual rejection",
            SafeMode::Execute,
            true,
            None,
        )
        .unwrap();
        assert!(output.contains("MergeRequested -> MergeAborted"));
        assert!(!output.contains("requires a persistent store"));

        let merge_events = store
            .query(&EventQuery::by_entity(
                EntityType::Merge,
                merge_id.to_string(),
            ))
            .unwrap();
        let rejected = merge_events
            .iter()
            .find(|event| event.event_type == "merge.rejected")
            .expect("expected merge.rejected event");
        assert_eq!(rejected.payload["to"], "MergeAborted");
        assert_eq!(rejected.payload["reason"], "manual rejection");
        let expected_rejected_key = format!("{merge_id}:merge.reject:merge.rejected");
        assert_eq!(
            rejected.idempotency_key.as_deref(),
            Some(expected_rejected_key.as_str())
        );
        let execution = merge_events
            .iter()
            .find(|event| event.event_type == "merge.execution_succeeded")
            .expect("expected merge.execution_succeeded event");
        assert_eq!(execution.payload["state"], "MergeAborted");

        let (merge_head_ok, _stdout, _stderr) = run_git(
            &binding.worktree_path,
            &["rev-parse", "--verify", "MERGE_HEAD"],
        );
        assert!(
            !merge_head_ok,
            "MERGE_HEAD should be cleared after merge.reject"
        );

        let policy_event = store
            .query(&EventQuery::by_correlation(corr))
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == "policy.decision")
            .expect("expected policy decision event");
        let expected_policy_key = format!("{merge_id}:merge.reject:policy.decision");
        assert_eq!(
            policy_event.idempotency_key.as_deref(),
            Some(expected_policy_key.as_str())
        );
    }

    #[test]
    fn cmd_audit_tail_nonexistent_file() {
        let result = cmd_audit_tail("/tmp/nonexistent_yarli_audit.jsonl", 20, None);
        assert!(result.is_ok()); // should gracefully report no file
    }

    #[test]
    fn cmd_audit_tail_empty_file() {
        let f = NamedTempFile::new().unwrap();
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 20, None);
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_tail_reads_entries() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        // Write some entries.
        let entry1 = AuditEntry::destructive_attempt(
            "scheduler",
            "force_push",
            "blocked by policy",
            Some(Uuid::nil()),
            None,
            serde_json::json!({}),
        );
        let entry2 = AuditEntry::gate_evaluation(
            "tests_passed",
            true,
            "all tests green",
            Uuid::nil(),
            Some(Uuid::nil()),
        );
        sink.append(&entry1).unwrap();
        sink.append(&entry2).unwrap();

        let result = cmd_audit_tail(f.path().to_str().unwrap(), 20, None);
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_tail_limits_output() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        for i in 0..5 {
            let entry =
                AuditEntry::gate_evaluation(format!("gate_{i}"), true, "ok", Uuid::nil(), None);
            sink.append(&entry).unwrap();
        }

        // Request only 2 lines — should not error.
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 2, None);
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_tail_filters_by_category() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        let entry1 = AuditEntry::destructive_attempt(
            "scheduler",
            "force_push",
            "blocked",
            None,
            None,
            serde_json::json!({}),
        );
        let entry2 = AuditEntry::gate_evaluation("tests_passed", true, "ok", Uuid::nil(), None);
        sink.append(&entry1).unwrap();
        sink.append(&entry2).unwrap();

        // Filter by GateEvaluation.
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 20, Some("GateEvaluation"));
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_tail_all_entries_with_zero_limit() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        for i in 0..3 {
            let entry =
                AuditEntry::gate_evaluation(format!("gate_{i}"), true, "ok", Uuid::nil(), None);
            sink.append(&entry).unwrap();
        }

        // 0 = show all.
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 0, None);
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_query_filters_to_json() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let denied = AuditEntry::destructive_attempt(
            "scheduler",
            "force_push",
            "blocked by policy",
            Some(run_id),
            Some(task_id),
            serde_json::json!({"rule": "no_force_push"}),
        );
        let allowed = AuditEntry::gate_evaluation(
            "policy-allow",
            true,
            "policy allow",
            run_id,
            Some(task_id),
        );
        sink.append(&denied).unwrap();
        sink.append(&allowed).unwrap();

        let result = cmd_audit_query(
            f.path().to_str().unwrap(),
            Some(&run_id.to_string()),
            Some(&task_id.to_string()),
            Some("destructive_attempt"),
            Some("scheduler"),
            None,
            None,
            None,
            0,
            10,
            AuditOutputFormat::Json,
        );
        assert!(result.is_ok());

        let result = cmd_audit_query(
            f.path().to_str().unwrap(),
            Some(&run_id.to_string()),
            None,
            Some("gate_evaluation"),
            None,
            None,
            None,
            None,
            10,
            10,
            AuditOutputFormat::Csv,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_query_rejects_invalid_filters() {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_str().unwrap();

        let invalid = cmd_audit_query(
            path,
            Some("not-a-uuid"),
            None,
            None,
            None,
            None,
            None,
            None,
            10,
            10,
            AuditOutputFormat::Table,
        );
        assert!(invalid.is_err());

        let invalid_category = cmd_audit_query(
            path,
            None,
            None,
            Some("invalid_category"),
            None,
            None,
            None,
            None,
            10,
            10,
            AuditOutputFormat::Table,
        );
        assert!(invalid_category.is_err());
    }

    #[tokio::test]
    async fn cancel_active_run_persists_cancelled_transitions() {
        let store = Arc::new(InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
        let runner = Arc::new(LocalCommandRunner::new());
        let scheduler = Scheduler::new(queue, store.clone(), runner, SchedulerConfig::default());

        let run = Run::new("cancel me", yarli_core::domain::SafeMode::Observe);
        let run_id = run.id;
        let corr = run.correlation_id;
        let task = Task::new(run_id, "task-1", "sleep 60", CommandClass::Io, corr);
        let task_id = task.id;

        scheduler.submit_run(run, vec![task]).await.unwrap();

        let cancelled = cancel_active_run(
            &scheduler,
            &store,
            run_id,
            "cancelled by operator interrupt",
            CancellationSource::Operator,
            Some(default_cancellation_provenance(
                CancellationSource::Operator,
                Some("operator interrupt".to_string()),
            )),
        )
        .await
        .unwrap();
        assert!(cancelled);

        let reg = scheduler.registry().read().await;
        assert_eq!(
            reg.get_task(&task_id).unwrap().state,
            TaskState::TaskCancelled
        );
        assert_eq!(reg.get_run(&run_id).unwrap().state, RunState::RunCancelled);
        drop(reg);

        let task_events = store
            .query(&EventQuery::by_entity(
                EntityType::Task,
                task_id.to_string(),
            ))
            .unwrap();
        assert!(task_events
            .iter()
            .any(|event| event.event_type == "task.cancelled"));

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert!(run_events
            .iter()
            .any(|event| event.event_type == "run.cancelled"));
    }

    #[tokio::test]
    async fn drive_scheduler_captures_sigterm_cancellation_source() {
        let (mut scheduler, store, run_id, correlation_id, task_names) =
            setup_drive_scheduler_fixture("sleep 60").await;
        let shutdown = ShutdownController::new();
        let cancel = shutdown.token();
        let (tx, _rx) = mpsc::unbounded_channel();

        let shutdown_signal = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            shutdown_signal.request_graceful_with_source(CancellationSource::Sigterm);
        });

        let payload = tokio::time::timeout(
            Duration::from_secs(5),
            drive_scheduler(
                &mut scheduler,
                &store,
                shutdown,
                cancel,
                tx,
                run_id,
                correlation_id,
                &task_names,
                AutoAdvancePolicy::StableOk,
                config::RunTaskHealthConfig::default(),
                None,
                0.9,
                false,
                None,
                None,
                None,
            ),
        )
        .await
        .expect("drive_scheduler timed out")
        .unwrap();

        assert_eq!(payload.exit_state, RunState::RunCancelled);
        assert_eq!(
            payload.cancellation_source,
            Some(CancellationSource::Sigterm)
        );
        assert_eq!(
            payload
                .cancellation_provenance
                .as_ref()
                .and_then(|prov| prov.signal_name.as_deref()),
            Some("SIGTERM")
        );

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        let cancelled = run_events
            .iter()
            .find(|event| {
                event.event_type == "run.cancelled"
                    && event
                        .payload
                        .get("cancellation_source")
                        .and_then(|value| value.as_str())
                        == Some("sigterm")
            })
            .expect("expected run.cancelled with sigterm source");
        assert_eq!(
            cancelled
                .payload
                .get("reason")
                .and_then(|value| value.as_str()),
            Some("cancelled by SIGTERM")
        );
    }

    #[tokio::test]
    async fn drive_scheduler_captures_operator_cancellation_source() {
        let store = Arc::new(InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
        let runner = Arc::new(LocalCommandRunner::new());
        let mut scheduler =
            Scheduler::new(queue, store.clone(), runner, SchedulerConfig::default());
        let run = Run::new(
            "drive operator cancel",
            yarli_core::domain::SafeMode::Execute,
        );
        let run_id = run.id;
        let correlation_id = run.correlation_id;
        let task_1 = Task::new(
            run_id,
            "task-1",
            "sleep 1",
            CommandClass::Io,
            correlation_id,
        );
        let mut task_2 = Task::new(
            run_id,
            "task-2",
            "echo done",
            CommandClass::Io,
            correlation_id,
        );
        task_2.depends_on(task_1.id);
        let task_names = vec![
            (task_1.id, "task-1".to_string()),
            (task_2.id, "task-2".to_string()),
        ];
        scheduler
            .submit_run(run, vec![task_1, task_2])
            .await
            .unwrap();
        let shutdown = ShutdownController::new();
        let cancel = shutdown.token();
        let (tx, _rx) = mpsc::unbounded_channel();

        let store_for_signal = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            store_for_signal
                .append(Event {
                    event_id: Uuid::now_v7(),
                    occurred_at: Utc::now(),
                    entity_type: EntityType::Run,
                    entity_id: run_id.to_string(),
                    event_type: "run.cancelled".to_string(),
                    payload: serde_json::json!({
                        "reason": "operator stop",
                    }),
                    correlation_id,
                    causation_id: None,
                    actor: OPERATOR_CONTROL_ACTOR.to_string(),
                    idempotency_key: None,
                })
                .unwrap();
        });

        let payload = tokio::time::timeout(
            Duration::from_secs(8),
            drive_scheduler(
                &mut scheduler,
                &store,
                shutdown,
                cancel,
                tx,
                run_id,
                correlation_id,
                &task_names,
                AutoAdvancePolicy::StableOk,
                config::RunTaskHealthConfig::default(),
                None,
                0.9,
                false,
                None,
                None,
                None,
            ),
        )
        .await
        .expect("drive_scheduler timed out")
        .unwrap();

        assert_eq!(payload.exit_state, RunState::RunCancelled);
        assert_eq!(
            payload.cancellation_source,
            Some(CancellationSource::Operator)
        );
        assert_eq!(
            payload
                .cancellation_provenance
                .as_ref()
                .and_then(|prov| prov.actor_kind),
            Some(CancellationActorKind::Operator)
        );

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        let cancelled = run_events
            .iter()
            .find(|event| {
                event.event_type == "run.cancelled"
                    && event
                        .payload
                        .get("cancellation_source")
                        .and_then(|value| value.as_str())
                        == Some("operator")
            })
            .expect("expected run.cancelled with operator source");
        assert_eq!(
            cancelled
                .payload
                .get("reason")
                .and_then(|value| value.as_str()),
            Some("operator stop")
        );
    }

    #[tokio::test]
    async fn drive_scheduler_captures_sw4rm_preemption_cancellation_source() {
        let (mut scheduler, store, run_id, correlation_id, task_names) =
            setup_drive_scheduler_fixture("sleep 60").await;
        let shutdown = ShutdownController::new();
        let cancel = shutdown.token();
        let (tx, _rx) = mpsc::unbounded_channel();

        let shutdown_signal = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            shutdown_signal.request_graceful_with_source(CancellationSource::Sw4rmPreemption);
        });

        let payload = tokio::time::timeout(
            Duration::from_secs(5),
            drive_scheduler(
                &mut scheduler,
                &store,
                shutdown,
                cancel,
                tx,
                run_id,
                correlation_id,
                &task_names,
                AutoAdvancePolicy::StableOk,
                config::RunTaskHealthConfig::default(),
                None,
                0.9,
                false,
                None,
                None,
                None,
            ),
        )
        .await
        .expect("drive_scheduler timed out")
        .unwrap();

        assert_eq!(payload.exit_state, RunState::RunCancelled);
        assert_eq!(
            payload.cancellation_source,
            Some(CancellationSource::Sw4rmPreemption)
        );
        assert_eq!(
            payload
                .cancellation_provenance
                .as_ref()
                .and_then(|prov| prov.actor_kind),
            Some(CancellationActorKind::Supervisor)
        );

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        let cancelled = run_events
            .iter()
            .find(|event| {
                event.event_type == "run.cancelled"
                    && event
                        .payload
                        .get("cancellation_source")
                        .and_then(|value| value.as_str())
                        == Some("sw4rm_preemption")
            })
            .expect("expected run.cancelled with sw4rm source");
        assert_eq!(
            cancelled
                .payload
                .get("reason")
                .and_then(|value| value.as_str()),
            Some("cancelled by sw4rm preemption")
        );
    }

    #[test]
    fn operator_control_signal_ignores_non_operator_actor() {
        let run_id = Uuid::now_v7();
        let event = Event {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_type: EntityType::Run,
            entity_id: run_id.to_string(),
            event_type: "run.blocked".to_string(),
            payload: serde_json::json!({
                "reason": "observer note"
            }),
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            actor: "observer.progress".to_string(),
            idempotency_key: None,
        };
        assert!(operator_control_signal_from_event(&event, run_id).is_none());
    }

    #[test]
    fn execute_run_pause_and_resume_controls_append_operator_transitions() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let pause = execute_run_pause_control(&store, run_id, "maintenance window").unwrap();
        assert!(pause.contains("RunBlocked"));
        let paused = load_run_projection(&store, run_id).unwrap().unwrap();
        assert_eq!(paused.state, RunState::RunBlocked);

        let resume = execute_run_resume_control(&store, run_id, "maintenance complete").unwrap();
        assert!(resume.contains("RunActive"));
        let resumed = load_run_projection(&store, run_id).unwrap().unwrap();
        assert_eq!(resumed.state, RunState::RunActive);

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert!(run_events.iter().any(|event| {
            event.event_type == "run.blocked"
                && event.actor == OPERATOR_CONTROL_ACTOR
                && event.payload.get("reason").and_then(|v| v.as_str())
                    == Some("maintenance window")
        }));
        assert!(run_events.iter().any(|event| {
            event.event_type == "run.activated"
                && event.actor == OPERATOR_CONTROL_ACTOR
                && event.payload.get("reason").and_then(|v| v.as_str())
                    == Some("maintenance complete")
        }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drive_scheduler_drains_without_claiming_new_tasks_during_pause() {
        let store = Arc::new(InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
        let runner = Arc::new(LocalCommandRunner::new());

        let config = SchedulerConfig {
            claim_batch_size: 1,
            tick_interval: Duration::from_millis(25),
            ..SchedulerConfig::default()
        };

        let mut scheduler = Scheduler::new(queue, store.clone(), runner, config.clone());

        let run = Run::new("drain and resume", yarli_core::domain::SafeMode::Execute);
        let run_id = run.id;
        let correlation_id = run.correlation_id;
        let primary_task = Task::new(
            run_id,
            "primary",
            "sleep 2",
            CommandClass::Io,
            correlation_id,
        );
        let primary_task_id = primary_task.id;
        let mut queued_task = Task::new(
            run_id,
            "queued",
            "sleep 1",
            CommandClass::Io,
            correlation_id,
        );
        // Ensure queued_task cannot be promoted until primary completes,
        // guaranteeing claim order regardless of HashMap iteration order.
        queued_task.depends_on(primary_task_id);
        let queued_task_id = queued_task.id;
        let task_names = vec![
            (primary_task_id, "primary".to_string()),
            (queued_task_id, "queued".to_string()),
        ];

        scheduler
            .submit_run(run, vec![primary_task, queued_task])
            .await
            .unwrap();

        let shutdown = ShutdownController::new();
        let cancel = shutdown.token();
        let (tx, _rx) = mpsc::unbounded_channel();
        let store_for_scheduler = store.clone();

        let run_handle = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_secs(12),
                drive_scheduler(
                    &mut scheduler,
                    &store_for_scheduler,
                    shutdown,
                    cancel,
                    tx,
                    run_id,
                    correlation_id,
                    &task_names,
                    AutoAdvancePolicy::StableOk,
                    config::RunTaskHealthConfig::default(),
                    None,
                    0.9,
                    false,
                    None,
                    None,
                    None,
                ),
            )
            .await
            .expect("drive_scheduler timed out")
        });

        // Wait for primary task to start executing.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(primary_projection) =
                    load_task_projection(store.as_ref(), primary_task_id).unwrap()
                {
                    if primary_projection.state == TaskState::TaskExecuting {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("primary task did not start executing");

        // Pause the run while the primary task is still executing.
        execute_run_pause_control(store.as_ref(), run_id, "maintenance window").unwrap();
        assert_eq!(
            load_run_projection(store.as_ref(), run_id)
                .unwrap()
                .expect("run projection")
                .state,
            RunState::RunBlocked,
        );

        // Wait for primary to complete (it was already running before pause).
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(pp) = load_task_projection(store.as_ref(), primary_task_id).unwrap() {
                    if pp.state == TaskState::TaskComplete {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("primary task should complete while paused");

        // Wait for the scheduler to process the pause signal (at least
        // 2 tick intervals of 100ms each, plus margin).
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Now verify that the queued task is NOT executing — the scheduler
        // should have detected the pause and stopped claiming new tasks.
        // The queued task depends on primary, so it may still be in TaskOpen
        // (no projection events yet) or TaskReady if promoted but not claimed.
        let queued_projection = load_task_projection(store.as_ref(), queued_task_id).unwrap();
        if let Some(qp) = &queued_projection {
            assert!(
                matches!(
                    qp.state,
                    TaskState::TaskReady | TaskState::TaskOpen | TaskState::TaskVerifying
                ),
                "queued task should be held while paused, got {:?}",
                qp.state
            );
        }
        // None means the task was never promoted (still TaskOpen in registry) — also correct.

        // Resume the run and verify both tasks complete.
        execute_run_resume_control(store.as_ref(), run_id, "maintenance complete").unwrap();
        let payload = run_handle
            .await
            .expect("scheduler loop panicked")
            .expect("scheduler loop failed");

        assert_eq!(payload.exit_state, RunState::RunCompleted);
        assert_eq!(
            load_run_projection(store.as_ref(), run_id)
                .unwrap()
                .expect("run projection")
                .state,
            RunState::RunCompleted,
        );
        assert_eq!(
            load_task_projection(store.as_ref(), queued_task_id)
                .unwrap()
                .expect("queued task projection")
                .state,
            TaskState::TaskComplete,
        );
    }

    #[test]
    fn execute_run_cancel_control_cancels_non_terminal_tasks_and_queue_entries() {
        let store = InMemoryEventStore::new();
        let queue = InMemoryTaskQueue::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let active_task_id = Uuid::now_v7();
        let completed_task_id = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                active_task_id.to_string(),
                "task.ready",
                corr,
                serde_json::json!({ "from": "TaskOpen", "to": "TaskReady", "attempt_no": 1 }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                completed_task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({ "from": "TaskVerifying", "to": "TaskComplete", "attempt_no": 1 }),
            ))
            .unwrap();

        queue
            .enqueue(active_task_id, run_id, 1, CommandClass::Io, None)
            .unwrap();

        let output =
            execute_run_cancel_control(&store, &queue, run_id, "operator stop", false).unwrap();
        assert!(output.contains("RunCancelled"));
        assert!(output.contains("cancelled 1 task(s)"));
        assert!(output.contains("drained 1 queue entry(ies)"));

        let run = load_run_projection(&store, run_id).unwrap().unwrap();
        assert_eq!(run.state, RunState::RunCancelled);

        let cancelled_task = load_task_projection(&store, active_task_id)
            .unwrap()
            .unwrap();
        assert_eq!(cancelled_task.state, TaskState::TaskCancelled);
        let completed_task = load_task_projection(&store, completed_task_id)
            .unwrap()
            .unwrap();
        assert_eq!(completed_task.state, TaskState::TaskComplete);

        let stats = queue.stats();
        assert_eq!(stats.cancelled, 1);
    }

    #[test]
    fn merge_apply_conflict_metadata_includes_workspace_path_and_repo_status() {
        let telemetry = vec![MergeApplyTelemetryEvent {
            event_type: "merge.apply.conflict".to_string(),
            task_key: Some("task-2".to_string()),
            task_index: Some(2),
            metadata: serde_json::json!({
                "task_key": "task-2",
                "patch_path": "patches/task-2.patch",
                "workspace_path": "/tmp/yarli/task-2",
                "conflicted_files": ["shared.txt", "src/lib.rs"],
                "recovery_hints": ["hint-a", "hint-b"],
                "repo_status": "UU src/lib.rs\nUU shared.txt"
            }),
        }];

        let (task_key, patch_path, workspace_path, conflicted_files, recovery_hints, repo_status) =
            merge_apply_conflict_metadata(&telemetry);

        assert_eq!(task_key.as_deref(), Some("task-2"));
        assert_eq!(patch_path.as_deref(), Some("patches/task-2.patch"));
        assert_eq!(workspace_path.as_deref(), Some("/tmp/yarli/task-2"));
        assert_eq!(
            conflicted_files,
            vec!["shared.txt".to_string(), "src/lib.rs".to_string()]
        );
        assert_eq!(
            recovery_hints,
            vec!["hint-a".to_string(), "hint-b".to_string()]
        );
        assert_eq!(repo_status.as_deref(), Some("UU src/lib.rs\nUU shared.txt"));
    }

    #[test]
    fn merge_apply_conflict_metadata_falls_back_to_repo_status_paths() {
        let telemetry = vec![MergeApplyTelemetryEvent {
            event_type: "merge.apply.conflict".to_string(),
            task_key: Some("task-status".to_string()),
            task_index: Some(1),
            metadata: serde_json::json!({
                "task_key": "task-status",
                "reason": "merge conflict without explicit conflicted_files array",
                "repo_status": "UU src/lib.rs\nAA shared.txt\n M README.md"
            }),
        }];

        let (_, _, _, conflicted_files, _, _) = merge_apply_conflict_metadata(&telemetry);
        assert_eq!(
            conflicted_files,
            vec!["shared.txt".to_string(), "src/lib.rs".to_string()]
        );
    }

    #[test]
    fn merge_apply_conflict_metadata_falls_back_to_reason_paths() {
        let telemetry = vec![MergeApplyTelemetryEvent {
            event_type: "merge.apply.conflict".to_string(),
            task_key: Some("task-reason".to_string()),
            task_index: Some(1),
            metadata: serde_json::json!({
                "task_key": "task-reason",
                "reason": "error: patch failed: src/core.rs:41\nerror: src/lib.rs: patch does not apply"
            }),
        }];

        let (_, _, _, conflicted_files, _, _) = merge_apply_conflict_metadata(&telemetry);
        assert_eq!(
            conflicted_files,
            vec!["src/core.rs".to_string(), "src/lib.rs".to_string()]
        );
    }

    #[test]
    fn merge_apply_conflict_metadata_uses_failed_finalize_event_when_needed() {
        let telemetry = vec![MergeApplyTelemetryEvent {
            event_type: "merge.apply.finalized".to_string(),
            task_key: Some("task-finalize".to_string()),
            task_index: Some(3),
            metadata: serde_json::json!({
                "task_key": "task-finalize",
                "status": "failed",
                "reason": "error: patch failed: shared.txt:1"
            }),
        }];

        let (task_key, patch_path, workspace_path, conflicted_files, recovery_hints, repo_status) =
            merge_apply_conflict_metadata(&telemetry);

        assert_eq!(task_key.as_deref(), Some("task-finalize"));
        assert!(patch_path.is_none());
        assert!(workspace_path.is_none());
        assert_eq!(conflicted_files, vec!["shared.txt".to_string()]);
        assert!(recovery_hints.is_empty());
        assert!(repo_status.is_none());
    }
}
