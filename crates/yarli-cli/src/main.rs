use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::{
    load_runtime_config_for_reads, load_runtime_config_for_writes, prepare_audit_sink,
    with_event_store, with_event_store_and_queue, AutoAdvancePolicy, BackendSelection,
    ExecutionRunner, LoadedConfig, UiMode,
};
use yarli_cli::dashboard::{DashboardConfig, DashboardRenderer};
use yarli_cli::mode::{self, RenderMode, TerminalInfo};
use yarli_cli::prompt;
use yarli_cli::stream::{HeadlessRenderer, StreamConfig, StreamEvent, StreamRenderer};
use yarli_core::domain::{
    CancellationActorKind, CancellationProvenance, CancellationSource, CancellationStage,
    CommandClass, EntityType, Event, Evidence, PolicyOutcome, SafeMode,
};
use yarli_core::entities::merge_intent::MergeStrategy;
use yarli_core::entities::run::Run;
use yarli_core::entities::task::Task;
use yarli_core::entities::worktree_binding::{SubmoduleMode, WorktreeBinding};
use yarli_core::explain::{DeteriorationReport, DeteriorationTrend, GateResult, GateType};
use yarli_core::fsm::merge::MergeState;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_core::fsm::worktree::WorktreeState;
use yarli_core::shutdown::ShutdownController;
use yarli_gates::{default_run_gates, default_task_gates, evaluate_all, GateContext};
use yarli_git::error::{GitError, RecoveryAction};
use yarli_git::{LocalMergeOrchestrator, LocalWorktreeManager, MergeOrchestrator, WorktreeManager};
use yarli_observability::{AuditEntry, AuditSink, JsonlAuditSink};
use yarli_policy::{ActionType, PolicyEngine, PolicyRequest};
use yarli_queue::{ResourceBudgetConfig, Scheduler, SchedulerConfig, TaskQueue};
use yarli_store::event_store::EventQuery;
use yarli_store::EventStore;

use clap::Parser;

mod cli;
mod commands;
mod config;
mod events;
mod observers;
mod persistence;
mod plan;
mod projection;
mod render;
#[cfg(test)]
mod test_helpers;
mod tranche;
mod workspace;
use crate::events::*;
use crate::persistence::*;
use crate::plan::*;
use crate::projection::*;
use cli::{
    AuditAction, Cli, Commands, GateAction, MergeAction, PlanAction, RunAction, TaskAction,
    TrancheAction, WorktreeAction,
};
use commands::*;

const BUILD_COMMIT: &str = env!("YARLI_BUILD_COMMIT");
const BUILD_DATE: &str = env!("YARLI_BUILD_DATE");
const BUILD_ID: &str = env!("YARLI_BUILD_ID");
const YARLI_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (commit ",
    env!("YARLI_BUILD_COMMIT"),
    ", date ",
    env!("YARLI_BUILD_DATE"),
    ", build ",
    env!("YARLI_BUILD_ID"),
    ")"
);

#[tokio::main]
async fn main() {
    // Initialize tracing (env filter: YARLI_LOG=debug)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("YARLI_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_target(false)
        .init();

    if let Err(e) = run().await {
        error!("{e:#}");
        process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    if let Commands::Init {
        path,
        force,
        print,
        backend,
    } = &cli.command
    {
        return config::cmd_init(path.clone(), *force, *print, *backend);
    }

    let loaded_config = LoadedConfig::load_default().context("failed to load runtime config")?;
    info!(
        config_path = %loaded_config.path().display(),
        config_source = loaded_config.source().label(),
        backend = loaded_config.config().core.backend.as_str(),
        "loaded runtime config"
    );

    // Detect terminal capabilities once; only commands that need a renderer
    // should fail render-mode selection.
    let term_info = TerminalInfo::detect();
    let select_render_mode = || {
        commands::resolve_render_mode(
            &term_info,
            cli.stream,
            cli.tui,
            loaded_config.config().ui.mode,
        )
    };

    match cli.command {
        Commands::Run {
            prompt_file,
            allow_recursive_run,
            action,
        } => match action {
            None => {
                let render_mode = select_render_mode()?;
                cmd_run_default(
                    render_mode,
                    &loaded_config,
                    prompt_file,
                    allow_recursive_run,
                )
                .await
            }
            Some(RunAction::Start {
                objective,
                cmd,
                pace,
                workdir,
                timeout,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                let plan =
                    resolve_run_plan(&loaded_config, objective, cmd, pace, workdir, timeout, None)?;
                let render_mode = select_render_mode()?;
                cmd_run_start(plan, render_mode, &loaded_config, allow_recursive_run).await
            }
            Some(RunAction::Batch {
                objective,
                pace,
                workdir,
                timeout,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                let plan = resolve_run_plan(
                    &loaded_config,
                    objective.unwrap_or_else(|| "batch".to_string()),
                    Vec::new(),
                    pace,
                    workdir,
                    timeout,
                    Some("batch"),
                )?;
                let render_mode = select_render_mode()?;
                cmd_run_start(plan, render_mode, &loaded_config, allow_recursive_run).await
            }
            Some(RunAction::Status { run_id }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_status(&run_id)
            }
            Some(RunAction::ExplainExit { run_id }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_explain(&run_id)
            }
            Some(RunAction::List) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_list()
            }
            Some(RunAction::Continue { file }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                let render_mode = select_render_mode()?;
                cmd_run_continue(file, render_mode, &loaded_config, allow_recursive_run).await
            }
            Some(RunAction::Pause {
                run_id,
                all_active,
                reason,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_pause(run_id.as_deref(), all_active, &reason)
            }
            Some(RunAction::Resume {
                run_id,
                all_paused,
                reason,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_resume(run_id.as_deref(), all_paused, &reason)
            }
            Some(RunAction::Cancel {
                run_id,
                all_active,
                reason,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_cancel(run_id.as_deref(), all_active, &reason)
            }
            #[cfg(feature = "sw4rm")]
            Some(RunAction::Sw4rm) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_sw4rm(&loaded_config).await
            }
        },
        Commands::Task { action } => match action {
            TaskAction::List { run_id } => cmd_task_list(&run_id),
            TaskAction::Explain { task_id } => cmd_task_explain(&task_id),
            TaskAction::Unblock { task_id, reason } => cmd_task_unblock(&task_id, &reason),
            TaskAction::Annotate { task_id, detail } => cmd_task_annotate(&task_id, &detail),
            TaskAction::Output { task_id } => cmd_task_output(&task_id),
        },
        Commands::Gate { action } => match action {
            GateAction::List { run } => cmd_gate_list(run),
            GateAction::Rerun { task_id, gate } => cmd_gate_rerun(&task_id, gate.as_deref()),
        },
        Commands::Worktree { action } => match action {
            WorktreeAction::Status { run_id } => cmd_worktree_status(&run_id),
            WorktreeAction::Recover {
                worktree_id,
                action,
            } => cmd_worktree_recover(&worktree_id, &action),
        },
        Commands::Merge { action } => match action {
            MergeAction::Request {
                source,
                target,
                run_id,
                strategy,
            } => cmd_merge_request(&source, &target, &run_id, &strategy),
            MergeAction::Approve { merge_id } => cmd_merge_approve(&merge_id),
            MergeAction::Reject { merge_id, reason } => cmd_merge_reject(&merge_id, &reason),
            MergeAction::Status { merge_id } => cmd_merge_status(&merge_id),
        },
        Commands::Audit { action } => match action {
            AuditAction::Tail {
                file,
                lines,
                category,
            } => cmd_audit_tail(&file, lines, category.as_deref()),
        },
        Commands::Plan { action } => match action {
            PlanAction::Tranche { action } => match action {
                TrancheAction::Add {
                    key,
                    summary,
                    group,
                    allowed_paths,
                } => cmd_plan_tranche_add(&key, &summary, group.as_deref(), &allowed_paths),
                TrancheAction::Complete { key } => cmd_plan_tranche_complete(&key),
                TrancheAction::List => cmd_plan_tranche_list(),
                TrancheAction::Remove { key } => cmd_plan_tranche_remove(&key),
            },
            PlanAction::Validate => cmd_plan_validate(),
        },
        Commands::Init { .. } => unreachable!("init command handled before runtime config load"),
        Commands::Info => {
            // `info` should report capabilities even if the current terminal
            // cannot satisfy a forced render mode.
            let render_mode = select_render_mode().unwrap_or(RenderMode::Stream);
            cmd_info(&term_info, render_mode, &loaded_config)
        }
    }
}
