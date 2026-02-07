use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use yarli_cli::config::{BackendSelection, LoadedConfig};
use yarli_cli::dashboard::{DashboardConfig, DashboardRenderer};
use yarli_cli::mode::{self, RenderMode, TerminalInfo};
use yarli_cli::stream::{StreamConfig, StreamEvent, StreamRenderer};
use yarli_core::domain::{CommandClass, EntityType, Event, Evidence, PolicyOutcome, SafeMode};
use yarli_core::entities::run::Run;
use yarli_core::entities::task::Task;
use yarli_core::explain::{explain_run, explain_task, GateResult, GateType, RunSnapshot, TaskSnapshot};
use yarli_core::fsm::merge::MergeState;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_core::fsm::worktree::WorktreeState;
use yarli_core::shutdown::ShutdownController;
use yarli_exec::LocalCommandRunner;
use yarli_gates::{all_passed, default_run_gates, default_task_gates, evaluate_all, GateContext};
use yarli_observability::{AuditEntry, AuditSink, JsonlAuditSink};
use yarli_policy::{ActionType, PolicyEngine, PolicyRequest};
use yarli_queue::{InMemoryTaskQueue, PostgresTaskQueue, Scheduler, SchedulerConfig, TaskQueue};
use yarli_store::event_store::EventQuery;
use yarli_store::{EventStore, InMemoryEventStore, PostgresEventStore};

/// YARLI — Yet Another Orchestrator Loop Implementation.
///
/// Deterministic orchestrator with state machines, event log, and safe Git handling.
#[derive(Parser)]
#[command(name = "yarli", version, about)]
struct Cli {
    /// Force stream mode output (inline viewport, no fullscreen TUI).
    #[arg(long, global = true)]
    stream: bool,

    /// Force dashboard mode (fullscreen TUI with panel layout).
    #[arg(long, global = true)]
    tui: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage orchestration runs.
    Run {
        #[command(subcommand)]
        action: RunAction,
    },
    /// Manage tasks within a run.
    Task {
        #[command(subcommand)]
        action: TaskAction,
    },
    /// Manage verification gates.
    Gate {
        #[command(subcommand)]
        action: GateAction,
    },
    /// Manage Git worktrees.
    Worktree {
        #[command(subcommand)]
        action: WorktreeAction,
    },
    /// Manage merge intents.
    Merge {
        #[command(subcommand)]
        action: MergeAction,
    },
    /// View the audit log.
    Audit {
        #[command(subcommand)]
        action: AuditAction,
    },
    /// Show version and detected terminal capabilities.
    Info,
}

#[derive(Subcommand)]
enum RunAction {
    /// Start a new orchestration run.
    Start {
        /// The objective describing what this run should accomplish.
        objective: String,
        /// Commands to execute (one per task). Use multiple times for multiple tasks.
        #[arg(short, long)]
        cmd: Vec<String>,
        /// Working directory for command execution (defaults to `execution.working_dir`).
        #[arg(short, long)]
        workdir: Option<String>,
        /// Command timeout in seconds (defaults to `execution.command_timeout_seconds`, 0 = no timeout).
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Show the current status of a run.
    Status {
        /// Run ID to query.
        run_id: String,
    },
    /// Explain why a run is not done (Why Not Done? engine).
    ExplainExit {
        /// Run ID to explain.
        run_id: String,
    },
}

#[derive(Subcommand)]
enum TaskAction {
    /// List tasks for a run.
    List {
        /// Run ID to list tasks for.
        run_id: String,
    },
    /// Explain why a task is not done (Why Not Done? engine).
    Explain {
        /// Task ID to explain.
        task_id: String,
    },
    /// Unblock a task (clear its blocker and transition to ready).
    Unblock {
        /// Task ID to unblock.
        task_id: String,
        /// Reason for unblocking.
        #[arg(short, long, default_value = "manually unblocked")]
        reason: String,
    },
}

#[derive(Subcommand)]
enum GateAction {
    /// List configured gates for a run or task.
    List {
        /// Show task-level gates (default), or --run for run-level gates.
        #[arg(long)]
        run: bool,
    },
    /// Re-run a specific gate evaluation.
    Rerun {
        /// Task ID to re-evaluate gates for.
        task_id: String,
        /// Specific gate name to re-run (e.g. "tests_passed"). If omitted, all gates are re-run.
        #[arg(short, long)]
        gate: Option<String>,
    },
}

#[derive(Subcommand)]
enum WorktreeAction {
    /// Show worktree status for a run.
    Status {
        /// Run ID to show worktree status for.
        run_id: String,
    },
    /// Recover from an interrupted git operation in a worktree.
    Recover {
        /// Worktree ID to recover.
        worktree_id: String,
        /// Recovery action: abort, resume, or manual-block.
        #[arg(short, long, default_value = "abort")]
        action: String,
    },
}

#[derive(Subcommand)]
enum MergeAction {
    /// Request a new merge intent.
    Request {
        /// Source branch or ref to merge from.
        source: String,
        /// Target branch or ref to merge into.
        target: String,
        /// Run ID this merge belongs to.
        #[arg(long)]
        run_id: String,
        /// Merge strategy: merge-no-ff, rebase-then-ff, squash-merge.
        #[arg(long, default_value = "merge-no-ff")]
        strategy: String,
    },
    /// Approve a pending merge intent.
    Approve {
        /// Merge intent ID to approve.
        merge_id: String,
    },
    /// Reject a pending merge intent.
    Reject {
        /// Merge intent ID to reject.
        merge_id: String,
        /// Reason for rejection.
        #[arg(short, long, default_value = "rejected")]
        reason: String,
    },
    /// Show status of a merge intent.
    Status {
        /// Merge intent ID to query.
        merge_id: String,
    },
}

#[derive(Subcommand)]
enum AuditAction {
    /// Tail the JSONL audit log.
    Tail {
        /// Path to the audit JSONL file.
        #[arg(short, long, default_value = ".yarl/audit.jsonl")]
        file: String,
        /// Number of most recent entries to show (0 = all).
        #[arg(short, long, default_value = "20")]
        lines: usize,
        /// Filter by category (policy_decision, destructive_attempt, token_consumed, gate_evaluation).
        #[arg(short, long)]
        category: Option<String>,
    },
}

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
    let loaded_config = LoadedConfig::load_default().context("failed to load runtime config")?;
    info!(
        config_path = %loaded_config.path().display(),
        config_source = loaded_config.source().label(),
        backend = loaded_config.config().core.backend.as_str(),
        "loaded runtime config"
    );

    // Detect terminal capabilities and select render mode.
    let term_info = TerminalInfo::detect();
    let _render_mode = mode::select_render_mode(&term_info, cli.stream, cli.tui)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    match cli.command {
        Commands::Run { action } => match action {
            RunAction::Start {
                objective,
                cmd,
                workdir,
                timeout,
            } => {
                cmd_run_start(objective, cmd, workdir, timeout, _render_mode, &loaded_config).await
            }
            RunAction::Status { run_id } => cmd_run_status(&run_id),
            RunAction::ExplainExit { run_id } => cmd_run_explain(&run_id),
        },
        Commands::Task { action } => match action {
            TaskAction::List { run_id } => cmd_task_list(&run_id),
            TaskAction::Explain { task_id } => cmd_task_explain(&task_id),
            TaskAction::Unblock { task_id, reason } => cmd_task_unblock(&task_id, &reason),
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
        Commands::Info => cmd_info(&term_info, _render_mode, &loaded_config),
    }
}

/// `yarli run start` — create a run, submit tasks, drive scheduler with stream output.
async fn cmd_run_start(
    objective: String,
    commands: Vec<String>,
    workdir: Option<String>,
    timeout_secs: Option<u64>,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
) -> Result<()> {
    let selected_workdir = workdir.unwrap_or_else(|| loaded_config.config().execution.working_dir.clone());
    let selected_timeout_secs =
        timeout_secs.unwrap_or(loaded_config.config().execution.command_timeout_seconds);

    match loaded_config.backend_selection()? {
        BackendSelection::InMemory => {
            println!("Using backend: in-memory");
            let store = Arc::new(InMemoryEventStore::new());
            let queue = Arc::new(InMemoryTaskQueue::new());
            cmd_run_start_with_backend(
                objective,
                commands,
                selected_workdir,
                selected_timeout_secs,
                render_mode,
                loaded_config,
                store,
                queue,
            )
            .await
        }
        BackendSelection::Postgres { database_url } => {
            println!("Using backend: postgres");
            let store = Arc::new(
                PostgresEventStore::new(&database_url)
                    .map_err(|e| anyhow::anyhow!("failed to initialize postgres event store: {e}"))?,
            );
            let queue = Arc::new(
                PostgresTaskQueue::new(&database_url)
                    .map_err(|e| anyhow::anyhow!("failed to initialize postgres task queue: {e}"))?,
            );
            cmd_run_start_with_backend(
                objective,
                commands,
                selected_workdir,
                selected_timeout_secs,
                render_mode,
                loaded_config,
                store,
                queue,
            )
            .await
        }
    }
}

async fn cmd_run_start_with_backend<Q, S>(
    objective: String,
    commands: Vec<String>,
    workdir: String,
    timeout_secs: u64,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
    store: Arc<S>,
    queue: Arc<Q>,
) -> Result<()>
where
    Q: TaskQueue + 'static,
    S: EventStore + 'static,
{
    if commands.is_empty() {
        bail!("at least one --cmd is required");
    }

    let runner = Arc::new(LocalCommandRunner::new());

    let command_timeout = if timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(timeout_secs))
    };

    let mut config = SchedulerConfig {
        working_dir: workdir.clone(),
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
    config.tick_interval =
        Duration::from_millis(loaded_config.config().execution.tick_interval_ms);
    config.concurrency.per_run_cap = loaded_config.config().queue.per_run_cap;
    config.concurrency.io_cap = loaded_config.config().queue.io_cap;
    config.concurrency.cpu_cap = loaded_config.config().queue.cpu_cap;
    config.concurrency.git_cap = loaded_config.config().queue.git_cap;
    config.concurrency.tool_cap = loaded_config.config().queue.tool_cap;
    if let Some(worker_id) = loaded_config.config().core.worker_id.as_ref() {
        config.worker_id = worker_id.clone();
    }
    config.enforce_policies = loaded_config.config().policy.enforce_policies;
    config.audit_decisions = loaded_config.config().policy.audit_decisions;

    let mut scheduler = Scheduler::new(queue, store.clone(), runner, config);
    if loaded_config.config().policy.audit_decisions {
        let audit_path = PathBuf::from(&loaded_config.config().observability.audit_file);
        if let Some(parent) = audit_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "failed to create audit directory {}",
                        parent.display()
                    )
                })?;
            }
        }
        scheduler = scheduler.with_audit_sink(Arc::new(JsonlAuditSink::new(&audit_path)));
    }

    // Build run and tasks.
    let run_snapshot = build_run_config_snapshot(loaded_config, &workdir, timeout_secs, &commands)
        .context("failed to build run config snapshot")?;
    let run = Run::with_config(
        &objective,
        loaded_config.config().core.safe_mode,
        run_snapshot.clone(),
    );
    let run_id = run.id;
    let correlation_id = run.correlation_id;

    let tasks: Vec<Task> = commands
        .iter()
        .enumerate()
        .map(|(i, cmd)| {
            Task::new(
                run_id,
                format!("task-{}", i + 1),
                cmd,
                CommandClass::Io,
                correlation_id,
            )
        })
        .collect();

    let task_names: Vec<(Uuid, String)> = tasks.iter().map(|t| (t.id, t.task_key.clone())).collect();

    store.append(Event {
        event_id: Uuid::now_v7(),
        occurred_at: chrono::Utc::now(),
        entity_type: EntityType::Run,
        entity_id: run_id.to_string(),
        event_type: "run.config_snapshot".to_string(),
        payload: serde_json::json!({
            "objective": objective.clone(),
            "safe_mode": loaded_config.config().core.safe_mode,
            "config_snapshot": run_snapshot,
        }),
        correlation_id,
        causation_id: None,
        actor: "cli".to_string(),
        idempotency_key: Some(format!("{run_id}:config_snapshot")),
    })?;

    // Submit run.
    scheduler
        .submit_run(run, tasks)
        .await
        .context("failed to submit run")?;

    info!(run_id = %run_id, "run started: {objective}");

    // Set up shutdown controller.
    let shutdown = ShutdownController::new();
    shutdown.install_signal_handler();
    let cancel = shutdown.token();

    // Set up event channel for renderer.
    let (tx, rx) = mpsc::unbounded_channel::<StreamEvent>();

    // Send initial run transition event.
    let _ = tx.send(StreamEvent::RunTransition {
        run_id,
        from: RunState::RunOpen,
        to: RunState::RunActive,
        reason: Some("run submitted".into()),
        at: chrono::Utc::now(),
    });

    // Spawn renderer task.
    let renderer_shutdown = shutdown.clone();
    let renderer_handle = tokio::task::spawn_blocking(move || {
        run_renderer(rx, render_mode, renderer_shutdown)
    });

    // Drive scheduler loop, emitting events to renderer channel.
    drive_scheduler(&scheduler, &store, cancel, tx, run_id, &task_names).await?;

    // Wait for renderer to finish.
    renderer_handle
        .await
        .context("renderer task panicked")?
        .context("renderer error")?;

    // Print final status.
    let reg = scheduler.registry().read().await;
    let run_state = reg
        .get_run(&run_id)
        .map(|r| r.state)
        .unwrap_or(RunState::RunOpen);

    match run_state {
        RunState::RunCompleted => {
            println!("Run {run_id} completed successfully.");
            Ok(())
        }
        RunState::RunFailed => {
            bail!("Run {run_id} failed.");
        }
        RunState::RunCancelled => {
            println!("Run {run_id} cancelled.");
            process::exit(130);
        }
        other => {
            bail!("Run {run_id} ended in unexpected state: {other:?}");
        }
    }
}

fn build_run_config_snapshot(
    loaded_config: &LoadedConfig,
    working_dir: &str,
    timeout_secs: u64,
    commands: &[String],
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "config_source": loaded_config.source().label(),
        "config_path": loaded_config.path().display().to_string(),
        "backend": loaded_config.config().core.backend.as_str(),
        "config": loaded_config.snapshot()?,
        "runtime": {
            "working_dir": working_dir,
            "timeout_secs": timeout_secs,
            "command_count": commands.len(),
            "commands": commands,
        },
    }))
}

/// Drive the scheduler, emitting StreamEvents to the renderer channel.
async fn drive_scheduler<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    store: &Arc<S>,
    cancel: CancellationToken,
    tx: mpsc::UnboundedSender<StreamEvent>,
    run_id: Uuid,
    task_names: &[(Uuid, String)],
) -> Result<()>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let mut tick_interval = tokio::time::interval(Duration::from_millis(100));
    let mut tick_count: u64 = 0;
    let mut last_event_count: usize = 0;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("scheduler cancelled");
                let _ = cancel_active_run(
                    scheduler,
                    store,
                    run_id,
                    "cancelled by operator interrupt",
                )
                .await?;
                emit_new_stream_events(store, &tx, task_names, &mut last_event_count)?;
                drop(tx);
                return Ok(());
            }
            _ = tick_interval.tick() => {
                tick_count += 1;

                // Run a scheduler tick.
                let _result = scheduler.tick().await
                    .context("scheduler tick failed")?;

                // Emit events by scanning the event store for new events.
                emit_new_stream_events(store, &tx, task_names, &mut last_event_count)?;

                // Send tick for spinner animation.
                let _ = tx.send(StreamEvent::Tick);

                // Check if the run is terminal.
                let reg = scheduler.registry().read().await;
                if let Some(run) = reg.get_run(&run_id) {
                    if run.state.is_terminal() {
                        info!(state = ?run.state, ticks = tick_count, "run reached terminal state");
                        drop(tx);
                        return Ok(());
                    }
                }

                // Safety: bail after 10000 ticks (~16 min at 100ms) to prevent infinite loops.
                if tick_count > 10_000 {
                    drop(tx);
                    bail!("scheduler exceeded max ticks (10000)");
                }
            }
        }
    }
}

fn emit_new_stream_events<S: EventStore>(
    store: &Arc<S>,
    tx: &mpsc::UnboundedSender<StreamEvent>,
    task_names: &[(Uuid, String)],
    last_event_count: &mut usize,
) -> Result<()> {
    let all_events = store.all().map_err(|e| anyhow::anyhow!("{e}"))?;
    let new_events = &all_events[*last_event_count..];
    *last_event_count = all_events.len();

    for event in new_events {
        let stream_event = event_to_stream_event(event, task_names);
        if let Some(se) = stream_event {
            let _ = tx.send(se);
        }
    }

    Ok(())
}

async fn cancel_active_run<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    store: &Arc<S>,
    run_id: Uuid,
    reason: &str,
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

    let mut events: Vec<Event> = Vec::new();
    let mut reg = scheduler.registry().write().await;

    for task_id in task_ids {
        let Some(task) = reg.get_task_mut(&task_id) else {
            continue;
        };
        if task.state.is_terminal() {
            continue;
        }

        let attempt_no = task.attempt_no;
        let transition = task.transition(
            TaskState::TaskCancelled,
            reason,
            "cli",
            None,
        )?;

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
            }),
            correlation_id,
            causation_id: None,
            actor: "cli".to_string(),
            idempotency_key: Some(format!("{task_id}:cancelled:{attempt_no}")),
        });
    }

    let run_cancelled = if let Some(run) = reg.get_run_mut(&run_id) {
        if run.state.is_terminal() {
            false
        } else {
            let transition = run.transition(
                RunState::RunCancelled,
                reason,
                "cli",
                None,
            )?;

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
                }),
                correlation_id,
                causation_id: None,
                actor: "cli".to_string(),
                idempotency_key: Some(format!("{run_id}:cancelled")),
            });
            true
        }
    } else {
        false
    };

    drop(reg);
    for event in events {
        store
            .append(event)
            .map_err(|e| anyhow::anyhow!("failed to persist cancellation event: {e}"))?;
    }

    Ok(run_cancelled)
}

/// Convert a domain Event to a StreamEvent for the renderer.
fn event_to_stream_event(
    event: &yarli_core::domain::Event,
    task_names: &[(Uuid, String)],
) -> Option<StreamEvent> {
    let task_name = |entity_id: &str| -> String {
        if let Ok(id) = entity_id.parse::<Uuid>() {
            task_names
                .iter()
                .find(|(tid, _)| *tid == id)
                .map(|(_, name)| name.clone())
                .unwrap_or_else(|| entity_id[..8].to_string())
        } else {
            entity_id.to_string()
        }
    };

    match event.event_type.as_str() {
        "task.ready" | "task.executing" | "task.verifying" | "task.completed" | "task.failed"
        | "task.retrying" | "task.cancelled" => {
            let from_str = event.payload.get("from").and_then(|v| v.as_str());
            let to_str = event.payload.get("to").and_then(|v| v.as_str());

            let from = from_str
                .and_then(|s| parse_task_state(s))
                .unwrap_or(TaskState::TaskOpen);
            let to = to_str
                .and_then(|s| parse_task_state(s))
                .unwrap_or(TaskState::TaskOpen);

            let exit_code = event
                .payload
                .get("exit_code")
                .and_then(|v| v.as_i64())
                .map(|c| c as i32);

            let name = task_name(&event.entity_id);

            Some(StreamEvent::TaskTransition {
                task_id: event.entity_id.parse().unwrap_or(Uuid::nil()),
                task_name: name,
                from,
                to,
                elapsed: None,
                exit_code,
                detail: event.payload.get("reason").and_then(|v| v.as_str()).map(String::from),
                at: event.occurred_at,
            })
        }
        "run.activated" | "run.verifying" | "run.completed" | "run.failed" => {
            let from_str = event.payload.get("from").and_then(|v| v.as_str());
            let to_str = event.payload.get("to").and_then(|v| v.as_str());

            let from = from_str
                .and_then(|s| parse_run_state(s))
                .unwrap_or(RunState::RunOpen);
            let to = to_str
                .and_then(|s| parse_run_state(s))
                .unwrap_or(RunState::RunOpen);

            let reason = event.payload.get("reason").and_then(|v| v.as_str()).map(String::from);

            let run_id = event.entity_id.parse().unwrap_or(Uuid::nil());

            Some(StreamEvent::RunTransition {
                run_id,
                from,
                to,
                reason,
                at: event.occurred_at,
            })
        }
        "command.output" => {
            let line = event
                .payload
                .get("line")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = task_name(&event.entity_id);
            Some(StreamEvent::CommandOutput {
                task_id: event.entity_id.parse().unwrap_or(Uuid::nil()),
                task_name: name,
                line,
            })
        }
        _ => None,
    }
}

/// Parse a TaskState from its serialized form.
fn parse_task_state(s: &str) -> Option<TaskState> {
    match s {
        "TaskOpen" => Some(TaskState::TaskOpen),
        "TaskReady" => Some(TaskState::TaskReady),
        "TaskExecuting" => Some(TaskState::TaskExecuting),
        "TaskWaiting" => Some(TaskState::TaskWaiting),
        "TaskBlocked" => Some(TaskState::TaskBlocked),
        "TaskVerifying" => Some(TaskState::TaskVerifying),
        "TaskComplete" => Some(TaskState::TaskComplete),
        "TaskFailed" => Some(TaskState::TaskFailed),
        "TaskCancelled" => Some(TaskState::TaskCancelled),
        _ => None,
    }
}

/// Parse a RunState from its serialized form.
fn parse_run_state(s: &str) -> Option<RunState> {
    match s {
        "RunOpen" => Some(RunState::RunOpen),
        "RunActive" => Some(RunState::RunActive),
        "RunBlocked" => Some(RunState::RunBlocked),
        "RunVerifying" => Some(RunState::RunVerifying),
        "RunCompleted" => Some(RunState::RunCompleted),
        "RunFailed" => Some(RunState::RunFailed),
        "RunCancelled" => Some(RunState::RunCancelled),
        _ => None,
    }
}

/// Parse a WorktreeState from its serialized form.
fn parse_worktree_state(s: &str) -> Option<WorktreeState> {
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
fn parse_merge_state(s: &str) -> Option<MergeState> {
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

/// Run the renderer in a blocking context (requires raw terminal access).
fn run_renderer(
    mut rx: mpsc::UnboundedReceiver<StreamEvent>,
    render_mode: RenderMode,
    shutdown: ShutdownController,
) -> Result<()> {
    match render_mode {
        RenderMode::Stream => {
            let config = StreamConfig::default();
            let mut renderer =
                StreamRenderer::new(config).context("failed to create stream renderer")?;

            while let Some(event) = rx.blocking_recv() {
                renderer
                    .handle_event(event)
                    .context("renderer handle_event failed")?;
            }
        }
        RenderMode::Dashboard => {
            let config = DashboardConfig::default();
            let mut renderer =
                DashboardRenderer::new(config).context("failed to create dashboard renderer")?;

            loop {
                // Process all pending stream events (non-blocking drain).
                while let Ok(event) = rx.try_recv() {
                    renderer.handle_event(event);
                }

                // Draw the dashboard.
                renderer.draw().context("dashboard draw failed")?;

                // Poll for keyboard input (blocks for tick_rate_ms).
                let quit = renderer.poll_input().context("dashboard input poll failed")?;
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
                            if renderer.poll_input().context("dashboard input poll failed")? {
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

#[derive(Debug, Clone)]
struct TaskProjection {
    task_id: Uuid,
    state: TaskState,
    correlation_id: Uuid,
    updated_at: chrono::DateTime<chrono::Utc>,
    last_event_id: Uuid,
    last_event_type: String,
    reason: Option<String>,
    attempt_no: Option<u32>,
    failed_gates: Vec<(GateType, String)>,
}

#[derive(Debug, Clone)]
struct RunProjection {
    run_id: Uuid,
    state: RunState,
    correlation_id: Uuid,
    updated_at: chrono::DateTime<chrono::Utc>,
    last_event_type: String,
    objective: Option<String>,
    tasks: Vec<TaskProjection>,
    failed_gates: Vec<(GateType, String)>,
}

#[derive(Debug, Clone)]
struct EntityProjection {
    entity_id: String,
    state: String,
    updated_at: chrono::DateTime<chrono::Utc>,
    last_event_type: String,
    reason: Option<String>,
}

#[derive(Debug, Clone)]
struct WorktreeProjection {
    worktree_id: Uuid,
    state: WorktreeState,
    correlation_id: Uuid,
    updated_at: chrono::DateTime<chrono::Utc>,
    last_event_id: Uuid,
    last_event_type: String,
    reason: Option<String>,
    run_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct MergeProjection {
    merge_id: Uuid,
    state: MergeState,
    run_id: Option<Uuid>,
    correlation_id: Uuid,
    updated_at: chrono::DateTime<chrono::Utc>,
    last_event_id: Uuid,
    last_event_type: String,
    reason: Option<String>,
    source_ref: Option<String>,
    target_ref: Option<String>,
    strategy: Option<String>,
}

fn load_runtime_config_for_reads() -> Result<LoadedConfig> {
    LoadedConfig::load_default().context("failed to load runtime config")
}

fn prepare_audit_sink(loaded_config: &LoadedConfig) -> Result<Option<JsonlAuditSink>> {
    if !loaded_config.config().policy.audit_decisions {
        return Ok(None);
    }

    let audit_path = PathBuf::from(&loaded_config.config().observability.audit_file);
    if let Some(parent) = audit_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create audit directory {}",
                    parent.display()
                )
            })?;
        }
    }

    Ok(Some(JsonlAuditSink::new(audit_path)))
}

fn with_event_store<T>(
    loaded_config: &LoadedConfig,
    operation: impl FnOnce(&dyn EventStore) -> Result<T>,
) -> Result<T> {
    match loaded_config.backend_selection()? {
        BackendSelection::InMemory => {
            let store = InMemoryEventStore::new();
            operation(&store)
        }
        BackendSelection::Postgres { database_url } => {
            let store = PostgresEventStore::new(&database_url)
                .map_err(|e| anyhow::anyhow!("failed to initialize postgres event store: {e}"))?;
            operation(&store)
        }
    }
}

fn query_events(store: &dyn EventStore, query: &EventQuery) -> Result<Vec<Event>> {
    store
        .query(query)
        .map_err(|e| anyhow::anyhow!("event query failed: {e}"))
}

fn append_event(store: &dyn EventStore, event: Event) -> Result<()> {
    store
        .append(event)
        .map_err(|e| anyhow::anyhow!("failed to append event: {e}"))
}

fn run_state_from_event(event: &Event) -> Option<RunState> {
    event
        .payload
        .get("to")
        .and_then(|v| v.as_str())
        .and_then(parse_run_state)
        .or_else(|| match event.event_type.as_str() {
            "run.activated" => Some(RunState::RunActive),
            "run.verifying" => Some(RunState::RunVerifying),
            "run.completed" => Some(RunState::RunCompleted),
            "run.failed" | "run.gate_failed" => Some(RunState::RunFailed),
            "run.cancelled" => Some(RunState::RunCancelled),
            _ => None,
        })
}

fn task_state_from_event(event: &Event) -> Option<TaskState> {
    event
        .payload
        .get("to")
        .and_then(|v| v.as_str())
        .and_then(parse_task_state)
        .or_else(|| match event.event_type.as_str() {
            "task.ready" | "task.retrying" | "task.unblocked" => Some(TaskState::TaskReady),
            "task.executing" => Some(TaskState::TaskExecuting),
            "task.verifying" => Some(TaskState::TaskVerifying),
            "task.completed" => Some(TaskState::TaskComplete),
            "task.failed" | "task.gate_failed" => Some(TaskState::TaskFailed),
            "task.blocked" => Some(TaskState::TaskBlocked),
            "task.cancelled" => Some(TaskState::TaskCancelled),
            _ => None,
        })
}

fn event_reason(event: &Event) -> Option<String> {
    event
        .payload
        .get("reason")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn extract_entity_state(event: &Event) -> String {
    for key in ["to", "state", "status"] {
        if let Some(value) = event.payload.get(key).and_then(|v| v.as_str()) {
            return value.to_string();
        }
    }
    event.event_type.clone()
}

fn parse_gate_failure_entry(entry: &str) -> Option<(GateType, String)> {
    let (raw_gate, raw_reason) = entry.split_once(':')?;
    let gate_name = raw_gate.trim().strip_prefix("gate.").unwrap_or(raw_gate.trim());
    let gate_type = parse_gate_type(gate_name)?;
    Some((gate_type, raw_reason.trim().to_string()))
}

fn gate_failures_from_event(event: &Event) -> Vec<(GateType, String)> {
    let mut failures = Vec::new();

    if let Some(items) = event.payload.get("failures").and_then(|v| v.as_array()) {
        for item in items {
            if let Some(value) = item.as_str().and_then(parse_gate_failure_entry) {
                failures.push(value);
            }
        }
    }

    if failures.is_empty() {
        if let Some(reason) = event.payload.get("reason").and_then(|v| v.as_str()) {
            if let Some(value) = parse_gate_failure_entry(reason) {
                failures.push(value);
            }
        }
    }

    failures
}

fn collect_task_projections(events: &[Event]) -> Vec<TaskProjection> {
    let mut tasks: BTreeMap<Uuid, TaskProjection> = BTreeMap::new();

    for event in events {
        let task_id = match event.entity_id.parse::<Uuid>() {
            Ok(id) => id,
            Err(_) => continue,
        };

        let entry = tasks.entry(task_id).or_insert_with(|| TaskProjection {
            task_id,
            state: TaskState::TaskOpen,
            correlation_id: event.correlation_id,
            updated_at: event.occurred_at,
            last_event_id: event.event_id,
            last_event_type: event.event_type.clone(),
            reason: None,
            attempt_no: None,
            failed_gates: Vec::new(),
        });

        entry.correlation_id = event.correlation_id;
        entry.updated_at = event.occurred_at;
        entry.last_event_id = event.event_id;
        entry.last_event_type = event.event_type.clone();

        if let Some(next_state) = task_state_from_event(event) {
            entry.state = next_state;
        }

        if let Some(reason) = event_reason(event) {
            entry.reason = Some(reason);
        }

        if let Some(attempt_no) = event
            .payload
            .get("attempt_no")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
        {
            entry.attempt_no = Some(attempt_no);
        }

        if event.event_type == "task.gate_failed" {
            entry.failed_gates = gate_failures_from_event(event);
        } else if matches!(
            entry.state,
            TaskState::TaskReady
                | TaskState::TaskExecuting
                | TaskState::TaskVerifying
                | TaskState::TaskComplete
                | TaskState::TaskCancelled
        ) {
            entry.failed_gates.clear();
        }
    }

    let mut values: Vec<_> = tasks.into_values().collect();
    values.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.task_id.cmp(&b.task_id))
    });
    values
}

fn load_task_projection(store: &dyn EventStore, task_id: Uuid) -> Result<Option<TaskProjection>> {
    let task_events = query_events(store, &EventQuery::by_entity(EntityType::Task, task_id.to_string()))?;
    if task_events.is_empty() {
        return Ok(None);
    }

    let projections = collect_task_projections(&task_events);
    Ok(projections.into_iter().next())
}

fn load_run_projection(store: &dyn EventStore, run_id: Uuid) -> Result<Option<RunProjection>> {
    let run_events = query_events(store, &EventQuery::by_entity(EntityType::Run, run_id.to_string()))?;
    if run_events.is_empty() {
        return Ok(None);
    }

    let mut state = RunState::RunOpen;
    let mut objective = None;
    let mut correlation_id = run_events[0].correlation_id;
    let mut updated_at = run_events[0].occurred_at;
    let mut last_event_type = run_events[0].event_type.clone();
    let mut failed_gates = Vec::new();

    for event in &run_events {
        correlation_id = event.correlation_id;
        updated_at = event.occurred_at;
        last_event_type = event.event_type.clone();

        if let Some(next_state) = run_state_from_event(event) {
            state = next_state;
        }

        if event.event_type == "run.config_snapshot" {
            objective = event
                .payload
                .get("objective")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        if event.event_type == "run.gate_failed" {
            failed_gates = gate_failures_from_event(event);
        } else if matches!(
            event.event_type.as_str(),
            "run.activated" | "run.verifying" | "run.completed" | "run.cancelled"
        ) {
            failed_gates.clear();
        }
    }

    let by_correlation = query_events(store, &EventQuery::by_correlation(correlation_id))?;
    let task_events: Vec<Event> = by_correlation
        .into_iter()
        .filter(|event| event.entity_type == EntityType::Task)
        .collect();
    let tasks = collect_task_projections(&task_events);

    Ok(Some(RunProjection {
        run_id,
        state,
        correlation_id,
        updated_at,
        last_event_type,
        objective,
        tasks,
        failed_gates,
    }))
}

fn load_worktree_projection(
    store: &dyn EventStore,
    worktree_id: Uuid,
) -> Result<Option<WorktreeProjection>> {
    let events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Worktree, worktree_id.to_string()),
    )?;
    if events.is_empty() {
        return Ok(None);
    }

    let mut state = WorktreeState::WtUnbound;
    let mut correlation_id = events[0].correlation_id;
    let mut updated_at = events[0].occurred_at;
    let mut last_event_id = events[0].event_id;
    let mut last_event_type = events[0].event_type.clone();
    let mut reason = None;
    let mut run_id = None;

    for event in &events {
        correlation_id = event.correlation_id;
        updated_at = event.occurred_at;
        last_event_id = event.event_id;
        last_event_type = event.event_type.clone();

        if let Some(next_state) = parse_worktree_state(&extract_entity_state(event)) {
            state = next_state;
        }

        if let Some(next_reason) = event_reason(event) {
            reason = Some(next_reason);
        }

        if let Some(value) = event
            .payload
            .get("run_id")
            .and_then(|value| value.as_str())
            .and_then(|raw| raw.parse::<Uuid>().ok())
        {
            run_id = Some(value);
        }
    }

    Ok(Some(WorktreeProjection {
        worktree_id,
        state,
        correlation_id,
        updated_at,
        last_event_id,
        last_event_type,
        reason,
        run_id,
    }))
}

fn load_merge_projection(store: &dyn EventStore, merge_id: Uuid) -> Result<Option<MergeProjection>> {
    let events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Merge, merge_id.to_string()),
    )?;
    if events.is_empty() {
        return Ok(None);
    }

    let mut state = MergeState::MergeRequested;
    let mut run_id = None;
    let mut correlation_id = events[0].correlation_id;
    let mut updated_at = events[0].occurred_at;
    let mut last_event_id = events[0].event_id;
    let mut last_event_type = events[0].event_type.clone();
    let mut reason = None;
    let mut source_ref = None;
    let mut target_ref = None;
    let mut strategy = None;

    for event in &events {
        correlation_id = event.correlation_id;
        updated_at = event.occurred_at;
        last_event_id = event.event_id;
        last_event_type = event.event_type.clone();

        if let Some(next_state) = parse_merge_state(&extract_entity_state(event)) {
            state = next_state;
        }

        if let Some(next_reason) = event_reason(event) {
            reason = Some(next_reason);
        }

        if let Some(value) = event
            .payload
            .get("run_id")
            .and_then(|value| value.as_str())
            .and_then(|raw| raw.parse::<Uuid>().ok())
        {
            run_id = Some(value);
        }

        if let Some(value) = event.payload.get("source").and_then(|value| value.as_str()) {
            source_ref = Some(value.to_string());
        }

        if let Some(value) = event.payload.get("target").and_then(|value| value.as_str()) {
            target_ref = Some(value.to_string());
        }

        if let Some(value) = event
            .payload
            .get("strategy")
            .and_then(|value| value.as_str())
        {
            strategy = Some(value.to_string());
        }
    }

    Ok(Some(MergeProjection {
        merge_id,
        state,
        run_id,
        correlation_id,
        updated_at,
        last_event_id,
        last_event_type,
        reason,
        source_ref,
        target_ref,
        strategy,
    }))
}

fn render_run_status(store: &dyn EventStore, run_id: Uuid) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };

    let total_tasks = run.tasks.len();
    let complete = run
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::TaskComplete)
        .count();
    let failed = run
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::TaskFailed)
        .count();
    let blocked = run
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::TaskBlocked)
        .count();
    let active = run
        .tasks
        .iter()
        .filter(|task| {
            !matches!(
                task.state,
                TaskState::TaskComplete | TaskState::TaskFailed | TaskState::TaskCancelled
            )
        })
        .count();

    let mut out = String::new();
    writeln!(&mut out, "Run {}", run.run_id)?;
    writeln!(&mut out, "State: {:?}", run.state)?;
    writeln!(&mut out, "Last event: {}", run.last_event_type)?;
    writeln!(
        &mut out,
        "Updated: {}",
        run.updated_at.format("%Y-%m-%d %H:%M:%S")
    )?;
    if let Some(objective) = run.objective.as_ref() {
        writeln!(&mut out, "Objective: {objective}")?;
    }
    writeln!(
        &mut out,
        "Tasks: {total_tasks} total ({complete} complete, {failed} failed, {blocked} blocked, {active} active)"
    )?;

    if run.tasks.is_empty() {
        writeln!(&mut out, "No task events recorded for this run.")?;
    } else {
        writeln!(&mut out)?;
        writeln!(&mut out, "Task states:")?;
        for task in &run.tasks {
            writeln!(
                &mut out,
                "  {}  {:?}  ({})",
                task.task_id, task.state, task.last_event_type
            )?;
        }
    }

    Ok(out.trim_end().to_string())
}

fn render_run_explain(store: &dyn EventStore, run_id: Uuid) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };

    let snapshot = RunSnapshot {
        run_id,
        state: run.state,
        tasks: run
            .tasks
            .iter()
            .map(|task| TaskSnapshot {
                task_id: task.task_id,
                name: task.task_id.to_string(),
                state: task.state,
                blocked_by: Vec::new(),
                gates: task
                    .failed_gates
                    .iter()
                    .map(|(gate_type, reason)| (*gate_type, GateResult::Failed { reason: reason.clone() }))
                    .collect(),
                last_transition_at: Some(task.updated_at),
            })
            .collect(),
        gates: run
            .failed_gates
            .iter()
            .map(|(gate_type, reason)| (*gate_type, GateResult::Failed { reason: reason.clone() }))
            .collect(),
    };
    let explain = explain_run(&snapshot);

    let mut out = String::new();
    writeln!(&mut out, "Run explain for {run_id}")?;
    writeln!(&mut out, "Status: {:?}", explain.status)?;
    writeln!(&mut out, "Blocking tasks: {}", explain.blocking_tasks.len())?;
    writeln!(&mut out, "Failed gates: {}", explain.failed_gates.len())?;

    if !explain.blocking_tasks.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Blocking tasks:")?;
        for blocker in &explain.blocking_tasks {
            writeln!(
                &mut out,
                "  {} ({:?}) - {}",
                blocker.task_name, blocker.state, blocker.reason
            )?;
        }
    }

    if !explain.failed_gates.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Failed gates:")?;
        for failure in &explain.failed_gates {
            writeln!(
                &mut out,
                "  {} ({}) - {}",
                failure.gate_type.label(),
                failure.entity_id,
                failure.reason
            )?;
        }
    }

    if !explain.suggested_actions.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Suggested actions:")?;
        for action in &explain.suggested_actions {
            writeln!(&mut out, "  - {}", action.description)?;
        }
    }

    Ok(out.trim_end().to_string())
}

fn render_task_explain(store: &dyn EventStore, task_id: Uuid) -> Result<String> {
    let task = match load_task_projection(store, task_id)? {
        Some(task) => task,
        None => return Ok(format!("Task {task_id} not found in persisted event log.")),
    };

    let snapshot = TaskSnapshot {
        task_id,
        name: task.task_id.to_string(),
        state: task.state,
        blocked_by: Vec::new(),
        gates: task
            .failed_gates
            .iter()
            .map(|(gate_type, reason)| (*gate_type, GateResult::Failed { reason: reason.clone() }))
            .collect(),
        last_transition_at: Some(task.updated_at),
    };
    let explain = explain_task(&snapshot);

    let mut out = String::new();
    writeln!(&mut out, "Task explain for {task_id}")?;
    writeln!(&mut out, "Status: {:?}", explain.status)?;
    writeln!(&mut out, "State: {:?}", task.state)?;
    writeln!(&mut out, "Failed gates: {}", explain.failed_gates.len())?;

    if let Some(reason) = task.reason.as_ref() {
        writeln!(&mut out, "Last reason: {reason}")?;
    }

    if !explain.failed_gates.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Failed gates:")?;
        for failure in &explain.failed_gates {
            writeln!(&mut out, "  {} - {}", failure.gate_type.label(), failure.reason)?;
        }
    }

    if !explain.suggested_actions.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Suggested actions:")?;
        for action in &explain.suggested_actions {
            writeln!(&mut out, "  - {}", action.description)?;
        }
    }

    Ok(out.trim_end().to_string())
}

fn render_task_list(store: &dyn EventStore, run_id: Uuid) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };

    let mut out = String::new();
    writeln!(
        &mut out,
        "Tasks for run {} ({} total):",
        run.run_id,
        run.tasks.len()
    )?;
    if run.tasks.is_empty() {
        writeln!(&mut out, "No task events recorded.")?;
    } else {
        for task in &run.tasks {
            writeln!(
                &mut out,
                "  {}  {:?}  last={}{}",
                task.task_id,
                task.state,
                task.last_event_type,
                task.reason
                    .as_ref()
                    .map(|reason| format!(" reason={reason}"))
                    .unwrap_or_default()
            )?;
        }
    }
    Ok(out.trim_end().to_string())
}

fn execute_task_unblock(store: &dyn EventStore, task_id: Uuid, reason: &str) -> Result<String> {
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

fn append_worktree_transition_event(
    store: &dyn EventStore,
    projection: &WorktreeProjection,
    event_type: &str,
    from: WorktreeState,
    to: WorktreeState,
    action: &str,
    reason: &str,
    causation_id: Option<Uuid>,
) -> Result<Event> {
    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: chrono::Utc::now(),
        entity_type: EntityType::Worktree,
        entity_id: projection.worktree_id.to_string(),
        event_type: event_type.to_string(),
        payload: serde_json::json!({
            "from": format!("{from:?}"),
            "to": format!("{to:?}"),
            "action": action,
            "reason": reason,
            "run_id": projection.run_id,
        }),
        correlation_id: projection.correlation_id,
        causation_id,
        actor: "cli".to_string(),
        idempotency_key: None,
    };
    append_event(store, event.clone())?;
    Ok(event)
}

fn execute_worktree_recover(
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
        )?;
        current_state = WorktreeState::WtRecovering;
        causation_id = Some(started.event_id);
        persisted_events += 1;
    }

    match action {
        "abort" => {
            append_worktree_transition_event(
                store,
                &projection,
                "worktree.recovered",
                current_state,
                WorktreeState::WtBoundHome,
                action,
                "aborted interrupted operation and restored bound home state",
                causation_id,
            )?;
            current_state = WorktreeState::WtBoundHome;
            persisted_events += 1;
        }
        "resume" => {
            append_worktree_transition_event(
                store,
                &projection,
                "worktree.recovered",
                current_state,
                WorktreeState::WtBoundHome,
                action,
                "resumed interrupted operation and restored bound home state",
                causation_id,
            )?;
            current_state = WorktreeState::WtBoundHome;
            persisted_events += 1;
        }
        "manual-block" => {
            append_event(
                store,
                Event {
                    event_id: Uuid::now_v7(),
                    occurred_at: chrono::Utc::now(),
                    entity_type: EntityType::Worktree,
                    entity_id: projection.worktree_id.to_string(),
                    event_type: "worktree.recovery_blocked".to_string(),
                    payload: serde_json::json!({
                        "from": format!("{current_state:?}"),
                        "to": "WtRecovering",
                        "action": action,
                        "reason": "manual intervention required before recovery can continue",
                        "run_id": projection.run_id,
                    }),
                    correlation_id: projection.correlation_id,
                    causation_id,
                    actor: "cli".to_string(),
                    idempotency_key: None,
                },
            )?;
            current_state = WorktreeState::WtRecovering;
            persisted_events += 1;
        }
        _ => unreachable!("action validated by caller"),
    }

    let mut output = String::new();
    writeln!(&mut output, "Worktree recovery for {worktree_id}")?;
    writeln!(&mut output, "Action: {action}")?;
    writeln!(&mut output, "Resulting state: {current_state:?}")?;
    writeln!(&mut output, "Persisted events: {persisted_events}")?;
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

fn gate_result_status(result: &GateResult) -> &'static str {
    match result {
        GateResult::Passed { .. } => "passed",
        GateResult::Failed { .. } => "failed",
        GateResult::Pending => "pending",
    }
}

fn parse_command_class_value(value: &serde_json::Value) -> Option<CommandClass> {
    serde_json::from_value::<CommandClass>(value.clone()).ok().or_else(|| {
        value.as_str().and_then(|raw| match raw {
            "io" | "Io" => Some(CommandClass::Io),
            "cpu" | "Cpu" => Some(CommandClass::Cpu),
            "git" | "Git" => Some(CommandClass::Git),
            "tool" | "Tool" => Some(CommandClass::Tool),
            _ => None,
        })
    })
}

fn is_command_event_for_task(event: &Event, task_id: Uuid) -> bool {
    if event.entity_type != EntityType::Command {
        return false;
    }
    let Some(idempotency_key) = event.idempotency_key.as_deref() else {
        return false;
    };
    idempotency_key.starts_with(&format!("{task_id}:cmd:"))
}

fn collect_task_command_evidence(
    events: &[Event],
    task_id: Uuid,
    run_id: Uuid,
) -> (Vec<Evidence>, Option<CommandClass>) {
    let mut started_meta: HashMap<String, (String, Option<CommandClass>)> = HashMap::new();
    let mut terminal_events: Vec<&Event> = Vec::new();

    for event in events.iter().filter(|event| is_command_event_for_task(event, task_id)) {
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

fn policy_outcome_is_allowed(payload: &serde_json::Value) -> bool {
    payload
        .get("outcome")
        .and_then(|value| value.as_str())
        .map(|outcome| outcome.eq_ignore_ascii_case("ALLOW"))
        .unwrap_or(false)
}

fn build_task_gate_context(
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
    let all_tasks_complete =
        !task_states.is_empty() && task_states.iter().all(|(_, _, state)| *state == TaskState::TaskComplete);

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
    for event in events.iter().filter(|event| event.entity_type == EntityType::Policy) {
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

fn render_gate_rerun_output(
    task_id: Uuid,
    run_id: Uuid,
    selected_count: usize,
    evaluations: &[yarli_gates::GateEvaluation],
) -> Result<String> {
    let mut output = String::new();
    writeln!(&mut output, "Gate re-run for task {task_id}")?;
    writeln!(&mut output, "Run: {run_id}")?;
    writeln!(
        &mut output,
        "Evaluated {} gate(s):",
        selected_count
    )?;

    for evaluation in evaluations {
        match &evaluation.result {
            GateResult::Passed { evidence_ids } => {
                writeln!(
                    &mut output,
                    "  {}: PASS ({} evidence id(s))",
                    evaluation.gate_type.label(),
                    evidence_ids.len()
                )?;
            }
            GateResult::Failed { reason } => {
                writeln!(
                    &mut output,
                    "  {}: FAIL ({reason})",
                    evaluation.gate_type.label()
                )?;
            }
            GateResult::Pending => {
                writeln!(&mut output, "  {}: PENDING", evaluation.gate_type.label())?;
            }
        }
        if let Some(remediation) = evaluation.remediation.as_ref() {
            writeln!(&mut output, "    remediation: {remediation}")?;
        }
    }

    writeln!(
        &mut output,
        "Overall: {}",
        if all_passed(evaluations) {
            "all selected gates passed"
        } else {
            "one or more selected gates failed"
        }
    )?;

    Ok(output.trim_end().to_string())
}

fn execute_gate_rerun(
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
                idempotency_key: None,
            },
        )?;
    }

    render_gate_rerun_output(task_id, run_id, selected_gates.len(), &evaluations)
}

fn collect_entity_projections(events: &[Event]) -> Vec<EntityProjection> {
    let mut entities: BTreeMap<String, EntityProjection> = BTreeMap::new();

    for event in events {
        let entry = entities
            .entry(event.entity_id.clone())
            .or_insert_with(|| EntityProjection {
                entity_id: event.entity_id.clone(),
                state: extract_entity_state(event),
                updated_at: event.occurred_at,
                last_event_type: event.event_type.clone(),
                reason: event_reason(event),
            });

        entry.state = extract_entity_state(event);
        entry.updated_at = event.occurred_at;
        entry.last_event_type = event.event_type.clone();
        entry.reason = event_reason(event);
    }

    let mut values: Vec<_> = entities.into_values().collect();
    values.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.entity_id.cmp(&b.entity_id))
    });
    values
}

fn render_worktree_status(store: &dyn EventStore, run_id: Uuid) -> Result<String> {
    let run = match load_run_projection(store, run_id)? {
        Some(run) => run,
        None => return Ok(format!("Run {run_id} not found in persisted event log.")),
    };

    let events = query_events(store, &EventQuery::by_correlation(run.correlation_id))?;
    let worktree_events: Vec<Event> = events
        .into_iter()
        .filter(|event| event.entity_type == EntityType::Worktree)
        .collect();
    let worktrees = collect_entity_projections(&worktree_events);

    let mut out = String::new();
    writeln!(
        &mut out,
        "Worktrees for run {} ({} total):",
        run.run_id,
        worktrees.len()
    )?;

    if worktrees.is_empty() {
        writeln!(&mut out, "No persisted worktree events recorded yet.")?;
    } else {
        for worktree in &worktrees {
            writeln!(
                &mut out,
                "  {}  {}  last={}",
                worktree.entity_id, worktree.state, worktree.last_event_type
            )?;
        }
    }

    Ok(out.trim_end().to_string())
}

fn policy_outcome_label(outcome: PolicyOutcome) -> &'static str {
    match outcome {
        PolicyOutcome::Allow => "allow",
        PolicyOutcome::Deny => "deny",
        PolicyOutcome::RequireApproval => "require_approval",
    }
}

fn persist_merge_policy_decision(
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
            idempotency_key: None,
        },
    )
}

fn append_merge_policy_audit(
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

fn evaluate_merge_policy(
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

fn execute_merge_request(
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
                "source": source,
                "target": target,
                "strategy": strategy,
                "reason": "pending approval",
            }),
            correlation_id: run.correlation_id,
            causation_id: None,
            actor: "cli".to_string(),
            idempotency_key: None,
        },
    )?;

    let mut out = String::new();
    writeln!(&mut out, "Merge intent requested")?;
    writeln!(&mut out, "Merge ID: {merge_id}")?;
    writeln!(&mut out, "Run: {run_id}")?;
    writeln!(&mut out, "Source: {source}")?;
    writeln!(&mut out, "Target: {target}")?;
    writeln!(&mut out, "Strategy: {strategy}")?;
    writeln!(&mut out, "State: MergeRequested")?;
    Ok(out.trim_end().to_string())
}

fn execute_merge_approve(
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
                    idempotency_key: None,
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

    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Merge,
            entity_id: merge_id.to_string(),
            event_type: "merge.approved".to_string(),
            payload: serde_json::json!({
                "from": "MergeRequested",
                "to": "MergePrecheck",
                "state": "MergePrecheck",
                "run_id": merge.run_id,
                "source": merge.source_ref,
                "target": merge.target_ref,
                "strategy": merge.strategy,
                "reason": "approved by operator",
            }),
            correlation_id: merge.correlation_id,
            causation_id,
            actor: "cli".to_string(),
            idempotency_key: None,
        },
    )?;

    Ok(format!(
        "Merge intent {merge_id} transitioned MergeRequested -> MergePrecheck."
    ))
}

fn execute_merge_reject(
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
                    idempotency_key: None,
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

    append_event(
        store,
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Merge,
            entity_id: merge_id.to_string(),
            event_type: "merge.rejected".to_string(),
            payload: serde_json::json!({
                "from": "MergeRequested",
                "to": "MergeAborted",
                "state": "MergeAborted",
                "run_id": merge.run_id,
                "source": merge.source_ref,
                "target": merge.target_ref,
                "strategy": merge.strategy,
                "reason": reason,
            }),
            correlation_id: merge.correlation_id,
            causation_id,
            actor: "cli".to_string(),
            idempotency_key: None,
        },
    )?;

    Ok(format!(
        "Merge intent {merge_id} transitioned MergeRequested -> MergeAborted (reason: {reason})."
    ))
}

fn render_merge_status(store: &dyn EventStore, merge_id: Uuid) -> Result<String> {
    let merge = match load_merge_projection(store, merge_id)? {
        Some(merge) => merge,
        None => {
            return Ok(format!(
                "Merge intent {merge_id} not found in persisted event log."
            ))
        }
    };

    let mut out = String::new();
    writeln!(&mut out, "Merge intent {}", merge.merge_id)?;
    writeln!(&mut out, "State: {:?}", merge.state)?;
    writeln!(&mut out, "Last event: {}", merge.last_event_type)?;
    writeln!(
        &mut out,
        "Updated: {}",
        merge.updated_at.format("%Y-%m-%d %H:%M:%S")
    )?;
    if let Some(run_id) = merge.run_id {
        writeln!(&mut out, "Run: {run_id}")?;
    }
    if let Some(source) = merge.source_ref {
        writeln!(&mut out, "Source: {source}")?;
    }
    if let Some(target) = merge.target_ref {
        writeln!(&mut out, "Target: {target}")?;
    }
    if let Some(strategy) = merge.strategy {
        writeln!(&mut out, "Strategy: {strategy}")?;
    }
    if let Some(reason) = merge.reason {
        writeln!(&mut out, "Reason: {reason}")?;
    }

    Ok(out.trim_end().to_string())
}

/// `yarli run status` — print current run/task state.
fn cmd_run_status(run_id_str: &str) -> Result<()> {
    let run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_run_status(store, run_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli run explain-exit` — run the Why Not Done? engine.
fn cmd_run_explain(run_id_str: &str) -> Result<()> {
    let run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_run_explain(store, run_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli task list` — list tasks for a run.
fn cmd_task_list(run_id_str: &str) -> Result<()> {
    let run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_task_list(store, run_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli task explain` — run the Why Not Done? engine for one task.
fn cmd_task_explain(task_id_str: &str) -> Result<()> {
    let task_id: Uuid = task_id_str
        .parse()
        .context("invalid task ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_task_explain(store, task_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli info` — show version and terminal capabilities.
fn cmd_info(info: &TerminalInfo, render_mode: RenderMode, loaded_config: &LoadedConfig) -> Result<()> {
    println!("yarli v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Terminal:");
    println!("  TTY:     {}", info.is_tty);
    println!("  Size:    {}x{}", info.cols, info.rows);
    println!("  Dashboard capable: {}", info.supports_dashboard());
    println!();
    println!("Render mode: {:?}", render_mode);
    println!("Backend: {}", loaded_config.config().core.backend.as_str());
    println!("Config:  {} ({})", loaded_config.path().display(), loaded_config.source().label());
    Ok(())
}

/// `yarli task unblock` — clear a task's blocker.
fn cmd_task_unblock(task_id_str: &str, reason: &str) -> Result<()> {
    let task_id: Uuid = task_id_str
        .parse()
        .context("invalid task ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output =
        with_event_store(&loaded_config, |store| execute_task_unblock(store, task_id, reason))?;
    println!("{output}");
    Ok(())
}

/// `yarli gate list` — show configured gate types.
fn cmd_gate_list(run_level: bool) -> Result<()> {
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
fn cmd_gate_rerun(task_id_str: &str, gate_name: Option<&str>) -> Result<()> {
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

    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| execute_gate_rerun(store, task_id, gate_name))?;
    println!("{output}");
    Ok(())
}

/// `yarli worktree status` — show worktree state.
fn cmd_worktree_status(run_id_str: &str) -> Result<()> {
    let run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_worktree_status(store, run_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli worktree recover` — recover from interrupted git operation.
fn cmd_worktree_recover(worktree_id_str: &str, action: &str) -> Result<()> {
    let worktree_id: Uuid = worktree_id_str
        .parse()
        .context("invalid worktree ID (expected UUID)")?;
    match action {
        "abort" | "resume" | "manual-block" => {}
        _ => bail!("invalid recovery action: {action}. Use: abort, resume, or manual-block"),
    }
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| execute_worktree_recover(store, worktree_id, action))?;
    println!("{output}");
    Ok(())
}

/// `yarli merge request` — create a merge intent.
fn cmd_merge_request(source: &str, target: &str, run_id_str: &str, strategy: &str) -> Result<()> {
    let run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    let strategy = parse_merge_strategy(strategy).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid merge strategy: {strategy}. Use: merge-no-ff, rebase-then-ff, or squash-merge"
        )
    })?;

    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| {
        execute_merge_request(store, source, target, run_id, strategy)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli merge approve` — approve a merge intent.
fn cmd_merge_approve(merge_id_str: &str) -> Result<()> {
    let merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
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
fn cmd_merge_reject(merge_id_str: &str, reason: &str) -> Result<()> {
    let merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
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
fn cmd_merge_status(merge_id_str: &str) -> Result<()> {
    let merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| render_merge_status(store, merge_id))?;
    println!("{output}");
    Ok(())
}

/// `yarli audit tail` — tail the JSONL audit log.
fn cmd_audit_tail(file: &str, lines: usize, category: Option<&str>) -> Result<()> {
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
        println!();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn gate_status_glyph() -> &'static str {
    "○"
}

fn all_gate_names() -> Vec<&'static str> {
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

fn parse_gate_type(name: &str) -> Option<GateType> {
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

fn parse_merge_strategy(name: &str) -> Option<&'static str> {
    match name {
        "merge-no-ff" => Some("merge-no-ff"),
        "rebase-then-ff" => Some("rebase-then-ff"),
        "squash-merge" => Some("squash-merge"),
        _ => None,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::NamedTempFile;
    use yarli_observability::{AuditCategory, AuditEntry, InMemoryAuditSink};
    use yarli_store::event_store::EventQuery;
    use yarli_store::InMemoryEventStore;

    const VALID_UUID: &str = "00000000-0000-0000-0000-000000000000";

    fn make_event(
        entity_type: EntityType,
        entity_id: impl Into<String>,
        event_type: &str,
        correlation_id: Uuid,
        payload: serde_json::Value,
    ) -> Event {
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_type,
            entity_id: entity_id.into(),
            event_type: event_type.to_string(),
            payload,
            correlation_id,
            causation_id: None,
            actor: "test".to_string(),
            idempotency_key: None,
        }
    }

    #[test]
    fn render_run_status_reconstructs_persisted_state() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                corr,
                serde_json::json!({ "objective": "ship it" }),
            ))
            .unwrap();
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
                "task.ready",
                corr,
                serde_json::json!({ "from": "TaskOpen", "to": "TaskReady" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({ "from": "TaskVerifying", "to": "TaskComplete" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.completed",
                corr,
                serde_json::json!({ "from": "RunVerifying", "to": "RunCompleted" }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("RunCompleted"));
        assert!(output.contains(&task_id.to_string()));
        assert!(!output.contains("requires a persistent store"));
    }

    #[test]
    fn render_run_explain_reads_persisted_gate_failures() {
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
                "task.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.tests_passed: 2 test(s) failing",
                    "failures": ["gate.tests_passed: 2 test(s) failing"],
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.required_tasks_closed: 1 required task(s) not complete",
                    "failures": ["gate.required_tasks_closed: 1 required task(s) not complete"],
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(output.contains("Failed gates: 2"));
        assert!(output.contains("gate.tests_passed"));
        assert!(output.contains("gate.required_tasks_closed"));
    }

    #[test]
    fn render_task_explain_reads_persisted_gate_failures() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.required_evidence_present: no evidence records found",
                    "failures": ["gate.required_evidence_present: no evidence records found"],
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(output.contains("Task explain for"));
        assert!(output.contains("Failed gates: 1"));
        assert!(output.contains("gate.required_evidence_present"));
        assert!(output.contains("no evidence records found"));
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
            .query(&EventQuery::by_entity(EntityType::Task, task_id.to_string()))
            .unwrap();
        assert!(
            events.iter().any(|event| event.event_type == "task.unblocked"),
            "expected task.unblocked event to be persisted"
        );
    }

    #[test]
    fn render_worktree_status_reads_persisted_events() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let worktree_id = Uuid::now_v7();

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
                "worktree.bound",
                corr,
                serde_json::json!({ "to": "WtBoundHome" }),
            ))
            .unwrap();

        let output = render_worktree_status(&store, run_id).unwrap();
        assert!(output.contains(&worktree_id.to_string()));
        assert!(output.contains("WtBoundHome"));
    }

    #[test]
    fn render_merge_status_reads_persisted_events() {
        let store = InMemoryEventStore::new();
        let merge_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({ "state": "MergeRequested", "reason": "pending approval" }),
            ))
            .unwrap();

        let output = render_merge_status(&store, merge_id).unwrap();
        assert!(output.contains("MergeRequested"));
        assert!(output.contains("pending approval"));
    }

    // ── parse_gate_type ──────────────────────────────────────────────

    #[test]
    fn parse_gate_type_valid_names() {
        assert_eq!(
            parse_gate_type("required_tasks_closed"),
            Some(GateType::RequiredTasksClosed)
        );
        assert_eq!(
            parse_gate_type("tests_passed"),
            Some(GateType::TestsPassed)
        );
        assert_eq!(
            parse_gate_type("policy_clean"),
            Some(GateType::PolicyClean)
        );
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

    // ── UUID validation ──────────────────────────────────────────────

    #[test]
    fn cmd_task_unblock_rejects_invalid_uuid() {
        let result = cmd_task_unblock("not-a-uuid", "test reason");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_task_unblock_accepts_valid_uuid() {
        let result = cmd_task_unblock(VALID_UUID, "test reason");
        assert!(result.is_ok());
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

    // ── gate list ────────────────────────────────────────────────────

    #[test]
    fn cmd_gate_list_task_level() {
        assert!(cmd_gate_list(false).is_ok());
    }

    #[test]
    fn cmd_gate_list_run_level() {
        assert!(cmd_gate_list(true).is_ok());
    }

    // ── gate rerun validation ────────────────────────────────────────

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
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_gate_rerun_accepts_no_gate() {
        let result = cmd_gate_rerun(VALID_UUID, None);
        assert!(result.is_ok());
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
            .query(&EventQuery::by_entity(EntityType::Gate, task_id.to_string()))
            .unwrap();
        assert_eq!(gate_events.len(), 1);
        assert_eq!(gate_events[0].event_type, "gate.evaluated");
        assert_eq!(gate_events[0].payload["gate"], "gate.tests_passed");
        assert_eq!(gate_events[0].payload["status"], "passed");
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
            .query(&EventQuery::by_entity(EntityType::Gate, task_id.to_string()))
            .unwrap();
        assert_eq!(gate_events.len(), default_task_gates().len());
        assert!(
            gate_events
                .iter()
                .all(|event| event.event_type == "gate.evaluated")
        );
    }

    // ── worktree recover validation ──────────────────────────────────

    #[test]
    fn cmd_worktree_recover_accepts_valid_actions() {
        assert!(cmd_worktree_recover(VALID_UUID, "abort").is_ok());
        assert!(cmd_worktree_recover(VALID_UUID, "resume").is_ok());
        assert!(cmd_worktree_recover(VALID_UUID, "manual-block").is_ok());
    }

    #[test]
    fn cmd_worktree_recover_rejects_invalid_action() {
        let result = cmd_worktree_recover(VALID_UUID, "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn execute_worktree_recover_abort_persists_state_and_updates_status_projection() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let worktree_id = Uuid::now_v7();
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
                EntityType::Worktree,
                worktree_id.to_string(),
                "worktree.conflict_detected",
                corr,
                serde_json::json!({
                    "from": "WtMerging",
                    "to": "WtConflict",
                    "run_id": run_id,
                    "reason": "merge conflict requires recovery",
                }),
            ))
            .unwrap();

        let output = execute_worktree_recover(&store, worktree_id, "abort").unwrap();
        assert!(output.contains("Resulting state: WtBoundHome"));
        assert!(!output.contains("requires a persistent store"));

        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Worktree,
                worktree_id.to_string(),
            ))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "worktree.recovery_started")
        );
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "worktree.recovered")
        );

        let status_output = render_worktree_status(&store, run_id).unwrap();
        assert!(status_output.contains(&worktree_id.to_string()));
        assert!(status_output.contains("WtBoundHome"));
    }

    #[test]
    fn execute_worktree_recover_manual_block_persists_recovering_state() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let worktree_id = Uuid::now_v7();
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
                EntityType::Worktree,
                worktree_id.to_string(),
                "worktree.conflict_detected",
                corr,
                serde_json::json!({
                    "from": "WtMerging",
                    "to": "WtConflict",
                    "run_id": run_id,
                    "reason": "manual intervention required",
                }),
            ))
            .unwrap();

        let output = execute_worktree_recover(&store, worktree_id, "manual-block").unwrap();
        assert!(output.contains("Resulting state: WtRecovering"));
        assert!(!output.contains("requires a persistent store"));

        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Worktree,
                worktree_id.to_string(),
            ))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "worktree.recovery_blocked")
        );

        let status_output = render_worktree_status(&store, run_id).unwrap();
        assert!(status_output.contains("WtRecovering"));
    }

    // ── merge lifecycle commands ─────────────────────────────────────

    fn merge_id_from_output(output: &str) -> Uuid {
        output
            .lines()
            .find_map(|line| line.strip_prefix("Merge ID: "))
            .and_then(|raw| raw.trim().parse::<Uuid>().ok())
            .expect("expected merge ID line in output")
    }

    #[test]
    fn cmd_merge_request_accepts_valid_strategies() {
        assert!(cmd_merge_request("feat", "main", VALID_UUID, "merge-no-ff").is_ok());
        assert!(cmd_merge_request("feat", "main", VALID_UUID, "rebase-then-ff").is_ok());
        assert!(cmd_merge_request("feat", "main", VALID_UUID, "squash-merge").is_ok());
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

        let output = execute_merge_request(&store, "feat/login", "main", run_id, "merge-no-ff").unwrap();
        assert!(output.contains("Merge intent requested"));
        assert!(!output.contains("requires a persistent store"));

        let merge_id = merge_id_from_output(&output);
        let events = store
            .query(&EventQuery::by_entity(EntityType::Merge, merge_id.to_string()))
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "merge.requested");
        assert_eq!(events[0].payload["state"], "MergeRequested");
        assert_eq!(events[0].payload["source"], "feat/login");
        assert_eq!(events[0].payload["target"], "main");
        assert_eq!(events[0].payload["strategy"], "merge-no-ff");
    }

    #[test]
    fn execute_merge_approve_persists_policy_and_transition_events() {
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
                    "reason": "pending approval",
                }),
            ))
            .unwrap();

        let output = execute_merge_approve(
            &store,
            merge_id,
            SafeMode::Execute,
            true,
            Some(&audit),
        )
        .unwrap();
        assert!(output.contains("MergeRequested -> MergePrecheck"));
        assert!(!output.contains("requires a persistent store"));

        let merge_events = store
            .query(&EventQuery::by_entity(EntityType::Merge, merge_id.to_string()))
            .unwrap();
        assert!(
            merge_events
                .iter()
                .any(|event| event.event_type == "merge.approved")
        );

        let all_events = store.query(&EventQuery::by_correlation(corr)).unwrap();
        assert!(
            all_events
                .iter()
                .any(|event| event.event_type == "policy.decision")
        );

        let audit_entries = audit.read_all().unwrap();
        assert!(
            audit_entries
                .iter()
                .any(|entry| entry.category == AuditCategory::PolicyDecision)
        );
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

        let output = execute_merge_approve(
            &store,
            merge_id,
            SafeMode::Observe,
            true,
            Some(&audit),
        )
        .unwrap();
        assert!(output.contains("blocked by policy"));
        assert!(!output.contains("requires a persistent store"));

        let merge_events = store
            .query(&EventQuery::by_entity(EntityType::Merge, merge_id.to_string()))
            .unwrap();
        assert!(
            !merge_events
                .iter()
                .any(|event| event.event_type == "merge.approved")
        );
        assert!(
            merge_events
                .iter()
                .any(|event| event.event_type == "merge.policy_blocked")
        );

        let policy_event = store
            .query(&EventQuery::by_correlation(corr))
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == "policy.decision")
            .expect("expected policy decision event");
        assert_eq!(policy_event.payload["outcome"].as_str(), Some("DENY"));

        let audit_entries = audit.read_all().unwrap();
        assert!(
            audit_entries
                .iter()
                .any(|entry| entry.category == AuditCategory::DestructiveAttempt)
        );
    }

    #[test]
    fn execute_merge_reject_persists_rejected_transition() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let merge_id = Uuid::now_v7();
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
            .query(&EventQuery::by_entity(EntityType::Merge, merge_id.to_string()))
            .unwrap();
        let rejected = merge_events
            .iter()
            .find(|event| event.event_type == "merge.rejected")
            .expect("expected merge.rejected event");
        assert_eq!(rejected.payload["to"], "MergeAborted");
        assert_eq!(rejected.payload["reason"], "manual rejection");
    }

    // ── audit tail ───────────────────────────────────────────────────

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
            let entry = AuditEntry::gate_evaluation(
                &format!("gate_{i}"),
                true,
                "ok",
                Uuid::nil(),
                None,
            );
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
        let entry2 = AuditEntry::gate_evaluation(
            "tests_passed",
            true,
            "ok",
            Uuid::nil(),
            None,
        );
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
            let entry = AuditEntry::gate_evaluation(
                &format!("gate_{i}"),
                true,
                "ok",
                Uuid::nil(),
                None,
            );
            sink.append(&entry).unwrap();
        }

        // 0 = show all.
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 0, None);
        assert!(result.is_ok());
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
        )
        .await
        .unwrap();
        assert!(cancelled);

        let reg = scheduler.registry().read().await;
        assert_eq!(reg.get_task(&task_id).unwrap().state, TaskState::TaskCancelled);
        assert_eq!(reg.get_run(&run_id).unwrap().state, RunState::RunCancelled);
        drop(reg);

        let task_events = store
            .query(&EventQuery::by_entity(EntityType::Task, task_id.to_string()))
            .unwrap();
        assert!(
            task_events
                .iter()
                .any(|event| event.event_type == "task.cancelled")
        );

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert!(
            run_events
                .iter()
                .any(|event| event.event_type == "run.cancelled")
        );
    }
}
