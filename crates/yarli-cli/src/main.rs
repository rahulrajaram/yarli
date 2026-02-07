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

use yarli_cli::dashboard::{DashboardConfig, DashboardRenderer};
use yarli_cli::mode::{self, RenderMode, TerminalInfo};
use yarli_cli::stream::{StreamConfig, StreamEvent, StreamRenderer};
use yarli_core::domain::{CommandClass, SafeMode};
use yarli_core::entities::run::Run;
use yarli_core::entities::task::Task;
use yarli_core::explain::GateType;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_core::shutdown::ShutdownController;
use yarli_exec::LocalCommandRunner;
use yarli_gates::{default_run_gates, default_task_gates};
use yarli_observability::{AuditSink, JsonlAuditSink};
use yarli_queue::{InMemoryTaskQueue, Scheduler, SchedulerConfig};
use yarli_store::{EventStore, InMemoryEventStore};

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
        /// Working directory for command execution.
        #[arg(short, long, default_value = ".")]
        workdir: String,
        /// Command timeout in seconds (0 = no timeout).
        #[arg(long, default_value = "300")]
        timeout: u64,
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
            } => cmd_run_start(objective, cmd, workdir, timeout, _render_mode).await,
            RunAction::Status { run_id } => cmd_run_status(&run_id),
            RunAction::ExplainExit { run_id } => cmd_run_explain(&run_id),
        },
        Commands::Task { action } => match action {
            TaskAction::List { run_id } => cmd_task_list(&run_id),
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
        Commands::Info => cmd_info(&term_info, _render_mode),
    }
}

/// `yarli run start` — create a run, submit tasks, drive scheduler with stream output.
async fn cmd_run_start(
    objective: String,
    commands: Vec<String>,
    workdir: String,
    timeout_secs: u64,
    render_mode: RenderMode,
) -> Result<()> {
    if commands.is_empty() {
        bail!("at least one --cmd is required");
    }

    let store = Arc::new(InMemoryEventStore::new());
    let queue = Arc::new(InMemoryTaskQueue::new());
    let runner = Arc::new(LocalCommandRunner::new());

    let command_timeout = if timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(timeout_secs))
    };

    let config = SchedulerConfig {
        working_dir: workdir,
        command_timeout,
        ..SchedulerConfig::default()
    };

    let scheduler = Scheduler::new(queue, store.clone(), runner, config);

    // Build run and tasks.
    let run = Run::new(&objective, SafeMode::Execute);
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

    // Submit run.
    scheduler
        .submit_run(run, tasks)
        .await
        .context("failed to submit run")?;

    info!(run_id = %run_id, "run started: {objective}");

    // Set up shutdown controller.
    let shutdown = ShutdownController::new();
    let cancel = shutdown.token().clone();

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
    let renderer_handle = tokio::task::spawn_blocking(move || {
        run_renderer(rx, render_mode)
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
        other => {
            bail!("Run {run_id} ended in unexpected state: {other:?}");
        }
    }
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
                drop(tx);
                return Ok(());
            }
            _ = tick_interval.tick() => {
                tick_count += 1;

                // Run a scheduler tick.
                let _result = scheduler.tick().await
                    .context("scheduler tick failed")?;

                // Emit events by scanning the event store for new events.
                let all_events = store.all().map_err(|e| anyhow::anyhow!("{e}"))?;
                let new_events = &all_events[last_event_count..];
                last_event_count = all_events.len();

                for event in new_events {
                    let stream_event = event_to_stream_event(event, task_names);
                    if let Some(se) = stream_event {
                        let _ = tx.send(se);
                    }
                }

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
        | "task.retrying" => {
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

/// Run the renderer in a blocking context (requires raw terminal access).
fn run_renderer(
    mut rx: mpsc::UnboundedReceiver<StreamEvent>,
    render_mode: RenderMode,
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
                    // Final draw to show terminal state.
                    renderer.draw().context("dashboard final draw failed")?;
                    // Wait for user to press q.
                    loop {
                        if renderer.poll_input().context("dashboard input poll failed")? {
                            break;
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

/// `yarli run status` — print current run/task state.
fn cmd_run_status(run_id_str: &str) -> Result<()> {
    // In M1, there's no persistent storage — runs exist only in-memory during execution.
    // This command is a placeholder that will be wired to Postgres in later milestones.
    let _run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    println!("Run status requires a persistent store (coming in Milestone 2+).");
    println!("During `yarli run start`, status is shown live via stream mode.");
    Ok(())
}

/// `yarli run explain-exit` — run the Why Not Done? engine.
fn cmd_run_explain(run_id_str: &str) -> Result<()> {
    let _run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    println!("Explain-exit requires a persistent store (coming in Milestone 2+).");
    println!("During `yarli run start`, the Why Not Done? summary is shown in the viewport.");
    Ok(())
}

/// `yarli task list` — list tasks for a run.
fn cmd_task_list(run_id_str: &str) -> Result<()> {
    let _run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    println!("Task list requires a persistent store (coming in Milestone 2+).");
    println!("During `yarli run start`, tasks are shown live via stream mode.");
    Ok(())
}

/// `yarli info` — show version and terminal capabilities.
fn cmd_info(info: &TerminalInfo, render_mode: RenderMode) -> Result<()> {
    println!("yarli v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Terminal:");
    println!("  TTY:     {}", info.is_tty);
    println!("  Size:    {}x{}", info.cols, info.rows);
    println!("  Dashboard capable: {}", info.supports_dashboard());
    println!();
    println!("Render mode: {:?}", render_mode);
    Ok(())
}

/// `yarli task unblock` — clear a task's blocker.
fn cmd_task_unblock(task_id_str: &str, reason: &str) -> Result<()> {
    let _task_id: Uuid = task_id_str
        .parse()
        .context("invalid task ID (expected UUID)")?;
    println!("Task unblock requires a persistent store (not yet available).");
    println!("Reason provided: {reason}");
    println!("During `yarli run start`, task lifecycle is managed automatically by the scheduler.");
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
    let _task_id: Uuid = task_id_str
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
        println!("Gate re-run for `{name}` requires a persistent store (not yet available).");
    } else {
        println!("Gate re-run (all gates) requires a persistent store (not yet available).");
    }
    println!("During `yarli run start`, gates are evaluated automatically after task execution.");
    Ok(())
}

/// `yarli worktree status` — show worktree state.
fn cmd_worktree_status(run_id_str: &str) -> Result<()> {
    let _run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    println!("Worktree status requires a persistent store (not yet available).");
    println!("During `yarli run start`, worktree state is managed by the git engine.");
    println!();
    println!("Worktree lifecycle: Unbound -> Creating -> BoundHome -> Merging/Conflict -> Closed");
    Ok(())
}

/// `yarli worktree recover` — recover from interrupted git operation.
fn cmd_worktree_recover(worktree_id_str: &str, action: &str) -> Result<()> {
    let _worktree_id: Uuid = worktree_id_str
        .parse()
        .context("invalid worktree ID (expected UUID)")?;
    match action {
        "abort" | "resume" | "manual-block" => {}
        _ => bail!("invalid recovery action: {action}. Use: abort, resume, or manual-block"),
    }
    println!("Worktree recovery requires a persistent store (not yet available).");
    println!("Recovery action: {action}");
    println!("During `yarli run start`, interrupted operations are detected and handled automatically.");
    Ok(())
}

/// `yarli merge request` — create a merge intent.
fn cmd_merge_request(source: &str, target: &str, run_id_str: &str, strategy: &str) -> Result<()> {
    let _run_id: Uuid = run_id_str
        .parse()
        .context("invalid run ID (expected UUID)")?;
    match strategy {
        "merge-no-ff" | "rebase-then-ff" | "squash-merge" => {}
        _ => bail!("invalid merge strategy: {strategy}. Use: merge-no-ff, rebase-then-ff, or squash-merge"),
    }
    println!("Merge request requires a persistent store (not yet available).");
    println!("Intent: merge {source} into {target} using strategy `{strategy}`");
    println!("During `yarli run start`, merges are orchestrated by the merge engine.");
    Ok(())
}

/// `yarli merge approve` — approve a merge intent.
fn cmd_merge_approve(merge_id_str: &str) -> Result<()> {
    let _merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    println!("Merge approval requires a persistent store (not yet available).");
    println!("Approval tokens are validated by the policy engine before merge apply.");
    Ok(())
}

/// `yarli merge reject` — reject a merge intent.
fn cmd_merge_reject(merge_id_str: &str, reason: &str) -> Result<()> {
    let _merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    println!("Merge rejection requires a persistent store (not yet available).");
    println!("Reason: {reason}");
    Ok(())
}

/// `yarli merge status` — show merge intent status.
fn cmd_merge_status(merge_id_str: &str) -> Result<()> {
    let _merge_id: Uuid = merge_id_str
        .parse()
        .context("invalid merge intent ID (expected UUID)")?;
    println!("Merge status requires a persistent store (not yet available).");
    println!("Merge lifecycle: Requested -> Precheck -> DryRun -> Apply -> Verify -> Done");
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use yarli_observability::AuditEntry;

    const VALID_UUID: &str = "00000000-0000-0000-0000-000000000000";

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

    // ── merge request validation ─────────────────────────────────────

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
}
