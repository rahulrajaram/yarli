use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use yarli_cli::config::{
    AutoAdvancePolicy, BackendSelection, ExecutionRunner, LoadedConfig, PromptMode,
    DEFAULT_CONFIG_PATH,
};
use yarli_cli::dashboard::{DashboardConfig, DashboardRenderer};
use yarli_cli::mode::{self, RenderMode, TerminalInfo};
use yarli_cli::prompt;
use yarli_cli::stream::{HeadlessRenderer, StreamConfig, StreamEvent, StreamRenderer};
use yarli_core::domain::{CommandClass, EntityType, Event, Evidence, PolicyOutcome, SafeMode};
use yarli_core::entities::command_execution::{CommandResourceUsage, TokenUsage};
use yarli_core::entities::merge_intent::{MergeIntent, MergeStrategy};
use yarli_core::entities::run::Run;
use yarli_core::entities::task::Task;
use yarli_core::entities::worktree_binding::{SubmoduleMode, WorktreeBinding};
use yarli_core::explain::{
    explain_run, explain_task, DeteriorationFactor, DeteriorationReport, DeteriorationTrend,
    GateResult, GateType, RunSnapshot, TaskSnapshot,
};
use yarli_core::fsm::merge::MergeState;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_core::fsm::worktree::WorktreeState;
use yarli_core::shutdown::ShutdownController;
use yarli_exec::{
    CommandRequest, CommandResult, CommandRunner, LocalCommandRunner, OverwatchCommandRunner,
    OverwatchRunnerConfig,
};
use yarli_gates::{all_passed, default_run_gates, default_task_gates, evaluate_all, GateContext};
use yarli_git::error::{GitError, RecoveryAction};
use yarli_git::{LocalMergeOrchestrator, LocalWorktreeManager, MergeOrchestrator, WorktreeManager};
use yarli_memory::{
    MemoryCliAdapter, InsertMemory, MemoryAdapter, MemoryClass, MemoryQuery, ScopeId,
};
use yarli_observability::{AuditEntry, AuditSink, JsonlAuditSink};
use yarli_policy::{ActionType, PolicyEngine, PolicyRequest};
use yarli_queue::{
    InMemoryTaskQueue, PostgresTaskQueue, ResourceBudgetConfig, Scheduler, SchedulerConfig,
    TaskQueue,
};
use yarli_store::event_store::EventQuery;
use yarli_store::{EventStore, InMemoryEventStore, PostgresEventStore};

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

/// YARLI — Yet Another Orchestrator Loop Implementation.
///
/// Deterministic orchestrator with state machines, event log, and safe Git handling.
///
/// Default workflow: `yarli run` resolves prompt context and executes config-driven plan tranches.
/// Prompt resolution precedence:
/// 1. `yarli run --prompt-file <path>`
/// 2. `yarli.toml` `[run].prompt_file`
/// 3. Legacy fallback lookup for `PROMPT.md`
/// Recommended durability: use Postgres (`core.backend = "postgres"`); in-memory mode blocks writes
/// unless explicitly opted in via `core.allow_in_memory_writes = true`.
///
/// Optional memories: Backend-backed memory hints/storage can be enabled via `yarli.toml`
/// (`[memory.backend] enabled = true`). See `yarli init --help` for config keys.
#[derive(Parser)]
#[command(name = "yarli", version = YARLI_VERSION, about)]
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

const INIT_LONG_ABOUT: &str = r#"Initialize `yarli.toml` with a fully annotated template.

This command is the fastest way to bootstrap YARLI configuration for local dev,
CI, or production-like runs.

`yarli run` is opinionated and resolves prompt context while driving execution
from runtime config and `IMPLEMENTATION_PLAN.md`; this
config file controls runtime behavior (durability, execution runner, budgets,
and scheduler caps).

Configuration sections and properties you can tune:

[core]
- core.backend (default: "in-memory"; set "postgres" for durable writes)
- core.allow_in_memory_writes (default: false; explicit ephemeral override)
- core.safe_mode (default: "execute"; values: observe|restricted|execute|breakglass)
- core.worker_id (optional worker identity for scheduler/audit attribution)

[postgres]
- postgres.database_url (required when core.backend = "postgres")

Recommended for consumers: use Postgres (`core.backend = "postgres"`) for durable runs.
`core.backend = "in-memory"` is intended for local throwaway usage only and blocks write
commands unless `core.allow_in_memory_writes = true`.

[cli]
- cli.backend (optional; codex|claude|gemini|custom)
- cli.prompt_mode (default: "arg"; values: arg|stdin)
- cli.command (optional; executable to invoke)
- cli.args (default: []; argv list)

[event_loop]
- event_loop.max_iterations (default: 5; reserved for future iterative loop controls)
- event_loop.max_runtime_seconds (default: 14400; reserved for future iterative loop controls)
- event_loop.idle_timeout_secs (default: 1800; reserved for future iterative loop controls)
- event_loop.checkpoint_interval (default: 5; reserved for future iterative loop controls)

[features]
- features.parallel (default: false; when false, force scheduler concurrency caps to 1)

[queue]
- queue.claim_batch_size (default: 4)
- queue.lease_ttl_seconds (default: 30)
- queue.heartbeat_interval_seconds (default: 5)
- queue.reclaim_interval_seconds (default: 10)
- queue.reclaim_grace_seconds (default: 5)
- queue.per_run_cap (default: 8)
- queue.io_cap (default: 16)
- queue.cpu_cap (default: 4)
- queue.git_cap (default: 2)
- queue.tool_cap (default: 8)

[execution]
- execution.runner (default: "native"; values: native|overwatch)
- execution.working_dir (default: ".")
- execution.command_timeout_seconds (default: 300; 0 disables timeout)
- execution.tick_interval_ms (default: 100)
- execution.overwatch.service_url (required when runner = "overwatch")
- execution.overwatch.profile (optional)
- execution.overwatch.soft_timeout_seconds (optional)
- execution.overwatch.silent_timeout_seconds (optional)
- execution.overwatch.max_log_bytes (optional)

[run]
- run.prompt_file (optional; default prompt file for `yarli run`, relative to repo root)
- run.continue_wait_timeout_seconds (default: 0; seconds to wait for continuation availability before failing)
- run.allow_stable_auto_advance (legacy compatibility toggle; prefer run.auto_advance_policy)
- run.auto_advance_policy (default: improving-only; values: improving-only|stable-ok|always)
- run.max_auto_advance_tranches (default: 0; 0 = unlimited auto-advance per invocation)
- run.default_pace (legacy; used by `yarli run batch`)
- run.paces.<name>.cmds (legacy; list of commands for the named pace)
- run.paces.<name>.working_dir (legacy; per-pace working dir override)
- run.paces.<name>.command_timeout_seconds (legacy; per-pace timeout override)

[budgets] (all optional; unset => unlimited)
- budgets.max_task_rss_bytes
- budgets.max_task_cpu_user_ticks
- budgets.max_task_cpu_system_ticks
- budgets.max_task_io_read_bytes
- budgets.max_task_io_write_bytes
- budgets.max_task_total_tokens
- budgets.max_run_total_tokens
- budgets.max_run_peak_rss_bytes
- budgets.max_run_cpu_user_ticks
- budgets.max_run_cpu_system_ticks
- budgets.max_run_io_read_bytes
- budgets.max_run_io_write_bytes

[git]
- git.default_target_branch (default: "main")
- git.destructive_default_deny (default: true)

[policy]
- policy.enforce_policies (default: true)
- policy.audit_decisions (default: true)

[memory.backend]
- memory.backend.enabled (default: false)
- memory.backend.endpoint (optional)
- memory.backend.command (default: "memory-backend")
- memory.backend.project_dir (optional; defaults to prompt root)
- memory.backend.query_limit (default: 8)
- memory.backend.inject_on_run_start (default: true)
- memory.backend.inject_on_failure (default: true)

[memory]
- memory.enabled (optional; master switch, defaults to memory.backend.enabled when unset)
- memory.project_id (optional; defaults to prompt-root directory name)

[observability]
- observability.audit_file (default: ".yarl/audit.jsonl")
- observability.log_level (optional)

[ui]
- ui.mode (default: "auto"; values: auto|stream|tui)

[sw4rm] (requires `--features sw4rm` at build time)
- sw4rm.agent_name (default: "yarli-orchestrator")
- sw4rm.capabilities (default: ["orchestrate", "verify", "git"])
- sw4rm.registry_url (default: "http://127.0.0.1:50051")
- sw4rm.router_url (default: "http://127.0.0.1:50052")
- sw4rm.scheduler_url (default: "http://127.0.0.1:50053")
- sw4rm.max_fix_iterations (default: 5)
- sw4rm.llm_response_timeout_secs (default: 300)

Examples:
- yarli init
- yarli init --path ./config/yarli.toml
- yarli init --force
- yarli init --print
- yarli init --backend codex
- yarli init --backend claude --print
- yarli init --backend gemini --path ./yarli.toml --force
"#;

const INIT_CONFIG_TEMPLATE: &str = r#"# YARLI runtime configuration
# Generated by: yarli init
#
# Start conservative, then tune by measured behavior.
# All values are editable; comments explain intent and defaults.

[core]
# Backend for run/task/event persistence.
# - "in-memory": ephemeral; write commands are blocked unless allow_in_memory_writes=true.
# - "postgres": durable production-style mode.
backend = "in-memory"

# Explicit ephemeral override.
# Keep false for durable-by-default safety; set true only for local throwaway usage.
allow_in_memory_writes = false

# Safety policy mode: observe | restricted | execute | breakglass
safe_mode = "execute"

# Optional worker identity for scheduler/audit attribution.
# worker_id = "worker-1"

[postgres]
# Required when core.backend = "postgres".
# database_url = "postgres://postgres:postgres@localhost:5432/yarli"

# --- CLI_BACKEND_BEGIN ---
[cli]
# LLM CLI backend configuration (used by default `yarli run` plan-driven dispatch).
# backend = "codex" | "claude" | "gemini" | "custom"
# prompt_mode = "arg" | "stdin"
# command = "codex"
# args = ["exec", "--json"]

[event_loop]
# Reserved for future iterative loop controls.
max_iterations = 5
max_runtime_seconds = 14400
idle_timeout_secs = 1800
checkpoint_interval = 5

[features]
# Parallel work controls scheduler concurrency caps only (no parallel worktrees).
parallel = false
# --- CLI_BACKEND_END ---

[queue]
# How many ready tasks a worker claims per tick.
claim_batch_size = 4
# Lease timeout for claimed tasks.
lease_ttl_seconds = 30
# Lease heartbeat interval.
heartbeat_interval_seconds = 5
# Stale lease sweep interval.
reclaim_interval_seconds = 10
# Additional grace before reclaiming stale leases.
reclaim_grace_seconds = 5
# Concurrency caps.
per_run_cap = 8
io_cap = 16
cpu_cap = 4
git_cap = 2
tool_cap = 8

[execution]
# Runner backend: native | overwatch
runner = "native"
# Working directory for command execution.
working_dir = "."
# Default command timeout in seconds (0 disables timeout).
command_timeout_seconds = 300
# Scheduler tick cadence.
tick_interval_ms = 100

[execution.overwatch]
# Overwatch service settings (used only when execution.runner = "overwatch").
service_url = "http://127.0.0.1:8089"
# profile = "default"
# soft_timeout_seconds = 300
# silent_timeout_seconds = 120
# max_log_bytes = 131072

[run]
# Optional default prompt file for `yarli run`.
# Resolution precedence: --prompt-file > run.prompt_file > PROMPT.md fallback.
# prompt_file = "PROMPT.md"
# Seconds to wait for continuation payload availability (`yarli run continue`).
continue_wait_timeout_seconds = 0
# Legacy compatibility toggle for stable-trend auto-advance.
allow_stable_auto_advance = false
# Preferred auto-advance policy: improving-only | stable-ok | always
auto_advance_policy = "improving-only"
# Maximum planned-tranche auto-advances per invocation (0 = unlimited).
max_auto_advance_tranches = 0
# default_pace = "batch"

[budgets]
# Optional hard limits. Leave commented/unset for no limit.
# max_task_rss_bytes = 1073741824
# max_task_cpu_user_ticks = 100000
# max_task_cpu_system_ticks = 100000
# max_task_io_read_bytes = 1073741824
# max_task_io_write_bytes = 1073741824
# max_task_total_tokens = 25000
# max_run_total_tokens = 250000
# max_run_peak_rss_bytes = 2147483648
# max_run_cpu_user_ticks = 500000
# max_run_cpu_system_ticks = 500000
# max_run_io_read_bytes = 4294967296
# max_run_io_write_bytes = 4294967296

[git]
default_target_branch = "main"
destructive_default_deny = true

[policy]
enforce_policies = true
audit_decisions = true

[memory.backend]
enabled = false
# [memory]
# enabled = true
# project_id = "my-project"
# command = "memory-backend"
# project_dir = "."            # defaults to the directory containing PROMPT.md
# query_limit = 8
# inject_on_run_start = true
# inject_on_failure = true
#
# Bootstrap Memory-backend for a repository:
# - `memory-backend init -y`
#
# Then YARLI can store/query memories during `yarli run` when enabled=true.
# endpoint is reserved for a future native gRPC/HTTP adapter.
# endpoint = "http://localhost:8080"

[observability]
audit_file = ".yarl/audit.jsonl"
# log_level = "info"

[ui]
# auto | stream | tui
mode = "auto"
"#;

fn init_config_template(backend: Option<InitBackend>) -> String {
    let base = INIT_CONFIG_TEMPLATE.to_string();
    let replacement = match backend {
        None => {
            r#"[cli]
# LLM CLI backend configuration (used by default `yarli run` plan-driven dispatch).
# backend = "codex" | "claude" | "gemini" | "custom"
# prompt_mode = "arg" | "stdin"
# command = "codex"
# args = ["exec", "--json"]

[event_loop]
# Opinionated loop controls (used by `yarli run`).
max_iterations = 5
max_runtime_seconds = 14400
idle_timeout_secs = 1800
checkpoint_interval = 5

[features]
# Parallel work controls scheduler concurrency caps only (no parallel worktrees).
parallel = false
"#
        }
        Some(InitBackend::Codex) => {
            r#"[cli]
backend = "codex"
prompt_mode = "arg"
command = "codex"
args = ["exec", "--json", "--dangerously-bypass-approvals-and-sandbox"]

[event_loop]
max_iterations = 5
max_runtime_seconds = 14400
idle_timeout_secs = 1800
checkpoint_interval = 5

[features]
parallel = false
"#
        }
        Some(InitBackend::Claude) => {
            r#"[cli]
backend = "claude"
prompt_mode = "arg"
command = "claude"
args = ["--model", "sonnet-4.5"]

[event_loop]
max_iterations = 5
max_runtime_seconds = 14400
idle_timeout_secs = 1800
checkpoint_interval = 5

[features]
parallel = false
"#
        }
        Some(InitBackend::Gemini) => {
            r#"[cli]
backend = "gemini"
prompt_mode = "arg"
command = "gemini"
args = ["--model", "gemini-2.0-flash"]

[event_loop]
max_iterations = 5
max_runtime_seconds = 14400
idle_timeout_secs = 1800
checkpoint_interval = 5

[features]
parallel = false
"#
        }
    };

    replace_between(
        &base,
        "# --- CLI_BACKEND_BEGIN ---",
        "# --- CLI_BACKEND_END ---",
        replacement,
    )
}

fn replace_between(haystack: &str, begin: &str, end: &str, replacement: &str) -> String {
    let begin_idx = haystack
        .find(begin)
        .unwrap_or_else(|| panic!("template missing begin marker {begin}"));
    let end_idx = haystack
        .find(end)
        .unwrap_or_else(|| panic!("template missing end marker {end}"));
    assert!(end_idx >= begin_idx, "template markers out of order");

    let mut out = String::with_capacity(haystack.len() + replacement.len());
    out.push_str(&haystack[..begin_idx]);
    out.push_str(replacement);
    out.push('\n');
    out.push_str(&haystack[end_idx + end.len()..]);
    out
}

#[derive(Subcommand)]
enum Commands {
    #[command(
        about = "Manage orchestration runs (default: config-first plan-driven execution)",
        long_about = "Manage orchestration runs.\n\nDefault behavior:\n- `yarli run` (no subcommand) resolves prompt context in this order:\n  1. `--prompt-file <path>`\n  2. `[run].prompt_file` in `yarli.toml`\n  3. fallback lookup of `PROMPT.md`\n- `yarli run` then discovers incomplete tranches from `IMPLEMENTATION_PLAN.md` and dispatches them via `[cli]` command settings, followed by a verification task.\n- If no incomplete tranches are found, `yarli run` executes verification-only.\n- Legacy prompt task/tranche orchestration is used only as fallback when config-first dispatch cannot be materialized.\n\nOptional integrations:\n- Memories: enable Backend-backed hints/storage via `yarli.toml` (`[memory.backend] enabled = true`). Memory hints are surfaced in `yarli run status` and `yarli run explain-exit`.\n\nExamples:\n- `yarli run`\n- `yarli run --prompt-file prompts/I8B.md --stream`\n\nOther subcommands:\n- `yarli run start ...` for ad-hoc runs with explicit `--cmd`.\n- `yarli run status ...` / `yarli run explain-exit ...` for inspection.\n- `yarli run batch ...` is legacy/back-compat pace-based execution."
    )]
    Run {
        /// Override the prompt file used by default `yarli run` (no subcommand).
        #[arg(long)]
        prompt_file: Option<PathBuf>,
        #[command(subcommand)]
        action: Option<RunAction>,
    },
    #[command(
        about = "Manage tasks within a run",
        long_about = "Manage tasks within a run.\n\nExamples:\n- `yarli task list <run-id>`\n- `yarli task explain <task-id>`\n- `yarli task unblock <task-id> --reason \"...\"`\n\nNotes:\n- `task unblock` is a write command and requires Postgres durability or explicit in-memory opt-in."
    )]
    Task {
        #[command(subcommand)]
        action: TaskAction,
    },
    #[command(
        about = "Manage verification gates",
        long_about = "Manage verification gates.\n\nExamples:\n- `yarli gate list`\n- `yarli gate list --run`\n- `yarli gate rerun <task-id>`\n- `yarli gate rerun <task-id> --gate tests_passed`\n\nNotes:\n- `gate rerun` is a write command and requires Postgres durability or explicit in-memory opt-in."
    )]
    Gate {
        #[command(subcommand)]
        action: GateAction,
    },
    #[command(
        about = "Manage Git worktrees",
        long_about = "Manage Git worktrees.\n\nExamples:\n- `yarli worktree status <run-id>`\n- `yarli worktree recover <worktree-id> --action abort`\n\nNotes:\n- Recovery actions are policy-gated write operations and are audited."
    )]
    Worktree {
        #[command(subcommand)]
        action: WorktreeAction,
    },
    #[command(
        about = "Manage merge intents",
        long_about = "Manage merge intents.\n\nExamples:\n- `yarli merge request <source> <target> --run-id <run-id> --strategy merge-no-ff`\n- `yarli merge approve <merge-id>`\n- `yarli merge reject <merge-id> --reason \"...\"`\n- `yarli merge status <merge-id>`\n\nNotes:\n- Approve/reject are policy-gated write operations and are audited."
    )]
    Merge {
        #[command(subcommand)]
        action: MergeAction,
    },
    #[command(
        about = "View the audit log",
        long_about = "View the audit log.\n\nExamples:\n- `yarli audit tail`\n- `yarli audit tail --file .yarl/audit.jsonl --lines 200`\n- `yarli audit tail --category policy_decision`"
    )]
    Audit {
        #[command(subcommand)]
        action: AuditAction,
    },
    #[command(
        about = "Initialize yarli.toml with a documented template",
        long_about = INIT_LONG_ABOUT
    )]
    Init {
        /// Destination path for generated config.
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        path: PathBuf,
        /// Overwrite an existing config file.
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Print the config template to stdout instead of writing a file.
        #[arg(long, default_value_t = false)]
        print: bool,
        /// Emit an opinionated template for a specific LLM CLI backend.
        #[arg(long)]
        backend: Option<InitBackend>,
    },
    #[command(
        about = "Show version and detected terminal capabilities",
        long_about = "Show version and detected terminal capabilities.\n\nExample:\n- `yarli info`"
    )]
    Info,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::upper_case_acronyms)]
enum InitBackend {
    Codex,
    Claude,
    Gemini,
}

#[derive(Subcommand)]
enum RunAction {
    #[command(
        about = "Start a new orchestration run",
        long_about = "Start a new orchestration run with an explicit objective and commands.\n\nProvide commands via `--cmd` (repeatable) or reference a named pace from\nyarli.toml via `--pace`. The two are mutually exclusive.\n\nExamples:\n  yarli run start \"fix linting\" --cmd \"cargo clippy -- -D warnings\"\n  yarli run start \"full check\" -c \"cargo fmt --check\" -c \"cargo test\"\n  yarli run start \"deploy\" --pace deploy -w /opt/app\n  yarli run start \"build\" --cmd \"make\" --timeout 600"
    )]
    Start {
        /// The objective describing what this run should accomplish.
        objective: String,
        /// Commands to execute (one per task). Use multiple times for multiple tasks.
        #[arg(short, long)]
        cmd: Vec<String>,
        /// Named run pace defined in yarli.toml ([run.paces.<name>]). Mutually exclusive with --cmd.
        #[arg(long)]
        pace: Option<String>,
        /// Working directory for command execution (defaults to `execution.working_dir`).
        #[arg(short, long)]
        workdir: Option<String>,
        /// Command timeout in seconds (defaults to `execution.command_timeout_seconds`, 0 = no timeout).
        #[arg(long)]
        timeout: Option<u64>,
    },
    #[command(
        about = "Start the default verification loop (config-backed) for this workspace",
        long_about = "Start the default verification loop using a named pace from yarli.toml.\n\nResolves the pace in this order:\n1. Explicit `--pace` argument\n2. A pace named \"batch\" if it exists in [run.paces]\n3. The value of `run.default_pace`\n\nThis is a legacy/back-compat entry point; prefer prompt-run-spec `yarli run` for\nnew projects.\n\nExamples:\n  yarli run batch\n  yarli run batch --pace ci\n  yarli run batch --objective \"nightly check\" -w /repo --timeout 900"
    )]
    Batch {
        /// Optional objective label (defaults to "batch").
        #[arg(long)]
        objective: Option<String>,
        /// Named run pace to use (defaults to "batch" if defined, otherwise run.default_pace).
        #[arg(long)]
        pace: Option<String>,
        /// Working directory for command execution (overrides pace/config defaults).
        #[arg(short, long)]
        workdir: Option<String>,
        /// Command timeout in seconds (overrides pace/config defaults, 0 = no timeout).
        #[arg(long)]
        timeout: Option<u64>,
    },
    #[command(
        about = "Show the current status of a run",
        long_about = "Show the current status of a run.\n\nDisplays the run state, objective, task summary, and gate evaluation results.\nIf Memory backend is enabled, includes relevant memory hints.\n\n`run-id` may be a full UUID or a unique prefix from `yarli run list`.\n\nExamples:\n  yarli run status 019577a2-...\n  yarli run status <run-id>"
    )]
    Status {
        /// Run ID to query (UUID).
        run_id: String,
    },
    #[command(
        about = "Explain why a run is not done (Why Not Done? engine)",
        long_about = "Explain why a run is not done (Why Not Done? engine).\n\nRuns the explain engine to diagnose why a run hasn't completed. Reports:\n- Open/blocked tasks and their blockers\n- Failed gate evaluations\n- Policy denials\n- Deterioration trends (repeated failures)\n\n`run-id` may be a full UUID or a unique prefix from `yarli run list`.\n\nExamples:\n  yarli run explain-exit 019577a2-...\n  yarli run explain-exit <run-id>"
    )]
    ExplainExit {
        /// Run ID to explain (UUID).
        run_id: String,
    },
    #[command(
        about = "List all known runs",
        long_about = "List all runs found in the event store.\n\nShows each run's ID (short), state, objective, task counts, and last update time.\nUseful for discovering active runs so you can query status or explain.\n\nExamples:\n  yarli run list"
    )]
    List,
    #[command(
        about = "Continue from a previous run's unfinished/failed tasks",
        long_about = "Continue from a previous run using the auto-tranche continuation spec.\n\nBy default, yarli first loads the latest persisted continuation payload from\nthe event store (`run.continuation`), then falls back to `.yarli/continuation.json`\n(written automatically on run exit). It then creates a new run from the suggested\nnext tranche (retry/unfinished or planned-next).\nWhen continuation is not yet available, wait behavior is configured via\n`[run] continue_wait_timeout_seconds` in yarli.toml.\nAfter each successful run, yarli auto-advances through planned tranches when\nquality gate criteria allow it.\n\nExamples:\n  yarli run continue\n  yarli run continue --file .yarli/continuation.json"
    )]
    Continue {
        /// Path to the continuation file (defaults to `.yarli/continuation.json`).
        #[arg(long, default_value = DEFAULT_CONTINUATION_FILE)]
        file: PathBuf,
    },
    #[cfg(feature = "sw4rm")]
    #[command(
        about = "Run as a sw4rm orchestrator agent (requires `sw4rm` feature)",
        long_about = "Run as a sw4rm orchestrator agent.\n\nBoots yarli as an agent in the sw4rm multi-agent protocol. The agent:\n1. Registers with the sw4rm registry\n2. Receives objectives from the sw4rm scheduler\n3. Dispatches implementation work to LLM agents via the router\n4. Verifies results using yarli's scheduler and gate engine\n5. Iterates (dispatch -> verify -> fix) until success or max iterations\n\nConfiguration is read from the `[sw4rm]` section of yarli.toml:\n- sw4rm.agent_name (default: \"yarli-orchestrator\")\n- sw4rm.capabilities (default: [\"orchestrate\", \"verify\", \"git\"])\n- sw4rm.registry_url (default: \"http://127.0.0.1:50051\")\n- sw4rm.router_url (default: \"http://127.0.0.1:50052\")\n- sw4rm.scheduler_url (default: \"http://127.0.0.1:50053\")\n- sw4rm.max_fix_iterations (default: 5)\n- sw4rm.llm_response_timeout_secs (default: 300)\n\nVerification commands are loaded using the same prompt resolution precedence as `yarli run` when available; otherwise defaults to `cargo build`.\n\nNote: requires `--features sw4rm` at build time.\n\nExamples:\n  yarli run sw4rm\n  YARLI_LOG=debug yarli run sw4rm"
    )]
    Sw4rm,
}

#[derive(Subcommand)]
enum TaskAction {
    #[command(
        about = "List tasks for a run",
        long_about = "List tasks for a run.\n\nShows each task's ID, key, state, command class, and blocker (if any).\n\nExamples:\n  yarli task list 019577a2-...\n  yarli task list <run-id>"
    )]
    List {
        /// Run ID to list tasks for (UUID).
        run_id: String,
    },
    #[command(
        about = "Explain why a task is not done (Why Not Done? engine)",
        long_about = "Explain why a task is not done (Why Not Done? engine).\n\nRuns the explain engine on a single task to diagnose why it hasn't completed.\nReports blockers, dependency status, gate failures, and attempt history.\n\nExamples:\n  yarli task explain 019577a3-...\n  yarli task explain <task-id>"
    )]
    Explain {
        /// Task ID to explain (UUID).
        task_id: String,
    },
    #[command(
        about = "Unblock a task (clear its blocker and transition to ready)",
        long_about = "Unblock a task (clear its blocker and transition to ready).\n\nClears the task's blocker and transitions it from Blocked to Ready so the\nscheduler can pick it up again. This is a write operation that requires\nPostgres durability or explicit in-memory opt-in.\n\nExamples:\n  yarli task unblock 019577a3-... --reason \"dependency resolved manually\"\n  yarli task unblock <task-id>\n  yarli task unblock <task-id> -r \"approved by operator\""
    )]
    Unblock {
        /// Task ID to unblock (UUID).
        task_id: String,
        /// Reason for unblocking (recorded in audit log).
        #[arg(short, long, default_value = "manually unblocked")]
        reason: String,
    },
    #[command(
        about = "Annotate a task with blocker detail (e.g. link to blocker file)",
        long_about = "Annotate a task with blocker detail.\n\nSets a free-form annotation on the task that is displayed in `task explain`\nand `run status` output. Useful for linking to external blocker files\nor adding context about why a task is blocked.\n\nExamples:\n  yarli task annotate 019577a3-... --detail \"see blocker-001.md\"\n  yarli task annotate <task-id> -d \"waiting on upstream fix\""
    )]
    Annotate {
        /// Task ID to annotate (UUID).
        task_id: String,
        /// Blocker detail text to attach to the task.
        #[arg(short, long)]
        detail: String,
    },
}

#[derive(Subcommand)]
enum GateAction {
    #[command(
        about = "List configured gates for a run or task",
        long_about = "List configured gates for a run or task.\n\nBy default lists task-level gates. Use `--run` to show run-level gates instead.\n\nTask gates: required_evidence_present, tests_passed, no_unresolved_conflicts,\n            worktree_consistent, policy_clean\nRun gates:  required_tasks_closed, required_evidence_present, no_unapproved_git_ops,\n            no_unresolved_conflicts, worktree_consistent, policy_clean\n\nExamples:\n  yarli gate list\n  yarli gate list --run"
    )]
    List {
        /// Show run-level gates instead of the default task-level gates.
        #[arg(long)]
        run: bool,
    },
    #[command(
        about = "Re-run a specific gate evaluation",
        long_about = "Re-run gate evaluation for a task.\n\nRe-evaluates all gates for the given task, or a single gate if `--gate` is\nspecified. This is a write operation that requires Postgres durability or\nexplicit in-memory opt-in.\n\nValid gate names: required_tasks_closed, required_evidence_present, tests_passed,\n                  no_unapproved_git_ops, no_unresolved_conflicts, worktree_consistent,\n                  policy_clean\n\nExamples:\n  yarli gate rerun 019577a3-...\n  yarli gate rerun <task-id> --gate tests_passed\n  yarli gate rerun <task-id> -g policy_clean"
    )]
    Rerun {
        /// Task ID to re-evaluate gates for (UUID).
        task_id: String,
        /// Specific gate name to re-run. If omitted, all gates are re-run.
        #[arg(short, long)]
        gate: Option<String>,
    },
}

#[derive(Subcommand)]
enum WorktreeAction {
    #[command(
        about = "Show worktree status for a run",
        long_about = "Show worktree status for a run.\n\nLists all worktree bindings associated with the run, including their state,\nbranch, path, and submodule mode.\n\nExamples:\n  yarli worktree status 019577a2-...\n  yarli worktree status <run-id>"
    )]
    Status {
        /// Run ID to show worktree status for (UUID).
        run_id: String,
    },
    #[command(
        about = "Recover from an interrupted git operation in a worktree",
        long_about = "Recover from an interrupted git operation in a worktree.\n\nWhen a git operation (merge, rebase, cherry-pick) is interrupted (e.g. by a\ncrash or signal), this command lets you resolve the worktree state.\n\nRecovery actions:\n  abort         — abort the in-progress operation and reset to pre-operation state\n  resume        — attempt to continue the interrupted operation\n  manual-block  — mark the worktree as manually blocked for operator intervention\n\nThis is a policy-gated write operation and is audited.\n\nExamples:\n  yarli worktree recover 019577a4-... --action abort\n  yarli worktree recover <worktree-id> -a resume\n  yarli worktree recover <worktree-id> --action manual-block"
    )]
    Recover {
        /// Worktree ID to recover (UUID).
        worktree_id: String,
        /// Recovery action to take.
        #[arg(short, long, default_value = "abort")]
        action: String,
    },
}

#[derive(Subcommand)]
enum MergeAction {
    #[command(
        about = "Request a new merge intent",
        long_about = "Request a new merge intent.\n\nCreates a merge intent record that must be approved before execution.\nThe intent tracks the source and target refs, merge strategy, and\nassociated run.\n\nMerge strategies:\n  merge-no-ff    — create a merge commit even if fast-forward is possible (default)\n  rebase-then-ff — rebase source onto target, then fast-forward\n  squash-merge   — squash all source commits into a single merge commit\n\nThis is a policy-gated write operation and is audited.\n\nExamples:\n  yarli merge request feature/foo main --run-id 019577a2-...\n  yarli merge request feature/bar develop --run-id <id> --strategy rebase-then-ff\n  yarli merge request hotfix/fix main --run-id <id> --strategy squash-merge"
    )]
    Request {
        /// Source branch or ref to merge from.
        source: String,
        /// Target branch or ref to merge into.
        target: String,
        /// Run ID this merge belongs to (UUID).
        #[arg(long)]
        run_id: String,
        /// Merge strategy to use.
        #[arg(long, default_value = "merge-no-ff")]
        strategy: String,
    },
    #[command(
        about = "Approve a pending merge intent",
        long_about = "Approve a pending merge intent.\n\nTransitions the merge intent from Pending to Approved, allowing the merge\norchestrator to execute it. This is a policy-gated write operation and is audited.\n\nExamples:\n  yarli merge approve 019577a5-...\n  yarli merge approve <merge-id>"
    )]
    Approve {
        /// Merge intent ID to approve (UUID).
        merge_id: String,
    },
    #[command(
        about = "Reject a pending merge intent",
        long_about = "Reject a pending merge intent.\n\nTransitions the merge intent from Pending to Rejected with a reason.\nThis is a policy-gated write operation and is audited.\n\nExamples:\n  yarli merge reject 019577a5-... --reason \"conflicts with release branch\"\n  yarli merge reject <merge-id> -r \"not ready\""
    )]
    Reject {
        /// Merge intent ID to reject (UUID).
        merge_id: String,
        /// Reason for rejection (recorded in audit log).
        #[arg(short, long, default_value = "rejected")]
        reason: String,
    },
    #[command(
        about = "Show status of a merge intent",
        long_about = "Show status of a merge intent.\n\nDisplays the merge intent's state, source/target refs, strategy, and\nassociated run.\n\nExamples:\n  yarli merge status 019577a5-...\n  yarli merge status <merge-id>"
    )]
    Status {
        /// Merge intent ID to query (UUID).
        merge_id: String,
    },
}

#[derive(Subcommand)]
enum AuditAction {
    #[command(
        about = "Tail the JSONL audit log",
        long_about = "Tail the JSONL audit log.\n\nReads the most recent entries from the audit log file and prints them.\nOptionally filter by category.\n\nCategories:\n  policy_decision      — policy engine allow/deny decisions\n  destructive_attempt   — blocked destructive git operations\n  token_consumed        — LLM token usage records\n  gate_evaluation       — gate pass/fail results\n\nExamples:\n  yarli audit tail\n  yarli audit tail --lines 50\n  yarli audit tail --category policy_decision\n  yarli audit tail --file /var/log/yarli-audit.jsonl -l 100 -c gate_evaluation"
    )]
    Tail {
        /// Path to the audit JSONL file.
        #[arg(short, long, default_value = ".yarl/audit.jsonl")]
        file: String,
        /// Number of most recent entries to show (0 = all).
        #[arg(short, long, default_value = "20")]
        lines: usize,
        /// Filter by category.
        #[arg(short, long)]
        category: Option<String>,
    },
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
        return cmd_init(path.clone(), *force, *print, *backend);
    }

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
        Commands::Run {
            prompt_file,
            action,
        } => match action {
            None => cmd_run_default(_render_mode, &loaded_config, prompt_file).await,
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
                cmd_run_start(plan, _render_mode, &loaded_config).await
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
                cmd_run_start(plan, _render_mode, &loaded_config).await
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
                cmd_run_continue(file, _render_mode, &loaded_config).await
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
        Commands::Init { .. } => unreachable!("init command handled before runtime config load"),
        Commands::Info => cmd_info(&term_info, _render_mode, &loaded_config),
    }
}

/// `yarli init` — create or print a richly documented default config.
fn cmd_init(path: PathBuf, force: bool, print: bool, backend: Option<InitBackend>) -> Result<()> {
    let template = init_config_template(backend);
    if print {
        print!("{template}");
        return Ok(());
    }

    if path.exists() && !force {
        bail!(
            "refusing to overwrite existing config at {} (use --force to overwrite)",
            path.display()
        );
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
    }

    fs::write(&path, template)
        .with_context(|| format!("failed to write config file {}", path.display()))?;

    println!("Initialized config at {}", path.display());
    println!("Review [core], [postgres], and [budgets] before first durable run.");
    println!("Tip: run `yarli init --help` for the full list of tunable properties.");
    Ok(())
}

/// `yarli run start` — create a run, submit tasks, drive scheduler with stream output.
#[derive(Debug, Clone)]
struct PlannedTask {
    task_key: String,
    command: String,
    command_class: CommandClass,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PlannedTranche {
    key: String,
    objective: String,
    task_keys: Vec<String>,
}

/// Fully resolved run execution plan.
#[derive(Debug, Clone)]
struct RunPlan {
    objective: String,
    tasks: Vec<PlannedTask>,
    task_catalog: Vec<PlannedTask>,
    workdir: String,
    timeout_secs: u64,
    pace: Option<String>,
    prompt_snapshot: Option<yarli_cli::prompt::PromptSnapshot>,
    run_spec: Option<yarli_cli::prompt::RunSpec>,
    tranche_plan: Vec<PlannedTranche>,
    current_tranche_index: Option<usize>,
}

#[derive(Debug, Clone)]
struct RunExecutionOutcome {
    run_id: Uuid,
    run_state: RunState,
    continuation_payload: yarli_core::entities::ContinuationPayload,
}

#[derive(Debug, Clone)]
struct PlanGuardContext {
    target: String,
    mode: prompt::RunSpecPlanGuardMode,
    was_complete: bool,
    plan_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImplementationPlanEntry {
    key: String,
    summary: String,
    is_complete: bool,
}

#[derive(Debug, Clone)]
struct CliInvocationConfig {
    command: String,
    args: Vec<String>,
    prompt_mode: PromptMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AutoAdvanceConfig {
    policy: AutoAdvancePolicy,
    max_tranches: u32,
}

impl AutoAdvanceConfig {
    fn from_loaded(loaded_config: &LoadedConfig) -> Self {
        Self {
            policy: loaded_config.config().run.effective_auto_advance_policy(),
            max_tranches: loaded_config.config().run.max_auto_advance_tranches,
        }
    }

    fn max_reached(self, advances_taken: usize) -> bool {
        self.max_tranches != 0 && advances_taken >= self.max_tranches as usize
    }
}

fn resolve_run_plan(
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

fn default_cli_command_for_backend(backend: &str) -> Option<String> {
    match backend.trim().to_ascii_lowercase().as_str() {
        "codex" => Some("codex".to_string()),
        "claude" => Some("claude".to_string()),
        "gemini" => Some("gemini".to_string()),
        _ => None,
    }
}

fn resolve_cli_invocation_config(loaded_config: &LoadedConfig) -> Result<CliInvocationConfig> {
    let cli = &loaded_config.config().cli;
    let command = cli
        .command
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            cli.backend
                .as_deref()
                .and_then(default_cli_command_for_backend)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "yarli run requires [cli].command (or a known [cli].backend) in {}",
                loaded_config.path().display()
            )
        })?;

    Ok(CliInvocationConfig {
        command,
        args: cli.args.clone(),
        prompt_mode: cli.prompt_mode,
    })
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        "''".to_string()
    } else {
        format!("'{}'", value.replace('\'', r#"'"'"'"#))
    }
}

fn build_cli_command(invocation: &CliInvocationConfig, prompt_text: &str) -> String {
    let mut base = shell_quote(&invocation.command);
    if !invocation.args.is_empty() {
        let joined = invocation
            .args
            .iter()
            .map(|arg| shell_quote(arg))
            .collect::<Vec<_>>()
            .join(" ");
        base.push(' ');
        base.push_str(&joined);
    }

    match invocation.prompt_mode {
        PromptMode::Arg => {
            let prompt = shell_quote(prompt_text);
            format!("{base} {prompt}")
        }
        PromptMode::Stdin => {
            let prompt = shell_quote(prompt_text);
            format!("printf %s {prompt} | {base}")
        }
    }
}

fn sanitize_task_key_component(raw: &str) -> String {
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

fn build_tranche_task_prompt(
    loaded_prompt: &prompt::LoadedPrompt,
    plan_path: &Path,
    objective: &str,
    tranche: &ImplementationPlanEntry,
    index: usize,
    total: usize,
) -> String {
    format!(
        "YARLI tranche task {}/{}.\nObjective: {}\nPrompt file: {}\nPlan file: {}\nTarget tranche: {}.\nTarget summary: {}\nMode: implementation.\n\nInstructions:\n1. Read PROMPT and plan context from the workspace paths above.\n2. Implement only the target tranche if it is still incomplete.\n3. Update IMPLEMENTATION_PLAN.md and evidence in-repo.\n4. Run the tranche's required verification commands before finishing.\n5. Keep output concise and concrete.",
        index + 1,
        total,
        objective,
        loaded_prompt.entry_path.display(),
        plan_path.display(),
        tranche.key,
        tranche.summary
    )
}

fn build_verification_task_prompt(
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

fn build_plan_driven_run_sequence(
    loaded_config: &LoadedConfig,
    loaded_prompt: &prompt::LoadedPrompt,
    objective: &str,
) -> Result<(Vec<PlannedTask>, Vec<PlannedTranche>)> {
    let plan_path = plan_path_for_prompt_entry(&loaded_prompt.entry_path)?;
    let plan_text = fs::read_to_string(&plan_path)
        .with_context(|| format!("failed to read plan file {}", plan_path.display()))?;
    let open_tranches: Vec<ImplementationPlanEntry> = discover_plan_entries(&plan_text)
        .into_iter()
        .filter(|entry| !entry.is_complete)
        .collect();

    let cli_invocation = resolve_cli_invocation_config(loaded_config)?;
    let mut task_catalog = Vec::new();
    let mut tranche_plan = Vec::new();

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
        );
        let command = build_cli_command(&cli_invocation, &prompt_text);
        task_catalog.push(PlannedTask {
            task_key: task_key.clone(),
            command,
            command_class: CommandClass::Tool,
        });
        tranche_plan.push(PlannedTranche {
            key: tranche.key.clone(),
            objective: format!("{objective} [{}]", tranche.key),
            task_keys: vec![task_key],
        });
    }

    let verification_key = format!("verify-{:03}", open_tranches.len() + 1);
    let verification_prompt =
        build_verification_task_prompt(loaded_prompt, &plan_path, objective, open_tranches.len());
    let verification_command = build_cli_command(&cli_invocation, &verification_prompt);
    task_catalog.push(PlannedTask {
        task_key: verification_key.clone(),
        command: verification_command,
        command_class: CommandClass::Tool,
    });
    tranche_plan.push(PlannedTranche {
        key: "verification".to_string(),
        objective: format!("{objective} [verification]"),
        task_keys: vec![verification_key],
    });

    Ok((task_catalog, tranche_plan))
}

fn parse_command_class(label: &str) -> Result<CommandClass> {
    match label {
        "io" => Ok(CommandClass::Io),
        "cpu" => Ok(CommandClass::Cpu),
        "git" => Ok(CommandClass::Git),
        "tool" => Ok(CommandClass::Tool),
        other => bail!("unknown command class {other:?}"),
    }
}

fn build_task_catalog_from_run_spec(
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
            })
        })
        .collect::<Result<Vec<_>>>()
}

fn build_tranche_plan_from_run_spec(
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
            })
            .collect()
    } else {
        vec![PlannedTranche {
            key: "default".to_string(),
            objective: default_objective.to_string(),
            task_keys,
        }]
    };

    if tranches.is_empty() {
        bail!("run spec must resolve to at least one tranche");
    }

    Ok(tranches)
}

fn tasks_for_tranche(
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

fn parse_task_catalog_from_snapshot(config_snapshot: &serde_json::Value) -> Vec<PlannedTask> {
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

fn parse_tranche_plan_from_snapshot(config_snapshot: &serde_json::Value) -> Vec<PlannedTranche> {
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
                    if task_keys.is_empty() {
                        return None;
                    }
                    Some(PlannedTranche {
                        key,
                        objective,
                        task_keys,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn parse_current_tranche_index_from_snapshot(config_snapshot: &serde_json::Value) -> Option<usize> {
    let value = config_snapshot
        .get("runtime")
        .and_then(|runtime| runtime.get("current_tranche_index"))?
        .as_u64()?;
    usize::try_from(value).ok()
}

async fn cmd_run_start(
    plan: RunPlan,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
) -> Result<()> {
    let outcome = execute_run_plan(plan, render_mode, loaded_config).await?;
    finalize_run_outcome(&outcome)
}

async fn execute_run_plan(
    plan: RunPlan,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
) -> Result<RunExecutionOutcome> {
    ensure_write_backend_guard(loaded_config, "run start")?;

    match loaded_config.backend_selection()? {
        BackendSelection::InMemory => {
            println!("Using backend: in-memory");
            let store = Arc::new(InMemoryEventStore::new());
            let queue = Arc::new(InMemoryTaskQueue::new());
            cmd_run_start_with_backend(plan.clone(), render_mode, loaded_config, store, queue).await
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
            cmd_run_start_with_backend(plan, render_mode, loaded_config, store, queue).await
        }
    }
}

fn finalize_run_outcome(outcome: &RunExecutionOutcome) -> Result<()> {
    match outcome.run_state {
        RunState::RunCompleted => {
            println!("Run {} completed successfully.", outcome.run_id);
            Ok(())
        }
        RunState::RunFailed => bail!("Run {} failed.", outcome.run_id),
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

fn build_plan_from_continuation_tranche(
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
        prompt_snapshot: None,
        run_spec: None,
        tranche_plan: parse_tranche_plan_from_snapshot(&tranche.config_snapshot),
        current_tranche_index,
    })
}

fn should_auto_advance_planned_tranche(
    payload: &yarli_core::entities::ContinuationPayload,
    auto_advance: AutoAdvanceConfig,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptSource {
    Cli,
    Config,
    Default,
}

impl PromptSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Config => "config",
            Self::Default => "default",
        }
    }
}

#[derive(Debug, Clone)]
struct ResolvedPromptPath {
    entry_path: PathBuf,
    source: PromptSource,
}

fn find_repo_root(start_dir: &Path) -> Option<PathBuf> {
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

fn config_dir_from_cwd(loaded_config: &LoadedConfig, cwd: &Path) -> PathBuf {
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

fn resolve_prompt_entry_path_with_cwd(
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

fn resolve_explicit_prompt_path(
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

fn resolve_prompt_entry_path(
    loaded_config: &LoadedConfig,
    prompt_file_override: Option<&Path>,
) -> Result<ResolvedPromptPath> {
    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    resolve_prompt_entry_path_with_cwd(loaded_config, prompt_file_override, &cwd)
}

fn verify_only_override_enabled() -> bool {
    std::env::var("VERIFY_ONLY")
        .map(|raw| is_truthy_env(raw.trim()))
        .unwrap_or(false)
}

fn is_truthy_env(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn parse_plan_checkbox_status(line: &str) -> Option<(bool, &str)> {
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

fn strip_markdown_list_marker(line: &str) -> Option<&str> {
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

fn parse_plan_status_keywords(text: &str) -> Option<bool> {
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

fn extract_plan_target_key(text: &str) -> Option<String> {
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

fn token_has_alpha_and_digit(token: &str) -> bool {
    let has_alpha = token.chars().any(|ch| ch.is_ascii_alphabetic());
    let has_digit = token.chars().any(|ch| ch.is_ascii_digit());
    has_alpha && has_digit
}

fn parse_plan_entry_line(line: &str) -> Option<ImplementationPlanEntry> {
    if let Some((is_complete, entry)) = parse_plan_checkbox_status(line) {
        let key = extract_plan_target_key(entry)?;
        return Some(ImplementationPlanEntry {
            key,
            summary: entry.trim().to_string(),
            is_complete,
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
    let key = extract_plan_target_key(candidate)?;
    Some(ImplementationPlanEntry {
        key,
        summary: candidate.trim().to_string(),
        is_complete,
    })
}

fn discover_plan_entries(plan_text: &str) -> Vec<ImplementationPlanEntry> {
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

fn target_matches_entry_key(entry_key: &str, target: &str) -> bool {
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

fn plan_target_completion_state(plan_text: &str, target: &str) -> Result<Option<bool>> {
    Ok(discover_plan_entries(plan_text)
        .into_iter()
        .find(|entry| target_matches_entry_key(&entry.key, target))
        .map(|entry| entry.is_complete))
}

fn plan_path_for_prompt_entry(entry_prompt_path: &Path) -> Result<PathBuf> {
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

fn run_spec_mentions_verification(run_spec: &prompt::RunSpec) -> bool {
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

fn run_spec_plan_guard_preflight_with_override(
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

fn run_spec_plan_guard_preflight(
    loaded: &prompt::LoadedPrompt,
) -> Result<Option<PlanGuardContext>> {
    run_spec_plan_guard_preflight_with_override(loaded, verify_only_override_enabled())
}

fn enforce_plan_guard_post_run(
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

fn read_continuation_payload_from_file_if_exists(
    file: &Path,
) -> Result<Option<yarli_core::entities::ContinuationPayload>> {
    let content = match fs::read_to_string(file) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read continuation file: {}", file.display()));
        }
    };

    let payload = serde_json::from_str(&content).context("failed to parse continuation file")?;
    Ok(Some(payload))
}

fn load_latest_continuation_payload_from_store(
    store: &dyn EventStore,
) -> Result<Option<yarli_core::entities::ContinuationPayload>> {
    let mut run_events = query_events(store, &EventQuery::by_entity_type(EntityType::Run))?;
    run_events.sort_by(|a, b| {
        a.occurred_at
            .cmp(&b.occurred_at)
            .then_with(|| a.event_id.cmp(&b.event_id))
    });

    for event in run_events.into_iter().rev() {
        if event.event_type != RUN_CONTINUATION_EVENT_TYPE {
            continue;
        }
        let Some(raw_payload) = event.payload.get("continuation_payload").cloned() else {
            warn!(
                event_id = %event.event_id,
                run_id = %event.entity_id,
                "ignoring continuation event missing continuation_payload"
            );
            continue;
        };
        match serde_json::from_value::<yarli_core::entities::ContinuationPayload>(raw_payload) {
            Ok(payload) => return Ok(Some(payload)),
            Err(e) => {
                warn!(
                    event_id = %event.event_id,
                    run_id = %event.entity_id,
                    error = %e,
                    "ignoring malformed continuation payload event"
                );
            }
        }
    }

    Ok(None)
}

fn try_load_continuation_payload_for_continue(
    file: &Path,
    loaded_config: &LoadedConfig,
) -> Result<Option<yarli_core::entities::ContinuationPayload>> {
    if file == Path::new(DEFAULT_CONTINUATION_FILE) {
        match with_event_store(loaded_config, load_latest_continuation_payload_from_store) {
            Ok(Some(payload)) => return Ok(Some(payload)),
            Ok(None) => {}
            Err(e) => debug!(
                error = %e,
                "failed to load continuation payload from event store; trying file artifact"
            ),
        }
    }

    read_continuation_payload_from_file_if_exists(file)
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
        if let Some(payload) = try_load_continuation_payload_for_continue(file, loaded_config)? {
            if waiting_logged {
                info!(
                    run_id = %payload.run_id,
                    "continuation payload became available while waiting"
                );
            } else if file == Path::new(DEFAULT_CONTINUATION_FILE) {
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
async fn cmd_run_continue(
    file: PathBuf,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
) -> Result<()> {
    let payload = load_continuation_payload_for_continue(&file, loaded_config).await?;

    let tranche = payload
        .next_tranche
        .ok_or_else(|| anyhow::anyhow!("nothing to continue — all tasks completed successfully"))?;

    let auto_advance = AutoAdvanceConfig::from_loaded(loaded_config);
    let mut plan = build_plan_from_continuation_tranche(&tranche, loaded_config)?;
    let mut iteration = 1usize;
    let mut advances_taken = 0usize;
    loop {
        info!(
            objective = %plan.objective,
            task_count = plan.tasks.len(),
            tranche_index = ?plan.current_tranche_index,
            iteration,
            "continuing from previous run"
        );
        let outcome = execute_run_plan(plan.clone(), render_mode, loaded_config).await?;
        if outcome.run_state != RunState::RunCompleted {
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
            return finalize_run_outcome(&outcome);
        }

        let Some(next) = outcome.continuation_payload.next_tranche.as_ref() else {
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
async fn cmd_run_default(
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
    prompt_file_override: Option<PathBuf>,
) -> Result<()> {
    let resolved_prompt =
        resolve_prompt_entry_path(loaded_config, prompt_file_override.as_deref())?;
    info!(
        prompt_entry_path = %resolved_prompt.entry_path.display(),
        prompt_source = resolved_prompt.source.as_str(),
        "resolved prompt file for yarli run"
    );
    let loaded =
        prompt::load_prompt_and_run_spec(&resolved_prompt.entry_path).with_context(|| {
            format!(
                "failed to load run spec from prompt file {}",
                resolved_prompt.entry_path.display()
            )
        })?;
    let plan_guard_context = run_spec_plan_guard_preflight(&loaded)?;

    let objective = loaded
        .run_spec
        .objective
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "yarli run".to_string());

    let run_spec = loaded.run_spec.clone();
    let auto_advance = AutoAdvanceConfig::from_loaded(loaded_config);

    let (task_catalog, tranche_plan, execution_mode): (
        Vec<PlannedTask>,
        Vec<PlannedTranche>,
        &'static str,
    ) = match build_plan_driven_run_sequence(loaded_config, &loaded, &objective) {
        Ok((tasks, tranches)) => (tasks, tranches, "config-first-plan"),
        Err(err) if !run_spec.tasks.items.is_empty() => {
            warn!(
                error = %err,
                "falling back to legacy prompt-defined task orchestration"
            );
            let tasks = build_task_catalog_from_run_spec(&run_spec)?;
            let tranches = build_tranche_plan_from_run_spec(&run_spec, &objective)?;
            (tasks, tranches, "legacy-prompt")
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
        run_spec: Some(run_spec.clone()),
        tranche_plan: tranche_plan.clone(),
        current_tranche_index: Some(0),
    };

    let mut iteration = 1usize;
    let mut advances_taken = 0usize;
    loop {
        info!(
            objective = %plan.objective,
            task_count = plan.tasks.len(),
            tranche_index = ?plan.current_tranche_index,
            iteration,
            "running prompt-defined tranche"
        );
        let outcome = execute_run_plan(plan.clone(), render_mode, loaded_config).await?;
        if outcome.run_state != RunState::RunCompleted {
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
            return finalize_run_outcome(&outcome);
        }

        let Some(next) = outcome.continuation_payload.next_tranche.as_ref() else {
            if let Some(context) = plan_guard_context.as_ref() {
                enforce_plan_guard_post_run(&loaded, context)?;
            }
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

async fn cmd_run_start_with_backend<Q, S>(
    plan: RunPlan,
    render_mode: RenderMode,
    loaded_config: &LoadedConfig,
    store: Arc<S>,
    queue: Arc<Q>,
) -> Result<RunExecutionOutcome>
where
    Q: TaskQueue + 'static,
    S: EventStore + 'static,
{
    if plan.tasks.is_empty() {
        bail!("at least one task is required");
    }

    let runner = match loaded_config.config().execution.runner {
        ExecutionRunner::Native => {
            Arc::new(SelectedCommandRunner::Native(LocalCommandRunner::new()))
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

    let mut config = SchedulerConfig {
        working_dir: plan.workdir.clone(),
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

    let mut scheduler = Scheduler::new(queue, store.clone(), runner, config);
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

    let tasks: Vec<Task> = plan
        .tasks
        .iter()
        .map(|t| {
            Task::new(
                run_id,
                t.task_key.clone(),
                &t.command,
                t.command_class,
                correlation_id,
            )
        })
        .collect();

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

    info!(run_id = %run_id, objective = %plan.objective, "run started");

    // Set up shutdown controller.
    let shutdown = ShutdownController::new();
    shutdown.install_signal_handler();
    let cancel = shutdown.token();

    // Set up event channel for renderer.
    let (tx, rx) = mpsc::unbounded_channel::<StreamEvent>();

    // Emit run ID immediately so operators can discover the active run.
    let _ = tx.send(StreamEvent::RunStarted {
        run_id,
        objective: plan.objective.clone(),
        at: chrono::Utc::now(),
    });

    // Spawn renderer task.
    let renderer_shutdown = shutdown.clone();
    let renderer_handle =
        tokio::task::spawn_blocking(move || run_renderer(rx, render_mode, renderer_shutdown));

    // Drive scheduler loop, emitting events to renderer channel.
    let continuation_payload = drive_scheduler(
        &scheduler,
        &store,
        cancel,
        tx,
        run_id,
        correlation_id,
        &task_names,
        loaded_config.config().run.effective_auto_advance_policy(),
        build_memory_observer(loaded_config, run_id, correlation_id, &plan, &task_names)?,
    )
    .await?;

    // Wait for renderer to finish.
    renderer_handle
        .await
        .context("renderer task panicked")?
        .context("renderer error")?;

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

    Ok(RunExecutionOutcome {
        run_id,
        run_state: continuation_payload.exit_state,
        continuation_payload,
    })
}

/// `yarli run sw4rm` — boot as a sw4rm orchestrator agent.
///
/// Reads `[sw4rm]` config from `yarli.toml`, creates the agent infrastructure,
/// connects to the sw4rm runtime, and enters the agent message loop.
#[cfg(feature = "sw4rm")]
async fn cmd_run_sw4rm(loaded_config: &LoadedConfig) -> Result<()> {
    use yarli_sw4rm::{
        orchestrator::{VerificationCommand, VerificationSpec},
        OrchestratorLoop, ShutdownBridge, YarliAgent,
    };

    let sw4rm_config = loaded_config.config().sw4rm.clone();
    println!("Booting sw4rm agent: {}", sw4rm_config.agent_name);

    // Build verification spec from the configured/default prompt run spec if available.
    let verification = match resolve_prompt_entry_path(loaded_config, None).and_then(|resolved| {
        prompt::load_prompt_and_run_spec(&resolved.entry_path).with_context(|| {
            format!(
                "failed to load sw4rm verification prompt {}",
                resolved.entry_path.display()
            )
        })
    }) {
        Ok(loaded) => {
            let commands: Vec<VerificationCommand> = loaded
                .run_spec
                .tasks
                .items
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
        }
        Err(_) => {
            // No usable prompt — use a minimal verification spec.
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
        }
    };

    // NOTE: Using MockRouterSender — real RouterClient transport not yet implemented.
    // The agent will boot and register but LLM dispatch will not function.
    eprintln!("WARNING: using mock router sender — real sw4rm transport not yet implemented");
    let router = std::sync::Arc::new(yarli_sw4rm::mock::MockRouterSender::new());
    let orchestrator = std::sync::Arc::new(OrchestratorLoop::new(
        router,
        sw4rm_config.clone(),
        verification,
    ));

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

async fn seed_postgres_run_state_if_needed(
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
    .execute(&mut *tx)
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
        .execute(&mut *tx)
        .await
        .with_context(|| format!("failed to seed task row {}", task.id))?;
    }

    tx.commit()
        .await
        .context("failed to commit run/task seed transaction")?;
    Ok(())
}

fn run_state_db(state: RunState) -> &'static str {
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

fn task_state_db(state: TaskState) -> &'static str {
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

fn command_class_db(class: CommandClass) -> &'static str {
    match class {
        CommandClass::Io => "io",
        CommandClass::Cpu => "cpu",
        CommandClass::Git => "git",
        CommandClass::Tool => "tool",
    }
}

fn safe_mode_db(mode: SafeMode) -> &'static str {
    match mode {
        SafeMode::Observe => "observe",
        SafeMode::Execute => "execute",
        SafeMode::Restricted => "restricted",
        SafeMode::Breakglass => "breakglass",
    }
}

fn exit_reason_db(reason: yarli_core::domain::ExitReason) -> &'static str {
    match reason {
        yarli_core::domain::ExitReason::CompletedAllGates => "completed_all_gates",
        yarli_core::domain::ExitReason::BlockedOpenTasks => "blocked_open_tasks",
        yarli_core::domain::ExitReason::BlockedGateFailure => "blocked_gate_failure",
        yarli_core::domain::ExitReason::FailedPolicyDenial => "failed_policy_denial",
        yarli_core::domain::ExitReason::FailedRuntimeError => "failed_runtime_error",
        yarli_core::domain::ExitReason::CancelledByOperator => "cancelled_by_operator",
        yarli_core::domain::ExitReason::TimedOut => "timed_out",
        yarli_core::domain::ExitReason::StalledNoProgress => "stalled_no_progress",
    }
}

fn task_blocker_db(blocker: &yarli_core::entities::task::BlockerCode) -> String {
    match blocker {
        yarli_core::entities::task::BlockerCode::DependencyPending => "dependency_pending".into(),
        yarli_core::entities::task::BlockerCode::MergeConflict => "merge_conflict".into(),
        yarli_core::entities::task::BlockerCode::PolicyDenial => "policy_denial".into(),
        yarli_core::entities::task::BlockerCode::GateFailure => "gate_failure".into(),
        yarli_core::entities::task::BlockerCode::ManualHold => "manual_hold".into(),
        yarli_core::entities::task::BlockerCode::Custom(value) => value.clone(),
    }
}

/// Sync the materialized run and task states in Postgres to match in-memory registry.
///
/// The `runs.state` and `tasks.state` columns are seeded once at INSERT and never updated,
/// leaving them stale (e.g. RUN_OPEN) while the event-sourced in-memory registry advances.
/// This function writes the current in-memory state back to Postgres for observability.
async fn sync_postgres_state<Q, S, R>(
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

    let reg = scheduler.registry().read().await;
    let run = match reg.get_run(&run_id) {
        Some(r) => r,
        None => return Ok(()),
    };

    let run_state = run_state_db(run.state);
    let exit_reason = run.exit_reason.map(exit_reason_db);
    let task_states: Vec<(Uuid, &'static str)> = reg
        .tasks_for_run(&run_id)
        .iter()
        .map(|t| (t.id, task_state_db(t.state)))
        .collect();
    drop(reg);

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .context("sync_postgres_state: failed to connect")?;

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
    .bind(run_state)
    .bind(exit_reason)
    .bind(run_id)
    .execute(&mut *tx)
    .await
    .context("sync_postgres_state: update runs")?;

    for (task_id, state) in &task_states {
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
        .execute(&mut *tx)
        .await
        .with_context(|| format!("sync_postgres_state: update task {task_id}"))?;
    }

    tx.commit().await.context("sync_postgres_state: commit")?;

    debug!(
        run_id = %run_id,
        run_state = run_state,
        tasks = task_states.len(),
        "synced postgres state"
    );
    Ok(())
}

fn build_run_config_snapshot(
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
            })).collect::<Vec<_>>(),
            "task_catalog": task_catalog.iter().map(|t| serde_json::json!({
                "task_key": &t.task_key,
                "command": &t.command,
                "command_class": format!("{:?}", t.command_class),
            })).collect::<Vec<_>>(),
            "tranche_plan": tranche_plan.iter().map(|t| serde_json::json!({
                "key": &t.key,
                "objective": &t.objective,
                "task_keys": &t.task_keys,
            })).collect::<Vec<_>>(),
            "current_tranche_index": current_tranche_index,
            "prompt": prompt_snapshot,
            "run_spec": run_spec,
        },
    }))
}

fn build_memory_observer(
    loaded_config: &LoadedConfig,
    run_id: Uuid,
    correlation_id: Uuid,
    plan: &RunPlan,
    task_names: &[(Uuid, String)],
) -> Result<Option<MemoryObserver>> {
    let memory_cfg = &loaded_config.config().memory;
    let mem = &memory_cfg.backend;
    let enabled = memory_cfg.enabled.unwrap_or(mem.enabled);
    if !enabled || !mem.enabled {
        return Ok(None);
    }

    let cwd = std::env::current_dir().context("failed to read current working directory")?;
    let prompt_root = prompt::find_prompt_upwards(cwd.clone())
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or(cwd);

    let project_id = memory_cfg
        .project_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            prompt_root
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "default".to_string());

    let project_dir = mem
        .project_dir
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .map(|raw| {
            let path = PathBuf::from(raw);
            if path.is_absolute() {
                path
            } else {
                prompt_root.join(path)
            }
        })
        .unwrap_or_else(|| prompt_root.clone());

    let adapter = MemoryCliAdapter::new(mem.command.clone(), project_dir);

    let task_keys = plan.tasks.iter().map(|t| t.task_key.clone()).collect();

    Ok(Some(MemoryObserver::new(
        project_id,
        run_id,
        correlation_id,
        plan.objective.clone(),
        adapter,
        mem.query_limit,
        mem.inject_on_run_start,
        mem.inject_on_failure,
        task_keys,
        task_names,
    )))
}

fn compute_quality_gate(
    report: Option<&DeteriorationReport>,
    auto_advance_policy: AutoAdvancePolicy,
) -> yarli_core::entities::continuation::ContinuationQualityGate {
    match report {
        Some(report) => {
            let (allow_auto_advance, reason) = match report.trend {
                DeteriorationTrend::Improving => {
                    (true, "deterioration trend improving".to_string())
                }
                DeteriorationTrend::Stable => {
                    if matches!(
                        auto_advance_policy,
                        AutoAdvancePolicy::StableOk | AutoAdvancePolicy::Always
                    ) {
                        (
                            true,
                            "deterioration trend stable (policy allows auto-advance)".to_string(),
                        )
                    } else {
                        (
                            false,
                            "deterioration trend stable (stagnation blocked)".to_string(),
                        )
                    }
                }
                DeteriorationTrend::Deteriorating => {
                    if auto_advance_policy == AutoAdvancePolicy::Always {
                        (
                            true,
                            "deterioration trend deteriorating (always policy overrides gate)"
                                .to_string(),
                        )
                    } else {
                        (false, "deterioration trend deteriorating".to_string())
                    }
                }
            };
            yarli_core::entities::continuation::ContinuationQualityGate {
                allow_auto_advance,
                reason,
                trend: Some(report.trend),
                score: Some(report.score),
            }
        }
        None => yarli_core::entities::continuation::ContinuationQualityGate {
            allow_auto_advance: auto_advance_policy == AutoAdvancePolicy::Always,
            reason: if auto_advance_policy == AutoAdvancePolicy::Always {
                "no deterioration signal emitted (always policy overrides gate)".to_string()
            } else {
                "no deterioration signal emitted".to_string()
            },
            trend: None,
            score: None,
        },
    }
}

fn persist_continuation_payload_event(
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

fn build_continuation_payload(
    run: &Run,
    tasks: &[&yarli_core::entities::Task],
    report: Option<&DeteriorationReport>,
    auto_advance_policy: AutoAdvancePolicy,
) -> yarli_core::entities::ContinuationPayload {
    let mut payload = yarli_core::entities::ContinuationPayload::build(run, tasks);
    payload.quality_gate = Some(compute_quality_gate(report, auto_advance_policy));
    payload
}

/// Drive the scheduler, emitting StreamEvents to the renderer channel.
#[allow(clippy::too_many_arguments)]
async fn drive_scheduler<Q, S, R>(
    scheduler: &Scheduler<Q, S, R>,
    store: &Arc<S>,
    cancel: CancellationToken,
    tx: mpsc::UnboundedSender<StreamEvent>,
    run_id: Uuid,
    correlation_id: Uuid,
    task_names: &[(Uuid, String)],
    auto_advance_policy: AutoAdvancePolicy,
    mut memory_observer: Option<MemoryObserver>,
) -> Result<yarli_core::entities::ContinuationPayload>
where
    Q: yarli_queue::TaskQueue,
    S: EventStore,
    R: yarli_exec::CommandRunner + Clone,
{
    let mut tick_interval = tokio::time::interval(Duration::from_millis(100));
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(5));
    let mut reclaim_interval = tokio::time::interval(Duration::from_secs(10));
    let mut tick_count: u64 = 0;
    let mut stream_cursor = IncrementalEventCursor::new(
        EventQuery::by_correlation(correlation_id),
        STREAM_EVENT_BATCH_LIMIT,
    );
    let mut deterioration_observer =
        DeteriorationObserver::new(run_id, correlation_id, OBSERVER_WINDOW_SIZE);

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

    let mut zero_progress_ticks: u64 = 0;

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
                let _new_events =
                    emit_new_stream_events(store, &tx, task_names, &mut stream_cursor)?;
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
                    build_continuation_payload(
                        run,
                        &tasks,
                        deterioration_observer.latest_report(),
                        auto_advance_policy,
                    )
                };
                let _ = tx.send(StreamEvent::RunExited {
                    payload: payload.clone(),
                });
                drop(tx);
                return Ok(payload);
            }
            _ = heartbeat_interval.tick() => {
                scheduler.heartbeat_active_leases().await;
            }
            _ = reclaim_interval.tick() => {
                scheduler.reclaim_stale_leases().await;
            }
            _ = tick_interval.tick() => {
                tick_count += 1;

                // Run a scheduler tick.
                let _result = scheduler.tick_with_cancel(cancel.child_token()).await
                    .context("scheduler tick failed")?;

                debug!(
                    tick = tick_count,
                    promoted = _result.promoted,
                    claimed = _result.claimed,
                    executed = _result.executed,
                    failed = _result.failed,
                    errors = _result.errors,
                    "drive_scheduler tick"
                );

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

                // Emit events using incremental cursor reads.
                let _new_events =
                    emit_new_stream_events(store, &tx, task_names, &mut stream_cursor)?;
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

                // Send tick for spinner animation.
                let _ = tx.send(StreamEvent::Tick);

                // Check if the run is terminal.
                let reg = scheduler.registry().read().await;
                if let Some(run) = reg.get_run(&run_id) {
                    if run.state.is_terminal() {
                        info!(state = ?run.state, ticks = tick_count, "run reached terminal state");

                        // Build and emit continuation payload before closing channel.
                        let tasks: Vec<&yarli_core::entities::Task> = run
                            .task_ids
                            .iter()
                            .filter_map(|tid| reg.get_task(tid))
                            .collect();
                        let payload = build_continuation_payload(
                            run,
                            &tasks,
                            deterioration_observer.latest_report(),
                            auto_advance_policy,
                        );
                        let _ = tx.send(StreamEvent::RunExited {
                            payload: payload.clone(),
                        });

                        drop(tx);
                        return Ok(payload);
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
    cursor: &mut IncrementalEventCursor,
) -> Result<Vec<Event>> {
    let new_events = cursor.read_new_events(store.as_ref())?;

    for event in &new_events {
        if let Some(se) = event_to_stream_event(event, task_names) {
            let _ = tx.send(se);
        }
    }

    Ok(new_events)
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
        let transition = task.transition(TaskState::TaskCancelled, reason, "cli", None)?;

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
            let transition = run.transition(RunState::RunCancelled, reason, "cli", None)?;

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
                .and_then(parse_task_state)
                .unwrap_or(TaskState::TaskOpen);
            let to = to_str
                .and_then(parse_task_state)
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
                detail: event
                    .payload
                    .get("detail")
                    .and_then(|v| v.as_str())
                    .or_else(|| event.payload.get("reason").and_then(|v| v.as_str()))
                    .map(String::from),
                at: event.occurred_at,
            })
        }
        "run.activated" | "run.verifying" | "run.completed" | "run.failed" | "run.cancelled" => {
            let from_str = event.payload.get("from").and_then(|v| v.as_str());
            let to_str = event.payload.get("to").and_then(|v| v.as_str());

            let from = from_str
                .and_then(parse_run_state)
                .unwrap_or(RunState::RunOpen);
            let to = to_str
                .and_then(parse_run_state)
                .unwrap_or(RunState::RunOpen);

            let reason = event
                .payload
                .get("detail")
                .and_then(|v| v.as_str())
                .or_else(|| event.payload.get("reason").and_then(|v| v.as_str()))
                .map(String::from);

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

fn parse_submodule_mode(s: &str) -> Option<SubmoduleMode> {
    match s {
        "locked" | "Locked" => Some(SubmoduleMode::Locked),
        "allow_fast_forward" | "AllowFastForward" => Some(SubmoduleMode::AllowFastForward),
        "allow_any" | "AllowAny" => Some(SubmoduleMode::AllowAny),
        _ => None,
    }
}

fn parse_merge_strategy_value(value: &str) -> Option<MergeStrategy> {
    match value {
        "merge-no-ff" | "merge_no_ff" | "MergeNoFf" => Some(MergeStrategy::MergeNoFf),
        "rebase-then-ff" | "rebase_then_ff" | "RebaseThenFf" => Some(MergeStrategy::RebaseThenFf),
        "squash-merge" | "squash_merge" | "SquashMerge" => Some(MergeStrategy::SquashMerge),
        _ => None,
    }
}

fn parse_recovery_action(action: &str) -> RecoveryAction {
    match action {
        "abort" => RecoveryAction::Abort,
        "resume" => RecoveryAction::Resume,
        "manual-block" => RecoveryAction::ManualBlock,
        _ => unreachable!("recovery action validated by caller"),
    }
}

fn block_on_current_runtime<F>(future: F) -> Result<F::Output>
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
fn run_renderer(
    mut rx: mpsc::UnboundedReceiver<StreamEvent>,
    render_mode: RenderMode,
    shutdown: ShutdownController,
) -> Result<()> {
    match render_mode {
        RenderMode::Stream => {
            let config = StreamConfig::default();
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
        }
        RenderMode::Dashboard => {
            let config = DashboardConfig::default();
            let mut renderer = match DashboardRenderer::new(config) {
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

const STREAM_EVENT_BATCH_LIMIT: usize = 256;
const OBSERVER_EVENT_BATCH_LIMIT: usize = 256;
const OBSERVER_WINDOW_SIZE: usize = 64;
const CONTINUATION_WAIT_POLL_INTERVAL_MS: u64 = 250;
const DEFAULT_CONTINUATION_FILE: &str = ".yarli/continuation.json";
const RUN_CONTINUATION_EVENT_TYPE: &str = "run.continuation";

#[derive(Debug, Clone)]
struct IncrementalEventCursor {
    query: EventQuery,
    after_event_id: Option<Uuid>,
    batch_limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct MemoryHint {
    memory_id: String,
    scope_id: String,
    memory_class: MemoryClass,
    relevance_score: f64,
    content_snippet: String,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct MemoryHintsReport {
    query_text: String,
    limit: u32,
    results: Vec<MemoryHint>,
}

#[derive(Debug, Clone)]
struct MemoryObserver {
    enabled: bool,
    project_id: String,
    project_scope: ScopeId,
    run_id: Uuid,
    correlation_id: Uuid,
    query_limit: u32,
    inject_on_run_start: bool,
    inject_on_failure: bool,
    adapter: MemoryCliAdapter,
    run_objective: String,
    task_keys: Vec<String>,
    task_names: BTreeMap<Uuid, String>,
    run_start_done: bool,
}

impl MemoryObserver {
    #[allow(clippy::too_many_arguments)]
    fn new(
        project_id: String,
        run_id: Uuid,
        correlation_id: Uuid,
        run_objective: String,
        adapter: MemoryCliAdapter,
        query_limit: u32,
        inject_on_run_start: bool,
        inject_on_failure: bool,
        task_keys: Vec<String>,
        task_names: &[(Uuid, String)],
    ) -> Self {
        let project_scope = ScopeId(format!("project/{project_id}"));
        Self {
            enabled: true,
            project_id,
            project_scope,
            run_id,
            correlation_id,
            query_limit,
            inject_on_run_start,
            inject_on_failure,
            adapter,
            run_objective,
            task_keys,
            task_names: task_names.iter().cloned().collect(),
            run_start_done: false,
        }
    }

    async fn observe_run_start(&mut self, store: &dyn EventStore) {
        if !self.enabled || self.run_start_done || !self.inject_on_run_start {
            self.run_start_done = true;
            return;
        }
        self.run_start_done = true;

        let query_text = format!(
            "objective: {} tasks: {}",
            self.run_objective,
            self.task_keys.join(",")
        );
        let query = MemoryQuery {
            scope_id: self.project_scope.clone(),
            query_text: query_text.clone(),
            limit: self.query_limit,
            memory_class: Some(MemoryClass::Semantic),
        };

        match self.adapter.query(&self.project_id, query).await {
            Ok(records) => {
                let report = MemoryHintsReport {
                    query_text,
                    limit: self.query_limit,
                    results: records
                        .into_iter()
                        .map(|r| MemoryHint {
                            memory_id: r.memory_id,
                            scope_id: r.scope_id.0,
                            memory_class: r.memory_class,
                            relevance_score: r.relevance_score,
                            content_snippet: truncate_for_snippet(&r.content),
                            metadata: r.metadata.into_iter().collect(),
                        })
                        .collect(),
                };

                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Run,
                        entity_id: self.run_id.to_string(),
                        event_type: "run.observer.memory_hints".to_string(),
                        payload: serde_json::to_value(&report).unwrap_or_else(|_| {
                            serde_json::json!({
                                "query_text": report.query_text,
                                "limit": report.limit,
                                "results": [],
                            })
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: None,
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
            Err(err) => {
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Run,
                        entity_id: self.run_id.to_string(),
                        event_type: "run.observer.memory_query_failed".to_string(),
                        payload: serde_json::json!({
                            "error": err.to_string(),
                            "scope_id": self.project_scope.as_str(),
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: None,
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
        }
    }

    async fn observe_events(&mut self, store: &dyn EventStore, events: &[Event]) {
        if !self.enabled {
            return;
        }

        for event in events {
            match event.event_type.as_str() {
                "task.failed" => {
                    self.on_task_failed(store, event).await;
                    if self.inject_on_failure {
                        self.emit_task_hints(store, event, "failed").await;
                    }
                }
                "task.blocked" => {
                    if self.inject_on_failure {
                        self.emit_task_hints(store, event, "blocked").await;
                    }
                }
                _ => {}
            }
        }
    }

    async fn on_task_failed(&self, store: &dyn EventStore, event: &Event) {
        let Ok(task_id) = event.entity_id.parse::<Uuid>() else {
            return;
        };
        let task_key = self
            .task_names
            .get(&task_id)
            .cloned()
            .unwrap_or_else(|| event.entity_id[..8].to_string());

        let reason = event
            .payload
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let exit_code = event.payload.get("exit_code").and_then(|v| v.as_i64());
        let detail = event
            .payload
            .get("detail")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let content = truncate_for_memory(&format!(
            "task_failed: task_key={task_key} reason={reason} exit_code={} detail={detail}",
            exit_code
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));

        let mut semantic =
            InsertMemory::new(self.project_scope.clone(), MemoryClass::Semantic, content);
        semantic
            .metadata
            .insert("run_id".to_string(), self.run_id.to_string());
        semantic
            .metadata
            .insert("task_id".to_string(), task_id.to_string());
        semantic
            .metadata
            .insert("task_key".to_string(), task_key.clone());
        semantic
            .metadata
            .insert("reason".to_string(), reason.clone());
        semantic
            .metadata
            .insert("event_id".to_string(), event.event_id.to_string());

        match self.adapter.store(&self.project_id, semantic).await {
            Ok(record) => {
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Task,
                        entity_id: task_id.to_string(),
                        event_type: "task.observer.memory_stored".to_string(),
                        payload: serde_json::json!({
                            "memory_id": record.memory_id,
                            "scope_id": record.scope_id.as_str(),
                            "memory_class": record.memory_class,
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: Some(event.event_id),
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
            Err(err) => {
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Task,
                        entity_id: task_id.to_string(),
                        event_type: "task.observer.memory_store_failed".to_string(),
                        payload: serde_json::json!({
                            "error": err.to_string(),
                            "scope_id": self.project_scope.as_str(),
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: Some(event.event_id),
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
        }
    }

    async fn emit_task_hints(&self, store: &dyn EventStore, event: &Event, state: &str) {
        let Ok(task_id) = event.entity_id.parse::<Uuid>() else {
            return;
        };

        let task_key = self
            .task_names
            .get(&task_id)
            .cloned()
            .unwrap_or_else(|| event.entity_id[..8].to_string());
        let reason = event
            .payload
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let query_text = format!("{state} task_key={task_key} reason={reason}");

        let query = MemoryQuery {
            scope_id: self.project_scope.clone(),
            query_text: query_text.clone(),
            limit: self.query_limit,
            memory_class: Some(MemoryClass::Semantic),
        };

        match self.adapter.query(&self.project_id, query).await {
            Ok(records) => {
                let report = MemoryHintsReport {
                    query_text,
                    limit: self.query_limit,
                    results: records
                        .into_iter()
                        .map(|r| MemoryHint {
                            memory_id: r.memory_id,
                            scope_id: r.scope_id.0,
                            memory_class: r.memory_class,
                            relevance_score: r.relevance_score,
                            content_snippet: truncate_for_snippet(&r.content),
                            metadata: r.metadata.into_iter().collect(),
                        })
                        .collect(),
                };
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Task,
                        entity_id: task_id.to_string(),
                        event_type: "task.observer.memory_hints".to_string(),
                        payload: serde_json::to_value(&report).unwrap_or_else(|_| {
                            serde_json::json!({
                                "query_text": report.query_text,
                                "limit": report.limit,
                                "results": [],
                            })
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: Some(event.event_id),
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
            Err(err) => {
                let _ = append_event(
                    store,
                    Event {
                        event_id: Uuid::now_v7(),
                        occurred_at: chrono::Utc::now(),
                        entity_type: EntityType::Task,
                        entity_id: task_id.to_string(),
                        event_type: "task.observer.memory_query_failed".to_string(),
                        payload: serde_json::json!({
                            "error": err.to_string(),
                            "scope_id": self.project_scope.as_str(),
                        }),
                        correlation_id: self.correlation_id,
                        causation_id: Some(event.event_id),
                        actor: "observer.memory".to_string(),
                        idempotency_key: None,
                    },
                );
            }
        }
    }
}

fn truncate_for_snippet(content: &str) -> String {
    const MAX: usize = 160;
    let trimmed = content.trim();
    if trimmed.len() <= MAX {
        return trimmed.to_string();
    }
    format!("{}...", &trimmed[..MAX])
}

fn truncate_for_memory(content: &str) -> String {
    const MAX: usize = 1024;
    let trimmed = content.trim();
    if trimmed.len() <= MAX {
        return trimmed.to_string();
    }
    format!("{}...", &trimmed[..MAX])
}

impl IncrementalEventCursor {
    fn new(query: EventQuery, batch_limit: usize) -> Self {
        Self {
            query,
            after_event_id: None,
            batch_limit,
        }
    }

    fn read_new_events(&mut self, store: &dyn EventStore) -> Result<Vec<Event>> {
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

#[derive(Debug, Clone)]
struct DeteriorationObserver {
    run_id: Uuid,
    correlation_id: Uuid,
    cursor: IncrementalEventCursor,
    state: DeteriorationObserverState,
    latest_report: Option<DeteriorationReport>,
}

impl DeteriorationObserver {
    fn new(run_id: Uuid, correlation_id: Uuid, window_size: usize) -> Self {
        Self {
            run_id,
            correlation_id,
            cursor: IncrementalEventCursor::new(
                EventQuery::by_correlation(correlation_id),
                OBSERVER_EVENT_BATCH_LIMIT,
            ),
            state: DeteriorationObserverState::new(window_size),
            latest_report: None,
        }
    }

    fn observe_store(&mut self, store: &dyn EventStore) -> Result<()> {
        let events = self.cursor.read_new_events(store)?;
        if events.is_empty() {
            return Ok(());
        }

        let has_relevant = self.state.ingest(&events);
        if !has_relevant {
            return Ok(());
        }

        let report = self.state.report();
        self.latest_report = Some(report.clone());
        append_event(
            store,
            Event {
                event_id: Uuid::now_v7(),
                occurred_at: chrono::Utc::now(),
                entity_type: EntityType::Run,
                entity_id: self.run_id.to_string(),
                event_type: "run.observer.deterioration".to_string(),
                payload: serde_json::json!({
                    "score": report.score,
                    "window_size": report.window_size,
                    "factors": report.factors,
                    "trend": report.trend,
                }),
                correlation_id: self.correlation_id,
                causation_id: events.last().map(|event| event.event_id),
                actor: "observer.deterioration".to_string(),
                idempotency_key: None,
            },
        )?;

        Ok(())
    }

    fn latest_report(&self) -> Option<&DeteriorationReport> {
        self.latest_report.as_ref()
    }
}

#[derive(Debug, Clone)]
struct RuntimeSample {
    command_key: String,
    duration_ms: f64,
}

#[derive(Debug, Clone)]
struct BudgetSample {
    headroom: f64,
}

#[derive(Debug, Clone)]
enum ObserverSignal {
    Runtime(RuntimeSample),
    Retry,
    Blocked,
    Failure { reason_bucket: String },
    Budget(BudgetSample),
}

#[derive(Debug, Clone)]
struct DeteriorationObserverState {
    window_size: usize,
    signals: VecDeque<ObserverSignal>,
    pending_commands: HashMap<String, String>,
    previous_score: Option<f64>,
}

impl DeteriorationObserverState {
    fn new(window_size: usize) -> Self {
        Self {
            window_size,
            signals: VecDeque::new(),
            pending_commands: HashMap::new(),
            previous_score: None,
        }
    }

    fn ingest(&mut self, events: &[Event]) -> bool {
        let mut changed = false;

        for event in events {
            match event.event_type.as_str() {
                "command.started" => {
                    let command_key = event
                        .payload
                        .get("command")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    self.pending_commands
                        .insert(event.entity_id.clone(), command_key);
                }
                "command.exited" | "command.timed_out" | "command.killed" => {
                    let duration_ms = event
                        .payload
                        .get("duration_ms")
                        .and_then(|v| v.as_f64())
                        .or_else(|| {
                            event
                                .payload
                                .get("duration_ms")
                                .and_then(|v| v.as_i64())
                                .map(|v| v as f64)
                        })
                        .unwrap_or(0.0)
                        .max(0.0);
                    let command_key = self
                        .pending_commands
                        .remove(&event.entity_id)
                        .unwrap_or_else(|| "unknown".to_string());
                    self.push_signal(ObserverSignal::Runtime(RuntimeSample {
                        command_key,
                        duration_ms,
                    }));
                    changed = true;
                }
                "task.retrying" => {
                    self.push_signal(ObserverSignal::Retry);
                    changed = true;
                }
                "task.blocked" => {
                    self.push_signal(ObserverSignal::Blocked);
                    changed = true;
                }
                "task.failed" => {
                    let bucket = event
                        .payload
                        .get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    self.push_signal(ObserverSignal::Failure {
                        reason_bucket: bucket,
                    });
                    changed = true;

                    if let (Some(observed), Some(limit)) = (
                        event.payload.get("observed").and_then(|v| v.as_f64()),
                        event.payload.get("limit").and_then(|v| v.as_f64()),
                    ) {
                        if limit > 0.0 {
                            let headroom = (1.0 - (observed / limit)).clamp(0.0, 1.0);
                            self.push_signal(ObserverSignal::Budget(BudgetSample { headroom }));
                        }
                    }
                }
                _ => {}
            }
        }

        changed
    }

    fn push_signal(&mut self, signal: ObserverSignal) {
        self.signals.push_back(signal);
        while self.signals.len() > self.window_size {
            self.signals.pop_front();
        }
    }

    fn report(&mut self) -> DeteriorationReport {
        let runtime_drift = self.runtime_drift_score();
        let retry_inflation = self.retry_inflation_score();
        let blocker_churn = self.blocker_churn_score();
        let failure_drift = self.failure_drift_score();
        let budget_erosion = self.budget_erosion_score();

        let mut factors = vec![
            DeteriorationFactor {
                name: "runtime_drift".to_string(),
                impact: runtime_drift,
                detail: "runtime trend for repeated command keys".to_string(),
            },
            DeteriorationFactor {
                name: "retry_inflation".to_string(),
                impact: retry_inflation,
                detail: "retry events per rolling window".to_string(),
            },
            DeteriorationFactor {
                name: "blocker_churn".to_string(),
                impact: blocker_churn,
                detail: "task.blocked churn in rolling window".to_string(),
            },
            DeteriorationFactor {
                name: "failure_rate_drift".to_string(),
                impact: failure_drift,
                detail: "failure bucket rate drift".to_string(),
            },
            DeteriorationFactor {
                name: "budget_headroom_erosion".to_string(),
                impact: budget_erosion,
                detail: "budget headroom trend".to_string(),
            },
        ];

        factors.sort_by(|a, b| b.impact.total_cmp(&a.impact));
        factors.truncate(3);

        let score = (runtime_drift * 30.0
            + retry_inflation * 20.0
            + blocker_churn * 15.0
            + failure_drift * 25.0
            + budget_erosion * 10.0)
            .clamp(0.0, 100.0);

        let trend = match self.previous_score {
            Some(prev) if score - prev > 5.0 => DeteriorationTrend::Deteriorating,
            Some(prev) if prev - score > 5.0 => DeteriorationTrend::Improving,
            Some(_) => DeteriorationTrend::Stable,
            None => DeteriorationTrend::Stable,
        };
        self.previous_score = Some(score);

        DeteriorationReport {
            score,
            window_size: self.signals.len(),
            factors,
            trend,
        }
    }

    fn runtime_drift_score(&self) -> f64 {
        let mut per_key: HashMap<&str, Vec<f64>> = HashMap::new();
        for signal in &self.signals {
            if let ObserverSignal::Runtime(sample) = signal {
                per_key
                    .entry(sample.command_key.as_str())
                    .or_default()
                    .push(sample.duration_ms);
            }
        }

        let mut drifts = Vec::new();
        for durations in per_key.values() {
            if durations.len() < 2 {
                continue;
            }
            let split = durations.len() / 2;
            if split == 0 {
                continue;
            }
            let first_avg = durations[..split].iter().sum::<f64>() / split as f64;
            let second_avg =
                durations[split..].iter().sum::<f64>() / (durations.len() - split) as f64;
            let drift = ((second_avg - first_avg) / first_avg.max(1.0)).max(0.0);
            drifts.push(drift.min(1.0));
        }

        if drifts.is_empty() {
            0.0
        } else {
            drifts.iter().sum::<f64>() / drifts.len() as f64
        }
    }

    fn retry_inflation_score(&self) -> f64 {
        let retries = self
            .signals
            .iter()
            .filter(|signal| matches!(signal, ObserverSignal::Retry))
            .count() as f64;
        (retries / self.window_size.max(1) as f64).clamp(0.0, 1.0)
    }

    fn blocker_churn_score(&self) -> f64 {
        let blocked = self
            .signals
            .iter()
            .filter(|signal| matches!(signal, ObserverSignal::Blocked))
            .count() as f64;
        (blocked / self.window_size.max(1) as f64).clamp(0.0, 1.0)
    }

    fn failure_drift_score(&self) -> f64 {
        let failures: Vec<&str> = self
            .signals
            .iter()
            .filter_map(|signal| {
                if let ObserverSignal::Failure { reason_bucket } = signal {
                    Some(reason_bucket.as_str())
                } else {
                    None
                }
            })
            .collect();
        if failures.len() < 2 {
            return 0.0;
        }

        let split = failures.len() / 2;
        if split == 0 {
            return 0.0;
        }

        let mut first_counts: HashMap<&str, usize> = HashMap::new();
        let mut second_counts: HashMap<&str, usize> = HashMap::new();
        for bucket in &failures[..split] {
            *first_counts.entry(*bucket).or_default() += 1;
        }
        for bucket in &failures[split..] {
            *second_counts.entry(*bucket).or_default() += 1;
        }

        let first_total = split as f64;
        let second_total = (failures.len() - split) as f64;
        let mut max_increase: f64 = 0.0;

        for bucket in second_counts.keys() {
            let first_rate = *first_counts.get(bucket).unwrap_or(&0) as f64 / first_total.max(1.0);
            let second_rate =
                *second_counts.get(bucket).unwrap_or(&0) as f64 / second_total.max(1.0);
            max_increase = max_increase.max((second_rate - first_rate).max(0.0));
        }

        max_increase.clamp(0.0, 1.0)
    }

    fn budget_erosion_score(&self) -> f64 {
        let headrooms: Vec<f64> = self
            .signals
            .iter()
            .filter_map(|signal| {
                if let ObserverSignal::Budget(sample) = signal {
                    Some(sample.headroom)
                } else {
                    None
                }
            })
            .collect();
        if headrooms.len() < 2 {
            return 0.0;
        }

        let split = headrooms.len() / 2;
        if split == 0 {
            return 0.0;
        }

        let first_avg = headrooms[..split].iter().sum::<f64>() / split as f64;
        let second_avg = headrooms[split..].iter().sum::<f64>() / (headrooms.len() - split) as f64;
        (first_avg - second_avg).clamp(0.0, 1.0)
    }
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
    resource_usage: Option<CommandResourceUsage>,
    token_usage: Option<TokenUsage>,
    budget_breach_reason: Option<String>,
    memory_hints: Option<MemoryHintsReport>,
    last_error: Option<String>,
    blocker_detail: Option<String>,
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
    deterioration: Option<DeteriorationReport>,
    memory_hints: Option<MemoryHintsReport>,
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
    task_id: Option<Uuid>,
    repo_root: Option<PathBuf>,
    worktree_path: Option<PathBuf>,
    branch_name: Option<String>,
    base_ref: Option<String>,
    head_ref: Option<String>,
    submodule_mode: Option<SubmoduleMode>,
}

#[derive(Debug, Clone)]
struct MergeProjection {
    merge_id: Uuid,
    state: MergeState,
    run_id: Option<Uuid>,
    worktree_id: Option<Uuid>,
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

fn ensure_write_backend_guard(loaded_config: &LoadedConfig, command_name: &str) -> Result<()> {
    if matches!(
        loaded_config.backend_selection()?,
        BackendSelection::InMemory
    ) && !loaded_config.config().core.allow_in_memory_writes
    {
        bail!(
            "`{command_name}` refuses in-memory write mode. Configure durable storage with [core] backend = \"postgres\" and [postgres] database_url, or explicitly opt in with [core] allow_in_memory_writes = true."
        );
    }
    Ok(())
}

fn load_runtime_config_for_writes(command_name: &str) -> Result<LoadedConfig> {
    let loaded_config = load_runtime_config_for_reads()?;
    ensure_write_backend_guard(&loaded_config, command_name)?;
    Ok(loaded_config)
}

fn prepare_audit_sink(loaded_config: &LoadedConfig) -> Result<Option<JsonlAuditSink>> {
    if !loaded_config.config().policy.audit_decisions {
        return Ok(None);
    }

    let audit_path = PathBuf::from(&loaded_config.config().observability.audit_file);
    if let Some(parent) = audit_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create audit directory {}", parent.display())
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
        .or(match event.event_type.as_str() {
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
        .or(match event.event_type.as_str() {
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
    let gate_name = raw_gate
        .trim()
        .strip_prefix("gate.")
        .unwrap_or(raw_gate.trim());
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

fn deterioration_from_event(event: &Event) -> Option<DeteriorationReport> {
    if event.event_type != "run.observer.deterioration" {
        return None;
    }
    serde_json::from_value(event.payload.clone()).ok()
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
            resource_usage: None,
            token_usage: None,
            budget_breach_reason: None,
            memory_hints: None,
            last_error: None,
            blocker_detail: None,
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

        // Extract resource and token usage from command/task events.
        if let Some(ru) = event
            .payload
            .get("resource_usage")
            .or_else(|| event.payload.get("command_resource_usage"))
        {
            if let Ok(usage) = serde_json::from_value::<CommandResourceUsage>(ru.clone()) {
                entry.resource_usage = Some(usage);
            }
        }
        if let Some(tu) = event
            .payload
            .get("token_usage")
            .or_else(|| event.payload.get("command_token_usage"))
        {
            if let Ok(usage) = serde_json::from_value::<TokenUsage>(tu.clone()) {
                entry.token_usage = Some(usage);
            }
        }

        // Detect budget breach reason.
        if event.payload.get("reason").and_then(|v| v.as_str()) == Some("budget_exceeded") {
            let detail = event
                .payload
                .get("detail")
                .and_then(|v| v.as_str())
                .unwrap_or("budget_exceeded");
            entry.budget_breach_reason = Some(detail.to_string());
        }

        if event.event_type == "task.observer.memory_hints" {
            if let Ok(report) = serde_json::from_value::<MemoryHintsReport>(event.payload.clone()) {
                entry.memory_hints = Some(report);
            }
        }

        // Extract last_error from failure events (preserve first occurrence).
        if event.event_type == "task.failed" || event.event_type == "task.gate_failed" {
            if entry.last_error.is_none() {
                let error_msg = event
                    .payload
                    .get("detail")
                    .and_then(|v| v.as_str())
                    .or_else(|| event.payload.get("reason").and_then(|v| v.as_str()));
                if let Some(msg) = error_msg {
                    entry.last_error = Some(msg.to_string());
                }
            }
        }

        // Extract blocker_detail from annotate events.
        if event.event_type == "task.annotated" {
            entry.blocker_detail = event
                .payload
                .get("blocker_detail")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
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
    let task_events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Task, task_id.to_string()),
    )?;
    if task_events.is_empty() {
        return Ok(None);
    }

    let projections = collect_task_projections(&task_events);
    Ok(projections.into_iter().next())
}

fn load_run_projection(store: &dyn EventStore, run_id: Uuid) -> Result<Option<RunProjection>> {
    let run_events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Run, run_id.to_string()),
    )?;
    if run_events.is_empty() {
        return Ok(None);
    }

    let mut state = RunState::RunOpen;
    let mut objective = None;
    let mut correlation_id = run_events[0].correlation_id;
    let mut updated_at = run_events[0].occurred_at;
    let mut last_event_type = run_events[0].event_type.clone();
    let mut failed_gates = Vec::new();
    let mut deterioration = None;
    let mut memory_hints = None;

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

        if let Some(report) = deterioration_from_event(event) {
            deterioration = Some(report);
        }

        if event.event_type == "run.observer.memory_hints" {
            if let Ok(report) = serde_json::from_value::<MemoryHintsReport>(event.payload.clone()) {
                memory_hints = Some(report);
            }
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
        deterioration,
        memory_hints,
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
    let mut task_id = None;
    let mut repo_root = None;
    let mut worktree_path = None;
    let mut branch_name = None;
    let mut base_ref = None;
    let mut head_ref = None;
    let mut submodule_mode = None;

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

        if let Some(value) = event
            .payload
            .get("task_id")
            .and_then(|value| value.as_str())
            .and_then(|raw| raw.parse::<Uuid>().ok())
        {
            task_id = Some(value);
        }

        if let Some(value) = event
            .payload
            .get("repo_root")
            .and_then(|value| value.as_str())
        {
            repo_root = Some(PathBuf::from(value));
        }

        if let Some(value) = event
            .payload
            .get("worktree_path")
            .and_then(|value| value.as_str())
        {
            worktree_path = Some(PathBuf::from(value));
        }

        if let Some(value) = event
            .payload
            .get("branch_name")
            .and_then(|value| value.as_str())
        {
            branch_name = Some(value.to_string());
        }

        if let Some(value) = event
            .payload
            .get("base_ref")
            .and_then(|value| value.as_str())
        {
            base_ref = Some(value.to_string());
        }

        if let Some(value) = event
            .payload
            .get("head_ref")
            .and_then(|value| value.as_str())
        {
            head_ref = Some(value.to_string());
        }

        if let Some(value) = event
            .payload
            .get("submodule_mode")
            .and_then(|value| value.as_str())
            .and_then(parse_submodule_mode)
        {
            submodule_mode = Some(value);
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
        task_id,
        repo_root,
        worktree_path,
        branch_name,
        base_ref,
        head_ref,
        submodule_mode,
    }))
}

fn load_merge_projection(
    store: &dyn EventStore,
    merge_id: Uuid,
) -> Result<Option<MergeProjection>> {
    let events = query_events(
        store,
        &EventQuery::by_entity(EntityType::Merge, merge_id.to_string()),
    )?;
    if events.is_empty() {
        return Ok(None);
    }

    let mut state = MergeState::MergeRequested;
    let mut run_id = None;
    let mut worktree_id = None;
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

        if let Some(value) = event
            .payload
            .get("worktree_id")
            .and_then(|value| value.as_str())
            .and_then(|raw| raw.parse::<Uuid>().ok())
        {
            worktree_id = Some(value);
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
        worktree_id,
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

fn load_latest_worktree_projection_for_run(
    store: &dyn EventStore,
    run_id: Uuid,
    correlation_id: Uuid,
) -> Result<Option<WorktreeProjection>> {
    let events = query_events(store, &EventQuery::by_correlation(correlation_id))?;
    let mut worktree_ids: Vec<Uuid> = events
        .iter()
        .filter(|event| event.entity_type == EntityType::Worktree)
        .filter_map(|event| event.entity_id.parse::<Uuid>().ok())
        .collect();
    worktree_ids.sort();
    worktree_ids.dedup();

    let mut projections = Vec::new();
    for worktree_id in worktree_ids {
        if let Some(projection) = load_worktree_projection(store, worktree_id)? {
            if projection.run_id == Some(run_id) {
                projections.push(projection);
            }
        }
    }

    projections.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.worktree_id.cmp(&b.worktree_id))
    });

    Ok(projections.pop())
}

fn build_worktree_binding(projection: &WorktreeProjection) -> Result<WorktreeBinding> {
    let run_id = projection.run_id.ok_or_else(|| {
        anyhow::anyhow!("worktree {} missing run_id context", projection.worktree_id)
    })?;
    let repo_root = projection
        .repo_root
        .as_ref()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "worktree {} missing repo_root context",
                projection.worktree_id
            )
        })?
        .clone();
    let branch_name = projection
        .branch_name
        .as_ref()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "worktree {} missing branch_name context",
                projection.worktree_id
            )
        })?
        .clone();
    let worktree_path = projection
        .worktree_path
        .as_ref()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "worktree {} missing worktree_path context",
                projection.worktree_id
            )
        })?
        .clone();
    let base_ref = projection
        .base_ref
        .clone()
        .or_else(|| projection.head_ref.clone())
        .unwrap_or_else(|| "HEAD".to_string());

    let mut binding = WorktreeBinding::new(
        run_id,
        repo_root,
        branch_name,
        base_ref,
        projection.correlation_id,
    );
    binding.id = projection.worktree_id;
    binding.state = projection.state;
    binding.updated_at = projection.updated_at;
    binding.set_worktree_path(worktree_path);
    if let Some(task_id) = projection.task_id {
        binding = binding.with_task(task_id);
    }
    if let Some(head_ref) = projection.head_ref.as_ref() {
        binding.head_ref = head_ref.clone();
    }
    if let Some(mode) = projection.submodule_mode {
        binding = binding.with_submodule_mode(mode);
    }
    Ok(binding)
}

fn build_merge_intent(merge: &MergeProjection, worktree_id: Uuid) -> Result<MergeIntent> {
    let run_id = merge
        .run_id
        .ok_or_else(|| anyhow::anyhow!("merge intent {} missing run_id context", merge.merge_id))?;
    let source_ref = merge
        .source_ref
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("merge intent {} missing source ref", merge.merge_id))?
        .clone();
    let target_ref = merge
        .target_ref
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("merge intent {} missing target ref", merge.merge_id))?
        .clone();

    let mut intent = MergeIntent::new(
        run_id,
        worktree_id,
        source_ref,
        target_ref,
        merge.correlation_id,
    );
    intent.id = merge.merge_id;
    intent.state = merge.state;
    intent.updated_at = merge.updated_at;
    if let Some(strategy_raw) = merge.strategy.as_deref() {
        let strategy = parse_merge_strategy_value(strategy_raw).ok_or_else(|| {
            anyhow::anyhow!(
                "merge intent {} has unsupported strategy {strategy_raw}",
                merge.merge_id
            )
        })?;
        intent.strategy = strategy;
    }
    Ok(intent)
}

fn resolve_merge_worktree_projection(
    store: &dyn EventStore,
    merge: &MergeProjection,
) -> Result<WorktreeProjection> {
    if let Some(worktree_id) = merge.worktree_id {
        return load_worktree_projection(store, worktree_id)?.ok_or_else(|| {
            anyhow::anyhow!(
                "worktree {} referenced by merge intent {} not found",
                worktree_id,
                merge.merge_id
            )
        });
    }

    let run_id = merge
        .run_id
        .ok_or_else(|| anyhow::anyhow!("merge intent {} missing run_id context", merge.merge_id))?;
    load_latest_worktree_projection_for_run(store, run_id, merge.correlation_id)?.ok_or_else(|| {
        anyhow::anyhow!(
            "no worktree context found for merge intent {}",
            merge.merge_id
        )
    })
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
    if let Some(report) = run.deterioration.as_ref() {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Deterioration: score={:.1} trend={:?} window_size={}",
            report.score, report.trend, report.window_size
        )?;
        for factor in &report.factors {
            writeln!(
                &mut out,
                "  factor {} impact={:.2} ({})",
                factor.name, factor.impact, factor.detail
            )?;
        }
    }

    if let Some(hints) = run.memory_hints.as_ref() {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Memories: {} hint(s) (query={:?})",
            hints.results.len(),
            hints.query_text
        )?;
        for hint in hints.results.iter().take(3) {
            writeln!(
                &mut out,
                "  {} score={:.2} {}",
                hint.memory_id, hint.relevance_score, hint.content_snippet
            )?;
        }
    }

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
            if let Some(ref last_error) = task.last_error {
                writeln!(&mut out, "    last_error: {last_error}")?;
            }
            if let Some(ref detail) = task.blocker_detail {
                writeln!(&mut out, "    blocker_detail: {detail}")?;
            }
            if let Some(ref breach) = task.budget_breach_reason {
                writeln!(&mut out, "    budget_exceeded: {breach}")?;
            }
            if let Some(ref tu) = task.token_usage {
                writeln!(
                    &mut out,
                    "    token_usage: prompt_tokens={} completion_tokens={} total_tokens={}",
                    tu.prompt_tokens, tu.completion_tokens, tu.total_tokens
                )?;
            }
            if let Some(ref ru) = task.resource_usage {
                let rss = ru
                    .max_rss_bytes
                    .map(|v| format!("{v}"))
                    .unwrap_or_else(|| "-".into());
                writeln!(&mut out, "    resource_usage: max_rss_bytes={rss}")?;
            }
            if let Some(ref hints) = task.memory_hints {
                writeln!(
                    &mut out,
                    "    memory_hints: {} (query={:?})",
                    hints.results.len(),
                    hints.query_text
                )?;
            }
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
                    .map(|(gate_type, reason)| {
                        (
                            *gate_type,
                            GateResult::Failed {
                                reason: reason.clone(),
                            },
                        )
                    })
                    .collect(),
                last_transition_at: Some(task.updated_at),
                resource_usage: task.resource_usage.clone(),
                token_usage: task.token_usage.clone(),
                budget_breach_reason: task.budget_breach_reason.clone(),
            })
            .collect(),
        gates: run
            .failed_gates
            .iter()
            .map(|(gate_type, reason)| {
                (
                    *gate_type,
                    GateResult::Failed {
                        reason: reason.clone(),
                    },
                )
            })
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

    if !explain.budget_breaches.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Budget breaches:")?;
        for breach in &explain.budget_breaches {
            writeln!(&mut out, "  {} - {}", breach.task_name, breach.reason)?;
        }
    }

    if !explain.suggested_actions.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Suggested actions:")?;
        for action in &explain.suggested_actions {
            writeln!(&mut out, "  - {}", action.description)?;
        }
    }

    if let Some(report) = run.deterioration.as_ref() {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Sequence deterioration: score={:.1} trend={:?} window_size={}",
            report.score, report.trend, report.window_size
        )?;
        for factor in &report.factors {
            writeln!(
                &mut out,
                "  {} impact={:.2} ({})",
                factor.name, factor.impact, factor.detail
            )?;
        }
    }

    if let Some(hints) = run.memory_hints.as_ref() {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Memories: {} hint(s) (query={:?})",
            hints.results.len(),
            hints.query_text
        )?;
        for hint in hints.results.iter().take(5) {
            writeln!(
                &mut out,
                "  {} score={:.2} {}",
                hint.memory_id, hint.relevance_score, hint.content_snippet
            )?;
        }
    }

    let hinted_tasks: Vec<_> = run
        .tasks
        .iter()
        .filter(|task| task.memory_hints.is_some())
        .collect();
    if !hinted_tasks.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Task memory hints:")?;
        for task in hinted_tasks {
            let hints = task.memory_hints.as_ref().unwrap();
            writeln!(
                &mut out,
                "  {} {:?}: {} hint(s) (query={:?})",
                task.task_id,
                task.state,
                hints.results.len(),
                hints.query_text
            )?;
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
            .map(|(gate_type, reason)| {
                (
                    *gate_type,
                    GateResult::Failed {
                        reason: reason.clone(),
                    },
                )
            })
            .collect(),
        last_transition_at: Some(task.updated_at),
        resource_usage: task.resource_usage.clone(),
        token_usage: task.token_usage.clone(),
        budget_breach_reason: task.budget_breach_reason.clone(),
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

    if let Some(ref last_error) = task.last_error {
        writeln!(&mut out, "Last error: {last_error}")?;
    }

    if let Some(ref detail) = task.blocker_detail {
        writeln!(&mut out, "Blocker detail: {detail}")?;
    }

    if !explain.budget_breaches.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Budget breaches:")?;
        for breach in &explain.budget_breaches {
            writeln!(&mut out, "  budget_exceeded: {}", breach.reason)?;
        }
    }

    if let Some(ref tu) = task.token_usage {
        writeln!(&mut out)?;
        writeln!(
            &mut out,
            "Token usage: prompt_tokens={} completion_tokens={} total_tokens={}",
            tu.prompt_tokens, tu.completion_tokens, tu.total_tokens
        )?;
    }

    if let Some(ref ru) = task.resource_usage {
        let rss = ru
            .max_rss_bytes
            .map(|v| format!("{v}"))
            .unwrap_or_else(|| "-".into());
        writeln!(&mut out, "Resource usage: max_rss_bytes={rss}")?;
    }

    if !explain.failed_gates.is_empty() {
        writeln!(&mut out)?;
        writeln!(&mut out, "Failed gates:")?;
        for failure in &explain.failed_gates {
            writeln!(
                &mut out,
                "  {} - {}",
                failure.gate_type.label(),
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

#[allow(clippy::too_many_arguments)]
fn append_worktree_transition_event(
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

fn gate_result_status(result: &GateResult) -> &'static str {
    match result {
        GateResult::Passed { .. } => "passed",
        GateResult::Failed { .. } => "failed",
        GateResult::Pending => "pending",
    }
}

fn parse_command_class_value(value: &serde_json::Value) -> Option<CommandClass> {
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

fn render_gate_rerun_output(
    task_id: Uuid,
    run_id: Uuid,
    selected_count: usize,
    evaluations: &[yarli_gates::GateEvaluation],
) -> Result<String> {
    let mut output = String::new();
    writeln!(&mut output, "Gate re-run for task {task_id}")?;
    writeln!(&mut output, "Run: {run_id}")?;
    writeln!(&mut output, "Evaluated {} gate(s):", selected_count)?;

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
                idempotency_key: Some(gate_rerun_idempotency_key(
                    task_id,
                    evaluation.gate_type.label(),
                )),
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

fn gate_rerun_idempotency_key(task_id: Uuid, gate_label: &str) -> String {
    format!("{task_id}:gate_rerun:{gate_label}")
}

fn worktree_recovery_idempotency_key(worktree_id: Uuid, action: &str, event_type: &str) -> String {
    format!("{worktree_id}:worktree_recover:{action}:{event_type}")
}

fn merge_request_idempotency_key(
    run_id: Uuid,
    source: &str,
    target: &str,
    strategy: &str,
) -> String {
    format!("{run_id}:merge_request:{source}:{target}:{strategy}")
}

fn merge_operation_idempotency_key(merge_id: Uuid, operation: &str, event_type: &str) -> String {
    format!("{merge_id}:{operation}:{event_type}")
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
            idempotency_key: Some(merge_operation_idempotency_key(
                merge.merge_id,
                operation,
                "policy.decision",
            )),
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

#[allow(clippy::too_many_arguments)]
fn append_merge_execution_event(
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
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| {
        let run_id = resolve_run_id_input(store, run_id_str)?;
        render_run_status(store, run_id)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli run list` — list all known runs.
fn cmd_run_list() -> Result<()> {
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, render_run_list)?;
    println!("{output}");
    Ok(())
}

/// Render a table of all runs discovered in the event store.
fn render_run_list(store: &dyn EventStore) -> Result<String> {
    let run_events = query_events(store, &EventQuery::by_entity_type(EntityType::Run))?;

    if run_events.is_empty() {
        return Ok("No runs found in event store.".to_string());
    }

    // Group events by run entity_id to discover unique runs.
    let mut runs: BTreeMap<String, (RunState, Option<String>, chrono::DateTime<chrono::Utc>, u32)> =
        BTreeMap::new();
    for event in &run_events {
        let entry = runs.entry(event.entity_id.clone()).or_insert((
            RunState::RunOpen,
            None,
            event.occurred_at,
            0,
        ));
        entry.2 = event.occurred_at; // last update
        entry.3 += 1; // event count

        if let Some(next_state) = run_state_from_event(event) {
            entry.0 = next_state;
        }
        if event.event_type == "run.config_snapshot" {
            entry.1 = event
                .payload
                .get("objective")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
    }

    // Count tasks per run.
    let task_events = query_events(store, &EventQuery::by_entity_type(EntityType::Task))?;
    let mut task_counts: HashMap<String, (u32, u32, u32)> = HashMap::new(); // total, complete, failed
                                                                            // Map task_id -> run_id via correlation_id.
    let mut corr_to_run: HashMap<Uuid, String> = HashMap::new();
    for event in &run_events {
        corr_to_run
            .entry(event.correlation_id)
            .or_insert_with(|| event.entity_id.clone());
    }
    // Track task states per run.
    let mut task_states: HashMap<String, HashMap<String, TaskState>> = HashMap::new();
    for event in &task_events {
        if let Some(run_id) = corr_to_run.get(&event.correlation_id) {
            let tasks = task_states.entry(run_id.clone()).or_default();
            if let Some(state) = task_state_from_event(event) {
                tasks.insert(event.entity_id.clone(), state);
            } else if !tasks.contains_key(&event.entity_id) {
                tasks.insert(event.entity_id.clone(), TaskState::TaskOpen);
            }
        }
    }
    for (run_id, tasks) in &task_states {
        let total = tasks.len() as u32;
        let complete = tasks
            .values()
            .filter(|s| **s == TaskState::TaskComplete)
            .count() as u32;
        let failed = tasks
            .values()
            .filter(|s| **s == TaskState::TaskFailed)
            .count() as u32;
        task_counts.insert(run_id.clone(), (total, complete, failed));
    }

    let run_id_prefixes = unique_run_id_prefixes(runs.keys().cloned().collect::<Vec<_>>(), 10);

    let mut out = String::new();
    out.push_str(&format!(
        "{:<14} {:<14} {:<30} {:<16} {}\n",
        "RUN ID", "STATE", "OBJECTIVE", "TASKS (C/F/T)", "UPDATED"
    ));
    out.push_str(&"-".repeat(85));
    out.push('\n');

    for (run_id_str, (state, objective, updated_at, _event_count)) in &runs {
        let short_id = run_id_prefixes
            .get(run_id_str)
            .cloned()
            .unwrap_or_else(|| compact_run_id(run_id_str));
        let obj = objective.as_deref().unwrap_or("-");
        let obj_display = if obj.len() > 28 {
            format!("{}…", &obj[..27])
        } else {
            obj.to_string()
        };
        let (total, complete, failed) = task_counts
            .get(run_id_str.as_str())
            .copied()
            .unwrap_or((0, 0, 0));
        let tasks_str = format!("{complete}/{failed}/{total}");
        let time_str = updated_at.format("%Y-%m-%d %H:%M");

        out.push_str(&format!(
            "{:<14} {:<14} {:<30} {:<16} {}\n",
            short_id,
            format!("{:?}", state),
            obj_display,
            tasks_str,
            time_str
        ));
    }

    Ok(out.trim_end().to_string())
}

fn compact_run_id(run_id: &str) -> String {
    if let Ok(parsed) = Uuid::parse_str(run_id) {
        parsed.simple().to_string()
    } else {
        run_id.chars().filter(|c| *c != '-').collect()
    }
}

fn resolve_run_id_input(store: &dyn EventStore, run_id_input: &str) -> Result<Uuid> {
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

fn unique_run_id_prefixes(run_ids: Vec<String>, min_len: usize) -> HashMap<String, String> {
    let compact: Vec<(String, String)> = run_ids
        .into_iter()
        .map(|run_id| {
            let compact = compact_run_id(&run_id);
            (run_id, compact)
        })
        .collect();
    let mut prefixes = HashMap::new();

    for (run_id, compact_id) in &compact {
        let mut chosen = compact_id.clone();
        let start = min_len.min(compact_id.len()).max(1);
        for len in start..=compact_id.len() {
            let prefix = &compact_id[..len];
            let is_unique = compact
                .iter()
                .filter(|(other_id, _)| other_id != run_id)
                .all(|(_, other_compact)| !other_compact.starts_with(prefix));
            if is_unique {
                chosen = prefix.to_string();
                break;
            }
        }
        prefixes.insert(run_id.clone(), chosen);
    }

    prefixes
}

/// `yarli run explain-exit` — run the Why Not Done? engine.
fn cmd_run_explain(run_id_str: &str) -> Result<()> {
    let loaded_config = load_runtime_config_for_reads()?;
    let output = with_event_store(&loaded_config, |store| {
        let run_id = resolve_run_id_input(store, run_id_str)?;
        render_run_explain(store, run_id)
    })?;
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
fn cmd_info(
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
fn cmd_task_unblock(task_id_str: &str, reason: &str) -> Result<()> {
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
fn cmd_task_annotate(task_id_str: &str, detail: &str) -> Result<()> {
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
fn execute_task_annotate(store: &dyn EventStore, task_id: Uuid, detail: &str) -> Result<String> {
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

    let loaded_config = load_runtime_config_for_writes("gate rerun")?;
    let output = with_event_store(&loaded_config, |store| {
        execute_gate_rerun(store, task_id, gate_name)
    })?;
    println!("{output}");
    Ok(())
}

/// `yarli worktree status` — show worktree state.
fn cmd_worktree_status(run_id_str: &str) -> Result<()> {
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
fn cmd_worktree_recover(worktree_id_str: &str, action: &str) -> Result<()> {
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
fn cmd_merge_request(source: &str, target: &str, run_id_str: &str, strategy: &str) -> Result<()> {
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
fn cmd_merge_approve(merge_id_str: &str) -> Result<()> {
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
fn cmd_merge_reject(merge_id_str: &str, reason: &str) -> Result<()> {
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
    use clap::CommandFactory;
    use std::path::Path;
    use std::process::Command;
    use tempfile::{NamedTempFile, TempDir};
    use yarli_observability::{AuditCategory, AuditEntry, InMemoryAuditSink};
    use yarli_store::event_store::EventQuery;
    use yarli_store::InMemoryEventStore;

    const VALID_UUID: &str = "00000000-0000-0000-0000-000000000000";

    fn write_test_config(contents: &str) -> LoadedConfig {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(&path, contents).unwrap();
        LoadedConfig::load(path).unwrap()
    }

    fn write_test_config_at(path: &Path, contents: &str) -> LoadedConfig {
        std::fs::write(path, contents).unwrap();
        LoadedConfig::load(path).unwrap()
    }

    #[test]
    fn cmd_init_writes_documented_config_template() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");

        cmd_init(path.clone(), false, false, None).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("[core]"));
        assert!(raw.contains("backend = \"in-memory\""));
        assert!(raw.contains("[budgets]"));
        assert!(raw.contains("max_task_total_tokens"));
        assert!(raw.contains("[ui]"));
        assert!(raw.contains("mode = \"auto\""));
    }

    #[test]
    fn cmd_init_refuses_overwrite_without_force() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(&path, "existing = true\n").unwrap();

        let err = cmd_init(path.clone(), false, false, None).unwrap_err();
        assert!(err.to_string().contains("use --force"));

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("existing = true"));
    }

    #[test]
    fn cmd_init_force_overwrites_existing_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(&path, "existing = true\n").unwrap();

        cmd_init(path.clone(), true, false, None).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("existing = true"));
        assert!(raw.contains("[execution]"));
    }

    #[test]
    fn init_help_lists_all_tunable_properties() {
        let mut cmd = Cli::command();
        let init = cmd
            .find_subcommand_mut("init")
            .expect("init subcommand should exist");
        let mut help = Vec::new();
        init.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        for key in [
            "core.backend",
            "core.allow_in_memory_writes",
            "core.safe_mode",
            "core.worker_id",
            "postgres.database_url",
            "cli.backend",
            "cli.prompt_mode",
            "cli.command",
            "cli.args",
            "event_loop.max_iterations",
            "event_loop.max_runtime_seconds",
            "event_loop.idle_timeout_secs",
            "event_loop.checkpoint_interval",
            "features.parallel",
            "queue.claim_batch_size",
            "queue.lease_ttl_seconds",
            "queue.heartbeat_interval_seconds",
            "queue.reclaim_interval_seconds",
            "queue.reclaim_grace_seconds",
            "queue.per_run_cap",
            "queue.io_cap",
            "queue.cpu_cap",
            "queue.git_cap",
            "queue.tool_cap",
            "execution.working_dir",
            "execution.command_timeout_seconds",
            "execution.tick_interval_ms",
            "execution.runner",
            "execution.overwatch.service_url",
            "execution.overwatch.profile",
            "execution.overwatch.soft_timeout_seconds",
            "execution.overwatch.silent_timeout_seconds",
            "execution.overwatch.max_log_bytes",
            "run.prompt_file",
            "run.continue_wait_timeout_seconds",
            "run.allow_stable_auto_advance",
            "run.auto_advance_policy",
            "run.max_auto_advance_tranches",
            "run.default_pace",
            "run.paces.<name>.cmds",
            "run.paces.<name>.working_dir",
            "run.paces.<name>.command_timeout_seconds",
            "budgets.max_task_rss_bytes",
            "budgets.max_task_cpu_user_ticks",
            "budgets.max_task_cpu_system_ticks",
            "budgets.max_task_io_read_bytes",
            "budgets.max_task_io_write_bytes",
            "budgets.max_task_total_tokens",
            "budgets.max_run_total_tokens",
            "budgets.max_run_peak_rss_bytes",
            "budgets.max_run_cpu_user_ticks",
            "budgets.max_run_cpu_system_ticks",
            "budgets.max_run_io_read_bytes",
            "budgets.max_run_io_write_bytes",
            "git.default_target_branch",
            "git.destructive_default_deny",
            "policy.enforce_policies",
            "policy.audit_decisions",
            "memory.backend.enabled",
            "memory.backend.endpoint",
            "memory.backend.command",
            "memory.backend.project_dir",
            "memory.backend.query_limit",
            "memory.backend.inject_on_run_start",
            "memory.backend.inject_on_failure",
            "memory.enabled",
            "memory.project_id",
            "observability.audit_file",
            "observability.log_level",
            "ui.mode",
        ] {
            assert!(
                help.contains(key),
                "init --help should mention config property {key}"
            );
        }
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
        assert!(
            help.contains("--prompt-file")
                && help.contains("[run].prompt_file")
                && help.contains("PROMPT.md")
                && help.contains("yarli run")
                && help.contains("no subcommand"),
            "run --help should mention prompt resolution precedence"
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

    fn make_command_started(
        command_id: Uuid,
        correlation_id: Uuid,
        command: &str,
        command_class: &str,
    ) -> Event {
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_type: EntityType::Command,
            entity_id: command_id.to_string(),
            event_type: "command.started".to_string(),
            payload: serde_json::json!({
                "command": command,
                "command_class": command_class,
                "working_dir": "/tmp",
            }),
            correlation_id,
            causation_id: None,
            actor: "test".to_string(),
            idempotency_key: None,
        }
    }

    fn make_command_terminal(
        command_id: Uuid,
        correlation_id: Uuid,
        event_type: &str,
        duration_ms: u64,
        exit_code: Option<i32>,
    ) -> Event {
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_type: EntityType::Command,
            entity_id: command_id.to_string(),
            event_type: event_type.to_string(),
            payload: serde_json::json!({
                "duration_ms": duration_ms,
                "exit_code": exit_code,
            }),
            correlation_id,
            causation_id: None,
            actor: "test".to_string(),
            idempotency_key: None,
        }
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
    fn deterioration_scoring_distinguishes_stable_vs_deteriorating_trails() {
        let corr = Uuid::now_v7();
        let mut stable = DeteriorationObserverState::new(64);
        let mut stable_events = Vec::new();
        for _ in 0..8 {
            let command_id = Uuid::now_v7();
            stable_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            stable_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                120,
                Some(0),
            ));
        }
        assert!(stable.ingest(&stable_events));
        let stable_report = stable.report();
        assert!(
            stable_report.score < 25.0,
            "stable score={}",
            stable_report.score
        );

        let mut degrading = DeteriorationObserverState::new(64);
        let mut baseline_events = Vec::new();
        for _ in 0..6 {
            let command_id = Uuid::now_v7();
            baseline_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            baseline_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                100,
                Some(0),
            ));
        }
        degrading.ingest(&baseline_events);
        let baseline_report = degrading.report();

        let mut degrade_events = Vec::new();
        for i in 0..6 {
            let command_id = Uuid::now_v7();
            degrade_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            degrade_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                400 + (i * 250),
                Some(1),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.retrying",
                corr,
                serde_json::json!({"attempt_no": 2}),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.blocked",
                corr,
                serde_json::json!({"reason": "policy_denial"}),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "reason": if i % 2 == 0 { "budget_exceeded" } else { "nonzero_exit" },
                    "observed": 120.0 + (i as f64 * 10.0),
                    "limit": 100.0,
                }),
            ));
        }
        assert!(degrading.ingest(&degrade_events));
        let degrading_report = degrading.report();

        assert!(degrading_report.score > baseline_report.score);
        assert_eq!(degrading_report.trend, DeteriorationTrend::Deteriorating);
    }

    #[test]
    fn deterioration_observer_emits_incremental_events() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let command_id = Uuid::now_v7();

        store
            .append(make_command_started(command_id, corr, "cargo test", "io"))
            .unwrap();
        store
            .append(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                100,
                Some(0),
            ))
            .unwrap();

        let mut observer = DeteriorationObserver::new(run_id, corr, 32);
        observer.observe_store(&store).unwrap();
        observer.observe_store(&store).unwrap();

        let observer_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert_eq!(
            observer_events
                .iter()
                .filter(|event| event.event_type == "run.observer.deterioration")
                .count(),
            1
        );

        store
            .append(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.retrying",
                corr,
                serde_json::json!({"attempt_no": 2}),
            ))
            .unwrap();
        observer.observe_store(&store).unwrap();
        let observer_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert_eq!(
            observer_events
                .iter()
                .filter(|event| event.event_type == "run.observer.deterioration")
                .count(),
            2
        );
    }

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

    // ── UUID validation ──────────────────────────────────────────────

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
    fn render_run_list_empty_store() {
        let store = InMemoryEventStore::new();
        let output = render_run_list(&store).unwrap();
        assert!(output.contains("No runs found"));
    }

    #[test]
    fn render_run_list_shows_active_run() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
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

        let output = render_run_list(&store).unwrap();
        let map = unique_run_id_prefixes(vec![run_id.to_string()], 10);
        let prefix = map.get(&run_id.to_string()).unwrap();
        assert!(output.contains(prefix.as_str()));
        assert!(output.contains("RunActive"));
        assert!(output.contains("ship it"));
    }

    #[test]
    fn render_run_list_counts_tasks() {
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
                "task.completed",
                corr,
                serde_json::json!({ "from": "TaskExecuting", "to": "TaskComplete" }),
            ))
            .unwrap();

        let output = render_run_list(&store).unwrap();
        // 1 complete, 0 failed, 1 total => "1/0/1"
        assert!(output.contains("1/0/1"));
    }

    #[test]
    fn resolve_run_id_input_accepts_unique_prefix() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::parse_str("019c5056-d8a7-7133-9ad0-77652b8be1e8").unwrap();
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

        let resolved = resolve_run_id_input(&store, "019c5056d8a7").unwrap();
        assert_eq!(resolved, run_id);
    }

    #[test]
    fn resolve_run_id_input_rejects_ambiguous_prefix() {
        let store = InMemoryEventStore::new();
        let a = Uuid::parse_str("019c4f51-aaaa-7000-8000-000000000001").unwrap();
        let b = Uuid::parse_str("019c4f51-bbbb-7000-8000-000000000002").unwrap();
        let corr_a = Uuid::now_v7();
        let corr_b = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                a.to_string(),
                "run.activated",
                corr_a,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                b.to_string(),
                "run.activated",
                corr_b,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let err = resolve_run_id_input(&store, "019c4f51").unwrap_err();
        assert!(err.to_string().contains("ambiguous run ID prefix"));
    }

    #[test]
    fn resolve_run_id_input_rejects_unknown_prefix() {
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

        let err = resolve_run_id_input(&store, "deadbeef").unwrap_err();
        assert!(err.to_string().contains("unique run-list prefix"));
    }

    #[test]
    fn unique_run_id_prefixes_expand_on_collision() {
        let a = "019c4f51-aaaa-7000-8000-000000000001".to_string();
        let b = "019c4f51-bbbb-7000-8000-000000000002".to_string();
        let map = unique_run_id_prefixes(vec![a.clone(), b.clone()], 10);
        let pa = map.get(&a).unwrap();
        let pb = map.get(&b).unwrap();
        assert_ne!(pa, pb);
        assert!(compact_run_id(&a).starts_with(pa));
        assert!(compact_run_id(&b).starts_with(pb));
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

    // ── worktree recover validation ──────────────────────────────────

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

    // ── merge lifecycle commands ─────────────────────────────────────

    fn merge_id_from_output(output: &str) -> Uuid {
        output
            .lines()
            .find_map(|line| line.strip_prefix("Merge ID: "))
            .and_then(|raw| raw.trim().parse::<Uuid>().ok())
            .expect("expected merge ID line in output")
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
    fn ensure_write_backend_guard_blocks_in_memory_writes_by_default() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("yarli.toml");
        let loaded_config = LoadedConfig::load(config_path).unwrap();

        let result = ensure_write_backend_guard(&loaded_config, "task unblock");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn ensure_write_backend_guard_allows_explicit_ephemeral_override() {
        let loaded_config = write_test_config(
            r#"
[core]
backend = "in-memory"
allow_in_memory_writes = true
"#,
        );

        let result = ensure_write_backend_guard(&loaded_config, "task unblock");
        assert!(result.is_ok());
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

    #[test]
    fn render_run_status_surfaces_budget_exceeded_and_token_usage() {
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
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    },
                    "command_resource_usage": {
                        "max_rss_bytes": 1048576,
                        "cpu_user_ticks": 100,
                        "cpu_system_ticks": 50,
                        "io_read_bytes": 4096,
                        "io_write_bytes": 2048
                    }
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("budget_exceeded"),
            "status output must surface budget_exceeded: {output}"
        );
        assert!(
            output.contains("prompt_tokens=2500"),
            "status output must surface prompt_tokens: {output}"
        );
        assert!(
            output.contains("completion_tokens=2500"),
            "status output must surface completion_tokens: {output}"
        );
        assert!(
            output.contains("total_tokens=5000"),
            "status output must surface total_tokens: {output}"
        );
        assert!(
            output.contains("max_rss_bytes=1048576"),
            "status output must surface resource_usage: {output}"
        );
    }

    #[test]
    fn render_run_explain_surfaces_budget_breach() {
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
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    }
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(
            output.contains("Budget breaches:"),
            "explain output must show budget breaches section: {output}"
        );
        assert!(
            output.contains("budget_exceeded") || output.contains("max_task_total_tokens"),
            "explain output must reference budget breach detail: {output}"
        );
    }

    #[test]
    fn render_run_status_surfaces_deterioration_report() {
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
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.deterioration",
                corr,
                serde_json::json!({
                    "score": 72.5,
                    "window_size": 32,
                    "factors": [
                        {
                            "name": "runtime_drift",
                            "impact": 0.8,
                            "detail": "runtime trend for repeated command keys"
                        }
                    ],
                    "trend": "deteriorating"
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("Deterioration: score=72.5"));
        assert!(output.contains("runtime_drift"));
        assert!(output.contains("Deteriorating"));
    }

    #[test]
    fn render_run_status_surfaces_memory_hints() {
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
                EntityType::Run,
                run_id.to_string(),
                "run.observer.memory_hints",
                corr,
                serde_json::json!({
                    "query_text": "objective: verify",
                    "limit": 8,
                    "results": [
                        {
                            "memory_id": "m1",
                            "scope_id": "project/test",
                            "memory_class": "semantic",
                            "relevance_score": 0.9,
                            "content_snippet": "remember to run migrations",
                            "metadata": { "fingerprint": "abc" }
                        }
                    ]
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.observer.memory_hints",
                corr,
                serde_json::json!({
                    "query_text": "failed reason=budget_exceeded",
                    "limit": 8,
                    "results": [
                        {
                            "memory_id": "m2",
                            "scope_id": "project/test",
                            "memory_class": "semantic",
                            "relevance_score": 0.2,
                            "content_snippet": "previous budget exceeded fix: lower parallelism",
                            "metadata": {}
                        }
                    ]
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("Memories: 1 hint(s)"));
        assert!(output.contains("remember to run migrations"));
        assert!(output.contains("memory_hints: 1"));
    }

    #[test]
    fn render_run_explain_surfaces_deterioration_report() {
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
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.deterioration",
                corr,
                serde_json::json!({
                    "score": 58.0,
                    "window_size": 20,
                    "factors": [
                        {
                            "name": "failure_rate_drift",
                            "impact": 0.6,
                            "detail": "failure bucket rate drift"
                        }
                    ],
                    "trend": "deteriorating"
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(output.contains("Sequence deterioration: score=58.0"));
        assert!(output.contains("failure_rate_drift"));
        assert!(output.contains("Deteriorating"));
    }

    #[test]
    fn render_task_explain_surfaces_budget_breach_and_token_usage() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    },
                    "command_resource_usage": {
                        "max_rss_bytes": 1048576
                    }
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(
            output.contains("budget_exceeded"),
            "task explain must surface budget_exceeded: {output}"
        );
        assert!(
            output.contains("prompt_tokens=2500"),
            "task explain must surface prompt_tokens: {output}"
        );
        assert!(
            output.contains("completion_tokens=2500"),
            "task explain must surface completion_tokens: {output}"
        );
        assert!(
            output.contains("total_tokens=5000"),
            "task explain must surface total_tokens: {output}"
        );
        assert!(
            output.contains("max_rss_bytes=1048576"),
            "task explain must surface resource_usage: {output}"
        );
    }

    #[test]
    fn task_annotate_persists_and_displays_in_explain() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        // Create a task event first.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.started",
                corr,
                serde_json::json!({"from": "TaskOpen", "to": "TaskReady"}),
            ))
            .unwrap();

        // Annotate the task.
        let result = execute_task_annotate(&store, task_id, "see blocker-001.md").unwrap();
        assert!(
            result.contains("blocker-001.md"),
            "annotate result must contain detail: {result}"
        );

        // Verify explain output includes the annotation.
        let explain_output = render_task_explain(&store, task_id).unwrap();
        assert!(
            explain_output.contains("blocker-001.md"),
            "explain must show blocker detail: {explain_output}"
        );
    }

    #[test]
    fn task_annotate_nonexistent_task_returns_not_found() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let result = execute_task_annotate(&store, task_id, "detail").unwrap();
        assert!(
            result.contains("not found"),
            "should report not found: {result}"
        );
    }

    #[test]
    fn run_status_displays_blocker_detail() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        // Create run event.
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.started",
                corr,
                serde_json::json!({"from": "RunOpen", "to": "RunActive"}),
            ))
            .unwrap();

        // Create task event linked by correlation.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.started",
                corr,
                serde_json::json!({"from": "TaskOpen", "to": "TaskReady"}),
            ))
            .unwrap();

        // Annotate task.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.annotated",
                corr,
                serde_json::json!({"blocker_detail": "see blocker-002.md"}),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("blocker-002.md"),
            "run status must show blocker detail: {output}"
        );
    }

    #[test]
    fn task_explain_displays_last_error() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "nonzero_exit",
                    "detail": "command exited with code 1"
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(
            output.contains("Last error: command exited with code 1"),
            "explain must show last_error: {output}"
        );
    }

    #[test]
    fn run_status_displays_last_error() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.started",
                corr,
                serde_json::json!({"from": "RunOpen", "to": "RunActive"}),
            ))
            .unwrap();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "nonzero_exit",
                    "detail": "command exited with code 42"
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("last_error: command exited with code 42"),
            "run status must show last_error: {output}"
        );
    }

    // ── Continuation payload tests ──

    fn sample_continuation_payload(
        run_id: Uuid,
        objective: &str,
    ) -> yarli_core::entities::ContinuationPayload {
        use yarli_core::entities::continuation::{ContinuationPayload, RunSummary};

        ContinuationPayload {
            run_id,
            objective: objective.to_string(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
            completed_at: Utc::now(),
            tasks: Vec::new(),
            summary: RunSummary {
                total: 0,
                completed: 0,
                failed: 0,
                cancelled: 0,
                pending: 0,
            },
            next_tranche: None,
            quality_gate: None,
        }
    }

    #[test]
    fn read_continuation_payload_from_file_if_exists_returns_none_for_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("missing-continuation.json");
        let payload = read_continuation_payload_from_file_if_exists(&file_path).unwrap();
        assert!(payload.is_none());
    }

    #[test]
    fn read_continuation_payload_from_file_if_exists_reads_payload() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("continuation.json");
        let payload = sample_continuation_payload(Uuid::new_v4(), "from-file");
        let json = serde_json::to_string_pretty(&payload).unwrap();
        std::fs::write(&file_path, json).unwrap();

        let loaded = read_continuation_payload_from_file_if_exists(&file_path)
            .unwrap()
            .expect("expected payload");
        assert_eq!(loaded.run_id, payload.run_id);
        assert_eq!(loaded.objective, payload.objective);
    }

    #[test]
    fn load_latest_continuation_payload_prefers_most_recent_event() {
        let store = InMemoryEventStore::new();
        let corr = Uuid::new_v4();
        let older = sample_continuation_payload(Uuid::new_v4(), "older");
        let newer = sample_continuation_payload(Uuid::new_v4(), "newer");

        store
            .append(make_event(
                EntityType::Run,
                older.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": older,
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                newer.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": newer.clone(),
                }),
            ))
            .unwrap();

        let loaded = load_latest_continuation_payload_from_store(&store)
            .unwrap()
            .expect("expected continuation payload");
        assert_eq!(loaded.run_id, newer.run_id);
        assert_eq!(loaded.objective, "newer");
    }

    #[test]
    fn load_latest_continuation_payload_skips_malformed_latest_event() {
        let store = InMemoryEventStore::new();
        let corr = Uuid::new_v4();
        let valid = sample_continuation_payload(Uuid::new_v4(), "valid");

        store
            .append(make_event(
                EntityType::Run,
                valid.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": valid.clone(),
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                Uuid::new_v4().to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": {
                        "run_id": "not-a-uuid"
                    },
                }),
            ))
            .unwrap();

        let loaded = load_latest_continuation_payload_from_store(&store)
            .unwrap()
            .expect("expected fallback continuation payload");
        assert_eq!(loaded.run_id, valid.run_id);
        assert_eq!(loaded.objective, "valid");
    }

    #[test]
    fn continuation_payload_round_trips_through_file() {
        use yarli_core::entities::continuation::{ContinuationPayload, RunSummary, TrancheSpec};

        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "build everything".into(),
            exit_state: RunState::RunFailed,
            exit_reason: Some(yarli_core::domain::ExitReason::FailedRuntimeError),
            completed_at: Utc::now(),
            tasks: vec![
                yarli_core::entities::continuation::TaskOutcome {
                    task_id: Uuid::new_v4(),
                    task_key: "build".into(),
                    state: TaskState::TaskComplete,
                    attempt_no: 1,
                    last_error: None,
                    blocker: None,
                },
                yarli_core::entities::continuation::TaskOutcome {
                    task_id: Uuid::new_v4(),
                    task_key: "test".into(),
                    state: TaskState::TaskFailed,
                    attempt_no: 2,
                    last_error: Some("3 tests failed".into()),
                    blocker: None,
                },
            ],
            summary: RunSummary {
                total: 2,
                completed: 1,
                failed: 1,
                cancelled: 0,
                pending: 0,
            },
            next_tranche: Some(TrancheSpec {
                suggested_objective: "Retry failed tasks: test".into(),
                kind: yarli_core::entities::continuation::TrancheKind::RetryUnfinished,
                retry_task_keys: vec!["test".into()],
                unfinished_task_keys: vec![],
                planned_task_keys: vec![],
                planned_tranche_key: None,
                cursor: None,
                config_snapshot: serde_json::json!({"tasks": [{"task_key": "test", "command": "cargo test"}]}),
            }),
            quality_gate: None,
        };

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("continuation.json");
        let json = serde_json::to_string_pretty(&payload).unwrap();
        std::fs::write(&file_path, &json).unwrap();

        let read_back: ContinuationPayload =
            serde_json::from_str(&std::fs::read_to_string(&file_path).unwrap()).unwrap();

        assert_eq!(read_back.run_id, payload.run_id);
        assert_eq!(read_back.summary.failed, 1);
        assert_eq!(read_back.summary.completed, 1);
        let tranche = read_back.next_tranche.unwrap();
        assert_eq!(tranche.retry_task_keys, vec!["test"]);
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
        let tranche = TrancheSpec {
            suggested_objective: "planned-next".into(),
            kind: TrancheKind::PlannedNext,
            retry_task_keys: vec![],
            unfinished_task_keys: vec![],
            planned_task_keys: vec!["two_task".into()],
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
                        {"task_key": "two_task", "command": "true", "command_class": "Io"}
                    ],
                    "tranche_plan": [
                        {"key": "one", "objective": "first", "task_keys": ["one_task"]},
                        {"key": "two", "objective": "second", "task_keys": ["two_task"]}
                    ],
                    "current_tranche_index": 0
                }
            }),
        };

        let plan = build_plan_from_continuation_tranche(&tranche, &loaded).unwrap();
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].task_key, "two_task");
        assert_eq!(plan.tasks[0].command, "true");
        assert_eq!(plan.current_tranche_index, Some(1));
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
            AutoAdvanceConfig {
                policy: AutoAdvancePolicy::ImprovingOnly,
                max_tranches: 0,
            },
            0,
        );
        assert!(allow);
    }

    #[test]
    fn auto_advance_blocks_stable_quality_gate() {
        use yarli_core::entities::continuation::{
            ContinuationPayload, ContinuationQualityGate, RunSummary, TrancheKind, TrancheSpec,
        };

        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
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
                allow_auto_advance: false,
                reason: "stable".into(),
                trend: Some(DeteriorationTrend::Stable),
                score: Some(30.0),
            }),
        };

        let (allow, reason) = should_auto_advance_planned_tranche(
            &payload,
            AutoAdvanceConfig {
                policy: AutoAdvancePolicy::ImprovingOnly,
                max_tranches: 0,
            },
            0,
        );
        assert!(!allow);
        assert_eq!(reason, "stable");
    }

    #[test]
    fn auto_advance_policy_always_overrides_quality_gate() {
        use yarli_core::entities::continuation::{
            ContinuationPayload, ContinuationQualityGate, RunSummary, TrancheKind, TrancheSpec,
        };

        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
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
                allow_auto_advance: false,
                reason: "stable".into(),
                trend: Some(DeteriorationTrend::Stable),
                score: Some(30.0),
            }),
        };

        let (allow, reason) = should_auto_advance_planned_tranche(
            &payload,
            AutoAdvanceConfig {
                policy: AutoAdvancePolicy::Always,
                max_tranches: 0,
            },
            0,
        );
        assert!(allow);
        assert_eq!(reason, "auto_advance_policy=always");
    }

    #[test]
    fn auto_advance_respects_max_tranche_cap() {
        use yarli_core::entities::continuation::{
            ContinuationPayload, ContinuationQualityGate, RunSummary, TrancheKind, TrancheSpec,
        };

        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
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
                score: Some(5.0),
            }),
        };

        let (allow, reason) = should_auto_advance_planned_tranche(
            &payload,
            AutoAdvanceConfig {
                policy: AutoAdvancePolicy::ImprovingOnly,
                max_tranches: 2,
            },
            2,
        );
        assert!(!allow);
        assert!(reason.contains("max_auto_advance_tranches=2"));
    }

    #[test]
    fn plan_target_completion_state_detects_status() {
        let plan = "- [x] CARD-R8-01\n- [ ] CARD-R8-02\n";
        assert_eq!(
            plan_target_completion_state(plan, "CARD-R8-01").unwrap(),
            Some(true)
        );
        assert_eq!(
            plan_target_completion_state(plan, "CARD-R8-02").unwrap(),
            Some(false)
        );
        assert_eq!(
            plan_target_completion_state(plan, "CARD-R8-03").unwrap(),
            None
        );
    }

    #[test]
    fn plan_target_completion_state_supports_common_non_checkbox_format() {
        let plan = "1. I8A complete\n2. I8B incomplete\n";
        assert_eq!(
            plan_target_completion_state(plan, "I8A").unwrap(),
            Some(true)
        );
        assert_eq!(
            plan_target_completion_state(plan, "I8B").unwrap(),
            Some(false)
        );
    }

    #[test]
    fn plan_guard_preflight_allows_completed_implement_target_for_auto_verify() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "echo ok" }]
[plan_guard]
target = "CARD-R8-01"
mode = "implement"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] CARD-R8-01\n",
        )
        .unwrap();
        let loaded = prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();

        let context = run_spec_plan_guard_preflight_with_override(&loaded, false).unwrap();
        assert!(context.is_some());
    }

    #[test]
    fn plan_guard_preflight_allows_completed_target_with_verify_only_override() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "echo ok" }]
[plan_guard]
target = "CARD-R8-01"
mode = "implement"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] CARD-R8-01\n",
        )
        .unwrap();
        let loaded = prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();

        let context = run_spec_plan_guard_preflight_with_override(&loaded, true)
            .expect("override should allow");
        assert!(context.is_some());
    }

    #[test]
    fn plan_guard_verify_only_requires_verification_objective_text() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "echo ok" }]
[plan_guard]
target = "CARD-R8-01"
mode = "verify-only"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] CARD-R8-01\n",
        )
        .unwrap();
        let loaded = prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();

        let err = run_spec_plan_guard_preflight_with_override(&loaded, false).unwrap_err();
        assert!(err.to_string().contains("requires objective text"));
    }

    #[test]
    fn plan_guard_post_run_allows_same_prompt_after_completion() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement CARD-R8-01"
[tasks]
items = [{ key = "test", cmd = "echo ok" }]
[plan_guard]
target = "CARD-R8-01"
mode = "implement"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] CARD-R8-01\n",
        )
        .unwrap();
        let loaded = prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let context = run_spec_plan_guard_preflight_with_override(&loaded, false)
            .expect("preflight should pass");
        let context = context.expect("context should be present");

        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] CARD-R8-01\n",
        )
        .unwrap();

        enforce_plan_guard_post_run(&loaded, &context).unwrap();
    }
}
