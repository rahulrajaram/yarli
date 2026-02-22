use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::config::DEFAULT_CONFIG_PATH;
use crate::DEFAULT_CONTINUATION_FILE;
use crate::YARLI_VERSION;

/// YARLI — Yet Another Orchestrator Loop Implementation.
///
/// Deterministic orchestrator with state machines, event log, and safe Git handling.
///
/// Default workflow: `yarli run` resolves prompt context and executes config-driven plan tranches.
/// Prompt resolution precedence:
/// 1. `yarli run --prompt-file <path>`
/// 2. `yarli.toml` `[run].prompt_file`
/// 3. Legacy fallback lookup for `PROMPT.md`
///
/// Recommended durability: use Postgres (`core.backend = "postgres"`); in-memory mode blocks writes
/// unless explicitly opted in via `core.allow_in_memory_writes = true`.
///
/// Optional memories: Backend-backed memory hints/storage can be enabled via `yarli.toml`
/// (`[memory.backend] enabled = true`). See `yarli init --help` for config keys.
#[derive(Parser)]
#[command(name = "yarli", version = YARLI_VERSION, about)]
pub(crate) struct Cli {
    /// Force stream mode output (inline viewport, no fullscreen TUI).
    #[arg(long, global = true)]
    pub(crate) stream: bool,

    /// Force dashboard mode (fullscreen TUI with panel layout).
    #[arg(long, global = true)]
    pub(crate) tui: bool,

    #[command(subcommand)]
    pub(crate) command: Commands,
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
- postgres.database_url_file (file containing DSN; Kubernetes secret volume pattern)
- or DATABASE_URL env var for deployment-time secret injection

Recommended for consumers: use Postgres (`core.backend = "postgres"`) for durable runs.
`core.backend = "in-memory"` is intended for local throwaway usage only and blocks write
commands unless `core.allow_in_memory_writes = true`.

[cli]
- cli.backend (optional; codex|claude|gemini|custom)
- cli.prompt_mode (default: "arg"; values: arg|stdin)
- cli.command (optional; executable to invoke)
- cli.args (default: []; argv list)
- cli.env_unset (default: []; environment variables to unset before CLI invocation)

[event_loop]
- event_loop.max_iterations (default: 5; reserved for future iterative loop controls)
- event_loop.max_runtime_seconds (default: 14400; reserved for future iterative loop controls)
- event_loop.idle_timeout_secs (default: 1800; reserved for future iterative loop controls)
- event_loop.checkpoint_interval (default: 5; reserved for future iterative loop controls)

[features]
- features.parallel (default: true; requires execution.worktree_root and enables per-task workspace execution)
- features.parallel_worktree (default: true; use git worktrees instead of directory copies for parallel workspaces)

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
- execution.working_dir (default: "."; expands `~` and `$ENV_VAR`)
- execution.worktree_root (default: unset; required when features.parallel = true; expands `~` and `$ENV_VAR`)
- execution.worktree_exclude_paths (default: [".yarl/workspaces",".yarli","target","node_modules",".venv","venv","__pycache__"]; names/paths excluded from workspace copies)
- execution.command_timeout_seconds (default: 300; 0 disables timeout)
- execution.tick_interval_ms (default: 100)
- execution.overwatch.service_url (required when runner = "overwatch")
- execution.overwatch.profile (optional)
- execution.overwatch.soft_timeout_seconds (optional)
- execution.overwatch.silent_timeout_seconds (optional)
- execution.overwatch.max_log_bytes (optional)

[run]
- run.prompt_file (optional; default prompt file for `yarli run`, relative to repo root)
- run.objective (optional; default objective when no prompt override is provided)
- run.continue_wait_timeout_seconds (default: 0; seconds to wait for continuation availability before failing)
- run.allow_stable_auto_advance (legacy compatibility toggle; prefer run.auto_advance_policy)
- run.auto_advance_policy (default: stable-ok; values: improving-only|stable-ok|always)
- run.task_health (config block: improving/stable/deteriorating actions)
- run.task_health.improving (default: "continue"; values: continue|checkpoint-now|force-pivot|stop-and-summarize)
- run.task_health.stable (default: "continue"; values: continue|checkpoint-now|force-pivot|stop-and-summarize)
- run.task_health.deteriorating (default: "continue"; values: continue|checkpoint-now|force-pivot|stop-and-summarize)
- run.soft_token_cap_ratio (default: 0.9; when set, checkpoints when total tokens reach this fraction of max_run_total_tokens)
- run.max_auto_advance_tranches (default: 0; 0 = unlimited auto-advance per invocation)
- run.enable_plan_tranche_grouping (default: false; group adjacent plan entries by shared tranche_group metadata)
- run.max_grouped_tasks_per_tranche (default: 0; 0 = unlimited tasks per grouped tranche)
- run.enforce_plan_tranche_allowed_paths (default: false; surface `allowed_paths=` plan metadata as scope constraints)
- run.merge_conflict_resolution (default: "fail"; values: fail|manual|auto-repair|llm-assisted)
- run.merge_repair_command (optional; shell command for llm-assisted conflict repair)
- run.merge_repair_timeout_seconds (default: 300; 0 = no timeout)
- run.auto_commit_interval (default: 1; commit state files every N tranches; 0 disables)
- run.auto_commit_message (optional; template with {tranche_key}/{run_id}/{tranches_completed}/{tranches_total})
- run.tasks (optional; array-of-table `[[run.tasks]]` entries with key/cmd/class)
- run.tranches (optional; array-of-table `[[run.tranches]]` entries with key/objective/task_keys)
- run.plan_guard.target (optional; when set, enforces plan target contract)
- run.plan_guard.mode (default: implement; values: implement|verify-only)
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
- ui.verbose_output (default: false; stream command output to terminal scrollback)
- ui.cancellation_diagnostics (default: false; include extra cancel provenance diagnostics)

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
# Alternatively set a file path mounted from a secret:
# database_url_file = "/run/secrets/yarli-postgres-url"

# --- CLI_BACKEND_BEGIN ---
[cli]
# LLM CLI backend configuration (used by default `yarli run` plan-driven dispatch).
# backend = "codex" | "claude" | "gemini" | "custom"
# prompt_mode = "arg" | "stdin"
# command = "codex"
# args = ["exec", "--json"]
# env_unset = ["CLAUDECODE"]

[event_loop]
# Reserved for future iterative loop controls.
max_iterations = 5
max_runtime_seconds = 14400
idle_timeout_secs = 1800
checkpoint_interval = 5

[features]
# Parallel task execution (requires [execution].worktree_root for per-task workspaces).
parallel = true
# Use git worktrees instead of full directory copies for parallel workspaces.
# Falls back to copy mode when git worktrees are unavailable.
# parallel_worktree = true
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
# Working directory for command execution (`~` and `$VARS` are expanded).
working_dir = "."
# Root directory for per-task workspaces/worktrees (required when features.parallel = true).
# `~` and `$VARS` are expanded.
# worktree_root = ".yarl/workspaces"
# Directory names or paths to exclude from per-task workspace copies.
worktree_exclude_paths = [".yarl/workspaces", ".yarli", "target", "node_modules", ".venv", "venv", "__pycache__"]
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
# Optional default objective when no prompt override is present.
# objective = "verify workspace"
# Seconds to wait for continuation payload availability (`yarli run continue`).
continue_wait_timeout_seconds = 0
# Legacy compatibility toggle for stable-trend auto-advance.
allow_stable_auto_advance = false
# Preferred auto-advance policy: improving-only | stable-ok | always
auto_advance_policy = "stable-ok"
# Optional task-health actions by deterioration trend:
# - continue: proceed with continuation + planned auto-advance evaluation
# - checkpoint-now: stop planned auto-advance and checkpoint the current context
# - force-pivot: force the next action to switch task focus (requires operator follow-up)
# - stop-and-summarize: stop auto-advance and summarize state for manual review
[run.task_health]
improving = "continue"
stable = "continue"
deteriorating = "continue"
# Optional soft-token checkpoint trigger ratio for run-level token hard caps.
# - default: 0.9
# - 0 disables the soft cap.
soft_token_cap_ratio = 0.9
# Maximum planned-tranche auto-advances per invocation (0 = unlimited).
max_auto_advance_tranches = 0
# Group adjacent open plan entries with matching `tranche_group=<name>` metadata.
enable_plan_tranche_grouping = false
# Cap grouped tasks per tranche (0 = unlimited).
max_grouped_tasks_per_tranche = 0
# Surface per-tranche `allowed_paths=...` metadata as explicit scope instructions.
enforce_plan_tranche_allowed_paths = false
# Merge conflict resolution strategy: fail | manual | auto-repair | llm-assisted
# merge_conflict_resolution = "fail"
# Shell command for LLM-assisted merge conflict repair (required when llm-assisted).
# merge_repair_command = "claude --print"
# Timeout in seconds for the repair command (0 = no timeout).
# merge_repair_timeout_seconds = 300
# Auto-commit YARLI state files after every N tranches (0 = disabled).
# auto_commit_interval = 1
# Template for auto-commit messages (placeholders: {tranche_key}, {run_id}, {tranches_completed}, {tranches_total}).
# auto_commit_message = "yarli: checkpoint after {tranche_key} ({tranches_completed}/{tranches_total})"
# Optional run-spec task catalog (project-level verification/work commands).
# [[run.tasks]]
# key = "lint"
# cmd = "cargo clippy --workspace -- -D warnings"
# class = "cpu"
#
# [[run.tasks]]
# key = "test"
# cmd = "cargo test --workspace"
# class = "io"
#
# Optional explicit tranche definitions for run-spec execution.
# [[run.tranches]]
# key = "verify"
# objective = "verification tranche"
# task_keys = ["lint", "test"]
#
# Optional plan-guard contract for run-spec execution.
# [run.plan_guard]
# target = "I8B"
# mode = "implement"
# default_pace = "batch"

[budgets]
# Token guardrails (enabled by default to prevent runaway cost).
max_task_total_tokens = 25000
max_run_total_tokens = 250000

# Optional hard limits. Leave commented/unset for no limit.
# max_task_rss_bytes = 1073741824
# max_task_cpu_user_ticks = 100000
# max_task_cpu_system_ticks = 100000
# max_task_io_read_bytes = 1073741824
# max_task_io_write_bytes = 1073741824
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
# verbose_output = false
# cancellation_diagnostics = false
"#;
pub(crate) fn init_config_template(backend: Option<InitBackend>) -> String {
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
# Parallel execution requires [execution].worktree_root.
parallel = true
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
parallel = true
"#
        }
        Some(InitBackend::Claude) => {
            r#"[cli]
backend = "claude"
prompt_mode = "arg"
command = "claude"
args = ["-p", "--dangerously-skip-permissions", "--model", "sonnet-4.5"]
# env_unset = ["CLAUDECODE"]

[event_loop]
max_iterations = 5
max_runtime_seconds = 14400
idle_timeout_secs = 1800
checkpoint_interval = 5

[features]
parallel = true
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
parallel = true
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
pub(crate) fn replace_between(haystack: &str, begin: &str, end: &str, replacement: &str) -> String {
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
pub(crate) enum Commands {
    #[command(
        about = "Manage orchestration runs (default: config-first plan-driven execution)",
        long_about = "Manage orchestration runs.\n\nDefault behavior:\n- `yarli run` (no subcommand) resolves prompt context in this order:\n  1. `--prompt-file <path>`\n  2. `[run].prompt_file` in `yarli.toml`\n  3. fallback lookup of `PROMPT.md`\n- Run-spec baseline configuration can be defined in `yarli.toml` under `[run]` + `[[run.tasks]]` + `[[run.tranches]]` + `[run.plan_guard]`.\n- `PROMPT.md` may optionally include a `yarli-run` fenced block as a per-prompt override layer.\n- `yarli run` discovers incomplete tranches from `IMPLEMENTATION_PLAN.md` and dispatches them via `[cli]` command settings, followed by a verification task.\n- Optional grouped dispatch is available with `[run].enable_plan_tranche_grouping = true` and `tranche_group=<name>` plan metadata.\n- If no incomplete tranches are found and no run-spec configuration is present, `yarli run` dispatches the full prompt text as a single task.\n- Legacy run-spec task/tranche orchestration is used only as fallback when config-first dispatch cannot be materialized.\n\nControl model:\n- Built-in Yarli policy gates are code-defined checks (`yarli gate ...`) that evaluate run/task state.\n- Verification command chain is plan/config/script-defined execution work (tranches + verification commands).\n- Observer events are telemetry only and do not gate or mutate active run execution.\n- Operator controls (`yarli run pause|resume|cancel`) are explicit control-plane actions.\n\nOptional integrations:\n- Memories: enable Backend-backed hints/storage via `yarli.toml` (`[memory.backend] enabled = true`). Memory hints are surfaced in `yarli run status` and `yarli run explain-exit`.\n\nExamples:\n- `yarli run`\n- `yarli run --prompt-file prompts/I8B.md --stream`\n\nOther subcommands:\n- `yarli run start ...` for ad-hoc runs with explicit `--cmd`.\n- `yarli run status ...` / `yarli run explain-exit ...` for inspection.\n- `yarli run pause|resume|cancel ...` for explicit operator control.\n- `yarli run batch ...` is legacy/back-compat pace-based execution."
    )]
    Run {
        /// Override the prompt file used by default `yarli run` (no subcommand).
        #[arg(long)]
        prompt_file: Option<PathBuf>,
        /// Allow recursive `yarli run` from task commands for this invocation.
        #[arg(long, default_value_t = false)]
        allow_recursive_run: bool,
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
        long_about = "Manage built-in Yarli policy gates (code-defined checks).\n\nExamples:\n- `yarli gate list`\n- `yarli gate list --run`\n- `yarli gate rerun <task-id>`\n- `yarli gate rerun <task-id> --gate tests_passed`\n\nNotes:\n- These gates are distinct from the verification command chain configured by plan/config/scripts.\n- `gate rerun` is a write command and requires Postgres durability or explicit in-memory opt-in."
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
        about = "Manage the structured plan (tranches file)",
        long_about = "Manage the structured plan in .yarli/tranches.toml.\n\nExamples:\n  yarli plan tranche add --key TP-05 --summary \"Config loader\"\n  yarli plan tranche complete --key TP-05\n  yarli plan tranche list\n  yarli plan tranche remove --key TP-05\n  yarli plan validate"
    )]
    Plan {
        #[command(subcommand)]
        action: PlanAction,
    },
    #[command(
        about = "Inspect live scheduler state (run-level and queue internals)",
        long_about = "Inspect live scheduler state from persisted and in-memory surfaces.\n\nExamples:\n  yarli debug queue-depth\n  yarli debug active-leases\n  yarli debug resource-usage <run-id>"
    )]
    Debug {
        #[command(subcommand)]
        action: DebugAction,
    },
    #[command(
        about = "Manage Postgres migration lifecycle and rollback safety",
        long_about = "Manage Postgres migrations for YARLI durable storage.

Examples:
- `yarli migrate status`
- `yarli migrate up`
- `yarli migrate up --target 0002_init`
- `yarli migrate down`
- `yarli migrate down --target 0001`
- `yarli migrate backup`
- `yarli migrate restore --label 20260222_120000`

`status`: show applied migrations vs code-defined pending migrations.
`up`: apply pending migrations in order.
`down`: apply reverse migrations to a target checkpoint (creates a backup first).
`backup`: capture a schema-level snapshot for rollback safety.
`restore`: copy a previous backup snapshot back into live schema tables."
    )]
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
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
pub(crate) enum InitBackend {
    Codex,
    Claude,
    Gemini,
}

#[derive(Subcommand)]
pub(crate) enum RunAction {
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
    #[command(
        about = "Pause active runs (operator control)",
        long_about = "Pause active runs (operator control).\n\nTransitions the selected run(s) to RUN_BLOCKED with operator reason metadata.\nThis is an explicit control-plane action and does not rely on external observers.\n\nSelection:\n- Provide `<run-id>` (UUID or unique run-list prefix), or\n- use `--all-active` to pause all active/verifying runs.\n\nExamples:\n  yarli run pause 019577a2-...\n  yarli run pause --all-active --reason \"maintenance window\""
    )]
    Pause {
        /// Run ID to pause (UUID or unique run-list prefix).
        run_id: Option<String>,
        /// Pause all active/verifying runs.
        #[arg(long, default_value_t = false, conflicts_with = "run_id")]
        all_active: bool,
        /// Reason for pause.
        #[arg(short, long, default_value = "paused by operator")]
        reason: String,
    },
    #[command(
        about = "Resume paused runs (operator control)",
        long_about = "Resume paused runs (operator control).\n\nTransitions selected RUN_BLOCKED runs back to RUN_ACTIVE.\nThis is an explicit control-plane action and does not rely on external observers.\n\nSelection:\n- Provide `<run-id>` (UUID or unique run-list prefix), or\n- use `--all-paused` to resume all paused runs.\n\nExamples:\n  yarli run resume 019577a2-...\n  yarli run resume --all-paused --reason \"maintenance complete\""
    )]
    Resume {
        /// Run ID to resume (UUID or unique run-list prefix).
        run_id: Option<String>,
        /// Resume all paused runs.
        #[arg(long, default_value_t = false, conflicts_with = "run_id")]
        all_paused: bool,
        /// Reason for resume.
        #[arg(short, long, default_value = "resumed by operator")]
        reason: String,
    },
    #[command(
        about = "Cancel active runs (operator control)",
        long_about = "Cancel active runs (operator control).\n\nTransitions selected run(s) to RUN_CANCELLED, cancels non-terminal tasks,\nand drains queued entries for those runs.\nThis is an explicit control-plane action and does not rely on external observers.\n\nSelection:\n- Provide `<run-id>` (UUID or unique run-list prefix), or\n- use `--all-active` to cancel all active/verifying runs.\n\nExamples:\n  yarli run cancel 019577a2-...\n  yarli run cancel --all-active --reason \"operator stop\""
    )]
    Cancel {
        /// Run ID to cancel (UUID or unique run-list prefix).
        run_id: Option<String>,
        /// Cancel all active/verifying runs.
        #[arg(long, default_value_t = false, conflicts_with = "run_id")]
        all_active: bool,
        /// Reason for cancellation.
        #[arg(short, long, default_value = "cancelled by operator")]
        reason: String,
    },
    #[cfg(feature = "sw4rm")]
    #[command(
        about = "Run as a sw4rm orchestrator agent (requires `sw4rm` feature)",
        long_about = "Run as a sw4rm orchestrator agent.\n\nBoots yarli as an agent in the sw4rm multi-agent protocol. The agent:\n1. Registers with the sw4rm registry\n2. Receives objectives from the sw4rm scheduler\n3. Dispatches implementation work to LLM agents via the router\n4. Verifies results using yarli's scheduler and gate engine\n5. Iterates (dispatch -> verify -> fix) until success or max iterations\n\nConfiguration is read from the `[sw4rm]` section of yarli.toml:\n- sw4rm.agent_name (default: \"yarli-orchestrator\")\n- sw4rm.capabilities (default: [\"orchestrate\", \"verify\", \"git\"])\n- sw4rm.registry_url (default: \"http://127.0.0.1:50051\")\n- sw4rm.router_url (default: \"http://127.0.0.1:50052\")\n- sw4rm.scheduler_url (default: \"http://127.0.0.1:50053\")\n- sw4rm.max_fix_iterations (default: 5)\n- sw4rm.llm_response_timeout_secs (default: 300)\n\nVerification commands are loaded using the same prompt resolution precedence as `yarli run` when available; otherwise defaults to `cargo build`.\n\nNote: requires `--features sw4rm` at build time.\n\nExamples:\n  yarli run sw4rm\n  YARLI_LOG=debug yarli run sw4rm"
    )]
    Sw4rm,
}

#[derive(Subcommand)]
pub(crate) enum TaskAction {
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
    #[command(
        about = "Show command output for a task",
        long_about = "Show command output for a task.\n\nDumps all captured stdout/stderr from the task's command execution.\nRequires a durable backend (Postgres) to have output events persisted.\n\nExamples:\n  yarli task output 019577a3-...\n  yarli task output <task-id>"
    )]
    Output {
        /// Task ID to show output for (UUID).
        task_id: String,
    },
}

#[derive(Subcommand)]
pub(crate) enum GateAction {
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
pub(crate) enum WorktreeAction {
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
pub(crate) enum MergeAction {
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
pub(crate) enum AuditAction {
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
    #[command(
        about = "Query the JSONL audit log",
        long_about = "Query the JSONL audit log.\n\nFilter by run ID, task ID, category, actor, and time range.\nSupports multiple output formats and pagination.\n\nCategories:\n  policy_decision      — policy engine allow/deny decisions\n  destructive_attempt   — blocked destructive git operations\n  token_consumed        — LLM token usage records\n  gate_evaluation      — gate pass/fail results\n  command_execution    — command execution terminal events\n\nOutput formats:\n  table (default), json, csv\n\nExamples:\n  yarli audit query --category policy_decision\n  yarli audit query --run-id 019577a5-... --format table\n  yarli audit query --since 2026-02-20T00:00:00Z --before 2026-02-22T23:59:59Z\n  yarli audit query --after 019577a5-... --limit 25 --format csv"
    )]
    Query {
        /// Path to the audit JSONL file.
        #[arg(short, long, default_value = ".yarl/audit.jsonl")]
        file: String,
        /// Filter by run ID.
        #[arg(long)]
        run_id: Option<String>,
        /// Filter by task ID.
        #[arg(long)]
        task_id: Option<String>,
        /// Filter by category.
        #[arg(short, long)]
        category: Option<String>,
        /// Filter by actor.
        #[arg(long)]
        actor: Option<String>,
        /// Include entries on or after this UTC timestamp (RFC 3339 or YYYY-MM-DD).
        #[arg(long)]
        since: Option<String>,
        /// Include entries on or before this UTC timestamp (RFC 3339 or YYYY-MM-DD).
        #[arg(long)]
        before: Option<String>,
        /// Cursor-based pagination. Start after this audit ID.
        #[arg(long)]
        after: Option<String>,
        /// Offset entries before returning the page.
        #[arg(long, default_value = "0")]
        offset: usize,
        /// Max entries to return (0 = all).
        #[arg(short, long, default_value = "50")]
        limit: usize,
        /// Output format.
        #[arg(short, long, default_value_t = AuditOutputFormat::Table, value_enum)]
        format: AuditOutputFormat,
    },
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq, Eq)]
pub(crate) enum AuditOutputFormat {
    Table,
    Json,
    Csv,
}

#[derive(Subcommand)]
pub(crate) enum PlanAction {
    #[command(
        about = "Manage structured tranches",
        long_about = "Manage structured tranches in .yarli/tranches.toml.\n\nExamples:\n  yarli plan tranche add --key TP-05 --summary \"Config loader\"\n  yarli plan tranche complete --key TP-05\n  yarli plan tranche list\n  yarli plan tranche remove --key TP-05"
    )]
    Tranche {
        #[command(subcommand)]
        action: TrancheAction,
    },
    #[command(
        about = "Validate the structured tranches file",
        long_about = "Validate the structured tranches file (.yarli/tranches.toml).\n\nChecks all tranche keys for validity, detects duplicates, and reports errors.\n\nExamples:\n  yarli plan validate"
    )]
    Validate,
}

#[derive(Subcommand)]
pub(crate) enum TrancheAction {
    #[command(about = "Add a new tranche definition")]
    Add {
        /// Tranche key (e.g. TP-05, M3A, CARD-001).
        #[arg(short, long)]
        key: String,
        /// Short summary describing the tranche objective.
        #[arg(short, long)]
        summary: String,
        /// Optional tranche group for grouped dispatch.
        #[arg(short, long)]
        group: Option<String>,
        /// Optional comma-separated allowed paths for scope enforcement.
        #[arg(short, long, value_delimiter = ',')]
        allowed_paths: Vec<String>,
        /// Verification command to run after completion (e.g. "cargo test --offline query_cache").
        #[arg(short, long)]
        verify: Option<String>,
        /// Done-criteria describing what constitutes completion.
        #[arg(short, long)]
        done_when: Option<String>,
        /// Per-tranche token budget override.
        #[arg(short, long)]
        max_tokens: Option<u64>,
    },
    #[command(about = "Mark a tranche as complete")]
    Complete {
        /// Tranche key to mark complete.
        #[arg(short, long)]
        key: String,
    },
    #[command(about = "List all tranches")]
    List,
    #[command(about = "Remove a tranche definition")]
    Remove {
        /// Tranche key to remove.
        #[arg(short, long)]
        key: String,
    },
}

#[derive(Subcommand)]
pub(crate) enum DebugAction {
    #[command(
        about = "Show queue depth by run and command class",
        long_about = "Show live queue depth grouped by run and command class.\n\nUseful to identify backlog growth or class-level saturation.\n\nExamples:\n  yarli debug queue-depth"
    )]
    QueueDepth,
    #[command(
        about = "Show active task leases and TTL",
        long_about = "Show currently leased tasks with owner and lease expiry.\n\nExamples:\n  yarli debug active-leases"
    )]
    ActiveLeases,
    #[command(
        about = "Show aggregated resource usage for a run",
        long_about = "Show current run-level resource usage totals and per-budget comparison.\n\nExamples:\n  yarli debug resource-usage 019577a2-..."
    )]
    ResourceUsage {
        /// Run ID (UUID).
        run_id: String,
    },
}

#[derive(Subcommand)]
pub(crate) enum MigrateAction {
    #[command(
        about = "Show migration status and pending/applied deltas",
        long_about = "Show the migration status for the durable Postgres store.

This command reports each migration entry in code order and whether it is
currently applied in the target database."
    )]
    Status,

    #[command(
        about = "Apply pending migrations",
        long_about = "Apply pending migrations to the target Postgres database.

Use --target to stop at a specific migration version. If omitted, all pending
migrations are applied."
    )]
    Up {
        /// Stop after applying this migration (for example 0001 or 0001_init).
        #[arg(short, long)]
        target: Option<String>,
    },

    #[command(
        about = "Revert migrations to a target checkpoint",
        long_about = "Revert migrations in reverse order to the target migration.

If --target is omitted, the system rolls back exactly one migration.
Before rollback, the command automatically creates a labeled backup snapshot
in `yarli_migration_backups` to support manual restore if needed."
    )]
    Down {
        /// Target checkpoint (for example 0001 or 0001_init). Defaults to one step back.
        #[arg(short, long)]
        target: Option<String>,
        /// Optional backup label used for rollback restore. Defaults to timestamp.
        #[arg(short, long)]
        backup_label: Option<String>,
    },

    #[command(
        about = "Create a backup snapshot for migration rollback safety",
        long_about = "Create a full data snapshot of migration-related tables.

Backup snapshots are stored in `yarli_migration_backups.<label>` and can be
restored with `yarli migrate restore --label <label>`."
    )]
    Backup {
        /// Backup label (defaults to timestamp-based label).
        #[arg(short, long)]
        label: Option<String>,
    },

    #[command(
        about = "Restore data from a migration backup snapshot",
        long_about = "Restore known migration-related tables from a backup snapshot
captured by `yarli migrate backup`. This command only restores tables in the
snapshot schema and writes a warning when schemas differ."
    )]
    Restore {
        /// Snapshot label to restore from.
        label: String,
    },
}
