# YARLI CLI Help Snapshots

Generated from live clap output using `target/debug/yarli ... --help`.

## `target/debug/yarli`

```text
YARLI — Yet Another Orchestrator Loop Implementation.

Deterministic orchestrator with state machines, event log, and safe Git handling.

Default workflow: `yarli run` resolves prompt context and executes config-driven plan tranches. Prompt resolution precedence: 1. `yarli run --prompt-file <path>` 2. `yarli.toml` `[run].prompt_file` 3. Legacy fallback lookup for `PROMPT.md`

Recommended durability: use Postgres (`core.backend = "postgres"`); in-memory mode blocks writes unless explicitly opted in via `core.allow_in_memory_writes = true`.

Optional memories: adapter-backed memory hints/storage can be enabled via `yarli.toml` (`[memory] provider = "default"` + `[memory.providers.default] ...`, or legacy `[memory.backend]`). See `yarli init --help` for config keys.

Usage: yarli [OPTIONS] <COMMAND>

Commands:
  run       Manage orchestration runs (default: config-first plan-driven execution)
  task      Manage tasks within a run
  gate      Manage verification gates
  worktree  Manage Git worktrees
  merge     Manage merge intents
  audit     View the audit log
  plan      Manage the structured plan (tranches file)
  debug     Inspect live scheduler state (run-level and queue internals)
  migrate   Manage Postgres migration lifecycle and rollback safety
  init      Initialize yarli.toml with a documented template
  info      Show version and detected terminal capabilities
  help      Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

## `target/debug/yarli run`

```text
Manage orchestration runs.

Default behavior:
- `yarli run` (no subcommand) resolves prompt context in this order:
  1. `--prompt-file <path>`
  2. `[run].prompt_file` in `yarli.toml`
  3. fallback lookup of `PROMPT.md`
- Run-spec baseline configuration can be defined in `yarli.toml` under `[run]` + `[[run.tasks]]` + `[[run.tranches]]` + `[run.plan_guard]`.
- `PROMPT.md` may optionally include a `yarli-run` fenced block as a per-prompt override layer.
- `yarli run` discovers incomplete tranches from `IMPLEMENTATION_PLAN.md` and dispatches them via `[cli]` command settings, followed by a verification task.
- Optional grouped dispatch is available with `[run].enable_plan_tranche_grouping = true` and `tranche_group=<name>` plan metadata.
- If no incomplete tranches are found and no run-spec configuration is present, `yarli run` dispatches the full prompt text as a single task.
- Legacy run-spec task/tranche orchestration is used only as fallback when config-first dispatch cannot be materialized.

Control model:
- Built-in Yarli policy gates are code-defined checks (`yarli gate ...`) that evaluate run/task state.
- Verification command chain is plan/config/script-defined execution work (tranches + verification commands).
- Observer events are telemetry only and do not gate or mutate active run execution.
- Operator controls (`yarli run pause|resume|cancel`) are explicit control-plane actions.

Optional integrations:
- Memories: enable adapter-backed hints/storage via `yarli.toml` (`[memory] provider = "default"` + `[memory.providers.default] ...`, or legacy `[memory.backend]`). Memory hints are surfaced in `yarli run status` and `yarli run explain-exit`.

Examples:
- `yarli run`
- `yarli run --prompt-file prompts/I8B.md --stream`

Other subcommands:
- `yarli run start ...` for ad-hoc runs with explicit `--cmd`.
- `yarli run status ...` / `yarli run explain-exit ...` for inspection.
- `yarli run pause|resume|cancel ...` for explicit operator control.
- `yarli run batch ...` is legacy/back-compat pace-based execution.

Usage: yarli run [OPTIONS] [COMMAND]

Commands:
  start         Start a new orchestration run
  batch         Start the default verification loop (config-backed) for this workspace
  status        Show the current status of a run
  explain-exit  Explain why a run is not done (Why Not Done? engine)
  list          List all known runs
  continue      Continue from a previous run's unfinished/failed tasks
  pause         Pause active runs (operator control)
  resume        Resume paused runs (operator control)
  cancel        Cancel active runs (operator control)
  help          Print this message or the help of the given subcommand(s)

Options:
      --prompt-file <PROMPT_FILE>
          Override the prompt file used by default `yarli run` (no subcommand)

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --allow-recursive-run
          Allow recursive `yarli run` from task commands for this invocation

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli task`

```text
Manage tasks within a run.

Examples:
- `yarli task list <run-id>`
- `yarli task explain <task-id>`
- `yarli task unblock <task-id> --reason "..."`

Notes:
- `task unblock` is a write command and requires Postgres durability or explicit in-memory opt-in.

Usage: yarli task [OPTIONS] <COMMAND>

Commands:
  list      List tasks for a run
  explain   Explain why a task is not done (Why Not Done? engine)
  unblock   Unblock a task (clear its blocker and transition to ready)
  annotate  Annotate a task with blocker detail (e.g. link to blocker file)
  output    Show command output for a task
  help      Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli gate`

```text
Manage built-in Yarli policy gates (code-defined checks).

Examples:
- `yarli gate list`
- `yarli gate list --run`
- `yarli gate rerun <task-id>`
- `yarli gate rerun <task-id> --gate tests_passed`

Notes:
- These gates are distinct from the verification command chain configured by plan/config/scripts.
- `gate rerun` is a write command and requires Postgres durability or explicit in-memory opt-in.

Usage: yarli gate [OPTIONS] <COMMAND>

Commands:
  list   List configured gates for a run or task
  rerun  Re-run a specific gate evaluation
  help   Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli worktree`

```text
Manage Git worktrees.

Examples:
- `yarli worktree status <run-id>`
- `yarli worktree recover <worktree-id> --action abort`

Notes:
- Recovery actions are policy-gated write operations and are audited.

Usage: yarli worktree [OPTIONS] <COMMAND>

Commands:
  status   Show worktree status for a run
  recover  Recover from an interrupted git operation in a worktree
  help     Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli merge`

```text
Manage merge intents.

Examples:
- `yarli merge request <source> <target> --run-id <run-id> --strategy merge-no-ff`
- `yarli merge approve <merge-id>`
- `yarli merge reject <merge-id> --reason "..."`
- `yarli merge status <merge-id>`

Notes:
- Approve/reject are policy-gated write operations and are audited.

Usage: yarli merge [OPTIONS] <COMMAND>

Commands:
  request  Request a new merge intent
  approve  Approve a pending merge intent
  reject   Reject a pending merge intent
  status   Show status of a merge intent
  help     Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli audit`

```text
View the audit log.

Examples:
- `yarli audit tail`
- `yarli audit tail --file .yarl/audit.jsonl --lines 200`
- `yarli audit tail --category policy_decision`

Usage: yarli audit [OPTIONS] <COMMAND>

Commands:
  tail   Tail the JSONL audit log
  query  Query the JSONL audit log
  help   Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli plan`

```text
Manage the structured plan in .yarli/tranches.toml.

Examples:
  yarli plan tranche add --key TP-05 --summary "Config loader"
  yarli plan tranche complete --key TP-05
  yarli plan tranche list
  yarli plan tranche remove --key TP-05
  yarli plan validate

Usage: yarli plan [OPTIONS] <COMMAND>

Commands:
  tranche   Manage structured tranches
  validate  Validate the structured tranches file
  help      Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli debug`

```text
Inspect live scheduler state from persisted and in-memory surfaces.

Examples:
  yarli debug queue-depth
  yarli debug active-leases
  yarli debug resource-usage <run-id>

Usage: yarli debug [OPTIONS] <COMMAND>

Commands:
  queue-depth     Show queue depth by run and command class
  active-leases   Show active task leases and TTL
  resource-usage  Show aggregated resource usage for a run
  help            Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli migrate`

```text
Manage Postgres migrations for YARLI durable storage.

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
`restore`: copy a previous backup snapshot back into live schema tables.

Usage: yarli migrate [OPTIONS] <COMMAND>

Commands:
  status   Show migration status and pending/applied deltas
  up       Apply pending migrations
  down     Revert migrations to a target checkpoint
  backup   Create a backup snapshot for migration rollback safety
  restore  Restore data from a migration backup snapshot
  help     Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli init`

```text
Initialize `yarli.toml` with a fully annotated template.

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

[memory]
- memory.enabled (optional; master switch, defaults to selected provider enabled state)
- memory.project_id (optional; defaults to prompt-root directory name)
- memory.provider (optional; select `[memory.providers.<name>]`; e.g. "default" or "kafka")

[memory.providers.<name>]
- memory.providers.<name>.type (default: "cli")
- memory.providers.<name>.enabled (default: true)
- memory.providers.<name>.endpoint (optional)
- memory.providers.<name>.command (default: "memory-backend")
- memory.providers.<name>.project_dir (optional; defaults to prompt root)
- memory.providers.<name>.query_limit (default: 8)
- memory.providers.<name>.inject_on_run_start (default: true)
- memory.providers.<name>.inject_on_failure (default: true)

[memory.backend] (legacy fallback)
- memory.backend.enabled (default: false)
- memory.backend.endpoint (optional)
- memory.backend.command (default: "memory-backend")
- memory.backend.project_dir (optional; defaults to prompt root)
- memory.backend.query_limit (default: 8)
- memory.backend.inject_on_run_start (default: true)
- memory.backend.inject_on_failure (default: true)

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


Usage: yarli init [OPTIONS]

Options:
      --path <PATH>
          Destination path for generated config
          
          [default: yarli.toml]

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --force
          Overwrite an existing config file

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

      --print
          Print the config template to stdout instead of writing a file

      --backend <BACKEND>
          Emit an opinionated template for a specific LLM CLI backend
          
          [possible values: codex, claude, gemini]

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli info`

```text
Show version and detected terminal capabilities.

Example:
- `yarli info`

Usage: yarli info [OPTIONS]

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run start`

```text
Start a new orchestration run with an explicit objective and commands.

Provide commands via `--cmd` (repeatable) or reference a named pace from
yarli.toml via `--pace`. The two are mutually exclusive.

Examples:
  yarli run start "fix linting" --cmd "cargo clippy -- -D warnings"
  yarli run start "full check" -c "cargo fmt --check" -c "cargo test"
  yarli run start "deploy" --pace deploy -w /opt/app
  yarli run start "build" --cmd "make" --timeout 600

Usage: yarli run start [OPTIONS] <OBJECTIVE>

Arguments:
  <OBJECTIVE>
          The objective describing what this run should accomplish

Options:
  -c, --cmd <CMD>
          Commands to execute (one per task). Use multiple times for multiple tasks

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --pace <PACE>
          Named run pace defined in yarli.toml ([run.paces.<name>]). Mutually exclusive with --cmd

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -w, --workdir <WORKDIR>
          Working directory for command execution (defaults to `execution.working_dir`)

      --timeout <TIMEOUT>
          Command timeout in seconds (defaults to `execution.command_timeout_seconds`, 0 = no timeout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run batch`

```text
Start the default verification loop using a named pace from yarli.toml.

Resolves the pace in this order:
1. Explicit `--pace` argument
2. A pace named "batch" if it exists in [run.paces]
3. The value of `run.default_pace`

This is a legacy/back-compat entry point; prefer prompt-run-spec `yarli run` for
new projects.

Examples:
  yarli run batch
  yarli run batch --pace ci
  yarli run batch --objective "nightly check" -w /repo --timeout 900

Usage: yarli run batch [OPTIONS]

Options:
      --objective <OBJECTIVE>
          Optional objective label (defaults to "batch")

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --pace <PACE>
          Named run pace to use (defaults to "batch" if defined, otherwise run.default_pace)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -w, --workdir <WORKDIR>
          Working directory for command execution (overrides pace/config defaults)

      --timeout <TIMEOUT>
          Command timeout in seconds (overrides pace/config defaults, 0 = no timeout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run status`

```text
Show the current status of a run.

Displays the run state, objective, task summary, and gate evaluation results.
If Memory backend is enabled, includes relevant memory hints.

`run-id` may be a full UUID or a unique prefix from `yarli run list`.

Examples:
  yarli run status 019577a2-...
  yarli run status <run-id>

Usage: yarli run status [OPTIONS] <RUN_ID>

Arguments:
  <RUN_ID>
          Run ID to query (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run explain-exit`

```text
Explain why a run is not done (Why Not Done? engine).

Runs the explain engine to diagnose why a run hasn't completed. Reports:
- Open/blocked tasks and their blockers
- Failed gate evaluations
- Policy denials
- Deterioration trends (repeated failures)

`run-id` may be a full UUID or a unique prefix from `yarli run list`.

Examples:
  yarli run explain-exit 019577a2-...
  yarli run explain-exit <run-id>

Usage: yarli run explain-exit [OPTIONS] <RUN_ID>

Arguments:
  <RUN_ID>
          Run ID to explain (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run list`

```text
List all runs found in the event store.

Shows each run's ID (short), state, objective, task counts, and last update time.
Useful for discovering active runs so you can query status or explain.

Examples:
  yarli run list

Usage: yarli run list [OPTIONS]

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run continue`

```text
Continue from a previous run using the auto-tranche continuation spec.

By default, yarli first loads the latest persisted continuation payload from
the event store (`run.continuation`), then falls back to `.yarli/continuation.json`
(written automatically on run exit). It then creates a new run from the suggested
next tranche (retry/unfinished or planned-next).
When continuation is not yet available, wait behavior is configured via
`[run] continue_wait_timeout_seconds` in yarli.toml.
After each successful run, yarli auto-advances through planned tranches when
quality gate criteria allow it.

Examples:
  yarli run continue
  yarli run continue --file .yarli/continuation.json

Usage: yarli run continue [OPTIONS]

Options:
      --file <FILE>
          Path to the continuation file (defaults to `.yarli/continuation.json`)
          
          [default: .yarli/continuation.json]

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run pause`

```text
Pause active runs (operator control).

Transitions the selected run(s) to RUN_BLOCKED with operator reason metadata.
This is an explicit control-plane action and does not rely on external observers.

Selection:
- Provide `<run-id>` (UUID or unique run-list prefix), or
- use `--all-active` to pause all active/verifying runs.

Examples:
  yarli run pause 019577a2-...
  yarli run pause --all-active --reason "maintenance window"

Usage: yarli run pause [OPTIONS] [RUN_ID]

Arguments:
  [RUN_ID]
          Run ID to pause (UUID or unique run-list prefix)

Options:
      --all-active
          Pause all active/verifying runs

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

  -r, --reason <REASON>
          Reason for pause
          
          [default: "paused by operator"]

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run resume`

```text
Resume paused runs (operator control).

Transitions selected RUN_BLOCKED runs back to RUN_ACTIVE.
This is an explicit control-plane action and does not rely on external observers.

Selection:
- Provide `<run-id>` (UUID or unique run-list prefix), or
- use `--all-paused` to resume all paused runs.

Examples:
  yarli run resume 019577a2-...
  yarli run resume --all-paused --reason "maintenance complete"

Usage: yarli run resume [OPTIONS] [RUN_ID]

Arguments:
  [RUN_ID]
          Run ID to resume (UUID or unique run-list prefix)

Options:
      --all-paused
          Resume all paused runs

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

  -r, --reason <REASON>
          Reason for resume
          
          [default: "resumed by operator"]

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli run cancel`

```text
Cancel active runs (operator control).

Transitions selected run(s) to RUN_CANCELLED, cancels non-terminal tasks,
and drains queued entries for those runs.
This is an explicit control-plane action and does not rely on external observers.

Selection:
- Provide `<run-id>` (UUID or unique run-list prefix), or
- use `--all-active` to cancel all active/verifying runs.

Examples:
  yarli run cancel 019577a2-...
  yarli run cancel --all-active --reason "operator stop"

Usage: yarli run cancel [OPTIONS] [RUN_ID]

Arguments:
  [RUN_ID]
          Run ID to cancel (UUID or unique run-list prefix)

Options:
      --all-active
          Cancel all active/verifying runs

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

  -r, --reason <REASON>
          Reason for cancellation
          
          [default: "cancelled by operator"]

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli task list`

```text
List tasks for a run.

Shows each task's ID, key, state, command class, and blocker (if any).

Examples:
  yarli task list 019577a2-...
  yarli task list <run-id>

Usage: yarli task list [OPTIONS] <RUN_ID>

Arguments:
  <RUN_ID>
          Run ID to list tasks for (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli task explain`

```text
Explain why a task is not done (Why Not Done? engine).

Runs the explain engine on a single task to diagnose why it hasn't completed.
Reports blockers, dependency status, gate failures, and attempt history.

Examples:
  yarli task explain 019577a3-...
  yarli task explain <task-id>

Usage: yarli task explain [OPTIONS] <TASK_ID>

Arguments:
  <TASK_ID>
          Task ID to explain (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli task unblock`

```text
Unblock a task (clear its blocker and transition to ready).

Clears the task's blocker and transitions it from Blocked to Ready so the
scheduler can pick it up again. This is a write operation that requires
Postgres durability or explicit in-memory opt-in.

Examples:
  yarli task unblock 019577a3-... --reason "dependency resolved manually"
  yarli task unblock <task-id>
  yarli task unblock <task-id> -r "approved by operator"

Usage: yarli task unblock [OPTIONS] <TASK_ID>

Arguments:
  <TASK_ID>
          Task ID to unblock (UUID)

Options:
  -r, --reason <REASON>
          Reason for unblocking (recorded in audit log)
          
          [default: "manually unblocked"]

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli task annotate`

```text
Annotate a task with blocker detail.

Sets a free-form annotation on the task that is displayed in `task explain`
and `run status` output. Useful for linking to external blocker files
or adding context about why a task is blocked.

Examples:
  yarli task annotate 019577a3-... --detail "see blocker-001.md"
  yarli task annotate <task-id> -d "waiting on upstream fix"

Usage: yarli task annotate [OPTIONS] --detail <DETAIL> <TASK_ID>

Arguments:
  <TASK_ID>
          Task ID to annotate (UUID)

Options:
  -d, --detail <DETAIL>
          Blocker detail text to attach to the task

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli task output`

```text
Show command output for a task.

Dumps all captured stdout/stderr from the task's command execution.
Requires a durable backend (Postgres) to have output events persisted.

Examples:
  yarli task output 019577a3-...
  yarli task output <task-id>

Usage: yarli task output [OPTIONS] <TASK_ID>

Arguments:
  <TASK_ID>
          Task ID to show output for (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli gate list`

```text
List configured gates for a run or task.

By default lists task-level gates. Use `--run` to show run-level gates instead.

Task gates: required_evidence_present, tests_passed, no_unresolved_conflicts,
            worktree_consistent, policy_clean
Run gates:  required_tasks_closed, required_evidence_present, no_unapproved_git_ops,
            no_unresolved_conflicts, worktree_consistent, policy_clean

Examples:
  yarli gate list
  yarli gate list --run

Usage: yarli gate list [OPTIONS]

Options:
      --run
          Show run-level gates instead of the default task-level gates

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli gate rerun`

```text
Re-run gate evaluation for a task.

Re-evaluates all gates for the given task, or a single gate if `--gate` is
specified. This is a write operation that requires Postgres durability or
explicit in-memory opt-in.

Valid gate names: required_tasks_closed, required_evidence_present, tests_passed,
                  no_unapproved_git_ops, no_unresolved_conflicts, worktree_consistent,
                  policy_clean

Examples:
  yarli gate rerun 019577a3-...
  yarli gate rerun <task-id> --gate tests_passed
  yarli gate rerun <task-id> -g policy_clean

Usage: yarli gate rerun [OPTIONS] <TASK_ID>

Arguments:
  <TASK_ID>
          Task ID to re-evaluate gates for (UUID)

Options:
  -g, --gate <GATE>
          Specific gate name to re-run. If omitted, all gates are re-run

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli worktree status`

```text
Show worktree status for a run.

Lists all worktree bindings associated with the run, including their state,
branch, path, and submodule mode.

Examples:
  yarli worktree status 019577a2-...
  yarli worktree status <run-id>

Usage: yarli worktree status [OPTIONS] <RUN_ID>

Arguments:
  <RUN_ID>
          Run ID to show worktree status for (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli worktree recover`

```text
Recover from an interrupted git operation in a worktree.

When a git operation (merge, rebase, cherry-pick) is interrupted (e.g. by a
crash or signal), this command lets you resolve the worktree state.

Recovery actions:
  abort         — abort the in-progress operation and reset to pre-operation state
  resume        — attempt to continue the interrupted operation
  manual-block  — mark the worktree as manually blocked for operator intervention

This is a policy-gated write operation and is audited.

Examples:
  yarli worktree recover 019577a4-... --action abort
  yarli worktree recover <worktree-id> -a resume
  yarli worktree recover <worktree-id> --action manual-block

Usage: yarli worktree recover [OPTIONS] <WORKTREE_ID>

Arguments:
  <WORKTREE_ID>
          Worktree ID to recover (UUID)

Options:
  -a, --action <ACTION>
          Recovery action to take
          
          [default: abort]

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli merge request`

```text
Request a new merge intent.

Creates a merge intent record that must be approved before execution.
The intent tracks the source and target refs, merge strategy, and
associated run.

Merge strategies:
  merge-no-ff    — create a merge commit even if fast-forward is possible (default)
  rebase-then-ff — rebase source onto target, then fast-forward
  squash-merge   — squash all source commits into a single merge commit

This is a policy-gated write operation and is audited.

Examples:
  yarli merge request feature/foo main --run-id 019577a2-...
  yarli merge request feature/bar develop --run-id <id> --strategy rebase-then-ff
  yarli merge request hotfix/fix main --run-id <id> --strategy squash-merge

Usage: yarli merge request [OPTIONS] --run-id <RUN_ID> <SOURCE> <TARGET>

Arguments:
  <SOURCE>
          Source branch or ref to merge from

  <TARGET>
          Target branch or ref to merge into

Options:
      --run-id <RUN_ID>
          Run ID this merge belongs to (UUID)

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --strategy <STRATEGY>
          Merge strategy to use
          
          [default: merge-no-ff]

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli merge approve`

```text
Approve a pending merge intent.

Transitions the merge intent from Pending to Approved, allowing the merge
orchestrator to execute it. This is a policy-gated write operation and is audited.

Examples:
  yarli merge approve 019577a5-...
  yarli merge approve <merge-id>

Usage: yarli merge approve [OPTIONS] <MERGE_ID>

Arguments:
  <MERGE_ID>
          Merge intent ID to approve (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli merge reject`

```text
Reject a pending merge intent.

Transitions the merge intent from Pending to Rejected with a reason.
This is a policy-gated write operation and is audited.

Examples:
  yarli merge reject 019577a5-... --reason "conflicts with release branch"
  yarli merge reject <merge-id> -r "not ready"

Usage: yarli merge reject [OPTIONS] <MERGE_ID>

Arguments:
  <MERGE_ID>
          Merge intent ID to reject (UUID)

Options:
  -r, --reason <REASON>
          Reason for rejection (recorded in audit log)
          
          [default: rejected]

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli merge status`

```text
Show status of a merge intent.

Displays the merge intent's state, source/target refs, strategy, and
associated run.

Examples:
  yarli merge status 019577a5-...
  yarli merge status <merge-id>

Usage: yarli merge status [OPTIONS] <MERGE_ID>

Arguments:
  <MERGE_ID>
          Merge intent ID to query (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli audit tail`

```text
Tail the JSONL audit log.

Reads the most recent entries from the audit log file and prints them.
Optionally filter by category.

Categories:
  policy_decision      — policy engine allow/deny decisions
  destructive_attempt   — blocked destructive git operations
  token_consumed        — LLM token usage records
  gate_evaluation       — gate pass/fail results

Examples:
  yarli audit tail
  yarli audit tail --lines 50
  yarli audit tail --category policy_decision
  yarli audit tail --file /var/log/yarli-audit.jsonl -l 100 -c gate_evaluation

Usage: yarli audit tail [OPTIONS]

Options:
  -f, --file <FILE>
          Path to the audit JSONL file
          
          [default: .yarl/audit.jsonl]

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

  -l, --lines <LINES>
          Number of most recent entries to show (0 = all)
          
          [default: 20]

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -c, --category <CATEGORY>
          Filter by category

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli audit query`

```text
Query the JSONL audit log.

Filter by run ID, task ID, category, actor, and time range.
Supports multiple output formats and pagination.

Categories:
  policy_decision      — policy engine allow/deny decisions
  destructive_attempt   — blocked destructive git operations
  token_consumed        — LLM token usage records
  gate_evaluation      — gate pass/fail results
  command_execution    — command execution terminal events

Output formats:
  table (default), json, csv

Examples:
  yarli audit query --category policy_decision
  yarli audit query --run-id 019577a5-... --format table
  yarli audit query --since 2026-02-20T00:00:00Z --before 2026-02-22T23:59:59Z
  yarli audit query --after 019577a5-... --limit 25 --format csv

Usage: yarli audit query [OPTIONS]

Options:
  -f, --file <FILE>
          Path to the audit JSONL file

          [default: .yarl/audit.jsonl]

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --run-id <RUN_ID>
          Filter by run ID

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

      --task-id <TASK_ID>
          Filter by task ID

  -c, --category <CATEGORY>
          Filter by category

      --actor <ACTOR>
          Filter by actor

      --since <SINCE>
          Include entries on or after this UTC timestamp (RFC 3339 or YYYY-MM-DD)

      --before <BEFORE>
          Include entries on or before this UTC timestamp (RFC 3339 or YYYY-MM-DD)

      --after <AFTER>
          Cursor-based pagination. Start after this audit ID

      --offset <OFFSET>
          Offset entries before returning the page

          [default: 0]

  -l, --limit <LIMIT>
          Max entries to return (0 = all)

          [default: 50]

  -F, --format <FORMAT>
          Output format

          [default: table]
          [possible values: table, json, csv]

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli plan tranche`

```text
Manage structured tranches in .yarli/tranches.toml.

Examples:
  yarli plan tranche add --key TP-05 --summary "Config loader"
  yarli plan tranche complete --key TP-05
  yarli plan tranche list
  yarli plan tranche remove --key TP-05

Usage: yarli plan tranche [OPTIONS] <COMMAND>

Commands:
  add       Add a new tranche definition
  complete  Mark a tranche as complete
  list      List all tranches
  remove    Remove a tranche definition
  help      Print this message or the help of the given subcommand(s)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli plan validate`

```text
Validate the structured tranches file (.yarli/tranches.toml).

Checks all tranche keys for validity, detects duplicates, and reports errors.

Examples:
  yarli plan validate

Usage: yarli plan validate [OPTIONS]

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli plan tranche add`

```text
Add a new tranche definition

Usage: yarli plan tranche add [OPTIONS] --key <KEY> --summary <SUMMARY>

Options:
  -k, --key <KEY>                      Tranche key (e.g. TP-05, M3A, CARD-001)
      --stream                         Force stream mode output (inline viewport, no fullscreen TUI)
  -s, --summary <SUMMARY>              Short summary describing the tranche objective
      --tui                            Force dashboard mode (fullscreen TUI with panel layout)
  -g, --group <GROUP>                  Optional tranche group for grouped dispatch
  -a, --allowed-paths <ALLOWED_PATHS>  Optional comma-separated allowed paths for scope enforcement
  -v, --verify <VERIFY>                Verification command to run after completion (e.g. "cargo test --offline query_cache")
  -d, --done-when <DONE_WHEN>          Done-criteria describing what constitutes completion
  -m, --max-tokens <MAX_TOKENS>        Per-tranche token budget override
  -h, --help                           Print help
```

## `target/debug/yarli plan tranche complete`

```text
Mark a tranche as complete

Usage: yarli plan tranche complete [OPTIONS] --key <KEY>

Options:
  -k, --key <KEY>  Tranche key to mark complete
      --stream     Force stream mode output (inline viewport, no fullscreen TUI)
      --tui        Force dashboard mode (fullscreen TUI with panel layout)
  -h, --help       Print help
```

## `target/debug/yarli plan tranche list`

```text
List all tranches

Usage: yarli plan tranche list [OPTIONS]

Options:
      --stream  Force stream mode output (inline viewport, no fullscreen TUI)
      --tui     Force dashboard mode (fullscreen TUI with panel layout)
  -h, --help    Print help
```

## `target/debug/yarli plan tranche remove`

```text
Remove a tranche definition

Usage: yarli plan tranche remove [OPTIONS] --key <KEY>

Options:
  -k, --key <KEY>  Tranche key to remove
      --stream     Force stream mode output (inline viewport, no fullscreen TUI)
      --tui        Force dashboard mode (fullscreen TUI with panel layout)
  -h, --help       Print help
```

## `target/debug/yarli debug queue-depth`

```text
Show live queue depth grouped by run and command class.

Useful to identify backlog growth or class-level saturation.

Examples:
  yarli debug queue-depth

Usage: yarli debug queue-depth [OPTIONS]

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli debug active-leases`

```text
Show currently leased tasks with owner and lease expiry.

Examples:
  yarli debug active-leases

Usage: yarli debug active-leases [OPTIONS]

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli debug resource-usage`

```text
Show current run-level resource usage totals and per-budget comparison.

Examples:
  yarli debug resource-usage 019577a2-...

Usage: yarli debug resource-usage [OPTIONS] <RUN_ID>

Arguments:
  <RUN_ID>
          Run ID (UUID)

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli migrate status`

```text
Show the migration status for the durable Postgres store.

This command reports each migration entry in code order and whether it is
currently applied in the target database.

Usage: yarli migrate status [OPTIONS]

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli migrate up`

```text
Apply pending migrations to the target Postgres database.

Use --target to stop at a specific migration version. If omitted, all pending
migrations are applied.

Usage: yarli migrate up [OPTIONS]

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

  -t, --target <TARGET>
          Stop after applying this migration (for example 0001 or 0001_init)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli migrate down`

```text
Revert migrations in reverse order to the target migration.

If --target is omitted, the system rolls back exactly one migration.
Before rollback, the command automatically creates a labeled backup snapshot
in `yarli_migration_backups` to support manual restore if needed.

Usage: yarli migrate down [OPTIONS]

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

  -t, --target <TARGET>
          Target checkpoint (for example 0001 or 0001_init). Defaults to one step back

  -b, --backup-label <BACKUP_LABEL>
          Optional backup label used for rollback restore. Defaults to timestamp

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli migrate backup`

```text
Create a full data snapshot of migration-related tables.

Backup snapshots are stored in `yarli_migration_backups.<label>` and can be
restored with `yarli migrate restore --label <label>`.

Usage: yarli migrate backup [OPTIONS]

Options:
  -l, --label <LABEL>
          Backup label (defaults to timestamp-based label)

      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

## `target/debug/yarli migrate restore`

```text
Restore known migration-related tables from a backup snapshot
captured by `yarli migrate backup`. This command only restores tables in the
snapshot schema and writes a warning when schemas differ.

Usage: yarli migrate restore [OPTIONS] <LABEL>

Arguments:
  <LABEL>
          Snapshot label to restore from

Options:
      --stream
          Force stream mode output (inline viewport, no fullscreen TUI)

      --tui
          Force dashboard mode (fullscreen TUI with panel layout)

  -h, --help
          Print help (see a summary with '-h')
```

