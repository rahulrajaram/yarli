# YARLI CLI Usage Guide

This is the exhaustive, command-by-command usage guide for `yarli`.

## Core Concepts

### Prompt Resolution Precedence

`yarli run` is opinionated:

- It resolves the prompt file in this order:
  1. `yarli run --prompt-file <path>`
  2. `[run].prompt_file` in `yarli.toml`
  3. fallback lookup for `PROMPT.md` by walking upward from your current directory
- Relative prompt paths from `--prompt-file` or `run.prompt_file` are resolved from repo root (`.git` ancestor) when available, otherwise from the config file directory.
- It expands any `@include <path>` directives (confined under the directory containing the resolved prompt file).
- Run-spec defaults can be configured in `yarli.toml` (`[run]`, `[[run.tasks]]`, `[[run.tranches]]`, `[run.plan_guard]`).
- If present, a fenced code block with info string `yarli-run` provides per-prompt run-spec overrides.
- It uses prompt context/objective plus `yarli.toml` runtime config for execution behavior.
- Default execution discovers incomplete tranches in `IMPLEMENTATION_PLAN.md`, dispatches each tranche as a task, then appends a verification task.
- Optional grouped dispatch is enabled with `[run].enable_plan_tranche_grouping = true` and `tranche_group=<name>` plan metadata.
- Optional tranche file-scope policy metadata is `allowed_paths=...` on plan lines; enable explicit scope instructions with `[run].enforce_plan_tranche_allowed_paths = true`.
- If no run-spec configuration exists and no incomplete tranches are found, it dispatches the full prompt text as one task.

Minimum `PROMPT.md` structure:

````markdown
# Project Prompt

@include IMPLEMENTATION_PLAN.md
````

For config-first mode, dispatch is configured in `yarli.toml` `[cli]` (`command`, `args`, `prompt_mode`, optional `env_unset`).
Prompt-embedded `[tasks]` remains legacy fallback compatibility (overrides `yarli.toml` by task key).

### Control Terminology

- Built-in Yarli policy gates: code-defined checks evaluated by Yarli (`yarli gate ...`).
- Verification command chain: plan/config/script-defined command execution (tranche + verification tasks).
- Observer mode: monitoring/reporting only; observer events never gate or mutate active run execution.
- Operator controls: explicit control-plane actions via `yarli run pause|resume|cancel`.

Optional plan guard (recommended for tranche/card workflows):

```toml
[run.plan_guard]
target = "CARD-R8-01"
mode = "implement"   # "implement" (default) or "verify-only"
```

- `mode = "implement"` validates target presence/state and allows completed targets to proceed via verification-only routing.
- `mode = "verify-only"` requires the target to already be complete and requires verification-oriented objective text.
- `target` matching supports checklist lines and common status formats such as `I8B ... complete/incomplete`.

### Runtime Config: `yarli.toml`

By default, `yarli` loads runtime configuration from `yarli.toml` in the current working directory.

If `yarli.toml` is missing, defaults are used. Defaults are intentionally conservative:

- `core.backend = "in-memory"` and `core.allow_in_memory_writes = false`
- This means write commands (including starting runs) are blocked unless you explicitly opt into ephemeral writes or configure Postgres.

Bootstrap a config with `yarli init`.

### Memories (Memory-backend)

YARLI can store and query "memories" (short, reusable incident summaries) using Memory-backend.

What gets stored (high level):

- Failures (`task.failed`) as short semantic memories scoped to `project/<project_id>`
- Query-based hints:
  - at run start (`run.observer.memory_hints`)
  - on task failure/block (`task.observer.memory_hints`)

Enable Backend-backed memories in `yarli.toml`:

```toml
[memory]
# Optional master switch. If unset, `[memory.backend].enabled` is the effective toggle.
# enabled = true
# project_id = "project"

[memory.backend]
enabled = true
command = "memory-backend"
# project_dir = "."            # defaults to the directory containing PROMPT.md
query_limit = 8
inject_on_run_start = true
inject_on_failure = true
```

Memory backend bootstrap (per repo):

```bash
memory-backend init -y
```

### Output Modes

Global flags:

- `--stream`: force stream output (inline, no fullscreen UI)
- `--tui`: force fullscreen dashboard UI

If `--stream` is requested but the current environment is not a TTY, YARLI will fall back to headless mode.

## Command Reference

### `yarli init`

Purpose:

- Creates a documented `yarli.toml` template to bootstrap a workspace.
- This is where you initialize durability (Postgres vs ephemeral), execution backend (native vs Overwatch), budgets, policy mode, and UI mode.

Common examples:

```bash
# Create yarli.toml in the current directory.
yarli init

# Print the template (for review) without writing.
yarli init --print

# Write to a different path.
yarli init --path ./config/yarli.toml

# Overwrite an existing file.
yarli init --force
```

Durability examples:

```bash
# 1) Quick local try (ephemeral writes):
# Edit yarli.toml:
# [core]
# backend = "in-memory"
# allow_in_memory_writes = true
#
# Then:
yarli run --stream

# 2) Durable local dev (Postgres):
# Edit yarli.toml:
# [core]
# backend = "postgres"
#
# [postgres]
# database_url = "postgres://postgres:postgres@localhost:5432/yarli"
#
# Apply migrations (see docs/OPERATIONS.md), then:
yarli run --stream
```

Execution backend examples:

```bash
# Keep the default native process runner:
# [execution]
# runner = "native"

# Use the Overwatch service runner (opt-in):
# [execution]
# runner = "overwatch"
#
# [execution.overwatch]
# service_url = "http://127.0.0.1:8089"
```

LLM CLI backend templates (mirrors `orchestrator.yml` conventions):

```bash
# Emit a Codex-flavored template section.
yarli init --backend codex

# Print a Claude-flavored template section without writing.
yarli init --backend claude --print

# Emit a Gemini-flavored template to a specific path.
yarli init --backend gemini --path ./yarli.toml --force
```

Notes:

- `yarli run` uses `[cli]` as the primary dispatch backend in config-first mode.
- The template is modeled after `orchestrator.yml` to keep migration straightforward.

Orchestrator-orchestrator config mapping reference:

- See `orchestrator.yml` in this repository.
- Rough mapping (conceptual):
  - `orchestrator.yml: event_loop.prompt_file` -> `yarli.toml: [run].prompt_file` (or `--prompt-file` override)
  - `orchestrator.yml: cli.backend/prompt_mode/command/args` -> `yarli.toml: [cli] backend/prompt_mode/command/args`
  - `orchestrator.yml: features.parallel` -> `yarli.toml: [features] parallel` + `[execution] worktree_root` (parallel per-task workspaces)

### `yarli run`

Purpose:

- Start, monitor, and explain orchestration runs.
- `yarli run` (no subcommand) is config-first and plan-driven (primary workflow).
- `yarli run` is the authoritative execution entry point; observers are telemetry-only.
- Parallel mode defaults to enabled (`[features].parallel = true`).
- Parallel mode requires `[execution].worktree_root`; otherwise `yarli run` fails fast and asks you to update `yarli.toml`.
- In parallel mode, YARLI prepares one workspace copy per task under `execution.worktree_root` (`~` and `$ENV_VAR` tokens are expanded for execution paths).
- Configure workspace copy exclusions with `[execution].worktree_exclude_paths` (for example: `["target", "node_modules", ".venv", "venv", "__pycache__"]`).
- On `RunCompleted`, YARLI scopes each task merge to paths that actually differ from the source workspace, then applies a patch with `git apply --3way`.
- If any workspace merge conflicts, YARLI records `run.parallel_merge_failed`, returns a non-zero exit, preserves the run workspace root, and writes `PARALLEL_MERGE_RECOVERY.txt` with deterministic recovery commands.
- Auto-advance policy is configured with:
  - `[run] auto_advance_policy = "improving-only" | "stable-ok" | "always"` (default: `stable-ok`)
  - `[run] max_auto_advance_tranches = <N>` (`0` = unlimited)
- Grouped tranche dispatch is configured with:
  - `[run] enable_plan_tranche_grouping = true|false`
  - `[run] max_grouped_tasks_per_tranche = <N>` (`0` = unlimited)
  - `IMPLEMENTATION_PLAN.md` metadata: append `tranche_group=<name>` to related open lines.
- Optional tranche file-scope policy is configured with:
  - `[run] enforce_plan_tranche_allowed_paths = true|false`
  - `IMPLEMENTATION_PLAN.md` metadata: append `allowed_paths=path/a,path/b` to tranche lines.
- Optional run-spec task catalog in config:
  - `[[run.tasks]]` entries (`key`, `cmd`, optional `class`)
  - `[[run.tranches]]` entries (`key`, optional `objective`, `task_keys`)
  - `[run.plan_guard]` for target/mode plan contract
- Operator controls are available for live runs:
  - `yarli run pause [<run-id>|--all-active]`
  - `yarli run resume [<run-id>|--all-paused]`
  - `yarli run cancel [<run-id>|--all-active]`
- Long-running execution emits heartbeat progress events (`run.observer.progress`) so stream output does not go silent for minutes.
- `run.allow_stable_auto_advance` remains as a legacy compatibility toggle.
- If continuation is not yet published when `yarli run continue` is invoked, configure wait behavior with:
  - `[run] continue_wait_timeout_seconds = <seconds>` (default `0` = fail fast).
- If `[run.plan_guard]` (or prompt override plan guard) is set in the effective run spec, `yarli run` performs a preflight plan/prompt consistency check against `IMPLEMENTATION_PLAN.md` before dispatching tasks.

Examples:

```bash
# Run the workspace's default prompt-defined loop.
yarli run --stream

# Override the prompt file for this invocation.
yarli run --prompt-file prompts/I8C.md --stream

# Start an ad-hoc run with explicit commands (one task per --cmd).
yarli run start "verify" --cmd "cargo fmt --all" --cmd "cargo test --workspace" --stream

# Query status.
yarli run status <run-id>

# Explain why the run exited (or why it is not done).
yarli run explain-exit <run-id>

# Pause one active run or all active/verifying runs.
yarli run pause <run-id>
yarli run pause --all-active --reason "maintenance window"

# Resume one paused run or all paused runs.
yarli run resume <run-id>
yarli run resume --all-paused --reason "maintenance complete"

# Cancel one run or all active/verifying runs.
yarli run cancel <run-id>
yarli run cancel --all-active --reason "operator stop"

# Continue from the latest persisted continuation payload
# (event store first, `.yarli/continuation.json` fallback).
yarli run continue
```

Notes:

- For `yarli run status` and `yarli run explain-exit`, `<run-id>` can be either a full UUID or a unique short prefix from `yarli run list`.
- `yarli run status` includes tranche mapping (`tranche -> group -> task_key -> task_id -> worker_actor`) plus scope metadata when available.
- `ui.verbose_output` only controls command-output verbosity in stream mode; it does not enable or disable cancellation provenance capture.
- Cancellation provenance (`signal`, PID context, actor/stage attribution) is emitted in run/task cancel events and persisted in `run.continuation` payloads.
- Provenance inspection durability depends on backend:
  - `core.backend = "postgres"` persists provenance across process restarts.
  - `core.backend = "in-memory"` is ephemeral; `run status` / `run explain-exit` only reflect currently running process state unless `.yarli/continuation.json` is still present.

Legacy prompt-run-spec tranche example (`PROMPT.md` `yarli-run` block):

```toml
version = 1
objective = "workspace verification"

[tasks]
items = [
  { key = "fmt", cmd = "cargo fmt --all -- --check", class = "io" },
  { key = "lint", cmd = "cargo clippy --workspace -- -D warnings", class = "cpu" },
  { key = "test", cmd = "cargo test --workspace", class = "cpu" },
]

[tranches]
items = [
  { key = "fast", task_keys = ["fmt", "lint"] },
  { key = "full", objective = "full verification", task_keys = ["test"] },
]
```

Legacy compatibility:

- `yarli run batch` exists for backward compatibility and can use `[run]` paces in `yarli.toml`.
- The primary workflow is `yarli run` with `PROMPT.md`.

### `yarli task`

Purpose:

- Inspect tasks and clear blockers.

Examples:

```bash
# List tasks for a run.
yarli task list <run-id>

# Explain a specific task (Why Not Done?).
yarli task explain <task-id>

# Unblock a task (write command; requires durable backend or explicit ephemeral override).
yarli task unblock <task-id> --reason "rechecked dependency"
```

### `yarli gate`

Purpose:

- Inspect configured gates and manually re-run gate evaluation.

Examples:

```bash
# List task-level gates.
yarli gate list

# List run-level gates.
yarli gate list --run

# Re-run all gates for a task.
yarli gate rerun <task-id>

# Re-run a single named gate.
yarli gate rerun <task-id> --gate tests_passed
```

### `yarli worktree`

Purpose:

- Inspect and recover git worktree state for a run.

Examples:

```bash
# Show worktree status for a run.
yarli worktree status <run-id>

# Recover from an interrupted operation in a worktree.
# action values: abort | resume | manual-block
yarli worktree recover <worktree-id> --action abort
```

### `yarli merge`

Purpose:

- Request, approve, reject, and inspect merge intents.

Examples:

```bash
# Request a merge intent associated with a run.
# strategy values: merge-no-ff | rebase-then-ff | squash-merge
yarli merge request feature-branch main --run-id <run-id> --strategy merge-no-ff

# Approve a merge intent.
yarli merge approve <merge-id>

# Reject a merge intent.
yarli merge reject <merge-id> --reason "tests failing"

# Show merge intent status.
yarli merge status <merge-id>
```

### `yarli audit`

Purpose:

- Tail the JSONL audit log emitted by policy decisions and governance accounting.

Examples:

```bash
# Tail the last 20 entries (default path: .yarl/audit.jsonl).
yarli audit tail

# Tail a specific file and show more lines.
yarli audit tail --file .yarl/audit.jsonl --lines 200

# Filter by category.
yarli audit tail --category policy_decision
```

### `yarli info`

Purpose:

- Print version and detected terminal capabilities.

Example:

```bash
yarli info
```

## Troubleshooting

### "write commands blocked" in in-memory mode

If you see errors indicating in-memory writes are blocked, either:

1. Configure durable mode:
   - `core.backend = "postgres"`
   - `postgres.database_url = "..."` and apply migrations
2. Or explicitly opt into ephemeral writes (local throwaway usage only):
   - `core.backend = "in-memory"`
   - `core.allow_in_memory_writes = true`

### Stream requested but "headless mode"

If you run `yarli run --stream` in a non-interactive environment (no TTY),
YARLI will fall back to headless mode and still execute the run.
