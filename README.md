<p align="center">
  <img src="assets/logo.png" alt="YARLI" width="360">
</p>

<p align="center">
  Deterministic run/task orchestration with event sourcing, queue scheduling, and safe Git controls.
</p>

# YARLI

## What is this?

YARLI is a Rust project for executing plan-driven engineering workflows with durable state and explicit operator control. It treats runs, tasks, worktrees, merges, command execution, and policy decisions as state-machine entities persisted through an event log.

The primary workflow is `yarli run`: resolve prompt context, discover open tranches, dispatch execution tasks, and emit explainable run/task status. YARLI supports both local ephemeral development and durable Postgres-backed operation.

This project is for teams that want reproducible orchestration behavior, auditable transitions, and safe defaults around Git operations, policy checks, and runtime controls.

## Features

- Config-first orchestration (`yarli.toml`) with plan-driven tranche dispatch.
- Durable event store and queue backends (Postgres) with in-memory mode for local testing.
- Explicit operator controls (`pause`, `resume`, `cancel`, `drain`) and explainability commands.
- Policy and gate evaluation over task/run state.
- Parallel workspace execution with scoped merge and recovery artifacts.
- Startup salvage of abandoned git-worktree run roots before parallel slot allocation.
- Optional memory provider adapters via `[memory.providers.<name>]`.
- REST API crate (`yarli-api`) with health, status, control, webhook, and websocket event routes.

## Installation

### Build from source

```bash
cargo build --release
./target/release/yarli --help
```

### Cargo install (local path)

```bash
cargo install --path . --bin yarli --locked
yarli --help
```

### Shared commithooks

This repo tracks project-local hooks in `.githooks/`. Building `yarli`
refreshes the shared dispatchers and hook library into `.git/hooks/` and
`.git/lib/` when `COMMITHOOKS_DIR` or `$HOME/Documents/commithooks` is
available.

### Install script

```bash
./install.sh
~/.local/bin/yarli --version
```

### Docker

```bash
docker build -t yarli:local .
docker run --rm yarli:local --help
```

## Quick Start

```bash
# 1) Generate config template
yarli init

# 2) For local ephemeral writes, set in yarli.toml:
# [core]
# backend = "in-memory"
# allow_in_memory_writes = true

# 3) Ensure PROMPT.md exists, then run
yarli run --stream
```

For durable mode, configure:

```toml
[core]
backend = "postgres"

[postgres]
database_url = "postgres://postgres:postgres@localhost:5432/yarli"
```

Then apply migrations:

```bash
yarli migrate up
```

`in-memory` mode is ephemeral: once the process exits, captured task output and
audit history do not survive. In durable Postgres mode, YARLI persists run/task
state plus captured command output so operators and agents can inspect failures
later with `yarli run explain-exit`, `yarli task explain`, `yarli task output`,
and `yarli audit`.

## Core Concepts

### Prompt Resolution

`yarli run` resolves the prompt file in this order:

1. `yarli run --prompt-file <path>`
2. `[run].prompt_file` in `yarli.toml`
3. Fallback lookup for `PROMPT.md` by walking upward from your current directory

Relative prompt paths are resolved from repo root (`.git` ancestor) when available, otherwise from the config file directory. `@include <path>` directives are expanded (confined under the directory containing the resolved prompt file).

Default execution discovers incomplete tranches in `IMPLEMENTATION_PLAN.md`, dispatches each tranche as a task, then appends a verification task. If no run-spec configuration exists and no incomplete tranches are found, it dispatches the full prompt text as one task.

Minimum `PROMPT.md` structure:

````markdown
# Project Prompt

@include IMPLEMENTATION_PLAN.md
````

### Tranche Metadata

Optional metadata on `IMPLEMENTATION_PLAN.md` tranche lines:

- `tranche_group=<name>` — group adjacent entries for dispatch together
- `allowed_paths=path/a,path/b` — scope constraints (requires `[run].enforce_plan_tranche_allowed_paths = true`)
- `verify="cmd"` — post-tranche verification command hint
- `done_when="criteria"` — explicit completion contract
- `max_tokens=N` — per-tranche token-budget hint

### Control Terminology

- **Policy gates**: code-defined checks evaluated by YARLI (`yarli gate ...`).
- **Verification command chain**: plan/config/script-defined command execution (tranche + verification tasks).
- **Observer mode**: monitoring/reporting only; observer events never gate or mutate active run execution.
- **Operator controls**: explicit control-plane actions via `yarli run pause|resume|cancel|drain`.

### Output Modes

Global flags:

- `--stream`: force stream output (inline, no fullscreen UI)
- `--tui`: force fullscreen dashboard UI

If `--stream` is requested but the current environment is not a TTY, YARLI falls back to headless mode.

## CLI Reference

### `yarli init`

Creates a documented `yarli.toml` template to bootstrap a workspace. This is where you initialize durability (Postgres vs ephemeral), execution backend (native vs Overwatch), budgets, policy mode, and UI mode.

```bash
yarli init                                    # Create yarli.toml
yarli init --print                            # Print template without writing
yarli init --path ./config/yarli.toml         # Write to a different path
yarli init --force                            # Overwrite existing file
yarli init --backend codex                    # Codex-flavored template
yarli init --backend claude --print           # Claude-flavored template (preview)
yarli init --backend gemini --path ./yarli.toml --force
yarli init --backend kiro-cli                 # Kiro CLI-flavored template
```

### `yarli run`

Start, monitor, and explain orchestration runs. `yarli run` (no subcommand) is the primary config-first, plan-driven workflow.

- Parallel mode defaults to enabled (`[features].parallel = true`); requires `[execution].worktree_root`.
- In parallel mode, YARLI prepares one workspace per task under `execution.worktree_root`.
- Before creating new git-worktree task slots, YARLI sweeps `execution.worktree_root` for abandoned `run-*` roots, salvages dirty worktrees onto durable branches, and reclaims empty stale roots.
- On `RunCompleted`, YARLI scopes each task merge to paths that actually differ, then applies a patch with `git apply --3way`.
- Merge conflict resolution is controlled by `[run].merge_conflict_resolution`: `fail` (default), `manual`, `auto-repair`.
- Auto-advance policy: `[run] auto_advance_policy = "improving-only" | "stable-ok" | "always"` (default: `stable-ok`).
- Task health trends (`[run.task_health.improving|stable|deteriorating]`) can trigger `continue`, `checkpoint-now`, `force-pivot`, or `stop-and-summarize`.
- `[run].soft_token_cap_ratio` triggers checkpoint-now when total token usage reaches `ratio * max_run_total_tokens` (default: `0.9`).
- `yarli run continue` replays the prior continuation snapshot. If current `.yarli/tranches.toml` has newer open tranche keys, it refuses and tells you to use `yarli run --fresh-from-tranches`.
- `yarli run --fresh-from-tranches` explicitly rebuilds from current prompt/plan/tranches state and ignores continuation as an execution source.
- Use `yarli run --allow-recursive-run ...` when a task command intentionally needs to invoke nested `yarli run` execution for that invocation.

```bash
# Run the workspace's default prompt-defined loop.
yarli run --stream

# Make the live plan/tranches rebuild explicit.
yarli run --fresh-from-tranches --stream

# Override the prompt file for this invocation.
yarli run --prompt-file prompts/I8C.md --stream

# Start an ad-hoc run with explicit commands (one task per --cmd).
yarli run start "verify" --cmd "cargo fmt --all" --cmd "cargo test --workspace" --stream

# Start using a named pace from yarli.toml.
yarli run start "deploy" --pace deploy -w /opt/app --timeout 600

# Query status (run-id can be a full UUID or unique prefix from `yarli run list`).
yarli run status <run-id>

# Explain why the run exited (or why it is not done).
yarli run explain-exit <run-id>

# List all runs.
yarli run list

# Operator controls.
yarli run pause <run-id> --reason "maintenance window"
yarli run pause --all-active --reason "maintenance window"
yarli run resume <run-id> --reason "maintenance complete"
yarli run resume --all-paused --reason "maintenance complete"
yarli run drain <run-id> --reason "stop after current"
yarli run drain --all-active --reason "stop after current"
yarli run cancel <run-id> --reason "operator stop"
yarli run cancel --all-active --reason "operator stop"

# Continue from the latest persisted continuation payload.
yarli run continue
yarli run continue --file .yarli/continuation.json

# Rebuild from the current prompt/plan/tranches state instead of replaying continuation.
yarli run --fresh-from-tranches

# Legacy pace-based execution.
yarli run batch --pace ci
yarli run batch --objective "nightly check" -w /repo --timeout 900
```

Feature-gated sw4rm agent mode:

```bash
cargo run --features sw4rm -- run sw4rm --help
yarli run sw4rm
YARLI_LOG=debug yarli run sw4rm
```

`yarli run sw4rm` requires a build with `--features sw4rm` and reads its runtime configuration from `[sw4rm]` in `yarli.toml`.

Plan guard (recommended for tranche/card workflows):

```toml
[run.plan_guard]
target = "CARD-R8-01"
mode = "implement"   # "implement" (default) or "verify-only"
```

### `yarli task`

Inspect tasks and clear blockers.

Use `yarli task output <task-id>` to retrieve the raw stdout/stderr captured for
that task's command execution. This retrieval path depends on durable persisted
output events, so it is most useful in Postgres mode.

```bash
yarli task list <run-id>
yarli task explain <task-id>
yarli task unblock <task-id> --reason "rechecked dependency"
yarli task annotate <task-id> --detail "see blocker-001.md"
yarli task output <task-id>
```

### `yarli gate`

Inspect configured gates and manually re-run gate evaluation.

```bash
yarli gate list                              # Task-level gates
yarli gate list --run                        # Run-level gates
yarli gate rerun <task-id>                   # Re-run all gates
yarli gate rerun <task-id> --gate tests_passed
```

### `yarli worktree`

Inspect and recover git worktree state for a run.

```bash
yarli worktree status <run-id>
# action values: abort | resume | manual-block
yarli worktree recover <worktree-id> --action abort
```

### `yarli merge`

Request, approve, reject, and inspect merge intents.

```bash
# strategy values: merge-no-ff | rebase-then-ff | squash-merge
yarli merge request feature-branch main --run-id <run-id> --strategy merge-no-ff
yarli merge approve <merge-id>
yarli merge reject <merge-id> --reason "tests failing"
yarli merge status <merge-id>
```

### `yarli audit`

Tail and query the JSONL audit log emitted by policy decisions, governance accounting, and command execution events.

Use audit history alongside `yarli task output`: task output shows what the
command printed, while audit entries show the surrounding policy, gate, and
operator-decision trail. By default `yarli audit tail` reads the local
`.yarl/audit.jsonl` file.

```bash
yarli audit tail
yarli audit tail --file .yarl/audit.jsonl --lines 200
yarli audit tail --category policy_decision

yarli audit query --run-id <run-id> --task-id <task-id>
yarli audit query --category gate_evaluation --since 2026-02-20T00:00:00Z --before 2026-02-22T23:59:59Z
yarli audit query --category policy_decision --format csv --limit 50
```

Quick diagnosis workflow for a failed durable run:

```bash
# 1) Get the summary of why the run exited or stopped.
yarli run explain-exit <run-id>

# 2) Inspect the failing task's raw captured stdout/stderr.
yarli task output <task-id>

# 3) Query the event and policy trail around that task.
yarli audit query --task-id <task-id> --category gate_evaluation
```

### `yarli evidence`

Validate structured tranche evidence files in `.yarli/evidence/`. Checks TOML frontmatter, filename-to-tranche matching, required markdown headings, and metadata consistency.

```bash
yarli evidence validate
yarli evidence validate --path .yarli/evidence/IWIRE-GATE-01.md
```

### `yarli plan`

Manage the structured tranches file (`.yarli/tranches.toml`) used by plan-driven dispatch.

```bash
yarli plan validate
yarli plan tranche list
yarli plan tranche add --key TP-05 --summary "Config loader hardening"
yarli plan tranche add --key TP-05 --summary "Config loader hardening" --idempotent
yarli plan tranche add --key TP-06 --summary "Guard runner" --group runtime --allowed-paths crates/yarli-exec,crates/yarli-cli --verify "cargo test -p yarli" --done-when "guard diagnostics render in run status" --max-tokens 60000
yarli plan tranche complete --key TP-05
yarli plan tranche remove --key TP-05
```

`yarli plan tranche add` also supports tranche metadata fields used by plan-driven dispatch: `--group`, `--allowed-paths`, `--verify`, `--done-when`, and `--max-tokens`. Add `--idempotent` when an agent or script may enqueue the same tranche key repeatedly and you want matching definitions to no-op instead of failing.

### `yarli debug`

Inspect live scheduler internals and queue/runtime telemetry.

```bash
yarli debug queue-depth
yarli debug active-leases
yarli debug resource-usage <run-id>
```

### `yarli migrate`

Manage Postgres migration lifecycle for durable mode.

```bash
yarli migrate status
yarli migrate up
yarli migrate up --target 0001_init
yarli migrate down
yarli migrate down --target 0001_init --backup-label rollback_20260224
yarli migrate backup --label pre_release
yarli migrate restore pre_release
```

### `yarli serve`

Start the HTTP API server for run, task, and audit introspection.

```bash
yarli serve
yarli serve --bind 0.0.0.0 --port 8080
```

### `yarli info`

Print version and detected terminal capabilities.

```bash
yarli info
```

## AI Agent Integration

YARLI integrates with agent CLIs through `[cli]` configuration (for example `codex`, `claude`, `gemini`, `kiro-cli`, or custom command wiring).

```toml
[cli]
backend = "codex"
prompt_mode = "arg"
command = "codex"
args = ["exec"]
```

Kiro CLI example (`kiro-cli` is Claude under the hood):

```toml
[cli]
backend = "kiro-cli"
prompt_mode = "arg"
command = "kiro-cli"
args = ["chat", "--no-interactive", "--trust-all-tools"]
```

YARLI is not currently an MCP server. Agent integration is command-dispatch based.

## gRPC API

YARLI does not currently expose a native gRPC API. I did not find any `.proto`
files in this repo, and the supported network API surface today is the HTTP
server from `crates/yarli-api`.

## REST API

Routes from `crates/yarli-api/src/server.rs`:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |
| GET | `/v1/events/ws` | WebSocket event stream |
| POST | `/v1/webhooks` | Register webhook |
| POST | `/v1/runs` | Create run |
| GET | `/v1/runs` | List runs |
| GET | `/v1/runs/:run_id/status` | Run status |
| POST | `/v1/runs/:run_id/pause` | Pause run |
| POST | `/v1/runs/:run_id/resume` | Resume run |
| POST | `/v1/runs/:run_id/cancel` | Cancel run |
| GET | `/v1/tasks` | List tasks |
| GET | `/v1/tasks/:task_id` | Task status |
| POST | `/v1/tasks/:task_id/annotate` | Annotate task |
| POST | `/v1/tasks/:task_id/unblock` | Unblock task |
| POST | `/v1/tasks/:task_id/retry` | Retry task |
| GET | `/v1/audit` | Audit log |
| GET | `/v1/metrics/reporting` | Reporting metrics |

Debug-only routes (feature-gated: `debug-api`):

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/tasks/:task_id/priority` | Override priority |
| GET | `/debug/queue-depth` | Queue depth |
| GET | `/debug/active-leases` | Active leases |
| GET | `/debug/resource-usage/:run_id` | Resource usage |

Authentication is enabled when `YARLI_API_KEYS` is set. Public endpoints (`/health`, `/metrics`) bypass auth.

```bash
# Public endpoint
curl -sS http://127.0.0.1:3000/health

# Non-public endpoint when auth is disabled
curl -sS http://127.0.0.1:3000/v1/runs

# Non-public endpoint when auth is enabled
curl -sS -H "Authorization: Bearer <key>" http://127.0.0.1:3000/v1/runs/<run-id>/status
```

## Configuration

### `yarli.toml`

Main config file, generated via `yarli init`. Example in `yarli.example.toml`. Run `yarli init --help` for a full annotated list of every config key and its default.

Key sections:

| Section | Purpose |
|---------|---------|
| `[core]` | Backend (`in-memory`/`postgres`), safe mode, worker identity |
| `[postgres]` | Database URL, URL file (Kubernetes secret pattern) |
| `[cli]` | LLM CLI backend (`codex`/`claude`/`gemini`/`kiro-cli`/custom), prompt mode, command, args |
| `[features]` | Parallel execution, git worktree mode |
| `[queue]` | Claim batch size, lease TTL, per-class caps |
| `[execution]` | Runner (`native`/`overwatch`), working dir, worktree root, timeouts |
| `[run]` | Prompt file, auto-advance policy, task health, merge conflict resolution, plan guard |
| `[budgets]` | Per-task and per-run resource limits (RSS, CPU, IO, tokens) |
| `[git]` | Default target branch, destructive operation deny |
| `[policy]` | Policy enforcement, audit toggle |
| `[memory]` | Provider selection and adapter config |
| `[observability]` | Audit file path, log level |
| `[ui]` | Display mode, verbose output, cancellation diagnostics |
| `[sw4rm]` | Agent name, capabilities, registry/router/scheduler URLs (requires `--features sw4rm`) |

### Memory Providers

YARLI can store and query memories (short, reusable incident summaries) via provider adapters.

```toml
[memory]
provider = "default"

[memory.providers.default]
type = "cli"
enabled = true
command = "memory-backend"
query_limit = 8
inject_on_run_start = true
inject_on_failure = true
```

Bootstrap per repo: `memory-backend init -y`

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Postgres DSN fallback |
| `YARLI_LOG` | Log level (populates `RUST_LOG` when unset) |
| `YARLI_ALLOW_RECURSIVE_RUN` | Recursive-run override |
| `YARLI_API_KEYS` | Comma-separated API auth keys |
| `YARLI_API_RATE_LIMIT_PER_MINUTE` | API rate limit (default: 120) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP tracing exporter endpoint |
| `YARLI_TEST_DATABASE_URL` | Postgres URL for integration tests |
| `YARLI_REQUIRE_POSTGRES_TESTS` | Enable Postgres integration tests |

## Architecture

```text
                +---------------------+
                |  yarli CLI (run/*)  |
                +----------+----------+
                           |
                           v
                +---------------------+
                | Scheduler + Queue   |
                | (yarli-queue)       |
                +----------+----------+
                           |
        +------------------+------------------+
        v                                     v
+---------------+                    +------------------+
| Command Runner|                    | Policy + Gates   |
| (yarli-exec)  |                    | (yarli-policy,   |
+-------+-------+                    |  yarli-gates)    |
        |                            +--------+---------+
        v                                     |
+---------------+                             |
| Git/Worktree  |<----------------------------+
| (yarli-git)   |
+-------+-------+
        |
        v
+---------------+
| Event Store   |
| (in-memory or |
| Postgres)     |
+-------+-------+
        |
        v
+---------------+
| API / Observe |
| (yarli-api)   |
+---------------+
```

Key implementation modules and integration tests still live under `crates/yarli-*`, but Cargo now builds through the single root package `yarli`.

## Security

- Safe mode and policy enforcement are configurable (`observe`, `restricted`, `execute`, `breakglass`).
- Destructive Git operations are denied by default (`git.destructive_default_deny = true`).
- API authentication is enabled when `YARLI_API_KEYS` is set (supports `Authorization: Bearer`, `x-api-key`, `api-key` headers).
- API key rate limiting uses `YARLI_API_RATE_LIMIT_PER_MINUTE` (default 120/min).
- Durable mode is recommended for write commands; in-memory mode blocks writes unless explicitly allowed.

## Runner Hardening and sw4rm Notes

- `yarli run` now wires per-task scheduler budgets into command execution as `ResourceLimits` and applies Linux `setrlimit(2)` caps for memory, CPU, open files, and process count before spawning workers.
- Prometheus now exposes `yarli_enforcement_outcomes_total{mechanism,outcome,reason}` for runner hardening paths, including `rlimit`, `cgroup`, `pidfd`, and `pid_termination` outcomes plus `rlimits_only_*` fallback reasons.
- `sw4rm` agent runtime transport uses a correlation-aware gRPC sender/receiver (`GrpcRouterSender`) and `CT_IMPLEMENTATION_RESPONSE` now maps back to pending local correlation state.
- `yarli run sw4rm` fails fast when router/report clients or runtime registration cannot connect; it does not silently degrade into a partial local mode.
- Invalid sw4rm verification run-spec configuration falls back to minimal verification defaults, while correlation drops from shutdown/stream cancellation surface as `Cancelled` and request expiry surfaces as `LlmTimeout`.
- Process lifecycle control prefers pidfd (`pidfd_open(2)` / `pidfd_send_signal(2)`) when available, with automatic fallback for older kernels.
- A cgroup v2 sandbox is enabled when writable; when unavailable or unwritable, execution continues in fallback mode using rlimits only.
- Containerized cgroup enforcement requires a writable delegated cgroup subtree for the YARLI process. Read-only or non-delegated hierarchies are supported, but they stay in rlimits-only fallback mode.
- `/proc/self/cgroup` is used in integration probes to confirm sandbox attachment under writable cgroup v2 runs.
- Build `yarli` with `--features sw4rm` when running the sw4rm runtime (`yarli run sw4rm`), plus compatible gRPC endpoints.

## Examples

```bash
# Bootstrap config, then run the default plan-driven loop.
yarli init
yarli run --stream

# Start an ad-hoc run with explicit commands.
yarli run start "full check" -c "cargo fmt --check" -c "cargo test"

# Inspect or control a live run.
yarli run status <run-id>
yarli run drain <run-id> --reason "stop after current"

# Recover interrupted worktree state.
yarli worktree status <run-id>
yarli worktree recover <worktree-id> --action abort
```

## Playbooks

### Runtime Guard Response

When guard-related telemetry indicates a stop or pivot condition:

1. `yarli run status <run-id>` — inspect budget and deterioration signals.
2. `yarli run explain-exit <run-id>` — inspect guard breach reasons and trend summary.
3. `yarli task explain <task-id>` — inspect task-level budget, usage, and failure provenance.
4. `yarli run pause|resume|cancel|drain <run-id>` — hold, stop, or finish-current-then-exit while adjusting scope.
5. `yarli run continue` only when the continuation snapshot still matches current open tranches; otherwise use `yarli run --fresh-from-tranches`.

### Merge Conflict Recovery

When `run.status` or `run.explain-exit` show unresolved merge state:

1. Inspect: `yarli run status <run-id>` and `yarli audit tail --category merge.apply.conflict`.
2. Open `PARALLEL_MERGE_RECOVERY.txt` in the preserved workspace root.
3. Follow the printed recovery steps (inspect, review patch, retry apply, resolve markers).
4. Pick action based on `[run].merge_conflict_resolution` policy (`fail`, `manual`, `auto-repair`).
5. Use `yarli run continue` if the snapshot still owns the remaining work, or `yarli run --fresh-from-tranches` if you queued new tranches during remediation.

## Troubleshooting

### "write commands blocked" in in-memory mode

Either configure durable mode (`core.backend = "postgres"` + `DATABASE_URL` or `[postgres].database_url`) and apply migrations, or explicitly opt into ephemeral writes:

```toml
[core]
backend = "in-memory"
allow_in_memory_writes = true
```

### Secret rotation

1. Mount credentials as files in a dedicated secret volume (e.g. `/run/secrets/...`).
2. Set `postgres.database_url_file` to that mounted path in `yarli.toml`.
3. Update the secret, then restart YARLI (`kubectl rollout restart ...`).

### Stream requested but "headless mode"

If you run `yarli run --stream` in a non-interactive environment (no TTY), YARLI falls back to headless mode and still executes the run.

## License

Dual licensed under `MIT OR Apache-2.0`.
