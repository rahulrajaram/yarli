# YARLI Operations

This runbook covers the baseline local operator workflow for durable mode, migrations, and test execution.
For final verification and release acceptance decisions, use `docs/ACCEPTANCE_RUBRIC.md`.

For an exhaustive, command-by-command CLI usage guide, see `docs/CLI.md`.
For memory behavior (when YARLI stores/queries memories), see `docs/MEMORY_POLICY.md`.

## Requirements

- Rust toolchain compatible with workspace `rust-version` (currently 1.75+).
- PostgreSQL accessible from the local machine.
- `psql` client for manual migration execution.

## Required Environment Variables

- `YARLI_TEST_DATABASE_URL`
  - Required when running Postgres integration tests locally.
  - CI uses: `postgres://postgres:postgres@localhost:5432/postgres`.
  - Must point to an admin database that can create and drop temporary databases.
- `YARLI_REQUIRE_POSTGRES_TESTS`
  - Set to `1` for strict mode.
  - In strict mode, missing `YARLI_TEST_DATABASE_URL` is a hard failure (never a skip-success).

## One-Command Acceptance Verification

From repository root:

```bash
export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
export YARLI_REQUIRE_POSTGRES_TESTS=1
bash scripts/verify_acceptance_rubric.sh r8
```

Expected behavior:

- Exit `0` and print `PASS` only when every rubric check is proven.
- Print `UNVERIFIED` and exit non-zero for any missing or failing proof.
- Write evidence to `evidence/r8/` (or generally `evidence/<loop-id>/`) with command logs and `README.md`.

## Fresh-Clone and Clean-Shell Repro Path

Use this sequence to prove reproducibility from a fresh clone and a clean shell.

```bash
git clone <repo-url> yarli
cd yarli
docker rm -f yarli-r8-postgres >/dev/null 2>&1 || true
docker run -d --name yarli-r8-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 55432:5432 \
  postgres:16
until pg_isready -h 127.0.0.1 -p 55432 -U postgres -d postgres >/dev/null 2>&1; do sleep 1; done
env -i HOME="$HOME" PATH="$PATH" bash --noprofile --norc -lc '
  export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
  export YARLI_REQUIRE_POSTGRES_TESTS=1
  bash scripts/verify_acceptance_rubric.sh r8
'
docker rm -f yarli-r8-postgres
```

Expected verification signals:

- Command output prints `PASS`.
- Exit code is `0`.
- `evidence/r8/README.md` records command blocks with `Exit Code:` and `Key Output:`.

## Durable Local Configuration

Create `yarli.toml` using `yarli.example.toml` as a starting point. Durable write commands require:

```toml
[core]
backend = "postgres"

[postgres]
database_url = "postgres://postgres:postgres@localhost:5432/yarli"
```

`core.backend = "in-memory"` is supported only for explicit ephemeral workflows, and write commands are blocked unless `core.allow_in_memory_writes = true`.

## Execution Backend Selection

Native process execution remains default:

```toml
[execution]
runner = "native"
```

## Default `yarli run` (Config-First)

To avoid repeating long `--cmd ...` lists, YARLI is opinionated: `yarli run` resolves prompt context in this order, expands `@include <path>`, and drives execution from config + plan state.

1. `yarli run --prompt-file <path>`
2. `yarli.toml` `[run].prompt_file`
3. fallback lookup of `PROMPT.md` by walking upward from the current directory

Execution behavior:

- Discover incomplete tranches from `IMPLEMENTATION_PLAN.md` in plan order.
- Dispatch each tranche as its own Yarli task via `[cli]` command settings.
- Optional grouping: set `[run].enable_plan_tranche_grouping = true` and annotate related plan lines with `tranche_group=<name>` to dispatch shared tranches.
- Optional per-tranche file-scope policy: annotate plan lines with `allowed_paths=path/a,path/b` and set `[run].enforce_plan_tranche_allowed_paths = true` to inject explicit scope constraints into tranche prompts.
- Optional run-spec defaults can live in `yarli.toml` (`[run]`, `[[run.tasks]]`, `[[run.tranches]]`, `[run.plan_guard]`), with `PROMPT.md` `yarli-run` blocks used only for per-prompt overrides/backward compatibility.
- Parallel mode defaults to enabled (`[features].parallel = true`) and requires `[execution].worktree_root`.
- In parallel mode, YARLI prepares one workspace copy per task under `execution.worktree_root` before execution.
- After `RunCompleted`, YARLI auto-merges task workspace changes into the source repo using `git apply --3way`.
- If a workspace merge conflicts, YARLI emits `run.parallel_merge_failed`, exits non-zero, and leaves conflict markers for manual resolution.
- Append a verification task automatically.
- If no embedded run spec exists and no incomplete tranches are found, dispatch the full prompt text as one task.
- `[cli].env_unset` can remove parent-session environment variables before CLI launch (for example `CLAUDECODE`).

Control model clarifications:

- `yarli run` is the authoritative execution entry point.
- Built-in Yarli policy gates are code-defined checks; verification command chains come from plan/config/script content.
- Observer integrations emit telemetry only and must not gate active run progression.
- Use explicit operator controls for run state transitions: `yarli run pause`, `yarli run resume`, `yarli run cancel`.

```bash
yarli run --stream
```

Notes:

- `yarli run start ... --cmd ...` remains available for ad-hoc runs.
- `yarli run batch` and `[run]` paces are supported for backward compatibility but are no longer the primary workflow.

Optional Overwatch-backed execution is opt-in:

```toml
[execution]
runner = "overwatch"

[execution.overwatch]
service_url = "http://127.0.0.1:8089"
profile = "default"
soft_timeout_seconds = 900
silent_timeout_seconds = 300
max_log_bytes = 131072
```

Behavior notes:

- `runner = "native"` keeps current local process behavior unchanged.
- `runner = "overwatch"` submits commands to Overwatch (`/run`), polls `/status/{task_id}`, reads final logs from `/output/{task_id}`, and maps terminal state into YARLI command transitions.
- Scheduler shutdown cancellation propagates to command execution; Overwatch runner calls `/cancel/{task_id}` for in-flight tasks.
- Cancellation transitions emit structured provenance (`run.cancel_provenance` / `task.cancel_provenance`) including signal identity, receiver/parent PID context, actor kind/detail, and stage attribution.
- `ui.verbose_output` affects stream verbosity only. Provenance capture is independent; enable `ui.cancellation_diagnostics = true` to append extra diagnostic context in provenance `actor_detail`.
- Durable provenance history requires Postgres backend. In-memory backend keeps provenance only for the lifetime of the current process (plus any existing `.yarli/continuation.json` artifact).

## Runtime Resource and Token Budgets

YARLI can enforce explicit per-task and per-run budgets from `yarli.toml`:

```toml
[budgets]
max_task_total_tokens = 25000
max_run_total_tokens = 250000
max_task_rss_bytes = 1073741824
max_run_peak_rss_bytes = 2147483648
```

Behavior:

- Budget breaches are fail-closed (`task.failed` with `reason = "budget_exceeded"`).
- Breach events include observed metric values, limits, command resource usage, token usage, and run usage totals.
- Token usage currently uses deterministic `char_count_div4_estimate_v1` estimation and is recorded in command/task events.

Operator-visible command surfaces for resource_usage and token_usage:

- `yarli run status <run-id>`: shows per-task `budget_exceeded`, `token_usage` (prompt_tokens, completion_tokens, total_tokens), and `resource_usage` (max_rss_bytes).
- `yarli run explain-exit <run-id>`: shows `Budget breaches:` section with task name and breach detail, plus per-task token and resource usage.
- `yarli task explain <task-id>`: shows `budget_exceeded` reason, token usage, and resource usage for individual tasks.

## Sequence Deterioration Observer

YARLI emits rolling deterioration analysis events (`run.observer.deterioration`) during run execution.
The observer consumes event deltas incrementally using event-store cursor reads (`after_event_id`), not full-history rescans.
YARLI also emits heartbeat progress events (`run.observer.progress`) so stream output remains active during long-running tasks.

Signals included in the score:

- Runtime drift for repeated command keys.
- Retry inflation and blocker churn.
- Failure-rate drift by reason bucket.
- Budget headroom erosion (when observed/limit values are present).

Operator surfaces:

- `yarli run status <run-id>` includes latest deterioration score/trend/factors when available.
- `yarli run explain-exit <run-id>` includes a sequence deterioration section.
- `yarli-api` (if you run it) exposes the latest report as an optional `deterioration` field on `GET /v1/runs/<run-id>/status`.

## Reproducing Budget Stress Checks Locally

Run these commands to verify budget governance under parallel workload:

```bash
# Single-task budget breach: task exceeds token limit, fails without retry
cargo test -p yarli-queue scheduler::tests::test_budget_exceeded_fails_task_without_retry -- --nocapture

# Run-level budget breach: cumulative tokens across tasks exceed run limit
cargo test -p yarli-queue scheduler::tests::test_run_token_budget_exceeded_across_tasks -- --nocapture

# Parallel-task budget stress: 4 concurrent tasks with tight run budget,
# proves accounting consistency and no silent continuation after breach
cargo test -p yarli-queue scheduler::tests::test_parallel_tasks_budget_accounting_consistency -- --nocapture

# Command execution resource capture (exec layer)
cargo test -p yarli-exec -- --nocapture
```

Expected behavior:

- All commands exit `0`.
- Budget failure events include `reason = "budget_exceeded"` with observed/limit metrics.
- Run does not reach `RunCompleted` after any budget breach.

## Migration Workflow

YARLI schema SQL lives under `crates/yarli-store/migrations/`.

Apply migrations in order:

```bash
psql "$DATABASE_URL" -f crates/yarli-store/migrations/0001_init.sql
psql "$DATABASE_URL" -f crates/yarli-store/migrations/0002_indexes.sql
```

Recommended:

- Use a dedicated database for YARLI runtime data.
- Apply migrations before running durable CLI write commands.

## Deterministic Local Strict Postgres Verification Workflow

Run this exact sequence from repository root on a clean shell.

Start and prepare a local Postgres 16 instance:

```bash
docker rm -f yarli-r4v-postgres >/dev/null 2>&1 || true
docker run -d --name yarli-r4v-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 55432:5432 \
  postgres:16
until pg_isready -h 127.0.0.1 -p 55432 -U postgres -d postgres >/dev/null 2>&1; do sleep 1; done
```

Sanity-check strict mode fails fast when DB env is missing:

```bash
unset YARLI_TEST_DATABASE_URL
export YARLI_REQUIRE_POSTGRES_TESTS=1
cargo test -p yarli-store --test postgres_integration -- --nocapture
```

Expected result: command fails and includes
`postgres integration tests require YARLI_TEST_DATABASE_URL when YARLI_REQUIRE_POSTGRES_TESTS=1`.

Run strict Postgres integration suites (real execution path):

```bash
export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
export YARLI_REQUIRE_POSTGRES_TESTS=1
cargo test -p yarli-store --test postgres_integration -- --nocapture
cargo test -p yarli-queue --test postgres_integration -- --nocapture
cargo test -p yarli-cli --test postgres_integration -- --nocapture
```

True execution signals:

- Each command exits with status `0`.
- Output contains `test result: ok`.
- Output does not contain `skipping postgres integration test`.

Cleanup:

```bash
docker rm -f yarli-r4v-postgres
```

## Baseline Workspace Verification

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

## Consistency and Governance Contract

See `docs/CONSISTENCY_CONTRACT.md` for the canonical strong-consistency and runtime-governance contract, including:

- Per-aggregate transition ordering and linearizable state.
- Durable-state-before-side-effects invariant.
- Idempotency key replay behavior.
- Queue lease single-owner invariant.
- Resource and token accounting fields and budget breach outcomes.

## Acceptance Decision

Use `docs/ACCEPTANCE_RUBRIC.md` to determine a binary outcome:

- `PASS`: all required checks are proven with tracked evidence.
- `UNVERIFIED`: any required check is missing, failing, or not evidenced.
