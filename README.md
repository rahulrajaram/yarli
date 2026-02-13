# YARLI

YARLI is a Rust workspace for durable run/task orchestration with event sourcing, queue scheduling, and git workflow controls.

Execution backends:

- `execution.runner = "native"` (default)
- `execution.runner = "overwatch"` (opt-in; Overwatch service API integration)

## Quick Verification

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

## Default `yarli run`

`yarli run` is opinionated: prompt resolution precedence is:
1. `yarli run --prompt-file <path>`
2. `yarli.toml` `[run].prompt_file`
3. fallback lookup for `PROMPT.md` (walking up from the current directory)

YARLI then expands any `@include <path>` directives, discovers incomplete tranches from
`IMPLEMENTATION_PLAN.md`, and dispatches tranche + verification tasks via `[cli]` config.
Run-spec defaults can be defined in `yarli.toml` under `[run]`, `[[run.tasks]]`,
`[[run.tranches]]`, and `[run.plan_guard]`; a `PROMPT.md` `yarli-run` block is optional
and acts as an override layer.
When `PROMPT.md` has no embedded `yarli-run` block and the plan has no open tranches,
YARLI dispatches the full prompt text as a single task.
When `[features].parallel = true` (default), `yarli run` requires `[execution].worktree_root`
and provisions per-task workspace copies under that root before execution.
Execution paths (`execution.working_dir` and `execution.worktree_root`) expand `~` and `$ENV_VAR`
tokens before resolution.
Use `[execution].worktree_exclude_paths` to skip heavy/generated paths during workspace copy
(for example `target`, `node_modules`, `.venv`, `venv`, `__pycache__`).
After a completed run, YARLI auto-merges each task workspace back into the source repo by scoping
to paths that differ from the source workspace and applying with `git apply --3way`.
If a merge cannot be completed automatically, YARLI preserves the run workspace root and writes
`PARALLEL_MERGE_RECOVERY.txt` with deterministic operator recovery commands.
Optional grouping is available with `[run].enable_plan_tranche_grouping = true` plus
`tranche_group=<name>` metadata on plan lines.
Optional tranche file scope hints are available with `allowed_paths=...` metadata and
`[run].enforce_plan_tranche_allowed_paths = true`.
Legacy prompt-embedded task execution remains as fallback compatibility when config-first
dispatch cannot be materialized.
CLI environment isolation is configurable with `[cli].env_unset` (for example
`env_unset = ["CLAUDECODE"]` for nested Claude subprocesses).

Control model:
- `yarli run` is the authoritative execution entry point.
- Built-in Yarli policy gates are code-defined checks; verification command chains are plan/config/script-defined.
- Observer integrations are read-only telemetry and do not gate active run progression.
- Operator controls are explicit: `yarli run pause`, `yarli run resume`, `yarli run cancel`.

```bash
yarli run --stream
```

## CLI Usage

See `docs/CLI.md` for an exhaustive, command-by-command guide (with `init` backend examples).

## Postgres Integration Tests

```bash
export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo test -p yarli-store --test postgres_integration
cargo test -p yarli-queue --test postgres_integration
cargo test -p yarli-cli --test postgres_integration
```

## Operations

Operational setup, migration steps, and local runbook details are documented in `docs/OPERATIONS.md`.
