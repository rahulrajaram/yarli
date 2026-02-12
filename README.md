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
Legacy prompt-embedded task execution remains as fallback compatibility when config-first
dispatch cannot be materialized.

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
