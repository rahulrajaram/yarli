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

## Postgres Integration Tests

```bash
export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo test -p yarli-store --test postgres_integration
cargo test -p yarli-queue --test postgres_integration
cargo test -p yarli-cli --test postgres_integration
```

## Operations

Operational setup, migration steps, and local runbook details are documented in `docs/OPERATIONS.md`.
