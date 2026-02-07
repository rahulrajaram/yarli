# YARLI Operations

This runbook covers the baseline local operator workflow for durable mode, migrations, and test execution.

## Requirements

- Rust toolchain compatible with workspace `rust-version` (currently 1.75+).
- PostgreSQL accessible from the local machine.
- `psql` client for manual migration execution.

## Required Environment Variables

- `YARLI_TEST_DATABASE_URL`
  - Required when running Postgres integration tests locally.
  - CI uses: `postgres://postgres:postgres@localhost:5432/postgres`.
  - Must point to an admin database that can create and drop temporary databases.

## Durable Local Configuration

Create `yarli.toml` using `yarli.example.toml` as a starting point. Durable write commands require:

```toml
[core]
backend = "postgres"

[postgres]
database_url = "postgres://postgres:postgres@localhost:5432/yarli"
```

`core.backend = "in-memory"` is supported only for explicit ephemeral workflows, and write commands are blocked unless `core.allow_in_memory_writes = true`.

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

## Local Verification Workflow

Run baseline verification:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

Run Postgres integration suites explicitly:

```bash
export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo test -p yarli-store --test postgres_integration
cargo test -p yarli-queue --test postgres_integration
cargo test -p yarli-cli --test postgres_integration
```
