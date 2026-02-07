# YARLI Operations

This runbook covers the baseline local operator workflow for durable mode, migrations, and test execution.
For final verification and release acceptance decisions, use `docs/ACCEPTANCE_RUBRIC.md`.

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
bash scripts/verify_acceptance_rubric.sh r5
```

Expected behavior:

- Exit `0` and print `PASS` only when every rubric check is proven.
- Print `UNVERIFIED` and exit non-zero for any missing or failing proof.
- Write evidence to `evidence/r5/` (or generally `evidence/<loop-id>/`) with command logs and `README.md`.

## Fresh-Clone and Clean-Shell Repro Path

Use this sequence to prove reproducibility from a fresh clone and a clean shell.

```bash
git clone <repo-url> yarli
cd yarli
docker rm -f yarli-r5-postgres >/dev/null 2>&1 || true
docker run -d --name yarli-r5-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 55432:5432 \
  postgres:16
until pg_isready -h 127.0.0.1 -p 55432 -U postgres -d postgres >/dev/null 2>&1; do sleep 1; done
env -i HOME="$HOME" PATH="$PATH" bash --noprofile --norc -lc '
  export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
  export YARLI_REQUIRE_POSTGRES_TESTS=1
  bash scripts/verify_acceptance_rubric.sh r5
'
docker rm -f yarli-r5-postgres
```

Expected verification signals:

- Command output prints `PASS`.
- Exit code is `0`.
- `evidence/r5/README.md` records command blocks with `Exit Code:` and `Key Output:`.

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

## Acceptance Decision

Use `docs/ACCEPTANCE_RUBRIC.md` to determine a binary outcome:

- `PASS`: all required checks are proven with tracked evidence.
- `UNVERIFIED`: any required check is missing, failing, or not evidenced.
