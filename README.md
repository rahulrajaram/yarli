<p align="center">
  <img src="assets/logo.png" alt="YARLI" width="360">
</p>

<p align="center">
  Deterministic run/task orchestration with event sourcing, queue scheduling, and safe Git controls.
</p>

## What is this?
YARLI is a Rust workspace for executing plan-driven engineering workflows with durable state and explicit operator control. It treats runs, tasks, worktrees, merges, command execution, and policy decisions as state-machine entities persisted through an event log.

The primary workflow is `yarli run`: resolve prompt context, discover open tranches, dispatch execution tasks, and emit explainable run/task status. YARLI supports both local ephemeral development and durable Postgres-backed operation.

This project is for teams that want reproducible orchestration behavior, auditable transitions, and safe defaults around Git operations, policy checks, and runtime controls.

## Features
- Config-first orchestration (`yarli.toml`) with plan-driven tranche dispatch.
- Durable event store and queue backends (Postgres) with in-memory mode for local testing.
- Explicit operator controls (`pause`, `resume`, `cancel`) and explainability commands.
- Policy and gate evaluation over task/run state.
- Parallel workspace execution with scoped merge and recovery artifacts.
- Optional memory provider adapters via `[memory.providers.<name>]`.
- REST API crate (`yarli-api`) with health, status, control, webhook, and websocket event routes.

## Installation
### Build from source
```bash
cargo build --release -p yarli-cli --bin yarli
./target/release/yarli --help
```

### Cargo install (local path)
```bash
cargo install --path crates/yarli-cli --bin yarli
yarli --help
```

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

## CLI Reference
Verified command tree (from live `--help`):
- `yarli run`: `start`, `batch`, `status`, `explain-exit`, `list`, `continue`, `pause`, `resume`, `cancel`
- `yarli task`: `list`, `explain`, `unblock`, `annotate`, `output`
- `yarli gate`: `list`, `rerun`
- `yarli worktree`: `status`, `recover`
- `yarli merge`: `request`, `approve`, `reject`, `status`
- `yarli audit`: `tail`, `query`
- `yarli plan`: `tranche` (`add`, `complete`, `list`, `remove`), `validate`
- `yarli debug`: `queue-depth`, `active-leases`, `resource-usage`
- `yarli migrate`: `status`, `up`, `down`, `backup`, `restore`
- `yarli init`, `yarli info`

Exact clap output snapshots for every command/subcommand are in `docs/CLI_HELP.md`.

## AI Agent Integration
YARLI integrates with agent CLIs through `[cli]` configuration (for example `codex`, `claude`, `gemini`, or custom command wiring).

Example:
```toml
[cli]
backend = "codex"
prompt_mode = "arg"
command = "codex"
args = ["exec"]
```

YARLI is not currently an MCP server. Agent integration is command-dispatch based.

## gRPC API
Current status: no gRPC service is exposed by default in this repository.  
`yarli-api` currently exposes HTTP/WebSocket routes (Axum). If gRPC is added later, document proto service definitions and RPCs here.

## REST API
Core routes (from `crates/yarli-api/src/server.rs`):
- `GET /health`
- `GET /metrics`
- `GET /v1/events/ws`
- `POST /v1/webhooks`
- `POST /v1/runs`, `GET /v1/runs`
- `GET /v1/runs/:run_id/status`
- `POST /v1/runs/:run_id/pause|resume|cancel`
- `GET /v1/tasks`
- `GET /v1/tasks/:task_id`
- `POST /v1/tasks/:task_id/annotate|unblock|retry`
- `GET /v1/audit`
- `GET /v1/metrics/reporting`
- `POST /v1/tasks/:task_id/priority` and `/debug/*` routes when built with `debug-api`

Example curl:
```bash
curl -sS http://127.0.0.1:3000/health
curl -sS http://127.0.0.1:3000/v1/runs
curl -sS http://127.0.0.1:3000/v1/runs/<run-id>/status
```

Minimal server embedding example:
```rust
use std::sync::Arc;
use tokio::net::TcpListener;
use yarli_api::server::serve;
use yarli_store::InMemoryEventStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    let store = Arc::new(InMemoryEventStore::new());
    serve(listener, store).await?;
    Ok(())
}
```

## Configuration
Main config file: `yarli.toml` (template via `yarli init`; example in `yarli.example.toml`).

Important environment variables read by code:
- `DATABASE_URL` (Postgres DSN fallback)
- `YARLI_LOG` (used to populate `RUST_LOG` when unset)
- `YARLI_ALLOW_RECURSIVE_RUN` (recursive-run override path)
- `YARLI_API_KEYS` (API auth keys)
- `YARLI_API_RATE_LIMIT_PER_MINUTE` (API rate limit)
- `OTEL_EXPORTER_OTLP_ENDPOINT` (OTLP tracing exporter endpoint)
- `YARLI_TEST_DATABASE_URL`, `YARLI_REQUIRE_POSTGRES_TESTS` (Postgres integration test controls)

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

## Security
- Safe mode and policy enforcement are configurable (`observe`, `restricted`, `execute`, `breakglass`).
- Destructive Git operations are denied by default (`git.destructive_default_deny = true`).
- API authentication is enabled when `YARLI_API_KEYS` is set.
- API key rate limiting uses `YARLI_API_RATE_LIMIT_PER_MINUTE` (default 120/min).
- Durable mode is recommended for write commands; in-memory mode blocks writes unless explicitly allowed.

## Examples
```bash
# List runs and inspect a run
yarli run list
yarli run status <run-id>

# Explain unresolved run/task state
yarli run explain-exit <run-id>
yarli task explain <task-id>

# Operator controls
yarli run pause <run-id> --reason "maintenance window"
yarli run resume <run-id> --reason "maintenance complete"
yarli run cancel <run-id> --reason "operator stop"

# Migration lifecycle
yarli migrate status
yarli migrate up
yarli migrate backup --label pre_change
```

## License
Dual licensed under `MIT OR Apache-2.0`.
