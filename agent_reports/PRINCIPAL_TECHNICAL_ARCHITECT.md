# MVP Readiness Report — YARLI

## Executive Summary

- Verdict: HOLD
- Confidence: 0.78
- Primary Blockers (P0): yarli-api is not wired into the CLI binary; the REST API is unreachable at runtime. `yarli serve` does not exist as a command.
- Next Review ETA: After API server is wired and a `yarli serve` command exists, re-evaluate for Ship.

---

## Architecture Overview

### Components and Interfaces

12 crates in workspace:

| Crate | Role | State |
|---|---|---|
| yarli-core | Domain primitives, FSMs (5), entities, explain engine | Complete |
| yarli-store | In-memory and Postgres event store | Complete |
| yarli-queue | Task queue, lease scheduling, scheduler loop, Postgres queue | Complete |
| yarli-exec | Command runner (native + overwatch), process introspection | Complete |
| yarli-git | Worktree manager, merge orchestrator, submodule policy, recovery | Complete |
| yarli-gates | Gate evaluators (7 built-in), evidence schema | Complete |
| yarli-policy | Policy engine, approval tokens, safe modes | Complete |
| yarli-memory | Haake memory adapter | Complete |
| yarli-observability | JSONL audit sink, OpenTelemetry, Prometheus, run_analyzer | Complete |
| yarli-sw4rm | sw4rm multi-agent integration (feature-gated) | Complete |
| yarli-api | HTTP server (axum 0.7): 17 routes + debug feature | NOT WIRED INTO CLI |
| yarli-chaos | Fault injection harness (feature-gated) | Stub — no injection points |
| yarli-cli | CLI binary, stream/dashboard renderers, command handlers | Missing `yarli serve` |

### Data and State Flow

```
yarli run → LoadedConfig → with_event_store/with_event_store_and_queue
         → Scheduler::run_loop → drive_scheduler()
         → observe_run_end() → MemoryObserver + AuditSink
         → ContinuationPayload (JSONL, .yarli/ dir)
```

REST API path exists in yarli-api but is never started: `router(store)` and `serve(listener, store)` are defined, tested in isolation, but `yarli-cli/Cargo.toml` does not list `yarli-api` as a dependency. No `yarli serve` command exists.

### Cross-Cutting Concerns

- Auth: API key enforcement via `YARLI_API_KEYS` env var — implemented in yarli-api, but server is never started.
- Logging: OpenTelemetry tracing initialized in main.rs via `init_tracing()`. YARLI_LOG / RUST_LOG wired.
- Observability: JSONL audit sink wired into `drive_scheduler()` at all run-exit paths. Prometheus metrics registry exists inside `ApiState`, but metrics collection is isolated to the API layer and is therefore also unreachable at runtime.
- Error handling: `anyhow` throughout CLI; typed errors in library crates via `thiserror`.
- Graceful shutdown: Two-stage Ctrl+C via CancellationToken. SIGTERM+SIGKILL escalation for child processes.

---

## Gap Analysis

### Implemented vs Planned (Delta Table)

| Area | Documented in README | Implemented | Gap |
|---|---|---|---|
| `yarli run` (default, start, batch, status, explain-exit, list, continue, pause, resume, cancel) | Yes | Yes | None |
| `yarli task list/explain/unblock/annotate/output` | Yes | Yes | None |
| `yarli gate list/rerun` | Yes | Yes | None |
| `yarli worktree status/recover` | Yes | Yes | None |
| `yarli merge request/approve/reject/status` | Yes | Yes | None |
| `yarli audit tail/query` | Yes | Yes | None |
| `yarli plan validate/tranche add/list/complete/remove` | Yes | Yes | None |
| `yarli debug queue-depth/active-leases/resource-usage` | Yes | Yes | None |
| `yarli migrate status/up/down/backup/restore` | Yes | Yes | None |
| `yarli init` | Yes | Yes | None |
| `yarli info` | Yes | Yes | None |
| REST API (`/health`, `/metrics`, `/v1/runs`, `/v1/tasks`, `/v1/audit`, `/v1/events/ws`, webhooks) | Yes — README documents 17 routes | Implemented in yarli-api/src/server.rs | `yarli-api` not in yarli-cli deps; `yarli serve` command absent; API is unreachable at runtime |
| Prometheus metrics (runtime) | Yes — `/metrics` route documented | Registry created inside ApiState only | Metrics are not collected during `yarli run` (no scheduler→metrics bridge) |
| `yarli serve` subcommand | Listed in README as REST API section | Missing from Commands enum | P0 blocker |
| OTLP tracing export | `OTEL_EXPORTER_OTLP_ENDPOINT` documented | `init_tracing()` wired; exporter depends on env var | Functional if env var set — no gap |
| yarli-chaos injection points | Crate exists, tests pass | Traits defined, but `ChaosController::inject()` not called from yarli-queue/store/exec code paths | Low risk for production; chaos is test-only currently |

### Risk Matrix

| Risk | Severity | Likelihood | Impact |
|---|---|---|---|
| API server never starts — webhook/WS consumers cannot integrate | Critical | Certain (code gap confirmed) | External integrations broken entirely |
| Prometheus metrics unreachable — SRE dashboards cannot scrape | High | Certain | Zero runtime observability for ops teams |
| `yarli-chaos` has no injection sites — chaos feature unusable | Low | Certain | Affects only chaos/resilience testing |
| Timer-sensitive flake in yarli-cli unit tests (observed once, passed with --no-fail-fast) | Medium | Occasional | CI instability; currently low frequency |
| `build_continuation_payload` has a `never used` clippy warning | Low | Certain | Dead code accumulation |
| UUID v7 worktree path collision under sub-millisecond creation | Medium | Low (prod) | Silent worktree clobber in high-parallelism scenarios |
| debug commands (`queue-depth`, `active-leases`) read from in-memory state only — do not work against a running scheduler over Postgres | High | Certain | Operator tooling misleads in durable mode |

### Remediation Effort (LoE)

| Item | LoE |
|---|---|
| Add `yarli-api` to yarli-cli deps, add `yarli serve --port` command, launch `serve()` in tokio task | 1–2 days |
| Wire Prometheus metrics into `drive_scheduler()` (task completions, gate failures, timing) | 1–2 days |
| Fix debug commands to query Postgres queue state (not in-memory queue only) | 1 day |
| Replace `Uuid::now_v7()` with `Uuid::new_v4()` in worktree path generation | 2 hours |
| E2E smoke test covering `yarli run` through real Postgres via CI | 2–3 days |
| Suppress or use `build_continuation_payload` (resolve dead-code warning) | 1 hour |

---

## Test Integrity

- **Total tests (workspace, excluding integration crate):** 1208 as of last MEMORY.md snapshot
- **Integration test suite (tests/integration/):** 11 test files covering: multi-task lifecycle, concurrent runs, concurrency stress, crash recovery, budget breach, gate verification retry, graceful shutdown, long-running stability, resource exhaustion, time-based failure, event trail consistency. All require no Postgres by default; skip cleanly when `YARLI_TEST_DATABASE_URL` is unset.
- **Postgres integration tests:** Exist in yarli-api, yarli-cli, yarli-queue, yarli-store — all guard-skipped unless `YARLI_TEST_DATABASE_URL` is set. CI likely runs without Postgres (durable path undertested in CI).
- **API integration test (postgres_integration.rs):** Tests `GET /v1/runs/:run_id/status` and `GET /v1/tasks/:task_id` with real Postgres. Only 1 test. No tests for POST routes (create_run, pause, resume, cancel, annotate, unblock, retry), webhooks, or WebSocket event stream.
- **CI Stability Index:** One flaky failure observed during this audit run (291/292 tests passed first attempt in yarli-cli, all 292 passed with --no-fail-fast). Indicates a non-deterministic timing test.
- **Coverage blind spots:**
  - No E2E test exercises `yarli run` as a real binary against a real LLM CLI backend.
  - API routes for mutation (POST) are not tested in the integration test.
  - WebSocket event stream (`/v1/events/ws`) has no test.
  - Webhook dispatch loop (500ms polling, retry-with-backoff) has no test.
  - `yarli-chaos` injection points do not exist — chaos tests cannot exercise real failure paths.
  - `cmd_debug_queue_depth` / `cmd_debug_active_leases` work only against the in-memory queue constructed at CLI startup, not a running scheduler — not integration tested.

---

## Critical Path

### P0 — Production Blockers

1. **Wire yarli-api into yarli-cli binary.** Add `yarli-api` to `crates/yarli-cli/Cargo.toml` dependencies. Add `Commands::Serve { port: u16, bind: String }` to `crates/yarli-cli/src/cli.rs`. Add `cmd_serve()` to `crates/yarli-cli/src/commands.rs` that calls `yarli_api::serve(listener, store)`. The API server documented in README must actually start.

2. **Wire Prometheus metrics into the scheduler.** `ApiState` creates a `YarliMetrics` registry but the scheduler never increments counters. Metrics at `/metrics` will be all-zeros at runtime. Either pass the registry into `drive_scheduler()` or expose a channel from the scheduler to the API state.

### P1 — High Impact, Ship-Blocking for Production Users

3. **Fix debug commands for durable (Postgres) mode.** `cmd_debug_queue_depth()` calls `queue.entries()` on the in-memory queue constructed at CLI startup. In durable mode this returns empty. Users running `yarli debug queue-depth` against a live Postgres-backed deployment will see no data. Needs a query path against the Postgres queue table.

4. **Add `yarli serve` API integration tests.** The existing Postgres integration test covers only 2 read routes. POST routes (create, pause, resume, cancel), the WebSocket stream, and webhook registration have no tests. A failure in any of these is silent.

5. **Fix worktree UUID collision.** `crates/yarli-git/src/manager.rs` (or wherever worktree paths are constructed) uses `Uuid::now_v7()` for path segments. Under sub-millisecond scheduler creation cadence, paths collide. Use `Uuid::new_v4()` for worktree path randomness.

### P2 — Quality and Maintainability

6. **Resolve dead-code warning.** `build_continuation_payload` triggers a `warning: function is never used`. Confirm if it is intentionally kept for future use; suppress with `#[allow(dead_code)]` or remove.

7. **Add chaos injection points.** `yarli-chaos` is a complete framework with no call sites. Connect `ChaosController::inject()` at: store append, queue claim, command runner spawn. Required for chaos/resilience regression tests.

8. **E2E smoke test via CI.** A test that compiles `yarli`, calls `yarli init`, `yarli run start "smoke" --cmd "echo ok"` against in-memory backend, and asserts exit 0 would catch CLI regressions that unit tests miss.

---

## Technical Debt Register

| Location | Severity | Description | Suggested Fix |
|---|---|---|---|
| `crates/yarli-cli/Cargo.toml` | P0 | `yarli-api` not listed as dependency; REST API is dead code from the CLI's perspective | Add `yarli-api = { workspace = true }` |
| `crates/yarli-cli/src/cli.rs` | P0 | No `Commands::Serve` variant; `yarli serve` is absent from the CLI | Add `Serve { port, bind }` to Commands enum |
| `crates/yarli-api/src/server.rs:75` | P1 | `YarliMetrics::new(&mut metrics_registry)` result is discarded with `let _ =`; metrics registry is populated but scheduler never writes to it | Bridge scheduler events to the metrics registry |
| `crates/yarli-cli/src/commands.rs:7217` | P1 | `cmd_debug_queue_depth` uses `with_event_store_and_queue` which constructs a fresh in-memory queue — empty for Postgres deployments | Query Postgres queue table directly |
| `crates/yarli-cli/src/commands.rs:2964` | P2 | `build_continuation_payload` is never called directly (only `_with_gate_failures` variant is used); causes clippy `never used` warning | Merge or remove the wrapper; add `#[allow(dead_code)]` if intentional |
| `crates/yarli-chaos/src/lib.rs` | P2 | Four fault types implemented but `inject()` is never called from production code | Add `ChaosController::inject()` call sites in yarli-store, yarli-queue, yarli-exec behind `#[cfg(feature = "chaos")]` gates |
| `crates/yarli-api/src/server.rs` (Cargo.toml description) | Low | Package `description` says "gRPC services" but it is an HTTP/axum server | Update description |

---

## Task Routing by Domain

### Frontend

- No frontend gaps. Stream renderer (braille spinner, 4-tier hierarchy) and TUI dashboard (ratatui 0.29) are complete with 49 + 30 tests.

### Backend

- P0: Add `yarli-api` dep to yarli-cli; add `yarli serve` command handler.
- P0: Bridge `YarliMetrics` counters into scheduler tick loop (task completions, gate failures, command timing).
- P1: Fix `cmd_debug_queue_depth` and `cmd_debug_active_leases` for Postgres mode.
- P2: Resolve `build_continuation_payload` dead-code warning.
- P2: Add chaos injection call sites in yarli-store, yarli-queue, yarli-exec.

### Platform / Orchestration

- P1: Replace `Uuid::now_v7()` with `Uuid::new_v4()` in worktree path construction.
- P1: Add `yarli serve` startup to CLI, including graceful shutdown integration (pass `CancellationToken` to the serve task).

### Data and Database

- No schema gaps. Two migrations are complete and tested. Postgres integration test pattern is consistent across crates.

### Testing and QA

- P1: Add API integration tests for all POST mutation routes (create_run, pause, resume, cancel, unblock, retry, annotate).
- P1: Add WebSocket event stream integration test.
- P1: Add webhook dispatch integration test.
- P1: Add E2E smoke test that invokes the `yarli` binary itself.
- P2: Investigate and fix the timing-sensitive flake in yarli-cli (1 test failed on first run, passed on re-run).
- P2: Enable Postgres integration tests in CI by running `docker run postgres:16` in the CI workflow.

### Infra and SRE

- P0: Without `/metrics` returning real data, Prometheus/Grafana dashboards cannot be built.
- P1: Document how to run `yarli serve` alongside `yarli run` (sidecar pattern vs. embedded).
- P2: Document Kubernetes deployment pattern (secret mount for `postgres.database_url_file`, liveness probe on `/health`).

### Security

- No critical gaps. API key auth, rate limiting, and safe mode enforcement are implemented. The `YARLI_API_KEYS` pattern is functional once the server starts.
- P2: API keys are compared with `==` (constant-time comparison would be preferable for timing-attack resistance).

---

## Readiness Verdict

**HOLD — Do Not Ship to Production**

**Rationale:** The REST API is the primary integration surface documented in README and is entirely non-functional at runtime. `yarli-api` is not a dependency of `yarli-cli`. No `yarli serve` command exists. A user following the README's REST API section will find no server running. Prometheus metrics are similarly unreachable. These are not feature gaps — they are wiring failures between an implemented library and the binary that should expose it.

The CLI-only workflow (`yarli run`, operator controls, audit, plan, migrate) is production-ready for teams comfortable with CLI-only operation against a Postgres backend. If the target users are ops teams integrating via REST API or Prometheus scraping, the system is not ready.

**Ship criteria:**
1. `yarli serve --port 3000` starts the axum server and the `/health` endpoint returns 200.
2. `GET /metrics` returns non-zero scheduler counters during a live run.
3. At least one POST mutation route (`POST /v1/runs/:id/pause`) is covered by an integration test.
4. The worktree UUID collision is patched.

Estimated remediation: 3–5 developer-days.

---

## Change Log

- 2026-03-03: Initial audit. All milestones M0–M4 plus sw4rm and post-run analyzer confirmed complete. Identified yarli-api not wired into CLI binary as P0 blocker. Confirmed 1208 tests pass (1 timing flake observed, non-blocking). Documented gap table, risk matrix, and domain-routed task list.
