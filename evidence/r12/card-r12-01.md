# CARD-R12-01 Evidence

Date: 2026-02-22

## Scope
- Added scheduler telemetry hooks in `crates/yarli-queue/src/scheduler.rs` for:
  - queue depth, run transition, task transition, gate failure, command execution count/duration, and run usage metrics.
  - queue lifecycle operations now funnel through helpers that keep queue-depth metrics synchronized.
  - stale lease reclamation increments lease-timeout counter.
- Added `/metrics` endpoint in `crates/yarli-api/src/server.rs` that returns Prometheus exposition from a shared registry.
- Added `yarli-observability` + `prometheus-client` wiring in `crates/yarli-api/Cargo.toml` for API metric encoding.
- Added API integration test for `/metrics` route.

## Verification
- `cargo fmt --all -- --check`  
  - **FAILED**: Formatting check found diffs in multiple files (including `crates/yarli-cli/src/stream/headless.rs`, `crates/yarli-cli/src/stream/renderer.rs`, and others).
- `cargo clippy --workspace --all-targets -- -D warnings`  
  - **FAILED**: `clippy::type-complexity` in `crates/yarli-core/src/shutdown.rs` (error in `capture_process_context`) under `-D warnings`.
- `cargo test --workspace`  
  - **FAILED**: compile errors in `crates/yarli-observability/src/tracing_init.rs` and `crates/yarli-observability/src/metrics.rs` (OpenTelemetry OTLP API compatibility and `prometheus-client` gauge value type mismatches).

Status: failed
