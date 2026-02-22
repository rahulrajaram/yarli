# CARD-R16-01 Verification Evidence

Date: 2026-02-22

Objective: implement task priority semantics (high-to-low), dependency priority inheritance, and queue fairness, then run verification.

Command outcomes:

- `cargo fmt --all`
  - Exit: `0`
  - Result: formatting applied successfully.

- `cargo fmt --all -- --check`
  - Exit: `0`
  - Result: formatting clean.

- `cargo clippy --workspace --all-targets -- -D warnings`
  - Exit: `101`
  - Blockers (from unrelated `crates/yarli-api/src/server.rs`):
    - `unused import: HashSet`
    - `missing generics for struct axum::http::Request`
    - `type mismatch in function arguments` on `map_err(ApiError::Store)`
    - `no field 'after' on type EventQuery`
    - `EventFilter: serde::Serialize` trait not implemented
    - `unused import: futures_util::SinkExt`

- `cargo test --workspace`
  - Exit: `101`
  - Same compile blockers in `crates/yarli-api/src/server.rs` prevent test execution.

Implementation file touchpoints remain:
- `crates/yarli-core/src/entities/task.rs`
- `crates/yarli-queue/src/queue.rs`
- `crates/yarli-queue/src/memory.rs`
- `crates/yarli-queue/src/postgres.rs`
- `crates/yarli-queue/src/scheduler.rs`
- `crates/yarli-queue/src/memory/tests.rs`
- `crates/yarli-store/migrations/0001_init.sql`
