# CARD-R12-02 Evidence

Date: 2026-02-22

## Scope
- Added `yarli audit query` in `crates/yarli-cli/src/cli.rs` and `crates/yarli-cli/src/main.rs` wiring.
- Added audit query handlers and render formats (table/json/csv) in `crates/yarli-cli/src/commands.rs`.
- Added cursor/offset/limit pagination and run/task/category/actor/time filtering.
- Added audit panel to dashboard TUI and wired audit file read path in `crates/yarli-cli/src/dashboard/renderer.rs` and `crates/yarli-cli/src/state.rs`/`input.rs`.
- Updated `docs/CLI.md` to document query examples.
- Added regression tests for query filters and invalid filter handling.

## Verification
- `cargo fmt --all -- --check`
  - command: `cargo fmt --all -- --check`
  - exit: 1
  - key output: parser and style issues are reported with large cross-file formatting diffs, notably touching many pre-existing files unrelated to this tranche.

- `cargo clippy --workspace --all-targets -- -D warnings`
  - command: `cargo clippy --workspace --all-targets -- -D warnings`
  - exit: 1
  - key output: failed in `crates/yarli-core/src/shutdown.rs` due `clippy::type_complexity` on
    `capture_process_context() -> (Option<u32>, Option<u32>, Option<u32>, Option<u32>, Option<String>)`.

- `cargo test --workspace`
  - command: `cargo test --workspace`
  - exit: 1
  - key output: blocked by pre-existing `crates/yarli-observability` compile errors in `tracing_init.rs` (API mismatch with `opentelemetry_otlp`, incompatible tracing layer types) and `metrics.rs` gauge integer type mismatches.
