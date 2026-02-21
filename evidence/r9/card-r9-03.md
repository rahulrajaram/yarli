# CARD-R9-03 Evidence

Date: 2026-02-21  
Tranche: CARD-R9-03  
Target: Detect deterioration cycles and emit structured events

## Verification commands

Executed in `/home/rahul/Documents/yarl`.

1. `cargo fmt --all -- --check`
- Exit: `1`
- Output: formatting differences present; includes files outside this tranche, e.g. `crates/yarli-cli/src/commands.rs`, `crates/yarli-cli/src/config.rs`, `crates/yarli-cli/tests/postgres_integration.rs`, and other existing files.

2. `cargo clippy --workspace --all-targets -- -D warnings`
- Exit: `101`
- Blocked by pre-existing warning in unrelated file:
  - `crates/yarli-core/src/shutdown.rs:485`
  - `clippy::type-complexity` (`capture_process_context` tuple return type).

3. `cargo test --workspace`
- Exit: `0`
- Test suite passed. New tranche tests observed:
  - `observers::tests::detect_deterioration_cycle_reports_patterns`
  - `observers::tests::stable_trend_does_not_trigger_cycle`

## Outcome

- `crates/yarli-cli/src/observers.rs` updated with:
  - trend history tracking for deterioration cycles,
  - `run.observer.deterioration_cycle` event emission,
  - cycle detection helpers and tests.
