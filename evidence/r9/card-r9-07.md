# CARD-R9-07 Evidence

Date: 2026-02-21
Card target: Add regression tests for deterioration and soft checkpointing

## Commands run

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`

## Command results

### `cargo fmt --all -- --check`
- Exit code: 1
- Key output: formatting differences were reported in multiple existing files not introduced by this tranche, including:
  - `crates/yarli-cli/src/stream/headless.rs`
  - `crates/yarli-cli/src/stream/renderer.rs`
  - `crates/yarli-cli/src/commands.rs`
  - `crates/yarli-cli/src/config.rs`
  - `crates/yarli-cli/src/observers.rs`
  - `crates/yarli-exec/src/runner.rs`

### `cargo clippy --workspace --all-targets -- -D warnings`
- Exit code: 101
- Key output: pre-existing strict lint failure in `crates/yarli-core/src/shutdown.rs`:
  - `error: very complex type used. Consider factoring parts into type definitions`
  - `clippy::type-complexity` on `capture_process_context` return tuple.

### `cargo test --workspace`
- Exit code: 0
- Key output:
  - New tests added for `compute_quality_gate` pass:
    - `plan::tests::compute_quality_gate_soft_token_cap_overrides_deterioration_report`
    - `plan::tests::compute_quality_gate_soft_token_cap_overrides_stable_report_blocking_policy`
  - `142` tests in `yarli_cli` and all workspace tests report as passing in this run.
