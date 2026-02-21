# CARD-R9-08 Evidence: Document Runtime Guard Telemetry and Operator Playbook

## Scope
- Added operator-facing telemetry and playbook guidance for runtime guard behavior.
- Updated:
  - `docs/OPERATIONS.md`
  - `docs/CLI.md`
  - `IMPLEMENTATION_PLAN.md`

## Verification Commands

### 1) `cargo fmt --all -- --check`
- Exit code: `1`
- Result: Formatting check failed (pre-existing repository formatting debt across multiple files).

### 2) `cargo clippy --workspace --all-targets -- -D warnings`
- Exit code: `1`
- Result: Fails on pre-existing warning-as-error in `crates/yarli-core/src/shutdown.rs:485` (type complexity warning).

### 3) `cargo test --workspace`
- Exit code: `0`
- Result: All tests passed for workspace.

### 4) `cargo test -p yarli-cli` (tranche-required)
- Exit code: *No terminal completion captured* (command remained active with multiple hanging `parallel_merge_integration` subprocesses in `futex_wait_queue` after ~6+ minutes).
- Result: Rerun was stopped to avoid indefinite blocking; required command did not produce a pass/fail result in this session.
