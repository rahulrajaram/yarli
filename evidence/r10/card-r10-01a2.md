# CARD-R10-01A2 Evidence

Date: 2026-02-21

## Verification

### Command
- `cargo test -p yarli-cli`

### Result
- Exit code: 0 (PASS)
- Test summary: `running 142 tests` (`src/lib.rs`), `running 229 tests` (`src/main.rs`), integration suites `6 + 8 + 1` tests, all passed.
- Warnings: non-failing `dead_code` warning in `crates/yarli-cli/src/observers.rs` for `ObserverSignal::Failure.reason_bucket`.

## Notes
- Updated `cmd_run_start_with_backend` in `crates/yarli-cli/src/commands.rs` so non-completed parallel runs now print actionable recovery paths:
  - `Parallel workspace root preserved for recovery: <path>`
  - `Failed/unfinished task workspace paths:` with task key/state and workspace path entries.
- Cleanup behavior for completed runs and merge-failure runs remains unchanged (merge/failure handling still controls root retention).
