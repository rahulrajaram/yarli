# CARD-R10-01A1 Evidence

Date: 2026-02-21

## Verification

### Command
- `cargo test -p yarli-core`

### Result
- Exit code: 0 (PASS)
- Test summary: `running 124 tests` and `test result: ok. 124 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out`.

## Notes
- Updated run completion flow in `crates/yarli-cli/src/commands.rs` to avoid returning `RunCompleted` when parallel merge finalization fails.
- On merge finalization failure, CLI now persists continuation payload with:
  - `exit_state = RunFailed`
  - `exit_reason = failed_runtime_error`
  - `next_tranche = None`
- Merge failure is still logged and event persisted as `run.parallel_merge_failed`.
