# CARD-R10-01A3 Evidence

Date: 2026-02-21

## Verification

### Command
- `cargo test -p yarli-cli`

### Result
- Exit code: 0 (PASS)
- Test summary:
  - `src/lib.rs`: 142 tests, all passed
  - `src/main.rs`: 231 tests, all passed
  - integration tests: `6 + 8 + 1` tests, all passed
- Warning: non-failing `dead_code` warning in `crates/yarli-cli/src/observers.rs` for `ObserverSignal::Failure.reason_bucket`.

## Notes
- Updated `crates/yarli-cli/src/projection.rs` to project `run.parallel_merge_failed` reasons into run status/explain data as `merge_finalization_blockers`.
- Updated `crates/yarli-cli/src/render.rs` to render merge-finalization blockers in:
  - `render_run_status`
  - `render_run_explain`
- Added regression tests in `crates/yarli-cli/src/render.rs` for both status and explain visibility:
  - `run_status_surfaces_merge_finalization_blockers`
  - `render_run_explain_surfaces_merge_finalization_blockers`
