# CARD-R11-05 Evidence

Date: 2026-02-22

## Scope
- Added cycle-latch tracking in `crates/yarli-cli/src/observers.rs`.
- Updated `build_continuation_payload` in `crates/yarli-cli/src/commands.rs` to:
  - detect previously persisted strategy-pivot checkpoints from prior continuation tasks,
  - switch to `TaskHealthAction::StopAndSummarize` when a deterioration cycle persists after one pivot,
  - preserve partial-work continuation while preventing duplicate pivot instructions.
- Added headless and TUI renderer guidance for `TaskHealthAction::StopAndSummarize` in:
  - `crates/yarli-cli/src/stream/headless.rs`
  - `crates/yarli-cli/src/stream/renderer.rs`
- Added new unit coverage in `crates/yarli-cli/src/commands.rs`:
  - `build_continuation_payload_force_pivot_appends_checkpoint_intervention`
  - `build_continuation_payload_after_previous_pivot_cycles_stops_and_summarizes`

## Verification
- Command: `cargo test -p yarli-cli`
- Result: **FAIL**
- Summary:
  - `142` unit tests passed in `crates/yarli-cli/src/lib.rs`.
  - `243` unit tests ran in `crates/yarli-cli/src/main.rs`.
  - Final: `240 passed`, `3 failed`, `0 ignored`.

### Key results
- New tranche tests passed:
  - `commands::tests::build_continuation_payload_force_pivot_appends_checkpoint_intervention ... ok`
  - `commands::tests::build_continuation_payload_after_previous_pivot_cycles_stops_and_summarizes ... ok`
- Pre-existing failing observers tests remain:
  - `observers::tests::normalize_error_signature_handles_backend_task_health`
  - `observers::tests::deterioration_detected_for_repeated_backend_failure_signatures`
  - `observers::tests::deterioration_detected_for_alternating_backend_failure_signatures`
- Representative fail output:
  - `test result: FAILED. 240 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.33s`
  - `assertion failed` lines in `crates/yarli-cli/src/observers.rs` comparing expected backend failure signatures.
