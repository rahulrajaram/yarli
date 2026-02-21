# CARD-R10-01D Evidence

- Date: 2026-02-21
- Objective: Add focused FSM regression coverage for merge-finalization non-completion behavior.

## Verification Command

- `cargo test -p yarli-cli`
- Result: FAIL

Failure: Rust compiler error in `crates/yarli-cli/src/workspace.rs`:
- `no method named context found for struct ParallelWorkspaceMergeApplyError`
- This blocks completion of this tranche's verification until the workspace error is resolved.

## Regression Checks Added

- `crates/yarli-cli/tests/parallel_merge_integration.rs::run_start_parallel_merge_conflict_preserves_unmerged_artifacts_and_escalates`
  - Added assertion that conflict-path command output does not include completed-terminal reporting (`RunCompleted`/`State: RunCompleted`) before merge-finalization.
  - Existing assertions already verify non-complete continuation outcome: `RunFailed` + `merge_conflict`.
