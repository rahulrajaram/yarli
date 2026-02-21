# CARD-R10-01B Evidence

## Status
- Target: map unresolved parallel merge apply conflicts to explicit `merge_conflict` termination semantics.
- Date: 2026-02-21

## Evidence
- Verification command: `cargo test -p yarli-core`
- Result: passed
- Files changed:
  - `crates/yarli-core/src/domain.rs`
  - `crates/yarli-cli/src/commands.rs`
  - `crates/yarli-cli/src/workspace.rs`
  - `crates/yarli-cli/tests/parallel_merge_integration.rs`
  - `IMPLEMENTATION_PLAN.md`

### Observed behavior
- Parallel merge apply failure failures now classify as either conflict/runtime.
- Merge-conflict failures keep runs in `RunFailed` with `exit_reason=merge_conflict` and block continuation.
- Regression assertion added in `parallel_merge_integration` validates continuation payload contains:
  - `exit_state: RunFailed`
  - `exit_reason: merge_conflict`
