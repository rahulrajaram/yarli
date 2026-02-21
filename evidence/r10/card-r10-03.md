# CARD-R10-03 Evidence: Surface merge conflict diagnostics in status and explain-exit

- Contract: show merge conflict details, conflicted files, and recovery hints in `run status` and `run explain-exit` whenever merge finalization is blocked by unresolved conflicts.
- Completion status: IMPLEMENTED IN CODE, UNVERIFIED DUE TO FAILED `cargo test -p yarli-cli`.

## Verification command

```bash
cargo test -p yarli-cli
```

### Result
- **Fail** (exit code `101`)
- 374 tests executed, `5 passed` and `1 failed` in `tests/parallel_merge_integration.rs` integration binary.
- Failing test: `run_start_parallel_merge_conflict_preserves_unmerged_artifacts_and_escalates`
  - Assertion failure: expected continuation `exit_state` `RunFailed`, got `RUN_FAILED`.
- Relevant pass signals:
  - `render::tests::run_status_surfaces_merge_finalization_blockers` passed.
  - `render::tests::run_explain_surfaces_merge_finalization_blockers` passed.
