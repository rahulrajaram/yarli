# CARD-R10-04 Evidence: Add explicit merge conflict policy modes

- Contract: support `fail`, `manual`, and `auto-repair` merge conflict policies with deterministic behavior and clear invalid-value validation.
- Completion status: implemented in code and recorded in plan.

## Verification command

```bash
cargo test -p yarli-cli
```

### Result
- **Fail** (`exit code 101`)
- `232` library tests and `233` binary tests passed in `yarli-cli` unit suites, `5` integration tests passed and `1` integration test failed in `crates/yarli-cli/tests/parallel_merge_integration.rs`.
- Failing integration test:
  - `run_start_parallel_merge_conflict_preserves_unmerged_artifacts_and_escalates`
    - Assertion: expected continuation `exit_state` `RunFailed`; observed `RUN_FAILED`.
