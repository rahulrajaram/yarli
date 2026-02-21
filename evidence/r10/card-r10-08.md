# CARD-R10-08 Evidence: Harden fail/manual behavior and continuation semantics

- Contract: unresolved parallel merge-conflict finalization blocks completion with `merge_conflict` exit state and reasons, while keeping continuation/explain surfaces aligned with unresolved merge artifacts.
- Primary verifier: `cargo test -p yarli-cli`

## Verification command

```bash
cargo test -p yarli-cli
```

### Result
- **FAIL** (command timed out before integration completion)
- `142` tests passed; `0` failed in crate unit suite.
- `234` tests passed; `0` failed in CLI unit suite.
- `tests/parallel_merge_integration.rs` stalled in `run_start_parallel_merge_applies_committed_workspace_changes` and did not finish within timeout.
