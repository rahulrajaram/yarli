# Evidence for CARD-R13-03: Concurrency stress tests

## Verification Output

### 2026-02-22 Workspace verification run

Command: `cargo fmt --all -- --check`

```text
exit: 0
no diff found
```

Command: `cargo clippy --workspace --all-targets -- -D warnings`

```text
exit: 0
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.24s
```

Command: `cargo test --workspace`

```text
exit: 0
running 2 tests
test concurrency_stress_multi_worker_claim_contention ... ok
test concurrency_stress_heartbeat_reclaim_cancellation ... ok
test result: ok. 0 failed; 0 ignored
```

## Implementation Notes

- Added `tests/integration/tests/concurrency_stress.rs` with two race-sensitive stress tests:
  - multi-worker claim contention under high parallelism,
  - stale heartbeat reclaim combined with cancellation and cancellation accounting.
- Updated the reclaim test to deterministically observe at least one stale sweep before active workers begin completion.
