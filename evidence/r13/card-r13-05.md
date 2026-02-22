# Evidence for CARD-R13-05: Time-based failure scenarios

## 2026-02-22 Workspace verification command-level proof

### `cargo fmt --all -- --check`

```text
exit: 0
Formatting completed successfully.
```

### `cargo clippy --workspace --all-targets -- -D warnings`

```text
exit: 0
    Checking yarli-integration-tests v0.0.0 (...)
    Checking yarli-cli v0.1.0 (...)
Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.48s
```

### `cargo test --workspace`

```text
exit: 101
... 
Running tests/resource_exhaustion.rs
running 4 tests
test resource_exhaustion_disk_full_append_failure_is_recoverable ... FAILED
...

failures:
    resource_exhaustion_disk_full_append_failure_is_recoverable

thread 'resource_exhaustion_disk_full_append_failure_is_recoverable' panicked at tests/integration/tests/resource_exhaustion.rs:225:5:
recoverable append failure should not block completion

error: test failed, to rerun pass `-p yarli-integration-tests --test resource_exhaustion`
```

### Notes

- `time_based_failure_scenarios.rs` compiles and is included in this workspace run.
- The workspace-wide failure is in pre-existing `tests/integration/tests/resource_exhaustion.rs` and is unrelated to this tranche’s new assertions.

## 2026-02-22 Card-level targeted evidence

Command: `cargo test -p yarli-integration-tests --test time_based_failure_scenarios -- --nocapture`

```text
exit: 0
running 5 tests
skipping postgres integration test ...
test time_based_lease_reclaim_honors_clock_skew_boundaries_postgres ... ok
test time_based_smoke_no_unexpected_reclaim_after_clock_tamper ... ok
test concurrent_event_appends_keep_timestamp_ordering ... ok
test task_timeout_is_enforced_without_hanging ... ok
test heartbeat_extensions_prevent_stale_reclaim_under_contention ... ok
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.90s
```
