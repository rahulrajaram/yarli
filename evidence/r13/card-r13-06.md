# Evidence for CARD-R13-06: Long-running stability tests

## 2026-02-22 Command-level verification

### `cargo fmt --all -- --check`

```text
exit: 0
```

### `cargo clippy --workspace --all-targets -- -D warnings`

```text
exit: 0
```

### `cargo test --workspace`

```text
exit: 1

failed tests:
    resource_exhaustion_disk_full_append_failure_is_recoverable
    
panic message:
    recoverable append failure should not block completion
    
location:
    tests/integration/tests/resource_exhaustion.rs:225:5
```

## Notes

- Added `tests/integration/tests/long_running_stability.rs` with:
  - Long-running continuous submission/completion soak test using in-memory queue/store, including bounded resource sampling for file descriptor and memory-page deltas.
  - Postgres connection stability smoke test under sustained short-run load (skipped unless `YARLI_TEST_DATABASE_URL` is configured).
  - Assertions for run/task terminalization and queue cleanup after load (no remaining leased/pending queue entries when workloads complete).
- `IMPLEMENTATION_PLAN.md` updated with `CARD-R13-06` completion status and evidence reference.
- Target suite result:
  - `tests/integration/tests/long_running_stability.rs`: 2 passed.
