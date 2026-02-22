# Evidence for CARD-R13-04: Resource exhaustion handling

## Verification Output

### 2026-02-22 Workspace verification run

Command: `cargo test --workspace`

```text
exit: 0
running 7 tests
....
running 1 test
.
running 3 tests
...
running 142 tests
.... (output includes scheduler and stream transitions)
... (all suites completed)
exit: 0
all tests passed
```

## 2026-02-22 Command-level verification

```text
$ cargo fmt --all -- --check
cargo fmt --all -- --check: OK

$ cargo clippy --workspace --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.12s
cargo clippy --workspace --all-targets -- -D warnings: OK

$ cargo test --workspace
... (same full-suite pass; no failing tests)
cargo test --workspace: OK
```

## Implementation Notes

- Added `tests/integration/tests/resource_exhaustion.rs` with coverage for:
  - Disk-exhaustion handling by failing event-log append once and verifying scheduler recovers after the temporary failure.
  - Memory pressure handling via synthetic command execution results that exceed `max_task_rss_bytes`.
  - Queue no-claim backpressure by forcing `claim_batch_size = 0`.
  - Postgres connection pool exhaustion and recovery by holding the only pool connection and then releasing it.
