# Evidence for CARD-R13-02: Crash-recovery invariant tests

## Verification Output

### 2026-02-22 Workspace and card verification

Command: `cargo fmt --all -- --check`

```text
exit: 0
no diff found
```

Command: `cargo clippy --workspace --all-targets -- -D warnings`

```text
exit: 0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.30s
```

Command: `cargo test --workspace`

```text
exit: 0
no test failures reported
```

Command: `YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test --test crash_recovery --features chaos -- --nocapture`

```text
exit: 0
running 1 test
thread 'crash_recovery_invariant_postgres' panicked at crates/yarli-chaos/src/lib.rs:163:9:
CHAOS: Simulated scheduler crash after claim
...
test crash_recovery_invariant_postgres ... ok
```

## Implementation Notes

- Added event-driven recovery state replay in `tests/integration/tests/crash_recovery.rs` so recovery registry reconstruction now uses latest persisted run state from event stream (`run.activated`) instead of stale DB-only state.
- Added robust state/materialization helpers (`run_state_from_db`, `task_state_from_db`, `_to_db`/`from_db` mappers) and DB-backed sync helpers to keep durability assertions deterministic.
- Ensured seeded DB rows for run/tasks are stable for Postgres assertions, and recovery reconstruction uses those states/attempts to rehydrate scheduler registries.
- Added final reconciliation pass from event store back to DB (`sync_postgres_states_from_events`) before terminal assertions.
