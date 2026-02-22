# Evidence for CARD-R14-02: Database migration tooling

## 2026-02-22 Command-level verification

### `cargo fmt --all -- --check`

```text
exit: 0
```

### `cargo clippy --workspace --all-targets -- -D warnings`

```text
exit: 0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.52s
```

### `cargo test --workspace`

```text
exit: 101
test resource_exhaustion_postgres_connection_pool_exhaustion_recovery ... ok
test resource_exhaustion_queue_no_workers_can_claim ... ok
test resource_exhaustion_disk_full_append_failure_is_recoverable ... FAILED
test resource_exhaustion_memory_pressure_budget_exceeded ... ok

failures:

---- resource_exhaustion_disk_full_append_failure_is_recoverable stdout ----

thread 'resource_exhaustion_disk_full_append_failure_is_recoverable' panicked at tests/integration/tests/resource_exhaustion.rs:225:5:
recoverable append failure should not block completion
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

failures:
    resource_exhaustion_disk_full_append_failure_is_recoverable
test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
error: test failed, to rerun pass `-p yarli-integration-tests --test resource_exhaustion`
```

## Notes

- `crates/yarli-cli/src/cli.rs`: enabled `migrate` subcommand action variants and dispatch wiring.
- `crates/yarli-cli/src/main.rs`: wired `migrate` command handling.
- `crates/yarli-cli/src/commands.rs`: added migration status/up/down/backup/restore flows, migration lock + metadata/bootstrap helpers, and SQL execution utilities.
- `crates/yarli-store/src/lib.rs`: added `MIGRATION_0001_DOWN` / `MIGRATION_0002_DOWN` constants.
- `docs/CLI.md`, `docs/OPERATIONS.md`: updated migration guidance to command-first workflow.
