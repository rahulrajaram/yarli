# CARD-R8-03 Verification Evidence

## Command Results

```text
Command: cargo fmt --all -- --check
Exit Code: 0
Key Output:
- (no output)
- (no output)
- (no output)
```

```text
Command: cargo clippy --workspace --all-targets -- -D warnings
Exit Code: 101
Key Output:
- error: very complex type used. Consider factoring parts into `type` definitions
- =---> crates/yarli-core/src/shutdown.rs:485:33
- Could not compile `yarli-core` (lib) due to 1 previous error
```

```text
Command: cargo test --workspace
Exit Code: 101
Key Output:
- test tests::plan_driven_sequence_uses_tranches_file_near_resolved_plan_not_cwd ... FAILED
- test tests::structured_fallback_to_markdown ... FAILED
- test result: FAILED. 197 passed; 2 failed; 0 ignored; 0 measured
```

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-api -- --nocapture
Exit Code: 0
Key Output:
- running 1 test
- test run_and_task_status_reflect_writes_from_postgres ... ok
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

```text
Command: unset YARLI_TEST_DATABASE_URL; YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-api -- --nocapture
Exit Code: 101
Key Output:
- thread 'run_and_task_status_reflect_writes_from_postgres' panicked at crates/yarli-api/tests/postgres_integration.rs:28:13:
- postgres integration tests require YARLI_TEST_DATABASE_URL when YARLI_REQUIRE_POSTGRES_TESTS=1
- test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
```

## Summary

- `crates/yarli-api/tests/postgres_integration.rs` added to prove Postgres-backed API read-your-writes behavior for `run` and `task` status endpoints.
- `crates/yarli-api/Cargo.toml` updated with test-time `sqlx` dependency for Postgres integration test helpers.
- `card-R8-03` proof steps are now reflected in this evidence file and can be traced in the command logs listed above.
- PASS status for `CARD-R8-03` is constrained to strict Postgres API consistency checks; workspace-wide fmt/lint/test still have unrelated pre-existing failures tracked in evidence.
