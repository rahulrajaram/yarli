# CARD-R8-02 Verification Evidence

## Command Results

```text
Command: cargo test -p yarli-api
Exit Code: 0
Key Output:
- running 6 tests
- test server::tests::task_status_endpoint_returns_not_found_for_unknown_task ... ok
- test server::tests::task_status_endpoint_rejects_invalid_task_id ... ok
- test server::tests::health_endpoint_returns_ok ... ok
- test server::tests::run_status_endpoint_replays_persisted_events ... ok
- test server::tests::task_status_endpoint_replays_persisted_task_events ... ok
- test server::tests::run_status_endpoint_returns_not_found_for_unknown_run ... ok
- test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

```text
Command: cargo test --workspace
Exit Code: 0
Key Output:
- Running all workspace tests after adding crates/yarli-api to workspace members
- test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
- test result: ok. 159 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
- test result: ok. 88 passed; 0 failed; 0 measured; 0 ignored; 0 filtered out
```

```text
Command: cargo fmt --all -- --check
Exit Code: 1
Key Output:
- Diff in crates/yarli-api/src/lib.rs: pub use server::{ ... TaskStatusResponse, TaskStatusSummary }
- Additional pre-existing workspace diffs reported in crates/yarli-api/cli and crates/yarli-cli.
```

```text
Command: cargo clippy --workspace --all-targets -- -D warnings
Exit Code: 1
Key Output:
- crates/yarli-core/src/shutdown.rs:485:33
  - clippy::type_complexity for tuple-return function signature
  - Could not compile yarli-core under -D warnings
```

```text
Command: rg -n "not found|invalid|status|task|run" crates/yarli-api/src/server.rs docs/API_CONTRACT.md
Exit Code: 0
Key Output:
- docs/API_CONTRACT.md includes /v1/tasks/{task_id} contract row
- crate/server.rs includes task_status handler and associated ApiError variants
- run/status routing and task/status routing are present in the server
```
