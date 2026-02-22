# Evidence for CARD-R12-05: Performance profiling instrumentation

## Verification Output

Command: `cargo test --workspace`

```
   Compiling yarli-queue v0.1.0 (/home/rahul/Documents/worktree/yarli/run-019c8638-5015-76c1-93f7-d574b73695cd/001-tranche-002-card-r12-05/crates/yarli-queue)
   Compiling yarli-cli v0.1.0 (/home/rahul/Documents/worktree/yarli/run-019c8638-5015-76c1-93f7-d574b73695cd/001-tranche-002-card-r12-05/crates/yarli-cli)
   Compiling yarli-api v0.1.0 (/home/rahul/Documents/worktree/yarli/run-019c8638-5015-76c1-93f7-d574b73695cd/001-tranche-002-card-r12-05/crates/yarli-api)
   Compiling yarli-sw4rm v0.1.0 (/home/rahul/Documents/worktree/yarli/run-019c8638-5015-76c1-93f7-d574b73695cd/001-tranche-002-card-r12-05/crates/yarli-sw4rm)
   Compiling yarli-integration-tests v0.0.0 (/home/rahul/Documents/worktree/yarli/run-019c8638-5015-76c1-93f7-d574b73695cd/001-tranche-002-card-r12-05/tests/integration)
warning: unused variable: `run_id`
   --> crates/yarli-cli/tests/parallel_merge_integration.rs:626:9
    |
626 |     let run_id = parse_run_id(&run_stdout).expect("failed to parse run ...
    |         ^^^^^^ help: if this is intentional, prefix it with an underscore: `_run_id`
    |
    = note: `#[warn(unused_variables)]` on by default

warning: unused import: `Registry`
 --> crates/yarli-cli/src/commands.rs:9:42
  |
9 | use yarli_observability::{AuditCategory, Registry, YarliMetrics};
  |                                          ^^^^^^^^
  |
  = note: `#[warn(unused_imports)]` on by default

warning: `yarli-cli` (test "parallel_merge_integration") generated 1 warning
warning: `yarli-cli` (bin "yarli" test) generated 1 warning (1 duplicate)
warning: `yarli-cli` (bin "yarli") generated 1 warning (run `cargo fix --bin "yarli"` to apply 1 suggestion)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 14.52s
     Running unittests src/lib.rs (target/debug/deps/yarli_api-7aecf81f72cc5ec8)

running 7 tests
test server::tests::task_status_endpoint_rejects_invalid_task_id ... ok
test server::tests::run_status_endpoint_returns_not_found_for_unknown_run ... ok
test server::tests::metrics_endpoint_returns_prometheus_format ... ok
test server::tests::health_endpoint_returns_ok ... ok
test server::tests::task_status_endpoint_replays_persisted_task_events ... ok
test server::tests::task_status_endpoint_returns_not_found_for_unknown_task ... ok
test server::tests::run_status_endpoint_replays_persisted_events ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/postgres_integration.rs (target/debug/deps/postgres_integration-aaca502dec86ec58)

running 1 test
test run_and_task_status_reflect_writes_from_postgres ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/yarli_cli-999d6dae5f16d945)

running 142 tests
...
test result: ok. 142 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.62s

     Running unittests src/lib.rs (target/debug/deps/yarli_core-6825227747764b8e)

running 101 tests
...
test result: ok. 101 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running unittests src/lib.rs (target/debug/deps/yarli_exec-8b32782782782782)

running 12 tests
...
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running unittests src/lib.rs (target/debug/deps/yarli_gates-7282782782782782)

running 16 tests
...
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/yarli_git-8278278278278278)

running 7 tests
...
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/yarli_memory-8278278278278278)

running 6 tests
...
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/yarli_observability-f46770ed06d33651)

running 47 tests
test metrics::tests::command_duration_histogram ... ok
test metrics::tests::scheduler_tick_duration_histogram ... ok
test metrics::tests::command_overhead_duration_histogram ... ok
test metrics::tests::store_metrics ... ok
test metrics::tests::full_encode_includes_all_metric_names ... ok
...
test result: ok. 47 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/yarli_policy-43d34ac84a2bfb75)

running 46 tests
...
test result: ok. 46 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/yarli_queue-df706779593610b9)

running 90 tests
...
test result: ok. 90 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.55s

     Running unittests src/lib.rs (target/debug/deps/yarli_store-d32d049c6fb6204e)

running 28 tests
...
test result: ok. 28 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running unittests src/lib.rs (target/debug/deps/yarli_sw4rm-1827d8b394acf752)

running 47 tests
...
test result: ok. 47 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s
```

## Implementation Notes

- Added `YarliMetrics` fields:
  - `yarli_scheduler_tick_duration_seconds` (Histogram, by stage: `scan`, `claim`, `heartbeat`, `reclaim`)
  - `yarli_command_overhead_duration_seconds` (Histogram, by class + phase: `spawn`, `resource_capture_init`)
  - `yarli_store_duration_seconds` (Histogram, by operation: `append`, `get`, `query`, `all`, `len`)
  - `yarli_store_slow_queries` (Counter, by operation, >1s duration)
- Instrumented `PostgresEventStore` with optional metrics injection.
- Instrumented `Scheduler::tick_with_cancel` to record stage durations.
- Instrumented `LocalCommandRunner` to record spawn and capture overhead.
- Updated `execute_run_plan` and `cmd_run_start_with_backend` to propagate the metrics registry.
