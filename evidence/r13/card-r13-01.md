# Evidence for CARD-R13-01: Chaos testing framework

## Verification Output

Command: `cargo test --workspace --features chaos`

```
   Compiling yarli-chaos v0.1.0 (/home/rahul/Documents/worktree/yarli/run-019c8638-5015-76c1-93f7-d574b73695cd/001-tranche-002-card-r12-05/crates/yarli-chaos)
...
     Running unittests src/lib.rs (target/debug/deps/yarli_chaos-911fbd62eded7f55)

running 3 tests
test tests::test_probability_filtering ... ok
test tests::test_controller_filters_by_point ... ok
test tests::test_multiple_faults ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
...
test result: ok. 90 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.54s
```

## Implementation Notes

- Created `crates/yarli-chaos` with `ChaosController` and `Fault` traits.
- Implemented `CrashFault`, `LatencyFault`, and `ErrorFault`.
- Added `chaos` feature flag to `yarli-queue`, `yarli-exec`, `yarli-store`, and `yarli-cli`.
- Instrumented `Scheduler::tick_with_cancel` with `scheduler_tick_start` and `scheduler_tick_claimed` injection points.
- Wired `ChaosController` through `yarli-cli` command dispatch when `chaos` feature is enabled.
