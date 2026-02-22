# Evidence for CARD-R13-01: Chaos testing framework

## Verification Output

### 2026-02-22 Workspace verification run (legacy snapshot)

Command: `cargo fmt --all -- --check`

```text
exit: 0
no diff found
```

Command: `cargo clippy --workspace --all-targets -- -D warnings`

```text
exit: 0
    Blocking waiting for file lock on package cache
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.17s
```

Command: `cargo test --workspace`

```text
exit: 0
no test failures reported
```

Command: `cargo test --workspace --features chaos`

```text
exit: 0
no test failures reported
```

### 2026-02-22 Tranche 1/29 re-verification (workspace + chaos)

Command: `cargo fmt --all -- --check`

```text
exit: 0
no diff found
```

Command: `cargo clippy --workspace --all-targets -- -D warnings`

```text
exit: 0
    Blocking waiting for file lock on package cache
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.93s
```

Command: `cargo test --workspace`

```text
exit: 0
no test failures reported
```

Command: `cargo test --workspace --features chaos`

```text
exit: 0
no test failures reported
```

## Implementation Notes

- Created `crates/yarli-chaos` with `ChaosController` and `Fault` traits.
- Implemented `CrashFault`, `LatencyFault`, and `ErrorFault`.
- Added `chaos` feature flag to `yarli-queue`, `yarli-exec`, `yarli-store`, and `yarli-cli`.
- Instrumented `Scheduler::tick_with_cancel` with `scheduler_tick_start` and `scheduler_tick_claimed` injection points.
- Wired `ChaosController` through `yarli-cli` command dispatch when `chaos` feature is enabled.
