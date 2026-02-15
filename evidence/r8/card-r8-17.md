# CARD-R8-17 Verification Evidence

Date: 2026-02-15

## Commands run

### 1) `cargo fmt --all -- --check`

```text
Exit code: 0
```

Notes:
- Formatting check completed successfully after wiring-only refactor of `main.rs` and helper move adjustments in `commands.rs`.

### 2) `cargo clippy --workspace --all-targets -- -D warnings`

```text
error: very complex type used. Consider factoring parts into `type` definitions
   --> crates/yarli-core/src/shutdown.rs:485:33
...
error: could not compile `yarli-core` (lib) due to 1 previous error
Exit code: 101
```

Notes:
- Failure remains in unrelated crate `yarli-core`, unchanged relative to previous tranches.

### 3) `cargo test --workspace`

```text
error[E0432]: unresolved import `yarli_core::domain::DeteriorationTrend`
   --> crates/yarli-cli/src/tranche.rs:489:9
...
error[E0422]: cannot find struct, variant or union type `PlannedTranche` in this scope
   --> crates/yarli-cli/src/tranche.rs:687:25
...
error: could not compile `yarli-cli` (bin "yarli") due to 14 previous errors
Exit code: 101
```

Notes:
- `cargo test --workspace` fails in `crates/yarli-cli/src/tranche.rs` on unresolved symbols and types that pre-exist in this tranche run context.
