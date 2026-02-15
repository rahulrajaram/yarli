# Verification Evidence (018)

Generated at (UTC): `2026-02-15T23:11:58Z`

## Command Results

```text
Command: cargo fmt --all -- --check
Exit Code: 0
Key Output:
- (no output)
```

```text
Command: cargo clippy --workspace --all-targets -- -D warnings
Exit Code: 101
Key Output:
- error: very complex type used. Consider factoring parts into `type` definitions
- --> crates/yarli-core/src/shutdown.rs:485:33
- error: could not compile `yarli-core` (lib) due to 1 previous error
```

```text
Command: cargo test --workspace
Exit Code: 101
Key Output:
- warning: constant `INIT_CONFIG_TEMPLATE` is never used
- error[E0432]: unresolved import `yarli_core::domain::DeteriorationTrend`
- --> crates/yarli-cli/src/tranche.rs:489:9
- error[E0422]: cannot find struct, variant or union type `PlannedTranche` in this scope
- --> crates/yarli-cli/src/tranche.rs:687:25
- error: could not compile `yarli-cli` (bin "yarli" test) due to 14 previous errors; 12 warnings emitted
```

## Decision

- Result: `UNVERIFIED`
- Failure count: `2`
