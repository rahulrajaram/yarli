# CARD-R8-09 Verification Evidence

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
-   --> crates/yarli-core/src/shutdown.rs:485:33
- fn capture_process_context() -> (
-     Option<u32>, Option<u32>, Option<u32>, Option<u32>, Option<String>,
- )
-     = help: `clippy::type-complexity`
```

```text
Command: cargo test --workspace
Exit Code: 0
Key Output:
- (warnings only)
- warning: constant `INIT_CONFIG_TEMPLATE` is never used
- warning: `yarli-cli` (lib) generated 27 warnings
- warning: `yarli-cli` (bin "yarli" test) generated 5 warnings
- warning: `yarli-cli` (bin "yarli") generated 4 warnings
- test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
- test result: ok. 159 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
- test result: ok. 140 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
- test result: ok. 88 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.54s
- test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
- test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
- test result: ok. 28 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
- test result: ok. 44 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
- test result: ok. 46 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## Summary

- `CARD-R8-09` projection extraction is implemented in:
  - `crates/yarli-cli/src/main.rs` (delegation/module wiring updated)
  - `crates/yarli-cli/src/projection.rs` (new projection module)
- `cargo fmt --all -- --check` passes.
- `cargo test --workspace` passes.
- `cargo clippy --workspace --all-targets -- -D warnings` fails on a pre-existing `yarli-core` `clippy::type-complexity` issue unrelated to this card.

