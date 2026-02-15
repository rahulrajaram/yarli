# CARD-R8-11 Verification Evidence

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
- = note: `-D warnings` with `clippy::type-complexity` is pre-existing in `yarli-core`
```

```text
Command: cargo test --workspace
Exit Code: 0
Key Output:
- All workspace tests passed.
- Representative suite:
  - `test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s`
  - `test result: ok. 159 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s`
  - `test result: ok. 140 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`
  - `test result: ok. 159 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s` (main binary tests)
  - `test result: ok. 88 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.76s`
  - `test result: ok. 44 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s`
```

## Summary

- `crates/yarli-cli/src/commands.rs` is now the command-handler home for extracted CLI command and parser logic.
- `crates/yarli-cli/src/main.rs` uses `mod commands;` with command routing preserved.
- `IncrementalEventCursor` helpers are exposed as `pub(crate)` to satisfy callsites in `main.rs` and `observers.rs`.
- Verification result is blocked in lint-only by an unrelated pre-existing `yarli-core` clippy warning; format and tests pass.
