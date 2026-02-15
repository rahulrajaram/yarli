# CARD-R8-14 Verification Evidence

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
  - crates/yarli-core/src/shutdown.rs:485:33 (`capture_process_context`)
- note: failure is pre-existing and outside persistence-extraction scope (`yarli-cli` unchanged in this area)
```

```text
Command: cargo test --workspace
Exit Code: 0
Key Output:
- workspace test suite completed successfully
- 159 tests passed in yarli-cli unit tests
- multiple integration tests passed, including PostgreSQL-backed checks
- only non-fatal warnings surfaced (unused imports/items in multiple crates)
```
