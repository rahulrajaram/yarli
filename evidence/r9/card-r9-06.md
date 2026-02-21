# CARD-R9-06 Evidence

Date: 2026-02-21
Card target: Checkpoint prompt contract and verify hook

## Commands run

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`

## Command results

### `cargo fmt --all -- --check`
Exit code: 1
Result: style check reported formatting diffs in unrelated existing files:

- `crates/yarli-cli/src/stream/headless.rs`
- `crates/yarli-cli/src/stream/renderer.rs`
- `crates/yarli-cli/src/commands.rs`
- `crates/yarli-cli/src/config.rs`
- `crates/yarli-cli/src/observers.rs`
- `crates/yarli-exec/src/runner.rs`
- `crates/yarli-queue/src/scheduler.rs`
- plus other non-tranche files

### `cargo clippy --workspace --all-targets -- -D warnings`
Exit code: 101
Result: fail due pre-existing lint in `crates/yarli-core/src/shutdown.rs`:

```
error: very complex type used. Consider factoring parts into `type` definitions
  --> crates/yarli-core/src/shutdown.rs:485:33
```

### `cargo test --workspace`
Exit code: 101
Result: mostly green; one failing test in `crates/yarli-cli`:

- `tranche::tests::auto_complete_structured_tranche_marks_matching_key_using_prompt_root`
- Failure: `No such file or directory` reading expected fixture path (`Os { code: 2 ... }`).

All tranche-facing changes introduced for CARD-R9-06 currently compile and associated new tests pass (`plan::tests::plan_driven_tranche_prompt_includes_contract_fields`, `plan::tests::tranches_file_to_entries_preserves_contract_fields`, etc.), but full workspace verification is blocked by pre-existing/facility failures above.
