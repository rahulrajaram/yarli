# CARD-R9-05 Evidence

Date: 2026-02-21  
Tranche: CARD-R9-05  
Target: Add soft token-cap checkpoint trigger

## Verification commands

Executed in `/home/rahul/Documents/worktree/yarli/run-019c81f0-6e16-7202-97c7-06628fd39290/001-tranche-005-card-r9-05`.

1. `cargo fmt --all -- --check`
- Exit: `1`
- Key output:
  - Numerous diffs under `crates/yarli-cli/src/stream/headless.rs` and `crates/yarli-cli/src/stream/renderer.rs`.
  - Diff includes many unrelated formatting changes outside this tranche.

2. `cargo clippy --workspace --all-targets -- -D warnings`
- Exit: `101`
- Key output:
  - `crates/yarli-core/src/shutdown.rs:485:33`
  - `error: very complex type used. Consider factoring parts into type definitions`
  - `clippy::type-complexity` (pre-existing lint failure in core crate).

3. `cargo test --workspace`
- Exit: `1`
- Key output:
  - Test suites mostly pass, including many `yarli_cli` cases and 1 warning (`observers.rs` dead code).
  - Failing test:
    - `tranche::tests::auto_complete_structured_tranche_marks_matching_key_using_prompt_root`
    - `called Result::unwrap() on Err { code: 2, message: "No such file or directory" }`
    - `crates/yarli-cli/src/tranche.rs:1727:49`  
      (environment-specific or pre-existing path resolution issue).

## Outcome

- `crates/yarli-cli/src/config.rs`
  - Added `run.soft_token_cap_ratio` config value with default and defaults integration.
  - Added config tests for default and parsed values.
- `crates/yarli-cli/src/commands.rs`
  - Continuation quality-gate calculation now accepts `run_total_tokens`, hard cap and soft ratio.
  - Soft-cap breach forces `TaskHealthAction::CheckpointNow` and blocks auto-advance.
- `crates/yarli-cli/src/plan.rs`
  - Added soft-cap unit tests for forced checkpoint and disabled conditions.
- `crates/yarli-cli/src/stream/headless.rs`
  - Added human-readable guidance output for `TaskHealthAction::CheckpointNow`.
- `crates/yarli-cli/src/stream/renderer.rs`
  - Rendered checkpoint guidance in continuation summary.
- `docs/CLI.md`, `yarli.example.toml`, `crates/yarli-cli/src/cli.rs`
  - Documented and exposed `run.soft_token_cap_ratio`.
