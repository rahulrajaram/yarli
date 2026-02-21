# CARD-R9-04 Evidence

Date: 2026-02-21  
Tranche: CARD-R9-04  
Target: Inject forced-pivot guidance after deterioration

## Verification commands

Executed in `/home/rahul/Documents/worktree/yarli/run-019c81ed-a024-7160-b784-c6b316b27c31/001-tranche-004-card-r9-04`.

1. `cargo fmt --all -- --check`
- Exit: `1`
- Key output:
  - `Diff in crates/yarli-cli/src/stream/headless.rs:176`
  - `Diff in crates/yarli-cli/src/stream/renderer.rs:480`
  - Output also shows formatting differences in unrelated files outside this tranche (pre-existing).

2. `cargo clippy --workspace --all-targets -- -D warnings`
- Exit: `101`
- Key output:
  - `crates/yarli-core/src/shutdown.rs:485:33`
  - `error: very complex type used. Consider factoring parts into type definitions`
  - `clippy::type-complexity` on `capture_process_context` return tuple.

3. `cargo test --workspace`
- Exit: `0`
- Key output:
  - `test result: ok. 6 passed; 0 failed` (`yarli_api`)
  - `test result: ok. 1 passed; 0 failed` (`postgres_integration`)
  - `test result: ok. 142 passed; 0 failed` (`yarli_cli`)
  - `warning: field reason_bucket is never read` in `crates/yarli-cli/src/observers.rs` (pre-existing warning).

## Outcome

- `crates/yarli-cli/src/stream/renderer.rs`
  - Continuation summary now conditionally emits force-pivot guidance only when deterioration trend is present.
  - Added helper `force_pivot_guidance` branch to output:
    - `sequence quality is deteriorating; narrow scope and shift task focus before continuing.`
- `crates/yarli-cli/src/stream/headless.rs`
  - Run-exit output now prints the same forced-pivot guidance for headless mode under the same condition.
- `docs/CLI.md`
  - Added note that forced-pivot continuation guidance is emitted on run completion when applicable.
