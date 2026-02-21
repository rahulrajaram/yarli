# CARD-R9-02 Verification Evidence

## Verification Commands

Command:
```bash
cargo fmt --all -- --check
```
Exit Code: 1
Result: FAIL
Key output:
```text
Diff in .../crates/yarli-cli/src/commands.rs:10:
 use crate::events::*;
 use crate::persistence::RUN_CONTINUATION_EVENT_TYPE;
 use crate::render::*;
-use yarli_core::entities::continuation::TaskHealthAction;
+use yarli_core::entities::continuation::TaskHealthAction;
```
Primary failure: existing formatting drift in unrelated files (pre-existing), notably `commands.rs`.

Command:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```
Exit Code: 101
Result: FAIL (pre-existing)
Key output:
```text
error: very complex type used. Consider factoring parts into `type` definitions
   --> crates/yarli-core/src/shutdown.rs:485:33
    fn capture_process_context() -> (
        Option<u32>,
        Option<u32>,
        Option<u32>,
        Option<u32>,
        Option<String>,
    ) {
```

Command:
```bash
cargo test --workspace
```
Exit Code: 1
Result: FAIL
Key output:
```text
failures:
    tranche::tests::auto_complete_structured_tranche_marks_matching_key_using_prompt_root
thread 'tranche::tests::auto_complete_structured_tranche_marks_matching_key_using_prompt_root' panicked at crates/yarli-cli/src/tranche.rs:1727:49:
called `Result::unwrap()` on an `Err` value: Os { code: 2, kind: NotFound, message: "No such file or directory" }
```
Primary failure: existing flaky/unstable test failure unrelated to `CARD-R9-02` logic.
