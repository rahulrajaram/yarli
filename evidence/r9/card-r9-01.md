# CARD-R9-01 Verification Evidence

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
...
```
Primary failure: formatting is needed in `commands.rs` and multiple existing files that were not introduced by this tranche.

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
Exit Code: 0
Result: PASS
Key output:
```text
test result: ok. 0 failed; 0 ignored; 0 measured; 0 filtered out
```
