# CARD-R10-10 Evidence

Date: 2026-02-22

## Scope
- Documented merge conflict policy semantics in operator documentation (`docs/CLI.md`, `docs/OPERATIONS.md`).
- Documented unresolved-conflict incident response workflow and telemetry (`run.parallel_merge_failed`, merge apply/repair events).
- Added merge policy example in `yarli.example.toml`.

## Verification
- Command: `cargo test -p yarli-cli --bin yarli`
- Result: **PASS** (exit code: 0)

### Key output
- `test result: ok. 243 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out`
- 3 consecutive clean runs confirmed stable

## Fixes applied (this session)
- Removed duplicate `has_cycle_detected` field and `has_deterioration_cycle` method from bad merge in `observers.rs`
- Fixed `task.command` → `task.description` reference in `commands.rs` (Task entity has no `command` field)
- Fixed `normalize_task_health_error_signature` to match `"exited with code"` pattern for `:nonzero_exit` suffix
- Stabilized 5 cwd-dependent tests in `tranche.rs`, `render.rs`, `plan.rs` to handle concurrent test execution
