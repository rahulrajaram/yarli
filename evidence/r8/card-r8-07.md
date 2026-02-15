# CARD-R8-07 Verification Evidence

## Command Results

```text
Command: cargo fmt --all -- --check
Exit Code: 0
Key Output:
- (no output)
- (no output)
```

```text
Command: cargo clippy --workspace --all-targets -- -D warnings
Exit Code: 101
Key Output:
- error: very complex type used. Consider factoring parts into `type` definitions
-    --> crates/yarli-core/src/shutdown.rs:485:33
-  fn capture_process_context() -> (
-      Option<u32>,
-      Option<u32>,
-      Option<u32>,
-      Option<u32>,
-      Option<String>,
-  ) {
-  }
-  = help: for further information visit https://rust-lang.github.io/rust-clippy/master/index.html#type-complexity
-  = note: `-D clippy::type-complexity` implied by `-D warnings`
```

```text
Command: cargo test --workspace
Exit Code: 101
Key Output:
- warning: constant `INIT_CONFIG_TEMPLATE` is never used
- warning: function `init_config_template` is never used
- warning: function `replace_between` is never used
- warning: function `cmd_init` is never used
- warning: function `load_runtime_config_for_reads` is never used
- warning: function `ensure_write_backend_guard` is never used
- warning: function `load_runtime_config_for_writes` is never used
- warning: function `prepare_audit_sink` is never used
- warning: function `with_event_store` is never used
- warning: function `with_event_store_and_queue` is never used
- warning: struct `CliInvocationConfig` is never constructed
- warning: struct `AutoAdvanceConfig` is never constructed
- warning: function `resolve_prompt_entry_path` is never used
- warning: `read_tranches_file` generated 2 warnings
- error: test failed, to rerun pass `-p yarli-cli --bin yarli`
- test tests::plan_driven_sequence_uses_tranches_file_near_resolved_plan_not_cwd ... FAILED
- test tests::structured_fallback_to_markdown ... FAILED
- failures:
- thread 'tests::plan_driven_sequence_uses_tranches_file_near_resolved_plan_not_cwd' panicked at crates/yarli-cli/src/main.rs:10182:52: called `Result::unwrap()` on an `Err` value: Os { code: 2, kind: NotFound, message: "No such file or directory" }
- thread 'tests::structured_fallback_to_markdown' panicked at crates/yarli-cli/src/main.rs:14893:52: called `Result::unwrap()` on an `Err` value: Os { code: 2, kind: NotFound, message: "No such file or directory" }
```

## Summary

- `CARD-R8-07` refactor moved planning APIs into `crates/yarli-cli/src/plan.rs` as implemented.
- `cargo fmt --all -- --check` passes.
- Verification is currently blocked by pre-existing workspace lint/test failures unrelated to formatting and by two `yarli-cli` plan tests that currently fail at runtime path handling in temporary-directory test setup.
