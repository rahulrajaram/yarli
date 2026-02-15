# CARD-R8-06 Verification Evidence

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
-     Checking yarli-core v0.1.0 (/home/rahul/Documents/worktree/yarli/run-019c6353-2bc1-73f0-a017-99f3646f104d/001-tranche-006-card-r8-06/crates/yarli-core)
- error: very complex type used. Consider factoring parts into `type` definitions
-    --> crates/yarli-core/src/shutdown.rs:485:33
-     |
- 485 |   fn capture_process_context() -> (
-     |  _________________________________^
- 486 | |     Option<u32>,
- 487 | |     Option<u32>,
- 488 | |     Option<u32>,
- 489 | |     Option<u32>,
- 490 | |     Option<String>,
- 491 | | ) {
-     | |_^
-     |
-     = help: for further information visit https://rust-lang.github.io/rust-clippy/master/index.html#type_complexity
-     = note: `-D clippy::type-complexity` implied by `-D warnings`
-     = help: to override `-D warnings` add `#[allow(clippy::type_complexity)]`
- 
- error: could not compile `yarli-core` (lib) due to 1 previous error
- warning: build failed, waiting for other jobs to finish...
- error: could not compile `yarli-core` (lib test) due to 1 previous error
```

```text
Command: cargo test --workspace
Exit Code: 0
Key Output:
-    Compiling yarli-cli v0.1.0 (/home/rahul/Documents/worktree/yarli/run-019c6353-2bc1-73f0-a017-99f3646f104d/001-tranche-006-card-r8-06/crates/yarli-cli)
- warning: constant `INIT_CONFIG_TEMPLATE` is never used
-    --> crates/yarli-cli/src/cli.rs:186:7
-     |
- 186 | const INIT_CONFIG_TEMPLATE: &str = r#"# YARLI runtime configuration
-     |       ^^^^^^^^^^^^^^^^^^^^
-     |
-     = note: `#[warn(dead_code)]` on by default
- 
- warning: function `init_config_template` is never used
-    --> crates/yarli-cli/src/cli.rs:368:15
-     |
- 368 | pub(crate) fn init_config_template(backend: Option<InitBackend>) -> String {
-     |               ^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `replace_between` is never used
-    --> crates/yarli-cli/src/cli.rs:452:15
-     |
- 452 | pub(crate) fn replace_between(haystack: &str, begin: &str, end: &str, replacement: &str) -> String {
-     |               ^^^^^^^^^^^^^^^
- 
- warning: function `cmd_init` is never used
-   --> crates/yarli-cli/src/config.rs:18:15
-    |
- 18 | pub(crate) fn cmd_init(
-    |               ^^^^^^^^
- 
- warning: function `load_runtime_config_for_reads` is never used
-   --> crates/yarli-cli/src/config.rs:53:15
-    |
- 53 | pub(crate) fn load_runtime_config_for_reads() -> Result<LoadedConfig> {
-    |               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `ensure_write_backend_guard` is never used
-   --> crates/yarli-cli/src/config.rs:57:15
-    |
- 57 | pub(crate) fn ensure_write_backend_guard(
-    |               ^^^^^^^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `load_runtime_config_for_writes` is never used
-   --> crates/yarli-cli/src/config.rs:73:15
-    |
- 73 | pub(crate) fn load_runtime_config_for_writes(command_name: &str) -> Result<LoadedConfig> {
-    |               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `prepare_audit_sink` is never used
-   --> crates/yarli-cli/src/config.rs:79:15
-    |
- 79 | pub(crate) fn prepare_audit_sink(loaded_config: &LoadedConfig) -> Result<Option<JsonlAuditSink>> {
-    |               ^^^^^^^^^^^^^^^^^^
- 
- warning: function `with_event_store` is never used
-   --> crates/yarli-cli/src/config.rs:96:15
-    |
- 96 | pub(crate) fn with_event_store<T>(
-    |               ^^^^^^^^^^^^^^^^
- 
- warning: function `with_event_store_and_queue` is never used
-    --> crates/yarli-cli/src/config.rs:113:15
-     |
- 113 | pub(crate) fn with_event_store_and_queue<T>(
-     |               ^^^^^^^^^^^^^^^^^^^^^^^^^^
- 
- warning: struct `CliInvocationConfig` is never constructed
-    --> crates/yarli-cli/src/config.rs:134:19
-     |
- 134 | pub(crate) struct CliInvocationConfig {
-     |                   ^^^^^^^^^^^^^^^^^^^
- 
- warning: struct `AutoAdvanceConfig` is never constructed
-    --> crates/yarli-cli/src/config.rs:142:19
-     |
- 142 | pub(crate) struct AutoAdvanceConfig {
-     |                   ^^^^^^^^^^^^^^^^^
- 
- warning: associated items `from_loaded` and `max_reached` are never used
-    --> crates/yarli-cli/src/config.rs:148:19
-     |
- 147 | impl AutoAdvanceConfig {
-     | ---------------------- associated items in this implementation
- 148 |     pub(crate) fn from_loaded(loaded_config: &LoadedConfig) -> Self {
-     |                   ^^^^^^^^^^^
- ...
- 155 |     pub(crate) fn max_reached(self, advances_taken: usize) -> bool {
-     |                   ^^^^^^^^^^^
- 
- warning: function `configured_parallel_worktree_root` is never used
-    --> crates/yarli-cli/src/config.rs:160:15
-     |
- 160 | pub(crate) fn configured_parallel_worktree_root(loaded_config: &LoadedConfig) -> Option<String> {
-     |               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `resolve_path_from_cwd` is never used
-    --> crates/yarli-cli/src/config.rs:171:4
-     |
- 171 | fn resolve_path_from_cwd(path: &Path) -> Result<PathBuf> {
-     |    ^^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `home_directory_for_expansion` is never used
-    --> crates/yarli-cli/src/config.rs:181:4
-     |
- 181 | fn home_directory_for_expansion() -> Option<PathBuf> {
-     |    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `is_env_var_name_start` is never used
-    --> crates/yarli-cli/src/config.rs:194:4
-     |
- 194 | fn is_env_var_name_start(ch: char) -> bool {
-     |    ^^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `is_env_var_name_continue` is never used
-    --> crates/yarli-cli/src/config.rs:198:4
-     |
- 198 | fn is_env_var_name_continue(ch: char) -> bool {
-     |    ^^^^^^^^^^^^^^^^^^^^^^^^
- 
- warning: function `expand_env_variables` is never used
-    --> crates/yarli-cli/src/config.rs:202:4
-     |
- 202 | fn expand_env_variables(raw: &str) -> String {
- ... (truncated) ...
```

## Summary
- cargo fmt --all -- --check: pass
- cargo clippy --workspace --all-targets -- -D warnings: fail
- cargo test --workspace: pass
