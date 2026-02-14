//! Runtime configuration loader for `yarli.toml`.
//!
//! Loop-2 requires explicit backend selection and typed config sections.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use yarli_core::domain::SafeMode;

pub const DEFAULT_CONFIG_PATH: &str = "yarli.toml";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    File,
    Defaults,
}

impl ConfigSource {
    pub fn label(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Defaults => "defaults",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    path: PathBuf,
    source: ConfigSource,
    config: YarliConfig,
}

impl LoadedConfig {
    pub fn load_default() -> Result<Self> {
        Self::load(DEFAULT_CONFIG_PATH)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Ok(Self {
                path,
                source: ConfigSource::Defaults,
                config: YarliConfig::default(),
            });
        }

        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let config: YarliConfig = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;

        Ok(Self {
            path,
            source: ConfigSource::File,
            config,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn source(&self) -> ConfigSource {
        self.source
    }

    pub fn config(&self) -> &YarliConfig {
        &self.config
    }

    pub fn backend_selection(&self) -> Result<BackendSelection> {
        match self.config.core.backend {
            BackendKind::InMemory => Ok(BackendSelection::InMemory),
            BackendKind::Postgres => {
                let database_url = self
                    .config
                    .postgres
                    .database_url
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "core.backend=postgres requires postgres.database_url in {}",
                            self.path.display()
                        )
                    })?;
                Ok(BackendSelection::Postgres { database_url })
            }
        }
    }

    pub fn snapshot(&self) -> Result<serde_json::Value> {
        serde_json::to_value(&self.config).context("failed to serialize config snapshot")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendSelection {
    InMemory,
    Postgres { database_url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct YarliConfig {
    #[serde(default)]
    pub core: CoreConfig,
    #[serde(default)]
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub cli: CliConfig,
    #[serde(default)]
    pub event_loop: EventLoopConfig,
    #[serde(default)]
    pub features: FeaturesConfig,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub run: RunConfig,
    #[serde(default)]
    pub budgets: BudgetsConfig,
    #[serde(default)]
    pub git: GitConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub memory: MemoryConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub ui: UiConfig,
    /// sw4rm agent integration (only compiled with `sw4rm` feature).
    #[cfg(feature = "sw4rm")]
    #[serde(default)]
    pub sw4rm: yarli_sw4rm::Sw4rmConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum BackendKind {
    InMemory,
    Postgres,
}

impl BackendKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InMemory => "in-memory",
            Self::Postgres => "postgres",
        }
    }
}

impl Default for BackendKind {
    fn default() -> Self {
        Self::InMemory
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreConfig {
    #[serde(default)]
    pub backend: BackendKind,
    #[serde(default)]
    pub allow_in_memory_writes: bool,
    #[serde(default = "default_safe_mode")]
    pub safe_mode: SafeMode,
    #[serde(default)]
    pub worker_id: Option<String>,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            backend: BackendKind::InMemory,
            allow_in_memory_writes: false,
            safe_mode: default_safe_mode(),
            worker_id: None,
        }
    }
}

fn default_safe_mode() -> SafeMode {
    SafeMode::Execute
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PostgresConfig {
    #[serde(default)]
    pub database_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CliConfig {
    /// Named backend for operator readability (codex|claude|gemini|custom).
    #[serde(default)]
    pub backend: Option<String>,
    /// How the prompt is passed to the CLI: arg or stdin.
    #[serde(default)]
    pub prompt_mode: PromptMode,
    /// Executable to invoke (e.g. "codex", "claude", "gemini").
    #[serde(default)]
    pub command: Option<String>,
    /// Arguments to pass to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables to unset before invoking the CLI command.
    #[serde(default)]
    pub env_unset: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PromptMode {
    #[default]
    Arg,
    Stdin,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventLoopConfig {
    #[serde(default = "default_max_iterations")]
    pub max_iterations: u32,
    #[serde(default = "default_max_runtime_seconds")]
    pub max_runtime_seconds: u64,
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: u32,
}

impl Default for EventLoopConfig {
    fn default() -> Self {
        Self {
            max_iterations: default_max_iterations(),
            max_runtime_seconds: default_max_runtime_seconds(),
            idle_timeout_secs: default_idle_timeout_secs(),
            checkpoint_interval: default_checkpoint_interval(),
        }
    }
}

fn default_max_iterations() -> u32 {
    5
}

fn default_max_runtime_seconds() -> u64 {
    4 * 60 * 60
}

fn default_idle_timeout_secs() -> u64 {
    30 * 60
}

fn default_checkpoint_interval() -> u32 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeaturesConfig {
    #[serde(default = "default_features_parallel")]
    pub parallel: bool,
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            parallel: default_features_parallel(),
        }
    }
}

fn default_features_parallel() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueConfig {
    #[serde(default = "default_claim_batch_size")]
    pub claim_batch_size: usize,
    #[serde(default = "default_lease_ttl_seconds")]
    pub lease_ttl_seconds: i64,
    #[serde(default = "default_heartbeat_interval_seconds")]
    pub heartbeat_interval_seconds: u64,
    #[serde(default = "default_reclaim_interval_seconds")]
    pub reclaim_interval_seconds: u64,
    #[serde(default = "default_reclaim_grace_seconds")]
    pub reclaim_grace_seconds: i64,
    #[serde(default = "default_per_run_cap")]
    pub per_run_cap: usize,
    #[serde(default = "default_io_cap")]
    pub io_cap: usize,
    #[serde(default = "default_cpu_cap")]
    pub cpu_cap: usize,
    #[serde(default = "default_git_cap")]
    pub git_cap: usize,
    #[serde(default = "default_tool_cap")]
    pub tool_cap: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            claim_batch_size: default_claim_batch_size(),
            lease_ttl_seconds: default_lease_ttl_seconds(),
            heartbeat_interval_seconds: default_heartbeat_interval_seconds(),
            reclaim_interval_seconds: default_reclaim_interval_seconds(),
            reclaim_grace_seconds: default_reclaim_grace_seconds(),
            per_run_cap: default_per_run_cap(),
            io_cap: default_io_cap(),
            cpu_cap: default_cpu_cap(),
            git_cap: default_git_cap(),
            tool_cap: default_tool_cap(),
        }
    }
}

fn default_claim_batch_size() -> usize {
    4
}

fn default_lease_ttl_seconds() -> i64 {
    30
}

fn default_heartbeat_interval_seconds() -> u64 {
    5
}

fn default_reclaim_interval_seconds() -> u64 {
    10
}

fn default_reclaim_grace_seconds() -> i64 {
    5
}

fn default_per_run_cap() -> usize {
    8
}

fn default_io_cap() -> usize {
    16
}

fn default_cpu_cap() -> usize {
    4
}

fn default_git_cap() -> usize {
    2
}

fn default_tool_cap() -> usize {
    8
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub runner: ExecutionRunner,
    #[serde(default = "default_working_dir")]
    pub working_dir: String,
    /// Optional root directory where per-task workspaces/worktrees are created.
    ///
    /// Required when `features.parallel = true`.
    #[serde(default)]
    pub worktree_root: Option<String>,
    /// Directory names or relative paths to exclude from parallel workspace copies.
    ///
    /// Examples: `target`, `node_modules`, `.venv`, `venv`, `sdks/rust_sdk/target`.
    #[serde(default = "default_worktree_exclude_paths")]
    pub worktree_exclude_paths: Vec<String>,
    #[serde(default = "default_command_timeout_seconds")]
    pub command_timeout_seconds: u64,
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
    #[serde(default)]
    pub overwatch: OverwatchConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            runner: ExecutionRunner::default(),
            working_dir: default_working_dir(),
            worktree_root: None,
            worktree_exclude_paths: default_worktree_exclude_paths(),
            command_timeout_seconds: default_command_timeout_seconds(),
            tick_interval_ms: default_tick_interval_ms(),
            overwatch: OverwatchConfig::default(),
        }
    }
}

fn default_working_dir() -> String {
    ".".to_string()
}

fn default_command_timeout_seconds() -> u64 {
    300
}

fn default_tick_interval_ms() -> u64 {
    100
}

fn default_worktree_exclude_paths() -> Vec<String> {
    vec![
        ".yarl/workspaces".to_string(),
        ".yarli".to_string(),
        "target".to_string(),
        "node_modules".to_string(),
        ".venv".to_string(),
        "venv".to_string(),
        "__pycache__".to_string(),
    ]
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionRunner {
    #[default]
    Native,
    Overwatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OverwatchConfig {
    #[serde(default = "default_overwatch_service_url")]
    pub service_url: String,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub soft_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub silent_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub max_log_bytes: Option<u64>,
}

impl Default for OverwatchConfig {
    fn default() -> Self {
        Self {
            service_url: default_overwatch_service_url(),
            profile: None,
            soft_timeout_seconds: None,
            silent_timeout_seconds: None,
            max_log_bytes: None,
        }
    }
}

fn default_overwatch_service_url() -> String {
    "http://127.0.0.1:8089".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RunConfig {
    /// Optional prompt file for `yarli run` default execution.
    ///
    /// When unset, `yarli run` falls back to the legacy `PROMPT.md` lookup.
    #[serde(default)]
    pub prompt_file: Option<String>,
    /// Optional default objective when no prompt-level override is provided.
    #[serde(default)]
    pub objective: Option<String>,
    /// How long `yarli run continue` should wait for continuation availability.
    ///
    /// Value is seconds. `0` disables waiting (fail-fast behavior).
    #[serde(default)]
    pub continue_wait_timeout_seconds: u64,
    /// Allow planned-tranche auto-advance when deterioration trend is `stable`.
    ///
    /// Legacy compatibility toggle; prefer `auto_advance_policy`.
    #[serde(default)]
    pub allow_stable_auto_advance: bool,
    /// Auto-advance policy for planned tranches.
    ///
    /// `stable-ok` (default): stable and improving trends advance.
    /// `improving-only`: only improving deterioration trend advances.
    /// `stable-ok`: stable and improving trends advance.
    /// `always`: always advance to planned next tranche.
    #[serde(default)]
    pub auto_advance_policy: AutoAdvancePolicy,
    /// Maximum number of planned-tranche auto-advances in a single invocation.
    ///
    /// `0` means unlimited.
    #[serde(default)]
    pub max_auto_advance_tranches: u32,
    /// Enable grouping adjacent plan entries with shared `tranche_group=` metadata.
    #[serde(default)]
    pub enable_plan_tranche_grouping: bool,
    /// Cap grouped tasks per planned tranche.
    ///
    /// `0` means unlimited.
    #[serde(default)]
    pub max_grouped_tasks_per_tranche: u32,
    /// Enforce per-tranche allowed path hints from `IMPLEMENTATION_PLAN.md`.
    ///
    /// When enabled, `allowed_paths=` metadata is surfaced as explicit scope instructions
    /// in tranche task prompts.
    #[serde(default)]
    pub enforce_plan_tranche_allowed_paths: bool,
    /// Allow recursive `yarli run` invocation from task commands.
    ///
    /// Defaults to false; recursive runs require explicit per-invocation opt-in.
    #[serde(default)]
    pub allow_recursive_run: bool,
    /// Optional project-level task catalog for run-spec execution.
    ///
    /// Serialized as `[[run.tasks]]`.
    #[serde(default)]
    pub tasks: Vec<RunTaskConfig>,
    /// Optional explicit tranche definitions for run-spec execution.
    ///
    /// Serialized as `[[run.tranches]]`.
    #[serde(default)]
    pub tranches: Vec<RunTrancheConfig>,
    /// Optional plan guard contract for run-spec execution.
    ///
    /// Serialized as `[run.plan_guard]`.
    #[serde(default)]
    pub plan_guard: Option<RunPlanGuardConfig>,
    /// The default named pace used by shorthand commands like `yarli run batch`.
    #[serde(default)]
    pub default_pace: Option<String>,
    /// Named run presets (pace -> commands + optional overrides).
    #[serde(default)]
    pub paces: std::collections::BTreeMap<String, RunPaceConfig>,
}

impl RunConfig {
    pub fn effective_auto_advance_policy(&self) -> AutoAdvancePolicy {
        if self.auto_advance_policy != AutoAdvancePolicy::ImprovingOnly {
            return self.auto_advance_policy;
        }
        if self.allow_stable_auto_advance {
            AutoAdvancePolicy::StableOk
        } else {
            AutoAdvancePolicy::ImprovingOnly
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum AutoAdvancePolicy {
    ImprovingOnly,
    #[default]
    StableOk,
    Always,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RunPaceConfig {
    /// Commands to execute in order (one per task).
    #[serde(default)]
    pub cmds: Vec<String>,
    /// Optional working directory override for this pace.
    #[serde(default)]
    pub working_dir: Option<String>,
    /// Optional timeout override for this pace (0 disables timeout).
    #[serde(default)]
    pub command_timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunTaskConfig {
    pub key: String,
    pub cmd: String,
    #[serde(default)]
    pub class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunTrancheConfig {
    pub key: String,
    #[serde(default)]
    pub objective: Option<String>,
    pub task_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunPlanGuardConfig {
    pub target: String,
    #[serde(default)]
    pub mode: RunPlanGuardModeConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RunPlanGuardModeConfig {
    Implement,
    VerifyOnly,
}

impl Default for RunPlanGuardModeConfig {
    fn default() -> Self {
        Self::Implement
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BudgetsConfig {
    #[serde(default)]
    pub max_task_rss_bytes: Option<u64>,
    #[serde(default)]
    pub max_task_cpu_user_ticks: Option<u64>,
    #[serde(default)]
    pub max_task_cpu_system_ticks: Option<u64>,
    #[serde(default)]
    pub max_task_io_read_bytes: Option<u64>,
    #[serde(default)]
    pub max_task_io_write_bytes: Option<u64>,
    #[serde(default)]
    pub max_task_total_tokens: Option<u64>,
    #[serde(default)]
    pub max_run_total_tokens: Option<u64>,
    #[serde(default)]
    pub max_run_peak_rss_bytes: Option<u64>,
    #[serde(default)]
    pub max_run_cpu_user_ticks: Option<u64>,
    #[serde(default)]
    pub max_run_cpu_system_ticks: Option<u64>,
    #[serde(default)]
    pub max_run_io_read_bytes: Option<u64>,
    #[serde(default)]
    pub max_run_io_write_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitConfig {
    #[serde(default = "default_main_branch")]
    pub default_target_branch: String,
    #[serde(default = "default_true")]
    pub destructive_default_deny: bool,
}

impl Default for GitConfig {
    fn default() -> Self {
        Self {
            default_target_branch: default_main_branch(),
            destructive_default_deny: true,
        }
    }
}

fn default_main_branch() -> String {
    "main".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyConfig {
    #[serde(default = "default_true")]
    pub enforce_policies: bool,
    #[serde(default = "default_true")]
    pub audit_decisions: bool,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            enforce_policies: true,
            audit_decisions: true,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MemoryConfig {
    /// Master switch. When unset, defaults to `memory.backend.enabled`.
    ///
    /// This is intentionally optional to preserve the simple legacy toggle:
    /// setting `[memory.backend].enabled = true` should be enough to turn memory on.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Optional explicit project identifier for memory scoping.
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub backend: MemoryBackendConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryBackendConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Executable to invoke for Memory backend CLI integration.
    #[serde(default = "default_memory_command")]
    pub command: String,
    /// Directory to run memory backend CLI from (defaults to the directory containing `PROMPT.md`).
    #[serde(default)]
    pub project_dir: Option<String>,
    /// Max results to query and inject.
    #[serde(default = "default_memory_query_limit")]
    pub query_limit: u32,
    /// Query and emit memory hints once at run start.
    #[serde(default = "default_true")]
    pub inject_on_run_start: bool,
    /// Query and emit memory hints when a task blocks/fails.
    #[serde(default = "default_true")]
    pub inject_on_failure: bool,
}

impl Default for MemoryBackendConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            command: default_memory_command(),
            project_dir: None,
            query_limit: default_memory_query_limit(),
            inject_on_run_start: true,
            inject_on_failure: true,
        }
    }
}

fn default_memory_command() -> String {
    "memory-backend".to_string()
}

fn default_memory_query_limit() -> u32 {
    8
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservabilityConfig {
    #[serde(default = "default_audit_file")]
    pub audit_file: String,
    #[serde(default)]
    pub log_level: Option<String>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            audit_file: default_audit_file(),
            log_level: None,
        }
    }
}

fn default_audit_file() -> String {
    ".yarl/audit.jsonl".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UiConfig {
    #[serde(default)]
    pub mode: UiMode,
    /// When true, command output lines are streamed to terminal scrollback.
    #[serde(default)]
    pub verbose_output: bool,
    /// When true, cancellation events include extra diagnostic detail in provenance output.
    #[serde(default)]
    pub cancellation_diagnostics: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum UiMode {
    #[default]
    Auto,
    Stream,
    Tui,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn missing_file_uses_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        let loaded = LoadedConfig::load(&path).unwrap();
        assert_eq!(loaded.source(), ConfigSource::Defaults);
        assert_eq!(loaded.config().core.backend, BackendKind::InMemory);
        assert!(!loaded.config().core.allow_in_memory_writes);
        assert_eq!(loaded.config().event_loop.max_iterations, 5);
        assert!(loaded.config().features.parallel);
        assert_eq!(loaded.config().execution.runner, ExecutionRunner::Native);
        assert_eq!(loaded.config().execution.working_dir, ".");
        assert_eq!(loaded.config().execution.worktree_root, None);
        assert!(loaded
            .config()
            .execution
            .worktree_exclude_paths
            .iter()
            .any(|path| path == "target"));
        assert!(loaded
            .config()
            .execution
            .worktree_exclude_paths
            .iter()
            .any(|path| path == "node_modules"));
        assert!(loaded
            .config()
            .execution
            .worktree_exclude_paths
            .iter()
            .any(|path| path == ".venv"));
        assert_eq!(loaded.config().run.prompt_file, None);
        assert_eq!(loaded.config().run.objective, None);
        assert_eq!(loaded.config().run.continue_wait_timeout_seconds, 0);
        assert!(!loaded.config().run.allow_stable_auto_advance);
        assert_eq!(
            loaded.config().run.auto_advance_policy,
            AutoAdvancePolicy::StableOk
        );
        assert_eq!(loaded.config().run.max_auto_advance_tranches, 0);
        assert!(!loaded.config().run.enable_plan_tranche_grouping);
        assert_eq!(loaded.config().run.max_grouped_tasks_per_tranche, 0);
        assert!(!loaded.config().run.enforce_plan_tranche_allowed_paths);
        assert!(loaded.config().run.tasks.is_empty());
        assert!(loaded.config().run.tranches.is_empty());
        assert!(loaded.config().run.plan_guard.is_none());
        assert!(loaded.config().cli.env_unset.is_empty());
    }

    #[test]
    fn parses_required_sections() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(
            file,
            r#"
[core]
backend = "postgres"
allow_in_memory_writes = true
safe_mode = "restricted"

[postgres]
database_url = "postgres://localhost/yarli"

[cli]
backend = "codex"
prompt_mode = "arg"
command = "codex"
args = ["exec", "--json"]
env_unset = ["CLAUDECODE"]

[event_loop]
max_iterations = 3
max_runtime_seconds = 60
idle_timeout_secs = 10
checkpoint_interval = 2

[features]
parallel = true

[queue]
claim_batch_size = 2

[execution]
runner = "overwatch"
working_dir = "/work"
worktree_root = "/worktrees"
worktree_exclude_paths = ["target", "node_modules", ".venv", "venv"]
command_timeout_seconds = 600
[execution.overwatch]
service_url = "http://127.0.0.1:9999"
profile = "ci"
soft_timeout_seconds = 120
silent_timeout_seconds = 60
max_log_bytes = 2048

[run]
prompt_file = "prompts/I8B.md"
objective = "ship it"
continue_wait_timeout_seconds = 7
allow_stable_auto_advance = true
auto_advance_policy = "always"
max_auto_advance_tranches = 0
enable_plan_tranche_grouping = true
max_grouped_tasks_per_tranche = 3
enforce_plan_tranche_allowed_paths = true
default_pace = "batch"
[[run.tasks]]
key = "lint"
cmd = "cargo clippy --workspace -- -D warnings"
class = "cpu"

[[run.tasks]]
key = "test"
cmd = "cargo test --workspace"

[[run.tranches]]
key = "verify"
task_keys = ["lint", "test"]
objective = "verification tranche"

[run.plan_guard]
target = "I8B"
mode = "verify-only"
[run.paces.batch]
cmds = ["echo fmt", "echo clippy", "echo test"]
working_dir = "/repo"
command_timeout_seconds = 123

[budgets]
max_task_total_tokens = 1000
max_run_total_tokens = 5000

[git]
default_target_branch = "develop"

[policy]
enforce_policies = true

	[memory.backend]
	enabled = true
	endpoint = "http://localhost:8080"
	command = "memory-backend"
	project_dir = "."
	query_limit = 5
	inject_on_run_start = true
	inject_on_failure = true

[observability]
audit_file = "/tmp/yarli-audit.jsonl"

[ui]
mode = "stream"
"#
        )
        .unwrap();

        let loaded = LoadedConfig::load(&path).unwrap();
        assert_eq!(loaded.source(), ConfigSource::File);
        assert_eq!(loaded.config().core.backend, BackendKind::Postgres);
        assert!(loaded.config().core.allow_in_memory_writes);
        assert_eq!(loaded.config().core.safe_mode, SafeMode::Restricted);
        assert_eq!(loaded.config().cli.backend.as_deref(), Some("codex"));
        assert_eq!(loaded.config().cli.command.as_deref(), Some("codex"));
        assert_eq!(
            loaded.config().cli.env_unset,
            vec!["CLAUDECODE".to_string()]
        );
        assert_eq!(loaded.config().event_loop.max_iterations, 3);
        assert!(loaded.config().features.parallel);
        assert_eq!(loaded.config().execution.runner, ExecutionRunner::Overwatch);
        assert_eq!(
            loaded.config().execution.worktree_root.as_deref(),
            Some("/worktrees")
        );
        assert_eq!(
            loaded.config().execution.worktree_exclude_paths,
            vec![
                "target".to_string(),
                "node_modules".to_string(),
                ".venv".to_string(),
                "venv".to_string()
            ]
        );
        assert_eq!(
            loaded.config().execution.overwatch.service_url,
            "http://127.0.0.1:9999"
        );
        assert_eq!(
            loaded.config().execution.overwatch.soft_timeout_seconds,
            Some(120)
        );
        assert_eq!(
            loaded.config().execution.overwatch.max_log_bytes,
            Some(2048)
        );
        assert!(loaded.config().memory.backend.enabled);
        assert_eq!(loaded.config().memory.backend.command, "memory-backend");
        assert_eq!(
            loaded.config().memory.backend.project_dir.as_deref(),
            Some(".")
        );
        assert_eq!(loaded.config().memory.backend.query_limit, 5);
        assert!(loaded.config().memory.backend.inject_on_run_start);
        assert!(loaded.config().memory.backend.inject_on_failure);
        assert_eq!(loaded.config().ui.mode, UiMode::Stream);
        assert_eq!(loaded.config().budgets.max_task_total_tokens, Some(1000));
        assert_eq!(
            loaded.config().run.prompt_file.as_deref(),
            Some("prompts/I8B.md")
        );
        assert_eq!(loaded.config().run.objective.as_deref(), Some("ship it"));
        assert_eq!(loaded.config().run.continue_wait_timeout_seconds, 7);
        assert!(loaded.config().run.allow_stable_auto_advance);
        assert_eq!(
            loaded.config().run.auto_advance_policy,
            AutoAdvancePolicy::Always
        );
        assert_eq!(loaded.config().run.max_auto_advance_tranches, 0);
        assert!(loaded.config().run.enable_plan_tranche_grouping);
        assert_eq!(loaded.config().run.max_grouped_tasks_per_tranche, 3);
        assert!(loaded.config().run.enforce_plan_tranche_allowed_paths);
        assert_eq!(loaded.config().run.tasks.len(), 2);
        assert_eq!(loaded.config().run.tasks[0].key, "lint");
        assert_eq!(loaded.config().run.tasks[0].class.as_deref(), Some("cpu"));
        assert_eq!(loaded.config().run.tranches.len(), 1);
        assert_eq!(loaded.config().run.tranches[0].key, "verify");
        assert_eq!(
            loaded.config().run.tranches[0].objective.as_deref(),
            Some("verification tranche")
        );
        assert_eq!(
            loaded
                .config()
                .run
                .plan_guard
                .as_ref()
                .map(|guard| guard.target.as_str()),
            Some("I8B")
        );
        assert_eq!(
            loaded
                .config()
                .run
                .plan_guard
                .as_ref()
                .map(|guard| guard.mode),
            Some(RunPlanGuardModeConfig::VerifyOnly)
        );
        assert_eq!(loaded.config().run.default_pace.as_deref(), Some("batch"));
        assert_eq!(
            loaded
                .config()
                .run
                .paces
                .get("batch")
                .unwrap()
                .command_timeout_seconds,
            Some(123)
        );
    }

    #[test]
    fn postgres_backend_requires_database_url() {
        let mut config = YarliConfig::default();
        config.core.backend = BackendKind::Postgres;
        let loaded = LoadedConfig {
            path: PathBuf::from("yarli.toml"),
            source: ConfigSource::Defaults,
            config,
        };
        let err = loaded.backend_selection().unwrap_err();
        assert!(
            err.to_string().contains("postgres.database_url"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn snapshot_serializes_all_sections() {
        let loaded = LoadedConfig {
            path: PathBuf::from("yarli.toml"),
            source: ConfigSource::Defaults,
            config: YarliConfig::default(),
        };

        let snapshot = loaded.snapshot().unwrap();
        let object = snapshot.as_object().unwrap();
        for key in [
            "core",
            "postgres",
            "cli",
            "event_loop",
            "features",
            "queue",
            "execution",
            "run",
            "budgets",
            "git",
            "policy",
            "memory",
            "observability",
            "ui",
        ] {
            assert!(object.contains_key(key), "snapshot missing section {key}");
        }
    }

    #[test]
    fn backend_selection_for_in_memory() {
        let loaded = LoadedConfig {
            path: PathBuf::from("yarli.toml"),
            source: ConfigSource::Defaults,
            config: YarliConfig::default(),
        };
        assert_eq!(
            loaded.backend_selection().unwrap(),
            BackendSelection::InMemory
        );
    }

    #[test]
    fn backend_kind_as_str() {
        assert_eq!(BackendKind::InMemory.as_str(), "in-memory");
        assert_eq!(BackendKind::Postgres.as_str(), "postgres");
    }

    #[test]
    fn run_effective_auto_advance_policy_respects_explicit_policy_and_legacy_toggle() {
        let default_cfg = RunConfig::default();
        assert_eq!(
            default_cfg.effective_auto_advance_policy(),
            AutoAdvancePolicy::StableOk
        );

        let improving_explicit = RunConfig {
            auto_advance_policy: AutoAdvancePolicy::ImprovingOnly,
            allow_stable_auto_advance: false,
            ..RunConfig::default()
        };
        assert_eq!(
            improving_explicit.effective_auto_advance_policy(),
            AutoAdvancePolicy::ImprovingOnly
        );

        let improving_with_legacy_toggle = RunConfig {
            auto_advance_policy: AutoAdvancePolicy::ImprovingOnly,
            allow_stable_auto_advance: true,
            ..RunConfig::default()
        };
        assert_eq!(
            improving_with_legacy_toggle.effective_auto_advance_policy(),
            AutoAdvancePolicy::StableOk
        );
    }
}
