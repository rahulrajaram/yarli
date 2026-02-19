//! Runtime configuration loader for `yarli.toml`.
//!
//! Loop-2 requires explicit backend selection and typed config sections.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use yarli_core::domain::SafeMode;
use yarli_observability::JsonlAuditSink;
use yarli_queue::{InMemoryTaskQueue, PostgresTaskQueue, TaskQueue};
use yarli_store::{EventStore, InMemoryEventStore, PostgresEventStore};

pub const DEFAULT_CONFIG_PATH: &str = "yarli.toml";

pub(crate) fn cmd_init(
    path: PathBuf,
    force: bool,
    print: bool,
    backend: Option<crate::cli::InitBackend>,
) -> Result<()> {
    let template = crate::cli::init_config_template(backend);
    if print {
        print!("{template}");
        return Ok(());
    }

    if path.exists() && !force {
        bail!(
            "refusing to overwrite existing config at {} (use --force to overwrite)",
            path.display()
        );
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
    }

    fs::write(&path, template)
        .with_context(|| format!("failed to write config file {}", path.display()))?;

    println!("Initialized config at {}", path.display());
    println!("Review [core], [postgres], and [budgets] before first durable run.");
    println!("Tip: run `yarli init --help` for the full list of tunable properties.");
    Ok(())
}

pub(crate) fn load_runtime_config_for_reads() -> Result<LoadedConfig> {
    LoadedConfig::load_default().context("failed to load runtime config")
}

pub(crate) fn ensure_write_backend_guard(
    loaded_config: &LoadedConfig,
    command_name: &str,
) -> Result<()> {
    if matches!(
        loaded_config.backend_selection()?,
        BackendSelection::InMemory
    ) && !loaded_config.config().core.allow_in_memory_writes
    {
        bail!(
            "`{command_name}` refuses in-memory write mode. Configure durable storage with [core] backend = \"postgres\" and [postgres] database_url, or explicitly opt in with [core] allow_in_memory_writes = true."
        );
    }
    Ok(())
}

pub(crate) fn load_runtime_config_for_writes(command_name: &str) -> Result<LoadedConfig> {
    let loaded_config = load_runtime_config_for_reads()?;
    ensure_write_backend_guard(&loaded_config, command_name)?;
    Ok(loaded_config)
}

pub(crate) fn prepare_audit_sink(loaded_config: &LoadedConfig) -> Result<Option<JsonlAuditSink>> {
    if !loaded_config.config().policy.audit_decisions {
        return Ok(None);
    }

    let audit_path = PathBuf::from(&loaded_config.config().observability.audit_file);
    if let Some(parent) = audit_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create audit directory {}", parent.display())
            })?;
        }
    }

    Ok(Some(JsonlAuditSink::new(audit_path)))
}

pub(crate) fn with_event_store<T>(
    loaded_config: &LoadedConfig,
    operation: impl FnOnce(&dyn EventStore) -> Result<T>,
) -> Result<T> {
    match loaded_config.backend_selection()? {
        BackendSelection::InMemory => {
            let store = InMemoryEventStore::new();
            operation(&store)
        }
        BackendSelection::Postgres { database_url } => {
            let store = PostgresEventStore::new(&database_url)
                .map_err(|e| anyhow::anyhow!("failed to initialize postgres event store: {e}"))?;
            operation(&store)
        }
    }
}

pub(crate) fn with_event_store_and_queue<T>(
    loaded_config: &LoadedConfig,
    operation: impl FnOnce(&dyn EventStore, &dyn TaskQueue) -> Result<T>,
) -> Result<T> {
    match loaded_config.backend_selection()? {
        BackendSelection::InMemory => {
            let store = InMemoryEventStore::new();
            let queue = InMemoryTaskQueue::new();
            operation(&store, &queue)
        }
        BackendSelection::Postgres { database_url } => {
            let store = PostgresEventStore::new(&database_url)
                .map_err(|e| anyhow::anyhow!("failed to initialize postgres event store: {e}"))?;
            let queue = PostgresTaskQueue::new(&database_url)
                .map_err(|e| anyhow::anyhow!("failed to initialize postgres task queue: {e}"))?;
            operation(&store, &queue)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CliInvocationConfig {
    pub(crate) command: String,
    pub(crate) args: Vec<String>,
    pub(crate) prompt_mode: PromptMode,
    pub(crate) env_unset: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AutoAdvanceConfig {
    pub(crate) policy: AutoAdvancePolicy,
    pub(crate) max_tranches: u32,
}

impl AutoAdvanceConfig {
    pub(crate) fn from_loaded(loaded_config: &LoadedConfig) -> Self {
        Self {
            policy: loaded_config.config().run.effective_auto_advance_policy(),
            max_tranches: loaded_config.config().run.max_auto_advance_tranches,
        }
    }

    pub(crate) fn max_reached(self, advances_taken: usize) -> bool {
        self.max_tranches != 0 && advances_taken >= self.max_tranches as usize
    }
}

pub(crate) fn configured_parallel_worktree_root(loaded_config: &LoadedConfig) -> Option<String> {
    loaded_config
        .config()
        .execution
        .worktree_root
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

fn resolve_path_from_cwd(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()
            .context("failed to read current working directory")?
            .join(path))
    }
}

fn home_directory_for_expansion() -> Option<PathBuf> {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .or_else(|| {
            let drive = env::var_os("HOMEDRIVE")?;
            let home_path = env::var_os("HOMEPATH")?;
            let mut combined = PathBuf::from(drive);
            combined.push(home_path);
            Some(combined.into_os_string())
        })
        .map(PathBuf::from)
}

fn is_env_var_name_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_env_var_name_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn expand_env_variables(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut index = 0;
    while index < raw.len() {
        let ch = raw[index..]
            .chars()
            .next()
            .expect("index should be in bounds");
        if ch != '$' {
            out.push(ch);
            index += ch.len_utf8();
            continue;
        }

        let token_start = index;
        index += ch.len_utf8();
        if index >= raw.len() {
            out.push('$');
            break;
        }

        let next = raw[index..]
            .chars()
            .next()
            .expect("index should be in bounds");
        if next == '{' {
            let mut cursor = index + next.len_utf8();
            let name_start = cursor;
            while cursor < raw.len() {
                let c = raw[cursor..]
                    .chars()
                    .next()
                    .expect("cursor should be in bounds");
                if c == '}' {
                    break;
                }
                cursor += c.len_utf8();
            }

            if cursor >= raw.len() {
                out.push_str(&raw[token_start..]);
                break;
            }

            let name = &raw[name_start..cursor];
            index = cursor + 1;
            let valid_name = !name.is_empty()
                && name
                    .chars()
                    .next()
                    .map(is_env_var_name_start)
                    .unwrap_or(false)
                && name.chars().skip(1).all(is_env_var_name_continue);
            if valid_name {
                if let Ok(value) = env::var(name) {
                    out.push_str(&value);
                } else {
                    out.push_str(&raw[token_start..index]);
                }
            } else {
                out.push_str(&raw[token_start..index]);
            }
            continue;
        }

        if is_env_var_name_start(next) {
            let name_start = index;
            index += next.len_utf8();
            while index < raw.len() {
                let c = raw[index..]
                    .chars()
                    .next()
                    .expect("index should be in bounds");
                if !is_env_var_name_continue(c) {
                    break;
                }
                index += c.len_utf8();
            }
            let name = &raw[name_start..index];
            if let Ok(value) = env::var(name) {
                out.push_str(&value);
            } else {
                out.push('$');
                out.push_str(name);
            }
            continue;
        }

        out.push('$');
    }

    out
}

fn expand_home_prefix(path: &str) -> Result<PathBuf> {
    if path == "~" {
        let home = home_directory_for_expansion().ok_or_else(|| {
            anyhow::anyhow!("cannot expand `~` because HOME/USERPROFILE is unset")
        })?;
        return Ok(home);
    }

    if path.starts_with("~/") || path.starts_with("~\\") {
        let home = home_directory_for_expansion().ok_or_else(|| {
            anyhow::anyhow!("cannot expand `~` because HOME/USERPROFILE is unset")
        })?;
        let suffix = &path[2..];
        return Ok(home.join(suffix));
    }

    Ok(PathBuf::from(path))
}

pub(crate) fn resolve_execution_path_from_cwd(
    raw_path: &str,
    setting_name: &str,
) -> Result<PathBuf> {
    let trimmed = raw_path.trim();
    if trimmed.is_empty() {
        bail!("{setting_name} path is empty");
    }

    let env_expanded = expand_env_variables(trimmed);
    let home_expanded = expand_home_prefix(&env_expanded).with_context(|| {
        format!(
            "failed to expand {setting_name} from configured value {:?}",
            raw_path
        )
    })?;
    resolve_path_from_cwd(&home_expanded).with_context(|| {
        format!(
            "failed to resolve {setting_name} from configured value {:?}",
            raw_path
        )
    })
}

pub(crate) fn ensure_parallel_workspace_contract(loaded_config: &LoadedConfig) -> Result<()> {
    if !loaded_config.config().features.parallel {
        return Ok(());
    }

    if configured_parallel_worktree_root(loaded_config).is_some() {
        return Ok(());
    }

    bail!(
        "`yarli run` requires `[execution].worktree_root` when `[features].parallel = true`.\nUpdate {} with:\n\n[execution]\nworktree_root = \".yarl/workspaces\"",
        loaded_config.path().display()
    );
}

fn default_cli_command_for_backend(backend: &str) -> Option<String> {
    match backend.trim().to_ascii_lowercase().as_str() {
        "codex" => Some("codex".to_string()),
        "claude" => Some("claude".to_string()),
        "gemini" => Some("gemini".to_string()),
        _ => None,
    }
}

pub(crate) fn resolve_cli_invocation_config(
    loaded_config: &LoadedConfig,
) -> Result<CliInvocationConfig> {
    let cli = &loaded_config.config().cli;
    let command = cli
        .command
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            cli.backend
                .as_deref()
                .and_then(default_cli_command_for_backend)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "yarli run requires [cli].command (or a known [cli].backend) in {}",
                loaded_config.path().display()
            )
        })?;
    let mut env_unset = Vec::with_capacity(cli.env_unset.len());
    for name in &cli.env_unset {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            bail!("cli.env_unset entries must be non-empty environment variable names");
        }
        if !is_valid_env_var_name(trimmed) {
            bail!("invalid cli.env_unset entry {trimmed:?}: must match [A-Za-z_][A-Za-z0-9_]*");
        }
        env_unset.push(trimmed.to_string());
    }

    Ok(CliInvocationConfig {
        command,
        args: cli.args.clone(),
        prompt_mode: cli.prompt_mode,
        env_unset,
    })
}

fn is_valid_env_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        "''".to_string()
    } else {
        format!("'{}'", value.replace('\'', r#"'"'"'"#))
    }
}

pub(crate) fn build_cli_command(invocation: &CliInvocationConfig, prompt_text: &str) -> String {
    let mut parts =
        Vec::with_capacity(2 + invocation.env_unset.len() * 2 + invocation.args.len() + 1);
    if !invocation.env_unset.is_empty() {
        parts.push("env".to_string());
        for name in &invocation.env_unset {
            parts.push("-u".to_string());
            parts.push(name.clone());
        }
    }
    parts.push(invocation.command.clone());
    parts.extend(invocation.args.iter().cloned());
    let base = parts
        .iter()
        .map(|part| shell_quote(part))
        .collect::<Vec<_>>()
        .join(" ");

    match invocation.prompt_mode {
        PromptMode::Arg => {
            let prompt = shell_quote(prompt_text);
            format!("{base} {prompt}")
        }
        PromptMode::Stdin => {
            let prompt = shell_quote(prompt_text);
            format!("printf %s {prompt} | {base}")
        }
    }
}

const PREFLIGHT_MARKER: &str = "YARLI_PREFLIGHT_OK";

/// Exercise the configured CLI backend with a trivial prompt to verify it works
/// before dispatching real tasks. Catches missing binaries, bad flags, permission
/// failures, and auth issues early.
pub(crate) fn preflight_cli_backend(invocation: &CliInvocationConfig) -> Result<()> {
    let preflight_prompt = format!("Respond with exactly: {PREFLIGHT_MARKER}");
    let cmd = build_cli_command(invocation, &preflight_prompt);
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(&cmd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .with_context(|| {
            format!("CLI backend preflight failed to execute: {cmd}")
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let exit_code = output
            .status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());
        bail!(
            "CLI backend preflight failed (exit {exit_code}).\n\
             Command: {cmd}\n\
             Stderr: {stderr}\n\
             Fix: verify that the [cli] command and args in yarli.toml are correct and the binary is installed."
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.contains(PREFLIGHT_MARKER) {
        bail!(
            "CLI backend preflight succeeded (exit 0) but output did not contain expected marker.\n\
             Command: {cmd}\n\
             Expected marker: {PREFLIGHT_MARKER}\n\
             Stdout (first 500 chars): {}\n\
             Fix: the CLI backend may be permission-blocked or misconfigured.",
            &stdout[..stdout.len().min(500)]
        );
    }

    Ok(())
}

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
    /// Strategy for handling merge conflicts during parallel workspace patch application.
    ///
    /// `fail` (default): hard-fail on merge conflicts.
    /// `agent`: spawn an agent to attempt automated resolution (stub — falls back to fail).
    /// `manual`: preserve conflicted workspaces and emit operator instructions.
    #[serde(default)]
    pub merge_conflict_resolution: MergeConflictResolution,
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
pub enum MergeConflictResolution {
    /// Hard-fail on merge conflicts (current default behavior).
    #[default]
    Fail,
    /// Spawn an agent to attempt automated conflict resolution.
    Agent,
    /// Preserve conflicted workspaces and emit operator instructions.
    Manual,
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
    use crate::cli::Cli;
    use clap::CommandFactory;
    use std::io::Write;
    use std::path::Path;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn write_test_config(contents: &str) -> LoadedConfig {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(&path, contents).unwrap();
        LoadedConfig::load(path).unwrap()
    }

    fn write_test_config_at(path: &Path, contents: &str) -> LoadedConfig {
        std::fs::write(path, contents).unwrap();
        LoadedConfig::load(path).unwrap()
    }

    #[test]
    fn cmd_init_writes_documented_config_template() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");

        cmd_init(path.clone(), false, false, None).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("[core]"));
        assert!(raw.contains("backend = \"in-memory\""));
        assert!(raw.contains("[budgets]"));
        assert!(raw.contains("max_task_total_tokens"));
        assert!(raw.contains("[ui]"));
        assert!(raw.contains("mode = \"auto\""));
        assert!(raw.contains("[features]"));
        assert!(raw.contains("parallel = true"));
        assert!(raw.contains("worktree_root"));
    }

    #[test]
    fn cmd_init_refuses_overwrite_without_force() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(&path, "existing = true\n").unwrap();

        let err = cmd_init(path.clone(), false, false, None).unwrap_err();
        assert!(err.to_string().contains("use --force"));

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("existing = true"));
    }

    #[test]
    fn cmd_init_force_overwrites_existing_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(&path, "existing = true\n").unwrap();

        cmd_init(path.clone(), true, false, None).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("existing = true"));
        assert!(raw.contains("[execution]"));
    }

    #[test]
    fn init_help_lists_all_tunable_properties() {
        let mut cmd = Cli::command();
        let init = cmd
            .find_subcommand_mut("init")
            .expect("init subcommand should exist");
        let mut help = Vec::new();
        init.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        for key in [
            "core.backend",
            "core.allow_in_memory_writes",
            "core.safe_mode",
            "core.worker_id",
            "postgres.database_url",
            "cli.backend",
            "cli.prompt_mode",
            "cli.command",
            "cli.args",
            "event_loop.max_iterations",
            "event_loop.max_runtime_seconds",
            "event_loop.idle_timeout_secs",
            "event_loop.checkpoint_interval",
            "features.parallel",
            "queue.claim_batch_size",
            "queue.lease_ttl_seconds",
            "queue.heartbeat_interval_seconds",
            "queue.reclaim_interval_seconds",
            "queue.reclaim_grace_seconds",
            "queue.per_run_cap",
            "queue.io_cap",
            "queue.cpu_cap",
            "queue.git_cap",
            "queue.tool_cap",
            "execution.working_dir",
            "execution.worktree_root",
            "execution.worktree_exclude_paths",
            "execution.command_timeout_seconds",
            "execution.tick_interval_ms",
            "execution.runner",
            "execution.overwatch.service_url",
            "execution.overwatch.profile",
            "execution.overwatch.soft_timeout_seconds",
            "execution.overwatch.silent_timeout_seconds",
            "execution.overwatch.max_log_bytes",
            "run.prompt_file",
            "run.objective",
            "run.continue_wait_timeout_seconds",
            "run.allow_stable_auto_advance",
            "run.auto_advance_policy",
            "run.max_auto_advance_tranches",
            "run.enable_plan_tranche_grouping",
            "run.max_grouped_tasks_per_tranche",
            "run.enforce_plan_tranche_allowed_paths",
            "run.merge_conflict_resolution",
            "run.tasks",
            "run.tranches",
            "run.plan_guard.target",
            "run.plan_guard.mode",
            "run.default_pace",
            "run.paces.<name>.cmds",
            "run.paces.<name>.working_dir",
            "run.paces.<name>.command_timeout_seconds",
            "budgets.max_task_rss_bytes",
            "budgets.max_task_cpu_user_ticks",
            "budgets.max_task_cpu_system_ticks",
            "budgets.max_task_io_read_bytes",
            "budgets.max_task_io_write_bytes",
            "budgets.max_task_total_tokens",
            "budgets.max_run_total_tokens",
            "budgets.max_run_peak_rss_bytes",
            "budgets.max_run_cpu_user_ticks",
            "budgets.max_run_cpu_system_ticks",
            "budgets.max_run_io_read_bytes",
            "budgets.max_run_io_write_bytes",
            "git.default_target_branch",
            "git.destructive_default_deny",
            "policy.enforce_policies",
            "policy.audit_decisions",
            "memory.backend.enabled",
            "memory.backend.endpoint",
            "memory.backend.command",
            "memory.backend.project_dir",
            "memory.backend.query_limit",
            "memory.backend.inject_on_run_start",
            "memory.backend.inject_on_failure",
            "memory.enabled",
            "memory.project_id",
            "observability.audit_file",
            "observability.log_level",
            "ui.mode",
            "ui.verbose_output",
            "ui.cancellation_diagnostics",
        ] {
            assert!(
                help.contains(key),
                "init --help should mention config property {key}"
            );
        }
    }

    #[test]
    fn parallel_workspace_contract_requires_worktree_root_when_parallel_enabled() {
        let loaded = write_test_config(
            r#"
[features]
parallel = true

[execution]
working_dir = "."
"#,
        );

        let err = ensure_parallel_workspace_contract(&loaded).unwrap_err();
        assert!(err.to_string().contains("execution].worktree_root"));
    }

    #[test]
    fn parallel_workspace_contract_allows_parallel_disabled_without_worktree_root() {
        let loaded = write_test_config(
            r#"
[features]
parallel = false
"#,
        );

        ensure_parallel_workspace_contract(&loaded).unwrap();
    }

    #[test]
    fn resolve_execution_path_from_cwd_expands_env_tokens() {
        let temp_dir = TempDir::new().unwrap();
        let env_key = format!("YARLI_TEST_EXEC_PATH_{}", Uuid::now_v7().simple());
        std::env::set_var(&env_key, temp_dir.path());
        let tokenized = format!("${{{env_key}}}/nested");
        let resolved =
            resolve_execution_path_from_cwd(&tokenized, "execution.worktree_root").unwrap();
        std::env::remove_var(&env_key);
        assert_eq!(resolved, temp_dir.path().join("nested"));
    }

    #[test]
    fn resolve_execution_path_from_cwd_expands_tilde_prefix() {
        let Some(home) = home_directory_for_expansion() else {
            return;
        };

        let resolved = resolve_execution_path_from_cwd("~/yarli-tmp", "execution.working_dir")
            .expect("tilde expansion should succeed when HOME/USERPROFILE is set");
        assert_eq!(resolved, home.join("yarli-tmp"));
    }

    #[test]
    fn build_cli_command_applies_env_unset_prefix() {
        let invocation = CliInvocationConfig {
            command: "claude".to_string(),
            args: vec!["--model".to_string(), "sonnet-4.5".to_string()],
            prompt_mode: PromptMode::Arg,
            env_unset: vec!["CLAUDECODE".to_string(), "FOO".to_string()],
        };

        let command = build_cli_command(&invocation, "hello");
        assert!(
            command.contains("'env' '-u' 'CLAUDECODE' '-u' 'FOO' 'claude' '--model' 'sonnet-4.5'")
        );
        assert!(command.ends_with(" 'hello'"));
    }

    #[test]
    fn resolve_cli_invocation_config_rejects_invalid_env_unset_entries() {
        let temp = TempDir::new().unwrap();
        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "claude"
args = ["--model", "sonnet-4.5"]
env_unset = ["BAD-NAME"]
"#,
        );

        let err = resolve_cli_invocation_config(&loaded_config).unwrap_err();
        assert!(err.to_string().contains("invalid cli.env_unset entry"));
    }

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

    #[test]
    fn ensure_write_backend_guard_blocks_in_memory_writes_by_default() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("yarli.toml");
        let loaded_config = LoadedConfig::load(config_path).unwrap();

        let result = ensure_write_backend_guard(&loaded_config, "task unblock");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn ensure_write_backend_guard_allows_explicit_ephemeral_override() {
        let loaded_config = write_test_config(
            r#"
[core]
backend = "in-memory"
allow_in_memory_writes = true
"#,
        );

        let result = ensure_write_backend_guard(&loaded_config, "task unblock");
        assert!(result.is_ok());
    }

    #[test]
    fn preflight_cli_backend_succeeds_with_echo_command() {
        let invocation = CliInvocationConfig {
            command: "echo".to_string(),
            args: vec![],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };
        // echo will output the prompt text which includes YARLI_PREFLIGHT_OK
        preflight_cli_backend(&invocation).unwrap();
    }

    #[test]
    fn preflight_cli_backend_fails_on_missing_binary() {
        let invocation = CliInvocationConfig {
            command: "yarli-nonexistent-binary-12345".to_string(),
            args: vec![],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };
        let err = preflight_cli_backend(&invocation).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("preflight failed"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn preflight_cli_backend_fails_on_bad_exit_code() {
        let invocation = CliInvocationConfig {
            command: "false".to_string(),
            args: vec![],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };
        let err = preflight_cli_backend(&invocation).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("preflight failed"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn preflight_cli_backend_fails_when_marker_missing() {
        // `true` exits 0 but produces no output
        let invocation = CliInvocationConfig {
            command: "true".to_string(),
            args: vec![],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };
        let err = preflight_cli_backend(&invocation).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("did not contain expected marker"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn merge_conflict_resolution_config_parses_all_variants() {
        let loaded = write_test_config(
            r#"
[run]
merge_conflict_resolution = "fail"
"#,
        );
        assert_eq!(
            loaded.config().run.merge_conflict_resolution,
            MergeConflictResolution::Fail
        );

        let loaded = write_test_config(
            r#"
[run]
merge_conflict_resolution = "agent"
"#,
        );
        assert_eq!(
            loaded.config().run.merge_conflict_resolution,
            MergeConflictResolution::Agent
        );

        let loaded = write_test_config(
            r#"
[run]
merge_conflict_resolution = "manual"
"#,
        );
        assert_eq!(
            loaded.config().run.merge_conflict_resolution,
            MergeConflictResolution::Manual
        );
    }

    #[test]
    fn merge_conflict_resolution_defaults_to_fail() {
        let loaded = write_test_config("[run]\n");
        assert_eq!(
            loaded.config().run.merge_conflict_resolution,
            MergeConflictResolution::Fail
        );
    }
}
