//! Runtime configuration loader for `yarli.toml`.
//!
//! Loop-2 requires explicit backend selection and typed config sections.

mod migrate;

use std::collections::BTreeMap;
use std::env;
use std::fs;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{de::Deserializer, Deserialize, Serialize};
use sqlx::postgres::PgConnectOptions;
use yarli_cli::yarli_core::domain::SafeMode;
use yarli_cli::yarli_core::entities::continuation::TaskHealthAction;
use yarli_cli::yarli_observability::JsonlAuditSink;
use yarli_cli::yarli_queue::{InMemoryTaskQueue, PostgresTaskQueue, TaskQueue};
use yarli_cli::yarli_store::{EventStore, InMemoryEventStore, PostgresEventStore};

pub const DEFAULT_CONFIG_PATH: &str = "yarli.toml";
const DATABASE_URL_ENV: &str = "DATABASE_URL";

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
            "`{command_name}` refuses in-memory write mode. Configure durable storage with [core] backend = \"postgres\" and set DATABASE_URL (or [postgres].database_url / [postgres].database_url_file), or explicitly opt in with [core] allow_in_memory_writes = true."
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
    pub(crate) task_health: RunTaskHealthConfig,
}

impl AutoAdvanceConfig {
    pub(crate) fn from_loaded(loaded_config: &LoadedConfig) -> Self {
        Self {
            policy: loaded_config.config().run.effective_auto_advance_policy(),
            max_tranches: loaded_config.config().run.max_auto_advance_tranches,
            task_health: loaded_config.config().run.task_health,
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
        "kiro-cli" => Some("kiro-cli".to_string()),
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
const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(15);

fn has_claude_model_flag(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--model")
}

fn fallback_preflight_invocation(invocation: &CliInvocationConfig) -> Option<CliInvocationConfig> {
    let command_file = Path::new(&invocation.command)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    if !matches!(command_file, "claude" | "kiro-cli") {
        return None;
    }
    if !has_claude_model_flag(&invocation.args) {
        return None;
    }

    let mut filtered_args = Vec::with_capacity(invocation.args.len());
    let mut skip_next = false;
    for arg in &invocation.args {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg == "--model" {
            skip_next = true;
            continue;
        }
        filtered_args.push(arg.clone());
    }

    Some(CliInvocationConfig {
        command: invocation.command.clone(),
        args: filtered_args,
        prompt_mode: invocation.prompt_mode,
        env_unset: invocation.env_unset.clone(),
    })
}

fn execute_preflight_command(
    invocation: &CliInvocationConfig,
    preflight_prompt: &str,
) -> Result<()> {
    execute_preflight_command_with_timeout(invocation, preflight_prompt, PREFLIGHT_TIMEOUT)
}

fn execute_preflight_command_with_timeout(
    invocation: &CliInvocationConfig,
    preflight_prompt: &str,
    timeout: Duration,
) -> Result<()> {
    let cmd = build_cli_command(invocation, preflight_prompt);
    let mut command = std::process::Command::new("sh");
    command
        .arg("-c")
        .arg(&cmd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[cfg(unix)]
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("CLI backend preflight failed to execute: {cmd}"))?;
    #[cfg(unix)]
    let process_group_id = Some(child.id() as i32);
    #[cfg(not(unix))]
    let process_group_id = None;

    let deadline = Instant::now() + timeout;
    let output = loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                break child.wait_with_output().with_context(|| {
                    format!("CLI backend preflight failed to collect output: {cmd}")
                })?;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    terminate_preflight_child(&mut child, process_group_id);
                    let output = child.wait_with_output().with_context(|| {
                        format!(
                            "CLI backend preflight timed out and failed to collect output: {cmd}"
                        )
                    })?;
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    bail!(
                        "CLI backend preflight timed out after {}ms.\n\
                         \n\
                         Command: {cmd}\n\
                         \n\
                         Stdout (first 500 chars): {}\n\
                         \n\
                         Stderr (first 500 chars): {}\n\
                         \n\
                         Fix: the configured CLI backend is hanging before Yarli can register a run. Check auth, TTY requirements, blocking flags, or the backend command in yarli.toml.",
                        timeout.as_millis(),
                        &stdout[..stdout.len().min(500)],
                        &stderr[..stderr.len().min(500)]
                    );
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(err) => {
                terminate_preflight_child(&mut child, process_group_id);
                let _ = child.wait_with_output();
                return Err(err).with_context(|| {
                    format!("CLI backend preflight failed while polling child: {cmd}")
                });
            }
        }
    };

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

fn terminate_preflight_child(child: &mut std::process::Child, process_group_id: Option<i32>) {
    #[cfg(unix)]
    if let Some(group_id) = process_group_id {
        unsafe {
            libc::killpg(group_id, libc::SIGKILL);
        }
        return;
    }

    let _ = child.kill();
}

fn format_preflight_failure_section(
    label: &str,
    invocation: &CliInvocationConfig,
    preflight_prompt: &str,
    error: &anyhow::Error,
) -> String {
    format!(
        "{label}:\nCommand: {}\nError: {error}",
        build_cli_command(invocation, preflight_prompt)
    )
}

fn preflight_cli_backend_with_executor(
    invocation: &CliInvocationConfig,
    execute: impl Fn(&CliInvocationConfig, &str) -> Result<()>,
) -> Result<()> {
    let preflight_prompt = format!("Respond with exactly: {PREFLIGHT_MARKER}");
    match execute(invocation, &preflight_prompt) {
        Ok(()) => Ok(()),
        Err(primary_error) => {
            let Some(fallback) = fallback_preflight_invocation(invocation) else {
                return Err(primary_error);
            };

            match execute(&fallback, &preflight_prompt) {
                Ok(()) => Ok(()),
                Err(fallback_error) => bail!(
                    "CLI backend preflight failed for the configured invocation, and fallback without `--model` also failed.\n\
                     \n\
                     {}\n\
                     \n\
                     {}",
                    format_preflight_failure_section(
                        "Primary attempt",
                        invocation,
                        &preflight_prompt,
                        &primary_error
                    ),
                    format_preflight_failure_section(
                        "Fallback attempt",
                        &fallback,
                        &preflight_prompt,
                        &fallback_error
                    )
                ),
            }
        }
    }
}

#[allow(dead_code)]
fn preflight_cli_backend_with_timeout(
    invocation: &CliInvocationConfig,
    timeout: Duration,
) -> Result<()> {
    preflight_cli_backend_with_executor(invocation, |attempt, prompt| {
        execute_preflight_command_with_timeout(attempt, prompt, timeout)
    })
}

/// Exercise the configured CLI backend with a trivial prompt to verify it works
/// before dispatching real tasks. Catches missing binaries, bad flags, permission
/// failures, and auth issues early.
pub(crate) fn preflight_cli_backend(invocation: &CliInvocationConfig) -> Result<()> {
    preflight_cli_backend_with_executor(invocation, execute_preflight_command)
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
        let loaded = Self::load(DEFAULT_CONFIG_PATH)?;
        loaded.validate()?;
        Ok(loaded)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            let loaded = Self {
                path,
                source: ConfigSource::Defaults,
                config: YarliConfig::default(),
            };
            loaded.validate()?;
            return Ok(loaded);
        }

        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;

        // Pass 1: parse into untyped TOML tree.
        let mut value: toml::Value = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;

        // Apply migrations for deprecated keys.
        let report = migrate::apply_migrations(&mut value);
        for m in &report.applied {
            tracing::warn!(rule = m.rule_id, "{}", m.warning);
        }

        // Pass 2: deserialize patched tree into typed config.
        let config: YarliConfig = value
            .try_into()
            .with_context(|| format!("failed to deserialize config from {}", path.display()))?;

        let loaded = Self {
            path,
            source: ConfigSource::File,
            config,
        };
        loaded.validate()?;
        Ok(loaded)
    }

    fn validate(&self) -> Result<()> {
        if self.config.core.backend == BackendKind::Postgres {
            self.resolve_postgres_database_url()?;
        }
        self.config.run.validate_merge_repair_config()?;
        Ok(())
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

    /// Mutable access to the config — used by CLI flag overrides before dispatch.
    pub fn config_mut(&mut self) -> &mut YarliConfig {
        &mut self.config
    }

    pub fn backend_selection(&self) -> Result<BackendSelection> {
        match self.config.core.backend {
            BackendKind::InMemory => Ok(BackendSelection::InMemory),
            BackendKind::Postgres => {
                let database_url = self.resolve_postgres_database_url()?;
                Ok(BackendSelection::Postgres { database_url })
            }
        }
    }

    pub fn snapshot(&self) -> Result<serde_json::Value> {
        serde_json::to_value(&self.config).context("failed to serialize config snapshot")
    }

    fn resolve_postgres_database_url(&self) -> Result<String> {
        if let Ok(value) = env::var(DATABASE_URL_ENV) {
            let value = value.trim();
            if !value.is_empty() {
                return validate_postgres_database_url("DATABASE_URL env var", value);
            }
        }

        if let Some(database_url) = self
            .config
            .postgres
            .database_url
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            return validate_postgres_database_url("[postgres].database_url", database_url);
        }

        if let Some(database_url_path) = self
            .config
            .postgres
            .database_url_file
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            let raw_url = fs::read_to_string(database_url_path).with_context(|| {
                format!("failed to read postgres.database_url_file {database_url_path}")
            })?;
            let database_url = raw_url.trim();
            if database_url.is_empty() {
                bail!("postgres.database_url_file {database_url_path} is empty");
            }
            return validate_postgres_database_url("postgres.database_url_file", database_url);
        }

        bail!(
            "core.backend=postgres requires DATABASE_URL, [postgres].database_url, or [postgres].database_url_file in {}",
            self.path.display()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendSelection {
    InMemory,
    Postgres { database_url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
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
    pub sw4rm: yarli_cli::yarli_sw4rm::Sw4rmConfig,
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
    #[serde(default)]
    pub database_url_file: Option<String>,
}

fn validate_postgres_database_url(source: &str, database_url: &str) -> Result<String> {
    PgConnectOptions::from_str(database_url).with_context(|| {
        format!(
            "invalid postgres database url in {source}: expected a valid Postgres connection URI"
        )
    })?;
    Ok(database_url.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CliConfig {
    /// Named backend for operator readability (codex|claude|gemini|kiro-cli|custom).
    #[serde(default)]
    pub backend: Option<String>,
    /// How the prompt is passed to the CLI: arg or stdin.
    #[serde(default)]
    pub prompt_mode: PromptMode,
    /// Executable to invoke (e.g. "codex", "claude", "gemini", "kiro-cli").
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
    /// Use git worktrees instead of full directory copies for parallel workspaces.
    ///
    /// Falls back to copy mode when git worktrees are unavailable.
    #[serde(default = "default_true")]
    pub parallel_worktree: bool,
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            parallel: default_features_parallel(),
            parallel_worktree: true,
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

fn default_soft_token_cap_ratio() -> f64 {
    0.9
}

fn default_advisory_tranche_warn_tokens() -> u64 {
    70_000
}

fn default_advisory_tranche_max_tokens() -> u64 {
    100_000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdvisoryTrancheTokenThresholds {
    #[serde(default = "default_advisory_tranche_warn_tokens")]
    pub warn_tokens: u64,
    #[serde(default = "default_advisory_tranche_max_tokens")]
    pub max_recommended_tokens: u64,
}

impl Default for AdvisoryTrancheTokenThresholds {
    fn default() -> Self {
        Self {
            warn_tokens: default_advisory_tranche_warn_tokens(),
            max_recommended_tokens: default_advisory_tranche_max_tokens(),
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    /// Policy when `yarli run continue` detects tranche drift (newer open
    /// tranches not present in the continuation snapshot).
    ///
    /// `refuse` (default): hard-fail and tell the operator to use `--fresh-from-tranches`.
    /// `fallback-fresh`: log a warning and automatically rebuild from the current
    /// prompt/plan/tranches state instead of refusing.
    #[serde(default)]
    pub continuation_drift_policy: ContinuationDriftPolicy,
    /// Allow planned-tranche auto-advance when deterioration trend is `stable`.
    ///
    /// Legacy compatibility toggle; prefer `auto_advance_policy`.
    #[serde(default)]
    pub allow_stable_auto_advance: bool,
    /// Configure task-health actions per deterioration trend.
    ///
    /// Defaults keep behavior unchanged (`continue` for all trends).
    #[serde(default)]
    pub task_health: RunTaskHealthConfig,
    /// Fractional hard-token cap threshold for the soft checkpoint trigger.
    ///
    /// Value is in `[0.0, 1.0]`. Set to `0.0` to disable.
    #[serde(default = "default_soft_token_cap_ratio")]
    pub soft_token_cap_ratio: f64,
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
    /// Advisory tranche-token thresholds used for operator guidance and warnings only.
    ///
    /// These thresholds do not hard-stop execution; they surface quality signals so
    /// operators and planners can regroup oversized tranches before quality drifts.
    #[serde(default)]
    pub tranche_token_advisory: AdvisoryTrancheTokenThresholds,
    /// Optional backend/model-specific advisory tranche-token overrides.
    ///
    /// Keys are matched case-insensitively against `[cli].backend` first, then
    /// `[cli].command` when backend is unset.
    #[serde(default)]
    pub tranche_token_advisory_by_backend:
        std::collections::BTreeMap<String, AdvisoryTrancheTokenThresholds>,
    /// Enforce per-tranche allowed path hints from `IMPLEMENTATION_PLAN.md`.
    ///
    /// When enabled, `allowed_paths=` metadata is surfaced as explicit scope instructions
    /// in tranche task prompts.
    #[serde(default)]
    pub enforce_plan_tranche_allowed_paths: bool,
    /// Optional hardening policy for tranche execution contracts.
    ///
    /// This is opt-in and preserves legacy behavior by default.
    #[serde(default)]
    pub tranche_contract: TrancheContractConfig,
    /// Allow recursive `yarli run` invocation from task commands.
    ///
    /// Defaults to false; recursive runs require explicit per-invocation opt-in.
    #[serde(default)]
    pub allow_recursive_run: bool,
    /// Strategy for handling merge conflicts during parallel workspace patch application.
    ///
    /// `fail` (default): hard-fail on merge conflicts.
    /// `auto-repair`: attempt a deterministic automated repair before failing.
    /// `manual`: preserve conflicted workspaces and emit operator instructions.
    /// `llm-assisted`: invoke a user-configured shell command to resolve conflicts.
    #[serde(default, deserialize_with = "deserialize_merge_conflict_resolution")]
    pub merge_conflict_resolution: MergeConflictResolution,
    /// Shell command for LLM-assisted merge conflict repair.
    ///
    /// Required when `merge_conflict_resolution = "llm-assisted"`.
    /// Runs via `sh -c` in the source workdir. Should edit conflicted files in place.
    #[serde(default)]
    pub merge_repair_command: Option<String>,
    /// Timeout in seconds for the repair command. Default: 300. 0 = no timeout.
    #[serde(default = "default_merge_repair_timeout_seconds")]
    pub merge_repair_timeout_seconds: u64,
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
    /// Commit YARLI state files after every N tranches. `0` disables auto-commit.
    ///
    /// Default is `1` (commit after every tranche).
    #[serde(default = "default_auto_commit_interval")]
    pub auto_commit_interval: u32,
    /// Template for auto-commit messages. Placeholders: `{tranche_key}`,
    /// `{run_id}`, `{tranches_completed}`, `{tranches_total}`.
    #[serde(default)]
    pub auto_commit_message: Option<String>,
    /// When true (default), yarli automatically commits uncommitted edits inside
    /// touched submodules during parallel-merge finalization. This prevents dirty
    /// submodule state from causing the next run's merge to fail. Set to false
    /// (or pass `--no-submodule-auto-commit` at the CLI) to preserve the dirty
    /// state instead of auto-committing (useful for inspection or manual cleanup).
    #[serde(default = "default_auto_commit_submodule_edits")]
    pub auto_commit_submodule_edits: bool,
    /// The default named pace used by shorthand commands like `yarli run batch`.
    #[serde(default)]
    pub default_pace: Option<String>,
    /// Named run presets (pace -> commands + optional overrides).
    #[serde(default)]
    pub paces: std::collections::BTreeMap<String, RunPaceConfig>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunTaskHealthConfig {
    #[serde(default)]
    pub improving: TaskHealthAction,
    #[serde(default)]
    pub stable: TaskHealthAction,
    #[serde(default)]
    pub deteriorating: TaskHealthAction,
}

impl Default for RunTaskHealthConfig {
    fn default() -> Self {
        Self {
            improving: TaskHealthAction::Continue,
            stable: TaskHealthAction::Continue,
            deteriorating: TaskHealthAction::Continue,
        }
    }
}

impl RunConfig {
    pub fn validate_merge_repair_config(&self) -> Result<()> {
        if self.merge_conflict_resolution == MergeConflictResolution::LlmAssisted {
            match &self.merge_repair_command {
                None => bail!(
                    "merge_conflict_resolution = \"llm-assisted\" requires a non-empty [run].merge_repair_command"
                ),
                Some(cmd) if cmd.trim().is_empty() => bail!(
                    "merge_conflict_resolution = \"llm-assisted\" requires a non-empty [run].merge_repair_command"
                ),
                _ => {}
            }
        }
        validate_advisory_tranche_token_thresholds(
            "run.tranche_token_advisory",
            &self.tranche_token_advisory,
        )?;
        for (backend, thresholds) in &self.tranche_token_advisory_by_backend {
            validate_advisory_tranche_token_thresholds(
                &format!("run.tranche_token_advisory_by_backend.{backend}"),
                thresholds,
            )?;
        }
        Ok(())
    }

    pub fn should_surface_allowed_paths_in_prompts(&self) -> bool {
        self.enforce_plan_tranche_allowed_paths
            || self
                .tranche_contract
                .effective_enforce_allowed_paths_on_merge()
    }

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

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn advisory_tranche_token_thresholds_for_backend(
        &self,
        backend: Option<&str>,
    ) -> AdvisoryTrancheTokenThresholds {
        let Some(backend) = backend.map(str::trim).filter(|value| !value.is_empty()) else {
            return self.tranche_token_advisory.clone();
        };
        let normalized = backend.to_ascii_lowercase();
        self.tranche_token_advisory_by_backend
            .iter()
            .find(|(key, _)| key.trim().eq_ignore_ascii_case(&normalized))
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| self.tranche_token_advisory.clone())
    }
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            prompt_file: None,
            objective: None,
            continue_wait_timeout_seconds: 0,
            continuation_drift_policy: ContinuationDriftPolicy::Refuse,
            allow_stable_auto_advance: false,
            task_health: RunTaskHealthConfig::default(),
            soft_token_cap_ratio: default_soft_token_cap_ratio(),
            auto_advance_policy: AutoAdvancePolicy::StableOk,
            max_auto_advance_tranches: 0,
            enable_plan_tranche_grouping: false,
            max_grouped_tasks_per_tranche: 0,
            tranche_token_advisory: AdvisoryTrancheTokenThresholds::default(),
            tranche_token_advisory_by_backend: std::collections::BTreeMap::new(),
            enforce_plan_tranche_allowed_paths: false,
            tranche_contract: TrancheContractConfig::default(),
            allow_recursive_run: false,
            merge_conflict_resolution: MergeConflictResolution::Fail,
            merge_repair_command: None,
            merge_repair_timeout_seconds: default_merge_repair_timeout_seconds(),
            tasks: Vec::new(),
            tranches: Vec::new(),
            auto_commit_interval: default_auto_commit_interval(),
            auto_commit_message: None,
            auto_commit_submodule_edits: default_auto_commit_submodule_edits(),
            plan_guard: None,
            default_pace: None,
            paces: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TrancheContractConfig {
    /// Convenience umbrella for the strict tranche contract checks below.
    ///
    /// When enabled, Yarli requires `verify`, requires `done_when`, and
    /// hard-enforces `allowed_paths` during merge finalization when
    /// `allowed_paths` are present on a tranche.
    #[serde(default)]
    pub strict: bool,
    /// Require open tranches to declare a verification command.
    #[serde(default)]
    pub require_verify: bool,
    /// Require open tranches to declare explicit done criteria.
    #[serde(default)]
    pub require_done_when: bool,
    /// Fail merge finalization if a tranche edits paths outside its
    /// declared `allowed_paths`.
    #[serde(default)]
    pub enforce_allowed_paths_on_merge: bool,
}

impl TrancheContractConfig {
    pub fn effective_require_verify(&self) -> bool {
        self.strict || self.require_verify
    }

    pub fn effective_require_done_when(&self) -> bool {
        self.strict || self.require_done_when
    }

    pub fn effective_enforce_allowed_paths_on_merge(&self) -> bool {
        self.strict || self.enforce_allowed_paths_on_merge
    }

    pub fn any_contract_checks_enabled(&self) -> bool {
        self.effective_require_verify()
            || self.effective_require_done_when()
            || self.effective_enforce_allowed_paths_on_merge()
    }
}

fn validate_advisory_tranche_token_thresholds(
    label: &str,
    thresholds: &AdvisoryTrancheTokenThresholds,
) -> Result<()> {
    if thresholds.warn_tokens == 0 {
        bail!("{label}.warn_tokens must be greater than zero");
    }
    if thresholds.max_recommended_tokens == 0 {
        bail!("{label}.max_recommended_tokens must be greater than zero");
    }
    if thresholds.warn_tokens > thresholds.max_recommended_tokens {
        bail!(
            "{label}.warn_tokens ({}) must be <= {label}.max_recommended_tokens ({})",
            thresholds.warn_tokens,
            thresholds.max_recommended_tokens
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum MergeConflictResolution {
    /// Hard-fail on merge conflicts (current default behavior).
    #[default]
    Fail,
    /// Attempt an automatic repair pass before failing.
    AutoRepair,
    /// Preserve conflicted workspaces and emit operator instructions.
    Manual,
    /// Invoke an LLM-assisted repair command to resolve conflicts.
    LlmAssisted,
}

fn default_merge_repair_timeout_seconds() -> u64 {
    300
}

fn default_auto_commit_interval() -> u32 {
    1
}

fn default_auto_commit_submodule_edits() -> bool {
    true
}

fn deserialize_merge_conflict_resolution<'de, D>(
    deserializer: D,
) -> Result<MergeConflictResolution, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    match raw.as_str() {
        "fail" => Ok(MergeConflictResolution::Fail),
        "auto-repair" => Ok(MergeConflictResolution::AutoRepair),
        "manual" => Ok(MergeConflictResolution::Manual),
        "llm-assisted" => Ok(MergeConflictResolution::LlmAssisted),
        _ => Err(serde::de::Error::custom(format!(
            "invalid [run].merge_conflict_resolution = {raw:?}; expected one of: \"fail\", \"manual\", \"auto-repair\", \"llm-assisted\""
        ))),
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

/// Policy for handling tranche drift during `yarli run continue`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ContinuationDriftPolicy {
    /// Hard-fail when newer open tranches are detected (current default).
    #[default]
    Refuse,
    /// Log a warning and rebuild from current prompt/plan/tranches state.
    FallbackFresh,
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
    #[serde(default)]
    pub depends_on: Vec<String>,
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
    /// Master switch. When unset, defaults to selected provider enabled state.
    ///
    /// Backward-compatible behavior:
    /// - when using `[memory.backend]`, defaults to `memory.backend.enabled`
    /// - when using `[memory.providers.<name>]`, defaults to selected provider's `enabled`
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Optional explicit project identifier for memory scoping.
    #[serde(default)]
    pub project_id: Option<String>,
    /// Named provider to select from `[memory.providers.<name>]`.
    ///
    /// Examples:
    /// - `provider = "default"`
    /// - `provider = "kafka"`
    ///
    /// When omitted, YARLI falls back to legacy `[memory.backend]`.
    #[serde(default)]
    pub provider: Option<String>,
    /// Provider registry keyed by name.
    #[serde(default)]
    pub providers: BTreeMap<String, MemoryProviderConfig>,
    /// Legacy single-backend configuration (kept for backward compatibility).
    #[serde(default)]
    pub backend: MemoryBackendConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryProviderKind {
    #[default]
    Cli,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryProviderConfig {
    /// Provider type. `cli` shells out to a plugin binary implementing memory operations.
    #[serde(rename = "type", default)]
    pub kind: MemoryProviderKind,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Executable to invoke for CLI provider integration.
    #[serde(default = "default_memory_command")]
    pub command: String,
    /// Directory to run provider CLI from (defaults to the directory containing `PROMPT.md`).
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

impl Default for MemoryProviderConfig {
    fn default() -> Self {
        Self {
            kind: MemoryProviderKind::Cli,
            enabled: true,
            endpoint: None,
            command: default_memory_command(),
            project_dir: None,
            query_limit: default_memory_query_limit(),
            inject_on_run_start: true,
            inject_on_failure: true,
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedMemoryProviderConfig {
    pub(crate) name: String,
    pub(crate) kind: MemoryProviderKind,
    pub(crate) endpoint: Option<String>,
    pub(crate) command: String,
    pub(crate) project_dir: Option<String>,
    pub(crate) query_limit: u32,
    pub(crate) inject_on_run_start: bool,
    pub(crate) inject_on_failure: bool,
}

impl MemoryConfig {
    pub(crate) fn resolve_provider(&self) -> Result<Option<ResolvedMemoryProviderConfig>> {
        let selected_provider = self
            .provider
            .as_deref()
            .and_then(|name| trim_to_non_empty(name).map(|name| name.to_string()));

        if let Some(provider_name) = selected_provider {
            let provider = self.providers.get(&provider_name).with_context(|| {
                format!(
                    "memory.provider={provider_name:?} does not exist in [memory.providers.<name>]"
                )
            })?;
            let enabled = self.enabled.unwrap_or(provider.enabled);
            if !enabled || !provider.enabled {
                return Ok(None);
            }
            return Ok(Some(ResolvedMemoryProviderConfig {
                name: provider_name,
                kind: provider.kind,
                endpoint: provider.endpoint.clone(),
                command: provider.command.clone(),
                project_dir: provider.project_dir.clone(),
                query_limit: provider.query_limit,
                inject_on_run_start: provider.inject_on_run_start,
                inject_on_failure: provider.inject_on_failure,
            }));
        }

        let enabled = self.enabled.unwrap_or(self.backend.enabled);
        if !enabled || !self.backend.enabled {
            return Ok(None);
        }

        Ok(Some(ResolvedMemoryProviderConfig {
            name: "legacy-backend".to_string(),
            kind: MemoryProviderKind::Cli,
            endpoint: self.backend.endpoint.clone(),
            command: self.backend.command.clone(),
            project_dir: self.backend.project_dir.clone(),
            query_limit: self.backend.query_limit,
            inject_on_run_start: self.backend.inject_on_run_start,
            inject_on_failure: self.backend.inject_on_failure,
        }))
    }
}

fn trim_to_non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
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
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    use tempfile::TempDir;
    use uuid::Uuid;

    static DATABASE_URL_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn with_database_url_env_lock() -> std::sync::MutexGuard<'static, ()> {
        DATABASE_URL_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("database URL env lock should be obtainable")
    }

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

    #[cfg(unix)]
    fn write_test_cli_script(temp_dir: &TempDir, name: &str, body: &str) -> String {
        let path = temp_dir.path().join(name);
        std::fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).unwrap();
        path.display().to_string()
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
            "postgres.database_url_file",
            "cli.backend",
            "cli.prompt_mode",
            "cli.command",
            "cli.args",
            "event_loop.max_iterations",
            "event_loop.max_runtime_seconds",
            "event_loop.idle_timeout_secs",
            "event_loop.checkpoint_interval",
            "features.parallel",
            "features.parallel_worktree",
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
            "run.task_health",
            "run.task_health.improving",
            "run.task_health.stable",
            "run.task_health.deteriorating",
            "run.soft_token_cap_ratio",
            "run.max_auto_advance_tranches",
            "run.enable_plan_tranche_grouping",
            "run.max_grouped_tasks_per_tranche",
            "run.tranche_token_advisory.warn_tokens",
            "run.tranche_token_advisory.max_recommended_tokens",
            "run.tranche_token_advisory_by_backend.<name>.warn_tokens",
            "run.tranche_token_advisory_by_backend.<name>.max_recommended_tokens",
            "run.enforce_plan_tranche_allowed_paths",
            "run.tranche_contract.strict",
            "run.tranche_contract.require_verify",
            "run.tranche_contract.require_done_when",
            "run.tranche_contract.enforce_allowed_paths_on_merge",
            "run.merge_conflict_resolution",
            "run.merge_repair_command",
            "run.merge_repair_timeout_seconds",
            "run.auto_commit_interval",
            "run.auto_commit_message",
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
            "memory.provider",
            "memory.providers.<name>.type",
            "memory.providers.<name>.enabled",
            "memory.providers.<name>.endpoint",
            "memory.providers.<name>.command",
            "memory.providers.<name>.project_dir",
            "memory.providers.<name>.query_limit",
            "memory.providers.<name>.inject_on_run_start",
            "memory.providers.<name>.inject_on_failure",
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
            args: vec!["--model".to_string(), "sonnet-4.6".to_string()],
            prompt_mode: PromptMode::Arg,
            env_unset: vec!["CLAUDECODE".to_string(), "FOO".to_string()],
        };

        let command = build_cli_command(&invocation, "hello");
        assert!(
            command.contains("'env' '-u' 'CLAUDECODE' '-u' 'FOO' 'claude' '--model' 'sonnet-4.6'")
        );
        assert!(command.ends_with(" 'hello'"));
    }

    #[test]
    fn fallback_preflight_invocation_removes_claude_model() {
        let invocation = CliInvocationConfig {
            command: "claude".to_string(),
            args: vec![
                "--print".to_string(),
                "--model".to_string(),
                "claude-sonnet-4-6".to_string(),
                "--dangerously-skip-permissions".to_string(),
            ],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };
        let fallback = fallback_preflight_invocation(&invocation).unwrap();
        assert_eq!(
            fallback.args,
            vec!["--print", "--dangerously-skip-permissions"]
        );
    }

    #[test]
    fn fallback_preflight_invocation_removes_kiro_cli_model() {
        let invocation = CliInvocationConfig {
            command: "kiro-cli".to_string(),
            args: vec![
                "chat".to_string(),
                "--no-interactive".to_string(),
                "--model".to_string(),
                "some-model".to_string(),
                "--trust-all-tools".to_string(),
            ],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };
        let fallback = fallback_preflight_invocation(&invocation).unwrap();
        assert_eq!(
            fallback.args,
            vec!["chat", "--no-interactive", "--trust-all-tools"]
        );
    }

    #[test]
    fn fallback_preflight_invocation_returns_none_for_kiro_cli_without_model() {
        let invocation = CliInvocationConfig {
            command: "kiro-cli".to_string(),
            args: vec![
                "chat".to_string(),
                "--no-interactive".to_string(),
                "--trust-all-tools".to_string(),
            ],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };
        assert!(fallback_preflight_invocation(&invocation).is_none());
    }

    #[test]
    fn default_cli_command_for_kiro_cli_backend() {
        assert_eq!(
            default_cli_command_for_backend("kiro-cli"),
            Some("kiro-cli".to_string())
        );
    }

    #[test]
    fn resolve_cli_invocation_config_resolves_kiro_cli_backend() {
        let loaded = write_test_config(
            r#"
[cli]
backend = "kiro-cli"
args = ["chat", "--no-interactive", "--trust-all-tools"]
"#,
        );
        let invocation = resolve_cli_invocation_config(&loaded).unwrap();
        assert_eq!(invocation.command, "kiro-cli");
        assert_eq!(
            invocation.args,
            vec!["chat", "--no-interactive", "--trust-all-tools"]
        );
        assert!(matches!(invocation.prompt_mode, PromptMode::Arg));
    }

    #[test]
    fn init_config_template_kiro_cli_produces_valid_toml() {
        let template = crate::cli::init_config_template(Some(crate::cli::InitBackend::KiroCli));
        let parsed: toml::Value =
            toml::from_str(&template).expect("kiro-cli init template should be valid TOML");
        let cli = parsed.get("cli").expect("template should have [cli]");
        assert_eq!(cli.get("backend").unwrap().as_str().unwrap(), "kiro-cli");
        assert_eq!(cli.get("command").unwrap().as_str().unwrap(), "kiro-cli");
        let args: Vec<&str> = cli
            .get("args")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(args, vec!["chat", "--no-interactive", "--trust-all-tools"]);
    }

    #[test]
    fn resolve_cli_invocation_config_rejects_invalid_env_unset_entries() {
        let temp = TempDir::new().unwrap();
        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "claude"
args = ["--model", "sonnet-4.6"]
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
        assert_eq!(loaded.config().run.soft_token_cap_ratio, 0.9);
        assert_eq!(
            loaded.config().run.task_health,
            RunTaskHealthConfig::default()
        );
        assert_eq!(
            loaded.config().run.auto_advance_policy,
            AutoAdvancePolicy::StableOk
        );
        assert_eq!(loaded.config().run.max_auto_advance_tranches, 0);
        assert!(!loaded.config().run.enable_plan_tranche_grouping);
        assert_eq!(loaded.config().run.max_grouped_tasks_per_tranche, 0);
        assert_eq!(
            loaded.config().run.tranche_token_advisory,
            AdvisoryTrancheTokenThresholds::default()
        );
        assert!(loaded
            .config()
            .run
            .tranche_token_advisory_by_backend
            .is_empty());
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
soft_token_cap_ratio = 0.9
max_auto_advance_tranches = 0
enable_plan_tranche_grouping = true
max_grouped_tasks_per_tranche = 3
enforce_plan_tranche_allowed_paths = true
default_pace = "batch"

[run.tranche_contract]
strict = true
require_verify = true
require_done_when = true
enforce_allowed_paths_on_merge = true

[run.task_health]
improving = "continue"
stable = "checkpoint-now"
deteriorating = "stop-and-summarize"

[run.tranche_token_advisory]
warn_tokens = 60000
max_recommended_tokens = 90000

[run.tranche_token_advisory_by_backend.codex]
warn_tokens = 50000
max_recommended_tokens = 80000
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
            loaded.config().run.task_health,
            RunTaskHealthConfig {
                improving: TaskHealthAction::Continue,
                stable: TaskHealthAction::CheckpointNow,
                deteriorating: TaskHealthAction::StopAndSummarize,
            }
        );
        assert_eq!(loaded.config().run.soft_token_cap_ratio, 0.9);
        assert_eq!(
            loaded.config().run.auto_advance_policy,
            AutoAdvancePolicy::Always
        );
        assert_eq!(loaded.config().run.max_auto_advance_tranches, 0);
        assert!(loaded.config().run.enable_plan_tranche_grouping);
        assert_eq!(loaded.config().run.max_grouped_tasks_per_tranche, 3);
        assert_eq!(
            loaded.config().run.tranche_token_advisory,
            AdvisoryTrancheTokenThresholds {
                warn_tokens: 60_000,
                max_recommended_tokens: 90_000,
            }
        );
        assert_eq!(
            loaded
                .config()
                .run
                .advisory_tranche_token_thresholds_for_backend(Some("codex")),
            AdvisoryTrancheTokenThresholds {
                warn_tokens: 50_000,
                max_recommended_tokens: 80_000,
            }
        );
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
    fn memory_provider_registry_resolves_named_provider() {
        let loaded = write_test_config(
            r#"
[memory]
enabled = true
project_id = "proj-1"
provider = "default"

[memory.providers.default]
type = "cli"
enabled = true
command = "memory-backend"
project_dir = "."
query_limit = 11
inject_on_run_start = true
inject_on_failure = false
"#,
        );

        let resolved = loaded
            .config()
            .memory
            .resolve_provider()
            .expect("provider resolution should succeed")
            .expect("provider should be enabled");

        assert_eq!(resolved.name, "default");
        assert_eq!(resolved.kind, MemoryProviderKind::Cli);
        assert_eq!(resolved.command, "memory-backend");
        assert_eq!(resolved.project_dir.as_deref(), Some("."));
        assert_eq!(resolved.query_limit, 11);
        assert!(resolved.inject_on_run_start);
        assert!(!resolved.inject_on_failure);
    }

    #[test]
    fn memory_provider_registry_falls_back_to_legacy_backend() {
        let loaded = write_test_config(
            r#"
[memory]
enabled = true

[memory.backend]
enabled = true
command = "memory-backend"
query_limit = 5
inject_on_run_start = true
inject_on_failure = true
"#,
        );

        let resolved = loaded
            .config()
            .memory
            .resolve_provider()
            .expect("provider resolution should succeed")
            .expect("legacy backend should be enabled");

        assert_eq!(resolved.name, "legacy-backend");
        assert_eq!(resolved.kind, MemoryProviderKind::Cli);
        assert_eq!(resolved.command, "memory-backend");
        assert_eq!(resolved.query_limit, 5);
    }

    #[test]
    fn advisory_tranche_thresholds_validate_warn_not_above_max() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(
            &path,
            r#"
[run.tranche_token_advisory]
warn_tokens = 120000
max_recommended_tokens = 100000
"#,
        )
        .unwrap();

        let err = LoadedConfig::load(&path).unwrap_err();
        assert!(err.to_string().contains("warn_tokens"));
    }

    #[test]
    fn memory_provider_registry_errors_on_unknown_provider() {
        let loaded = write_test_config(
            r#"
[memory]
enabled = true
provider = "missing"
"#,
        );

        let err = loaded.config().memory.resolve_provider().unwrap_err();
        assert!(err.to_string().contains("memory.provider"));
    }

    #[test]
    fn postgres_backend_requires_database_url() {
        let mut config = YarliConfig::default();
        config.core.backend = BackendKind::Postgres;
        let _guard = with_database_url_env_lock();
        std::env::remove_var(DATABASE_URL_ENV);
        let loaded = LoadedConfig {
            path: PathBuf::from("yarli.toml"),
            source: ConfigSource::Defaults,
            config,
        };
        let err = loaded.backend_selection().unwrap_err();
        assert!(err.to_string().contains("DATABASE_URL"));
    }

    #[test]
    fn postgres_backend_prefers_environment_database_url() {
        let mut config = YarliConfig::default();
        config.core.backend = BackendKind::Postgres;
        let _guard = with_database_url_env_lock();
        std::env::set_var(
            DATABASE_URL_ENV,
            "postgres://postgres:postgres@localhost:5432/env-var",
        );
        let loaded = LoadedConfig {
            path: PathBuf::from("yarli.toml"),
            source: ConfigSource::Defaults,
            config,
        };

        let selection = loaded.backend_selection().unwrap();
        assert_eq!(
            selection,
            BackendSelection::Postgres {
                database_url: "postgres://postgres:postgres@localhost:5432/env-var".to_string()
            }
        );
        std::env::remove_var(DATABASE_URL_ENV);
    }

    #[test]
    fn postgres_backend_prefers_configured_database_url_file() {
        let _guard = with_database_url_env_lock();
        let previous_env = std::env::var(DATABASE_URL_ENV).ok();
        std::env::remove_var(DATABASE_URL_ENV);
        let temp_dir = TempDir::new().unwrap();
        let secret_path = temp_dir.path().join("database-url");
        std::fs::write(
            &secret_path,
            "postgres://postgres:postgres@localhost:5432/file-backed",
        )
        .unwrap();
        let loaded = write_test_config(&format!(
            r#"
[core]
backend = "postgres"

[postgres]
database_url_file = "{}"
"#,
            secret_path.display()
        ));

        let selection = loaded.backend_selection().unwrap();
        assert_eq!(
            selection,
            BackendSelection::Postgres {
                database_url: "postgres://postgres:postgres@localhost:5432/file-backed".to_string()
            }
        );

        match previous_env {
            Some(value) => std::env::set_var(DATABASE_URL_ENV, value),
            None => std::env::remove_var(DATABASE_URL_ENV),
        }
    }

    #[test]
    fn postgres_backend_rejects_invalid_database_url() {
        let _guard = with_database_url_env_lock();
        std::env::remove_var(DATABASE_URL_ENV);
        let mut config = YarliConfig::default();
        config.core.backend = BackendKind::Postgres;
        config.postgres.database_url = Some("not-a-valid-url".to_string());
        let loaded = LoadedConfig {
            path: PathBuf::from("yarli.toml"),
            source: ConfigSource::Defaults,
            config,
        };
        let err = loaded.backend_selection().unwrap_err();
        assert!(err.to_string().contains("invalid postgres database url"));
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
        assert!(msg.contains("preflight failed"), "unexpected error: {msg}");
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
        assert!(msg.contains("preflight failed"), "unexpected error: {msg}");
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
    fn preflight_cli_backend_times_out_when_command_hangs() {
        let invocation = CliInvocationConfig {
            command: "sh".to_string(),
            args: vec!["-c".to_string(), "sleep 1".to_string()],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };
        let err = execute_preflight_command_with_timeout(
            &invocation,
            &format!("Respond with exactly: {PREFLIGHT_MARKER}"),
            Duration::from_millis(10),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("timed out"), "unexpected error: {msg}");
        assert!(
            msg.contains("before Yarli can register a run"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn preflight_cli_backend_succeeds_after_model_fallback() {
        let temp_dir = TempDir::new().unwrap();
        let command = write_test_cli_script(
            &temp_dir,
            "claude",
            r#"
if [ "${1:-}" = "--model" ]; then
  echo "primary-model-unsupported" >&2
  exit 12
fi
printf '%s\n' "${1:-}"
"#,
        );
        let invocation = CliInvocationConfig {
            command,
            args: vec!["--model".to_string(), "claude-sonnet-4-6".to_string()],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };

        preflight_cli_backend(&invocation).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn preflight_cli_backend_reports_primary_and_fallback_failures() {
        let temp_dir = TempDir::new().unwrap();
        let command = write_test_cli_script(
            &temp_dir,
            "claude",
            r#"
if [ "${1:-}" = "--model" ]; then
  echo "primary-model-unsupported" >&2
  exit 12
fi
echo "fallback-still-broken" >&2
exit 23
"#,
        );
        let invocation = CliInvocationConfig {
            command,
            args: vec!["--model".to_string(), "claude-sonnet-4-6".to_string()],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };

        let err = preflight_cli_backend(&invocation).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("fallback without `--model` also failed"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("Primary attempt"), "unexpected error: {msg}");
        assert!(msg.contains("Fallback attempt"), "unexpected error: {msg}");
        assert!(
            msg.contains("primary-model-unsupported"),
            "unexpected error: {msg}"
        );
        assert!(
            msg.contains("fallback-still-broken"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn preflight_cli_backend_applies_timeout_to_fallback_attempt() {
        let temp_dir = TempDir::new().unwrap();
        let command = write_test_cli_script(
            &temp_dir,
            "claude",
            r#"
if [ "${1:-}" = "--model" ]; then
  echo "primary-model-unsupported" >&2
  exit 12
fi
sleep 1
"#,
        );
        let invocation = CliInvocationConfig {
            command,
            args: vec!["--model".to_string(), "claude-sonnet-4-6".to_string()],
            prompt_mode: PromptMode::Arg,
            env_unset: vec![],
        };

        let err =
            preflight_cli_backend_with_timeout(&invocation, Duration::from_millis(10)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("primary-model-unsupported"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("Fallback attempt"), "unexpected error: {msg}");
        assert!(
            msg.contains("timed out after 10ms"),
            "unexpected error: {msg}"
        );
        assert!(
            msg.contains("before Yarli can register a run"),
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
merge_conflict_resolution = "auto-repair"
"#,
        );
        assert_eq!(
            loaded.config().run.merge_conflict_resolution,
            MergeConflictResolution::AutoRepair
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
    fn tranche_contract_config_parses_and_applies_strict_defaults() {
        let loaded = write_test_config(
            r#"
[run.tranche_contract]
strict = true
"#,
        );
        let contract = &loaded.config().run.tranche_contract;
        assert!(contract.strict);
        assert!(contract.effective_require_verify());
        assert!(contract.effective_require_done_when());
        assert!(contract.effective_enforce_allowed_paths_on_merge());
        assert!(loaded
            .config()
            .run
            .should_surface_allowed_paths_in_prompts());
    }

    #[test]
    fn tranche_contract_config_supports_granular_flags_without_strict_mode() {
        let loaded = write_test_config(
            r#"
[run]
enforce_plan_tranche_allowed_paths = false

[run.tranche_contract]
strict = false
require_verify = true
require_done_when = false
enforce_allowed_paths_on_merge = true
"#,
        );
        let contract = &loaded.config().run.tranche_contract;
        assert!(!contract.strict);
        assert!(contract.effective_require_verify());
        assert!(!contract.effective_require_done_when());
        assert!(contract.effective_enforce_allowed_paths_on_merge());
        assert!(loaded
            .config()
            .run
            .should_surface_allowed_paths_in_prompts());
    }

    #[test]
    fn merge_conflict_resolution_rejects_unknown_values_with_clear_error() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(
            &path,
            r#"
[run]
merge_conflict_resolution = "agentic"
"#,
        )
        .unwrap();

        let err = LoadedConfig::load(&path).unwrap_err();
        let msg = err.to_string();
        let root_msg = err.root_cause().to_string();
        assert!(
            msg.contains("failed to deserialize config from")
                || msg.contains("failed to parse config file"),
            "unexpected error: {msg}"
        );
        assert!(
            root_msg.contains("invalid [run].merge_conflict_resolution"),
            "unexpected root error: {root_msg}"
        );
        assert!(
            root_msg.contains(
                "expected one of: \"fail\", \"manual\", \"auto-repair\", \"llm-assisted\""
            ),
            "unexpected root error: {root_msg}"
        );
        assert!(root_msg.contains("agentic"));
    }

    #[test]
    fn merge_conflict_resolution_defaults_to_fail() {
        let loaded = write_test_config("[run]\n");
        assert_eq!(
            loaded.config().run.merge_conflict_resolution,
            MergeConflictResolution::Fail
        );
    }

    #[test]
    fn merge_conflict_resolution_parses_llm_assisted_with_command() {
        let loaded = write_test_config(
            r#"
[run]
merge_conflict_resolution = "llm-assisted"
merge_repair_command = "claude --print"
merge_repair_timeout_seconds = 120
"#,
        );
        assert_eq!(
            loaded.config().run.merge_conflict_resolution,
            MergeConflictResolution::LlmAssisted
        );
        assert_eq!(
            loaded.config().run.merge_repair_command.as_deref(),
            Some("claude --print")
        );
        assert_eq!(loaded.config().run.merge_repair_timeout_seconds, 120);
    }

    #[test]
    fn merge_repair_config_defaults() {
        let loaded = write_test_config("[run]\n");
        assert_eq!(loaded.config().run.merge_repair_command, None);
        assert_eq!(loaded.config().run.merge_repair_timeout_seconds, 300);
    }

    #[test]
    fn validate_merge_repair_config_rejects_llm_assisted_without_command() {
        let config = RunConfig {
            merge_conflict_resolution: MergeConflictResolution::LlmAssisted,
            merge_repair_command: None,
            ..RunConfig::default()
        };
        let err = config.validate_merge_repair_config().unwrap_err();
        assert!(err.to_string().contains("merge_repair_command"));

        let config_empty = RunConfig {
            merge_conflict_resolution: MergeConflictResolution::LlmAssisted,
            merge_repair_command: Some("  ".to_string()),
            ..RunConfig::default()
        };
        let err = config_empty.validate_merge_repair_config().unwrap_err();
        assert!(err.to_string().contains("merge_repair_command"));
    }

    #[test]
    fn validate_merge_repair_config_allows_non_llm_assisted_without_command() {
        for strategy in [
            MergeConflictResolution::Fail,
            MergeConflictResolution::AutoRepair,
            MergeConflictResolution::Manual,
        ] {
            let config = RunConfig {
                merge_conflict_resolution: strategy,
                merge_repair_command: None,
                ..RunConfig::default()
            };
            config.validate_merge_repair_config().unwrap();
        }
    }

    #[test]
    fn parallel_worktree_defaults_to_true() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        let loaded = LoadedConfig::load(&path).unwrap();
        assert!(loaded.config().features.parallel_worktree);
    }

    #[test]
    fn parallel_worktree_can_be_disabled() {
        let loaded = write_test_config(
            r#"
[features]
parallel_worktree = false
"#,
        );
        assert!(!loaded.config().features.parallel_worktree);
    }

    #[test]
    fn auto_commit_interval_defaults_to_one() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        let loaded = LoadedConfig::load(&path).unwrap();
        assert_eq!(loaded.config().run.auto_commit_interval, 1);
    }

    #[test]
    fn auto_commit_interval_zero_disables() {
        let loaded = write_test_config(
            r#"
[run]
auto_commit_interval = 0
"#,
        );
        assert_eq!(loaded.config().run.auto_commit_interval, 0);
    }

    #[test]
    fn auto_commit_message_template_parsed() {
        let loaded = write_test_config(
            r#"
[run]
auto_commit_message = "checkpoint: {tranche_key}"
"#,
        );
        assert_eq!(
            loaded.config().run.auto_commit_message.as_deref(),
            Some("checkpoint: {tranche_key}")
        );
    }

    // -----------------------------------------------------------------------
    // Round-trip migration integration tests
    // -----------------------------------------------------------------------

    #[test]
    fn migration_allow_stable_auto_advance_round_trips_to_stable_ok() {
        let loaded = write_test_config(
            r#"
[run]
allow_stable_auto_advance = true
"#,
        );
        assert_eq!(
            loaded.config().run.effective_auto_advance_policy(),
            AutoAdvancePolicy::StableOk,
        );
    }

    #[test]
    fn migration_enforce_plan_tranche_allowed_paths_round_trips() {
        let loaded = write_test_config(
            r#"
[run]
enforce_plan_tranche_allowed_paths = true
"#,
        );
        assert!(loaded
            .config()
            .run
            .should_surface_allowed_paths_in_prompts());
    }

    #[test]
    fn continuation_drift_policy_defaults_to_refuse() {
        let loaded = write_test_config("[run]\n");
        assert_eq!(
            loaded.config().run.continuation_drift_policy,
            ContinuationDriftPolicy::Refuse,
        );
    }

    #[test]
    fn continuation_drift_policy_parses_fallback_fresh() {
        let loaded = write_test_config(
            r#"
[run]
continuation_drift_policy = "fallback-fresh"
"#,
        );
        assert_eq!(
            loaded.config().run.continuation_drift_policy,
            ContinuationDriftPolicy::FallbackFresh,
        );
    }

    #[test]
    fn continuation_drift_policy_parses_refuse_explicitly() {
        let loaded = write_test_config(
            r#"
[run]
continuation_drift_policy = "refuse"
"#,
        );
        assert_eq!(
            loaded.config().run.continuation_drift_policy,
            ContinuationDriftPolicy::Refuse,
        );
    }
}
