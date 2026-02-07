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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct YarliConfig {
    #[serde(default)]
    pub core: CoreConfig,
    #[serde(default)]
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
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
}

impl Default for YarliConfig {
    fn default() -> Self {
        Self {
            core: CoreConfig::default(),
            postgres: PostgresConfig::default(),
            queue: QueueConfig::default(),
            execution: ExecutionConfig::default(),
            git: GitConfig::default(),
            policy: PolicyConfig::default(),
            memory: MemoryConfig::default(),
            observability: ObservabilityConfig::default(),
            ui: UiConfig::default(),
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresConfig {
    #[serde(default)]
    pub database_url: Option<String>,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self { database_url: None }
    }
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
    #[serde(default = "default_working_dir")]
    pub working_dir: String,
    #[serde(default = "default_command_timeout_seconds")]
    pub command_timeout_seconds: u64,
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            working_dir: default_working_dir(),
            command_timeout_seconds: default_command_timeout_seconds(),
            tick_interval_ms: default_tick_interval_ms(),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryConfig {
    #[serde(default)]
    pub backend: MemoryBackendConfig,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            haake: MemoryBackendConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryBackendConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl Default for MemoryBackendConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UiConfig {
    #[serde(default)]
    pub mode: UiMode,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            mode: UiMode::default(),
        }
    }
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
        assert_eq!(loaded.config().execution.working_dir, ".");
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

[queue]
claim_batch_size = 2

[execution]
working_dir = "/work"
command_timeout_seconds = 600

[git]
default_target_branch = "develop"

[policy]
enforce_policies = true

[memory.backend]
enabled = true
endpoint = "http://localhost:8080"

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
        assert_eq!(loaded.config().memory.backend.enabled, true);
        assert_eq!(loaded.config().ui.mode, UiMode::Stream);
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
            "queue",
            "execution",
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
        assert_eq!(loaded.backend_selection().unwrap(), BackendSelection::InMemory);
    }

    #[test]
    fn backend_kind_as_str() {
        assert_eq!(BackendKind::InMemory.as_str(), "in-memory");
        assert_eq!(BackendKind::Postgres.as_str(), "postgres");
    }
}
