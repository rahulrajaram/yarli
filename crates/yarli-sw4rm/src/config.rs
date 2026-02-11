//! Configuration for the sw4rm agent integration.

use serde::{Deserialize, Serialize};

/// sw4rm agent configuration (maps to `[sw4rm]` section in yarli.toml).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Sw4rmConfig {
    /// Agent display name registered with the sw4rm registry.
    #[serde(default = "default_agent_name")]
    pub agent_name: String,

    /// Capabilities advertised to the scheduler.
    #[serde(default = "default_capabilities")]
    pub capabilities: Vec<String>,

    /// sw4rm registry service URL.
    #[serde(default = "default_registry_url")]
    pub registry_url: String,

    /// sw4rm router service URL.
    #[serde(default = "default_router_url")]
    pub router_url: String,

    /// sw4rm scheduler service URL.
    #[serde(default = "default_scheduler_url")]
    pub scheduler_url: String,

    /// Maximum fix-it iterations before giving up.
    #[serde(default = "default_max_fix_iterations")]
    pub max_fix_iterations: u32,

    /// Timeout waiting for an LLM agent response (seconds).
    #[serde(default = "default_llm_response_timeout_secs")]
    pub llm_response_timeout_secs: u64,
}

impl Default for Sw4rmConfig {
    fn default() -> Self {
        Self {
            agent_name: default_agent_name(),
            capabilities: default_capabilities(),
            registry_url: default_registry_url(),
            router_url: default_router_url(),
            scheduler_url: default_scheduler_url(),
            max_fix_iterations: default_max_fix_iterations(),
            llm_response_timeout_secs: default_llm_response_timeout_secs(),
        }
    }
}

fn default_agent_name() -> String {
    "yarli-orchestrator".to_string()
}

fn default_capabilities() -> Vec<String> {
    vec![
        "orchestrate".to_string(),
        "verify".to_string(),
        "git".to_string(),
    ]
}

fn default_registry_url() -> String {
    "http://127.0.0.1:50051".to_string()
}

fn default_router_url() -> String {
    "http://127.0.0.1:50052".to_string()
}

fn default_scheduler_url() -> String {
    "http://127.0.0.1:50053".to_string()
}

fn default_max_fix_iterations() -> u32 {
    5
}

fn default_llm_response_timeout_secs() -> u64 {
    300
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_expected_values() {
        let cfg = Sw4rmConfig::default();
        assert_eq!(cfg.agent_name, "yarli-orchestrator");
        assert_eq!(cfg.capabilities, vec!["orchestrate", "verify", "git"]);
        assert_eq!(cfg.max_fix_iterations, 5);
        assert_eq!(cfg.llm_response_timeout_secs, 300);
    }

    #[test]
    fn config_round_trips_through_json() {
        let cfg = Sw4rmConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let parsed: Sw4rmConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, parsed);
    }

    #[test]
    fn config_deserializes_partial_toml() {
        let toml_str = r#"
agent_name = "my-agent"
max_fix_iterations = 10
"#;
        let cfg: Sw4rmConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.agent_name, "my-agent");
        assert_eq!(cfg.max_fix_iterations, 10);
        // defaults filled in
        assert_eq!(cfg.llm_response_timeout_secs, 300);
        assert_eq!(cfg.registry_url, "http://127.0.0.1:50051");
    }
}
