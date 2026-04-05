//! Two-pass config migration layer for deprecated `yarli.toml` keys.
//!
//! Each [`MigrationRule`] detects a deprecated config path, optionally remaps
//! its value into the replacement location (only when the new-style key is not
//! already explicitly set), and produces a human-readable warning.

use toml::Value;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

pub(crate) struct AppliedMigration {
    pub(crate) rule_id: &'static str,
    pub(crate) warning: String,
}

pub(crate) struct MigrationReport {
    pub(crate) applied: Vec<AppliedMigration>,
}

// ---------------------------------------------------------------------------
// Internal rule definition
// ---------------------------------------------------------------------------

struct MigrationRule {
    id: &'static str,
    deprecated_path: &'static str,
    replacement_path: &'static str,
    apply: fn(root: &mut Value) -> bool,
}

// ---------------------------------------------------------------------------
// Rule implementations
// ---------------------------------------------------------------------------

/// Rule 1: `run.allow_stable_auto_advance = true` →
///   Set `run.auto_advance_policy = "stable-ok"` (only when policy is absent
///   or `"improving-only"`).
fn apply_allow_stable_auto_advance(root: &mut Value) -> bool {
    let run = match root.get("run").and_then(|v| v.as_table()) {
        Some(t) => t,
        None => return false,
    };

    let flag = match run
        .get("allow_stable_auto_advance")
        .and_then(|v| v.as_bool())
    {
        Some(v) => v,
        None => return false,
    };

    if !flag {
        // Deprecated key is present but false — detected, no remap needed.
        return true;
    }

    // Only remap when auto_advance_policy is absent or "improving-only".
    let current_policy = run
        .get("auto_advance_policy")
        .and_then(|v| v.as_str())
        .unwrap_or("improving-only");

    if current_policy != "improving-only" {
        // Explicit new-style policy wins — detected but no remap.
        return true;
    }

    // Perform the remap.
    let run = root
        .get_mut("run")
        .and_then(|v| v.as_table_mut())
        .expect("run table verified above");
    run.insert(
        "auto_advance_policy".to_string(),
        Value::String("stable-ok".to_string()),
    );

    true
}

/// Rule 2: `run.enforce_plan_tranche_allowed_paths = true` →
///   Set `run.tranche_contract.enforce_allowed_paths_on_merge = true`
///   (only when the target is not already set).
fn apply_enforce_plan_tranche_allowed_paths(root: &mut Value) -> bool {
    let run = match root.get("run").and_then(|v| v.as_table()) {
        Some(t) => t,
        None => return false,
    };

    let flag = match run
        .get("enforce_plan_tranche_allowed_paths")
        .and_then(|v| v.as_bool())
    {
        Some(v) => v,
        None => return false,
    };

    if !flag {
        return true;
    }

    // Check whether the target is already explicitly set.
    let already_set = run
        .get("tranche_contract")
        .and_then(|v| v.as_table())
        .and_then(|t| t.get("enforce_allowed_paths_on_merge"))
        .is_some();

    if already_set {
        return true;
    }

    // Perform the remap: ensure tranche_contract table exists, then insert.
    let run = root
        .get_mut("run")
        .and_then(|v| v.as_table_mut())
        .expect("run table verified above");

    let contract = run
        .entry("tranche_contract")
        .or_insert_with(|| Value::Table(toml::map::Map::new()));
    if let Some(table) = contract.as_table_mut() {
        table.insert(
            "enforce_allowed_paths_on_merge".to_string(),
            Value::Boolean(true),
        );
    }

    true
}

/// Rule 3: `memory.backend` table (non-empty) →
///   Copy into `memory.providers.legacy-backend` and set
///   `memory.provider = "legacy-backend"` (only when `provider` is unset and
///   `providers` is empty).
fn apply_memory_backend(root: &mut Value) -> bool {
    let memory = match root.get("memory").and_then(|v| v.as_table()) {
        Some(t) => t,
        None => return false,
    };

    let backend = match memory.get("backend").and_then(|v| v.as_table()) {
        Some(t) if !t.is_empty() => t.clone(),
        _ => return false,
    };

    // Detected. Check whether new-style config is already set.
    let provider_set = memory
        .get("provider")
        .and_then(|v| v.as_str())
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);

    let providers_nonempty = memory
        .get("providers")
        .and_then(|v| v.as_table())
        .map(|t| !t.is_empty())
        .unwrap_or(false);

    if provider_set || providers_nonempty {
        // New-style config exists — detected but no remap.
        return true;
    }

    // Perform the remap.
    let memory = root
        .get_mut("memory")
        .and_then(|v| v.as_table_mut())
        .expect("memory table verified above");

    // Create providers.legacy-backend with the backend contents.
    let mut providers = toml::map::Map::new();
    providers.insert("legacy-backend".to_string(), Value::Table(backend));
    memory.insert("providers".to_string(), Value::Table(providers));
    memory.insert(
        "provider".to_string(),
        Value::String("legacy-backend".to_string()),
    );

    true
}

// ---------------------------------------------------------------------------
// Rule registry
// ---------------------------------------------------------------------------

const RULES: &[MigrationRule] = &[
    MigrationRule {
        id: "deprecated-allow-stable-auto-advance",
        deprecated_path: "run.allow_stable_auto_advance",
        replacement_path: "run.auto_advance_policy",
        apply: apply_allow_stable_auto_advance,
    },
    MigrationRule {
        id: "deprecated-enforce-plan-tranche-allowed-paths",
        deprecated_path: "run.enforce_plan_tranche_allowed_paths",
        replacement_path: "run.tranche_contract.enforce_allowed_paths_on_merge",
        apply: apply_enforce_plan_tranche_allowed_paths,
    },
    MigrationRule {
        id: "deprecated-memory-backend",
        deprecated_path: "memory.backend",
        replacement_path: "memory.providers.<name>",
        apply: apply_memory_backend,
    },
];

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub(crate) fn apply_migrations(root: &mut Value) -> MigrationReport {
    let mut applied = Vec::new();

    for rule in RULES {
        if (rule.apply)(root) {
            applied.push(AppliedMigration {
                rule_id: rule.id,
                warning: format!(
                    "deprecated config key `{}` found; migrate to `{}`",
                    rule.deprecated_path, rule.replacement_path,
                ),
            });
        }
    }

    MigrationReport { applied }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml_str: &str) -> Value {
        toml::from_str(toml_str).expect("test TOML should parse")
    }

    fn rule_ids(report: &MigrationReport) -> Vec<&'static str> {
        report.applied.iter().map(|m| m.rule_id).collect()
    }

    // -----------------------------------------------------------------------
    // Rule 1: allow_stable_auto_advance
    // -----------------------------------------------------------------------

    #[test]
    fn rule1_true_absent_policy_remaps_to_stable_ok() {
        let mut root = parse(
            r#"
[run]
allow_stable_auto_advance = true
"#,
        );
        let report = apply_migrations(&mut root);
        assert_eq!(
            rule_ids(&report),
            vec!["deprecated-allow-stable-auto-advance"]
        );
        assert_eq!(
            root["run"]["auto_advance_policy"].as_str().unwrap(),
            "stable-ok"
        );
    }

    #[test]
    fn rule1_true_improving_only_remaps_to_stable_ok() {
        let mut root = parse(
            r#"
[run]
allow_stable_auto_advance = true
auto_advance_policy = "improving-only"
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-allow-stable-auto-advance"));
        assert_eq!(
            root["run"]["auto_advance_policy"].as_str().unwrap(),
            "stable-ok"
        );
    }

    #[test]
    fn rule1_true_explicit_always_no_override() {
        let mut root = parse(
            r#"
[run]
allow_stable_auto_advance = true
auto_advance_policy = "always"
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-allow-stable-auto-advance"));
        // Explicit policy preserved.
        assert_eq!(
            root["run"]["auto_advance_policy"].as_str().unwrap(),
            "always"
        );
    }

    #[test]
    fn rule1_false_detected_no_remap() {
        let mut root = parse(
            r#"
[run]
allow_stable_auto_advance = false
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-allow-stable-auto-advance"));
        // No auto_advance_policy injected.
        assert!(root["run"].get("auto_advance_policy").is_none());
    }

    #[test]
    fn rule1_absent_no_detection() {
        let mut root = parse(
            r#"
[run]
objective = "test"
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(!rule_ids(&report).contains(&"deprecated-allow-stable-auto-advance"));
    }

    // -----------------------------------------------------------------------
    // Rule 2: enforce_plan_tranche_allowed_paths
    // -----------------------------------------------------------------------

    #[test]
    fn rule2_true_no_contract_creates_entry() {
        let mut root = parse(
            r#"
[run]
enforce_plan_tranche_allowed_paths = true
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-enforce-plan-tranche-allowed-paths"));
        assert!(
            root["run"]["tranche_contract"]["enforce_allowed_paths_on_merge"]
                .as_bool()
                .unwrap(),
        );
    }

    #[test]
    fn rule2_true_existing_contract_no_override() {
        let mut root = parse(
            r#"
[run]
enforce_plan_tranche_allowed_paths = true

[run.tranche_contract]
enforce_allowed_paths_on_merge = false
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-enforce-plan-tranche-allowed-paths"));
        // Explicit value preserved.
        assert!(
            !root["run"]["tranche_contract"]["enforce_allowed_paths_on_merge"]
                .as_bool()
                .unwrap(),
        );
    }

    #[test]
    fn rule2_false_detected_no_remap() {
        let mut root = parse(
            r#"
[run]
enforce_plan_tranche_allowed_paths = false
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-enforce-plan-tranche-allowed-paths"));
        assert!(root["run"].get("tranche_contract").is_none());
    }

    #[test]
    fn rule2_absent_no_detection() {
        let mut root = parse(
            r#"
[run]
objective = "test"
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(!rule_ids(&report).contains(&"deprecated-enforce-plan-tranche-allowed-paths"));
    }

    // -----------------------------------------------------------------------
    // Rule 3: memory.backend
    // -----------------------------------------------------------------------

    #[test]
    fn rule3_backend_present_providers_empty_remaps() {
        let mut root = parse(
            r#"
[memory.backend]
enabled = true
command = "my-backend"
query_limit = 10
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-memory-backend"));
        assert_eq!(
            root["memory"]["provider"].as_str().unwrap(),
            "legacy-backend"
        );
        let lb = &root["memory"]["providers"]["legacy-backend"];
        assert_eq!(lb["command"].as_str().unwrap(), "my-backend");
        assert_eq!(lb["query_limit"].as_integer().unwrap(), 10);
        assert!(lb["enabled"].as_bool().unwrap());
    }

    #[test]
    fn rule3_backend_with_provider_set_detected_no_remap() {
        let mut root = parse(
            r#"
[memory]
provider = "custom"

[memory.backend]
enabled = true
command = "my-backend"
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-memory-backend"));
        // provider not overwritten.
        assert_eq!(root["memory"]["provider"].as_str().unwrap(), "custom");
        // providers not created.
        assert!(root["memory"].get("providers").is_none());
    }

    #[test]
    fn rule3_backend_with_providers_nonempty_detected_no_remap() {
        let mut root = parse(
            r#"
[memory.backend]
enabled = true
command = "old"

[memory.providers.custom]
command = "new"
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(rule_ids(&report).contains(&"deprecated-memory-backend"));
        // Existing providers not clobbered.
        assert!(root["memory"]["providers"]
            .as_table()
            .unwrap()
            .contains_key("custom"));
        assert!(!root["memory"]["providers"]
            .as_table()
            .unwrap()
            .contains_key("legacy-backend"));
    }

    #[test]
    fn rule3_backend_absent_no_detection() {
        let mut root = parse(
            r#"
[memory]
enabled = true
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(!rule_ids(&report).contains(&"deprecated-memory-backend"));
    }

    #[test]
    fn rule3_backend_empty_table_no_detection() {
        let mut root = parse(
            r#"
[memory]
enabled = true

[memory.backend]
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(!rule_ids(&report).contains(&"deprecated-memory-backend"));
    }

    // -----------------------------------------------------------------------
    // Aggregate tests
    // -----------------------------------------------------------------------

    #[test]
    fn clean_config_produces_empty_report() {
        let mut root = parse(
            r#"
[core]
backend = "in-memory"

[run]
auto_advance_policy = "stable-ok"
"#,
        );
        let report = apply_migrations(&mut root);
        assert!(report.applied.is_empty());
    }

    #[test]
    fn all_three_deprecated_patterns_fire() {
        let mut root = parse(
            r#"
[run]
allow_stable_auto_advance = true
enforce_plan_tranche_allowed_paths = true

[memory.backend]
enabled = true
command = "old-cmd"
"#,
        );
        let report = apply_migrations(&mut root);
        let ids = rule_ids(&report);
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"deprecated-allow-stable-auto-advance"));
        assert!(ids.contains(&"deprecated-enforce-plan-tranche-allowed-paths"));
        assert!(ids.contains(&"deprecated-memory-backend"));
    }
}
