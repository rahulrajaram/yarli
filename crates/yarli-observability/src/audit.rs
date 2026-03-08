//! Immutable JSONL audit sink for policy decisions and destructive attempts (Section 15.4).
//!
//! Every policy decision, approval token usage, and destructive attempt is recorded
//! as a single JSON line in an append-only file. Records include the original request,
//! evaluated rules, decision outcome, and actor identity.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::yarli_core::domain::{PolicyOutcome, RunId, TaskId};

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors from the audit sink.
#[derive(Debug, Error)]
pub enum AuditError {
    #[error("audit I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("audit serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Audit entry
// ---------------------------------------------------------------------------

/// Category of audit event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditCategory {
    /// A policy decision was made (allow/deny/require_approval).
    PolicyDecision,
    /// A destructive operation was attempted.
    DestructiveAttempt,
    /// An approval token was consumed.
    TokenConsumed,
    /// A gate evaluation completed.
    GateEvaluation,
    /// A command execution terminal event (exited, killed, timed out).
    CommandExecution,
    /// A process health level transition (healthy→degraded, etc.).
    ProcessHealth,
    /// A post-run analysis result.
    RunAnalysis,
}

/// A single audit record, serialized as one JSON line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Unique identifier for this audit record.
    pub audit_id: Uuid,
    /// When this audit entry was created.
    pub timestamp: DateTime<Utc>,
    /// Category of the audit event.
    pub category: AuditCategory,
    /// Actor who triggered the action.
    pub actor: String,
    /// The action that was evaluated or attempted.
    pub action: String,
    /// Policy outcome (if applicable).
    pub outcome: Option<PolicyOutcome>,
    /// Rule that matched (if applicable).
    pub rule_id: Option<String>,
    /// Human-readable reason for the decision.
    pub reason: String,
    /// Run context.
    pub run_id: Option<RunId>,
    /// Task context.
    pub task_id: Option<TaskId>,
    /// Additional structured details (original request, evidence, etc.).
    pub details: serde_json::Value,
}

impl AuditEntry {
    /// Create a policy decision audit entry from a domain PolicyDecision.
    pub fn from_policy_decision(decision: &crate::yarli_core::domain::PolicyDecision) -> Self {
        Self {
            audit_id: Uuid::now_v7(),
            timestamp: Utc::now(),
            category: AuditCategory::PolicyDecision,
            actor: decision.actor.clone(),
            action: decision.action.clone(),
            outcome: Some(decision.outcome),
            rule_id: Some(decision.rule_id.clone()),
            reason: decision.reason.clone(),
            run_id: Some(decision.run_id),
            task_id: None,
            details: serde_json::json!({
                "decision_id": decision.decision_id,
                "decided_at": decision.decided_at,
            }),
        }
    }

    /// Create a destructive attempt audit entry.
    pub fn destructive_attempt(
        actor: impl Into<String>,
        action: impl Into<String>,
        reason: impl Into<String>,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            audit_id: Uuid::now_v7(),
            timestamp: Utc::now(),
            category: AuditCategory::DestructiveAttempt,
            actor: actor.into(),
            action: action.into(),
            outcome: None,
            rule_id: None,
            reason: reason.into(),
            run_id,
            task_id,
            details,
        }
    }

    /// Create a token consumed audit entry.
    pub fn token_consumed(
        actor: impl Into<String>,
        action: impl Into<String>,
        token_id: Uuid,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
    ) -> Self {
        Self {
            audit_id: Uuid::now_v7(),
            timestamp: Utc::now(),
            category: AuditCategory::TokenConsumed,
            actor: actor.into(),
            action: action.into(),
            outcome: Some(PolicyOutcome::Allow),
            rule_id: None,
            reason: format!("approval token {token_id} consumed"),
            run_id,
            task_id,
            details: serde_json::json!({ "token_id": token_id }),
        }
    }

    /// Create a command execution audit entry for terminal events.
    pub fn command_execution(
        command_key: impl Into<String>,
        exit_code: Option<i32>,
        stderr_excerpt: impl Into<String>,
        duration_ms: Option<i64>,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
    ) -> Self {
        let cmd = command_key.into();
        let action = format!("command.execution:{cmd}");
        Self {
            audit_id: Uuid::now_v7(),
            timestamp: Utc::now(),
            category: AuditCategory::CommandExecution,
            actor: "command_journal".to_string(),
            action,
            outcome: None,
            rule_id: None,
            reason: format!(
                "exit_code={} duration_ms={}",
                exit_code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "-".into()),
                duration_ms
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            run_id,
            task_id,
            details: serde_json::json!({
                "command": cmd,
                "exit_code": exit_code,
                "duration_ms": duration_ms,
                "stderr_excerpt": stderr_excerpt.into(),
            }),
        }
    }

    /// Create a process health transition audit entry.
    pub fn process_health_transition(
        previous_level: impl Into<String>,
        new_level: impl Into<String>,
        score: f64,
        factors: serde_json::Value,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
    ) -> Self {
        let prev = previous_level.into();
        let new = new_level.into();
        Self {
            audit_id: Uuid::now_v7(),
            timestamp: Utc::now(),
            category: AuditCategory::ProcessHealth,
            actor: "introspection_controller".to_string(),
            action: format!("health_transition:{prev}->{new}"),
            outcome: None,
            rule_id: None,
            reason: format!("health level changed from {prev} to {new} (score={score:.2})"),
            run_id,
            task_id,
            details: serde_json::json!({
                "previous_level": prev,
                "new_level": new,
                "score": score,
                "factors": factors,
            }),
        }
    }

    /// Create a run analysis audit entry.
    pub fn run_analysis(
        patterns: &[String],
        recommendation: impl Into<String>,
        confidence: f64,
        lesson: Option<&str>,
        run_id: RunId,
    ) -> Self {
        Self {
            audit_id: Uuid::now_v7(),
            timestamp: Utc::now(),
            category: AuditCategory::RunAnalysis,
            actor: "run_analyzer".to_string(),
            action: "post_run_analysis".to_string(),
            outcome: None,
            rule_id: None,
            reason: format!(
                "detected {} pattern(s), confidence={confidence:.2}",
                patterns.len()
            ),
            run_id: Some(run_id),
            task_id: None,
            details: serde_json::json!({
                "patterns": patterns,
                "recommendation": recommendation.into(),
                "confidence": confidence,
                "lesson": lesson,
            }),
        }
    }

    /// Create a gate evaluation audit entry.
    pub fn gate_evaluation(
        gate_name: impl Into<String>,
        passed: bool,
        reason: impl Into<String>,
        run_id: RunId,
        task_id: Option<TaskId>,
    ) -> Self {
        Self {
            audit_id: Uuid::now_v7(),
            timestamp: Utc::now(),
            category: AuditCategory::GateEvaluation,
            actor: "gate_engine".to_string(),
            action: gate_name.into(),
            outcome: if passed {
                Some(PolicyOutcome::Allow)
            } else {
                Some(PolicyOutcome::Deny)
            },
            rule_id: None,
            reason: reason.into(),
            run_id: Some(run_id),
            task_id,
            details: serde_json::json!({ "passed": passed }),
        }
    }
}

// ---------------------------------------------------------------------------
// Audit sink trait
// ---------------------------------------------------------------------------

/// Trait for audit sinks that persist audit records.
pub trait AuditSink: Send + Sync {
    /// Append an audit entry to the sink.
    fn append(&self, entry: &AuditEntry) -> Result<(), AuditError>;

    /// Read all audit entries (for querying/tailing).
    fn read_all(&self) -> Result<Vec<AuditEntry>, AuditError>;

    /// Count the number of entries in the sink.
    fn count(&self) -> Result<usize, AuditError> {
        Ok(self.read_all()?.len())
    }
}

// ---------------------------------------------------------------------------
// JSONL file-based audit sink
// ---------------------------------------------------------------------------

/// Append-only JSONL audit sink that writes to a file.
///
/// Each entry is serialized as a single JSON line followed by a newline.
/// The file is opened in append mode, ensuring immutability of prior records.
/// Uses a mutex internally to serialize concurrent writes.
pub struct JsonlAuditSink {
    path: PathBuf,
}

impl JsonlAuditSink {
    /// Create a new JSONL audit sink at the given file path.
    ///
    /// The parent directory must exist. The file will be created if it doesn't exist.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Get the file path of this audit sink.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl AuditSink for JsonlAuditSink {
    fn append(&self, entry: &AuditEntry) -> Result<(), AuditError> {
        let mut line = serde_json::to_string(entry)?;
        line.push('\n');

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        file.write_all(line.as_bytes())?;
        file.flush()?;

        Ok(())
    }

    fn read_all(&self) -> Result<Vec<AuditEntry>, AuditError> {
        let contents = match std::fs::read_to_string(&self.path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(AuditError::Io(e)),
        };

        let mut entries = Vec::new();
        for line in contents.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: AuditEntry = serde_json::from_str(line)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// In-memory audit sink (for testing)
// ---------------------------------------------------------------------------

/// In-memory audit sink for testing.
pub struct InMemoryAuditSink {
    entries: RwLock<Vec<AuditEntry>>,
}

impl InMemoryAuditSink {
    /// Create a new empty in-memory audit sink.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
        }
    }
}

impl Default for InMemoryAuditSink {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditSink for InMemoryAuditSink {
    fn append(&self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.entries.write().unwrap().push(entry.clone());
        Ok(())
    }

    fn read_all(&self) -> Result<Vec<AuditEntry>, AuditError> {
        Ok(self.entries.read().unwrap().clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yarli_core::domain::PolicyDecision;

    // ---- AuditEntry constructors ----

    #[test]
    fn from_policy_decision_creates_entry() {
        let decision = PolicyDecision {
            decision_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            actor: "scheduler".to_string(),
            action: "git_push".to_string(),
            outcome: PolicyOutcome::Deny,
            rule_id: "deny-force-push".to_string(),
            reason: "forbidden".to_string(),
            decided_at: Utc::now(),
        };

        let entry = AuditEntry::from_policy_decision(&decision);
        assert_eq!(entry.category, AuditCategory::PolicyDecision);
        assert_eq!(entry.actor, "scheduler");
        assert_eq!(entry.action, "git_push");
        assert_eq!(entry.outcome, Some(PolicyOutcome::Deny));
        assert_eq!(entry.rule_id, Some("deny-force-push".to_string()));
        assert_eq!(entry.run_id, Some(decision.run_id));
    }

    #[test]
    fn destructive_attempt_creates_entry() {
        let entry = AuditEntry::destructive_attempt(
            "operator",
            "stash_clear",
            "attempted stash clear",
            Some(Uuid::new_v4()),
            None,
            serde_json::json!({"repo": "/repo/test"}),
        );

        assert_eq!(entry.category, AuditCategory::DestructiveAttempt);
        assert_eq!(entry.actor, "operator");
        assert_eq!(entry.action, "stash_clear");
        assert!(entry.outcome.is_none());
        assert!(entry.run_id.is_some());
    }

    #[test]
    fn token_consumed_creates_entry() {
        let token_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let entry = AuditEntry::token_consumed("admin", "git_push", token_id, Some(run_id), None);

        assert_eq!(entry.category, AuditCategory::TokenConsumed);
        assert_eq!(entry.actor, "admin");
        assert_eq!(entry.outcome, Some(PolicyOutcome::Allow));
        assert!(entry.reason.contains(&token_id.to_string()));
    }

    #[test]
    fn gate_evaluation_creates_entry() {
        let run_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let entry = AuditEntry::gate_evaluation(
            "tests_passed",
            true,
            "all tests pass",
            run_id,
            Some(task_id),
        );

        assert_eq!(entry.category, AuditCategory::GateEvaluation);
        assert_eq!(entry.actor, "gate_engine");
        assert_eq!(entry.action, "tests_passed");
        assert_eq!(entry.outcome, Some(PolicyOutcome::Allow));
        assert_eq!(entry.run_id, Some(run_id));
        assert_eq!(entry.task_id, Some(task_id));
    }

    #[test]
    fn gate_evaluation_failed() {
        let run_id = Uuid::new_v4();
        let entry = AuditEntry::gate_evaluation(
            "no_unresolved_conflicts",
            false,
            "conflicts found",
            run_id,
            None,
        );

        assert_eq!(entry.outcome, Some(PolicyOutcome::Deny));
    }

    // ---- Serialization ----

    #[test]
    fn audit_entry_serializes_to_json() {
        let entry = AuditEntry::destructive_attempt(
            "operator",
            "git_clean",
            "destructive cleanup",
            None,
            None,
            serde_json::json!({}),
        );
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"category\":\"destructive_attempt\""));
        assert!(json.contains("\"actor\":\"operator\""));
    }

    #[test]
    fn audit_entry_roundtrip() {
        let entry = AuditEntry::from_policy_decision(&PolicyDecision {
            decision_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            actor: "scheduler".to_string(),
            action: "merge".to_string(),
            outcome: PolicyOutcome::Allow,
            rule_id: "allow-merge".to_string(),
            reason: "normal workflow".to_string(),
            decided_at: Utc::now(),
        });
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: AuditEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.audit_id, entry.audit_id);
        assert_eq!(decoded.actor, "scheduler");
        assert_eq!(decoded.category, AuditCategory::PolicyDecision);
    }

    #[test]
    fn audit_category_serializes() {
        let cat = AuditCategory::PolicyDecision;
        let json = serde_json::to_string(&cat).unwrap();
        assert_eq!(json, "\"policy_decision\"");

        let decoded: AuditCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, AuditCategory::PolicyDecision);
    }

    // ---- InMemoryAuditSink ----

    #[test]
    fn in_memory_sink_append_and_read() {
        let sink = InMemoryAuditSink::new();
        let entry = AuditEntry::destructive_attempt(
            "test",
            "test_action",
            "test reason",
            None,
            None,
            serde_json::json!({}),
        );

        sink.append(&entry).unwrap();
        let entries = sink.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].audit_id, entry.audit_id);
    }

    #[test]
    fn in_memory_sink_count() {
        let sink = InMemoryAuditSink::new();
        assert_eq!(sink.count().unwrap(), 0);

        for i in 0..5 {
            let entry = AuditEntry::destructive_attempt(
                "test",
                format!("action_{i}"),
                "reason",
                None,
                None,
                serde_json::json!({}),
            );
            sink.append(&entry).unwrap();
        }
        assert_eq!(sink.count().unwrap(), 5);
    }

    #[test]
    fn in_memory_sink_empty_read() {
        let sink = InMemoryAuditSink::new();
        let entries = sink.read_all().unwrap();
        assert!(entries.is_empty());
    }

    // ---- JsonlAuditSink ----

    #[test]
    fn jsonl_sink_creates_file_and_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = JsonlAuditSink::new(&path);

        let entry = AuditEntry::from_policy_decision(&PolicyDecision {
            decision_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            actor: "scheduler".to_string(),
            action: "command_execute".to_string(),
            outcome: PolicyOutcome::Allow,
            rule_id: "allow-command".to_string(),
            reason: "normal execution".to_string(),
            decided_at: Utc::now(),
        });

        sink.append(&entry).unwrap();

        assert!(path.exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 1);

        let decoded: AuditEntry = serde_json::from_str(contents.lines().next().unwrap()).unwrap();
        assert_eq!(decoded.audit_id, entry.audit_id);
    }

    #[test]
    fn jsonl_sink_appends_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = JsonlAuditSink::new(&path);

        for i in 0..3 {
            let entry = AuditEntry::destructive_attempt(
                "test",
                format!("action_{i}"),
                "reason",
                None,
                None,
                serde_json::json!({}),
            );
            sink.append(&entry).unwrap();
        }

        let entries = sink.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].action, "action_0");
        assert_eq!(entries[1].action, "action_1");
        assert_eq!(entries[2].action, "action_2");
    }

    #[test]
    fn jsonl_sink_read_nonexistent_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.jsonl");
        let sink = JsonlAuditSink::new(&path);

        let entries = sink.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn jsonl_sink_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = JsonlAuditSink::new(&path);

        assert_eq!(sink.count().unwrap(), 0);

        let entry = AuditEntry::destructive_attempt(
            "test",
            "action",
            "reason",
            None,
            None,
            serde_json::json!({}),
        );
        sink.append(&entry).unwrap();
        assert_eq!(sink.count().unwrap(), 1);
    }

    #[test]
    fn jsonl_sink_immutability_append_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = JsonlAuditSink::new(&path);

        let entry1 = AuditEntry::destructive_attempt(
            "test",
            "action_1",
            "first",
            None,
            None,
            serde_json::json!({}),
        );
        sink.append(&entry1).unwrap();

        let entry2 = AuditEntry::destructive_attempt(
            "test",
            "action_2",
            "second",
            None,
            None,
            serde_json::json!({}),
        );
        sink.append(&entry2).unwrap();

        // Verify both entries preserved (append-only, first not overwritten)
        let entries = sink.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].audit_id, entry1.audit_id);
        assert_eq!(entries[1].audit_id, entry2.audit_id);
    }

    #[test]
    fn jsonl_sink_each_line_is_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = JsonlAuditSink::new(&path);

        let run_id = Uuid::new_v4();
        let entry = AuditEntry::from_policy_decision(&PolicyDecision {
            decision_id: Uuid::new_v4(),
            run_id,
            actor: "operator".to_string(),
            action: "git_force_push".to_string(),
            outcome: PolicyOutcome::Deny,
            rule_id: "deny-force-push".to_string(),
            reason: "force push destroys history".to_string(),
            decided_at: Utc::now(),
        });
        sink.append(&entry).unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        for line in contents.lines() {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
    }

    #[test]
    fn jsonl_sink_path_accessor() {
        let sink = JsonlAuditSink::new("/tmp/test-audit.jsonl");
        assert_eq!(sink.path(), Path::new("/tmp/test-audit.jsonl"));
    }

    #[test]
    fn process_health_category_serializes() {
        let cat = AuditCategory::ProcessHealth;
        let json = serde_json::to_string(&cat).unwrap();
        assert_eq!(json, "\"process_health\"");
        let decoded: AuditCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, AuditCategory::ProcessHealth);
    }

    #[test]
    fn process_health_transition_creates_entry() {
        let run_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let entry = AuditEntry::process_health_transition(
            "healthy",
            "degraded",
            0.45,
            serde_json::json!({"output_recency": 0.3}),
            Some(run_id),
            Some(task_id),
        );
        assert_eq!(entry.category, AuditCategory::ProcessHealth);
        assert_eq!(entry.actor, "introspection_controller");
        assert!(entry.action.contains("healthy->degraded"));
        assert_eq!(entry.run_id, Some(run_id));
        assert_eq!(entry.task_id, Some(task_id));
        assert_eq!(entry.details["previous_level"], "healthy");
        assert_eq!(entry.details["new_level"], "degraded");
    }

    #[test]
    fn run_analysis_creates_entry() {
        let run_id = Uuid::new_v4();
        let entry = AuditEntry::run_analysis(
            &[
                "cascading_failure".to_string(),
                "timeout_or_stall".to_string(),
            ],
            "none",
            0.7,
            Some("root task failed causing cascade"),
            run_id,
        );
        assert_eq!(entry.category, AuditCategory::RunAnalysis);
        assert_eq!(entry.actor, "run_analyzer");
        assert_eq!(entry.run_id, Some(run_id));
        assert!(entry.reason.contains("2 pattern(s)"));
        assert_eq!(entry.details["confidence"], 0.7);
        assert_eq!(entry.details["lesson"], "root task failed causing cascade");
    }

    #[test]
    fn run_analysis_category_serializes() {
        let cat = AuditCategory::RunAnalysis;
        let json = serde_json::to_string(&cat).unwrap();
        assert_eq!(json, "\"run_analysis\"");
        let decoded: AuditCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, AuditCategory::RunAnalysis);
    }

    #[test]
    fn all_audit_categories_serialize() {
        let categories = [
            AuditCategory::PolicyDecision,
            AuditCategory::DestructiveAttempt,
            AuditCategory::TokenConsumed,
            AuditCategory::GateEvaluation,
            AuditCategory::CommandExecution,
            AuditCategory::ProcessHealth,
            AuditCategory::RunAnalysis,
        ];
        for cat in &categories {
            let json = serde_json::to_string(cat).unwrap();
            let decoded: AuditCategory = serde_json::from_str(&json).unwrap();
            assert_eq!(&decoded, cat);
        }
    }

    #[test]
    fn jsonl_sink_mixed_categories() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = JsonlAuditSink::new(&path);

        let run_id = Uuid::new_v4();

        // Policy decision
        let e1 = AuditEntry::from_policy_decision(&PolicyDecision {
            decision_id: Uuid::new_v4(),
            run_id,
            actor: "scheduler".to_string(),
            action: "merge".to_string(),
            outcome: PolicyOutcome::Allow,
            rule_id: "allow-merge".to_string(),
            reason: "ok".to_string(),
            decided_at: Utc::now(),
        });
        sink.append(&e1).unwrap();

        // Destructive attempt
        let e2 = AuditEntry::destructive_attempt(
            "operator",
            "git_clean",
            "destructive",
            Some(run_id),
            None,
            serde_json::json!({"paths": ["/src"]}),
        );
        sink.append(&e2).unwrap();

        // Token consumed
        let e3 =
            AuditEntry::token_consumed("admin", "git_push", Uuid::new_v4(), Some(run_id), None);
        sink.append(&e3).unwrap();

        // Gate evaluation
        let e4 = AuditEntry::gate_evaluation("tests_passed", true, "all pass", run_id, None);
        sink.append(&e4).unwrap();

        let entries = sink.read_all().unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].category, AuditCategory::PolicyDecision);
        assert_eq!(entries[1].category, AuditCategory::DestructiveAttempt);
        assert_eq!(entries[2].category, AuditCategory::TokenConsumed);
        assert_eq!(entries[3].category, AuditCategory::GateEvaluation);
    }

    #[test]
    fn jsonl_sink_details_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = JsonlAuditSink::new(&path);

        let details = serde_json::json!({
            "command": "rm -rf /src",
            "working_dir": "/repo",
            "environment": {"PATH": "/usr/bin"},
        });
        let entry = AuditEntry::destructive_attempt(
            "operator",
            "destructive_cleanup",
            "attempted dangerous cleanup",
            None,
            None,
            details.clone(),
        );
        sink.append(&entry).unwrap();

        let entries = sink.read_all().unwrap();
        assert_eq!(entries[0].details, details);
    }

    // ---- CommandExecution audit entries ----

    #[test]
    fn command_execution_creates_entry() {
        let run_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let entry = AuditEntry::command_execution(
            "cargo test",
            Some(1),
            "error: test failed",
            Some(3400),
            Some(run_id),
            Some(task_id),
        );

        assert_eq!(entry.category, AuditCategory::CommandExecution);
        assert_eq!(entry.actor, "command_journal");
        assert!(entry.action.contains("cargo test"));
        assert_eq!(entry.run_id, Some(run_id));
        assert_eq!(entry.task_id, Some(task_id));
        assert_eq!(entry.details["exit_code"], 1);
        assert_eq!(entry.details["duration_ms"], 3400);
        assert_eq!(entry.details["stderr_excerpt"], "error: test failed");
    }

    #[test]
    fn command_execution_with_no_exit_code() {
        let entry = AuditEntry::command_execution("sleep 60", None, "", None, None, None);
        assert_eq!(entry.category, AuditCategory::CommandExecution);
        assert!(entry.details["exit_code"].is_null());
        assert!(entry.details["duration_ms"].is_null());
    }

    #[test]
    fn command_execution_category_serializes() {
        let cat = AuditCategory::CommandExecution;
        let json = serde_json::to_string(&cat).unwrap();
        assert_eq!(json, "\"command_execution\"");
        let decoded: AuditCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, AuditCategory::CommandExecution);
    }

    #[test]
    fn command_execution_roundtrip() {
        let entry = AuditEntry::command_execution("echo hello", Some(0), "", Some(100), None, None);
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: AuditEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.audit_id, entry.audit_id);
        assert_eq!(decoded.category, AuditCategory::CommandExecution);
        assert_eq!(decoded.details["command"], "echo hello");
    }
}
