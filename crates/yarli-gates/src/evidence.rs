//! Evidence schemas and validation.
//!
//! Every task class declares minimum evidence schema (Section 11.3).
//! Evidence proves that a task outcome is real and auditable.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use yarli_core::domain::{CommandClass, RunId, TaskId};

/// Evidence type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    /// Command execution result (exit code, output, duration).
    CommandResult,
    /// Test report artifact with summary metrics.
    TestReport,
    /// Git mutation evidence (before/after refs, changed files).
    GitMutation,
    /// Memory operation evidence (query/store status).
    MemoryOperation,
    /// Gate evaluation result evidence.
    GateEvaluation,
}

/// Validated evidence record.
///
/// Created from raw `Evidence` domain objects after schema validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatedEvidence {
    pub evidence_id: Uuid,
    pub task_id: TaskId,
    pub run_id: RunId,
    pub kind: EvidenceKind,
    pub validated_at: DateTime<Utc>,
    pub payload: serde_json::Value,
}

/// Evidence for command execution (Section 11.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandEvidence {
    /// Command that was executed.
    pub command: String,
    /// Exit code from the process.
    pub exit_code: i32,
    /// Total execution duration in milliseconds.
    pub duration_ms: u64,
    /// Selected output excerpts (truncated if large).
    pub output_excerpt: Option<String>,
    /// Whether the command timed out.
    pub timed_out: bool,
    /// Whether the command was killed.
    pub killed: bool,
}

/// Evidence for test execution (Section 11.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestReportEvidence {
    /// Total number of tests.
    pub total: u32,
    /// Number of passing tests.
    pub passed: u32,
    /// Number of failing tests.
    pub failed: u32,
    /// Number of skipped tests.
    pub skipped: u32,
    /// Total duration in milliseconds.
    pub duration_ms: u64,
    /// Failure details (test name + message).
    pub failures: Vec<TestFailure>,
}

/// A single test failure record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestFailure {
    pub test_name: String,
    pub message: String,
}

/// Evidence for git mutations (Section 11.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitMutationEvidence {
    /// Ref before the operation.
    pub before_ref: String,
    /// Ref after the operation.
    pub after_ref: String,
    /// Files changed by the operation.
    pub changed_files: Vec<String>,
    /// Conflict report (if any).
    pub conflicts: Vec<String>,
    /// Whether this was a merge operation.
    pub is_merge: bool,
}

/// Evidence for memory operations (Section 11.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryOperationEvidence {
    /// Type of memory operation (query/store/link).
    pub operation: String,
    /// Whether the operation succeeded.
    pub success: bool,
    /// Reference to stored/queried record.
    pub record_ref: Option<String>,
}

/// Validate raw evidence payload against expected schema for a command class.
///
/// Returns `Ok(kind)` with the detected evidence kind, or `Err` with details.
pub fn validate_evidence_schema(
    command_class: CommandClass,
    payload: &serde_json::Value,
) -> Result<EvidenceKind, String> {
    match command_class {
        CommandClass::Io | CommandClass::Cpu | CommandClass::Tool => {
            validate_command_evidence(payload)
        }
        CommandClass::Git => validate_git_evidence(payload),
    }
}

fn validate_command_evidence(payload: &serde_json::Value) -> Result<EvidenceKind, String> {
    // Check if it's a test report first
    if payload.get("total").is_some() && payload.get("passed").is_some() {
        let _: TestReportEvidence = serde_json::from_value(payload.clone())
            .map_err(|e| format!("invalid test report evidence: {e}"))?;
        return Ok(EvidenceKind::TestReport);
    }

    // Otherwise, check for command result
    if payload.get("exit_code").is_some() {
        let _: CommandEvidence = serde_json::from_value(payload.clone())
            .map_err(|e| format!("invalid command evidence: {e}"))?;
        return Ok(EvidenceKind::CommandResult);
    }

    Err("evidence payload must contain either 'exit_code' (command) or 'total'+'passed' (test report)".to_string())
}

fn validate_git_evidence(payload: &serde_json::Value) -> Result<EvidenceKind, String> {
    if payload.get("before_ref").is_some() && payload.get("after_ref").is_some() {
        let _: GitMutationEvidence = serde_json::from_value(payload.clone())
            .map_err(|e| format!("invalid git mutation evidence: {e}"))?;
        return Ok(EvidenceKind::GitMutation);
    }

    // Fall back to command evidence (git commands still produce exit codes)
    validate_command_evidence(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_command_evidence_schema() {
        let payload = serde_json::json!({
            "command": "cargo build",
            "exit_code": 0,
            "duration_ms": 5000,
            "output_excerpt": "Finished release",
            "timed_out": false,
            "killed": false,
        });
        let kind = validate_evidence_schema(CommandClass::Cpu, &payload).unwrap();
        assert_eq!(kind, EvidenceKind::CommandResult);
    }

    #[test]
    fn validate_test_report_evidence_schema() {
        let payload = serde_json::json!({
            "total": 47,
            "passed": 44,
            "failed": 3,
            "skipped": 0,
            "duration_ms": 12000,
            "failures": [
                {"test_name": "test_foo", "message": "assertion failed"},
            ],
        });
        let kind = validate_evidence_schema(CommandClass::Cpu, &payload).unwrap();
        assert_eq!(kind, EvidenceKind::TestReport);
    }

    #[test]
    fn validate_git_mutation_evidence_schema() {
        let payload = serde_json::json!({
            "before_ref": "abc123",
            "after_ref": "def456",
            "changed_files": ["src/main.rs"],
            "conflicts": [],
            "is_merge": true,
        });
        let kind = validate_evidence_schema(CommandClass::Git, &payload).unwrap();
        assert_eq!(kind, EvidenceKind::GitMutation);
    }

    #[test]
    fn validate_git_command_evidence_fallback() {
        let payload = serde_json::json!({
            "command": "git status",
            "exit_code": 0,
            "duration_ms": 100,
            "timed_out": false,
            "killed": false,
        });
        let kind = validate_evidence_schema(CommandClass::Git, &payload).unwrap();
        assert_eq!(kind, EvidenceKind::CommandResult);
    }

    #[test]
    fn reject_empty_payload() {
        let payload = serde_json::json!({});
        let result = validate_evidence_schema(CommandClass::Io, &payload);
        assert!(result.is_err());
    }

    #[test]
    fn reject_invalid_command_evidence() {
        let payload = serde_json::json!({
            "exit_code": "not_a_number",
        });
        let result = validate_evidence_schema(CommandClass::Io, &payload);
        assert!(result.is_err());
    }

    #[test]
    fn reject_invalid_test_report() {
        let payload = serde_json::json!({
            "total": "not_a_number",
            "passed": 0,
        });
        let result = validate_evidence_schema(CommandClass::Cpu, &payload);
        assert!(result.is_err());
    }

    #[test]
    fn reject_invalid_git_evidence() {
        let payload = serde_json::json!({
            "before_ref": "abc",
            "after_ref": 123,
            "changed_files": "not_an_array",
        });
        let result = validate_evidence_schema(CommandClass::Git, &payload);
        assert!(result.is_err());
    }

    #[test]
    fn command_evidence_roundtrip() {
        let evidence = CommandEvidence {
            command: "cargo test".to_string(),
            exit_code: 0,
            duration_ms: 3000,
            output_excerpt: Some("test result: ok".to_string()),
            timed_out: false,
            killed: false,
        };
        let json = serde_json::to_value(&evidence).unwrap();
        let decoded: CommandEvidence = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.exit_code, 0);
        assert_eq!(decoded.command, "cargo test");
    }

    #[test]
    fn test_report_roundtrip() {
        let report = TestReportEvidence {
            total: 100,
            passed: 97,
            failed: 2,
            skipped: 1,
            duration_ms: 45000,
            failures: vec![TestFailure {
                test_name: "test_auth".to_string(),
                message: "timeout".to_string(),
            }],
        };
        let json = serde_json::to_value(&report).unwrap();
        let decoded: TestReportEvidence = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.total, 100);
        assert_eq!(decoded.failures.len(), 1);
    }

    #[test]
    fn git_mutation_roundtrip() {
        let evidence = GitMutationEvidence {
            before_ref: "abc123".to_string(),
            after_ref: "def456".to_string(),
            changed_files: vec!["src/main.rs".to_string()],
            conflicts: vec![],
            is_merge: false,
        };
        let json = serde_json::to_value(&evidence).unwrap();
        let decoded: GitMutationEvidence = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.before_ref, "abc123");
        assert!(!decoded.is_merge);
    }

    #[test]
    fn evidence_kind_serialize() {
        let kind = EvidenceKind::CommandResult;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, "\"command_result\"");

        let decoded: EvidenceKind = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, EvidenceKind::CommandResult);
    }

    #[test]
    fn validated_evidence_serialize() {
        let ve = ValidatedEvidence {
            evidence_id: Uuid::nil(),
            task_id: Uuid::nil(),
            run_id: Uuid::nil(),
            kind: EvidenceKind::TestReport,
            validated_at: chrono::Utc::now(),
            payload: serde_json::json!({"total": 10}),
        };
        let json = serde_json::to_string(&ve).unwrap();
        assert!(json.contains("test_report"));
    }
}
