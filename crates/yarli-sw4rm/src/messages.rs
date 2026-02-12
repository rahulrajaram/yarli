//! Protocol messages between yarli and LLM agents.
//!
//! These types are serialized as JSON and sent via sw4rm envelope payloads.

use serde::{Deserialize, Serialize};

/// Content type for implementation requests sent to LLM agents.
pub const CT_IMPLEMENTATION_REQUEST: &str = "application/vnd.yarli.implementation.request+json;v=1";

/// Content type for implementation responses from LLM agents.
pub const CT_IMPLEMENTATION_RESPONSE: &str =
    "application/vnd.yarli.implementation.response+json;v=1";

/// Content type for orchestration result reports sent back to the sw4rm scheduler.
pub const CT_ORCHESTRATION_REPORT: &str = "application/vnd.yarli.orchestration.report+json;v=1";

/// Request sent to an LLM agent to implement or fix code.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImplementationRequest {
    /// The high-level objective to accomplish.
    pub objective: String,

    /// Scope hint (e.g. file paths, module names) to focus the agent.
    #[serde(default)]
    pub scope: Vec<String>,

    /// Previous verification failures to fix (empty on first iteration).
    #[serde(default)]
    pub failures: Vec<VerificationFailure>,

    /// Current iteration number (1-based).
    pub iteration: u32,

    /// Correlation ID linking request/response pairs.
    pub correlation_id: String,

    /// Optional repository context (branch, commit, working dir).
    #[serde(default)]
    pub repo_context: Option<RepoContext>,
}

/// Response from an LLM agent after implementing or fixing code.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImplementationResponse {
    /// Whether the agent believes the task is complete.
    pub complete: bool,

    /// Files that were modified by the agent.
    #[serde(default)]
    pub files_modified: Vec<String>,

    /// Human-readable summary of changes made.
    pub summary: String,

    /// Optional additional verification commands the agent suggests.
    #[serde(default)]
    pub additional_verification: Vec<String>,
}

/// A verification failure included in fix-it requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerificationFailure {
    /// Task key that failed (e.g. "cargo-test", "clippy").
    pub task_key: String,

    /// The command that was executed.
    pub command: String,

    /// Process exit code (None if killed/timeout).
    pub exit_code: Option<i32>,

    /// Tail of stdout+stderr output.
    pub output_tail: String,

    /// Classification of the failure.
    pub failure_type: FailureType,
}

/// Classification of a verification failure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureType {
    /// Compilation error.
    CompileError,
    /// Test failure.
    TestFailure,
    /// Lint/clippy warning treated as error.
    LintError,
    /// Process timed out.
    Timeout,
    /// Process was killed (e.g. OOM, signal).
    Killed,
    /// Unknown/other failure.
    Other,
}

/// Report sent back to the sw4rm scheduler after orchestration completes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrchestrationReport {
    /// Whether the objective was accomplished.
    pub success: bool,
    /// Total iterations used.
    pub iterations_used: u32,
    /// Files modified across all iterations.
    #[serde(default)]
    pub files_modified: Vec<String>,
    /// Summary from the final LLM response.
    pub final_summary: String,
    /// Remaining verification failures (empty if success).
    #[serde(default)]
    pub remaining_failures: Vec<VerificationFailure>,
    /// Error message if the orchestration failed with an error (not just verification failure).
    #[serde(default)]
    pub error: Option<String>,
}

/// Repository context provided with implementation requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoContext {
    /// Current branch name.
    #[serde(default)]
    pub branch: Option<String>,

    /// Current commit hash.
    #[serde(default)]
    pub commit: Option<String>,

    /// Working directory path.
    #[serde(default)]
    pub working_dir: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implementation_request_round_trips() {
        let req = ImplementationRequest {
            objective: "Add error handling".to_string(),
            scope: vec!["src/lib.rs".to_string()],
            failures: vec![],
            iteration: 1,
            correlation_id: "corr-123".to_string(),
            repo_context: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ImplementationRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn implementation_response_round_trips() {
        let resp = ImplementationResponse {
            complete: true,
            files_modified: vec!["src/main.rs".to_string()],
            summary: "Added error handling to main".to_string(),
            additional_verification: vec![],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ImplementationResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, parsed);
    }

    #[test]
    fn verification_failure_round_trips() {
        let failure = VerificationFailure {
            task_key: "cargo-test".to_string(),
            command: "cargo test".to_string(),
            exit_code: Some(101),
            output_tail: "test result: FAILED".to_string(),
            failure_type: FailureType::TestFailure,
        };
        let json = serde_json::to_string(&failure).unwrap();
        let parsed: VerificationFailure = serde_json::from_str(&json).unwrap();
        assert_eq!(failure, parsed);
    }

    #[test]
    fn request_with_failures_serializes() {
        let req = ImplementationRequest {
            objective: "Fix tests".to_string(),
            scope: vec![],
            failures: vec![VerificationFailure {
                task_key: "test".to_string(),
                command: "cargo test".to_string(),
                exit_code: Some(1),
                output_tail: "assertion failed".to_string(),
                failure_type: FailureType::TestFailure,
            }],
            iteration: 2,
            correlation_id: "corr-456".to_string(),
            repo_context: Some(RepoContext {
                branch: Some("feature/fix".to_string()),
                commit: Some("abc123".to_string()),
                working_dir: Some("/repo".to_string()),
            }),
        };
        let json = serde_json::to_string_pretty(&req).unwrap();
        assert!(json.contains("Fix tests"));
        assert!(json.contains("assertion failed"));
        assert!(json.contains("feature/fix"));
    }

    #[test]
    fn failure_type_variants_serialize_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&FailureType::CompileError).unwrap(),
            "\"compile_error\""
        );
        assert_eq!(
            serde_json::to_string(&FailureType::TestFailure).unwrap(),
            "\"test_failure\""
        );
        assert_eq!(
            serde_json::to_string(&FailureType::LintError).unwrap(),
            "\"lint_error\""
        );
        assert_eq!(
            serde_json::to_string(&FailureType::Timeout).unwrap(),
            "\"timeout\""
        );
        assert_eq!(
            serde_json::to_string(&FailureType::Killed).unwrap(),
            "\"killed\""
        );
        assert_eq!(
            serde_json::to_string(&FailureType::Other).unwrap(),
            "\"other\""
        );
    }

    #[test]
    fn empty_response_defaults() {
        let json = r#"{"complete":false,"summary":"wip"}"#;
        let resp: ImplementationResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.complete);
        assert!(resp.files_modified.is_empty());
        assert!(resp.additional_verification.is_empty());
    }

    #[test]
    fn content_type_constants_are_correct() {
        assert!(CT_IMPLEMENTATION_REQUEST.starts_with("application/vnd.yarli."));
        assert!(CT_IMPLEMENTATION_RESPONSE.starts_with("application/vnd.yarli."));
        assert!(CT_ORCHESTRATION_REPORT.starts_with("application/vnd.yarli."));
    }

    #[test]
    fn orchestration_report_round_trips() {
        let report = OrchestrationReport {
            success: false,
            iterations_used: 3,
            files_modified: vec!["a.rs".to_string()],
            final_summary: "gave up".to_string(),
            remaining_failures: vec![VerificationFailure {
                task_key: "test".to_string(),
                command: "cargo test".to_string(),
                exit_code: Some(1),
                output_tail: "failed".to_string(),
                failure_type: FailureType::TestFailure,
            }],
            error: None,
        };
        let json = serde_json::to_string(&report).unwrap();
        let parsed: OrchestrationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(report, parsed);
    }

    #[test]
    fn orchestration_report_with_error() {
        let report = OrchestrationReport {
            success: false,
            iterations_used: 0,
            files_modified: vec![],
            final_summary: String::new(),
            remaining_failures: vec![],
            error: Some("LLM agent response timed out".to_string()),
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("timed out"));
    }
}
