//! Gate engine: evaluates verification gates for tasks and runs.
//!
//! All required gates must pass for `TASK_COMPLETE` and `RUN_COMPLETED`
//! (Section 11.2). Gates are deterministic functions over persisted
//! evidence and state. Failures emit structured blocker records and
//! remediation hints.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::yarli_core::domain::{Evidence, RunId, TaskId};
use crate::yarli_core::explain::{GateResult, GateType};
use crate::yarli_core::fsm::task::TaskState;

use crate::yarli_gates::evidence::validate_evidence_schema;

// ---------------------------------------------------------------------------
// Gate evaluation context
// ---------------------------------------------------------------------------

/// Input context for gate evaluation.
///
/// A snapshot of relevant state that gate evaluators inspect.
/// Built by the caller from persisted state before invoking gates.
#[derive(Debug, Clone)]
pub struct GateContext {
    /// The run being evaluated.
    pub run_id: RunId,
    /// The specific task being evaluated (None for run-level gates).
    pub task_id: Option<TaskId>,
    /// Task key (human-readable name).
    pub task_key: Option<String>,
    /// Evidence records for this task/run.
    pub evidence: Vec<Evidence>,
    /// States of all tasks in the run (task_id, task_key, state).
    pub task_states: Vec<(TaskId, String, TaskState)>,
    /// Whether all required tasks are in terminal success state.
    pub all_tasks_complete: bool,
    /// Whether any task has unresolved merge conflicts.
    pub has_unresolved_conflicts: bool,
    /// Whether any unapproved git operations exist.
    pub has_unapproved_git_ops: bool,
    /// Whether the worktree is in a consistent state.
    pub worktree_consistent: bool,
    /// Policy violations found during evaluation.
    pub policy_violations: Vec<String>,
    /// Command class of the task (for evidence schema validation).
    pub command_class: Option<crate::yarli_core::domain::CommandClass>,
}

impl GateContext {
    /// Create a minimal context for a task-level evaluation.
    pub fn for_task(run_id: RunId, task_id: TaskId, task_key: impl Into<String>) -> Self {
        Self {
            run_id,
            task_id: Some(task_id),
            task_key: Some(task_key.into()),
            evidence: Vec::new(),
            task_states: Vec::new(),
            all_tasks_complete: false,
            has_unresolved_conflicts: false,
            has_unapproved_git_ops: false,
            worktree_consistent: true,
            policy_violations: Vec::new(),
            command_class: None,
        }
    }

    /// Create a minimal context for a run-level evaluation.
    pub fn for_run(run_id: RunId) -> Self {
        Self {
            run_id,
            task_id: None,
            task_key: None,
            evidence: Vec::new(),
            task_states: Vec::new(),
            all_tasks_complete: false,
            has_unresolved_conflicts: false,
            has_unapproved_git_ops: false,
            worktree_consistent: true,
            policy_violations: Vec::new(),
            command_class: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Gate evaluation result
// ---------------------------------------------------------------------------

/// Detailed result from a single gate evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateEvaluation {
    /// Which gate was evaluated.
    pub gate_type: GateType,
    /// The outcome.
    pub result: GateResult,
    /// When this evaluation was performed.
    pub evaluated_at: DateTime<Utc>,
    /// Evidence IDs that were inspected.
    pub inspected_evidence: Vec<Uuid>,
    /// Remediation hint if the gate failed.
    pub remediation: Option<String>,
}

// ---------------------------------------------------------------------------
// Individual gate evaluators
// ---------------------------------------------------------------------------

/// Evaluate a single gate type against the provided context.
pub fn evaluate_gate(gate_type: GateType, ctx: &GateContext) -> GateEvaluation {
    let (result, remediation) = match gate_type {
        GateType::RequiredTasksClosed => eval_required_tasks_closed(ctx),
        GateType::RequiredEvidencePresent => eval_required_evidence_present(ctx),
        GateType::TestsPassed => eval_tests_passed(ctx),
        GateType::NoUnapprovedGitOps => eval_no_unapproved_git_ops(ctx),
        GateType::NoUnresolvedConflicts => eval_no_unresolved_conflicts(ctx),
        GateType::WorktreeConsistent => eval_worktree_consistent(ctx),
        GateType::PolicyClean => eval_policy_clean(ctx),
    };

    let inspected = ctx.evidence.iter().map(|e| e.evidence_id).collect();

    GateEvaluation {
        gate_type,
        result,
        evaluated_at: Utc::now(),
        inspected_evidence: inspected,
        remediation,
    }
}

/// Evaluate all required gates and return results.
///
/// The `required_gates` list determines which gates are checked.
/// All must pass for the entity to be considered verified.
pub fn evaluate_all(required_gates: &[GateType], ctx: &GateContext) -> Vec<GateEvaluation> {
    required_gates
        .iter()
        .map(|gate| evaluate_gate(*gate, ctx))
        .collect()
}

/// Check if all gate evaluations passed.
pub fn all_passed(evaluations: &[GateEvaluation]) -> bool {
    evaluations
        .iter()
        .all(|e| matches!(e.result, GateResult::Passed { .. }))
}

/// Collect failures from gate evaluations.
pub fn collect_failures(evaluations: &[GateEvaluation]) -> Vec<&GateEvaluation> {
    evaluations
        .iter()
        .filter(|e| matches!(e.result, GateResult::Failed { .. }))
        .collect()
}

/// Default set of gates for task-level verification.
pub fn default_task_gates() -> Vec<GateType> {
    vec![
        GateType::RequiredEvidencePresent,
        GateType::TestsPassed,
        GateType::NoUnresolvedConflicts,
        GateType::WorktreeConsistent,
        GateType::PolicyClean,
    ]
}

/// Default set of gates for run-level verification.
pub fn default_run_gates() -> Vec<GateType> {
    vec![
        GateType::RequiredTasksClosed,
        GateType::RequiredEvidencePresent,
        GateType::NoUnapprovedGitOps,
        GateType::NoUnresolvedConflicts,
        GateType::WorktreeConsistent,
        GateType::PolicyClean,
    ]
}

// ---------------------------------------------------------------------------
// Gate implementations
// ---------------------------------------------------------------------------

/// gate.required_tasks_closed: All required tasks must be in TaskComplete state.
fn eval_required_tasks_closed(ctx: &GateContext) -> (GateResult, Option<String>) {
    if ctx.all_tasks_complete {
        return (
            GateResult::Passed {
                evidence_ids: Vec::new(),
            },
            None,
        );
    }

    let incomplete: Vec<String> = ctx
        .task_states
        .iter()
        .filter(|(_, _, state)| *state != TaskState::TaskComplete)
        .map(|(_, key, state)| format!("{key} ({state:?})"))
        .collect();

    if incomplete.is_empty() {
        (
            GateResult::Passed {
                evidence_ids: Vec::new(),
            },
            None,
        )
    } else {
        (
            GateResult::Failed {
                reason: format!(
                    "{} task(s) not complete: {}",
                    incomplete.len(),
                    incomplete.join(", ")
                ),
            },
            Some("Complete or cancel all remaining tasks before run can finish.".to_string()),
        )
    }
}

/// gate.required_evidence_present: Task must have at least one valid evidence record.
fn eval_required_evidence_present(ctx: &GateContext) -> (GateResult, Option<String>) {
    if ctx.evidence.is_empty() {
        return (
            GateResult::Failed {
                reason: "no evidence records found".to_string(),
            },
            Some(
                "Ensure the task produces evidence (command result, test report, etc.)".to_string(),
            ),
        );
    }

    // Validate evidence schemas if command class is known
    if let Some(cmd_class) = ctx.command_class {
        let mut valid_ids = Vec::new();
        let mut invalid = Vec::new();

        for ev in &ctx.evidence {
            match validate_evidence_schema(cmd_class, &ev.payload) {
                Ok(_) => valid_ids.push(ev.evidence_id),
                Err(msg) => invalid.push(format!("{}: {msg}", ev.evidence_id)),
            }
        }

        if valid_ids.is_empty() {
            return (
                GateResult::Failed {
                    reason: format!(
                        "no valid evidence for command class {:?}: {}",
                        cmd_class,
                        invalid.join("; ")
                    ),
                },
                Some(
                    "Check evidence payload matches expected schema for this task class."
                        .to_string(),
                ),
            );
        }

        return (
            GateResult::Passed {
                evidence_ids: valid_ids,
            },
            None,
        );
    }

    // No command class specified, just check evidence exists
    let ids = ctx.evidence.iter().map(|e| e.evidence_id).collect();
    (GateResult::Passed { evidence_ids: ids }, None)
}

/// gate.tests_passed: Evidence must show all tests passing (exit_code == 0 or 0 failures).
fn eval_tests_passed(ctx: &GateContext) -> (GateResult, Option<String>) {
    // Look for test report evidence first
    for ev in &ctx.evidence {
        if let Ok(report) = serde_json::from_value::<crate::yarli_gates::evidence::TestReportEvidence>(
            ev.payload.clone(),
        ) {
            if report.failed > 0 {
                return (
                    GateResult::Failed {
                        reason: format!("{}/{} tests failed", report.failed, report.total),
                    },
                    Some(format!(
                        "Fix {} failing test(s): {}",
                        report.failed,
                        report
                            .failures
                            .iter()
                            .map(|f| f.test_name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )),
                );
            }

            return (
                GateResult::Passed {
                    evidence_ids: vec![ev.evidence_id],
                },
                None,
            );
        }
    }

    // Fall back to command evidence: check exit_code == 0
    for ev in &ctx.evidence {
        if let Some(exit_code) = ev.payload.get("exit_code").and_then(|v| v.as_i64()) {
            if exit_code != 0 {
                return (
                    GateResult::Failed {
                        reason: format!("command exited with code {exit_code}"),
                    },
                    Some("Fix the command failure and re-run.".to_string()),
                );
            }

            return (
                GateResult::Passed {
                    evidence_ids: vec![ev.evidence_id],
                },
                None,
            );
        }
    }

    // No test-relevant evidence at all: pending
    (GateResult::Pending, None)
}

/// gate.no_unapproved_git_ops: No unapproved git operations detected.
fn eval_no_unapproved_git_ops(ctx: &GateContext) -> (GateResult, Option<String>) {
    if ctx.has_unapproved_git_ops {
        (
            GateResult::Failed {
                reason: "unapproved git operations detected".to_string(),
            },
            Some("Request policy approval for pending git operations.".to_string()),
        )
    } else {
        (
            GateResult::Passed {
                evidence_ids: Vec::new(),
            },
            None,
        )
    }
}

/// gate.no_unresolved_conflicts: No merge conflicts remaining.
fn eval_no_unresolved_conflicts(ctx: &GateContext) -> (GateResult, Option<String>) {
    if ctx.has_unresolved_conflicts {
        (
            GateResult::Failed {
                reason: "unresolved merge conflicts exist".to_string(),
            },
            Some("Resolve all merge conflicts before proceeding.".to_string()),
        )
    } else {
        (
            GateResult::Passed {
                evidence_ids: Vec::new(),
            },
            None,
        )
    }
}

/// gate.worktree_consistent: Worktree is in a valid, consistent state.
fn eval_worktree_consistent(ctx: &GateContext) -> (GateResult, Option<String>) {
    if ctx.worktree_consistent {
        (
            GateResult::Passed {
                evidence_ids: Vec::new(),
            },
            None,
        )
    } else {
        (
            GateResult::Failed {
                reason: "worktree is in an inconsistent state".to_string(),
            },
            Some("Check worktree status and recover if needed: yarli worktree recover".to_string()),
        )
    }
}

/// gate.policy_clean: No policy violations.
fn eval_policy_clean(ctx: &GateContext) -> (GateResult, Option<String>) {
    if ctx.policy_violations.is_empty() {
        (
            GateResult::Passed {
                evidence_ids: Vec::new(),
            },
            None,
        )
    } else {
        (
            GateResult::Failed {
                reason: format!(
                    "{} policy violation(s): {}",
                    ctx.policy_violations.len(),
                    ctx.policy_violations.join("; ")
                ),
            },
            Some("Address policy violations or request breakglass approval.".to_string()),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yarli_core::domain::{CommandClass, Evidence};

    fn make_evidence(payload: serde_json::Value) -> Evidence {
        Evidence {
            evidence_id: Uuid::new_v4(),
            task_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            evidence_type: "test".to_string(),
            payload,
            created_at: Utc::now(),
        }
    }

    fn make_context() -> GateContext {
        GateContext::for_run(Uuid::new_v4())
    }

    // ---- gate.required_tasks_closed ----

    #[test]
    fn required_tasks_closed_all_complete() {
        let mut ctx = make_context();
        ctx.all_tasks_complete = true;
        ctx.task_states = vec![
            (Uuid::new_v4(), "build".into(), TaskState::TaskComplete),
            (Uuid::new_v4(), "test".into(), TaskState::TaskComplete),
        ];

        let eval = evaluate_gate(GateType::RequiredTasksClosed, &ctx);
        assert!(matches!(eval.result, GateResult::Passed { .. }));
        assert!(eval.remediation.is_none());
    }

    #[test]
    fn required_tasks_closed_some_incomplete() {
        let mut ctx = make_context();
        ctx.all_tasks_complete = false;
        ctx.task_states = vec![
            (Uuid::new_v4(), "build".into(), TaskState::TaskComplete),
            (Uuid::new_v4(), "test".into(), TaskState::TaskFailed),
        ];

        let eval = evaluate_gate(GateType::RequiredTasksClosed, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
        if let GateResult::Failed { reason } = &eval.result {
            assert!(reason.contains("test"));
            assert!(reason.contains("TaskFailed"));
        }
        assert!(eval.remediation.is_some());
    }

    #[test]
    fn required_tasks_closed_empty_tasks() {
        let ctx = make_context();
        // all_tasks_complete defaults to false, but task_states is empty
        let eval = evaluate_gate(GateType::RequiredTasksClosed, &ctx);
        // No incomplete tasks found → pass (empty set)
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    // ---- gate.required_evidence_present ----

    #[test]
    fn evidence_present_with_valid_evidence() {
        let mut ctx = GateContext::for_task(Uuid::new_v4(), Uuid::new_v4(), "build");
        ctx.command_class = Some(CommandClass::Cpu);
        ctx.evidence = vec![make_evidence(serde_json::json!({
            "command": "cargo build",
            "exit_code": 0,
            "duration_ms": 5000,
            "timed_out": false,
            "killed": false,
        }))];

        let eval = evaluate_gate(GateType::RequiredEvidencePresent, &ctx);
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    #[test]
    fn evidence_present_no_evidence() {
        let ctx = GateContext::for_task(Uuid::new_v4(), Uuid::new_v4(), "build");
        let eval = evaluate_gate(GateType::RequiredEvidencePresent, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
        if let GateResult::Failed { reason } = &eval.result {
            assert!(reason.contains("no evidence"));
        }
    }

    #[test]
    fn evidence_present_invalid_schema() {
        let mut ctx = GateContext::for_task(Uuid::new_v4(), Uuid::new_v4(), "build");
        ctx.command_class = Some(CommandClass::Io);
        ctx.evidence = vec![make_evidence(serde_json::json!({"foo": "bar"}))];

        let eval = evaluate_gate(GateType::RequiredEvidencePresent, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
    }

    #[test]
    fn evidence_present_without_command_class() {
        let mut ctx = GateContext::for_task(Uuid::new_v4(), Uuid::new_v4(), "build");
        ctx.evidence = vec![make_evidence(serde_json::json!({"anything": true}))];

        let eval = evaluate_gate(GateType::RequiredEvidencePresent, &ctx);
        // Without command class, just checks evidence exists
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    // ---- gate.tests_passed ----

    #[test]
    fn tests_passed_with_report() {
        let mut ctx = make_context();
        ctx.evidence = vec![make_evidence(serde_json::json!({
            "total": 47,
            "passed": 47,
            "failed": 0,
            "skipped": 0,
            "duration_ms": 12000,
            "failures": [],
        }))];

        let eval = evaluate_gate(GateType::TestsPassed, &ctx);
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    #[test]
    fn tests_passed_with_failures() {
        let mut ctx = make_context();
        ctx.evidence = vec![make_evidence(serde_json::json!({
            "total": 47,
            "passed": 44,
            "failed": 3,
            "skipped": 0,
            "duration_ms": 12000,
            "failures": [
                {"test_name": "test_auth", "message": "timeout"},
                {"test_name": "test_db", "message": "connection refused"},
                {"test_name": "test_parse", "message": "assertion failed"},
            ],
        }))];

        let eval = evaluate_gate(GateType::TestsPassed, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
        if let GateResult::Failed { reason } = &eval.result {
            assert!(reason.contains("3/47"));
        }
        assert!(eval.remediation.is_some());
        assert!(eval.remediation.as_ref().unwrap().contains("test_auth"));
    }

    #[test]
    fn tests_passed_via_exit_code() {
        let mut ctx = make_context();
        ctx.evidence = vec![make_evidence(serde_json::json!({
            "exit_code": 0,
        }))];

        let eval = evaluate_gate(GateType::TestsPassed, &ctx);
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    #[test]
    fn tests_passed_nonzero_exit() {
        let mut ctx = make_context();
        ctx.evidence = vec![make_evidence(serde_json::json!({
            "exit_code": 1,
        }))];

        let eval = evaluate_gate(GateType::TestsPassed, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
    }

    #[test]
    fn tests_passed_no_evidence() {
        let ctx = make_context();
        let eval = evaluate_gate(GateType::TestsPassed, &ctx);
        assert!(matches!(eval.result, GateResult::Pending));
    }

    // ---- gate.no_unapproved_git_ops ----

    #[test]
    fn no_unapproved_git_ops_clean() {
        let ctx = make_context();
        let eval = evaluate_gate(GateType::NoUnapprovedGitOps, &ctx);
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    #[test]
    fn no_unapproved_git_ops_dirty() {
        let mut ctx = make_context();
        ctx.has_unapproved_git_ops = true;
        let eval = evaluate_gate(GateType::NoUnapprovedGitOps, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
    }

    // ---- gate.no_unresolved_conflicts ----

    #[test]
    fn no_conflicts_clean() {
        let ctx = make_context();
        let eval = evaluate_gate(GateType::NoUnresolvedConflicts, &ctx);
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    #[test]
    fn no_conflicts_with_conflicts() {
        let mut ctx = make_context();
        ctx.has_unresolved_conflicts = true;
        let eval = evaluate_gate(GateType::NoUnresolvedConflicts, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
    }

    // ---- gate.worktree_consistent ----

    #[test]
    fn worktree_consistent_ok() {
        let ctx = make_context();
        let eval = evaluate_gate(GateType::WorktreeConsistent, &ctx);
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    #[test]
    fn worktree_inconsistent() {
        let mut ctx = make_context();
        ctx.worktree_consistent = false;
        let eval = evaluate_gate(GateType::WorktreeConsistent, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
    }

    // ---- gate.policy_clean ----

    #[test]
    fn policy_clean_ok() {
        let ctx = make_context();
        let eval = evaluate_gate(GateType::PolicyClean, &ctx);
        assert!(matches!(eval.result, GateResult::Passed { .. }));
    }

    #[test]
    fn policy_violations_present() {
        let mut ctx = make_context();
        ctx.policy_violations = vec![
            "forbidden push attempted".to_string(),
            "unapproved branch delete".to_string(),
        ];
        let eval = evaluate_gate(GateType::PolicyClean, &ctx);
        assert!(matches!(eval.result, GateResult::Failed { .. }));
        if let GateResult::Failed { reason } = &eval.result {
            assert!(reason.contains("2 policy violation(s)"));
            assert!(reason.contains("forbidden push"));
        }
    }

    // ---- evaluate_all / all_passed / collect_failures ----

    #[test]
    fn evaluate_all_gates_pass() {
        let mut ctx = make_context();
        ctx.all_tasks_complete = true;
        ctx.evidence = vec![make_evidence(serde_json::json!({
            "total": 10,
            "passed": 10,
            "failed": 0,
            "skipped": 0,
            "duration_ms": 1000,
            "failures": [],
        }))];

        let gates = vec![
            GateType::RequiredTasksClosed,
            GateType::NoUnresolvedConflicts,
            GateType::WorktreeConsistent,
            GateType::PolicyClean,
        ];
        let evals = evaluate_all(&gates, &ctx);
        assert_eq!(evals.len(), 4);
        assert!(all_passed(&evals));
        assert!(collect_failures(&evals).is_empty());
    }

    #[test]
    fn evaluate_all_some_fail() {
        let mut ctx = make_context();
        ctx.all_tasks_complete = false;
        ctx.task_states = vec![(Uuid::new_v4(), "test".into(), TaskState::TaskFailed)];
        ctx.has_unresolved_conflicts = true;

        let gates = vec![
            GateType::RequiredTasksClosed,
            GateType::NoUnresolvedConflicts,
            GateType::WorktreeConsistent,
        ];
        let evals = evaluate_all(&gates, &ctx);
        assert_eq!(evals.len(), 3);
        assert!(!all_passed(&evals));

        let failures = collect_failures(&evals);
        assert_eq!(failures.len(), 2); // tasks + conflicts
    }

    #[test]
    fn default_task_gates_count() {
        let gates = default_task_gates();
        assert_eq!(gates.len(), 5);
        assert!(gates.contains(&GateType::RequiredEvidencePresent));
        assert!(gates.contains(&GateType::TestsPassed));
    }

    #[test]
    fn default_run_gates_count() {
        let gates = default_run_gates();
        assert_eq!(gates.len(), 6);
        assert!(gates.contains(&GateType::RequiredTasksClosed));
        assert!(gates.contains(&GateType::NoUnapprovedGitOps));
    }

    #[test]
    fn gate_evaluation_has_timestamp() {
        let ctx = make_context();
        let eval = evaluate_gate(GateType::PolicyClean, &ctx);
        // Just verify it's set to a reasonable time
        let now = Utc::now();
        assert!(eval.evaluated_at <= now);
    }

    #[test]
    fn gate_evaluation_serializes() {
        let ctx = make_context();
        let eval = evaluate_gate(GateType::PolicyClean, &ctx);
        let json = serde_json::to_string(&eval).unwrap();
        assert!(json.contains("policy_clean"));
        let decoded: GateEvaluation = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.gate_type, GateType::PolicyClean);
    }

    #[test]
    fn gate_context_for_task_builder() {
        let run_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let ctx = GateContext::for_task(run_id, task_id, "build");
        assert_eq!(ctx.run_id, run_id);
        assert_eq!(ctx.task_id, Some(task_id));
        assert_eq!(ctx.task_key.as_deref(), Some("build"));
        assert!(ctx.worktree_consistent); // default true
        assert!(ctx.policy_violations.is_empty());
    }

    #[test]
    fn gate_context_for_run_builder() {
        let run_id = Uuid::new_v4();
        let ctx = GateContext::for_run(run_id);
        assert_eq!(ctx.run_id, run_id);
        assert!(ctx.task_id.is_none());
        assert!(ctx.task_key.is_none());
    }

    #[test]
    fn tests_passed_prefers_test_report_over_exit_code() {
        let mut ctx = make_context();
        // Both test report and exit code present - test report takes precedence
        ctx.evidence = vec![
            make_evidence(serde_json::json!({
                "total": 10,
                "passed": 9,
                "failed": 1,
                "skipped": 0,
                "duration_ms": 1000,
                "failures": [{"test_name": "test_x", "message": "bad"}],
            })),
            make_evidence(serde_json::json!({"exit_code": 0})),
        ];

        let eval = evaluate_gate(GateType::TestsPassed, &ctx);
        // Test report shows 1 failure, so gate should fail
        assert!(matches!(eval.result, GateResult::Failed { .. }));
    }

    #[test]
    fn evidence_inspected_ids_collected() {
        let mut ctx = make_context();
        let ev1 = make_evidence(serde_json::json!({"exit_code": 0}));
        let ev2 = make_evidence(serde_json::json!({"foo": "bar"}));
        let id1 = ev1.evidence_id;
        let id2 = ev2.evidence_id;
        ctx.evidence = vec![ev1, ev2];

        let eval = evaluate_gate(GateType::PolicyClean, &ctx);
        assert!(eval.inspected_evidence.contains(&id1));
        assert!(eval.inspected_evidence.contains(&id2));
    }
}
