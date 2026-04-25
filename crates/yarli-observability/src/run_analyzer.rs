//! Post-run failure pattern analysis.
//!
//! A pure function that analyzes a completed run and produces structured insights
//! about failure patterns, retry recommendations, and lessons for semantic memory.

use serde::{Deserialize, Serialize};

use crate::yarli_core::domain::ExitReason;
use crate::yarli_core::entities::task::BlockerCode;
use crate::yarli_core::fsm::run::RunState;
use crate::yarli_core::fsm::task::TaskState;

/// Outcome record for a single task (mirrors continuation::TaskOutcome but decoupled).
#[derive(Debug, Clone)]
pub struct TaskOutcome {
    pub task_key: String,
    pub state: TaskState,
    pub last_error: Option<String>,
    pub blocker: Option<BlockerCode>,
}

/// Structured analysis of a completed run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAnalysis {
    /// Detected failure patterns.
    pub patterns: Vec<FailurePattern>,
    /// Structured lesson for semantic memory (None for clean completions).
    pub run_lesson: Option<String>,
    /// Retry recommendation.
    pub retry_recommendation: RetryScope,
    /// Confidence in the recommendation (0.0–1.0).
    pub confidence: f64,
}

/// Detected failure pattern in a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FailurePattern {
    /// A root task failed and multiple dependents were blocked.
    CascadingFailure {
        root_task_key: String,
        affected: Vec<String>,
    },
    /// Same blocker code appeared across 2+ tasks.
    RepeatedBlocker {
        blocker: String,
        task_keys: Vec<String>,
    },
    /// All tasks completed but the run failed due to gate failures.
    GateOnlyFailure { gate_names: Vec<String> },
    /// All tasks succeeded but run still failed (non-gate reason).
    AllTasksSucceededRunFailed { exit_reason: String },
    /// Exactly one task failed.
    SingleTaskFailure { task_key: String, error: String },
    /// Run timed out or stalled.
    TimeoutOrStall { exit_reason: String },
}

/// Retry recommendation scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum RetryScope {
    /// Retry the entire run.
    Full,
    /// Retry only these task keys.
    Subset { keys: Vec<String> },
    /// Do not retry — needs manual intervention.
    None,
}

/// Analyze a completed run and produce structured insights.
///
/// This is a pure function with no side effects.
pub fn analyze_run(
    exit_state: RunState,
    exit_reason: Option<ExitReason>,
    tasks: &[TaskOutcome],
    gate_failures: &[String],
) -> RunAnalysis {
    // Clean completion — no patterns, no lesson
    if exit_state.is_work_done() && gate_failures.is_empty() {
        let all_ok = tasks
            .iter()
            .all(|t| t.state == TaskState::TaskComplete || t.state == TaskState::TaskCancelled);
        if all_ok {
            return RunAnalysis {
                patterns: vec![],
                run_lesson: None,
                retry_recommendation: RetryScope::None,
                confidence: 1.0,
            };
        }
    }

    let mut patterns = Vec::new();

    // Detect TimeoutOrStall
    if matches!(
        exit_reason,
        Some(ExitReason::TimedOut) | Some(ExitReason::StalledNoProgress)
    ) {
        patterns.push(FailurePattern::TimeoutOrStall {
            exit_reason: exit_reason.unwrap().to_string(),
        });
    }

    // Detect CascadingFailure: tasks blocked with DependencyPending where a root task failed
    let failed_keys: Vec<&str> = tasks
        .iter()
        .filter(|t| t.state == TaskState::TaskFailed)
        .map(|t| t.task_key.as_str())
        .collect();
    let dependency_blocked: Vec<&TaskOutcome> = tasks
        .iter()
        .filter(|t| {
            t.state == TaskState::TaskBlocked
                && matches!(t.blocker, Some(BlockerCode::DependencyPending))
        })
        .collect();
    if !failed_keys.is_empty() && dependency_blocked.len() >= 2 {
        // Use the first failed task as root (simplification)
        let root = failed_keys[0].to_string();
        let affected: Vec<String> = dependency_blocked
            .iter()
            .map(|t| t.task_key.clone())
            .collect();
        patterns.push(FailurePattern::CascadingFailure {
            root_task_key: root,
            affected,
        });
    }

    // Detect RepeatedBlocker: same BlockerCode across 2+ tasks
    let mut blocker_groups: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for t in tasks.iter().filter(|t| t.blocker.is_some()) {
        let code = format!("{:?}", t.blocker.as_ref().unwrap());
        blocker_groups
            .entry(code)
            .or_default()
            .push(t.task_key.clone());
    }
    for (blocker, task_keys) in &blocker_groups {
        if task_keys.len() >= 2 {
            patterns.push(FailurePattern::RepeatedBlocker {
                blocker: blocker.clone(),
                task_keys: task_keys.clone(),
            });
        }
    }

    // Detect GateOnlyFailure
    let all_tasks_ok = tasks
        .iter()
        .all(|t| t.state == TaskState::TaskComplete || t.state == TaskState::TaskCancelled);
    if all_tasks_ok
        && !gate_failures.is_empty()
        && matches!(exit_reason, Some(ExitReason::BlockedGateFailure))
    {
        patterns.push(FailurePattern::GateOnlyFailure {
            gate_names: gate_failures.to_vec(),
        });
    }

    // Detect AllTasksSucceededRunFailed (non-gate)
    if all_tasks_ok
        && gate_failures.is_empty()
        && !exit_state.is_work_done()
        && !matches!(exit_reason, Some(ExitReason::BlockedGateFailure))
        && !matches!(
            exit_reason,
            Some(ExitReason::TimedOut) | Some(ExitReason::StalledNoProgress)
        )
    {
        let reason_str = exit_reason
            .map(|r| r.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        patterns.push(FailurePattern::AllTasksSucceededRunFailed {
            exit_reason: reason_str,
        });
    }

    // Detect SingleTaskFailure
    let failed_tasks: Vec<&TaskOutcome> = tasks
        .iter()
        .filter(|t| t.state == TaskState::TaskFailed)
        .collect();
    if failed_tasks.len() == 1 {
        let t = failed_tasks[0];
        patterns.push(FailurePattern::SingleTaskFailure {
            task_key: t.task_key.clone(),
            error: t
                .last_error
                .clone()
                .unwrap_or_else(|| "unknown error".to_string()),
        });
    }

    // Compute retry recommendation
    let retry_recommendation = compute_retry_recommendation(&patterns, tasks);

    // Compute confidence
    let confidence = compute_confidence(&patterns);

    // Build lesson
    let run_lesson = build_lesson(&patterns, exit_reason, gate_failures);

    RunAnalysis {
        patterns,
        run_lesson,
        retry_recommendation,
        confidence,
    }
}

fn compute_retry_recommendation(patterns: &[FailurePattern], tasks: &[TaskOutcome]) -> RetryScope {
    if patterns.is_empty() {
        return RetryScope::None;
    }

    // Check for patterns that preclude retry
    for pattern in patterns {
        match pattern {
            FailurePattern::TimeoutOrStall { .. } => return RetryScope::None,
            FailurePattern::RepeatedBlocker { blocker, .. } if blocker.contains("PolicyDenial") => {
                return RetryScope::None;
            }
            _ => {}
        }
    }

    // Check for patterns with specific retry scopes
    for pattern in patterns {
        match pattern {
            FailurePattern::CascadingFailure { root_task_key, .. } => {
                return RetryScope::Subset {
                    keys: vec![root_task_key.clone()],
                };
            }
            FailurePattern::GateOnlyFailure { .. } => {
                return RetryScope::Full;
            }
            FailurePattern::SingleTaskFailure { task_key, .. } => {
                return RetryScope::Subset {
                    keys: vec![task_key.clone()],
                };
            }
            _ => {}
        }
    }

    // Fallback: retry all failed tasks
    let failed_keys: Vec<String> = tasks
        .iter()
        .filter(|t| t.state == TaskState::TaskFailed)
        .map(|t| t.task_key.clone())
        .collect();
    if failed_keys.is_empty() {
        RetryScope::None
    } else {
        RetryScope::Subset { keys: failed_keys }
    }
}

fn compute_confidence(patterns: &[FailurePattern]) -> f64 {
    match patterns.len() {
        0 => 1.0,
        1 => 0.9,
        2 => 0.7,
        _ => 0.5,
    }
}

fn build_lesson(
    patterns: &[FailurePattern],
    exit_reason: Option<ExitReason>,
    gate_failures: &[String],
) -> Option<String> {
    if patterns.is_empty() {
        return None;
    }

    let mut parts = Vec::new();

    if let Some(reason) = exit_reason {
        parts.push(format!("run exited with: {reason}"));
    }

    for pattern in patterns {
        match pattern {
            FailurePattern::CascadingFailure {
                root_task_key,
                affected,
            } => {
                parts.push(format!(
                    "cascading failure from root task '{root_task_key}' blocking {} dependent tasks",
                    affected.len()
                ));
            }
            FailurePattern::RepeatedBlocker { blocker, task_keys } => {
                parts.push(format!(
                    "repeated blocker '{blocker}' across tasks: {}",
                    task_keys.join(", ")
                ));
            }
            FailurePattern::GateOnlyFailure { gate_names } => {
                parts.push(format!(
                    "all tasks passed but gates failed: {}",
                    gate_names.join(", ")
                ));
            }
            FailurePattern::AllTasksSucceededRunFailed { exit_reason } => {
                parts.push(format!(
                    "all tasks succeeded but run failed with: {exit_reason}"
                ));
            }
            FailurePattern::SingleTaskFailure { task_key, error } => {
                parts.push(format!("single task failure: '{task_key}' — {error}"));
            }
            FailurePattern::TimeoutOrStall { exit_reason } => {
                parts.push(format!("run stalled/timed out: {exit_reason}"));
            }
        }
    }

    if !gate_failures.is_empty() && !parts.iter().any(|p| p.contains("gates failed")) {
        parts.push(format!("gate failures: {}", gate_failures.join(", ")));
    }

    Some(parts.join("; "))
}

/// Extract pattern names as strings for audit logging.
pub fn pattern_names(patterns: &[FailurePattern]) -> Vec<String> {
    patterns
        .iter()
        .map(|p| match p {
            FailurePattern::CascadingFailure { .. } => "cascading_failure".to_string(),
            FailurePattern::RepeatedBlocker { .. } => "repeated_blocker".to_string(),
            FailurePattern::GateOnlyFailure { .. } => "gate_only_failure".to_string(),
            FailurePattern::AllTasksSucceededRunFailed { .. } => {
                "all_tasks_succeeded_run_failed".to_string()
            }
            FailurePattern::SingleTaskFailure { .. } => "single_task_failure".to_string(),
            FailurePattern::TimeoutOrStall { .. } => "timeout_or_stall".to_string(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(key: &str, state: TaskState) -> TaskOutcome {
        TaskOutcome {
            task_key: key.to_string(),
            state,
            last_error: None,
            blocker: None,
        }
    }

    fn make_failed_task(key: &str, error: &str) -> TaskOutcome {
        TaskOutcome {
            task_key: key.to_string(),
            state: TaskState::TaskFailed,
            last_error: Some(error.to_string()),
            blocker: None,
        }
    }

    fn make_blocked_task(key: &str, blocker: BlockerCode) -> TaskOutcome {
        TaskOutcome {
            task_key: key.to_string(),
            state: TaskState::TaskBlocked,
            last_error: None,
            blocker: Some(blocker),
        }
    }

    #[test]
    fn analyze_clean_completion() {
        let tasks = vec![
            make_task("build", TaskState::TaskComplete),
            make_task("test", TaskState::TaskComplete),
        ];
        let result = analyze_run(
            RunState::RunCompleted,
            Some(ExitReason::CompletedAllGates),
            &tasks,
            &[],
        );
        assert!(result.patterns.is_empty());
        assert!(result.run_lesson.is_none());
        assert_eq!(result.retry_recommendation, RetryScope::None);
        assert!((result.confidence - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn analyze_completed_with_merge_failure_as_work_done() {
        let tasks = vec![
            make_task("build", TaskState::TaskComplete),
            make_task("test", TaskState::TaskComplete),
        ];
        let result = analyze_run(
            RunState::RunCompletedWithMergeFailure,
            Some(ExitReason::CompletedMergeTeardownFailed),
            &tasks,
            &[],
        );
        assert!(result.patterns.is_empty());
        assert!(result.run_lesson.is_none());
        assert_eq!(result.retry_recommendation, RetryScope::None);
    }

    #[test]
    fn analyze_single_task_failure() {
        let tasks = vec![
            make_task("build", TaskState::TaskComplete),
            make_failed_task("test", "exit code 1"),
        ];
        let result = analyze_run(
            RunState::RunFailed,
            Some(ExitReason::BlockedOpenTasks),
            &tasks,
            &[],
        );
        assert!(result
            .patterns
            .iter()
            .any(|p| matches!(p, FailurePattern::SingleTaskFailure { task_key, .. } if task_key == "test")));
        assert_eq!(
            result.retry_recommendation,
            RetryScope::Subset {
                keys: vec!["test".to_string()]
            }
        );
    }

    #[test]
    fn analyze_cascading_failure() {
        let tasks = vec![
            make_failed_task("root", "compilation error"),
            make_blocked_task("dep-a", BlockerCode::DependencyPending),
            make_blocked_task("dep-b", BlockerCode::DependencyPending),
        ];
        let result = analyze_run(
            RunState::RunFailed,
            Some(ExitReason::BlockedOpenTasks),
            &tasks,
            &[],
        );
        assert!(result.patterns.iter().any(
            |p| matches!(p, FailurePattern::CascadingFailure { root_task_key, affected } if root_task_key == "root" && affected.len() == 2)
        ));
        assert_eq!(
            result.retry_recommendation,
            RetryScope::Subset {
                keys: vec!["root".to_string()]
            }
        );
    }

    #[test]
    fn analyze_repeated_blocker() {
        let tasks = vec![
            make_blocked_task("task-a", BlockerCode::PolicyDenial),
            make_blocked_task("task-b", BlockerCode::PolicyDenial),
        ];
        let result = analyze_run(
            RunState::RunFailed,
            Some(ExitReason::FailedPolicyDenial),
            &tasks,
            &[],
        );
        assert!(result
            .patterns
            .iter()
            .any(|p| matches!(p, FailurePattern::RepeatedBlocker { task_keys, .. } if task_keys.len() == 2)));
        // PolicyDenial blockers → no retry
        assert_eq!(result.retry_recommendation, RetryScope::None);
    }

    #[test]
    fn analyze_gate_only_failure() {
        let tasks = vec![
            make_task("build", TaskState::TaskComplete),
            make_task("test", TaskState::TaskComplete),
        ];
        let result = analyze_run(
            RunState::RunFailed,
            Some(ExitReason::BlockedGateFailure),
            &tasks,
            &["tests_passed".to_string(), "lint_clean".to_string()],
        );
        assert!(result.patterns.iter().any(
            |p| matches!(p, FailurePattern::GateOnlyFailure { gate_names } if gate_names.len() == 2)
        ));
        assert_eq!(result.retry_recommendation, RetryScope::Full);
    }

    #[test]
    fn analyze_timeout() {
        let tasks = vec![make_task("build", TaskState::TaskExecuting)];
        let result = analyze_run(RunState::RunFailed, Some(ExitReason::TimedOut), &tasks, &[]);
        assert!(result
            .patterns
            .iter()
            .any(|p| matches!(p, FailurePattern::TimeoutOrStall { .. })));
        assert_eq!(result.retry_recommendation, RetryScope::None);
    }

    #[test]
    fn analyze_stalled() {
        let tasks = vec![make_task("build", TaskState::TaskExecuting)];
        let result = analyze_run(
            RunState::RunFailed,
            Some(ExitReason::StalledNoProgress),
            &tasks,
            &[],
        );
        assert!(result.patterns.iter().any(
            |p| matches!(p, FailurePattern::TimeoutOrStall { exit_reason } if exit_reason == "stalled_no_progress")
        ));
    }

    #[test]
    fn analyze_all_tasks_ok_but_run_failed_non_gate() {
        let tasks = vec![
            make_task("build", TaskState::TaskComplete),
            make_task("test", TaskState::TaskComplete),
        ];
        let result = analyze_run(
            RunState::RunFailed,
            Some(ExitReason::FailedRuntimeError),
            &tasks,
            &[],
        );
        assert!(result
            .patterns
            .iter()
            .any(|p| matches!(p, FailurePattern::AllTasksSucceededRunFailed { .. })));
    }

    #[test]
    fn analyze_mixed_failures() {
        let tasks = vec![
            make_failed_task("root", "build error"),
            make_blocked_task("dep-a", BlockerCode::DependencyPending),
            make_blocked_task("dep-b", BlockerCode::DependencyPending),
        ];
        let result = analyze_run(RunState::RunFailed, Some(ExitReason::TimedOut), &tasks, &[]);
        // Should detect both TimeoutOrStall and CascadingFailure
        assert!(result
            .patterns
            .iter()
            .any(|p| matches!(p, FailurePattern::TimeoutOrStall { .. })));
        assert!(result
            .patterns
            .iter()
            .any(|p| matches!(p, FailurePattern::CascadingFailure { .. })));
        assert!(result.patterns.len() >= 2);
    }

    #[test]
    fn lesson_text_contains_pattern_info() {
        let tasks = vec![make_failed_task("test", "assertion failed")];
        let result = analyze_run(
            RunState::RunFailed,
            Some(ExitReason::BlockedOpenTasks),
            &tasks,
            &[],
        );
        let lesson = result.run_lesson.expect("should have a lesson");
        assert!(lesson.contains("test"));
        assert!(lesson.contains("assertion failed"));
    }

    #[test]
    fn confidence_high_for_single_pattern() {
        let tasks = vec![make_failed_task("test", "exit code 1")];
        let result = analyze_run(
            RunState::RunFailed,
            Some(ExitReason::BlockedOpenTasks),
            &tasks,
            &[],
        );
        assert!(
            result.confidence >= 0.85,
            "confidence={}",
            result.confidence
        );
    }

    #[test]
    fn confidence_lower_for_multiple_patterns() {
        let tasks = vec![
            make_failed_task("root", "build error"),
            make_blocked_task("dep-a", BlockerCode::DependencyPending),
            make_blocked_task("dep-b", BlockerCode::DependencyPending),
        ];
        let result = analyze_run(RunState::RunFailed, Some(ExitReason::TimedOut), &tasks, &[]);
        assert!(
            result.confidence < 0.85,
            "confidence={} should be lower for multiple patterns",
            result.confidence
        );
    }
}
