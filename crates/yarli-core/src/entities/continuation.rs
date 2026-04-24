//! Structured continuation payload emitted when a run reaches a terminal state.
//!
//! The `ContinuationPayload` captures "what changed + what remains" as
//! machine-readable JSON so downstream tooling (or `yarli run continue`)
//! can pick up where the previous run left off.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::yarli_core::domain::{
    CancellationProvenance, CancellationSource, ExitReason, RunId, TaskId,
};
use crate::yarli_core::entities::run::Run;
use crate::yarli_core::entities::task::{BlockerCode, Task};
use crate::yarli_core::explain::DeteriorationTrend;
use crate::yarli_core::fsm::run::RunState;
use crate::yarli_core::fsm::task::TaskState;

/// Structured handoff emitted when a run reaches a terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuationPayload {
    pub run_id: RunId,
    pub objective: String,
    #[serde(with = "run_state_pascal_case")]
    pub exit_state: RunState,
    pub exit_reason: Option<ExitReason>,
    #[serde(default)]
    pub cancellation_source: Option<CancellationSource>,
    #[serde(default)]
    pub cancellation_provenance: Option<CancellationProvenance>,
    pub completed_at: DateTime<Utc>,
    pub tasks: Vec<TaskOutcome>,
    pub summary: RunSummary,
    pub next_tranche: Option<TrancheSpec>,
    #[serde(default)]
    pub quality_gate: Option<ContinuationQualityGate>,
    #[serde(default)]
    pub retry_recommendation: Option<RetryScope>,
    #[serde(default)]
    pub tranche_token_usage: Vec<TrancheTokenUsageSummary>,
    #[serde(default)]
    pub tranche_token_thresholds: Option<TrancheTokenThresholds>,
}

mod run_state_pascal_case {
    use crate::yarli_core::fsm::run::RunState;
    use serde::de::Error as DeError;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(state: &RunState, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match state {
            RunState::RunOpen => "RunOpen",
            RunState::RunActive => "RunActive",
            RunState::RunVerifying => "RunVerifying",
            RunState::RunBlocked => "RunBlocked",
            RunState::RunFailed => "RunFailed",
            RunState::RunCompleted => "RunCompleted",
            RunState::RunCompletedWithMergeFailure => "RunCompletedWithMergeFailure",
            RunState::RunCancelled => "RunCancelled",
            RunState::RunDrained => "RunDrained",
        })
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<RunState, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(match raw.as_str() {
            "RunOpen" | "RUN_OPEN" => RunState::RunOpen,
            "RunActive" | "RUN_ACTIVE" => RunState::RunActive,
            "RunVerifying" | "RUN_VERIFYING" => RunState::RunVerifying,
            "RunBlocked" | "RUN_BLOCKED" => RunState::RunBlocked,
            "RunFailed" | "RUN_FAILED" => RunState::RunFailed,
            "RunCompleted" | "RUN_COMPLETED" => RunState::RunCompleted,
            "RunCompletedWithMergeFailure" | "RUN_COMPLETED_WITH_MERGE_FAILURE" => {
                RunState::RunCompletedWithMergeFailure
            }
            "RunCancelled" | "RUN_CANCELLED" => RunState::RunCancelled,
            "RunDrained" | "RUN_DRAINED" => RunState::RunDrained,
            other => {
                return Err(D::Error::unknown_variant(
                    other,
                    &[
                        "RunOpen",
                        "RunActive",
                        "RunVerifying",
                        "RunBlocked",
                        "RunFailed",
                        "RunCompleted",
                        "RunCompletedWithMergeFailure",
                        "RunCancelled",
                        "RunDrained",
                        "RUN_OPEN",
                        "RUN_ACTIVE",
                        "RUN_VERIFYING",
                        "RUN_BLOCKED",
                        "RUN_FAILED",
                        "RUN_COMPLETED",
                        "RUN_COMPLETED_WITH_MERGE_FAILURE",
                        "RUN_CANCELLED",
                        "RUN_DRAINED",
                    ],
                ))
            }
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskHealthAction {
    #[default]
    Continue,
    CheckpointNow,
    ForcePivot,
    StopAndSummarize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContinuationInterventionKind {
    StrategyPivotCheckpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContinuationIntervention {
    pub kind: ContinuationInterventionKind,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrancheTokenAdvisoryLevel {
    Healthy,
    Warning,
    Exceeded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrancheTokenThresholds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    pub target_tokens: u64,
    pub max_recommended_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrancheTokenUsageSummary {
    pub tranche_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tranche_group: Option<String>,
    pub task_count: u32,
    pub completed_tasks: u32,
    pub failed_tasks: u32,
    pub retried_tasks: u32,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rehydration_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advisory: Option<TrancheTokenAdvisoryLevel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Outcome record for a single task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskOutcome {
    pub task_id: TaskId,
    pub task_key: String,
    pub state: TaskState,
    pub attempt_no: u32,
    pub last_error: Option<String>,
    pub blocker: Option<BlockerCode>,
}

const ALLOWED_PATHS_SCOPE_MISMATCH_PREFIX: &str = "allowed_paths scope mismatch:";

pub fn parse_allowed_paths_scope_mismatch(last_error: &str) -> Option<Vec<String>> {
    let remainder = last_error
        .strip_prefix(ALLOWED_PATHS_SCOPE_MISMATCH_PREFIX)?
        .trim();
    let paths_segment = remainder
        .split_once(" (allowed_paths:")
        .map(|(paths, _)| paths)
        .unwrap_or(remainder);
    let paths: Vec<String> = paths_segment
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    (!paths.is_empty()).then_some(paths)
}

pub fn format_allowed_paths_scope_mismatch(
    suggested_allowed_paths: &[String],
    allowed_paths: &[String],
) -> String {
    let suggested = suggested_allowed_paths.join(", ");
    if allowed_paths.is_empty() {
        format!("{ALLOWED_PATHS_SCOPE_MISMATCH_PREFIX} {suggested}")
    } else {
        format!(
            "{ALLOWED_PATHS_SCOPE_MISMATCH_PREFIX} {suggested} (allowed_paths: {})",
            allowed_paths.join(", ")
        )
    }
}

pub fn collect_allowed_paths_scope_suggestions(tasks: &[TaskOutcome]) -> Vec<String> {
    let mut suggestions: Vec<String> = tasks
        .iter()
        .filter(|task| task.state == TaskState::TaskFailed)
        .filter_map(|task| task.last_error.as_deref())
        .filter_map(parse_allowed_paths_scope_mismatch)
        .flatten()
        .collect();
    suggestions.sort();
    suggestions.dedup();
    suggestions
}

/// Aggregate counts across all tasks in the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub total: u32,
    pub completed: u32,
    pub failed: u32,
    pub cancelled: u32,
    pub pending: u32,
}

/// Aggregate token usage for a planned tranche.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrancheTokenSummary {
    pub tranche_key: String,
    #[serde(default)]
    pub tranche_group: Option<String>,
    pub task_count: u32,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    /// Heuristic estimate of repeated prompt/context loading within the tranche.
    #[serde(default)]
    pub rehydration_tokens: u64,
    #[serde(default)]
    pub warning: Option<String>,
}

/// What to do next — enough info for `yarli run continue`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrancheSpec {
    pub suggested_objective: String,
    #[serde(default)]
    pub kind: TrancheKind,
    pub retry_task_keys: Vec<String>,
    pub unfinished_task_keys: Vec<String>,
    #[serde(default)]
    pub planned_task_keys: Vec<String>,
    #[serde(default)]
    pub planned_tranche_key: Option<String>,
    #[serde(default)]
    pub cursor: Option<TrancheCursor>,
    pub config_snapshot: serde_json::Value,
    #[serde(default)]
    pub interventions: Vec<ContinuationIntervention>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrancheKind {
    #[default]
    RetryUnfinished,
    PlannedNext,
    /// All tasks completed but run-level gates failed. Re-run the same tranche
    /// so gates can be re-evaluated after the operator addresses the gate issue.
    GateRetry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrancheCursor {
    pub current_tranche_index: Option<usize>,
    pub next_tranche_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContinuationQualityGate {
    pub allow_auto_advance: bool,
    pub reason: String,
    pub trend: Option<DeteriorationTrend>,
    pub score: Option<f64>,
    #[serde(default)]
    pub task_health_action: TaskHealthAction,
}

/// Retry recommendation derived from run analysis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum RetryScope {
    /// Retry the full run.
    Full,
    /// Retry a subset of tasks.
    Subset { keys: Vec<String> },
    /// Do not retry; run may need manual intervention.
    None,
}

impl ContinuationPayload {
    /// Build a continuation payload from a terminal run and its tasks.
    pub fn build(run: &Run, tasks: &[&Task]) -> Self {
        let task_outcomes: Vec<TaskOutcome> = tasks
            .iter()
            .map(|t| TaskOutcome {
                task_id: t.id,
                task_key: t.task_key.clone(),
                state: t.state,
                attempt_no: t.attempt_no,
                last_error: t.last_error.clone(),
                blocker: t.blocker.clone(),
            })
            .collect();

        let mut completed = 0u32;
        let mut failed = 0u32;
        let mut cancelled = 0u32;
        let mut pending = 0u32;

        for outcome in &task_outcomes {
            match outcome.state {
                TaskState::TaskComplete => completed += 1,
                TaskState::TaskFailed => failed += 1,
                TaskState::TaskCancelled => cancelled += 1,
                _ => pending += 1,
            }
        }

        let total = task_outcomes.len() as u32;
        let summary = RunSummary {
            total,
            completed,
            failed,
            cancelled,
            pending,
        };

        let current_tranche_index = current_tranche_index_from_snapshot(&run.config_snapshot);

        // Build tranche spec if there's remaining work.
        let retry_task_keys: Vec<String> = task_outcomes
            .iter()
            .filter(|o| o.state == TaskState::TaskFailed)
            .map(|o| o.task_key.clone())
            .collect();

        let unfinished_task_keys: Vec<String> = task_outcomes
            .iter()
            .filter(|o| !o.state.is_terminal())
            .map(|o| o.task_key.clone())
            .collect();

        let scope_violation_task_keys: Vec<String> = task_outcomes
            .iter()
            .filter(|o| o.state == TaskState::TaskFailed)
            .filter(|o| {
                o.last_error
                    .as_deref()
                    .and_then(parse_allowed_paths_scope_mismatch)
                    .is_some()
            })
            .map(|o| o.task_key.clone())
            .collect();
        let scope_violation_suggestions = collect_allowed_paths_scope_suggestions(&task_outcomes);

        let next_tranche = if !retry_task_keys.is_empty() || !unfinished_task_keys.is_empty() {
            let suggested_objective = if !scope_violation_task_keys.is_empty() {
                if scope_violation_suggestions.is_empty() {
                    format!(
                        "Recover verified allowed_paths scope mismatch for tasks: {}",
                        scope_violation_task_keys.join(", ")
                    )
                } else {
                    format!(
                        "Recover verified allowed_paths scope mismatch for tasks: {}. Reconcile allowed_paths with: {}",
                        scope_violation_task_keys.join(", "),
                        scope_violation_suggestions.join(", ")
                    )
                }
            } else if !retry_task_keys.is_empty() {
                format!("Retry failed tasks: {}", retry_task_keys.join(", "))
            } else {
                format!(
                    "Continue unfinished tasks: {}",
                    unfinished_task_keys.join(", ")
                )
            };

            Some(TrancheSpec {
                suggested_objective,
                kind: TrancheKind::RetryUnfinished,
                retry_task_keys,
                unfinished_task_keys,
                planned_task_keys: Vec::new(),
                planned_tranche_key: None,
                cursor: Some(TrancheCursor {
                    current_tranche_index,
                    next_tranche_index: current_tranche_index,
                }),
                config_snapshot: run.config_snapshot.clone(),
                interventions: Vec::new(),
            })
        } else if run.state == RunState::RunFailed
            && run.exit_reason == Some(crate::yarli_core::domain::ExitReason::BlockedGateFailure)
        {
            // All tasks completed but run-level gates failed. Emit a GateRetry
            // tranche so the operator can re-verify after addressing the gate issue.
            let all_task_keys: Vec<String> =
                task_outcomes.iter().map(|o| o.task_key.clone()).collect();
            Some(TrancheSpec {
                suggested_objective: "Re-verify after gate failure (all tasks completed)"
                    .to_string(),
                kind: TrancheKind::GateRetry,
                retry_task_keys: all_task_keys,
                unfinished_task_keys: Vec::new(),
                planned_task_keys: Vec::new(),
                planned_tranche_key: None,
                cursor: Some(TrancheCursor {
                    current_tranche_index,
                    next_tranche_index: current_tranche_index,
                }),
                config_snapshot: run.config_snapshot.clone(),
                interventions: Vec::new(),
            })
        } else if run.state == RunState::RunCompleted {
            planned_next_from_snapshot(&run.config_snapshot, current_tranche_index).map(|next| {
                let suggested_objective = next
                    .objective
                    .unwrap_or_else(|| format!("Continue planned tranche: {}", next.tranche_key));
                TrancheSpec {
                    suggested_objective,
                    kind: TrancheKind::PlannedNext,
                    retry_task_keys: Vec::new(),
                    unfinished_task_keys: Vec::new(),
                    planned_task_keys: next.task_keys,
                    planned_tranche_key: Some(next.tranche_key),
                    cursor: Some(TrancheCursor {
                        current_tranche_index,
                        next_tranche_index: Some(next.next_index),
                    }),
                    config_snapshot: run.config_snapshot.clone(),
                    interventions: Vec::new(),
                }
            })
        } else {
            None
        };

        Self {
            run_id: run.id,
            objective: run.objective.clone(),
            exit_state: run.state,
            exit_reason: run.exit_reason,
            cancellation_source: run.cancellation_source,
            cancellation_provenance: run.cancellation_provenance.clone(),
            completed_at: Utc::now(),
            tasks: task_outcomes,
            summary,
            next_tranche,
            quality_gate: None,
            retry_recommendation: None,
            tranche_token_usage: Vec::new(),
            tranche_token_thresholds: None,
        }
    }
}

#[derive(Debug, Clone)]
struct PlannedNextSpec {
    tranche_key: String,
    objective: Option<String>,
    task_keys: Vec<String>,
    next_index: usize,
}

fn current_tranche_index_from_snapshot(config_snapshot: &serde_json::Value) -> Option<usize> {
    let value = config_snapshot
        .get("runtime")
        .and_then(|runtime| runtime.get("current_tranche_index"))?
        .as_u64()?;
    usize::try_from(value).ok()
}

fn planned_next_from_snapshot(
    config_snapshot: &serde_json::Value,
    current_index: Option<usize>,
) -> Option<PlannedNextSpec> {
    let runtime = config_snapshot.get("runtime")?;
    let plan = runtime.get("tranche_plan")?.as_array()?;
    let current_index = current_index?;
    let next_index = current_index.saturating_add(1);
    let next = plan.get(next_index)?;
    let tranche_key = next.get("key")?.as_str()?.to_string();
    let objective = next
        .get("objective")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    let task_keys: Vec<String> = next
        .get("task_keys")?
        .as_array()?
        .iter()
        .filter_map(|value| value.as_str().map(ToString::to_string))
        .collect();
    if task_keys.is_empty() {
        return None;
    }
    Some(PlannedNextSpec {
        tranche_key,
        objective,
        task_keys,
        next_index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yarli_core::domain::{CommandClass, SafeMode};
    use crate::yarli_core::entities::run::Run;
    use crate::yarli_core::entities::task::Task;

    fn make_run() -> Run {
        Run::with_config(
            "test objective",
            SafeMode::Execute,
            serde_json::json!({"key": "value"}),
        )
    }

    fn make_task(run: &Run, key: &str) -> Task {
        Task::new(
            run.id,
            key,
            format!("do {key}"),
            CommandClass::Io,
            run.correlation_id,
        )
    }

    #[test]
    fn build_payload_all_complete() {
        let run = make_run();
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;
        let mut t2 = make_task(&run, "test");
        t2.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1, &t2]);

        assert_eq!(payload.run_id, run.id);
        assert_eq!(payload.objective, "test objective");
        assert_eq!(payload.summary.total, 2);
        assert_eq!(payload.summary.completed, 2);
        assert_eq!(payload.summary.failed, 0);
        assert_eq!(payload.summary.pending, 0);
        assert!(payload.next_tranche.is_none());
    }

    #[test]
    fn build_payload_with_failed_tasks() {
        let run = make_run();
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;
        let mut t2 = make_task(&run, "lint");
        t2.state = TaskState::TaskFailed;
        t2.last_error = Some("clippy warnings".into());
        let mut t3 = make_task(&run, "test");
        t3.state = TaskState::TaskFailed;
        t3.last_error = Some("3 tests failed".into());

        let payload = ContinuationPayload::build(&run, &[&t1, &t2, &t3]);

        assert_eq!(payload.summary.total, 3);
        assert_eq!(payload.summary.completed, 1);
        assert_eq!(payload.summary.failed, 2);
        let tranche = payload.next_tranche.as_ref().unwrap();
        assert_eq!(tranche.kind, TrancheKind::RetryUnfinished);
        assert_eq!(tranche.retry_task_keys, vec!["lint", "test"]);
        assert!(tranche.unfinished_task_keys.is_empty());
        assert!(tranche.suggested_objective.contains("Retry failed tasks"));
        assert_eq!(tranche.config_snapshot, serde_json::json!({"key": "value"}));
    }

    #[test]
    fn build_payload_with_unfinished_tasks() {
        let run = make_run();
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;
        let mut t2 = make_task(&run, "deploy");
        t2.state = TaskState::TaskOpen;

        let payload = ContinuationPayload::build(&run, &[&t1, &t2]);

        assert_eq!(payload.summary.completed, 1);
        assert_eq!(payload.summary.pending, 1);
        let tranche = payload.next_tranche.as_ref().unwrap();
        assert_eq!(tranche.kind, TrancheKind::RetryUnfinished);
        assert!(tranche.retry_task_keys.is_empty());
        assert_eq!(tranche.unfinished_task_keys, vec!["deploy"]);
        assert!(tranche.suggested_objective.contains("Continue unfinished"));
    }

    #[test]
    fn build_payload_with_mixed_states() {
        let run = make_run();
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;
        let mut t2 = make_task(&run, "lint");
        t2.state = TaskState::TaskFailed;
        let mut t3 = make_task(&run, "deploy");
        t3.state = TaskState::TaskCancelled;
        let mut t4 = make_task(&run, "notify");
        t4.state = TaskState::TaskBlocked;

        let payload = ContinuationPayload::build(&run, &[&t1, &t2, &t3, &t4]);

        assert_eq!(payload.summary.total, 4);
        assert_eq!(payload.summary.completed, 1);
        assert_eq!(payload.summary.failed, 1);
        assert_eq!(payload.summary.cancelled, 1);
        assert_eq!(payload.summary.pending, 1);
        let tranche = payload.next_tranche.as_ref().unwrap();
        assert_eq!(tranche.kind, TrancheKind::RetryUnfinished);
        assert_eq!(tranche.retry_task_keys, vec!["lint"]);
        assert_eq!(tranche.unfinished_task_keys, vec!["notify"]);
    }

    #[test]
    fn build_payload_preserves_exit_reason() {
        let mut run = make_run();
        run.exit_reason = Some(ExitReason::FailedRuntimeError);
        run.state = RunState::RunFailed;
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskFailed;

        let payload = ContinuationPayload::build(&run, &[&t1]);

        assert_eq!(payload.exit_state, RunState::RunFailed);
        assert_eq!(payload.exit_reason, Some(ExitReason::FailedRuntimeError));
    }

    #[test]
    fn build_payload_empty_tasks() {
        let run = make_run();
        let payload = ContinuationPayload::build(&run, &[]);

        assert_eq!(payload.summary.total, 0);
        assert!(payload.next_tranche.is_none());
    }

    #[test]
    fn payload_serializes_to_json() {
        let run = make_run();
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskFailed;
        t1.last_error = Some("compilation error".into());

        let payload = ContinuationPayload::build(&run, &[&t1]);
        let json = serde_json::to_string(&payload).unwrap();
        let roundtrip: ContinuationPayload = serde_json::from_str(&json).unwrap();

        assert_eq!(roundtrip.run_id, payload.run_id);
        assert_eq!(roundtrip.summary.failed, 1);
        assert_eq!(
            roundtrip.tasks[0].last_error,
            Some("compilation error".into())
        );
    }

    #[test]
    fn task_outcome_preserves_blocker() {
        let run = make_run();
        let mut t1 = make_task(&run, "deploy");
        t1.state = TaskState::TaskBlocked;
        t1.blocker = Some(BlockerCode::PolicyDenial);

        let payload = ContinuationPayload::build(&run, &[&t1]);

        assert_eq!(payload.tasks[0].blocker, Some(BlockerCode::PolicyDenial));
    }

    #[test]
    fn build_payload_includes_planned_next_on_completed_run() {
        let mut run = Run::with_config(
            "test objective",
            SafeMode::Execute,
            serde_json::json!({
                "runtime": {
                    "current_tranche_index": 0,
                    "tranche_plan": [
                        { "key": "one", "objective": "first", "task_keys": ["build"] },
                        { "key": "two", "objective": "second", "task_keys": ["test"] }
                    ]
                }
            }),
        );
        run.state = RunState::RunCompleted;
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1]);
        let tranche = payload.next_tranche.expect("planned next tranche expected");
        assert_eq!(tranche.kind, TrancheKind::PlannedNext);
        assert_eq!(tranche.planned_task_keys, vec!["test"]);
        assert_eq!(tranche.planned_tranche_key.as_deref(), Some("two"));
        assert!(tranche.retry_task_keys.is_empty());
        assert!(tranche.unfinished_task_keys.is_empty());
    }

    #[test]
    fn build_payload_gate_retry_when_all_tasks_completed_but_run_failed_gate() {
        let mut run = make_run();
        run.state = RunState::RunFailed;
        run.exit_reason = Some(ExitReason::BlockedGateFailure);
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;
        let mut t2 = make_task(&run, "test");
        t2.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1, &t2]);

        assert_eq!(payload.exit_state, RunState::RunFailed);
        assert_eq!(payload.exit_reason, Some(ExitReason::BlockedGateFailure));
        let tranche = payload
            .next_tranche
            .as_ref()
            .expect("gate retry tranche expected");
        assert_eq!(tranche.kind, TrancheKind::GateRetry);
        assert_eq!(tranche.retry_task_keys, vec!["build", "test"]);
        assert!(tranche.unfinished_task_keys.is_empty());
        assert!(tranche.suggested_objective.contains("gate failure"));
    }

    #[test]
    fn build_payload_no_gate_retry_when_tasks_also_failed() {
        let mut run = make_run();
        run.state = RunState::RunFailed;
        run.exit_reason = Some(ExitReason::BlockedGateFailure);
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;
        let mut t2 = make_task(&run, "test");
        t2.state = TaskState::TaskFailed;

        let payload = ContinuationPayload::build(&run, &[&t1, &t2]);

        // When tasks are failed, RetryUnfinished takes precedence over GateRetry
        let tranche = payload.next_tranche.as_ref().unwrap();
        assert_eq!(tranche.kind, TrancheKind::RetryUnfinished);
        assert_eq!(tranche.retry_task_keys, vec!["test"]);
    }

    #[test]
    fn build_payload_no_gate_retry_when_run_failed_for_other_reason() {
        let mut run = make_run();
        run.state = RunState::RunFailed;
        run.exit_reason = Some(ExitReason::FailedRuntimeError);
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1]);

        // Non-gate failures with all tasks complete → no next_tranche
        assert!(payload.next_tranche.is_none());
    }

    #[test]
    fn gate_retry_tranche_round_trips_through_json() {
        let mut run = make_run();
        run.state = RunState::RunFailed;
        run.exit_reason = Some(ExitReason::BlockedGateFailure);
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1]);
        let json = serde_json::to_string(&payload).unwrap();
        let roundtrip: ContinuationPayload = serde_json::from_str(&json).unwrap();

        let tranche = roundtrip.next_tranche.as_ref().unwrap();
        assert_eq!(tranche.kind, TrancheKind::GateRetry);
    }

    #[test]
    fn allowed_paths_scope_mismatch_round_trips_into_recovery_objective() {
        let run = make_run();
        let mut t1 = make_task(&run, "task-scope");
        t1.state = TaskState::TaskFailed;
        t1.last_error = Some(format_allowed_paths_scope_mismatch(
            &["README.md".to_string()],
            &["src/lib.rs".to_string()],
        ));

        let payload = ContinuationPayload::build(&run, &[&t1]);
        let tranche = payload.next_tranche.expect("retry tranche expected");

        assert!(
            tranche
                .suggested_objective
                .contains("allowed_paths scope mismatch"),
            "objective should call out scope mismatch: {}",
            tranche.suggested_objective
        );
        assert!(
            tranche.suggested_objective.contains("README.md"),
            "objective should include suggested path: {}",
            tranche.suggested_objective
        );
    }

    #[test]
    fn collect_allowed_paths_scope_suggestions_deduplicates_paths() {
        let tasks = vec![
            TaskOutcome {
                task_id: TaskId::nil(),
                task_key: "task-a".to_string(),
                state: TaskState::TaskFailed,
                attempt_no: 1,
                last_error: Some(format_allowed_paths_scope_mismatch(
                    &["README.md".to_string(), "docs/CLI.md".to_string()],
                    &["src/lib.rs".to_string()],
                )),
                blocker: None,
            },
            TaskOutcome {
                task_id: TaskId::nil(),
                task_key: "task-b".to_string(),
                state: TaskState::TaskFailed,
                attempt_no: 1,
                last_error: Some(format_allowed_paths_scope_mismatch(
                    &["README.md".to_string()],
                    &["src/lib.rs".to_string()],
                )),
                blocker: None,
            },
        ];

        assert_eq!(
            collect_allowed_paths_scope_suggestions(&tasks),
            vec!["README.md".to_string(), "docs/CLI.md".to_string()]
        );
    }

    // NXT-381: RunCompletedWithMergeFailure is distinct from RunFailed and round-trips correctly.
    #[test]
    fn run_completed_with_merge_failure_is_distinct_from_run_failed() {
        // A run that completed all tasks (RunCompleted) but whose post-completion
        // parallel-merge teardown failed should surface as RunCompletedWithMergeFailure,
        // not RunFailed. This lets tooling (yarli-loop-inspect.sh, dashboards) distinguish
        // "work undone" from "work done but teardown failed".
        let mut run = make_run();
        // Simulate: yarli commands.rs overrides the exit state after merge failure.
        run.state = RunState::RunCompletedWithMergeFailure;
        run.exit_reason = Some(ExitReason::CompletedMergeTeardownFailed);
        let mut t1 = make_task(&run, "build");
        t1.state = TaskState::TaskComplete;
        let mut t2 = make_task(&run, "test");
        t2.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1, &t2]);

        // The exit state must be the new variant — not RunFailed.
        assert_eq!(
            payload.exit_state,
            RunState::RunCompletedWithMergeFailure,
            "post-completion merge failure must not be reported as RunFailed"
        );
        assert_ne!(
            payload.exit_state,
            RunState::RunFailed,
            "RunCompletedWithMergeFailure must be distinct from RunFailed"
        );
        assert_eq!(
            payload.exit_reason,
            Some(ExitReason::CompletedMergeTeardownFailed)
        );
        // All tasks completed — summary must reflect that.
        assert_eq!(payload.summary.completed, 2);
        assert_eq!(payload.summary.failed, 0);

        // Round-trip through JSON so continuation.json consumers see the new string.
        let json = serde_json::to_string(&payload).unwrap();
        assert!(
            json.contains("RunCompletedWithMergeFailure"),
            "continuation.json must contain 'RunCompletedWithMergeFailure', got: {json}"
        );
        assert!(
            !json.contains("\"RunFailed\""),
            "continuation.json must NOT contain 'RunFailed' for this case"
        );
        let roundtrip: ContinuationPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip.exit_state, RunState::RunCompletedWithMergeFailure);
        assert_eq!(
            roundtrip.exit_reason,
            Some(ExitReason::CompletedMergeTeardownFailed)
        );
    }

    // NXT-381: is_work_done() returns true for both RunCompleted and RunCompletedWithMergeFailure.
    #[test]
    fn run_state_is_work_done_covers_both_completed_variants() {
        assert!(
            RunState::RunCompleted.is_work_done(),
            "RunCompleted must be considered work-done"
        );
        assert!(
            RunState::RunCompletedWithMergeFailure.is_work_done(),
            "RunCompletedWithMergeFailure must be considered work-done"
        );
        assert!(
            !RunState::RunFailed.is_work_done(),
            "RunFailed must NOT be considered work-done"
        );
        assert!(
            !RunState::RunCancelled.is_work_done(),
            "RunCancelled must NOT be considered work-done"
        );
    }
}
