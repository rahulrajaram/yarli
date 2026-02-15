//! Structured continuation payload emitted when a run reaches a terminal state.
//!
//! The `ContinuationPayload` captures "what changed + what remains" as
//! machine-readable JSON so downstream tooling (or `yarli run continue`)
//! can pick up where the previous run left off.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::{CancellationProvenance, CancellationSource, ExitReason, RunId, TaskId};
use crate::entities::run::Run;
use crate::entities::task::{BlockerCode, Task};
use crate::explain::DeteriorationTrend;
use crate::fsm::run::RunState;
use crate::fsm::task::TaskState;

/// Structured handoff emitted when a run reaches a terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuationPayload {
    pub run_id: RunId,
    pub objective: String,
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

/// Aggregate counts across all tasks in the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub total: u32,
    pub completed: u32,
    pub failed: u32,
    pub cancelled: u32,
    pub pending: u32,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrancheKind {
    RetryUnfinished,
    PlannedNext,
    /// All tasks completed but run-level gates failed. Re-run the same tranche
    /// so gates can be re-evaluated after the operator addresses the gate issue.
    GateRetry,
}

impl Default for TrancheKind {
    fn default() -> Self {
        Self::RetryUnfinished
    }
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

        let next_tranche = if !retry_task_keys.is_empty() || !unfinished_task_keys.is_empty() {
            let suggested_objective = if !retry_task_keys.is_empty() {
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
            })
        } else if run.state == RunState::RunFailed
            && run.exit_reason == Some(crate::domain::ExitReason::BlockedGateFailure)
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
    use crate::domain::{CommandClass, SafeMode};
    use crate::entities::run::Run;
    use crate::entities::task::Task;

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
}
