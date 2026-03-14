//! "Why Not Done?" explain engine.
//!
//! Core domain function that computes structured [`ExplainResult`] from
//! run/task/gate state. Every rendering mode (stream, CLI, dashboard)
//! consumes this same structure.
//!
//! Answerable at every level:
//! - **Run level**: "2 tasks remain — task/test FAILED, task/gate-verify blocked by test"
//! - **Task level**: "gate.tests_passed FAILED — 3/47 tests failing"
//! - **Gate level**: checklist of pass/fail per gate with evidence links

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::yarli_core::domain::{RunId, TaskId};
use crate::yarli_core::entities::command_execution::{CommandResourceUsage, TokenUsage};
use crate::yarli_core::fsm::run::RunState;
use crate::yarli_core::fsm::task::TaskState;

// ---------------------------------------------------------------------------
// Gate types (Section 11.1)
// ---------------------------------------------------------------------------

/// Gate type identifiers as defined in Section 11.1 of the spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateType {
    RequiredTasksClosed,
    RequiredEvidencePresent,
    TestsPassed,
    NoUnapprovedGitOps,
    NoUnresolvedConflicts,
    WorktreeConsistent,
    PolicyClean,
}

impl GateType {
    /// Human-readable label for display.
    pub fn label(self) -> &'static str {
        match self {
            GateType::RequiredTasksClosed => "gate.required_tasks_closed",
            GateType::RequiredEvidencePresent => "gate.required_evidence_present",
            GateType::TestsPassed => "gate.tests_passed",
            GateType::NoUnapprovedGitOps => "gate.no_unapproved_git_ops",
            GateType::NoUnresolvedConflicts => "gate.no_unresolved_conflicts",
            GateType::WorktreeConsistent => "gate.worktree_consistent",
            GateType::PolicyClean => "gate.policy_clean",
        }
    }
}

// ---------------------------------------------------------------------------
// Gate status for explain input
// ---------------------------------------------------------------------------

/// Result of a single gate evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GateResult {
    /// Gate passed with optional evidence references.
    Passed { evidence_ids: Vec<Uuid> },
    /// Gate failed with a reason.
    Failed { reason: String },
    /// Gate has not been evaluated yet.
    Pending,
}

// ---------------------------------------------------------------------------
// Input types — callers build these from persisted state
// ---------------------------------------------------------------------------

/// Snapshot of a single task's state for explain computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub task_id: TaskId,
    pub name: String,
    pub state: TaskState,
    /// Task IDs this task is blocked by (dependency graph edges).
    pub blocked_by: Vec<TaskId>,
    /// Gate results for this specific task.
    pub gates: Vec<(GateType, GateResult)>,
    /// When the task last changed state.
    pub last_transition_at: Option<DateTime<Utc>>,
    /// Resource usage from the last command execution (if available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_usage: Option<CommandResourceUsage>,
    /// Token usage from the last command execution (if available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenUsage>,
    /// If the task failed due to budget breach, the failure reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_breach_reason: Option<String>,
}

/// Snapshot of a run's state for explain computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub run_id: RunId,
    pub state: RunState,
    pub tasks: Vec<TaskSnapshot>,
    /// Run-level gates (e.g. required_tasks_closed applies at run level).
    pub gates: Vec<(GateType, GateResult)>,
}

// ---------------------------------------------------------------------------
// Output types — the ExplainResult tree
// ---------------------------------------------------------------------------

/// High-level run status for display.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunStatus {
    /// All tasks complete, all gates passed.
    Done,
    /// Actively executing tasks.
    Active,
    /// Blocked by task failures or gate failures.
    Blocked,
    /// At least one task has failed.
    Failed,
    /// Run was cancelled.
    Cancelled,
    /// Run just opened, no work started.
    Pending,
}

/// Information about a blocking task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockerInfo {
    pub task_id: TaskId,
    pub task_name: String,
    pub state: TaskState,
    /// Why this task is a blocker (human-readable).
    pub reason: String,
}

/// A failed gate with details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateFailure {
    pub gate_type: GateType,
    /// Which entity (run or task) owns this gate.
    pub entity_id: String,
    /// Failure reason from the gate evaluation.
    pub reason: String,
    /// Evidence IDs that were evaluated (if any).
    pub evidence_ids: Vec<Uuid>,
}

/// A link in the blocker chain (transitive dependency path).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockerChainLink {
    pub entity_name: String,
    pub relation: BlockerRelation,
}

/// Type of blocking relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockerRelation {
    /// Blocked by another task.
    BlockedBy,
    /// Failed directly.
    Failed,
    /// Gate failure on this entity.
    GateFailed,
}

/// A suggested remediation action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuggestedAction {
    /// Human-readable description.
    pub description: String,
    /// CLI command to perform this action (if applicable).
    pub command: Option<String>,
}

/// Summary of a budget breach for operator display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetBreachSummary {
    pub task_name: String,
    pub reason: String,
}

/// Direction of sequence deterioration in a rolling window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeteriorationTrend {
    Improving,
    Stable,
    Deteriorating,
}

/// Top contributing factor to a deterioration score.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeteriorationFactor {
    pub name: String,
    pub impact: f64,
    pub detail: String,
}

/// Structured observer output for sequence deterioration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeteriorationReport {
    pub score: f64,
    pub window_size: usize,
    pub factors: Vec<DeteriorationFactor>,
    pub trend: DeteriorationTrend,
}

/// The full explain result — answer to "Why Not Done?"
///
/// Computed purely from run/task/gate state snapshots.
/// Every rendering mode (stream, CLI, dashboard) consumes this.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResult {
    pub status: RunStatus,
    pub blocking_tasks: Vec<BlockerInfo>,
    pub failed_gates: Vec<GateFailure>,
    pub blocker_chain: Vec<BlockerChainLink>,
    pub suggested_actions: Vec<SuggestedAction>,
    /// Budget breaches detected across tasks.
    pub budget_breaches: Vec<BudgetBreachSummary>,
}

// ---------------------------------------------------------------------------
// Explain engine — pure computation
// ---------------------------------------------------------------------------

/// Compute the explain result for a run.
///
/// This is the core "Why Not Done?" function. It examines the run state,
/// task states, and gate results to produce a structured explanation of
/// what's blocking completion.
pub fn explain_run(snapshot: &RunSnapshot) -> ExplainResult {
    let status = compute_run_status(snapshot);
    let blocking_tasks = find_blocking_tasks(&snapshot.tasks);
    let failed_gates = find_failed_gates(snapshot);
    let blocker_chain = compute_blocker_chain(&snapshot.tasks);
    let budget_breaches = find_budget_breaches(&snapshot.tasks);
    let suggested_actions = suggest_actions(&blocking_tasks, &failed_gates);

    ExplainResult {
        status,
        blocking_tasks,
        failed_gates,
        blocker_chain,
        suggested_actions,
        budget_breaches,
    }
}

/// Compute explain result scoped to a single task.
pub fn explain_task(task: &TaskSnapshot) -> ExplainResult {
    let status = if task.state == TaskState::TaskComplete {
        RunStatus::Done
    } else if task.state == TaskState::TaskFailed {
        RunStatus::Failed
    } else if task.state == TaskState::TaskCancelled {
        RunStatus::Cancelled
    } else if task.state == TaskState::TaskBlocked {
        RunStatus::Blocked
    } else {
        RunStatus::Active
    };

    let failed_gates: Vec<GateFailure> = task
        .gates
        .iter()
        .filter_map(|(gate_type, result)| match result {
            GateResult::Failed { reason } => Some(GateFailure {
                gate_type: *gate_type,
                entity_id: task.name.clone(),
                reason: reason.clone(),
                evidence_ids: Vec::new(),
            }),
            _ => None,
        })
        .collect();

    let mut suggested_actions = Vec::new();
    if task.state == TaskState::TaskFailed {
        suggested_actions.push(SuggestedAction {
            description: format!("Retry the failed task: {}", task.name),
            command: Some(format!("yarli task retry {}", task.name)),
        });
    }
    for failure in &failed_gates {
        suggested_actions.push(SuggestedAction {
            description: format!(
                "Fix {} failure: {}",
                failure.gate_type.label(),
                failure.reason
            ),
            command: None,
        });
    }

    let budget_breaches = if let Some(ref reason) = task.budget_breach_reason {
        vec![BudgetBreachSummary {
            task_name: task.name.clone(),
            reason: reason.clone(),
        }]
    } else {
        Vec::new()
    };

    ExplainResult {
        status,
        blocking_tasks: Vec::new(),
        failed_gates,
        blocker_chain: Vec::new(),
        suggested_actions,
        budget_breaches,
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn compute_run_status(snapshot: &RunSnapshot) -> RunStatus {
    match snapshot.state {
        RunState::RunCompleted => RunStatus::Done,
        RunState::RunCancelled => RunStatus::Cancelled,
        RunState::RunDrained => RunStatus::Cancelled,
        RunState::RunFailed => RunStatus::Failed,
        RunState::RunOpen => RunStatus::Pending,
        RunState::RunBlocked => {
            if snapshot
                .tasks
                .iter()
                .any(|t| t.state == TaskState::TaskFailed)
            {
                RunStatus::Failed
            } else {
                RunStatus::Blocked
            }
        }
        RunState::RunActive | RunState::RunVerifying => RunStatus::Active,
    }
}

fn find_blocking_tasks(tasks: &[TaskSnapshot]) -> Vec<BlockerInfo> {
    tasks
        .iter()
        .filter_map(|t| {
            let reason = match t.state {
                TaskState::TaskFailed => Some(format!("task/{} FAILED", t.name)),
                TaskState::TaskBlocked => {
                    let blockers: Vec<&str> = tasks
                        .iter()
                        .filter(|other| t.blocked_by.contains(&other.task_id))
                        .map(|other| other.name.as_str())
                        .collect();
                    if blockers.is_empty() {
                        Some(format!("task/{} BLOCKED (unknown blocker)", t.name))
                    } else {
                        Some(format!(
                            "task/{} blocked by: {}",
                            t.name,
                            blockers.join(", ")
                        ))
                    }
                }
                TaskState::TaskOpen
                | TaskState::TaskReady
                | TaskState::TaskExecuting
                | TaskState::TaskWaiting
                | TaskState::TaskVerifying => {
                    Some(format!("task/{} not yet complete ({:?})", t.name, t.state))
                }
                TaskState::TaskComplete | TaskState::TaskCancelled => None,
            };

            reason.map(|r| BlockerInfo {
                task_id: t.task_id,
                task_name: t.name.clone(),
                state: t.state,
                reason: r,
            })
        })
        .collect()
}

fn find_failed_gates(snapshot: &RunSnapshot) -> Vec<GateFailure> {
    let mut failures = Vec::new();

    // Run-level gates
    for (gate_type, result) in &snapshot.gates {
        if let GateResult::Failed { reason } = result {
            failures.push(GateFailure {
                gate_type: *gate_type,
                entity_id: snapshot.run_id.to_string(),
                reason: reason.clone(),
                evidence_ids: Vec::new(),
            });
        }
    }

    // Task-level gates
    for task in &snapshot.tasks {
        for (gate_type, result) in &task.gates {
            if let GateResult::Failed { reason } = result {
                failures.push(GateFailure {
                    gate_type: *gate_type,
                    entity_id: task.name.clone(),
                    reason: reason.clone(),
                    evidence_ids: Vec::new(),
                });
            }
        }
    }

    failures
}

/// Build the blocker chain by following blocked_by edges to root causes.
///
/// Produces a flattened path from the first blocked task to the root cause.
/// Example: gate-verify --blocked-by--> merge-prep --blocked-by--> test --FAILED-->
fn compute_blocker_chain(tasks: &[TaskSnapshot]) -> Vec<BlockerChainLink> {
    let task_map: std::collections::HashMap<TaskId, &TaskSnapshot> =
        tasks.iter().map(|t| (t.task_id, t)).collect();

    // Find blocked tasks and trace each back to root cause
    for task in tasks {
        if task.state != TaskState::TaskBlocked {
            continue;
        }

        let mut path = vec![BlockerChainLink {
            entity_name: task.name.clone(),
            relation: BlockerRelation::BlockedBy,
        }];

        let mut visited = std::collections::HashSet::new();
        visited.insert(task.task_id);
        let mut current_id = task.task_id;

        while let Some(current) = task_map.get(&current_id) {
            let next = current
                .blocked_by
                .iter()
                .find(|id| !visited.contains(id))
                .copied();

            let Some(next_id) = next else {
                break;
            };

            visited.insert(next_id);
            let Some(next_task) = task_map.get(&next_id) else {
                break;
            };

            let relation = if next_task.state == TaskState::TaskFailed {
                BlockerRelation::Failed
            } else if next_task
                .gates
                .iter()
                .any(|(_, r)| matches!(r, GateResult::Failed { .. }))
            {
                BlockerRelation::GateFailed
            } else {
                BlockerRelation::BlockedBy
            };

            path.push(BlockerChainLink {
                entity_name: next_task.name.clone(),
                relation,
            });

            if relation == BlockerRelation::Failed || relation == BlockerRelation::GateFailed {
                break; // reached root cause
            }
            current_id = next_id;
        }

        if path.len() > 1 {
            return path; // return the first complete chain found
        }
    }

    Vec::new()
}

fn find_budget_breaches(tasks: &[TaskSnapshot]) -> Vec<BudgetBreachSummary> {
    tasks
        .iter()
        .filter_map(|t| {
            t.budget_breach_reason
                .as_ref()
                .map(|reason| BudgetBreachSummary {
                    task_name: t.name.clone(),
                    reason: reason.clone(),
                })
        })
        .collect()
}

fn suggest_actions(
    blocking_tasks: &[BlockerInfo],
    failed_gates: &[GateFailure],
) -> Vec<SuggestedAction> {
    let mut actions = Vec::new();

    for blocker in blocking_tasks {
        if blocker.state == TaskState::TaskFailed {
            actions.push(SuggestedAction {
                description: format!("Fix failures and re-run: {}", blocker.task_name),
                command: Some(format!("yarli task retry {}", blocker.task_name)),
            });
        } else if blocker.state == TaskState::TaskBlocked {
            actions.push(SuggestedAction {
                description: format!("Unblock task: {}", blocker.task_name),
                command: Some(format!(
                    "yarli task unblock {} --reason \"manual override\"",
                    blocker.task_name
                )),
            });
        }
    }

    for gate in failed_gates {
        actions.push(SuggestedAction {
            description: format!(
                "Resolve {} on {}: {}",
                gate.gate_type.label(),
                gate.entity_id,
                gate.reason
            ),
            command: None,
        });
    }

    actions
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(name: &str, state: TaskState) -> TaskSnapshot {
        TaskSnapshot {
            task_id: Uuid::new_v4(),
            name: name.to_string(),
            state,
            blocked_by: Vec::new(),
            gates: Vec::new(),
            last_transition_at: None,
            resource_usage: None,
            token_usage: None,
            budget_breach_reason: None,
        }
    }

    #[test]
    fn completed_run_reports_done() {
        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunCompleted,
            tasks: vec![make_task("build", TaskState::TaskComplete)],
            gates: vec![(
                GateType::RequiredTasksClosed,
                GateResult::Passed {
                    evidence_ids: vec![],
                },
            )],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.status, RunStatus::Done);
        assert!(result.blocking_tasks.is_empty());
        assert!(result.failed_gates.is_empty());
        assert!(result.suggested_actions.is_empty());
    }

    #[test]
    fn failed_task_shows_as_blocker() {
        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunBlocked,
            tasks: vec![
                make_task("lint", TaskState::TaskComplete),
                make_task("test", TaskState::TaskFailed),
            ],
            gates: vec![],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.blocking_tasks.len(), 1);
        assert_eq!(result.blocking_tasks[0].task_name, "test");
        assert!(result.blocking_tasks[0].reason.contains("FAILED"));
        assert!(!result.suggested_actions.is_empty());
        assert!(result.suggested_actions[0]
            .command
            .as_ref()
            .unwrap()
            .contains("retry"));
    }

    #[test]
    fn blocked_task_chain() {
        let test_id = Uuid::new_v4();
        let merge_id = Uuid::new_v4();
        let gate_id = Uuid::new_v4();

        let mut test_task = make_task("test", TaskState::TaskFailed);
        test_task.task_id = test_id;

        let mut merge_task = make_task("merge-prep", TaskState::TaskBlocked);
        merge_task.task_id = merge_id;
        merge_task.blocked_by = vec![test_id];

        let mut gate_task = make_task("gate-verify", TaskState::TaskBlocked);
        gate_task.task_id = gate_id;
        gate_task.blocked_by = vec![merge_id];

        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunBlocked,
            tasks: vec![gate_task, merge_task, test_task],
            gates: vec![],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.status, RunStatus::Failed);
        assert!(result.blocker_chain.len() >= 2);
        assert_eq!(result.blocker_chain[0].entity_name, "gate-verify");
        assert_eq!(result.blocker_chain[0].relation, BlockerRelation::BlockedBy);

        let last = result.blocker_chain.last().unwrap();
        assert_eq!(last.entity_name, "test");
        assert_eq!(last.relation, BlockerRelation::Failed);
    }

    #[test]
    fn gate_failure_surfaced() {
        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunActive,
            tasks: vec![{
                let mut t = make_task("build", TaskState::TaskVerifying);
                t.gates = vec![(
                    GateType::TestsPassed,
                    GateResult::Failed {
                        reason: "3/47 tests failing".to_string(),
                    },
                )];
                t
            }],
            gates: vec![],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.failed_gates.len(), 1);
        assert_eq!(result.failed_gates[0].gate_type, GateType::TestsPassed);
        assert!(result.failed_gates[0].reason.contains("3/47"));
    }

    #[test]
    fn run_level_gate_failure() {
        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunVerifying,
            tasks: vec![make_task("build", TaskState::TaskComplete)],
            gates: vec![(
                GateType::PolicyClean,
                GateResult::Failed {
                    reason: "unapproved policy override".to_string(),
                },
            )],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.failed_gates.len(), 1);
        assert_eq!(result.failed_gates[0].gate_type, GateType::PolicyClean);
    }

    #[test]
    fn pending_run() {
        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunOpen,
            tasks: vec![make_task("init", TaskState::TaskOpen)],
            gates: vec![],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.status, RunStatus::Pending);
        assert_eq!(result.blocking_tasks.len(), 1);
    }

    #[test]
    fn explain_task_failed() {
        let mut task = make_task("test", TaskState::TaskFailed);
        task.gates = vec![(
            GateType::TestsPassed,
            GateResult::Failed {
                reason: "exit code 1".to_string(),
            },
        )];

        let result = explain_task(&task);
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.failed_gates.len(), 1);
        assert!(!result.suggested_actions.is_empty());
    }

    #[test]
    fn explain_task_complete() {
        let task = make_task("build", TaskState::TaskComplete);
        let result = explain_task(&task);
        assert_eq!(result.status, RunStatus::Done);
        assert!(result.failed_gates.is_empty());
        assert!(result.suggested_actions.is_empty());
    }

    #[test]
    fn gate_type_labels() {
        assert_eq!(
            GateType::RequiredTasksClosed.label(),
            "gate.required_tasks_closed"
        );
        assert_eq!(GateType::TestsPassed.label(), "gate.tests_passed");
        assert_eq!(GateType::PolicyClean.label(), "gate.policy_clean");
    }

    #[test]
    fn cancelled_run() {
        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunCancelled,
            tasks: vec![make_task("build", TaskState::TaskCancelled)],
            gates: vec![],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.status, RunStatus::Cancelled);
    }

    #[test]
    fn active_run_with_executing_tasks() {
        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunActive,
            tasks: vec![
                make_task("lint", TaskState::TaskComplete),
                make_task("build", TaskState::TaskExecuting),
                make_task("test", TaskState::TaskReady),
            ],
            gates: vec![],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.status, RunStatus::Active);
        assert_eq!(result.blocking_tasks.len(), 2);
    }

    #[test]
    fn blocker_chain_with_gate_failure_root() {
        let build_id = Uuid::new_v4();
        let deploy_id = Uuid::new_v4();

        let mut build_task = make_task("build", TaskState::TaskVerifying);
        build_task.task_id = build_id;
        build_task.gates = vec![(
            GateType::TestsPassed,
            GateResult::Failed {
                reason: "2 tests failing".to_string(),
            },
        )];

        let mut deploy_task = make_task("deploy", TaskState::TaskBlocked);
        deploy_task.task_id = deploy_id;
        deploy_task.blocked_by = vec![build_id];

        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunActive,
            tasks: vec![deploy_task, build_task],
            gates: vec![],
        };

        let result = explain_run(&snapshot);
        assert!(result.blocker_chain.len() >= 2);
        assert_eq!(result.blocker_chain[0].entity_name, "deploy");
        assert_eq!(result.blocker_chain[1].entity_name, "build");
        assert_eq!(
            result.blocker_chain[1].relation,
            BlockerRelation::GateFailed
        );
    }

    #[test]
    fn budget_exceeded_task_surfaces_breach_in_run_explain() {
        let mut task = make_task("compute", TaskState::TaskFailed);
        task.budget_breach_reason =
            Some("budget_exceeded: task max_task_total_tokens observed=5000 limit=1".to_string());
        task.token_usage = Some(TokenUsage {
            prompt_tokens: 2500,
            completion_tokens: 2500,
            total_tokens: 5000,
            rehydration_tokens: None,
            source: "char_count_div4_estimate_v1".to_string(),
        });

        let snapshot = RunSnapshot {
            run_id: Uuid::new_v4(),
            state: RunState::RunBlocked,
            tasks: vec![task],
            gates: vec![],
        };

        let result = explain_run(&snapshot);
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.budget_breaches.len(), 1);
        assert_eq!(result.budget_breaches[0].task_name, "compute");
        assert!(result.budget_breaches[0].reason.contains("budget_exceeded"));
    }

    #[test]
    fn budget_exceeded_task_explain_surfaces_breach() {
        let mut task = make_task("compute", TaskState::TaskFailed);
        task.budget_breach_reason =
            Some("budget_exceeded: task max_task_total_tokens observed=5000 limit=1".to_string());
        task.resource_usage = Some(CommandResourceUsage {
            max_rss_bytes: Some(1024 * 1024),
            cpu_user_ticks: Some(100),
            cpu_system_ticks: Some(50),
            io_read_bytes: Some(4096),
            io_write_bytes: Some(2048),
        });

        let result = explain_task(&task);
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.budget_breaches.len(), 1);
        assert!(result.budget_breaches[0].reason.contains("budget_exceeded"));
    }

    #[test]
    fn no_breach_when_task_succeeds() {
        let task = make_task("build", TaskState::TaskComplete);
        let result = explain_task(&task);
        assert!(result.budget_breaches.is_empty());
    }

    #[test]
    fn token_usage_accessible_on_task_snapshot() {
        let mut task = make_task("llm-call", TaskState::TaskComplete);
        task.token_usage = Some(TokenUsage {
            prompt_tokens: 1000,
            completion_tokens: 500,
            total_tokens: 1500,
            rehydration_tokens: None,
            source: "char_count_div4_estimate_v1".to_string(),
        });
        assert_eq!(task.token_usage.as_ref().unwrap().prompt_tokens, 1000);
        assert_eq!(task.token_usage.as_ref().unwrap().completion_tokens, 500);
        assert_eq!(task.token_usage.as_ref().unwrap().total_tokens, 1500);
    }

    #[test]
    fn resource_usage_accessible_on_task_snapshot() {
        let mut task = make_task("heavy-compute", TaskState::TaskComplete);
        task.resource_usage = Some(CommandResourceUsage {
            max_rss_bytes: Some(2 * 1024 * 1024 * 1024),
            cpu_user_ticks: Some(500),
            cpu_system_ticks: Some(200),
            io_read_bytes: Some(1024 * 1024),
            io_write_bytes: Some(512 * 1024),
        });
        assert_eq!(
            task.resource_usage.as_ref().unwrap().max_rss_bytes,
            Some(2 * 1024 * 1024 * 1024)
        );
    }
}
