//! Task entity — smallest schedulable unit with dependencies and evidence.
//!
//! A `Task` belongs to a `Run`, tracks its own lifecycle via the Task FSM
//! (Section 7.2), carries dependency edges, evidence requirements, and
//! an attempt counter for retry semantics.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::yarli_core::domain::{CommandClass, CorrelationId, EventId, RunId, TaskId};
use crate::yarli_core::error::TransitionError;
use crate::yarli_core::fsm::task::TaskState;

use super::transition::Transition;

/// A blocker code explaining why a task is in `TaskBlocked`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockerCode {
    /// Blocked by another task that hasn't completed.
    DependencyPending,
    /// Blocked by a merge conflict.
    MergeConflict,
    /// Blocked by a policy denial.
    PolicyDenial,
    /// Blocked by a gate failure.
    GateFailure,
    /// Blocked by manual hold from operator.
    ManualHold,
    /// Custom blocker with description.
    Custom(String),
}

/// A task within a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique task identifier (UUIDv7 for time-ordering).
    pub id: TaskId,
    /// The run this task belongs to.
    pub run_id: RunId,
    /// Human-readable key for deduplication (unique per run).
    pub task_key: String,
    /// Description of what this task should accomplish.
    pub description: String,
    /// Current FSM state.
    pub state: TaskState,
    /// Command class for concurrency caps.
    pub command_class: CommandClass,
    /// Tasks this task depends on (must be `TaskComplete` before this can run).
    pub depends_on: Vec<TaskId>,
    /// Evidence IDs attached to this task.
    pub evidence_ids: Vec<Uuid>,
    /// Current attempt number (starts at 1, incremented on retry).
    pub attempt_no: u32,
    /// Maximum allowed attempts before permanent failure.
    pub max_attempts: u32,
    /// Blocker code when in `TaskBlocked` state.
    pub blocker: Option<BlockerCode>,
    /// Root-cause error message preserved across state changes.
    /// Set on first failure; not overwritten by subsequent kills or timeouts.
    pub last_error: Option<String>,
    /// Free-form annotation for blocker context (e.g. "see blocker-001.md").
    /// Set by external agents or the `task annotate` CLI command.
    pub blocker_detail: Option<String>,
    /// Correlation ID (inherited from parent run).
    pub correlation_id: CorrelationId,
    /// When the task was created.
    pub created_at: DateTime<Utc>,
    /// When the task last changed state.
    pub updated_at: DateTime<Utc>,
    /// Priority for queue ordering (0-100, higher is more urgent).
    pub priority: u32,
}

fn clamp_task_priority(priority: u32) -> u32 {
    priority.min(100)
}

impl Task {
    /// Create a new task in `TaskOpen` state.
    pub fn new(
        run_id: RunId,
        task_key: impl Into<String>,
        description: impl Into<String>,
        command_class: CommandClass,
        correlation_id: CorrelationId,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::now_v7(),
            run_id,
            task_key: task_key.into(),
            description: description.into(),
            state: TaskState::TaskOpen,
            command_class,
            depends_on: Vec::new(),
            evidence_ids: Vec::new(),
            attempt_no: 1,
            max_attempts: 3,
            blocker: None,
            last_error: None,
            blocker_detail: None,
            correlation_id,
            created_at: now,
            updated_at: now,
            priority: 3,
        }
    }

    /// Add a dependency on another task.
    pub fn depends_on(&mut self, task_id: TaskId) {
        if !self.depends_on.contains(&task_id) {
            self.depends_on.push(task_id);
        }
    }

    /// Set the task priority (0-100, higher is more urgent).
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = clamp_task_priority(priority);
        self
    }

    /// Set the maximum number of attempts.
    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = max;
        self
    }

    /// Set the root-cause error message. Only sets if not already populated,
    /// preserving the original failure reason across subsequent state changes.
    pub fn set_last_error(&mut self, error: impl Into<String>) {
        if self.last_error.is_none() {
            self.last_error = Some(error.into());
        }
    }

    /// Set the blocker detail annotation.
    pub fn set_blocker_detail(&mut self, detail: impl Into<String>) {
        self.blocker_detail = Some(detail.into());
    }

    /// Clear the blocker detail annotation.
    pub fn clear_blocker_detail(&mut self) {
        self.blocker_detail = None;
    }

    /// Attach evidence to this task.
    pub fn attach_evidence(&mut self, evidence_id: Uuid) {
        if !self.evidence_ids.contains(&evidence_id) {
            self.evidence_ids.push(evidence_id);
        }
    }

    /// Attempt a state transition. Returns a `Transition` event on success.
    ///
    /// Enforces Section 7.2 rules:
    /// - Terminal states are immutable.
    /// - Only valid transitions are allowed.
    /// - `TaskBlocked` requires a blocker code.
    /// - Retry from `TaskFailed` to `TaskReady` increments attempt_no.
    pub fn transition(
        &mut self,
        to: TaskState,
        reason: impl Into<String>,
        actor: impl Into<String>,
        causation_id: Option<EventId>,
    ) -> Result<Transition, TransitionError> {
        let from = self.state;

        if from.is_terminal() {
            // Allow retry: TaskFailed -> TaskReady
            if from == TaskState::TaskFailed && to == TaskState::TaskReady {
                if self.attempt_no >= self.max_attempts {
                    return Err(TransitionError::TerminalState(format!(
                        "{from:?} (max attempts {} reached)",
                        self.max_attempts
                    )));
                }
                // Retry: increment attempt_no
                self.attempt_no += 1;
            } else {
                return Err(TransitionError::TerminalState(format!("{from:?}")));
            }
        }

        if !from.can_transition_to(to) {
            return Err(TransitionError::InvalidTaskTransition { from, to });
        }

        let reason_str = reason.into();
        let actor_str = actor.into();

        self.state = to;
        self.updated_at = Utc::now();

        // Clear blocker when leaving blocked state.
        if from == TaskState::TaskBlocked && to != TaskState::TaskBlocked {
            self.blocker = None;
        }

        Ok(Transition::new(
            "task",
            self.id,
            format!("{from:?}"),
            format!("{to:?}"),
            reason_str,
            actor_str,
            self.correlation_id,
            causation_id,
        ))
    }

    /// Transition to `TaskBlocked` with a mandatory blocker code.
    pub fn block(
        &mut self,
        blocker: BlockerCode,
        reason: impl Into<String>,
        actor: impl Into<String>,
        causation_id: Option<EventId>,
    ) -> Result<Transition, TransitionError> {
        self.blocker = Some(blocker);
        self.transition(TaskState::TaskBlocked, reason, actor, causation_id)
    }

    /// Check if all dependencies are satisfied given a predicate.
    pub fn dependencies_satisfied<F>(&self, is_complete: F) -> bool
    where
        F: Fn(&TaskId) -> bool,
    {
        self.depends_on.iter().all(is_complete)
    }
}
