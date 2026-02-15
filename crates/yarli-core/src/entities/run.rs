//! Run entity — an orchestration instance with objective and lifecycle.
//!
//! A `Run` is the top-level orchestration unit. It manages a set of tasks,
//! enforces gate-based completion (Section 7.1), and carries an immutable
//! config snapshot taken at creation time.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{
    CancellationProvenance, CancellationSource, CorrelationId, EventId, ExitReason, RunId,
    SafeMode, TaskId,
};
use crate::error::TransitionError;
use crate::fsm::run::RunState;

use super::transition::Transition;

/// An orchestration run instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    /// Unique run identifier (UUIDv7 for time-ordering).
    pub id: RunId,
    /// Human-readable objective describing what this run should accomplish.
    pub objective: String,
    /// Current FSM state.
    pub state: RunState,
    /// Safe mode for this run (observe, execute, restricted, breakglass).
    pub safe_mode: SafeMode,
    /// Exit reason, set when the run reaches a terminal state.
    pub exit_reason: Option<ExitReason>,
    /// Cancellation source, set when the run transitions to cancelled.
    #[serde(default)]
    pub cancellation_source: Option<CancellationSource>,
    /// Structured cancellation provenance when run transitions to cancelled.
    #[serde(default)]
    pub cancellation_provenance: Option<CancellationProvenance>,
    /// Tasks belonging to this run (task IDs).
    pub task_ids: Vec<TaskId>,
    /// Correlation ID for all events in this run.
    pub correlation_id: CorrelationId,
    /// When the run was created.
    pub created_at: DateTime<Utc>,
    /// When the run last changed state.
    pub updated_at: DateTime<Utc>,
    /// Config snapshot (opaque JSON) captured at run creation.
    pub config_snapshot: serde_json::Value,
}

impl Run {
    /// Create a new run in `RunOpen` state.
    pub fn new(objective: impl Into<String>, safe_mode: SafeMode) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::now_v7(),
            objective: objective.into(),
            state: RunState::RunOpen,
            safe_mode,
            exit_reason: None,
            cancellation_source: None,
            cancellation_provenance: None,
            task_ids: Vec::new(),
            correlation_id: Uuid::now_v7(),
            created_at: now,
            updated_at: now,
            config_snapshot: serde_json::Value::Null,
        }
    }

    /// Create a new run with a specific config snapshot.
    pub fn with_config(
        objective: impl Into<String>,
        safe_mode: SafeMode,
        config: serde_json::Value,
    ) -> Self {
        let mut run = Self::new(objective, safe_mode);
        run.config_snapshot = config;
        run
    }

    /// Register a task ID with this run.
    pub fn add_task(&mut self, task_id: TaskId) {
        if !self.task_ids.contains(&task_id) {
            self.task_ids.push(task_id);
        }
    }

    /// Attempt a state transition. Returns a `Transition` event on success.
    ///
    /// Enforces Section 7.1 rules:
    /// - Terminal states are immutable.
    /// - Only valid transitions are allowed.
    /// - Transitions to terminal states require an exit reason.
    pub fn transition(
        &mut self,
        to: RunState,
        reason: impl Into<String>,
        actor: impl Into<String>,
        causation_id: Option<EventId>,
    ) -> Result<Transition, TransitionError> {
        let from = self.state;

        if from.is_terminal() {
            return Err(TransitionError::TerminalState(format!("{from:?}")));
        }

        if !from.can_transition_to(to) {
            return Err(TransitionError::InvalidRunTransition { from, to });
        }

        let reason_str = reason.into();
        let actor_str = actor.into();

        self.state = to;
        self.updated_at = Utc::now();

        // Set exit reason when entering terminal state.
        if to.is_terminal() && self.exit_reason.is_none() {
            self.exit_reason = Some(Self::default_exit_reason(to, &reason_str));
        }

        Ok(Transition::new(
            "run",
            self.id,
            format!("{from:?}"),
            format!("{to:?}"),
            reason_str,
            actor_str,
            self.correlation_id,
            causation_id,
        ))
    }

    /// Derive a default exit reason from the target terminal state.
    fn default_exit_reason(state: RunState, reason: &str) -> ExitReason {
        match state {
            RunState::RunCompleted => ExitReason::CompletedAllGates,
            RunState::RunFailed => {
                if reason.contains("gate") {
                    ExitReason::BlockedGateFailure
                } else if reason.contains("policy") {
                    ExitReason::FailedPolicyDenial
                } else if reason.contains("timeout") || reason.contains("timed_out") {
                    ExitReason::TimedOut
                } else if reason.contains("stall") {
                    ExitReason::StalledNoProgress
                } else {
                    ExitReason::FailedRuntimeError
                }
            }
            RunState::RunCancelled => ExitReason::CancelledByOperator,
            _ => ExitReason::FailedRuntimeError,
        }
    }
}
