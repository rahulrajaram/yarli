//! Task state machine (Section 7.2).
//!
//! Rules:
//! - TASK_COMPLETE requires successful gate evaluation and required evidence.
//! - TASK_BLOCKED must carry a machine-readable blocker code.
//! - Retry transitions must increment attempt_no.

use serde::{Deserialize, Serialize};

/// Task lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    TaskOpen,
    TaskReady,
    TaskExecuting,
    TaskWaiting,
    TaskBlocked,
    TaskVerifying,
    TaskComplete,
    TaskFailed,
    TaskCancelled,
}

impl TaskState {
    /// Terminal states cannot transition further.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskState::TaskComplete | TaskState::TaskFailed | TaskState::TaskCancelled
        )
    }

    /// Returns the set of states reachable from this state.
    pub fn valid_transitions(self) -> &'static [TaskState] {
        use TaskState::*;
        match self {
            TaskOpen => &[TaskReady, TaskBlocked, TaskCancelled],
            TaskReady => &[TaskExecuting, TaskBlocked, TaskCancelled],
            TaskExecuting => &[TaskWaiting, TaskVerifying, TaskFailed, TaskCancelled],
            TaskWaiting => &[TaskExecuting, TaskBlocked, TaskFailed, TaskCancelled],
            TaskBlocked => &[TaskReady, TaskFailed, TaskCancelled],
            TaskVerifying => &[TaskComplete, TaskFailed, TaskBlocked, TaskCancelled],
            TaskComplete => &[],
            TaskFailed => &[TaskReady], // retry
            TaskCancelled => &[],
        }
    }

    /// Check if transitioning to `target` is valid.
    pub fn can_transition_to(self, target: TaskState) -> bool {
        self.valid_transitions().contains(&target)
    }
}
