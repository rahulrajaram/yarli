//! Stream event types — the renderer's input protocol.
//!
//! The stream renderer receives `StreamEvent`s from the scheduler/executor
//! and renders them either to the inline viewport (transient) or pushes
//! completed transitions to scrollback (permanent).

use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use yarli_core::domain::TaskId;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;

/// Events the stream renderer can consume.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A task changed state (Section 33: single structured line per transition).
    TaskTransition {
        task_id: TaskId,
        task_name: String,
        from: TaskState,
        to: TaskState,
        elapsed: Option<Duration>,
        exit_code: Option<i32>,
        detail: Option<String>,
        at: DateTime<Utc>,
    },

    /// A run changed state.
    RunTransition {
        run_id: Uuid,
        from: RunState,
        to: RunState,
        reason: Option<String>,
        at: DateTime<Utc>,
    },

    /// Live command output from an executing task.
    CommandOutput {
        task_id: TaskId,
        task_name: String,
        line: String,
    },

    /// A transient status message (e.g. "connecting to postgres...").
    /// Shows only in inline viewport, never pushed to scrollback.
    TransientStatus { message: String },

    /// A "Why Not Done?" summary update.
    ExplainUpdate { summary: String },

    /// A task's worker assignment for display.
    TaskWorker { task_id: TaskId, worker_id: String },

    /// Tick event — advance spinners and refresh viewport.
    Tick,
}

/// Snapshot of a task's current state for viewport rendering.
#[derive(Debug, Clone)]
pub struct TaskView {
    pub task_id: TaskId,
    pub name: String,
    pub state: TaskState,
    pub elapsed: Option<Duration>,
    pub last_output_line: Option<String>,
    pub blocked_by: Option<String>,
    pub worker_id: Option<String>,
}

impl TaskView {
    /// Is this task currently active (executing or waiting)?
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            TaskState::TaskExecuting | TaskState::TaskWaiting
        )
    }

    /// Is this task terminal (complete, failed, cancelled)?
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_view_active_states() {
        let mut v = TaskView {
            task_id: Uuid::new_v4(),
            name: "test".into(),
            state: TaskState::TaskExecuting,
            elapsed: None,
            last_output_line: None,
            blocked_by: None,
            worker_id: None,
        };
        assert!(v.is_active());

        v.state = TaskState::TaskWaiting;
        assert!(v.is_active());

        v.state = TaskState::TaskReady;
        assert!(!v.is_active());
    }

    #[test]
    fn task_view_terminal_states() {
        let mut v = TaskView {
            task_id: Uuid::new_v4(),
            name: "test".into(),
            state: TaskState::TaskComplete,
            elapsed: None,
            last_output_line: None,
            blocked_by: None,
            worker_id: None,
        };
        assert!(v.is_terminal());

        v.state = TaskState::TaskFailed;
        assert!(v.is_terminal());

        v.state = TaskState::TaskExecuting;
        assert!(!v.is_terminal());
    }
}
