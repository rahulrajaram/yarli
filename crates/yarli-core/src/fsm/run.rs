//! Run state machine (Section 7.1).
//!
//! Rules:
//! - RUN_COMPLETED reachable only from RUN_VERIFYING when all required gates pass.
//! - Any unresolved required task forces RUN_BLOCKED or RUN_ACTIVE, never RUN_COMPLETED.
//! - Terminal states are immutable.

use serde::{Deserialize, Serialize};

/// Run lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunState {
    RunOpen,
    RunActive,
    RunVerifying,
    RunBlocked,
    RunFailed,
    RunCompleted,
    /// All tasks completed and gates passed, but post-completion parallel-merge teardown
    /// failed (e.g. dirty submodule content or merge conflict during finalization).
    /// The tranche work itself landed via direct git commits; only the workspace merge
    /// could not be finalized. Distinct from `RunFailed` so tooling can recognize that
    /// the underlying work is done — not undone.
    RunCompletedWithMergeFailure,
    RunCancelled,
    RunDrained,
}

impl RunState {
    /// Terminal states cannot transition further.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunState::RunCompleted
                | RunState::RunCompletedWithMergeFailure
                | RunState::RunFailed
                | RunState::RunCancelled
                | RunState::RunDrained
        )
    }

    /// Returns true when the run's underlying task work is considered done,
    /// regardless of whether teardown (merge finalization) succeeded.
    pub fn is_work_done(self) -> bool {
        matches!(
            self,
            RunState::RunCompleted | RunState::RunCompletedWithMergeFailure
        )
    }

    /// Returns the set of states reachable from this state.
    pub fn valid_transitions(self) -> &'static [RunState] {
        use RunState::*;
        match self {
            RunOpen => &[RunActive, RunCancelled],
            RunActive => &[
                RunVerifying,
                RunBlocked,
                RunFailed,
                RunCancelled,
                RunDrained,
            ],
            RunVerifying => &[
                RunCompleted,
                RunCompletedWithMergeFailure,
                RunActive,
                RunBlocked,
                RunFailed,
                RunCancelled,
                RunDrained,
            ],
            RunBlocked => &[RunActive, RunFailed, RunCancelled],
            RunFailed => &[],
            RunCompleted => &[RunCompletedWithMergeFailure],
            RunCompletedWithMergeFailure => &[],
            RunCancelled => &[],
            RunDrained => &[],
        }
    }

    /// Check if transitioning to `target` is valid.
    pub fn can_transition_to(self, target: RunState) -> bool {
        self.valid_transitions().contains(&target)
    }
}
