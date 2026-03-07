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
    RunCancelled,
    RunDrained,
}

impl RunState {
    /// Terminal states cannot transition further.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunState::RunCompleted
                | RunState::RunFailed
                | RunState::RunCancelled
                | RunState::RunDrained
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
                RunActive,
                RunBlocked,
                RunFailed,
                RunCancelled,
                RunDrained,
            ],
            RunBlocked => &[RunActive, RunFailed, RunCancelled],
            RunFailed => &[],
            RunCompleted => &[],
            RunCancelled => &[],
            RunDrained => &[],
        }
    }

    /// Check if transitioning to `target` is valid.
    pub fn can_transition_to(self, target: RunState) -> bool {
        self.valid_transitions().contains(&target)
    }
}
