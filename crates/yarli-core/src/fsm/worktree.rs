//! Worktree state machine (Section 7.3).
//!
//! Rules:
//! - At most one active binding per worker lease.
//! - A run cannot execute repository-mutating commands in WT_UNBOUND.
//! - WT_MERGING requires a MergeIntent with policy approval.

use serde::{Deserialize, Serialize};

/// Worktree lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorktreeState {
    WtUnbound,
    WtCreating,
    WtBoundHome,
    WtSwitchPending,
    WtBoundNonHome,
    WtMerging,
    WtConflict,
    WtRecovering,
    WtCleanupPending,
    WtClosed,
}

impl WorktreeState {
    /// Terminal states cannot transition further.
    pub fn is_terminal(self) -> bool {
        matches!(self, WorktreeState::WtClosed)
    }

    /// Returns the set of states reachable from this state.
    pub fn valid_transitions(self) -> &'static [WorktreeState] {
        use WorktreeState::*;
        match self {
            WtUnbound => &[WtCreating],
            WtCreating => &[WtBoundHome, WtRecovering],
            WtBoundHome => &[WtSwitchPending, WtMerging, WtCleanupPending],
            WtSwitchPending => &[WtBoundNonHome, WtBoundHome, WtRecovering],
            WtBoundNonHome => &[WtSwitchPending, WtBoundHome, WtMerging, WtCleanupPending],
            WtMerging => &[WtBoundHome, WtBoundNonHome, WtConflict, WtRecovering],
            WtConflict => &[WtRecovering, WtBoundHome, WtBoundNonHome],
            WtRecovering => &[WtBoundHome, WtBoundNonHome, WtCleanupPending],
            WtCleanupPending => &[WtClosed, WtRecovering],
            WtClosed => &[],
        }
    }

    /// Check if transitioning to `target` is valid.
    pub fn can_transition_to(self, target: WorktreeState) -> bool {
        self.valid_transitions().contains(&target)
    }
}
