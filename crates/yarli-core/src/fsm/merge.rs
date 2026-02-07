//! Merge state machine (Section 7.4).
//!
//! Rules:
//! - Merge flow must pass precheck and dry-run before apply.
//! - MERGE_DONE requires verify gates (tests + repository consistency).
//! - No direct transition from MERGE_REQUESTED to MERGE_APPLY.

use serde::{Deserialize, Serialize};

/// Merge lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MergeState {
    MergeRequested,
    MergePrecheck,
    MergeDryRun,
    MergeApply,
    MergeVerify,
    MergeDone,
    MergeConflict,
    MergeAborted,
}

impl MergeState {
    /// Terminal states cannot transition further.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            MergeState::MergeDone | MergeState::MergeAborted
        )
    }

    /// Returns the set of states reachable from this state.
    ///
    /// Note: No direct transition from MERGE_REQUESTED to MERGE_APPLY.
    pub fn valid_transitions(self) -> &'static [MergeState] {
        use MergeState::*;
        match self {
            MergeRequested => &[MergePrecheck, MergeAborted],
            MergePrecheck => &[MergeDryRun, MergeAborted],
            MergeDryRun => &[MergeApply, MergeConflict, MergeAborted],
            MergeApply => &[MergeVerify, MergeConflict, MergeAborted],
            MergeVerify => &[MergeDone, MergeAborted],
            MergeDone => &[],
            MergeConflict => &[MergeAborted, MergePrecheck], // restart from precheck after resolution
            MergeAborted => &[],
        }
    }

    /// Check if transitioning to `target` is valid.
    pub fn can_transition_to(self, target: MergeState) -> bool {
        self.valid_transitions().contains(&target)
    }
}
