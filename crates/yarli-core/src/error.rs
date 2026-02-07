//! Error types for yarli-core.

use thiserror::Error;

use crate::fsm::{
    command::CommandState, merge::MergeState, run::RunState, task::TaskState,
    worktree::WorktreeState,
};

/// Errors from invalid state transitions.
#[derive(Debug, Error)]
pub enum TransitionError {
    #[error("invalid run transition: {from:?} -> {to:?}")]
    InvalidRunTransition { from: RunState, to: RunState },

    #[error("invalid task transition: {from:?} -> {to:?}")]
    InvalidTaskTransition { from: TaskState, to: TaskState },

    #[error("invalid worktree transition: {from:?} -> {to:?}")]
    InvalidWorktreeTransition {
        from: WorktreeState,
        to: WorktreeState,
    },

    #[error("invalid merge transition: {from:?} -> {to:?}")]
    InvalidMergeTransition { from: MergeState, to: MergeState },

    #[error("invalid command transition: {from:?} -> {to:?}")]
    InvalidCommandTransition {
        from: CommandState,
        to: CommandState,
    },

    #[error("terminal state {0:?} cannot transition")]
    TerminalState(String),
}
