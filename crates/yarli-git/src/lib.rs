//! yarli-git: Worktree, submodule, and merge orchestration.

pub mod commit_message;
pub mod constants;
pub mod error;
pub mod merge;
pub mod submodule;
pub mod worktree;

pub use commit_message::{
    generate_commit_message, render_commit_message, DiffSpec, GeneratedCommitMessage,
};
pub use merge::{LocalMergeOrchestrator, MergeOrchestrator};
pub use submodule::{SubmoduleEntry, SubmoduleStatus};
pub use worktree::{GitCommandOutput, LocalWorktreeManager, WorktreeManager};

#[cfg(test)]
mod tests;
