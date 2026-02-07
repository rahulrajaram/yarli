//! Error types for yarli-git.
//!
//! Covers worktree lifecycle, merge orchestration, submodule policy,
//! recovery, and forbidden-operation errors (Sections 12.1–12.12).

use std::path::PathBuf;

use thiserror::Error;

use yarli_core::fsm::merge::MergeState;
use yarli_core::fsm::worktree::WorktreeState;

/// Top-level error type for all git-plane operations.
#[derive(Debug, Error)]
pub enum GitError {
    // ── Worktree errors (Section 12.1–12.3, 12.11) ────────────────
    /// Failed to create a worktree (Section 12.2).
    #[error("worktree creation failed: {reason}")]
    WorktreeCreation { reason: String },

    /// Worktree path already exists on disk.
    #[error("worktree path already exists: {path}")]
    WorktreePathExists { path: PathBuf },

    /// Worktree not found at expected path.
    #[error("worktree not found: {path}")]
    WorktreeNotFound { path: PathBuf },

    /// A command tried to mutate the repository from an invalid worktree state.
    /// Section 12.3: mutations denied in WtUnbound/WtClosed/WtCleanupPending.
    #[error("mutations denied in worktree state {state:?}")]
    MutationDenied { state: WorktreeState },

    /// Path traversal outside worktree root detected (Section 12.3).
    #[error("path escapes worktree root: {path} is outside {root}")]
    PathConfinementViolation { path: PathBuf, root: PathBuf },

    /// The worktree has uncommitted changes when a clean state is required.
    #[error("worktree is dirty: {path}")]
    DirtyWorktree { path: PathBuf },

    /// Detached HEAD without explicit override policy (Section 12.3).
    #[error("detached HEAD in worktree: {path}")]
    DetachedHead { path: PathBuf },

    /// The `.git` indirection file is missing or invalid (Section 12.2 step 5).
    #[error("invalid .git indirection in worktree: {path}")]
    InvalidGitIndirection { path: PathBuf },

    /// Cleanup preconditions not met (Section 12.11).
    #[error("worktree cleanup blocked: {reason}")]
    CleanupBlocked { reason: String },

    // ── Merge errors (Section 12.5–12.9) ───────────────────────────
    /// Merge precheck failed (Section 12.6).
    #[error("merge precheck failed: {reason}")]
    MergePrecheckFailed { reason: String },

    /// Target ref moved since precheck — must restart (Section 12.8 step 2).
    #[error("target ref stale: expected {expected}, found {actual}")]
    TargetRefStale { expected: String, actual: String },

    /// Merge conflicts detected (Section 12.7, 12.9).
    #[error("merge conflicts detected: {count} file(s)")]
    MergeConflict { count: usize },

    /// Could not acquire merge lock for the target branch (Section 12.8).
    #[error("merge lock unavailable for branch {branch}")]
    MergeLockUnavailable { branch: String },

    /// Merge verification failed after apply (Section 12.8 step 4).
    #[error("merge verification failed: {reason}")]
    MergeVerifyFailed { reason: String },

    /// Merge operation attempted in an invalid merge state.
    #[error("invalid merge state for operation: {state:?}")]
    InvalidMergeState { state: MergeState },

    // ── Submodule errors (Section 12.4) ────────────────────────────
    /// Uninitialized submodule detected before task start.
    #[error("uninitialized submodule: {path}")]
    UninitializedSubmodule { path: String },

    /// Dirty submodule detected before merge.
    #[error("dirty submodule: {path}")]
    DirtySubmodule { path: String },

    /// Submodule update violates the configured policy mode.
    #[error("submodule policy violation at {path}: {reason}")]
    SubmodulePolicyViolation { path: String, reason: String },

    // ── Forbidden operations (Section 12.12) ───────────────────────
    /// A forbidden git operation was attempted without policy approval.
    #[error("forbidden git operation: {operation}")]
    ForbiddenOperation { operation: ForbiddenOp },

    // ── Recovery errors (Section 12.10) ────────────────────────────
    /// An interrupted git operation was detected on recovery.
    #[error("interrupted {operation} detected in {path}")]
    InterruptedOperation {
        operation: InterruptedOp,
        path: PathBuf,
    },

    /// Recovery action failed.
    #[error("recovery failed: {reason}")]
    RecoveryFailed { reason: String },

    // ── Ref resolution ─────────────────────────────────────────────
    /// A ref (branch/tag/sha) could not be resolved.
    #[error("ref not found: {refspec}")]
    RefNotFound { refspec: String },

    /// Branch already exists when expecting to create a new one.
    #[error("branch already exists: {branch}")]
    BranchAlreadyExists { branch: String },

    // ── Underlying errors ──────────────────────────────────────────
    /// Git command returned a non-zero exit code.
    #[error("git command failed (exit {exit_code}): {stderr}")]
    CommandFailed { exit_code: i32, stderr: String },

    /// I/O error from filesystem or process operations.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// State transition error from yarli-core.
    #[error("transition error: {0}")]
    Transition(#[from] yarli_core::error::TransitionError),

    /// Exec error from yarli-exec command runner.
    #[error("exec error: {0}")]
    Exec(#[from] yarli_exec::error::ExecError),
}

/// Forbidden git operations (Section 12.12, default-deny).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForbiddenOp {
    /// `git push` without explicit push intent approved.
    Push,
    /// `git push --force` or `git push --force-with-lease`.
    ForcePush,
    /// `git tag` / release tagging.
    Tag,
    /// Branch deletion outside policy.
    BranchDelete { branch: String },
    /// `git stash clear` or other global stash operations.
    StashClear,
    /// Repository-wide destructive cleanup (e.g. `git clean -fdx`).
    DestructiveCleanup { command: String },
}

impl std::fmt::Display for ForbiddenOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ForbiddenOp::Push => write!(f, "git push"),
            ForbiddenOp::ForcePush => write!(f, "git push --force"),
            ForbiddenOp::Tag => write!(f, "git tag"),
            ForbiddenOp::BranchDelete { branch } => {
                write!(f, "git branch -D {branch}")
            }
            ForbiddenOp::StashClear => write!(f, "git stash clear"),
            ForbiddenOp::DestructiveCleanup { command } => write!(f, "{command}"),
        }
    }
}

/// Interrupted git operations that can be detected on recovery (Section 12.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptedOp {
    /// An in-progress merge (`MERGE_HEAD` exists).
    Merge,
    /// An in-progress rebase (`.git/rebase-merge` or `.git/rebase-apply` exists).
    Rebase,
    /// An in-progress cherry-pick (`CHERRY_PICK_HEAD` exists).
    CherryPick,
    /// An in-progress revert (`REVERT_HEAD` exists).
    Revert,
}

impl std::fmt::Display for InterruptedOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InterruptedOp::Merge => write!(f, "merge"),
            InterruptedOp::Rebase => write!(f, "rebase"),
            InterruptedOp::CherryPick => write!(f, "cherry-pick"),
            InterruptedOp::Revert => write!(f, "revert"),
        }
    }
}

/// Recovery policy for interrupted operations (Section 12.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Abort the interrupted operation and return to a clean state.
    Abort,
    /// Attempt to resume/continue the interrupted operation.
    Resume,
    /// Block and require manual intervention.
    ManualBlock,
}
