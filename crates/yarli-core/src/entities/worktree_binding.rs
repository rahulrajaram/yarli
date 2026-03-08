//! Worktree binding entity — explicit worktree state and ownership metadata.
//!
//! A `WorktreeBinding` tracks the lifecycle of a Git worktree created for
//! a run/task. It records the worktree path, branch, base/head refs, dirty
//! status, and submodule state (Sections 7.3, 12.1).

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::yarli_core::domain::{CorrelationId, EventId, RunId, TaskId, WorktreeId};
use crate::yarli_core::error::TransitionError;
use crate::yarli_core::fsm::worktree::WorktreeState;

use super::transition::Transition;

/// Submodule policy mode (Section 12.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmoduleMode {
    /// Submodule SHAs may not change.
    Locked,
    /// Only fast-forward updates to pinned branch allowed.
    AllowFastForward,
    /// Unrestricted (requires explicit policy approval).
    AllowAny,
}

/// A worktree binding record (Section 12.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeBinding {
    /// Unique worktree binding ID (UUIDv7).
    pub id: WorktreeId,
    /// The run this worktree belongs to.
    pub run_id: RunId,
    /// The task this worktree is bound to (if any).
    pub task_id: Option<TaskId>,
    /// Current FSM state.
    pub state: WorktreeState,
    /// Repository root path.
    pub repo_root: PathBuf,
    /// Worktree path: `${repo_root}/.yarl/worktrees/{run_id}-{task_id_short}`.
    pub worktree_path: PathBuf,
    /// Branch name: `yarl/{run_id}/{task_slug}`.
    pub branch_name: String,
    /// Base ref (commit SHA the worktree was created from).
    pub base_ref: String,
    /// Head ref (current commit SHA in the worktree).
    pub head_ref: String,
    /// Whether the worktree has uncommitted changes.
    pub dirty: bool,
    /// Hash of submodule state for change detection.
    pub submodule_state_hash: Option<String>,
    /// Submodule policy mode for this worktree.
    pub submodule_mode: SubmoduleMode,
    /// Worker that holds the lease on this worktree (if any).
    pub lease_owner: Option<String>,
    /// Correlation ID (inherited from parent run).
    pub correlation_id: CorrelationId,
    /// When the worktree binding was created.
    pub created_at: DateTime<Utc>,
    /// When the worktree last changed state.
    pub updated_at: DateTime<Utc>,
}

impl WorktreeBinding {
    /// Create a new worktree binding in `WtUnbound` state.
    pub fn new(
        run_id: RunId,
        repo_root: impl Into<PathBuf>,
        branch_name: impl Into<String>,
        base_ref: impl Into<String>,
        correlation_id: CorrelationId,
    ) -> Self {
        let now = Utc::now();
        let base = base_ref.into();
        Self {
            id: Uuid::now_v7(),
            run_id,
            task_id: None,
            state: WorktreeState::WtUnbound,
            repo_root: repo_root.into(),
            worktree_path: PathBuf::new(),
            branch_name: branch_name.into(),
            base_ref: base.clone(),
            head_ref: base,
            dirty: false,
            submodule_state_hash: None,
            submodule_mode: SubmoduleMode::Locked,
            lease_owner: None,
            correlation_id,
            created_at: now,
            updated_at: now,
        }
    }

    /// Bind this worktree to a specific task.
    pub fn with_task(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set the submodule policy mode.
    pub fn with_submodule_mode(mut self, mode: SubmoduleMode) -> Self {
        self.submodule_mode = mode;
        self
    }

    /// Set the worktree path (typically computed during creation).
    pub fn set_worktree_path(&mut self, path: impl Into<PathBuf>) {
        self.worktree_path = path.into();
    }

    /// Update the head ref after a commit or merge.
    pub fn update_head_ref(&mut self, sha: impl Into<String>) {
        self.head_ref = sha.into();
        self.updated_at = Utc::now();
    }

    /// Mark the worktree as dirty or clean.
    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
        self.updated_at = Utc::now();
    }

    /// Update the submodule state hash.
    pub fn update_submodule_hash(&mut self, hash: impl Into<String>) {
        self.submodule_state_hash = Some(hash.into());
        self.updated_at = Utc::now();
    }

    /// Set the lease owner (worker binding).
    pub fn set_lease_owner(&mut self, owner: Option<String>) {
        self.lease_owner = owner;
        self.updated_at = Utc::now();
    }

    /// Check if this worktree allows repository-mutating commands.
    /// Section 12.3: Cannot execute in WtUnbound.
    pub fn allows_mutations(&self) -> bool {
        !matches!(
            self.state,
            WorktreeState::WtUnbound | WorktreeState::WtClosed | WorktreeState::WtCleanupPending
        )
    }

    /// Attempt a state transition. Returns a `Transition` event on success.
    ///
    /// Enforces Section 7.3 rules:
    /// - Terminal states (WtClosed) are immutable.
    /// - Only valid transitions are allowed.
    pub fn transition(
        &mut self,
        to: WorktreeState,
        reason: impl Into<String>,
        actor: impl Into<String>,
        causation_id: Option<EventId>,
    ) -> Result<Transition, TransitionError> {
        let from = self.state;

        if from.is_terminal() {
            return Err(TransitionError::TerminalState(format!("{from:?}")));
        }

        if !from.can_transition_to(to) {
            return Err(TransitionError::InvalidWorktreeTransition { from, to });
        }

        let reason_str = reason.into();
        let actor_str = actor.into();

        self.state = to;
        self.updated_at = Utc::now();

        Ok(Transition::new(
            "worktree",
            self.id,
            format!("{from:?}"),
            format!("{to:?}"),
            reason_str,
            actor_str,
            self.correlation_id,
            causation_id,
        ))
    }
}
