//! Merge intent entity — approved request to merge a source into a target.
//!
//! A `MergeIntent` records the merge request, strategy selection, precheck
//! results, conflict details, and approval chain. It drives the Merge FSM
//! (Section 7.4) through precheck → dry-run → apply → verify → done.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::yarli_core::domain::{CorrelationId, EventId, MergeIntentId, RunId, WorktreeId};
use crate::yarli_core::error::TransitionError;
use crate::yarli_core::fsm::merge::MergeState;

use super::transition::Transition;

/// Merge strategy (Section 12.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Explicit merge commit with no fast-forward (default).
    MergeNoFf,
    /// Rebase then fast-forward.
    RebaseThenFf,
    /// Squash merge into single commit.
    SquashMerge,
}

/// Classification of a merge conflict (Section 12.9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictType {
    /// Text conflict (overlapping edits).
    Text,
    /// Rename/rename conflict.
    RenameRename,
    /// Delete/modify conflict.
    DeleteModify,
    /// Submodule pointer conflict.
    SubmodulePointer,
    /// Binary file conflict.
    Binary,
}

/// A single conflict record within a merge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictRecord {
    /// Path of the conflicting file.
    pub path: String,
    /// Type of conflict.
    pub conflict_type: ConflictType,
    /// Number of conflict markers (for text conflicts).
    pub marker_count: Option<u32>,
}

/// An approved merge request (Section 12.5-12.9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeIntent {
    /// Unique merge intent ID (UUIDv7).
    pub id: MergeIntentId,
    /// The run requesting this merge.
    pub run_id: RunId,
    /// The worktree binding where the merge executes.
    pub worktree_id: WorktreeId,
    /// Current FSM state.
    pub state: MergeState,
    /// Source branch or ref to merge from.
    pub source_ref: String,
    /// Target branch or ref to merge into.
    pub target_ref: String,
    /// Source commit SHA at precheck time.
    pub source_sha: Option<String>,
    /// Target commit SHA at precheck time (must match at apply time).
    pub target_sha: Option<String>,
    /// Merge strategy to use.
    pub strategy: MergeStrategy,
    /// Resulting merge commit SHA (set after successful apply).
    pub result_sha: Option<String>,
    /// Conflicts detected during dry-run or apply.
    pub conflicts: Vec<ConflictRecord>,
    /// Policy approval token ID (if required).
    pub approval_token_id: Option<String>,
    /// Optional custom commit message template. If `None`, uses the default
    /// conventional-commits template from `MERGE_COMMIT_TEMPLATE`.
    /// Supports placeholders: `{source}`, `{target}`, `{run_id}`, `{task_id}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_message_template: Option<String>,
    /// Correlation ID (inherited from parent run).
    pub correlation_id: CorrelationId,
    /// When the merge intent was created.
    pub created_at: DateTime<Utc>,
    /// When the merge intent last changed state.
    pub updated_at: DateTime<Utc>,
}

impl MergeIntent {
    /// Create a new merge intent in `MergeRequested` state.
    pub fn new(
        run_id: RunId,
        worktree_id: WorktreeId,
        source_ref: impl Into<String>,
        target_ref: impl Into<String>,
        correlation_id: CorrelationId,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::now_v7(),
            run_id,
            worktree_id,
            state: MergeState::MergeRequested,
            source_ref: source_ref.into(),
            target_ref: target_ref.into(),
            source_sha: None,
            target_sha: None,
            strategy: MergeStrategy::MergeNoFf,
            result_sha: None,
            conflicts: Vec::new(),
            approval_token_id: None,
            commit_message_template: None,
            correlation_id,
            created_at: now,
            updated_at: now,
        }
    }

    /// Set the merge strategy.
    pub fn with_strategy(mut self, strategy: MergeStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set a custom merge commit message template (overrides the default).
    pub fn with_commit_template(mut self, template: impl Into<String>) -> Self {
        self.commit_message_template = Some(template.into());
        self
    }

    /// Set the policy approval token.
    pub fn with_approval(mut self, token_id: impl Into<String>) -> Self {
        self.approval_token_id = Some(token_id.into());
        self
    }

    /// Record resolved SHAs from precheck phase.
    pub fn set_precheck_shas(
        &mut self,
        source_sha: impl Into<String>,
        target_sha: impl Into<String>,
    ) {
        self.source_sha = Some(source_sha.into());
        self.target_sha = Some(target_sha.into());
        self.updated_at = Utc::now();
    }

    /// Record conflicts detected during dry-run or apply.
    pub fn set_conflicts(&mut self, conflicts: Vec<ConflictRecord>) {
        self.conflicts = conflicts;
        self.updated_at = Utc::now();
    }

    /// Record the resulting merge commit SHA after successful apply.
    pub fn set_result_sha(&mut self, sha: impl Into<String>) {
        self.result_sha = Some(sha.into());
        self.updated_at = Utc::now();
    }

    /// Check whether the target ref has changed since precheck.
    /// Section 12.8: target ref must be unchanged from precheck SHA.
    pub fn target_ref_stale(&self, current_target_sha: &str) -> bool {
        match &self.target_sha {
            Some(expected) => expected != current_target_sha,
            None => false, // no precheck yet, not stale
        }
    }

    /// Whether this merge has unresolved conflicts.
    pub fn has_conflicts(&self) -> bool {
        !self.conflicts.is_empty()
    }

    /// Attempt a state transition. Returns a `Transition` event on success.
    ///
    /// Enforces Section 7.4 rules:
    /// - Terminal states (MergeDone, MergeAborted) are immutable.
    /// - No direct transition from MergeRequested to MergeApply.
    /// - Only valid transitions are allowed.
    pub fn transition(
        &mut self,
        to: MergeState,
        reason: impl Into<String>,
        actor: impl Into<String>,
        causation_id: Option<EventId>,
    ) -> Result<Transition, TransitionError> {
        let from = self.state;

        if from.is_terminal() {
            return Err(TransitionError::TerminalState(format!("{from:?}")));
        }

        if !from.can_transition_to(to) {
            return Err(TransitionError::InvalidMergeTransition { from, to });
        }

        let reason_str = reason.into();
        let actor_str = actor.into();

        self.state = to;
        self.updated_at = Utc::now();

        Ok(Transition::new(
            "merge",
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
