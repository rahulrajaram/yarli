//! Merge orchestration (Sections 12.5–12.9).
//!
//! [`MergeOrchestrator`] drives a [`MergeIntent`] through the merge FSM:
//! precheck → dry-run → apply → verify → done. [`LocalMergeOrchestrator`]
//! implements it using git CLI via [`WorktreeManager`].

use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use yarli_core::entities::merge_intent::{
    ConflictRecord, ConflictType, MergeIntent, MergeStrategy,
};
use yarli_core::entities::worktree_binding::WorktreeBinding;
use yarli_core::fsm::merge::MergeState;
use yarli_core::fsm::worktree::WorktreeState;

use crate::constants::*;
use crate::error::GitError;
use crate::submodule::SubmoduleEntry;
use crate::worktree::{LocalWorktreeManager, WorktreeManager};

/// Result of a merge precheck (Section 12.6).
#[derive(Debug)]
pub struct PrecheckResult {
    /// Resolved source SHA.
    pub source_sha: String,
    /// Resolved target SHA.
    pub target_sha: String,
    /// Submodule status snapshot captured during precheck (Section 12.4).
    pub submodule_snapshot: Vec<SubmoduleEntry>,
}

/// Result of a dry-run merge (Section 12.7).
#[derive(Debug)]
pub struct DryRunResult {
    /// Whether the dry-run succeeded without conflicts.
    pub clean: bool,
    /// Conflicts found during dry-run (empty if clean).
    pub conflicts: Vec<ConflictRecord>,
    /// Changed files summary (for audit evidence).
    pub changed_files: Vec<String>,
}

/// Result of an applied merge (Section 12.8).
#[derive(Debug)]
pub struct ApplyResult {
    /// The merge commit SHA.
    pub merge_sha: String,
    /// Files changed by the merge.
    pub changed_files: Vec<String>,
}

/// Trait for orchestrating merges through the merge FSM.
///
/// All operations respect cancellation via [`CancellationToken`].
/// The orchestrator drives the [`MergeIntent`] through its state machine.
#[allow(async_fn_in_trait)]
pub trait MergeOrchestrator: Send + Sync {
    /// Precheck: resolve refs, verify conditions (Section 12.6).
    ///
    /// Transitions: MergeRequested → MergePrecheck.
    /// On success, records resolved SHAs on the intent.
    async fn precheck(
        &self,
        intent: &mut MergeIntent,
        binding: &WorktreeBinding,
        cancel: CancellationToken,
    ) -> Result<PrecheckResult, GitError>;

    /// Dry-run: attempt merge without committing (Section 12.7).
    ///
    /// Transitions: MergePrecheck → MergeDryRun.
    /// On conflict: MergeDryRun → MergeConflict.
    async fn dry_run(
        &self,
        intent: &mut MergeIntent,
        binding: &WorktreeBinding,
        cancel: CancellationToken,
    ) -> Result<DryRunResult, GitError>;

    /// Apply: perform the actual merge in the worktree (Section 12.8).
    ///
    /// Transitions: MergeDryRun → MergeApply.
    /// On conflict: MergeApply → MergeConflict.
    async fn apply(
        &self,
        intent: &mut MergeIntent,
        binding: &mut WorktreeBinding,
        cancel: CancellationToken,
    ) -> Result<ApplyResult, GitError>;

    /// Verify: check merge integrity (Section 12.8 step 4).
    ///
    /// Transitions: MergeApply → MergeVerify → MergeDone.
    /// Includes submodule policy verification (Section 12.4).
    /// The `before_submodules` snapshot should come from [`PrecheckResult::submodule_snapshot`].
    async fn verify(
        &self,
        intent: &mut MergeIntent,
        binding: &WorktreeBinding,
        before_submodules: &[SubmoduleEntry],
        cancel: CancellationToken,
    ) -> Result<(), GitError>;

    /// Abort: cancel a merge at any non-terminal state.
    ///
    /// Transitions: any non-terminal → MergeAborted.
    async fn abort(
        &self,
        intent: &mut MergeIntent,
        binding: &mut WorktreeBinding,
        reason: &str,
        cancel: CancellationToken,
    ) -> Result<(), GitError>;
}

/// Simple in-process merge lock keyed by branch name.
///
/// Section 12.8: acquire merge lock for target branch to prevent concurrent merges.
#[derive(Debug, Default)]
pub struct MergeLockMap {
    locks: tokio::sync::Mutex<std::collections::HashSet<String>>,
}

impl MergeLockMap {
    pub fn new() -> Self {
        Self {
            locks: tokio::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Try to acquire the lock for a branch. Returns true if acquired.
    pub async fn try_acquire(&self, branch: &str) -> bool {
        let mut set = self.locks.lock().await;
        set.insert(branch.to_string())
    }

    /// Release the lock for a branch.
    pub async fn release(&self, branch: &str) {
        let mut set = self.locks.lock().await;
        set.remove(branch);
    }
}

/// Local merge orchestrator using git CLI via [`LocalWorktreeManager`].
#[derive(Debug)]
pub struct LocalMergeOrchestrator {
    /// Underlying worktree manager for git operations.
    wt_manager: LocalWorktreeManager,
    /// Per-branch merge locks (Section 12.8).
    pub(crate) lock_map: Arc<MergeLockMap>,
}

impl LocalMergeOrchestrator {
    pub fn new(wt_manager: LocalWorktreeManager) -> Self {
        Self {
            wt_manager,
            lock_map: Arc::new(MergeLockMap::new()),
        }
    }

    pub fn with_lock_map(mut self, lock_map: Arc<MergeLockMap>) -> Self {
        self.lock_map = lock_map;
        self
    }

    /// Run a git command in a directory, returning output.
    async fn run_git(
        &self,
        cwd: &Path,
        args: &[&str],
        cancel: &CancellationToken,
    ) -> Result<crate::worktree::GitCommandOutput, GitError> {
        self.wt_manager.run_git(cwd, args, cancel).await
    }

    /// Parse `git diff --name-only` output into a file list.
    pub(crate) fn parse_changed_files(stdout: &str) -> Vec<String> {
        stdout
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect()
    }

    /// Parse conflict markers from `git diff --check` or `git status` output.
    pub(crate) fn parse_conflicts(stdout: &str) -> Vec<ConflictRecord> {
        // Parse `git diff --name-only --diff-filter=U` for conflicting files.
        // Each line is a path to a conflicting file.
        stdout
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .map(|path| ConflictRecord {
                path,
                conflict_type: ConflictType::Text,
                marker_count: None,
            })
            .collect()
    }

    /// Inner apply logic, called with lock held. Lock release handled by caller.
    async fn apply_inner(
        &self,
        intent: &mut MergeIntent,
        binding: &mut WorktreeBinding,
        cancel: &CancellationToken,
    ) -> Result<ApplyResult, GitError> {
        let repo_root = binding.repo_root.clone();
        let wt_path = binding.worktree_path.clone();

        // Transition to MergeApply.
        intent
            .transition(
                MergeState::MergeApply,
                "starting merge apply",
                "merge_orchestrator",
                None,
            )
            .map_err(GitError::Transition)?;

        // Transition worktree to WtMerging.
        binding
            .transition(
                WorktreeState::WtMerging,
                "merge apply started",
                "merge_orchestrator",
                None,
            )
            .map_err(GitError::Transition)?;

        // Step 2: Ensure target ref unchanged from precheck SHA (Section 12.8 step 2).
        let current_target_sha = self
            .wt_manager
            .resolve_ref(&repo_root, &intent.target_ref)
            .await?;

        if intent.target_ref_stale(&current_target_sha) {
            let expected = intent.target_sha.clone().unwrap_or_default();
            // Transition worktree back to bound.
            binding
                .transition(
                    WorktreeState::WtBoundHome,
                    "merge aborted: target ref stale",
                    "merge_orchestrator",
                    None,
                )
                .map_err(GitError::Transition)?;

            return Err(GitError::TargetRefStale {
                expected,
                actual: current_target_sha,
            });
        }

        let source_sha = intent
            .source_sha
            .as_ref()
            .ok_or_else(|| GitError::InvalidMergeState {
                state: intent.state,
            })?
            .clone();

        // Step 3: Perform the merge.
        let commit_msg = Self::build_commit_message(intent);
        let merge_result = match intent.strategy {
            MergeStrategy::MergeNoFf => {
                self.run_git(
                    &wt_path,
                    &["merge", "--no-ff", "-m", &commit_msg, &source_sha],
                    cancel,
                )
                .await?
            }
            MergeStrategy::SquashMerge => {
                // Squash merge: stage then commit separately.
                let squash = self
                    .run_git(&wt_path, &["merge", "--squash", &source_sha], cancel)
                    .await?;
                if squash.exit_code != 0 {
                    squash
                } else {
                    self.run_git(&wt_path, &["commit", "-m", &commit_msg], cancel)
                        .await?
                }
            }
            MergeStrategy::RebaseThenFf => {
                // Rebase source onto target, then fast-forward merge.
                let rebase = self
                    .run_git(&wt_path, &["rebase", &intent.target_ref], cancel)
                    .await?;
                if rebase.exit_code != 0 {
                    // Abort the rebase.
                    let _ = self.run_git(&wt_path, &["rebase", "--abort"], cancel).await;
                    rebase
                } else {
                    self.run_git(&wt_path, &["merge", "--ff-only", &source_sha], cancel)
                        .await?
                }
            }
        };

        // Check for conflicts during apply.
        if merge_result.exit_code != 0 {
            // Collect conflicting files.
            let conflict_output = self
                .run_git(
                    &wt_path,
                    &["diff", "--name-only", "--diff-filter=U"],
                    cancel,
                )
                .await?;
            let conflicts = Self::parse_conflicts(&conflict_output.stdout);

            // Abort the merge.
            let _ = self.run_git(&wt_path, &["merge", "--abort"], cancel).await;

            let count = conflicts.len();
            intent.set_conflicts(conflicts);

            // Transition to MergeConflict.
            intent
                .transition(
                    MergeState::MergeConflict,
                    format!("{count} conflict(s) detected during apply"),
                    "merge_orchestrator",
                    None,
                )
                .map_err(GitError::Transition)?;

            // Transition worktree to WtConflict.
            binding
                .transition(
                    WorktreeState::WtConflict,
                    "merge conflict during apply",
                    "merge_orchestrator",
                    None,
                )
                .map_err(GitError::Transition)?;

            warn!(conflicts = count, "merge apply detected conflicts");

            return Err(GitError::MergeConflict { count });
        }

        // Step 4: Collect results.
        let merge_sha = self.wt_manager.resolve_head(&wt_path).await?;
        intent.set_result_sha(&merge_sha);

        // Collect changed files.
        let diff_output = self
            .run_git(&wt_path, &["diff", "--name-only", "HEAD~1..HEAD"], cancel)
            .await?;
        let changed_files = Self::parse_changed_files(&diff_output.stdout);

        // Update binding head ref.
        binding.update_head_ref(&merge_sha);

        // Transition worktree back to WtBoundHome after successful merge.
        binding
            .transition(
                WorktreeState::WtBoundHome,
                "merge apply succeeded",
                "merge_orchestrator",
                None,
            )
            .map_err(GitError::Transition)?;

        info!(
            merge_sha = %merge_sha,
            changed_files = changed_files.len(),
            strategy = ?intent.strategy,
            "merge applied"
        );

        Ok(ApplyResult {
            merge_sha,
            changed_files,
        })
    }

    /// Build the merge commit message from the template.
    pub(crate) fn build_commit_message(intent: &MergeIntent) -> String {
        MERGE_COMMIT_TEMPLATE
            .replace("{source}", &intent.source_ref)
            .replace("{target}", &intent.target_ref)
            .replace("{run_id}", &intent.run_id.to_string())
            .replace("{task_id}", &intent.worktree_id.to_string())
    }
}

impl MergeOrchestrator for LocalMergeOrchestrator {
    async fn precheck(
        &self,
        intent: &mut MergeIntent,
        binding: &WorktreeBinding,
        _cancel: CancellationToken,
    ) -> Result<PrecheckResult, GitError> {
        // Validate current state.
        if intent.state != MergeState::MergeRequested && intent.state != MergeState::MergeConflict {
            return Err(GitError::InvalidMergeState {
                state: intent.state,
            });
        }

        let repo_root = &binding.repo_root;
        let wt_path = &binding.worktree_path;

        // Transition to MergePrecheck.
        intent
            .transition(
                MergeState::MergePrecheck,
                "starting merge precheck",
                "merge_orchestrator",
                None,
            )
            .map_err(GitError::Transition)?;

        // Step 1: Confirm source and target refs exist.
        let source_sha = self
            .wt_manager
            .resolve_ref(repo_root, &intent.source_ref)
            .await
            .map_err(|_| GitError::MergePrecheckFailed {
                reason: format!("source ref '{}' not found", intent.source_ref),
            })?;

        let target_sha = self
            .wt_manager
            .resolve_ref(repo_root, &intent.target_ref)
            .await
            .map_err(|_| GitError::MergePrecheckFailed {
                reason: format!("target ref '{}' not found", intent.target_ref),
            })?;

        // Step 2: Verify no unresolved worktree conflicts.
        let interrupted = self.wt_manager.detect_interrupted(wt_path).await?;
        if let Some(op) = interrupted {
            return Err(GitError::MergePrecheckFailed {
                reason: format!("worktree has interrupted {op}"),
            });
        }

        // Step 3: Verify worktree is not dirty.
        if self.wt_manager.is_dirty(wt_path).await? {
            return Err(GitError::MergePrecheckFailed {
                reason: "worktree has uncommitted changes".into(),
            });
        }

        // Step 4: Check for dirty submodules (Section 12.4).
        let dirty_subs = self.wt_manager.check_dirty_submodules(wt_path).await?;
        if !dirty_subs.is_empty() {
            return Err(GitError::DirtySubmodule {
                path: dirty_subs.join(", "),
            });
        }

        // Step 5: Capture submodule status snapshot for policy verification.
        let submodule_snapshot = self.wt_manager.submodule_status(wt_path).await?;

        // Record resolved SHAs.
        intent.set_precheck_shas(&source_sha, &target_sha);

        info!(
            source_ref = %intent.source_ref,
            target_ref = %intent.target_ref,
            source_sha = %source_sha,
            target_sha = %target_sha,
            submodules = submodule_snapshot.len(),
            "merge precheck passed"
        );

        Ok(PrecheckResult {
            source_sha,
            target_sha,
            submodule_snapshot,
        })
    }

    async fn dry_run(
        &self,
        intent: &mut MergeIntent,
        binding: &WorktreeBinding,
        cancel: CancellationToken,
    ) -> Result<DryRunResult, GitError> {
        // Must be in MergePrecheck state.
        if intent.state != MergeState::MergePrecheck {
            return Err(GitError::InvalidMergeState {
                state: intent.state,
            });
        }

        let wt_path = &binding.worktree_path;

        // Transition to MergeDryRun.
        intent
            .transition(
                MergeState::MergeDryRun,
                "starting merge dry-run",
                "merge_orchestrator",
                None,
            )
            .map_err(GitError::Transition)?;

        let source_sha = intent
            .source_sha
            .as_ref()
            .ok_or_else(|| GitError::InvalidMergeState {
                state: intent.state,
            })?
            .clone();

        // Section 12.7: attempt merge without committing.
        let merge_result = match intent.strategy {
            MergeStrategy::MergeNoFf => {
                self.run_git(
                    wt_path,
                    &["merge", "--no-ff", "--no-commit", &source_sha],
                    &cancel,
                )
                .await?
            }
            MergeStrategy::SquashMerge => {
                self.run_git(
                    wt_path,
                    &["merge", "--squash", "--no-commit", &source_sha],
                    &cancel,
                )
                .await?
            }
            MergeStrategy::RebaseThenFf => {
                // For dry-run of rebase, do a regular merge check — actual rebase
                // happens in apply phase.
                self.run_git(
                    wt_path,
                    &["merge", "--no-ff", "--no-commit", &source_sha],
                    &cancel,
                )
                .await?
            }
        };

        // Collect changed files from the staged diff.
        let diff_output = self
            .run_git(wt_path, &["diff", "--cached", "--name-only"], &cancel)
            .await?;
        let changed_files = Self::parse_changed_files(&diff_output.stdout);

        // Check for conflicts.
        if merge_result.exit_code != 0 {
            // Merge had conflicts. Collect conflicting files.
            let conflict_output = self
                .run_git(
                    wt_path,
                    &["diff", "--name-only", "--diff-filter=U"],
                    &cancel,
                )
                .await?;
            let conflicts = Self::parse_conflicts(&conflict_output.stdout);

            // Abort the failed merge to return to clean state.
            let _ = self.run_git(wt_path, &["merge", "--abort"], &cancel).await;

            let count = conflicts.len();
            intent.set_conflicts(conflicts.clone());

            // Transition to MergeConflict.
            intent
                .transition(
                    MergeState::MergeConflict,
                    format!("{count} conflict(s) detected in dry-run"),
                    "merge_orchestrator",
                    None,
                )
                .map_err(GitError::Transition)?;

            warn!(
                conflicts = count,
                source = %intent.source_ref,
                target = %intent.target_ref,
                "merge dry-run detected conflicts"
            );

            return Ok(DryRunResult {
                clean: false,
                conflicts,
                changed_files,
            });
        }

        // Dry-run succeeded — abort the uncommitted merge to return to clean state.
        let _ = self.run_git(wt_path, &["merge", "--abort"], &cancel).await;

        info!(
            changed_files = changed_files.len(),
            source = %intent.source_ref,
            target = %intent.target_ref,
            "merge dry-run succeeded"
        );

        Ok(DryRunResult {
            clean: true,
            conflicts: Vec::new(),
            changed_files,
        })
    }

    async fn apply(
        &self,
        intent: &mut MergeIntent,
        binding: &mut WorktreeBinding,
        cancel: CancellationToken,
    ) -> Result<ApplyResult, GitError> {
        // Must be in MergeDryRun state.
        if intent.state != MergeState::MergeDryRun {
            return Err(GitError::InvalidMergeState {
                state: intent.state,
            });
        }

        // Step 1: Acquire merge lock for target branch (Section 12.8).
        let lock_acquired = self.lock_map.try_acquire(&intent.target_ref).await;
        if !lock_acquired {
            return Err(GitError::MergeLockUnavailable {
                branch: intent.target_ref.clone(),
            });
        }

        // Delegate to inner method; release lock on all exit paths.
        let result = self.apply_inner(intent, binding, &cancel).await;
        self.lock_map.release(&intent.target_ref).await;
        result
    }

    async fn verify(
        &self,
        intent: &mut MergeIntent,
        binding: &WorktreeBinding,
        before_submodules: &[SubmoduleEntry],
        _cancel: CancellationToken,
    ) -> Result<(), GitError> {
        // Must be in MergeApply state.
        if intent.state != MergeState::MergeApply {
            return Err(GitError::InvalidMergeState {
                state: intent.state,
            });
        }

        // Transition to MergeVerify.
        intent
            .transition(
                MergeState::MergeVerify,
                "starting merge verification",
                "merge_orchestrator",
                None,
            )
            .map_err(GitError::Transition)?;

        let wt_path = &binding.worktree_path;

        // Basic integrity checks: worktree is clean and HEAD is valid.
        if self.wt_manager.is_dirty(wt_path).await? {
            return Err(GitError::MergeVerifyFailed {
                reason: "worktree is dirty after merge".into(),
            });
        }

        let head = self.wt_manager.resolve_head(wt_path).await?;
        if head.is_empty() {
            return Err(GitError::MergeVerifyFailed {
                reason: "HEAD is empty after merge".into(),
            });
        }

        // Section 12.4: Verify submodule policy.
        let after_submodules = self.wt_manager.submodule_status(wt_path).await?;
        self.wt_manager
            .verify_submodule_policy(binding.submodule_mode, before_submodules, &after_submodules)
            .map_err(|e| GitError::MergeVerifyFailed {
                reason: format!("submodule policy: {e}"),
            })?;

        // Gates deferred to M3.
        // Section 12.8 step 4: run required verification suite.
        // When M3 is implemented, this will invoke gate evaluations.

        // Transition to MergeDone.
        intent
            .transition(
                MergeState::MergeDone,
                "merge verification passed",
                "merge_orchestrator",
                None,
            )
            .map_err(GitError::Transition)?;

        info!(
            merge_id = %intent.id,
            result_sha = ?intent.result_sha,
            "merge completed"
        );

        Ok(())
    }

    async fn abort(
        &self,
        intent: &mut MergeIntent,
        binding: &mut WorktreeBinding,
        reason: &str,
        cancel: CancellationToken,
    ) -> Result<(), GitError> {
        if intent.state.is_terminal() {
            return Err(GitError::InvalidMergeState {
                state: intent.state,
            });
        }

        let wt_path = &binding.worktree_path;

        // If worktree is in WtMerging or WtConflict, try to abort the git merge.
        if binding.state == WorktreeState::WtMerging || binding.state == WorktreeState::WtConflict {
            let _ = self.run_git(wt_path, &["merge", "--abort"], &cancel).await;

            // Transition worktree through WtRecovering to WtBoundHome.
            binding
                .transition(
                    WorktreeState::WtRecovering,
                    format!("merge aborted: {reason}"),
                    "merge_orchestrator",
                    None,
                )
                .map_err(GitError::Transition)?;

            binding
                .transition(
                    WorktreeState::WtBoundHome,
                    "recovered from merge abort",
                    "merge_orchestrator",
                    None,
                )
                .map_err(GitError::Transition)?;
        }

        // Transition merge to MergeAborted.
        intent
            .transition(MergeState::MergeAborted, reason, "merge_orchestrator", None)
            .map_err(GitError::Transition)?;

        // Release any merge lock.
        self.lock_map.release(&intent.target_ref).await;

        info!(
            merge_id = %intent.id,
            reason = %reason,
            "merge aborted"
        );

        Ok(())
    }
}
