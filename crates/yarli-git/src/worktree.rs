//! Worktree lifecycle management (Sections 12.1–12.3, 12.10–12.11).
//!
//! [`WorktreeManager`] abstracts over worktree creation, status inspection,
//! path confinement, interrupted-operation detection, recovery, and cleanup.
//! [`LocalWorktreeManager`] implements it using `git worktree` subcommands
//! via shell execution.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::yarli_core::entities::worktree_binding::WorktreeBinding;
use crate::yarli_core::fsm::worktree::WorktreeState;

use crate::yarli_git::constants::*;
use crate::yarli_git::error::{GitError, InterruptedOp, RecoveryAction};
use crate::yarli_git::submodule::{self, SubmoduleEntry};

/// Output from a git command execution.
#[derive(Debug)]
pub struct GitCommandOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Trait for managing worktree lifecycles.
///
/// All operations respect cancellation via [`CancellationToken`] and enforce
/// path confinement to the worktree root.
#[allow(async_fn_in_trait)]
pub trait WorktreeManager: Send + Sync {
    /// Create a worktree from a base ref (Section 12.2).
    ///
    /// Steps: verify repo cleanliness, resolve base ref, create branch,
    /// create worktree, validate .git indirection, transition to WtBoundHome.
    async fn create(
        &self,
        binding: &mut WorktreeBinding,
        cancel: CancellationToken,
    ) -> Result<(), GitError>;

    /// Check whether a worktree has uncommitted changes.
    async fn is_dirty(&self, worktree_path: &Path) -> Result<bool, GitError>;

    /// Get the current HEAD SHA for a worktree.
    async fn resolve_head(&self, worktree_path: &Path) -> Result<String, GitError>;

    /// Resolve a ref (branch name, tag, or SHA) to a full SHA in the repo.
    async fn resolve_ref(&self, repo_root: &Path, refspec: &str) -> Result<String, GitError>;

    /// Detect interrupted git operations by checking sentinel files (Section 12.10).
    async fn detect_interrupted(
        &self,
        worktree_path: &Path,
    ) -> Result<Option<InterruptedOp>, GitError>;

    /// Recover from an interrupted operation (Section 12.10).
    async fn recover(
        &self,
        binding: &mut WorktreeBinding,
        action: RecoveryAction,
        cancel: CancellationToken,
    ) -> Result<(), GitError>;

    /// Validate that a path is confined within the worktree root (Section 12.3).
    fn validate_path_confinement(&self, path: &Path, worktree_root: &Path) -> Result<(), GitError>;

    /// Validate the .git indirection file in a worktree (Section 12.2 step 5).
    async fn validate_git_indirection(&self, worktree_path: &Path) -> Result<(), GitError>;

    /// Compute a hash of `git submodule status` output for change detection.
    async fn submodule_status_hash(&self, worktree_path: &Path) -> Result<String, GitError>;

    /// Parse `git submodule status` into structured entries (Section 12.4).
    async fn submodule_status(&self, worktree_path: &Path)
        -> Result<Vec<SubmoduleEntry>, GitError>;

    /// Check for uninitialized submodules. Returns paths of uninitialized submodules.
    async fn check_uninitialized_submodules(
        &self,
        worktree_path: &Path,
    ) -> Result<Vec<String>, GitError>;

    /// Check for dirty (modified/conflicted) submodules. Returns paths of dirty submodules.
    async fn check_dirty_submodules(&self, worktree_path: &Path) -> Result<Vec<String>, GitError>;

    /// Verify submodule policy between before/after states (Section 12.4).
    ///
    /// Compares submodule status before and after an operation and validates
    /// that changes conform to the configured [`SubmoduleMode`].
    fn verify_submodule_policy(
        &self,
        mode: crate::yarli_core::entities::worktree_binding::SubmoduleMode,
        before: &[SubmoduleEntry],
        after: &[SubmoduleEntry],
    ) -> Result<(), GitError>;

    /// Remove the worktree and optionally its branch (Section 12.11).
    async fn cleanup(
        &self,
        binding: &mut WorktreeBinding,
        delete_branch: bool,
        cancel: CancellationToken,
    ) -> Result<(), GitError>;
}

/// Local worktree manager using git CLI commands.
#[derive(Debug, Clone)]
pub struct LocalWorktreeManager {
    /// Default timeout for git commands.
    pub default_timeout: Duration,
}

impl LocalWorktreeManager {
    pub fn new() -> Self {
        Self {
            default_timeout: GIT_COMMAND_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Run a git command in the given directory, respecting cancellation and timeout.
    pub(crate) async fn run_git(
        &self,
        cwd: &Path,
        args: &[&str],
        cancel: &CancellationToken,
    ) -> Result<GitCommandOutput, GitError> {
        self.run_git_with_timeout(cwd, args, cancel, self.default_timeout)
            .await
    }

    /// Run a git command with a specific timeout.
    pub(crate) async fn run_git_with_timeout(
        &self,
        cwd: &Path,
        args: &[&str],
        cancel: &CancellationToken,
        timeout: Duration,
    ) -> Result<GitCommandOutput, GitError> {
        use std::process::Stdio;
        use tokio::process::Command;

        let mut cmd = Command::new("git");
        cmd.args(args)
            .current_dir(cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        debug!(cwd = %cwd.display(), args = ?args, "running git command");

        let child = cmd.spawn().map_err(GitError::Io)?;

        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // child is killed on drop
                Err(GitError::CommandFailed {
                    exit_code: -1,
                    stderr: "cancelled by shutdown".into(),
                })
            }
            _ = tokio::time::sleep(timeout) => {
                warn!(args = ?args, "git command timed out");
                Err(GitError::CommandFailed {
                    exit_code: -1,
                    stderr: format!("timed out after {timeout:?}"),
                })
            }
            result = child.wait_with_output() => {
                let output = result.map_err(GitError::Io)?;
                let exit_code = output.status.code().unwrap_or(-1);
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();

                debug!(exit_code, stdout_len = stdout.len(), stderr_len = stderr.len(), "git command finished");

                Ok(GitCommandOutput {
                    exit_code,
                    stdout,
                    stderr,
                })
            }
        }
    }

    /// Run a git command and return Ok only if exit code is 0.
    pub(crate) async fn run_git_ok(
        &self,
        cwd: &Path,
        args: &[&str],
        cancel: &CancellationToken,
    ) -> Result<GitCommandOutput, GitError> {
        let output = self.run_git(cwd, args, cancel).await?;
        if output.exit_code != 0 {
            return Err(GitError::CommandFailed {
                exit_code: output.exit_code,
                stderr: output.stderr,
            });
        }
        Ok(output)
    }

    /// Compute worktree path from binding metadata.
    fn compute_worktree_path(binding: &WorktreeBinding) -> PathBuf {
        if !binding.worktree_path.as_os_str().is_empty() {
            return binding.worktree_path.clone();
        }

        let run_short = &binding.run_id.to_string()[..SHORT_ID_LEN.min(36)];
        let task_short = binding
            .task_id
            .map(|t| t.to_string()[..SHORT_ID_LEN.min(36)].to_string())
            .unwrap_or_else(|| "notask".to_string());
        let random_short = Uuid::new_v4().simple().to_string();
        let random_short = &random_short[..SHORT_ID_LEN.min(32)];
        binding
            .repo_root
            .join(WORKTREE_DIR)
            .join(format!("{run_short}-{task_short}-{random_short}"))
    }

    /// Find the git directory for a worktree (reads .git file).
    async fn find_git_dir(&self, worktree_path: &Path) -> Result<PathBuf, GitError> {
        let dot_git = worktree_path.join(DOT_GIT_FILE);
        let content = tokio::fs::read_to_string(&dot_git).await.map_err(|_| {
            GitError::InvalidGitIndirection {
                path: worktree_path.to_path_buf(),
            }
        })?;

        // .git file in worktrees contains "gitdir: <path>"
        let gitdir = content.trim().strip_prefix("gitdir: ").ok_or_else(|| {
            GitError::InvalidGitIndirection {
                path: worktree_path.to_path_buf(),
            }
        })?;

        Ok(PathBuf::from(gitdir))
    }
}

impl Default for LocalWorktreeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WorktreeManager for LocalWorktreeManager {
    async fn create(
        &self,
        binding: &mut WorktreeBinding,
        cancel: CancellationToken,
    ) -> Result<(), GitError> {
        let repo_root = binding.repo_root.clone();

        // Step 1: Verify repo exists and is clean (no implicit stash).
        // Transition: WtUnbound → WtCreating
        binding
            .transition(
                WorktreeState::WtCreating,
                "starting worktree creation",
                "worktree_manager",
                None,
            )
            .map_err(GitError::Transition)?;

        // Check that the repo is a valid git repository.
        let status_output = self
            .run_git_ok(&repo_root, &["rev-parse", "--git-dir"], &cancel)
            .await
            .map_err(|_| GitError::WorktreeCreation {
                reason: "not a git repository".into(),
            })?;
        let _ = status_output;

        // Step 2: Resolve base ref deterministically.
        let base_sha = self.resolve_ref(&repo_root, &binding.base_ref).await?;
        binding.update_head_ref(&base_sha);

        // Step 3: Create branch if absent at base SHA.
        let branch_check = self
            .run_git(
                &repo_root,
                &[
                    "rev-parse",
                    "--verify",
                    &format!("refs/heads/{}", binding.branch_name),
                ],
                &cancel,
            )
            .await?;

        if branch_check.exit_code != 0 {
            // Branch does not exist — create it at base SHA.
            self.run_git_ok(
                &repo_root,
                &["branch", &binding.branch_name, &base_sha],
                &cancel,
            )
            .await
            .map_err(|e| GitError::WorktreeCreation {
                reason: format!("failed to create branch: {e}"),
            })?;
            debug!(branch = %binding.branch_name, base = %base_sha, "created branch");
        }

        // Step 4: Create worktree with explicit branch binding.
        let worktree_path = Self::compute_worktree_path(binding);

        // Ensure parent directory exists.
        let parent = worktree_path
            .parent()
            .ok_or_else(|| GitError::WorktreeCreation {
                reason: "invalid worktree path".into(),
            })?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(GitError::Io)?;

        // Check worktree path doesn't already exist.
        if worktree_path.exists() {
            // Transition to recovering if path exists (may be leftover).
            binding
                .transition(
                    WorktreeState::WtRecovering,
                    "worktree path already exists",
                    "worktree_manager",
                    None,
                )
                .map_err(GitError::Transition)?;
            return Err(GitError::WorktreePathExists {
                path: worktree_path,
            });
        }

        let wt_path_str = worktree_path.to_string_lossy().to_string();
        self.run_git_ok(
            &repo_root,
            &["worktree", "add", &wt_path_str, &binding.branch_name],
            &cancel,
        )
        .await
        .map_err(|e| GitError::WorktreeCreation {
            reason: format!("git worktree add failed: {e}"),
        })?;

        binding.set_worktree_path(&worktree_path);

        // Step 5: Validate .git indirection and path confinement.
        self.validate_git_indirection(&worktree_path).await?;
        self.validate_path_confinement(&worktree_path, &repo_root)?;

        // Step 6: Submodule init is handled separately by caller (Section 12.4).

        // Step 7: Transition to WtBoundHome.
        let head = self.resolve_head(&worktree_path).await?;
        binding.update_head_ref(&head);

        binding
            .transition(
                WorktreeState::WtBoundHome,
                "worktree created and bound",
                "worktree_manager",
                None,
            )
            .map_err(GitError::Transition)?;

        info!(
            worktree = %worktree_path.display(),
            branch = %binding.branch_name,
            head = %head,
            "worktree created"
        );

        Ok(())
    }

    async fn is_dirty(&self, worktree_path: &Path) -> Result<bool, GitError> {
        let cancel = CancellationToken::new();
        let output = self
            .run_git_ok(worktree_path, &["status", "--porcelain"], &cancel)
            .await?;
        Ok(!output.stdout.trim().is_empty())
    }

    async fn resolve_head(&self, worktree_path: &Path) -> Result<String, GitError> {
        let cancel = CancellationToken::new();
        let output = self
            .run_git_ok(worktree_path, &["rev-parse", "HEAD"], &cancel)
            .await?;
        let sha = output.stdout.trim().to_string();
        if sha.is_empty() {
            return Err(GitError::RefNotFound {
                refspec: "HEAD".into(),
            });
        }
        Ok(sha)
    }

    async fn resolve_ref(&self, repo_root: &Path, refspec: &str) -> Result<String, GitError> {
        let cancel = CancellationToken::new();
        let output = self
            .run_git(repo_root, &["rev-parse", "--verify", refspec], &cancel)
            .await?;
        if output.exit_code != 0 {
            return Err(GitError::RefNotFound {
                refspec: refspec.to_string(),
            });
        }
        let sha = output.stdout.trim().to_string();
        if sha.is_empty() {
            return Err(GitError::RefNotFound {
                refspec: refspec.to_string(),
            });
        }
        Ok(sha)
    }

    async fn detect_interrupted(
        &self,
        worktree_path: &Path,
    ) -> Result<Option<InterruptedOp>, GitError> {
        // Look for the git dir — in worktrees .git is a file pointing to the actual gitdir.
        let git_dir = self.find_git_dir(worktree_path).await.unwrap_or_else(|_| {
            // Fallback: maybe it's a regular repo with .git directory
            worktree_path.join(DOT_GIT_FILE)
        });

        // Check sentinel files in order of likelihood.
        if git_dir.join(MERGE_HEAD_FILE).exists() {
            return Ok(Some(InterruptedOp::Merge));
        }
        if git_dir.join(REBASE_MERGE_DIR).exists() || git_dir.join(REBASE_APPLY_DIR).exists() {
            return Ok(Some(InterruptedOp::Rebase));
        }
        if git_dir.join(CHERRY_PICK_HEAD_FILE).exists() {
            return Ok(Some(InterruptedOp::CherryPick));
        }
        if git_dir.join(REVERT_HEAD_FILE).exists() {
            return Ok(Some(InterruptedOp::Revert));
        }

        Ok(None)
    }

    async fn recover(
        &self,
        binding: &mut WorktreeBinding,
        action: RecoveryAction,
        cancel: CancellationToken,
    ) -> Result<(), GitError> {
        let wt_path = binding.worktree_path.clone();

        // Detect what operation is interrupted.
        let interrupted = self.detect_interrupted(&wt_path).await?;
        let op = interrupted.ok_or_else(|| GitError::RecoveryFailed {
            reason: "no interrupted operation detected".into(),
        })?;

        // Transition to WtRecovering (if not already there).
        if binding.state != WorktreeState::WtRecovering {
            binding
                .transition(
                    WorktreeState::WtRecovering,
                    format!("recovering from interrupted {op}"),
                    "worktree_manager",
                    None,
                )
                .map_err(GitError::Transition)?;
        }

        match action {
            RecoveryAction::Abort => {
                let abort_cmd = match op {
                    InterruptedOp::Merge => "merge",
                    InterruptedOp::Rebase => "rebase",
                    InterruptedOp::CherryPick => "cherry-pick",
                    InterruptedOp::Revert => "revert",
                };
                self.run_git_ok(&wt_path, &[abort_cmd, "--abort"], &cancel)
                    .await
                    .map_err(|e| GitError::RecoveryFailed {
                        reason: format!("{abort_cmd} --abort failed: {e}"),
                    })?;
                info!(operation = %op, "aborted interrupted operation");
            }
            RecoveryAction::Resume => {
                let continue_cmd = match op {
                    InterruptedOp::Merge => {
                        // For merge, we commit (assumes conflicts resolved).
                        self.run_git_ok(&wt_path, &["commit", "--no-edit"], &cancel)
                            .await
                            .map_err(|e| GitError::RecoveryFailed {
                                reason: format!("merge commit failed: {e}"),
                            })?;
                        info!(operation = %op, "resumed merge via commit");
                        // Skip the continue path below.
                        let head = self.resolve_head(&wt_path).await?;
                        binding.update_head_ref(&head);
                        binding
                            .transition(
                                WorktreeState::WtBoundHome,
                                format!("recovered from interrupted {op}"),
                                "worktree_manager",
                                None,
                            )
                            .map_err(GitError::Transition)?;
                        return Ok(());
                    }
                    InterruptedOp::Rebase => "rebase",
                    InterruptedOp::CherryPick => "cherry-pick",
                    InterruptedOp::Revert => "revert",
                };
                self.run_git_ok(&wt_path, &[continue_cmd, "--continue"], &cancel)
                    .await
                    .map_err(|e| GitError::RecoveryFailed {
                        reason: format!("{continue_cmd} --continue failed: {e}"),
                    })?;
                info!(operation = %op, "resumed interrupted operation");
            }
            RecoveryAction::ManualBlock => {
                warn!(operation = %op, "manual intervention required");
                return Err(GitError::InterruptedOperation {
                    operation: op,
                    path: wt_path,
                });
            }
        }

        // After recovery, update head and transition to bound state.
        let head = self.resolve_head(&wt_path).await?;
        binding.update_head_ref(&head);

        binding
            .transition(
                WorktreeState::WtBoundHome,
                format!("recovered from interrupted {op}"),
                "worktree_manager",
                None,
            )
            .map_err(GitError::Transition)?;

        Ok(())
    }

    fn validate_path_confinement(&self, path: &Path, worktree_root: &Path) -> Result<(), GitError> {
        // Canonicalize if possible, otherwise use starts_with on the raw paths.
        let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let canonical_root = worktree_root
            .canonicalize()
            .unwrap_or_else(|_| worktree_root.to_path_buf());

        if !canonical_path.starts_with(&canonical_root) {
            return Err(GitError::PathConfinementViolation {
                path: path.to_path_buf(),
                root: worktree_root.to_path_buf(),
            });
        }
        Ok(())
    }

    async fn validate_git_indirection(&self, worktree_path: &Path) -> Result<(), GitError> {
        let dot_git = worktree_path.join(DOT_GIT_FILE);

        // In a worktree, .git is a file (not a directory) containing "gitdir: ..."
        let metadata =
            tokio::fs::metadata(&dot_git)
                .await
                .map_err(|_| GitError::InvalidGitIndirection {
                    path: worktree_path.to_path_buf(),
                })?;

        if metadata.is_dir() {
            // This is a full repo, not a worktree.
            return Err(GitError::InvalidGitIndirection {
                path: worktree_path.to_path_buf(),
            });
        }

        // Verify content starts with "gitdir: "
        let content = tokio::fs::read_to_string(&dot_git).await.map_err(|_| {
            GitError::InvalidGitIndirection {
                path: worktree_path.to_path_buf(),
            }
        })?;

        if !content.trim().starts_with("gitdir: ") {
            return Err(GitError::InvalidGitIndirection {
                path: worktree_path.to_path_buf(),
            });
        }

        Ok(())
    }

    async fn submodule_status_hash(&self, worktree_path: &Path) -> Result<String, GitError> {
        use sha2::{Digest, Sha256};

        let cancel = CancellationToken::new();
        let output = self
            .run_git_ok(worktree_path, &["submodule", "status"], &cancel)
            .await?;

        let mut hasher = Sha256::new();
        hasher.update(output.stdout.as_bytes());
        let result = hasher.finalize();
        Ok(format!("{result:x}"))
    }

    async fn submodule_status(
        &self,
        worktree_path: &Path,
    ) -> Result<Vec<SubmoduleEntry>, GitError> {
        let cancel = CancellationToken::new();
        let output = self
            .run_git_ok(worktree_path, &["submodule", "status"], &cancel)
            .await?;
        Ok(submodule::parse_submodule_status(&output.stdout))
    }

    async fn check_uninitialized_submodules(
        &self,
        worktree_path: &Path,
    ) -> Result<Vec<String>, GitError> {
        let entries = self.submodule_status(worktree_path).await?;
        Ok(submodule::find_uninitialized(&entries)
            .into_iter()
            .map(|e| e.path.clone())
            .collect())
    }

    async fn check_dirty_submodules(&self, worktree_path: &Path) -> Result<Vec<String>, GitError> {
        let entries = self.submodule_status(worktree_path).await?;
        Ok(submodule::find_dirty(&entries)
            .into_iter()
            .map(|e| e.path.clone())
            .collect())
    }

    fn verify_submodule_policy(
        &self,
        mode: crate::yarli_core::entities::worktree_binding::SubmoduleMode,
        before: &[SubmoduleEntry],
        after: &[SubmoduleEntry],
    ) -> Result<(), GitError> {
        submodule::check_policy(mode, before, after)
    }

    async fn cleanup(
        &self,
        binding: &mut WorktreeBinding,
        delete_branch: bool,
        cancel: CancellationToken,
    ) -> Result<(), GitError> {
        let repo_root = binding.repo_root.clone();
        let wt_path = binding.worktree_path.clone();
        let branch = binding.branch_name.clone();

        // Preconditions (Section 12.11).
        if binding.lease_owner.is_some() {
            return Err(GitError::CleanupBlocked {
                reason: "active lease exists".into(),
            });
        }

        // Transition to WtCleanupPending.
        binding
            .transition(
                WorktreeState::WtCleanupPending,
                "cleanup initiated",
                "worktree_manager",
                None,
            )
            .map_err(GitError::Transition)?;

        // Remove worktree via git.
        let wt_path_str = wt_path.to_string_lossy().to_string();
        let remove_result = self
            .run_git_ok(
                &repo_root,
                &["worktree", "remove", &wt_path_str, "--force"],
                &cancel,
            )
            .await;

        if let Err(e) = remove_result {
            // If git worktree remove fails, try manual cleanup.
            warn!(error = %e, path = %wt_path.display(), "git worktree remove failed, attempting manual cleanup");
            if wt_path.exists() {
                tokio::fs::remove_dir_all(&wt_path)
                    .await
                    .map_err(|io_err| GitError::CleanupBlocked {
                        reason: format!("manual removal failed: {io_err}"),
                    })?;
            }
            // Prune stale worktree references.
            let _ = self
                .run_git(&repo_root, &["worktree", "prune"], &cancel)
                .await;
        }

        // Optionally delete the branch.
        if delete_branch {
            let delete_result = self
                .run_git_ok(&repo_root, &["branch", "-d", &branch], &cancel)
                .await;
            if let Err(e) = delete_result {
                warn!(error = %e, branch = %branch, "branch deletion failed (may have unmerged changes)");
            }
        }

        // Transition to WtClosed.
        binding
            .transition(
                WorktreeState::WtClosed,
                "worktree cleaned up",
                "worktree_manager",
                None,
            )
            .map_err(GitError::Transition)?;

        info!(
            worktree = %wt_path.display(),
            branch = %branch,
            deleted_branch = delete_branch,
            "worktree cleaned up"
        );

        Ok(())
    }
}
