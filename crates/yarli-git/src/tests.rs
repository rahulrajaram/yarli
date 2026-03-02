//! Tests for yarli-git error types, constants, worktree manager, and merge orchestrator.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::constants::*;
use crate::error::*;
use crate::merge::{LocalMergeOrchestrator, MergeLockMap, MergeOrchestrator};
use crate::worktree::{LocalWorktreeManager, WorktreeManager};

use yarli_core::entities::merge_intent::{MergeIntent, MergeStrategy};
use yarli_core::entities::worktree_binding::WorktreeBinding;
use yarli_core::fsm::merge::MergeState;
use yarli_core::fsm::worktree::WorktreeState;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// ── GitError display tests ──────────────────────────────────────────────

#[test]
fn worktree_creation_error_display() {
    let err = GitError::WorktreeCreation {
        reason: "branch checkout failed".into(),
    };
    assert!(err.to_string().contains("worktree creation failed"));
    assert!(err.to_string().contains("branch checkout failed"));
}

#[test]
fn worktree_path_exists_error_display() {
    let err = GitError::WorktreePathExists {
        path: PathBuf::from("/repo/.yarl/worktrees/abc-12345678"),
    };
    assert!(err.to_string().contains("worktree path already exists"));
}

#[test]
fn worktree_not_found_error_display() {
    let err = GitError::WorktreeNotFound {
        path: PathBuf::from("/repo/.yarl/worktrees/gone"),
    };
    assert!(err.to_string().contains("worktree not found"));
}

#[test]
fn mutation_denied_error_display() {
    let err = GitError::MutationDenied {
        state: WorktreeState::WtUnbound,
    };
    let msg = err.to_string();
    assert!(msg.contains("mutations denied"));
    assert!(msg.contains("WtUnbound"));
}

#[test]
fn path_confinement_violation_display() {
    let err = GitError::PathConfinementViolation {
        path: PathBuf::from("/etc/passwd"),
        root: PathBuf::from("/repo/.yarl/worktrees/abc"),
    };
    let msg = err.to_string();
    assert!(msg.contains("path escapes worktree root"));
    assert!(msg.contains("/etc/passwd"));
}

#[test]
fn dirty_worktree_error_display() {
    let err = GitError::DirtyWorktree {
        path: PathBuf::from("/repo/wt"),
    };
    assert!(err.to_string().contains("worktree is dirty"));
}

#[test]
fn detached_head_error_display() {
    let err = GitError::DetachedHead {
        path: PathBuf::from("/repo/wt"),
    };
    assert!(err.to_string().contains("detached HEAD"));
}

#[test]
fn invalid_git_indirection_display() {
    let err = GitError::InvalidGitIndirection {
        path: PathBuf::from("/repo/wt"),
    };
    assert!(err.to_string().contains("invalid .git indirection"));
}

#[test]
fn cleanup_blocked_display() {
    let err = GitError::CleanupBlocked {
        reason: "active lease".into(),
    };
    assert!(err.to_string().contains("cleanup blocked"));
    assert!(err.to_string().contains("active lease"));
}

#[test]
fn merge_precheck_failed_display() {
    let err = GitError::MergePrecheckFailed {
        reason: "tests not green".into(),
    };
    assert!(err.to_string().contains("merge precheck failed"));
}

#[test]
fn target_ref_stale_display() {
    let err = GitError::TargetRefStale {
        expected: "abc123".into(),
        actual: "def456".into(),
    };
    let msg = err.to_string();
    assert!(msg.contains("target ref stale"));
    assert!(msg.contains("abc123"));
    assert!(msg.contains("def456"));
}

#[test]
fn merge_conflict_display() {
    let err = GitError::MergeConflict { count: 3 };
    assert!(err.to_string().contains("3 file(s)"));
}

#[test]
fn hook_rejected_display() {
    let err = GitError::HookRejected {
        hook: "commit-msg".into(),
        stderr: "Subject does not follow Conventional Commits".into(),
    };
    let msg = err.to_string();
    assert!(msg.contains("hook rejected"));
    assert!(msg.contains("commit-msg"));
    assert!(msg.contains("Conventional Commits"));
}

#[test]
fn merge_lock_unavailable_display() {
    let err = GitError::MergeLockUnavailable {
        branch: "main".into(),
    };
    assert!(err.to_string().contains("merge lock unavailable"));
    assert!(err.to_string().contains("main"));
}

#[test]
fn merge_verify_failed_display() {
    let err = GitError::MergeVerifyFailed {
        reason: "test suite failed".into(),
    };
    assert!(err.to_string().contains("merge verification failed"));
}

#[test]
fn invalid_merge_state_display() {
    let err = GitError::InvalidMergeState {
        state: MergeState::MergeDone,
    };
    assert!(err.to_string().contains("MergeDone"));
}

#[test]
fn uninitialized_submodule_display() {
    let err = GitError::UninitializedSubmodule {
        path: "vendor/lib".into(),
    };
    assert!(err.to_string().contains("uninitialized submodule"));
    assert!(err.to_string().contains("vendor/lib"));
}

#[test]
fn dirty_submodule_display() {
    let err = GitError::DirtySubmodule {
        path: "vendor/lib".into(),
    };
    assert!(err.to_string().contains("dirty submodule"));
}

#[test]
fn submodule_policy_violation_display() {
    let err = GitError::SubmodulePolicyViolation {
        path: "vendor/lib".into(),
        reason: "non-ff update in Locked mode".into(),
    };
    let msg = err.to_string();
    assert!(msg.contains("submodule policy violation"));
    assert!(msg.contains("Locked mode"));
}

#[test]
fn ref_not_found_display() {
    let err = GitError::RefNotFound {
        refspec: "origin/nonexistent".into(),
    };
    assert!(err.to_string().contains("ref not found"));
    assert!(err.to_string().contains("origin/nonexistent"));
}

#[test]
fn branch_already_exists_display() {
    let err = GitError::BranchAlreadyExists {
        branch: "yarl/run-1/task-a".into(),
    };
    assert!(err.to_string().contains("branch already exists"));
}

#[test]
fn command_failed_display() {
    let err = GitError::CommandFailed {
        exit_code: 128,
        stderr: "fatal: not a git repository".into(),
    };
    let msg = err.to_string();
    assert!(msg.contains("exit 128"));
    assert!(msg.contains("not a git repository"));
}

#[test]
fn io_error_conversion() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    let err: GitError = io_err.into();
    assert!(err.to_string().contains("file missing"));
}

#[test]
fn transition_error_conversion() {
    let te = yarli_core::error::TransitionError::TerminalState("WtClosed".into());
    let err: GitError = te.into();
    assert!(err.to_string().contains("WtClosed"));
}

// ── ForbiddenOp tests ───────────────────────────────────────────────────

#[test]
fn forbidden_op_push_display() {
    let op = ForbiddenOp::Push;
    assert_eq!(op.to_string(), "git push");
}

#[test]
fn forbidden_op_force_push_display() {
    let op = ForbiddenOp::ForcePush;
    assert_eq!(op.to_string(), "git push --force");
}

#[test]
fn forbidden_op_tag_display() {
    let op = ForbiddenOp::Tag;
    assert_eq!(op.to_string(), "git tag");
}

#[test]
fn forbidden_op_branch_delete_display() {
    let op = ForbiddenOp::BranchDelete {
        branch: "feature/old".into(),
    };
    assert_eq!(op.to_string(), "git branch -D feature/old");
}

#[test]
fn forbidden_op_stash_clear_display() {
    let op = ForbiddenOp::StashClear;
    assert_eq!(op.to_string(), "git stash clear");
}

#[test]
fn forbidden_op_destructive_cleanup_display() {
    let op = ForbiddenOp::DestructiveCleanup {
        command: "git clean -fdx".into(),
    };
    assert_eq!(op.to_string(), "git clean -fdx");
}

#[test]
fn forbidden_op_in_git_error_display() {
    let err = GitError::ForbiddenOperation {
        operation: ForbiddenOp::Push,
    };
    assert!(err.to_string().contains("forbidden git operation"));
    assert!(err.to_string().contains("git push"));
}

// ── InterruptedOp tests ─────────────────────────────────────────────────

#[test]
fn interrupted_op_merge_display() {
    assert_eq!(InterruptedOp::Merge.to_string(), "merge");
}

#[test]
fn interrupted_op_rebase_display() {
    assert_eq!(InterruptedOp::Rebase.to_string(), "rebase");
}

#[test]
fn interrupted_op_cherry_pick_display() {
    assert_eq!(InterruptedOp::CherryPick.to_string(), "cherry-pick");
}

#[test]
fn interrupted_op_revert_display() {
    assert_eq!(InterruptedOp::Revert.to_string(), "revert");
}

#[test]
fn interrupted_operation_error_display() {
    let err = GitError::InterruptedOperation {
        operation: InterruptedOp::Merge,
        path: PathBuf::from("/repo/.yarl/worktrees/abc"),
    };
    let msg = err.to_string();
    assert!(msg.contains("interrupted merge"));
    assert!(msg.contains("/repo/.yarl/worktrees/abc"));
}

#[test]
fn recovery_failed_display() {
    let err = GitError::RecoveryFailed {
        reason: "abort command exited non-zero".into(),
    };
    assert!(err.to_string().contains("recovery failed"));
}

// ── RecoveryAction tests ────────────────────────────────────────────────

#[test]
fn recovery_action_variants() {
    // Just assert the enum is usable (no Display needed, it's internal)
    assert_eq!(RecoveryAction::Abort, RecoveryAction::Abort);
    assert_eq!(RecoveryAction::Resume, RecoveryAction::Resume);
    assert_eq!(RecoveryAction::ManualBlock, RecoveryAction::ManualBlock);
    assert_ne!(RecoveryAction::Abort, RecoveryAction::Resume);
}

// ── ForbiddenOp equality ────────────────────────────────────────────────

#[test]
fn forbidden_op_equality() {
    assert_eq!(ForbiddenOp::Push, ForbiddenOp::Push);
    assert_ne!(ForbiddenOp::Push, ForbiddenOp::ForcePush);
    assert_eq!(
        ForbiddenOp::BranchDelete { branch: "x".into() },
        ForbiddenOp::BranchDelete { branch: "x".into() }
    );
}

// ── Constants tests ─────────────────────────────────────────────────────

#[test]
fn worktree_dir_is_under_yarl() {
    assert!(WORKTREE_DIR.starts_with(".yarl/"));
    assert!(WORKTREE_DIR.contains("worktrees"));
}

#[test]
fn branch_prefix_is_yarl() {
    assert_eq!(BRANCH_PREFIX, "yarl/");
}

#[test]
fn sentinel_files_are_correct() {
    assert_eq!(MERGE_HEAD_FILE, "MERGE_HEAD");
    assert_eq!(CHERRY_PICK_HEAD_FILE, "CHERRY_PICK_HEAD");
    assert_eq!(REVERT_HEAD_FILE, "REVERT_HEAD");
    assert_eq!(REBASE_MERGE_DIR, "rebase-merge");
    assert_eq!(REBASE_APPLY_DIR, "rebase-apply");
}

#[test]
fn dot_git_file_constant() {
    assert_eq!(DOT_GIT_FILE, ".git");
}

#[test]
fn merge_commit_template_has_placeholders() {
    assert!(MERGE_COMMIT_TEMPLATE.contains("{source}"));
    assert!(MERGE_COMMIT_TEMPLATE.contains("{target}"));
    assert!(MERGE_COMMIT_TEMPLATE.contains("{run_id}"));
    assert!(MERGE_COMMIT_TEMPLATE.contains("{task_id}"));
}

#[test]
fn git_command_timeout_is_reasonable() {
    assert!(GIT_COMMAND_TIMEOUT.as_secs() >= 30);
    assert!(GIT_COMMAND_TIMEOUT.as_secs() <= 600);
}

#[test]
fn merge_lock_timeout_is_reasonable() {
    assert!(MERGE_LOCK_TIMEOUT.as_secs() >= 5);
    assert!(MERGE_LOCK_TIMEOUT.as_secs() <= 120);
}

#[test]
fn worktree_create_timeout_is_reasonable() {
    assert!(WORKTREE_CREATE_TIMEOUT.as_secs() >= 60);
}

#[test]
fn git_kill_grace_matches_core() {
    // Section 31: SIGKILL after 5s
    assert_eq!(GIT_KILL_GRACE.as_secs(), 5);
}

#[test]
fn forbidden_subcommands_includes_push() {
    assert!(FORBIDDEN_SUBCOMMANDS.contains(&"push"));
    assert!(FORBIDDEN_SUBCOMMANDS.contains(&"tag"));
}

#[test]
fn forbidden_args_includes_force() {
    assert!(FORBIDDEN_ARGS.contains(&"--force"));
    assert!(FORBIDDEN_ARGS.contains(&"--force-with-lease"));
    assert!(FORBIDDEN_ARGS.contains(&"-D"));
}

#[test]
fn short_id_len_is_8() {
    assert_eq!(SHORT_ID_LEN, 8);
}

#[test]
fn submodule_prefixes() {
    assert_eq!(SUBMODULE_UNINIT_PREFIX, '-');
    assert_eq!(SUBMODULE_MODIFIED_PREFIX, '+');
    assert_eq!(SUBMODULE_CONFLICT_PREFIX, 'U');
}

// ── WorktreeManager tests ────────────────────────────────────────────────

/// Helper: create a temporary git repo with an initial commit.
async fn create_test_repo(dir: &Path) {
    use std::process::Command;

    Command::new("git")
        .args(["init"])
        .current_dir(dir)
        .output()
        .expect("git init");

    Command::new("git")
        .args(["config", "user.email", "test@yarli.dev"])
        .current_dir(dir)
        .output()
        .expect("git config email");

    Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(dir)
        .output()
        .expect("git config name");

    std::fs::write(dir.join("README.md"), "# test repo\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add");

    Command::new("git")
        .args(["commit", "-m", "initial commit"])
        .current_dir(dir)
        .output()
        .expect("git commit");
}

/// Helper: create a WorktreeBinding for testing.
fn make_binding(repo_root: &Path) -> WorktreeBinding {
    let run_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();
    let task_id = Uuid::now_v7();
    WorktreeBinding::new(
        run_id,
        repo_root,
        format!("yarl/{}/test-task", &run_id.to_string()[..8]),
        "HEAD",
        corr_id,
    )
    .with_task(task_id)
}

// ── LocalWorktreeManager unit tests ──────────────────────────────────────

#[test]
fn local_worktree_manager_default_timeout() {
    let mgr = LocalWorktreeManager::new();
    assert_eq!(mgr.default_timeout, GIT_COMMAND_TIMEOUT);
}

#[test]
fn local_worktree_manager_custom_timeout() {
    use std::time::Duration;
    let mgr = LocalWorktreeManager::new().with_timeout(Duration::from_secs(10));
    assert_eq!(mgr.default_timeout, Duration::from_secs(10));
}

#[test]
fn local_worktree_manager_default_is_same_as_new() {
    let a = LocalWorktreeManager::new();
    let b = LocalWorktreeManager::default();
    assert_eq!(a.default_timeout, b.default_timeout);
}

// ── Path confinement tests ───────────────────────────────────────────────

#[test]
fn path_confinement_accepts_child_path() {
    let mgr = LocalWorktreeManager::new();
    let root = PathBuf::from("/tmp/repo/worktree");
    let child = PathBuf::from("/tmp/repo/worktree/src/main.rs");
    assert!(mgr.validate_path_confinement(&child, &root).is_ok());
}

#[test]
fn path_confinement_accepts_exact_root() {
    let mgr = LocalWorktreeManager::new();
    let root = PathBuf::from("/tmp/repo/worktree");
    assert!(mgr.validate_path_confinement(&root, &root).is_ok());
}

#[test]
fn path_confinement_rejects_parent_traversal() {
    let mgr = LocalWorktreeManager::new();
    let root = PathBuf::from("/tmp/repo/worktree");
    let escape = PathBuf::from("/tmp/repo/other");
    let result = mgr.validate_path_confinement(&escape, &root);
    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::PathConfinementViolation { path, root: r } => {
            assert_eq!(path, PathBuf::from("/tmp/repo/other"));
            assert_eq!(r, root);
        }
        other => panic!("expected PathConfinementViolation, got {other:?}"),
    }
}

#[test]
fn path_confinement_rejects_absolute_escape() {
    let mgr = LocalWorktreeManager::new();
    let root = PathBuf::from("/tmp/repo/worktree");
    let escape = PathBuf::from("/etc/passwd");
    assert!(mgr.validate_path_confinement(&escape, &root).is_err());
}

// ── Integration tests (require git) ──────────────────────────────────────

#[tokio::test]
async fn create_worktree_full_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    // Create worktree.
    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    assert_eq!(binding.state, WorktreeState::WtBoundHome);
    assert!(!binding.worktree_path.as_os_str().is_empty());
    assert!(binding.worktree_path.exists());
    assert!(!binding.head_ref.is_empty());
    assert_ne!(binding.head_ref, "HEAD");

    // Verify .git indirection is valid.
    mgr.validate_git_indirection(&binding.worktree_path)
        .await
        .unwrap();

    // Worktree should be clean.
    let dirty = mgr.is_dirty(&binding.worktree_path).await.unwrap();
    assert!(!dirty);

    // Cleanup.
    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
    assert_eq!(binding.state, WorktreeState::WtClosed);
    assert!(!binding.worktree_path.exists());
}

#[tokio::test]
async fn resolve_ref_returns_sha() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let sha = mgr.resolve_ref(&repo, "HEAD").await.unwrap();

    // SHA should be 40 hex characters.
    assert_eq!(sha.len(), 40);
    assert!(sha.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn resolve_ref_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let result = mgr.resolve_ref(&repo, "nonexistent-ref-abc123").await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::RefNotFound { refspec } => {
            assert_eq!(refspec, "nonexistent-ref-abc123");
        }
        other => panic!("expected RefNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn resolve_head_in_worktree() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let head = mgr.resolve_head(&binding.worktree_path).await.unwrap();
    assert_eq!(head.len(), 40);
    assert_eq!(head, binding.head_ref);

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn is_dirty_detects_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Initially clean.
    assert!(!mgr.is_dirty(&binding.worktree_path).await.unwrap());

    // Create an untracked file.
    std::fs::write(binding.worktree_path.join("dirty.txt"), "dirty").unwrap();
    assert!(mgr.is_dirty(&binding.worktree_path).await.unwrap());

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn detect_no_interrupted_operation() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let result = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert!(result.is_none());

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn detect_interrupted_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Simulate an interrupted merge by creating the MERGE_HEAD sentinel.
    // In a worktree, the git dir is pointed to by .git file.
    let git_dir = {
        let content = std::fs::read_to_string(binding.worktree_path.join(".git")).unwrap();
        let gitdir = content.trim().strip_prefix("gitdir: ").unwrap();
        PathBuf::from(gitdir)
    };
    std::fs::write(git_dir.join(MERGE_HEAD_FILE), "abc123\n").unwrap();

    let result = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert_eq!(result, Some(InterruptedOp::Merge));

    // Clean up sentinel so worktree remove works.
    std::fs::remove_file(git_dir.join(MERGE_HEAD_FILE)).unwrap();
    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn detect_interrupted_rebase() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let git_dir = {
        let content = std::fs::read_to_string(binding.worktree_path.join(".git")).unwrap();
        let gitdir = content.trim().strip_prefix("gitdir: ").unwrap();
        PathBuf::from(gitdir)
    };
    std::fs::create_dir_all(git_dir.join(REBASE_MERGE_DIR)).unwrap();

    let result = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert_eq!(result, Some(InterruptedOp::Rebase));

    std::fs::remove_dir_all(git_dir.join(REBASE_MERGE_DIR)).unwrap();
    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn detect_interrupted_cherry_pick() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let git_dir = {
        let content = std::fs::read_to_string(binding.worktree_path.join(".git")).unwrap();
        let gitdir = content.trim().strip_prefix("gitdir: ").unwrap();
        PathBuf::from(gitdir)
    };
    std::fs::write(git_dir.join(CHERRY_PICK_HEAD_FILE), "abc123\n").unwrap();

    let result = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert_eq!(result, Some(InterruptedOp::CherryPick));

    std::fs::remove_file(git_dir.join(CHERRY_PICK_HEAD_FILE)).unwrap();
    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn detect_interrupted_revert() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let git_dir = {
        let content = std::fs::read_to_string(binding.worktree_path.join(".git")).unwrap();
        let gitdir = content.trim().strip_prefix("gitdir: ").unwrap();
        PathBuf::from(gitdir)
    };
    std::fs::write(git_dir.join(REVERT_HEAD_FILE), "abc123\n").unwrap();

    let result = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert_eq!(result, Some(InterruptedOp::Revert));

    std::fs::remove_file(git_dir.join(REVERT_HEAD_FILE)).unwrap();
    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn create_worktree_path_already_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    // Create the worktree path manually before create().
    let wt_path = repo.join(WORKTREE_DIR).join(format!(
        "{}-{}",
        &binding.run_id.to_string()[..SHORT_ID_LEN],
        &binding.task_id.unwrap().to_string()[..SHORT_ID_LEN],
    ));
    std::fs::create_dir_all(&wt_path).unwrap();

    let result = mgr.create(&mut binding, cancel).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::WorktreePathExists { path } => {
            assert_eq!(path, wt_path);
        }
        other => panic!("expected WorktreePathExists, got {other:?}"),
    }
    // Binding should be in WtRecovering since the path existed.
    assert_eq!(binding.state, WorktreeState::WtRecovering);
}

#[tokio::test]
async fn cleanup_blocked_with_active_lease() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Set a lease owner — cleanup should be blocked.
    binding.set_lease_owner(Some("worker-1".into()));

    let result = mgr.cleanup(&mut binding, false, cancel).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::CleanupBlocked { reason } => {
            assert!(reason.contains("lease"));
        }
        other => panic!("expected CleanupBlocked, got {other:?}"),
    }
    // State should still be WtBoundHome (cleanup didn't transition).
    assert_eq!(binding.state, WorktreeState::WtBoundHome);
}

#[tokio::test]
async fn submodule_status_hash_is_deterministic() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Hash should be deterministic (no submodules = empty output = same hash).
    let hash1 = mgr
        .submodule_status_hash(&binding.worktree_path)
        .await
        .unwrap();
    let hash2 = mgr
        .submodule_status_hash(&binding.worktree_path)
        .await
        .unwrap();
    assert_eq!(hash1, hash2);
    assert_eq!(hash1.len(), 64); // SHA-256 hex is 64 chars.

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn validate_git_indirection_on_worktree() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // .git should be a file (not a directory) in a worktree.
    let dot_git = binding.worktree_path.join(".git");
    assert!(dot_git.exists());
    assert!(dot_git.is_file());

    mgr.validate_git_indirection(&binding.worktree_path)
        .await
        .unwrap();

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn validate_git_indirection_fails_on_main_repo() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    // The main repo has .git as a directory, not a file.
    let result = mgr.validate_git_indirection(&repo).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::InvalidGitIndirection { .. } => {}
        other => panic!("expected InvalidGitIndirection, got {other:?}"),
    }
}

#[tokio::test]
async fn create_worktree_sets_head_to_actual_sha() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // head_ref should be a valid SHA, not "HEAD".
    assert_ne!(binding.head_ref, "HEAD");
    assert_eq!(binding.head_ref.len(), 40);
    assert!(binding.head_ref.chars().all(|c| c.is_ascii_hexdigit()));

    // Should match what git says.
    let actual_head = mgr.resolve_head(&binding.worktree_path).await.unwrap();
    assert_eq!(binding.head_ref, actual_head);

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn create_worktree_creates_branch() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    let branch_name = binding.branch_name.clone();
    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Verify the branch exists in the repo.
    let output = std::process::Command::new("git")
        .args(["branch", "--list", &branch_name])
        .current_dir(&repo)
        .output()
        .unwrap();
    let branches = String::from_utf8_lossy(&output.stdout);
    assert!(
        branches.contains(&branch_name),
        "branch should exist: {branches}"
    );

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn cleanup_without_branch_deletion() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    let branch_name = binding.branch_name.clone();
    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Cleanup without deleting branch.
    mgr.cleanup(&mut binding, false, cancel).await.unwrap();
    assert_eq!(binding.state, WorktreeState::WtClosed);

    // Branch should still exist.
    let output = std::process::Command::new("git")
        .args(["branch", "--list", &branch_name])
        .current_dir(&repo)
        .output()
        .unwrap();
    let branches = String::from_utf8_lossy(&output.stdout);
    assert!(branches.contains(&branch_name), "branch should still exist");
}

#[tokio::test]
async fn recovery_manual_block_returns_error() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Create a fake merge sentinel.
    let git_dir = {
        let content = std::fs::read_to_string(binding.worktree_path.join(".git")).unwrap();
        let gitdir = content.trim().strip_prefix("gitdir: ").unwrap();
        PathBuf::from(gitdir)
    };
    std::fs::write(git_dir.join(MERGE_HEAD_FILE), "abc123\n").unwrap();

    // Transition to WtMerging first (WtBoundHome cannot go directly to WtRecovering).
    binding
        .transition(WorktreeState::WtMerging, "test merge", "test", None)
        .unwrap();

    // ManualBlock should return InterruptedOperation error.
    let result = mgr
        .recover(&mut binding, RecoveryAction::ManualBlock, cancel.clone())
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::InterruptedOperation { operation, .. } => {
            assert_eq!(operation, InterruptedOp::Merge);
        }
        other => panic!("expected InterruptedOperation, got {other:?}"),
    }
    assert_eq!(binding.state, WorktreeState::WtRecovering);

    // Clean up.
    std::fs::remove_file(git_dir.join(MERGE_HEAD_FILE)).unwrap();
    binding
        .transition(
            WorktreeState::WtCleanupPending,
            "manual cleanup",
            "test",
            None,
        )
        .unwrap();
    binding
        .transition(WorktreeState::WtClosed, "closed", "test", None)
        .unwrap();
}

#[tokio::test]
async fn recovery_no_interrupted_op_returns_error() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // No interrupted operation — recovery should fail.
    let result = mgr
        .recover(&mut binding, RecoveryAction::Abort, cancel.clone())
        .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::RecoveryFailed { reason } => {
            assert!(reason.contains("no interrupted operation"));
        }
        other => panic!("expected RecoveryFailed, got {other:?}"),
    }

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn worktree_path_computed_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Path should be under .yarl/worktrees/
    let wt_path = binding.worktree_path.clone();
    assert!(wt_path.starts_with(&repo));
    let relative = wt_path.strip_prefix(&repo).unwrap();
    let rel_str = relative.to_string_lossy();
    assert!(
        rel_str.starts_with(WORKTREE_DIR),
        "path should start with {WORKTREE_DIR}, got {rel_str}"
    );

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

// ── MergeOrchestrator tests ────────────────────────────────────────────────

/// Helper: create a test repo with a feature branch that has diverged from main.
async fn create_merge_test_repo(dir: &Path) -> (String, String) {
    use std::process::Command;

    Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(dir)
        .output()
        .expect("git init");

    Command::new("git")
        .args(["config", "user.email", "test@yarli.dev"])
        .current_dir(dir)
        .output()
        .expect("git config email");

    Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(dir)
        .output()
        .expect("git config name");

    std::fs::write(dir.join("README.md"), "# test repo\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add");

    Command::new("git")
        .args(["commit", "-m", "initial commit"])
        .current_dir(dir)
        .output()
        .expect("git commit");

    // Create feature branch with a change.
    Command::new("git")
        .args(["checkout", "-b", "feature/test-merge"])
        .current_dir(dir)
        .output()
        .expect("git checkout -b feature");

    std::fs::write(dir.join("feature.txt"), "feature content\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add feature");

    Command::new("git")
        .args(["commit", "-m", "add feature"])
        .current_dir(dir)
        .output()
        .expect("git commit feature");

    // Switch back to main.
    Command::new("git")
        .args(["checkout", "main"])
        .current_dir(dir)
        .output()
        .expect("git checkout main");

    ("main".to_string(), "feature/test-merge".to_string())
}

/// Helper: create a merge test setup with worktree and intent.
async fn setup_merge_test() -> (
    tempfile::TempDir,
    LocalMergeOrchestrator,
    WorktreeBinding,
    MergeIntent,
) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (main_branch, feature_branch) = create_merge_test_repo(&repo).await;

    let wt_mgr = LocalWorktreeManager::new();
    let cancel = CancellationToken::new();

    let run_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();
    let task_id = Uuid::now_v7();
    let wt_id = Uuid::now_v7();

    let mut binding = WorktreeBinding::new(
        run_id,
        &repo,
        format!("yarl/{}/merge-task", &run_id.to_string()[..8]),
        "main",
        corr_id,
    )
    .with_task(task_id);

    wt_mgr.create(&mut binding, cancel).await.unwrap();

    let intent = MergeIntent::new(run_id, wt_id, &feature_branch, &main_branch, corr_id);

    let orchestrator = LocalMergeOrchestrator::new(wt_mgr);

    (tmp, orchestrator, binding, intent)
}

#[tokio::test]
async fn merge_precheck_resolves_shas() {
    let (_tmp, orchestrator, binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    let result = orchestrator
        .precheck(&mut intent, &binding, cancel)
        .await
        .unwrap();

    assert_eq!(intent.state, MergeState::MergePrecheck);
    assert_eq!(result.source_sha.len(), 40);
    assert_eq!(result.target_sha.len(), 40);
    assert!(intent.source_sha.is_some());
    assert!(intent.target_sha.is_some());
}

#[tokio::test]
async fn merge_precheck_fails_on_invalid_source_ref() {
    let (_tmp, orchestrator, binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    intent.source_ref = "nonexistent-branch".into();

    let result = orchestrator.precheck(&mut intent, &binding, cancel).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::MergePrecheckFailed { reason } => {
            assert!(reason.contains("source ref"));
        }
        other => panic!("expected MergePrecheckFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_precheck_fails_on_dirty_worktree() {
    let (_tmp, orchestrator, binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    std::fs::write(binding.worktree_path.join("dirty.txt"), "dirty").unwrap();

    let result = orchestrator.precheck(&mut intent, &binding, cancel).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::MergePrecheckFailed { reason } => {
            assert!(reason.contains("uncommitted changes"));
        }
        other => panic!("expected MergePrecheckFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_precheck_rejects_wrong_state() {
    let (_tmp, orchestrator, binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    intent.state = MergeState::MergeDryRun;

    let result = orchestrator.precheck(&mut intent, &binding, cancel).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::InvalidMergeState { state } => {
            assert_eq!(state, MergeState::MergeDryRun);
        }
        other => panic!("expected InvalidMergeState, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_dry_run_clean() {
    let (_tmp, orchestrator, binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();

    let result = orchestrator
        .dry_run(&mut intent, &binding, cancel)
        .await
        .unwrap();

    assert!(result.clean);
    assert!(result.conflicts.is_empty());
    assert!(!result.changed_files.is_empty());
    assert_eq!(intent.state, MergeState::MergeDryRun);
}

#[tokio::test]
async fn merge_dry_run_detects_conflict() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main_branch, _feature_branch) = create_merge_test_repo(&repo).await;

    // Create a conflicting change on main.
    std::fs::write(repo.join("feature.txt"), "main conflicting content\n").unwrap();
    std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(&repo)
        .output()
        .unwrap();
    std::process::Command::new("git")
        .args(["commit", "-m", "conflicting change on main"])
        .current_dir(&repo)
        .output()
        .unwrap();

    let wt_mgr = LocalWorktreeManager::new();
    let cancel = CancellationToken::new();
    let run_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();
    let task_id = Uuid::now_v7();
    let wt_id = Uuid::now_v7();

    let mut binding = WorktreeBinding::new(
        run_id,
        &repo,
        format!("yarl/{}/conflict-task", &run_id.to_string()[..8]),
        "main",
        corr_id,
    )
    .with_task(task_id);

    wt_mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let mut intent = MergeIntent::new(run_id, wt_id, "feature/test-merge", "main", corr_id);

    let orchestrator = LocalMergeOrchestrator::new(wt_mgr);

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();

    let result = orchestrator
        .dry_run(&mut intent, &binding, cancel)
        .await
        .unwrap();

    assert!(!result.clean);
    assert!(!result.conflicts.is_empty());
    assert_eq!(intent.state, MergeState::MergeConflict);
    assert!(intent.has_conflicts());
}

#[tokio::test]
async fn merge_full_lifecycle_merge_no_ff() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    let precheck = orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    assert_eq!(intent.state, MergeState::MergePrecheck);

    let dry = orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    assert!(dry.clean);
    assert_eq!(intent.state, MergeState::MergeDryRun);

    let apply = orchestrator
        .apply(&mut intent, &mut binding, cancel.clone())
        .await
        .unwrap();
    assert_eq!(apply.merge_sha.len(), 40);
    assert!(!apply.changed_files.is_empty());
    assert_eq!(intent.state, MergeState::MergeApply);
    assert!(intent.result_sha.is_some());
    assert_eq!(binding.state, WorktreeState::WtBoundHome);

    orchestrator
        .verify(&mut intent, &binding, &precheck.submodule_snapshot, cancel)
        .await
        .unwrap();
    assert_eq!(intent.state, MergeState::MergeDone);
    assert!(intent.state.is_terminal());
}

#[tokio::test]
async fn merge_full_lifecycle_squash() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();
    intent.strategy = MergeStrategy::SquashMerge;

    let precheck = orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();

    let dry = orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    assert!(dry.clean);

    let apply = orchestrator
        .apply(&mut intent, &mut binding, cancel.clone())
        .await
        .unwrap();
    assert_eq!(apply.merge_sha.len(), 40);

    orchestrator
        .verify(&mut intent, &binding, &precheck.submodule_snapshot, cancel)
        .await
        .unwrap();
    assert_eq!(intent.state, MergeState::MergeDone);
}

#[tokio::test]
async fn merge_abort_from_requested() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    assert_eq!(intent.state, MergeState::MergeRequested);

    orchestrator
        .abort(&mut intent, &mut binding, "user cancelled", cancel)
        .await
        .unwrap();

    assert_eq!(intent.state, MergeState::MergeAborted);
    assert!(intent.state.is_terminal());
    assert_eq!(binding.state, WorktreeState::WtBoundHome);
}

#[tokio::test]
async fn merge_abort_from_precheck() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    assert_eq!(intent.state, MergeState::MergePrecheck);

    orchestrator
        .abort(&mut intent, &mut binding, "precheck abort", cancel)
        .await
        .unwrap();

    assert_eq!(intent.state, MergeState::MergeAborted);
}

#[tokio::test]
async fn merge_abort_from_terminal_fails() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    intent.state = MergeState::MergeDone;

    let result = orchestrator
        .abort(&mut intent, &mut binding, "should fail", cancel)
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::InvalidMergeState { state } => {
            assert_eq!(state, MergeState::MergeDone);
        }
        other => panic!("expected InvalidMergeState, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_lock_prevents_concurrent_apply() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();

    // Pre-acquire the lock for the target branch.
    orchestrator.lock_map.try_acquire(&intent.target_ref).await;

    let result = orchestrator.apply(&mut intent, &mut binding, cancel).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::MergeLockUnavailable { branch } => {
            assert_eq!(branch, "main");
        }
        other => panic!("expected MergeLockUnavailable, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_lock_map_acquire_release() {
    let lock_map = MergeLockMap::new();

    assert!(lock_map.try_acquire("main").await);
    assert!(!lock_map.try_acquire("main").await);

    lock_map.release("main").await;
    assert!(lock_map.try_acquire("main").await);
}

#[tokio::test]
async fn merge_lock_map_independent_branches() {
    let lock_map = MergeLockMap::new();

    assert!(lock_map.try_acquire("main").await);
    assert!(lock_map.try_acquire("develop").await);

    assert!(!lock_map.try_acquire("main").await);
    assert!(!lock_map.try_acquire("develop").await);

    lock_map.release("main").await;
    assert!(lock_map.try_acquire("main").await);
    assert!(!lock_map.try_acquire("develop").await);
}

#[tokio::test]
async fn merge_apply_releases_lock_on_success() {
    let lock_map = Arc::new(MergeLockMap::new());
    let (_tmp, mut orchestrator, mut binding, mut intent) = setup_merge_test().await;
    orchestrator.lock_map = Arc::clone(&lock_map);
    let cancel = CancellationToken::new();

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .apply(&mut intent, &mut binding, cancel.clone())
        .await
        .unwrap();

    // Lock should be released after apply.
    assert!(lock_map.try_acquire("main").await);
}

#[tokio::test]
async fn merge_dry_run_rejects_wrong_state() {
    let (_tmp, orchestrator, binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    let result = orchestrator.dry_run(&mut intent, &binding, cancel).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::InvalidMergeState { state } => {
            assert_eq!(state, MergeState::MergeRequested);
        }
        other => panic!("expected InvalidMergeState, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_apply_rejects_wrong_state() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();

    let result = orchestrator.apply(&mut intent, &mut binding, cancel).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::InvalidMergeState { state } => {
            assert_eq!(state, MergeState::MergePrecheck);
        }
        other => panic!("expected InvalidMergeState, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_verify_rejects_wrong_state() {
    let (_tmp, orchestrator, binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    let result = orchestrator
        .verify(&mut intent, &binding, &[], cancel)
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::InvalidMergeState { state } => {
            assert_eq!(state, MergeState::MergeRequested);
        }
        other => panic!("expected InvalidMergeState, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_commit_message_has_attribution() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .apply(&mut intent, &mut binding, cancel.clone())
        .await
        .unwrap();

    let output = std::process::Command::new("git")
        .args(["log", "-1", "--format=%B"])
        .current_dir(&binding.worktree_path)
        .output()
        .unwrap();
    let msg = String::from_utf8_lossy(&output.stdout);
    assert!(
        msg.contains("yarli-run:"),
        "commit should contain yarli-run attribution"
    );
    assert!(
        msg.contains("yarli-task:"),
        "commit should contain yarli-task attribution"
    );
    assert!(
        msg.contains("chore(yarli): merge"),
        "commit should use conventional commits format: {msg}"
    );
}

#[tokio::test]
async fn merge_worktree_transitions_through_merging() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    assert_eq!(binding.state, WorktreeState::WtBoundHome);

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .apply(&mut intent, &mut binding, cancel.clone())
        .await
        .unwrap();

    assert_eq!(binding.state, WorktreeState::WtBoundHome);
    assert_eq!(
        binding.head_ref,
        intent.result_sha.as_ref().unwrap().as_str()
    );
}

#[tokio::test]
async fn merge_result_sha_is_valid() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .apply(&mut intent, &mut binding, cancel.clone())
        .await
        .unwrap();

    let result_sha = intent.result_sha.as_ref().unwrap();
    assert_eq!(result_sha.len(), 40);
    assert!(result_sha.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(result_sha, intent.source_sha.as_ref().unwrap());
    assert_ne!(result_sha, intent.target_sha.as_ref().unwrap());
}

#[test]
fn merge_build_commit_message() {
    let run_id = Uuid::now_v7();
    let wt_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();

    let intent = MergeIntent::new(run_id, wt_id, "feature/branch", "main", corr_id);

    let msg = LocalMergeOrchestrator::build_commit_message(&intent);
    assert!(
        msg.contains("chore(yarli): merge feature/branch into main"),
        "default template should use conventional commits: {msg}"
    );
    assert!(msg.contains(&run_id.to_string()));
    assert!(msg.contains(&wt_id.to_string()));
}

#[tokio::test]
async fn merge_conflict_from_precheck_allows_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main, _feature) = create_merge_test_repo(&repo).await;

    // Create conflicting change on main.
    std::fs::write(repo.join("feature.txt"), "main conflicting\n").unwrap();
    std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(&repo)
        .output()
        .unwrap();
    std::process::Command::new("git")
        .args(["commit", "-m", "conflict on main"])
        .current_dir(&repo)
        .output()
        .unwrap();

    let wt_mgr = LocalWorktreeManager::new();
    let cancel = CancellationToken::new();
    let run_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();
    let task_id = Uuid::now_v7();
    let wt_id = Uuid::now_v7();

    let mut binding = WorktreeBinding::new(
        run_id,
        &repo,
        format!("yarl/{}/retry-task", &run_id.to_string()[..8]),
        "main",
        corr_id,
    )
    .with_task(task_id);

    wt_mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let mut intent = MergeIntent::new(run_id, wt_id, "feature/test-merge", "main", corr_id);
    let orchestrator = LocalMergeOrchestrator::new(wt_mgr);

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();

    let dry = orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    assert!(!dry.clean);
    assert_eq!(intent.state, MergeState::MergeConflict);

    assert!(intent.state.can_transition_to(MergeState::MergePrecheck));

    orchestrator
        .precheck(&mut intent, &binding, cancel)
        .await
        .unwrap();
    assert_eq!(intent.state, MergeState::MergePrecheck);
}

#[test]
fn merge_parse_changed_files() {
    let files =
        LocalMergeOrchestrator::parse_changed_files("file1.txt\nfile2.rs\n\ndir/file3.md\n");
    assert_eq!(files, vec!["file1.txt", "file2.rs", "dir/file3.md"]);
}

#[test]
fn merge_parse_changed_files_empty() {
    let files = LocalMergeOrchestrator::parse_changed_files("");
    assert!(files.is_empty());

    let files = LocalMergeOrchestrator::parse_changed_files("\n\n");
    assert!(files.is_empty());
}

#[test]
fn merge_parse_conflicts() {
    let conflicts = LocalMergeOrchestrator::parse_conflicts("file1.txt\nfile2.rs\n");
    assert_eq!(conflicts.len(), 2);
    assert_eq!(conflicts[0].path, "file1.txt");
    assert_eq!(conflicts[1].path, "file2.rs");
    assert_eq!(
        conflicts[0].conflict_type,
        yarli_core::entities::merge_intent::ConflictType::Text
    );
}

// ── Submodule policy enforcement tests ─────────────────────────────────

use crate::submodule::{SubmoduleEntry, SubmoduleStatus};
use yarli_core::entities::worktree_binding::SubmoduleMode;

#[tokio::test]
async fn submodule_status_returns_empty_for_no_submodules() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let entries = mgr.submodule_status(&binding.worktree_path).await.unwrap();
    assert!(
        entries.is_empty(),
        "no submodules should produce empty list"
    );

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn check_uninitialized_submodules_empty_for_no_submodules() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let uninit = mgr
        .check_uninitialized_submodules(&binding.worktree_path)
        .await
        .unwrap();
    assert!(uninit.is_empty());

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn check_dirty_submodules_empty_for_no_submodules() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    create_test_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let dirty = mgr
        .check_dirty_submodules(&binding.worktree_path)
        .await
        .unwrap();
    assert!(dirty.is_empty());

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[test]
fn verify_submodule_policy_locked_rejects_change() {
    let mgr = LocalWorktreeManager::new();
    let before = vec![SubmoduleEntry {
        status: SubmoduleStatus::Current,
        sha: "a".repeat(40),
        path: "vendor/lib".into(),
        descriptor: None,
    }];
    let after = vec![SubmoduleEntry {
        status: SubmoduleStatus::Current,
        sha: "b".repeat(40),
        path: "vendor/lib".into(),
        descriptor: None,
    }];
    let result = mgr.verify_submodule_policy(SubmoduleMode::Locked, &before, &after);
    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::SubmodulePolicyViolation { path, .. } => {
            assert_eq!(path, "vendor/lib");
        }
        other => panic!("expected SubmodulePolicyViolation, got {other:?}"),
    }
}

#[test]
fn verify_submodule_policy_locked_allows_same() {
    let mgr = LocalWorktreeManager::new();
    let entries = vec![SubmoduleEntry {
        status: SubmoduleStatus::Current,
        sha: "a".repeat(40),
        path: "vendor/lib".into(),
        descriptor: None,
    }];
    assert!(mgr
        .verify_submodule_policy(SubmoduleMode::Locked, &entries, &entries)
        .is_ok());
}

#[test]
fn verify_submodule_policy_allow_any_allows_everything() {
    let mgr = LocalWorktreeManager::new();
    let before = vec![];
    let after = vec![SubmoduleEntry {
        status: SubmoduleStatus::Modified,
        sha: "b".repeat(40),
        path: "vendor/new".into(),
        descriptor: None,
    }];
    assert!(mgr
        .verify_submodule_policy(SubmoduleMode::AllowAny, &before, &after)
        .is_ok());
}

#[test]
fn verify_submodule_policy_allow_ff_rejects_conflict() {
    let mgr = LocalWorktreeManager::new();
    let before = vec![SubmoduleEntry {
        status: SubmoduleStatus::Current,
        sha: "a".repeat(40),
        path: "vendor/lib".into(),
        descriptor: None,
    }];
    let after = vec![SubmoduleEntry {
        status: SubmoduleStatus::Conflict,
        sha: "a".repeat(40),
        path: "vendor/lib".into(),
        descriptor: None,
    }];
    let result = mgr.verify_submodule_policy(SubmoduleMode::AllowFastForward, &before, &after);
    assert!(result.is_err());
}

#[tokio::test]
async fn merge_precheck_captures_submodule_snapshot() {
    let (_tmp, orchestrator, binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    let result = orchestrator
        .precheck(&mut intent, &binding, cancel)
        .await
        .unwrap();

    // No submodules in test repo — snapshot should be empty.
    assert!(result.submodule_snapshot.is_empty());
}

#[tokio::test]
async fn merge_verify_passes_with_no_submodules() {
    let (_tmp, orchestrator, mut binding, mut intent) = setup_merge_test().await;
    let cancel = CancellationToken::new();

    let precheck = orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .apply(&mut intent, &mut binding, cancel.clone())
        .await
        .unwrap();

    // Verify with empty before-snapshot — should pass since no submodules.
    orchestrator
        .verify(&mut intent, &binding, &precheck.submodule_snapshot, cancel)
        .await
        .unwrap();
    assert_eq!(intent.state, MergeState::MergeDone);
}

// ── Interrupted operation recovery integration tests ─────────────────────

/// Create a repo with conflicting changes on main and a feature branch.
/// Returns (main_branch, feature_branch).
async fn create_conflict_repo(dir: &Path) -> (String, String) {
    use std::process::Command;

    Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(dir)
        .output()
        .expect("git init");

    Command::new("git")
        .args(["config", "user.email", "test@yarli.dev"])
        .current_dir(dir)
        .output()
        .expect("git config email");

    Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(dir)
        .output()
        .expect("git config name");

    std::fs::write(dir.join("shared.txt"), "original content\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add");

    Command::new("git")
        .args(["commit", "-m", "initial commit"])
        .current_dir(dir)
        .output()
        .expect("git commit");

    // Create feature branch with conflicting change.
    Command::new("git")
        .args(["checkout", "-b", "feature/conflict"])
        .current_dir(dir)
        .output()
        .expect("git checkout -b feature");

    std::fs::write(dir.join("shared.txt"), "feature version\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add feature");

    Command::new("git")
        .args(["commit", "-m", "feature change"])
        .current_dir(dir)
        .output()
        .expect("git commit feature");

    // Back to main with a different change to same file.
    Command::new("git")
        .args(["checkout", "main"])
        .current_dir(dir)
        .output()
        .expect("git checkout main");

    std::fs::write(dir.join("shared.txt"), "main version\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add main");

    Command::new("git")
        .args(["commit", "-m", "main change"])
        .current_dir(dir)
        .output()
        .expect("git commit main");

    ("main".to_string(), "feature/conflict".to_string())
}

/// Helper: create a worktree from a conflict-repo and produce a real interrupted merge.
/// Returns the worktree git dir (for sentinel verification).
async fn create_interrupted_merge_in_worktree(
    _repo: &Path,
    binding: &mut WorktreeBinding,
    feature_branch: &str,
) -> PathBuf {
    use std::process::Command;

    let mgr = LocalWorktreeManager::new();
    let cancel = CancellationToken::new();
    mgr.create(binding, cancel).await.unwrap();

    let wt_path = &binding.worktree_path;

    // Start a merge that will conflict.
    let output = Command::new("git")
        .args(["merge", "--no-ff", feature_branch])
        .current_dir(wt_path)
        .output()
        .expect("git merge");

    // Merge should fail due to conflict.
    assert!(!output.status.success(), "merge should conflict");

    // Read the git dir path from the .git file.
    let content = std::fs::read_to_string(wt_path.join(".git")).unwrap();
    let gitdir = content.trim().strip_prefix("gitdir: ").unwrap();
    PathBuf::from(gitdir)
}

#[tokio::test]
async fn recover_abort_real_interrupted_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main_branch, feature_branch) = create_conflict_repo(&repo).await;

    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    let git_dir = create_interrupted_merge_in_worktree(&repo, &mut binding, &feature_branch).await;

    // Verify MERGE_HEAD exists (real interrupted merge).
    assert!(git_dir.join(MERGE_HEAD_FILE).exists());

    // Detect should find the merge.
    let mgr = LocalWorktreeManager::new();
    let detected = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert_eq!(detected, Some(InterruptedOp::Merge));

    // Transition to WtMerging (simulate that the orchestrator set this).
    binding
        .transition(WorktreeState::WtMerging, "merge started", "test", None)
        .unwrap();

    // Recover via abort.
    mgr.recover(&mut binding, RecoveryAction::Abort, cancel.clone())
        .await
        .unwrap();

    // Binding should be back to WtBoundHome.
    assert_eq!(binding.state, WorktreeState::WtBoundHome);

    // MERGE_HEAD should be gone.
    assert!(!git_dir.join(MERGE_HEAD_FILE).exists());

    // Worktree should be clean.
    assert!(!mgr.is_dirty(&binding.worktree_path).await.unwrap());

    // HEAD should be a valid SHA.
    assert_eq!(binding.head_ref.len(), 40);

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn recover_resume_real_interrupted_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main_branch, feature_branch) = create_conflict_repo(&repo).await;

    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    let git_dir = create_interrupted_merge_in_worktree(&repo, &mut binding, &feature_branch).await;

    // Verify MERGE_HEAD exists.
    assert!(git_dir.join(MERGE_HEAD_FILE).exists());

    // Resolve the conflict manually.
    std::fs::write(
        binding.worktree_path.join("shared.txt"),
        "resolved content\n",
    )
    .unwrap();
    let output = std::process::Command::new("git")
        .args(["add", "shared.txt"])
        .current_dir(&binding.worktree_path)
        .output()
        .expect("git add resolved");
    assert!(output.status.success());

    // Transition to WtMerging.
    binding
        .transition(WorktreeState::WtMerging, "merge started", "test", None)
        .unwrap();

    let mgr = LocalWorktreeManager::new();

    // Recover via resume (commit the resolved merge).
    mgr.recover(&mut binding, RecoveryAction::Resume, cancel.clone())
        .await
        .unwrap();

    // Binding should be back to WtBoundHome.
    assert_eq!(binding.state, WorktreeState::WtBoundHome);

    // MERGE_HEAD should be gone (merge committed).
    assert!(!git_dir.join(MERGE_HEAD_FILE).exists());

    // Worktree should be clean.
    assert!(!mgr.is_dirty(&binding.worktree_path).await.unwrap());

    // HEAD should point to the new merge commit.
    assert_eq!(binding.head_ref.len(), 40);

    // Verify the merge commit exists with correct content.
    let content = std::fs::read_to_string(binding.worktree_path.join("shared.txt")).unwrap();
    assert_eq!(content, "resolved content\n");

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn recover_abort_from_wt_conflict_state() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main_branch, feature_branch) = create_conflict_repo(&repo).await;

    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    let git_dir = create_interrupted_merge_in_worktree(&repo, &mut binding, &feature_branch).await;

    // Transition to WtMerging then WtConflict (as the merge orchestrator would).
    binding
        .transition(WorktreeState::WtMerging, "merge started", "test", None)
        .unwrap();
    binding
        .transition(
            WorktreeState::WtConflict,
            "conflicts detected",
            "test",
            None,
        )
        .unwrap();

    let mgr = LocalWorktreeManager::new();

    // Recover from WtConflict via abort.
    mgr.recover(&mut binding, RecoveryAction::Abort, cancel.clone())
        .await
        .unwrap();

    assert_eq!(binding.state, WorktreeState::WtBoundHome);
    assert!(!git_dir.join(MERGE_HEAD_FILE).exists());
    assert!(!mgr.is_dirty(&binding.worktree_path).await.unwrap());

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

/// Helper: create a repo with a commit suitable for cherry-pick/revert conflicts.
async fn create_cherry_pick_repo(dir: &Path) -> String {
    use std::process::Command;

    Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(dir)
        .output()
        .expect("git init");

    Command::new("git")
        .args(["config", "user.email", "test@yarli.dev"])
        .current_dir(dir)
        .output()
        .expect("git config email");

    Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(dir)
        .output()
        .expect("git config name");

    std::fs::write(dir.join("shared.txt"), "original content\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add");

    Command::new("git")
        .args(["commit", "-m", "initial commit"])
        .current_dir(dir)
        .output()
        .expect("git commit");

    // Create a branch with a change.
    Command::new("git")
        .args(["checkout", "-b", "feature/pick"])
        .current_dir(dir)
        .output()
        .expect("checkout branch");

    std::fs::write(dir.join("shared.txt"), "feature pick content\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add");

    Command::new("git")
        .args(["commit", "-m", "feature pick change"])
        .current_dir(dir)
        .output()
        .expect("git commit");

    // Get the SHA of this commit.
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(dir)
        .output()
        .expect("rev-parse");
    let pick_sha = String::from_utf8_lossy(&output.stdout).trim().to_string();

    // Back to main, make a conflicting change.
    Command::new("git")
        .args(["checkout", "main"])
        .current_dir(dir)
        .output()
        .expect("checkout main");

    std::fs::write(dir.join("shared.txt"), "main conflicting content\n").unwrap();

    Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .expect("git add");

    Command::new("git")
        .args(["commit", "-m", "main conflicting change"])
        .current_dir(dir)
        .output()
        .expect("git commit");

    pick_sha
}

#[tokio::test]
async fn recover_abort_real_interrupted_cherry_pick() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let pick_sha = create_cherry_pick_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Start a cherry-pick that will conflict.
    let output = std::process::Command::new("git")
        .args(["cherry-pick", &pick_sha])
        .current_dir(&binding.worktree_path)
        .output()
        .expect("git cherry-pick");
    assert!(!output.status.success(), "cherry-pick should conflict");

    // Detect should find cherry-pick.
    let detected = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert_eq!(detected, Some(InterruptedOp::CherryPick));

    // Transition to a state that allows WtRecovering.
    binding
        .transition(
            WorktreeState::WtMerging,
            "cherry-pick started",
            "test",
            None,
        )
        .unwrap();

    // Abort the cherry-pick.
    mgr.recover(&mut binding, RecoveryAction::Abort, cancel.clone())
        .await
        .unwrap();

    assert_eq!(binding.state, WorktreeState::WtBoundHome);

    // CHERRY_PICK_HEAD should be gone.
    let git_dir = {
        let content = std::fs::read_to_string(binding.worktree_path.join(".git")).unwrap();
        let gitdir = content.trim().strip_prefix("gitdir: ").unwrap();
        PathBuf::from(gitdir)
    };
    assert!(!git_dir.join(CHERRY_PICK_HEAD_FILE).exists());

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn recover_abort_real_interrupted_revert() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();

    // Use cherry_pick_repo — we'll revert the main-branch commit while on the feature branch
    // to create a conflict.
    let _pick_sha = create_cherry_pick_repo(&repo).await;

    let mgr = LocalWorktreeManager::new();
    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    mgr.create(&mut binding, cancel.clone()).await.unwrap();

    // Get the HEAD commit SHA (the "main conflicting change" commit).
    let head_sha = mgr.resolve_head(&binding.worktree_path).await.unwrap();

    // Create a new commit on top so we have something to revert that will conflict.
    std::fs::write(
        binding.worktree_path.join("shared.txt"),
        "yet another version\n",
    )
    .unwrap();
    let output = std::process::Command::new("git")
        .args(["add", "shared.txt"])
        .current_dir(&binding.worktree_path)
        .output()
        .expect("git add");
    assert!(output.status.success());
    let output = std::process::Command::new("git")
        .args(["commit", "-m", "another change"])
        .current_dir(&binding.worktree_path)
        .output()
        .expect("git commit");
    assert!(output.status.success());

    // Now revert the HEAD~ commit (the conflicting one) — this should conflict
    // because the file was changed again in the latest commit.
    let output = std::process::Command::new("git")
        .args(["revert", "--no-edit", &head_sha])
        .current_dir(&binding.worktree_path)
        .output()
        .expect("git revert");

    if output.status.success() {
        // If revert succeeded (no conflict), the test setup didn't produce a conflict.
        // This can happen depending on git version. Skip by cleaning up.
        mgr.cleanup(&mut binding, true, cancel).await.unwrap();
        return;
    }

    // Detect should find revert.
    let detected = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert_eq!(detected, Some(InterruptedOp::Revert));

    // Transition to a state that can reach WtRecovering.
    binding
        .transition(WorktreeState::WtMerging, "revert started", "test", None)
        .unwrap();

    // Abort the revert.
    mgr.recover(&mut binding, RecoveryAction::Abort, cancel.clone())
        .await
        .unwrap();

    assert_eq!(binding.state, WorktreeState::WtBoundHome);

    // REVERT_HEAD should be gone.
    let git_dir = {
        let content = std::fs::read_to_string(binding.worktree_path.join(".git")).unwrap();
        let gitdir = content.trim().strip_prefix("gitdir: ").unwrap();
        PathBuf::from(gitdir)
    };
    assert!(!git_dir.join(REVERT_HEAD_FILE).exists());

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn detect_then_recover_restart_flow() {
    // Simulates what happens on restart: detect interrupted op → choose action → recover.
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main_branch, feature_branch) = create_conflict_repo(&repo).await;

    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    create_interrupted_merge_in_worktree(&repo, &mut binding, &feature_branch).await;

    // On restart, the orchestrator would:
    // 1. Detect interrupted operation.
    let mgr = LocalWorktreeManager::new();
    let detected = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert!(detected.is_some());
    let op = detected.unwrap();
    assert_eq!(op, InterruptedOp::Merge);

    // 2. Transition to WtRecovering (from whatever state we're in).
    //    In a real restart, the binding would be loaded from persisted state.
    //    The worktree was in WtBoundHome (pre-merge) — but the orchestrator would
    //    transition through WtMerging to allow WtRecovering.
    binding
        .transition(
            WorktreeState::WtMerging,
            "detected merge on restart",
            "test",
            None,
        )
        .unwrap();

    // 3. Choose recovery action based on policy (here: abort).
    mgr.recover(&mut binding, RecoveryAction::Abort, cancel.clone())
        .await
        .unwrap();

    // 4. Verify clean state.
    assert_eq!(binding.state, WorktreeState::WtBoundHome);
    assert!(!mgr.is_dirty(&binding.worktree_path).await.unwrap());

    // 5. Verify no interrupted operation remains.
    let after = mgr
        .detect_interrupted(&binding.worktree_path)
        .await
        .unwrap();
    assert!(after.is_none());

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn recover_abort_updates_head_ref() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main_branch, feature_branch) = create_conflict_repo(&repo).await;

    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    create_interrupted_merge_in_worktree(&repo, &mut binding, &feature_branch).await;

    binding
        .transition(WorktreeState::WtMerging, "merge started", "test", None)
        .unwrap();

    let mgr = LocalWorktreeManager::new();
    mgr.recover(&mut binding, RecoveryAction::Abort, cancel.clone())
        .await
        .unwrap();

    // After abort, HEAD should still be a valid 40-char SHA.
    assert_eq!(binding.head_ref.len(), 40);
    assert!(binding.head_ref.chars().all(|c| c.is_ascii_hexdigit()));

    // The binding.head_ref is updated by recover() and should match actual git HEAD.
    let actual_head = mgr.resolve_head(&binding.worktree_path).await.unwrap();
    assert_eq!(binding.head_ref, actual_head);

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn recover_resume_updates_head_to_merge_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main_branch, feature_branch) = create_conflict_repo(&repo).await;

    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    create_interrupted_merge_in_worktree(&repo, &mut binding, &feature_branch).await;

    let pre_merge_head = binding.head_ref.clone();

    // Resolve conflicts.
    std::fs::write(binding.worktree_path.join("shared.txt"), "resolved\n").unwrap();
    let output = std::process::Command::new("git")
        .args(["add", "shared.txt"])
        .current_dir(&binding.worktree_path)
        .output()
        .expect("git add");
    assert!(output.status.success());

    binding
        .transition(WorktreeState::WtMerging, "merge started", "test", None)
        .unwrap();

    let mgr = LocalWorktreeManager::new();
    mgr.recover(&mut binding, RecoveryAction::Resume, cancel.clone())
        .await
        .unwrap();

    // After resume (commit), HEAD should advance to a new merge commit.
    assert_eq!(binding.head_ref.len(), 40);
    assert_ne!(
        binding.head_ref, pre_merge_head,
        "HEAD should advance after merge commit"
    );

    // The merge commit should have 2 parents.
    let output = std::process::Command::new("git")
        .args(["cat-file", "-p", &binding.head_ref])
        .current_dir(&binding.worktree_path)
        .output()
        .expect("git cat-file");
    let commit_info = String::from_utf8_lossy(&output.stdout);
    let parent_count = commit_info
        .lines()
        .filter(|l| l.starts_with("parent "))
        .count();
    assert_eq!(parent_count, 2, "merge commit should have 2 parents");

    mgr.cleanup(&mut binding, true, cancel).await.unwrap();
}

#[tokio::test]
async fn recover_manual_block_preserves_interrupted_state() {
    // Verify that ManualBlock leaves the repo in the interrupted state
    // (sentinel files remain).
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (_main_branch, feature_branch) = create_conflict_repo(&repo).await;

    let mut binding = make_binding(&repo);
    let cancel = CancellationToken::new();

    let git_dir = create_interrupted_merge_in_worktree(&repo, &mut binding, &feature_branch).await;

    binding
        .transition(WorktreeState::WtMerging, "merge started", "test", None)
        .unwrap();

    let mgr = LocalWorktreeManager::new();
    let result = mgr
        .recover(&mut binding, RecoveryAction::ManualBlock, cancel.clone())
        .await;

    // Should return error.
    assert!(result.is_err());
    match result.unwrap_err() {
        GitError::InterruptedOperation { operation, .. } => {
            assert_eq!(operation, InterruptedOp::Merge);
        }
        other => panic!("expected InterruptedOperation, got {other:?}"),
    }

    // Binding should be in WtRecovering (transitioned but not resolved).
    assert_eq!(binding.state, WorktreeState::WtRecovering);

    // MERGE_HEAD should still exist (ManualBlock doesn't touch git state).
    assert!(git_dir.join(MERGE_HEAD_FILE).exists());

    // Clean up for the test (abort the merge, then cleanup worktree).
    std::process::Command::new("git")
        .args(["merge", "--abort"])
        .current_dir(&binding.worktree_path)
        .output()
        .expect("git merge --abort");

    binding
        .transition(
            WorktreeState::WtCleanupPending,
            "manual cleanup",
            "test",
            None,
        )
        .unwrap();
    binding
        .transition(WorktreeState::WtClosed, "closed", "test", None)
        .unwrap();
}

// ── Hook rejection detection tests ─────────────────────────────────────

#[test]
fn is_hook_rejection_detects_commit_msg_hook() {
    let stderr = "[commit-msg] Subject does not follow Conventional Commits format.\nNot committing merge; use 'git commit' to complete the merge.";
    assert!(LocalMergeOrchestrator::is_hook_rejection(stderr));
}

#[test]
fn is_hook_rejection_detects_pre_commit_hook() {
    let stderr = "pre-commit hook failed (add --no-verify to bypass)";
    assert!(LocalMergeOrchestrator::is_hook_rejection(stderr));
}

#[test]
fn is_hook_rejection_detects_generic_hook_mention() {
    let stderr = "error: hook declined to update refs/heads/main";
    assert!(LocalMergeOrchestrator::is_hook_rejection(stderr));
}

#[test]
fn is_hook_rejection_false_for_real_conflict() {
    let stderr = "CONFLICT (content): Merge conflict in src/main.rs\nAutomatic merge failed; fix conflicts and then commit the result.";
    assert!(!LocalMergeOrchestrator::is_hook_rejection(stderr));
}

#[test]
fn detect_hook_name_finds_commit_msg() {
    let stderr = "[commit-msg] Subject line too long";
    assert_eq!(
        LocalMergeOrchestrator::detect_hook_name(stderr),
        "commit-msg"
    );
}

#[test]
fn detect_hook_name_finds_pre_commit() {
    let stderr = "pre-commit hook failed";
    assert_eq!(
        LocalMergeOrchestrator::detect_hook_name(stderr),
        "pre-commit"
    );
}

#[test]
fn detect_hook_name_falls_back_to_unknown() {
    let stderr = "some hook error occurred";
    assert_eq!(
        LocalMergeOrchestrator::detect_hook_name(stderr),
        "unknown"
    );
}

// ── Custom commit message template tests ───────────────────────────────

#[test]
fn build_commit_message_with_custom_template() {
    let run_id = Uuid::now_v7();
    let wt_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();

    let intent = MergeIntent::new(run_id, wt_id, "feature/auth", "main", corr_id)
        .with_commit_template("fix: merge {source} into {target}");

    let msg = LocalMergeOrchestrator::build_commit_message(&intent);
    assert!(
        msg.starts_with("fix: merge feature/auth into main"),
        "custom template should be used: {msg}"
    );
    assert!(!msg.contains("yarli-run:"));
}

#[test]
fn build_commit_message_custom_template_with_all_placeholders() {
    let run_id = Uuid::now_v7();
    let wt_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();

    let intent = MergeIntent::new(run_id, wt_id, "dev", "main", corr_id).with_commit_template(
        "feat(merge): {source} → {target}\n\nrun={run_id} task={task_id}",
    );

    let msg = LocalMergeOrchestrator::build_commit_message(&intent);
    assert!(msg.contains(&run_id.to_string()));
    assert!(msg.contains(&wt_id.to_string()));
    assert!(msg.starts_with("feat(merge): dev → main"));
}

// ── Integration test: hook rejection during merge apply ────────────────

#[tokio::test]
async fn merge_apply_hook_rejection_returns_hook_error() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().to_path_buf();
    let (main_branch, feature_branch) = create_merge_test_repo(&repo).await;

    let wt_mgr = LocalWorktreeManager::new();
    let cancel = CancellationToken::new();
    let run_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();
    let task_id = Uuid::now_v7();
    let wt_id = Uuid::now_v7();

    let mut binding = WorktreeBinding::new(
        run_id,
        &repo,
        format!("yarl/{}/hook-test", &run_id.to_string()[..8]),
        "main",
        corr_id,
    )
    .with_task(task_id);

    wt_mgr.create(&mut binding, cancel.clone()).await.unwrap();

    let mut intent = MergeIntent::new(run_id, wt_id, &feature_branch, &main_branch, corr_id);

    let orchestrator = LocalMergeOrchestrator::new(wt_mgr);

    orchestrator
        .precheck(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();
    orchestrator
        .dry_run(&mut intent, &binding, cancel.clone())
        .await
        .unwrap();

    // Install a commit-msg hook in the MAIN repo's .git/hooks/.
    // Git worktrees inherit hooks from the main repo, not from the
    // worktree-specific gitdir.
    let hooks_dir = repo.join(".git").join("hooks");
    std::fs::create_dir_all(&hooks_dir).unwrap();
    let hook_path = hooks_dir.join("commit-msg");
    std::fs::write(
        &hook_path,
        "#!/bin/sh\necho '[commit-msg] Subject does not follow Conventional Commits' >&2\nexit 1\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hook_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Apply should fail with HookRejected, not MergeConflict.
    let err = orchestrator
        .apply(&mut intent, &mut binding, cancel.clone())
        .await
        .expect_err("apply should fail due to hook");

    match &err {
        GitError::HookRejected { hook, stderr } => {
            assert_eq!(hook, "commit-msg");
            assert!(
                stderr.contains("Conventional Commits"),
                "stderr should contain hook message: {stderr}"
            );
        }
        other => panic!("expected HookRejected, got {other:?}"),
    }

    // Worktree should be back to WtBoundHome, not WtConflict.
    assert_eq!(
        binding.state,
        WorktreeState::WtBoundHome,
        "hook rejection should recover worktree to WtBoundHome, not WtConflict"
    );
}
