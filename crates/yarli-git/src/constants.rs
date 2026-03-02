//! Constants for yarli-git: path conventions, branch naming, sentinel files,
//! and configuration defaults (Sections 12.1–12.12).

use std::time::Duration;

// ── Worktree path conventions (Section 12.1) ────────────────────────────

/// Subdirectory under repo root for YARLI-managed worktrees.
/// Full path: `${repo_root}/.yarl/worktrees/{run_id}-{task_id_short}`
pub const WORKTREE_DIR: &str = ".yarl/worktrees";

/// Branch prefix for YARLI-created branches.
/// Full pattern: `yarl/{run_id}/{task_slug}`
pub const BRANCH_PREFIX: &str = "yarl/";

// ── Git sentinel files for interrupted operation detection (Section 12.10) ──

/// Sentinel file indicating an in-progress merge.
pub const MERGE_HEAD_FILE: &str = "MERGE_HEAD";

/// Sentinel file indicating an in-progress cherry-pick.
pub const CHERRY_PICK_HEAD_FILE: &str = "CHERRY_PICK_HEAD";

/// Sentinel file indicating an in-progress revert.
pub const REVERT_HEAD_FILE: &str = "REVERT_HEAD";

/// Directory indicating an in-progress rebase (merge-based).
pub const REBASE_MERGE_DIR: &str = "rebase-merge";

/// Directory indicating an in-progress rebase (apply-based).
pub const REBASE_APPLY_DIR: &str = "rebase-apply";

/// The `.git` file in a worktree (indirection to main repo's gitdir).
pub const DOT_GIT_FILE: &str = ".git";

// ── Merge commit template (Section 12.5) ─────────────────────────────────

/// Default merge commit message template (Conventional Commits format).
/// Placeholders:
/// - `{source}`: source branch/ref
/// - `{target}`: target branch/ref
/// - `{run_id}`: YARLI run ID
/// - `{task_id}`: YARLI task ID (if applicable)
pub const MERGE_COMMIT_TEMPLATE: &str =
    "chore(yarli): merge {source} into {target}\n\nyarli-run: {run_id}\nyarli-task: {task_id}";

// ── Timeouts and limits ──────────────────────────────────────────────────

/// Default timeout for individual git commands.
pub const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

/// Maximum time to wait for a merge lock acquisition.
pub const MERGE_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum time to wait for worktree creation (includes submodule init).
pub const WORKTREE_CREATE_TIMEOUT: Duration = Duration::from_secs(300);

/// Grace period before force-killing a git child process.
pub const GIT_KILL_GRACE: Duration = Duration::from_secs(5);

// ── Forbidden command patterns (Section 12.12) ───────────────────────────

/// Git subcommands that are forbidden by default (require policy approval).
pub const FORBIDDEN_SUBCOMMANDS: &[&str] = &[
    "push",
    "tag",
    "stash clear",
    "stash drop --all",
    "clean -fdx",
    "clean -fd",
];

/// Arguments that upgrade a command to forbidden status.
pub const FORBIDDEN_ARGS: &[&str] = &["--force", "--force-with-lease", "-D"];

// ── Short ID length ──────────────────────────────────────────────────────

/// Number of hex characters for short task ID in worktree path names.
/// e.g. `run_id-abcdef12` where `abcdef12` is the first 8 chars of task UUID.
pub const SHORT_ID_LEN: usize = 8;

// ── Submodule constants (Section 12.4) ────────────────────────────────────

/// Git submodule status prefix for uninitialized submodule.
pub const SUBMODULE_UNINIT_PREFIX: char = '-';

/// Git submodule status prefix for modified (dirty) submodule.
pub const SUBMODULE_MODIFIED_PREFIX: char = '+';

/// Git submodule status prefix for merge conflict in submodule.
pub const SUBMODULE_CONFLICT_PREFIX: char = 'U';
