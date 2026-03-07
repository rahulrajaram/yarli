use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Write as IoWrite;
use std::path::{Component, Path, PathBuf};
use std::process::{self, Stdio};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Serialize;
use tracing::{info, warn};
use uuid::Uuid;
use yarli_git::{generate_commit_message, render_commit_message, DiffSpec};

use crate::config;
use crate::config::LoadedConfig;
use crate::plan::sanitize_task_key_component;
use crate::plan::RunPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParallelWorkspaceMode {
    FileCopy,
    GitWorktree,
}

#[derive(Debug, Clone)]
pub(crate) struct ParallelWorkspaceLayout {
    pub(crate) run_workspace_root: PathBuf,
    pub(crate) task_workspace_dirs: Vec<PathBuf>,
    /// Source HEAD captured at workspace creation time. Used for merge
    /// ancestry validation so we compare against a commit that is guaranteed
    /// to exist in the copied workspace's object database.
    pub(crate) source_head_at_creation: Option<String>,
    /// How the parallel workspaces were created.
    pub(crate) mode: ParallelWorkspaceMode,
    /// Branch names for each task workspace (only populated in GitWorktree mode).
    pub(crate) worktree_branches: Vec<String>,
    /// Source working directory (needed for worktree cleanup).
    pub(crate) source_workdir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub(crate) struct ParallelWorkspaceMergeReport {
    pub(crate) merged_task_keys: Vec<String>,
    pub(crate) skipped_task_keys: Vec<String>,
    pub(crate) task_outcomes: Vec<ParallelTaskMergeOutcome>,
    pub(crate) preserve_workspace_root: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MergeApplyTelemetryEvent {
    pub(crate) event_type: String,
    pub(crate) task_key: Option<String>,
    pub(crate) task_index: Option<usize>,
    pub(crate) metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParallelTaskMergeDisposition {
    Merged,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParallelTaskSkipReason {
    NoScopedPaths,
    EmptyPatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ParallelTaskMergeOutcome {
    pub(crate) task_key: String,
    pub(crate) disposition: ParallelTaskMergeDisposition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) skip_reason: Option<ParallelTaskSkipReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) workspace_head: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_head: Option<String>,
    pub(crate) soft_reset_applied: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) patch_path: Option<String>,
}

/// Bundles resolution strategy with associated parameters for LLM-assisted repair.
#[derive(Debug, Clone)]
pub(crate) struct MergeResolutionConfig {
    pub(crate) strategy: config::MergeConflictResolution,
    pub(crate) repair_command: Option<String>,
    pub(crate) repair_timeout_seconds: u64,
}

impl MergeResolutionConfig {
    pub(crate) fn from_run_config(run: &config::RunConfig) -> Self {
        Self {
            strategy: run.merge_conflict_resolution,
            repair_command: run.merge_repair_command.clone(),
            repair_timeout_seconds: run.merge_repair_timeout_seconds,
        }
    }
}

pub(crate) fn path_within_any_root(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path.starts_with(root))
}

pub(crate) fn path_matches_excluded_dir_name(
    path: &Path,
    file_type: &fs::FileType,
    dir_names: &[String],
) -> bool {
    if dir_names.is_empty() {
        return false;
    }

    let mut path_components = Vec::new();
    for component in path.components() {
        if let Component::Normal(name) = component {
            path_components.push(name.to_string_lossy().to_string());
        }
    }
    if path_components.is_empty() {
        return false;
    }

    for excluded in dir_names {
        if excluded.is_empty() {
            continue;
        }
        if file_type.is_dir()
            && path_components
                .last()
                .map(|part| part == excluded)
                .unwrap_or(false)
        {
            return true;
        }
        if path_components[..path_components.len().saturating_sub(1)]
            .iter()
            .any(|part| part == excluded)
        {
            return true;
        }
    }
    false
}

pub(crate) fn resolve_workspace_copy_exclusions(
    source_workdir: &Path,
    loaded_config: &LoadedConfig,
) -> (Vec<PathBuf>, Vec<String>) {
    let mut excluded_roots = Vec::new();
    let mut excluded_dir_names = Vec::new();
    for raw in &loaded_config.config().execution.worktree_exclude_paths {
        let trimmed = raw.trim().trim_end_matches('/').trim_end_matches('\\');
        if trimmed.is_empty() {
            continue;
        }
        if let Some(name) = trimmed.strip_prefix("**/") {
            if !name.is_empty() && !name.contains('/') && !name.contains('\\') {
                excluded_dir_names.push(name.to_string());
                continue;
            }
        }
        if !trimmed.contains('/') && !trimmed.contains('\\') {
            excluded_dir_names.push(trimmed.to_string());
            continue;
        }
        let path = Path::new(trimmed);
        let resolved = if path.is_absolute() {
            path.to_path_buf()
        } else {
            source_workdir.join(path)
        };
        excluded_roots.push(resolved);
    }

    (excluded_roots, excluded_dir_names)
}

pub(crate) fn copy_workspace_tree(
    src_root: &Path,
    dst_root: &Path,
    excluded_roots: &[PathBuf],
    excluded_dir_names: &[String],
) -> Result<()> {
    if dst_root.exists() {
        bail!(
            "workspace destination already exists: {}",
            dst_root.display()
        );
    }
    fs::create_dir_all(dst_root).with_context(|| {
        format!(
            "failed to create workspace directory {}",
            dst_root.display()
        )
    })?;
    copy_workspace_tree_recursive(
        src_root,
        src_root,
        dst_root,
        excluded_roots,
        excluded_dir_names,
    )
}

pub(crate) fn copy_workspace_tree_recursive(
    src_root: &Path,
    current: &Path,
    dst_root: &Path,
    excluded_roots: &[PathBuf],
    excluded_dir_names: &[String],
) -> Result<()> {
    for entry in fs::read_dir(current)
        .with_context(|| format!("failed to read directory {}", current.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry in {}", current.display()))?;
        let src_path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type for {}", src_path.display()))?;
        if path_within_any_root(&src_path, excluded_roots) {
            continue;
        }
        let relative = src_path.strip_prefix(src_root).with_context(|| {
            format!(
                "failed to compute relative path for {} from {}",
                src_path.display(),
                src_root.display()
            )
        })?;
        if path_matches_excluded_dir_name(relative, &file_type, excluded_dir_names) {
            continue;
        }
        let dst_path = dst_root.join(relative);

        if file_type.is_dir() {
            fs::create_dir_all(&dst_path)
                .with_context(|| format!("failed to create directory {}", dst_path.display()))?;
            copy_workspace_tree_recursive(
                src_root,
                &src_path,
                dst_root,
                excluded_roots,
                excluded_dir_names,
            )?;
        } else if file_type.is_file() {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create directory {}", parent.display()))?;
            }
            fs::copy(&src_path, &dst_path).with_context(|| {
                format!(
                    "failed to copy file {} -> {}",
                    src_path.display(),
                    dst_path.display()
                )
            })?;
        } else if file_type.is_symlink() {
            copy_symlink_entry(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn copy_symlink_entry(src_path: &Path, dst_path: &Path) -> Result<()> {
    let target = fs::read_link(src_path)
        .with_context(|| format!("failed to read symlink target {}", src_path.display()))?;
    if let Some(parent) = dst_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    std::os::unix::fs::symlink(&target, dst_path).with_context(|| {
        format!(
            "failed to create symlink {} -> {}",
            dst_path.display(),
            target.display()
        )
    })?;
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn copy_symlink_entry(src_path: &Path, _dst_path: &Path) -> Result<()> {
    bail!(
        "workspace source contains symlink {} but this platform does not support symlink cloning",
        src_path.display()
    )
}

pub(crate) fn prepare_parallel_workspace_layout(
    plan: &RunPlan,
    loaded_config: &LoadedConfig,
) -> Result<Option<ParallelWorkspaceLayout>> {
    if !loaded_config.config().features.parallel {
        return Ok(None);
    }

    let use_worktrees = loaded_config.config().features.parallel_worktree;
    if use_worktrees {
        let source_workdir =
            config::resolve_execution_path_from_cwd(&plan.workdir, "execution.working_dir")?;
        if source_workdir.is_dir() && git_worktree_available(&source_workdir) {
            return prepare_parallel_workspace_layout_worktree(plan, loaded_config);
        }
        warn!("git worktrees unavailable; falling back to file-copy mode");
    }

    prepare_parallel_workspace_layout_copy(plan, loaded_config)
}

fn prepare_parallel_workspace_layout_copy(
    plan: &RunPlan,
    loaded_config: &LoadedConfig,
) -> Result<Option<ParallelWorkspaceLayout>> {
    let configured_root =
        config::configured_parallel_worktree_root(loaded_config).ok_or_else(|| {
            anyhow::anyhow!(
            "`yarli run` requires `[execution].worktree_root` when `[features].parallel = true`"
        )
        })?;
    let source_workdir =
        config::resolve_execution_path_from_cwd(&plan.workdir, "execution.working_dir")?;
    if !source_workdir.is_dir() {
        bail!(
            "execution working_dir is not a directory: {}",
            source_workdir.display()
        );
    }
    let source_workdir = source_workdir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize execution working_dir {}",
            source_workdir.display()
        )
    })?;

    let worktree_root =
        config::resolve_execution_path_from_cwd(&configured_root, "execution.worktree_root")?;
    fs::create_dir_all(&worktree_root).with_context(|| {
        format!(
            "failed to create execution.worktree_root {}",
            worktree_root.display()
        )
    })?;
    let worktree_root = worktree_root.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize execution.worktree_root {}",
            worktree_root.display()
        )
    })?;
    if worktree_root == source_workdir {
        bail!(
            "execution.worktree_root must not equal execution.working_dir ({}); choose a dedicated workspace root",
            worktree_root.display()
        );
    }

    let run_workspace_root = worktree_root.join(format!("run-{}", Uuid::now_v7()));
    fs::create_dir_all(&run_workspace_root).with_context(|| {
        format!(
            "failed to create run workspace root {}",
            run_workspace_root.display()
        )
    })?;

    let mut excluded_roots = Vec::new();
    if worktree_root.starts_with(&source_workdir) {
        excluded_roots.push(worktree_root.clone());
    }
    let (mut configured_excluded_roots, excluded_dir_names) =
        resolve_workspace_copy_exclusions(&source_workdir, loaded_config);
    excluded_roots.append(&mut configured_excluded_roots);

    // Capture source HEAD now so merge validation uses a commit the workspaces
    // are guaranteed to have (the source may advance between now and merge time).
    let source_head_at_creation = run_git_capture(&source_workdir, &["rev-parse", "HEAD"])
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());

    let mut task_workspace_dirs = Vec::with_capacity(plan.tasks.len());
    for (index, task) in plan.tasks.iter().enumerate() {
        let slug = sanitize_task_key_component(&task.task_key);
        let slug = if slug.is_empty() {
            format!("task-{}", index + 1)
        } else {
            slug
        };
        let workspace_dir = run_workspace_root.join(format!("{:03}-{slug}", index + 1));
        copy_workspace_tree(
            &source_workdir,
            &workspace_dir,
            &excluded_roots,
            &excluded_dir_names,
        )?;
        task_workspace_dirs.push(workspace_dir);
    }

    Ok(Some(ParallelWorkspaceLayout {
        run_workspace_root,
        task_workspace_dirs,
        source_head_at_creation,
        mode: ParallelWorkspaceMode::FileCopy,
        worktree_branches: Vec::new(),
        source_workdir: Some(source_workdir),
    }))
}

/// Maximum number of git worktrees allowed per project.
const MAX_WORKTREES: usize = 10;
const WORKTREE_SLOT_WAIT_INITIAL_SECONDS: u64 = 1;
const WORKTREE_SLOT_WAIT_MAX_SECONDS: u64 = 30;

/// Check if `git worktree` is available in the given directory.
pub(crate) fn git_worktree_available(cwd: &Path) -> bool {
    run_git_capture(cwd, &["worktree", "list"])
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Count existing worktrees (excluding the main worktree).
pub(crate) fn count_existing_worktrees(cwd: &Path) -> Result<usize> {
    let output = run_git_capture(cwd, &["worktree", "list", "--porcelain"])?;
    ensure_git_success(
        output.clone(),
        cwd,
        &["worktree", "list", "--porcelain"],
        "count existing worktrees",
    )?;
    let text = String::from_utf8_lossy(&output.stdout);
    // Each worktree block starts with "worktree <path>". The first is always main.
    let count = text.lines().filter(|l| l.starts_with("worktree ")).count();
    Ok(count.saturating_sub(1))
}

/// Prune stale worktree metadata.
pub(crate) fn prune_stale_worktrees(cwd: &Path) -> Result<()> {
    let output = run_git_capture(cwd, &["worktree", "prune"])?;
    ensure_git_success(output, cwd, &["worktree", "prune"], "prune stale worktrees")?;
    Ok(())
}

/// Wait until enough worktree slots are available for `needed` worktrees.
fn wait_for_available_worktree_slots(source_workdir: &Path, needed: usize) -> Result<usize> {
    if needed > MAX_WORKTREES {
        bail!(
            "creating {} worktrees would exceed the maximum of {}; reduce parallel task count",
            needed,
            MAX_WORKTREES
        );
    }

    let mut wait_seconds = WORKTREE_SLOT_WAIT_INITIAL_SECONDS;
    loop {
        prune_stale_worktrees(source_workdir)?;
        let existing = count_existing_worktrees(source_workdir)?;
        if existing + needed <= MAX_WORKTREES {
            return Ok(existing);
        }

        warn!(
            existing_worktrees = existing,
            requested_worktrees = needed,
            max_worktrees = MAX_WORKTREES,
            wait_seconds,
            "worktree limit reached, waiting for slots to free"
        );
        std::thread::sleep(Duration::from_secs(wait_seconds));
        wait_seconds = (wait_seconds.saturating_mul(2)).min(WORKTREE_SLOT_WAIT_MAX_SECONDS);
    }
}

/// Create parallel workspaces using git worktrees.
fn prepare_parallel_workspace_layout_worktree(
    plan: &RunPlan,
    loaded_config: &LoadedConfig,
) -> Result<Option<ParallelWorkspaceLayout>> {
    let configured_root =
        config::configured_parallel_worktree_root(loaded_config).ok_or_else(|| {
            anyhow::anyhow!(
                "`yarli run` requires `[execution].worktree_root` when `[features].parallel = true`"
            )
        })?;
    let source_workdir =
        config::resolve_execution_path_from_cwd(&plan.workdir, "execution.working_dir")?;
    if !source_workdir.is_dir() {
        bail!(
            "execution working_dir is not a directory: {}",
            source_workdir.display()
        );
    }
    let source_workdir = source_workdir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize execution working_dir {}",
            source_workdir.display()
        )
    })?;

    let worktree_root =
        config::resolve_execution_path_from_cwd(&configured_root, "execution.worktree_root")?;
    fs::create_dir_all(&worktree_root).with_context(|| {
        format!(
            "failed to create execution.worktree_root {}",
            worktree_root.display()
        )
    })?;
    let worktree_root = worktree_root.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize execution.worktree_root {}",
            worktree_root.display()
        )
    })?;
    if worktree_root == source_workdir {
        bail!(
            "execution.worktree_root must not equal execution.working_dir ({}); choose a dedicated workspace root",
            worktree_root.display()
        );
    }

    let needed = plan.tasks.len();
    let _existing = wait_for_available_worktree_slots(&source_workdir, needed)?;

    let source_head_at_creation = run_git_capture(&source_workdir, &["rev-parse", "HEAD"])
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());

    let run_id_short = &Uuid::now_v7().simple().to_string()[..12];
    let run_workspace_root = worktree_root.join(format!("run-{run_id_short}"));
    fs::create_dir_all(&run_workspace_root).with_context(|| {
        format!(
            "failed to create run workspace root {}",
            run_workspace_root.display()
        )
    })?;

    let mut task_workspace_dirs = Vec::with_capacity(needed);
    let mut worktree_branches = Vec::with_capacity(needed);
    let mut created_worktrees: Vec<(PathBuf, String)> = Vec::new();

    for (index, task) in plan.tasks.iter().enumerate() {
        let slug = sanitize_task_key_component(&task.task_key);
        let slug = if slug.is_empty() {
            format!("task-{}", index + 1)
        } else {
            slug
        };
        let branch = format!("yarli/{run_id_short}/{:03}-{slug}", index + 1);
        let workspace_dir = run_workspace_root.join(format!("{:03}-{slug}", index + 1));

        let workspace_dir_str = workspace_dir.display().to_string();
        let result = run_git_capture(
            &source_workdir,
            &["worktree", "add", "-b", &branch, &workspace_dir_str, "HEAD"],
        );

        match result {
            Ok(output) if output.status.success() => {
                if let Err(err) = mirror_runtime_config_into_workspace(
                    &source_workdir,
                    &workspace_dir,
                    loaded_config,
                ) {
                    warn!(
                        branch = %branch,
                        workspace = %workspace_dir.display(),
                        "failed to mirror runtime config into worktree: {err}; rolling back"
                    );
                    rollback_worktrees(&source_workdir, &created_worktrees);
                    let _ = fs::remove_dir_all(&run_workspace_root);
                    return Err(err.context(format!(
                        "failed to mirror runtime config into worktree branch {branch}"
                    )));
                }
                created_worktrees.push((workspace_dir.clone(), branch.clone()));
                task_workspace_dirs.push(workspace_dir);
                worktree_branches.push(branch);
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                warn!(
                    branch = %branch,
                    workspace = %workspace_dir.display(),
                    "git worktree add failed: {stderr}; rolling back"
                );
                rollback_worktrees(&source_workdir, &created_worktrees);
                let _ = fs::remove_dir_all(&run_workspace_root);
                bail!("git worktree add failed for branch {branch}: {stderr}");
            }
            Err(err) => {
                warn!(
                    branch = %branch,
                    workspace = %workspace_dir.display(),
                    "git worktree add failed: {err}; rolling back"
                );
                rollback_worktrees(&source_workdir, &created_worktrees);
                let _ = fs::remove_dir_all(&run_workspace_root);
                return Err(err.context(format!("git worktree add failed for branch {branch}")));
            }
        }
    }

    Ok(Some(ParallelWorkspaceLayout {
        run_workspace_root,
        task_workspace_dirs,
        source_head_at_creation,
        mode: ParallelWorkspaceMode::GitWorktree,
        worktree_branches,
        source_workdir: Some(source_workdir),
    }))
}

/// Mirror local runtime config into a newly created worktree workspace.
///
/// `git worktree add` only materializes tracked files from HEAD. Since local
/// runtime config (notably `yarli.toml`) is intentionally untracked in this
/// repository, copy it explicitly so worker workspaces preserve parity.
fn mirror_runtime_config_into_workspace(
    source_workdir: &Path,
    workspace_dir: &Path,
    loaded_config: &LoadedConfig,
) -> Result<()> {
    let default_config = source_workdir.join(config::DEFAULT_CONFIG_PATH);
    let loaded_path = loaded_config.path();
    let loaded_candidate = if loaded_path.is_absolute() {
        loaded_path.to_path_buf()
    } else {
        source_workdir.join(loaded_path)
    };

    let candidates = if loaded_candidate == default_config {
        vec![default_config]
    } else {
        vec![default_config, loaded_candidate]
    };

    for source_config in candidates {
        if !source_config.is_file() {
            continue;
        }
        let Some(file_name) = source_config.file_name() else {
            continue;
        };
        let destination = workspace_dir.join(file_name);
        if destination.exists() {
            return Ok(());
        }
        fs::copy(&source_config, &destination).with_context(|| {
            format!(
                "failed to copy runtime config {} to {}",
                source_config.display(),
                destination.display()
            )
        })?;
        return Ok(());
    }

    Ok(())
}

/// Remove worktrees and their branches on failure.
fn rollback_worktrees(source_workdir: &Path, created: &[(PathBuf, String)]) {
    for (path, branch) in created.iter().rev() {
        let path_str = path.display().to_string();
        let _ = run_git_capture(
            source_workdir,
            &["worktree", "remove", &path_str, "--force"],
        );
        let _ = run_git_capture(source_workdir, &["branch", "-D", branch]);
    }
    let _ = run_git_capture(source_workdir, &["worktree", "prune"]);
}

fn copy_state_file_if_present(
    source_workdir: &Path,
    workspace_dir: &Path,
    relative_path: &str,
) -> Result<()> {
    let workspace_path = workspace_dir.join(relative_path);
    if !workspace_path.exists() {
        return Ok(());
    }
    let destination_path = source_workdir.join(relative_path);
    if let Some(parent) = destination_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create state file parent directory {}",
                parent.display()
            )
        })?;
    }
    fs::copy(&workspace_path, &destination_path).with_context(|| {
        format!(
            "failed to sync state file {} -> {}",
            workspace_path.display(),
            destination_path.display()
        )
    })?;
    Ok(())
}

fn copy_directory_files_recursive(src_dir: &Path, dst_dir: &Path) -> Result<()> {
    if !src_dir.is_dir() {
        return Ok(());
    }
    fs::create_dir_all(dst_dir).with_context(|| {
        format!(
            "failed to create destination directory {}",
            dst_dir.display()
        )
    })?;
    for entry in fs::read_dir(src_dir)
        .with_context(|| format!("failed to read source directory {}", src_dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry in {}", src_dir.display()))?;
        let src_path = entry.path();
        let dst_path = dst_dir.join(entry.file_name());
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type for {}", src_path.display()))?;
        if file_type.is_dir() {
            copy_directory_files_recursive(&src_path, &dst_path)?;
        } else if file_type.is_file() {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create directory {}", parent.display()))?;
            }
            fs::copy(&src_path, &dst_path).with_context(|| {
                format!(
                    "failed to copy file {} -> {}",
                    src_path.display(),
                    dst_path.display()
                )
            })?;
        } else if file_type.is_symlink() {
            copy_symlink_entry(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Sync runtime state artifacts from a task workspace back to source.
///
/// This ensures `.yarli/tranches.toml` and evidence survive even when the
/// worktree had no tracked code changes (skip path).
fn sync_workspace_state_artifacts(source_workdir: &Path, workspace_dir: &Path) -> Result<()> {
    copy_state_file_if_present(source_workdir, workspace_dir, ".yarli/tranches.toml")?;
    copy_state_file_if_present(source_workdir, workspace_dir, ".yarli/continuation.json")?;

    let evidence_src = workspace_dir.join(".yarli/evidence");
    let evidence_dst = source_workdir.join(".yarli/evidence");
    copy_directory_files_recursive(&evidence_src, &evidence_dst)?;
    Ok(())
}

/// Remove a merged/skipped task worktree and its branch to free worktree slots.
fn release_worktree_slot(source_workdir: &Path, workspace_dir: &Path, branch: &str) -> Result<()> {
    let workspace_dir_str = workspace_dir.display().to_string();
    let remove_args = ["worktree", "remove", &workspace_dir_str, "--force"];
    let remove_output = run_git_capture(source_workdir, &remove_args)?;
    ensure_git_success(
        remove_output,
        source_workdir,
        &remove_args,
        "remove merged task worktree",
    )?;

    match run_git_capture(source_workdir, &["branch", "-D", branch]) {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(branch = %branch, "failed to delete worktree branch after merge: {stderr}");
        }
        Err(err) => {
            warn!(branch = %branch, "failed to delete worktree branch after merge: {err}");
        }
    }

    if let Err(err) = prune_stale_worktrees(source_workdir) {
        warn!("failed to prune worktree metadata after slot release: {err}");
    }
    Ok(())
}

/// Clean up a parallel workspace layout.
pub(crate) fn cleanup_parallel_workspace(layout: &ParallelWorkspaceLayout) {
    match layout.mode {
        ParallelWorkspaceMode::FileCopy => {
            if let Err(err) = fs::remove_dir_all(&layout.run_workspace_root) {
                warn!(
                    error = %err,
                    workspace_root = %layout.run_workspace_root.display(),
                    "failed to clean parallel run workspace root"
                );
            }
        }
        ParallelWorkspaceMode::GitWorktree => {
            let source_workdir = match layout.source_workdir.as_ref() {
                Some(dir) => dir,
                None => {
                    warn!("no source_workdir in layout; falling back to dir removal");
                    let _ = fs::remove_dir_all(&layout.run_workspace_root);
                    return;
                }
            };
            for (dir, branch) in layout
                .task_workspace_dirs
                .iter()
                .zip(layout.worktree_branches.iter())
            {
                let dir_str = dir.display().to_string();
                let _ =
                    run_git_capture(source_workdir, &["worktree", "remove", &dir_str, "--force"]);
                let _ = run_git_capture(source_workdir, &["branch", "-D", branch]);
            }
            let _ = run_git_capture(source_workdir, &["worktree", "prune"]);
            // Remove run workspace root if empty or still has remnants.
            let _ = fs::remove_dir_all(&layout.run_workspace_root);
        }
    }
}

/// Auto-commit YARLI state files after a tranche merge.
///
/// Stages only `.yarli/continuation.json` and `.yarli/tranches.toml`, then commits
/// with a templated message. Returns `Ok(true)` if a commit was made.
fn generated_commit_message(
    repo: &Path,
    diff: DiffSpec<'_>,
    metadata: Vec<(String, String)>,
    fallback_subject: &str,
    fallback_scope: Option<&str>,
) -> String {
    render_commit_message(&generate_commit_message(
        repo,
        diff,
        &metadata,
        fallback_subject,
        fallback_scope,
    ))
}

pub(crate) fn auto_commit_state_files(
    source_workdir: &Path,
    tranche_key: &str,
    run_id: &str,
    tranches_completed: u32,
    tranches_total: u32,
    message_template: Option<&str>,
) -> Result<bool> {
    let state_files = [".yarli/continuation.json", ".yarli/tranches.toml"];
    let mut any_staged = false;
    for file in &state_files {
        let full = source_workdir.join(file);
        if full.exists() {
            let output = run_git_capture(source_workdir, &["add", "-f", "--", file])?;
            if output.status.success() {
                any_staged = true;
            }
        }
    }

    if !any_staged {
        return Ok(false);
    }

    // Check if there are actually staged changes.
    let diff_output = run_git_capture(source_workdir, &["diff", "--cached", "--quiet"])?;
    if diff_output.status.success() {
        // No staged changes.
        return Ok(false);
    }

    let message = match message_template {
        Some(template) => template
            .replace("{tranche_key}", tranche_key)
            .replace("{run_id}", run_id)
            .replace("{tranches_completed}", &tranches_completed.to_string())
            .replace("{tranches_total}", &tranches_total.to_string()),
        None => generated_commit_message(
            source_workdir,
            DiffSpec::Staged,
            vec![
                ("yarli-run".to_string(), run_id.to_string()),
                ("yarli-tranche".to_string(), tranche_key.to_string()),
                (
                    "yarli-progress".to_string(),
                    format!("{tranches_completed}/{tranches_total}"),
                ),
            ],
            "chore(state): checkpoint runtime state",
            Some("state"),
        ),
    };

    let commit_result = run_git_capture(source_workdir, &["commit", "--no-verify", "-m", &message]);
    match commit_result {
        Ok(output) if output.status.success() => {
            info!(
                tranche_key = %tranche_key,
                "auto-committed state files: {message}"
            );
            Ok(true)
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                tranche_key = %tranche_key,
                "auto-commit of state files failed: {stderr}"
            );
            Ok(false)
        }
        Err(err) => {
            warn!(
                tranche_key = %tranche_key,
                error = %err,
                "auto-commit of state files failed"
            );
            Ok(false)
        }
    }
}

pub(crate) fn run_git_capture(cwd: &Path, args: &[&str]) -> Result<std::process::Output> {
    process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run git {:?} in {}", args, cwd.display()))
}

pub(crate) fn run_git_capture_with_input(
    cwd: &Path,
    args: &[&str],
    stdin_data: &[u8],
) -> Result<std::process::Output> {
    let mut child = process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn git {:?} in {}", args, cwd.display()))?;

    {
        let stdin = child.stdin.as_mut().ok_or_else(|| {
            anyhow::anyhow!(
                "failed to open git stdin for {:?} in {}",
                args,
                cwd.display()
            )
        })?;
        stdin.write_all(stdin_data).with_context(|| {
            format!(
                "failed to write git stdin for {:?} in {}",
                args,
                cwd.display()
            )
        })?;
    }

    child
        .wait_with_output()
        .with_context(|| format!("failed to wait for git {:?} in {}", args, cwd.display()))
}

pub(crate) fn ensure_git_success(
    output: std::process::Output,
    cwd: &Path,
    args: &[&str],
    context: &str,
) -> Result<String> {
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).to_string());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    bail!(
        "{context}: git {:?} failed in {}: {}",
        args,
        cwd.display(),
        stderr.trim()
    );
}

pub(crate) fn ensure_git_repository(cwd: &Path) -> Result<()> {
    let output = run_git_capture(cwd, &["rev-parse", "--git-dir"])?;
    ensure_git_success(
        output,
        cwd,
        &["rev-parse", "--git-dir"],
        "git repository check",
    )?;
    Ok(())
}

/// Parses a unified diff patch for `new file mode` entries and removes any that already
/// exist (untracked) in the source working directory. Returns backed-up content so the
/// caller can detect content divergence after the patch is applied.
pub(crate) fn remove_conflicting_new_files_for_patch(
    source_workdir: &Path,
    patch: &[u8],
) -> Result<Vec<(PathBuf, Vec<u8>)>> {
    let patch_str = String::from_utf8_lossy(patch);
    let lines: Vec<&str> = patch_str.lines().collect();
    let mut backed_up = Vec::new();

    let mut i = 0;
    while i < lines.len() {
        if let Some(rest) = lines[i].strip_prefix("diff --git a/") {
            // Extract path from "diff --git a/X b/Y" — take the b/Y part
            if let Some(b_idx) = rest.find(" b/") {
                let rel_path = &rest[b_idx + 3..];
                // Peek at next non-empty line for "new file mode"
                let mut j = i + 1;
                while j < lines.len() && lines[j].is_empty() {
                    j += 1;
                }
                if j < lines.len() && lines[j].starts_with("new file mode") {
                    let full_path = source_workdir.join(rel_path);
                    if full_path.exists() {
                        let content = fs::read(&full_path).with_context(|| {
                            format!(
                                "reading conflicting new file {} for backup",
                                full_path.display()
                            )
                        })?;
                        // Remove from git index first (if tracked/staged), then working tree
                        let _ = run_git_capture(
                            source_workdir,
                            &["rm", "--cached", "--force", "--quiet", "--", rel_path],
                        );
                        fs::remove_file(&full_path).with_context(|| {
                            format!(
                                "removing conflicting new file {} before patch",
                                full_path.display()
                            )
                        })?;
                        backed_up.push((PathBuf::from(rel_path), content));
                        info!(
                            path = rel_path,
                            "removed conflicting new file from source before patch application"
                        );
                    }
                }
            }
        }
        i += 1;
    }

    Ok(backed_up)
}

/// After a patch is applied, compares backed-up file content with the newly created
/// content. Logs a warning if they differ (the later workspace's version wins).
pub(crate) fn warn_on_new_file_content_divergence(
    source_workdir: &Path,
    backed_up: &[(PathBuf, Vec<u8>)],
) {
    for (rel_path, old_content) in backed_up {
        let full_path = source_workdir.join(rel_path);
        match fs::read(&full_path) {
            Ok(new_content) if new_content != *old_content => {
                warn!(
                    path = %rel_path.display(),
                    "new file content divergence: earlier workspace version was overwritten by later workspace"
                );
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParallelWorkspaceMergeFailureKind {
    MergeConflict,
    RuntimeFailure,
}

#[derive(Debug)]
pub(crate) struct ParallelWorkspaceMergeApplyError {
    pub(crate) kind: ParallelWorkspaceMergeFailureKind,
    pub(crate) message: String,
    pub(crate) repair_attempted: bool,
}

impl ParallelWorkspaceMergeApplyError {
    fn new(
        kind: ParallelWorkspaceMergeFailureKind,
        repair_attempted: bool,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            repair_attempted,
            message: message.into(),
        }
    }
}

impl fmt::Display for ParallelWorkspaceMergeApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ParallelWorkspaceMergeApplyError {}

fn merge_conflict_from_stderr(stderr: &str) -> bool {
    let text = stderr.to_lowercase();
    text.contains("conflict")
        || text.contains("patch failed")
        || text.contains("does not apply")
        || text.contains("did not apply")
        || text.contains("apply with 3-way merge")
        || text.contains("merge conflict")
}

fn classify_merge_apply_failure(stderr: &str) -> ParallelWorkspaceMergeFailureKind {
    if merge_conflict_from_stderr(stderr) {
        ParallelWorkspaceMergeFailureKind::MergeConflict
    } else {
        ParallelWorkspaceMergeFailureKind::RuntimeFailure
    }
}

fn collect_conflicted_workspace_files(source_workdir: &Path) -> Vec<String> {
    let mut conflicted = std::collections::HashSet::new();
    let commands: &[&[&str]] = &[
        &["diff", "--name-only", "--diff-filter=U"],
        &["diff", "--cached", "--name-only", "--diff-filter=U"],
    ];

    for args in commands {
        let output = match run_git_capture(source_workdir, args) {
            Ok(output) => output,
            Err(error) => {
                warn!(
                    error = %error,
                    workspace = %source_workdir.display(),
                    "failed to collect conflicted workspace files"
                );
                continue;
            }
        };
        let stdout = match ensure_git_success(
            output,
            source_workdir,
            args,
            "collect conflicted workspace file list",
        ) {
            Ok(stdout) => stdout,
            Err(error) => {
                warn!(
                    error = %error,
                    workspace = %source_workdir.display(),
                    "failed to collect conflicted workspace file list"
                );
                continue;
            }
        };
        for line in stdout.lines() {
            let path = line.trim();
            if !path.is_empty() {
                let _ = conflicted.insert(path.to_string());
            }
        }
    }

    let mut files = conflicted.into_iter().collect::<Vec<String>>();
    files.sort_unstable();
    files
}

fn parse_unmerged_paths_from_status_snapshot(status: &str) -> Vec<String> {
    let mut paths = HashSet::new();
    for raw_line in status.lines() {
        let line = raw_line.trim_end();
        if line.len() < 3 {
            continue;
        }
        let status_code = &line[..2];
        let is_unmerged = matches!(status_code, "DD" | "AU" | "UD" | "UA" | "DU" | "AA" | "UU");
        if !is_unmerged {
            continue;
        }
        let remainder = line[2..].trim();
        if remainder.is_empty() {
            continue;
        }
        let candidate = remainder
            .split(" -> ")
            .last()
            .unwrap_or(remainder)
            .trim()
            .trim_matches('"')
            .trim_matches('\'');
        if !candidate.is_empty() {
            let _ = paths.insert(candidate.to_string());
        }
    }
    let mut values = paths.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn collect_patch_target_paths(patch: &str) -> Vec<String> {
    let mut paths = HashSet::new();
    for raw_line in patch.lines() {
        if let Some(path) = raw_line.strip_prefix("+++ b/") {
            let path = path.trim();
            if !path.is_empty() && path != "/dev/null" {
                let _ = paths.insert(path.to_string());
            }
            continue;
        }
        if let Some(path) = raw_line.strip_prefix("--- a/") {
            let path = path.trim();
            if !path.is_empty() && path != "/dev/null" {
                let _ = paths.insert(path.to_string());
            }
            continue;
        }
        if let Some(path) = raw_line.strip_prefix("rename to ") {
            let path = path.trim();
            if !path.is_empty() {
                let _ = paths.insert(path.to_string());
            }
        }
    }
    let mut values = paths.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn collect_workspace_status_snapshot(source_workdir: &Path) -> String {
    let status_args = ["status", "--short"];
    match run_git_capture(source_workdir, &status_args) {
        Ok(output) => match ensure_git_success(
            output,
            source_workdir,
            &status_args,
            "collect repository status for repair payload",
        ) {
            Ok(status) => {
                let status = status.trim();
                if status.is_empty() {
                    "(clean)".to_string()
                } else {
                    status.to_string()
                }
            }
            Err(error) => {
                warn!(
                    error = %error,
                    workspace = %source_workdir.display(),
                    "failed to collect repository status for repair payload"
                );
                "(unavailable: status command failed)".to_string()
            }
        },
        Err(error) => {
            warn!(
                error = %error,
                workspace = %source_workdir.display(),
                "failed to run status command for repair payload"
            );
            "(unavailable: status command failed)".to_string()
        }
    }
}

fn merge_conflict_recovery_hints(source_workdir: &Path, patch_path: &Path) -> Vec<String> {
    vec![
        format!(
            "Inspect conflicted files: git -C \"{}\" diff --name-only --diff-filter=U",
            source_workdir.display()
        ),
        format!(
            "Review task patch: git -C \"{}\" apply --stat \"{}\"",
            source_workdir.display(),
            patch_path.display()
        ),
        format!(
            "Retry patch manually: git -C \"{}\" apply --3way --whitespace=nowarn \"{}\"",
            source_workdir.display(),
            patch_path.display()
        ),
    ]
}

fn repair_with_workspace_versions(
    source_workdir: &Path,
    task_key: &str,
) -> Result<(), ParallelWorkspaceMergeApplyError> {
    let conflicted_files = collect_conflicted_workspace_files(source_workdir);
    if conflicted_files.is_empty() {
        return Err(ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!(
                "parallel workspace merge auto-repair could not identify conflicted files for task {task_key}"
            ),
        ));
    }

    for path in &conflicted_files {
        run_git_capture(source_workdir, &["checkout", "--theirs", "--", path]).map_err(|error| {
            ParallelWorkspaceMergeApplyError::new(
                ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                true,
                format!(
                    "parallel workspace merge auto-repair failed while resolving {path} for task {task_key}: {error}"
                ),
            )
        })?;
    }

    let add_output = run_git_capture(source_workdir, &["add", "-A"])
        .map_err(|error| ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!(
                "parallel workspace merge auto-repair failed while staging resolved conflicts for task {task_key}: {error}"
            ),
        ))?;
    ensure_git_success(
        add_output,
        source_workdir,
        &["add", "-A"],
        "parallel workspace merge auto-repair",
    )
    .map_err(|error| {
        ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!("parallel workspace merge auto-repair failed for task {task_key}: {error}"),
        )
    })?;

    let unresolved_conflicts = collect_conflicted_workspace_files(source_workdir);
    if !unresolved_conflicts.is_empty() {
        return Err(ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::MergeConflict,
            true,
            format!(
                "parallel workspace merge auto-repair could not resolve all conflicts for task {task_key}: {}",
                unresolved_conflicts.join(", ")
            ),
        ));
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct MergeConflictContext {
    task_key: String,
    conflicted_files: Vec<ConflictedFileContext>,
    repo_status: String,
}

#[derive(Debug, Serialize)]
struct ConflictedFileContext {
    path: String,
    base: Option<String>,
    ours: Option<String>,
    theirs: Option<String>,
    conflict_markers: String,
}

fn extract_stage_content(source_workdir: &Path, stage: u8, path: &str) -> Option<String> {
    let spec = format!(":{stage}:{path}");
    let output = run_git_capture(source_workdir, &["show", &spec]).ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        None
    }
}

fn build_conflict_prompt(context: &MergeConflictContext) -> String {
    let mut prompt = String::new();
    prompt.push_str("# Merge Conflict Resolution\n\n");
    prompt.push_str(&format!(
        "Task `{}` has merge conflicts in {} file(s).\n",
        context.task_key,
        context.conflicted_files.len()
    ));
    prompt.push_str("Your job is to resolve these conflicts by editing the files in place.\n\n");
    prompt.push_str("## Instructions\n\n");
    prompt.push_str("1. Edit each conflicted file to integrate changes from BOTH sides.\n");
    prompt.push_str("2. Remove ALL conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`).\n");
    prompt.push_str("3. The result should compile/parse correctly.\n\n");

    for file in &context.conflicted_files {
        prompt.push_str(&format!("## File: `{}`\n\n", file.path));
        if let Some(base) = &file.base {
            prompt.push_str("### Base version (common ancestor)\n```\n");
            prompt.push_str(base);
            prompt.push_str("\n```\n\n");
        }
        if let Some(ours) = &file.ours {
            prompt.push_str("### Ours (current branch)\n```\n");
            prompt.push_str(ours);
            prompt.push_str("\n```\n\n");
        }
        if let Some(theirs) = &file.theirs {
            prompt.push_str("### Theirs (incoming patch)\n```\n");
            prompt.push_str(theirs);
            prompt.push_str("\n```\n\n");
        }
        prompt.push_str("### Current working tree (with conflict markers)\n```\n");
        prompt.push_str(&file.conflict_markers);
        prompt.push_str("\n```\n\n");
    }

    prompt
}

fn repair_with_llm_command(
    source_workdir: &Path,
    task_key: &str,
    repair_command: &str,
    timeout_seconds: u64,
) -> Result<(), ParallelWorkspaceMergeApplyError> {
    let conflicted_files = collect_conflicted_workspace_files(source_workdir);
    if conflicted_files.is_empty() {
        return Err(ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!(
                "llm-assisted merge repair could not identify conflicted files for task {task_key}"
            ),
        ));
    }

    // Build structured context
    let file_contexts: Vec<ConflictedFileContext> = conflicted_files
        .iter()
        .map(|path| {
            let conflict_markers = fs::read_to_string(source_workdir.join(path))
                .unwrap_or_else(|_| "(unable to read file)".to_string());
            ConflictedFileContext {
                path: path.clone(),
                base: extract_stage_content(source_workdir, 1, path),
                ours: extract_stage_content(source_workdir, 2, path),
                theirs: extract_stage_content(source_workdir, 3, path),
                conflict_markers,
            }
        })
        .collect();

    let repo_status = collect_workspace_status_snapshot(source_workdir);
    let context = MergeConflictContext {
        task_key: task_key.to_string(),
        conflicted_files: file_contexts,
        repo_status,
    };

    // Write artifacts to .yarli/ directory
    let yarli_dir = source_workdir.join(".yarli");
    fs::create_dir_all(&yarli_dir).map_err(|error| {
        ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!("failed to create .yarli directory for merge context: {error}"),
        )
    })?;

    let context_json_path = yarli_dir.join("merge-conflict-context.json");
    let context_json = serde_json::to_string_pretty(&context).map_err(|error| {
        ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!("failed to serialize merge conflict context: {error}"),
        )
    })?;
    fs::write(&context_json_path, &context_json).map_err(|error| {
        ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!("failed to write merge conflict context JSON: {error}"),
        )
    })?;

    let prompt_text = build_conflict_prompt(&context);
    let prompt_path = yarli_dir.join("merge-conflict-prompt.md");
    fs::write(&prompt_path, &prompt_text).map_err(|error| {
        ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!("failed to write merge conflict prompt: {error}"),
        )
    })?;

    let files_list = conflicted_files.join(",");

    // Spawn the repair command
    let mut child = process::Command::new("sh")
        .arg("-c")
        .arg(repair_command)
        .current_dir(source_workdir)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("YARLI_CONFLICT_CONTEXT", &context_json_path)
        .env("YARLI_CONFLICT_PROMPT", &prompt_path)
        .env("YARLI_CONFLICTED_FILES", &files_list)
        .env("YARLI_TASK_KEY", task_key)
        .spawn()
        .map_err(|error| {
            ParallelWorkspaceMergeApplyError::new(
                ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                true,
                format!("failed to spawn llm-assisted repair command for task {task_key}: {error}"),
            )
        })?;

    // Pipe the prompt to stdin
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(prompt_text.as_bytes());
        // drop stdin to close the pipe
    }

    // Wait with timeout
    let status = if timeout_seconds == 0 {
        child.wait().map_err(|error| {
            ParallelWorkspaceMergeApplyError::new(
                ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                true,
                format!(
                    "failed to wait for llm-assisted repair command for task {task_key}: {error}"
                ),
            )
        })?
    } else {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_seconds);
        loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(ParallelWorkspaceMergeApplyError::new(
                            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                            true,
                            format!(
                                "llm-assisted repair command timed out after {timeout_seconds}s for task {task_key}"
                            ),
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
                Err(error) => {
                    return Err(ParallelWorkspaceMergeApplyError::new(
                        ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                        true,
                        format!(
                            "failed to poll llm-assisted repair command for task {task_key}: {error}"
                        ),
                    ));
                }
            }
        }
    };

    if !status.success() {
        let exit_code = status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());
        return Err(ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!("llm-assisted repair command exited with {exit_code} for task {task_key}"),
        ));
    }

    // Safety check: scan resolved files for leftover conflict markers BEFORE staging.
    for path in &conflicted_files {
        let full_path = source_workdir.join(path);
        if let Ok(content) = fs::read_to_string(&full_path) {
            if content.contains("<<<<<<<") || content.contains(">>>>>>>") {
                return Err(ParallelWorkspaceMergeApplyError::new(
                    ParallelWorkspaceMergeFailureKind::MergeConflict,
                    true,
                    format!(
                        "llm-assisted repair resolved git conflicts but left conflict markers in {path} for task {task_key}"
                    ),
                ));
            }
        }
    }

    // Stage all resolved files (this clears the conflict state in the index)
    let add_output = run_git_capture(source_workdir, &["add", "-A"]).map_err(|error| {
        ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!(
                "llm-assisted merge repair failed while staging resolved files for task {task_key}: {error}"
            ),
        )
    })?;
    ensure_git_success(
        add_output,
        source_workdir,
        &["add", "-A"],
        "llm-assisted merge repair staging",
    )
    .map_err(|error| {
        ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::RuntimeFailure,
            true,
            format!("llm-assisted merge repair failed for task {task_key}: {error}"),
        )
    })?;

    // Verify: no remaining conflicted files after staging
    let remaining_conflicts = collect_conflicted_workspace_files(source_workdir);
    if !remaining_conflicts.is_empty() {
        return Err(ParallelWorkspaceMergeApplyError::new(
            ParallelWorkspaceMergeFailureKind::MergeConflict,
            true,
            format!(
                "llm-assisted repair command completed but did not resolve all conflicts for task {task_key}: {}",
                remaining_conflicts.join(", ")
            ),
        ));
    }

    Ok(())
}

pub(crate) fn apply_workspace_patch_to_source(
    source_workdir: &Path,
    task_key: &str,
    patch: &[u8],
    resolution_config: &MergeResolutionConfig,
) -> Result<bool, ParallelWorkspaceMergeApplyError> {
    let backed_up_new_files = remove_conflicting_new_files_for_patch(source_workdir, patch)
        .map_err(|error| {
            ParallelWorkspaceMergeApplyError::new(
                ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                false,
                format!(
                    "parallel workspace merge pre-processing failed for task {task_key}: {error}"
                ),
            )
        })?;

    let apply_args = ["apply", "--3way", "--whitespace=nowarn", "-"];
    let apply_output = run_git_capture_with_input(source_workdir, &apply_args, patch).map_err(
        |error| {
            ParallelWorkspaceMergeApplyError::new(
                ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                false,
                format!(
                    "parallel workspace merge command failed for task {task_key} with --3way: {error}"
                ),
            )
        },
    )?;
    if apply_output.status.success() {
        warn_on_new_file_content_divergence(source_workdir, &backed_up_new_files);
        return Ok(false);
    }

    // --3way failed. Check if it's an index-related issue where plain apply might
    // work (e.g. permission-only changes, shallow repos without base blobs).
    let apply_stderr = String::from_utf8_lossy(&apply_output.stderr);
    let index_related = apply_stderr.contains("does not exist in index")
        || apply_stderr.contains("does not match index");
    if index_related {
        let direct_apply_args = ["apply", "--whitespace=nowarn", "-"];
        let direct_apply_output = run_git_capture_with_input(source_workdir, &direct_apply_args, patch)
            .map_err(|error| {
                ParallelWorkspaceMergeApplyError::new(
                    ParallelWorkspaceMergeFailureKind::RuntimeFailure,
                    true,
                    format!(
                        "parallel workspace merge direct apply command failed for task {task_key} after index mismatch fallback: {error}"
                    ),
                )
            })?;
        if direct_apply_output.status.success() {
            warn_on_new_file_content_divergence(source_workdir, &backed_up_new_files);
            return Ok(true);
        }

        let direct_stderr = String::from_utf8_lossy(&direct_apply_output.stderr);
        return Err(ParallelWorkspaceMergeApplyError::new(
            classify_merge_apply_failure(&direct_stderr),
            true,
            format!(
                "parallel workspace merge direct apply failed for task {task_key} after index mismatch fallback: {}",
                direct_stderr.trim()
            ),
        ));
    }

    // Not index-related — apply the merge conflict resolution strategy.
    match resolution_config.strategy {
        config::MergeConflictResolution::AutoRepair => {
            repair_with_workspace_versions(source_workdir, task_key)?;
            warn!(
                task_key,
                "merge_conflict_resolution=auto-repair resolved workspace conflicts by preferring patch side"
            );
            warn_on_new_file_content_divergence(source_workdir, &backed_up_new_files);
            return Ok(true);
        }
        config::MergeConflictResolution::LlmAssisted => {
            let command = resolution_config.repair_command.as_deref().unwrap_or("");
            repair_with_llm_command(
                source_workdir,
                task_key,
                command,
                resolution_config.repair_timeout_seconds,
            )?;
            warn_on_new_file_content_divergence(source_workdir, &backed_up_new_files);
            return Ok(true);
        }
        config::MergeConflictResolution::Manual => {
            return Err(ParallelWorkspaceMergeApplyError::new(
                ParallelWorkspaceMergeFailureKind::MergeConflict,
                false,
                format!(
                    "merge conflict in task {task_key} (resolution=manual): workspace preserved for operator intervention.\n\
                     Resolve conflicts manually, then re-run:\n\
                       git -C \"{}\" status --short",
                    source_workdir.display()
                ),
            ));
        }
        config::MergeConflictResolution::Fail => {}
    }

    Err(ParallelWorkspaceMergeApplyError::new(
        classify_merge_apply_failure(&apply_stderr),
        false,
        format!(
            "parallel workspace merge apply failed for task {task_key}: {}",
            apply_stderr.trim()
        ),
    ))
}

pub(crate) fn is_parallel_merge_internal_path(relative: &Path) -> bool {
    let Some(component) = relative.components().next() else {
        return false;
    };
    match component {
        Component::Normal(value) => {
            let value = value.to_string_lossy();
            value == ".git" || value == ".yarl" || value == ".yarli"
        }
        _ => false,
    }
}

const PARALLEL_MERGE_IGNORE_DIR_NAMES: [&str; 4] =
    ["target", "node_modules", "__pycache__", ".build"];

fn is_parallel_merge_ignored_artifact(relative: &Path) -> bool {
    relative
        .components()
        .filter_map(|component| {
            if let Component::Normal(name) = component {
                Some(name.to_string_lossy())
            } else {
                None
            }
        })
        .any(|name| {
            PARALLEL_MERGE_IGNORE_DIR_NAMES
                .iter()
                .any(|ignore| ignore == &name.as_ref())
        })
}

fn should_skip_parallel_merge_path(source_workdir: &Path, relative: &Path) -> bool {
    if !is_parallel_merge_ignored_artifact(relative) {
        return false;
    }
    !source_workdir.join(relative).exists()
}

pub(crate) fn workspace_candidate_paths(
    source_workdir: &Path,
    workspace_dir: &Path,
) -> Result<Vec<PathBuf>> {
    let args_unstaged = [
        "ls-files",
        "-z",
        "-m",
        "-o",
        "--exclude-standard",
        "--deleted",
    ];
    let unstaged_output = run_git_capture(workspace_dir, &args_unstaged)?;
    let unstaged_stdout = ensure_git_success(
        unstaged_output,
        workspace_dir,
        &args_unstaged,
        "workspace changed-path discovery",
    )?;
    let args_staged = ["diff", "--cached", "--name-only", "-z"];
    let staged_output = run_git_capture(workspace_dir, &args_staged)?;
    let staged_stdout = ensure_git_success(
        staged_output,
        workspace_dir,
        &args_staged,
        "workspace staged-path discovery",
    )?;

    let mut seen = HashSet::new();
    let mut paths = Vec::new();
    for token in unstaged_stdout.split('\0').chain(staged_stdout.split('\0')) {
        if token.is_empty() {
            continue;
        }
        let relative = PathBuf::from(token);
        if should_skip_parallel_merge_path(source_workdir, &relative) {
            continue;
        }
        if is_parallel_merge_internal_path(&relative) {
            continue;
        }
        if seen.insert(relative.clone()) {
            paths.push(relative);
        }
    }
    Ok(paths)
}

pub(crate) fn metadata_if_exists(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}

pub(crate) fn workspace_path_matches_source(
    source_workdir: &Path,
    workspace_dir: &Path,
    relative: &Path,
) -> Result<bool> {
    let source_path = source_workdir.join(relative);
    let workspace_path = workspace_dir.join(relative);
    let source_meta = metadata_if_exists(&source_path)?;
    let workspace_meta = metadata_if_exists(&workspace_path)?;

    let Some(source_meta) = source_meta else {
        return Ok(workspace_meta.is_none());
    };
    let Some(workspace_meta) = workspace_meta else {
        return Ok(false);
    };

    let source_type = source_meta.file_type();
    let workspace_type = workspace_meta.file_type();
    if source_type.is_symlink() || workspace_type.is_symlink() {
        if !source_type.is_symlink() || !workspace_type.is_symlink() {
            return Ok(false);
        }
        let source_target = fs::read_link(&source_path)
            .with_context(|| format!("failed to read symlink {}", source_path.display()))?;
        let workspace_target = fs::read_link(&workspace_path)
            .with_context(|| format!("failed to read symlink {}", workspace_path.display()))?;
        return Ok(source_target == workspace_target);
    }

    if source_meta.is_dir() || workspace_meta.is_dir() {
        return Ok(source_meta.is_dir() && workspace_meta.is_dir());
    }

    if source_meta.is_file() && workspace_meta.is_file() {
        if source_meta.len() != workspace_meta.len() {
            return Ok(false);
        }
        let source_contents = fs::read(&source_path)
            .with_context(|| format!("failed to read {}", source_path.display()))?;
        let workspace_contents = fs::read(&workspace_path)
            .with_context(|| format!("failed to read {}", workspace_path.display()))?;
        return Ok(source_contents == workspace_contents
            && file_permissions_equivalent(&source_meta, &workspace_meta));
    }

    Ok(false)
}

#[cfg(unix)]
pub(crate) fn file_permissions_equivalent(
    source_meta: &fs::Metadata,
    workspace_meta: &fs::Metadata,
) -> bool {
    use std::os::unix::fs::PermissionsExt;
    source_meta.permissions().mode() == workspace_meta.permissions().mode()
}

#[cfg(not(unix))]
pub(crate) fn file_permissions_equivalent(
    source_meta: &fs::Metadata,
    workspace_meta: &fs::Metadata,
) -> bool {
    source_meta.permissions().readonly() == workspace_meta.permissions().readonly()
}

pub(crate) fn scoped_workspace_changed_paths(
    source_workdir: &Path,
    workspace_dir: &Path,
) -> Result<Vec<PathBuf>> {
    let mut scoped_paths = Vec::new();
    for relative in workspace_candidate_paths(source_workdir, workspace_dir)? {
        if !workspace_path_matches_source(source_workdir, workspace_dir, &relative)? {
            scoped_paths.push(relative);
        }
    }
    Ok(scoped_paths)
}

pub(crate) fn stage_workspace_paths(workspace_dir: &Path, scoped_paths: &[PathBuf]) -> Result<()> {
    let reset_args = ["reset", "--quiet"];
    let reset_output = run_git_capture(workspace_dir, &reset_args)?;
    ensure_git_success(
        reset_output,
        workspace_dir,
        &reset_args,
        "workspace index reset",
    )?;

    for relative in scoped_paths {
        let relative = relative.to_string_lossy().to_string();
        let add_args = ["add", "-A", "--", relative.as_str()];
        let add_output = run_git_capture(workspace_dir, &add_args)?;
        ensure_git_success(
            add_output,
            workspace_dir,
            &add_args,
            "workspace scoped staging",
        )?;
    }
    Ok(())
}

pub(crate) fn export_staged_workspace_patch(workspace_dir: &Path) -> Result<String> {
    let diff_args = ["diff", "--binary", "--cached", "--no-color"];
    let patch_output = run_git_capture(workspace_dir, &diff_args)?;
    ensure_git_success(
        patch_output,
        workspace_dir,
        &diff_args,
        "workspace patch export",
    )
}

pub(crate) fn persist_workspace_patch_for_recovery(
    run_workspace_root: &Path,
    task_key: &str,
    task_index: usize,
    patch: &str,
) -> Result<PathBuf> {
    let patch_dir = run_workspace_root.join("merge-patches");
    fs::create_dir_all(&patch_dir).with_context(|| {
        format!(
            "failed to create merge patch directory {}",
            patch_dir.display()
        )
    })?;
    let task_slug = sanitize_task_key_component(task_key);
    let task_slug = if task_slug.is_empty() {
        format!("task-{}", task_index + 1)
    } else {
        task_slug
    };
    let patch_path = patch_dir.join(format!("{:03}-{task_slug}.patch", task_index + 1));
    fs::write(&patch_path, patch)
        .with_context(|| format!("failed to write merge patch {}", patch_path.display()))?;
    Ok(patch_path)
}

pub(crate) fn write_parallel_merge_recovery_note(
    run_id: Uuid,
    source_workdir: &Path,
    run_workspace_root: &Path,
    workspace_dir: &Path,
    task_key: &str,
    patch_path: &Path,
    original_workspace_head: Option<&str>,
) -> Result<PathBuf> {
    let note_path = run_workspace_root.join("PARALLEL_MERGE_RECOVERY.txt");
    let restore_guidance = if let Some(head) = original_workspace_head {
        format!(
            "\nOriginal workspace HEAD before merge normalization: {head}\n\
To restore this workspace to its pre-merge-attempt commit:\n\
   git -C \"{}\" reset --hard \"{}\"\n",
            workspace_dir.display(),
            head
        )
    } else {
        String::new()
    };
    let note = format!(
        "Parallel workspace merge failed for run {run_id} task {task_key}.\n\n\
Source workspace: {}\n\
Task workspace: {}\n\
Patch artifact: {}\n{}\
Operator recovery steps:\n\
1. Inspect the repo and conflicted files:\n\
   git -C \"{}\" status --short\n\
2. Preview the generated patch:\n\
   git -C \"{}\" apply --stat \"{}\"\n\
3. Retry manual apply for this task:\n\
   git -C \"{}\" apply --3way --whitespace=nowarn \"{}\"\n\
4. If conflicts remain, resolve markers, stage the files, and continue your normal flow.\n\
5. If you want to abort this task patch, restore conflicting files before retrying.\n",
        source_workdir.display(),
        workspace_dir.display(),
        patch_path.display(),
        restore_guidance,
        source_workdir.display(),
        source_workdir.display(),
        patch_path.display(),
        source_workdir.display(),
        patch_path.display()
    );
    fs::write(&note_path, note).with_context(|| {
        format!(
            "failed to write merge recovery note {}",
            note_path.display()
        )
    })?;
    Ok(note_path)
}

pub(crate) fn restore_workspace_head_after_failed_merge(
    workspace_dir: &Path,
    original_workspace_head: &str,
) -> Result<()> {
    let reset_args = ["reset", "--hard", original_workspace_head];
    let reset_output = run_git_capture(workspace_dir, &reset_args)?;
    ensure_git_success(
        reset_output,
        workspace_dir,
        &reset_args,
        "restore workspace HEAD after failed merge",
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_parallel_merge_lineage_recovery_note(
    run_id: Uuid,
    source_workdir: &Path,
    run_workspace_root: &Path,
    workspace_dir: &Path,
    task_key: &str,
    source_head: &str,
    workspace_head: &str,
    reason: &str,
) -> Result<PathBuf> {
    let note_path = run_workspace_root.join("PARALLEL_MERGE_RECOVERY.txt");
    let note = format!(
        "Parallel workspace merge blocked for run {run_id} task {task_key}.\n\n\
Reason: {reason}\n\
Source HEAD: {source_head}\n\
Workspace HEAD: {workspace_head}\n\n\
Source workspace: {}\n\
Task workspace: {}\n\n\
Operator recovery steps:\n\
1. Inspect source merge state:\n\
   git -C \"{}\" status --short\n\
2. Inspect workspace commit lineage:\n\
   git -C \"{}\" log --oneline --decorate --graph -n 20\n\
3. Compare workspace changes relative to source HEAD:\n\
   git -C \"{}\" diff --stat \"{}\"\n\
4. Manually reconcile the workspace changes, then re-run yarli.\n",
        source_workdir.display(),
        workspace_dir.display(),
        source_workdir.display(),
        workspace_dir.display(),
        workspace_dir.display(),
        source_head
    );
    fs::write(&note_path, note).with_context(|| {
        format!(
            "failed to write merge recovery note {}",
            note_path.display()
        )
    })?;
    Ok(note_path)
}

#[cfg(test)]
pub(crate) fn merge_parallel_workspace_results(
    source_workdir: &Path,
    run_id: Uuid,
    run_workspace_root: &Path,
    task_workspaces: &[(String, PathBuf)],
) -> Result<ParallelWorkspaceMergeReport> {
    let resolution_config = MergeResolutionConfig {
        strategy: config::MergeConflictResolution::Fail,
        repair_command: None,
        repair_timeout_seconds: 300,
    };
    merge_parallel_workspace_results_with_resolution(
        source_workdir,
        run_id,
        run_workspace_root,
        task_workspaces,
        &resolution_config,
        None,
    )
}

/// Merge parallel workspace results using git branch merges (worktree mode).
///
/// For each task worktree: commit any changes, then merge the branch into the
/// current branch in source_workdir.
pub(crate) fn merge_worktree_workspace_results(
    source_workdir: &Path,
    run_id: Uuid,
    task_workspaces: &[(String, PathBuf)],
    worktree_branches: &[String],
    resolution_config: &MergeResolutionConfig,
    apply_telemetry: &mut Vec<MergeApplyTelemetryEvent>,
) -> Result<ParallelWorkspaceMergeReport> {
    ensure_git_repository(source_workdir)?;

    merge_apply_telemetry_event(
        apply_telemetry,
        "merge.apply.started",
        None,
        None,
        serde_json::json!({
            "run_id": run_id.to_string(),
            "mode": "git_worktree",
            "source_workdir": source_workdir.display().to_string(),
            "task_count": task_workspaces.len(),
            "merge_conflict_resolution": format!("{:?}", resolution_config.strategy),
        }),
    );

    // Stash any pre-existing dirty state so `git merge` doesn't refuse due to
    // local modifications. This mirrors the stash/unstash flow in the copy-mode
    // merge (merge_parallel_workspace_results_with_resolution_with_events).
    let pre_status = run_git_capture(source_workdir, &["status", "--porcelain"])?;
    let pre_status_text = String::from_utf8_lossy(&pre_status.stdout);
    let has_stashed_dirty_state = if !pre_status_text.trim().is_empty() {
        info!("stashing pre-existing dirty state before worktree merge");
        let _ = run_git_capture(source_workdir, &["add", "-A"]);
        let stash_result = run_git_capture(
            source_workdir,
            &[
                "stash",
                "push",
                "-m",
                "yarli: stash pre-existing workspace state before worktree merge",
            ],
        );
        stash_result.map(|o| o.status.success()).unwrap_or(false)
    } else {
        false
    };

    // Run the merge loop in a closure so we can always pop the stash on exit,
    // even if the merge loop errors out.
    let merge_loop_result =
        (|| -> Result<(Vec<String>, Vec<String>, Vec<ParallelTaskMergeOutcome>)> {
            let mut merged_task_keys = Vec::new();
            let mut skipped_task_keys = Vec::new();
            let mut task_outcomes = Vec::new();

            for (task_index, ((task_key, workspace_dir), branch)) in task_workspaces
                .iter()
                .zip(worktree_branches.iter())
                .enumerate()
            {
                // Check if worktree has any changes.
                let status_output = run_git_capture(workspace_dir, &["status", "--porcelain"])?;
                let status_text = String::from_utf8_lossy(&status_output.stdout);
                if status_text.trim().is_empty() {
                    // No changes — check if there are committed changes beyond HEAD.
                    let source_head_output =
                        run_git_capture(source_workdir, &["rev-parse", "HEAD"])?;
                    let source_head = String::from_utf8_lossy(&source_head_output.stdout)
                        .trim()
                        .to_string();
                    let ws_head_output = run_git_capture(workspace_dir, &["rev-parse", "HEAD"])?;
                    let ws_head = String::from_utf8_lossy(&ws_head_output.stdout)
                        .trim()
                        .to_string();

                    if source_head == ws_head {
                        sync_workspace_state_artifacts(source_workdir, workspace_dir)
                            .with_context(|| {
                                format!(
                                    "failed to sync yarli state artifacts from skipped worktree {}",
                                    workspace_dir.display()
                                )
                            })?;
                        release_worktree_slot(source_workdir, workspace_dir, branch).with_context(
                            || {
                                format!(
                                    "failed to release skipped worktree slot for task {task_key}"
                                )
                            },
                        )?;
                        skipped_task_keys.push(task_key.clone());
                        task_outcomes.push(ParallelTaskMergeOutcome {
                            task_key: task_key.clone(),
                            disposition: ParallelTaskMergeDisposition::Skipped,
                            skip_reason: Some(ParallelTaskSkipReason::NoScopedPaths),
                            workspace_head: Some(ws_head),
                            source_head: Some(source_head),
                            soft_reset_applied: false,
                            patch_path: None,
                        });
                        continue;
                    }
                }

                // Stage and commit all changes in the worktree.
                if !status_text.trim().is_empty() {
                    let _ = run_git_capture(workspace_dir, &["add", "-A"]);
                    let commit_msg = generated_commit_message(
                        workspace_dir,
                        DiffSpec::Staged,
                        vec![
                            ("yarli-run".to_string(), run_id.to_string()),
                            ("yarli-task".to_string(), task_key.clone()),
                            ("yarli-branch".to_string(), branch.clone()),
                        ],
                        "chore(workspace): update staged changes",
                        Some(task_key),
                    );
                    let commit_result = run_git_capture(
                        workspace_dir,
                        &["commit", "--no-verify", "--allow-empty", "-m", &commit_msg],
                    );
                    if let Ok(output) = &commit_result {
                        if !output.status.success() {
                            let stderr = String::from_utf8_lossy(&output.stderr);
                            warn!(
                                task_key = %task_key,
                                "git commit in worktree failed: {stderr}"
                            );
                        }
                    }
                }

                // Merge the worktree branch into the main working directory.
                let merge_msg = generated_commit_message(
                    source_workdir,
                    DiffSpec::Range {
                        base: "HEAD",
                        head: branch,
                    },
                    vec![
                        ("yarli-run".to_string(), run_id.to_string()),
                        ("yarli-task".to_string(), task_key.clone()),
                        ("yarli-source-branch".to_string(), branch.clone()),
                    ],
                    "chore(integration): integrate workspace changes",
                    Some(task_key),
                );
                let merge_result = run_git_capture(
                    source_workdir,
                    &["merge", "--no-ff", branch, "-m", &merge_msg],
                );

                match merge_result {
                    Ok(output) if output.status.success() => {
                        let ws_head_output =
                            run_git_capture(workspace_dir, &["rev-parse", "HEAD"])?;
                        let ws_head = String::from_utf8_lossy(&ws_head_output.stdout)
                            .trim()
                            .to_string();
                        let source_head_output =
                            run_git_capture(source_workdir, &["rev-parse", "HEAD"])?;
                        let source_head = String::from_utf8_lossy(&source_head_output.stdout)
                            .trim()
                            .to_string();
                        sync_workspace_state_artifacts(source_workdir, workspace_dir)
                            .with_context(|| {
                                format!(
                                    "failed to sync yarli state artifacts from merged worktree {}",
                                    workspace_dir.display()
                                )
                            })?;
                        release_worktree_slot(source_workdir, workspace_dir, branch).with_context(
                            || {
                                format!(
                                    "failed to release merged worktree slot for task {task_key}"
                                )
                            },
                        )?;
                        merged_task_keys.push(task_key.clone());
                        task_outcomes.push(ParallelTaskMergeOutcome {
                            task_key: task_key.clone(),
                            disposition: ParallelTaskMergeDisposition::Merged,
                            skip_reason: None,
                            workspace_head: Some(ws_head),
                            source_head: Some(source_head),
                            soft_reset_applied: false,
                            patch_path: None,
                        });
                        merge_apply_telemetry_event(
                            apply_telemetry,
                            "merge.apply.task_merged",
                            Some(task_key),
                            Some(task_index),
                            serde_json::json!({
                                "run_id": run_id.to_string(),
                                "task_key": task_key,
                                "branch": branch,
                                "mode": "git_worktree",
                            }),
                        );
                    }
                    Ok(output) => {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        // Merge conflict — attempt resolution based on strategy.
                        match resolution_config.strategy {
                            config::MergeConflictResolution::AutoRepair => {
                                // Accept theirs (worktree version) for all conflicts.
                                let status =
                                    run_git_capture(source_workdir, &["status", "--porcelain"])?;
                                let status_text = String::from_utf8_lossy(&status.stdout);
                                for line in status_text.lines() {
                                    if line.len() < 3 {
                                        continue;
                                    }
                                    let prefix = &line[..2];
                                    let path = line[3..].trim();
                                    match prefix {
                                        "UU" | "AA" => {
                                            let _ = run_git_capture(
                                                source_workdir,
                                                &["checkout", "--theirs", "--", path],
                                            );
                                            let _ = run_git_capture(
                                                source_workdir,
                                                &["add", "--", path],
                                            );
                                        }
                                        "DU" => {
                                            let _ = run_git_capture(
                                                source_workdir,
                                                &["checkout", "--theirs", "--", path],
                                            );
                                            let _ = run_git_capture(
                                                source_workdir,
                                                &["add", "--", path],
                                            );
                                        }
                                        "UD" => {
                                            let _ = run_git_capture(
                                                source_workdir,
                                                &["rm", "--", path],
                                            );
                                        }
                                        _ => {}
                                    }
                                }
                                let repair_msg = generated_commit_message(
                                    source_workdir,
                                    DiffSpec::Staged,
                                    vec![
                                        ("yarli-run".to_string(), run_id.to_string()),
                                        ("yarli-task".to_string(), task_key.clone()),
                                        ("yarli-resolution".to_string(), "auto-repair".to_string()),
                                    ],
                                    "fix(integration): update repaired merge changes",
                                    Some(task_key),
                                );
                                let _ = run_git_capture(
                                    source_workdir,
                                    &["commit", "--no-verify", "--allow-empty", "-m", &repair_msg],
                                );
                                merged_task_keys.push(task_key.clone());
                                task_outcomes.push(ParallelTaskMergeOutcome {
                                    task_key: task_key.clone(),
                                    disposition: ParallelTaskMergeDisposition::Merged,
                                    skip_reason: None,
                                    workspace_head: None,
                                    source_head: None,
                                    soft_reset_applied: false,
                                    patch_path: None,
                                });
                            }
                            _ => {
                                // Abort the merge and report failure.
                                let _ = run_git_capture(source_workdir, &["merge", "--abort"]);
                                merge_apply_telemetry_event(
                                    apply_telemetry,
                                    "merge.apply.conflict",
                                    Some(task_key),
                                    Some(task_index),
                                    serde_json::json!({
                                        "run_id": run_id.to_string(),
                                        "task_key": task_key,
                                        "branch": branch,
                                        "mode": "git_worktree",
                                        "stderr": stderr.to_string(),
                                    }),
                                );
                                return Err(ParallelWorkspaceMergeApplyError::new(
                                    ParallelWorkspaceMergeFailureKind::MergeConflict,
                                    false,
                                    format!("merge conflict for task {task_key} (branch {branch}): {stderr}"),
                                ).into());
                            }
                        }
                    }
                    Err(err) => {
                        return Err(err.context(format!(
                            "git merge failed for task {task_key} branch {branch}"
                        )));
                    }
                }
            }

            Ok((merged_task_keys, skipped_task_keys, task_outcomes))
        })();

    // Always reapply stashed dirty state, even if the merge loop failed.
    // This prevents leaving the working tree in a stashed state.
    if has_stashed_dirty_state {
        let pop_result = run_git_capture(source_workdir, &["stash", "pop"]);
        let pop_ok = pop_result
            .as_ref()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !pop_ok {
            warn!("pre-existing dirty state partially conflicted with worktree merge; resolving conflicts in favor of workspace");
            if let Ok(status_output) = run_git_capture(source_workdir, &["status", "--porcelain"]) {
                let status_text = String::from_utf8_lossy(&status_output.stdout);
                for line in status_text.lines() {
                    if line.len() < 3 {
                        continue;
                    }
                    let prefix = &line[..2];
                    let path = line[3..].trim();
                    match prefix {
                        "UU" | "AA" | "UD" => {
                            let _ = run_git_capture(
                                source_workdir,
                                &["checkout", "--ours", "--", path],
                            );
                        }
                        "DU" => {
                            let _ = run_git_capture(source_workdir, &["rm", "-f", "--", path]);
                        }
                        _ => {}
                    }
                }
            }
            let _ = run_git_capture(source_workdir, &["add", "-A"]);
            let _ = run_git_capture(source_workdir, &["stash", "drop"]);
        }
        // Commit the reapplied dirty state (clean pop or resolved conflicts).
        let _ = run_git_capture(source_workdir, &["add", "-A"]);
        let reapply_msg = generated_commit_message(
            source_workdir,
            DiffSpec::Staged,
            vec![
                ("yarli-run".to_string(), run_id.to_string()),
                (
                    "yarli-restore".to_string(),
                    "pre-existing workspace state after worktree merge".to_string(),
                ),
            ],
            "chore(workspace): restore local workspace changes",
            Some("workspace"),
        );
        let _ = run_git_capture(
            source_workdir,
            &["commit", "--no-verify", "--allow-empty", "-m", &reapply_msg],
        );
    }

    // Propagate merge loop error after stash has been restored.
    let (merged_task_keys, skipped_task_keys, task_outcomes) = merge_loop_result?;

    merge_apply_telemetry_event(
        apply_telemetry,
        "merge.apply.finalized",
        None,
        None,
        serde_json::json!({
            "run_id": run_id.to_string(),
            "status": "succeeded",
            "mode": "git_worktree",
            "task_count": task_workspaces.len(),
            "merged_task_count": merged_task_keys.len(),
            "skipped_task_count": skipped_task_keys.len(),
        }),
    );

    Ok(ParallelWorkspaceMergeReport {
        preserve_workspace_root: !skipped_task_keys.is_empty(),
        merged_task_keys,
        skipped_task_keys,
        task_outcomes,
    })
}

fn merge_apply_telemetry_event(
    events: &mut Vec<MergeApplyTelemetryEvent>,
    event_type: &str,
    task_key: Option<&str>,
    task_index: Option<usize>,
    metadata: serde_json::Value,
) {
    events.push(MergeApplyTelemetryEvent {
        event_type: event_type.to_string(),
        task_key: task_key.map(ToString::to_string),
        task_index,
        metadata,
    });
}

#[allow(dead_code)]
pub(crate) fn merge_parallel_workspace_results_with_resolution(
    source_workdir: &Path,
    run_id: Uuid,
    run_workspace_root: &Path,
    task_workspaces: &[(String, PathBuf)],
    resolution_config: &MergeResolutionConfig,
    source_head_at_creation: Option<&str>,
) -> Result<ParallelWorkspaceMergeReport> {
    let mut apply_telemetry = Vec::new();
    let report = merge_parallel_workspace_results_with_resolution_with_events(
        source_workdir,
        run_id,
        run_workspace_root,
        task_workspaces,
        resolution_config,
        source_head_at_creation,
        &mut apply_telemetry,
    )?;
    Ok(report)
}

pub(crate) fn merge_parallel_workspace_results_with_resolution_with_events(
    source_workdir: &Path,
    run_id: Uuid,
    run_workspace_root: &Path,
    task_workspaces: &[(String, PathBuf)],
    resolution_config: &MergeResolutionConfig,
    source_head_at_creation: Option<&str>,
    apply_telemetry: &mut Vec<MergeApplyTelemetryEvent>,
) -> Result<ParallelWorkspaceMergeReport> {
    ensure_git_repository(source_workdir)?;

    // Prefer the source HEAD captured at workspace creation time. If the source
    // repo advanced since then (external commits, other processes), the current
    // HEAD may not exist in the workspace's copied object database.
    let source_head = if let Some(head) = source_head_at_creation {
        head.to_string()
    } else {
        let source_head_args = ["rev-parse", "HEAD"];
        let source_head_output = run_git_capture(source_workdir, &source_head_args)?;
        let h = ensure_git_success(
            source_head_output,
            source_workdir,
            &source_head_args,
            "capture source HEAD for workspace merge",
        )?;
        h.trim().to_string()
    };

    merge_apply_telemetry_event(
        apply_telemetry,
        "merge.apply.started",
        None,
        None,
        serde_json::json!({
            "run_id": run_id.to_string(),
            "source_head": source_head,
            "source_head_from_creation": source_head_at_creation.is_some(),
            "source_workdir": source_workdir.display().to_string(),
            "workspace_root": run_workspace_root.display().to_string(),
            "task_count": task_workspaces.len(),
            "merge_conflict_resolution": format!("{:?}", resolution_config.strategy),
        }),
    );

    // Stash any pre-existing dirty state from prior runs so workspace patches
    // apply against the clean HEAD they were cloned from. Previous yarli versions
    // did not commit after workspace merges, leaving staged/modified files whose
    // content diverges from HEAD. Committing this dirty state moves HEAD forward,
    // causing workspace patches (whose context lines match the old HEAD) to fail
    // with --3way conflicts. Stashing preserves the dirty state without moving
    // HEAD, then we reapply it after all workspace patches are merged.
    let pre_status = run_git_capture(source_workdir, &["status", "--porcelain"])?;
    let pre_status_text = String::from_utf8_lossy(&pre_status.stdout);
    let has_stashed_dirty_state = if !pre_status_text.trim().is_empty() {
        info!("stashing pre-existing dirty state before parallel workspace merge");
        let _ = run_git_capture(source_workdir, &["add", "-A"]);
        let stash_result = run_git_capture(
            source_workdir,
            &[
                "stash",
                "push",
                "-m",
                "yarli: stash pre-existing workspace state before merge",
            ],
        );
        stash_result.map(|o| o.status.success()).unwrap_or(false)
    } else {
        false
    };

    let mut merged_task_keys = Vec::new();
    let mut skipped_task_keys = Vec::new();
    let mut task_outcomes = Vec::new();
    let mut repair_started_count = 0usize;
    let mut repair_succeeded_count = 0usize;
    let mut repair_failed_count = 0usize;
    let mut conflict_count = 0usize;
    for (task_index, (task_key, workspace_dir)) in task_workspaces.iter().enumerate() {
        ensure_git_repository(workspace_dir)?;
        let mut original_workspace_head_for_restore: Option<String> = None;
        let mut soft_reset_applied = false;
        let ws_head_args = ["rev-parse", "HEAD"];
        let ws_head_output = run_git_capture(workspace_dir, &ws_head_args)?;
        let ws_head = ensure_git_success(
            ws_head_output,
            workspace_dir,
            &ws_head_args,
            "capture workspace HEAD for merge comparison",
        )?;
        let ws_head = ws_head.trim().to_string();
        if ws_head != source_head {
            let ancestry_args = [
                "merge-base",
                "--is-ancestor",
                source_head.as_str(),
                ws_head.as_str(),
            ];
            let ancestry_output = run_git_capture(workspace_dir, &ancestry_args)?;
            if !ancestry_output.status.success() {
                let ancestry_stderr = String::from_utf8_lossy(&ancestry_output.stderr);
                if ancestry_output.status.code() == Some(1) {
                    let reason = format!(
                        "workspace {} HEAD ({}) is not a descendant of source HEAD ({})",
                        workspace_dir.display(),
                        &ws_head[..12.min(ws_head.len())],
                        &source_head[..12.min(source_head.len())]
                    );
                    let note_path = write_parallel_merge_lineage_recovery_note(
                        run_id,
                        source_workdir,
                        run_workspace_root,
                        workspace_dir,
                        task_key,
                        &source_head,
                        &ws_head,
                        &reason,
                    )
                    .ok();
                    let mut guidance = format!(
                        "{reason}; cannot safely merge committed workspace changes\n\
Operator recovery steps:\n\
1. Inspect source merge state: git -C \"{}\" status --short\n\
2. Inspect workspace lineage: git -C \"{}\" log --oneline --decorate --graph -n 20\n\
3. Compare workspace changes: git -C \"{}\" diff --stat \"{}\"",
                        source_workdir.display(),
                        workspace_dir.display(),
                        workspace_dir.display(),
                        source_head
                    );
                    if let Some(path) = note_path {
                        guidance.push_str(&format!("\nDetailed recovery note: {}", path.display()));
                    }
                    guidance.push_str(&format!(
                        "\nWorkspace root preserved for inspection: {}",
                        run_workspace_root.display()
                    ));
                    bail!("{guidance}");
                }
                let reason = format!(
                    "workspace ancestry validation failed for {} (source {}, workspace {}): {}",
                    workspace_dir.display(),
                    &source_head[..12.min(source_head.len())],
                    &ws_head[..12.min(ws_head.len())],
                    ancestry_stderr.trim()
                );
                let note_path = write_parallel_merge_lineage_recovery_note(
                    run_id,
                    source_workdir,
                    run_workspace_root,
                    workspace_dir,
                    task_key,
                    &source_head,
                    &ws_head,
                    &reason,
                )
                .ok();
                let mut guidance = format!(
                    "{reason}\n\
Operator recovery steps:\n\
1. Inspect source merge state: git -C \"{}\" status --short\n\
2. Inspect workspace lineage: git -C \"{}\" log --oneline --decorate --graph -n 20\n\
3. Compare workspace changes: git -C \"{}\" diff --stat \"{}\"",
                    source_workdir.display(),
                    workspace_dir.display(),
                    workspace_dir.display(),
                    source_head
                );
                if let Some(path) = note_path {
                    guidance.push_str(&format!("\nDetailed recovery note: {}", path.display()));
                }
                guidance.push_str(&format!(
                    "\nWorkspace root preserved for inspection: {}",
                    run_workspace_root.display()
                ));
                bail!("{guidance}");
            }

            info!(
                "workspace {} HEAD ({}) advanced beyond source HEAD ({}); soft-resetting to surface committed changes",
                workspace_dir.display(),
                &ws_head[..12.min(ws_head.len())],
                &source_head[..12.min(source_head.len())]
            );
            let reset_args = ["reset", "--soft", source_head.as_str()];
            let reset_output = run_git_capture(workspace_dir, &reset_args)?;
            ensure_git_success(
                reset_output,
                workspace_dir,
                &reset_args,
                "soft-reset workspace to source HEAD",
            )?;
            original_workspace_head_for_restore = Some(ws_head.clone());
            soft_reset_applied = true;
        }

        let restore_workspace_head_on_error = |error_context: &str| -> Option<String> {
            let original_head = original_workspace_head_for_restore.as_deref()?;
            match restore_workspace_head_after_failed_merge(workspace_dir, original_head) {
                Ok(()) => {
                    info!(
                        workspace = %workspace_dir.display(),
                        original_head = %original_head,
                        "{error_context}; restored workspace HEAD after merge failure"
                    );
                    Some(format!(
                        "\nWorkspace restored to pre-merge HEAD: {original_head}"
                    ))
                }
                Err(restore_err) => {
                    warn!(
                        error = %restore_err,
                        workspace = %workspace_dir.display(),
                        original_head = %original_head,
                        "{error_context}; failed to restore workspace HEAD after merge failure"
                    );
                    Some(format!(
                        "\nWARNING: failed to restore workspace to pre-merge HEAD {original_head}: {restore_err}"
                    ))
                }
            }
        };

        let scoped_paths = match scoped_workspace_changed_paths(source_workdir, workspace_dir) {
            Ok(paths) => paths,
            Err(err) => {
                let restore_note =
                    restore_workspace_head_on_error("workspace scoped path computation failed")
                        .unwrap_or_default();
                return Err(err.context(format!(
                    "parallel workspace merge failed while computing scoped paths for task {task_key}{restore_note}"
                )));
            }
        };
        if scoped_paths.is_empty() {
            skipped_task_keys.push(task_key.clone());
            task_outcomes.push(ParallelTaskMergeOutcome {
                task_key: task_key.clone(),
                disposition: ParallelTaskMergeDisposition::Skipped,
                skip_reason: Some(ParallelTaskSkipReason::NoScopedPaths),
                workspace_head: Some(ws_head.clone()),
                source_head: Some(source_head.clone()),
                soft_reset_applied,
                patch_path: None,
            });
            continue;
        }

        if let Err(err) = stage_workspace_paths(workspace_dir, &scoped_paths) {
            let restore_note =
                restore_workspace_head_on_error("workspace staging failed").unwrap_or_default();
            return Err(err.context(format!(
                "parallel workspace merge failed while staging paths for task {task_key}{restore_note}"
            )));
        }
        let patch = match export_staged_workspace_patch(workspace_dir) {
            Ok(patch) => patch,
            Err(err) => {
                let restore_note = restore_workspace_head_on_error("workspace patch export failed")
                    .unwrap_or_default();
                return Err(err.context(format!(
                    "parallel workspace merge failed while exporting patch for task {task_key}{restore_note}"
                )));
            }
        };
        if patch.trim().is_empty() {
            skipped_task_keys.push(task_key.clone());
            task_outcomes.push(ParallelTaskMergeOutcome {
                task_key: task_key.clone(),
                disposition: ParallelTaskMergeDisposition::Skipped,
                skip_reason: Some(ParallelTaskSkipReason::EmptyPatch),
                workspace_head: Some(ws_head.clone()),
                source_head: Some(source_head.clone()),
                soft_reset_applied,
                patch_path: None,
            });
            continue;
        }

        let patch_path = match persist_workspace_patch_for_recovery(
            run_workspace_root,
            task_key,
            task_index,
            &patch,
        ) {
            Ok(path) => path,
            Err(err) => {
                let restore_note = restore_workspace_head_on_error(
                    "failed to persist workspace recovery patch artifact",
                )
                .unwrap_or_default();
                return Err(err.context(format!(
                    "parallel workspace merge failed while persisting patch artifact for task {task_key}{restore_note}"
                )));
            }
        };
        match apply_workspace_patch_to_source(
            source_workdir,
            task_key,
            patch.as_bytes(),
            resolution_config,
        ) {
            Ok(repaired) => {
                if repaired {
                    repair_succeeded_count += 1;
                    merge_apply_telemetry_event(
                        apply_telemetry,
                        "merge.repair.succeeded",
                        Some(task_key),
                        Some(task_index),
                        serde_json::json!({
                            "run_id": run_id.to_string(),
                            "task_index": task_index,
                            "task_key": task_key,
                            "patch_path": patch_path.display().to_string(),
                            "source_head": source_head,
                            "workspace_head": ws_head,
                            "soft_reset_applied": soft_reset_applied,
                            "repair_strategy": format!("{:?}", resolution_config.strategy),
                        }),
                    );
                }
            }
            Err(err) => {
                if err.repair_attempted {
                    repair_started_count += 1;
                    repair_failed_count += 1;
                    merge_apply_telemetry_event(
                        apply_telemetry,
                        "merge.repair.failed",
                        Some(task_key),
                        Some(task_index),
                        serde_json::json!({
                            "run_id": run_id.to_string(),
                            "task_index": task_index,
                            "task_key": task_key,
                            "patch_path": patch_path.display().to_string(),
                            "source_head": source_head,
                            "workspace_head": ws_head,
                            "soft_reset_applied": soft_reset_applied,
                            "reason": err.message,
                            "repair_strategy": format!("{:?}", resolution_config.strategy),
                        }),
                    );
                }
                if err.kind == ParallelWorkspaceMergeFailureKind::MergeConflict {
                    conflict_count += 1;
                    let repo_status = collect_workspace_status_snapshot(source_workdir);
                    let mut conflicted_files = collect_conflicted_workspace_files(source_workdir);
                    if conflicted_files.is_empty() {
                        conflicted_files = parse_unmerged_paths_from_status_snapshot(&repo_status);
                    }
                    if conflicted_files.is_empty() {
                        conflicted_files = collect_patch_target_paths(&patch);
                    }
                    let recovery_hints = merge_conflict_recovery_hints(source_workdir, &patch_path);
                    merge_apply_telemetry_event(
                        apply_telemetry,
                        "merge.apply.conflict",
                        Some(task_key),
                        Some(task_index),
                        serde_json::json!({
                            "run_id": run_id.to_string(),
                            "task_index": task_index,
                            "task_key": task_key,
                            "patch_path": patch_path.display().to_string(),
                            "workspace_path": workspace_dir.display().to_string(),
                            "source_head": source_head,
                            "workspace_head": ws_head,
                            "soft_reset_applied": soft_reset_applied,
                            "reason": err.message,
                            "conflicted_files": conflicted_files,
                            "repo_status": repo_status,
                            "recovery_hints": recovery_hints,
                        }),
                    );
                }
                let restore_note = restore_workspace_head_on_error("workspace patch apply failed")
                    .unwrap_or_default();
                let note_path = write_parallel_merge_recovery_note(
                    run_id,
                    source_workdir,
                    run_workspace_root,
                    workspace_dir,
                    task_key,
                    &patch_path,
                    original_workspace_head_for_restore.as_deref(),
                )
                .ok();
                let mut guidance = format!(
                    "parallel workspace merge failed for run {run_id} task {task_key}\n\
Operator recovery steps:\n\
1. Inspect merge state: git -C \"{}\" status --short\n\
2. Review task patch: git -C \"{}\" apply --stat \"{}\"\n\
3. Retry task patch: git -C \"{}\" apply --3way --whitespace=nowarn \"{}\"",
                    source_workdir.display(),
                    source_workdir.display(),
                    patch_path.display(),
                    source_workdir.display(),
                    patch_path.display()
                );
                if let Some(path) = note_path {
                    guidance.push_str(&format!("\nDetailed recovery note: {}", path.display()));
                }
                guidance.push_str(&restore_note);
                guidance.push_str(&format!(
                    "\nWorkspace root preserved for inspection: {}",
                    run_workspace_root.display()
                ));
                merge_apply_telemetry_event(
                    apply_telemetry,
                    "merge.apply.finalized",
                    Some(task_key),
                    Some(task_index),
                    serde_json::json!({
                        "run_id": run_id.to_string(),
                        "task_index": task_index,
                        "task_key": task_key,
                        "status": "failed",
                        "merged_task_count": merged_task_keys.len(),
                        "skipped_task_count": skipped_task_keys.len(),
                        "conflict_task_count": conflict_count,
                        "repair_started_count": repair_started_count,
                        "repair_succeeded_count": repair_succeeded_count,
                        "repair_failed_count": repair_failed_count,
                        "reason": err.message,
                    }),
                );
                return Err(err).context(format!(
                    "parallel workspace merge apply failed for task {task_key}: {guidance}"
                ));
            }
        }
        merged_task_keys.push(task_key.clone());
        task_outcomes.push(ParallelTaskMergeOutcome {
            task_key: task_key.clone(),
            disposition: ParallelTaskMergeDisposition::Merged,
            skip_reason: None,
            workspace_head: Some(ws_head.clone()),
            source_head: Some(source_head.clone()),
            soft_reset_applied,
            patch_path: Some(patch_path.display().to_string()),
        });

        // Commit the merged result so the next workspace starts with a clean
        // working tree + index. git apply --3way requires working tree == index,
        // and without committing, a prior merge's staged results would cause
        // "does not match index" errors for subsequent patches. These interim
        // commits are transparent to the user (not pushed, can be squashed).
        let _ = run_git_capture(source_workdir, &["add", "-A"]);
        let commit_msg = generated_commit_message(
            source_workdir,
            DiffSpec::Staged,
            vec![
                ("yarli-run".to_string(), run_id.to_string()),
                ("yarli-task".to_string(), task_key.clone()),
                ("yarli-workspace-head".to_string(), ws_head.clone()),
                ("yarli-source-head".to_string(), source_head.clone()),
                ("yarli-patch".to_string(), patch_path.display().to_string()),
            ],
            "chore(integration): integrate workspace result",
            Some(task_key),
        );
        let _ = run_git_capture(
            source_workdir,
            &["commit", "--no-verify", "--allow-empty", "-m", &commit_msg],
        );
    }

    merge_apply_telemetry_event(
        apply_telemetry,
        "merge.apply.finalized",
        None,
        None,
        serde_json::json!({
            "run_id": run_id.to_string(),
            "status": "succeeded",
            "task_count": task_workspaces.len(),
            "merged_task_count": merged_task_keys.len(),
            "skipped_task_count": skipped_task_keys.len(),
            "conflict_task_count": conflict_count,
            "repair_started_count": repair_started_count,
            "repair_succeeded_count": repair_succeeded_count,
            "repair_failed_count": repair_failed_count,
            "preserve_workspace_root": !skipped_task_keys.is_empty(),
            "workspace_root": run_workspace_root.display().to_string(),
        }),
    );

    // Reapply stashed dirty state. If it conflicts with workspace changes,
    // resolve conflicting files in favor of the workspace (HEAD) while
    // preserving non-conflicting stash changes (e.g. edits to lines the
    // workspace didn't touch).
    if has_stashed_dirty_state {
        let pop_result = run_git_capture(source_workdir, &["stash", "pop"]);
        let pop_ok = pop_result
            .as_ref()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !pop_ok {
            warn!("pre-existing dirty state partially conflicted with workspace merge; resolving conflicts in favor of workspace");
            // Resolve each conflicted file by keeping the workspace (ours) version.
            // Non-conflicting stash changes are already applied in the working tree.
            if let Ok(status_output) = run_git_capture(source_workdir, &["status", "--porcelain"]) {
                let status_text = String::from_utf8_lossy(&status_output.stdout);
                for line in status_text.lines() {
                    if line.len() < 3 {
                        continue;
                    }
                    let prefix = &line[..2];
                    let path = line[3..].trim();
                    match prefix {
                        "UU" | "AA" | "UD" => {
                            // Both modified / both added / ours modified, theirs deleted
                            // Keep workspace (ours) version.
                            let _ = run_git_capture(
                                source_workdir,
                                &["checkout", "--ours", "--", path],
                            );
                        }
                        "DU" => {
                            // Deleted in workspace (ours), modified in stash (theirs).
                            // Keep it deleted — workspace decision wins.
                            let _ = run_git_capture(source_workdir, &["rm", "-f", "--", path]);
                        }
                        _ => {}
                    }
                }
            }
            let _ = run_git_capture(source_workdir, &["add", "-A"]);
            let _ = run_git_capture(source_workdir, &["stash", "drop"]);
        }
        // Commit the reapplied dirty state (clean pop or resolved conflicts).
        let _ = run_git_capture(source_workdir, &["add", "-A"]);
        let reapply_msg = generated_commit_message(
            source_workdir,
            DiffSpec::Staged,
            vec![
                ("yarli-run".to_string(), run_id.to_string()),
                (
                    "yarli-restore".to_string(),
                    "pre-existing workspace state after merge".to_string(),
                ),
            ],
            "chore(workspace): restore local workspace changes",
            Some("workspace"),
        );
        let _ = run_git_capture(
            source_workdir,
            &["commit", "--no-verify", "--allow-empty", "-m", &reapply_msg],
        );
    }

    Ok(ParallelWorkspaceMergeReport {
        preserve_workspace_root: !skipped_task_keys.is_empty(),
        merged_task_keys,
        skipped_task_keys,
        task_outcomes,
    })
}

#[cfg(test)]
#[allow(clippy::needless_range_loop)]
mod tests {
    use super::*;
    use crate::plan::{PlannedTask, RunPlan};
    use crate::test_helpers::write_test_config_at;
    use std::path::PathBuf;
    use std::process::Command;
    use tempfile::TempDir;
    use uuid::Uuid;
    use yarli_core::domain::CommandClass;

    fn run_git(repo: &Path, args: &[&str]) -> (bool, String, String) {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .expect("git command should run");
        (
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    }

    fn run_git_expect_ok(repo: &Path, args: &[&str]) {
        let (ok, _stdout, stderr) = run_git(repo, args);
        assert!(ok, "git {:?} failed: {stderr}", args);
    }

    #[test]
    fn parse_unmerged_paths_from_status_snapshot_extracts_conflict_entries() {
        let status = "UU src/lib.rs\nAA shared.txt\n M README.md\nR  old.rs -> new.rs\n";
        let files = parse_unmerged_paths_from_status_snapshot(status);
        assert_eq!(
            files,
            vec!["shared.txt".to_string(), "src/lib.rs".to_string()]
        );
    }

    #[test]
    fn collect_patch_target_paths_extracts_paths_from_patch_headers() {
        let patch = "\
diff --git a/src/lib.rs b/src/lib.rs\n\
index 1111111..2222222 100644\n\
--- a/src/lib.rs\n\
+++ b/src/lib.rs\n\
@@ -1 +1 @@\n\
-old\n\
+new\n\
diff --git a/shared.txt b/shared.txt\n\
deleted file mode 100644\n\
index 3333333..0000000\n\
--- a/shared.txt\n\
+++ /dev/null\n";
        let files = collect_patch_target_paths(patch);
        assert_eq!(
            files,
            vec!["shared.txt".to_string(), "src/lib.rs".to_string()]
        );
    }

    #[test]
    fn prepare_parallel_workspace_layout_creates_task_workspaces() {
        let temp_dir = TempDir::new().unwrap();
        let repo = temp_dir.path().join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::create_dir_all(repo.join("sdks/rust_sdk/target")).unwrap();
        std::fs::create_dir_all(repo.join("node_modules/pkg")).unwrap();
        std::fs::create_dir_all(repo.join(".venv/lib")).unwrap();
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        std::fs::write(repo.join("src").join("main.rs"), "fn main() {}\n").unwrap();
        std::fs::write(
            repo.join("sdks/rust_sdk/target/cache.bin"),
            "compiled artifact",
        )
        .unwrap();
        std::fs::write(
            repo.join("node_modules/pkg/index.js"),
            "module.exports = 1;\n",
        )
        .unwrap();
        std::fs::write(repo.join(".venv/lib/python"), "python").unwrap();

        let config_path = temp_dir.path().join("yarli.toml");
        let config = format!(
            r#"
[features]
parallel = true

[execution]
working_dir = "{}"
worktree_root = "{}"
"#,
            repo.display(),
            repo.join(".yarl/workspaces").display()
        );
        let loaded = write_test_config_at(&config_path, &config);

        let task = PlannedTask {
            task_key: "I1".to_string(),
            command: "echo one".to_string(),
            command_class: CommandClass::Io,
            tranche_key: None,
            tranche_group: None,
            depends_on: Vec::new(),
            allowed_paths: Vec::new(),
        };
        let plan = RunPlan {
            objective: "test".to_string(),
            tasks: vec![
                task.clone(),
                PlannedTask {
                    task_key: "I2".to_string(),
                    command: "echo two".to_string(),
                    ..task
                },
            ],
            task_catalog: Vec::new(),
            workdir: repo.display().to_string(),
            timeout_secs: 60,
            pace: None,
            prompt_snapshot: None,
            run_spec: None,
            tranche_plan: Vec::new(),
            current_tranche_index: None,
        };

        let layout = prepare_parallel_workspace_layout(&plan, &loaded)
            .unwrap()
            .expect("parallel workspace layout should be present");
        assert_eq!(layout.task_workspace_dirs.len(), 2);
        for workspace in &layout.task_workspace_dirs {
            assert!(workspace.exists());
            assert!(workspace.join("README.md").exists());
            assert!(workspace.join("src/main.rs").exists());
            assert!(
                !workspace.join("sdks/rust_sdk/target/cache.bin").exists(),
                "workspace clone should exclude target directories by default"
            );
            assert!(
                !workspace.join("node_modules/pkg/index.js").exists(),
                "workspace clone should exclude node_modules directories by default"
            );
            assert!(
                !workspace.join(".venv/lib/python").exists(),
                "workspace clone should exclude virtualenv directories by default"
            );
            assert!(
                !workspace.join(".yarl/workspaces").exists(),
                "workspace clone should exclude nested worktree root to avoid recursion"
            );
        }
    }

    #[test]
    fn prepare_parallel_workspace_layout_ignores_excluded_names_in_repo_ancestors() {
        let temp_dir = TempDir::new().unwrap();
        let repo = temp_dir.path().join("target").join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(repo.join("src").join("main.rs"), "fn main() {}\n").unwrap();

        let config_path = temp_dir.path().join("yarli.toml");
        let config = format!(
            r#"
[features]
parallel = true

[execution]
working_dir = "{}"
worktree_root = "{}"
"#,
            repo.display(),
            repo.join(".yarl/workspaces").display()
        );
        let loaded = write_test_config_at(&config_path, &config);
        let plan = RunPlan {
            objective: "test".to_string(),
            tasks: vec![PlannedTask {
                task_key: "I1".to_string(),
                command: "echo one".to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
                depends_on: Vec::new(),
                allowed_paths: Vec::new(),
            }],
            task_catalog: Vec::new(),
            workdir: repo.display().to_string(),
            timeout_secs: 60,
            pace: None,
            prompt_snapshot: None,
            run_spec: None,
            tranche_plan: Vec::new(),
            current_tranche_index: None,
        };

        let layout = prepare_parallel_workspace_layout(&plan, &loaded)
            .unwrap()
            .expect("parallel workspace layout should be present");
        assert_eq!(layout.task_workspace_dirs.len(), 1);
        assert!(
            layout.task_workspace_dirs[0].join("src/main.rs").exists(),
            "repo ancestor named `target` should not trigger workspace exclusions"
        );
    }

    #[test]
    fn resolve_workspace_copy_exclusions_supports_paths_and_dir_names() {
        let temp_dir = TempDir::new().unwrap();
        let source = temp_dir.path().join("repo");
        std::fs::create_dir_all(&source).unwrap();
        let loaded = write_test_config_at(
            &temp_dir.path().join("yarli.toml"),
            r#"
[execution]
worktree_exclude_paths = ["target", "**/dist", "sdks/rust_sdk/target"]
"#,
        );

        let (roots, dir_names) = resolve_workspace_copy_exclusions(&source, &loaded);
        assert!(dir_names.iter().any(|name| name == "target"));
        assert!(dir_names.iter().any(|name| name == "dist"));
        assert!(roots
            .iter()
            .any(|root| root == &source.join("sdks/rust_sdk/target")));
    }

    #[test]
    fn prepare_parallel_workspace_layout_rejects_equal_root_and_workdir() {
        let temp_dir = TempDir::new().unwrap();
        let repo = temp_dir.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("README.md"), "hello").unwrap();

        let config_path = temp_dir.path().join("yarli.toml");
        let config = format!(
            r#"
[features]
parallel = true

[execution]
working_dir = "{}"
worktree_root = "{}"
"#,
            repo.display(),
            repo.display()
        );
        let loaded = write_test_config_at(&config_path, &config);

        let plan = RunPlan {
            objective: "test".to_string(),
            tasks: vec![PlannedTask {
                task_key: "I1".to_string(),
                command: "echo one".to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
                depends_on: Vec::new(),
                allowed_paths: Vec::new(),
            }],
            task_catalog: Vec::new(),
            workdir: repo.display().to_string(),
            timeout_secs: 60,
            pace: None,
            prompt_snapshot: None,
            run_spec: None,
            tranche_plan: Vec::new(),
            current_tranche_index: None,
        };

        let err = prepare_parallel_workspace_layout(&plan, &loaded).unwrap_err();
        assert!(err
            .to_string()
            .contains("execution.worktree_root must not equal execution.working_dir"));
    }

    #[test]
    fn merge_parallel_workspace_results_applies_non_conflicting_workspace_changes() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("alpha.txt"), "alpha base\n").unwrap();
        std::fs::write(source_repo.join("beta.txt"), "beta base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace_one = temp_dir.path().join("workspace-one");
        let workspace_two = temp_dir.path().join("workspace-two");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_one_str = workspace_one.to_str().unwrap();
        let workspace_two_str = workspace_two.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_one_str],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_two_str],
        );

        std::fs::write(
            workspace_one.join("alpha.txt"),
            "alpha merged from workspace one\n",
        )
        .unwrap();
        std::fs::write(
            workspace_two.join("beta.txt"),
            "beta merged from workspace two\n",
        )
        .unwrap();

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[
                ("task-alpha".to_string(), workspace_one.clone()),
                ("task-beta".to_string(), workspace_two.clone()),
            ],
        )
        .unwrap();

        assert_eq!(
            report.merged_task_keys,
            vec!["task-alpha".to_string(), "task-beta".to_string()]
        );
        assert!(report.skipped_task_keys.is_empty());
        assert_eq!(
            std::fs::read_to_string(source_repo.join("alpha.txt")).unwrap(),
            "alpha merged from workspace one\n"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("beta.txt")).unwrap(),
            "beta merged from workspace two\n"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_merges_staged_only_workspace_changes() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("alpha.txt"), "alpha base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace = temp_dir.path().join("workspace-one");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_str = workspace.to_str().unwrap();
        run_git_expect_ok(temp_dir.path(), &["clone", source_repo_str, workspace_str]);

        std::fs::write(workspace.join("alpha.txt"), "alpha staged-only change\n").unwrap();
        run_git_expect_ok(&workspace, &["add", "alpha.txt"]);
        let (_ok, status_stdout, _stderr) = run_git(&workspace, &["status", "--porcelain"]);
        assert!(
            status_stdout.starts_with("M  alpha.txt"),
            "expected staged-only workspace change, got: {status_stdout:?}"
        );

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[("task-alpha".to_string(), workspace.clone())],
        )
        .unwrap();

        assert_eq!(report.merged_task_keys, vec!["task-alpha".to_string()]);
        assert!(report.skipped_task_keys.is_empty());
        assert_eq!(
            std::fs::read_to_string(source_repo.join("alpha.txt")).unwrap(),
            "alpha staged-only change\n"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_detects_committed_changes() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("file.txt"), "original\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace = temp_dir.path().join("workspace");
        run_git_expect_ok(
            temp_dir.path(),
            &[
                "clone",
                source_repo.to_str().unwrap(),
                workspace.to_str().unwrap(),
            ],
        );
        run_git_expect_ok(&workspace, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&workspace, &["config", "user.name", "Yarli Test"]);

        std::fs::write(workspace.join("file.txt"), "modified by worker\n").unwrap();
        std::fs::write(workspace.join("new-file.txt"), "brand new file\n").unwrap();
        run_git_expect_ok(&workspace, &["add", "."]);
        run_git_expect_ok(&workspace, &["commit", "-m", "worker implementation"]);

        let (_, status_out, _) = run_git(&workspace, &["status", "--porcelain"]);
        assert!(
            status_out.trim().is_empty(),
            "workspace should be clean after commit"
        );

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[("task-committed".to_string(), workspace.clone())],
        )
        .unwrap();

        assert_eq!(report.merged_task_keys, vec!["task-committed".to_string()]);
        assert!(report.skipped_task_keys.is_empty());
        assert_eq!(
            std::fs::read_to_string(source_repo.join("file.txt")).unwrap(),
            "modified by worker\n"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("new-file.txt")).unwrap(),
            "brand new file\n"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_handles_mixed_committed_and_uncommitted() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("committed.txt"), "original\n").unwrap();
        std::fs::write(source_repo.join("uncommitted.txt"), "original\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace = temp_dir.path().join("workspace");
        run_git_expect_ok(
            temp_dir.path(),
            &[
                "clone",
                source_repo.to_str().unwrap(),
                workspace.to_str().unwrap(),
            ],
        );
        run_git_expect_ok(&workspace, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&workspace, &["config", "user.name", "Yarli Test"]);

        std::fs::write(workspace.join("committed.txt"), "committed change\n").unwrap();
        run_git_expect_ok(&workspace, &["add", "."]);
        run_git_expect_ok(&workspace, &["commit", "-m", "worker commit"]);

        std::fs::write(workspace.join("uncommitted.txt"), "uncommitted change\n").unwrap();

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[("task-mixed".to_string(), workspace.clone())],
        )
        .unwrap();

        assert_eq!(report.merged_task_keys, vec!["task-mixed".to_string()]);
        assert!(report.skipped_task_keys.is_empty());
        assert_eq!(
            std::fs::read_to_string(source_repo.join("committed.txt")).unwrap(),
            "committed change\n"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("uncommitted.txt")).unwrap(),
            "uncommitted change\n"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_multiple_commits_in_workspace() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("file.txt"), "base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace = temp_dir.path().join("workspace");
        run_git_expect_ok(
            temp_dir.path(),
            &[
                "clone",
                source_repo.to_str().unwrap(),
                workspace.to_str().unwrap(),
            ],
        );
        run_git_expect_ok(&workspace, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&workspace, &["config", "user.name", "Yarli Test"]);

        std::fs::write(workspace.join("file.txt"), "first edit\n").unwrap();
        run_git_expect_ok(&workspace, &["add", "."]);
        run_git_expect_ok(&workspace, &["commit", "-m", "first"]);

        std::fs::write(workspace.join("file.txt"), "second edit\n").unwrap();
        std::fs::write(workspace.join("extra.txt"), "extra file\n").unwrap();
        run_git_expect_ok(&workspace, &["add", "."]);
        run_git_expect_ok(&workspace, &["commit", "-m", "second"]);

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[("task-multi".to_string(), workspace.clone())],
        )
        .unwrap();

        assert_eq!(report.merged_task_keys, vec!["task-multi".to_string()]);
        assert_eq!(
            std::fs::read_to_string(source_repo.join("file.txt")).unwrap(),
            "second edit\n"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("extra.txt")).unwrap(),
            "extra file\n"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_rejects_non_descendant_workspace() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("file.txt"), "original\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace = temp_dir.path().join("workspace");
        run_git_expect_ok(
            temp_dir.path(),
            &[
                "clone",
                source_repo.to_str().unwrap(),
                workspace.to_str().unwrap(),
            ],
        );
        run_git_expect_ok(&workspace, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&workspace, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("file.txt"), "source advanced\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "source advance"]);
        run_git_expect_ok(&workspace, &["fetch", "origin"]);

        std::fs::write(workspace.join("file.txt"), "worker change\n").unwrap();
        run_git_expect_ok(&workspace, &["add", "."]);
        run_git_expect_ok(&workspace, &["commit", "-m", "worker commit"]);

        let result = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[("task-diverged".to_string(), workspace.clone())],
        );
        assert!(
            result.is_err(),
            "expected error for non-descendant workspace"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("not a descendant"),
            "error should mention non-descendant ancestry: {err_msg}"
        );
        let note_path = run_workspace_root.join("PARALLEL_MERGE_RECOVERY.txt");
        assert!(
            note_path.exists(),
            "expected recovery note at {}",
            note_path.display()
        );
        let note = std::fs::read_to_string(&note_path).unwrap();
        assert!(
            note.contains("Operator recovery steps"),
            "expected operator guidance in recovery note: {note}"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("file.txt")).unwrap(),
            "source advanced\n"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_ignores_baseline_untracked_artifacts() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("alpha.txt"), "alpha base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace_one = temp_dir.path().join("workspace-one");
        let workspace_two = temp_dir.path().join("workspace-two");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_one_str = workspace_one.to_str().unwrap();
        let workspace_two_str = workspace_two.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_one_str],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_two_str],
        );

        let artifact_rel = Path::new("tmp/cache.bin");
        let source_artifact = source_repo.join(artifact_rel);
        let ws1_artifact = workspace_one.join(artifact_rel);
        let ws2_artifact = workspace_two.join(artifact_rel);
        std::fs::create_dir_all(source_artifact.parent().unwrap()).unwrap();
        std::fs::create_dir_all(ws1_artifact.parent().unwrap()).unwrap();
        std::fs::create_dir_all(ws2_artifact.parent().unwrap()).unwrap();
        std::fs::write(&source_artifact, "baseline artifact\n").unwrap();
        std::fs::write(&ws1_artifact, "baseline artifact\n").unwrap();
        std::fs::write(&ws2_artifact, "baseline artifact\n").unwrap();

        std::fs::write(workspace_one.join("alpha.txt"), "alpha merged by task\n").unwrap();

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[
                ("task-alpha".to_string(), workspace_one.clone()),
                ("task-beta".to_string(), workspace_two.clone()),
            ],
        )
        .unwrap();

        assert_eq!(report.merged_task_keys, vec!["task-alpha".to_string()]);
        assert_eq!(report.skipped_task_keys, vec!["task-beta".to_string()]);
        assert_eq!(
            std::fs::read_to_string(source_repo.join("alpha.txt")).unwrap(),
            "alpha merged by task\n"
        );
        assert_eq!(
            std::fs::read_to_string(source_artifact).unwrap(),
            "baseline artifact\n"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_ignores_build_artifacts() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("alpha.txt"), "alpha base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace_one = temp_dir.path().join("workspace-one");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_one_str = workspace_one.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_one_str],
        );

        std::fs::create_dir_all(workspace_one.join("tests/fixtures/rust-sample/target")).unwrap();
        std::fs::write(
            workspace_one.join("tests/fixtures/rust-sample/target/.rustc_info.json"),
            "generated artifact\n",
        )
        .unwrap();
        std::fs::write(workspace_one.join("alpha.txt"), "alpha merged by task\n").unwrap();

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[("task-alpha".to_string(), workspace_one.clone())],
        )
        .unwrap();

        assert_eq!(report.merged_task_keys, vec!["task-alpha".to_string()]);
        assert!(report.skipped_task_keys.is_empty());
        assert_eq!(
            std::fs::read_to_string(source_repo.join("alpha.txt")).unwrap(),
            "alpha merged by task\n"
        );
        assert!(
            !source_repo
                .join("tests/fixtures/rust-sample/target/.rustc_info.json")
                .exists(),
            "build artifact should not be merged from workspace"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_ignores_unrelated_tracked_drift() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("alpha.txt"), "alpha base\n").unwrap();
        std::fs::write(source_repo.join("beta.txt"), "beta base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace_one = temp_dir.path().join("workspace-one");
        let workspace_two = temp_dir.path().join("workspace-two");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_one_str = workspace_one.to_str().unwrap();
        let workspace_two_str = workspace_two.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_one_str],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_two_str],
        );

        std::fs::write(source_repo.join("beta.txt"), "beta local drift\n").unwrap();
        std::fs::write(workspace_one.join("beta.txt"), "beta local drift\n").unwrap();
        std::fs::write(workspace_two.join("beta.txt"), "beta local drift\n").unwrap();

        std::fs::write(
            workspace_one.join("alpha.txt"),
            "alpha merged from workspace one\n",
        )
        .unwrap();
        std::fs::write(
            workspace_two.join("gamma.txt"),
            "gamma from workspace two\n",
        )
        .unwrap();

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[
                ("task-alpha".to_string(), workspace_one.clone()),
                ("task-gamma".to_string(), workspace_two.clone()),
            ],
        )
        .unwrap();

        assert_eq!(
            report.merged_task_keys,
            vec!["task-alpha".to_string(), "task-gamma".to_string()]
        );
        assert!(report.skipped_task_keys.is_empty());
        assert_eq!(
            std::fs::read_to_string(source_repo.join("alpha.txt")).unwrap(),
            "alpha merged from workspace one\n"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("gamma.txt")).unwrap(),
            "gamma from workspace two\n"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("beta.txt")).unwrap(),
            "beta local drift\n"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_merges_non_overlapping_hunks_in_same_file() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        let baseline = "line1\nline2\nline3\nline4\nline5\nline6\n";
        std::fs::write(source_repo.join("shared.txt"), baseline).unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace_one = temp_dir.path().join("workspace-one");
        let workspace_two = temp_dir.path().join("workspace-two");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_one_str = workspace_one.to_str().unwrap();
        let workspace_two_str = workspace_two.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_one_str],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_two_str],
        );

        std::fs::write(
            workspace_one.join("shared.txt"),
            "line1\nline2 task-one\nline3\nline4\nline5\nline6\n",
        )
        .unwrap();
        std::fs::write(
            workspace_two.join("shared.txt"),
            "line1\nline2\nline3\nline4\nline5 task-two\nline6\n",
        )
        .unwrap();

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[
                ("task-one".to_string(), workspace_one.clone()),
                ("task-two".to_string(), workspace_two.clone()),
            ],
        )
        .unwrap();
        assert_eq!(
            report.merged_task_keys,
            vec!["task-one".to_string(), "task-two".to_string()]
        );
        assert!(report.skipped_task_keys.is_empty());

        let merged = std::fs::read_to_string(source_repo.join("shared.txt")).unwrap();
        assert!(
            merged.contains("line2 task-one"),
            "merged contents: {merged}"
        );
        assert!(
            merged.contains("line5 task-two"),
            "merged contents: {merged}"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_applies_permission_only_changes() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        let script_path = source_repo.join("script.sh");
        std::fs::write(&script_path, "#!/usr/bin/env bash\necho hi\n").unwrap();
        let mut source_perms = std::fs::metadata(&script_path).unwrap().permissions();
        source_perms.set_mode(0o644);
        std::fs::set_permissions(&script_path, source_perms).unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace = temp_dir.path().join("workspace-one");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_str = workspace.to_str().unwrap();
        run_git_expect_ok(temp_dir.path(), &["clone", source_repo_str, workspace_str]);

        let workspace_script = workspace.join("script.sh");
        let mut workspace_perms = std::fs::metadata(&workspace_script).unwrap().permissions();
        workspace_perms.set_mode(0o755);
        std::fs::set_permissions(&workspace_script, workspace_perms).unwrap();

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[("task-mode".to_string(), workspace.clone())],
        )
        .unwrap();
        assert_eq!(report.merged_task_keys, vec!["task-mode".to_string()]);
        assert!(report.skipped_task_keys.is_empty());

        let source_mode = std::fs::metadata(script_path).unwrap().permissions().mode();
        assert_ne!(
            source_mode & 0o111,
            0,
            "expected executable bit to be merged"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_applies_committed_permission_only_changes() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        let script_path = source_repo.join("script.sh");
        std::fs::write(&script_path, "#!/usr/bin/env bash\necho hi\n").unwrap();
        let mut source_perms = std::fs::metadata(&script_path).unwrap().permissions();
        source_perms.set_mode(0o644);
        std::fs::set_permissions(&script_path, source_perms).unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace = temp_dir.path().join("workspace-one");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_str = workspace.to_str().unwrap();
        run_git_expect_ok(temp_dir.path(), &["clone", source_repo_str, workspace_str]);
        run_git_expect_ok(&workspace, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&workspace, &["config", "user.name", "Yarli Test"]);

        let workspace_script = workspace.join("script.sh");
        let mut workspace_perms = std::fs::metadata(&workspace_script).unwrap().permissions();
        workspace_perms.set_mode(0o755);
        std::fs::set_permissions(&workspace_script, workspace_perms).unwrap();
        run_git_expect_ok(&workspace, &["add", "script.sh"]);
        run_git_expect_ok(&workspace, &["commit", "-m", "chmod +x script"]);

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[("task-mode-committed".to_string(), workspace.clone())],
        )
        .unwrap();
        assert_eq!(
            report.merged_task_keys,
            vec!["task-mode-committed".to_string()]
        );
        assert!(report.skipped_task_keys.is_empty());

        let source_mode = std::fs::metadata(script_path).unwrap().permissions().mode();
        assert_ne!(
            source_mode & 0o111,
            0,
            "expected committed executable bit to be merged"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_returns_error_on_conflicting_workspace_changes() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("shared.txt"), "base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace_one = temp_dir.path().join("workspace-one");
        let workspace_two = temp_dir.path().join("workspace-two");
        let source_repo_str = source_repo.to_str().unwrap();
        let workspace_one_str = workspace_one.to_str().unwrap();
        let workspace_two_str = workspace_two.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_one_str],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_two_str],
        );

        std::fs::write(workspace_one.join("shared.txt"), "workspace one change\n").unwrap();
        std::fs::write(workspace_two.join("shared.txt"), "workspace two change\n").unwrap();

        let run_id = Uuid::now_v7();
        let err = merge_parallel_workspace_results(
            &source_repo,
            run_id,
            &run_workspace_root,
            &[
                ("task-one".to_string(), workspace_one.clone()),
                ("task-two".to_string(), workspace_two.clone()),
            ],
        )
        .unwrap_err();

        let err_text = err.to_string();
        assert!(err_text.contains("task task-two"), "{err_text}");
        assert!(err_text.contains("Operator recovery steps"), "{err_text}");
        let note_path = run_workspace_root.join("PARALLEL_MERGE_RECOVERY.txt");
        assert!(
            note_path.exists(),
            "expected recovery note at {}",
            note_path.display()
        );
        let note = std::fs::read_to_string(&note_path).unwrap();
        assert!(note.contains(&run_id.to_string()));
        assert!(note.contains("task-two"));
        let merged_contents = std::fs::read_to_string(source_repo.join("shared.txt")).unwrap();
        assert!(
            merged_contents.contains("<<<<<<<"),
            "expected conflict markers in merge result: {merged_contents}"
        );
        assert!(
            merged_contents.contains("workspace one change"),
            "expected first workspace change to remain visible: {merged_contents}"
        );
        assert!(
            merged_contents.contains("workspace two change"),
            "expected second workspace change to remain visible: {merged_contents}"
        );
    }

    #[test]
    fn apply_workspace_patch_to_source_auto_repair_prefers_workspace_version() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        let workspace = temp_dir.path().join("workspace");
        std::fs::create_dir_all(&source_repo).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("shared.txt"), "base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        run_git_expect_ok(
            temp_dir.path(),
            &[
                "clone",
                source_repo.to_str().unwrap(),
                workspace.to_str().unwrap(),
            ],
        );

        std::fs::write(source_repo.join("shared.txt"), "source update\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "shared.txt"]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "source update"]);

        std::fs::write(workspace.join("shared.txt"), "workspace update\n").unwrap();
        let (ok, patch, stderr) = run_git(&workspace, &["diff"]);
        assert!(ok, "failed to produce workspace patch: {stderr}");

        let resolution_config = MergeResolutionConfig {
            strategy: config::MergeConflictResolution::AutoRepair,
            repair_command: None,
            repair_timeout_seconds: 300,
        };
        let repaired = apply_workspace_patch_to_source(
            &source_repo,
            "task-auto-repair",
            patch.as_bytes(),
            &resolution_config,
        )
        .unwrap();
        assert!(repaired);

        let merged = std::fs::read_to_string(source_repo.join("shared.txt")).unwrap();
        assert_eq!(merged, "workspace update\n");
        let (_ok, status, _stderr) = run_git(&source_repo, &["status", "--porcelain"]);
        assert!(
            !status.contains("UU"),
            "expected no unresolved conflict markers after auto-repair: {status}"
        );
        assert!(
            !merged.contains("<<<<<<<"),
            "expected resolved file content after auto-repair: {merged}"
        );
    }

    #[test]
    fn merge_parallel_workspace_results_restores_workspace_head_after_failed_apply() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);
        std::fs::write(source_repo.join("shared.txt"), "base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let workspace_one = temp_dir.path().join("workspace-one");
        let workspace_two = temp_dir.path().join("workspace-two");
        let source_repo_str = source_repo.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_one.to_str().unwrap()],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_repo_str, workspace_two.to_str().unwrap()],
        );
        run_git_expect_ok(&workspace_one, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&workspace_one, &["config", "user.name", "Yarli Test"]);
        run_git_expect_ok(&workspace_two, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&workspace_two, &["config", "user.name", "Yarli Test"]);

        std::fs::write(workspace_one.join("shared.txt"), "workspace one change\n").unwrap();
        run_git_expect_ok(&workspace_one, &["add", "shared.txt"]);
        run_git_expect_ok(&workspace_one, &["commit", "-m", "workspace one commit"]);

        std::fs::write(workspace_two.join("shared.txt"), "workspace two change\n").unwrap();
        run_git_expect_ok(&workspace_two, &["add", "shared.txt"]);
        run_git_expect_ok(&workspace_two, &["commit", "-m", "workspace two commit"]);

        let (_ok, ws2_head_stdout, _stderr) = run_git(&workspace_two, &["rev-parse", "HEAD"]);
        let ws2_head_before_merge = ws2_head_stdout.trim().to_string();

        let run_id = Uuid::now_v7();
        let err = merge_parallel_workspace_results(
            &source_repo,
            run_id,
            &run_workspace_root,
            &[
                ("task-one".to_string(), workspace_one.clone()),
                ("task-two".to_string(), workspace_two.clone()),
            ],
        )
        .unwrap_err();
        let err_text = err.to_string();
        assert!(err_text.contains("task task-two"), "{err_text}");
        assert!(
            err_text.contains("Workspace restored to pre-merge HEAD"),
            "{err_text}"
        );

        let (_ok, ws2_head_after_stdout, _stderr) = run_git(&workspace_two, &["rev-parse", "HEAD"]);
        assert_eq!(ws2_head_after_stdout.trim(), ws2_head_before_merge);
        let (_ok, ws2_status_stdout, _stderr) = run_git(&workspace_two, &["status", "--porcelain"]);
        assert!(
            ws2_status_stdout.trim().is_empty(),
            "workspace should be clean after automatic head restore: {ws2_status_stdout:?}"
        );
        assert_eq!(
            std::fs::read_to_string(workspace_two.join("shared.txt")).unwrap(),
            "workspace two change\n"
        );

        let note_path = run_workspace_root.join("PARALLEL_MERGE_RECOVERY.txt");
        assert!(
            note_path.exists(),
            "expected recovery note at {}",
            note_path.display()
        );
        let note = std::fs::read_to_string(&note_path).unwrap();
        assert!(note.contains(&run_id.to_string()));
        assert!(note.contains("Original workspace HEAD before merge normalization"));
        assert!(note.contains(&ws2_head_before_merge));
        assert!(
            note.contains("reset --hard"),
            "expected reset guidance in recovery note: {note}"
        );
    }

    /// Integration test 7: 3 workspaces merging non-conflicting edits to different files
    /// plus a new file addition, verifying all 3 changes land.
    #[test]
    fn parallel_workspace_merge_three_workspaces_with_new_file() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("alpha.txt"), "alpha base\n").unwrap();
        std::fs::write(source_repo.join("beta.txt"), "beta base\n").unwrap();
        std::fs::write(source_repo.join("gamma.txt"), "gamma base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let ws1 = temp_dir.path().join("ws1");
        let ws2 = temp_dir.path().join("ws2");
        let ws3 = temp_dir.path().join("ws3");
        let source_str = source_repo.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws1.to_str().unwrap()],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws2.to_str().unwrap()],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws3.to_str().unwrap()],
        );

        // ws1 edits alpha
        std::fs::write(ws1.join("alpha.txt"), "alpha modified by ws1\n").unwrap();
        // ws2 edits beta
        std::fs::write(ws2.join("beta.txt"), "beta modified by ws2\n").unwrap();
        // ws3 adds delta.txt (new file)
        std::fs::write(ws3.join("delta.txt"), "delta new file from ws3\n").unwrap();

        let run_id = Uuid::now_v7();
        let report = merge_parallel_workspace_results(
            &source_repo,
            run_id,
            &run_workspace_root,
            &[
                ("task-alpha".to_string(), ws1.clone()),
                ("task-beta".to_string(), ws2.clone()),
                ("task-delta".to_string(), ws3.clone()),
            ],
        )
        .unwrap();

        // All 3 task keys should be merged
        assert_eq!(report.merged_task_keys.len(), 3);
        assert!(report.merged_task_keys.contains(&"task-alpha".to_string()));
        assert!(report.merged_task_keys.contains(&"task-beta".to_string()));
        assert!(report.merged_task_keys.contains(&"task-delta".to_string()));
        assert!(report.skipped_task_keys.is_empty());

        // Source repo should have all changes
        assert_eq!(
            std::fs::read_to_string(source_repo.join("alpha.txt")).unwrap(),
            "alpha modified by ws1\n"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("beta.txt")).unwrap(),
            "beta modified by ws2\n"
        );
        assert!(
            source_repo.join("delta.txt").exists(),
            "delta.txt should exist in source repo"
        );
        assert_eq!(
            std::fs::read_to_string(source_repo.join("delta.txt")).unwrap(),
            "delta new file from ws3\n"
        );

        // gamma.txt should remain unchanged
        assert_eq!(
            std::fs::read_to_string(source_repo.join("gamma.txt")).unwrap(),
            "gamma base\n"
        );

        // No conflict markers in any file
        for filename in ["alpha.txt", "beta.txt", "gamma.txt", "delta.txt"] {
            let contents = std::fs::read_to_string(source_repo.join(filename)).unwrap();
            assert!(
                !contents.contains("<<<<<<<"),
                "no conflict markers expected in {filename}: {contents}"
            );
        }

        // Patch files should be persisted for recovery
        let merge_patches = run_workspace_root.join("merge-patches");
        assert!(
            merge_patches.exists(),
            "merge-patches directory should exist at {}",
            merge_patches.display()
        );
    }

    /// Integration test 8: Deletions, permissions, same-file non-overlapping hunks combined.
    #[test]
    fn parallel_workspace_edge_cases_combined() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        // Create main.rs with 20 lines (enough separation for non-overlapping hunks)
        let mut main_rs_lines: Vec<String> = (1..=20)
            .map(|i| format!("line {i}: original content"))
            .collect();
        let main_rs_content = main_rs_lines.join("\n") + "\n";
        std::fs::write(source_repo.join("main.rs"), &main_rs_content).unwrap();
        std::fs::write(source_repo.join("lib.rs"), "// library code\n").unwrap();
        std::fs::write(source_repo.join("test.rs"), "// test code\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let ws1 = temp_dir.path().join("ws1");
        let ws2 = temp_dir.path().join("ws2");
        let source_str = source_repo.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws1.to_str().unwrap()],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws2.to_str().unwrap()],
        );

        // ws1: edit main.rs lines 1-3 (top), delete test.rs
        main_rs_lines[0] = "line 1: ws1 edit".to_string();
        main_rs_lines[1] = "line 2: ws1 edit".to_string();
        main_rs_lines[2] = "line 3: ws1 edit".to_string();
        let ws1_main = main_rs_lines.join("\n") + "\n";
        std::fs::write(ws1.join("main.rs"), &ws1_main).unwrap();
        std::fs::remove_file(ws1.join("test.rs")).unwrap();

        // ws2: edit main.rs lines 18-20 (bottom, non-overlapping), make lib.rs executable
        // Reset main_rs_lines to original for ws2
        let mut ws2_lines: Vec<String> = (1..=20)
            .map(|i| format!("line {i}: original content"))
            .collect();
        ws2_lines[17] = "line 18: ws2 edit".to_string();
        ws2_lines[18] = "line 19: ws2 edit".to_string();
        ws2_lines[19] = "line 20: ws2 edit".to_string();
        let ws2_main = ws2_lines.join("\n") + "\n";
        std::fs::write(ws2.join("main.rs"), &ws2_main).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let lib_rs = ws2.join("lib.rs");
            let mut perms = std::fs::metadata(&lib_rs).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&lib_rs, perms).unwrap();
        }

        let report = merge_parallel_workspace_results(
            &source_repo,
            Uuid::now_v7(),
            &run_workspace_root,
            &[
                ("task-ws1".to_string(), ws1.clone()),
                ("task-ws2".to_string(), ws2.clone()),
            ],
        )
        .unwrap();

        // Both workspaces should merge successfully
        assert_eq!(report.merged_task_keys.len(), 2);
        assert!(report.skipped_task_keys.is_empty());

        // test.rs should be deleted in source
        assert!(
            !source_repo.join("test.rs").exists(),
            "test.rs should be deleted from source"
        );

        // main.rs should contain both ws1 and ws2 edits (non-overlapping hunks)
        let merged_main = std::fs::read_to_string(source_repo.join("main.rs")).unwrap();
        assert!(
            merged_main.contains("line 1: ws1 edit"),
            "main.rs should have ws1 edits at top: {merged_main}"
        );
        assert!(
            merged_main.contains("line 2: ws1 edit"),
            "main.rs should have ws1 edits: {merged_main}"
        );
        assert!(
            merged_main.contains("line 18: ws2 edit"),
            "main.rs should have ws2 edits at bottom: {merged_main}"
        );
        assert!(
            merged_main.contains("line 20: ws2 edit"),
            "main.rs should have ws2 edits: {merged_main}"
        );

        // No conflict markers
        assert!(
            !merged_main.contains("<<<<<<<"),
            "no conflict markers in main.rs: {merged_main}"
        );

        // lib.rs should have executable bit (unix only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let lib_mode = std::fs::metadata(source_repo.join("lib.rs"))
                .unwrap()
                .permissions()
                .mode();
            assert_ne!(lib_mode & 0o111, 0, "lib.rs should have executable bit set");
        }
    }

    /// Integration test 9: Two workspaces both create the same new file and both edit the
    /// same existing file at non-overlapping locations. Reproduces the production failure
    /// where `git apply` refuses with "already exists in working directory" for new files
    /// and "patch does not apply" for context mismatches.
    #[test]
    fn parallel_workspace_merge_overlapping_new_files_and_edits() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();
        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        // Create PLAN.md with 30 lines — enough separation for non-overlapping hunks
        let plan_lines: Vec<String> = (1..=30)
            .map(|i| format!("plan line {i}: original content"))
            .collect();
        let plan_content = plan_lines.join("\n") + "\n";
        std::fs::write(source_repo.join("PLAN.md"), &plan_content).unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        let ws1 = temp_dir.path().join("ws1");
        let ws2 = temp_dir.path().join("ws2");
        let source_str = source_repo.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws1.to_str().unwrap()],
        );
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws2.to_str().unwrap()],
        );

        // ws1: create .evidence/report.md + edit PLAN.md lines 1-3
        std::fs::create_dir_all(ws1.join(".evidence")).unwrap();
        std::fs::write(ws1.join(".evidence/report.md"), "ws1 report content\n").unwrap();
        let mut ws1_plan = plan_lines.clone();
        for i in 0..3 {
            ws1_plan[i] = format!("plan line {}: ws1 edit", i + 1);
        }
        std::fs::write(ws1.join("PLAN.md"), ws1_plan.join("\n") + "\n").unwrap();

        // ws2: create .evidence/report.md (same path!) + edit PLAN.md lines 28-30
        std::fs::create_dir_all(ws2.join(".evidence")).unwrap();
        std::fs::write(ws2.join(".evidence/report.md"), "ws2 report content\n").unwrap();
        let mut ws2_plan = plan_lines.clone();
        for i in 27..30 {
            ws2_plan[i] = format!("plan line {}: ws2 edit", i + 1);
        }
        std::fs::write(ws2.join("PLAN.md"), ws2_plan.join("\n") + "\n").unwrap();

        let run_id = Uuid::now_v7();
        let report = merge_parallel_workspace_results(
            &source_repo,
            run_id,
            &run_workspace_root,
            &[
                ("task-ws1".to_string(), ws1.clone()),
                ("task-ws2".to_string(), ws2.clone()),
            ],
        )
        .expect("merge should succeed despite overlapping new files and edits");

        assert_eq!(report.merged_task_keys.len(), 2);
        assert!(report.skipped_task_keys.is_empty());

        // .evidence/report.md should exist — ws2's version wins (merged second)
        let report_content =
            std::fs::read_to_string(source_repo.join(".evidence/report.md")).unwrap();
        assert_eq!(
            report_content, "ws2 report content\n",
            "ws2 (merged second) should overwrite ws1's version of the new file"
        );

        // PLAN.md should contain both ws1 and ws2 edits (non-overlapping hunks via --3way)
        let merged_plan = std::fs::read_to_string(source_repo.join("PLAN.md")).unwrap();
        assert!(
            merged_plan.contains("plan line 1: ws1 edit"),
            "PLAN.md should have ws1 edits at top: {merged_plan}"
        );
        assert!(
            merged_plan.contains("plan line 3: ws1 edit"),
            "PLAN.md should have ws1 edits: {merged_plan}"
        );
        assert!(
            merged_plan.contains("plan line 28: ws2 edit"),
            "PLAN.md should have ws2 edits at bottom: {merged_plan}"
        );
        assert!(
            merged_plan.contains("plan line 30: ws2 edit"),
            "PLAN.md should have ws2 edits: {merged_plan}"
        );

        // No conflict markers
        assert!(
            !merged_plan.contains("<<<<<<<"),
            "no conflict markers expected in PLAN.md: {merged_plan}"
        );
    }

    /// Integration test 10: Simulates cross-run dirty state where the source working tree
    /// has uncommitted changes from a prior merge, and a new workspace patch has context
    /// drift on the same file plus creates a new file that already exists in source.
    /// Reproduces the production failure where --3way --check rejects the patch with
    /// "does not match index" but --3way apply would actually succeed via 3-way merge.
    #[test]
    fn parallel_workspace_merge_with_prior_dirty_state_and_context_drift() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        // Create PLAN.md with 30 lines
        let plan_lines: Vec<String> = (1..=30)
            .map(|i| format!("plan line {i}: original"))
            .collect();
        std::fs::write(source_repo.join("PLAN.md"), plan_lines.join("\n") + "\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        // --- Simulate run 1 merge: modify source working tree (uncommitted) ---
        // This represents a prior yarli run's merge that modified lines 1-3 and created a report.
        let mut prior_plan = plan_lines.clone();
        for i in 0..3 {
            prior_plan[i] = format!("plan line {}: prior run edit", i + 1);
        }
        std::fs::write(source_repo.join("PLAN.md"), prior_plan.join("\n") + "\n").unwrap();
        std::fs::create_dir_all(source_repo.join(".evidence")).unwrap();
        std::fs::write(
            source_repo.join(".evidence/report.md"),
            "prior run report\n",
        )
        .unwrap();
        // Commit the changes (simulating the temp commit after a prior run's merge)
        run_git_expect_ok(&source_repo, &["add", "PLAN.md", ".evidence/report.md"]);
        run_git_expect_ok(
            &source_repo,
            &["commit", "--no-verify", "-m", "yarli: merge prior run"],
        );

        // --- Now simulate run 2: clone from HEAD (not the dirty state) ---
        let ws = temp_dir.path().join("ws-run2");
        let source_str = source_repo.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws.to_str().unwrap()],
        );

        // ws modifies lines 25-27 (non-overlapping with prior run's 1-3) and creates
        // a new version of .evidence/report.md. The workspace cloned from the committed
        // state, so it already has the prior run's edits in lines 1-3.
        let mut ws_plan = prior_plan.clone();
        for i in 24..27 {
            ws_plan[i] = format!("plan line {}: run2 edit", i + 1);
        }
        std::fs::write(ws.join("PLAN.md"), ws_plan.join("\n") + "\n").unwrap();
        std::fs::create_dir_all(ws.join(".evidence")).unwrap();
        std::fs::write(ws.join(".evidence/report.md"), "run2 report\n").unwrap();

        let run_workspace_root = temp_dir.path().join("parallel-run2");
        std::fs::create_dir_all(&run_workspace_root).unwrap();
        let run_id = Uuid::now_v7();

        let report = merge_parallel_workspace_results(
            &source_repo,
            run_id,
            &run_workspace_root,
            &[("task-run2".to_string(), ws.clone())],
        )
        .expect("merge should succeed despite dirty source state and context drift");

        assert_eq!(report.merged_task_keys, vec!["task-run2".to_string()]);

        // PLAN.md should have both prior run edits (lines 1-3) and run2 edits (lines 25-27)
        let merged = std::fs::read_to_string(source_repo.join("PLAN.md")).unwrap();
        assert!(
            merged.contains("plan line 1: prior run edit"),
            "should preserve prior run edits: {merged}"
        );
        assert!(
            merged.contains("plan line 25: run2 edit"),
            "should have run2 edits: {merged}"
        );
        assert!(
            !merged.contains("<<<<<<<"),
            "no conflict markers expected: {merged}"
        );

        // .evidence/report.md should be run2's version (overwrites prior)
        let report_content =
            std::fs::read_to_string(source_repo.join(".evidence/report.md")).unwrap();
        assert_eq!(report_content, "run2 report\n");
    }

    /// Integration test 10b: Pre-existing dirty state (staged + working tree changes)
    /// from prior yarli runs that did NOT have the temp commit code. The source has
    /// uncommitted modifications that cause --3way to fail with "does not match index".
    /// Verifies that the pre-merge dirty state commit resolves this.
    #[test]
    fn parallel_workspace_merge_with_preexisting_uncommitted_dirty_state() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        // Create PLAN.md with 30 lines
        let plan_lines: Vec<String> = (1..=30)
            .map(|i| format!("plan line {i}: original"))
            .collect();
        std::fs::write(source_repo.join("PLAN.md"), plan_lines.join("\n") + "\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        // --- Simulate old yarli leaving dirty state (no temp commit) ---
        // Modify lines 1-3 and create .evidence/report.md, stage both but do NOT commit.
        let mut dirty_plan = plan_lines.clone();
        for i in 0..3 {
            dirty_plan[i] = format!("plan line {}: prior dirty edit", i + 1);
        }
        std::fs::write(source_repo.join("PLAN.md"), dirty_plan.join("\n") + "\n").unwrap();
        std::fs::create_dir_all(source_repo.join(".evidence")).unwrap();
        std::fs::write(
            source_repo.join(".evidence/report.md"),
            "prior dirty report\n",
        )
        .unwrap();
        // Stage both files but do NOT commit — this is the key difference from test 10.
        run_git_expect_ok(&source_repo, &["add", "PLAN.md", ".evidence/report.md"]);

        // --- Clone workspace from source HEAD (committed state, not dirty state) ---
        let ws = temp_dir.path().join("ws-new-run");
        let source_str = source_repo.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws.to_str().unwrap()],
        );

        // Workspace modifies lines 25-27 and creates .evidence/report.md
        let mut ws_plan = plan_lines.clone();
        for i in 24..27 {
            ws_plan[i] = format!("plan line {}: new run edit", i + 1);
        }
        std::fs::write(ws.join("PLAN.md"), ws_plan.join("\n") + "\n").unwrap();
        std::fs::create_dir_all(ws.join(".evidence")).unwrap();
        std::fs::write(ws.join(".evidence/report.md"), "new run report\n").unwrap();

        let run_workspace_root = temp_dir.path().join("parallel-run-dirty");
        std::fs::create_dir_all(&run_workspace_root).unwrap();
        let run_id = Uuid::now_v7();

        let report = merge_parallel_workspace_results(
            &source_repo,
            run_id,
            &run_workspace_root,
            &[("task-new-run".to_string(), ws.clone())],
        )
        .expect("merge should succeed despite pre-existing uncommitted dirty state");

        assert_eq!(report.merged_task_keys, vec!["task-new-run".to_string()]);

        // PLAN.md should have both prior dirty edits (1-3) and new run edits (25-27)
        let merged = std::fs::read_to_string(source_repo.join("PLAN.md")).unwrap();
        assert!(
            merged.contains("plan line 1: prior dirty edit"),
            "should preserve prior dirty edits: {merged}"
        );
        assert!(
            merged.contains("plan line 3: prior dirty edit"),
            "should preserve prior dirty edits: {merged}"
        );
        assert!(
            merged.contains("plan line 25: new run edit"),
            "should have new run edits: {merged}"
        );
        assert!(
            merged.contains("plan line 27: new run edit"),
            "should have new run edits: {merged}"
        );
        assert!(
            !merged.contains("<<<<<<<"),
            "no conflict markers expected: {merged}"
        );

        // .evidence/report.md should be new run's version
        let report_content =
            std::fs::read_to_string(source_repo.join(".evidence/report.md")).unwrap();
        assert_eq!(report_content, "new run report\n");
    }

    /// Integration test 10c: Dirty state and workspace both modify the SAME region of a
    /// file (overlapping hunks). Reproduces the production failure where IMPLEMENTATION_PLAN.md
    /// has prior-run appended entries AND the workspace also appends entries in the same spot.
    /// The stash pop should conflict on the overlapping file, and we resolve by keeping the
    /// workspace version while preserving non-conflicting dirty state changes.
    #[test]
    fn parallel_workspace_merge_dirty_state_overlapping_with_workspace() {
        let temp_dir = TempDir::new().unwrap();
        let source_repo = temp_dir.path().join("source");
        std::fs::create_dir_all(&source_repo).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        // Create PLAN.md ending with a "### Next" marker, and a separate config file.
        let plan_content = "\
plan line 1: original
plan line 2: original
plan line 3: original
### Next Priority Actions
1. Do the next thing
";
        std::fs::write(source_repo.join("PLAN.md"), plan_content).unwrap();
        std::fs::write(source_repo.join("config.txt"), "key=value\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        // --- Dirty state: prior run appended entries BEFORE "### Next" AND modified config ---
        let dirty_plan = "\
plan line 1: original
plan line 2: original
plan line 3: original
4. Prior run entry A
5. Prior run entry B
### Next Priority Actions
1. Do the next thing
";
        std::fs::write(source_repo.join("PLAN.md"), dirty_plan).unwrap();
        std::fs::write(source_repo.join("config.txt"), "key=dirty_value\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "PLAN.md", "config.txt"]);
        // Do NOT commit — simulates old yarli

        // --- Clone workspace from HEAD (gets original, not dirty) ---
        let ws = temp_dir.path().join("ws");
        let source_str = source_repo.to_str().unwrap();
        run_git_expect_ok(
            temp_dir.path(),
            &["clone", source_str, ws.to_str().unwrap()],
        );

        // Workspace appends DIFFERENT entries at the same spot
        let ws_plan = "\
plan line 1: original
plan line 2: original
plan line 3: original
10. Workspace entry X
11. Workspace entry Y
### Next Priority Actions
1. Do the next thing
";
        std::fs::write(ws.join("PLAN.md"), ws_plan).unwrap();

        let run_workspace_root = temp_dir.path().join("parallel-run");
        std::fs::create_dir_all(&run_workspace_root).unwrap();
        let run_id = Uuid::now_v7();

        let report = merge_parallel_workspace_results(
            &source_repo,
            run_id,
            &run_workspace_root,
            &[("task-overlap".to_string(), ws.clone())],
        )
        .expect("merge should succeed despite overlapping dirty state");

        assert_eq!(report.merged_task_keys, vec!["task-overlap".to_string()]);

        let merged = std::fs::read_to_string(source_repo.join("PLAN.md")).unwrap();
        // Workspace entries must be present (workspace always wins for conflicts)
        assert!(
            merged.contains("10. Workspace entry X"),
            "workspace entries must be present: {merged}"
        );
        assert!(
            merged.contains("11. Workspace entry Y"),
            "workspace entries must be present: {merged}"
        );
        // No conflict markers
        assert!(
            !merged.contains("<<<<<<<"),
            "no conflict markers expected: {merged}"
        );

        // config.txt should have the dirty value (non-conflicting stash change preserved)
        let config = std::fs::read_to_string(source_repo.join("config.txt")).unwrap();
        assert_eq!(
            config, "key=dirty_value\n",
            "non-conflicting dirty state should be preserved"
        );
    }

    /// End-to-end integration test: full parallel worktree pipeline.
    ///
    /// Exercises the complete flow:
    /// 1. Create a real git repo with tracked files
    /// 2. Use `prepare_parallel_workspace_layout` to clone per-task workspaces
    /// 3. Run actual shell commands in those workspaces that modify files
    ///    (including potentially conflicting edits to the same file)
    /// 4. Merge all workspace results back to the source repo
    /// 5. Verify the merged result handles both clean merges and conflicts
    #[test]
    fn parallel_worktree_end_to_end_create_execute_merge() {
        let temp_dir = TempDir::new().unwrap();
        let repo = temp_dir.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();

        // Initialize a real git repo with tracked files
        run_git_expect_ok(&repo, &["init"]);
        run_git_expect_ok(&repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&repo, &["config", "user.name", "Yarli Test"]);

        // Create files with enough content for non-overlapping hunk merges
        let config_content = (1..=10)
            .map(|i| format!("config_line_{i} = original"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(repo.join("config.txt"), &config_content).unwrap();
        std::fs::write(
            repo.join("data.csv"),
            "id,name,value\n1,alpha,100\n2,beta,200\n",
        )
        .unwrap();
        std::fs::write(repo.join("README.md"), "# Project\nOriginal readme.\n").unwrap();
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(
            repo.join("src/lib.rs"),
            "pub fn hello() { println!(\"hello\"); }\n",
        )
        .unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "initial commit"]);

        // Set up workspace layout via prepare_parallel_workspace_layout
        let config_path = temp_dir.path().join("yarli.toml");
        let worktree_root = temp_dir.path().join("workspaces");
        let config_toml = format!(
            r#"
[features]
parallel = true

[execution]
working_dir = "{}"
worktree_root = "{}"
"#,
            repo.display(),
            worktree_root.display()
        );
        let loaded = write_test_config_at(&config_path, &config_toml);

        // Plan 3 tasks that will make different changes
        let tasks = vec![
            PlannedTask {
                task_key: "edit-config-top".to_string(),
                command: "sed -i 's/config_line_1 = original/config_line_1 = modified_by_task1/' config.txt".to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
                depends_on: Vec::new(),
                allowed_paths: Vec::new(),
            },
            PlannedTask {
                task_key: "edit-config-bottom".to_string(),
                command: "sed -i 's/config_line_10 = original/config_line_10 = modified_by_task2/' config.txt".to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
                depends_on: Vec::new(),
                allowed_paths: Vec::new(),
            },
            PlannedTask {
                task_key: "add-new-module".to_string(),
                command: "echo 'pub fn greet() { println!(\"greet\"); }' > src/greet.rs && echo '3,gamma,300' >> data.csv".to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
                depends_on: Vec::new(),
                allowed_paths: Vec::new(),
            },
        ];

        let plan = RunPlan {
            objective: "e2e parallel workspace test".to_string(),
            tasks: tasks.clone(),
            task_catalog: tasks,
            workdir: repo.display().to_string(),
            timeout_secs: 60,
            pace: None,
            prompt_snapshot: None,
            run_spec: None,
            tranche_plan: Vec::new(),
            current_tranche_index: None,
        };

        let layout = prepare_parallel_workspace_layout(&plan, &loaded)
            .unwrap()
            .expect("should create parallel workspace layout");
        assert_eq!(layout.task_workspace_dirs.len(), 3);

        // Verify each workspace was cloned correctly
        for ws in &layout.task_workspace_dirs {
            assert!(
                ws.join("config.txt").exists(),
                "workspace should have config.txt"
            );
            assert!(
                ws.join("src/lib.rs").exists(),
                "workspace should have src/lib.rs"
            );
            assert!(
                ws.join("data.csv").exists(),
                "workspace should have data.csv"
            );
        }

        // Execute actual commands in each workspace (simulating scheduler execution)
        for (i, planned_task) in plan.tasks.iter().enumerate() {
            let ws = &layout.task_workspace_dirs[i];
            let output = std::process::Command::new("bash")
                .arg("-c")
                .arg(&planned_task.command)
                .current_dir(ws)
                .output()
                .expect("command should run");
            assert!(
                output.status.success(),
                "task {:?} command failed: {}",
                planned_task.task_key,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Verify changes landed in their respective workspaces
        let ws0_config =
            std::fs::read_to_string(layout.task_workspace_dirs[0].join("config.txt")).unwrap();
        assert!(
            ws0_config.contains("config_line_1 = modified_by_task1"),
            "ws0 should have task1 edit"
        );

        let ws1_config =
            std::fs::read_to_string(layout.task_workspace_dirs[1].join("config.txt")).unwrap();
        assert!(
            ws1_config.contains("config_line_10 = modified_by_task2"),
            "ws1 should have task2 edit"
        );

        assert!(
            layout.task_workspace_dirs[2].join("src/greet.rs").exists(),
            "ws2 should have new src/greet.rs"
        );

        // Build task_workspaces for merge
        let task_workspaces: Vec<(String, PathBuf)> = plan
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (t.task_key.clone(), layout.task_workspace_dirs[i].clone()))
            .collect();

        let run_id = Uuid::now_v7();
        let report = merge_parallel_workspace_results(
            &repo,
            run_id,
            &layout.run_workspace_root,
            &task_workspaces,
        )
        .unwrap();

        // All 3 tasks should merge successfully (non-overlapping changes)
        assert_eq!(
            report.merged_task_keys.len(),
            3,
            "all 3 tasks should merge: {:?}",
            report.merged_task_keys
        );
        assert!(report.skipped_task_keys.is_empty());

        // Verify merged result in source repo
        let merged_config = std::fs::read_to_string(repo.join("config.txt")).unwrap();
        assert!(
            merged_config.contains("config_line_1 = modified_by_task1"),
            "source should have task1's edit to line 1: {merged_config}"
        );
        assert!(
            merged_config.contains("config_line_10 = modified_by_task2"),
            "source should have task2's edit to line 10: {merged_config}"
        );
        // Lines 2-9 should be untouched
        assert!(
            merged_config.contains("config_line_5 = original"),
            "middle lines should be untouched: {merged_config}"
        );

        // New file should exist
        assert!(
            repo.join("src/greet.rs").exists(),
            "src/greet.rs should be merged into source"
        );
        let greet = std::fs::read_to_string(repo.join("src/greet.rs")).unwrap();
        assert!(greet.contains("greet"), "greet.rs should have content");

        // data.csv should have the new row
        let data = std::fs::read_to_string(repo.join("data.csv")).unwrap();
        assert!(
            data.contains("3,gamma,300"),
            "data.csv should have the appended row: {data}"
        );

        // Original files that were not changed should be intact
        assert_eq!(
            std::fs::read_to_string(repo.join("README.md")).unwrap(),
            "# Project\nOriginal readme.\n"
        );
        assert_eq!(
            std::fs::read_to_string(repo.join("src/lib.rs")).unwrap(),
            "pub fn hello() { println!(\"hello\"); }\n"
        );

        // No conflict markers in any merged file
        for path in ["config.txt", "data.csv", "src/greet.rs", "src/lib.rs"] {
            let contents = std::fs::read_to_string(repo.join(path)).unwrap();
            assert!(
                !contents.contains("<<<<<<<"),
                "no conflict markers expected in {path}"
            );
        }

        // Patch files should exist for recovery
        let merge_patches = layout.run_workspace_root.join("merge-patches");
        assert!(
            merge_patches.exists(),
            "merge-patches dir should exist for recovery"
        );
    }

    /// End-to-end test: parallel worktrees with conflicting edits to the same line.
    ///
    /// Two workspaces edit the exact same line in the same file.
    /// This should produce a merge conflict error with recovery guidance.
    #[test]
    fn parallel_worktree_end_to_end_conflicting_edits() {
        let temp_dir = TempDir::new().unwrap();
        let repo = temp_dir.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();

        run_git_expect_ok(&repo, &["init"]);
        run_git_expect_ok(&repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(
            repo.join("shared.txt"),
            "line1: original\nline2: original\n",
        )
        .unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "initial"]);

        // Set up workspace layout
        let config_path = temp_dir.path().join("yarli.toml");
        let worktree_root = temp_dir.path().join("workspaces");
        let config_toml = format!(
            r#"
[features]
parallel = true

[execution]
working_dir = "{}"
worktree_root = "{}"
"#,
            repo.display(),
            worktree_root.display()
        );
        let loaded = write_test_config_at(&config_path, &config_toml);

        let tasks = vec![
            PlannedTask {
                task_key: "task-A".to_string(),
                command: "sed -i 's/line1: original/line1: edited by task A/' shared.txt"
                    .to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
                depends_on: Vec::new(),
                allowed_paths: Vec::new(),
            },
            PlannedTask {
                task_key: "task-B".to_string(),
                command: "sed -i 's/line1: original/line1: edited by task B/' shared.txt"
                    .to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
                depends_on: Vec::new(),
                allowed_paths: Vec::new(),
            },
        ];

        let plan = RunPlan {
            objective: "conflicting parallel workspace test".to_string(),
            tasks: tasks.clone(),
            task_catalog: tasks,
            workdir: repo.display().to_string(),
            timeout_secs: 60,
            pace: None,
            prompt_snapshot: None,
            run_spec: None,
            tranche_plan: Vec::new(),
            current_tranche_index: None,
        };

        let layout = prepare_parallel_workspace_layout(&plan, &loaded)
            .unwrap()
            .expect("layout should be created");

        // Execute commands in workspaces
        for (i, t) in plan.tasks.iter().enumerate() {
            let output = std::process::Command::new("bash")
                .arg("-c")
                .arg(&t.command)
                .current_dir(&layout.task_workspace_dirs[i])
                .output()
                .expect("command should run");
            assert!(output.status.success(), "task {} failed", t.task_key);
        }

        // Verify both workspaces have conflicting edits
        let ws0 =
            std::fs::read_to_string(layout.task_workspace_dirs[0].join("shared.txt")).unwrap();
        assert!(ws0.contains("edited by task A"));
        let ws1 =
            std::fs::read_to_string(layout.task_workspace_dirs[1].join("shared.txt")).unwrap();
        assert!(ws1.contains("edited by task B"));

        // Merge should fail with conflict
        let task_workspaces: Vec<(String, PathBuf)> = plan
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (t.task_key.clone(), layout.task_workspace_dirs[i].clone()))
            .collect();

        let run_id = Uuid::now_v7();
        let err = merge_parallel_workspace_results(
            &repo,
            run_id,
            &layout.run_workspace_root,
            &task_workspaces,
        )
        .unwrap_err();

        let err_text = err.to_string();
        // Should mention the failing task
        assert!(
            err_text.contains("task-B"),
            "error should mention conflicting task: {err_text}"
        );
        // Should include recovery guidance
        assert!(
            err_text.contains("Operator recovery steps"),
            "error should include recovery steps: {err_text}"
        );
        // First task's patch should have been applied successfully
        let merged = std::fs::read_to_string(repo.join("shared.txt")).unwrap();
        assert!(
            merged.contains("task A") || merged.contains("<<<<<<<"),
            "source should have task A's edit or conflict markers: {merged}"
        );
        // Recovery note should exist
        let note = layout
            .run_workspace_root
            .join("PARALLEL_MERGE_RECOVERY.txt");
        assert!(
            note.exists(),
            "recovery note should exist at {}",
            note.display()
        );
    }

    /// Helper: set up diverged source + workspace so that applying the workspace
    /// patch with `--3way` will produce a merge conflict. Returns the source repo
    /// path and the patch bytes (the patch is NOT pre-applied).
    fn setup_diverged_repo_for_conflict(temp_dir: &TempDir) -> (PathBuf, String) {
        let source_repo = temp_dir.path().join("source");
        let workspace = temp_dir.path().join("workspace");
        std::fs::create_dir_all(&source_repo).unwrap();

        run_git_expect_ok(&source_repo, &["init"]);
        run_git_expect_ok(&source_repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(&source_repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(&source_repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(source_repo.join("shared.txt"), "base\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "."]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "initial"]);

        run_git_expect_ok(
            temp_dir.path(),
            &[
                "clone",
                source_repo.to_str().unwrap(),
                workspace.to_str().unwrap(),
            ],
        );

        // Diverge source (commit a different change)
        std::fs::write(source_repo.join("shared.txt"), "source update\n").unwrap();
        run_git_expect_ok(&source_repo, &["add", "shared.txt"]);
        run_git_expect_ok(&source_repo, &["commit", "-m", "source update"]);

        // Diverge workspace (unstaged modification)
        std::fs::write(workspace.join("shared.txt"), "workspace update\n").unwrap();
        let (ok, patch, stderr) = run_git(&workspace, &["diff"]);
        assert!(ok, "failed to produce workspace patch: {stderr}");

        (source_repo, patch)
    }

    #[test]
    fn llm_assisted_resolves_conflict() {
        let temp_dir = TempDir::new().unwrap();
        let (source_repo, patch) = setup_diverged_repo_for_conflict(&temp_dir);

        // Repair command that simply writes resolved content
        let resolution_config = MergeResolutionConfig {
            strategy: config::MergeConflictResolution::LlmAssisted,
            repair_command: Some("printf 'resolved content\\n' > shared.txt".to_string()),
            repair_timeout_seconds: 10,
        };
        let repaired = apply_workspace_patch_to_source(
            &source_repo,
            "task-llm",
            patch.as_bytes(),
            &resolution_config,
        )
        .unwrap();
        assert!(repaired);

        let merged = std::fs::read_to_string(source_repo.join("shared.txt")).unwrap();
        assert_eq!(merged, "resolved content\n");
        // No conflict markers should remain
        assert!(!merged.contains("<<<<<<<"));
    }

    #[test]
    fn llm_assisted_command_fails() {
        let temp_dir = TempDir::new().unwrap();
        let (source_repo, patch) = setup_diverged_repo_for_conflict(&temp_dir);

        let resolution_config = MergeResolutionConfig {
            strategy: config::MergeConflictResolution::LlmAssisted,
            repair_command: Some("exit 1".to_string()),
            repair_timeout_seconds: 10,
        };
        let err = apply_workspace_patch_to_source(
            &source_repo,
            "task-llm-fail",
            patch.as_bytes(),
            &resolution_config,
        )
        .unwrap_err();
        assert_eq!(err.kind, ParallelWorkspaceMergeFailureKind::RuntimeFailure);
        assert!(
            err.message.contains("exited with"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn llm_assisted_leaves_conflicts() {
        let temp_dir = TempDir::new().unwrap();
        let (source_repo, patch) = setup_diverged_repo_for_conflict(&temp_dir);

        // Command succeeds (exit 0) but doesn't actually fix the conflicts —
        // the file still has conflict markers, caught by the safety check.
        let resolution_config = MergeResolutionConfig {
            strategy: config::MergeConflictResolution::LlmAssisted,
            repair_command: Some("true".to_string()),
            repair_timeout_seconds: 10,
        };
        let err = apply_workspace_patch_to_source(
            &source_repo,
            "task-llm-noop",
            patch.as_bytes(),
            &resolution_config,
        )
        .unwrap_err();
        assert_eq!(err.kind, ParallelWorkspaceMergeFailureKind::MergeConflict);
        assert!(
            err.message.contains("left conflict markers"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn llm_assisted_timeout() {
        let temp_dir = TempDir::new().unwrap();
        let (source_repo, patch) = setup_diverged_repo_for_conflict(&temp_dir);

        let resolution_config = MergeResolutionConfig {
            strategy: config::MergeConflictResolution::LlmAssisted,
            repair_command: Some("sleep 60".to_string()),
            repair_timeout_seconds: 1,
        };
        let err = apply_workspace_patch_to_source(
            &source_repo,
            "task-llm-timeout",
            patch.as_bytes(),
            &resolution_config,
        )
        .unwrap_err();
        assert_eq!(err.kind, ParallelWorkspaceMergeFailureKind::RuntimeFailure);
        assert!(
            err.message.contains("timed out"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn llm_assisted_context_files_written() {
        let temp_dir = TempDir::new().unwrap();
        let (source_repo, patch) = setup_diverged_repo_for_conflict(&temp_dir);

        // Command that resolves and lets us verify artifacts were written
        let resolution_config = MergeResolutionConfig {
            strategy: config::MergeConflictResolution::LlmAssisted,
            repair_command: Some("printf 'resolved\\n' > shared.txt".to_string()),
            repair_timeout_seconds: 10,
        };
        apply_workspace_patch_to_source(
            &source_repo,
            "task-ctx",
            patch.as_bytes(),
            &resolution_config,
        )
        .unwrap();

        let context_path = source_repo.join(".yarli/merge-conflict-context.json");
        assert!(
            context_path.exists(),
            "context JSON should exist at {}",
            context_path.display()
        );
        let context_json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&context_path).unwrap()).unwrap();
        assert_eq!(context_json["task_key"], "task-ctx");
        assert!(
            !context_json["conflicted_files"]
                .as_array()
                .unwrap()
                .is_empty(),
            "should have at least one conflicted file"
        );
        let first_file = &context_json["conflicted_files"][0];
        assert_eq!(first_file["path"], "shared.txt");
        // Should have conflict_markers content
        assert!(
            first_file["conflict_markers"]
                .as_str()
                .unwrap()
                .contains("<<<<<<<"),
            "conflict_markers should contain markers"
        );

        let prompt_path = source_repo.join(".yarli/merge-conflict-prompt.md");
        assert!(
            prompt_path.exists(),
            "prompt markdown should exist at {}",
            prompt_path.display()
        );
        let prompt = std::fs::read_to_string(&prompt_path).unwrap();
        assert!(prompt.contains("Merge Conflict Resolution"));
        assert!(prompt.contains("shared.txt"));
    }

    #[test]
    fn llm_assisted_conflict_markers_check() {
        let temp_dir = TempDir::new().unwrap();
        let (source_repo, patch) = setup_diverged_repo_for_conflict(&temp_dir);

        // Command that resolves git conflicts (via checkout --theirs) but leaves
        // marker-like content in the file.
        let resolution_config = MergeResolutionConfig {
            strategy: config::MergeConflictResolution::LlmAssisted,
            repair_command: Some(
                r#"git checkout --theirs -- shared.txt && git add -A && printf '<<<<<<< leftover\nresolved\n>>>>>>> leftover\n' > shared.txt"#.to_string(),
            ),
            repair_timeout_seconds: 10,
        };
        let err = apply_workspace_patch_to_source(
            &source_repo,
            "task-markers",
            patch.as_bytes(),
            &resolution_config,
        )
        .unwrap_err();
        assert_eq!(err.kind, ParallelWorkspaceMergeFailureKind::MergeConflict);
        assert!(
            err.message.contains("left conflict markers"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn llm_assisted_env_vars_set() {
        let temp_dir = TempDir::new().unwrap();
        let (source_repo, patch) = setup_diverged_repo_for_conflict(&temp_dir);

        let env_dump_path = temp_dir.path().join("env_dump.txt");
        let cmd = format!(
            r#"env | grep ^YARLI_ > '{}' && printf 'resolved\n' > shared.txt"#,
            env_dump_path.display()
        );
        let resolution_config = MergeResolutionConfig {
            strategy: config::MergeConflictResolution::LlmAssisted,
            repair_command: Some(cmd),
            repair_timeout_seconds: 10,
        };
        apply_workspace_patch_to_source(
            &source_repo,
            "task-env",
            patch.as_bytes(),
            &resolution_config,
        )
        .unwrap();

        let env_dump = std::fs::read_to_string(&env_dump_path).unwrap();
        assert!(
            env_dump.contains("YARLI_CONFLICT_CONTEXT="),
            "should set YARLI_CONFLICT_CONTEXT: {env_dump}"
        );
        assert!(
            env_dump.contains("YARLI_CONFLICT_PROMPT="),
            "should set YARLI_CONFLICT_PROMPT: {env_dump}"
        );
        assert!(
            env_dump.contains("YARLI_CONFLICTED_FILES="),
            "should set YARLI_CONFLICTED_FILES: {env_dump}"
        );
        assert!(
            env_dump.contains("YARLI_TASK_KEY=task-env"),
            "should set YARLI_TASK_KEY: {env_dump}"
        );
    }

    // ── Worktree mode tests ──

    fn init_git_repo(path: &Path) {
        run_git_expect_ok(path, &["init"]);
        run_git_expect_ok(path, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(path, &["config", "user.name", "Test"]);
        // Make tests deterministic regardless of user-global gitignore patterns.
        run_git_expect_ok(path, &["config", "core.excludesFile", ".git/info/exclude"]);
    }

    #[test]
    fn git_worktree_available_true_in_git_repo() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);
        assert!(git_worktree_available(&repo));
    }

    #[test]
    fn git_worktree_available_false_outside_repo() {
        let temp = TempDir::new().unwrap();
        assert!(!git_worktree_available(temp.path()));
    }

    #[test]
    fn count_existing_worktrees_zero_for_fresh_repo() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);
        assert_eq!(count_existing_worktrees(&repo).unwrap(), 0);
    }

    #[test]
    fn count_existing_worktrees_counts_added() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        let wt1 = temp.path().join("wt1");
        run_git_expect_ok(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "b1",
                &wt1.display().to_string(),
                "HEAD",
            ],
        );
        assert_eq!(count_existing_worktrees(&repo).unwrap(), 1);

        let wt2 = temp.path().join("wt2");
        run_git_expect_ok(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "b2",
                &wt2.display().to_string(),
                "HEAD",
            ],
        );
        assert_eq!(count_existing_worktrees(&repo).unwrap(), 2);
    }

    fn make_planned_task(key: &str) -> PlannedTask {
        PlannedTask {
            task_key: key.into(),
            command: format!("echo {key}"),
            command_class: CommandClass::Io,
            tranche_key: None,
            tranche_group: None,
            depends_on: Vec::new(),
            allowed_paths: Vec::new(),
        }
    }

    fn make_run_plan(workdir: &Path, tasks: Vec<PlannedTask>) -> RunPlan {
        RunPlan {
            objective: "test".to_string(),
            tasks,
            task_catalog: Vec::new(),
            workdir: workdir.display().to_string(),
            timeout_secs: 60,
            pace: None,
            prompt_snapshot: None,
            run_spec: None,
            tranche_plan: Vec::new(),
            current_tranche_index: None,
        }
    }

    #[test]
    fn prepare_worktree_layout_creates_worktrees() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("src/main.rs"), "fn main() {}").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        let wt_root = temp.path().join("workspaces");
        let config_path = repo.join("yarli.toml");
        let loaded = write_test_config_at(
            &config_path,
            &format!(
                r#"
[features]
parallel = true
parallel_worktree = true

[execution]
working_dir = "{}"
worktree_root = "{}"
"#,
                repo.display(),
                wt_root.display()
            ),
        );

        let plan = make_run_plan(
            &repo,
            vec![make_planned_task("alpha"), make_planned_task("beta")],
        );

        let layout = prepare_parallel_workspace_layout(&plan, &loaded)
            .unwrap()
            .expect("layout should be Some");

        assert_eq!(layout.mode, ParallelWorkspaceMode::GitWorktree);
        assert_eq!(layout.task_workspace_dirs.len(), 2);
        assert_eq!(layout.worktree_branches.len(), 2);

        for dir in &layout.task_workspace_dirs {
            assert!(
                dir.is_dir(),
                "workspace dir should exist: {}",
                dir.display()
            );
            // In a worktree, .git is a file (not a directory).
            let git_file = dir.join(".git");
            assert!(git_file.exists(), ".git should exist in worktree");
            assert!(
                git_file.is_file(),
                ".git should be a file in worktree, not a dir"
            );
            assert!(
                dir.join("yarli.toml").is_file(),
                "runtime config should be mirrored into worktree"
            );
        }

        for branch in &layout.worktree_branches {
            assert!(
                branch.starts_with("yarli/"),
                "branch should start with yarli/: {branch}"
            );
        }

        // Cleanup.
        cleanup_parallel_workspace(&layout);
        assert!(!layout.run_workspace_root.exists());
    }

    #[test]
    fn prepare_worktree_layout_enforces_cap() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        let wt_root = temp.path().join("workspaces");
        let config_path = repo.join("yarli.toml");
        let loaded = write_test_config_at(
            &config_path,
            &format!(
                r#"
[features]
parallel = true
parallel_worktree = true

[execution]
working_dir = "{}"
worktree_root = "{}"
"#,
                repo.display(),
                wt_root.display()
            ),
        );

        // Create MAX_WORKTREES + 1 tasks — should exceed cap.
        let tasks: Vec<PlannedTask> = (0..MAX_WORKTREES + 1)
            .map(|i| make_planned_task(&format!("task-{i}")))
            .collect();

        let plan = make_run_plan(&repo, tasks);

        let err = prepare_parallel_workspace_layout(&plan, &loaded).unwrap_err();
        assert!(
            err.to_string().contains("maximum"),
            "expected cap error: {err}"
        );
    }

    #[test]
    fn prepare_layout_falls_back_to_copy_when_worktree_disabled() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("src/main.rs"), "fn main() {}").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        let wt_root = temp.path().join("workspaces");
        let config_path = repo.join("yarli.toml");
        let loaded = write_test_config_at(
            &config_path,
            &format!(
                r#"
[features]
parallel = true
parallel_worktree = false

[execution]
working_dir = "{}"
worktree_root = "{}"
"#,
                repo.display(),
                wt_root.display()
            ),
        );

        let plan = make_run_plan(&repo, vec![make_planned_task("alpha")]);

        let layout = prepare_parallel_workspace_layout(&plan, &loaded)
            .unwrap()
            .expect("layout should be Some");

        assert_eq!(layout.mode, ParallelWorkspaceMode::FileCopy);
        assert!(layout.worktree_branches.is_empty());
    }

    #[test]
    fn cleanup_worktree_removes_worktrees_and_branches() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        let wt_root = temp.path().join("workspaces");
        std::fs::create_dir_all(&wt_root).unwrap();
        let run_root = wt_root.join("run-test");
        std::fs::create_dir_all(&run_root).unwrap();

        let wt1 = run_root.join("001-alpha");
        let wt1_str = wt1.display().to_string();
        run_git_expect_ok(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "yarli/test/001-alpha",
                &wt1_str,
                "HEAD",
            ],
        );

        let layout = ParallelWorkspaceLayout {
            run_workspace_root: run_root.clone(),
            task_workspace_dirs: vec![wt1],
            source_head_at_creation: None,
            mode: ParallelWorkspaceMode::GitWorktree,
            worktree_branches: vec!["yarli/test/001-alpha".to_string()],
            source_workdir: Some(repo.clone()),
        };

        cleanup_parallel_workspace(&layout);

        // Only main worktree should remain.
        assert_eq!(count_existing_worktrees(&repo).unwrap(), 0);
        assert!(!run_root.exists());
    }

    #[test]
    fn cleanup_copy_mode_removes_dir_tree() {
        let temp = TempDir::new().unwrap();
        let run_root = temp.path().join("run-test");
        std::fs::create_dir_all(run_root.join("001-task")).unwrap();
        std::fs::write(run_root.join("001-task/file.txt"), "content").unwrap();

        let layout = ParallelWorkspaceLayout {
            run_workspace_root: run_root.clone(),
            task_workspace_dirs: vec![run_root.join("001-task")],
            source_head_at_creation: None,
            mode: ParallelWorkspaceMode::FileCopy,
            worktree_branches: Vec::new(),
            source_workdir: None,
        };

        cleanup_parallel_workspace(&layout);
        assert!(!run_root.exists());
    }

    #[test]
    fn worktree_merge_flow_commits_and_merges_branch() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        let wt_root = temp.path().join("workspaces");
        std::fs::create_dir_all(&wt_root).unwrap();
        let run_root = wt_root.join("run-merge-test");
        std::fs::create_dir_all(&run_root).unwrap();

        let wt1 = run_root.join("001-alpha");
        let wt1_str = wt1.display().to_string();
        run_git_expect_ok(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "yarli/merge/001-alpha",
                &wt1_str,
                "HEAD",
            ],
        );

        // Make changes in the worktree.
        std::fs::write(wt1.join("new_file.txt"), "new content from alpha").unwrap();

        let resolution_config = MergeResolutionConfig {
            strategy: crate::config::MergeConflictResolution::Fail,
            repair_command: None,
            repair_timeout_seconds: 300,
        };
        let task_workspaces = vec![("alpha".to_string(), wt1.clone())];
        let branches = vec!["yarli/merge/001-alpha".to_string()];
        let mut telemetry = Vec::new();

        let report = merge_worktree_workspace_results(
            &repo,
            Uuid::now_v7(),
            &task_workspaces,
            &branches,
            &resolution_config,
            &mut telemetry,
        )
        .unwrap();

        assert_eq!(report.merged_task_keys, vec!["alpha"]);
        assert!(report.skipped_task_keys.is_empty());

        // Verify the file exists in the main repo.
        assert!(repo.join("new_file.txt").exists());
        let content = std::fs::read_to_string(repo.join("new_file.txt")).unwrap();
        assert_eq!(content, "new content from alpha");

        // Check git log shows merge commit.
        let (ok, log_output, _) = run_git(&repo, &["log", "--oneline", "-5"]);
        assert!(ok);
        assert!(
            log_output.contains("feat(new-file): add new file"),
            "git log: {log_output}"
        );
        assert!(
            !log_output.contains("yarli: merge alpha"),
            "git log should avoid workflow-centric subjects: {log_output}"
        );

        // Merged task worktree should be removed immediately to free slots.
        assert_eq!(count_existing_worktrees(&repo).unwrap(), 0);
        assert!(!wt1.exists(), "worktree path should be removed after merge");
    }

    #[test]
    fn worktree_merge_skip_syncs_tranche_state_and_releases_slot() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join(".yarli")).unwrap();
        init_git_repo(&repo);

        // Ignore runtime state directory to emulate real `.yarli` behavior.
        std::fs::write(repo.join(".gitignore"), ".yarli/\n").unwrap();
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        std::fs::write(
            repo.join(".yarli/tranches.toml"),
            "status = \"incomplete\"\n",
        )
        .unwrap();
        run_git_expect_ok(&repo, &["add", ".gitignore", "README.md"]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        let wt_root = temp.path().join("workspaces");
        std::fs::create_dir_all(&wt_root).unwrap();
        let run_root = wt_root.join("run-skip-sync-test");
        std::fs::create_dir_all(&run_root).unwrap();
        let wt1 = run_root.join("001-task");
        let wt1_str = wt1.display().to_string();
        run_git_expect_ok(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "yarli/skip/001-task",
                &wt1_str,
                "HEAD",
            ],
        );

        // Only update ignored runtime state (no tracked code changes).
        std::fs::create_dir_all(wt1.join(".yarli")).unwrap();
        std::fs::write(wt1.join(".yarli/tranches.toml"), "status = \"complete\"\n").unwrap();

        let resolution_config = MergeResolutionConfig {
            strategy: crate::config::MergeConflictResolution::Fail,
            repair_command: None,
            repair_timeout_seconds: 300,
        };
        let task_workspaces = vec![("task".to_string(), wt1.clone())];
        let branches = vec!["yarli/skip/001-task".to_string()];
        let mut telemetry = Vec::new();

        let report = merge_worktree_workspace_results(
            &repo,
            Uuid::now_v7(),
            &task_workspaces,
            &branches,
            &resolution_config,
            &mut telemetry,
        )
        .unwrap();

        assert!(report.merged_task_keys.is_empty());
        assert_eq!(report.skipped_task_keys, vec!["task"]);

        // Ignored runtime state should still sync back to source workspace.
        let synced = std::fs::read_to_string(repo.join(".yarli/tranches.toml")).unwrap();
        assert_eq!(synced, "status = \"complete\"\n");

        // Skipped task worktree should also be removed immediately.
        assert_eq!(count_existing_worktrees(&repo).unwrap(), 0);
        assert!(!wt1.exists(), "worktree path should be removed after skip");
    }

    #[test]
    fn auto_commit_stages_state_files_only() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join(".yarli")).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        // Create state files and an unrelated file.
        std::fs::write(repo.join(".yarli/continuation.json"), "{}").unwrap();
        std::fs::write(repo.join(".yarli/tranches.toml"), "").unwrap();
        std::fs::write(repo.join("unrelated.txt"), "should not be committed").unwrap();

        let committed =
            auto_commit_state_files(&repo, "tranche-001", "run-123", 1, 3, None).unwrap();
        assert!(committed);

        // Verify only state files were committed.
        let (ok, diff_output, _) = run_git(&repo, &["diff", "--name-only", "HEAD~1..HEAD"]);
        assert!(ok, "git diff should succeed");
        assert!(diff_output.contains(".yarli/continuation.json"));
        assert!(diff_output.contains(".yarli/tranches.toml"));
        assert!(
            !diff_output.contains("unrelated.txt"),
            "unrelated.txt should not be committed: {diff_output}"
        );

        // Verify commit message.
        let (ok, log, _) = run_git(&repo, &["log", "-1", "--pretty=%B"]);
        assert!(ok);
        assert!(
            log.starts_with("chore(state): checkpoint runtime state"),
            "commit message: {log}"
        );
        assert!(
            log.contains("yarli-tranche: tranche-001"),
            "commit message: {log}"
        );
        assert!(log.contains("yarli-progress: 1/3"), "commit message: {log}");
    }

    #[test]
    fn worktree_merge_stashes_dirty_state_before_merge() {
        // Reproduces the real failure: dirty `.yarli/tranches.toml` in the main
        // worktree blocks `git merge --no-ff` because git refuses to overwrite
        // local modifications.
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join(".yarli")).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        std::fs::write(repo.join(".yarli/tranches.toml"), "initial").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        // Create worktree.
        let wt_root = temp.path().join("workspaces");
        std::fs::create_dir_all(&wt_root).unwrap();
        let run_root = wt_root.join("run-stash-test");
        std::fs::create_dir_all(&run_root).unwrap();
        let wt1 = run_root.join("001-task");
        let wt1_str = wt1.display().to_string();
        run_git_expect_ok(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "yarli/stash/001-task",
                &wt1_str,
                "HEAD",
            ],
        );

        // Make changes in the worktree that touch the same file.
        std::fs::write(wt1.join(".yarli/tranches.toml"), "modified by task").unwrap();
        std::fs::write(wt1.join("new_file.txt"), "task output").unwrap();

        // NOW dirty the main worktree — this is the scenario that caused the failure.
        std::fs::write(repo.join(".yarli/tranches.toml"), "dirty local state").unwrap();

        let resolution_config = MergeResolutionConfig {
            strategy: crate::config::MergeConflictResolution::Fail,
            repair_command: None,
            repair_timeout_seconds: 300,
        };
        let task_workspaces = vec![("task".to_string(), wt1.clone())];
        let branches = vec!["yarli/stash/001-task".to_string()];
        let mut telemetry = Vec::new();

        // This should succeed (stash dirty state, merge, pop stash).
        let report = merge_worktree_workspace_results(
            &repo,
            Uuid::now_v7(),
            &task_workspaces,
            &branches,
            &resolution_config,
            &mut telemetry,
        )
        .unwrap();

        assert_eq!(report.merged_task_keys, vec!["task"]);

        // Verify the task output was merged.
        assert!(repo.join("new_file.txt").exists());
        let content = std::fs::read_to_string(repo.join("new_file.txt")).unwrap();
        assert_eq!(content, "task output");

        // Verify git log shows merge + reapply commits.
        let (ok, log, _) = run_git(&repo, &["log", "--oneline", "-5"]);
        assert!(ok);
        assert!(log.contains("merge task"), "git log: {log}");
        assert!(
            log.contains("reapply pre-existing"),
            "should reapply stashed state: {log}"
        );

        // Verify no stash entries remain.
        let (ok, stash_list, _) = run_git(&repo, &["stash", "list"]);
        assert!(ok);
        assert!(
            stash_list.trim().is_empty(),
            "stash should be empty: {stash_list}"
        );
    }

    #[test]
    fn worktree_merge_stash_popped_even_on_merge_failure() {
        // Ensures the stash is restored even when the merge fails, so the
        // working tree isn't left in a stashed state.
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_git_repo(&repo);
        std::fs::write(repo.join("README.md"), "hello").unwrap();
        std::fs::write(repo.join("shared.txt"), "original").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "init"]);

        // Create worktree.
        let wt_root = temp.path().join("workspaces");
        std::fs::create_dir_all(&wt_root).unwrap();
        let run_root = wt_root.join("run-fail-test");
        std::fs::create_dir_all(&run_root).unwrap();
        let wt1 = run_root.join("001-conflict");
        let wt1_str = wt1.display().to_string();
        run_git_expect_ok(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "yarli/fail/001-conflict",
                &wt1_str,
                "HEAD",
            ],
        );

        // Commit conflicting change on main branch first, so the worktree branch
        // diverges and `git merge --no-ff` will produce a real conflict.
        std::fs::write(repo.join("shared.txt"), "changed on main").unwrap();
        run_git_expect_ok(&repo, &["add", "."]);
        run_git_expect_ok(&repo, &["commit", "-m", "main diverges"]);

        // Make conflicting change in worktree.
        std::fs::write(wt1.join("shared.txt"), "changed in worktree").unwrap();

        // Also dirty the main worktree so the stash codepath activates.
        std::fs::write(repo.join("dirty_local.txt"), "dirty").unwrap();

        let resolution_config = MergeResolutionConfig {
            strategy: crate::config::MergeConflictResolution::Fail,
            repair_command: None,
            repair_timeout_seconds: 300,
        };
        let task_workspaces = vec![("conflict".to_string(), wt1.clone())];
        let branches = vec!["yarli/fail/001-conflict".to_string()];
        let mut telemetry = Vec::new();

        // This should fail due to merge conflict with Fail strategy.
        let result = merge_worktree_workspace_results(
            &repo,
            Uuid::now_v7(),
            &task_workspaces,
            &branches,
            &resolution_config,
            &mut telemetry,
        );
        assert!(result.is_err(), "should fail on merge conflict");

        // Verify no stash entries remain (stash was popped despite error).
        let (ok, stash_list, _) = run_git(&repo, &["stash", "list"]);
        assert!(ok);
        assert!(
            stash_list.trim().is_empty(),
            "stash should be empty after error: {stash_list}"
        );

        // Verify the dirty file is back in the working tree.
        assert!(
            repo.join("dirty_local.txt").exists(),
            "dirty local file should be restored from stash"
        );
    }
}
