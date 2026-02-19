use std::collections::HashSet;
use std::fs;
use std::io::Write as IoWrite;
use std::path::{Component, Path, PathBuf};
use std::process::{self, Stdio};

use anyhow::{bail, Context, Result};
use serde::Serialize;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config;
use crate::config::LoadedConfig;
use crate::plan::sanitize_task_key_component;
use crate::plan::RunPlan;

#[derive(Debug, Clone)]
pub(crate) struct ParallelWorkspaceLayout {
    pub(crate) run_workspace_root: PathBuf,
    pub(crate) task_workspace_dirs: Vec<PathBuf>,
    /// Source HEAD captured at workspace creation time. Used for merge
    /// ancestry validation so we compare against a commit that is guaranteed
    /// to exist in the copied workspace's object database.
    pub(crate) source_head_at_creation: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ParallelWorkspaceMergeReport {
    pub(crate) merged_task_keys: Vec<String>,
    pub(crate) skipped_task_keys: Vec<String>,
    pub(crate) task_outcomes: Vec<ParallelTaskMergeOutcome>,
    pub(crate) preserve_workspace_root: bool,
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
    }))
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

pub(crate) fn apply_workspace_patch_to_source(
    source_workdir: &Path,
    task_key: &str,
    patch: &[u8],
    merge_conflict_resolution: config::MergeConflictResolution,
) -> Result<()> {
    let backed_up_new_files = remove_conflicting_new_files_for_patch(source_workdir, patch)?;

    let apply_args = ["apply", "--3way", "--whitespace=nowarn", "-"];
    let apply_output = run_git_capture_with_input(source_workdir, &apply_args, patch)?;
    if apply_output.status.success() {
        warn_on_new_file_content_divergence(source_workdir, &backed_up_new_files);
        return Ok(());
    }

    // --3way failed. Check if it's an index-related issue where plain apply might
    // work (e.g. permission-only changes, shallow repos without base blobs).
    let apply_stderr = String::from_utf8_lossy(&apply_output.stderr);
    let index_related = apply_stderr.contains("does not exist in index")
        || apply_stderr.contains("does not match index");
    if index_related {
        let direct_apply_args = ["apply", "--whitespace=nowarn", "-"];
        let direct_apply_output =
            run_git_capture_with_input(source_workdir, &direct_apply_args, patch)?;
        ensure_git_success(
            direct_apply_output,
            source_workdir,
            &direct_apply_args,
            &format!(
                "parallel workspace merge apply failed for task {task_key} after index-mismatch fallback"
            ),
        )?;
        warn_on_new_file_content_divergence(source_workdir, &backed_up_new_files);
        return Ok(());
    }

    // Not index-related — apply the merge conflict resolution strategy.
    match merge_conflict_resolution {
        config::MergeConflictResolution::Agent => {
            // TODO: spawn an agent to resolve conflicts. For now, fall through to Fail.
            warn!(task_key, "merge_conflict_resolution=agent is not yet implemented; falling back to fail");
        }
        config::MergeConflictResolution::Manual => {
            bail!(
                "merge conflict in task {task_key} (resolution=manual): workspace preserved for operator intervention.\n\
                 Resolve conflicts manually, then re-run:\n\
                   git -C \"{}\" status --short",
                source_workdir.display()
            );
        }
        config::MergeConflictResolution::Fail => {}
    }

    // Propagate the original --3way error.
    ensure_git_success(
        apply_output,
        source_workdir,
        &apply_args,
        &format!("parallel workspace merge apply failed for task {task_key}"),
    )?;
    warn_on_new_file_content_divergence(source_workdir, &backed_up_new_files);
    Ok(())
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
    merge_parallel_workspace_results_with_resolution(
        source_workdir,
        run_id,
        run_workspace_root,
        task_workspaces,
        config::MergeConflictResolution::Fail,
        None,
    )
}

pub(crate) fn merge_parallel_workspace_results_with_resolution(
    source_workdir: &Path,
    run_id: Uuid,
    run_workspace_root: &Path,
    task_workspaces: &[(String, PathBuf)],
    merge_conflict_resolution: config::MergeConflictResolution,
    source_head_at_creation: Option<&str>,
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
            let Some(original_head) = original_workspace_head_for_restore.as_deref() else {
                return None;
            };
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
        if let Err(err) =
            apply_workspace_patch_to_source(source_workdir, task_key, patch.as_bytes(), merge_conflict_resolution)
        {
            let restore_note =
                restore_workspace_head_on_error("workspace patch apply failed").unwrap_or_default();
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
            return Err(err.context(guidance));
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
        let commit_msg = format!("yarli: merge workspace result for {task_key}");
        let _ = run_git_capture(
            source_workdir,
            &["commit", "--no-verify", "--allow-empty", "-m", &commit_msg],
        );
    }

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
        let _ = run_git_capture(
            source_workdir,
            &[
                "commit",
                "--no-verify",
                "--allow-empty",
                "-m",
                "yarli: reapply pre-existing workspace state after merge",
            ],
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
                allowed_paths: Vec::new(),
            },
            PlannedTask {
                task_key: "edit-config-bottom".to_string(),
                command: "sed -i 's/config_line_10 = original/config_line_10 = modified_by_task2/' config.txt".to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
                allowed_paths: Vec::new(),
            },
            PlannedTask {
                task_key: "add-new-module".to_string(),
                command: "echo 'pub fn greet() { println!(\"greet\"); }' > src/greet.rs && echo '3,gamma,300' >> data.csv".to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
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
                allowed_paths: Vec::new(),
            },
            PlannedTask {
                task_key: "task-B".to_string(),
                command: "sed -i 's/line1: original/line1: edited by task B/' shared.txt"
                    .to_string(),
                command_class: CommandClass::Io,
                tranche_key: None,
                tranche_group: None,
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
}
