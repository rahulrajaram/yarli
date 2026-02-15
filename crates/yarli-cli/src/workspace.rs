use std::collections::HashSet;
use std::fs;
use std::io::Write as IoWrite;
use std::path::{Component, Path, PathBuf};
use std::process::{self, Stdio};

use anyhow::{bail, Context, Result};
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
}

#[derive(Debug, Clone)]
pub(crate) struct ParallelWorkspaceMergeReport {
    pub(crate) merged_task_keys: Vec<String>,
    pub(crate) skipped_task_keys: Vec<String>,
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

    // Not index-related — propagate the original --3way error.
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

const PARALLEL_MERGE_IGNORE_DIR_NAMES: [&str; 4] = ["target", "node_modules", "__pycache__", ".build"];

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
        .any(|name| PARALLEL_MERGE_IGNORE_DIR_NAMES.iter().any(|ignore| ignore == &name.as_ref()))
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
) -> Result<PathBuf> {
    let note_path = run_workspace_root.join("PARALLEL_MERGE_RECOVERY.txt");
    let note = format!(
        "Parallel workspace merge failed for run {run_id} task {task_key}.\n\n\
Source workspace: {}\n\
Task workspace: {}\n\
Patch artifact: {}\n\n\
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

pub(crate) fn merge_parallel_workspace_results(
    source_workdir: &Path,
    run_id: Uuid,
    run_workspace_root: &Path,
    task_workspaces: &[(String, PathBuf)],
) -> Result<ParallelWorkspaceMergeReport> {
    ensure_git_repository(source_workdir)?;

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
    for (task_index, (task_key, workspace_dir)) in task_workspaces.iter().enumerate() {
        ensure_git_repository(workspace_dir)?;
        let scoped_paths = scoped_workspace_changed_paths(source_workdir, workspace_dir)?;
        if scoped_paths.is_empty() {
            skipped_task_keys.push(task_key.clone());
            continue;
        }

        stage_workspace_paths(workspace_dir, &scoped_paths)?;
        let patch = export_staged_workspace_patch(workspace_dir)?;
        if patch.trim().is_empty() {
            skipped_task_keys.push(task_key.clone());
            continue;
        }

        let patch_path =
            persist_workspace_patch_for_recovery(run_workspace_root, task_key, task_index, &patch)?;
        if let Err(err) =
            apply_workspace_patch_to_source(source_workdir, task_key, patch.as_bytes())
        {
            let note_path = write_parallel_merge_recovery_note(
                run_id,
                source_workdir,
                run_workspace_root,
                workspace_dir,
                task_key,
                &patch_path,
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
            guidance.push_str(&format!(
                "\nWorkspace root preserved for inspection: {}",
                run_workspace_root.display()
            ));
            return Err(err.context(guidance));
        }
        merged_task_keys.push(task_key.clone());

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
        merged_task_keys,
        skipped_task_keys,
    })
}
