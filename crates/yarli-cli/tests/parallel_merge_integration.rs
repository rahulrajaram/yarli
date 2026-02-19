use std::path::Path;
use std::process::{Command, Output};

use tempfile::TempDir;

fn run_git(cwd: &Path, args: &[&str]) -> Output {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run git {:?} in {}: {err}", args, cwd.display()));
    assert!(
        output.status.success(),
        "git {:?} failed in {}: stdout:\n{}\nstderr:\n{}",
        args,
        cwd.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

fn parse_run_id(output: &str) -> Option<String> {
    output.lines().find_map(|line| {
        line.strip_prefix("Run ")
            .and_then(|rest| rest.split_whitespace().next())
            .map(str::to_string)
    })
}

fn run_output_path() -> std::path::PathBuf {
    match std::env::var_os("CARGO_BIN_EXE_yarli") {
        Some(path) => path.into(),
        None => {
            let current_exe = std::env::current_exe().expect("failed to resolve test binary path");
            let debug_dir = current_exe
                .parent()
                .and_then(|path| path.parent())
                .expect("failed to derive target/debug directory");
            let binary_name = format!("yarli{}", std::env::consts::EXE_SUFFIX);
            let fallback = debug_dir.join(binary_name);
            if fallback.is_file() {
                fallback
            } else {
                panic!(
                    "CARGO_BIN_EXE_yarli is not set and target/debug/yarli fallback was not found"
                );
            }
        }
    }
}

fn list_run_workspace_roots(worktree_root: &Path) -> Vec<std::path::PathBuf> {
    let mut roots = Vec::new();
    if let Ok(entries) = std::fs::read_dir(worktree_root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.starts_with("run-") {
                roots.push(path);
            }
        }
    }
    roots.sort();
    roots
}

#[test]
fn run_start_parallel_merge_ignores_untracked_rust_target_artifact() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    std::fs::create_dir_all(repo_dir.join(".yarl/workspaces")).expect("create worktree root dir");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["checkout", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.email", "test@yarli.dev"]);
    run_git(&repo_dir, &["config", "user.name", "Yarli Test"]);
    std::fs::write(repo_dir.join("alpha.txt"), "alpha baseline\n").expect("write baseline alpha");
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "initial"]);

    let config = format!(
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true

[features]
parallel = true

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        repo_dir.join(".yarl/workspaces").display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let task_cmd = "mkdir -p tests/fixtures/rust-sample/target && printf 'generated artifact\\n' > tests/fixtures/rust-sample/target/.rustc_info.json && printf 'alpha merged\\n' > alpha.txt";
    let run_output = Command::new(&binary)
        .current_dir(&repo_dir)
        .args([
            "run",
            "start",
            "parallel merge artifact regression",
            "--stream",
            "--cmd",
            task_cmd,
        ])
        .output()
        .expect("run start command invocation failed");

    assert!(
        run_output.status.success(),
        "run start command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_stdout = String::from_utf8_lossy(&run_output.stdout);
    let run_id = parse_run_id(&run_stdout).expect("failed to parse run id from command output");
    assert!(
        run_stdout.contains("RunCompleted") || run_stdout.contains("State: RunCompleted"),
        "run {run_id} should complete in output, saw:\n{run_stdout}"
    );

    let merged_alpha =
        std::fs::read_to_string(repo_dir.join("alpha.txt")).expect("read merged alpha.txt");
    assert_eq!(merged_alpha, "alpha merged\n");

    let artifact = repo_dir.join("tests/fixtures/rust-sample/target/.rustc_info.json");
    assert!(
        !artifact.exists(),
        "untracked build artifact should not be merged: {}",
        artifact.display()
    );
}

#[test]
fn run_start_parallel_merge_applies_committed_workspace_changes() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    std::fs::create_dir_all(repo_dir.join(".yarl/workspaces")).expect("create worktree root dir");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["checkout", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.email", "test@yarli.dev"]);
    run_git(&repo_dir, &["config", "user.name", "Yarli Test"]);
    std::fs::write(repo_dir.join("alpha.txt"), "alpha baseline\n").expect("write alpha baseline");
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "initial"]);

    let config = format!(
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true

[features]
parallel = true

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        repo_dir.join(".yarl/workspaces").display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let task_cmd = "git config user.email test@yarli.dev && git config user.name 'Yarli Test' && printf 'alpha committed\\n' > alpha.txt && printf 'new committed file\\n' > committed.txt && git add -A && git commit -m 'worker commit'";
    let run_output = Command::new(&binary)
        .current_dir(&repo_dir)
        .args([
            "run",
            "start",
            "parallel merge committed workspace regression",
            "--stream",
            "--cmd",
            task_cmd,
        ])
        .output()
        .expect("run start command invocation failed");

    assert!(
        run_output.status.success(),
        "run start command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_stdout = String::from_utf8_lossy(&run_output.stdout);
    let run_id = parse_run_id(&run_stdout).expect("failed to parse run id from command output");
    assert!(
        run_stdout.contains("RunCompleted") || run_stdout.contains("State: RunCompleted"),
        "run {run_id} should complete in output, saw:\n{run_stdout}"
    );

    let merged_alpha =
        std::fs::read_to_string(repo_dir.join("alpha.txt")).expect("read merged alpha.txt");
    assert_eq!(merged_alpha, "alpha committed\n");
    let merged_committed =
        std::fs::read_to_string(repo_dir.join("committed.txt")).expect("read merged committed.txt");
    assert_eq!(merged_committed, "new committed file\n");
}

#[test]
fn run_start_parallel_merge_preserves_workspace_root_when_task_is_skipped() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    let worktree_root = repo_dir.join(".yarl/workspaces");
    std::fs::create_dir_all(&worktree_root).expect("create worktree root dir");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["checkout", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.email", "test@yarli.dev"]);
    run_git(&repo_dir, &["config", "user.name", "Yarli Test"]);
    std::fs::write(repo_dir.join("alpha.txt"), "alpha baseline\n").expect("write alpha baseline");
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "initial"]);

    let config = format!(
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true

[features]
parallel = true

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let run_output = Command::new(&binary)
        .current_dir(&repo_dir)
        .args([
            "run",
            "start",
            "parallel merge preserve skipped workspace regression",
            "--stream",
            "--cmd",
            "git reset --hard HEAD >/dev/null && git clean -fdx >/dev/null",
            "--cmd",
            "printf 'beta merged\\n' > beta.txt",
        ])
        .output()
        .expect("run start command invocation failed");

    assert!(
        run_output.status.success(),
        "run start command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        run_stdout.contains("retained task workspaces for operator review"),
        "expected preserved-workspace note in output:\n{run_stdout}"
    );

    let run_roots = list_run_workspace_roots(&worktree_root);
    assert_eq!(
        run_roots.len(),
        1,
        "expected run workspace root to be preserved when at least one task is skipped"
    );
    let run_root = &run_roots[0];
    assert!(
        run_root.join("001-task-1").exists(),
        "skipped task workspace should be preserved"
    );
    assert!(
        run_root.join("002-task-2").exists(),
        "merged task workspace should be preserved for operator review"
    );
    assert_eq!(
        std::fs::read_to_string(repo_dir.join("beta.txt")).unwrap(),
        "beta merged\n"
    );
}

#[test]
fn run_start_parallel_merge_cleans_workspace_root_when_all_tasks_merge() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    let worktree_root = repo_dir.join(".yarl/workspaces");
    std::fs::create_dir_all(&worktree_root).expect("create worktree root dir");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["checkout", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.email", "test@yarli.dev"]);
    run_git(&repo_dir, &["config", "user.name", "Yarli Test"]);
    std::fs::write(repo_dir.join("shared.txt"), "base\n").expect("write shared baseline");
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "initial"]);

    let config = format!(
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true

[features]
parallel = true

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let run_output = Command::new(&binary)
        .current_dir(&repo_dir)
        .args([
            "run",
            "start",
            "parallel merge cleanup on full merge regression",
            "--stream",
            "--cmd",
            "printf 'one\\n' > task_one.txt",
            "--cmd",
            "printf 'two\\n' > task_two.txt",
        ])
        .output()
        .expect("run start command invocation failed");

    assert!(
        run_output.status.success(),
        "run start command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_roots = list_run_workspace_roots(&worktree_root);
    assert_eq!(
        run_roots.len(),
        0,
        "workspace root should be cleaned when all task workspaces merge cleanly"
    );
    assert_eq!(
        std::fs::read_to_string(repo_dir.join("task_one.txt")).unwrap(),
        "one\n"
    );
    assert_eq!(
        std::fs::read_to_string(repo_dir.join("task_two.txt")).unwrap(),
        "two\n"
    );
}

#[test]
fn run_start_parallel_merge_conflict_preserves_unmerged_artifacts_and_escalates() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    let worktree_root = repo_dir.join(".yarl/workspaces");
    std::fs::create_dir_all(&worktree_root).expect("create worktree root dir");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["checkout", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.email", "test@yarli.dev"]);
    run_git(&repo_dir, &["config", "user.name", "Yarli Test"]);
    std::fs::write(repo_dir.join("shared.txt"), "base\n").expect("write shared baseline");
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "initial"]);

    let config = format!(
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true

[features]
parallel = true

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let task_one = "git config user.email test@yarli.dev && git config user.name 'Yarli Test' && printf 'task one\\n' > shared.txt && git add shared.txt && git commit -m 'task one commit'";
    let task_two = "git config user.email test@yarli.dev && git config user.name 'Yarli Test' && printf 'task two\\n' > shared.txt && git add shared.txt && git commit -m 'task two commit'";
    let run_output = Command::new(&binary)
        .current_dir(&repo_dir)
        .args([
            "run",
            "start",
            "parallel merge conflict retention regression",
            "--stream",
            "--cmd",
            task_one,
            "--cmd",
            task_two,
        ])
        .output()
        .expect("run start command invocation failed");

    assert!(
        !run_output.status.success(),
        "run should fail due to merge conflict\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_roots = list_run_workspace_roots(&worktree_root);
    assert_eq!(
        run_roots.len(),
        1,
        "expected failed run workspace root to be preserved"
    );
    let run_root = &run_roots[0];
    assert!(
        run_root.join("001-task-1").exists(),
        "task-1 workspace should be preserved"
    );
    assert!(
        run_root.join("002-task-2").exists(),
        "task-2 workspace should be preserved"
    );
    assert_eq!(
        std::fs::read_to_string(run_root.join("001-task-1/shared.txt")).unwrap(),
        "task one\n"
    );
    assert_eq!(
        std::fs::read_to_string(run_root.join("002-task-2/shared.txt")).unwrap(),
        "task two\n"
    );

    let recovery_note = run_root.join("PARALLEL_MERGE_RECOVERY.txt");
    assert!(
        recovery_note.exists(),
        "expected recovery note at {}",
        recovery_note.display()
    );
    let recovery_text = std::fs::read_to_string(&recovery_note).unwrap();
    assert!(
        recovery_text.contains("Operator recovery steps"),
        "expected human escalation guidance in recovery note: {recovery_text}"
    );

    let merge_patches = run_root.join("merge-patches");
    assert!(merge_patches.is_dir(), "expected merge patch directory");
    let patch_count = std::fs::read_dir(&merge_patches)
        .unwrap()
        .flatten()
        .filter(|entry| entry.path().extension().and_then(|e| e.to_str()) == Some("patch"))
        .count();
    assert!(
        patch_count >= 2,
        "expected preserved patch artifacts for both tasks, got {patch_count}"
    );

    let source_shared =
        std::fs::read_to_string(repo_dir.join("shared.txt")).expect("read source shared.txt");
    assert!(
        source_shared.contains("<<<<<<<"),
        "expected conflict markers in source file after failed merge: {source_shared}"
    );
}

#[test]
fn run_start_parallel_merge_lineage_failure_preserves_workspace_and_escalates() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    let worktree_root = repo_dir.join(".yarl/workspaces");
    std::fs::create_dir_all(&worktree_root).expect("create worktree root dir");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["checkout", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.email", "test@yarli.dev"]);
    run_git(&repo_dir, &["config", "user.name", "Yarli Test"]);
    std::fs::write(repo_dir.join("shared.txt"), "base\n").expect("write shared baseline");
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "initial"]);

    let config = format!(
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true

[features]
parallel = true

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let lineage_break_cmd = "git checkout --orphan rogue && git rm -rf . >/dev/null 2>&1 || true && git config user.email test@yarli.dev && git config user.name 'Yarli Test' && printf 'rogue\\n' > rogue.txt && git add -A && git commit -m 'rogue commit'";
    let run_output = Command::new(&binary)
        .current_dir(&repo_dir)
        .args([
            "run",
            "start",
            "parallel merge lineage safety regression",
            "--stream",
            "--cmd",
            lineage_break_cmd,
        ])
        .output()
        .expect("run start command invocation failed");

    assert!(
        !run_output.status.success(),
        "run should fail on non-descendant workspace lineage\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_roots = list_run_workspace_roots(&worktree_root);
    assert_eq!(
        run_roots.len(),
        1,
        "expected failed run workspace root to be preserved"
    );
    let run_root = &run_roots[0];
    assert!(
        run_root.join("001-task-1/rogue.txt").exists(),
        "lineage-breaking workspace output should be preserved for manual recovery"
    );

    let recovery_note = run_root.join("PARALLEL_MERGE_RECOVERY.txt");
    assert!(
        recovery_note.exists(),
        "expected recovery note at {}",
        recovery_note.display()
    );
    let recovery_text = std::fs::read_to_string(&recovery_note).unwrap();
    assert!(
        recovery_text.contains("Operator recovery steps"),
        "expected human escalation guidance in recovery note: {recovery_text}"
    );

    let source_shared =
        std::fs::read_to_string(repo_dir.join("shared.txt")).expect("read source shared.txt");
    assert_eq!(
        source_shared, "base\n",
        "source should remain unchanged after lineage safety rejection"
    );
}
