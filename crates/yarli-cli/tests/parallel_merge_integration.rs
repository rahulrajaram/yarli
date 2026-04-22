use std::io::Read as _;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;

/// Default timeout for integration tests (60 seconds).
const YARLI_TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Run the yarli binary with a timeout to prevent indefinite hangs.
/// Panics with a descriptive message if the process does not exit in time.
fn run_yarli(binary: &Path, cwd: &Path, args: &[&str]) -> Output {
    let mut child = Command::new(binary)
        .current_dir(cwd)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn yarli binary");

    // Take pipes before polling so we can read them in threads.
    let mut stdout_pipe = child.stdout.take().unwrap();
    let mut stderr_pipe = child.stderr.take().unwrap();
    let stdout_thread = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout_pipe.read_to_end(&mut buf);
        buf
    });
    let stderr_thread = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr_pipe.read_to_end(&mut buf);
        buf
    });

    let start = Instant::now();
    let status = loop {
        match child.try_wait().expect("failed to check child status") {
            Some(status) => break status,
            None if start.elapsed() >= YARLI_TEST_TIMEOUT => {
                let _ = child.kill();
                let _ = child.wait();
                let secs = YARLI_TEST_TIMEOUT.as_secs();
                panic!("yarli binary timed out after {secs}s — possible channel deadlock");
            }
            None => std::thread::sleep(Duration::from_millis(200)),
        }
    };

    let stdout = stdout_thread.join().unwrap_or_default();
    let stderr = stderr_thread.join().unwrap_or_default();
    Output {
        status,
        stdout,
        stderr,
    }
}

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

fn git_stdout(cwd: &Path, args: &[&str]) -> String {
    String::from_utf8(run_git(cwd, args).stdout).expect("git output should be utf-8")
}

fn is_change_centric_subject(subject: &str) -> bool {
    [
        "feat(",
        "fix(",
        "refactor(",
        "docs(",
        "test(",
        "build(",
        "ci(",
        "chore(",
    ]
    .iter()
    .any(|prefix| subject.starts_with(prefix))
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

fn read_continuation_payload(repo_dir: &Path) -> Value {
    let continuation_path = repo_dir.join(".yarli").join("continuation.json");
    let contents = std::fs::read_to_string(&continuation_path).unwrap_or_else(|err| {
        panic!(
            "failed to read continuation payload at {}: {err}",
            continuation_path.display()
        )
    });
    serde_json::from_str(&contents).unwrap_or_else(|err| {
        panic!("failed to parse continuation payload JSON: {err}\n{contents}")
    })
}

fn assert_output_state_matches_continuation(run_output: &str, continuation: &Value) {
    let Some(exit_state) = continuation.get("exit_state").and_then(Value::as_str) else {
        return;
    };
    let state_matches = match exit_state {
        "RunOpen" => run_output.contains("RunOpen") || run_output.contains("RUN_OPEN"),
        "RunActive" => run_output.contains("RunActive") || run_output.contains("RUN_ACTIVE"),
        "RunCompleted" => {
            run_output.contains("RunCompleted") || run_output.contains("RUN_COMPLETED")
        }
        "RunFailed" => run_output.contains("RunFailed") || run_output.contains("RUN_FAILED"),
        "RunCancelled" => {
            run_output.contains("RunCancelled") || run_output.contains("RUN_CANCELLED")
        }
        "RunBlocked" => run_output.contains("RunBlocked") || run_output.contains("RUN_BLOCKED"),
        "RunVerifying" => {
            run_output.contains("RunVerifying") || run_output.contains("RUN_VERIFYING")
        }
        _ => true,
    };
    assert!(
        state_matches,
        "expected run output to reflect continuation exit_state={exit_state}\n{run_output}"
    );
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
parallel_worktree = false

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        repo_dir.join(".yarl/workspaces").display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let task_cmd = "mkdir -p tests/fixtures/rust-sample/target && printf 'generated artifact\\n' > tests/fixtures/rust-sample/target/.rustc_info.json && printf 'alpha merged\\n' > alpha.txt";
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge artifact regression",
            "--stream",
            "--cmd",
            task_cmd,
        ],
    );

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
parallel_worktree = false

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        repo_dir.join(".yarl/workspaces").display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let task_cmd = "git config user.email test@yarli.dev && git config user.name 'Yarli Test' && printf 'alpha committed\\n' > alpha.txt && printf 'new committed file\\n' > committed.txt && git add -A && git commit -m 'worker commit'";
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge committed workspace regression",
            "--stream",
            "--cmd",
            task_cmd,
        ],
    );

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
fn run_start_parallel_merge_writes_change_centric_commit_history() {
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
parallel_worktree = false

[run]
auto_commit_interval = 1

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        repo_dir.join(".yarl/workspaces").display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let task_cmd = "git config user.email test@yarli.dev && git config user.name 'Yarli Test' && printf 'alpha committed\\n' > alpha.txt && printf 'new committed file\\n' > committed.txt && git add -A && git commit -m 'worker commit'";
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge commit narrative regression",
            "--stream",
            "--cmd",
            task_cmd,
        ],
    );

    assert!(
        run_output.status.success(),
        "run start command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let subjects = git_stdout(&repo_dir, &["log", "-3", "--pretty=%s"]);
    let mut subject_lines = subjects.lines();
    let latest_subject = subject_lines.next().expect("latest commit subject");

    assert!(
        is_change_centric_subject(latest_subject),
        "expected change-centric latest subject, saw: {latest_subject}"
    );
    assert!(
        !latest_subject.contains("yarli:"),
        "latest subject should not leak workflow metadata: {latest_subject}"
    );
    assert!(
        !latest_subject.contains("tranche"),
        "latest subject should not mention tranche bookkeeping: {latest_subject}"
    );

    for subject in subjects.lines().take(2) {
        assert!(
            !subject.starts_with("yarli:"),
            "recent commit history should avoid workflow-centric subjects: {subjects}"
        );
    }

    let checkpoint_body = git_stdout(
        &repo_dir,
        &[
            "log",
            "--format=%B%x00",
            "--grep",
            "^chore\\(state\\): checkpoint runtime state$",
            "-n",
            "1",
        ],
    );
    if !checkpoint_body.trim().is_empty() {
        assert!(
            checkpoint_body.contains("yarli-run:"),
            "checkpoint commit should preserve yarli run attribution:\n{checkpoint_body}"
        );
    }
}

// NOTE: `run_start_parallel_worktree_auto_commit_writes_checkpoint_message`
// was removed intentionally. It validated that a dedicated "chore(state):
// checkpoint runtime state" commit is written for `.yarli/continuation.json`
// and `.yarli/tranches.toml` after each tranche. That behavior only existed
// because `auto_commit_state_files` used `git add -f` to force state files
// past `.gitignore`. After the `fix(workspace): respect gitignore in
// auto_commit_state_files` change, state files are auto-committed only when
// the operator actually tracks them — in which case they land inside the
// existing change-centric worktree-merge commit rather than a separate
// checkpoint. The untracked/gitignored path is covered by
// `auto_commit_respects_gitignore_for_untracked_state_files` in workspace.rs.

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
parallel_worktree = false

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge preserve skipped workspace regression",
            "--stream",
            "--cmd",
            "git reset --hard HEAD >/dev/null && git clean -fdx >/dev/null",
            "--cmd",
            "printf 'beta merged\\n' > beta.txt",
        ],
    );

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
parallel_worktree = false

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge cleanup on full merge regression",
            "--stream",
            "--cmd",
            "printf 'one\\n' > task_one.txt",
            "--cmd",
            "printf 'two\\n' > task_two.txt",
        ],
    );

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
parallel_worktree = false

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
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge conflict retention regression",
            "--stream",
            "--cmd",
            task_one,
            "--cmd",
            task_two,
        ],
    );

    assert!(
        !run_output.status.success(),
        "run should fail due to merge conflict\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_stdout = String::from_utf8_lossy(&run_output.stdout);
    // The run state machine transiently reaches RunCompleted before merge finalization
    // overrides the continuation to RunFailed. Check the summary exit state instead.
    assert!(
        run_stdout.contains("Exit state:  RunFailed"),
        "conflict run should report RunFailed in summary\nstdout:\n{}",
        run_stdout
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

    let continuation = read_continuation_payload(&repo_dir);
    assert_eq!(
        continuation.get("exit_state").and_then(Value::as_str),
        Some("RunFailed"),
        "expected continuation exit_state to be RunFailed after merge conflict"
    );
    assert_eq!(
        continuation.get("exit_reason").and_then(Value::as_str),
        Some("merge_conflict"),
        "expected continuation exit_reason to be merge_conflict after merge conflict escalation"
    );
    let next_tranche = continuation
        .get("next_tranche")
        .and_then(Value::as_object)
        .expect("expected merge-conflict continuation to include recovery tranche");
    assert_eq!(
        next_tranche.get("kind").and_then(Value::as_str),
        Some("retry_unfinished"),
        "expected recovery tranche kind to be retry_unfinished"
    );
    let retry_task_keys = next_tranche
        .get("retry_task_keys")
        .and_then(Value::as_array)
        .expect("expected retry_task_keys array");
    assert!(
        !retry_task_keys.is_empty(),
        "expected at least one retry task key in recovery tranche"
    );
    assert_output_state_matches_continuation(&run_stdout, &continuation);
}

#[test]
fn run_start_parallel_merge_auto_repair_resolves_overlap_and_completes() {
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
parallel_worktree = false

[run]
merge_conflict_resolution = "auto-repair"

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
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge auto-repair success regression",
            "--stream",
            "--cmd",
            task_one,
            "--cmd",
            task_two,
        ],
    );

    assert!(
        run_output.status.success(),
        "run should succeed after auto-repair resolves overlap\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_stdout = String::from_utf8_lossy(&run_output.stdout);
    let _run_id = parse_run_id(&run_stdout).expect("failed to parse run id from command output");
    assert!(
        run_stdout.contains("RunCompleted") || run_stdout.contains("RUN_COMPLETED"),
        "auto-repair run should complete in output, saw:\n{run_stdout}"
    );

    let run_roots = list_run_workspace_roots(&worktree_root);
    assert_eq!(
        run_roots.len(),
        0,
        "workspace root should be cleaned after successful auto-repair"
    );

    let continuation = read_continuation_payload(&repo_dir);
    assert_eq!(
        continuation.get("exit_state").and_then(Value::as_str),
        Some("RunCompleted"),
        "expected continuation exit_state to be RunCompleted after repaired merge conflict run"
    );
    assert_eq!(
        continuation.get("exit_reason").and_then(Value::as_str),
        Some("completed_all_gates"),
        "auto-repair success should carry completed_all_gates exit reason"
    );
    assert_output_state_matches_continuation(&run_stdout, &continuation);

    let merged_shared =
        std::fs::read_to_string(repo_dir.join("shared.txt")).expect("read merged shared.txt");
    assert_eq!(merged_shared, "task two\n");
    assert!(
        !merged_shared.contains("<<<<<<<"),
        "expected auto-repaired merge result to contain no conflict markers: {merged_shared}"
    );
}

#[test]
fn run_start_parallel_merge_auto_repair_fails_and_preserves_conflict_state() {
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
parallel_worktree = false

[run]
merge_conflict_resolution = "auto-repair"

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let task_one = "git config user.email test@yarli.dev && git config user.name 'Yarli Test' && printf 'task one\\n' > shared.txt && git add shared.txt && git commit -m 'task one commit'";
    let task_two = "git config user.email test@yarli.dev && git config user.name 'Yarli Test' && git rm -f shared.txt && git commit -m 'task two delete'";
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge auto-repair failure regression",
            "--stream",
            "--cmd",
            task_one,
            "--cmd",
            task_two,
        ],
    );

    assert!(
        !run_output.status.success(),
        "run should fail when auto-repair cannot apply overlapping delete conflict\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_stdout = String::from_utf8_lossy(&run_output.stdout);
    // The run state machine transiently reaches RunCompleted before merge finalization
    // overrides the continuation to RunFailed. Check the summary exit state instead.
    assert!(
        run_stdout.contains("Exit state:  RunFailed"),
        "auto-repair failure run should report RunFailed in summary\nstdout:\n{run_stdout}"
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
    let recovery_note = run_root.join("PARALLEL_MERGE_RECOVERY.txt");
    assert!(
        recovery_note.exists(),
        "expected recovery note at {}",
        recovery_note.display()
    );

    let continuation = read_continuation_payload(&repo_dir);
    assert_eq!(
        continuation.get("exit_state").and_then(Value::as_str),
        Some("RunFailed"),
        "expected continuation exit_state to be RunFailed after auto-repair merge conflict failure"
    );
    assert_eq!(
        continuation
            .get("exit_reason")
            .and_then(Value::as_str),
        Some("failed_runtime_error"),
        "expected continuation exit_reason to be failed_runtime_error after auto-repair failure on delete conflict"
    );
    let next_tranche = continuation
        .get("next_tranche")
        .and_then(Value::as_object)
        .expect("expected merge-finalization runtime failure to include recovery tranche");
    let retry_task_keys = next_tranche
        .get("retry_task_keys")
        .and_then(Value::as_array)
        .expect("expected retry_task_keys array");
    assert!(
        !retry_task_keys.is_empty(),
        "expected at least one retry task key for merge-finalization recovery"
    );
    assert_output_state_matches_continuation(&run_stdout, &continuation);
}

#[test]
fn run_plan_parallel_scope_violation_surfaces_allowed_paths_recovery_guidance() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(repo_dir.join("src")).expect("create src dir");
    let worktree_root = repo_dir.join(".yarl/workspaces");
    std::fs::create_dir_all(&worktree_root).expect("create worktree root dir");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["checkout", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.email", "test@yarli.dev"]);
    run_git(&repo_dir, &["config", "user.name", "Yarli Test"]);
    // Isolate from the host's global core.excludesFile so PROMPT.md,
    // yarli.toml, etc. aren't filtered out of git-tracked state depending
    // on whether the runner has a user-level gitignore for them.
    run_git(&repo_dir, &["config", "core.excludesFile", "/dev/null"]);

    std::fs::write(repo_dir.join("src/lib.rs"), "pub fn hi() {}\n").expect("write src/lib.rs");
    std::fs::write(repo_dir.join("README.md"), "baseline\n").expect("write README.md");
    std::fs::write(
        repo_dir.join("mock-agent.sh"),
        r#"#!/bin/sh
set -eu
prompt="${1:-}"
case "$prompt" in
  *YARLI_PREFLIGHT_OK*)
    printf 'YARLI_PREFLIGHT_OK\n'
    ;;
  *)
    printf 'scope leak\n' > README.md
    printf 'task finished\n'
    ;;
esac
"#,
    )
    .expect("write mock agent");
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "initial"]);

    std::fs::write(
        repo_dir.join("PROMPT.md"),
        r#"
```yarli-run
version = 1
objective = "implement scoped plan"
```
"#,
    )
    .expect("write prompt file");
    std::fs::write(
        repo_dir.join("IMPLEMENTATION_PLAN.md"),
        "- [ ] AP-01 tighten merge recovery guidance allowed_paths=src/lib.rs\n",
    )
    .expect("write implementation plan");

    let config = format!(
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true

[features]
parallel = true
parallel_worktree = false

[run]
enforce_plan_tranche_allowed_paths = true

[run.tranche_contract]
enforce_allowed_paths_on_merge = true

[execution]
working_dir = "."
worktree_root = "{}"

[cli]
command = "sh"
args = ["./mock-agent.sh"]
prompt_mode = "arg"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let run_output = Command::new(&binary)
        .current_dir(&repo_dir)
        .args(["run", "--stream"])
        .output()
        .expect("run command invocation failed");

    assert!(
        !run_output.status.success(),
        "run should fail on allowed_paths scope violation during merge finalization\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );

    let run_stdout = String::from_utf8_lossy(&run_output.stdout);
    let combined_output = format!(
        "{}{}",
        run_stdout,
        String::from_utf8_lossy(&run_output.stderr)
    );
    assert!(
        combined_output.contains("Exit state:  RunFailed"),
        "scope violation run should report RunFailed in summary\n{combined_output}"
    );
    // The full set of suggested paths may also include incidentally-changed
    // control files (PROMPT.md, yarli.toml, etc.) depending on what the
    // host filesystem and global gitignore expose; we only require that the
    // guidance surfaces AND names README.md, which the mock agent actually
    // scope-violated.
    let suggestions_line = combined_output
        .lines()
        .find(|line| line.starts_with("Suggested allowed_paths additions:"))
        .unwrap_or_else(|| {
            panic!(
                "scope violation run should surface allowed_paths recovery guidance\n{combined_output}"
            )
        });
    assert!(
        suggestions_line.contains("README.md"),
        "suggestions line must include README.md, saw: {suggestions_line}"
    );

    let continuation = read_continuation_payload(&repo_dir);
    assert_eq!(
        continuation.get("exit_state").and_then(Value::as_str),
        Some("RunFailed"),
        "expected continuation exit_state to be RunFailed after scope violation"
    );
    assert_eq!(
        continuation.get("exit_reason").and_then(Value::as_str),
        Some("failed_runtime_error"),
        "expected continuation exit_reason to be failed_runtime_error after scope violation"
    );
    let next_tranche = continuation
        .get("next_tranche")
        .and_then(Value::as_object)
        .expect("expected scope violation continuation to include recovery tranche");
    assert_eq!(
        next_tranche.get("kind").and_then(Value::as_str),
        Some("retry_unfinished"),
        "expected recovery tranche kind to be retry_unfinished"
    );
    let retry_task_keys = next_tranche
        .get("retry_task_keys")
        .and_then(Value::as_array)
        .expect("expected retry_task_keys array");
    assert!(
        !retry_task_keys.is_empty(),
        "expected at least one retry task key for scope violation recovery"
    );
    let suggested_objective = next_tranche
        .get("suggested_objective")
        .and_then(Value::as_str)
        .expect("expected suggested_objective");
    assert!(
        suggested_objective.contains("allowed_paths scope mismatch"),
        "expected scope mismatch wording in continuation objective: {suggested_objective}"
    );
    assert!(
        suggested_objective.contains("README.md"),
        "expected suggested allowed path in continuation objective: {suggested_objective}"
    );
    assert_output_state_matches_continuation(&run_stdout, &continuation);
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
parallel_worktree = false

[execution]
working_dir = "."
worktree_root = "{}"
"#,
        worktree_root.display()
    );
    std::fs::write(repo_dir.join("yarli.toml"), config).expect("write yarli.toml config");

    let binary = run_output_path();
    let lineage_break_cmd = "git checkout --orphan rogue && git rm -rf . >/dev/null 2>&1 || true && git config user.email test@yarli.dev && git config user.name 'Yarli Test' && printf 'rogue\\n' > rogue.txt && git add -A && git commit -m 'rogue commit'";
    let run_output = run_yarli(
        &binary,
        &repo_dir,
        &[
            "run",
            "start",
            "parallel merge lineage safety regression",
            "--stream",
            "--cmd",
            lineage_break_cmd,
        ],
    );

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
