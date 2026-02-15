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
                panic!("CARGO_BIN_EXE_yarli is not set and target/debug/yarli fallback was not found");
            }
        }
    }
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

    let merged_alpha = std::fs::read_to_string(repo_dir.join("alpha.txt"))
        .expect("read merged alpha.txt");
    assert_eq!(merged_alpha, "alpha merged\n");

    let artifact = repo_dir.join("tests/fixtures/rust-sample/target/.rustc_info.json");
    assert!(
        !artifact.exists(),
        "untracked build artifact should not be merged: {}",
        artifact.display()
    );
}
