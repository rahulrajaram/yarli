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

#[test]
fn run_default_fails_closed_when_tranches_toml_is_malformed() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    std::fs::create_dir_all(repo_dir.join(".yarl/workspaces")).expect("create worktree root dir");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["checkout", "-b", "main"]);
    run_git(&repo_dir, &["config", "user.email", "test@yarli.dev"]);
    run_git(&repo_dir, &["config", "user.name", "Yarli Test"]);
    std::fs::write(repo_dir.join("README.md"), "baseline\n").expect("write baseline");
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "initial"]);

    std::fs::write(
        repo_dir.join("PROMPT.md"),
        r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
    )
    .expect("write prompt file");
    std::fs::write(
        repo_dir.join("IMPLEMENTATION_PLAN.md"),
        "## Next Work Tranches\n1. ST-01 `Structured`: incomplete.\n",
    )
    .expect("write plan file");
    std::fs::create_dir_all(repo_dir.join(".yarli")).expect("create .yarli");
    std::fs::write(
        repo_dir.join(".yarli/tranches.toml"),
        r#"
version = 1
[[tranches]]
key = "ST-01"
summary = "Structured tranche"
status = incomplete
"#,
    )
    .expect("write malformed tranches file");

    let config = format!(
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true

[execution]
working_dir = "."
worktree_root = "{}"

[cli]
command = "sh"
args = ["-lc"]
prompt_mode = "arg"
"#,
        repo_dir.join(".yarl/workspaces").display()
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
        "run should fail closed on malformed tranches.toml\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );
    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );
    assert!(
        combined_output.contains("structured tranches preflight failed"),
        "expected preflight failure message in output:\n{combined_output}"
    );
}
