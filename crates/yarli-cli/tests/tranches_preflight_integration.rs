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

#[test]
fn run_continue_refuses_when_tranches_have_drifted_from_snapshot() {
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

    // PROMPT.md with yarli-run block
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

    // IMPLEMENTATION_PLAN.md with two open tranches
    std::fs::write(
        repo_dir.join("IMPLEMENTATION_PLAN.md"),
        "## Next Work Tranches\n\
         1. TP-05 `Loader`: incomplete.\n\
         2. TP-06 `Guard`: incomplete.\n",
    )
    .expect("write plan file");

    // Valid tranches.toml with both TP-05 and TP-06 open
    std::fs::create_dir_all(repo_dir.join(".yarli")).expect("create .yarli");
    std::fs::write(
        repo_dir.join(".yarli/tranches.toml"),
        r#"version = 1

[[tranches]]
key = "TP-05"
summary = "Implement config loader hardening"
status = "incomplete"

[[tranches]]
key = "TP-06"
summary = "Add continuation drift guard"
status = "incomplete"
"#,
    )
    .expect("write tranches file");

    // continuation.json whose snapshot only mentions TP-05 (drift: TP-06 is missing)
    let prompt_entry_path = repo_dir.join("PROMPT.md");
    let continuation_json = format!(
        r#"{{
  "run_id": "01957a00-0000-7000-8000-000000000001",
  "objective": "continue",
  "exit_state": "RunCompleted",
  "exit_reason": null,
  "completed_at": "2026-03-25T00:00:00Z",
  "tasks": [],
  "summary": {{ "total": 1, "completed": 1, "failed": 0, "cancelled": 0, "pending": 0 }},
  "tranche_token_usage": [],
  "next_tranche": {{
    "suggested_objective": "continue",
    "kind": "planned_next",
    "retry_task_keys": [],
    "unfinished_task_keys": [],
    "planned_task_keys": ["tp05_task"],
    "planned_tranche_key": "TP-05",
    "cursor": {{ "current_tranche_index": 0, "next_tranche_index": 0 }},
    "config_snapshot": {{
      "runtime": {{
        "working_dir": "{}",
        "timeout_secs": 300,
        "task_catalog": [
          {{ "task_key": "tp05_task", "command": "echo tp05", "command_class": "Io" }}
        ],
        "tranche_plan": [
          {{ "key": "TP-05", "objective": "Loader", "task_keys": ["tp05_task"] }}
        ],
        "current_tranche_index": 0,
        "prompt": {{
          "entry_path": "{}",
          "expanded_sha256": "abc123",
          "included_files": []
        }}
      }}
    }},
    "interventions": []
  }}
}}"#,
        repo_dir.display(),
        prompt_entry_path.display()
    );
    std::fs::write(
        repo_dir.join(".yarli/continuation.json"),
        &continuation_json,
    )
    .expect("write continuation.json");

    // yarli.toml with in-memory backend
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
        .args(["run", "continue", "--stream"])
        .output()
        .expect("run continue command invocation failed");

    assert!(
        !run_output.status.success(),
        "run continue should fail when tranches have drifted\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr),
    );
    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );
    assert!(
        combined_output.contains("does not include newer open tranches"),
        "expected drift refusal message in output:\n{combined_output}"
    );
}

/// Binary-level roundtrip: add → list → complete → validate proves the full
/// `yarli plan tranche` CLI path works end-to-end through clap dispatch,
/// `tranches.toml` I/O, and plan validation.
#[test]
fn plan_tranche_add_complete_list_validate_roundtrip() {
    let temp_dir = TempDir::new().expect("create temp workspace");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");

    // Minimal yarli.toml — plan commands only need [core].
    std::fs::write(
        repo_dir.join("yarli.toml"),
        r#"[core]
backend = "in-memory"
allow_in_memory_writes = true
"#,
    )
    .expect("write yarli.toml");

    let binary = run_output_path();

    // Helper to run yarli and return (success, stdout+stderr).
    let yarli = |args: &[&str]| -> (bool, String) {
        let output = Command::new(&binary)
            .current_dir(&repo_dir)
            .args(args)
            .output()
            .unwrap_or_else(|err| panic!("failed to run yarli {args:?}: {err}"));
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        (output.status.success(), combined)
    };

    // 1. Add two tranches.
    let (ok, out) = yarli(&[
        "plan",
        "tranche",
        "add",
        "--key",
        "RT-01",
        "--summary",
        "First roundtrip tranche",
        "--group",
        "roundtrip",
    ]);
    assert!(ok, "tranche add RT-01 should succeed:\n{out}");
    assert!(
        out.contains("Added tranche 'RT-01'"),
        "expected confirmation for RT-01:\n{out}",
    );

    let (ok, out) = yarli(&[
        "plan",
        "tranche",
        "add",
        "--key",
        "RT-02",
        "--summary",
        "Second roundtrip tranche",
        "--group",
        "roundtrip",
        "--verify",
        "cargo test --offline roundtrip",
    ]);
    assert!(ok, "tranche add RT-02 should succeed:\n{out}");
    assert!(
        out.contains("Added tranche 'RT-02'"),
        "expected confirmation for RT-02:\n{out}",
    );

    // 2. List — both should appear as incomplete.
    let (ok, out) = yarli(&["plan", "tranche", "list"]);
    assert!(ok, "tranche list should succeed:\n{out}");
    assert!(
        out.contains("RT-01") && out.contains("RT-02"),
        "list should show both tranches:\n{out}",
    );
    assert!(
        out.contains("[ ]") || out.contains("incomplete"),
        "tranches should be incomplete:\n{out}",
    );

    // 3. Validate — should pass with 2 tranches.
    let (ok, out) = yarli(&["plan", "validate"]);
    assert!(ok, "validate should pass with 2 valid tranches:\n{out}");
    assert!(
        out.contains("2 tranches defined"),
        "expected '2 tranches defined' in validate output:\n{out}",
    );

    // 4. Complete RT-01.
    let (ok, out) = yarli(&["plan", "tranche", "complete", "--key", "RT-01"]);
    assert!(ok, "tranche complete RT-01 should succeed:\n{out}");
    assert!(
        out.contains("Marked tranche 'RT-01' as complete"),
        "expected completion confirmation:\n{out}",
    );

    // 5. List again — RT-01 should be [x], RT-02 still [ ].
    let (ok, out) = yarli(&["plan", "tranche", "list"]);
    assert!(ok, "tranche list after complete should succeed:\n{out}");
    assert!(
        out.contains("[x] RT-01"),
        "RT-01 should be marked complete:\n{out}",
    );
    assert!(
        out.contains("[ ] RT-02"),
        "RT-02 should still be incomplete:\n{out}",
    );

    // 6. Validate again — still 2 tranches, still valid.
    let (ok, out) = yarli(&["plan", "validate"]);
    assert!(
        ok,
        "validate should still pass after completing one:\n{out}"
    );

    // 7. Idempotent add of RT-01 should fail (it's Complete now).
    let (ok, out) = yarli(&[
        "plan",
        "tranche",
        "add",
        "--key",
        "RT-01",
        "--summary",
        "First roundtrip tranche",
        "--group",
        "roundtrip",
        "--idempotent",
    ]);
    assert!(
        !ok,
        "idempotent add of completed tranche should fail:\n{out}",
    );
    assert!(
        out.contains("Complete"),
        "error should mention Complete status:\n{out}",
    );

    // 8. Idempotent add of RT-02 should no-op (it's still Incomplete).
    let (ok, out) = yarli(&[
        "plan",
        "tranche",
        "add",
        "--key",
        "RT-02",
        "--summary",
        "Second roundtrip tranche",
        "--group",
        "roundtrip",
        "--verify",
        "cargo test --offline roundtrip",
        "--idempotent",
    ]);
    assert!(
        ok,
        "idempotent add of matching incomplete tranche should no-op:\n{out}"
    );
    assert!(
        out.contains("no changes made"),
        "expected no-op message:\n{out}",
    );
}
