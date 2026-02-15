use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[cfg(test)]
use crate::config::ensure_write_backend_guard;
use crate::config::{
    load_runtime_config_for_reads, load_runtime_config_for_writes, prepare_audit_sink,
    with_event_store, with_event_store_and_queue, AutoAdvancePolicy, BackendSelection,
    ExecutionRunner, LoadedConfig, UiMode,
};
use yarli_cli::dashboard::{DashboardConfig, DashboardRenderer};
use yarli_cli::mode::{self, RenderMode, TerminalInfo};
use yarli_cli::prompt;
use yarli_cli::stream::{HeadlessRenderer, StreamConfig, StreamEvent, StreamRenderer};
use yarli_core::domain::{
    CancellationActorKind, CancellationProvenance, CancellationSource, CancellationStage,
    CommandClass, EntityType, Event, Evidence, PolicyOutcome, SafeMode,
};
use yarli_core::entities::command_execution::{CommandResourceUsage, TokenUsage};
use yarli_core::entities::merge_intent::{MergeIntent, MergeStrategy};
use yarli_core::entities::run::Run;
use yarli_core::entities::task::Task;
use yarli_core::entities::worktree_binding::{SubmoduleMode, WorktreeBinding};
use yarli_core::explain::{
    explain_run, explain_task, DeteriorationFactor, DeteriorationReport, DeteriorationTrend,
    GateResult, GateType, RunSnapshot, TaskSnapshot,
};
use yarli_core::fsm::merge::MergeState;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_core::fsm::worktree::WorktreeState;
use yarli_core::shutdown::ShutdownController;
use yarli_exec::LocalCommandRunner;
use yarli_gates::{all_passed, default_run_gates, default_task_gates, evaluate_all, GateContext};
use yarli_git::error::{GitError, RecoveryAction};
use yarli_git::{LocalMergeOrchestrator, LocalWorktreeManager, MergeOrchestrator, WorktreeManager};
use yarli_observability::{AuditEntry, AuditSink, JsonlAuditSink};
use yarli_policy::{ActionType, PolicyEngine, PolicyRequest};
#[cfg(test)]
use yarli_queue::{InMemoryTaskQueue, PostgresTaskQueue};
use yarli_queue::{ResourceBudgetConfig, Scheduler, SchedulerConfig, TaskQueue};
use yarli_store::event_store::EventQuery;
use yarli_store::EventStore;
#[cfg(test)]
use yarli_store::{InMemoryEventStore, PostgresEventStore};

use clap::{CommandFactory, Parser};

mod cli;
mod commands;
mod config;
mod events;
mod observers;
mod persistence;
mod plan;
mod projection;
mod render;
mod tranche;
mod workspace;
use crate::events::*;
use crate::persistence::*;
use crate::plan::*;
use crate::projection::*;
use crate::render::*;
use cli::{
    AuditAction, Cli, Commands, GateAction, MergeAction, PlanAction, RunAction, TaskAction,
    TrancheAction, WorktreeAction,
};
use commands::*;

const BUILD_COMMIT: &str = env!("YARLI_BUILD_COMMIT");
const BUILD_DATE: &str = env!("YARLI_BUILD_DATE");
const BUILD_ID: &str = env!("YARLI_BUILD_ID");
const YARLI_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (commit ",
    env!("YARLI_BUILD_COMMIT"),
    ", date ",
    env!("YARLI_BUILD_DATE"),
    ", build ",
    env!("YARLI_BUILD_ID"),
    ")"
);

#[tokio::main]
async fn main() {
    // Initialize tracing (env filter: YARLI_LOG=debug)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("YARLI_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_target(false)
        .init();

    if let Err(e) = run().await {
        error!("{e:#}");
        process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    if let Commands::Init {
        path,
        force,
        print,
        backend,
    } = &cli.command
    {
        return config::cmd_init(path.clone(), *force, *print, *backend);
    }

    let loaded_config = LoadedConfig::load_default().context("failed to load runtime config")?;
    info!(
        config_path = %loaded_config.path().display(),
        config_source = loaded_config.source().label(),
        backend = loaded_config.config().core.backend.as_str(),
        "loaded runtime config"
    );

    // Detect terminal capabilities once; only commands that need a renderer
    // should fail render-mode selection.
    let term_info = TerminalInfo::detect();
    let select_render_mode = || {
        commands::resolve_render_mode(
            &term_info,
            cli.stream,
            cli.tui,
            loaded_config.config().ui.mode,
        )
    };

    match cli.command {
        Commands::Run {
            prompt_file,
            allow_recursive_run,
            action,
        } => match action {
            None => {
                let render_mode = select_render_mode()?;
                cmd_run_default(
                    render_mode,
                    &loaded_config,
                    prompt_file,
                    allow_recursive_run,
                )
                .await
            }
            Some(RunAction::Start {
                objective,
                cmd,
                pace,
                workdir,
                timeout,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                let plan =
                    resolve_run_plan(&loaded_config, objective, cmd, pace, workdir, timeout, None)?;
                let render_mode = select_render_mode()?;
                cmd_run_start(plan, render_mode, &loaded_config, allow_recursive_run).await
            }
            Some(RunAction::Batch {
                objective,
                pace,
                workdir,
                timeout,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                let plan = resolve_run_plan(
                    &loaded_config,
                    objective.unwrap_or_else(|| "batch".to_string()),
                    Vec::new(),
                    pace,
                    workdir,
                    timeout,
                    Some("batch"),
                )?;
                let render_mode = select_render_mode()?;
                cmd_run_start(plan, render_mode, &loaded_config, allow_recursive_run).await
            }
            Some(RunAction::Status { run_id }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_status(&run_id)
            }
            Some(RunAction::ExplainExit { run_id }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_explain(&run_id)
            }
            Some(RunAction::List) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_list()
            }
            Some(RunAction::Continue { file }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                let render_mode = select_render_mode()?;
                cmd_run_continue(file, render_mode, &loaded_config, allow_recursive_run).await
            }
            Some(RunAction::Pause {
                run_id,
                all_active,
                reason,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_pause(run_id.as_deref(), all_active, &reason)
            }
            Some(RunAction::Resume {
                run_id,
                all_paused,
                reason,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_resume(run_id.as_deref(), all_paused, &reason)
            }
            Some(RunAction::Cancel {
                run_id,
                all_active,
                reason,
            }) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_cancel(run_id.as_deref(), all_active, &reason)
            }
            #[cfg(feature = "sw4rm")]
            Some(RunAction::Sw4rm) => {
                if prompt_file.is_some() {
                    bail!("--prompt-file is only valid for default `yarli run` (no subcommand)");
                }
                cmd_run_sw4rm(&loaded_config).await
            }
        },
        Commands::Task { action } => match action {
            TaskAction::List { run_id } => cmd_task_list(&run_id),
            TaskAction::Explain { task_id } => cmd_task_explain(&task_id),
            TaskAction::Unblock { task_id, reason } => cmd_task_unblock(&task_id, &reason),
            TaskAction::Annotate { task_id, detail } => cmd_task_annotate(&task_id, &detail),
            TaskAction::Output { task_id } => cmd_task_output(&task_id),
        },
        Commands::Gate { action } => match action {
            GateAction::List { run } => cmd_gate_list(run),
            GateAction::Rerun { task_id, gate } => cmd_gate_rerun(&task_id, gate.as_deref()),
        },
        Commands::Worktree { action } => match action {
            WorktreeAction::Status { run_id } => cmd_worktree_status(&run_id),
            WorktreeAction::Recover {
                worktree_id,
                action,
            } => cmd_worktree_recover(&worktree_id, &action),
        },
        Commands::Merge { action } => match action {
            MergeAction::Request {
                source,
                target,
                run_id,
                strategy,
            } => cmd_merge_request(&source, &target, &run_id, &strategy),
            MergeAction::Approve { merge_id } => cmd_merge_approve(&merge_id),
            MergeAction::Reject { merge_id, reason } => cmd_merge_reject(&merge_id, &reason),
            MergeAction::Status { merge_id } => cmd_merge_status(&merge_id),
        },
        Commands::Audit { action } => match action {
            AuditAction::Tail {
                file,
                lines,
                category,
            } => cmd_audit_tail(&file, lines, category.as_deref()),
        },
        Commands::Plan { action } => match action {
            PlanAction::Tranche { action } => match action {
                TrancheAction::Add {
                    key,
                    summary,
                    group,
                    allowed_paths,
                } => cmd_plan_tranche_add(&key, &summary, group.as_deref(), &allowed_paths),
                TrancheAction::Complete { key } => cmd_plan_tranche_complete(&key),
                TrancheAction::List => cmd_plan_tranche_list(),
                TrancheAction::Remove { key } => cmd_plan_tranche_remove(&key),
            },
            PlanAction::Validate => cmd_plan_validate(),
        },
        Commands::Init { .. } => unreachable!("init command handled before runtime config load"),
        Commands::Info => {
            // `info` should report capabilities even if the current terminal
            // cannot satisfy a forced render mode.
            let render_mode = select_render_mode().unwrap_or(RenderMode::Stream);
            cmd_info(&term_info, render_mode, &loaded_config)
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observers::{DeteriorationObserver, DeteriorationObserverState};
    use chrono::Utc;
    use std::path::Path;
    use std::process::Command;
    use tempfile::{NamedTempFile, TempDir};
    use yarli_observability::{AuditCategory, AuditEntry, InMemoryAuditSink};
    use yarli_store::event_store::EventQuery;
    use yarli_store::InMemoryEventStore;

    const VALID_UUID: &str = "00000000-0000-0000-0000-000000000000";

    fn write_test_config(contents: &str) -> LoadedConfig {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("yarli.toml");
        std::fs::write(&path, contents).unwrap();
        LoadedConfig::load(path).unwrap()
    }

    fn write_test_config_at(path: &Path, contents: &str) -> LoadedConfig {
        std::fs::write(path, contents).unwrap();
        LoadedConfig::load(path).unwrap()
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
        run_git_expect_ok(temp_dir.path(), &["clone", source_repo_str, workspace_one_str]);

        std::fs::create_dir_all(workspace_one.join("tests/fixtures/rust-sample/target")).unwrap();
        std::fs::write(
            workspace_one.join("tests/fixtures/rust-sample/target/.rustc_info.json"),
            "generated artifact\n",
        )
        .unwrap();
        std::fs::write(
            workspace_one.join("alpha.txt"),
            "alpha merged by task\n",
        )
        .unwrap();

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

    #[cfg(unix)]
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

    #[test]
    fn run_help_mentions_prompt_resolution_precedence() {
        let mut cmd = Cli::command();
        let run = cmd
            .find_subcommand_mut("run")
            .expect("run subcommand should exist");
        let mut help = Vec::new();
        run.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();
        let help_lower = help.to_ascii_lowercase();
        assert!(
            help.contains("--prompt-file")
                && help.contains("[run].prompt_file")
                && help.contains("PROMPT.md")
                && help.contains("yarli run")
                && help.contains("no subcommand")
                && help_lower.contains("built-in yarli policy gates")
                && help_lower.contains("verification command chain")
                && help_lower.contains("observer events are telemetry only")
                && help_lower.contains("pause|resume|cancel"),
            "run --help should mention prompt resolution precedence"
        );
    }

    #[test]
    fn run_config_has_run_spec_data_detects_configured_sections() {
        let loaded_empty = write_test_config(
            r#"
[run]
continue_wait_timeout_seconds = 0
"#,
        );
        assert!(!run_config_has_run_spec_data(&loaded_empty.config().run));

        let loaded_objective = write_test_config(
            r#"
[run]
objective = "verify config"
"#,
        );
        assert!(run_config_has_run_spec_data(&loaded_objective.config().run));

        let loaded_tasks = write_test_config(
            r#"
[[run.tasks]]
key = "lint"
cmd = "cargo clippy --workspace -- -D warnings"
"#,
        );
        assert!(run_config_has_run_spec_data(&loaded_tasks.config().run));
    }

    #[test]
    fn run_spec_from_run_config_maps_tasks_tranches_and_plan_guard() {
        let loaded = write_test_config(
            r#"
[run]
objective = "verify all"

[[run.tasks]]
key = "lint"
cmd = "cargo clippy --workspace -- -D warnings"
class = "cpu"

[[run.tasks]]
key = "test"
cmd = "cargo test --workspace"

[[run.tranches]]
key = "verify"
objective = "verification tranche"
task_keys = ["lint", "test"]

[run.plan_guard]
target = "I8B"
mode = "verify-only"
"#,
        );

        let run_spec = run_spec_from_run_config(&loaded.config().run);
        assert_eq!(run_spec.objective.as_deref(), Some("verify all"));
        assert_eq!(run_spec.tasks.items.len(), 2);
        assert_eq!(run_spec.tasks.items[0].key, "lint");
        assert_eq!(run_spec.tasks.items[0].class.as_deref(), Some("cpu"));
        assert_eq!(
            run_spec
                .tranches
                .as_ref()
                .map(|tranches| tranches.items.len()),
            Some(1)
        );
        let guard = run_spec.plan_guard.as_ref().expect("plan guard expected");
        assert_eq!(guard.target, "I8B");
        assert_eq!(guard.mode, prompt::RunSpecPlanGuardMode::VerifyOnly);
    }

    #[test]
    fn merge_run_specs_applies_prompt_overrides_on_top_of_config_defaults() {
        let base = prompt::RunSpec {
            version: 1,
            objective: Some("config objective".to_string()),
            tasks: prompt::RunSpecTasks {
                items: vec![
                    prompt::RunSpecTask {
                        key: "lint".to_string(),
                        cmd: "cargo clippy --workspace -- -D warnings".to_string(),
                        class: Some("cpu".to_string()),
                    },
                    prompt::RunSpecTask {
                        key: "test".to_string(),
                        cmd: "cargo test --workspace".to_string(),
                        class: Some("io".to_string()),
                    },
                ],
            },
            tranches: Some(prompt::RunSpecTranches {
                items: vec![prompt::RunSpecTranche {
                    key: "verify".to_string(),
                    objective: Some("config tranche".to_string()),
                    task_keys: vec!["lint".to_string(), "test".to_string()],
                }],
            }),
            plan_guard: Some(prompt::RunSpecPlanGuard {
                target: "I8A".to_string(),
                mode: prompt::RunSpecPlanGuardMode::Implement,
            }),
        };
        let prompt_override = prompt::RunSpec {
            version: 1,
            objective: Some("prompt objective".to_string()),
            tasks: prompt::RunSpecTasks {
                items: vec![
                    prompt::RunSpecTask {
                        key: "lint".to_string(),
                        cmd: "cargo clippy --workspace --all-targets -- -D warnings".to_string(),
                        class: Some("cpu".to_string()),
                    },
                    prompt::RunSpecTask {
                        key: "docs".to_string(),
                        cmd: "make docs-build".to_string(),
                        class: Some("io".to_string()),
                    },
                ],
            },
            tranches: Some(prompt::RunSpecTranches {
                items: vec![prompt::RunSpecTranche {
                    key: "prompt-verify".to_string(),
                    objective: Some("prompt tranche".to_string()),
                    task_keys: vec!["lint".to_string(), "docs".to_string()],
                }],
            }),
            plan_guard: Some(prompt::RunSpecPlanGuard {
                target: "I8B".to_string(),
                mode: prompt::RunSpecPlanGuardMode::VerifyOnly,
            }),
        };

        let merged = merge_run_specs(&base, Some(&prompt_override));
        assert_eq!(merged.objective.as_deref(), Some("prompt objective"));
        assert_eq!(merged.tasks.items.len(), 3);
        assert_eq!(merged.tasks.items[0].key, "lint");
        assert!(merged.tasks.items[0]
            .cmd
            .contains("--all-targets -- -D warnings"));
        assert_eq!(merged.tasks.items[1].key, "test");
        assert_eq!(merged.tasks.items[2].key, "docs");
        assert_eq!(
            merged
                .tranches
                .as_ref()
                .map(|tranches| tranches.items[0].key.as_str()),
            Some("prompt-verify")
        );
        assert_eq!(
            merged
                .plan_guard
                .as_ref()
                .map(|guard| guard.target.as_str()),
            Some("I8B")
        );
    }

    #[test]
    fn resolve_render_mode_uses_configured_ui_mode_when_flags_absent() {
        let tty_large = TerminalInfo {
            is_tty: true,
            cols: 120,
            rows: 40,
        };
        assert_eq!(
            resolve_render_mode(&tty_large, false, false, UiMode::Stream).unwrap(),
            RenderMode::Stream
        );
        assert_eq!(
            resolve_render_mode(&tty_large, false, false, UiMode::Tui).unwrap(),
            RenderMode::Dashboard
        );
    }

    #[test]
    fn resolve_render_mode_cli_flags_override_configured_ui_mode() {
        let tty_large = TerminalInfo {
            is_tty: true,
            cols: 120,
            rows: 40,
        };
        assert_eq!(
            resolve_render_mode(&tty_large, true, false, UiMode::Tui).unwrap(),
            RenderMode::Stream
        );
        assert_eq!(
            resolve_render_mode(&tty_large, false, true, UiMode::Stream).unwrap(),
            RenderMode::Dashboard
        );
    }

    #[test]
    fn root_help_mentions_prompt_default_behavior() {
        let mut cmd = Cli::command();
        let mut help = Vec::new();
        cmd.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();
        assert!(
            help.contains("Default workflow")
                && help.contains("PROMPT.md")
                && help.contains("yarli run"),
            "yarli --help should mention PROMPT.md default execution behavior"
        );
    }

    #[test]
    fn resolve_prompt_uses_config_prompt_file_when_set() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        std::fs::create_dir_all(temp.path().join("prompts")).unwrap();
        std::fs::write(temp.path().join("prompts/I8B.md"), "# prompt").unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[run]
prompt_file = "prompts/I8B.md"
"#,
        );

        let resolved = resolve_prompt_entry_path_with_cwd(&loaded, None, temp.path()).unwrap();
        assert_eq!(resolved.source, PromptSource::Config);
        assert_eq!(resolved.entry_path, temp.path().join("prompts/I8B.md"));
    }

    #[test]
    fn resolve_prompt_cli_override_wins_over_config() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        std::fs::create_dir_all(temp.path().join("prompts")).unwrap();
        std::fs::write(temp.path().join("prompts/I8B.md"), "# prompt").unwrap();
        std::fs::write(temp.path().join("prompts/I8C.md"), "# prompt").unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[run]
prompt_file = "prompts/I8B.md"
"#,
        );

        let resolved = resolve_prompt_entry_path_with_cwd(
            &loaded,
            Some(Path::new("prompts/I8C.md")),
            temp.path(),
        )
        .unwrap();
        assert_eq!(resolved.source, PromptSource::Cli);
        assert_eq!(resolved.entry_path, temp.path().join("prompts/I8C.md"));
    }

    #[test]
    fn resolve_prompt_defaults_to_prompt_md_lookup() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        std::fs::create_dir_all(temp.path().join("nested/work")).unwrap();
        std::fs::write(temp.path().join("PROMPT.md"), "# prompt").unwrap();
        let loaded = LoadedConfig::load(temp.path().join("yarli.toml")).unwrap();

        let resolved =
            resolve_prompt_entry_path_with_cwd(&loaded, None, &temp.path().join("nested/work"))
                .unwrap();
        assert_eq!(resolved.source, PromptSource::Default);
        assert_eq!(resolved.entry_path, temp.path().join("PROMPT.md"));
    }

    #[test]
    fn resolve_prompt_relative_paths_use_repo_root_before_config_dir() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        std::fs::create_dir_all(temp.path().join("prompts")).unwrap();
        std::fs::create_dir_all(temp.path().join("config")).unwrap();
        std::fs::write(temp.path().join("prompts/I8B.md"), "# prompt").unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("config/yarli.toml"),
            r#"
[run]
prompt_file = "prompts/I8B.md"
"#,
        );

        let resolved =
            resolve_prompt_entry_path_with_cwd(&loaded, None, &temp.path().join("config")).unwrap();
        assert_eq!(resolved.source, PromptSource::Config);
        assert_eq!(resolved.entry_path, temp.path().join("prompts/I8B.md"));
    }

    #[test]
    fn resolve_prompt_relative_paths_fallback_to_config_dir_without_repo_root() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join("config/prompts")).unwrap();
        std::fs::write(temp.path().join("config/prompts/I8B.md"), "# prompt").unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("config/yarli.toml"),
            r#"
[run]
prompt_file = "prompts/I8B.md"
"#,
        );

        let resolved =
            resolve_prompt_entry_path_with_cwd(&loaded, None, &temp.path().join("somewhere"))
                .unwrap();
        assert_eq!(resolved.source, PromptSource::Config);
        assert_eq!(
            resolved.entry_path,
            temp.path().join("config/prompts/I8B.md")
        );
    }

    #[test]
    fn resolve_prompt_missing_configured_file_error_includes_resolved_path() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join(".git")).unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[run]
prompt_file = "prompts/missing.md"
"#,
        );

        let err = resolve_prompt_entry_path_with_cwd(&loaded, None, temp.path()).unwrap_err();
        assert!(err
            .to_string()
            .contains(&temp.path().join("prompts/missing.md").display().to_string()));
        assert!(err.to_string().contains("run.prompt_file"));
    }

    #[test]
    fn resolve_prompt_rejects_empty_config_prompt_file() {
        let temp = TempDir::new().unwrap();
        let loaded = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[run]
prompt_file = "   "
"#,
        );

        let err = resolve_prompt_entry_path_with_cwd(&loaded, None, temp.path()).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn run_config_snapshot_records_resolved_prompt_entry_path() {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join("prompts")).unwrap();
        let prompt_path = temp.path().join("prompts/I8B.md");
        std::fs::write(
            &prompt_path,
            r#"
```yarli-run
version = 1
objective = "verify"
[tasks]
items = [{ key = "fmt", cmd = "cargo fmt --all -- --check" }]
```
"#,
        )
        .unwrap();

        let loaded_prompt = prompt::load_prompt_and_run_spec(&prompt_path).unwrap();
        let loaded_config = LoadedConfig::load(temp.path().join("yarli.toml")).unwrap();
        let task_catalog = build_task_catalog_from_run_spec(&loaded_prompt.run_spec).unwrap();
        let tranche_plan =
            build_tranche_plan_from_run_spec(&loaded_prompt.run_spec, "verify").unwrap();
        let first_tasks = tasks_for_tranche(&task_catalog, tranche_plan.first().unwrap()).unwrap();
        let snapshot = build_run_config_snapshot(
            &loaded_config,
            ".",
            300,
            &first_tasks,
            &task_catalog,
            None,
            Some(&loaded_prompt.snapshot),
            Some(&loaded_prompt.run_spec),
            &tranche_plan,
            Some(0),
        )
        .unwrap();

        assert_eq!(
            snapshot["runtime"]["prompt"]["entry_path"].as_str(),
            Some(loaded_prompt.snapshot.entry_path.as_str())
        );
    }

    #[test]
    fn plan_driven_sequence_builds_open_tranches_plus_verification() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] I8A first tranche\n- [ ] I8B second tranche\n- [x] I8C done\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement active plan")
                .unwrap();

        assert_eq!(tasks.len(), 3);
        assert_eq!(tranches.len(), 3);
        assert_eq!(tranches[0].key, "I8A");
        assert_eq!(tranches[1].key, "I8B");
        assert_eq!(tranches[2].key, "verification");
        assert!(tasks[0].command.contains("codex"));
        assert!(tasks[0].command.contains("I8A"));
        assert!(tasks[2].command.contains("verification"));
    }

    #[test]
    fn plan_driven_sequence_uses_tranches_file_near_resolved_plan_not_cwd() {
        let repo = TempDir::new().unwrap();
        std::fs::write(
            repo.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            repo.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] MD-99 markdown tranche\n",
        )
        .unwrap();
        std::fs::create_dir_all(repo.path().join(".yarli")).unwrap();
        std::fs::write(
            repo.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "ST-01"
summary = "Structured tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let decoy_cwd = TempDir::new().unwrap();
        std::fs::create_dir_all(decoy_cwd.path().join(".yarli")).unwrap();
        std::fs::write(
            decoy_cwd.path().join(".yarli/tranches.toml"),
            r#"
version = 1

[[tranches]]
key = "WRONG-99"
summary = "Decoy tranche"
status = "incomplete"
"#,
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &repo.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&repo.path().join("PROMPT.md")).unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(decoy_cwd.path()).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement active plan")
                .unwrap();
        std::env::set_current_dir(original_dir).unwrap();

        assert_eq!(tasks.len(), 2);
        assert_eq!(tranches.len(), 2);
        assert_eq!(tranches[0].key, "ST-01");
        assert_eq!(tranches[1].key, "verification");
    }

    #[test]
    fn plan_driven_sequence_runs_verification_only_when_no_open_tranches() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [x] I8A first tranche\n- [x] I8B second tranche\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement active plan")
                .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tranches.len(), 1);
        assert_eq!(tranches[0].key, "verification");
        assert!(tasks[0].command.contains("verification"));
    }

    #[test]
    fn plain_prompt_sequence_dispatches_expanded_prompt_text() {
        let temp = TempDir::new().unwrap();
        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt = prompt::LoadedPrompt {
            entry_path: temp.path().join("PROMPT.md"),
            expanded_text: "# plain prompt\nImplement step 2.3.\n".to_string(),
            snapshot: prompt::PromptSnapshot {
                entry_path: temp.path().join("PROMPT.md").display().to_string(),
                expanded_sha256: "abc".to_string(),
                included_files: Vec::new(),
            },
            run_spec: prompt::RunSpec {
                version: 1,
                objective: None,
                tasks: prompt::RunSpecTasks::default(),
                tranches: None,
                plan_guard: None,
            },
        };

        let (tasks, tranches) =
            build_plain_prompt_run_sequence(&loaded_config, &loaded_prompt, "yarli run").unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tranches.len(), 1);
        assert_eq!(tranches[0].key, "prompt");
        assert_eq!(tasks[0].task_key, "prompt-001");
        assert!(tasks[0].command.contains("plain prompt"));
        assert!(tasks[0].command.contains("Implement step 2.3."));
    }

    #[test]
    fn plan_driven_sequence_ignores_stale_keys_in_non_header_evidence_lines() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement active plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            r#"
## Next Work Tranches
11. I9 `Runtime Contract`: complete. tranche_group=runtime-contract
    Verification evidence:
    1. Open-tranche dispatch evidence remains intact: `yarli run status 019c5308-e73b-7a23-8b7a-c4acc8b95e52` includes `I11` and `YARLI_DETERIORATION_REPORT_V1`.
12. I10 `Follow-up`: complete. tranche_group=runtime-contract

## Notes
1. YARLI_DETERIORATION_REPORT_V1 incomplete in historical notes.
"#,
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement active plan")
                .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tranches.len(), 1);
        assert_eq!(tranches[0].key, "verification");
        assert!(tasks[0].command.contains("verification"));
    }

    #[test]
    fn plan_driven_sequence_groups_adjacent_entries_by_tranche_group_when_enabled() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement grouped plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] I8A first tranche_group=core\n- [ ] I8B second tranche_group=core\n- [ ] I8C third tranche_group=ui\n- [ ] I8D fourth\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"

[run]
enable_plan_tranche_grouping = true
max_grouped_tasks_per_tranche = 0
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) = build_plan_driven_run_sequence(
            &loaded_config,
            &loaded_prompt,
            "implement grouped plan",
        )
        .unwrap();

        assert_eq!(tasks.len(), 5);
        assert_eq!(tranches.len(), 4);
        assert_eq!(tranches[0].key, "group-001-core");
        assert_eq!(tranches[0].tranche_group.as_deref(), Some("core"));
        assert_eq!(tranches[0].task_keys.len(), 2);
        assert_eq!(tranches[1].key, "group-003-ui");
        assert_eq!(tranches[1].tranche_group.as_deref(), Some("ui"));
        assert_eq!(tranches[1].task_keys.len(), 1);
        assert_eq!(tranches[2].key, "I8D");
        assert_eq!(tranches[2].task_keys.len(), 1);
        assert_eq!(tranches[3].key, "verification");
        assert!(tasks[0].command.contains("Tranche group: core."));
        assert!(tasks[2].command.contains("Tranche group: ui."));
    }

    #[test]
    fn plan_driven_sequence_grouping_respects_max_grouped_tasks_per_tranche_cap() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement grouped plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] I8A first tranche_group=core\n- [ ] I8B second tranche_group=core\n- [ ] I8C third tranche_group=core\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"

[run]
enable_plan_tranche_grouping = true
max_grouped_tasks_per_tranche = 2
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (_tasks, tranches) = build_plan_driven_run_sequence(
            &loaded_config,
            &loaded_prompt,
            "implement grouped plan",
        )
        .unwrap();

        assert_eq!(tranches.len(), 3);
        assert_eq!(tranches[0].key, "group-001-core");
        assert_eq!(tranches[0].task_keys.len(), 2);
        assert_eq!(tranches[1].key, "group-003-core");
        assert_eq!(tranches[1].task_keys.len(), 1);
        assert_eq!(tranches[2].key, "verification");
    }

    #[test]
    fn plan_driven_sequence_surfaces_allowed_paths_scope_when_enforced() {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("PROMPT.md"),
            r#"
```yarli-run
version = 1
objective = "implement scoped plan"
```
"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("IMPLEMENTATION_PLAN.md"),
            "- [ ] I8A scoped tranche allowed_paths=src/main.rs,docs/CLI.md,../reject\n",
        )
        .unwrap();

        let loaded_config = write_test_config_at(
            &temp.path().join("yarli.toml"),
            r#"
[cli]
command = "codex"
args = ["exec", "--json"]
prompt_mode = "arg"

[run]
enforce_plan_tranche_allowed_paths = true
"#,
        );
        let loaded_prompt =
            prompt::load_prompt_and_run_spec(&temp.path().join("PROMPT.md")).unwrap();
        let (tasks, tranches) =
            build_plan_driven_run_sequence(&loaded_config, &loaded_prompt, "implement scoped plan")
                .unwrap();

        assert_eq!(tasks.len(), 2);
        assert_eq!(tranches.len(), 2);
        assert_eq!(
            tasks[0].allowed_paths,
            vec!["src/main.rs".to_string(), "docs/CLI.md".to_string()]
        );
        assert!(tasks[0]
            .command
            .contains("Allowed file scope: src/main.rs, docs/CLI.md."));
        assert!(tasks[0]
            .command
            .contains("Restrict edits to the allowed file scope above."));
        assert!(!tasks[0].command.contains("../reject"));
    }

    #[test]
    fn version_includes_build_provenance_fields() {
        let cmd = Cli::command();
        let version = cmd
            .get_version()
            .expect("version should be set on root command");
        assert!(version.contains("commit "));
        assert!(version.contains("date "));
        assert!(version.contains("build "));
    }

    #[test]
    fn resolve_run_plan_rejects_cmd_and_pace() {
        let loaded = write_test_config("");

        let err = resolve_run_plan(
            &loaded,
            "obj".to_string(),
            vec!["echo hi".to_string()],
            Some("batch".to_string()),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn resolve_run_plan_uses_named_pace_for_commands_and_overrides() {
        let loaded = write_test_config(
            r#"
[execution]
working_dir = "/default"
command_timeout_seconds = 111

[run]
default_pace = "batch"

[run.paces.batch]
cmds = ["echo one", "echo two"]
working_dir = "/pace"
command_timeout_seconds = 222
"#,
        );

        let plan = resolve_run_plan(
            &loaded,
            "obj".to_string(),
            Vec::new(),
            Some("batch".to_string()),
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(plan.tasks.len(), 2);
        assert_eq!(plan.tasks[0].task_key, "task-1");
        assert_eq!(plan.tasks[0].command, "echo one");
        assert_eq!(plan.tasks[1].task_key, "task-2");
        assert_eq!(plan.tasks[1].command, "echo two");
        assert_eq!(plan.workdir, "/pace");
        assert_eq!(plan.timeout_secs, 222);
        assert_eq!(plan.pace.as_deref(), Some("batch"));
    }

    #[test]
    fn resolve_run_plan_start_without_cmd_uses_run_default_pace() {
        let loaded = write_test_config(
            r#"
[run]
default_pace = "batch"

[run.paces.batch]
cmds = ["echo ok"]
"#,
        );

        let plan = resolve_run_plan(
            &loaded,
            "obj".to_string(),
            Vec::new(),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].command, "echo ok");
        assert_eq!(plan.pace.as_deref(), Some("batch"));
    }

    #[test]
    fn resolve_run_plan_batch_defaults_to_batch_pace_name() {
        let loaded = write_test_config(
            r#"
[run.paces.batch]
cmds = ["echo ok"]
"#,
        );

        let plan = resolve_run_plan(
            &loaded,
            "batch".to_string(),
            Vec::new(),
            None,
            None,
            None,
            Some("batch"),
        )
        .unwrap();
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].command, "echo ok");
        assert_eq!(plan.pace.as_deref(), Some("batch"));
    }

    #[test]
    fn resolve_run_plan_batch_falls_back_to_default_pace_when_batch_not_defined() {
        let loaded = write_test_config(
            r#"
[run]
default_pace = "ci"

[run.paces.ci]
cmds = ["echo ok"]
"#,
        );

        let plan = resolve_run_plan(
            &loaded,
            "batch".to_string(),
            Vec::new(),
            None,
            None,
            None,
            Some("batch"),
        )
        .unwrap();
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].command, "echo ok");
        assert_eq!(plan.pace.as_deref(), Some("ci"));
    }

    fn make_event(
        entity_type: EntityType,
        entity_id: impl Into<String>,
        event_type: &str,
        correlation_id: Uuid,
        payload: serde_json::Value,
    ) -> Event {
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_type,
            entity_id: entity_id.into(),
            event_type: event_type.to_string(),
            payload,
            correlation_id,
            causation_id: None,
            actor: "test".to_string(),
            idempotency_key: None,
        }
    }

    fn make_command_started(
        command_id: Uuid,
        correlation_id: Uuid,
        command: &str,
        command_class: &str,
    ) -> Event {
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_type: EntityType::Command,
            entity_id: command_id.to_string(),
            event_type: "command.started".to_string(),
            payload: serde_json::json!({
                "command": command,
                "command_class": command_class,
                "working_dir": "/tmp",
            }),
            correlation_id,
            causation_id: None,
            actor: "test".to_string(),
            idempotency_key: None,
        }
    }

    fn make_command_terminal(
        command_id: Uuid,
        correlation_id: Uuid,
        event_type: &str,
        duration_ms: u64,
        exit_code: Option<i32>,
    ) -> Event {
        Event {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_type: EntityType::Command,
            entity_id: command_id.to_string(),
            event_type: event_type.to_string(),
            payload: serde_json::json!({
                "duration_ms": duration_ms,
                "exit_code": exit_code,
            }),
            correlation_id,
            causation_id: None,
            actor: "test".to_string(),
            idempotency_key: None,
        }
    }

    #[test]
    fn incremental_event_cursor_reads_only_new_events() {
        let store = InMemoryEventStore::new();
        let corr = Uuid::now_v7();
        let mut first_batch_ids = Vec::new();
        for i in 0..5 {
            let event = make_event(
                EntityType::Run,
                format!("run-{i}"),
                "run.activated",
                corr,
                serde_json::json!({"to":"RunActive"}),
            );
            first_batch_ids.push(event.event_id);
            store.append(event).unwrap();
        }

        let mut cursor = IncrementalEventCursor::new(EventQuery::by_correlation(corr), 2);
        let first = cursor.read_new_events(&store).unwrap();
        assert_eq!(first.len(), 5);
        assert_eq!(first[0].event_id, first_batch_ids[0]);
        assert_eq!(first[4].event_id, first_batch_ids[4]);

        let second = cursor.read_new_events(&store).unwrap();
        assert!(second.is_empty());

        let mut second_batch_ids = Vec::new();
        for i in 0..2 {
            let event = make_event(
                EntityType::Run,
                format!("run-late-{i}"),
                "run.verifying",
                corr,
                serde_json::json!({"to":"RunVerifying"}),
            );
            second_batch_ids.push(event.event_id);
            store.append(event).unwrap();
        }

        let third = cursor.read_new_events(&store).unwrap();
        assert_eq!(third.len(), 2);
        assert_eq!(third[0].event_id, second_batch_ids[0]);
        assert_eq!(third[1].event_id, second_batch_ids[1]);
    }

    #[test]
    fn deterioration_scoring_distinguishes_stable_vs_deteriorating_trails() {
        let corr = Uuid::now_v7();
        let mut stable = DeteriorationObserverState::new(64);
        let mut stable_events = Vec::new();
        for _ in 0..8 {
            let command_id = Uuid::now_v7();
            stable_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            stable_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                120,
                Some(0),
            ));
        }
        assert!(stable.ingest(&stable_events));
        let stable_report = stable.report();
        assert!(
            stable_report.score < 25.0,
            "stable score={}",
            stable_report.score
        );

        let mut degrading = DeteriorationObserverState::new(64);
        let mut baseline_events = Vec::new();
        for _ in 0..6 {
            let command_id = Uuid::now_v7();
            baseline_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            baseline_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                100,
                Some(0),
            ));
        }
        degrading.ingest(&baseline_events);
        let baseline_report = degrading.report();

        let mut degrade_events = Vec::new();
        for i in 0..6 {
            let command_id = Uuid::now_v7();
            degrade_events.push(make_command_started(command_id, corr, "cargo test", "io"));
            degrade_events.push(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                400 + (i * 250),
                Some(1),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.retrying",
                corr,
                serde_json::json!({"attempt_no": 2}),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.blocked",
                corr,
                serde_json::json!({"reason": "policy_denial"}),
            ));
            degrade_events.push(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "reason": if i % 2 == 0 { "budget_exceeded" } else { "nonzero_exit" },
                    "observed": 120.0 + (i as f64 * 10.0),
                    "limit": 100.0,
                }),
            ));
        }
        assert!(degrading.ingest(&degrade_events));
        let degrading_report = degrading.report();

        assert!(degrading_report.score > baseline_report.score);
        assert_eq!(degrading_report.trend, DeteriorationTrend::Deteriorating);
    }

    #[test]
    fn deterioration_observer_emits_incremental_events() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let command_id = Uuid::now_v7();

        store
            .append(make_command_started(command_id, corr, "cargo test", "io"))
            .unwrap();
        store
            .append(make_command_terminal(
                command_id,
                corr,
                "command.exited",
                100,
                Some(0),
            ))
            .unwrap();

        let mut observer = DeteriorationObserver::new(run_id, corr, 32);
        observer.observe_store(&store).unwrap();
        observer.observe_store(&store).unwrap();

        let observer_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert_eq!(
            observer_events
                .iter()
                .filter(|event| event.event_type == "run.observer.deterioration")
                .count(),
            1
        );

        store
            .append(make_event(
                EntityType::Task,
                Uuid::now_v7().to_string(),
                "task.retrying",
                corr,
                serde_json::json!({"attempt_no": 2}),
            ))
            .unwrap();
        observer.observe_store(&store).unwrap();
        let observer_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert_eq!(
            observer_events
                .iter()
                .filter(|event| event.event_type == "run.observer.deterioration")
                .count(),
            2
        );
    }

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

    fn seed_worktree_event_payload(
        binding: &WorktreeBinding,
        from: &str,
        to: &str,
        reason: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "from": from,
            "to": to,
            "run_id": binding.run_id,
            "task_id": binding.task_id,
            "repo_root": binding.repo_root.display().to_string(),
            "worktree_path": binding.worktree_path.display().to_string(),
            "branch_name": binding.branch_name.clone(),
            "base_ref": binding.base_ref.clone(),
            "head_ref": binding.head_ref.clone(),
            "submodule_mode": "locked",
            "reason": reason,
        })
    }

    fn create_merge_fixture(
        conflict: bool,
    ) -> (TempDir, Uuid, Uuid, String, String, WorktreeBinding) {
        let temp_dir = TempDir::new().unwrap();
        let repo = temp_dir.path();
        run_git_expect_ok(repo, &["init"]);
        run_git_expect_ok(repo, &["checkout", "-b", "main"]);
        run_git_expect_ok(repo, &["config", "user.email", "test@yarli.dev"]);
        run_git_expect_ok(repo, &["config", "user.name", "Yarli Test"]);

        std::fs::write(repo.join("shared.txt"), "base\n").unwrap();
        run_git_expect_ok(repo, &["add", "."]);
        run_git_expect_ok(repo, &["commit", "-m", "initial commit"]);

        let source_branch = "feature/test-merge".to_string();
        let target_branch = "main".to_string();
        run_git_expect_ok(repo, &["checkout", "-b", &source_branch]);
        std::fs::write(repo.join("shared.txt"), "feature change\n").unwrap();
        run_git_expect_ok(repo, &["add", "shared.txt"]);
        run_git_expect_ok(repo, &["commit", "-m", "feature change"]);
        run_git_expect_ok(repo, &["checkout", &target_branch]);

        if conflict {
            std::fs::write(repo.join("shared.txt"), "main conflicting change\n").unwrap();
            run_git_expect_ok(repo, &["add", "shared.txt"]);
            run_git_expect_ok(repo, &["commit", "-m", "main conflict change"]);
        } else {
            std::fs::write(repo.join("main-only.txt"), "main only\n").unwrap();
            run_git_expect_ok(repo, &["add", "main-only.txt"]);
            run_git_expect_ok(repo, &["commit", "-m", "main baseline change"]);
        }

        let run_id = Uuid::now_v7();
        let correlation_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let mut binding = WorktreeBinding::new(
            run_id,
            repo,
            format!("yarl/{}/merge-task", &run_id.to_string()[..8]),
            "main",
            correlation_id,
        )
        .with_task(task_id);
        let manager = LocalWorktreeManager::new();
        block_on_current_runtime(manager.create(&mut binding, CancellationToken::new()))
            .unwrap()
            .unwrap();

        if conflict {
            let (ok, _stdout, _stderr) =
                run_git(&binding.worktree_path, &["merge", &source_branch]);
            assert!(!ok, "expected merge conflict to produce interrupted state");
        }

        (
            temp_dir,
            run_id,
            correlation_id,
            source_branch,
            target_branch,
            binding,
        )
    }

    #[test]
    fn render_run_status_reconstructs_persisted_state() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                corr,
                serde_json::json!({ "objective": "ship it" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.ready",
                corr,
                serde_json::json!({ "from": "TaskOpen", "to": "TaskReady" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({ "from": "TaskVerifying", "to": "TaskComplete" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.completed",
                corr,
                serde_json::json!({ "from": "RunVerifying", "to": "RunCompleted" }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("RunCompleted"));
        assert!(output.contains(&task_id.to_string()));
        assert!(!output.contains("requires a persistent store"));
    }

    #[test]
    fn render_run_status_updated_tracks_latest_task_activity() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let base = Utc::now();

        let mut run_event = make_event(
            EntityType::Run,
            run_id.to_string(),
            "run.activated",
            corr,
            serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
        );
        run_event.occurred_at = base;
        store.append(run_event).unwrap();

        let mut task_event = make_event(
            EntityType::Task,
            task_id.to_string(),
            "task.executing",
            corr,
            serde_json::json!({ "from": "TaskReady", "to": "TaskExecuting" }),
        );
        task_event.occurred_at = base + chrono::Duration::seconds(42);
        store.append(task_event).unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("Last event: task.executing"));
        assert!(output.contains(
            &(base + chrono::Duration::seconds(42))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        ));
    }

    #[test]
    fn list_runs_by_latest_state_ignores_non_transition_events() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.progress",
                corr,
                serde_json::json!({
                    "tick": 3,
                    "summary": "heartbeat"
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.task_catalog",
                corr,
                serde_json::json!({
                    "tasks": []
                }),
            ))
            .unwrap();

        let states = list_runs_by_latest_state(&store).unwrap();
        assert_eq!(states.get(&run_id), Some(&RunState::RunActive));
    }

    #[test]
    fn render_run_list_updated_tracks_latest_task_event() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let base = Utc::now();

        let mut run_event = make_event(
            EntityType::Run,
            run_id.to_string(),
            "run.activated",
            corr,
            serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
        );
        run_event.occurred_at = base;
        store.append(run_event).unwrap();

        let mut task_event = make_event(
            EntityType::Task,
            task_id.to_string(),
            "task.ready",
            corr,
            serde_json::json!({ "from": "TaskOpen", "to": "TaskReady" }),
        );
        task_event.occurred_at = base + chrono::Duration::minutes(3);
        store.append(task_event).unwrap();

        let output = render_run_list(&store).unwrap();
        assert!(output.contains(
            &(base + chrono::Duration::minutes(3))
                .format("%Y-%m-%d %H:%M")
                .to_string()
        ));
    }

    #[test]
    fn render_run_status_surfaces_tranche_group_task_worker_mapping() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                corr,
                serde_json::json!({
                    "objective": "ship grouped tranche",
                    "config_snapshot": {
                        "runtime": {
                            "tranche_plan": [
                                {
                                    "key": "group-001-core",
                                    "objective": "core work",
                                    "task_keys": ["core_task"],
                                    "tranche_group": "core"
                                }
                            ]
                        }
                    }
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.task_catalog",
                corr,
                serde_json::json!({
                    "tasks": [
                        {
                            "task_id": task_id,
                            "task_key": "core_task",
                            "tranche_key": "group-001-core",
                            "tranche_group": "core",
                            "allowed_paths": ["src/main.rs"]
                        }
                    ]
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.executing",
                corr,
                serde_json::json!({
                    "from": "TaskReady",
                    "to": "TaskExecuting",
                    "task_key": "core_task",
                    "worker": "worker-a"
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({
                    "from": "TaskVerifying",
                    "to": "TaskComplete",
                    "task_key": "core_task"
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("scope: tranche=group-001-core group=core allowed_paths=src/main.rs")
        );
        assert!(output.contains("Tranche mapping:"));
        assert!(output.contains("tranche=group-001-core group=core"));
        assert!(output.contains("task_key=core_task"));
        assert!(output.contains(&task_id.to_string()));
        assert!(output.contains("worker_actor=worker-a"));
    }

    #[test]
    fn event_to_stream_event_maps_observer_progress_to_transient_status() {
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let event = make_event(
            EntityType::Run,
            run_id.to_string(),
            "run.observer.progress",
            corr,
            serde_json::json!({
                "summary": "heartbeat pending=1 leased=0"
            }),
        );

        let mapped = event_to_stream_event(&event, &[]).expect("progress event should map");
        match mapped {
            StreamEvent::TransientStatus { message, .. } => {
                assert_eq!(message, "heartbeat pending=1 leased=0");
            }
            other => panic!("expected transient status, got {other:?}"),
        }
    }

    #[test]
    fn event_to_stream_event_maps_command_output_chunks_to_task_output() {
        let corr = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let command_id = Uuid::now_v7();
        let event = Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Command,
            entity_id: command_id.to_string(),
            event_type: "command.output".to_string(),
            payload: serde_json::json!({
                "chunk_count": 2,
                "chunks": [
                    { "seq": 1, "stream": "stdout", "data": "line one" },
                    { "seq": 2, "stream": "stderr", "data": "line two" }
                ]
            }),
            correlation_id: corr,
            causation_id: None,
            actor: "worker".to_string(),
            idempotency_key: Some(format!("{task_id}:cmd:1:output")),
        };

        let mapped = event_to_stream_event(&event, &[(task_id, "tranche-001-i5".to_string())])
            .expect("command output should map");
        match mapped {
            StreamEvent::CommandOutput {
                task_id: mapped_task_id,
                task_name,
                line,
            } => {
                assert_eq!(mapped_task_id, task_id);
                assert_eq!(task_name, "tranche-001-i5");
                assert_eq!(line, "line one\nline two");
            }
            other => panic!("expected command output stream event, got {other:?}"),
        }
    }

    #[test]
    fn event_to_stream_event_skips_empty_command_output_chunks() {
        let corr = Uuid::now_v7();
        let event = Event {
            event_id: Uuid::now_v7(),
            occurred_at: chrono::Utc::now(),
            entity_type: EntityType::Command,
            entity_id: Uuid::now_v7().to_string(),
            event_type: "command.output".to_string(),
            payload: serde_json::json!({
                "chunk_count": 1,
                "chunks": [
                    { "seq": 1, "stream": "stdout", "data": "   " }
                ]
            }),
            correlation_id: corr,
            causation_id: None,
            actor: "worker".to_string(),
            idempotency_key: Some(format!("{}:cmd:1:output", Uuid::now_v7())),
        };

        assert!(
            event_to_stream_event(&event, &[]).is_none(),
            "empty command output should not emit stream event"
        );
    }

    #[test]
    fn stream_task_catalog_entries_extracts_task_ids_and_keys() {
        let corr = Uuid::now_v7();
        let task_a = Uuid::now_v7();
        let task_b = Uuid::now_v7();
        let event = make_event(
            EntityType::Run,
            Uuid::now_v7().to_string(),
            "run.task_catalog",
            corr,
            serde_json::json!({
                "tasks": [
                    { "task_id": task_a, "task_key": "first" },
                    { "task_id": task_b }
                ]
            }),
        );

        let entries = stream_task_catalog_entries(&event);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (task_a, "first".to_string()));
        assert_eq!(entries[1].0, task_b);
    }

    #[test]
    fn task_catalog_entries_from_event_extracts_workspace_dir() {
        let corr = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let event = make_event(
            EntityType::Run,
            Uuid::now_v7().to_string(),
            "run.task_catalog",
            corr,
            serde_json::json!({
                "tasks": [
                    {
                        "task_id": task_id,
                        "task_key": "first",
                        "workspace_dir": "/tmp/yarli-ws/task-001"
                    }
                ]
            }),
        );

        let entries = task_catalog_entries_from_event(&event);
        let entry = entries.get(&task_id).expect("task should be present");
        assert_eq!(
            entry.workspace_dir.as_deref(),
            Some("/tmp/yarli-ws/task-001")
        );
    }

    #[test]
    fn emit_initial_stream_state_emits_run_and_task_discovery() {
        let run_id = Uuid::now_v7();
        let task_a = Uuid::now_v7();
        let task_b = Uuid::now_v7();
        let task_names = vec![
            (task_a, "tranche-001".to_string()),
            (task_b, "tranche-002".to_string()),
        ];
        let (tx, mut rx) = mpsc::unbounded_channel::<StreamEvent>();

        emit_initial_stream_state(&tx, run_id, "objective", &task_names);

        match rx.try_recv().expect("run started event") {
            StreamEvent::RunStarted {
                run_id: received_run_id,
                objective,
                ..
            } => {
                assert_eq!(received_run_id, run_id);
                assert_eq!(objective, "objective");
            }
            other => panic!("expected run started event, got {other:?}"),
        }

        match rx.try_recv().expect("task A discovery") {
            StreamEvent::TaskDiscovered { task_id, task_name } => {
                assert_eq!(task_id, task_a);
                assert_eq!(task_name, "tranche-001");
            }
            other => panic!("expected task discovered event, got {other:?}"),
        }

        match rx.try_recv().expect("task B discovery") {
            StreamEvent::TaskDiscovered { task_id, task_name } => {
                assert_eq!(task_id, task_b);
                assert_eq!(task_name, "tranche-002");
            }
            other => panic!("expected task discovered event, got {other:?}"),
        }
    }

    #[test]
    fn render_run_explain_reads_persisted_gate_failures() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.tests_passed: 2 test(s) failing",
                    "failures": ["gate.tests_passed: 2 test(s) failing"],
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.required_tasks_closed: 1 required task(s) not complete",
                    "failures": ["gate.required_tasks_closed: 1 required task(s) not complete"],
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(output.contains("Failed gates: 2"));
        assert!(output.contains("gate.tests_passed"));
        assert!(output.contains("gate.required_tasks_closed"));
    }

    #[test]
    fn render_task_explain_reads_persisted_gate_failures() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.gate_failed",
                corr,
                serde_json::json!({
                    "reason": "1 gate(s) failed: gate.required_evidence_present: no evidence records found",
                    "failures": ["gate.required_evidence_present: no evidence records found"],
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(output.contains("Task explain for"));
        assert!(output.contains("Failed gates: 1"));
        assert!(output.contains("gate.required_evidence_present"));
        assert!(output.contains("no evidence records found"));
    }

    #[test]
    fn execute_task_unblock_appends_unblocked_event() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.blocked",
                corr,
                serde_json::json!({ "from": "TaskReady", "to": "TaskBlocked", "reason": "dependency pending" }),
            ))
            .unwrap();

        let output = execute_task_unblock(&store, task_id, "manual override").unwrap();
        assert!(output.contains("TaskBlocked -> TaskReady"));

        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Task,
                task_id.to_string(),
            ))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "task.unblocked"),
            "expected task.unblocked event to be persisted"
        );
    }

    #[test]
    fn render_worktree_status_reads_persisted_events() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let worktree_id = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                worktree_id.to_string(),
                "worktree.bound",
                corr,
                serde_json::json!({ "to": "WtBoundHome" }),
            ))
            .unwrap();

        let output = render_worktree_status(&store, run_id).unwrap();
        assert!(output.contains(&worktree_id.to_string()));
        assert!(output.contains("WtBoundHome"));
    }

    #[test]
    fn render_merge_status_reads_persisted_events() {
        let store = InMemoryEventStore::new();
        let merge_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({ "state": "MergeRequested", "reason": "pending approval" }),
            ))
            .unwrap();

        let output = render_merge_status(&store, merge_id).unwrap();
        assert!(output.contains("MergeRequested"));
        assert!(output.contains("pending approval"));
    }

    // ── parse_gate_type ──────────────────────────────────────────────

    #[test]
    fn parse_gate_type_valid_names() {
        assert_eq!(
            parse_gate_type("required_tasks_closed"),
            Some(GateType::RequiredTasksClosed)
        );
        assert_eq!(parse_gate_type("tests_passed"), Some(GateType::TestsPassed));
        assert_eq!(parse_gate_type("policy_clean"), Some(GateType::PolicyClean));
    }

    #[test]
    fn parse_gate_type_unknown_returns_none() {
        assert_eq!(parse_gate_type("nonexistent"), None);
        assert_eq!(parse_gate_type(""), None);
    }

    #[test]
    fn all_gate_names_covers_all_gate_types() {
        let names = all_gate_names();
        assert_eq!(names.len(), 7);
        for name in &names {
            assert!(
                parse_gate_type(name).is_some(),
                "gate name {name} should parse to a GateType"
            );
        }
    }

    // ── UUID validation ──────────────────────────────────────────────

    #[test]
    fn cmd_task_unblock_rejects_invalid_uuid() {
        let result = cmd_task_unblock("not-a-uuid", "test reason");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_task_unblock_blocks_in_memory_writes_by_default() {
        let result = cmd_task_unblock(VALID_UUID, "test reason");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn cmd_run_status_rejects_invalid_uuid() {
        let result = cmd_run_status("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_run_explain_rejects_invalid_uuid() {
        let result = cmd_run_explain("bad");
        assert!(result.is_err());
    }

    #[test]
    fn render_run_list_empty_store() {
        let store = InMemoryEventStore::new();
        let output = render_run_list(&store).unwrap();
        assert!(output.contains("No runs found"));
    }

    #[test]
    fn render_run_list_shows_active_run() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.config_snapshot",
                corr,
                serde_json::json!({ "objective": "ship it" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let output = render_run_list(&store).unwrap();
        let map = unique_run_id_prefixes(vec![run_id.to_string()], 10);
        let prefix = map.get(&run_id.to_string()).unwrap();
        assert!(output.contains(prefix.as_str()));
        assert!(output.contains("RunActive"));
        assert!(output.contains("ship it"));
    }

    #[test]
    fn render_run_list_counts_tasks() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({ "from": "TaskExecuting", "to": "TaskComplete" }),
            ))
            .unwrap();

        let output = render_run_list(&store).unwrap();
        // 1 complete, 0 failed, 1 total => "1/0/1"
        assert!(output.contains("1/0/1"));
    }

    #[test]
    fn resolve_run_id_input_accepts_unique_prefix() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::parse_str("019c5056-d8a7-7133-9ad0-77652b8be1e8").unwrap();
        let corr = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let resolved = resolve_run_id_input(&store, "019c5056d8a7").unwrap();
        assert_eq!(resolved, run_id);
    }

    #[test]
    fn resolve_run_id_input_rejects_ambiguous_prefix() {
        let store = InMemoryEventStore::new();
        let a = Uuid::parse_str("019c4f51-aaaa-7000-8000-000000000001").unwrap();
        let b = Uuid::parse_str("019c4f51-bbbb-7000-8000-000000000002").unwrap();
        let corr_a = Uuid::now_v7();
        let corr_b = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                a.to_string(),
                "run.activated",
                corr_a,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                b.to_string(),
                "run.activated",
                corr_b,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let err = resolve_run_id_input(&store, "019c4f51").unwrap_err();
        assert!(err.to_string().contains("ambiguous run ID prefix"));
    }

    #[test]
    fn resolve_run_id_input_rejects_unknown_prefix() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let err = resolve_run_id_input(&store, "deadbeef").unwrap_err();
        assert!(err.to_string().contains("unique run-list prefix"));
    }

    #[test]
    fn unique_run_id_prefixes_expand_on_collision() {
        let a = "019c4f51-aaaa-7000-8000-000000000001".to_string();
        let b = "019c4f51-bbbb-7000-8000-000000000002".to_string();
        let map = unique_run_id_prefixes(vec![a.clone(), b.clone()], 10);
        let pa = map.get(&a).unwrap();
        let pb = map.get(&b).unwrap();
        assert_ne!(pa, pb);
        assert!(compact_run_id(&a).starts_with(pa));
        assert!(compact_run_id(&b).starts_with(pb));
    }

    #[test]
    fn cmd_task_list_rejects_invalid_uuid() {
        let result = cmd_task_list("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_task_explain_rejects_invalid_uuid() {
        let result = cmd_task_explain("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_worktree_status_rejects_invalid_uuid() {
        let result = cmd_worktree_status("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_merge_approve_rejects_invalid_uuid() {
        let result = cmd_merge_approve("bad");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_merge_reject_rejects_invalid_uuid() {
        let result = cmd_merge_reject("bad", "reason");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_merge_status_rejects_invalid_uuid() {
        let result = cmd_merge_status("bad");
        assert!(result.is_err());
    }

    // ── gate list ────────────────────────────────────────────────────

    #[test]
    fn cmd_gate_list_task_level() {
        assert!(cmd_gate_list(false).is_ok());
    }

    #[test]
    fn cmd_gate_list_run_level() {
        assert!(cmd_gate_list(true).is_ok());
    }

    // ── gate rerun validation ────────────────────────────────────────

    #[test]
    fn cmd_gate_rerun_rejects_invalid_uuid() {
        let result = cmd_gate_rerun("not-uuid", None);
        assert!(result.is_err());
    }

    #[test]
    fn cmd_gate_rerun_rejects_unknown_gate() {
        let result = cmd_gate_rerun(VALID_UUID, Some("nonexistent_gate"));
        assert!(result.is_err());
    }

    #[test]
    fn cmd_gate_rerun_accepts_valid_gate() {
        let result = cmd_gate_rerun(VALID_UUID, Some("tests_passed"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn cmd_gate_rerun_blocks_in_memory_writes_by_default() {
        let result = cmd_gate_rerun(VALID_UUID, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn execute_gate_rerun_persists_single_gate_evaluation() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let command_id = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.verifying",
                corr,
                serde_json::json!({ "from": "TaskExecuting", "to": "TaskVerifying" }),
            ))
            .unwrap();
        store
            .append(Event {
                event_id: Uuid::now_v7(),
                occurred_at: Utc::now(),
                entity_type: EntityType::Command,
                entity_id: command_id.to_string(),
                event_type: "command.started".to_string(),
                payload: serde_json::json!({
                    "command": "cargo test",
                    "working_dir": "/tmp",
                    "command_class": "io",
                }),
                correlation_id: corr,
                causation_id: None,
                actor: "test".to_string(),
                idempotency_key: Some(format!("{task_id}:cmd:1:started")),
            })
            .unwrap();
        store
            .append(Event {
                event_id: Uuid::now_v7(),
                occurred_at: Utc::now(),
                entity_type: EntityType::Command,
                entity_id: command_id.to_string(),
                event_type: "command.exited".to_string(),
                payload: serde_json::json!({
                    "exit_code": 0,
                    "duration_ms": 42,
                    "state": "CmdExited",
                }),
                correlation_id: corr,
                causation_id: None,
                actor: "test".to_string(),
                idempotency_key: Some(format!("{task_id}:cmd:1:terminal")),
            })
            .unwrap();

        let output = execute_gate_rerun(&store, task_id, Some("tests_passed")).unwrap();
        assert!(output.contains("gate.tests_passed: PASS"));
        assert!(!output.contains("requires a persistent store"));

        let gate_events = store
            .query(&EventQuery::by_entity(
                EntityType::Gate,
                task_id.to_string(),
            ))
            .unwrap();
        assert_eq!(gate_events.len(), 1);
        assert_eq!(gate_events[0].event_type, "gate.evaluated");
        assert_eq!(gate_events[0].payload["gate"], "gate.tests_passed");
        assert_eq!(gate_events[0].payload["status"], "passed");
        let expected_gate_key = format!("{task_id}:gate_rerun:gate.tests_passed");
        assert_eq!(
            gate_events[0].idempotency_key.as_deref(),
            Some(expected_gate_key.as_str())
        );
    }

    #[test]
    fn execute_gate_rerun_persists_all_default_task_gates() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.verifying",
                corr,
                serde_json::json!({ "from": "TaskExecuting", "to": "TaskVerifying" }),
            ))
            .unwrap();

        let output = execute_gate_rerun(&store, task_id, None).unwrap();
        assert!(output.contains("Evaluated 5 gate(s):"));
        assert!(!output.contains("requires a persistent store"));

        let gate_events = store
            .query(&EventQuery::by_entity(
                EntityType::Gate,
                task_id.to_string(),
            ))
            .unwrap();
        assert_eq!(gate_events.len(), default_task_gates().len());
        assert!(gate_events
            .iter()
            .all(|event| event.event_type == "gate.evaluated"));
        assert!(gate_events.iter().all(|event| event
            .idempotency_key
            .as_deref()
            .is_some_and(|key| key.starts_with(&format!("{task_id}:gate_rerun:gate.")))));
    }

    // ── worktree recover validation ──────────────────────────────────

    #[test]
    fn cmd_worktree_recover_blocks_in_memory_writes_by_default() {
        let abort = cmd_worktree_recover(VALID_UUID, "abort");
        let resume = cmd_worktree_recover(VALID_UUID, "resume");
        let manual_block = cmd_worktree_recover(VALID_UUID, "manual-block");
        assert!(abort.is_err());
        assert!(resume.is_err());
        assert!(manual_block.is_err());
        assert!(abort
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn cmd_worktree_recover_rejects_invalid_action() {
        let result = cmd_worktree_recover(VALID_UUID, "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn execute_worktree_recover_abort_persists_state_and_updates_status_projection() {
        let store = InMemoryEventStore::new();
        let (_temp_dir, run_id, corr, source_branch, _target_branch, binding) =
            create_merge_fixture(true);
        let worktree_id = binding.id;

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                worktree_id.to_string(),
                "worktree.conflict_detected",
                corr,
                seed_worktree_event_payload(
                    &binding,
                    "WtMerging",
                    "WtConflict",
                    "merge conflict requires recovery",
                ),
            ))
            .unwrap();

        let output = execute_worktree_recover(&store, worktree_id, "abort").unwrap();
        assert!(output.contains("Resulting state: WtBoundHome"));
        assert!(output.contains("Side effect: executed"));
        assert!(!output.contains("requires a persistent store"));

        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Worktree,
                worktree_id.to_string(),
            ))
            .unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "worktree.recovery_started"));
        assert!(events
            .iter()
            .any(|event| event.event_type == "worktree.recovered"));
        let started = events
            .iter()
            .find(|event| event.event_type == "worktree.recovery_started")
            .expect("expected worktree.recovery_started event");
        let expected_started_key =
            format!("{worktree_id}:worktree_recover:abort:worktree.recovery_started");
        assert_eq!(
            started.idempotency_key.as_deref(),
            Some(expected_started_key.as_str())
        );
        let recovered = events
            .iter()
            .find(|event| event.event_type == "worktree.recovered")
            .expect("expected worktree.recovered event");
        let expected_recovered_key =
            format!("{worktree_id}:worktree_recover:abort:worktree.recovered");
        assert_eq!(
            recovered.idempotency_key.as_deref(),
            Some(expected_recovered_key.as_str())
        );
        assert_eq!(recovered.payload["side_effect"].as_bool(), Some(true));

        let (merge_head_ok, _stdout, _stderr) = run_git(
            &binding.worktree_path,
            &["rev-parse", "--verify", "MERGE_HEAD"],
        );
        assert!(
            !merge_head_ok,
            "MERGE_HEAD should be cleared after abort recovery on branch {source_branch}"
        );

        let status_output = render_worktree_status(&store, run_id).unwrap();
        assert!(status_output.contains(&worktree_id.to_string()));
        assert!(status_output.contains("WtBoundHome"));
    }

    #[test]
    fn execute_worktree_recover_manual_block_persists_recovering_state() {
        let store = InMemoryEventStore::new();
        let (_temp_dir, run_id, corr, _source_branch, _target_branch, binding) =
            create_merge_fixture(true);
        let worktree_id = binding.id;

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                worktree_id.to_string(),
                "worktree.conflict_detected",
                corr,
                seed_worktree_event_payload(
                    &binding,
                    "WtMerging",
                    "WtConflict",
                    "manual intervention required",
                ),
            ))
            .unwrap();

        let output = execute_worktree_recover(&store, worktree_id, "manual-block").unwrap();
        assert!(output.contains("Resulting state: WtRecovering"));
        assert!(output.contains("Side effect: blocked"));
        assert!(!output.contains("requires a persistent store"));

        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Worktree,
                worktree_id.to_string(),
            ))
            .unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "worktree.recovery_blocked"));
        let blocked = events
            .iter()
            .find(|event| event.event_type == "worktree.recovery_blocked")
            .expect("expected worktree.recovery_blocked event");
        let expected_blocked_key =
            format!("{worktree_id}:worktree_recover:manual-block:worktree.recovery_blocked");
        assert_eq!(
            blocked.idempotency_key.as_deref(),
            Some(expected_blocked_key.as_str())
        );
        assert_eq!(blocked.payload["side_effect"].as_bool(), Some(true));

        let status_output = render_worktree_status(&store, run_id).unwrap();
        assert!(status_output.contains("WtRecovering"));
    }

    // ── merge lifecycle commands ─────────────────────────────────────

    fn merge_id_from_output(output: &str) -> Uuid {
        output
            .lines()
            .find_map(|line| line.strip_prefix("Merge ID: "))
            .and_then(|raw| raw.trim().parse::<Uuid>().ok())
            .expect("expected merge ID line in output")
    }

    #[test]
    fn cmd_merge_request_blocks_in_memory_writes_by_default() {
        let merge_no_ff = cmd_merge_request("feat", "main", VALID_UUID, "merge-no-ff");
        let rebase_then_ff = cmd_merge_request("feat", "main", VALID_UUID, "rebase-then-ff");
        let squash_merge = cmd_merge_request("feat", "main", VALID_UUID, "squash-merge");
        assert!(merge_no_ff.is_err());
        assert!(rebase_then_ff.is_err());
        assert!(squash_merge.is_err());
        assert!(merge_no_ff
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn ensure_write_backend_guard_blocks_in_memory_writes_by_default() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("yarli.toml");
        let loaded_config = LoadedConfig::load(config_path).unwrap();

        let result = ensure_write_backend_guard(&loaded_config, "task unblock");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refuses in-memory write mode"));
    }

    #[test]
    fn ensure_write_backend_guard_allows_explicit_ephemeral_override() {
        let loaded_config = write_test_config(
            r#"
[core]
backend = "in-memory"
allow_in_memory_writes = true
"#,
        );

        let result = ensure_write_backend_guard(&loaded_config, "task unblock");
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_merge_request_rejects_invalid_strategy() {
        let result = cmd_merge_request("feat", "main", VALID_UUID, "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn cmd_merge_request_rejects_invalid_uuid() {
        let result = cmd_merge_request("feat", "main", "bad", "merge-no-ff");
        assert!(result.is_err());
    }

    #[test]
    fn execute_merge_request_persists_requested_event() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let output =
            execute_merge_request(&store, "feat/login", "main", run_id, "merge-no-ff").unwrap();
        assert!(output.contains("Merge intent requested"));
        assert!(!output.contains("requires a persistent store"));

        let merge_id = merge_id_from_output(&output);
        let events = store
            .query(&EventQuery::by_entity(
                EntityType::Merge,
                merge_id.to_string(),
            ))
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "merge.requested");
        assert_eq!(events[0].payload["state"], "MergeRequested");
        assert_eq!(events[0].payload["source"], "feat/login");
        assert_eq!(events[0].payload["target"], "main");
        assert_eq!(events[0].payload["strategy"], "merge-no-ff");
        let expected_request_key = format!("{run_id}:merge_request:feat/login:main:merge-no-ff");
        assert_eq!(
            events[0].idempotency_key.as_deref(),
            Some(expected_request_key.as_str())
        );
    }

    #[test]
    fn execute_merge_approve_persists_policy_and_transition_events() {
        let store = InMemoryEventStore::new();
        let merge_id = Uuid::now_v7();
        let audit = InMemoryAuditSink::new();
        let (_temp_dir, run_id, corr, source_branch, target_branch, binding) =
            create_merge_fixture(false);

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                binding.id.to_string(),
                "worktree.bound",
                corr,
                seed_worktree_event_payload(
                    &binding,
                    "WtCreating",
                    "WtBoundHome",
                    "worktree created for merge",
                ),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({
                    "to": "MergeRequested",
                    "state": "MergeRequested",
                    "run_id": run_id,
                    "worktree_id": binding.id,
                    "source": source_branch,
                    "target": target_branch,
                    "strategy": "merge-no-ff",
                    "reason": "pending approval",
                }),
            ))
            .unwrap();

        let output =
            execute_merge_approve(&store, merge_id, SafeMode::Execute, true, Some(&audit)).unwrap();
        assert!(output.contains("approved and executed to MergeDone"));
        assert!(!output.contains("requires a persistent store"));

        let merge_events = store
            .query(&EventQuery::by_entity(
                EntityType::Merge,
                merge_id.to_string(),
            ))
            .unwrap();
        assert!(merge_events
            .iter()
            .any(|event| event.event_type == "merge.approved"));
        let approved = merge_events
            .iter()
            .find(|event| event.event_type == "merge.approved")
            .expect("expected merge.approved event");
        let expected_approved_key = format!("{merge_id}:merge.approve:merge.approved");
        assert_eq!(
            approved.idempotency_key.as_deref(),
            Some(expected_approved_key.as_str())
        );
        let execution = merge_events
            .iter()
            .find(|event| event.event_type == "merge.execution_succeeded")
            .expect("expected merge.execution_succeeded event");
        assert_eq!(execution.payload["state"], "MergeDone");
        let merge_sha = execution
            .payload
            .get("merge_sha")
            .and_then(|value| value.as_str())
            .expect("expected merge_sha");
        assert_eq!(merge_sha.len(), 40);

        let all_events = store.query(&EventQuery::by_correlation(corr)).unwrap();
        let policy = all_events
            .iter()
            .find(|event| event.event_type == "policy.decision")
            .expect("expected policy.decision event");
        let expected_policy_key = format!("{merge_id}:merge.approve:policy.decision");
        assert_eq!(
            policy.idempotency_key.as_deref(),
            Some(expected_policy_key.as_str())
        );

        let audit_entries = audit.read_all().unwrap();
        assert!(audit_entries
            .iter()
            .any(|entry| entry.category == AuditCategory::PolicyDecision));
    }

    #[test]
    fn execute_merge_approve_in_observe_mode_blocks_and_audits() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let merge_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let audit = InMemoryAuditSink::new();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({
                    "to": "MergeRequested",
                    "state": "MergeRequested",
                    "run_id": run_id,
                    "source": "feat/login",
                    "target": "main",
                    "strategy": "merge-no-ff",
                }),
            ))
            .unwrap();

        let output =
            execute_merge_approve(&store, merge_id, SafeMode::Observe, true, Some(&audit)).unwrap();
        assert!(output.contains("blocked by policy"));
        assert!(!output.contains("requires a persistent store"));

        let merge_events = store
            .query(&EventQuery::by_entity(
                EntityType::Merge,
                merge_id.to_string(),
            ))
            .unwrap();
        assert!(!merge_events
            .iter()
            .any(|event| event.event_type == "merge.approved"));
        assert!(merge_events
            .iter()
            .any(|event| event.event_type == "merge.policy_blocked"));

        let policy_event = store
            .query(&EventQuery::by_correlation(corr))
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == "policy.decision")
            .expect("expected policy decision event");
        assert_eq!(policy_event.payload["outcome"].as_str(), Some("DENY"));
        let expected_policy_key = format!("{merge_id}:merge.approve:policy.decision");
        assert_eq!(
            policy_event.idempotency_key.as_deref(),
            Some(expected_policy_key.as_str())
        );
        let blocked_event = merge_events
            .iter()
            .find(|event| event.event_type == "merge.policy_blocked")
            .expect("expected merge.policy_blocked event");
        let expected_blocked_key = format!("{merge_id}:merge.approve:merge.policy_blocked");
        assert_eq!(
            blocked_event.idempotency_key.as_deref(),
            Some(expected_blocked_key.as_str())
        );

        let audit_entries = audit.read_all().unwrap();
        assert!(audit_entries
            .iter()
            .any(|entry| entry.category == AuditCategory::DestructiveAttempt));
    }

    #[test]
    fn execute_merge_reject_persists_rejected_transition() {
        let store = InMemoryEventStore::new();
        let merge_id = Uuid::now_v7();
        let (_temp_dir, run_id, corr, source_branch, target_branch, binding) =
            create_merge_fixture(true);

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Worktree,
                binding.id.to_string(),
                "worktree.conflict_detected",
                corr,
                seed_worktree_event_payload(
                    &binding,
                    "WtMerging",
                    "WtConflict",
                    "interrupted merge pending rejection",
                ),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Merge,
                merge_id.to_string(),
                "merge.requested",
                corr,
                serde_json::json!({
                    "to": "MergeRequested",
                    "state": "MergeRequested",
                    "run_id": run_id,
                    "worktree_id": binding.id,
                    "source": source_branch,
                    "target": target_branch,
                    "strategy": "merge-no-ff",
                    "reason": "pending approval",
                }),
            ))
            .unwrap();

        let output = execute_merge_reject(
            &store,
            merge_id,
            "manual rejection",
            SafeMode::Execute,
            true,
            None,
        )
        .unwrap();
        assert!(output.contains("MergeRequested -> MergeAborted"));
        assert!(!output.contains("requires a persistent store"));

        let merge_events = store
            .query(&EventQuery::by_entity(
                EntityType::Merge,
                merge_id.to_string(),
            ))
            .unwrap();
        let rejected = merge_events
            .iter()
            .find(|event| event.event_type == "merge.rejected")
            .expect("expected merge.rejected event");
        assert_eq!(rejected.payload["to"], "MergeAborted");
        assert_eq!(rejected.payload["reason"], "manual rejection");
        let expected_rejected_key = format!("{merge_id}:merge.reject:merge.rejected");
        assert_eq!(
            rejected.idempotency_key.as_deref(),
            Some(expected_rejected_key.as_str())
        );
        let execution = merge_events
            .iter()
            .find(|event| event.event_type == "merge.execution_succeeded")
            .expect("expected merge.execution_succeeded event");
        assert_eq!(execution.payload["state"], "MergeAborted");

        let (merge_head_ok, _stdout, _stderr) = run_git(
            &binding.worktree_path,
            &["rev-parse", "--verify", "MERGE_HEAD"],
        );
        assert!(
            !merge_head_ok,
            "MERGE_HEAD should be cleared after merge.reject"
        );

        let policy_event = store
            .query(&EventQuery::by_correlation(corr))
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == "policy.decision")
            .expect("expected policy decision event");
        let expected_policy_key = format!("{merge_id}:merge.reject:policy.decision");
        assert_eq!(
            policy_event.idempotency_key.as_deref(),
            Some(expected_policy_key.as_str())
        );
    }

    // ── audit tail ───────────────────────────────────────────────────

    #[test]
    fn cmd_audit_tail_nonexistent_file() {
        let result = cmd_audit_tail("/tmp/nonexistent_yarli_audit.jsonl", 20, None);
        assert!(result.is_ok()); // should gracefully report no file
    }

    #[test]
    fn cmd_audit_tail_empty_file() {
        let f = NamedTempFile::new().unwrap();
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 20, None);
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_tail_reads_entries() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        // Write some entries.
        let entry1 = AuditEntry::destructive_attempt(
            "scheduler",
            "force_push",
            "blocked by policy",
            Some(Uuid::nil()),
            None,
            serde_json::json!({}),
        );
        let entry2 = AuditEntry::gate_evaluation(
            "tests_passed",
            true,
            "all tests green",
            Uuid::nil(),
            Some(Uuid::nil()),
        );
        sink.append(&entry1).unwrap();
        sink.append(&entry2).unwrap();

        let result = cmd_audit_tail(f.path().to_str().unwrap(), 20, None);
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_tail_limits_output() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        for i in 0..5 {
            let entry =
                AuditEntry::gate_evaluation(format!("gate_{i}"), true, "ok", Uuid::nil(), None);
            sink.append(&entry).unwrap();
        }

        // Request only 2 lines — should not error.
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 2, None);
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_tail_filters_by_category() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        let entry1 = AuditEntry::destructive_attempt(
            "scheduler",
            "force_push",
            "blocked",
            None,
            None,
            serde_json::json!({}),
        );
        let entry2 = AuditEntry::gate_evaluation("tests_passed", true, "ok", Uuid::nil(), None);
        sink.append(&entry1).unwrap();
        sink.append(&entry2).unwrap();

        // Filter by GateEvaluation.
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 20, Some("GateEvaluation"));
        assert!(result.is_ok());
    }

    #[test]
    fn cmd_audit_tail_all_entries_with_zero_limit() {
        let f = NamedTempFile::new().unwrap();
        let sink = JsonlAuditSink::new(f.path());

        for i in 0..3 {
            let entry =
                AuditEntry::gate_evaluation(format!("gate_{i}"), true, "ok", Uuid::nil(), None);
            sink.append(&entry).unwrap();
        }

        // 0 = show all.
        let result = cmd_audit_tail(f.path().to_str().unwrap(), 0, None);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn cancel_active_run_persists_cancelled_transitions() {
        let store = Arc::new(InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
        let runner = Arc::new(LocalCommandRunner::new());
        let scheduler = Scheduler::new(queue, store.clone(), runner, SchedulerConfig::default());

        let run = Run::new("cancel me", yarli_core::domain::SafeMode::Observe);
        let run_id = run.id;
        let corr = run.correlation_id;
        let task = Task::new(run_id, "task-1", "sleep 60", CommandClass::Io, corr);
        let task_id = task.id;

        scheduler.submit_run(run, vec![task]).await.unwrap();

        let cancelled = cancel_active_run(
            &scheduler,
            &store,
            run_id,
            "cancelled by operator interrupt",
            CancellationSource::Operator,
            Some(default_cancellation_provenance(
                CancellationSource::Operator,
                Some("operator interrupt".to_string()),
            )),
        )
        .await
        .unwrap();
        assert!(cancelled);

        let reg = scheduler.registry().read().await;
        assert_eq!(
            reg.get_task(&task_id).unwrap().state,
            TaskState::TaskCancelled
        );
        assert_eq!(reg.get_run(&run_id).unwrap().state, RunState::RunCancelled);
        drop(reg);

        let task_events = store
            .query(&EventQuery::by_entity(
                EntityType::Task,
                task_id.to_string(),
            ))
            .unwrap();
        assert!(task_events
            .iter()
            .any(|event| event.event_type == "task.cancelled"));

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert!(run_events
            .iter()
            .any(|event| event.event_type == "run.cancelled"));
    }

    async fn setup_drive_scheduler_fixture(
        command: &str,
    ) -> (
        Scheduler<InMemoryTaskQueue, InMemoryEventStore, LocalCommandRunner>,
        Arc<InMemoryEventStore>,
        Uuid,
        Uuid,
        Vec<(Uuid, String)>,
    ) {
        let store = Arc::new(InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
        let runner = Arc::new(LocalCommandRunner::new());
        let scheduler = Scheduler::new(queue, store.clone(), runner, SchedulerConfig::default());

        let run = Run::new("drive cancel", yarli_core::domain::SafeMode::Execute);
        let run_id = run.id;
        let correlation_id = run.correlation_id;
        let task = Task::new(run_id, "task-1", command, CommandClass::Io, correlation_id);
        let task_names = vec![(task.id, "task-1".to_string())];
        scheduler.submit_run(run, vec![task]).await.unwrap();

        (scheduler, store, run_id, correlation_id, task_names)
    }

    #[tokio::test]
    async fn drive_scheduler_captures_sigterm_cancellation_source() {
        let (scheduler, store, run_id, correlation_id, task_names) =
            setup_drive_scheduler_fixture("sleep 60").await;
        let shutdown = ShutdownController::new();
        let cancel = shutdown.token();
        let (tx, _rx) = mpsc::unbounded_channel();

        let shutdown_signal = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            shutdown_signal.request_graceful_with_source(CancellationSource::Sigterm);
        });

        let payload = tokio::time::timeout(
            Duration::from_secs(5),
            drive_scheduler(
                &scheduler,
                &store,
                shutdown,
                cancel,
                tx,
                run_id,
                correlation_id,
                &task_names,
                AutoAdvancePolicy::StableOk,
                false,
                None,
            ),
        )
        .await
        .expect("drive_scheduler timed out")
        .unwrap();

        assert_eq!(payload.exit_state, RunState::RunCancelled);
        assert_eq!(
            payload.cancellation_source,
            Some(CancellationSource::Sigterm)
        );
        assert_eq!(
            payload
                .cancellation_provenance
                .as_ref()
                .and_then(|prov| prov.signal_name.as_deref()),
            Some("SIGTERM")
        );

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        let cancelled = run_events
            .iter()
            .find(|event| {
                event.event_type == "run.cancelled"
                    && event
                        .payload
                        .get("cancellation_source")
                        .and_then(|value| value.as_str())
                        == Some("sigterm")
            })
            .expect("expected run.cancelled with sigterm source");
        assert_eq!(
            cancelled
                .payload
                .get("reason")
                .and_then(|value| value.as_str()),
            Some("cancelled by SIGTERM")
        );
    }

    #[tokio::test]
    async fn drive_scheduler_captures_operator_cancellation_source() {
        let store = Arc::new(InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
        let runner = Arc::new(LocalCommandRunner::new());
        let scheduler = Scheduler::new(queue, store.clone(), runner, SchedulerConfig::default());
        let run = Run::new(
            "drive operator cancel",
            yarli_core::domain::SafeMode::Execute,
        );
        let run_id = run.id;
        let correlation_id = run.correlation_id;
        let task_1 = Task::new(
            run_id,
            "task-1",
            "sleep 1",
            CommandClass::Io,
            correlation_id,
        );
        let mut task_2 = Task::new(
            run_id,
            "task-2",
            "echo done",
            CommandClass::Io,
            correlation_id,
        );
        task_2.depends_on(task_1.id);
        let task_names = vec![
            (task_1.id, "task-1".to_string()),
            (task_2.id, "task-2".to_string()),
        ];
        scheduler
            .submit_run(run, vec![task_1, task_2])
            .await
            .unwrap();
        let shutdown = ShutdownController::new();
        let cancel = shutdown.token();
        let (tx, _rx) = mpsc::unbounded_channel();

        let store_for_signal = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            store_for_signal
                .append(Event {
                    event_id: Uuid::now_v7(),
                    occurred_at: Utc::now(),
                    entity_type: EntityType::Run,
                    entity_id: run_id.to_string(),
                    event_type: "run.cancelled".to_string(),
                    payload: serde_json::json!({
                        "reason": "operator stop",
                    }),
                    correlation_id,
                    causation_id: None,
                    actor: OPERATOR_CONTROL_ACTOR.to_string(),
                    idempotency_key: None,
                })
                .unwrap();
        });

        let payload = tokio::time::timeout(
            Duration::from_secs(8),
            drive_scheduler(
                &scheduler,
                &store,
                shutdown,
                cancel,
                tx,
                run_id,
                correlation_id,
                &task_names,
                AutoAdvancePolicy::StableOk,
                false,
                None,
            ),
        )
        .await
        .expect("drive_scheduler timed out")
        .unwrap();

        assert_eq!(payload.exit_state, RunState::RunCancelled);
        assert_eq!(
            payload.cancellation_source,
            Some(CancellationSource::Operator)
        );
        assert_eq!(
            payload
                .cancellation_provenance
                .as_ref()
                .and_then(|prov| prov.actor_kind),
            Some(CancellationActorKind::Operator)
        );

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        let cancelled = run_events
            .iter()
            .find(|event| {
                event.event_type == "run.cancelled"
                    && event
                        .payload
                        .get("cancellation_source")
                        .and_then(|value| value.as_str())
                        == Some("operator")
            })
            .expect("expected run.cancelled with operator source");
        assert_eq!(
            cancelled
                .payload
                .get("reason")
                .and_then(|value| value.as_str()),
            Some("operator stop")
        );
    }

    #[tokio::test]
    async fn drive_scheduler_captures_sw4rm_preemption_cancellation_source() {
        let (scheduler, store, run_id, correlation_id, task_names) =
            setup_drive_scheduler_fixture("sleep 60").await;
        let shutdown = ShutdownController::new();
        let cancel = shutdown.token();
        let (tx, _rx) = mpsc::unbounded_channel();

        let shutdown_signal = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            shutdown_signal.request_graceful_with_source(CancellationSource::Sw4rmPreemption);
        });

        let payload = tokio::time::timeout(
            Duration::from_secs(5),
            drive_scheduler(
                &scheduler,
                &store,
                shutdown,
                cancel,
                tx,
                run_id,
                correlation_id,
                &task_names,
                AutoAdvancePolicy::StableOk,
                false,
                None,
            ),
        )
        .await
        .expect("drive_scheduler timed out")
        .unwrap();

        assert_eq!(payload.exit_state, RunState::RunCancelled);
        assert_eq!(
            payload.cancellation_source,
            Some(CancellationSource::Sw4rmPreemption)
        );
        assert_eq!(
            payload
                .cancellation_provenance
                .as_ref()
                .and_then(|prov| prov.actor_kind),
            Some(CancellationActorKind::Supervisor)
        );

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        let cancelled = run_events
            .iter()
            .find(|event| {
                event.event_type == "run.cancelled"
                    && event
                        .payload
                        .get("cancellation_source")
                        .and_then(|value| value.as_str())
                        == Some("sw4rm_preemption")
            })
            .expect("expected run.cancelled with sw4rm source");
        assert_eq!(
            cancelled
                .payload
                .get("reason")
                .and_then(|value| value.as_str()),
            Some("cancelled by sw4rm preemption")
        );
    }

    #[test]
    fn operator_control_signal_ignores_non_operator_actor() {
        let run_id = Uuid::now_v7();
        let event = Event {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_type: EntityType::Run,
            entity_id: run_id.to_string(),
            event_type: "run.blocked".to_string(),
            payload: serde_json::json!({
                "reason": "observer note"
            }),
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            actor: "observer.progress".to_string(),
            idempotency_key: None,
        };
        assert!(operator_control_signal_from_event(&event, run_id).is_none());
    }

    #[test]
    fn execute_run_pause_and_resume_controls_append_operator_transitions() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();

        let pause = execute_run_pause_control(&store, run_id, "maintenance window").unwrap();
        assert!(pause.contains("RunBlocked"));
        let paused = load_run_projection(&store, run_id).unwrap().unwrap();
        assert_eq!(paused.state, RunState::RunBlocked);

        let resume = execute_run_resume_control(&store, run_id, "maintenance complete").unwrap();
        assert!(resume.contains("RunActive"));
        let resumed = load_run_projection(&store, run_id).unwrap().unwrap();
        assert_eq!(resumed.state, RunState::RunActive);

        let run_events = store
            .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
            .unwrap();
        assert!(run_events.iter().any(|event| {
            event.event_type == "run.blocked"
                && event.actor == OPERATOR_CONTROL_ACTOR
                && event.payload.get("reason").and_then(|v| v.as_str())
                    == Some("maintenance window")
        }));
        assert!(run_events.iter().any(|event| {
            event.event_type == "run.activated"
                && event.actor == OPERATOR_CONTROL_ACTOR
                && event.payload.get("reason").and_then(|v| v.as_str())
                    == Some("maintenance complete")
        }));
    }

    #[test]
    fn execute_run_cancel_control_cancels_non_terminal_tasks_and_queue_entries() {
        let store = InMemoryEventStore::new();
        let queue = InMemoryTaskQueue::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();
        let active_task_id = Uuid::now_v7();
        let completed_task_id = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                active_task_id.to_string(),
                "task.ready",
                corr,
                serde_json::json!({ "from": "TaskOpen", "to": "TaskReady", "attempt_no": 1 }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                completed_task_id.to_string(),
                "task.completed",
                corr,
                serde_json::json!({ "from": "TaskVerifying", "to": "TaskComplete", "attempt_no": 1 }),
            ))
            .unwrap();

        queue
            .enqueue(active_task_id, run_id, 1, CommandClass::Io, None)
            .unwrap();

        let output =
            execute_run_cancel_control(&store, &queue, run_id, "operator stop", false).unwrap();
        assert!(output.contains("RunCancelled"));
        assert!(output.contains("cancelled 1 task(s)"));
        assert!(output.contains("drained 1 queue entry(ies)"));

        let run = load_run_projection(&store, run_id).unwrap().unwrap();
        assert_eq!(run.state, RunState::RunCancelled);

        let cancelled_task = load_task_projection(&store, active_task_id)
            .unwrap()
            .unwrap();
        assert_eq!(cancelled_task.state, TaskState::TaskCancelled);
        let completed_task = load_task_projection(&store, completed_task_id)
            .unwrap()
            .unwrap();
        assert_eq!(completed_task.state, TaskState::TaskComplete);

        let stats = queue.stats();
        assert_eq!(stats.cancelled, 1);
    }

    #[test]
    fn render_run_status_surfaces_budget_exceeded_and_token_usage() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    },
                    "command_resource_usage": {
                        "max_rss_bytes": 1048576,
                        "cpu_user_ticks": 100,
                        "cpu_system_ticks": 50,
                        "io_read_bytes": 4096,
                        "io_write_bytes": 2048
                    }
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("budget_exceeded"),
            "status output must surface budget_exceeded: {output}"
        );
        assert!(
            output.contains("prompt_tokens=2500"),
            "status output must surface prompt_tokens: {output}"
        );
        assert!(
            output.contains("completion_tokens=2500"),
            "status output must surface completion_tokens: {output}"
        );
        assert!(
            output.contains("total_tokens=5000"),
            "status output must surface total_tokens: {output}"
        );
        assert!(
            output.contains("max_rss_bytes=1048576"),
            "status output must surface resource_usage: {output}"
        );
    }

    #[test]
    fn render_run_explain_surfaces_budget_breach() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    }
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(
            output.contains("Budget breaches:"),
            "explain output must show budget breaches section: {output}"
        );
        assert!(
            output.contains("budget_exceeded") || output.contains("max_task_total_tokens"),
            "explain output must reference budget breach detail: {output}"
        );
    }

    #[test]
    fn render_run_status_surfaces_deterioration_report() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.deterioration",
                corr,
                serde_json::json!({
                    "score": 72.5,
                    "window_size": 32,
                    "factors": [
                        {
                            "name": "runtime_drift",
                            "impact": 0.8,
                            "detail": "runtime trend for repeated command keys"
                        }
                    ],
                    "trend": "deteriorating"
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("Deterioration: score=72.5"));
        assert!(output.contains("runtime_drift"));
        assert!(output.contains("Deteriorating"));
    }

    #[test]
    fn render_run_status_surfaces_memory_hints() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.memory_hints",
                corr,
                serde_json::json!({
                    "query_text": "objective: verify",
                    "limit": 8,
                    "results": [
                        {
                            "memory_id": "m1",
                            "scope_id": "project/test",
                            "memory_class": "semantic",
                            "relevance_score": 0.9,
                            "content_snippet": "remember to run migrations",
                            "metadata": { "fingerprint": "abc" }
                        }
                    ]
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.observer.memory_hints",
                corr,
                serde_json::json!({
                    "query_text": "failed reason=budget_exceeded",
                    "limit": 8,
                    "results": [
                        {
                            "memory_id": "m2",
                            "scope_id": "project/test",
                            "memory_class": "semantic",
                            "relevance_score": 0.2,
                            "content_snippet": "previous budget exceeded fix: lower parallelism",
                            "metadata": {}
                        }
                    ]
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(output.contains("Memories: 1 hint(s)"));
        assert!(output.contains("remember to run migrations"));
        assert!(output.contains("memory_hints: 1"));
    }

    #[test]
    fn render_run_explain_surfaces_deterioration_report() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({ "from": "RunOpen", "to": "RunActive" }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.observer.deterioration",
                corr,
                serde_json::json!({
                    "score": 58.0,
                    "window_size": 20,
                    "factors": [
                        {
                            "name": "failure_rate_drift",
                            "impact": 0.6,
                            "detail": "failure bucket rate drift"
                        }
                    ],
                    "trend": "deteriorating"
                }),
            ))
            .unwrap();

        let output = render_run_explain(&store, run_id).unwrap();
        assert!(output.contains("Sequence deterioration: score=58.0"));
        assert!(output.contains("failure_rate_drift"));
        assert!(output.contains("Deteriorating"));
    }

    #[test]
    fn render_task_explain_surfaces_budget_breach_and_token_usage() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "budget_exceeded",
                    "detail": "task max_task_total_tokens observed=5000 limit=1",
                    "command_token_usage": {
                        "prompt_tokens": 2500,
                        "completion_tokens": 2500,
                        "total_tokens": 5000,
                        "source": "char_count_div4_estimate_v1"
                    },
                    "command_resource_usage": {
                        "max_rss_bytes": 1048576
                    }
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(
            output.contains("budget_exceeded"),
            "task explain must surface budget_exceeded: {output}"
        );
        assert!(
            output.contains("prompt_tokens=2500"),
            "task explain must surface prompt_tokens: {output}"
        );
        assert!(
            output.contains("completion_tokens=2500"),
            "task explain must surface completion_tokens: {output}"
        );
        assert!(
            output.contains("total_tokens=5000"),
            "task explain must surface total_tokens: {output}"
        );
        assert!(
            output.contains("max_rss_bytes=1048576"),
            "task explain must surface resource_usage: {output}"
        );
    }

    #[test]
    fn task_annotate_persists_and_displays_in_explain() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        // Create a task event first.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.started",
                corr,
                serde_json::json!({"from": "TaskOpen", "to": "TaskReady"}),
            ))
            .unwrap();

        // Annotate the task.
        let result = execute_task_annotate(&store, task_id, "see blocker-001.md").unwrap();
        assert!(
            result.contains("blocker-001.md"),
            "annotate result must contain detail: {result}"
        );

        // Verify explain output includes the annotation.
        let explain_output = render_task_explain(&store, task_id).unwrap();
        assert!(
            explain_output.contains("blocker-001.md"),
            "explain must show blocker detail: {explain_output}"
        );
    }

    #[test]
    fn task_annotate_nonexistent_task_returns_not_found() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let result = execute_task_annotate(&store, task_id, "detail").unwrap();
        assert!(
            result.contains("not found"),
            "should report not found: {result}"
        );
    }

    #[test]
    fn run_status_displays_blocker_detail() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        // Create run event.
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.started",
                corr,
                serde_json::json!({"from": "RunOpen", "to": "RunActive"}),
            ))
            .unwrap();

        // Create task event linked by correlation.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.started",
                corr,
                serde_json::json!({"from": "TaskOpen", "to": "TaskReady"}),
            ))
            .unwrap();

        // Annotate task.
        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.annotated",
                corr,
                serde_json::json!({"blocker_detail": "see blocker-002.md"}),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("blocker-002.md"),
            "run status must show blocker detail: {output}"
        );
    }

    #[test]
    fn task_explain_displays_last_error() {
        let store = InMemoryEventStore::new();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "nonzero_exit",
                    "detail": "command exited with code 1"
                }),
            ))
            .unwrap();

        let output = render_task_explain(&store, task_id).unwrap();
        assert!(
            output.contains("Last error: command exited with code 1"),
            "explain must show last_error: {output}"
        );
    }

    #[test]
    fn run_status_displays_last_error() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::now_v7();
        let task_id = Uuid::now_v7();
        let corr = Uuid::now_v7();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.started",
                corr,
                serde_json::json!({"from": "RunOpen", "to": "RunActive"}),
            ))
            .unwrap();

        store
            .append(make_event(
                EntityType::Task,
                task_id.to_string(),
                "task.failed",
                corr,
                serde_json::json!({
                    "from": "TaskExecuting",
                    "to": "TaskFailed",
                    "reason": "nonzero_exit",
                    "detail": "command exited with code 42"
                }),
            ))
            .unwrap();

        let output = render_run_status(&store, run_id).unwrap();
        assert!(
            output.contains("last_error: command exited with code 42"),
            "run status must show last_error: {output}"
        );
    }

    #[test]
    fn run_status_and_explain_surface_cancel_provenance_summary() {
        let store = InMemoryEventStore::new();
        let run_id = Uuid::new_v4();
        let corr = Uuid::new_v4();

        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.activated",
                corr,
                serde_json::json!({
                    "from": "RunOpen",
                    "to": "RunActive",
                    "reason": "scheduler start"
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                run_id.to_string(),
                "run.cancelled",
                corr,
                serde_json::json!({
                    "from": "RunActive",
                    "to": "RunCancelled",
                    "reason": "cancelled by SIGTERM",
                    "exit_reason": "cancelled_by_operator",
                    "cancellation_source": "sigterm",
                    "cancellation_provenance": {
                        "cancellation_source": "sigterm",
                        "signal_name": "SIGTERM",
                        "signal_number": 15,
                        "sender_pid": null,
                        "receiver_pid": 4321,
                        "parent_pid": 1,
                        "process_group_id": 4321,
                        "session_id": 4321,
                        "tty": null,
                        "actor_kind": "supervisor",
                        "actor_detail": "SIGTERM likely originated from a supervisor",
                        "stage": "executing"
                    }
                }),
            ))
            .unwrap();

        let status = render_run_status(&store, run_id).unwrap();
        assert!(
            status.contains("Cancellation source: sigterm"),
            "status must show cancellation source: {status}"
        );
        assert!(
            status.contains(
                "Cancellation provenance: signal=SIGTERM sender=unknown receiver=yarli(4321) actor=supervisor stage=executing"
            ),
            "status must show provenance summary: {status}"
        );

        let explain = render_run_explain(&store, run_id).unwrap();
        assert!(
            explain.contains(
                "Cancellation provenance: signal=SIGTERM sender=unknown receiver=yarli(4321) actor=supervisor stage=executing"
            ),
            "explain must show provenance summary: {explain}"
        );
    }

    // ── Continuation payload tests ──

    fn sample_continuation_payload(
        run_id: Uuid,
        objective: &str,
    ) -> yarli_core::entities::ContinuationPayload {
        use yarli_core::entities::continuation::{ContinuationPayload, RunSummary};

        ContinuationPayload {
            run_id,
            objective: objective.to_string(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: Vec::new(),
            summary: RunSummary {
                total: 0,
                completed: 0,
                failed: 0,
                cancelled: 0,
                pending: 0,
            },
            next_tranche: None,
            quality_gate: None,
        }
    }

    #[test]
    fn read_continuation_payload_from_file_if_exists_returns_none_for_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("missing-continuation.json");
        let payload =
            persistence::read_continuation_payload_from_file_if_exists(&file_path).unwrap();
        assert!(payload.is_none());
    }

    #[test]
    fn read_continuation_payload_from_file_if_exists_reads_payload() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("continuation.json");
        let payload = sample_continuation_payload(Uuid::new_v4(), "from-file");
        let json = serde_json::to_string_pretty(&payload).unwrap();
        std::fs::write(&file_path, json).unwrap();

        let loaded = persistence::read_continuation_payload_from_file_if_exists(&file_path)
            .unwrap()
            .expect("expected payload");
        assert_eq!(loaded.run_id, payload.run_id);
        assert_eq!(loaded.objective, payload.objective);
    }

    #[test]
    fn load_latest_continuation_payload_prefers_most_recent_event() {
        let store = InMemoryEventStore::new();
        let corr = Uuid::new_v4();
        let older = sample_continuation_payload(Uuid::new_v4(), "older");
        let newer = sample_continuation_payload(Uuid::new_v4(), "newer");

        store
            .append(make_event(
                EntityType::Run,
                older.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": older,
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                newer.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": newer.clone(),
                }),
            ))
            .unwrap();

        let loaded = persistence::load_latest_continuation_payload_from_store(&store)
            .unwrap()
            .expect("expected continuation payload");
        assert_eq!(loaded.run_id, newer.run_id);
        assert_eq!(loaded.objective, "newer");
    }

    #[test]
    fn load_latest_continuation_payload_skips_malformed_latest_event() {
        let store = InMemoryEventStore::new();
        let corr = Uuid::new_v4();
        let valid = sample_continuation_payload(Uuid::new_v4(), "valid");

        store
            .append(make_event(
                EntityType::Run,
                valid.run_id.to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": valid.clone(),
                }),
            ))
            .unwrap();
        store
            .append(make_event(
                EntityType::Run,
                Uuid::new_v4().to_string(),
                RUN_CONTINUATION_EVENT_TYPE,
                corr,
                serde_json::json!({
                    "continuation_payload": {
                        "run_id": "not-a-uuid"
                    },
                }),
            ))
            .unwrap();

        let loaded = persistence::load_latest_continuation_payload_from_store(&store)
            .unwrap()
            .expect("expected fallback continuation payload");
        assert_eq!(loaded.run_id, valid.run_id);
        assert_eq!(loaded.objective, "valid");
    }

    #[test]
    fn continuation_payload_round_trips_through_file() {
        use yarli_core::entities::continuation::{ContinuationPayload, RunSummary, TrancheSpec};

        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "build everything".into(),
            exit_state: RunState::RunFailed,
            exit_reason: Some(yarli_core::domain::ExitReason::FailedRuntimeError),
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: vec![
                yarli_core::entities::continuation::TaskOutcome {
                    task_id: Uuid::new_v4(),
                    task_key: "build".into(),
                    state: TaskState::TaskComplete,
                    attempt_no: 1,
                    last_error: None,
                    blocker: None,
                },
                yarli_core::entities::continuation::TaskOutcome {
                    task_id: Uuid::new_v4(),
                    task_key: "test".into(),
                    state: TaskState::TaskFailed,
                    attempt_no: 2,
                    last_error: Some("3 tests failed".into()),
                    blocker: None,
                },
            ],
            summary: RunSummary {
                total: 2,
                completed: 1,
                failed: 1,
                cancelled: 0,
                pending: 0,
            },
            next_tranche: Some(TrancheSpec {
                suggested_objective: "Retry failed tasks: test".into(),
                kind: yarli_core::entities::continuation::TrancheKind::RetryUnfinished,
                retry_task_keys: vec!["test".into()],
                unfinished_task_keys: vec![],
                planned_task_keys: vec![],
                planned_tranche_key: None,
                cursor: None,
                config_snapshot: serde_json::json!({"tasks": [{"task_key": "test", "command": "cargo test"}]}),
            }),
            quality_gate: None,
        };

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("continuation.json");
        let json = serde_json::to_string_pretty(&payload).unwrap();
        std::fs::write(&file_path, &json).unwrap();

        let read_back: ContinuationPayload =
            serde_json::from_str(&std::fs::read_to_string(&file_path).unwrap()).unwrap();

        assert_eq!(read_back.run_id, payload.run_id);
        assert_eq!(read_back.summary.failed, 1);
        assert_eq!(read_back.summary.completed, 1);
        let tranche = read_back.next_tranche.unwrap();
        assert_eq!(tranche.retry_task_keys, vec!["test"]);
    }

    #[test]
    fn continuation_no_tranche_when_all_complete() {
        use yarli_core::entities::continuation::ContinuationPayload;

        let run = Run::new("all done", SafeMode::Execute);
        let mut t1 = Task::new(
            run.id,
            "build",
            "cargo build",
            CommandClass::Io,
            run.correlation_id,
        );
        t1.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1]);
        assert!(payload.next_tranche.is_none());
    }

    #[test]
    fn continuation_tranche_includes_failed_and_unfinished() {
        use yarli_core::entities::continuation::ContinuationPayload;

        let run = Run::new("mixed", SafeMode::Execute);
        let mut t1 = Task::new(
            run.id,
            "lint",
            "cargo clippy",
            CommandClass::Io,
            run.correlation_id,
        );
        t1.state = TaskState::TaskFailed;
        let mut t2 = Task::new(
            run.id,
            "deploy",
            "deploy.sh",
            CommandClass::Io,
            run.correlation_id,
        );
        t2.state = TaskState::TaskOpen;
        let mut t3 = Task::new(
            run.id,
            "build",
            "cargo build",
            CommandClass::Io,
            run.correlation_id,
        );
        t3.state = TaskState::TaskComplete;

        let payload = ContinuationPayload::build(&run, &[&t1, &t2, &t3]);
        let tranche = payload.next_tranche.as_ref().unwrap();
        assert_eq!(tranche.retry_task_keys, vec!["lint"]);
        assert_eq!(tranche.unfinished_task_keys, vec!["deploy"]);
    }

    #[test]
    fn continuation_planned_next_resolves_command_from_task_catalog() {
        use yarli_core::entities::continuation::{TrancheKind, TrancheSpec};

        let loaded = write_test_config("");
        let prompt_entry_path = "/tmp/project/PROMPT.md";
        let tranche = TrancheSpec {
            suggested_objective: "planned-next".into(),
            kind: TrancheKind::PlannedNext,
            retry_task_keys: vec![],
            unfinished_task_keys: vec![],
            planned_task_keys: vec!["two_task".into(), "three_task".into()],
            planned_tranche_key: Some("two".into()),
            cursor: Some(yarli_core::entities::continuation::TrancheCursor {
                current_tranche_index: Some(0),
                next_tranche_index: Some(1),
            }),
            config_snapshot: serde_json::json!({
                "runtime": {
                    "working_dir": ".",
                    "timeout_secs": 300,
                    "tasks": [
                        {"task_key": "one_task", "command": "true", "command_class": "Io"}
                    ],
                    "task_catalog": [
                        {"task_key": "one_task", "command": "true", "command_class": "Io"},
                        {"task_key": "two_task", "command": "echo second", "command_class": "Io"},
                        {"task_key": "three_task", "command": "echo third", "command_class": "Io"}
                    ],
                    "tranche_plan": [
                        {"key": "one", "objective": "first", "task_keys": ["one_task"]},
                        {"key": "two", "objective": "second", "task_keys": ["two_task", "three_task"], "tranche_group": "core"}
                    ],
                    "current_tranche_index": 0,
                    "prompt": {
                        "entry_path": prompt_entry_path,
                        "expanded_sha256": "abc123",
                        "included_files": []
                    }
                }
            }),
        };

        let plan = build_plan_from_continuation_tranche(&tranche, &loaded).unwrap();
        assert_eq!(plan.tasks.len(), 2);
        assert_eq!(plan.tasks[0].task_key, "two_task");
        assert_eq!(plan.tasks[0].command, "echo second");
        assert_eq!(plan.tasks[1].task_key, "three_task");
        assert_eq!(plan.tasks[1].command, "echo third");
        assert_eq!(plan.tranche_plan[1].tranche_group.as_deref(), Some("core"));
        assert_eq!(plan.current_tranche_index, Some(1));
        assert_eq!(
            plan.prompt_snapshot
                .as_ref()
                .map(|snapshot| snapshot.entry_path.as_str()),
            Some(prompt_entry_path)
        );
    }

    #[test]
    fn compute_quality_gate_blocks_stable_when_policy_disabled() {
        let report = DeteriorationReport {
            score: 22.0,
            window_size: 8,
            factors: Vec::new(),
            trend: DeteriorationTrend::Stable,
        };

        let gate = compute_quality_gate(Some(&report), AutoAdvancePolicy::ImprovingOnly);
        assert!(!gate.allow_auto_advance);
        assert_eq!(
            gate.reason,
            "deterioration trend stable (stagnation blocked)"
        );
    }

    #[test]
    fn compute_quality_gate_allows_stable_when_policy_enabled() {
        let report = DeteriorationReport {
            score: 22.0,
            window_size: 8,
            factors: Vec::new(),
            trend: DeteriorationTrend::Stable,
        };

        let gate = compute_quality_gate(Some(&report), AutoAdvancePolicy::StableOk);
        assert!(gate.allow_auto_advance);
        assert_eq!(
            gate.reason,
            "deterioration trend stable (policy allows auto-advance)"
        );
    }

    #[test]
    fn auto_advance_requires_planned_next_and_allowed_quality_gate() {
        use yarli_core::entities::continuation::{
            ContinuationPayload, ContinuationQualityGate, RunSummary, TrancheKind, TrancheSpec,
        };

        let payload = ContinuationPayload {
            run_id: Uuid::new_v4(),
            objective: "x".into(),
            exit_state: RunState::RunCompleted,
            exit_reason: None,
            cancellation_source: None,
            cancellation_provenance: None,
            completed_at: Utc::now(),
            tasks: Vec::new(),
            summary: RunSummary {
                total: 1,
                completed: 1,
                failed: 0,
                cancelled: 0,
                pending: 0,
            },
            next_tranche: Some(TrancheSpec {
                suggested_objective: "next".into(),
                kind: TrancheKind::PlannedNext,
                retry_task_keys: Vec::new(),
                unfinished_task_keys: Vec::new(),
                planned_task_keys: vec!["test".into()],
                planned_tranche_key: Some("full".into()),
                cursor: None,
                config_snapshot: serde_json::json!({}),
            }),
            quality_gate: Some(ContinuationQualityGate {
                allow_auto_advance: true,
                reason: "improving".into(),
                trend: Some(DeteriorationTrend::Improving),
                score: Some(10.0),
            }),
        };

        let (allow, _) = should_auto_advance_planned_tranche(
            &payload,
            config::AutoAdvanceConfig {
                policy: AutoAdvancePolicy::ImprovingOnly,
                max_tranches: 0,
            },
            0,
        );
        assert!(allow);
    }
}
