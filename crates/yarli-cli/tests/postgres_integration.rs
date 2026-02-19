use std::env;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use sqlx::ConnectOptions;
use tempfile::TempDir;
use uuid::Uuid;
use yarli_store::{MIGRATION_0001_INIT, MIGRATION_0002_INDEXES};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::{sleep, Duration, Instant};

const TEST_DATABASE_URL_ENV: &str = "YARLI_TEST_DATABASE_URL";
const REQUIRE_POSTGRES_TESTS_ENV: &str = "YARLI_REQUIRE_POSTGRES_TESTS";
const TEST_DATABASE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const LOCAL_POSTGRES_BOOTSTRAP_HINT: &str =
    "docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16";

#[tokio::test]
async fn merge_request_and_status_roundtrip_against_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test(
        "merge_request_and_status_roundtrip_against_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;
    let run_id = seed_run_event(&database.database_url).await?;

    let temp_dir = TempDir::new()?;
    write_test_config(temp_dir.path(), &database.database_url)?;
    let binary = yarli_binary_path()?;

    let request_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "merge",
            "request",
            "feature/integration",
            "main",
            "--run-id",
            &run_id.to_string(),
            "--strategy",
            "merge-no-ff",
        ])
        .output()?;

    assert!(
        request_output.status.success(),
        "merge request command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&request_output.stdout),
        String::from_utf8_lossy(&request_output.stderr)
    );
    let request_stdout = String::from_utf8(request_output.stdout)?;
    let merge_id = parse_merge_id(&request_stdout).ok_or("missing merge id in CLI output")?;

    let status_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["merge", "status", &merge_id.to_string()])
        .output()?;

    assert!(
        status_output.status.success(),
        "merge status command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_stdout = String::from_utf8(status_output.stdout)?;
    assert!(status_stdout.contains(&format!("Merge intent {merge_id}")));
    assert!(status_stdout.contains("State: MergeRequested"));
    assert!(status_stdout.contains("Last event: merge.requested"));

    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn run_start_and_status_roundtrip_against_postgres() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(admin_database_url) = test_database_url_for_test(
        "run_start_and_status_roundtrip_against_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let temp_dir = TempDir::new()?;
    write_test_config(temp_dir.path(), &database.database_url)?;
    let binary = yarli_binary_path()?;

    let run_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "start",
            "postgres roundtrip",
            "--stream",
            "--cmd",
            "echo integration-ok",
        ])
        .output()?;

    assert!(
        run_output.status.success(),
        "run start command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );
    let run_stdout = String::from_utf8(run_output.stdout)?;
    let run_id = parse_run_id(&run_stdout).ok_or("missing run id in CLI output")?;

    let status_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "status", &run_id.to_string()])
        .output()?;
    assert!(
        status_output.status.success(),
        "run status command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_stdout = String::from_utf8(status_output.stdout)?;
    assert!(status_stdout.contains("State: RunCompleted"));
    assert!(status_stdout.contains("TaskComplete"));

    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn run_projection_state_consistency_roundtrip_against_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test(
        "run_projection_state_consistency_roundtrip_against_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let temp_dir = TempDir::new()?;
    write_test_config(temp_dir.path(), &database.database_url)?;
    let binary = yarli_binary_path()?;

    let pool = connect_postgres(
        &database.database_url,
        "run_projection_state_consistency_roundtrip",
    )
    .await?;

    let completed_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "start",
            "postgres completed run",
            "--stream",
            "--cmd",
            "echo integration-ok",
        ])
        .output()?;

    assert!(
        completed_output.status.success(),
        "completed run command failed\nstderr:\n{}",
        String::from_utf8_lossy(&completed_output.stderr)
    );
    let completed_stdout = String::from_utf8(completed_output.stdout)?;
    let completed_run_id = parse_run_id(&completed_stdout).ok_or("missing run id in completed run output")?;

    verify_projection_state_consistency(&pool, completed_run_id).await?;

    let failed_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "start",
            "postgres failed run",
            "--stream",
            "--cmd",
            "false",
        ])
        .output()?;

    if !failed_output.status.success() {
        eprintln!(
            "failed run command intentionally exited with status code {}",
            failed_output.status
        );
    }
    let failed_stdout = String::from_utf8(failed_output.stdout)?;
    let failed_run_id = parse_run_id(&failed_stdout)
        .ok_or("missing run id in failed run output")?;

    verify_projection_state_consistency(&pool, failed_run_id).await?;

    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn run_cancel_hardening_roundtrip_against_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test(
        "run_cancel_hardening_roundtrip_against_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let temp_dir = TempDir::new()?;
    write_test_config(temp_dir.path(), &database.database_url)?;
    let binary = yarli_binary_path()?;

    let pool = connect_postgres(&database.database_url, "run_cancel_hardening_roundtrip").await?;

    let start_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "start",
            "postgres cancellation hardening",
            "--stream",
            "--cmd",
            "sleep 30",
        ])
        .output()?;

    assert!(
        start_output.status.success(),
        "run start command failed before cancellation\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&start_output.stdout),
        String::from_utf8_lossy(&start_output.stderr)
    );
    let start_stdout = String::from_utf8(start_output.stdout)?;
    let run_id = parse_run_id(&start_stdout).ok_or("missing run id in cancellation hardening output")?;

    wait_for_run_state_in(
        &pool,
        run_id,
        &["RUN_ACTIVE", "RUN_VERIFYING"],
        Duration::from_secs(10),
    )
    .await?;

    let cancel_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "cancel",
            &run_id.to_string(),
            "--reason",
            "integration cancellation",
        ])
        .output()?;
    assert!(
        cancel_output.status.success(),
        "run cancel command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&cancel_output.stdout),
        String::from_utf8_lossy(&cancel_output.stderr)
    );

    wait_for_run_state(&pool, run_id, "RUN_CANCELLED", Duration::from_secs(15)).await?;
    let (state, exit_reason) = fetch_run_state(&pool, run_id).await?;
    assert_eq!(state, "RUN_CANCELLED");
    assert_eq!(exit_reason, Some("cancelled_by_operator".to_string()));

    let task_states = fetch_task_states(&pool, run_id).await?;
    assert!(!task_states.is_empty(), "expected tasks for cancelled run");
    assert!(task_states.iter().all(|state| state == "TASK_CANCELLED"));

    assert!(
        count_events(&pool, "run", "run.cancelled", run_id).await? >= 1,
        "expected at least one run.cancelled event"
    );
    assert!(
        count_events(&pool, "run", "run.cancel_provenance", run_id).await? >= 1,
        "expected at least one run.cancel_provenance event"
    );
    assert!(
        count_task_events_for_run(&pool, run_id, "task.cancelled").await? >= 1,
        "expected at least one task.cancelled event"
    );

    let status_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "status", &run_id.to_string()])
        .output()?;
    assert!(
        status_output.status.success(),
        "run status command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_stdout = String::from_utf8(status_output.stdout)?;
    assert!(status_stdout.contains("State: RunCancelled"));
    assert!(status_stdout.contains("Cancellation source: operator"));
    assert!(status_stdout.contains("Cancellation provenance:"));

    let explain_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "explain-exit", &run_id.to_string()])
        .output()?;
    assert!(
        explain_output.status.success(),
        "run explain-exit command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&explain_output.stdout),
        String::from_utf8_lossy(&explain_output.stderr)
    );
    let explain_stdout = String::from_utf8(explain_output.stdout)?;
    assert!(explain_stdout.contains("Exit reason: cancelled_by_operator"));
    assert!(explain_stdout.contains("Cancellation source: operator"));
    assert!(explain_stdout.contains("Cancellation provenance:"));

    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn run_cancel_load_hardening_roundtrip_against_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test(
        "run_cancel_load_hardening_roundtrip_against_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let temp_dir = TempDir::new()?;
    write_test_config(temp_dir.path(), &database.database_url)?;
    let binary = yarli_binary_path()?;

    let pool = connect_postgres(&database.database_url, "run_cancel_load_hardening_roundtrip").await?;

    let start_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "start",
            "postgres cancellation load hardening",
            "--stream",
            "--timeout",
            "20",
            "--cmd",
            "sleep 20",
            "--cmd",
            "sleep 20",
            "--cmd",
            "sleep 20",
            "--cmd",
            "sleep 20",
        ])
        .output()?;

    assert!(
        start_output.status.success(),
        "run start command failed before cancellation load\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&start_output.stdout),
        String::from_utf8_lossy(&start_output.stderr)
    );
    let start_stdout = String::from_utf8(start_output.stdout)?;
    let run_id = parse_run_id(&start_stdout).ok_or("missing run id in cancellation load hardening output")?;

    wait_for_run_state_in(
        &pool,
        run_id,
        &["RUN_ACTIVE", "RUN_VERIFYING"],
        Duration::from_secs(20),
    )
    .await?;
    wait_for_task_count(&pool, run_id, 4, Duration::from_secs(25)).await?;
    let task_states_before_cancel = fetch_task_states(&pool, run_id).await?;
    assert!(
        task_states_before_cancel.len() >= 4,
        "expected at least four tasks for cancel-load run"
    );

    let cancel_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "cancel",
            &run_id.to_string(),
            "--reason",
            "integration cancellation load",
        ])
        .output()?;
    assert!(
        cancel_output.status.success(),
        "run cancel command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&cancel_output.stdout),
        String::from_utf8_lossy(&cancel_output.stderr)
    );

    wait_for_run_state(&pool, run_id, "RUN_CANCELLED", Duration::from_secs(15)).await?;
    let (state, exit_reason) = fetch_run_state(&pool, run_id).await?;
    assert_eq!(state, "RUN_CANCELLED");
    assert_eq!(exit_reason, Some("cancelled_by_operator".to_string()));

    let task_states_after = fetch_task_states(&pool, run_id).await?;
    assert!(!task_states_after.is_empty(), "expected tasks for cancelled run");
    assert!(
        task_states_after
            .iter()
            .all(|state| state != "TASK_COMPLETE"),
        "expected load-run cancellation to prevent task completion",
    );

    assert!(
        count_events(&pool, "run", "run.cancelled", run_id).await? >= 1,
        "expected at least one run.cancelled event"
    );
    assert!(
        count_task_events_for_run(&pool, run_id, "task.cancelled").await? >= 1,
        "expected at least one task.cancelled event"
    );
    assert!(
        count_events(&pool, "run", "run.cancel_provenance", run_id).await? >= 1,
        "expected at least one run.cancel_provenance event"
    );

    let explain_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "explain-exit", &run_id.to_string()])
        .output()?;
    assert!(
        explain_output.status.success(),
        "run explain-exit command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&explain_output.stdout),
        String::from_utf8_lossy(&explain_output.stderr)
    );
    let explain_stdout = String::from_utf8(explain_output.stdout)?;
    assert!(explain_stdout.contains("Exit reason: cancelled_by_operator"));
    assert!(explain_stdout.contains("Cancellation source: operator"));
    assert!(explain_stdout.contains("Cancellation provenance:"));

    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn run_timeout_hardening_roundtrip_against_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test(
        "run_timeout_hardening_roundtrip_against_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let temp_dir = TempDir::new()?;
    write_test_config(temp_dir.path(), &database.database_url)?;
    let binary = yarli_binary_path()?;

    let pool = connect_postgres(&database.database_url, "run_timeout_hardening_roundtrip").await?;

    let start_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "start",
            "postgres timeout hardening",
            "--stream",
            "--timeout",
            "1",
            "--cmd",
            "sleep 10",
        ])
        .output()?;

    assert!(
        start_output.status.success(),
        "run start command failed before timeout validation\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&start_output.stdout),
        String::from_utf8_lossy(&start_output.stderr)
    );
    let start_stdout = String::from_utf8(start_output.stdout)?;
    let run_id = parse_run_id(&start_stdout).ok_or("missing run id in timeout hardening output")?;

    wait_for_run_state(&pool, run_id, "RUN_FAILED", Duration::from_secs(20)).await?;
    let (state, exit_reason) = fetch_run_state(&pool, run_id).await?;
    assert_eq!(state, "RUN_FAILED");
    assert_eq!(exit_reason, Some("timed_out".to_string()));

    let status_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "status", &run_id.to_string()])
        .output()?;
    assert!(
        status_output.status.success(),
        "run status command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    assert!(String::from_utf8(status_output.stdout)?.contains("State: RunFailed"));

    assert!(
        count_events_by_entity_type_for_run(&pool, run_id, "command", "command.timed_out").await? >= 1,
        "expected at least one command.timed_out event"
    );
    assert!(
        count_events(&pool, "run", "run.failed", run_id).await? >= 1,
        "expected at least one run.failed event"
    );

    let explain_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "explain-exit", &run_id.to_string()])
        .output()?;
    assert!(
        explain_output.status.success(),
        "run explain-exit command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&explain_output.stdout),
        String::from_utf8_lossy(&explain_output.stderr)
    );
    let explain_stdout = String::from_utf8(explain_output.stdout)?;
    assert!(explain_stdout.contains("Status: RunFailed"));
    assert!(explain_stdout.contains("Exit reason: timed_out"));

    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn overwatch_runner_hardening_roundtrip_against_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test(
        "overwatch_runner_hardening_roundtrip_against_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let (overwatch_url, mock_state, mock_task) =
        spawn_mock_overwatch_server(OverwatchTerminalState::Completed).await?;
    let temp_dir = TempDir::new()?;
    write_overwatch_test_config(temp_dir.path(), &database.database_url, &overwatch_url)?;
    let binary = yarli_binary_path()?;

    let pool = connect_postgres(&database.database_url, "overwatch_runner_hardening_roundtrip").await?;

    let run_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "start",
            "postgres overwatch hardening",
            "--stream",
            "--cmd",
            "echo overwatch smoke",
        ])
        .output()?;

    assert!(
        run_output.status.success(),
        "run start command failed with overwatch backend\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );
    let run_stdout = String::from_utf8(run_output.stdout)?;
    let run_id = parse_run_id(&run_stdout).ok_or("missing run id in overwatch hardening output")?;

    wait_for_run_state(&pool, run_id, "RUN_COMPLETED", Duration::from_secs(15)).await?;

    let task_states = fetch_task_states(&pool, run_id).await?;
    assert!(task_states.iter().all(|state| state == "TASK_COMPLETE"));

    let state = mock_state.lock().await;
    assert!(state.run_requests >= 1, "expected at least one /run call");
    assert!(
        state.status_requests >= 2,
        "expected status polling before run completion"
    );
    assert!(
        state.output_requests >= 1,
        "expected overwatch output request after completion"
    );
    assert_eq!(
        state.cancel_requests, 0,
        "did not expect /cancel for successful run"
    );

    let status_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "status", &run_id.to_string()])
        .output()?;
    assert!(
        status_output.status.success(),
        "run status command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    assert!(
        String::from_utf8(status_output.stdout)?
            .contains("State: RunCompleted"),
        "overwatch run should complete"
    );
    drop(state);

    mock_task.abort();
    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn overwatch_runner_failure_roundtrip_against_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test(
        "overwatch_runner_failure_roundtrip_against_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let (overwatch_url, mock_state, mock_task) =
        spawn_mock_overwatch_server(OverwatchTerminalState::Failed).await?;
    let temp_dir = TempDir::new()?;
    write_overwatch_test_config(temp_dir.path(), &database.database_url, &overwatch_url)?;
    let binary = yarli_binary_path()?;

    let pool = connect_postgres(
        &database.database_url,
        "overwatch_runner_failure_roundtrip",
    )
    .await?;

    let run_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args([
            "run",
            "start",
            "postgres overwatch failure hardening",
            "--stream",
            "--cmd",
            "echo overwatch failure",
        ])
        .output()?;

    assert!(
        run_output.status.success(),
        "run start command failed with failing overwatch backend\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );
    let run_stdout = String::from_utf8(run_output.stdout)?;
    let run_id = parse_run_id(&run_stdout).ok_or("missing run id in overwatch failure hardening output")?;

    wait_for_run_state(&pool, run_id, "RUN_FAILED", Duration::from_secs(20)).await?;

    let (state, exit_reason) = fetch_run_state(&pool, run_id).await?;
    assert_eq!(state, "RUN_FAILED");
    assert!(
        exit_reason.is_some(),
        "expected failed run to persist an exit reason"
    );

    assert!(
        count_events(&pool, "run", "run.failed", run_id).await? >= 1,
        "expected at least one run.failed event"
    );
    assert!(
        count_task_events_for_run(&pool, run_id, "task.failed").await? >= 1,
        "expected at least one task.failed event"
    );

    let status_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "status", &run_id.to_string()])
        .output()?;
    assert!(
        status_output.status.success(),
        "run status command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_stdout = String::from_utf8(status_output.stdout)?;
    assert!(status_stdout.contains("State: RunFailed"));

    let explain_output = Command::new(&binary)
        .current_dir(temp_dir.path())
        .args(["run", "explain-exit", &run_id.to_string()])
        .output()?;
    assert!(
        explain_output.status.success(),
        "run explain-exit command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&explain_output.stdout),
        String::from_utf8_lossy(&explain_output.stderr)
    );
    let explain_stdout = String::from_utf8(explain_output.stdout)?;
    assert!(explain_stdout.contains("Status: RunFailed"));
    assert!(explain_stdout.contains("Exit reason:"));

    let state = mock_state.lock().await;
    assert!(state.run_requests >= 1, "expected at least one /run call");
    assert!(state.status_requests >= 1, "expected status polling for failed run");
    assert_eq!(state.cancel_requests, 0, "did not expect /cancel for failed run");

    mock_task.abort();
    database.drop().await?;
    Ok(())
}

fn parse_merge_id(output: &str) -> Option<Uuid> {
    output.lines().find_map(|line| {
        line.strip_prefix("Merge ID: ")
            .and_then(|value| value.trim().parse::<Uuid>().ok())
    })
}

fn parse_run_id(output: &str) -> Option<Uuid> {
    output.lines().find_map(|line| {
        line.strip_prefix("Run ")
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|value| value.trim().parse::<Uuid>().ok())
    })
}

#[derive(Debug, Clone, Copy)]
enum OverwatchTerminalState {
    Completed,
    Failed,
}

#[derive(Debug, Default)]
struct OverwatchState {
    run_requests: u64,
    status_requests: u64,
    output_requests: u64,
    cancel_requests: u64,
    status_calls_by_task: HashMap<String, u64>,
}

async fn spawn_mock_overwatch_server(
    terminal_state: OverwatchTerminalState,
) -> Result<
    (
        String,
        Arc<TokioMutex<OverwatchState>>,
        tokio::task::JoinHandle<()>,
    ),
    Box<dyn std::error::Error>,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr = listener.local_addr()?;
    let state = Arc::new(TokioMutex::new(OverwatchState::default()));
    let serve_state = state.clone();

    let server_task = tokio::spawn(async move {
        loop {
            let accepted = listener.accept().await;
            let Ok((stream, _)) = accepted else {
                return;
            };

            let handler_state = serve_state.clone();
            tokio::spawn(async move {
                if let Err(err) = handle_overwatch_connection(stream, handler_state, terminal_state).await {
                    eprintln!("mock overwatch server connection error: {err}");
                }
            });
        }
    });

    Ok((format!("http://{listen_addr}"), state, server_task))
}

async fn handle_overwatch_connection(
    mut stream: tokio::net::TcpStream,
    state: Arc<TokioMutex<OverwatchState>>,
    terminal_state: OverwatchTerminalState,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let mut headers_end: Option<usize> = None;
    let mut content_length = 0usize;

    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..n]);

        if headers_end.is_none() {
            headers_end = find_headers_end(&buffer);
            if let Some(end) = headers_end {
                content_length = parse_content_length(&buffer[..end]);
            }
        }

        if let Some(headers_end) = headers_end {
            let body_len = buffer.len().saturating_sub(headers_end + 4);
            if body_len >= content_length {
                break;
            }
        }
    }

    let headers_end = headers_end.ok_or_else(|| {
        "mock overwatch request did not include complete HTTP headers".to_string()
    })?;
    let body = buffer
        .get(headers_end + 4..)
        .unwrap_or(&[])
        .to_vec();

    let header_text = String::from_utf8_lossy(&buffer[..headers_end]);
    let request_line = header_text.lines().next().unwrap_or("");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or("").to_uppercase();
    let path = request_parts.next().unwrap_or("/").to_string();

            match (method.as_str(), path.as_str()) {
        ("POST", "/run") => {
            let mut state_guard = state.lock().await;
            state_guard.run_requests = state_guard.run_requests.saturating_add(1);

            let payload: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            let task_id = payload
                .get("task_id")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
                .unwrap_or_else(|| Uuid::now_v7().to_string());
            state_guard.status_calls_by_task.entry(task_id.clone()).or_insert(0);
            send_json_response(
                &mut stream,
                200,
                &serde_json::json!({ "task_id": task_id }).to_string(),
            )
            .await?;
        }
        _ if method == "GET" && path.starts_with("/status/") => {
            let task_id = path.trim_start_matches("/status/").to_string();
            let mut state_guard = state.lock().await;
            state_guard.status_requests = state_guard.status_requests.saturating_add(1);
            let calls = state_guard
                .status_calls_by_task
                .entry(task_id.clone())
                .or_insert(0);
            *calls = calls.saturating_add(1);

            let body = match terminal_state {
                OverwatchTerminalState::Completed => {
                    if *calls <= 1 {
                        serde_json::json!({
                            "state": "running",
                            "terminal": false,
                            "runtime_sec": 0.01,
                        })
                    } else {
                        serde_json::json!({
                            "state": "completed",
                            "terminal": true,
                            "exit_code": 0,
                            "runtime_sec": 0.05,
                            "reason": "completed",
                        })
                    }
                }
                OverwatchTerminalState::Failed => {
                    if *calls <= 1 {
                        serde_json::json!({
                            "state": "running",
                            "terminal": false,
                            "runtime_sec": 0.01,
                        })
                    } else {
                        serde_json::json!({
                            "state": "failed",
                            "terminal": true,
                            "exit_code": 14,
                            "runtime_sec": 0.05,
                            "reason": "command failed",
                        })
                    }
                }
            };
            send_json_response(&mut stream, 200, &body.to_string()).await?;
        }
        _ if method == "GET" && path.starts_with("/output/") => {
            let _task_id = path.trim_start_matches("/output/");
            let mut state_guard = state.lock().await;
            state_guard.output_requests = state_guard.output_requests.saturating_add(1);

            send_json_response(
                &mut stream,
                200,
                &serde_json::json!({
                    "stdout_lines": ["mocked output"],
                    "stderr_lines": [],
                })
                .to_string(),
            )
            .await?;
        }
        _ if method == "POST" && path.starts_with("/cancel/") => {
            let _task_id = path.trim_start_matches("/cancel/");
            let mut state_guard = state.lock().await;
            state_guard.cancel_requests = state_guard.cancel_requests.saturating_add(1);
            send_json_response(&mut stream, 200, "{}").await?;
        }
        _ => {
            send_json_response(
                &mut stream,
                404,
                &serde_json::json!({ "error": "not_found" }).to_string(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn send_json_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    body: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        400 => "Bad Request",
        _ => "OK",
    };

    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}",
        status = status,
        status_text = status_text,
        length = body.len(),
        body = body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn parse_content_length(header_block: &[u8]) -> usize {
    String::from_utf8_lossy(header_block)
        .lines()
        .find_map(|line| {
            let mut parts = line.splitn(2, ':');
            let key = parts.next()?.trim().to_ascii_lowercase();
            let value = parts.next()?.trim();
            if key == "content-length" {
                value.parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0)
}

fn find_headers_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

async fn wait_for_run_state(
    pool: &PgPool,
    run_id: Uuid,
    expected: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_run_state_in(pool, run_id, &[expected], timeout).await
}

async fn wait_for_run_state_in(
    pool: &PgPool,
    run_id: Uuid,
    expected_states: &[&str],
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "run state wait timeout overflow".to_string())?;

    loop {
        let state: Option<String> = sqlx::query_scalar("SELECT state FROM runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(pool)
            .await?
            .map(|value| value);

        if let Some(state) = state {
            if expected_states.contains(&state.as_str()) {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for run {run_id} state in {expected_states:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn fetch_run_state(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    let row = sqlx::query("SELECT state, exit_reason FROM runs WHERE run_id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await?;
    let state: String = row.try_get("state")?;
    let exit_reason: Option<String> = row.try_get("exit_reason")?;
    Ok((state, exit_reason))
}

async fn fetch_task_states(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows = sqlx::query_scalar("SELECT state FROM tasks WHERE run_id = $1")
        .bind(run_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

async fn count_events(
    pool: &PgPool,
    entity_type: &str,
    event_type: &str,
    run_id: Uuid,
) -> Result<i64, Box<dyn std::error::Error>> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM events WHERE entity_type = $1 AND event_type = $2 AND entity_id = $3",
    )
    .bind(entity_type)
    .bind(event_type)
    .bind(run_id.to_string())
    .fetch_one(pool)
    .await?;
    Ok(count)
}

async fn count_task_events_for_run(
    pool: &PgPool,
    run_id: Uuid,
    event_type: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM events e JOIN tasks t ON e.entity_id = t.task_id::text WHERE t.run_id = $1 \
AND e.entity_type = 'task' AND e.event_type = $2",
    )
    .bind(run_id)
    .bind(event_type)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

async fn count_events_by_entity_type_for_run(
    pool: &PgPool,
    run_id: Uuid,
    entity_type: &str,
    event_type: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM events e JOIN commands c ON e.entity_id = c.command_id::text \
WHERE c.run_id = $1 AND e.entity_type = $2 AND e.event_type = $3",
    )
    .bind(run_id)
    .bind(entity_type)
    .bind(event_type)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

fn map_run_state_to_db(state: &str) -> Option<&'static str> {
    match state {
        "RunOpen" => Some("RUN_OPEN"),
        "RunActive" => Some("RUN_ACTIVE"),
        "RunVerifying" => Some("RUN_VERIFYING"),
        "RunBlocked" => Some("RUN_BLOCKED"),
        "RunFailed" => Some("RUN_FAILED"),
        "RunCompleted" => Some("RUN_COMPLETED"),
        "RunCancelled" => Some("RUN_CANCELLED"),
        _ => None,
    }
}

fn map_task_state_to_db(state: &str) -> Option<&'static str> {
    match state {
        "TaskOpen" => Some("TASK_OPEN"),
        "TaskReady" => Some("TASK_READY"),
        "TaskExecuting" => Some("TASK_EXECUTING"),
        "TaskWaiting" => Some("TASK_WAITING"),
        "TaskBlocked" => Some("TASK_BLOCKED"),
        "TaskVerifying" => Some("TASK_VERIFYING"),
        "TaskComplete" => Some("TASK_COMPLETE"),
        "TaskFailed" => Some("TASK_FAILED"),
        "TaskCancelled" => Some("TASK_CANCELLED"),
        _ => None,
    }
}

fn derive_run_state_from_event(event_type: &str, to_state: Option<&str>) -> Option<&'static str> {
    if let Some(value) = to_state.and_then(map_run_state_to_db) {
        return Some(value);
    }

    match event_type {
        "run.activated" => Some("RUN_ACTIVE"),
        "run.blocked" => Some("RUN_BLOCKED"),
        "run.verifying" => Some("RUN_VERIFYING"),
        "run.completed" => Some("RUN_COMPLETED"),
        "run.failed" | "run.gate_failed" => Some("RUN_FAILED"),
        "run.cancelled" => Some("RUN_CANCELLED"),
        _ => None,
    }
}

fn derive_task_state_from_event(event_type: &str, to_state: Option<&str>) -> Option<&'static str> {
    if let Some(value) = to_state.and_then(map_task_state_to_db) {
        return Some(value);
    }

    match event_type {
        "task.ready" | "task.retrying" | "task.unblocked" => Some("TASK_READY"),
        "task.executing" => Some("TASK_EXECUTING"),
        "task.waiting" => Some("TASK_WAITING"),
        "task.verifying" => Some("TASK_VERIFYING"),
        "task.completed" => Some("TASK_COMPLETE"),
        "task.failed" | "task.gate_failed" => Some("TASK_FAILED"),
        "task.blocked" => Some("TASK_BLOCKED"),
        "task.cancelled" => Some("TASK_CANCELLED"),
        _ => None,
    }
}

async fn expected_run_state_from_events(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<String, Box<dyn std::error::Error>> {
    let row = sqlx::query(
        "SELECT event_type, (payload->>'to') AS to_state FROM events WHERE entity_type='run' AND entity_id = $1 ORDER BY occurred_at DESC, event_id DESC LIMIT 1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?;

    let event_type: String = row.try_get("event_type")?;
    let to_state: Option<String> = row.try_get("to_state").ok();
    derive_run_state_from_event(&event_type, to_state.as_deref())
        .map(|state| state.to_string())
        .ok_or_else(|| "unable to derive run state from last event".into())
}

async fn expected_task_state_from_events(
    pool: &PgPool,
    task_id: Uuid,
) -> Result<String, Box<dyn std::error::Error>> {
    let row = sqlx::query(
        "SELECT event_type, (payload->>'to') AS to_state FROM events WHERE entity_type='task' AND entity_id = $1 ORDER BY occurred_at DESC, event_id DESC LIMIT 1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await?;

    let event_type: String = row.try_get("event_type")?;
    let to_state: Option<String> = row.try_get("to_state").ok();
    derive_task_state_from_event(&event_type, to_state.as_deref())
        .map(|state| state.to_string())
        .ok_or_else(|| "unable to derive task state from last event".into())
}

async fn verify_projection_state_consistency(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<(), Box<dyn std::error::Error>> {
    let materialized_run_state: String = sqlx::query_scalar(
        "SELECT state FROM runs WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?;
    let projected_run_state = expected_run_state_from_events(pool, run_id).await?;
    assert_eq!(
        materialized_run_state,
        projected_run_state,
        "run state should match event projection for run {run_id}",
    );

    let task_rows = sqlx::query("SELECT task_id, state FROM tasks WHERE run_id = $1")
        .bind(run_id)
        .fetch_all(pool)
        .await?;
    assert!(!task_rows.is_empty(), "expected at least one task for run {run_id}");

    for row in task_rows {
        let task_id: Uuid = row.try_get("task_id")?;
        let materialized_task_state: String = row.try_get("state")?;
        let projected_task_state = expected_task_state_from_events(pool, task_id).await?;
        assert_eq!(
            materialized_task_state,
            projected_task_state,
            "task state should match event projection for task {task_id}",
        );
    }

    Ok(())
}

async fn wait_for_task_count(
    pool: &PgPool,
    run_id: Uuid,
    min_count: usize,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "task count wait timeout overflow".to_string())?;

    loop {
        let task_states = fetch_task_states(pool, run_id).await?;
        let has_non_terminal = task_states
            .iter()
            .any(|state| state.as_str() != "TASK_COMPLETE");
        if task_states.len() >= min_count && has_non_terminal {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for at least {min_count} tasks on run {run_id}"
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn seed_run_event(database_url: &str) -> Result<Uuid, Box<dyn std::error::Error>> {
    let pool = connect_postgres(database_url, "seed_run_event").await?;

    let run_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();

    sqlx::query(
        r#"
        INSERT INTO events (
            event_id,
            occurred_at,
            entity_type,
            entity_id,
            event_type,
            payload,
            correlation_id,
            causation_id,
            actor,
            idempotency_key
        )
        VALUES ($1, $2, 'run', $3, 'run.activated', $4, $5, NULL, 'integration-test', $6)
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(Utc::now())
    .bind(run_id.to_string())
    .bind(serde_json::json!({
        "from": "RunOpen",
        "to": "RunActive",
    }))
    .bind(correlation_id)
    .bind(format!("cli-integration:run.activated:{run_id}"))
    .execute(&pool)
    .await?;

    Ok(run_id)
}

fn write_test_config(dir: &Path, database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let escaped_database_url = database_url.replace('\\', "\\\\").replace('"', "\\\"");
    let config = format!(
        r#"[core]
backend = "postgres"

[postgres]
database_url = "{escaped_database_url}"
"#
    );
    fs::write(dir.join("yarli.toml"), config)?;
    Ok(())
}

fn write_overwatch_test_config(
    dir: &Path,
    database_url: &str,
    overwatch_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let escaped_database_url = database_url.replace('\\', "\\\\").replace('"', "\\\"");
    let escaped_overwatch_url = overwatch_url.replace('\\', "\\\\").replace('"', "\\\"");
    let config = format!(
        r#"[core]
backend = "postgres"

[postgres]
database_url = "{escaped_database_url}"

[execution]
runner = "overwatch"

[execution.overwatch]
service_url = "{escaped_overwatch_url}"
"#
    );
    fs::write(dir.join("yarli.toml"), config)?;
    Ok(())
}

fn yarli_binary_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    match env::var_os("CARGO_BIN_EXE_yarli") {
        Some(path) => Ok(PathBuf::from(path)),
        None => {
            let current_exe = env::current_exe()?;
            let debug_dir = current_exe.parent().and_then(|path| path.parent()).ok_or(
                "failed to derive target/debug directory from current test executable path",
            )?;
            let binary_name = format!("yarli{}", std::env::consts::EXE_SUFFIX);
            let fallback = debug_dir.join(binary_name);
            if fallback.is_file() {
                Ok(fallback)
            } else {
                Err(
                    "CARGO_BIN_EXE_yarli is not set and target/debug/yarli fallback was not found"
                        .into(),
                )
            }
        }
    }
}

struct TestDatabase {
    admin_database_url: String,
    database_name: String,
    database_url: String,
}

impl TestDatabase {
    async fn create(admin_database_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let admin_pool = connect_postgres(admin_database_url, "TestDatabase::create(admin)").await?;

        let database_name = format!("yarli_test_{}", Uuid::now_v7().simple());
        sqlx::query(&format!(r#"CREATE DATABASE "{database_name}""#))
            .execute(&admin_pool)
            .await?;

        let connect_options = PgConnectOptions::from_str(admin_database_url)?;
        let database_url = connect_options
            .database(&database_name)
            .to_url_lossy()
            .to_string();

        Ok(Self {
            admin_database_url: admin_database_url.to_string(),
            database_name,
            database_url,
        })
    }

    async fn drop(self) -> Result<(), Box<dyn std::error::Error>> {
        let admin_pool = connect_postgres(&self.admin_database_url, "TestDatabase::drop(admin)").await?;

        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
        )
        .bind(&self.database_name)
        .execute(&admin_pool)
        .await?;

        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}""#,
            self.database_name
        ))
        .execute(&admin_pool)
        .await?;

        Ok(())
    }
}

async fn apply_migrations(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pool = connect_postgres(database_url, "apply_migrations").await?;

    for statement in MIGRATION_0001_INIT
        .split(';')
        .chain(MIGRATION_0002_INDEXES.split(';'))
    {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        sqlx::query(statement).execute(&pool).await?;
    }

    Ok(())
}

fn test_database_url() -> Option<String> {
    env::var(TEST_DATABASE_URL_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn test_database_url_for_test(test_name: &str) -> Option<String> {
    let Some(admin_database_url) = test_database_url() else {
        if require_postgres_tests() {
            panic!(
                "postgres integration tests require {TEST_DATABASE_URL_ENV} when {REQUIRE_POSTGRES_TESTS_ENV}=1"
            );
        }
        eprintln!(
            "skipping postgres integration test '{test_name}': set {TEST_DATABASE_URL_ENV} (example: postgres://postgres:postgres@localhost:5432/postgres)"
        );
        eprintln!("local bootstrap example: {LOCAL_POSTGRES_BOOTSTRAP_HINT}");
        return None;
    };

    Some(admin_database_url)
}

fn require_postgres_tests() -> bool {
    env::var(REQUIRE_POSTGRES_TESTS_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

async fn connect_postgres(
    database_url: &str,
    context: &str,
) -> Result<PgPool, Box<dyn std::error::Error>> {
    connect_postgres_with_timeout(database_url, context, TEST_DATABASE_CONNECT_TIMEOUT).await
}

async fn connect_postgres_with_timeout(
    database_url: &str,
    context: &str,
    timeout: Duration,
) -> Result<PgPool, Box<dyn std::error::Error>> {
    let redacted_url = redact_database_url(database_url);
    eprintln!("[{context}] attempting Postgres connect to {redacted_url} with timeout {timeout:?}");

    let connect = PgPoolOptions::new().max_connections(1).connect(database_url);
    match tokio::time::timeout(timeout, connect).await {
        Ok(Ok(pool)) => Ok(pool),
        Ok(Err(err)) => Err(format!(
            "[{context}] Postgres connect failed for {redacted_url}: {err}"
        )
        .into()),
        Err(err) => Err(format!(
            "[{context}] Postgres connect timed out after {timeout:?}: {redacted_url}: {err}"
        )
        .into()),
    }
}

fn redact_database_url(database_url: &str) -> String {
    let Some(scheme_end) = database_url.find("://") else {
        return database_url.to_string();
    };
    let scheme = &database_url[..scheme_end + 3];
    let remainder = &database_url[scheme_end + 3..];
    let Some(at_pos) = remainder.rfind('@') else {
        return database_url.to_string();
    };

    let credentials = &remainder[..at_pos];
    let host_and_db = &remainder[at_pos + 1..];
    let user = credentials.split(':').next().unwrap_or_default();
    if user.is_empty() {
        return format!("{scheme}***@{host_and_db}");
    }

    format!("{scheme}{user}:***@{host_and_db}")
}
