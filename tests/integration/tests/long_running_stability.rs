//! Integration test: long-running scheduler stability under sustained load.
//!
//! Scenario:
//! - Submit many short-lived runs over many ticks.
//! - Verify tasks progress to completion without orphaned queue rows.
//! - Track basic resource and queue health signals while running.
//! - Optionally assert PostgreSQL connection stability when DB URL is available.

use std::collections::HashSet;
use std::time::Duration;

use yarli_core::domain::{CommandClass, SafeMode};
use yarli_core::entities::{Run, Task};
use yarli_exec::LocalCommandRunner;
use yarli_queue::{
    ConcurrencyConfig, InMemoryTaskQueue, ResourceBudgetConfig, Scheduler, SchedulerConfig,
    TaskQueue,
};
use yarli_store::{event_store::EventQuery, EventStore, InMemoryEventStore, PostgresEventStore};

#[cfg(unix)]
use std::fs;

use uuid::Uuid;
use yarli_integration_tests::{apply_migrations, test_database_url_for_test, TestDatabase};

fn make_scheduler_config(
    worker_id: &str,
    claim_batch_size: usize,
    command_timeout: Option<Duration>,
) -> SchedulerConfig {
    SchedulerConfig {
        worker_id: worker_id.to_string(),
        claim_batch_size,
        lease_ttl: chrono::Duration::seconds(30),
        tick_interval: Duration::from_millis(5),
        heartbeat_interval: Duration::from_secs(5),
        reclaim_interval: Duration::from_secs(10),
        reclaim_grace: chrono::Duration::seconds(5),
        concurrency: ConcurrencyConfig::default(),
        command_timeout,
        working_dir: "/tmp".to_string(),
        task_gates: vec![],
        run_gates: vec![],
        enforce_policies: true,
        audit_decisions: true,
        budgets: ResourceBudgetConfig::default(),
        allow_recursive_run: false,
    }
}

fn make_run(objective: &str) -> Run {
    Run::new(objective, SafeMode::Execute)
}

fn make_task(run_id: Uuid, key: &str, command: &str, correlation_id: Uuid) -> Task {
    Task::new(run_id, key, command, CommandClass::Io, correlation_id)
}

#[cfg(unix)]
fn open_file_descriptors() -> Option<usize> {
    fs::read_dir("/proc/self/fd")
        .ok()
        .map(|entries| entries.count())
}

#[cfg(not(unix))]
fn open_file_descriptors() -> Option<usize> {
    None
}

#[cfg(unix)]
fn resident_pages() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let resident = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(resident)
}

#[cfg(not(unix))]
fn resident_pages() -> Option<u64> {
    None
}

#[tokio::test]
async fn long_running_stability_continuous_submission_completes_without_orphans() {
    let queue = std::sync::Arc::new(InMemoryTaskQueue::new());
    let store = std::sync::Arc::new(InMemoryEventStore::new());
    let runner = std::sync::Arc::new(LocalCommandRunner::new());
    let sched = Scheduler::new(
        queue.clone(),
        store.clone(),
        runner,
        make_scheduler_config("r13-06-soak", 8, Some(Duration::from_secs(2))),
    );

    let baseline_fd = open_file_descriptors();
    let baseline_pages = resident_pages();

    let mut run_ids = Vec::new();
    let mut all_tasks = HashSet::new();
    let mut max_fd = baseline_fd.unwrap_or(0);
    let mut max_pages = baseline_pages.unwrap_or(0);

    for cycle in 0..160 {
        let run = make_run(&format!("long-running-soak-{cycle}"));
        let run_id = run.id;
        let corr_id = run.correlation_id;

        let mut task_a = make_task(run_id, "echo-a", "echo alpha", corr_id);
        let mut task_b = make_task(run_id, "echo-b", "echo bravo", corr_id);
        task_a = task_a.with_max_attempts(1);
        task_b = task_b.with_max_attempts(1);
        all_tasks.insert(task_a.id);
        all_tasks.insert(task_b.id);

        sched.submit_run(run, vec![task_a, task_b]).await.unwrap();
        run_ids.push(run_id);

        // Keep the run moving while continuously loading work.
        for _ in 0..4 {
            sched.tick().await.unwrap();
        }

        if cycle % 20 == 0 {
            let fd_count = open_file_descriptors();
            let resident = resident_pages();
            let recent_events = store.len();
            let _latest_tail = store
                .query(&EventQuery::default().with_limit(20))
                .expect("should support bounded event query at high load");

            if let Some(fd_count) = fd_count {
                max_fd = max_fd.max(fd_count);
                assert!(
                    fd_count <= baseline_fd.unwrap_or(0).saturating_add(192),
                    "file descriptor growth indicates possible leak: {fd_count} > {}",
                    baseline_fd.unwrap_or(0).saturating_add(192),
                );
            }

            if let Some(resident) = resident {
                max_pages = max_pages.max(resident);
                assert!(
                    resident <= baseline_pages.unwrap_or(0).saturating_add(1_000_000),
                    "resident memory page growth indicates possible leak: {resident} > {}",
                    baseline_pages.unwrap_or(0).saturating_add(1_000_000),
                );
            }

            assert!(
                recent_events >= cycle * 4,
                "event store should keep up with soak workload"
            );
            eprintln!(
                "soak cycle {cycle:>3}: run={:?}, events={recent_events}, fd={:?}, pages={:?}, max_fd={max_fd}, max_pages={max_pages}",
                run_id,
                fd_count,
                resident
            );
        }
    }

    for _ in 0..400 {
        let _ = sched.tick().await.unwrap();
        let reg = sched.registry().read().await;
        if run_ids.iter().all(|run_id| {
            reg.get_run(run_id)
                .is_some_and(|run| run.state.is_terminal())
        }) {
            break;
        }
        drop(reg);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let reg = sched.registry().read().await;
    for run_id in &run_ids {
        let run = reg
            .get_run(run_id)
            .expect("run should still be present for in-memory soak tracking");
        assert!(
            run.state.is_terminal(),
            "run {run_id} should complete during soak test"
        );

        for task in reg.tasks_for_run(run_id) {
            assert!(
                task.state.is_terminal(),
                "task {} should finish as part of run {run_id} completion",
                task.id
            );
        }
    }

    let stats = queue.stats();
    assert_eq!(
        stats.leased, 0,
        "no tasks should remain leased after soak completion"
    );
    assert_eq!(
        queue.pending_count(),
        0,
        "pending queue work should drain after completion"
    );

    let final_fd = open_file_descriptors();
    let final_pages = resident_pages();

    if let (Some(start), Some(end)) = (baseline_fd, final_fd) {
        assert!(end <= start.saturating_add(192));
    }
    if let (Some(start), Some(end)) = (baseline_pages, final_pages) {
        assert!(end <= start.saturating_add(1_000_000));
    }

    let events = store.all().unwrap();
    assert!(
        events.len() >= all_tasks.len(),
        "event log should include task-level state transitions"
    );

    let all_task_ids: usize = run_ids
        .iter()
        .map(|run_id| reg.tasks_for_run(run_id).len())
        .sum();
    assert_eq!(
        all_task_ids,
        all_tasks.len(),
        "tracked tasks should equal submitted tasks"
    );
}

async fn postgres_connection_count(pool: &sqlx::PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_stat_activity WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await
}

#[tokio::test]
async fn long_running_stability_postgres_connection_leak_check(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) =
        test_database_url_for_test("long_running_stability_postgres_connection_leak_check")
    else {
        eprintln!("skipping postgres connection leak check: set YARLI_TEST_DATABASE_URL");
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&database.database_url)
        .await?;

    let store = std::sync::Arc::new(PostgresEventStore::from_pool(pool.clone()));
    let queue = std::sync::Arc::new(InMemoryTaskQueue::new());
    let runner = std::sync::Arc::new(LocalCommandRunner::new());
    let sched = Scheduler::new(
        queue,
        store,
        runner,
        make_scheduler_config("r13-06-postgres", 16, Some(Duration::from_secs(1))),
    );

    let before = postgres_connection_count(&pool).await?;

    let mut run_ids = Vec::new();
    for cycle in 0..120 {
        let run = make_run(&format!("long-running-postgres-{cycle}"));
        let run_id = run.id;
        let corr_id = run.correlation_id;

        let mut task_a = make_task(run_id, "echo-a", "echo one", corr_id);
        let mut task_b = make_task(run_id, "echo-b", "echo two", corr_id);
        task_a = task_a.with_max_attempts(1);
        task_b = task_b.with_max_attempts(1);
        sched.submit_run(run, vec![task_a, task_b]).await?;

        run_ids.push(run_id);
        for _ in 0..3 {
            sched.tick().await?;
        }
    }

    for _ in 0..240 {
        sched.tick().await?;
        let reg = sched.registry().read().await;
        if run_ids.iter().all(|run_id| {
            reg.get_run(run_id)
                .is_some_and(|run| run.state.is_terminal())
        }) {
            break;
        }
        drop(reg);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let reg = sched.registry().read().await;
    for run_id in &run_ids {
        let run = reg
            .get_run(run_id)
            .expect("postgres-stability run must remain visible in registry");
        assert!(run.state.is_terminal());
    }
    drop(reg);

    let after = postgres_connection_count(&pool).await?;
    assert!(
        after <= before.saturating_add(6),
        "postgres connection count grew unexpectedly: before={before}, after={after}"
    );

    database.drop().await?;
    Ok(())
}
