use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use yarli_cli::yarli_chaos::{ChaosController, PanicFault};
use yarli_cli::yarli_core::domain::{CommandClass, EntityType, SafeMode};
use yarli_cli::yarli_core::entities::{Run, Task};
use yarli_cli::yarli_core::fsm::run::RunState;
use yarli_cli::yarli_core::fsm::task::TaskState;
use yarli_cli::yarli_exec::LocalCommandRunner;
#[path = "../src/lib.rs"]
mod integration_helpers;

use integration_helpers::{
    apply_migrations, connect_postgres, test_database_url_for_test, TestDatabase,
};
use yarli_cli::yarli_queue::{PostgresTaskQueue, Scheduler, SchedulerConfig, TaskRegistry};
use yarli_cli::yarli_store::event_store::EventQuery;
use yarli_cli::yarli_store::{EventStore, PostgresEventStore};

fn run_state_from_db(value: &str) -> RunState {
    match value {
        "RUN_OPEN" => RunState::RunOpen,
        "RUN_ACTIVE" => RunState::RunActive,
        "RUN_VERIFYING" => RunState::RunVerifying,
        "RUN_COMPLETED" => RunState::RunCompleted,
        "RUN_FAILED" => RunState::RunFailed,
        "RUN_CANCELLED" => RunState::RunCancelled,
        "RUN_BLOCKED" => RunState::RunBlocked,
        "RUN_DRAINED" => RunState::RunDrained,
        "RunOpen" => RunState::RunOpen,
        "RunActive" => RunState::RunActive,
        "RunVerifying" => RunState::RunVerifying,
        "RunCompleted" => RunState::RunCompleted,
        "RunFailed" => RunState::RunFailed,
        "RunCancelled" => RunState::RunCancelled,
        "RunBlocked" => RunState::RunBlocked,
        "RunDrained" => RunState::RunDrained,
        _ => RunState::RunOpen,
    }
}

fn task_state_from_db(value: &str) -> TaskState {
    match value {
        "TASK_OPEN" => TaskState::TaskOpen,
        "TASK_READY" => TaskState::TaskReady,
        "TASK_EXECUTING" => TaskState::TaskExecuting,
        "TASK_WAITING" => TaskState::TaskWaiting,
        "TASK_BLOCKED" => TaskState::TaskBlocked,
        "TASK_VERIFYING" => TaskState::TaskVerifying,
        "TASK_COMPLETE" => TaskState::TaskComplete,
        "TASK_FAILED" => TaskState::TaskFailed,
        "TASK_CANCELLED" => TaskState::TaskCancelled,
        "TaskOpen" => TaskState::TaskOpen,
        "TaskReady" => TaskState::TaskReady,
        "TaskExecuting" => TaskState::TaskExecuting,
        "TaskWaiting" => TaskState::TaskWaiting,
        "TaskBlocked" => TaskState::TaskBlocked,
        "TaskVerifying" => TaskState::TaskVerifying,
        "TaskComplete" => TaskState::TaskComplete,
        "TaskFailed" => TaskState::TaskFailed,
        "TaskCancelled" => TaskState::TaskCancelled,
        _ => TaskState::TaskOpen,
    }
}

fn run_state_to_db(state: RunState) -> &'static str {
    match state {
        RunState::RunOpen => "RUN_OPEN",
        RunState::RunActive => "RUN_ACTIVE",
        RunState::RunVerifying => "RUN_VERIFYING",
        RunState::RunCompleted => "RUN_COMPLETED",
        RunState::RunFailed => "RUN_FAILED",
        RunState::RunCancelled => "RUN_CANCELLED",
        RunState::RunBlocked => "RUN_BLOCKED",
        RunState::RunDrained => "RUN_DRAINED",
    }
}

fn task_state_to_db(state: TaskState) -> &'static str {
    match state {
        TaskState::TaskOpen => "TASK_OPEN",
        TaskState::TaskReady => "TASK_READY",
        TaskState::TaskExecuting => "TASK_EXECUTING",
        TaskState::TaskWaiting => "TASK_WAITING",
        TaskState::TaskBlocked => "TASK_BLOCKED",
        TaskState::TaskVerifying => "TASK_VERIFYING",
        TaskState::TaskComplete => "TASK_COMPLETE",
        TaskState::TaskFailed => "TASK_FAILED",
        TaskState::TaskCancelled => "TASK_CANCELLED",
    }
}

fn safe_mode_to_db(safe_mode: SafeMode) -> &'static str {
    match safe_mode {
        SafeMode::Observe => "observe",
        SafeMode::Execute => "execute",
        SafeMode::Restricted => "restricted",
        SafeMode::Breakglass => "breakglass",
    }
}

fn command_class_to_db(class: CommandClass) -> &'static str {
    match class {
        CommandClass::Io => "io",
        CommandClass::Cpu => "cpu",
        CommandClass::Git => "git",
        CommandClass::Tool => "tool",
    }
}

async fn seed_run_and_tasks(
    pool: &sqlx::PgPool,
    run: &Run,
    tasks: &[Task],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO runs (
            run_id, objective, state, safe_mode, correlation_id, config_snapshot
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (run_id) DO NOTHING
        "#,
    )
    .bind(run.id)
    .bind(&run.objective)
    .bind(run_state_to_db(run.state))
    .bind(safe_mode_to_db(run.safe_mode))
    .bind(run.correlation_id)
    .bind(&run.config_snapshot)
    .execute(pool)
    .await?;

    for task in tasks {
        sqlx::query(
            r#"
            INSERT INTO tasks (
                task_id,
                run_id,
                task_key,
                description,
                state,
                command_class,
                attempt_no,
                max_attempts,
                blocker_code,
                correlation_id,
                priority
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (task_id, run_id) DO NOTHING
            "#,
        )
        .bind(task.id)
        .bind(task.run_id)
        .bind(&task.task_key)
        .bind(&task.description)
        .bind(task_state_to_db(task.state))
        .bind(command_class_to_db(task.command_class))
        .bind(task.attempt_no as i32)
        .bind(task.max_attempts as i32)
        .bind::<Option<String>>(None::<String>)
        .bind(task.correlation_id)
        .bind(task.priority as i32)
        .execute(pool)
        .await?;
    }

    Ok(())
}

async fn sync_postgres_states_from_events(
    store: &PostgresEventStore,
    pool: &sqlx::PgPool,
    run_id: uuid::Uuid,
    task_ids: &[uuid::Uuid],
) -> Result<(), Box<dyn std::error::Error>> {
    let run_events = store
        .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
        .map_err(|err| format!("query run events: {err}"))?;
    let mut run_state = None;
    for event in run_events {
        if let Some(state) = event.payload.get("to").and_then(|value| value.as_str()) {
            run_state = Some(run_state_from_db(state));
        }
    }

    let task_states: Vec<(uuid::Uuid, TaskState)> = task_ids
        .iter()
        .map(|task_id| {
            let task_events = store
                .query(&EventQuery::by_entity(
                    EntityType::Task,
                    task_id.to_string(),
                ))
                .map_err(|err| format!("query task events: {err}"))?;
            let mut task_state = None;

            for event in task_events {
                if let Some(state) = event.payload.get("to").and_then(|value| value.as_str()) {
                    task_state = Some(task_state_from_db(state));
                }
            }

            task_state
                .map(|state| (*task_id, state))
                .ok_or_else(|| format!("missing task state transitions for {task_id}").into())
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

    let mut tx = pool.begin().await?;
    if let Some(state) = run_state {
        sqlx::query(
            r#"
            UPDATE runs
            SET state = $1,
                updated_at = now()
            WHERE run_id = $2
            "#,
        )
        .bind(run_state_to_db(state))
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
    }

    for (task_id, state) in task_states {
        sqlx::query(
            r#"
            UPDATE tasks
            SET state = $1,
                updated_at = now()
            WHERE task_id = $2
            "#,
        )
        .bind(task_state_to_db(state))
        .bind(task_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(())
}

async fn latest_run_state_from_events(
    store: &PostgresEventStore,
    run_id: uuid::Uuid,
) -> Result<RunState, Box<dyn std::error::Error>> {
    let run_events = store
        .query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))
        .map_err(|err| format!("query run events: {err}"))?;

    let mut state = RunState::RunOpen;
    for event in run_events {
        if let Some(state_value) = event.payload.get("to").and_then(|value| value.as_str()) {
            state = run_state_from_db(state_value);
        }
    }

    Ok(state)
}

#[tokio::test]
async fn crash_recovery_invariant_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test("crash_recovery_invariant_postgres")
    else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;
    let pool = connect_postgres(&database.database_url, "crash_verification").await?;

    let store = Arc::new(PostgresEventStore::new(&database.database_url)?);
    let queue = Arc::new(PostgresTaskQueue::new(&database.database_url)?);
    let runner = Arc::new(LocalCommandRunner::new());

    let chaos = Arc::new(ChaosController::new());
    // Panic after claiming, before execution continues.
    chaos.add_fault(Arc::new(PanicFault {
        point: "scheduler_tick_claimed".to_string(),
        message: "Simulated scheduler crash after claim".to_string(),
        probability: 1.0,
    }));

    let config = SchedulerConfig {
        worker_id: "crasher-worker".to_string(),
        tick_interval: Duration::from_millis(10),
        // Short lease for faster recovery test
        lease_ttl: chrono::Duration::seconds(2),
        reclaim_grace: chrono::Duration::seconds(1),
        ..Default::default()
    };

    let sched = Scheduler::new(queue.clone(), store.clone(), runner.clone(), config.clone())
        .with_chaos(chaos.clone());

    let run = Run::new("crash test", SafeMode::Execute);
    let run_id = run.id;
    let corr_id = run.correlation_id;
    let task = Task::new(run_id, "task-1", "sleep 1", CommandClass::Io, corr_id);
    let task_id = task.id;
    let tasks = vec![task.clone()];

    seed_run_and_tasks(&pool, &run, &tasks).await?;

    sched.submit_run(run.clone(), vec![task.clone()]).await?;

    // Run scheduler in a separate task and expect panic
    let handle = tokio::spawn(async move { sched.tick().await });

    // Wait until the task is leased before forcing a simulated crash.
    let row = {
        let start = std::time::Instant::now();
        let mut row = None;
        while start.elapsed() < Duration::from_secs(5) {
            if handle.is_finished() {
                break;
            }

            let candidate = sqlx::query(
                "SELECT lease_owner, lease_expires_at FROM task_queue WHERE task_id = $1",
            )
            .bind(task_id)
            .fetch_optional(&pool)
            .await?;

            if let Some(found) = candidate {
                let lease_owner: Option<String> = found.try_get("lease_owner")?;
                let lease_expires_at: Option<chrono::DateTime<chrono::Utc>> =
                    found.try_get("lease_expires_at")?;
                if lease_owner.is_some() {
                    row = Some((lease_owner, lease_expires_at));
                    break;
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        row.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "task queue row was never leased",
            )
        })?
    };

    let (_lease_owner, _lease_expires_at) = row;

    // Force a simulated crash path even if chaos hook did not panic.
    handle.abort();

    let result = handle.await;
    match result {
        Ok(inner) => {
            assert!(
                inner.is_err(),
                "scheduler tick should not complete successfully after forced crash"
            );
        }
        Err(err) => {
            assert!(
                err.is_cancelled() || err.is_panic(),
                "scheduler termination should be cancel/panic semantics"
            );
        }
    }

    // Check queue state
    let row = {
        let start = std::time::Instant::now();
        let mut row = None;
        while start.elapsed() < Duration::from_secs(5) {
            let candidate = sqlx::query(
                "SELECT lease_owner, lease_expires_at FROM task_queue WHERE task_id = $1",
            )
            .bind(task_id)
            .fetch_optional(&pool)
            .await?;
            if let Some(found) = candidate {
                row = Some(found);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        row.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "task queue row missing after crash",
            )
        })?
    };
    let lease_owner: Option<String> = row.try_get("lease_owner")?;
    let lease_expires_at: Option<chrono::DateTime<chrono::Utc>> =
        row.try_get("lease_expires_at")?;

    assert_eq!(
        lease_owner.as_deref(),
        Some("crasher-worker"),
        "Task should be leased by crasher"
    );
    assert!(lease_expires_at.is_some(), "Lease should have expiration");

    // Now start a new scheduler (recovery) with reconstructed registry.
    let mut recovery_config = config.clone();
    recovery_config.worker_id = "recovery-worker".to_string();
    let task_state: String = sqlx::query_scalar("SELECT state FROM tasks WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(&pool)
        .await?;
    let task_attempt: i32 = sqlx::query_scalar("SELECT attempt_no FROM tasks WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(&pool)
        .await?;

    let mut recovery_run = run;
    recovery_run.state = latest_run_state_from_events(store.as_ref(), run_id).await?;
    recovery_run.add_task(task_id);

    let mut recovery_task = task;
    let recovered_task_state = {
        let state = task_state_from_db(&task_state);
        // A crashed scheduler can leave the task as executing; ensure recovery
        // can replay the lease by reconstructing a runnable task state.
        if state == TaskState::TaskExecuting {
            TaskState::TaskReady
        } else {
            state
        }
    };
    recovery_task.state = recovered_task_state;
    recovery_task.attempt_no = task_attempt as u32;

    let mut registry = TaskRegistry::new();
    registry.add_run(recovery_run);
    registry.add_task(recovery_task);

    let recovery_sched = Scheduler::with_registry(
        queue.clone(),
        store.clone(),
        runner.clone(),
        recovery_config,
        registry,
    );

    // Wait for lease to expire (2s + buffer)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Run recovery loop
    let recovery_handle = tokio::spawn(async move {
        // Run loop for a bit to allow reclaim and execution
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            let _ = recovery_sched.reclaim_stale_leases().await;
            let _ = recovery_sched.tick().await;

            // Check completion
            let reg = recovery_sched.registry().read().await;
            if reg.is_task_complete(&task_id) {
                return Ok(());
            }
            drop(reg);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err("Timed out waiting for recovery")
    });

    let _ = recovery_handle.await?;

    sync_postgres_states_from_events(store.as_ref(), &pool, run_id, &[task_id]).await?;

    // Verify success
    let task_state: String = sqlx::query_scalar("SELECT state FROM tasks WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(task_state_from_db(&task_state), TaskState::TaskComplete);

    let run_state: String = sqlx::query_scalar("SELECT state FROM runs WHERE run_id = $1")
        .bind(run_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(run_state_from_db(&run_state), RunState::RunCompleted);

    database.drop().await?;
    Ok(())
}
