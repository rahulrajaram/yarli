use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use yarli_chaos::{ChaosController, PanicFault};
use yarli_core::domain::{CommandClass, SafeMode};
use yarli_core::entities::{Run, Task};
use yarli_exec::LocalCommandRunner;
use yarli_integration_tests::{
    apply_migrations, connect_postgres, test_database_url_for_test, TestDatabase,
};
use yarli_queue::{PostgresTaskQueue, Scheduler, SchedulerConfig};
use yarli_store::PostgresEventStore;

#[tokio::test]
async fn crash_recovery_invariant_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test("crash_recovery_invariant_postgres")
    else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let store = Arc::new(PostgresEventStore::new(&database.database_url)?);
    let queue = Arc::new(PostgresTaskQueue::new(&database.database_url)?);
    let runner = Arc::new(LocalCommandRunner::new());

    let chaos = Arc::new(ChaosController::new());
    // Panic after claiming, before execution finishes
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
    let task = Task::new(
        run_id,
        "task-1",
        "echo recovered",
        CommandClass::Io,
        corr_id,
    );
    let task_id = task.id;

    sched.submit_run(run.clone(), vec![task.clone()]).await?;

    // Run scheduler in a separate task and expect panic
    let handle = tokio::spawn(async move {
        // We need to run tick loop manually or use run()
        // run() loops forever, tick() runs once.
        // We want to loop until panic.
        loop {
            let _ = sched.tick().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Wait for panic (task should fail)
    let result = handle.await;
    assert!(result.is_err(), "Scheduler should have panicked");
    assert!(
        result.unwrap_err().is_panic(),
        "Scheduler should have panicked"
    );

    // Verify task is leased but not complete
    let pool = connect_postgres(&database.database_url, "crash_verification").await?;

    // Check queue state
    let row =
        sqlx::query("SELECT lease_owner, lease_expires_at FROM task_queue WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await?;
    let lease_owner: Option<String> = row.try_get("lease_owner")?;
    let lease_expires_at: Option<chrono::DateTime<chrono::Utc>> =
        row.try_get("lease_expires_at")?;

    assert_eq!(
        lease_owner.as_deref(),
        Some("crasher-worker"),
        "Task should be leased by crasher"
    );
    assert!(lease_expires_at.is_some(), "Lease should have expiration");

    // Now start a new scheduler (recovery)
    let mut recovery_config = config.clone();
    recovery_config.worker_id = "recovery-worker".to_string();

    let recovery_sched = Scheduler::new(
        queue.clone(),
        store.clone(),
        runner.clone(),
        recovery_config,
    );
    // Re-submit to hydrate registry (simulates restart of process with same inputs)
    recovery_sched.submit_run(run, vec![task]).await?;

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

    recovery_handle.await??;

    // Verify success
    let task_state: String = sqlx::query_scalar("SELECT state FROM tasks WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(task_state, "TASK_COMPLETE");

    database.drop().await?;
    Ok(())
}
