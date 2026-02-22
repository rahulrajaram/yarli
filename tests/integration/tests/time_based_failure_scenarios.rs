//! Integration test: time-based failure and ordering scenarios.
//!
//! Primary goals:
//! - Clock-skewed lease reclaim invariants.
//! - Timeout enforcement without silent hangs.
//! - Heartbeat timing under high lease-pressure/retry churn.
//! - Event timestamp ordering under concurrent writes.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use chrono::{Duration as ChronoDuration, Utc};
use sqlx::PgPool;
use tokio::time::{self, Duration as TokioDuration};
use uuid::Uuid;

use yarli_core::domain::{CommandClass, Event, SafeMode};
use yarli_core::entities::{Run, Task};
use yarli_core::fsm::run::RunState;
use yarli_exec::LocalCommandRunner;
use yarli_integration_tests::{
    apply_migrations, connect_postgres, test_database_url_for_test, TestDatabase,
};
use yarli_queue::{
    ClaimRequest, ConcurrencyConfig, InMemoryTaskQueue, PostgresTaskQueue, QueueStatus,
    ResourceBudgetConfig, Scheduler, SchedulerConfig, TaskQueue,
};
use yarli_store::{EventStore, InMemoryEventStore};

fn make_scheduler_config(
    worker_id: &str,
    command_timeout: Option<TokioDuration>,
) -> SchedulerConfig {
    SchedulerConfig {
        worker_id: worker_id.to_string(),
        claim_batch_size: 4,
        lease_ttl: ChronoDuration::seconds(3),
        tick_interval: TokioDuration::from_millis(100),
        heartbeat_interval: TokioDuration::from_millis(10),
        reclaim_interval: TokioDuration::from_secs(1),
        reclaim_grace: ChronoDuration::seconds(1),
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

async fn seed_postgres_run_and_tasks(
    pool: &PgPool,
    run_id: Uuid,
    correlation_id: Uuid,
    tasks: &[(Uuid, &str)],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO runs (run_id, objective, state, safe_mode, correlation_id, config_snapshot)
        VALUES ($1, $2, 'RUN_ACTIVE', 'execute', $3, '{}'::jsonb)
        "#,
    )
    .bind(run_id)
    .bind("time-based failure scenarios")
    .bind(correlation_id)
    .execute(pool)
    .await?;

    for (task_id, key) in tasks {
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
            VALUES ($1, $2, $3, $4, 'TASK_READY', 'io', 1, 3, NULL, $5, 1)
            "#,
        )
        .bind(task_id)
        .bind(run_id)
        .bind(key.to_string())
        .bind(format!("time based {key}"))
        .bind(correlation_id)
        .execute(pool)
        .await?;
    }

    Ok(())
}

#[tokio::test]
async fn time_based_lease_reclaim_honors_clock_skew_boundaries_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url_for_test(
        "time_based_lease_reclaim_honors_clock_skew_boundaries_postgres",
    ) else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;
    let pool = connect_postgres(&database.database_url, "time-based-skip-test").await?;

    let run_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();
    let task_id_a = Uuid::now_v7();
    let task_id_b = Uuid::now_v7();
    seed_postgres_run_and_tasks(
        &pool,
        run_id,
        corr_id,
        &[(task_id_a, "a"), (task_id_b, "b")],
    )
    .await?;

    let queue = PostgresTaskQueue::new(&database.database_url)?;
    queue.enqueue(task_id_a, run_id, 1, CommandClass::Io, None)?;
    queue.enqueue(task_id_b, run_id, 1, CommandClass::Io, None)?;

    let claimed = queue.claim(
        &ClaimRequest::new("clock-skew-worker", 2, ChronoDuration::seconds(5)),
        &ConcurrencyConfig::default(),
    )?;
    assert_eq!(claimed.len(), 2);

    let skewed_stale = claimed[0].queue_id;
    let skewed_future = claimed[1].queue_id;

    // Simulate a backward clock skew on one lease: expiry pushed into the past.
    let stale_time = Utc::now() - ChronoDuration::seconds(120);
    // Simulate a forward clock skew on the other lease: expiry far in the future.
    let future_time = Utc::now() + ChronoDuration::minutes(2);

    sqlx::query("UPDATE task_queue SET lease_expires_at = $1 WHERE queue_id = $2")
        .bind(stale_time)
        .bind(skewed_stale)
        .execute(&pool)
        .await?;

    sqlx::query("UPDATE task_queue SET lease_expires_at = $1 WHERE queue_id = $2")
        .bind(future_time)
        .bind(skewed_future)
        .execute(&pool)
        .await?;

    let reclaimed = queue.reclaim_stale(ChronoDuration::seconds(0)).unwrap();
    assert_eq!(reclaimed, 1, "one stale lease should be reclaimed");

    let entries = queue.entries();
    let stale_entry = entries
        .iter()
        .find(|entry| entry.queue_id == skewed_stale)
        .expect("skewed stale entry should still be present");
    let future_entry = entries
        .iter()
        .find(|entry| entry.queue_id == skewed_future)
        .expect("future-skewed entry should still be present");

    assert_eq!(stale_entry.status, QueueStatus::Pending);
    assert_eq!(future_entry.status, QueueStatus::Leased);

    let reclaimed_again = queue.reclaim_stale(ChronoDuration::zero()).unwrap();
    assert_eq!(
        reclaimed_again, 0,
        "future lease under skew should not reclaim immediately"
    );

    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn task_timeout_is_enforced_without_hanging() {
    let queue = std::sync::Arc::new(InMemoryTaskQueue::new());
    let store = std::sync::Arc::new(InMemoryEventStore::new());
    let runner = std::sync::Arc::new(LocalCommandRunner::new());

    let config = make_scheduler_config("timeout-enforcer", Some(TokioDuration::from_millis(120)));
    let sched = Scheduler::new(queue, store.clone(), runner, config);

    let run = make_run("timeout enforcement test");
    let run_id = run.id;
    let corr_id = run.correlation_id;
    let mut task = make_task(run_id, "slow", "sleep 60", corr_id);
    task = task.with_max_attempts(1);

    sched.submit_run(run, vec![task]).await.unwrap();

    let tick_result = time::timeout(TokioDuration::from_secs(2), sched.tick()).await;
    assert!(
        tick_result.is_ok(),
        "scheduler tick should return under timeout budget"
    );
    let tick = tick_result.unwrap().unwrap();
    assert_eq!(tick.timed_out, 1, "one command execution should time out");

    let reg = sched.registry().read().await;
    let run = reg.get_run(&run_id).unwrap();
    assert_eq!(run.state, RunState::RunFailed, "timed out run should fail");

    let events = store.all().unwrap();
    let timeout_event = events.iter().find(|event| {
        event.event_type == "task.failed"
            && event.payload.get("reason").and_then(|value| value.as_str()) == Some("timeout")
    });
    assert!(
        timeout_event.is_some(),
        "task failure event should explicitly record timeout reason"
    );
}

#[tokio::test]
async fn heartbeat_extensions_prevent_stale_reclaim_under_contention() {
    let queue = std::sync::Arc::new(InMemoryTaskQueue::new());
    let run_id = Uuid::now_v7();

    queue
        .enqueue(Uuid::now_v7(), run_id, 1, CommandClass::Io, None)
        .unwrap();

    let claimed = queue
        .claim(
            &ClaimRequest::new("hb-worker", 1, ChronoDuration::milliseconds(150)),
            &ConcurrencyConfig::default(),
        )
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let queue_id = claimed[0].queue_id;

    let mut observed_expiry = claimed[0]
        .lease_expires_at
        .expect("claim should set lease_expires_at");

    // Send periodic heartbeat refreshes as if scheduler heartbeat interval is active.
    for _ in 0..8 {
        time::sleep(TokioDuration::from_millis(80)).await;
        queue
            .heartbeat(queue_id, "hb-worker", ChronoDuration::milliseconds(150))
            .unwrap();

        let entries = queue.entries();
        let entry = entries
            .iter()
            .find(|entry| entry.queue_id == queue_id)
            .expect("entry should still exist while leased");
        assert_eq!(entry.status, QueueStatus::Leased);

        let current_expiry = entry
            .lease_expires_at
            .expect("heartbeat should keep lease_expiry");
        assert!(
            current_expiry >= observed_expiry,
            "heartbeat should not move lease expiry backward"
        );
        observed_expiry = current_expiry;

        let reclaimed = queue.reclaim_stale(ChronoDuration::zero()).unwrap();
        assert_eq!(
            reclaimed, 0,
            "heartbeat activity should prevent reclaim under ongoing lease pressure"
        );
    }

    // Stop heartbeats and confirm stale reclaim eventually recovers the lease.
    time::sleep(TokioDuration::from_millis(250)).await;
    let reclaimed = queue.reclaim_stale(ChronoDuration::zero()).unwrap();
    assert_eq!(
        reclaimed, 1,
        "lease should be reclaimed after heartbeat window is missed"
    );
}

#[tokio::test]
async fn concurrent_event_appends_keep_timestamp_ordering() {
    let store = std::sync::Arc::new(InMemoryEventStore::new());
    let task_count = 64;
    let base = Utc::now();
    let next_sequence = Arc::new(AtomicUsize::new(0));
    let next_turn = Arc::new(AtomicUsize::new(0));
    let correlation_id = Uuid::now_v7();

    let mut handles = Vec::with_capacity(task_count);

    for _ in 0..task_count {
        let store = Arc::clone(&store);
        let next_sequence = Arc::clone(&next_sequence);
        let next_turn = Arc::clone(&next_turn);

        handles.push(tokio::spawn(async move {
            let seq = next_sequence.fetch_add(1, Ordering::SeqCst) as i64;
            // Add a little jitter to prove writes were raced to start.
            time::sleep(TokioDuration::from_millis((seq % 4 * 2) as u64)).await;

            while next_turn.load(Ordering::Acquire) != seq as usize {
                tokio::task::yield_now().await;
            }

            let event = Event {
                event_id: Uuid::now_v7(),
                occurred_at: base + ChronoDuration::milliseconds(seq),
                entity_type: yarli_core::domain::EntityType::Task,
                entity_id: Uuid::now_v7().to_string(),
                event_type: "task.event".to_string(),
                payload: serde_json::json!({ "seq": seq }),
                correlation_id,
                causation_id: None,
                actor: "test".to_string(),
                idempotency_key: None,
            };
            store.append(event).unwrap();

            next_turn.fetch_add(1, Ordering::Release);
        }));
    }

    for handle in handles {
        handle
            .await
            .expect("event writer task should complete without panic");
    }

    let events = store.all().unwrap();
    assert_eq!(
        events.len(),
        task_count,
        "all concurrent event writes must be recorded"
    );

    for window in events.windows(2) {
        assert!(
            window[1].occurred_at >= window[0].occurred_at,
            "event query order should not go backward in timestamp"
        );
    }
}

#[tokio::test]
async fn time_based_smoke_no_unexpected_reclaim_after_clock_tamper(
) -> Result<(), Box<dyn std::error::Error>> {
    // Lightweight smoke variant that shares the same DB setup path as the main skew test.
    let Some(admin_database_url) =
        test_database_url_for_test("time_based_smoke_no_unexpected_reclaim_after_clock_tamper")
    else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;
    let pool = connect_postgres(&database.database_url, "time-based-skip-test-smoke").await?;

    let run_id = Uuid::now_v7();
    let corr_id = Uuid::now_v7();
    let task_id = Uuid::now_v7();

    seed_postgres_run_and_tasks(&pool, run_id, corr_id, &[(task_id, "smoke")]).await?;

    let queue = PostgresTaskQueue::new(&database.database_url)?;
    queue.enqueue(task_id, run_id, 1, CommandClass::Io, None)?;

    let claimed = queue.claim(
        &ClaimRequest::single("clock-smoke-worker"),
        &ConcurrencyConfig::default(),
    )?;
    assert_eq!(claimed.len(), 1);

    let skewed = Utc::now() + ChronoDuration::minutes(1);
    sqlx::query("UPDATE task_queue SET lease_expires_at = $1 WHERE queue_id = $2")
        .bind(skewed)
        .bind(claimed[0].queue_id)
        .execute(&pool)
        .await?;

    let reclaimed = queue.reclaim_stale(ChronoDuration::zero()).unwrap();
    assert_eq!(
        reclaimed, 0,
        "future-skewed lease should not reclaim as stale"
    );

    let reclaimed_later = queue.reclaim_stale(ChronoDuration::seconds(3600)).unwrap();
    assert_eq!(
        reclaimed_later, 0,
        "even with large reclaim grace, future-skewed lease should remain leased"
    );

    database.drop().await?;
    Ok(())
}
