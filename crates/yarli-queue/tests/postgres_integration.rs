use std::collections::HashSet;
use std::env;
use std::str::FromStr;

use chrono::{Duration, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::ConnectOptions;
use uuid::Uuid;
use yarli_core::domain::{CommandClass, EntityType, Event};
use yarli_queue::{ClaimRequest, ConcurrencyConfig, PostgresTaskQueue, TaskQueue};
use yarli_store::{EventStore, PostgresEventStore, MIGRATION_0001_INIT, MIGRATION_0002_INDEXES};

const TEST_DATABASE_URL_ENV: &str = "YARLI_TEST_DATABASE_URL";
const REQUIRE_POSTGRES_TESTS_ENV: &str = "YARLI_REQUIRE_POSTGRES_TESTS";

#[tokio::test]
async fn enqueue_and_claim_lease_roundtrip_against_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url() else {
        if require_postgres_tests() {
            panic!(
                "postgres integration tests require {TEST_DATABASE_URL_ENV} when {REQUIRE_POSTGRES_TESTS_ENV}=1"
            );
        }
        eprintln!(
            "skipping postgres integration test: set {TEST_DATABASE_URL_ENV} (example: postgres://postgres:postgres@localhost:5432/postgres)"
        );
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database.database_url)
        .await?;
    let run_id = Uuid::now_v7();
    let task_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();

    sqlx::query(
        r#"
        INSERT INTO runs (run_id, objective, state, safe_mode, correlation_id, config_snapshot)
        VALUES ($1, $2, 'RUN_ACTIVE', 'execute', $3, '{}'::jsonb)
        "#,
    )
    .bind(run_id)
    .bind("queue integration test run")
    .bind(correlation_id)
    .execute(&pool)
    .await?;

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
        VALUES ($1, $2, $3, $4, 'TASK_READY', 'git', 1, 3, NULL, $5, 1)
        "#,
    )
    .bind(task_id)
    .bind(run_id)
    .bind("queue-integration-task")
    .bind("queue integration task")
    .bind(correlation_id)
    .execute(&pool)
    .await?;

    let queue = PostgresTaskQueue::new(&database.database_url)?;
    let queue_id = queue.enqueue(task_id, run_id, 1, CommandClass::Git, None)?;

    let claim_request = ClaimRequest::new("worker-queue-integration", 1, Duration::seconds(30));
    let claimed = queue.claim(&claim_request, &ConcurrencyConfig::default())?;

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].queue_id, queue_id);
    assert_eq!(claimed[0].task_id, task_id);
    assert_eq!(claimed[0].run_id, run_id);
    assert_eq!(
        claimed[0].lease_owner.as_deref(),
        Some("worker-queue-integration")
    );
    assert!(claimed[0].lease_expires_at.is_some());
    assert_eq!(queue.pending_count(), 0);
    assert_eq!(queue.leased_count_for_run(run_id), 1);
    assert_eq!(queue.leased_count_for_class(CommandClass::Git), 1);

    database.drop().await?;
    Ok(())
}

/// Prove that >=2 concurrent workers cannot double-claim the same queue entry.
///
/// This validates SC-2 from the consistency matrix: single-active-lease invariant
/// under Postgres FOR UPDATE SKIP LOCKED.
#[tokio::test]
async fn concurrent_claim_no_duplicate_lease_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url() else {
        if require_postgres_tests() {
            panic!(
                "postgres integration tests require {TEST_DATABASE_URL_ENV} when {REQUIRE_POSTGRES_TESTS_ENV}=1"
            );
        }
        eprintln!(
            "skipping postgres integration test: set {TEST_DATABASE_URL_ENV} (example: postgres://postgres:postgres@localhost:5432/postgres)"
        );
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    // Insert 5 tasks into a single run
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database.database_url)
        .await?;

    let run_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();

    sqlx::query(
        r#"
        INSERT INTO runs (run_id, objective, state, safe_mode, correlation_id, config_snapshot)
        VALUES ($1, $2, 'RUN_ACTIVE', 'execute', $3, '{}'::jsonb)
        "#,
    )
    .bind(run_id)
    .bind("concurrent claim test run")
    .bind(correlation_id)
    .execute(&pool)
    .await?;

    let task_ids: Vec<Uuid> = (0..5).map(|_| Uuid::now_v7()).collect();

    for (i, task_id) in task_ids.iter().enumerate() {
        sqlx::query(
            r#"
            INSERT INTO tasks (
                task_id, run_id, task_key, description, state, command_class,
                attempt_no, max_attempts, blocker_code, correlation_id, priority
            )
            VALUES ($1, $2, $3, $4, 'TASK_READY', 'io', 1, 3, NULL, $5, 1)
            "#,
        )
        .bind(task_id)
        .bind(run_id)
        .bind(format!("concurrent-task-{i}"))
        .bind(format!("concurrent task {i}"))
        .bind(correlation_id)
        .execute(&pool)
        .await?;
    }

    let queue = PostgresTaskQueue::new(&database.database_url)?;
    for &task_id in &task_ids {
        queue.enqueue(task_id, run_id, 1, CommandClass::Io, None)?;
    }

    // Have 3 workers each claim up to 5 tasks concurrently
    let mut handles = Vec::new();
    for worker_idx in 0..3 {
        let q = queue.clone();
        handles.push(std::thread::spawn(move || {
            let worker_id = format!("worker-{worker_idx}");
            let claim_request = ClaimRequest::new(&worker_id, 5, Duration::seconds(60));
            q.claim(&claim_request, &ConcurrencyConfig::default())
                .expect("claim should not error")
        }));
    }

    let mut all_claimed_task_ids = Vec::new();
    for handle in handles {
        let entries = handle.join().expect("thread should not panic");
        for entry in &entries {
            all_claimed_task_ids.push(entry.task_id);
        }
    }

    // Exactly 5 tasks should be claimed in total (no duplicates)
    assert_eq!(
        all_claimed_task_ids.len(),
        5,
        "total claims must equal total tasks"
    );

    let unique: HashSet<Uuid> = all_claimed_task_ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        5,
        "all claimed task IDs must be unique (no duplicate lease)"
    );

    // All 5 original task_ids must be present
    for task_id in &task_ids {
        assert!(
            unique.contains(task_id),
            "task {task_id} must be claimed by exactly one worker"
        );
    }

    assert_eq!(queue.pending_count(), 0, "no tasks should remain pending");

    database.drop().await?;
    Ok(())
}

#[tokio::test]
async fn claim_paths_execute_without_sql_syntax_errors_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url() else {
        if require_postgres_tests() {
            panic!(
                "postgres integration tests require {TEST_DATABASE_URL_ENV} when {REQUIRE_POSTGRES_TESTS_ENV}=1"
            );
        }
        eprintln!(
            "skipping postgres integration test: set {TEST_DATABASE_URL_ENV} (example: postgres://postgres:postgres@localhost:5432/postgres)"
        );
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database.database_url)
        .await?;
    let queue = PostgresTaskQueue::new(&database.database_url)?;

    let run_a = Uuid::now_v7();
    let run_b = Uuid::now_v7();
    let correlation_a = Uuid::now_v7();
    let correlation_b = Uuid::now_v7();

    sqlx::query(
        r#"
        INSERT INTO runs (run_id, objective, state, safe_mode, correlation_id, config_snapshot)
        VALUES ($1, $2, 'RUN_ACTIVE', 'execute', $3, '{}'::jsonb)
        "#,
    )
    .bind(run_a)
    .bind("claim path test run a")
    .bind(correlation_a)
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO runs (run_id, objective, state, safe_mode, correlation_id, config_snapshot)
        VALUES ($1, $2, 'RUN_ACTIVE', 'execute', $3, '{}'::jsonb)
        "#,
    )
    .bind(run_b)
    .bind("claim path test run b")
    .bind(correlation_b)
    .execute(&pool)
    .await?;

    let task_a = Uuid::now_v7();
    let task_b1 = Uuid::now_v7();
    let task_b2 = Uuid::now_v7();

    sqlx::query(
        r#"
        INSERT INTO tasks (
            task_id, run_id, task_key, description, state, command_class,
            attempt_no, max_attempts, blocker_code, correlation_id, priority
        )
        VALUES ($1, $2, $3, $4, 'TASK_READY', 'git', 1, 3, NULL, $5, $6)
        "#,
    )
    .bind(task_a)
    .bind(run_a)
    .bind("claim-path-task-a")
    .bind("claim path task a")
    .bind(correlation_a)
    .bind(50_i32)
    .execute(&pool)
    .await?;

    for (idx, task_id) in [task_b1, task_b2].iter().enumerate() {
        sqlx::query(
            r#"
            INSERT INTO tasks (
                task_id, run_id, task_key, description, state, command_class,
                attempt_no, max_attempts, blocker_code, correlation_id, priority
            )
            VALUES ($1, $2, $3, $4, 'TASK_READY', 'git', 1, 3, NULL, $5, $6)
            "#,
        )
        .bind(task_id)
        .bind(run_b)
        .bind(format!("claim-path-task-b-{idx}"))
        .bind(format!("claim path task b {idx}"))
        .bind(correlation_b)
        .bind(1_i32)
        .execute(&pool)
        .await?;
    }

    queue.enqueue(task_a, run_a, 50, CommandClass::Git, None)?;
    queue.enqueue(task_b1, run_b, 1, CommandClass::Git, None)?;
    queue.enqueue(task_b2, run_b, 1, CommandClass::Git, None)?;
    let mut unconstrained_git_caps = ConcurrencyConfig::default();
    unconstrained_git_caps.git_cap = 8;

    // Unscoped claim path (allowed_run_ids = None).
    let unscoped = queue.claim(
        &ClaimRequest::new("worker-unscoped", 1, Duration::seconds(30)),
        &unconstrained_git_caps,
    )?;
    assert_eq!(unscoped.len(), 1);
    assert_eq!(unscoped[0].run_id, run_a);

    // Scoped claim path (allowed_run_ids = Some(...)).
    let scoped = queue.claim(
        &ClaimRequest::new("worker-scoped", 5, Duration::seconds(30))
            .with_allowed_run_ids(vec![run_b]),
        &unconstrained_git_caps,
    )?;
    assert_eq!(scoped.len(), 2);
    assert!(scoped.iter().all(|entry| entry.run_id == run_b));

    database.drop().await?;
    Ok(())
}

/// Prove that replaying events via idempotency keys produces no duplicate records.
///
/// This validates the replay/restart safety invariant: after a crash, re-appending
/// the same events with the same idempotency keys does not create duplicates, and
/// querying the store returns exactly one event per key.
#[tokio::test]
async fn replay_idempotency_no_duplicate_terminal_transition_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url() else {
        if require_postgres_tests() {
            panic!(
                "postgres integration tests require {TEST_DATABASE_URL_ENV} when {REQUIRE_POSTGRES_TESTS_ENV}=1"
            );
        }
        eprintln!(
            "skipping postgres integration test: set {TEST_DATABASE_URL_ENV} (example: postgres://postgres:postgres@localhost:5432/postgres)"
        );
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;

    let store = PostgresEventStore::new(&database.database_url)?;
    let task_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();

    // Simulate a task lifecycle: started → completed (terminal transition)
    let started_event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Task,
        entity_id: task_id.to_string(),
        event_type: "task.started".to_string(),
        payload: serde_json::json!({
            "from": "TaskReady",
            "to": "TaskRunning",
        }),
        correlation_id,
        causation_id: None,
        actor: "replay-test".to_string(),
        idempotency_key: Some(format!("{task_id}:1:started")),
    };

    let completed_event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Task,
        entity_id: task_id.to_string(),
        event_type: "task.completed".to_string(),
        payload: serde_json::json!({
            "from": "TaskRunning",
            "to": "TaskComplete",
        }),
        correlation_id,
        causation_id: None,
        actor: "replay-test".to_string(),
        idempotency_key: Some(format!("{task_id}:1:completed")),
    };

    // First append (normal execution)
    store.append(started_event.clone())?;
    store.append(completed_event.clone())?;
    assert_eq!(store.len(), 2, "initial append should produce 2 events");

    // Simulate restart: re-append with same idempotency keys but new event IDs
    // (as would happen if the scheduler replayed after a crash)
    let replay_started = Event {
        event_id: Uuid::now_v7(), // new event_id, same idempotency_key
        ..started_event.clone()
    };
    let replay_completed = Event {
        event_id: Uuid::now_v7(), // new event_id, same idempotency_key
        ..completed_event.clone()
    };

    // Idempotency key dedup should reject these
    let result_started = store.append(replay_started);
    assert!(
        result_started.is_err(),
        "replay of started event must be rejected by idempotency key"
    );

    let result_completed = store.append(replay_completed);
    assert!(
        result_completed.is_err(),
        "replay of completed event must be rejected by idempotency key"
    );

    // Store still contains exactly 2 events (no duplicates)
    assert_eq!(
        store.len(),
        2,
        "store must contain exactly 2 events after replay attempt"
    );

    // Query by entity confirms only one terminal transition exists
    use yarli_store::event_store::EventQuery;
    let task_events = store.query(&EventQuery {
        entity_type: Some(EntityType::Task),
        entity_id: Some(task_id.to_string()),
        correlation_id: None,
        event_type: Some("task.completed".to_string()),
        limit: None,
        after_event_id: None,
    })?;
    assert_eq!(
        task_events.len(),
        1,
        "exactly one terminal transition must exist for task"
    );

    database.drop().await?;
    Ok(())
}

struct TestDatabase {
    admin_database_url: String,
    database_name: String,
    database_url: String,
}

impl TestDatabase {
    async fn create(admin_database_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(admin_database_url)
            .await?;

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
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_database_url)
            .await?;

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
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

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

fn require_postgres_tests() -> bool {
    env::var(REQUIRE_POSTGRES_TESTS_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}
