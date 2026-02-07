use std::env;
use std::str::FromStr;

use chrono::Duration;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::ConnectOptions;
use uuid::Uuid;
use yarli_core::domain::CommandClass;
use yarli_queue::{ClaimRequest, ConcurrencyConfig, PostgresTaskQueue, TaskQueue};
use yarli_store::{MIGRATION_0001_INIT, MIGRATION_0002_INDEXES};

const TEST_DATABASE_URL_ENV: &str = "YARLI_TEST_DATABASE_URL";

#[tokio::test]
async fn enqueue_and_claim_lease_roundtrip_against_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) = test_database_url() else {
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
