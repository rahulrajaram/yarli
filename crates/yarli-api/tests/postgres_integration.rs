use std::{env, str::FromStr, sync::Arc};

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use chrono::Utc;
use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::ConnectOptions;
use tower::ServiceExt;
use uuid::Uuid;

use yarli_api::router;
use yarli_core::domain::{EntityType, Event};
use yarli_store::{
    EventStore, PostgresEventStore, MIGRATION_0001_INIT, MIGRATION_0002_INDEXES,
    MIGRATION_0003_RUN_DRAINED_STATE,
};

const TEST_DATABASE_URL_ENV: &str = "YARLI_TEST_DATABASE_URL";
const REQUIRE_POSTGRES_TESTS_ENV: &str = "YARLI_REQUIRE_POSTGRES_TESTS";

#[tokio::test]
async fn run_and_task_status_reflect_writes_from_postgres() -> Result<(), Box<dyn std::error::Error>>
{
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
    let run_id = Uuid::now_v7();
    let task_id = Uuid::now_v7();
    let correlation_id = Uuid::now_v7();
    let now = Utc::now();

    store.append(make_event(
        EntityType::Run,
        run_id.to_string(),
        "run.config_snapshot",
        correlation_id,
        now,
        json!({"objective": "prove read-your-writes on Postgres"}),
    ))?;

    store.append(make_event(
        EntityType::Run,
        run_id.to_string(),
        "run.activated",
        correlation_id,
        now + chrono::Duration::seconds(1),
        json!({"to": "RunActive"}),
    ))?;

    store.append(make_event(
        EntityType::Task,
        task_id.to_string(),
        "task.completed",
        correlation_id,
        now + chrono::Duration::seconds(2),
        json!({"to": "TaskComplete"}),
    ))?;

    let app = router(Arc::new(store));

    let run_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{run_id}/status"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(run_response.status(), StatusCode::OK);
    let body = to_bytes(run_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(payload["run_id"], json!(run_id));
    assert_eq!(payload["state"], "RunActive");
    assert_eq!(payload["last_event_type"], "run.activated");
    assert_eq!(payload["correlation_id"], json!(correlation_id));
    assert_eq!(payload["objective"], "prove read-your-writes on Postgres");
    assert_eq!(payload["task_summary"]["total"], 1);
    assert_eq!(payload["task_summary"]["complete"], 1);

    let task_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/tasks/{task_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(task_response.status(), StatusCode::OK);
    let body = to_bytes(task_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(payload["task_id"], json!(task_id));
    assert_eq!(payload["state"], "TaskComplete");
    assert_eq!(payload["last_event_type"], "task.completed");
    assert_eq!(payload["correlation_id"], json!(correlation_id));

    database.drop().await?;
    Ok(())
}

fn make_event(
    entity_type: EntityType,
    entity_id: String,
    event_type: &str,
    correlation_id: Uuid,
    occurred_at: chrono::DateTime<Utc>,
    payload: serde_json::Value,
) -> Event {
    Event {
        event_id: Uuid::now_v7(),
        occurred_at,
        entity_type,
        entity_id,
        event_type: event_type.to_string(),
        payload,
        correlation_id,
        causation_id: None,
        actor: "integration-test".to_string(),
        idempotency_key: Some(format!("api-integration:{entity_type:?}:{event_type}")),
    }
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

        let database_name = format!("yarli_test_api_{}", Uuid::now_v7().simple());
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
        .chain(MIGRATION_0003_RUN_DRAINED_STATE.split(';'))
    {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        sqlx::query(statement).execute(&pool).await?;
    }

    Ok(())
}
