use std::env;
use std::str::FromStr;

use chrono::Utc;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::ConnectOptions;
use uuid::Uuid;
use yarli_cli::yarli_core::domain::{EntityType, Event};
use yarli_cli::yarli_store::event_store::EventQuery;
use yarli_cli::yarli_store::{
    EventStore, PostgresEventStore, MIGRATION_0001_INIT, MIGRATION_0002_INDEXES,
    MIGRATION_0003_RUN_DRAINED_STATE,
};

const TEST_DATABASE_URL_ENV: &str = "YARLI_TEST_DATABASE_URL";
const REQUIRE_POSTGRES_TESTS_ENV: &str = "YARLI_REQUIRE_POSTGRES_TESTS";

#[tokio::test]
async fn append_and_query_roundtrip_against_postgres() -> Result<(), Box<dyn std::error::Error>> {
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
    let correlation_id = Uuid::now_v7();
    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Run,
        entity_id: run_id.to_string(),
        event_type: "run.activated".to_string(),
        payload: serde_json::json!({
            "from": "RunOpen",
            "to": "RunActive",
        }),
        correlation_id,
        causation_id: None,
        actor: "integration-test".to_string(),
        idempotency_key: Some(format!("store-integration:run.activated:{run_id}")),
    };

    store.append(event.clone())?;

    let run_events = store.query(&EventQuery::by_entity(EntityType::Run, run_id.to_string()))?;
    assert_eq!(run_events.len(), 1);
    assert_eq!(run_events[0].event_type, "run.activated");
    assert_eq!(run_events[0].payload["to"], "RunActive");

    let loaded = store.get(event.event_id)?;
    assert_eq!(loaded.event_id, event.event_id);
    assert_eq!(loaded.entity_id, run_id.to_string());
    assert_eq!(loaded.correlation_id, correlation_id);

    assert_eq!(store.len(), 1);

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
