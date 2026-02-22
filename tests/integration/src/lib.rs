// Shared integration test helpers.
// This is a lib crate so that integration tests under tests/ can import it.

use std::env;
use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool};
use uuid::Uuid;
use yarli_store::{MIGRATION_0001_INIT, MIGRATION_0002_INDEXES};

pub const TEST_DATABASE_URL_ENV: &str = "YARLI_TEST_DATABASE_URL";
pub const REQUIRE_POSTGRES_TESTS_ENV: &str = "YARLI_REQUIRE_POSTGRES_TESTS";
const TEST_DATABASE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const LOCAL_POSTGRES_BOOTSTRAP_HINT: &str =
    "docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16";

pub struct TestDatabase {
    admin_database_url: String,
    pub database_name: String,
    pub database_url: String,
}

impl TestDatabase {
    pub async fn create(admin_database_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let admin_pool =
            connect_postgres(admin_database_url, "TestDatabase::create(admin)").await?;

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

    pub async fn drop(self) -> Result<(), Box<dyn std::error::Error>> {
        let admin_pool =
            connect_postgres(&self.admin_database_url, "TestDatabase::drop(admin)").await?;

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

pub async fn apply_migrations(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
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

pub fn test_database_url() -> Option<String> {
    env::var(TEST_DATABASE_URL_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub fn test_database_url_for_test(test_name: &str) -> Option<String> {
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

pub async fn connect_postgres(
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

    let connect = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url);
    match tokio::time::timeout(timeout, connect).await {
        Ok(Ok(pool)) => Ok(pool),
        Ok(Err(err)) => {
            Err(format!("[{context}] Postgres connect failed for {redacted_url}: {err}").into())
        }
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
