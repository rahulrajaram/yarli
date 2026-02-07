use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use chrono::Utc;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::ConnectOptions;
use tempfile::TempDir;
use uuid::Uuid;
use yarli_store::{MIGRATION_0001_INIT, MIGRATION_0002_INDEXES};

const TEST_DATABASE_URL_ENV: &str = "YARLI_TEST_DATABASE_URL";
const REQUIRE_POSTGRES_TESTS_ENV: &str = "YARLI_REQUIRE_POSTGRES_TESTS";

#[tokio::test]
async fn merge_request_and_status_roundtrip_against_postgres(
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

fn parse_merge_id(output: &str) -> Option<Uuid> {
    output.lines().find_map(|line| {
        line.strip_prefix("Merge ID: ")
            .and_then(|value| value.trim().parse::<Uuid>().ok())
    })
}

async fn seed_run_event(database_url: &str) -> Result<Uuid, Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

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

fn yarli_binary_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    match env::var_os("CARGO_BIN_EXE_yarli") {
        Some(path) => Ok(PathBuf::from(path)),
        None => {
            let current_exe = env::current_exe()?;
            let debug_dir = current_exe
                .parent()
                .and_then(|path| path.parent())
                .ok_or("failed to derive target/debug directory from current test executable path")?;
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
