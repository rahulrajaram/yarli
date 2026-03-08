//! Postgres-backed task queue implementation.
//!
//! Implements `TaskQueue` semantics over the `task_queue` table from migration
//! `0001_init.sql`. Claiming uses `FOR UPDATE SKIP LOCKED` to avoid workers
//! contending on the same pending rows.

use std::collections::HashMap;
use std::future::Future;
use std::thread;

use crate::yarli_core::domain::{CommandClass, RunId, TaskId};
use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use tokio::runtime::{Builder, Handle, RuntimeFlavor};
use tracing::warn;
use uuid::Uuid;

use crate::yarli_queue::error::QueueError;
use crate::yarli_queue::queue::{
    ClaimRequest, ConcurrencyConfig, QueueEntry, QueueStats, QueueStatus, TaskQueue,
};

/// Claim candidates SQL without run_id filter (used when allowed_run_ids is None).
const CLAIM_CANDIDATES_SQL: &str = r#"
    SELECT
        queue_id,
        run_id,
        command_class
    FROM task_queue
    WHERE status = 'pending'
      AND available_at <= $1
    ORDER BY (priority + (GREATEST(0, EXTRACT(EPOCH FROM ($1 - available_at))::BIGINT / 60))) DESC, priority DESC, available_at ASC, queue_id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
"#;

/// Claim candidates SQL with run_id filter (used when allowed_run_ids is Some).
/// The SQL LIMIT would otherwise hide the current run's rows behind stale
/// pending rows from prior runs, causing zero-claim stalls.
const CLAIM_CANDIDATES_SCOPED_SQL: &str = r#"
    SELECT
        queue_id,
        run_id,
        command_class
    FROM task_queue
    WHERE status = 'pending'
      AND available_at <= $1
      AND run_id = ANY($3::uuid[])
    ORDER BY (priority + (GREATEST(0, EXTRACT(EPOCH FROM ($1 - available_at))::BIGINT / 60))) DESC, priority DESC, available_at ASC, queue_id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
"#;

/// Postgres-backed queue implementation.
#[derive(Debug, Clone)]
pub struct PostgresTaskQueue {
    pool: PgPool,
}

impl PostgresTaskQueue {
    /// Create a queue backed by a lazily-connected `PgPool`.
    pub fn new(database_url: &str) -> Result<Self, QueueError> {
        let pool = PgPoolOptions::new()
            .connect_lazy(database_url)
            .map_err(|error| QueueError::Database(error.to_string()))?;
        Ok(Self { pool })
    }

    /// Construct from an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn run_async<T, Fut>(&self, fut: Fut) -> Result<T, QueueError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, QueueError>> + Send + 'static,
    {
        match Handle::try_current() {
            Ok(handle) => match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(fut)),
                RuntimeFlavor::CurrentThread => thread::spawn(move || {
                    let runtime = Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| QueueError::Runtime(error.to_string()))?;
                    runtime.block_on(fut)
                })
                .join()
                .map_err(|_| QueueError::Runtime("postgres operation panicked".to_string()))?,
                _ => {
                    let runtime = Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| QueueError::Runtime(error.to_string()))?;
                    runtime.block_on(fut)
                }
            },
            Err(_) => {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| QueueError::Runtime(error.to_string()))?;
                runtime.block_on(fut)
            }
        }
    }
}

impl TaskQueue for PostgresTaskQueue {
    fn enqueue(
        &self,
        task_id: TaskId,
        run_id: RunId,
        priority: u32,
        command_class: CommandClass,
        available_at: Option<DateTime<Utc>>,
    ) -> Result<Uuid, QueueError> {
        let pool = self.pool.clone();
        self.run_async(async move {
            let now = Utc::now();
            let queue_id = Uuid::now_v7();
            let result = sqlx::query(
                r#"
                INSERT INTO task_queue (
                    queue_id,
                    task_id,
                    run_id,
                    priority,
                    available_at,
                    attempt_no,
                    command_class,
                    status,
                    lease_owner,
                    lease_expires_at,
                    last_heartbeat,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, 1, $6, 'pending',
                    NULL, NULL, NULL, $7, $7
                )
                "#,
            )
            .bind(queue_id)
            .bind(task_id)
            .bind(run_id)
            .bind(priority as i32)
            .bind(available_at.unwrap_or(now))
            .bind(command_class_to_db(command_class))
            .bind(now)
            .execute(&pool)
            .await;

            match result {
                Ok(_) => Ok(queue_id),
                Err(sqlx::Error::Database(db_error))
                    if db_error.code().as_deref() == Some("23505") =>
                {
                    match classify_unique_violation(db_error.constraint()) {
                        UniqueViolation::ActiveTask => Err(QueueError::DuplicateTask(task_id)),
                        UniqueViolation::Unknown => {
                            Err(QueueError::Database(db_error.message().to_string()))
                        }
                    }
                }
                Err(error) => Err(QueueError::Database(error.to_string())),
            }
        })
    }

    fn claim(
        &self,
        request: &ClaimRequest,
        config: &ConcurrencyConfig,
    ) -> Result<Vec<QueueEntry>, QueueError> {
        let pool = self.pool.clone();
        let worker_id = request.worker_id.clone();
        let limit = request.limit;
        let lease_ttl = request.lease_ttl;
        let config = config.clone();
        let allowed_run_ids = request.allowed_run_ids.clone();

        self.run_async(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }

            let mut tx = pool
                .begin()
                .await
                .map_err(|error| QueueError::Database(error.to_string()))?;
            let now = Utc::now();

            let leased_by_run_rows = sqlx::query(
                r#"
                SELECT run_id, COUNT(*)::bigint AS leased_count
                FROM task_queue
                WHERE status = 'leased'
                GROUP BY run_id
                "#,
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            let mut leased_by_run: HashMap<RunId, usize> = HashMap::new();
            for row in leased_by_run_rows {
                let run_id: RunId = row
                    .try_get("run_id")
                    .map_err(|error| QueueError::Database(error.to_string()))?;
                let count: i64 = row
                    .try_get("leased_count")
                    .map_err(|error| QueueError::Database(error.to_string()))?;
                leased_by_run.insert(run_id, count_to_usize(count));
            }

            let leased_by_class_rows = sqlx::query(
                r#"
                SELECT command_class, COUNT(*)::bigint AS leased_count
                FROM task_queue
                WHERE status = 'leased'
                GROUP BY command_class
                "#,
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            let mut leased_by_class: HashMap<CommandClass, usize> = HashMap::new();
            for row in leased_by_class_rows {
                let class_raw: String = row
                    .try_get("command_class")
                    .map_err(|error| QueueError::Database(error.to_string()))?;
                let class = command_class_from_db(&class_raw)?;
                let count: i64 = row
                    .try_get("leased_count")
                    .map_err(|error| QueueError::Database(error.to_string()))?;
                leased_by_class.insert(class, count_to_usize(count));
            }

            // Fetch a wider candidate window so per-run/per-class caps can be enforced
            // in-memory while keeping claim+lease assignment atomic in one transaction.
            let candidate_window = limit.saturating_mul(8).max(limit).min(1024);
            let candidate_rows = if let Some(ref run_ids) = allowed_run_ids {
                // Push run_id filter into SQL to avoid stale rows from prior runs
                // consuming the entire LIMIT window.
                sqlx::query(CLAIM_CANDIDATES_SCOPED_SQL)
                    .bind(now)
                    .bind(candidate_window as i64)
                    .bind(run_ids)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|error| QueueError::Database(error.to_string()))?
            } else {
                sqlx::query(CLAIM_CANDIDATES_SQL)
                    .bind(now)
                    .bind(candidate_window as i64)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|error| QueueError::Database(error.to_string()))?
            };

            let mut selected_ids = Vec::new();
            for row in candidate_rows {
                if selected_ids.len() >= limit {
                    break;
                }

                let queue_id: Uuid = row
                    .try_get("queue_id")
                    .map_err(|error| QueueError::Database(error.to_string()))?;
                let run_id: RunId = row
                    .try_get("run_id")
                    .map_err(|error| QueueError::Database(error.to_string()))?;
                let command_class_raw: String = row
                    .try_get("command_class")
                    .map_err(|error| QueueError::Database(error.to_string()))?;
                let command_class = command_class_from_db(&command_class_raw)?;

                // Filter by allowed run IDs if specified.
                if let Some(ref allowed) = allowed_run_ids {
                    if !allowed.contains(&run_id) {
                        continue;
                    }
                }

                let run_leased = leased_by_run.get(&run_id).copied().unwrap_or(0);
                if run_leased >= config.per_run_cap {
                    continue;
                }

                let class_leased = leased_by_class.get(&command_class).copied().unwrap_or(0);
                if class_leased >= config.cap_for(command_class) {
                    continue;
                }

                selected_ids.push(queue_id);
                leased_by_run.insert(run_id, run_leased + 1);
                leased_by_class.insert(command_class, class_leased + 1);
            }

            if selected_ids.is_empty() {
                tx.commit()
                    .await
                    .map_err(|error| QueueError::Database(error.to_string()))?;
                return Ok(Vec::new());
            }

            let lease_expires_at = now + lease_ttl;
            let updated_rows = sqlx::query(
                r#"
                UPDATE task_queue
                SET status = 'leased',
                    lease_owner = $1,
                    lease_expires_at = $2,
                    last_heartbeat = $3,
                    updated_at = $3
                WHERE queue_id = ANY($4::uuid[])
                RETURNING
                    queue_id,
                    task_id,
                    run_id,
                    priority,
                    available_at,
                    attempt_no,
                    command_class,
                    status,
                    lease_owner,
                    lease_expires_at,
                    last_heartbeat,
                    created_at,
                    updated_at
                "#,
            )
            .bind(&worker_id)
            .bind(lease_expires_at)
            .bind(now)
            .bind(&selected_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            tx.commit()
                .await
                .map_err(|error| QueueError::Database(error.to_string()))?;

            let mut updated_by_id = HashMap::<Uuid, QueueEntry>::new();
            for row in updated_rows {
                let entry = row_to_queue_entry(row)?;
                updated_by_id.insert(entry.queue_id, entry);
            }

            let mut claimed = Vec::with_capacity(selected_ids.len());
            for queue_id in selected_ids {
                if let Some(entry) = updated_by_id.remove(&queue_id) {
                    claimed.push(entry);
                }
            }

            Ok(claimed)
        })
    }

    fn heartbeat(
        &self,
        queue_id: Uuid,
        worker_id: &str,
        lease_ttl: Duration,
    ) -> Result<(), QueueError> {
        let pool = self.pool.clone();
        let worker_id = worker_id.to_string();
        self.run_async(async move {
            let mut tx = pool
                .begin()
                .await
                .map_err(|error| QueueError::Database(error.to_string()))?;

            let row = sqlx::query(
                r#"
                SELECT status, lease_owner, lease_expires_at
                FROM task_queue
                WHERE queue_id = $1
                FOR UPDATE
                "#,
            )
            .bind(queue_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            let row = row.ok_or(QueueError::NotFound(queue_id))?;
            let status_raw: String = row
                .try_get("status")
                .map_err(|error| QueueError::Database(error.to_string()))?;
            let status = queue_status_from_db(&status_raw)?;
            if status != QueueStatus::Leased {
                return Err(QueueError::InvalidStatus {
                    entry_id: queue_id,
                    expected: "leased",
                    actual: format!("{status:?}"),
                });
            }

            let lease_owner: Option<String> = row
                .try_get("lease_owner")
                .map_err(|error| QueueError::Database(error.to_string()))?;
            let actual_owner = lease_owner.unwrap_or_default();
            if actual_owner != worker_id {
                return Err(QueueError::LeaseOwnerMismatch {
                    entry_id: queue_id,
                    expected: worker_id,
                    actual: actual_owner,
                });
            }

            let lease_expires_at: Option<DateTime<Utc>> = row
                .try_get("lease_expires_at")
                .map_err(|error| QueueError::Database(error.to_string()))?;
            let now = Utc::now();
            if let Some(expires_at) = lease_expires_at {
                if now > expires_at {
                    return Err(QueueError::LeaseExpired(queue_id));
                }
            }

            sqlx::query(
                r#"
                UPDATE task_queue
                SET lease_expires_at = $2,
                    last_heartbeat = $1,
                    updated_at = $1
                WHERE queue_id = $3
                "#,
            )
            .bind(now)
            .bind(now + lease_ttl)
            .bind(queue_id)
            .execute(&mut *tx)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            tx.commit()
                .await
                .map_err(|error| QueueError::Database(error.to_string()))?;
            Ok(())
        })
    }

    fn complete(&self, queue_id: Uuid, worker_id: &str) -> Result<(), QueueError> {
        let pool = self.pool.clone();
        let worker_id = worker_id.to_string();
        self.run_async(async move {
            transition_leased_entry_async(&pool, queue_id, &worker_id, QueueStatus::Completed).await
        })
    }

    fn fail(&self, queue_id: Uuid, worker_id: &str) -> Result<(), QueueError> {
        let pool = self.pool.clone();
        let worker_id = worker_id.to_string();
        self.run_async(async move {
            transition_leased_entry_async(&pool, queue_id, &worker_id, QueueStatus::Failed).await
        })
    }

    fn override_priority(&self, task_id: TaskId, priority: u32) -> Result<(), QueueError> {
        let pool = self.pool.clone();
        self.run_async(async move {
            let now = Utc::now();
            let result = sqlx::query(
                r#"
                UPDATE task_queue
                SET priority = $1,
                    updated_at = $2
                WHERE task_id = $3
                "#,
            )
            .bind(priority as i32)
            .bind(now)
            .bind(task_id)
            .execute(&pool)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            if result.rows_affected() == 0 {
                return Err(QueueError::NotFound(task_id));
            }

            Ok(())
        })
    }

    fn cancel(&self, queue_id: Uuid) -> Result<(), QueueError> {
        let pool = self.pool.clone();
        self.run_async(async move {
            let mut tx = pool
                .begin()
                .await
                .map_err(|error| QueueError::Database(error.to_string()))?;
            let row = sqlx::query(
                r#"
                SELECT status
                FROM task_queue
                WHERE queue_id = $1
                FOR UPDATE
                "#,
            )
            .bind(queue_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            let row = row.ok_or(QueueError::NotFound(queue_id))?;
            let status_raw: String = row
                .try_get("status")
                .map_err(|error| QueueError::Database(error.to_string()))?;
            let status = queue_status_from_db(&status_raw)?;

            match status {
                QueueStatus::Pending | QueueStatus::Leased => {
                    sqlx::query(
                        r#"
                        UPDATE task_queue
                        SET status = 'cancelled',
                            updated_at = $1
                        WHERE queue_id = $2
                        "#,
                    )
                    .bind(Utc::now())
                    .bind(queue_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| QueueError::Database(error.to_string()))?;

                    tx.commit()
                        .await
                        .map_err(|error| QueueError::Database(error.to_string()))?;
                    Ok(())
                }
                _ => Err(QueueError::InvalidStatus {
                    entry_id: queue_id,
                    expected: "pending or leased",
                    actual: format!("{status:?}"),
                }),
            }
        })
    }

    fn reclaim_stale(&self, grace_period: Duration) -> Result<usize, QueueError> {
        let pool = self.pool.clone();
        self.run_async(async move {
            let now = Utc::now();
            let cutoff = now - grace_period;
            let result = sqlx::query(
                r#"
                UPDATE task_queue
                SET status = 'pending',
                    attempt_no = attempt_no + 1,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_heartbeat = NULL,
                    updated_at = $1
                WHERE status = 'leased'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < $2
                "#,
            )
            .bind(now)
            .bind(cutoff)
            .execute(&pool)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            Ok(result.rows_affected() as usize)
        })
    }

    fn stats(&self) -> QueueStats {
        let pool = self.pool.clone();
        match self.run_async(async move {
            let row = sqlx::query(
                r#"
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending')::bigint AS pending,
                    COUNT(*) FILTER (WHERE status = 'leased')::bigint AS leased,
                    COUNT(*) FILTER (WHERE status = 'completed')::bigint AS completed,
                    COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed,
                    COUNT(*) FILTER (WHERE status = 'cancelled')::bigint AS cancelled
                FROM task_queue
                "#,
            )
            .fetch_one(&pool)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            Ok(QueueStats {
                pending: count_to_usize(
                    row.try_get("pending")
                        .map_err(|error| QueueError::Database(error.to_string()))?,
                ),
                leased: count_to_usize(
                    row.try_get("leased")
                        .map_err(|error| QueueError::Database(error.to_string()))?,
                ),
                completed: count_to_usize(
                    row.try_get("completed")
                        .map_err(|error| QueueError::Database(error.to_string()))?,
                ),
                failed: count_to_usize(
                    row.try_get("failed")
                        .map_err(|error| QueueError::Database(error.to_string()))?,
                ),
                cancelled: count_to_usize(
                    row.try_get("cancelled")
                        .map_err(|error| QueueError::Database(error.to_string()))?,
                ),
            })
        }) {
            Ok(stats) => stats,
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to compute queue stats; returning defaults"
                );
                QueueStats::default()
            }
        }
    }

    fn entries(&self) -> Vec<QueueEntry> {
        let pool = self.pool.clone();
        match self.run_async(async move {
            let rows = sqlx::query(
                "SELECT queue_id, task_id, run_id, priority, available_at, attempt_no, command_class, status, lease_owner, lease_expires_at, last_heartbeat, created_at, updated_at FROM task_queue ORDER BY created_at DESC, queue_id ASC"
            )
                .fetch_all(&pool)
                .await
                .map_err(|error| QueueError::Database(error.to_string()))?;

            let entries = rows
                .into_iter()
                .filter_map(|row| row_to_queue_entry(row).ok())
                .collect::<Vec<_>>();
            Ok(entries)
        }) {
            Ok(entries) => entries,
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to read queue entries; returning empty list"
                );
                Vec::new()
            }
        }
    }

    fn leased_count_for_run(&self, run_id: RunId) -> usize {
        let pool = self.pool.clone();
        match self.run_async(async move {
            let count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::bigint
                FROM task_queue
                WHERE status = 'leased'
                  AND run_id = $1
                "#,
            )
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            Ok(count_to_usize(count))
        }) {
            Ok(count) => count,
            Err(error) => {
                warn!(
                    error = %error,
                    run_id = %run_id,
                    "failed to compute leased count by run; returning zero"
                );
                0
            }
        }
    }

    fn leased_count_for_class(&self, class: CommandClass) -> usize {
        let pool = self.pool.clone();
        let class_db = command_class_to_db(class).to_string();
        match self.run_async(async move {
            let count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::bigint
                FROM task_queue
                WHERE status = 'leased'
                  AND command_class = $1
                "#,
            )
            .bind(class_db)
            .fetch_one(&pool)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            Ok(count_to_usize(count))
        }) {
            Ok(count) => count,
            Err(error) => {
                warn!(
                    error = %error,
                    class = ?class,
                    "failed to compute leased count by command class; returning zero"
                );
                0
            }
        }
    }

    fn pending_count(&self) -> usize {
        let pool = self.pool.clone();
        match self.run_async(async move {
            let count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::bigint
                FROM task_queue
                WHERE status = 'pending'
                "#,
            )
            .fetch_one(&pool)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;
            Ok(count_to_usize(count))
        }) {
            Ok(count) => count,
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to compute pending count; returning zero"
                );
                0
            }
        }
    }

    fn cancel_for_run(&self, run_id: RunId) -> Result<usize, QueueError> {
        let pool = self.pool.clone();
        self.run_async(async move {
            let result = sqlx::query(
                r#"
                UPDATE task_queue
                SET status = 'cancelled',
                    updated_at = $1
                WHERE run_id = $2
                  AND status IN ('pending', 'leased')
                "#,
            )
            .bind(Utc::now())
            .bind(run_id)
            .execute(&pool)
            .await
            .map_err(|error| QueueError::Database(error.to_string()))?;

            Ok(result.rows_affected() as usize)
        })
    }

    fn cancel_stale_runs(&self, active_run_ids: &[RunId]) -> Result<usize, QueueError> {
        let pool = self.pool.clone();
        let active_ids = active_run_ids.to_vec();
        self.run_async(async move {
            let result = if active_ids.is_empty() {
                // No active runs — cancel ALL pending/leased rows.
                sqlx::query(
                    r#"
                    UPDATE task_queue
                    SET status = 'cancelled',
                        updated_at = $1
                    WHERE status IN ('pending', 'leased')
                    "#,
                )
                .bind(Utc::now())
                .execute(&pool)
                .await
                .map_err(|error| QueueError::Database(error.to_string()))?
            } else {
                // Cancel pending/leased rows NOT belonging to active runs.
                sqlx::query(
                    r#"
                    UPDATE task_queue
                    SET status = 'cancelled',
                        updated_at = $1
                    WHERE status IN ('pending', 'leased')
                      AND run_id != ALL($2::uuid[])
                    "#,
                )
                .bind(Utc::now())
                .bind(&active_ids)
                .execute(&pool)
                .await
                .map_err(|error| QueueError::Database(error.to_string()))?
            };

            Ok(result.rows_affected() as usize)
        })
    }
}

async fn transition_leased_entry_async(
    pool: &PgPool,
    queue_id: Uuid,
    worker_id: &str,
    next_status: QueueStatus,
) -> Result<(), QueueError> {
    let worker_id = worker_id.to_string();
    let status_db = queue_status_to_db(next_status).to_string();
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| QueueError::Database(error.to_string()))?;

    let row = sqlx::query(
        r#"
        SELECT status, lease_owner
        FROM task_queue
        WHERE queue_id = $1
        FOR UPDATE
        "#,
    )
    .bind(queue_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| QueueError::Database(error.to_string()))?;

    let row = row.ok_or(QueueError::NotFound(queue_id))?;
    let current_status_raw: String = row
        .try_get("status")
        .map_err(|error| QueueError::Database(error.to_string()))?;
    let current_status = queue_status_from_db(&current_status_raw)?;
    if current_status != QueueStatus::Leased {
        return Err(QueueError::InvalidStatus {
            entry_id: queue_id,
            expected: "leased",
            actual: format!("{current_status:?}"),
        });
    }

    let lease_owner: Option<String> = row
        .try_get("lease_owner")
        .map_err(|error| QueueError::Database(error.to_string()))?;
    let actual_owner = lease_owner.unwrap_or_default();
    if actual_owner != worker_id {
        return Err(QueueError::LeaseOwnerMismatch {
            entry_id: queue_id,
            expected: worker_id,
            actual: actual_owner,
        });
    }

    sqlx::query(
        r#"
        UPDATE task_queue
        SET status = $1,
            updated_at = $2
        WHERE queue_id = $3
        "#,
    )
    .bind(status_db)
    .bind(Utc::now())
    .bind(queue_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| QueueError::Database(error.to_string()))?;

    tx.commit()
        .await
        .map_err(|error| QueueError::Database(error.to_string()))?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UniqueViolation {
    ActiveTask,
    Unknown,
}

fn classify_unique_violation(constraint: Option<&str>) -> UniqueViolation {
    match constraint {
        Some("ux_task_queue_active_task") => UniqueViolation::ActiveTask,
        _ => UniqueViolation::Unknown,
    }
}

fn count_to_usize(value: i64) -> usize {
    value.max(0) as usize
}

fn queue_status_to_db(status: QueueStatus) -> &'static str {
    match status {
        QueueStatus::Pending => "pending",
        QueueStatus::Leased => "leased",
        QueueStatus::Completed => "completed",
        QueueStatus::Failed => "failed",
        QueueStatus::Cancelled => "cancelled",
    }
}

fn queue_status_from_db(value: &str) -> Result<QueueStatus, QueueError> {
    match value {
        "pending" => Ok(QueueStatus::Pending),
        "leased" => Ok(QueueStatus::Leased),
        "completed" => Ok(QueueStatus::Completed),
        "failed" => Ok(QueueStatus::Failed),
        "cancelled" => Ok(QueueStatus::Cancelled),
        other => Err(QueueError::InvalidQueueStatus(other.to_string())),
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

fn command_class_from_db(value: &str) -> Result<CommandClass, QueueError> {
    match value {
        "io" => Ok(CommandClass::Io),
        "cpu" => Ok(CommandClass::Cpu),
        "git" => Ok(CommandClass::Git),
        "tool" => Ok(CommandClass::Tool),
        other => Err(QueueError::InvalidCommandClass(other.to_string())),
    }
}

fn row_to_queue_entry(row: PgRow) -> Result<QueueEntry, QueueError> {
    let status_raw: String = row
        .try_get("status")
        .map_err(|error| QueueError::Database(error.to_string()))?;
    let command_class_raw: String = row
        .try_get("command_class")
        .map_err(|error| QueueError::Database(error.to_string()))?;
    let priority: i32 = row
        .try_get("priority")
        .map_err(|error| QueueError::Database(error.to_string()))?;
    let attempt_no: i32 = row
        .try_get("attempt_no")
        .map_err(|error| QueueError::Database(error.to_string()))?;

    Ok(QueueEntry {
        queue_id: row
            .try_get("queue_id")
            .map_err(|error| QueueError::Database(error.to_string()))?,
        task_id: row
            .try_get("task_id")
            .map_err(|error| QueueError::Database(error.to_string()))?,
        run_id: row
            .try_get("run_id")
            .map_err(|error| QueueError::Database(error.to_string()))?,
        priority: priority.max(0) as u32,
        available_at: row
            .try_get("available_at")
            .map_err(|error| QueueError::Database(error.to_string()))?,
        attempt_no: attempt_no.max(0) as u32,
        command_class: command_class_from_db(&command_class_raw)?,
        status: queue_status_from_db(&status_raw)?,
        lease_owner: row
            .try_get("lease_owner")
            .map_err(|error| QueueError::Database(error.to_string()))?,
        lease_expires_at: row
            .try_get("lease_expires_at")
            .map_err(|error| QueueError::Database(error.to_string()))?,
        last_heartbeat: row
            .try_get("last_heartbeat")
            .map_err(|error| QueueError::Database(error.to_string()))?,
        created_at: row
            .try_get("created_at")
            .map_err(|error| QueueError::Database(error.to_string()))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|error| QueueError::Database(error.to_string()))?,
    })
}

#[cfg(test)]
mod tests {
    use crate::yarli_core::domain::CommandClass;

    use super::{
        classify_unique_violation, command_class_from_db, command_class_to_db,
        queue_status_from_db, queue_status_to_db, UniqueViolation, CLAIM_CANDIDATES_SCOPED_SQL,
        CLAIM_CANDIDATES_SQL,
    };
    use crate::yarli_queue::queue::QueueStatus;

    #[test]
    fn claim_sql_uses_skip_locked() {
        assert!(CLAIM_CANDIDATES_SQL.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn claim_sql_uses_valid_aging_order_expression() {
        let expected =
            "(priority + (GREATEST(0, EXTRACT(EPOCH FROM ($1 - available_at))::BIGINT / 60))) DESC";
        assert!(CLAIM_CANDIDATES_SQL.contains(expected));
        assert!(CLAIM_CANDIDATES_SCOPED_SQL.contains(expected));
    }

    #[test]
    fn status_codec_round_trips() {
        let statuses = [
            QueueStatus::Pending,
            QueueStatus::Leased,
            QueueStatus::Completed,
            QueueStatus::Failed,
            QueueStatus::Cancelled,
        ];

        for status in statuses {
            let db_value = queue_status_to_db(status);
            let parsed = queue_status_from_db(db_value).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn command_class_codec_round_trips() {
        let classes = [
            CommandClass::Io,
            CommandClass::Cpu,
            CommandClass::Git,
            CommandClass::Tool,
        ];

        for class in classes {
            let db_value = command_class_to_db(class);
            let parsed = command_class_from_db(db_value).unwrap();
            assert_eq!(parsed, class);
        }
    }

    #[test]
    fn classify_unique_violation_maps_active_task_constraint() {
        assert_eq!(
            classify_unique_violation(Some("ux_task_queue_active_task")),
            UniqueViolation::ActiveTask
        );
        assert_eq!(
            classify_unique_violation(Some("some_other_constraint")),
            UniqueViolation::Unknown
        );
        assert_eq!(classify_unique_violation(None), UniqueViolation::Unknown);
    }
}
