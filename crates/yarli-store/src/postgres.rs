//! Postgres-backed event store implementation.
//!
//! Implements `EventStore` semantics over the `events` table from migration
//! `0001_init.sql`.

use std::future::Future;
use std::thread;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use tokio::runtime::{Builder, Handle, RuntimeFlavor};
use tracing::warn;
use yarli_core::domain::{EntityType, Event, EventId};

use crate::error::StoreError;
use crate::event_store::{EventQuery, EventStore};

/// Postgres-backed event store.
#[derive(Debug, Clone)]
pub struct PostgresEventStore {
    pool: PgPool,
}

impl PostgresEventStore {
    /// Create a store backed by a lazily-connected `PgPool`.
    pub fn new(database_url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .connect_lazy(database_url)
            .map_err(|error| StoreError::Database(error.to_string()))?;

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

    fn run_async<T, Fut>(&self, fut: Fut) -> Result<T, StoreError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, StoreError>> + Send + 'static,
    {
        match Handle::try_current() {
            Ok(handle) => match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(fut)),
                RuntimeFlavor::CurrentThread => {
                    thread::spawn(move || {
                        let runtime = Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|error| StoreError::Runtime(error.to_string()))?;
                        runtime.block_on(fut)
                    })
                    .join()
                    .map_err(|_| StoreError::Runtime("postgres operation panicked".to_string()))?
                }
                _ => {
                    let runtime = Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| StoreError::Runtime(error.to_string()))?;
                    runtime.block_on(fut)
                }
            },
            Err(_) => {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| StoreError::Runtime(error.to_string()))?;
                runtime.block_on(fut)
            }
        }
    }
}

impl EventStore for PostgresEventStore {
    fn append(&self, event: Event) -> Result<(), StoreError> {
        let pool = self.pool.clone();
        self.run_async(async move {
            let event_id = event.event_id;
            let idempotency_key = event.idempotency_key.clone();
            let entity_type = entity_type_to_db(event.entity_type);

            let result = sqlx::query(
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
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
            )
            .bind(event_id)
            .bind(event.occurred_at)
            .bind(entity_type)
            .bind(event.entity_id)
            .bind(event.event_type)
            .bind(event.payload)
            .bind(event.correlation_id)
            .bind(event.causation_id)
            .bind(event.actor)
            .bind(event.idempotency_key)
            .execute(&pool)
            .await;

            match result {
                Ok(_) => Ok(()),
                Err(sqlx::Error::Database(db_error))
                    if db_error.code().as_deref() == Some("23505") =>
                {
                    match classify_unique_violation(db_error.constraint()) {
                        UniqueViolation::IdempotencyKey => {
                            let key = idempotency_key.unwrap_or_else(|| "<unknown>".to_string());
                            Err(StoreError::DuplicateIdempotencyKey(key))
                        }
                        UniqueViolation::EventId => Err(StoreError::DuplicateEventId(event_id)),
                        UniqueViolation::Unknown => {
                            Err(StoreError::Database(db_error.message().to_string()))
                        }
                    }
                }
                Err(error) => Err(StoreError::Database(error.to_string())),
            }
        })
    }

    fn get(&self, event_id: EventId) -> Result<Event, StoreError> {
        let pool = self.pool.clone();
        self.run_async(async move {
            let row = sqlx::query(
                r#"
                SELECT
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
                FROM events
                WHERE event_id = $1
                "#,
            )
            .bind(event_id)
            .fetch_optional(&pool)
            .await
            .map_err(|error| StoreError::Database(error.to_string()))?;

            match row {
                Some(row) => row_to_event(row),
                None => Err(StoreError::EventNotFound(event_id)),
            }
        })
    }

    fn query(&self, query: &EventQuery) -> Result<Vec<Event>, StoreError> {
        let pool = self.pool.clone();
        let entity_type = query.entity_type.map(entity_type_to_db);
        let entity_id = query.entity_id.clone();
        let correlation_id = query.correlation_id;
        let event_type = query.event_type.clone();
        let limit = query.limit.map(|value| value.min(i64::MAX as usize) as i64);
        let after_event_id = query.after_event_id;

        self.run_async(async move {
            let after_occurred_at = match after_event_id {
                Some(anchor_id) => {
                    let anchor = sqlx::query(
                        r#"
                        SELECT occurred_at
                        FROM events
                        WHERE event_id = $1
                        "#,
                    )
                    .bind(anchor_id)
                    .fetch_optional(&pool)
                    .await
                    .map_err(|error| StoreError::Database(error.to_string()))?;

                    match anchor {
                        Some(row) => Some(
                            row.try_get::<DateTime<Utc>, _>("occurred_at")
                                .map_err(|error| StoreError::Database(error.to_string()))?,
                        ),
                        None => return Err(StoreError::EventNotFound(anchor_id)),
                    }
                }
                None => None,
            };

            let rows = sqlx::query(
                r#"
                SELECT
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
                FROM events
                WHERE
                    ($1::text IS NULL OR entity_type = $1)
                    AND ($2::text IS NULL OR entity_id = $2)
                    AND ($3::uuid IS NULL OR correlation_id = $3)
                    AND ($4::text IS NULL OR event_type = $4)
                    AND (
                        $5::timestamptz IS NULL
                        OR occurred_at > $5
                        OR (occurred_at = $5 AND event_id > $6::uuid)
                    )
                ORDER BY occurred_at ASC, event_id ASC
                LIMIT COALESCE($7::bigint, 9223372036854775807)
                "#,
            )
            .bind(entity_type)
            .bind(entity_id)
            .bind(correlation_id)
            .bind(event_type)
            .bind(after_occurred_at)
            .bind(after_event_id)
            .bind(limit)
            .fetch_all(&pool)
            .await
            .map_err(|error| StoreError::Database(error.to_string()))?;

            rows.into_iter().map(row_to_event).collect()
        })
    }

    fn all(&self) -> Result<Vec<Event>, StoreError> {
        let pool = self.pool.clone();
        self.run_async(async move {
            let rows = sqlx::query(
                r#"
                SELECT
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
                FROM events
                ORDER BY created_at ASC, event_id ASC
                "#,
            )
            .fetch_all(&pool)
            .await
            .map_err(|error| StoreError::Database(error.to_string()))?;

            rows.into_iter().map(row_to_event).collect()
        })
    }

    fn len(&self) -> usize {
        let pool = self.pool.clone();
        match self.run_async(async move {
            let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::bigint FROM events")
                .fetch_one(&pool)
                .await
                .map_err(|error| StoreError::Database(error.to_string()))?;
            Ok(count.max(0) as usize)
        }) {
            Ok(count) => count,
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to compute event store length; returning zero"
                );
                0
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UniqueViolation {
    EventId,
    IdempotencyKey,
    Unknown,
}

fn classify_unique_violation(constraint: Option<&str>) -> UniqueViolation {
    match constraint {
        Some("ux_events_idempotency_key") => UniqueViolation::IdempotencyKey,
        Some("events_pkey") => UniqueViolation::EventId,
        _ => UniqueViolation::Unknown,
    }
}

fn entity_type_to_db(entity_type: EntityType) -> &'static str {
    match entity_type {
        EntityType::Run => "run",
        EntityType::Task => "task",
        EntityType::Worktree => "worktree",
        EntityType::Merge => "merge",
        EntityType::Command => "command",
        EntityType::Gate => "gate",
        EntityType::Policy => "policy",
    }
}

fn entity_type_from_db(value: &str) -> Result<EntityType, StoreError> {
    match value {
        "run" => Ok(EntityType::Run),
        "task" => Ok(EntityType::Task),
        "worktree" => Ok(EntityType::Worktree),
        "merge" => Ok(EntityType::Merge),
        "command" => Ok(EntityType::Command),
        "gate" => Ok(EntityType::Gate),
        "policy" => Ok(EntityType::Policy),
        other => Err(StoreError::InvalidEntityType(other.to_string())),
    }
}

fn row_to_event(row: PgRow) -> Result<Event, StoreError> {
    let entity_type_raw: String = row
        .try_get("entity_type")
        .map_err(|error| StoreError::Database(error.to_string()))?;

    Ok(Event {
        event_id: row
            .try_get("event_id")
            .map_err(|error| StoreError::Database(error.to_string()))?,
        occurred_at: row
            .try_get("occurred_at")
            .map_err(|error| StoreError::Database(error.to_string()))?,
        entity_type: entity_type_from_db(&entity_type_raw)?,
        entity_id: row
            .try_get("entity_id")
            .map_err(|error| StoreError::Database(error.to_string()))?,
        event_type: row
            .try_get("event_type")
            .map_err(|error| StoreError::Database(error.to_string()))?,
        payload: row
            .try_get("payload")
            .map_err(|error| StoreError::Database(error.to_string()))?,
        correlation_id: row
            .try_get("correlation_id")
            .map_err(|error| StoreError::Database(error.to_string()))?,
        causation_id: row
            .try_get("causation_id")
            .map_err(|error| StoreError::Database(error.to_string()))?,
        actor: row
            .try_get("actor")
            .map_err(|error| StoreError::Database(error.to_string()))?,
        idempotency_key: row
            .try_get("idempotency_key")
            .map_err(|error| StoreError::Database(error.to_string()))?,
    })
}

#[cfg(test)]
mod tests {
    use yarli_core::domain::EntityType;

    use super::{
        classify_unique_violation, entity_type_from_db, entity_type_to_db, UniqueViolation,
    };

    #[test]
    fn entity_type_codec_round_trips() {
        let values = [
            EntityType::Run,
            EntityType::Task,
            EntityType::Worktree,
            EntityType::Merge,
            EntityType::Command,
            EntityType::Gate,
            EntityType::Policy,
        ];

        for value in values {
            let db_value = entity_type_to_db(value);
            let parsed = entity_type_from_db(db_value).unwrap();
            assert_eq!(parsed, value);
        }
    }

    #[test]
    fn classify_unique_violation_maps_known_constraints() {
        assert_eq!(
            classify_unique_violation(Some("events_pkey")),
            UniqueViolation::EventId
        );
        assert_eq!(
            classify_unique_violation(Some("ux_events_idempotency_key")),
            UniqueViolation::IdempotencyKey
        );
        assert_eq!(
            classify_unique_violation(Some("some_other_constraint")),
            UniqueViolation::Unknown
        );
        assert_eq!(classify_unique_violation(None), UniqueViolation::Unknown);
    }
}
