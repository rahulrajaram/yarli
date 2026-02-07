//! Error types for yarli-store.

use thiserror::Error;
use uuid::Uuid;

/// Errors from event store operations.
#[derive(Debug, Error)]
pub enum StoreError {
    /// An event with this idempotency key already exists.
    #[error("duplicate idempotency key: {0}")]
    DuplicateIdempotencyKey(String),

    /// An event with this ID already exists.
    #[error("duplicate event ID: {0}")]
    DuplicateEventId(Uuid),

    /// Event not found.
    #[error("event not found: {0}")]
    EventNotFound(Uuid),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
