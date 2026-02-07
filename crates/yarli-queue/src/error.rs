//! Error types for yarli-queue.

use thiserror::Error;
use uuid::Uuid;

/// Errors from task queue operations.
#[derive(Debug, Error)]
pub enum QueueError {
    /// Entry not found in the queue.
    #[error("queue entry not found: {0}")]
    NotFound(Uuid),

    /// Entry is not in the expected status for this operation.
    #[error("invalid status for {entry_id}: expected {expected}, got {actual}")]
    InvalidStatus {
        entry_id: Uuid,
        expected: &'static str,
        actual: String,
    },

    /// Lease has expired and was reclaimed.
    #[error("lease expired for entry {0}")]
    LeaseExpired(Uuid),

    /// Lease owner mismatch — another worker holds the lease.
    #[error("lease owner mismatch for entry {entry_id}: expected {expected}, got {actual}")]
    LeaseOwnerMismatch {
        entry_id: Uuid,
        expected: String,
        actual: String,
    },

    /// Duplicate task_id already in queue for this run.
    #[error("duplicate task in queue: task_id={0}")]
    DuplicateTask(Uuid),

    /// Concurrency cap exceeded.
    #[error("concurrency cap exceeded: {0}")]
    ConcurrencyCapExceeded(String),
}
