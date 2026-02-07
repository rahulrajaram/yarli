//! yarli-queue: Lease-based task queue and scheduling.
//!
//! Provides the [`TaskQueue`] trait and an in-memory implementation
//! for development and testing. The queue uses lease semantics with
//! heartbeat and stale reclamation (Section 9.3, 10.2).
//!
//! A Postgres-backed implementation using `FOR UPDATE SKIP LOCKED`
//! will follow in a later milestone.

pub mod error;
pub mod memory;
pub mod queue;
pub mod scheduler;

pub use error::QueueError;
pub use memory::InMemoryTaskQueue;
pub use queue::{
    ClaimRequest, ConcurrencyConfig, QueueEntry, QueueStats, QueueStatus, TaskQueue,
};
pub use scheduler::{
    Scheduler, SchedulerConfig, SchedulerError, TaskOutcome, TaskRegistry, TickResult,
};
