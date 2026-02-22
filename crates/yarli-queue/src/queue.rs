//! TaskQueue trait — abstraction over lease-based task scheduling.
//!
//! The queue implements `FOR UPDATE SKIP LOCKED`-style claim semantics
//! (Section 9.3): tasks are enqueued with priority and availability time,
//! claimed with a lease TTL, and must be heartbeated to prevent reclamation.
//!
//! Lease semantics (Section 10.2):
//! - Worker lease TTL: default 30s (configurable).
//! - Heartbeat every 5s while task executes.
//! - Scheduler reclaims stale leases after TTL + grace window.
//! - Reclaimed tasks move back to `pending` with incremented attempt.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use yarli_core::domain::{CommandClass, RunId, TaskId};

use crate::QueueError;

/// Status of a queue entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    /// Waiting to be claimed.
    Pending,
    /// Claimed by a worker with active lease.
    Leased,
    /// Successfully completed.
    Completed,
    /// Failed (may be retried).
    Failed,
    /// Cancelled — will not be retried.
    Cancelled,
}

/// A single entry in the task queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    /// Unique queue entry identifier.
    pub queue_id: Uuid,
    /// The task this entry represents.
    pub task_id: TaskId,
    /// The run this task belongs to (for per-run concurrency caps).
    pub run_id: RunId,
    /// Priority for ordering (1 = highest, higher numbers = lower priority).
    pub priority: u32,
    /// Earliest time this entry becomes available for claiming.
    pub available_at: DateTime<Utc>,
    /// Current attempt number (starts at 1, incremented on reclaim).
    pub attempt_no: u32,
    /// Command class for global concurrency caps.
    pub command_class: CommandClass,
    /// Current status.
    pub status: QueueStatus,
    /// Worker that holds the lease (if leased).
    pub lease_owner: Option<String>,
    /// When the lease expires (if leased).
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// Last heartbeat timestamp.
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// When this entry was created.
    pub created_at: DateTime<Utc>,
    /// When this entry was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Parameters for claiming tasks from the queue.
#[derive(Debug, Clone)]
pub struct ClaimRequest {
    /// Worker identity (e.g. "worker-1").
    pub worker_id: String,
    /// Maximum number of entries to claim.
    pub limit: usize,
    /// Lease TTL duration.
    pub lease_ttl: Duration,
    /// If set, only claim tasks belonging to these run IDs.
    /// `None` means no filtering (claim from any run).
    pub allowed_run_ids: Option<Vec<RunId>>,
}

impl ClaimRequest {
    pub fn new(worker_id: impl Into<String>, limit: usize, lease_ttl: Duration) -> Self {
        Self {
            worker_id: worker_id.into(),
            limit,
            lease_ttl,
            allowed_run_ids: None,
        }
    }

    /// Single-task claim with default 30s TTL.
    pub fn single(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            limit: 1,
            lease_ttl: Duration::seconds(30),
            allowed_run_ids: None,
        }
    }

    /// Restrict claims to only the given run IDs.
    pub fn with_allowed_run_ids(mut self, run_ids: Vec<RunId>) -> Self {
        self.allowed_run_ids = Some(run_ids);
        self
    }
}

/// Concurrency configuration for backpressure (Section 10.3).
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Per-run maximum concurrent leased tasks.
    pub per_run_cap: usize,
    /// Global cap per command class.
    pub io_cap: usize,
    pub cpu_cap: usize,
    pub git_cap: usize,
    pub tool_cap: usize,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            per_run_cap: 8,
            io_cap: 16,
            cpu_cap: 4,
            git_cap: 2,
            tool_cap: 8,
        }
    }
}

impl ConcurrencyConfig {
    /// Get the cap for a specific command class.
    pub fn cap_for(&self, class: CommandClass) -> usize {
        match class {
            CommandClass::Io => self.io_cap,
            CommandClass::Cpu => self.cpu_cap,
            CommandClass::Git => self.git_cap,
            CommandClass::Tool => self.tool_cap,
        }
    }
}

/// Abstract task queue. Implementations must be thread-safe.
///
/// Mirrors the lease-based scheduling from Section 9.3 and 10.2.
pub trait TaskQueue: Send + Sync {
    /// Enqueue a task for scheduling.
    ///
    /// Returns the `queue_id`. Rejects duplicates (same task_id already pending/leased).
    fn enqueue(
        &self,
        task_id: TaskId,
        run_id: RunId,
        priority: u32,
        command_class: CommandClass,
        available_at: Option<DateTime<Utc>>,
    ) -> Result<Uuid, QueueError>;

    /// Claim up to `limit` pending tasks, ordered by priority ASC then available_at ASC.
    ///
    /// Implements `SELECT ... FOR UPDATE SKIP LOCKED LIMIT N` semantics:
    /// only returns entries that are `Pending`, not claimed by another worker,
    /// and whose `available_at <= now`. Sets lease owner and expiry atomically.
    ///
    /// Respects concurrency caps from the provided config.
    fn claim(
        &self,
        request: &ClaimRequest,
        config: &ConcurrencyConfig,
    ) -> Result<Vec<QueueEntry>, QueueError>;

    /// Extend the lease for an active entry (heartbeat).
    ///
    /// The worker must match the current lease owner. Resets lease_expires_at
    /// to `now + lease_ttl`.
    fn heartbeat(
        &self,
        queue_id: Uuid,
        worker_id: &str,
        lease_ttl: Duration,
    ) -> Result<(), QueueError>;

    /// Mark a leased entry as completed. Only the lease owner can complete.
    fn complete(&self, queue_id: Uuid, worker_id: &str) -> Result<(), QueueError>;

    /// Mark a leased entry as failed. Only the lease owner can fail.
    fn fail(&self, queue_id: Uuid, worker_id: &str) -> Result<(), QueueError>;

    /// Cancel a pending or leased entry. Cancelled entries are not retried.
    fn cancel(&self, queue_id: Uuid) -> Result<(), QueueError>;

    /// Reclaim stale leases: entries where `lease_expires_at + grace < now`.
    ///
    /// Reclaimed entries move to `Pending` with incremented attempt_no.
    /// Returns the number of entries reclaimed.
    fn reclaim_stale(&self, grace_period: Duration) -> Result<usize, QueueError>;

    /// Return the number of entries in each status.
    fn stats(&self) -> QueueStats;

    /// Return a snapshot of all known queue entries.
    ///
    /// The snapshot is best-effort and suitable for read-only debug/introspection
    /// surfaces. Entries are ordered according to underlying storage semantics.
    fn entries(&self) -> Vec<QueueEntry>;

    /// Return the number of currently leased entries for a given run.
    fn leased_count_for_run(&self, run_id: RunId) -> usize;

    /// Return the number of currently leased entries for a given command class.
    fn leased_count_for_class(&self, class: CommandClass) -> usize;

    /// Return total pending entries.
    fn pending_count(&self) -> usize;

    /// Cancel all pending and leased entries for a given run.
    ///
    /// Returns the number of entries cancelled. Used during run cancellation
    /// to drain stale queue rows that would otherwise starve concurrency caps.
    fn cancel_for_run(&self, run_id: RunId) -> Result<usize, QueueError>;

    /// Cancel all pending and leased entries NOT belonging to the given set of run IDs.
    ///
    /// Used at scheduler startup to drain stale rows from prior crashed/cancelled runs.
    /// Returns the number of entries cancelled.
    fn cancel_stale_runs(&self, active_run_ids: &[RunId]) -> Result<usize, QueueError>;
}

/// Summary statistics for the queue.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueStats {
    pub pending: usize,
    pub leased: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
}
