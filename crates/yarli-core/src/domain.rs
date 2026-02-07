//! Core domain primitives: Run, Task, Evidence, Gate, PolicyDecision, etc.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a run.
pub type RunId = Uuid;

/// Unique identifier for a task.
pub type TaskId = Uuid;

/// Unique identifier for an event.
pub type EventId = Uuid;

/// Unique identifier for a correlation chain.
pub type CorrelationId = Uuid;

/// Unique identifier for a worktree binding.
pub type WorktreeId = Uuid;

/// Unique identifier for a merge intent.
pub type MergeIntentId = Uuid;

/// Entity types in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Run,
    Task,
    Worktree,
    Merge,
    Command,
    Gate,
    Policy,
}

/// An immutable event in the event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_id: EventId,
    pub occurred_at: DateTime<Utc>,
    pub entity_type: EntityType,
    pub entity_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<EventId>,
    pub actor: String,
    pub idempotency_key: Option<String>,
}

/// Evidence proving a task outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub evidence_id: Uuid,
    pub task_id: TaskId,
    pub run_id: RunId,
    pub evidence_type: String,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// A policy decision record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub decision_id: Uuid,
    pub run_id: RunId,
    pub actor: String,
    pub action: String,
    pub outcome: PolicyOutcome,
    pub rule_id: String,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
}

/// Outcome of a policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyOutcome {
    Allow,
    Deny,
    RequireApproval,
}

/// Exit reason codes for runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitReason {
    CompletedAllGates,
    BlockedOpenTasks,
    BlockedGateFailure,
    FailedPolicyDenial,
    FailedRuntimeError,
    CancelledByOperator,
    TimedOut,
    StalledNoProgress,
}

/// Safe modes for the orchestrator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafeMode {
    /// Read-only, no mutations.
    Observe,
    /// Normal execution with policy.
    Execute,
    /// Only allowlisted commands.
    Restricted,
    /// Temporary elevated mode with high audit level.
    Breakglass,
}

/// Command class for concurrency caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandClass {
    Io,
    Cpu,
    Git,
    Tool,
}
