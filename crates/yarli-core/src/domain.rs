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
    MergeConflict,
    FailedPolicyDenial,
    FailedRuntimeError,
    CancelledByOperator,
    TimedOut,
    StalledNoProgress,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CompletedAllGates => write!(f, "completed_all_gates"),
            Self::BlockedOpenTasks => write!(f, "blocked_open_tasks"),
            Self::BlockedGateFailure => write!(f, "blocked_gate_failure"),
            Self::MergeConflict => write!(f, "merge_conflict"),
            Self::FailedPolicyDenial => write!(f, "failed_policy_denial"),
            Self::FailedRuntimeError => write!(f, "failed_runtime_error"),
            Self::CancelledByOperator => write!(f, "cancelled_by_operator"),
            Self::TimedOut => write!(f, "timed_out"),
            Self::StalledNoProgress => write!(f, "stalled_no_progress"),
        }
    }
}

/// Provenance for cancellation-triggered shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationSource {
    Operator,
    Sigint,
    Sigterm,
    Sw4rmPreemption,
    Unknown,
}

impl std::fmt::Display for CancellationSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Operator => write!(f, "operator"),
            Self::Sigint => write!(f, "sigint"),
            Self::Sigterm => write!(f, "sigterm"),
            Self::Sw4rmPreemption => write!(f, "sw4rm_preemption"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Best-effort actor classification for cancellation origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationActorKind {
    Operator,
    System,
    Supervisor,
    Unknown,
}

impl std::fmt::Display for CancellationActorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Operator => write!(f, "operator"),
            Self::System => write!(f, "system"),
            Self::Supervisor => write!(f, "supervisor"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Pipeline stage where cancellation was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationStage {
    Executing,
    Retrying,
    Verifying,
    Unknown,
}

impl std::fmt::Display for CancellationStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Executing => write!(f, "executing"),
            Self::Retrying => write!(f, "retrying"),
            Self::Verifying => write!(f, "verifying"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Structured cancellation provenance attached to cancellation terminal events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancellationProvenance {
    pub cancellation_source: CancellationSource,
    #[serde(default)]
    pub signal_name: Option<String>,
    #[serde(default)]
    pub signal_number: Option<i32>,
    #[serde(default)]
    pub sender_pid: Option<u32>,
    #[serde(default)]
    pub receiver_pid: Option<u32>,
    #[serde(default)]
    pub parent_pid: Option<u32>,
    #[serde(default)]
    pub process_group_id: Option<u32>,
    #[serde(default)]
    pub session_id: Option<u32>,
    #[serde(default)]
    pub tty: Option<String>,
    #[serde(default)]
    pub actor_kind: Option<CancellationActorKind>,
    #[serde(default)]
    pub actor_detail: Option<String>,
    #[serde(default)]
    pub stage: Option<CancellationStage>,
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
