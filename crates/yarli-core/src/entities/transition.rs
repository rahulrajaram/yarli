//! Transition event — immutable record of a state change.
//!
//! Every state transition in the system produces a `Transition` record
//! that captures the actor, cause, correlation, and timing (Section 8.4).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{CorrelationId, EventId};

/// A state transition event. Produced by entity state machines and persisted
/// to the event log before any side effects proceed (Invariant 4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transition {
    /// Unique event ID for this transition.
    pub event_id: EventId,
    /// When the transition occurred (UTC).
    pub occurred_at: DateTime<Utc>,
    /// The entity kind that transitioned (e.g. "run", "task").
    pub entity_kind: String,
    /// The entity's unique ID.
    pub entity_id: Uuid,
    /// Previous state name (serialized).
    pub from_state: String,
    /// New state name (serialized).
    pub to_state: String,
    /// Reason code explaining why this transition happened.
    pub reason: String,
    /// Actor that triggered the transition (system, operator, policy).
    pub actor: String,
    /// Correlation ID linking related transitions.
    pub correlation_id: CorrelationId,
    /// Causation ID — the event that caused this transition.
    pub causation_id: Option<EventId>,
}

impl Transition {
    /// Create a new transition record with auto-generated event ID and timestamp.
    pub fn new(
        entity_kind: impl Into<String>,
        entity_id: Uuid,
        from_state: impl Into<String>,
        to_state: impl Into<String>,
        reason: impl Into<String>,
        actor: impl Into<String>,
        correlation_id: CorrelationId,
        causation_id: Option<EventId>,
    ) -> Self {
        Self {
            event_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            entity_kind: entity_kind.into(),
            entity_id,
            from_state: from_state.into(),
            to_state: to_state.into(),
            reason: reason.into(),
            actor: actor.into(),
            correlation_id,
            causation_id,
        }
    }
}
