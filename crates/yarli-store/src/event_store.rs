//! EventStore trait — abstraction over event persistence.
//!
//! The event store is the source of truth for all state transitions
//! (Section 9.1). Every transition must be persisted before side effects
//! proceed (Invariant 4). The store supports event-sourced replay to
//! reconstruct materialized state (Invariant 6, Section 18.2).

use yarli_core::domain::{CorrelationId, EntityType, Event, EventId};

use crate::StoreError;

/// Query filter for retrieving events.
#[derive(Debug, Default, Clone)]
pub struct EventQuery {
    /// Filter by entity type.
    pub entity_type: Option<EntityType>,
    /// Filter by entity ID.
    pub entity_id: Option<String>,
    /// Filter by correlation ID.
    pub correlation_id: Option<CorrelationId>,
    /// Filter by event type.
    pub event_type: Option<String>,
    /// Maximum number of events to return.
    pub limit: Option<usize>,
    /// Return events after this event ID (for pagination).
    pub after_event_id: Option<EventId>,
}

impl EventQuery {
    pub fn by_entity(entity_type: EntityType, entity_id: impl Into<String>) -> Self {
        Self {
            entity_type: Some(entity_type),
            entity_id: Some(entity_id.into()),
            ..Default::default()
        }
    }

    pub fn by_correlation(correlation_id: CorrelationId) -> Self {
        Self {
            correlation_id: Some(correlation_id),
            ..Default::default()
        }
    }

    pub fn by_entity_type(entity_type: EntityType) -> Self {
        Self {
            entity_type: Some(entity_type),
            ..Default::default()
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn after(mut self, event_id: EventId) -> Self {
        self.after_event_id = Some(event_id);
        self
    }
}

/// Abstract event store. Implementations must be thread-safe.
pub trait EventStore: Send + Sync {
    /// Append an event to the store.
    ///
    /// If the event has an `idempotency_key` that already exists, returns
    /// [`StoreError::DuplicateIdempotencyKey`]. Duplicate `event_id` values
    /// return [`StoreError::DuplicateEventId`].
    fn append(&self, event: Event) -> Result<(), StoreError>;

    /// Retrieve a single event by ID.
    fn get(&self, event_id: EventId) -> Result<Event, StoreError>;

    /// Query events matching a filter. Results are ordered by `occurred_at` ASC.
    fn query(&self, query: &EventQuery) -> Result<Vec<Event>, StoreError>;

    /// Return all events in insertion order (for replay).
    fn all(&self) -> Result<Vec<Event>, StoreError>;

    /// Return the total number of stored events.
    fn len(&self) -> usize;

    /// Check whether the store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
