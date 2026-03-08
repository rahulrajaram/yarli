//! In-memory event store for development and testing.
//!
//! Uses `RwLock<Vec<Event>>` for thread-safe concurrent access.
//! Events are stored in append order and never mutated.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::yarli_core::domain::{Event, EventId};

use crate::yarli_store::error::StoreError;
use crate::yarli_store::event_store::{EventQuery, EventStore};

/// In-memory event store backed by a `Vec<Event>` behind a `RwLock`.
///
/// Suitable for single-process usage in development and tests.
/// For production, use the Postgres-backed store.
#[derive(Debug, Default)]
pub struct InMemoryEventStore {
    events: RwLock<Vec<Event>>,
    /// Index: event_id -> position in events vec.
    event_index: RwLock<HashMap<EventId, usize>>,
    /// Set of known idempotency keys for dedup.
    idempotency_keys: RwLock<HashSet<String>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl EventStore for InMemoryEventStore {
    fn append(&self, event: Event) -> Result<(), StoreError> {
        // Check idempotency key first (outside write lock).
        if let Some(ref key) = event.idempotency_key {
            let keys = self.idempotency_keys.read().unwrap();
            if keys.contains(key) {
                return Err(StoreError::DuplicateIdempotencyKey(key.clone()));
            }
        }

        let mut events = self.events.write().unwrap();
        let mut index = self.event_index.write().unwrap();
        let mut keys = self.idempotency_keys.write().unwrap();

        // Double-check idempotency key under write lock.
        if let Some(ref key) = event.idempotency_key {
            if keys.contains(key) {
                return Err(StoreError::DuplicateIdempotencyKey(key.clone()));
            }
        }

        // Check duplicate event ID.
        if index.contains_key(&event.event_id) {
            return Err(StoreError::DuplicateEventId(event.event_id));
        }

        let pos = events.len();
        index.insert(event.event_id, pos);
        if let Some(ref key) = event.idempotency_key {
            keys.insert(key.clone());
        }
        events.push(event);

        Ok(())
    }

    fn get(&self, event_id: EventId) -> Result<Event, StoreError> {
        let index = self.event_index.read().unwrap();
        let events = self.events.read().unwrap();

        match index.get(&event_id) {
            Some(&pos) => Ok(events[pos].clone()),
            None => Err(StoreError::EventNotFound(event_id)),
        }
    }

    fn query(&self, query: &EventQuery) -> Result<Vec<Event>, StoreError> {
        let events = self.events.read().unwrap();
        let index = self.event_index.read().unwrap();

        // Resolve the starting position for after_event_id pagination.
        let start_pos = match query.after_event_id {
            Some(after_id) => match index.get(&after_id) {
                Some(&pos) => pos + 1,
                None => return Err(StoreError::EventNotFound(after_id)),
            },
            None => 0,
        };

        let mut results: Vec<Event> = events[start_pos..]
            .iter()
            .filter(|e| {
                if let Some(ref et) = query.entity_type {
                    if e.entity_type != *et {
                        return false;
                    }
                }
                if let Some(ref eid) = query.entity_id {
                    if e.entity_id != *eid {
                        return false;
                    }
                }
                if let Some(ref cid) = query.correlation_id {
                    if e.correlation_id != *cid {
                        return false;
                    }
                }
                if let Some(ref evt) = query.event_type {
                    if e.event_type != *evt {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        // Events are already in insertion order (which is occurred_at ASC).
        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    fn all(&self) -> Result<Vec<Event>, StoreError> {
        let events = self.events.read().unwrap();
        Ok(events.clone())
    }

    fn len(&self) -> usize {
        self.events.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests;
