//! Tests for the in-memory event store.

use chrono::Utc;
use uuid::Uuid;

use yarli_core::domain::{CorrelationId, EntityType, Event};

use crate::event_store::{EventQuery, EventStore};
use crate::memory::InMemoryEventStore;
use crate::StoreError;

/// Helper to create a test event with reasonable defaults.
fn make_event(entity_type: EntityType, entity_id: &str, event_type: &str) -> Event {
    Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type,
        entity_id: entity_id.to_string(),
        event_type: event_type.to_string(),
        payload: serde_json::json!({"test": true}),
        correlation_id: Uuid::now_v7(),
        causation_id: None,
        actor: "test".to_string(),
        idempotency_key: None,
    }
}

/// Helper to create an event with a specific correlation ID.
fn make_event_with_correlation(
    entity_type: EntityType,
    entity_id: &str,
    event_type: &str,
    correlation_id: CorrelationId,
) -> Event {
    let mut e = make_event(entity_type, entity_id, event_type);
    e.correlation_id = correlation_id;
    e
}

// ---- append ----

#[test]
fn append_single_event() {
    let store = InMemoryEventStore::new();
    let event = make_event(EntityType::Run, "run-1", "state_transition");
    assert!(store.append(event).is_ok());
    assert_eq!(store.len(), 1);
}

#[test]
fn append_multiple_events() {
    let store = InMemoryEventStore::new();
    for i in 0..10 {
        let event = make_event(EntityType::Task, &format!("task-{i}"), "state_transition");
        store.append(event).unwrap();
    }
    assert_eq!(store.len(), 10);
}

#[test]
fn append_rejects_duplicate_event_id() {
    let store = InMemoryEventStore::new();
    let event = make_event(EntityType::Run, "run-1", "state_transition");
    let dup = Event {
        event_type: "different_type".to_string(),
        ..event.clone()
    };
    store.append(event).unwrap();
    let err = store.append(dup).unwrap_err();
    assert!(matches!(err, StoreError::DuplicateEventId(_)));
}

#[test]
fn append_rejects_duplicate_idempotency_key() {
    let store = InMemoryEventStore::new();
    let mut e1 = make_event(EntityType::Run, "run-1", "cmd_start");
    e1.idempotency_key = Some("key-abc".to_string());

    let mut e2 = make_event(EntityType::Run, "run-1", "cmd_start");
    e2.idempotency_key = Some("key-abc".to_string());

    store.append(e1).unwrap();
    let err = store.append(e2).unwrap_err();
    assert!(matches!(err, StoreError::DuplicateIdempotencyKey(_)));
}

#[test]
fn append_allows_different_idempotency_keys() {
    let store = InMemoryEventStore::new();
    let mut e1 = make_event(EntityType::Run, "run-1", "cmd_start");
    e1.idempotency_key = Some("key-1".to_string());

    let mut e2 = make_event(EntityType::Run, "run-1", "cmd_start");
    e2.idempotency_key = Some("key-2".to_string());

    store.append(e1).unwrap();
    store.append(e2).unwrap();
    assert_eq!(store.len(), 2);
}

#[test]
fn append_allows_none_idempotency_keys() {
    let store = InMemoryEventStore::new();
    let e1 = make_event(EntityType::Run, "run-1", "a");
    let e2 = make_event(EntityType::Run, "run-1", "b");
    store.append(e1).unwrap();
    store.append(e2).unwrap();
    assert_eq!(store.len(), 2);
}

// ---- get ----

#[test]
fn get_existing_event() {
    let store = InMemoryEventStore::new();
    let event = make_event(EntityType::Run, "run-1", "state_transition");
    let id = event.event_id;
    store.append(event).unwrap();

    let retrieved = store.get(id).unwrap();
    assert_eq!(retrieved.event_id, id);
    assert_eq!(retrieved.entity_id, "run-1");
}

#[test]
fn get_missing_event() {
    let store = InMemoryEventStore::new();
    let err = store.get(Uuid::now_v7()).unwrap_err();
    assert!(matches!(err, StoreError::EventNotFound(_)));
}

// ---- query by entity type ----

#[test]
fn query_by_entity_type() {
    let store = InMemoryEventStore::new();
    store
        .append(make_event(EntityType::Run, "r1", "transition"))
        .unwrap();
    store
        .append(make_event(EntityType::Task, "t1", "transition"))
        .unwrap();
    store
        .append(make_event(EntityType::Run, "r2", "transition"))
        .unwrap();

    let results = store
        .query(&EventQuery::by_entity_type(EntityType::Run))
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|e| e.entity_type == EntityType::Run));
}

// ---- query by entity ----

#[test]
fn query_by_entity() {
    let store = InMemoryEventStore::new();
    store
        .append(make_event(EntityType::Task, "t1", "created"))
        .unwrap();
    store
        .append(make_event(EntityType::Task, "t1", "started"))
        .unwrap();
    store
        .append(make_event(EntityType::Task, "t2", "created"))
        .unwrap();

    let results = store
        .query(&EventQuery::by_entity(EntityType::Task, "t1"))
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|e| e.entity_id == "t1"));
}

// ---- query by correlation ----

#[test]
fn query_by_correlation() {
    let store = InMemoryEventStore::new();
    let corr = Uuid::now_v7();

    store
        .append(make_event_with_correlation(
            EntityType::Run,
            "r1",
            "started",
            corr,
        ))
        .unwrap();
    store
        .append(make_event_with_correlation(
            EntityType::Task,
            "t1",
            "created",
            corr,
        ))
        .unwrap();
    // Different correlation.
    store
        .append(make_event(EntityType::Task, "t2", "created"))
        .unwrap();

    let results = store.query(&EventQuery::by_correlation(corr)).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|e| e.correlation_id == corr));
}

// ---- query with limit ----

#[test]
fn query_with_limit() {
    let store = InMemoryEventStore::new();
    for i in 0..5 {
        store
            .append(make_event(EntityType::Run, &format!("r{i}"), "transition"))
            .unwrap();
    }

    let results = store
        .query(&EventQuery::by_entity_type(EntityType::Run).with_limit(3))
        .unwrap();
    assert_eq!(results.len(), 3);
}

// ---- query with pagination (after_event_id) ----

#[test]
fn query_with_pagination() {
    let store = InMemoryEventStore::new();
    let corr = Uuid::now_v7();
    let mut ids = Vec::new();

    for i in 0..5 {
        let e = make_event_with_correlation(EntityType::Run, &format!("r{i}"), "t", corr);
        ids.push(e.event_id);
        store.append(e).unwrap();
    }

    // Get events after the second one.
    let results = store
        .query(&EventQuery::by_correlation(corr).after(ids[1]))
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].event_id, ids[2]);
    assert_eq!(results[1].event_id, ids[3]);
    assert_eq!(results[2].event_id, ids[4]);
}

#[test]
fn query_after_nonexistent_event_returns_error() {
    let store = InMemoryEventStore::new();
    store
        .append(make_event(EntityType::Run, "r1", "t"))
        .unwrap();

    let query = EventQuery::default().after(Uuid::now_v7());
    let err = store.query(&query).unwrap_err();
    assert!(matches!(err, StoreError::EventNotFound(_)));
}

// ---- query by event_type ----

#[test]
fn query_by_event_type() {
    let store = InMemoryEventStore::new();
    store
        .append(make_event(EntityType::Task, "t1", "state_transition"))
        .unwrap();
    store
        .append(make_event(EntityType::Task, "t1", "evidence_attached"))
        .unwrap();
    store
        .append(make_event(EntityType::Task, "t2", "state_transition"))
        .unwrap();

    let mut q = EventQuery::default();
    q.event_type = Some("state_transition".to_string());
    let results = store.query(&q).unwrap();
    assert_eq!(results.len(), 2);
}

// ---- combined filters ----

#[test]
fn query_combined_filters() {
    let store = InMemoryEventStore::new();
    let corr = Uuid::now_v7();

    store
        .append(make_event_with_correlation(
            EntityType::Task,
            "t1",
            "state_transition",
            corr,
        ))
        .unwrap();
    store
        .append(make_event_with_correlation(
            EntityType::Task,
            "t1",
            "evidence_attached",
            corr,
        ))
        .unwrap();
    store
        .append(make_event_with_correlation(
            EntityType::Run,
            "r1",
            "state_transition",
            corr,
        ))
        .unwrap();

    let q = EventQuery {
        entity_type: Some(EntityType::Task),
        entity_id: Some("t1".to_string()),
        correlation_id: Some(corr),
        event_type: Some("state_transition".to_string()),
        limit: None,
        after_event_id: None,
    };
    let results = store.query(&q).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entity_id, "t1");
    assert_eq!(results[0].event_type, "state_transition");
}

// ---- all ----

#[test]
fn all_returns_events_in_order() {
    let store = InMemoryEventStore::new();
    let mut ids = Vec::new();
    for i in 0..3 {
        let e = make_event(EntityType::Run, &format!("r{i}"), "t");
        ids.push(e.event_id);
        store.append(e).unwrap();
    }

    let all = store.all().unwrap();
    assert_eq!(all.len(), 3);
    for (i, event) in all.iter().enumerate() {
        assert_eq!(event.event_id, ids[i]);
    }
}

// ---- empty store ----

#[test]
fn empty_store() {
    let store = InMemoryEventStore::new();
    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
    assert!(store.all().unwrap().is_empty());
}

#[test]
fn is_empty_false_after_append() {
    let store = InMemoryEventStore::new();
    store
        .append(make_event(EntityType::Run, "r1", "t"))
        .unwrap();
    assert!(!store.is_empty());
}

// ---- query empty result ----

#[test]
fn query_returns_empty_for_no_matches() {
    let store = InMemoryEventStore::new();
    store
        .append(make_event(EntityType::Run, "r1", "t"))
        .unwrap();

    let results = store
        .query(&EventQuery::by_entity_type(EntityType::Gate))
        .unwrap();
    assert!(results.is_empty());
}

// ---- event payload round-trip ----

#[test]
fn event_payload_preserved() {
    let store = InMemoryEventStore::new();
    let mut event = make_event(EntityType::Command, "c1", "stream_chunk");
    event.payload = serde_json::json!({
        "stream_type": "stdout",
        "data": "hello world",
        "seq": 42
    });
    let id = event.event_id;
    store.append(event).unwrap();

    let retrieved = store.get(id).unwrap();
    assert_eq!(retrieved.payload["stream_type"], "stdout");
    assert_eq!(retrieved.payload["seq"], 42);
}

// ---- causation chain ----

#[test]
fn causation_chain_queryable() {
    let store = InMemoryEventStore::new();
    let corr = Uuid::now_v7();

    let e1 = make_event_with_correlation(EntityType::Run, "r1", "started", corr);
    let e1_id = e1.event_id;
    store.append(e1).unwrap();

    let mut e2 = make_event_with_correlation(EntityType::Task, "t1", "created", corr);
    e2.causation_id = Some(e1_id);
    let e2_id = e2.event_id;
    store.append(e2).unwrap();

    let mut e3 = make_event_with_correlation(EntityType::Task, "t1", "started", corr);
    e3.causation_id = Some(e2_id);
    store.append(e3).unwrap();

    // All three share the same correlation.
    let all_correlated = store.query(&EventQuery::by_correlation(corr)).unwrap();
    assert_eq!(all_correlated.len(), 3);

    // We can verify causation chain by inspecting events.
    let retrieved_e2 = store.get(e2_id).unwrap();
    assert_eq!(retrieved_e2.causation_id, Some(e1_id));
}

// ---- thread safety (basic) ----

#[test]
fn concurrent_appends() {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(InMemoryEventStore::new());
    let mut handles = Vec::new();

    for i in 0..10 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            let e = make_event(EntityType::Task, &format!("t{i}"), "created");
            store.append(e).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(store.len(), 10);
}

#[test]
fn concurrent_reads_during_writes() {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(InMemoryEventStore::new());

    // Pre-populate.
    for i in 0..5 {
        store
            .append(make_event(EntityType::Run, &format!("r{i}"), "t"))
            .unwrap();
    }

    let mut handles = Vec::new();

    // Writers.
    for i in 5..10 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            let e = make_event(EntityType::Run, &format!("r{i}"), "t");
            store.append(e).unwrap();
        }));
    }

    // Readers.
    for _ in 0..5 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            let _ = store.all().unwrap();
            let _ = store
                .query(&EventQuery::by_entity_type(EntityType::Run))
                .unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(store.len(), 10);
}
