//! Cross-system bridges between sw4rm and yarli infrastructure.
//!
//! - `ShutdownBridge`: forwards sw4rm preemption requests to yarli's
//!   `ShutdownController` so that running tasks are gracefully cancelled.
//! - `log_envelope_to_store`: persists sw4rm envelopes to yarli's EventStore
//!   for audit trail.

use chrono::Utc;
use sw4rm_sdk::{EnvelopeData, PreemptionManager};
use tracing::info;
use uuid::Uuid;

use yarli_core::domain::CancellationSource;
use yarli_core::domain::{EntityType, Event};
use yarli_core::shutdown::ShutdownController;
use yarli_store::EventStore;

/// Bridges sw4rm preemption to yarli's shutdown controller.
#[derive(Clone)]
pub struct ShutdownBridge {
    shutdown: ShutdownController,
}

impl ShutdownBridge {
    pub fn new(shutdown: ShutdownController) -> Self {
        Self { shutdown }
    }

    /// Get the underlying `CancellationToken` for use in async select loops.
    /// This token is cancelled when `trigger_graceful()` is called.
    pub fn token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown.token()
    }

    /// Trigger a graceful shutdown (equivalent to first Ctrl+C).
    pub fn trigger_graceful(&self) {
        info!("sw4rm preemption → yarli graceful shutdown");
        self.shutdown
            .request_graceful_with_source(CancellationSource::Sw4rmPreemption);
    }

    /// Spawn a background task that watches a `PreemptionManager` and
    /// triggers yarli shutdown when preemption is requested.
    pub fn spawn_watcher(
        &self,
        preemption: PreemptionManager,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let bridge = self.clone();
        tokio::spawn(async move {
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                if preemption.is_preemption_requested() {
                    let reason = preemption
                        .preemption_reason()
                        .unwrap_or_else(|| "unknown".to_string());
                    info!(reason = %reason, "preemption detected, triggering shutdown");
                    bridge.trigger_graceful();
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        })
    }
}

/// Persist a sw4rm envelope to the yarli event store for audit.
pub fn log_envelope_to_store<S: EventStore>(
    store: &S,
    envelope: &EnvelopeData,
) -> Result<(), yarli_store::StoreError> {
    let event = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Run,
        entity_id: Uuid::nil().to_string(),
        event_type: format!("sw4rm.envelope.{}", envelope.content_type),
        payload: serde_json::json!({
            "message_id": envelope.message_id,
            "producer_id": envelope.producer_id,
            "correlation_id": envelope.correlation_id,
            "content_type": envelope.content_type,
            "content_length": envelope.content_length,
            "sequence_number": envelope.sequence_number,
        }),
        correlation_id: Uuid::nil(),
        causation_id: None,
        actor: "yarli-sw4rm".to_string(),
        idempotency_key: None,
    };
    store.append(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sw4rm_sdk::EnvelopeBuilder;
    use tokio_util::sync::CancellationToken;
    use yarli_store::InMemoryEventStore;

    #[test]
    fn shutdown_bridge_clone_shares_state() {
        let ctrl = ShutdownController::new();
        let bridge1 = ShutdownBridge::new(ctrl.clone());
        let bridge2 = bridge1.clone();
        bridge2.trigger_graceful();
        assert!(ctrl.token().is_cancelled());
    }

    #[test]
    fn bridge_token_reflects_shutdown_state() {
        let ctrl = ShutdownController::new();
        let bridge = ShutdownBridge::new(ctrl.clone());
        let token = bridge.token();
        assert!(!token.is_cancelled());
        ctrl.request_graceful();
        assert!(token.is_cancelled());
    }

    #[test]
    fn trigger_graceful_cancels_token() {
        let ctrl = ShutdownController::new();
        let bridge = ShutdownBridge::new(ctrl.clone());
        assert!(!ctrl.token().is_cancelled());
        bridge.trigger_graceful();
        assert!(ctrl.token().is_cancelled());
    }

    #[tokio::test]
    async fn watcher_triggers_on_preemption() {
        let ctrl = ShutdownController::new();
        let bridge = ShutdownBridge::new(ctrl.clone());
        let preemption = PreemptionManager::new();
        let cancel = CancellationToken::new();

        let handle = bridge.spawn_watcher(preemption.clone(), cancel.clone());

        preemption.request_preemption(Some("test reason".to_string()));

        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("watcher should complete")
            .expect("watcher should not panic");

        assert!(ctrl.token().is_cancelled());
    }

    #[tokio::test]
    async fn watcher_stops_on_cancel() {
        let ctrl = ShutdownController::new();
        let bridge = ShutdownBridge::new(ctrl.clone());
        let preemption = PreemptionManager::new();
        let cancel = CancellationToken::new();

        let handle = bridge.spawn_watcher(preemption, cancel.clone());
        cancel.cancel();

        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("watcher should complete")
            .expect("watcher should not panic");

        assert!(!ctrl.token().is_cancelled());
    }

    #[test]
    fn log_envelope_persists_event() {
        let store = InMemoryEventStore::new();
        let envelope = EnvelopeBuilder::new("test-producer".to_string(), 1)
            .with_content_type("application/test".to_string())
            .with_correlation_id("corr-123".to_string())
            .with_payload(b"hello".to_vec())
            .build();

        log_envelope_to_store(&store, &envelope).unwrap();

        let events = store.all().unwrap();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(
            event.event_type.contains("sw4rm.envelope"),
            "event_type was: {}",
            event.event_type
        );
        assert_eq!(event.payload["producer_id"], "test-producer");
        assert_eq!(event.payload["correlation_id"], "corr-123");
    }

    #[test]
    fn log_envelope_captures_metadata() {
        let store = InMemoryEventStore::new();
        let envelope = EnvelopeBuilder::new("producer-abc".to_string(), 2)
            .with_content_type("application/vnd.test".to_string())
            .with_correlation_id("xyz".to_string())
            .with_sequence_number(42)
            .with_payload(b"data".to_vec())
            .build();

        log_envelope_to_store(&store, &envelope).unwrap();

        let events = store.all().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["sequence_number"], 42);
    }
}
