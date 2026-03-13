//! sw4rm gRPC transport with correlation-based request/response.
//!
//! The sw4rm protocol uses fire-and-forget gRPC sends. To implement the
//! synchronous `RouterSender` trait (send request → await response), we
//! maintain a [`CorrelationRegistry`] that maps correlation IDs to oneshot
//! channels. The response path (in `YarliAgent::on_message`) completes
//! the channel when a matching response arrives.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::yarli_sw4rm::messages::{
    ImplementationRequest, ImplementationResponse, CT_IMPLEMENTATION_REQUEST,
};
use crate::yarli_sw4rm::orchestrator::{OrchestratorError, RouterSender};

/// Registry of in-flight correlation IDs awaiting responses.
///
/// Thread-safe via `DashMap` — multiple orchestrator loops can register
/// correlations concurrently.
pub struct CorrelationRegistry {
    pending: DashMap<Uuid, oneshot::Sender<ImplementationResponse>>,
}

impl CorrelationRegistry {
    pub fn new() -> Self {
        Self {
            pending: DashMap::new(),
        }
    }

    /// Register a new correlation and return a receiver for the response.
    pub fn register(&self, correlation_id: Uuid) -> oneshot::Receiver<ImplementationResponse> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert(correlation_id, tx);
        rx
    }

    /// Complete a pending correlation with a response.
    ///
    /// Returns `true` if the correlation was found and completed, `false` if
    /// the correlation ID was unknown (already completed or never registered).
    pub fn complete(&self, correlation_id: Uuid, response: ImplementationResponse) -> bool {
        if let Some((_, tx)) = self.pending.remove(&correlation_id) {
            tx.send(response).is_ok()
        } else {
            false
        }
    }

    /// Cancel a pending correlation (e.g. on timeout or shutdown).
    ///
    /// Drops the sender, causing the receiver to get a `RecvError`.
    pub fn cancel(&self, correlation_id: Uuid) {
        self.pending.remove(&correlation_id);
    }

    /// Number of in-flight correlations.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

impl Default for CorrelationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// RouterSender implementation backed by a real sw4rm gRPC client.
///
/// Sends implementation requests as envelopes and awaits responses via
/// the correlation registry.
pub struct GrpcRouterSender {
    client: Mutex<sw4rm_sdk::RouterClient>,
    correlations: Arc<CorrelationRegistry>,
    agent_id: String,
    timeout: Duration,
}

impl GrpcRouterSender {
    pub fn new(
        client: sw4rm_sdk::RouterClient,
        correlations: Arc<CorrelationRegistry>,
        agent_id: String,
        timeout: Duration,
    ) -> Self {
        Self {
            client: Mutex::new(client),
            correlations,
            agent_id,
            timeout,
        }
    }
}

#[async_trait]
impl RouterSender for GrpcRouterSender {
    async fn send_implementation_request(
        &self,
        request: ImplementationRequest,
    ) -> Result<ImplementationResponse, OrchestratorError> {
        let correlation_id = Uuid::now_v7();
        let rx = self.correlations.register(correlation_id);

        // Build and send the envelope.
        let envelope = sw4rm_sdk::EnvelopeBuilder::new(self.agent_id.clone(), 1)
            .with_json_payload(&request)
            .map_err(|e| OrchestratorError::Scheduler(format!("envelope build: {e}")))?
            .with_content_type(CT_IMPLEMENTATION_REQUEST.to_string())
            .with_correlation_id(correlation_id.to_string())
            .build();

        {
            let mut client = self.client.lock().await;
            client
                .send_message(&envelope)
                .await
                .map_err(|e| OrchestratorError::Scheduler(format!("grpc send: {e}")))?;
        }

        debug!(
            correlation_id = %correlation_id,
            "implementation request sent, awaiting response"
        );

        // Await the response with timeout.
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                // Sender was dropped (cancelled).
                Err(OrchestratorError::Cancelled)
            }
            Err(_) => {
                // Timeout — clean up the pending correlation.
                self.correlations.cancel(correlation_id);
                warn!(
                    correlation_id = %correlation_id,
                    timeout = ?self.timeout,
                    "implementation request timed out"
                );
                Err(OrchestratorError::LlmTimeout)
            }
        }
    }
}

/// Fire-and-forget sender for orchestration reports.
pub struct ReportSender {
    client: Mutex<sw4rm_sdk::RouterClient>,
    agent_id: String,
}

impl ReportSender {
    pub fn new(client: sw4rm_sdk::RouterClient, agent_id: String) -> Self {
        Self {
            client: Mutex::new(client),
            agent_id,
        }
    }

    /// Send a pre-built envelope to the router.
    pub async fn send_envelope(
        &self,
        envelope: &sw4rm_sdk::EnvelopeData,
    ) -> Result<(), sw4rm_sdk::Error> {
        let mut client = self.client.lock().await;
        client.send_message(envelope).await?;
        Ok(())
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_register_and_complete() {
        let registry = CorrelationRegistry::new();
        let id = Uuid::now_v7();
        let mut rx = registry.register(id);

        let response = ImplementationResponse {
            complete: true,
            files_modified: vec!["a.rs".to_string()],
            summary: "done".to_string(),
            additional_verification: vec![],
        };

        assert!(registry.complete(id, response.clone()));
        let received = rx.try_recv().unwrap();
        assert_eq!(received, response);
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn correlation_cancel_drops_sender() {
        let registry = CorrelationRegistry::new();
        let id = Uuid::now_v7();
        let mut rx = registry.register(id);

        registry.cancel(id);
        assert!(rx.try_recv().is_err());
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn correlation_complete_unknown_id_returns_false() {
        let registry = CorrelationRegistry::new();
        let response = ImplementationResponse {
            complete: true,
            files_modified: vec![],
            summary: "done".to_string(),
            additional_verification: vec![],
        };
        assert!(!registry.complete(Uuid::now_v7(), response));
    }

    #[tokio::test]
    async fn correlation_timeout_cleans_up() {
        let registry = Arc::new(CorrelationRegistry::new());
        let id = Uuid::now_v7();
        let rx = registry.register(id);

        // Simulate timeout: cancel without completing.
        registry.cancel(id);
        assert_eq!(registry.pending_count(), 0);

        // Receiver should error.
        assert!(rx.await.is_err());
    }

    #[test]
    fn correlation_multiple_concurrent() {
        let registry = CorrelationRegistry::new();
        let id1 = Uuid::now_v7();
        let id2 = Uuid::now_v7();
        let mut rx1 = registry.register(id1);
        let mut rx2 = registry.register(id2);

        assert_eq!(registry.pending_count(), 2);

        let resp1 = ImplementationResponse {
            complete: true,
            files_modified: vec![],
            summary: "first".to_string(),
            additional_verification: vec![],
        };
        let resp2 = ImplementationResponse {
            complete: false,
            files_modified: vec!["b.rs".to_string()],
            summary: "second".to_string(),
            additional_verification: vec![],
        };

        assert!(registry.complete(id1, resp1));
        assert!(registry.complete(id2, resp2));

        assert_eq!(rx1.try_recv().unwrap().summary, "first");
        assert_eq!(rx2.try_recv().unwrap().summary, "second");
        assert_eq!(registry.pending_count(), 0);
    }

    #[test]
    fn correlation_default_trait() {
        let registry = CorrelationRegistry::default();
        assert_eq!(registry.pending_count(), 0);
    }
}
