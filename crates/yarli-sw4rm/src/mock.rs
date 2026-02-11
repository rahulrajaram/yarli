//! Mock implementations for testing.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::messages::{ImplementationRequest, ImplementationResponse};
use crate::orchestrator::RouterSender;

/// Mock router sender that records sent requests and returns canned responses.
#[derive(Debug, Clone)]
pub struct MockRouterSender {
    /// Recorded requests in order.
    pub sent_requests: Arc<Mutex<Vec<ImplementationRequest>>>,
    /// Canned responses to return (popped in order).
    responses: Arc<Mutex<Vec<ImplementationResponse>>>,
}

impl MockRouterSender {
    pub fn new() -> Self {
        Self {
            sent_requests: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Queue a response to be returned by the next `send_implementation_request` call.
    pub fn enqueue_response(&self, response: ImplementationResponse) {
        // Use try_lock since this is called from non-async context in tests
        self.responses.try_lock().unwrap().push(response);
    }

    /// Get all sent requests.
    pub async fn requests(&self) -> Vec<ImplementationRequest> {
        self.sent_requests.lock().await.clone()
    }
}

impl Default for MockRouterSender {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RouterSender for MockRouterSender {
    async fn send_implementation_request(
        &self,
        request: ImplementationRequest,
    ) -> Result<ImplementationResponse, crate::orchestrator::OrchestratorError> {
        self.sent_requests.lock().await.push(request);
        let mut responses = self.responses.lock().await;
        if responses.is_empty() {
            return Err(crate::orchestrator::OrchestratorError::LlmTimeout);
        }
        Ok(responses.remove(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_records_requests_and_returns_responses() {
        let mock = MockRouterSender::new();
        mock.enqueue_response(ImplementationResponse {
            complete: true,
            files_modified: vec!["a.rs".to_string()],
            summary: "done".to_string(),
            additional_verification: vec![],
        });

        let req = ImplementationRequest {
            objective: "test".to_string(),
            scope: vec![],
            failures: vec![],
            iteration: 1,
            correlation_id: "c1".to_string(),
            repo_context: None,
        };

        let resp = mock.send_implementation_request(req.clone()).await.unwrap();
        assert!(resp.complete);
        assert_eq!(resp.files_modified, vec!["a.rs"]);

        let sent = mock.requests().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].objective, "test");
    }

    #[tokio::test]
    async fn mock_returns_error_when_no_responses_queued() {
        let mock = MockRouterSender::new();
        let req = ImplementationRequest {
            objective: "test".to_string(),
            scope: vec![],
            failures: vec![],
            iteration: 1,
            correlation_id: "c1".to_string(),
            repo_context: None,
        };
        let result = mock.send_implementation_request(req).await;
        assert!(result.is_err());
    }
}
