//! YarliAgent — sw4rm Agent implementation.
//!
//! Implements the `sw4rm_sdk::Agent` trait to receive objectives from the
//! sw4rm scheduler, dispatch them through the `OrchestratorLoop`, and
//! report results back.

use std::sync::Arc;

use async_trait::async_trait;
use sw4rm_sdk::{
    AgentConfig, EnvelopeBuilder, EnvelopeData, PreemptionManager, CT_SCHEDULER_COMMAND_V1,
};
use tracing::{debug, error, info, warn};

use crate::yarli_exec::{CommandRunner, LocalCommandRunner};
use crate::yarli_sw4rm::bridge::ShutdownBridge;
use crate::yarli_sw4rm::config::Sw4rmConfig;
use crate::yarli_sw4rm::messages::{
    OrchestrationReport, CT_IMPLEMENTATION_RESPONSE, CT_ORCHESTRATION_REPORT,
};
use crate::yarli_sw4rm::orchestrator::{ObjectiveParams, OrchestratorLoop, RouterSender};
use crate::yarli_sw4rm::transport::{CorrelationRegistry, ReportSender};

/// YarliAgent implements `sw4rm_sdk::Agent` to act as an orchestrator
/// in the sw4rm protocol.
pub struct YarliAgent<R: RouterSender, V: CommandRunner = LocalCommandRunner> {
    agent_config: AgentConfig,
    preemption: PreemptionManager,
    #[allow(dead_code)]
    sw4rm_config: Sw4rmConfig,
    orchestrator: Arc<OrchestratorLoop<R, V>>,
    shutdown_bridge: Option<ShutdownBridge>,
    /// Correlation registry shared with the GrpcRouterSender.
    /// When a response arrives in `on_message`, the registry completes the
    /// corresponding oneshot channel.
    correlations: Option<Arc<CorrelationRegistry>>,
    /// Report sender for delivering orchestration reports to the sw4rm router.
    report_sender: Option<Arc<ReportSender>>,
}

impl<R: RouterSender + 'static, V: CommandRunner + Clone + 'static> YarliAgent<R, V> {
    pub fn new(
        agent_config: AgentConfig,
        sw4rm_config: Sw4rmConfig,
        orchestrator: Arc<OrchestratorLoop<R, V>>,
    ) -> Self {
        Self {
            agent_config,
            preemption: PreemptionManager::new(),
            sw4rm_config,
            orchestrator,
            shutdown_bridge: None,
            correlations: None,
            report_sender: None,
        }
    }

    /// Attach a correlation registry (shared with GrpcRouterSender).
    pub fn with_correlations(mut self, correlations: Arc<CorrelationRegistry>) -> Self {
        self.correlations = Some(correlations);
        self
    }

    /// Attach a report sender for delivering orchestration reports.
    pub fn with_report_sender(mut self, sender: Arc<ReportSender>) -> Self {
        self.report_sender = Some(sender);
        self
    }

    /// Attach a shutdown bridge for preemption forwarding.
    pub fn with_shutdown_bridge(mut self, bridge: ShutdownBridge) -> Self {
        self.shutdown_bridge = Some(bridge);
        self
    }

    /// Handle a scheduler command (stage=Run).
    async fn handle_scheduler_command(&self, envelope: &EnvelopeData) -> sw4rm_sdk::Result<()> {
        let cmd: sw4rm_sdk::SchedulerCommandV1 = envelope.json_payload().map_err(|e| {
            sw4rm_sdk::Error::Internal(format!("failed to parse scheduler command: {e}"))
        })?;

        let objective = cmd
            .input
            .as_ref()
            .and_then(|v| v.get("objective"))
            .and_then(|v| v.as_str())
            .unwrap_or("(no objective)")
            .to_string();

        info!(
            stage = ?cmd.stage,
            objective = %objective,
            correlation_id = %envelope.correlation_id,
            "received scheduler command"
        );

        // Fix #1: Wire cancellation from ShutdownBridge into the orchestrator's
        // CancellationToken so preemption actually stops the loop.
        let cancel = match &self.shutdown_bridge {
            Some(bridge) => bridge.token(),
            None => tokio_util::sync::CancellationToken::new(),
        };

        let correlation_id = envelope.correlation_id.clone();

        // Fix #8: Extract scope from scheduler command input if provided
        let scope = cmd
            .input
            .as_ref()
            .and_then(|v| v.get("scope"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let completed_tranche_work = cmd
            .input
            .as_ref()
            .and_then(|v| v.get("completed_tranche_work"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let params = ObjectiveParams {
            scope,
            completed_tranche_work,
            repo_context: None,
        };

        let result = self
            .orchestrator
            .run_objective(&objective, &correlation_id, cancel, &params)
            .await;

        // Fix #3: Build a report to send back to the sw4rm scheduler.
        let report = match &result {
            Ok(outcome) => {
                info!(
                    success = outcome.success,
                    iterations = outcome.iterations_used,
                    "orchestration complete"
                );
                OrchestrationReport {
                    success: outcome.success,
                    iterations_used: outcome.iterations_used,
                    files_modified: outcome.files_modified.clone(),
                    final_summary: outcome.final_summary.clone(),
                    remaining_failures: outcome.remaining_failures.clone(),
                    error: None,
                }
            }
            Err(e) => {
                error!(error = %e, "orchestration failed");
                OrchestrationReport {
                    success: false,
                    iterations_used: 0,
                    files_modified: vec![],
                    final_summary: String::new(),
                    remaining_failures: vec![],
                    error: Some(e.to_string()),
                }
            }
        };

        // Emit the report as an envelope via the report sender (if wired).
        match EnvelopeBuilder::new(self.agent_config.agent_id.clone(), 1).with_json_payload(&report)
        {
            Ok(builder) => {
                let report_envelope = builder
                    .with_content_type(CT_ORCHESTRATION_REPORT.to_string())
                    .with_correlation_id(correlation_id)
                    .build();

                if let Some(ref sender) = self.report_sender {
                    if let Err(e) = sender.send_envelope(&report_envelope).await {
                        warn!(error = %e, "failed to send orchestration report");
                    } else {
                        debug!("orchestration report sent via transport");
                    }
                } else {
                    debug!("orchestration report built (no transport wired)");
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to build orchestration report envelope");
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<R: RouterSender + 'static, V: CommandRunner + Clone + 'static> sw4rm_sdk::Agent
    for YarliAgent<R, V>
{
    async fn on_message(&mut self, envelope: EnvelopeData) -> sw4rm_sdk::Result<()> {
        match envelope.content_type.as_str() {
            CT_SCHEDULER_COMMAND_V1 => self.handle_scheduler_command(&envelope).await,
            CT_IMPLEMENTATION_RESPONSE => {
                // Complete the pending correlation so GrpcRouterSender unblocks.
                if let Some(ref registry) = self.correlations {
                    match envelope
                        .json_payload::<crate::yarli_sw4rm::messages::ImplementationResponse>()
                    {
                        Ok(response) => {
                            let corr_id = uuid::Uuid::parse_str(&envelope.correlation_id)
                                .unwrap_or_else(|_| uuid::Uuid::nil());
                            if registry.complete(corr_id, response) {
                                debug!(
                                    correlation_id = %envelope.correlation_id,
                                    "implementation response completed correlation"
                                );
                            } else {
                                warn!(
                                    correlation_id = %envelope.correlation_id,
                                    "no pending correlation for implementation response"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                "failed to parse implementation response payload"
                            );
                        }
                    }
                }
                Ok(())
            }
            other => {
                debug!(content_type = other, "ignoring unknown message type");
                Ok(())
            }
        }
    }

    async fn on_control(&mut self, envelope: EnvelopeData) -> sw4rm_sdk::Result<()> {
        info!(
            message_type = envelope.message_type,
            "received control message"
        );

        if let Some(bridge) = &self.shutdown_bridge {
            bridge.trigger_graceful();
        }

        self.preemption
            .request_preemption(Some("control message received".to_string()));

        Ok(())
    }

    fn config(&self) -> &AgentConfig {
        &self.agent_config
    }

    fn preemption_manager(&self) -> &PreemptionManager {
        &self.preemption
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yarli_core::domain::CommandClass;
    use crate::yarli_sw4rm::messages::{
        ImplementationResponse, CT_IMPLEMENTATION_REQUEST, CT_IMPLEMENTATION_RESPONSE,
    };
    use crate::yarli_sw4rm::mock::MockRouterSender;
    use crate::yarli_sw4rm::orchestrator::{VerificationCommand, VerificationSpec};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use sw4rm_sdk::{
        proto::sw4rm, Agent, EnvelopeBuilder, RouterClient, SchedulerCommandV1, SchedulerStage,
    };
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot, Mutex};
    use tokio_stream::{
        wrappers::{TcpListenerStream, UnboundedReceiverStream},
        Stream, StreamExt,
    };
    use tonic::{self, Request, Response, Status};

    use crate::yarli_sw4rm::transport::{CorrelationRegistry, GrpcRouterSender};

    #[derive(Clone, Default)]
    struct MockRouterService {
        messages: Arc<tokio::sync::Mutex<Vec<sw4rm::common::Envelope>>>,
    }

    #[async_trait::async_trait]
    impl sw4rm::router::router_service_server::RouterService for MockRouterService {
        async fn send_message(
            &self,
            request: Request<sw4rm::router::SendMessageRequest>,
        ) -> Result<Response<sw4rm::router::SendMessageResponse>, Status> {
            let req = request.into_inner();

            if let Some(envelope) = req.msg {
                self.messages.lock().await.push(envelope);
                Ok(Response::new(sw4rm::router::SendMessageResponse {
                    accepted: true,
                    reason: "ok".to_string(),
                }))
            } else {
                Ok(Response::new(sw4rm::router::SendMessageResponse {
                    accepted: false,
                    reason: "missing envelope".to_string(),
                }))
            }
        }

        type StreamIncomingStream =
            std::pin::Pin<Box<dyn Stream<Item = Result<sw4rm::router::StreamItem, Status>> + Send>>;

        async fn stream_incoming(
            &self,
            _request: Request<sw4rm::router::StreamRequest>,
        ) -> Result<Response<Self::StreamIncomingStream>, Status> {
            let (_tx, rx) = mpsc::unbounded_channel();
            let stream = UnboundedReceiverStream::new(rx).map(Ok);
            Ok(Response::new(Box::pin(stream)))
        }
    }

    impl MockRouterService {
        async fn pop_message(&self) -> Option<sw4rm::common::Envelope> {
            self.messages.lock().await.pop()
        }

        async fn len(&self) -> usize {
            self.messages.lock().await.len()
        }
    }

    struct MockSw4rmRouter {
        service: MockRouterService,
        endpoint: String,
        shutdown: Option<oneshot::Sender<()>>,
        handle: tokio::task::JoinHandle<()>,
    }

    impl MockSw4rmRouter {
        async fn start() -> Self {
            let service = MockRouterService::default();
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("mock router bind");
            let addr: SocketAddr = listener.local_addr().expect("mock router local addr");

            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let service_for_server = service.clone();
            let endpoint = format!("http://{addr}");

            let handle = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(
                        sw4rm::router::router_service_server::RouterServiceServer::new(
                            service_for_server,
                        ),
                    )
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                        shutdown_rx.await.ok();
                    })
                    .await
                    .expect("mock router server");
            });

            Self {
                service,
                endpoint,
                shutdown: Some(shutdown_tx),
                handle,
            }
        }

        async fn message_once(&self) -> sw4rm::common::Envelope {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                let maybe_msg = self.service.pop_message().await;
                if let Some(msg) = maybe_msg {
                    return msg;
                }
                if tokio::time::Instant::now() > deadline {
                    panic!("timed out waiting for mock router message");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        fn endpoint(&self) -> String {
            self.endpoint.clone()
        }

        async fn message_count(&self) -> usize {
            self.service.len().await
        }

        async fn shutdown(mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
            let _ = self.handle.await;
        }
    }

    async fn test_agent_with_grpc() -> (
        std::sync::Arc<tokio::sync::Mutex<YarliAgent<GrpcRouterSender>>>,
        Arc<CorrelationRegistry>,
        MockSw4rmRouter,
    ) {
        let agent_config = AgentConfig::new("grpc-agent".into(), "gRPC Agent".into());
        let sw4rm_config = Sw4rmConfig {
            llm_response_timeout_secs: 1,
            max_fix_iterations: 1,
            ..Sw4rmConfig::default()
        };

        let mock_router = MockSw4rmRouter::start().await;
        let router_client = RouterClient::new(&mock_router.endpoint())
            .await
            .expect("router client");

        let correlations = Arc::new(CorrelationRegistry::new());
        let router_sender = Arc::new(GrpcRouterSender::new(
            router_client,
            correlations.clone(),
            "grpc-agent".to_string(),
            Duration::from_secs(1),
        ));

        let verification = VerificationSpec {
            commands: vec![],
            working_dir: "/tmp".to_string(),
            task_gates: Some(vec![]),
            run_gates: Some(vec![]),
        };
        let orchestrator = Arc::new(OrchestratorLoop::new(
            router_sender,
            sw4rm_config.clone(),
            verification,
        ));

        let agent = YarliAgent::new(agent_config, sw4rm_config, orchestrator)
            .with_correlations(correlations.clone());

        (Arc::new(Mutex::new(agent)), correlations, mock_router)
    }

    fn test_agent() -> YarliAgent<MockRouterSender> {
        let config = AgentConfig::new("test-agent".into(), "Test Agent".into());
        let sw4rm_config = Sw4rmConfig {
            max_fix_iterations: 2,
            ..Sw4rmConfig::default()
        };

        let mock_router = Arc::new(MockRouterSender::new());
        mock_router.enqueue_response(ImplementationResponse {
            complete: true,
            files_modified: vec![],
            summary: "done".to_string(),
            additional_verification: vec![],
        });

        let verification = VerificationSpec {
            commands: vec![VerificationCommand {
                task_key: "test".to_string(),
                command: "echo ok".to_string(),
                class: CommandClass::Io,
            }],
            working_dir: "/tmp".to_string(),
            task_gates: Some(vec![]),
            run_gates: Some(vec![]),
        };

        let orchestrator = Arc::new(OrchestratorLoop::new(
            mock_router,
            sw4rm_config.clone(),
            verification,
        ));

        YarliAgent::new(config, sw4rm_config, orchestrator)
    }

    #[tokio::test]
    async fn agent_handles_scheduler_command() {
        let mut agent = test_agent();

        let cmd = SchedulerCommandV1::new(SchedulerStage::Run)
            .with_input(serde_json::json!({"objective": "implement feature X"}));

        let envelope = EnvelopeBuilder::new("scheduler".to_string(), 1)
            .with_json_payload(&cmd)
            .unwrap()
            .with_content_type(CT_SCHEDULER_COMMAND_V1.to_string())
            .with_correlation_id("corr-agent-1".to_string())
            .build();

        let result = agent.on_message(envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn agent_ignores_unknown_content_type() {
        let mut agent = test_agent();

        let envelope = EnvelopeBuilder::new("unknown".to_string(), 1)
            .with_content_type("application/unknown".to_string())
            .with_payload(b"hello".to_vec())
            .build();

        let result = agent.on_message(envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn agent_control_triggers_preemption() {
        let mut agent = test_agent();

        let envelope = EnvelopeBuilder::new("scheduler".to_string(), 2)
            .with_content_type("application/control".to_string())
            .with_payload(b"preempt".to_vec())
            .build();

        agent.on_control(envelope).await.unwrap();
        assert!(agent.preemption_manager().is_preemption_requested());
    }

    #[test]
    fn agent_config_accessible() {
        let agent = test_agent();
        assert_eq!(agent.config().agent_id, "test-agent");
    }

    #[tokio::test]
    async fn agent_with_shutdown_bridge() {
        let shutdown = crate::yarli_core::shutdown::ShutdownController::new();
        let bridge = ShutdownBridge::new(shutdown.clone());
        let agent = test_agent().with_shutdown_bridge(bridge);
        assert!(agent.shutdown_bridge.is_some());
    }

    #[tokio::test]
    async fn agent_cancellation_wired_through_bridge() {
        let shutdown = crate::yarli_core::shutdown::ShutdownController::new();
        let bridge = ShutdownBridge::new(shutdown.clone());

        // Pre-cancel the shutdown controller
        shutdown.request_graceful();

        let mut agent = test_agent().with_shutdown_bridge(bridge);

        let cmd = SchedulerCommandV1::new(SchedulerStage::Run)
            .with_input(serde_json::json!({"objective": "should be cancelled"}));

        let envelope = EnvelopeBuilder::new("scheduler".to_string(), 1)
            .with_json_payload(&cmd)
            .unwrap()
            .with_content_type(CT_SCHEDULER_COMMAND_V1.to_string())
            .with_correlation_id("corr-cancel-test".to_string())
            .build();

        // Should complete without error (cancelled orchestration is reported, not propagated)
        let result = agent.on_message(envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn scheduler_command_with_scope() {
        let mut agent = test_agent();

        let cmd = SchedulerCommandV1::new(SchedulerStage::Run).with_input(serde_json::json!({
            "objective": "fix module",
            "scope": ["src/lib.rs", "src/util.rs"]
        }));

        let envelope = EnvelopeBuilder::new("scheduler".to_string(), 1)
            .with_json_payload(&cmd)
            .unwrap()
            .with_content_type(CT_SCHEDULER_COMMAND_V1.to_string())
            .with_correlation_id("corr-scope-1".to_string())
            .build();

        let result = agent.on_message(envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn agent_grpc_transport_dispatches_and_completes_correlation_response() {
        let (agent, correlations, mock_router) = test_agent_with_grpc().await;
        let scheduler_cmd = SchedulerCommandV1::new(SchedulerStage::Run)
            .with_input(serde_json::json!({"objective": "run transport path"}));
        let scheduler_envelope = EnvelopeBuilder::new("scheduler".to_string(), 1)
            .with_json_payload(&scheduler_cmd)
            .unwrap()
            .with_content_type(CT_SCHEDULER_COMMAND_V1.to_string())
            .with_correlation_id("scheduler-correlation".to_string())
            .build();

        let run_agent = agent.clone();
        let run_handle = tokio::spawn(async move {
            let mut agent = run_agent.lock().await;
            agent.on_message(scheduler_envelope).await
        });

        // Capture the routed request envelope and echo its correlation id back through on_message.
        let outbound = mock_router.message_once().await;
        assert_eq!(outbound.content_type, CT_IMPLEMENTATION_REQUEST);

        let response = ImplementationResponse {
            complete: true,
            files_modified: vec!["src/lib.rs".to_string()],
            summary: "implemented".to_string(),
            additional_verification: vec![],
        };
        let response_envelope = EnvelopeBuilder::new("grpc-agent".to_string(), 1)
            .with_json_payload(&response)
            .unwrap()
            .with_content_type(CT_IMPLEMENTATION_RESPONSE.to_string())
            .with_correlation_id(outbound.correlation_id.clone())
            .build();

        {
            let mut agent = agent.lock().await;
            let response_result = agent.on_message(response_envelope).await;
            assert!(response_result.is_ok());
        }

        let outcome = run_handle.await.expect("join");
        assert!(outcome.is_ok());
        assert!(mock_router.message_count().await <= 1);
        assert_eq!(correlations.pending_count(), 0);

        mock_router.shutdown().await;
    }
}
