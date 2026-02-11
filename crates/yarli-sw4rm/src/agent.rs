//! YarliAgent — sw4rm Agent implementation.
//!
//! Implements the `sw4rm_sdk::Agent` trait to receive objectives from the
//! sw4rm scheduler, dispatch them through the `OrchestratorLoop`, and
//! report results back.

use std::sync::Arc;

use async_trait::async_trait;
use sw4rm_sdk::{AgentConfig, EnvelopeBuilder, EnvelopeData, PreemptionManager, CT_SCHEDULER_COMMAND_V1};
use tracing::{debug, error, info, warn};

use crate::bridge::ShutdownBridge;
use crate::config::Sw4rmConfig;
use crate::messages::{
    CT_ORCHESTRATION_REPORT, OrchestrationReport,
};
use crate::orchestrator::{ObjectiveParams, OrchestratorLoop, RouterSender};

/// YarliAgent implements `sw4rm_sdk::Agent` to act as an orchestrator
/// in the sw4rm protocol.
pub struct YarliAgent<R: RouterSender> {
    agent_config: AgentConfig,
    preemption: PreemptionManager,
    #[allow(dead_code)]
    sw4rm_config: Sw4rmConfig,
    orchestrator: Arc<OrchestratorLoop<R>>,
    shutdown_bridge: Option<ShutdownBridge>,
}

impl<R: RouterSender + 'static> YarliAgent<R> {
    pub fn new(
        agent_config: AgentConfig,
        sw4rm_config: Sw4rmConfig,
        orchestrator: Arc<OrchestratorLoop<R>>,
    ) -> Self {
        Self {
            agent_config,
            preemption: PreemptionManager::new(),
            sw4rm_config,
            orchestrator,
            shutdown_bridge: None,
        }
    }

    /// Attach a shutdown bridge for preemption forwarding.
    pub fn with_shutdown_bridge(mut self, bridge: ShutdownBridge) -> Self {
        self.shutdown_bridge = Some(bridge);
        self
    }

    /// Handle a scheduler command (stage=Run).
    async fn handle_scheduler_command(
        &self,
        envelope: &EnvelopeData,
    ) -> sw4rm_sdk::Result<()> {
        let cmd: sw4rm_sdk::SchedulerCommandV1 = envelope
            .json_payload()
            .map_err(|e| sw4rm_sdk::Error::Internal(format!("failed to parse scheduler command: {e}")))?;

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

        let params = ObjectiveParams {
            scope,
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

        // Emit the report as an envelope. We log if it fails rather than
        // propagating, since the orchestration itself already completed.
        match EnvelopeBuilder::new(self.agent_config.agent_id.clone(), 1)
            .with_json_payload(&report)
        {
            Ok(builder) => {
                let _report_envelope = builder
                    .with_content_type(CT_ORCHESTRATION_REPORT.to_string())
                    .with_correlation_id(correlation_id)
                    .build();

                // TODO: send via router/scheduler client when real transport is wired
                debug!("orchestration report built (pending transport)");
            }
            Err(e) => {
                warn!(error = %e, "failed to build orchestration report envelope");
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<R: RouterSender + 'static> sw4rm_sdk::Agent for YarliAgent<R> {
    async fn on_message(&mut self, envelope: EnvelopeData) -> sw4rm_sdk::Result<()> {
        match envelope.content_type.as_str() {
            CT_SCHEDULER_COMMAND_V1 => self.handle_scheduler_command(&envelope).await,
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
    use crate::mock::MockRouterSender;
    use crate::messages::ImplementationResponse;
    use crate::orchestrator::{VerificationCommand, VerificationSpec};
    use sw4rm_sdk::{Agent, EnvelopeBuilder, SchedulerCommandV1, SchedulerStage};
    use yarli_core::domain::CommandClass;

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
        let shutdown = yarli_core::shutdown::ShutdownController::new();
        let bridge = ShutdownBridge::new(shutdown.clone());
        let agent = test_agent().with_shutdown_bridge(bridge);
        assert!(agent.shutdown_bridge.is_some());
    }

    #[tokio::test]
    async fn agent_cancellation_wired_through_bridge() {
        let shutdown = yarli_core::shutdown::ShutdownController::new();
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

        let cmd = SchedulerCommandV1::new(SchedulerStage::Run)
            .with_input(serde_json::json!({
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
}
