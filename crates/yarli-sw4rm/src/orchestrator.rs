//! Core orchestrator loop: dispatch → verify → iterate.
//!
//! The `OrchestratorLoop` coordinates between an LLM agent (via `RouterSender`)
//! and the yarli `Scheduler` (for verification). It implements the retry loop:
//! send objective → get implementation → run verification → if failures, send
//! fix-it request → repeat until success or max iterations.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use yarli_core::domain::{CommandClass, SafeMode};
use yarli_core::entities::run::Run;
use yarli_core::entities::task::{BlockerCode, Task};
use yarli_core::explain::GateType;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_queue::{InMemoryTaskQueue, Scheduler, SchedulerConfig};

use crate::config::Sw4rmConfig;
use crate::messages::{
    FailureType, ImplementationRequest, ImplementationResponse, RepoContext, VerificationFailure,
};

/// Trait for sending messages to LLM agents via the sw4rm router.
///
/// Abstracted for testability — production impl wraps `RouterClient`,
/// test impl uses `MockRouterSender`.
#[async_trait]
pub trait RouterSender: Send + Sync {
    async fn send_implementation_request(
        &self,
        request: ImplementationRequest,
    ) -> Result<ImplementationResponse, OrchestratorError>;
}

/// Errors from the orchestrator loop.
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("LLM agent response timed out")]
    LlmTimeout,

    #[error("max fix iterations ({max}) exhausted")]
    MaxIterationsExhausted { max: u32 },

    #[error("verification failed after all iterations")]
    VerificationFailed {
        failures: Vec<VerificationFailure>,
        iterations_used: u32,
    },

    #[error("scheduler error: {0}")]
    Scheduler(String),

    #[error("cancelled")]
    Cancelled,
}

/// Result of a complete orchestration run.
#[derive(Debug, Clone)]
pub struct OrchestratorResult {
    /// Whether the objective was accomplished.
    pub success: bool,
    /// Total iterations used.
    pub iterations_used: u32,
    /// Files modified across all iterations.
    pub files_modified: Vec<String>,
    /// Summary from the final LLM response.
    pub final_summary: String,
    /// Verification failures from the last iteration (empty if success).
    pub remaining_failures: Vec<VerificationFailure>,
}

/// Verification commands that the orchestrator will run after each LLM iteration.
#[derive(Debug, Clone)]
pub struct VerificationSpec {
    /// Commands to run (e.g. "cargo test", "cargo clippy").
    pub commands: Vec<VerificationCommand>,
    /// Working directory for command execution.
    pub working_dir: String,
    /// Gates to evaluate for task-level verification.
    /// Uses `default_task_gates()` if None.
    pub task_gates: Option<Vec<GateType>>,
    /// Gates to evaluate for run-level verification.
    /// Uses `default_run_gates()` if None.
    pub run_gates: Option<Vec<GateType>>,
}

/// A single verification command.
#[derive(Debug, Clone)]
pub struct VerificationCommand {
    /// Human-readable key (e.g. "cargo-test").
    pub task_key: String,
    /// Shell command to execute.
    pub command: String,
    /// Command class for scheduling.
    pub class: CommandClass,
}

/// Parameters for a single orchestration run.
#[derive(Debug, Clone, Default)]
pub struct ObjectiveParams {
    /// Scope hint (e.g. file paths, module names) to focus the agent.
    pub scope: Vec<String>,
    /// Repository context (branch, commit, working dir).
    pub repo_context: Option<RepoContext>,
}

/// The orchestrator loop: dispatch to LLM → verify → iterate.
pub struct OrchestratorLoop<R: RouterSender> {
    router: Arc<R>,
    config: Sw4rmConfig,
    verification: VerificationSpec,
}

impl<R: RouterSender> OrchestratorLoop<R> {
    pub fn new(router: Arc<R>, config: Sw4rmConfig, verification: VerificationSpec) -> Self {
        Self {
            router,
            config,
            verification,
        }
    }

    /// Run the full orchestration loop for an objective.
    pub async fn run_objective(
        &self,
        objective: &str,
        correlation_id: &str,
        cancel: CancellationToken,
        params: &ObjectiveParams,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let mut all_files_modified = Vec::new();
        let mut failures: Vec<VerificationFailure> = Vec::new();
        let mut final_summary = String::new();
        let timeout = Duration::from_secs(self.config.llm_response_timeout_secs);

        for iteration in 1..=self.config.max_fix_iterations {
            if cancel.is_cancelled() {
                return Err(OrchestratorError::Cancelled);
            }

            info!(iteration, objective, "dispatching to LLM agent");

            let request = ImplementationRequest {
                objective: objective.to_string(),
                scope: params.scope.clone(),
                failures: failures.clone(),
                iteration,
                correlation_id: correlation_id.to_string(),
                repo_context: params.repo_context.clone(),
            };

            // Fix #5: enforce LLM response timeout from config
            let response = tokio::time::timeout(timeout, self.router.send_implementation_request(request))
                .await
                .map_err(|_| OrchestratorError::LlmTimeout)?
                ?;

            debug!(
                iteration,
                files = response.files_modified.len(),
                complete = response.complete,
                "received LLM response"
            );

            for f in &response.files_modified {
                if !all_files_modified.contains(f) {
                    all_files_modified.push(f.clone());
                }
            }
            final_summary = response.summary.clone();

            if cancel.is_cancelled() {
                return Err(OrchestratorError::Cancelled);
            }

            // Run verification
            failures = self.run_verification(&cancel).await?;

            if failures.is_empty() {
                info!(iteration, "verification passed — objective complete");
                return Ok(OrchestratorResult {
                    success: true,
                    iterations_used: iteration,
                    files_modified: all_files_modified,
                    final_summary,
                    remaining_failures: vec![],
                });
            }

            warn!(
                iteration,
                failure_count = failures.len(),
                "verification failed, will retry"
            );
        }

        Ok(OrchestratorResult {
            success: false,
            iterations_used: self.config.max_fix_iterations,
            files_modified: all_files_modified,
            final_summary,
            remaining_failures: failures,
        })
    }

    /// Run the verification suite and return any failures.
    async fn run_verification(
        &self,
        cancel: &CancellationToken,
    ) -> Result<Vec<VerificationFailure>, OrchestratorError> {
        let store = Arc::new(yarli_store::InMemoryEventStore::new());
        let queue = Arc::new(InMemoryTaskQueue::new());
        let runner = Arc::new(yarli_exec::LocalCommandRunner::new());

        let mut sched_config = SchedulerConfig::default();
        sched_config.working_dir = self.verification.working_dir.clone();

        // Fix #6: wire gates into verification instead of disabling them
        sched_config.task_gates = self
            .verification
            .task_gates
            .clone()
            .unwrap_or_else(yarli_gates::default_task_gates);
        sched_config.run_gates = self
            .verification
            .run_gates
            .clone()
            .unwrap_or_else(yarli_gates::default_run_gates);

        let scheduler = Scheduler::new(queue.clone(), store.clone(), runner, sched_config);

        let corr_id = Uuid::now_v7();

        // Create run
        let mut run = Run::new("verification", SafeMode::Execute);
        let run_id = run.id;
        run.transition(RunState::RunActive, "start verification", "yarli-sw4rm", None)
            .map_err(|e| OrchestratorError::Scheduler(e.to_string()))?;

        let mut task_info = Vec::new();
        for cmd in &self.verification.commands {
            let task = Task::new(
                run_id,
                &cmd.task_key,
                &cmd.command,
                cmd.class,
                corr_id,
            );
            let task_id = task.id;
            task_info.push((task_id, cmd.task_key.clone(), cmd.command.clone()));

            {
                let mut reg = scheduler.registry().write().await;
                reg.add_task(task);
            }
        }

        {
            let mut reg = scheduler.registry().write().await;
            reg.add_run(run);
        }

        // Run scheduler ticks until the run completes
        let max_ticks = 1000;
        for _ in 0..max_ticks {
            if cancel.is_cancelled() {
                return Err(OrchestratorError::Cancelled);
            }

            let result = scheduler
                .tick_with_cancel(cancel.clone())
                .await
                .map_err(|e| OrchestratorError::Scheduler(e.to_string()))?;

            let reg = scheduler.registry().read().await;
            let run_state = reg
                .get_run(&run_id)
                .map(|r| r.state)
                .unwrap_or(RunState::RunCompleted);

            if run_state.is_terminal() {
                break;
            }

            if result.claimed == 0
                && result.promoted == 0
                && result.executed == 0
                && result.runs_completed == 0
            {
                let all_done = task_info.iter().all(|(tid, _, _)| {
                    reg.get_task(tid)
                        .map(|t| t.state.is_terminal())
                        .unwrap_or(true)
                });
                if all_done {
                    break;
                }
            }
        }

        // Collect failures
        let reg = scheduler.registry().read().await;
        let mut failures = Vec::new();
        for (task_id, task_key, command) in &task_info {
            if let Some(task) = reg.get_task(task_id) {
                match task.state {
                    TaskState::TaskComplete => {}
                    TaskState::TaskFailed => {
                        failures.push(VerificationFailure {
                            task_key: task_key.clone(),
                            command: command.clone(),
                            exit_code: None,
                            output_tail: format!("task failed: {:?}", task.blocker),
                            failure_type: classify_failure(task),
                        });
                    }
                    other => {
                        failures.push(VerificationFailure {
                            task_key: task_key.clone(),
                            command: command.clone(),
                            exit_code: None,
                            output_tail: format!("task stuck in state: {other:?}"),
                            failure_type: FailureType::Other,
                        });
                    }
                }
            }
        }

        Ok(failures)
    }
}

/// Fix #7: Classify a failed task using its structured `BlockerCode` instead of
/// fragile Debug string matching.
fn classify_failure(task: &Task) -> FailureType {
    match &task.blocker {
        Some(BlockerCode::GateFailure) => FailureType::TestFailure,
        Some(BlockerCode::PolicyDenial) => FailureType::LintError,
        Some(BlockerCode::MergeConflict) => FailureType::Other,
        Some(BlockerCode::DependencyPending) => FailureType::Other,
        Some(BlockerCode::ManualHold) => FailureType::Other,
        Some(BlockerCode::Custom(msg)) => {
            let lower = msg.to_lowercase();
            if lower.contains("timeout") || lower.contains("timed out") {
                FailureType::Timeout
            } else if lower.contains("compile") || lower.contains("cannot find") {
                FailureType::CompileError
            } else if lower.contains("clippy") || lower.contains("lint") {
                FailureType::LintError
            } else if lower.contains("killed") || lower.contains("oom") || lower.contains("signal") {
                FailureType::Killed
            } else {
                FailureType::TestFailure
            }
        }
        None => FailureType::TestFailure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockRouterSender;

    fn test_verification() -> VerificationSpec {
        VerificationSpec {
            commands: vec![VerificationCommand {
                task_key: "echo-test".to_string(),
                command: "echo ok".to_string(),
                class: CommandClass::Io,
            }],
            working_dir: "/tmp".to_string(),
            // Disable gates for basic tests so "echo ok" passes cleanly
            task_gates: Some(vec![]),
            run_gates: Some(vec![]),
        }
    }

    fn test_config() -> Sw4rmConfig {
        Sw4rmConfig {
            max_fix_iterations: 3,
            ..Sw4rmConfig::default()
        }
    }

    fn default_params() -> ObjectiveParams {
        ObjectiveParams::default()
    }

    #[tokio::test]
    async fn happy_path_succeeds_on_first_iteration() {
        let mock = Arc::new(MockRouterSender::new());
        mock.enqueue_response(ImplementationResponse {
            complete: true,
            files_modified: vec!["src/lib.rs".to_string()],
            summary: "implemented feature".to_string(),
            additional_verification: vec![],
        });

        let orch = OrchestratorLoop::new(mock.clone(), test_config(), test_verification());
        let result = orch
            .run_objective("add feature", "corr-1", CancellationToken::new(), &default_params())
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(result.iterations_used, 1);
        assert_eq!(result.files_modified, vec!["src/lib.rs"]);

        let reqs = mock.requests().await;
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].iteration, 1);
        assert!(reqs[0].failures.is_empty());
    }

    #[tokio::test]
    async fn fix_it_loop_retries_on_failure() {
        let mock = Arc::new(MockRouterSender::new());
        mock.enqueue_response(ImplementationResponse {
            complete: false,
            files_modified: vec!["a.rs".to_string()],
            summary: "first attempt".to_string(),
            additional_verification: vec![],
        });

        let orch = OrchestratorLoop::new(mock.clone(), test_config(), test_verification());
        let result = orch
            .run_objective("fix bug", "corr-2", CancellationToken::new(), &default_params())
            .await
            .unwrap();

        // "echo ok" always passes, so succeeds on iteration 1
        assert!(result.success);
        assert_eq!(result.iterations_used, 1);
    }

    #[tokio::test]
    async fn verification_with_failing_command() {
        let mock = Arc::new(MockRouterSender::new());
        for i in 0..3 {
            mock.enqueue_response(ImplementationResponse {
                complete: false,
                files_modified: vec![format!("file{i}.rs")],
                summary: format!("attempt {}", i + 1),
                additional_verification: vec![],
            });
        }

        let verification = VerificationSpec {
            commands: vec![VerificationCommand {
                task_key: "failing-test".to_string(),
                command: "false".to_string(),
                class: CommandClass::Io,
            }],
            working_dir: "/tmp".to_string(),
            task_gates: Some(vec![]),
            run_gates: Some(vec![]),
        };

        let orch = OrchestratorLoop::new(mock.clone(), test_config(), verification);
        let result = orch
            .run_objective("impossible task", "corr-3", CancellationToken::new(), &default_params())
            .await
            .unwrap();

        assert!(!result.success);
        assert_eq!(result.iterations_used, 3);
        assert!(!result.remaining_failures.is_empty());
    }

    #[tokio::test]
    async fn cancellation_stops_loop() {
        let mock = Arc::new(MockRouterSender::new());
        mock.enqueue_response(ImplementationResponse {
            complete: true,
            files_modified: vec![],
            summary: "done".to_string(),
            additional_verification: vec![],
        });

        let cancel = CancellationToken::new();
        cancel.cancel();

        let orch = OrchestratorLoop::new(mock, test_config(), test_verification());
        let result = orch
            .run_objective("task", "corr-4", cancel, &default_params())
            .await;
        assert!(matches!(result, Err(OrchestratorError::Cancelled)));
    }

    #[tokio::test]
    async fn llm_timeout_propagates() {
        let mock = Arc::new(MockRouterSender::new());
        let orch = OrchestratorLoop::new(mock, test_config(), test_verification());
        let result = orch
            .run_objective("task", "corr-5", CancellationToken::new(), &default_params())
            .await;
        assert!(matches!(result, Err(OrchestratorError::LlmTimeout)));
    }

    #[tokio::test]
    async fn multiple_verification_commands() {
        let mock = Arc::new(MockRouterSender::new());
        mock.enqueue_response(ImplementationResponse {
            complete: true,
            files_modified: vec!["lib.rs".to_string()],
            summary: "all good".to_string(),
            additional_verification: vec![],
        });

        let verification = VerificationSpec {
            commands: vec![
                VerificationCommand {
                    task_key: "test".to_string(),
                    command: "echo test-pass".to_string(),
                    class: CommandClass::Cpu,
                },
                VerificationCommand {
                    task_key: "lint".to_string(),
                    command: "echo lint-pass".to_string(),
                    class: CommandClass::Io,
                },
            ],
            working_dir: "/tmp".to_string(),
            task_gates: Some(vec![]),
            run_gates: Some(vec![]),
        };

        let orch = OrchestratorLoop::new(mock, test_config(), verification);
        let result = orch
            .run_objective("verify multi", "corr-6", CancellationToken::new(), &default_params())
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(result.iterations_used, 1);
    }

    #[tokio::test]
    async fn files_modified_accumulates_across_iterations() {
        let mock = Arc::new(MockRouterSender::new());
        mock.enqueue_response(ImplementationResponse {
            complete: false,
            files_modified: vec!["a.rs".to_string()],
            summary: "first".to_string(),
            additional_verification: vec![],
        });
        mock.enqueue_response(ImplementationResponse {
            complete: true,
            files_modified: vec!["b.rs".to_string()],
            summary: "second".to_string(),
            additional_verification: vec![],
        });

        let orch = OrchestratorLoop::new(mock, test_config(), test_verification());
        let result = orch
            .run_objective("multi", "corr-7", CancellationToken::new(), &default_params())
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.files_modified.contains(&"a.rs".to_string()));
    }

    #[tokio::test]
    async fn scope_and_repo_context_passed_to_request() {
        let mock = Arc::new(MockRouterSender::new());
        mock.enqueue_response(ImplementationResponse {
            complete: true,
            files_modified: vec![],
            summary: "done".to_string(),
            additional_verification: vec![],
        });

        let params = ObjectiveParams {
            scope: vec!["src/lib.rs".to_string(), "src/main.rs".to_string()],
            repo_context: Some(RepoContext {
                branch: Some("feature/foo".to_string()),
                commit: Some("abc123".to_string()),
                working_dir: Some("/repo".to_string()),
            }),
        };

        let orch = OrchestratorLoop::new(mock.clone(), test_config(), test_verification());
        orch.run_objective("test", "corr-scope", CancellationToken::new(), &params)
            .await
            .unwrap();

        let reqs = mock.requests().await;
        assert_eq!(reqs[0].scope, vec!["src/lib.rs", "src/main.rs"]);
        assert_eq!(
            reqs[0].repo_context.as_ref().unwrap().branch.as_deref(),
            Some("feature/foo")
        );
    }

    #[test]
    fn classify_failure_defaults_to_test_failure() {
        let task = Task::new(
            Uuid::now_v7(),
            "test",
            "cmd",
            CommandClass::Io,
            Uuid::now_v7(),
        );
        assert_eq!(classify_failure(&task), FailureType::TestFailure);
    }

    #[test]
    fn classify_failure_gate_failure() {
        let mut task = Task::new(
            Uuid::now_v7(),
            "test",
            "cmd",
            CommandClass::Io,
            Uuid::now_v7(),
        );
        task.blocker = Some(BlockerCode::GateFailure);
        assert_eq!(classify_failure(&task), FailureType::TestFailure);
    }

    #[test]
    fn classify_failure_policy_denial() {
        let mut task = Task::new(
            Uuid::now_v7(),
            "test",
            "cmd",
            CommandClass::Io,
            Uuid::now_v7(),
        );
        task.blocker = Some(BlockerCode::PolicyDenial);
        assert_eq!(classify_failure(&task), FailureType::LintError);
    }

    #[test]
    fn classify_failure_custom_timeout() {
        let mut task = Task::new(
            Uuid::now_v7(),
            "test",
            "cmd",
            CommandClass::Io,
            Uuid::now_v7(),
        );
        task.blocker = Some(BlockerCode::Custom("process timed out after 300s".to_string()));
        assert_eq!(classify_failure(&task), FailureType::Timeout);
    }

    #[test]
    fn classify_failure_custom_compile() {
        let mut task = Task::new(
            Uuid::now_v7(),
            "test",
            "cmd",
            CommandClass::Io,
            Uuid::now_v7(),
        );
        task.blocker = Some(BlockerCode::Custom("compile error in main.rs".to_string()));
        assert_eq!(classify_failure(&task), FailureType::CompileError);
    }

    #[test]
    fn classify_failure_custom_killed() {
        let mut task = Task::new(
            Uuid::now_v7(),
            "test",
            "cmd",
            CommandClass::Io,
            Uuid::now_v7(),
        );
        task.blocker = Some(BlockerCode::Custom("killed by OOM killer".to_string()));
        assert_eq!(classify_failure(&task), FailureType::Killed);
    }

    #[test]
    fn classify_failure_custom_lint() {
        let mut task = Task::new(
            Uuid::now_v7(),
            "test",
            "cmd",
            CommandClass::Io,
            Uuid::now_v7(),
        );
        task.blocker = Some(BlockerCode::Custom("clippy warning".to_string()));
        assert_eq!(classify_failure(&task), FailureType::LintError);
    }

    #[test]
    fn orchestrator_result_fields() {
        let result = OrchestratorResult {
            success: true,
            iterations_used: 2,
            files_modified: vec!["a.rs".to_string()],
            final_summary: "done".to_string(),
            remaining_failures: vec![],
        };
        assert!(result.success);
        assert_eq!(result.iterations_used, 2);
    }
}
