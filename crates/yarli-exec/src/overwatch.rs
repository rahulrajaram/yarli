//! Overwatch-backed command runner.

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::yarli_core::entities::command_execution::{
    CommandExecution, CommandResourceUsage, StreamChunk, StreamType,
};
use crate::yarli_core::fsm::command::CommandState;

use crate::yarli_exec::error::ExecError;
use crate::yarli_exec::runner::{
    command_id_for_request, estimate_token_usage, CommandRequest, CommandResult, CommandRunner,
};

const OVERWATCH_RUNNER_ACTOR: &str = "overwatch_runner";

#[derive(Debug, Clone)]
pub struct OverwatchRunnerConfig {
    pub service_url: String,
    pub profile: Option<String>,
    pub soft_timeout_seconds: Option<u64>,
    pub silent_timeout_seconds: Option<u64>,
    pub max_log_bytes: Option<u64>,
    pub poll_interval: Duration,
}

impl OverwatchRunnerConfig {
    pub fn validate(&self) -> Result<(), ExecError> {
        Url::parse(&self.service_url)
            .map(|_| ())
            .map_err(|e| ExecError::Protocol(format!("invalid overwatch.service_url: {e}")))
    }
}

impl Default for OverwatchRunnerConfig {
    fn default() -> Self {
        Self {
            service_url: "http://127.0.0.1:8089".to_string(),
            profile: None,
            soft_timeout_seconds: None,
            silent_timeout_seconds: None,
            max_log_bytes: None,
            poll_interval: Duration::from_millis(250),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OverwatchCommandRunner {
    client: reqwest::Client,
    config: OverwatchRunnerConfig,
}

impl OverwatchCommandRunner {
    pub fn new(config: OverwatchRunnerConfig) -> Result<Self, ExecError> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ExecError::Protocol(format!("failed to build HTTP client: {e}")))?;
        Ok(Self { client, config })
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.config.poll_interval = poll_interval;
        self
    }

    fn endpoint(&self, suffix: &str) -> Result<Url, ExecError> {
        let base = self.config.service_url.trim_end_matches('/');
        let full = format!("{base}{suffix}");
        Url::parse(&full)
            .map_err(|e| ExecError::Protocol(format!("invalid overwatch endpoint {full}: {e}")))
    }

    async fn submit_run(&self, request: &CommandRequest) -> Result<String, ExecError> {
        let mut env: HashMap<String, String> = HashMap::new();
        for (k, v) in &request.env {
            env.insert(k.clone(), v.clone());
        }

        let payload = OverwatchRunRequest {
            command: request.command.clone(),
            working_dir: request.working_dir.clone(),
            command_class: format!("{:?}", request.command_class),
            run_id: request.run_id.to_string(),
            task_id: request.task_id.to_string(),
            correlation_id: request.correlation_id.to_string(),
            profile: self.config.profile.clone(),
            soft_timeout_seconds: request
                .timeout
                .map(|timeout| timeout.as_secs())
                .or(self.config.soft_timeout_seconds),
            silent_timeout_seconds: self.config.silent_timeout_seconds,
            max_log_bytes: self.config.max_log_bytes,
            env,
        };

        let response = self
            .client
            .post(self.endpoint("/run")?)
            .json(&payload)
            .send()
            .await?
            .error_for_status()?;

        let body: OverwatchRunResponse = response.json().await?;
        if body.task_id.trim().is_empty() {
            return Err(ExecError::Protocol(
                "overwatch /run returned empty task_id".to_string(),
            ));
        }
        Ok(body.task_id)
    }

    async fn status(&self, task_id: &str) -> Result<OverwatchStatusResponse, ExecError> {
        let response = self
            .client
            .get(self.endpoint(&format!("/status/{task_id}"))?)
            .send()
            .await?
            .error_for_status()?;
        response.json().await.map_err(ExecError::from)
    }

    async fn output(&self, task_id: &str) -> Result<serde_json::Value, ExecError> {
        let response = self
            .client
            .get(self.endpoint(&format!("/output/{task_id}"))?)
            .send()
            .await?
            .error_for_status()?;
        response.json().await.map_err(ExecError::from)
    }

    async fn cancel(&self, task_id: &str) -> Result<(), ExecError> {
        self.client
            .post(self.endpoint(&format!("/cancel/{task_id}"))?)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

impl CommandRunner for OverwatchCommandRunner {
    async fn run(
        &self,
        request: CommandRequest,
        cancel: CancellationToken,
    ) -> Result<CommandResult, ExecError> {
        let mut execution = CommandExecution::new(
            request.task_id,
            request.run_id,
            &request.command,
            &request.working_dir,
            request.command_class,
            request.correlation_id,
        );
        execution.id = command_id_for_request(request.idempotency_key.as_deref());
        if let Some(key) = &request.idempotency_key {
            execution = execution.with_idempotency_key(key);
        }

        execution
            .transition(
                CommandState::CmdStarted,
                "submitted to overwatch",
                OVERWATCH_RUNNER_ACTOR,
                None,
            )
            .map_err(ExecError::Transition)?;
        execution
            .transition(
                CommandState::CmdStreaming,
                "polling overwatch task status",
                OVERWATCH_RUNNER_ACTOR,
                None,
            )
            .map_err(ExecError::Transition)?;

        let task_id = self.submit_run(&request).await?;
        debug!(task_id = %task_id, "overwatch task submitted");

        let status = loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    if let Err(err) = self.cancel(&task_id).await {
                        warn!(task_id = %task_id, error = %err, "overwatch cancel call failed");
                    }
                    execution.chunk_count = 0;
                    execution.token_usage = Some(estimate_token_usage(
                        &execution.command,
                        &[],
                        request.rehydration_tokens,
                    ));
                    execution
                        .transition(
                            CommandState::CmdKilled,
                            "killed: shutdown",
                            OVERWATCH_RUNNER_ACTOR,
                            None,
                        )
                        .map_err(ExecError::Transition)?;

                    let metadata = serde_json::json!({
                        "backend": "overwatch",
                        "task_id": task_id,
                        "state": "cancelled",
                        "reason": "shutdown",
                    });
                    return Ok(CommandResult {
                        execution,
                        chunks: Vec::new(),
                        runner_actor: OVERWATCH_RUNNER_ACTOR.to_string(),
                        backend_metadata: Some(metadata),
                    });
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    let status = self.status(&task_id).await?;
                    if status.is_terminal() {
                        break status;
                    }
                }
            }
        };

        let output_payload = self.output(&task_id).await?;
        let (stdout_lines, stderr_lines) = extract_output_lines(&output_payload, &status);
        let chunks = build_chunks(
            execution.id,
            stdout_lines,
            stderr_lines,
            self.config.max_log_bytes,
        );

        execution.resource_usage = status.resource_usage.clone();
        execution.token_usage = Some(estimate_token_usage(
            &execution.command,
            &chunks,
            request.rehydration_tokens,
        ));
        execution.chunk_count = chunks.len() as u64;

        match map_terminal_outcome(&status) {
            TerminalOutcome::Exited { exit_code } => {
                execution
                    .exit(exit_code, OVERWATCH_RUNNER_ACTOR, None)
                    .map_err(ExecError::Transition)?;
            }
            TerminalOutcome::TimedOut => {
                execution
                    .transition(
                        CommandState::CmdTimedOut,
                        status
                            .reason
                            .clone()
                            .unwrap_or_else(|| "timeout".to_string()),
                        OVERWATCH_RUNNER_ACTOR,
                        None,
                    )
                    .map_err(ExecError::Transition)?;
            }
            TerminalOutcome::Killed => {
                execution
                    .transition(
                        CommandState::CmdKilled,
                        status
                            .reason
                            .clone()
                            .unwrap_or_else(|| "killed".to_string()),
                        OVERWATCH_RUNNER_ACTOR,
                        None,
                    )
                    .map_err(ExecError::Transition)?;
            }
        }

        let metadata = serde_json::json!({
            "backend": "overwatch",
            "task_id": task_id,
            "state": status.state,
            "reason": status.reason,
            "runtime_sec": status.runtime_sec,
            "exit_code": status.exit_code,
        });

        Ok(CommandResult {
            execution,
            chunks,
            runner_actor: OVERWATCH_RUNNER_ACTOR.to_string(),
            backend_metadata: Some(metadata),
        })
    }
}

#[derive(Debug, Serialize)]
struct OverwatchRunRequest {
    command: String,
    working_dir: String,
    command_class: String,
    run_id: String,
    task_id: String,
    correlation_id: String,
    profile: Option<String>,
    soft_timeout_seconds: Option<u64>,
    silent_timeout_seconds: Option<u64>,
    max_log_bytes: Option<u64>,
    env: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct OverwatchRunResponse {
    task_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OverwatchStatusResponse {
    state: String,
    reason: Option<String>,
    runtime_sec: Option<f64>,
    exit_code: Option<i32>,
    #[serde(default)]
    last_stdout_lines: Vec<String>,
    #[serde(default)]
    last_stderr_lines: Vec<String>,
    #[serde(default)]
    resource_usage: Option<CommandResourceUsage>,
    #[serde(default)]
    terminal: Option<bool>,
}

impl OverwatchStatusResponse {
    fn is_terminal(&self) -> bool {
        if let Some(terminal) = self.terminal {
            return terminal;
        }
        let state = self.state.to_ascii_lowercase();
        matches!(
            state.as_str(),
            "completed"
                | "complete"
                | "succeeded"
                | "success"
                | "failed"
                | "error"
                | "timed_out"
                | "timeout"
                | "cancelled"
                | "canceled"
                | "killed"
                | "terminated"
                | "aborted"
                | "finished"
                | "done"
                | "exited"
        )
    }
}

enum TerminalOutcome {
    Exited { exit_code: i32 },
    TimedOut,
    Killed,
}

fn map_terminal_outcome(status: &OverwatchStatusResponse) -> TerminalOutcome {
    let state = status.state.to_ascii_lowercase();
    let reason = status
        .reason
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();

    if state.contains("timeout") || reason.contains("timeout") {
        return TerminalOutcome::TimedOut;
    }

    if matches!(
        state.as_str(),
        "cancelled" | "canceled" | "killed" | "terminated" | "aborted"
    ) || reason.contains("cancel")
        || reason.contains("killed")
    {
        return TerminalOutcome::Killed;
    }

    let exit_code = status.exit_code.unwrap_or(
        if matches!(
            state.as_str(),
            "success" | "succeeded" | "completed" | "done"
        ) {
            0
        } else {
            -1
        },
    );
    TerminalOutcome::Exited { exit_code }
}

fn extract_output_lines(
    output_payload: &serde_json::Value,
    status: &OverwatchStatusResponse,
) -> (Vec<String>, Vec<String>) {
    let stdout_lines = extract_lines(output_payload, &["stdout_lines", "stdout"])
        .or_else(|| {
            output_payload
                .get("output")
                .and_then(|value| extract_lines(value, &["stdout_lines", "stdout"]))
        })
        .unwrap_or_else(|| status.last_stdout_lines.clone());

    let stderr_lines = extract_lines(output_payload, &["stderr_lines", "stderr"])
        .or_else(|| {
            output_payload
                .get("output")
                .and_then(|value| extract_lines(value, &["stderr_lines", "stderr"]))
        })
        .unwrap_or_else(|| status.last_stderr_lines.clone());

    (stdout_lines, stderr_lines)
}

fn extract_lines(value: &serde_json::Value, keys: &[&str]) -> Option<Vec<String>> {
    for key in keys {
        let Some(candidate) = value.get(*key) else {
            continue;
        };
        if let Some(array) = candidate.as_array() {
            return Some(
                array
                    .iter()
                    .filter_map(|item| item.as_str().map(ToString::to_string))
                    .collect(),
            );
        }
        if let Some(raw) = candidate.as_str() {
            return Some(raw.lines().map(|line| line.to_string()).collect());
        }
        if let Some(lines) = candidate.get("lines").and_then(|v| v.as_array()) {
            return Some(
                lines
                    .iter()
                    .filter_map(|item| item.as_str().map(ToString::to_string))
                    .collect(),
            );
        }
    }
    None
}

fn build_chunks(
    command_id: uuid::Uuid,
    stdout_lines: Vec<String>,
    stderr_lines: Vec<String>,
    max_log_bytes: Option<u64>,
) -> Vec<StreamChunk> {
    let mut chunks = Vec::new();
    let mut seq = 0u64;
    let mut emitted_bytes = 0u64;

    let mut emit_line = |stream: StreamType, line: String| {
        let line_bytes = line.len() as u64;
        if let Some(max) = max_log_bytes {
            if emitted_bytes >= max {
                return;
            }
            let remaining = max - emitted_bytes;
            let data = if line_bytes > remaining {
                let take = remaining as usize;
                String::from_utf8_lossy(&line.into_bytes()[..take]).to_string()
            } else {
                line
            };
            emitted_bytes = emitted_bytes.saturating_add(data.len() as u64);
            chunks.push(StreamChunk {
                command_id,
                sequence: seq,
                stream,
                data,
                captured_at: Utc::now(),
            });
            seq = seq.saturating_add(1);
            return;
        }

        emitted_bytes = emitted_bytes.saturating_add(line_bytes);
        chunks.push(StreamChunk {
            command_id,
            sequence: seq,
            stream,
            data: line,
            captured_at: Utc::now(),
        });
        seq = seq.saturating_add(1);
    };

    for line in stdout_lines {
        emit_line(StreamType::Stdout, line);
    }
    for line in stderr_lines {
        emit_line(StreamType::Stderr, line);
    }

    chunks
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use axum08::extract::{Path, State};
    use axum08::routing::{get, post};
    use axum08::{Json, Router};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    use crate::yarli_core::domain::CommandClass;
    use crate::yarli_core::fsm::command::CommandState;

    use super::*;

    #[derive(Clone, Default)]
    struct MockState {
        run_task_id: String,
        run_requests: Vec<serde_json::Value>,
        status_seq: HashMap<String, Vec<OverwatchStatusResponse>>,
        outputs: HashMap<String, serde_json::Value>,
        cancelled: Vec<String>,
    }

    async fn run_handler(
        State(state): State<Arc<Mutex<MockState>>>,
        Json(body): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        let mut state = state.lock().await;
        state.run_requests.push(body);
        Json(serde_json::json!({"task_id": state.run_task_id}))
    }

    async fn status_handler(
        State(state): State<Arc<Mutex<MockState>>>,
        Path(task_id): Path<String>,
    ) -> Json<OverwatchStatusResponse> {
        let mut state = state.lock().await;
        let seq = state
            .status_seq
            .get_mut(&task_id)
            .expect("status sequence should exist for task");
        let status = if seq.len() > 1 {
            seq.remove(0)
        } else {
            seq[0].clone()
        };
        Json(status)
    }

    async fn output_handler(
        State(state): State<Arc<Mutex<MockState>>>,
        Path(task_id): Path<String>,
    ) -> Json<serde_json::Value> {
        let state = state.lock().await;
        Json(
            state
                .outputs
                .get(&task_id)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        )
    }

    async fn cancel_handler(
        State(state): State<Arc<Mutex<MockState>>>,
        Path(task_id): Path<String>,
    ) -> Json<serde_json::Value> {
        let mut state = state.lock().await;
        state.cancelled.push(task_id);
        Json(serde_json::json!({"ok": true}))
    }

    async fn spawn_mock_server(state: MockState) -> (String, Arc<Mutex<MockState>>) {
        let shared = Arc::new(Mutex::new(state));
        let app = Router::new()
            .route("/run", post(run_handler))
            .route("/status/{task_id}", get(status_handler))
            .route("/output/{task_id}", get(output_handler))
            .route("/cancel/{task_id}", post(cancel_handler))
            .with_state(shared.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum08::serve(listener, app).await.unwrap();
        });
        (format!("http://{}", addr), shared)
    }

    fn req() -> CommandRequest {
        CommandRequest {
            task_id: uuid::Uuid::now_v7(),
            run_id: uuid::Uuid::now_v7(),
            command: "echo hello".to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Io,
            correlation_id: uuid::Uuid::now_v7(),
            idempotency_key: Some("ow:test".to_string()),
            timeout: None,
            env: vec![("A".to_string(), "B".to_string())],
            live_output_tx: None,
            resource_limits: None,
            rehydration_tokens: None,
        }
    }

    #[tokio::test]
    async fn overwatch_runner_happy_path() {
        let mut status_map = HashMap::new();
        status_map.insert(
            "task-1".to_string(),
            vec![
                OverwatchStatusResponse {
                    state: "running".to_string(),
                    reason: None,
                    runtime_sec: Some(0.1),
                    exit_code: None,
                    last_stdout_lines: vec![],
                    last_stderr_lines: vec![],
                    resource_usage: None,
                    terminal: Some(false),
                },
                OverwatchStatusResponse {
                    state: "completed".to_string(),
                    reason: Some("ok".to_string()),
                    runtime_sec: Some(0.2),
                    exit_code: Some(0),
                    last_stdout_lines: vec!["hello".to_string()],
                    last_stderr_lines: vec![],
                    resource_usage: Some(CommandResourceUsage {
                        max_rss_bytes: Some(1024),
                        cpu_user_ticks: None,
                        cpu_system_ticks: None,
                        io_read_bytes: None,
                        io_write_bytes: None,
                    }),
                    terminal: Some(true),
                },
            ],
        );

        let mut outputs = HashMap::new();
        outputs.insert(
            "task-1".to_string(),
            serde_json::json!({
                "stdout": ["hello", "world"],
                "stderr": ["warn"]
            }),
        );

        let (service_url, state) = spawn_mock_server(MockState {
            run_task_id: "task-1".to_string(),
            run_requests: Vec::new(),
            status_seq: status_map,
            outputs,
            cancelled: Vec::new(),
        })
        .await;

        let runner = OverwatchCommandRunner::new(OverwatchRunnerConfig {
            service_url,
            profile: Some("default".to_string()),
            soft_timeout_seconds: Some(10),
            silent_timeout_seconds: Some(20),
            max_log_bytes: Some(1024),
            poll_interval: Duration::from_millis(10),
        })
        .unwrap();

        let result = runner.run(req(), CancellationToken::new()).await.unwrap();
        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.execution.exit_code, Some(0));
        assert_eq!(result.chunks.len(), 3);
        assert_eq!(result.runner_actor, OVERWATCH_RUNNER_ACTOR);
        assert_eq!(
            result
                .execution
                .resource_usage
                .as_ref()
                .unwrap()
                .max_rss_bytes,
            Some(1024)
        );

        let state = state.lock().await;
        assert_eq!(state.run_requests.len(), 1);
        assert_eq!(state.run_requests[0]["profile"], "default");
        assert_eq!(state.run_requests[0]["soft_timeout_seconds"], 10);
    }

    #[tokio::test]
    async fn overwatch_runner_maps_timeout_and_failure() {
        let mut status_map = HashMap::new();
        status_map.insert(
            "task-timeout".to_string(),
            vec![OverwatchStatusResponse {
                state: "timed_out".to_string(),
                reason: Some("silent timeout".to_string()),
                runtime_sec: Some(3.0),
                exit_code: None,
                last_stdout_lines: vec![],
                last_stderr_lines: vec![],
                resource_usage: None,
                terminal: Some(true),
            }],
        );

        let mut outputs = HashMap::new();
        outputs.insert("task-timeout".to_string(), serde_json::json!({}));

        let (service_url, _state) = spawn_mock_server(MockState {
            run_task_id: "task-timeout".to_string(),
            run_requests: Vec::new(),
            status_seq: status_map,
            outputs,
            cancelled: Vec::new(),
        })
        .await;

        let runner = OverwatchCommandRunner::new(OverwatchRunnerConfig {
            service_url,
            profile: None,
            soft_timeout_seconds: None,
            silent_timeout_seconds: None,
            max_log_bytes: None,
            poll_interval: Duration::from_millis(5),
        })
        .unwrap();

        let timed_out = runner.run(req(), CancellationToken::new()).await.unwrap();
        assert_eq!(timed_out.execution.state, CommandState::CmdTimedOut);

        let mut status_map = HashMap::new();
        status_map.insert(
            "task-fail".to_string(),
            vec![OverwatchStatusResponse {
                state: "failed".to_string(),
                reason: Some("exit code".to_string()),
                runtime_sec: Some(1.0),
                exit_code: Some(7),
                last_stdout_lines: vec![],
                last_stderr_lines: vec![],
                resource_usage: None,
                terminal: Some(true),
            }],
        );
        let mut outputs = HashMap::new();
        outputs.insert("task-fail".to_string(), serde_json::json!({}));

        let (service_url, _state) = spawn_mock_server(MockState {
            run_task_id: "task-fail".to_string(),
            run_requests: Vec::new(),
            status_seq: status_map,
            outputs,
            cancelled: Vec::new(),
        })
        .await;

        let runner = OverwatchCommandRunner::new(OverwatchRunnerConfig {
            service_url,
            profile: None,
            soft_timeout_seconds: None,
            silent_timeout_seconds: None,
            max_log_bytes: None,
            poll_interval: Duration::from_millis(5),
        })
        .unwrap();

        let failed = runner.run(req(), CancellationToken::new()).await.unwrap();
        assert_eq!(failed.execution.state, CommandState::CmdExited);
        assert_eq!(failed.execution.exit_code, Some(7));
    }

    #[tokio::test]
    async fn overwatch_runner_cancels_remote_task_on_shutdown() {
        let mut status_map = HashMap::new();
        status_map.insert(
            "task-cancel".to_string(),
            vec![OverwatchStatusResponse {
                state: "running".to_string(),
                reason: None,
                runtime_sec: Some(0.5),
                exit_code: None,
                last_stdout_lines: vec![],
                last_stderr_lines: vec![],
                resource_usage: None,
                terminal: Some(false),
            }],
        );

        let mut outputs = HashMap::new();
        outputs.insert("task-cancel".to_string(), serde_json::json!({}));

        let (service_url, state) = spawn_mock_server(MockState {
            run_task_id: "task-cancel".to_string(),
            run_requests: Vec::new(),
            status_seq: status_map,
            outputs,
            cancelled: Vec::new(),
        })
        .await;

        let runner = OverwatchCommandRunner::new(OverwatchRunnerConfig {
            service_url,
            profile: None,
            soft_timeout_seconds: None,
            silent_timeout_seconds: None,
            max_log_bytes: None,
            poll_interval: Duration::from_millis(10),
        })
        .unwrap();

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel_clone.cancel();
        });

        let result = runner.run(req(), cancel).await.unwrap();
        assert_eq!(result.execution.state, CommandState::CmdKilled);

        let state = state.lock().await;
        assert_eq!(state.cancelled, vec!["task-cancel".to_string()]);
    }
}
