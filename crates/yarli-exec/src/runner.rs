//! Command runner — spawns child processes and streams output.
//!
//! The [`CommandRunner`] trait abstracts over command execution so callers
//! don't depend on OS process semantics directly. [`LocalCommandRunner`]
//! implements it using `tokio::process::Command`.
//!
//! Integration points:
//! - Uses `CommandExecution` entity from yarli-core for state tracking.
//! - Emits `StreamChunk`s for stdout/stderr lines.
//! - Respects `CancellationToken` from shutdown infrastructure.
//! - Supports configurable timeouts.

use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use yarli_core::domain::{CommandClass, CorrelationId, RunId, TaskId};
use yarli_core::entities::command_execution::{CommandExecution, StreamChunk, StreamType};
use yarli_core::fsm::command::CommandState;

use crate::error::ExecError;

/// Request to execute a command.
#[derive(Debug, Clone)]
pub struct CommandRequest {
    /// Task that owns this command.
    pub task_id: TaskId,
    /// Run this command belongs to.
    pub run_id: RunId,
    /// The command string (program + args, shell-parsed).
    pub command: String,
    /// Working directory.
    pub working_dir: String,
    /// Command class for concurrency accounting.
    pub command_class: CommandClass,
    /// Correlation ID for tracing.
    pub correlation_id: CorrelationId,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
    /// Timeout for the command (None = no timeout).
    pub timeout: Option<Duration>,
    /// Environment variables to set (in addition to inherited env).
    pub env: Vec<(String, String)>,
}

/// Result of a completed command execution.
#[derive(Debug)]
pub struct CommandResult {
    /// The final command execution entity (in a terminal state).
    pub execution: CommandExecution,
    /// All captured output chunks.
    pub chunks: Vec<StreamChunk>,
}

/// Trait for running commands. Implementations must be Send + Sync for
/// use across async tasks.
#[allow(async_fn_in_trait)]
pub trait CommandRunner: Send + Sync {
    /// Execute a command to completion, streaming output as it arrives.
    ///
    /// The runner should:
    /// 1. Transition the command through CmdQueued → CmdStarted → CmdStreaming → terminal.
    /// 2. Capture stdout/stderr as StreamChunks.
    /// 3. Respect the cancellation token for graceful shutdown.
    /// 4. Apply timeout if configured.
    async fn run(
        &self,
        request: CommandRequest,
        cancel: CancellationToken,
    ) -> Result<CommandResult, ExecError>;
}

/// Local command runner that spawns OS processes via `tokio::process`.
#[derive(Debug, Clone)]
pub struct LocalCommandRunner {
    /// Default timeout if not specified per-command.
    pub default_timeout: Option<Duration>,
}

impl LocalCommandRunner {
    pub fn new() -> Self {
        Self {
            default_timeout: None,
        }
    }

    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }
}

impl Default for LocalCommandRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandRunner for LocalCommandRunner {
    async fn run(
        &self,
        request: CommandRequest,
        cancel: CancellationToken,
    ) -> Result<CommandResult, ExecError> {
        let timeout = request.timeout.or(self.default_timeout);

        // Create entity in CmdQueued state.
        let mut execution = CommandExecution::new(
            request.task_id,
            request.run_id,
            &request.command,
            &request.working_dir,
            request.command_class,
            request.correlation_id,
        );
        if let Some(key) = &request.idempotency_key {
            execution = execution.with_idempotency_key(key);
        }

        let mut chunks: Vec<StreamChunk> = Vec::new();
        let mut seq: u64 = 0;
        let cmd_id = execution.id;

        // Spawn the child process.
        debug!(command = %request.command, working_dir = %request.working_dir, "spawning command");

        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&request.command)
            .current_dir(&request.working_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(request.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .kill_on_drop(true)
            .spawn()
            .map_err(ExecError::SpawnFailed)?;

        // Transition: CmdQueued → CmdStarted
        execution
            .transition(
                CommandState::CmdStarted,
                "process spawned",
                "local_runner",
                None,
            )
            .map_err(ExecError::Transition)?;

        info!(cmd_id = %cmd_id, pid = ?child.id(), "command started");

        // Take stdout/stderr handles before moving child into wait.
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        // Transition: CmdStarted → CmdStreaming
        execution
            .transition(
                CommandState::CmdStreaming,
                "reading output",
                "local_runner",
                None,
            )
            .map_err(ExecError::Transition)?;

        // Spawn tasks to read stdout and stderr concurrently.
        let (stdout_tx, mut stdout_rx) = tokio::sync::mpsc::channel::<(StreamType, String)>(256);
        let stderr_tx = stdout_tx.clone();

        if let Some(out) = stdout {
            tokio::spawn(async move {
                let reader = BufReader::new(out);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if stdout_tx.send((StreamType::Stdout, line)).await.is_err() {
                        break;
                    }
                }
            });
        }

        if let Some(err) = stderr {
            tokio::spawn(async move {
                let reader = BufReader::new(err);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if stderr_tx.send((StreamType::Stderr, line)).await.is_err() {
                        break;
                    }
                }
            });
        }

        // Drop the last sender so the channel closes when readers finish.
        // (stdout_tx was cloned to stderr_tx, and stdout_tx moved into the spawn,
        //  but we still hold the original — we need to drop it here. Actually, both
        //  senders were moved into spawns, so we just need to handle the channel.)

        // Collect output while waiting for the process to exit.
        let wait_result = if let Some(dur) = timeout {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    warn!(cmd_id = %cmd_id, "command cancelled by shutdown");
                    kill_child(&mut child).await;
                    // Drain remaining output.
                    drain_channel(&mut stdout_rx, cmd_id, &mut seq, &mut chunks).await;
                    Err(ExecError::Killed { reason: "shutdown".into() })
                }
                _ = tokio::time::sleep(dur) => {
                    warn!(cmd_id = %cmd_id, timeout = ?dur, "command timed out");
                    kill_child(&mut child).await;
                    drain_channel(&mut stdout_rx, cmd_id, &mut seq, &mut chunks).await;
                    Err(ExecError::Timeout(dur))
                }
                result = collect_and_wait(&mut child, &mut stdout_rx, cmd_id, &mut seq, &mut chunks) => {
                    result
                }
            }
        } else {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    warn!(cmd_id = %cmd_id, "command cancelled by shutdown");
                    kill_child(&mut child).await;
                    drain_channel(&mut stdout_rx, cmd_id, &mut seq, &mut chunks).await;
                    Err(ExecError::Killed { reason: "shutdown".into() })
                }
                result = collect_and_wait(&mut child, &mut stdout_rx, cmd_id, &mut seq, &mut chunks) => {
                    result
                }
            }
        };

        // Apply terminal transition based on outcome.
        match wait_result {
            Ok(exit_code) => {
                execution.chunk_count = seq;
                execution
                    .exit(exit_code, "local_runner", None)
                    .map_err(ExecError::Transition)?;
                info!(cmd_id = %cmd_id, exit_code, "command exited");
                Ok(CommandResult { execution, chunks })
            }
            Err(ExecError::Timeout(dur)) => {
                execution.chunk_count = seq;
                execution
                    .transition(
                        CommandState::CmdTimedOut,
                        format!("timeout after {dur:?}"),
                        "local_runner",
                        None,
                    )
                    .map_err(ExecError::Transition)?;
                Ok(CommandResult { execution, chunks })
            }
            Err(ExecError::Killed { ref reason }) => {
                execution.chunk_count = seq;
                execution
                    .transition(
                        CommandState::CmdKilled,
                        format!("killed: {reason}"),
                        "local_runner",
                        None,
                    )
                    .map_err(ExecError::Transition)?;
                Ok(CommandResult { execution, chunks })
            }
            Err(e) => Err(e),
        }
    }
}

/// Collect output from the channel and wait for the process to exit.
async fn collect_and_wait(
    child: &mut tokio::process::Child,
    rx: &mut tokio::sync::mpsc::Receiver<(StreamType, String)>,
    cmd_id: Uuid,
    seq: &mut u64,
    chunks: &mut Vec<StreamChunk>,
) -> Result<i32, ExecError> {
    // Read output lines until the channel closes (readers done).
    // Meanwhile the child is running.
    loop {
        tokio::select! {
            biased;
            line = rx.recv() => {
                match line {
                    Some((stream, data)) => {
                        *seq += 1;
                        chunks.push(StreamChunk {
                            command_id: cmd_id,
                            sequence: *seq,
                            stream,
                            data,
                            captured_at: Utc::now(),
                        });
                    }
                    None => break, // channel closed, readers done
                }
            }
        }
    }

    // Wait for the process to fully exit.
    let status = child.wait().await.map_err(ExecError::Io)?;
    Ok(status.code().unwrap_or(-1))
}

/// Drain remaining output from the channel after killing.
async fn drain_channel(
    rx: &mut tokio::sync::mpsc::Receiver<(StreamType, String)>,
    cmd_id: Uuid,
    seq: &mut u64,
    chunks: &mut Vec<StreamChunk>,
) {
    // Close the channel and drain remaining messages.
    rx.close();
    while let Some((stream, data)) = rx.recv().await {
        *seq += 1;
        chunks.push(StreamChunk {
            command_id: cmd_id,
            sequence: *seq,
            stream,
            data,
            captured_at: Utc::now(),
        });
    }
}

/// Kill a child process (best-effort).
async fn kill_child(child: &mut tokio::process::Child) {
    if let Err(e) = child.kill().await {
        warn!(error = %e, "failed to kill child process");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(cmd: &str) -> CommandRequest {
        CommandRequest {
            task_id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            command: cmd.to_string(),
            working_dir: "/tmp".to_string(),
            command_class: CommandClass::Io,
            correlation_id: Uuid::now_v7(),
            idempotency_key: None,
            timeout: None,
            env: vec![],
        }
    }

    #[tokio::test]
    async fn test_simple_echo() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("echo hello");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.execution.exit_code, Some(0));
        assert!(result.execution.started_at.is_some());
        assert!(result.execution.ended_at.is_some());
        assert!(!result.chunks.is_empty());
        assert_eq!(result.chunks[0].data, "hello");
        assert_eq!(result.chunks[0].stream, StreamType::Stdout);
        assert_eq!(result.chunks[0].sequence, 1);
    }

    #[tokio::test]
    async fn test_multiline_output() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("printf 'line1\nline2\nline3\n'");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.execution.exit_code, Some(0));
        assert_eq!(result.chunks.len(), 3);
        assert_eq!(result.chunks[0].data, "line1");
        assert_eq!(result.chunks[1].data, "line2");
        assert_eq!(result.chunks[2].data, "line3");
        // Sequences are monotonically increasing.
        assert_eq!(result.chunks[0].sequence, 1);
        assert_eq!(result.chunks[1].sequence, 2);
        assert_eq!(result.chunks[2].sequence, 3);
    }

    #[tokio::test]
    async fn test_stderr_capture() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("echo err >&2");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.execution.exit_code, Some(0));
        assert!(!result.chunks.is_empty());
        assert_eq!(result.chunks[0].data, "err");
        assert_eq!(result.chunks[0].stream, StreamType::Stderr);
    }

    #[tokio::test]
    async fn test_mixed_stdout_stderr() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("echo out && echo err >&2");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.chunks.len(), 2);
        let has_stdout = result.chunks.iter().any(|c| c.stream == StreamType::Stdout);
        let has_stderr = result.chunks.iter().any(|c| c.stream == StreamType::Stderr);
        assert!(has_stdout);
        assert!(has_stderr);
    }

    #[tokio::test]
    async fn test_nonzero_exit_code() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("exit 42");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.execution.exit_code, Some(42));
    }

    #[tokio::test]
    async fn test_timeout() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let mut req = make_request("sleep 60");
        req.timeout = Some(Duration::from_millis(100));
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdTimedOut);
        assert!(result.execution.ended_at.is_some());
    }

    #[tokio::test]
    async fn test_cancellation() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("sleep 60");

        let cancel_clone = cancel.clone();
        // Cancel after a short delay.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel_clone.cancel();
        });

        let result = runner.run(req, cancel).await.unwrap();
        assert_eq!(result.execution.state, CommandState::CmdKilled);
        assert!(result.execution.ended_at.is_some());
    }

    #[tokio::test]
    async fn test_env_vars() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let mut req = make_request("echo $MY_VAR");
        req.env = vec![("MY_VAR".to_string(), "test_value".to_string())];
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.execution.exit_code, Some(0));
        assert!(!result.chunks.is_empty());
        assert_eq!(result.chunks[0].data, "test_value");
    }

    #[tokio::test]
    async fn test_working_directory() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("pwd");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert!(!result.chunks.is_empty());
        assert_eq!(result.chunks[0].data, "/tmp");
    }

    #[tokio::test]
    async fn test_spawn_failure() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let mut req = make_request("true");
        req.working_dir = "/nonexistent_dir_that_should_not_exist".to_string();
        let result = runner.run(req, cancel).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ExecError::SpawnFailed(_) => {}
            other => panic!("expected SpawnFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_idempotency_key_preserved() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let mut req = make_request("echo ok");
        req.idempotency_key = Some("test-key-123".to_string());
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(
            result.execution.idempotency_key.as_deref(),
            Some("test-key-123")
        );
    }

    #[tokio::test]
    async fn test_chunk_count_matches() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("printf 'a\nb\nc\n'");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.chunk_count, result.chunks.len() as u64);
    }

    #[tokio::test]
    async fn test_default_timeout() {
        let runner = LocalCommandRunner::new()
            .with_default_timeout(Duration::from_millis(100));
        let cancel = CancellationToken::new();
        let req = make_request("sleep 60");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdTimedOut);
    }

    #[tokio::test]
    async fn test_per_command_timeout_overrides_default() {
        let runner = LocalCommandRunner::new()
            .with_default_timeout(Duration::from_secs(60));
        let cancel = CancellationToken::new();
        let mut req = make_request("sleep 60");
        req.timeout = Some(Duration::from_millis(100));
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdTimedOut);
    }

    #[tokio::test]
    async fn test_duration_populated() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("echo fast");
        let result = runner.run(req, cancel).await.unwrap();

        let dur = result.execution.duration();
        assert!(dur.is_some());
        // Should be very short for echo.
        assert!(dur.unwrap().num_seconds() < 5);
    }

    #[tokio::test]
    async fn test_empty_output() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("true");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.execution.exit_code, Some(0));
        assert!(result.chunks.is_empty());
        assert_eq!(result.execution.chunk_count, 0);
    }
}
