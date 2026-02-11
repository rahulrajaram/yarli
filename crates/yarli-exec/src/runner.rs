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

use std::fs;
use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use yarli_core::domain::{CommandClass, CorrelationId, RunId, TaskId};
use yarli_core::entities::command_execution::{
    CommandExecution, CommandResourceUsage, StreamChunk, StreamType, TokenUsage,
};
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
    /// Actor label for persisted journal events.
    pub runner_actor: String,
    /// Optional backend-specific metadata for audit/debug.
    pub backend_metadata: Option<serde_json::Value>,
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

        // TODO(phase1): Before spawn, create cgroup sandbox and set resource limits.
        //   After spawn, obtain pidfd for race-free lifecycle management.
        //   See IMPLEMENTATION_PLAN.md Section 18 sections 5.1, 5.2, 6.
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&request.command)
            .current_dir(&request.working_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(request.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .kill_on_drop(true)
            .spawn()
            .map_err(ExecError::SpawnFailed)?;

        let monitor = child.id().map(spawn_resource_monitor);

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

        let resource_usage = if let Some((stop_tx, monitor_handle)) = monitor {
            let _ = stop_tx.send(());
            match monitor_handle.await {
                Ok(usage) => usage,
                Err(_) => None,
            }
        } else {
            None
        };
        execution.resource_usage = resource_usage;
        execution.token_usage = Some(estimate_token_usage(&execution.command, &chunks));

        // Apply terminal transition based on outcome.
        match wait_result {
            Ok(exit_code) => {
                execution.chunk_count = seq;
                execution
                    .exit(exit_code, "local_runner", None)
                    .map_err(ExecError::Transition)?;
                info!(cmd_id = %cmd_id, exit_code, "command exited");
                Ok(CommandResult {
                    execution,
                    chunks,
                    runner_actor: "local_runner".to_string(),
                    backend_metadata: None,
                })
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
                Ok(CommandResult {
                    execution,
                    chunks,
                    runner_actor: "local_runner".to_string(),
                    backend_metadata: None,
                })
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
                Ok(CommandResult {
                    execution,
                    chunks,
                    runner_actor: "local_runner".to_string(),
                    backend_metadata: None,
                })
            }
            Err(e) => Err(e),
        }
    }
}

pub(crate) fn estimate_token_usage(command: &str, chunks: &[StreamChunk]) -> TokenUsage {
    let prompt_tokens = estimate_tokens(command);
    let completion_chars: u64 = chunks.iter().map(|c| c.data.chars().count() as u64).sum();
    let completion_tokens = if completion_chars == 0 {
        0
    } else {
        completion_chars.div_ceil(4)
    };
    let total_tokens = prompt_tokens.saturating_add(completion_tokens);
    TokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens,
        source: "char_count_div4_estimate_v1".to_string(),
    }
}

fn estimate_tokens(input: &str) -> u64 {
    let chars = input.chars().count() as u64;
    if chars == 0 {
        0
    } else {
        chars.div_ceil(4)
    }
}

// TODO: Replace /proc polling with proactive enforcement. See IMPLEMENTATION_PLAN.md Section 18.
//
// TODO(phase1): cgroup v2 sandbox — create /sys/fs/cgroup/yarli/<run_id>/<task_id>/,
//   set memory.max, cpu.max, pids.max before spawn, use cgroup.kill on timeout.
//
// TODO(phase1): pidfd-based spawn/kill/reap — use pidfd_open(2) for race-free
//   liveness checks and SIGTERM→SIGKILL escalation without PID recycling races.
//
// TODO(phase2): rlimits — set RLIMIT_AS, RLIMIT_CPU, RLIMIT_NPROC, RLIMIT_NOFILE
//   in child pre-exec via Command::pre_exec + libc::setrlimit.
//
// TODO(phase2): eBPF-based resource tracking for zero-overhead in-kernel enforcement.
//   - Attach BPF programs to task cgroup to track RSS, CPU, and I/O.
//   - Use eBPF maps to enforce budgets and send signals when thresholds are breached.
//   - Consider libbpf-rs or aya crate for safe Rust BPF program loading.
//
// TODO(phase2): DTrace-based resource tracking on macOS/Illumos/FreeBSD.
//   - Use DTrace probes (proc:::, syscall:::, io:::) to instrument child processes.
//   - On macOS where /proc is unavailable, DTrace is the primary introspection path.
//   - Consider shelling out to `dtrace -n` with a D script or using libdtrace bindings.
//   - Fall back gracefully when DTrace is not available or SIP restricts it.
//
// TODO(phase3): seccomp profiles per command class, namespace isolation.
fn spawn_resource_monitor(
    pid: u32,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Option<CommandResourceUsage>>,
) {
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let mut usage = CommandResourceUsage::default();
        let mut saw_sample = false;

        if let Some(sample) = read_process_sample(pid) {
            saw_sample = true;
            apply_sample(&mut usage, sample);
        }

        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(25)) => {
                    if let Some(sample) = read_process_sample(pid) {
                        saw_sample = true;
                        apply_sample(&mut usage, sample);
                    }
                }
                _ = &mut stop_rx => {
                    break;
                }
            }
        }

        if let Some(sample) = read_process_sample(pid) {
            saw_sample = true;
            apply_sample(&mut usage, sample);
        }

        if saw_sample {
            Some(usage)
        } else {
            None
        }
    });
    (stop_tx, handle)
}

#[derive(Debug, Clone, Copy, Default)]
struct ProcessSample {
    rss_bytes: Option<u64>,
    cpu_user_ticks: Option<u64>,
    cpu_system_ticks: Option<u64>,
    io_read_bytes: Option<u64>,
    io_write_bytes: Option<u64>,
}

fn apply_sample(usage: &mut CommandResourceUsage, sample: ProcessSample) {
    if let Some(rss_bytes) = sample.rss_bytes {
        usage.max_rss_bytes = Some(usage.max_rss_bytes.unwrap_or(0).max(rss_bytes));
    }
    if sample.cpu_user_ticks.is_some() {
        usage.cpu_user_ticks = sample.cpu_user_ticks;
    }
    if sample.cpu_system_ticks.is_some() {
        usage.cpu_system_ticks = sample.cpu_system_ticks;
    }
    if sample.io_read_bytes.is_some() {
        usage.io_read_bytes = sample.io_read_bytes;
    }
    if sample.io_write_bytes.is_some() {
        usage.io_write_bytes = sample.io_write_bytes;
    }
}

#[cfg(target_os = "linux")]
fn read_process_sample(pid: u32) -> Option<ProcessSample> {
    let mut sample = ProcessSample::default();

    // /proc/<pid>/status -> VmRSS (kB)
    let status_path = format!("/proc/{pid}/status");
    let status = fs::read_to_string(&status_path).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .split_whitespace()
                .next()
                .and_then(|n| n.parse::<u64>().ok());
            sample.rss_bytes = kb.map(|v| v.saturating_mul(1024));
            break;
        }
    }

    // /proc/<pid>/stat -> utime/stime ticks.
    let stat_path = format!("/proc/{pid}/stat");
    if let Ok(stat_text) = fs::read_to_string(&stat_path) {
        if let Some(end_comm) = stat_text.rfind(')') {
            let after = stat_text.get(end_comm + 2..).unwrap_or("");
            let fields: Vec<&str> = after.split_whitespace().collect();
            // after starts at field #3, so utime (#14) => idx 11, stime (#15) => idx 12.
            sample.cpu_user_ticks = fields.get(11).and_then(|v| v.parse::<u64>().ok());
            sample.cpu_system_ticks = fields.get(12).and_then(|v| v.parse::<u64>().ok());
        }
    }

    // /proc/<pid>/io -> read_bytes/write_bytes.
    let io_path = format!("/proc/{pid}/io");
    if let Ok(io_text) = fs::read_to_string(&io_path) {
        for line in io_text.lines() {
            if let Some(v) = line.strip_prefix("read_bytes:") {
                sample.io_read_bytes = v.trim().parse::<u64>().ok();
            } else if let Some(v) = line.strip_prefix("write_bytes:") {
                sample.io_write_bytes = v.trim().parse::<u64>().ok();
            }
        }
    }

    Some(sample)
}

// TODO: Implement macOS/BSD process sampling via DTrace or libproc.
//   - macOS: use `proc_pidinfo(PROC_PIDTASKINFO)` from libproc for RSS/CPU,
//     or DTrace probes for IO tracking.
//   - FreeBSD/Illumos: use kinfo_proc via sysctl or DTrace.
#[cfg(not(target_os = "linux"))]
fn read_process_sample(_pid: u32) -> Option<ProcessSample> {
    None
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
        let runner = LocalCommandRunner::new().with_default_timeout(Duration::from_millis(100));
        let cancel = CancellationToken::new();
        let req = make_request("sleep 60");
        let result = runner.run(req, cancel).await.unwrap();

        assert_eq!(result.execution.state, CommandState::CmdTimedOut);
    }

    #[tokio::test]
    async fn test_per_command_timeout_overrides_default() {
        let runner = LocalCommandRunner::new().with_default_timeout(Duration::from_secs(60));
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

    #[tokio::test]
    async fn test_token_usage_is_attached() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("echo token-test");
        let result = runner.run(req, cancel).await.unwrap();

        let usage = result
            .execution
            .token_usage
            .expect("token usage should exist");
        assert_eq!(usage.source, "char_count_div4_estimate_v1");
        assert!(usage.total_tokens >= usage.prompt_tokens);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_resource_usage_is_captured_for_long_running_command() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let req = make_request("sleep 0.2");
        let result = runner.run(req, cancel).await.unwrap();

        let usage = result
            .execution
            .resource_usage
            .expect("resource usage should exist on linux for running command");

        let has_signal = usage.max_rss_bytes.is_some()
            || usage.cpu_user_ticks.is_some()
            || usage.cpu_system_ticks.is_some()
            || usage.io_read_bytes.is_some()
            || usage.io_write_bytes.is_some();
        assert!(has_signal, "expected at least one resource usage metric");
    }
}
