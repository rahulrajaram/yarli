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
use std::time::{Duration, Instant};

use crate::yarli_observability::YarliMetrics;
use chrono::Utc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::yarli_core::domain::{CommandClass, CorrelationId, RunId, TaskId};
use crate::yarli_core::entities::command_execution::{
    CommandExecution, CommandResourceUsage, StreamChunk, StreamType, TokenUsage,
};
use crate::yarli_core::fsm::command::CommandState;
use crate::yarli_core::shutdown::ShutdownController;

use crate::yarli_exec::error::ExecError;

/// Derive a deterministic command ID from an idempotency key.
///
/// This keeps command entity IDs stable across retries/replays and lets callers
/// pre-emit lifecycle events before runner completion.
pub(crate) fn command_id_from_idempotency_key(idempotency_key: &str) -> Uuid {
    let namespaced = format!("yarli:command:{idempotency_key}");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, namespaced.as_bytes())
}

pub(crate) fn command_id_for_request(idempotency_key: Option<&str>) -> Uuid {
    idempotency_key
        .map(command_id_from_idempotency_key)
        .unwrap_or_else(Uuid::now_v7)
}

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
    /// Optional channel for live output streaming. Each chunk is sent as it arrives.
    pub live_output_tx: Option<tokio::sync::mpsc::UnboundedSender<StreamChunk>>,
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
    fn run(
        &self,
        request: CommandRequest,
        cancel: CancellationToken,
    ) -> impl std::future::Future<Output = Result<CommandResult, ExecError>> + Send;
}

/// Local command runner that spawns OS processes via `tokio::process`.
#[derive(Debug, Clone)]
pub struct LocalCommandRunner {
    /// Default timeout if not specified per-command.
    pub default_timeout: Option<Duration>,
    /// Kill the child process if no stdout/stderr output for this duration.
    /// Detects processes stuck in infinite read-think-compact loops.
    pub idle_kill_timeout: Option<Duration>,
    /// Optional metrics registry for telemetry.
    pub metrics: Option<std::sync::Arc<YarliMetrics>>,
    /// Optional shutdown controller for tracking child PIDs.
    /// When set, spawned children are registered so `terminate_children()`
    /// can clean them up on programmatic failure (not just Ctrl+C).
    shutdown: Option<ShutdownController>,
    #[cfg(feature = "chaos")]
    chaos: Option<std::sync::Arc<crate::yarli_chaos::ChaosController>>,
}

impl LocalCommandRunner {
    pub fn new() -> Self {
        Self {
            default_timeout: None,
            idle_kill_timeout: None,
            metrics: None,
            shutdown: None,
            #[cfg(feature = "chaos")]
            chaos: None,
        }
    }

    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    pub fn with_idle_kill_timeout(mut self, timeout: Duration) -> Self {
        self.idle_kill_timeout = Some(timeout);
        self
    }

    pub fn with_metrics(mut self, metrics: std::sync::Arc<YarliMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_shutdown(mut self, shutdown: ShutdownController) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    #[cfg(feature = "chaos")]
    pub fn with_chaos(
        mut self,
        chaos: std::sync::Arc<crate::yarli_chaos::ChaosController>,
    ) -> Self {
        self.chaos = Some(chaos);
        self
    }

    fn record_overhead(&self, class: CommandClass, phase: &str, duration: Duration) {
        if let Some(metrics) = &self.metrics {
            let label = match class {
                CommandClass::Io => "io",
                CommandClass::Cpu => "cpu",
                CommandClass::Git => "git",
                CommandClass::Tool => "tool",
            };
            metrics.record_command_overhead_duration(label, phase, duration.as_secs_f64());
        }
    }
}

impl Default for LocalCommandRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandRunner for LocalCommandRunner {
    #[tracing::instrument(
        skip(self, request, cancel),
        fields(
            run_id = %request.run_id,
            task_id = %request.task_id,
            correlation_id = %request.correlation_id,
            command = %request.command
        )
    )]
    async fn run(
        &self,
        request: CommandRequest,
        cancel: CancellationToken,
    ) -> Result<CommandResult, ExecError> {
        let timeout = request.timeout.or(self.default_timeout);
        let idle_kill_timeout = self.idle_kill_timeout;
        let live_tx = request.live_output_tx;

        // Create entity in CmdQueued state.
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

        let mut chunks: Vec<StreamChunk> = Vec::new();
        let mut seq: u64 = 0;
        let cmd_id = execution.id;

        // Spawn the child process.
        #[cfg(feature = "chaos")]
        if let Some(chaos) = &self.chaos {
            chaos
                .inject("exec_command_spawn")
                .await
                .map_err(|e| ExecError::Io(std::io::Error::other(e.to_string())))?;
        }

        debug!(command = %request.command, working_dir = %request.working_dir, "spawning command");

        let spawn_start = Instant::now();
        // TODO(phase1): Before spawn, create cgroup sandbox and set resource limits.
        //   After spawn, obtain pidfd for race-free lifecycle management.
        //   See IMPLEMENTATION_PLAN.md Section 18 sections 5.1, 5.2, 6.
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(&request.command)
            .current_dir(&request.working_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(request.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .kill_on_drop(true);
        #[cfg(unix)]
        // Isolate each command in its own process group so cancellation tears
        // down descendants (shell + child toolchain processes) reliably.
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let mut child = command.spawn().map_err(ExecError::SpawnFailed)?;
        self.record_overhead(request.command_class, "spawn", spawn_start.elapsed());
        let child_pid = child.id();
        let process_group_id = child_pid.and_then(pid_to_process_group_id);

        // Track child PID in shutdown controller so it can be killed on
        // programmatic failure (RunFailed) — not just on Ctrl+C signal.
        #[cfg(unix)]
        if let (Some(pid), Some(shutdown)) = (child_pid, &self.shutdown) {
            shutdown.track_child(pid);
        }

        let capture_start = Instant::now();
        let monitor = child.id().map(spawn_resource_monitor);
        self.record_overhead(
            request.command_class,
            "resource_capture_init",
            capture_start.elapsed(),
        );

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
                    kill_child(&mut child, process_group_id).await;
                    // Drain remaining output.
                    drain_channel(&mut stdout_rx, cmd_id, &mut seq, &mut chunks, &live_tx).await;
                    Err(ExecError::Killed { reason: "shutdown".into() })
                }
                _ = tokio::time::sleep(dur) => {
                    warn!(cmd_id = %cmd_id, timeout = ?dur, "command timed out");
                    kill_child(&mut child, process_group_id).await;
                    drain_channel(&mut stdout_rx, cmd_id, &mut seq, &mut chunks, &live_tx).await;
                    Err(ExecError::Timeout(dur))
                }
                result = collect_and_wait(&mut child, process_group_id, &mut stdout_rx, cmd_id, &mut seq, &mut chunks, &live_tx, idle_kill_timeout) => {
                    result
                }
            }
        } else {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    warn!(cmd_id = %cmd_id, "command cancelled by shutdown");
                    kill_child(&mut child, process_group_id).await;
                    drain_channel(&mut stdout_rx, cmd_id, &mut seq, &mut chunks, &live_tx).await;
                    Err(ExecError::Killed { reason: "shutdown".into() })
                }
                result = collect_and_wait(&mut child, process_group_id, &mut stdout_rx, cmd_id, &mut seq, &mut chunks, &live_tx, idle_kill_timeout) => {
                    result
                }
            }
        };
        // Drop the live sender to signal streaming is done for this command.
        drop(live_tx);

        let resource_usage = if let Some((stop_tx, monitor_handle)) = monitor {
            let _ = stop_tx.send(());
            monitor_handle.await.unwrap_or_default()
        } else {
            None
        };
        execution.resource_usage = resource_usage;
        execution.token_usage = Some(estimate_token_usage(&execution.command, &chunks));

        // Untrack child PID — process has exited (normally, timed out, or killed).
        #[cfg(unix)]
        if let (Some(pid), Some(shutdown)) = (child_pid, &self.shutdown) {
            shutdown.untrack_child(pid);
        }

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
///
/// If `idle_kill_timeout` is Some, the child will be killed if no output
/// is received within the given duration (detects hung processes).
#[allow(clippy::too_many_arguments)]
async fn collect_and_wait(
    child: &mut tokio::process::Child,
    process_group_id: Option<i32>,
    rx: &mut tokio::sync::mpsc::Receiver<(StreamType, String)>,
    cmd_id: Uuid,
    seq: &mut u64,
    chunks: &mut Vec<StreamChunk>,
    live_tx: &Option<tokio::sync::mpsc::UnboundedSender<StreamChunk>>,
    idle_kill_timeout: Option<Duration>,
) -> Result<i32, ExecError> {
    let mut last_output_at = Instant::now();

    // Read output lines until the channel closes (readers done).
    // Meanwhile the child is running.
    loop {
        let idle_sleep = async {
            match idle_kill_timeout {
                Some(dur) => {
                    let remaining = dur.saturating_sub(last_output_at.elapsed());
                    tokio::time::sleep(remaining).await;
                }
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;
            line = rx.recv() => {
                match line {
                    Some((stream, data)) => {
                        last_output_at = Instant::now();
                        *seq += 1;
                        let chunk = StreamChunk {
                            command_id: cmd_id,
                            sequence: *seq,
                            stream,
                            data,
                            captured_at: Utc::now(),
                        };
                        if let Some(tx) = live_tx {
                            let _ = tx.send(chunk.clone());
                        }
                        chunks.push(chunk);
                    }
                    None => break, // channel closed, readers done
                }
            }
            _ = idle_sleep => {
                let dur = idle_kill_timeout.unwrap();
                warn!(
                    cmd_id = %cmd_id,
                    idle_timeout = ?dur,
                    "command produced no output for idle_kill_timeout — killing"
                );
                kill_child(child, process_group_id).await;
                drain_channel(rx, cmd_id, seq, chunks, live_tx).await;
                return Err(ExecError::Timeout(dur));
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
    live_tx: &Option<tokio::sync::mpsc::UnboundedSender<StreamChunk>>,
) {
    // Close the channel and drain remaining messages.
    rx.close();
    while let Some((stream, data)) = rx.recv().await {
        *seq += 1;
        let chunk = StreamChunk {
            command_id: cmd_id,
            sequence: *seq,
            stream,
            data,
            captured_at: Utc::now(),
        };
        if let Some(tx) = live_tx {
            let _ = tx.send(chunk.clone());
        }
        chunks.push(chunk);
    }
}

fn pid_to_process_group_id(pid: u32) -> Option<i32> {
    i32::try_from(pid).ok()
}

/// Kill a child process (best-effort).
async fn kill_child(child: &mut tokio::process::Child, process_group_id: Option<i32>) {
    #[cfg(unix)]
    if let Some(pgid) = process_group_id {
        let _ = signal_process_group(pgid, libc::SIGTERM);
        for _ in 0..10 {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => tokio::time::sleep(Duration::from_millis(25)).await,
                Err(err) => {
                    warn!(error = %err, "failed to poll child process status during cancellation");
                    break;
                }
            }
        }
        let _ = signal_process_group(pgid, libc::SIGKILL);
    }
    if let Err(e) = child.kill().await {
        warn!(error = %e, "failed to kill child process");
    }
}

#[cfg(unix)]
fn signal_process_group(process_group_id: i32, signal: i32) -> std::io::Result<()> {
    // Negative PID targets the entire process group.
    let rc = unsafe { libc::kill(-process_group_id, signal) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if matches!(err.raw_os_error(), Some(code) if code == libc::ESRCH) {
        return Ok(());
    }
    Err(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::io;

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
            live_output_tx: None,
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

    #[cfg(unix)]
    #[tokio::test]
    async fn test_cancellation_terminates_descendant_processes() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let (live_tx, mut live_rx) = tokio::sync::mpsc::unbounded_channel::<StreamChunk>();
        let mut req = make_request("sleep 60 & echo child:$!; wait");
        req.live_output_tx = Some(live_tx);

        tokio::spawn(async move {
            while let Some(chunk) = live_rx.recv().await {
                if chunk.data.starts_with("child:") {
                    cancel_clone.cancel();
                    break;
                }
            }
        });

        let result = runner.run(req, cancel).await.unwrap();
        assert_eq!(result.execution.state, CommandState::CmdKilled);

        let child_pid = result
            .chunks
            .iter()
            .find_map(|chunk| chunk.data.strip_prefix("child:"))
            .and_then(|raw| raw.trim().parse::<i32>().ok())
            .expect("expected child pid line in output");

        wait_for_process_exit(child_pid, Duration::from_secs(2))
            .await
            .expect("descendant process should be terminated by cancellation");
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

    #[tokio::test]
    async fn test_live_output_tx_receives_chunks_as_they_arrive() {
        let runner = LocalCommandRunner::new();
        let cancel = CancellationToken::new();
        let (live_tx, mut live_rx) = tokio::sync::mpsc::unbounded_channel::<StreamChunk>();
        let mut req = make_request("printf 'line1\nline2\nline3\n'");
        req.live_output_tx = Some(live_tx);

        let result = runner.run(req, cancel).await.unwrap();
        assert_eq!(result.execution.state, CommandState::CmdExited);
        assert_eq!(result.chunks.len(), 3);

        // All chunks should also have been sent through the live channel.
        let mut live_chunks = Vec::new();
        while let Ok(chunk) = live_rx.try_recv() {
            live_chunks.push(chunk);
        }
        assert_eq!(live_chunks.len(), 3);
        assert_eq!(live_chunks[0].data, "line1");
        assert_eq!(live_chunks[1].data, "line2");
        assert_eq!(live_chunks[2].data, "line3");
        // Sequences must match.
        assert_eq!(live_chunks[0].sequence, result.chunks[0].sequence);
    }

    #[cfg(unix)]
    fn process_exists(pid: i32) -> bool {
        let rc = unsafe { libc::kill(pid, 0) };
        if rc == 0 {
            return true;
        }
        let err = io::Error::last_os_error();
        matches!(err.raw_os_error(), Some(code) if code == libc::EPERM)
    }

    #[cfg(unix)]
    async fn wait_for_process_exit(pid: i32, timeout: Duration) -> Result<(), ()> {
        let started = Instant::now();
        loop {
            if !process_exists(pid) {
                return Ok(());
            }
            if started.elapsed() >= timeout {
                return Err(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}
