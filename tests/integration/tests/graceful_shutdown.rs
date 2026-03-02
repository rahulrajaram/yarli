//! Integration test: Graceful shutdown during active command execution.
//!
//! Crates exercised: yarli-queue, yarli-exec, yarli-store, yarli-core
//!
//! Scenario:
//! - Task A: `sleep 60` (long-running). Task B: depends on A.
//! - Tick once to start Task A execution.
//! - After 200ms, cancel via CancellationToken.
//! - Scheduler exits cleanly.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use yarli_core::domain::{CommandClass, SafeMode};
use yarli_core::entities::{Run, Task};
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_core::shutdown::ShutdownController;
use yarli_exec::LocalCommandRunner;
use yarli_queue::{
    ConcurrencyConfig, InMemoryTaskQueue, ResourceBudgetConfig, Scheduler, SchedulerConfig,
};
use yarli_store::InMemoryEventStore;

#[tokio::test]
async fn graceful_shutdown_cancels_running_tasks() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());

    let config = SchedulerConfig {
        worker_id: "shutdown-test".to_string(),
        claim_batch_size: 4,
        lease_ttl: chrono::Duration::seconds(30),
        tick_interval: Duration::from_millis(50),
        heartbeat_interval: Duration::from_secs(5),
        reclaim_interval: Duration::from_secs(10),
        reclaim_grace: chrono::Duration::seconds(5),
        concurrency: ConcurrencyConfig::default(),
        // Short timeout so test doesn't hang
        command_timeout: Some(Duration::from_secs(10)),
        working_dir: "/tmp".to_string(),
        task_gates: vec![],
        run_gates: vec![],
        enforce_policies: true,
        audit_decisions: true,
        budgets: ResourceBudgetConfig::default(),
        allow_recursive_run: false,
        max_runtime: None,
        idle_timeout: None,
    };
    let sched = Scheduler::new(queue, store.clone(), runner, config);

    // Task A: long-running sleep
    let run = Run::new("shutdown test", SafeMode::Execute);
    let run_id = run.id;
    let corr_id = run.correlation_id;

    let mut task_a = Task::new(run_id, "slow", "sleep 60", CommandClass::Io, corr_id);
    task_a = task_a.with_max_attempts(1);
    let a_id = task_a.id;

    // Task B: depends on A (should never execute)
    let mut task_b = Task::new(run_id, "after-slow", "echo done", CommandClass::Io, corr_id);
    task_b.depends_on(a_id);
    let b_id = task_b.id;

    sched.submit_run(run, vec![task_a, task_b]).await.unwrap();

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    // Cancel after 300ms (enough for one tick to start executing the sleep)
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        cancel_clone.cancel();
    });

    // Run the scheduler loop — should exit within ~2 seconds
    let start = std::time::Instant::now();
    let result = sched.run(cancel).await;
    let elapsed = start.elapsed();

    assert!(
        result.is_ok(),
        "scheduler should exit cleanly on cancel: {:?}",
        result.err()
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "scheduler should exit within 5 seconds, took {:?}",
        elapsed
    );

    // Task B should remain in TaskOpen (never promoted because A didn't complete)
    let reg = sched.registry().read().await;
    let task_b = reg.get_task(&b_id).unwrap();
    assert_eq!(
        task_b.state,
        TaskState::TaskOpen,
        "task B should never have been promoted"
    );

    // Run should not be in RunCompleted
    let run = reg.get_run(&run_id).unwrap();
    assert_ne!(
        run.state,
        RunState::RunCompleted,
        "run should not have completed"
    );
}

#[tokio::test]
async fn scheduler_loop_with_cancellation_completes_fast_task() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());

    let config = SchedulerConfig {
        worker_id: "cancel-loop-test".to_string(),
        claim_batch_size: 4,
        lease_ttl: chrono::Duration::seconds(30),
        tick_interval: Duration::from_millis(10),
        heartbeat_interval: Duration::from_secs(5),
        reclaim_interval: Duration::from_secs(10),
        reclaim_grace: chrono::Duration::seconds(5),
        concurrency: ConcurrencyConfig::default(),
        command_timeout: Some(Duration::from_secs(5)),
        working_dir: "/tmp".to_string(),
        task_gates: vec![],
        run_gates: vec![],
        enforce_policies: true,
        audit_decisions: true,
        budgets: ResourceBudgetConfig::default(),
        allow_recursive_run: false,
        max_runtime: None,
        idle_timeout: None,
    };
    let sched = Scheduler::new(queue, store.clone(), runner, config);

    let run = Run::new("fast cancel test", SafeMode::Execute);
    let run_id = run.id;
    let corr_id = run.correlation_id;
    let task = Task::new(run_id, "echo", "echo done", CommandClass::Io, corr_id);

    sched.submit_run(run, vec![task]).await.unwrap();

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    // Cancel after 500ms (enough for the echo to complete)
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel_clone.cancel();
    });

    let result = sched.run(cancel).await;
    assert!(result.is_ok(), "scheduler should exit cleanly");

    // Fast task should have completed during the loop
    let reg = sched.registry().read().await;
    let run = reg.get_run(&run_id).unwrap();
    assert_eq!(
        run.state,
        RunState::RunCompleted,
        "fast run should complete before cancellation"
    );
}

/// Verify that `terminate_children()` kills tracked child processes.
///
/// This tests the zombie prevention fix: when a run reaches RunFailed
/// programmatically, `terminate_children()` must kill any surviving children.
#[cfg(unix)]
#[tokio::test]
async fn terminate_children_kills_tracked_processes() {
    use yarli_core::entities::command_execution::StreamChunk;
    use yarli_exec::{CommandRequest, CommandRunner};

    let shutdown = ShutdownController::new();
    let runner = LocalCommandRunner::new().with_shutdown(shutdown.clone());

    // Spawn a long-running sleep via the runner, capturing its PID via live output.
    let (live_tx, mut live_rx) = tokio::sync::mpsc::unbounded_channel::<StreamChunk>();
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    // The command prints both its shell PID and the PID of a background sleep,
    // so we can verify that process-group signalling kills descendants too.
    let req = CommandRequest {
        task_id: uuid::Uuid::now_v7(),
        run_id: uuid::Uuid::now_v7(),
        command: "sleep 300 & echo sleeper:$!; echo shell:$$; wait".to_string(),
        working_dir: "/tmp".to_string(),
        command_class: CommandClass::Io,
        correlation_id: uuid::Uuid::now_v7(),
        idempotency_key: None,
        timeout: None,
        env: vec![],
        live_output_tx: Some(live_tx),
    };

    // Run the command in a background task, cancel it once we capture PIDs.
    let runner_handle = tokio::spawn(async move { runner.run(req, cancel).await });

    // Wait for PIDs to be printed.
    let mut shell_pid: Option<i32> = None;
    let mut sleeper_pid: Option<i32> = None;
    while let Some(chunk) = live_rx.recv().await {
        if let Some(pid_str) = chunk.data.strip_prefix("shell:") {
            shell_pid = pid_str.trim().parse::<i32>().ok();
        }
        if let Some(pid_str) = chunk.data.strip_prefix("sleeper:") {
            sleeper_pid = pid_str.trim().parse::<i32>().ok();
        }
        if shell_pid.is_some() && sleeper_pid.is_some() {
            break;
        }
    }
    let shell_pid = shell_pid.expect("should have captured shell PID");
    let sleeper_pid = sleeper_pid.expect("should have captured sleeper PID");

    // Verify both processes are alive.
    assert!(
        process_alive(shell_pid),
        "shell process {shell_pid} should be alive"
    );
    assert!(
        process_alive(sleeper_pid),
        "sleeper process {sleeper_pid} should be alive"
    );

    // Now call terminate_children — this simulates what happens on RunFailed.
    shutdown.terminate_children().await;

    // Wait briefly for signals to be delivered.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both processes should be dead (process-group kill).
    assert!(
        !process_alive(shell_pid),
        "shell process {shell_pid} should have been killed by terminate_children()"
    );
    assert!(
        !process_alive(sleeper_pid),
        "sleeper process {sleeper_pid} should have been killed by terminate_children()"
    );

    // Clean up the runner task.
    cancel_clone.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), runner_handle).await;
}

#[cfg(unix)]
fn process_alive(pid: i32) -> bool {
    let rc = unsafe { libc::kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    let err = std::io::Error::last_os_error();
    matches!(err.raw_os_error(), Some(code) if code == libc::EPERM)
}
