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
