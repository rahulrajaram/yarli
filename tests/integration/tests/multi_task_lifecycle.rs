//! Integration test: mixed outcomes with dependency chain.
//!
//! Crates exercised: yarli-queue, yarli-store, yarli-exec, yarli-core
//!
//! Scenario:
//! - Run with 4 tasks: A → B, C (independent), D depends on B
//! - Task B uses a temp file sentinel: fails attempt 1 (file absent), creates file, succeeds on retry
//! - C runs independently in parallel with A

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use uuid::Uuid;

use yarli_core::domain::{CommandClass, SafeMode};
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_exec::LocalCommandRunner;
use yarli_queue::{
    ConcurrencyConfig, InMemoryTaskQueue, ResourceBudgetConfig, Scheduler, SchedulerConfig,
};
use yarli_store::{EventStore, InMemoryEventStore};

fn test_config() -> SchedulerConfig {
    SchedulerConfig {
        worker_id: "integration-test".to_string(),
        claim_batch_size: 8,
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
    }
}

fn test_scheduler() -> (
    Scheduler<InMemoryTaskQueue, InMemoryEventStore, LocalCommandRunner>,
    Arc<InMemoryEventStore>,
) {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());
    let scheduler = Scheduler::new(queue, store.clone(), runner, test_config());
    (scheduler, store)
}

fn make_run(objective: &str) -> yarli_core::entities::Run {
    yarli_core::entities::Run::new(objective, SafeMode::Execute)
}

fn make_task(
    run_id: Uuid,
    key: &str,
    command: &str,
    correlation_id: Uuid,
) -> yarli_core::entities::Task {
    yarli_core::entities::Task::new(run_id, key, command, CommandClass::Io, correlation_id)
}

/// Tick the scheduler until the run reaches a terminal state or we exceed max_ticks.
async fn tick_until_terminal(
    sched: &Scheduler<InMemoryTaskQueue, InMemoryEventStore, LocalCommandRunner>,
    run_id: Uuid,
    max_ticks: u32,
) -> RunState {
    for tick in 0..max_ticks {
        sched.tick().await.unwrap();
        let reg = sched.registry().read().await;
        if let Some(run) = reg.get_run(&run_id) {
            if run.state.is_terminal() {
                return run.state;
            }
        }
        drop(reg);
        // Small delay to let IO settle
        if tick > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let reg = sched.registry().read().await;
    reg.get_run(&run_id).unwrap().state
}

#[tokio::test]
async fn multi_task_lifecycle_with_dependencies_and_retry() {
    let tmp = TempDir::new().unwrap();
    let sentinel = tmp.path().join("sentinel.txt");
    let sentinel_path = sentinel.display().to_string();

    let (sched, store) = test_scheduler();

    let run = make_run("multi-task lifecycle");
    let run_id = run.id;
    let corr_id = run.correlation_id;

    // Task A: simple success (no deps)
    let task_a = make_task(run_id, "task-a", "echo task-a-done", corr_id);
    let a_id = task_a.id;

    // Task B: depends on A, fails first attempt (file absent), succeeds on retry
    // Uses: test -f <sentinel> && echo ok || (touch <sentinel> && exit 1)
    let b_cmd = format!("test -f {sentinel_path} && echo ok || (touch {sentinel_path} && exit 1)");
    let mut task_b = make_task(run_id, "task-b", &b_cmd, corr_id);
    task_b.depends_on(a_id);
    task_b = task_b.with_max_attempts(3);
    let b_id = task_b.id;

    // Task C: independent (no deps), runs in parallel with A
    let task_c = make_task(run_id, "task-c", "echo task-c-done", corr_id);
    let c_id = task_c.id;

    // Task D: depends on B
    let mut task_d = make_task(run_id, "task-d", "echo task-d-done", corr_id);
    task_d.depends_on(b_id);
    let d_id = task_d.id;

    sched
        .submit_run(run, vec![task_a, task_b, task_c, task_d])
        .await
        .unwrap();

    // Tick 1: A and C should promote (no deps), B blocked by A, D blocked by B
    let r1 = sched.tick().await.unwrap();
    assert!(
        r1.promoted >= 2,
        "A and C should promote, got {}",
        r1.promoted
    );

    let reg = sched.registry().read().await;
    // C should have started (it has no deps)
    let c_state = reg.get_task(&c_id).unwrap().state;
    assert!(
        c_state == TaskState::TaskComplete || c_state == TaskState::TaskExecuting,
        "C should start without waiting for A/B"
    );
    // D should still be open (blocked by B)
    assert_eq!(reg.get_task(&d_id).unwrap().state, TaskState::TaskOpen);
    drop(reg);

    // Tick until terminal
    let final_state = tick_until_terminal(&sched, run_id, 20).await;
    assert_eq!(
        final_state,
        RunState::RunCompleted,
        "run should complete successfully"
    );

    // Verify task states
    let reg = sched.registry().read().await;
    assert_eq!(reg.get_task(&a_id).unwrap().state, TaskState::TaskComplete);
    assert_eq!(reg.get_task(&b_id).unwrap().state, TaskState::TaskComplete);
    assert_eq!(reg.get_task(&c_id).unwrap().state, TaskState::TaskComplete);
    assert_eq!(reg.get_task(&d_id).unwrap().state, TaskState::TaskComplete);

    // B should have attempt_no == 2 (failed once, retried once)
    let task_b = reg.get_task(&b_id).unwrap();
    assert_eq!(
        task_b.attempt_no, 2,
        "B should be on attempt 2 after one retry"
    );
    drop(reg);

    // Verify event trail for B
    let events = store.all().unwrap();

    // B should have task.executing events (at least 2: attempt 1 + attempt 2)
    let b_executing: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "task.executing" && e.entity_id == b_id.to_string())
        .collect();
    assert!(
        b_executing.len() >= 2,
        "B should have at least 2 executing events, got {}",
        b_executing.len()
    );

    // B should have at least 1 task.failed event
    let b_failed: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "task.failed" && e.entity_id == b_id.to_string())
        .collect();
    assert!(
        !b_failed.is_empty(),
        "B should have at least 1 failed event"
    );

    // B should have a task.retrying event
    let b_retrying: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "task.retrying" && e.entity_id == b_id.to_string())
        .collect();
    assert!(
        !b_retrying.is_empty(),
        "B should have at least 1 retrying event"
    );
}
