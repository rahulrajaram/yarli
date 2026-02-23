//! Integration test: Run-level token budget across parallel tasks.
//!
//! Crates exercised: yarli-queue, yarli-store, yarli-exec, yarli-core
//!
//! Scenario:
//! - max_run_total_tokens set low so 2-3 tasks succeed before budget breach
//! - Budget-failed tasks have attempt_no == max_attempts (permanent failure)
//! - Run transitions to RunFailed (not stuck in RunActive)

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use yarli_core::domain::{CommandClass, SafeMode};
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_exec::LocalCommandRunner;
use yarli_queue::{
    ConcurrencyConfig, InMemoryTaskQueue, ResourceBudgetConfig, Scheduler, SchedulerConfig,
};
use yarli_store::{EventStore, InMemoryEventStore};

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

#[tokio::test]
async fn budget_breach_fails_tasks_and_run() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());

    // Token budget of 12: allows ~2 tasks (~5 tokens each), breaches on 3rd.
    let config = SchedulerConfig {
        worker_id: "budget-test".to_string(),
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
        budgets: ResourceBudgetConfig {
            max_run_total_tokens: Some(12),
            ..ResourceBudgetConfig::default()
        },
        allow_recursive_run: false,
        max_runtime: None,
        idle_timeout: None,
    };
    let sched = Scheduler::new(queue, store.clone(), runner, config);

    let run = make_run("budget breach test");
    let run_id = run.id;
    let corr_id = run.correlation_id;

    // 4 parallel IO tasks, each producing ~5 tokens
    let t1 = make_task(run_id, "p1", "echo task1-output", corr_id);
    let t2 = make_task(run_id, "p2", "echo task2-output", corr_id);
    let t3 = make_task(run_id, "p3", "echo task3-output", corr_id);
    let t4 = make_task(run_id, "p4", "echo task4-output", corr_id);

    sched.submit_run(run, vec![t1, t2, t3, t4]).await.unwrap();

    // Tick until all tasks are processed
    let mut total_succeeded = 0usize;
    let mut total_failed = 0usize;
    for _ in 0..10 {
        let r = sched.tick().await.unwrap();
        total_succeeded += r.succeeded;
        total_failed += r.failed;
        if total_succeeded + total_failed >= 4 {
            break;
        }
    }

    // At least 1 task must succeed, at least 1 must fail
    assert!(
        total_succeeded >= 1,
        "at least 1 task should succeed (got {total_succeeded})"
    );
    assert!(
        total_failed >= 1,
        "at least 1 task should fail from budget (got {total_failed})"
    );

    // Verify budget failure events
    let events = store.all().unwrap();
    let budget_failures: Vec<_> = events
        .iter()
        .filter(|e| {
            e.event_type == "task.failed"
                && e.payload.get("reason").and_then(|v| v.as_str()) == Some("budget_exceeded")
        })
        .collect();
    assert!(
        !budget_failures.is_empty(),
        "must have explicit budget_exceeded failure events"
    );

    // Budget failure events should include run_usage_totals
    for failure in &budget_failures {
        assert!(
            failure.payload.get("run_usage_totals").is_some(),
            "budget failure must include run_usage_totals"
        );
    }

    // Failed tasks should have attempt_no == max_attempts (permanent failure)
    let reg = sched.registry().read().await;
    for task in reg.tasks_for_run(&run_id) {
        if task.state == TaskState::TaskFailed {
            assert_eq!(
                task.attempt_no, task.max_attempts,
                "budget-failed task {} should be marked permanent (attempt_no={}, max={})",
                task.task_key, task.attempt_no, task.max_attempts
            );
        }
    }
    drop(reg);

    // No task retried after budget breach
    let retry_after_budget: Vec<_> = events
        .iter()
        .filter(|e| {
            e.event_type == "task.retrying"
                && budget_failures.iter().any(|f| f.entity_id == e.entity_id)
        })
        .collect();
    assert!(
        retry_after_budget.is_empty(),
        "no task should retry after budget breach"
    );

    // Tick a few more times to let the run transition
    for _ in 0..5 {
        let _ = sched.tick().await;
    }

    // Run must reach RunFailed (not stuck in RunActive)
    let reg = sched.registry().read().await;
    let run = reg.get_run(&run_id).unwrap();
    assert_eq!(
        run.state,
        RunState::RunFailed,
        "run should transition to RunFailed after budget breach, got {:?}",
        run.state
    );
}
