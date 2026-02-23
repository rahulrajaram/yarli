//! Integration test: Audit trail integrity across retries and failures.
//!
//! Crates exercised: yarli-queue, yarli-store, yarli-exec, yarli-core
//!
//! Scenario:
//! - Run with 3 tasks: A (succeeds), B (fails twice, succeeds on attempt 3), C (depends on B)
//! - After completion, walk the entire event log validating ordering and consistency

use std::collections::HashSet;
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
        worker_id: "event-trail-test".to_string(),
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
async fn event_trail_is_consistent_across_retries() {
    let tmp = TempDir::new().unwrap();
    // Counter file: B fails on attempt 1 and 2, succeeds on attempt 3
    let counter_file = tmp.path().join("counter.txt");
    let counter_path = counter_file.display().to_string();

    // Command: read counter, increment, fail if < 3
    // Using a simple bash approach: count lines in file
    let b_cmd = format!("echo x >> {counter_path} && test $(wc -l < {counter_path}) -ge 3");

    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());
    let sched = Scheduler::new(queue, store.clone(), runner, test_config());

    let run = make_run("event trail test");
    let run_id = run.id;
    let corr_id = run.correlation_id;

    // A: simple success
    let task_a = make_task(run_id, "task-a", "echo success", corr_id);
    let a_id = task_a.id;

    // B: fails twice, succeeds on attempt 3
    let mut task_b = make_task(run_id, "task-b", &b_cmd, corr_id);
    task_b = task_b.with_max_attempts(5);
    let b_id = task_b.id;

    // C: depends on B
    let mut task_c = make_task(run_id, "task-c", "echo after-b", corr_id);
    task_c.depends_on(b_id);
    let c_id = task_c.id;

    sched
        .submit_run(run, vec![task_a, task_b, task_c])
        .await
        .unwrap();

    // Tick until terminal
    for _ in 0..30 {
        sched.tick().await.unwrap();
        let reg = sched.registry().read().await;
        if let Some(r) = reg.get_run(&run_id) {
            if r.state.is_terminal() {
                break;
            }
        }
        drop(reg);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Verify run completed
    let reg = sched.registry().read().await;
    let run = reg.get_run(&run_id).unwrap();
    assert_eq!(run.state, RunState::RunCompleted);

    // Verify task states
    assert_eq!(reg.get_task(&a_id).unwrap().state, TaskState::TaskComplete);
    assert_eq!(reg.get_task(&b_id).unwrap().state, TaskState::TaskComplete);
    assert_eq!(reg.get_task(&c_id).unwrap().state, TaskState::TaskComplete);
    assert_eq!(
        reg.get_task(&b_id).unwrap().attempt_no,
        3,
        "B should be on attempt 3"
    );
    drop(reg);

    // Walk the event log
    let events = store.all().unwrap();

    // 1. Every task.failed event has a preceding task.executing for the same task
    for failed_ev in events.iter().filter(|e| e.event_type == "task.failed") {
        let has_prior_executing = events.iter().any(|e| {
            e.event_type == "task.executing"
                && e.entity_id == failed_ev.entity_id
                && e.occurred_at <= failed_ev.occurred_at
        });
        assert!(
            has_prior_executing,
            "task.failed for {} must have preceding task.executing",
            failed_ev.entity_id
        );
    }

    // 2. Every task.retrying event has a preceding task.failed for the same task
    for retry_ev in events.iter().filter(|e| e.event_type == "task.retrying") {
        let has_prior_failed = events.iter().any(|e| {
            e.event_type == "task.failed"
                && e.entity_id == retry_ev.entity_id
                && e.occurred_at <= retry_ev.occurred_at
        });
        assert!(
            has_prior_failed,
            "task.retrying for {} must have preceding task.failed",
            retry_ev.entity_id
        );
    }

    // 3. State transition events (task.*/run.*) are ordered within each entity.
    //    Command-level events (command.output/command.exited) can have nanosecond
    //    clock jitter within the same entity so we only check state-transition events.
    let state_prefixes = ["task.", "run."];
    let mut per_entity_state_events: std::collections::HashMap<&str, Vec<_>> =
        std::collections::HashMap::new();
    for event in &events {
        if state_prefixes
            .iter()
            .any(|p| event.event_type.starts_with(p))
        {
            per_entity_state_events
                .entry(event.entity_id.as_str())
                .or_default()
                .push(event);
        }
    }
    for (entity_id, entity_events) in &per_entity_state_events {
        for window in entity_events.windows(2) {
            assert!(
                window[1].occurred_at >= window[0].occurred_at,
                "state events for {} must be ordered: {} ({}) should be >= {} ({})",
                entity_id,
                window[1].event_type,
                window[1].occurred_at,
                window[0].event_type,
                window[0].occurred_at
            );
        }
    }

    // 4. All idempotency keys are unique (where present)
    let idem_keys: Vec<&str> = events
        .iter()
        .filter_map(|e| e.idempotency_key.as_deref())
        .collect();
    let unique_keys: HashSet<&str> = idem_keys.iter().copied().collect();
    assert_eq!(
        idem_keys.len(),
        unique_keys.len(),
        "idempotency keys must be unique"
    );

    // 5. All events share correlation_id from run
    for event in &events {
        assert_eq!(
            event.correlation_id, corr_id,
            "event {} has wrong correlation_id",
            event.event_type
        );
    }

    // 6. Causation IDs reference valid prior event IDs (where present)
    let all_event_ids: HashSet<Uuid> = events.iter().map(|e| e.event_id).collect();
    for event in &events {
        if let Some(cid) = event.causation_id {
            assert!(
                all_event_ids.contains(&cid),
                "causation_id {} in event {} references non-existent event",
                cid,
                event.event_type
            );
        }
    }

    // 7. B should have exactly 2 task.failed events (attempt 1 and 2)
    let b_failures: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "task.failed" && e.entity_id == b_id.to_string())
        .collect();
    assert_eq!(
        b_failures.len(),
        2,
        "B should have exactly 2 failure events, got {}",
        b_failures.len()
    );

    // 8. B should have exactly 2 task.retrying events
    let b_retries: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "task.retrying" && e.entity_id == b_id.to_string())
        .collect();
    assert_eq!(
        b_retries.len(),
        2,
        "B should have exactly 2 retrying events, got {}",
        b_retries.len()
    );
}
