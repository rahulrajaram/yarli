//! Integration test: Gate failure after execution, then re-verify.
//!
//! Crates exercised: yarli-queue, yarli-gates, yarli-store, yarli-exec, yarli-core
//!
//! Scenario:
//! 1. Configure RequiredEvidencePresent as run-level gate (which will fail since run context
//!    has no evidence). Task gates are empty so tasks auto-complete.
//! 2. Task A executes and completes (auto-verified).
//! 3. Run-level gate fails → RunFailed.
//!
//! Also tests the gated task path: with task gates enabled, a successful command should
//! pass all gates and complete the run normally.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use yarli_cli::yarli_core::domain::{CommandClass, SafeMode};
use yarli_cli::yarli_core::explain::GateType;
use yarli_cli::yarli_core::fsm::run::RunState;
use yarli_cli::yarli_core::fsm::task::TaskState;
use yarli_cli::yarli_exec::LocalCommandRunner;
use yarli_cli::yarli_queue::{
    ConcurrencyConfig, InMemoryTaskQueue, ResourceBudgetConfig, Scheduler, SchedulerConfig,
};
use yarli_cli::yarli_store::{EventStore, InMemoryEventStore};

fn base_config() -> SchedulerConfig {
    SchedulerConfig {
        worker_id: "gate-test".to_string(),
        claim_batch_size: 8,
        lease_ttl: chrono::Duration::seconds(30),
        tick_interval: Duration::from_millis(10),
        heartbeat_interval: Duration::from_secs(5),
        reclaim_interval: Duration::from_secs(10),
        reclaim_grace: chrono::Duration::seconds(5),
        concurrency: ConcurrencyConfig::default(),
        command_timeout: Some(Duration::from_secs(5)),
        working_dir: "/tmp".to_string(),
        trusted_backend_write_roots: Vec::new(),
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

fn make_run(objective: &str) -> yarli_cli::yarli_core::entities::Run {
    yarli_cli::yarli_core::entities::Run::new(objective, SafeMode::Execute)
}

fn make_task(
    run_id: Uuid,
    key: &str,
    command: &str,
    correlation_id: Uuid,
) -> yarli_cli::yarli_core::entities::Task {
    yarli_cli::yarli_core::entities::Task::new(
        run_id,
        key,
        command,
        CommandClass::Io,
        correlation_id,
    )
}

/// Gated task path: task gates enabled, successful command should pass all gates.
#[tokio::test]
async fn gated_task_passes_all_gates_and_run_completes() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());

    let config = SchedulerConfig {
        task_gates: yarli_cli::yarli_gates::default_task_gates(),
        run_gates: vec![
            GateType::RequiredTasksClosed,
            GateType::NoUnapprovedGitOps,
            GateType::NoUnresolvedConflicts,
            GateType::WorktreeConsistent,
            GateType::PolicyClean,
        ],
        ..base_config()
    };
    let sched = Scheduler::new(queue, store.clone(), runner, config);

    let run = make_run("gated full lifecycle");
    let run_id = run.id;
    let corr_id = run.correlation_id;

    let task = make_task(run_id, "echo", "echo gated-success", corr_id);
    let task_id = task.id;

    sched.submit_run(run, vec![task]).await.unwrap();

    let result = sched.tick().await.unwrap();
    assert_eq!(result.succeeded, 1, "task should pass gates");

    let reg = sched.registry().read().await;
    assert_eq!(
        reg.get_task(&task_id).unwrap().state,
        TaskState::TaskComplete,
        "task should be complete after passing gates"
    );
    assert_eq!(
        reg.get_run(&run_id).unwrap().state,
        RunState::RunCompleted,
        "run should complete when all gates pass"
    );
    drop(reg);

    // Verify gate evaluation recorded in events
    let events = store.all().unwrap();
    let completed = events
        .iter()
        .find(|e| e.event_type == "task.completed")
        .unwrap();
    assert_eq!(
        completed
            .payload
            .get("auto_verified")
            .and_then(|v| v.as_bool()),
        Some(false),
        "should NOT be auto-verified when gates are active"
    );
    assert!(
        completed.payload.get("gates_evaluated").is_some(),
        "should include gates_evaluated in payload"
    );
}

/// Run-level gate failure: RequiredEvidencePresent fails since run context has no evidence.
#[tokio::test]
async fn run_gate_failure_transitions_to_run_failed() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());

    let config = SchedulerConfig {
        task_gates: vec![], // auto-complete tasks
        run_gates: vec![GateType::RequiredEvidencePresent],
        ..base_config()
    };
    let sched = Scheduler::new(queue, store.clone(), runner, config);

    let run = make_run("run gate fail");
    let run_id = run.id;
    let corr_id = run.correlation_id;
    let task = make_task(run_id, "echo", "echo hello", corr_id);
    let task_id = task.id;

    sched.submit_run(run, vec![task]).await.unwrap();

    let result = sched.tick().await.unwrap();
    // Task should succeed (no task gates)
    assert_eq!(result.succeeded, 1);
    // Run should NOT complete (gate fails)
    assert_eq!(result.runs_completed, 0);

    let reg = sched.registry().read().await;
    let task = reg.get_task(&task_id).unwrap();
    assert_eq!(task.state, TaskState::TaskComplete);

    let run = reg.get_run(&run_id).unwrap();
    assert_eq!(
        run.state,
        RunState::RunFailed,
        "run should fail when run-level gates fail"
    );
    drop(reg);

    // Verify run.gate_failed event
    let events = store.all().unwrap();
    let gate_failed = events
        .iter()
        .find(|e| e.event_type == "run.gate_failed")
        .expect("should have run.gate_failed event");
    assert!(
        gate_failed.payload.get("failures").is_some(),
        "gate_failed should include failures"
    );

    let failures = gate_failed
        .payload
        .get("failures")
        .unwrap()
        .as_array()
        .unwrap();
    assert!(!failures.is_empty(), "failures should not be empty");
    let first_failure = failures[0].as_str().unwrap();
    assert!(
        first_failure.contains("evidence"),
        "failure reason should mention evidence: {first_failure}"
    );
}

/// Multi-task gated lifecycle: dependency chain with gates.
#[tokio::test]
async fn gated_multi_task_with_dependencies() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());

    let config = SchedulerConfig {
        task_gates: yarli_cli::yarli_gates::default_task_gates(),
        run_gates: vec![
            GateType::RequiredTasksClosed,
            GateType::NoUnapprovedGitOps,
            GateType::NoUnresolvedConflicts,
            GateType::WorktreeConsistent,
            GateType::PolicyClean,
        ],
        ..base_config()
    };
    let sched = Scheduler::new(queue, store.clone(), runner, config);

    let run = make_run("gated multi");
    let run_id = run.id;
    let corr_id = run.correlation_id;

    let t1 = make_task(run_id, "step-1", "echo first", corr_id);
    let t1_id = t1.id;
    let mut t2 = make_task(run_id, "step-2", "echo second", corr_id);
    t2.depends_on(t1_id);
    let t2_id = t2.id;

    sched.submit_run(run, vec![t1, t2]).await.unwrap();

    // Tick 1: t1 promoted, executed, passes gates
    let r = sched.tick().await.unwrap();
    assert_eq!(r.promoted, 1);
    assert_eq!(r.succeeded, 1);

    let reg = sched.registry().read().await;
    assert_eq!(reg.get_task(&t1_id).unwrap().state, TaskState::TaskComplete);
    assert_eq!(reg.get_task(&t2_id).unwrap().state, TaskState::TaskOpen);
    drop(reg);

    // Tick 2: t2 promoted, executed, passes gates, run completes
    let r = sched.tick().await.unwrap();
    assert_eq!(r.promoted, 1);
    assert_eq!(r.succeeded, 1);
    assert_eq!(r.runs_completed, 1);

    let reg = sched.registry().read().await;
    assert_eq!(reg.get_task(&t2_id).unwrap().state, TaskState::TaskComplete);
    assert_eq!(reg.get_run(&run_id).unwrap().state, RunState::RunCompleted);
}
