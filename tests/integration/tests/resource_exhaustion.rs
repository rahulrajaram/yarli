//! Integration tests: resource exhaustion handling.
//!
//! Covered scenarios:
//! - Disk exhaustion simulation (event log append failures).
//! - Memory pressure leading to budget enforcement.
//! - Queue no-claim behavior from zero claim batch size.
//! - Postgres connection pool exhaustion and recovery.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use yarli_core::domain::{CommandClass, EntityType, Event, SafeMode};
use yarli_core::entities::command_execution::{
    CommandExecution, CommandResourceUsage, StreamChunk, TokenUsage,
};
use yarli_core::entities::task::Task;
use yarli_core::entities::Run;
use yarli_core::fsm::command::CommandState;
use yarli_core::fsm::run::RunState;
use yarli_core::fsm::task::TaskState;
use yarli_exec::{CommandRequest, CommandResult, CommandRunner, ExecError, LocalCommandRunner};
use yarli_queue::{
    ConcurrencyConfig, InMemoryTaskQueue, ResourceBudgetConfig, Scheduler, SchedulerConfig,
    TaskQueue,
};
use yarli_store::{EventStore, InMemoryEventStore, PostgresEventStore, StoreError};

use yarli_integration_tests::{apply_migrations, test_database_url_for_test, TestDatabase};

fn make_scheduler_config(
    worker_id: &str,
    claim_batch_size: usize,
    budgets: ResourceBudgetConfig,
) -> SchedulerConfig {
    SchedulerConfig {
        worker_id: worker_id.to_string(),
        claim_batch_size,
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
        budgets,
        allow_recursive_run: false,
        max_runtime: None,
        idle_timeout: None,
    }
}

fn make_run(objective: &str) -> Run {
    Run::new(objective, SafeMode::Execute)
}

fn make_task(run_id: Uuid, key: &str, command: &str, correlation_id: Uuid) -> Task {
    Task::new(run_id, key, command, CommandClass::Io, correlation_id)
}

#[derive(Debug)]
struct FlakyEventStore {
    inner: Arc<InMemoryEventStore>,
    fail_next_task_ready: AtomicBool,
}

impl FlakyEventStore {
    fn new(inner: Arc<InMemoryEventStore>) -> Self {
        Self {
            inner,
            fail_next_task_ready: AtomicBool::new(true),
        }
    }
}

impl EventStore for FlakyEventStore {
    fn append(&self, event: Event) -> Result<(), StoreError> {
        if event.event_type == "task.ready"
            && self.fail_next_task_ready.swap(false, Ordering::AcqRel)
        {
            return Err(StoreError::Database(
                "simulated ENOSPC: event log append failed".to_string(),
            ));
        }

        self.inner.append(event)
    }

    fn get(&self, event_id: yarli_core::domain::EventId) -> Result<Event, StoreError> {
        self.inner.get(event_id)
    }

    fn query(
        &self,
        query: &yarli_store::event_store::EventQuery,
    ) -> Result<Vec<Event>, StoreError> {
        self.inner.query(query)
    }

    fn all(&self) -> Result<Vec<Event>, StoreError> {
        self.inner.all()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

#[derive(Clone, Debug)]
struct SyntheticCommandRunner {
    command_exit_code: i32,
    resource_usage: Option<CommandResourceUsage>,
    token_usage: Option<TokenUsage>,
}

impl CommandRunner for SyntheticCommandRunner {
    async fn run(
        &self,
        request: CommandRequest,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<CommandResult, ExecError> {
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

        execution.transition(
            CommandState::CmdStarted,
            "synthetic command started",
            "synthetic-command-runner",
            None,
        )?;
        execution.transition(
            CommandState::CmdStreaming,
            "synthetic command streaming",
            "synthetic-command-runner",
            None,
        )?;
        execution.exit(self.command_exit_code, "synthetic-command-runner", None)?;

        execution.resource_usage = self.resource_usage.clone();
        execution.token_usage = self.token_usage.clone();
        execution.chunk_count = 0;

        Ok(CommandResult {
            execution,
            chunks: Vec::<StreamChunk>::new(),
            runner_actor: "synthetic-command-runner".to_string(),
            backend_metadata: None,
        })
    }
}

#[tokio::test]
async fn resource_exhaustion_disk_full_append_failure_is_recoverable() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let backing_store = Arc::new(InMemoryEventStore::new());
    let store = Arc::new(FlakyEventStore::new(backing_store.clone()));
    let runner = Arc::new(LocalCommandRunner::new());
    let sched = Scheduler::new(
        queue.clone(),
        store,
        runner,
        make_scheduler_config("r13-04-disk-full", 8, ResourceBudgetConfig::default()),
    );

    let run = make_run("disk full append failure test");
    let run_id = run.id;
    let corr_id = run.correlation_id;
    let task = make_task(run_id, "write-buffer", "printf done", corr_id);
    let task_id = task.id;

    sched.submit_run(run, vec![task]).await.unwrap();

    let failed_tick = sched.tick().await.unwrap_err();
    let failed_message = failed_tick.to_string();
    assert!(
        failed_message.contains("simulated ENOSPC: event log append failed"),
        "unexpected failure: {failed_message}"
    );

    {
        let reg = sched.registry().read().await;
        let task = reg.get_task(&task_id).unwrap();
        let run = reg.get_run(&run_id).unwrap();
        assert_eq!(task.state, TaskState::TaskOpen);
        assert_eq!(run.state, RunState::RunActive);
    }

    assert_eq!(
        queue.pending_count(),
        0,
        "failed append should rollback state; task not enqueued"
    );

    let mut complete = false;
    for _ in 0..20 {
        let _ = sched.tick().await.unwrap();

        let reg = sched.registry().read().await;
        if reg.get_task(&task_id).unwrap().state == TaskState::TaskComplete {
            let run = reg.get_run(&run_id).unwrap();
            assert_eq!(run.state, RunState::RunCompleted);
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "recoverable append failure should not block completion"
    );

    let events = backing_store.all().unwrap();
    assert!(
        events.iter().any(|event| event.event_type == "task.ready"),
        "task.ready event should be persisted after recovery"
    );
}

#[tokio::test]
async fn resource_exhaustion_memory_pressure_budget_exceeded() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(SyntheticCommandRunner {
        command_exit_code: 0,
        resource_usage: Some(CommandResourceUsage {
            max_rss_bytes: Some(1024),
            ..Default::default()
        }),
        token_usage: None,
    });
    let config = make_scheduler_config(
        "r13-04-memory-pressure",
        4,
        ResourceBudgetConfig {
            max_task_rss_bytes: Some(512),
            ..ResourceBudgetConfig::default()
        },
    );
    let sched = Scheduler::new(queue, store.clone(), runner, config);

    let run = make_run("memory pressure budget test");
    let run_id = run.id;
    let corr_id = run.correlation_id;
    let mut task = make_task(run_id, "heap-hog", "echo heavy", corr_id);
    task = task.with_max_attempts(4);
    let task_id = task.id;

    sched.submit_run(run, vec![task]).await.unwrap();
    for _ in 0..20 {
        let _ = sched.tick().await.unwrap();
        let reg = sched.registry().read().await;
        let task = reg.get_task(&task_id).unwrap();
        if task.state == TaskState::TaskFailed {
            break;
        }
    }

    let reg = sched.registry().read().await;
    let task = reg.get_task(&task_id).unwrap();
    assert_eq!(task.state, TaskState::TaskFailed);
    assert_eq!(task.attempt_no, task.max_attempts);

    let run = reg.get_run(&run_id).unwrap();
    assert_eq!(run.state, RunState::RunFailed);

    let events = store.all().unwrap();
    let budget_failure = events
        .iter()
        .find(|event| {
            event.event_type == "task.failed"
                && event.payload.get("reason").and_then(|v| v.as_str()) == Some("budget_exceeded")
        })
        .expect("expected budget_exceeded task failure");
    assert_eq!(
        budget_failure.payload.get("scope").and_then(|v| v.as_str()),
        Some("task"),
        "budget failure payload should indicate task scope"
    );
}

#[tokio::test]
async fn resource_exhaustion_queue_no_workers_can_claim() {
    let queue = Arc::new(InMemoryTaskQueue::new());
    let store = Arc::new(InMemoryEventStore::new());
    let runner = Arc::new(LocalCommandRunner::new());
    let sched = Scheduler::new(
        queue.clone(),
        store,
        runner,
        make_scheduler_config("r13-04-no-workers", 0, ResourceBudgetConfig::default()),
    );

    let run = make_run("queue claim batch zero test");
    let run_id = run.id;
    let corr_id = run.correlation_id;
    let t1 = make_task(run_id, "batchless-a", "echo a", corr_id);
    let t1_id = t1.id;
    let t2 = make_task(run_id, "batchless-b", "echo b", corr_id);
    let t2_id = t2.id;

    sched.submit_run(run, vec![t1, t2]).await.unwrap();

    for _ in 0..8 {
        let result = sched.tick().await.unwrap();
        assert_eq!(
            result.claimed, 0,
            "no tasks should ever be claimed when batch size is 0"
        );
    }

    let reg = sched.registry().read().await;
    let run = reg.get_run(&run_id).unwrap();
    assert_eq!(
        run.state,
        RunState::RunActive,
        "run should remain active while backlog is not claimable"
    );

    let task1 = reg.get_task(&t1_id).unwrap();
    let task2 = reg.get_task(&t2_id).unwrap();
    assert_eq!(task1.state, TaskState::TaskReady);
    assert_eq!(task2.state, TaskState::TaskReady);
    assert_eq!(queue.pending_count(), 2);
    assert_eq!(queue.stats().leased, 0);
}

#[tokio::test]
async fn resource_exhaustion_postgres_connection_pool_exhaustion_recovery(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(admin_database_url) =
        test_database_url_for_test("resource_exhaustion_postgres_connection_pool")
    else {
        return Ok(());
    };

    let database = TestDatabase::create(&admin_database_url).await?;
    apply_migrations(&database.database_url).await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(250))
        .connect(&database.database_url)
        .await?;

    let store = PostgresEventStore::from_pool(pool.clone());
    let _holder = pool.acquire().await?;
    let first = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Run,
        entity_id: Uuid::now_v7().to_string(),
        event_type: "run.activated".to_string(),
        payload: serde_json::json!({"reason": "pool-exhausted"}),
        correlation_id: Uuid::now_v7(),
        causation_id: None,
        actor: "pool-exhaustion-test".to_string(),
        idempotency_key: None,
    };

    let result = tokio::time::timeout(Duration::from_secs(1), async {
        store.append(first.clone())
    })
    .await;
    assert!(
        result.is_ok(),
        "pool-exhaustion append should return within timeout"
    );
    assert!(
        result.unwrap().is_err(),
        "append should fail while all pool connections are in use"
    );

    drop(_holder);

    let second = Event {
        event_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        entity_type: EntityType::Run,
        entity_id: Uuid::now_v7().to_string(),
        event_type: "run.activated".to_string(),
        payload: serde_json::json!({"reason": "pool-recovery"}),
        correlation_id: Uuid::now_v7(),
        causation_id: None,
        actor: "pool-exhaustion-test".to_string(),
        idempotency_key: Some("pool-recovery".to_string()),
    };
    let recovered = store.append(second);
    assert!(
        recovered.is_ok(),
        "append should succeed again after releasing a held connection"
    );

    database.drop().await?;
    Ok(())
}
