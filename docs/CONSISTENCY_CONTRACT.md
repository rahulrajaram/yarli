# YARLI Consistency and Runtime Governance Contract

Version: 1.1
Baseline provenance: commit `7b8f5c2` (runtime: enforce resource budgets and surface usage telemetry)
Verifier workflow: `bash scripts/verify_acceptance_rubric.sh <loop-id>`

## 1. Strong Consistency Guarantees

### 1.1 Per-Aggregate Transition Ordering

Every state transition for a given entity (Run, Task, CommandExecution, WorktreeBinding, MergeIntent) is linearizable within its aggregate boundary:

- Transitions are validated against the FSM `valid_transitions()` set before persistence.
- Terminal states are immutable — once a Run, Task, or Command reaches a terminal state, no further transitions are accepted.
- Transition events are appended to the event store atomically; the in-memory entity state is updated only after successful persistence.

Implementation: `yarli-core::entities::{run,task,command_execution,worktree_binding,merge_intent}::transition()`.

### 1.2 Durable-State-Before-Side-Effects

The scheduler enforces a strict durable-state-before-side-effects ordering:

- State transition events are persisted to the event store **before** any external side effect (command spawning, git operations, policy decisions) is initiated.
- On failure to persist, the side effect is not attempted and the error propagates to the caller.
- This guarantees that replaying the event log always produces an accurate picture of what was attempted.

Implementation: `yarli-queue::scheduler::Scheduler::tick()`, `handle_command_success()`, `handle_command_failure()`.

### 1.3 Idempotency Key Replay Behavior

Every event appended to the store may carry an `idempotency_key`:

- The event store enforces uniqueness on idempotency keys (unique partial index `ux_events_idempotency_key` in Postgres, `HashSet` dedup in the in-memory store).
- Duplicate appends with the same idempotency key are silently ignored (no error, no duplicate record).
- Task execution events use `{task_id}:{attempt_no}:{suffix}` as the idempotency key to prevent double-processing on retries.
- Budget violation events set an idempotency key to prevent retry after a budget-exceeded failure.

Implementation: `yarli-store::memory::InMemoryEventStore::append()`, `yarli-store::postgres::PostgresEventStore::append()`, `crates/yarli-store/migrations/0002_indexes.sql`.

### 1.4 Queue Lease Single-Owner Invariant

The task queue guarantees that at most one worker holds the lease for a given queue entry at any time:

- Claims use `FOR UPDATE SKIP LOCKED` in the Postgres path to prevent double-claim under concurrency.
- The in-memory queue checks `lease_owner` and `lease_expires_at` before granting claims.
- Heartbeat and complete/fail operations verify the claiming worker identity (`wrong worker` error on mismatch).
- Stale leases are reclaimed only after expiry plus the configured grace period.
- Concurrency caps (`per_run_cap`, per-class caps) are enforced at claim time.

Implementation: `yarli-queue::memory::InMemoryTaskQueue::claim()`, `yarli-queue::TaskQueue` trait, `crates/yarli-store/migrations/0002_indexes.sql` (claim path indexes).

## 2. Runtime Governance Guarantees

### 2.1 Resource Accounting Fields

Each completed command execution records resource usage in `CommandResourceUsage`:

| Field | Type | Description |
|---|---|---|
| `max_rss_bytes` | `Option<u64>` | Peak resident set size (bytes) |
| `cpu_user_ticks` | `Option<u64>` | CPU user time (clock ticks) |
| `cpu_system_ticks` | `Option<u64>` | CPU system time (clock ticks) |
| `io_read_bytes` | `Option<u64>` | Disk read bytes (`/proc/<pid>/io`) |
| `io_write_bytes` | `Option<u64>` | Disk write bytes (`/proc/<pid>/io`) |

Resource usage is accumulated per-run in `RunUsageTotals` for aggregate budget enforcement.

Implementation: `yarli-core::entities::command_execution::CommandResourceUsage`, `yarli-queue::scheduler::RunUsageTotals`.

### 2.2 Token Accounting Fields

Each completed command execution records token usage in `TokenUsage`:

| Field | Type | Description |
|---|---|---|
| `prompt_tokens` | `u64` | Input tokens consumed |
| `completion_tokens` | `u64` | Output tokens consumed |
| `total_tokens` | `u64` | Sum of prompt + completion |
| `source` | `String` | Estimation method identifier |

Token counts are currently produced by the deterministic `char_count_div4_estimate_v1` estimator and accumulated across tasks within a run for run-level budget enforcement.

Implementation: `yarli-core::entities::command_execution::TokenUsage`, `yarli-exec::runner::LocalCommandRunner`.

### 2.3 Deterministic Budget Breach Outcome

Budget violations follow a `budget_exceeded` fail-fast path with no silent continuation:

1. After each command completes, the scheduler checks all configured budgets against observed resource and token usage.
2. If any budget is exceeded, the task transitions immediately to `TaskFailed` with `reason = "budget_exceeded"`.
3. Budget failures are **not retried** — an idempotency key is set to prevent re-execution.
4. The failure event payload includes:
   - `reason`: `"budget_exceeded"`
   - `scope`: `"task"` or `"run"`
   - `metric`: the specific violated metric name
   - `observed`: the actual measured value
   - `limit`: the configured threshold
   - `command_resource_usage`: full `CommandResourceUsage` snapshot
   - `command_token_usage`: full `TokenUsage` snapshot
   - `run_usage_totals`: aggregated `RunUsageTotals` snapshot
5. If any task fails due to a run-level budget breach, the run itself is prevented from reaching `RunCompleted`.

Configurable budgets (all `Option<u64>`, enforcement only when set):

| Scope | Config Key | Metric |
|---|---|---|
| Task | `max_task_rss_bytes` | Peak RSS |
| Task | `max_task_cpu_user_ticks` | CPU user time |
| Task | `max_task_cpu_system_ticks` | CPU system time |
| Task | `max_task_io_read_bytes` | Disk reads |
| Task | `max_task_io_write_bytes` | Disk writes |
| Task | `max_task_total_tokens` | Token count |
| Run | `max_run_total_tokens` | Cumulative tokens |
| Run | `max_run_peak_rss_bytes` | Peak RSS high-water |
| Run | `max_run_cpu_user_ticks` | Aggregate CPU user |
| Run | `max_run_cpu_system_ticks` | Aggregate CPU system |
| Run | `max_run_io_read_bytes` | Total disk reads |
| Run | `max_run_io_write_bytes` | Total disk writes |

Implementation: `yarli-queue::scheduler::ResourceBudgetConfig`, `check_budget_violations()`, `handle_command_success()`.

## 3. Scale-Consistency Verification Matrix (Loop R7)

Each row maps an invariant to its executable check, expected outcome, and PASS/UNVERIFIED criteria.
A verifier determines Loop-7 PASS by executing every check and confirming every row passes.

| # | Invariant | Executable Check | Expected Outcome | PASS Criteria |
|---|-----------|-----------------|-------------------|---------------|
| SC-1 | Single-active-lease: at most one worker holds a queue lease per entry | `cargo test -p yarli concurrent_claims_no_double_leasing -- --nocapture` | Exit 0, `test result: ok` | No duplicate lease observed across 5 concurrent claimants |
| SC-2 | Single-active-lease under Postgres FOR UPDATE SKIP LOCKED | `YARLI_TEST_DATABASE_URL=... YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli --test yarli_queue_postgres_integration concurrent_claim_no_duplicate_lease_postgres -- --nocapture` | Exit 0, concurrent-claim test passes | Postgres-backed concurrent claim yields no double-lease |
| SC-3 | No duplicate terminal transition per task attempt | `cargo test -p yarli terminal_state_rejects -- --nocapture` | Exit 0, `test result: ok` | All 5 entity types reject transition from terminal state |
| SC-4 | Durable-state-before-side-effects on restart/replay | `cargo test -p yarli scheduler -- --nocapture` | Exit 0, scheduler lifecycle tests pass | Events persisted before command spawn; replay produces consistent state |
| SC-5 | Idempotency key replay prevents duplicate events | `cargo test -p yarli idempotency -- --nocapture` | Exit 0, `test result: ok` | Duplicate idempotency keys rejected; unique keys accepted |
| SC-6 | Idempotency keys unique per attempt (no cross-retry collision) | `cargo test -p yarli test_idempotency_keys_unique_per_attempt -- --nocapture` | Exit 0, `test result: ok` | Each retry attempt generates distinct idempotency keys |
| SC-7 | Budget breach → immediate TaskFailed, no retry | `cargo test -p yarli test_budget_exceeded_fails_task_without_retry -- --nocapture` | Exit 0, `test result: ok` | Task transitions to TaskFailed with reason="budget_exceeded", not retried |
| SC-8 | Run-level budget enforcement across tasks | `cargo test -p yarli test_run_token_budget_exceeded_across_tasks -- --nocapture` | Exit 0, `test result: ok` | Cumulative token budget enforced, run cannot complete after breach |
| SC-9 | Stale lease reclamation with attempt increment | `cargo test -p yarli reclaim_stale -- --nocapture` | Exit 0, `test result: ok` | Expired leases reclaimed; attempt_no incremented |
| SC-10 | Queue rejects duplicate enqueue of active task | `cargo test -p yarli enqueue_duplicate_active -- --nocapture` | Exit 0, `test result: ok` | DuplicateTask error on active re-enqueue |

### 3.1 Verifier Coverage

These matrix checks are enforced by the one-command verifier:

- `bash scripts/verify_acceptance_rubric.sh <loop-id>` runs workspace tests, strict Postgres positive/negative paths, governance/budget checks, and evidence integrity checks.
- Loop-7 adds Postgres-backed concurrency/replay invariant tests (SC-2) and capacity/budget stress proofs (SC-7, SC-8) to the verifier.
- See `docs/ACCEPTANCE_RUBRIC.md` for the complete rubric with Loop-7 checks.

### 3.2 Baseline Verification

These guarantees are also proven by:

- **Unit tests**: `cargo test -p yarli terminal_state_rejects -- --nocapture` (FSM transition validation, terminal immutability, entity models).
- **Scheduler tests**: `cargo test -p yarli scheduler -- --nocapture` (budget enforcement, lease lifecycle, idempotency).
- **Postgres integration tests**: `YARLI_TEST_DATABASE_URL=... YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli --test yarli_queue_postgres_integration -- --nocapture` (idempotency key dedup, queue claim semantics).
- **One-command acceptance**: `bash scripts/verify_acceptance_rubric.sh <loop-id>`.

See `docs/OPERATIONS.md` for operator verification workflows and `docs/ACCEPTANCE_RUBRIC.md` for rubric criteria.
