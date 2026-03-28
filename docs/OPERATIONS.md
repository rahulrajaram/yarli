# YARLI Operations

This runbook covers the baseline local operator workflow for durable mode, migrations, and test execution.
For final verification and release acceptance decisions, use `docs/ACCEPTANCE_RUBRIC.md`.

For an exhaustive, command-by-command CLI usage guide, see `docs/CLI.md`.
For memory behavior (when YARLI stores/queries memories), see `docs/MEMORY_POLICY.md`.

## Requirements

- Rust toolchain compatible with workspace `rust-version` (currently 1.75+).
- PostgreSQL accessible from the local machine.
- `psql` client for manual migration execution.
- CI/CD pipeline integrations for API-triggered runs are documented in
  `docs/CI_CD_INTEGRATION_EXAMPLES.md` with GitHub Actions, GitLab CI, Jenkins, and wait-for-completion scripts.

## Production Deployment Prerequisites

- Kubernetes:
  - Kubernetes `v1.27+` cluster.
  - `kubectl` matching cluster version.
- Container tooling:
  - `helm` `v3+`.
  - Container runtime tooling for image signing/build reproducibility.
- Event store:
  - PostgreSQL `16+` for production deployments.
  - Stable connectivity from all scheduler pods to the database.
  - Database role policy allowing migration execution and backup tooling.
  - Backups and restore drills included in runbook cadence.
- Runtime:
  - Secrets mounted from platform vault/secret manager for DB credentials.
  - At least one namespace boundary around scheduler and operator tooling.

Deployment prerequisites before first rollout:

- Set `core.backend = "postgres"` in effective config.
- Set one of `DATABASE_URL` or `postgres.database_url_file`.
- Run `yarli migrate status` and confirm no unapplied mandatory migrations.
- Validate `cargo test --workspace` in a clean environment before release packaging.

## Zero-Drift Deployment Hygiene

- Use immutable image tags for scheduler and api images.
- Keep migration execution and scheduler startup in separate rollout steps.
- Preserve `run` and `task` identifiers in event store evidence.

## Required Environment Variables

- `YARLI_TEST_DATABASE_URL`
  - Required when running Postgres integration tests locally.
  - CI uses: `postgres://postgres:postgres@localhost:5432/postgres`.
  - Must point to an admin database that can create and drop temporary databases.
- `DATABASE_URL`
  - Optional for durable runtime.
  - If set, overrides `postgres.database_url` and `postgres.database_url_file`.
- `YARLI_REQUIRE_POSTGRES_TESTS`
  - Set to `1` for strict mode.
  - In strict mode, missing `YARLI_TEST_DATABASE_URL` is a hard failure (never a skip-success).

## One-Command Acceptance Verification

From repository root:

```bash
export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
export YARLI_REQUIRE_POSTGRES_TESTS=1
bash scripts/verify_acceptance_rubric.sh r8
```

Expected behavior:

- Exit `0` and print `PASS` only when every rubric check is proven.
- Print `UNVERIFIED` and exit non-zero for any missing or failing proof.
- Write evidence to `evidence/r8/` (or generally `evidence/<loop-id>/`) with command logs and `README.md`.

## Fresh-Clone and Clean-Shell Repro Path

Use this sequence to prove reproducibility from a fresh clone and a clean shell.

```bash
git clone <repo-url> yarli
cd yarli
docker rm -f yarli-r8-postgres >/dev/null 2>&1 || true
docker run -d --name yarli-r8-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 55432:5432 \
  postgres:16
until pg_isready -h 127.0.0.1 -p 55432 -U postgres -d postgres >/dev/null 2>&1; do sleep 1; done
env -i HOME="$HOME" PATH="$PATH" bash --noprofile --norc -lc '
  export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
  export YARLI_REQUIRE_POSTGRES_TESTS=1
  bash scripts/verify_acceptance_rubric.sh r8
'
docker rm -f yarli-r8-postgres
```

Expected verification signals:

- Command output prints `PASS`.
- Exit code is `0`.
- `evidence/r8/README.md` records command blocks with `Exit Code:` and `Key Output:`.

## Packaging and Deployment Smoke Verification (Loop-8)

This sequence is the deterministic "can we deploy" proof path for API and durable CLI behavior:

```bash
export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
export YARLI_REQUIRE_POSTGRES_TESTS=1

# Packaging/build step
cargo build --release -p yarli --bin yarli

# API smoke checks (in-process and persistent read paths)
cargo test --workspace
cargo test -p yarli --test yarli_api_postgres_integration -- --nocapture

# Durable CLI write/read roundtrip
cargo test -p yarli --test yarli_cli_postgres_integration -- --nocapture
```

Expected smoke signals:

- Release build command exits `0`.
- API logs contain `health_endpoint_returns_ok` and `run_status_endpoint_replays_persisted_events`.
- API deploy check log contains `run_and_task_status_reflect_writes_from_postgres`.
- CLI strict-postgres smoke log contains both:
  - `run_start_and_status_roundtrip_against_postgres`
  - `merge_request_and_status_roundtrip_against_postgres`
- These commands are tracked in `evidence/<loop-id>/` when run via `scripts/verify_acceptance_rubric.sh`.

## Durable Local Configuration

Create `yarli.toml` using `yarli.example.toml` as a starting point. Durable write commands require:

```toml
[core]
backend = "postgres"

[postgres]
database_url = "postgres://postgres:postgres@localhost:5432/yarli"
# or
database_url_file = "/run/secrets/yarli-postgres-url"
```

`core.backend = "in-memory"` is supported only for explicit ephemeral workflows, and write commands are blocked unless `core.allow_in_memory_writes = true`.

### Secret rotation for durable deployments

To avoid rebuilding images when credentials rotate:

- Set `DATABASE_URL` in the runtime environment and rely on deployment restart/reload.
- Or mount credentials at `postgres.database_url_file` and rotate the mounted secret.

### API key management and rotation

API keys control optional API authentication when `YARLI_API_KEYS` is set in the runtime environment.

- Set `YARLI_API_KEYS` to a comma-separated list of allowed API keys.
- Set `YARLI_API_RATE_LIMIT_PER_MINUTE` (default: `120`) to tune per-key request throttling.
- Rotate keys by updating the environment value and restarting or reloading the API process.

Authentication is required when `YARLI_API_KEYS` is non-empty and applies to all non-health/metrics routes.

### Zero-downtime scheduler upgrades (blue/green)

Use this runbook for minor/patch upgrades and scheduler-safe rollouts:

1. Build and validate the upgraded image as `yarli-scheduler:<new-version>`.
2. Deploy the upgraded scheduler as a green replica (same queue/postgres view, no traffic yet).
3. Pause active work on the blue scheduler: `yarli run pause --all-active --reason "pre-upgrade drain"`.
4. Wait for in-flight tasks to reach a natural checkpoint (`TaskComplete` or explicit operator-reviewed pause state).
5. Stop the blue scheduler process after queue drain progress indicates no additional claims.
6. Shift traffic to green (`kubectl rollout`, service selector swap, or equivalent).
7. Resume paused runs: `yarli run resume --all-paused --reason "upgrade complete"`.

If green shows stable health and queue progress resumes, proceed. If not, swap traffic back to blue and use
`yarli run pause`/`yarli run cancel` to contain impact while diagnostics run.

Operational checkpoint before rollout:

- `yarli run status --all` shows bounded backlog growth and no unexpected `TaskFailed` spikes.
- Existing active run IDs remain visible and resumable from `run status` and `run explain-exit`.
- Queue lease owner identities should only be from the active scheduler deployment.

### Event schema compatibility matrix

YARLI does not require coordinated downtime for additive event-schema changes.

| Producer schema | Consumer schema | Additive fields only | Backward reads | Breaking reads |
| --- | --- | --- | --- | --- |
| 1.0 | 1.0 | allowed | allowed | blocked |
| 1.1 (schema extension) | 1.0 | allowed | allowed | blocked |
| 1.0 | 1.1 | allowed | allowed | blocked |
| 1.1 | 1.2 | allowed | allowed with defaults | blocked |
| 1.2 | 1.1 | required | blocked | blocked |

Guidance:

- Keep event consumers resilient to optional/new fields.
- For breaking payload changes, land a compatibility shim before enabling mixed-version rollouts.
- Record any approved schema bump in `docs/ACCEPTANCE_RUBRIC.md` and rollback plan.

## Execution Backend Selection

Native process execution remains default:

```toml
[execution]
runner = "native"
```

## Default `yarli run` (Config-First)

To avoid repeating long `--cmd ...` lists, YARLI is opinionated: `yarli run` resolves prompt context in this order, expands `@include <path>`, and drives execution from config + plan state.

1. `yarli run --prompt-file <path>`
2. `yarli.toml` `[run].prompt_file`
3. fallback lookup of `PROMPT.md` by walking upward from the current directory

Execution behavior:

- Discover incomplete tranches from `IMPLEMENTATION_PLAN.md` in plan order.
- Dispatch each tranche as its own Yarli task via `[cli]` command settings.
- Optional grouping: set `[run].enable_plan_tranche_grouping = true` and annotate related plan lines with `tranche_group=<name>` to dispatch shared tranches.
- Optional per-tranche file-scope policy: annotate plan lines with `allowed_paths=path/a,path/b` and set `[run].enforce_plan_tranche_allowed_paths = true` to inject explicit scope constraints into tranche prompts.
- Optional tranche hardening: configure `[run.tranche_contract]` to require `verify`, require `done_when`, and fail merge finalization on out-of-scope edits. These checks are opt-in and preserve legacy behavior when unset.
- Optional run-spec defaults can live in `yarli.toml` (`[run]`, `[[run.tasks]]`, `[[run.tranches]]`, `[run.plan_guard]`), with `PROMPT.md` `yarli-run` blocks used only for per-prompt overrides/backward compatibility.
- Parallel mode defaults to enabled (`[features].parallel = true`) and requires `[execution].worktree_root`.
- In parallel mode, YARLI prepares one workspace copy per task under `execution.worktree_root` before execution.
- After `RunCompleted`, YARLI auto-merges task workspace changes into the source repo using `git apply --3way`.
- If a workspace merge conflicts, YARLI emits `run.parallel_merge_failed`, exits non-zero, and leaves conflict markers for manual resolution.
- Append a verification task automatically.
- If no embedded run spec exists and no incomplete tranches are found, dispatch the full prompt text as one task.
- `[cli].env_unset` can remove parent-session environment variables before CLI launch (for example `CLAUDECODE`).

Control model clarifications:

- `yarli run` is the authoritative execution entry point.
- Built-in Yarli policy gates are code-defined checks; verification command chains come from plan/config/script content.
- Observer integrations emit telemetry only and must not gate active run progression.
- Use explicit operator controls for run state transitions: `yarli run pause`, `yarli run resume`, `yarli run cancel`.

```bash
yarli run --stream
```

Notes:

- `yarli run start ... --cmd ...` remains available for ad-hoc runs.
- `yarli run batch` and `[run]` paces are supported for backward compatibility but are no longer the primary workflow.

Optional Overwatch-backed execution is opt-in:

```toml
[execution]
runner = "overwatch"

[execution.overwatch]
service_url = "http://127.0.0.1:8089"
profile = "default"
soft_timeout_seconds = 900
silent_timeout_seconds = 300
max_log_bytes = 131072
```

Behavior notes:

- `runner = "native"` keeps current local process behavior unchanged.
- `runner = "overwatch"` submits commands to Overwatch (`/run`), polls `/status/{task_id}`, reads final logs from `/output/{task_id}`, and maps terminal state into YARLI command transitions.
- Scheduler shutdown cancellation propagates to command execution; Overwatch runner calls `/cancel/{task_id}` for in-flight tasks.
- Cancellation transitions emit structured provenance (`run.cancel_provenance` / `task.cancel_provenance`) including signal identity, receiver/parent PID context, actor kind/detail, and stage attribution.
- `ui.verbose_output` affects stream verbosity only. Provenance capture is independent; enable `ui.cancellation_diagnostics = true` to append extra diagnostic context in provenance `actor_detail`.
- Durable provenance history requires Postgres backend. In-memory backend keeps provenance only for the lifetime of the current process (plus any existing `.yarli/continuation.json` artifact).

## Runner Hardening Telemetry and cgroup Delegation

Use these signals when validating the runner hardening path in production-like environments:

- `yarli_enforcement_outcomes_total{mechanism,outcome,reason}`
  - `mechanism="rlimit"` confirms pre-exec `setrlimit(2)` application.
  - `mechanism="pidfd"` distinguishes race-free pidfd control from `raw_pid` fallback.
  - `mechanism="cgroup"` reports `attached` success or `rlimits_only_*` fallback reasons.
  - `mechanism="pid_termination"` records bounded failure reasons during cancellation escalation.
- `yarli_command_overhead_duration_seconds`
  - Useful for spotting expensive spawn/resource-capture phases during runner rollout.
- `/proc/self/cgroup`
  - Confirms whether the YARLI process is operating inside the expected delegated subtree.

Delegation requirements for cgroup v2:

- YARLI needs a writable delegated cgroup subtree, not blanket write access to the root hierarchy.
- In systemd environments, run the service in a delegated unit/slice so it can create leaf cgroups below its assigned boundary.
- In container/Kubernetes environments, verify the container runtime exposes a writable subtree for the service user before expecting cgroup enforcement.
- Non-root execution is supported; if the subtree is read-only or not delegated, YARLI falls back to rlimits-only mode and emits `rlimits_only_*` telemetry.

Operator checklist before enabling cgroup enforcement:

1. Confirm the service user is non-root and can create/remove a test directory under its delegated cgroup subtree.
2. Start YARLI and hit `/metrics`, then verify `yarli_enforcement_outcomes_total` emits either `attached` or an explicit `rlimits_only_*` fallback.
3. Run a bounded verification command and confirm `yarli debug resource-usage <run-id>` and `/proc/self/cgroup` match expectations.
4. If metrics show repeated `permission_denied` or `read_only` fallback, keep the deployment in rlimits-only mode until delegation is fixed.

## sw4rm Fallback Behavior

`yarli run sw4rm` is intentionally fail-closed around transport setup and stream loss:

- Invalid merged sw4rm run-spec configuration falls back to a minimal verification suite (`cargo build`) so the agent still has a bounded verification path.
- Router/report client initialization, runtime init, or registry registration failures stop startup immediately and return a non-zero error to the operator.
- If the response correlation is dropped because shutdown/preemption cancels the in-flight stream, the orchestrator surfaces `Cancelled` rather than fabricating a result.
- If the correlation stays pending past `sw4rm.llm_response_timeout_secs`, the orchestrator cleans up the pending entry and returns `LlmTimeout`.

Recommended operator response:

1. Treat startup failures as connectivity/configuration issues, not partial success.
2. Retry only after router/registry reachability and credentials are confirmed.
3. Treat `Cancelled` as an interrupted orchestration and resume or requeue explicitly.
4. Treat `LlmTimeout` as an execution-budget or transport-latency issue and inspect router/runtime health before retrying.

## Runtime Resource and Token Budgets

YARLI can enforce explicit per-task and per-run budgets from `yarli.toml`:

```toml
[budgets]
max_task_total_tokens = 25000
max_run_total_tokens = 250000
max_task_rss_bytes = 1073741824
max_run_peak_rss_bytes = 2147483648
```

Behavior:

- Budget breaches are fail-closed (`task.failed` with `reason = "budget_exceeded"`).
- Breach events include observed metric values, limits, command resource usage, token usage, and run usage totals.
- Token usage currently uses deterministic `char_count_div4_estimate_v1` estimation and is recorded in command/task events.

Operator-visible command surfaces for resource_usage and token_usage:

- `yarli run status <run-id>`: shows per-task `budget_exceeded`, `token_usage` (prompt_tokens, completion_tokens, total_tokens), and `resource_usage` (max_rss_bytes).
- `yarli run explain-exit <run-id>`: shows `Budget breaches:` section with task name and breach detail, plus per-task token and resource usage.
- `yarli task explain <task-id>`: shows `budget_exceeded` reason, token usage, and resource usage for individual tasks.
- `yarli task output <task-id>`: prints the raw captured stdout/stderr for a task when durable output events are available.

Durability note:

- `core.backend = "in-memory"` is ephemeral; captured task output and local audit history are lost when the process exits.
- `core.backend = "postgres"` preserves run/task state and captured output for later diagnosis.

## Sequence Deterioration Observer

YARLI emits rolling deterioration analysis events (`run.observer.deterioration`) during run execution.
The observer consumes event deltas incrementally using event-store cursor reads (`after_event_id`), not full-history rescans.
YARLI also emits heartbeat progress events (`run.observer.progress`) so stream output remains active during long-running tasks.

Signals included in the score:

- Runtime drift for repeated command keys.
- Retry inflation and blocker churn.
- Failure-rate drift by reason bucket.
- Budget headroom erosion (when observed/limit values are present).

Operator surfaces:

- `yarli run status <run-id>` includes latest deterioration score/trend/factors when available.
- `yarli run explain-exit <run-id>` includes a sequence deterioration section.
- `yarli-api` (if you run it) exposes the latest report as an optional `deterioration` field on `GET /v1/runs/<run-id>/status`.

## Runtime Guard Telemetry and Operator Playbook

Runtime guards now combine hard guardrails and quality gating:

- Budget guardrails from `[budgets]` stop execution on token/usage overages.
- Task-health and soft-token-cap guardrails produce continuation guidance (`checkpoint-now`, `force-pivot`, or `stop-and-summarize`) on completion.
- Guard telemetry is observability-first and does not itself gate active scheduler semantics.

Telemetry to monitor:

- `run.observer.deterioration` and `run.observer.deterioration_cycle`
  - Detect recurring instability patterns before automatic continuation paths drift too far.
- `run.observer.progress`
  - Confirms progress visibility for long-running tasks and supports hang triage.
- `task.failed` with `reason = "budget_exceeded"`
  - Includes observed/limit metrics, command usage, token usage, and aggregate run usage snapshots.
- `run.continuation` (`quality_gate` payload)
  - Captures `task_health_action`, `reason`, and optional `trend` for operator follow-up.

Operator playbook:

Quick diagnosis cheat sheet:

- Why did the run stop? -> `yarli run explain-exit <run-id>`
- What did the task actually print? -> `yarli task output <task-id>`
- Why is a task blocked or failing? -> `yarli task explain <task-id>`
- What policy or gate decided this? -> `yarli audit query --task-id <task-id> --category gate_evaluation`

- Budget breach (`budget_exceeded`) response:
  1. Pause if active: `yarli run pause <run-id>`
  2. Inspect: `yarli run status <run-id>`, `yarli run explain-exit <run-id>`, `yarli task explain <task-id>`, `yarli task output <task-id>`
  3. Decide: lower scope, raise limits, or cancel and rerun (`yarli run cancel <run-id>`)
- Deterioration-cycle response:
  1. Inspect current state with `yarli run status <run-id>` and `yarli run explain-exit <run-id>`
  2. If guidance is `force-pivot` or `stop-and-summarize`, pause/stop before continuing
  3. Re-run with narrower scope, then continue via `yarli run continue` only if the continuation snapshot still matches current open tranches
- Soft-cap continuation guidance (`checkpoint-now`):
  1. Treat as a review checkpoint and avoid blind continuation
  2. Evaluate remaining objective and current usage headroom
  3. If safe, continue; otherwise cancel and restart with adjusted `[run]` strategy

Continuation semantics:

- `yarli run continue` replays the prior continuation snapshot and is the right tool for retry/unfinished/planned-next work that already belongs to that snapshot.
- `yarli run --fresh-from-tranches` rebuilds from the current prompt/plan/tranches state and is the right tool after you enqueue new tranches or otherwise change `.yarli/tranches.toml`.
- When current `.yarli/tranches.toml` contains open tranche keys not represented in the continuation snapshot, `yarli run continue` now refuses with an actionable drift message instead of silently replaying stale scope.

## Merge Conflict Policy and Incident Response

- `run.parallel_merge_failed` is emitted for unresolved or unrecoverable conflicts during parallel workspace merge finalization.
- `run.parallel_merge_succeeded` confirms successful parallel merge finalization.
- Merge telemetry events are also emitted as part of merge apply tracking:
  - `merge.apply.started`
  - `merge.apply.conflict`
  - `merge.apply.finalized`
  - `merge.repair.succeeded`
  - `merge.repair.failed`

Merge policy modes (`[run].merge_conflict_resolution`) are:

- `fail` (default): stop on conflict, preserve workspace for operator review.
- `manual`: preserve workspace and emit explicit recovery instructions for manual intervention.
- `auto-repair`: attempt deterministic patch-side repair before falling back to manual recovery.

Incident response workflow:

- Pull latest failure context:
  1. `yarli run status <run-id>`
  2. `yarli run explain-exit <run-id>`
  3. `yarli task output <task-id>` for raw command stdout/stderr from the failing task.
  4. `yarli audit tail` or `yarli audit query` for merge telemetry, policy decisions, and task-level context.
- Open `PARALLEL_MERGE_RECOVERY.txt` in the preserved workspace root reported by `run.parallel_merge_failed`.
- Run recovery commands from the note in order (status, patch diff stats, manual `git apply` retry).
- Resolve conflicts and re-run with your normal operator decision path:
  - `yarli run continue` when continuation is available and still matches current open tranches, or
  - rerun the target tranche after scope and policy review.
- If recovery requires policy change, update `run.merge_conflict_resolution` (`fail`, `manual`, or `auto-repair`) and resume according to post-incident policy.

Evidence capture for incident records should include:

- `run.parallel_merge_failed` payload (including `task_key`, `patch_path`, `workspace_path`, `conflicted_files`, `repo_status`, `recovery_hints`)
- associated `merge.apply.*` and `merge.repair.*` telemetry events
- `PARALLEL_MERGE_RECOVERY.txt` and operator remediation steps.

Policy defaults:

- Keep guard-related telemetry in evidence (`evidence/<loop-id>/`) to preserve trend history.
- Avoid runtime-guard bypass shortcuts; use continuation/replan actions and explicit operator commands.
- Treat `yarli audit tail` as a local JSONL view by default; in durable deployments it complements, but does not replace, the persisted event-store record.

## External Agent Skill Contract

The external `yarli-execution-loop` skill is expected to treat Yarli as the durable control plane:

- Inspect Yarli state first (`.yarli/continuation.json`, `.yarli/tranches.toml`, `yarli run status`, `yarli run explain-exit`).
- Enqueue newly discovered work durably through `yarli plan tranche add --idempotent`.
- Choose `yarli run continue` only for snapshot-owned work with no drift.
- Choose `yarli run --fresh-from-tranches` after new tranche enqueue or other live-plan changes.
- End each agent cycle with a `YARLI_DECISION_V1` block containing `status`, `reason`, `enqueued_tranches`, and `next_command`.

### Idempotent Tranche Enqueue

`yarli plan tranche add --idempotent` is designed for safe repeated invocation in
agent loops. The semantics are:

- **Matching fields → no-op.** If a tranche with the given key already exists and
  all effective fields (summary, group, allowed_paths, verify, done_when, max_tokens)
  match the request, the command prints a confirmation message and returns success
  without modifying the file.
- **Mismatched fields → error.** If the key exists but any effective field differs,
  the command fails with a message listing which fields differ. This prevents
  accidental overwrites while making the conflict visible to the caller.
- **New key → normal add.** If no tranche with the key exists, the tranche is
  appended exactly as with a non-idempotent `add`.
- **Safe for repeated invocation.** Because identical calls are no-ops and
  conflicting calls are errors, agents can call `--idempotent` unconditionally at the
  start of every cycle without side effects or silent data loss.

### Strict Tranche Contract

Use `[run.tranche_contract]` when agent consumers need stronger execution guarantees without breaking legacy repositories:

- `strict = true` turns on all checks below.
- `require_verify = true` rejects open tranches that omit `verify`.
- `require_done_when = true` rejects open tranches that omit `done_when`.
- `enforce_allowed_paths_on_merge = true` fail-closes merge finalization when a tranche edits paths outside its declared `allowed_paths`.

Behavior notes:

- These checks apply to open tranches only; completed legacy tranches are left alone.
- When merge-time path enforcement is enabled, YARLI automatically surfaces `allowed_paths` in tranche prompts even if `[run].enforce_plan_tranche_allowed_paths` is otherwise false.
- Control-plane files such as `IMPLEMENTATION_PLAN.md` and `.yarli/*` remain exempt so normal plan/evidence bookkeeping does not trip the scope gate.

## Reproducing Budget Stress Checks Locally

Run these commands to verify budget governance under parallel workload:

```bash
# Single-task budget breach: task exceeds token limit, fails without retry
cargo test -p yarli test_budget_exceeded_fails_task_without_retry -- --nocapture

# Run-level budget breach: cumulative tokens across tasks exceed run limit
cargo test -p yarli test_run_token_budget_exceeded_across_tasks -- --nocapture

# Parallel-task budget stress: 4 concurrent tasks with tight run budget,
# proves accounting consistency and no silent continuation after breach
cargo test -p yarli test_parallel_tasks_budget_accounting_consistency -- --nocapture

# Command execution resource capture (exec layer)
cargo test -p yarli-exec -- --nocapture
```

Expected behavior:

- All commands exit `0`.
- Budget failure events include `reason = "budget_exceeded"` with observed/limit metrics.
- Run does not reach `RunCompleted` after any budget breach.

## Migration Workflow

YARLI schema SQL lives under `crates/yarli-store/migrations/`.

Use CLI migration workflow:

```bash
yarli migrate status
yarli migrate up
yarli migrate backup --label <label>   # optional before maintenance windows
yarli migrate down --target 0002         # optional rollback (creates backup)
yarli migrate restore --label <label>     # rollback recovery
```

Recommended:

- Use a dedicated database for YARLI runtime data.
- Apply migrations before running durable CLI write commands.

## Event Store Backup and Restore for Production

YARLI’s source of truth is the Postgres event store and queue state.

Recommended production backup policy:

- Daily validated dump.
- Timely incremental backup or PITR according to retention policy.
- Quarterly restore drill in a non-production cluster.

Example full backup command:

```bash
export YARLI_DB_URL="postgres://postgres:postgres@postgres.example.internal:5432/yarli"
export BACKUP_FILE="/var/backups/yarli-eventstore-$(date -u +%Y%m%dT%H%M%SZ).dump"
pg_dump -Fc "$YARLI_DB_URL" > "$BACKUP_FILE"
sha256sum "$BACKUP_FILE" > "${BACKUP_FILE}.sha256"
```

Example restore validation workflow:

```bash
export YARLI_RESTORE_DB_URL="postgres://postgres:postgres@postgres.example.internal:5432/yarli_restore"
createdb -h postgres.example.internal -U postgres yarli_restore
pg_restore --clean --if-exists --no-owner --no-privileges --dbname "$YARLI_RESTORE_DB_URL" "$BACKUP_FILE"
export DATABASE_URL="$YARLI_RESTORE_DB_URL"
yarli migrate status
```

Validation rules:

- Restore commands complete without ownership/permission warnings.
- `yarli migrate status` shows a healthy, readable event store.
- `yarli run status --all` works against restored data.

## Scaling Strategy for Queue and Scheduler Capacity

- Use `yarli run status --all` and run-level backlog patterns as first indicators.
- Horizontal scale first when sustained backlog grows across multiple active runs:
  - `helm upgrade <release> deploy/helm/yarli --set scheduler.replicas=<N>`
- Vertical tuning when backlog remains with healthy pod counts:
  - update scheduler CPU/memory in `deploy/helm/yarli/values.yaml` (`scheduler.resources`).
- Throughput tuning for event-driven claim bursts:
  - `queue.per_run_cap`
  - `queue.io_cap`
  - `queue.cpu_cap`
  - `queue.git_cap`
  - `queue.tool_cap`
- Vertical and horizontal changes should be paired with post-scale verification.

## Incident Response Playbook

Use this flow for production incidents requiring operator action without code changes.

### 1) Stuck runs

1. `yarli run status --all`
2. For the affected run:
   - `yarli run explain-exit <run-id>`
   - `yarli task list <run-id>`
   - `yarli task explain <task-id>`
3. Pull recent operator telemetry:
   - `yarli audit tail`
4. If the run must be stopped:
   - `yarli run pause --all-active --reason "incident triage"`
   - fix root cause, then `yarli run resume --all-paused --reason "triage complete"`

### 2) Queue backlogs

1. Confirm queue behavior:
   - `yarli run status --all`
2. Correlate with `run` and `task` telemetry:
   - `yarli audit tail`
3. Contain and recover:
   - reduce new claim pressure if needed
   - increase scheduler replicas
   - pause and drain if the backlog is destabilizing critical workloads
4. Resume in a controlled mode after scheduler and DB signals stabilize.

### 3) Budget breaches

1. Detect breach and isolate impact:
   - `yarli task explain <task-id>`
   - `yarli run explain-exit <run-id>`
2. Preserve incident evidence:
   - `yarli run status <run-id>`
   - `yarli run explain-exit <run-id>`
   - `yarli audit tail`
3. Corrective actions:
   - `yarli run pause <run-id>` if mitigation needs immediate human hold
   - adjust `[budgets]` for rerun
   - `yarli run continue` after intent review.

## Deterministic Local Strict Postgres Verification Workflow

Run this exact sequence from repository root on a clean shell.

Start and prepare a local Postgres 16 instance:

```bash
docker rm -f yarli-r4v-postgres >/dev/null 2>&1 || true
docker run -d --name yarli-r4v-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 55432:5432 \
  postgres:16
until pg_isready -h 127.0.0.1 -p 55432 -U postgres -d postgres >/dev/null 2>&1; do sleep 1; done
```

Sanity-check strict mode fails fast when DB env is missing:

```bash
unset YARLI_TEST_DATABASE_URL
export YARLI_REQUIRE_POSTGRES_TESTS=1
cargo test -p yarli --test yarli_store_postgres_integration -- --nocapture
```

Expected result: command fails and includes
`postgres integration tests require YARLI_TEST_DATABASE_URL when YARLI_REQUIRE_POSTGRES_TESTS=1`.

Run strict Postgres integration suites (real execution path):

```bash
export YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
export YARLI_REQUIRE_POSTGRES_TESTS=1
cargo test -p yarli --test yarli_store_postgres_integration -- --nocapture
cargo test -p yarli --test yarli_queue_postgres_integration -- --nocapture
cargo test -p yarli --test yarli_cli_postgres_integration -- --nocapture
```

True execution signals:

- Each command exits with status `0`.
- Output contains `test result: ok`.
- Output does not contain `skipping postgres integration test`.

Cleanup:

```bash
docker rm -f yarli-r4v-postgres
```

## Baseline Workspace Verification

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

## Consistency and Governance Contract

See `docs/CONSISTENCY_CONTRACT.md` for the canonical strong-consistency and runtime-governance contract, including:

- Per-aggregate transition ordering and linearizable state.
- Durable-state-before-side-effects invariant.
- Idempotency key replay behavior.
- Queue lease single-owner invariant.
- Resource and token accounting fields and budget breach outcomes.

## Acceptance Decision

Use `docs/ACCEPTANCE_RUBRIC.md` to determine a binary outcome:

- `PASS`: all required checks are proven with tracked evidence.
- `UNVERIFIED`: any required check is missing, failing, or not evidenced.

## Telemetry and Observability

YARLI exports structured telemetry via OpenTelemetry (OTLP) when configured. This enables deep introspection into scheduler performance, command execution latency, and resource utilization.

### OTLP Collector Setup (Local Example)

To capture and visualize telemetry locally using Jaeger (traces) and Prometheus (metrics):

1. **Start the collector stack (Jaeger + Prometheus + OTel Collector):**

   ```bash
   docker run -d --name jaeger \
     -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
     -p 5775:5775/udp \
     -p 6831:6831/udp \
     -p 6832:6832/udp \
     -p 5778:5778 \
     -p 16686:16686 \
     -p 14268:14268 \
     -p 14250:14250 \
     -p 9411:9411 \
     jaegertracing/all-in-one:1.60

   # (Optional) Prometheus and OTel Collector would go here for a full stack.
   # For basic tracing, Jaeger all-in-one accepts OTLP over gRPC at 4317 if configured,
   # or you can point YARLI directly to a collector.
   ```

2. **Configure YARLI to export telemetry:**

   Set environment variables before running `yarli run`:

   ```bash
   export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"
   export OTEL_SERVICE_NAME="yarli-scheduler"
   export RUST_LOG="info,yarli=debug" # Optional: enable debug logs for correlation
   ```

3. **Visualize Traces:**
   Open http://localhost:16686/ in your browser.

### Metric and Trace Conventions

**Metrics (Prometheus format):**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `yarli_queue_depth` | Gauge | - | Current tasks in queue (pending + leased). |
| `yarli_runs_total` | Counter | `state` | Run state transitions. |
| `yarli_tasks_total` | Counter | `state`, `command_class` | Task state transitions. |
| `yarli_commands_total` | Counter | `command_class`, `exit_reason` | Command execution outcomes. |
| `yarli_command_duration_seconds` | Histogram | `command_class` | End-to-end command duration. |
| `yarli_command_overhead_duration_seconds` | Histogram | `command_class`, `phase` | Internal overhead (spawn, capture). |
| `yarli_scheduler_tick_duration_seconds` | Histogram | `stage` | Scheduler loop timing (`scan`, `claim`, etc). |
| `yarli_store_duration_seconds` | Histogram | `operation` | Event store latency (`append`, `query`). |
| `yarli_store_slow_queries` | Counter | `operation` | Database operations exceeding 1s. |

**Traces (Spans):**

- `run_execution`: Root span for a `yarli run` invocation.
- `scheduler.tick`: Wraps a single scheduler loop iteration.
- `scheduler.execute_task`: Wraps a task execution attempt.
- `command.execute`: Wraps the low-level process spawn and wait.
- `store.append`, `store.query`: Wraps database operations.

### Recommended Alerting Thresholds

For production deployments, monitor these signals:

1. **Stalled Queue:**
   - Alert if `yarli_queue_depth > 0` and `yarli_scheduler_tick_duration_seconds_count` stops increasing for 5m.
   - Indicates scheduler hang or crash.

2. **High Failure Rate:**
   - Alert if `rate(yarli_commands_total{exit_reason!="success"}[5m]) / rate(yarli_commands_total[5m]) > 0.1`.
   - Indicates systemic failure (bad config, network down).

3. **Slow Database:**
   - Alert if `rate(yarli_store_slow_queries[5m]) > 0`.
   - Indicates DB performance degradation affecting scheduler throughput.

4. **Resource Saturation:**
   - Alert if `yarli_run_resource_usage` approaches configured budget limits.

### Troubleshooting Telemetry

**Missing Exports:**
- Verify `OTEL_EXPORTER_OTLP_ENDPOINT` is reachable.
- Check logs for "OpenTelemetry trace error" or "metrics export failed".
- Ensure the collector accepts OTLP/gRPC (port 4317) or OTLP/HTTP (port 4318) matching your endpoint scheme.

**High Cardinality:**
- YARLI metrics use bounded label sets (enums).
- Avoid adding unbounded labels (like `run_id` or `task_id`) to long-lived metrics.
- `run_resource_usage` is an exception; it is labelled by `run_id` but should be transient or aggregated.
