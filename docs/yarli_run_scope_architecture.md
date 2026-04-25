# YARLI Run Scope Architecture

This note maps the current run-scope boundaries for discovery, operator control,
queue cleanup, and startup preflight behavior.

## Scope Model

Run scope is represented by `RunDiscoveryScope` in
`crates/yarli-cli/src/run_scope.rs`.

- `AllRepos` means the command should inspect every run visible in the event
  store.
- `CurrentScope { scope_root }` means the command should only inspect runs whose
  persisted `run.config_snapshot.config_snapshot.runtime.scope_root` matches the
  current repository root.

When `scope_root` is missing from older snapshots, scope matching falls back to
`runtime.working_dir.starts_with(scope_root)`.

`resolve_run_discovery_scope(false)` resolves the current working directory,
finds the nearest repository root with `plan::find_repo_root`, canonicalizes it
when possible, and returns `CurrentScope`. Passing `true` returns `AllRepos`.
The CLI-level `--all-repos` check is intentionally small and lives next to the
scope helpers.

## Run Discovery

Run discovery is event-store based. `list_runs_by_latest_state_scoped` queries
all `EntityType::Run` events and builds a latest-state map from transition
events via `run_state_from_event`.

Filtering happens only after the run's snapshot payload proves it belongs to the
selected scope:

- `AllRepos`: every parseable run id is included.
- `CurrentScope`: only runs with a matching `run.config_snapshot` are included.

`render_run_list_scoped` in `crates/yarli-cli/src/render.rs` uses the same scope
predicate for display. This keeps operator-facing listing and control selection
aligned.

## Operator Control

`yarli run pause`, `resume`, `drain`, and `cancel` establish scope before they
load the event store and queue handles:

1. Load runtime config through the read/write config boundary.
2. Resolve `RunDiscoveryScope`, honoring `--all-repos`.
3. Select target runs with `select_run_targets_for_control` or
   `resolve_run_id_input_scoped`.
4. Apply the command-specific transition or cancellation behavior in
   `commands.rs`.

The selection helper enforces eligible states before mutation. If no explicit
run id is supplied, one eligible run is selected automatically, multiple
eligible runs require the relevant `--all-*` flag, and empty scope-specific
results include a `--all-repos` hint.

`cancel` uses scoped id resolution for explicit run ids and scoped target
selection for bulk cancellation, then drains queue entries through the queue
handle for each cancelled run.

## Queue Cleanup

Startup queue cleanup happens after the new run has a persisted
`run.config_snapshot` and before the first scheduler tick.

`commands.rs` resolves the current run's persisted scope with
`run_scope_for_run`:

- If the run has a scope, it lists terminal runs in that same scope and calls
  `Scheduler::cleanup_stale_queue_for_runs` for those run ids only.
- If the run has no scope snapshot, it falls back to
  `Scheduler::cleanup_stale_queue`.
- If scope resolution fails, it warns and falls back to global stale cleanup.

This boundary prevents one repository's startup from cancelling queue residue
for unrelated repositories while still preserving a fallback for older or
malformed run records.

## Preflight Boundaries

Preflight checks run before the run is registered and therefore before
run-scope discovery can be based on a new run id.

Current preflights include:

- Prompt/run-spec and structured tranche validation.
- CLI backend preflight through `config::preflight_cli_backend`.
- Dirty submodule guard through `preflight_check_dirty_submodules`, unless
  `[run].allow_dirty_submodules = true` or the equivalent CLI override is used.

These checks are scoped to startup safety and input validity. They do not select
existing runs, mutate operator-control state, or clean queues.

## Ownership Boundary

`run_scope.rs` owns discovery, matching, prefix resolution, list filtering, and
control-target selection. `commands.rs` owns command side effects: appending
events, invoking queue operations, and driving scheduler behavior. Scheduler
task binding remains outside this boundary.
