# CARD-R11-03 Evidence

Date: 2026-02-22

## Scope
- Added backend-output signature ingestion for `run.observer.task_health` failures in `crates/yarli-cli/src/observers.rs`.
- Added repeat and alternating-pattern detection for backend failure signatures.
- Added `run.observer.deterioration_detected` telemetry with signature details/counts.
- Added regression tests for normalization and both detection modes.

## Verification
- Command: `cargo test -p yarli-cli`
- Result: **FAIL/UNVERIFIED (did not complete)**
- Status detail: integration suite did not complete; the run stalled in `parallel_merge_integration`.

### Key output
- `running 142 tests` (lib suite)
- `running 237 tests` (main suite)
- `running 8 tests` (`tests/parallel_merge_integration.rs`)
- `test run_start_parallel_merge_applies_committed_workspace_changes has been running for over 60 seconds`
- `test run_start_parallel_merge_auto_repair_fails_and_preserves_unmerged_artifacts_and_escalates has been running for over 60 seconds`
- `test run_start_parallel_merge_auto_repair_resolves_overlap_and_completes has been running for over 60 seconds`
- `test run_start_parallel_merge_cleans_workspace_root_when_all_tasks_merge has been running for over 60 seconds`
- `test run_start_parallel_merge_conflict_preserves_unmerged_artifacts_and_escalates has been running for over 60 seconds`
- `test run_start_parallel_merge_ignores_untracked_rust_target_artifact has been running for over 60 seconds`
- `test run_start_parallel_merge_lineage_failure_preserves_workspace_and_escalates has been running for over 60 seconds`
- `test run_start_parallel_merge_preserves_workspace_root_when_task_is_skipped has been running for over 60 seconds`
