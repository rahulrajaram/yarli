# Evidence for CARD-R12-06: Operational runbook for telemetry

## Verification Commands (CARD-R12-06 Tranche)

Command: `cargo fmt --all -- --check`

- Exit code: 0
- Result: passed (no formatting changes required)

Command: `cargo clippy --workspace --all-targets -- -D warnings`

- Exit code: 0
- Result: passed

Command: `cargo test --workspace`

- Exit code: 1
- Result: failed (4 unrelated integration tests in `crates/yarli-cli/tests/parallel_merge_integration.rs` failed)
- Failed tests:
  - `run_start_parallel_merge_conflict_preserves_unmerged_artifacts_and_escalates`
  - `run_start_parallel_merge_lineage_failure_preserves_workspace_and_escalates`
  - `run_start_parallel_merge_auto_repair_fails_and_preserves_conflict_state`
  - `run_start_parallel_merge_ignores_untracked_rust_target_artifact`
- Failure summary: `parallel_merge_integration` reported `4 passed; 4 failed` and terminated the workspace test run.

## Operator Documentation Snapshot

The target file already contains the telemetry/runbook updates:

- `docs/OPERATIONS.md`
  - Added "Telemetry and Observability" section.
  - Documented local OTLP collector example (Jaeger/Prometheus).
  - Added metric/trace naming and cardinality guidance.
  - Added recommended alerting thresholds.
  - Added troubleshooting guidance for missing telemetry exports and high-cardinality issues.
