# CARD-R11-02 Evidence

Date: 2026-02-22

## Scope
- Added a task-health artifact observer for incremental `.yarl/runs/{task_id}.jsonl` tail reads in `crates/yarli-cli/src/observers.rs`.
- Wired the new artifact observer into `drive_scheduler` in `crates/yarli-cli/src/commands.rs` so parsed tool outcomes are emitted as structured `run.observer.task_health` events during incremental scheduler ticks and cancel paths.
- Added verification through existing unit coverage for tool outcome parsing and artifact tail processing.

## Verification
- Command: `cargo test -p yarli-cli`
- Result: **FAILED** (exit code: 1)

### Key output
- `test observers::tests::parse_tool_health_observation_reads_nested_tool_payload ... ok`
- `test observers::tests::task_health_observer_reads_task_output_artifact_incrementally ... ok`
- `test tranche::tests::auto_complete_structured_tranche_marks_matching_key_using_prompt_root ... FAILED`
- `test result: FAILED. 236 passed; 1 failed; 0 ignored; 0 filtered out; finished in 1.01s`
- Failure detail:
  - `thread 'tranche::tests::auto_complete_structured_tranche_marks_matching_key_using_prompt_root' panicked ... Os { code: 2, kind: NotFound, message: "No such file or directory" }`
