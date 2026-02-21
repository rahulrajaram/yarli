# CARD-R11-01 Evidence

Date: 2026-02-22

## Scope
- Added backend command-output artifact persistence at `.yarl/runs/{task_id}.jsonl` in `yarli-exec`.
- Added task-output artifact read path in `render_task_output` with event-store fallback.
- Added unit test coverage for artifact-backed task output rendering.

## Verification
- Command: `cargo test -p yarli-cli`
- Result: **FAILED** (exit code: 1)

### Key output
- `test render::tests::render_task_output_uses_task_artifact_when_available ... ok`
- `test tranche::tests::auto_complete_structured_tranche_marks_matching_key_using_prompt_root ... FAILED`
- `test result: FAILED. 234 passed; 1 failed; 0 ignored; 0 filtered out; finished in 1.01s`
- Failure detail:
  - `thread 'tranche::tests::auto_complete_structured_tranche_marks_matching_key_using_prompt_root' panicked ... Os { code: 2, kind: NotFound, message: "No such file or directory" }`
