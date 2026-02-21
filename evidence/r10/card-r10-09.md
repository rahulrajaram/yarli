# CARD-R10-09 Evidence

Date: 2026-02-21

## Scope
- Added regression coverage for overlapping merge conflicts and auto-repair success/failure transitions.
- Added continuation/output status consistency assertions.

## Verification
- Command: `cargo test`
- Command: `cargo test -p yarli-cli --test parallel_merge_integration`

## Notes
- Tests include:
  - overlap conflict reproduction with preserved unmerged artifacts (failure path)
  - auto-repair success path for overlap conflict
  - auto-repair failure path for overlap/delete conflict
  - status-consistency assertions that compare `run` output state with continuation `exit_state`
