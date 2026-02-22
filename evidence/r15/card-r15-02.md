# CARD-R15-02 Evidence

Date: 2026-02-22
Tranche: Task mutation and annotation APIs
Workspace: `/home/rahul/Documents/worktree/yarli/run-019c86e204f7/001-tranche-014-card-r15-02`

## Changes made
- Added task mutation endpoints in `crates/yarli-api/src/server.rs`:
  - `POST /v1/tasks/{task_id}/annotate`
  - `POST /v1/tasks/{task_id}/unblock`
  - `POST /v1/tasks/{task_id}/retry`
  - `POST /v1/tasks/{task_id}/priority` (debug queue router only)
- Added task transition validation and structured error handling for task mutations.
- Added queue-side priority override support in `TaskQueue`:
  - `crates/yarli-queue/src/memory.rs`
  - `crates/yarli-queue/src/postgres.rs`
- Added unit tests for in-memory override in `crates/yarli-queue/src/memory/tests.rs`.
- Added API tests for annotation/unblock/retry/priority override in `crates/yarli-api/src/server.rs`.
- Updated API contract documentation in `docs/API_CONTRACT.md`.

## Verification commands
1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test --workspace`

## Verification evidence
- To be filled after running verification.
