# CARD-R15-01 Evidence

Date: 2026-02-22
Tranche: Run lifecycle mutation APIs
Workspace: `/home/rahul/Documents/worktree/yarli/run-019c86dbd41c/001-tranche-013-card-r15-01`

## Changes made
- Added API endpoints in `crates/yarli-api/src/server.rs`:
  - `POST /v1/runs`
  - `POST /v1/runs/:run_id/pause`
  - `POST /v1/runs/:run_id/resume`
  - `POST /v1/runs/:run_id/cancel`
- Added request payload handling and idempotency support for the new mutation endpoints.
- Added validation and transition handling for run state changes with 409-level conflict reporting.
- Added tests for create run idempotency and pause/resume/cancel behavior.

## Verification commands
1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test --workspace`

## Verification evidence
- `cargo fmt --all -- --check`: pass (exit `0`).
- `cargo clippy --workspace --all-targets -- -D warnings`: pass (exit `0`).
- `cargo test --workspace`: fail (exit `101`).
  - Failing test: `resource_exhaustion_disk_full_append_failure_is_recoverable` in
    `tests/integration/tests/resource_exhaustion.rs`.
  - Failure message: `recoverable append failure should not block completion`.
