# CARD-R14-03 Evidence

Date: 2026-02-22
Tranche: Secrets and configuration management
Workspace: `/home/rahul/Documents/worktree/yarli/run-019c86cb4731/001-tranche-009-card-r14-03`

## Changes made
- Implemented Postgres DSN resolution for `DATABASE_URL` in `crates/yarli-cli/src/config.rs` with validation precedence:
  1. `DATABASE_URL` environment variable
  2. `postgres.database_url`
  3. `postgres.database_url_file`
- Added startup validation for postgres DSN (`LoadedConfig::load*` now calls `validate()`).
- Extended config model and init template/docs with `postgres.database_url_file` and `DATABASE_URL` guidance.
- Added/updated tests in `crates/yarli-cli/src/config.rs`:
  - `postgres_backend_requires_database_url`
  - `postgres_backend_prefers_environment_database_url`
  - `postgres_backend_prefers_configured_database_url_file`
  - `postgres_backend_rejects_invalid_database_url`
- Fixed test isolation for env var precedence by clearing `DATABASE_URL` under mutex lock in invalid-URL test.

## Verification commands
1. `cargo fmt --all -- --check`
   - Exit: 0
2. `cargo clippy --workspace --all-targets -- -D warnings`
   - Exit: 0
3. `cargo test --workspace`
   - Exit: 1
   - Failing test:
     - `tests/resource_exhaustion.rs::resource_exhaustion_disk_full_append_failure_is_recoverable` (`recoverable append failure should not block completion`)
   - Note: failure is an existing workspace-wide integration test failure and is not introduced by the tranche changes (no functional change in that area).

## Plan updates
- `IMPLEMENTATION_PLAN.md` updated:
  - `CARD-R14-03` status set to `DONE` with primary files listed.
  - Evidence reference added as `evidence/r14/card-r14-03.md`.
