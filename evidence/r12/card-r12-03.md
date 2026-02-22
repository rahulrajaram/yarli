# CARD-R12-03 Evidence: Runtime introspection endpoints

Date: 2026-02-22
Card: `CARD-R12-03`

## Scope
- Added CLI debug commands:
  - `yarli debug queue-depth`
  - `yarli debug active-leases`
  - `yarli debug resource-usage <run-id>`
- Added optional debug API routes behind feature flag in `yarli-api`:
  - `GET /debug/queue-depth`
  - `GET /debug/active-leases`
  - `GET /debug/resource-usage/:run_id`
- Extended queue abstraction with non-destructive `entries()` helper and implemented for memory/Postgres queues.
- Added API-side aggregation helpers for per-run resource totals and run-level budget extraction.

## Verification
- Ran required command:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
- Result: all passed.
