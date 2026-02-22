# CARD-R15-06 Verification Evidence

Date: 2026-02-22  
Card: API authentication and rate limiting  
Tranche run: `001-tranche-018-card-r15-06`

## Summary

Target tranche implementation is present and code changes were previously made for:

- API key authentication for non-public API routes (`Bearer`, `x-api-key`, `api-key`)
- Per-key fixed-window rate limiting and `Retry-After` handling
- Audit actor propagation for API mutation events
- Documentation updates for security and API key rotation
- Target tests added under `crates/yarli-api/src/server.rs`

## Required verification commands

### 1) Format

Command:
`cargo fmt --all -- --check`

Result:
- Exit code: `0`
- Outcome: `PASS`
- Key output: none (check completed successfully)

### 2) Lint

Command:
`cargo clippy --workspace --all-targets -- -D warnings`

Result:
- Exit code: `101`
- Outcome: `FAIL`
- Failure excerpt:

```
error[E0428]: the name `override_priority` is defined multiple times
   --> crates/yarli-queue/src/queue.rs:217:5
...
error[E0201]: duplicate definitions with name `override_priority`:
   --> crates/yarli-queue/src/memory.rs:340:5
...
error[E0201]: duplicate definitions with name `override_priority`:
   --> crates/yarli-queue/src/postgres.rs:556:5
...
error[E0046]: not all trait items implemented, missing: `override_priority`
   --> crates/yarli-queue/src/memory.rs:50:1
```

### 3) Test

Command:
`cargo test --workspace`

Result:
- Exit code: `101`
- Outcome: `FAIL`
- Failure cause: same `override_priority` duplicate-definition errors in `crates/yarli-queue` as above.

## Current status

- Tranche behavior appears implemented, but full workspace verification is currently blocked by a pre-existing duplicate `override_priority` regression in `crates/yarli-queue`.
- This issue is outside the scope of `CARD-R15-06` work and prevents this tranche from being marked fully verified until fixed.
