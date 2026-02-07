# YARLI Acceptance Rubric (Loop R7)

This rubric defines a binary outcome for acceptance evidence closure.

## Canonical Verifier Entrypoint

Use the scripted verifier to execute this rubric consistently:

- `bash scripts/verify_acceptance_rubric.sh <loop-id>`

The script must write tracked evidence under `evidence/<loop-id>/` including per-command logs and a summary `README.md` with command, exit code, and key output.

## Allowed Outcomes

- `PASS`: every required check below is proven with tracked evidence.
- `UNVERIFIED`: any required check is missing, fails, or is not evidenced.

No partial pass state is allowed.

## Required Checks

1. Structural workspace verification
- Run `cargo test --workspace`.
- Require exit code `0`.
- Record command, exit code, and key output lines in tracked evidence.

2. Strict negative-path verification
- Run strict mode without DB URL:
  - `unset YARLI_TEST_DATABASE_URL`
  - `YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-store --test postgres_integration -- --nocapture`
- Require non-zero exit code.
- Require output line:
  - `postgres integration tests require YARLI_TEST_DATABASE_URL when YARLI_REQUIRE_POSTGRES_TESTS=1`

3. Strict positive-path verification
- Set:
  - `YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres`
  - `YARLI_REQUIRE_POSTGRES_TESTS=1`
- Run:
  - `cargo test -p yarli-store --test postgres_integration -- --nocapture`
  - `cargo test -p yarli-queue --test postgres_integration -- --nocapture`
  - `cargo test -p yarli-cli --test postgres_integration -- --nocapture`
- Each command must exit `0` and show test success output.

4. Non-skip enforcement in strict runs
- Strict evidence must not contain `skipping postgres integration test`.
- Verify by searching tracked strict-run evidence logs/sections.

5. Governance and budget enforcement verification
- Run budget enforcement unit tests:
  - `cargo test -p yarli-queue -- test_budget_exceeded --nocapture`
- Run governance explain tests:
  - `cargo test -p yarli-core -- budget_exceeded_task --nocapture`
- Run governance CLI surface tests:
  - `cargo test -p yarli-cli -- budget --nocapture`
- All must exit `0` with test success output.

6. Evidence integrity checks
- Evidence-of-record must be under tracked paths only: `evidence/<loop-id>/...`.
- `IMPLEMENTATION_PLAN.md` and `PROMPT.md` must not reference `.[a]gent/`.
- Reject machine-local temporary evidence references (for example `/tmp/`).

7. Scale-consistency: Postgres concurrency/replay invariant tests (Loop R7)
- Run Postgres-backed concurrency and replay tests:
  - `YARLI_TEST_DATABASE_URL=... YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-queue --test postgres_integration -- --nocapture`
- Require exit code `0` with test success output.
- Must prove: concurrent lease claims under Postgres produce no duplicate leases; replay from persisted state is deterministic.

8. Scale-consistency: capacity/budget stress proofs (Loop R7)
- Run budget enforcement stress tests:
  - `cargo test -p yarli-queue -- test_budget_exceeded_fails_task_without_retry --nocapture`
  - `cargo test -p yarli-queue -- test_run_token_budget_exceeded_across_tasks --nocapture`
- Require exit code `0` with test success output for each.
- Must prove: budget governance under parallel workload transitions to explicit failure, no silent continuation.

9. Scale-consistency: verification matrix coverage (Loop R7)
- Run matrix keyword check:
  - `rg -n "single-active-lease|duplicate terminal|restart|replay|budget" docs/CONSISTENCY_CONTRACT.md`
- Require all matrix invariant keywords present.
- See `docs/CONSISTENCY_CONTRACT.md` Section 3 for the full verification matrix.

## Decision Rule

- Mark `PASS` only if all nine required checks are satisfied with tracked evidence.
- Otherwise mark `UNVERIFIED`, add a blocker note, and stop.
