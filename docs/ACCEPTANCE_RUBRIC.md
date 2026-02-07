# YARLI Acceptance Rubric (Loop R4V)

This rubric defines a binary outcome for Loop-4 evidence closure.

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

5. Evidence integrity checks
- Evidence-of-record must be under tracked paths only: `evidence/<loop-id>/...`.
- `IMPLEMENTATION_PLAN.md` and `PROMPT.md` must not reference `.[a]gent/`.
- Reject machine-local temporary evidence references (for example `/tmp/`).

## Decision Rule

- Mark `PASS` only if all five required checks are satisfied with tracked evidence.
- Otherwise mark `UNVERIFIED`, add a blocker note, and stop.
