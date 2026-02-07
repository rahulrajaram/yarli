# CARD-R5-02 Verification Evidence

```text
Command: bash scripts/verify_acceptance_rubric.sh --help
Exit Code: 0
Key Output:
- Usage: bash scripts/verify_acceptance_rubric.sh <loop-id>
- Runs the acceptance rubric checks and writes tracked evidence to:
- Exit behavior: 0 => PASS, 1 => UNVERIFIED, 2 => invalid usage
```

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 bash scripts/verify_acceptance_rubric.sh r5
Exit Code: 0
Key Output:
- PASS
- evidence/r5/README.md records all rubric commands with exit codes and key output.
- Decision block in evidence/r5/README.md reports Result: `PASS`, Failure count: `0`.
```
