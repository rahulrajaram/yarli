# R4V Evidence Record

This file seals Loop-4 verification evidence into tracked repository artifacts.

## Workspace test run

```text
Command: cargo test --workspace
Exit Code: 0
Key Output:
- Running tests/postgres_integration.rs (target/debug/deps/postgres_integration-13bc915f70d0c06d)
- test result: ok. 65 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.54s
- test result: ok. 28 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## Strict missing-env failure proof

```text
Command: unset YARLI_TEST_DATABASE_URL; YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-store --test postgres_integration -- --nocapture
Exit Code: 101
Key Output:
- postgres integration tests require YARLI_TEST_DATABASE_URL when YARLI_REQUIRE_POSTGRES_TESTS=1
- test append_and_query_roundtrip_against_postgres ... FAILED
- test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## Strict store integration success proof

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-store --test postgres_integration -- --nocapture
Exit Code: 0
Key Output:
- running 1 test
- test append_and_query_roundtrip_against_postgres ... ok
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.63s
```

## Strict queue integration success proof

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-queue --test postgres_integration -- --nocapture
Exit Code: 0
Key Output:
- running 1 test
- test enqueue_and_claim_lease_roundtrip_against_postgres ... ok
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.85s
```

## Strict cli integration success proof

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-cli --test postgres_integration -- --nocapture
Exit Code: 0
Key Output:
- running 1 test
- test merge_request_and_status_roundtrip_against_postgres ... ok
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.82s
```

## CARD-R4V-04 verification commands

```text
Command: rg -n "\\.[a]gent/" IMPLEMENTATION_PLAN.md PROMPT.md
Exit Code: 1
Key Output:
- (no output)
- (no output)
- (no output)
```

```text
Command: test -s evidence/r4v/README.md
Exit Code: 0
Key Output:
- (no output)
- (no output)
- (no output)
```

```text
Command: rg -n "Strict missing-env|workspace|store|queue|cli" evidence/r4v/README.md
Exit Code: 0
Key Output:
- 8:Command: cargo test --workspace
- 16:## Strict missing-env failure proof
- 49:## Strict cli integration success proof
```

## CARD-R4V-07 final re-verification (2026-02-07)

Logs for this run are tracked under `evidence/r4v/r4v07-*.log`.

```text
Command: cargo test --workspace
Exit Code: 0
Key Output:
- Running tests/postgres_integration.rs (target/debug/deps/postgres_integration-8d8caf1a58f97a42)
- test result: ok. 65 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.51s
- test result: ok. 28 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

```text
Command: unset YARLI_TEST_DATABASE_URL; YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-store --test postgres_integration -- --nocapture
Exit Code: 101
Key Output:
- postgres integration tests require YARLI_TEST_DATABASE_URL when YARLI_REQUIRE_POSTGRES_TESTS=1
- test append_and_query_roundtrip_against_postgres ... FAILED
- test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-store --test postgres_integration -- --nocapture
Exit Code: 0
Key Output:
- running 1 test
- test append_and_query_roundtrip_against_postgres ... ok
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.64s
```

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-queue --test postgres_integration -- --nocapture
Exit Code: 0
Key Output:
- running 1 test
- test enqueue_and_claim_lease_roundtrip_against_postgres ... ok
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.75s
```

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 cargo test -p yarli-cli --test postgres_integration -- --nocapture
Exit Code: 0
Key Output:
- running 1 test
- test merge_request_and_status_roundtrip_against_postgres ... ok
- test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.68s
```
