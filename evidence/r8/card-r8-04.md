# CARD-R8-04 Verification Evidence

## Command Results

```text
Command: bash scripts/verify_acceptance_rubric.sh --help
Exit Code: 0
Key Output:
- Usage: bash scripts/verify_acceptance_rubric.sh <loop-id>
- Runs the acceptance rubric checks and writes tracked evidence to:
- evidence/<loop-id>/
- Exit behavior:
-  0 => PASS
```

```text
Command: YARLI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres YARLI_REQUIRE_POSTGRES_TESTS=1 bash scripts/verify_acceptance_rubric.sh r8
Exit Code: 1
Key Output:
- UNVERIFIED
- Result: `UNVERIFIED`
- test run_start_and_status_roundtrip_against_postgres ... FAILED
- test merge_request_and_status_roundtrip_against_postgres ... ok
- test run_and_task_status_reflect_writes_from_postgres ... ok
- test health_endpoint_returns_ok ... ok
```

```text
Command: rg -n "package|smoke|api|UNVERIFIED" scripts/verify_acceptance_rubric.sh docs/ACCEPTANCE_RUBRIC.md docs/OPERATIONS.md
Exit Code: 0
Key Output:
- scripts/verify_acceptance_rubric.sh:14:  1 => UNVERIFIED
- scripts/verify_acceptance_rubric.sh:171:    package_build_log="${evidence_dir}/15-packaging-build.log"
- docs/ACCEPTANCE_RUBRIC.md:93:11. Loop-8 packaging/deployment smoke checks
- docs/OPERATIONS.md:81:cargo test -p yarli-api -- --nocapture
```

```text
Command: cargo fmt --all -- --check
Exit Code: 0
Key Output:
- (no output)
- (no output)
- (no output)
```

```text
Command: cargo clippy --workspace --all-targets -- -D warnings
Exit Code: 101
Key Output:
- error: very complex type used. Consider factoring parts into `type` definitions
-    --> crates/yarli-core/src/shutdown.rs:485:33
- error: could not compile `yarli-core` (lib) due to 1 previous error
```

```text
Command: cargo test --workspace
Exit Code: 101
Key Output:
- test tests::plan_driven_sequence_uses_tranches_file_near_resolved_plan_not_cwd ... FAILED
- test tests::structured_fallback_to_markdown ... FAILED
- test result: FAILED. 197 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out
```

## Summary

- `scripts/verify_acceptance_rubric.sh` now includes Loop-8 packaging/deployment smoke gates (release build, API smoke, API deploy smoke, and CLI roundtrip verification).
- `docs/ACCEPTANCE_RUBRIC.md` now includes Loop-8 packaging/deployment smoke verification criteria.
- `docs/OPERATIONS.md` now includes a deterministic loop-8 packaging smoke workflow.
- `evidence/r8/README.md` now captures full loop-8 verifier output.
- Blocker for `CARD-R8-04`: strict loop-8 run remains `UNVERIFIED` because `run_start_and_status_roundtrip_against_postgres` fails in strict CLI integration mode requiring `worktree_root` for parallel execution.
