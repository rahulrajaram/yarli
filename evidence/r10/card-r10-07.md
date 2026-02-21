# CARD-R10-07 Evidence: Re-run verification chain before final completion after repair

- Contract: after successful repair, verification commands are rerun and `RunCompleted` is only emitted when verification succeeds.
- Primary verifier: `cargo test -p yarli-queue`

## Verification command

```bash
cargo test -p yarli-queue
```

### Result
- **PASS** (exit code `0`)
- `90 tests passed; 0 failed` in `yarli-queue` unit suite.
- `3 tests passed; 0 failed` in `tests/postgres_integration.rs`.
