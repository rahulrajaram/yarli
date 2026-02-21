# CARD-R10-02 Evidence: Emit merge apply lifecycle telemetry and audit events

- Contract: Emit merge.apply.started, merge.apply.conflict, merge.apply.finalized and merge.repair.* events with structured metadata, and surface them in audit output.
- Completion status: DONE

## Verification command

```bash
cargo test -p yarli-observability
```

### Result
- **Pass** (exit code `0`)
- Completed in approximately `11.27s`
- `44 tests` executed, `0 failed`
- Observed test output confirms all `yarli-observability` unit tests passed.

