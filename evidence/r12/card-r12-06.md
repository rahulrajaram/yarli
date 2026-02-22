# Evidence for CARD-R12-06: Operational runbook for telemetry

## Verification Output

Command: `cargo test --workspace`

```
   Compiling yarli-cli v0.1.0 (/home/rahul/Documents/worktree/yarli/run-019c8638-5015-76c1-93f7-d574b73695cd/001-tranche-002-card-r12-05/crates/yarli-cli)
...
test result: ok. 142 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.62s
```

## Implementation Notes

- Added "Telemetry and Observability" section to `docs/OPERATIONS.md`.
- Documented local OTLP collector setup with Jaeger example.
- Documented metric families and trace span conventions.
- Added alerting thresholds for stalled queue, high failure rate, slow DB, and resource saturation.
- Added troubleshooting guide for missing exports and cardinality.
