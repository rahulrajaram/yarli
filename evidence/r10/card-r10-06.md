# CARD-R10-06 Evidence: Dispatch auto-repair task through configured CLI backend

- Contract: auto-repair mode must create and schedule repair tasks using the same CLI backend adapter and preserve completed tranche work across requests.
- Implementation changed in:
  - `crates/yarli-sw4rm/src/orchestrator.rs`
  - `crates/yarli-sw4rm/src/messages.rs`
  - `crates/yarli-sw4rm/src/agent.rs`
  - `crates/yarli-cli/src/commands.rs`

## Verification command

```bash
cargo test -p yarli-sw4rm
```

### Result
- **PASS** (exit code `0`)
- 47 tests passed; no failures.
