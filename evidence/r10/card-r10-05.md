# CARD-R10-05 Evidence: Build repair payload synthesis from merge conflict artifacts

- Contract: on merge conflict, capture workspace path, patch path, conflicted files, and repository status into the repair-task payload.
- Completion status: implemented in `crates/yarli-cli` command/projection paths and captured in this run.

## Verification command

```bash
cargo test -p yarli-git
```

### Result
- **PASS** (exit code `0`)
- `cargo test -p yarli-git` completed with all tests passing:
  - `140` unit tests executed in `crates/yarli-git`
  - `0` ignored
