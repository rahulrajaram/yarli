# CARD-R14-05 Evidence

Date: 2026-02-22
Tranche: Kubernetes manifests and Helm chart
Workspace: `/home/rahul/Documents/worktree/yarli/run-019c86d70ebb/001-tranche-011-card-r14-05`

## Changes made
- Added Helm chart scaffolding and templates under `deploy/helm/yarli`:
  - `Chart.yaml`
  - `values.yaml`
  - `templates/_helpers.tpl`
  - `templates/configmap.yaml`
  - `templates/secret.yaml`
  - `templates/deployment.yaml`
  - `templates/service-api.yaml`
  - `templates/job-migrate.yaml`
- Added scheduler deployment configuration with probes, resource controls, and migration hook job.
- Added runtime config and secret injection wiring for runtime config and DB URL.
- Updated `IMPLEMENTATION_PLAN.md` `CARD-R14-05` section:
  - Primary files enumerated.
  - Status set to `DONE`.
  - Evidence link added: `evidence/r14/card-r14-05.md`.

## Verification commands
1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test --workspace`

## Verification evidence
- `cargo fmt --all -- --check`: pass
- `cargo clippy --workspace --all-targets -- -D warnings`: pass
- `cargo test --workspace`: fail (exit 101)
  - Failing test: `commands::tests::drive_scheduler_drains_without_claiming_new_tasks_during_pause`
  - Failure: `queued task should remain ready while paused` in `crates/yarli-cli/src/commands.rs:8290`
  - This appears to be outside this tranche’s Helm chart scope.
