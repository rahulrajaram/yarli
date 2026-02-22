# Evidence for CARD-R14-01: Production Dockerfile and image build

## 2026-02-22 Command-level verification

### `cargo fmt --all -- --check`

```text
exit: 0
```

### `cargo clippy --workspace --all-targets -- -D warnings`

```text
exit: 0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.30s
```

### `cargo test --workspace`

```text
exit: 101
test resource_exhaustion_postgres_connection_pool_exhaustion_recovery ... ok
test resource_exhaustion_queue_no_workers_can_claim ... ok
test resource_exhaustion_disk_full_append_failure_is_recoverable ... FAILED
test resource_exhaustion_memory_pressure_budget_exceeded ... ok

failures:

---- resource_exhaustion_disk_full_append_failure_is_recoverable stdout ----

thread 'resource_exhaustion_disk_full_append_failure_is_recoverable' panicked at tests/integration/tests/resource_exhaustion.rs:225:5:
recoverable append failure should not block completion
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    resource_exhaustion_disk_full_append_failure_is_recoverable

test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

error: test failed, to rerun pass `-p yarli-integration-tests --test resource_exhaustion`
```

### `bash scripts/docker_build_image.sh`

```text
exit: 0
 - Building images:
 -   ghcr.io/local/yarli/yarli:r14-20260222
 -   ghcr.io/local/yarli/yarli:233eb20
#0 building with "default" instance using docker driver

#1 [internal] load build definition from Dockerfile
#1 transferring dockerfile: 1.05kB done
#1 DONE 0.0s

#2 resolve image config for docker-image://docker.io/docker/dockerfile:1
#2 DONE 0.1s

...
```

### `bash scripts/docker_smoke_test.sh ghcr.io/local/yarli/yarli:r14-20260222`

```text
exit: 0
yarli 0.1.0 (commit unknown, date unknown, build release-1771787548)
```

## Notes

- Added production container build/smoke scaffolding:
  - `Dockerfile`
  - `.dockerignore`
  - `docker/healthcheck.sh`
  - `scripts/docker_build_image.sh`
  - `scripts/docker_smoke_test.sh`
- `IMPLEMENTATION_PLAN.md` updated to mark `CARD-R14-01` as complete and reference this evidence file.
- `cargo test --workspace` currently fails on pre-existing `tests/integration/tests/resource_exhaustion.rs::resource_exhaustion_disk_full_append_failure_is_recoverable`.
