# CI/CD Integration Examples (YARLI API)

This document provides copy-ready examples for running YARLI from CI systems using the HTTP API.
It includes GitHub Actions, GitLab CI, and Jenkins examples plus wait-for-completion scripts.

## Prerequisites

- `YARLI_API_BASE_URL` points to a live YARLI API (for example `http://127.0.0.1:3000`).
- `jq` and `curl` available in the runner image.
- Optional API token can be sent in `Authorization: Bearer <token>` header.

```bash
export YARLI_API_BASE_URL="http://127.0.0.1:3000"
export YARLI_API_TOKEN="${YARLI_API_TOKEN:-}"
```

Create a helper header for authenticated calls:

```bash
if [ -n "${YARLI_API_TOKEN}" ]; then
  export YARLI_AUTH_HEADER="Authorization: Bearer ${YARLI_API_TOKEN}"
else
  export YARLI_AUTH_HEADER=""
fi
```

## Shared API call examples

### Start a YARLI run

```bash
run_payload="$(mktemp)"
curl -sS -X POST \
  -H "content-type: application/json" \
  ${YARLI_AUTH_HEADER:+-H "$YARLI_AUTH_HEADER"} \
  -d '{"objective":"ci-run","reason":"CI pipeline trigger"}' \
  "${YARLI_API_BASE_URL}/v1/runs" | tee "$run_payload"

run_id="$(jq -r '.run_id' "$run_payload")"
printf 'run_id=%s\n' "$run_id"
```

### Pause / resume / cancel

```bash
curl -sS -X POST \
  -H "content-type: application/json" \
  ${YARLI_AUTH_HEADER:+-H "$YARLI_AUTH_HEADER"} \
  -d '{"reason":"CI maintenance window"}' \
  "${YARLI_API_BASE_URL}/v1/runs/${run_id}/pause"

curl -sS -X POST \
  -H "content-type: application/json" \
  ${YARLI_AUTH_HEADER:+-H "$YARLI_AUTH_HEADER"} \
  -d '{"reason":"CI resume"}' \
  "${YARLI_API_BASE_URL}/v1/runs/${run_id}/resume"

curl -sS -X POST \
  -H "content-type: application/json" \
  ${YARLI_AUTH_HEADER:+-H "$YARLI_AUTH_HEADER"} \
  -d '{"reason":"CI timeout"}' \
  "${YARLI_API_BASE_URL}/v1/runs/${run_id}/cancel"
```

### Poll run status

```bash
curl -sS \
  ${YARLI_AUTH_HEADER:+-H "$YARLI_AUTH_HEADER"} \
  "${YARLI_API_BASE_URL}/v1/runs/${run_id}/status"
```

The status response contains `state` values such as `RunOpen`, `RunActive`, `RunVerifying`,
`RunBlocked`, `RunCompleted`, `RunFailed`, and `RunCancelled`.

## Example script: wait-for-completion

Use this script to block a pipeline until terminal state:

- `RunCompleted`
- `RunFailed`
- `RunCancelled`

Save as `docs/ci/wait-for-completion.sh`.

```bash
#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?run_id required}"
base_url="${YARLI_API_BASE_URL:-http://127.0.0.1:3000}"
auth_header="${YARLI_AUTH_HEADER:-}"
poll_delay="${YARLI_POLL_SECONDS:-5}"
poll_attempts="${YARLI_POLL_ATTEMPTS:-120}"
attempt=0

while ((attempt < poll_attempts)); do
  payload="$(curl -sS \
    ${auth_header:+-H "$auth_header"} \
    "${base_url}/v1/runs/${run_id}/status")"

  state="$(jq -r '.state' <<<"${payload}")"
  updated_at="$(jq -r '.updated_at // "unknown"' <<<"${payload}")"
  echo "run ${run_id} state=${state} at=${updated_at}"

  case "${state}" in
    RunCompleted | RunFailed | RunCancelled)
      echo "run reached terminal state: ${state}"
      exit 0
      ;;
    "")
      echo "empty or invalid status response"
      exit 1
      ;;
  esac

  attempt=$((attempt + 1))
  sleep "${poll_delay}"
done

echo "timed out waiting for terminal state after ${poll_attempts} attempts"
exit 2
```

## GitHub Actions workflow sample

```yaml
name: YARLI run in CI
on:
  workflow_dispatch:

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: dtolnay/rust-toolchain@stable

      - name: Start YARLI API
        run: |
          cargo build -p yarli-api --quiet
          nohup cargo run -p yarli-api -- --listen-addr 127.0.0.1:3000 > yarli-api.log 2>&1 &
          for i in $(seq 1 30); do
            if curl -fsS http://127.0.0.1:3000/health >/dev/null; then
              break
            fi
            sleep 1
          done
        env:
          RUST_LOG: warn

      - name: Trigger run and wait
        env:
          YARLI_API_BASE_URL: http://127.0.0.1:3000
        run: |
          run_payload="$(mktemp)"
          run_id="$(curl -sS -X POST \
            -H 'content-type: application/json' \
            -d '{"objective":"ci-github-actions"}' \
            "${YARLI_API_BASE_URL}/v1/runs" \
            | tee "$run_payload" | jq -r '.run_id')"
          bash docs/ci/wait-for-completion.sh "$run_id"
```

## GitLab CI sample

```yaml
stages:
  - trigger

trigger_yarli:
  image: rust:1.75
  stage: trigger
  script:
    - cargo build -p yarli-api --quiet
    - nohup cargo run -p yarli-api -- --listen-addr 127.0.0.1:3000 > yarli-api.log 2>&1 &
    - |
      for i in $(seq 1 30); do
        curl -fsS http://127.0.0.1:3000/health >/dev/null && break
        sleep 1
      done
    - export YARLI_API_BASE_URL=http://127.0.0.1:3000
    - |
      run_id="$(curl -sS -X POST \
        -H 'content-type: application/json' \
        -d '{"objective":"ci-gitlab"}' \
        "${YARLI_API_BASE_URL}/v1/runs" \
        | jq -r '.run_id')"
    - bash docs/ci/wait-for-completion.sh "$run_id"
```

## Jenkins pipeline sample

```groovy
pipeline {
  agent any
  stages {
    stage('Trigger YARLI') {
      steps {
        sh """
          cargo build -p yarli-api --quiet
          nohup cargo run -p yarli-api -- --listen-addr 127.0.0.1:3000 > yarli-api.log 2>&1 &
          for i in \$(seq 1 30); do
            curl -fsS http://127.0.0.1:3000/health >/dev/null && break || sleep 1
          done
          run_id=\$(curl -sS -X POST -H 'content-type: application/json' \
            -d '{\\\"objective\\\":\\\"ci-jenkins\\\"}' \
            http://127.0.0.1:3000/v1/runs | jq -r '.run_id')
          YARLI_API_BASE_URL=http://127.0.0.1:3000 \
          bash docs/ci/wait-for-completion.sh \"\\${run_id}\"
        """
      }
    }
  }
}
```

## Monitoring and troubleshooting

- Poll `/v1/runs?state=active&limit=50` to observe active work.
- Poll `/v1/audit?run_id=<uuid>` for event-level evidence.
- Poll via websocket endpoint `/v1/events/ws` or consume webhooks via `/v1/webhooks` for async visibility.
