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
