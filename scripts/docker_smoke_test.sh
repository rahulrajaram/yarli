#!/usr/bin/env bash
set -euo pipefail

DEFAULT_REPOSITORY="${YARLI_IMAGE_REPOSITORY:-ghcr.io/${GITHUB_REPOSITORY:-local/yarli}/yarli}"
DEFAULT_TAG="${YARLI_IMAGE_VERSION_TAG:-${YARLI_VERSION:-r14}-$(date -u +%Y%m%d)}"

if [ "$#" -ge 1 ]; then
  IMAGE="$1"
else
  IMAGE="${DEFAULT_REPOSITORY}:${DEFAULT_TAG}"
fi

docker run --rm "$IMAGE" --version

docker run --rm --entrypoint /usr/local/bin/yarli-healthcheck "$IMAGE"
