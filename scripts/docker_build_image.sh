#!/usr/bin/env bash
set -euo pipefail

IMAGE_REPOSITORY="${YARLI_IMAGE_REPOSITORY:-ghcr.io/${GITHUB_REPOSITORY:-local/yarli}/yarli}"
VERSION_TAG="${YARLI_IMAGE_VERSION_TAG:-${YARLI_VERSION:-r14}-$(date -u +%Y%m%d)}"
GIT_SHA="${GITHUB_SHA:-$(git rev-parse --short HEAD)}"
DOCKER_IMAGE="${IMAGE_REPOSITORY}:${VERSION_TAG}"
SHA_TAG="${IMAGE_REPOSITORY}:${GIT_SHA}"
LATEST_TAG="${IMAGE_REPOSITORY}:latest"

printf 'Building images:\n  %s\n  %s\n' "$DOCKER_IMAGE" "$SHA_TAG" | sed 's/^/ - /'

docker build \
  --file Dockerfile \
  --tag "$DOCKER_IMAGE" \
  --tag "$SHA_TAG" \
  --tag "$LATEST_TAG" \
  .

if [ "${YARLI_DOCKER_IMAGE_PUSH:-0}" = "1" ]; then
  docker push "$DOCKER_IMAGE"
  docker push "$SHA_TAG"
  docker push "$LATEST_TAG"
fi

printf 'Built image: %s\n' "$DOCKER_IMAGE"
printf 'Built image: %s\n' "$SHA_TAG"
printf 'Built image: %s\n' "$LATEST_TAG"
