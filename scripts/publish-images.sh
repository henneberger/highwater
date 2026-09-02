#!/usr/bin/env bash
set -euo pipefail

version="${1:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: scripts/publish-images.sh VERSION" >&2
  exit 2
fi

package_version="$(awk -F'"' '/^version = "/ {print $2; exit}' pyproject.toml)"
if [[ "$version" != "$package_version" ]]; then
  echo "image version $version does not match package version $package_version" >&2
  exit 2
fi

: "${GHCR_TOKEN:?set GHCR_TOKEN to a GitHub token with write:packages}"
registry_user="${GHCR_USER:-henneberger}"
registry_root="${HIGHWATER_REGISTRY:-ghcr.io/henneberger}"
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io --username "$registry_user" --password-stdin >/dev/null

builder="highwater-release"
if ! docker buildx inspect "$builder" >/dev/null 2>&1; then
  docker buildx create --name "$builder" --driver docker-container >/dev/null
fi
docker buildx use "$builder"
docker buildx inspect --bootstrap >/dev/null

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --file Dockerfile \
  --tag "$registry_root/highwater-server:$version" \
  --provenance mode=max \
  --sbom true \
  --push .

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --file Dockerfile.worker \
  --tag "$registry_root/highwater-worker:$version" \
  --provenance mode=max \
  --sbom true \
  --push .

docker buildx imagetools inspect "$registry_root/highwater-server:$version"
docker buildx imagetools inspect "$registry_root/highwater-worker:$version"
