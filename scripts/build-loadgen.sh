#!/usr/bin/env bash
# Build the load generator image with provenance: the commit and whether the
# tree was dirty go into the binary, so every run.json records them.
set -euo pipefail
cd "$(dirname "$0")/.."
GIT_COMMIT=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain)" ]; then GIT_DIRTY=true; else GIT_DIRTY=false; fi
export GIT_COMMIT GIT_DIRTY
echo "building drishti-loadgen:dev at ${GIT_COMMIT:0:7} dirty=${GIT_DIRTY}"
docker compose --profile bench build loadgen
