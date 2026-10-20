#!/usr/bin/env bash
# storage-check.sh <run_id> [records_per_request=512]
# Loss/dup per sent request and path latency for one load generator run
# that went through the storage consumer (scripts/storage-check.sql).
set -euo pipefail
cd "$(dirname "$0")/.."
run=${1:?usage: $0 <run_id> [records_per_request]}
per=${2:-512}
docker exec -i drishti-clickhouse clickhouse-client --user drishti --password drishti_dev \
  --param_run="$run" --param_per="$per" --multiquery < scripts/storage-check.sql
