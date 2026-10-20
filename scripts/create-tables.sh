#!/usr/bin/env bash
# Apply clickhouse/schema/*.sql in order. Idempotent (CREATE ... IF NOT
# EXISTS), so safe to rerun. Changing an existing table is a migration
# (ALTER), not an edit of these files.
set -euo pipefail
cd "$(dirname "$0")/.."
for f in clickhouse/schema/*.sql; do
  echo "applying $f"
  docker exec -i drishti-clickhouse clickhouse-client --user drishti --password drishti_dev --multiquery < "$f"
done
docker exec drishti-clickhouse clickhouse-client --user drishti --password drishti_dev \
  --query "SELECT name, engine, partition_key, sorting_key FROM system.tables WHERE database = 'drishti' AND name NOT LIKE 'lab_%' FORMAT PrettyCompact"
