#!/usr/bin/env bash
# Creates the eval topics and database (docs/evals.md, Week 5 design E6 and
# change 1). Idempotent; --reset deletes both first, for loading another
# recording.
#
# Recordings are replayed with their original timestamps, so:
#   - Kafka: retention.ms=-1 and retention.bytes=-1. The broker default (24 h,
#     by record timestamp) would delete a week-old recording right after the
#     load. kafkadump load refuses topics without these settings.
#   - ClickHouse: drishti_eval is built from the same schema files with the
#     TTL clauses removed (cmd/evalschema). The 7-day TTL on received_at drops
#     already-expired rows on insert.
#
# Usage: scripts/create-eval.sh [--reset]
set -euo pipefail
cd "$(dirname "$0")/.."
BOOTSTRAP="kafka:29092"
EVAL_DB="${EVAL_DB:-drishti_eval}"
TOPICS=(eval.otlp.logs eval.otlp.metrics eval.anomalies)
kt() { docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" "$@"; }
ch() { docker exec -i drishti-clickhouse clickhouse-client --user drishti --password drishti_dev "$@"; }

if [[ "${1:-}" == "--reset" ]]; then
  for t in "${TOPICS[@]}"; do kt --delete --if-exists --topic "$t" || true; done
  # Topic deletion is asynchronous; wait until they're gone before recreating.
  for _ in $(seq 1 30); do
    left=$(kt --list | grep -cE '^eval\.' || true)
    [[ "$left" == "0" ]] && break
    sleep 1
  done
  ch --query "DROP DATABASE IF EXISTS ${EVAL_DB} SYNC"
fi

for t in "${TOPICS[@]}"; do
  kt --create --if-not-exists --topic "$t" --partitions 6 --replication-factor 1 \
    --config retention.ms=-1 --config retention.bytes=-1
done

go run ./cmd/evalschema -db "$EVAL_DB" clickhouse/schema/*.sql | ch --multiquery

# Check what the evals rely on, not just that the commands ran.
ch --query "SELECT name, sorting_key FROM system.tables WHERE database = '${EVAL_DB}' FORMAT PrettyCompact"
ttl=$(ch --query "SELECT count() FROM system.tables WHERE database = '${EVAL_DB}' AND positionCaseInsensitive(create_table_query, ' TTL ') > 0")
if [[ "$ttl" != "0" ]]; then echo "FAIL: ${ttl} eval table(s) still have a TTL" >&2; exit 1; fi
for t in "${TOPICS[@]}"; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
    --describe --entity-type topics --entity-name "$t" | tr ',' '\n' | grep -E 'retention\.(ms|bytes)=' | sed "s/^ */$t /"
done
