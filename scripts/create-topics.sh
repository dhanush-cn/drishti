#!/usr/bin/env bash
# Creates Drishti's Kafka topics. Idempotent (--if-not-exists).
#
# Partitions = 6: the unit of consumer parallelism in weeks 2-3. Increasing
# partitions later is possible but changes key->partition mapping (and so
# per-service ordering across the change), so pick deliberately.
# Replication factor 1: single broker (known limitation, see PROGRESS.md).
# Retention inherits the broker's 24h.
set -euo pipefail

PARTITIONS="${PARTITIONS:-6}"
BOOTSTRAP="kafka:29092"   # INTERNAL listener, resolvable inside the container

for topic in otlp.logs otlp.metrics; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor 1
done

# The detector's outputs (docs/detector.md).
#   anomalies: keyed by service, so one service's events stay in order; 6
#     partitions like the inputs. 7 days: the Week 5 agent and the evals read
#     incidents back.
#   live.stats: one small snapshot per detector per second; only the last
#     minutes matter (the dashboard rewinds 15). 1 partition keeps them in
#     order; 1 hour is plenty.
create() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 --config retention.ms="$3"
}
create anomalies "$PARTITIONS" 604800000
create live.stats 1 3600000

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic otlp.logs
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic otlp.metrics
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic anomalies
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic live.stats
