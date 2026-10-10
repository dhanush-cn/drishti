#!/usr/bin/env bash
# Creates the receiver's Kafka topics. Idempotent (--if-not-exists).
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

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic otlp.logs
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic otlp.metrics
