-- drishti.logs: one row per OTLP log record. Design record: docs/storage.md.
-- Applied by scripts/create-tables.sh (idempotent: IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS drishti.logs
(
    -- Event time (record time_unix_nano; falls back to observed, then received).
    -- Nanoseconds because OTLP carries them; Delta+ZSTD makes the precision
    -- nearly free on sorted data (lab §5: 20.7x on DateTime64(3)).
    ts                  DateTime64(9) CODEC(Delta, ZSTD(1)),
    observed_ts         DateTime64(9) CODEC(Delta, ZSTD(1)),
    -- Kafka record timestamp = when the receiver produced it. Drives the
    -- partition key and TTL (not ts): a sender's clock or a stale payload
    -- cannot put rows in old partitions or expire them on arrival (lab §6).
    received_at         DateTime64(3) CODEC(Delta, ZSTD(1)),
    -- Set by the server once per insert block: event -> queryable latency is
    -- inserted_at - ts, measurable from SQL with no extra instrumentation.
    inserted_at         DateTime64(3) DEFAULT now64(3) CODEC(Delta, ZSTD(1)),
    service             LowCardinality(String),
    severity_number     UInt8,
    severity_text       LowCardinality(String),
    body                String CODEC(ZSTD(1)),
    trace_id            String CODEC(ZSTD(1)),
    span_id             String CODEC(ZSTD(1)),
    flags               UInt32,
    event_name          LowCardinality(String),
    scope_name          LowCardinality(String),
    -- Attributes as maps: every key kept, no schema per service. Typed
    -- columns for hot keys come later, from real query patterns.
    resource_attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    log_attributes      Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    -- Where the row came from in Kafka. This is what makes the consumer
    -- exactly-once: the offsets live in the same atomic part as the rows.
    kafka_partition     UInt16,
    kafka_offset        UInt64 CODEC(Delta, ZSTD(1)),

    -- Resume point per Kafka partition, pre-aggregated inside every part, so
    -- the consumer's startup query reads a few rows instead of the offset
    -- column of the whole table. Built atomically with its part.
    PROJECTION ingest_progress
    (
        SELECT kafka_partition, max(kafka_offset) GROUP BY kafka_partition
    )
)
ENGINE = MergeTree
-- Day partitions: data management (DROP PARTITION, TTL), not speed (lab §4).
PARTITION BY toDate(received_at)
-- "Service X over the last N minutes" is the dominant read (dashboards and
-- the agent's tools). Revisit with real OTLP compression numbers (lab §3).
ORDER BY (service, ts)
TTL toDateTime(received_at) + INTERVAL 7 DAY
-- Remember the last 1000 insert tokens: a retried batch is dropped.
SETTINGS non_replicated_deduplication_window = 1000;
