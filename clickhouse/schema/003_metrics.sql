-- drishti.metrics: one row per OTLP metric data point, stored as received.
-- Design record: docs/storage.md, decision 10.
--
-- Cumulative sums and histograms stay cumulative here. Deltas are computed
-- at query time (internal/metricsql), the way Prometheus stores counters
-- and computes rate() when asked. Turning them into deltas on the way in
-- would make a row depend on the previous point of its series, i.e. on what
-- this consumer process happened to see before, and break the storage
-- consumer's rule that a record's rows depend on that record alone
-- (exactly-once by offsets, decision 1).
CREATE TABLE IF NOT EXISTS drishti.metrics
(
    -- Point time and, for cumulative points, the series' start time: a new
    -- start time is a counter reset (the producer restarted).
    ts                  DateTime64(9) CODEC(Delta, ZSTD(1)),
    start_ts            DateTime64(9) CODEC(Delta, ZSTD(1)),
    -- As drishti.logs: Kafka timestamp drives the partition key and TTL.
    received_at         DateTime64(3) CODEC(Delta, ZSTD(1)),
    inserted_at         DateTime64(3) DEFAULT now64(3) CODEC(Delta, ZSTD(1)),
    -- The receiver's owner key: service.name, else "container:<name>", else
    -- "host:<name>" (internal/otlpmetrics.Owner.Key, the detector's key).
    service             LowCardinality(String),
    -- Which replica: service.instance.id, else container.id, else
    -- container.name, else host.name. Part of the series identity, so two
    -- instances' counters are never read as one series (whose values would
    -- jump between them and look like resets).
    instance            LowCardinality(String),
    metric              LowCardinality(String),
    kind                Enum8('gauge' = 1, 'sum' = 2, 'histogram' = 3),
    -- false for sums that are levels (memory usage, limits): read as gauges.
    monotonic           Bool,
    -- OTLP's AggregationTemporality numbering.
    temporality         Enum8('unspecified' = 0, 'delta' = 1, 'cumulative' = 2),
    value               Float64 CODEC(Gorilla, ZSTD(1)),
    -- Explicit-bucket histograms; empty for gauges and sums.
    count               UInt64 CODEC(T64, ZSTD(1)),
    sum                 Float64 CODEC(Gorilla, ZSTD(1)),
    bounds              Array(Float64) CODEC(ZSTD(1)),
    buckets             Array(UInt64) CODEC(ZSTD(1)),
    -- Data point attributes, written sorted by key, so equal attribute sets
    -- are equal maps and hash the same.
    attributes          Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    resource_attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    series_id           UInt64 MATERIALIZED cityHash64(service, instance, metric, attributes),
    kafka_partition     UInt16,
    kafka_offset        UInt64 CODEC(Delta, ZSTD(1)),

    PROJECTION ingest_progress
    (
        SELECT kafka_partition, max(kafka_offset) GROUP BY kafka_partition
    )
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
-- A series' points sit together in ts order: query-time deltas are a window
-- function over (series_id ORDER BY ts), read sequentially.
ORDER BY (service, metric, series_id, ts)
TTL toDateTime(received_at) + INTERVAL 7 DAY
SETTINGS non_replicated_deduplication_window = 1000;
