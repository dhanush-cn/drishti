-- drishti.anomalies: the detector's events (anomalies topic, JSON schema v1,
-- docs/detector.md section 5), one row per Kafka record. Design record:
-- docs/storage.md decision 11.
--
-- The agent's tools and the evals read alerts from here, bounded by time
-- ("what was open at T"), next to the logs and metrics they point at.
--
-- Delivery is at least once: the detector re-sends events after a restart
-- (its 15 min rewind) and repeats "updated" while an anomaly stays open.
-- Each Kafka record is stored once (the consumer's usual exactly-once), and
-- readers collapse repeats by (id, event); the IDs are deterministic.
CREATE TABLE IF NOT EXISTS drishti.anomalies
(
    id              String,
    event           LowCardinality(String),  -- opened | updated | resolved
    rule            LowCardinality(String),
    service         LowCardinality(String),
    series          Map(LowCardinality(String), String),
    opened_at       DateTime64(3),
    detected_at     DateTime64(3),
    evidence_at     DateTime64(3),
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    ev_window       LowCardinality(String),
    observed        Float64,
    expected        Float64,
    calls           Float64,
    baseline_ratio  Float64,
    log10_p         Float64,
    detail          String,
    template        String CODEC(ZSTD(1)),
    replayed        Bool,
    -- The whole event as produced, so a field added to schema v1 later is
    -- never lost before a column exists for it.
    raw             String CODEC(ZSTD(1)),
    received_at     DateTime64(3) CODEC(Delta, ZSTD(1)),
    inserted_at     DateTime64(3) DEFAULT now64(3) CODEC(Delta, ZSTD(1)),
    kafka_partition UInt16,
    kafka_offset    UInt64 CODEC(Delta, ZSTD(1)),

    PROJECTION ingest_progress
    (
        SELECT kafka_partition, max(kafka_offset) GROUP BY kafka_partition
    )
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (service, opened_at, id)
-- The topic keeps 7 days; so does the table.
TTL toDateTime(received_at) + INTERVAL 7 DAY
SETTINGS non_replicated_deduplication_window = 1000;
