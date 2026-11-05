-- drishti.logs_1m: log lines per minute, service and severity class, kept up
-- to date by a materialized view on every insert into drishti.logs.
-- Design record: docs/detector.md ("Pre-aggregated views").
--
-- Why: "how many lines / errors did service X log per minute over the last
-- hour (or week)" is what the dashboard's history and the Week 5 agent ask
-- most. On drishti.logs that reads every row of the range; here it reads one
-- row per (service, severity, minute).
--
-- Exactly-once, like drishti.logs: the storage consumer retries a batch with
-- the same insert_deduplication_token, and drishti.logs drops the copy. The
-- view's insert is dropped with it only if (1) this table has its own
-- deduplication window and (2) deduplicate_blocks_in_dependent_materialized_views
-- is on (the consumer sends it with every insert). Without (1) a retried
-- batch is counted twice here while drishti.logs holds it once (checked on
-- ClickHouse 26.9, scripts/mv-dedup-check.sh).
--
-- Covers inserts from the view's creation on: rows already in drishti.logs
-- are not backfilled.
CREATE TABLE IF NOT EXISTS drishti.logs_1m
(
    -- Event time, as drishti.logs is ordered (ts), to the minute.
    minute      DateTime CODEC(Delta, ZSTD(1)),
    service     LowCardinality(String),
    -- error | warn | info, the detector's classes (severity_number, else
    -- severity_text): an ERROR template and an INFO one are different
    -- questions.
    severity    LowCardinality(String),
    lines       SimpleAggregateFunction(sum, UInt64),
    body_bytes  SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
-- Monthly: the table is small (rows = services x 3 x minutes), and by event
-- time a sender with a wrong clock can only reach a bounded set of months.
PARTITION BY toYYYYMM(minute)
ORDER BY (service, severity, minute)
-- Aggregates outlive the 7 days of raw rows.
TTL minute + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS drishti.logs_1m_mv TO drishti.logs_1m AS
SELECT
    toStartOfMinute(ts) AS minute,
    service,
    multiIf(
        severity_number >= 17, 'error',
        severity_number >= 13, 'warn',
        severity_number > 0, 'info',
        lower(severity_text) IN ('error', 'err', 'fatal', 'critical', 'crit', 'alert', 'emerg', 'emergency'), 'error',
        lower(severity_text) IN ('warn', 'warning'), 'warn',
        'info') AS severity,
    count() AS lines,
    sum(length(body)) AS body_bytes
FROM drishti.logs
GROUP BY minute, service, severity;

-- Reading it: parts merge in the background, so always aggregate, e.g.
--   SELECT minute, sum(lines) FROM drishti.logs_1m
--   WHERE service = 'cart' AND severity = 'error' AND minute >= now() - INTERVAL 1 HOUR
--   GROUP BY minute ORDER BY minute;
