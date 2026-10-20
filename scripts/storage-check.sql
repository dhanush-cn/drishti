-- storage-check.sql: end-to-end check for one load generator run.
-- Usage: scripts/storage-check.sh <run_id> <records_per_request>
-- {run:String} and {per:UInt32} are query parameters.

-- 1. Loss / duplicates per sent request (docs/loadgen.md §12). Each
--    (run_id, seq) must hold exactly `per` records: fewer = loss, more = dup.
SELECT
    sum(n)                                    AS rows,
    uniqExact(seq)                            AS seqs,
    countIf(n < {per:UInt32})                 AS seqs_short,
    countIf(n > {per:UInt32})                 AS seqs_long,
    sumIf(n - {per:UInt32}, n > {per:UInt32}) AS extra_rows
FROM
(
    SELECT resource_attributes['drishti.loadgen.seq'] AS seq, count() AS n
    FROM drishti.logs
    WHERE resource_attributes['drishti.loadgen.run_id'] = {run:String}
    GROUP BY seq
)
FORMAT Vertical;

-- 2. Latency along the path, ms (p50 / p99 / max):
--    receive  = received_at - ts   (generator send -> receiver produce)
--    store    = inserted_at - received_at (Kafka -> ClickHouse part)
--    queryable = inserted_at - ts  (end to end)
SELECT
    arrayMap(x -> round(x, 1), quantilesExact(0.5, 0.99)(dateDiff('microsecond', ts, received_at) / 1000))           AS receive_p50_p99,
    arrayMap(x -> round(x, 1), quantilesExact(0.5, 0.99)(dateDiff('microsecond', received_at, inserted_at) / 1000))  AS store_p50_p99,
    arrayMap(x -> round(x, 1), quantilesExact(0.5, 0.99)(dateDiff('microsecond', ts, inserted_at) / 1000))           AS queryable_p50_p99,
    round(max(dateDiff('microsecond', ts, inserted_at)) / 1000, 1)                                                   AS queryable_max
FROM drishti.logs
WHERE resource_attributes['drishti.loadgen.run_id'] = {run:String}
FORMAT Vertical;

-- 3. How it was written: inserts (distinct inserted_at per partition is a
--    proxy for insert blocks), rows per insert.
SELECT
    uniqExact(kafka_partition, inserted_at)                       AS inserts,
    round(count() / uniqExact(kafka_partition, inserted_at))      AS rows_per_insert
FROM drishti.logs
WHERE resource_attributes['drishti.loadgen.run_id'] = {run:String}
FORMAT Vertical;
