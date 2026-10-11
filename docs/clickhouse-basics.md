# ClickHouse basics: hands-on lab (Week 1, prep for Week 2)

Goal: before designing the Week 2 tables, see with your own queries how MergeTree
stores data, why the sort key decides query cost, what partitions are (and aren't)
for, and why the storage consumer must batch and be idempotent. Every exercise
ends with something to **notice** and a question to be able to **answer** in an
interview. Run everything against the Drishti container; nothing here touches
the receiver or Kafka.

```bash
cd ~/drishti && docker compose up -d clickhouse
alias chq='docker exec -i drishti-clickhouse clickhouse-client --user drishti --password drishti_dev --database drishti'
chq --query "SELECT version()"
```

Paste multi-line SQL with `chq --multiquery <<'SQL' ... SQL`. All tables are
prefixed `lab_` and dropped at the end.

---

## 1. A MergeTree table, and what an insert creates

```sql
CREATE TABLE lab_logs
(
    ts       DateTime64(3),
    service  LowCardinality(String),
    severity LowCardinality(String),
    body     String
)
ENGINE = MergeTree
ORDER BY (service, ts);

INSERT INTO lab_logs
SELECT
    now64(3) - toIntervalMillisecond(number * 7),
    concat('svc-', toString(number % 20)),
    ['DEBUG','INFO','INFO','INFO','WARN','ERROR'][1 + number % 6],
    concat('GET /api/cart completed status=200 duration_ms=', toString(number % 2500))
FROM numbers(5000000);

SELECT count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts WHERE table = 'lab_logs' AND active;
```

**Notice:** one INSERT made one or a few *parts*: immutable, sorted directories
on disk. Background merges combine parts over time; nothing is updated in place.

**Answer:** why is ClickHouse append-only, and what does that buy for log data?

## 2. The sort key is the index

```sql
-- Filter on the sort key's prefix:
SELECT count() FROM lab_logs WHERE service = 'svc-3';
-- Filter on a column that is not in the sort key:
SELECT count() FROM lab_logs WHERE severity = 'ERROR';

SYSTEM FLUSH LOGS;
SELECT query, read_rows, formatReadableSize(read_bytes) AS read, query_duration_ms
FROM system.query_log
WHERE type = 'QueryFinish' AND query LIKE 'SELECT count() FROM lab_logs WHERE%'
ORDER BY event_time DESC LIMIT 2;

EXPLAIN indexes = 1 SELECT count() FROM lab_logs WHERE service = 'svc-3';
```

**Notice:** the `service` query reads roughly 1/20th of the rows. The sparse
primary index holds one mark per *granule* (8192 rows by default), so whole
granules that cannot match are skipped. The `severity` query reads everything.

**Answer:** why is the primary index *sparse*, and what goes first in a sort key:
the column you filter on most, or the one with the most distinct values?

## 3. Sort-key order: same data, different key

```sql
CREATE TABLE lab_logs_by_ts AS lab_logs ENGINE = MergeTree ORDER BY (ts);
INSERT INTO lab_logs_by_ts SELECT * FROM lab_logs;

SELECT count() FROM lab_logs_by_ts WHERE service = 'svc-3';
SELECT table, formatReadableSize(sum(data_compressed_bytes)) AS compressed
FROM system.columns WHERE table LIKE 'lab_logs%' GROUP BY table;
```
Re-run the `system.query_log` query from step 2 for this table.

**Notice:** two things change with key order: rows read for a service filter,
and how well columns compress. Sorted data puts similar values next to each
other, so `(service, ts)` usually compresses better than `(ts)`.

**Answer:** Drishti's dashboards ask "service X over the last hour". Which key,
and what would you lose for "all services, last 5 minutes"?

## 4. Partitions: data management, not speed

```sql
CREATE TABLE lab_logs_p AS lab_logs
ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (service, ts);

INSERT INTO lab_logs_p SELECT * FROM lab_logs;
INSERT INTO lab_logs_p SELECT ts - toIntervalDay(3), service, severity, body FROM lab_logs LIMIT 100000;

SELECT partition, count() AS parts, sum(rows) AS rows
FROM system.parts WHERE table = 'lab_logs_p' AND active GROUP BY partition ORDER BY partition;

ALTER TABLE lab_logs_p DROP PARTITION tuple(toDate(now() - INTERVAL 3 DAY));
```

**Notice:** dropping a day is a metadata operation, effectively instant. Parts
never merge across partitions, so every insert spanning many partitions creates
a part per partition. That is why a too-fine key (per hour × per service) leads
to "too many parts".

**Answer:** what is a partition key for, and why is it usually coarse (a day),
while the sort key carries the fine-grained filtering?

## 5. Column types and codecs

```sql
CREATE TABLE lab_codecs
(
    ts      DateTime64(3) CODEC(Delta, ZSTD(1)),
    service LowCardinality(String),
    plain   String,
    body    String CODEC(ZSTD(3))
)
ENGINE = MergeTree ORDER BY (service, ts);

INSERT INTO lab_codecs SELECT ts, service, service, body FROM lab_logs;

SELECT name, type, compression_codec,
       formatReadableSize(data_compressed_bytes) AS compressed,
       formatReadableSize(data_uncompressed_bytes) AS raw,
       round(data_uncompressed_bytes / data_compressed_bytes, 1) AS ratio
FROM system.columns WHERE table = 'lab_codecs';
```

**Notice:** `service` (LowCardinality) vs `plain` (String, same values):
dictionary encoding is far smaller. `Delta` on a monotonically increasing
timestamp stores small differences, which ZSTD then compresses well.

**Answer:** which OTLP fields are LowCardinality candidates
(service.name, severity_text, metric name) and which are not (trace_id, body)?

## 6. TTL, and why the load generator's stale timestamps matter

```sql
CREATE TABLE lab_ttl AS lab_logs
ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (service, ts)
TTL toDateTime(ts) + INTERVAL 1 DAY;

INSERT INTO lab_ttl SELECT * FROM lab_logs LIMIT 1000;                           -- fresh
INSERT INTO lab_ttl SELECT ts - toIntervalDay(5), service, severity, body FROM lab_logs LIMIT 1000; -- 5 days old

OPTIMIZE TABLE lab_ttl FINAL;
SELECT count() FROM lab_ttl;
```

**Notice:** the old rows disappear on merge. This is exactly the
`docs/loadgen.md` §12 trigger: pooled payloads carry their generation-time
timestamps, so once storage is in the benchmark path, a long run's records can
land in one old partition or be expired on arrival. That's why Week 2 needs the
per-send restamping before storage numbers mean anything.

**Answer:** TTL is applied during merges, not on insert. What does that imply
for disk usage right after a burst of expired data?

## 7. Why the storage consumer must batch

```bash
# 500 single-row inserts
for i in $(seq 1 500); do
  chq --query "INSERT INTO lab_logs VALUES (now64(3), 'svc-x', 'INFO', 'tiny insert $i')"
done
chq --query "SELECT count() FROM system.parts WHERE table='lab_logs' AND active"
```
```sql
-- vs one insert of the same 500 rows
INSERT INTO lab_logs SELECT now64(3), 'svc-y', 'INFO', concat('batched ', toString(number)) FROM numbers(500);
```

**Notice:** every INSERT creates at least one part, so 500 tiny inserts create
hundreds of parts for merges to clean up. ClickHouse refuses inserts past a
threshold ("Too many parts"). Sort and compression cost is per part too.

**Answer:** this is the core Week 2 trade-off: large batches keep ClickHouse
healthy but delay queryability. How do Drishti's batch size and flush interval
relate to the "event → queryable p99 < 5s" target, and why does the live
dashboard read from Redis instead? (`async_insert` is the server-side
alternative; know that it exists and what it trades away.)

## 8. Idempotent writes for at-least-once delivery

The receiver is at-least-once (`docs/receiver.md` §4): a retried export can put
the same records into Kafka twice, so the consumer can see duplicates.

```sql
-- (a) Duplicate rows collapse on merge
CREATE TABLE lab_dedup
(
    run_id String, seq UInt64, idx UInt32, ts DateTime64(3), body String
)
ENGINE = ReplacingMergeTree
ORDER BY (run_id, seq, idx);

INSERT INTO lab_dedup SELECT 'r1', number, 0, now64(3), 'x' FROM numbers(1000);
INSERT INTO lab_dedup SELECT 'r1', number, 0, now64(3), 'x' FROM numbers(500);   -- a "retry"

SELECT count() FROM lab_dedup;          -- 1500 until merged
SELECT count() FROM lab_dedup FINAL;    -- 1000: duplicates collapsed at read time
OPTIMIZE TABLE lab_dedup FINAL;
SELECT count() FROM lab_dedup;          -- 1000

-- (b) Block-level dedup for a retried batch (non-replicated tables need the window)
CREATE TABLE lab_blockdedup (n UInt64) ENGINE = MergeTree ORDER BY n
SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO lab_blockdedup SETTINGS insert_deduplication_token = 'batch-42' SELECT number FROM numbers(10);
INSERT INTO lab_blockdedup SETTINGS insert_deduplication_token = 'batch-42' SELECT number FROM numbers(10);
SELECT count() FROM lab_blockdedup;     -- 10
```

**Notice:** two mechanisms. ReplacingMergeTree deduplicates *eventually* (on
merge, or at read time with `FINAL`, which costs CPU), keyed by the sort key.
An insert deduplication token rejects a *whole retried batch*, as long as the
consumer re-sends exactly the same batch with the same token (for example,
derived from Kafka topic/partition/offset range).

**Answer:** which fits a Kafka consumer that may re-deliver after a crash? Note
that `(run_id, seq, resource_idx, record_idx)` is the `docs/loadgen.md` §12
identity, which exists so this zero-dup check can be made end to end.

## Clean up

```sql
DROP TABLE IF EXISTS lab_logs; DROP TABLE IF EXISTS lab_logs_by_ts; DROP TABLE IF EXISTS lab_logs_p;
DROP TABLE IF EXISTS lab_codecs; DROP TABLE IF EXISTS lab_ttl; DROP TABLE IF EXISTS lab_dedup;
DROP TABLE IF EXISTS lab_blockdedup;
```

## What to carry into the Week 2 table design

Write one line per decision, with the measured numbers from this lab:
sort key, partition key, LowCardinality columns, codecs, TTL, batch size and
flush interval for the consumer, and the dedup mechanism.
