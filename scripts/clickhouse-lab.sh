#!/usr/bin/env bash
# clickhouse-lab.sh: runs every exercise of docs/clickhouse-basics.md against
# drishti-clickhouse and saves the output, so you can read the results next to
# each exercise's "Notice" and "Answer" questions. Doing the questions is still
# your part: this only runs the SQL.
#
#   scripts/clickhouse-lab.sh        (output in ./clickhouse-lab-out/, or set OUT=dir)
set -uo pipefail
OUT=${OUT:-./clickhouse-lab-out}
mkdir -p "$OUT"; LOG=$OUT/lab.txt; : > "$LOG"
exec > >(tee -a "$LOG") 2>&1
CH=(docker exec -i drishti-clickhouse clickhouse-client --user drishti --password drishti_dev --database drishti --format PrettyCompactMonoBlock)
q()  { echo; echo "SQL> $1"; "${CH[@]}" --query "$1" || echo "!! ERROR (above)"; }
mq() { echo; echo "SQL (multi)>"; echo "$1" | sed 's/^/  /'; echo "$1" | "${CH[@]}" --multiquery || echo "!! ERROR (above)"; }
h()  { printf '\n\n==================== %s ====================\n' "$*"; }

cd ~/drishti && docker compose up -d clickhouse >/dev/null
for i in $(seq 1 30); do "${CH[@]}" --query "SELECT 1" >/dev/null 2>&1 && break; sleep 2; done
q "SELECT version()"
mq "DROP TABLE IF EXISTS lab_logs; DROP TABLE IF EXISTS lab_logs_by_ts; DROP TABLE IF EXISTS lab_logs_p; DROP TABLE IF EXISTS lab_codecs; DROP TABLE IF EXISTS lab_ttl; DROP TABLE IF EXISTS lab_dedup; DROP TABLE IF EXISTS lab_blockdedup;"

h "1. MergeTree table, and what an insert creates"
mq "CREATE TABLE lab_logs
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
FROM numbers(5000000);"
q "SELECT count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size FROM system.parts WHERE database = 'drishti' AND table = 'lab_logs' AND active"
sleep 20
echo "(after 20s of background merges:)"
q "SELECT count() AS parts, sum(rows) AS rows FROM system.parts WHERE database = 'drishti' AND table = 'lab_logs' AND active"

h "2. The sort key is the index"
q "SELECT count() FROM lab_logs WHERE service = 'svc-3'"
q "SELECT count() FROM lab_logs WHERE severity = 'ERROR'"
q "SYSTEM FLUSH LOGS"
q "SELECT query, read_rows, formatReadableSize(read_bytes) AS read, query_duration_ms FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE 'SELECT count() FROM lab_logs WHERE%' ORDER BY event_time DESC LIMIT 2"
q "EXPLAIN indexes = 1 SELECT count() FROM lab_logs WHERE service = 'svc-3'"

h "3. Sort-key order: same data, different key"
mq "CREATE TABLE lab_logs_by_ts AS lab_logs ENGINE = MergeTree ORDER BY (ts);
INSERT INTO lab_logs_by_ts SELECT * FROM lab_logs;"
q "SELECT count() FROM lab_logs_by_ts WHERE service = 'svc-3'"
q "SYSTEM FLUSH LOGS"
q "SELECT query, read_rows, query_duration_ms FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE 'SELECT count() FROM lab_logs_by_ts WHERE%' ORDER BY event_time DESC LIMIT 1"
q "SELECT table, formatReadableSize(sum(data_compressed_bytes)) AS compressed FROM system.columns WHERE database = 'drishti' AND table LIKE 'lab_logs%' GROUP BY table"

h "4. Partitions: data management, not speed"
mq "CREATE TABLE lab_logs_p AS lab_logs ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (service, ts);
INSERT INTO lab_logs_p SELECT * FROM lab_logs;
INSERT INTO lab_logs_p SELECT ts - toIntervalDay(3), service, severity, body FROM lab_logs LIMIT 100000;"
q "SELECT partition, count() AS parts, sum(rows) AS rows FROM system.parts WHERE database = 'drishti' AND table = 'lab_logs_p' AND active GROUP BY partition ORDER BY partition"
q "ALTER TABLE lab_logs_p DROP PARTITION tuple(toDate(now() - INTERVAL 3 DAY))"
q "SELECT partition, sum(rows) AS rows FROM system.parts WHERE database = 'drishti' AND table = 'lab_logs_p' AND active GROUP BY partition ORDER BY partition"

h "5. Column types and codecs"
mq "CREATE TABLE lab_codecs
(
    ts      DateTime64(3) CODEC(Delta, ZSTD(1)),
    service LowCardinality(String),
    plain   String,
    body    String CODEC(ZSTD(3))
)
ENGINE = MergeTree ORDER BY (service, ts);
INSERT INTO lab_codecs SELECT ts, service, service, body FROM lab_logs;"
q "SELECT name, type, compression_codec, formatReadableSize(data_compressed_bytes) AS compressed, formatReadableSize(data_uncompressed_bytes) AS raw, round(data_uncompressed_bytes / data_compressed_bytes, 1) AS ratio FROM system.columns WHERE database = 'drishti' AND table = 'lab_codecs'"

h "6. TTL"
mq "CREATE TABLE lab_ttl AS lab_logs ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (service, ts) TTL toDateTime(ts) + INTERVAL 1 DAY;
INSERT INTO lab_ttl SELECT * FROM lab_logs LIMIT 1000;
INSERT INTO lab_ttl SELECT ts - toIntervalDay(5), service, severity, body FROM lab_logs LIMIT 1000;"
q "SELECT count() AS before_merge FROM lab_ttl"
q "OPTIMIZE TABLE lab_ttl FINAL"
q "SELECT count() AS after_merge FROM lab_ttl"

h "7. Why the storage consumer must batch"
q "SELECT count() AS parts_before FROM system.parts WHERE database = 'drishti' AND table = 'lab_logs' AND active"
# 500 single-row INSERTs in one client session: each is still its own insert -> its own part
INS=$(for i in $(seq 1 500); do echo "INSERT INTO lab_logs VALUES (now64(3), 'svc-x', 'INFO', 'tiny insert $i');"; done)
echo; echo "SQL (multi)> 500 x INSERT INTO lab_logs VALUES (... one row ...)"
start=$(date +%s.%N); echo "$INS" | "${CH[@]}" --multiquery >/dev/null || echo "!! ERROR"; end=$(date +%s.%N)
echo "500 single-row inserts took $(awk "BEGIN{printf \"%.2f\", $end-$start}") s"
q "SELECT count() AS parts_after_500_tiny_inserts FROM system.parts WHERE database = 'drishti' AND table = 'lab_logs' AND active"
start=$(date +%s.%N)
q "INSERT INTO lab_logs SELECT now64(3), 'svc-y', 'INFO', concat('batched ', toString(number)) FROM numbers(500)"
end=$(date +%s.%N); echo "one 500-row insert took $(awk "BEGIN{printf \"%.2f\", $end-$start}") s"
q "SELECT count() AS parts_after_one_batched_insert FROM system.parts WHERE database = 'drishti' AND table = 'lab_logs' AND active"

h "8. Idempotent writes for at-least-once delivery"
mq "CREATE TABLE lab_dedup (run_id String, seq UInt64, idx UInt32, ts DateTime64(3), body String) ENGINE = ReplacingMergeTree ORDER BY (run_id, seq, idx);
INSERT INTO lab_dedup SELECT 'r1', number, 0, now64(3), 'x' FROM numbers(1000);
INSERT INTO lab_dedup SELECT 'r1', number, 0, now64(3), 'x' FROM numbers(500);"
q "SELECT count() AS before_merge FROM lab_dedup"
q "SELECT count() AS with_final FROM lab_dedup FINAL"
q "OPTIMIZE TABLE lab_dedup FINAL"
q "SELECT count() AS after_optimize FROM lab_dedup"
mq "CREATE TABLE lab_blockdedup (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO lab_blockdedup SETTINGS insert_deduplication_token = 'batch-42' SELECT number FROM numbers(10);
INSERT INTO lab_blockdedup SETTINGS insert_deduplication_token = 'batch-42' SELECT number FROM numbers(10);"
q "SELECT count() AS rows_after_retried_batch FROM lab_blockdedup"

h "cleanup"
mq "DROP TABLE IF EXISTS lab_logs; DROP TABLE IF EXISTS lab_logs_by_ts; DROP TABLE IF EXISTS lab_logs_p; DROP TABLE IF EXISTS lab_codecs; DROP TABLE IF EXISTS lab_ttl; DROP TABLE IF EXISTS lab_dedup; DROP TABLE IF EXISTS lab_blockdedup;"
echo; grep -c '!! ERROR' "$LOG" | xargs echo "statements that errored:"
echo "saved: $LOG"
