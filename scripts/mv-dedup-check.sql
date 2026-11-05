-- Run by scripts/mv-dedup-check.sh against a scratch database: the schema in
-- clickhouse/schema/ with drishti. renamed. Inserts batches the way the
-- storage consumer does (async_insert=0, a dedup token, the dependent-view
-- setting), retries one with the same token, and compares drishti.logs with
-- logs_1m. One row per check: name, ok (1/0), detail.
INSERT INTO drishti_check.logs (ts, observed_ts, received_at, service, severity_number, severity_text, body, kafka_partition, kafka_offset)
SETTINGS async_insert = 0, insert_deduplication_token = 'mvcheck-a', deduplicate_blocks_in_dependent_materialized_views = 1
SELECT now64(9) - toIntervalSecond(number % 120), now64(9), now64(3), 'cart', [9, 13, 17][number % 3 + 1], '', 'line ' || toString(number), 0, number
FROM numbers(3000);

-- The retry: same batch, same token (the consumer's ambiguous-timeout case).
INSERT INTO drishti_check.logs (ts, observed_ts, received_at, service, severity_number, severity_text, body, kafka_partition, kafka_offset)
SETTINGS async_insert = 0, insert_deduplication_token = 'mvcheck-a', deduplicate_blocks_in_dependent_materialized_views = 1
SELECT now64(9) - toIntervalSecond(number % 120), now64(9), now64(3), 'cart', [9, 13, 17][number % 3 + 1], '', 'line ' || toString(number), 0, number
FROM numbers(3000);

-- Another batch, with text-only severities.
INSERT INTO drishti_check.logs (ts, observed_ts, received_at, service, severity_number, severity_text, body, kafka_partition, kafka_offset)
SETTINGS async_insert = 0, insert_deduplication_token = 'mvcheck-b', deduplicate_blocks_in_dependent_materialized_views = 1
SELECT now64(9), now64(9), now64(3), 'checkout', 0, ['WARNING', 'Error', 'debug'][number % 3 + 1], 'x', 1, number
FROM numbers(300);

SELECT 'retry dropped from logs' AS check, (SELECT count() FROM drishti_check.logs) = 3300 AS ok,
       toString((SELECT count() FROM drishti_check.logs)) AS detail
UNION ALL
SELECT 'retry dropped from logs_1m', (SELECT sum(lines) FROM drishti_check.logs_1m) = 3300,
       toString((SELECT sum(lines) FROM drishti_check.logs_1m))
UNION ALL
SELECT 'per minute/service/severity equal to logs',
       (SELECT count() FROM (
            SELECT toStartOfMinute(ts) AS minute, service,
                   multiIf(severity_number >= 17, 'error', severity_number >= 13, 'warn', severity_number > 0, 'info',
                           lower(severity_text) IN ('error','err','fatal','critical','crit','alert','emerg','emergency'), 'error',
                           lower(severity_text) IN ('warn','warning'), 'warn', 'info') AS severity, count() AS n
            FROM drishti_check.logs GROUP BY ALL
        ) a FULL OUTER JOIN (
            SELECT minute, service, severity, sum(lines) AS n FROM drishti_check.logs_1m GROUP BY ALL
        ) b USING (minute, service, severity)
        WHERE a.n != b.n) = 0,
       ''
UNION ALL
SELECT 'severity_text fallback', (SELECT groupArray(severity) FROM (SELECT severity FROM drishti_check.logs_1m WHERE service = 'checkout' GROUP BY severity ORDER BY severity)) = ['error', 'info', 'warn'],
       toString((SELECT groupArray(severity) FROM (SELECT severity FROM drishti_check.logs_1m WHERE service = 'checkout' GROUP BY severity ORDER BY severity)))
FORMAT TSV;
