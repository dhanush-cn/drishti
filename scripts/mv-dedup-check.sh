#!/usr/bin/env bash
# mv-dedup-check.sh: is drishti.logs_1m exactly-once like drishti.logs on this
# ClickHouse? Applies clickhouse/schema/*.sql to a scratch database
# (drishti_check), runs scripts/mv-dedup-check.sql, drops the database.
# Exit 0 only if every check passes. Touches nothing in drishti.
set -euo pipefail
cd "$(dirname "$0")/.."
ch() { docker exec -i drishti-clickhouse clickhouse-client --user drishti --password drishti_dev "$@"; }
ch --query "DROP DATABASE IF EXISTS drishti_check"
ch --query "CREATE DATABASE drishti_check"
trap 'ch --query "DROP DATABASE IF EXISTS drishti_check" || true' EXIT
for f in clickhouse/schema/*.sql; do
  sed 's/drishti\./drishti_check./g' "$f" | ch --multiquery
done
echo "ClickHouse $(ch --query 'SELECT version()')"
out=$(ch --multiquery < scripts/mv-dedup-check.sql)
echo "$out"
fails=$(awk -F'\t' '$2 != 1' <<<"$out" | wc -l)
if [ "$fails" -ne 0 ] || [ "$(wc -l <<<"$out")" -ne 4 ]; then
  echo "mv-dedup-check: FAILED"
  exit 1
fi
echo "mv-dedup-check: all checks passed"
