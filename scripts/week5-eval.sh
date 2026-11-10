#!/usr/bin/env bash
# Evals on a replayed recording (docs/evals.md). Runs natively from WSL against the
# stack's Kafka (localhost:9092) and ClickHouse (localhost:8123); one recording in
# the eval database at a time.
#
#   scripts/week5-eval.sh load RECORDING.kd.gz      reset eval topics + drishti_eval, load, store, replay detector
#   scripts/week5-eval.sh queryapi                  (re)start the eval query API on 127.0.0.1:8091
#   scripts/week5-eval.sh leakcheck TIMELINE        every endpoint, every service, every scenario: no leaks
#   scripts/week5-eval.sh run NAME TIMELINE OUT [runner args...]   python -m drishti_agent.evals.run ...
#
# Results and logs go under $EVAL_WORK (default ./eval-work, gitignored).
set -euo pipefail
cd "$(dirname "$0")/.."
WORK="${EVAL_WORK:-$PWD/eval-work}"
BIN="$WORK/bin"
EVAL_DB="${EVAL_DB:-drishti_eval}"
BROKERS="${DRISHTI_KAFKA_BROKERS:-localhost:9092}"
CH_URL="${DRISHTI_CLICKHOUSE_URL:-http://localhost:8123}"
API_ADDR="${EVAL_API_ADDR:-127.0.0.1:8091}"
mkdir -p "$WORK" "$BIN"

build() {
  for c in storage kafkadump detector queryapi evalschema; do
    go build -o "$BIN/$c" "./cmd/$c"
  done
}

# Runs a storage consumer until it has read `want` records AND stored all of
# them: records are counted when decoded, rows when their insert succeeds, and
# a stopped consumer doesn't flush its open batches. So: wait for records >=
# want, then for the stored-row count to stop moving across stats lines (age
# flush is 2 s, stats every 10 s), then check ClickHouse holds exactly that.
store() {
  local signal=$1 topic=$2 table=$3 want=$4 log="$WORK/storage-$1.log"
  "$BIN/storage" -signal "$signal" -brokers "$BROKERS" -topic "$topic" -table "$EVAL_DB.$table" \
    -group "eval-$signal-$(date +%s)" -reset earliest -clickhouse-url "$CH_URL" -session-timeout 10s >"$log" 2>&1 &
  local pid=$! got=0 rows=-1 prev=-2 stable=0
  for _ in $(seq 1 900); do
    sleep 2
    if ! kill -0 "$pid" 2>/dev/null; then echo "storage $signal died:"; tail -20 "$log"; exit 1; fi
    local line; line=$(grep '"msg":"stats"' "$log" | tail -1 || true)
    [[ -z "$line" ]] && continue
    got=$(sed -E 's/.*"records":([0-9]+).*/\1/' <<<"$line")
    rows=$(sed -E 's/.*"rows":([0-9]+).*/\1/' <<<"$line")
    if [[ "$got" -ge "$want" && "$rows" == "$prev" && "$rows" -gt 0 ]]; then
      stable=$((stable + 1)); [[ $stable -ge 6 ]] && break   # unchanged for >= 12 s of polling past a stats line
    else
      stable=0
    fi
    prev=$rows
  done
  kill "$pid"; wait "$pid" 2>/dev/null || true
  local stored; stored=$(count "SELECT count() FROM $EVAL_DB.$table")
  if [[ "$got" -lt "$want" || "$stored" != "$rows" ]]; then
    echo "storage $signal: read $got of $want records, stats say $rows rows stored, table holds $stored"; exit 1
  fi
  echo "storage $signal: $got records, $rows rows stored (table holds $stored)"
}

count() {
  curl -fsS -H "X-ClickHouse-User: ${CH_USER:-drishti}" -H "X-ClickHouse-Key: ${CH_PASSWORD:-drishti_dev}" \
    --data-binary "$1" "$CH_URL/"
}

case "${1:-}" in
  load)
    rec=${2:?recording}
    build
    # EVAL_SKIP_CREATE=1: topics and database already made (tests of this script only).
    [[ "${EVAL_SKIP_CREATE:-0}" == "1" ]] || scripts/create-eval.sh --reset
    "$BIN/kafkadump" load -brokers "$BROKERS" -in "$rec" -prefix eval. | tee "$WORK/load.txt"
    nlogs=$(awk '/^eval.otlp.logs:/ {print $2}' "$WORK/load.txt")
    nmet=$(awk '/^eval.otlp.metrics:/ {print $2}' "$WORK/load.txt")
    store logs eval.otlp.logs logs "$nlogs"
    store metrics eval.otlp.metrics metrics "$nmet"
    "$BIN/detector" -replay "$rec" -out "$WORK/events.jsonl" | tail -1
    "$BIN/kafkadump" load-events -brokers "$BROKERS" -in "$WORK/events.jsonl" -topic eval.anomalies | tee "$WORK/load-events.txt"
    nev=$(awk '/^eval.anomalies:/ {print $2}' "$WORK/load-events.txt")
    store anomalies eval.anomalies anomalies "$nev"
    echo "stored: logs $(count "SELECT count() FROM $EVAL_DB.logs"), metrics $(count "SELECT count() FROM $EVAL_DB.metrics"), anomalies $(count "SELECT count() FROM $EVAL_DB.anomalies")" | tee "$WORK/stored.txt"
    echo "received_at: $(count "SELECT min(received_at), max(received_at) FROM $EVAL_DB.logs")" | tee -a "$WORK/stored.txt"
    ;;
  queryapi)
    build
    pkill -f "queryapi -listen $API_ADDR" 2>/dev/null || true
    "$BIN/queryapi" -listen "$API_ADDR" -db "$EVAL_DB" -clickhouse-url "$CH_URL" >"$WORK/queryapi.log" 2>&1 &
    for _ in $(seq 1 20); do curl -fs "http://$API_ADDR/v1/services?until=2026-01-01T00:00:00Z" >/dev/null 2>&1 && break; sleep 0.5; done
    echo "eval query API on http://$API_ADDR (db $EVAL_DB, pid $!)"
    ;;
  leakcheck)
    python3 scripts/leakcheck.py --api "http://$API_ADDR" --timeline "${2:?timeline}"
    ;;
  run)
    name=${2:?name}; tl=${3:?timeline}; out=${4:?out dir}; shift 4
    if [[ ! -x agent/.venv/bin/python ]]; then
      python3 -m venv agent/.venv
      agent/.venv/bin/pip install -q -e agent
    fi
    (cd agent && .venv/bin/python -W ignore -m unittest discover -s tests >/dev/null 2>&1) || { echo "agent unit tests failed"; exit 1; }
    DRISHTI_GIT_COMMIT=$(git rev-parse --short HEAD) agent/.venv/bin/python -m drishti_agent.evals.run \
      --api "http://$API_ADDR" --timeline "$tl" --recording "$name" --out "$out" "$@"
    ;;
  *)
    sed -n '2,12p' "$0"; exit 2 ;;
esac
