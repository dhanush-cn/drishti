#!/usr/bin/env bash
# measure-batch-shape.sh — pipeline batch shape from the collector's debug exporter (loadgen.md §13).
# Usage: ./measure-batch-shape.sh [seconds]   (default 300)
# Requires: OTel Demo running (collector container "otel-collector"), Drishti receiver + Kafka up.
# Writes everything to ./batch-shape-<UTC timestamp>/ and prints a summary.
set -euo pipefail

DUR="${1:-300}"
OUT="batch-shape-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
KAFKA=drishti-kafka
OFFSETS=/opt/kafka/bin/kafka-get-offsets.sh

offsets_total() { # sum of end offsets across partitions for topic $1
  docker exec "$KAFKA" "$OFFSETS" --bootstrap-server localhost:9092 --topic "$1" \
    | awk -F: '{s+=$3} END {print s+0}'
}

echo "== pre-checks"
docker inspect -f '{{.State.Running}}' otel-collector >/dev/null || { echo "otel-collector not found"; exit 1; }
docker inspect -f '{{.State.Running}}' "$KAFKA" >/dev/null || { echo "$KAFKA not found"; exit 1; }

START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
L0=$(offsets_total otlp.logs); M0=$(offsets_total otlp.metrics)
echo "start=$START logs_offset=$L0 metrics_offset=$M0"
echo "== collecting for ${DUR}s (Locust should be running at its default user count)"
sleep "$DUR"
END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
L1=$(offsets_total otlp.logs); M1=$(offsets_total otlp.metrics)

docker logs --since "$START" --until "$END" otel-collector > "$OUT/collector.log" 2>&1
cat > "$OUT/window.txt" <<EOF
start=$START
end=$END
duration_s=$DUR
kafka_logs_delta=$((L1-L0))
kafka_metrics_delta=$((M1-M0))
EOF

# debug exporter 'basic' lines look like (0.1xx):
#   ... info  Logs     {... "resource logs": 1, "log records": 12}
#   ... info  Metrics  {... "resource metrics": 3, "metrics": 40, "data points": 95}
# The parser tolerates extra fields and either quoting style.
python3 - "$OUT" <<'PY'
import json, re, sys, statistics, pathlib
out = pathlib.Path(sys.argv[1])
text = (out / "collector.log").read_text(errors="replace").splitlines()
def num(line, key):
    m = re.search(r'"?%s"?\s*[:=]\s*(\d+)' % re.escape(key), line)
    return int(m.group(1)) if m else None
logs, mets = [], []
for ln in text:
    rl = num(ln, "resource logs"); lr = num(ln, "log records")
    if rl is not None and lr is not None:
        logs.append((rl, lr)); continue
    rm = num(ln, "resource metrics"); dp = num(ln, "data points")
    if rm is not None and dp is not None:
        mets.append((rm, num(ln, "metrics") or 0, dp))
def dist(xs):
    if not xs: return None
    xs = sorted(xs)
    p90 = xs[min(len(xs)-1, int(round(0.9*(len(xs)-1))))]
    return {"n": len(xs), "median": statistics.median(xs), "p90": p90, "max": xs[-1], "sum": sum(xs)}
win = dict(l.split("=", 1) for l in (out / "window.txt").read_text().split())
res = {
  "window": win,
  "logs": {"batches": len(logs),
           "records_per_batch": dist([b[1] for b in logs]),
           "resources_per_batch": dist([b[0] for b in logs])},
  "metrics": {"batches": len(mets),
              "data_points_per_batch": dist([b[2] for b in mets]),
              "metrics_per_batch": dist([b[1] for b in mets]),
              "resources_per_batch": dist([b[0] for b in mets])},
}
# Loss/dup sanity check (NOT a re-batching detector — see loadgen.md §13)
for sig, key in (("logs", "kafka_logs_delta"), ("metrics", "kafka_metrics_delta")):
    r = res[sig]["resources_per_batch"]
    res[sig]["sanity"] = {"resources_sent": r["sum"] if r else 0, "kafka_delta": int(win[key])}
(out / "summary.json").write_text(json.dumps(res, indent=2))
print(json.dumps(res, indent=2))
if not logs and not mets:
    print("\nWARNING: no debug-exporter count lines matched. Paste `docker logs otel-collector --tail 20` so the parser can be fixed.")
PY
echo "== saved to $OUT/"
