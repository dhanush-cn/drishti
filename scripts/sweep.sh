#!/usr/bin/env bash
# sweep.sh: run the load generator at several rates, N repeats each, with
# docker stats sampled DURING every run, then summarize.
#
#   scripts/sweep.sh -o OUT -r "200000 225000 250000" [-n 3] [-g 10] [-- loadgen flags...]
#
#   -o OUT   results directory (created): index.tsv, stats/<label>.csv,
#            bench-runs/<run dir>/, summary.md
#   -r RATES space-separated offered rates (log records/s)
#   -n N     repeats per rate (default 3: one run is not a result)
#   -g SEC   idle gap between runs (default 10), so one run's tail
#            (Kafka flushes, GC) does not leak into the next
#   -s       stop after the first rate where every repeat FAILed: higher
#            rates would only fail again (saves ~5 min per skipped rate)
#   -- ...   passed to every loadgen run (e.g. -stamp=false -duration 30s)
#
# Expects benchmark mode: base compose only, receiver + kafka up, loadgen
# image built from a clean commit (scripts/build-loadgen.sh). Labels are
# r<rate>_<a|b|c...>; a label suffix can be added with SWEEP_TAG=x.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT='' RATES='' N=3 GAP=10 STOP=0
while getopts "o:r:n:g:s" f; do
  case $f in o) OUT=$OPTARG;; r) RATES=$OPTARG;; n) N=$OPTARG;; g) GAP=$OPTARG;; s) STOP=1;; *) exit 2;; esac
done
shift $((OPTIND - 1))
[ -n "$OUT" ] && [ -n "$RATES" ] || { echo "usage: $0 -o OUT -r \"RATES\" [-n N] [-g SEC] [-- loadgen flags]" >&2; exit 2; }
mkdir -p "$OUT/stats" "$OUT/bench-runs" bench/runs
TAG=${SWEEP_TAG:+_$SWEEP_TAG}
reps=(a b c d e f g h i j)

for rate in $RATES; do
  fails=0
  for ((i = 0; i < N; i++)); do
    label="r${rate}${TAG}_${reps[$i]}"
    printf '\n\033[1;36m== %s  (loadgen %s)\033[0m\n' "$label" "$*"
    csv="$OUT/stats/$label.csv"; : > "$csv"
    # One docker stats snapshot takes ~2 s, so this samples every ~2 s.
    ( while :; do t=$(date +%s); docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' | sed "s/^/$t,/" >> "$csv"; done ) &
    sampler=$!
    set +e
    res=$(docker compose --profile bench run --rm loadgen -rate "$rate" "$@" 2>&1 | tee /dev/stderr)
    set -e
    grep -q '^PASS ' <<<"$res" || fails=$((fails + 1))
    kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null || true
    dir=$(grep -o 'saved /runs/[^ ]*' <<<"$res" | sed 's|saved /runs/||' || true)
    printf '%s\t%s\t%s\t%s\n' "$label" "$rate" "$*" "$dir" >> "$OUT/index.tsv"
    [ -n "$dir" ] && cp -r "bench/runs/$dir" "$OUT/bench-runs/"
    sleep "$GAP"
  done
  if [ "$STOP" = 1 ] && [ "$fails" = "$N" ]; then
    echo "every repeat at $rate failed: stopping (-s)"; break
  fi
done
python3 scripts/sweep-summary.py "$OUT"
