#!/usr/bin/env python3
"""Summarize a sweep.sh results directory into summary.md (and stdout).

Per run: verdict, latency, schedule lag, and mean / max CPU per container
over the steady window only (warm-up and drain excluded), from the docker
stats samples taken during the run. 100% CPU = one core.

Per rate: PASS only if every repeat passed; some-but-not-all = KNEE; none =
FAIL; a repeat without run.json makes the rate INCOMPLETE (never judged on
the remaining repeats). The knee is the first rate that is not PASS.
"""
import collections, csv, datetime, json, pathlib, re, statistics, sys

out = pathlib.Path(sys.argv[1])
rows, by_rate = [], collections.OrderedDict()


def parse_start(s):
    # Go writes 9 fractional digits; Python < 3.11 accepts at most 6.
    s = re.sub(r"(\.\d{6})\d+", r"\1", s).replace("Z", "+00:00")
    return datetime.datetime.fromisoformat(s).timestamp()


def cpu_window(label, lo, hi):
    cpu = collections.defaultdict(list)
    path = out / "stats" / f"{label}.csv"
    if not path.exists():
        return cpu
    for rec in csv.reader(open(path)):
        if len(rec) < 3 or not rec[2].endswith("%"):
            continue
        t, name, c = int(rec[0]), rec[1], rec[2]
        if lo <= t <= hi:
            n = "loadgen" if "loadgen" in name else re.sub(r"^drishti-|-1$", "", name)
            cpu[n].append(float(c[:-1]))
    return cpu


def fmt(cpu, k):
    v = cpu.get(k, [])
    return f"{statistics.mean(v):.0f} / {max(v):.0f}" if v else "n/a"


for line in (out / "index.tsv").read_text().splitlines():
    label, rate, args, d = (line.split("\t") + [""] * 4)[:4]
    # Group repeats by label minus the repeat letter, not by loadgen flags:
    # two configurations can share flags and differ in the system under
    # test (e.g. receiver linger), and SWEEP_TAG is what tells them apart.
    key = (int(rate), re.sub(r"_[a-j]$", "", label), args)
    by_rate.setdefault(key, [])
    rj = out / "bench-runs" / d / "run.json"
    if not d or not rj.exists():
        rows.append(f"| {label} | {rate} | NO RESULT | | | | | | | | |")
        by_rate[key].append("NO RESULT")
        continue
    r = json.loads(rj.read_text())
    start = parse_start(r["result"]["start"])
    lo = start + r["config"]["warmup_sec"]
    hi = lo + r["config"]["steady_duration_sec"]
    cpu = cpu_window(label, lo, hi)
    s, v = r["summary"], ("PASS" if r["verdict"]["pass"] else "FAIL")
    by_rate[key].append(v)
    rows.append(
        f"| {label} | {rate} | {v} | {s['p50_us']/1e3:.1f} | {s['p99_us']/1e3:.1f} | {s['p999_us']/1e3:.1f} | "
        f"{r['result']['max_schedule_lag_ns']/1e6:.0f} | {fmt(cpu,'receiver')} | {fmt(cpu,'kafka')} | {fmt(cpu,'loadgen')} | "
        f"{'; '.join(r['verdict']['reasons'])} |"
    )

md = [
    f"# Sweep {out.name} (internal, provisional thresholds)",
    "",
    "CPU% = docker stats per container over the steady window (100% = one core): mean / max.",
    "",
    "| run | rec/s | verdict | p50 ms | p99 ms | p99.9 ms | lag ms | receiver CPU | kafka CPU | loadgen CPU | reasons |",
    "|---|---|---|---|---|---|---|---|---|---|---|",
    *rows,
    "",
    "## Per rate (PASS = every repeat passed; KNEE = some passed; FAIL = none)",
    "",
]
for (rate, group, args), vs in by_rate.items():
    ran = [v for v in vs if v != "NO RESULT"]
    if not ran:
        verdict = "NO RESULT"
    elif len(ran) < len(vs):
        verdict = "INCOMPLETE"  # a repeat produced no run.json: rerun before judging
    else:
        verdict = "PASS" if all(v == "PASS" for v in ran) else ("KNEE" if any(v == "PASS" for v in ran) else "FAIL")
    md.append(f"- {group} ({rate} rec/s{(', ' + args) if args else ''}): **{verdict}** ({', '.join(vs)})")
(out / "summary.md").write_text("\n".join(md) + "\n")
print("\n".join(md))
