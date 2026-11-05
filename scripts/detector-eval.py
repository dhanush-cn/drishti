#!/usr/bin/env python3
"""detector-eval.py: score detector events against a capture's scenario timeline.

  scripts/detector-eval.py <events.jsonl> <timeline.jsonl> [--md]

events.jsonl: one detector Event per line (cmd/detector -replay, or the anomalies
topic dumped during a live run). timeline.jsonl: week4-capture's (or week4b's) flag
timeline.

For each scenario: was an anomaly opened on a service the flag affects, between the
flag going on and 90 s after it went off, and how long after the flag? Everything
opened outside scenario windows (baseline and the quiet stretch 2 min after each flag
went off) is a false alarm, reported per hour of quiet time.

"Affected" services come from docs/detector.md section 1 (what the demo's code
does per flag), decided before tuning, not from what fired.
"""
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime

AFFECTED = {
    "adFailure": {"ad", "frontend"},
    "cartFailure": {"cart", "checkout", "frontend", "frontend-proxy"},
    "paymentFailure": {"payment", "checkout", "frontend", "frontend-proxy"},
    "paymentUnreachable": {"checkout", "payment", "frontend", "frontend-proxy"},
    "adHighCpu": {"ad", "frontend"},
    "adManualGc": {"ad", "frontend"},
    "productCatalogLockContention": {"product-catalog", "frontend", "frontend-proxy", "astronomy-db"},
    "imageSlowLoad": {"frontend-proxy", "frontend-web", "image-provider"},
    "productCatalogFailure": {"product-catalog", "frontend", "frontend-proxy", "frontend-web"},
    "intlShippingSlowdown": {"shipping", "checkout", "frontend"},
    "loadGeneratorFloodHomepage": {"frontend", "frontend-proxy", "frontend-web"},
    "recommendationCacheFailure": {"recommendation", "frontend"},
    "emailMemoryLeak": {"email", "checkout"},
}
GRACE_AFTER_OFF = 90   # s: effects (and detection) can trail the flag
QUIET_AFTER_OFF = 120  # s: recovery transients are not counted as false alarms


def ts(s):
    s = s.replace("Z", "+00:00")
    if "." in s:
        head, rest = s.split(".", 1)
        frac = "".join(c for c in rest if c.isdigit())
        tz = rest[len(frac):]
        s = f"{head}.{frac[:6]}{tz}"
    return datetime.fromisoformat(s).timestamp()


def main():
    md = "--md" in sys.argv
    evs = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
    tl = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
    opened = [e for e in evs if e["event"] == "opened" and not e.get("replayed")]
    scen, quiet = [], []
    base = next(x for x in tl if x["phase"] == "baseline")
    quiet.append((ts(base["start"]), ts(base["end"])))
    rows = [x for x in tl if x["phase"] == "scenario"]
    end = next((ts(x["end"]) for x in tl if x["phase"] == "end"), None)
    for i, x in enumerate(rows):
        on, off = ts(x["on"]["on_disk_at"]), ts(x["off"]["on_disk_at"])
        slept = off - on > 3 * 60 * x["on_minutes"]
        if slept:  # laptop slept mid-scenario (capture 2): judge the first on_minutes only
            off = on + 60 * x["on_minutes"]
        scen.append((x, on, off, slept))
        nxt = ts(rows[i + 1]["on"]["on_disk_at"]) if i + 1 < len(rows) else end
        q0 = off + QUIET_AFTER_OFF
        if nxt and nxt - q0 > 0 and not slept:
            quiet.append((q0, nxt))
    out = []
    p = out.append
    p("| # | Flag | Detected | First anomaly after the flag | Rule | Service / series | Related anomalies | Unrelated anomalies in window |")
    p("|---|---|---|---|---|---|---|---|")
    detected = 0
    for x, on, off, slept in scen:
        aff = AFFECTED.get(x["flag"], set())
        win = [e for e in opened if on <= ts(e["opened_at"]) <= off + GRACE_AFTER_OFF]
        rel = sorted((e for e in win if e["service"] in aff), key=lambda e: (ts(e["opened_at"]), e["rule"], e["service"], e["id"]))
        unrel = [e for e in win if e["service"] not in aff]
        if rel:
            detected += 1
            f = rel[0]
            ser = f["service"] + " " + " ".join(f"{k}={v}" for k, v in sorted((f.get("series") or {}).items()) if k != "template_id")
            if f.get("template"):
                ser += f' "{f["template"][:60]}"'
            p(f'| {x["n"]} | {x["flag"]} | yes | {ts(f["opened_at"]) - on:.0f} s | {f["rule"]} | {ser} | {len(rel)} | {len(unrel)} |')
        else:
            p(f'| {x["n"]} | {x["flag"]} | **no** | | | | 0 | {len(unrel)} |')
    qsec = sum(b - a for a, b in quiet)
    fa = [e for e in opened if any(a <= ts(e["opened_at"]) <= b for a, b in quiet)]
    p("")
    p(f"Detected {detected} of {len(scen)} scenarios.")
    p(f"False alarms: {len(fa)} anomalies opened in {qsec / 3600:.2f} h of quiet time ({len(fa) / max(qsec / 3600, 1e-9):.1f} per hour).")
    by_rule = Counter(e["rule"] for e in fa)
    if by_rule:
        p("By rule: " + ", ".join(f"{r} {n}" for r, n in by_rule.most_common()))
        by_series = Counter((e["rule"], e["service"], (e.get("series") or {}).get("span.name") or (e.get("template") or "")[:50]) for e in fa)
        p("")
        p("| False alarms | Rule | Service | Series |")
        p("|---|---|---|---|")
        for (r, s, ser), n in by_series.most_common(15):
            p(f"| {n} | {r} | {s} | {ser} |")
    print("\n".join(out))


if __name__ == "__main__":
    main()
