#!/usr/bin/env python3
"""Leakage check for the query API (docs/evals.md section 3, design E7).

For every scenario in a recording's timeline, as of a few minutes after its
flag went on, calls every endpoint for every service the API lists and
searches each response for anything that would hand the agent the answer:
a flag name, the flag system's words, or the load generator, anywhere in the
text; and a hidden service as a service (listed, or owning data). Exit 1 on any hit.

The observability stack (the collector, Drishti's containers) is hidden as a
service: its own telemetry is not the system under diagnosis (design E7). A
service's log that merely names it is that service's own evidence and reveals
nothing about the fault (capture B: recommendation "Failed to export traces to
otel-collector:4317 ... DEADLINE_EXCEEDED" under recommendationCacheFailure), so
such a mention is not a leak. Decided before any capture B score existed.

    scripts/leakcheck.py --api http://localhost:8091 --timeline timeline.jsonl
"""
import argparse, json, re, sys, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

FLAGS = ["adFailure", "adHighCpu", "adManualGc", "cartFailure", "emailMemoryLeak", "failedReadinessProbe",
         "imageSlowLoad", "intlShippingSlowdown", "kafkaQueueProblems", "llmInaccurateResponse", "llmRateLimitError",
         "loadGeneratorFloodHomepage", "paymentFailure", "paymentUnreachable", "productCatalogFailure",
         "productCatalogLockContention", "recommendationCacheFailure"]
PHRASES = ["feature flag", "featureflag", "feature_flag", "problempattern", "flagd", "flagservice"]
# Anywhere in a response: these describe the fault injection or the test traffic.
HIDDEN_TEXT = ["flagd-ui", "load-generator"]
# Only as a service: the observability stack.
HIDDEN_OWNERS = ["otel-collector", "otelcol-contrib", "drishti-"]
NEEDLES = [f.lower() for f in FLAGS] + PHRASES + HIDDEN_TEXT
OWNER = re.compile(r'"service"\s*:\s*"(?:container:|host:)?(' + "|".join(re.escape(h) for h in HIDDEN_OWNERS) + r')')


def leak(body: str):
    """The first leak in a response body, as (needle, index), or None."""
    low = body.lower()
    for n in NEEDLES:
        if n in low:
            return n, low.index(n)
    m = OWNER.search(low)
    if m:
        return m.group(1), m.start()
    return None


def hidden_listed(services):
    return [s for s in services if any(h in s.lower() for h in HIDDEN_OWNERS + HIDDEN_TEXT + ["flagd"])]


def get(api, path, **params):
    url = f"{api}{path}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        return e.read().decode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True)
    ap.add_argument("--timeline", required=True)
    ap.add_argument("--after", type=float, default=3.0, help="minutes after flag-on")
    a = ap.parse_args()
    scenarios = [json.loads(l) for l in open(a.timeline) if '"scenario"' in l]
    hits, calls, masked = [], 0, 0
    for sc in scenarios:
        on = datetime.fromisoformat(sc["on"]["set_at"].replace("Z", "+00:00"))
        until = (on + timedelta(minutes=a.after)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        services = json.loads(get(a.api, "/v1/services", until=until))["data"]["services"]
        for s in hidden_listed(services):
            hits.append(f"{sc['flag']} services: hidden service {s!r} listed")
        bodies = [("overview", get(a.api, "/v1/overview", until=until, window="5m")),
                  ("anomalies", get(a.api, "/v1/anomalies", until=until, window="15m"))]
        for s in services:
            bodies += [(f"spans {s}", get(a.api, "/v1/spans", until=until, window="5m", service=s)),
                       (f"log_templates {s}", get(a.api, "/v1/log_templates", until=until, window="5m", service=s)),
                       (f"logs {s}", get(a.api, "/v1/logs", until=until, window="5m", service=s, limit="50")),
                       (f"logs warn {s}", get(a.api, "/v1/logs", until=until, window="5m", service=s, limit="50", min_severity="warn"))]
        for name, b in bodies:
            calls += 1
            try:
                masked += json.loads(b).get("masked_lines", 0)
            except ValueError:
                pass
            low = b.lower()
            if '"error"' in low[:20]:
                hits.append(f"{sc['flag']} {name}: error response {b[:200]}")
                continue
            found = leak(b)
            if found:
                n, i = found
                hits.append(f"{sc['flag']} {name}: {n!r} in …{b[max(0, i-80):i+80]}…")
    print(f"{len(scenarios)} scenarios, {calls} responses, masked lines reported: {masked}, leaks: {len(hits)}")
    for h in hits[:40]:
        print("  LEAK", h)
    sys.exit(1 if hits else 0)


if __name__ == "__main__":
    main()
