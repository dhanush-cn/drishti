# Streaming detector, anomalies topic, live dashboard

Week 4 milestone: **flip a demo failure flag → an anomaly appears in seconds**, on the dashboard
and on the `anomalies` topic that the Week 5 agent will read.

> Status (4 Nov): built and tuned on one recording of the demo (capture 2, 3–4 Nov). Every number
> below is measured, and says where. **Tuned and scored on the same recording**, so the results in
> section 10 are an upper bound until run B validates them on fresh traffic.

## 0. Decisions

| # | Decision | As built |
|---|---|---|
| D1 | Signals the detector reads | Logs + span metrics (from traces) + container memory |
| D2 | Demo collector change | `span_metrics` flush every 5 s (patch 28) |
| D3 | Detection method | 7 count rules on one statistical primitive + 1 memory trend rule. No ML |
| D4 | Detector state | In memory, one shard per input partition; rewind 15 min of Kafka on assignment; no commits |
| D5 | Delivery | `anomalies` topic, JSON v1, deterministic IDs, at-least-once; consumers dedupe by ID |
| D6 | Dashboard transport | Server-Sent Events, live data via Kafka `live.stats`; no Redis |
| D7 | ClickHouse this week | `logs_1m` per-minute rollup (materialized view); metrics storage is Week 5 |
| D8 | "Alert in seconds" | Two numbers: flag → alert, and evidence → alert (Drishti's own latency) |

What changed from the 3 Nov design, and why, is in section 11.

## 1. What the demo emits per scenario

Read from the OTel Demo 3.1.0 source and collector-contrib 0.159.0. "Where" is the service whose
telemetry changes; often not the root cause, which is the agent's job.

| Flag | What the code does | Visible in |
|---|---|---|
| adFailure | 1 in 10 `GetAds` throws UNAVAILABLE | frontend error log, ad span errors |
| cartFailure | `EmptyCart` hits a bad store; **no log line in cart's code** | cart/checkout span errors (and, in capture 2, a new cart log template) |
| paymentFailure | `charge.js` throws; `index.js` logs `warn` | payment logs, payment/checkout span errors |
| paymentUnreachable | checkout dials a bad address | checkout span errors, frontend error log |
| adHighCpu | `CPULoad` burns CPU in ad; logs one INFO line when enabled | ad logs, ad latency |
| adManualGc | full GC per request, WARN per GC | ad logs, ad latency |
| emailMemoryLeak | retains memory per email | email container memory (docker_stats, every 10 s) |
| recommendationCacheFailure | cache grows; `cache miss`/`cache hit` mix shifts | recommendation logs, latency |
| productCatalogLockContention | lock contention | product-catalog latency |
| imageSlowLoad | browser-only: Envoy delays `/images` | frontend-proxy latency (Locust's browser user) |
| productCatalogFailure | one product fails | product-catalog/frontend span errors |
| intlShippingSlowdown | international orders delayed, one INFO line each | shipping logs, latency |
| loadGeneratorFloodHomepage | Locust floods `/` | frontend/frontend-proxy call rate (no fault) |

Signal latency before Drishti sees anything: SDK logs ≤ 1 s, spans ≤ 5 s plus the 5 s
`span_metrics` flush, container stats 10 s.

## 2. Architecture

```
otlp.logs ─────┐                                     ┌─► anomalies   (JSON v1, key = service, 6 partitions, 7 d)
               ├─► detector (cmd/detector) ──────────┤
otlp.metrics ──┘   one shard per input partition:    └─► live.stats  (1 s snapshot per instance, 1 partition, 1 h)
                   decode, Drain, 5 s buckets,                │
                   windows, rules                             ▼
                                            dashboard (cmd/dashboard): 15 min in memory, SSE to the page
storage consumer ─► drishti.logs ─► logs_1m (materialized view)
```

- **Packages.** `internal/otlpmetrics` (OTLP metrics → points, cumulative → delta),
  `internal/drain` (log templates), `internal/detect` (shards, rules, replay; no Kafka client),
  `internal/detector` (the Kafka service around it), `internal/dashboard`.
- **One shard per input partition** (topic, partition), not per partition number. `otlp.logs/0` and
  `otlp.metrics/0` are fetched independently and can be minutes apart. A shard's clock is its own
  records' timestamps, so it must see one ordered stream. Found on capture 2: with one shard per
  partition number, 13-minute-old metric deltas landed in the current bucket and read as 300×
  traffic surges (93 false alarms/h). No rule combines a log series with a metric series, so the
  split costs nothing.
- **Time = Kafka record timestamp** (receiver produce time). Container clocks can skew; receive time
  is near-monotonic per partition, so buckets close without a reordering buffer.
- **Records are keyed by service** (receiver), so all of a service's series live in one shard, and N
  detector instances split the partitions with no shared state.
- **Live loop** (`internal/detector`): poll (at most 1 s), process every shard that got records in
  parallel (one goroutine per shard, shards share nothing), then advance the clocks of **caught-up**
  shards to the wall clock minus a 2 s grace, so a quiet partition still closes its windows. A shard
  still catching up stays on its records' clock. Revoked partitions' shards are dropped; the new owner
  rewinds.

## 3. One statistical primitive

Every count rule asks: *in this window I observed k, the baseline expects μ: how surprising is k?*

- **Counts:** Poisson upper tail `P(X ≥ k | μ)`; for a drop, `P(X = 0 | μ)`.
- **Ratios** (errors or slow calls / calls): binomial upper tail against the baseline ratio.
- **A window fires** when log10 p < **−6**, k ≥ **3**, and observed ≥ **3×** expected. The ratio
  guard matters on busy series: +10% on 1000 calls/bucket is "significant" and meaningless.
- **Windows: 30 s, 2 min, 5 min**, all evaluated at every 5 s bucket boundary. The short window
  catches bursts; the long ones catch trickles. At 5 Locust users a 50% payment failure is about one
  error per 50 s, which never reaches k = 3 in 30 s.
- **Baselines:** per series, an exponentially weighted per-bucket rate, half-life 10 min, with bias
  correction (a new series doesn't read as quiet).
  - **Lag:** a bucket enters the baseline only when it leaves the longest window, so a 5-minute
    trickle can't raise its own expectation before it is judged.
  - **Taint:** buckets behind a firing never enter the baseline, so an incident doesn't become the new
    normal (a test holds 8× for 20 min as one open anomaly).
  - **Floor:** a series that never had an event still expects 0.5 per history seen (capped at 30 min):
    its first burst gets a finite probability (without it, log10 p = −Inf, which JSON can't encode).
- **Warm-up:** a window is judged only when it lies wholly after its service's first 10 min. Without
  that, a window still holding warm-up data was judged against an empty baseline as warm-up ended
  (errors at minute 5 fired at minute 10: test `TestWarmUp_NothingFires`).
- **Lifecycle:** `opened` on the first firing, `updated` every 30 s while it holds, `resolved` 60 s
  after the last firing. ID = `sha1(rule, series, opened bucket end)[:16]`: a restart's rewind
  re-emits an anomaly under the ID it had.

| Rule | Series | Fires on |
|---|---|---|
| `log_template_new` | (service, severity, template) | a template first seen after warm-up: ERROR on its 1st line, WARN on its 2nd within 5 min |
| `log_template_spike` | (service, severity, template) | count up |
| `log_error_spike` | service, WARN+ lines | count up |
| `span_errors` | (service, span name, kind) | binomial, error ratio vs baseline (floor 0.5%) |
| `span_slow` | (service, span name, kind) | binomial: share of calls above the baseline's p99 bucket edge vs baseline (floor 1%; needs 50 calls of history) |
| `span_traffic_surge` | (service, span name, kind) | calls up |
| `span_traffic_drop` | SERVER spans only | no calls in a 2 or 5 min window where the baseline expects them; only for series with calls in ≥ 90% of baseline buckets |
| `container_memory_growth` | container | least squares over 5 min: R² ≥ 0.8, grew ≥ 2% of the limit, and the limit is ≤ 15 min away at this slope or usage is ≥ 90% of it |

INTERNAL spans are skipped (they repeat their SERVER parent; the browser's `resourceFetch` swings
with page content). The load generator is never alerted on (`-ignore-services`): it is the users, not
the system.

**Drain** (He et al. 2017), per (service, severity): depth 4, similarity 0.4, ≤ 100 children per node,
≤ 2000 templates per parser, lines cut at 64 tokens. Tokens containing a digit are masked as `<#>`
(`key=value` keeps the key); `<#>` counts as a match, so `attempt=1` and `attempt=2` are one template.
Per severity because an ERROR "API request failed" otherwise merged into the INFO "API request
completed" (2 of 3 tokens equal). Measured: Loghub 2k samples, mean grouping accuracy 0.821 at depth 4
vs 0.762 at depth 3; the load generator's templates are grouped with purity 1.

**Cumulative → delta** (`internal/otlpmetrics`): the first point of a series gives no delta; a lower
value or a new start time is a reset; a changed histogram layout restarts the series.

## 4. State, restarts, gaps

- **No commits.** On every assignment the detector starts each partition at `now − 15 min` by
  timestamp (`AdjustFetchOffsetsFn`, so a partition taken over an hour later still rewinds 15 min, not
  to process start). Warm-up (10 min) + the longest window (5 min) = 15 min: a restart can fire again
  as soon as it catches up.
- **`replayed: true`** marks events from buckets that ended before the process started. They carry
  the IDs they had the first time; the dashboard absorbs the copies.
- **Long gaps** (a sleeping laptop; > 6 min without records): the open bucket is closed if it holds
  records, then the clock jumps. Nothing was observed in the gap, so nothing is judged, learned or
  forgotten from it:
  - gap buckets are not evaluated (every steady SERVER span would read as a traffic drop on wake);
  - baselines take in the observed buckets still in the ring and are **not decayed** over the gap.
    Decaying 7.5 h scaled a baseline's weight to 2⁻⁴⁵, so the first bucket after wake became the whole
    baseline, and on capture 2 every series surged on wake;
  - open anomalies resolve at the time they would have;
  - the gap isn't idle time (series aren't evicted on wake);
  - cumulative-metric state is forgotten, so the first point after a stopped stream doesn't put the
    whole gap's calls in one bucket.
  On capture 2 this cut anomalies opened in the 3 min after the 7.5 h sleep from ~60 to 9. The 9 left
  are real data: the demo's collector flushing its queue on wake.
- **Bounds:** ≤ 50,000 series per shard (more are dropped and counted); series idle 30 min are evicted.
  Peak RSS replaying all of capture 2 in one process: **28 MiB** (cloud VM). Compose gives it
  `GOMEMLIMIT` 256 MiB / `mem_limit` 384m.

## 5. `anomalies` topic (JSON, schema v1)

Key = service (one service's events in order). 6 partitions, 7 days. A real event from capture 2:

```json
{
  "schema": 1, "id": "f8b437a3a210c916", "event": "opened",
  "rule": "span_errors", "service": "payment",
  "series": {"span.kind": "SERVER", "span.name": "oteldemo.PaymentService/Charge"},
  "opened_at": "2026-11-03T17:08:55Z", "detected_at": "2026-11-03T17:08:55Z", "evidence_at": "2026-11-03T17:08:55Z",
  "window_start": "2026-11-03T17:06:55Z", "window_end": "2026-11-03T17:08:55Z",
  "evidence": {"window": "2m0s", "observed": 3, "expected": 0.02, "calls": 4, "baseline_ratio": 0.005, "log10_p": -6.303},
  "detector": {"version": "dev", "topic": "otlp.metrics", "partition": 5},
  "source": {"partition": 5, "offsets": [16655, 16830], "topic": "otlp.metrics"}
}
```

- `detected_at`: wall clock when the bucket was judged (live); the bucket end in replay.
- `evidence_at`: newest record timestamp behind the evaluation. `detected_at − evidence_at` is
  Drishti's own latency (D8).
- `source.offsets`: the shard's records behind the window, so the evals and the agent can fetch
  exactly what caused the alert.
- `template` (log rules): the Drain template text. Template IDs are per process and not stable
  across restarts; the text is.

`live.stats` (1 partition, 1 h): one record per detector instance per second, key = instance:
`{"schema":1,"instance":…,"at":…,"period_ms":…,"max_record_ts":…,"services":{svc:{"logs","warn","calls","errors","open_anomalies"}}}`.
Counts are over the period; `open_anomalies` is the level now; `calls`/`errors` are SERVER spans.

## 6. Live dashboard

`cmd/dashboard` reads `anomalies` and `live.stats` with no consumer group (every dashboard reads
everything), from 15 min back by timestamp, keeps that in memory, and serves one static page
(`internal/dashboard/web/index.html`, no build step, no CDN) plus `/events` (SSE) and `/api/state`.

- **SSE, not WebSocket:** data flows one way; SSE is plain HTTP and `EventSource` reconnects by itself.
- **No Redis:** Kafka already holds both streams with replay; a restarted dashboard rewinds and has
  the same state.
- **Dedupe:** an event is pushed only if it changes the page: a new ID, a later lifecycle stage, or a
  later `updated`. A second `opened`/`resolved` (a restarted detector's rewind) is absorbed.
- **Back-pressure:** a page that stops reading is disconnected after 1024 queued messages, never
  waited on. It reconnects and gets a fresh snapshot.
- **Measured (cloud VM, in-process Kafka, headless Chromium):** Kafka → rendered card 13–54 ms over
  8 events, one per rule. On the laptop this adds the real broker hop; run B measures it.

## 7. ClickHouse: `logs_1m`

`clickhouse/schema/002_logs_1m.sql`: lines and body bytes per (service, severity class, minute),
AggregatingMergeTree fed by a materialized view on `drishti.logs`, TTL 30 days. It covers inserts from
its creation on; there is no backfill.

**Exactly-once, checked on ClickHouse 26.9 (chdb):** the storage consumer retries a batch with the
same `insert_deduplication_token`, and `drishti.logs` drops the copy. The view drops it too only if
**both** of these hold:
1. the view's table has its own `non_replicated_deduplication_window`;
2. `deduplicate_blocks_in_dependent_materialized_views = 1`.

Without (1), 3,000 rows inserted twice with one token gave 3,000 in `logs` and 6,000 in `logs_1m`.
The consumer now sends (2) explicitly with every insert (26.9's default is already 1; it is pinned
like `async_insert=0`). `scripts/mv-dedup-check.sh` runs the same check against the laptop's
ClickHouse in a scratch database.

## 8. Throughput

- **Per shard, synthetic logs** (`BenchmarkShardLogs`: 100-line records, 8 services × 20 templates):
  **1.23 M lines/s** on one core of the cloud VM (Xeon 2.1 GHz). Decode and Drain tokenizing dominate
  the profile. The week's target is 100k log rows/s; shards run in parallel on top of that.
- **Replay of capture 2** (one goroutine, gunzip + logs + metrics): 124k records, 255k log lines and
  526k metric points in 7.3 s.
- Live throughput beside storage at 100k rows/s is measured in run B (consumer lag stays flat).

## 9. How it's tuned and evaluated

1. **Capture:** capture 2 (3–4 Nov) recorded both input topics with `cmd/kafkadump`: a 20 min
   no-flag baseline, then 13 scenarios (flag on 4 min, 3 min off; memory 10 min). The laptop slept
   7.5 h during scenario 12. Slept scenarios are judged on their first `on_minutes` only, and their
   quiet time is not counted.
2. **Tune offline:** `cmd/detector -replay capture.kd.gz -out events.jsonl`, the same code as live,
   then `scripts/detector-eval.py events.jsonl timeline.jsonl`. A scenario counts as detected if an
   anomaly opens on a service the flag affects (section 1, fixed before tuning) between flag-on and
   90 s after flag-off. Anything opened in quiet time (the baseline, and from 2 min after each
   flag-off to the next flag-on) is a false alarm.
3. **Validate live (run B), on fresh traffic.**

## 10. Results on capture 2 (tuned on the same data)

**12 of 13 scenarios detected. 1 false alarm in 0.54 h of quiet time (1.9/h).** Replays are
deterministic: two runs give byte-identical output.

| # | Flag | First anomaly (scorer) | First fault signal (by hand) |
|---|---|---|---|
| 1 | adFailure | 29 s, new ERROR template, frontend "API request failed" | same |
| 2 | cartFailure | 104 s, traffic surge, frontend `POST` | **119 s**, new template, cart "Wasn't able to connect to redis" (the surge is not the fault) |
| 3 | paymentFailure | 13 s, new WARN template, payment "Payment request failed. Invalid token…" | same; span errors at 103–108 s |
| 4 | paymentUnreachable | 57 s, new ERROR template, frontend "Checkout failed to place order" | same; span errors at 117–137 s |
| 5 | adHighCpu | 11 s, template spike, ad "High CPU-Load problempattern enabled" | same (the demo's own INFO line); latency at 76 s |
| 6 | adManualGc | 25 s, WARN+ spike, ad | same, with its new template |
| 7 | productCatalogLockContention | 39 s, slow calls, frontend `GET` | same; product-catalog at 44 s |
| 8 | imageSlowLoad | 23 s, slow calls, frontend-proxy `GET` | same |
| 9 | productCatalogFailure | 8 s, WARN+ spike, frontend | same; span errors at 13–28 s |
| 10 | intlShippingSlowdown | **missed** | one INFO line in 4 min: nothing to count |
| 11 | loadGeneratorFloodHomepage | 11 s, traffic surge, frontend `GET /` | same |
| 12 | recommendationCacheFailure | 5 s, template spike, recommendation "get_product_list: cache <*>" | same |
| 13 | emailMemoryLeak | 105 s, traffic surge, checkout → currency (post-wake noise) | **155 s**, memory growth, email (+2 MiB/min, limit in 14 min) |

Times are from the flag write, so they include flagd propagation and the demo's telemetry delays
(section 1). The false alarm is a `span_traffic_drop` on flagd's `EventStream`, a long-lived stream
whose calls are reported when they end.

**Sensitivity** (each row changes one setting; same capture, same scorer):

| Setting | Detected | False alarms in 0.54 h |
|---|---|---|
| defaults (α 10⁻⁶, ratio 3, windows 30 s/2 m/5 m, half-life 10 min) | 12/13 | 1 |
| log10 α −5 / −7 / −8 | 12/13 | 2 / 1 / 1 |
| log10 α −4 | 13/13 | 5 |
| min-ratio 2.5 / 4 / 5 | 13/13 / 12/13 / 12/13 | 1 / 1 / 1 |
| min-ratio 2 | 13/13 | 3 |
| min-count 2 / 5 | 12/13 | 1 |
| any one or two of the three windows | 12/13 | 0–1 |
| half-life 5 min / 20 min | 13/13 / 12/13 | 2 / 1 |
| min-error-ratio 0.25% / 1% | 12/13 | 1 |
| drop-min-active 0.8 / 0.95 | 12/13 | 1 |

The result isn't on a knife edge: 12/13 with 0–2 false alarms across two decades of α and ratios from
2.5 to 5. The "13/13" rows catch intlShippingSlowdown with **an unrelated signal** (at ratio 2.5, a
frontend `POST /api/cart` surge) that the service-level scorer accepts. Defaults stay. 0.54 h of quiet
time is too little to rank settings by false alarms; run B's 30 min no-flag window adds more.

## 11. Changed from the 3 Nov design

| Design said | Built | Why |
|---|---|---|
| 30 s windows | 30 s, 2 min, 5 min | trickles at 5 users never reach k = 3 in 30 s (capture 2) |
| one shard per partition | per input partition (topic, partition) | 93 false alarms/h from mixed clocks (capture 2) |
| new template: WARN+ twice | ERROR once, WARN twice | an ERROR line is news on its own (adFailure, paymentUnreachable) |
| slow calls vs 1% | vs baseline share, floored at 1% | a series whose normal slow share is above 1% would fire all the time |
| memory: +25% over a 10 min median | slope, R², time-to-limit, near-limit | a JVM sitting at its heap is not a leak; time-to-limit is what matters |
| one traffic rule | surge, and drop with guards | bursty and long-lived streams go quiet as a matter of course (4 false alarms/0.54 h) |
| Drain per service | per (service, severity), depth 4, `<#>` masking | INFO/ERROR merges; depth 3 lost 6 points on Loghub |
| warm-up per series | windows wholly after the service's warm-up | warm-up data was judged as warm-up ended (test) |
| gaps decay baselines | gaps are unobserved: not judged, learned or decayed | every series surged after the 7.5 h sleep (capture 2) |
| load generator | never alerted on | it is the users |

## 12. Known limits

- Tuned and scored on one recording; run B is the real test.
- A single INFO line per event (intlShippingSlowdown) is invisible to counting; latency would catch
  it if shipping's span metrics showed it (they didn't in capture 2).
- One false alarm source: flagd `EventStream` drops. A per-series exclusion would hide it; left
  visible on purpose.
- `logs_1m` has no backfill. Metrics aren't stored in ClickHouse yet (Week 5).

## 13. Patches and runs

| Patch | Content | Verified (cloud) |
|---|---|---|
| 29 | demo memory headroom | capture 2 ran healthy |
| 30 | `internal/otlpmetrics` | 11 revert-proofs |
| 31 | `internal/drain` | 11 revert-proofs; Loghub (optional) |
| 32 | `internal/detect` + `scripts/detector-eval.py` | 32 revert-proofs; capture 2 replay; race ×3 |
| 33 | `internal/detector` + `cmd/detector` | kfake: rewind by time, replayed flag, live detection via the wall-clock tick, rebalance; 6 revert-proofs |
| 34 | `internal/dashboard` + `cmd/dashboard` | unit + kfake + headless Chromium; 9 revert-proofs |
| 35 | `logs_1m` view + storage sends the view-dedup setting + `scripts/mv-dedup-check.sh` | chdb 26.9; 3 revert-proofs + storage test |
| 36 | compose: detector, dashboard; Dockerfile targets; `anomalies`, `live.stats` topics | `docker compose config` |
| 37 | this document | — |

New dependencies: **none** (standard library, franz-go, the OTLP protos already in use).
