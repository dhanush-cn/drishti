# Load generator (`cmd/loadgen`)

Design record for the synthetic OTLP load generator, decided 11 Oct 2026. It follows the pattern of `docs/receiver.md`: each decision comes with the reason for it, so every number the generator produces can be defended.

**Scope of v1:** a receiver-only ingest benchmark. The generator sends OTLP over gRPC to the receiver. A request counts as ingested when the receiver returns OK, which it does only after Kafka acks. Storage (ClickHouse) is out of scope until Week 2. Items deferred to then are listed at the end with the evidence that would trigger each one.

**This doc describes the full tool; it ships in two passes.**
- **Pass 1 (Week 1, first honest number):** open-loop arithmetic schedule with catch-up bursts (§3); latency from due time into HdrHistogram with plain `RecordValue` (§4); `-timeout`, `-inflight` with saturation counting, warm-up split, and end-of-window drain (§3–4); payload pool with `-services`, templated log bodies, gzip, and `run_id` (§6); the computed verdict with all §10 conditions, written to `run.json` plus `latency.hlog` (§9–10). **Logs only first**; metrics follow in the same week if time allows. Pass 1 gives a defensible single-rate PASS/FAIL.
- **Pass 2 (as the week allows, otherwise Weeks 2–3):** `partitions.json` with kadm and the shared partitioner (§7); `intervals.csv`; `env.json` capture; `bench.sh` pre-flight and `docker stats` sampling (§11); 3-repeat sweeps (§10); the template manifest (§6); `-conns > 1` (§5); closed-loop probe mode (§2). Pass 2 turns it into a reproducible benchmark.
- **Pass 1 numbers are internal.** Promotion to `bench/results/` (§9) requires `env.json`, including the dirty flag, and the 3-repeat rule (§10). Both are Pass 2, so no Pass 1 number goes into the README.
- **Pass 1 still records provenance:** the git commit and dirty flag are passed as image build args and written into `run.json`. This is for traceability, not quotability: a Pass 1 run found in `bench/runs/` weeks later can always be tied to the exact code that produced it.

---

## 1. What an "event" is

**Decision:** an event is **one log record or one metric data point that the receiver acknowledged.** Logs and metrics are reported as **separate** numbers. Every number is reported together with its payload size and batch shape.

**Why not OTLP requests/sec:** this unit measures batching, not ingestion. Changing the batch size from 100 to 1,000 records would cut requests/sec by 10× while the real work stays about the same.

**Why not Kafka messages/sec:** one message is one OTLP Resource, so the count depends on how many resources are in a request and on the key design. The hot-partition finding showed it moves with the partition key. A unit that changes when the design changes is not a throughput unit.

**Why records and data points:** they don't depend on any of Drishti's design choices, and they're the industry's units (log lines; Prometheus-style samples/sec), so the numbers can be compared to known systems.

**Why logs and metrics are separate:** a log record with a ~200 B body and attributes weighs several times more than a gauge data point. A combined number lets a metrics-heavy mix inflate the headline.

**Counting rules:**
- A histogram data point counts as **one** event, whatever its bucket count. The buckets show up in bytes/sec.
- **Ingested** = records in requests that returned OK, minus `partial_success.rejected_log_records` / `rejected_data_points`.
- Non-OK responses of any code count zero and are reported separately, by code.
- One signal per run (`-signal logs|metrics`). This gives clean attribution, so a logs number never depends on metrics traffic in the same run. **This is a v1 constraint.** A mixed-signal run is still worth doing later as a realism check, because the collector sends both at once. Its numbers are not headline numbers.

## 2. Load model: open-loop

**Decision:** **open-loop** is the measurement mode. You set a target rate in records/sec. With a fixed batch size, that becomes a request rate. Requests are sent on schedule whether or not earlier ones have returned. A **closed-loop** mode (`-mode closed`, N workers each waiting for its previous response) exists only for quick capacity probes that tell you where to start a rate sweep. **Its latency numbers are never quoted.**

**Why: coordinated omission.** If the receiver stalls for 2s, a closed-loop generator stops sending during the stall. The histogram gets one bad sample instead of the ~2,000 that an app emitting at 1,000 req/s would have experienced. The generator has coordinated with the system to skip the requests that would have suffered.

**Why open-loop matches reality:** telemetry sources are open-loop. Apps emit logs whether or not the backend is slow. The collector's exporter queue buffers this and eventually drops data. The question being measured is "at rate X, does Drishti keep up?", and that is an open-loop question.

## 3. Scheduling

**Decision:** the schedule is computed arithmetically, **not with `time.Ticker`**.

- Request `i` is due at `start + i × interval`.
- The loop sends every request whose due time has passed, then sleeps until the next due time. If it falls behind, it **sends a burst to catch up and never skips a request.**
- At high rates the loop wakes about every 1 ms and sends however many requests have become due. No timer is needed per request, so sub-millisecond intervals work.

**Why not `time.Ticker`:** its channel has a buffer of 1, and it **drops** ticks when the receiving goroutine falls behind. A generator built on it quietly sends fewer requests than scheduled. That is coordinated omission hidden inside the generator.

**Self-check: schedule lag** = now − due time of the next unsent request. If the lag grows while the receiver is healthy, the generator is the bottleneck and the run is invalid (see §10). This matters because the generator and the stack share the same 16 cores.

**In-flight cap** (`-inflight`): a bound on concurrent RPCs so that a stalled receiver can't make generator memory grow without limit. When a request would exceed the cap, it is **counted as saturated, not waited for.** Waiting would quietly turn the run closed-loop. A saturated request never enters the latency histogram, so it is an omitted sample. **Any saturation fails the run** (§10), and the saturated count says why.

## 4. Latency measurement

**Decision:** latency is measured from the request's **due time**, not its actual send time, and recorded in **HdrHistogram** (`github.com/HdrHistogram/hdrhistogram-go`; check its maintenance status before adding the dependency).

- Measuring from due time is what actually fixes coordinated omission. Generator lateness counts against the system, the way it would for a real app.
- Use plain `RecordValue`, **not `RecordCorrectedValue`**. The corrected form back-fills samples it assumes were missed. Since latency is already measured from due time, using it would correct twice and inflate the tail.
- HdrHistogram records a wide value range in fixed memory and keeps high percentiles accurate. Gil Tene designed it for this problem.
- Keep one histogram per 1s interval for time-series views and **merge** them to get run totals. **Never average per-interval percentiles**, because percentiles don't average.

**Client deadline:** every RPC carries a client-side deadline, `-timeout`, default **5s**, which matches the collector's OTLP exporter so the generator behaves like the real sender. This is separate from the receiver's 4s server-side publish deadline. A request that hits it returns `DEADLINE_EXCEEDED`, and that counts as a failure.

**Draining at the end of the window:** a request belongs to the steady-state window if its **due time** falls inside it. When the schedule ends, wait for every in-flight request to resolve, which is bounded by `-timeout`, and record each one. A request that times out is a **failure**, not a missing sample. Dropping in-flight requests at the end would cut off exactly the tail being measured.

**Warm-up:** the first export costs about 510 ms (connection, metadata, producer ID). Requests due in the `-warmup` window (default 10s) go into a separate histogram that is reported but never used for the verdict.

## 5. Connections

**Decision:** `-conns N`, default 1, means **N separate `grpc.ClientConn`s with requests sent round-robin across them**. It is not a dial option. A single ClientConn to one address keeps one subchannel, so N would otherwise do nothing.

**Why the flag exists:** all RPCs on one HTTP/2 connection share one TCP stream and its flow-control window. If throughput plateaus while CPU is idle, try a small connection pool before blaming the receiver. This is a candidate bottleneck for Weeks 2–3, to be **measured, not assumed.**

## 6. Payload

**Pool:** at startup, pre-generate **K** distinct requests (`-pool`, default 256) and send them in rotation. Repeating one payload would distort results in two ways: lz4 and gzip compress identical bytes unrealistically well, inflating bytes/sec, and a single key puts everything on one partition.

**Services:** the run uses **S** distinct service keys (`-services`, default 20), so keys spread across partitions. `-services 1` reproduces the hot partition on purpose, which makes the Week 3 comparison a two-flag experiment. Twenty keys don't spread evenly over 6 partitions, so the actual spread is always reported (§7).

**Resources per request** (`-resources-per-request`, default **1**, decided 13 Oct from §13's observation): each request carries one service's resource, and services rotate across consecutive requests, which is what the collector actually sends. The pool is rounded up so every key is equally frequent (256 → 260 for 20 services; otherwise some keys are sent 13 times per rotation and others 12). `-resources-per-request 20` reproduces the Pass 1 shape (one request = 20 small Kafka messages) and stays a sweep point. `run.json` records both the requested and effective value, so runs at different settings are never mixed silently.

**Log shape:** templated bodies with variable tokens, body length drawn from `-body-min`..`-body-max` (default about 80–400 B), a mix of severities, and `-attrs` attributes (default about 5–8). Templates are used rather than random bytes because real logs compress the way templated text does, not the way noise does, and the Week 4 Drain detector needs templated input.

**Metric shape:** a mix of gauge, sum, and histogram, with:
- `-series`: distinct series per service (metric name × attribute combination). Series cardinality is what determines ClickHouse sort-key and storage behaviour in Week 2, and it's the first thing anyone asks about a metrics benchmark.
- `-points`: data points per metric per request.

**Drain ground truth stays out of the payload.** The generator writes a side manifest mapping template ID → template string, with counts per run (§9). Template IDs are **not** stamped into records. That would make payloads unrealistic and hand the detector the answer.

**Run identity:** every resource carries `drishti.loadgen.run_id`. This is enough for v1. Full per-record identity is deferred (§12).

**Marshaling:** v1 sends pre-built proto messages, and gRPC marshals each one on every send. Profile the generator at a high rate before optimizing. The pass-through codec is deferred (§12).

**Compression:** `-gzip` defaults to **on**, matching the collector's OTLP exporter. Decompression is real work the receiver does in production, so a benchmark without it overstates throughput. The compression setting is reported with every number.

**Timestamps:** pool payloads carry the timestamp from when they were generated. With `-stamp` (default on since Week 2) every send is a shallow copy with record timestamps set to the send time and the resource tagged `drishti.loadgen.seq` (§12, built). `-stamp=false` sends pool requests as built, as in Week 1; receiver-only sweeps use it so their numbers stay comparable with Week 1's.

## 7. Partition accounting

**Decision:** for each run, the generator computes the expected key → partition mapping and checks it against actual Kafka offset deltas.

- **No hash is reimplemented.** A shared internal package exports **one constructor for the `kgo.Partitioner` the receiver is configured with.** The receiver passes it to its client. The generator predicts partitions with `partitioning.ForKey` on the same constructor. (Not `ForTopic(topic).Partition(record, n)`: franz-go's default uniform-bytes partitioner implements only `PartitionByBackup`, and its plain `Partition` panics by design. For keyed records `PartitionByBackup` hashes the key and returns before reading the backlog, so the answer is deterministic.) The hash, positive mask, and modulo all come from one implementation. The receiver's partitioner must be set explicitly through this constructor, not left to the library default.
- **The explicit constructor must reproduce the current default behaviour exactly** (murmur2 for keyed records), so the key → partition mapping doesn't change for data already in Kafka. The pinned pairs below are the proof. This change is **its own receiver commit, with the test, landing before any loadgen code.**
- **Partition count is read from topic metadata via `kadm`, never from a flag.** If a topic is recreated with a different count, a flag would give a confident, wrong cross-check.
- **Test pinned to observed ground truth:** in the 10 Oct smoke run, `smoke-svc-1` and `smoke-svc-2` went to p0 and `smoke-svc-0` to p2 (keys from `cmd/otlpsmoke`). The test asserts these pairs against the shared partitioner. They're used because they were observed in Kafka, not computed.
- **Offsets come from the generator itself:** `kadm` lists end offsets per partition before and after the run. No shell output is parsed and there's no manual step.

**Reported:** expected messages per partition, actual deltas, and the max/min spread. If the default run is already skewed, the `-services 1` comparison has no clean baseline, so the spread is always visible.

## 8. Deployment: the generator runs as a container

**Decision:** the generator runs **in a container on Drishti's default Compose network**, targeting `receiver:4317` (OTLP) and `kafka:29092` (kadm offsets).

**Why:** run natively in WSL, the generator would reach the receiver via `localhost:14317` through Docker Desktop's port forwarding from WSL into the Docker VM. Real telemetry never takes that hop, since the collector talks container-to-container. The forwarding path could become the bottleneck and get blamed on the receiver.

**Network split (prerequisite):** benchmarks require the OTel Demo to be stopped (§11). But `make stop` in the demo removes the `opentelemetry-demo` network. A receiver that always attaches to that external network can't start without the demo. Creating the network by hand is fragile, because the demo's Compose may then reject a network that exists without its labels. The fix:
- The base `docker-compose.yml` keeps the receiver on Drishti's default network only. Benchmarks use this.
- An override file, `docker-compose.demo.yml`, adds the external `opentelemetry-demo` attachment and the `drishti-receiver` alias. The top-level `external: true` network definition moves into the override as well.
- Demo runs use `docker compose -f docker-compose.yml -f docker-compose.demo.yml up`.

**Merge trap:** a service with no `networks:` key is implicitly on `default`. But a service that declares `networks:` is attached **only** to the networks it lists. If the override adds `networks:` to a receiver whose base has none, the merged service lands on `opentelemetry-demo` alone. It then can't resolve `kafka:29092`, and every export fails. **Fix:** the base file declares the receiver's network explicitly, `networks: { default: {} }`. Service-level network maps merge across files, so the override adds the demo network on top. **Verification for that commit:** `docker compose -f docker-compose.yml -f docker-compose.demo.yml config` must show the receiver on both `default` and `opentelemetry-demo`, and plain `docker compose config` must show `default` only.

**Every Compose command in a demo session needs the same file pair** (`up`, `logs`, `ps`, `stop`). Otherwise Compose sees a different config, and a stray plain `docker compose up` quietly recreates the receiver without the demo network. For demo sessions, set `export COMPOSE_FILE=docker-compose.yml:docker-compose.demo.yml` (or use a Makefile target), and `unset COMPOSE_FILE` before benchmarking. `bench.sh` pre-flight treats a set `COMPOSE_FILE` that includes the demo override as a failure.

This confines the "start the demo first" ordering trap to demo runs only. It's a small infra commit, landing after the partitioner commit and before any loadgen code. **The same commit updates PROGRESS.md's startup-order line and the `docs/test-harness.md` open item**, so the docs and the compose files never disagree.

**Consequences:**
- `containers.csv` covers the generator too. A native process would be invisible to `docker stats`.
- `run.json` also records the generator's **self-reported CPU** (process rusage plus Go runtime metrics), regardless of deployment.
- The generator image is labelled with the git commit it was built from, using a build arg.
- **One footnote measurement:** run the native path once at the same rate to quantify the forwarding overhead.

## 9. Output: what each run saves

**Rule:** every quotable number comes from a saved run, and the **verdict is computed by the tool**. Nothing is reconstructed from memory or terminal scrollback.

One directory per run: `bench/runs/<UTC-timestamp>-<run_id>/`

| File | Contents |
|---|---|
| `run.json` | All flags; full shape (signal, batch size, K, services, series, points, body range, attrs, compression, conns, in-flight cap); thresholds used; warm-up and steady-state durations; summary results; generator self-reported CPU; verdict with reasons |
| `env.json` | CPU model, nproc, WSL memory, AC vs battery, Windows power mode, Docker/Compose/Go versions, receiver and generator image digests, Drishti git commit plus **dirty flag** |
| `intervals.csv` | Per second: offered, achieved, non-OK by code, saturated, max schedule lag, p50/p99/p99.9 for that interval (for plotting only, never averaged) |
| `latency.hlog` | **Encoded** steady-state histogram (hdrhistogram-go Encode, base64). This is the source of truth; percentiles can be recomputed without rerunning |
| `latency.hgrm` | Human-readable percentile printout of the same histogram |
| `warmup.hlog` | Encoded warm-up histogram (reported, not judged) |
| `partitions.json` | §7: expected mapping, expected messages per partition, actual deltas, spread |
| `templates.json` | Drain manifest: template ID → string, count this run (logs runs) |
| `containers.csv` | Sampled CPU and memory per container, generator included |

**Promotion:** `bench/runs/` is gitignored. To quote a number, copy its run directory into `bench/results/` and commit it. Every number in the README links to a committed `run.json`. **A run from a dirty tree can't be promoted.** This makes "only real, measured numbers" a mechanical rule.

## 10. Verdict, repeats, and the sustainable rate

**Per-run verdict:** PASS only if, over the steady-state window:
- achieved ≥ offered within tolerance,
- **zero non-OK responses of any gRPC code** (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `INTERNAL`, or anything else),
- zero saturated,
- max schedule lag under its threshold,
- p99 under its bound.

A FAIL lists **every** condition that broke, with non-OK counts **broken down by code**, e.g. `["non_ok: UNAVAILABLE=37 DEADLINE_EXCEEDED=4", "saturated=412", "schedule_lag_max=180ms"]`. That list is the first clue to the bottleneck. Breaking down by code means a new failure mode can never pass silently. (Requests that time out during the end-of-window drain appear here as `DEADLINE_EXCEEDED`.)

**Repeats:** each rate is run **at least 3 times**. The Core 7 240H is a hybrid CPU (P-cores and E-cores), and laptop power state and thermals move results noticeably. Report the median with min and max.

**Verdict at a rate:** PASS only if **all** repeats pass. A rate that passes 2 of 3 is the **knee**, not a result.

**Sustainable throughput** = the highest offered rate that passes. Find it by sweeping rates, starting near the closed-loop probe's estimate. The knee is the first bottleneck, which is the Week 2–3 story.

## 11. `scripts/bench.sh`: pre-flight and capture

The generator stays pure: it applies load, measures, and writes files. The wrapper script does everything else.

**Pre-flight (refuses to run on failure):**
- No OTel Demo, FundKit, or Sankalp containers running. This enforces the benchmark rule.
- Drishti was started from the base `docker-compose.yml` only, without `docker-compose.demo.yml` (§8), so the receiver isn't attached to the demo network.
- Receiver is healthy (gRPC health check).
- Both topics exist with the expected partition count.
- The generator image's commit label matches the commit being recorded.
- A dirty tree may run, but `env.json` marks it, and it can't be promoted.

**Capture:** environment for `env.json` (including power state), background `docker stats` sampling into `containers.csv`, and running the generator container N times per rate.

**Built so far (Week 2): `scripts/sweep.sh` + `scripts/sweep-summary.py`**, the capture half: N repeats per rate, `docker stats` sampled during every run (`stats/<label>.csv`, ~2 s resolution, the cost of one `--no-stream` snapshot), run directories copied out, and `summary.md` with per-container CPU over the **steady window only** and the per-rate verdict above (a repeat with no `run.json` makes the rate INCOMPLETE rather than being judged on the rest). Pre-flight and `env.json` are still the driver script's job, so sweeps stay internal until `bench.sh` exists.

## 12. Deferred, with trigger

Each item is built only when its trigger appears.

| Item | What it is | Trigger to build it |
|---|---|---|
| **Per-send patching** (**built, Week 2**, `internal/loadgen/stamp.go`) | Shallow copy of each pooled payload at send time; the pool is never mutated (concurrent sends share it) | Week 2, when ClickHouse is in the benchmark path |
| ↳ Timestamp restamping (built) | Record timestamps = send time | Stale timestamps land in one old time partition or get expired by TTL on arrival, which distorts storage numbers |
| ↳ Per-send identity (built) | `drishti.loadgen.seq` (send counter, int) on every resource. With 1 resource/request, `(run_id, seq)` is one Kafka message. Positions (`resource_idx`, `record_idx`) are **not** stamped: the check is records per `(run_id, seq)` against records per request, which catches loss (missing seq, short count) and duplicates (long count) as long as a duplicate is a whole message, which is what Kafka at-least-once produces | End-to-end zero-loss/zero-dup checks. Counting per `run_id` can't tell one loss plus one duplicate from a clean run |
| **Pass-through codec** | Pre-marshal each pooled request to `[]byte` once and send it with `grpc.ForceCodecV2`, so generator marshal cost is near zero | The generator profile shows proto marshaling, **or** schedule lag grows at high rates while the receiver is healthy |
| ↳ Pre-compressed payloads | Pre-gzip bytes once. The generator registers its own compressor under the name `"gzip"` (identity on compress) so the header still says gzip and the receiver decompresses normally. This overrides the standard gzip compressor **process-wide**: acceptable in a dedicated generator binary, and must be documented. Works only with `-stamp=false` (stamped payloads change per send), i.e. for receiver-only sweeps | The profile shows gzip compression cost in the generator. **Triggered 20 Oct:** at 150k, `flate.(*fastEncL6).encode` alone is 56% cumulative of the generator's CPU (level-6 gzip), proto marshal 12.7%. The generator used 2.9–4.4 cores from 150k to 225k on the same laptop as the receiver, so it moves the measured knee (`docs/receiver.md`) |
| **Receiver shape histograms** | Records-per-request and resources-per-request histograms in the receiver, per signal | Week 3 bottleneck analysis needs per-request shape under load. Not built for Week 1, because every commit before loadgen code delays the first number |
| **Connection pool > 1** | Already a flag; this row covers choosing a non-default value | Throughput plateaus while receiver and generator CPU are idle |

## 13. Values to set before the first quoted run

These thresholds go into `run.json` and must be chosen and justified before any number is quoted:

- Achieved-vs-offered tolerance
- Max schedule lag
- p99 bound for a PASS
- Steady-state duration (and confirmation that 10s of warm-up is enough)
- Default batch size (records per request). Settled below; values marked provisional are recorded as such in `run.json`.

### Batch size: sources and status

**Context.** A `batch` processor is **confirmed absent** in demo 3.1.0 (`otelcol-config.yml`: logs `[resource_detection, memory_limiter, transform/sanitize_logs]`, metrics `[resource_detection, memory_limiter]`). The exporterhelper README (main branch) says sending-queue batching is off unless configured, which is not yet verified for 0.159. If it's off, the collector applies no size cap of its own and forwards roughly what it receives.

**Logs default = 512 records per request (provisional).** Source: the OTel Logs SDK spec, Batching processor, `maxExportBatchSize` defaults to 512, with `scheduledDelayMillis` 1000 and `maxQueueSize` 2048. Under sustained load, SDK batches converge to this size cap, which makes it the defensible default for throughput runs. The demo's services use several language SDKs, and any of them may override this (e.g. via `OTEL_BLRP_*` env vars). The observed maximum from the measurement below catches that.

**Metrics default: taken from the observation directly.** Scrape and SDK-collection output has no equivalent size cap.

**Observed demo shape (measurement, zero receiver code).** The collector's `debug` exporter sits in the same logs and metrics pipelines as `otlp_grpc/drishti`, so each batch fans out to both exporters. At `basic` verbosity (the base config defines `debug:` bare), it logs one line per batch with counts.
1. Verify that 0.159's `basic` output includes per-batch counts: `docker logs otel-collector --tail 20`.
2. Run the demo with Locust at its default user count for about 5 minutes: `scripts/measure-batch-shape.sh 300` records Kafka offsets before and after, saves the collector logs for the window, and prints steps 3–4.
3. From the collector logs, take the distribution (median, p90, max) of records per batch and resources per batch, separately for logs and metrics (data points per batch for metrics).
4. **Loss/duplication sanity check:** resources per batch, summed over a fixed window, should roughly equal the Kafka offset delta for that topic over the same window, since one Kafka message is one resource. This checks the collector → receiver → Kafka path for loss or duplication. **It does not detect re-batching.** A merging queue appends each request's resource entries into a larger request without combining them, so the resource total is unchanged. Only size-based splitting changes it, and splitting pushes the total up, not down.

**What these numbers are:** the **pipeline batch shape**, meaning what the collector's pipeline hands to its exporters, before any exporter queue. They are **not** the receiver's request shape. Whether 0.159's sending queue re-batches is **unverified**. The deferred receiver shape histograms (§12) are what would settle it. Nothing depends on this now: the logs default comes from the SDK spec, and the observed shape is context only.

**SDK override check** (run before trusting the observed maximum):
```
grep -rn "OTEL_BLRP" ~/otel-demo/.env ~/otel-demo/docker-compose*.yml
grep -rniE "max_?export_?batch_?size" ~/otel-demo/src
```
The first catches env-var overrides; the second catches batch sizes set in code when a service builds its processor. No hits in either means the spec defaults apply, subject to each language SDK's conformance.

**Expected outcome:** at the demo's rate, pipeline batches are well under 512 because they are flushed by time, not size. The observed shape is quoted as context, not used as the default.

### Observed (13 Oct 2026)

Demo 3.1.0 `minimal-no-o11y`, Locust 5 users, 300 s window, collector logger sampling **off**. A first run with sampling on showed exactly 300 `Logs` lines (10 per 10 s tick) against 2,366 Kafka messages and was discarded; see `docs/test-harness.md`.

| | Logs | Metrics |
|---|---|---|
| Batches in 300 s | 2,414 (8.0/s) | 238 (0.8/s) |
| Records / data points per batch | median **3**, p90 15, p99 36, max **56** | median 37, p90 436, max 438 data points |
| Resources per batch | **1 always** | median 1, p90 24, max 53 |
| Total rate | ~49 log records/s | ~94 data points/s |
| Loss/dup sanity (resources vs Kafka delta) | 2,414 vs 2,428 | 1,600 vs 1,560 |

SDK override check: no `OTEL_BLRP_*` in `.env*` or `compose*.yaml`, and no batch size set in code. One service (`quote`, PHP) reads `OTEL_BLRP_SCHEDULE_DELAY`, which is the flush interval, not the size, and nothing sets it. So the 512 cap holds for every service, subject to each SDK's conformance.

**Reading it:**
- **Logs: confirmed time-flushed.** No batch got anywhere near 512 (max 56), so the default stays **512, provisional**, with the SDK spec as the source and the observed shape as context.
- **Logs: one resource per request.** Every pipeline batch carried exactly one resource. Without a `batch` processor, the collector forwards each SDK export as it arrives, and an SDK export is one service. The generator's default (`-services 20`, so 20 resources in every request) does not match this. It turns one request into 20 small Kafka messages instead of one large one. **Open decision** (below).
- **Metrics: two populations.** About half the batches are SDK pushes (1 resource, tens of points). The rest are scraper batches (≈24–53 resources, ≈436 points; docker_stats/host_metrics-style). The metrics default should model both and is set when metrics are added to the generator, from this table.
- **Scale:** the 100k records/s Pass 1 runs are about 2,000× the demo's log rate at 5 Locust users. The demo is for realism and labelled incidents; volume comes from the generator, as `docs/test-harness.md` says.

**Decided (13 Oct): resources per request defaults to 1** (§6), services rotating across requests, so the default request shape matches observed collector traffic. 20 stays a sweep point. This changes Kafka message size (one ~246 KB message per request vs twenty ~12 KB ones), so the first Pass 1 runs (20 resources per request) are not comparable with runs after the change; `run.json` records the value.

**Week 3:** batch size becomes a sweep parameter (e.g. 64 / 512 / 2048), so the default needs a defensible source, not a perfect one.
