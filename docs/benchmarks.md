# Benchmark report v1 (Week 3, 21–23 Oct 2026)

**Status: internal.** Every number here was measured on one laptop, with provisional PASS/FAIL thresholds (`docs/loadgen.md` §13) and no Pass 2 metadata yet (`partitions.json`, per-container `containers.csv`). Nothing here goes on a résumé or README until those are settled. What the report does claim: each number was measured, repeated (×3 unless stated), and the bottleneck behind it is identified.

Raw data: `Downloads/drishti-week3/results-3b/` (driver `week3b.sh`, summary `WEEK3-RESULTS.md`). Design records: `docs/receiver.md`, `docs/storage.md`, `docs/loadgen.md`.

## Setup

| | |
|---|---|
| Machine | HP Victus 15, Intel Core 7 240H (6P + 4E cores, 16 threads), WSL2 with 15 GiB, Docker Desktop 4.85 |
| Stack | Receiver (Go, gRPC OTLP) → Kafka 4.3.1 (1 broker, 6 partitions, RF 1) → storage consumer (Go) → ClickHouse 26.8.21. All on the same laptop as the load generator |
| Code | `2c42320` (patches 17–24 on top of Week 2) |
| Load shape | OTLP logs, 512 records per request, 1 resource per request (matches the demo collector), 20 services, ~477 B per record uncompressed, gzip on the wire |
| Power | Recorded per step (`env.json`). Steps marked † started on battery and ended on AC; the exact switch time is unknown |

## Results against the project targets

| Target (`PROJECT_CONTEXT.md` §6) | Week 2 | Week 3 | Status |
|---|---|---|---|
| Sustained ingestion 50k events/s (stretch 200k) | receiver 175k; end to end ≈ 70k | receiver **275k** (×3), knee 275k–300k; end to end **100k keeps up** (×3), storage ceiling ~125–142k rows/s | met, stretch met at the receiver |
| Event → queryable p99 < 5 s | 2.64 s at 50k | **2.04–2.05 s at 50k, 2.06–2.75 s at 100k** (×3 each) | met up to 100k |
| Burst 3× for 60 s, zero loss, backlog cleared in minutes | not measured | 0 loss / 0 dup ×3; backlog back to normal **24–35 s** after the burst | met on the storage side (see caveat) |
| Component crash: no loss, automatic recovery | 0 loss; ~47 s without inserts | 0 loss; **10.3 s** without inserts after `kill -9` + restart; replay skip exercised live | met |
| Event → live dashboard, event → alert, query p99, agent targets | — | — | Weeks 4–7 |

## 1. Storage: background inserts (patch 17)

**Before:** the consumer's loop was serial. The 21 Oct `loop` instrumentation (patch 16) showed it at the ~70k ceiling: 0% waiting for Kafka, ~50% decoding and encoding, ~50% standing still while ClickHouse wrote. Inserts now run in the background, one per partition, in offset order (`docs/storage.md` decision 6).

† Storage step (21 Oct). Generator with `-stamp` (needed for the per-record loss check). 70 s runs.

| Offered rec/s | Loss / dup (per request) | Queryable p50 / p99 | Catch-up after the generator stopped | Peak rows/s (10 s window) | Loop: add / flush | Consumer cores |
|---|---|---|---|---|---|---|
| 50k ×3 | 0 / 0 | 1.07 s / 2.04–2.05 s | 3 s | 51k | 33% / 0% | 0.44 |
| 100k ×3 | 0 / 0 | 1.01–1.10 s / 2.06–2.75 s | 3 s | 103–110k | 73–77% / 0% | 0.76–0.86 |
| 150k ×3 | 0 / 0 | 3.2–5.9 s / 35–44 s | 18–25 s | 125–139k | 99% / 0% | 0.99–1.01 |
| 200k ×3 | 0 / 0 | 8.3–16.7 s / 49–57 s | 31–44 s | 135–142k | 97–99% / 0% | 0.96–1.03 |

- **Every acknowledged record stored exactly once in all 12 runs.** `system.part_log`: 2,804 inserts, none wrote more than one part, so each insert stayed atomic with background inserts too.
- **The ceiling moved from ~70k to ~125–142k rows/s, and the limit changed.** The loop no longer waits for ClickHouse (flush 0%). It spends ~99% of its time decoding and encoding on one goroutine (1.0 core). Profile at 150k: `proto.Unmarshal` 61%, `AppendLogs` (RowBinary) 19%, allocation (`mallocgc`) 21%.
- **Generator caveat:** at 150k and 200k the load generator (with `-stamp`, ~3.3–4.1 cores) could not keep its schedule next to the rest of the stack (saturated requests; 200k delivered 155–174k). The storage numbers count what was actually acknowledged.
- **Next bottleneck: one decoding goroutine.** Options: decode per partition in parallel (6 partitions → up to 6 cores), or decode less (skip fields the row doesn't need).

## 2. Failover (patch 18)

On AC, 50k with `-stamp`.

| Run | Loss / dup | Longest gap between inserts | Notes |
|---|---|---|---|
| `kill -9` at ~25 s, restart 5 s later | 0 / 0 | **10.3 s** (Week 2: ~47 s) | Includes the deliberate 5 s pause before the restart and container start. Static membership: the restart takes its predecessor's place at once instead of waiting out a 45 s session |
| Debug crash right after an insert succeeds, before its commit (`-debug-crash-after-inserts 60`) | 0 / 0 | 6.9 s | Exit code 3 as designed; the restart **skipped 70 replayed records** that were already stored. First live run of the resume skip (Week 2 kills never landed in that gap) |

## 3. Receiver knee (patches 19, 21, 23)

Receiver only (storage stopped), pre-compressed payloads (`-precompress -stamp=false`): the generator used ~0.3–0.5 cores instead of 3–4, so the knee measures the receiver.

| Rate | Result | p99 | Receiver cores | Power |
|---|---|---|---|---|
| 175k | PASS, PASS, FAIL | 31–38 ms (the FAIL ran across a laptop sleep: 98 min instead of 70 s; discarded) | 2.6 | † |
| 200k | FAIL (`saturated=13`, the first run after the sleep), PASS, PASS | 35–585 ms | 2.6–2.8 | † |
| 225k | PASS ×3 | 43–437 ms | 3.0–3.1 | † |
| 250k | PASS ×3 | 144–285 ms | 3.3 | † |
| 275k | PASS ×3 | 176–289 ms | 3.4–3.5 | † |
| 300k | **FAIL ×3** (achieved 281–294k, saturated 685–2,192) | 0.87–1.28 s | 3.4 | AC (23 Oct) |

- **Sustainable: 275k (×3). Knee: between 275k and 300k.** Week 2: sustainable 175k, knee 200k.
- **Receiver CPU at 150k:** 2.34 cores sampled by pprof (Week 2: 3.73), 1.78 cores in docker stats with lz4 (Week 1 sweep: 3.05). The profile shifted: proto unmarshal is now the largest share (39%, was 23.7%), lz4 compression 22% (was 15.2%), gzip decode 18% (was 24.7%, including 9.7% of byte-at-a-time reads that patch 19 removed). The re-encode is gone from the profile (pass-through, patch 21).
- **What limits it at 300k is not CPU:** the receiver used ~3.4 of 16 threads and the generator ~0.4. Every run here used **one gRPC connection** (one HTTP/2 transport). Hypothesis for Week 4: `-conns > 1` in the generator (the deferred §12 item whose trigger, "throughput plateaus while receiver and generator CPU are idle", has now fired).
- † The 175k–275k sweep started on battery and was on AC by its end; a laptop sleep interrupted it at 175k. 300k was measured on AC. Re-running 250k/275k fully on AC would remove that doubt.

## 4. Kafka compression (patch 20)

On AC, 150k, receiver only, ×3 per codec. Each run checked the codec the receiver actually ran with (the first attempt, 21 Oct, silently measured lz4 four times: `docker compose run loadgen` recreated the receiver with the default; fixed with `--no-deps`).

| Codec | Receiver cores | Kafka bytes per log record | p50 | p99 |
|---|---|---|---|---|
| **lz4 (kept)** | 1.78 | 258 | 14.5 ms | 21–22 ms |
| zstd | 2.15 (+21%) | 159 (−38%) | 22.5 ms | 55–66 ms |
| snappy | 1.81 | 264 | 14.2 ms | 21 ms |
| none | 1.67 (−6%) | 477 (+85%) | 11.3 ms | 17–18 ms |

- **lz4 stays.** zstd saves 38% of Kafka disk and network for 21% more receiver CPU and ~3× the p99. It becomes the better choice if Kafka disk or network is the limit, which on one laptop with 24 h retention it isn't.
- "none" saves only 6% CPU despite lz4 being 22% of the receiver profile: producing 85% more bytes costs CPU too (bigger produce requests, more copying).
- These are synthetic bodies (random hex). Real demo logs compress far better (section 7), so the absolute byte counts here overstate real traffic.

## 5. Burst (patch 24)

On AC, ×3. 50k base, 150k (3×) for 60 s, storage on, `-stamp`.

| Run | Loss / dup | Peak backlog | Back to pre-burst lag after the burst | Generator |
|---|---|---|---|---|
| 1 | 0 / 0 | 4,032 messages (~2.1M rows) | 24 s | PASS; burst p99 776 ms |
| 2 | 0 / 0 | 3,567 (~1.8M rows) | 24 s | FAIL: 8.79M of 9.00M burst records offered (saturated); burst p99 2.2 s |
| 3 | 0 / 0 | 4,561 (~2.3M rows) | 35 s | FAIL: `saturated=114`; burst p99 1.65 s |

- **Storage side: met.** Nothing lost or duplicated, and the backlog cleared in 24–35 s.
- **Ingest side: partly.** In 2 of 3 runs the generator could not send every burst request. "Saturated" requests were never sent, so they are not lost acknowledged data. But the receiver's p99 rose to 1.6–2.2 s during the burst, with the stamping generator (~3 cores), the consumer, ClickHouse and the receiver all on one laptop. The receiver alone sustains 275k (section 3), so this is CPU contention on a shared machine, not a receiver limit.

## 6. Hot partition (patch 22)

OTel Demo, 10 min, on AC. Messages per partition:

| Topic | Before (10 Oct) | After the key fallback (23 Oct) | Busiest key now |
|---|---|---|---|
| `otlp.metrics` | partition 1: ~90% (`unknown_service`) | largest partition **50%**, others 5–13% | `docker-desktop` (`host.name` fallback): every host metric comes from one host |
| `otlp.logs` | partition 0: ~50% | partition 0: 51% | `product-catalog`, `frontend-proxy`: real per-service volume, not a missing name |

- The fallback spread the scraper metrics (`docker_stats` per container now has its own key) but cannot spread one host's metrics or one noisy service's logs without giving up per-key ordering. Accepted for now: at demo volume (~35 log records/s) no partition is near a limit. Revisit if the detector (Week 4) needs more parallelism than per-service ordering allows.

## 7. Compression on real logs

20,728 demo log records in ClickHouse (14 services, ~35/s):

| | Real demo logs | Synthetic benchmark (Week 2, 21M rows) |
|---|---|---|
| Bytes per row on disk | **65 B** (948 B uncompressed, 14.6×) | ~132 B |
| `body` | 10.7× | 2.7× |
| `resource_attributes` | 36× | 32× |
| `log_attributes` | 27× | 3.7× |
| `trace_id` / `span_id` (hex `String`) | 3.4× / 3.0× | — |

- **Synthetic data overstates storage cost about 2×**, because the generator fills bodies and attributes with random hex. Storage capacity estimates should use the real figure.
- `trace_id` and `span_id` are hex strings and compress poorly. Storing them as `FixedString(16)` / `FixedString(8)` binary would halve them before compression; decide when the agent's query patterns (Week 5) show whether they are filtered on.
- Small sample (one 10-minute run, small parts), so ratios may improve with larger merged parts.

## Bottlenecks, in order

1. **Storage decode on one goroutine** (~125–142k rows/s at 1.0 core): parallel decode per partition.
2. **Receiver at 275k–300k with idle CPU:** test more than one gRPC connection.
3. **One laptop for everything:** in full-pipeline runs above ~150k the stamping generator competes for CPU with the system under test. Results above that rate need the generator on another machine, or `-precompress` (which can't check per-record loss).

## What went wrong while measuring (kept so it isn't repeated)

- Two driver hangs: a bare `wait` also waited for the CPU sampler. Then the fix didn't reach the laptop: the file transfer re-sent a cached older copy. Since then every delivered file is checked by checksum.
- `grep -q` inside a pipeline under `set -o pipefail` made successful commands look failed (SIGPIPE).
- The first codec comparison measured lz4 four times (compose recreated the receiver).
- The demo consumer group kept offsets from an earlier run and read a day of backlog. Now a new group and an empty table every run.
- Several steps ran on battery or across a sleep. The driver now records the power state and keeps Windows awake.
