# Storage consumer: Kafka → ClickHouse

`cmd/storage` consumes `otlp.logs` (consumer group `drishti-storage`), flattens each OTLP log record into a row, and inserts batches into `drishti.logs` (`clickhouse/schema/001_logs.sql`). Every Kafka record lands in the table **exactly once**, without Kafka or ClickHouse transactions.

```
otlp.logs (6 partitions) ──► poll ──► per-partition batch (RowBinary bytes) ──► queue ──► HTTP INSERT ──► part
          ▲                                                       (one in flight per partition) │ 200 OK
          └── loop keeps polling while inserts run          commit offset last+1  ◄────────────┘
```

| File | Job |
|---|---|
| `internal/storage/consumer.go` | poll, batch, background inserts, commit, rebalance callbacks, resume |
| `internal/storage/loopstats.go` | the `loop` line: where each 10 s window went |
| `internal/storage/batch.go` | one partition's batch; flush limits |
| `internal/storage/rows.go` | OTLP → row (column order, timestamp fallbacks, attribute flattening) |
| `internal/storage/rowbinary.go` | RowBinary encoding |
| `internal/storage/clickhouse.go` | HTTP insert, progress query, startup check |

## Decisions

### 1. Exactly-once into ClickHouse: the offsets live in the part

The receiver is at-least-once and Kafka consumers are at-least-once, so the storage write has to absorb replays. The usual answers and why they weren't enough on their own (lab §8):

| Option | Gap |
|---|---|
| `ReplacingMergeTree` | Dedups only on merge or with `FINAL` (read-time CPU); duplicates are visible until then, and every count query must remember `FINAL` |
| `insert_deduplication_token` alone | Works only if the replayed batch is byte-identical. After a crash the new consumer reads a backlog and cuts **different** batches (time-cut batches become count-cut), so the token never matches |
| Kafka transactions / an offsets table | ClickHouse can't join a Kafka transaction; a separate offsets table is a second write that isn't atomic with the data |

**Chosen:**
1. Every row carries `kafka_partition`, `kafka_offset`.
2. One insert = one Kafka partition's contiguous offset range **and** one table partition (day of `received_at`). Such an insert is one block → one part, and a part is atomic: all its rows (and their offsets) become visible together or not at all.
3. The Kafka offset is committed only **after** ClickHouse answers 200.
4. On partition assignment the consumer asks the table `max(kafka_offset)` per partition and drops replayed records at or below it.

Crash between 2 and 3 → the replayed records are already in the table → skipped. Crash before 2 → nothing stored, nothing committed → re-read. A failed commit costs nothing but a skipped replay.

**Why the resume query is cheap:** an aggregate **projection** (`ingest_progress`: `max(kafka_offset) GROUP BY kafka_partition`) is stored inside every part, built atomically with it. `EXPLAIN` shows `ReadFromMergeTree (ingest_progress)`: 1 granule on ClickHouse 26.9 (chdb), 17 granules for 17 parts on 26.8.21 with ~21M rows.

**The token stays as a second layer:** it covers the one case the skip can't, an insert that timed out on our side but was written on the server, retried by the *same* process with identical bytes.

**Guard:** if the table holds an offset at or beyond a partition's end offset, the topic was deleted and recreated (offsets restart at 0) and skipping would silently drop every new record. The consumer refuses to start (`topic recreated?`).

**What would break it, stated so it can be checked:**
- An insert split into several blocks. ClickHouse emits a new block at `min_insert_block_size_rows` (1,048,449) **or** `min_insert_block_size_bytes` (256 MiB). `Limits.Validate` keeps batches below both (rows < 1,048,449; bytes ≤ 200 MiB, one message of margin). The Week 2 driver checks `system.part_log` for any insert query that created more than one part.
- `async_insert`. The server buffers and merges inserts, so "200 OK" no longer means "this part exists". Some 26.x builds default it **on** (chdb 26.9: `async_insert = 1`). Every insert sends `async_insert=0`.
- Inserts of one partition landing out of order. Step 4 trusts `max(kafka_offset)`: if batch k+1 landed and batch k didn't, the resume would skip k's records forever. So each partition has **at most one insert in flight**, the next starts only after the previous one succeeded, and a batch that never stores drops its partition's queue (re-read after restart) instead of letting later batches past it (decision 6).
- Partitions moving between consumers while an insert is in flight. `BlockRebalanceOnPoll` limits rebalances to between loop iterations, but background inserts may still be running then. The **revoke callback waits** for the revoked partitions' in-flight inserts, commits what they stored, and drops queued and open batches; the next owner reads `max(kafka_offset)` only after that. Without the wait, an insert landing after the new owner's read is stored twice (`TestRun_RevokeWaitsForInFlightInsert` shows exactly that when the wait is removed). `OnPartitionsLost` (session already expired) waits the same way but can't commit.

### 2. Batching: per Kafka partition, cut by rows, bytes, age, or day

- **Limits:** 100,000 rows **or** 32 MiB encoded **or** 2 s after the batch's first record, whichever comes first (`-batch-rows`, `-batch-bytes`, `-batch-wait`). A message is never split: the row cut happens before a message that would cross the limit.
- **The trade-off (lab §7):** ClickHouse wants few, large inserts; freshness wants small, frequent ones. At 175k records/s over 6 partitions (~29k/s each) a partition fills 100k rows in ~3.4 s, so the **2 s age cut decides**: about 6 inserts per 2 s (~3 inserts/s) of ~58k rows. At low traffic the age cut bounds event → queryable to ~2 s + insert time. The live dashboard reads Redis (Week 4), not ClickHouse, for sub-second freshness.
- Due batches go to their partition's insert queue; inserts of different partitions run concurrently, one per partition at a time (decision 6).

### 3. HTTP + RowBinary, not the native protocol client

- Server-side insert work (parse → block → sort → compress → part) is the same. RowBinary is the cheapest row format to produce (no escaping) and to parse.
- No new dependency; the encoder is ~40 lines we can profile.
- Given up: the native protocol ships columnar blocks the server doesn't transpose, and LZ4 on the wire. Revisit if the ClickHouse-side profile shows RowBinary parsing as the bottleneck.
- **Verified:** rows encoded by `AppendLogs` were inserted into ClickHouse 26.9 (chdb) with `FORMAT RowBinary` and read back exactly: nanosecond timestamps, maps, UTF-8 bodies, the timestamp fallbacks, partition and offset.

### 4. Table design (`clickhouse/schema/001_logs.sql`)

| Choice | Reason |
|---|---|
| `ORDER BY (service, ts)` | "Service X, last N minutes" is the dominant read (dashboards, agent tools). Lab §3: 81,921 vs 5,000,000 rows read. Compression was slightly *worse* than `(ts)` on synthetic data; re-measure on real OTLP |
| `PARTITION BY toDate(received_at)` | Day partitions are for data management (DROP, TTL), not speed (lab §4). `received_at` (Kafka timestamp = receiver produce time), **not** event `ts`: a skewed sender clock or a stale payload can't scatter rows into old partitions or make one insert span days |
| `TTL received_at + 7 days` | Same reason: lab §6 showed already-expired rows are **dropped on insert**. Keyed on `ts`, a stale payload would vanish on arrival and look like loss |
| `inserted_at DEFAULT now64(3)` | Evaluated once per insert block by the server, so **event → queryable = `inserted_at - ts`** straight from SQL |
| `DateTime64(9)` + `Delta, ZSTD` | OTLP carries nanoseconds; Delta on sorted timestamps makes precision nearly free (lab §5: 20.7x) |
| `LowCardinality(String)` for service, severity, scope, event name | Lab §5: 12x smaller than plain String |
| `Map(LowCardinality(String), String)` attributes | Every key kept, no per-service schema. Typed columns for hot keys come later from real query patterns |
| Timestamp fallbacks | `ts` = time → observed → received; `observed_ts` = observed → received. A row never lands in 1970 |

### 5. Failure behaviour

| Failure | Behaviour |
|---|---|
| ClickHouse down / insert error | Same batch retried with the same token, backoff 100 ms → 5 s, forever. Offsets aren't committed, so Kafka holds the backlog (24 h retention). Consumer lag grows; nothing is dropped |
| Shutdown mid-insert | The in-flight attempt is not cancelled (`WithoutCancel` + insert timeout): cancelling it would turn a clean stop into an "was it written?" case. Shutdown cuts every open batch, gives each queued batch its attempt, stops retries only, then commits what succeeded |
| ClickHouse slower than Kafka | Pending bytes reach `-max-pending-bytes`; the loop stops polling until inserts catch up (`flush_pct` in the `loop` line). Memory stays bounded, lag grows in Kafka |
| Undecodable Kafka message | Logged with partition/offset, counted (`decode_errors`), skipped. Retrying would wedge the partition forever |
| Consumer killed (`kill -9`) and restarted | Static member (decision 7): the restart takes its predecessor's place at once; replayed records are skipped by stored offset. Measured before: ~47 s with no inserts (dynamic member, 45 s session) |
| Consumer dies for good, another member exists | Its partitions move after the session timeout, 15 s here (was 45 s) |

### 6. Background inserts (Week 3, patch 17)

**Why:** patch 16's `loop` line showed the ~70k rows/s ceiling exactly: the loop never waited for Kafka (`poll_pct` 0), and its time split ~50/50 between decode + encode (~7.4 µs/row on one goroutine) and **standing still while ClickHouse wrote** (~150 ms per iteration). Two equal halves that never overlapped: ~21k rows per ~305 ms iteration ≈ 68k rows/s.

**What changed:** the loop hands each cut batch to its partition's queue and goes straight back to polling. Each partition runs at most one insert at a time, in offset order; when one succeeds, its last record is noted for commit and the next queued batch starts. The loop commits everything stored since the last iteration (between `PollFetches` and `AllowRebalance`, as before).

| Rule | Why |
|---|---|
| One insert in flight per partition, in offset order | the resume trusts `max(kafka_offset)` (decision 1) |
| A batch that never stores drops its partition's queue and stops `Run` | nothing later may land past it; the dropped batches were never committed, so they are re-read |
| Revoke waits for in-flight inserts, then commits them | the next owner must read the table after they land |
| `-max-pending-bytes` (192 MiB): past it the loop stops polling until inserts catch up | backpressure: if ClickHouse is slower than Kafka, lag grows in Kafka (24 h retention), not in memory |
| Commits stay in the loop or the revoke callback | they never overlap, and per partition they only move forward |

**Expected:** iteration time ≈ add time only, so the ceiling moves to what one goroutine can decode and encode (≈ 1/7.4 µs ≈ 135k rows/s), minus CPU taken by the receiver and generator on the same laptop. `flush_pct` should fall to ~0 and `add_pct` rise towards 100. If ClickHouse can't keep up instead, `flush_pct` comes back as backpressure, and lag shows it.

### 7. Failover: static membership, 15 s session (Week 3, patch 18)

**Why:** the 20 Oct kill test inserted nothing for ~47 s after the restart. The restarted process joined as a **new** member, and the group waited for the dead member's 45 s session to expire before rebalancing.

| Choice | Reason |
|---|---|
| `-instance-id` (compose: `storage-1`), static membership (KIP-345) | A restart with the same ID replaces the old member at once, with no rebalance. `TestRun_StaticMemberRestartTakesOverAtOnce`: 0.1 s with a 25 s session; the same test under a new ID waits the full 25 s |
| `-session-timeout 15s` (heartbeat every 3 s) | Covers a member that dies for good while another is running. Static members don't send LeaveGroup on shutdown, so the session also decides how long a cleanly stopped, never-restarted member keeps its partitions |
| Classic group protocol, not KIP-848 | franz-go keeps classic unless `ServerSideBalancer` is set; KIP-848 is recommended from Kafka 4.3 (we run 4.3.1). It's a separate change with its own failure modes; not mixed into this one |

**What it costs:** two live processes must never share an instance ID (the newer fences the older; an insert the fenced one still has in flight could land after the new owner read the table). One replica here; scaling out needs one ID per replica (e.g. from the pod name).

**Live check of the resume skip:** `-debug-crash-after-inserts N` (`STORAGE_CRASH_AFTER=N` in compose) stops the consumer right after its Nth insert succeeds, **without committing it**, and exits with code 3, like a kill landing between insert and commit. The restart must show `skipped_replayed` > 0 and the run 0 loss / 0 duplicates. Debug only; off by default.

### 8. Parallel decode, fork-join per poll (Week 4, patch 26)

**Why:** after background inserts the 23 Oct runs reached ~125–142k rows/s with the loop ~99% in `add` and `flush` at 0%: decoding (`proto.Unmarshal`, 61% of consumer CPU) and RowBinary encoding (`AppendLogs`, 19%) on one goroutine, ~1.0 core. Everything else was already overlapped. Batches are per Kafka partition and no partition's batch depends on another's, so the parallelism was there, unused.

**What changed (`internal/storage/decode.go`):** after `PollFetches`, each partition with records gets one goroutine (at most `-decode-workers` at once, default `GOMAXPROCS`) that decodes that partition's records **in offset order** into that partition's open batch. The loop joins all of them, then enqueues the cut batches exactly as before. `-decode-workers 1` decodes on the loop goroutine with no goroutines at all: the patch 17 loop, kept as the benchmark's control.

| Rule | Why it keeps exactly-once (decision 1, decision 6) |
|---|---|
| One goroutine per partition, records in offset order, a partition never split | a batch is still one partition's consecutive records cut by the same rules (day, rows, never split a message): one insert = one part |
| Each partition's cut batches enqueued in cut order after the join | the insert pipeline (≤ 1 in flight per partition, offset order, commit after insert) sees exactly what the serial loop gave it; order across partitions never mattered |
| Fork-join between `PollFetches` and `AllowRebalance` | with `BlockRebalanceOnPoll`, franz-go runs no assign/revoke/lost callback in that window (all three go through `waitAndAddRebalance`, checked in the v1.22.1 source). `release()` never sees a half-decoded partition, so the decoders need no locking against it |
| Goroutines touch no `Consumer` map | before the fork, under `mu`, each open batch is taken out of `c.open` and the skip offset copied out of `c.skip`; after the join, under `mu`, they go back. `mu` is free during decode, so finishing inserts are not held up |
| One reusable decoded request per partition | a shared one would be a data race (`-race` catches it) |

**Alternatives considered:** a long-lived worker per partition (decode overlaps the next poll, but it adds a second asynchronous stage that revoke and backpressure would have to drain); per-message fan-out with a reorder buffer (parallelism independent of partition count, but the most new ordering code, for a ceiling we can't use with 6 partitions); scaling out to more consumer processes (no code; exactly-once already holds per partition owner; costs one process, one progress query and one instance ID per replica). The drain test measures scale-out next to it.

**Limits:**
- Parallelism is capped by the partitions in a poll: 6 here. More needs more partitions.
- Each poll is a barrier, so an iteration lasts as long as its slowest partition. When catching up, fetches are full (~1 MiB per partition), so polls are balanced; but the partition holding the most backlog drains last and alone. Total drain rate ≈ single-goroutine rate ÷ the hottest partition's share. On real demo logs p0 held 51% (Week 3), so the cap there is ~2×.
- It spends more cores; it doesn't make a row cheaper. Decoding less (skip unused fields, generated unmarshalers) is a separate change.

**Expected (before the run):**
- `-decode-workers 1` reproduces the Week 3 ceiling (~125–142k rows/s).
- At 6 workers, `add_pct` stops being the limit. The next limit is probably ClickHouse: `flush_pct` returns as backpressure at `-max-pending-bytes`, with ClickHouse CPU up. `poll_pct` rising would point at the fetch instead (`FetchMaxPartitionBytes`, `MaxConcurrentFetches`).

**Verdict (measured 25 Oct, section "Measured, 25 Oct"):**
- Exactly-once held: 0 loss / 0 dup in 21 runs.
- `-decode-workers 1` matched Week 3 (143–157k rows/s).
- ClickHouse did come back first (`flush_pct` 14–28%, `poll_pct` 1–9% with all partitions busy).
- **What the prediction missed:** each decoding goroutine got ~2.5× slower in parallel (allocation and buffer growth; heap held at `GOMEMLIMIT`). The whole drain gained 1.46×, not the decode stage's 3.2×.

### 9. One process per signal, behind a codec seam (Week 5, patch 40)

**Why:** Week 5 stores metrics (and later anomalies) next to logs. The consumer's exactly-once rules (decision 1) don't depend on what a record contains, but the code read the logs proto directly. Its per-partition state (`open`, `skip`, the insert pipelines) is keyed by partition **number**, and the resume reads one table's `ingest_progress`. One process reading two topics would collide on both.

**What changed (`internal/storage/codec.go`):** a `Codec` = INSERT column list + a per-partition `Decoder` with two steps, `Decode(value) → rows` and `Append(rowBuf, Source)`. The count comes first because a batch is cut *before* a message that would cross the row limit (a message is never split). `storage.Logs` is the old code moved behind the interface. `Config.Codec` defaults to it, and `CH.Columns` comes from the codec (`New` refuses a mismatch). Each signal runs as its own `cmd/storage` process with its own group, table and instance ID.

**The one promise a codec makes:** a record's rows are a pure function of that record. No state may carry from one record to the next. Otherwise a replayed record could produce different rows, and the dedup token and the resume skip would stop meaning "already stored". This is why metrics are stored raw, not as deltas (decision 10).

**No behaviour change for logs:** every existing test passes unchanged (including byte-identical batches vs a one-record-per-poll reference and the crash-between-insert-and-commit test), and the stored-offset skip proof was re-verified through the seam.

### 10. Metrics stored as received; deltas at query time (Week 5, patch 41)

**What:** `cmd/storage -signal metrics` (compose `storage-metrics`) stores every OTLP metric data point in `drishti.metrics` (`clickhouse/schema/003_metrics.sql`): one row per point, with its kind, temporality, monotonic flag, value or histogram arrays, attributes and Kafka offset. Exponential histograms and summaries are skipped (neither recording has any: 0 of 1.19M / 1.29M points).

**Why raw, not deltas:** the detector turns cumulative points into deltas in memory (`otlpmetrics.Deltas`). Doing that here would make a row depend on the series' previous point, i.e. on what this process had seen before. After a crash, or when a partition moves to another consumer, the first point of every series would be stored differently. That breaks the codec contract (decision 9) and with it exactly-once. Prometheus makes the same choice: counters are stored raw and `rate()` is computed when asked.

**Query-time deltas (`internal/metricsql`):** a window function over `(series_id ORDER BY ts)`, which the sort key `(service, metric, series_id, ts)` keeps sequential. The rules are the detector's, so a number the agent reads means what an alert meant:

| Point | Delta |
|---|---|
| first of its series | nothing (its total covers hours) |
| cumulative sum, start time changed or value went down | value (a reset: everything since) |
| cumulative sum, otherwise | value − previous |
| cumulative histogram, start changed or any bucket went down | the new bucket counts |
| cumulative histogram, bounds changed | nothing (can't subtract different layouts) |
| delta temporality | value as is |

Queries read 3 minutes before `from`, so the window's first point has its predecessor. Times are passed as `DateTime64(9, 'UTC')` parameters, because a bare `DateTime64` parameter is read in the server's time zone. The test found this on a server set to Asia/Calcutta: every window came back empty.

**Series identity:** `series_id = cityHash64(service, instance, metric, attributes)`.
- `instance` (service.instance.id, else container.id, else container.name, else host.name) keeps two replicas' counters apart. Otherwise their values would interleave and every switch would read as a reset.
- Attributes are written sorted by key, because ClickHouse hashes a Map in insertion order.
- `service` is the receiver's owner key (`container:email` for docker_stats); the query API maps it to `email` (Week 5 design, change 3).

**Volume (measured on the recordings with the codec):** capture B 1.29M points in 2 h 23 min, about 150 points/s. RowBinary is about 0.9 KB per row before compression, most of it resource attributes repeated per point (ZSTD columns).

**Verified (cloud VM, ClickHouse 26.9 engine via chdb behind an HTTP shim; correctness only):**
- `TestDeltas_AgreeWithTheDetectorsDeltas`: the SQL and `otlpmetrics.Deltas` agree point for point on a fixture where every rule has a point only that rule explains (72 sum, 27 histogram, 28 gauge points).
- `TestMetricsCodec_RoundTripThroughClickHouse`: the server reads back the values meant; reordered attributes = one series; two instances = two series; resume from the metrics table's projection.
- `TestRun_MetricsEndToEndCrashLeavesNoDuplicates`: kfake → consumer with the metrics codec → ClickHouse, crash between insert and commit, every point once.

These tests skip unless `DRISHTI_TEST_CLICKHOUSE_URL` is set; the Week 5 driver runs them against the laptop's ClickHouse 26.8.

### 11. Anomalies in ClickHouse (Week 5, patch 43)

**What:** `cmd/storage -signal anomalies` (compose `storage-anomalies`) stores every detector event from the `anomalies` topic in `drishti.anomalies` (`004_anomalies.sql`): the schema v1 fields as columns plus the raw JSON (so a field added later is never lost). The agent's "list anomalies" tool and the evals read alerts there, bounded by time, next to the logs and metrics they point at.

**Repeats:** delivery from the detector is at least once (its restart rewind re-sends events, and `updated` repeats while an anomaly stays open). The table stores each Kafka record once, like every table here, and readers collapse repeats by `(id, event)`. The IDs are deterministic.

**Determinism:** `series` is a Go map, and Go randomizes map order, so the codec writes its keys sorted. Otherwise the same event could encode to different bytes, and a retried insert would carry different content under the same dedup token.

**Verified (cloud VM):** codec round trip against ClickHouse 26.9. Capture 2 end to end: `cmd/detector -replay` → 1,158 events (108 opened) → `kafkadump load-events` → `eval.anomalies` → `cmd/storage -signal anomalies` → 1,158 rows, 108 opened.

## Run

```bash
docker compose up -d kafka clickhouse
scripts/create-tables.sh
docker compose up -d --build storage receiver
docker compose logs -f storage     # every 10 s: "stats" (cumulative) and "loop" (that window only)
scripts/storage-check.sh <run_id>  # after a loadgen run: loss/dup per seq, path latency
```

A new consumer group starts at the **end** of `otlp.logs` here (`DRISHTI_STORAGE_RESET=latest` in compose): Kafka holds hours of receiver-only benchmark traffic. Production would use `earliest`. To skip a backlog later: stop `storage`, then `kafka-consumer-groups.sh --group drishti-storage --topic otlp.logs --reset-offsets --to-latest --execute`.

## Verified

- `go test -race`: every record once across 6 partitions; **crash between insert and commit leaves no duplicates** (replay cut into different batches, so only the stored-offset skip can save it); failed inserts retried; day and row cuts; refusal on stored offsets beyond the topic end; Limits; attribute rendering; SQL quoting. Crash test run 30x with `-race`: stable after the shutdown fix below.
- Background inserts (patch 17): the loop polls and stores other partitions while one insert is stuck; one insert per partition, in offset order, while partitions overlap; a batch that never stores holds back its partition only; backpressure stops polling at the pending budget; a rebalance during an in-flight insert stores nothing twice (two consumers in one group). Crash/retry/every-record tests 30x and the rebalance test 10x with `-race`: stable.
- **End to end, cloud VM** (in-memory Kafka + ClickHouse 26.9 engine behind a small HTTP shim; not benchmark numbers): loadgen 10k rec/s, 22 s → 219,648 rows = ingested records, 429 seqs, none short or long, **including a `kill -9` of the consumer mid-run and a restart**. That kill landed after a commit, so `skipped_replayed` was 0; the unit test covers the gap deterministically.
- Bugs found on the way: a **data race** (`-race`, only when applying the patches to a fresh clone): `kgo.NewClient` starts group management at once, so `onAssigned` could run before `New` had stored the admin client; it now uses the client the callback is handed. `Close` hung in `LeaveGroup` (with `BlockRebalanceOnPoll` you must `AllowRebalance` first); a shutdown mid-insert returned `context canceled` (now the in-flight attempt completes); the startup check quoted the table with Go's `"…"`, which ClickHouse reads as an identifier (`UNKNOWN_IDENTIFIER`), found only end to end.

## Measured, 20 Oct (laptop, internal, 1 run per rate)

ClickHouse 26.8.21. Generator with `-stamp` (timestamps = send time, `drishti.loadgen.seq`), 512 records/request, 70 s per run (10 s warm-up + 60 s steady). Receiver, Kafka, consumer, ClickHouse and generator share the laptop. Raw data: `Downloads/drishti-week2/results/storage/`.

| rec/s | loss / dup (per seq) | event → queryable p50 / p99 | caught up after generator stopped | rows / insert | consumer cores | ClickHouse cores |
|---|---|---|---|---|---|---|
| 50k | 0 / 0 (3.50M rows, 6,835 seqs) | **1.36 s / 2.64 s** | 2 s | 19,771 | 0.46 | 0.87 |
| 100k | 0 / 0 (7.00M, 13,671) | 3.2 s / 37.5 s | 27 s | 34,481 | 0.65 | 1.38 |
| 150k | 0 / 0 (10.49M, 20,481) | 40 s / 86 s | 80 s | 35,668 | 0.65 | 1.26 |

- **Every acknowledged record stored exactly once in every run**, including the kill test.
- **The 5 s event → queryable target holds at 50k** (p99 2.64 s; the 2 s age cut plus a ~0.35 s insert). Above that the consumer falls behind.
- **Storage ceiling ≈ 70k records/s:** 7.00M rows in 97 s at 100k (72k/s), 10.49M rows in 150 s at 150k (70k/s). The same number at two offered rates means a fixed ceiling.
- **First storage bottleneck: not CPU.** The consumer averaged 0.65 cores and ClickHouse 1.3 of 16 threads while lag grew. Inserts took ~360 ms on average (`avg_insert_ms`). Hypothesis for Week 3: the loop is serial (poll → decode → **wait for the insert** → commit), and franz-go buffers one fetch at a time with a 1 MiB per-partition fetch limit, so each ~0.4 s iteration brings in roughly one fetch's worth of records. Test it by logging records per poll and loop time, then pipeline inserts (decode the next batch while the previous one is being written, commit in order) and raise `FetchMaxPartitionBytes`.
- At 150k the generator also reported `saturated=26`: the receiver was healthy at 150k alone (pprof run PASS), so this is CPU contention with ClickHouse and the consumer on one laptop.
- **Atomicity assumption held:** `system.part_log` shows 674 insert queries, **0 with more than one part**.
- **`async_insert` defaults to 1 on 26.8.21** (`changed = 0`): sending `async_insert=0` explicitly was necessary, not defensive.
- **Projection on 26.8:** the driver's `EXPLAIN` ran on an empty table (`ReadNothing`), so it proved nothing. Re-checked with data (20 Oct, ~21M rows): `EXPLAIN indexes = 1 SELECT kafka_partition, max(kafka_offset) FROM drishti.logs GROUP BY kafka_partition` shows `ReadFromMergeTree (ingest_progress)`, 17 parts / 17 granules, one granule per part. The resume query reads the projection, not the table.
- **Kill test (50k, `kill -9` at ~27 s, restart 5 s later):** 3.50M rows = every acked record, 0 short, 0 long. The replacement inserted nothing for ~47 s (the dead member's session timeout), then caught up 30 s after the generator stopped; event → queryable p50 28 s for that run. `skipped_replayed` was 0 again: no kill so far has landed between an insert and its commit, so the live replay-skip path is still only covered by the unit test. Week 3: session timeout / static membership, and a kill timed inside an insert.
- **Compression on this synthetic data** (21M rows, 2.59 GiB on disk, ~132 B/row): `body` 2.7x and `log_attributes` 3.7x (the generator pads bodies and fills attributes with random hex, which doesn't compress); `resource_attributes` 32x, timestamps 420–540x, LowCardinality columns 150–180x. Real OTLP from the demo will differ; measure it before tuning codecs.

## Measured, 21 Oct (laptop, internal, patch 16, 3 runs per rate)

Same setup as 20 Oct, serial loop (before patch 17), `loop` line on. Raw data: `Downloads/drishti-week3/results/loop/`.

| rec/s | loss / dup | queryable p50 / p99 | poll % | add % | flush % | msgs / poll | KiB per partition-fetch (cap 1024) | consumer cores |
|---|---|---|---|---|---|---|---|---|
| 50k ×3 | 0 / 0 each | 1.37–1.41 s / 2.66–2.78 s | 39–46 | 34–36 | 19–25 | 1.4–1.6 | 159–168 | 0.42 |
| 100k ×3 | 0 / 0 each | 4.0–7.1 s / 40.7–44.0 s | 0 | 47–51 | 48–50 | 33–35 | 784–808 | 0.55 |
| 150k ×3 | 0 / 0 each | 36–40 s / 81–88 s | 0 | 50–52 | 47–49 | 34–42 | 887–898 | 0.57–0.63 |

- **Ceiling explained.** At 100k and 150k the loop never waited for Kafka: the buffered fetch was always ready (`poll_pct` 0). Its time split evenly between **decode + encode** and **waiting for the insert**. Over 22 steady windows (all 6 partitions per poll): add ≈ **7.4 µs/row** (6.2–8.9), ~21k rows per poll, ~150 ms per iteration waiting for inserts (0.67 inserts/iteration × ~230 ms). 155 ms + 150 ms per 21k rows ≈ **68k rows/s**, the measured ceiling (7.0M rows in 101 s at 100k, 10.5M in 155 s at 150k).
- **The fetch limit was not the cause.** Fetches came back full (~800–900 KiB of 1024: the next whole batch didn't fit), but always before the loop asked for them. Raising `FetchMaxPartitionBytes` alone wouldn't help.
- **Insert time barely depends on rows** (~240 ms at ~20k rows per insert, ~230 ms at ~33k): per-insert overhead dominates at these sizes. At 50k, due batches of ~3 partitions were inserted together (one wait for three inserts); at the ceiling they staggered, one wait per insert.
- **My prediction before the run was wrong:** I expected poll + flush to dominate; it was add + flush. The reading table below lacked that row; it has it now.
- Fix: decision 6 (background inserts).

## Measured, 25 Oct (laptop, internal, patch 26, `1e70af8`, 3 runs per configuration)

Driver `week4a.sh`, results `Downloads/drishti-week4/results-4a/`. Laptop on AC for every run (`require_ac` before each). Core 7 240H, WSL 15 GiB, 16 threads.

**Decode stage alone** (`BenchmarkAddFetch`: 6 partitions × 6 load-generator-shaped messages of 512 rows, no Kafka or ClickHouse), median of 3:

| Workers | rows/s | vs 1 |
|---|---|---|
| 1 | 199k | 1.00× |
| 2 | 349k | 1.76× |
| 4 | 481k | 2.42× |
| 6 | 635k | 3.20× |

**Drain of a fixed backlog.**
- **Setup:** 12,999,680 rows (25,390 messages, 100k rows/s × 130 s of stamped load-generator traffic, written with storage stopped). Each drain uses a fresh table (`drishti.logs_drain`) and a fresh consumer group set to the backlog start. Configurations are interleaved across the 3 repeats.
- **Partition skew** (7 of 20 keys hash to p1): p1 35%, p5 20%, p0 15%, p2/p3/p4 10% each.

| Workers | All 6 partitions busy, rows/s (median, range) | Total drain s | Overall rows/s | Tail s (p1 alone) |
|---|---|---|---|---|
| 1 | 154k (143–157k) | 87.2 (83.6–91.2) | 149k | 37.1 |
| 2 | 210k (203–214k) | 66.9 (63.7–70.3) | 194k | 29.4 |
| 4 | 232k (230–236k) | 62.1 (60.5–63.7) | 209k | 29.2 |
| 6 | 241k (218–254k) | 59.9 (59.6–64.8) | 217k | 28.7 |

- **Correctness:** every drain stored exactly the 12,999,680 acked rows; 0 short / 0 long per sequence in all 18 drains; no insert wrote more than one part; no OOM kill.
- **Loop with all partitions busy, 6 workers:**
  - `decode_par` 5.0–5.2 of a cap of 6 (the goroutines do run together);
  - `flush_pct` 14–28%: backpressure at `-max-pending-bytes`, ClickHouse at ~4 cores;
  - `poll_pct` 1–9%.
  - At 1 worker: `add_pct` 94–100%, as in Week 3.
- **Why per-goroutine speed fell** (CPU profiles at 1 and 6 workers, 20 s each, same shape at both):
  - `proto.Unmarshal` 52–54% of samples (incl. `reflect.New` ~14%, UTF-8 validation ~12%);
  - `runtime.memmove` 18–19% flat;
  - `rowBuf.str` → `growslice` ~21%: batch buffers grow by doubling up to ~25–32 MB, copying each time;
  - `mallocgc` 19–23%, with allocator-lock time (`mheap.allocSpan`, `procyield`) appearing only at 6 workers.
  - The consumer used 2.5–2.7 cores at 6 workers, so ~28% of decode goroutine time was not on CPU.
- **Memory** (gctrace):
  - the heap sits at the `GOMEMLIMIT` goal (~562 MB) in every drain, even at 1 worker;
  - GC CPU: 0% at 1 worker, 4–7% at 6;
  - heap peak 587–593 MB at 1 worker, up to 696 MB at 6, against the 768 MiB container limit.
- **The tail is partition skew, not decode:** each fetch brings ≤ 1 MiB per partition, so all partitions drain at the same speed. p1, with 35% of the rows, finishes ~29 s after the smallest, alone on one goroutine at ~140–150k rows/s.

**Scale-out, matched decode goroutines** (3 runs each):

| Configuration | All 6 busy, rows/s | Total drain s | Overall rows/s |
|---|---|---|---|
| 1 consumer × 2 workers | 197k (195–202k) | 69.8 | 186k |
| 2 consumers × 1 worker | 217k (211–233k) | 64.3 | 202k |

Two processes beat two goroutines by ~10%. Matched goroutines, not matched resources: two processes also get twice the `GOMEMLIMIT`, twice the pending budget, two fetch sessions and two allocators. That fits the profile: in-process, decode is held back by memory and allocation, not by the fork-join design.

**Live, 100k ×3, compose defaults** (`-decode-workers` = GOMAXPROCS):
- 0 loss / 0 dup;
- queryable p50 1.00–1.07 s, p99 2.05 / 2.67 / 2.84 s (Week 3: 2.06–2.75 s);
- caught up 2–3 s after the generator stopped;
- `decode_par` 1.4–1.5: at 100k the consumer keeps up, and a poll rarely holds every partition.
- One run's generator verdict was FAIL (its receiver-side p99 1.27 s); storage stored every acked row.

**Next storage levers (not taken yet):**
- reuse and pre-size batch buffers (removes the doubling copies and the large allocations);
- decode the OTLP wire format straight into RowBinary without building protobuf structs (most of the 54%);
- then ClickHouse's insert side;
- more partitions or better key spread for the tail.

## Reading the loop line (Week 3 instrumentation)

The ~70k rows/s ceiling came with idle CPU, so the question is where the loop spends its time. `Run` is serial: **poll → add (decode + RowBinary encode) → flush (wait for every due insert) → commit**, then poll again. Every 10 s the consumer logs a `loop` line covering only that window (`internal/storage/loopstats.go`), next to the cumulative `stats` line. At startup it logs `fetch limits` (the client's real `FetchMaxBytes`, `FetchMaxPartitionBytes`, `MaxConcurrentFetches`, `poll_wait`).

| Field | Meaning |
|---|---|
| `poll_pct` / `add_pct` / `flush_pct` / `commit_pct` / `other_pct` | share of the window's wall time in each phase; serial loop, so they sum to 100 |
| `msgs`, `msgs_per_poll`, `max_msgs_per_poll` | Kafka records (one OTLP resource, 512 log rows at the benchmark shape) per non-empty poll |
| `parts_per_poll` | partitions with data per non-empty poll (6 = every partition had data) |
| `kib_per_part_fetch` vs `cap_kib` | compressed KiB a partition brought per fetch, from franz-go's `OnFetchBatchRead` hook, against `FetchMaxPartitionBytes` (default 1 MiB). The broker fills a partition's share with whole record batches up to the cap, so **near `cap_kib` = fetches come back full** |
| `fetch_compressed_mib` / `fetch_uncompressed_mib` | what was fetched in the window: the lz4 ratio on the wire |
| `flushes`, `batches_per_flush`, `avg_flush_ms`, `max_flush_ms` | iterations that inserted, and how long the loop stood still for ClickHouse each time |
| `rows_per_s`, `avg_iter_ms` | rows inserted in the window per second; wall time per iteration |
| `decode_par_cap` | effective decode parallelism: mean of min(`-decode-workers`, partitions in the poll) over polls that decoded |
| `decode_par` | measured mean concurrency of the decode fork-join: summed per-partition decode time ÷ fork-join wall time. Per-partition time is wall time, so under CPU contention it includes time runnable but not running |
| `decode_slowest_pct` | share of the fork-join wall time the slowest partition took; near 100 = the join waited for one partition |
| `decode_busy_ms`, `decode_wall_ms` | the sums behind the two ratios above |

Why each fetch is bounded: with one broker, franz-go keeps **at most one fetch buffered** (`MaxConcurrentFetches` default = number of brokers). The next fetch is only requested once the buffered one has been polled. So each iteration gets at most one fetch (≤ ~1 MiB compressed per partition), and the loop's iteration time decides how often a fetch goes out.

How to read it at the ceiling (100k and 150k offered, consumer behind):

| What the lines show | Meaning | Next change |
|---|---|---|
| `flush_pct` high, `kib_per_part_fetch` near `cap_kib` | the loop stands still during inserts, and each iteration only brings in one capped fetch | pipeline inserts (keep polling and encoding while the previous batch is written, commit in offset order) **and** raise `FetchMaxPartitionBytes` |
| `poll_pct` high, `kib_per_part_fetch` near `cap_kib` | waiting on the fetch round trip, a full 1 MiB per partition each time: the fetch size is the limit | raise `FetchMaxPartitionBytes` first |
| `poll_pct` high, fetches well under `cap_kib` | Kafka has less to give than we can take; check that lag is actually growing | look at the receiver / broker, not the consumer |
| `poll_pct` ~0, `add_pct` and `flush_pct` ~50 each (**measured 21 Oct**) | serial loop: decode + encode and the insert wait take turns, nothing overlaps | background inserts (decision 6) |
| `add_pct` high (consumer near 1 core) | decode + encode on one goroutine is the limit | parallelise decode per partition, profile `AppendLogs` |
| `flush_pct` high after patch 17 | backpressure: pending bytes at `-max-pending-bytes`, ClickHouse slower than the loop | ClickHouse side: insert size, concurrency, parts/merges |
| `add_pct` high, `decode_par` near `decode_par_cap` (after patch 26) | decode runs in parallel and is still the limit: not enough cores, or one partition per goroutine isn't enough | more partitions, or decode less per row |
| `add_pct` high, `decode_par` well under `decode_par_cap`, `decode_slowest_pct` near 100 | the join waits for one hot partition | partition skew: keys, partition count |
| `add_pct` high, `decode_par` well under `decode_par_cap`, `decode_slowest_pct` well under 100 | goroutines exist but don't run: CPU contention on the machine | look at what else holds the cores (docker stats) |
| `add_pct` low, `poll_pct` rising (after patch 26) | the single buffered fetch is now the limit | raise `FetchMaxPartitionBytes` / `MaxConcurrentFetches` |

The instrumentation reads clocks four times per iteration and does not change behaviour. Every iteration is counted, empty polls included.

## Revert-proofs (break → red → revert → green)

| Mechanism | Break it | Expected failure |
|---|---|---|
| Stored-offset skip | in `decodePartition` (was `add` before patch 26), make the skip condition `false && …` | `TestRun_CrashBetweenInsertAndCommitLeavesNoDuplicates`: duplicated rows listed (verified, re-verified 25 Oct) |
| Day cut | drop `b.day != day \|\|` from the cut in `decodePartition` | `TestAdd_CutsBatchAtDayBoundaryAndRowLimit`: `[{0 2} {3 4}]` (verified, re-verified 25 Oct) |
| Row cut | drop the row condition from the cut and make `full` return false | same test: `[{0 0} {1 4}]` (verified, re-verified 25 Oct) |
| Recreated-topic guard | make `stored >= end.Offset` always false | `TestRun_RefusesStoredOffsetsBeyondTopicEnd`: `err = <nil>` (verified) |
| AllowRebalance before Close | delete it from `Run`'s defer | every `TestRun_*` hangs in `LeaveGroup` (found that way) |
| Fetch hook | drop `kgo.WithHooks(&c.fetch)` from `New` | `TestRun_LoopLineAccountsForEveryMessage`: `kib_per_part_fetch never > 0` (verified) |
| Message count per poll | stop adding `n` to `it.msgs` | same test: `counted 0 messages, want 21` (verified) |
| Window observe | drop `c.window.observe(it)` from `Run` | same test: 0 messages, no flush counted (verified) |
| Background inserts | make the loop wait for every insert each iteration (`if true { waitFor(allIdle) }`) | `TestRun_PollsAndInsertsWhileAnotherInsertIsInFlight`: `timed out ... waiting for p1 rows stored while p0's insert is in flight` (verified) |
| One insert per partition | drop the `busy` check from `startNext` and `enqueue` | `TestRun_OneInsertPerPartitionInOffsetOrder`: `partition 0 had 15 inserts in flight at once` (verified) |
| Failed batch holds back its partition | on final failure, start the next queued batch instead of dropping the queue | `TestRun_FailingBatchHoldsBackItsPartitionOnly`: `partition 0 stored 10 rows behind a failed batch` (verified) |
| Backpressure | make the pending-budget wait always true | `TestRun_StopsPollingAtPendingBudget`: `loop read 70 records while 16350 bytes were pending` (verified) |
| Revoke waits for in-flight inserts | delete the `for ps.busy { idle.Wait() }` loop in `release` | `TestRun_RevokeWaitsForInFlightInsert`: every row of the revoked partition stored twice (verified) |
| Static membership | restart under a different instance ID | `TestRun_StaticMemberRestartTakesOverAtOnce`: `timed out after 6s` (waited the 25 s session) (verified) |
| Instance ID reaches the client | drop `kgo.InstanceID` from `New` | `TestNew_GroupMembershipOptions`: `InstanceID = , want storage-1` (verified) |
| Parallel decode: a partition never split | in `addFetch`, give the second half of each partition's records its own goroutine | `TestAddFetch_BatchesDoNotDependOnPollsOrWorkers`: `workers=1 vs one record per poll: partition 0: 15 batches in the reference, 10 here` (verified) |
| Parallel decode: one decoder per partition | hand every partition `c.decoder(0)` | same test under `-race`: `DATA RACE` (verified) |
| Parallel decode: skip offset copied in | stop copying `c.skip` into the work item | same test: `skipped 0 records, want 5` (verified) |
| Parallel decode: open batch written back | don't put `w.open` back into `c.open` after the join | same test: `reference produced batches for 0 partitions, want 6` (verified) |
| Parallel decode: cut order kept | append each partition's cut batches in reverse | `TestRun_ParallelDecodeKeepsEachPartitionInOrder`: `partition 1 inserts out of order: [38 36 34 …]` (verified) |
| Parallel decode: really parallel | always take the serial path | `TestAddFetch_DecodesPartitionsInParallelUpToTheLimit`: `max partitions decoding at once = 1, want 3` (verified) |
| Parallel decode: capped at `-decode-workers` | size the semaphore 64 instead of `workers` | same test: `max partitions decoding at once = 5, want 2` (verified) |
| Codec row count drives the cut | ignore `n` in the cut condition (`b.rows+0*n`) | `TestCodecSeam_CutsBeforeAMessageThatWouldCrossTheRowLimit`: `first batch = "aaabbb" (6 rows), want "aaa"` (verified 10 Nov) |
| Columns match the codec | skip the `slices.Equal` check in `New` | `TestNew_RejectsClickHouseColumnsThatDifferFromTheCodec`: `err = <nil>` (verified 10 Nov) |
| Stored-offset skip, through the seam | `false &&` the skip condition | `TestRun_CrashBetweenInsertAndCommitLeavesNoDuplicates` red (re-verified 10 Nov) |
| Attributes sorted before hashing | skip the `slices.SortFunc` in the metrics codec | `TestMetricsCodec_StoresPointsAsReceived`: `not sorted by key`; round trip: `series/points = "3\t3", want 2 series` (verified 10 Nov) |
| Instance in the series identity | drop `instance` from `series_id` in 003_metrics.sql | round trip: `series/points = "1\t3", want 2 series` (verified 10 Nov) |
| Metrics resume skip | `false &&` the skip condition | `TestRun_MetricsEndToEndCrashLeavesNoDuplicates`: `"360\t60", want 240 points from 60 records` (verified 10 Nov) |
| Sum reset by start time | drop `start_ts != prev_start OR` from `SumDeltas` | `TestDeltas_…`: `sums: disagree on 1 of 72 points` (verified 10 Nov) |
| Sum reset by decrease | drop `OR value < prev_value` | same: 1 of 72 (verified 10 Nov) |
| First point yields nothing | drop `n > 1` from `SumDeltas` | same: 1 of 72 (the series that starts inside the window) (verified 10 Nov) |
| Lookback before `from` | lookback 0 | same: 1 of 72 (verified 10 Nov) |
| Delta temporality passes through | `if(false, value, …)` | same: 21 of 72 (verified 10 Nov) |
| Histogram bucket drop = reset | drop the `arrayExists` term | `histograms: disagree on 1 of 27` (verified 10 Nov) |
| Histogram start change = reset | drop `start_ts != prev_start` from `HistDeltas` | same: 1 of 27 (verified 10 Nov) |
| Histogram bounds change yields nothing | drop `bounds = prev_bounds AND length(…)` | same: 1 of 27 (verified 10 Nov) |
| Times typed UTC | `DateTime64(9)` parameters | on a non-UTC server: `disagree on 72 of 72` (verified 10 Nov) |
| Anomaly series written in key order | skip the key sort in the anomalies codec | `TestAnomaliesCodec_SameEventSameBytes`: `encoded to different bytes` (verified 10 Nov) |
