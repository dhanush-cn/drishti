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
| Consumer killed (`kill -9`) | Partitions move after the group session timeout (~45 s by default, seen locally); replay is skipped by stored offset. **Week 3:** shorter session timeout or static membership |

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

The instrumentation reads clocks four times per iteration and does not change behaviour. Every iteration is counted, empty polls included.

## Revert-proofs (break → red → revert → green)

| Mechanism | Break it | Expected failure |
|---|---|---|
| Stored-offset skip | in `add`, make the skip condition `false && …` | `TestRun_CrashBetweenInsertAndCommitLeavesNoDuplicates`: duplicated rows listed (verified) |
| Day cut | drop `b.day != day \|\|` from the cut in `add` | `TestAdd_CutsBatchAtDayBoundaryAndRowLimit`: `[{0 2} {3 4}]` (verified) |
| Row cut | drop the row condition from the cut and make `full` return false | same test: `[{0 0} {1 4}]` (verified) |
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
