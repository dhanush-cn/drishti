# Storage consumer: Kafka → ClickHouse

`cmd/storage` consumes `otlp.logs` (consumer group `drishti-storage`), flattens each OTLP log record into a row, and inserts batches into `drishti.logs` (`clickhouse/schema/001_logs.sql`). Every Kafka record lands in the table **exactly once**, without Kafka or ClickHouse transactions.

```
otlp.logs (6 partitions) ──► poll ──► per-partition batch (RowBinary bytes) ──► HTTP INSERT ──► part
                                                                                   │ 200 OK
                                                     commit offset last+1  ◄───────┘
```

| File | Job |
|---|---|
| `internal/storage/consumer.go` | poll, batch, flush, commit, rebalance callbacks, resume |
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

**Why the resume query is cheap:** an aggregate **projection** (`ingest_progress`: `max(kafka_offset) GROUP BY kafka_partition`) is stored inside every part, built atomically with it. `EXPLAIN` shows `ReadFromMergeTree (ingest_progress)`, 1 granule (checked on ClickHouse 26.9 via chdb; verify on 26.8 with the driver).

**The token stays as a second layer:** it covers the one case the skip can't, an insert that timed out on our side but was written on the server, retried by the *same* process with identical bytes.

**Guard:** if the table holds an offset at or beyond a partition's end offset, the topic was deleted and recreated (offsets restart at 0) and skipping would silently drop every new record. The consumer refuses to start (`topic recreated?`).

**What would break it, stated so it can be checked:**
- An insert split into several blocks. ClickHouse emits a new block at `min_insert_block_size_rows` (1,048,449) **or** `min_insert_block_size_bytes` (256 MiB). `Limits.Validate` keeps batches below both (rows < 1,048,449; bytes ≤ 200 MiB, one message of margin). The Week 2 driver checks `system.part_log` for any insert query that created more than one part.
- `async_insert`. The server buffers and merges inserts, so "200 OK" no longer means "this part exists". Some 26.x builds default it **on** (chdb 26.9: `async_insert = 1`). Every insert sends `async_insert=0`.
- Partitions moving between consumers mid-batch. `BlockRebalanceOnPoll`: a rebalance can only happen between loop iterations, after flush and commit. Revoke/lost callbacks drop unflushed batches; the next owner re-reads them.

### 2. Batching: per Kafka partition, cut by rows, bytes, age, or day

- **Limits:** 100,000 rows **or** 32 MiB encoded **or** 2 s after the batch's first record, whichever comes first (`-batch-rows`, `-batch-bytes`, `-batch-wait`). A message is never split: the row cut happens before a message that would cross the limit.
- **The trade-off (lab §7):** ClickHouse wants few, large inserts; freshness wants small, frequent ones. At 175k records/s over 6 partitions (~29k/s each) a partition fills 100k rows in ~3.4 s, so the **2 s age cut decides**: about 6 inserts per 2 s (~3 inserts/s) of ~58k rows. At low traffic the age cut bounds event → queryable to ~2 s + insert time. The live dashboard reads Redis (Week 4), not ClickHouse, for sub-second freshness.
- Due batches are inserted **concurrently** (at most one per partition at a time) and committed together.

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
| Shutdown mid-insert | The in-flight attempt is not cancelled (`WithoutCancel` + insert timeout): cancelling it would turn a clean stop into an "was it written?" case. Shutdown stops retries only, then commits what succeeded |
| Undecodable Kafka message | Logged with partition/offset, counted (`decode_errors`), skipped. Retrying would wedge the partition forever |
| Consumer killed (`kill -9`) | Partitions move after the group session timeout (~45 s by default, seen locally); replay is skipped by stored offset. **Week 3:** shorter session timeout or static membership |

## Run

```bash
docker compose up -d kafka clickhouse
scripts/create-tables.sh
docker compose up -d --build storage receiver
docker compose logs -f storage     # "stats" every 10 s: records, rows, inserts, avg_insert_ms, skipped_replayed
scripts/storage-check.sh <run_id>  # after a loadgen run: loss/dup per seq, path latency
```

A new consumer group starts at the **end** of `otlp.logs` here (`DRISHTI_STORAGE_RESET=latest` in compose): Kafka holds hours of receiver-only benchmark traffic. Production would use `earliest`. To skip a backlog later: stop `storage`, then `kafka-consumer-groups.sh --group drishti-storage --topic otlp.logs --reset-offsets --to-latest --execute`.

## Verified

- `go test -race`: every record once across 6 partitions; **crash between insert and commit leaves no duplicates** (replay cut into different batches, so only the stored-offset skip can save it); failed inserts retried; day and row cuts; refusal on stored offsets beyond the topic end; Limits; attribute rendering; SQL quoting. Crash test run 30x with `-race`: stable after the shutdown fix below.
- **End to end, cloud VM** (in-memory Kafka + ClickHouse 26.9 engine behind a small HTTP shim; not benchmark numbers): loadgen 10k rec/s, 22 s → 219,648 rows = ingested records, 429 seqs, none short or long, **including a `kill -9` of the consumer mid-run and a restart**. That kill landed after a commit, so `skipped_replayed` was 0; the unit test covers the gap deterministically.
- Bugs found on the way: a **data race** (`-race`, only when applying the patches to a fresh clone): `kgo.NewClient` starts group management at once, so `onAssigned` could run before `New` had stored the admin client; it now uses the client the callback is handed. `Close` hung in `LeaveGroup` (with `BlockRebalanceOnPoll` you must `AllowRebalance` first); a shutdown mid-insert returned `context canceled` (now the in-flight attempt completes); the startup check quoted the table with Go's `"…"`, which ClickHouse reads as an identifier (`UNKNOWN_IDENTIFIER`), found only end to end.

## Revert-proofs (break → red → revert → green)

| Mechanism | Break it | Expected failure |
|---|---|---|
| Stored-offset skip | in `add`, make the skip condition `false && …` | `TestRun_CrashBetweenInsertAndCommitLeavesNoDuplicates`: duplicated rows listed (verified) |
| Day cut | drop `b.day != day \|\|` from the cut in `add` | `TestAdd_CutsBatchAtDayBoundaryAndRowLimit`: `[{0 2} {3 4}]` (verified) |
| Row cut | drop the row condition from the cut and make `full` return false | same test: `[{0 0} {1 4}]` (verified) |
| Recreated-topic guard | make `stored >= end.Offset` always false | `TestRun_RefusesStoredOffsetsBeyondTopicEnd`: `err = <nil>` (verified) |
| AllowRebalance before Close | delete it from `Run`'s defer | every `TestRun_*` hangs in `LeaveGroup` (found that way) |
