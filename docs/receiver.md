# OTLP receiver → Kafka

`cmd/receiver` accepts OTLP logs and metrics over gRPC on `:14317` and publishes them to Kafka (`otlp.logs`, `otlp.metrics`). It answers OK only after Kafka has acknowledged the data.

```
sender ──gRPC/gzip──► otlpgrpc (thin) ──► ingest (split, key, timeout) ──► kafkasink (franz-go) ──► Kafka
```

| Package | Job | Knows about |
|---|---|---|
| `internal/otlpgrpc` | decode, call ingest, map errors to OTLP status codes | gRPC |
| `internal/ingest` | split requests into messages, key them, bound the wait | OTLP protos only |
| `internal/kafkasink` | produce, wait for acks, check topics | Kafka |

## Decisions

### 1. Transport: gRPC only, transport-agnostic core
- Both senders (demo collector, our load generator) support gRPC, and the OTLP spec defines retry semantics for it (`UNAVAILABLE` + `RetryInfo`).
- Throughput did not decide it: with collector batching, 50k events/s is ~50 requests/s, trivial for either transport.
- `ingest` takes OTLP structs, not gRPC types, so OTLP/HTTP later is a small adapter calling the same `Ingester`.
- Lost debuggability (no `curl`) is recovered with gRPC reflection: `grpcurl -plaintext localhost:14317 list` works without `.proto` files.
- Port 14317 because the demo collector already publishes 4317/4318.

### 2. Kafka client: franz-go
- Pure Go, so week 3 pprof flame graphs include batching, compression and retries. With confluent-kafka-go (cgo + librdkafka) that hot path is invisible to pprof and its memory is outside the Go heap / `GOMEMLIMIT`.
- Its bounded produce buffer (`MaxBufferedBytes`) is the backpressure mechanism (decision 4).
- Idempotent producer by default with `acks=all`: broker-side retries can't duplicate.
- segmentio/kafka-go rejected: slow-moving pre-1.0, and its Writer's time-based batching default adds latency on every produce.
- Trade-offs owned: librdkafka has more years at extreme scale; franz-go is largely one maintainer.

### 3. Message unit: one OTLP Resource per Kafka message, keyed by `service.name`
| Option | Problem |
|---|---|
| Whole export request | Mixes services: no meaningful key, no per-service ordering, unbounded size |
| One record per message | ~10x more messages; resource attributes duplicated into every one |
| **Per resource** | Key = service → same partition → ordered per service; resource attributes stored once; value is still a standard `Export*ServiceRequest` |

- Value = protobuf `ExportLogsServiceRequest` / `ExportMetricsServiceRequest` with exactly one resource, so consumers use the standard OTLP types.
- Resources over `--max-message-bytes` (900 KiB, under Kafka's ~1 MiB `max.message.bytes`) are split by halving, order preserved. A single record that can't fit alone is dropped and reported as OTLP **partial success** (not retryable; retrying can't make it fit).
- Metrics split at whole-`Metric` granularity; rejections are counted in data points, as the spec requires.
- Separate topic per signal: different schema, different consumers.
- Known risk: one noisy service = one hot partition. That's week 3's hot-partition work.
- Topics have 6 partitions (`scripts/create-topics.sh`). Adding partitions later remaps keys, so per-service ordering breaks across the change.
- The partitioner is set explicitly through `internal/partitioning.New()`, not inherited. It is franz-go's default (`UniformBytesPartitioner`, murmur2 for keyed records, same as the Java client), so existing data keeps its key → partition mapping. The load generator predicts partitions with the same constructor (`docs/loadgen.md` §7). `TestNew_MatchesLibraryDefault` proves equivalence with the library default; `TestForKey_ObservedPairs` pins pairs observed in Kafka on 10 Oct (`smoke-svc-1`, `smoke-svc-2` → p0, `smoke-svc-0` → p2).

### 4. Backpressure: bounded wait, then retryable rejection; the backlog lives in the sender
- Each export waits for Kafka acks before returning OK. Acking before Kafka (fire-and-forget) is faster but loses data on a crash, after we told the sender it was safe.
- If Kafka is slow, franz-go's buffer fills (64 MiB bound) and produce blocks. Every export has a 4s deadline (`--publish-timeout`), below the collector's 5s, so the sender gets our answer rather than its own timeout.
- On timeout or Kafka error: `UNAVAILABLE` + `RetryInfo(1s)`. The collector keeps the batch in its sending queue and retries with backoff. Memory in the receiver stays bounded; the queue is in the sender, which is built for it.
- **Bug found by the live kill test (fixed):** the first version used franz-go's `ProduceSync`. With Kafka stopped *after* the client had fetched metadata, `ProduceSync` kept waiting on delivery retries past the 4s deadline, so the sender hit its own 5s timeout (`DeadlineExceeded`) instead of getting our `UNAVAILABLE`. The context only bounds getting records *into* the buffer, not their delivery. Fix: produce asynchronously and stop waiting when the deadline passes. The unit test that passed before covered a broker that was never reachable (a different code path), which is why it missed this. `TestPublish_BrokerDiesMidRunReturnsAtDeadline` now covers the real case.
- Records already buffered when an export times out may still be delivered once Kafka returns, and the sender retries them too: another at-least-once duplicate source. `--delivery-timeout` (30s) stops them retrying forever and pinning buffer space during a long outage.
- Bugs (e.g. marshal failure) return `INTERNAL`: not retryable, since a retry would repeat the bug.
- **Delivery is at-least-once.** If 3 of 5 messages are acked and then the deadline hits, the sender retries all 5 and 3 are written twice. Exactly-once would need Kafka transactions per export (latency, complexity). Instead the week 2 storage consumer must write idempotently.
- gRPC `MaxRecvMsgSize` is 16 MiB (default 4 MiB). An oversized request gets `RESOURCE_EXHAUSTED`, which looks like backpressure but isn't. Size collector batches under it.
- Shutdown order: health → `NOT_SERVING`, `GracefulStop` (in-flight exports finish and are acked), then flush and close the producer.

## Run

```bash
cd ~/drishti
go mod tidy                      # fetches franz-go, grpc, otlp protos; writes go.sum
go vet ./... && go test ./...    # unit + in-memory Kafka (kfake) tests, no Docker needed

docker compose up -d kafka
chmod +x scripts/create-topics.sh && ./scripts/create-topics.sh

go run ./cmd/receiver            # terminal 1
go run ./cmd/otlpsmoke           # terminal 2: 3 services x 100 logs + 3 metrics
```

Check it landed (3 messages per topic, keys are the service names):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic otlp.logs --from-beginning \
  --property print.key=true --property print.partition=true --max-messages 3 | cut -c1-80
```

## Profiling (pprof)

`-pprof-addr` / `DRISHTI_PPROF_ADDR` serves `/debug/pprof/` on its own listener (`internal/pprofsrv`). Off by default; compose sets `:6060` and publishes it on `127.0.0.1:16060` only.

- **Own listener, own mux.** The OTLP port is the data path and shares nothing with debugging. `net/http/pprof`'s `init` registers its handlers on `http.DefaultServeMux` regardless (a test asserting otherwise failed), so the rule is that the receiver never serves `DefaultServeMux`.
- **Cost.** Idle: nothing. A CPU profile samples at 100 Hz only while `profile?seconds=N` is being fetched. Heap/allocs read counters the runtime already keeps. Block and mutex profiling stay off (they have a standing cost).
- **Write timeout 120 s**, above the longest profile we request (30 s); a shorter timeout cuts the response mid-profile.
- **Symbols survive `-ldflags="-s -w"`**: profiles are symbolized from the runtime's own tables, not DWARF, so `go tool pprof` on the WSL side needs no binary.

```bash
curl -s -o cpu.pprof 'http://127.0.0.1:16060/debug/pprof/profile?seconds=30'   # during a load run
go tool pprof -top -nodecount=40 cpu.pprof
go tool pprof -top -cum -nodecount=40 cpu.pprof
```

The load generator profiles itself with `-cpuprofile` (warm-up + steady + drain, not pool generation) into `cpu.pprof` in its run directory.

## Measured, 20 Oct (laptop, internal: provisional thresholds, not quotable)

Core 7 240H, 16 threads, WSL 15 GiB, AC + Best performance. 512 records/request, 1 resource/request, `-stamp=false` (Week 1 payloads), gzip. Generator, receiver and Kafka share the laptop. Raw data: `Downloads/drishti-week2/results/`.

### Knee: sustainable 175k, knee at 200k

| rec/s | verdict ×3 | p50 ms | p99 ms | receiver cores | generator cores | Kafka cores |
|---|---|---|---|---|---|---|
| 175k | PASS, PASS, PASS | 31–37 | 189–608 | 4.2–4.5 | 3.1–3.4 | 0.6–0.8 |
| 200k | FAIL, FAIL, PASS (**knee**) | 55–104 | 765–1216 | 5.0–5.2 | 3.8–3.9 | 0.6–0.7 |
| 225k | FAIL ×3 | 186–208 | 1347–1425 | 5.3–5.4 | 4.2–4.4 | 0.6 |

- At 225k the generator kept its schedule (lag ≤ 19 ms) while achieved fell 1.4–2.8% short and up to 749 requests saturated: the system, not the generator, ran out.
- The stack used ~9.5 "cores" of 16 threads at 200k, about 40% of it the generator. On one laptop, the generator's cost moves the knee (`docs/loadgen.md` §12: pre-compressed payloads are now triggered).
- **Unexplained:** at 175k, p99 was 189–608 ms vs 54–85 ms on 13 Oct, and the receiver used ~12% more core-time per record (4.2–4.5 vs 3.8 cores). Same code path, same payloads. A lower CPU clock (thermal or power state) would explain both; nothing records it yet. Pass 2's `env.json` should.

### The ~22 ms p50 floor: per-record CPU on both ends, not linger

20k rec/s, 30 s steady, 3 repeats each:

| Config | p50 ms (3 runs) |
|---|---|
| baseline: 512 rec/req, linger 5 ms | 25.8, 24.2, 25.7 |
| linger 0 | 24.5, 24.4, 24.5 |
| 128 rec/req | 12.0, 11.8, 11.7 |

- **Linger is not it:** −0.7 ms (I predicted −5 ms; wrong).
- Fitting p50 = a + b × records/request through the two batch sizes: **b ≈ 35 µs per record**, **a ≈ 7 ms per request**. The per-record slope matches the measured CPU cost of one record on both ends (receiver ~22 µs + generator ~16 µs from the Week 1 sweep): each 512-record request is processed serially, on one goroutine, by both sides. The ~7 ms fixed part is per request: produce round trip with acks, gRPC, scheduling.
- So the floor is a request-size effect, not a delay: smaller requests have lower latency at the same record rate (at the cost of 4x the requests and ~1.3x the receiver CPU at 20k: 0.60 vs 0.45 cores).

### CPU profile at 150k (30 s, `results/pprof/receiver.*`)

3.73 cores sampled. Largest cumulative costs:

| Where | cum % |
|---|---|
| gzip decompression (`flate.huffmanBlock`), of which gRPC's `mem.Reader.ReadByte` (byte-at-a-time reads) ~9.7% | 24.7 |
| protobuf unmarshal (`unmarshalPointerEager`) | 23.7 |
| lz4 compression for Kafka (`lz4block.CompressBlock`) | 15.2 |
| protobuf size computation (`sizePointerSlow`: split + re-marshal) | 8.8 |

About 40% is codec work (gunzip + lz4) and about a third is decoding and re-encoding protobuf. That is ~22 µs per record. Week 3 candidates, each to be measured, not assumed:
1. **Pass-through for one-resource requests** (all observed collector traffic, `docs/loadgen.md` §13): the request bytes already are a valid Kafka value; only `service.name` has to be read. Skips unmarshal + size + re-marshal.
2. **Faster gzip decoding**: a buffered reader in front of flate, or klauspost's decoder (already a dependency).
3. **Kafka compression**: lz4 vs none vs zstd, with broker disk and network in the comparison.

## Week 3 changes (each measured on the laptop by `week3b.sh`, not assumed)

### 5. gzip decoding: bulk copy, then klauspost (patch 19)

`internal/otlpgrpc/gzip.go` registers its own `"gzip"` compressor in place of grpc-go's. The old path ran the standard library's inflater over gRPC's `mem.Reader`, one interface `ReadByte` call per byte (the 9.7% in the profile). The new one copies the compressed message (tens of KiB) into a pooled buffer in one go, then inflates from a `*bytes.Reader` with klauspost/compress, whose inflater has a specialised loop for that concrete type. gRPC's limits are unchanged: compressed size is checked against `-max-recv-bytes` before `Decompress`, and decompressed reads are capped at the same limit + 1.

- Cloud VM microbenchmark (`BenchmarkDecompress`, one 512-record request with random-hex bodies; relative only): stdlib over `mem.Reader` ~3.3–3.7 ms, gzipCodec ~1.6–1.8 ms per request, 27 → 12 allocations. The laptop question is what share of receiver CPU it removes at 150k.
- Wire behaviour is unchanged: stdlib-written gzip reads back, ours reads in stdlib (`TestGzipRoundTrip`), and the collector-path test still passes.

### 6. Kafka batch compression is a flag (patch 20)

`-compression` / `DRISHTI_KAFKA_COMPRESSION` (compose: `RECEIVER_COMPRESSION`, default lz4): lz4, zstd, snappy, gzip or none. lz4 was 15.2% of receiver CPU at 150k. The choice trades receiver CPU against bytes on Kafka's disk and network and the consumer's decompression CPU, so `week3b.sh` measures all three per codec at the same rate: receiver CPU (docker stats), topic bytes per record (log end size), consumer CPU. An unknown name is a startup error, not a silent lz4 (a typo would benchmark lz4 under another label). `TestPublish_CompressionCodecOnTheWire` reads the codec of each fetched batch back from Kafka.

### 7. Pass-through for one-resource logs requests (patch 21)

Decision 3 makes the Kafka value an `ExportLogsServiceRequest` with exactly one resource. A request that already has exactly one resource (every logs batch observed from the demo collector, `docs/loadgen.md` §13) **is** that value, so the receiver publishes its uncompressed wire bytes instead of re-encoding the decoded structs.

- How the bytes are kept: gRPC's generated handler decodes and drops them. The logs service is registered with an equivalent descriptor (same service, method and metadata, so clients and grpcurl see standard OTLP) whose handler decodes into `rawLogs`; the server's codec (`grpc.ForceServerCodecV2`) copies the bytes for that one type and hands every other message to gRPC's proto codec unchanged. The copy is needed because gRPC frees the buffers when decoding returns, while the value may still sit in franz-go's buffer after the export answered. It replaces the re-encode's allocation of the same size.
- What still happens: the request is still decoded (the key needs `service.name`, and a malformed request must fail as before). Only proto size + marshal are skipped.
- Falls back to the split path (unchanged output, `TestExportLogsRaw_OtherShapesAreSplitAsBefore`) for: several resources, a resource with no records, or a request over `-max-message-bytes`.
- Cloud VM microbenchmark (`BenchmarkExportLogs512`, relative only): split path ~0.37–0.42 ms and 230 KB allocated per 512-record request after decoding; raw path ~1 µs. Against ~11 ms of receiver CPU per request on the laptop, expect a few percent, not a step change; the laptop profile decides.
- Metrics keep the split path: scraper batches carry many resources (§13), so pass-through would rarely apply.

## Revert-proofs (break → red → revert → green)

| Mechanism | Break it | Expected failure |
|---|---|---|
| gzip support | register `gzipCodec` under another name (`Name()` returns `"gzip-k"`) | `TestGzipCompressorIsOurs`: `registered gzip compressor is <nil>` (verified); `TestExport_GzipFromCollectorIsAccepted` |
| Pass-through | make the condition in `ExportLogsRaw` always false | `TestExportLogsRaw_OneResourcePublishesTheRequestBytes`: `value is not the request's bytes (re-encoded?)`; `TestExport_OneResourceRequestPassesThroughAsSent`: `paths pass-through=0 split=1` (verified) |
| Raw handler | register with `collogs.RegisterLogsServiceServer` again | `TestExport_OneResourceRequestPassesThroughAsSent`: `paths pass-through=0 split=1` (verified) |
| Compression flag | hardcode `kgo.Lz4Compression()` in `kafkasink.New` | `TestPublish_CompressionCodecOnTheWire`: `batch codecs on the wire map[3:2], want only 4` (and 0, 1, 2) (verified) |
| Retryable mapping | in `toStatus`, return `codes.Internal` for `ErrPublish` | `TestExport_KafkaFailureIsUnavailableWithRetryInfo` |
| Publish deadline | remove `context.WithTimeout` in `ingest.publish` | `TestExport_PublishTimeoutBoundsTheWait` |
| Deadline after broker death | in `kafkasink.Publish`, replace the async block with `return s.cl.ProduceSync(ctx, recs...).FirstErr()` | `TestPublish_BrokerDiesMidRunReturnsAtDeadline` fails at 3s (verified) |
| Size cap | in `splitLogs`, change `<= maxBytes` to `<= maxBytes*100` | `TestSplitLogs_OversizedResourceIsSplitUnderLimitInOrder` |
| Key → partition | in `kafkasink.Publish`, set `Key: nil` | `TestPublish_RoundTripKeysHeadersAndPartitioning` (keys not preserved) |
| Explicit partitioner | in `partitioning.New()`, return `kgo.StickyKeyPartitioner(kgo.SaramaHasher(fnv32a))` (an FNV-1a `func([]byte) uint32`) | `TestForKey_ObservedPairs`, `TestNew_MatchesLibraryDefault`, `TestPublish_PartitionsMatchSharedPartitioner` |
| Fail-fast topics | start the receiver before `create-topics.sh` | exits with `topic "otlp.logs" missing` |
| pprof format | in `pprofsrv.Handler`, map `/debug/pprof/profile` to `pprof.Index` | `TestStart_ServesCPUProfile` (`404 Unknown profile`, verified) |
| Real backpressure | receiver + smoke running, `docker compose stop kafka`, run smoke again | smoke prints `Unavailable` after ~4s, receiver stays up; `start kafka` → smoke OK again |

## Next
1. Point the demo collector at the receiver (`otelcol-config-extras.yml`; networking choice between `host.docker.internal:14317` and joining `opentelemetry-demo`).
2. Load generator v1, then the first real ingest number.
