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

## Revert-proofs (break → red → revert → green)

| Mechanism | Break it | Expected failure |
|---|---|---|
| gzip support | delete the `encoding/gzip` import in `internal/otlpgrpc/server.go` | `TestExport_GzipFromCollectorIsAccepted` |
| Retryable mapping | in `toStatus`, return `codes.Internal` for `ErrPublish` | `TestExport_KafkaFailureIsUnavailableWithRetryInfo` |
| Publish deadline | remove `context.WithTimeout` in `ingest.publish` | `TestExport_PublishTimeoutBoundsTheWait` |
| Deadline after broker death | in `kafkasink.Publish`, replace the async block with `return s.cl.ProduceSync(ctx, recs...).FirstErr()` | `TestPublish_BrokerDiesMidRunReturnsAtDeadline` fails at 3s (verified) |
| Size cap | in `splitLogs`, change `<= maxBytes` to `<= maxBytes*100` | `TestSplitLogs_OversizedResourceIsSplitUnderLimitInOrder` |
| Key → partition | in `kafkasink.Publish`, set `Key: nil` | `TestPublish_RoundTripKeysHeadersAndPartitioning` (keys not preserved) |
| Fail-fast topics | start the receiver before `create-topics.sh` | exits with `topic "otlp.logs" missing` |
| Real backpressure | receiver + smoke running, `docker compose stop kafka`, run smoke again | smoke prints `Unavailable` after ~4s, receiver stays up; `start kafka` → smoke OK again |

## Next
1. Point the demo collector at the receiver (`otelcol-config-extras.yml`; networking choice between `host.docker.internal:14317` and joining `opentelemetry-demo`).
2. Load generator v1, then the first real ingest number.
