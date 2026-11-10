# Evals: recordings → eval database → agent

How Drishti's root-cause agent is evaluated, and why each piece is shaped the way it is.
Week 5 design: decisions E1–E9 and the review changes, summarised where they apply.

## 1. Data: recordings replayed through the real pipeline (patch 42)

The two demo recordings (`capture.kd.gz`, 3–4 Nov; `capture-b.kd.gz`, 5 Nov; `cmd/kafkadump dump`) hold
every record of `otlp.logs` and `otlp.metrics` for a 20–30 min no-flag baseline followed by 13 fault
scenarios. Evals run on them, not on live demo runs:
- the same input every run, so a prompt change is the only thing that changes;
- no 3-hour demo run per iteration;
- capture B can be kept for one final scoring.

```bash
scripts/create-eval.sh --reset                       # eval.* topics (retention -1) + drishti_eval (no TTL)
go run ./cmd/kafkadump load -in capture.kd.gz        # → eval.otlp.logs, eval.otlp.metrics
go run ./cmd/storage -signal logs    -topic eval.otlp.logs    -table drishti_eval.logs    -group eval-logs    -reset earliest
go run ./cmd/storage -signal metrics -topic eval.otlp.metrics -table drishti_eval.metrics -group eval-metrics -reset earliest
go run ./cmd/detector -replay capture.kd.gz -out events.jsonl
go run ./cmd/kafkadump load-events -in events.jsonl  # → eval.anomalies
go run ./cmd/storage -signal anomalies -topic eval.anomalies -table drishti_eval.anomalies -group eval-anomalies -reset earliest
```

**Why replay into Kafka instead of writing ClickHouse directly:** the eval data goes through the same
consumers and codecs as live data, with the same exactly-once rules. A side loader would be a second
encoder that could disagree with the first.

**Original timestamps, and the two places they'd be deleted:**

| Where | Default | Would do | Eval setting |
|---|---|---|---|
| Kafka topic retention | 24 h, by record timestamp | delete a week-old recording soon after loading | `eval.*` topics: `retention.ms=-1`, `retention.bytes=-1`; `kafkadump load` refuses anything else |
| ClickHouse TTL | 7 days on `received_at` | drop expired rows **on insert** (lab, 13 Oct) | `drishti_eval` built by `cmd/evalschema`: same schema files, TTL lines removed; `create-eval.sh` checks no table kept one |

`kafkadump load` also refuses non-empty target topics. A reloaded record is a new Kafka record, so the
exactly-once consumer would store it again: everything twice.

**Verified (cloud VM; kfake + ClickHouse 26.9 engine via chdb; correctness, not speed):**
- capture 2 loaded with `kafkadump load`: 55,941 + 68,353 records, as in its manifest;
- the real `cmd/storage` binaries (logs, metrics) stored 254,743 log rows (= the capture summary's
  per-service total), 1,193,686 metric points (= the codec's count over the file) and 254,743 lines in
  `logs_1m`, received_at 16:23:04 → 02:06:15 UTC as recorded, every metrics record once, 0 decode errors;
- `TestEvalSchema_KeepsRowsAProductionTTLWouldDropOnInsert`: a row received 30 days ago is dropped on
  insert by the production schema and kept by the eval schema;
- loader: partition, key, value and timestamp preserved; refuses a second load, finite retention, too few
  partitions, missing topics; events land on the partition the live detector would use.

**What the data showed about names (for the query API, section 3):** docker_stats resources have no
`service.name`, so metrics from them are owned by `container:<compose name>` (`container:email`) while logs
and spans say `email`. One instance per container in the demo. docker_stats also reports Drishti's own
containers (`drishti-kafka`, `drishti-storage-1`, …), infrastructure (`valkey-cart`, `astronomy-db`), and
`flagd`, `flagd-ui` and `load-generator`, which the leakage filter must cover under both names.
