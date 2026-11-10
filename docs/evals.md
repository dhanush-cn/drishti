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

## 3. The agent's view: the query API (patch 44)

`cmd/queryapi` (`internal/queryapi`; compose `queryapi` on 127.0.0.1:8090 for live data, `queryapi-eval`
on :8091 for `drishti_eval`) is the only way the agent sees the system. Each endpoint answers one question
an on-call engineer asks first, in a bounded, typed response:

| Endpoint | Question | Built from |
|---|---|---|
| `/v1/services` | what exists? | services with spans or logs |
| `/v1/overview` | what changed, everywhere? | SERVER span calls/errors/p95, log lines, container CPU and memory; window vs baseline, plus a **deterministic** `changes` list |
| `/v1/anomalies` | what did the detector see? | `anomalies`, collapsed by id, state as of `until` |
| `/v1/spans?service=` | which operation, and which dependency? | span metrics per operation; a client span's `callee` is the service that serves a SERVER span with that name (found in the data, no hand-written map) |
| `/v1/timeseries?service=&signal=` | when did it start? | one of calls, errors, error_ratio, p95_ms, cpu, memory_mib, log_lines, log_errors, ≤ 120 points |
| `/v1/log_templates?service=` | what are the logs saying that's new? | Drain templates (`internal/drain`, the detector's), window vs baseline |
| `/v1/logs?service=` | show me the lines | ≤ 50 lines, filtered by text and severity, bodies ≤ 300 chars, stack traces dropped |

**Rules every endpoint follows, each tested (`internal/queryapi`, against ClickHouse) and revert-proofed:**
- **`until` is required, and nothing after it is read.** Log lines, metric points and anomaly events
  after `until` are never counted or shown. An anomaly resolved after `until` is still `open`.
- **Bounded:** window and baseline ≤ 2 h; every list capped. ClickHouse runs every query with
  `readonly=2`, a time limit and a result-row cap. Arguments are query parameters, never SQL text,
  because they come from an LLM.
- **Hidden services don't exist:** `flagd`, `flagd-ui`, `load-generator`, the collector and Drishti's own
  containers aren't listed, aren't counted, and are "unknown service" when asked for. Their telemetry
  describes the fault injection, the test traffic or the observability stack, not the system under
  diagnosis.
- **One name per service:** `container:email` (docker_stats) is shown and queried as `email` when
  `email` has spans or logs. Infrastructure keeps its prefix (`container:valkey-cart`).
- **Masking (design change 2):** log lines whose body or attribute values contain a flag name,
  "feature flag", "problempattern", "flagd" or "flagservice", or which carry a `feature_flag*` attribute,
  are dropped **in SQL** before counting (one predicate, `MaskedSQL`; `TestMasker_SQLAgreesWithGo` checks it
  against the Go version on the real leak lines). Every response reports `masked_lines`. Anomaly templates
  that name a flag are replaced with `[masked: …]`. Span rows of the flag system itself (clients'
  `flagd.evaluation…` calls, the proxy's `flagd-ui` route) are dropped, because their rate moves when a
  flag changes. **`scripts/leakcheck.py` found that one:** it calls every endpoint for every service, as of
  3 min into each scenario, and searches every response for a flag name, a masked phrase or a hidden
  service. Capture 2: 13 scenarios, 754 responses, 29,926 masked lines reported, **0 leaks** after the fix.
- **Cost of masking, stated:** adManualGc's and adHighCpu's only log lines are masked. The agent must find
  them from latency and CPU, which is how a real incident without a confession in the logs looks.

**The triage step is deterministic (design §7):** `/v1/overview` lists, per service, what crossed fixed
thresholds between baseline and window:
- error ratio ≥ 2× baseline (+0.02, ≥ 0.01);
- p95 ≥ 2× baseline (+20 ms);
- calls ×3 or ÷3;
- warn/error lines ≥ 3× (≥ 0.5/min);
- log lines ×3 or ÷3;
- CPU ≥ 2× (+10);
- memory rising ≥ 1 MiB/min.

Services with changes come first. The thresholds were set before any eval run and are not tuned on the
recordings. They decide what the agent looks at first, not what it concludes.

**Known limits:**
- p95 comes from span-metric buckets (2 ms … 15 s), so a p95 at 15000 ms means "at least 15 s".
- The first point of a series yields nothing (the detector's rule). An error series born with its first
  errors loses that first 5 s interval.
- p95 over a 1-minute step at ~2 calls/min is a quantile of two calls.
