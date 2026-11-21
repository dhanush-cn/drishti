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

## 2. Scenarios, labels and the fault gate (patch 45)

Each recording holds 13 scenarios, one demo flag each (`timeline.jsonl` from the capture drivers).

**Labels (`agent/drishti_agent/evals/labels.py`, fixed before any agent run):** the root cause an engineer
should name. This is stricter than the detector's "affected" sets, which list every service whose telemetry
changes, most of them victims. Two flags accept two names, because the fault sits between two services and
the telemetry can't separate them:
- **paymentUnreachable:** checkout dials a wrong payment address. Naming checkout or payment is right;
  payment itself is healthy.
- **loadGeneratorFloodHomepage:** a surge with nothing broken; frontend or frontend-proxy.

Fault types are scored separately (error, latency, cpu, memory, traffic).

**Fault gate (design E9):** a scenario is scored only if its fault produced a visible symptom by flag-off,
compared with the recording's own no-flag baseline phase. The 30 minutes before a scenario contain earlier
scenarios, so they can't serve as a clean comparison.

| Gate | Flags |
|---|---|
| ≥ 3 errors on the faulty call | adFailure, cartFailure, paymentFailure, paymentUnreachable, productCatalogFailure |
| server p95 ≥ 1.5× | adManualGc (or CPU), productCatalogLockContention, imageSlowLoad (frontend-proxy), intlShippingSlowdown, recommendationCacheFailure (or memory) |
| CPU ≥ 1.5× | adHighCpu |
| memory rising ≥ 0.5 MiB/min | emailMemoryLeak, recommendationCacheFailure |
| server calls ≥ 2× | loadGeneratorFloodHomepage |

Scenarios that fail are listed as **not scored** with the measurement, never dropped silently. Capture 2:
**10 of 13 scored**. adFailure produced 1 failed GetAds in 66 calls and cartFailure 2 errors: the same
"flag barely acted" case as run B's paymentFailure. intlShippingSlowdown didn't move shipping's p95
(5.4 vs 4.35 ms).

**Triggers (design E8):**
- **agent-only:** as of flag-off (on + on_minutes; the slept capture 2 scenario is cut at its first
  10 min). This measures diagnosis on a developed incident.
- **end-to-end:** as of the first alert opened between flag-on and 90 s after flag-off, on any service.
  That's what would page someone. No alert means "missed", scored wrong.

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
  3 min into each scenario, and searches every response for a flag name, a masked phrase, the flag
  system or the load generator anywhere, and for a hidden service listed or owning data. Capture 2:
  13 scenarios, 754 responses, 29,926 masked lines reported, **0 leaks** after the fix.
- **A service naming the collector is not a leak (patch 54).** Capture B's first leak check failed on
  recommendation's own log "Failed to export traces to otel-collector:4317 … DEADLINE_EXCEEDED" under
  recommendationCacheFailure. The collector is hidden as a service (its telemetry isn't the system under
  diagnosis), but that line names no flag and says nothing about what was injected: it is recommendation
  struggling, which an on-call engineer would see. The query API is unchanged; leakcheck now counts the
  observability stack's names only as a service. Decided before any capture B score existed. Capture 2
  under the new rule: still 754 responses, 0 leaks.
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
- The first point of a series yields nothing (the detector's rule). Checked on capture 2: the demo's span
  metrics start a new series (e.g. the first ERROR series of a span) with `start_ts` at its creation and a
  first value of 0, so nothing is lost there. A producer that starts a series with a non-zero count would
  lose that first interval.
- p95 over a 1-minute step at ~2 calls/min is a quantile of two calls.

## 4. The agent, the baseline, the harness (patch 45)

`agent/` (Python, one dependency: Pydantic; HTTP is the standard library).

**Agent v0 (`agent.py`): a plain tool-calling loop.** Week 6 turns it into a LangGraph hypothesis graph. Patch 56 ported this loop into LangGraph first (nodes triage → model ⇄ tools) with no behaviour change: `tests/test_graph_parity.py` replays 19 scripted paths and requires the same results, model requests and query API calls that the Week 5 loop recorded. Prompts are unchanged (`40abe73ebe28`), so the freeze still holds.
1. **Triage first, no LLM:** the harness calls `overview` (the last 2 m vs the 30 m before, or vs the 5 m
   before when paged; see §5) and
   `anomalies` itself and puts both in the first message, after a one-line briefing: the alert that paged
   it (end-to-end) or "no single alert identifies it" (agent-only). A paged briefing also states the rule:
   the root cause changed at or after the alert's onset; an earlier incident goes in ruled_out.
2. **Loop:** the model calls tools (the seven query API endpoints) and gets compact results back.
   - The overview goes back as text: changed services and their changes first, then one line per service.
     JSON for ~25 services is longer than the 6,000-character result cap, and the cut-off was found by
     running the harness on capture 2.
   - The harness fixes `until`: tools take windows and services, never a time.
3. **Ends** when the model calls `submit_diagnosis` (parsed into `Diagnosis`; an invalid answer is
   recorded, never repaired), or when the budget (12 tool calls) runs out. Out of budget, the model gets
   one last turn **without tools** and is told to answer "unsure" if the evidence isn't there. A run that
   neither calls a tool nor answers gets 2 reminders, then stops as unsure (at most 16 turns). It never
   gets more budget, and the harness never fills in an answer.

**Baseline (`baseline.py`, design §8):** same model, one prompt, no tools, given a fixed dump of what the
tools would show at a glance: overview, alerts, and every service's top log templates, capped at
48,000 characters.

**Heuristic floor (`--provider heuristic`, not a model):** trusts the page. End-to-end it names the
service of the alert that paged it; agent-only (no alert) it names the first service the triage overview
lists with changes. It shows whether the LLM adds anything over "repeat the alert" or "pick the top
changed service", and it runs the whole harness without a model. The alert half came in patch 49 (§5). **Capture 2 (the tuning recording):**
- v1 (5-minute triage): agent-only top-1 **5/10**, top-3 7/10; end-to-end top-1 **1/10**.
- v2 (2-minute triage): agent-only top-1 **4/10**, top-3 7/10; end-to-end top-1 **1/10**. Six scenarios
  changed answer each way; the floor is unstable to the window, which is why it is a floor and not tuned.
- End-to-end, the top changed service was often still the previous scenario's, but the alert's own
  service was the root cause in **7/10**. Repeating the alert (patch 49): end-to-end top-1 **7/10**,
  agent-only unchanged at 4/10. That 7/10 is the bar any end-to-end system has to beat.

**Providers (`llm.py`):**
- `ollama`: OpenAI-compatible `/v1` at localhost:11434, for free iteration on the RTX 5050;
- `openai`: any OpenAI-compatible hosted API, given `--base-url`;
- `anthropic`: the Messages API.
- Gemini (patch 50): `openai` with `--base-url https://generativelanguage.googleapis.com/v1beta/openai`.
  - Assistant turns go back to the API exactly as they came (`raw`). Gemini 3 models attach thought
    signatures to their turns and expect them on the next request; a rebuilt message would drop them.
  - Free tiers limit requests per minute and per day. `--rpm N` spaces calls to N per minute, and a 429
    is waited out (Retry-After, else 15 s doubling to 2 min, 5 times) before the run is recorded failed.
    A 503 ("high demand") is waited out the same way (patch 51).
  - Thinking can't be turned off on Gemini 3; `--reasoning-effort low` keeps it small, and
    `--max-tokens` leaves room for it.
  - `python -m drishti_agent.evals.hostcheck` runs one two-turn tool call first, so a wrong key,
    model id or rejected turn fails in seconds, not one scenario into the run.

Temperature 0. Calls time out after 120 s; provider errors are retried twice with backoff, then recorded as failed runs. Cost =
tokens × the prices you pass (`--price-in/--price-out`, USD per million tokens). Nothing is priced from
memory; token counts are always recorded.

**Scoring (`evals/score.py`):**
- top-1 (the named root cause is accepted);
- top-3 (an accepted service in root cause + `ranked_services`);
- fault type;
- unsure (nothing named: honest, scored wrong);
- confidently wrong (wrong with confidence ≥ 0.7; target < 10%).

Plus tool calls, tokens, seconds and cost. `RESULTS.md` reports counts ("7/10"), per-scenario rows, and
the not-scored scenarios with their gate measurement.

**Tune, freeze, test (design changes 6 and 7):**
1. Iterate prompts on **capture 2** with Ollama (`--provider ollama`, 3 runs per cell).
2. Run the **hosted model once on capture 2**, to catch model-specific failures such as tool-call
   formatting before the held-out run.
3. `python -m drishti_agent.evals.freeze --tuned-on capture-2 --hosted-check <that run>` writes
   `agent/FROZEN.json` (prompt version + commit). It refuses without a hosted run of the current prompts.
4. Score **capture B once**, with `--held-out`. The runner refuses if the prompts changed after the freeze.

What's held out:
- On capture B the **agent-only column is held out**.
- The **end-to-end column is not fully held out**: the detector's clumping fix (patch 38) was tuned on
  run B, so B's alert times are partly tuned.
- About 13 scenarios before the gate means a single scenario moves the result by 8–10 percentage points.
  Read counts, not percentages.

```bash
scripts/week5-eval.sh load capture-2      # create-eval --reset, load, store, replay detector, load events
scripts/week5-eval.sh run capture-2 ollama qwen2.5:7b
```

## 5. Tuning on capture 2: v1 → v2 (patches 46, 47)

Everything here was measured on **capture 2**, the tuning recording. Capture B was not looked at.

**v1 run** (Qwen 2.5 7B on Ollama, prompts `43e93a7f9a5a`, 3 runs per cell, 10 scored scenarios):

| | top-1 | top-3 | confidently wrong |
|---|---|---|---|
| agent, agent-only | 19/30 | 21/30 | 11/30 |
| agent, end-to-end | 5/30 | 7/30 | 25/30 |
| baseline, both columns | 0/30 (all invalid) | | |

Runs 1–3 at temperature 0 gave the same answer in most cells, so 3 runs mostly measure the same thing 3 times.

**Harness lessons (patch 46, no prompt change):**
- **Ollama silently truncates.** With its default context, the first run's prompts were cut to
  ~2,050 tokens with no error, and the results were meaningless. Run `ollama serve` with
  `OLLAMA_CONTEXT_LENGTH=24576`: on the RTX 5050 (8 GB) that stays 100% on the GPU (6.0 GB); 32,768
  spilled 9% to the CPU. A run where every call reports the same input token count is truncated.
- **The schema threw away the baseline.** Qwen wrote `ruled_out` as an object in all 60 baseline replies,
  so all 60 were invalid. `evidence`/`ruled_out` now accept a list, an object or a string;
  `ranked_services` a comma string; `confidence` `"0.8"` or `"80%"` (a bare number above 1 stays invalid:
  the scale is ambiguous). Re-scored offline, the same 60 replies give baseline top-1 **15/30** agent-only
  and **9/30** end-to-end. The baseline beats the v1 agent end-to-end (9 vs 5).
- **One stalled call froze the run for minutes** (300 s × 5 retries). Now 120 s × 2, and stalled runs stop.

**Why the agent failed, per scenario (v1), and the v2 change for each (patch 47):**
- **End-to-end, 5/30, and 25/30 confidently wrong.** The agent was not told which alert paged it, and
  the 5-minute triage window still held the previous scenario (e.g. ad latency during
  productCatalogLockContention, so it answered `ad` 3/3). v2: the first message names the alert
  (service, rule, series, time), triage covers 2 minutes, and the prompt says to anchor on the onset.
  The baseline gets the same briefing and window, so the comparison stays fair.
- **adHighCpu, 0/3 both columns.** `changes` listed CPU only at +10 points, but docker_stats reports
  percent of the host: ad went 0.35 → 3.16 under the fault. v2 rule: at least 2× and +1 point
  (`TestChanges_CPUInDockerStatsPercent`; normal jitter such as 1.76 → 2.55 stays out).
- **productCatalogFailure, 0/3 (checkout).** It stopped at the service with errors instead of asking
  what checkout calls. v2 prompt: follow the callee chain until the errors stop.
- **Confidently wrong, 11/30 even agent-only.** v2 prompt: 0.8 or more only when the service's own
  signal changed at the onset and its callers point to it; otherwise below 0.5 or null.
- **emailMemoryLeak, 0/3.** That scenario starts right after the laptop slept, so its traffic baseline
  is distorted. Not tuned for; it may stay a miss.

**v2 run** (prompts `57bbd9adf6b6`, code `f326ec8` on the laptop = patches 40–47; 3 runs per cell):

| | top-1 | top-3 | confidently wrong | unsure |
|---|---|---|---|---|
| agent, agent-only | 14/30 (v1 19) | 23/30 | 10/30 | 6/30 |
| agent, end-to-end | 11/30 (v1 5) | 22/30 | 19/30 | 0/30 |
| baseline, agent-only | 15/30 | 30/30 | 15/30 | 0/30 |
| baseline, end-to-end | 24/30 | 27/30 | 6/30 | 0/30 |
| floor (patch 49), agent-only / end-to-end | 4/10 / 7/10 | | | |

What it showed:
- **The briefing helped end-to-end (5 → 11), but the agent still loses to the alert.** It names the
  alert's service far less often than the baseline does: it investigates, then talks itself out of it.
  Two patterns: following the callee chain past the root cause (recommendationCacheFailure:
  recommendation's cache fails, it calls product-catalog more, the agent names product-catalog) and
  misreading numbers (it wrote "latency increased from 38.8 ms to 32.4 ms").
- **The baseline, with the same briefing and no tools, scores 24/30 end-to-end**, above the floor (21/30
  over 3 runs) only through productCatalogFailure. One prompt plus the alert is most of the signal here.
- **Agent-only fell 19 → 14.** 4 runs answered in the same turn as their tool calls, before any result
  (patch 48 refuses that); 2 runs stalled until the reminder limit; productCatalogLockContention went 3/3
  → 0/3 and recommendationCacheFailure 3/3 → 0/3 with long tool chains.
- **Calibration did not take:** wrong answers still came at confidence 0.8–0.9.
- Repeats at temperature 0 disagree more: 11 of 20 agent cells have mixed answers (v1: 7 of 20). Mean
  input tokens per run: 30k agent-only (v1 35k), 46k end-to-end (v1 37k). The run took 3 h 13 min
  (v1: 49 min).

Read with care: 10 scenarios, one recording, a 7B model. These are tuning numbers, not results.

**Hosted check** (Gemini 3.5 Flash-Lite, free tier, prompts `57bbd9adf6b6`, code = patches 40–50, 1 run per cell):

| | agent-only | end-to-end |
|---|---|---|
| agent | **8/10** (confidently wrong 2) | 5/10 (confidently wrong 4, 1 provider error) |
| baseline | 7/10 | 7/10 |
| floor (top changed / repeat the alert) | 4/10 | 7/10 |

- With a stronger model the loop works agent-only: above the baseline and twice the floor. Much of the
  Qwen 7B gap was the model.
- End-to-end, all 4 wrong answers named **the previous scenario's fault**: paymentFailure → cart
  (cartFailure), adHighCpu → payment (paymentUnreachable), productCatalogLockContention → ad
  (adManualGc), recommendationCacheFailure → a traffic surge (loadGeneratorFloodHomepage), at
  confidence 0.8–0.95. At the first alert the new fault is seconds old; the agent widened its windows to
  10–15 minutes and explained the older, louder incident. The 5th miss was three 503s from Gemini.

**Patch 51: explain the page, not the loudest incident.**
- Paged triage compares the last 2 minutes with the **5 minutes just before** (agent-only keeps 30: the
  5 before would hold the fault). An incident already running shows as flat or falling. Measured with the
  floor's ranker at the end-to-end trigger: top changed service right 1/10 with 30 m, 2/10 with 5 m; the
  residue left the top for paymentFailure (cart) and productCatalogLockContention (ad). It helps, it does
  not decide, hence the next two.
- The paged briefing states the rule: the root cause changed at or after the onset; an earlier incident
  goes in ruled_out. Same text for agent and baseline.
- **Onset check (removed in patch 52):** when the agent named a service other than the paging one, the
  answer was refused once with a request to check that service's timeseries against the onset.
- Everything the model is told (briefings, the onset check, triage window and baselines) now lives in
  `prompts.py` and is part of `PROMPT_VERSION`; before, the briefing and triage window were outside the
  freeze check. Prompts `8fd0c4cf47f9`.

**Hosted check of patch 51** (same model, 1 run per cell):

| | agent-only | end-to-end | confidently wrong (both) |
|---|---|---|---|
| agent, before 51 | 8/10 | 5/10 | 6 |
| agent, with 51 | 7/10 | 4/10 | 4 |
| baseline, with 51 | 7/10 | 7/10 | 3 |

- No end-to-end answer named the previous scenario any more, but the agent turned cautious: 4 unsure
  end-to-end (3 of them right before 51). Fewer confident errors, fewer right answers. Mean input tokens
  per end-to-end run rose from 45k to 68k.
- The onset check fired twice: productCatalogFailure, right (product-catalog) withdrawn to unsure;
  productCatalogLockContention, wrong (ad) swapped for wrong (frontend). It fixed nothing.
- One run of 10 scenarios: a one-scenario change is noise. The check's record is not.

**Patch 52 and the stopping rule.** Three rounds on the same 10 scenarios have not lifted the agent above
"repeat the alert" end-to-end; more rounds would fit capture 2's quirks rather than the agent. Patch 52
removes only the onset check (the component with a record of harm and no benefit) and keeps the 5-minute
paged comparison and the briefing rule. Decided before running it: one hosted check of patch 52 on
capture 2, then freeze those prompts **whatever they score**, then capture B. Prompts `40abe73ebe28`.

**Frozen prompts on capture 2** (patch 52, prompts `40abe73ebe28`, 1 run): agent 8/10 agent-only, 5/10
end-to-end; baseline 6/10 and 7/10; floor 4/10 and 7/10. Frozen as decided (`agent/FROZEN.json`).

## 6. Held-out result: capture B (frozen prompts)

Gemini 3.5 Flash-Lite, prompts `40abe73ebe28` (checked against `agent/FROZEN.json` by `--held-out`),
code `26c1860`, 3 runs on 19–20 Nov. Nothing was tuned on capture B. Two changes landed between the freeze
and the run, neither touching prompts: the loader's batch cap (patch 53: the load stopped at Kafka's
1 MiB limit) and the leak check's rule for collector mentions (patch 54, decided before any capture B score).

- Load: 267,636 log rows, 1,289,933 metric points, 1,232 anomaly events, matching the consumers' counts.
- Leak check: 13 scenarios, 754 responses, 31,364 masked lines, **0 leaks**.
- Fault gate: **9 of 13 scored.** Not scored: cartFailure (1 error), paymentFailure (0 payment errors, as in
  the Week 4 live run), intlShippingSlowdown (p95 5.68 vs 5.37 ms), loadGeneratorFloodHomepage (calls ×1.8,
  need ×2).

Counts over 9 scenarios × 3 runs:

| | agent-only (held out) | end-to-end (not fully held out) |
|---|---|---|
| agent, top-1 | **22/27** (8, 7, 7) | 10/27 (3, 4, 3) |
| agent, top-3 | 24/27 | 15/27 |
| agent, confidently wrong | 4/27 | 8/27 |
| agent, unsure | 1/27 | 9/27 |
| baseline (no tools), top-1 | 19/27 (7, 6, 6) | 17/27 (5, 7, 5) |
| floor (deterministic), top-1 | 7/9 per run | 4/9 per run |

What it says:
- **Agent-only:** the tool loop is above the one-prompt baseline (22 vs 19) and level with the floor (22
  against 21 over three runs). The floor is much stronger on capture B (7/9) than on capture 2 (4/10): here
  the top changed service usually is the culprit, which leaves the agent little room. Same order as capture 2
  (agent > baseline > floor there, by more).
- **End-to-end:** the agent is below both the baseline (10 vs 17) and repeating the alert (≈12), with 8
  confidently wrong and 9 unsure answers. The same weakness as on capture 2: at the first alert the new fault
  is seconds old, and the agent either picks a louder neighbour (payment for recommendationCacheFailure in
  2 of 3 runs, ad for productCatalogLockContention in 2 of 3) or withdraws to unsure. The one-prompt
  baseline, which reads the alert and one snapshot, does better. This column is not fully held out (the
  detector's clumping fix was tuned on capture B), which if anything flatters it.
- Repeats at temperature 0 differ: per-run agent top-1 7–8 (agent-only) and 3–4 (end-to-end).
- Small numbers: 9 scenarios on one recording. One scenario is 3 of 27.

**Conclusion for Week 5.** With a capable model, an agent that investigates with tools explains an incident
better than one prompt over a snapshot when it starts after the fault has been running a few minutes. At
the first alert it does not yet beat the alert itself or the snapshot. The Week 6 design work (an explicit
"confirm or reassign the alert's service" step rather than a free search) starts from here.

## 7. Week 6: the hypothesis graph in the harness (patch 67)

The Week 6 agent (`--systems graph`, docs/agent.md) runs in the same harness, on the same scenarios, triggers, fault
gate and scorer. The Week 5 loop stays as `--systems agent`.

**Variants for the Week 7 comparison**, each a flag, labelled so one results folder holds them all (`--label` makes
rows `graph:<label>` / `agent:<label>`):

| Variant | Flags |
|---|---|
| full graph | `--systems graph --runbooks-postgres URL --memory-postgres URL --memory-from <other recordings>` |
| no runbooks | the same without `--runbooks-postgres`, `--label no-rb` |
| no memory | the same without `--memory-*`, `--label no-mem` |
| F4 on (off by default since patch 69, docs/agent.md section 9) | `--f4 --label f4` |
| v0 | `--systems agent` |
| v0 at the graph's query budget | `--systems agent --budget 20 --label b20` (separates the design's effect from more lookups) |
| baseline, floor | as Week 5 |

- **Memory** is read-only during a run and seeded from other recordings' labels (`python -m drishti_agent.memory
  seed`, no model calls); the runner refuses the recording being scored. On capture C the headline is without memory.
- **Resuming across days** (`--resume`): every cell (system, scenario, column, run) is appended as it finishes; a rerun
  skips finished cells and retries failed ones, and the report keeps the latest row per cell. A rate limit that
  outlasts its waits stops the run with exit code 3 instead of recording a string of failures: rerun the same command
  with `--resume` the next day. This is how the capture C score fits the free tier (design §11: about 2,300 requests,
  about 5 days of quota).
- **Request ledger and daily cap** (patch 74, `--ledger FILE` or `$DRISHTI_LEDGER`): every model request is appended
  to the ledger (time, model, cell, tokens, status, failed requests included; USD at the given prices, what it would
  cost on a paid tier), and before each request the day's count for that model is summed from the file. At
  `--max-requests-per-day` (default 450, below the free tier's 500) the request is refused and the run stops like a rate
  limit (exit 3, `--resume` continues); a cell that can't make its first request isn't started. The day runs from 08:00
  UTC (13:30 IST, the free tier's reset). `python -m drishti_agent.evals.quota --ledger FILE` prints the day's totals.
  Why: on 21 Nov the free quota ran out although our runs had counted 148 + 61 requests (something else used the key),
  and nothing on our side could say how much we had used. Capture C is scored on the free tier only, from a project with
  no billing account, over several days.
- **Fix on target** (would the proposed fix target a labelled service) is recorded per graph row and shown in the
  per-scenario table. It equals top-1 except for unsure, so it is not presented as a separate result.
- **Freeze:** `FROZEN.json` now records the graph's prompt version too, and `--held-out` refuses graph runs whose
  prompts aren't frozen or have changed. The Week 5 freeze file has no graph version, so the held-out capture C run
  needs a new freeze after Week 7's tuning, as planned.

## 8. Week 6 measured (21 Nov 2026)

Internal, provisional: replayed recordings of the OTel Demo on one laptop, 1 run per cell. A difference of one
scenario is within the noise of a single run; Week 7 repeats runs before any number is presented.

**Capture 2 (the tuning recording), graph, full variant (runbooks, memory from capture B), F4 off, page_check cap 12,
10 scenarios scored.** Before = graph prompts `586e21b83481`; after = patches 70-71, `3213caa16cbc`.

| | agent-only top-1 | end-to-end top-1 | end-to-end unsure / confidently wrong |
|---|---|---|---|
| Gemini 3.5 Flash-Lite, after | **10/10** | **5/10** | 3 / 2 |
| Gemini 3.5 Flash-Lite, before | 8/10 | 4/10 | 5 / 1 |
| Qwen 2.5 7B (Ollama), after | 4/10 | 5/10 | 2 / 3 |
| Qwen 2.5 7B (Ollama), before | 5/10 | 3/10 | 4 / 3 |
| Week 5 agent, frozen (Gemini, section 6) | 8/10 | 5/10 | |

- End-to-end stays below the floor that repeats the alert (7/10 on capture 2). Gemini still refutes ad in adHighCpu
  end-to-end (its log spike read as informational); imageSlowLoad end-to-end turned from unsure to wrong.
- The after run spread over two days of free quota (`--resume`, 15 cells kept).

**Capture C (held out): fault gate and leak check only, no agent and no model.** 11 of 13 scenarios scored (not
scored: cartFailure, 1 error; intlShippingSlowdown, p95 43.5 ms vs 180 ms). Leak check: 13 scenarios, 754 responses,
33,016 masked lines, 0 leaks.

**Live, once (week6.sh live): the whole loop on the running demo, Gemini 3.5 Flash-Lite.** productCatalogFailure
switched on after a 15-minute warm-up (the detector judges nothing before 10 minutes of a service's history; a first
attempt with 5 minutes produced no alert).

| Step | Measured |
|---|---|
| alert | 31 s after the flag: `span_errors` on frontend, SERVER `GET /api/recommendations` (a victim) |
| diagnosis | product-catalog, error, confidence 0.85, accepted by the judge; proposal `rollback_config` on product-catalog, 27 s after the investigation started |
| approval | by Dhanush, through the API |
| action | 1 flag turned off: productCatalogFailure |
| recovery | product-catalog's server error ratio back within the threshold of its pre-incident reference (0) for 4 checks in a row, 30 s apart; confirmed 180 s after the action (7 checks) |

One live run shows the loop works end to end; it is not an accuracy measurement.

## 9. Week 7 ceiling check: the recheck (G2) and the stale guard (G3′) (patch 75)

No model calls. `rulecheck --recheck 1,2,3 --stale --wrong-runs RUNS.jsonl ...` adds to the F4 check, per scored scenario
with an end-to-end alert:

- **G2:** page_check again at the trigger + 1, 2 and 3 minutes (2 is the design's delay, 1 and 3 for information): is
  the labelled service in the candidate table with an own-signal change by then, and is that moment still before
  flag-off?
- **G3′ on the label:** would the stale-change guard (docs/agent.md section 11) reject the labelled service at the
  trigger or at the trigger + 2 minutes? The condition set on 21 Nov: **0 rejected on capture 2 and 0 on capture B**,
  printed as PASS or FAIL; any rejection is listed with its reason, and the guard isn't put in the judge as written.
- **Catches:** every committed wrong end-to-end answer in the given results files (agent or graph rows, never the
  baseline), checked at its own run's `until`. Per source: confidently wrong (confidence ≥ 0.7) caught / total, other
  wrong answers caught / total, and how many name the paged service (which the guard never checks).

Week 7's sources: capture 2, the Week 6 graph runs (`graph-hosted`, `graph-hosted-3213caa16cbc`, `graph-ollama`,
`graph-ollama-3213caa16cbc`); capture B, the Week 5 v0 runs (`hosted-run1..3`).

**Measured 21 Nov (laptop, capture 2 and capture B, no model calls).**

| Labelled service in the table with its own change | at the page | +1 min | +2 min | +3 min | past flag-off |
|---|---|---|---|---|---|
| capture 2 (10 scored) | 2/10 | 8/10 | **10/10** | 8/10 | never |
| capture B (9 scored) | 4/9 | 7/9 | **8/9** | 7/9 | never |

The recheck (G2) passed its gate and was built (patch 76). The stale guard (G3′) **failed**: 3 labelled answers rejected
on capture 2 and 3 on capture B (docs/agent.md section 11); it caught 2 of 9 past confidently-wrong answers on capture 2
(Week 6 graph runs) and 2 of 8 on capture B (Week 5 v0 runs). Dropped, as agreed before the check.
