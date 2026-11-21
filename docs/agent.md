# The Week 6 investigator

Design record for the root-cause agent's Week 6 graph. The Week 5 loop, its evals and the held-out result are
in `docs/evals.md`; the design note with every decision (F1–F14) and the reviews that changed it is
`WEEK6-DESIGN.md` in the project notes. This file grows one section per patch.

Week 5 ended with the agent above the baseline when it starts on a developed incident (agent-only, capture B:
22/27 vs 19/27), and below it at the first alert (end-to-end: 10/27 vs 17/27, and below simply repeating the
alert). At the first alert the fault is seconds old, and the free tool loop either drifted to a louder neighbour
or withdrew to "unsure". Week 6 makes the page the **default hypothesis**: the agent confirms it or moves it to
another service with evidence. The mechanical parts are deterministic code; the model keeps the judgement.

## 0. The loop as a graph (patch 56)

`agent.py` runs the Week 5 loop as a LangGraph graph, unchanged: triage (code) → model ⇄ tools, plus a turn-limit
step. State is plain data and every model turn is its own step, so a checkpointer can later save it between steps.
`tests/test_graph_parity.py` holds it to the Week 5 loop's recorded results on 19 scripted paths. Model calls stay
in `llm.py` (HTTP from the standard library; Gemini's thought signatures go back verbatim).

## 1. page_check and rule F4 (patch 57)

`drishti_agent/pagecheck.py`. No model.

### The candidate table

From the paging service (end-to-end), or the top 5 changed services in triage (agent-only):

1. `overview` (the triage windows): each service's own changes, by the query API's deterministic thresholds.
2. `spans` per service: SERVER rows that changed are the service's **own** change; CLIENT rows that err or slow
   down are **victim signs**, and their `callee` is followed (breadth-first, depth ≤ 3).
3. Services that changed in triage but weren't reached are added as **changed (no edge)**: an alert can land on
   a service with no call path to the root cause (capture B's emailMemoryLeak was paged on recommendation).
4. `timeseries` (30 min, 30 s steps) for each candidate's main changed signal: its **onset** and **trend**.

Own change counts SERVER errors, latency and calls, CPU and memory. Log changes do not: a log spike is also what a
victim writes when its calls fail.

Every call goes through a counted budget (`DEFAULT_CAP`, design F5; 8 at first, 12 since the ceiling check, section 9) and the walk stops when it runs out.
`rulecheck` measures what a full walk needs on the recordings, which is what the cap should be set from.

### Rules, fixed before any run

All thresholds are the overview's (`internal/queryapi` `changes()`): error ratio ≥ 2× and +0.02 (≥ 0.01),
p95 ≥ 2× and +20 ms, CPU ≥ 2× and +1 point, calls ×3 / ÷3 (≥ 1/min), memory rising ≥ 1 MiB/min.

- **Onset of a timeseries.** Reference R = the 10th percentile of the 30 minutes (90th for a traffic drop): faults
  fill up to 4 of every 7 minutes of a recording, so a mean or median would sit inside them. Points are smoothed
  with a 1-minute mean. Onset = the first point of the final run of points past the threshold that reaches the
  newest point; the run breaks only on 2 smoothed points not past (about 1.5 min of quiet). Newest point not past:
  no current change. Resolution 30 s; a step whose halfway value doesn't cross the threshold shows one point late.
- **Onset of memory.** Change-point fit: for every split, a flat level before it and a least-squares line after it;
  the split with the least total squared error is the knee, and it is an onset if the line climbs ≥ 1 MiB/min with
  R² ≥ 0.8 (the detector's). The first version ("the earliest point whose fit over the rest passes") was wrong: its
  unit test showed a flat-then-climbing series fits a line well enough from the very first point.
- **Trend at the page.** The last 2 minutes' mean against the 2 before, with the same threshold in the fault's
  direction: past it = worsening, past it the other way = recovering, else holding. Memory: slope over the last
  4 minutes, ±1 MiB/min.
- **The paging service's onset, by the rule that fired.** New template: "first seen" from the alert's evidence.
  Template or error-log spike: the start of the window that fired. Span rules: that span's own timeseries, SERVER
  or CLIENT, whichever fired (`/v1/timeseries kind=client`, patch 59); other kinds fall back to the window start and
  say so. Memory: the climb's knee.

**Patch 59, `/v1/timeseries kind=server|client`.** Span signals default to SERVER as before. Flag-system spans
(`flagd.evaluation…` calls, the proxy's flagd-ui route) are now dropped in SQL (`MaskedSpanSQL`, checked against the
Go masker): every service makes them as CLIENT calls and their rate moves when a flag flips. This also changes the
SERVER path, which summed a service's flag-system SERVER spans into its series while `/v1/spans` already dropped
them. That leak is numeric, so `scripts/leakcheck.py` (which searches text) can't see it; the query API's tests do.

### F4: naming a service other than the paged one (end-to-end only)

A final answer naming S ≠ the paging service P is accepted only if:
- **S's own signal changed** (a candidate with an own change: a service that only fails because of a callee
  doesn't qualify), and
- **S is not an earlier incident**: its onset more than 5 min before **P's onset** while it is holding or
  recovering at the page. A change that began long before but is still worsening (a slow leak) is allowed.

Why P's onset and not the alert time: alerts come late (capture 2: cartFailure's own signal 119 s,
emailMemoryLeak 155 s; capture B: emailMemoryLeak about 17 min), and a root cause leads its victims. Why 5 minutes:
chosen with the recordings' 7-minute scenario cadence in view (the previous scenario starts about 7 min earlier
and is recovering by the page), and fixed before the ceiling check. A call edge to the page is evidence, never
required. Agent-only has no page, so F4 does not apply there.

### The ceiling check

`python -m drishti_agent.evals.rulecheck` (or `scripts/week5-eval.sh rulecheck NAME TIMELINE OUT --runs RUNS`),
no model calls. Per scored scenario, from its trigger alert only: is the labelled service in the table (by an edge
or not, with the cap and with a full walk), would F4 accept it, would F4 refuse each wrong service the Week 5
agent named, how many query calls the walk used, and how far each onset is from the flag-on time. Its first runs on
capture 2 and capture B are reviewed before the hypothesis loop (patch 59) is built on the rule.

## 2. The hypothesis graph (patch 60)

`drishti_agent/graph.py`, system `graph` in the eval runner (`--systems graph`). The Week 5 loop (`agent.py`, system
`agent`) is unchanged and stays a control. The graph has its own prompts (`prompts_graph.py`, `GRAPH_PROMPT_VERSION`):
the Week 5 briefing anchored the root cause on the alert's time, and F4 anchors on the paging service's onset.

```
triage (code) → page_check (code) → hypothesize (model) → test: model ⇄ tools → judge (code)
                                          ↑                                        │
                                          └──── next hypothesis / new round ───────┘
                                                  judge → report | unsure
```

- **hypothesize:** the model sees the briefing, triage, the candidate table and the hypotheses so far, and calls
  `submit_hypotheses` with up to 3 untested ones (service, fault type, prior, why). A hypothesis is a service and
  a fault type: the same pair isn't tested twice, but a service refuted as one fault type can be tested as another
  (patch 70; on capture 2 the model's second try at recommendation, as latency after a refuted error, was dropped).
- **test:** one hypothesis at a time, highest prior first. The model calls the query tools (at most 4 per hypothesis)
  and ends with `mark_hypothesis`: supported, refuted or inconclusive, a confidence, and the ids of the results that
  show it (`[t3]`, or `candidates` for its row in the table). A verdict sent in the same turn as tool calls is
  refused, as Week 5 refuses a batched answer.
- **judge (code):**
  1. *Grounding:* a cited result counts only if it is **about** the hypothesis's service: a per-service tool whose
     `service` argument is it, its own row in the overview (a line starting with it) or an alert whose `service` is
     it, or its candidate row when that shows an own change. A failed query counts for nothing. An overview mentions
     every service, so "the result mentions it" would accept anything.
  2. Supported, grounded, confidence ≥ 0.7 → **report**. (With `--f4`, end-to-end answers must also pass F4: off by
     default since patch 69, section 9.)
  3. F4 refusal: the hypothesis is marked refused with the reason, which the model sees in the next round. A second
     refusal ends the run as **unsure**, listing what was refused. The judge never substitutes an answer.
  4. Otherwise the next open hypothesis, or a new round (at most 3), or unsure when the budget is out.
- **Budgets:** 12 model tool calls in total (as v0), 4 per hypothesis, 3 rounds, 15 model calls (one run of about 26
  investigations then stays inside a 500-request day), and page_check's own query cap. Running out is "unsure, here's
  what I ruled out".
- **The report** is built by code from the accepted hypothesis (no extra model call): its service and fault type, the
  cited results as evidence, everything refuted, refused or inconclusive as `ruled_out`, and `ranked_services` for
  top-3. An unsure report still ranks the open and weakly supported hypotheses.

Tests: `tests/test_graph.py`, a scripted model over a fake call graph. Every judge edge, grounding (other service,
mention inside another row, candidate row without an own change, unknown id, failed query), F4 (refused and continued,
refused twice, switched off, agent-only, no-edge service accepted on its own signal) and every budget. 16 breaks of the
graph's rules each turn a test red.

## 3. Surviving a crash (patch 61)

`drishti_agent/durable.py` runs the graph with LangGraph's Postgres checkpointer (compose `postgres`, Postgres 17,
`127.0.0.1:5434`, a persistent volume). The state is saved after every step; a step that was running when the process
died is run again, nothing before it is.

- **Step size.** Every model turn and every tool batch is its own step, so a crash costs at most one repeated model
  call. Query calls are read-only, so repeating one is harmless. The only step with an effect, executing an approved
  fix (section 6), is idempotent.
- **`durability="sync"`.** LangGraph's default writes a step's checkpoint while the next step runs. The process test
  killed a child right after a model turn and found the previous step (hypothesize) run again on resume: its
  checkpoint was lost with the process. Synchronous writes cost a few KB per step, next to a model call.
- **thread id** = hash(alert, until): the same alert resumes instead of starting over, and a finished investigation is
  returned as it was, with no model call.
- **Crash point.** `DRISHTI_AGENT_CRASH_AFTER=<node>:<n>` exits with status 3 after the n-th run of a node computes
  and before it is saved, as storage's `-debug-crash-after-inserts` does.
- **Evals** don't use Postgres: a replay doesn't need to survive anything, and its results mustn't depend on it.

Tests (`tests/test_durable.py`): a crash after each node, resumed on the in-memory and the Postgres checkpointer, must
give the same diagnosis with only the interrupted step repeated (one model call if it was a model turn; its own query
calls if it made any); a killed child process (`os._exit(3)`) resumed by a new one, on Postgres. Reverted: async
durability fails the process test 3 times out of 3; no checkpointer and resume-from-scratch fail too.

## 4. Watching an investigation: events and tracing (patch 62)

**Events.** `durable.stream()` yields one readable event per finished step, built from LangGraph's per-node updates:
`start`, `candidates` (the table and the paging service's onset), `hypotheses` (only the new ones), `tool_calls`,
`tool_results` (id, arguments, ok, a one-line summary; only the new ones), `verdict`, `judge` (the decision in words),
`diagnosis`, `end`. A resumed run says `resumed`; a finished one replays only its diagnosis, with no model call.
`durable.investigate()` is the same stream, drained. The agent service (section 7) serves it as SSE.

**LangSmith.** Off unless `LANGSMITH_TRACING=true` and `LANGSMITH_API_KEY` are set; then each `llm.chat` is an `llm` run
(inputs, output, tool calls, token usage) nested under the graph's node runs, which LangGraph traces by itself. Off,
`traced(llm)` is the model object itself and nothing leaves the machine; the evals' default. Model calls stay in
`llm.py`, so this wraps them rather than using a LangChain chat model. Cost per incident still comes from our own token
counts. `tracing.flush()` drains both queues (LangChain's tracer for nodes, langsmith's client for model runs) before a
short-lived process exits.

Tests (`tests/test_events.py`): the event order of a run; each event carries only what's new (two rounds); resumed
and finished runs. Tracing goes to a local stand-in for the LangSmith API, and the test counts the `llm` runs named after
the model (3). The first version of that test passed without the wrapper, because node traces carry the same words;
it now fails without it. LangSmith caches environment lookups, so the test clears them before and after.

## 5. Runbooks (patch 63)

`drishti_agent/runbook_tools.py`, data in `drishti_agent/runbooks/`.

- **Facts:** `services.json`, each demo service's role and call relations, paraphrased from the demo's public service
  page and architecture diagram (sources in the file). The two pages disagree in places (a service's language; whether
  the catalog reads a file or a database), so the facts stop at roles and relations.
- **One template.** Every service's runbook is rendered from the same template (`rendered/*.md`, committed and checked
  against the template): its relations, then the same checks for every fault type (error, latency, CPU, memory,
  traffic) and how to tell cause from victim. No service gets a failure mode of its own. The runbooks were written
  knowing the scenarios; a uniform template is how they can't become an answer key. Tests check the headings are
  identical, that with placeholder relations every service renders to one of two texts (calls others or not), that the
  per-service facts carry no fault vocabulary, and that no word of the fault injection appears anywhere.
- **Symptoms guide:** `symptoms.md`, general operations knowledge, one section per symptom (connection refused,
  timeouts, steady memory climb vs a flat level, traffic surge, …). Also written knowing the scenarios: the Week 7
  no-runbooks run measures what the runbooks are worth.
- **Tools** (graph test step, counted against the tool budget, never evidence for the judge): `runbook(service)`, a
  plain lookup, and `search_runbooks(query)`, Postgres full-text search (`websearch_to_tsquery`, ranked by
  `ts_rank_cd`, falling back to OR-ed words). Hypothesize also sees the relations line of each changed candidate.
- **Recall@3**, on `queries.json` (30 symptom queries with the sections that answer them, written before any agent run):
  - first measurement, every section indexed: **23/30**. The misses had one cause: a service's per-fault sections are
    the same template text for every service, and 13 near-copies filled the top 3 of most symptom queries;
  - index changed to the symptoms guide and each service's relations section (the per-fault text stays one
    `runbook(service)` away), query set unchanged: **30/30**. Embeddings aren't needed for this corpus.
- Runner: `--runbooks-postgres URL` turns runbooks on (the index is rebuilt from the files at start); without it, the
  no-runbooks variant.

## 6. Incident memory (patch 64)

`drishti_agent/memory.py`, table `incidents` in the agent's Postgres.

- **Signature (code):** the page (service and rule), each candidate's own changes by kind (error, latency, traffic, CPU,
  memory) and the victims (candidates with failing or slow calls), from page_check's table. Similarity is Jaccard over
  these sets; the top 2 at 0.5 or more go into hypothesize with their outcome. No embeddings: the signature is already
  structured.
- **Outcomes:** `confirmed`, `wrong` (kept and shown as "X was blamed and it was WRONG; it was Y"), and `unconfirmed`,
  never retrieved. `resolve()` records what turned out to be true (a verified recovery, or a human's verdict).
- **Sources** keep memories apart: `live` for the running system, a recording's name for eval seeds.
- **Evals:** memory is read-only during a run, so no scenario learns from another and order can't matter (tested).
  `python -m drishti_agent.memory seed` writes postmortems from a recording's labels: for each scored scenario, the
  signature at each trigger and the labelled root cause, confirmed, with no model calls. The runner's
  `--memory-postgres URL --memory-from <recordings>` reads them and refuses the recording being scored. Scored incidents
  then recur incident types already in memory, memory's best case: on capture C the headline is without memory.

## 7. Human-approved remediation (patch 65)

`drishti_agent/remediate.py`; graph nodes after `report`, only when the graph is built with a `Remediation` (live).
Evals don't change: a report still ends the run there.

```
report → propose (code) → approve (a human: LangGraph interrupt) → execute (code) → verify (code) → end
            └ unsure: nothing proposed        └ rejected: closed, nothing touched
```

- **One allowlisted action, `rollback_config(service)`.** In the demo every fault is a config flag, so rolling back the
  diagnosed service's config is the fix that works; in a real system it would be a deploy or config rollback API.
  Restart is left out: restarting a leaking service hides the leak and changes the demo for later scenarios.
- **propose** is a lookup (diagnosis → action), not a model call.
- **approve** pauses the run with LangGraph's `interrupt`; the state waits in Postgres until `durable.decide(thread,
  approve, by)` resumes it. A decision without who made it is refused. Asking about a waiting run changes nothing.
- **execute** refuses anything outside the allowlist and anything without an approval record. The executor alone maps a
  service to the flags it owns (from the eval labels) and turns off those that are on, through the demo's flag API (as
  `scripts/demo-flag.py`). The agent never sees a flag name. Turning a flag off twice leaves it off: a crash during
  execute resumes with no second change (tested). A wrong diagnosis rolls back the wrong service, nothing relevant
  changes, and verify reports it: an honest failure, left to the human.
- **verify** polls `/v1/timeseries` live every 30 s for the diagnosed service's changed signal, against the reference
  level page_check measured its onset from (now kept per candidate), with the same threshold: recovered after 2 minutes
  in a row back within it; memory recovers when the climb stops (slope below 0.5 MiB/min). It gives up after 10 minutes,
  and says so when there's no measured signal to judge by.

Tests (`tests/test_remediate.py`): the executor's guards and idempotency on an in-memory flag API; the verifier against a
local query API that recovers, relapses or never recovers; the full approve and reject flows; unsure proposes nothing;
a crash during execute. Eight breaks (no approval check, no allowlist, every flag rolled back, writing when nothing
changed, a relapse not resetting the count, one good check enough, a rejection executing, no pause) each turn a test red.

## 8. The agent service and the dashboard link (patch 66)

`drishti_agent/service.py` (FastAPI), run with `scripts/agent-service.sh` on `127.0.0.1:8092`:

| | |
|---|---|
| `POST /v1/investigations {"anomaly_id"}` | start (201), or the existing run for that alert (200) |
| `GET /v1/investigations[/{id}]` | recent runs; one run's status, diagnosis, proposal, action, recovery |
| `GET /v1/investigations/{id}/events?after=N` | SSE: every event after N, then live ones; ends with `done` |
| `POST /v1/investigations/{id}/decision {"approve", "by"}` | resumes a run waiting for approval; 400 without `by`, 409 if not waiting |
| `GET /ui/anomaly/{anomaly_id}` | a page: start, watch, approve or reject |

- **Manual trigger.** Every investigation costs model calls; auto-investigating every alert needs Week 8's rate limit and
  daily spend cap. At most 2 run at once. One run per alert (its thread id comes from the anomaly id).
- **Durable.** Checkpoints, each run's events (a page opened later replays them), the runbook index and live incident
  memory are all in the agent's Postgres. A run waiting for approval survives a restart; a run killed midway (status
  still `running`) is resumed from its checkpoint when the service starts (both tested).
- **One decision per approval.** The decision endpoint changes the status from `awaiting_approval` atomically, so a
  second click gets 409 instead of a second execution.
- **Memory, live:** a finished run is remembered unconfirmed; a verified recovery confirms it.
- **The page** shows event text with `textContent` only (it comes from a model) and closes its stream on `done`
  (EventSource would otherwise reconnect on its own). After a decision it follows from the last event it saw.
- **Natively in WSL, not a container:** Docker Desktop's host gateway reaches Windows, not the WSL distro (Week 1), and
  Ollama runs in WSL. Natively, the query API, Postgres, the demo's flag API and Ollama are all on localhost.
- **Dashboard:** `-agent-url` (`DRISHTI_AGENT_URL`) is served at `/config.json`; each open alert's card gets an
  Investigate link to the agent's page. The agent's page starts the run (same origin), so the dashboard sends nothing
  to the agent and needs no CORS.

Tests (`tests/test_service.py`, Postgres): start, one run per alert, SSE, decision validation, approve → executed and
verified → remembered confirmed; reject; unknown alert; a restart while waiting; a run killed midway resumed at
startup; the page never uses innerHTML. Five breaks (no atomic claim, no resume at startup, recovery not remembered, a
run per click, no `by` check) each turn a test red. Go: `/config.json` and the page's link (2 breaks red).

## 9. F4 measured, and switched off by default (patch 69)

The ceiling check (section 1) ran on the laptop on 20 Nov, no model calls, against both recordings' trigger alerts:

| | capture 2 (10 with an alert) | capture B (9) |
|---|---|---|
| labelled service in the candidate table | 8 | 8 (by an edge or paged: 6) |
| F4 accepts the labelled service | **7** | **7** |
| Week 5 agent's wrong end-to-end answers refused | 1 of 2 | 4 of 8 |
| refusals by the earlier-incident condition | 0 | 0 |
| largest walk (query calls) | 11 | 7 |

- **Every refusal of a labelled answer came from "own change required"** (capture 2: paymentUnreachable,
  productCatalogLockContention, productCatalogFailure; capture B: adFailure, productCatalogFailure). At the first
  alert, page_check saw no change in the root cause's own signals over its 2-minute window, while the services calling
  it already failed. paymentUnreachable shows the split: refused on capture 2 (checkout and payment show no own
  change), accepted on capture B (checkout's own change, onset at flag-on).
- **The same condition refused the 5 wrong answers F4 caught** (product-catalog for imageSlowLoad, ad for
  productCatalogLockContention). As many right answers as wrong ones: a wash.
- **The earlier-incident condition never fired.** It rarely has data at the first alert: a log-spike page's onset is
  the start of a 5-minute window, and most candidates' onsets aren't measurable yet.
- **The Week 5 agent's most frequent wrong answer on capture B passes F4 anyway** (payment for the two memory faults:
  payment's memory really was climbing).

Decision (20 Nov, Dhanush): **F4 off by default**; `--f4` keeps it as a Week 7 variant measured with a model. Not
re-tuned on these recordings: any version fitted to two recordings already looked at would only fit them. The graph
keeps the candidate table as context and the grounding check as its judge; the model's prompt no longer describes F4
(when it is on, a refusal's reason still reaches the model through the hypotheses board). page_check's query cap is
**12**, set from the largest walk (11); 8 cut one walk short of its onsets.

Also from the check: the labelled service is reached by every agent-only walk (10/10, 9/9).

## 10. The page is evidence about the paged service (patch 71)

The first graph runs on capture 2 (21 Nov, 1 run each, F4 off): Qwen 7B 5/10 agent-only and 3/10 end-to-end; Gemini
3.5 Flash-Lite 8/10 and 4/10 (unsure 5, confidently wrong 1). In several end-to-end misses the page was on the root
cause and the model ruled it out: Gemini refuted ad (adHighCpu) and recommendation (recommendationCacheFailure); Qwen
refuted ad and recommendation and left payment (paymentFailure) inconclusive.

- **Why:** the page came before the fault reached the paged service's metrics. Triage's rows at the alert:
  ad CPU 0.494 vs 0.468; recommendation memory +0.14 MiB/min; payment server error ratio 0 vs 0. The model guessed a
  fault type, found that signal flat, and refuted the service. No comparison window could show it: the change wasn't
  there yet. The alert (a log spike, a new log line) was the only own change so far.
- **Change:** the judge accepts `page` as evidence for the paged service, and for no other (a citation of the alert
  that paged). The prompt says an alert often fires seconds into a fault, before metrics move: don't refute the paged
  service because its metrics haven't moved yet, refute it when another service's own change explains the page (a
  callee whose own signals fail) or it's an earlier incident; and mark the fault type found rather than refuting a
  wrong guess. With patch 70, a wrong first guess no longer rules a service out.
- **Risk, to be measured:** pages on a victim (frontend for paymentUnreachable and productCatalogFailure) may now be
  accepted more often; the victim rule (failing client calls point to the callee) still applies. Capture 2 only (the
  tuning recording); capture B and C untouched.
- Measured after this change: docs/evals.md section 8.

## 11. The stale-change guard, G3′ (patch 75: the rule and its ceiling check; failed it, dropped, never in the judge)

On capture 2 (21 Nov) both confidently-wrong end-to-end answers named a service whose change had already ended before
the page started: ad for productCatalogLockContention (ad's p95 spike was the previous scenario, back to normal two
minutes before the page) and product-catalog for imageSlowLoad (likewise). The model's own evidence showed the recovery.
"Had a big spike in the last ten minutes" is not "is the cause now", and that is a timing fact, so it belongs in code.

`pagecheck.stale_change(api, S, paged, p_onset)`, fixed in the Week 7 design note (section 4) before its ceiling check,
reusing page_check's pieces and adding no threshold:

- S's own signals: server error ratio, p95, CPU, calls (a rise and a drop), memory, each read as page_check reads an
  onset (30 min at 30 s; 5 query calls, the two call directions share one series).
- Runs of past points: `onset_of`'s reference, 2-point smoothing and `past()`; a run **ends where 2 consecutive smoothed
  points are not past** (page_check's run-break rule). `runs_of` is tested to start its newest run exactly at `onset_of`.
- A signal is **live** if a run reaches the newest point and `trend_of` isn't "recovering". It is **over** if its last
  run ended by the run-break rule before P's onset, or if a run reaches the newest point, is "recovering", and began
  before P's onset. Memory: live if `memory_onset` finds a climb that isn't recovering; over only by recovering.
- **S is stale if at least one signal is over and none is live.** No run at all is not stale (no change is not a stale
  change: F4 refused paymentUnreachable's payment for showing none, and was switched off for it). The paged service, no
  answer, and an unknown P onset are not checked.

Patch 77 puts it in the judge (end-to-end only): a stale answer is sent back once with the reason, a second one becomes
unsure. Only if the ceiling check passes: `rulecheck --stale` must reject **0 labelled answers on capture 2 and on capture
B**, at the trigger and two minutes later (the Week 7 recheck), and it reports how many committed wrong answers in past
results files it would have caught (docs/evals.md section 9).

**Measured 21 Nov, and dropped.** The ceiling check rejected 3 labelled answers on capture 2 and 3 on capture B (the
condition was 0 on each; docs/evals.md section 9). Four of the six were the same service causing the previous scenario
too (product-catalog for productCatalogFailure after LockContention, on both captures; payment for paymentUnreachable
after paymentFailure; frontend-proxy carrying scenario 9's errors), so its old change looked stale before its new fault
showed; one was a single-point run 30 minutes earlier (ad on capture B). It caught 2 of 9 past confidently-wrong answers
on capture 2 and 2 of 8 on capture B, one of them through noise, and not the LockContention "ad" answer (ad's CPU was
still raised at the page). Rescuing it would mean a new rule chosen after seeing these labels (a minimum run length,
"same service as the last incident"), which is tuning; it was dropped instead. `stale_change` stays as a measuring tool
in rulecheck.

## 12. The second look (patch 76, Week 7 design G2)

End-to-end only, on by default in the evals (`--recheck-minutes 2`) and in the live service: when the first look is about
to end unsure, the investigation looks again two minutes after the page instead.

- **Why:** on capture 2 four of the five end-to-end misses came 5-39 s into a fault, before the root cause's own signal
  moved, and the same graph was 10/10 a few minutes later (agent-only). The ceiling check (patch 75) found the labelled
  service in the candidate table with its own change at the page in 2 of 10 scenarios (capture B 4 of 9), and two
  minutes later in 10 of 10 (8 of 9), never past flag-off.
- **What it does:** triage and page_check again at `until + 2 min`; one more round of hypotheses with a fresh, smaller
  budget (6 tool calls, 6 model calls); the first look's hypotheses stay on the board. The model is told what changed: the
  services with an own change that wasn't there at the first look. A first-look hypothesis about such a service may be
  proposed again (same service and fault type); one about a service that didn't change may not. Still unsure: unsure.
- **Never:** after a committed answer (so it can't turn a right answer wrong, and it can't reach a confidently wrong one:
  on capture 2 the two confidently-wrong end-to-end answers stay out of its reach, so end-to-end tops out near 8/10),
  agent-only (no page), twice, or after a model error.
- **Live:** the node sleeps until the page + 2 min, computed from the saved first look, so a crash during the wait resumes
  only the rest of it (tested). In replay it only moves the query API's `until`; the `until` guarantee holds. The wait is
  part of time to diagnosis (the state's `recheck` record; patch 79 reports it).
- **Controls:** `--recheck-minutes off` (no second look) and `--recheck-minutes 0` (the same extra round at the page's
  moment: separates more time from more calls).
- The prompts changed (the second look's message), so GRAPH_PROMPT_VERSION changed; the Week 5 prompts didn't.

## 13. Every report ranks three services (patch 77, Week 7 design G4)

Top-3 counts the labelled service anywhere in a report's ranked list; top-1 counts only a committed answer; unsure is
never top-1 and never confidently wrong. Until now an unsure report ranked only the hypotheses it hadn't refuted, so on
capture 2 two unsure end-to-end reports (adHighCpu, recommendationCacheFailure) whose paged service was the root cause
ranked nothing. Now the list is the answer (if any), then the hypotheses not refuted (by confidence or prior), padded from
the candidate table in its order: the paged service, callees by depth, services changed with no call edge. The padding
may bring back a refuted service, deliberately: the page is evidence about the paged service whatever the model
concluded. `Diagnosis` keeps at most 3, de-duplicated.
