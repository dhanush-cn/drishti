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

Every call goes through a counted budget (`DEFAULT_CAP` = 8, design F5) and the walk stops when it runs out.
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
  `submit_hypotheses` with up to 3 untested ones (service, fault type, prior, why). Services already tested aren't
  tested again.
- **test:** one hypothesis at a time, highest prior first. The model calls the query tools (at most 4 per hypothesis)
  and ends with `mark_hypothesis`: supported, refuted or inconclusive, a confidence, and the ids of the results that
  show it (`[t3]`, or `candidates` for its row in the table). A verdict sent in the same turn as tool calls is
  refused, as Week 5 refuses a batched answer.
- **judge (code):**
  1. *Grounding:* a cited result counts only if it is **about** the hypothesis's service: a per-service tool whose
     `service` argument is it, its own row in the overview (a line starting with it) or an alert whose `service` is
     it, or its candidate row when that shows an own change. A failed query counts for nothing. An overview mentions
     every service, so "the result mentions it" would accept anything.
  2. Supported, grounded, confidence ≥ 0.7, and (end-to-end, unless switched off with `--no-f4`) F4 → **report**.
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
