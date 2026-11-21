"""The Week 6 investigation: a LangGraph hypothesis graph (docs/agent.md section 2; design F2, F4, F5).

    triage (code) -> page_check (code) -> hypothesize (model) -> test: model <-> tools -> judge (code)
                                               ^                                            |
                                               +---- next round / next hypothesis ----------+
                                                       judge -> report | unsure

- Code does the mechanical parts: triage (as Week 5), page_check (call-graph walk, onsets, pagecheck.py), and the
  judge (grounding, rule F4, budgets). The model proposes hypotheses and tests them with the query tools.
- Every tool result gets an id ([t3]) that the model cites. The judge accepts a supported hypothesis only if a
  cited result is ABOUT that service: a per-service tool whose service argument is it, or its row in the overview
  or the alerts, or its row in the candidate table, or (for the paged service only) the page itself. An overview
  mentions every service, so "the result mentions it" would accept anything (second review).
- F4 (end-to-end only) is OFF by default since patch 69: on capture 2 and capture B its "own change" condition refused
  as many correct answers (5) as wrong ones (5), because at the first alert the root cause's own signal often hasn't
  crossed the thresholds yet; its earlier-incident condition never fired (docs/agent.md section 9). On (f4=True, the
  Week 7 variant): another service than the paged one needs its own signal change and must not be an earlier
  incident; refused once, sent back with the reason; refused twice, "unsure". The judge never swaps in a guess.
- Budgets: page_check has its own query cap; the model has `budget` tool calls in total (12, as v0), at most
  PER_HYPOTHESIS per hypothesis, MAX_ROUNDS rounds of hypotheses and MAX_LLM_CALLS model calls (quota: design §5).
  Running out ends in "unsure, here's what I ruled out", never a guess.
- Recheck (patch 76, Week 7 design G2; end-to-end only, off unless recheck_minutes is given): when the first pass is
  about to end unsure, the investigation looks again `recheck_minutes` later instead: triage and page_check at the new
  moment, one more round of hypotheses with a fresh, smaller budget (RECHECK_TOOLS tool calls, RECHECK_LLM_CALLS model
  calls), the first look's hypotheses kept. Why: on capture 2 four of the five end-to-end misses were 5-39 s into a
  fault, before the root cause's own signal moved, and the labelled service showed its own change two minutes later in
  10 of 10 scenarios (capture B: 8 of 9; rulecheck, 21 Nov). A committed answer is never rechecked, and there is no
  second recheck. In replay the moment just moves (the query API's `until`); live (live_wait) the node sleeps until the
  first look + recheck_minutes, computed from the saved state, so a crash during the wait resumes the rest of it.

State is plain data, one model turn per step, so a checkpointer can save it between steps (patch 61).
"""
from __future__ import annotations

import copy
import json
import re
import time
from datetime import timedelta
from typing import Callable, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from .agent import triage
from .api import TOOLS, QueryAPI, call_tool
from .llm import LLMError
from .models import Diagnosis, FaultType, RunResult, ToolRecord, Usage
from .pagecheck import DEFAULT_CAP, PageCheck, iso, norm, page_check, parse_ts, reassignment
from .prompts_graph import (BRIEFING_PAGED, BRIEFING_UNPAGED, GRAPH_SYSTEM, HYPOTHESIZE, MARK_TOOL, RECHECK,
                            SUBMIT_HYPOTHESES_TOOL, TEST)

DEFAULT_BUDGET = 12        # model tool calls in total, as v0 (page_check's queries are separate, design F5)
PER_HYPOTHESIS = 4
MAX_ROUNDS = 3
MAX_LLM_CALLS = 15         # one run of ~26 investigations stays inside a 500-request day (design §5)
CONFIDENT = 0.7
MAX_NUDGES = 2
MAX_F4_REFUSALS = 2        # the second refusal ends the investigation as unsure
RECURSION_LIMIT = 200      # backstop only: the call caps above end every run first
RECHECK_MINUTES = 2.0      # the design's delay (G2): the trend and onset rules work on 2-minute windows
RECHECK_TOOLS = 6          # the second look's fresh budget: tool calls ...
RECHECK_LLM_CALLS = 6      # ... and model calls (one round: a hypothesize call and a test or two)

PER_SERVICE_TOOLS = {"spans", "timeseries", "log_templates", "logs"}
FAULTS = {f.value for f in FaultType}


class State(TypedDict, total=False):
    context: str                    # briefing + triage + candidate table: the start of every model conversation
    pc: dict                        # PageCheck.to_json()
    hypotheses: list[dict]          # id, service, fault_type, prior, why, round, status, confidence, evidence, summary, note
    current: Optional[str]          # hypothesis under test
    msgs: list[dict]                # the current model conversation (a test, or one hypothesize call)
    transcript: list[dict]          # every conversation, in order, with phase markers
    tool_log: list[dict]            # id, name, arguments, ok, text, ms, hypothesis
    pending: list[dict]             # tool calls from the last test turn
    used: int                       # model tool calls run
    hyp_used: int                   # ... for the hypothesis under test
    llm_calls: int
    usage: dict
    round: int
    nudges: int
    hyp_nudges: int                 # hypothesize calls that gave nothing usable
    refusals: list[str]             # F4 refusals, with reasons
    verdicts: list[str]             # the judge's decisions, in words (streamed, and kept in the transcript)
    next: str                       # where the judge sends the run next
    done: bool
    diagnosis: Optional[dict]
    valid: bool
    budget_exhausted: bool
    error: Optional[str]
    until0: str                     # the first look's moment (the page)
    until: str                      # the moment the queries are made at now (until0, or until0 + recheck)
    cap_tools: int                  # tool calls allowed in total so far (budget; + RECHECK_TOOLS after a recheck)
    cap_llm: int                    # model calls allowed in total so far
    max_rounds: int
    recheck: Optional[dict]         # the second look: minutes, until, what changed, seconds waited (patch 76)
    proposal: Optional[dict]        # remediation (patch 65): the allowlisted action proposed, if any
    approval: Optional[dict]        # the human's decision: approve, by, at
    action: Optional[dict]          # what the executor did
    recovery: Optional[dict]        # what verify measured


def briefing(until: str, alert: Optional[dict]) -> str:
    if alert:
        series = ", ".join(f"{k}={v}" for k, v in (alert.get("series") or {}).items())
        return BRIEFING_PAGED.format(until=until, service=alert["service"], rule=alert["rule"],
                                     series=f" ({series})" if series else "", opened_at=alert["opened_at"])
    return BRIEFING_UNPAGED.format(until=until)


def board(hyps: list[dict]) -> str:
    """The hypotheses so far, as the model sees them."""
    if not hyps:
        return "Hypotheses so far: none."
    rows = []
    for h in hyps:
        rows.append(f"- {h['id']} (round {h['round']}): {h['service']} ({h['fault_type']}), prior {h['prior']:.2f}: {h['status']}"
                    + (f", confidence {h['confidence']:.2f}" if h.get("confidence") is not None else "")
                    + (f": {h['summary']}" if h.get("summary") else "") + (f" [judge: {h['note']}]" if h.get("note") else ""))
    return "Hypotheses so far:\n" + "\n".join(rows)


def about(entry: dict, service: str) -> bool:
    """Is this tool result about `service` itself (not merely mentioning it)?"""
    s = norm(service)
    if entry["name"] in PER_SERVICE_TOOLS:
        return norm((entry.get("arguments") or {}).get("service")) == s
    text = entry.get("text") or ""
    if entry["name"] == "overview":   # its row: "- svc: changes" or "svc: numbers" at the start of a line
        return re.search(rf"(?mi)^(- )?{re.escape(service)}:", text) is not None
    if entry["name"] == "anomalies":  # an alert whose service field is it
        return re.search(rf'"service":"{re.escape(service)}"', text, re.I) is not None
    return False


def grounded(h: dict, tool_log: list[dict], pc: PageCheck) -> list[str]:
    """The cited evidence ids that hold up."""
    by_id = {e["id"]: e for e in tool_log}
    ok = []
    for ref in h.get("evidence") or []:
        ref = str(ref).strip().strip("[]").lower()
        if ref == "candidates":
            c = pc.get(h["service"])
            if c is not None and c.own_change:
                ok.append(ref)
        elif ref == "page":   # the alert is the paged service's own change (patch 71); about no other service
            if pc.paged and norm(pc.paged) == norm(h["service"]):
                ok.append(ref)
        elif ref in by_id and by_id[ref]["ok"] and about(by_id[ref], h["service"]):
            ok.append(ref)
    return ok


def _f(x, default=0.0) -> float:
    try:
        return max(0.0, min(1.0, float(str(x).strip().rstrip("%")) / (100 if str(x).strip().endswith("%") else 1)))
    except (TypeError, ValueError):
        return default


def _fault(x) -> str:
    x = str(x or "unknown").strip().lower()
    return x if x in FAULTS else "unknown"


def hyp_keys(h: dict) -> set[tuple[str, str]]:
    """What a hypothesis has tested: its service with the fault type proposed and the one the test marked."""
    return {(norm(h["service"]), h["fault_type"]), (norm(h["service"]), h.get("proposed_fault", h["fault_type"]))}


def build_graph(api: QueryAPI, llm, budget: int = DEFAULT_BUDGET, alert: Optional[dict] = None, *, f4: bool = False,
                pc_cap: int = DEFAULT_CAP, extras: Optional[list[Callable[[dict], str]]] = None, checkpointer=None,
                after_node: Optional[Callable[[str], None]] = None, local_tools: Optional[dict] = None,
                remediation=None, thread_id: str = "", recheck_minutes: Optional[float] = None, live_wait: bool = False):
    """One investigation. api, llm, alert and the switches are fixed for its lifetime (closed over); everything
    that changes between steps is in State. `extras` add context for hypothesize (runbooks, memory; patches 63-64).
    `after_node(name)` runs after a node computes and before its result is saved (durable.py's crash point).
    `local_tools` (name -> (schema, function(arguments) -> (ok, text))) are offered in tests next to the query tools,
    counted against the same budget, and never evidence for the judge (runbooks, patch 63).
    `remediation` (remediate.Remediation, live only) adds propose -> approve (a human, via interrupt) -> execute ->
    verify after a report; without it a report ends the run, as in evals. Needs a checkpointer to wait for approval.
    `recheck_minutes` (end-to-end only; None = no recheck) and `live_wait` (sleep until the second look's moment): see
    the module docstring."""
    extras = extras or []
    local_tools = local_tools or {}
    test_tools = TOOLS + [schema for schema, _ in local_tools.values()]
    pc_holder: dict = {}
    apis: dict = {api.until: api}

    def api_at(state: State) -> QueryAPI:
        """The query API at the investigation's current moment (the page, or the second look's)."""
        until = state.get("until") or api.until
        if until not in apis:
            a = copy.copy(api)
            a.until = until
            apis[until] = a
        return apis[until]

    def pc_of(state: State) -> PageCheck:
        if pc_holder.get("until") != state.get("until"):
            pc_holder.pop("pc", None)
        if "pc" not in pc_holder:
            pc_holder["until"] = state.get("until")
            d = state["pc"]
            from .pagecheck import Candidate
            pc_holder["pc"] = PageCheck(**{**d, "candidates": [Candidate(**c) for c in d["candidates"]]})
        return pc_holder["pc"]

    def chat(state: State, msgs: list[dict], tools: list[dict]):
        """One model call, counted; the reply's turn appended verbatim (Gemini thought signatures)."""
        reply = llm.chat(GRAPH_SYSTEM, msgs, tools)
        usage = Usage.model_validate(state["usage"])
        usage.add(reply.usage)
        msgs = msgs + [{"role": "assistant", "content": reply.text,
                        "tool_calls": [{"id": c.id, "name": c.name, "arguments": c.arguments} for c in reply.tool_calls],
                        **({"raw": reply.raw} if reply.raw else {})}]
        return reply, msgs, {"llm_calls": state["llm_calls"] + 1, "usage": usage.model_dump()}

    def out_of_calls(state: State) -> bool:
        return state["llm_calls"] >= state.get("cap_llm", MAX_LLM_CALLS)

    def cap(state: State) -> int:
        return state.get("cap_tools", budget)

    # ---- nodes ----

    def triage_node(state: State) -> dict:
        return {"context": f"{briefing(api.until, alert)}\n\nTriage at that moment:\n\n{triage(api, alert)}",
                "hypotheses": [], "current": None, "msgs": [], "transcript": [], "tool_log": [], "pending": [],
                "used": 0, "hyp_used": 0, "llm_calls": 0, "usage": Usage().model_dump(), "round": 0, "nudges": 0,
                "hyp_nudges": 0, "refusals": [], "verdicts": [], "next": "", "done": False, "diagnosis": None,
                "valid": False, "budget_exhausted": False, "error": None, "until0": api.until, "until": api.until,
                "cap_tools": budget, "cap_llm": MAX_LLM_CALLS, "max_rounds": MAX_ROUNDS, "recheck": None}

    def page_check_node(state: State) -> dict:
        pc = page_check(api, alert, cap=pc_cap)
        pc_holder.update(pc=pc, until=state.get("until"))
        return {"pc": pc.to_json(), "context": state["context"] + "\n\n## candidates (code: call-graph walk from the page)\n" + pc.table()}

    def hypothesize_node(state: State) -> dict:
        if out_of_calls(state):
            return {"next": "unsure", "verdicts": state["verdicts"] + [f"model-call cap ({state.get('cap_llm', MAX_LLM_CALLS)}) reached"]}
        rnd = state["round"] + (0 if state["hyp_nudges"] else 1)
        extra = "\n\n".join(t for t in (e(dict(state)) for e in extras) if t)
        msgs = [{"role": "user", "content": state["context"] + (f"\n\n{extra}" if extra else "")
                 + f"\n\n{board(state['hypotheses'])}\n\n{HYPOTHESIZE}"}]
        try:
            reply, msgs, upd = chat(state, msgs, [SUBMIT_HYPOTHESES_TOOL])
        except LLMError as e:
            return {"next": "error", "error": str(e)}
        transcript = state["transcript"] + [{"role": "phase", "content": f"hypothesize, round {rnd}"}] + msgs
        call = next((c for c in reply.tool_calls if c.name == "submit_hypotheses"), None)
        items = None
        if call is not None and isinstance(call.arguments.get("hypotheses"), list):
            items = call.arguments["hypotheses"]
        elif not reply.tool_calls:
            m = re.search(r"\{.*\}", reply.text or "", re.S)   # some models answer in text
            try:
                items = json.loads(m.group(0)).get("hypotheses") if m else None
            except (ValueError, AttributeError):
                items = None
        base = {**upd, "transcript": transcript, "round": rnd}
        if items is None:
            if state["hyp_nudges"] + 1 >= MAX_NUDGES:
                return {**base, "next": "unsure", "verdicts": state["verdicts"] + ["no usable hypotheses from the model"]}
            return {**base, "hyp_nudges": state["hyp_nudges"] + 1, "next": "hypothesize"}
        # A hypothesis is a service AND a fault type: a service refuted as "error" can still be tested as "cpu"
        # (capture 2, 21 Nov: recommendation refuted as an error, its round-2 latency hypothesis was dropped).
        # Both the proposed type and the one the test marked count as tested. After a recheck, a first-look hypothesis
        # about a service whose own signal has changed since may be proposed again (design §3).
        again = {norm(s) for s in ((state.get("recheck") or {}).get("changed") or [])}
        tested = {k for h in state["hypotheses"] for k in hyp_keys(h)
                  if not (h.get("look", 1) == 1 and state.get("recheck") and k[0] in again)}
        hyps = list(state["hypotheses"])
        for it in items[:3]:
            if not isinstance(it, dict) or not str(it.get("service") or "").strip():
                continue
            svc, fault = str(it["service"]).strip(), _fault(it.get("fault_type"))
            if (norm(svc), fault) in tested:
                continue
            tested.add((norm(svc), fault))
            hyps.append({"id": f"h{len(hyps) + 1}", "service": svc, "fault_type": fault, "proposed_fault": fault,
                         "prior": _f(it.get("prior"), 0.3), "why": str(it.get("why") or ""), "round": rnd,
                         "status": "open", "confidence": None, "evidence": [], "summary": "", "note": "",
                         "look": 2 if state.get("recheck") else 1})
        new = [h for h in hyps if h["status"] == "open"]
        if not new:
            return {**base, "hypotheses": hyps, "hyp_nudges": 0, "next": "unsure",
                    "verdicts": state["verdicts"] + [f"round {rnd}: no untested hypothesis left"]}
        cur = max(new, key=lambda h: h["prior"])["id"]
        return {**base, "hypotheses": hyps, "hyp_nudges": 0, "current": cur, "msgs": [], "hyp_used": 0, "nudges": 0, "next": "test"}

    def test_model_node(state: State) -> dict:
        """One model turn of the current test."""
        if out_of_calls(state):
            return {"next": "unsure", "verdicts": state["verdicts"] + [f"model-call cap ({state.get('cap_llm', MAX_LLM_CALLS)}) reached"]}
        hyps = state["hypotheses"]
        h = next(x for x in hyps if x["id"] == state["current"])
        msgs = list(state["msgs"])
        transcript = list(state["transcript"])
        if not msgs:
            left = min(PER_HYPOTHESIS, cap(state) - state["used"])
            msgs = [{"role": "user", "content": state["context"] + f"\n\n{board(hyps)}\n\n" + TEST.format(
                hid=h["id"], service=h["service"], fault_type=h["fault_type"], prior=f"{h['prior']:.2f}", why=h["why"] or "-",
                left=left, total_left=cap(state) - state["used"])}]
            transcript.append({"role": "phase", "content": f"test {h['id']} {h['service']}"})
        can_query = state["hyp_used"] < PER_HYPOTHESIS and state["used"] < cap(state)
        try:
            reply, msgs2, upd = chat(state, msgs, (test_tools + [MARK_TOOL]) if can_query else [MARK_TOOL])
        except LLMError as e:
            return {"next": "error", "error": str(e)}
        base = {**upd, "msgs": msgs2, "transcript": transcript + msgs2[len(msgs) - 1:] if not state["msgs"] else transcript + msgs2[-1:]}
        mark = next((c for c in reply.tool_calls if c.name == "mark_hypothesis"), None)
        others = [c for c in reply.tool_calls if c.name != "mark_hypothesis"]
        if mark is not None and others and can_query:
            # A verdict sent with tool calls was written before their results existed (as v0's batched submit).
            msgs2.append({"role": "tool", "tool_call_id": mark.id, "name": mark.name, "content":
                          "not accepted: sent in the same turn as tool calls whose results you had not seen. "
                          "Read the results, then call mark_hypothesis on its own."})
            base["transcript"] = base["transcript"] + msgs2[-1:]
            mark = None
        if mark is not None:
            a = mark.arguments or {}
            status = str(a.get("status") or "").lower()
            status = status if status in ("supported", "refuted", "inconclusive") else "inconclusive"
            hyps = [dict(x) for x in hyps]
            cur = next(x for x in hyps if x["id"] == h["id"])
            ev = a.get("evidence") or []
            cur.update(status=status, confidence=_f(a.get("confidence"), 0.0), summary=str(a.get("summary") or ""),
                       evidence=[ev] if isinstance(ev, str) else [str(e) for e in ev],
                       fault_type=_fault(a.get("fault_type")) if a.get("fault_type") else cur["fault_type"])
            return {**base, "hypotheses": hyps, "next": "judge"}
        if not others:
            if state["nudges"] >= MAX_NUDGES:
                hyps = [dict(x) for x in hyps]
                next(x for x in hyps if x["id"] == h["id"]).update(status="inconclusive", note="no verdict from the model")
                return {**base, "hypotheses": hyps, "next": "judge"}
            msgs2.append({"role": "user", "content": "Use the tools to test the hypothesis, or call mark_hypothesis."})
            base["transcript"] = base["transcript"] + msgs2[-1:]
            return {**base, "nudges": state["nudges"] + 1, "next": "test"}
        if not can_query:  # tools weren't offered; the model called one anyway
            msgs2.append({"role": "user", "content": "No tool calls are left for this hypothesis: call mark_hypothesis now."})
            return {**base, "nudges": state["nudges"] + 1, "next": "test"}
        return {**base, "pending": [{"id": c.id, "name": c.name, "arguments": c.arguments} for c in others], "next": "tools"}

    def test_tools_node(state: State) -> dict:
        msgs, log = list(state["msgs"]), list(state["tool_log"])
        used, hyp_used = state["used"], state["hyp_used"]
        limit = cap(state)
        qapi = api_at(state)
        added = []
        for c in state["pending"]:
            if used >= limit or hyp_used >= PER_HYPOTHESIS:
                text = "error: no tool calls left for this hypothesis; call mark_hypothesis now"
            else:
                used, hyp_used = used + 1, hyp_used + 1
                if c["name"] in local_tools:
                    t0 = time.monotonic()
                    ok, res = local_tools[c["name"]][1](c["arguments"] or {})
                    ms = int((time.monotonic() - t0) * 1000)
                else:
                    ok, res, ms = call_tool(qapi, c["name"], c["arguments"])
                tid = f"t{len(log) + 1}"
                log.append({"id": tid, "name": c["name"], "arguments": {k: v for k, v in (c["arguments"] or {}).items() if k != "until"},
                            "ok": ok, "text": res, "ms": ms, "hypothesis": state["current"]})
                text = f"[{tid}] {res}"
            msgs.append({"role": "tool", "tool_call_id": c["id"], "name": c["name"], "content": text})
            added.append(msgs[-1])
        if used >= limit or hyp_used >= PER_HYPOTHESIS:
            msgs.append({"role": "user", "content": "No tool calls are left for this hypothesis. Call mark_hypothesis now."})
            added.append(msgs[-1])
        return {"msgs": msgs, "tool_log": log, "used": used, "hyp_used": hyp_used, "pending": [],
                "transcript": state["transcript"] + added, "budget_exhausted": used >= limit}

    def judge_node(state: State) -> dict:
        pc = pc_of(state)
        hyps = [dict(x) for x in state["hypotheses"]]
        h = next(x for x in hyps if x["id"] == state["current"])
        verdicts = list(state["verdicts"])
        refusals = list(state["refusals"])
        if h["status"] == "supported":
            ok_refs = grounded(h, state["tool_log"], pc)
            if not ok_refs:
                h.update(status="inconclusive", note=f"no cited evidence is about {h['service']} itself")
            elif h["confidence"] < CONFIDENT:
                h["note"] = f"supported at confidence {h['confidence']:.2f}, below {CONFIDENT}"
            else:
                allowed, why = reassignment(h["service"], pc) if (f4 and alert) else (True, "")
                if not allowed:
                    refusals.append(why)
                    h.update(status="refused", note=f"F4: {why}")
                    verdicts.append(f"{h['id']} refused: {why}")
                    if len(refusals) >= MAX_F4_REFUSALS:
                        return {"hypotheses": hyps, "refusals": refusals, "verdicts": verdicts + ["refused twice: unsure"], "next": "unsure"}
                else:
                    h["evidence"] = ok_refs
                    verdicts.append(f"{h['id']} accepted: {h['service']} ({h['fault_type']}), confidence {h['confidence']:.2f}"
                                    + (f"; {why}" if why else ""))
                    return {"hypotheses": hyps, "verdicts": verdicts, "next": "report"}
        verdicts.append(f"{h['id']} {h['service']}: {h['status']}" + (f" ({h['note']})" if h.get("note") else ""))
        budget_left = state["used"] < cap(state) and not out_of_calls(state)
        opened = [x for x in hyps if x["status"] == "open"]
        if opened and budget_left:
            cur = max(opened, key=lambda x: x["prior"])["id"]
            return {"hypotheses": hyps, "refusals": refusals, "verdicts": verdicts, "current": cur, "msgs": [],
                    "hyp_used": 0, "nudges": 0, "next": "test"}
        if state["round"] < state.get("max_rounds", MAX_ROUNDS) and budget_left:
            return {"hypotheses": hyps, "refusals": refusals, "verdicts": verdicts, "next": "hypothesize"}
        why = "tool budget exhausted" if state["used"] >= cap(state) else ("model-call cap reached" if out_of_calls(state)
                                                                           else f"{state.get('max_rounds', MAX_ROUNDS)} rounds of hypotheses")
        return {"hypotheses": hyps, "refusals": refusals, "verdicts": verdicts + [f"stop: {why}"], "next": "unsure",
                "budget_exhausted": state["used"] >= cap(state)}

    def _ranked(state: State, first: Optional[str] = None) -> list[str]:
        """Up to 3 services, for top-3 (patch 77, Week 7 design G4): the answer, then hypotheses not refuted (supported,
        open, inconclusive, refused; by confidence or prior), then padded from the candidate table in its order (the
        paged service, callees by depth, services changed with no edge). Every report has one, an unsure one too: on
        capture 2 two unsure end-to-end reports had the right paged service and an empty list. The padding may bring
        back a refuted service: the page is evidence about the paged service whatever the model concluded."""
        hyps = state["hypotheses"]
        order = {"supported": 0, "open": 1, "inconclusive": 2, "refused": 3, "refuted": 4}
        rest = sorted(hyps, key=lambda x: (order.get(x["status"], 5), -(x.get("confidence") or x["prior"])))
        out = [first] if first else []
        seen = {norm(first)} if first else set()
        for svc in [x["service"] for x in rest if x["status"] != "refuted"] + \
                   [c["service"] for c in (state.get("pc") or {}).get("candidates") or []]:
            if norm(svc) not in seen:
                out.append(svc)
                seen.add(norm(svc))
        return out[:3]

    def _ruled_out(hyps: list[dict]) -> list[str]:
        return [f"{x['service']} ({x['fault_type']}): {x['status']}" + (f": {x['summary']}" if x.get("summary") else "")
                + (f" [judge: {x['note']}]" if x.get("note") else "")
                for x in hyps if x["status"] in ("refuted", "inconclusive", "refused")]

    def report_node(state: State) -> dict:
        hyps = state["hypotheses"]
        h = next(x for x in hyps if x["id"] == state["current"])
        by_id = {e["id"]: e for e in state["tool_log"]}
        evidence = []
        for ref in h["evidence"]:
            if ref == "candidates":
                evidence.append(f"[candidates] {h['service']}: " + "; ".join(pc_of(state).get(h["service"]).own_change))
            elif ref == "page":
                evidence.append(f"[page] {alert['service']} {alert['rule']} opened at {alert['opened_at']}")
            else:
                e = by_id[ref]
                evidence.append(f"[{ref}] {e['name']} {json.dumps(e['arguments'], sort_keys=True)}: {e['text'][:200]}")
        d = Diagnosis(root_cause_service=h["service"], fault_type=FaultType(h["fault_type"]), confidence=h["confidence"],
                      ranked_services=_ranked(state, h["service"]), evidence=evidence, ruled_out=_ruled_out(hyps),
                      summary=h["summary"] or f"{h['service']} ({h['fault_type']})")
        return {"done": True, "diagnosis": d.model_dump(mode="json"), "valid": True}

    def unsure_node(state: State) -> dict:
        hyps = state["hypotheses"]
        why = state["verdicts"][-1] if state["verdicts"] else "no diagnosis"
        d = Diagnosis(root_cause_service=None, fault_type=FaultType.unknown, confidence=0.0, ranked_services=_ranked(state),
                      ruled_out=_ruled_out(hyps) + [f"refused by F4: {r}" for r in state["refusals"]],
                      summary=f"unsure: {why}")
        return {"done": True, "diagnosis": d.model_dump(mode="json"), "valid": True}

    def can_recheck(state: State) -> bool:
        return recheck_minutes is not None and alert is not None and not state.get("recheck")

    def recheck_node(state: State) -> dict:
        """The second look (design G2): the same alert, recheck_minutes later. Live, wait for that moment first."""
        target = parse_ts(state["until0"]) + timedelta(minutes=recheck_minutes)
        waited = 0.0
        if live_wait:
            from datetime import datetime, timezone
            t0 = time.monotonic()
            while (left := (target - datetime.now(timezone.utc)).total_seconds()) > 0:
                time.sleep(min(left, 5.0))
            waited = round(time.monotonic() - t0, 1)
        until = iso(target) if recheck_minutes else state["until0"]
        before = {norm(c["service"]): bool(c.get("own_change")) for c in state["pc"]["candidates"]}
        st = {**state, "until": until}
        qapi = api_at(st)
        pc = page_check(qapi, alert, cap=pc_cap)
        pc_holder.update(pc=pc, until=until)
        changed = [c.service for c in pc.candidates if c.own_change and not before.get(norm(c.service))]
        info = {"minutes": recheck_minutes, "until": until, "changed": changed, "waited_s": waited,
                "llm_calls_before": state["llm_calls"], "tools_before": state["used"],
                "pagecheck_calls_first": state["pc"].get("calls", 0)}
        note = (f"Own changes that were not there at the first look: {', '.join(changed)}." if changed
                else "No service shows an own change that wasn't there at the first look.")
        context = (f"{briefing(until, alert)}\n\nTriage at that moment:\n\n{triage(qapi, alert)}"
                   f"\n\n## candidates (code: call-graph walk from the page)\n{pc.table()}"
                   f"\n\n{RECHECK.format(minutes=f'{recheck_minutes:g}', until=until, changed=note, tools=RECHECK_TOOLS, calls=RECHECK_LLM_CALLS)}")
        return {"until": until, "pc": pc.to_json(), "context": context, "recheck": info,
                "cap_tools": state["used"] + RECHECK_TOOLS, "cap_llm": state["llm_calls"] + RECHECK_LLM_CALLS,
                "max_rounds": state["round"] + 1, "current": None, "msgs": [], "hyp_used": 0, "nudges": 0, "hyp_nudges": 0,
                "budget_exhausted": False, "next": "hypothesize",
                "verdicts": state["verdicts"] + [f"first look unsure: looking again at {until}"
                                                 + (f" (new own changes: {', '.join(changed)})" if changed else "")],
                "transcript": state["transcript"] + [{"role": "phase", "content": f"recheck at {until}"}]}

    def propose_node(state: State) -> dict:
        from .remediate import propose
        p = propose(state["diagnosis"])
        return {"proposal": p, "verdicts": state["verdicts"] + [f"proposed: {p['action']}({p['service']})" if p else "no action proposed"]}

    def approve_node(state: State) -> dict:
        # Pauses the run until a human answers (durable.decide); the state waits in Postgres meanwhile.
        decision = interrupt({"proposal": state["proposal"], "diagnosis": state["diagnosis"]})
        d = decision if isinstance(decision, dict) else {}
        approval = {"approve": bool(d.get("approve")), "by": str(d.get("by") or ""),
                    "at": d.get("at") or iso_now()}
        return {"approval": approval, "verdicts": state["verdicts"] + [
            f"{'approved' if approval['approve'] else 'rejected'} by {approval['by'] or 'nobody'}"]}

    def execute_node(state: State) -> dict:
        from .remediate import execute
        act = execute(state["proposal"], state["approval"], remediation.flags, thread_id)
        return {"action": act}

    def verify_node(state: State) -> dict:
        if not (state.get("action") or {}).get("ok"):
            return {"recovery": {"recovered": None, "why": "nothing was executed"}}
        return {"recovery": remediation.verifier.verify(state["pc"], state["proposal"]["service"])}

    def error_node(state: State) -> dict:
        d = Diagnosis.unsure(f"model error: {state['error']}", ruled_out=_ruled_out(state["hypotheses"]))
        return {"done": True, "diagnosis": d.model_dump(mode="json"), "valid": False}

    def hooked(name, fn):
        if after_node is None:
            return fn

        def run(state):
            out = fn(state)
            after_node(name)
            return out
        return run

    def route(state: State) -> str:
        return "recheck" if state["next"] == "unsure" and can_recheck(state) else state["next"]

    g = StateGraph(State)
    for name, fn in (("triage", triage_node), ("page_check", page_check_node), ("hypothesize", hypothesize_node),
                     ("test", test_model_node), ("tools", test_tools_node), ("judge", judge_node), ("report", report_node),
                     ("unsure", unsure_node), ("error", error_node), ("propose", propose_node), ("approve", approve_node),
                     ("execute", execute_node), ("verify", verify_node), ("recheck", recheck_node)):
        g.add_node(name, hooked(name, fn))
    g.add_edge(START, "triage")
    g.add_edge("triage", "page_check")
    g.add_edge("page_check", "hypothesize")
    g.add_conditional_edges("hypothesize", route, ["hypothesize", "test", "unsure", "error", "recheck"])
    g.add_conditional_edges("test", route, ["test", "tools", "judge", "unsure", "error", "recheck"])
    g.add_edge("tools", "test")
    g.add_conditional_edges("judge", route, ["test", "hypothesize", "report", "unsure", "recheck"])
    g.add_edge("recheck", "hypothesize")
    if remediation is None:
        g.add_edge("report", END)
    else:
        g.add_edge("report", "propose")
        g.add_conditional_edges("propose", lambda s: "approve" if s.get("proposal") else END, ["approve", END])
        g.add_conditional_edges("approve", lambda s: "execute" if (s.get("approval") or {}).get("approve") else END, ["execute", END])
        g.add_edge("execute", "verify")
        g.add_edge("verify", END)
    for n in ("unsure", "error"):
        g.add_edge(n, END)
    return g.compile(checkpointer=checkpointer)


def iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def result_from(s: dict, seconds: float) -> RunResult:
    """A RunResult from a finished graph state (the same shape the harness scores for v0)."""
    graph_info = {"role": "graph", "content": json.dumps({"hypotheses": s.get("hypotheses"), "verdicts": s.get("verdicts"),
                                                          "refusals": s.get("refusals"), "pagecheck": s.get("pc"),
                                                          "proposal": s.get("proposal"), "approval": s.get("approval"),
                                                          "action": s.get("action"), "recovery": s.get("recovery"),
                                                          "recheck": s.get("recheck")}, default=str)}
    return RunResult(system="graph", diagnosis=Diagnosis.model_validate(s["diagnosis"]), valid_answer=s["valid"],
                     budget_exhausted=s.get("budget_exhausted", False),
                     tool_calls=[ToolRecord(name=e["name"], arguments=e["arguments"], ok=e["ok"], chars=len(e["text"]), ms=e["ms"])
                                 for e in s.get("tool_log") or []],
                     llm_calls=s.get("llm_calls", 0), usage=Usage.model_validate(s.get("usage") or {}), error=s.get("error"),
                     transcript=(s.get("transcript") or []) + [graph_info], seconds=seconds)


def investigate(api: QueryAPI, llm, budget: int = DEFAULT_BUDGET, alert: Optional[dict] = None, *, f4: bool = False,
                pc_cap: int = DEFAULT_CAP, extras=None, local_tools=None, recheck_minutes: Optional[float] = None) -> RunResult:
    from .tracing import traced
    start = time.monotonic()
    s = build_graph(api, traced(llm), budget, alert, f4=f4, pc_cap=pc_cap, extras=extras, local_tools=local_tools,
                    recheck_minutes=recheck_minutes).invoke({}, {"recursion_limit": RECURSION_LIMIT})
    return result_from(s, round(time.monotonic() - start, 2))
