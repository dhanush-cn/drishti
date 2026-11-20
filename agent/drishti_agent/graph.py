"""The Week 6 investigation: a LangGraph hypothesis graph (docs/agent.md section 2; design F2, F4, F5).

    triage (code) -> page_check (code) -> hypothesize (model) -> test: model <-> tools -> judge (code)
                                               ^                                            |
                                               +---- next round / next hypothesis ----------+
                                                       judge -> report | unsure

- Code does the mechanical parts: triage (as Week 5), page_check (call-graph walk, onsets, pagecheck.py), and the
  judge (grounding, rule F4, budgets). The model proposes hypotheses and tests them with the query tools.
- Every tool result gets an id ([t3]) that the model cites. The judge accepts a supported hypothesis only if a
  cited result is ABOUT that service: a per-service tool whose service argument is it, or its row in the overview
  or the alerts, or its row in the candidate table. An overview mentions every service, so "the result mentions
  it" would accept anything (second review).
- F4 (end-to-end only) is OFF by default since patch 69: on capture 2 and capture B its "own change" condition refused
  as many correct answers (5) as wrong ones (5), because at the first alert the root cause's own signal often hasn't
  crossed the thresholds yet; its earlier-incident condition never fired (docs/agent.md section 9). On (f4=True, the
  Week 7 variant): another service than the paged one needs its own signal change and must not be an earlier
  incident; refused once, sent back with the reason; refused twice, "unsure". The judge never swaps in a guess.
- Budgets: page_check has its own query cap; the model has `budget` tool calls in total (12, as v0), at most
  PER_HYPOTHESIS per hypothesis, MAX_ROUNDS rounds of hypotheses and MAX_LLM_CALLS model calls (quota: design §5).
  Running out ends in "unsure, here's what I ruled out", never a guess.

State is plain data, one model turn per step, so a checkpointer can save it between steps (patch 61).
"""
from __future__ import annotations

import json
import re
import time
from typing import Callable, Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from .agent import triage
from .api import TOOLS, QueryAPI, call_tool
from .llm import LLMError
from .models import Diagnosis, FaultType, RunResult, ToolRecord, Usage
from .pagecheck import DEFAULT_CAP, PageCheck, norm, page_check, reassignment
from .prompts_graph import (BRIEFING_PAGED, BRIEFING_UNPAGED, GRAPH_SYSTEM, HYPOTHESIZE, MARK_TOOL,
                            SUBMIT_HYPOTHESES_TOOL, TEST)

DEFAULT_BUDGET = 12        # model tool calls in total, as v0 (page_check's queries are separate, design F5)
PER_HYPOTHESIS = 4
MAX_ROUNDS = 3
MAX_LLM_CALLS = 15         # one run of ~26 investigations stays inside a 500-request day (design §5)
CONFIDENT = 0.7
MAX_NUDGES = 2
MAX_F4_REFUSALS = 2        # the second refusal ends the investigation as unsure
RECURSION_LIMIT = 200      # backstop only: the call caps above end every run first

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


def build_graph(api: QueryAPI, llm, budget: int = DEFAULT_BUDGET, alert: Optional[dict] = None, *, f4: bool = False,
                pc_cap: int = DEFAULT_CAP, extras: Optional[list[Callable[[dict], str]]] = None, checkpointer=None,
                after_node: Optional[Callable[[str], None]] = None, local_tools: Optional[dict] = None,
                remediation=None, thread_id: str = ""):
    """One investigation. api, llm, alert and the switches are fixed for its lifetime (closed over); everything
    that changes between steps is in State. `extras` add context for hypothesize (runbooks, memory; patches 63-64).
    `after_node(name)` runs after a node computes and before its result is saved (durable.py's crash point).
    `local_tools` (name -> (schema, function(arguments) -> (ok, text))) are offered in tests next to the query tools,
    counted against the same budget, and never evidence for the judge (runbooks, patch 63).
    `remediation` (remediate.Remediation, live only) adds propose -> approve (a human, via interrupt) -> execute ->
    verify after a report; without it a report ends the run, as in evals. Needs a checkpointer to wait for approval."""
    extras = extras or []
    local_tools = local_tools or {}
    test_tools = TOOLS + [schema for schema, _ in local_tools.values()]
    pc_holder: dict = {}

    def pc_of(state: State) -> PageCheck:
        if "pc" not in pc_holder:
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
        return state["llm_calls"] >= MAX_LLM_CALLS

    # ---- nodes ----

    def triage_node(state: State) -> dict:
        return {"context": f"{briefing(api.until, alert)}\n\nTriage at that moment:\n\n{triage(api, alert)}",
                "hypotheses": [], "current": None, "msgs": [], "transcript": [], "tool_log": [], "pending": [],
                "used": 0, "hyp_used": 0, "llm_calls": 0, "usage": Usage().model_dump(), "round": 0, "nudges": 0,
                "hyp_nudges": 0, "refusals": [], "verdicts": [], "next": "", "done": False, "diagnosis": None,
                "valid": False, "budget_exhausted": False, "error": None}

    def page_check_node(state: State) -> dict:
        pc = page_check(api, alert, cap=pc_cap)
        pc_holder["pc"] = pc
        return {"pc": pc.to_json(), "context": state["context"] + "\n\n## candidates (code: call-graph walk from the page)\n" + pc.table()}

    def hypothesize_node(state: State) -> dict:
        if out_of_calls(state):
            return {"next": "unsure", "verdicts": state["verdicts"] + [f"model-call cap ({MAX_LLM_CALLS}) reached"]}
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
        tested = {norm(h["service"]) for h in state["hypotheses"]}
        hyps = list(state["hypotheses"])
        for it in items[:3]:
            if not isinstance(it, dict) or not str(it.get("service") or "").strip():
                continue
            svc = str(it["service"]).strip()
            if norm(svc) in tested:
                continue
            tested.add(norm(svc))
            hyps.append({"id": f"h{len(hyps) + 1}", "service": svc, "fault_type": _fault(it.get("fault_type")),
                         "prior": _f(it.get("prior"), 0.3), "why": str(it.get("why") or ""), "round": rnd,
                         "status": "open", "confidence": None, "evidence": [], "summary": "", "note": ""})
        new = [h for h in hyps if h["status"] == "open"]
        if not new:
            return {**base, "hypotheses": hyps, "hyp_nudges": 0, "next": "unsure",
                    "verdicts": state["verdicts"] + [f"round {rnd}: no untested hypothesis left"]}
        cur = max(new, key=lambda h: h["prior"])["id"]
        return {**base, "hypotheses": hyps, "hyp_nudges": 0, "current": cur, "msgs": [], "hyp_used": 0, "nudges": 0, "next": "test"}

    def test_model_node(state: State) -> dict:
        """One model turn of the current test."""
        if out_of_calls(state):
            return {"next": "unsure", "verdicts": state["verdicts"] + [f"model-call cap ({MAX_LLM_CALLS}) reached"]}
        hyps = state["hypotheses"]
        h = next(x for x in hyps if x["id"] == state["current"])
        msgs = list(state["msgs"])
        transcript = list(state["transcript"])
        if not msgs:
            left = min(PER_HYPOTHESIS, budget - state["used"])
            msgs = [{"role": "user", "content": state["context"] + f"\n\n{board(hyps)}\n\n" + TEST.format(
                hid=h["id"], service=h["service"], fault_type=h["fault_type"], prior=f"{h['prior']:.2f}", why=h["why"] or "-",
                left=left, total_left=budget - state["used"])}]
            transcript.append({"role": "phase", "content": f"test {h['id']} {h['service']}"})
        can_query = state["hyp_used"] < PER_HYPOTHESIS and state["used"] < budget
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
        added = []
        for c in state["pending"]:
            if used >= budget or hyp_used >= PER_HYPOTHESIS:
                text = "error: no tool calls left for this hypothesis; call mark_hypothesis now"
            else:
                used, hyp_used = used + 1, hyp_used + 1
                if c["name"] in local_tools:
                    t0 = time.monotonic()
                    ok, res = local_tools[c["name"]][1](c["arguments"] or {})
                    ms = int((time.monotonic() - t0) * 1000)
                else:
                    ok, res, ms = call_tool(api, c["name"], c["arguments"])
                tid = f"t{len(log) + 1}"
                log.append({"id": tid, "name": c["name"], "arguments": {k: v for k, v in (c["arguments"] or {}).items() if k != "until"},
                            "ok": ok, "text": res, "ms": ms, "hypothesis": state["current"]})
                text = f"[{tid}] {res}"
            msgs.append({"role": "tool", "tool_call_id": c["id"], "name": c["name"], "content": text})
            added.append(msgs[-1])
        if used >= budget or hyp_used >= PER_HYPOTHESIS:
            msgs.append({"role": "user", "content": "No tool calls are left for this hypothesis. Call mark_hypothesis now."})
            added.append(msgs[-1])
        return {"msgs": msgs, "tool_log": log, "used": used, "hyp_used": hyp_used, "pending": [],
                "transcript": state["transcript"] + added, "budget_exhausted": used >= budget}

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
        budget_left = state["used"] < budget and not out_of_calls(state)
        opened = [x for x in hyps if x["status"] == "open"]
        if opened and budget_left:
            cur = max(opened, key=lambda x: x["prior"])["id"]
            return {"hypotheses": hyps, "refusals": refusals, "verdicts": verdicts, "current": cur, "msgs": [],
                    "hyp_used": 0, "nudges": 0, "next": "test"}
        if state["round"] < MAX_ROUNDS and budget_left:
            return {"hypotheses": hyps, "refusals": refusals, "verdicts": verdicts, "next": "hypothesize"}
        why = "tool budget exhausted" if state["used"] >= budget else ("model-call cap reached" if out_of_calls(state)
                                                                        else f"{MAX_ROUNDS} rounds of hypotheses")
        return {"hypotheses": hyps, "refusals": refusals, "verdicts": verdicts + [f"stop: {why}"], "next": "unsure",
                "budget_exhausted": state["used"] >= budget}

    def _ranked(hyps: list[dict], first: Optional[str] = None) -> list[str]:
        order = {"supported": 0, "open": 1, "inconclusive": 2, "refused": 3, "refuted": 4}
        rest = sorted(hyps, key=lambda x: (order.get(x["status"], 5), -(x.get("confidence") or x["prior"])))
        out = [first] if first else []
        for x in rest:
            if x["status"] != "refuted" and x["service"] not in out:
                out.append(x["service"])
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
            else:
                e = by_id[ref]
                evidence.append(f"[{ref}] {e['name']} {json.dumps(e['arguments'], sort_keys=True)}: {e['text'][:200]}")
        d = Diagnosis(root_cause_service=h["service"], fault_type=FaultType(h["fault_type"]), confidence=h["confidence"],
                      ranked_services=_ranked(hyps, h["service"]), evidence=evidence, ruled_out=_ruled_out(hyps),
                      summary=h["summary"] or f"{h['service']} ({h['fault_type']})")
        return {"done": True, "diagnosis": d.model_dump(mode="json"), "valid": True}

    def unsure_node(state: State) -> dict:
        hyps = state["hypotheses"]
        why = state["verdicts"][-1] if state["verdicts"] else "no diagnosis"
        d = Diagnosis(root_cause_service=None, fault_type=FaultType.unknown, confidence=0.0, ranked_services=_ranked(hyps),
                      ruled_out=_ruled_out(hyps) + [f"refused by F4: {r}" for r in state["refusals"]],
                      summary=f"unsure: {why}")
        return {"done": True, "diagnosis": d.model_dump(mode="json"), "valid": True}

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

    route = lambda state: state["next"]
    g = StateGraph(State)
    for name, fn in (("triage", triage_node), ("page_check", page_check_node), ("hypothesize", hypothesize_node),
                     ("test", test_model_node), ("tools", test_tools_node), ("judge", judge_node), ("report", report_node),
                     ("unsure", unsure_node), ("error", error_node), ("propose", propose_node), ("approve", approve_node),
                     ("execute", execute_node), ("verify", verify_node)):
        g.add_node(name, hooked(name, fn))
    g.add_edge(START, "triage")
    g.add_edge("triage", "page_check")
    g.add_edge("page_check", "hypothesize")
    g.add_conditional_edges("hypothesize", route, ["hypothesize", "test", "unsure", "error"])
    g.add_conditional_edges("test", route, ["test", "tools", "judge", "unsure", "error"])
    g.add_edge("tools", "test")
    g.add_conditional_edges("judge", route, ["test", "hypothesize", "report", "unsure"])
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
                                                          "action": s.get("action"), "recovery": s.get("recovery")}, default=str)}
    return RunResult(system="graph", diagnosis=Diagnosis.model_validate(s["diagnosis"]), valid_answer=s["valid"],
                     budget_exhausted=s.get("budget_exhausted", False),
                     tool_calls=[ToolRecord(name=e["name"], arguments=e["arguments"], ok=e["ok"], chars=len(e["text"]), ms=e["ms"])
                                 for e in s.get("tool_log") or []],
                     llm_calls=s.get("llm_calls", 0), usage=Usage.model_validate(s.get("usage") or {}), error=s.get("error"),
                     transcript=(s.get("transcript") or []) + [graph_info], seconds=seconds)


def investigate(api: QueryAPI, llm, budget: int = DEFAULT_BUDGET, alert: Optional[dict] = None, *, f4: bool = False,
                pc_cap: int = DEFAULT_CAP, extras=None, local_tools=None) -> RunResult:
    from .tracing import traced
    start = time.monotonic()
    s = build_graph(api, traced(llm), budget, alert, f4=f4, pc_cap=pc_cap, extras=extras,
                    local_tools=local_tools).invoke({}, {"recursion_limit": RECURSION_LIMIT})
    return result_from(s, round(time.monotonic() - start, 2))
