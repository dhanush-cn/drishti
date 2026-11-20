"""The investigation loop, as a LangGraph graph (patch 56: the Week 5 loop, ported with no behaviour change;
tests/test_graph_parity.py holds it to v0's recorded results on every path).

    triage (code) -> model -> tools -> model -> ... -> submit_diagnosis
                       \\-- budget exhausted -> "unsure, here's what I ruled out"

Why a graph for the same loop: Week 6 needs persisted state (resume after a crash), a pause for a human, and
an explicit hypothesis loop. Those need the loop's state in one object that is saved between steps instead of
local variables of one function. Each model turn is its own step, so a checkpointer (patch 60) loses at most
one model call to a crash. The rules below are unchanged from Week 5:

1. Triage first, no LLM: the harness calls overview and anomalies itself and
   hands both to the model in the first message. Detection is deterministic
   code's job; the model starts where an engineer would, at "what changed".
2. Loop: each model turn may call tools; results go back as compact JSON. The
   time of the investigation is fixed by the harness (api.QueryAPI.until).
3. It ends when the model calls submit_diagnosis (parsed into Diagnosis,
   checked by Pydantic), or when the tool budget runs out. Out of budget, the
   model gets one last turn with no tools to submit; it is told to say
   "unsure" if the evidence isn't there. It never gets more budget, and the
   harness never fills in an answer.
"""
from __future__ import annotations

import json
import re
import time

from typing import Optional, TypedDict

from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from .api import TOOLS, QueryAPI, call_tool, compact
from .llm import LLMError
from .models import Diagnosis, RunResult, ToolRecord, Usage
from .prompts import (AGENT_SYSTEM, BRIEFING_PAGED, BRIEFING_UNPAGED, SUBMIT_TOOL, TRIAGE_BASELINE,
                      TRIAGE_BASELINE_PAGED, TRIAGE_WINDOW)

DEFAULT_BUDGET = 12          # tool calls (not counting submit_diagnosis)
MAX_TURNS = 16               # model turns, a hard stop
MAX_NUDGES = 2               # turns that neither call a tool nor answer, before the run stops as unsure



def briefing(api: QueryAPI, alert: dict | None) -> str:
    """How the investigation started, as an on-call engineer would be told."""
    if alert:
        series = ", ".join(f"{k}={v}" for k, v in (alert.get("series") or {}).items())
        return BRIEFING_PAGED.format(until=api.until, service=alert["service"], rule=alert["rule"],
                                     series=f" ({series})" if series else "", opened_at=alert["opened_at"])
    return BRIEFING_UNPAGED.format(until=api.until)


def triage_baseline(alert: dict | None) -> str:
    return TRIAGE_BASELINE_PAGED if alert else TRIAGE_BASELINE


def triage(api: QueryAPI, alert: dict | None = None) -> str:
    """The deterministic first look, written into the first user message."""
    parts = []
    for name, args in (("overview", {"window": TRIAGE_WINDOW, "baseline": triage_baseline(alert)}), ("anomalies", {"window": "15m"})):
        ok, text, _ = call_tool(api, name, args)
        parts.append(f"## {name} {json.dumps(args)}\n{text}")
    return "\n\n".join(parts)


def parse_diagnosis(args_or_text) -> Diagnosis | None:
    """A Diagnosis from submit_diagnosis arguments, or from a JSON object in text
    (some models answer in text instead of calling the tool). None if neither parses."""
    obj = args_or_text
    if isinstance(args_or_text, str):
        m = re.search(r"\{.*\}", args_or_text, re.S)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except ValueError:
            return None
    if not isinstance(obj, dict):
        return None
    try:
        return Diagnosis.model_validate(obj)
    except ValidationError:
        return None


class State(TypedDict, total=False):
    """Everything the loop knows between steps. Plain data only (dicts, lists, numbers, strings), so a
    checkpointer can save it as is. Pydantic objects are built from it at the end (RunResult)."""
    msgs: list[dict]                 # the conversation, as sent to the model (Week 5's transcript)
    used: int                        # tool calls run (submit_diagnosis not counted)
    nudges: int                      # turns that neither called a tool nor answered
    turns: int                       # model turns taken
    pending: list[dict]              # tool calls from the last model turn, still to run
    tool_records: list[dict]         # ToolRecord dicts
    llm_calls: int
    usage: dict                      # Usage dict
    done: bool                       # an answer, an unsure stop, or an error ended the run
    diagnosis: Optional[dict]        # Diagnosis dict, once decided
    valid: bool
    budget_exhausted: bool
    error: Optional[str]


def _decide(d: Diagnosis, valid: bool, **extra) -> dict:
    return {"done": True, "diagnosis": d.model_dump(mode="json"), "valid": valid, **extra}


def build_graph(api: QueryAPI, llm, budget: int = DEFAULT_BUDGET, alert: dict | None = None):
    """The loop for one investigation. api, llm, budget and alert are fixed for its lifetime, so they are
    closed over instead of kept in State: only what changes from step to step is state."""
    tools = TOOLS + [SUBMIT_TOOL]

    def triage_node(state: State) -> dict:
        first = (f"{briefing(api, alert)}\n\nTriage at that moment:\n\n{triage(api, alert)}\n\n"
                 f"You have {budget} tool calls. Investigate, then call submit_diagnosis.")
        return {"msgs": [{"role": "user", "content": first}], "used": 0, "nudges": 0, "turns": 0, "pending": [],
                "tool_records": [], "llm_calls": 0, "usage": Usage().model_dump(), "done": False, "diagnosis": None,
                "valid": False, "budget_exhausted": False, "error": None}

    def model_node(state: State) -> dict:
        """One model turn: the reply, and what it means (an answer, tool calls to run, a stall)."""
        msgs, used, nudges = list(state["msgs"]), state["used"], state["nudges"]
        out_of_budget = used >= budget
        upd: dict = {"turns": state["turns"] + 1, "pending": []}
        try:
            reply = llm.chat(AGENT_SYSTEM, msgs, None if out_of_budget else tools)
        except LLMError as e:
            return {**upd, **_decide(Diagnosis.unsure(f"model error: {e}"), False, error=str(e))}
        usage = Usage.model_validate(state["usage"])
        usage.add(reply.usage)
        upd.update(llm_calls=state["llm_calls"] + 1, usage=usage.model_dump())
        calls = reply.tool_calls
        msgs.append({"role": "assistant", "content": reply.text,
                     "tool_calls": [{"id": c.id, "name": c.name, "arguments": c.arguments} for c in calls],
                     **({"raw": reply.raw} if reply.raw else {})})
        upd["msgs"] = msgs

        submit = next((c for c in calls if c.name == "submit_diagnosis"), None)
        others = [c for c in calls if c.name != "submit_diagnosis"]
        if submit is not None and others and not out_of_budget:
            # An answer sent in the same turn as tool calls was written before their results existed
            # (capture 2, v2: 4 of 60 runs, all wrong). Run the calls, refuse the answer, ask again.
            calls = others
            msgs.append({"role": "tool", "tool_call_id": submit.id, "name": submit.name, "content":
                         "not accepted: submitted in the same turn as tool calls whose results you had not seen. "
                         "Read the results below, then call submit_diagnosis on its own."})
            submit = None
        if submit is not None:
            d = parse_diagnosis(submit.arguments)
            decided = (d, True) if d else (Diagnosis.unsure("invalid submit_diagnosis arguments"), False)
            return {**upd, **_decide(*decided, budget_exhausted=out_of_budget)}
        if not calls:
            # No tool call: an answer in text, or a stall.
            d = parse_diagnosis(reply.text)
            if d is not None:
                return {**upd, **_decide(d, True, budget_exhausted=out_of_budget)}
            if out_of_budget:
                return {**upd, **_decide(Diagnosis.unsure("tool budget exhausted without a diagnosis"), False, budget_exhausted=True)}
            if nudges >= MAX_NUDGES:
                return {**upd, **_decide(Diagnosis.unsure(f"no tool call and no diagnosis after {MAX_NUDGES} reminders"), False)}
            msgs.append({"role": "user", "content": "Use the tools to investigate, or call submit_diagnosis."})
            return {**upd, "nudges": nudges + 1}
        upd["pending"] = [{"id": c.id, "name": c.name, "arguments": c.arguments} for c in calls]
        return upd

    def tools_node(state: State) -> dict:
        """Run the last turn's tool calls (never past the budget) and hand the results back."""
        msgs, used, records = list(state["msgs"]), state["used"], list(state["tool_records"])
        for c in state["pending"]:
            if used >= budget:
                text = "error: tool budget exhausted; call submit_diagnosis now"
            else:
                used += 1
                ok, text, ms = call_tool(api, c["name"], c["arguments"])
                records.append(ToolRecord(name=c["name"], arguments=c["arguments"], ok=ok, chars=len(text), ms=ms).model_dump())
            msgs.append({"role": "tool", "tool_call_id": c["id"], "name": c["name"], "content": text})
        if used >= budget:
            msgs.append({"role": "user", "content":
                         "Tool budget exhausted. Submit your diagnosis now with submit_diagnosis (as JSON if you cannot call it). "
                         "If the evidence does not single out a service, answer root_cause_service null and list what you ruled out."})
        return {"msgs": msgs, "used": used, "tool_records": records, "pending": []}

    def turn_limit_node(state: State) -> dict:
        return _decide(Diagnosis.unsure(f"no diagnosis after {MAX_TURNS} model turns"), False)

    def after_model(state: State) -> str:
        if state["done"]:
            return END
        if state["pending"]:
            return "tools"
        return "turn_limit" if state["turns"] >= MAX_TURNS else "model"

    def after_tools(state: State) -> str:
        return "turn_limit" if state["turns"] >= MAX_TURNS else "model"

    g = StateGraph(State)
    g.add_node("triage", triage_node)
    g.add_node("model", model_node)
    g.add_node("tools", tools_node)
    g.add_node("turn_limit", turn_limit_node)
    g.add_edge(START, "triage")
    g.add_edge("triage", "model")
    g.add_conditional_edges("model", after_model, ["tools", "model", "turn_limit", END])
    g.add_conditional_edges("tools", after_tools, ["model", "turn_limit"])
    g.add_edge("turn_limit", END)
    return g.compile()


# A backstop on graph steps. The longest legitimate run is triage + MAX_TURNS model turns + a tool step after
# each + the turn-limit step = 2 * MAX_TURNS + 2 (34), and MAX_TURNS ends it first. LangGraph's own default is
# 10,007 in 1.x (it was 25 before), so without this a routing bug would make model calls until that.
RECURSION_LIMIT = 2 * MAX_TURNS + 8


def investigate(api: QueryAPI, llm, budget: int = DEFAULT_BUDGET, alert: dict | None = None) -> RunResult:
    start = time.monotonic()
    s = build_graph(api, llm, budget, alert).invoke({}, {"recursion_limit": RECURSION_LIMIT})
    res = RunResult(system="agent", diagnosis=Diagnosis.model_validate(s["diagnosis"]), valid_answer=s["valid"],
                    budget_exhausted=s["budget_exhausted"], tool_calls=[ToolRecord.model_validate(t) for t in s["tool_records"]],
                    llm_calls=s["llm_calls"], usage=Usage.model_validate(s["usage"]), error=s["error"], transcript=s["msgs"])
    res.seconds = round(time.monotonic() - start, 2)
    return res
