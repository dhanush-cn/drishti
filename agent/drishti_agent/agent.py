"""The investigation loop (Week 5: a plain tool-calling loop; Week 6 turns it
into a LangGraph hypothesis graph with a checkpointer).

    triage (deterministic) -> model -> tool calls -> model -> ... -> submit_diagnosis
                                 \\-- budget exhausted -> "unsure, here's what I ruled out"

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

from pydantic import ValidationError

from .api import TOOLS, QueryAPI, call_tool, compact
from .llm import LLMError
from .models import Diagnosis, RunResult, ToolRecord
from .prompts import (AGENT_SYSTEM, BRIEFING_PAGED, BRIEFING_UNPAGED, ONSET_CHECK, SUBMIT_TOOL, TRIAGE_BASELINE,
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


def investigate(api: QueryAPI, llm, budget: int = DEFAULT_BUDGET, alert: dict | None = None) -> RunResult:
    start = time.monotonic()
    res = RunResult(system="agent", diagnosis=Diagnosis.unsure("no answer"), valid_answer=False)
    tools = TOOLS + [SUBMIT_TOOL]
    msgs: list[dict] = [{"role": "user", "content":
                         f"{briefing(api, alert)}\n\nTriage at that moment:\n\n{triage(api, alert)}\n\n"
                         f"You have {budget} tool calls. Investigate, then call submit_diagnosis."}]
    used = 0
    nudges = 0
    checked = False
    try:
        for _turn in range(MAX_TURNS):
            out_of_budget = used >= budget
            reply = llm.chat(AGENT_SYSTEM, msgs, None if out_of_budget else tools)
            res.llm_calls += 1
            res.usage.add(reply.usage)
            calls = reply.tool_calls
            msgs.append({"role": "assistant", "content": reply.text,
                         "tool_calls": [{"id": c.id, "name": c.name, "arguments": c.arguments} for c in calls],
                         **({"raw": reply.raw} if reply.raw else {})})

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
                if (d and alert and not checked and not out_of_budget and d.root_cause_service
                        and d.root_cause_service != alert["service"]):
                    checked = True
                    res.onset_check = d.root_cause_service
                    msgs.append({"role": "tool", "tool_call_id": submit.id, "name": submit.name, "content":
                                 ONSET_CHECK.format(named=d.root_cause_service, service=alert["service"], opened_at=alert["opened_at"])})
                    continue
                res.diagnosis, res.valid_answer = (d, True) if d else (Diagnosis.unsure("invalid submit_diagnosis arguments"), False)
                res.budget_exhausted = out_of_budget
                break
            if not calls:
                # No tool call: an answer in text, or a stall.
                d = parse_diagnosis(reply.text)
                if d is not None:
                    res.diagnosis, res.valid_answer = d, True
                    res.budget_exhausted = out_of_budget
                    break
                if out_of_budget:
                    res.diagnosis = Diagnosis.unsure("tool budget exhausted without a diagnosis")
                    res.budget_exhausted = True
                    break
                if nudges >= MAX_NUDGES:
                    res.diagnosis = Diagnosis.unsure(f"no tool call and no diagnosis after {MAX_NUDGES} reminders")
                    break
                nudges += 1
                msgs.append({"role": "user", "content": "Use the tools to investigate, or call submit_diagnosis."})
                continue
            for c in calls:
                if used >= budget:
                    text, ok, ms = "error: tool budget exhausted; call submit_diagnosis now", False, 0
                else:
                    used += 1
                    ok, text, ms = call_tool(api, c.name, c.arguments)
                    res.tool_calls.append(ToolRecord(name=c.name, arguments=c.arguments, ok=ok, chars=len(text), ms=ms))
                msgs.append({"role": "tool", "tool_call_id": c.id, "name": c.name, "content": text})
            if used >= budget:
                msgs.append({"role": "user", "content":
                             "Tool budget exhausted. Submit your diagnosis now with submit_diagnosis (as JSON if you cannot call it). "
                             "If the evidence does not single out a service, answer root_cause_service null and list what you ruled out."})
        else:
            res.diagnosis = Diagnosis.unsure(f"no diagnosis after {MAX_TURNS} model turns")
    except LLMError as e:
        res.error = str(e)
        res.diagnosis = Diagnosis.unsure(f"model error: {e}")
    res.seconds = round(time.monotonic() - start, 2)
    res.transcript = msgs
    return res
