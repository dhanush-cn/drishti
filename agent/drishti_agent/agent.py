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
from .prompts import AGENT_SYSTEM, SUBMIT_TOOL

DEFAULT_BUDGET = 12          # tool calls (not counting submit_diagnosis)
MAX_TURNS = 24               # model turns, a hard stop against a model that loops without calling tools


def triage(api: QueryAPI) -> str:
    """The deterministic first look, written into the first user message."""
    parts = []
    for name, args in (("overview", {"window": "5m", "baseline": "30m"}), ("anomalies", {"window": "15m"})):
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


def investigate(api: QueryAPI, llm, budget: int = DEFAULT_BUDGET) -> RunResult:
    start = time.monotonic()
    res = RunResult(system="agent", diagnosis=Diagnosis.unsure("no answer"), valid_answer=False)
    tools = TOOLS + [SUBMIT_TOOL]
    msgs: list[dict] = [{"role": "user", "content":
                         f"Incident in progress. Triage at the moment of the incident:\n\n{triage(api)}\n\n"
                         f"You have {budget} tool calls. Investigate, then call submit_diagnosis."}]
    used = 0
    try:
        for _turn in range(MAX_TURNS):
            out_of_budget = used >= budget
            reply = llm.chat(AGENT_SYSTEM, msgs, None if out_of_budget else tools)
            res.llm_calls += 1
            res.usage.add(reply.usage)
            calls = reply.tool_calls
            msgs.append({"role": "assistant", "content": reply.text,
                         "tool_calls": [{"id": c.id, "name": c.name, "arguments": c.arguments} for c in calls]})

            submit = next((c for c in calls if c.name == "submit_diagnosis"), None)
            if submit is not None:
                d = parse_diagnosis(submit.arguments)
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
