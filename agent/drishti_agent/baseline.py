"""The baseline: same model, one prompt, a fixed dump, no tools (design §8).

The dump is what the tools would show at a glance: the overview of every
service, the detector's alerts, and every service's top log templates, all
as of the incident, capped at DUMP_CHARS. It answers "does the loop add
anything over pasting the dashboards into one prompt?"
"""
from __future__ import annotations

import time

from .agent import parse_diagnosis
from .api import QueryAPI, call_tool
from .llm import LLMError
from .models import Diagnosis, RunResult
from .prompts import BASELINE_SYSTEM

DUMP_CHARS = 48_000  # ~12k tokens


def dump(api: QueryAPI, limit: int = DUMP_CHARS) -> str:
    parts = []
    _, text, _ = call_tool(api, "overview", {"window": "5m", "baseline": "30m"})
    parts.append(f"## overview (window 5m vs the 30m before)\n{text}")
    _, text, _ = call_tool(api, "anomalies", {"window": "15m"})
    parts.append(f"## detector alerts (last 15m)\n{text}")
    ok, text, _ = call_tool(api, "list_services", {})
    try:
        import json
        services = json.loads(text)["data"]["services"]
    except Exception:
        services = []
    for s in services:
        _, text, _ = call_tool(api, "log_templates", {"service": s, "window": "5m", "baseline": "30m"})
        parts.append(f"## log templates: {s}\n{text[:2500]}")
    out = "\n\n".join(parts)
    return out[:limit]


def diagnose(api: QueryAPI, llm) -> RunResult:
    start = time.monotonic()
    res = RunResult(system="baseline", diagnosis=Diagnosis.unsure("no answer"), valid_answer=False)
    snapshot = dump(api)
    msgs = [{"role": "user", "content": f"Telemetry snapshot at the moment of the incident:\n\n{snapshot}\n\nAnswer with the JSON object."}]
    try:
        reply = llm.chat(BASELINE_SYSTEM, msgs, None)
        res.llm_calls = 1
        res.usage.add(reply.usage)
        d = parse_diagnosis(reply.text)
        if d is not None:
            res.diagnosis, res.valid_answer = d, True
        else:
            res.diagnosis = Diagnosis.unsure("answer did not parse")
        msgs.append({"role": "assistant", "content": reply.text})
    except LLMError as e:
        res.error = str(e)
        res.diagnosis = Diagnosis.unsure(f"model error: {e}")
    res.seconds = round(time.monotonic() - start, 2)
    res.transcript = msgs
    return res
