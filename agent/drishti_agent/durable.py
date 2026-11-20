"""Investigations that survive a crash: the graph's state in Postgres (design F6; docs/agent.md section 3).

LangGraph saves the state after every step. A step that was running when the process died is run again on resume;
everything before it is not. Each model turn is its own step (graph.py), so a crash costs at most one repeated model
call. Query tool calls are read-only, so running one again is harmless. The one step with an effect, executing an
approved fix (patch 65), is written to be idempotent.

- thread_id = investigation id = hash(alert id, until): starting the same alert twice resumes it instead of
  duplicating it, and a finished investigation is returned as it was, with no model call.
- DRISHTI_AGENT_CRASH_AFTER=<node>:<n> exits with status 3 right after the n-th run of that node computes and before
  its result is saved, as storage's -debug-crash-after-inserts does (a kill at the worst moment, on purpose).
- Evals use no checkpointer (or the in-memory one): a replay doesn't need Postgres and its results don't depend on it.
"""
from __future__ import annotations

import hashlib
import os
import time
from contextlib import contextmanager
from typing import Optional

from . import graph
from .tracing import traced
from .api import QueryAPI
from .models import RunResult

CRASH_ENV = "DRISHTI_AGENT_CRASH_AFTER"

# "sync": each step's checkpoint is written before the next step starts. LangGraph's default ("async") writes it
# while the next step runs, so a killed process can lose the last finished step and redo it: the process test
# caught exactly that (hypothesize ran twice after a kill in the following test turn). A step here is a model call
# or a query, so a synchronous write of a few KB costs little next to it.
DURABILITY = "sync"


class Crash(Exception):
    """Raised by the crash point in tests (mode "raise"); a real crash is os._exit(3)."""


def investigation_id(alert: Optional[dict], until: str) -> str:
    key = f"{(alert or {}).get('id') or (alert or {}).get('service') or 'no-alert'}|{(alert or {}).get('opened_at', '')}|{until}"
    return "inv-" + hashlib.sha256(key.encode()).hexdigest()[:16]


def crash_point(spec: Optional[str], mode: str = "exit"):
    """A hook for build_graph: called after each node computes; ends the process at the chosen run of a node."""
    if not spec:
        return None
    node, _, n = spec.partition(":")
    want, seen = int(n or 1), {"n": 0}

    def hook(name: str):
        if name != node:
            return
        seen["n"] += 1
        if seen["n"] == want:
            if mode == "raise":
                raise Crash(f"crash after {node} #{want}")
            os._exit(3)
    return hook


@contextmanager
def postgres_saver(url: str):
    """A LangGraph Postgres checkpointer, its tables created if missing."""
    from langgraph.checkpoint.postgres import PostgresSaver
    with PostgresSaver.from_conn_string(url) as saver:
        saver.setup()
        yield saver


def investigate(api: QueryAPI, llm, checkpointer, alert: Optional[dict] = None, *, thread_id: Optional[str] = None,
                budget: int = graph.DEFAULT_BUDGET, f4: bool = False, pc_cap: int = graph.DEFAULT_CAP, extras=None,
                local_tools=None, crash: Optional[str] = None, crash_mode: str = "exit") -> RunResult:
    """Run, resume, or return a finished investigation, by thread id."""
    start = time.monotonic()
    final = {}
    for ev in stream(api, llm, checkpointer, alert, thread_id=thread_id, budget=budget, f4=f4, pc_cap=pc_cap,
                     extras=extras, local_tools=local_tools, crash=crash, crash_mode=crash_mode):
        if ev["type"] == "end":
            final = ev["state"]
    return graph.result_from(final, round(time.monotonic() - start, 2))


def decide(api: QueryAPI, llm, checkpointer, thread_id: str, approve: bool, by: str, remediation, **kw):
    """A human's decision on a proposed fix: resumes the paused investigation (execute and verify, or close)."""
    if not str(by or "").strip():
        raise ValueError("a decision needs who made it")
    return stream(api, llm, checkpointer, thread_id=thread_id, remediation=remediation,
                  resume={"approve": bool(approve), "by": by.strip()}, **kw)


def _summary(text: str, n: int = 160) -> str:
    t = " ".join((text or "").split())
    return t if len(t) <= n else t[:n] + "…"


def events_for(node: str, update: dict, state: dict) -> list[dict]:
    """Readable events for one finished step: what the dashboard shows while the investigation runs."""
    out = []
    if node == "page_check":
        pc = update.get("pc") or {}
        out.append({"type": "candidates", "paged": pc.get("paged"), "p_onset": pc.get("p_onset"), "calls": pc.get("calls"),
                    "candidates": [{k: c.get(k) for k in ("service", "relation", "depth", "own_change", "onset", "trend")}
                                   for c in pc.get("candidates") or []]})
    elif node == "hypothesize" and "hypotheses" in update:
        seen = {h["id"] for h in state.get("hypotheses") or []}
        new = [h for h in update["hypotheses"] if h["id"] not in seen]
        out.append({"type": "hypotheses", "round": update.get("round"),
                    "items": [{k: h[k] for k in ("id", "service", "fault_type", "prior", "why")} for h in new]})
    elif node == "test":
        if update.get("pending"):
            out.append({"type": "tool_calls", "hypothesis": state.get("current"),
                        "calls": [{"name": c["name"], "arguments": c["arguments"]} for c in update["pending"]]})
        if update.get("next") == "judge":
            h = next((x for x in update.get("hypotheses") or [] if x["id"] == state.get("current")), None)
            if h:
                out.append({"type": "verdict", "hypothesis": h["id"], "service": h["service"], "status": h["status"],
                            "confidence": h.get("confidence"), "evidence": h.get("evidence"), "summary": h.get("summary")})
    elif node == "tools":
        prev = len(state.get("tool_log") or [])
        out.append({"type": "tool_results", "results": [{"id": e["id"], "name": e["name"], "arguments": e["arguments"], "ok": e["ok"],
                                                         "summary": _summary(e["text"])} for e in update["tool_log"][prev:]]})
    elif node == "judge":
        out.append({"type": "judge", "verdict": (update.get("verdicts") or [""])[-1], "next": update.get("next")})
    elif node in ("report", "unsure", "error"):
        out.append({"type": "diagnosis", "diagnosis": update.get("diagnosis"), "valid": update.get("valid")})
    elif node == "propose":
        out.append({"type": "proposal", "proposal": update.get("proposal")})
    elif node == "approve":
        out.append({"type": "decision", "approval": update.get("approval")})
    elif node == "execute":
        out.append({"type": "action", "action": update.get("action")})
    elif node == "verify":
        out.append({"type": "recovery", "recovery": update.get("recovery")})
    return out


def stream(api: QueryAPI, llm, checkpointer, alert: Optional[dict] = None, *, thread_id: Optional[str] = None,
           budget: int = graph.DEFAULT_BUDGET, f4: bool = False, pc_cap: int = graph.DEFAULT_CAP, extras=None,
           local_tools=None, crash: Optional[str] = None, crash_mode: str = "exit", remediation=None,
           resume: Optional[dict] = None):
    """Run, resume, or replay a finished investigation, yielding events as steps finish. The last event is
    {"type": "end", "state": <state>}. A finished investigation yields only its diagnosis and end. With remediation,
    a run pauses at the approval: it yields "awaiting_approval" and ends; stream again with resume={"approve": bool,
    "by": "<who>"} (decide()) to continue."""
    tid = thread_id or investigation_id(alert, api.until)
    hook = crash_point(crash if crash is not None else os.environ.get(CRASH_ENV), crash_mode)
    g = graph.build_graph(api, traced(llm), budget, alert, f4=f4, pc_cap=pc_cap, extras=extras, checkpointer=checkpointer,
                          after_node=hook, local_tools=local_tools, remediation=remediation, thread_id=tid)
    config = {"configurable": {"thread_id": tid}, "recursion_limit": graph.RECURSION_LIMIT}
    yield {"type": "start", "thread_id": tid, "alert": alert, "until": api.until}
    snap = g.get_state(config)
    if snap.values and not snap.next:          # finished: as it was, no model call
        yield {"type": "diagnosis", "diagnosis": snap.values.get("diagnosis"), "valid": snap.values.get("valid"), "replayed": True}
        yield {"type": "end", "state": dict(snap.values)}
        return
    state: dict = dict(snap.values) if snap.values else {}
    waiting = [i for t in (snap.tasks or ()) for i in (t.interrupts or ())]
    if waiting and resume is None:              # paused at the approval: say so again, change nothing
        yield {"type": "awaiting_approval", **(waiting[0].value or {})}
        yield {"type": "end", "state": state}
        return
    if state:
        yield {"type": "resumed", "next": list(snap.next)}
    from langgraph.types import Command
    start_input = Command(resume=resume) if (waiting and resume is not None) else (None if state else {})
    for chunk in g.stream(start_input, config, stream_mode="updates", durability=DURABILITY):
        for node, update in chunk.items():
            if node == "__interrupt__":
                for intr in update:
                    yield {"type": "awaiting_approval", **(intr.value or {}), "node": node}
                continue
            if not isinstance(update, dict):
                continue
            for ev in events_for(node, update, state):
                yield {**ev, "node": node}
            state.update(update)
    yield {"type": "end", "state": state}
