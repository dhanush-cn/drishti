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
                budget: int = graph.DEFAULT_BUDGET, f4: bool = True, pc_cap: int = graph.DEFAULT_CAP, extras=None,
                crash: Optional[str] = None, crash_mode: str = "exit") -> RunResult:
    """Run, resume, or return a finished investigation, by thread id."""
    start = time.monotonic()
    tid = thread_id or investigation_id(alert, api.until)
    hook = crash_point(crash if crash is not None else os.environ.get(CRASH_ENV), crash_mode)
    g = graph.build_graph(api, llm, budget, alert, f4=f4, pc_cap=pc_cap, extras=extras, checkpointer=checkpointer, after_node=hook)
    config = {"configurable": {"thread_id": tid}, "recursion_limit": graph.RECURSION_LIMIT}
    snap = g.get_state(config)
    if snap.values and not snap.next:
        s = snap.values                      # finished: return it as it was
    elif snap.values:
        s = g.invoke(None, config, durability=DURABILITY)   # interrupted: continue from the last saved step
    else:
        s = g.invoke({}, config, durability=DURABILITY)     # new
    return graph.result_from(s, round(time.monotonic() - start, 2))
