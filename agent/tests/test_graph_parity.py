"""Patch 56: the LangGraph port must behave exactly like the Week 5 loop.

tests/golden/v0_loop.json was written by the Week 5 loop (agent.py at a02ce8e)
with DRISHTI_WRITE_GOLDEN=1, before the port. Each case below drives
investigate() with a scripted model through one branch of the loop, and the
test compares everything observable with what v0 did:
  - the RunResult (diagnosis, validity, budget flag, tool records, LLM calls,
    tokens, error, transcript), seconds excepted;
  - every request the model was sent (system prompt, messages, tools offered), by hash;
  - every query API call, in order.
Any difference is a behaviour change, which patch 56 promises not to make.
"""
import hashlib
import json
import os
import unittest
from pathlib import Path

from langgraph.errors import GraphRecursionError

from drishti_agent import agent
from drishti_agent.llm import Heuristic, Reply, Scripted, ToolCall
from drishti_agent.models import Usage

from test_agent import FakeAPI, submit

GOLDEN = Path(__file__).parent / "golden" / "v0_loop.json"
ALERT = {"service": "checkout", "rule": "span_errors", "series": {"span.name": "PlaceOrder", "span.kind": "SERVER"},
         "opened_at": "2026-11-03T17:10:20Z"}


def tc(i, name="overview", **args):
    return ToolCall(f"c{i}", name, args)


def cases():
    """name -> (llm, budget, alert). Each case exercises one path through the loop."""
    u = Usage(input_tokens=500, output_tokens=40)
    return {
        "submit_at_once": (Scripted([submit()]), 5, None),
        "tools_then_submit": (Scripted([Reply("look", [tc(1, "spans", service="checkout", window="5m")], u), submit()]), 5, None),
        "paged_tools_then_submit": (Scripted([Reply("", [tc(1, "timeseries", service="payment", signal="errors")], u),
                                              submit(root_cause_service="payment")]), 5, ALERT),
        "batched_submit_refused": (Scripted([Reply("", [tc(1, "spans", service="payment"),
                                                       ToolCall("s0", "submit_diagnosis", {"root_cause_service": "x", "confidence": 0.9})]),
                                             submit()]), 5, None),
        "budget_overrun_in_one_turn": (Scripted([Reply("", [tc(i) for i in range(3)]), Reply("", [tc(i + 3) for i in range(3)]),
                                                 Reply("probably payment")]), 4, None),
        "budget_then_text_answer": (Scripted([Reply("", [tc(1)]),
                                              Reply('{"root_cause_service": "payment", "fault_type": "error", "confidence": 0.6}')]), 1, None),
        "budget_then_submit": (Scripted([Reply("", [tc(1), tc(2)]), submit(confidence=0.4)]), 2, None),
        "budget_then_silence": (Scripted([Reply("", [tc(1)]), Reply("no idea")]), 1, None),
        "invalid_submit": (Scripted([submit(confidence=5)]), 3, None),
        "text_answer_with_budget_left": (Scripted([Reply('answer: {"root_cause_service": "cart", "confidence": 0.3}')]), 5, None),
        "tool_error": (Scripted([Reply("", [tc(1, "spans", service="nope")]), submit()]), 3, None),
        "stall_two_nudges": (Scripted([Reply("hmm"), Reply("hmm"), Reply("hmm"), submit()]), 5, None),
        "nudge_then_recover": (Scripted([Reply("hmm"), Reply("", [tc(1)]), submit()]), 5, None),
        "model_error_mid_run": (Scripted([Reply("", [tc(1)], u)]), 5, None),
        "model_error_at_start": (Scripted([]), 5, ALERT),
        "turn_limit": (Scripted([Reply("", [tc(i)]) for i in range(agent.MAX_TURNS)]), 100, None),
        "turn_limit_with_nudges": (Scripted([Reply("x"), Reply("x")] + [Reply("", [tc(i)]) for i in range(agent.MAX_TURNS - 2)]), 100, None),
        "heuristic_paged": (Heuristic(), 5, ALERT),
        "heuristic_unpaged": (Heuristic(), 5, None),
    }


def observe(name, llm, budget, alert):
    api = FakeAPI()
    r = agent.investigate(api, llm, budget=budget, alert=alert)
    out = r.model_dump(mode="json", exclude={"seconds"})
    # Each request the model was sent, as a hash of its canonical JSON: any byte that differs changes it,
    # and the file stays small (every request repeats the whole conversation so far).
    out["sent"] = [hashlib.sha256(json.dumps(req, sort_keys=True).encode()).hexdigest()[:16]
                   for req in getattr(llm, "sent", [])]
    out["api_calls"] = api.calls
    return json.loads(json.dumps(out))


class Parity(unittest.TestCase):
    def test_every_path_matches_the_week5_loop(self):
        got = {name: observe(name, *args) for name, args in cases().items()}
        if os.environ.get("DRISHTI_WRITE_GOLDEN") == "1":
            GOLDEN.parent.mkdir(exist_ok=True)
            GOLDEN.write_text(json.dumps(got, indent=1, sort_keys=True) + "\n")
            self.skipTest("golden written")
        want = json.loads(GOLDEN.read_text())
        self.assertEqual(sorted(got), sorted(want), "case list changed")
        for name in want:
            with self.subTest(case=name):
                self.assertEqual(got[name], want[name])


class StepLimit(unittest.TestCase):
    def test_longest_legitimate_run_fits_and_a_runaway_loop_stops(self):
        # 16 model turns, each calling a tool: 2 * MAX_TURNS + 2 steps. It must finish under the limit...
        r = agent.investigate(FakeAPI(), Scripted([Reply("", [tc(i)]) for i in range(agent.MAX_TURNS)]), budget=100)
        self.assertIn("16 model turns", r.diagnosis.summary)
        # ...and a limit one step short of it must not, so the limit is what bounds a runaway graph.
        real = agent.RECURSION_LIMIT
        try:
            agent.RECURSION_LIMIT = 2 * agent.MAX_TURNS + 1
            with self.assertRaises(GraphRecursionError):
                agent.investigate(FakeAPI(), Scripted([Reply("", [tc(i)]) for i in range(agent.MAX_TURNS)]), budget=100)
        finally:
            agent.RECURSION_LIMIT = real
