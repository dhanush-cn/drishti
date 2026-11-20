"""Crash and resume (patch 61). No network, no model.

In-process tests use LangGraph's in-memory checkpointer and, when DRISHTI_TEST_POSTGRES_URL is set (the laptop's
drishti-postgres, or a local server), the real Postgres one. The process test kills a child with os._exit(3) at a
chosen step and resumes in a new process: Postgres only.

The model answers by content, not by reply order (ByContent), so a resumed process gets the same answers.
"""
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from langgraph.checkpoint.memory import InMemorySaver

from drishti_agent import durable, graph
from drishti_agent.llm import Reply, ToolCall
from drishti_agent.models import Usage

from test_graph import API
from test_pagecheck import ALERT, graph_series

PG = os.environ.get("DRISHTI_TEST_POSTGRES_URL", "")


class ByContent:
    """A deterministic model keyed on what it is asked: hypothesize -> product-catalog; test -> spans, then a verdict
    citing the result. Every call is appended to `log` (a file, so a parent can count a child's calls too)."""

    name = "by-content"

    def __init__(self, log: str):
        self.log = log

    def chat(self, system, messages, tools):
        names = [t["name"] for t in tools or []]
        with open(self.log, "a") as f:
            f.write(("hypothesize" if "submit_hypotheses" in names else "test") + "\n")
        if "submit_hypotheses" in names:
            return Reply("", [ToolCall("h", "submit_hypotheses", {"hypotheses": [
                {"service": "product-catalog", "fault_type": "error", "prior": 0.6, "why": "callee errors"}]})], Usage(input_tokens=100))
        results = [m for m in messages if m["role"] == "tool"]
        if not results:
            return Reply("", [ToolCall("c1", "spans", {"service": "product-catalog", "window": "2m"})], Usage(input_tokens=100))
        ids = re.findall(r"^\[(t\d+)\]", results[-1]["content"])
        return Reply("", [ToolCall("m", "mark_hypothesis", {"status": "supported", "confidence": 0.85, "evidence": ids,
                                                            "summary": "server errors"})], Usage(input_tokens=100))


def calls(log: str) -> int:
    if not os.path.exists(log):
        return 0
    with open(log) as f:
        return sum(1 for _ in f)


MODEL_NODES = {"hypothesize", "test"}


class Resume(unittest.TestCase):
    savers = [("memory", InMemorySaver)]

    def baseline(self):
        log = tempfile.mktemp()
        api = API(graph_series())
        r = durable.investigate(api, ByContent(log), InMemorySaver(), ALERT, thread_id="base")
        return r, calls(log), len(api.calls)

    def test_crash_after_each_node_resumes_without_redoing_earlier_steps(self):
        want, want_llm, want_api = self.baseline()
        self.assertEqual(want.diagnosis.root_cause_service, "product-catalog")
        for label, make in self.savers:
            for node in ("triage", "page_check", "hypothesize", "test", "tools", "judge"):
                with self.subTest(saver=label, node=node):
                    saver, log, api = make() if label == "memory" else make, tempfile.mktemp(), API(graph_series())
                    tid = f"t-{label}-{node}-{os.getpid()}"
                    with self.assertRaises(durable.Crash):
                        durable.investigate(api, ByContent(log), saver, ALERT, thread_id=tid, crash=f"{node}:1", crash_mode="raise")
                    before_api = len(api.calls)
                    r = durable.investigate(api, ByContent(log), saver, ALERT, thread_id=tid, crash="")
                    self.assertEqual(r.diagnosis.model_dump(), want.diagnosis.model_dump())
                    # Only the interrupted step ran twice: one more model call if it was a model turn, its own query
                    # calls again if it made any (triage: 2, page_check: its walk, tools: one tool call), nothing else.
                    self.assertEqual(calls(log), want_llm + (1 if node in MODEL_NODES else 0))
                    redo = {"triage": 2, "page_check": None, "tools": 1}.get(node, 0)
                    if redo is not None:
                        self.assertEqual(len(api.calls), want_api + redo, api.calls[before_api:])
                    else:
                        self.assertGreater(len(api.calls), want_api)

    def test_a_finished_investigation_is_returned_without_a_model_call(self):
        saver, log = InMemorySaver(), tempfile.mktemp()
        first = durable.investigate(API(graph_series()), ByContent(log), saver, ALERT, thread_id="done")
        n, api = calls(log), API(graph_series())
        again = durable.investigate(api, ByContent(log), saver, ALERT, thread_id="done")
        self.assertEqual((calls(log), len(api.calls)), (n, 0))
        self.assertEqual(again.diagnosis, first.diagnosis)

    def test_thread_id_is_the_alert_and_the_moment(self):
        a = durable.investigation_id(ALERT, "2026-11-03T17:11:00Z")
        self.assertEqual(a, durable.investigation_id(dict(ALERT), "2026-11-03T17:11:00Z"))
        self.assertNotEqual(a, durable.investigation_id(ALERT, "2026-11-03T17:12:00Z"))
        self.assertNotEqual(a, durable.investigation_id(None, "2026-11-03T17:11:00Z"))


@unittest.skipUnless(PG, "DRISHTI_TEST_POSTGRES_URL not set")
class ResumePostgres(Resume):
    """The same, on the real Postgres checkpointer."""

    @classmethod
    def setUpClass(cls):
        cls._cm = durable.postgres_saver(PG)
        cls.saver = cls._cm.__enter__()
        cls.savers = [("postgres", cls.saver)]

    @classmethod
    def tearDownClass(cls):
        cls._cm.__exit__(None, None, None)

    def test_a_killed_process_resumes_in_a_new_one(self):
        """os._exit(3) right after the second model turn computes; a new process finishes the investigation."""
        log, tid = tempfile.mktemp(), f"proc-{os.getpid()}"
        tests = str(Path(__file__).parent)
        child = (f"import sys; sys.path.insert(0, {tests!r})\n"
                 "from drishti_agent import durable\nfrom test_durable import ByContent, PG\n"
                 "from test_graph import API\nfrom test_pagecheck import ALERT, graph_series\n"
                 "with durable.postgres_saver(PG) as s:\n"
                 f"    r = durable.investigate(API(graph_series()), ByContent({log!r}), s, ALERT, thread_id={tid!r})\n"
                 "    print(r.diagnosis.root_cause_service)\n")
        env = {**os.environ, durable.CRASH_ENV: "test:1"}
        p = subprocess.run([sys.executable, "-c", child], env=env, capture_output=True, text=True, timeout=120)
        self.assertEqual(p.returncode, 3, p.stderr[-2000:])
        self.assertEqual(calls(log), 2, "hypothesize, then the crashed test turn")
        env.pop(durable.CRASH_ENV)
        p = subprocess.run([sys.executable, "-c", child], env=env, capture_output=True, text=True, timeout=120)
        self.assertEqual((p.returncode, p.stdout.strip()), (0, "product-catalog"), p.stderr[-2000:])
        _, want_llm, _ = self.baseline()
        self.assertEqual(calls(log), want_llm + 1, "only the interrupted model turn ran twice")
