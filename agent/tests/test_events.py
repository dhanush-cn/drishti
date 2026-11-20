"""Streaming events and LangSmith tracing (patch 62). No network: tracing goes to a local stand-in server."""
import gzip
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from langgraph.checkpoint.memory import InMemorySaver

from drishti_agent import durable, graph, tracing
from drishti_agent.llm import Scripted

from test_graph import API, call, hyps, mark
from test_pagecheck import ALERT, graph_series

FLOW = lambda: [hyps(("product-catalog", "error", 0.6)), call("c1", "spans", service="product-catalog"),
                mark("supported", 0.85, ["t1"])]


class Events(unittest.TestCase):
    def test_a_run_streams_what_happens_in_order(self):
        evs = list(durable.stream(API(graph_series()), Scripted(FLOW()), InMemorySaver(), ALERT, thread_id="e1"))
        self.assertEqual([e["type"] for e in evs],
                         ["start", "candidates", "hypotheses", "tool_calls", "tool_results", "verdict", "judge", "diagnosis", "end"])
        by = {e["type"]: e for e in evs}
        self.assertEqual(by["candidates"]["paged"], "frontend")
        self.assertEqual(by["hypotheses"]["items"][0]["service"], "product-catalog")
        self.assertEqual(by["tool_calls"]["calls"], [{"name": "spans", "arguments": {"service": "product-catalog"}}])
        self.assertEqual(by["tool_results"]["results"][0]["id"], "t1")
        self.assertLessEqual(len(by["tool_results"]["results"][0]["summary"]), 161)
        self.assertEqual((by["verdict"]["status"], by["verdict"]["evidence"]), ("supported", ["t1"]))
        self.assertIn("accepted", by["judge"]["verdict"])
        self.assertEqual(by["diagnosis"]["diagnosis"]["root_cause_service"], "product-catalog")

    def test_each_event_carries_only_what_is_new(self):
        flow = [hyps(("frontend", "error", 0.6)),                                   # round 1
                call("c1", "spans", service="frontend"), mark("refuted", 0.1, ["t1"]),
                hyps(("product-catalog", "error", 0.5)),                            # round 2
                call("c2", "spans", service="product-catalog"), mark("supported", 0.8, ["t2"])]
        evs = list(durable.stream(API(graph_series()), Scripted(flow), InMemorySaver(), ALERT, thread_id="e3"))
        results = [e for e in evs if e["type"] == "tool_results"]
        self.assertEqual([[r["id"] for r in e["results"]] for e in results], [["t1"], ["t2"]])
        self.assertEqual([[h["id"] for h in e["items"]] for e in evs if e["type"] == "hypotheses"], [["h1"], ["h2"]])
        self.assertEqual([e["service"] for e in evs if e["type"] == "verdict"], ["frontend", "product-catalog"])

    def test_a_resumed_run_says_so_and_a_finished_one_replays_its_answer(self):
        saver = InMemorySaver()
        with self.assertRaises(durable.Crash):
            list(durable.stream(API(graph_series()), Scripted(FLOW()), saver, ALERT, thread_id="e2", crash="tools:1", crash_mode="raise"))
        rest = Scripted(FLOW()[2:])
        evs = list(durable.stream(API(graph_series()), rest, saver, ALERT, thread_id="e2", crash=""))
        self.assertEqual(evs[1]["type"], "resumed")
        self.assertEqual(evs[-2]["diagnosis"]["root_cause_service"], "product-catalog")
        again = list(durable.stream(API(graph_series()), Scripted([]), saver, ALERT, thread_id="e2"))
        self.assertEqual([e["type"] for e in again], ["start", "diagnosis", "end"])
        self.assertTrue(again[1]["replayed"])


class Collector:
    """Stands in for the LangSmith API: records every request body."""

    def __init__(self):
        self.bodies = []
        outer = self

        class H(BaseHTTPRequestHandler):
            def _ok(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                if self.headers.get("Content-Encoding") == "gzip" or body[:2] == b"\x1f\x8b":
                    body = gzip.decompress(body)
                outer.bodies.append((self.command, self.path, body))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"{}")

            do_POST = do_PATCH = do_GET = _ok

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"


def clear_langsmith_caches():
    """LangSmith caches environment lookups (lru_cache). A process's environment doesn't change in production; in a
    test that switches tracing on and off it does, so the caches must go with the environment, or every later test
    is traced to a server that has stopped."""
    import functools
    from langsmith import utils as lsu
    for name in dir(lsu):
        f = getattr(lsu, name)
        if isinstance(f, functools._lru_cache_wrapper):
            f.cache_clear()
    from langsmith import run_trees
    if hasattr(run_trees, "_CLIENT"):
        run_trees._CLIENT = None


class Tracing(unittest.TestCase):
    def setUp(self):
        clear_langsmith_caches()

    def tearDown(self):
        clear_langsmith_caches()

    def test_off_by_default_the_model_is_untouched(self):
        with mock.patch.dict(os.environ, {"LANGSMITH_TRACING": "", "LANGSMITH_API_KEY": ""}):
            llm = Scripted([])
            self.assertIs(tracing.traced(llm), llm)
        with mock.patch.dict(os.environ, {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": ""}):
            self.assertFalse(tracing.enabled(), "no key: off")

    def test_on_each_model_call_is_sent_with_its_usage(self):
        c = Collector()
        env = {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": "test-key", "LANGSMITH_ENDPOINT": c.url,
               "LANGSMITH_PROJECT": "drishti-test"}
        with mock.patch.dict(os.environ, env):
            clear_langsmith_caches()
            r = graph.investigate(API(graph_series()), Scripted(FLOW()), alert=ALERT)
            tracing.flush()
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")
        sent = b"".join(b for _, _, b in c.bodies)
        # LangGraph traces its nodes by itself, and node outputs carry the same words; the model calls are the
        # runs of type "llm" named after the model, which only the wrapper creates (3 here: hypothesize, 2 test turns).
        import re as _re
        llm_runs = _re.findall(rb'"name":\s*"chat scripted",\s*"run_type":\s*"llm"', sent)
        self.assertEqual(len(llm_runs), 3, sent[:3000])
        self.assertIn(b'"run_type":"llm"', sent.replace(b" ", b""))
        self.assertIn(b"usage_metadata", sent)
        c.srv.shutdown()
