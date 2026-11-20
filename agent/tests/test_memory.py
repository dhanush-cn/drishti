"""Incident memory (patch 64). Postgres tests skip without DRISHTI_TEST_POSTGRES_URL."""
import os
import unittest
import uuid
from unittest import mock

from drishti_agent import graph, memory
from drishti_agent.llm import Scripted
from drishti_agent.pagecheck import page_check

from test_graph import API, hyps
from test_pagecheck import ALERT, graph_series

PG = os.environ.get("DRISHTI_TEST_POSTGRES_URL", "")


def pc():
    return page_check(API(graph_series()), ALERT, cap=20).to_json()


class Signature(unittest.TestCase):
    def test_signature_is_the_page_own_changes_and_victims(self):
        sig = memory.signature(pc(), ALERT)
        self.assertIn("page:frontend:log_template_new", sig)
        self.assertIn("own:product-catalog:error", sig)
        self.assertIn("own:email:memory", sig)
        self.assertIn("own:ad:cpu", sig)
        self.assertIn("victim:frontend", sig)
        self.assertEqual(sig, sorted(set(sig)), "deterministic: sorted, no duplicates")

    def test_span_rows_are_classified_by_what_moved(self):
        self.assertEqual(memory._kind("SERVER oteldemo.X/Get error ratio 0.400 (was 0.0)"), "error")
        self.assertEqual(memory._kind("SERVER GET /api p95 900 ms (was 20)"), "latency")
        self.assertEqual(memory._kind("server calls 90.0/min (was 10.0/min)"), "traffic")

    def test_jaccard(self):
        self.assertEqual(memory.jaccard(["a", "b"], ["a", "b"]), 1.0)
        self.assertEqual(memory.jaccard(["a", "b"], ["b", "c"]), 1 / 3)
        self.assertEqual(memory.jaccard([], []), 0.0)


@unittest.skipUnless(PG, "DRISHTI_TEST_POSTGRES_URL not set")
class Store(unittest.TestCase):
    def setUp(self):
        self.src = f"test-{uuid.uuid4().hex[:8]}"
        self.mem = memory.Memory(PG, source=self.src)
        self.mem.setup()
        self.sig = memory.signature(pc(), ALERT)

    def test_confirmed_and_wrong_are_retrieved_unconfirmed_never(self):
        self.mem.write(f"{self.src}:1", self.sig, "product-catalog", "error", "server errors", outcome="confirmed", root_cause="product-catalog")
        self.mem.write(f"{self.src}:2", self.sig, "frontend", "error", outcome="wrong", root_cause="product-catalog")
        self.mem.write(f"{self.src}:3", self.sig, "ad", "cpu")                       # unconfirmed
        hits = self.mem.similar(self.sig)
        self.assertEqual(sorted(h["id"] for h in hits), [f"{self.src}:1", f"{self.src}:2"])
        text = self.mem.extra(ALERT)({"pc": pc()})
        self.assertIn("root cause was product-catalog (error), confirmed", text)
        self.assertIn("frontend was blamed and it was WRONG; it was product-catalog", text)
        self.assertNotIn("ad", [h["named"] for h in hits])

    def test_dissimilar_incidents_are_not_retrieved(self):
        self.mem.write(f"{self.src}:x", ["page:cart:span_errors", "own:cart:error"], "cart", "error", outcome="confirmed", root_cause="cart")
        self.assertEqual(self.mem.similar(self.sig), [])

    def test_a_resolved_unconfirmed_incident_becomes_retrievable(self):
        self.mem.write(f"{self.src}:r", self.sig, "product-catalog", "error")
        self.assertEqual(self.mem.similar(self.sig), [])
        self.assertTrue(self.mem.resolve(f"{self.src}:r", "confirmed", "product-catalog", "recovery verified"))
        self.assertEqual([h["id"] for h in self.mem.similar(self.sig)], [f"{self.src}:r"])

    def test_sources_keep_eval_seeds_apart(self):
        self.mem.write(f"{self.src}:s", self.sig, "product-catalog", "error", outcome="confirmed", root_cause="product-catalog")
        other = memory.Memory(PG, source="live", read_from=[f"other-{self.src}"])
        self.assertEqual(other.similar(self.sig), [])

    def test_read_only_memory_never_writes_so_order_cannot_matter(self):
        ro = memory.Memory(PG, source=self.src, read_only=True)
        self.assertFalse(ro.write(f"{self.src}:ro", self.sig, "x", "error", outcome="confirmed"))
        self.assertFalse(ro.resolve(f"{self.src}:ro", "confirmed", "x", "me"))
        self.assertEqual(self.mem.similar(self.sig), [])
        # Two investigations in either order see the same memory.
        self.mem.write(f"{self.src}:o", self.sig, "product-catalog", "error", outcome="confirmed", root_cause="product-catalog")
        seen = []
        for order in (("a", "b"), ("b", "a")):
            for name in order:
                llm = Scripted([hyps()])
                graph.investigate(API(graph_series()), llm, alert=ALERT, extras=[ro.extra(ALERT)])
                seen.append((name, llm.sent[0][1][0]["content"]))
        self.assertEqual(len({c for _, c in seen}), 1)
        self.assertIn("## similar past incidents", seen[0][1])

    def test_seed_from_labels_writes_confirmed_postmortems(self):
        from drishti_agent.evals.scenarios import Scenario
        from test_pagecheck import UNTIL, at
        import test_pagecheck as tp
        sc = Scenario(n=9, flag="productCatalogFailure", variant="on", on=tp.pc.parse_ts(at(29)), off=UNTIL, slept=False)
        def set_triggers(base, s):
            s.trigger_alert = ALERT
            s.triggers = {"agent-only": UNTIL, "end-to-end": tp.pc.parse_ts(ALERT["opened_at"])}
        def gate(base, s, b):
            s.gate_passed = True
        with mock.patch("drishti_agent.evals.scenarios.load_timeline", return_value=([sc], (None, None))), \
             mock.patch("drishti_agent.evals.scenarios.set_triggers", set_triggers), \
             mock.patch("drishti_agent.evals.scenarios.gate", gate), \
             mock.patch("drishti_agent.api.QueryAPI", lambda base, until: API(graph_series())):
            n = memory.seed("http://x", "t", self.src, self.mem, 20)
        self.assertEqual(n, 2, "one per trigger column")
        hits = self.mem.similar(self.sig)
        self.assertEqual(hits[0]["outcome"], "confirmed")
        self.assertEqual(hits[0]["root_cause"], "product-catalog")


class Runner(unittest.TestCase):
    def test_memory_from_the_scored_recording_is_refused(self):
        import tempfile
        from drishti_agent.evals import run as runner
        with tempfile.TemporaryDirectory() as d, mock.patch("drishti_agent.evals.run.load_timeline", return_value=([], (None, None))):
            code = runner.main(["--api", "http://x", "--timeline", "t", "--recording", "capture-2", "--provider", "ollama",
                                "--model", "m", "--systems", "graph", "--memory-postgres", "postgresql://x", "--memory-from",
                                "capture-2", "--out", d])
        self.assertEqual(code, 2)
