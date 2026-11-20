"""Runbooks, their retrieval, and their place in the graph (patch 63). Postgres tests skip without DRISHTI_TEST_POSTGRES_URL."""
import os
import re
import unittest

from drishti_agent import graph, runbook_tools as rb
from drishti_agent.llm import Scripted

from test_graph import API, call, hyps, mark
from test_pagecheck import ALERT, graph_series

PG = os.environ.get("DRISHTI_TEST_POSTGRES_URL", "")
FLAGS = ["adFailure", "adHighCpu", "adManualGc", "cartFailure", "emailMemoryLeak", "imageSlowLoad", "intlShippingSlowdown",
         "loadGeneratorFloodHomepage", "paymentFailure", "paymentUnreachable", "productCatalogFailure",
         "productCatalogLockContention", "recommendationCacheFailure"]


def all_text() -> str:
    parts = [rb.render(n, s) for n, s in rb.services().items()]
    parts += [(rb.DIR / f).read_text() for f in ("symptoms.md", "services.json", "queries.json")]
    return "\n".join(parts)


class Content(unittest.TestCase):
    def test_rendered_files_match_the_template(self):
        for name, s in rb.services().items():
            self.assertEqual((rb.DIR / "rendered" / f"{name}.md").read_text(), rb.render(name, s),
                             f"{name}: rerun python -m drishti_agent.runbook_tools render")

    def test_every_service_gets_the_same_checks(self):
        """Uniform by construction: the same sections, and with placeholder relations every service renders to one of
        two texts (it calls other services, or it doesn't). Only the facts in services.json differ."""
        svcs = rb.services()
        heads = {n: re.findall(r"(?m)^## (.+)$", rb.render(n, s)) for n, s in svcs.items()}
        self.assertTrue(all(h == list(rb.SECTIONS) for h in heads.values()), heads)
        texts = {re.sub(rf"(?<![\w-]){re.escape(n)}(?![\w-])", "N",
                        rb.render(n, {"role": "R", "calls": ["D"] if s["calls"] else [], "called_by": ["C"]}))
                 for n, s in svcs.items()}
        self.assertEqual(len(texts), 2, "a service got text of its own")

    def test_service_facts_carry_no_fault_vocabulary(self):
        """The only free text per service is its role and relations: no hint of how it could fail."""
        words = ["leak", "grow", "slow", "fail", "error", "cpu", "memory", "lock", "contention", "gc", "garbage", "timeout",
                 "unreachable", "flood", "surge", "outage", "bug", "crash", "cache"]
        for n, s in rb.services().items():
            facts = " ".join([s["role"]] + s["calls"] + s["called_by"]).lower()
            for w in words:
                self.assertIsNone(re.search(rf"\b{w}", facts), f"{n}: {w!r} in {facts!r}")

    def test_no_word_of_the_fault_injection(self):
        text = all_text().lower()
        for w in ["flag", "flagd", "problempattern", "locust", "load-generator", "load generator", "injected"] + [f.lower() for f in FLAGS]:
            self.assertNotIn(w, text, w)

    def test_lookup(self):
        ok, text = rb.local_tools(None)["runbook"][1]({"service": "Payment"})
        self.assertTrue(ok)
        self.assertIn("called by checkout", text)
        ok, text = rb.local_tools(None)["runbook"][1]({"service": "nope"})
        self.assertFalse(ok)
        self.assertIn("known:", text)

    def test_relations_for_hypothesize(self):
        extra = rb.relations_extra({"pc": {"candidates": [{"service": "product-catalog", "own_change": ["x"]},
                                                          {"service": "frontend", "own_change": []}]}})
        self.assertIn("product-catalog: lists, searches", extra)
        self.assertNotIn("frontend:", extra)


class InTheGraph(unittest.TestCase):
    def test_runbook_calls_count_against_the_budget_and_are_never_evidence(self):
        tools = rb.local_tools(None)
        llm = Scripted([hyps(("product-catalog", "error", 0.6)),
                        call("c1", "runbook", service="product-catalog"),
                        mark("supported", 0.9, ["t1"]),                       # cites the runbook: not evidence
                        hyps()])
        r = graph.investigate(API(graph_series()), llm, alert=ALERT, local_tools=tools)
        self.assertEqual([(t.name, t.ok) for t in r.tool_calls], [("runbook", True)])
        self.assertTrue(any("called by checkout, frontend, recommendation" in (m.get("content") or "")
                            for m in r.transcript if m.get("role") == "tool"), "the runbook text came back")
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertIn("runbook", [t["name"] for t in llm.sent[1][2]], "offered next to the query tools")

    def test_relations_reach_hypothesize(self):
        llm = Scripted([hyps()])
        graph.investigate(API(graph_series()), llm, alert=ALERT, extras=[rb.relations_extra])
        self.assertIn("## what the changed candidates do (runbooks)", llm.sent[0][1][0]["content"])


@unittest.skipUnless(PG, "DRISHTI_TEST_POSTGRES_URL not set")
class Search(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.idx = rb.PostgresIndex(PG)
        cls.n = cls.idx.build()

    def test_indexes_symptoms_and_relations_only(self):
        self.assertEqual(self.n, len([c for c in rb.chunks() if not c.service or c.id.endswith("#relations")]))

    def test_search_tool(self):
        ok, text = rb.local_tools(self.idx)["search_runbooks"][1]({"query": "deadline exceeded on client spans"})
        self.assertTrue(ok)
        self.assertTrue(text.startswith("[symptoms#timeouts-and-deadline-exceeded]"), text[:200])

    def test_recall_at_3_on_the_fixed_query_set(self):
        hit, total, misses = rb.recall(self.idx)
        self.assertEqual((hit, total), (30, 30), misses)

    def test_rebuild_is_idempotent(self):
        self.assertEqual(self.idx.build(), self.n)
        self.assertEqual(self.idx.build(), self.n)
