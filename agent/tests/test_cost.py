"""Cost and time per investigation (patch 79, Week 7 design G8). No network, no model."""
import json
import tempfile
import unittest
from pathlib import Path

from drishti_agent.evals import report
from drishti_agent.evals.run import PRICES, price_for


def row(llm_calls, tools, tin, tout, secs, graph=None):
    t = [{"role": "user", "content": "x"}]
    if graph is not None:
        t.append({"role": "graph", "content": json.dumps(graph)})
    return {"result": {"llm_calls": llm_calls, "tool_calls": [{"name": "spans"}] * tools, "seconds": secs,
                       "usage": {"input_tokens": tin, "output_tokens": tout}, "transcript": t}}


class CostTime(unittest.TestCase):
    def test_graph_counts_both_page_checks_and_the_wait(self):
        g = {"pagecheck": {"calls": 5}, "recheck": {"minutes": 2.0, "pagecheck_calls_first": 3}}
        c = report.cost_time([row(7, 4, 10_000, 1_000, 30.0, g)], 0.30, 2.50)
        self.assertEqual((c["requests_median"], c["queries_median"], c["rechecks"]), (7, 12, 1))
        self.assertEqual(c["wall_s_median"], 30.0)
        self.assertEqual(c["time_to_diagnosis_s_median"], 150.0, "wall time + the 2-minute wait")
        self.assertAlmostEqual(c["usd_median"], 0.0055)

    def test_without_a_second_look_or_a_graph(self):
        c = report.cost_time([row(3, 2, 100, 10, 9.0, {"pagecheck": {"calls": 4}}), row(11, 9, 100, 10, 50.0)], 0, 0)
        self.assertEqual(c["rechecks"], 0)
        self.assertEqual(sorted([c["queries_median"]]), [7.5], "graph 2 + 4 page_check = 6, v0 9 tool calls: median 7.5")
        self.assertEqual(c["time_to_diagnosis_s_median"], c["wall_s_median"])
        self.assertEqual(c["usd_total"], 0)

    def test_p90_is_nearest_rank(self):
        self.assertEqual(report.p90(list(range(1, 11))), 9)
        self.assertEqual(report.p90([5]), 5)
        self.assertIsNone(report.p90([]))


class Prices(unittest.TestCase):
    def test_the_price_file_prices_the_eval_model_by_name(self):
        pin, pout, src = price_for("openai-compat:gemini-3.5-flash-lite")
        self.assertEqual((pin, pout), (0.30, 2.50))
        self.assertIn("ai.google.dev", src)
        self.assertEqual(price_for("openai-compat:qwen2.5:7b")[:2], (0.0, 0.0))
        self.assertTrue(PRICES.exists())

    def test_a_missing_file_prices_nothing(self):
        self.assertEqual(price_for("gemini-3.5-flash-lite", Path(tempfile.mktemp())), (0.0, 0.0, ""))


class InTheReport(unittest.TestCase):
    def test_results_and_summary_carry_cost_and_time(self):
        from test_harness import Harness
        from test_durable import ByContent
        h = Harness("run_")
        h.setUp()
        try:
            _, _, out = h.run_(ByContent(tempfile.mktemp()), "--price-in", "1", "--price-out", "10")
            s = json.loads((Path(out) / "summary.json").read_text())
            c = s["totals"]["graph/end-to-end"]["cost_time"]
            self.assertEqual((c["n"], c["requests_median"], c["rechecks"]), (2, 3, 0))
            self.assertEqual(c["usd_median"], 0.0003, "300 tokens in at $1 per million")
            self.assertEqual(s["meta"]["price_source"], "command line")
            md = (Path(out) / "RESULTS.md").read_text()
            self.assertIn("## Cost and time per investigation", md)
            self.assertIn("| graph/end-to-end | 3.0 / 3 | 7.0 / 7 |", md)
        finally:
            h.tearDown()

    def test_the_runner_prices_a_known_model_from_the_file(self):
        from test_harness import Harness
        from test_durable import ByContent

        class Gemini(ByContent):
            name = "openai-compat:gemini-3.5-flash-lite"
        h = Harness("run_")
        h.setUp()
        try:
            _, _, out = h.run_(Gemini(tempfile.mktemp()))
            meta = json.loads((Path(out) / "summary.json").read_text())["meta"]
            self.assertEqual((meta["price_in"], meta["price_out"]), (0.30, 2.50))
            self.assertIn("ai.google.dev", meta["price_source"])
        finally:
            h.tearDown()


if __name__ == "__main__":
    unittest.main()
