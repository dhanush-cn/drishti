"""Eval harness tests: timeline parsing, freeze rules, report. No network."""
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

from drishti_agent.evals import freeze, report, run
from drishti_agent.evals.labels import LABELS
from drishti_agent.evals.scenarios import load_timeline
from drishti_agent.prompts import PROMPT_VERSION

TIMELINE = """{"phase":"warmup","start":"2026-11-03T16:23:04.70Z"}
{"phase":"baseline","start":"2026-11-03T16:33:04.784666462Z","end":"2026-11-03T16:53:04.707298543Z"}
{"phase":"scenario","n":1,"flag":"adFailure","variant":"on","on_minutes":4,"on":{"set_at":"2026-11-03T16:53:05.665Z"},"off":{"set_at":"2026-11-03T16:57:05.777Z"}}
{"phase":"scenario","n":12,"flag":"recommendationCacheFailure","variant":"on","on_minutes":10,"on":{"set_at":"2026-11-03T18:10:15.111Z"},"off":{"set_at":"2026-11-04T01:50:14.371Z"}}
"""


class Timeline(unittest.TestCase):
    def test_scenarios_baseline_and_slept_cut(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            f.write(TIMELINE)
        scs, (b0, b1) = load_timeline(f.name)
        self.assertEqual([s.flag for s in scs], ["adFailure", "recommendationCacheFailure"])
        self.assertEqual(b1 - b0, timedelta(minutes=19, seconds=59, microseconds=922632))
        self.assertFalse(scs[0].slept)
        self.assertEqual(scs[0].off.isoformat(), "2026-11-03T16:57:05.777000+00:00")
        # The laptop slept through scenario 12: judged on its first on_minutes only.
        self.assertTrue(scs[1].slept)
        self.assertEqual(scs[1].off - scs[1].on, timedelta(minutes=10))

    def test_every_demo_scenario_has_a_label_and_a_gate(self):
        for flag in ["adFailure", "cartFailure", "paymentFailure", "paymentUnreachable", "adHighCpu", "adManualGc",
                     "productCatalogLockContention", "imageSlowLoad", "productCatalogFailure", "intlShippingSlowdown",
                     "loadGeneratorFloodHomepage", "recommendationCacheFailure", "emailMemoryLeak"]:
            self.assertIn(flag, LABELS)
            self.assertTrue(LABELS[flag].gate, flag)


class Freeze(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.frozen = self.tmp / "FROZEN.json"
        p1 = mock.patch.object(run, "FROZEN", self.frozen)
        p2 = mock.patch.object(freeze, "FROZEN", self.frozen)
        p1.start(), p2.start()
        self.addCleanup(p1.stop), self.addCleanup(p2.stop)

    def hosted(self, provider="anthropic", version=PROMPT_VERSION):
        d = self.tmp / f"h-{provider}-{version}"
        d.mkdir()
        (d / "summary.json").write_text(json.dumps({"meta": {"provider": provider, "prompt_version": version, "model": "m"}}))
        return str(d)

    def test_held_out_refuses_without_or_against_a_freeze(self):
        args = ["--api", "x", "--timeline", "t", "--recording", "capture-b", "--provider", "heuristic", "--model", "-",
                "--out", str(self.tmp / "o"), "--held-out"]
        self.assertEqual(run.main(args), 2)  # no FROZEN.json
        self.frozen.write_text(json.dumps({"prompt_version": "something-else"}))
        self.assertEqual(run.main(args), 2)  # prompts changed after the freeze

    def test_freeze_needs_a_hosted_run_of_the_current_prompts(self):
        self.assertEqual(freeze.main(["--tuned-on", "c2", "--hosted-check", str(self.tmp / "missing")]), 2)
        self.assertEqual(freeze.main(["--tuned-on", "c2", "--hosted-check", self.hosted("ollama")]), 2)
        self.assertEqual(freeze.main(["--tuned-on", "c2", "--hosted-check", self.hosted(version="old")]), 2)
        self.assertEqual(freeze.main(["--tuned-on", "c2", "--hosted-check", self.hosted()]), 0)
        self.assertEqual(json.loads(self.frozen.read_text())["prompt_version"], PROMPT_VERSION)


class Report(unittest.TestCase):
    def test_counts_only_scored_scenarios_and_lists_the_rest(self):
        out = Path(tempfile.mkdtemp())
        (out / "scenarios.json").write_text(json.dumps([
            {"n": 1, "flag": "paymentFailure", "gate_passed": True, "gate_detail": "", "trigger_alert": None},
            {"n": 2, "flag": "adFailure", "gate_passed": False, "gate_detail": "errors 1 (need 3)", "trigger_alert": None}]))
        rows = []
        for n, flag, gate, top1 in [(1, "paymentFailure", True, True), (2, "adFailure", False, True)]:
            rows.append({"scenario": n, "flag": flag, "column": "agent-only", "system": "agent", "run": 0, "gate_passed": gate,
                         "missed": False, "score": {"top1": top1, "top3": True, "fault": True, "unsure": False, "confidently_wrong": False,
                                                    "named": "payment", "confidence": 0.9},
                         "result": {"tool_calls": [{}, {}], "usage": {"input_tokens": 10, "output_tokens": 2}, "seconds": 1.0, "valid_answer": True}})
        rows.append({"scenario": 1, "flag": "paymentFailure", "column": "end-to-end", "system": "agent", "run": 0, "gate_passed": True,
                     "missed": True, "score": {"top1": False, "top3": False, "fault": False, "unsure": True, "confidently_wrong": False,
                                               "named": None, "confidence": 0}})
        (out / "runs.jsonl").write_text("\n".join(json.dumps(r) for r in rows))
        s = report.write(out, {"recording": "t", "model": "m", "prompt_version": "p", "git_commit": "g", "runs": 1, "budget": 12,
                               "temperature": 0, "held_out": False, "started": "s", "price_in": 0, "price_out": 0})
        self.assertEqual(s["totals"]["agent/agent-only"]["runs"], 1, "the unscored scenario must not count")
        self.assertEqual(s["totals"]["agent/end-to-end"]["missed"], 1)
        md = (out / "RESULTS.md").read_text()
        self.assertIn("NOT SCORED: errors 1 (need 3)", md)
        self.assertIn("| agent/agent-only | 1/1 |", md)

    def test_skipped_rows_show_as_dash(self):
        out = Path(tempfile.mkdtemp())
        (out / "scenarios.json").write_text(json.dumps([{"n": 2, "flag": "adFailure", "gate_passed": False, "gate_detail": "x", "trigger_alert": None}]))
        (out / "runs.jsonl").write_text(json.dumps({"scenario": 2, "flag": "adFailure", "column": "agent-only", "system": "agent", "run": 0,
                                                    "gate_passed": False, "skipped": True, "missed": False,
                                                    "score": {"top1": False, "top3": False, "fault": False, "unsure": True,
                                                              "confidently_wrong": False, "named": None, "confidence": 0}}))
        s = report.write(out, {"recording": "t", "model": "m", "prompt_version": "p", "git_commit": "g", "runs": 1, "budget": 12,
                               "temperature": 0, "held_out": False, "started": "s", "price_in": 0, "price_out": 0})
        self.assertEqual(s["totals"], {})
        self.assertIn("| 2 | adFailure | NOT SCORED: x | no alert | – |", (out / "RESULTS.md").read_text())


if __name__ == "__main__":
    unittest.main()
