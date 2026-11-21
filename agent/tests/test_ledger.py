"""The request ledger and its daily cap (patch 74). No network, no model."""
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone

from drishti_agent.ledger import CapReached, Ledger, Metered, day_of, totals
from drishti_agent.llm import LLMError, Reply
from drishti_agent.models import Usage

NOON = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)


class Fixed:
    """A model client that answers with fixed token counts, or fails."""
    name = "fixed"

    def __init__(self, fail=False):
        self.fail, self.calls = fail, 0

    def chat(self, system, messages, tools=None):
        self.calls += 1
        if self.fail:
            raise LLMError("HTTP 500: boom")
        return Reply("ok", usage=Usage(input_tokens=10_000, output_tokens=1_000))


def ledger(path, clock=lambda: NOON, **kw):
    return Ledger(path, "gemini", price_in=0.30, price_out=2.50, clock=clock, **kw)


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), "ledger.jsonl")

    def rows(self):
        return [json.loads(l) for l in open(self.path)]

    def test_every_request_is_recorded_with_tokens_and_usd(self):
        m = Metered(Fixed(), ledger(self.path))
        m.cell = "graph/adHighCpu/end-to-end/0"
        m.chat("s", [{"role": "user", "content": "x"}])
        r = self.rows()
        self.assertEqual(len(r), 1)
        self.assertEqual((r[0]["cell"], r[0]["status"], r[0]["input_tokens"], r[0]["output_tokens"]),
                         ("graph/adHighCpu/end-to-end/0", "ok", 10_000, 1_000))
        self.assertAlmostEqual(r[0]["usd"], 10_000 / 1e6 * 0.30 + 1_000 / 1e6 * 2.50)
        self.assertEqual(m.name, "fixed", "attributes pass through")

    def test_a_failed_request_is_recorded_and_counted(self):
        m = Metered(Fixed(fail=True), ledger(self.path))
        with self.assertRaises(LLMError):
            m.chat("s", [])
        self.assertEqual([r["status"] for r in self.rows()], ["error"])
        self.assertEqual(totals(self.path)["gemini"]["requests"], 1)

    def test_request_cap_refuses_before_calling_the_model(self):
        inner = Fixed()
        m = Metered(inner, ledger(self.path, max_requests=2))
        m.chat("s", [])
        m.chat("s", [])
        with self.assertRaises(CapReached) as e:
            m.chat("s", [])
        self.assertEqual(inner.calls, 2, "the third request never reached the model")
        self.assertIn("quota", str(e.exception))
        self.assertEqual(len(self.rows()), 2, "a refused request is not a request")

    def test_usd_is_recorded_but_never_capped(self):
        # No billing account: nothing to spend. USD is what it would cost on a paid tier, for the cost column.
        m = Metered(Fixed(), ledger(self.path, max_requests=100))
        for _ in range(50):
            m.chat("s", [])
        self.assertAlmostEqual(totals(self.path)["gemini"]["usd"], 50 * 0.0055)

    def test_the_day_starts_at_0800_utc(self):
        self.assertEqual(str(day_of(datetime(2026, 11, 25, 7, 59, tzinfo=timezone.utc))), "2026-11-24")
        self.assertEqual(str(day_of(datetime(2026, 11, 25, 8, 0, tzinfo=timezone.utc))), "2026-11-25")
        t = [datetime(2026, 11, 25, 7, 30, tzinfo=timezone.utc)]
        m = Metered(Fixed(), ledger(self.path, clock=lambda: t[0], max_requests=1))
        m.chat("s", [])
        with self.assertRaises(CapReached):
            m.chat("s", [])
        t[0] = datetime(2026, 11, 25, 8, 30, tzinfo=timezone.utc)   # a new ledger day
        m.chat("s", [])

    def test_caps_are_per_model_and_the_file_is_shared(self):
        a = Metered(Fixed(), ledger(self.path, max_requests=1))
        a.chat("s", [])
        other = Metered(Fixed(), Ledger(self.path, "qwen", clock=lambda: NOON, max_requests=1))
        other.chat("s", [])                      # another model: its own budget
        b = Metered(Fixed(), ledger(self.path, max_requests=1))
        with self.assertRaises(CapReached):      # a second process on the same file sees the first one's request
            b.chat("s", [])


class RunnerStops(unittest.TestCase):
    """The runner with a ledger: a cap stops it like a rate limit, and --resume continues (the real runner, over HTTP)."""

    def test_cap_stops_the_run_and_resume_finishes(self):
        from test_harness import Harness
        from test_durable import ByContent
        h = Harness("run_")
        h.setUp()
        try:
            led = os.path.join(h.d, "ledger.jsonl")
            log = tempfile.mktemp()
            # One investigation of the fake call graph takes 3 model calls: a cap of 4 lets the first cell finish
            # and stops the second one partway (an error row, retried on resume).
            code, rows, _ = h.run_(ByContent(log), "--ledger", led, "--max-requests-per-day", "4")
            self.assertEqual(code, 3, "stopped: rerun with --resume")
            self.assertTrue(rows[0]["score"]["top1"])
            self.assertIn("ledger cap", rows[-1]["result"]["error"])
            cells = [json.loads(l)["cell"] for l in open(led)]
            self.assertEqual(len(cells), 4)
            self.assertEqual(cells[0], "graph/productCatalogFailure/end-to-end/0")
            code, rows, _ = h.run_(ByContent(log), "--ledger", led, "--max-requests-per-day", "100", "--resume")
            self.assertEqual(code, 0)
            self.assertEqual(sum(1 for r in rows if r["run"] == 1 and r["score"]["top1"]), 1)
        finally:
            h.tearDown()


class NotFromTheEnvironment(unittest.TestCase):
    def test_an_exported_variable_does_not_make_the_runner_write_a_ledger(self):
        from unittest import mock
        from test_harness import Harness
        from test_durable import ByContent
        h = Harness("run_")
        h.setUp()
        try:
            led = os.path.join(h.d, "env-ledger.jsonl")
            with mock.patch.dict(os.environ, {"DRISHTI_LEDGER": led}):
                code, _, out = h.run_(ByContent(tempfile.mktemp()))
            self.assertEqual(code, 0)
            self.assertFalse(os.path.exists(led), "only --ledger turns the ledger on")
            self.assertEqual(json.loads(open(os.path.join(out, "summary.json")).read())["meta"]["ledger"], "")
        finally:
            h.tearDown()


class RunnerChecksBeforeACell(unittest.TestCase):
    def test_a_cap_reached_between_cells_writes_no_failed_row(self):
        from test_harness import Harness
        from test_durable import ByContent
        h = Harness("run_")
        h.setUp()
        try:
            led = os.path.join(h.d, "ledger.jsonl")
            code, rows, _ = h.run_(ByContent(tempfile.mktemp()), "--ledger", led, "--max-requests-per-day", "3")
            self.assertEqual(code, 3)
            self.assertEqual(len(rows), 1, "the second cell was not started: no error row to retry")
            self.assertTrue(rows[0]["score"]["top1"])
        finally:
            h.tearDown()


if __name__ == "__main__":
    unittest.main()
