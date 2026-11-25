"""scripts/replay_export.py (Week 8 kit 1, design H2/H3): runs.jsonl -> the replay page's runs.json.

Rows are built from real scripted graph runs (the same fakes as test_graph / test_recheck), so the transcript and
graph record have the shapes the harness writes. No network, no model.
"""
import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from drishti_agent.evals import report
from drishti_agent.evals.score import score

from test_graph import call, hyps, mark, run
from test_recheck import UNSURE_AT_ONCE, investigate

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "replay_export.py"
spec = importlib.util.spec_from_file_location("replay_export", SCRIPT)
rx = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rx)


def row(r, flag, scenario=1, column="end-to-end", system="graph", k=0, **extra):
    return {"scenario": scenario, "flag": flag, "column": column, "system": system, "run": k, "gate_passed": True,
            "until": "x", "missed": False, "score": score(flag, r).to_json(), "usd": 0.0,
            "result": r.model_dump(mode="json"), **extra}


def simple_run():
    r, _, _ = run([hyps(("product-catalog", "error", 0.6), ("frontend", "error", 0.3)),
                   call("c1", "spans", service="product-catalog", window="2m"),
                   mark("supported", 0.85, ["t1"])])
    return r


def recheck_run():
    r, _, _, _ = investigate(UNSURE_AT_ONCE + [hyps(("cart", "error", 0.7)), call("c1", "spans", service="cart"),
                                               mark("supported", 0.8, ["t1"])])
    return r


def fake(scenario, k, cls, column="end-to-end", system="graph", **extra):
    """A minimal scored row of a given class (for the selection rule only)."""
    s = {"top1": cls in ("right_first", "right_second"), "top3": False, "unsure": cls == "unsure",
         "confidently_wrong": cls == "conf_wrong", "fault": False, "named": None, "confidence": 0.9}
    rec = {"recheck": {"minutes": 2} if cls == "right_second" else None}
    return {"scenario": scenario, "flag": "adFailure", "column": column, "system": system, "run": k,
            "gate_passed": True, "missed": False, "score": s,
            "result": {"transcript": [{"role": "graph", "content": json.dumps(rec)}], "valid_answer": True}, **extra}


class Events(unittest.TestCase):
    def test_a_first_look_run_reads_as_the_live_stream(self):
        ev = rx.events_from_row(row(simple_run(), "productCatalogFailure"))
        self.assertEqual([e["type"] for e in ev],
                         ["candidates", "hypotheses", "tool_calls", "tool_results", "verdict", "diagnosis", "proposal"])
        c = ev[0]
        self.assertEqual((c["look"], c["paged"]), (1, "frontend"))
        self.assertEqual(set(c["candidates"][0]), {"service", "relation", "depth", "own_change", "onset", "trend"},
                         "the same keys as durable.events_for")
        self.assertEqual([(h["id"], h["service"], h["prior"]) for h in ev[1]["items"]],
                         [("h1", "product-catalog", 0.6), ("h2", "frontend", 0.3)])
        self.assertEqual(ev[2], {"type": "tool_calls", "hypothesis": "h1",
                                 "calls": [{"name": "spans", "arguments": {"service": "product-catalog", "window": "2m"}}]})
        t = ev[3]["results"][0]
        self.assertEqual((t["id"], t["name"], t["ok"]), ("t1", "spans", True))
        self.assertLessEqual(len(t["summary"]), 161, "tool output cut to a summary")
        self.assertFalse(t["summary"].startswith("[t1]"))
        v = ev[4]
        self.assertEqual((v["hypothesis"], v["status"], v["confidence"]), ("h1", "supported", 0.85))
        self.assertTrue(v["judge"].startswith("h1 accepted"))
        self.assertEqual(ev[5]["diagnosis"]["root_cause_service"], "product-catalog")
        self.assertEqual((ev[6]["proposal"]["action"], ev[6]["proposal"]["service"], ev[6]["executed"]),
                         ("rollback_config", "product-catalog", False))

    def test_a_second_look_run_has_no_first_look_table_and_the_table_after_the_recheck(self):
        ev = rx.events_from_row(row(recheck_run(), "cartFailure"))
        types = [e["type"] for e in ev]
        i = types.index("recheck")
        self.assertNotIn("candidates", types[:i], "the row doesn't keep the first look's table: none is invented")
        self.assertEqual(ev[i + 1]["type"], "candidates")
        self.assertEqual(ev[i + 1]["look"], 2)
        self.assertEqual(ev[i]["changed"], ["cart"])
        self.assertTrue(ev[i]["judge"].startswith("first look unsure"))
        self.assertEqual([e["round"] for e in ev if e["type"] == "hypotheses"], [1, 2])
        self.assertEqual(ev[-2]["diagnosis"]["root_cause_service"], "cart")

    def test_every_verdict_string_is_shown_once(self):
        for r in (simple_run(), recheck_run()):
            rec = json.loads(r.transcript[-1]["content"])
            ev = rx.events_from_row(row(r, "cartFailure"))
            shown = [e.get("judge") or e.get("verdict") for e in ev if e["type"] in ("verdict", "judge", "recheck")]
            self.assertEqual(sorted(x for x in shown if x), sorted(rec["verdicts"]))

    def test_ok_is_unknown_when_the_tool_log_and_transcript_disagree(self):
        rw = row(simple_run(), "productCatalogFailure")
        rw["result"]["tool_calls"] = []
        t = next(e for e in rx.events_from_row(rw) if e["type"] == "tool_results")
        self.assertIsNone(t["results"][0]["ok"], "not guessed")

    def test_unsure_proposes_nothing(self):
        r, _, _ = run([hyps()] * 3)
        ev = rx.events_from_row(row(r, "adFailure"))
        self.assertIsNone(ev[-2]["diagnosis"]["root_cause_service"])
        self.assertIsNone(ev[-1]["proposal"])


class Selection(unittest.TestCase):
    def test_the_h2_rule(self):
        rows = [fake(5, 0, "right_first"), fake(3, 0, "right_first"), fake(2, 1, "right_first"),
                fake(9, 1, "right_second"), fake(4, 2, "right_second"),          # no run 0: run 1 before run 2
                fake(1, 0, "conf_wrong", system="agent"),                       # not the graph
                fake(7, 0, "conf_wrong"),
                fake(6, 0, "unsure"), fake(1, 0, "unsure", missed=True),         # a miss isn't scored
                fake(8, 0, "right_first", column="agent-only"), fake(2, 0, "right_first", column="agent-only")]
        got = [(c, r["column"], r["scenario"], r["run"]) for c, r in rx.pick(rows)]
        self.assertEqual(got, [("right_first", "end-to-end", 3, 0), ("right_second", "end-to-end", 9, 1),
                               ("conf_wrong", "end-to-end", 7, 0), ("unsure", "end-to-end", 6, 0),
                               ("right_first", "agent-only", 2, 0)])

    def test_an_errored_row_is_not_scored_and_the_latest_row_wins(self):
        bad = fake(1, 0, "right_first")
        bad["result"]["error"] = "HTTP 503"
        self.assertEqual(rx.pick([bad]), [])
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "runs.jsonl"
            p.write_text(json.dumps(bad) + "\n" + json.dumps(fake(1, 0, "unsure")) + "\n")
            rows = rx.read_rows(p)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rx.pick(rows)[0][0], "unsure")

    def test_a_labelled_graph_system_counts(self):
        self.assertEqual(len(rx.pick([fake(1, 0, "unsure", system="graph:011b9a0338f5")])), 1)


class Headline(unittest.TestCase):
    def test_counts_match_report_write(self):
        rows = [row(simple_run(), "productCatalogFailure", 1, k=0), row(recheck_run(), "productCatalogFailure", 1, k=1),
                row(simple_run(), "cartFailure", 2, column="agent-only"),
                {**row(simple_run(), "adFailure", 3), "missed": True, "result": None},
                {**row(simple_run(), "adFailure", 4), "gate_passed": False}]
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            (out / "runs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
            (out / "scenarios.json").write_text(json.dumps([{"n": i, "flag": f, "gate_passed": True}
                                                          for i, f in enumerate(["productCatalogFailure", "cartFailure", "adFailure", "adFailure"], 1)]))
            totals = report.write(out, {"recording": "t", "model": "m", "prompt_version": "p", "git_commit": "c", "runs": 1,
                                        "budget": 12, "temperature": 0, "held_out": False, "started": "", "price_in": 0.0,
                                        "price_out": 0.0})["totals"]
        h = rx.headline([("graph", rows)])[0]
        for col, key in (("end-to-end", "graph/end-to-end"), ("agent-only", "graph/agent-only")):
            mine, theirs = h[col.replace("-", "_")], totals[key]
            self.assertEqual((mine["n"], mine["top1"], mine["top3"], mine["unsure"], mine["confidently_wrong"], mine["invalid"]),
                             (theirs["runs"], theirs["top1"], theirs["top3"], theirs["unsure"], theirs["confidently_wrong"],
                              theirs["invalid"]), col)

    def test_a_fixed_confidence_is_no_confidence(self):
        def answer(scenario, named, conf):
            r = fake(scenario, 0, "right_first")
            r["score"].update(named=named, confidence=conf)
            return r
        floor = [answer(1, "ad", 0.5), answer(2, "cart", 0.5), answer(3, None, 0.0)]     # the floor's 0.5, and unsure
        self.assertFalse(rx.tally(floor, "end-to-end")["gives_confidence"])
        self.assertTrue(rx.tally([answer(1, "ad", 0.5), answer(2, "cart", 0.85)], "end-to-end")["gives_confidence"])


class Secrets(unittest.TestCase):
    def _main(self, rows):
        with tempfile.TemporaryDirectory() as d:
            src, out = Path(d) / "runs.jsonl", Path(d) / "site" / "runs.json"
            src.write_text("".join(json.dumps(r) + "\n" for r in rows))
            o, e = io.StringIO(), io.StringIO()
            with redirect_stdout(o), redirect_stderr(e):
                code = rx.main(["--replay-from", str(src), "--headline", f"graph={src}", "--out", str(out)])
            return code, o.getvalue(), out.exists()

    def test_clean_output_is_written_and_says_zero(self):
        code, printed, written = self._main([row(simple_run(), "productCatalogFailure")])
        self.assertEqual((code, written), (0, True))
        self.assertIn("secrets: 0", printed)

    def test_a_key_in_a_tool_result_stops_the_write(self):
        for leak in ("AIza" + "Sy" + "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6", "GEMINI_API_KEY=whatever", "sk-" + "abcdefghijklmnop1234"):
            rw = row(simple_run(), "productCatalogFailure")
            for m in rw["result"]["transcript"]:
                if m["role"] == "tool":
                    m["content"] = "[t1] " + leak
            code, printed, written = self._main([rw])
            self.assertEqual((code, written), (2, False), leak[:8])
            self.assertIn("secrets: 1", printed)

    def test_ordinary_text_is_not_a_secret(self):
        self.assertEqual(rx.secret_hits({"a": ["skip-list", "api key rotation", "task-runner", "AIza"]}), 0)


class Page(unittest.TestCase):
    """site/index.html shows model text, so it must never parse a string as HTML (design H3)."""
    PAGE = Path(__file__).resolve().parents[2] / "site" / "index.html"

    def test_no_html_insertion(self):
        js = self.PAGE.read_text()
        for api in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "createContextualFragment",
                    "DOMParser", "srcdoc", "eval(", "new Function"):
            self.assertNotIn(api, js, api)

    def test_no_third_party_requests(self):
        import re
        urls = re.findall(r"""(?:src|href)=["'](https?://[^"']+)""", self.PAGE.read_text())
        self.assertTrue(all(u.startswith("https://github.com/dhanush-cn/drishti") for u in urls), urls)
        self.assertNotIn("@import", self.PAGE.read_text())


if __name__ == "__main__":
    unittest.main()
