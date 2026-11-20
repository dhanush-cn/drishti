"""The eval runner for Week 7 (patch 67): the graph system, variant labels, resuming across days, stopping on a rate
limit. Runs the real runner end to end over HTTP against the fake call graph. No network, no model."""
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from drishti_agent.evals import run as runner
from drishti_agent.llm import LLMError

from test_durable import ByContent, calls
from test_graph import API
from test_pagecheck import ALERT, at, graph_series, pc

ON = pc.parse_ts(at(29))


class Server:
    def __init__(self):
        graph = API(graph_series())

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.startswith("/v1/anomalies"):
                    body, code = {"data": [{**ALERT, "id": "a1", "state": "open"}]}, 200
                else:
                    try:
                        body, code = graph._fetch("http://fake" + self.path), 200
                    except Exception as e:
                        body, code = {"error": str(e)}, 400
                data = json.dumps(body).encode()
                self.send_response(code)
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"


def timeline(d: str) -> str:
    p = Path(d) / "timeline.jsonl"
    from datetime import timedelta
    rows = [{"phase": "baseline", "start": pc.iso(ON - timedelta(hours=1)), "end": pc.iso(ON - timedelta(minutes=20))},
            {"phase": "scenario", "n": 9, "flag": "productCatalogFailure", "variant": "on", "on_minutes": 4,
             "on": {"set_at": pc.iso(ON)}, "off": {"set_at": pc.iso(ON + timedelta(minutes=4))}}]
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return str(p)


class Failing:
    name = "failing"

    def chat(self, *a, **k):
        raise LLMError("HTTP 429: rate limit exceeded after 5 waits")


class Harness(unittest.TestCase):
    def setUp(self):
        self.s = Server()
        self.d = tempfile.mkdtemp()
        self.tl = timeline(self.d)
        self.log = tempfile.mktemp()

    def tearDown(self):
        self.s.srv.shutdown()

    def run_(self, llm, *extra):
        out = os.path.join(self.d, "out")
        with mock.patch.object(runner, "from_args", lambda *a, **k: llm):
            code = runner.main(["--api", self.s.url, "--timeline", self.tl, "--recording", "fake", "--provider", "ollama",
                                "--model", "m", "--systems", "graph", "--columns", "end-to-end", "--runs", "2", "--out", out, *extra])
        rows = [json.loads(l) for l in open(os.path.join(out, "runs.jsonl")) if l.strip()]
        return code, rows, out

    def test_graph_rows_and_a_label(self):
        code, rows, out = self.run_(ByContent(self.log), "--label", "no-rb")
        self.assertEqual(code, 0)
        self.assertEqual([(r["system"], r["run"]) for r in rows], [("graph:no-rb", 0), ("graph:no-rb", 1)])
        self.assertTrue(all(r["score"]["top1"] and r["fix_target"] for r in rows))
        res = (Path(out) / "RESULTS.md").read_text()
        self.assertIn("Graph:no-rb (end-to-end)", res)
        self.assertIn("fix on target 2/2", res)
        meta = json.loads((Path(out) / "summary.json").read_text())["meta"]
        self.assertEqual((meta["label"], len(meta["graph_prompt_version"])), ("no-rb", 12))

    def test_resume_skips_finished_cells(self):
        self.run_(ByContent(self.log))
        n = calls(self.log)
        code, rows, _ = self.run_(ByContent(self.log), "--resume")
        self.assertEqual((code, calls(self.log), len(rows)), (0, n, 2), "nothing re-run, nothing appended")

    def test_a_rate_limit_stops_cleanly_and_resume_finishes(self):
        code, rows, _ = self.run_(Failing())
        self.assertEqual(code, 3, "stopped: rerun with --resume")
        self.assertEqual(len(rows), 1, "stopped after the first failed cell")
        self.assertIn("429", rows[0]["result"]["error"])
        code, rows, out = self.run_(ByContent(self.log), "--resume")
        self.assertEqual(code, 0)
        self.assertEqual(len(rows), 3, "the failed cell retried and appended, the second cell run")
        summary = json.loads((Path(out) / "summary.json").read_text())
        self.assertEqual(summary["totals"]["graph/end-to-end"]["runs"], 2, "the report keeps the latest row per cell")
        self.assertEqual(summary["totals"]["graph/end-to-end"]["top1"], 2)

    def test_held_out_graph_needs_the_graph_prompts_frozen(self):
        frozen = os.path.join(self.d, "FROZEN.json")
        from drishti_agent.prompts import PROMPT_VERSION
        Path(frozen).write_text(json.dumps({"prompt_version": PROMPT_VERSION}))
        with mock.patch.object(runner, "FROZEN", Path(frozen)):
            code = runner.main(["--api", self.s.url, "--timeline", self.tl, "--recording", "fake", "--provider", "ollama",
                                "--model", "m", "--systems", "graph", "--held-out", "--out", os.path.join(self.d, "o2")])
        self.assertEqual(code, 2)
