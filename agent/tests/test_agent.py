"""Agent, baseline, scoring and provider tests. No network, no model:
    cd agent && python -m unittest discover -s tests -v
"""
import json
import threading
import urllib.parse
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from drishti_agent import agent, baseline
from drishti_agent.api import APIError, QueryAPI, compact
from drishti_agent.evals.score import score
from drishti_agent.llm import Anthropic, LLMError, OpenAICompat, Reply, Scripted, ToolCall
from drishti_agent.models import Diagnosis, FaultType, RunResult, Usage

UNTIL = "2026-11-03T17:11:07Z"


class FakeAPI(QueryAPI):
    """Records every call; answers like the query API."""

    def __init__(self):
        super().__init__("http://fake", UNTIL)
        self.calls = []

    def _fetch(self, url):
        # Only the HTTP fetch is faked: QueryAPI.get builds the URL, so the
        # until the tests see is the one that would go over the wire.
        u = urllib.parse.urlparse(url)
        path, params = u.path, dict(urllib.parse.parse_qsl(u.query))
        self.calls.append((path, params))
        if path == "/v1/services":
            return {"data": {"services": ["checkout", "payment"]}, "masked_lines": 0}
        if path == "/v1/spans" and params.get("service") == "nope":
            raise APIError('unknown service "nope"; known: checkout, payment')
        return {"window": params.get("window", "10m0s"), "masked_lines": 3, "data": {"path": path, "args": params}}


def submit(**kw):
    args = {"root_cause_service": "payment", "fault_type": "error", "confidence": 0.8, "ranked_services": ["payment", "checkout"],
            "evidence": ["payment Charge error ratio 0.67"], "summary": "payment fails charges"}
    args.update(kw)
    return Reply("", [ToolCall("s1", "submit_diagnosis", args)], Usage(input_tokens=100, output_tokens=20))


class AgentLoop(unittest.TestCase):
    def test_triage_then_tools_then_submit(self):
        api = FakeAPI()
        llm = Scripted([
            Reply("checking checkout", [ToolCall("c1", "spans", {"service": "checkout", "window": "5m", "until": "2030-01-01T00:00:00Z"})], Usage(input_tokens=1000, output_tokens=50)),
            submit(),
        ])
        r = agent.investigate(api, llm, budget=5)
        self.assertTrue(r.valid_answer)
        self.assertEqual(r.diagnosis.root_cause_service, "payment")
        self.assertEqual([t.name for t in r.tool_calls], ["spans"])
        # Triage ran before the first model call: overview and anomalies, by the harness.
        self.assertEqual([p for p, _ in api.calls[:2]], ["/v1/overview", "/v1/anomalies"])
        self.assertIn("## overview", llm.sent[0][1][0]["content"])
        # The model asked for a future until; every call used the harness's.
        self.assertTrue(all(p["until"] == UNTIL for _, p in api.calls), api.calls)
        self.assertEqual(r.usage.input_tokens, 1100)
        self.assertEqual(r.llm_calls, 2)
        self.assertIn("submit_diagnosis", [t["name"] for t in llm.sent[0][2]])

    def test_budget_is_never_exceeded_and_last_turn_has_no_tools(self):
        api = FakeAPI()
        many = [ToolCall(f"c{i}", "overview", {}) for i in range(3)]
        llm = Scripted([Reply("", many), Reply("", many), Reply("I think it's payment but I'm not sure")])
        r = agent.investigate(api, llm, budget=4)
        self.assertEqual(len(r.tool_calls), 4, "tool calls beyond the budget must not run")
        self.assertIsNone(llm.sent[-1][2], "the out-of-budget turn must offer no tools")
        self.assertTrue(r.budget_exhausted)
        self.assertIsNone(r.diagnosis.root_cause_service, "no parsable answer: unsure, never a guess")
        refused = [m for m in r.transcript if m["role"] == "tool" and "budget exhausted" in m["content"]]
        self.assertEqual(len(refused), 2)

    def test_answer_in_text_after_budget_is_accepted(self):
        llm = Scripted([Reply("", [ToolCall("c1", "overview", {})]),
                        Reply('```json\n{"root_cause_service": "payment", "fault_type": "error", "confidence": 0.6}\n```')])
        r = agent.investigate(FakeAPI(), llm, budget=1)
        self.assertTrue(r.valid_answer)
        self.assertEqual(r.diagnosis.root_cause_service, "payment")

    def test_invalid_submission_is_recorded_not_repaired(self):
        r = agent.investigate(FakeAPI(), Scripted([submit(confidence=5)]), budget=3)
        self.assertFalse(r.valid_answer)
        self.assertIsNone(r.diagnosis.root_cause_service)

    def test_tool_errors_go_back_to_the_model(self):
        llm = Scripted([Reply("", [ToolCall("c1", "spans", {"service": "nope"})]), submit()])
        r = agent.investigate(FakeAPI(), llm, budget=3)
        tool_msg = next(m for m in r.transcript if m["role"] == "tool")
        self.assertTrue(tool_msg["content"].startswith("error: unknown service"))
        self.assertFalse(r.tool_calls[0].ok)

    def test_model_failure_is_an_error_not_an_answer(self):
        r = agent.investigate(FakeAPI(), Scripted([]), budget=3)
        self.assertIn("ran out", r.error)
        self.assertIsNone(r.diagnosis.root_cause_service)


class Baseline(unittest.TestCase):
    def test_one_prompt_dump_no_tools(self):
        llm = Scripted([Reply('Here: {"root_cause_service": "checkout", "fault_type": "error", "confidence": 0.9, "ranked_services": ["checkout"]}')])
        api = FakeAPI()
        r = baseline.diagnose(api, llm)
        self.assertTrue(r.valid_answer)
        self.assertEqual(r.diagnosis.root_cause_service, "checkout")
        self.assertIsNone(llm.sent[0][2], "the baseline gets no tools")
        paths = [p for p, _ in api.calls]
        self.assertIn("/v1/overview", paths)
        self.assertEqual(paths.count("/v1/log_templates"), 2, "templates of every listed service")
        self.assertLessEqual(len(llm.sent[0][1][0]["content"]), baseline.DUMP_CHARS + 200)


class Scoring(unittest.TestCase):
    def r(self, **kw):
        return RunResult(system="agent", diagnosis=Diagnosis(**kw), valid_answer=True)

    def test_scores(self):
        s = score("paymentFailure", self.r(root_cause_service="payment", fault_type="error", confidence=0.9))
        self.assertTrue(s.top1 and s.top3 and s.fault and not s.confidently_wrong)
        s = score("paymentFailure", self.r(root_cause_service="checkout", ranked_services=["checkout", "payment"], confidence=0.9))
        self.assertTrue(not s.top1 and s.top3 and s.confidently_wrong)
        s = score("paymentFailure", self.r(root_cause_service="checkout", confidence=0.5))
        self.assertFalse(s.confidently_wrong)
        s = score("paymentFailure", self.r(root_cause_service="unknown"))
        self.assertTrue(s.unsure and not s.top1 and not s.confidently_wrong)
        s = score("paymentUnreachable", self.r(root_cause_service="checkout"))
        self.assertTrue(s.top1)
        s = score("recommendationCacheFailure", self.r(root_cause_service="recommendation", fault_type="latency"))
        self.assertTrue(s.fault)

    def test_top3_dedupes_and_caps(self):
        d = Diagnosis(root_cause_service="a", ranked_services=["a", "b", "b", "c", "d"])
        self.assertEqual(d.top3(), ["a", "b", "c"])

    def test_compact_drops_nulls_and_caps(self):
        self.assertEqual(compact({"a": None, "b": [], "c": 1}), '{"c":1}')
        self.assertIn("truncated", compact({"x": "y" * 9000}, limit=100))


class Wire(unittest.TestCase):
    """The providers' request/response translation, against a local stub."""

    def serve(self, response):
        seen = []

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                seen.append((self.path, {k.lower(): v for k, v in self.headers.items()}, body))
                out = json.dumps(response).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

        srv = HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.shutdown)
        return f"http://127.0.0.1:{srv.server_port}", seen

    history = [
        {"role": "user", "content": "go"},
        {"role": "assistant", "content": "", "tool_calls": [{"id": "t1", "name": "overview", "arguments": {"window": "5m"}},
                                                            {"id": "t2", "name": "anomalies", "arguments": {}}]},
        {"role": "tool", "tool_call_id": "t1", "name": "overview", "content": "{}"},
        {"role": "tool", "tool_call_id": "t2", "name": "anomalies", "content": "[]"},
    ]

    def test_openai_compat(self):
        url, seen = self.serve({"choices": [{"message": {"content": None, "tool_calls": [
            {"id": "x", "type": "function", "function": {"name": "spans", "arguments": "{\"service\": \"payment\"}"}}]}}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 7}})
        r = OpenAICompat("m", url + "/v1", api_key="k").chat("sys", self.history, [{"name": "overview", "description": "d", "parameters": {}}])
        self.assertEqual(r.tool_calls[0].arguments, {"service": "payment"})
        self.assertEqual((r.usage.input_tokens, r.usage.output_tokens), (11, 7))
        path, headers, body = seen[0]
        self.assertEqual(path, "/v1/chat/completions")
        self.assertEqual(headers["authorization"], "Bearer k")
        self.assertEqual(body["messages"][0], {"role": "system", "content": "sys"})
        self.assertEqual(json.loads(body["messages"][2]["tool_calls"][0]["function"]["arguments"]), {"window": "5m"})
        self.assertEqual([m["role"] for m in body["messages"]], ["system", "user", "assistant", "tool", "tool"])
        self.assertEqual(body["tools"][0]["type"], "function")

    def test_anthropic(self):
        url, seen = self.serve({"content": [{"type": "text", "text": "hm"}, {"type": "tool_use", "id": "u1", "name": "spans", "input": {"service": "cart"}}],
                                "usage": {"input_tokens": 20, "output_tokens": 5}})
        r = Anthropic("claude-x", "key", base_url=url).chat("sys", self.history, [{"name": "overview", "description": "d", "parameters": {"type": "object"}}])
        self.assertEqual((r.text, r.tool_calls[0].name, r.tool_calls[0].arguments), ("hm", "spans", {"service": "cart"}))
        path, headers, body = seen[0]
        self.assertEqual(path, "/v1/messages")
        self.assertEqual(headers["x-api-key"], "key")
        self.assertEqual(body["system"], "sys")
        # Both tool results in ONE user turn, right after the assistant's tool_use turn.
        self.assertEqual([m["role"] for m in body["messages"]], ["user", "assistant", "user"])
        self.assertEqual([b["tool_use_id"] for b in body["messages"][2]["content"]], ["t1", "t2"])
        self.assertEqual(body["messages"][1]["content"][0]["type"], "tool_use")
        self.assertEqual(body["tools"][0]["input_schema"], {"type": "object"})

    def test_non_retryable_error_raises(self):
        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"error":"bad model"}')

        srv = HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.shutdown)
        with self.assertRaises(LLMError):
            OpenAICompat("m", f"http://127.0.0.1:{srv.server_port}/v1").chat("s", [{"role": "user", "content": "x"}])


if __name__ == "__main__":
    unittest.main()
