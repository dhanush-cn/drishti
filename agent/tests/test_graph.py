"""The Week 6 hypothesis graph (patch 60): every judge edge, grounding, F4, the budgets. Scripted model, no network.

The judge-rule tests here (Grounding, F4Judge) are the ones Dhanush reviews line by line (design Q7, option c).
"""
import json
import unittest

from drishti_agent import graph
from drishti_agent.api import APIError
from drishti_agent.llm import Reply, Scripted, ToolCall
from drishti_agent.models import Usage
from drishti_agent.prompts_graph import GRAPH_PROMPT_VERSION
from drishti_agent.prompts import PROMPT_VERSION

from test_pagecheck import ALERT, GraphAPI, graph_series


class API(GraphAPI):
    """The call-graph fake plus the endpoints triage and the tests use."""

    def _fetch(self, url):
        import urllib.parse
        u = urllib.parse.urlparse(url)
        path, q = u.path, dict(urllib.parse.parse_qsl(u.query))
        if path == "/v1/anomalies":
            self.calls.append((path, q))
            return {"data": [{"id": "a1", "service": "frontend", "rule": "log_template_new", "state": "open"}]}
        if path == "/v1/spans" and q.get("service") == "payment":
            self.calls.append((path, q))
            raise APIError("query failed: timeout")
        if path in ("/v1/logs", "/v1/log_templates"):
            self.calls.append((path, q))
            return {"data": {"service": q["service"], "lines": []}}
        return super()._fetch(url)


def hyps(*items, cid="h"):
    return Reply("", [ToolCall(cid, "submit_hypotheses", {"hypotheses": [
        {"service": s, "fault_type": f, "prior": p, "why": f"because {s}"} for s, f, p in items]})], Usage(input_tokens=1000, output_tokens=50))


def call(cid, name, **args):
    return Reply("", [ToolCall(cid, name, args)], Usage(input_tokens=1000, output_tokens=20))


def mark(status, conf, evidence, summary="found it", cid="m", **extra):
    return Reply("", [ToolCall(cid, "mark_hypothesis", {"status": status, "confidence": conf, "evidence": evidence,
                                                        "summary": summary, **extra})], Usage(input_tokens=1000, output_tokens=20))


def run(replies, alert=ALERT, **kw):
    llm = Scripted(replies)
    r = graph.investigate(API(graph_series()), llm, alert=alert, **kw)
    info = json.loads(r.transcript[-1]["content"])
    return r, info, llm


class Flow(unittest.TestCase):
    def test_page_to_callee_supported_and_grounded(self):
        r, info, llm = run([hyps(("product-catalog", "error", 0.6), ("frontend", "error", 0.3)),
                            call("c1", "spans", service="product-catalog", window="2m"),
                            mark("supported", 0.85, ["t1"])])
        self.assertEqual((r.diagnosis.root_cause_service, r.diagnosis.fault_type.value), ("product-catalog", "error"))
        self.assertEqual((r.llm_calls, [t.name for t in r.tool_calls]), (3, ["spans"]))
        self.assertTrue(r.valid_answer)
        self.assertIn("[t1] spans", r.diagnosis.evidence[0])
        self.assertEqual(r.diagnosis.ranked_services[:2], ["product-catalog", "frontend"])
        self.assertIn("accepted", info["verdicts"][-1])
        # The model saw the candidate table and the result tagged with its id.
        first = llm.sent[0][1][0]["content"]
        self.assertIn("## candidates", first)
        self.assertIn("product-catalog [callee depth 1 via frontend]", first)
        self.assertTrue(any(m.get("content", "").startswith("[t1] ") for m in llm.sent[2][1] if m["role"] == "tool"))

    def test_refuted_then_next_hypothesis(self):
        r, info, _ = run([hyps(("frontend", "error", 0.6), ("product-catalog", "error", 0.4)),
                          call("c1", "spans", service="frontend"), mark("refuted", 0.1, ["t1"], "only client errors"),
                          call("c2", "spans", service="product-catalog"), mark("supported", 0.8, ["t2"])])
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")
        self.assertIn("frontend (error): refuted", r.diagnosis.ruled_out[0])

    def test_new_round_when_a_round_is_exhausted(self):
        r, info, llm = run([hyps(("frontend", "error", 0.6)),
                            call("c1", "spans", service="frontend"), mark("refuted", 0.1, ["t1"]),
                            hyps(("product-catalog", "error", 0.5)),
                            call("c2", "spans", service="product-catalog"), mark("supported", 0.8, ["t2"])])
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")
        self.assertEqual([h["round"] for h in info["hypotheses"]], [1, 2])
        self.assertIn("h1 (round 1): frontend (error), prior 0.60: refuted", llm.sent[3][1][0]["content"])

    def test_already_tested_services_are_not_retested(self):
        r, info, _ = run([hyps(("frontend", "error", 0.6)),
                          call("c1", "spans", service="frontend"), mark("refuted", 0.1, ["t1"]),
                          hyps(("frontend", "error", 0.9))])
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertIn("no untested hypothesis", info["verdicts"][-1])

    def test_a_refuted_service_can_be_tested_with_another_fault_type(self):
        # Capture 2, 21 Nov: recommendation refuted as an error; round 2's latency hypothesis was dropped as "tested".
        r, info, llm = run([hyps(("frontend", "error", 0.6)),
                            call("c1", "spans", service="frontend"), mark("refuted", 0.1, ["t1"]),
                            hyps(("frontend", "traffic", 0.5)),
                            call("c2", "spans", service="frontend"), mark("supported", 0.8, ["t2"])])
        self.assertEqual([(h["service"], h["fault_type"], h["status"]) for h in info["hypotheses"]],
                         [("frontend", "error", "refuted"), ("frontend", "traffic", "supported")])
        self.assertIn("Test this hypothesis: h2: frontend (traffic)", llm.sent[4][1][0]["content"])
        self.assertEqual((r.diagnosis.root_cause_service, r.diagnosis.fault_type.value), ("frontend", "traffic"))

    def test_the_fault_type_a_test_marked_counts_as_tested_too(self):
        r, info, _ = run([hyps(("frontend", "error", 0.6)),
                          call("c1", "spans", service="frontend"), mark("refuted", 0.1, ["t1"], fault_type="latency"),
                          hyps(("frontend", "latency", 0.9), ("frontend", "error", 0.8))])
        self.assertEqual(len(info["hypotheses"]), 1)
        self.assertIn("no untested hypothesis", info["verdicts"][-1])

    def test_empty_hypothesis_list_is_unsure(self):
        r, _, _ = run([hyps()])
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertTrue(r.valid_answer, "an honest unsure is a valid answer")


class Grounding(unittest.TestCase):
    """The judge accepts only evidence ABOUT the hypothesis's service."""

    def test_evidence_about_another_service_does_not_count(self):
        r, info, _ = run([hyps(("product-catalog", "error", 0.6)),
                          call("c1", "spans", service="frontend"),       # frontend's spans, cited for product-catalog
                          mark("supported", 0.9, ["t1"]),
                          hyps()])
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertIn("no cited evidence is about product-catalog", info["hypotheses"][0]["note"])

    def test_an_overview_counts_only_by_its_row(self):
        # product-catalog has a row in the overview the fake returns: grounded.
        r, _, _ = run([hyps(("product-catalog", "error", 0.6)),
                       call("c1", "overview", window="2m"), mark("supported", 0.8, ["t1"])])
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")
        self.assertFalse(graph.about({"name": "overview", "text": "services with changes:\n- frontend: x\nfrontend: 1 | 2"},
                                     "product-catalog"))
        self.assertFalse(graph.about({"name": "overview", "text": "- frontend: calls to product-catalog failing"}, "product-catalog"),
                         "a mention inside another service's row is not its row")

    def test_the_candidate_row_counts_when_it_shows_an_own_change(self):
        r, _, _ = run([hyps(("product-catalog", "error", 0.6)), mark("supported", 0.8, ["candidates"])])
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")
        r, info, _ = run([hyps(("frontend", "error", 0.6)), mark("supported", 0.8, ["candidates"]), hyps()])
        self.assertIsNone(r.diagnosis.root_cause_service, "frontend's row shows victim signs only")

    def test_the_page_counts_for_the_paged_service_only(self):
        # Patch 71: on capture 2 the page often came before the root cause's metrics moved (adHighCpu: ad's CPU
        # 0.494 vs 0.468 at the alert); the alert itself is the paged service's own change.
        r, _, _ = run([hyps(("frontend", "error", 0.6)), mark("supported", 0.8, ["page"])])
        self.assertEqual(r.diagnosis.root_cause_service, "frontend")
        self.assertEqual(r.diagnosis.evidence, [f"[page] frontend log_template_new opened at {ALERT['opened_at']}"])
        r, info, _ = run([hyps(("product-catalog", "error", 0.6)), mark("supported", 0.9, ["page"]), hyps()])
        self.assertIsNone(r.diagnosis.root_cause_service, "the page is not about another service")
        self.assertIn("no cited evidence is about product-catalog", info["hypotheses"][0]["note"])
        r, info, _ = run([hyps(("frontend", "error", 0.6)), mark("supported", 0.9, ["page"]), hyps()], alert=None)
        self.assertIsNone(r.diagnosis.root_cause_service, "no page, nothing to cite (agent-only)")

    def test_unknown_ids_do_not_count(self):
        r, info, _ = run([hyps(("product-catalog", "error", 0.6)), mark("supported", 0.9, ["t99"]), hyps()])
        self.assertIsNone(r.diagnosis.root_cause_service)

    def test_a_failed_query_about_the_service_does_not_count(self):
        # The call was about payment, but it failed: it shows nothing.
        # Agent-only, so F4 (which would also refuse payment: not a candidate) can't be what stops it.
        r, info, _ = run([hyps(("payment", "error", 0.6)),
                          call("c1", "spans", service="payment"), mark("supported", 0.9, ["t1"]), hyps()], alert=None)
        self.assertFalse(r.tool_calls[0].ok)
        self.assertIsNone(r.diagnosis.root_cause_service)

    def test_low_confidence_support_is_not_reported(self):
        r, info, _ = run([hyps(("product-catalog", "error", 0.6)),
                          call("c1", "spans", service="product-catalog"), mark("supported", 0.5, ["t1"]), hyps()])
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertEqual(r.diagnosis.ranked_services[0], "product-catalog", "still ranked first for top-3")


class F4Judge(unittest.TestCase):
    """F4 in the judge: off by default (patch 69); switched on, end-to-end only, refused once = sent back, twice = unsure."""

    def test_earlier_incident_is_refused_and_the_run_goes_on(self):
        r, info, llm = run([hyps(("ad", "cpu", 0.7), ("product-catalog", "error", 0.4)),
                            call("c1", "timeseries", service="ad", signal="cpu"), mark("supported", 0.9, ["t1"]),
                            call("c2", "spans", service="product-catalog"), mark("supported", 0.8, ["t2"])], f4=True)
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")
        self.assertEqual(info["hypotheses"][0]["status"], "refused")
        self.assertIn("earlier incident", info["refusals"][0])
        self.assertIn("ad (cpu): refused", r.diagnosis.ruled_out[0])

    def test_refused_twice_is_unsure_never_a_guess(self):
        r, info, _ = run([hyps(("ad", "cpu", 0.7), ("checkout", "error", 0.4)),
                          call("c1", "timeseries", service="ad", signal="cpu"), mark("supported", 0.9, ["t1"]),
                          call("c2", "spans", service="checkout"), mark("supported", 0.9, ["t2"])], f4=True)
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertEqual(len(info["refusals"]), 2)
        self.assertTrue(any("refused by F4" in x for x in r.diagnosis.ruled_out))

    def test_off_by_default(self):
        # ad is an earlier incident by F4's rule; with F4 off (the default since the ceiling check) it is accepted.
        r, info, _ = run([hyps(("ad", "cpu", 0.7)), call("c1", "timeseries", service="ad", signal="cpu"),
                          mark("supported", 0.9, ["t1"])])
        self.assertEqual(r.diagnosis.root_cause_service, "ad")
        self.assertEqual(info["refusals"], [])

    def test_agent_only_has_no_f4(self):
        r, _, llm = run([hyps(("ad", "cpu", 0.7)), call("c1", "timeseries", service="ad", signal="cpu"),
                         mark("supported", 0.9, ["t1"])], alert=None, f4=True)
        self.assertEqual(r.diagnosis.root_cause_service, "ad")
        self.assertIn("no single alert identifies it", llm.sent[0][1][0]["content"])

    def test_no_edge_service_is_accepted_on_its_own_signal(self):
        r, _, _ = run([hyps(("email", "memory", 0.6)), call("c1", "timeseries", service="email", signal="memory_mib"),
                       mark("supported", 0.85, ["t1"], fault_type="memory")], f4=True)
        self.assertEqual((r.diagnosis.root_cause_service, r.diagnosis.fault_type.value), ("email", "memory"))


class Budgets(unittest.TestCase):
    def test_per_hypothesis_cap_then_only_mark_is_offered(self):
        replies = [hyps(("product-catalog", "error", 0.6))]
        replies += [call(f"c{i}", "spans", service="product-catalog") for i in range(graph.PER_HYPOTHESIS)]
        replies += [mark("supported", 0.8, ["t1"])]
        r, _, llm = run(replies)
        self.assertEqual(len(r.tool_calls), graph.PER_HYPOTHESIS)
        self.assertEqual([t["name"] for t in llm.sent[-1][2]], ["mark_hypothesis"])
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")

    def test_total_budget_ends_unsure(self):
        replies = [hyps(("frontend", "error", 0.5), ("checkout", "error", 0.4), ("cart", "error", 0.3))]
        for k, svc in enumerate(("frontend", "checkout", "cart")):
            replies += [call(f"{svc}{i}", "spans", service=svc) for i in range(graph.PER_HYPOTHESIS)]
            replies += [mark("inconclusive", 0.2, [])]
        r, info, _ = run(replies, budget=10)
        self.assertEqual(len(r.tool_calls), 10)
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertTrue(r.budget_exhausted)

    def test_model_call_cap(self):
        # Three rounds of three hypotheses, each tested with one call and a verdict, would take 3 x 7 = 21 model
        # calls: the cap stops it at MAX_LLM_CALLS (the tool budget is out of the way).
        replies = []
        for rnd in range(3):
            svcs = [f"s{rnd}{k}" for k in range(3)]
            replies.append(hyps(*[(s, "error", 0.5) for s in svcs]))
            for s_ in svcs:
                replies += [call(f"c{s_}", "spans", service=s_), mark("inconclusive", 0.2, [])]
        r, info, llm = run(replies, budget=100)
        self.assertEqual(r.llm_calls, graph.MAX_LLM_CALLS)
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertIn("model-call cap", " ".join(info["verdicts"]))

    def test_page_check_has_its_own_cap(self):
        api = API(graph_series())
        graph.investigate(api, Scripted([hyps()]), alert=ALERT, pc_cap=2)
        walk = [p for p, _ in api.calls[2:]]  # after triage's overview + anomalies
        self.assertEqual(len(walk), 2)


class Turns(unittest.TestCase):
    def test_verdict_batched_with_tool_calls_is_refused(self):
        r, _, llm = run([hyps(("product-catalog", "error", 0.6)),
                         Reply("", [ToolCall("c1", "spans", {"service": "product-catalog"}),
                                    ToolCall("m0", "mark_hypothesis", {"status": "supported", "confidence": 0.9, "evidence": ["t1"], "summary": "x"})]),
                         mark("supported", 0.8, ["t1"])])
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")
        self.assertTrue(any("not accepted" in (m.get("content") or "") for m in llm.sent[-1][1]))

    def test_stall_becomes_inconclusive(self):
        r, info, _ = run([hyps(("product-catalog", "error", 0.6)), Reply("hmm"), Reply("hmm"), Reply("hmm"), hyps()])
        self.assertEqual(info["hypotheses"][0]["status"], "inconclusive")

    def test_hypotheses_in_text_are_accepted(self):
        text = Reply('{"hypotheses": [{"service": "product-catalog", "fault_type": "error", "prior": 0.6, "why": "x"}]}')
        r, _, _ = run([text, call("c1", "spans", service="product-catalog"), mark("supported", 0.8, ["t1"])])
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")

    def test_no_usable_hypotheses_twice_is_unsure(self):
        r, info, _ = run([Reply("no idea"), Reply("still no idea")])
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertIn("no usable hypotheses", info["verdicts"][-1])

    def test_model_error_is_recorded_not_answered(self):
        r, _, _ = run([hyps(("product-catalog", "error", 0.6))])
        self.assertIn("ran out", r.error)
        self.assertFalse(r.valid_answer)
        self.assertIsNone(r.diagnosis.root_cause_service)


class Prompts(unittest.TestCase):
    def test_v0_prompts_untouched_and_graph_prompts_versioned(self):
        self.assertEqual(PROMPT_VERSION, "40abe73ebe28", "the Week 5 freeze must hold")
        self.assertEqual(len(GRAPH_PROMPT_VERSION), 12)


class Runner(unittest.TestCase):
    def test_heuristic_floor_is_refused_for_the_graph(self):
        import tempfile
        from drishti_agent.evals import run as runner
        with tempfile.TemporaryDirectory() as d:
            code = runner.main(["--api", "http://x", "--timeline", "/nonexistent", "--recording", "r", "--provider", "heuristic",
                                "--model", "none", "--systems", "graph", "--out", d])
        self.assertEqual(code, 2)
