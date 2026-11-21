"""The second look (patch 76, Week 7 design G2): how it works once it fires. Scripted model, no network.

These tests cover the mechanism: the moment moves, the budget is fresh and small, what may be proposed again, the
live wait and its resume after a crash. WHEN it fires (unsure end-to-end only; never after a committed answer, never
agent-only, never twice, never after a model error) is tested in test_recheck_trigger.py, which Dhanush writes
(the Week 6 split, design Q7 option c)."""
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from drishti_agent import graph
from drishti_agent.llm import Scripted
from drishti_agent.pagecheck import iso, parse_ts

from test_graph import API, call, hyps, mark
from test_pagecheck import ALERT, graph_series

SECOND = iso(parse_ts(ALERT["opened_at"]) + timedelta(minutes=2))


class Later(API):
    """The fake call graph, plus cart's own errors from one minute after the page (in the overview only after that)."""

    def _fetch(self, url):
        import urllib.parse
        u = urllib.parse.urlparse(url)
        q = dict(urllib.parse.parse_qsl(u.query))
        out = super()._fetch(url)
        if u.path == "/v1/overview" and parse_ts(q["until"]) >= parse_ts(ALERT["opened_at"]) + timedelta(minutes=1):
            out = {"data": {"services": out["data"]["services"] + [{"service": "cart", "changes": ["server error ratio 0.300 (was 0.000)"]}]}}
        return out


def investigate(replies, minutes=2.0, api=None):
    llm = Scripted(replies)
    api = api or Later(graph_series())
    api.until = ALERT["opened_at"]
    r = graph.investigate(api, llm, alert=ALERT, recheck_minutes=minutes)
    return r, json.loads(r.transcript[-1]["content"]), llm, api


UNSURE_AT_ONCE = [hyps()]   # the first look's hypothesize call proposes nothing: unsure


class SecondLook(unittest.TestCase):
    def test_queries_move_to_the_second_moment_and_the_model_is_told(self):
        r, info, llm, api = investigate(UNSURE_AT_ONCE + [hyps(("cart", "error", 0.7)), call("c1", "spans", service="cart"),
                                                           mark("supported", 0.8, ["t1"])])
        self.assertEqual(r.diagnosis.root_cause_service, "cart")
        self.assertEqual((info["recheck"]["until"], info["recheck"]["minutes"], info["recheck"]["changed"]), (SECOND, 2.0, ["cart"]))
        untils = [q["until"] for _, q in api.calls]
        first_look = untils.index(SECOND)
        self.assertTrue(all(u == ALERT["opened_at"] for u in untils[:first_look]), "the first look queries the page's moment")
        self.assertTrue(all(u == SECOND for u in untils[first_look:]), "everything after it, the tools included, the second moment")
        prompt = llm.sent[1][1][0]["content"]
        self.assertIn("Second look, 2 min after the first", prompt)
        self.assertIn("Own changes that were not there at the first look: cart", prompt)
        self.assertIn(f"paged at {SECOND}", prompt)
        self.assertIn("looking again", " ".join(info["verdicts"]))
        self.assertGreater(info["recheck"]["pagecheck_calls_first"], 0, "the first look's page_check calls are kept for the cost report")

    def test_the_second_look_has_its_own_small_budget(self):
        endless = [hyps(("cart", "error", 0.7), ("ad", "cpu", 0.6), ("email", "memory", 0.5))]
        for i in range(30):
            endless.append(call(f"c{i}", "spans", service="cart"))
        r, info, _, _ = investigate(UNSURE_AT_ONCE + endless)
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertLessEqual(r.llm_calls, 1 + graph.RECHECK_LLM_CALLS)
        self.assertLessEqual(len(r.tool_calls), graph.RECHECK_TOOLS)
        self.assertEqual(info["recheck"]["llm_calls_before"], 1)

    def test_the_second_look_is_one_round(self):
        r, info, _, _ = investigate(UNSURE_AT_ONCE + [hyps(("cart", "error", 0.7)), call("c1", "spans", service="cart"),
                                                       mark("refuted", 0.2, ["t1"], "nothing")])
        self.assertIsNone(r.error, "no second hypothesize call was made (the script has none)")
        self.assertEqual(r.llm_calls, 4)
        self.assertIn("rounds of hypotheses", r.diagnosis.summary)

    def test_a_refuted_service_returns_only_if_its_own_signal_changed(self):
        first = [hyps(("cart", "error", 0.5), ("ad", "cpu", 0.4)),
                 call("c1", "spans", service="cart"), mark("refuted", 0.1, ["t1"], "nothing on cart"),
                 call("c2", "spans", service="ad"), mark("refuted", 0.1, ["t2"], "nothing on ad"),
                 hyps()]
        second = [hyps(("cart", "error", 0.6), ("ad", "cpu", 0.6)), call("c3", "spans", service="cart"),
                  mark("supported", 0.8, ["t3"])]
        r, info, _, _ = investigate(first + second)
        looks = [(h["service"], h.get("look")) for h in info["hypotheses"]]
        self.assertEqual(looks, [("cart", 1), ("ad", 1), ("cart", 2)], "cart changed since: back; ad didn't: still tested")
        self.assertEqual(r.diagnosis.root_cause_service, "cart")

    def test_at_zero_minutes_the_moment_stays(self):
        r, info, _, api = investigate(UNSURE_AT_ONCE + [hyps()], minutes=0.0)
        self.assertEqual((info["recheck"]["until"], info["recheck"]["minutes"]), (ALERT["opened_at"], 0.0))
        self.assertTrue(all(q["until"] == ALERT["opened_at"] for _, q in api.calls))
        self.assertEqual(r.llm_calls, 2, "the extra round still runs")


class Killed(Exception):
    pass


class LiveWait(unittest.TestCase):
    def test_waits_for_the_second_moment_and_a_crash_mid_wait_resumes_the_rest(self):
        from langgraph.checkpoint.memory import InMemorySaver
        saver = InMemorySaver()
        cfg = {"configurable": {"thread_id": "t"}, "recursion_limit": graph.RECURSION_LIMIT}
        page = datetime.now(timezone.utc) - timedelta(minutes=2) + timedelta(seconds=3)
        alert = {**ALERT, "opened_at": iso(page)}

        def build(llm):
            api = API(graph_series())
            api.until = iso(page)
            return graph.build_graph(api, llm, alert=alert, checkpointer=saver, recheck_minutes=2.0, live_wait=True)

        real_sleep = graph.time.sleep
        with mock.patch.object(graph.time, "sleep", side_effect=Killed("killed during the wait")):
            with self.assertRaises(Killed):
                build(Scripted(UNSURE_AT_ONCE)).invoke({}, cfg)
        mid = build(Scripted([])).get_state(cfg)
        self.assertEqual(mid.next, ("recheck",), "the first look is saved; the recheck is what's left")
        self.assertEqual(mid.values["llm_calls"], 1)
        with mock.patch.object(graph.time, "sleep", side_effect=real_sleep):
            s = build(Scripted([hyps()])).invoke(None, cfg)
        self.assertEqual(s["llm_calls"], 2, "the first look's model call was not repeated")
        self.assertGreater(s["recheck"]["waited_s"], 0.5)
        self.assertLess(s["recheck"]["waited_s"], 5.0, "only the rest of the wait")
        self.assertGreaterEqual(parse_ts(s["recheck"]["until"]), page + timedelta(minutes=2))


if __name__ == "__main__":
    unittest.main()
