"""When the second look fires (patch 76, Week 7 design G2). Scripted model, no network.

Fires once, on an end-to-end run whose first look ends unsure. Never after a committed answer, never agent-only (no
page), never twice, never after a model error, never when switched off."""
import json
import unittest
from drishti_agent import graph
from drishti_agent.llm import Scripted

from test_graph import call, hyps, mark
from test_pagecheck import ALERT, graph_series
from test_recheck import UNSURE_AT_ONCE, Later, investigate


def info_of(r):
    return json.loads(r.transcript[-1]["content"])


class Trigger(unittest.TestCase):
    def test_an_unsure_end_to_end_run_looks_again_once(self):
        r, info, _, _ = investigate(UNSURE_AT_ONCE + [hyps()])
        self.assertIsNotNone(info["recheck"])
        self.assertEqual(r.llm_calls, 2, "the first look's call, then the second look's")
        self.assertEqual(sum("looking again" in v for v in info["verdicts"]), 1)
        self.assertIsNone(r.diagnosis.root_cause_service)

    def test_a_committed_answer_is_never_rechecked(self):
        r, info, _, _ = investigate([hyps(("product-catalog", "error", 0.6)), call("c1", "spans", service="product-catalog"),
                                     mark("supported", 0.85, ["t1"])])
        self.assertEqual(r.diagnosis.root_cause_service, "product-catalog")
        self.assertIsNone(info["recheck"])
        self.assertEqual(r.llm_calls, 3)

    def test_agent_only_never_looks_again(self):
        api = Later(graph_series())
        api.until = ALERT["opened_at"]
        r = graph.investigate(api, Scripted(UNSURE_AT_ONCE + [hyps()]), alert=None, recheck_minutes=2.0)
        self.assertIsNone(info_of(r)["recheck"])
        self.assertEqual(r.llm_calls, 1, "no page, no second look")

    def test_there_is_no_second_recheck(self):
        r, info, llm, _ = investigate(UNSURE_AT_ONCE + [hyps()] + [hyps()] * 3)
        self.assertEqual(r.llm_calls, 2)
        self.assertEqual(len(llm.replies), 3, "the spare replies were never asked for")
        self.assertIsNone(r.error)

    def test_a_model_error_is_never_rechecked(self):
        r, info, _, _ = investigate([])   # the model fails on its first call
        self.assertIsNotNone(r.error)
        self.assertIsNone(info["recheck"])
        self.assertEqual(r.llm_calls, 0)

    def test_switched_off_it_never_fires(self):
        api = Later(graph_series())
        api.until = ALERT["opened_at"]
        r = graph.investigate(api, Scripted(UNSURE_AT_ONCE + [hyps()]), alert=ALERT, recheck_minutes=None)
        self.assertIsNone(info_of(r)["recheck"])
        self.assertEqual(r.llm_calls, 1)


if __name__ == "__main__":
    unittest.main()
