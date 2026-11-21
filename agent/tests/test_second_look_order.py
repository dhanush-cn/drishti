"""The second look's order (patch 81, Week 7 change 2): its few calls go first to what changed. Scripted model."""
import unittest

from drishti_agent.graph import next_to_test

from test_graph import call, hyps, mark
from test_recheck import investigate


def h(i, service, prior):
    return {"id": i, "service": service, "prior": prior}


class Order(unittest.TestCase):
    def test_first_look_goes_by_prior(self):
        self.assertEqual(next_to_test([h("h1", "ad", 0.4), h("h2", "cart", 0.9)], None, "frontend"), "h2")

    def test_second_look_puts_the_changed_page_first_then_changed_services(self):
        rc = {"changed": ["Frontend-Proxy", "checkout"]}
        open_ = [h("h1", "ad", 0.95), h("h2", "checkout", 0.6), h("h3", "frontend-proxy", 0.2)]
        self.assertEqual(next_to_test(open_, rc, "frontend-proxy"), "h3", "the page changed: first, whatever its prior")
        self.assertEqual(next_to_test(open_[:2], rc, "frontend-proxy"), "h2", "a changed service before a higher prior")
        self.assertEqual(next_to_test(open_, rc, "frontend"), "h2", "an unchanged page gets no priority")
        self.assertEqual(next_to_test([h("h1", "ad", 0.3), h("h2", "email", 0.7)], rc, "frontend"), "h2", "then by prior")
        self.assertEqual(next_to_test([h("h1", "frontend", 0.9), h("h2", "checkout", 0.3)], rc, "frontend"), "h2",
                         "the page didn't change: a changed service goes before it")


class SecondLook(unittest.TestCase):
    def test_leftovers_about_unchanged_services_are_closed_and_the_changed_one_is_tested_first(self):
        first = [hyps(("ad", "cpu", 0.9), ("email", "memory", 0.8), ("payment", "error", 0.7))]
        for svc in ("ad", "email"):
            first += [call(f"{svc}{i}", "spans", service=svc) for i in range(4)] + [mark("refuted", 0.2, [], "nothing")]
        first += [call(f"p{i}", "spans", service="payment") for i in range(4)]   # the 15th model call: cap reached
        second = [hyps(("email", "latency", 0.9), ("cart", "error", 0.3)), call("c1", "spans", service="cart"),
                  mark("supported", 0.8, ["t13"])]
        r, info, llm, _ = investigate(first + second)
        self.assertEqual(info["recheck"]["llm_calls_before"], 15)
        self.assertIn("own signal changed since the first look are tested first", llm.sent[15][1][0]["content"])
        by_id = {x["id"]: x for x in info["hypotheses"]}
        self.assertEqual((by_id["h3"]["service"], by_id["h3"]["status"]), ("payment", "inconclusive"))
        self.assertEqual(by_id["h3"]["note"], "not tested: superseded by the second look")
        self.assertEqual(info["recheck"]["superseded"], ["h3"])
        self.assertEqual(r.tool_calls[12].arguments["service"], "cart", "cart changed: tested before email's higher prior")
        self.assertEqual(r.diagnosis.root_cause_service, "cart")
        self.assertEqual(by_id["h4"]["status"], "open", "email latency was never reached")


    def test_a_leftover_about_a_changed_service_stays_open_and_goes_first(self):
        first = [hyps(("ad", "cpu", 0.9), ("email", "memory", 0.8), ("cart", "error", 0.7))]
        for svc in ("ad", "email"):
            first += [call(f"{svc}{i}", "spans", service=svc) for i in range(4)] + [mark("refuted", 0.2, [], "nothing")]
        first += [call(f"c{i}", "spans", service="cart") for i in range(4)]      # cap reached while testing cart
        second = [hyps(("payment", "error", 0.9)), call("c9", "spans", service="cart"), mark("supported", 0.8, ["t13"])]
        r, info, _, _ = investigate(first + second)
        by_id = {x["id"]: x for x in info["hypotheses"]}
        self.assertEqual(info["recheck"]["superseded"], [])
        self.assertEqual((by_id["h3"]["service"], by_id["h3"]["status"]), ("cart", "supported"))
        self.assertEqual(r.diagnosis.root_cause_service, "cart")


if __name__ == "__main__":
    unittest.main()
