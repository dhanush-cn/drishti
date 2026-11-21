"""Every report ranks up to 3 services, an unsure one too (patch 77, Week 7 design G4). Scripted model, no network."""
import unittest

from drishti_agent.evals.score import score

from test_graph import call, hyps, mark, run


class Ranked(unittest.TestCase):
    def test_unsure_still_ranks_the_page_first_even_if_refuted(self):
        # Capture 2, recommendationCacheFailure: the model refuted the paged service on narrative and ended unsure
        # with an empty list. The page is evidence about the paged service, so it stays in the top 3.
        r, _, _ = run([hyps(("frontend", "error", 0.6)), call("c1", "spans", service="frontend"),
                       mark("refuted", 0.2, ["t1"], "only client errors"), hyps()])
        self.assertIsNone(r.diagnosis.root_cause_service)
        self.assertEqual(r.diagnosis.ranked_services[0], "frontend")
        self.assertIn("product-catalog", r.diagnosis.ranked_services, "then its callee from the candidate table")
        self.assertLessEqual(len(r.diagnosis.ranked_services), 3)

    def test_hypotheses_not_refuted_come_before_the_padding(self):
        r, _, _ = run([hyps(("email", "memory", 0.5)), call("c1", "spans", service="email"),
                       mark("inconclusive", 0.4, ["t1"], "maybe"), hyps()])
        self.assertEqual(r.diagnosis.ranked_services, ["email", "frontend", "product-catalog"])

    def test_a_refuted_hypothesis_is_not_ranked(self):
        r, _, _ = run([hyps(("cart", "error", 0.6)), call("c1", "spans", service="cart"),
                       mark("refuted", 0.1, ["t1"], "nothing"), hyps()])
        self.assertNotIn("cart", r.diagnosis.ranked_services, "cart isn't in the candidate table: nothing brings it back")

    def test_a_committed_answer_is_first_and_nothing_repeats(self):
        r, _, _ = run([hyps(("Product-Catalog", "error", 0.6)), call("c1", "spans", service="Product-Catalog"),
                       mark("supported", 0.85, ["t1"])])
        ranked = r.diagnosis.ranked_services
        self.assertEqual(ranked[0], "Product-Catalog")
        self.assertEqual(len({s.lower() for s in ranked}), len(ranked), "product-catalog from the table is not added again")
        self.assertEqual(len(ranked), 3)

    def test_unsure_counts_for_top3_not_top1(self):
        r, _, _ = run([hyps()])
        s = score("productCatalogFailure", r)
        self.assertEqual((s.top1, s.top3, s.unsure, s.confidently_wrong), (False, True, True, False))


if __name__ == "__main__":
    unittest.main()
