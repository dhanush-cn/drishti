"""scripts/leakcheck.py's rule: what counts as handing the agent the answer."""
import importlib.util
import pathlib
import unittest

_p = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "leakcheck.py"
_spec = importlib.util.spec_from_file_location("leakcheck", _p)
lc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lc)


class LeakRule(unittest.TestCase):
    def test_flag_names_and_flag_words_leak_anywhere(self):
        self.assertEqual(lc.leak('{"body":"High CPU-Load problempattern enabled"}')[0], "problempattern")
        self.assertEqual(lc.leak('{"body":"recommendationCacheFailure on"}')[0], "recommendationcachefailure")
        self.assertEqual(lc.leak('{"body":"GET /flagservice/x"}')[0], "flagservice")

    def test_load_generator_leaks_anywhere(self):
        self.assertIsNotNone(lc.leak('{"body":"request from load-generator"}'))

    def test_collector_named_in_a_services_own_log_is_not_a_leak(self):
        body = ('{"service":"recommendation","severity":"ERROR","body":"Failed to export traces to otel-collector:4317, '
                'error code: StatusCode.DEADLINE_EXCEEDED"}')
        self.assertIsNone(lc.leak(body))

    def test_collector_as_a_service_is_a_leak(self):
        self.assertEqual(lc.leak('{"service":"otel-collector","cpu":0.4}')[0], "otel-collector")
        self.assertEqual(lc.leak('{"service": "container:otelcol-contrib"}')[0], "otelcol-contrib")
        self.assertEqual(lc.hidden_listed(["ad", "otel-collector", "cart"]), ["otel-collector"])
        self.assertEqual(lc.hidden_listed(["ad", "flagd"]), ["flagd"])


if __name__ == "__main__":
    unittest.main()
