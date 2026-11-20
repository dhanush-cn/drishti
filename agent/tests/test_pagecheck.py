"""page_check, the onset and trend rules, and design rule F4 (patch 57). No network, no model."""
import unittest
import urllib.parse
from datetime import datetime, timedelta, timezone

from drishti_agent import pagecheck as pc
from drishti_agent.api import APIError, QueryAPI

UNTIL = datetime(2026, 11, 3, 17, 11, 0, tzinfo=timezone.utc)


def series(vals, end=UNTIL, step=30):
    """Points every `step` s, the last one at `end`, as /v1/timeseries returns them."""
    n = len(vals)
    return [{"t": pc.iso(end - timedelta(seconds=step * (n - 1 - i))), "v": v} for i, v in enumerate(vals)]


def at(i, n=60, end=UNTIL, step=30):
    return pc.iso(end - timedelta(seconds=step * (n - 1 - i)))


class OnsetRule(unittest.TestCase):
    def test_step_change_onset_is_where_it_starts(self):
        o = pc.onset_of(series([0.0] * 20 + [0.5] * 40), "error_ratio")
        self.assertEqual(o.at, at(20))

    def test_one_quiet_point_does_not_split_the_run_two_do(self):
        v = [10.0] * 20 + [100.0] * 40
        v[40] = v[41] = 10.0             # two quiet raw points: smoothing blurs one back, so one smoothed gap
        self.assertEqual(pc.onset_of(series(v), "p95_ms").at, at(20))
        v[42] = 10.0                     # three (~1.5 min of quiet): two smoothed gaps, the change restarts after
        self.assertEqual(pc.onset_of(series(v), "p95_ms").at, at(43))

    def test_no_current_change_has_no_onset(self):
        o = pc.onset_of(series([0.0] * 20 + [0.5] * 20 + [0.0] * 20), "error_ratio")
        self.assertIsNone(o.at)

    def test_reference_is_the_quiet_tail_even_when_faults_fill_most_of_the_window(self):
        # The previous scenario's residue (8 points), 6 quiet points, then this fault: quiet is only 10%.
        # A mean or median reference would sit inside the faults and find no change.
        v = [100.0] * 8 + [10.0] * 6 + [100.0] * 46
        o = pc.onset_of(series(v), "p95_ms")
        self.assertEqual(o.reference, 10.0)
        self.assertEqual(o.at, at(14))

    def test_traffic_drop_uses_the_high_tail(self):
        v = [10.0] * 30 + [1.0] * 30      # 20/min -> 2/min per 30 s step: 10 -> 1
        o = pc.onset_of(series(v), "calls", direction="down")
        self.assertEqual(o.reference, 10.0)
        # One point late: the smoothed step point is (10 + 1) / 2 = 5.5, not yet a third of 10 (the rule's resolution).
        self.assertEqual(o.at, at(31))

    def test_memory_onset_is_where_the_steady_climb_starts(self):
        flat = [300.0] * 20
        climb = [300.0 + 0.75 * i for i in range(1, 41)]   # 1.5 MiB/min at 30 s steps
        o = pc.onset_of(series(flat + climb), "memory_mib")
        got = [p["t"] for p in series(flat + climb)].index(o.at)
        self.assertIn(got, (19, 20), f"onset at point {got}, climb starts at 20")
        self.assertIsNone(pc.onset_of(series([300.0] * 60), "memory_mib").at)
        # A climb filling the whole window: the leak began at least that long ago.
        self.assertEqual(pc.onset_of(series([300.0 + 0.75 * i for i in range(60)]), "memory_mib").at, at(0))
        # Noise around a level is not a leak.
        self.assertIsNone(pc.onset_of(series([300.0 + (3 if i % 2 else -3) for i in range(60)]), "memory_mib").at)


class TrendRule(unittest.TestCase):
    def test_last_two_minutes_against_the_two_before(self):
        self.assertEqual(pc.trend_of(series([10.0] * 52 + [10.0] * 4 + [100.0] * 4), "p95_ms"), "worsening")
        self.assertEqual(pc.trend_of(series([100.0] * 60), "p95_ms"), "holding")
        self.assertEqual(pc.trend_of(series([100.0] * 56 + [10.0] * 4), "p95_ms"), "recovering")

    def test_noise_inside_the_threshold_is_holding(self):
        self.assertEqual(pc.trend_of(series([100.0, 130.0] * 30), "p95_ms"), "holding")

    def test_traffic_drop_directions(self):
        self.assertEqual(pc.trend_of(series([10.0] * 56 + [1.0] * 4), "calls", "down"), "worsening")
        self.assertEqual(pc.trend_of(series([1.0] * 56 + [10.0] * 4), "calls", "down"), "recovering")

    def test_memory_trend_by_slope(self):
        self.assertEqual(pc.trend_of(series([300.0 + 0.75 * i for i in range(60)]), "memory_mib"), "worsening")
        self.assertEqual(pc.trend_of(series([500.0] * 60), "memory_mib"), "holding")


class Durations(unittest.TestCase):
    def test_go_durations(self):
        self.assertEqual(pc.go_duration("2m0s"), timedelta(minutes=2))
        self.assertEqual(pc.go_duration("1h0m0s"), timedelta(hours=1))
        self.assertEqual(pc.go_duration("30s"), timedelta(seconds=30))


class GraphAPI(QueryAPI):
    """frontend -CLIENT GetProduct-> product-catalog (SERVER errors since point 30);
    email: memory climbing since point 26 (no call edge from frontend); ad: CPU up since point 10 but
    holding (an earlier incident); checkout: only failing client calls (a victim)."""

    def __init__(self, ts=None):
        super().__init__("http://fake", pc.iso(UNTIL))
        self.calls = []
        self.ts = ts or {}

    def _fetch(self, url):
        u = urllib.parse.urlparse(url)
        path, q = u.path, dict(urllib.parse.parse_qsl(u.query))
        self.calls.append((path, q))
        if path == "/v1/overview":
            return {"data": {"services": [
                {"service": "product-catalog", "changes": ["server error ratio 0.400 (was 0.000)"]},
                {"service": "frontend", "changes": ["warn/error log lines 30.0/min (was 0.1/min)"]},
                {"service": "email", "changes": ["memory rising 1.5 MiB/min (40% of limit)"]},
                {"service": "ad", "changes": ["CPU utilization 3.2 (was 0.4)"]},
                {"service": "checkout", "changes": []},
            ]}}
        if path == "/v1/spans":
            s = q["service"]
            rows = {
                "frontend": [{"kind": "CLIENT", "span": "oteldemo.ProductCatalogService/GetProduct", "callee": "product-catalog",
                              "error_ratio": {"window": 0.4, "baseline": 0.0}, "p95_ms": {"window": 5, "baseline": 5},
                              "calls_per_min": {"window": 30, "baseline": 30}},
                             {"kind": "SERVER", "span": "GET /api/products", "error_ratio": {"window": 0.0, "baseline": 0.0},
                              "p95_ms": {"window": 20, "baseline": 20}, "calls_per_min": {"window": 30, "baseline": 30}}],
                "product-catalog": [{"kind": "SERVER", "span": "oteldemo.ProductCatalogService/GetProduct",
                                     "error_ratio": {"window": 0.4, "baseline": 0.0}, "p95_ms": {"window": 3, "baseline": 3},
                                     "calls_per_min": {"window": 30, "baseline": 30}}],
            }.get(s, [])
            return {"data": rows}
        if path == "/v1/timeseries":
            key = (q["service"], q["signal"], q.get("span", "")) + ((q["kind"],) if q.get("kind", "server") != "server" else ())
            if key not in self.ts:
                raise APIError(f"no series {key}")
            return {"data": {"points": self.ts[key]}}
        raise APIError(f"unexpected {path}")


def graph_series():
    return {
        ("product-catalog", "error_ratio", ""): series([0.0] * 30 + [0.4] * 30),
        ("email", "memory_mib", ""): series([300.0] * 26 + [300.0 + 0.75 * i for i in range(1, 35)]),
        ("ad", "cpu", ""): series([0.4] * 10 + [3.2] * 50),
        ("frontend", "error_ratio", "GET /api/products"): series([0.0] * 60),
    }


ALERT = {"service": "frontend", "rule": "log_template_new", "opened_at": at(31), "window": "2m0s",
         "detail": f"first seen {at(30)}", "series": {"severity": "error", "template_id": "7"}}


class Walk(unittest.TestCase):
    def test_paged_victim_leads_to_its_failing_callee(self):
        r = pc.page_check(GraphAPI(graph_series()), ALERT, cap=20)
        self.assertEqual((r.paged, r.p_onset), ("frontend", at(30)))
        cat = r.get("product-catalog")
        self.assertEqual((cat.relation, cat.depth, cat.via), ("callee", 1, "frontend"))
        self.assertTrue(cat.own_change)
        self.assertEqual((cat.onset, cat.trend), (at(30), "holding"))
        self.assertEqual(r.get("frontend").own_change, [], "a log spike and failing client calls are victim signs, not own change")
        self.assertTrue(r.get("frontend").client_trouble)

    def test_changed_services_without_an_edge_are_still_candidates(self):
        r = pc.page_check(GraphAPI(graph_series()), ALERT, cap=20)
        self.assertEqual(r.get("email").relation, "changed (no edge)")
        self.assertEqual(r.get("email").trend, "worsening")
        self.assertIsNone(r.get("checkout"), "no own change and not reached: not a candidate")

    def test_the_cap_bounds_query_calls(self):
        api = GraphAPI(graph_series())
        r = pc.page_check(api, ALERT, cap=3)
        self.assertEqual((r.calls, len(api.calls), r.capped), (3, 3, True))
        self.assertIn("cap", r.table())

    def test_agent_only_walks_from_the_top_changed_services(self):
        r = pc.page_check(GraphAPI(graph_series()), None, cap=20)
        self.assertIsNone(r.paged)
        self.assertEqual({c.service for c in r.candidates if c.relation == "start"}, {"product-catalog", "frontend", "email", "ad"})


class POnset(unittest.TestCase):
    def onset(self, alert, ts=None):
        return pc.p_onset(pc.Budget(GraphAPI(ts or graph_series()), 5), alert)

    def test_by_rule(self):
        self.assertEqual(self.onset(ALERT)[0], at(30))
        spike = {**ALERT, "rule": "log_template_spike", "detail": ""}
        self.assertEqual(self.onset(spike)[0], pc.iso(pc.parse_ts(at(31)) - timedelta(minutes=2)))
        server = {"service": "product-catalog", "rule": "span_errors", "opened_at": at(32), "window": "2m0s",
                  "series": {"span.name": "GetProduct", "span.kind": "SERVER"}}
        ts = {("product-catalog", "error_ratio", "GetProduct"): series([0.0] * 29 + [0.4] * 31)}
        self.assertEqual(self.onset(server, ts)[0], at(29))
        mem = {"service": "email", "rule": "container_memory_growth", "opened_at": at(59), "window": "5m0s"}
        got = self.onset(mem)[0]
        self.assertLess(pc.parse_ts(got), pc.parse_ts(at(30)), "the climb's start, long before the alert")

    def test_client_span_pages_are_timed_from_the_client_series(self):
        client = {"service": "frontend", "rule": "span_errors", "opened_at": at(32), "window": "2m0s",
                  "series": {"span.name": "GetProduct", "span.kind": "CLIENT"}}
        ts = {("frontend", "error_ratio", "GetProduct", "client"): series([0.0] * 28 + [0.4] * 32)}
        t, how = self.onset(client, ts)
        self.assertEqual(t, at(28))
        self.assertIn("CLIENT", how)

    def test_other_span_kinds_fall_back_to_the_window_and_say_so(self):
        producer = {"service": "checkout", "rule": "span_errors", "opened_at": at(32), "window": "2m0s",
                    "series": {"span.name": "orders publish", "span.kind": "PRODUCER"}}
        t, how = self.onset(producer)
        self.assertEqual(t, pc.iso(pc.parse_ts(at(32)) - timedelta(minutes=2)))
        self.assertIn("coarse", how)


class F4(unittest.TestCase):
    def setUp(self):
        self.r = pc.page_check(GraphAPI(graph_series()), ALERT, cap=20)

    def test_paged_or_unsure_always_allowed(self):
        self.assertTrue(pc.reassignment("frontend", self.r)[0])
        self.assertTrue(pc.reassignment(None, self.r)[0])

    def test_callee_with_own_change_is_allowed(self):
        ok, why = pc.reassignment("product-catalog", self.r)
        self.assertTrue(ok, why)
        self.assertIn("reached from frontend", why)

    def test_no_edge_is_allowed_on_own_signal(self):
        # The slow leak began 2 min before the page's onset here, and is still worsening.
        ok, why = pc.reassignment("email", self.r)
        self.assertTrue(ok, why)
        self.assertIn("no call edge", why)

    def test_slow_leak_far_earlier_but_worsening_is_allowed(self):
        ts = graph_series()
        ts[("email", "memory_mib", "")] = series([300.0] * 4 + [300.0 + 0.75 * i for i in range(1, 57)])  # ~27 min early
        r = pc.page_check(GraphAPI(ts), ALERT, cap=20)
        self.assertTrue(pc.reassignment("email", r)[0])

    def test_earlier_incident_is_refused(self):
        ok, why = pc.reassignment("ad", self.r)          # CPU up 10 min before the page's onset, holding
        self.assertFalse(ok)
        self.assertIn("earlier incident", why)

    def test_victim_without_own_change_is_refused(self):
        r = pc.page_check(GraphAPI(graph_series()), {**ALERT, "service": "product-catalog", "rule": "log_template_new"}, cap=20)
        ok, why = pc.reassignment("frontend", r)
        self.assertFalse(ok)
        self.assertIn("own signals", why)

    def test_victim_in_the_table_without_own_change_is_refused(self):
        # checkout is reached from the page by a call edge and its calls fail, but nothing of its own changed.
        table = pc.PageCheck(paged="frontend", p_rule="span_errors", p_onset=at(30), p_onset_how="", calls=4, cap=8, capped=False,
                             candidates=[pc.Candidate("frontend", "paged"),
                                         pc.Candidate("checkout", "callee", 1, "frontend", client_trouble=["CLIENT Charge error ratio 0.5"]),
                                         pc.Candidate("payment", "callee", 2, "checkout", own_change=["SERVER Charge error ratio 0.5"],
                                                      signal="error_ratio", onset=at(30), trend="holding")])
        ok, why = pc.reassignment("checkout", table)
        self.assertFalse(ok)
        self.assertIn("own signals", why)
        self.assertTrue(pc.reassignment("payment", table)[0])

    def test_agent_only_has_no_rule(self):
        r = pc.page_check(GraphAPI(graph_series()), None, cap=20)
        self.assertTrue(pc.reassignment("ad", r)[0])


class RuleCheck(unittest.TestCase):
    """rulecheck end to end over HTTP: a local server answers like the query API (the fake call graph)."""

    def setUp(self):
        import json as _json
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        graph = GraphAPI(graph_series())

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    body, code = graph._fetch("http://fake" + self.path), 200
                except APIError as e:
                    body, code = {"error": str(e)}, 400
                data = _json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.srv.server_address[1]}"

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def test_ceiling_row_and_what_f4_would_block(self):
        import json as _json
        import tempfile
        from drishti_agent.evals import rulecheck
        from drishti_agent.evals.scenarios import Scenario
        sc = Scenario(n=9, flag="productCatalogFailure", variant="on", on=pc.parse_ts(at(29)), off=UNTIL, slept=False)
        sc.gate_passed, sc.trigger_alert = True, ALERT
        sc.triggers = {"agent-only": UNTIL, "end-to-end": pc.parse_ts(ALERT["opened_at"])}
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            for named in ("ad", "checkout", "email"):
                f.write(_json.dumps({"system": "agent", "column": "end-to-end", "flag": "productCatalogFailure",
                                     "score": {"top1": False, "named": named}}) + "\n")
            f.write(_json.dumps({"system": "agent", "column": "end-to-end", "flag": "productCatalogFailure",
                                 "score": {"top1": True, "named": "product-catalog"}}) + "\n")
        wrong = rulecheck.v0_wrong_answers([f.name])
        self.assertEqual(wrong, {"productCatalogFailure": ["ad", "checkout", "email"]})
        r = rulecheck.check_scenario(self.base, sc, wrong, cap=20)
        e = r["end_to_end"]
        self.assertTrue(e["f4_accepts_label"], e["f4_why"])
        self.assertEqual((e["labelled_capped"]["relation"], e["labelled_capped"]["depth"]), ("callee", 1))
        self.assertEqual(e["labelled_onset_after_flag_s"], 30)     # onset at point 30, flag on at point 29
        self.assertEqual(e["p_onset_after_flag_s"], 30)
        self.assertEqual({b["named"]: b["f4_accepts"] for b in e["v0_wrong"]}, {"ad": False, "checkout": False, "email": True})
        md = rulecheck.summary([r], "fake", 20)
        self.assertIn("F4 accepts the labelled service in 1/1", md)
        self.assertIn("F4 would refuse 2", md)
