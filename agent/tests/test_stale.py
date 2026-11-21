"""G3', the stale-change guard, and the Week 7 ceiling check (patch 75). No network, no model.

The fake world mirrors capture 2's productCatalogLockContention miss: the page is frontend's span_slow seconds after
product-catalog slowed down; ad's p95 spiked minutes earlier (the previous scenario) and is back to normal; email leaks
memory steadily (a live change that started long before); payment never changed."""
import json
import random
import tempfile
import threading
import unittest
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from drishti_agent import pagecheck as pc
from drishti_agent.api import APIError, QueryAPI

T = datetime(2026, 11, 3, 17, 35, 10, tzinfo=timezone.utc)   # flag on
TRIGGER = T + timedelta(seconds=40)                           # the page
OFF = T + timedelta(minutes=4)


def series(vals, end, step=30):
    n = len(vals)
    return [{"t": pc.iso(end - timedelta(seconds=step * (n - 1 - i))), "v": v} for i, v in enumerate(vals)]


def value(service, signal, t, span="", kind="server"):
    if service == "cart" and signal == "p95_ms":       # an old spike, over...
        return 400.0 if T - timedelta(minutes=8) <= t <= T - timedelta(minutes=4) else 3.0
    if service == "cart" and signal == "memory_mib":   # ...and a leak, live
        return 200.0 + max(0.0, (t - (T - timedelta(minutes=20))).total_seconds() / 60) * 1.5
    if service == "ad" and signal == "p95_ms":
        return 400.0 if T - timedelta(minutes=8) <= t <= T - timedelta(minutes=4) else 3.0
    if service == "product-catalog" and signal == "p95_ms":
        return 15000.0 if t >= T + timedelta(seconds=20) else 2.0
    if service == "frontend" and signal == "p95_ms":
        return 90.0 if t >= T + timedelta(seconds=20) else 40.0
    if service == "email" and signal == "memory_mib":
        return 200.0 + max(0.0, (t - (T - timedelta(minutes=20))).total_seconds() / 60) * 1.5
    if signal == "memory_mib":
        return 100.0
    if signal == "cpu":
        return 0.4
    if signal == "calls":
        return 30.0
    if signal == "error_ratio":
        return 0.0
    return 3.0


class World(QueryAPI):
    """Answers like the query API as of `until`: 30-minute timeseries at 30 s, and an overview/spans that show
    product-catalog's slowdown only once a minute of it is in the 2-minute window."""

    def __init__(self, until):
        super().__init__("http://fake", pc.iso(until))
        self.until = until

    def _fetch(self, url):
        u = urllib.parse.urlparse(url)
        q = dict(urllib.parse.parse_qsl(u.query))
        until = pc.parse_ts(q.get("until", pc.iso(self.until)))
        slow = until >= T + timedelta(seconds=80)
        calls_slow = until >= T + timedelta(seconds=20)   # the caller feels it first
        if u.path == "/v1/overview":
            rows = [{"service": "email", "changes": ["memory rising 1.5 MiB/min (40% of limit)"]},
                    {"service": "frontend", "changes": []}, {"service": "ad", "changes": []}, {"service": "payment", "changes": []}]
            if slow:
                rows.insert(0, {"service": "product-catalog", "changes": ["server p95 15000 ms (was 2 ms)"]})
            return {"data": {"services": rows}}
        if u.path == "/v1/spans":
            if q["service"] == "frontend":
                return {"data": [{"kind": "CLIENT", "span": "oteldemo.ProductCatalogService/GetProduct", "callee": "product-catalog",
                                  "error_ratio": {"window": 0, "baseline": 0}, "p95_ms": {"window": 9000 if calls_slow else 2, "baseline": 2},
                                  "calls_per_min": {"window": 30, "baseline": 30}}]}
            if q["service"] == "product-catalog":
                return {"data": [{"kind": "SERVER", "span": "oteldemo.ProductCatalogService/GetProduct",
                                  "error_ratio": {"window": 0, "baseline": 0}, "p95_ms": {"window": 9000 if slow else 2, "baseline": 2},
                                  "calls_per_min": {"window": 30, "baseline": 30}}]}
            return {"data": []}
        if u.path == "/v1/timeseries":
            if q["service"] == "nowhere":
                raise APIError("unknown service")
            n = 60
            pts = [value(q["service"], q["signal"], until - timedelta(seconds=30 * (n - 1 - i))) for i in range(n)]
            return {"data": {"points": series(pts, until)}}
        raise APIError(f"unexpected {u.path}")


ALERT = {"service": "frontend", "rule": "span_slow", "opened_at": pc.iso(TRIGGER), "window": "30s",
         "series": {"span.kind": "SERVER", "span.name": "GET"}, "id": "a7"}


class Runs(unittest.TestCase):
    def test_the_run_reaching_the_newest_point_starts_at_onset_of(self):
        rnd = random.Random(7)
        for _ in range(300):
            vals = [rnd.choice([2.0, 2.0, 2.0, 60.0, 80.0]) for _ in range(40)]
            pts = series(vals, T)
            runs, closed = pc.runs_of(pts, "p95_ms")
            o = pc.onset_of(pts, "p95_ms")
            reaching = runs and runs[-1][1] == len(pts) - 1
            self.assertEqual(bool(o.at), bool(reaching), vals)
            if o.at:
                self.assertEqual(o.at, pts[runs[-1][0]]["t"], vals)
                self.assertFalse(closed)

    def test_one_quiet_point_does_not_end_a_run_two_do(self):
        pts = series([2.0] * 10 + [100.0] * 5 + [2.0] + [100.0] * 3 + [2.0] * 3 + [2.0] * 5, T)
        runs, closed = pc.runs_of(pts, "p95_ms")
        self.assertEqual(len(runs), 1)
        self.assertTrue(closed)


class SignalStates(unittest.TestCase):
    P = T

    def state(self, vals, sig="p95_ms", after=1):
        """60 points at 30 s ending `after` minutes past the page's onset P."""
        return pc.signal_state(series(vals, self.P + timedelta(minutes=after)), sig, "up", self.P)

    def test_ended_before_the_page_is_over(self):
        s = self.state([3.0] * 20 + [400.0] * 10 + [3.0] * 30)
        self.assertEqual(s.state, "over (ended)")

    def test_ended_after_the_page_counts_neither_way(self):
        s = self.state([3.0] * 40 + [400.0] * 16 + [3.0] * 4, after=3)   # last past point 1.5 min after P
        self.assertEqual(s.state, "other")
        self.assertIn("after the page", s.note)

    def test_still_changing_is_live(self):
        self.assertEqual(self.state([3.0] * 50 + [400.0] * 10).state, "live")

    def test_recovering_since_before_the_page_is_over(self):
        s = self.state([3.0] * 20 + [800.0] * 36 + [100.0] * 4)   # still past (2x, +20 ms) but falling hard
        self.assertEqual((s.state, s.trend), ("over (recovering)", "recovering"))

    def test_fading_with_one_quiet_point_counts_neither_way(self):
        s = self.state([3.0] * 20 + [400.0] * 38 + [3.0] * 2)
        self.assertEqual(s.state, "other")
        self.assertIn("fading", s.note)

    def test_no_run_is_none(self):
        self.assertEqual(self.state([3.0] * 60).state, "none")

    def test_a_leak_still_climbing_is_live(self):
        s = self.state([200.0 + 0.75 * i for i in range(60)], "memory_mib")
        self.assertEqual(s.state, "live")


class StaleGuard(unittest.TestCase):
    def check(self, svc):
        p = pc.page_check(World(TRIGGER), ALERT, cap=20)
        return pc.stale_change(World(TRIGGER), svc, p.paged, p.p_onset), p

    def test_a_change_over_before_the_page_is_stale(self):
        st, p = self.check("ad")
        self.assertTrue(st.applies and st.stale, st.why)
        self.assertIn("p95_ms", st.why)
        self.assertEqual(st.calls, 5, "5 series: calls up and down share one")

    def test_the_root_cause_changing_now_is_not_stale(self):
        st, _ = self.check("product-catalog")
        self.assertFalse(st.stale, st.why)
        self.assertIn("live", st.why)

    def test_no_change_at_all_is_not_stale(self):
        st, _ = self.check("payment")
        self.assertTrue(st.applies)
        self.assertFalse(st.stale, st.why)

    def test_an_old_but_live_leak_is_not_stale(self):
        st, _ = self.check("email")
        self.assertFalse(st.stale, st.why)

    def test_one_live_signal_outweighs_an_over_one(self):
        st, _ = self.check("cart")
        self.assertEqual(sorted({x.state for x in st.signals} - {"none"}), ["live", "over (ended)"])
        self.assertFalse(st.stale, st.why)

    def test_the_paged_service_and_unknown_onset_are_not_checked(self):
        st, p = self.check("frontend")
        self.assertFalse(st.applies)
        self.assertFalse(pc.stale_change(World(TRIGGER), "ad", "frontend", None).applies)

    def test_a_signal_that_cannot_be_read_counts_neither_way(self):
        st, p = self.check("nowhere")
        self.assertFalse(st.stale)
        self.assertTrue(all(s.state == "other" for s in st.signals))


class Server:
    def __init__(self):
        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    body, code = World(TRIGGER)._fetch("http://fake" + self.path), 200
                except APIError as e:
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


class CeilingCheck(unittest.TestCase):
    """rulecheck's Week 7 part end to end over HTTP: recheck rows, the guard on the label, catches from a results file."""

    def setUp(self):
        self.s = Server()

    def tearDown(self):
        self.s.srv.shutdown()

    def scenario(self):
        from drishti_agent.evals.scenarios import Scenario
        sc = Scenario(n=7, flag="productCatalogLockContention", variant="on", on=T, off=OFF, slept=False)
        sc.gate_passed, sc.trigger_alert = True, ALERT
        sc.triggers = {"agent-only": OFF, "end-to-end": TRIGGER}
        return sc

    def results_file(self):
        d = tempfile.mkdtemp()
        import os
        os.makedirs(f"{d}/drishti-week6/results/capture-2/graph-hosted")
        p = f"{d}/drishti-week6/results/capture-2/graph-hosted/runs.jsonl"
        row = lambda system, named, conf, cw, top1=False, **k: {"system": system, "column": "end-to-end", "flag": "productCatalogLockContention",
                                                                 "run": 0, "until": pc.iso(TRIGGER), **k,
                                                                 "score": {"top1": top1, "named": named, "confidence": conf, "confidently_wrong": cw}}
        with open(p, "w") as f:
            for r in (row("graph", "ad", 0.9, True), row("graph", "payment", 0.5, False), row("graph", "frontend", 0.9, True),
                      row("graph", "product-catalog", 0.9, False, top1=True), row("baseline", "ad", 0.9, True),
                      row("graph", "ad", 0.9, True, skipped=True), {**row("graph", "ad", 0.9, True), "column": "agent-only"}):
                f.write(json.dumps(r) + "\n")
        return p

    def test_week7_row_and_summary(self):
        from drishti_agent.evals import rulecheck
        path = self.results_file()
        wrong = rulecheck.wrong_answers([path])
        self.assertEqual([(w["named"], w["confidently_wrong"], w["source"]) for w in wrong["productCatalogLockContention"]],
                         [("ad", True, "week6:capture-2/graph-hosted"), ("payment", False, "week6:capture-2/graph-hosted"),
                          ("frontend", True, "week6:capture-2/graph-hosted")],
                         "committed wrong end-to-end answers only: no right answer, baseline, skipped or agent-only row")
        sc = self.scenario()
        w7 = rulecheck.check_week7(self.s.url, sc, wrong, cap=20, recheck=[1, 2, 3, 4], stale=True, gate_minutes=2)
        rc = w7["recheck"]
        self.assertEqual(rc["0"]["labelled"]["relation"], "callee", "reached from frontend's slow client calls...")
        self.assertFalse(rc["0"]["own_change"], "...but its own slowdown isn't in the 2-minute window yet")
        self.assertTrue(rc["2"]["own_change"])
        self.assertEqual(rc["2"]["labelled"]["relation"], "callee")
        self.assertTrue(rc["3"]["before_off"])
        self.assertFalse(rc["4"]["before_off"], "the trigger + 4 min is past flag-off")
        self.assertEqual(w7["label_rejected"], [], [c["why"] for c in w7["label_stale"]])
        self.assertEqual(sorted(c["minutes"] for c in w7["label_stale"]), [0, 2])
        verdicts = {w["named"]: (w["applies"], w["stale"]) for w in w7["wrong"]}
        self.assertEqual(verdicts, {"ad": (True, True), "payment": (True, False), "frontend": (False, False)})
        md = rulecheck.week7_summary([{"n": 7, "flag": sc.flag, "week7": w7}], "fake", [1, 2, 3, 4], True, 2)
        self.assertIn("0 rejected -> PASS", md)
        self.assertIn("| week6:capture-2/graph-hosted | 1 / 2 | 0 / 1 | 1 |", md)
        self.assertIn("callee, own change", md)
        self.assertIn("callee, no own change", md)
        self.assertIn("(after off)", md)

    def test_a_rejected_label_fails_the_check(self):
        from drishti_agent.evals import rulecheck
        from drishti_agent.evals.labels import LABELS, Label
        sc = self.scenario()
        sc.flag = "fakeStaleLabel"
        LABELS["fakeStaleLabel"] = Label(services=frozenset({"ad"}), faults=frozenset({"latency"}))
        try:
            w7 = rulecheck.check_week7(self.s.url, sc, {}, cap=20, recheck=[], stale=True, gate_minutes=2)
        finally:
            del LABELS["fakeStaleLabel"]
        self.assertEqual(len(w7["label_rejected"]), 2, "rejected at the trigger and at +2 min")
        md = rulecheck.week7_summary([{"n": 7, "flag": sc.flag, "week7": w7}], "fake", [], True, 2)
        self.assertIn("2 rejected -> FAIL", md)
        self.assertIn("REJECTED: #7 fakeStaleLabel ad", md)


if __name__ == "__main__":
    unittest.main()
