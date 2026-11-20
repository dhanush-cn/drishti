"""Remediation: propose, approve, execute, verify (patch 65). No network: fake flag API, local query API."""
import json
import threading
import unittest
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from langgraph.checkpoint.memory import InMemorySaver

from drishti_agent import durable, remediate
from drishti_agent.llm import Scripted

from test_graph import API, call, hyps, mark
from test_pagecheck import ALERT, graph_series, series


class FakeFlags:
    """The demo's flag file, in memory, shaped like /feature/api/read."""

    def __init__(self, on=()):
        flags = {n: {"variants": {"on": True, "off": False, "50%": True}, "defaultVariant": "off"}
                 for n in remediate.LABELS}
        flags["productCatalogFailure"] = {"variants": {"on": True, "off": False}, "defaultVariant": "off",
                                          "targeting": {"if": [{"==": [1, 1]}, "off", "off"]}}
        for n in on:
            if n == "productCatalogFailure":
                flags[n]["targeting"]["if"][1] = "on"
            else:
                flags[n]["defaultVariant"] = "on"
        self.body, self.writes = {"flags": flags}, 0

    def read(self):
        return json.loads(json.dumps(self.body))

    def write(self, body):
        self.writes += 1
        self.body = json.loads(json.dumps(body))

    def state(self, name):
        f = self.body["flags"][name]
        return f["targeting"]["if"][1] if name == "productCatalogFailure" else f["defaultVariant"]


APPROVED = {"approve": True, "by": "dhanush", "at": "2026-11-20T15:00:00Z"}


class Executor(unittest.TestCase):
    def test_owned_flags_come_from_the_labels(self):
        self.assertEqual(remediate.owned_flags("ad"), ["adFailure", "adHighCpu", "adManualGc"])
        self.assertEqual(remediate.owned_flags("checkout"), ["paymentUnreachable"])
        self.assertEqual(remediate.owned_flags("currency"), [])

    def test_propose_only_for_a_named_service(self):
        self.assertEqual(remediate.propose({"root_cause_service": "cart", "fault_type": "error", "confidence": 0.9})["action"],
                         "rollback_config")
        self.assertIsNone(remediate.propose({"root_cause_service": None}))

    def test_refuses_outside_the_allowlist_or_without_an_approval_record(self):
        f = FakeFlags(on=["cartFailure"])
        self.assertIn("not allowlisted", remediate.execute({"action": "restart", "service": "cart"}, APPROVED, f)["refused"])
        self.assertIn("approval", remediate.execute({"action": "rollback_config", "service": "cart"}, {"approve": True}, f)["refused"])
        self.assertIn("approval", remediate.execute({"action": "rollback_config", "service": "cart"}, {"approve": False, "by": "x"}, f)["refused"])
        self.assertEqual((f.writes, f.state("cartFailure")), (0, "on"))

    def test_rolls_back_only_the_services_flags_and_is_idempotent(self):
        f = FakeFlags(on=["adHighCpu", "cartFailure", "productCatalogFailure"])
        r = remediate.execute({"action": "rollback_config", "service": "ad"}, APPROVED, f, "t1")
        self.assertEqual((r["ok"], r["flags_turned_off"]), (True, ["adHighCpu"]))
        self.assertEqual((f.state("adHighCpu"), f.state("cartFailure")), ("off", "on"))
        again = remediate.execute({"action": "rollback_config", "service": "ad"}, APPROVED, f, "t1")
        self.assertEqual((again["changed"], f.writes, again["action_id"]), (0, 1, r["action_id"]))
        pc = remediate.execute({"action": "rollback_config", "service": "product-catalog"}, APPROVED, f)
        self.assertIn("productCatalogFailure", pc["flags_turned_off"])
        self.assertEqual(f.state("productCatalogFailure"), "off")

    def test_a_service_with_no_known_config_changes_nothing(self):
        f = FakeFlags(on=["cartFailure"])
        r = remediate.execute({"action": "rollback_config", "service": "currency"}, APPROVED, f)
        self.assertEqual((r["ok"], r["changed"], f.writes), (True, 0, 0))
        self.assertIn("nothing rolled back", r["note"])


class LiveAPI:
    """A local query API whose product-catalog error ratio stays high for `bad` checks, then recovers."""

    def __init__(self, bad: int, flap_at=None, memory=False):
        self.n = 0
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                q = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(self.path).query))
                outer.n += 1
                until = datetime.fromisoformat(q["until"].replace("Z", "+00:00"))
                if q["signal"] == "memory_mib":
                    pts = series([300.0 + (0.75 * i if outer.n <= bad else 0.0) for i in range(10)], end=until)
                else:
                    high = outer.n <= bad or outer.n == flap_at
                    pts = series([0.4 if high else 0.0] * 10, end=until)
                body = json.dumps({"data": {"points": pts}}).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"


PC = {"candidates": [{"service": "product-catalog", "own_change": ["server error ratio 0.4"], "signal": "error_ratio", "reference": 0.0},
                     {"service": "email", "own_change": ["memory rising"], "signal": "memory_mib", "reference": None},
                     {"service": "frontend", "own_change": [], "signal": None}]}


def verifier(api, **kw):
    clock = {"t": datetime(2026, 11, 20, 15, 0, tzinfo=timezone.utc)}
    def sleep(s):
        clock["t"] += timedelta(seconds=s)
    return remediate.Verifier(api.url, now=lambda: clock["t"], sleep=sleep, **kw)


class Verify(unittest.TestCase):
    def test_recovered_after_two_minutes_back_within_the_threshold(self):
        api = LiveAPI(bad=3)
        r = verifier(api).verify(PC, "product-catalog")
        self.assertTrue(r["recovered"])
        self.assertEqual(r["checks"], 3 + 4, "3 bad checks, then 4 good in a row (2 minutes at 30 s)")

    def test_a_relapse_restarts_the_count(self):
        api = LiveAPI(bad=1, flap_at=4)
        r = verifier(api).verify(PC, "product-catalog")
        self.assertTrue(r["recovered"])
        self.assertEqual(r["checks"], 4 + 4)

    def test_not_recovered_within_the_timeout(self):
        r = verifier(LiveAPI(bad=10_000), timeout=300).verify(PC, "product-catalog")
        self.assertFalse(r["recovered"])
        self.assertEqual(r["checks"], 11, "every 30 s for 300 s")

    def test_memory_recovers_when_the_climb_stops(self):
        r = verifier(LiveAPI(bad=2, memory=True)).verify(PC, "email")
        self.assertTrue(r["recovered"])

    def test_nothing_to_verify_against_is_said_not_guessed(self):
        r = verifier(LiveAPI(bad=0)).verify(PC, "frontend")
        self.assertIsNone(r["recovered"])


FLOW = lambda: [hyps(("product-catalog", "error", 0.6)), call("c1", "spans", service="product-catalog"),
                mark("supported", 0.85, ["t1"])]


class StubVerifier:
    def __init__(self):
        self.calls = 0

    def verify(self, pc, service):
        self.calls += 1
        return {"recovered": True, "service": service, "checks": 4}


class Flow(unittest.TestCase):
    def setUp(self):
        self.flags, self.ver = FakeFlags(on=["productCatalogFailure"]), StubVerifier()
        self.rem = remediate.Remediation(self.flags, self.ver)
        self.saver = InMemorySaver()

    def start(self, tid, replies):
        return list(durable.stream(API(graph_series()), Scripted(replies), self.saver, ALERT, thread_id=tid, remediation=self.rem))

    def test_pauses_for_a_human_then_executes_and_verifies(self):
        evs = self.start("r1", FLOW())
        self.assertEqual([e["type"] for e in evs][-4:], ["diagnosis", "proposal", "awaiting_approval", "end"])
        self.assertEqual(evs[-2]["proposal"]["service"], "product-catalog")
        self.assertEqual((self.flags.writes, self.ver.calls), (0, 0), "nothing happens before approval")
        # Asking again while it waits changes nothing.
        again = list(durable.stream(API(graph_series()), Scripted([]), self.saver, ALERT, thread_id="r1", remediation=self.rem))
        self.assertEqual([e["type"] for e in again], ["start", "awaiting_approval", "end"])
        out = list(durable.decide(API(graph_series()), Scripted([]), self.saver, "r1", True, "dhanush", self.rem))
        types = [e["type"] for e in out]
        self.assertEqual(types[-4:], ["decision", "action", "recovery", "end"])
        by = {e["type"]: e for e in out}
        self.assertEqual(by["decision"]["approval"]["by"], "dhanush")
        self.assertEqual(by["action"]["action"]["flags_turned_off"], ["productCatalogFailure"])
        self.assertTrue(by["recovery"]["recovery"]["recovered"])
        self.assertEqual(self.flags.state("productCatalogFailure"), "off")

    def test_rejected_closes_without_touching_anything(self):
        self.start("r2", FLOW())
        out = list(durable.decide(API(graph_series()), Scripted([]), self.saver, "r2", False, "dhanush", self.rem))
        self.assertEqual([e["type"] for e in out][-2:], ["decision", "end"])
        self.assertEqual((self.flags.writes, self.ver.calls), (0, 0))
        self.assertEqual(self.flags.state("productCatalogFailure"), "on")

    def test_a_decision_needs_who_made_it(self):
        self.start("r3", FLOW())
        with self.assertRaises(ValueError):
            list(durable.decide(API(graph_series()), Scripted([]), self.saver, "r3", True, "  ", self.rem))

    def test_unsure_proposes_nothing(self):
        evs = self.start("r4", [hyps()])
        self.assertNotIn("awaiting_approval", [e["type"] for e in evs])
        self.assertEqual(self.flags.writes, 0)

    def test_crash_during_execute_resumes_without_a_second_change(self):
        self.start("r5", FLOW())
        with self.assertRaises(durable.Crash):
            list(durable.decide(API(graph_series()), Scripted([]), self.saver, "r5", True, "dhanush", self.rem,
                                crash="execute:1", crash_mode="raise"))
        self.assertEqual((self.flags.writes, self.flags.state("productCatalogFailure")), (1, "off"), "the change happened")
        out = list(durable.stream(API(graph_series()), Scripted([]), self.saver, ALERT, thread_id="r5", remediation=self.rem, crash=""))
        by = {e["type"]: e for e in out}
        self.assertEqual(by["action"]["action"]["changed"], 0, "re-run found it already rolled back")
        self.assertEqual(self.flags.writes, 1, "no second write")
        self.assertTrue(by["recovery"]["recovery"]["recovered"])
