"""The agent service (patch 66), on the real Postgres: needs DRISHTI_TEST_POSTGRES_URL."""
import json
import os
import tempfile
import time
import unittest
import uuid

from drishti_agent import durable, service

from test_durable import ByContent
from test_graph import API
from test_pagecheck import ALERT, graph_series
from test_remediate import FakeFlags, StubVerifier

PG = os.environ.get("DRISHTI_TEST_POSTGRES_URL", "")


def deps_events(deps, id):
    """The stored events of one run, straight from the agent's Postgres."""
    return service.Store(deps.pool).events(id)


class AlertAPI(API):
    """The fake query API, with one alert under a test's own id."""

    aid = "a1"

    def _fetch(self, url):
        if "/v1/anomalies" in url:
            return {"data": [{**ALERT, "id": AlertAPI.aid, "state": "open"}]}
        return super()._fetch(url)


@unittest.skipUnless(PG, "DRISHTI_TEST_POSTGRES_URL not set")
class Service(unittest.TestCase):
    def setUp(self):
        from fastapi.testclient import TestClient
        AlertAPI.aid = f"anom-{uuid.uuid4().hex[:10]}"
        self.flags, self.ver, self.log = FakeFlags(on=["productCatalogFailure"]), StubVerifier(), tempfile.mktemp()
        self.app, self.deps = self.make()
        self.c = TestClient(self.app)

    def make(self):
        cfg = service.Config(postgres=PG, queryapi="http://fake")
        deps = service.Deps(cfg=cfg, llm=ByContent(self.log), flags=self.flags, verifier=self.ver,
                            api_factory=lambda base, until: AlertAPI(graph_series()))
        return service.create_app(deps), deps

    def wait(self, id, status, client=None, timeout=20):
        client = client or self.c
        t0 = time.time()
        while time.time() - t0 < timeout:
            v = client.get(f"/v1/investigations/{id}").json()
            if v["status"] == status:
                return v
            time.sleep(0.1)
        self.fail(f"{id} never reached {status}: {v['status']}")

    def sse(self, id, after=0):
        with self.c.stream("GET", f"/v1/investigations/{id}/events?after={after}") as r:
            body = "".join(r.iter_text())
        evs = []
        for block in body.strip().split("\n\n"):
            lines = dict(l.split(": ", 1) for l in block.splitlines() if ": " in l and not l.startswith(":"))
            if "event" in lines:
                evs.append((lines["event"], json.loads(lines["data"])))
        return evs

    def test_start_watch_approve_verify_and_remember(self):
        r = self.c.post("/v1/investigations", json={"anomaly_id": AlertAPI.aid})
        self.assertEqual(r.status_code, 201)
        id = r.json()["id"]
        self.assertEqual(self.c.post("/v1/investigations", json={"anomaly_id": AlertAPI.aid}).json()["id"], id, "one per alert")
        v = self.wait(id, "awaiting_approval")
        self.assertEqual((v["diagnosis"]["root_cause_service"], v["proposal"]["service"]), ("product-catalog", "product-catalog"))
        evs = self.sse(id)
        self.assertEqual([t for t, _ in evs][-3:], ["awaiting_approval", "end", "done"])
        self.assertEqual(self.flags.writes, 0, "nothing executed before approval")
        self.assertEqual(self.c.post(f"/v1/investigations/{id}/decision", json={"approve": True, "by": " "}).status_code, 400)
        self.assertEqual(self.c.post(f"/v1/investigations/{id}/decision", json={"approve": True, "by": "dhanush"}).status_code, 200)
        self.assertEqual(self.c.post(f"/v1/investigations/{id}/decision", json={"approve": True, "by": "dhanush"}).status_code, 409,
                         "a second click is not a second decision")
        v = self.wait(id, "finished")
        self.assertEqual((v["approval"]["by"], v["action"]["flags_turned_off"], v["recovery"]["recovered"]),
                         ("dhanush", ["productCatalogFailure"], True))
        after = self.sse(id, after=len(evs) - 1)
        self.assertIn("recovery", [t for t, _ in after])
        # This run's incident, by its id: similar() returns the top 2, and a Postgres that keeps earlier test runs holds
        # other incidents with the same signature (the first version of this check failed on a second run).
        import psycopg
        with psycopg.connect(PG) as conn:
            row = conn.execute("SELECT outcome, root_cause, outcome_by FROM incidents WHERE id = %s", (id,)).fetchone()
        self.assertEqual(row, ("confirmed", "product-catalog", "recovery verified"))

    def test_rejected_and_not_waiting(self):
        id = self.c.post("/v1/investigations", json={"anomaly_id": AlertAPI.aid}).json()["id"]
        self.wait(id, "awaiting_approval")
        self.assertEqual(self.c.post(f"/v1/investigations/{id}/decision", json={"approve": False, "by": "dhanush"}).status_code, 200)
        v = self.wait(id, "finished")
        self.assertEqual((v["approval"]["approve"], self.flags.writes), (False, 0))
        self.assertEqual(self.c.post(f"/v1/investigations/{id}/decision", json={"approve": True, "by": "x"}).status_code, 409)

    def test_a_setup_failure_is_recorded_not_left_running(self):
        """Patch 91: the run's setup (query API, recheck settings) is inside the try. On 26 Nov a NameError there killed
        the thread before it, and the run stayed "running" forever with no event saying why."""
        from fastapi.testclient import TestClient
        calls = []

        def factory(base, until):          # the first call is create() looking up the alert; the second is the run's
            calls.append(until)
            if len(calls) > 1:
                raise RuntimeError("setup broke")
            return AlertAPI(graph_series())
        deps = service.Deps(cfg=service.Config(postgres=PG, queryapi="http://fake"), llm=ByContent(self.log),
                            flags=self.flags, verifier=self.ver, api_factory=factory)
        c = TestClient(service.create_app(deps))
        id = c.post("/v1/investigations", json={"anomaly_id": AlertAPI.aid}).json()["id"]
        self.wait(id, "failed", client=c)
        self.assertTrue(any(e.get("type") == "failed" and "setup broke" in e.get("error", "")
                            for _, e in deps_events(deps, id)))

    def test_unknown_alert(self):
        self.assertEqual(self.c.post("/v1/investigations", json={"anomaly_id": "nope"}).status_code, 404)

    def test_a_restart_while_waiting_keeps_the_approval(self):
        from fastapi.testclient import TestClient
        id = self.c.post("/v1/investigations", json={"anomaly_id": AlertAPI.aid}).json()["id"]
        self.wait(id, "awaiting_approval")
        app2, _ = self.make()                      # a new process, same Postgres
        c2 = TestClient(app2)
        self.assertEqual(c2.post(f"/v1/investigations/{id}/decision", json={"approve": True, "by": "dhanush"}).status_code, 200)
        v = self.wait(id, "finished", client=c2)
        self.assertTrue(v["recovery"]["recovered"])

    def test_a_run_killed_midway_resumes_when_the_service_starts(self):
        """A process killed during a run leaves status 'running' and a checkpoint; the next start resumes it."""
        from fastapi.testclient import TestClient
        aid = AlertAPI.aid
        id = service.thread_for(aid)
        store = service.Store(self.deps.pool)
        alert = {**ALERT, "id": aid}
        store.create(id, aid, alert, "2026-11-03T17:11:00Z")
        with self.assertRaises(durable.Crash):
            list(durable.stream(AlertAPI(graph_series()), ByContent(self.log), self.deps.checkpointer, alert, thread_id=id,
                                crash="test:1", crash_mode="raise"))
        app2, _ = self.make()                      # startup finds it 'running' and resumes from the checkpoint
        v = self.wait(id, "awaiting_approval", client=TestClient(app2))
        self.assertEqual(v["proposal"]["service"], "product-catalog")

    def test_the_page(self):
        html = self.c.get(f"/ui/anomaly/{AlertAPI.aid}").text
        self.assertIn(json.dumps(AlertAPI.aid), html)
        self.assertNotIn("innerHTML", html, "event text comes from a model: never parsed as HTML")
