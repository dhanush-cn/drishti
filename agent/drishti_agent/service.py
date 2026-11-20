"""The agent service: start an investigation from an alert, watch it, approve its fix (design F8; docs/agent.md section 8).

    POST /v1/investigations            {"anomaly_id": "..."} -> the investigation (201 new, 200 existing)
    GET  /v1/investigations            recent investigations
    GET  /v1/investigations/{id}       status, diagnosis, proposal, action, recovery
    GET  /v1/investigations/{id}/events   SSE: every event so far, then live ones, until it stops
    POST /v1/investigations/{id}/decision {"approve": true, "by": "dhanush"} -> resumes a run waiting for approval
    GET  /ui/anomaly/{anomaly_id}      a page: start, watch, approve or reject (the dashboard's Investigate link)

- Manual trigger only: every investigation costs model calls. Auto-investigating every alert needs the rate limit and
  daily spend cap planned for Week 8. At most MAX_RUNNING investigations run at once.
- One investigation per alert: its thread id comes from the anomaly id, so a second click shows the first run.
- Everything durable is in the agent's Postgres: LangGraph checkpoints (a run survives a crash; a run waiting for
  approval survives a restart), the events of each run (so a page opened later replays them), the runbook index and
  incident memory (live: a finished run is remembered unconfirmed; a verified recovery confirms it).
- Runs natively in WSL (scripts/agent-service.sh), not in a container: Docker Desktop's host gateway reaches Windows,
  not the WSL distro (Week 1), and Ollama runs in WSL. Everything it talks to is on localhost there.

Configuration, each an environment variable: DRISHTI_QUERYAPI_URL, DRISHTI_AGENT_POSTGRES, DRISHTI_FLAGS_URL (the demo's
flag API through its proxy), AGENT_PROVIDER / AGENT_MODEL / AGENT_BASE_URL / AGENT_API_KEY_ENV / AGENT_RPM /
AGENT_REASONING / AGENT_MAX_TOKENS (llm.from_args), DRISHTI_VERIFY_INTERVAL / DRISHTI_VERIFY_TIMEOUT (seconds).
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

MAX_RUNNING = 2

from pydantic import BaseModel  # noqa: E402  (request bodies: module level, so FastAPI can resolve their names)


class StartBody(BaseModel):
    anomaly_id: str


class DecisionBody(BaseModel):
    approve: bool
    by: str


@dataclass
class Config:
    queryapi: str = "http://127.0.0.1:8090"
    postgres: str = "postgresql://drishti:drishti_dev@127.0.0.1:5434/drishti_agent"
    flags: str = "http://localhost:8080"
    provider: str = "ollama"
    model: str = "qwen2.5:7b"
    base_url: str = ""
    api_key_env: str = ""
    rpm: float = 0.0
    reasoning: str = ""
    max_tokens: int = 2048
    verify_interval: float = 30
    verify_timeout: float = 600
    runbooks: bool = True
    memory: bool = True

    @classmethod
    def from_env(cls) -> "Config":
        e = os.environ.get
        return cls(queryapi=e("DRISHTI_QUERYAPI_URL", cls.queryapi), postgres=e("DRISHTI_AGENT_POSTGRES", cls.postgres),
                   flags=e("DRISHTI_FLAGS_URL", cls.flags), provider=e("AGENT_PROVIDER", cls.provider),
                   model=e("AGENT_MODEL", cls.model), base_url=e("AGENT_BASE_URL", ""), api_key_env=e("AGENT_API_KEY_ENV", ""),
                   rpm=float(e("AGENT_RPM", "0")), reasoning=e("AGENT_REASONING", ""), max_tokens=int(e("AGENT_MAX_TOKENS", "2048")),
                   verify_interval=float(e("DRISHTI_VERIFY_INTERVAL", "30")), verify_timeout=float(e("DRISHTI_VERIFY_TIMEOUT", "600")),
                   runbooks=e("DRISHTI_AGENT_RUNBOOKS", "1") != "0", memory=e("DRISHTI_AGENT_MEMORY", "1") != "0")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def thread_for(anomaly_id: str) -> str:
    return "inv-" + hashlib.sha256(f"live|{anomaly_id}".encode()).hexdigest()[:16]


class Store:
    """Investigations and their events, in the agent's Postgres."""

    def __init__(self, pool):
        self.pool = pool

    def setup(self) -> None:
        with self.pool.connection() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS agent_investigations (
                id text PRIMARY KEY, anomaly_id text UNIQUE NOT NULL, alert jsonb NOT NULL, until text NOT NULL,
                status text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())""")
            c.execute("""CREATE TABLE IF NOT EXISTS agent_events (
                id text NOT NULL, seq int NOT NULL, at timestamptz NOT NULL DEFAULT now(), event jsonb NOT NULL,
                PRIMARY KEY (id, seq))""")

    def create(self, id: str, anomaly_id: str, alert: dict, until: str) -> bool:
        with self.pool.connection() as c:
            n = c.execute("""INSERT INTO agent_investigations (id, anomaly_id, alert, until, status) VALUES (%s, %s, %s::jsonb, %s, 'running')
                             ON CONFLICT DO NOTHING""", (id, anomaly_id, json.dumps(alert), until)).rowcount
        return n == 1

    def get(self, id: str) -> Optional[dict]:
        with self.pool.connection() as c:
            r = c.execute("SELECT id, anomaly_id, alert, until, status, created_at, updated_at FROM agent_investigations WHERE id = %s", (id,)).fetchone()
        if not r:
            return None
        return {"id": r[0], "anomaly_id": r[1], "alert": r[2], "until": r[3], "status": r[4], "created_at": r[5].isoformat(),
                "updated_at": r[6].isoformat()}

    def by_status(self, status: str) -> list[dict]:
        with self.pool.connection() as c:
            ids = [r[0] for r in c.execute("SELECT id FROM agent_investigations WHERE status = %s", (status,)).fetchall()]
        return [self.get(i) for i in ids]

    def recent(self, n: int = 50) -> list[dict]:
        with self.pool.connection() as c:
            ids = [r[0] for r in c.execute("SELECT id FROM agent_investigations ORDER BY created_at DESC LIMIT %s", (n,)).fetchall()]
        return [self.get(i) for i in ids]

    def set_status(self, id: str, status: str) -> None:
        with self.pool.connection() as c:
            c.execute("UPDATE agent_investigations SET status = %s, updated_at = now() WHERE id = %s", (status, id))

    def claim(self, id: str, frm: str, to: str) -> bool:
        """Change status only if it is `frm` (one decision per approval, even with two clicks)."""
        with self.pool.connection() as c:
            n = c.execute("UPDATE agent_investigations SET status = %s, updated_at = now() WHERE id = %s AND status = %s",
                          (to, id, frm)).rowcount
        return n == 1

    def add_event(self, id: str, event: dict) -> int:
        with self.pool.connection() as c:
            seq = c.execute("SELECT coalesce(max(seq), 0) + 1 FROM agent_events WHERE id = %s", (id,)).fetchone()[0]
            c.execute("INSERT INTO agent_events (id, seq, event) VALUES (%s, %s, %s::jsonb)", (id, seq, json.dumps(event, default=str)))
        return seq

    def events(self, id: str, after: int = 0) -> list[tuple[int, dict]]:
        with self.pool.connection() as c:
            return [(r[0], r[1]) for r in c.execute("SELECT seq, event FROM agent_events WHERE id = %s AND seq > %s ORDER BY seq",
                                                     (id, after)).fetchall()]


@dataclass
class Deps:
    """What the service needs, injectable for tests."""
    cfg: Config
    llm: object = None
    flags: object = None
    verifier: object = None
    api_factory: object = None        # (base, until) -> QueryAPI
    pool: object = None
    checkpointer: object = None
    threads: dict = field(default_factory=dict)


def summary(events: list[dict]) -> dict:
    out = {}
    for e in events:
        t = e.get("type")
        if t == "diagnosis":
            out["diagnosis"] = e.get("diagnosis")
        elif t == "proposal":
            out["proposal"] = e.get("proposal")
        elif t == "decision":
            out["approval"] = e.get("approval")
        elif t == "action":
            out["action"] = e.get("action")
        elif t == "recovery":
            out["recovery"] = e.get("recovery")
    return out


def create_app(deps: Deps):
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse, StreamingResponse

    from . import durable, remediate
    from .api import APIError, QueryAPI
    from .llm import from_args
    from .memory import Memory, signature
    from . import runbook_tools

    cfg = deps.cfg
    api_factory = deps.api_factory or (lambda base, until: QueryAPI(base, until))
    if deps.pool is None:
        from psycopg_pool import ConnectionPool
        deps.pool = ConnectionPool(cfg.postgres, kwargs={"autocommit": True, "prepare_threshold": 0}, open=True)
    if deps.checkpointer is None:
        from langgraph.checkpoint.postgres import PostgresSaver
        deps.checkpointer = PostgresSaver(deps.pool)
        deps.checkpointer.setup()
    store = Store(deps.pool)
    store.setup()
    llm = deps.llm or from_args(cfg.provider, cfg.model, cfg.base_url, cfg.api_key_env, rpm=cfg.rpm,
                                reasoning_effort=cfg.reasoning, max_tokens=cfg.max_tokens)
    rem = remediate.Remediation(deps.flags or remediate.FlagAPI(cfg.flags),
                                deps.verifier or remediate.Verifier(cfg.queryapi, interval=cfg.verify_interval, timeout=cfg.verify_timeout))
    extras, tools = [], {}
    if cfg.runbooks:
        idx = runbook_tools.PostgresIndex(cfg.postgres)
        idx.build()
        extras.append(runbook_tools.relations_extra)
        tools = runbook_tools.local_tools(idx)
    mem = None
    if cfg.memory:
        mem = Memory(cfg.postgres, source="live")
        mem.setup()
    running = threading.Semaphore(MAX_RUNNING)

    def remember(inv: dict, evs: list[dict], final: dict) -> None:
        if mem is None:
            return
        s = summary(evs)
        d = s.get("diagnosis") or {}
        if not d:
            return
        mem.write(inv["id"], signature(final.get("pc") or {}, inv["alert"]), d.get("root_cause_service"),
                  d.get("fault_type") or "unknown", (d.get("summary") or "")[:300])
        rec = s.get("recovery") or {}
        if rec.get("recovered") is True:
            mem.resolve(inv["id"], "confirmed", d.get("root_cause_service"), "recovery verified")

    def drive(inv: dict, resume: Optional[dict] = None) -> None:
        """Run (or resume) an investigation in this thread, storing every event."""
        with running:
            api = api_factory(cfg.queryapi, inv["until"])
            kw = dict(thread_id=inv["id"], remediation=rem, extras=extras + ([mem.extra(inv["alert"])] if mem else []),
                      local_tools=tools)
            evs = []
            try:
                if resume:
                    rest = {k: v for k, v in kw.items() if k not in ("thread_id", "remediation")}
                    gen = durable.decide(api, llm, deps.checkpointer, inv["id"], resume["approve"], resume["by"], rem, **rest)
                else:
                    gen = durable.stream(api, llm, deps.checkpointer, inv["alert"], **kw)
                status, final = "finished", {}
                for ev in gen:
                    if ev["type"] == "end":
                        final = ev["state"]
                        continue
                    if ev["type"] == "awaiting_approval":
                        status = "awaiting_approval"
                    evs.append(ev)
                    store.add_event(inv["id"], ev)
                all_events = [e for _, e in store.events(inv["id"])]
                if status == "finished":
                    remember(inv, all_events, final)
                store.add_event(inv["id"], {"type": "end", "status": status})
                store.set_status(inv["id"], status)
            except Exception as e:  # recorded, never silent; a restart resumes from the last checkpoint
                store.add_event(inv["id"], {"type": "failed", "error": str(e)[:500]})
                store.add_event(inv["id"], {"type": "end", "status": "failed"})
                store.set_status(inv["id"], "failed")

    def start(inv: dict, resume: Optional[dict] = None) -> None:
        t = threading.Thread(target=drive, args=(inv, resume), daemon=True, name=f"inv {inv['id']}")
        deps.threads[inv["id"]] = t
        t.start()

    # Resume what a restart interrupted: runs that were running continue from their last checkpoint. A run waiting
    # for approval needs nothing: its state is in Postgres and the decision endpoint resumes it.
    for inv in store.by_status("running"):
        start(inv)

    app = FastAPI(title="Drishti agent")

    def view(inv: dict) -> dict:
        return {**inv, **summary([e for _, e in store.events(inv["id"])])}

    @app.post("/v1/investigations")
    def create(body: StartBody):
        from fastapi.responses import JSONResponse
        id = thread_for(body.anomaly_id)
        existing = store.get(id)
        if existing:
            return JSONResponse(view(existing), status_code=200)
        until = now_iso()
        try:
            alerts = api_factory(cfg.queryapi, until).get("/v1/anomalies", window="2h")["data"] or []
        except APIError as e:
            raise HTTPException(502, f"query API: {e}")
        alert = next((a for a in alerts if a.get("id") == body.anomaly_id), None)
        if alert is None:
            raise HTTPException(404, f"no alert {body.anomaly_id!r} in the last 2 hours")
        alert = {k: alert.get(k) for k in ("id", "service", "rule", "series", "opened_at", "window", "detail")}
        if store.create(id, body.anomaly_id, alert, until):
            inv = store.get(id)
            start(inv)
            return JSONResponse(view(inv), status_code=201)
        return JSONResponse(view(store.get(id)), status_code=200)

    @app.get("/v1/investigations")
    def recent():
        return store.recent()

    @app.get("/v1/investigations/{id}")
    def one(id: str):
        inv = store.get(id)
        if not inv:
            raise HTTPException(404, "no such investigation")
        return view(inv)

    @app.post("/v1/investigations/{id}/decision")
    def decide(id: str, body: DecisionBody):
        inv = store.get(id)
        if not inv:
            raise HTTPException(404, "no such investigation")
        if not body.by.strip():
            raise HTTPException(400, "a decision needs who made it ('by')")
        if not store.claim(id, "awaiting_approval", "running"):
            raise HTTPException(409, f"not waiting for a decision (status {inv['status']})")
        start(store.get(id), resume={"approve": body.approve, "by": body.by.strip()})
        return {"id": id, "status": "running"}

    @app.get("/v1/investigations/{id}/events")
    def events(id: str, after: int = 0):
        if not store.get(id):
            raise HTTPException(404, "no such investigation")

        def gen():
            """Every event after `after`, then live ones. Ends with a "done" event once the run is not running and
            everything has been sent; the page closes on it (EventSource would otherwise reconnect by itself)."""
            last, idle = after, 0.0
            while True:
                batch = store.events(id, last)
                for seq, ev in batch:
                    last = seq
                    yield f"id: {seq}\nevent: {ev.get('type', 'message')}\ndata: {json.dumps(ev, default=str)}\n\n"
                if not batch and store.get(id)["status"] != "running" and not store.events(id, last):
                    yield f"event: done\ndata: {json.dumps({'status': store.get(id)['status']})}\n\n"
                    return
                if not batch:
                    time.sleep(0.25)
                    idle += 0.25
                    if idle >= 15:
                        idle = 0.0
                        yield ": keep-alive\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

    @app.get("/ui/anomaly/{anomaly_id}", response_class=HTMLResponse)
    def page(anomaly_id: str):
        return PAGE.replace("__ANOMALY_ID__", json.dumps(anomaly_id))

    @app.get("/healthz")
    def health():
        return {"ok": True}

    return app


# The page: plain JS, text only (textContent, never innerHTML with event data: it comes from a model).
PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>Drishti investigation</title>
<style>body{font:14px system-ui,sans-serif;margin:24px;max-width:960px}h1{font-size:18px}.ev{border-left:3px solid #ccc;padding:4px 10px;margin:6px 0;white-space:pre-wrap}
.diagnosis{border-color:#2a7}.awaiting_approval{border-color:#e90}.recovery{border-color:#27a}.failed{border-color:#c33}button{margin-right:8px}</style></head>
<body><h1>Investigation of alert <span id="aid"></span></h1>
<p><button id="start">Investigate</button><span id="status"></span></p>
<div id="decide" hidden><p>Proposed fix: <b id="fix"></b></p><input id="by" placeholder="your name">
<button id="approve">Approve</button><button id="reject">Reject</button></div><div id="log"></div>
<script>
const anomaly = __ANOMALY_ID__; let inv = null, es = null;
const $ = id => document.getElementById(id); $("aid").textContent = anomaly;
function line(type, text) { const d = document.createElement("div"); d.className = "ev " + type; d.textContent = type + ": " + text; $("log").appendChild(d); }
function show(ev) {
  const t = ev.type;
  if (t === "candidates") line(t, (ev.candidates || []).map(c => `${c.service} [${c.relation}] ${(c.own_change || []).join("; ") || "no own change"}`).join("\\n"));
  else if (t === "hypotheses") line(t, ev.items.map(h => `${h.id} ${h.service} (${h.fault_type}) prior ${h.prior}: ${h.why}`).join("\\n"));
  else if (t === "tool_calls") line(t, ev.calls.map(c => `${c.name} ${JSON.stringify(c.arguments)}`).join("\\n"));
  else if (t === "tool_results") line(t, ev.results.map(r => `[${r.id}] ${r.name}: ${r.summary}`).join("\\n"));
  else if (t === "verdict") line(t, `${ev.hypothesis} ${ev.service}: ${ev.status} (${ev.confidence}) ${ev.summary || ""}`);
  else if (t === "judge") line(t, ev.verdict);
  else if (t === "diagnosis") line(t, JSON.stringify(ev.diagnosis, null, 1));
  else if (t === "awaiting_approval") { $("decide").hidden = false; $("fix").textContent = `${ev.proposal.action}(${ev.proposal.service})`; line(t, ev.proposal.why); }
  else if (t === "end") { $("status").textContent = " " + ev.status; if (ev.status !== "awaiting_approval") $("decide").hidden = true; }
  else line(t, JSON.stringify(ev));
}
let lastSeq = 0;
function follow() { if (es) es.close(); es = new EventSource(`/v1/investigations/${inv.id}/events?after=${lastSeq}`);
  const on = e => { lastSeq = Math.max(lastSeq, +e.lastEventId || 0); show(JSON.parse(e.data)); };
  ["start","resumed","candidates","hypotheses","tool_calls","tool_results","verdict","judge","diagnosis","proposal","awaiting_approval","decision","action","recovery","failed","end"]
    .forEach(t => es.addEventListener(t, on));
  es.addEventListener("done", () => es.close()); }
async function post(url, body) { const r = await fetch(url, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body)});
  if (!r.ok) { line("failed", `${r.status} ${await r.text()}`); return null; } return r.json(); }
$("start").onclick = async () => { inv = await post("/v1/investigations", {anomaly_id: anomaly}); if (inv) { $("status").textContent = " " + inv.status; follow(); } };
async function decide(approve) { const by = $("by").value.trim(); if (!by) { line("failed", "enter your name first"); return; }
  if (await post(`/v1/investigations/${inv.id}/decision`, {approve, by})) { $("decide").hidden = true; follow(); } }
$("approve").onclick = () => decide(true); $("reject").onclick = () => decide(false);
</script></body></html>"""


def main() -> None:
    import uvicorn
    cfg = Config.from_env()
    uvicorn.run(create_app(Deps(cfg=cfg)), host=os.environ.get("DRISHTI_AGENT_HOST", "127.0.0.1"),
                port=int(os.environ.get("DRISHTI_AGENT_PORT", "8092")))


if __name__ == "__main__":
    main()
