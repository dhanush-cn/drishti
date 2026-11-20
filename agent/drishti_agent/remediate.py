"""Human-approved remediation and recovery verification (design F9, F10; docs/agent.md section 7).

The allowlist has one action: rollback_config(service). In the demo every fault is a config flag, so rolling back the
diagnosed service's config is the fix that works; in a real system it would be a deploy or config rollback API.
Restart is left out on purpose: restarting a leaking service hides the leak, and it changes the demo's state for the
scenarios that follow.

- propose (code): a diagnosis naming S -> rollback_config(S); unsure -> no action. One action per service: a lookup,
  not a judgement, so no model.
- execute (code): refuses anything outside the allowlist and anything without an approval record (who, when). The
  executor alone knows which flags a service owns (from the eval labels); the agent never sees a flag name. It turns
  off every flag that service owns that is on. Turning a flag off twice leaves it off: a re-run after a crash is
  harmless. A wrong diagnosis rolls back the wrong service, nothing relevant changes, and verify says so: an honest
  failure, left to the human.
- verify (code): every 30 s, the diagnosed service's changed signal (as page_check measured it) against its
  pre-incident reference with the same threshold; recovered after 2 minutes in a row back within it; memory: slope
  below 0.5 MiB/min (memory doesn't shrink without a restart). Gives up after 10 minutes. No shell, ever.
"""
from __future__ import annotations

import hashlib
import json
import time
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Optional

from .api import APIError, QueryAPI
from .evals.labels import LABELS
from .pagecheck import iso, linfit, norm, past, _minutes

ACTIONS = ("rollback_config",)


def owned_flags(service: str) -> list[str]:
    """The demo flags whose fault sits in this service's config (an accepted root cause in the labels)."""
    return sorted(f for f, lab in LABELS.items() if norm(service) in {norm(s) for s in lab.services})


def propose(diagnosis: dict) -> Optional[dict]:
    svc = diagnosis.get("root_cause_service")
    if not svc:
        return None
    return {"action": "rollback_config", "service": svc,
            "why": f"diagnosed root cause: {svc} ({diagnosis.get('fault_type')}), confidence {diagnosis.get('confidence')}"}


class FlagAPI:
    """The demo's flag API (flagd-ui /feature/api/read and /write), the way scripts/demo-flag.py uses it."""

    def __init__(self, base: str = "http://localhost:8080", timeout: float = 10):
        self.base, self.timeout = base.rstrip("/"), timeout

    def _http(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read() or b"{}")

    def read(self) -> dict:
        return self._http("GET", "/feature/api/read")

    def write(self, body: dict) -> None:
        self._http("POST", "/feature/api/write", {"data": body})


def _variant(name: str, flag: dict) -> str:
    return flag["targeting"]["if"][1] if name == "productCatalogFailure" else flag["defaultVariant"]


def _set_off(name: str, flag: dict) -> None:
    if name == "productCatalogFailure":
        flag["targeting"]["if"][1] = "off"
    else:
        flag["defaultVariant"] = "off"


def execute(proposal: dict, approval: dict, flags: FlagAPI, thread_id: str = "") -> dict:
    """Run an approved, allowlisted action. Idempotent."""
    if not proposal or proposal.get("action") not in ACTIONS:
        return {"ok": False, "refused": f"action {proposal and proposal.get('action')!r} is not allowlisted"}
    if not approval or not approval.get("approve") or not str(approval.get("by") or "").strip():
        return {"ok": False, "refused": "no approval record (who approved it)"}
    svc = proposal["service"]
    action_id = "act-" + hashlib.sha256(f"{thread_id}|{proposal['action']}|{svc}".encode()).hexdigest()[:12]
    owned = owned_flags(svc)
    body = flags.read()
    turned = []
    for name in owned:
        f = body["flags"].get(name)
        if f and "off" in f.get("variants", {}) and _variant(name, f) != "off":
            _set_off(name, f)
            turned.append(name)
    if turned:
        flags.write(body)
    return {"ok": True, "action_id": action_id, "service": svc, "changed": len(turned), "flags_turned_off": turned,
            "note": "" if owned else f"no config change is known for {svc}: nothing rolled back"}


class Verifier:
    """Recovery from metrics, live (until = now at each check)."""

    def __init__(self, api_base: str, *, interval: float = 30, timeout: float = 600, need: int = 4,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc), sleep: Callable[[float], None] = time.sleep):
        self.base, self.interval, self.timeout, self.need, self.now, self.sleep = api_base, interval, timeout, need, now, sleep

    def check(self, service: str, signal: str, reference: Optional[float]) -> tuple[Optional[bool], Optional[float]]:
        """Is the signal back within the threshold of its reference right now? (None: no data.)"""
        try:
            d = QueryAPI(self.base, iso(self.now())).get("/v1/timeseries", service=service, signal=signal, window="5m", step="30s")["data"]
        except APIError:
            return None, None
        pts = d.get("points") or []
        if signal == "memory_mib":
            m = _minutes(pts)[-8:]
            if len(m) < 3:
                return None, None
            slope, _ = linfit([p[0] for p in m], [p[1] for p in m])
            return slope < 0.5, round(slope, 3)
        vals = [p["v"] for p in pts[-4:] if p.get("v") is not None]
        if not vals or reference is None:
            return None, None
        mean = sum(vals) / len(vals)
        return not past(signal, mean, reference, 30.0), round(mean, 4)

    def verify(self, pc: dict, service: str) -> dict:
        c = next((x for x in pc.get("candidates") or [] if norm(x["service"]) == norm(service)), None)
        if not c or not c.get("own_change") or not c.get("signal"):
            return {"recovered": None, "why": f"{service} has no measured own signal to verify against"}
        start, streak, checks, last = time.monotonic(), 0, 0, None
        waited = 0.0
        while waited <= self.timeout:
            ok, last = self.check(service, c["signal"], c.get("reference"))
            checks += 1
            streak = streak + 1 if ok else 0
            if streak >= self.need:
                return {"recovered": True, "signal": c["signal"], "reference": c.get("reference"), "last": last,
                        "checks": checks, "seconds": round(waited, 1)}
            self.sleep(self.interval)
            waited += self.interval
        return {"recovered": False, "signal": c["signal"], "reference": c.get("reference"), "last": last, "checks": checks,
                "seconds": round(waited, 1), "why": f"not back within the threshold for {self.need * self.interval:.0f} s in a row"}


class Remediation:
    """What the graph needs after a report: the flag API and the verifier (live only; evals pass none)."""

    def __init__(self, flags: FlagAPI, verifier: Verifier):
        self.flags, self.verifier = flags, verifier
