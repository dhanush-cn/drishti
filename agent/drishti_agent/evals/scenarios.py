"""Scenarios from a recording's timeline, their trigger times and fault gate.

Trigger times (design E8, docs/evals.md section 4):
  agent-only  the investigation is as of flag-off (on + on_minutes): the
              incident has fully developed, so this measures diagnosis, not
              detection speed.
  end-to-end  as of the first alert the detector opened between flag-on and
              90 s after flag-off, on any service: what would page someone.
              No alert = missed (scored wrong, not skipped).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from ..api import APIError, QueryAPI
from .labels import LABELS, Check

GRACE_AFTER_OFF = timedelta(seconds=90)  # as scripts/detector-eval.py


def parse_ts(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    if "." in s:  # Python < 3.11 takes at most 6 fractional digits
        head, rest = s.split(".", 1)
        frac = "".join(c for c in rest if c.isdigit())
        s = f"{head}.{frac[:6]}{rest[len(frac):]}"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class Scenario:
    n: int
    flag: str
    variant: str
    on: datetime
    off: datetime            # end of the judged on-period (slept scenarios: on + on_minutes)
    slept: bool
    triggers: dict = field(default_factory=dict)   # column -> until (datetime) or None
    trigger_alert: Optional[dict] = None
    gate_passed: Optional[bool] = None
    gate_detail: str = ""

    def to_json(self) -> dict:
        return {"n": self.n, "flag": self.flag, "variant": self.variant, "on": iso(self.on), "off": iso(self.off), "slept": self.slept,
                "triggers": {k: (iso(v) if v else None) for k, v in self.triggers.items()}, "trigger_alert": self.trigger_alert,
                "gate_passed": self.gate_passed, "gate_detail": self.gate_detail}


def load_timeline(path: str) -> tuple[list[Scenario], tuple[datetime, datetime]]:
    """Scenarios and the no-flag baseline phase (start, end)."""
    rows = [json.loads(l) for l in open(path) if l.strip()]
    base = next(r for r in rows if r.get("phase") == "baseline")
    out = []
    for r in rows:
        if r.get("phase") != "scenario":
            continue
        on, off = parse_ts(r["on"]["set_at"]), parse_ts(r["off"]["set_at"])
        mins = timedelta(minutes=r["on_minutes"])
        slept = off - on > 3 * mins  # the laptop slept mid-scenario (capture 2, scenario 12)
        out.append(Scenario(n=r["n"], flag=r["flag"], variant=r.get("variant", ""), on=on, off=on + mins if slept else off, slept=slept))
    return out, (parse_ts(base["start"]), parse_ts(base["end"]))


def first_alert(base_url: str, sc: Scenario) -> Optional[dict]:
    end = sc.off + GRACE_AFTER_OFF
    span = end - sc.on
    window = f"{max(int(span.total_seconds()) + 1, 60)}s"
    api = QueryAPI(base_url, iso(end))
    try:
        alerts = api.get("/v1/anomalies", window=window)["data"] or []
    except APIError:
        return None
    alerts = [a for a in alerts if sc.on <= parse_ts(a["opened_at"]) <= end]
    alerts.sort(key=lambda a: (a["opened_at"], a["service"], a["rule"], a["id"]))
    return alerts[0] if alerts else None


def set_triggers(base_url: str, sc: Scenario) -> None:
    sc.triggers["agent-only"] = sc.off
    a = first_alert(base_url, sc)
    sc.trigger_alert = {"service": a["service"], "rule": a["rule"], "opened_at": a["opened_at"]} if a else None
    # In replay an alert's detected_at is its bucket end = opened_at (docs/detector.md section 5).
    sc.triggers["end-to-end"] = parse_ts(a["opened_at"]) if a else None


# ---- fault gate ----

def _overview(base_url: str, until: datetime, window: timedelta) -> dict:
    w = f"{max(int(window.total_seconds()), 60)}s"
    d = QueryAPI(base_url, iso(until)).get("/v1/overview", window=w, baseline="0s")["data"]["services"]
    return {s["service"]: s for s in d}


def _spans(base_url: str, until: datetime, window: timedelta, service: str) -> list:
    w = f"{max(int(window.total_seconds()), 60)}s"
    return QueryAPI(base_url, iso(until)).get("/v1/spans", service=service, window=w, baseline="0s")["data"] or []


def _w(stat: Optional[dict]) -> Optional[float]:
    return None if not stat else stat.get("window")


def check(base_url: str, c: Check, sc: Scenario, baseline: tuple[datetime, datetime]) -> tuple[bool, str]:
    win = sc.off - sc.on
    now = _overview(base_url, sc.off, win)
    before = _overview(base_url, baseline[1], baseline[1] - baseline[0])
    s, b = now.get(c.service, {}), before.get(c.service, {})
    if c.kind == "errors":
        n = 0.0
        for r in _spans(base_url, sc.off, win, c.service):
            if c.span_kind and r["kind"] != c.span_kind:
                continue
            if c.span and r["span"] != c.span:
                continue
            ratio, cpm = _w(r.get("error_ratio")) or 0, _w(r.get("calls_per_min")) or 0
            n += ratio * cpm * win.total_seconds() / 60
        return n >= c.value, f"{c.service} {c.span_kind or 'any'} {c.span or ''} errors {n:.0f} (need {c.value:g})"
    key = {"p95": "server_p95_ms", "cpu": "cpu_utilization", "calls": "server_calls_per_min"}.get(c.kind)
    if key:
        x, y = _w(s.get(key)), _w(b.get(key))
        if x is None or not y:
            return False, f"{c.service} {key}: no data (now {x}, baseline {y})"
        return x >= c.value * y, f"{c.service} {key} {x:.3g} vs baseline {y:.3g} (need x{c.value:g})"
    if c.kind == "memory":
        slope = s.get("memory_mib_per_min")
        return (slope or 0) >= c.value, f"{c.service} memory {slope} MiB/min (need {c.value:g})"
    raise ValueError(c.kind)


def gate(base_url: str, sc: Scenario, baseline: tuple[datetime, datetime]) -> None:
    label = LABELS.get(sc.flag)
    if label is None:
        sc.gate_passed, sc.gate_detail = False, "no label for this flag"
        return
    details = []
    for c in label.gate:
        try:
            ok, d = check(base_url, c, sc, baseline)
        except APIError as e:
            ok, d = False, f"{c.kind} {c.service}: {e}"
        details.append(("PASS " if ok else "fail ") + d)
        if ok:
            sc.gate_passed = True
            sc.gate_detail = "; ".join(details)
            return
    sc.gate_passed, sc.gate_detail = False, "; ".join(details)
