"""The query API client and the tools the model sees (docs/evals.md section 3).

The harness fixes `until` for the whole investigation; no tool takes a time.
The model can choose windows, services and signals, but it can never look
past the moment it was called, which keeps a replayed incident honest.

Tool results go back to the model as compact JSON, nulls dropped and capped
at MAX_RESULT_CHARS: a bounded number of tokens per call.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

MAX_RESULT_CHARS = 6000


class APIError(Exception):
    pass


class QueryAPI:
    def __init__(self, base_url: str, until: str, timeout: float = 60.0):
        self.base = base_url.rstrip("/")
        self.until = until
        self.timeout = timeout

    def get(self, path: str, **params: Any) -> dict:
        q = {k: v for k, v in params.items() if v not in (None, "")}
        q["until"] = self.until  # always the harness's, whatever the caller passed
        return self._fetch(f"{self.base}{path}?{urllib.parse.urlencode(q)}")

    def _fetch(self, url: str) -> dict:
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            try:
                msg = json.loads(body).get("error", body)
            except ValueError:
                msg = body
            raise APIError(msg) from None


def _schema(props: dict, required: list[str] | None = None) -> dict:
    return {"type": "object", "properties": props, "required": required or [], "additionalProperties": False}


_WINDOW = {"type": "string", "description": "How far back the 'now' window reaches from the investigation time, e.g. '5m' (1m..2h). Default 10m."}
_BASELINE = {"type": "string", "description": "The comparison period just before the window, e.g. '30m' (0..2h). Default 30m."}
_SERVICE = {"type": "string", "description": "A service name from list_services or overview."}

# The tools the agent may call. Names and arguments map one to one to the
# query API; until is added by QueryAPI.
TOOLS: list[dict] = [
    {"name": "list_services", "description": "List the instrumented services.", "parameters": _schema({})},
    {"name": "overview",
     "description": "Every service now (window) vs before (baseline): server calls/min, server error ratio, server p95 latency, "
                    "log lines and warn/error lines per minute, container CPU and memory (with its trend). 'changes' lists what crossed "
                    "fixed thresholds; services with changes come first.",
     "parameters": _schema({"window": _WINDOW, "baseline": _BASELINE})},
    {"name": "anomalies", "description": "Alerts the streaming detector opened in the window, with their evidence and whether still open.",
     "parameters": _schema({"window": _WINDOW})},
    {"name": "spans",
     "description": "One service's operations now vs before: calls/min, error ratio, p95 latency per span. For CLIENT spans, 'callee' is "
                    "the service that serves that operation, so this shows which dependency is failing or slow.",
     "parameters": _schema({"service": _SERVICE, "window": _WINDOW, "baseline": _BASELINE}, ["service"])},
    {"name": "timeseries", "description": "One signal of one service over the window, to see when something started.",
     "parameters": _schema({"service": _SERVICE, "window": _WINDOW,
                            "signal": {"type": "string", "enum": ["calls", "errors", "error_ratio", "p95_ms", "cpu", "memory_mib", "log_lines", "log_errors"]},
                            "step": {"type": "string", "description": "Bucket size, e.g. '30s' or '1m'."},
                            "span": {"type": "string", "description": "Optional: one SERVER span name."}}, ["service", "signal"])},
    {"name": "log_templates",
     "description": "One service's log message templates now vs before (new templates and ones that became more frequent come first).",
     "parameters": _schema({"service": _SERVICE, "window": _WINDOW, "baseline": _BASELINE}, ["service"])},
    {"name": "logs", "description": "One service's recent log lines in the window, newest first, optionally filtered.",
     "parameters": _schema({"service": _SERVICE, "window": _WINDOW,
                            "contains": {"type": "string", "description": "Case-insensitive text the body must contain."},
                            "min_severity": {"type": "string", "enum": ["info", "warn", "error"]},
                            "limit": {"type": "integer", "description": "1..50, default 20."}}, ["service"])},
]

_PATHS = {"list_services": "/v1/services", "overview": "/v1/overview", "anomalies": "/v1/anomalies", "spans": "/v1/spans",
          "timeseries": "/v1/timeseries", "log_templates": "/v1/log_templates", "logs": "/v1/logs"}


def _prune(x: Any) -> Any:
    if isinstance(x, dict):
        return {k: _prune(v) for k, v in x.items() if v not in (None, [], {}, "")}
    if isinstance(x, list):
        return [_prune(v) for v in x]
    return x


def compact(obj: Any, limit: int = MAX_RESULT_CHARS) -> str:
    s = json.dumps(_prune(obj), separators=(",", ":"), ensure_ascii=False)
    if len(s) > limit:
        s = s[:limit] + f'…[truncated: {len(s) - limit} more chars; narrow the window or pick one service]'
    return s


def _pair(stat, fmt="{:.3g}") -> str:
    if not stat:
        return "-"
    w, b = stat.get("window"), stat.get("baseline")
    f = lambda x: "-" if x is None else fmt.format(x)
    return f"{f(w)}/{f(b)}"


def format_overview(res: dict) -> str:
    """The overview as compact text: changed services and their changes first,
    then one line per service. JSON for ~25 services is longer than the tool
    result cap; truncated JSON would hide services from the model."""
    services = (res.get("data") or {}).get("services") or []
    lines = [f"masked_lines={res.get('masked_lines', 0)} window={res.get('window')} baseline={res.get('baseline')}"]
    changed = [s for s in services if s.get("changes")]
    lines.append("services with changes (most changes first):" if changed else "no service crossed a change threshold")
    for s in changed:
        lines.append(f"- {s['service']}: " + "; ".join(s["changes"]))
    lines.append("all services, now/before: server calls/min | server error ratio | server p95 ms | log lines/min | "
                 "warn+error lines/min | cpu | memory MiB (trend MiB/min, % of limit)")
    for s in services:
        mem = "-"
        if s.get("memory_mib") is not None:
            mem = f"{s['memory_mib']:.0f}"
            extra = []
            if s.get("memory_mib_per_min") is not None:
                extra.append(f"{s['memory_mib_per_min']:+.2f}/min")
            if s.get("memory_pct_of_limit") is not None:
                extra.append(f"{s['memory_pct_of_limit']:.0f}%")
            if extra:
                mem += f" ({', '.join(extra)})"
        lines.append(f"{s['service']}: {_pair(s.get('server_calls_per_min'))} | {_pair(s.get('server_error_ratio'))} | "
                     f"{_pair(s.get('server_p95_ms'))} | {_pair(s.get('log_lines_per_min'))} | "
                     f"{_pair(s.get('log_warn_error_lines_per_min'))} | {_pair(s.get('cpu_utilization'))} | {mem}")
    return "\n".join(lines)


def call_tool(api: QueryAPI, name: str, args: dict) -> tuple[bool, str, int]:
    """Runs one tool call. Returns (ok, text for the model, milliseconds).
    Errors go back to the model as text it can act on (wrong service name,
    bad window), not as exceptions."""
    start = time.monotonic()
    if name not in _PATHS:
        return False, f"error: unknown tool {name!r}", 0
    args = {k: v for k, v in (args or {}).items() if k != "until"}
    try:
        res = api.get(_PATHS[name], **args)
        if name == "overview":
            return True, format_overview(res)[:MAX_RESULT_CHARS * 2], int((time.monotonic() - start) * 1000)
        ok, text = True, compact({"masked_lines": res.get("masked_lines", 0), "window": res.get("window"),
                                  "baseline": res.get("baseline"), "data": res.get("data")})
    except APIError as e:
        ok, text = False, f"error: {e}"
    except Exception as e:  # network trouble: tell the model, let the budget decide
        ok, text = False, f"error: query failed: {e}"
    return ok, text, int((time.monotonic() - start) * 1000)
