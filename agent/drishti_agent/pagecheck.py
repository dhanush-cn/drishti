"""page_check: the deterministic first step of a Week 6 investigation (docs/agent.md; design F3/F4).

No model is involved. Starting from the alert's service (end-to-end) or the services triage shows changed
(agent-only), it walks the call graph and builds a candidate table:

  service | relation to the page (paged / callee at depth k / changed, no edge) | its own signal change |
  onset of that change | trend at the page

Why code: following a client span's callee is a graph walk, and timing a change is arithmetic. Week 5's free
loop spent its tool budget doing both by hand and drifted on the way (docs/evals.md section 6). What is left
for the model is judgement: which candidate's own behaviour broke, and is the evidence enough.

Every rule here was fixed before any run (design note, section 4) and mirrors the query API's overview
thresholds (internal/queryapi changes()), so "changed" means the same thing in triage and here.

reassignment() is design rule F4, applied by the judge to end-to-end answers only.
"""
from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from .api import APIError, QueryAPI
from .prompts import TRIAGE_BASELINE, TRIAGE_BASELINE_PAGED, TRIAGE_WINDOW

MAX_DEPTH = 3                    # callee hops followed from a start service
DEFAULT_CAP = 12                 # query calls page_check may make (design F5). Set from the ceiling check (20 Nov):
                                 # the largest walk needed 11 (capture 2) and 7 (capture B); 8 cut one walk short.
ONSET_WINDOW = "30m"             # how far back an onset is searched
STEP = "30s"
EARLIER_INCIDENT = timedelta(minutes=5)   # F4; chosen with the 7-minute scenario cadence in view (design section 4)
UNPAGED_STARTS = 5               # agent-only: walk from the top changed services


# ---- time helpers ----

def parse_ts(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    if "." in s:  # Python < 3.11 parsed at most 6 fractional digits; keep it portable
        head, rest = s.split(".", 1)
        frac = "".join(c for c in rest if c.isdigit())
        s = f"{head}.{frac[:6]}{rest[len(frac):]}"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


_GO_DUR = re.compile(r"(\d+(?:\.\d+)?)(h|ms|m|s)")


def go_duration(s: str) -> timedelta:
    """A Go duration string as the detector writes it ("2m0s", "1h0m0s", "30s")."""
    parts = _GO_DUR.findall(s or "")
    if not parts:
        raise ValueError(f"not a duration: {s!r}")
    unit = {"h": 3600, "m": 60, "s": 1, "ms": 0.001}
    return timedelta(seconds=sum(float(n) * unit[u] for n, u in parts))


# ---- signals: what "past the threshold" means, per signal (the overview's numbers) ----

@dataclass(frozen=True)
class Signal:
    name: str          # timeseries signal
    direction: str     # "up" (a rise is the fault) or "down" (a drop is the fault: traffic drop)


def past(sig: str, v: Optional[float], ref: Optional[float], step_s: float, direction: str = "up") -> bool:
    """Is value v changed relative to reference ref, by the overview's thresholds?
    Counts per step are compared per minute where the overview has a per-minute floor."""
    if v is None or ref is None:
        return False
    per_min = 60.0 / step_s
    if sig == "error_ratio":
        return v >= max(2 * ref, ref + 0.02) and v >= 0.01
    if sig == "p95_ms":
        return v >= 2 * ref and v - ref >= 20
    if sig == "cpu":
        return v >= 2 * ref and v - ref >= 1
    if sig in ("calls", "log_lines"):
        if ref * per_min < 1:
            return False
        return v <= ref / 3 if direction == "down" else v >= 3 * ref
    if sig in ("errors", "log_errors"):
        return v * per_min >= 0.5 and v >= 3 * ref
    raise ValueError(f"no threshold for signal {sig}")


def percentile(xs: list[float], q: float) -> Optional[float]:
    """Nearest-rank percentile (q in 0..100). Plain and reproducible: no interpolation choices."""
    if not xs:
        return None
    s = sorted(xs)
    k = max(0, min(len(s) - 1, math.ceil(q / 100 * len(s)) - 1))
    return s[k]


def smooth(vals: list[Optional[float]]) -> list[Optional[float]]:
    """2-point (1-minute) mean, so one noisy 30 s point can't start or end a change."""
    out = []
    for i, v in enumerate(vals):
        pair = [x for x in (vals[i - 1] if i else None, v) if x is not None]
        out.append(sum(pair) / len(pair) if pair else None)
    return out


@dataclass
class Onset:
    at: Optional[str]          # ISO time of the onset, None if the signal has no current change
    reference: Optional[float] # the quiet level the change is measured against
    how: str                   # which rule produced it


def onset_of(points: list[dict], sig: str, direction: str = "up", step_s: float = 30.0) -> Onset:
    """Onset of the current change in a timeseries (design section 4, fixed before any run):
    - reference R = 10th percentile of the raw values (90th for a drop): faults fill up to 4 of every 7
      minutes of a recording, so a mean or median would sit inside them; the quiet minutes set the low tail;
    - a smoothed point is past when past(sig, v, R);
    - onset = first point of the final run of past points that reaches the newest point; the run breaks
      only on >= 2 consecutive smoothed points that are not past (3 quiet raw points, ~1.5 min, since the
      smoothing blurs one back). Newest point not past: no current change.
    Resolution: 30 s, and the trailing smoothing can put an onset one point late on a step whose halfway
    value doesn't cross the threshold."""
    if sig == "memory_mib":
        return memory_onset(points)
    ts = [p["t"] for p in points]
    raw = [p.get("v") for p in points]
    vals = [v for v in raw if v is not None]
    ref = percentile(vals, 90 if direction == "down" else 10)
    sm = smooth(raw)
    flags = [past(sig, v, ref, step_s, direction) for v in sm]
    if not flags or not flags[-1]:
        return Onset(None, ref, f"{sig}: newest point not past the threshold")
    start, quiet = len(flags) - 1, 0
    for i in range(len(flags) - 1, -1, -1):
        if flags[i]:
            start, quiet = i, 0
        else:
            quiet += 1
            if quiet >= 2:
                break
    return Onset(ts[start], ref, f"{sig}: run of past points from here (reference {ref:g})")


def linfit(xs: list[float], ys: list[float]) -> tuple[float, float]:
    """Least-squares slope and R² (the detector's memory test, internal/detect linfit)."""
    n = len(xs)
    sx, sy = sum(xs), sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    syy = sum(y * y for y in ys)
    den = n * sxx - sx * sx
    if den == 0:
        return 0.0, 0.0
    slope = (n * sxy - sx * sy) / den
    vy = n * syy - sy * sy
    if vy <= 0:
        return slope, 0.0
    r = (n * sxy - sx * sy) / math.sqrt(den * vy)
    return slope, r * r


MEM_SLOPE = 1.0   # MiB/min, the overview's "memory rising" threshold
MEM_R2 = 0.8      # the detector's MemMinR2


def _minutes(points: list[dict]) -> list[tuple[float, float, str]]:
    """(minutes since the first point, value, time) for the points with a value. Relative x keeps the
    least-squares sums exact enough: minutes since 1970 (~29 million) squared lose the residuals."""
    pts = [(parse_ts(p["t"]).timestamp(), p["v"], p["t"]) for p in points if p.get("v") is not None]
    return [((t - pts[0][0]) / 60.0, v, s) for t, v, s in pts] if pts else []


def _line(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """Least-squares line: slope, R², and the sum of squared residuals."""
    slope, r2 = linfit(xs, ys)
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sse = sum((y - (my + slope * (x - mx))) ** 2 for x, y in zip(xs, ys))
    return slope, r2, sse


def memory_onset(points: list[dict]) -> Onset:
    """Memory: a leak has no step to find; its start is where a flat level turns into a steady climb.
    Change-point fit: for every split t, a flat level before t and a least-squares line from t to the newest
    point; the split with the smallest total squared error is the knee. It is an onset only if that line
    climbs at >= 1 MiB/min with R² >= 0.8. A climb that fills the whole window gives t = the window start
    (the leak began at least that long ago).
    (The first version took "the earliest t whose fit over [t, newest] passes", and its unit test showed
    a flat-then-climbing series fits a line well enough from the very first point: wrong, found before any
    data reached it.)"""
    pts = _minutes(points)
    best = None
    for t in range(0, len(pts) - 3):  # at least 4 points on the line
        left = [p[1] for p in pts[:t]]
        sse_left = sum((y - sum(left) / len(left)) ** 2 for y in left) if left else 0.0
        slope, r2, sse_right = _line([p[0] for p in pts[t:]], [p[1] for p in pts[t:]])
        if best is None or sse_left + sse_right < best[0] - 1e-9:
            best = (sse_left + sse_right, t, slope, r2)
    if best is None:
        return Onset(None, None, "memory: too few points")
    _, t, slope, r2 = best
    if slope >= MEM_SLOPE and r2 >= MEM_R2:
        return Onset(pts[t][2], None, f"memory: knee of level-then-line fit; climb {slope:.2f} MiB/min, R² {r2:.2f}")
    return Onset(None, None, f"memory: no steady climb (best fit {slope:.2f} MiB/min, R² {r2:.2f})")


def trend_of(points: list[dict], sig: str, direction: str = "up", step_s: float = 30.0) -> Optional[str]:
    """Trend at the page (fixed before any run): mean of the last 2 minutes vs the 2 before, with the same
    threshold in the fault's direction. Past it = worsening; past it the other way = recovering; else holding.
    Memory: slope over the last 4 minutes, >= 1 MiB/min worsening, <= -1 recovering, else holding."""
    n = max(1, round(120 / step_s))
    if sig == "memory_mib":
        pts = _minutes(points)[-2 * n:]
        if len(pts) < 3:
            return None
        slope, _ = linfit([p[0] for p in pts], [p[1] for p in pts])
        return "worsening" if slope >= MEM_SLOPE else "recovering" if slope <= -MEM_SLOPE else "holding"
    vals = [p.get("v") for p in points]
    last, prev = [v for v in vals[-n:] if v is not None], [v for v in vals[-2 * n:-n] if v is not None]
    if not last or not prev:
        return None
    a, b = sum(last) / len(last), sum(prev) / len(prev)   # a = last 2 min, b = the 2 before
    if past(sig, a, b, step_s, direction):
        return "worsening"
    # Recovering = the same threshold crossed the other way: for a rise, the earlier 2 minutes were past
    # relative to now; for a traffic drop, traffic is now 3x what it was.
    back = past(sig, b, a, step_s, "up") if direction == "up" else past(sig, a, b, step_s, "up")
    return "recovering" if back else "holding"


# ---- the candidate table ----

@dataclass
class Candidate:
    service: str
    relation: str                       # paged | callee | changed (no edge) | start (agent-only)
    depth: int = 0
    via: Optional[str] = None           # the caller it was reached from
    own_change: list[str] = field(default_factory=list)   # what changed in its OWN behaviour
    client_trouble: list[str] = field(default_factory=list)  # failing/slow calls it makes (victim signs)
    signal: Optional[str] = None        # the signal its onset was measured on
    onset: Optional[str] = None
    trend: Optional[str] = None
    reference: Optional[float] = None   # the quiet level the onset was measured against (recovery is judged against it)
    note: str = ""


@dataclass
class PageCheck:
    paged: Optional[str]
    p_rule: Optional[str]
    p_onset: Optional[str]
    p_onset_how: str
    candidates: list[Candidate]
    calls: int
    cap: int
    capped: bool

    def get(self, service: Optional[str]) -> Optional[Candidate]:
        k = norm(service)
        return next((c for c in self.candidates if norm(c.service) == k), None)

    def to_json(self) -> dict:
        return asdict(self)

    def table(self) -> str:
        """Compact text for the model (patch 59 puts it in the hypothesize prompt)."""
        head = (f"paged: {self.paged} ({self.p_rule}), its onset {self.p_onset or 'unknown'} [{self.p_onset_how}]\n"
                if self.paged else "no page (agent-only): candidates walked from the top changed services\n")
        rows = []
        for c in self.candidates:
            rel = c.relation + (f" depth {c.depth} via {c.via}" if c.relation == "callee" else "")
            rows.append(f"- {c.service} [{rel}] own: {'; '.join(c.own_change) or 'none'}"
                        + (f" | calls failing/slow: {'; '.join(c.client_trouble)}" if c.client_trouble else "")
                        + (f" | onset {c.onset} ({c.signal}), trend {c.trend}" if c.onset else "")
                        + (f" | {c.note}" if c.note else ""))
        tail = f"\n(query calls used: {self.calls} of {self.cap}{', capped' if self.capped else ''})"
        return head + "\n".join(rows) + tail


def norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


class Budget:
    """Counts query calls; refuses past the cap. page_check stops walking when it runs out."""

    def __init__(self, api: QueryAPI, cap: int):
        self.api, self.cap, self.used, self.capped = api, cap, 0, False

    def get(self, path: str, **params):
        if self.used >= self.cap:
            self.capped = True
            return None
        self.used += 1
        try:
            return self.api.get(path, **params)["data"]
        except APIError:
            return None


def _w(stat: Optional[dict]) -> tuple[Optional[float], Optional[float]]:
    return (stat or {}).get("window"), (stat or {}).get("baseline")


def span_changes(row: dict) -> list[tuple[str, str]]:
    """(signal, words) for what moved on one spans row, by the overview's thresholds."""
    out = []
    w, b = _w(row.get("error_ratio"))
    if w is not None and ((b is not None and w >= max(2 * b, b + 0.02) and w >= 0.01) or (b is None and w >= 0.01)):
        out.append(("error_ratio", f"{row['kind']} {row['span']} error ratio {w:.3f} (was {b if b is not None else 'n/a'})"))
    w, b = _w(row.get("p95_ms"))
    if w is not None and b is not None and w >= 2 * b and w - b >= 20:
        out.append(("p95_ms", f"{row['kind']} {row['span']} p95 {w:.0f} ms (was {b:.0f})"))
    w, b = _w(row.get("calls_per_min"))
    if w is not None and b is not None and b >= 1 and (w >= 3 * b or w <= b / 3):
        out.append(("calls", f"{row['kind']} {row['span']} calls {w:.1f}/min (was {b:.1f})"))
    return out


# Order in which a candidate's own changes pick the signal its onset is measured on.
_PRIORITY = ("error_ratio", "p95_ms", "cpu", "memory_mib", "calls")
_OVERVIEW_SIGNALS = (("server error ratio", "error_ratio"), ("server p95", "p95_ms"), ("CPU utilization", "cpu"),
                     ("memory rising", "memory_mib"), ("server calls", "calls"))


def _overview_own(row: dict) -> list[tuple[str, str]]:
    """Own changes from an overview row's deterministic `changes` (log changes are not own-signal for F4:
    a log spike is also what a victim writes when its calls fail)."""
    out = []
    for words in row.get("changes") or []:
        for prefix, sig in _OVERVIEW_SIGNALS:
            if words.startswith(prefix):
                out.append((sig, words))
    return out


def p_onset(b: Budget, alert: dict) -> tuple[Optional[str], str]:
    """The paging service's onset, by the rule that fired (design section 4 table)."""
    rule, opened = alert.get("rule", ""), alert["opened_at"]
    window = alert.get("window")
    fallback = None
    if window:
        try:
            fallback = iso(parse_ts(opened) - go_duration(window))
        except ValueError:
            pass
    if rule == "log_template_new":
        m = re.search(r"first seen (\S+)", alert.get("detail") or "")
        if m:
            return iso(parse_ts(m.group(1))), "template first seen (alert evidence)"
        return fallback, "window start (no first-seen in evidence)"
    if rule in ("log_template_spike", "log_error_spike"):
        return fallback, f"start of the window that fired ({window})"
    if rule.startswith("span_"):
        series = alert.get("series") or {}
        kind, name = series.get("span.kind", ""), series.get("span.name", "")
        sig, direction = {"span_errors": ("error_ratio", "up"), "span_slow": ("p95_ms", "up"),
                          "span_traffic_surge": ("calls", "up"), "span_traffic_drop": ("calls", "down")}.get(rule, (None, "up"))
        if kind not in ("SERVER", "CLIENT") or sig is None:
            # /v1/timeseries reads SERVER and CLIENT span series (patch 59); others aren't timed.
            return fallback, f"start of the window that fired ({window}); {kind or 'unknown'} span series not timed (coarse)"
        data = b.get("/v1/timeseries", service=alert["service"], signal=sig, span=name, kind=kind.lower(), window=ONSET_WINDOW, step=STEP)
        if data is None:
            return fallback, "window start (timeseries unavailable or cap reached)"
        o = onset_of(data.get("points") or [], sig, direction)
        return (o.at, f"{name} {kind} {sig} timeseries") if o.at else (fallback, f"window start ({o.how})")
    if rule == "container_memory_growth":
        data = b.get("/v1/timeseries", service=alert["service"], signal="memory_mib", window=ONSET_WINDOW, step=STEP)
        if data is not None:
            o = memory_onset(data.get("points") or [])
            if o.at:
                return o.at, "memory climb start"
        return fallback, "window start (no steady climb found)"
    return fallback, f"window start (rule {rule or 'unknown'})"


def page_check(api: QueryAPI, alert: Optional[dict] = None, cap: int = DEFAULT_CAP) -> PageCheck:
    b = Budget(api, cap)
    baseline = TRIAGE_BASELINE_PAGED if alert else TRIAGE_BASELINE
    ov = b.get("/v1/overview", window=TRIAGE_WINDOW, baseline=baseline)
    rows = {r["service"]: r for r in ((ov or {}).get("services") or [])}

    cands: dict[str, Candidate] = {}
    queue: list[Candidate] = []
    if alert:
        start = Candidate(alert["service"], "paged")
        cands[norm(start.service)] = start
        queue.append(start)
    else:
        changed = [s for s, r in rows.items() if r.get("changes")]
        for s in changed[:UNPAGED_STARTS]:
            c = Candidate(s, "start")
            cands[norm(s)] = c
            queue.append(c)

    p_at, p_how = (None, "no page")
    if alert:
        p_at, p_how = p_onset(b, alert)

    # Breadth-first along failing or slow client calls: who does this service call that is in trouble?
    while queue:
        c = queue.pop(0)
        for sig, words in _overview_own(rows.get(c.service, {})):
            if words not in c.own_change:
                c.own_change.append(words)
                c.signal = c.signal or sig
        spans = b.get("/v1/spans", service=c.service, window=TRIAGE_WINDOW, baseline=baseline)
        if spans is None:
            c.note = "spans not read (cap)" if b.capped else "spans unavailable"
            continue
        for row in spans:
            ch = span_changes(row)
            if not ch:
                continue
            if row.get("kind") == "SERVER":
                for sig, words in ch:
                    if words not in c.own_change:
                        c.own_change.append(words)
                        if c.signal is None or _PRIORITY.index(sig) < _PRIORITY.index(c.signal):
                            c.signal = sig
            elif row.get("kind") == "CLIENT":
                c.client_trouble += [w for _, w in ch]
                if c.depth >= MAX_DEPTH:
                    continue
                for callee in filter(None, (row.get("callee") or "").split(",")):
                    if norm(callee) not in cands:
                        nc = Candidate(callee, "callee", depth=c.depth + 1, via=c.service)
                        cands[norm(callee)] = nc
                        queue.append(nc)

    # Services triage shows changed that no walk reached: candidates without an edge (an alert can land on a
    # service with no path to the root cause; capture B's emailMemoryLeak was paged on recommendation).
    for s, r in rows.items():
        own = _overview_own(r)
        if own and norm(s) not in cands:
            c = Candidate(s, "changed (no edge)", own_change=[w for _, w in own], signal=own[0][0])
            cands[norm(s)] = c

    # Onsets and trends, for candidates with an own change, paged service first, then by depth.
    order = sorted(cands.values(), key=lambda c: (c.relation != "paged", c.relation.startswith("changed"), c.depth))
    for c in order:
        if not c.own_change or c.signal is None:
            continue
        data = b.get("/v1/timeseries", service=c.service, signal=c.signal, window=ONSET_WINDOW, step=STEP)
        if data is None:
            c.note = (c.note + "; " if c.note else "") + ("onset not measured (cap)" if b.capped else "onset unavailable")
            continue
        pts = data.get("points") or []
        o = onset_of(pts, c.signal)
        c.onset, c.trend, c.reference = o.at, trend_of(pts, c.signal), o.reference
        if not o.at:
            c.note = (c.note + "; " if c.note else "") + o.how

    return PageCheck(paged=alert["service"] if alert else None, p_rule=alert.get("rule") if alert else None,
                     p_onset=p_at, p_onset_how=p_how, candidates=order, calls=b.used, cap=cap, capped=b.capped)


# ---- design rule F4 (end-to-end only) ----

def reassignment(answer: Optional[str], pc: PageCheck) -> tuple[bool, str]:
    """May the answer name this service? F4, end-to-end only:
    - naming the paged service, or nothing (unsure), is always allowed;
    - another service S needs its OWN signal change in the candidate table (a service that only fails
      because of a callee doesn't qualify), and
    - S must not be an earlier incident: onset more than 5 min before the paging service's onset AND
      holding or recovering at the page. A change that started long before but is still worsening (a slow
      leak) is allowed. A call edge to the page is evidence, never required."""
    if pc.paged is None:
        return True, "agent-only: F4 does not apply"
    if answer is None or norm(answer) == norm(pc.paged):
        return True, "names the paged service" if answer else "unsure"
    c = pc.get(answer)
    if c is None or not c.own_change:
        return False, f"{answer}: no change in its own signals was seen (only a service whose own behaviour changed can be the root cause)"
    if c.onset and pc.p_onset:
        lead = parse_ts(pc.p_onset) - parse_ts(c.onset)
        if lead > EARLIER_INCIDENT and c.trend in ("holding", "recovering"):
            return False, (f"{answer}: earlier incident: its change began {int(lead.total_seconds() // 60)} min before "
                           f"{pc.paged}'s and is {c.trend} at the page")
    edge = f"reached from {pc.paged}" if c.relation in ("callee", "paged") else "no call edge to the page"
    return True, f"{answer}: own change ({c.own_change[0]}); {edge}" + ("" if c.onset else "; onset not measured")


# ---- design rule G3′, the stale-change guard (Week 7; end-to-end only) ----
#
# Fixed in the design note (section 4) before its ceiling check: it reuses onset_of's reference, smoothing, past()
# thresholds and run-break rule, trend_of and memory_onset, and adds no threshold of its own.

STALE_SIGNALS = (("error_ratio", "up"), ("p95_ms", "up"), ("cpu", "up"), ("calls", "up"), ("calls", "down"), ("memory_mib", "up"))


def runs_of(points: list[dict], sig: str, direction: str = "up", step_s: float = 30.0) -> tuple[list[tuple[int, int]], bool]:
    """Runs of past smoothed points as (first, last) indexes, with page_check's run-break rule: a run ends where 2
    consecutive smoothed points are not past. Returns the runs and whether the last one is closed by that rule
    (False: it reaches the newest point, or only 1 quiet point follows it so far). Same reference and smoothing as
    onset_of, so the run reaching the newest point starts exactly at onset_of's onset."""
    raw = [p.get("v") for p in points]
    vals = [v for v in raw if v is not None]
    ref = percentile(vals, 90 if direction == "down" else 10)
    flags = [past(sig, v, ref, step_s, direction) for v in smooth(raw)]
    runs: list[tuple[int, int]] = []
    start = last = None
    quiet = 0
    for i, f in enumerate(flags):
        if f:
            if start is None:
                start = i
            last, quiet = i, 0
        elif start is not None:
            quiet += 1
            if quiet >= 2:
                runs.append((start, last))
                start = None
    if start is not None:
        runs.append((start, last))
        return runs, False
    return runs, bool(runs)


@dataclass
class SignalState:
    signal: str
    direction: str
    state: str                 # live | over (ended) | over (recovering) | none | other (ignored: see note)
    start: Optional[str] = None
    end: Optional[str] = None
    trend: Optional[str] = None
    note: str = ""


def signal_state(points: list[dict], sig: str, direction: str, p_onset: datetime, step_s: float = 30.0) -> SignalState:
    """One of S's signals against the paging service's onset (design note section 4):
    live            a run reaches the newest point and the trend at the page is not recovering;
    over (ended)    the last run was closed by the run-break rule, and its last past point is before P's onset;
    over (recovering)  a run reaches the newest point, the trend is recovering, and the run began before P's onset;
    none            no run in the window;
    other           a run that is none of these (ended after P's onset, recovering but begun after it, or fading with
                    one quiet point so far): it counts neither way."""
    ts = [p["t"] for p in points]
    trend = trend_of(points, sig, direction, step_s) if points else None
    if sig == "memory_mib":
        o = memory_onset(points)
        if not o.at:
            return SignalState(sig, direction, "none", trend=trend, note=o.how)
        if trend != "recovering":
            return SignalState(sig, direction, "live", start=o.at, trend=trend)
        if parse_ts(o.at) < p_onset:
            return SignalState(sig, direction, "over (recovering)", start=o.at, trend=trend)
        return SignalState(sig, direction, "other", start=o.at, trend=trend, note="recovering, began after the page's onset")
    runs, closed = runs_of(points, sig, direction, step_s)
    if not runs:
        return SignalState(sig, direction, "none", trend=trend)
    s, e = runs[-1]
    start, end = ts[s], ts[e]
    if e == len(points) - 1:
        if trend != "recovering":
            return SignalState(sig, direction, "live", start=start, trend=trend)
        if parse_ts(start) < p_onset:
            return SignalState(sig, direction, "over (recovering)", start=start, trend=trend)
        return SignalState(sig, direction, "other", start=start, trend=trend, note="recovering, began after the page's onset")
    if closed:
        if parse_ts(end) < p_onset:
            return SignalState(sig, direction, "over (ended)", start=start, end=end, trend=trend)
        return SignalState(sig, direction, "other", start=start, end=end, trend=trend, note="ended after the page's onset")
    return SignalState(sig, direction, "other", start=start, end=end, trend=trend, note="fading: one quiet point so far")


@dataclass
class StaleCheck:
    applies: bool
    stale: bool
    why: str
    signals: list[SignalState] = field(default_factory=list)
    calls: int = 0

    def to_json(self) -> dict:
        return asdict(self)


def stale_change(api: QueryAPI, service: Optional[str], paged: Optional[str], p_onset: Optional[str]) -> StaleCheck:
    """Is the answer S a stale change (G3′)? S is stale if at least one of its own signals is over and none is live.
    Not applied to the paged service, to no answer, or when P's onset is unknown. A service with no run at all is not
    stale: no change is not a stale change (paymentUnreachable's payment shows none; F4 refused it for that)."""
    if not service or not paged or norm(service) == norm(paged):
        return StaleCheck(False, False, "the paged service or no answer: not checked")
    if not p_onset:
        return StaleCheck(False, False, "the page's onset is unknown: not checked")
    p_at = parse_ts(p_onset)
    states: list[SignalState] = []
    calls = 0
    series: dict[str, Optional[list]] = {}
    for sig, direction in STALE_SIGNALS:
        if sig not in series:
            calls += 1
            try:
                series[sig] = (api.get("/v1/timeseries", service=service, signal=sig, window=ONSET_WINDOW, step=STEP)["data"]
                               or {}).get("points") or []
            except APIError:
                series[sig] = None
        pts = series[sig]
        if pts is None:
            states.append(SignalState(sig, direction, "other", note="timeseries unavailable"))
        else:
            states.append(signal_state(pts, sig, direction, p_at))
    live = [s for s in states if s.state == "live"]
    over = [s for s in states if s.state.startswith("over")]
    if over and not live:
        s = over[0]
        when = f"past {s.start} to {s.end}" if s.state == "over (ended)" else f"past since {s.start} and recovering at the page"
        return StaleCheck(True, True, f"{service}: stale change: {s.signal}{' drop' if s.direction == 'down' else ''} was {when}; "
                                      f"{paged}'s page started {p_onset}; no own signal of {service} is changing now", states, calls)
    if live:
        s = live[0]
        why = f"{service}: live change ({s.signal}{' drop' if s.direction == 'down' else ''} past since {s.start}, {s.trend})"
    else:
        why = f"{service}: no over signal (no change, or none that ended before the page)"
    return StaleCheck(True, False, why, states, calls)
