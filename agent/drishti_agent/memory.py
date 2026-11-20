"""Incident memory: finished investigations, retrieved for similar alerts (design F12; docs/agent.md section 6).

- Signature (code, no model): the page (service and rule), each candidate's own changes by kind, and the victims
  (candidates with failing or slow calls), from page_check's table. Similarity = Jaccard over these sets; the top 2
  above 0.5 go into hypothesize with their outcome. No embeddings: the signature is already structured.
- Outcomes: "confirmed" (an approved fix whose recovery was verified, or a human said it was right), "wrong" (kept:
  "last time X was blamed and it was wrong; it was Y"), "unconfirmed" (never retrieved: an unchecked answer is not
  knowledge).
- Sources keep memories apart: "live" for the running system, a recording's name for eval seeds.
- Evals: memory is READ-ONLY during a run (no learning across scenarios, so results don't depend on scenario order),
  and seeded from a different recording than the one scored (seed): for each scored scenario, the signature at its
  trigger and the labelled root cause, as "confirmed": a stand-in for human postmortems, with no model calls. The
  incidents scored then recur incident types already in memory: memory's best case, so results are reported with and
  without it, the headline without (design §10).

    python -m drishti_agent.memory seed --api URL --timeline T --recording capture-b --postgres URL
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Optional

from .pagecheck import norm

OUTCOMES = ("confirmed", "wrong", "unconfirmed")
MIN_SIMILARITY = 0.5
TOP = 2

_KINDS = (("server error ratio", "error"), ("server p95", "latency"), ("server calls", "traffic"), ("cpu utilization", "cpu"),
          ("memory rising", "memory"))


def _kind(words: str) -> str:
    w = words.lower()
    for prefix, k in _KINDS:
        if w.startswith(prefix):
            return k
    if " error ratio " in f" {w} ":
        return "error"
    if " p95 " in f" {w} ":
        return "latency"
    if " calls " in f" {w} ":
        return "traffic"
    return "other"


def signature(pc: dict, alert: Optional[dict] = None) -> list[str]:
    """The structured shape of an incident, from page_check's table (PageCheck.to_json())."""
    sig = set()
    if alert:
        sig.add(f"page:{norm(alert.get('service'))}:{alert.get('rule', '')}")
    for c in pc.get("candidates") or []:
        svc = norm(c["service"])
        for words in c.get("own_change") or []:
            sig.add(f"own:{svc}:{_kind(words)}")
        if c.get("client_trouble"):
            sig.add(f"victim:{svc}")
    return sorted(sig)


def jaccard(a, b) -> float:
    a, b = set(a), set(b)
    return len(a & b) / len(a | b) if a | b else 0.0


class Memory:
    TABLE = "incidents"

    def __init__(self, url: str, *, source: str = "live", read_from: Optional[list[str]] = None, read_only: bool = False):
        self.url, self.source, self.read_only = url, source, read_only
        self.read_from = read_from or [source]

    def _conn(self):
        import psycopg
        return psycopg.connect(self.url, autocommit=True)

    def setup(self) -> None:
        with self._conn() as c:
            c.execute(f"""CREATE TABLE IF NOT EXISTS {self.TABLE} (
                id text PRIMARY KEY, source text NOT NULL, created_at timestamptz NOT NULL,
                signature jsonb NOT NULL, named text, fault_type text, summary text NOT NULL DEFAULT '',
                outcome text NOT NULL CHECK (outcome IN ('confirmed', 'wrong', 'unconfirmed')),
                root_cause text, outcome_by text NOT NULL DEFAULT '')""")

    def write(self, id: str, sig: list[str], named: Optional[str], fault_type: str, summary: str = "",
              outcome: str = "unconfirmed", root_cause: Optional[str] = None, by: str = "") -> bool:
        """Store a finished investigation. False (and nothing written) in read-only mode."""
        if self.read_only:
            return False
        if outcome not in OUTCOMES:
            raise ValueError(f"outcome {outcome!r}")
        with self._conn() as c:
            c.execute(f"""INSERT INTO {self.TABLE} (id, source, created_at, signature, named, fault_type, summary, outcome, root_cause, outcome_by)
                          VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
                          ON CONFLICT (id) DO UPDATE SET signature = EXCLUDED.signature, named = EXCLUDED.named,
                            fault_type = EXCLUDED.fault_type, summary = EXCLUDED.summary""",
                      (id, self.source, datetime.now(timezone.utc), json.dumps(sig), named, fault_type, summary, outcome, root_cause, by))
        return True

    def resolve(self, id: str, outcome: str, root_cause: Optional[str], by: str) -> bool:
        """Record what turned out to be true (a verified recovery, or a human's verdict)."""
        if self.read_only:
            return False
        if outcome not in OUTCOMES:
            raise ValueError(f"outcome {outcome!r}")
        with self._conn() as c:
            n = c.execute(f"UPDATE {self.TABLE} SET outcome = %s, root_cause = %s, outcome_by = %s WHERE id = %s",
                          (outcome, root_cause, by, id)).rowcount
        return n == 1

    def similar(self, sig: list[str], exclude: Optional[str] = None) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(f"""SELECT id, signature, named, fault_type, summary, outcome, root_cause FROM {self.TABLE}
                                 WHERE outcome <> 'unconfirmed' AND source = ANY(%s) AND id IS DISTINCT FROM %s""",
                             (self.read_from, exclude)).fetchall()
        scored = [{"id": r[0], "similarity": round(jaccard(sig, r[1]), 2), "named": r[2], "fault_type": r[3], "summary": r[4],
                   "outcome": r[5], "root_cause": r[6]} for r in rows]
        scored = [s for s in scored if s["similarity"] >= MIN_SIMILARITY]
        scored.sort(key=lambda s: (-s["similarity"], s["id"]))
        return scored[:TOP]

    def extra(self, alert: Optional[dict] = None):
        """Context for hypothesize (graph extras): similar past incidents, with what turned out to be true."""
        def ctx(state: dict) -> str:
            hits = self.similar(signature(state.get("pc") or {}, alert))
            if not hits:
                return ""
            lines = []
            for h in hits:
                if h["outcome"] == "confirmed":
                    lines.append(f"- similarity {h['similarity']:.2f}: root cause was {h['root_cause'] or h['named']}"
                                 f" ({h['fault_type']}), confirmed. {h['summary']}".rstrip())
                else:
                    lines.append(f"- similarity {h['similarity']:.2f}: {h['named'] or 'nobody'} was blamed and it was WRONG"
                                 + (f"; it was {h['root_cause']}" if h["root_cause"] else "") + ".")
            return ("## similar past incidents (memory; a past incident can look alike and differ: test, don't assume)\n"
                    + "\n".join(lines))
        return ctx


def seed(api_url: str, timeline: str, recording: str, mem: Memory, pc_cap: int) -> int:
    """Postmortems from a recording's labels: for each scored scenario, the signature at each trigger and the labelled
    root cause, confirmed. No model calls."""
    from .api import QueryAPI
    from .evals.labels import LABELS
    from .evals.scenarios import gate, iso, load_timeline, set_triggers
    from .pagecheck import page_check
    scenarios, base = load_timeline(timeline)
    n = 0
    for sc in scenarios:
        if sc.flag not in LABELS:
            continue
        set_triggers(api_url, sc)
        gate(api_url, sc, base)
        if not sc.gate_passed:
            continue
        lab = LABELS[sc.flag]
        root, fault = sorted(lab.services)[0], sorted(lab.faults)[0]
        for col, until in sc.triggers.items():
            if until is None:
                continue
            alert = sc.trigger_alert if col == "end-to-end" else None
            pc = page_check(QueryAPI(api_url, iso(until)), alert, cap=pc_cap).to_json()
            mem.write(f"{recording}:{sc.n}:{col}", signature(pc, alert), root, fault, summary=f"postmortem ({recording})",
                      outcome="confirmed", root_cause=root, by=f"label:{recording}")
            n += 1
    return n


def main(argv=None) -> int:
    from .pagecheck import DEFAULT_CAP
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["seed"])
    ap.add_argument("--api", required=True)
    ap.add_argument("--timeline", required=True)
    ap.add_argument("--recording", required=True)
    ap.add_argument("--postgres", required=True)
    ap.add_argument("--pagecheck-cap", type=int, default=DEFAULT_CAP)
    a = ap.parse_args(argv)
    mem = Memory(a.postgres, source=a.recording)
    mem.setup()
    n = seed(a.api, a.timeline, a.recording, mem, a.pagecheck_cap)
    print(f"seeded {n} incidents from {a.recording}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
