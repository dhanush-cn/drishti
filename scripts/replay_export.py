#!/usr/bin/env python3
"""Recorded investigations for the static replay page (Week 8 design H2, H3).

Reads capture C's runs.jsonl files and writes site/runs.json: the capture's headline table and a few graph runs,
each turned into the event shapes the live dashboard streams (durable.events_for): candidates, hypotheses,
tool_calls, tool_results, verdict, judge, recheck, diagnosis, proposal. The page and the live service read one format.

Which runs (H2, fixed before looking at what it picks): graph, end-to-end, run 0, the lowest-numbered scored
scenario in each outcome class; no run 0 in a class -> run 1, then run 2. Plus one agent-only run of class 1.
  1 right_first  right at the first look (no second look)
  2 right_second unsure at the first look, right after the second
  3 conf_wrong   confidently wrong (shown with what was actually broken)
  4 unsure       unsure at the end
Rows: the latest per (system, scenario, column, run), as report.write; earlier rows of a cell were retried
infrastructure errors. Scored = fault gate passed, an alert fired, no error.

Rebuilt from what a row keeps, nothing invented:
- The row keeps page_check's final table only. After a second look the first look's table is gone, so the
  candidates event comes after the recheck event (marked look 2) and the first look has none.
- Eval runs have remediation off: no proposal was made. The proposal event is remediate.propose(diagnosis), the
  same lookup the live service runs, marked "not executed".
- Tool output is cut to durable._summary (160 characters).

Safety: the output must not carry a secret. Every string is searched for API-key shapes; any hit -> nothing is
written, exit 2. It prints "secrets: 0" on success.

    cd agent && python ../scripts/replay_export.py \\
        --replay-from <capture-c>/graph-011b9a0338f5/runs.jsonl \\
        --headline graph=<capture-c>/graph-011b9a0338f5/runs.jsonl --headline v0=<...>/v0-.../runs.jsonl \\
        --headline baseline=<...> --headline floor=<...> --out ../site/runs.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

from drishti_agent.durable import _summary
from drishti_agent.evals.labels import LABELS
from drishti_agent.remediate import propose

CLASSES = [("right_first", "Right at the first look"),
           ("right_second", "Unsure at the first look, right after the second"),
           ("conf_wrong", "Confidently wrong"),
           ("unsure", "Unsure at the end")]
CONTROL_TOOLS = {"submit_hypotheses", "mark_hypothesis"}   # the graph's own protocol, not queries

SECRET = re.compile(r"AIza[0-9A-Za-z_\-]{20,}|sk-[A-Za-z0-9_\-]{16,}|GEMINI_API_KEY|OPENAI_API_KEY|"
                    r"Bearer\s+[A-Za-z0-9._\-]{16,}|(?i:api[_-]?key)\s*[=:]\s*\S{8,}")


# ---- rows -----------------------------------------------------------------------------------------------------

def read_rows(path: Path) -> list[dict]:
    """The latest row per cell, in first-seen order (report.write's rule)."""
    latest = {}
    with open(path) as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                latest[(r["system"], r["scenario"], r["column"], r["run"])] = r
    return list(latest.values())


def scored(r: dict) -> bool:
    return bool(r.get("gate_passed")) and not r.get("missed") and bool(r.get("result")) and not r["result"].get("error")


def graph_record(result: dict) -> dict:
    t = (result or {}).get("transcript") or []
    if t and t[-1].get("role") == "graph":
        try:
            return json.loads(t[-1]["content"]) or {}
        except (ValueError, TypeError):
            return {}
    return {}


def outcome(r: dict) -> str | None:
    """The H2 class of a scored row, or None (wrong but not confident: not a class on the page)."""
    s = r["score"]
    second = bool(graph_record(r["result"]).get("recheck"))
    if s["top1"]:
        return "right_second" if second else "right_first"
    if s["confidently_wrong"]:
        return "conf_wrong"
    if s["unsure"]:
        return "unsure"
    return None


def pick(rows: list[dict]) -> list[tuple[str, dict]]:
    """H2: per class, end-to-end, the lowest scenario at run 0 (else run 1, else 2); plus agent-only class 1."""
    graph = [r for r in rows if str(r["system"]).split(":")[0] == "graph" and scored(r)]
    out = []

    def first(column: str, cls: str):
        for k in (0, 1, 2):
            c = sorted((r for r in graph if r["column"] == column and r["run"] == k and outcome(r) == cls),
                       key=lambda r: r["scenario"])
            if c:
                return c[0]
        return None

    for cls, _ in CLASSES:
        r = first("end-to-end", cls)
        if r:
            out.append((cls, r))
    r = first("agent-only", "right_first")
    if r:
        out.append(("right_first", r))
    return out


# ---- events ---------------------------------------------------------------------------------------------------

def _candidates(pc: dict, look: int) -> dict:
    return {"type": "candidates", "look": look, "paged": pc.get("paged"), "p_onset": pc.get("p_onset"),
            "calls": pc.get("calls"),
            "candidates": [{k: c.get(k) for k in ("service", "relation", "depth", "own_change", "onset", "trend")}
                           for c in pc.get("candidates") or []]}


def _tool_id(text: str) -> tuple[str | None, str]:
    m = re.match(r"\[(t\d+)\]\s?", text or "")
    return (m.group(1), text[m.end():]) if m else (None, text or "")


def events_from_row(r: dict) -> list[dict]:
    """The row's investigation as the live service's events, in order."""
    res = r["result"]
    rec = graph_record(res)
    hyps = rec.get("hypotheses") or []
    by_id = {h["id"]: h for h in hyps}
    verdicts = list(rec.get("verdicts") or [])
    used_v = [False] * len(verdicts)
    recheck = rec.get("recheck")
    pc = rec.get("pagecheck") or {}
    transcript = [m for m in res.get("transcript") or [] if m.get("role") != "graph"]
    oks = [t.get("ok") for t in res.get("tool_calls") or []]
    tool_msgs = [m for m in transcript if m.get("role") == "tool" and m.get("name") not in CONTROL_TOOLS]
    ok_aligned = len(oks) == len(tool_msgs)     # tool_log and the transcript agree, else "ok" is left unknown
    ti = 0

    def take(pred) -> str | None:
        for i, v in enumerate(verdicts):
            if not used_v[i] and pred(v):
                used_v[i] = True
                return v
        return None

    # Last test phase of each hypothesis: its final status belongs there.
    tests = [m["content"].split()[1] for m in transcript
             if m.get("role") == "phase" and m["content"].startswith("test ")]
    last_test = {h: i for i, h in enumerate(tests)}

    ev: list[dict] = []
    if not recheck and pc:
        ev.append(_candidates(pc, 1))
    current, test_n = None, -1
    shown_h, seen_rounds = set(), set()

    def close_test():
        if current is None:
            return
        h = by_id.get(current, {})
        judge = take(lambda v: v.startswith(f"{current} "))
        e = {"type": "verdict", "hypothesis": current, "service": h.get("service"), "judge": judge}
        if last_test.get(current) == test_n:
            e.update(status=h.get("status"), confidence=h.get("confidence"), evidence=h.get("evidence"),
                     summary=_summary(h.get("summary") or ""))
        ev.append(e)
        if judge and "refused twice" not in judge:
            stop = take(lambda v: v.startswith("refused twice"))
            if stop:
                ev.append({"type": "judge", "verdict": stop})

    for m in transcript:
        role = m.get("role")
        if role == "phase":
            text = m["content"]
            close_test()
            current = None
            if text.startswith("hypothesize, round "):
                rnd = int(text.rsplit(" ", 1)[1])
                new = [h for h in hyps if h.get("round") == rnd and h["id"] not in shown_h]
                shown_h.update(h["id"] for h in new)
                if new or rnd not in seen_rounds:     # a retried call in the same round adds nothing new
                    ev.append({"type": "hypotheses", "round": rnd,
                               "items": [{"id": h["id"], "service": h["service"], "fault_type": h.get("fault_type"),
                                          "prior": h.get("prior"), "why": _summary(h.get("why") or "")} for h in new]})
                seen_rounds.add(rnd)
            elif text.startswith("test "):
                current = text.split()[1]
                test_n += 1
            elif text.startswith("recheck at ") and recheck:
                ev.append({"type": "recheck", "until": recheck.get("until"), "minutes": recheck.get("minutes"),
                           "changed": recheck.get("changed"), "superseded": recheck.get("superseded"),
                           "judge": take(lambda v: v.startswith("first look unsure"))})
                if pc:
                    ev.append(_candidates(pc, 2))
        elif role == "assistant" and m.get("tool_calls"):
            calls = [{"name": c["name"], "arguments": c.get("arguments")} for c in m["tool_calls"]
                     if c.get("name") not in CONTROL_TOOLS]
            if calls:
                ev.append({"type": "tool_calls", "hypothesis": current, "calls": calls})
        elif role == "tool" and m.get("name") not in CONTROL_TOOLS:
            tid, body = _tool_id(m.get("content") or "")
            ev.append({"type": "tool_results", "results": [{"id": tid, "name": m.get("name"),
                                                            "ok": oks[ti] if ok_aligned else None,
                                                            "summary": _summary(body)}]})
            ti += 1
    close_test()
    for i, v in enumerate(verdicts):        # what's left: stops, budget ends
        if not used_v[i]:
            ev.append({"type": "judge", "verdict": v})

    d = res.get("diagnosis") or {}
    ev.append({"type": "diagnosis", "valid": res.get("valid_answer"),
               "diagnosis": {"root_cause_service": d.get("root_cause_service"), "fault_type": d.get("fault_type"),
                             "confidence": d.get("confidence"), "ranked_services": (d.get("ranked_services") or [])[:3],
                             "summary": _summary(d.get("summary") or "", 300),
                             "ruled_out": [_summary(x) for x in d.get("ruled_out") or []]}})
    p = propose(d)
    ev.append({"type": "proposal", "proposal": p, "executed": False,
               "note": "remediation is off in eval runs: this is what the live service would propose, not executed"})
    return ev


def alert_line(r: dict) -> str | None:
    """What the agent was told first: the briefing's opening line (end-to-end: the page; agent-only: no alert)."""
    for m in (r.get("result") or {}).get("transcript") or []:
        if m.get("role") == "user":
            line = (m.get("content") or "").split("\n", 1)[0]
            first = line.split(". ", 1)[0]          # the first sentence: timestamps have dots but no ". "
            return _summary(first if first.endswith(".") else first + ".", 300)
    return None


def replay_entry(cls: str, r: dict) -> dict:
    res = r["result"]
    u = res.get("usage") or {}
    return {"class": cls, "class_label": dict(CLASSES)[cls], "flag": r["flag"], "scenario": r["scenario"],
            "column": r["column"], "run": r["run"], "system": r["system"],
            "label_services": sorted(LABELS[r["flag"]].services), "score": r["score"], "alert": alert_line(r),
            "stats": {"model_requests": res.get("llm_calls"), "tool_calls": len(res.get("tool_calls") or []),
                      "seconds": res.get("seconds"), "tokens_in": u.get("input_tokens"), "tokens_out": u.get("output_tokens")},
            "events": events_from_row(r)}


# ---- headline -------------------------------------------------------------------------------------------------

def tally(rows: list[dict], column: str) -> dict:
    """Counts over gate-passed rows (report.write's cells: misses and errors count as wrong)."""
    rs = [r for r in rows if r.get("gate_passed") and r["column"] == column]
    s = [r["score"] for r in rs]
    return {"n": len(rs), "top1": sum(x["top1"] for x in s), "top3": sum(x["top3"] for x in s),
            "unsure": sum(x["unsure"] for x in s), "confidently_wrong": sum(x["confidently_wrong"] for x in s),
            "invalid": sum(1 for r in rs if r.get("result") and not r["result"].get("valid_answer")),
            # A system that states the same number for every answer (the floor: 0.5) gives no confidence, so
            # "confidently wrong" doesn't apply to it (docs/evals.md section 12 prints "-").
            "gives_confidence": len({x.get("confidence") for x in s if x.get("named")}) > 1}


def headline(named: list[tuple[str, list[dict]]]) -> list[dict]:
    return [{"system": name, "end_to_end": tally(rows, "end-to-end"), "agent_only": tally(rows, "agent-only")}
            for name, rows in named]


# ---- secrets --------------------------------------------------------------------------------------------------

def strings(x):
    if isinstance(x, str):
        yield x
    elif isinstance(x, dict):
        for k, v in x.items():
            yield str(k)
            yield from strings(v)
    elif isinstance(x, list):
        for v in x:
            yield from strings(v)


def secret_hits(doc) -> int:
    return sum(len(SECRET.findall(s)) for s in strings(doc))


# ---- main -----------------------------------------------------------------------------------------------------

def md5(p: Path) -> str:
    return hashlib.md5(p.read_bytes()).hexdigest()


def build(replay_from: Path, heads: list[tuple[str, Path]]) -> dict:
    rows = read_rows(replay_from)
    named = [(n, read_rows(p)) for n, p in heads]
    return {"version": 1,
            "rule": "graph, end-to-end, run 0 (else 1, else 2), lowest-numbered scored scenario per outcome class; "
                    "plus one agent-only run right at the first look (Week 8 design H2)",
            "sources": [{"name": n, "file": p.parent.name + "/" + p.name, "md5": md5(p)} for n, p in
                        [("replay", replay_from)] + heads],
            "headline": headline(named),
            "runs": [replay_entry(c, r) for c, r in pick(rows)]}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--replay-from", type=Path, required=True, help="the graph's runs.jsonl to pick replays from")
    ap.add_argument("--headline", action="append", default=[], metavar="NAME=RUNS.JSONL",
                    help="a headline row (repeatable, in table order)")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)
    heads = []
    for h in a.headline:
        name, _, path = h.partition("=")
        if not path:
            ap.error(f"--headline wants NAME=PATH, got {h!r}")
        heads.append((name, Path(path)))
    doc = build(a.replay_from, heads)
    n = secret_hits(doc)
    print(f"secrets: {n}")
    if n:
        print("refusing to write: something in the output looks like an API key", file=sys.stderr)
        return 2
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    for r in doc["runs"]:
        print(f"  {r['class']:13} {r['column']:10} {r['flag']} run {r['run']}: {len(r['events'])} events")
    for h in doc["headline"]:
        e, g = h["end_to_end"], h["agent_only"]
        print(f"  {h['system']:9} e2e {e['top1']}/{e['n']} top-3 {e['top3']} unsure {e['unsure']} cw {e['confidently_wrong']}"
              f" | agent-only {g['top1']}/{g['n']} top-3 {g['top3']} cw {g['confidently_wrong']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
