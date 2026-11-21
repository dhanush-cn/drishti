"""The ceiling check for design rule F4, before any model runs under it (design note section 4).

No model calls. For every scenario that passes the fault gate, using only its trigger alert, page_check and
the rule, against the labelled root-cause service:

  1. is the labelled service in the candidate table? reached from the page by a call edge (depth <= 3), or
     only as "changed (no edge)"? (with the page_check cap, and with no practical cap)
  2. would F4 accept it as the answer?          -> items 1-2 = the rule's ceiling on the end-to-end column
  3. would F4 accept each WRONG service the Week 5 agent named? (from runs.jsonl: what the rule would block)
  4. query calls page_check used (with no practical cap: what a full walk needs)
  5. onset error: detected onset minus flag-on, for the labelled service and for the paging service

Agent-only gets item 1 only: F4 doesn't apply there (no page).

    python -m drishti_agent.evals.rulecheck --api http://127.0.0.1:8091 --timeline T --recording capture-2 \\
        --runs results/capture-2/hosted/runs.jsonl --out results/capture-2/rulecheck

Capture B may be checked too: it is "seen" for Week 6 (design F13), and this checks rule against label,
it tunes no prompt.

Week 7 (patch 75; design note Week 7, sections 3-4), also no model calls:

  --recheck 1,2,3   G2: page_check again at the end-to-end trigger + N minutes (2 is the design's delay; 1 and 3 are
                    for information): is the labelled service in the table with an own-signal change by then, and is
                    that moment still before flag-off?
  --stale           G3': would the stale-change guard reject the labelled service, at the trigger or at trigger +
                    --gate-minutes? Pass = 0 rejected (the condition set on 21 Nov). And with --wrong-runs, every
                    committed wrong end-to-end answer in those results files, checked at its own run's until: how many
                    the guard would have caught (confidently wrong ones counted separately).

    python -m drishti_agent.evals.rulecheck ... --recheck 1,2,3 --stale \
        --wrong-runs results/capture-2/graph-hosted/runs.jsonl --wrong-runs ...
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Optional

from ..api import QueryAPI
from ..pagecheck import DEFAULT_CAP, PageCheck, norm, page_check, parse_ts, reassignment, stale_change
from .labels import LABELS
from .scenarios import gate, iso, load_timeline, set_triggers

FULL_CAP = 60   # "no practical cap": enough for every walk the demo can produce


def v0_wrong_answers(paths: list[str]) -> dict[str, list[str]]:
    """flag -> services the Week 5 agent named wrongly, end-to-end (one entry per run)."""
    out: dict[str, list[str]] = {}
    for p in paths:
        with open(p) as f:
            rows = [json.loads(line) for line in f if line.strip()]
        for r in rows:
            if r.get("system") != "agent" or r.get("column") != "end-to-end" or r.get("skipped") or r.get("missed"):
                continue
            s = r.get("score") or {}
            if not s.get("top1") and s.get("named"):
                out.setdefault(r["flag"], []).append(s["named"])
    return out


def source_of(path: str) -> str:
    """A short name for a results file: the kit and the folder under results/ (week6:capture-2/graph-hosted)."""
    parts = Path(path).resolve().parts
    if "results" in parts:
        i = len(parts) - 1 - parts[::-1].index("results")
        kit = parts[i - 1].replace("drishti-", "") if i > 0 else ""
        return f"{kit}:{'/'.join(parts[i + 1:-1])}"
    return str(Path(path).parent.name)


def wrong_answers(paths: list[str]) -> dict[str, list[dict]]:
    """flag -> every committed wrong end-to-end answer (agent or graph rows, any label; never the baseline), one per row."""
    out: dict[str, list[dict]] = {}
    for p in paths:
        src = source_of(p)
        with open(p) as f:
            rows = [json.loads(line) for line in f if line.strip()]
        for r in rows:
            if r.get("column") != "end-to-end" or r.get("skipped") or r.get("missed") or r.get("system", "").startswith("baseline"):
                continue
            s = r.get("score") or {}
            if s.get("named") and not s.get("top1"):
                out.setdefault(r["flag"], []).append({"source": src, "system": r["system"], "run": r.get("run"), "until": r["until"],
                                                      "named": s["named"], "confidence": s.get("confidence"),
                                                      "confidently_wrong": bool(s.get("confidently_wrong"))})
    return out


def labelled(pc: PageCheck, services) -> Optional[dict]:
    """The best-placed labelled service in the table (two flags accept two names)."""
    rank = {"paged": 0, "callee": 1, "start": 1, "changed (no edge)": 2}
    best = None
    for s in services:
        c = pc.get(s)
        if c and (best is None or rank.get(c.relation, 3) < rank.get(best.relation, 3)):
            best = c
    if best is None:
        return None
    return {"service": best.service, "relation": best.relation, "depth": best.depth, "own_change": best.own_change,
            "onset": best.onset, "trend": best.trend, "note": best.note}


def secs(a: Optional[str], b) -> Optional[int]:
    return None if not a else int((parse_ts(a) - b).total_seconds())


def check_scenario(base: str, sc, wrong: dict[str, list[str]], cap: int) -> dict:
    lab = LABELS[sc.flag]
    row: dict = {"n": sc.n, "flag": sc.flag, "labels": sorted(lab.services), "gate_passed": sc.gate_passed,
                 "gate_detail": sc.gate_detail, "alert": sc.trigger_alert}
    if not sc.gate_passed:
        return row
    # Agent-only: is the labelled service among the candidates walked from the top changed services?
    ao = page_check(QueryAPI(base, iso(sc.triggers["agent-only"])), None, cap=FULL_CAP)
    row["agent_only"] = {"labelled": labelled(ao, lab.services), "calls": ao.calls}
    if not sc.trigger_alert:
        row["end_to_end"] = {"missed": True}
        return row
    api = QueryAPI(base, iso(sc.triggers["end-to-end"]))
    full = page_check(api, sc.trigger_alert, cap=FULL_CAP)
    capped = page_check(api, sc.trigger_alert, cap=cap)
    lab_capped = labelled(capped, lab.services)
    verdicts = [reassignment(s, capped) for s in sorted(lab.services)]
    accepted = any(ok for ok, _ in verdicts)
    blocked = []
    for w in wrong.get(sc.flag, []):
        ok, why = reassignment(w, capped)
        blocked.append({"named": w, "f4_accepts": ok, "why": why})
    row["end_to_end"] = {
        "paged": capped.paged, "rule": capped.p_rule, "p_onset": capped.p_onset, "p_onset_how": capped.p_onset_how,
        "p_onset_after_flag_s": secs(capped.p_onset, sc.on),
        "labelled_full": labelled(full, lab.services), "labelled_capped": lab_capped,
        "labelled_onset_after_flag_s": secs(lab_capped["onset"], sc.on) if lab_capped else None,
        "f4_accepts_label": accepted, "f4_why": [why for _, why in verdicts],
        "calls_full": full.calls, "calls_capped": capped.calls, "capped": capped.capped,
        "v0_wrong": blocked, "table": capped.table()}
    return row


def check_week7(base: str, sc, wrong: dict[str, list[dict]], cap: int, recheck: list[int], stale: bool, gate_minutes: int) -> dict:
    """G2's recheck and G3''s stale guard against the label, at the end-to-end trigger (no model calls)."""
    lab = LABELS[sc.flag]
    t0, alert = sc.triggers["end-to-end"], sc.trigger_alert
    at = {0: t0}
    for m in sorted(set(recheck + ([gate_minutes] if stale else []))):
        at[m] = t0 + timedelta(minutes=m)
    pcs = {m: page_check(QueryAPI(base, iso(t)), alert, cap=cap) for m, t in at.items()}
    out: dict = {"paged": pcs[0].paged, "p_onset": pcs[0].p_onset, "recheck": {}}
    for m in [0] + sorted(recheck):
        l = labelled(pcs[m], lab.services)
        out["recheck"][str(m)] = {"until": iso(at[m]), "before_off": at[m] < sc.off, "labelled": l,
                                  "own_change": bool(l and l["own_change"]), "calls": pcs[m].calls, "capped": pcs[m].capped,
                                  "p_onset": pcs[m].p_onset}
    if not stale:
        return out
    checks = []
    for m in (0, gate_minutes):
        for svc in sorted(lab.services):
            st = stale_change(QueryAPI(base, iso(at[m])), svc, pcs[m].paged, pcs[m].p_onset)
            checks.append({"minutes": m, "service": svc, **st.to_json()})
    out["label_stale"] = checks
    out["label_rejected"] = [c for c in checks if c["stale"]]
    caught = []
    for w in wrong.get(sc.flag, []):
        until = parse_ts(w["until"])
        p_onset = pcs[0].p_onset if until == t0 else page_check(QueryAPI(base, w["until"]), alert, cap=cap).p_onset
        st = stale_change(QueryAPI(base, w["until"]), w["named"], pcs[0].paged, p_onset)
        caught.append({**w, "applies": st.applies, "stale": st.stale, "why": st.why, "calls": st.calls,
                       "signals": [x for x in st.to_json()["signals"]]})
    out["wrong"] = caught
    return out


def week7_summary(rows: list[dict], recording: str, recheck: list[int], stale: bool, gate_minutes: int) -> str:
    e2e = [r for r in rows if r.get("week7")]
    out = [f"## Week 7 ceiling check (G2 recheck, G3' stale guard): {recording}", ""]
    if stale:
        rej = [(r["n"], r["flag"], c) for r in e2e for c in r["week7"]["label_rejected"]]
        out += [f"- **G3' on the labelled service (at the trigger and at +{gate_minutes} min): {len(rej)} rejected -> "
                f"{'PASS' if not rej else 'FAIL'}** (condition: 0 on capture 2 and 0 on capture B).", ]
        for n, flag, c in rej:
            out.append(f"  - REJECTED: #{n} {flag} {c['service']} at +{c['minutes']} min: {c['why']}")
        ws = [w for r in e2e for w in r["week7"]["wrong"]]
        by: dict[str, dict] = {}
        for w in ws:
            b = by.setdefault(w["source"], {"cw": 0, "cw_caught": 0, "other": 0, "other_caught": 0, "paged": 0})
            if not w["applies"]:
                b["paged"] += 1
            k = "cw" if w["confidently_wrong"] else "other"
            b[k] += 1
            b[k + "_caught"] += bool(w["stale"])
        out += ["", "**Catches: committed wrong end-to-end answers in past results, checked at their own until.**", "",
                "| source | confidently wrong: caught / total | other wrong: caught / total | of which name the page (not checkable) |",
                "|---|---|---|---|"]
        for src in sorted(by):
            b = by[src]
            out.append(f"| {src} | {b['cw_caught']} / {b['cw']} | {b['other_caught']} / {b['other']} | {b['paged']} |")
        if not by:
            out.append("| (no wrong answers given) | | | |")
        out += ["", "| # | flag | source | named (conf) | guard | why |", "|---|---|---|---|---|---|"]
        for r in e2e:
            for w in r["week7"]["wrong"]:
                verdict = "n/a" if not w["applies"] else ("CAUGHT" if w["stale"] else "passes")
                out.append(f"| {r['n']} | {r['flag']} | {w['source']} | {w['named']} ({w['confidence']}) | {verdict} | {w['why']} |")
        out.append("")
    if recheck:
        mins = [0] + sorted(recheck)
        out += [f"**G2: is the labelled service in the candidate table with an own-signal change?** (+N min after the trigger; "
                "'after off' = that moment is past flag-off)", "",
                "| # | flag | page | " + " | ".join(f"+{m} min" for m in mins) + f" | calls at +{max(mins)} |",
                "|---|---|---|" + "---|" * len(mins) + "---|"]
        for r in e2e:
            w7 = r["week7"]
            cells = []
            for m in mins:
                c = w7["recheck"][str(m)]
                l = c["labelled"]
                txt = "not in table" if not l else (f"{l['relation']}, " + ("own change" if l["own_change"] else "no own change"))
                cells.append(txt + ("" if c["before_off"] else " (after off)"))
            out.append(f"| {r['n']} | {r['flag']} | {w7['paged']} | " + " | ".join(cells) + f" | {w7['recheck'][str(max(mins))]['calls']} |")
        out.append("")
    return "\n".join(out) + "\n"


def summary(rows: list[dict], recording: str, cap: int) -> str:
    scored = [r for r in rows if r["gate_passed"]]
    e2e = [r for r in scored if r.get("end_to_end") and not r["end_to_end"].get("missed")]
    out = [f"# F4 ceiling check: {recording}", "",
           f"Scored scenarios: {len(scored)} of {len(rows)}; end-to-end with an alert: {len(e2e)}. page_check cap: {cap}.", ""]
    acc = sum(r["end_to_end"]["f4_accepts_label"] for r in e2e)
    reach = sum(bool(r["end_to_end"]["labelled_capped"]) for r in e2e)
    edge = sum(bool(r["end_to_end"]["labelled_capped"]) and r["end_to_end"]["labelled_capped"]["relation"] in ("paged", "callee") for r in e2e)
    ao = sum(bool(r["agent_only"]["labelled"]) for r in scored)
    v0 = [b for r in e2e for b in r["end_to_end"]["v0_wrong"]]
    out += [f"- **Ceiling (end-to-end): F4 accepts the labelled service in {acc}/{len(e2e)}** scenarios with an alert "
            f"(misses and unscored excluded).",
            f"- Labelled service in the candidate table: {reach}/{len(e2e)} (by a call edge or paged: {edge}).",
            f"- Agent-only: labelled service among the candidates in {ao}/{len(scored)}.",
            f"- Week 5 agent's wrong end-to-end answers: {len(v0)}; F4 would refuse {sum(not b['f4_accepts'] for b in v0)}.",
            f"- page_check query calls (end-to-end): full walk max {max((r['end_to_end']['calls_full'] for r in e2e), default=0)}, "
            f"capped runs that hit the cap: {sum(r['end_to_end']['capped'] for r in e2e)}.", "",
            "| # | flag | page (rule) | P onset vs flag-on | labelled: where | onset vs flag-on | trend | F4 | calls full/capped | v0 wrong → F4 |",
            "|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        if not r["gate_passed"]:
            out.append(f"| {r['n']} | {r['flag']} | not scored: {r['gate_detail'][:60]} | | | | | | | |")
            continue
        e = r.get("end_to_end") or {}
        if e.get("missed"):
            out.append(f"| {r['n']} | {r['flag']} | no alert (missed) | | | | | | | |")
            continue
        lab = e["labelled_capped"]
        where = "not in table" if not lab else (lab["relation"] + (f" d{lab['depth']}" if lab["relation"] == "callee" else ""))
        if not lab and e["labelled_full"]:
            where += f" (full walk: {e['labelled_full']['relation']})"
        fmt = lambda s: "?" if s is None else f"{s:+d}s"
        v0 = ", ".join(f"{b['named']}→{'ok' if b['f4_accepts'] else 'refused'}" for b in e["v0_wrong"]) or "-"
        out.append(f"| {r['n']} | {r['flag']} | {e['paged']} ({e['rule']}) | {fmt(e['p_onset_after_flag_s'])} | {where} | "
                   f"{fmt(e['labelled_onset_after_flag_s'])} | {(lab or {}).get('trend') or '-'} | "
                   f"{'accept' if e['f4_accepts_label'] else 'REFUSE'} | {e['calls_full']}/{e['calls_capped']} | {v0} |")
    out += ["", "F4 reasons and candidate tables: rulecheck.json."]
    return "\n".join(out) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", required=True)
    ap.add_argument("--timeline", required=True)
    ap.add_argument("--recording", required=True)
    ap.add_argument("--runs", action="append", default=[], help="Week 5 runs.jsonl (repeatable): wrong answers to test F4 on")
    ap.add_argument("--cap", type=int, default=DEFAULT_CAP)
    ap.add_argument("--recheck", default="", help="Week 7, G2: minutes after the end-to-end trigger to run page_check again, e.g. 1,2,3")
    ap.add_argument("--stale", action="store_true", help="Week 7, G3': the stale-change guard against the label (and --wrong-runs)")
    ap.add_argument("--gate-minutes", type=int, default=2, help="G3' is also checked at the trigger + this many minutes (G2's delay)")
    ap.add_argument("--wrong-runs", action="append", default=[], help="runs.jsonl (repeatable): committed wrong answers to test G3' on")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    scenarios, base = load_timeline(a.timeline)
    wrong = v0_wrong_answers(a.runs)
    recheck = [int(x) for x in a.recheck.split(",") if x.strip()]
    wrong7 = wrong_answers(a.wrong_runs)
    week7 = bool(recheck or a.stale)
    rows = []
    for sc in scenarios:
        if sc.flag not in LABELS:
            continue
        set_triggers(a.api, sc)
        gate(a.api, sc, base)
        r = check_scenario(a.api, sc, wrong, a.cap)
        e = r.get("end_to_end") or {}
        if week7 and e and not e.get("missed"):
            r["week7"] = check_week7(a.api, sc, wrong7, a.cap, recheck, a.stale, a.gate_minutes)
        print(f"scenario {sc.n} {sc.flag}: gate {'PASS' if sc.gate_passed else 'fail'}"
              + (f"; F4 {'accepts' if e.get('f4_accepts_label') else 'REFUSES'} label; calls {e.get('calls_full')}/{e.get('calls_capped')}"
                 if e and not e.get("missed") else ""), flush=True)
        rows.append(r)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "rulecheck.json").write_text(json.dumps({"recording": a.recording, "cap": a.cap, "rows": rows}, indent=1))
    md = summary(rows, a.recording, a.cap)
    if week7:
        md += "\n" + week7_summary(rows, a.recording, recheck, a.stale, a.gate_minutes)
    (out / "RULECHECK.md").write_text(md)
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
