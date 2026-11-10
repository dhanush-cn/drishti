"""RESULTS.md and summary.json from runs.jsonl (docs/evals.md section 4).

Counts, not just percentages (about a dozen scored incidents per recording):
"7/11" says how much a single scenario moves the number. Scenarios that
failed the fault gate are listed as not scored, with the reason, never
silently dropped. End-to-end misses (no alert) count as wrong.
"""
from __future__ import annotations

import json
import statistics
from collections import defaultdict
from pathlib import Path


def write(out: Path, meta: dict) -> dict:
    rows = [json.loads(l) for l in open(out / "runs.jsonl") if l.strip()]
    scen = json.loads((out / "scenarios.json").read_text())
    cells = defaultdict(list)
    for r in rows:
        if r["gate_passed"]:
            cells[(r["system"], r["column"])].append(r)

    def agg(rs: list) -> dict:
        n = len(rs)
        if n == 0:
            return {"runs": 0}
        s = [r["score"] for r in rs]
        results = [r.get("result") or {} for r in rs]
        tools = [len(x.get("tool_calls", [])) for x in results if x]
        toks_in = [x.get("usage", {}).get("input_tokens", 0) for x in results if x]
        toks_out = [x.get("usage", {}).get("output_tokens", 0) for x in results if x]
        secs = [x.get("seconds", 0) for x in results if x]
        return {
            "runs": n,
            "top1": sum(x["top1"] for x in s), "top3": sum(x["top3"] for x in s), "fault": sum(x["fault"] for x in s),
            "unsure": sum(x["unsure"] for x in s), "confidently_wrong": sum(x["confidently_wrong"] for x in s),
            "missed": sum(r.get("missed", False) for r in rs),
            "errors": sum(1 for x in results if x and x.get("error")),
            "invalid": sum(1 for x in results if x and not x.get("valid_answer")),
            "median_tool_calls": statistics.median(tools) if tools else None,
            "mean_input_tokens": round(statistics.mean(toks_in)) if toks_in else None,
            "mean_output_tokens": round(statistics.mean(toks_out)) if toks_out else None,
            "median_seconds": statistics.median(secs) if secs else None,
            "usd_total": round(sum(r.get("usd", 0) for r in rs), 4),
        }

    totals = {f"{sys}/{col}": agg(rs) for (sys, col), rs in sorted(cells.items())}
    summary = {"meta": meta, "totals": totals, "scored_scenarios": sum(1 for s in scen if s["gate_passed"]),
               "scenarios": len(scen)}
    (out / "summary.json").write_text(json.dumps(summary, indent=1))

    L = []
    p = L.append
    p(f"# Eval results: {meta['recording']}")
    p("")
    p(f"Model `{meta['model']}`, prompts `{meta['prompt_version']}`, code `{meta['git_commit']}`, {meta['runs']} run(s) per cell, "
      f"tool budget {meta['budget']}, temperature {meta['temperature']}{', **held-out**' if meta['held_out'] else ''}. "
      f"{meta['started']} → {meta.get('finished', '')}.")
    p("")
    p(f"Scored scenarios (fault gate passed): **{summary['scored_scenarios']} of {summary['scenarios']}**. Each cell counts runs over "
      "the scored scenarios (scenarios × runs).")
    p("")
    p("| System / column | Root cause (top-1) | Top-3 | Fault type | Unsure | Confidently wrong | Missed (no alert) | Errors / invalid | Median tool calls | Mean tokens in / out | Median s | USD |")
    p("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for k, t in totals.items():
        if not t["runs"]:
            continue
        n = t["runs"]
        p(f"| {k} | {t['top1']}/{n} | {t['top3']}/{n} | {t['fault']}/{n} | {t['unsure']}/{n} | {t['confidently_wrong']}/{n} | "
          f"{t['missed']}/{n} | {t['errors']} / {t['invalid']} | {t['median_tool_calls']} | {t['mean_input_tokens']} / {t['mean_output_tokens']} | "
          f"{t['median_seconds']} | {t['usd_total'] if meta['price_in'] or meta['price_out'] else 'not priced'} |")
    p("")
    p("## Scenarios")
    p("")
    p("| # | Flag | Gate | End-to-end trigger | Agent (agent-only) | Baseline (agent-only) | Agent (end-to-end) | Baseline (end-to-end) |")
    p("|---|---|---|---|---|---|---|---|")
    by = defaultdict(list)
    for r in rows:
        by[(r["scenario"], r["system"], r["column"])].append(r)

    def cell(n, sys, col):
        rs = by.get((n, sys, col), [])
        if not rs:
            return "–"
        if rs[0].get("skipped"):
            return "–"
        if rs[0].get("missed"):
            return "missed"
        names = [r["score"]["named"] or "unsure" for r in rs]
        ok = sum(r["score"]["top1"] for r in rs)
        return f"{ok}/{len(rs)} ({', '.join(names)})"

    for s in scen:
        t = s.get("trigger_alert")
        trig = f"{t['service']} {t['rule']} {t['opened_at'][11:19]}" if t else "no alert"
        p(f"| {s['n']} | {s['flag']} | {'scored' if s['gate_passed'] else 'NOT SCORED: ' + s['gate_detail']} | {trig} | "
          f"{cell(s['n'], 'agent', 'agent-only')} | {cell(s['n'], 'baseline', 'agent-only')} | "
          f"{cell(s['n'], 'agent', 'end-to-end')} | {cell(s['n'], 'baseline', 'end-to-end')} |")
    p("")
    p("Internal, provisional: replayed recordings of the OTel Demo on one laptop; see docs/evals.md for what each number means.")
    (out / "RESULTS.md").write_text("\n".join(L) + "\n")
    return summary
