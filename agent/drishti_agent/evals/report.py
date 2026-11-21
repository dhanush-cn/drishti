"""RESULTS.md and summary.json from runs.jsonl (docs/evals.md section 4).

Counts, not just percentages (about a dozen scored incidents per recording):
"7/11" says how much a single scenario moves the number. Scenarios that
failed the fault gate are listed as not scored, with the reason, never
silently dropped. End-to-end misses (no alert) count as wrong.
"""
from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def p90(xs: list) -> float | None:
    """Nearest-rank 90th percentile (no interpolation choices)."""
    if not xs:
        return None
    s = sorted(xs)
    return s[max(0, math.ceil(0.9 * len(s)) - 1)]


def graph_record(result: dict) -> dict:
    """The graph's state record (its last transcript entry, role "graph"), or {} for v0 and the baseline."""
    t = (result or {}).get("transcript") or []
    if t and t[-1].get("role") == "graph":
        try:
            return json.loads(t[-1]["content"])
        except (ValueError, TypeError):
            return {}
    return {}


def cost_time(rs: list, price_in: float, price_out: float) -> dict:
    """Per investigation (patch 79, Week 7 design G8): model requests, query calls, tokens, USD at the given prices
    (what it would cost on a paid tier; the free tier charges nothing), wall time, and time to diagnosis = wall time +
    the time it waited on purpose (the second look's minutes: simulated in replay, slept live). Query calls = the
    model's tool calls + page_check's calls at each look; triage's own queries (the same for every system) aren't
    counted. Median and p90 (nearest rank)."""
    reqs, queries, tin, tout, usd, wall, ttd, rechecks = [], [], [], [], [], [], [], 0
    for r in rs:
        x = r.get("result")
        if not x:
            continue
        g = graph_record(x)
        rc = g.get("recheck") or {}
        pc_calls = ((g.get("pagecheck") or {}).get("calls") or 0) + (rc.get("pagecheck_calls_first") or 0)
        wait = 60.0 * (rc.get("minutes") or 0) if rc else 0.0
        rechecks += bool(rc)
        u = x.get("usage") or {}
        reqs.append(x.get("llm_calls", 0))
        queries.append(len(x.get("tool_calls") or []) + pc_calls)
        tin.append(u.get("input_tokens", 0))
        tout.append(u.get("output_tokens", 0))
        usd.append(u.get("input_tokens", 0) / 1e6 * price_in + u.get("output_tokens", 0) / 1e6 * price_out)
        wall.append(x.get("seconds", 0.0))
        ttd.append(x.get("seconds", 0.0) + wait)
    if not reqs:
        return {"n": 0}
    m = statistics.median
    return {"n": len(reqs), "rechecks": rechecks,
            "requests_median": m(reqs), "requests_p90": p90(reqs), "requests_total": sum(reqs),
            "queries_median": m(queries), "queries_p90": p90(queries),
            "tokens_in_median": m(tin), "tokens_out_median": m(tout),
            "usd_median": round(m(usd), 6), "usd_p90": round(p90(usd), 6), "usd_total": round(sum(usd), 4),
            "wall_s_median": m(wall), "wall_s_p90": p90(wall),
            "time_to_diagnosis_s_median": m(ttd), "time_to_diagnosis_s_p90": p90(ttd)}


def write(out: Path, meta: dict) -> dict:
    with open(out / "runs.jsonl") as f:
        raw = [json.loads(l) for l in f if l.strip()]
    # A resumed run appends: the latest row of a cell (system, scenario, column, run) replaces earlier ones (a retried
    # failure). Order is kept as first seen.
    latest = {}
    for r in raw:
        latest[(r["system"], r["scenario"], r["column"], r["run"])] = r
    rows = list(latest.values())
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
    for (sys, col), rs in sorted(cells.items()):
        totals[f"{sys}/{col}"]["cost_time"] = cost_time(rs, meta.get("price_in", 0.0), meta.get("price_out", 0.0))
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
    priced = meta.get("price_in") or meta.get("price_out")
    p("## Cost and time per investigation")
    p("")
    p(f"Median / p90 over the scored runs. USD at {meta.get('price_in', 0)} / {meta.get('price_out', 0)} per million tokens in / out"
      + (f" ({meta['price_source']})" if meta.get("price_source") else "") + ": what the run would cost on a paid tier"
      + ("" if priced else " (not priced)") + ". Query calls = tool calls + page_check (triage's queries not counted). "
      "Time to diagnosis = wall time + the second look's wait (simulated in replay).")
    p("")
    p("| System / column | Model requests | Query calls | Tokens in / out (median) | USD per investigation | USD total | Wall s | Time to diagnosis s | Second looks |")
    p("|---|---|---|---|---|---|---|---|---|")
    for k, t in totals.items():
        c = t.get("cost_time") or {}
        if not c.get("n"):
            continue
        p(f"| {k} | {c['requests_median']} / {c['requests_p90']} | {c['queries_median']} / {c['queries_p90']} | "
          f"{c['tokens_in_median']} / {c['tokens_out_median']} | {c['usd_median']:.4f} / {c['usd_p90']:.4f} | {c['usd_total']:.4f} | "
          f"{c['wall_s_median']} / {c['wall_s_p90']} | {c['time_to_diagnosis_s_median']} / {c['time_to_diagnosis_s_p90']} | "
          f"{c['rechecks']}/{c['n']} |")
    p("")
    p("## Scenarios")
    p("")
    order = lambda name: ({"agent": 0, "baseline": 3}.get(name, 2 if name.startswith("agent:") else 1), name)
    systems = sorted({r["system"] for r in rows} | {"agent", "baseline"}, key=order)
    systems = [x for x in systems if x in {r["system"] for r in rows} or x in ("agent", "baseline")]
    head = [f"{x[:1].upper()}{x[1:]} ({c})" for c in ("agent-only", "end-to-end") for x in systems]
    p("| # | Flag | Gate | End-to-end trigger | " + " | ".join(head) + " |")
    p("|---|---|---|---|" + "---|" * len(head))
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
        fix = [r["fix_target"] for r in rs if "fix_target" in r]
        return f"{ok}/{len(rs)} ({', '.join(names)})" + (f"; fix on target {sum(fix)}/{len(fix)}" if fix else "")

    for s in scen:
        t = s.get("trigger_alert")
        trig = f"{t['service']} {t['rule']} {t['opened_at'][11:19]}" if t else "no alert"
        cells = [cell(s["n"], x, c) for c in ("agent-only", "end-to-end") for x in systems]
        p(f"| {s['n']} | {s['flag']} | {'scored' if s['gate_passed'] else 'NOT SCORED: ' + s['gate_detail']} | {trig} | "
          + " | ".join(cells) + " |")
    p("")
    p("Internal, provisional: replayed recordings of the OTel Demo on one laptop; see docs/evals.md for what each number means.")
    (out / "RESULTS.md").write_text("\n".join(L) + "\n")
    return summary
