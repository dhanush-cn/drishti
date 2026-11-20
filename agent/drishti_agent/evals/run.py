"""The eval runner (docs/evals.md section 4).

    python -m drishti_agent.evals.run --api http://localhost:8091 --timeline timeline.jsonl --recording capture-2 \\
        --provider ollama --model qwen2.5:7b --runs 3 --out results/c2-ollama

For each scenario of the recording: triggers (agent-only, end-to-end), the
fault gate, then K runs of each system (agent, baseline) per column. Writes
runs.jsonl (every run with its transcript), scenarios.json, summary.json and
RESULTS.md.

--held-out marks the final scoring of a recording that was never used for
tuning (capture B). It refuses to run unless the prompts match the version
recorded by `python -m drishti_agent.evals.freeze` (FROZEN.json), so the
held-out number is from prompts fixed before it was seen.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from .. import agent, baseline, graph, tracing
from ..api import QueryAPI
from ..llm import from_args
from ..pagecheck import DEFAULT_CAP
from ..prompts import PROMPT_VERSION
from ..prompts_graph import GRAPH_PROMPT_VERSION
from . import report
from .labels import LABELS
from .scenarios import gate, iso, load_timeline, set_triggers
from .score import cost, score

FROZEN = Path(__file__).resolve().parents[2] / "FROZEN.json"


def git_commit() -> str:
    if os.environ.get("DRISHTI_GIT_COMMIT"):
        return os.environ["DRISHTI_GIT_COMMIT"]
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=Path(__file__).parent, text=True).strip()
    except Exception:
        return "unknown"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", required=True, help="query API over the eval database")
    ap.add_argument("--timeline", required=True)
    ap.add_argument("--recording", required=True, help="a name for the report, e.g. capture-2")
    ap.add_argument("--provider", required=True, choices=["ollama", "openai", "anthropic", "heuristic"])
    ap.add_argument("--model", required=True)
    ap.add_argument("--base-url", default="")
    ap.add_argument("--api-key-env", default="")
    ap.add_argument("--price-in", type=float, default=0.0, help="USD per million input tokens (0 = not priced)")
    ap.add_argument("--price-out", type=float, default=0.0, help="USD per million output tokens")
    ap.add_argument("--systems", default="agent,baseline", help="any of agent (v0), graph (Week 6), baseline")
    ap.add_argument("--no-f4", action="store_true", help="graph: switch rule F4 off (Week 7 comparison)")
    ap.add_argument("--pagecheck-cap", type=int, default=DEFAULT_CAP, help="graph: query calls page_check may make")
    ap.add_argument("--runbooks-postgres", default="", help="graph: runbooks on, searched in this Postgres (absent: the no-runbooks variant)")
    ap.add_argument("--memory-postgres", default="", help="graph: incident memory on, read-only, from this Postgres (absent: no memory)")
    ap.add_argument("--memory-from", default="", help="graph: memory sources to read, comma-separated recording names (never the one scored)")
    ap.add_argument("--columns", default="agent-only,end-to-end")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--budget", type=int, default=agent.DEFAULT_BUDGET)
    ap.add_argument("--scenarios", default="", help="comma-separated flags to run (default all)")
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--rpm", type=float, default=0.0, help="pace model requests to this many per minute (free tiers); 0 = no pacing")
    ap.add_argument("--reasoning-effort", default="", help="passed to OpenAI-compatible APIs (thinking models), e.g. low")
    ap.add_argument("--max-tokens", type=int, default=2048, help="output cap per model call (thinking models may need more)")
    ap.add_argument("--held-out", action="store_true", help="final scoring of an untouched recording: requires FROZEN.json")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)

    if a.held_out:
        if not FROZEN.exists():
            print("refusing --held-out: no FROZEN.json (run python -m drishti_agent.evals.freeze first)", file=sys.stderr)
            return 2
        frozen = json.loads(FROZEN.read_text())
        if frozen["prompt_version"] != PROMPT_VERSION:
            print(f"refusing --held-out: prompts changed since the freeze ({frozen['prompt_version']} -> {PROMPT_VERSION})", file=sys.stderr)
            return 2

    if a.memory_postgres and (not [x for x in a.memory_from.split(",") if x] or a.recording in a.memory_from.split(",")):
        print("--memory-from must name recordings other than the one scored", file=sys.stderr)
        return 2
    if a.provider == "heuristic" and "graph" in a.systems.split(","):
        print("the heuristic floor only answers the v0 loop's first message; run it with --systems agent", file=sys.stderr)
        return 2
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    scenarios, base = load_timeline(a.timeline)
    if a.scenarios:
        keep = set(a.scenarios.split(","))
        scenarios = [s for s in scenarios if s.flag in keep]
    llm = from_args(a.provider, a.model, a.base_url, a.api_key_env or ("ANTHROPIC_API_KEY" if a.provider == "anthropic" else ""), a.temperature,
                    rpm=a.rpm, reasoning_effort=a.reasoning_effort, max_tokens=a.max_tokens)
    meta = {"recording": a.recording, "model": getattr(llm, "name", a.model), "provider": a.provider, "prompt_version": PROMPT_VERSION,
            "graph_prompt_version": GRAPH_PROMPT_VERSION, "f4": not a.no_f4, "pagecheck_cap": a.pagecheck_cap,
            "runbooks": bool(a.runbooks_postgres), "memory_from": a.memory_from if a.memory_postgres else "",
            "git_commit": git_commit(), "runs": a.runs, "budget": a.budget, "held_out": a.held_out, "temperature": a.temperature, "rpm": a.rpm, "reasoning_effort": a.reasoning_effort, "max_tokens": a.max_tokens,
            "price_in": a.price_in, "price_out": a.price_out, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    print(json.dumps(meta))

    for sc in scenarios:
        set_triggers(a.api, sc)
        gate(a.api, sc, base)
        print(f"scenario {sc.n} {sc.flag}: gate {'PASS' if sc.gate_passed else 'fail'} ({sc.gate_detail}); "
              f"triggers {', '.join(f'{k}={iso(v) if v else None}' for k, v in sc.triggers.items())}", flush=True)
    (out / "scenarios.json").write_text(json.dumps([s.to_json() for s in scenarios], indent=1))

    systems = a.systems.split(",")
    columns = a.columns.split(",")
    gextras, gtools = [], {}
    if a.runbooks_postgres:
        from .. import runbook_tools
        idx = runbook_tools.PostgresIndex(a.runbooks_postgres)
        print(f"runbooks: {idx.build()} sections indexed", flush=True)
        gextras.append(runbook_tools.relations_extra)
        gtools.update(runbook_tools.local_tools(idx))
    mem = None
    if a.memory_postgres:
        from ..memory import Memory
        sources = [x for x in a.memory_from.split(",") if x]
        mem = Memory(a.memory_postgres, read_from=sources, read_only=True)
    with open(out / "runs.jsonl", "w") as f:
        for sc in scenarios:
            if sc.flag not in LABELS:
                continue
            for col in columns:
                until = sc.triggers.get(col)
                for sysname in systems:
                    for k in range(a.runs):
                        row = {"scenario": sc.n, "flag": sc.flag, "column": col, "system": sysname, "run": k,
                               "gate_passed": sc.gate_passed, "until": iso(until) if until else None}
                        if not sc.gate_passed:
                            # Not scored (fault gate): nothing to learn, and model time or money spent for nothing.
                            row.update(skipped=True, missed=False, score={"top1": False, "top3": False, "fault": False, "unsure": True,
                                                                           "confidently_wrong": False, "named": None, "confidence": 0.0})
                        elif until is None:
                            row.update(missed=True, score={"top1": False, "top3": False, "fault": False, "unsure": True,
                                                            "confidently_wrong": False, "named": None, "confidence": 0.0})
                        else:
                            api = QueryAPI(a.api, iso(until))
                            # End-to-end runs start from the alert that paged; agent-only runs from a report with no alert.
                            alert = sc.trigger_alert if col == "end-to-end" else None
                            if sysname == "agent":
                                r = agent.investigate(api, llm, a.budget, alert)
                            elif sysname == "graph":
                                r = graph.investigate(api, llm, a.budget, alert, f4=not a.no_f4, pc_cap=a.pagecheck_cap,
                                                      extras=gextras + ([mem.extra(alert)] if mem else []), local_tools=gtools)
                            else:
                                r = baseline.diagnose(api, llm, alert)
                            s = score(sc.flag, r)
                            row.update(missed=False, score=s.to_json(), usd=round(cost(r, a.price_in, a.price_out), 6),
                                       result=r.model_dump(mode="json"))
                            print(f"  {sc.flag} {col} {sysname} #{k}: named={s.named} top1={s.top1} top3={s.top3} conf={s.confidence:.2f} "
                                  f"tools={len(r.tool_calls)} tok={r.usage.input_tokens}+{r.usage.output_tokens} {r.seconds}s"
                                  + (f" ERROR {r.error}" if r.error else ""), flush=True)
                        f.write(json.dumps(row) + "\n")
                        f.flush()
    tracing.flush()  # LangSmith, when on: send buffered traces before the process exits
    meta["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    summary = report.write(out, meta)
    print(json.dumps(summary["totals"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
