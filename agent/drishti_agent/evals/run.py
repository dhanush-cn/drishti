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

from .. import agent, baseline
from ..api import QueryAPI
from ..llm import from_args
from ..prompts import PROMPT_VERSION
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
    ap.add_argument("--systems", default="agent,baseline")
    ap.add_argument("--columns", default="agent-only,end-to-end")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--budget", type=int, default=agent.DEFAULT_BUDGET)
    ap.add_argument("--scenarios", default="", help="comma-separated flags to run (default all)")
    ap.add_argument("--temperature", type=float, default=0.0)
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

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    scenarios, base = load_timeline(a.timeline)
    if a.scenarios:
        keep = set(a.scenarios.split(","))
        scenarios = [s for s in scenarios if s.flag in keep]
    llm = from_args(a.provider, a.model, a.base_url, a.api_key_env or ("ANTHROPIC_API_KEY" if a.provider == "anthropic" else ""), a.temperature)
    meta = {"recording": a.recording, "model": getattr(llm, "name", a.model), "provider": a.provider, "prompt_version": PROMPT_VERSION,
            "git_commit": git_commit(), "runs": a.runs, "budget": a.budget, "held_out": a.held_out, "temperature": a.temperature,
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
                            r = agent.investigate(api, llm, a.budget) if sysname == "agent" else baseline.diagnose(api, llm)
                            s = score(sc.flag, r)
                            row.update(missed=False, score=s.to_json(), usd=round(cost(r, a.price_in, a.price_out), 6),
                                       result=r.model_dump(mode="json"))
                            print(f"  {sc.flag} {col} {sysname} #{k}: named={s.named} top1={s.top1} top3={s.top3} conf={s.confidence:.2f} "
                                  f"tools={len(r.tool_calls)} tok={r.usage.input_tokens}+{r.usage.output_tokens} {r.seconds}s"
                                  + (f" ERROR {r.error}" if r.error else ""), flush=True)
                        f.write(json.dumps(row) + "\n")
                        f.flush()
    meta["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    summary = report.write(out, meta)
    print(json.dumps(summary["totals"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
