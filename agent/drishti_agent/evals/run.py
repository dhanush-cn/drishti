"""The eval runner (docs/evals.md section 4).

    python -m drishti_agent.evals.run --api http://localhost:8091 --timeline timeline.jsonl --recording capture-2 \\
        --provider ollama --model qwen2.5:7b --runs 3 --out results/c2-ollama

For each scenario of the recording: triggers (agent-only, end-to-end), the
fault gate, then K runs of each system (agent, baseline) per column. Writes
runs.jsonl (every run with its transcript), scenarios.json, summary.json and
RESULTS.md.

--ledger FILE records every model request and stops the run at a daily cap of requests per model
(patch 74, drishti_agent.ledger): like a rate limit that outlasted its waits, rerun with --resume.

--held-out marks the final scoring of a recording that was never used for
tuning (capture B). It refuses to run unless the prompts match the version
recorded by `python -m drishti_agent.evals.freeze` (FROZEN.json), so the
held-out number is from prompts fixed before it was seen.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from .. import agent, baseline, graph, tracing
from ..api import QueryAPI
from ..ledger import DEFAULT_MAX_REQUESTS, CapReached, Ledger, Metered
from ..llm import from_args
from ..pagecheck import DEFAULT_CAP
from ..prompts import PROMPT_VERSION
from ..prompts_graph import GRAPH_PROMPT_VERSION
from . import report
from .labels import LABELS
from .scenarios import gate, iso, load_timeline, set_triggers
from .score import cost, score

FROZEN = Path(__file__).resolve().parents[2] / "FROZEN.json"
PRICES = Path(__file__).resolve().parents[2] / "prices.json"


def price_for(model: str, path: Path = PRICES) -> tuple[float, float, str]:
    """USD per million tokens in / out for a model, from agent/prices.json (patch 79): what a run would cost on a paid
    tier. (0, 0, "") when the file has no entry whose name appears in the model's name."""
    if not path.exists():
        return 0.0, 0.0, ""
    for name, p in json.loads(path.read_text()).items():
        if name in model:
            return float(p["input_per_million"]), float(p["output_per_million"]), p.get("source", "")
    return 0.0, 0.0, ""


def git_commit() -> str:
    if os.environ.get("DRISHTI_GIT_COMMIT"):
        return os.environ["DRISHTI_GIT_COMMIT"]
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=Path(__file__).parent, text=True).strip()
    except Exception:
        return "unknown"


def code_hash() -> str:
    """sha256 of the agent's code and data (every file under agent/drishti_agent but caches), by path and content.
    Independent of git, FROZEN.json and the docs, so committing the freeze doesn't change it (patch 80)."""
    import hashlib
    root = Path(__file__).resolve().parents[1]
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc":
            h.update(str(p.relative_to(root)).encode() + b"\x00" + p.read_bytes() + b"\x00")
    return h.hexdigest()[:16]


def settings_signature(a) -> dict:
    """The settings that make a run one variant and not another (patch 80; evals/variants.py)."""
    from .variants import signature
    recheck = None if a.recheck_minutes.strip().lower() == "off" else float(a.recheck_minutes)
    return signature(systems=a.systems, label=a.label, recheck_minutes=recheck, budget=a.budget, pagecheck_cap=a.pagecheck_cap,
                     f4=a.f4, runbooks=bool(a.runbooks_postgres), memory_from=a.memory_from if a.memory_postgres else "",
                     columns=a.columns, floor=a.floor, inject=a.inject)


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", required=True, help="query API over the eval database")
    ap.add_argument("--timeline", required=True)
    ap.add_argument("--recording", required=True, help="a name for the report, e.g. capture-2")
    ap.add_argument("--provider", required=True, choices=["ollama", "openai", "anthropic", "heuristic"])
    ap.add_argument("--model", required=True)
    ap.add_argument("--base-url", default="")
    ap.add_argument("--api-key-env", default="")
    ap.add_argument("--price-in", type=float, default=0.0, help="USD per million input tokens (0 = from agent/prices.json, else not priced)")
    ap.add_argument("--price-out", type=float, default=0.0, help="USD per million output tokens (0 = from agent/prices.json)")
    ap.add_argument("--systems", default="agent,baseline", help="any of agent (v0), graph (Week 6), baseline")
    ap.add_argument("--f4", action="store_true", help="graph: switch rule F4 on (off by default since patch 69; a Week 7 variant)")
    ap.add_argument("--pagecheck-cap", type=int, default=DEFAULT_CAP, help="graph: query calls page_check may make")
    ap.add_argument("--inject", default="", help="guardrail eval (patch 78): a bystander service that appears to log an "
                    "instruction to the agent (evals/inject.py); capture 2 only")
    ap.add_argument("--recheck-minutes", default=f"{graph.RECHECK_MINUTES:g}",
                    help="graph, end-to-end: look again this many minutes after the page when the first look ends unsure "
                         "(patch 76; default 2; 0 = the same moment, the time-vs-calls control; off = no second look)")
    ap.add_argument("--runbooks-postgres", default="", help="graph: runbooks on, searched in this Postgres (absent: the no-runbooks variant)")
    ap.add_argument("--memory-postgres", default="", help="graph: incident memory on, read-only, from this Postgres (absent: no memory)")
    ap.add_argument("--memory-from", default="", help="graph: memory sources to read, comma-separated recording names (never the one scored)")
    ap.add_argument("--label", default="", help="variant label, e.g. no-rb, no-mem, f4, b20: agent and graph rows become system:label")
    ap.add_argument("--resume", action="store_true", help="keep runs.jsonl, skip finished cells, retry failed ones (a run spread over days)")
    ap.add_argument("--columns", default="agent-only,end-to-end")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--budget", type=int, default=agent.DEFAULT_BUDGET)
    ap.add_argument("--scenarios", default="", help="comma-separated flags to run (default all)")
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--rpm", type=float, default=0.0, help="pace model requests to this many per minute (free tiers); 0 = no pacing")
    ap.add_argument("--reasoning-effort", default="", help="passed to OpenAI-compatible APIs (thinking models), e.g. low")
    ap.add_argument("--max-tokens", type=int, default=2048, help="output cap per model call (thinking models may need more)")
    # Explicit only, never from the environment: an exported variable made every runner test write into the real
    # ledger (Week 7 check 1, 21 Nov: 12 test requests under the Gemini model's name).
    ap.add_argument("--ledger", default="", help="request ledger file: every model request recorded, a daily cap enforced")
    ap.add_argument("--max-requests-per-day", type=int, default=DEFAULT_MAX_REQUESTS, help="ledger cap, per model per ledger day")
    ap.add_argument("--held-out", action="store_true", help="final scoring of an untouched recording: requires FROZEN.json")
    ap.add_argument("--floor", action="store_true", help="the deterministic floor (repeat the alert / top changed service) "
                    "instead of a model: overrides --provider and --model, needs --systems agent")
    ap.add_argument("--out", required=True)
    return ap


def main(argv=None) -> int:
    a = parser().parse_args(argv)
    if a.floor:
        a.provider, a.model = "heuristic", "floor"
    recheck = None if a.recheck_minutes.strip().lower() == "off" else float(a.recheck_minutes)
    if a.inject and a.held_out:
        print("refusing --inject with --held-out: the injection fixture is for capture 2 only", file=sys.stderr)
        return 2

    frozen = None
    if a.held_out:
        if not FROZEN.exists():
            print("refusing --held-out: no FROZEN.json (run python -m drishti_agent.evals.freeze first)", file=sys.stderr)
            return 2
        frozen = json.loads(FROZEN.read_text())
        if frozen["prompt_version"] != PROMPT_VERSION:
            print(f"refusing --held-out: prompts changed since the freeze ({frozen['prompt_version']} -> {PROMPT_VERSION})", file=sys.stderr)
            return 2
        if "graph" in a.systems.split(",") and frozen.get("graph_prompt_version") != GRAPH_PROMPT_VERSION:
            print(f"refusing --held-out: graph prompts not frozen or changed ({frozen.get('graph_prompt_version')} -> {GRAPH_PROMPT_VERSION})",
                  file=sys.stderr)
            return 2
        # Week 7 freeze (patch 80): the code and the variant are frozen too, not only the prompts.
        if frozen.get("version") != 2:
            print("refusing --held-out: FROZEN.json predates the Week 7 freeze (no code hash or variant set): freeze again", file=sys.stderr)
            return 2
        if frozen["code_hash"] != code_hash():
            print(f"refusing --held-out: the agent's code changed since the freeze ({frozen['code_hash']} -> {code_hash()})", file=sys.stderr)
            return 2
        sig = settings_signature(a)
        if sig not in frozen["variants"].values():
            print(f"refusing --held-out: these settings are not a frozen variant ({json.dumps(sig, sort_keys=True)}); "
                  f"frozen: {', '.join(frozen['variants'])}", file=sys.stderr)
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
    price_source = "command line" if (a.price_in or a.price_out) else ""
    if not price_source and a.provider != "heuristic":
        a.price_in, a.price_out, price_source = price_for(getattr(llm, "name", a.model))
    if frozen and a.provider != "heuristic" and getattr(llm, "name", a.model) != frozen["model"]:
        print(f"refusing --held-out: model {getattr(llm, 'name', a.model)} is not the frozen one ({frozen['model']})", file=sys.stderr)
        return 2
    if a.ledger and a.provider != "heuristic":   # the floor makes no model request
        llm = Metered(llm, Ledger(a.ledger, getattr(llm, "name", a.model), a.price_in, a.price_out, a.max_requests_per_day))
    meta = {"recording": a.recording, "model": getattr(llm, "name", a.model), "provider": a.provider, "prompt_version": PROMPT_VERSION,
            "graph_prompt_version": GRAPH_PROMPT_VERSION, "f4": a.f4, "pagecheck_cap": a.pagecheck_cap, "recheck_minutes": recheck, "inject": a.inject,
            "runbooks": bool(a.runbooks_postgres), "memory_from": a.memory_from if a.memory_postgres else "", "label": a.label,
            "git_commit": git_commit(), "runs": a.runs, "budget": a.budget, "held_out": a.held_out, "temperature": a.temperature, "rpm": a.rpm, "reasoning_effort": a.reasoning_effort, "max_tokens": a.max_tokens,
            "price_in": a.price_in, "price_out": a.price_out, "price_source": price_source, "ledger": a.ledger, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
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
    def label_of(sysname: str) -> str:
        return f"{sysname}:{a.label}" if a.label and sysname != "baseline" else sysname

    done = set()
    if a.resume and (out / "runs.jsonl").exists():
        with open(out / "runs.jsonl") as f:
            for line in f:
                if line.strip():
                    r0 = json.loads(line)
                    if not (r0.get("result") or {}).get("error"):      # failed cells are retried
                        done.add((r0["system"], r0["scenario"], r0["column"], r0["run"]))
        print(f"resume: {len(done)} finished cells kept", flush=True)
    stopped = None
    with open(out / "runs.jsonl", "a" if a.resume else "w") as f:
        for sc in scenarios:
            if sc.flag not in LABELS or stopped:
                continue
            for col in columns:
                until = sc.triggers.get(col)
                for sysname in systems:
                    for k in range(a.runs):
                        if stopped or (label_of(sysname), sc.n, col, k) in done:
                            continue
                        if isinstance(llm, Metered) and sc.gate_passed and until is not None:
                            try:
                                llm.ledger.check()   # don't start a cell the caps won't let finish its first request
                            except CapReached as e:
                                stopped = str(e)
                                print(f"stopping: {e}. Rerun the same command with --resume to continue.", flush=True)
                                continue
                            llm.cell = f"{label_of(sysname)}/{sc.flag}/{col}/{k}"
                        row = {"scenario": sc.n, "flag": sc.flag, "column": col, "system": label_of(sysname), "run": k,
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
                            if a.inject:
                                from .inject import InjectingAPI
                                api = InjectingAPI(api, a.inject)
                            # End-to-end runs start from the alert that paged; agent-only runs from a report with no alert.
                            alert = sc.trigger_alert if col == "end-to-end" else None
                            if sysname == "agent":
                                r = agent.investigate(api, llm, a.budget, alert)
                            elif sysname == "graph":
                                r = graph.investigate(api, llm, a.budget, alert, f4=a.f4, pc_cap=a.pagecheck_cap,
                                                      extras=gextras + ([mem.extra(alert)] if mem else []), local_tools=gtools,
                                                      recheck_minutes=recheck)
                            else:
                                r = baseline.diagnose(api, llm, alert)
                            s = score(sc.flag, r)
                            row.update(missed=False, score=s.to_json(), usd=round(cost(r, a.price_in, a.price_out), 6),
                                       result=r.model_dump(mode="json"))
                            if sysname == "graph":
                                # Would the proposed fix target a labelled service? Equals top-1 except for unsure:
                                # kept in the rows, not presented as a separate result (design §15).
                                row["fix_target"] = bool(r.diagnosis.root_cause_service) and s.top1
                            if r.error and re.search(r"429|rate.?limit|quota|resource.?exhausted", r.error, re.I):
                                stopped = r.error
                            print(f"  {sc.flag} {col} {sysname} #{k}: named={s.named} top1={s.top1} top3={s.top3} conf={s.confidence:.2f} "
                                  f"tools={len(r.tool_calls)} tok={r.usage.input_tokens}+{r.usage.output_tokens} {r.seconds}s"
                                  + (f" ERROR {r.error}" if r.error else ""), flush=True)
                        f.write(json.dumps(row) + "\n")
                        f.flush()
                        if stopped:
                            print(f"stopping: {'the daily cap' if 'ledger cap' in stopped else 'the provider' + chr(39) + 's limit outlasted its waits'} ({stopped[:120]}). "
                                  "Rerun the same command with --resume to continue.", flush=True)
    tracing.flush()  # LangSmith, when on: send buffered traces before the process exits
    meta["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    summary = report.write(out, meta)
    print(json.dumps(summary["totals"]))
    return 3 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
