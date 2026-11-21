"""Freeze the agent before the held-out run (docs/evals.md sections 4 and 11; Week 7 design G5).

    python -m drishti_agent.evals.freeze --tuned-on capture-2 --hosted-check results/capture-2/graph-hosted-<version>

Writes agent/FROZEN.json, version 2 (patch 80). The Week 5 file froze the v0 prompts; Week 7's also freezes:
- the graph's prompts (GRAPH_PROMPT_VERSION) and the agent's code (a hash of every file under agent/drishti_agent, so a
  code change after the freeze is caught even when no prompt changed);
- the model (from the hosted check), and the capture C variant set with each variant's settings (evals/variants.py);
- the caps, the second look's settings, the memory sources, the price file's hash, the git commit and tree.

It refuses: without the hosted model's graph run on the tuning recording, of the current prompts, with the second look
on at its frozen delay (design change 7 in Week 5: catch model-specific failures before the one held-out run); and with
uncommitted changes to tracked files (FROZEN.json itself aside), so the commit it records is the code that was run.
The runner's --held-out then refuses a cell whose code, prompts, model or settings don't match.
"""
import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

from .. import graph
from ..pagecheck import DEFAULT_CAP
from ..prompts import PROMPT_VERSION
from ..prompts_graph import GRAPH_PROMPT_VERSION
from .run import FROZEN, PRICES, code_hash, git_commit
from .variants import VARIANTS, signatures


def _git(*args) -> str:
    return subprocess.check_output(["git", *args], cwd=Path(__file__).parent, text=True).strip()


def dirty() -> list[str]:
    """Tracked files with uncommitted changes, FROZEN.json aside."""
    try:
        lines = _git("status", "--porcelain", "--untracked-files=no").splitlines()
    except Exception:
        return []
    return [l for l in lines if not l.rstrip().endswith("agent/FROZEN.json")]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tuned-on", required=True)
    ap.add_argument("--hosted-check", required=True, help="results dir of the hosted model's graph run on the tuning recording")
    a = ap.parse_args(argv)
    s = Path(a.hosted_check) / "summary.json"
    if not s.exists():
        print(f"refusing to freeze: {s} missing (run the hosted model on {a.tuned_on} first)", file=sys.stderr)
        return 2
    meta = json.loads(s.read_text())["meta"]
    if meta["prompt_version"] != PROMPT_VERSION or meta["provider"] in ("ollama", "heuristic"):
        print(f"refusing to freeze: {s} is not a hosted run of the current prompts ({meta['provider']}, {meta['prompt_version']} vs {PROMPT_VERSION})", file=sys.stderr)
        return 2
    if meta.get("graph_prompt_version") != GRAPH_PROMPT_VERSION:
        print(f"refusing to freeze: {s} ran graph prompts {meta.get('graph_prompt_version')}, the code has {GRAPH_PROMPT_VERSION}", file=sys.stderr)
        return 2
    if meta.get("recheck_minutes") != graph.RECHECK_MINUTES:
        print(f"refusing to freeze: {s} ran with the second look at {meta.get('recheck_minutes')}, not {graph.RECHECK_MINUTES:g} min", file=sys.stderr)
        return 2
    d = dirty()
    if d:
        print("refusing to freeze: uncommitted changes to tracked files:\n  " + "\n  ".join(d), file=sys.stderr)
        return 2
    try:
        tree = _git("rev-parse", "HEAD^{tree}")
    except Exception:
        tree = "unknown"
    frozen = {
        "version": 2,
        "prompt_version": PROMPT_VERSION, "graph_prompt_version": GRAPH_PROMPT_VERSION, "code_hash": code_hash(),
        "git_commit": git_commit(), "git_tree": tree, "model": meta["model"],
        "variants": signatures(),
        "variant_sets": {name: {"set": which, "runs": runs} for name, which, runs, _ in VARIANTS},
        "caps": {"model_tool_calls": graph.DEFAULT_BUDGET, "pagecheck_calls": DEFAULT_CAP, "rounds": graph.MAX_ROUNDS,
                 "llm_calls": graph.MAX_LLM_CALLS, "recheck_tools": graph.RECHECK_TOOLS, "recheck_llm_calls": graph.RECHECK_LLM_CALLS},
        "recheck_minutes": graph.RECHECK_MINUTES, "f4": False, "memory_from": "capture-2,capture-b",
        "prices_sha256": hashlib.sha256(PRICES.read_bytes()).hexdigest()[:16] if PRICES.exists() else None,
        "tuned_on": a.tuned_on, "hosted_check": {"model": meta["model"], "dir": str(a.hosted_check)},
        "frozen_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    FROZEN.write_text(json.dumps(frozen, indent=1) + "\n")
    print(FROZEN.read_text())
    return 0


if __name__ == "__main__":
    sys.exit(main())
