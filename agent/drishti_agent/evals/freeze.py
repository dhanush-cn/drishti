"""Freeze the prompts before the held-out run (docs/evals.md section 4, design change 7).

    python -m drishti_agent.evals.freeze --tuned-on capture-2 --hosted-check results/c2-hosted

Writes agent/FROZEN.json with the prompt version and the code commit. The
runner's --held-out refuses to score if the prompts no longer match. It
refuses to freeze without the hosted model's run on the tuning recording
(change 7: catch model-specific failures, such as tool-call formatting,
before the one held-out run).
"""
import argparse
import json
import sys
import time
from pathlib import Path

from ..prompts import PROMPT_VERSION
from .run import FROZEN, git_commit


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tuned-on", required=True)
    ap.add_argument("--hosted-check", required=True, help="results dir of the hosted model's run on the tuning recording")
    a = ap.parse_args(argv)
    s = Path(a.hosted_check) / "summary.json"
    if not s.exists():
        print(f"refusing to freeze: {s} missing (run the hosted model on {a.tuned_on} first)", file=sys.stderr)
        return 2
    meta = json.loads(s.read_text())["meta"]
    if meta["prompt_version"] != PROMPT_VERSION or meta["provider"] == "ollama":
        print(f"refusing to freeze: {s} is not a hosted run of the current prompts ({meta['provider']}, {meta['prompt_version']} vs {PROMPT_VERSION})", file=sys.stderr)
        return 2
    FROZEN.write_text(json.dumps({"prompt_version": PROMPT_VERSION, "git_commit": git_commit(), "tuned_on": a.tuned_on,
                                  "hosted_check": {"model": meta["model"], "dir": str(a.hosted_check)},
                                  "frozen_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, indent=1) + "\n")
    print(FROZEN.read_text())
    return 0


if __name__ == "__main__":
    sys.exit(main())
