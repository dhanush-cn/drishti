"""The capture C variant set (patch 80; Week 7 design section 7, option B), fixed before the freeze and recorded in it.

    python -m drishti_agent.evals.variants --set first        # one line per variant: name, runs, runner flags

"first" runs first on the free tier: the headline (graph without memory, v0, baseline, the floor; 3 runs each) and two
controls (no second look; no runbooks; 1 run each). "extra" runs only with quota left over, in this order. A control that
never runs is reported as not run, never estimated. The held-out runner refuses any setting that isn't one of these
(run.py, --held-out).

Placeholders in the flags: {pg} = the agent's Postgres (runbooks, memory). Memory, when on, reads capture 2's and
capture B's postmortems (seeded from their labels), never capture C's.
"""
from __future__ import annotations

import argparse
import sys

# name, set, runs, flags (the runner's, without --api/--timeline/--recording/--out/--provider/--model/--runs)
VARIANTS = [
    ("graph", "first", 3, ["--systems", "graph", "--runbooks-postgres", "{pg}"]),
    ("v0", "first", 3, ["--systems", "agent"]),
    ("baseline", "first", 3, ["--systems", "baseline"]),
    ("floor", "first", 3, ["--systems", "agent", "--floor"]),
    ("no-recheck", "first", 1, ["--systems", "graph", "--runbooks-postgres", "{pg}", "--recheck-minutes", "off",
                                "--columns", "end-to-end", "--label", "no-recheck"]),
    ("no-rb", "first", 1, ["--systems", "graph", "--label", "no-rb"]),
    ("recheck0", "extra", 1, ["--systems", "graph", "--runbooks-postgres", "{pg}", "--recheck-minutes", "0",
                              "--columns", "end-to-end", "--label", "recheck0"]),
    ("mem", "extra", 1, ["--systems", "graph", "--runbooks-postgres", "{pg}", "--memory-postgres", "{pg}",
                         "--memory-from", "capture-2,capture-b", "--label", "mem"]),
    ("b20", "extra", 1, ["--systems", "agent", "--budget", "20", "--label", "b20"]),
]


def signature(*, systems: str, label: str, recheck_minutes, budget: int, pagecheck_cap: int, f4: bool, runbooks: bool,
              memory_from: str, columns: str, floor: bool, inject: str) -> dict:
    """What makes a run one variant and not another (runs and resume are not part of it)."""
    graph = "graph" in systems.split(",")
    return {"systems": systems, "label": label, "recheck_minutes": recheck_minutes if graph else None, "budget": budget,
            "pagecheck_cap": pagecheck_cap if graph else None, "f4": f4 if graph else False,
            "runbooks": runbooks if graph else False, "memory_from": memory_from if graph else "",
            "columns": columns, "floor": floor, "inject": inject}


def signatures() -> dict[str, dict]:
    """Each variant's signature, by parsing its flags with the runner's own parser (no second source of truth)."""
    from .run import parser, settings_signature
    out = {}
    for name, _, _, flags in VARIANTS:
        a = parser().parse_args(["--api", "x", "--timeline", "x", "--recording", "x", "--provider", "ollama", "--model", "m",
                                 "--out", "x"] + [f.replace("{pg}", "postgresql://pg") for f in flags])
        out[name] = settings_signature(a)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", choices=["first", "extra", "all"], default="first")
    a = ap.parse_args(argv)
    for name, which, runs, flags in VARIANTS:
        if a.set in ("all", which):
            print(name, runs, " ".join(flags))
    return 0


if __name__ == "__main__":
    sys.exit(main())
