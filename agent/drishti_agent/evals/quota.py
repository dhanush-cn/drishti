"""Today's model requests and spend from the ledger (patch 74).

    python -m drishti_agent.evals.quota --ledger results/ledger.jsonl [--day 2026-11-25]

One line per model: requests (failed ones included), tokens, USD, for the ledger day (08:00 UTC to 08:00 UTC,
13:30 IST, when the free tier's daily limit resets), and how many requests are left under the cap.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone

from ..ledger import DEFAULT_MAX_REQUESTS, day_of, totals


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--day", default="", help="a ledger day YYYY-MM-DD (default: today's)")
    ap.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    a = ap.parse_args(argv)
    day = date.fromisoformat(a.day) if a.day else day_of(datetime.now(timezone.utc))
    t = totals(a.ledger, day=day)
    print(f"ledger day {day} (from 08:00 UTC / 13:30 IST)")
    if not t:
        print("no requests")
    for model, m in sorted(t.items()):
        print(f"{model}: {m['requests']} requests ({m['errors']} failed), {m['input_tokens']} in / {m['output_tokens']} out tokens, "
              f"${m['usd']:.4f} at the given prices; left today: {max(0, a.max_requests - m['requests'])} requests")
    return 0


if __name__ == "__main__":
    sys.exit(main())
