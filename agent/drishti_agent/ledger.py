"""The request ledger: every model request written down, and a daily cap that stops a run before the free quota runs
out under it (patch 74; design note Week 7, G7).

    llm = Metered(llm, Ledger("results/ledger.jsonl", model=llm.name))
    llm.cell = "graph/adHighCpu/end-to-end/0"     # what the next requests are for
    llm.chat(...)                                   # checked against the cap, then recorded

Why: on 21 Nov the free daily quota ran out although our own runs had counted 148 + 61 requests. Something else
used the key's quota, and nothing on our side could say how much we had used. Week 7 scores capture C on the free tier
only, over several days, from a project with no billing account (nothing can be charged). So every request is appended
as one JSON line (time, model, cell, tokens, status), and before each request the day's count for that model is summed
from the file. At the cap the request is refused with CapReached, which the eval runner treats like a 429 that
outlasted its waits: it stops cleanly and `--resume` continues the next day. The cap sits below the free tier's 500 so
live checks and retries keep some headroom, and a run stops on our count before the provider starts refusing.

Choices:
- The file is the state. It is read on every check (a day is a few thousand lines), so two runs started at once share
  one budget instead of each believing it has the whole cap.
- A failed request is recorded and counted. Whether the provider counts it depends on the failure; counting it errs
  towards stopping early, never towards overspending.
- The cap is per model: the free tier's limits are per model, and a local Ollama run in the same ledger uses none.
- The day starts at 08:00 UTC = 13:30 IST = midnight Pacific (standard time), when the free tier's daily limit resets.
- USD is recorded per request when prices are given (what the run would cost on a paid tier, reported in Week 7's cost
  column), but nothing is capped on it: with no billing account there is nothing to spend.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Callable, Optional

from .llm import LLMError
from .models import Usage

DAY_START_UTC = time(8, 0)
DEFAULT_MAX_REQUESTS = 450   # per model per day; the free tier allows 500, so live checks keep some headroom


class CapReached(LLMError):
    """The ledger's own daily cap. The text says "quota" so a runner that stops on provider quota errors stops here too."""


def day_of(t: datetime, day_start: time = DAY_START_UTC) -> date:
    """The ledger day a moment belongs to: days run from day_start UTC to day_start UTC."""
    shift = timedelta(hours=day_start.hour, minutes=day_start.minute)
    return (t.astimezone(timezone.utc) - shift).date()


class Ledger:
    def __init__(self, path: str, model: str, price_in: float = 0.0, price_out: float = 0.0,
                 max_requests: int = DEFAULT_MAX_REQUESTS,
                 day_start: time = DAY_START_UTC, clock: Optional[Callable[[], datetime]] = None):
        self.path, self.model = path, model
        self.price_in, self.price_out = price_in, price_out
        self.max_requests = max_requests
        self.day_start = day_start
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)

    def usd(self, u: Usage) -> float:
        return u.input_tokens / 1e6 * self.price_in + u.output_tokens / 1e6 * self.price_out

    def today(self) -> dict:
        """This model's totals for the current ledger day."""
        return totals(self.path, self.day_start, day=day_of(self.clock(), self.day_start)).get(
            self.model, {"requests": 0, "errors": 0, "input_tokens": 0, "output_tokens": 0, "usd": 0.0})

    def check(self) -> None:
        t = self.today()
        if t["requests"] >= self.max_requests:
            raise CapReached(f"ledger cap: daily quota of {self.max_requests} requests for {self.model} reached "
                             f"({t['requests']} today, day starts {self.day_start:%H:%M} UTC)")

    def record(self, cell: str, usage: Usage, status: str, error: str = "") -> None:
        row = {"t": self.clock().astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
               "model": self.model, "cell": cell, "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
               "usd": round(self.usd(usage), 8), "status": status}
        if error:
            row["error"] = error[:300]
        with open(self.path, "a") as f:
            f.write(json.dumps(row) + "\n")


def totals(path: str, day_start: time = DAY_START_UTC, day: Optional[date] = None) -> dict[str, dict]:
    """Per model totals for one ledger day (requests include failed ones)."""
    out: dict[str, dict] = {}
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            t = datetime.fromisoformat(r["t"].replace("Z", "+00:00"))
            if day is not None and day_of(t, day_start) != day:
                continue
            m = out.setdefault(r["model"], {"requests": 0, "errors": 0, "input_tokens": 0, "output_tokens": 0, "usd": 0.0})
            m["requests"] += 1
            m["errors"] += r.get("status") != "ok"
            m["input_tokens"] += r.get("input_tokens", 0)
            m["output_tokens"] += r.get("output_tokens", 0)
            m["usd"] += r.get("usd", 0.0)
    return out


class Metered:
    """An LLM client that checks the ledger before each request and records it after. Everything else passes through."""

    def __init__(self, inner, ledger: Ledger):
        self.inner, self.ledger = inner, ledger
        self.cell = ""

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def chat(self, system, messages, tools=None):
        self.ledger.check()
        try:
            r = self.inner.chat(system, messages, tools)
        except LLMError as e:
            self.ledger.record(self.cell, Usage(), "error", str(e))
            raise
        self.ledger.record(self.cell, r.usage, "ok")
        return r
