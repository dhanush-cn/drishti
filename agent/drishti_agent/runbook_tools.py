"""Runbooks and their retrieval (design F11; docs/agent.md section 5).

- One runbook per service, RENDERED from one template and the facts in runbooks/services.json (role, what it calls,
  who calls it; from the demo's public docs). Every service gets the same checks for every fault type and none gets a
  failure mode of its own: the runbooks were written knowing the scenarios, and a uniform template is how they can't
  turn into an answer key. A test checks the headings are identical and no fault-injection word appears.
- runbooks/symptoms.md: general operations knowledge, one section per symptom.
- Two tools for the graph's test step: runbook(service) is a plain lookup (one document per service: no search
  needed), search_runbooks(query) is Postgres full-text search over every section. Measured on a fixed query set
  (runbooks/queries.json, written before any agent run): recall@3. Embeddings only if that number is poor.
- Runbook results are never evidence for the judge: a runbook says what to check, not what happened.

    python -m drishti_agent.runbook_tools render     # write the rendered runbooks to runbooks/rendered/ (committed)
    python -m drishti_agent.runbook_tools recall --postgres URL
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DIR = Path(__file__).parent / "runbooks"
SECTIONS = ("relations", "error", "latency", "cpu", "memory", "traffic", "cause-or-victim")
MAX_CHUNK = 900


@dataclass(frozen=True)
class Chunk:
    id: str          # "<service>#<section>" or "symptoms#<slug>"
    service: str     # "" for symptoms
    title: str
    body: str


def services() -> dict:
    return json.loads((DIR / "services.json").read_text())["services"]


def _list(xs: list[str]) -> str:
    return ", ".join(xs) if xs else "nothing (no other service)"


def render(name: str, s: dict) -> str:
    """One service's runbook. The template is the same for every service: only its name and relations vary."""
    calls, callers = s["calls"], s["called_by"]
    deps = _list(calls)
    leaf = not calls
    return f"""# {name}

## relations
{name} {s['role']}. It calls {deps}. It is called by {_list(callers)}.

## error
Look at {name}'s SERVER spans: is the error ratio up on its own operations, on one or on all of them? """ + (
        f"{name} calls no other service, so errors on its SERVER spans are its own. " if leaf else
        f"If its errors are on CLIENT spans to {deps}, the failing callee is the next suspect, not {name}. ") + f"""Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare {name}'s SERVER p95 with its baseline, operation by operation. """ + (
        f"{name} calls no other service, so slowness on its SERVER spans comes from {name} itself: check its CPU and memory. " if leaf else
        f"If its CLIENT spans to {deps} slowed down first, follow the slowest callee. If not, check {name}'s CPU and memory. ") + f"""Use a timeseries to see when the slowdown started.

## cpu
Compare {name}'s container CPU with its baseline. CPU up together with SERVER p95 up means {name} is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from {_list(callers)}.

## memory
Look at {name}'s memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare {name}'s SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers ({_list(callers)}) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When {name} is at fault, its callers ({_list(callers)}) show CLIENT errors or latency towards {name}, starting at or after
{name}'s own change. When {name} is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans""" + ("" if leaf else f" to {deps}") + ".\n"


def _slug(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


def chunks() -> list[Chunk]:
    out = []
    for name, s in sorted(services().items()):
        for part in render(name, s).split("\n## ")[1:]:
            title, _, body = part.partition("\n")
            out.append(Chunk(f"{name}#{title.strip()}", name, f"{name}: {title.strip()}", body.strip()))
    for part in (DIR / "symptoms.md").read_text().split("\n## ")[1:]:
        title, _, body = part.partition("\n")
        out.append(Chunk(f"symptoms#{_slug(title)}", "", title.strip(), body.strip()))
    return out


def lookup(service: str) -> Optional[str]:
    s = services().get((service or "").strip().lower())
    return render(service.strip().lower(), s) if s else None


class PostgresIndex:
    """Full-text search over every section, in the agent's Postgres (one database, three uses: design §6)."""

    TABLE = "runbook_chunks"

    def __init__(self, url: str):
        self.url = url

    def _conn(self):
        import psycopg
        return psycopg.connect(self.url, autocommit=True)

    def build(self) -> int:
        """(Re)build the index from the files. Idempotent: the table is replaced, so it always matches the repo.
        Indexed: the symptoms guide and each service's relations section. A service's per-fault sections are the
        same template text for every service; indexed, 13 copies of it filled the top 3 of most symptom queries
        (first measurement: recall@3 23/30). They stay one runbook(service) call away."""
        cs = [x for x in chunks() if not x.service or x.id.endswith("#relations")]
        with self._conn() as c:
            c.execute(f"DROP TABLE IF EXISTS {self.TABLE}")
            c.execute(f"""CREATE TABLE {self.TABLE} (id text PRIMARY KEY, service text NOT NULL, title text NOT NULL, body text NOT NULL,
                          tsv tsvector GENERATED ALWAYS AS (setweight(to_tsvector('english', title), 'A') ||
                                                            setweight(to_tsvector('english', body), 'B')) STORED)""")
            c.execute(f"CREATE INDEX {self.TABLE}_tsv ON {self.TABLE} USING GIN (tsv)")
            with c.cursor() as cur:
                cur.executemany(f"INSERT INTO {self.TABLE} (id, service, title, body) VALUES (%s, %s, %s, %s)",
                                [(x.id, x.service, x.title, x.body) for x in cs])
        return len(cs)

    def search(self, query: str, k: int = 3) -> list[dict]:
        """Ranked sections for a free-text query. websearch_to_tsquery ANDs the words; if that finds nothing, the
        words are ORed, so a long query still returns its closest sections."""
        words = re.findall(r"[a-z0-9]+", (query or "").lower())
        if not words:
            return []
        with self._conn() as c:
            sql = (f"SELECT id, title, body, ts_rank_cd(tsv, q) AS rank FROM {self.TABLE}, {{q}} AS q "
                   f"WHERE tsv @@ q ORDER BY rank DESC, id LIMIT %s")
            rows = c.execute(sql.format(q="websearch_to_tsquery('english', %s)"), (query, k)).fetchall()
            if not rows:
                rows = c.execute(sql.format(q="to_tsquery('english', %s)"), (" | ".join(words), k)).fetchall()
        return [{"id": r[0], "title": r[1], "body": r[2][:MAX_CHUNK]} for r in rows]


RUNBOOK_TOOL = {"name": "runbook", "description": "The runbook for one service: what it does, what it calls, who calls it, and what "
                                                  "to check for each kind of fault. Says what to look at, not what happened.",
                "parameters": {"type": "object", "properties": {"service": {"type": "string"}}, "required": ["service"]}}
SEARCH_TOOL = {"name": "search_runbooks", "description": "Search every runbook section and the symptoms guide for a symptom, e.g. "
                                                        "'deadline exceeded on client spans'. Returns the 3 closest sections.",
               "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}


def local_tools(index: PostgresIndex) -> dict:
    """The graph's local tools: name -> (schema, function(arguments) -> (ok, text))."""
    def runbook(args):
        text = lookup(str(args.get("service") or ""))
        return (True, text) if text else (False, f"error: no runbook for {args.get('service')!r}; known: {', '.join(sorted(services()))}")

    def search(args):
        try:
            hits = index.search(str(args.get("query") or ""))
        except Exception as e:  # the investigation goes on without runbooks
            return False, f"error: runbook search failed: {e}"
        if not hits:
            return True, "no matching runbook section"
        return True, "\n\n".join(f"[{h['id']}] {h['title']}\n{h['body']}" for h in hits)

    return {"runbook": (RUNBOOK_TOOL, runbook), "search_runbooks": (SEARCH_TOOL, search)}


def relations_extra(state: dict) -> str:
    """Context for hypothesize: the relations line of each candidate with an own change (short; the full runbook is a
    tool call away)."""
    lines = []
    for c in (state.get("pc") or {}).get("candidates") or []:
        if c.get("own_change"):
            s = services().get(c["service"])
            if s:
                lines.append(f"- {c['service']}: {s['role']}; calls {_list(s['calls'])}; called by {_list(s['called_by'])}")
    return ("## what the changed candidates do (runbooks)\n" + "\n".join(lines)) if lines else ""


def recall(index: PostgresIndex, k: int = 3) -> tuple[int, int, list[dict]]:
    qs = json.loads((DIR / "queries.json").read_text())["queries"]
    misses, hit = [], 0
    for q in qs:
        got = [h["id"] for h in index.search(q["q"], k)]
        if any(w in got for w in q["want"]):
            hit += 1
        else:
            misses.append({"q": q["q"], "want": q["want"], "got": got})
    return hit, len(qs), misses


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if argv[:1] == ["render"]:
        out = DIR / "rendered"
        out.mkdir(exist_ok=True)
        for name, s in sorted(services().items()):
            (out / f"{name}.md").write_text(render(name, s))
        print(f"{len(services())} runbooks in {out}")
        return 0
    if argv[:1] == ["recall"] and len(argv) >= 3 and argv[1] == "--postgres":
        idx = PostgresIndex(argv[2])
        n = idx.build()
        hit, total, misses = recall(idx)
        print(json.dumps({"chunks": n, "recall_at_3": f"{hit}/{total}", "misses": misses}, indent=1))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
