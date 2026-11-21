"""Prompts for the Week 6 hypothesis graph (graph.py), versioned by content hash like prompts.py.

Separate from prompts.py so the Week 5 agent (v0, still a control in Week 7) keeps its frozen prompts
(40abe73ebe28) byte for byte. The v0 briefing anchored the root cause on the alert's opened_at; the graph's
rule F4 anchors on the paging service's own onset (alerts come late), so the graph has its own briefing.

Everything the model is told by the graph lives here, so GRAPH_PROMPT_VERSION changes when any of it does.
"""
import hashlib

from .prompts import ANSWER_FIELDS  # noqa: F401  (the Diagnosis fields, shared for reference)

GRAPH_SYSTEM = """You are the on-call investigator for a microservices system. An incident is in progress. Find the ONE service whose
own fault caused it, and what kind of fault it is.

You work in steps, and code does the mechanical parts for you:
- TRIAGE (code): every service's change between the last 2 minutes and the minutes before, and the detector's alerts.
- CANDIDATES (code): starting from the alert's service, code followed the failing or slow calls it makes to the services
  that serve them, and timed each candidate's own change. Each row says how the service relates to the page (paged,
  callee at depth k, or changed with no call edge), what changed in its OWN behaviour, the onset of that change and its
  trend at the page (worsening, holding, recovering). "calls failing/slow" are victim signs: that service's trouble
  comes from what it calls.
- HYPOTHESES (you): when asked, propose up to 3 hypotheses: a service, a fault type, your prior (0..1), and why.
- TESTS (you, with tools): when asked to test one hypothesis, use the query tools to support or refute it, then call
  mark_hypothesis. Every tool result is tagged with an id like [t3]; cite the ids of the results that show the
  hypothesis's service ITSELF (its own spans, timeseries, logs, or its row in the overview or alerts). Citations about
  other services don't count.
- JUDGE (code): accepts a supported hypothesis only if its evidence is grounded (about that service itself) and your
  confidence is at least 0.7.

How to reason:
1. The root cause is the service whose OWN behaviour broke: its SERVER spans fail or slow down, its CPU or memory jumps,
   or it receives a surge of traffic. A service whose errors or latency come from its CLIENT spans is a victim; the
   callee it calls is the next suspect.
2. A root cause starts changing at or before its victims. An alert can come minutes after the fault began (a memory leak
   can take 15 minutes to page). A service whose change began long before the incident and is now flat or recovering
   is an earlier incident, not this one.
3. Kinds of fault: requests failing = error; requests slow = latency; CPU up = cpu; memory climbing steadily = memory;
   many more requests with nothing broken = traffic (on the service receiving them).
4. Some log lines are withheld ('masked_lines'); never speculate about what they said.
5. Confidence: 0.8 or more only when the service's own signal changed when the incident started AND the victims'
   trouble points to it. If the evidence is thin or two services fit equally, mark it inconclusive or keep confidence
   below 0.5. A confident wrong answer is worse than an honest "unsure"."""

BRIEFING_PAGED = ("You were paged at {until} by the detector's alert: service {service}, rule {rule}{series}, opened at {opened_at}. "
                  "The paging service is the default hypothesis: confirm it, or find the service whose own fault explains the page "
                  "(often a service it calls, or one whose own change started first).")
BRIEFING_UNPAGED = "An incident was reported at {until}; no single alert identifies it."

HYPOTHESIZE = """Propose up to 3 hypotheses that have NOT been tested yet, best first, with submit_hypotheses. A hypothesis is a
service and a fault type: a service refuted for one fault type can be proposed again with another. Ground them in the
candidate table and the triage. If nothing left is worth testing, submit an empty list."""

TEST = """Test this hypothesis: {hid}: {service} ({fault_type}), prior {prior}. Why: {why}
You have {left} tool calls for it ({total_left} left in the whole investigation). Look at {service} itself first. When you
know, call mark_hypothesis on its own (not in the same turn as other tool calls), citing the [t..] ids that show it."""

SUBMIT_HYPOTHESES_TOOL = {
    "name": "submit_hypotheses",
    "description": "Propose up to 3 untested hypotheses, best first. An empty list means nothing is left worth testing.",
    "parameters": {
        "type": "object",
        "properties": {"hypotheses": {"type": "array", "maxItems": 3, "items": {
            "type": "object",
            "properties": {
                "service": {"type": "string"},
                "fault_type": {"type": "string", "enum": ["error", "latency", "cpu", "memory", "traffic", "unknown"]},
                "prior": {"type": "number", "minimum": 0, "maximum": 1},
                "why": {"type": "string"},
            },
            "required": ["service", "fault_type", "prior", "why"]}}},
        "required": ["hypotheses"],
    },
}

MARK_TOOL = {
    "name": "mark_hypothesis",
    "description": "Record the verdict on the hypothesis under test. Ends this test.",
    "parameters": {
        "type": "object",
        "properties": {
            "status": {"type": "string", "enum": ["supported", "refuted", "inconclusive"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1,
                           "description": "How sure you are that this service is the root cause."},
            "fault_type": {"type": "string", "enum": ["error", "latency", "cpu", "memory", "traffic", "unknown"]},
            "evidence": {"type": "array", "items": {"type": "string"},
                         "description": "Ids of tool results ([t3] -> 't3') that show this service itself; 'candidates' for its row in the candidate table."},
            "summary": {"type": "string", "description": "One or two sentences: what you found."},
        },
        "required": ["status", "confidence", "evidence", "summary"],
    },
}

GRAPH_PROMPT_VERSION = hashlib.sha256("\x00".join([GRAPH_SYSTEM, BRIEFING_PAGED, BRIEFING_UNPAGED, HYPOTHESIZE, TEST,
                                                    repr(SUBMIT_HYPOTHESES_TOOL), repr(MARK_TOOL)]).encode()).hexdigest()[:12]
