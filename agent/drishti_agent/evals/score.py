"""Scoring one run against its label (docs/evals.md section 4).

  top1             root_cause_service is an accepted service
  top3             an accepted service is in the answer's top 3 (root cause first, then ranked_services)
  fault            fault_type is an accepted fault type
  unsure           no root cause named (null): honest, scored wrong
  confidently_wrong  a root cause named, wrong, with confidence >= CONFIDENT
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from ..models import RunResult
from .labels import LABELS

CONFIDENT = 0.7


@dataclass
class Score:
    top1: bool
    top3: bool
    fault: bool
    unsure: bool
    confidently_wrong: bool
    named: str | None
    confidence: float

    def to_json(self) -> dict:
        return asdict(self)


def score(flag: str, r: RunResult) -> Score:
    label = LABELS[flag]
    d = r.diagnosis
    named = d.root_cause_service
    top1 = named in label.services if named else False
    top3 = any(s in label.services for s in d.top3())
    unsure = named is None
    return Score(top1=top1, top3=top3, fault=d.fault_type.value in label.faults, unsure=unsure,
                 confidently_wrong=(not unsure and not top1 and d.confidence >= CONFIDENT), named=named, confidence=d.confidence)


def cost(r: RunResult, price_in: float, price_out: float) -> float:
    """USD, from token counts and prices per million tokens given on the command line."""
    return r.usage.input_tokens / 1e6 * price_in + r.usage.output_tokens / 1e6 * price_out
